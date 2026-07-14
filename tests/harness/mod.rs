// Each integration test file (`tests/*.rs`) is compiled as a separate binary
// crate. `mod harness;` includes the full harness in every binary, so functions
// not used by a particular test trigger dead_code warnings. There is no way to
// share a library module across integration test binaries without extracting it
// into a separate crate.
#![allow(dead_code)]

pub mod alpaca_mocks;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, GasFiller, JoinFill, NonceFiller,
    SimpleNonceManager,
};
use alloy::providers::{Identity, ProviderBuilder};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use httpmock::Mock;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use std::collections::HashMap;
use std::net::SocketAddr;
use url::Url;

use st0x_issuance::account::{AccountLinkResponse, RegisterAccountResponse};
use st0x_issuance::bindings::IST0xOrchestratorV1;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::{
    LocalEvm, ROLE_CERTIFY, ROLE_DEPOSIT, ROLE_WITHDRAW,
};
use st0x_issuance::{
    AlpacaConfig, AuthConfig, ChainConfig, Config, Environment, IpWhitelist,
    LogLevel, Network, SignerConfig, VaultModeConfig, VaultModeKind,
};

/// The internal API key every harness-built config and request header share:
/// the config value and the `X-API-KEY` header must stay identical, or every
/// authenticated assertion fails with a 401 that hides the real cause.
pub const TEST_API_KEY: &str = "test-key-12345678901234567890123456";

pub type TestProviderBuilder = ProviderBuilder<
    Identity,
    JoinFill<
        JoinFill<
            JoinFill<JoinFill<Identity, GasFiller>, BlobGasFiller>,
            NonceFiller<SimpleNonceManager>,
        >,
        ChainIdFiller,
    >,
>;

pub async fn wait_for_shares<T>(
    vault: &OffchainAssetReceiptVaultInstance<T>,
    wallet: Address,
) -> Result<U256, Box<dyn std::error::Error>>
where
    T: alloy::providers::Provider,
{
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(15);
    let poll_interval = tokio::time::Duration::from_millis(100);

    loop {
        let balance = vault.balanceOf(wallet).call().await?;
        if balance > U256::ZERO {
            return Ok(balance);
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for shares after {}s",
                timeout.as_secs()
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn wait_for_burn<T>(
    vault: &OffchainAssetReceiptVaultInstance<T>,
    wallet: Address,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: alloy::providers::Provider,
{
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(15);
    let poll_interval = tokio::time::Duration::from_millis(100);

    loop {
        let balance = vault.balanceOf(wallet).call().await?;
        if balance == U256::ZERO {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for burn. Balance: {balance}"
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn wait_for_mock_hit(
    mock: &Mock<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_mock_hits(mock, 1).await
}

/// Polls the event store until the mint's terminal `MintCompleted` event is
/// COMMITTED. A callback-mock hit alone is not terminality: the mock counts
/// the request on arrival, while the service still has to process the
/// response and persist `RecordCallbackSent -> MintCompleted` — a window a
/// fast test can win locally and lose on a loaded CI runner. Anything that
/// gates on the mint being terminal (service shutdown before a custody
/// migration's quiescence check, restarts asserting no recovery work) must
/// wait on this, not on the mock.
pub async fn wait_for_mint_completed(
    db_url: &str,
    issuer_request_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await?;
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(15);
    let poll_interval = tokio::time::Duration::from_millis(50);

    loop {
        let count = sqlx::query_scalar::<_, i64>(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Mint'
              AND aggregate_id = ?
              AND event_type = 'MintEvent::MintCompleted'
            ",
        )
        .bind(issuer_request_id)
        .fetch_one(&pool)
        .await?;

        if count >= 1 {
            pool.close().await;
            return Ok(());
        }

        if start.elapsed() >= timeout {
            pool.close().await;
            return Err(format!(
                "Timeout waiting for MintCompleted on {issuer_request_id} \
                 after {}s",
                timeout.as_secs()
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn wait_for_mock_hits(
    mock: &Mock<'_>,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    // Must comfortably exceed the transfer poller's 5s POLL_INTERVAL: a
    // transfer landing just after a poll pass is only detected on the next
    // tick, so a 5s wait races the poller by construction.
    let timeout = tokio::time::Duration::from_secs(15);
    let poll_interval = tokio::time::Duration::from_millis(50);

    loop {
        if mock.calls_async().await >= expected {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for mock to be hit {expected} time(s) \
                 (got {} after {}s)",
                mock.calls_async().await,
                timeout.as_secs()
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Launches the full HTTP service on an OS-assigned ephemeral port in the
/// background and returns its base URL once it accepts connections, so e2e
/// tests can drive it through a real network client (e.g.
/// `st0x_issuance_client::IssuanceClient`) over TCP rather than the in-process
/// `rocket::local` client. Each call binds a distinct port, so test binaries
/// that `cargo test` runs in parallel never collide.
pub async fn spawn_http_server(
    config: Config,
) -> Result<Url, Box<dyn std::error::Error>> {
    // Bind port 0 to let the OS pick a free port, read the assigned number,
    // then drop the listener so Rocket can claim it. This replaces a fixed
    // port that would collide across parallel test binaries. The release/rebind
    // gap is a brief TOCTOU, but if anything grabs the port first the bind
    // failure surfaces through `launch_err_rx` below as a clear error rather
    // than a silent run against the wrong process.
    let address: SocketAddr =
        std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;

    let rocket = initialize_rocket(config).await?;

    // `build_rocket` hard-codes port 8000 for production; override it with the
    // ephemeral port for this test without touching the production path.
    let figment =
        rocket.figment().clone().merge((rocket::Config::PORT, address.port()));
    let rocket = rocket.configure(figment);

    // Forward a `launch()` failure on a oneshot rather than letting it vanish
    // into a detached task: without this, a launch error (bind failure, ignite
    // error) would degrade into the generic readiness timeout below with the
    // real cause lost to stderr.
    let (launch_err_tx, launch_err_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        if let Err(error) = rocket.launch().await {
            // If readiness already won the select the receiver is gone and
            // `send` hands the error back. `rocket::Error` panics on Drop unless
            // it has been inspected, so Display it here (which also keeps the
            // post-startup-failure diagnostic) instead of dropping it silently.
            if let Err(unsent) = launch_err_tx.send(error) {
                eprintln!("e2e HTTP server exited after startup: {unsent}");
            }
        }
    });

    // Race readiness against a launch failure so the actual error surfaces as
    // the test failure instead of a 10s "never started listening" timeout.
    tokio::select! {
        biased;
        launch_err = launch_err_rx => {
            // `Ok` carries the launch error (Display-ing it also marks the
            // rocket::Error handled, dodging its Drop panic); `Err` is a dropped
            // sender, i.e. the server task ended without a launch error.
            let message = launch_err.map_or_else(
                |_| "e2e HTTP server exited before it became ready".to_string(),
                |error| format!("e2e HTTP server failed to launch: {error}"),
            );
            Err(message.into())
        }
        ready = wait_until_listening(address) => {
            ready?;
            Ok(Url::parse(&format!("http://{address}/"))?)
        }
    }
}

async fn wait_until_listening(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(10);
    let poll_interval = tokio::time::Duration::from_millis(50);

    loop {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "HTTP server never started listening on {address}"
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn seed_tokenized_asset(client: &Client, vault: Address) {
    seed_tokenized_asset_with(client, vault, "AAPL", "tAAPL").await;
}

pub async fn seed_tokenized_asset_with(
    client: &Client,
    vault: Address,
    underlying: &str,
    token: &str,
) {
    let response = client
        .post("/tokenized-assets")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .body(
            json!({
                "underlying": underlying,
                "token": token,
                "network": "base",
                "vault": vault
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert!(
        response.status() == rocket::http::Status::Created
            || response.status() == rocket::http::Status::Ok,
        "Failed to seed tokenized asset: {:?}",
        response.into_string().await
    );
}

/// Pre-seeds the tokenized asset into the database BEFORE the service starts.
///
/// This allows `initialize_rocket` to discover the asset during startup,
/// so that receipt backfill and redemption monitoring are wired for this vault.
///
/// Seeds the `events` table with a `TokenizedAsset::Added` event before the
/// Rocket service starts.
///
/// Per AGENTS.md "Setup phase exception", direct event store seeding is
/// permitted in e2e test setup phases. The tokenized asset view is rebuilt
/// from events by `initialize_rocket` during startup (via the
/// `TokenizedAsset` projection's catch-up in `StoreBuilder::build`), so only
/// the event needs to be seeded.
pub async fn preseed_tokenized_asset(
    db_url: &str,
    vault: Address,
    underlying: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    preseed_tokenized_asset_into_pool(&pool, vault, underlying, token).await?;

    pool.close().await;

    Ok(())
}

/// Seeds the `events` table with a `TokenizedAsset::Added` event using an
/// existing pool. Use this when the caller manages the pool lifecycle
/// (e.g., when multiple assets must be seeded before closing the pool).
pub async fn preseed_tokenized_asset_into_pool(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    vault: Address,
    underlying: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    preseed_tokenized_asset_into_pool_with_network(
        pool,
        vault,
        underlying,
        token,
        Network::Base,
    )
    .await
}

pub async fn preseed_tokenized_asset_into_pool_with_network(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    vault: Address,
    underlying: &str,
    token: &str,
    network: Network,
) -> Result<(), Box<dyn std::error::Error>> {
    let aggregate_id = format!("{underlying}:{}", network.as_str());
    let now = Utc::now();

    let event_payload = json!({
        "Added": {
            "underlying": underlying,
            "token": token,
            "network": network.as_str(),
            "vault": vault,
            "added_at": now
        }
    });

    let event_payload_str = event_payload.to_string();

    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (
            'TokenizedAsset',
            ?,
            1,
            'TokenizedAssetEvent::Added', '1.0', ?, '{}'
        )
        ",
    )
    .bind(aggregate_id)
    .bind(&event_payload_str)
    .execute(pool)
    .await?;

    Ok(())
}

/// Seeds an asset that is currently frozen: an `Added` listing event plus a
/// `Frozen` event on the underlying-keyed `Underlying` aggregate (corporate
/// action freezes are underlying-scoped). Per the AGENTS.md setup-phase
/// exception, only the event store is seeded; the views are rebuilt from
/// these events by `initialize_rocket` at startup.
pub async fn preseed_frozen_tokenized_asset(
    db_url: &str,
    vault: Address,
    underlying: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    preseed_tokenized_asset_into_pool(&pool, vault, underlying, token).await?;

    let frozen_payload = json!({
        "Frozen": { "frozen_at": Utc::now() }
    })
    .to_string();

    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (
            'Underlying',
            ?,
            1,
            'UnderlyingEvent::Frozen', '1.0', ?, '{}'
        )
        ",
    )
    .bind(underlying)
    .bind(&frozen_payload)
    .execute(&pool)
    .await?;

    pool.close().await;

    Ok(())
}

pub async fn setup_account(
    client: &Client,
    user_wallet: Address,
) -> AccountLinkResponse {
    let register_response = client
        .post("/accounts")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .body(json!({"email": "user@example.com"}).to_string())
        .dispatch()
        .await;

    assert_eq!(register_response.status(), rocket::http::Status::Ok);
    let _: RegisterAccountResponse = register_response
        .into_json()
        .await
        .expect("register response must contain valid JSON");

    let link_response = client
        .post("/accounts/connect")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .body(
            json!({"email": "user@example.com", "account": "USER123"})
                .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(link_response.status(), rocket::http::Status::Ok);
    let link_body: AccountLinkResponse = link_response
        .into_json()
        .await
        .expect("link response must contain valid JSON");

    let whitelist_response = client
        .post(format!("/accounts/{}/wallets", link_body.client_id))
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .body(json!({"wallet": user_wallet}).to_string())
        .dispatch()
        .await;

    assert_eq!(whitelist_response.status(), rocket::http::Status::Ok);

    link_body
}

/// Builds a test [`Config`] with a single Base chain wired to `evm`.
pub fn create_config_with_db(
    db_path: &str,
    mock_alpaca: &MockServer,
    evm: &LocalEvm,
) -> Result<Config, Box<dyn std::error::Error>> {
    let rpc_url = Url::parse(&evm.endpoint)?;

    Ok(Config {
        database_url: db_path.to_string(),
        database_max_connections: 5,
        rpc_url: rpc_url.clone(),
        chain_id: evm.chain_id,
        signer: SignerConfig::Local(evm.private_key),
        backfill_start_block: 0,
        receipt_poll_interval: tokio::time::Duration::from_millis(500),
        auth: AuthConfig {
            issuer_api_key: TEST_API_KEY.parse().expect("Valid API key"),
            alpaca_ip_ranges: IpWhitelist::single(
                "127.0.0.1/32".parse().expect("Valid IP range"),
            ),
            internal_ip_ranges: "127.0.0.0/8,::1/128"
                .parse()
                .expect("Valid IP ranges"),
        },
        log_level: LogLevel::Debug,
        environment: Environment::Development,
        hyperdx: None,
        alpaca: AlpacaConfig {
            api_base_url: mock_alpaca.base_url(),
            account_id: "test-account".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        },
        lifecycle_notifications:
            st0x_issuance::LifecycleNotificationsConfig::disabled(),
        chains: vec![ChainConfig {
            network: Network::Base,
            chain_id: evm.chain_id,
            rpc_url,
            backfill_start_block: 0,
        }],
        vault_mode_config: VaultModeConfig::default(),
    })
}

/// Builds a [`Config`] wired to two Anvil chains: Base and Ethereum, both
/// entries in `Config::chains`.
pub fn create_multichain_config_with_db(
    db_path: &str,
    mock_alpaca: &MockServer,
    base_evm: &LocalEvm,
    eth_evm: &LocalEvm,
) -> Result<Config, Box<dyn std::error::Error>> {
    let mut base_config =
        create_config_with_db(db_path, mock_alpaca, base_evm)?;
    base_config.chains = vec![
        ChainConfig {
            network: Network::Base,
            chain_id: base_evm.chain_id,
            rpc_url: Url::parse(&base_evm.endpoint)?,
            backfill_start_block: 0,
        },
        ChainConfig {
            network: Network::Ethereum,
            chain_id: eth_evm.chain_id,
            rpc_url: Url::parse(&eth_evm.endpoint)?,
            backfill_start_block: 0,
        },
    ];

    Ok(base_config)
}

/// Same as [`create_config_with_db`] but with a per-asset `VaultModeConfig`
/// (orchestrator-mode overrides) instead of the all-vault-direct default.
pub fn create_config_with_vault_modes(
    db_path: &str,
    mock_alpaca: &MockServer,
    evm: &LocalEvm,
    vault_mode_config: VaultModeConfig,
) -> Result<Config, Box<dyn std::error::Error>> {
    let mut config = create_config_with_db(db_path, mock_alpaca, evm)?;
    config.vault_mode_config = vault_mode_config;
    Ok(config)
}

/// Mints `amount` share-wei of the primary vault's token to
/// `recipient_signer`'s address through `orchestrator.mint()`, creating one
/// orchestrator-custodied receipt per call. The recipient authorizes the mint
/// by signing the orchestrator's EIP-712 digest; the bot wallet (deployer,
/// `MINT_ROLE`) submits it.
pub async fn orchestrator_mint_to(
    evm: &LocalEvm,
    orchestrator_address: Address,
    recipient_signer: &PrivateKeySigner,
    amount: U256,
    nonce: B256,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider = bot_provider(evm).await?;
    let orchestrator =
        IST0xOrchestratorV1::new(orchestrator_address, &provider);

    let recipient = recipient_signer.address();
    let digest = orchestrator
        .mintAuthDigest(evm.vault_address, recipient, amount, nonce)
        .call()
        .await?;
    let signature = recipient_signer.sign_hash_sync(&digest)?;
    let auth = IST0xOrchestratorV1::MintAuthV1 {
        nonce,
        signature: Bytes::from(signature.as_bytes().to_vec()),
    };

    orchestrator
        .mint(evm.vault_address, recipient, amount, auth, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    Ok(())
}

/// The one-time unlimited ERC-20 approval ops issues at token onboarding so
/// the orchestrator can pull the bot wallet's shares via `transferFrom`.
pub async fn approve_orchestrator(
    evm: &LocalEvm,
    orchestrator_address: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider = bot_provider(evm).await?;
    let vault =
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &provider);

    vault
        .approve(orchestrator_address, U256::MAX)
        .send()
        .await?
        .get_receipt()
        .await?;

    Ok(())
}

pub async fn setup_roles(
    evm: &LocalEvm,
    user_wallet: Address,
    bot_wallet: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    Ok(())
}

pub async fn setup_roles_on_vault(
    evm: &LocalEvm,
    authorizer_address: Address,
    vault_address: Address,
    user_wallet: Address,
    bot_wallet: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    evm.grant_role_on_authorizer(authorizer_address, ROLE_DEPOSIT, user_wallet)
        .await?;
    evm.grant_role_on_authorizer(authorizer_address, ROLE_WITHDRAW, bot_wallet)
        .await?;
    evm.grant_role_on_authorizer(
        authorizer_address,
        ROLE_CERTIFY,
        evm.wallet_address,
    )
    .await?;
    evm.certify_specific_vault(vault_address, U256::MAX).await?;
    Ok(())
}

pub struct MintFlowRequest<'a> {
    pub client_id: &'a str,
    pub tokenization_request_id: &'a str,
    pub quantity: &'a str,
    pub underlying: &'a str,
    pub token: &'a str,
    pub network: Network,
}

pub async fn perform_mint_and_confirm(
    client: &Client,
    wallet: Address,
    client_id: &str,
    tokenization_request_id: &str,
    quantity: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    perform_mint_and_confirm_with(
        client,
        wallet,
        MintFlowRequest {
            client_id,
            tokenization_request_id,
            quantity,
            underlying: "AAPL",
            token: "tAAPL",
            network: Network::Base,
        },
    )
    .await
}

pub async fn perform_mint_and_confirm_with(
    client: &Client,
    wallet: Address,
    request: MintFlowRequest<'_>,
) -> Result<String, Box<dyn std::error::Error>> {
    let issuer_request_id =
        initiate_mint_request(client, wallet, &request).await?;
    confirm_mint_journal(
        client,
        request.tokenization_request_id,
        &issuer_request_id,
    )
    .await?;

    Ok(issuer_request_id)
}

/// Drives `POST /inkind/issuance` alone, returning the issuer request id —
/// orchestrator mint flows deliver the recipient authorization between this
/// and [`confirm_mint_journal`].
pub async fn initiate_mint_request(
    client: &Client,
    wallet: Address,
    request: &MintFlowRequest<'_>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mint_response = client
        .post("/inkind/issuance")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .body(
            json!({
                "tokenization_request_id": request.tokenization_request_id,
                "qty": request.quantity,
                "underlying_symbol": request.underlying,
                "token_symbol": request.token,
                "network": request.network.as_str(),
                "client_id": request.client_id,
                "wallet_address": wallet
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(mint_response.status(), rocket::http::Status::Ok);
    let mint_body: MintResponse = mint_response
        .into_json()
        .await
        .expect("mint response must contain valid JSON");

    Ok(mint_body.issuer_request_id.to_string())
}

/// Drives `POST /inkind/issuance/confirm` for an initiated mint.
pub async fn confirm_mint_journal(
    client: &Client,
    tokenization_request_id: &str,
    issuer_request_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let confirm_response = client
        .post("/inkind/issuance/confirm")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .body(
            json!({
                "tokenization_request_id": tokenization_request_id,
                "issuer_request_id": issuer_request_id,
                "status": "completed"
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(confirm_response.status(), rocket::http::Status::Ok);

    Ok(())
}

/// Seeds a `SchemaRegistry` event recording the last-known schema version for
/// an aggregate. Used in setup phases to simulate production DB state before a
/// `SCHEMA_VERSION` bump.
pub async fn seed_schema_registry_version(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    aggregate_name: &str,
    version: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let sequence: i64 = sqlx::query_scalar(
        "
        SELECT COALESCE(MAX(sequence), 0) + 1
        FROM events
        WHERE aggregate_type = 'SchemaRegistry'
          AND aggregate_id = 'schema'
        ",
    )
    .fetch_one(pool)
    .await?;

    let payload = json!({
        "VersionUpdated": {
            "name": aggregate_name,
            "version": version,
        }
    })
    .to_string();

    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (
            'SchemaRegistry',
            'schema',
            ?,
            'SchemaRegistryEvent::VersionUpdated',
            '1.0',
            ?,
            '{}'
        )
        ",
    )
    .bind(sequence)
    .bind(payload)
    .execute(pool)
    .await?;

    Ok(())
}

/// Inserts a pre-event-sorcery snapshot row: bare aggregate enum JSON (e.g.
/// `{"Completed": {...}}`) rather than the `Lifecycle` wrapper
/// (`{"Live": {...}}`). Reproduces production DB state that bricks startup if
/// stale snapshots are not cleared before projection catch-up.
pub async fn seed_pre_lifecycle_snapshot(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    aggregate_type: &str,
    aggregate_id: &str,
    last_sequence: i64,
    payload: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "
        INSERT INTO snapshots (
            aggregate_type,
            aggregate_id,
            last_sequence,
            snapshot_version,
            payload,
            timestamp
        )
        VALUES (?, ?, ?, 0, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .bind(last_sequence)
    .bind(payload.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

/// Inserts a pre-event-sorcery canonical projection row (bare aggregate JSON).
pub async fn seed_pre_lifecycle_view_row(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    table: &str,
    view_id: &str,
    version: i64,
    payload: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = match table {
        "account_view" => {
            "
            INSERT INTO account_view (view_id, version, payload)
            VALUES (?, ?, ?)
        "
        }
        "mint_view" => {
            "
            INSERT INTO mint_view (view_id, version, payload)
            VALUES (?, ?, ?)
        "
        }
        "tokenized_asset_view" => {
            "
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES (?, ?, ?)
        "
        }
        other => {
            return Err(format!(
                "unsupported view table for pre-Lifecycle fixture: {other}"
            )
            .into());
        }
    };

    sqlx::query(query)
        .bind(view_id)
        .bind(version)
        .bind(payload.to_string())
        .execute(pool)
        .await?;

    Ok(())
}

pub fn create_provider() -> TestProviderBuilder {
    ProviderBuilder::new()
        .disable_recommended_fillers()
        .with_gas_estimation()
        .filler(BlobGasFiller)
        .with_simple_nonce_management()
        .filler(ChainIdFiller::default())
}

/// `amount` whole tokens in 18-decimal share-wei.
pub fn tokens(amount: u64) -> U256 {
    U256::from(amount) * U256::from(10u64).pow(U256::from(18u64))
}

/// A `VaultModeConfig` putting one underlying in orchestrator mode over a
/// vault-direct default — the single-asset-pilot shape, with the
/// orchestrator address registered for the harness's Base network.
pub fn orchestrator_vault_modes(
    underlying: &str,
    orchestrator_address: Address,
) -> VaultModeConfig {
    VaultModeConfig::new(
        HashMap::from([(underlying.to_string(), VaultModeKind::Orchestrator)]),
        VaultModeKind::VaultDirect,
        HashMap::from([(Network::Base, orchestrator_address)]),
    )
}

/// A provider signing with the bot wallet's key.
pub async fn bot_provider(
    evm: &LocalEvm,
) -> Result<impl alloy::providers::Provider + Clone, Box<dyn std::error::Error>>
{
    let bot_signer = PrivateKeySigner::from_bytes(&evm.private_key)?;
    Ok(create_provider()
        .wallet(EthereumWallet::from(bot_signer))
        .connect(&evm.endpoint)
        .await?)
}

/// Dispatches an authenticated admin `GET`, failing loudly on a non-OK
/// status or a non-JSON body so a broken endpoint can never read as a
/// healthy/empty response.
pub async fn authenticated_get_json(
    client: &Client,
    path: &str,
) -> serde_json::Value {
    let response = client
        .get(path)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .dispatch()
        .await;
    assert_eq!(
        response.status(),
        rocket::http::Status::Ok,
        "{path} must respond OK"
    );
    response
        .into_json()
        .await
        .unwrap_or_else(|| panic!("{path} must return a JSON body"))
}

/// Fetches the current `GET /admin/stuck` entries, failing loudly on an
/// endpoint error so a broken endpoint can never read as "nothing stuck".
pub async fn fetch_stuck_entries(client: &Client) -> Vec<serde_json::Value> {
    let body = authenticated_get_json(client, "/admin/stuck").await;

    body["stuck"]
        .as_array()
        .expect("/admin/stuck must contain a stuck array")
        .clone()
}
