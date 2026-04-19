// Each integration test file (`tests/*.rs`) is compiled as a separate binary
// crate. `mod harness;` includes the full harness in every binary, so functions
// not used by a particular test trigger dead_code warnings. There is no way to
// share a library module across integration test binaries without extracting it
// into a separate crate.
#![allow(dead_code)]

pub mod alpaca_mocks;

use alloy::hex;
use alloy::primitives::{Address, U256};
use chrono::Utc;
use httpmock::Mock;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use std::collections::HashMap;
use url::Url;

use st0x_issuance::account::{AccountLinkResponse, RegisterAccountResponse};
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::{
    LocalEvm, ROLE_CERTIFY, ROLE_DEPOSIT, ROLE_WITHDRAW,
};
use st0x_issuance::{
    ANVIL_CHAIN_ID, AlpacaConfig, AuthConfig, Config, IpWhitelist, LogLevel,
    SignerConfig,
};

pub async fn wait_for_shares<T>(
    vault: &OffchainAssetReceiptVaultInstance<T>,
    wallet: Address,
) -> Result<U256, Box<dyn std::error::Error>>
where
    T: alloy::providers::Provider,
{
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
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
    let timeout = tokio::time::Duration::from_secs(5);
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

pub async fn wait_for_mock_hits(
    mock: &Mock<'_>,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
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
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
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
/// from events by `initialize_rocket` during startup (via
/// `replay_tokenized_asset_view`), so only the event needs to be seeded.
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
    let aggregate_id = underlying;
    let now = Utc::now();

    let event_payload = json!({
        "Added": {
            "underlying": underlying,
            "token": token,
            "network": "base",
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

pub async fn setup_account(
    client: &Client,
    user_wallet: Address,
) -> AccountLinkResponse {
    let register_response = client
        .post("/accounts")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(json!({"email": "user@example.com"}).to_string())
        .dispatch()
        .await;

    assert_eq!(register_response.status(), rocket::http::Status::Ok);
    let _: RegisterAccountResponse =
        register_response.into_json().await.unwrap();

    let link_response = client
        .post("/accounts/connect")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(
            json!({"email": "user@example.com", "account": "USER123"})
                .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(link_response.status(), rocket::http::Status::Ok);
    let link_body: AccountLinkResponse =
        link_response.into_json().await.unwrap();

    let whitelist_response = client
        .post(format!("/accounts/{}/wallets", link_body.client_id))
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(json!({"wallet": user_wallet}).to_string())
        .dispatch()
        .await;

    assert_eq!(whitelist_response.status(), rocket::http::Status::Ok);

    link_body
}

/// Schema hash used by the mock subgraph in e2e tests.
pub const TEST_OA_SCHEMA_HASH: &str =
    "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm";

/// Builds a mock Rain meta v1 `information` hex string containing the given
/// schema hash, suitable for the subgraph `receiptVaultInformations` response.
fn mock_information_hex(schema_hash: &str) -> String {
    let schema_hex = hex::encode(schema_hash);
    let payload_hex = format!("78{:02x}{schema_hex}", schema_hash.len());

    // Rain meta v1 prefix + OA_SCHEMA CBOR item + OA_HASH_LIST CBOR item
    format!(
        "0xff0a89c674ee7874\
         a40058020000011bffa8e8a9b9cf4a3102706170706c69636174696f6e2f6a736f6e03676465666c617465\
         a200{payload_hex}011bff9fae3cc645f463"
    )
}

/// Sets up a mock subgraph server that returns `OA_SCHEMA` hashes for specific
/// vaults. Each entry maps a vault address to a schema hash. The mock validates
/// the request body contains the expected vault address (as lowercase hex),
/// ensuring `OaSchemaCache` sends correct per-vault GraphQL queries.
pub fn setup_mock_subgraph(
    vault_schemas: &HashMap<Address, &str>,
) -> MockServer {
    let server = MockServer::start();

    for (vault, schema_hash) in vault_schemas {
        let vault_hex = format!("{vault:#x}");
        let information = mock_information_hex(schema_hash);

        server.mock(|when, then| {
            when.method(POST).path("/").body_includes(&vault_hex);
            then.status(200).json_body(json!({
                "data": {
                    "receiptVaultInformations": [{
                        "information": information
                    }]
                }
            }));
        });
    }

    server
}

/// Returns `(Config, MockServer)` — the caller must keep the `MockServer` alive
/// for the duration of the test so the subgraph mock remains reachable.
pub fn create_config_with_db(
    db_path: &str,
    mock_alpaca: &MockServer,
    evm: &LocalEvm,
) -> Result<(Config, MockServer), Box<dyn std::error::Error>> {
    let vault_schemas =
        HashMap::from([(evm.vault_address, TEST_OA_SCHEMA_HASH)]);
    let mock_subgraph = setup_mock_subgraph(&vault_schemas);
    let subgraph_url =
        Url::parse(&mock_subgraph.base_url()).expect("valid mock subgraph URL");

    Ok((
        Config {
            database_url: db_path.to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse(&evm.endpoint)?,
            chain_id: ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(evm.private_key),
            backfill_start_block: 0,
            auth: AuthConfig {
                issuer_api_key: "test-key-12345678901234567890123456"
                    .parse()
                    .expect("Valid API key"),
                alpaca_ip_ranges: IpWhitelist::single(
                    "127.0.0.1/32".parse().expect("Valid IP range"),
                ),
                internal_ip_ranges: "127.0.0.0/8,::1/128"
                    .parse()
                    .expect("Valid IP ranges"),
            },
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig {
                api_base_url: mock_alpaca.base_url(),
                account_id: "test-account".to_string(),
                api_key: "test-key".to_string(),
                api_secret: "test-secret".to_string(),
                connect_timeout_secs: 10,
                request_timeout_secs: 30,
            },
            subgraph_url,
        },
        mock_subgraph,
    ))
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
        client_id,
        tokenization_request_id,
        quantity,
        "AAPL",
        "tAAPL",
    )
    .await
}

pub async fn perform_mint_and_confirm_with(
    client: &Client,
    wallet: Address,
    client_id: &str,
    tokenization_request_id: &str,
    quantity: &str,
    underlying: &str,
    token: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mint_response = client
        .post("/inkind/issuance")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(
            json!({
                "tokenization_request_id": tokenization_request_id,
                "qty": quantity,
                "underlying_symbol": underlying,
                "token_symbol": token,
                "network": "base",
                "client_id": client_id,
                "wallet_address": wallet
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(mint_response.status(), rocket::http::Status::Ok);
    let mint_body: MintResponse = mint_response.into_json().await.unwrap();
    let issuer_request_id = mint_body.issuer_request_id.to_string();

    let confirm_response = client
        .post("/inkind/issuance/confirm")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(
            json!({
                "tokenization_request_id": tokenization_request_id,
                "issuer_request_id": mint_body.issuer_request_id,
                "status": "completed"
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(confirm_response.status(), rocket::http::Status::Ok);

    Ok(issuer_request_id)
}
