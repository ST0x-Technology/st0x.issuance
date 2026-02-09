#![allow(clippy::unwrap_used)]

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, Bytes, TxHash, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::{Mock, prelude::*};
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use url::Url;

use st0x_issuance::account::{AccountLinkResponse, RegisterAccountResponse};
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::mint::{IssuerRequestId, MintResponse};
use st0x_issuance::test_utils::{LocalEvm, test_alpaca_legacy_auth};
use st0x_issuance::{
    AlpacaConfig, AuthConfig, Config, IpWhitelist, initialize_rocket,
};

async fn wait_for_shares<T>(
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

async fn wait_for_burn<T>(
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

        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for burn. Balance: {balance}"
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn wait_for_mock_hit(
    mock: &Mock<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
    let poll_interval = tokio::time::Duration::from_millis(50);

    loop {
        if mock.calls_async().await > 0 {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err("Timeout waiting for mock to be hit".into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn seed_tokenized_asset(client: &Client, vault: Address) {
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
                "underlying": "AAPL",
                "token": "tAAPL",
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

async fn setup_account(
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

async fn perform_mint_flow(
    client: &Client,
    evm: &LocalEvm,
    user_wallet: Address,
) -> Result<U256, Box<dyn std::error::Error>> {
    let link_body = setup_account(client, user_wallet).await;

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
                "tokenization_request_id": "alp-mint-789",
                "qty": "50.0",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "network": "base",
                "client_id": link_body.client_id,
                "wallet_address": user_wallet
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(mint_response.status(), rocket::http::Status::Ok);
    let mint_body: MintResponse = mint_response.into_json().await.unwrap();

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
                "tokenization_request_id": "alp-mint-789",
                "issuer_request_id": mint_body.issuer_request_id,
                "status": "completed"
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(confirm_response.status(), rocket::http::Status::Ok);

    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    wait_for_shares(&vault, user_wallet).await
}

fn setup_mint_mocks(mock_alpaca: &MockServer) -> Mock {
    let (basic_auth, api_key, api_secret) = test_alpaca_legacy_auth();

    mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/callback/mint")
            .header("authorization", basic_auth)
            .header("APCA-API-KEY-ID", api_key)
            .header("APCA-API-SECRET-KEY", api_secret);
        then.status(200).body("");
    })
}

struct RedemptionState {
    issuer_request_id: String,
    tx_hash: String,
    qty: String,
}

fn setup_redemption_mocks(
    mock_alpaca: &MockServer,
    user_wallet: Address,
) -> (Mock, Mock) {
    let (basic_auth, api_key, api_secret) = test_alpaca_legacy_auth();
    let shared_state = Arc::new(Mutex::new(Vec::<RedemptionState>::new()));

    let shared_state_redeem = Arc::clone(&shared_state);

    let redeem_mock = mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/redeem")
            .header("authorization", &basic_auth)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret);

        then.status(200).respond_with(
            move |req: &httpmock::HttpMockRequest| {
                let body: serde_json::Value =
                    serde_json::from_slice(req.body().as_ref()).unwrap();
                let issuer_request_id =
                    body["issuer_request_id"].as_str().unwrap().to_string();
                let tx_hash = body["tx_hash"].as_str().unwrap().to_string();
                let qty = body["qty"].as_str().unwrap().to_string();

                shared_state_redeem.lock().unwrap().push(RedemptionState {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: tx_hash.clone(),
                    qty: qty.clone(),
                });

                let response_body = serde_json::to_string(&json!({
                    "tokenization_request_id": format!("tok-redeem-{}", issuer_request_id),
                    "issuer_request_id": issuer_request_id,
                    "created_at": "2025-09-12T17:28:48.642437-04:00",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": qty,
                    "issuer": "test-issuer",
                    "network": "base",
                    "wallet_address": user_wallet,
                    "tx_hash": tx_hash,
                    "fees": "0.5"
                }))
                .unwrap();

                httpmock::HttpMockResponse {
                    status: Some(200),
                    headers: None,
                    body: Some(response_body.into()),
                }
            },
        );
    });

    let poll_mock = mock_alpaca.mock(|when, then| {
        when.method(GET)
            .path_matches(r"^/v1/accounts/test-account/tokenization/requests.*")
            .header("authorization", &basic_auth)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret);

        then.status(200).respond_with(
            move |_req: &httpmock::HttpMockRequest| {
                let responses: Vec<_> = {
                    let states = shared_state.lock().unwrap();
                    states
                        .iter()
                        .map(|state| {
                            json!({
                                "tokenization_request_id": format!("tok-redeem-{}", state.issuer_request_id),
                                "issuer_request_id": state.issuer_request_id,
                                "created_at": "2025-09-12T17:28:48.642437-04:00",
                                "type": "redeem",
                                "status": "completed",
                                "underlying_symbol": "AAPL",
                                "token_symbol": "tAAPL",
                                "qty": state.qty,
                                "issuer": "test-issuer",
                                "network": "base",
                                "wallet_address": user_wallet,
                                "tx_hash": state.tx_hash,
                                "fees": "0.5"
                            })
                        })
                        .collect()
                };

                let response_body = serde_json::to_string(&responses).unwrap();

                httpmock::HttpMockResponse {
                    status: Some(200),
                    headers: None,
                    body: Some(response_body.into()),
                }
            },
        );
    });

    (redeem_mock, poll_mock)
}

#[tokio::test]
async fn test_tokenization_flow() -> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    // Use second Anvil test account for user (first account is bot)
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    let config = Config {
        database_url: ":memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse(&evm.endpoint)?,
        chain_id: st0x_issuance::ANVIL_CHAIN_ID,
        signer: st0x_issuance::SignerConfig::Local(evm.private_key),
        vault: evm.vault_address,
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
        log_level: st0x_issuance::LogLevel::Debug,
        hyperdx: None,
        alpaca: AlpacaConfig {
            api_base_url: mock_alpaca.base_url(),
            account_id: "test-account".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        },
    };

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    seed_tokenized_asset(&client, evm.vault_address).await;

    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    let shares_minted = perform_mint_flow(&client, &evm, user_wallet).await?;
    mint_callback_mock.assert();

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    vault
        .transfer(bot_wallet, shares_minted)
        .send()
        .await?
        .get_receipt()
        .await?;

    wait_for_burn(&vault, bot_wallet).await?;

    redeem_mock.assert();
    poll_mock.assert();

    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no shares after redemption"
    );

    Ok(())
}

/// Helper to create a config with a specific database path
fn create_config_with_db(
    db_path: &str,
    mock_alpaca: &MockServer,
    evm: &LocalEvm,
) -> Result<Config, Box<dyn std::error::Error>> {
    Ok(Config {
        database_url: db_path.to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse(&evm.endpoint)?,
        chain_id: st0x_issuance::ANVIL_CHAIN_ID,
        signer: st0x_issuance::SignerConfig::Local(evm.private_key),
        vault: evm.vault_address,
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
        log_level: st0x_issuance::LogLevel::Debug,
        hyperdx: None,
        alpaca: AlpacaConfig {
            api_base_url: mock_alpaca.base_url(),
            account_id: "test-account".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        },
    })
}

/// Sets up redemption mocks that use shared state across restarts.
/// The `poll_should_succeed` flag controls whether the poll returns "completed" or "pending".
fn setup_redemption_mocks_with_shared_state(
    mock_alpaca: &MockServer,
    user_wallet: Address,
    shared_issuer_id: Arc<Mutex<Option<IssuerRequestId>>>,
    shared_tx_hash: Arc<Mutex<Option<TxHash>>>,
    poll_should_succeed: Arc<AtomicBool>,
) -> (Mock, Mock) {
    let (basic_auth, api_key, api_secret) = test_alpaca_legacy_auth();

    let shared_issuer_id_clone = Arc::clone(&shared_issuer_id);
    let shared_tx_hash_clone = Arc::clone(&shared_tx_hash);

    let redeem_mock = mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/redeem")
            .header("authorization", &basic_auth)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret);

        then.status(200).respond_with(
            move |req: &httpmock::HttpMockRequest| {
                let body: serde_json::Value =
                    serde_json::from_slice(req.body().as_ref()).unwrap();
                let issuer_request_id = IssuerRequestId::new(
                    body["issuer_request_id"].as_str().unwrap(),
                );
                let tx_hash: TxHash =
                    body["tx_hash"].as_str().unwrap().parse().unwrap();

                *shared_issuer_id_clone.lock().unwrap() =
                    Some(issuer_request_id.clone());
                *shared_tx_hash_clone.lock().unwrap() = Some(tx_hash);

                let response_body = serde_json::to_string(&json!({
                    "tokenization_request_id": "tok-redeem-recovery",
                    "issuer_request_id": issuer_request_id.to_string(),
                    "created_at": "2025-09-12T17:28:48.642437-04:00",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "50",
                    "issuer": "test-issuer",
                    "network": "base",
                    "wallet_address": user_wallet,
                    "tx_hash": tx_hash,
                    "fees": "0.5"
                }))
                .unwrap();

                httpmock::HttpMockResponse {
                    status: Some(200),
                    headers: None,
                    body: Some(response_body.into()),
                }
            },
        );
    });

    let poll_mock = mock_alpaca.mock(|when, then| {
        when.method(GET)
            .path_matches(r"^/v1/accounts/test-account/tokenization/requests.*")
            .header("authorization", &basic_auth)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret);

        then.status(200).respond_with(
            move |_req: &httpmock::HttpMockRequest| {
                let issuer_request_id =
                    shared_issuer_id.lock().unwrap().clone().expect(
                        "issuer_request_id should be set by redeem mock",
                    );
                let tx_hash = shared_tx_hash
                    .lock()
                    .unwrap()
                    .expect("tx_hash should be set by redeem mock");

                // Check the flag to determine status
                let status = if poll_should_succeed.load(Ordering::SeqCst) {
                    "completed"
                } else {
                    "pending"
                };

                let response_body = serde_json::to_string(&json!([{
                    "tokenization_request_id": "tok-redeem-recovery",
                    "issuer_request_id": issuer_request_id.to_string(),
                    "created_at": "2025-09-12T17:28:48.642437-04:00",
                    "type": "redeem",
                    "status": status,
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "50",
                    "issuer": "test-issuer",
                    "network": "base",
                    "wallet_address": user_wallet,
                    "tx_hash": tx_hash,
                    "fees": "0.5"
                }]))
                .unwrap();

                httpmock::HttpMockResponse {
                    status: Some(200),
                    headers: None,
                    body: Some(response_body.into()),
                }
            },
        );
    });

    (redeem_mock, poll_mock)
}

/// Tests that MintManager and BurnManager share nonce state correctly.
///
/// This test reproduces the "nonce too low" bug that occurs when:
/// 1. MintManager and BurnManager have separate blockchain providers
/// 2. Each provider has its own nonce cache
/// 3. After a burn advances the on-chain nonce (via BurnManager's provider),
///    MintManager's provider still has its old cached nonce
/// 4. The next mint fails with "nonce too low"
///
/// The test performs: mint -> burn -> mint
/// With the bug present, the second mint fails.
/// After the fix (shared provider), all operations succeed.
#[tokio::test]
async fn test_mint_burn_mint_nonce_synchronization()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    let config = create_config_with_db(":memory:", &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    seed_tokenized_asset(&client, evm.vault_address).await;
    setup_roles(&evm, user_wallet, bot_wallet).await?;

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Step 1: First mint
    let link_body = setup_account(&client, user_wallet).await;
    let client_id = link_body.client_id.to_string();

    perform_mint_and_confirm(
        &client,
        user_wallet,
        &client_id,
        "alp-mint-1",
        "50.0",
    )
    .await?;
    let shares_minted = wait_for_shares(&vault, user_wallet).await?;
    assert!(
        shares_minted > U256::ZERO,
        "User should have shares after first mint"
    );

    // Step 2: Burn (advances on-chain nonce)
    vault
        .transfer(bot_wallet, shares_minted)
        .send()
        .await?
        .get_receipt()
        .await?;
    wait_for_burn(&vault, bot_wallet).await?;
    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no shares after burn"
    );

    // Step 3: Second mint (fails with stale nonce if bug is present)
    perform_mint_and_confirm(
        &client,
        user_wallet,
        &client_id,
        "alp-mint-2",
        "25.0",
    )
    .await?;
    let final_balance = wait_for_shares(&vault, user_wallet).await?;
    assert!(
        final_balance > U256::ZERO,
        "User should have shares after second mint"
    );

    Ok(())
}

async fn setup_roles(
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

async fn perform_mint_and_confirm(
    client: &Client,
    wallet: Address,
    client_id: &str,
    tokenization_request_id: &str,
    quantity: &str,
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
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
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

async fn verify_burn_records_created(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    expected_burned: U256,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
    let poll_interval = tokio::time::Duration::from_millis(100);

    let burn_records = loop {
        let records = sqlx::query!(
            r#"
            SELECT view_id, payload as "payload!: String"
            FROM receipt_burns_view
            WHERE json_extract(payload, '$.Burned') IS NOT NULL
            "#
        )
        .fetch_all(pool)
        .await?;

        if !records.is_empty() {
            break records;
        }

        assert!(
            start.elapsed() <= timeout,
            "Timeout waiting for burn record in receipt_burns_view"
        );

        tokio::time::sleep(poll_interval).await;
    };

    let burn_payload: serde_json::Value =
        serde_json::from_str(&burn_records[0].payload)?;
    let burned_shares_hex = burn_payload["Burned"]["burns"][0]["shares_burned"]
        .as_str()
        .expect("Should have shares_burned");
    let burned_shares =
        U256::from_str_radix(burned_shares_hex.trim_start_matches("0x"), 16)?;

    assert_eq!(
        burned_shares, expected_burned,
        "Burn record should show expected shares were burned"
    );

    Ok(())
}

/// Tests that burn tracking correctly computes available balance.
///
/// The architecture uses two separate views:
/// - `ReceiptInventoryView`: tracks mints (keyed by mint's issuer_request_id)
/// - `ReceiptBurnsView`: tracks burns (keyed by redemption aggregate_id)
///
/// Available balance is computed at query time by joining these views.
/// This avoids the cross-aggregate update problem where cqrs-es GenericQuery
/// uses aggregate_id as view_id.
#[tokio::test]
async fn test_burn_tracking_computes_available_balance_correctly()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Use file-based database so we can query it directly
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_receipt_inventory.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    let config = create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    seed_tokenized_asset(&client, evm.vault_address).await;

    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Setup user provider
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Setup account and perform mint
    let link_body = setup_account(&client, user_wallet).await;
    let issuer_request_id = perform_mint_and_confirm(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-inventory",
        "50.0",
    )
    .await?;

    // Wait for shares to be minted
    let shares_minted = wait_for_shares(&vault, user_wallet).await?;
    assert!(shares_minted > U256::ZERO, "Mint should create shares");

    // Query receipt_inventory_view to get initial state
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let initial_view: (String,) = sqlx::query_as(
        "SELECT payload FROM receipt_inventory_view WHERE view_id = ?",
    )
    .bind(&issuer_request_id)
    .fetch_one(&query_pool)
    .await?;

    let initial_payload: serde_json::Value =
        serde_json::from_str(&initial_view.0)?;
    let initial_balance = initial_payload["Active"]["current_balance"]
        .as_str()
        .expect("Should have current_balance");

    assert_eq!(
        initial_balance,
        format!("{shares_minted:#x}"),
        "Initial balance should match minted shares"
    );

    // Transfer all shares to bot wallet to trigger redemption
    vault
        .transfer(bot_wallet, shares_minted)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Wait for burn to complete
    wait_for_burn(&vault, bot_wallet).await?;

    // Verify burns are tracked correctly
    verify_burn_records_created(&query_pool, shares_minted).await?;

    // Verify receipt_inventory_view was NOT updated (expected with new architecture)
    let after_burn_view: (String,) = sqlx::query_as(
        "SELECT payload FROM receipt_inventory_view WHERE view_id = ?",
    )
    .bind(&issuer_request_id)
    .fetch_one(&query_pool)
    .await?;

    let after_burn_payload: serde_json::Value =
        serde_json::from_str(&after_burn_view.0)?;
    let view_balance_hex = after_burn_payload["Active"]["current_balance"]
        .as_str()
        .expect("Should still be Active state");
    let view_balance =
        U256::from_str_radix(view_balance_hex.trim_start_matches("0x"), 16)?;

    assert_eq!(
        view_balance, shares_minted,
        "ReceiptInventoryView.current_balance should remain at initial value"
    );

    Ok(())
}

/// Tests that redemption recovery works after a simulated restart.
///
/// This test simulates a scenario where:
/// 1. A redemption is initiated (tokens transferred to bot wallet)
/// 2. The redemption progresses to the "AlpacaCalled" state
/// 3. The application "crashes" (we drop the rocket instance)
/// 4. On restart, recovery picks up the stuck redemption and completes it
#[tokio::test]
async fn test_redemption_recovery_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Use a temp file for the database so it persists between rocket instances
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_recovery.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Shared state across mock instances - None until populated by the redeem mock
    let shared_issuer_id: Arc<Mutex<Option<IssuerRequestId>>> =
        Arc::new(Mutex::new(None));
    let shared_tx_hash: Arc<Mutex<Option<TxHash>>> = Arc::new(Mutex::new(None));
    let poll_should_succeed = Arc::new(AtomicBool::new(false));

    // Setup mocks - poll returns "pending" until we flip the flag
    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks_with_shared_state(
        &mock_alpaca,
        user_wallet,
        Arc::clone(&shared_issuer_id),
        Arc::clone(&shared_tx_hash),
        Arc::clone(&poll_should_succeed),
    );

    let config = create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    seed_tokenized_asset(&client, evm.vault_address).await;

    // Grant roles
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Perform mint flow
    let shares_minted = perform_mint_flow(&client, &evm, user_wallet).await?;
    mint_callback_mock.assert();

    // Setup user provider for transfer
    let user_wallet_instance = EthereumWallet::from(user_signer.clone());
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Initiate redemption by transferring tokens to bot wallet
    vault
        .transfer(bot_wallet, shares_minted)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Wait for the redemption to be detected and Alpaca redeem endpoint to be called
    // The poll mock returns "pending" so it will stay in AlpacaCalled state
    wait_for_mock_hit(&redeem_mock).await?;

    // Verify bot has the shares (redemption in progress)
    let bot_balance = vault.balanceOf(bot_wallet).call().await?;
    assert!(
        bot_balance > U256::ZERO,
        "Bot should have shares during redemption"
    );

    // "Crash" - drop the client and rocket instance
    drop(client);

    // Flip the flag so polls now return "completed"
    poll_should_succeed.store(true, Ordering::SeqCst);

    let config2 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Recovery should pick up the stuck redemption and complete the burn
    wait_for_burn(&vault, bot_wallet).await?;

    // Verify all shares are burned
    assert_eq!(
        vault.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "Bot wallet should have no shares after recovery"
    );
    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User wallet should have no shares after recovery"
    );

    Ok(())
}

/// Tests that receipt backfill checkpoint is independent from live monitor discoveries.
///
/// This test verifies the fix for a bug where:
/// 1. Monitor discovers a receipt at block N
/// 2. Service restarts
/// 3. Backfill would incorrectly start from block N (missing blocks 0..N)
///
/// The fix separates `last_backfilled_block` from receipt discovery events.
/// Only `BackfillCheckpoint` events (emitted after backfill completes) advance
/// the checkpoint, not `Discovered` events from the monitor.
///
/// Test scenario:
/// 1. Mint receipt directly before service starts (simulates historic receipt)
/// 2. Start service - backfill discovers the receipt, checkpoints to current block
/// 3. Mint another receipt while service running - monitor detects it
/// 4. Restart service
/// 5. Verify both receipts are usable (backfill didn't skip the early one)
#[tokio::test]
async fn test_backfill_checkpoint_independent_from_monitor_discoveries()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Use file-based database to persist across restarts
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_backfill_checkpoint.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Setup roles before any minting
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Step 1: Mint a receipt BEFORE the service starts (simulates historic receipt)
    let historic_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
    let (historic_receipt_id, _historic_shares) =
        evm.mint_directly(historic_amount, bot_wallet).await?;

    // Setup mocks
    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    // Preseed asset before starting service so backfill can discover receipts
    let pool =
        SqlitePoolOptions::new().max_connections(5).connect(&db_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    preseed_tokenized_asset(&pool, "AAPL", "tAAPL", evm.vault_address).await;
    pool.close().await;

    // Step 2: Start service - backfill should discover the historic receipt
    let config1 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    // Give backfill and monitor time to fully start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Step 3: Mint another receipt while service is running (monitor should detect)
    let live_amount = U256::from(50) * U256::from(10).pow(U256::from(18));
    let (live_receipt_id, _live_shares) =
        evm.mint_directly(live_amount, bot_wallet).await?;

    // Give monitor time to detect the new receipt
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Step 4: "Crash" - drop the first service instance
    drop(client1);

    // Step 5: Restart service - backfill should use checkpoint, not monitor's block
    let config2 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Give backfill time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Step 6: Verify both receipts are tracked by querying all events
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    // Query ALL events for the ReceiptInventory aggregate to debug
    let all_events: Vec<(String, String, String)> = sqlx::query_as(
        r"
        SELECT aggregate_type, event_type, payload
        FROM events
        WHERE aggregate_type = 'ReceiptInventory'
        ORDER BY sequence
        ",
    )
    .fetch_all(&query_pool)
    .await?;

    // Find Discovered events (the event_type might include the full path)
    let discovered_events: Vec<&(String, String, String)> = all_events
        .iter()
        .filter(|(_, event_type, _)| event_type.contains("Discovered"))
        .collect();

    assert!(
        discovered_events.len() >= 2,
        "Expected at least 2 discovered receipts, found {}. \
         All events: {:?}",
        discovered_events.len(),
        all_events.iter().map(|(_, t, _)| t.as_str()).collect::<Vec<_>>()
    );

    // Verify the specific receipt IDs are present
    let receipt_ids: Vec<String> = discovered_events
        .iter()
        .filter_map(|(_, _, payload)| {
            let json: serde_json::Value = serde_json::from_str(payload).ok()?;
            // Handle both possible JSON structures
            json["receipt_id"]
                .as_str()
                .or_else(|| json["Discovered"]["receipt_id"].as_str())
                .map(ToString::to_string)
        })
        .collect();

    let historic_id_hex = format!("{historic_receipt_id:#x}");
    let live_id_hex = format!("{live_receipt_id:#x}");

    assert!(
        receipt_ids.iter().any(|id| id == &historic_id_hex),
        "Historic receipt {historic_id_hex} not found in discovered receipts: {receipt_ids:?}"
    );

    assert!(
        receipt_ids.iter().any(|id| id == &live_id_hex),
        "Live receipt {live_id_hex} not found in discovered receipts: {receipt_ids:?}"
    );

    Ok(())
}

/// Tests that mint recovery works even when the mint view needs reprojection.
///
/// This test reproduces a bug where:
/// 1. Mint reaches JournalConfirmed state
/// 2. Service "crashes"
/// 3. Mint view is deleted (simulating schema migration or corruption)
/// 4. Service restarts, but recovery runs BEFORE view reprojection
/// 5. Recovery queries empty view, misses the stuck mint
///
/// The fix: Reprojections must run BEFORE recovery.
///
/// To create a mint stuck in JournalConfirmed state, we:
/// 1. Complete a mint normally (all the way through)
/// 2. Delete events after JournalConfirmed to roll back the aggregate state
/// 3. Delete the view
/// 4. Restart - recovery should find the JournalConfirmed mint
#[tokio::test]
async fn test_mint_recovery_after_view_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    // Standard Anvil test account #2 (not a secret - these are well-known test keys)
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Use file-based database to persist across restarts
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_view_reprojection.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Setup mocks
    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    let config1 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    seed_tokenized_asset(&client1, evm.vault_address).await;
    setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Setup account and complete a mint
    let link_body = setup_account(&client1, user_wallet).await;
    let issuer_request_id = perform_mint_and_confirm(
        &client1,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-view-test",
        "50.0",
    )
    .await?;

    // Wait for mint to complete
    let user_wallet_instance = EthereumWallet::from(user_signer.clone());
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    wait_for_shares(&vault, user_wallet).await?;

    // "Crash" - drop the service
    drop(client1);

    // Connect to the database to manipulate state
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    // Roll back the mint to JournalConfirmed state by deleting events after sequence 2
    // Event sequence: 1=MintInitiated, 2=JournalConfirmed, 3+=MintingStarted, TokensMinted, etc.
    // The aggregate_id is just the issuer_request_id string
    let aggregate_id = issuer_request_id.clone();
    sqlx::query!(
        r#"
        DELETE FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND sequence > 2
        "#,
        aggregate_id
    )
    .execute(&query_pool)
    .await?;

    // Verify we have exactly 2 events (MintInitiated, JournalConfirmed)
    let event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        aggregate_id
    )
    .fetch_one(&query_pool)
    .await?;
    assert_eq!(
        event_count.count, 2,
        "Expected exactly 2 events (MintInitiated, JournalConfirmed)"
    );

    // Delete the mint view to force reprojection
    sqlx::query!("DELETE FROM mint_view").execute(&query_pool).await?;

    // Verify view is empty
    let view_count =
        sqlx::query!(r#"SELECT COUNT(*) as "count!: i32" FROM mint_view"#)
            .fetch_one(&query_pool)
            .await?;
    assert_eq!(view_count.count, 0, "Mint view should be empty after deletion");

    // Close the pool before restarting
    query_pool.close().await;

    // Restart service - recovery should find the JournalConfirmed mint
    // BUG: Currently recovery runs BEFORE view reprojection, so it queries
    // an empty view and misses the stuck mint.
    let config2 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // If recovery worked, the mint should complete and user gets shares
    // Note: The on-chain mint already happened, but we deleted the events,
    // so recovery will attempt to mint again. This might fail due to
    // duplicate mint, but that's a separate issue (Task 7-9 handles this).
    // For now, we just want to verify recovery FINDS the stuck mint.

    // Give recovery time to run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check if the mint progressed past JournalConfirmed
    let query_pool2 =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let final_event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        aggregate_id
    )
    .fetch_one(&query_pool2)
    .await?;

    // If recovery worked, there should be more than 2 events
    // (at minimum MintingStarted would be added)
    assert!(
        final_event_count.count > 2,
        "Recovery should have processed the JournalConfirmed mint. \
         Expected >2 events, found {}. This indicates recovery ran before \
         view reprojection and missed the stuck mint.",
        final_event_count.count
    );

    Ok(())
}

/// Tests recovery from `Minting` state when the on-chain mint already succeeded.
///
/// Scenario:
/// 1. Complete a mint normally (creates on-chain receipt)
/// 2. Roll back event store to `Minting` state (delete TokensMinted and later events)
/// 3. Delete view
/// 4. Restart service
/// 5. Recovery should detect the receipt exists and record mint success (NOT re-mint)
///
/// This test should FAIL until mint recovery for `Minting` state is implemented.
#[tokio::test]
async fn test_mint_recovery_from_minting_state_when_receipt_exists()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path =
        temp_dir.path().join("test_minting_recovery_receipt_exists.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    let config1 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    seed_tokenized_asset(&client1, evm.vault_address).await;
    setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = setup_account(&client1, user_wallet).await;
    let issuer_request_id = perform_mint_and_confirm(
        &client1,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-minting-recovery-1",
        "50.0",
    )
    .await?;

    let user_wallet_instance = EthereumWallet::from(user_signer.clone());
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares_before = wait_for_shares(&vault, user_wallet).await?;

    drop(client1);

    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    // Roll back to Minting state: keep events 1-3 (Initiated, JournalConfirmed, MintingStarted)
    let aggregate_id = issuer_request_id.clone();
    sqlx::query!(
        r#"
        DELETE FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND sequence > 3
        "#,
        aggregate_id
    )
    .execute(&query_pool)
    .await?;

    let event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        aggregate_id
    )
    .fetch_one(&query_pool)
    .await?;
    assert_eq!(
        event_count.count, 3,
        "Expected exactly 3 events (Initiated, JournalConfirmed, MintingStarted)"
    );

    sqlx::query!("DELETE FROM mint_view").execute(&query_pool).await?;
    query_pool.close().await;

    let config2 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let query_pool2 =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let final_event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        aggregate_id
    )
    .fetch_one(&query_pool2)
    .await?;

    // Recovery should have added TokensMinted event (recognizing the existing receipt)
    // and possibly MintCompleted after callback
    assert!(
        final_event_count.count > 3,
        "Recovery should have processed the Minting mint and added events. \
         Expected >3 events, found {}. Recovery for Minting state not implemented.",
        final_event_count.count
    );

    // Verify the user still has the same shares (no double mint)
    let shares_after = vault.balanceOf(user_wallet).call().await?;
    assert_eq!(
        shares_after, shares_before,
        "User should have same shares (no double mint)"
    );

    Ok(())
}

/// Inserts a mint event into the event store for testing.
async fn insert_mint_event(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    aggregate_id: &str,
    sequence: i32,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload_str = payload.to_string();
    sqlx::query!(
        r#"
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES ('Mint', ?, ?, ?, '1.0', ?, '{}')
        "#,
        aggregate_id,
        sequence,
        event_type,
        payload_str
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Inserts events to create a mint in Minting state
// (events 1-3: Initiated, JournalConfirmed, MintingStarted).
async fn insert_minting_state_events(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    issuer_request_id: &str,
    tokenization_request_id: &str,
    client_id: &str,
    wallet: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = chrono::Utc::now();

    let initiated_payload = json!({
        "Initiated": {
            "issuer_request_id": issuer_request_id,
            "tokenization_request_id": tokenization_request_id,
            "quantity": "50.0",
            "underlying": "AAPL",
            "token": "tAAPL",
            "network": "base",
            "client_id": client_id,
            "wallet": wallet,
            "initiated_at": now.to_rfc3339()
        }
    });
    insert_mint_event(
        pool,
        issuer_request_id,
        1,
        "MintEvent::Initiated",
        &initiated_payload,
    )
    .await?;

    let confirmed_payload = json!({
        "JournalConfirmed": {
            "issuer_request_id": issuer_request_id,
            "confirmed_at": now.to_rfc3339()
        }
    });
    insert_mint_event(
        pool,
        issuer_request_id,
        2,
        "MintEvent::JournalConfirmed",
        &confirmed_payload,
    )
    .await?;

    let started_payload = json!({
        "MintingStarted": {
            "issuer_request_id": issuer_request_id,
            "started_at": now.to_rfc3339()
        }
    });
    insert_mint_event(
        pool,
        issuer_request_id,
        3,
        "MintEvent::MintingStarted",
        &started_payload,
    )
    .await?;

    Ok(())
}

/// Tests recovery from `Minting` state when the on-chain mint did NOT succeed.
///
/// Scenario:
/// 1. Create mint events directly in event store up to `Minting` state
/// 2. No on-chain mint happens (simulates crash before blockchain call)
/// 3. Start service
/// 4. Recovery should retry the mint and complete it
///
/// This test should FAIL until mint recovery for `Minting` state is implemented.
#[tokio::test]
async fn test_mint_recovery_from_minting_state_when_no_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_minting_recovery_no_receipt.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    // First start the service to set up the database schema and seed data
    let config1 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    seed_tokenized_asset(&client1, evm.vault_address).await;
    setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Setup account to get a valid client_id
    let link_body = setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    drop(client1);

    // Now insert events directly to create a mint stuck in Minting state
    // WITHOUT any on-chain transaction
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let issuer_request_id = "iss-minting-no-receipt";
    insert_minting_state_events(
        &query_pool,
        issuer_request_id,
        "tok-minting-no-receipt",
        &client_id.to_string(),
        user_wallet,
    )
    .await?;

    query_pool.close().await;

    // Start service - recovery should find the stuck mint and retry it
    let config2 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Wait for recovery and mint to complete
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Recovery should retry the mint and user should receive shares
    let shares = wait_for_shares(&vault, user_wallet).await;
    assert!(
        shares.is_ok(),
        "Recovery should have retried the mint and user should have shares. \
         Recovery for Minting state (no receipt) not implemented."
    );

    Ok(())
}

/// Tests double-mint prevention when recovery detects an existing receipt.
///
/// Scenario:
/// 1. Manually mint on-chain with a specific issuer_request_id
/// 2. Create mint events directly in event store with matching issuer_request_id
/// 3. Start service
/// 4. Recovery should find the receipt and NOT mint again
///
/// This test should FAIL until mint recovery for `Minting` state is implemented.
#[tokio::test]
async fn test_mint_recovery_prevents_double_mint()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_double_mint_prevention.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Manually mint on-chain with a specific issuer_request_id BEFORE service starts
    let issuer_request_id = "iss-double-mint-test";
    let receipt_info_json = json!({
        "tokenization_request_id": "tok-double-mint-test",
        "issuer_request_id": issuer_request_id,
        "underlying": "AAPL",
        "quantity": "50.0",
        "operation_type": "Mint",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "notes": null
    });
    let encoded_receipt_info =
        Bytes::from(serde_json::to_vec(&receipt_info_json)?);

    let amount = U256::from(50) * U256::from(10).pow(U256::from(18));
    // Mint to bot_wallet (not user_wallet) because in the real ITN flow, the bot
    // calls deposit() and receives the receipt. The backfill only looks for receipts
    // owned by bot_wallet.
    let (_receipt_id, _shares_minted, _) = evm
        .mint_directly_with_info(amount, bot_wallet, encoded_receipt_info)
        .await?;

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    // Start service to set up database
    let config1 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    seed_tokenized_asset(&client1, evm.vault_address).await;
    let link_body = setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    drop(client1);

    // Insert events to create a mint stuck in Minting state with matching issuer_request_id
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    insert_minting_state_events(
        &query_pool,
        issuer_request_id,
        "tok-double-mint-test",
        &client_id.to_string(),
        user_wallet,
    )
    .await?;

    query_pool.close().await;

    // Get initial share balance of bot_wallet (which received shares from the manual mint)
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares_before = vault.balanceOf(bot_wallet).call().await?;

    // Start service - recovery should find the existing receipt and NOT mint again
    let config2 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check events - recovery should have recorded the existing mint
    let query_pool2 =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let final_event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        issuer_request_id
    )
    .fetch_one(&query_pool2)
    .await?;

    assert!(
        final_event_count.count > 3,
        "Recovery should have added events for existing mint. \
         Expected >3 events, found {}. Double-mint prevention not implemented.",
        final_event_count.count
    );

    // Most importantly: bot should have SAME shares (no double mint)
    // If recovery re-minted, the bot's share balance would double.
    let shares_after = vault.balanceOf(bot_wallet).call().await?;
    assert_eq!(
        shares_after, shares_before,
        "Bot should have same shares after recovery (no double mint). \
         Before: {shares_before}, After: {shares_after}"
    );

    Ok(())
}

/// Tests recovery from `MintingFailed` state.
///
/// Scenario:
/// 1. Create mint events directly in event store up to `MintingFailed` state
/// 2. Start service
/// 3. Recovery should retry the mint and complete it
///
/// This test should FAIL until mint recovery for `MintingFailed` state is implemented.
#[tokio::test]
async fn test_mint_recovery_from_minting_failed_state()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_minting_failed_recovery.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        setup_redemption_mocks(&mock_alpaca, user_wallet);

    // Start service to set up database schema
    let config1 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    seed_tokenized_asset(&client1, evm.vault_address).await;
    setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    drop(client1);

    // Insert events to create a mint in MintingFailed state
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let issuer_request_id = "iss-minting-failed-recovery";

    insert_minting_state_events(
        &query_pool,
        issuer_request_id,
        "tok-minting-failed",
        &client_id.to_string(),
        user_wallet,
    )
    .await?;

    // Event 4: MintingFailed
    let failed_payload = json!({
        "MintingFailed": {
            "issuer_request_id": issuer_request_id,
            "error": "Simulated blockchain error",
            "failed_at": chrono::Utc::now().to_rfc3339()
        }
    });
    insert_mint_event(
        &query_pool,
        issuer_request_id,
        4,
        "MintEvent::MintingFailed",
        &failed_payload,
    )
    .await?;

    query_pool.close().await;

    // Start service - recovery should find the failed mint and retry it
    let config2 = create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Wait for recovery and mint to complete
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Recovery should retry the mint and user should receive shares
    let shares = wait_for_shares(&vault, user_wallet).await;
    assert!(
        shares.is_ok(),
        "Recovery should have retried the failed mint and user should have shares. \
         Recovery for MintingFailed state not implemented."
    );

    Ok(())
}

/// Seeds a tokenized asset directly in the database BEFORE rocket starts.
/// This is used to test startup behavior with pre-configured assets.
async fn preseed_tokenized_asset(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    underlying: &str,
    token: &str,
    vault: Address,
) {
    let view_id = underlying;
    let payload = json!({
        "Asset": {
            "underlying": underlying,
            "token": token,
            "network": "base",
            "vault": vault,
            "enabled": true,
            "added_at": "2024-01-01T00:00:00Z"
        }
    });

    sqlx::query!(
        r#"
        INSERT INTO tokenized_asset_view (view_id, version, payload)
        VALUES (?, 1, ?)
        "#,
        view_id,
        payload
    )
    .execute(pool)
    .await
    .unwrap();
}

/// Tests that receipt backfill discovers receipts from ALL enabled tokenized assets,
/// not just the single vault in Config.
///
/// This test verifies multi-vault support:
/// - Each tokenized asset has its own vault contract
/// - Backfill processes all enabled tokenized assets
/// - Receipts minted on any vault are discovered
#[tokio::test]
async fn test_multi_vault_backfill_discovers_receipts_from_all_assets()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;

    // Deploy a SECOND vault for TSLA (first vault is AAPL)
    let (tsla_vault, tsla_authorizer) = evm.deploy_additional_vault().await?;

    // Grant roles on TSLA vault to bot wallet
    evm.grant_role_on_authorizer(tsla_authorizer, "DEPOSIT", bot_wallet)
        .await?;
    evm.grant_role_on_authorizer(tsla_authorizer, "CERTIFY", bot_wallet)
        .await?;
    evm.certify_specific_vault(tsla_vault, U256::MAX).await?;

    // Mint directly on TSLA vault BEFORE service starts
    // This simulates a receipt that exists on-chain but the service doesn't know about
    let mint_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
    let (receipt_id, _shares) =
        evm.mint_directly_on_vault(tsla_vault, mint_amount, bot_wallet).await?;

    // Create a temp file database so we can preseed assets before rocket starts
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    let database_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Create and migrate the database, then preseed assets
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    // Preseed BOTH tokenized assets BEFORE rocket starts
    // This simulates production where assets are configured before the service runs
    preseed_tokenized_asset(&pool, "AAPL", "tAAPL", evm.vault_address).await;
    preseed_tokenized_asset(&pool, "TSLA", "tTSLA", tsla_vault).await;

    // Close the pool so rocket can open it
    pool.close().await;

    let config = Config {
        database_url,
        database_max_connections: 5,
        rpc_url: Url::parse(&evm.endpoint)?,
        chain_id: st0x_issuance::ANVIL_CHAIN_ID,
        signer: st0x_issuance::SignerConfig::Local(evm.private_key),
        vault: evm.vault_address,
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
        log_level: st0x_issuance::LogLevel::Debug,
        hyperdx: None,
        alpaca: AlpacaConfig {
            api_base_url: mock_alpaca.base_url(),
            account_id: "test-account".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        },
    };

    // Start rocket - backfill should run and discover the TSLA receipt
    initialize_rocket(config).await?;

    // Give backfill time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Reconnect to verify receipt was discovered
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite:{}", db_path.display()))
        .await?;

    // Query events table to verify the TSLA receipt was discovered by backfill.
    // Backfill stores ReceiptInventoryEvent::Discovered events in the events table.
    let tsla_vault_str = tsla_vault.to_string();
    let receipt_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count: i64"
        FROM events
        WHERE aggregate_type = 'ReceiptInventory'
          AND aggregate_id = ?
          AND event_type = 'ReceiptInventoryEvent::Discovered'
        "#,
        tsla_vault_str
    )
    .fetch_one(&pool)
    .await?;

    assert!(
        receipt_count > 0,
        "Receipt {receipt_id} from TSLA vault ({tsla_vault}) should have been discovered by backfill. \
         Found {receipt_count} Discovered events for vault."
    );

    Ok(())
}
