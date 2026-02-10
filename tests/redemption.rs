#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, TxHash, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::Mock;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use st0x_issuance::account::AccountLinkResponse;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::mint::{IssuerRequestId, MintResponse};
use st0x_issuance::test_utils::{LocalEvm, test_alpaca_legacy_auth};

async fn perform_mint_flow(
    client: &Client,
    evm: &LocalEvm,
    user_wallet: Address,
    link_body: &AccountLinkResponse,
) -> Result<U256, Box<dyn std::error::Error>> {
    let mint_response = client
        .post("/inkind/issuance")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(
            serde_json::json!({
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
            serde_json::json!({
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

    harness::wait_for_shares(&vault, user_wallet).await
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
                    body["issuer_request_id"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap(),
                );
                let tx_hash: TxHash =
                    body["tx_hash"].as_str().unwrap().parse().unwrap();

                *shared_issuer_id_clone.lock().unwrap() =
                    Some(issuer_request_id.clone());
                *shared_tx_hash_clone.lock().unwrap() = Some(tx_hash);

                let response_body = serde_json::to_string(&serde_json::json!({
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

                let response_body =
                    serde_json::to_string(&serde_json::json!([{
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

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca, user_wallet);

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::seed_tokenized_asset(&client, evm.vault_address).await;

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
    let link_body = harness::setup_account(&client, user_wallet).await;
    let issuer_request_id = harness::perform_mint_and_confirm(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-inventory",
        "50.0",
    )
    .await?;

    // Wait for shares to be minted
    let shares_minted = harness::wait_for_shares(&vault, user_wallet).await?;
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
    harness::wait_for_burn(&vault, bot_wallet).await?;

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
    let mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks_with_shared_state(
        &mock_alpaca,
        user_wallet,
        Arc::clone(&shared_issuer_id),
        Arc::clone(&shared_tx_hash),
        Arc::clone(&poll_should_succeed),
    );

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::seed_tokenized_asset(&client, evm.vault_address).await;

    // Grant roles
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Perform mint flow
    let link_body = harness::setup_account(&client, user_wallet).await;
    let shares_minted =
        perform_mint_flow(&client, &evm, user_wallet, &link_body).await?;
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
    harness::wait_for_mock_hit(&redeem_mock).await?;

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

    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Recovery should pick up the stuck redemption and complete the burn
    harness::wait_for_burn(&vault, bot_wallet).await?;

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
