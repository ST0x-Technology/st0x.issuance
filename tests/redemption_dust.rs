#![allow(clippy::unwrap_used)]

//! Tests for redemption dust handling.
//!
//! # Problem
//! Alpaca's tokenization API only accepts quantities with up to 9 decimal
//! places, but on-chain tokens use 18 decimals. A redemption for
//! `0.450574852280275235` tokens would be rejected because it exceeds
//! 9 decimals.
//!
//! # Solution
//! Truncate quantity to 9 decimals for Alpaca, track the dust, and
//! atomically return dust to sender when burning.

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::Mock;
use httpmock::prelude::*;
use serde_json::json;
use std::sync::{Arc, Mutex};

use st0x_issuance::account::AccountLinkResponse;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::{LocalEvm, test_alpaca_legacy_auth};

/// Quantity with >9 decimal precision (18 decimals on-chain).
/// Represents: 0.450574852280275235 shares
/// - Original (18 dec): 450574852280275235
/// - Truncated (9 dec): 450574852000000000 (0.450574852)
/// - Dust: 280275235
const ORIGINAL_SHARES: U256 =
    U256::from_limbs([450_574_852_280_275_235, 0, 0, 0]);
const DUST_SHARES: U256 = U256::from_limbs([280_275_235, 0, 0, 0]);

/// Truncated quantity string for Alpaca (9 decimals): "0.450574852"
const TRUNCATED_QTY_STR: &str = "0.450574852";

async fn wait_for_balance<T>(
    vault: &OffchainAssetReceiptVaultInstance<T>,
    wallet: Address,
    expected: U256,
    description: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: alloy::providers::Provider,
{
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(10);
    let poll_interval = tokio::time::Duration::from_millis(100);

    loop {
        let balance = vault.balanceOf(wallet).call().await?;
        if balance == expected {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for {description}: \
                 expected {expected}, got {balance}"
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Mints tokens to the user via the full mint flow.
/// Returns the exact amount of shares minted.
async fn perform_mint_flow(
    client: &rocket::local::asynchronous::Client,
    evm: &LocalEvm,
    user_wallet: Address,
    link_body: &AccountLinkResponse,
    mint_qty: &str,
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
            json!({
                "tokenization_request_id": "alp-mint-dust",
                "qty": mint_qty,
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
                "tokenization_request_id": "alp-mint-dust",
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

/// Sets up redemption mocks that capture the quantity sent to Alpaca.
fn setup_redemption_mocks_with_qty_capture(
    mock_alpaca: &MockServer,
    user_wallet: Address,
    captured_qty: Arc<Mutex<Option<String>>>,
) -> (Mock, Mock) {
    let (basic_auth, api_key, api_secret) = test_alpaca_legacy_auth();
    let shared_issuer_id = Arc::new(Mutex::new(String::new()));
    let shared_tx_hash = Arc::new(Mutex::new(String::new()));

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
                let issuer_request_id =
                    body["issuer_request_id"].as_str().unwrap().to_string();
                let tx_hash = body["tx_hash"].as_str().unwrap().to_string();
                let qty = body["qty"].as_str().unwrap().to_string();

                *captured_qty.lock().unwrap() = Some(qty.clone());

                shared_issuer_id_clone
                    .lock()
                    .unwrap()
                    .clone_from(&issuer_request_id);
                shared_tx_hash_clone.lock().unwrap().clone_from(&tx_hash);

                let response_body = serde_json::to_string(&json!({
                    "tokenization_request_id": "tok-redeem-dust",
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
                    "fees": "0.001"
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
                    shared_issuer_id.lock().unwrap().clone();
                let tx_hash = shared_tx_hash.lock().unwrap().clone();

                let response_body = serde_json::to_string(&json!([{
                    "tokenization_request_id": "tok-redeem-dust",
                    "issuer_request_id": issuer_request_id,
                    "created_at": "2025-09-12T17:28:48.642437-04:00",
                    "type": "redeem",
                    "status": "completed",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": TRUNCATED_QTY_STR,
                    "issuer": "test-issuer",
                    "network": "base",
                    "wallet_address": user_wallet,
                    "tx_hash": tx_hash,
                    "fees": "0.001"
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

/// Tests that redemption with >9 decimal precision handles dust correctly.
#[tokio::test]
async fn test_redemption_returns_dust_to_user()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let captured_qty: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks_with_qty_capture(
        &mock_alpaca,
        user_wallet,
        Arc::clone(&captured_qty),
    );

    let config =
        harness::create_config_with_db(":memory:", &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::seed_tokenized_asset(&client, evm.vault_address).await;

    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    // 0.450574852280275235 * 10^18 = 450574852280275235
    let mint_qty = "0.450574852280275235";
    let minted_shares =
        perform_mint_flow(&client, &evm, user_wallet, &link_body, mint_qty)
            .await?;

    assert_eq!(
        minted_shares, ORIGINAL_SHARES,
        "Minted shares should match expected amount"
    );

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
        .transfer(bot_wallet, ORIGINAL_SHARES)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_mock_hit(&redeem_mock).await?;

    let qty_sent_to_alpaca = captured_qty.lock().unwrap().clone();
    assert_eq!(
        qty_sent_to_alpaca,
        Some(TRUNCATED_QTY_STR.to_string()),
        "Alpaca should receive truncated quantity (9 decimals)"
    );

    wait_for_balance(&vault, user_wallet, DUST_SHARES, "dust returned to user")
        .await?;

    let bot_balance = vault.balanceOf(bot_wallet).call().await?;
    assert_eq!(
        bot_balance,
        U256::ZERO,
        "Bot wallet should be empty after burn"
    );

    Ok(())
}

/// Tests that redemption with exactly 9 decimals has no dust.
#[tokio::test]
async fn test_redemption_no_dust_when_9_decimals()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let captured_qty: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks_with_qty_capture(
        &mock_alpaca,
        user_wallet,
        Arc::clone(&captured_qty),
    );

    let config =
        harness::create_config_with_db(":memory:", &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::seed_tokenized_asset(&client, evm.vault_address).await;

    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    let mint_qty = "0.123456789";
    let minted_shares =
        perform_mint_flow(&client, &evm, user_wallet, &link_body, mint_qty)
            .await?;

    // 0.123456789 * 10^18 = 123456789000000000
    let expected_shares = U256::from(123_456_789_000_000_000_u64);
    assert_eq!(minted_shares, expected_shares);

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
        .transfer(bot_wallet, minted_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_mock_hit(&redeem_mock).await?;

    let qty_sent = captured_qty.lock().unwrap().clone();
    assert_eq!(qty_sent, Some(mint_qty.to_string()));

    wait_for_balance(&vault, user_wallet, U256::ZERO, "zero balance (no dust)")
        .await?;

    Ok(())
}
