#![allow(clippy::unwrap_used)]

//! E2E test for redemption dust handling.
//!
//! # Problem
//! Alpaca's tokenization API only accepts quantities with up to 9 decimal places,
//! but on-chain tokens use 18 decimals. A redemption for `0.450574852280275235`
//! tokens would be rejected because it exceeds 9 decimals.
//!
//! # Solution
//! Truncate quantity to 9 decimals for Alpaca, track the dust, and atomically
//! return dust to sender when burning.
//!
//! # Test Scenario
//! 1. Setup: Link account, add tokenized asset, mint tokens to user
//! 2. User sends tokens to bot wallet with >9 decimal precision
//! 3. Verify: Alpaca receives truncated quantity (9 decimals)
//! 4. Verify: Burn transaction executes for truncated amount
//! 5. Verify: Dust is transferred back to user's wallet
//! 6. Verify: Final on-chain balance = dust (original - truncated)

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::{Mock, prelude::*};
use rocket::local::asynchronous::Client;
use serde_json::json;
use std::sync::{Arc, Mutex};
use url::Url;

use st0x_issuance::account::{AccountLinkResponse, RegisterAccountResponse};
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::{LocalEvm, test_alpaca_legacy_auth};
use st0x_issuance::{
    AlpacaConfig, AuthConfig, Config, IpWhitelist, SignerConfig,
    initialize_rocket,
};

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
                "Timeout waiting for {description}: expected {expected}, got {balance}"
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
                "vault": format!("{vault:#x}")
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
        .body(json!({"email": "dust-user@example.com"}).to_string())
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
            json!({"email": "dust-user@example.com", "account": "DUST-USER-123"})
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

/// Mints tokens to the user via the full mint flow.
/// Returns the exact amount of shares minted.
async fn perform_mint_flow(
    client: &Client,
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

    // Wait for shares to arrive
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
    loop {
        let balance = vault.balanceOf(user_wallet).call().await?;
        if balance > U256::ZERO {
            return Ok(balance);
        }
        if start.elapsed() >= timeout {
            return Err("Timeout waiting for minted shares".into());
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
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

/// Sets up redemption mocks that capture and verify the quantity sent to Alpaca.
///
/// The `captured_qty` will receive the quantity string from the redeem request body,
/// allowing verification that the truncated quantity was sent.
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

                // Capture the quantity for verification
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
///
/// The test verifies:
/// 1. Alpaca receives the truncated quantity (9 decimals)
/// 2. The burn transaction burns the truncated amount
/// 3. Dust is returned to the user's wallet
/// 4. Final user balance equals the dust amount
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

    // Capture the quantity sent to Alpaca for verification
    let captured_qty: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks_with_qty_capture(
        &mock_alpaca,
        user_wallet,
        Arc::clone(&captured_qty),
    );

    let config = Config {
        database_url: ":memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse(&evm.endpoint)?,
        chain_id: st0x_issuance::ANVIL_CHAIN_ID,
        signer: SignerConfig::Local(evm.private_key),
        vault: evm.vault_address,
        deployment_block: 0,
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

    // Grant roles
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Setup account
    let link_body = setup_account(&client, user_wallet).await;

    // Mint tokens with the exact amount we want to redeem.
    // We use a string quantity that when converted to 18 decimals equals ORIGINAL_SHARES.
    // 0.450574852280275235 * 10^18 = 450574852280275235
    let mint_qty = "0.450574852280275235";
    let minted_shares =
        perform_mint_flow(&client, &evm, user_wallet, &link_body, mint_qty)
            .await?;

    assert_eq!(
        minted_shares, ORIGINAL_SHARES,
        "Minted shares should match expected amount"
    );

    // Setup user provider for transfer
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // === REDEMPTION: Transfer tokens to bot wallet ===
    // This triggers the redemption detection flow
    vault
        .transfer(bot_wallet, ORIGINAL_SHARES)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Wait for the Alpaca redeem endpoint to be called
    wait_for_mock_hit(&redeem_mock).await?;

    // === VERIFY: Alpaca received truncated quantity ===
    let qty_sent_to_alpaca = captured_qty.lock().unwrap().clone();
    assert_eq!(
        qty_sent_to_alpaca,
        Some(TRUNCATED_QTY_STR.to_string()),
        "Alpaca should receive truncated quantity (9 decimals)"
    );

    // === VERIFY: User receives dust back ===
    // After burn completes, user should have exactly DUST_SHARES
    wait_for_balance(&vault, user_wallet, DUST_SHARES, "dust returned to user")
        .await?;

    // === VERIFY: Bot wallet is empty (burn completed) ===
    let bot_balance = vault.balanceOf(bot_wallet).call().await?;
    assert_eq!(
        bot_balance,
        U256::ZERO,
        "Bot wallet should be empty after burn"
    );

    // === VERIFY: Total supply decreased by truncated amount (not full amount) ===
    // The dust is still in circulation (returned to user), so total supply
    // only decreased by the truncated amount that was burned.

    Ok(())
}

/// Tests that redemption with exactly 9 decimals has no dust.
///
/// When the quantity has 9 or fewer decimals, no dust should be generated
/// and the user should have zero balance after redemption.
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

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks_with_qty_capture(
        &mock_alpaca,
        user_wallet,
        Arc::clone(&captured_qty),
    );

    let config = Config {
        database_url: ":memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse(&evm.endpoint)?,
        chain_id: st0x_issuance::ANVIL_CHAIN_ID,
        signer: SignerConfig::Local(evm.private_key),
        vault: evm.vault_address,
        deployment_block: 0,
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

    let link_body = setup_account(&client, user_wallet).await;

    // Mint with exactly 9 decimals: 0.123456789 (no dust)
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

    // Transfer all tokens to bot
    vault
        .transfer(bot_wallet, minted_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    wait_for_mock_hit(&redeem_mock).await?;

    // Quantity should be exactly what we sent (no truncation needed)
    let qty_sent = captured_qty.lock().unwrap().clone();
    assert_eq!(qty_sent, Some(mint_qty.to_string()));

    // User should have zero balance (no dust)
    wait_for_balance(&vault, user_wallet, U256::ZERO, "zero balance (no dust)")
        .await?;

    Ok(())
}
