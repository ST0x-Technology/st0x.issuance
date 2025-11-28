#![allow(clippy::unwrap_used)]

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::{Mock, prelude::*};
use rocket::local::asynchronous::Client;
use serde_json::json;
use std::sync::{Arc, Mutex};
use url::Url;

use st0x_issuance::account::AccountLinkResponse;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::{LocalEvm, test_alpaca_auth_header};
use st0x_issuance::{AlpacaConfig, Config, initialize_rocket};

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

async fn perform_mint_flow(
    client: &Client,
    evm: &LocalEvm,
    user_wallet: Address,
) -> Result<U256, Box<dyn std::error::Error>> {
    let link_response = client
        .post("/accounts/connect")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .header(rocket::http::Header::new("X-Real-IP", "127.0.0.1"))
        .body(
            json!({
                "email": "user@example.com",
                "account": "USER123"
            })
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
        .body(
            json!({
                "wallet": user_wallet
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(whitelist_response.status(), rocket::http::Status::Ok);

    let mint_response = client
        .post("/inkind/issuance")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .header(rocket::http::Header::new("X-Real-IP", "127.0.0.1"))
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
        .header(rocket::http::Header::new("X-Real-IP", "127.0.0.1"))
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
    let test_auth = test_alpaca_auth_header();

    mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/callback/mint")
            .header("authorization", &test_auth);
        then.status(200).body("");
    })
}

fn setup_redemption_mocks(
    mock_alpaca: &MockServer,
    user_wallet: Address,
) -> (Mock, Mock) {
    let test_auth = test_alpaca_auth_header();
    let shared_issuer_id = Arc::new(Mutex::new(String::new()));
    let shared_tx_hash = Arc::new(Mutex::new(String::new()));

    let shared_issuer_id_clone = Arc::clone(&shared_issuer_id);
    let shared_tx_hash_clone = Arc::clone(&shared_tx_hash);

    let redeem_mock = mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/redeem")
            .header("authorization", &test_auth);

        then.status(200).respond_with(
            move |req: &httpmock::HttpMockRequest| {
                let body: serde_json::Value =
                    serde_json::from_slice(req.body().as_ref()).unwrap();
                let issuer_request_id =
                    body["issuer_request_id"].as_str().unwrap().to_string();
                let tx_hash = body["tx_hash"].as_str().unwrap().to_string();

                shared_issuer_id_clone
                    .lock()
                    .unwrap()
                    .clone_from(&issuer_request_id);
                shared_tx_hash_clone.lock().unwrap().clone_from(&tx_hash);

                let response_body = serde_json::to_string(&json!({
                    "tokenization_request_id": "tok-redeem-123",
                    "issuer_request_id": issuer_request_id,
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
            .header("authorization", &test_auth);

        then.status(200).respond_with(
            move |_req: &httpmock::HttpMockRequest| {
                let issuer_request_id =
                    shared_issuer_id.lock().unwrap().clone();
                let tx_hash = shared_tx_hash.lock().unwrap().clone();

                let response_body = serde_json::to_string(&json!({
                    "requests": [{
                        "tokenization_request_id": "tok-redeem-123",
                        "issuer_request_id": issuer_request_id,
                        "created_at": "2025-09-12T17:28:48.642437-04:00",
                        "type": "redeem",
                        "status": "completed",
                        "underlying_symbol": "AAPL",
                        "token_symbol": "tAAPL",
                        "qty": "50",
                        "issuer": "test-issuer",
                        "network": "base",
                        "wallet_address": user_wallet,
                        "tx_hash": tx_hash,
                        "fees": "0.5"
                    }]
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
        private_key: evm.private_key,
        vault: evm.vault_address,
        bot: bot_wallet,
        issuer_api_key: "test-key-12345678901234567890123456".to_string(),
        alpaca_ip_ranges: vec!["127.0.0.1/32".parse().expect("Valid IP range")],
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
