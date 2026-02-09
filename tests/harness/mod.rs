#![allow(dead_code)]

mod alpaca_mocks;

pub use alpaca_mocks::*;

use alloy::primitives::{Address, U256};
use httpmock::Mock;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use url::Url;

use st0x_issuance::account::{AccountLinkResponse, RegisterAccountResponse};
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{AlpacaConfig, AuthConfig, Config, IpWhitelist};

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

pub async fn seed_tokenized_asset(client: &Client, vault: Address) {
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

pub fn create_config_with_db(
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

pub async fn perform_mint_and_confirm(
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
