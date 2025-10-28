use alloy::primitives::{Address, U256, address};
use alloy::providers::ProviderBuilder;
use httpmock::prelude::*;
use rocket::http::{ContentType, Status};
use rocket::local::asynchronous::Client;
use serde_json::json;
use url::Url;

use st0x_issuance::account::AccountLinkResponse;
use st0x_issuance::bindings::OffchainAssetReceiptVault;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::{LocalEvm, test_alpaca_auth_header};
use st0x_issuance::{AlpacaConfig, Config};

async fn link_account(
    client: &Client,
    wallet_address: Address,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = client
        .post("/accounts/connect")
        .header(ContentType::JSON)
        .body(
            json!({
                "email": "test@example.com",
                "account": "ALPACA123",
                "wallet": wallet_address.to_string()
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Ok);
    let body: AccountLinkResponse =
        response.into_json().await.ok_or("Failed to parse JSON")?;
    Ok(body.client_id.0)
}

async fn initiate_mint(
    client: &Client,
    client_id: &str,
    wallet_address: Address,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = client
        .post("/inkind/issuance")
        .header(ContentType::JSON)
        .body(
            json!({
                "tokenization_request_id": "alp-test-456",
                "qty": "100.0",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "network": "base",
                "client_id": client_id,
                "wallet_address": wallet_address.to_string()
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Ok);
    let body: MintResponse =
        response.into_json().await.ok_or("Failed to parse JSON")?;
    Ok(body.issuer_request_id.0)
}

async fn confirm_mint(
    client: &Client,
    issuer_request_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .post("/inkind/issuance/confirm")
        .header(ContentType::JSON)
        .body(
            json!({
                "tokenization_request_id": "alp-test-456",
                "issuer_request_id": issuer_request_id,
                "status": "completed"
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Ok);
    Ok(())
}

#[tokio::test]
async fn test_complete_mint_flow_with_anvil()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let test_auth = test_alpaca_auth_header();

    let callback_mock = mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/callback/mint")
            .header("authorization", &test_auth);
        then.status(200).body("");
    });

    let config = Config {
        database_url: ":memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Some(Url::parse(&evm.endpoint)?),
        private_key: Some(evm.private_key_hex()),
        vault_address: Some(evm.vault_address),
        redemption_wallet: None,
        alpaca: AlpacaConfig {
            api_base_url: mock_alpaca.base_url(),
            account_id: "test-account".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        },
    };

    let wallet_address = address!("0x1234567890abcdef1234567890abcdef12345678");

    evm.grant_deposit_role(wallet_address).await?;

    let rocket = st0x_issuance::initialize_rocket(config).await?;
    let client = Client::tracked(rocket).await?;

    let client_id = link_account(&client, wallet_address).await?;
    let issuer_request_id =
        initiate_mint(&client, &client_id, wallet_address).await?;
    confirm_mint(&client, &issuer_request_id).await?;

    let provider = ProviderBuilder::new().connect(&evm.endpoint).await?;
    let vault = OffchainAssetReceiptVault::new(evm.vault_address, &provider);

    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
    let poll_interval = tokio::time::Duration::from_millis(100);

    let shares_balance = loop {
        let balance = vault.balanceOf(wallet_address).call().await?;
        if balance > U256::ZERO {
            break balance;
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for non-zero balance after {}s",
                timeout.as_secs()
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    };

    callback_mock.assert();

    assert!(
        shares_balance > U256::ZERO,
        "Expected shares to be minted on-chain, but balance is zero"
    );

    Ok(())
}
