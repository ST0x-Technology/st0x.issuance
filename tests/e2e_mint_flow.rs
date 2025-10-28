use alloy::primitives::address;
use httpmock::prelude::*;
use rocket::http::{ContentType, Status};
use rocket::local::asynchronous::Client;
use serde_json::json;
use url::Url;

use st0x_issuance::account::AccountLinkResponse;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{AlpacaConfig, Config};

#[tokio::test]
async fn test_complete_mint_flow_with_anvil()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let callback_mock = mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/callback/mint")
            .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=")
            .json_body(json!({
                "tokenization_request_id": "alp-test-456"
            }));
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

    let account_response = client
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

    assert_eq!(account_response.status(), Status::Ok);

    let account_body: AccountLinkResponse =
        account_response.into_json().await.ok_or("Failed to parse JSON")?;
    let client_id = &account_body.client_id.0;

    let mint_response = client
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

    assert_eq!(mint_response.status(), Status::Ok);

    let mint_body: MintResponse =
        mint_response.into_json().await.ok_or("Failed to parse JSON")?;
    let issuer_request_id = &mint_body.issuer_request_id.0;

    let confirm_response = client
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

    assert_eq!(confirm_response.status(), Status::Ok);

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    callback_mock.assert();

    let provider =
        alloy::providers::ProviderBuilder::new().connect(&evm.endpoint).await?;

    let vault = st0x_issuance::bindings::OffchainAssetReceiptVault::new(
        evm.vault_address,
        &provider,
    );

    let shares_balance = vault.balanceOf(wallet_address).call().await?;

    assert!(
        shares_balance > alloy::primitives::U256::ZERO,
        "Expected shares to be minted on-chain, but balance is zero"
    );

    Ok(())
}
