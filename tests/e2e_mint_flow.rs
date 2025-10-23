use rocket::http::{ContentType, Status};
use rocket::local::asynchronous::Client;
use serde_json::json;

use st0x_issuance::account::AccountLinkResponse;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::setup_test_rocket;

#[tokio::test]
async fn test_complete_mint_flow_happy_path()
-> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Set up test environment with in-memory database and mock services
    let test_env = setup_test_rocket().await;
    let client = Client::tracked(test_env.rocket).await?;

    // Step 2: Create account via /accounts/connect
    let account_response = client
        .post("/accounts/connect")
        .header(ContentType::JSON)
        .body(
            json!({
                "email": "test@example.com",
                "account": "ALPACA123"
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(account_response.status(), Status::Ok);

    let account_body: AccountLinkResponse = account_response
        .into_json()
        .await
        .ok_or("Failed to parse JSON response")?;
    let client_id = &account_body.client_id.0;

    // Step 3: Initiate mint via /inkind/issuance
    // (assets are already seeded in setup_test_rocket)
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
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(mint_response.status(), Status::Ok);

    let mint_body: MintResponse = mint_response
        .into_json()
        .await
        .ok_or("Failed to parse JSON response")?;
    let issuer_request_id = &mint_body.issuer_request_id.0;

    // Step 4: Confirm journal via /inkind/issuance/confirm
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

    // Step 5: Wait for async processing to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Step 6: Verify mock services were called
    assert_eq!(
        test_env.blockchain_service.get_call_count(),
        1,
        "BlockchainService should be called exactly once"
    );
    assert_eq!(
        test_env.alpaca_service.get_call_count(),
        1,
        "AlpacaService should be called exactly once"
    );

    Ok(())
}
