#![allow(clippy::unwrap_used)]

mod harness;

use alloy::primitives::address;
use httpmock::prelude::*;

use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

#[tokio::test]
async fn test_vault_upgrade_via_add_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let config =
        harness::create_config_with_db(":memory:", &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let vault_a = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let vault_b = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    // Seed asset with vault A
    harness::seed_tokenized_asset_with(&client, vault_a, "AAPL", "tAAPL").await;

    // Re-post with vault B (same underlying, different vault)
    harness::seed_tokenized_asset_with(&client, vault_b, "AAPL", "tAAPL").await;

    // Query the internal detail endpoint to verify the vault was updated
    let response = client
        .get("/tokenized-assets/AAPL")
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .dispatch()
        .await;

    assert_eq!(
        response.status(),
        rocket::http::Status::Ok,
        "Internal asset detail endpoint should return 200"
    );

    let body: serde_json::Value = response.into_json().await.unwrap();
    let vault_in_response = body["vault"].as_str().unwrap();

    assert_eq!(
        vault_in_response,
        format!("{vault_b:#x}"),
        "Vault should be updated to vault B after re-posting with different vault"
    );

    Ok(())
}
