#![allow(clippy::unwrap_used)]

mod harness;

use httpmock::MockServer;

use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::tokenized_asset::{TokenizedAssetStatus, UnderlyingSymbol};
use st0x_issuance_client::IssuanceClient;

const UNDERLYING: &str = "TSLA";
const TOKEN: &str = "tTSLA";
const FROZEN_UNDERLYING: &str = "NVDA";
const FROZEN_TOKEN: &str = "tNVDA";
const INTERNAL_API_KEY: &str = "test-key-12345678901234567890123456";

/// Drives the running issuance service through the published Rust client
/// (`st0x_issuance_client`) over a real TCP connection, dogfooding the typed
/// client and shared DTOs against the live server for the non-Alpaca
/// tokenized-asset status endpoint.
#[tokio::test]
async fn tokenized_asset_status_via_client()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("status.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        UNDERLYING,
        TOKEN,
    )
    .await?;

    harness::preseed_frozen_tokenized_asset(
        &db_url,
        evm.vault_address,
        FROZEN_UNDERLYING,
        FROZEN_TOKEN,
    )
    .await?;

    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let base_url = harness::spawn_http_server(config).await?;
    let client = IssuanceClient::new(base_url, INTERNAL_API_KEY)?;

    let status = client
        .tokenized_asset_status(&UnderlyingSymbol::new(UNDERLYING))
        .await?
        .expect("seeded asset has a status");
    assert_eq!(status.underlying, UnderlyingSymbol::new(UNDERLYING));
    assert_eq!(
        status.status,
        TokenizedAssetStatus::Enabled,
        "seeded asset should be enabled (tradeable), not frozen"
    );

    let frozen = client
        .tokenized_asset_status(&UnderlyingSymbol::new(FROZEN_UNDERLYING))
        .await?
        .expect("seeded frozen asset has a status");
    assert_eq!(frozen.underlying, UnderlyingSymbol::new(FROZEN_UNDERLYING));
    assert_eq!(
        frozen.status,
        TokenizedAssetStatus::Frozen,
        "asset with a Frozen event should report frozen through the client"
    );

    let unknown =
        client.tokenized_asset_status(&UnderlyingSymbol::new("NOPE")).await?;
    assert!(unknown.is_none(), "unknown asset should have no status");

    Ok(())
}
