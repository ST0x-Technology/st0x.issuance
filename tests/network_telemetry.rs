mod harness;

use alloy::primitives::utils::parse_ether;
use alloy::providers::ProviderBuilder;
use alloy::providers::ext::AnvilApi;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use sqlx::sqlite::SqlitePoolOptions;
use std::time::Duration;

use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{ETHEREUM_TEST_CHAIN_ID, Network};

use crate::harness::{authenticated_get_json, initialize_rocket};

/// Polls `GET /admin/network-telemetry` until `predicate` accepts the row for
/// `network`, failing after a timeout with the last observed body.
async fn wait_for_network_row(
    client: &Client,
    network: Network,
    description: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut last_body = serde_json::Value::Null;

    loop {
        let body =
            authenticated_get_json(client, "/admin/network-telemetry").await;
        let row = body["networks"]
            .as_array()
            .expect("/admin/network-telemetry must contain a networks array")
            .iter()
            .find(|row| row["network"] == network.as_str())
            .cloned();

        if let Some(row) = row {
            if predicate(&row) {
                return row;
            }
            last_body = row;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {description} on {network}; last row: \
             {last_body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Boots the service on two Anvil chains with low gas thresholds configured,
/// then asserts the telemetry surface end to end: per network poller and
/// backfill pass counters with lag, and the gas monitor tracking the issuer
/// wallet's native balance through ok -> low -> ok as the balance moves
/// across the threshold on one chain while the other stays healthy.
#[tokio::test]
async fn network_telemetry_reports_gas_and_lag_per_network()
-> Result<(), Box<dyn std::error::Error>> {
    let base_evm = LocalEvm::new().await?;
    let eth_evm = LocalEvm::with_chain_id(ETHEREUM_TEST_CHAIN_ID).await?;
    let bot_wallet = base_evm.wallet_address;

    let mock_alpaca = MockServer::start();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("network_telemetry.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // A preseeded Base asset gives the transfer poller and the periodic
    // receipt backfill a vault to scan, so their pass counters and lag move.
    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    harness::preseed_tokenized_asset_into_pool(
        &pool,
        base_evm.vault_address,
        "RKLB",
        "tRKLB",
    )
    .await?;
    pool.close().await;

    let mut config = harness::create_multichain_config_with_db(
        &db_url,
        &mock_alpaca,
        &base_evm,
        &eth_evm,
    )?;
    let threshold = parse_ether("1")?;
    for chain in &mut config.chains {
        chain.low_gas_threshold = Some(threshold);
    }
    config.gas_poll_interval = Duration::from_millis(200);

    let rocket = initialize_rocket(config).await?;
    let client = Client::tracked(rocket).await?;

    // Anvil funds the bot wallet with 10000 ETH, far above the 1 ETH
    // threshold, so both chains must settle at gas status "ok".
    let base_row = wait_for_network_row(
        &client,
        Network::Base,
        "gas ok and loop passes",
        |row| {
            row["gas"]["status"] == "ok"
                && row["transfer_poller"]["passes"].as_u64().unwrap_or(0) >= 1
                && row["receipt_backfill"]["passes"].as_u64().unwrap_or(0) >= 1
        },
    )
    .await;

    assert_eq!(
        base_row["gas"]["threshold_wei"],
        threshold.to_string(),
        "gas row must echo the configured threshold"
    );
    assert!(
        base_row["transfer_poller"]["lag_blocks"].is_u64(),
        "a successful poller pass must record lag_blocks; row: {base_row}"
    );
    assert!(
        base_row["receipt_backfill"]["lag_blocks"].is_u64(),
        "a successful backfill pass must record lag_blocks; row: {base_row}"
    );
    assert_eq!(
        base_row["transfer_poller"]["failures"], 0,
        "healthy poller must report zero failures; row: {base_row}"
    );

    wait_for_network_row(&client, Network::Ethereum, "gas ok", |row| {
        row["gas"]["status"] == "ok"
    })
    .await;

    // Drain the wallet below the threshold on Base only: its gas status must
    // degrade to "low" while Ethereum keeps reporting "ok".
    let base_provider =
        ProviderBuilder::new().connect(&base_evm.endpoint).await?;
    base_provider.anvil_set_balance(bot_wallet, parse_ether("0.5")?).await?;

    let low_row =
        wait_for_network_row(&client, Network::Base, "gas low", |row| {
            row["gas"]["status"] == "low"
        })
        .await;
    assert_eq!(
        low_row["gas"]["balance_wei"],
        parse_ether("0.5")?.to_string(),
        "low gas row must carry the observed balance"
    );

    let eth_row = authenticated_get_json(&client, "/admin/network-telemetry")
        .await["networks"]
        .as_array()
        .expect("/admin/network-telemetry must contain a networks array")
        .iter()
        .find(|row| row["network"] == Network::Ethereum.as_str())
        .cloned()
        .expect("ethereum row must be present");
    assert_eq!(
        eth_row["gas"]["status"], "ok",
        "draining the Base wallet must not affect the Ethereum row: {eth_row}"
    );

    // Refunding the wallet must clear the low status without a restart.
    base_provider.anvil_set_balance(bot_wallet, parse_ether("10")?).await?;
    wait_for_network_row(&client, Network::Base, "gas recovered", |row| {
        row["gas"]["status"] == "ok"
    })
    .await;

    Ok(())
}
