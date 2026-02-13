#![allow(clippy::unwrap_used)]

mod harness;

use alloy::primitives::U256;
use httpmock::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use url::Url;

use st0x_issuance::test_utils::{LocalEvm, ROLE_CERTIFY, ROLE_DEPOSIT};
use st0x_issuance::{
    ANVIL_CHAIN_ID, AlpacaConfig, AuthConfig, Config, IpWhitelist, LogLevel,
    SignerConfig, initialize_rocket,
};

/// Tests that backfill checkpoint is independent from monitor discoveries.
///
/// The critical scenario:
/// 1. Mint receipt directly before service starts (simulates historic receipt)
/// 2. Start service - backfill discovers the receipt, checkpoints to current block
/// 3. Mint another receipt while service running - monitor detects it
/// 4. Restart service
/// 5. Verify both receipts are usable (backfill didn't skip the early one)
#[tokio::test]
async fn test_backfill_checkpoint_independent_from_monitor_discoveries()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    // Use file-based database to persist across restarts
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_backfill_checkpoint.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Setup roles before any minting
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Step 1: Mint a receipt BEFORE the service starts (simulates historic receipt)
    let historic_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
    let (historic_receipt_id, _historic_shares) =
        evm.mint_directly(historic_amount, bot_wallet).await?;

    // Setup mocks
    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca);

    // Preseed asset before starting service so backfill can discover receipts
    let pool =
        SqlitePoolOptions::new().max_connections(5).connect(&db_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    harness::preseed_tokenized_asset_into_pool(
        &pool,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await
    .unwrap();
    pool.close().await;

    // Step 2: Start service - backfill should discover the historic receipt
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    // Give backfill and monitor time to fully start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Step 3: Mint another receipt while service is running (monitor should detect)
    let live_amount = U256::from(50) * U256::from(10).pow(U256::from(18));
    let (live_receipt_id, _live_shares) =
        evm.mint_directly(live_amount, bot_wallet).await?;

    // Give monitor time to detect the new receipt
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Step 4: "Crash" - drop the first service instance
    drop(client1);

    // Step 5: Restart service - backfill should use checkpoint, not monitor's block
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Give backfill time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Step 6: Verify both receipts are tracked by querying all events
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    // Query ALL events for the ReceiptInventory aggregate to debug
    let all_events: Vec<(String, String, String)> = sqlx::query_as(
        r"
        SELECT aggregate_type, event_type, payload
        FROM events
        WHERE aggregate_type = 'ReceiptInventory'
        ORDER BY sequence
        ",
    )
    .fetch_all(&query_pool)
    .await?;

    // Find Discovered events (the event_type might include the full path)
    let discovered_events: Vec<&(String, String, String)> = all_events
        .iter()
        .filter(|(_, event_type, _)| event_type.contains("Discovered"))
        .collect();

    assert!(
        discovered_events.len() >= 2,
        "Expected at least 2 discovered receipts, found {}. \
         All events: {:?}",
        discovered_events.len(),
        all_events.iter().map(|(_, t, _)| t.as_str()).collect::<Vec<_>>()
    );

    // Verify the specific receipt IDs are present
    let receipt_ids: Vec<String> = discovered_events
        .iter()
        .filter_map(|(_, _, payload)| {
            let json: serde_json::Value = serde_json::from_str(payload).ok()?;
            // Handle both possible JSON structures
            json["receipt_id"]
                .as_str()
                .or_else(|| json["Discovered"]["receipt_id"].as_str())
                .map(ToString::to_string)
        })
        .collect();

    let historic_id_hex = format!("{historic_receipt_id:#x}");
    let live_id_hex = format!("{live_receipt_id:#x}");

    assert!(
        receipt_ids.iter().any(|id| id == &historic_id_hex),
        "Historic receipt {historic_id_hex} not found in discovered receipts: {receipt_ids:?}"
    );

    assert!(
        receipt_ids.iter().any(|id| id == &live_id_hex),
        "Live receipt {live_id_hex} not found in discovered receipts: {receipt_ids:?}"
    );

    Ok(())
}

/// Tests that receipt backfill discovers receipts from ALL enabled tokenized assets,
/// not just the single vault in Config.
///
/// This test verifies multi-vault support:
/// - Each tokenized asset has its own vault contract
/// - Backfill processes all enabled tokenized assets
/// - Receipts minted on any vault are discovered
#[tokio::test]
async fn test_multi_vault_backfill_discovers_receipts_from_all_assets()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;

    // Deploy a SECOND vault for TSLA (first vault is AAPL)
    let (tsla_vault, tsla_authorizer) = evm.deploy_additional_vault().await?;

    // Grant roles on TSLA vault to bot wallet
    evm.grant_role_on_authorizer(tsla_authorizer, ROLE_DEPOSIT, bot_wallet)
        .await?;
    evm.grant_role_on_authorizer(tsla_authorizer, ROLE_CERTIFY, bot_wallet)
        .await?;
    evm.certify_specific_vault(tsla_vault, U256::MAX).await?;

    // Mint directly on TSLA vault BEFORE service starts
    // This simulates a receipt that exists on-chain but the service doesn't know about
    let mint_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
    let (receipt_id, _shares) =
        evm.mint_directly_on_vault(tsla_vault, mint_amount, bot_wallet).await?;

    // Create a temp file database so we can preseed assets before rocket starts
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    let database_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Create and migrate the database, then preseed assets
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    // Preseed BOTH tokenized assets BEFORE rocket starts
    // This simulates production where assets are configured before the service runs
    harness::preseed_tokenized_asset_into_pool(
        &pool,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await
    .unwrap();
    harness::preseed_tokenized_asset_into_pool(
        &pool, tsla_vault, "TSLA", "tTSLA",
    )
    .await
    .unwrap();

    // Close the pool so rocket can open it
    pool.close().await;

    let config = Config {
        database_url,
        database_max_connections: 5,
        rpc_url: Url::parse(&evm.endpoint)?,
        chain_id: ANVIL_CHAIN_ID,
        signer: SignerConfig::Local(evm.private_key),
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
        log_level: LogLevel::Debug,
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

    // Start rocket - backfill should run and discover the TSLA receipt
    initialize_rocket(config).await?;

    // Give backfill time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Reconnect to verify receipt was discovered
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite:{}", db_path.display()))
        .await?;

    // Query events table to verify the TSLA receipt was discovered by backfill.
    // Backfill stores ReceiptInventoryEvent::Discovered events in the events table.
    let tsla_vault_str = tsla_vault.to_string();
    let receipt_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count: i64"
        FROM events
        WHERE aggregate_type = 'ReceiptInventory'
          AND aggregate_id = ?
          AND event_type = 'ReceiptInventoryEvent::Discovered'
        "#,
        tsla_vault_str
    )
    .fetch_one(&pool)
    .await?;

    assert!(
        receipt_count > 0,
        "Receipt {receipt_id} from TSLA vault ({tsla_vault}) should have been discovered by backfill. \
         Found {receipt_count} Discovered events for vault."
    );

    Ok(())
}
