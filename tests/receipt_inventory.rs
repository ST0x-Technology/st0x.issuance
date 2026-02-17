#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use std::time::Duration;
use url::Url;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::bindings::Receipt::ReceiptInstance;
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
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

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
    let client1 = Client::tracked(rocket1).await?;

    // Give backfill and monitor time to fully start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Step 3: Mint another receipt while service is running (monitor should detect)
    let live_amount = U256::from(50) * U256::from(10).pow(U256::from(18));
    let (live_receipt_id, _live_shares) =
        evm.mint_directly(live_amount, bot_wallet).await?;

    // Give monitor time to detect the new receipt
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Step 4: "Crash" - drop the first service instance
    drop(client1);

    // Step 5: Restart service - backfill should use checkpoint, not monitor's block
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 = Client::tracked(rocket2).await?;

    // Give backfill time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

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
    tokio::time::sleep(Duration::from_millis(500)).await;

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

/// Tests that startup reconciliation detects stale receipt balances (receipts
/// in the event store that no longer exist on-chain) and excludes them from
/// burn planning.
///
/// This proves the startup reconciliation path works end-to-end:
/// 1. Seed receipt A in the event store with balance 100e18 (never minted on Anvil)
/// 2. Start service — backfill finds no new receipts, reconciliation queries
///    balanceOf for receipt A → returns 0 → emits BalanceReconciled + Depleted
/// 3. Mint receipt B via API (50 shares to user, receipt held by bot)
/// 4. User transfers 50 shares to trigger redemption
/// 5. Service burns from receipt B (not stale A) → redemption succeeds
///
/// If reconciliation didn't run, the service would include stale receipt A
/// in the burn plan, and the on-chain burn would fail.
#[tokio::test]
async fn test_startup_reconciliation_detects_stale_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_startup_reconciliation.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Seed event store: tokenized asset + stale receipt that was never minted on Anvil.
    // Use receipt_id 0xff to avoid collision with Anvil's auto-assigned IDs (starting at 1).
    // On Anvil, balanceOf for receipt 0xff returns 0 since it was never minted.
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

    let vault_addr = evm.vault_address.to_string();
    let stale_balance = "0x56bc75e2d63100000"; // 100e18
    let payload = json!({
        "Discovered": {
            "receipt_id": "0xff",
            "balance": stale_balance,
            "block_number": 1_u64,
            "tx_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "source": "External"
        }
    });
    let payload_str = payload.to_string();
    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type, aggregate_id, sequence,
            event_type, event_version, payload, metadata
        )
        VALUES ('ReceiptInventory', ?, 1,
                'ReceiptInventoryEvent::Discovered', '1.0', ?, '{}')
        ",
    )
    .bind(&vault_addr)
    .bind(&payload_str)
    .execute(&pool)
    .await?;

    pool.close().await;

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    // Start service — reconciliation should detect stale receipt A (on-chain balance 0)
    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket = initialize_rocket(config).await?;
    let client = Client::tracked(rocket).await?;

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Mint receipt B via API (50 shares to user)
    let link_body = harness::setup_account(&client, user_wallet).await;
    harness::perform_mint_and_confirm(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-reconcile",
        "50.0",
    )
    .await?;

    let shares_b = harness::wait_for_shares(&vault, user_wallet).await?;

    // User transfers shares to trigger redemption
    vault.transfer(bot_wallet, shares_b).send().await?.get_receipt().await?;

    // Service should burn from receipt B (not stale A). If reconciliation didn't
    // run, the burn plan would include A (balance 100e18 in aggregate) and the
    // on-chain burn would revert since A doesn't exist.
    harness::wait_for_burn(&vault, bot_wallet).await?;

    assert_eq!(
        vault.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "Bot should have no shares after successful burn"
    );

    Ok(())
}

/// Tests that external burns are detected by the live Withdraw monitor and
/// update the receipt inventory, allowing subsequent redemptions to succeed.
///
/// This proves the Withdraw monitor works end-to-end:
/// 1. Mint receipt A directly to bot wallet (both receipts AND shares)
/// 2. Start service — backfill discovers receipt A
/// 3. External burn depletes receipt A entirely
/// 4. Mint receipt B via API (50 shares to user, receipt held by bot)
/// 5. User transfers 50 shares to trigger redemption
/// 6. Service burns from receipt B (not depleted A) → redemption succeeds
///
/// If the monitor didn't detect the external burn, the service would include
/// depleted receipt A in the burn plan, and the on-chain burn would fail.
#[tokio::test]
async fn test_external_burn_detected_by_withdraw_monitor()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_withdraw_monitor.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Bot needs deposit (to mint directly) + withdraw (to burn externally)
    // User needs deposit role for the API mint flow
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Step 1: Mint receipt A directly to bot wallet BEFORE service starts.
    // Direct mint gives bot BOTH shares (ERC-20) and receipts (ERC-1155),
    // enabling a subsequent external burn (withdraw requires both).
    let one_share = U256::from(10).pow(U256::from(18));
    let amount_a = U256::from(100) * one_share;
    let (receipt_a_id, shares_a) =
        evm.mint_directly(amount_a, bot_wallet).await?;

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Step 2: Start service — backfill discovers receipt A
    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket = initialize_rocket(config).await?;
    let client = Client::tracked(rocket).await?;

    // Wait for backfill + monitor to be ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    let receipt_contract_addr = Address::from(vault.receipt().call().await?.0);
    let receipt_contract =
        ReceiptInstance::new(receipt_contract_addr, &user_provider);

    // Step 3: External burn — deplete receipt A entirely
    evm.withdraw_directly(receipt_a_id, shares_a, bot_wallet).await?;

    // Verify on-chain: receipt A is fully burned
    let receipt_a_balance =
        receipt_contract.balanceOf(bot_wallet, receipt_a_id).call().await?;
    assert_eq!(
        receipt_a_balance,
        U256::ZERO,
        "Receipt A should be fully burned on-chain"
    );

    // Give the monitor time to detect the Withdraw event and reconcile
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 4: Mint receipt B via API (50 shares to user)
    let link_body = harness::setup_account(&client, user_wallet).await;
    harness::perform_mint_and_confirm(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-B",
        "50.0",
    )
    .await?;

    let shares_b = harness::wait_for_shares(&vault, user_wallet).await?;

    // Step 5: User transfers shares_b to trigger redemption
    vault.transfer(bot_wallet, shares_b).send().await?.get_receipt().await?;

    // Step 6: Service should burn from receipt B (not depleted A) and succeed.
    // If monitor didn't work, service would include A in burn plan → on-chain
    // burn would fail because A has 0 balance.
    harness::wait_for_burn(&vault, bot_wallet).await?;

    // Verify on-chain: bot has no shares left (burn succeeded)
    assert_eq!(
        vault.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "Bot should have no shares after successful burn"
    );

    Ok(())
}
