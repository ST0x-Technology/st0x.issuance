#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, Bytes, U256, b256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use httpmock::prelude::*;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use uuid::{Uuid, uuid};

use harness::alpaca_mocks::{setup_mint_mocks, setup_redemption_mocks};
use st0x_issuance::bindings::OffchainAssetReceiptVault::{
    self, OffchainAssetReceiptVaultInstance,
};
use st0x_issuance::test_utils::LocalEvm;

use crate::harness::{create_provider, initialize_rocket};

static RECOVERY_TEST_LOCK: Mutex<()> = Mutex::const_new(());

async fn recovery_test_guard() -> MutexGuard<'static, ()> {
    RECOVERY_TEST_LOCK.lock().await
}

async fn wait_for_event_count(
    db_url: &str,
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    min_count: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(30);
    let poll_interval = Duration::from_millis(100);

    loop {
        let count = sqlx::query_scalar::<_, i64>(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = ?
              AND aggregate_id = ?
              AND event_type = ?
            ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(event_type)
        .fetch_one(&pool)
        .await?;

        if count >= min_count {
            return Ok(count);
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for {event_type} on {aggregate_type}/{aggregate_id}; \
                 found {count}, expected at least {min_count}"
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn insert_event(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    aggregate_type: &str,
    aggregate_id: &str,
    sequence: i32,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload_str = payload.to_string();
    sqlx::query!(
        r#"
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (?, ?, ?, ?, '1.0', ?, '{}')
        "#,
        aggregate_type,
        aggregate_id,
        sequence,
        event_type,
        payload_str
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_minting_state_events(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    issuer_request_id: &str,
    tokenization_request_id: &Uuid,
    client_id: &str,
    wallet: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = chrono::Utc::now();

    let initiated_payload = json!({
        "Initiated": {
            "issuer_request_id": issuer_request_id,
            "tokenization_request_id": tokenization_request_id,
            "quantity": "50.0",
            "underlying": "AAPL",
            "token": "tAAPL",
            "network": "base",
            "client_id": client_id,
            "wallet": wallet,
            "initiated_at": now.to_rfc3339()
        }
    });
    insert_event(
        pool,
        "Mint",
        issuer_request_id,
        1,
        "MintEvent::Initiated",
        &initiated_payload,
    )
    .await?;

    let confirmed_payload = json!({
        "JournalConfirmed": {
            "issuer_request_id": issuer_request_id,
            "confirmed_at": now.to_rfc3339()
        }
    });
    insert_event(
        pool,
        "Mint",
        issuer_request_id,
        2,
        "MintEvent::JournalConfirmed",
        &confirmed_payload,
    )
    .await?;

    let started_payload = json!({
        "MintingStarted": {
            "issuer_request_id": issuer_request_id,
            "started_at": now.to_rfc3339()
        }
    });
    insert_event(
        pool,
        "Mint",
        issuer_request_id,
        3,
        "MintEvent::MintingStarted",
        &started_payload,
    )
    .await?;

    Ok(())
}

/// Tests that recovery finds mints stuck in JournalConfirmed state even after
/// the mint view has been deleted (requiring reprojection before recovery).
///
/// Setup:
/// 1. Complete a mint normally (all the way through)
/// 2. Delete events after JournalConfirmed to roll back the aggregate state
/// 3. Restart — views are cleared and rebuilt repopulates from rolled-back events
/// 4. Recovery finds the JournalConfirmed mint and resumes it
#[tokio::test]
async fn test_mint_recovery_after_view_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    // Standard Anvil test account #2 (not a secret - these are well-known test keys)
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Use file-based database to persist across restarts
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_view_reprojection.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Setup mocks
    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Setup account and complete a mint
    let link_body = harness::setup_account(&client1, user_wallet).await;
    let issuer_request_id = harness::perform_mint_and_confirm(
        &client1,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-view-test",
        "50.0",
    )
    .await?;

    // Wait for mint to complete
    let user_wallet_instance = EthereumWallet::from(user_signer.clone());
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    harness::wait_for_shares(&vault, user_wallet).await?;

    wait_for_event_count(
        &db_url,
        "Mint",
        &issuer_request_id,
        "MintEvent::MintCompleted",
        1,
    )
    .await?;

    // Stop the first service before mutating its persisted state and restarting.
    client1.terminate().await;

    // Connect to the database to manipulate state
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    // Roll back the mint to JournalConfirmed state by deleting events after sequence 2
    // Event sequence: 1=MintInitiated, 2=JournalConfirmed, 3+=MintingStarted, TokensMinted, etc.
    // The aggregate_id is just the issuer_request_id string
    let aggregate_id = issuer_request_id.clone();
    sqlx::query!(
        r#"
        DELETE FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND sequence > 2
        "#,
        aggregate_id
    )
    .execute(&query_pool)
    .await?;

    // Verify we have exactly 2 events (MintInitiated, JournalConfirmed)
    let event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        aggregate_id
    )
    .fetch_one(&query_pool)
    .await?;
    assert_eq!(
        event_count.count, 2,
        "Expected exactly 2 events (MintInitiated, JournalConfirmed)"
    );

    query_pool.close().await;

    // Restart service — views are cleared and rebuilt repopulates views from events,
    // recovery finds the JournalConfirmed mint and resumes it.
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // If recovery worked, the mint should complete and user gets shares
    // Note: The on-chain mint already happened, but we deleted the events,
    // so recovery will attempt to mint again. This might fail due to
    // duplicate mint, but that's a separate issue (Task 7-9 handles this).
    // For now, we just want to verify recovery FINDS the stuck mint.

    wait_for_event_count(
        &db_url,
        "Mint",
        &aggregate_id,
        "MintEvent::MintingStarted",
        1,
    )
    .await?;

    // Check if the mint progressed past JournalConfirmed
    let query_pool2 =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let final_event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        aggregate_id
    )
    .fetch_one(&query_pool2)
    .await?;

    // If recovery worked, there should be more than 2 events
    // (at minimum MintingStarted would be added)
    assert!(
        final_event_count.count > 2,
        "Recovery should have processed the JournalConfirmed mint. \
         Expected >2 events, found {}. This indicates recovery ran before \
         view reprojection and missed the stuck mint.",
        final_event_count.count
    );

    harness::wait_for_mock_hit(&mint_callback_mock).await?;

    Ok(())
}

/// Tests restart recovery from `Minting` after the on-chain deposit mined but
/// before `MintTxSubmitted` was durably recorded.
///
/// Under the durable submit job, prepare+broadcast are one step — there is no
/// intermediate `MintTxIntended` event. The crash window that used to sit on
/// `MintIntended` is now: aggregate still in `Minting`, receipt already on
/// chain / in inventory.
///
/// Scenario:
/// 1. Complete a mint normally (creates on-chain receipt)
/// 2. Roll back the event store to `MintingStarted`
/// 3. Restart service — views rebuild; recovery finds the existing receipt and
///    records success without re-minting
#[tracing_test::traced_test]
#[tokio::test]
async fn test_mint_recovery_from_minting_state_when_receipt_exists()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path =
        temp_dir.path().join("test_minting_recovery_receipt_exists.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = harness::setup_account(&client1, user_wallet).await;
    let issuer_request_id = harness::perform_mint_and_confirm(
        &client1,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-minting-recovery-1",
        "50.0",
    )
    .await?;

    let user_wallet_instance = EthereumWallet::from(user_signer.clone());
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares_before = harness::wait_for_shares(&vault, user_wallet).await?;
    harness::wait_for_mock_hits(&mint_callback_mock, 1).await?;

    client1.terminate().await;
    assert_eq!(
        mint_callback_mock.calls_async().await,
        1,
        "the first service must finish exactly one callback before restart"
    );

    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    // Keep MintingStarted but remove MintTxSubmitted and every later event,
    // reproducing a crash after the durable submit job broadcast/mined but
    // before RecordTxSubmitted persisted.
    let aggregate_id = issuer_request_id.clone();
    sqlx::query(
        r"
        DELETE FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND sequence > 3
        ",
    )
    .bind(&aggregate_id)
    .execute(&query_pool)
    .await?;

    let event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        aggregate_id
    )
    .fetch_one(&query_pool)
    .await?;
    assert_eq!(
        event_count.count, 3,
        "Expected the lifecycle prefix through MintingStarted"
    );

    query_pool.close().await;

    // Restart — views are cleared and rebuilt repopulates from events, recovery
    // finds the MintIntended mint and checks the backfilled receipt before
    // attempting another broadcast.
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    harness::wait_for_mock_hits(&mint_callback_mock, 2).await?;
    assert_eq!(
        mint_callback_mock.calls_async().await,
        2,
        "restart recovery must deliver exactly one additional callback"
    );
    assert!(logs_contain("Found existing receipt, recording recovery"));

    // Verify the user still has the same shares (no double mint)
    let shares_after = vault.balanceOf(user_wallet).call().await?;
    assert_eq!(
        shares_after, shares_before,
        "User should have same shares (no double mint)"
    );

    Ok(())
}

/// Tests recovery from `Minting` state when the on-chain mint did NOT succeed.
///
/// Scenario:
/// 1. Create mint events directly in event store up to `Minting` state
/// 2. No on-chain mint happens (simulates crash before blockchain call)
/// 3. Start service
/// 4. Recovery finds no receipt in inventory and retries the mint
#[tokio::test]
async fn test_mint_recovery_from_minting_state_when_no_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_minting_recovery_no_receipt.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    // First start the service to set up the database schema and seed data
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Setup account to get a valid client_id
    let link_body = harness::setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    client1.terminate().await;

    // Now insert events directly to create a mint stuck in Minting state
    // WITHOUT any on-chain transaction
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let issuer_request_id =
        uuid!("00000001-0000-0000-0000-000000000001").to_string();
    let issuer_request_id = issuer_request_id.as_str();
    let tokenization_request_id = uuid!("10000001-0000-0000-0000-000000000001");

    insert_minting_state_events(
        &query_pool,
        issuer_request_id,
        &tokenization_request_id,
        &client_id.to_string(),
        user_wallet,
    )
    .await?;

    query_pool.close().await;

    // Start service - recovery should find the stuck mint and retry it
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Wait for recovery and mint to complete
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Recovery should retry the mint and user should receive shares
    let shares = harness::wait_for_shares(&vault, user_wallet).await;
    assert!(
        shares.is_ok(),
        "Recovery should have retried the mint and user should have shares. \
         Recovery for Minting state (no receipt) not implemented."
    );

    harness::wait_for_mock_hit(&mint_callback_mock).await?;

    Ok(())
}

/// Tests double-mint prevention when recovery detects an existing receipt.
///
/// Scenario:
/// 1. Manually mint on-chain with a specific issuer_request_id
/// 2. Create mint events directly in event store with matching issuer_request_id
/// 3. Start service
/// 4. Recovery finds the existing receipt and records it without minting again
#[tokio::test]
async fn test_mint_recovery_prevents_double_mint()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_double_mint_prevention.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Manually mint on-chain with a specific issuer_request_id BEFORE service starts
    let issuer_request_id_uuid = uuid!("00000002-0000-0000-0000-000000000002");
    let issuer_request_id = issuer_request_id_uuid.to_string();
    let issuer_request_id = issuer_request_id.as_str();
    let tokenization_request_id = uuid!("10000002-0000-0000-0000-000000000002");

    let receipt_info_json = json!({
        "tokenization_request_id": tokenization_request_id,
        "issuer_request_id": issuer_request_id_uuid,
        "underlying": "AAPL",
        "quantity": "50.0",
        "operation_type": "Mint",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "notes": null
    });

    let encoded_receipt_info =
        Bytes::from(serde_json::to_vec(&receipt_info_json)?);

    let amount = U256::from(50) * U256::from(10).pow(U256::from(18));
    // Mint to bot_wallet (not user_wallet) because in the real ITN flow, the bot
    // calls deposit() and receives the receipt. The backfill only looks for receipts
    // owned by bot_wallet.
    let (_receipt_id, _shares_minted, _) = evm
        .mint_directly_with_info(amount, bot_wallet, encoded_receipt_info)
        .await?;

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    // Start service to set up database
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    let link_body = harness::setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    client1.terminate().await;

    // Insert events to create a mint stuck in Minting state with matching issuer_request_id
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    insert_minting_state_events(
        &query_pool,
        issuer_request_id,
        &tokenization_request_id,
        &client_id.to_string(),
        user_wallet,
    )
    .await?;

    query_pool.close().await;

    // Get initial share balance of bot_wallet (which received shares from the manual mint)
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares_before = vault.balanceOf(bot_wallet).call().await?;

    // Start service - recovery should find the existing receipt and NOT mint again
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check events - recovery should have recorded the existing mint
    let query_pool2 =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let final_event_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i32"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        "#,
        issuer_request_id
    )
    .fetch_one(&query_pool2)
    .await?;

    assert!(
        final_event_count.count > 3,
        "Recovery should have added events for existing mint. \
         Expected >3 events, found {}. Double-mint prevention not implemented.",
        final_event_count.count
    );

    // Most importantly: bot should have SAME shares (no double mint)
    // If recovery re-minted, the bot's share balance would double.
    let shares_after = vault.balanceOf(bot_wallet).call().await?;
    assert_eq!(
        shares_after, shares_before,
        "Bot should have same shares after recovery (no double mint). \
         Before: {shares_before}, After: {shares_after}"
    );

    harness::wait_for_mock_hit(&mint_callback_mock).await?;

    Ok(())
}

/// Tests that the receipt monitor triggers mint recovery when it discovers a
/// receipt for a mint stuck in `MintingFailed` state.
///
/// Production scenario this reproduces:
/// 1. Service submitted a mint transaction to the blockchain
/// 2. Service marked the mint as MintingFailed (polling timeout before confirmation)
/// 3. The on-chain transaction actually succeeded (receipt exists)
/// 4. The receipt monitor detects the Deposit event
/// 5. The receipt monitor should trigger recovery for the associated mint
///
/// Without the fix, the receipt monitor discovers the receipt and registers it
/// in the inventory, but nothing connects this to the failed mint. The mint
/// remains in MintingFailed state until the next service restart.
#[tokio::test]
async fn test_receipt_monitor_triggers_recovery_for_failed_mint()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_receipt_monitor_recovery.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Phase 1: Start service to set up DB schema, seed asset and account.
    // We need to stop and restart so the receipt monitor has vault configs
    // (assets must exist in the DB before initialize_rocket for monitors to spawn).
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    let link_body = harness::setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    client1.terminate().await;

    // Phase 2: Restart service. The receipt monitor will spawn (asset exists).
    // Auto-recovery runs but finds NO stuck mints (we haven't inserted any yet).
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Wait for auto-recovery to complete (it finds nothing) and for the
    // receipt monitor to establish its WebSocket subscription.
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // NOW insert MintingFailed events. Auto-recovery already ran and found
    // nothing. These events bypass the CQRS framework so the mint_view is NOT
    // updated — only the event store has them. The receipt monitor is the only
    // thing that can trigger recovery for this mint.
    let issuer_request_id_uuid = uuid!("00000004-0000-0000-0000-000000000004");
    let issuer_request_id = issuer_request_id_uuid.to_string();
    let issuer_request_id = issuer_request_id.as_str();
    let tokenization_request_id = uuid!("10000004-0000-0000-0000-000000000004");

    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    insert_minting_state_events(
        &query_pool,
        issuer_request_id,
        &tokenization_request_id,
        &client_id.to_string(),
        user_wallet,
    )
    .await?;

    let failed_payload = json!({
        "MintingFailed": {
            "issuer_request_id": issuer_request_id,
            "error": "Polling timeout: transaction may still confirm",
            "failed_at": chrono::Utc::now().to_rfc3339()
        }
    });
    insert_event(
        &query_pool,
        "Mint",
        issuer_request_id,
        4,
        "MintEvent::MintingFailed",
        &failed_payload,
    )
    .await?;
    query_pool.close().await;

    // Now simulate the on-chain transaction succeeding AFTER the service marked
    // it failed. The receipt monitor is running and will detect this Deposit event.
    let receipt_info_json = json!({
        "tokenization_request_id": tokenization_request_id,
        "issuer_request_id": issuer_request_id_uuid,
        "underlying": "AAPL",
        "quantity": "50.0",
        "operation_type": "Mint",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "notes": null
    });
    let encoded_receipt_info =
        Bytes::from(serde_json::to_vec(&receipt_info_json)?);
    let amount = U256::from(50) * U256::from(10).pow(U256::from(18));

    evm.mint_directly_with_info(amount, bot_wallet, encoded_receipt_info)
        .await?;

    // The receipt monitor should:
    // 1. Detect the Deposit event
    // 2. Discover the receipt (ReceiptSource::Itn with our issuer_request_id)
    // 3. Trigger recovery for the MintingFailed mint
    // 4. Recovery finds the receipt → ExistingMintRecovered → CallbackPending
    // 5. Recovery sends callback → MintCompleted

    // Assert: the Alpaca mock received the mint callback
    harness::wait_for_mock_hit(&mint_callback_mock).await?;

    Ok(())
}

/// Tests recovery from `MintingFailed` state.
///
/// Scenario:
/// 1. Create mint events directly in event store up to `MintingFailed` state
/// 2. Start service
/// 3. Recovery retries the mint and completes it
#[tokio::test]
async fn test_mint_recovery_from_minting_failed_state()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_minting_failed_recovery.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    // Start service to set up database schema
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = harness::setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    client1.terminate().await;

    // Insert events to create a mint in MintingFailed state
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let issuer_request_id =
        uuid!("00000003-0000-0000-0000-000000000003").to_string();
    let issuer_request_id = issuer_request_id.as_str();

    let tokenization_request_id = uuid!("10000003-0000-0000-0000-000000000003");
    insert_minting_state_events(
        &query_pool,
        issuer_request_id,
        &tokenization_request_id,
        &client_id.to_string(),
        user_wallet,
    )
    .await?;

    // Event 4: MintingFailed. failed_at is set well in the past so the first
    // automatic retry window (1m) has already elapsed and recovery retries
    // immediately rather than waiting.
    let failed_payload = json!({
        "MintingFailed": {
            "issuer_request_id": issuer_request_id,
            "error": "Simulated blockchain error",
            "failed_at": (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339()
        }
    });
    insert_event(
        &query_pool,
        "Mint",
        issuer_request_id,
        4,
        "MintEvent::MintingFailed",
        &failed_payload,
    )
    .await?;

    query_pool.close().await;

    // Start service - recovery should find the failed mint and retry it
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // Wait for recovery and mint to complete
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Recovery should retry the mint and user should receive shares
    let shares = harness::wait_for_shares(&vault, user_wallet).await;
    assert!(
        shares.is_ok(),
        "Recovery should have retried the failed mint and user should have shares. \
         Recovery for MintingFailed state not implemented."
    );

    harness::wait_for_mock_hit(&mint_callback_mock).await?;

    Ok(())
}

/// Tests that views are cleared and rebuilt from events on startup.
///
/// Scenario:
/// 1. Start service, set up account + asset via API, drop service
/// 2. Seed a RedemptionDetected event directly into the events table (no view row)
/// 3. Restart service — views are cleared and rebuilt from events
/// 4. Assert: recovery processes the seeded Detected redemption (calls Alpaca)
///
/// The seeded event has no view row until the rebuild creates one. If views
/// were not rebuilt, recovery wouldn't find the Detected redemption at all.
#[tokio::test]
async fn test_startup_clears_and_rebuilds_views()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_view_clearing.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    // Phase 1: Start service and set up account + asset via API
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_account(&client1, user_wallet).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    client1.terminate().await;

    // Phase 2: Seed a Detected redemption for the LINKED wallet into events
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let redemption_id = "red-cafebabe";
    let detected_payload = json!({
        "Detected": {
            "issuer_request_id": redemption_id,
            "underlying": "AAPL",
            "token": "tAAPL",
            "wallet": user_wallet,
            "quantity": "50.0",
            "tx_hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "block_number": 1,
            "detected_at": "2025-01-01T00:00:00Z"
        }
    });
    insert_event(
        &query_pool,
        "Redemption",
        redemption_id,
        1,
        "RedemptionEvent::Detected",
        &detected_payload,
    )
    .await?;

    query_pool.close().await;

    // Phase 3: Restart — views are cleared and rebuilt from events.
    // The seeded Detected event gets a view row from the rebuild.
    // Recovery finds it and calls Alpaca (wallet is linked).
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // If views were rebuilt from events, recovery found the Detected redemption
    // and called Alpaca. Without rebuild, the seeded event has no view row
    // and recovery never sees it.
    assert!(
        redeem_mock.calls_async().await >= 1,
        "Alpaca redeem should be called for the seeded Detected redemption, \
         proving views were rebuilt from events on startup. \
         Got 0 calls — views may not be clearing and rebuilding."
    );

    // Verify the redemption progressed past Detected
    let verify_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let event_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count: i64"
        FROM events
        WHERE aggregate_type = 'Redemption'
          AND aggregate_id = ?
        "#,
        redemption_id
    )
    .fetch_one(&verify_pool)
    .await?;

    assert!(
        event_count > 1,
        "Redemption should have more than just the Detected event \
         (recovery should have progressed it). Found {event_count} events."
    );

    Ok(())
}

/// Tests that a redemption stuck in Detected state with no linked account
/// is auto-failed on startup, not retried infinitely.
///
/// Scenario:
/// 1. Seed a Detected redemption for a wallet with no linked account
///    (simulates a redemption from an old wallet that was unlinked)
/// 2. Start service
/// 3. Assert: Alpaca redeem mock is NOT called for the orphaned redemption
/// 4. Assert: service can still process new mints normally
///
/// Without auto-failure, recovery calls recover_single_detected which hits
/// AccountNotFound, logs a warning, and retries on every startup forever.
#[tokio::test]
async fn test_detected_redemption_auto_failed_when_no_account()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_detected_auto_fail.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    // Phase 1: Start service to set up DB, seed asset and account
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = harness::setup_account(&client1, user_wallet).await;

    client1.terminate().await;

    // Phase 2: Seed a Detected redemption for an UNKNOWN wallet (no linked account)
    let orphan_wallet = Address::random();
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let orphan_id = "red-deadbeef";
    let detected_payload = json!({
        "Detected": {
            "issuer_request_id": orphan_id,
            "underlying": "AAPL",
            "token": "tAAPL",
            "wallet": orphan_wallet,
            "quantity": "50.0",
            "tx_hash": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "block_number": 1,
            "detected_at": "2025-01-01T00:00:00Z"
        }
    });
    insert_event(
        &query_pool,
        "Redemption",
        orphan_id,
        1,
        "RedemptionEvent::Detected",
        &detected_payload,
    )
    .await?;

    query_pool.close().await;

    // Phase 3: Restart service — recovery should auto-fail the orphaned redemption
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let client2 = rocket::local::asynchronous::Client::tracked(rocket2).await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assert: Alpaca redeem was NOT called for the orphaned redemption
    assert_eq!(
        redeem_mock.calls_async().await,
        0,
        "Alpaca redeem should NOT be called for orphaned redemption (no account). \
         Recovery should auto-fail it instead of retrying."
    );

    // Assert: The orphaned redemption should have a RedemptionFailed event
    let verify_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let failed_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count: i64"
        FROM events
        WHERE aggregate_type = 'Redemption'
          AND aggregate_id = 'red-deadbeef'
          AND event_type = 'RedemptionEvent::RedemptionFailed'
        "#
    )
    .fetch_one(&verify_pool)
    .await?;

    assert_eq!(
        failed_count, 1,
        "Orphaned redemption should have a RedemptionFailed event (auto-failed). \
         Found {failed_count}. Recovery is not auto-failing unrecoverable redemptions."
    );

    // Assert: service still works — can process a new mint
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    harness::perform_mint_and_confirm(
        &client2,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-after-orphan",
        "50.0",
    )
    .await?;

    harness::wait_for_shares(&vault, user_wallet).await?;

    // The mint's on-chain deposit and the Alpaca callback run in a background
    // task after confirm returns; wait_for_shares only guarantees the deposit,
    // so poll for the callback rather than asserting it has already fired.
    harness::wait_for_mock_hit(&mint_callback_mock).await?;

    Ok(())
}

/// Tests that a redemption stuck in BurnFailed state with no on-chain share
/// balance is auto-failed on startup, not retried infinitely.
///
/// Scenario:
/// 1. Start service to set up DB schema, seed asset and account
/// 2. Seed a BurnFailed redemption directly in the event store
/// 3. Restart service (bot has 0 shares on-chain — fresh Anvil)
/// 4. Assert: redemption is auto-failed (RedemptionFailed event emitted)
///
/// Without auto-failure, recovery checks on-chain balance, sees it's
/// insufficient, logs "MANUAL INTERVENTION REQUIRED", and skips — repeating
/// this warning on every startup forever.
#[tokio::test]
async fn test_burn_failed_redemption_auto_failed_when_no_balance()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_burning_auto_fail.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    // Phase 1: Start service to set up DB schema, seed asset and account
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let _link_body = harness::setup_account(&client1, user_wallet).await;

    client1.terminate().await;

    // Phase 2: Seed a BurnFailed redemption directly in the event store
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    let stuck_id = "red-baadf00d";
    let now = chrono::Utc::now().to_rfc3339();

    insert_event(&query_pool, "Redemption",
        stuck_id,
        1,
        "RedemptionEvent::Detected",
        &json!({
            "Detected": {
                "issuer_request_id": stuck_id,
                "underlying": "AAPL",
                "token": "tAAPL",
                "wallet": user_wallet,
                "quantity": "50.0",
                "tx_hash": "0xbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00d",
                "block_number": 999,
                "detected_at": &now
            }
        }),
    )
    .await?;

    insert_event(
        &query_pool,
        "Redemption",
        stuck_id,
        2,
        "RedemptionEvent::AlpacaCalled",
        &json!({
            "AlpacaCalled": {
                "issuer_request_id": stuck_id,
                "tokenization_request_id": "tok-stuck-burning",
                "alpaca_quantity": "50.0",
                "dust_quantity": "0",
                "called_at": &now
            }
        }),
    )
    .await?;

    insert_event(
        &query_pool,
        "Redemption",
        stuck_id,
        3,
        "RedemptionEvent::AlpacaJournalCompleted",
        &json!({
            "AlpacaJournalCompleted": {
                "issuer_request_id": stuck_id,
                "alpaca_journal_completed_at": &now
            }
        }),
    )
    .await?;

    insert_event(
        &query_pool,
        "Redemption",
        stuck_id,
        4,
        "RedemptionEvent::BurningFailed",
        &json!({
            "BurningFailed": {
                "issuer_request_id": stuck_id,
                "error": "ERC1155: burn amount exceeds balance",
                "failed_at": &now
            }
        }),
    )
    .await?;

    query_pool.close().await;

    // Phase 3: Restart service — bot has 0 shares on fresh Anvil, so recovery
    // should detect insufficient balance and auto-fail the stuck redemption
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    let failed_count = wait_for_event_count(
        &db_url,
        "Redemption",
        stuck_id,
        "RedemptionEvent::RedemptionFailed",
        1,
    )
    .await?;

    assert_eq!(
        failed_count, 1,
        "Stuck BurnFailed redemption should have a RedemptionFailed event (auto-failed). \
         Found {failed_count}. Recovery is not auto-failing redemptions with insufficient balance."
    );

    Ok(())
}

/// Count vault `Deposit` logs on Anvil (all owners). Used to assert a mint
/// recovery path never submits a second on-chain deposit.
async fn count_vault_deposits(
    provider: &impl Provider,
    vault_address: Address,
) -> Result<usize, Box<dyn std::error::Error>> {
    let vault = OffchainAssetReceiptVaultInstance::new(vault_address, provider);
    let deposit_logs =
        provider.get_logs(&vault.Deposit_filter().from_block(0).filter).await?;
    let mut count = 0usize;
    for log in &deposit_logs {
        OffchainAssetReceiptVault::Deposit::decode_log(&log.inner).map_err(
            |error| format!("failed to decode vault Deposit log: {error}"),
        )?;
        count += 1;
    }
    Ok(count)
}

/// Poison a completed mint back to `TxSubmitted` by deleting events after the
/// latest `MintTxSubmitted` (and clearing snapshots). Event-store-only setup
/// for double-mint recovery e2e scenarios.
async fn poison_mint_to_tx_submitted(
    db_url: &str,
    issuer_request_id: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;

    let submitted_sequence: i64 = sqlx::query_scalar(
        "
        SELECT sequence
        FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND event_type = 'MintEvent::MintTxSubmitted'
        ORDER BY sequence DESC
        LIMIT 1
        ",
    )
    .bind(issuer_request_id)
    .fetch_one(&query_pool)
    .await?;

    sqlx::query(
        "
        DELETE FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND sequence > ?
        ",
    )
    .bind(issuer_request_id)
    .bind(submitted_sequence)
    .execute(&query_pool)
    .await?;

    sqlx::query(
        "
        DELETE FROM snapshots
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
        ",
    )
    .bind(issuer_request_id)
    .execute(&query_pool)
    .await?;

    query_pool.close().await;
    Ok(submitted_sequence)
}

/// Poison a completed mint to `MintingFailed` after the latest
/// `MintTxSubmitted` (null-receipt class of production failure).
async fn poison_mint_to_minting_failed(
    db_url: &str,
    issuer_request_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let submitted_sequence =
        poison_mint_to_tx_submitted(db_url, issuer_request_id).await?;

    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;

    let failed_sequence = submitted_sequence + 1;
    let failed_payload = json!({
        "MintingFailed": {
            "issuer_request_id": issuer_request_id,
            "error": "Simulated null receipt / uncertain confirmation after mined deposit",
            "failed_at": (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339()
        }
    });
    insert_event(
        &query_pool,
        "Mint",
        issuer_request_id,
        i32::try_from(failed_sequence)?,
        "MintEvent::MintingFailed",
        &failed_payload,
    )
    .await?;

    query_pool.close().await;
    Ok(())
}

/// Production race: first vault deposit mined, confirmation observation was
/// null / uncertain (RPC `NullResp` / timeout). Pre-fix the bot recorded
/// `MintingFailed` and prepared a **second** deposit (~1 minute later),
/// double-minting supply (incident: 0.750 wtPTY).
///
/// Correct behaviour:
/// 1. Uncertain confirm leaves `TxSubmitted` (no `MintingFailed`).
/// 2. Recovery re-observes the **same** prepared hash and completes callback.
/// 3. Exactly one on-chain `Deposit` and one `MintTxIntended` for the mint.
///
/// This e2e cannot inject a live null receipt from Anvil; it reproduces the
/// durable mid-flight state after a mined deposit whose confirm never recorded
/// `TokensMinted` (rollback to `MintTxSubmitted`), then proves restart recovery
/// finishes without a second deposit.
#[tokio::test]
async fn test_mined_deposit_with_unrecorded_confirm_must_not_double_mint()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir
        .path()
        .join("test_mined_deposit_unrecorded_confirm_no_double_mint.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = harness::setup_account(&client1, user_wallet).await;
    let issuer_request_id = harness::perform_mint_and_confirm(
        &client1,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-uncertain-confirm-no-double-mint",
        "50.0",
    )
    .await?;

    let user_wallet_instance = EthereumWallet::from(user_signer.clone());
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares_before = harness::wait_for_shares(&vault, user_wallet).await?;
    wait_for_event_count(
        &db_url,
        "Mint",
        &issuer_request_id,
        "MintEvent::MintCompleted",
        1,
    )
    .await?;
    assert_eq!(
        mint_callback_mock.calls_async().await,
        1,
        "the initial mint must persist completion after exactly one callback"
    );

    let deposits_before =
        count_vault_deposits(&user_provider, evm.vault_address).await?;
    assert_eq!(
        deposits_before, 1,
        "happy-path mint must create exactly one Deposit before rollback"
    );

    let intended_before = wait_for_event_count(
        &db_url,
        "Mint",
        &issuer_request_id,
        "MintEvent::MintTxIntended",
        1,
    )
    .await?;
    assert_eq!(intended_before, 1, "happy-path mint must prepare exactly once");

    client1.terminate().await;

    // Setup phase: event-store only. Drop TokensMinted/MintCompleted so the
    // aggregate is again `TxSubmitted` with the live prepared identity while
    // the on-chain deposit remains mined — the durable window after uncertain
    // confirm (or a crash before TokensMinted).
    let submitted_sequence =
        poison_mint_to_tx_submitted(&db_url, &issuer_request_id).await?;

    let remaining = sqlx::query_scalar::<_, i64>(
        "
        SELECT COUNT(*)
        FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
        ",
    )
    .bind(&issuer_request_id)
    .fetch_one(
        &SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?,
    )
    .await?;
    assert_eq!(
        remaining, submitted_sequence,
        "rollback must leave the mint at MintTxSubmitted (seq {submitted_sequence})"
    );

    // Restart: recovery re-enqueues ConfirmMint for the same hash.
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    let completed_count = wait_for_event_count(
        &db_url,
        "Mint",
        &issuer_request_id,
        "MintEvent::MintCompleted",
        1,
    )
    .await?;
    // Settle briefly then re-assert exact count so late extra callbacks fail.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        mint_callback_mock.calls_async().await,
        2,
        "recovery must deliver exactly one additional mint callback (no extras)"
    );

    assert_eq!(
        completed_count, 1,
        "recovery must complete the mint exactly once"
    );

    let intended_after = sqlx::query_scalar::<_, i64>(
        "
        SELECT COUNT(*)
        FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND event_type = 'MintEvent::MintTxIntended'
        ",
    )
    .bind(&issuer_request_id)
    .fetch_one(
        &SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?,
    )
    .await?;
    assert_eq!(
        intended_after, 1,
        "recovery must not prepare a second MintTxIntended (same hash only)"
    );

    let minting_failed_count = sqlx::query_scalar::<_, i64>(
        "
        SELECT COUNT(*)
        FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND event_type = 'MintEvent::MintingFailed'
        ",
    )
    .bind(&issuer_request_id)
    .fetch_one(
        &SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?,
    )
    .await?;
    assert_eq!(
        minting_failed_count, 0,
        "uncertain/unrecorded confirm recovery must not record MintingFailed"
    );

    let deposits_after =
        count_vault_deposits(&user_provider, evm.vault_address).await?;
    assert_eq!(
        deposits_after, 1,
        "must keep exactly one on-chain Deposit; got {deposits_after}"
    );

    let shares_after = vault.balanceOf(user_wallet).call().await?;
    assert_eq!(
        shares_after, shares_before,
        "share balance must not change (no double mint). \
         Before: {shares_before}, After: {shares_after}"
    );

    Ok(())
}

/// Same production class of failure, but the durable poison state is
/// `MintingFailed` after a mined deposit (what the old bot wrote when it
/// treated null receipt as terminal). Recovery must reclassify the live
/// prepared identity / inventory hit and complete without a second Deposit.
#[tokio::test]
async fn test_minting_failed_after_mined_deposit_must_not_double_mint()
-> Result<(), Box<dyn std::error::Error>> {
    let _recovery_test_guard = recovery_test_guard().await;

    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir
        .path()
        .join("test_minting_failed_after_mined_deposit_no_double_mint.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let mint_callback_mock = setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(&mock_alpaca);

    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = harness::setup_account(&client1, user_wallet).await;
    let issuer_request_id = harness::perform_mint_and_confirm(
        &client1,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-minting-failed-after-mined-no-double",
        "50.0",
    )
    .await?;

    let user_wallet_instance = EthereumWallet::from(user_signer.clone());
    let user_provider = create_provider()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares_before = harness::wait_for_shares(&vault, user_wallet).await?;
    wait_for_event_count(
        &db_url,
        "Mint",
        &issuer_request_id,
        "MintEvent::MintCompleted",
        1,
    )
    .await?;
    assert_eq!(
        mint_callback_mock.calls_async().await,
        1,
        "the initial mint must persist completion after exactly one callback"
    );

    let deposits_before =
        count_vault_deposits(&user_provider, evm.vault_address).await?;
    assert_eq!(deposits_before, 1);

    client1.terminate().await;

    poison_mint_to_minting_failed(&db_url, &issuer_request_id).await?;

    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    let completed_count = wait_for_event_count(
        &db_url,
        "Mint",
        &issuer_request_id,
        "MintEvent::MintCompleted",
        1,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        mint_callback_mock.calls_async().await,
        2,
        "MintingFailed recovery must deliver exactly one additional callback"
    );

    assert_eq!(
        completed_count, 1,
        "MintingFailed recovery must complete the mint exactly once"
    );

    let intended_after = sqlx::query_scalar::<_, i64>(
        "
        SELECT COUNT(*)
        FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND event_type = 'MintEvent::MintTxIntended'
        ",
    )
    .bind(&issuer_request_id)
    .fetch_one(
        &SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?,
    )
    .await?;
    assert_eq!(
        intended_after, 1,
        "MintingFailed recovery after a mined deposit must not prepare a second intent"
    );

    // Pins the other poison state: recovery that records a fresh failure and
    // then completes through inventory would still satisfy the intent count.
    let minting_failed_after = sqlx::query_scalar::<_, i64>(
        "
        SELECT COUNT(*)
        FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND event_type = 'MintEvent::MintingFailed'
        ",
    )
    .bind(&issuer_request_id)
    .fetch_one(
        &SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?,
    )
    .await?;
    assert_eq!(
        minting_failed_after, 1,
        "recovery must not append another MintingFailed to the seeded one"
    );

    let deposits_after =
        count_vault_deposits(&user_provider, evm.vault_address).await?;
    assert_eq!(
        deposits_after, 1,
        "must keep exactly one on-chain Deposit after MintingFailed recovery"
    );

    let shares_after = vault.balanceOf(user_wallet).call().await?;
    assert_eq!(
        shares_after, shares_before,
        "share balance must not change (no double mint)"
    );

    Ok(())
}
