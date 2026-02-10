#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, Bytes, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use uuid::{Uuid, uuid};

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

async fn insert_mint_event(
    pool: &sqlx::Pool<sqlx::Sqlite>,
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
        VALUES ('Mint', ?, ?, ?, '1.0', ?, '{}')
        "#,
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
    insert_mint_event(
        pool,
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
    insert_mint_event(
        pool,
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
    insert_mint_event(
        pool,
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
/// Background: The ordering bug this catches:
/// 1. Service starts
/// 2. View table is empty (deleted or corrupted)
/// 3. Recovery runs BEFORE view reprojection
/// 4. Recovery queries the view for stuck mints
/// 5. Recovery queries empty view, misses the stuck mint
///
/// The fix: Reprojections must run BEFORE recovery.
///
/// To create a mint stuck in JournalConfirmed state, we:
/// 1. Complete a mint normally (all the way through)
/// 2. Delete events after JournalConfirmed to roll back the aggregate state
/// 3. Delete the view
/// 4. Restart - recovery should find the JournalConfirmed mint
#[tokio::test]
async fn test_mint_recovery_after_view_deletion()
-> Result<(), Box<dyn std::error::Error>> {
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
    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca, user_wallet);

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
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    harness::wait_for_shares(&vault, user_wallet).await?;

    // "Crash" - drop the service
    drop(client1);

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

    // Delete the mint view to force reprojection
    sqlx::query!("DELETE FROM mint_view").execute(&query_pool).await?;

    // Verify view is empty
    let view_count =
        sqlx::query!(r#"SELECT COUNT(*) as "count!: i32" FROM mint_view"#)
            .fetch_one(&query_pool)
            .await?;
    assert_eq!(view_count.count, 0, "Mint view should be empty after deletion");

    // Close the pool before restarting
    query_pool.close().await;

    // Restart service - recovery should find the JournalConfirmed mint.
    // initialize_rocket replays views (replay_mint_view, replay_redemption_view,
    // replay_receipt_burns_view) before spawning recovery, so the mint view
    // is repopulated before recovery queries it.
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // If recovery worked, the mint should complete and user gets shares
    // Note: The on-chain mint already happened, but we deleted the events,
    // so recovery will attempt to mint again. This might fail due to
    // duplicate mint, but that's a separate issue (Task 7-9 handles this).
    // For now, we just want to verify recovery FINDS the stuck mint.

    // Give recovery time to run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

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

    Ok(())
}

/// Tests recovery from `Minting` state when the on-chain mint already succeeded.
///
/// Scenario:
/// 1. Complete a mint normally (creates on-chain receipt)
/// 2. Roll back event store to `Minting` state (delete TokensMinted and later events)
/// 3. Delete view
/// 4. Restart service
/// 5. Recovery detects the existing receipt via receipt inventory and records
///    mint success without re-minting
#[tokio::test]
async fn test_mint_recovery_from_minting_state_when_receipt_exists()
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
    let db_path =
        temp_dir.path().join("test_minting_recovery_receipt_exists.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca, user_wallet);

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
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares_before = harness::wait_for_shares(&vault, user_wallet).await?;

    drop(client1);

    let query_pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    // Roll back to Minting state: keep events 1-3 (Initiated, JournalConfirmed, MintingStarted)
    let aggregate_id = issuer_request_id.clone();
    sqlx::query!(
        r#"
        DELETE FROM events
        WHERE aggregate_type = 'Mint'
          AND aggregate_id = ?
          AND sequence > 3
        "#,
        aggregate_id
    )
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
        "Expected exactly 3 events (Initiated, JournalConfirmed, MintingStarted)"
    );

    sqlx::query!("DELETE FROM mint_view").execute(&query_pool).await?;
    query_pool.close().await;

    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

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

    // Recovery should have added TokensMinted event (recognizing the existing receipt)
    // and possibly MintCompleted after callback
    assert!(
        final_event_count.count > 3,
        "Recovery should have processed the Minting mint and added events. \
         Expected >3 events, found {}. Recovery for Minting state not implemented.",
        final_event_count.count
    );

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

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca, user_wallet);

    // First start the service to set up the database schema and seed data
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Setup account to get a valid client_id
    let link_body = harness::setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    drop(client1);

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
    let user_provider = ProviderBuilder::new()
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

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca, user_wallet);

    // Start service to set up database
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    let link_body = harness::setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    drop(client1);

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
    let user_provider = ProviderBuilder::new()
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

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca, user_wallet);

    // Start service to set up database schema
    let config1 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket1 = initialize_rocket(config1).await?;
    let client1 = rocket::local::asynchronous::Client::tracked(rocket1).await?;

    harness::seed_tokenized_asset(&client1, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let link_body = harness::setup_account(&client1, user_wallet).await;
    let client_id = link_body.client_id;

    drop(client1);

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

    // Event 4: MintingFailed
    let failed_payload = json!({
        "MintingFailed": {
            "issuer_request_id": issuer_request_id,
            "error": "Simulated blockchain error",
            "failed_at": chrono::Utc::now().to_rfc3339()
        }
    });
    insert_mint_event(
        &query_pool,
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
    let user_provider = ProviderBuilder::new()
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

    Ok(())
}
