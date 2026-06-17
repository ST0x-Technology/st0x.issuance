#![allow(clippy::unwrap_used)]

//! Startup compatibility when the DB contains pre-event-sorcery snapshot or
//! canonical projection rows for any event-sourced aggregate.
//!
//! All five aggregates (`Mint`, `Redemption`, `Account`, `TokenizedAsset`,
//! `ReceiptInventory`) use `prepare_event_sourced_startup` before
//! `StoreBuilder::build`. Table-projected aggregates also need stale view
//! fixtures cleared before projection `catch_up`.
//!
//! Setup intentionally deviates from strict e2e rules (AGENTS.md): stale derived
//! rows are the fixtures under test, not the event store. Complements per-aggregate
//! `pre_lifecycle_*` unit tests in `src/*/mod.rs`.

mod harness;

use chrono::Utc;
use httpmock::prelude::*;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;

use harness::alpaca_mocks::{setup_mint_mocks, setup_redemption_mocks};
use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

const UNDERLYING: &str = "AAPL";
const TOKEN: &str = "tAAPL";

async fn open_seeded_db(
    db_name: &str,
    evm: &LocalEvm,
) -> Result<
    (tempfile::TempDir, String, sqlx::Pool<sqlx::Sqlite>),
    Box<dyn std::error::Error>,
> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join(db_name);
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        UNDERLYING,
        TOKEN,
    )
    .await?;

    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;

    Ok((temp_dir, db_url, pool))
}

async fn initialize_server(
    db_url: &str,
    mock_alpaca: &MockServer,
    evm: &LocalEvm,
) -> Result<(), Box<dyn std::error::Error>> {
    let _mint_callback_mock = setup_mint_mocks(mock_alpaca);
    let (_redeem_mock, _poll_mock) = setup_redemption_mocks(mock_alpaca);

    let (config, _mock_subgraph) =
        harness::create_config_with_db(db_url, mock_alpaca, evm)?;

    initialize_rocket(config).await?;

    Ok(())
}

#[tokio::test]
async fn startup_survives_pre_lifecycle_mint_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let mint_id = "550e8400-e29b-41d4-a716-446655440000";
    let (_temp_dir, db_url, pool) =
        open_seeded_db("startup_mint_snapshot.db", &evm).await?;
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (
            'Mint',
            ?,
            1,
            'MintEvent::Initiated',
            '1.0',
            ?,
            '{}'
        )
        ",
    )
    .bind(mint_id)
    .bind(
        json!({
            "Initiated": {
                "issuer_request_id": mint_id,
                "tokenization_request_id": "tok-pre-lifecycle",
                "quantity": "10.0",
                "underlying": UNDERLYING,
                "token": TOKEN,
                "network": "base",
                "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "wallet": "0x1234567890123456789012345678901234567890",
                "initiated_at": now,
            }
        })
        .to_string(),
    )
    .execute(&pool)
    .await?;

    harness::seed_schema_registry_version(&pool, "Mint", 1).await?;
    harness::seed_pre_lifecycle_snapshot(
        &pool,
        "Mint",
        mint_id,
        1,
        &json!({
            "Completed": {
                "issuer_request_id": mint_id,
                "tokenization_request_id": "tok-pre-lifecycle",
                "quantity": "10.0",
                "underlying": UNDERLYING,
                "token": TOKEN,
                "network": "base",
                "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "wallet": "0x1234567890123456789012345678901234567890",
                "initiated_at": now,
                "journal_confirmed_at": now,
                "tx_hash": "0xbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00d",
                "receipt_id": "1",
                "shares_minted": "1000000000000000000",
                "gas_used": 21000,
                "block_number": 1,
                "minted_at": now,
                "completed_at": now,
            }
        }),
    )
    .await?;

    pool.close().await;
    initialize_server(&db_url, &mock_alpaca, &evm).await?;

    Ok(())
}

#[tokio::test]
async fn startup_survives_pre_lifecycle_mint_view()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let mint_id = "550e8400-e29b-41d4-a716-446655440001";
    let (_temp_dir, db_url, pool) =
        open_seeded_db("startup_mint_view.db", &evm).await?;
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (
            'Mint',
            ?,
            1,
            'MintEvent::Initiated',
            '1.0',
            ?,
            '{}'
        )
        ",
    )
    .bind(mint_id)
    .bind(
        json!({
            "Initiated": {
                "issuer_request_id": mint_id,
                "tokenization_request_id": "tok-pre-lifecycle-view",
                "quantity": "10.0",
                "underlying": UNDERLYING,
                "token": TOKEN,
                "network": "base",
                "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "wallet": "0x1234567890123456789012345678901234567890",
                "initiated_at": now,
            }
        })
        .to_string(),
    )
    .execute(&pool)
    .await?;

    let completed_payload = json!({
        "Completed": {
            "issuer_request_id": mint_id,
            "tokenization_request_id": "tok-pre-lifecycle-view",
            "quantity": "10.0",
            "underlying": UNDERLYING,
            "token": TOKEN,
            "network": "base",
            "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "wallet": "0x1234567890123456789012345678901234567890",
            "initiated_at": now,
            "journal_confirmed_at": now,
            "tx_hash": "0xbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00d",
            "receipt_id": "1",
            "shares_minted": "1000000000000000000",
            "gas_used": 21000,
            "block_number": 1,
            "minted_at": now,
            "completed_at": now,
        }
    });

    harness::seed_pre_lifecycle_view_row(
        &pool,
        "mint_view",
        mint_id,
        1,
        &completed_payload,
    )
    .await?;
    harness::seed_schema_registry_version(&pool, "Mint", 1).await?;

    pool.close().await;
    initialize_server(&db_url, &mock_alpaca, &evm).await?;

    Ok(())
}

#[tokio::test]
async fn startup_survives_pre_lifecycle_redemption_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let redemption_id =
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let (_temp_dir, db_url, pool) =
        open_seeded_db("startup_redemption_snapshot.db", &evm).await?;
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (
            'Redemption',
            ?,
            1,
            'RedemptionEvent::Detected',
            '1.0',
            ?,
            '{}'
        )
        ",
    )
    .bind(redemption_id)
    .bind(
        json!({
            "Detected": {
                "issuer_request_id": redemption_id,
                "underlying": UNDERLYING,
                "token": TOKEN,
                "wallet": "0x1234567890123456789012345678901234567890",
                "quantity": "1.0",
                "tx_hash": redemption_id,
                "block_number": 1,
                "detected_at": now,
            }
        })
        .to_string(),
    )
    .execute(&pool)
    .await?;

    harness::seed_schema_registry_version(&pool, "Redemption", 1).await?;
    harness::seed_pre_lifecycle_snapshot(
        &pool,
        "Redemption",
        redemption_id,
        1,
        &json!({
            "Completed": {
                "issuer_request_id": redemption_id,
                "burn_tx_hash": redemption_id,
                "completed_at": now,
            }
        }),
    )
    .await?;

    pool.close().await;
    initialize_server(&db_url, &mock_alpaca, &evm).await?;

    Ok(())
}

#[tokio::test]
async fn startup_survives_pre_lifecycle_account_snapshot_and_view()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let client_id = "6ba7b810-9dad-11d1-80b4-00c04fd430c9";
    let (_temp_dir, db_url, pool) =
        open_seeded_db("startup_account_snapshot.db", &evm).await?;
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "
        INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (
            'Account',
            ?,
            1,
            'AccountEvent::Registered',
            '1.0',
            ?,
            '{}'
        )
        ",
    )
    .bind(client_id)
    .bind(
        json!({
            "Registered": {
                "client_id": client_id,
                "email": "user@example.com",
                "registered_at": now,
            }
        })
        .to_string(),
    )
    .execute(&pool)
    .await?;

    let stale = json!({
        "LinkedToAlpaca": {
            "client_id": client_id,
            "email": "user@example.com",
            "alpaca_account": "ALPACA-1",
            "whitelisted_wallets": [],
            "registered_at": now,
            "linked_at": now,
        }
    });

    harness::seed_schema_registry_version(&pool, "Account", 1).await?;
    harness::seed_pre_lifecycle_snapshot(
        &pool, "Account", client_id, 1, &stale,
    )
    .await?;
    harness::seed_pre_lifecycle_view_row(
        &pool,
        "account_view",
        client_id,
        1,
        &stale,
    )
    .await?;

    pool.close().await;
    initialize_server(&db_url, &mock_alpaca, &evm).await?;

    Ok(())
}

#[tokio::test]
async fn startup_survives_pre_lifecycle_tokenized_asset_snapshot_and_view()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let (_temp_dir, db_url, pool) =
        open_seeded_db("startup_tokenized_asset_snapshot.db", &evm).await?;
    let now = Utc::now().to_rfc3339();
    let vault = evm.vault_address;

    let stale = json!({
        "underlying": UNDERLYING,
        "token": TOKEN,
        "network": "base",
        "vault": vault,
        "status": "Enabled",
        "added_at": now,
    });

    harness::seed_schema_registry_version(&pool, "TokenizedAsset", 1).await?;
    harness::seed_pre_lifecycle_snapshot(
        &pool,
        "TokenizedAsset",
        UNDERLYING,
        1,
        &stale,
    )
    .await?;
    harness::seed_pre_lifecycle_view_row(
        &pool,
        "tokenized_asset_view",
        UNDERLYING,
        1,
        &stale,
    )
    .await?;

    pool.close().await;
    initialize_server(&db_url, &mock_alpaca, &evm).await?;

    Ok(())
}

#[tokio::test]
async fn startup_survives_pre_lifecycle_receipt_inventory_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let (_temp_dir, db_url, pool) =
        open_seeded_db("startup_receipt_inventory_snapshot.db", &evm).await?;
    let aggregate_id = format!("{:#x}", evm.vault_address);

    harness::seed_schema_registry_version(&pool, "ReceiptInventory", 1).await?;
    harness::seed_pre_lifecycle_snapshot(
        &pool,
        "ReceiptInventory",
        &aggregate_id,
        1,
        &json!({
            "receipts": {},
            "itn_receipts": {},
        }),
    )
    .await?;

    pool.close().await;
    initialize_server(&db_url, &mock_alpaca, &evm).await?;

    Ok(())
}
