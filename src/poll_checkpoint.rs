//! Persistent block-number checkpoints for periodic pollers.
//!
//! Replaces the previous event-sourced `TransferPollCheckpoint` aggregate and
//! the `BackfillCheckpoint` event on `ReceiptInventory`. Both modeled a single
//! monotonic counter as an aggregate, which meant every poll tick replayed
//! every prior event — the cause of the 2026-05-19 OOM (RAI-617).
//!
//! Checkpoints are not domain entities — there is no audit history to keep
//! and no state machine to enforce — so they live in a plain SQL table.

use alloy::primitives::Address;
use sqlx::{Pool, Sqlite};

/// Checkpoint name for the global transfer poller.
pub(crate) const TRANSFER_POLL: &str = "transfer_poll";

/// Checkpoint name for the receipt backfiller for a given vault.
pub(crate) fn receipt_backfill_name(vault: Address) -> String {
    format!("receipt_backfill:{vault:#x}")
}

/// Returns the highest block number recorded for `name`, or `None` if no
/// checkpoint has been written yet.
pub(crate) async fn load(
    pool: &Pool<Sqlite>,
    name: &str,
) -> Result<Option<u64>, CheckpointError> {
    let row = sqlx::query_as::<_, (i64,)>(
        "SELECT block_number FROM poll_checkpoints WHERE name = ?",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;

    row.map(|(block,)| u64::try_from(block).map_err(Into::into)).transpose()
}

/// Advances `name` to `block_number`, but only if the new value is strictly
/// greater than the existing one. Older or equal values are ignored, matching
/// the monotonic semantics of the aggregates this replaces.
pub(crate) async fn advance(
    pool: &Pool<Sqlite>,
    name: &str,
    block_number: u64,
) -> Result<(), CheckpointError> {
    let block_signed = i64::try_from(block_number)?;

    sqlx::query(
        "
        INSERT INTO poll_checkpoints (name, block_number)
        VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE
            SET block_number = excluded.block_number,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE excluded.block_number > poll_checkpoints.block_number
        ",
    )
    .bind(name)
    .bind(block_signed)
    .execute(pool)
    .await?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CheckpointError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("block_number out of i64 range: {0}")]
    Range(#[from] std::num::TryFromIntError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn setup_pool() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn load_returns_none_when_unset() {
        let pool = setup_pool().await;
        assert_eq!(load(&pool, TRANSFER_POLL).await.unwrap(), None);
    }

    #[tokio::test]
    async fn advance_sets_the_value() {
        let pool = setup_pool().await;
        advance(&pool, TRANSFER_POLL, 100).await.unwrap();
        assert_eq!(load(&pool, TRANSFER_POLL).await.unwrap(), Some(100));
    }

    #[tokio::test]
    async fn advance_is_monotonic() {
        let pool = setup_pool().await;
        advance(&pool, TRANSFER_POLL, 100).await.unwrap();
        advance(&pool, TRANSFER_POLL, 80).await.unwrap();
        assert_eq!(load(&pool, TRANSFER_POLL).await.unwrap(), Some(100));
    }

    #[tokio::test]
    async fn advance_to_same_block_is_noop() {
        let pool = setup_pool().await;
        advance(&pool, TRANSFER_POLL, 100).await.unwrap();
        advance(&pool, TRANSFER_POLL, 100).await.unwrap();
        assert_eq!(load(&pool, TRANSFER_POLL).await.unwrap(), Some(100));
    }

    #[tokio::test]
    async fn sequential_advances_grow_monotonically() {
        let pool = setup_pool().await;
        advance(&pool, TRANSFER_POLL, 100).await.unwrap();
        advance(&pool, TRANSFER_POLL, 200).await.unwrap();
        advance(&pool, TRANSFER_POLL, 350).await.unwrap();
        assert_eq!(load(&pool, TRANSFER_POLL).await.unwrap(), Some(350));
    }

    #[tokio::test]
    async fn multiple_names_are_independent() {
        let pool = setup_pool().await;
        let vault_a = address!("00000000000000000000000000000000000000aa");
        let vault_b = address!("00000000000000000000000000000000000000bb");
        advance(&pool, &receipt_backfill_name(vault_a), 100).await.unwrap();
        advance(&pool, &receipt_backfill_name(vault_b), 500).await.unwrap();
        advance(&pool, TRANSFER_POLL, 999).await.unwrap();

        assert_eq!(
            load(&pool, &receipt_backfill_name(vault_a)).await.unwrap(),
            Some(100)
        );
        assert_eq!(
            load(&pool, &receipt_backfill_name(vault_b)).await.unwrap(),
            Some(500)
        );
        assert_eq!(load(&pool, TRANSFER_POLL).await.unwrap(), Some(999));
    }

    #[tokio::test]
    async fn receipt_backfill_name_uses_lowercase_hex() {
        let vault = address!("AaBbCcDdEeFf00112233445566778899aAbBcCdD");
        assert_eq!(
            receipt_backfill_name(vault),
            "receipt_backfill:0xaabbccddeeff00112233445566778899aabbccdd"
        );
    }

    /// Production aggregate IDs were written via `Address::to_string()`, which
    /// produces EIP-55 mixed-case hex. The migration that seeds
    /// `poll_checkpoints` from existing `BackfillCheckpoint` events must
    /// normalize that to lowercase so the seeded row matches the runtime key
    /// built by `receipt_backfill_name`.
    #[tokio::test]
    async fn migration_seeds_receipt_backfill_with_lowercase_key() {
        let pool = setup_pool().await;
        let vault = address!("AaBbCcDdEeFf00112233445566778899aAbBcCdD");
        let mixed_case_aggregate_id = vault.to_string();
        assert_ne!(
            mixed_case_aggregate_id,
            format!("{vault:#x}"),
            "test precondition: aggregate_id format must differ in case from \
             runtime key — otherwise this test trivially passes"
        );

        // Wipe the seeded state and pretend the migration is running against
        // events that already exist.
        sqlx::query("DELETE FROM poll_checkpoints")
            .execute(&pool)
            .await
            .unwrap();
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
                'ReceiptInventory',
                ?,
                1,
                'ReceiptInventoryEvent::BackfillCheckpoint',
                '1.0',
                '{\"BackfillCheckpoint\":{\"block_number\":12345}}',
                '{}'
            )
            ",
        )
        .bind(&mixed_case_aggregate_id)
        .execute(&pool)
        .await
        .unwrap();

        // Re-run the receipt_backfill seeding step verbatim from the
        // migration. If the SQL is ever changed and no longer matches the
        // runtime key format, this assertion fails.
        sqlx::query(
            "
            INSERT INTO poll_checkpoints (name, block_number)
            SELECT
                'receipt_backfill:' || lower(aggregate_id),
                MAX(CAST(
                    json_extract(payload, '$.BackfillCheckpoint.block_number')
                    AS INTEGER
                ))
            FROM events
            WHERE aggregate_type = 'ReceiptInventory'
              AND event_type = 'ReceiptInventoryEvent::BackfillCheckpoint'
            GROUP BY lower(aggregate_id)
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            load(&pool, &receipt_backfill_name(vault)).await.unwrap(),
            Some(12345),
            "seeded checkpoint must be readable via the runtime key"
        );
    }
}
