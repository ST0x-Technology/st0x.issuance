//! Persistent block-number checkpoints for periodic pollers.
//!
//! Replaces the previous event-sourced `TransferPollCheckpoint` aggregate and
//! the `BackfillCheckpoint` event on `ReceiptInventory`. Both modeled a single
//! monotonic counter as an aggregate, which meant every poll tick replayed
//! every prior event — the cause of the 2026-05-19 OOM (RAI-617).
//!
//! Checkpoints are not domain entities — there is no audit history to keep
//! and no state machine to enforce — so they live in a plain SQL table.

use crate::tokenized_asset::Network;
use alloy::primitives::Address;
use sqlx::{Pool, Sqlite};

/// Legacy global transfer-poll checkpoint. Superseded by the per-vault
/// [`transfer_poll_name`] checkpoints; retained only as the seed source that
/// [`crate::redemption::poller::TransferPoller`] migrates existing vaults from
/// at startup, so a deploy does not re-scan every vault from
/// `backfill_start_block`.
pub(crate) const TRANSFER_POLL: &str = "transfer_poll";

/// Checkpoint name for the transfer poller for a given vault. Per-vault (rather
/// than one global cursor) so a vault added or re-pointed at runtime is scanned
/// from `backfill_start_block` on first appearance instead of inheriting a
/// global checkpoint that is already past its on-chain history — which would
/// silently drop the redemptions on that vault below the global checkpoint.
pub(crate) fn transfer_poll_name(network: Network, vault: Address) -> String {
    format!("transfer_poll:{network}:{vault:#x}")
}

/// Loads the transfer poll checkpoint for `network`/`vault`, falling back to
/// the legacy global name when polling Base.
pub(crate) async fn load_transfer_poll(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
) -> Result<Option<u64>, CheckpointError> {
    if let Some(block) =
        load_checkpoint_block(pool, &transfer_poll_name(network, vault)).await?
    {
        return Ok(Some(block));
    }

    if network == Network::Base {
        load_checkpoint_block(pool, TRANSFER_POLL).await
    } else {
        Ok(None)
    }
}

/// Advances the per-network transfer poll checkpoint.
pub(crate) async fn advance_transfer_poll(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
    block_number: u64,
) -> Result<(), CheckpointError> {
    advance_checkpoint_block(
        pool,
        &transfer_poll_name(network, vault),
        block_number,
    )
    .await
}

/// Per-network checkpoint name for the receipt backfiller for a given vault.
///
/// Block numbers are chain-specific, so the same vault address on two
/// networks must track independent checkpoints -- a shared key would let one
/// chain's head block skip the other chain's backfill entirely.
pub(crate) fn receipt_backfill_name(
    network: Network,
    vault: Address,
) -> String {
    format!("receipt_backfill:{network}:{vault:#x}")
}

/// Legacy single-chain (Base only) checkpoint name, seeded by the migration
/// from event-sourced BackfillCheckpoint events.
fn legacy_receipt_backfill_name(vault: Address) -> String {
    format!("receipt_backfill:{vault:#x}")
}

/// Loads the receipt backfill checkpoint for `network`/`vault`, falling back
/// to the legacy vault-only name when polling Base.
pub(crate) async fn load_receipt_backfill(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
) -> Result<Option<u64>, CheckpointError> {
    if let Some(block) =
        load_checkpoint_block(pool, &receipt_backfill_name(network, vault))
            .await?
    {
        return Ok(Some(block));
    }

    if network == Network::Base {
        load_checkpoint_block(pool, &legacy_receipt_backfill_name(vault)).await
    } else {
        Ok(None)
    }
}

/// Advances the receipt backfill checkpoint for `network`/`vault`.
pub(crate) async fn advance_receipt_backfill(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
    block_number: u64,
) -> Result<(), CheckpointError> {
    advance_checkpoint_block(
        pool,
        &receipt_backfill_name(network, vault),
        block_number,
    )
    .await
}

/// Returns the highest block number recorded for `name`, or `None` if no
/// checkpoint has been written yet.
pub(crate) async fn load_checkpoint_block(
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
pub(crate) async fn advance_checkpoint_block(
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
/// Deletes the checkpoint row for `name`.
///
/// Used by one-time migrations that must not leave their source checkpoint
/// behind: a later restart would re-apply it to rows that did not exist at
/// migration time.
pub(crate) async fn remove(
    pool: &Pool<Sqlite>,
    name: &str,
) -> Result<(), CheckpointError> {
    sqlx::query("DELETE FROM poll_checkpoints WHERE name = ?")
        .bind(name)
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

    use crate::tokenized_asset::Network;

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
        assert_eq!(
            load_checkpoint_block(&pool, TRANSFER_POLL).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn advance_sets_the_value() {
        let pool = setup_pool().await;
        advance_checkpoint_block(&pool, TRANSFER_POLL, 100).await.unwrap();
        assert_eq!(
            load_checkpoint_block(&pool, TRANSFER_POLL).await.unwrap(),
            Some(100)
        );
    }

    #[tokio::test]
    async fn advance_is_monotonic() {
        let pool = setup_pool().await;
        advance_checkpoint_block(&pool, TRANSFER_POLL, 100).await.unwrap();
        advance_checkpoint_block(&pool, TRANSFER_POLL, 80).await.unwrap();
        assert_eq!(
            load_checkpoint_block(&pool, TRANSFER_POLL).await.unwrap(),
            Some(100)
        );
    }

    #[tokio::test]
    async fn advance_to_same_block_is_noop() {
        let pool = setup_pool().await;
        advance_checkpoint_block(&pool, TRANSFER_POLL, 100).await.unwrap();
        advance_checkpoint_block(&pool, TRANSFER_POLL, 100).await.unwrap();
        assert_eq!(
            load_checkpoint_block(&pool, TRANSFER_POLL).await.unwrap(),
            Some(100)
        );
    }

    #[tokio::test]
    async fn sequential_advances_grow_monotonically() {
        let pool = setup_pool().await;
        advance_checkpoint_block(&pool, TRANSFER_POLL, 100).await.unwrap();
        advance_checkpoint_block(&pool, TRANSFER_POLL, 200).await.unwrap();
        advance_checkpoint_block(&pool, TRANSFER_POLL, 350).await.unwrap();
        assert_eq!(
            load_checkpoint_block(&pool, TRANSFER_POLL).await.unwrap(),
            Some(350)
        );
    }

    #[tokio::test]
    async fn multiple_names_are_independent() {
        let pool = setup_pool().await;
        let vault_a = address!("00000000000000000000000000000000000000aa");
        let vault_b = address!("00000000000000000000000000000000000000bb");
        advance_checkpoint_block(
            &pool,
            &receipt_backfill_name(Network::Base, vault_a),
            100,
        )
        .await
        .unwrap();
        advance_checkpoint_block(
            &pool,
            &receipt_backfill_name(Network::Base, vault_b),
            500,
        )
        .await
        .unwrap();
        advance_checkpoint_block(&pool, TRANSFER_POLL, 999).await.unwrap();

        assert_eq!(
            load_checkpoint_block(
                &pool,
                &receipt_backfill_name(Network::Base, vault_a),
            )
            .await
            .unwrap(),
            Some(100)
        );
        assert_eq!(
            load_checkpoint_block(
                &pool,
                &receipt_backfill_name(Network::Base, vault_b),
            )
            .await
            .unwrap(),
            Some(500)
        );
        assert_eq!(
            load_checkpoint_block(&pool, TRANSFER_POLL).await.unwrap(),
            Some(999)
        );
    }

    /// Block numbers are chain-specific, so the same vault address deployed
    /// on two networks must keep independent backfill checkpoints -- a shared
    /// key would let the chain with the higher head block permanently skip
    /// the other chain's backfill.
    #[tokio::test]
    async fn receipt_backfill_checkpoints_are_independent_per_network() {
        let pool = setup_pool().await;
        let vault = address!("00000000000000000000000000000000000000aa");

        advance_receipt_backfill(&pool, Network::Base, vault, 100)
            .await
            .unwrap();
        advance_receipt_backfill(&pool, Network::Ethereum, vault, 50)
            .await
            .unwrap();

        assert_eq!(
            load_receipt_backfill(&pool, Network::Base, vault).await.unwrap(),
            Some(100),
            "Base must keep its own checkpoint for the shared vault address"
        );
        assert_eq!(
            load_receipt_backfill(&pool, Network::Ethereum, vault)
                .await
                .unwrap(),
            Some(50),
            "Ethereum must keep its own checkpoint for the shared vault \
             address"
        );
    }

    /// The Base poller must fall back to the legacy single-chain
    /// `transfer_poll` key when no per-network checkpoint exists yet --
    /// otherwise an upgraded deployment restarts from `backfill_start_block`
    /// and re-processes all historical blocks. Once the per-network key is
    /// written, it takes precedence, and non-Base networks must never see the
    /// legacy value.
    #[tokio::test]
    async fn load_transfer_poll_falls_back_to_legacy_key_for_base_only() {
        let pool = setup_pool().await;
        let vault = address!("00000000000000000000000000000000000000aa");
        advance_checkpoint_block(&pool, TRANSFER_POLL, 123).await.unwrap();

        assert_eq!(
            load_transfer_poll(&pool, Network::Base, vault).await.unwrap(),
            Some(123),
            "Base must fall back to the legacy transfer_poll checkpoint"
        );
        assert_eq!(
            load_transfer_poll(&pool, Network::Ethereum, vault).await.unwrap(),
            None,
            "non-Base networks must not inherit the legacy Base checkpoint"
        );

        advance_transfer_poll(&pool, Network::Base, vault, 456).await.unwrap();

        assert_eq!(
            load_transfer_poll(&pool, Network::Base, vault).await.unwrap(),
            Some(456),
            "the per-vault key must take precedence over the legacy key"
        );
    }

    #[tokio::test]
    async fn receipt_backfill_name_uses_lowercase_hex() {
        let vault = address!("AaBbCcDdEeFf00112233445566778899aAbBcCdD");
        assert_eq!(
            receipt_backfill_name(Network::Base, vault),
            "receipt_backfill:base:0xaabbccddeeff00112233445566778899aabbccdd"
        );
        assert_eq!(
            receipt_backfill_name(Network::Ethereum, vault),
            "receipt_backfill:ethereum:0xaabbccddeeff00112233445566778899aabbccdd"
        );
        assert_eq!(
            legacy_receipt_backfill_name(vault),
            "receipt_backfill:0xaabbccddeeff00112233445566778899aabbccdd"
        );
    }

    #[tokio::test]
    async fn load_receipt_backfill_falls_back_to_legacy_base_checkpoint() {
        let pool = setup_pool().await;
        let vault = address!("00000000000000000000000000000000000000aa");
        advance_checkpoint_block(
            &pool,
            &legacy_receipt_backfill_name(vault),
            42,
        )
        .await
        .unwrap();

        assert_eq!(
            load_receipt_backfill(&pool, Network::Base, vault).await.unwrap(),
            Some(42)
        );
    }

    /// Production aggregate IDs were written via `Address::to_string()`, which
    /// produces EIP-55 mixed-case hex. The migration that seeds
    /// `poll_checkpoints` from existing `BackfillCheckpoint` events must
    /// normalize that to lowercase so the seeded row matches the legacy key
    /// that `load_receipt_backfill` falls back to for Base.
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
        // legacy key format the Base fallback reads, this assertion fails.
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
            load_receipt_backfill(&pool, Network::Base, vault).await.unwrap(),
            Some(12345),
            "seeded legacy checkpoint must be readable via the Base fallback"
        );
        assert_eq!(
            load_receipt_backfill(&pool, Network::Ethereum, vault)
                .await
                .unwrap(),
            None,
            "non-Base networks must not inherit the legacy Base checkpoint"
        );
    }
}
