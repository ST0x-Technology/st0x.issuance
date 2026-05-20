-- Replaces two event-sourced checkpoint aggregates with a single SQL table.
--
-- Background: TransferPollCheckpoint and ReceiptInventory.BackfillCheckpoint
-- were modeled as event-sourced aggregates. Each 5s poll appended a new event,
-- and every load_aggregate call replayed all prior events. After 5 days the
-- bot was reading 565K events per poll tick, driving 30 MB/s of disk reads
-- and ultimately the 2026-05-19 OOM (RAI-617). These values are simple
-- mutable counters with no domain-meaningful history.

CREATE TABLE poll_checkpoints (
    name TEXT PRIMARY KEY,
    block_number INTEGER NOT NULL CHECK (block_number >= 0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Seed the new table from existing event payloads. MAX() preserves the
-- monotonic-counter semantics from both aggregates.

INSERT INTO poll_checkpoints (name, block_number)
SELECT
    'transfer_poll',
    MAX(CAST(json_extract(payload, '$.Advanced.block_number') AS INTEGER))
FROM events
WHERE aggregate_type = 'TransferPollCheckpoint'
  AND event_type = 'TransferPollCheckpointEvent::Advanced'
HAVING COUNT(*) > 0;

-- Aggregate IDs were written via `Address::to_string()` (EIP-55 mixed case).
-- Runtime lookups use `receipt_backfill_name(vault)` which formats with
-- `{:#x}` (lowercase hex). Normalize here so the seeded rows match.
INSERT INTO poll_checkpoints (name, block_number)
SELECT
    'receipt_backfill:' || lower(aggregate_id),
    MAX(CAST(json_extract(payload, '$.BackfillCheckpoint.block_number') AS INTEGER))
FROM events
WHERE aggregate_type = 'ReceiptInventory'
  AND event_type = 'ReceiptInventoryEvent::BackfillCheckpoint'
GROUP BY lower(aggregate_id);

-- Delete the now-orphan checkpoint events. The remaining ReceiptInventory
-- events (Discovered, BalanceReconciled, Depleted) are legitimate domain
-- state and stay event-sourced.

DELETE FROM events
WHERE aggregate_type = 'TransferPollCheckpoint';

DELETE FROM events
WHERE aggregate_type = 'ReceiptInventory'
  AND event_type = 'ReceiptInventoryEvent::BackfillCheckpoint';

DELETE FROM snapshots
WHERE aggregate_type = 'TransferPollCheckpoint';

-- redemption_backfill_checkpoints predates RAI-487 (the eth_subscribe ->
-- eth_getLogs migration). It was kept as a one-time bridge for
-- seed_checkpoint_from_legacy and is no longer referenced.

DROP TABLE IF EXISTS redemption_backfill_checkpoints;
