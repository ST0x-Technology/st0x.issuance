-- Rekey TokenizedAsset aggregate ids from underlying-only to `{underlying}:base`.
-- Assumption: legacy aggregate ids are bare equity tickers and never contain
-- ':', so any ':' means the row is already in AssetKey form; the NOT GLOB
-- guard is what makes re-runs idempotent. GLOB is case-sensitive (unlike LIKE),
-- so mixed-case suffixes such as `AAPL:BASE` are not treated as already rekeyed.
--
-- Guard the assumption instead of trusting it: a ':'-bearing id that is not a
-- rekeyed `{underlying}:base` key would be silently skipped by the updates
-- below and left permanently unreadable. Inserting any such id into this
-- CHECK-violating temp table aborts the migration transaction before any row
-- is touched.
CREATE TEMP TABLE rekey_precondition (
    unexpected_aggregate_id TEXT CHECK (unexpected_aggregate_id IS NULL)
);
INSERT INTO rekey_precondition (unexpected_aggregate_id)
SELECT aggregate_id
FROM events
WHERE aggregate_type = 'TokenizedAsset'
  AND (
    aggregate_id = ''
    OR (
      aggregate_id GLOB '*:*'
      AND aggregate_id NOT GLOB '*:base'
    )
    -- GLOB '*' matches the empty string, so a blank-underlying id such as
    -- ':base' would otherwise pass as "already rekeyed" despite being
    -- unparseable as an AssetKey.
    OR (
      aggregate_id GLOB '*:base'
      AND trim(substr(aggregate_id, 1, length(aggregate_id) - 5)) = ''
    )
  );
DROP TABLE rekey_precondition;

-- Corporate-action freeze is a property of the underlying equity, not of a
-- per-network listing, so `Frozen`/`Unfrozen` events move to the
-- underlying-keyed `Underlying` aggregate instead of being rekeyed to
-- `{underlying}:base`. Their aggregate_id is already the bare underlying (the
-- pre-multichain TokenizedAsset id) and their JSON payloads are unchanged —
-- only aggregate_type and the event_type prefix are rewritten. The sequence
-- offset parks the moved rows far above any real sequence so the
-- resequencing pass below can assign 1..N without transient primary-key
-- collisions (PK is (aggregate_type, aggregate_id, sequence)).
UPDATE events
SET aggregate_type = 'Underlying',
    event_type = replace(
        event_type, 'TokenizedAssetEvent::', 'UnderlyingEvent::'
    ),
    sequence = sequence + 1000000
WHERE aggregate_type = 'TokenizedAsset'
  AND event_type IN (
    'TokenizedAssetEvent::Frozen',
    'TokenizedAssetEvent::Unfrozen'
  );

-- Removing freeze events leaves gaps in the surviving TokenizedAsset streams,
-- and the moved Underlying streams start at offset+N rather than 1. Restore
-- contiguous 1..N sequences on both, preserving relative order. The rank is
-- computed into a temp table first: a correlated subquery against `events`
-- itself would observe rows already renumbered earlier in the same UPDATE,
-- making the result depend on SQLite's unspecified row-visit order. The
-- offset applied to the not-yet-resequenced TokenizedAsset rows guarantees no
-- target value (1..N) collides with any current value mid-update.
UPDATE events
SET sequence = sequence + 1000000
WHERE aggregate_type = 'TokenizedAsset'
  AND sequence < 1000000;

CREATE TEMP TABLE reseq AS
SELECT rowid AS event_rowid,
       ROW_NUMBER() OVER (
           PARTITION BY aggregate_type, aggregate_id ORDER BY sequence
       ) AS new_sequence
FROM events
WHERE aggregate_type IN ('TokenizedAsset', 'Underlying');

UPDATE events
SET sequence = (
    SELECT new_sequence FROM reseq WHERE event_rowid = events.rowid
)
WHERE rowid IN (SELECT event_rowid FROM reseq);

DROP TABLE reseq;

UPDATE events
SET aggregate_id = aggregate_id || ':base'
WHERE aggregate_type = 'TokenizedAsset'
  AND aggregate_id != ''
  AND aggregate_id NOT GLOB '*:*';

-- TokenizedAsset snapshots predate the freeze split, so their payloads carry
-- the now-removed `status` field; they are pure caches, so drop them and let
-- replay rebuild (the pre-migration wiring never wrote snapshots anyway).
DELETE FROM snapshots
WHERE aggregate_type = 'TokenizedAsset';

-- View ids mirrored aggregate ids pre-migration; drop so projection catch-up
-- rebuilds from the rekeyed event log (schema version bump also clears this).
DELETE FROM tokenized_asset_view;
