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

UPDATE events
SET aggregate_id = aggregate_id || ':base'
WHERE aggregate_type = 'TokenizedAsset'
  AND aggregate_id != ''
  AND aggregate_id NOT GLOB '*:*';

UPDATE snapshots
SET aggregate_id = aggregate_id || ':base'
WHERE aggregate_type = 'TokenizedAsset'
  AND aggregate_id != ''
  AND aggregate_id NOT GLOB '*:*';

-- View ids mirrored aggregate ids pre-migration; drop so projection catch-up
-- rebuilds from the rekeyed event log (schema version bump also clears this).
DELETE FROM tokenized_asset_view;
