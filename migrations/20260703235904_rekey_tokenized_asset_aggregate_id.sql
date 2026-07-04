-- Rekey TokenizedAsset aggregate ids from underlying-only to `{underlying}:base`.
-- Assumption: legacy aggregate ids are bare equity tickers and never contain
-- ':', so any ':' means the row is already in AssetKey form; the NOT LIKE
-- guard is what makes re-runs idempotent.
UPDATE events
SET aggregate_id = aggregate_id || ':base'
WHERE aggregate_type = 'TokenizedAsset'
  AND aggregate_id NOT LIKE '%:%';

UPDATE snapshots
SET aggregate_id = aggregate_id || ':base'
WHERE aggregate_type = 'TokenizedAsset'
  AND aggregate_id NOT LIKE '%:%';

-- View ids mirrored aggregate ids pre-migration; drop so projection catch-up
-- rebuilds from the rekeyed event log (schema version bump also clears this).
DELETE FROM tokenized_asset_view;
