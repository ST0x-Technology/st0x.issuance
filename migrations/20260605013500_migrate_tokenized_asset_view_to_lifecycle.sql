-- tokenized_asset_view now stores the event-sorcery
-- `Lifecycle<TokenizedAsset>` payload (`{"Live": {...}}`) instead of the legacy
-- cqrs-es `TokenizedAssetView` payload. Drop the old table (its stale
-- `$.Asset.*` indexes) and recreate it clean. The original table only carried
-- perf-only indexes (no UNIQUE constraint), so they are not re-created.
-- `StoreBuilder::build` rebuilds every row from the event log on startup via
-- the projection's catch-up, so no data is lost.
DROP TABLE IF EXISTS tokenized_asset_view;

CREATE TABLE tokenized_asset_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
