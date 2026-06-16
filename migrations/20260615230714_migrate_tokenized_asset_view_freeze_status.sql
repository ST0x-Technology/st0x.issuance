-- The TokenizedAsset view payload now carries `status` (AssetStatus: "Enabled"
-- / "Frozen") in place of the boolean `enabled`. Drop + recreate the view so
-- `StoreBuilder::build`'s catch_up rebuilds every row from the event log with
-- the new shape. The events table is unchanged — `originate` sets every asset
-- to `Enabled`, and freeze state comes from the new `Frozen` / `Unfrozen`
-- events. Existing rows carrying the old `enabled` field would otherwise fail
-- to deserialize into the new view struct.
DROP TABLE IF EXISTS tokenized_asset_view;

CREATE TABLE tokenized_asset_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
