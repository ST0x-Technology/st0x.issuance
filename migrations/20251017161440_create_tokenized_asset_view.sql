-- Tokenized asset view: current supported assets
CREATE TABLE IF NOT EXISTS tokenized_asset_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tokenized_asset_view_enabled
    ON tokenized_asset_view(json_extract(payload, '$.Asset.enabled'));
CREATE INDEX IF NOT EXISTS idx_tokenized_asset_view_network
    ON tokenized_asset_view(json_extract(payload, '$.Asset.network'));
