-- Create redemption view table for tracking redemption state
CREATE TABLE redemption_view (
    view_id TEXT PRIMARY KEY,         -- issuer_request_id
    version BIGINT NOT NULL,          -- Last event sequence applied to this view
    payload JSON NOT NULL             -- Current redemption state as JSON
);

-- Index for querying redemptions by symbol
CREATE INDEX idx_redemption_view_symbol
    ON redemption_view(json_extract(payload, '$.underlying'));

-- Index for querying redemptions by transaction hash
CREATE INDEX idx_redemption_view_tx_hash
    ON redemption_view(json_extract(payload, '$.tx_hash'));
