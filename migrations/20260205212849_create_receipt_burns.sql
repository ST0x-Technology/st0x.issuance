-- Tracks individual burn operations for receipt balance computation.
-- Keyed by redemption aggregate_id (red-xxx), so GenericQuery works correctly.
-- Join with receipt_inventory_view on receipt_id to compute available balance.
CREATE TABLE IF NOT EXISTS receipt_burns_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_receipt_burns_view_receipt_id
    ON receipt_burns_view(json_extract(payload, '$.receipt_id'));
