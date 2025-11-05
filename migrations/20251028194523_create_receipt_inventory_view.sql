-- Receipt inventory view: tracks receipt balances through state transitions
CREATE TABLE IF NOT EXISTS receipt_inventory_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_receipt_inventory_view_pending_underlying
    ON receipt_inventory_view(json_extract(payload, '$.Pending.underlying'));
CREATE INDEX IF NOT EXISTS idx_receipt_inventory_view_active_underlying
    ON receipt_inventory_view(json_extract(payload, '$.Active.underlying'));

CREATE INDEX IF NOT EXISTS idx_receipt_inventory_view_pending_token
    ON receipt_inventory_view(json_extract(payload, '$.Pending.token'));
CREATE INDEX IF NOT EXISTS idx_receipt_inventory_view_active_token
    ON receipt_inventory_view(json_extract(payload, '$.Active.token'));

CREATE INDEX IF NOT EXISTS idx_receipt_inventory_view_current_balance
    ON receipt_inventory_view(json_extract(payload, '$.Active.current_balance'));
