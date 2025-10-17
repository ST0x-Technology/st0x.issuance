-- Account view: current account state
CREATE TABLE IF NOT EXISTS account_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_account_view_email
    ON account_view(json_extract(payload, '$.email'));
CREATE INDEX IF NOT EXISTS idx_account_view_alpaca
    ON account_view(json_extract(payload, '$.alpaca_account'));
CREATE INDEX IF NOT EXISTS idx_account_view_status
    ON account_view(json_extract(payload, '$.status'));
