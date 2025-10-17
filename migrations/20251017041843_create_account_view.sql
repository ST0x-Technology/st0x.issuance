-- Account view: current account state
CREATE TABLE IF NOT EXISTS account_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    email_indexed TEXT GENERATED ALWAYS AS (json_extract(payload, '$.email')) STORED
);

CREATE INDEX IF NOT EXISTS idx_account_view_email_indexed
    ON account_view(email_indexed);
CREATE INDEX IF NOT EXISTS idx_account_view_alpaca
    ON account_view(json_extract(payload, '$.alpaca_account'));
CREATE INDEX IF NOT EXISTS idx_account_view_status
    ON account_view(json_extract(payload, '$.status'));
