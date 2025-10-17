-- Mint view: current state of mint operations
CREATE TABLE IF NOT EXISTS mint_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mint_view_client_id
    ON mint_view(json_extract(payload, '$.Initiated.client_id'));
CREATE INDEX IF NOT EXISTS idx_mint_view_underlying
    ON mint_view(json_extract(payload, '$.Initiated.underlying'));
CREATE INDEX IF NOT EXISTS idx_mint_view_tokenization_request_id
    ON mint_view(json_extract(payload, '$.Initiated.tokenization_request_id'));
