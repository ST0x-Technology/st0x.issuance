-- Expression index for the tokenization-request-id -> issuer-request-id
-- lookup on the internal mint-authorization path. A mint is in exactly one
-- lifecycle state, so at most one of these payload paths is non-null and
-- COALESCE over them is the mint's tokenization id regardless of state
-- (`Closed` carries none and stays out of the lookup by design). The query
-- in `find_issuer_id_by_tokenization_request_id` uses this exact expression
-- so SQLite can serve it from the index instead of scanning the table.
CREATE INDEX IF NOT EXISTS idx_mint_view_live_tokenization_request_id
    ON mint_view(COALESCE(
        json_extract(payload, '$.Live.Initiated.tokenization_request_id'),
        json_extract(payload, '$.Live.JournalConfirmed.tokenization_request_id'),
        json_extract(payload, '$.Live.JournalRejected.tokenization_request_id'),
        json_extract(payload, '$.Live.Minting.tokenization_request_id'),
        json_extract(payload, '$.Live.TxIntended.tokenization_request_id'),
        json_extract(payload, '$.Live.TxSubmitted.tokenization_request_id'),
        json_extract(payload, '$.Live.CallbackPending.tokenization_request_id'),
        json_extract(payload, '$.Live.MintingFailed.tokenization_request_id'),
        json_extract(payload, '$.Live.Completed.tokenization_request_id')
    ));
