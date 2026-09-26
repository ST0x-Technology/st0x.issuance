-- Expression index for the recipient-authorization nonce lookup on the
-- internal mint-authorization path. A mint is in exactly one lifecycle
-- state, so at most one of these payload paths is non-null and COALESCE
-- over them is the mint's authorization nonce regardless of state (`Closed`
-- reads its pair from `unreleased_nonce`, which a close sets unless the
-- operator acknowledged the nonce as free). The
-- query in `find_mints_holding_nonce` uses this exact expression so SQLite
-- can serve it from the index instead of scanning the table.
--
-- The index only PRUNES candidates: which of them still holds the nonce is
-- decided in Rust by `Mint::held_authorization_nonce`, so every state
-- carrying the field is listed here, including the ones that release it.
CREATE INDEX IF NOT EXISTS idx_mint_view_live_authorization_nonce
    ON mint_view(COALESCE(
        json_extract(payload, '$.Live.Initiated.mint_authorization.nonce'),
        json_extract(payload, '$.Live.JournalConfirmed.mint_authorization.nonce'),
        json_extract(payload, '$.Live.JournalRejected.mint_authorization.nonce'),
        json_extract(payload, '$.Live.Minting.mint_authorization.nonce'),
        json_extract(payload, '$.Live.TxIntended.mint_authorization.nonce'),
        json_extract(payload, '$.Live.TxSubmitted.mint_authorization.nonce'),
        json_extract(payload, '$.Live.CallbackPending.mint_authorization.nonce'),
        json_extract(payload, '$.Live.MintingFailed.mint_authorization.nonce'),
        json_extract(payload, '$.Live.Completed.mint_authorization.nonce'),
        json_extract(payload, '$.Live.Closed.unreleased_nonce.nonce')
    ));
