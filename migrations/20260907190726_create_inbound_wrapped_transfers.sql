-- Inbound transfers of configured wrapped tokens to the issuer wallet. The
-- redemption poller watches the vault (unwrapped) token only, so these can
-- never be redeemed automatically; the per network watcher records each one
-- here for the operator (GET /admin/wrapped-transfers) and alerts. Identity is
-- the log itself, so a re-scan cannot record a transfer twice.
CREATE TABLE inbound_wrapped_transfers (
    network TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL CHECK (log_index >= 0),
    token TEXT NOT NULL,
    underlying TEXT NOT NULL,
    from_address TEXT NOT NULL,
    amount TEXT NOT NULL,
    block_number INTEGER NOT NULL CHECK (block_number >= 0),
    detected_at TEXT NOT NULL,
    PRIMARY KEY (network, tx_hash, log_index)
);
