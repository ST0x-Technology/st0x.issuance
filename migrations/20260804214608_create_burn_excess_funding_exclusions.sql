-- Path B burn-excess: exact funding Transfer log identities the redemption
-- poller must skip. Bound to one verified (network, vault, tx_hash, log_index);
-- never a general bypass.
CREATE TABLE IF NOT EXISTS burn_excess_funding_exclusions (
    network TEXT NOT NULL,
    vault TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    amount TEXT NOT NULL,
    deposit_tx_hash TEXT NOT NULL,
    excluded_at TEXT NOT NULL,
    PRIMARY KEY (network, vault, tx_hash, log_index)
);
