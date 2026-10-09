-- Path B burn-excess, live route: funding Transfers a stream expects but has
-- not yet proven and excluded. The redemption poller holds a matching log
-- (stops that vault's checkpoint before its block) instead of opening a
-- Redemption for it. One row per stream in `AwaitingFunding`; the row is
-- removed once the stream records its exclusion or closes.
CREATE TABLE IF NOT EXISTS burn_excess_funding_expectations (
    deposit_tx_hash TEXT PRIMARY KEY NOT NULL,
    network TEXT NOT NULL,
    vault TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    amount TEXT NOT NULL,
    expected_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_burn_excess_funding_expectations_transfer
    ON burn_excess_funding_expectations (
        network,
        vault,
        from_address,
        to_address,
        amount
    );
