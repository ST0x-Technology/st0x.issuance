-- Admit Robinhood Chain and BNB Smart Chain as signer nonce domains.
--
-- `active_signer_intents.network` and the two intent-origin validation
-- triggers each enumerate the networks the bot issues on, so a mint or burn
-- on a newly added chain would ABORT at the event append -- the guard would
-- read as a double-signing conflict rather than the missing enum value it is.
-- The enumerations must therefore grow in lockstep with `dto::Network`.
--
-- SQLite cannot widen a CHECK constraint in place, so the table is rebuilt:
-- rows are staged in an unconstrained table, the constrained table is dropped
-- and recreated with the wider CHECK, and the rows are copied back. Every
-- surviving row was already valid under the narrower CHECK, so the copy back
-- cannot lose one; a violation here would mean the pre-migration database
-- already held an unknown network and failing closed is correct.
--
-- The reserve/release triggers live on `events`, not on this table, so
-- dropping it does not drop them and they need no restatement. The two
-- validate triggers do hardcode the network list and are restated below,
-- unchanged apart from that list.
CREATE TABLE active_signer_intents_rebuild (
    network TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL
);

INSERT INTO active_signer_intents_rebuild (
    network,
    aggregate_type,
    aggregate_id
)
SELECT network, aggregate_type, aggregate_id FROM active_signer_intents;

DROP TABLE active_signer_intents;

CREATE TABLE active_signer_intents (
    network TEXT NOT NULL PRIMARY KEY
        CHECK (
            network IN (
                'base',
                'ethereum',
                'hyperevm',
                'robinhood',
                'binance'
            )
        ),
    aggregate_type TEXT NOT NULL
        CHECK (aggregate_type IN ('Mint', 'Redemption')),
    aggregate_id TEXT NOT NULL,
    UNIQUE (aggregate_type, aggregate_id)
);

INSERT INTO active_signer_intents (network, aggregate_type, aggregate_id)
SELECT network, aggregate_type, aggregate_id
FROM active_signer_intents_rebuild;

DROP TABLE active_signer_intents_rebuild;

DROP TRIGGER validate_mint_signer_intent_origin;

CREATE TRIGGER validate_mint_signer_intent_origin
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'Mint'
 AND NEW.event_type = 'MintEvent::MintTxIntended'
BEGIN
    SELECT CASE
        WHEN (
            SELECT COUNT(*)
            FROM events AS initiated
            WHERE initiated.aggregate_type = NEW.aggregate_type
              AND initiated.aggregate_id = NEW.aggregate_id
              AND initiated.event_type = 'MintEvent::Initiated'
        ) != 1
        THEN RAISE(ABORT, 'mint signer intent requires one Initiated event')
        WHEN (
            SELECT json_extract(initiated.payload, '$.Initiated.network')
            FROM events AS initiated
            WHERE initiated.aggregate_type = NEW.aggregate_type
              AND initiated.aggregate_id = NEW.aggregate_id
              AND initiated.event_type = 'MintEvent::Initiated'
            LIMIT 1
        ) IS NULL
        THEN RAISE(ABORT, 'mint signer intent requires network metadata')
        WHEN (
            SELECT json_extract(initiated.payload, '$.Initiated.network')
            FROM events AS initiated
            WHERE initiated.aggregate_type = NEW.aggregate_type
              AND initiated.aggregate_id = NEW.aggregate_id
              AND initiated.event_type = 'MintEvent::Initiated'
            LIMIT 1
        ) NOT IN ('base', 'ethereum', 'hyperevm', 'robinhood', 'binance')
        THEN RAISE(ABORT, 'mint signer intent has an unknown network')
    END;
END;

DROP TRIGGER validate_burn_signer_intent_origin;

CREATE TRIGGER validate_burn_signer_intent_origin
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'Redemption'
 AND NEW.event_type = 'RedemptionEvent::BurnIntended'
BEGIN
    SELECT CASE
        WHEN (
            SELECT COUNT(*)
            FROM events AS detected
            WHERE detected.aggregate_type = NEW.aggregate_type
              AND detected.aggregate_id = NEW.aggregate_id
              AND detected.event_type = 'RedemptionEvent::Detected'
        ) != 1
        THEN RAISE(ABORT, 'burn signer intent requires one Detected event')
        WHEN COALESCE((
            SELECT json_extract(detected.payload, '$.Detected.network')
            FROM events AS detected
            WHERE detected.aggregate_type = NEW.aggregate_type
              AND detected.aggregate_id = NEW.aggregate_id
              AND detected.event_type = 'RedemptionEvent::Detected'
            LIMIT 1
        ), 'base') NOT IN (
            'base',
            'ethereum',
            'hyperevm',
            'robinhood',
            'binance'
        )
        THEN RAISE(ABORT, 'burn signer intent has an unknown network')
    END;
END;
