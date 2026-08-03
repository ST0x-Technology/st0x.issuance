-- Serialize persisted transaction intentions per signer nonce domain.
--
-- The in-process wallet mutex prevents two jobs in one service process from
-- signing concurrently, but it cannot coordinate two service instances during
-- a rollout or operator overlap. SQLite serializes writers, so maintaining this
-- unique row in the SAME transaction that appends an intent event makes the
-- event append the durable arbitration point: a competing signed-but-unbroadcast
-- transaction is rejected before its intent can commit or its caller can submit
-- it.
CREATE TABLE active_signer_intents (
    network TEXT NOT NULL PRIMARY KEY
        CHECK (network IN ('base', 'ethereum', 'hyperevm')),
    aggregate_type TEXT NOT NULL
        CHECK (aggregate_type IN ('Mint', 'Redemption')),
    aggregate_id TEXT NOT NULL,
    UNIQUE (aggregate_type, aggregate_id)
);

-- Rebuild the derived guard from existing unresolved event streams. Missing
-- origin metadata and unknown network values intentionally violate the table's
-- constraints so startup fails closed instead of silently assigning an
-- ambiguous signer domain. Redemption explicitly supports pre-network Detected
-- events as Base; Mint has no such wire compatibility, so a missing mint
-- network is malformed and remains NULL to trip the NOT NULL constraint.
WITH unresolved_mints AS (
    SELECT DISTINCT
        CASE
            -- Exactly one Initiated event, matching the live validation
            -- trigger: zero OR duplicates leave the network NULL so the
            -- NOT NULL constraint aborts the backfill instead of guessing
            -- which origin is authoritative.
            WHEN (
                SELECT COUNT(*)
                FROM events AS initiated
                WHERE initiated.aggregate_type = intent.aggregate_type
                  AND initiated.aggregate_id = intent.aggregate_id
                  AND initiated.event_type = 'MintEvent::Initiated'
            ) = 1
            THEN (
                SELECT json_extract(
                    initiated.payload,
                    '$.Initiated.network'
                )
                FROM events AS initiated
                WHERE initiated.aggregate_type = intent.aggregate_type
                  AND initiated.aggregate_id = intent.aggregate_id
                  AND initiated.event_type = 'MintEvent::Initiated'
                ORDER BY initiated.sequence
                LIMIT 1
            )
        END AS network,
        'Mint' AS aggregate_type,
        intent.aggregate_id
    FROM events AS intent
    WHERE intent.aggregate_type = 'Mint'
      AND intent.event_type = 'MintEvent::MintTxIntended'
      AND NOT EXISTS (
          SELECT 1
          FROM events AS later
          WHERE later.aggregate_type = intent.aggregate_type
            AND later.aggregate_id = intent.aggregate_id
            AND later.sequence > intent.sequence
            AND later.event_type IN (
                'MintEvent::MintTxSubmitted',
                'MintEvent::TokensMinted',
                'MintEvent::ExistingMintRecovered',
                'MintEvent::MintClosed'
            )
      )
),
unresolved_burns AS (
    SELECT DISTINCT
        CASE
            -- Exactly one Detected event, matching the live validation
            -- trigger: zero or duplicates fail the backfill closed via the
            -- NOT NULL constraint rather than guessing an origin.
            WHEN (
                SELECT COUNT(*)
                FROM events AS detected
                WHERE detected.aggregate_type = intent.aggregate_type
                  AND detected.aggregate_id = intent.aggregate_id
                  AND detected.event_type = 'RedemptionEvent::Detected'
            ) = 1
            THEN COALESCE((
                SELECT json_extract(detected.payload, '$.Detected.network')
                FROM events AS detected
                WHERE detected.aggregate_type = intent.aggregate_type
                  AND detected.aggregate_id = intent.aggregate_id
                  AND detected.event_type = 'RedemptionEvent::Detected'
                ORDER BY detected.sequence
                LIMIT 1
            ), 'base')
        END AS network,
        'Redemption' AS aggregate_type,
        intent.aggregate_id
    FROM events AS intent
    WHERE intent.aggregate_type = 'Redemption'
      AND intent.event_type = 'RedemptionEvent::BurnIntended'
      AND NOT EXISTS (
          SELECT 1
          FROM events AS later
          WHERE later.aggregate_type = intent.aggregate_type
            AND later.aggregate_id = intent.aggregate_id
            AND later.sequence > intent.sequence
            AND later.event_type NOT IN (
                'RedemptionEvent::BurnIntended',
                'RedemptionEvent::BurnRecoveryAttempted',
                'RedemptionEvent::BurnPreparationRecoveryAttempted',
                'RedemptionEvent::BurnRecoveryExhausted',
                'RedemptionEvent::BurnPreparationRecoveryExhausted',
                'RedemptionEvent::BurningFailed',
                'RedemptionEvent::RedemptionFailed',
                'RedemptionEvent::BurnResumed',
                'RedemptionEvent::Reprocessed',
                'RedemptionEvent::AlpacaCalled',
                'RedemptionEvent::AlpacaCallFailed',
                'RedemptionEvent::AlpacaJournalCompleted'
            )
      )
)
-- A PRIMARY KEY violation here means the history already contains TWO
-- unresolved signed intents on the same network — the exact double-signing
-- hazard this table exists to prevent, produced before the guard existed.
-- The migration aborting is the correct outcome: do NOT weaken this insert
-- to pick a winner. Remediate by resolving the older intent's aggregate
-- first (recover or close it through the admin endpoints so its stream
-- gains a releasing event), then re-run the migration. Run the
-- verify-migrations binary against a prod snapshot before deploying to
-- surface such conflicts ahead of the rollout restart.
INSERT INTO active_signer_intents (network, aggregate_type, aggregate_id)
SELECT network, aggregate_type, aggregate_id FROM unresolved_mints
UNION ALL
SELECT network, aggregate_type, aggregate_id FROM unresolved_burns;

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
        ) NOT IN ('base', 'ethereum', 'hyperevm')
        THEN RAISE(ABORT, 'mint signer intent has an unknown network')
    END;
END;

CREATE TRIGGER reserve_mint_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'Mint'
 AND NEW.event_type = 'MintEvent::MintTxIntended'
BEGIN
    -- Explicit RAISE for the cross-instance case. Letting the PRIMARY KEY
    -- violation surface instead would be misclassified upstream: the event
    -- store treats any unique violation as a same-aggregate optimistic-lock
    -- conflict ("aggregate conflict"), pointing an operator at the wrong
    -- root cause during exactly the rollout-overlap incident this guard
    -- exists for.
    SELECT RAISE(
        ABORT,
        'signer network already reserved by another unresolved intent'
    )
    WHERE EXISTS (
        SELECT 1
        FROM active_signer_intents
        WHERE network = (
            SELECT json_extract(initiated.payload, '$.Initiated.network')
            FROM events AS initiated
            WHERE initiated.aggregate_type = NEW.aggregate_type
              AND initiated.aggregate_id = NEW.aggregate_id
              AND initiated.event_type = 'MintEvent::Initiated'
            LIMIT 1
        )
          AND NOT (
              aggregate_type = NEW.aggregate_type
              AND aggregate_id = NEW.aggregate_id
          )
    );

    INSERT INTO active_signer_intents (
        network,
        aggregate_type,
        aggregate_id
    )
    VALUES (
        (
            SELECT json_extract(initiated.payload, '$.Initiated.network')
            FROM events AS initiated
            WHERE initiated.aggregate_type = NEW.aggregate_type
              AND initiated.aggregate_id = NEW.aggregate_id
              AND initiated.event_type = 'MintEvent::Initiated'
            LIMIT 1
        ),
        NEW.aggregate_type,
        NEW.aggregate_id
    )
    ON CONFLICT (aggregate_type, aggregate_id)
    DO UPDATE SET network = excluded.network;
END;

-- MintClosed releases too: the admin close command is valid from any
-- non-terminal state, including TxIntended. Without it in this list a
-- routine operator close would strand the network's reservation forever
-- (the row is keyed by network, so every later mint AND burn on that
-- network would be rejected with no self-healing path).
CREATE TRIGGER release_mint_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'Mint'
 AND NEW.event_type IN (
     'MintEvent::MintTxSubmitted',
     'MintEvent::TokensMinted',
     'MintEvent::ExistingMintRecovered',
     'MintEvent::MintClosed'
 )
BEGIN
    DELETE FROM active_signer_intents
    WHERE aggregate_type = NEW.aggregate_type
      AND aggregate_id = NEW.aggregate_id;
END;

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
        ), 'base') NOT IN ('base', 'ethereum', 'hyperevm')
        THEN RAISE(ABORT, 'burn signer intent has an unknown network')
    END;
END;

CREATE TRIGGER reserve_burn_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'Redemption'
 AND NEW.event_type = 'RedemptionEvent::BurnIntended'
BEGIN
    -- Explicit RAISE for the cross-instance case, mirroring the mint
    -- reserve trigger: an implicit PRIMARY KEY violation would be
    -- misreported upstream as a same-aggregate optimistic-lock conflict.
    SELECT RAISE(
        ABORT,
        'signer network already reserved by another unresolved intent'
    )
    WHERE EXISTS (
        SELECT 1
        FROM active_signer_intents
        WHERE network = COALESCE((
            SELECT json_extract(detected.payload, '$.Detected.network')
            FROM events AS detected
            WHERE detected.aggregate_type = NEW.aggregate_type
              AND detected.aggregate_id = NEW.aggregate_id
              AND detected.event_type = 'RedemptionEvent::Detected'
            LIMIT 1
        ), 'base')
          AND NOT (
              aggregate_type = NEW.aggregate_type
              AND aggregate_id = NEW.aggregate_id
          )
    );

    INSERT INTO active_signer_intents (
        network,
        aggregate_type,
        aggregate_id
    )
    VALUES (
        COALESCE((
            SELECT json_extract(detected.payload, '$.Detected.network')
            FROM events AS detected
            WHERE detected.aggregate_type = NEW.aggregate_type
              AND detected.aggregate_id = NEW.aggregate_id
              AND detected.event_type = 'RedemptionEvent::Detected'
            LIMIT 1
        ), 'base'),
        NEW.aggregate_type,
        NEW.aggregate_id
    )
    ON CONFLICT (aggregate_type, aggregate_id)
    DO UPDATE SET network = excluded.network;
END;

-- Recovery bookkeeping does not release the nonce domain, while any other
-- later redemption event does. BurningFailed and RedemptionFailed are in the
-- exclusion set for the same reason MintingFailed never releases on the mint
-- side: a failed submit may still have broadcast the signed transaction, and
-- the ambiguous recover_single_burn_failed path emits BurningFailed then
-- RedemptionFailed while deliberately keeping the reservation — a release on
-- the second event would undo the first's retention. BurnResumed is excluded
-- too: it moves a Failed redemption back into Burning before the replacement
-- BurnIntended re-reserves, and the prior signed transaction's on-chain fate
-- is still unknown in that window. Reprocessed is excluded for the same
-- ambiguous-broadcast reason: it is valid from Failed while
-- unresolved_burn_tx is still retained, so releasing on reprocess would free
-- the nonce domain before the prior signed transaction's fate is known.
-- After Reprocessed, apply_reprocessed returns the aggregate to Detected, so
-- the next events are AlpacaCalled / AlpacaCallFailed / AlpacaJournalCompleted
-- — those must stay excluded too, or the reservation would drop one event
-- after reprocess while the prior signed burn may still be unknown on-chain.
-- Real terminal outcomes (TokensBurned, RedemptionClosed) release by falling
-- outside this list. This list must stay in lockstep with the
-- unresolved_burns backfill CTE above; the application-side guard reads the
-- active_signer_intents table itself, so the triggers and backfill are the
-- only two places encoding it.
CREATE TRIGGER release_burn_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'Redemption'
 AND NEW.event_type NOT IN (
     'RedemptionEvent::BurnIntended',
     'RedemptionEvent::BurnRecoveryAttempted',
     'RedemptionEvent::BurnPreparationRecoveryAttempted',
     'RedemptionEvent::BurnRecoveryExhausted',
     'RedemptionEvent::BurnPreparationRecoveryExhausted',
     'RedemptionEvent::BurningFailed',
     'RedemptionEvent::RedemptionFailed',
     'RedemptionEvent::BurnResumed',
     'RedemptionEvent::Reprocessed',
     'RedemptionEvent::AlpacaCalled',
     'RedemptionEvent::AlpacaCallFailed',
     'RedemptionEvent::AlpacaJournalCompleted'
 )
BEGIN
    DELETE FROM active_signer_intents
    WHERE aggregate_type = NEW.aggregate_type
      AND aggregate_id = NEW.aggregate_id;
END;
