-- Exact AP redemptions held by an expectation, with the account attribution
-- proven before the excess burn was broadcast. These rows survive expectation
-- release so a later wallet unlink cannot strand an already-mined redemption.
CREATE TABLE IF NOT EXISTS burn_excess_held_redemptions (
    network TEXT NOT NULL,
    vault TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    deposit_tx_hash TEXT NOT NULL,
    attribution JSON NOT NULL,
    PRIMARY KEY (network, vault, tx_hash, log_index)
);
ALTER TABLE burn_excess_funding_expectations
ADD COLUMN released INTEGER NOT NULL DEFAULT 0
    CHECK (released IN (0, 1));
ALTER TABLE burn_excess_funding_expectations
ADD COLUMN release_through_block INTEGER
    CHECK (release_through_block IS NULL OR release_through_block >= 0);

CREATE UNIQUE INDEX burn_excess_funding_expectation_shape
ON burn_excess_funding_expectations (
    network,
    vault,
    from_address,
    to_address,
    amount
);

CREATE TABLE burn_excess_expectation_guards (
    deposit_tx_hash TEXT PRIMARY KEY NOT NULL,
    network TEXT NOT NULL,
    vault TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    amount TEXT NOT NULL,
    UNIQUE (
        network,
        vault,
        from_address,
        to_address,
        amount
    )
);

-- Durable tombstones prevent startup rebuild from resurrecting a terminal
-- expectation after its released catch-up and configuration handoff completed.
CREATE TABLE burn_excess_expectation_catchups (
    deposit_tx_hash TEXT PRIMARY KEY NOT NULL,
    network TEXT NOT NULL,
    vault TEXT NOT NULL,
    scanned_through_block INTEGER NOT NULL
        CHECK (scanned_through_block >= 0),
    completed_at TEXT NOT NULL
);

CREATE TRIGGER validate_burn_excess_expectation_sender
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'BurnExcess'
 AND NEW.event_type = 'BurnExcessEvent::FundingExpected'
BEGIN
    SELECT CASE
        WHEN (
            SELECT COUNT(DISTINCT whitelisted.aggregate_id)
            FROM events AS whitelisted
            WHERE whitelisted.aggregate_type = 'Account'
              AND whitelisted.event_type =
                  'AccountEvent::WalletWhitelisted'
              AND lower(
                  json_extract(
                      whitelisted.payload,
                      '$.WalletWhitelisted.wallet'
                  )
              ) = lower(
                  json_extract(
                      NEW.payload,
                      '$.FundingExpected.bind.original_recipient'
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM events AS unwhitelisted
                  WHERE unwhitelisted.aggregate_type =
                        whitelisted.aggregate_type
                    AND unwhitelisted.aggregate_id =
                        whitelisted.aggregate_id
                    AND unwhitelisted.sequence > whitelisted.sequence
                    AND unwhitelisted.event_type =
                        'AccountEvent::WalletUnwhitelisted'
                    AND lower(
                        json_extract(
                            unwhitelisted.payload,
                            '$.WalletUnwhitelisted.wallet'
                        )
                    ) = lower(
                        json_extract(
                            NEW.payload,
                            '$.FundingExpected.bind.original_recipient'
                        )
                    )
              )
        ) != 1
        THEN RAISE(
            ABORT,
            'burn-excess expectation sender must be uniquely linked'
        )
    END;
END;

CREATE TRIGGER validate_burn_excess_expectation_vault
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'BurnExcess'
 AND NEW.event_type = 'BurnExcessEvent::FundingExpected'
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM events AS mint
            JOIN tokenized_asset_vault_owners AS owner
              ON owner.aggregate_id =
                 json_extract(mint.payload, '$.Initiated.underlying')
                 || ':'
                 || owner.network
            WHERE mint.aggregate_type = 'Mint'
              AND mint.aggregate_id = json_extract(
                  NEW.payload,
                  '$.FundingExpected.bind.issuer_request_id'
              )
              AND mint.event_type = 'MintEvent::Initiated'
              AND json_extract(
                  mint.payload,
                  '$.Initiated.network'
              ) = owner.network
              AND owner.network = json_extract(
                  NEW.payload,
                  '$.FundingExpected.bind.network'
              )
              AND lower(owner.vault) = lower(
                  json_extract(
                      NEW.payload,
                      '$.FundingExpected.bind.vault'
                  )
              )
        )
        THEN RAISE(
            ABORT,
            'burn-excess expectation vault must match the mint listing'
        )
    END;
END;

CREATE TRIGGER guard_burn_excess_expectation_event
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'BurnExcess'
 AND NEW.event_type = 'BurnExcessEvent::FundingExpected'
BEGIN
    INSERT INTO burn_excess_expectation_guards (
        deposit_tx_hash,
        network,
        vault,
        from_address,
        to_address,
        amount
    )
    VALUES (
        NEW.aggregate_id,
        json_extract(NEW.payload, '$.FundingExpected.bind.network'),
        lower(json_extract(NEW.payload, '$.FundingExpected.bind.vault')),
        lower(
            json_extract(
                NEW.payload,
                '$.FundingExpected.bind.original_recipient'
            )
        ),
        lower(
            json_extract(
                NEW.payload,
                '$.FundingExpected.bind.issuer_wallet'
            )
        ),
        json_extract(NEW.payload, '$.FundingExpected.bind.shares')
    )
    ON CONFLICT(deposit_tx_hash) DO NOTHING;
END;

CREATE TRIGGER clear_burn_excess_expectation_guard
AFTER DELETE ON burn_excess_funding_expectations
BEGIN
    DELETE FROM burn_excess_expectation_guards
    WHERE deposit_tx_hash = OLD.deposit_tx_hash;
END;

-- An expectation owns the old vault/account attribution until either the
-- recovery completes or a released close has been scanned once. Prevent
-- configuration changes from invalidating logs in that handoff window.
CREATE TRIGGER guard_burn_excess_expectation_vault_repoint
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'TokenizedAsset'
 AND NEW.event_type = 'TokenizedAssetEvent::VaultAddressUpdated'
BEGIN
    SELECT RAISE(
        ABORT,
        'burn-excess expectation still owns the previous vault'
    )
    WHERE EXISTS (
        SELECT 1
        FROM burn_excess_funding_expectations
        WHERE network = substr(
            NEW.aggregate_id,
            instr(NEW.aggregate_id, ':') + 1
        )
          AND vault = lower(
              json_extract(
                  NEW.payload,
                  '$.VaultAddressUpdated.previous_vault'
              )
          )
    )
       OR EXISTS (
           SELECT 1
           FROM burn_excess_expectation_guards
           WHERE network = substr(
               NEW.aggregate_id,
               instr(NEW.aggregate_id, ':') + 1
           )
             AND vault = lower(
                 json_extract(
                     NEW.payload,
                     '$.VaultAddressUpdated.previous_vault'
                 )
             )
       );
END;

CREATE TRIGGER guard_burn_excess_expectation_wallet_mutation
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'Account'
 AND NEW.event_type IN (
     'AccountEvent::WalletWhitelisted',
     'AccountEvent::WalletUnwhitelisted'
 )
BEGIN
    SELECT RAISE(
        ABORT,
        'burn-excess expectation still owns the wallet attribution'
    )
    WHERE EXISTS (
        SELECT 1
        FROM burn_excess_funding_expectations
        WHERE from_address = lower(
            CASE NEW.event_type
                WHEN 'AccountEvent::WalletWhitelisted'
                THEN json_extract(
                    NEW.payload,
                    '$.WalletWhitelisted.wallet'
                )
                WHEN 'AccountEvent::WalletUnwhitelisted'
                THEN json_extract(
                    NEW.payload,
                    '$.WalletUnwhitelisted.wallet'
                )
            END
        )
    )
       OR EXISTS (
           SELECT 1
           FROM burn_excess_expectation_guards
           WHERE from_address = lower(
               CASE NEW.event_type
                   WHEN 'AccountEvent::WalletWhitelisted'
                   THEN json_extract(
                       NEW.payload,
                       '$.WalletWhitelisted.wallet'
                   )
                   WHEN 'AccountEvent::WalletUnwhitelisted'
                   THEN json_extract(
                       NEW.payload,
                       '$.WalletUnwhitelisted.wallet'
                   )
               END
           )
       );
END;


-- Add burn-excess recovery to the database-backed signer nonce reservation.
--
-- The wallet mutex is process-local. Reserving on the first durable external
-- exclusion, or on an internal intent, makes SQLite arbitrate overlapping CLI
-- and service processes in the same transaction that appends the event.
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
SELECT network, aggregate_type, aggregate_id
FROM active_signer_intents;

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
        CHECK (aggregate_type IN ('Mint', 'Redemption', 'BurnExcess')),
    aggregate_id TEXT NOT NULL,
    UNIQUE (aggregate_type, aggregate_id)
);

INSERT INTO active_signer_intents (
    network,
    aggregate_type,
    aggregate_id
)
SELECT network, aggregate_type, aggregate_id
FROM active_signer_intents_rebuild;

DROP TABLE active_signer_intents_rebuild;

-- Backfill every nonterminal recovery. A uniqueness failure means history
-- already contains two unresolved operations for one signer nonce domain;
-- aborting deployment is safer than choosing a winner.
WITH unresolved_burn_excess AS (
    SELECT DISTINCT
        CASE intent.event_type
            WHEN 'BurnExcessEvent::FundingExclusionRecorded'
            THEN json_extract(
                intent.payload,
                '$.FundingExclusionRecorded.bind.network'
            )
            WHEN 'BurnExcessEvent::ExcessBurnIntended'
            THEN json_extract(
                intent.payload,
                '$.ExcessBurnIntended.bind.network'
            )
        END AS network,
        'BurnExcess' AS aggregate_type,
        intent.aggregate_id
    FROM events AS intent
    WHERE intent.aggregate_type = 'BurnExcess'
      AND intent.event_type IN (
          'BurnExcessEvent::FundingExclusionRecorded',
          'BurnExcessEvent::ExcessBurnIntended'
      )
      AND NOT EXISTS (
          SELECT 1
          FROM events AS terminal
          WHERE terminal.aggregate_type = intent.aggregate_type
            AND terminal.aggregate_id = intent.aggregate_id
            AND (
                terminal.event_type =
                    'BurnExcessEvent::ExcessBurnCompleted'
                OR (
                    terminal.event_type =
                        'BurnExcessEvent::ExcessBurnClosed'
                    AND (
                        (
                            NOT EXISTS (
                                SELECT 1
                                FROM events AS signed
                                WHERE signed.aggregate_type =
                                    intent.aggregate_type
                                  AND signed.aggregate_id =
                                    intent.aggregate_id
                                  AND signed.event_type =
                                    'BurnExcessEvent::ExcessBurnIntended'
                            )
                            AND COALESCE(
                                json_extract(
                                    terminal.payload,
                                    '$.ExcessBurnClosed.proof'
                                ),
                                'unsigned'
                            ) = 'unsigned'
                        )
                        OR (
                            EXISTS (
                                SELECT 1
                                FROM events AS signed
                                WHERE signed.aggregate_type =
                                    intent.aggregate_type
                                  AND signed.aggregate_id =
                                    intent.aggregate_id
                                  AND signed.event_type =
                                    'BurnExcessEvent::ExcessBurnIntended'
                            )
                            AND json_extract(
                                terminal.payload,
                                '$.ExcessBurnClosed.proof'
                            ) IN ('finalized_reverted', 'provably_dead')
                        )
                    )
                )
            )
      )
)
INSERT INTO active_signer_intents (
    network,
    aggregate_type,
    aggregate_id
)
SELECT network, aggregate_type, aggregate_id
FROM unresolved_burn_excess;

CREATE TRIGGER validate_burn_excess_signer_intent_origin
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'BurnExcess'
 AND NEW.event_type IN (
     'BurnExcessEvent::FundingExclusionRecorded',
     'BurnExcessEvent::ExcessBurnIntended'
 )
BEGIN
    SELECT CASE
        WHEN CASE NEW.event_type
            WHEN 'BurnExcessEvent::FundingExclusionRecorded'
            THEN json_extract(
                NEW.payload,
                '$.FundingExclusionRecorded.bind.network'
            )
            WHEN 'BurnExcessEvent::ExcessBurnIntended'
            THEN json_extract(
                NEW.payload,
                '$.ExcessBurnIntended.bind.network'
            )
        END IS NULL
        THEN RAISE(ABORT, 'burn-excess signer intent requires network metadata')
        WHEN CASE NEW.event_type
            WHEN 'BurnExcessEvent::FundingExclusionRecorded'
            THEN json_extract(
                NEW.payload,
                '$.FundingExclusionRecorded.bind.network'
            )
            WHEN 'BurnExcessEvent::ExcessBurnIntended'
            THEN json_extract(
                NEW.payload,
                '$.ExcessBurnIntended.bind.network'
            )
        END NOT IN (
            'base',
            'ethereum',
            'hyperevm',
            'robinhood',
            'binance'
        )
        THEN RAISE(ABORT, 'burn-excess signer intent has an unknown network')
    END;
END;

CREATE TRIGGER reserve_burn_excess_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'BurnExcess'
 AND NEW.event_type IN (
     'BurnExcessEvent::FundingExclusionRecorded',
     'BurnExcessEvent::ExcessBurnIntended'
 )
BEGIN
    SELECT RAISE(
        ABORT,
        'signer network already reserved by another unresolved intent'
    )
    WHERE EXISTS (
        SELECT 1
        FROM active_signer_intents
        WHERE network = CASE NEW.event_type
            WHEN 'BurnExcessEvent::FundingExclusionRecorded'
            THEN json_extract(
                NEW.payload,
                '$.FundingExclusionRecorded.bind.network'
            )
            WHEN 'BurnExcessEvent::ExcessBurnIntended'
            THEN json_extract(
                NEW.payload,
                '$.ExcessBurnIntended.bind.network'
            )
        END
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
        CASE NEW.event_type
            WHEN 'BurnExcessEvent::FundingExclusionRecorded'
            THEN json_extract(
                NEW.payload,
                '$.FundingExclusionRecorded.bind.network'
            )
            WHEN 'BurnExcessEvent::ExcessBurnIntended'
            THEN json_extract(
                NEW.payload,
                '$.ExcessBurnIntended.bind.network'
            )
        END,
        NEW.aggregate_type,
        NEW.aggregate_id
    )
    ON CONFLICT (aggregate_type, aggregate_id)
    DO UPDATE SET network = excluded.network;
END;

-- Closing an unsigned stream and abandoning a signed transaction are different
-- safety claims. Signed bytes retain the nonce reservation unless the service
-- persisted a finalized-death proof in the close event.
CREATE TRIGGER validate_burn_excess_close_proof
BEFORE INSERT ON events
WHEN NEW.aggregate_type = 'BurnExcess'
 AND NEW.event_type = 'BurnExcessEvent::ExcessBurnClosed'
BEGIN
    SELECT CASE
        WHEN json_extract(
            NEW.payload,
            '$.ExcessBurnClosed.proof'
        ) IS NULL
          OR json_extract(
              NEW.payload,
              '$.ExcessBurnClosed.proof'
          ) NOT IN (
              'unsigned',
              'finalized_reverted',
              'provably_dead'
          )
        THEN RAISE(ABORT, 'burn-excess close requires a valid safety proof')
        WHEN json_type(
            NEW.payload,
            '$.ExcessBurnClosed.release_through_block'
        ) IS NOT 'integer'
          OR json_extract(
              NEW.payload,
              '$.ExcessBurnClosed.release_through_block'
          ) < 0
        THEN RAISE(
            ABORT,
            'burn-excess close requires a release block'
        )
        WHEN EXISTS (
            SELECT 1
            FROM events AS intent
            WHERE intent.aggregate_type = NEW.aggregate_type
              AND intent.aggregate_id = NEW.aggregate_id
              AND intent.event_type =
                  'BurnExcessEvent::ExcessBurnIntended'
        )
         AND json_extract(
             NEW.payload,
             '$.ExcessBurnClosed.proof'
         ) = 'unsigned'
        THEN RAISE(
            ABORT,
            'signed burn-excess close requires finalized-dead proof'
        )
        WHEN NOT EXISTS (
            SELECT 1
            FROM events AS intent
            WHERE intent.aggregate_type = NEW.aggregate_type
              AND intent.aggregate_id = NEW.aggregate_id
              AND intent.event_type =
                  'BurnExcessEvent::ExcessBurnIntended'
        )
         AND json_extract(
             NEW.payload,
             '$.ExcessBurnClosed.proof'
         ) != 'unsigned'
        THEN RAISE(
            ABORT,
            'unsigned burn-excess close requires unsigned proof'
        )
    END;
END;

CREATE TRIGGER release_burn_excess_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'BurnExcess'
 AND NEW.event_type IN (
     'BurnExcessEvent::ExcessBurnCompleted',
     'BurnExcessEvent::ExcessBurnClosed'
 )
BEGIN
    DELETE FROM active_signer_intents
    WHERE aggregate_type = NEW.aggregate_type
      AND aggregate_id = NEW.aggregate_id;
END;
