-- Release the signer-intent reservation on the orchestrator mint outcomes.
--
-- The release trigger predates orchestrator mode and only knew the
-- vault-direct resolutions. An orchestrator mint's landing can be recorded
-- as `OrchestratorMintRecovered` while the reservation from its
-- `MintTxIntended` is still held (e.g. a crash after intent, a
-- `NonceReplayed` rebroadcast revert parking `MintingFailed`, then recovery
-- proving the landing) — with no release, the network-keyed row would
-- strand, rejecting every later mint AND burn on that network until an
-- operator `MintClosed`. `OrchestratorTokensMinted` is included for parity
-- with `TokensMinted`: today it only fires after `MintTxSubmitted` already
-- released the row, but the trigger must not depend on that ordering staying
-- true.
DROP TRIGGER release_mint_signer_intent;

CREATE TRIGGER release_mint_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'Mint'
 AND NEW.event_type IN (
     'MintEvent::MintTxSubmitted',
     'MintEvent::TokensMinted',
     'MintEvent::ExistingMintRecovered',
     'MintEvent::OrchestratorTokensMinted',
     'MintEvent::OrchestratorMintRecovered',
     'MintEvent::MintClosed'
 )
BEGIN
    DELETE FROM active_signer_intents
    WHERE aggregate_type = NEW.aggregate_type
      AND aggregate_id = NEW.aggregate_id;
END;

-- Heal any reservation the gap already stranded: a held mint intent whose
-- aggregate later recorded an orchestrator resolution is resolved, not
-- unresolved — mirror the release the trigger would have performed.
DELETE FROM active_signer_intents
WHERE aggregate_type = 'Mint'
  AND EXISTS (
      SELECT 1
      FROM events AS resolved
      WHERE resolved.aggregate_type = 'Mint'
        AND resolved.aggregate_id = active_signer_intents.aggregate_id
        AND resolved.event_type IN (
            'MintEvent::OrchestratorTokensMinted',
            'MintEvent::OrchestratorMintRecovered'
        )
  );
