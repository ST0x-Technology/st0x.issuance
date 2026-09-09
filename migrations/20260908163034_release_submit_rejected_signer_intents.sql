-- A submit rejection is a terminal before acceptance outcome for the
-- persisted transaction identity, so the signer intent can be released.
DROP TRIGGER release_mint_signer_intent;

CREATE TRIGGER release_mint_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'Mint'
 AND NEW.event_type IN (
     'MintEvent::MintTxSubmitted',
     'MintEvent::MintSubmitRejected',
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
