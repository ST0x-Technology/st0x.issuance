-- A nonce-too-low observation is recovery bookkeeping, not a terminal burn
-- outcome. Keep the wallet nonce domain reserved until the burn is confirmed,
-- replaced, or otherwise reaches a terminal event.
DROP TRIGGER release_burn_signer_intent;

CREATE TRIGGER release_burn_signer_intent
AFTER INSERT ON events
WHEN NEW.aggregate_type = 'Redemption'
 AND NEW.event_type NOT IN (
     'RedemptionEvent::BurnIntended',
     'RedemptionEvent::BurnRecoveryAttempted',
     'RedemptionEvent::BurnNonceTooLow',
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
