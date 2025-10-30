use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
use crate::redemption::{Redemption, RedemptionEvent};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionView {
    Unavailable,
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
    AlpacaCalled {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        called_at: DateTime<Utc>,
    },
    Burning {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        called_at: DateTime<Utc>,
        alpaca_completed_at: DateTime<Utc>,
    },
    Failed {
        issuer_request_id: IssuerRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl Default for RedemptionView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl RedemptionView {
    fn update_alpaca_called(
        &mut self,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        called_at: DateTime<Utc>,
    ) {
        let Self::Detected {
            underlying,
            token,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
            ..
        } = self
        else {
            return;
        };

        *self = Self::AlpacaCalled {
            issuer_request_id,
            tokenization_request_id,
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: *wallet,
            quantity: quantity.clone(),
            tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at,
        };
    }

    fn update_alpaca_journal_completed(
        &mut self,
        issuer_request_id: IssuerRequestId,
        alpaca_completed_at: DateTime<Utc>,
    ) {
        let Self::AlpacaCalled {
            tokenization_request_id,
            underlying,
            token,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
            called_at,
            ..
        } = self
        else {
            return;
        };

        *self = Self::Burning {
            issuer_request_id,
            tokenization_request_id: tokenization_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: *wallet,
            quantity: quantity.clone(),
            tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at: *called_at,
            alpaca_completed_at,
        };
    }
}

impl View<Redemption> for RedemptionView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        match &event.payload {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            } => {
                *self = Self::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token: token.clone(),
                    wallet: *wallet,
                    quantity: quantity.clone(),
                    tx_hash: *tx_hash,
                    block_number: *block_number,
                    detected_at: *detected_at,
                };
            }
            RedemptionEvent::AlpacaCalled {
                issuer_request_id,
                tokenization_request_id,
                called_at,
            } => {
                self.update_alpaca_called(
                    issuer_request_id.clone(),
                    tokenization_request_id.clone(),
                    *called_at,
                );
            }
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id,
                error,
                failed_at,
            } => {
                *self = Self::Failed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: error.clone(),
                    failed_at: *failed_at,
                };
            }
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id,
                alpaca_completed_at,
            } => {
                self.update_alpaca_journal_completed(
                    issuer_request_id.clone(),
                    *alpaca_completed_at,
                );
            }
            RedemptionEvent::RedemptionFailed {
                issuer_request_id,
                reason,
                failed_at,
            } => {
                *self = Self::Failed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: reason.clone(),
                    failed_at: *failed_at,
                };
            }
            RedemptionEvent::BurningStarted { .. }
            | RedemptionEvent::TokensBurned { .. }
            | RedemptionEvent::BurningFailed { .. } => {
                // TODO(Task 2): Implement burn event handling in view
                // - BurningStarted: possibly no-op
                // - TokensBurned: transition to Completed state (final success state)
                // - BurningFailed: transition to Failed state
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use chrono::Utc;
    use cqrs_es::{EventEnvelope, View};
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    use super::RedemptionView;
    use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
    use crate::redemption::{Redemption, RedemptionEvent};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    #[test]
    fn test_view_starts_as_unavailable() {
        let view = RedemptionView::default();
        assert!(matches!(view, RedemptionView::Unavailable));
    }

    #[test]
    fn test_view_updates_on_detected_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-view-123");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;
        let detected_at = Utc::now();

        let event = EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        };

        view.update(&event);

        let RedemptionView::Detected {
            issuer_request_id: view_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
        } = view
        else {
            panic!("Expected Detected view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
    }

    #[test]
    fn test_view_updates_on_alpaca_called_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-alpaca-called-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-tok-456");
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;
        let detected_at = Utc::now();
        let called_at = Utc::now();

        let detected_event = EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        };

        view.update(&detected_event);

        let alpaca_called_event = EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                called_at,
            },
            metadata: HashMap::default(),
        };

        view.update(&alpaca_called_event);

        let RedemptionView::AlpacaCalled {
            issuer_request_id: view_id,
            tokenization_request_id: view_tok_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
            called_at: view_called_at,
        } = view
        else {
            panic!("Expected AlpacaCalled view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tok_id, tokenization_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
        assert_eq!(view_called_at, called_at);
    }

    #[test]
    fn test_view_updates_on_alpaca_journal_completed_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-burning-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burning-456");
        let underlying = UnderlyingSymbol::new("NVDA");
        let token = TokenSymbol::new("tNVDA");
        let wallet = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let quantity = Quantity::new(Decimal::from(25));
        let tx_hash = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        let block_number = 99999;
        let detected_at = Utc::now();
        let called_at = Utc::now();
        let alpaca_completed_at = Utc::now();

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                called_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 3,
            payload: RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_completed_at,
            },
            metadata: HashMap::default(),
        });

        let RedemptionView::Burning {
            issuer_request_id: view_id,
            tokenization_request_id: view_tok_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
            called_at: view_called_at,
            alpaca_completed_at: view_alpaca_completed_at,
        } = view
        else {
            panic!("Expected Burning view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tok_id, tokenization_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
        assert_eq!(view_called_at, called_at);
        assert_eq!(view_alpaca_completed_at, alpaca_completed_at);
    }

    #[test]
    fn test_view_updates_on_redemption_failed_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-failed-123");
        let underlying = UnderlyingSymbol::new("META");
        let token = TokenSymbol::new("tMETA");
        let wallet = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let quantity = Quantity::new(Decimal::from(75));
        let tx_hash = b256!(
            "0x3333333333333333333333333333333333333333333333333333333333333333"
        );
        let block_number = 11111;
        let detected_at = Utc::now();
        let reason = "Alpaca journal timeout".to_string();
        let failed_at = Utc::now();

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::RedemptionFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
                failed_at,
            },
            metadata: HashMap::default(),
        });

        let RedemptionView::Failed {
            issuer_request_id: view_id,
            reason: view_reason,
            failed_at: view_failed_at,
        } = view
        else {
            panic!("Expected Failed view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_reason, reason);
        assert_eq!(view_failed_at, failed_at);
    }

    #[test]
    fn test_view_updates_on_alpaca_call_failed_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-call-failed-123");
        let underlying = UnderlyingSymbol::new("GOOGL");
        let token = TokenSymbol::new("tGOOGL");
        let wallet = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let quantity = Quantity::new(Decimal::from(10));
        let tx_hash = b256!(
            "0x4444444444444444444444444444444444444444444444444444444444444444"
        );
        let block_number = 22222;
        let detected_at = Utc::now();
        let error = "Alpaca API timeout".to_string();
        let failed_at = Utc::now();

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCallFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: error.clone(),
                failed_at,
            },
            metadata: HashMap::default(),
        });

        let RedemptionView::Failed {
            issuer_request_id: view_id,
            reason: view_reason,
            failed_at: view_failed_at,
        } = view
        else {
            panic!("Expected Failed view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_reason, error);
        assert_eq!(view_failed_at, failed_at);
    }
}
