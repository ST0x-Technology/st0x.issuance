mod cmd;
mod event;
mod view;

pub(crate) mod detector;
pub(crate) mod journal_manager;
pub(crate) mod redeem_call_manager;

use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};

use crate::Quantity;
use crate::mint::{IssuerRequestId, TokenizationRequestId};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
pub(crate) use cmd::RedemptionCommand;
pub(crate) use event::RedemptionEvent;
pub(crate) use view::RedemptionView;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Redemption {
    Uninitialized,
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        detected_tx_hash: B256,
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
        detected_tx_hash: B256,
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
        detected_tx_hash: B256,
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

impl Default for Redemption {
    fn default() -> Self {
        Self::Uninitialized
    }
}

impl Redemption {
    const fn state_name(&self) -> &'static str {
        match self {
            Self::Uninitialized => "Uninitialized",
            Self::Detected { .. } => "Detected",
            Self::AlpacaCalled { .. } => "AlpacaCalled",
            Self::Burning { .. } => "Burning",
            Self::Failed { .. } => "Failed",
        }
    }

    pub(crate) const fn validation_fields(
        &self,
    ) -> Option<(
        &IssuerRequestId,
        &UnderlyingSymbol,
        &TokenSymbol,
        &Quantity,
        &Address,
        &B256,
    )> {
        match self {
            Self::AlpacaCalled {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                detected_tx_hash,
                ..
            }
            | Self::Burning {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                detected_tx_hash,
                ..
            } => Some((
                issuer_request_id,
                underlying,
                token,
                quantity,
                wallet,
                detected_tx_hash,
            )),
            _ => None,
        }
    }

    fn apply_alpaca_called(
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
            detected_tx_hash,
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
            detected_tx_hash: *detected_tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at,
        };
    }

    fn apply_alpaca_journal_completed(
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
            detected_tx_hash,
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
            detected_tx_hash: *detected_tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at: *called_at,
            alpaca_completed_at,
        };
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum RedemptionError {
    #[error("Redemption already detected for request: {issuer_request_id}")]
    AlreadyDetected { issuer_request_id: String },

    #[error("Invalid state for operation: expected {expected}, found {found}")]
    InvalidState { expected: String, found: String },
}

#[async_trait]
impl Aggregate for Redemption {
    type Command = RedemptionCommand;
    type Event = RedemptionEvent;
    type Error = RedemptionError;
    type Services = ();

    fn aggregate_type() -> String {
        "Redemption".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            RedemptionCommand::Detect {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
            } => {
                if !matches!(self, Self::Uninitialized) {
                    return Err(RedemptionError::AlreadyDetected {
                        issuer_request_id: issuer_request_id.0,
                    });
                }

                let now = Utc::now();

                Ok(vec![RedemptionEvent::Detected {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                    detected_at: now,
                }])
            }
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id,
                tokenization_request_id,
            } => {
                if !matches!(self, Self::Detected { .. }) {
                    return Err(RedemptionError::InvalidState {
                        expected: "Detected".to_string(),
                        found: self.state_name().to_string(),
                    });
                }

                let now = Utc::now();

                Ok(vec![RedemptionEvent::AlpacaCalled {
                    issuer_request_id,
                    tokenization_request_id,
                    called_at: now,
                }])
            }
            RedemptionCommand::RecordAlpacaFailure {
                issuer_request_id,
                error,
            } => {
                if !matches!(self, Self::Detected { .. }) {
                    return Err(RedemptionError::InvalidState {
                        expected: "Detected".to_string(),
                        found: self.state_name().to_string(),
                    });
                }

                let now = Utc::now();

                Ok(vec![RedemptionEvent::AlpacaCallFailed {
                    issuer_request_id,
                    error,
                    failed_at: now,
                }])
            }
            RedemptionCommand::ConfirmAlpacaComplete { issuer_request_id } => {
                if !matches!(self, Self::AlpacaCalled { .. }) {
                    return Err(RedemptionError::InvalidState {
                        expected: "AlpacaCalled".to_string(),
                        found: self.state_name().to_string(),
                    });
                }

                let now = Utc::now();

                Ok(vec![RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id,
                    alpaca_completed_at: now,
                }])
            }
            RedemptionCommand::MarkFailed { issuer_request_id, reason } => {
                let now = Utc::now();

                Ok(vec![RedemptionEvent::RedemptionFailed {
                    issuer_request_id,
                    reason,
                    failed_at: now,
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
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
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    detected_tx_hash: tx_hash,
                    block_number,
                    detected_at,
                };
            }
            RedemptionEvent::AlpacaCalled {
                issuer_request_id,
                tokenization_request_id,
                called_at,
            } => {
                self.apply_alpaca_called(
                    issuer_request_id,
                    tokenization_request_id,
                    called_at,
                );
            }
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id,
                error,
                failed_at,
            } => {
                *self = Self::Failed {
                    issuer_request_id,
                    reason: error,
                    failed_at,
                };
            }
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id,
                alpaca_completed_at,
            } => {
                self.apply_alpaca_journal_completed(
                    issuer_request_id,
                    alpaca_completed_at,
                );
            }
            RedemptionEvent::RedemptionFailed {
                issuer_request_id,
                reason,
                failed_at,
            } => {
                *self = Self::Failed { issuer_request_id, reason, failed_at };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use chrono::Utc;
    use cqrs_es::{Aggregate, test::TestFramework};
    use rust_decimal::Decimal;

    use super::{
        Redemption, RedemptionCommand, RedemptionError, RedemptionEvent,
    };
    use crate::mint::{IssuerRequestId, Quantity};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    type RedemptionTestFramework = TestFramework<Redemption>;

    #[test]
    fn test_detect_redemption_creates_event() {
        let issuer_request_id = IssuerRequestId::new("red-123");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        let validator = RedemptionTestFramework::with(())
            .given_no_previous_events()
            .when(RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::Detected {
            issuer_request_id: event_id,
            underlying: event_underlying,
            token: event_token,
            wallet: event_wallet,
            quantity: event_quantity,
            tx_hash: event_tx_hash,
            block_number: event_block_number,
            detected_at,
        } = &events[0]
        else {
            panic!("Expected Detected event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_underlying, &underlying);
        assert_eq!(event_token, &token);
        assert_eq!(event_wallet, &wallet);
        assert_eq!(event_quantity, &quantity);
        assert_eq!(event_tx_hash, &tx_hash);
        assert_eq!(event_block_number, &block_number);
        assert!(detected_at.timestamp() > 0);
    }

    #[test]
    fn test_detect_redemption_when_already_detected_returns_error() {
        let issuer_request_id = IssuerRequestId::new("red-456");
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;

        RedemptionTestFramework::with(())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
            })
            .then_expect_error(RedemptionError::AlreadyDetected {
                issuer_request_id: issuer_request_id.0,
            });
    }

    #[test]
    fn test_apply_detected_event_updates_state() {
        let mut redemption = Redemption::default();

        assert!(matches!(redemption, Redemption::Uninitialized));

        let issuer_request_id = IssuerRequestId::new("red-789");
        let underlying = UnderlyingSymbol::new("NVDA");
        let token = TokenSymbol::new("tNVDA");
        let wallet = address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc");
        let quantity = Quantity::new(Decimal::from(25));
        let tx_hash = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        let block_number = 99999;
        let detected_at = Utc::now();

        redemption.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet,
            quantity: quantity.clone(),
            tx_hash,
            block_number,
            detected_at,
        });

        let Redemption::Detected {
            issuer_request_id: state_id,
            underlying: state_underlying,
            token: state_token,
            wallet: state_wallet,
            quantity: state_quantity,
            detected_tx_hash: state_tx_hash,
            block_number: state_block_number,
            detected_at: state_detected_at,
        } = redemption
        else {
            panic!("Expected Detected state, got Uninitialized");
        };

        assert_eq!(state_id, issuer_request_id);
        assert_eq!(state_underlying, underlying);
        assert_eq!(state_token, token);
        assert_eq!(state_wallet, wallet);
        assert_eq!(state_quantity, quantity);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_detected_at, detected_at);
    }

    #[test]
    fn test_record_alpaca_call_from_detected_state() {
        use crate::mint::TokenizationRequestId;

        let issuer_request_id = IssuerRequestId::new("red-call-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-tok-456");

        let validator = RedemptionTestFramework::with(())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                wallet: address!(
                    "0x1234567890abcdef1234567890abcdef12345678"
                ),
                quantity: Quantity::new(Decimal::from(100)),
                tx_hash: b256!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ),
                block_number: 12345,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::AlpacaCalled {
            issuer_request_id: event_id,
            tokenization_request_id: event_tok_id,
            called_at,
        } = &events[0]
        else {
            panic!("Expected AlpacaCalled event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_tok_id, &tokenization_request_id);
        assert!(called_at.timestamp() > 0);
    }

    #[test]
    fn test_record_alpaca_call_from_wrong_state_fails() {
        use crate::mint::TokenizationRequestId;

        let issuer_request_id = IssuerRequestId::new("red-call-fail-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-tok-789");

        RedemptionTestFramework::with(())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordAlpacaCall {
                issuer_request_id,
                tokenization_request_id,
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: "Uninitialized".to_string(),
            });
    }

    #[test]
    fn test_record_alpaca_failure_from_detected_state() {
        let issuer_request_id = IssuerRequestId::new("red-fail-123");
        let error = "API timeout".to_string();

        let validator = RedemptionTestFramework::with(())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("TSLA"),
                token: TokenSymbol::new("tTSLA"),
                wallet: address!(
                    "0x9876543210fedcba9876543210fedcba98765432"
                ),
                quantity: Quantity::new(Decimal::from(50)),
                tx_hash: b256!(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                ),
                block_number: 54321,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::RecordAlpacaFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: error.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::AlpacaCallFailed {
            issuer_request_id: event_id,
            error: event_error,
            failed_at,
        } = &events[0]
        else {
            panic!("Expected AlpacaCallFailed event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_error, &error);
        assert!(failed_at.timestamp() > 0);
    }

    #[test]
    fn test_record_alpaca_failure_from_wrong_state_fails() {
        let issuer_request_id = IssuerRequestId::new("red-fail-wrong-123");

        RedemptionTestFramework::with(())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordAlpacaFailure {
                issuer_request_id,
                error: "Some error".to_string(),
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: "Uninitialized".to_string(),
            });
    }

    #[test]
    fn test_confirm_alpaca_complete_from_alpaca_called_state() {
        use crate::mint::TokenizationRequestId;

        let issuer_request_id = IssuerRequestId::new("red-complete-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-complete-456");

        let validator = RedemptionTestFramework::with(())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    tx_hash: b256!(
                        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    ),
                    block_number: 12345,
                    detected_at: Utc::now(),
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id,
                    called_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::ConfirmAlpacaComplete {
                issuer_request_id: issuer_request_id.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: event_id,
            alpaca_completed_at,
        } = &events[0]
        else {
            panic!(
                "Expected AlpacaJournalCompleted event, got {:?}",
                &events[0]
            );
        };

        assert_eq!(event_id, &issuer_request_id);
        assert!(alpaca_completed_at.timestamp() > 0);
    }

    #[test]
    fn test_confirm_alpaca_complete_from_wrong_state_fails() {
        let issuer_request_id = IssuerRequestId::new("red-complete-wrong-123");

        RedemptionTestFramework::with(())
            .given_no_previous_events()
            .when(RedemptionCommand::ConfirmAlpacaComplete {
                issuer_request_id,
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "AlpacaCalled".to_string(),
                found: "Uninitialized".to_string(),
            });
    }

    #[test]
    fn test_apply_alpaca_journal_completed_transitions_to_burning() {
        use crate::mint::TokenizationRequestId;

        let mut redemption = Redemption::default();

        let issuer_request_id = IssuerRequestId::new("red-burning-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burning-456");
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
        let alpaca_completed_at = Utc::now();

        redemption.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet,
            quantity: quantity.clone(),
            tx_hash,
            block_number,
            detected_at,
        });

        redemption.apply(RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            called_at,
        });

        redemption.apply(RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: issuer_request_id.clone(),
            alpaca_completed_at,
        });

        let Redemption::Burning {
            issuer_request_id: state_id,
            tokenization_request_id: state_tok_id,
            underlying: state_underlying,
            token: state_token,
            wallet: state_wallet,
            quantity: state_quantity,
            detected_tx_hash: state_tx_hash,
            block_number: state_block_number,
            detected_at: state_detected_at,
            called_at: state_called_at,
            alpaca_completed_at: state_alpaca_completed_at,
        } = redemption
        else {
            panic!("Expected Burning state, got {redemption:?}");
        };

        assert_eq!(state_id, issuer_request_id);
        assert_eq!(state_tok_id, tokenization_request_id);
        assert_eq!(state_underlying, underlying);
        assert_eq!(state_token, token);
        assert_eq!(state_wallet, wallet);
        assert_eq!(state_quantity, quantity);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_detected_at, detected_at);
        assert_eq!(state_called_at, called_at);
        assert_eq!(state_alpaca_completed_at, alpaca_completed_at);
    }
}
