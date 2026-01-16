pub(crate) mod burn_manager;
pub(crate) mod detector;
pub(crate) mod journal_manager;
pub(crate) mod redeem_call_manager;

use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use crate::Quantity;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::mint::{IssuerRequestId, TokenizationRequestId};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionCommand {
    Detect {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
    },
    RecordAlpacaCall {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
    },
    RecordAlpacaFailure {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
    ConfirmAlpacaComplete {
        issuer_request_id: IssuerRequestId,
    },
    MarkFailed {
        issuer_request_id: IssuerRequestId,
        reason: String,
    },
    RecordBurnSuccess {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_burned: U256,
        gas_used: u64,
        block_number: u64,
    },
    RecordBurnFailure {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum RedemptionEvent {
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
        called_at: DateTime<Utc>,
    },
    AlpacaCallFailed {
        issuer_request_id: IssuerRequestId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    AlpacaJournalCompleted {
        issuer_request_id: IssuerRequestId,
        alpaca_journal_completed_at: DateTime<Utc>,
    },
    RedemptionFailed {
        issuer_request_id: IssuerRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    TokensBurned {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_burned: U256,
        gas_used: u64,
        block_number: u64,
        burned_at: DateTime<Utc>,
    },
    BurningFailed {
        issuer_request_id: IssuerRequestId,
        error: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for RedemptionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Detected { .. } => "RedemptionEvent::Detected".to_string(),
            Self::AlpacaCalled { .. } => {
                "RedemptionEvent::AlpacaCalled".to_string()
            }
            Self::AlpacaCallFailed { .. } => {
                "RedemptionEvent::AlpacaCallFailed".to_string()
            }
            Self::AlpacaJournalCompleted { .. } => {
                "RedemptionEvent::AlpacaJournalCompleted".to_string()
            }
            Self::RedemptionFailed { .. } => {
                "RedemptionEvent::RedemptionFailed".to_string()
            }
            Self::TokensBurned { .. } => {
                "RedemptionEvent::TokensBurned".to_string()
            }
            Self::BurningFailed { .. } => {
                "RedemptionEvent::BurningFailed".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RedemptionMetadata {
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) wallet: Address,
    pub(crate) quantity: Quantity,
    pub(crate) detected_tx_hash: B256,
    pub(crate) block_number: u64,
    pub(crate) detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Redemption {
    Detected {
        metadata: RedemptionMetadata,
    },
    AlpacaCalled {
        metadata: RedemptionMetadata,
        tokenization_request_id: TokenizationRequestId,
        called_at: DateTime<Utc>,
    },
    Burning {
        metadata: RedemptionMetadata,
        tokenization_request_id: TokenizationRequestId,
        called_at: DateTime<Utc>,
        alpaca_journal_completed_at: DateTime<Utc>,
    },
    Completed {
        issuer_request_id: IssuerRequestId,
        burn_tx_hash: B256,
        completed_at: DateTime<Utc>,
    },
    Failed {
        issuer_request_id: IssuerRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl Redemption {
    pub(crate) const fn metadata(&self) -> Option<&RedemptionMetadata> {
        match self {
            Self::Detected { metadata }
            | Self::AlpacaCalled { metadata, .. }
            | Self::Burning { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    const fn state_name(&self) -> &'static str {
        match self {
            Self::Detected { .. } => "Detected",
            Self::AlpacaCalled { .. } => "AlpacaCalled",
            Self::Burning { .. } => "Burning",
            Self::Completed { .. } => "Completed",
            Self::Failed { .. } => "Failed",
        }
    }

    fn from_event(
        event: &RedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
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
            } => Ok(Self::Detected {
                metadata: RedemptionMetadata {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token: token.clone(),
                    wallet: *wallet,
                    quantity: quantity.clone(),
                    detected_tx_hash: *tx_hash,
                    block_number: *block_number,
                    detected_at: *detected_at,
                },
            }),
            other => Err(LifecycleError::Mismatch {
                state: "Uninitialized".to_string(),
                event: other.event_type(),
            }),
        }
    }

    fn apply_transition(
        event: &RedemptionEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match (current, event) {
            (
                Self::Detected { metadata },
                RedemptionEvent::AlpacaCalled {
                    tokenization_request_id,
                    called_at,
                    ..
                },
            ) => Ok(Self::AlpacaCalled {
                metadata: metadata.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                called_at: *called_at,
            }),

            (
                Self::Detected { .. } | Self::Burning { .. },
                RedemptionEvent::AlpacaCallFailed {
                    issuer_request_id,
                    error,
                    failed_at,
                },
            )
            | (
                Self::Burning { .. },
                RedemptionEvent::BurningFailed {
                    issuer_request_id,
                    error,
                    failed_at,
                },
            ) => Ok(Self::Failed {
                issuer_request_id: issuer_request_id.clone(),
                reason: error.clone(),
                failed_at: *failed_at,
            }),

            (
                Self::Detected { .. }
                | Self::AlpacaCalled { .. }
                | Self::Burning { .. },
                RedemptionEvent::RedemptionFailed {
                    issuer_request_id,
                    reason,
                    failed_at,
                },
            ) => Ok(Self::Failed {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
                failed_at: *failed_at,
            }),

            (
                Self::AlpacaCalled {
                    metadata,
                    tokenization_request_id,
                    called_at,
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    alpaca_journal_completed_at,
                    ..
                },
            ) => Ok(Self::Burning {
                metadata: metadata.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                called_at: *called_at,
                alpaca_journal_completed_at: *alpaca_journal_completed_at,
            }),

            (
                Self::Burning { .. },
                RedemptionEvent::TokensBurned {
                    issuer_request_id,
                    tx_hash,
                    burned_at,
                    ..
                },
            ) => Ok(Self::Completed {
                issuer_request_id: issuer_request_id.clone(),
                burn_tx_hash: *tx_hash,
                completed_at: *burned_at,
            }),

            (current, event) => Err(LifecycleError::Mismatch {
                state: current.state_name().to_string(),
                event: event.event_type(),
            }),
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum RedemptionError {
    #[error("Redemption already detected for request: {issuer_request_id}")]
    AlreadyDetected { issuer_request_id: String },

    #[error("Invalid state for operation: expected {expected}, found {found}")]
    InvalidState { expected: String, found: String },
}

struct DetectionData {
    issuer_request_id: IssuerRequestId,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    wallet: Address,
    quantity: Quantity,
    tx_hash: B256,
    block_number: u64,
}

impl Lifecycle<Redemption, Never> {
    fn handle_detect(
        &self,
        data: DetectionData,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if self.live().is_ok() {
            return Err(RedemptionError::AlreadyDetected {
                issuer_request_id: data.issuer_request_id.0,
            });
        }

        Ok(vec![RedemptionEvent::Detected {
            issuer_request_id: data.issuer_request_id,
            underlying: data.underlying,
            token: data.token,
            wallet: data.wallet,
            quantity: data.quantity,
            tx_hash: data.tx_hash,
            block_number: data.block_number,
            detected_at: Utc::now(),
        }])
    }

    fn handle_record_alpaca_call(
        &self,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let Ok(inner) = self.live() else {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: "Uninitialized or Failed".to_string(),
            });
        };

        if !matches!(inner, Redemption::Detected { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: inner.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::AlpacaCalled {
            issuer_request_id,
            tokenization_request_id,
            called_at: Utc::now(),
        }])
    }

    fn handle_record_alpaca_failure(
        &self,
        issuer_request_id: IssuerRequestId,
        error: String,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let Ok(inner) = self.live() else {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: "Uninitialized or Failed".to_string(),
            });
        };

        if !matches!(inner, Redemption::Detected { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: inner.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::AlpacaCallFailed {
            issuer_request_id,
            error,
            failed_at: Utc::now(),
        }])
    }

    fn handle_confirm_alpaca_complete(
        &self,
        issuer_request_id: IssuerRequestId,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let Ok(inner) = self.live() else {
            return Err(RedemptionError::InvalidState {
                expected: "AlpacaCalled".to_string(),
                found: "Uninitialized or Failed".to_string(),
            });
        };

        if !matches!(inner, Redemption::AlpacaCalled { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "AlpacaCalled".to_string(),
                found: inner.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id,
            alpaca_journal_completed_at: Utc::now(),
        }])
    }

    fn handle_mark_failed(
        &self,
        issuer_request_id: IssuerRequestId,
        reason: String,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let Ok(inner) = self.live() else {
            return Err(RedemptionError::InvalidState {
                expected: "Detected, AlpacaCalled, or Burning".to_string(),
                found: "Uninitialized or Failed".to_string(),
            });
        };

        if !matches!(
            inner,
            Redemption::Detected { .. }
                | Redemption::AlpacaCalled { .. }
                | Redemption::Burning { .. }
        ) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected, AlpacaCalled, or Burning".to_string(),
                found: inner.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::RedemptionFailed {
            issuer_request_id,
            reason,
            failed_at: Utc::now(),
        }])
    }

    fn handle_record_burn_success(
        &self,
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_burned: U256,
        gas_used: u64,
        block_number: u64,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let Ok(inner) = self.live() else {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: "Uninitialized or Failed".to_string(),
            });
        };

        if !matches!(inner, Redemption::Burning { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: inner.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::TokensBurned {
            issuer_request_id,
            tx_hash,
            receipt_id,
            shares_burned,
            gas_used,
            block_number,
            burned_at: Utc::now(),
        }])
    }

    fn handle_record_burn_failure(
        &self,
        issuer_request_id: IssuerRequestId,
        error: String,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let Ok(inner) = self.live() else {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: "Uninitialized or Failed".to_string(),
            });
        };

        if !matches!(inner, Redemption::Burning { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: inner.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::BurningFailed {
            issuer_request_id,
            error,
            failed_at: Utc::now(),
        }])
    }
}

#[async_trait::async_trait]
impl cqrs_es::Aggregate for Lifecycle<Redemption, Never> {
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
            } => self.handle_detect(DetectionData {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
            }),
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id,
                tokenization_request_id,
            } => self.handle_record_alpaca_call(
                issuer_request_id,
                tokenization_request_id,
            ),
            RedemptionCommand::RecordAlpacaFailure {
                issuer_request_id,
                error,
            } => self.handle_record_alpaca_failure(issuer_request_id, error),
            RedemptionCommand::ConfirmAlpacaComplete { issuer_request_id } => {
                self.handle_confirm_alpaca_complete(issuer_request_id)
            }
            RedemptionCommand::MarkFailed { issuer_request_id, reason } => {
                self.handle_mark_failed(issuer_request_id, reason)
            }
            RedemptionCommand::RecordBurnSuccess {
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_burned,
                gas_used,
                block_number,
            } => self.handle_record_burn_success(
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_burned,
                gas_used,
                block_number,
            ),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id,
                error,
            } => self.handle_record_burn_failure(issuer_request_id, error),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, Redemption::apply_transition)
            .or_initialize(&event, Redemption::from_event);
    }
}

impl cqrs_es::View<Self> for Lifecycle<Redemption, Never> {
    fn update(&mut self, event: &cqrs_es::EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, Redemption::apply_transition)
            .or_initialize(&event.payload, Redemption::from_event);
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use chrono::Utc;
    use cqrs_es::{Aggregate, test::TestFramework};
    use rust_decimal::Decimal;

    use super::*;

    type RedemptionTestFramework = TestFramework<Lifecycle<Redemption, Never>>;

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
        let mut lifecycle: Lifecycle<Redemption, Never> = Lifecycle::default();

        assert!(matches!(lifecycle, Lifecycle::Uninitialized));

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

        lifecycle.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet,
            quantity: quantity.clone(),
            tx_hash,
            block_number,
            detected_at,
        });

        assert_eq!(
            lifecycle.live().unwrap(),
            &Redemption::Detected {
                metadata: RedemptionMetadata {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    detected_tx_hash: tx_hash,
                    block_number,
                    detected_at,
                }
            }
        );
    }

    #[test]
    fn test_record_alpaca_call_from_detected_state() {
        let issuer_request_id = IssuerRequestId::new("red-call-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-tok-456");

        let validator = RedemptionTestFramework::with(())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
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
                found: "Uninitialized or Failed".to_string(),
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
                wallet: address!("0x9876543210fedcba9876543210fedcba98765432"),
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
                found: "Uninitialized or Failed".to_string(),
            });
    }

    #[test]
    fn test_confirm_alpaca_complete_from_alpaca_called_state() {
        let issuer_request_id = IssuerRequestId::new("red-complete-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-complete-456");

        let validator = RedemptionTestFramework::with(())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
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
            alpaca_journal_completed_at,
        } = &events[0]
        else {
            panic!(
                "Expected AlpacaJournalCompleted event, got {:?}",
                &events[0]
            );
        };

        assert_eq!(event_id, &issuer_request_id);
        assert!(alpaca_journal_completed_at.timestamp() > 0);
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
                found: "Uninitialized or Failed".to_string(),
            });
    }

    #[test]
    fn test_apply_alpaca_journal_completed_transitions_to_burning() {
        let mut lifecycle: Lifecycle<Redemption, Never> = Lifecycle::default();

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
        let alpaca_journal_completed_at = Utc::now();

        lifecycle.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet,
            quantity: quantity.clone(),
            tx_hash,
            block_number,
            detected_at,
        });

        lifecycle.apply(RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            called_at,
        });

        lifecycle.apply(RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: issuer_request_id.clone(),
            alpaca_journal_completed_at,
        });

        assert_eq!(
            lifecycle.live().unwrap(),
            &Redemption::Burning {
                metadata: RedemptionMetadata {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    detected_tx_hash: tx_hash,
                    block_number,
                    detected_at,
                },
                tokenization_request_id,
                called_at,
                alpaca_journal_completed_at,
            }
        );
    }

    #[test]
    fn test_record_burn_success_from_burning_state() {
        let issuer_request_id = IssuerRequestId::new("red-burn-success-123");
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let receipt_id = uint!(42_U256);
        let shares_burned = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 20000;

        let validator = RedemptionTestFramework::with(())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("TSLA"),
                    token: TokenSymbol::new("tTSLA"),
                    wallet: address!("0x9876543210fedcba9876543210fedcba98765432"),
                    quantity: Quantity::new(Decimal::from(100)),
                    tx_hash: b256!(
                        "0x1111111111111111111111111111111111111111111111111111111111111111"
                    ),
                    block_number: 10000,
                    detected_at: Utc::now(),
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new("alp-burn-456"),
                    called_at: Utc::now(),
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::RecordBurnSuccess {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash,
                receipt_id,
                shares_burned,
                gas_used,
                block_number,
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::TokensBurned {
            issuer_request_id: event_id,
            tx_hash: event_tx_hash,
            receipt_id: event_receipt_id,
            shares_burned: event_shares_burned,
            gas_used: event_gas_used,
            block_number: event_block_number,
            burned_at,
        } = &events[0]
        else {
            panic!("Expected TokensBurned event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_tx_hash, &tx_hash);
        assert_eq!(event_receipt_id, &receipt_id);
        assert_eq!(event_shares_burned, &shares_burned);
        assert_eq!(event_gas_used, &gas_used);
        assert_eq!(event_block_number, &block_number);
        assert!(burned_at.timestamp() > 0);
    }

    #[test]
    fn test_record_burn_success_from_wrong_state_fails() {
        let issuer_request_id = IssuerRequestId::new("red-burn-wrong-123");

        RedemptionTestFramework::with(())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("NVDA"),
                token: TokenSymbol::new("tNVDA"),
                wallet: address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc"),
                quantity: Quantity::new(Decimal::from(25)),
                tx_hash: b256!(
                    "0x2222222222222222222222222222222222222222222222222222222222222222"
                ),
                block_number: 15000,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::RecordBurnSuccess {
                issuer_request_id,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                receipt_id: uint!(1_U256),
                shares_burned: uint!(25_000000000000000000_U256),
                gas_used: 40000,
                block_number: 16000,
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: "Detected".to_string(),
            });
    }

    #[test]
    fn test_record_burn_failure_from_burning_state() {
        let issuer_request_id = IssuerRequestId::new("red-burn-fail-123");
        let error = "Insufficient gas".to_string();

        let validator = RedemptionTestFramework::with(())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("GOOG"),
                    token: TokenSymbol::new("tGOOG"),
                    wallet: address!("0xabababababababababababababababababababab"),
                    quantity: Quantity::new(Decimal::from(50)),
                    tx_hash: b256!(
                        "0x3333333333333333333333333333333333333333333333333333333333333333"
                    ),
                    block_number: 30000,
                    detected_at: Utc::now(),
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new("alp-fail-789"),
                    called_at: Utc::now(),
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: error.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurningFailed {
            issuer_request_id: event_id,
            error: event_error,
            failed_at,
        } = &events[0]
        else {
            panic!("Expected BurningFailed event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_error, &error);
        assert!(failed_at.timestamp() > 0);
    }

    #[test]
    fn test_record_burn_failure_from_wrong_state_fails() {
        let issuer_request_id = IssuerRequestId::new("red-fail-wrong-123");

        RedemptionTestFramework::with(())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordBurnFailure {
                issuer_request_id,
                error: "Some error".to_string(),
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: "Uninitialized or Failed".to_string(),
            });
    }

    #[test]
    fn test_apply_tokens_burned_transitions_to_completed() {
        let mut lifecycle: Lifecycle<Redemption, Never> = Lifecycle::default();

        let issuer_request_id = IssuerRequestId::new("red-complete-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-complete-456");
        let underlying = UnderlyingSymbol::new("AMZN");
        let token = TokenSymbol::new("tAMZN");
        let wallet = address!("0xefefefefefefefefefefefefefefefefefefefef");
        let quantity = Quantity::new(Decimal::from(200));
        let detected_tx_hash = b256!(
            "0x5555555555555555555555555555555555555555555555555555555555555555"
        );
        let block_number = 50000;
        let detected_at = Utc::now();
        let called_at = Utc::now();
        let alpaca_journal_completed_at = Utc::now();
        let burn_tx_hash = b256!(
            "0x6666666666666666666666666666666666666666666666666666666666666666"
        );
        let receipt_id = uint!(99_U256);
        let shares_burned = uint!(200_000000000000000000_U256);
        let burned_at = Utc::now();

        lifecycle.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token,
            wallet,
            quantity,
            tx_hash: detected_tx_hash,
            block_number,
            detected_at,
        });

        lifecycle.apply(RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            called_at,
        });

        lifecycle.apply(RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: issuer_request_id.clone(),
            alpaca_journal_completed_at,
        });

        lifecycle.apply(RedemptionEvent::TokensBurned {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: burn_tx_hash,
            receipt_id,
            shares_burned,
            gas_used: 60000,
            block_number: 51000,
            burned_at,
        });

        assert_eq!(
            lifecycle.live().unwrap(),
            &Redemption::Completed {
                issuer_request_id,
                burn_tx_hash,
                completed_at: burned_at,
            }
        );
    }

    #[test]
    fn test_apply_burning_failed_transitions_to_failed() {
        let mut lifecycle: Lifecycle<Redemption, Never> = Lifecycle::default();

        let issuer_request_id = IssuerRequestId::new("red-failed-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-failed-456");
        let underlying = UnderlyingSymbol::new("NFLX");
        let token = TokenSymbol::new("tNFLX");
        let wallet = address!("0x1212121212121212121212121212121212121212");
        let quantity = Quantity::new(Decimal::from(150));
        let tx_hash = b256!(
            "0x7777777777777777777777777777777777777777777777777777777777777777"
        );
        let block_number = 60000;
        let detected_at = Utc::now();
        let called_at = Utc::now();
        let alpaca_journal_completed_at = Utc::now();
        let error = "Transaction reverted".to_string();
        let failed_at = Utc::now();

        lifecycle.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
        });

        lifecycle.apply(RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            called_at,
        });

        lifecycle.apply(RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: issuer_request_id.clone(),
            alpaca_journal_completed_at,
        });

        lifecycle.apply(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: error.clone(),
            failed_at,
        });

        assert_eq!(
            lifecycle.live().unwrap(),
            &Redemption::Failed { issuer_request_id, reason: error, failed_at }
        );
    }
}
