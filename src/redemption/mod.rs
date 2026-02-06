mod cmd;
mod event;
mod view;

pub(crate) mod burn_manager;
pub(crate) mod detector;
pub(crate) mod journal_manager;
pub(crate) mod redeem_call_manager;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

use crate::Quantity;
use crate::mint::{IssuerRequestId, TokenizationRequestId};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
use crate::vault::{BurnParams, ReceiptInformation, VaultService};

pub(crate) use cmd::RedemptionCommand;
pub(crate) use event::{BurnRecord, RedemptionEvent};
pub(crate) use view::{
    RedemptionView, RedemptionViewError, find_alpaca_called, find_detected,
    replay_redemption_view,
};

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
    Uninitialized,
    Detected {
        metadata: RedemptionMetadata,
    },
    AlpacaCalled {
        metadata: RedemptionMetadata,
        tokenization_request_id: TokenizationRequestId,
        /// Quantity sent to Alpaca (truncated to 9 decimals)
        alpaca_quantity: Quantity,
        /// Dust quantity to be returned to user
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
    },
    Burning {
        metadata: RedemptionMetadata,
        tokenization_request_id: TokenizationRequestId,
        /// Quantity to burn (what Alpaca processed, 9 decimals)
        alpaca_quantity: Quantity,
        /// Dust quantity to return to user
        dust_quantity: Quantity,
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

impl Default for Redemption {
    fn default() -> Self {
        Self::Uninitialized
    }
}

/// Input parameters for the BurnTokens command handler.
///
/// Groups burn-related parameters to reduce argument count. The `user` field
/// is derived from aggregate state, not passed in the command.
struct BurnInput {
    vault: Address,
    burn_shares: U256,
    dust_shares: U256,
    receipt_id: U256,
    owner: Address,
    receipt_info: ReceiptInformation,
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

    /// Returns the quantity sent to Alpaca (truncated to 9 decimals).
    /// Only available in AlpacaCalled and Burning states.
    pub(crate) const fn alpaca_quantity(&self) -> Option<&Quantity> {
        match self {
            Self::AlpacaCalled { alpaca_quantity, .. }
            | Self::Burning { alpaca_quantity, .. } => Some(alpaca_quantity),
            _ => None,
        }
    }

    const fn state_name(&self) -> &'static str {
        match self {
            Self::Uninitialized => "Uninitialized",
            Self::Detected { .. } => "Detected",
            Self::AlpacaCalled { .. } => "AlpacaCalled",
            Self::Burning { .. } => "Burning",
            Self::Completed { .. } => "Completed",
            Self::Failed { .. } => "Failed",
        }
    }

    fn handle_record_alpaca_call(
        &self,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(self, Self::Detected { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: self.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::AlpacaCalled {
            issuer_request_id,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at: Utc::now(),
        }])
    }

    fn handle_record_alpaca_failure(
        &self,
        issuer_request_id: IssuerRequestId,
        error: String,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(self, Self::Detected { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: self.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::AlpacaCallFailed {
            issuer_request_id,
            error,
            failed_at: Utc::now(),
        }])
    }

    fn handle_mark_failed(
        &self,
        issuer_request_id: IssuerRequestId,
        reason: String,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(
            self,
            Self::Detected { .. }
                | Self::AlpacaCalled { .. }
                | Self::Burning { .. }
        ) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected, AlpacaCalled, or Burning".to_string(),
                found: self.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::RedemptionFailed {
            issuer_request_id,
            reason,
            failed_at: Utc::now(),
        }])
    }

    async fn handle_burn_tokens(
        &self,
        services: &Arc<dyn VaultService>,
        issuer_request_id: IssuerRequestId,
        input: BurnInput,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let Self::Burning { metadata, .. } = self else {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: self.state_name().to_string(),
            });
        };

        let user_wallet = metadata.wallet;

        let burn = services
            .burn_and_return_dust(BurnParams {
                vault: input.vault,
                burn_shares: input.burn_shares,
                dust_shares: input.dust_shares,
                receipt_id: input.receipt_id,
                owner: input.owner,
                user: user_wallet,
                receipt_info: input.receipt_info,
            })
            .await?;

        // TODO Task 7: Convert single burn result to v2.0 event format
        let _ = burn;
        todo!("Task 7: Build TokensBurned v2.0 event from burn result")
    }

    fn handle_record_burn_failure(
        &self,
        issuer_request_id: IssuerRequestId,
        error: String,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(self, Self::Burning { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: self.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::BurningFailed {
            issuer_request_id,
            error,
            failed_at: Utc::now(),
        }])
    }

    async fn handle_retry_burn(
        &self,
        services: &Arc<dyn VaultService>,
        issuer_request_id: IssuerRequestId,
        input: BurnInput,
        user_wallet: Address,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(self, Self::Failed { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: self.state_name().to_string(),
            });
        }

        let burn = services
            .burn_and_return_dust(BurnParams {
                vault: input.vault,
                burn_shares: input.burn_shares,
                dust_shares: input.dust_shares,
                receipt_id: input.receipt_id,
                owner: input.owner,
                user: user_wallet,
                receipt_info: input.receipt_info,
            })
            .await?;

        // TODO Task 7: Convert single burn result to v2.0 event format
        let _ = burn;
        todo!("Task 7: Build TokensBurned v2.0 event from burn result")
    }

    fn handle_confirm_alpaca_complete(
        &self,
        issuer_request_id: IssuerRequestId,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(self, Self::AlpacaCalled { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "AlpacaCalled".to_string(),
                found: self.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id,
            alpaca_journal_completed_at: Utc::now(),
        }])
    }

    fn apply_alpaca_called(
        &mut self,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
    ) {
        let Self::Detected { metadata } = self else {
            warn!(
                issuer_request_id = %issuer_request_id.0,
                current_state = %self.state_name(),
                "AlpacaCalled event received in wrong state, expected Detected"
            );
            return;
        };

        *self = Self::AlpacaCalled {
            metadata: metadata.clone(),
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
        };
    }

    fn apply_alpaca_journal_completed(
        &mut self,
        issuer_request_id: &IssuerRequestId,
        alpaca_journal_completed_at: DateTime<Utc>,
    ) {
        let Self::AlpacaCalled {
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
        } = self
        else {
            warn!(
                issuer_request_id = %issuer_request_id.0,
                current_state = %self.state_name(),
                "AlpacaJournalCompleted event received in wrong state, expected AlpacaCalled"
            );
            return;
        };

        *self = Self::Burning {
            metadata: metadata.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            alpaca_quantity: alpaca_quantity.clone(),
            dust_quantity: dust_quantity.clone(),
            called_at: *called_at,
            alpaca_journal_completed_at,
        };
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RedemptionError {
    #[error("Redemption already detected for request: {issuer_request_id}")]
    AlreadyDetected { issuer_request_id: String },

    #[error("Invalid state for operation: expected {expected}, found {found}")]
    InvalidState { expected: String, found: String },

    #[error("Burn operation failed: {0}")]
    BurnFailed(#[from] crate::vault::VaultError),
}

impl PartialEq for RedemptionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::AlreadyDetected { issuer_request_id: a },
                Self::AlreadyDetected { issuer_request_id: b },
            ) => a == b,
            (
                Self::InvalidState { expected: e1, found: f1 },
                Self::InvalidState { expected: e2, found: f2 },
            ) => e1 == e2 && f1 == f2,
            (Self::BurnFailed(a), Self::BurnFailed(b)) => {
                a.to_string() == b.to_string()
            }
            _ => false,
        }
    }
}

#[async_trait]
impl Aggregate for Redemption {
    type Command = RedemptionCommand;
    type Event = RedemptionEvent;
    type Error = RedemptionError;
    type Services = Arc<dyn VaultService>;

    fn aggregate_type() -> String {
        "Redemption".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
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

                Ok(vec![RedemptionEvent::Detected {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                    detected_at: Utc::now(),
                }])
            }
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
            } => self.handle_record_alpaca_call(
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
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
            RedemptionCommand::BurnTokens {
                issuer_request_id,
                vault,
                burn_shares,
                dust_shares,
                receipt_id,
                owner,
                receipt_info,
            } => {
                self.handle_burn_tokens(
                    services,
                    issuer_request_id,
                    BurnInput {
                        vault,
                        burn_shares,
                        dust_shares,
                        receipt_id,
                        owner,
                        receipt_info,
                    },
                )
                .await
            }
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id,
                error,
            } => self.handle_record_burn_failure(issuer_request_id, error),
            RedemptionCommand::RetryBurn {
                issuer_request_id,
                vault,
                burn_shares,
                dust_shares,
                receipt_id,
                owner,
                receipt_info,
                user_wallet,
            } => {
                self.handle_retry_burn(
                    services,
                    issuer_request_id,
                    BurnInput {
                        vault,
                        burn_shares,
                        dust_shares,
                        receipt_id,
                        owner,
                        receipt_info,
                    },
                    user_wallet,
                )
                .await
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
                };
            }
            RedemptionEvent::AlpacaCalled {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
            } => {
                self.apply_alpaca_called(
                    &issuer_request_id,
                    tokenization_request_id,
                    alpaca_quantity,
                    dust_quantity,
                    called_at,
                );
            }
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id,
                error,
                failed_at,
            }
            | RedemptionEvent::RedemptionFailed {
                issuer_request_id,
                reason: error,
                failed_at,
            }
            | RedemptionEvent::BurningFailed {
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
                alpaca_journal_completed_at,
            } => {
                self.apply_alpaca_journal_completed(
                    &issuer_request_id,
                    alpaca_journal_completed_at,
                );
            }
            RedemptionEvent::TokensBurned {
                issuer_request_id,
                tx_hash,
                burned_at,
                ..
            } => {
                *self = Self::Completed {
                    issuer_request_id,
                    burn_tx_hash: tx_hash,
                    completed_at: burned_at,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address, b256, uint};
    use chrono::Utc;
    use cqrs_es::{Aggregate, test::TestFramework};
    use rust_decimal::Decimal;
    use std::sync::Arc;

    use super::{
        BurnRecord, Redemption, RedemptionCommand, RedemptionError,
        RedemptionEvent, RedemptionMetadata,
    };
    use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
    use crate::vault::mock::MockVaultService;
    use crate::vault::{OperationType, ReceiptInformation, VaultService};

    type RedemptionTestFramework = TestFramework<Redemption>;

    fn mock_services() -> Arc<dyn VaultService> {
        Arc::new(MockVaultService::new_success())
    }

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

        let validator = RedemptionTestFramework::with(mock_services())
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

        RedemptionTestFramework::with(mock_services())
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

        assert_eq!(
            redemption,
            Redemption::Detected {
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

        let validator = RedemptionTestFramework::with(mock_services())
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
                alpaca_quantity: Quantity::new(Decimal::from(100)),
                dust_quantity: Quantity::new(Decimal::ZERO),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::AlpacaCalled {
            issuer_request_id: event_id,
            tokenization_request_id: event_tok_id,
            called_at,
            ..
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

        RedemptionTestFramework::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordAlpacaCall {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity: Quantity::new(Decimal::from(100)),
                dust_quantity: Quantity::new(Decimal::ZERO),
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

        let validator = RedemptionTestFramework::with(mock_services())
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

        RedemptionTestFramework::with(mock_services())
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
        let issuer_request_id = IssuerRequestId::new("red-complete-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-complete-456");

        let validator = RedemptionTestFramework::with(mock_services())
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
                    alpaca_quantity: Quantity::new(Decimal::from(100)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
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

        RedemptionTestFramework::with(mock_services())
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
        let alpaca_journal_completed_at = Utc::now();

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

        let alpaca_quantity = Quantity::new(Decimal::from(50));
        let dust_quantity = Quantity::new(Decimal::ZERO);

        redemption.apply(RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            alpaca_quantity: alpaca_quantity.clone(),
            dust_quantity: dust_quantity.clone(),
            called_at,
        });

        redemption.apply(RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: issuer_request_id.clone(),
            alpaca_journal_completed_at,
        });

        assert_eq!(
            redemption,
            Redemption::Burning {
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
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
            }
        );
    }

    #[test]
    fn test_confirm_alpaca_complete_emits_one_event() {
        let issuer_request_id = IssuerRequestId::new("red-one-event-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-one-event-456");

        let validator = RedemptionTestFramework::with(mock_services())
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
                    alpaca_quantity: Quantity::new(Decimal::from(100)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
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
    fn test_burn_tokens_from_burning_state() {
        let issuer_request_id = IssuerRequestId::new("red-burn-success-123");
        let receipt_id = uint!(42_U256);
        let burn_shares = uint!(100_000000000000000000_U256);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let owner = address!("0x1111111111111111111111111111111111111111");
        let user_wallet =
            address!("0x9876543210fedcba9876543210fedcba98765432");

        let receipt_info = ReceiptInformation {
            tokenization_request_id: TokenizationRequestId::new("alp-burn-456"),
            issuer_request_id: issuer_request_id.clone(),
            underlying: UnderlyingSymbol::new("TSLA"),
            quantity: Quantity::new(Decimal::from(100)),
            operation_type: OperationType::Redeem,
            timestamp: Utc::now(),
            notes: None,
        };

        let validator = RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("TSLA"),
                    token: TokenSymbol::new("tTSLA"),
                    wallet: user_wallet,
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
                    alpaca_quantity: Quantity::new(Decimal::from(100)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at: Utc::now(),
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::BurnTokens {
                issuer_request_id: issuer_request_id.clone(),
                vault,
                burn_shares,
                dust_shares: U256::ZERO,
                receipt_id,
                owner,
                receipt_info,
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::TokensBurned {
            issuer_request_id: event_id,
            burns,
            burned_at,
            ..
        } = &events[0]
        else {
            panic!("Expected TokensBurned event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(burns.len(), 1);
        assert_eq!(burns[0].receipt_id, receipt_id);
        assert_eq!(burns[0].shares_burned, burn_shares);
        assert!(burned_at.timestamp() > 0);
    }

    #[test]
    fn test_burn_tokens_from_wrong_state_fails() {
        let issuer_request_id = IssuerRequestId::new("red-burn-wrong-123");

        let receipt_info = ReceiptInformation {
            tokenization_request_id: TokenizationRequestId::new("alp-123"),
            issuer_request_id: issuer_request_id.clone(),
            underlying: UnderlyingSymbol::new("NVDA"),
            quantity: Quantity::new(Decimal::from(25)),
            operation_type: OperationType::Redeem,
            timestamp: Utc::now(),
            notes: None,
        };

        RedemptionTestFramework::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("NVDA"),
                token: TokenSymbol::new("tNVDA"),
                wallet: address!(
                    "0xfedcbafedcbafedcbafedcbafedcbafedcbafedc"
                ),
                quantity: Quantity::new(Decimal::from(25)),
                tx_hash: b256!(
                    "0x2222222222222222222222222222222222222222222222222222222222222222"
                ),
                block_number: 15000,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::BurnTokens {
                issuer_request_id,
                vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                burn_shares: uint!(25_000000000000000000_U256),
                dust_shares: U256::ZERO,
                receipt_id: uint!(1_U256),
                owner: address!("0x1111111111111111111111111111111111111111"),
                receipt_info,
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

        let validator = RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("GOOG"),
                    token: TokenSymbol::new("tGOOG"),
                    wallet: address!(
                        "0xabababababababababababababababababababab"
                    ),
                    quantity: Quantity::new(Decimal::from(50)),
                    tx_hash: b256!(
                        "0x3333333333333333333333333333333333333333333333333333333333333333"
                    ),
                    block_number: 30000,
                    detected_at: Utc::now(),
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: crate::mint::TokenizationRequestId::new("alp-fail-789"),
                    alpaca_quantity: Quantity::new(Decimal::from(50)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
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

        RedemptionTestFramework::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordBurnFailure {
                issuer_request_id,
                error: "Some error".to_string(),
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: "Uninitialized".to_string(),
            });
    }

    #[test]
    fn test_apply_tokens_burned_transitions_to_completed() {
        let mut redemption = Redemption::default();

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

        redemption.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token,
            wallet,
            quantity,
            tx_hash: detected_tx_hash,
            block_number,
            detected_at,
        });

        redemption.apply(RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            alpaca_quantity: Quantity::new(Decimal::from(75)),
            dust_quantity: Quantity::new(Decimal::ZERO),
            called_at,
        });

        redemption.apply(RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: issuer_request_id.clone(),
            alpaca_journal_completed_at,
        });

        redemption.apply(RedemptionEvent::TokensBurned {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: burn_tx_hash,
            burns: vec![BurnRecord { receipt_id, shares_burned }],
            dust_returned: U256::ZERO,
            gas_used: 60000,
            block_number: 51000,
            burned_at,
        });

        assert_eq!(
            redemption,
            Redemption::Completed {
                issuer_request_id,
                burn_tx_hash,
                completed_at: burned_at,
            }
        );
    }

    #[test]
    fn test_apply_burning_failed_transitions_to_failed() {
        let mut redemption = Redemption::default();

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

        redemption.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
        });

        redemption.apply(RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            alpaca_quantity: Quantity::new(Decimal::from(150)),
            dust_quantity: Quantity::new(Decimal::ZERO),
            called_at,
        });

        redemption.apply(RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: issuer_request_id.clone(),
            alpaca_journal_completed_at,
        });

        redemption.apply(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: error.clone(),
            failed_at,
        });

        assert_eq!(
            redemption,
            Redemption::Failed { issuer_request_id, reason: error, failed_at }
        );
    }
}
