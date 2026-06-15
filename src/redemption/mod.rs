mod cmd;
mod event;
pub(crate) mod upcaster;
pub(crate) mod view;

pub(crate) mod burn_manager;
pub(crate) mod journal_manager;
pub(crate) mod poller;
pub(crate) mod redeem_call_manager;
#[cfg(test)]
pub(crate) mod test_utils;
pub(crate) mod transfer;

use alloy::hex;
use alloy::primitives::{Address, B256, FixedBytes, TxHash, U256};
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use cqrs_es::event_sink::EventSink;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

use crate::Quantity;
use crate::mint::TokenizationRequestId;

/// Issuer request ID for redemption operations.
///
/// New IDs are the full triggering transaction hash. Legacy IDs used the first
/// 4 bytes formatted as `"red-{hex}"`; keep parsing/serializing them so
/// historical aggregates remain operable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum IssuerRedemptionRequestId {
    Full(TxHash),
    Legacy(FixedBytes<4>),
}

impl IssuerRedemptionRequestId {
    #[must_use]
    pub(crate) const fn new(tx_hash: TxHash) -> Self {
        Self::Full(tx_hash)
    }

    #[cfg(test)]
    pub(crate) fn random() -> Self {
        Self::new(B256::random())
    }
}

impl std::fmt::Display for IssuerRedemptionRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(tx_hash) => write!(f, "{tx_hash:#x}"),
            Self::Legacy(id) => write!(f, "red-{}", hex::encode(id)),
        }
    }
}

impl std::str::FromStr for IssuerRedemptionRequestId {
    type Err = IssuerRedemptionRequestIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(hex_str) = s.strip_prefix("red-") {
            let bytes = hex::decode(hex_str)?;
            return Ok(Self::Legacy(FixedBytes::<4>::try_from(
                bytes.as_slice(),
            )?));
        }

        s.parse::<B256>()
            .map(Self::Full)
            .map_err(|_| IssuerRedemptionRequestIdParseError::Format)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum IssuerRedemptionRequestIdParseError {
    #[error("invalid hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error(transparent)]
    Slice(#[from] std::array::TryFromSliceError),
    #[error("expected full 0x transaction hash or legacy 'red-' prefix")]
    Format,
}

impl<'r> rocket::request::FromParam<'r> for IssuerRedemptionRequestId {
    type Error = IssuerRedemptionRequestIdParseError;

    fn from_param(param: &'r str) -> Result<Self, Self::Error> {
        param.parse()
    }
}

impl Serialize for IssuerRedemptionRequestId {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for IssuerRedemptionRequestId {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct BurnExternalTxId(String);

impl BurnExternalTxId {
    pub(crate) const fn from_string(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn base(detected_tx_hash: &B256) -> Self {
        Self(format!("burn-{detected_tx_hash}"))
    }

    pub(crate) fn retry(detected_tx_hash: &B256, attempt: u32) -> Self {
        Self(format!(
            "{}{}{}",
            Self::base(detected_tx_hash),
            Redemption::BURN_RETRY_EXTERNAL_TX_MARKER,
            attempt,
        ))
    }

    pub(crate) fn retry_attempt(&self) -> Option<u32> {
        self.0
            .rsplit_once(Redemption::BURN_RETRY_EXTERNAL_TX_MARKER)
            .and_then(|(_, attempt)| attempt.parse().ok())
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for BurnExternalTxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
use crate::vault::VaultError;
use crate::vault::{MultiBurnEntry, MultiBurnParams, VaultService};

pub(crate) use cmd::RedemptionCommand;
pub(crate) use event::{BurnRecord, RedemptionEvent};
pub(crate) use view::{
    RedemptionView, RedemptionViewError, find_alpaca_called, find_detected,
    find_stuck, replay_redemption_view,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RedemptionMetadata {
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) wallet: Address,
    pub(crate) quantity: Quantity,
    pub(crate) detected_tx_hash: B256,
    pub(crate) block_number: u64,
    pub(crate) detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Redemption {
    #[default]
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
        #[serde(default)]
        external_tx_id: Option<BurnExternalTxId>,
    },
    /// Burn transaction submitted to signing backend, awaiting on-chain confirmation.
    BurnSubmitted {
        metadata: RedemptionMetadata,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
        alpaca_journal_completed_at: DateTime<Utc>,
        external_tx_id: BurnExternalTxId,
        fireblocks_tx_id: String,
        planned_burns: Vec<event::BurnRecord>,
    },
    Completed {
        issuer_request_id: IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        completed_at: DateTime<Utc>,
    },
    Failed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    Closed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        closed_at: DateTime<Utc>,
    },
}

/// Burn-related parameters for the BurnTokens command handler.
///
/// Groups burn-related parameters to reduce argument count. The `user` field
/// is derived from aggregate state, not passed in the command.
struct BurnTokensParams {
    vault: Address,
    burns: Vec<MultiBurnEntry>,
    dust_shares: U256,
    owner: Address,
    external_tx_id: Option<BurnExternalTxId>,
}

struct RedemptionDetection {
    issuer_request_id: IssuerRedemptionRequestId,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    wallet: Address,
    quantity: Quantity,
    tx_hash: TxHash,
    block_number: u64,
}

struct BurnRecovery {
    issuer_request_id: IssuerRedemptionRequestId,
    metadata: RedemptionMetadata,
    tokenization_request_id: TokenizationRequestId,
    alpaca_quantity: Quantity,
    dust_quantity: Quantity,
    called_at: DateTime<Utc>,
    alpaca_journal_completed_at: DateTime<Utc>,
    external_tx_id: Option<BurnExternalTxId>,
}

impl Redemption {
    const BURN_RETRY_EXTERNAL_TX_MARKER: &'static str = "-retry-";

    pub(crate) fn retry_burn_external_tx_id_typed(
        detected_tx_hash: &B256,
        attempt: u32,
    ) -> BurnExternalTxId {
        BurnExternalTxId::retry(detected_tx_hash, attempt)
    }

    pub(crate) fn retry_attempt_from_burn_external_tx_id(
        external_tx_id: &BurnExternalTxId,
    ) -> Option<u32> {
        external_tx_id.retry_attempt()
    }

    pub(crate) fn next_burn_retry_external_tx_id(
        detected_tx_hash: &B256,
        latest_external_tx_id: &BurnExternalTxId,
    ) -> Result<BurnExternalTxId, RedemptionError> {
        let attempt =
            Self::retry_attempt_from_burn_external_tx_id(latest_external_tx_id)
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| RedemptionError::RetryAttemptOverflow {
                    latest_external_tx_id: latest_external_tx_id.clone(),
                })?;
        Ok(Self::retry_burn_external_tx_id_typed(detected_tx_hash, attempt))
    }

    pub(crate) const fn metadata(&self) -> Option<&RedemptionMetadata> {
        match self {
            Self::Detected { metadata }
            | Self::AlpacaCalled { metadata, .. }
            | Self::Burning { metadata, .. }
            | Self::BurnSubmitted { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    /// Returns the quantity sent to Alpaca (truncated to 9 decimals).
    /// Only available in AlpacaCalled, Burning, and BurnSubmitted states.
    pub(crate) const fn alpaca_quantity(&self) -> Option<&Quantity> {
        match self {
            Self::AlpacaCalled { alpaca_quantity, .. }
            | Self::Burning { alpaca_quantity, .. }
            | Self::BurnSubmitted { alpaca_quantity, .. } => {
                Some(alpaca_quantity)
            }
            _ => None,
        }
    }

    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::Uninitialized => "Uninitialized",
            Self::Detected { .. } => "Detected",
            Self::AlpacaCalled { .. } => "AlpacaCalled",
            Self::Burning { .. } => "Burning",
            Self::BurnSubmitted { .. } => "BurnSubmitted",
            Self::Completed { .. } => "Completed",
            Self::Failed { .. } => "Failed",
            Self::Closed { .. } => "Closed",
        }
    }

    async fn handle_detect(
        &mut self,
        sink: &EventSink<Self>,
        input: RedemptionDetection,
    ) -> Result<(), RedemptionError> {
        if !matches!(self, Self::Uninitialized) {
            return Err(RedemptionError::AlreadyDetected {
                issuer_request_id: input.issuer_request_id,
            });
        }

        sink.write(
            RedemptionEvent::Detected {
                issuer_request_id: input.issuer_request_id,
                underlying: input.underlying,
                token: input.token,
                wallet: input.wallet,
                quantity: input.quantity,
                tx_hash: input.tx_hash,
                block_number: input.block_number,
                detected_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_record_alpaca_call(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
    ) -> Result<(), RedemptionError> {
        if !matches!(self, Self::Detected { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: self.state_name().to_string(),
            });
        }

        sink.write(
            RedemptionEvent::AlpacaCalled {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_record_alpaca_failure(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
    ) -> Result<(), RedemptionError> {
        if !matches!(self, Self::Detected { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: self.state_name().to_string(),
            });
        }

        sink.write(
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id,
                error,
                failed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_mark_failed(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
    ) -> Result<(), RedemptionError> {
        if !matches!(
            self,
            Self::Detected { .. }
                | Self::AlpacaCalled { .. }
                | Self::Burning { .. }
                | Self::BurnSubmitted { .. }
                | Self::Failed { .. }
        ) {
            return Err(RedemptionError::InvalidState {
                expected: "Detected, AlpacaCalled, Burning, or Failed"
                    .to_string(),
                found: self.state_name().to_string(),
            });
        }

        sink.write(
            RedemptionEvent::RedemptionFailed {
                issuer_request_id,
                reason,
                failed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    /// Submits the burn transaction to the signing backend.
    /// Produces `BurnFireblocksSubmitted` on success; failure is propagated to caller.
    async fn handle_burn_tokens(
        &mut self,
        services: &Arc<dyn VaultService>,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        input: BurnTokensParams,
    ) -> Result<(), RedemptionError> {
        let Self::Burning { metadata, .. } = self else {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: self.state_name().to_string(),
            });
        };

        let user_wallet = metadata.wallet;

        let planned_burns: Vec<BurnRecord> = input
            .burns
            .iter()
            .map(|entry| BurnRecord {
                receipt_id: entry.receipt_id,
                shares_burned: entry.burn_shares,
            })
            .collect();

        let submitted = services
            .submit_burn(MultiBurnParams {
                vault: input.vault,
                burns: input.burns,
                dust_shares: input.dust_shares,
                owner: input.owner,
                user: user_wallet,
                issuer_request_id: issuer_request_id.clone(),
                detected_tx_hash: metadata.detected_tx_hash,
                external_tx_id: input.external_tx_id,
            })
            .await?;

        sink.write(
            RedemptionEvent::BurnFireblocksSubmitted {
                issuer_request_id,
                external_tx_id: BurnExternalTxId::from_string(
                    submitted.external_tx_id,
                ),
                fireblocks_tx_id: submitted.fireblocks_tx_id,
                planned_burns,
                submitted_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    /// Confirms a previously submitted burn transaction.
    async fn handle_confirm_burn(
        &mut self,
        services: &Arc<dyn VaultService>,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        fireblocks_tx_id: String,
        dust_shares: U256,
    ) -> Result<(), RedemptionError> {
        let Self::BurnSubmitted { fireblocks_tx_id: stored_tx_id, .. } = self
        else {
            return Err(RedemptionError::InvalidState {
                expected: "BurnSubmitted".to_string(),
                found: self.state_name().to_string(),
            });
        };

        if *stored_tx_id != fireblocks_tx_id {
            return Err(RedemptionError::FireblocksTxIdMismatch {
                expected: stored_tx_id.clone(),
                provided: fireblocks_tx_id,
            });
        }

        let result =
            services.confirm_burn(&fireblocks_tx_id, dust_shares).await?;

        let burns = result
            .burns
            .into_iter()
            .map(|b| BurnRecord {
                receipt_id: b.receipt_id,
                shares_burned: b.shares_burned,
            })
            .collect();

        sink.write(
            RedemptionEvent::TokensBurned {
                issuer_request_id,
                tx_hash: result.tx_hash,
                burns,
                dust_returned: result.dust_returned,
                gas_used: result.gas_used,
                block_number: result.block_number,
                burned_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_record_burn_failure(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
        fireblocks_tx_id: Option<String>,
        planned_burns: Vec<BurnRecord>,
    ) -> Result<(), RedemptionError> {
        if !matches!(self, Self::Burning { .. } | Self::BurnSubmitted { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Burning or BurnSubmitted".to_string(),
                found: self.state_name().to_string(),
            });
        }

        sink.write(
            RedemptionEvent::BurningFailed {
                issuer_request_id,
                error,
                failed_at: Utc::now(),
                fireblocks_tx_id,
                planned_burns,
            },
            self,
        )
        .await;

        Ok(())
    }

    /// Reprocessing is only valid from `Failed` state. Post-Alpaca states
    /// (`AlpacaCalled`, `Burning`) have dedicated recovery paths in the
    /// burn/journal managers and resetting them to `Detected` would cause
    /// a duplicate Alpaca redeem call.
    async fn handle_reprocess(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        metadata: RedemptionMetadata,
    ) -> Result<(), RedemptionError> {
        let Self::Failed { .. } = self else {
            return Err(match self {
                Self::Completed { .. } => {
                    RedemptionError::AlreadyCompleted { issuer_request_id }
                }
                _ => RedemptionError::InvalidState {
                    expected: "Failed".to_string(),
                    found: self.state_name().to_string(),
                },
            });
        };

        sink.write(
            RedemptionEvent::Reprocessed {
                issuer_request_id,
                underlying: metadata.underlying,
                token: metadata.token,
                wallet: metadata.wallet,
                quantity: metadata.quantity,
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
                detected_at: metadata.detected_at,
                previous_state: self.state_name().to_string(),
                reprocessed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    /// Resumes a post-Alpaca failed redemption directly to Burning state.
    /// Only valid from `Failed` state. The API layer validates that Alpaca
    /// was already called before issuing this command.
    async fn handle_resume_burn(
        &mut self,
        sink: &EventSink<Self>,
        input: BurnRecovery,
    ) -> Result<(), RedemptionError> {
        let Self::Failed { .. } = self else {
            return Err(match self {
                Self::Completed { .. } | Self::Closed { .. } => {
                    RedemptionError::AlreadyCompleted {
                        issuer_request_id: input.issuer_request_id,
                    }
                }
                _ => RedemptionError::InvalidState {
                    expected: "Failed".to_string(),
                    found: self.state_name().to_string(),
                },
            });
        };

        sink.write(
            RedemptionEvent::BurnResumed {
                issuer_request_id: input.issuer_request_id,
                underlying: input.metadata.underlying,
                token: input.metadata.token,
                wallet: input.metadata.wallet,
                quantity: input.metadata.quantity,
                tx_hash: input.metadata.detected_tx_hash,
                block_number: input.metadata.block_number,
                detected_at: input.metadata.detected_at,
                tokenization_request_id: input.tokenization_request_id,
                alpaca_quantity: input.alpaca_quantity,
                dust_quantity: input.dust_quantity,
                called_at: input.called_at,
                alpaca_journal_completed_at: input.alpaca_journal_completed_at,
                external_tx_id: input.external_tx_id,
                resumed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_record_existing_burn(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        fireblocks_tx_id: String,
        tx_hash: B256,
        planned_burns: Vec<BurnRecord>,
        block_number: u64,
    ) -> Result<(), RedemptionError> {
        let Self::Failed { .. } = self else {
            return Err(match self {
                Self::Completed { .. } | Self::Closed { .. } => {
                    RedemptionError::AlreadyCompleted { issuer_request_id }
                }
                _ => RedemptionError::InvalidState {
                    expected: "Failed".to_string(),
                    found: self.state_name().to_string(),
                },
            });
        };

        sink.write(
            RedemptionEvent::ExistingBurnRecovered {
                issuer_request_id,
                fireblocks_tx_id,
                tx_hash,
                burns: planned_burns,
                block_number,
                recovered_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    /// Admin-closes a redemption that cannot be auto-recovered. Valid from
    /// `Failed`, `Burning`, or `BurnSubmitted` — the honest terminal path for a
    /// redemption whose burn cannot/should not be re-submitted and is not
    /// verifiable on-chain. Widening beyond `Failed` covers redemptions stuck in
    /// `Burning` (including the `Failed -> Burning` recovery regression, which
    /// would otherwise strand a previously-closeable redemption).
    async fn handle_close_redemption(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
    ) -> Result<(), RedemptionError> {
        if !matches!(
            self,
            Self::Failed { .. }
                | Self::Burning { .. }
                | Self::BurnSubmitted { .. }
        ) {
            return Err(match self {
                Self::Completed { .. } | Self::Closed { .. } => {
                    RedemptionError::AlreadyCompleted { issuer_request_id }
                }
                _ => RedemptionError::InvalidState {
                    expected: "Failed, Burning, or BurnSubmitted".to_string(),
                    found: self.state_name().to_string(),
                },
            });
        }

        sink.write(
            RedemptionEvent::RedemptionClosed {
                issuer_request_id,
                reason,
                closed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    /// Admin-terminalizes a redemption stuck in `Burning`/`BurnSubmitted` whose
    /// burn already landed on-chain but was never recorded. The admin layer
    /// verifies `burn_tx_hash` on-chain before issuing this command, so the
    /// aggregate trusts the supplied tx hash and block number and records the
    /// proving terminal event, transitioning to `Completed`.
    async fn handle_force_complete_burn(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        block_number: u64,
        reason: String,
    ) -> Result<(), RedemptionError> {
        if !matches!(self, Self::Burning { .. } | Self::BurnSubmitted { .. }) {
            return Err(match self {
                Self::Completed { .. } | Self::Closed { .. } => {
                    RedemptionError::AlreadyCompleted { issuer_request_id }
                }
                _ => RedemptionError::InvalidState {
                    expected: "Burning or BurnSubmitted".to_string(),
                    found: self.state_name().to_string(),
                },
            });
        }

        sink.write(
            RedemptionEvent::BurnForceCompleted {
                issuer_request_id,
                burn_tx_hash,
                block_number,
                reason,
                completed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_confirm_alpaca_complete(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerRedemptionRequestId,
    ) -> Result<(), RedemptionError> {
        if !matches!(self, Self::AlpacaCalled { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "AlpacaCalled".to_string(),
                found: self.state_name().to_string(),
            });
        }

        sink.write(
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id,
                alpaca_journal_completed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    fn apply_alpaca_called(
        &mut self,
        issuer_request_id: &IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
    ) {
        let Self::Detected { metadata } = self else {
            warn!(
                issuer_request_id = %issuer_request_id,
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
        issuer_request_id: &IssuerRedemptionRequestId,
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
                issuer_request_id = %issuer_request_id,
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
            external_tx_id: None,
        };
    }
}

pub(crate) fn next_burn_retry_external_tx_id_from_history<'a>(
    detected_tx_hash: &B256,
    events: impl DoubleEndedIterator<Item = &'a RedemptionEvent>,
) -> Result<Option<BurnExternalTxId>, RedemptionError> {
    events
        .rev()
        .find_map(|event| match event {
            RedemptionEvent::BurnResumed {
                external_tx_id: Some(external_tx_id),
                ..
            } => Some(Ok(external_tx_id.clone())),
            RedemptionEvent::BurnFireblocksSubmitted {
                external_tx_id, ..
            } => Some(Redemption::next_burn_retry_external_tx_id(
                detected_tx_hash,
                external_tx_id,
            )),
            _ => None,
        })
        .transpose()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RedemptionError {
    #[error("Redemption already detected for request: {issuer_request_id}")]
    AlreadyDetected { issuer_request_id: IssuerRedemptionRequestId },
    #[error("Invalid state for operation: expected {expected}, found {found}")]
    InvalidState { expected: String, found: String },
    #[error(
        "Redemption already completed, cannot reprocess: {issuer_request_id}"
    )]
    AlreadyCompleted { issuer_request_id: IssuerRedemptionRequestId },
    #[error("Vault error: {0}")]
    Vault(#[from] VaultError),
    #[error(
        "Fireblocks tx ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    FireblocksTxIdMismatch { expected: String, provided: String },
    #[error(
        "Burn retry attempt counter overflowed for external tx id: {latest_external_tx_id}"
    )]
    RetryAttemptOverflow { latest_external_tx_id: BurnExternalTxId },
}

impl PartialEq for RedemptionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::AlreadyDetected { issuer_request_id: a },
                Self::AlreadyDetected { issuer_request_id: b },
            )
            | (
                Self::AlreadyCompleted { issuer_request_id: a },
                Self::AlreadyCompleted { issuer_request_id: b },
            ) => a == b,
            (
                Self::InvalidState { expected: e1, found: f1 },
                Self::InvalidState { expected: e2, found: f2 },
            ) => e1 == e2 && f1 == f2,
            (Self::Vault(a), Self::Vault(b)) => a.to_string() == b.to_string(),
            (
                Self::FireblocksTxIdMismatch { expected: e1, provided: p1 },
                Self::FireblocksTxIdMismatch { expected: e2, provided: p2 },
            ) => e1 == e2 && p1 == p2,
            (
                Self::RetryAttemptOverflow { latest_external_tx_id: a },
                Self::RetryAttemptOverflow { latest_external_tx_id: b },
            ) => a == b,
            _ => false,
        }
    }
}

impl Aggregate for Redemption {
    type Command = RedemptionCommand;
    type Event = RedemptionEvent;
    type Error = RedemptionError;
    type Services = Arc<dyn VaultService>;

    const TYPE: &'static str = "Redemption";

    async fn handle(
        &mut self,
        command: Self::Command,
        services: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
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
                self.handle_detect(
                    sink,
                    RedemptionDetection {
                        issuer_request_id,
                        underlying,
                        token,
                        wallet,
                        quantity,
                        tx_hash,
                        block_number,
                    },
                )
                .await
            }

            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
            } => {
                self.handle_record_alpaca_call(
                    sink,
                    issuer_request_id,
                    tokenization_request_id,
                    alpaca_quantity,
                    dust_quantity,
                )
                .await
            }

            RedemptionCommand::RecordAlpacaFailure {
                issuer_request_id,
                error,
            } => {
                self.handle_record_alpaca_failure(
                    sink,
                    issuer_request_id,
                    error,
                )
                .await
            }

            RedemptionCommand::ConfirmAlpacaComplete { issuer_request_id } => {
                self.handle_confirm_alpaca_complete(sink, issuer_request_id)
                    .await
            }

            RedemptionCommand::MarkFailed { issuer_request_id, reason } => {
                self.handle_mark_failed(sink, issuer_request_id, reason).await
            }

            RedemptionCommand::BurnTokens {
                issuer_request_id,
                vault,
                burns,
                dust_shares,
                owner,
                external_tx_id,
            } => {
                self.handle_burn_tokens(
                    services,
                    sink,
                    issuer_request_id,
                    BurnTokensParams {
                        vault,
                        burns,
                        dust_shares,
                        owner,
                        external_tx_id,
                    },
                )
                .await
            }

            RedemptionCommand::ConfirmBurn {
                issuer_request_id,
                fireblocks_tx_id,
                dust_shares,
            } => {
                self.handle_confirm_burn(
                    services,
                    sink,
                    issuer_request_id,
                    fireblocks_tx_id,
                    dust_shares,
                )
                .await
            }

            RedemptionCommand::RecordBurnFailure {
                issuer_request_id,
                error,
                fireblocks_tx_id,
                planned_burns,
            } => {
                self.handle_record_burn_failure(
                    sink,
                    issuer_request_id,
                    error,
                    fireblocks_tx_id,
                    planned_burns,
                )
                .await
            }

            RedemptionCommand::RecordExistingBurn {
                issuer_request_id,
                fireblocks_tx_id,
                tx_hash,
                planned_burns,
                block_number,
            } => {
                self.handle_record_existing_burn(
                    sink,
                    issuer_request_id,
                    fireblocks_tx_id,
                    tx_hash,
                    planned_burns,
                    block_number,
                )
                .await
            }

            RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason,
            } => {
                self.handle_close_redemption(sink, issuer_request_id, reason)
                    .await
            }

            RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash,
                block_number,
                reason,
            } => {
                self.handle_force_complete_burn(
                    sink,
                    issuer_request_id,
                    burn_tx_hash,
                    block_number,
                    reason,
                )
                .await
            }

            RedemptionCommand::Reprocess { issuer_request_id, metadata } => {
                self.handle_reprocess(sink, issuer_request_id, metadata).await
            }

            RedemptionCommand::ResumeBurn {
                issuer_request_id,
                metadata,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                external_tx_id,
            } => {
                self.handle_resume_burn(
                    sink,
                    BurnRecovery {
                        issuer_request_id,
                        metadata,
                        tokenization_request_id,
                        alpaca_quantity,
                        dust_quantity,
                        called_at,
                        alpaca_journal_completed_at,
                        external_tx_id,
                    },
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
            }
            | RedemptionEvent::Reprocessed {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
                ..
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
                ..
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
            RedemptionEvent::BurnFireblocksSubmitted {
                issuer_request_id: _,
                external_tx_id,
                fireblocks_tx_id,
                planned_burns,
                submitted_at: _,
            } => {
                let Self::Burning {
                    metadata,
                    tokenization_request_id,
                    alpaca_quantity,
                    dust_quantity,
                    called_at,
                    alpaca_journal_completed_at,
                    external_tx_id: _,
                } = self.clone()
                else {
                    return;
                };

                *self = Self::BurnSubmitted {
                    metadata,
                    tokenization_request_id,
                    alpaca_quantity,
                    dust_quantity,
                    called_at,
                    alpaca_journal_completed_at,
                    external_tx_id,
                    fireblocks_tx_id,
                    planned_burns,
                };
            }
            RedemptionEvent::BurnResumed {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                external_tx_id,
                ..
            } => {
                *self = Self::Burning {
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
                    external_tx_id,
                };
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
            RedemptionEvent::ExistingBurnRecovered {
                issuer_request_id,
                tx_hash,
                recovered_at,
                ..
            } => {
                *self = Self::Completed {
                    issuer_request_id,
                    burn_tx_hash: tx_hash,
                    completed_at: recovered_at,
                };
            }
            RedemptionEvent::RedemptionClosed {
                issuer_request_id,
                reason,
                closed_at,
            } => {
                *self = Self::Closed { issuer_request_id, reason, closed_at };
            }
            RedemptionEvent::BurnForceCompleted {
                issuer_request_id,
                burn_tx_hash,
                completed_at,
                ..
            } => {
                *self = Self::Completed {
                    issuer_request_id,
                    burn_tx_hash,
                    completed_at,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, U256, address, b256, uint};
    use chrono::Utc;
    use cqrs_es::{Aggregate, test::TestFramework};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use std::sync::Arc;

    use super::{
        BurnExternalTxId, BurnRecord, IssuerRedemptionRequestId, Redemption,
        RedemptionCommand, RedemptionError, RedemptionEvent,
        RedemptionMetadata, next_burn_retry_external_tx_id_from_history,
    };
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
    use crate::vault::mock::MockVaultService;
    use crate::vault::{MultiBurnEntry, VaultService};

    type RedemptionTestFramework = TestFramework<Redemption>;

    fn mock_services() -> Arc<dyn VaultService> {
        Arc::new(MockVaultService::new_success())
    }

    #[test]
    fn test_next_burn_retry_external_tx_id_advances_from_submission() {
        let detected_tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let events = [RedemptionEvent::BurnFireblocksSubmitted {
            issuer_request_id: IssuerRedemptionRequestId::new(detected_tx_hash),
            external_tx_id: BurnExternalTxId::base(&detected_tx_hash),
            fireblocks_tx_id: "fb-1".to_string(),
            planned_burns: vec![],
            submitted_at: Utc::now(),
        }];

        let next = next_burn_retry_external_tx_id_from_history(
            &detected_tx_hash,
            events.iter(),
        )
        .unwrap();

        assert_eq!(
            next,
            Some(Redemption::retry_burn_external_tx_id_typed(
                &detected_tx_hash,
                1
            ))
        );
    }

    #[test]
    fn test_next_burn_retry_external_tx_id_reuses_unaccepted_retry() {
        let detected_tx_hash = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        let retry_external_tx_id =
            Redemption::retry_burn_external_tx_id_typed(&detected_tx_hash, 1);
        let events = [
            RedemptionEvent::BurnFireblocksSubmitted {
                issuer_request_id: IssuerRedemptionRequestId::new(
                    detected_tx_hash,
                ),
                external_tx_id: BurnExternalTxId::base(&detected_tx_hash),
                fireblocks_tx_id: "fb-1".to_string(),
                planned_burns: vec![],
                submitted_at: Utc::now(),
            },
            RedemptionEvent::BurnResumed {
                issuer_request_id: IssuerRedemptionRequestId::new(
                    detected_tx_hash,
                ),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(1)),
                tx_hash: detected_tx_hash,
                block_number: 1,
                detected_at: Utc::now(),
                tokenization_request_id: TokenizationRequestId::new("tok-1"),
                alpaca_quantity: Quantity::new(Decimal::from(1)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at: Utc::now(),
                alpaca_journal_completed_at: Utc::now(),
                external_tx_id: Some(retry_external_tx_id.clone()),
                resumed_at: Utc::now(),
            },
        ];

        let next = next_burn_retry_external_tx_id_from_history(
            &detected_tx_hash,
            events.iter(),
        )
        .unwrap();

        assert_eq!(next, Some(retry_external_tx_id));
    }

    #[test]
    fn test_detect_redemption_creates_event() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
                issuer_request_id,
            });
    }

    #[test]
    fn test_apply_detected_event_updates_state() {
        let mut redemption = Redemption::default();

        assert!(matches!(redemption, Redemption::Uninitialized));

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let issuer_request_id = IssuerRedemptionRequestId::random();

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
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let issuer_request_id = IssuerRedemptionRequestId::random();

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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
                external_tx_id: None,
            }
        );
    }

    #[test]
    fn test_confirm_alpaca_complete_emits_one_event() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let receipt_id = uint!(42_U256);
        let burn_shares = uint!(100_000000000000000000_U256);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let owner = address!("0x1111111111111111111111111111111111111111");
        let user_wallet =
            address!("0x9876543210fedcba9876543210fedcba98765432");

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
                burns: vec![MultiBurnEntry {
                    receipt_id,
                    burn_shares,
                    receipt_info: None,
                    receipt_info_bytes: None,
                }],
                dust_shares: U256::ZERO,
                owner,
                external_tx_id: None,
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurnFireblocksSubmitted {
            issuer_request_id: event_id,
            planned_burns,
            ..
        } = &events[0]
        else {
            panic!(
                "Expected BurnFireblocksSubmitted event, got {:?}",
                &events[0]
            );
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(planned_burns.len(), 1);
        assert_eq!(planned_burns[0].receipt_id, receipt_id);
        assert_eq!(planned_burns[0].shares_burned, burn_shares);
    }

    #[test]
    fn test_burn_tokens_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

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
                burns: vec![MultiBurnEntry {
                    receipt_id: uint!(1_U256),
                    burn_shares: uint!(25_000000000000000000_U256),
                    receipt_info: None,
                    receipt_info_bytes: None,
                }],
                dust_shares: U256::ZERO,
                owner: address!("0x1111111111111111111111111111111111111111"),
                external_tx_id: None,
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: "Detected".to_string(),
            });
    }

    #[test]
    fn test_record_burn_failure_from_burning_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
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
                fireblocks_tx_id: None,
                planned_burns: vec![],
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurningFailed {
            issuer_request_id: event_id,
            error: event_error,
            failed_at,
            ..
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
        let issuer_request_id = IssuerRedemptionRequestId::random();

        RedemptionTestFramework::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordBurnFailure {
                issuer_request_id,
                error: "Some error".to_string(),
                fireblocks_tx_id: None,
                planned_burns: vec![],
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Burning or BurnSubmitted".to_string(),
                found: "Uninitialized".to_string(),
            });
    }

    /// Events that drive a redemption into the `Burning` state, for tests that
    /// exercise admin terminalization paths (close / force-complete).
    fn burning_given_events(
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Vec<RedemptionEvent> {
        vec![
            RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("ARKK"),
                token: TokenSymbol::new("tARKK"),
                wallet: address!("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
                quantity: Quantity::new(Decimal::from(17)),
                tx_hash: b256!(
                    "0x4444444444444444444444444444444444444444444444444444444444444444"
                ),
                block_number: 45_000_000,
                detected_at: Utc::now(),
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new(
                    "alp-arkk-799",
                ),
                alpaca_quantity: Quantity::new(Decimal::from(17)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at: Utc::now(),
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at: Utc::now(),
            },
        ]
    }

    fn burn_submitted_given_events(
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Vec<RedemptionEvent> {
        let mut events = burning_given_events(issuer_request_id);
        events.push(RedemptionEvent::BurnFireblocksSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: BurnExternalTxId::base(&b256!(
                "0x4444444444444444444444444444444444444444444444444444444444444444"
            )),
            fireblocks_tx_id: "fb-799".to_string(),
            planned_burns: vec![],
            submitted_at: Utc::now(),
        });
        events
    }

    #[test]
    fn test_close_redemption_from_burning_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let reason = "QQQM share accounting unverified".to_string();

        let validator = RedemptionTestFramework::with(mock_services())
            .given(burning_given_events(&issuer_request_id))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::RedemptionClosed {
            issuer_request_id: event_id,
            reason: event_reason,
            ..
        } = &events[0]
        else {
            panic!("Expected RedemptionClosed event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_reason, &reason);
    }

    #[test]
    fn test_close_redemption_from_failed_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let mut given = burning_given_events(&issuer_request_id);
        given.push(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "burn reverted".to_string(),
            failed_at: Utc::now(),
            fireblocks_tx_id: None,
            planned_burns: vec![],
        });

        let validator = RedemptionTestFramework::with(mock_services())
            .given(given)
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "unrecoverable".to_string(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], RedemptionEvent::RedemptionClosed { .. }));
    }

    #[test]
    fn test_close_redemption_from_burn_submitted_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let reason = "submitted burn unverifiable on-chain".to_string();

        let validator = RedemptionTestFramework::with(mock_services())
            .given(burn_submitted_given_events(&issuer_request_id))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::RedemptionClosed {
            issuer_request_id: event_id,
            reason: event_reason,
            ..
        } = &events[0]
        else {
            panic!("Expected RedemptionClosed event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_reason, &reason);
    }

    #[test]
    fn test_close_redemption_from_detected_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        RedemptionTestFramework::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("ARKK"),
                token: TokenSymbol::new("tARKK"),
                wallet: address!(
                    "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
                ),
                quantity: Quantity::new(Decimal::from(17)),
                tx_hash: b256!(
                    "0x4444444444444444444444444444444444444444444444444444444444444444"
                ),
                block_number: 45_000_000,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "too early".to_string(),
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Failed, Burning, or BurnSubmitted".to_string(),
                found: "Detected".to_string(),
            });
    }

    #[test]
    fn test_force_complete_burn_from_burning_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
        );
        let block_number = 45_989_009;
        let reason = "burn confirmed on-chain, unrecorded".to_string();

        let validator = RedemptionTestFramework::with(mock_services())
            .given(burning_given_events(&issuer_request_id))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id: issuer_request_id.clone(),
                burn_tx_hash,
                block_number,
                reason: reason.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurnForceCompleted {
            issuer_request_id: event_id,
            burn_tx_hash: event_tx_hash,
            block_number: event_block,
            reason: event_reason,
            ..
        } = &events[0]
        else {
            panic!("Expected BurnForceCompleted event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_tx_hash, &burn_tx_hash);
        assert_eq!(event_block, &block_number);
        assert_eq!(event_reason, &reason);
    }

    #[test]
    fn test_force_complete_burn_from_burn_submitted_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
        );
        let block_number = 45_989_009;
        let reason = "submitted burn confirmed on-chain".to_string();

        let validator = RedemptionTestFramework::with(mock_services())
            .given(burn_submitted_given_events(&issuer_request_id))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id: issuer_request_id.clone(),
                burn_tx_hash,
                block_number,
                reason: reason.clone(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurnForceCompleted {
            issuer_request_id: event_id,
            burn_tx_hash: event_tx_hash,
            block_number: event_block,
            reason: event_reason,
            ..
        } = &events[0]
        else {
            panic!("Expected BurnForceCompleted event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(event_tx_hash, &burn_tx_hash);
        assert_eq!(event_block, &block_number);
        assert_eq!(event_reason, &reason);
    }

    #[test]
    fn test_force_complete_burn_transitions_to_completed() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
        );

        let mut redemption = Redemption::default();
        for event in burning_given_events(&issuer_request_id) {
            redemption.apply(event);
        }

        redemption.apply(RedemptionEvent::BurnForceCompleted {
            issuer_request_id,
            burn_tx_hash,
            block_number: 45_989_009,
            reason: "verified".to_string(),
            completed_at: Utc::now(),
        });

        let Redemption::Completed { burn_tx_hash: stored, .. } = redemption
        else {
            panic!("Expected Completed state, got {}", redemption.state_name());
        };

        assert_eq!(stored, burn_tx_hash);
    }

    #[test]
    fn test_force_complete_burn_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        RedemptionTestFramework::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: b256!(
                    "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
                ),
                block_number: 45_989_009,
                reason: "nope".to_string(),
            })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Burning or BurnSubmitted".to_string(),
                found: "Uninitialized".to_string(),
            });
    }

    #[test]
    fn test_apply_tokens_burned_transitions_to_completed() {
        let mut redemption = Redemption::default();

        let issuer_request_id = IssuerRedemptionRequestId::random();
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
            fireblocks_tx_id: None,
            planned_burns: vec![],
        });

        assert_eq!(
            redemption,
            Redemption::Failed { issuer_request_id, reason: error, failed_at }
        );
    }

    #[test]
    fn test_mark_failed_from_failed_state_succeeds() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let validator = RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number: 1,
                    detected_at: Utc::now(),
                },
                RedemptionEvent::RedemptionFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: "BurningFailed: original error".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::MarkFailed {
                issuer_request_id,
                reason: "Auto-failed: insufficient balance".to_string(),
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            RedemptionEvent::RedemptionFailed { reason, .. }
            if reason.contains("insufficient balance")
        ));
    }

    // --- IssuerRedemptionRequestId tests ---

    prop_compose! {
        pub(crate) fn arb_issuer_redemption_request_id()(bytes in any::<[u8; 32]>()) -> IssuerRedemptionRequestId {
            IssuerRedemptionRequestId::new(TxHash::from(bytes))
        }
    }

    #[test]
    fn test_new_uses_full_tx_hash() {
        let tx_hash = b256!(
            "0x574378e000000000000000000000000000000000000000000000000000000000"
        );

        let id = IssuerRedemptionRequestId::new(tx_hash);

        assert_eq!(
            id.to_string(),
            "0x574378e000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_display_format_is_full_tx_hash() {
        let tx_hash = b256!(
            "0xdeadbeef00000000000000000000000000000000000000000000000000000000"
        );

        let id = IssuerRedemptionRequestId::new(tx_hash);

        let display = id.to_string();
        assert!(
            display.starts_with("0x"),
            "expected '0x' prefix, got: {display}"
        );
        assert_eq!(
            display.len(),
            66,
            "expected 66 chars (0x + 64 hex), got: {display}"
        );
        assert_eq!(
            display,
            "0xdeadbeef00000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_same_tx_hash_produces_equal_ids() {
        let tx_hash = b256!(
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );

        let id1 = IssuerRedemptionRequestId::new(tx_hash);
        let id2 = IssuerRedemptionRequestId::new(tx_hash);

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_different_tx_hashes_produce_different_ids() {
        let hash1 = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let hash2 = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );

        let id1 = IssuerRedemptionRequestId::new(hash1);
        let id2 = IssuerRedemptionRequestId::new(hash2);

        assert_ne!(id1, id2);
    }

    #[test]
    fn test_serialize_produces_full_tx_hash_string() {
        let tx_hash = b256!(
            "0x574378e000000000000000000000000000000000000000000000000000000000"
        );

        let id = IssuerRedemptionRequestId::new(tx_hash);
        let json = serde_json::to_string(&id).unwrap();

        assert_eq!(
            json,
            "\"0x574378e000000000000000000000000000000000000000000000000000000000\""
        );
    }

    #[test]
    fn test_deserialize_legacy_red_hex_string() {
        let json = "\"red-574378e0\"";

        let id: IssuerRedemptionRequestId = serde_json::from_str(json).unwrap();

        assert_eq!(id.to_string(), "red-574378e0");
    }

    #[test]
    fn test_deserialize_full_tx_hash_string() {
        let json = "\"0x574378e000000000000000000000000000000000000000000000000000000000\"";

        let id: IssuerRedemptionRequestId = serde_json::from_str(json).unwrap();

        assert_eq!(
            id.to_string(),
            "0x574378e000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        let tx_hash = b256!(
            "deadbeefcafebabe1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        let id = IssuerRedemptionRequestId::new(tx_hash);
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: IssuerRedemptionRequestId =
            serde_json::from_str(&json).unwrap();

        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_from_str_rejects_hash_without_0x_prefix() {
        let result = "574378e0".parse::<IssuerRedemptionRequestId>();
        assert!(matches!(
            result.unwrap_err(),
            super::IssuerRedemptionRequestIdParseError::Format
        ));
    }

    #[test]
    fn test_from_str_rejects_invalid_hex() {
        let result = "red-GGGGGGGG".parse::<IssuerRedemptionRequestId>();
        assert!(matches!(
            result.unwrap_err(),
            super::IssuerRedemptionRequestIdParseError::Hex(_)
        ));
    }

    #[test]
    fn test_from_str_rejects_wrong_length() {
        let result = "red-5743".parse::<IssuerRedemptionRequestId>();
        assert!(matches!(
            result.unwrap_err(),
            super::IssuerRedemptionRequestIdParseError::Slice(_)
        ));
    }

    fn test_metadata() -> RedemptionMetadata {
        RedemptionMetadata {
            issuer_request_id: IssuerRedemptionRequestId::random(),
            underlying: UnderlyingSymbol::new("RKLB"),
            token: TokenSymbol::new("tRKLB"),
            wallet: address!("0x9876543210fedcba9876543210fedcba98765432"),
            quantity: Quantity::new(Decimal::from(100)),
            detected_tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        }
    }

    #[test]
    fn test_reprocess_from_failed_state_succeeds() {
        let metadata = test_metadata();
        let expected_underlying = metadata.underlying.clone();
        let expected_id = metadata.issuer_request_id.clone();

        let validator = RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: expected_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    wallet: metadata.wallet,
                    quantity: metadata.quantity.clone(),
                    tx_hash: metadata.detected_tx_hash,
                    block_number: metadata.block_number,
                    detected_at: metadata.detected_at,
                },
                RedemptionEvent::AlpacaCallFailed {
                    issuer_request_id: expected_id.clone(),
                    error: "Alpaca bug".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::Reprocess {
                issuer_request_id: expected_id.clone(),
                metadata,
            });

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::Reprocessed {
            issuer_request_id: event_id,
            previous_state,
            underlying,
            ..
        } = &events[0]
        else {
            panic!("Expected Reprocessed event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &expected_id);
        assert_eq!(previous_state, "Failed");
        assert_eq!(underlying, &expected_underlying);
    }

    #[test]
    fn test_reprocess_from_detected_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        RedemptionTestFramework::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
                detected_at: metadata.detected_at,
            }])
            .when(RedemptionCommand::Reprocess { issuer_request_id, metadata })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Detected".to_string(),
            });
    }

    #[test]
    fn test_reprocess_from_burning_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    wallet: metadata.wallet,
                    quantity: metadata.quantity.clone(),
                    tx_hash: metadata.detected_tx_hash,
                    block_number: metadata.block_number,
                    detected_at: metadata.detected_at,
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-1",
                    ),
                    alpaca_quantity: metadata.quantity.clone(),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at: Utc::now(),
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::Reprocess { issuer_request_id, metadata })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Burning".to_string(),
            });
    }

    #[test]
    fn test_reprocess_from_completed_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    wallet: metadata.wallet,
                    quantity: metadata.quantity.clone(),
                    tx_hash: metadata.detected_tx_hash,
                    block_number: metadata.block_number,
                    detected_at: metadata.detected_at,
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-1",
                    ),
                    alpaca_quantity: metadata.quantity.clone(),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at: Utc::now(),
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
                RedemptionEvent::TokensBurned {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: b256!(
                        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    ),
                    burns: vec![BurnRecord {
                        receipt_id: uint!(1_U256),
                        shares_burned: uint!(100_000000000000000000_U256),
                    }],
                    dust_returned: U256::ZERO,
                    gas_used: 50000,
                    block_number: 99999,
                    burned_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::Reprocess {
                issuer_request_id: issuer_request_id.clone(),
                metadata,
            })
            .then_expect_error(RedemptionError::AlreadyCompleted {
                issuer_request_id,
            });
    }

    #[test]
    fn test_reprocess_from_uninitialized_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        RedemptionTestFramework::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::Reprocess { issuer_request_id, metadata })
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Uninitialized".to_string(),
            });
    }

    #[test]
    fn test_apply_reprocessed_transitions_to_detected() {
        let mut redemption = Redemption::default();
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        redemption.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at: metadata.detected_at,
        });

        redemption.apply(RedemptionEvent::AlpacaCallFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "Alpaca bug".to_string(),
            failed_at: Utc::now(),
        });

        assert!(matches!(redemption, Redemption::Failed { .. }));

        redemption.apply(RedemptionEvent::Reprocessed {
            issuer_request_id: issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at: metadata.detected_at,
            previous_state: "Failed".to_string(),
            reprocessed_at: Utc::now(),
        });

        assert_eq!(
            redemption,
            Redemption::Detected {
                metadata: RedemptionMetadata {
                    issuer_request_id,
                    underlying: metadata.underlying,
                    token: metadata.token,
                    wallet: metadata.wallet,
                    quantity: metadata.quantity,
                    detected_tx_hash: metadata.detected_tx_hash,
                    block_number: metadata.block_number,
                    detected_at: metadata.detected_at,
                }
            }
        );
    }

    fn test_resume_burn_command(
        metadata: &RedemptionMetadata,
    ) -> RedemptionCommand {
        RedemptionCommand::ResumeBurn {
            issuer_request_id: metadata.issuer_request_id.clone(),
            metadata: metadata.clone(),
            tokenization_request_id: TokenizationRequestId::new("tok-resume-1"),
            alpaca_quantity: Quantity::new(Decimal::from(100)),
            dust_quantity: Quantity::new(Decimal::ZERO),
            called_at: Utc::now(),
            alpaca_journal_completed_at: Utc::now(),
            external_tx_id: None,
        }
    }

    #[test]
    fn test_resume_burn_from_failed_state_succeeds() {
        let metadata = test_metadata();

        let validator = RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    wallet: metadata.wallet,
                    quantity: metadata.quantity.clone(),
                    tx_hash: metadata.detected_tx_hash,
                    block_number: metadata.block_number,
                    detected_at: metadata.detected_at,
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-resume-1",
                    ),
                    alpaca_quantity: Quantity::new(Decimal::from(100)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at: Utc::now(),
                },
                RedemptionEvent::RedemptionFailed {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    reason: "Alpaca journal timed out".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(test_resume_burn_command(&metadata));

        let events = validator.inspect_result().unwrap();
        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurnResumed {
            issuer_request_id: event_id,
            tokenization_request_id,
            ..
        } = &events[0]
        else {
            panic!("Expected BurnResumed event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &metadata.issuer_request_id);
        assert_eq!(
            tokenization_request_id,
            &TokenizationRequestId::new("tok-resume-1")
        );
    }

    #[test]
    fn test_resume_burn_from_completed_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        RedemptionTestFramework::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    wallet: metadata.wallet,
                    quantity: metadata.quantity.clone(),
                    tx_hash: metadata.detected_tx_hash,
                    block_number: metadata.block_number,
                    detected_at: metadata.detected_at,
                },
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-1",
                    ),
                    alpaca_quantity: metadata.quantity.clone(),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at: Utc::now(),
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
                RedemptionEvent::TokensBurned {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: b256!(
                        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    ),
                    burns: vec![BurnRecord {
                        receipt_id: uint!(1_U256),
                        shares_burned: uint!(100_000000000000000000_U256),
                    }],
                    dust_returned: U256::ZERO,
                    gas_used: 50000,
                    block_number: 99999,
                    burned_at: Utc::now(),
                },
            ])
            .when(test_resume_burn_command(&metadata))
            .then_expect_error(RedemptionError::AlreadyCompleted {
                issuer_request_id,
            });
    }

    #[test]
    fn test_resume_burn_from_detected_state_rejected() {
        let metadata = test_metadata();

        RedemptionTestFramework::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: metadata.issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
                detected_at: metadata.detected_at,
            }])
            .when(test_resume_burn_command(&metadata))
            .then_expect_error(RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Detected".to_string(),
            });
    }

    #[test]
    fn test_apply_burn_resumed_transitions_to_burning() {
        let mut redemption = Redemption::default();
        let metadata = test_metadata();
        let journal_completed_at = Utc::now();

        redemption.apply(RedemptionEvent::Detected {
            issuer_request_id: metadata.issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at: metadata.detected_at,
        });

        redemption.apply(RedemptionEvent::RedemptionFailed {
            issuer_request_id: metadata.issuer_request_id.clone(),
            reason: "timed out".to_string(),
            failed_at: Utc::now(),
        });

        assert!(matches!(redemption, Redemption::Failed { .. }));

        redemption.apply(RedemptionEvent::BurnResumed {
            issuer_request_id: metadata.issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at: metadata.detected_at,
            tokenization_request_id: TokenizationRequestId::new(
                "tok-resume-apply",
            ),
            alpaca_quantity: Quantity::new(Decimal::from(100)),
            dust_quantity: Quantity::new(Decimal::ZERO),
            called_at: Utc::now(),
            alpaca_journal_completed_at: journal_completed_at,
            external_tx_id: None,
            resumed_at: Utc::now(),
        });

        // Alpaca's updated_at is used as alpaca_journal_completed_at.
        let Redemption::Burning { alpaca_journal_completed_at, .. } =
            &redemption
        else {
            panic!(
                "Expected Burning state after BurnResumed, got {redemption:?}"
            );
        };
        assert_eq!(*alpaca_journal_completed_at, journal_completed_at);
    }

    proptest! {
        #[test]
        fn test_serde_roundtrip_proptest(id in arb_issuer_redemption_request_id()) {
            let json = serde_json::to_string(&id).unwrap();
            let deserialized: IssuerRedemptionRequestId =
                serde_json::from_str(&json).unwrap();
            prop_assert_eq!(&id, &deserialized);
        }

        #[test]
        fn test_display_always_uses_full_hash_for_new_ids(id in arb_issuer_redemption_request_id()) {
            let display = id.to_string();
            prop_assert!(display.starts_with("0x"));
            prop_assert_eq!(display.len(), 66);
        }
    }
}
