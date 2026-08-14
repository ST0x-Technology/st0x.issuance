mod cmd;
mod event;
pub(crate) mod view;

pub(crate) mod burn_manager;
pub(crate) mod force_complete;
pub(crate) mod journal_manager;
pub(crate) mod poller;
pub(crate) mod redeem_call_manager;
#[cfg(test)]
pub(crate) mod test_utils;
pub(crate) mod transfer;

use alloy::hex;
use alloy::primitives::{Address, B256, FixedBytes, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EventSourced, Nil};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::warn;

use crate::Quantity;
use crate::config::VaultMode;
use crate::mint::TokenizationRequestId;
use crate::redemption::burn_manager::{
    extract_tx_hash, is_pending_burn_confirmation, should_release_reserved_burn,
};

/// Returns whether any signed transaction on the same signer network — a
/// burn or a mint — is still awaiting its terminal outcome, excluding at
/// most this redemption's own reservation.
///
/// Reads the trigger-maintained `active_signer_intents` table rather than
/// re-deriving the answer from event streams: the triggers update the table
/// in the same transaction that appends the intent event, so the table is
/// the single source of truth and cannot drift from the reserve/release
/// rules the migration encodes. Because the table is keyed by network, an
/// outstanding Mint intent blocks a burn on the same nonce domain too —
/// both flows sign with the same key.
pub(crate) async fn has_unresolved_signer_intent(
    pool: &Pool<Sqlite>,
    network: Network,
    excluding: Option<&IssuerRedemptionRequestId>,
) -> Result<bool, sqlx::Error> {
    // `None` collapses to "" rather than a NULL bind: `aggregate_id = NULL`
    // is NULL in SQL, `NOT NULL` is NULL, and a NULL predicate silently
    // excludes EVERY row — the empty string matches no aggregate_id, which
    // is the intended "exclude nothing".
    let excluding = excluding.map(ToString::to_string).unwrap_or_default();
    let exists = sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM active_signer_intents
            WHERE network = ?
              AND NOT (aggregate_type = 'Redemption' AND aggregate_id = ?)
        )
        ",
    )
    .bind(network.as_str())
    .bind(excluding)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

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
use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
use crate::vault::{
    BurnRequestOrigin, BurnTxStatus, MultiBurnEntry, MultiBurnParams,
    NetworkVaultServices, SendableTxWithHash, TxId, UnconfiguredNetworkError,
    VaultError, VaultService,
};

pub(super) const fn default_redemption_network() -> Network {
    Network::Base
}

/// Per-network vault services for redemption command handling ([RAI-1207]).
pub(crate) struct RedemptionServices {
    vaults: NetworkVaultServices,
}

impl RedemptionServices {
    pub(crate) const fn new(vaults: NetworkVaultServices) -> Self {
        Self { vaults }
    }

    #[cfg(test)]
    pub(crate) fn with_single_vault(
        network: Network,
        vault: Arc<dyn VaultService>,
    ) -> Self {
        Self::new(NetworkVaultServices::with_single_vault(
            network,
            crate::test_utils::ANVIL_CHAIN_ID,
            vault,
        ))
    }

    fn vault_for(
        &self,
        network: Network,
    ) -> Result<&Arc<dyn VaultService>, RedemptionError> {
        Ok(self.vaults.service(network)?)
    }
}

pub(crate) use cmd::RedemptionCommand;
pub(crate) use event::{
    BurnFailureClassification, BurnRecord, RedemptionEvent, TokensBurnedData,
};
pub(crate) use view::{
    RedemptionView, RedemptionViewError, find_alpaca_called, find_detected,
    find_stuck,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RedemptionMetadata {
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    #[serde(default = "default_redemption_network")]
    pub(crate) network: Network,
    pub(crate) wallet: Address,
    pub(crate) quantity: Quantity,
    pub(crate) detected_tx_hash: B256,
    pub(crate) block_number: u64,
    pub(crate) detected_at: DateTime<Utc>,
    /// Mode anchor captured on `Detected`. Every mode-dependent burn step
    /// derives from this persisted value, never from live config. Snapshots
    /// and states persisted before orchestrator mode default to `VaultDirect`.
    #[serde(default)]
    pub(crate) burn_mode: VaultMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Redemption {
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
        /// Exact transaction from a failed attempt whose identity is retained
        /// while a reserved replacement is prepared.
        #[serde(default)]
        prior_burn_tx: Option<SendableTxWithHash>,
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
        tx_id: TxId,
        planned_burns: Vec<event::BurnRecord>,
        #[serde(default)]
        sendable_tx: SendableTxWithHash,
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
        /// A signed burn persisted before the failure and still capable of
        /// landing on-chain. Retained both for durable recovery exhaustion and
        /// so terminal admin actions cannot bypass the acknowledgement guard.
        #[serde(default)]
        unresolved_burn_tx: Option<SendableTxWithHash>,
    },
    Closed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        closed_at: DateTime<Utc>,
        /// The still-unresolved signed burn the pre-close state carried,
        /// retained through the closure (the close event records at most an
        /// acknowledged hash, and pre-acknowledgement closures recorded
        /// nothing). A later force-complete against a different proving
        /// transaction must re-acknowledge this transaction — it may still
        /// land and double-burn.
        #[serde(default)]
        unresolved_burn_tx: Option<SendableTxWithHash>,
    },
    BurnIntended {
        metadata: RedemptionMetadata,
        tokenization_request_id: TokenizationRequestId,
        /// Quantity to burn (what Alpaca processed, 9 decimals)
        alpaca_quantity: Quantity,
        /// Dust quantity to return to user
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
        alpaca_journal_completed_at: DateTime<Utc>,
        planned_burns: Vec<event::BurnRecord>,
        #[serde(default)]
        external_tx_id: Option<BurnExternalTxId>,
        #[serde(default)]
        sendable_tx: SendableTxWithHash,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BurnRecoveryAction {
    Rebroadcast,
    Replace,
}

/// Input parameters for the BurnTokens command handler.
///
/// Groups burn-related parameters to reduce argument count. The `user` field
/// is derived from aggregate state, not passed in the command.
struct BurnInput {
    vault: Address,
    burns: Vec<MultiBurnEntry>,
    dust_shares: U256,
    owner: Address,
    external_tx_id: Option<BurnExternalTxId>,
}

struct ResumeBurnInput {
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
            | Self::BurnSubmitted { metadata, .. }
            | Self::BurnIntended { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    /// Returns the quantity sent to Alpaca (truncated to 9 decimals).
    /// Available in every state from AlpacaCalled through burn submission.
    pub(crate) const fn alpaca_quantity(&self) -> Option<&Quantity> {
        match self {
            Self::AlpacaCalled { alpaca_quantity, .. }
            | Self::Burning { alpaca_quantity, .. }
            | Self::BurnIntended { alpaca_quantity, .. }
            | Self::BurnSubmitted { alpaca_quantity, .. } => {
                Some(alpaca_quantity)
            }
            _ => None,
        }
    }

    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::Detected { .. } => "Detected",
            Self::AlpacaCalled { .. } => "AlpacaCalled",
            Self::Burning { .. } => "Burning",
            Self::BurnSubmitted { .. } => "BurnSubmitted",
            Self::Completed { .. } => "Completed",
            Self::Failed { .. } => "Failed",
            Self::Closed { .. } => "Closed",
            Self::BurnIntended { .. } => "BurnIntended",
        }
    }

    fn handle_record_alpaca_call(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
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
        issuer_request_id: IssuerRedemptionRequestId,
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
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
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

        Ok(vec![RedemptionEvent::RedemptionFailed {
            issuer_request_id,
            reason,
            failed_at: Utc::now(),
        }])
    }

    /// Submits the burn transaction to the signing backend.
    /// Produces `BurnTxSubmitted` on success; failure is propagated to caller.
    async fn handle_burn_tokens(
        &self,
        services: &RedemptionServices,
        issuer_request_id: IssuerRedemptionRequestId,
        input: BurnInput,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let (Self::BurnIntended { metadata, sendable_tx, .. }
        | Self::BurnSubmitted { metadata, sendable_tx, .. }) = self
        else {
            return Err(RedemptionError::InvalidState {
                expected: "BurnIntended or BurnSubmitted".to_string(),
                found: self.state_name().to_string(),
            });
        };

        let user_wallet = metadata.wallet;
        let vault_service = services.vault_for(metadata.network)?;

        let planned_burns: Vec<BurnRecord> = input
            .burns
            .iter()
            .map(|entry| BurnRecord {
                receipt_id: entry.receipt_id,
                shares_burned: entry.burn_shares,
            })
            .collect();

        let params = MultiBurnParams {
            vault: input.vault,
            burns: input.burns,
            dust_shares: input.dust_shares,
            owner: input.owner,
            user: user_wallet,
            origin: BurnRequestOrigin::Redemption(issuer_request_id.clone()),
            detected_tx_hash: metadata.detected_tx_hash,
            external_tx_id: input.external_tx_id,
        };

        let submitted = vault_service
            .submit_burn(params, sendable_tx.clone())
            .await
            .map_err(|error: VaultError| RedemptionError::Vault {
                release_reservation: should_release_reserved_burn(&error),
                tx_id: extract_tx_hash(&error).map(Into::into),
                message: error.to_string(),
            })?;

        Ok(vec![RedemptionEvent::BurnTxSubmitted {
            issuer_request_id,
            external_tx_id: BurnExternalTxId::from_string(
                submitted.external_tx_id,
            ),
            tx_id: submitted.tx_id,
            planned_burns,
            submitted_at: Utc::now(),
        }])
    }

    /// Confirms a previously submitted burn transaction.
    async fn handle_confirm_burn(
        &self,
        services: &RedemptionServices,
        issuer_request_id: IssuerRedemptionRequestId,
        tx_id: TxId,
        dust_shares: U256,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let (metadata, stored_tx_id) = match self {
            Self::BurnSubmitted { metadata, tx_id, .. } => {
                (metadata, tx_id.clone())
            }
            Self::BurnIntended { metadata, sendable_tx, .. } => {
                (metadata, sendable_tx.hash.into())
            }
            _ => {
                return Err(RedemptionError::InvalidState {
                    expected: "BurnSubmitted or BurnIntended".to_string(),
                    found: self.state_name().to_string(),
                });
            }
        };

        if stored_tx_id != tx_id {
            return Err(RedemptionError::TxIdMismatch {
                expected: stored_tx_id,
                provided: tx_id,
            });
        }

        let vault_service = services.vault_for(metadata.network)?;

        let result = vault_service
            .confirm_burn(&tx_id, dust_shares)
            .await
            .map_err(|error| {
                if is_pending_burn_confirmation(&error) {
                    return RedemptionError::BurnConfirmationPending {
                        tx_id: tx_id.clone(),
                        message: error.to_string(),
                    };
                }

                RedemptionError::Vault {
                    release_reservation: should_release_reserved_burn(&error),
                    tx_id: extract_tx_hash(&error).map(Into::into),
                    message: error.to_string(),
                }
            })?;

        let burns = result
            .burns
            .into_iter()
            .map(|burn| BurnRecord {
                receipt_id: burn.receipt_id,
                shares_burned: burn.shares_burned,
            })
            .collect();

        Ok(vec![RedemptionEvent::TokensBurned(TokensBurnedData {
            issuer_request_id,
            tx_hash: result.tx_hash,
            burns,
            dust_returned: result.dust_returned,
            gas_used: result.gas_used,
            block_number: result.block_number,
            burned_at: Utc::now(),
        })])
    }

    fn handle_record_burn_failure(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
        tx_id: Option<TxId>,
        planned_burns: Vec<BurnRecord>,
        classification: BurnFailureClassification,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(
            self,
            Self::Burning { .. }
                | Self::BurnIntended { .. }
                | Self::BurnSubmitted { .. }
        ) {
            return Err(RedemptionError::InvalidState {
                expected: "Burning, BurnIntended, or BurnSubmitted".to_string(),
                found: self.state_name().to_string(),
            });
        }

        Ok(vec![RedemptionEvent::BurningFailed {
            issuer_request_id,
            error,
            failed_at: Utc::now(),
            tx_id,
            planned_burns,
            classification,
        }])
    }

    /// Reprocessing is only valid from `Failed` state. Post-Alpaca states
    /// (`AlpacaCalled`, `Burning`) have dedicated recovery paths in the
    /// burn/journal managers and resetting them to `Detected` would cause
    /// a duplicate Alpaca redeem call.
    fn handle_reprocess(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        metadata: RedemptionMetadata,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
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

        Ok(vec![RedemptionEvent::Reprocessed {
            issuer_request_id,
            underlying: metadata.underlying,
            token: metadata.token,
            network: metadata.network,
            wallet: metadata.wallet,
            quantity: metadata.quantity,
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at: metadata.detected_at,
            previous_state: self.state_name().to_string(),
            reprocessed_at: Utc::now(),
            burn_mode: metadata.burn_mode,
        }])
    }

    /// Resumes a post-Alpaca failed redemption directly to Burning state.
    /// Only valid from `Failed` state. The API layer validates that Alpaca
    /// was already called before issuing this command.
    fn handle_resume_burn(
        &self,
        input: ResumeBurnInput,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
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

        Ok(vec![RedemptionEvent::BurnResumed {
            issuer_request_id: input.issuer_request_id,
            underlying: input.metadata.underlying,
            token: input.metadata.token,
            network: input.metadata.network,
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
            burn_mode: input.metadata.burn_mode,
        }])
    }

    fn handle_record_existing_burn(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        tx_id: TxId,
        tx_hash: B256,
        planned_burns: Vec<BurnRecord>,
        block_number: u64,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
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

        Ok(vec![RedemptionEvent::ExistingBurnRecovered {
            issuer_request_id,
            tx_id,
            tx_hash,
            burns: planned_burns,
            block_number,
            recovered_at: Utc::now(),
        }])
    }

    /// Admin-closes a redemption that cannot be auto-recovered. Valid from
    /// `Failed`, `Burning`, `BurnIntended`, or `BurnSubmitted` — the honest terminal path for a
    /// redemption whose burn cannot/should not be re-submitted and is not
    /// verifiable on-chain. Widening beyond `Failed` covers redemptions stuck in
    /// `Burning` (including the `Failed -> Burning` recovery regression, which
    /// would otherwise strand a previously-closeable redemption).
    fn handle_close_redemption(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(
            self,
            Self::Failed { .. }
                | Self::Burning { .. }
                | Self::BurnIntended { .. }
                | Self::BurnSubmitted { .. }
        ) {
            return Err(match self {
                Self::Completed { .. } | Self::Closed { .. } => {
                    RedemptionError::AlreadyCompleted { issuer_request_id }
                }
                _ => RedemptionError::InvalidState {
                    expected: "Failed, Burning, BurnIntended, or BurnSubmitted"
                        .to_string(),
                    found: self.state_name().to_string(),
                },
            });
        }

        let persisted_burn_tx = self
            .persisted_unresolved_burn_tx()
            .filter(|sendable_tx| !sendable_tx.tx.is_empty());
        let acknowledged_unresolved_burn_tx_hash =
            match (persisted_burn_tx, acknowledged_unresolved_burn_tx_hash) {
                (Some(sendable_tx), acknowledgement) => {
                    Some(Self::require_unresolved_burn_acknowledgement(
                        sendable_tx.hash,
                        acknowledgement,
                    )?)
                }
                (None, Some(provided)) => {
                    return Err(
                    RedemptionError::UnexpectedUnresolvedBurnAcknowledgement {
                        provided,
                    },
                );
                }
                (None, None) => None,
            };

        Ok(vec![RedemptionEvent::RedemptionClosed {
            issuer_request_id,
            reason,
            acknowledged_unresolved_burn_tx_hash,
            closed_at: Utc::now(),
        }])
    }

    /// Admin-terminalizes a redemption stuck in
    /// `Burning`/`BurnIntended`/`BurnSubmitted` whose
    /// burn already landed on-chain but was never recorded. The admin layer
    /// verifies `burn_tx_hash` on-chain before issuing this command, so the
    /// aggregate trusts the supplied tx hash and block number and records the
    /// proving terminal event, transitioning to `Completed`.
    fn handle_force_complete_burn(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        block_number: u64,
        reason: String,
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(
            self,
            Self::Burning { .. }
                | Self::BurnIntended { .. }
                | Self::BurnSubmitted { .. }
                | Self::Failed { .. }
                | Self::Closed { .. }
        ) {
            return Err(match self {
                Self::Completed { .. } => {
                    RedemptionError::AlreadyCompleted { issuer_request_id }
                }
                _ => RedemptionError::InvalidState {
                    expected: "Burning, BurnIntended, BurnSubmitted, \
                               Failed, or Closed"
                        .to_string(),
                    found: self.state_name().to_string(),
                },
            });
        }

        // A legacy `Failed` redemption has no persisted signed transaction to
        // bind the proving hash against — the burn went out through a
        // custodian's API, identified only by a backend transaction id the
        // current backend cannot look up. For that shape the caller's
        // on-chain verification of the planned burns is the entire proof, so
        // there is no hash to bind and nothing unresolved to acknowledge.
        // `Closed` joins the same split: its state retains whatever signed
        // burn survived to the moment of closure (recorded acknowledgement or
        // not — pre-acknowledgement closures recorded nothing), so a closure
        // that still carries one keeps the full binding and acknowledgement
        // guard, and only a closure of a custodian-era burn with nothing
        // persisted goes through as legacy.
        let legacy_burn_without_persisted_tx =
            matches!(self, Self::Failed { .. } | Self::Closed { .. })
                && self
                    .persisted_unresolved_burn_tx()
                    .filter(|sendable_tx| !sendable_tx.tx.is_empty())
                    .is_none();
        let acknowledged_unresolved_burn_tx_hash =
            if legacy_burn_without_persisted_tx {
                if let Some(provided) = acknowledged_unresolved_burn_tx_hash {
                    return Err(
                    RedemptionError::RedundantUnresolvedBurnAcknowledgement {
                        provided,
                    },
                );
                }

                None
            } else {
                self.validate_force_complete_burn_hash(
                    burn_tx_hash,
                    acknowledged_unresolved_burn_tx_hash,
                )?
            };

        Ok(vec![RedemptionEvent::BurnForceCompleted {
            issuer_request_id,
            burn_tx_hash,
            block_number,
            reason,
            acknowledged_unresolved_burn_tx_hash,
            completed_at: Utc::now(),
        }])
    }

    fn require_unresolved_burn_acknowledgement(
        persisted_burn_hash: B256,
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    ) -> Result<B256, RedemptionError> {
        let acknowledged_hash = acknowledged_unresolved_burn_tx_hash.ok_or(
            RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                burn_tx_hash: persisted_burn_hash,
            },
        )?;
        if acknowledged_hash != persisted_burn_hash {
            return Err(
                RedemptionError::UnresolvedBurnAcknowledgementMismatch {
                    expected: persisted_burn_hash,
                    provided: acknowledged_hash,
                },
            );
        }

        Ok(acknowledged_hash)
    }

    fn handle_confirm_alpaca_complete(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
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
            prior_burn_tx: None,
        };
    }

    /// Prepares a signed tx and precomputed tx hash ready for broadcasting onchain
    /// Produces `BurnIntended` on success; failure is propagated to caller.
    async fn handle_intend_burn(
        &self,
        services: &RedemptionServices,
        issuer_request_id: IssuerRedemptionRequestId,
        input: BurnInput,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        // `BurnIntended` always holds a signed transaction, so re-preparing
        // from it would sign a replacement that could burn twice if the
        // original already reached the node. Recovery re-broadcasts the
        // persisted transaction instead.
        let Self::Burning { metadata, .. } = self else {
            return Err(RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: self.state_name().to_string(),
            });
        };

        let user_wallet = metadata.wallet;
        let vault_service = services.vault_for(metadata.network)?;

        let planned_burns: Vec<BurnRecord> = input
            .burns
            .iter()
            .map(|entry| BurnRecord {
                receipt_id: entry.receipt_id,
                shares_burned: entry.burn_shares,
            })
            .collect();

        let params = MultiBurnParams {
            vault: input.vault,
            burns: input.burns,
            dust_shares: input.dust_shares,
            owner: input.owner,
            user: user_wallet,
            origin: BurnRequestOrigin::Redemption(issuer_request_id.clone()),
            detected_tx_hash: metadata.detected_tx_hash,
            external_tx_id: input.external_tx_id,
        };

        let sendable_tx =
            vault_service.prepare_burn_tx(&params).await.map_err(
                |error: VaultError| RedemptionError::PreparingBurnTxFailed {
                    message: error.to_string(),
                },
            )?;

        Ok(vec![RedemptionEvent::BurnIntended {
            issuer_request_id,
            sendable_tx,
            planned_burns,
        }])
    }

    const fn current_sendable_tx(&self) -> Option<&SendableTxWithHash> {
        match self {
            Self::BurnIntended { sendable_tx, .. }
            | Self::BurnSubmitted { sendable_tx, .. } => Some(sendable_tx),
            _ => None,
        }
    }

    const fn persisted_unresolved_burn_tx(
        &self,
    ) -> Option<&SendableTxWithHash> {
        match self {
            Self::BurnIntended { sendable_tx, .. }
            | Self::BurnSubmitted { sendable_tx, .. } => Some(sendable_tx),
            Self::Burning { prior_burn_tx, .. } => prior_burn_tx.as_ref(),
            Self::Failed { unresolved_burn_tx, .. }
            | Self::Closed { unresolved_burn_tx, .. } => {
                unresolved_burn_tx.as_ref()
            }
            _ => None,
        }
    }

    fn persisted_burn_hash(&self) -> Result<B256, RedemptionError> {
        Ok(self.persisted_burn_tx()?.hash)
    }

    pub(crate) fn validate_force_complete_burn_hash(
        &self,
        proving_burn_tx_hash: B256,
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    ) -> Result<Option<B256>, RedemptionError> {
        let persisted_burn_hash = self.persisted_burn_hash()?;
        if persisted_burn_hash == proving_burn_tx_hash {
            if let Some(provided) = acknowledged_unresolved_burn_tx_hash {
                return Err(
                    RedemptionError::RedundantUnresolvedBurnAcknowledgement {
                        provided,
                    },
                );
            }

            return Ok(None);
        }

        Ok(Some(Self::require_unresolved_burn_acknowledgement(
            persisted_burn_hash,
            acknowledged_unresolved_burn_tx_hash,
        )?))
    }

    fn persisted_burn_tx(
        &self,
    ) -> Result<&SendableTxWithHash, RedemptionError> {
        self.persisted_unresolved_burn_tx()
            .filter(|sendable_tx| !sendable_tx.tx.is_empty())
            .ok_or(RedemptionError::PersistedBurnHashUnavailable)
    }

    fn verify_recovery_transaction(
        &self,
        tx_hash: B256,
        nonce: u64,
    ) -> Result<(), RedemptionError> {
        let sendable_tx = self.current_sendable_tx().ok_or_else(|| {
            RedemptionError::InvalidState {
                expected: "BurnIntended or BurnSubmitted".to_string(),
                found: self.state_name().to_string(),
            }
        })?;
        if sendable_tx.hash != tx_hash || sendable_tx.nonce != nonce {
            return Err(RedemptionError::RecoveryTransactionMismatch {
                expected_hash: sendable_tx.hash,
                expected_nonce: sendable_tx.nonce,
                provided_hash: tx_hash,
                provided_nonce: nonce,
            });
        }
        Ok(())
    }

    fn handle_record_burn_recovery_attempt(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        nonce: u64,
        action: BurnRecoveryAction,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        self.verify_recovery_transaction(tx_hash, nonce)?;
        Ok(vec![RedemptionEvent::BurnRecoveryAttempted {
            issuer_request_id,
            tx_hash,
            nonce,
            action,
            attempted_at: Utc::now(),
        }])
    }

    fn handle_record_burn_preparation_recovery_attempt(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        attempt: u32,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(self, Self::Failed { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: self.state_name().to_string(),
            });
        }
        Ok(vec![RedemptionEvent::BurnPreparationRecoveryAttempted {
            issuer_request_id,
            attempt,
            attempted_at: Utc::now(),
        }])
    }

    fn handle_record_burn_recovery_exhausted(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        nonce: u64,
        attempts: u32,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        self.verify_recovery_exhaustion_transaction(tx_hash, nonce)?;
        Ok(vec![RedemptionEvent::BurnRecoveryExhausted {
            issuer_request_id,
            tx_hash,
            nonce,
            attempts,
            exhausted_at: Utc::now(),
        }])
    }

    fn handle_record_burn_preparation_recovery_exhausted(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        attempts: u32,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        if !matches!(self, Self::Failed { .. }) {
            return Err(RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: self.state_name().to_string(),
            });
        }
        Ok(vec![RedemptionEvent::BurnPreparationRecoveryExhausted {
            issuer_request_id,
            attempts,
            exhausted_at: Utc::now(),
        }])
    }

    fn verify_recovery_exhaustion_transaction(
        &self,
        tx_hash: B256,
        nonce: u64,
    ) -> Result<(), RedemptionError> {
        let sendable_tx = self.current_sendable_tx().or(match self {
            Self::Burning { prior_burn_tx, .. } => prior_burn_tx.as_ref(),
            Self::Failed { unresolved_burn_tx, .. } => {
                unresolved_burn_tx.as_ref()
            }
            _ => None,
        });
        let sendable_tx =
            sendable_tx.ok_or_else(|| RedemptionError::InvalidState {
                expected: "state with an exact recovery transaction"
                    .to_string(),
                found: self.state_name().to_string(),
            })?;
        if sendable_tx.hash != tx_hash || sendable_tx.nonce != nonce {
            return Err(RedemptionError::RecoveryTransactionMismatch {
                expected_hash: sendable_tx.hash,
                expected_nonce: sendable_tx.nonce,
                provided_hash: tx_hash,
                provided_nonce: nonce,
            });
        }
        Ok(())
    }

    async fn handle_replace_dead_burn(
        &self,
        services: &RedemptionServices,
        issuer_request_id: IssuerRedemptionRequestId,
        owner: Address,
    ) -> Result<Vec<RedemptionEvent>, RedemptionError> {
        let network = self
            .metadata()
            .map(|metadata| metadata.network)
            .ok_or_else(|| RedemptionError::InvalidState {
                expected: "BurnIntended or BurnSubmitted".to_string(),
                found: self.state_name().to_string(),
            })?;
        let vault_service = services.vault_for(network)?;
        let sendable_tx = self.current_sendable_tx().ok_or_else(|| {
            RedemptionError::InvalidState {
                expected: "BurnIntended or BurnSubmitted".to_string(),
                found: self.state_name().to_string(),
            }
        })?;
        if vault_service.classify_burn_tx(owner, sendable_tx).await.map_err(
            |_| RedemptionError::BurnRecoveryClassificationFailed {
                tx_hash: sendable_tx.hash,
                nonce: sendable_tx.nonce,
            },
        )? != BurnTxStatus::ProvablyDead
        {
            return Err(RedemptionError::BurnReplacementNotSafe {
                tx_hash: sendable_tx.hash,
                nonce: sendable_tx.nonce,
            });
        }

        let planned_burns = match self {
            Self::BurnIntended { planned_burns, .. }
            | Self::BurnSubmitted { planned_burns, .. } => {
                planned_burns.clone()
            }
            _ => Vec::new(),
        };
        let replacement = vault_service
            .prepare_replacement_burn_tx(owner, sendable_tx)
            .await
            .map_err(|_| RedemptionError::BurnReplacementPreparationFailed {
                tx_hash: sendable_tx.hash,
                nonce: sendable_tx.nonce,
            })?;
        replacement
            .validate_replacement_for_owner(sendable_tx, owner)
            .map_err(|_| RedemptionError::BurnReplacementValidationFailed {
                previous_hash: sendable_tx.hash,
                previous_nonce: sendable_tx.nonce,
                replacement_hash: replacement.hash,
                replacement_nonce: replacement.nonce,
            })?;
        if replacement.nonce <= sendable_tx.nonce
            || replacement.hash == sendable_tx.hash
        {
            return Err(RedemptionError::BurnReplacementNotFresh {
                previous_hash: sendable_tx.hash,
                previous_nonce: sendable_tx.nonce,
                replacement_hash: replacement.hash,
                replacement_nonce: replacement.nonce,
            });
        }

        Ok(vec![RedemptionEvent::BurnIntended {
            issuer_request_id,
            sendable_tx: replacement,
            planned_burns,
        }])
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
            RedemptionEvent::BurnTxSubmitted { external_tx_id, .. } => {
                Some(Redemption::next_burn_retry_external_tx_id(
                    detected_tx_hash,
                    external_tx_id,
                ))
            }
            _ => None,
        })
        .transpose()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum RedemptionError {
    #[error("Redemption already detected for request: {issuer_request_id}")]
    AlreadyDetected { issuer_request_id: IssuerRedemptionRequestId },
    #[error("Invalid state for operation: expected {expected}, found {found}")]
    InvalidState { expected: String, found: String },
    #[error(
        "Redemption already completed, cannot reprocess: {issuer_request_id}"
    )]
    AlreadyCompleted { issuer_request_id: IssuerRedemptionRequestId },
    #[error("Vault error: {message}")]
    Vault {
        message: String,
        /// Whether the failed burn definitively consumed no shares, so its
        /// receipt reservation is safe to release. Computed from the typed
        /// `VaultError` here (the aggregate boundary) because event-sorcery
        /// requires this error to be serializable, so the `VaultError` itself
        /// cannot cross into the burn orchestrator.
        release_reservation: bool,
        /// Transaction id pulled from the `VaultError`, if any, so a
        /// possibly-in-flight burn stays recoverable after the type is erased.
        tx_id: Option<TxId>,
    },
    #[error(
        "Transaction ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    TxIdMismatch { expected: TxId, provided: TxId },
    #[error("Burn confirmation remains pending for {tx_id}: {message}")]
    BurnConfirmationPending { tx_id: TxId, message: String },
    #[error(
        "Unresolved persisted burn {burn_tx_hash:?} requires explicit operator acknowledgement"
    )]
    UnresolvedBurnRequiresAcknowledgement { burn_tx_hash: B256 },
    #[error(
        "Unresolved burn acknowledgement mismatch: expected {expected:?}, provided {provided:?}"
    )]
    UnresolvedBurnAcknowledgementMismatch { expected: B256, provided: B256 },
    #[error(
        "Unresolved burn acknowledgement {provided:?} was provided, but this redemption has no persisted signed burn"
    )]
    UnexpectedUnresolvedBurnAcknowledgement { provided: B256 },
    #[error(
        "Unresolved burn acknowledgement {provided:?} is redundant because the proving burn is the persisted signed burn"
    )]
    RedundantUnresolvedBurnAcknowledgement { provided: B256 },
    #[error(
        "Force-completion requires a non-empty exact transaction persisted for this redemption"
    )]
    PersistedBurnHashUnavailable,
    #[error(
        "Recovery transaction mismatch: expected {expected_hash:?}/{expected_nonce}, provided {provided_hash:?}/{provided_nonce}"
    )]
    RecoveryTransactionMismatch {
        expected_hash: B256,
        expected_nonce: u64,
        provided_hash: B256,
        provided_nonce: u64,
    },
    #[error(
        "Burn transaction {tx_hash:?} at nonce {nonce} can still land; replacement refused"
    )]
    BurnReplacementNotSafe { tx_hash: B256, nonce: u64 },
    #[error("Failed to classify persisted burn {tx_hash:?} at nonce {nonce}")]
    BurnRecoveryClassificationFailed { tx_hash: B256, nonce: u64 },
    #[error(
        "Failed to prepare replacement for burn {tx_hash:?} at nonce {nonce}"
    )]
    BurnReplacementPreparationFailed { tx_hash: B256, nonce: u64 },
    #[error(
        "Burn replacement {replacement_hash:?}/{replacement_nonce} failed validation against {previous_hash:?}/{previous_nonce}"
    )]
    BurnReplacementValidationFailed {
        previous_hash: B256,
        previous_nonce: u64,
        replacement_hash: B256,
        replacement_nonce: u64,
    },
    #[error(
        "Burn replacement {replacement_hash:?}/{replacement_nonce} is not fresh relative to {previous_hash:?}/{previous_nonce}"
    )]
    BurnReplacementNotFresh {
        previous_hash: B256,
        previous_nonce: u64,
        replacement_hash: B256,
        replacement_nonce: u64,
    },
    #[error(
        "Burn retry attempt counter overflowed for external tx id: {latest_external_tx_id}"
    )]
    RetryAttemptOverflow { latest_external_tx_id: BurnExternalTxId },
    #[error("Failed to prepare sendable signed tx: {message}")]
    PreparingBurnTxFailed { message: String },
    #[error("network {network} is not configured")]
    NetworkNotConfigured { network: Network },
}

impl From<UnconfiguredNetworkError> for RedemptionError {
    fn from(error: UnconfiguredNetworkError) -> Self {
        Self::NetworkNotConfigured { network: error.network }
    }
}

#[async_trait]
impl EventSourced for Redemption {
    type Id = IssuerRedemptionRequestId;
    type Event = RedemptionEvent;
    type Command = RedemptionCommand;
    type Error = RedemptionError;
    type Services = RedemptionServices;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "Redemption";
    const PROJECTION: Nil = Nil;
    // 6: `Closed` gained `unresolved_burn_tx`. Snapshots serialized under 5
    // would deserialize the field as `None` even when a still-mineable burn
    // survived the closure, silently dropping the re-acknowledgement guard;
    // the bump clears them so the state rebuilds from events, which carry
    // the retained transaction.
    const SCHEMA_VERSION: u64 = 6;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                network,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
                burn_mode,
            } => Some(Self::Detected {
                metadata: RedemptionMetadata {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token: token.clone(),
                    network: *network,
                    wallet: *wallet,
                    quantity: quantity.clone(),
                    detected_tx_hash: *tx_hash,
                    block_number: *block_number,
                    detected_at: *detected_at,
                    burn_mode: *burn_mode,
                },
            }),
            _ => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        let mut next = entity.clone();
        next.apply_event(event.clone());
        Ok(Some(next))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            RedemptionCommand::Detect {
                issuer_request_id,
                underlying,
                token,
                network,
                wallet,
                quantity,
                tx_hash,
                block_number,
                burn_mode,
            } => Ok(vec![RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                network,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at: Utc::now(),
                burn_mode,
            }]),
            RedemptionCommand::RecordAlpacaCall { .. }
            | RedemptionCommand::RecordAlpacaFailure { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "Detected".to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::ConfirmAlpacaComplete { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "AlpacaCalled".to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::MarkFailed { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "Detected, AlpacaCalled, Burning, or Failed"
                        .to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::IntendBurn { .. } => {
                Err(RedemptionError::InvalidState {
                    expected:
                        "Burning or BurnIntended without a signed transaction"
                            .to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::RecordBurnRecoveryAttempt { .. }
            | RedemptionCommand::RecordBurnPreparationRecoveryAttempt {
                ..
            }
            | RedemptionCommand::RecordBurnRecoveryExhausted { .. }
            | RedemptionCommand::RecordBurnPreparationRecoveryExhausted {
                ..
            }
            | RedemptionCommand::ReplaceDeadBurn { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "BurnIntended or BurnSubmitted".to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::BurnTokens { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "Burning".to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::ConfirmBurn { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "BurnSubmitted".to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::RecordBurnFailure { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "Burning, BurnIntended, or BurnSubmitted"
                        .to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::ForceCompleteBurn { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "Burning, BurnIntended, BurnSubmitted, \
                               Failed, or Closed"
                        .to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
            RedemptionCommand::RecordExistingBurn { .. }
            | RedemptionCommand::CloseRedemption { .. }
            | RedemptionCommand::Reprocess { .. }
            | RedemptionCommand::ResumeBurn { .. } => {
                Err(RedemptionError::InvalidState {
                    expected: "Failed".to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            RedemptionCommand::Detect { issuer_request_id, .. } => {
                Err(RedemptionError::AlreadyDetected { issuer_request_id })
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
                burns,
                dust_shares,
                owner,
                external_tx_id,
            } => {
                self.handle_burn_tokens(
                    services,
                    issuer_request_id,
                    BurnInput {
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
                tx_id,
                dust_shares,
            } => {
                self.handle_confirm_burn(
                    services,
                    issuer_request_id,
                    tx_id,
                    dust_shares,
                )
                .await
            }
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id,
                error,
                tx_id,
                planned_burns,
                classification,
            } => self.handle_record_burn_failure(
                issuer_request_id,
                error,
                tx_id,
                planned_burns,
                classification,
            ),
            RedemptionCommand::RecordExistingBurn {
                issuer_request_id,
                tx_id,
                tx_hash,
                planned_burns,
                block_number,
            } => self.handle_record_existing_burn(
                issuer_request_id,
                tx_id,
                tx_hash,
                planned_burns,
                block_number,
            ),
            RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason,
                acknowledged_unresolved_burn_tx_hash,
            } => self.handle_close_redemption(
                issuer_request_id,
                reason,
                acknowledged_unresolved_burn_tx_hash,
            ),
            RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash,
                block_number,
                reason,
                acknowledged_unresolved_burn_tx_hash,
            } => self.handle_force_complete_burn(
                issuer_request_id,
                burn_tx_hash,
                block_number,
                reason,
                acknowledged_unresolved_burn_tx_hash,
            ),
            RedemptionCommand::Reprocess { issuer_request_id, metadata } => {
                self.handle_reprocess(issuer_request_id, metadata)
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
            } => self.handle_resume_burn(ResumeBurnInput {
                issuer_request_id,
                metadata,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                external_tx_id,
            }),
            RedemptionCommand::IntendBurn {
                issuer_request_id,
                vault,
                burns,
                dust_shares,
                owner,
                external_tx_id,
            } => {
                self.handle_intend_burn(
                    services,
                    issuer_request_id,
                    BurnInput {
                        vault,
                        burns,
                        dust_shares,
                        owner,
                        external_tx_id,
                    },
                )
                .await
            }
            RedemptionCommand::RecordBurnRecoveryAttempt {
                issuer_request_id,
                tx_hash,
                nonce,
                action,
            } => self.handle_record_burn_recovery_attempt(
                issuer_request_id,
                tx_hash,
                nonce,
                action,
            ),
            RedemptionCommand::RecordBurnPreparationRecoveryAttempt {
                issuer_request_id,
                attempt,
            } => self.handle_record_burn_preparation_recovery_attempt(
                issuer_request_id,
                attempt,
            ),
            RedemptionCommand::RecordBurnRecoveryExhausted {
                issuer_request_id,
                tx_hash,
                nonce,
                attempts,
            } => self.handle_record_burn_recovery_exhausted(
                issuer_request_id,
                tx_hash,
                nonce,
                attempts,
            ),
            RedemptionCommand::RecordBurnPreparationRecoveryExhausted {
                issuer_request_id,
                attempts,
            } => self.handle_record_burn_preparation_recovery_exhausted(
                issuer_request_id,
                attempts,
            ),
            RedemptionCommand::ReplaceDeadBurn { issuer_request_id, owner } => {
                self.handle_replace_dead_burn(
                    services,
                    issuer_request_id,
                    owner,
                )
                .await
            }
        }
    }
}

impl Redemption {
    fn apply_event(&mut self, event: RedemptionEvent) {
        match &event {
            RedemptionEvent::Detected { .. }
            | RedemptionEvent::Reprocessed { .. } => {
                self.apply_detection_event(event);
            }
            RedemptionEvent::AlpacaCalled {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
            } => self.apply_alpaca_called(
                issuer_request_id,
                tokenization_request_id.clone(),
                alpaca_quantity.clone(),
                dust_quantity.clone(),
                *called_at,
            ),
            RedemptionEvent::AlpacaCallFailed { .. }
            | RedemptionEvent::RedemptionFailed { .. }
            | RedemptionEvent::BurningFailed { .. } => {
                self.apply_failure_event(event);
            }
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id,
                alpaca_journal_completed_at,
            } => self.apply_alpaca_journal_completed(
                issuer_request_id,
                *alpaca_journal_completed_at,
            ),
            RedemptionEvent::BurnTxSubmitted { .. } => {
                self.apply_burn_submitted_event(event);
            }
            RedemptionEvent::BurnResumed { .. } => {
                self.apply_burn_resumed_event(event);
            }
            RedemptionEvent::TokensBurned(_)
            | RedemptionEvent::ExistingBurnRecovered { .. }
            | RedemptionEvent::RedemptionClosed { .. }
            | RedemptionEvent::BurnForceCompleted { .. } => {
                self.apply_terminal_event(event);
            }
            RedemptionEvent::BurnIntended { .. } => {
                self.apply_burn_intended_event(event);
            }
            RedemptionEvent::BurnRecoveryAttempted { .. }
            | RedemptionEvent::BurnPreparationRecoveryAttempted { .. }
            | RedemptionEvent::BurnRecoveryExhausted { .. }
            | RedemptionEvent::BurnPreparationRecoveryExhausted { .. } => {}
        }
    }

    fn apply_detection_event(&mut self, event: RedemptionEvent) {
        let (RedemptionEvent::Detected {
            issuer_request_id,
            underlying,
            token,
            network,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
            burn_mode,
        }
        | RedemptionEvent::Reprocessed {
            issuer_request_id,
            underlying,
            token,
            network,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
            burn_mode,
            ..
        }) = event
        else {
            return;
        };

        *self = Self::Detected {
            metadata: RedemptionMetadata {
                issuer_request_id,
                underlying,
                token,
                network,
                wallet,
                quantity,
                detected_tx_hash: tx_hash,
                block_number,
                detected_at,
                burn_mode,
            },
        };
    }

    fn apply_failure_event(&mut self, event: RedemptionEvent) {
        let unresolved_burn_tx = self
            .persisted_unresolved_burn_tx()
            .filter(|sendable_tx| !sendable_tx.tx.is_empty())
            .cloned();
        let (issuer_request_id, reason, failed_at) = match event {
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id,
                error,
                failed_at,
            }
            | RedemptionEvent::BurningFailed {
                issuer_request_id,
                error,
                failed_at,
                ..
            } => (issuer_request_id, error, failed_at),
            RedemptionEvent::RedemptionFailed {
                issuer_request_id,
                reason,
                failed_at,
            } => (issuer_request_id, reason, failed_at),
            _ => return,
        };

        *self = Self::Failed {
            issuer_request_id,
            reason,
            failed_at,
            unresolved_burn_tx,
        };
    }

    fn apply_burn_submitted_event(&mut self, event: RedemptionEvent) {
        let RedemptionEvent::BurnTxSubmitted {
            external_tx_id,
            tx_id,
            planned_burns,
            ..
        } = event
        else {
            return;
        };

        let (Self::Burning {
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
            alpaca_journal_completed_at,
            ..
        }
        | Self::BurnIntended {
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
            alpaca_journal_completed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        let sendable_tx =
            self.current_sendable_tx().cloned().unwrap_or_default();

        *self = Self::BurnSubmitted {
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
            alpaca_journal_completed_at,
            external_tx_id,
            tx_id,
            planned_burns,
            sendable_tx,
        };
    }

    fn apply_burn_resumed_event(&mut self, event: RedemptionEvent) {
        let prior_burn_tx = match self {
            Self::Failed { unresolved_burn_tx, .. } => {
                unresolved_burn_tx.clone()
            }
            _ => None,
        };
        let RedemptionEvent::BurnResumed {
            issuer_request_id,
            underlying,
            token,
            network,
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
            burn_mode,
            ..
        } = event
        else {
            return;
        };

        *self = Self::Burning {
            metadata: RedemptionMetadata {
                issuer_request_id,
                underlying,
                token,
                network,
                wallet,
                quantity,
                detected_tx_hash: tx_hash,
                block_number,
                detected_at,
                burn_mode,
            },
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
            alpaca_journal_completed_at,
            external_tx_id,
            prior_burn_tx,
        };
    }

    fn apply_terminal_event(&mut self, event: RedemptionEvent) {
        match event {
            RedemptionEvent::TokensBurned(TokensBurnedData {
                issuer_request_id,
                tx_hash,
                burned_at,
                ..
            }) => {
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
                ..
            } => {
                // The close event records at most an acknowledged hash, and
                // pre-acknowledgement closures recorded nothing — the
                // pre-close state is the only reliable carrier of a signed
                // burn that may still land, so retain it through the closure.
                let unresolved_burn_tx = self
                    .persisted_unresolved_burn_tx()
                    .filter(|sendable_tx| !sendable_tx.tx.is_empty())
                    .cloned();
                *self = Self::Closed {
                    issuer_request_id,
                    reason,
                    closed_at,
                    unresolved_burn_tx,
                };
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
            _ => {}
        }
    }

    fn apply_burn_intended_event(&mut self, event: RedemptionEvent) {
        let RedemptionEvent::BurnIntended {
            sendable_tx, planned_burns, ..
        } = event
        else {
            return;
        };

        let transition = match self.clone() {
            Self::Burning {
                metadata,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                external_tx_id,
                ..
            }
            | Self::BurnIntended {
                metadata,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                external_tx_id,
                ..
            } => Some((
                metadata,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                external_tx_id,
            )),
            Self::BurnSubmitted {
                metadata,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                external_tx_id,
                ..
            } => Some((
                metadata,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                Some(external_tx_id),
            )),
            _ => None,
        };
        let Some((
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
            alpaca_journal_completed_at,
            external_tx_id,
        )) = transition
        else {
            return;
        };

        *self = Self::BurnIntended {
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
            alpaca_journal_completed_at,
            planned_burns,
            external_tx_id,
            sendable_tx,
        };
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{
        Address, B256, Bytes, TxHash, U256, address, b256, uint,
    };
    use chrono::Utc;
    use cqrs_es::{AggregateError, DomainEvent};
    use event_sorcery::{LifecycleError, StoreBuilder, TestHarness, replay};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{
        BurnExternalTxId, BurnRecord, BurnRecoveryAction,
        IssuerRedemptionRequestId, Redemption, RedemptionCommand,
        RedemptionError, RedemptionEvent, RedemptionMetadata,
        RedemptionServices, TokensBurnedData, has_unresolved_signer_intent,
        next_burn_retry_external_tx_id_from_history,
    };
    use crate::config::VaultMode;
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::prepare_event_sourced_startup;
    use crate::redemption::BurnFailureClassification;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        BurnTxStatus, MultiBurnEntry, SendableTxWithHash, TxId, VaultService,
    };

    fn mock_services() -> RedemptionServices {
        RedemptionServices::with_single_vault(
            Network::Base,
            Arc::new(MockVaultService::new_success()),
        )
    }

    #[tokio::test]
    async fn recovery_annotations_keep_the_wallet_intent_gate_closed() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations should run");
        let aggregate_id = IssuerRedemptionRequestId::random().to_string();

        for (sequence, event_type, payload) in [
            (
                1,
                "RedemptionEvent::Detected",
                r#"{"Detected":{"network":"base"}}"#,
            ),
            (2, "RedemptionEvent::BurnIntended", "{}"),
            (3, "RedemptionEvent::BurnRecoveryAttempted", "{}"),
            (4, "RedemptionEvent::BurnRecoveryExhausted", "{}"),
        ] {
            sqlx::query(
                "
                INSERT INTO events (
                    aggregate_type,
                    aggregate_id,
                    sequence,
                    event_type,
                    event_version,
                    payload,
                    metadata
                )
                VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(&aggregate_id)
            .bind(sequence)
            .bind(event_type)
            .bind(payload)
            .execute(&pool)
            .await
            .expect("test event should insert");
        }

        assert!(
            has_unresolved_signer_intent(&pool, Network::Base, None)
                .await
                .expect("intent query should succeed"),
            "recovery bookkeeping must keep the same network's gate closed"
        );
        assert!(
            !has_unresolved_signer_intent(&pool, Network::Ethereum, None)
                .await
                .expect("intent query should succeed"),
            "an intent on Base must not block an independent Ethereum signer"
        );

        let orphaned = sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'Redemption',
                'orphaned-intent',
                1,
                'RedemptionEvent::BurnIntended',
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .execute(&pool)
        .await;

        let orphaned_error = orphaned
            .expect_err(
                "an intent with no origin metadata must be rejected atomically",
            )
            .to_string();
        assert!(
            orphaned_error.contains("requires one Detected event"),
            "the validation trigger must name the missing origin, got: \
             {orphaned_error}"
        );
    }

    async fn insert_redemption_event(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
            ",
        )
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// A failed burn submission may still have broadcast the signed
    /// transaction, so BurningFailed must keep the network's reservation —
    /// mirroring the mint side, where MintingFailed never releases. The same
    /// holds for RedemptionFailed, Reprocessed (operator requeue without a
    /// terminal on-chain outcome), the post-reprocess Alpaca* events that
    /// follow once the aggregate returns to Detected, and BurnResumed, which
    /// re-enters Burning before the replacement intent re-reserves. Only a
    /// real terminal outcome (TokensBurned or RedemptionClosed) frees the
    /// nonce domain.
    #[tokio::test]
    async fn failed_and_resumed_burns_keep_the_signer_reservation() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations should run");
        let aggregate_id = IssuerRedemptionRequestId::random().to_string();

        for (sequence, event_type, payload) in [
            (
                1,
                "RedemptionEvent::Detected",
                r#"{"Detected":{"network":"base"}}"#,
            ),
            (2, "RedemptionEvent::BurnIntended", "{}"),
            // The ambiguous recover_single_burn_failed path emits
            // BurningFailed then RedemptionFailed; neither may release.
            (3, "RedemptionEvent::BurningFailed", "{}"),
            (4, "RedemptionEvent::RedemptionFailed", "{}"),
            // Reprocessed requeues without a terminal on-chain outcome;
            // apply_reprocessed returns the aggregate to Detected, so the
            // next event is AlpacaCalled (or sibling Alpaca*).
            (5, "RedemptionEvent::Reprocessed", "{}"),
            (6, "RedemptionEvent::AlpacaCalled", "{}"),
            (7, "RedemptionEvent::BurnResumed", "{}"),
        ] {
            insert_redemption_event(
                &pool,
                &aggregate_id,
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("test event should insert");
        }

        assert!(
            has_unresolved_signer_intent(&pool, Network::Base, None)
                .await
                .expect("intent query should succeed"),
            "a failed/reprocessed/AlpacaCalled/resumed burn may have \
             broadcast its tx and must keep the gate closed"
        );

        insert_redemption_event(
            &pool,
            &aggregate_id,
            8,
            "RedemptionEvent::RedemptionClosed",
            "{}",
        )
        .await
        .expect("the close event should insert");

        assert!(
            !has_unresolved_signer_intent(&pool, Network::Base, None)
                .await
                .expect("intent query should succeed"),
            "closing the redemption must release the signer reservation"
        );
    }

    /// The burn reserve trigger must reject a competing same-network intent
    /// with the explicit reservation message — mirroring the mint-side
    /// regression test, so a burn-trigger-only regression back to the
    /// implicit PK violation (misreported upstream as a same-aggregate
    /// conflict) cannot slip through.
    #[tokio::test]
    async fn competing_burn_intent_raises_the_explicit_reservation_error() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations should run");

        for (aggregate_id, sequence, event_type, payload) in [
            (
                "burn-first",
                1,
                "RedemptionEvent::Detected",
                r#"{"Detected":{"network":"base"}}"#,
            ),
            ("burn-first", 2, "RedemptionEvent::BurnIntended", "{}"),
            (
                "burn-second",
                1,
                "RedemptionEvent::Detected",
                r#"{"Detected":{"network":"base"}}"#,
            ),
        ] {
            insert_redemption_event(
                &pool,
                aggregate_id,
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("test history should insert");
        }

        let competing_error = insert_redemption_event(
            &pool,
            "burn-second",
            2,
            "RedemptionEvent::BurnIntended",
            "{}",
        )
        .await
        .expect_err("a competing same-network burn intent must be rejected")
        .to_string();
        assert!(
            competing_error.contains("signer network already reserved"),
            "the burn-side rejection must carry the explicit reservation \
             message, got: {competing_error}"
        );
    }

    /// The migration backfill must reconstruct a reservation for an
    /// unresolved historical burn, defaulting pre-network Detected events to
    /// Base for wire compatibility.
    #[tokio::test]
    async fn signer_intent_migration_backfills_unresolved_burns() {
        const INIT: &str =
            include_str!("../../migrations/20251016210348_init.sql");
        const GUARD: &str = include_str!(
            "../../migrations/20260801095000_enforce_active_signer_intents.sql"
        );

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::raw_sql(INIT).execute(&pool).await.expect("init schema");

        for (sequence, event_type, payload) in [
            (1, "RedemptionEvent::Detected", r#"{"Detected":{}}"#),
            (2, "RedemptionEvent::BurnIntended", "{}"),
            (3, "RedemptionEvent::BurningFailed", "{}"),
        ] {
            insert_redemption_event(
                &pool,
                "legacy-burn",
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("historical event should insert");
        }

        sqlx::raw_sql(GUARD).execute(&pool).await.expect("backfill");

        let active: (String, String, String) = sqlx::query_as(
            "SELECT network, aggregate_type, aggregate_id \
             FROM active_signer_intents",
        )
        .fetch_one(&pool)
        .await
        .expect("the unresolved burn must be backfilled");
        assert_eq!(
            active,
            (
                "base".to_string(),
                "Redemption".to_string(),
                "legacy-burn".to_string(),
            )
        );
    }

    /// The core double-signing hazard the table exists to prevent: TWO
    /// historical aggregates left unresolved burns on the same network. The
    /// backfill must abort on the PRIMARY KEY rather than pick a winner —
    /// remediation is resolving one aggregate, never guessing.
    #[tokio::test]
    async fn signer_intent_migration_aborts_on_conflicting_unresolved_burns() {
        const INIT: &str =
            include_str!("../../migrations/20251016210348_init.sql");
        const GUARD: &str = include_str!(
            "../../migrations/20260801095000_enforce_active_signer_intents.sql"
        );

        let conflicted = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::raw_sql(INIT).execute(&conflicted).await.expect("init schema");
        for aggregate_id in ["first-unresolved", "second-unresolved"] {
            insert_redemption_event(
                &conflicted,
                aggregate_id,
                1,
                "RedemptionEvent::Detected",
                r#"{"Detected":{"network":"base"}}"#,
            )
            .await
            .expect("historical Detected should insert");
            insert_redemption_event(
                &conflicted,
                aggregate_id,
                2,
                "RedemptionEvent::BurnIntended",
                "{}",
            )
            .await
            .expect("historical BurnIntended should insert");
        }
        let conflicted_error = sqlx::raw_sql(GUARD)
            .execute(&conflicted)
            .await
            .expect_err(
                "migration must abort on two unresolved burns sharing a \
                 network instead of silently choosing one",
            )
            .to_string();
        assert!(
            conflicted_error.contains("UNIQUE constraint failed"),
            "the historical conflict must abort on the network PRIMARY KEY \
             specifically, got: {conflicted_error}"
        );
    }

    #[test]
    fn test_next_burn_retry_external_tx_id_advances_from_submission() {
        let detected_tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let events = [RedemptionEvent::BurnTxSubmitted {
            issuer_request_id: IssuerRedemptionRequestId::new(detected_tx_hash),
            external_tx_id: BurnExternalTxId::base(&detected_tx_hash),
            tx_id: TxId::random(),
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
            RedemptionEvent::BurnTxSubmitted {
                issuer_request_id: IssuerRedemptionRequestId::new(
                    detected_tx_hash,
                ),
                external_tx_id: BurnExternalTxId::base(&detected_tx_hash),
                tx_id: TxId::random(),
                planned_burns: vec![],
                submitted_at: Utc::now(),
            },
            RedemptionEvent::BurnResumed {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: IssuerRedemptionRequestId::new(
                    detected_tx_hash,
                ),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
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

    #[tokio::test]
    async fn test_detect_redemption_creates_event() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        let events = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::Detect {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: Network::Base,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
            })
            .await
            .events();

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
            burn_mode: event_burn_mode,
            ..
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
        assert_eq!(event_burn_mode, &VaultMode::VaultDirect);
    }

    #[tokio::test]
    async fn test_detect_redemption_when_already_detected_returns_error() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("TSLA").unwrap();
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: Network::Base,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::Detect {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                network: Network::Base,
                wallet,
                quantity,
                tx_hash,
                block_number,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::AlreadyDetected { issuer_request_id }
        );
    }

    #[test]
    fn test_apply_detected_event_updates_state() {
        assert!(replay::<Redemption>(vec![]).unwrap().is_none());

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("NVDA").unwrap();
        let token = TokenSymbol::new("tNVDA");
        let wallet = address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc");
        let quantity = Quantity::new(Decimal::from(25));
        let tx_hash = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        let block_number = 99999;
        let detected_at = Utc::now();

        let redemption =
            replay::<Redemption>(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: Network::Base,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            }])
            .unwrap()
            .unwrap();

        assert_eq!(
            redemption,
            Redemption::Detected {
                metadata: RedemptionMetadata {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id,
                    underlying,
                    token,
                    network: Network::Base,
                    wallet,
                    quantity,
                    detected_tx_hash: tx_hash,
                    block_number,
                    detected_at,
                }
            }
        );
    }

    fn orchestrator_mode() -> VaultMode {
        VaultMode::Orchestrator {
            address: address!("0x00000000000000000000000000000000000000aa"),
        }
    }

    #[tokio::test]
    async fn detect_with_orchestrator_mode_anchors_it_on_the_event() {
        let events = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::Detect {
                issuer_request_id: IssuerRedemptionRequestId::random(),
                underlying: UnderlyingSymbol::new("RKLB").unwrap(),
                token: TokenSymbol::new("tRKLB"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(10)),
                tx_hash: B256::random(),
                block_number: 1,
                burn_mode: orchestrator_mode(),
                network: Network::Base,
            })
            .await
            .events();

        assert!(
            matches!(
                events.as_slice(),
                [RedemptionEvent::Detected { burn_mode, .. }]
                    if *burn_mode == orchestrator_mode()
            ),
            "Detected must carry the orchestrator anchor, got {events:?}"
        );
    }

    /// The mode anchor must survive replay through the whole pre-burn
    /// lifecycle: an orchestrator-detected redemption stays orchestrator in
    /// `Burning` state regardless of what the asset's config says later.
    #[test]
    fn burn_mode_anchor_survives_replay_to_burning() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let redemption = replay::<Redemption>(vec![
            RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("RKLB").unwrap(),
                token: TokenSymbol::new("tRKLB"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(10)),
                tx_hash: B256::random(),
                block_number: 1,
                detected_at: Utc::now(),
                burn_mode: orchestrator_mode(),
                network: Network::Base,
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-1"),
                alpaca_quantity: Quantity::new(Decimal::from(10)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at: Utc::now(),
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id,
                alpaca_journal_completed_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        let Redemption::Burning { metadata, .. } = redemption else {
            panic!("Expected Burning state, got {redemption:?}");
        };
        assert_eq!(metadata.burn_mode, orchestrator_mode());
    }

    /// `Reprocessed` and `BurnResumed` flatten metadata into the event; both
    /// must preserve the orchestrator anchor so a recovered redemption never
    /// silently falls back to vault-direct.
    #[test]
    fn burn_mode_anchor_survives_reprocess_and_resume_replay() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let detected_tx_hash = B256::random();
        let detected = RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: UnderlyingSymbol::new("RKLB").unwrap(),
            token: TokenSymbol::new("tRKLB"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(10)),
            tx_hash: detected_tx_hash,
            block_number: 1,
            detected_at: Utc::now(),
            burn_mode: orchestrator_mode(),
            network: Network::Base,
        };
        let failed = RedemptionEvent::RedemptionFailed {
            issuer_request_id: issuer_request_id.clone(),
            reason: "alpaca timeout".to_string(),
            failed_at: Utc::now(),
        };

        let reprocessed = replay::<Redemption>(vec![
            detected.clone(),
            failed.clone(),
            RedemptionEvent::Reprocessed {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("RKLB").unwrap(),
                token: TokenSymbol::new("tRKLB"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(10)),
                tx_hash: detected_tx_hash,
                block_number: 1,
                detected_at: Utc::now(),
                previous_state: "Failed".to_string(),
                reprocessed_at: Utc::now(),
                burn_mode: orchestrator_mode(),
                network: Network::Base,
            },
        ])
        .unwrap()
        .unwrap();
        let Redemption::Detected { metadata } = reprocessed else {
            panic!("Expected Detected state, got {reprocessed:?}");
        };
        assert_eq!(metadata.burn_mode, orchestrator_mode());

        let resumed = replay::<Redemption>(vec![
            detected,
            failed,
            RedemptionEvent::BurnResumed {
                issuer_request_id,
                underlying: UnderlyingSymbol::new("RKLB").unwrap(),
                token: TokenSymbol::new("tRKLB"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(10)),
                tx_hash: detected_tx_hash,
                block_number: 1,
                detected_at: Utc::now(),
                tokenization_request_id: TokenizationRequestId::new("tok-1"),
                alpaca_quantity: Quantity::new(Decimal::from(10)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at: Utc::now(),
                alpaca_journal_completed_at: Utc::now(),
                external_tx_id: None,
                resumed_at: Utc::now(),
                burn_mode: orchestrator_mode(),
                network: Network::Base,
            },
        ])
        .unwrap()
        .unwrap();
        let Redemption::Burning { metadata, .. } = resumed else {
            panic!("Expected Burning state, got {resumed:?}");
        };
        assert_eq!(metadata.burn_mode, orchestrator_mode());
    }

    /// `Reprocess` and `ResumeBurn` handlers copy the caller-supplied
    /// metadata's anchor onto the emitted event, so the persisted history
    /// keeps the orchestrator mode across admin recovery.
    #[tokio::test]
    async fn reprocess_command_preserves_orchestrator_burn_mode() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let detected_tx_hash = B256::random();
        let metadata = RedemptionMetadata {
            issuer_request_id: issuer_request_id.clone(),
            underlying: UnderlyingSymbol::new("RKLB").unwrap(),
            token: TokenSymbol::new("tRKLB"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(10)),
            detected_tx_hash,
            block_number: 1,
            detected_at: Utc::now(),
            burn_mode: orchestrator_mode(),
            network: Network::Base,
        };

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("RKLB").unwrap(),
                    token: TokenSymbol::new("tRKLB"),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                    quantity: Quantity::new(Decimal::from(10)),
                    tx_hash: detected_tx_hash,
                    block_number: 1,
                    detected_at: Utc::now(),
                    burn_mode: orchestrator_mode(),
                    network: Network::Base,
                },
                RedemptionEvent::RedemptionFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: "alpaca timeout".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::Reprocess { issuer_request_id, metadata })
            .await
            .events();

        assert!(
            matches!(
                events.as_slice(),
                [RedemptionEvent::Reprocessed { burn_mode, .. }]
                    if *burn_mode == orchestrator_mode()
            ),
            "Reprocessed must carry the orchestrator anchor, got {events:?}"
        );
    }

    #[tokio::test]
    async fn test_record_alpaca_call_from_detected_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-tok-456");

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
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
            })
            .await
            .events();

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

    #[tokio::test]
    async fn test_record_alpaca_call_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-tok-789");

        let error = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordAlpacaCall {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity: Quantity::new(Decimal::from(100)),
                dust_quantity: Quantity::new(Decimal::ZERO),
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: "Uninitialized".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn test_record_alpaca_failure_from_detected_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let error = "API timeout".to_string();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("TSLA").unwrap(),
                token: TokenSymbol::new("tTSLA"),
                network: Network::Base,
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
            })
            .await
            .events();

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

    #[tokio::test]
    async fn test_record_alpaca_failure_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordAlpacaFailure {
                issuer_request_id,
                error: "Some error".to_string(),
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Detected".to_string(),
                found: "Uninitialized".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn test_confirm_alpaca_complete_from_alpaca_called_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-complete-456");

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
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
            })
            .await
            .events();

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

    #[tokio::test]
    async fn test_confirm_alpaca_complete_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::ConfirmAlpacaComplete {
                issuer_request_id,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "AlpacaCalled".to_string(),
                found: "Uninitialized".to_string(),
            }
        );
    }

    #[test]
    fn test_apply_alpaca_journal_completed_transitions_to_burning() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burning-456");
        let underlying = UnderlyingSymbol::new("TSLA").unwrap();
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

        let alpaca_quantity = Quantity::new(Decimal::from(50));
        let dust_quantity = Quantity::new(Decimal::ZERO);

        let redemption = replay::<Redemption>(vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: Network::Base,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                alpaca_quantity: alpaca_quantity.clone(),
                dust_quantity: dust_quantity.clone(),
                called_at,
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            redemption,
            Redemption::Burning {
                metadata: RedemptionMetadata {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id,
                    underlying,
                    token,
                    network: Network::Base,
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
                prior_burn_tx: None,
            }
        );
    }

    #[tokio::test]
    async fn test_confirm_alpaca_complete_emits_one_event() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-one-event-456");

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
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
            })
            .await
            .events();

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

    #[tokio::test]
    async fn test_intend_burn_from_burning_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let receipt_id = uint!(42_U256);
        let burn_shares = uint!(100_000000000000000000_U256);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let owner = address!("0x1111111111111111111111111111111111111111");

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
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
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-intend-456",
                    ),
                    alpaca_quantity: Quantity::new(Decimal::from(100)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at: Utc::now(),
                },
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
            ])
            .when(RedemptionCommand::IntendBurn {
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
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurnIntended {
            issuer_request_id: event_id,
            planned_burns,
            ..
        } = &events[0]
        else {
            panic!("Expected BurnIntended event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(planned_burns.len(), 1);
        assert_eq!(planned_burns[0].receipt_id, receipt_id);
        assert_eq!(planned_burns[0].shares_burned, burn_shares);
    }

    #[tokio::test]
    async fn test_intend_burn_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("NVDA").unwrap(),
                token: TokenSymbol::new("tNVDA"),
                network: Network::Base,
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
            .when(RedemptionCommand::IntendBurn {
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
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Burning".to_string(),
                found: "Detected".to_string(),
            }
        );
    }

    fn intended_burn_history(
        issuer_request_id: &IssuerRedemptionRequestId,
        sendable_tx: SendableTxWithHash,
    ) -> Vec<RedemptionEvent> {
        vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
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
                tokenization_request_id: TokenizationRequestId::new(
                    "alp-replace-456",
                ),
                alpaca_quantity: Quantity::new(Decimal::from(100)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at: Utc::now(),
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at: Utc::now(),
            },
            RedemptionEvent::BurnIntended {
                issuer_request_id: issuer_request_id.clone(),
                sendable_tx,
                planned_burns: vec![BurnRecord {
                    receipt_id: uint!(42_U256),
                    shares_burned: uint!(100_000000000000000000_U256),
                }],
            },
        ]
    }

    fn replace_dead_burn_command(
        issuer_request_id: &IssuerRedemptionRequestId,
        owner: Address,
    ) -> RedemptionCommand {
        RedemptionCommand::ReplaceDeadBurn {
            issuer_request_id: issuer_request_id.clone(),
            owner,
        }
    }

    #[tokio::test]
    async fn replace_dead_burn_refuses_a_still_mineable_hash() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let old_tx = SendableTxWithHash {
            hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            nonce: 7,
            ..SendableTxWithHash::default()
        };
        let services: Arc<dyn VaultService> = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::StillMineable),
        );

        let error = TestHarness::<Redemption>::with(
            RedemptionServices::with_single_vault(Network::Base, services),
        )
        .given(intended_burn_history(&issuer_request_id, old_tx))
        .when(replace_dead_burn_command(
            &issuer_request_id,
            address!("0x1111111111111111111111111111111111111111"),
        ))
        .await
        .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::BurnReplacementNotSafe { .. }
            )
        ));
    }

    #[tokio::test]
    async fn replace_dead_burn_persists_a_fresh_hash() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let destination =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let old_tx = SendableTxWithHash::valid_for_test(
            7,
            destination,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let replacement_tx = SendableTxWithHash::valid_for_test(
            8,
            destination,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let replacement_hash = replacement_tx.hash;
        let owner = old_tx.signer_for_test();
        let services: Arc<dyn VaultService> = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(replacement_tx),
        );

        let events = TestHarness::<Redemption>::with(
            RedemptionServices::with_single_vault(Network::Base, services),
        )
        .given(intended_burn_history(&issuer_request_id, old_tx))
        .when(replace_dead_burn_command(&issuer_request_id, owner))
        .await
        .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::BurnIntended { sendable_tx, .. }]
                if sendable_tx.hash == replacement_hash
                    && sendable_tx.nonce == 8
        ));
    }

    #[tokio::test]
    async fn replace_dead_burn_refuses_a_non_fresh_nonce() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let destination =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let old_tx = SendableTxWithHash::valid_for_test(
            7,
            destination,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let non_fresh_tx = SendableTxWithHash::valid_for_test(
            7,
            destination,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = old_tx.signer_for_test();
        let services: Arc<dyn VaultService> = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(non_fresh_tx),
        );

        let error = TestHarness::<Redemption>::with(
            RedemptionServices::with_single_vault(Network::Base, services),
        )
        .given(intended_burn_history(&issuer_request_id, old_tx))
        .when(replace_dead_burn_command(&issuer_request_id, owner))
        .await
        .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::BurnReplacementNotFresh { .. }
            )
        ));
    }

    #[tokio::test]
    async fn replace_dead_burn_refuses_changed_calldata() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let destination =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let old_tx = SendableTxWithHash::valid_for_test(
            7,
            destination,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let changed_call = SendableTxWithHash::valid_for_test(
            8,
            destination,
            Bytes::from_static(&[0xbe, 0xef]),
        );
        let owner = old_tx.signer_for_test();
        let services: Arc<dyn VaultService> = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(changed_call),
        );

        let error = TestHarness::<Redemption>::with(
            RedemptionServices::with_single_vault(Network::Base, services),
        )
        .given(intended_burn_history(&issuer_request_id, old_tx))
        .when(replace_dead_burn_command(&issuer_request_id, owner))
        .await
        .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::BurnReplacementValidationFailed { .. }
            )
        ));
    }

    #[tokio::test]
    async fn dead_submitted_burn_replays_to_the_fresh_intent() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let destination =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let old_tx = SendableTxWithHash::valid_for_test(
            7,
            destination,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let replacement_tx = SendableTxWithHash::valid_for_test(
            8,
            destination,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = old_tx.signer_for_test();
        let mut history =
            intended_burn_history(&issuer_request_id, old_tx.clone());
        history.push(RedemptionEvent::BurnTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: BurnExternalTxId::base(&B256::random()),
            tx_id: old_tx.hash.into(),
            planned_burns: vec![BurnRecord {
                receipt_id: uint!(42_U256),
                shares_burned: uint!(100_000000000000000000_U256),
            }],
            submitted_at: Utc::now(),
        });
        let services: Arc<dyn VaultService> = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(replacement_tx.clone()),
        );

        let events = TestHarness::<Redemption>::with(
            RedemptionServices::with_single_vault(Network::Base, services),
        )
        .given(history.clone())
        .when(replace_dead_burn_command(&issuer_request_id, owner))
        .await
        .events();
        let replacement_event = events
            .first()
            .expect("replacement should append one event")
            .clone();
        history.push(replacement_event);
        let aggregate = replay::<Redemption>(history)
            .expect("replacement history should replay")
            .expect("redemption should exist");

        assert!(matches!(
            aggregate,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == replacement_tx
        ));
    }

    #[test]
    fn recovery_annotation_events_do_not_change_burn_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let sendable_tx = SendableTxWithHash {
            hash: B256::random(),
            nonce: 7,
            ..SendableTxWithHash::default()
        };
        let mut events =
            intended_burn_history(&issuer_request_id, sendable_tx.clone());
        events.push(RedemptionEvent::BurnRecoveryAttempted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: sendable_tx.hash,
            nonce: sendable_tx.nonce,
            action: BurnRecoveryAction::Rebroadcast,
            attempted_at: Utc::now(),
        });
        events.push(RedemptionEvent::BurnRecoveryExhausted {
            issuer_request_id,
            tx_hash: sendable_tx.hash,
            nonce: sendable_tx.nonce,
            attempts: 5,
            exhausted_at: Utc::now(),
        });

        let aggregate = replay::<Redemption>(events)
            .expect("history should replay")
            .expect("aggregate should exist");

        assert!(matches!(aggregate, Redemption::BurnIntended { .. }));
    }

    #[test]
    fn test_apply_burn_intended_event_transitions_to_burn_intended_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-intended-456");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;
        let detected_at = Utc::now();
        let alpaca_quantity = Quantity::new(Decimal::from(100));
        let dust_quantity = Quantity::new(Decimal::ZERO);
        let called_at = Utc::now();
        let alpaca_journal_completed_at = Utc::now();
        let receipt_id = uint!(7_U256);
        let burn_shares = uint!(100_000000000000000000_U256);
        let planned_burns =
            vec![BurnRecord { receipt_id, shares_burned: burn_shares }];

        let redemption = replay::<Redemption>(vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: Network::Base,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                alpaca_quantity: alpaca_quantity.clone(),
                dust_quantity: dust_quantity.clone(),
                called_at,
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at,
            },
            RedemptionEvent::BurnIntended {
                issuer_request_id: issuer_request_id.clone(),
                sendable_tx: SendableTxWithHash::default(),
                planned_burns: planned_burns.clone(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            redemption,
            Redemption::BurnIntended {
                metadata: RedemptionMetadata {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id,
                    underlying,
                    token,
                    network: Network::Base,
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
                planned_burns,
                external_tx_id: None,
                sendable_tx: SendableTxWithHash::default(),
            }
        );
    }

    #[tokio::test]
    async fn test_burn_tokens_from_burning_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let receipt_id = uint!(42_U256);
        let burn_shares = uint!(100_000000000000000000_U256);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let owner = address!("0x1111111111111111111111111111111111111111");
        let user_wallet =
            address!("0x9876543210fedcba9876543210fedcba98765432");

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("TSLA").unwrap(),
                    token: TokenSymbol::new("tTSLA"),
                    network: Network::Base,
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
                RedemptionEvent::BurnIntended {
                    issuer_request_id: issuer_request_id.clone(),
                    sendable_tx: SendableTxWithHash::default(),
                    planned_burns: vec![BurnRecord {
                        receipt_id,
                        shares_burned: burn_shares,
                    }],
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
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let RedemptionEvent::BurnTxSubmitted {
            issuer_request_id: event_id,
            planned_burns,
            ..
        } = &events[0]
        else {
            panic!("Expected BurnTxSubmitted event, got {:?}", &events[0]);
        };

        assert_eq!(event_id, &issuer_request_id);
        assert_eq!(planned_burns.len(), 1);
        assert_eq!(planned_burns[0].receipt_id, receipt_id);
        assert_eq!(planned_burns[0].shares_burned, burn_shares);
    }

    #[tokio::test]
    async fn test_burn_tokens_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("NVDA").unwrap(),
                token: TokenSymbol::new("tNVDA"),
                network: Network::Base,
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
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "BurnIntended or BurnSubmitted".to_string(),
                found: "Detected".to_string(),
            }
        );
    }

    fn burning_given_events_on_network(
        issuer_request_id: &IssuerRedemptionRequestId,
        network: Network,
    ) -> Vec<RedemptionEvent> {
        let mut events = burning_given_events(issuer_request_id);
        let RedemptionEvent::Detected { network: event_network, .. } =
            &mut events[0]
        else {
            panic!("burning_given_events must start with Detected");
        };
        *event_network = network;
        events
    }

    fn burn_submitted_given_events_on_network(
        issuer_request_id: &IssuerRedemptionRequestId,
        network: Network,
    ) -> Vec<RedemptionEvent> {
        let mut events =
            burning_given_events_on_network(issuer_request_id, network);
        events.push(RedemptionEvent::BurnTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: BurnExternalTxId::base(&b256!(
                "0x4444444444444444444444444444444444444444444444444444444444444444"
            )),
            tx_id: TxId::Legacy("fb-799".to_string()),
            planned_burns: vec![],
            submitted_at: Utc::now(),
        });
        events
    }

    /// The first network-touching command out of `Burning` is `IntendBurn`;
    /// on an unconfigured network it must fail closed before any signing.
    #[tokio::test]
    async fn test_intend_burn_network_not_configured() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burning_given_events_on_network(
                &issuer_request_id,
                Network::Ethereum,
            ))
            .when(RedemptionCommand::IntendBurn {
                issuer_request_id,
                vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                burns: vec![MultiBurnEntry {
                    receipt_id: uint!(1_U256),
                    burn_shares: uint!(17_000000000000000000_U256),
                    receipt_info: None,
                    receipt_info_bytes: None,
                }],
                dust_shares: U256::ZERO,
                owner: address!("0x1111111111111111111111111111111111111111"),
                external_tx_id: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::NetworkNotConfigured {
                network: Network::Ethereum,
            }
        );
    }

    #[tokio::test]
    async fn test_confirm_burn_network_not_configured() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_submitted_given_events_on_network(
                &issuer_request_id,
                Network::Ethereum,
            ))
            .when(RedemptionCommand::ConfirmBurn {
                issuer_request_id,
                tx_id: TxId::Legacy("fb-799".to_string()),
                dust_shares: U256::ZERO,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::NetworkNotConfigured {
                network: Network::Ethereum,
            }
        );
    }

    #[tokio::test]
    async fn test_record_burn_failure_from_burning_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let error = "Insufficient gas".to_string();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("GOOG").unwrap(),
                    token: TokenSymbol::new("tGOOG"),
                    network: Network::Base,
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
                classification: BurnFailureClassification::Unclassified,
                issuer_request_id: issuer_request_id.clone(),
                error: error.clone(),
                tx_id: None,
                planned_burns: vec![],
            })
            .await
            .events();

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

    #[tokio::test]
    async fn test_record_burn_failure_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::RecordBurnFailure {
                classification: BurnFailureClassification::Unclassified,
                issuer_request_id,
                error: "Some error".to_string(),
                tx_id: None,
                planned_burns: vec![],
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Burning, BurnIntended, or BurnSubmitted".to_string(),
                found: "Uninitialized".to_string(),
            }
        );
    }

    /// Events that drive a redemption into the `Burning` state, for tests that
    /// exercise admin terminalization paths (close / force-complete).
    fn burning_given_events(
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Vec<RedemptionEvent> {
        vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("ARKK").unwrap(),
                token: TokenSymbol::new("tARKK"),
                network: Network::Base,
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
        events.push(RedemptionEvent::BurnTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: BurnExternalTxId::base(&b256!(
                "0x4444444444444444444444444444444444444444444444444444444444444444"
            )),
            tx_id: TxId::random(),
            planned_burns: vec![],
            submitted_at: Utc::now(),
        });
        events
    }

    fn burn_intended_given_events(
        issuer_request_id: &IssuerRedemptionRequestId,
        tx_hash: B256,
    ) -> Vec<RedemptionEvent> {
        let mut events = burning_given_events(issuer_request_id);
        events.push(RedemptionEvent::BurnIntended {
            issuer_request_id: issuer_request_id.clone(),
            sendable_tx: SendableTxWithHash {
                tx: vec![1, 2, 3],
                hash: tx_hash,
                nonce: 7,
                signed_at: Utc::now(),
                dust_shares: U256::ZERO,
            },
            planned_burns: vec![],
        });
        events
    }

    #[test]
    fn burn_intended_preserves_alpaca_quantity_for_admin_context() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let redemption = replay::<Redemption>(burn_intended_given_events(
            &issuer_request_id,
            B256::random(),
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            redemption.alpaca_quantity(),
            Some(&Quantity::new(Decimal::from(17)))
        );
    }

    #[tokio::test]
    async fn test_confirm_burn_from_burn_intended_with_matching_hash() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(&issuer_request_id, tx_hash))
            .when(RedemptionCommand::ConfirmBurn {
                issuer_request_id,
                tx_id: TxId::Hash(tx_hash),
                dust_shares: U256::ZERO,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::TokensBurned(_)]
        ));
    }

    #[tokio::test]
    async fn test_confirm_burn_from_burn_intended_rejects_mismatched_hash() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let stored_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let provided_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(&issuer_request_id, stored_hash))
            .when(RedemptionCommand::ConfirmBurn {
                issuer_request_id,
                tx_id: TxId::Hash(provided_hash),
                dust_shares: U256::ZERO,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::TxIdMismatch {
                expected: TxId::Hash(stored_hash),
                provided: TxId::Hash(provided_hash),
            }
        );
    }

    #[tokio::test]
    async fn test_record_burn_failure_from_burn_intended_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(&issuer_request_id, tx_hash))
            .when(RedemptionCommand::RecordBurnFailure {
                classification: BurnFailureClassification::Unclassified,
                issuer_request_id,
                error: "receipt reverted".to_string(),
                tx_id: Some(TxId::Hash(tx_hash)),
                planned_burns: vec![],
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::BurningFailed { .. }]
        ));
    }

    #[tokio::test]
    async fn test_force_complete_burn_from_burn_intended_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(&issuer_request_id, burn_tx_hash))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash,
                block_number: 45_989_009,
                reason: "persisted burn confirmed on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::BurnForceCompleted { .. }]
        ));
    }

    #[tokio::test]
    async fn test_close_redemption_from_burning_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let reason = "QQQM share accounting unverified".to_string();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(burning_given_events(&issuer_request_id))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

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

    #[tokio::test]
    async fn close_redemption_rejects_acknowledgement_without_persisted_burn() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let acknowledged_hash = B256::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burning_given_events(&issuer_request_id))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "no signed burn exists".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(acknowledged_hash),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::UnexpectedUnresolvedBurnAcknowledgement {
                    provided,
                }
            ) if provided == acknowledged_hash
        ));
    }

    #[tokio::test]
    async fn test_close_redemption_from_failed_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let mut given = burning_given_events(&issuer_request_id);
        given.push(RedemptionEvent::BurningFailed {
            classification: BurnFailureClassification::Unclassified,
            issuer_request_id: issuer_request_id.clone(),
            error: "burn reverted".to_string(),
            failed_at: Utc::now(),
            tx_id: None,
            planned_burns: vec![],
        });

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(given)
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "unrecoverable".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], RedemptionEvent::RedemptionClosed { .. }));
    }

    #[tokio::test]
    async fn test_close_redemption_from_burn_submitted_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let reason = "submitted burn unverifiable on-chain".to_string();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(burn_submitted_given_events(&issuer_request_id))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

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

    #[tokio::test]
    async fn close_redemption_rejects_unacknowledged_persisted_burn() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = B256::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(&issuer_request_id, burn_tx_hash))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "operator reconciled externally".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                burn_tx_hash,
            }
        );
    }

    #[tokio::test]
    async fn close_redemption_rejects_wrong_burn_acknowledgement() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = B256::random();
        let acknowledged_hash = B256::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(&issuer_request_id, burn_tx_hash))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "operator reconciled externally".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(acknowledged_hash),
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::UnresolvedBurnAcknowledgementMismatch {
                expected: burn_tx_hash,
                provided: acknowledged_hash,
            }
        );
    }

    #[tokio::test]
    async fn close_redemption_records_matching_burn_acknowledgement() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = B256::random();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(&issuer_request_id, burn_tx_hash))
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "operator reconciled externally".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(burn_tx_hash),
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::RedemptionClosed {
                acknowledged_unresolved_burn_tx_hash: Some(acknowledged_hash),
                ..
            }] if *acknowledged_hash == burn_tx_hash
        ));
    }

    #[tokio::test]
    async fn close_failed_redemption_requires_persisted_burn_acknowledgement() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = B256::random();
        let mut history =
            burn_intended_given_events(&issuer_request_id, burn_tx_hash);
        history.push(RedemptionEvent::BurningFailed {
            classification: BurnFailureClassification::Unclassified,
            issuer_request_id: issuer_request_id.clone(),
            error: "confirmation was ambiguous".to_string(),
            failed_at: Utc::now(),
            tx_id: Some(TxId::Hash(burn_tx_hash)),
            planned_burns: vec![],
        });

        let missing = TestHarness::<Redemption>::with(mock_services())
            .given(history.clone())
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator reconciled externally".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();
        assert!(matches!(
            missing,
            LifecycleError::Apply(
                RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                    burn_tx_hash: unresolved_hash,
                }
            ) if unresolved_hash == burn_tx_hash
        ));

        let wrong_hash = B256::random();
        let mismatch = TestHarness::<Redemption>::with(mock_services())
            .given(history.clone())
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator reconciled externally".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(wrong_hash),
            })
            .await
            .then_expect_error();
        assert!(matches!(
            mismatch,
            LifecycleError::Apply(
                RedemptionError::UnresolvedBurnAcknowledgementMismatch {
                    expected,
                    provided,
                }
            ) if expected == burn_tx_hash && provided == wrong_hash
        ));

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "operator reconciled externally".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(burn_tx_hash),
            })
            .await
            .events();
        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::RedemptionClosed {
                acknowledged_unresolved_burn_tx_hash: Some(acknowledged_hash),
                ..
            }] if *acknowledged_hash == burn_tx_hash
        ));
    }

    #[tokio::test]
    async fn test_close_redemption_from_exhausted_burn_intended_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let sendable_tx = SendableTxWithHash::default();
        let mut history =
            intended_burn_history(&issuer_request_id, sendable_tx.clone());
        history.push(RedemptionEvent::BurnRecoveryExhausted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: sendable_tx.hash,
            nonce: sendable_tx.nonce,
            attempts: 5,
            exhausted_at: Utc::now(),
        });

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::CloseRedemption {
                issuer_request_id,
                reason: "operator reconciled exhausted burn".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::RedemptionClosed { .. }]
        ));
    }

    #[tokio::test]
    async fn test_close_redemption_from_detected_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("ARKK").unwrap(),
                token: TokenSymbol::new("tARKK"),
                network: Network::Base,
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
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Failed, Burning, BurnIntended, or BurnSubmitted"
                    .to_string(),
                found: "Detected".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn test_force_complete_burn_without_persisted_hash_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burning_given_events(&issuer_request_id))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: B256::random(),
                block_number: 45_989_009,
                reason: "another redemption's verified burn".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::PersistedBurnHashUnavailable
            )
        ));
    }

    #[tokio::test]
    async fn test_force_complete_legacy_submitted_burn_without_hash_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_submitted_given_events(&issuer_request_id))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: B256::random(),
                block_number: 45_989_009,
                reason: "another redemption's verified burn".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::PersistedBurnHashUnavailable
            )
        ));
    }

    /// The legacy custodian-era shape: the burn was submitted through the
    /// custodian's API (backend `tx_id`, no locally signed transaction), it
    /// landed on-chain, and a later recovery pass found the share balance
    /// already consumed and marked the redemption `Failed`. Nothing was ever
    /// persisted to bind a hash against, so the admin layer's on-chain
    /// verification of the planned burns is the proof — the aggregate must
    /// accept the verified terminalization instead of stranding the
    /// redemption in `Failed` forever.
    #[tokio::test]
    async fn force_complete_terminalizes_a_legacy_failed_burn() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let mut history = burning_given_events(&issuer_request_id);
        history.push(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "Fireblocks transaction polling timed out".to_string(),
            failed_at: Utc::now(),
            tx_id: Some(TxId::Legacy("fb-1417".to_string())),
            planned_burns: vec![BurnRecord {
                receipt_id: uint!(3_U256),
                shares_burned: uint!(40_000000000000000_U256),
            }],
            classification: BurnFailureClassification::Unclassified,
        });
        history.push(RedemptionEvent::RedemptionFailed {
            issuer_request_id: issuer_request_id.clone(),
            reason: "On-chain balance insufficient for BurnFailed recovery: \
                     balance=0, required=40000000000000000"
                .to_string(),
            failed_at: Utc::now(),
        });

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: b256!(
                    "0x5555555555555555555555555555555555555555555555555555555555555555"
                ),
                block_number: 33_000_000,
                reason: "operator verified the landed burn on-chain"
                    .to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::BurnForceCompleted { .. }]
        ));
    }

    /// The legacy shape above, wedged one step further: an operator closed
    /// the `Failed` redemption through the admin API (which does not settle
    /// the burn reservation) before the force-complete path learned the
    /// legacy shape. The verified landed burn is the same; a `Closed`
    /// aggregate must accept the terminalization so the reservation can
    /// settle instead of stranding the vault's custody migration forever.
    #[tokio::test]
    async fn force_complete_terminalizes_a_closed_legacy_burn() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let mut history = burning_given_events(&issuer_request_id);
        history.push(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "Fireblocks API error: error in reqwest-middleware: \
                    error sending request"
                .to_string(),
            failed_at: Utc::now(),
            tx_id: None,
            planned_burns: vec![BurnRecord {
                receipt_id: uint!(3_U256),
                shares_burned: uint!(40_000000000000000_U256),
            }],
            classification: BurnFailureClassification::Unclassified,
        });
        history.push(RedemptionEvent::RedemptionFailed {
            issuer_request_id: issuer_request_id.clone(),
            reason: "On-chain balance insufficient for BurnFailed recovery: \
                     balance=0, required=40000000000000000"
                .to_string(),
            failed_at: Utc::now(),
        });
        history.push(RedemptionEvent::RedemptionClosed {
            issuer_request_id: issuer_request_id.clone(),
            reason: "Burn verified on-chain; closed by admin because \
                     force-complete rejected the Failed state"
                .to_string(),
            closed_at: Utc::now(),
            acknowledged_unresolved_burn_tx_hash: None,
        });

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: b256!(
                    "0xfda15f8e5fb2b87e83bf115ea41c521bb251cc3ae875ac91f3e38f003c9a09ee"
                ),
                block_number: 48_929_042,
                reason: "operator verified the landed burn on-chain"
                    .to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::BurnForceCompleted { .. }]
        ));
    }

    /// The legacy shape has no persisted transaction, so there is nothing an
    /// acknowledgement could refer to — supplying one anyway must be refused
    /// rather than recorded as a meaningless fact on the terminal event.
    #[tokio::test]
    async fn force_complete_of_legacy_failed_burn_refuses_redundant_acknowledgement()
     {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let mut history = burning_given_events(&issuer_request_id);
        history.push(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "Fireblocks transaction polling timed out".to_string(),
            failed_at: Utc::now(),
            tx_id: Some(TxId::Legacy("fb-1417".to_string())),
            planned_burns: vec![BurnRecord {
                receipt_id: uint!(3_U256),
                shares_burned: uint!(40_000000000000000_U256),
            }],
            classification: BurnFailureClassification::Unclassified,
        });
        history.push(RedemptionEvent::RedemptionFailed {
            issuer_request_id: issuer_request_id.clone(),
            reason: "On-chain balance insufficient for BurnFailed recovery: \
                     balance=0, required=40000000000000000"
                .to_string(),
            failed_at: Utc::now(),
        });
        let provided = B256::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: b256!(
                    "0x5555555555555555555555555555555555555555555555555555555555555555"
                ),
                block_number: 33_000_000,
                reason: "operator verified the landed burn on-chain"
                    .to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(provided),
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::RedundantUnresolvedBurnAcknowledgement {
                provided,
            }
        );
    }

    /// A `Failed` redemption still carrying a persisted signed burn keeps the
    /// acknowledgement guard: force-completing against a different proving
    /// transaction must name the persisted hash the operator is stranding.
    #[tokio::test]
    async fn force_complete_from_failed_with_unresolved_burn_requires_acknowledgement()
     {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_hash = B256::random();
        let mut history =
            burn_intended_given_events(&issuer_request_id, persisted_hash);
        history.push(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "confirmation timed out".to_string(),
            failed_at: Utc::now(),
            tx_id: None,
            planned_burns: vec![],
            classification: BurnFailureClassification::Unclassified,
        });

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: B256::random(),
                block_number: 33_000_000,
                reason: "different burn verified on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                burn_tx_hash: persisted_hash,
            }
        );
    }

    /// The pre-#260 closure shape: `CloseRedemption` accepted persisted-tx
    /// states without recording any acknowledgement, so historical
    /// `RedemptionClosed` events replay with the acknowledgement field
    /// defaulted to `None` while a still-mineable signed burn survives in
    /// the pre-close history. The guard must key on that retained
    /// transaction, not on the recorded acknowledgement.
    #[tokio::test]
    async fn force_complete_of_pre_acknowledgement_closure_requires_acknowledgement()
     {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_hash = B256::random();
        let mut history =
            burn_intended_given_events(&issuer_request_id, persisted_hash);
        history.push(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "confirmation timed out".to_string(),
            failed_at: Utc::now(),
            tx_id: None,
            planned_burns: vec![],
            classification: BurnFailureClassification::Unclassified,
        });
        history.push(RedemptionEvent::RedemptionClosed {
            issuer_request_id: issuer_request_id.clone(),
            reason: "closed by a pre-acknowledgement admin build".to_string(),
            closed_at: Utc::now(),
            acknowledged_unresolved_burn_tx_hash: None,
        });

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: B256::random(),
                block_number: 33_000_000,
                reason: "different burn verified on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                burn_tx_hash: persisted_hash,
            }
        );
    }

    /// A redemption closed while still carrying an acknowledged, potentially
    /// still-mineable signed burn keeps the acknowledgement guard through the
    /// closure: force-completing against a different proving transaction must
    /// re-name the hash the operator is stranding, or the original
    /// transaction could land later and double-burn.
    #[tokio::test]
    async fn force_complete_of_closed_redemption_with_unresolved_burn_requires_acknowledgement()
     {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_hash = B256::random();
        let mut history =
            burn_intended_given_events(&issuer_request_id, persisted_hash);
        history.push(RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "confirmation timed out".to_string(),
            failed_at: Utc::now(),
            tx_id: None,
            planned_burns: vec![],
            classification: BurnFailureClassification::Unclassified,
        });
        history.push(RedemptionEvent::RedemptionClosed {
            issuer_request_id: issuer_request_id.clone(),
            reason: "closed by admin with the unresolved burn acknowledged"
                .to_string(),
            closed_at: Utc::now(),
            acknowledged_unresolved_burn_tx_hash: Some(persisted_hash),
        });

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: B256::random(),
                block_number: 33_000_000,
                reason: "different burn verified on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                burn_tx_hash: persisted_hash,
            }
        );
    }

    #[tokio::test]
    async fn test_force_complete_burn_from_exhausted_burn_intended_state() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let sendable_tx = SendableTxWithHash::valid_for_test(
            7,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xde, 0xad]),
        );
        let mut history =
            intended_burn_history(&issuer_request_id, sendable_tx.clone());
        history.push(RedemptionEvent::BurnRecoveryExhausted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: sendable_tx.hash,
            nonce: sendable_tx.nonce,
            attempts: 5,
            exhausted_at: Utc::now(),
        });

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(history)
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: sendable_tx.hash,
                block_number: 45_989_009,
                reason: "verified exhausted burn".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::BurnForceCompleted { .. }]
        ));
    }

    #[tokio::test]
    async fn force_complete_rejects_different_unacknowledged_burn() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_hash = B256::random();
        let proving_hash = B256::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(
                &issuer_request_id,
                persisted_hash,
            ))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: proving_hash,
                block_number: 45_989_009,
                reason: "different burn verified on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                burn_tx_hash: persisted_hash,
            }
        );
    }

    #[tokio::test]
    async fn force_complete_records_acknowledged_different_burn() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_hash = B256::random();
        let proving_hash = B256::random();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(
                &issuer_request_id,
                persisted_hash,
            ))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: proving_hash,
                block_number: 45_989_009,
                reason: "different burn verified on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(persisted_hash),
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [RedemptionEvent::BurnForceCompleted {
                burn_tx_hash,
                acknowledged_unresolved_burn_tx_hash: Some(acknowledged_hash),
                ..
            }] if *burn_tx_hash == proving_hash
                && *acknowledged_hash == persisted_hash
        ));
    }

    #[tokio::test]
    async fn force_complete_rejects_acknowledgement_for_persisted_burn() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_hash = B256::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(
                &issuer_request_id,
                persisted_hash,
            ))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: persisted_hash,
                block_number: 45_989_009,
                reason: "persisted burn verified on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(persisted_hash),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::RedundantUnresolvedBurnAcknowledgement {
                    provided,
                }
            ) if provided == persisted_hash
        ));
    }

    #[tokio::test]
    async fn force_complete_rejects_wrong_burn_acknowledgement() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_hash = B256::random();
        let proving_hash = B256::random();
        let acknowledged_hash = B256::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(burn_intended_given_events(
                &issuer_request_id,
                persisted_hash,
            ))
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: proving_hash,
                block_number: 45_989_009,
                reason: "different burn verified on-chain".to_string(),
                acknowledged_unresolved_burn_tx_hash: Some(acknowledged_hash),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(
                RedemptionError::UnresolvedBurnAcknowledgementMismatch {
                    expected,
                    provided,
                }
            ) if expected == persisted_hash && provided == acknowledged_hash
        ));
    }

    #[test]
    fn test_force_complete_burn_transitions_to_completed() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
        );

        let mut events = burning_given_events(&issuer_request_id);
        events.push(RedemptionEvent::BurnForceCompleted {
            issuer_request_id,
            burn_tx_hash,
            block_number: 45_989_009,
            reason: "verified".to_string(),
            acknowledged_unresolved_burn_tx_hash: None,
            completed_at: Utc::now(),
        });

        let redemption = replay::<Redemption>(events).unwrap().unwrap();

        let Redemption::Completed { burn_tx_hash: stored, .. } = redemption
        else {
            panic!("Expected Completed state, got {}", redemption.state_name());
        };

        assert_eq!(stored, burn_tx_hash);
    }

    #[tokio::test]
    async fn test_force_complete_burn_from_wrong_state_fails() {
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::ForceCompleteBurn {
                issuer_request_id,
                burn_tx_hash: b256!(
                    "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
                ),
                block_number: 45_989_009,
                reason: "nope".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Burning, BurnIntended, BurnSubmitted, Failed, or \
                           Closed"
                    .to_string(),
                found: "Uninitialized".to_string(),
            }
        );
    }

    #[test]
    fn test_apply_tokens_burned_transitions_to_completed() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-complete-456");
        let underlying = UnderlyingSymbol::new("AMZN").unwrap();
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

        let redemption = replay::<Redemption>(vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                network: Network::Base,
                wallet,
                quantity,
                tx_hash: detected_tx_hash,
                block_number,
                detected_at,
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                alpaca_quantity: Quantity::new(Decimal::from(75)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at,
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at,
            },
            RedemptionEvent::TokensBurned(TokensBurnedData {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: burn_tx_hash,
                burns: vec![BurnRecord { receipt_id, shares_burned }],
                dust_returned: U256::ZERO,
                gas_used: 60000,
                block_number: 51000,
                burned_at,
            }),
        ])
        .unwrap()
        .unwrap();

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
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-failed-456");
        let underlying = UnderlyingSymbol::new("NFLX").unwrap();
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

        let redemption = replay::<Redemption>(vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                network: Network::Base,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                alpaca_quantity: Quantity::new(Decimal::from(150)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at,
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at,
            },
            RedemptionEvent::BurningFailed {
                classification: BurnFailureClassification::Unclassified,
                issuer_request_id: issuer_request_id.clone(),
                error: error.clone(),
                failed_at,
                tx_id: None,
                planned_burns: vec![],
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            redemption,
            Redemption::Failed {
                issuer_request_id,
                reason: error,
                failed_at,
                unresolved_burn_tx: None,
            }
        );
    }

    #[tokio::test]
    async fn test_mark_failed_from_failed_state_succeeds() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    network: Network::Base,
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
            })
            .await
            .events();

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
            burn_mode: VaultMode::VaultDirect,
            issuer_request_id: IssuerRedemptionRequestId::random(),
            underlying: UnderlyingSymbol::new("RKLB").unwrap(),
            token: TokenSymbol::new("tRKLB"),
            network: Network::Base,
            wallet: address!("0x9876543210fedcba9876543210fedcba98765432"),
            quantity: Quantity::new(Decimal::from(100)),
            detected_tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_reprocess_from_failed_state_succeeds() {
        let metadata = test_metadata();
        let expected_underlying = metadata.underlying.clone();
        let expected_id = metadata.issuer_request_id.clone();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: expected_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    network: metadata.network,
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
            })
            .await
            .events();

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

    #[tokio::test]
    async fn test_reprocess_from_detected_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                network: metadata.network,
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
                detected_at: metadata.detected_at,
            }])
            .when(RedemptionCommand::Reprocess { issuer_request_id, metadata })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Detected".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn test_reprocess_from_burning_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    network: metadata.network,
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
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Burning".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn test_reprocess_from_completed_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    network: metadata.network,
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
                RedemptionEvent::TokensBurned(TokensBurnedData {
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
                }),
            ])
            .when(RedemptionCommand::Reprocess {
                issuer_request_id: issuer_request_id.clone(),
                metadata,
            })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::AlreadyCompleted { issuer_request_id }
        );
    }

    #[tokio::test]
    async fn test_reprocess_from_uninitialized_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given_no_previous_events()
            .when(RedemptionCommand::Reprocess { issuer_request_id, metadata })
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Uninitialized".to_string(),
            }
        );
    }

    #[test]
    fn test_apply_reprocessed_transitions_to_detected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        let mut redemption = replay::<Redemption>(vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                network: metadata.network,
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
                detected_at: metadata.detected_at,
            },
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "Alpaca bug".to_string(),
                failed_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert!(matches!(redemption, Redemption::Failed { .. }));

        redemption.apply_event(RedemptionEvent::Reprocessed {
            burn_mode: VaultMode::VaultDirect,
            issuer_request_id: issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            network: metadata.network,
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
                    network: metadata.network,
                    wallet: metadata.wallet,
                    quantity: metadata.quantity,
                    detected_tx_hash: metadata.detected_tx_hash,
                    block_number: metadata.block_number,
                    detected_at: metadata.detected_at,
                    burn_mode: VaultMode::VaultDirect,
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

    #[tokio::test]
    async fn test_resume_burn_from_failed_state_succeeds() {
        let metadata = test_metadata();

        let events = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    network: metadata.network,
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
            .when(test_resume_burn_command(&metadata))
            .await
            .events();

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

    #[tokio::test]
    async fn test_resume_burn_from_completed_state_rejected() {
        let metadata = test_metadata();
        let issuer_request_id = metadata.issuer_request_id.clone();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![
                RedemptionEvent::Detected {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: metadata.underlying.clone(),
                    token: metadata.token.clone(),
                    network: metadata.network,
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
                RedemptionEvent::TokensBurned(TokensBurnedData {
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
                }),
            ])
            .when(test_resume_burn_command(&metadata))
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::AlreadyCompleted { issuer_request_id }
        );
    }

    #[tokio::test]
    async fn test_resume_burn_from_detected_state_rejected() {
        let metadata = test_metadata();

        let error = TestHarness::<Redemption>::with(mock_services())
            .given(vec![RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: metadata.issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                network: metadata.network,
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
                detected_at: metadata.detected_at,
            }])
            .when(test_resume_burn_command(&metadata))
            .await
            .then_expect_error();

        let LifecycleError::Apply(error) = error else {
            panic!("Expected Apply error, got {error:?}");
        };
        assert_eq!(
            error,
            RedemptionError::InvalidState {
                expected: "Failed".to_string(),
                found: "Detected".to_string(),
            }
        );
    }

    #[test]
    fn test_apply_burn_resumed_transitions_to_burning() {
        let metadata = test_metadata();
        let journal_completed_at = Utc::now();

        let mut redemption = replay::<Redemption>(vec![
            RedemptionEvent::Detected {
                burn_mode: VaultMode::VaultDirect,
                issuer_request_id: metadata.issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                network: metadata.network,
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
                detected_at: metadata.detected_at,
            },
            RedemptionEvent::RedemptionFailed {
                issuer_request_id: metadata.issuer_request_id.clone(),
                reason: "timed out".to_string(),
                failed_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert!(matches!(redemption, Redemption::Failed { .. }));

        redemption.apply_event(RedemptionEvent::BurnResumed {
            burn_mode: VaultMode::VaultDirect,
            issuer_request_id: metadata.issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            network: metadata.network,
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

    #[test]
    fn prior_burn_transaction_survives_failure_resume_and_snapshot_roundtrip() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let mut history =
            intended_burn_history(&issuer_request_id, persisted_tx.clone());
        history.push(RedemptionEvent::BurningFailed {
            classification: BurnFailureClassification::Unclassified,
            issuer_request_id: issuer_request_id.clone(),
            error: "replacement preparation failed".to_string(),
            failed_at: Utc::now(),
            tx_id: None,
            planned_burns: vec![],
        });
        let failed = replay::<Redemption>(history.clone())
            .expect("failed history should replay")
            .expect("redemption should exist");
        assert!(matches!(
            &failed,
            Redemption::Failed {
                unresolved_burn_tx: Some(prior_burn_tx),
                ..
            } if prior_burn_tx == &persisted_tx
        ));

        history.push(RedemptionEvent::BurnResumed {
            burn_mode: VaultMode::VaultDirect,
            issuer_request_id,
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(100)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
            tokenization_request_id: TokenizationRequestId::new(
                "alp-replace-456",
            ),
            alpaca_quantity: Quantity::new(Decimal::from(100)),
            dust_quantity: Quantity::new(Decimal::ZERO),
            called_at: Utc::now(),
            alpaca_journal_completed_at: Utc::now(),
            external_tx_id: None,
            resumed_at: Utc::now(),
        });
        let burning = replay::<Redemption>(history)
            .expect("resumed history should replay")
            .expect("redemption should exist");
        let snapshot = serde_json::to_string(&burning)
            .expect("burning snapshot should serialize");
        let roundtripped: Redemption = serde_json::from_str(&snapshot)
            .expect("burning snapshot should deserialize");
        assert!(matches!(
            &roundtripped,
            Redemption::Burning {
                prior_burn_tx: Some(prior_burn_tx),
                ..
            } if prior_burn_tx == &persisted_tx
        ));

        let mut old_failed_snapshot =
            serde_json::to_value(failed).expect("snapshot should serialize");
        old_failed_snapshot
            .pointer_mut("/Failed")
            .and_then(serde_json::Value::as_object_mut)
            .expect("failed snapshot should exist")
            .remove("unresolved_burn_tx");
        let restored_failed: Redemption =
            serde_json::from_value(old_failed_snapshot)
                .expect("old failed snapshot should deserialize");
        assert!(matches!(
            restored_failed,
            Redemption::Failed { unresolved_burn_tx: None, .. }
        ));

        let mut old_burning_snapshot =
            serde_json::to_value(burning).expect("snapshot should serialize");
        old_burning_snapshot
            .pointer_mut("/Burning")
            .and_then(serde_json::Value::as_object_mut)
            .expect("burning snapshot should exist")
            .remove("prior_burn_tx");
        let restored_burning: Redemption =
            serde_json::from_value(old_burning_snapshot)
                .expect("old burning snapshot should deserialize");
        assert!(matches!(
            restored_burning,
            Redemption::Burning { prior_burn_tx: None, .. }
        ));
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

    #[traced_test]
    #[tokio::test]
    async fn redemption_v3_burn_submitted_snapshot_replays_the_persisted_transaction()
     {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!().run(&pool).await.expect("migrations should run");

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let mut history =
            intended_burn_history(&issuer_request_id, persisted_tx.clone());
        history.push(RedemptionEvent::BurnTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: BurnExternalTxId::base(&B256::random()),
            tx_id: persisted_tx.hash.into(),
            planned_burns: vec![BurnRecord {
                receipt_id: uint!(42_U256),
                shares_burned: uint!(100_000000000000000000_U256),
            }],
            submitted_at: Utc::now(),
        });

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'SchemaRegistry',
                'schema',
                1,
                'SchemaRegistryEvent::VersionUpdated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(
            serde_json::json!({
                "VersionUpdated": { "name": "Redemption", "version": 3 }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .expect("v3 schema registry event should seed");

        for (index, event) in history.iter().enumerate() {
            let sequence = i64::try_from(index + 1)
                .expect("test event sequence should fit i64");
            sqlx::query(
                "
                INSERT INTO events (
                    aggregate_type,
                    aggregate_id,
                    sequence,
                    event_type,
                    event_version,
                    payload,
                    metadata
                )
                VALUES ('Redemption', ?, ?, ?, ?, ?, '{}')
                ",
            )
            .bind(issuer_request_id.to_string())
            .bind(sequence)
            .bind(event.event_type())
            .bind(event.event_version())
            .bind(
                serde_json::to_string(event)
                    .expect("redemption event should serialize"),
            )
            .execute(&pool)
            .await
            .expect("redemption history should seed");
        }

        let last_sequence = i64::try_from(history.len())
            .expect("test event count should fit i64");
        let aggregate = replay::<Redemption>(history)
            .expect("history should replay")
            .expect("history should originate a redemption");
        let mut stale_snapshot = serde_json::json!({ "Live": aggregate });
        stale_snapshot
            .pointer_mut("/Live/BurnSubmitted")
            .and_then(serde_json::Value::as_object_mut)
            .expect("snapshot should contain BurnSubmitted")
            .remove("sendable_tx");
        sqlx::query(
            "
            INSERT INTO snapshots (
                aggregate_type,
                aggregate_id,
                last_sequence,
                snapshot_version,
                payload,
                timestamp
            )
            VALUES (
                'Redemption',
                ?,
                ?,
                3,
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ",
        )
        .bind(issuer_request_id.to_string())
        .bind(last_sequence)
        .bind(stale_snapshot.to_string())
        .execute(&pool)
        .await
        .expect("v3 snapshot should seed");

        prepare_event_sourced_startup::<Redemption>(&pool)
            .await
            .expect("schema reconciliation should succeed");
        let store = StoreBuilder::<Redemption>::new(pool.clone())
            .build(mock_services())
            .await
            .expect("redemption store should rebuild");
        let replayed = store
            .load(&issuer_request_id)
            .await
            .expect("redemption should load")
            .expect("redemption should exist");

        assert!(matches!(
            replayed,
            Redemption::BurnSubmitted { sendable_tx, .. }
                if sendable_tx == persisted_tx
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Cleared stale snapshots", "Redemption"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn redemption_v4_failed_snapshot_cannot_drop_unresolved_burn_guard() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let mut history =
            intended_burn_history(&issuer_request_id, persisted_tx.clone());
        history.push(RedemptionEvent::BurningFailed {
            classification: BurnFailureClassification::Unclassified,
            issuer_request_id: issuer_request_id.clone(),
            error: "ambiguous confirmation".to_string(),
            failed_at: Utc::now(),
            tx_id: Some(TxId::Hash(persisted_tx.hash)),
            planned_burns: vec![],
        });

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'SchemaRegistry',
                'schema',
                1,
                'SchemaRegistryEvent::VersionUpdated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(
            serde_json::json!({
                "VersionUpdated": { "name": "Redemption", "version": 4 }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();
        for (index, event) in history.iter().enumerate() {
            sqlx::query(
                "
                INSERT INTO events (
                    aggregate_type,
                    aggregate_id,
                    sequence,
                    event_type,
                    event_version,
                    payload,
                    metadata
                )
                VALUES ('Redemption', ?, ?, ?, ?, ?, '{}')
                ",
            )
            .bind(issuer_request_id.to_string())
            .bind(i64::try_from(index + 1).unwrap())
            .bind(event.event_type())
            .bind(event.event_version())
            .bind(serde_json::to_string(event).unwrap())
            .execute(&pool)
            .await
            .unwrap();
        }
        let aggregate = replay::<Redemption>(history.clone()).unwrap().unwrap();
        let mut stale_snapshot = serde_json::json!({ "Live": aggregate });
        stale_snapshot
            .pointer_mut("/Live/Failed")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("unresolved_burn_tx");
        sqlx::query(
            "
            INSERT INTO snapshots (
                aggregate_type,
                aggregate_id,
                last_sequence,
                snapshot_version,
                payload,
                timestamp
            )
            VALUES (
                'Redemption',
                ?,
                ?,
                4,
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ",
        )
        .bind(issuer_request_id.to_string())
        .bind(i64::try_from(history.len()).unwrap())
        .bind(stale_snapshot.to_string())
        .execute(&pool)
        .await
        .unwrap();

        prepare_event_sourced_startup::<Redemption>(&pool).await.unwrap();
        let store = StoreBuilder::<Redemption>::new(pool)
            .build(mock_services())
            .await
            .unwrap();
        let error = store
            .send(
                &issuer_request_id,
                RedemptionCommand::CloseRedemption {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: "operator close".to_string(),
                    acknowledged_unresolved_burn_tx_hash: None,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AggregateError::UserError(LifecycleError::Apply(
                RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                    burn_tx_hash,
                }
            )) if burn_tx_hash == persisted_tx.hash
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Cleared stale snapshots", "Redemption"]
        ));
    }

    /// Regression: pre-event-sorcery snapshot payloads must be cleared before
    /// `StoreBuilder::build` projection catch-up.
    #[tokio::test]
    async fn pre_lifecycle_snapshot_cleared_before_store_build() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );
        let redemption_id = IssuerRedemptionRequestId::new(tx_hash);
        let redemption_id_str = redemption_id.to_string();
        let now = Utc::now();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'SchemaRegistry',
                'schema',
                1,
                'SchemaRegistryEvent::VersionUpdated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(
            serde_json::json!({
                "VersionUpdated": { "name": "Redemption", "version": 1 }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'Redemption',
                ?,
                1,
                'RedemptionEvent::Detected',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(redemption_id_str.as_str())
        .bind(
            serde_json::json!({
                "Detected": {
                    "issuer_request_id": redemption_id_str,
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "quantity": "1.0",
                    "tx_hash": redemption_id_str,
                    "block_number": 1,
                    "detected_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO snapshots (
                aggregate_type,
                aggregate_id,
                last_sequence,
                snapshot_version,
                payload,
                timestamp
            )
            VALUES (
                'Redemption',
                ?,
                1,
                0,
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ",
        )
        .bind(redemption_id_str.as_str())
        .bind(
            serde_json::json!({
                "Completed": {
                    "issuer_request_id": redemption_id_str,
                    "burn_tx_hash": redemption_id_str,
                    "completed_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        prepare_event_sourced_startup::<Redemption>(&pool).await.unwrap();
        StoreBuilder::<Redemption>::new(pool.clone())
            .build(mock_services())
            .await
            .unwrap();

        let stale_snapshot_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM snapshots
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
            ",
        )
        .bind(redemption_id_str.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            stale_snapshot_count, 0,
            "Startup must clear incompatible Redemption snapshots"
        );
    }
}
