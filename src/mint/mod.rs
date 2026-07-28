pub(crate) mod api;
mod cmd;
mod event;
pub(crate) mod job;
pub(crate) mod recovery;
mod view;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use event_sorcery::{EventSourced, Table};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use uuid::Uuid;

use crate::config::{VaultMode, VaultModeKind};
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, TokenizedAssetViewFailure,
};
use crate::vault::{
    MintAuthorization, OrchestratorRevertReason, PreparedMintTx, TxId,
    UnconfiguredNetworkError, VaultError,
};

pub use api::MintResponse;

#[cfg(test)]
pub(crate) use api::test_utils;
pub(crate) use api::{authorize_mint, confirm_journal, initiate_mint};
pub(crate) use cmd::MintCommand;
pub(crate) use event::{MintEvent, MintFailureClassification};
pub(crate) use view::{MintView, find_all_recoverable_mints, find_stuck};

/// Returns whether any signed transaction on the same signer network — a
/// mint or a burn — is still awaiting its terminal outcome, excluding at
/// most this mint's own reservation.
///
/// Reads the trigger-maintained `active_signer_intents` table rather than
/// re-deriving the answer from event streams: the triggers update the table
/// in the same transaction that appends the intent event, so the table is
/// the single source of truth and cannot drift from the reserve/release
/// rules the migration encodes. Because the table is keyed by network, an
/// outstanding Redemption burn intent blocks a mint on the same nonce
/// domain too — which is exactly right, both flows sign with the same key.
pub(crate) async fn has_unresolved_signer_intent(
    pool: &Pool<Sqlite>,
    network: Network,
    excluding: Option<&IssuerMintRequestId>,
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
              AND NOT (aggregate_type = 'Mint' AND aggregate_id = ?)
        )
        ",
    )
    .bind(network.as_str())
    .bind(excluding)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

pub(crate) use crate::account::ClientId;
pub(crate) use crate::tokenized_asset::{
    Network, TokenSymbol, UnderlyingSymbol,
};
pub(crate) use crate::{Quantity, QuantityConversionError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TokenizationRequestId {
    #[cfg(test)]
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IssuerMintRequestId(Uuid);

impl IssuerMintRequestId {
    #[must_use]
    pub const fn new(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) fn random() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for IssuerMintRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for IssuerMintRequestId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct MintExternalTxId(String);

impl MintExternalTxId {
    pub(crate) const fn from_string(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for MintExternalTxId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Mint {
    Initiated {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
    },
    JournalConfirmed {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        journal_confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    Minting {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        journal_confirmed_at: DateTime<Utc>,
        minting_started_at: DateTime<Utc>,
        /// Failure history carried across a retry transition
        /// (`MintingFailed` -> `Minting` via `MintRetryStarted`); `None` on
        /// the first submission attempt. Preserving it keeps the automatic
        /// retry schedule escalating and the retry `external_tx_id` correct
        /// when the retry itself fails. `serde(default)` keeps pre-retry
        /// snapshots deserializable.
        #[serde(default)]
        retry: Option<MintRetryContext>,
    },
    /// Exact signed transaction persisted before broadcast.
    TxIntended {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        journal_confirmed_at: DateTime<Utc>,
        minting_started_at: DateTime<Utc>,
        prepared_tx: PreparedMintTx,
    },
    /// Transaction submitted to signing backend, awaiting on-chain confirmation.
    /// The `tx_id` enables recovery: on restart, the bot resumes
    /// polling this transaction instead of resubmitting (which would double-mint).
    TxSubmitted {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        journal_confirmed_at: DateTime<Utc>,
        minting_started_at: DateTime<Utc>,
        prepared_tx: Option<PreparedMintTx>,
        external_tx_id: String,
        tx_id: TxId,
    },
    CallbackPending {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        /// Vault-direct audit data: the ERC-1155 receipt id the deposit
        /// minted to the bot. `None` for orchestrator-mode mints — the
        /// orchestrator holds receipt custody, and `mint_nonce` is the
        /// analogous proof.
        receipt_id: Option<U256>,
        /// Orchestrator audit data: the authorization nonce the mint
        /// consumed. `None` for vault-direct mints.
        #[serde(default)]
        mint_nonce: Option<B256>,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        journal_confirmed_at: DateTime<Utc>,
        error: String,
        failed_at: DateTime<Utc>,
        /// Typed failure cause from `MintingFailed.classification`; typed
        /// classifications are never auto-retried.
        #[serde(default)]
        classification: MintFailureClassification,
        /// 1-indexed attempt number of the *next* retry, driving the automatic
        /// delay schedule and exhaustion cap. Unlike the `external_tx_id`-
        /// derived attempt (see `next_retry_attempt`), this advances on every
        /// failure — including submission failures that never reached onchain
        /// (no `TxSubmitted` predecessor) — so the schedule still
        /// escalates and eventually exhausts. The `external_tx_id` is derived
        /// separately and is reused unchanged across such failures to stay
        /// idempotent.
        ///
        /// Note: when a submission finally lands after pre-acceptance failures,
        /// the next failure re-seeds this from the new `TxSubmitted`
        /// predecessor's retry number, which can lower it (the delay schedule
        /// then runs longer than the nominal 1m/10m/30m/1h). This only affects
        /// schedule *duration*; the number of distinct on-chain mint
        /// transactions stays hard-capped at four because `external_tx_id`
        /// (and thus a new `TxSubmitted`) advances only on a successful
        /// submission. So there is no double-mint and no cap breach.
        attempts: u32,
        /// The state the mint was in before it failed. Used to determine
        /// whether receipt-triggered recovery is safe (only when failed
        /// from `Minting`, meaning a tx was actually submitted).
        failed_from: Box<Self>,
    },
    Completed {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// Mode anchor from `Initiated.mint_mode` — every mode-dependent mint
        /// step derives from this persisted value, never from live config.
        /// Snapshots persisted before orchestrator mode default to
        /// `VaultDirect`.
        #[serde(default)]
        mint_mode: VaultMode,
        /// The liquidity bot's validated `MintAuthV1`, absent until the
        /// internal mint-authorization call arrives (orchestrator mode only;
        /// always `None` for vault-direct mints). Orthogonal to `mint_mode` —
        /// an orchestrator mint whose authorization has not arrived yet is
        /// still an orchestrator mint.
        #[serde(default)]
        mint_authorization: Option<MintAuthorization>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        /// Vault-direct audit data: the ERC-1155 receipt id the deposit
        /// minted to the bot. `None` for orchestrator-mode mints — the
        /// orchestrator holds receipt custody, and `mint_nonce` is the
        /// analogous proof.
        receipt_id: Option<U256>,
        /// Orchestrator audit data: the authorization nonce the mint
        /// consumed. `None` for vault-direct mints.
        #[serde(default)]
        mint_nonce: Option<B256>,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
    Closed {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        /// Exact prepared deposit hash acknowledged at close, when any.
        acknowledged_unresolved_mint_tx_hash: Option<B256>,
        closed_at: DateTime<Utc>,
    },
}

struct ConfirmedMint {
    tx_id: TxId,
    tx_hash: B256,
    receipt_id: U256,
    shares_minted: U256,
    gas_used: u64,
    block_number: u64,
}

/// Audit payload `apply_tokens_minted` lands into `CallbackPending`, shared
/// by the vault-direct and orchestrator success events (which differ only in
/// receipt-id vs nonce audit data).
#[derive(Clone, Copy)]
struct MintedAudit {
    tx_hash: B256,
    receipt_id: Option<U256>,
    mint_nonce: Option<B256>,
    shares_minted: U256,
    gas_used: Option<u64>,
    block_number: u64,
    minted_at: DateTime<Utc>,
}

/// Orchestrator counterpart of [`ConfirmedMint`]: the `Minted` event carries
/// the consumed authorization `nonce` instead of a bot-held receipt id.
struct ConfirmedOrchestratorMint {
    tx_id: TxId,
    tx_hash: B256,
    nonce: B256,
    shares_minted: U256,
    gas_used: u64,
    block_number: u64,
}

/// Maps a decoded orchestrator mint revert to its typed classification.
/// Only the environment-wide logic-version halts classify from a revert;
/// `NonceConsumedByOtherMint` is assigned exclusively by the full-match
/// check after a `NonceReplayed` revert, and everything else stays
/// `Unclassified` (retryable).
const fn orchestrator_mint_failure_classification(
    error: &VaultError,
) -> MintFailureClassification {
    match error {
        // The reason match is exhaustive on purpose: a new revert reason
        // must not silently fall into the retryable bucket — adding one
        // forces an explicit classification decision here.
        VaultError::OrchestratorReverted { reason, .. } => match reason {
            OrchestratorRevertReason::VaultLogicMismatch => {
                MintFailureClassification::VaultLogicMismatch
            }
            OrchestratorRevertReason::ReceiptLogicMismatch => {
                MintFailureClassification::ReceiptLogicMismatch
            }
            OrchestratorRevertReason::BadRecipientSignature => {
                MintFailureClassification::BadRecipientSignature
            }
            OrchestratorRevertReason::RecipientCallbackRejected { .. } => {
                MintFailureClassification::RecipientCallbackRejected
            }
            OrchestratorRevertReason::VaultAmountMismatch { .. } => {
                MintFailureClassification::VaultAmountMismatch
            }
            // `NonceReplayed` is a recovery signal, not a failure verdict —
            // it only reaches here when no authorization exists to
            // full-match against, where retrying is the honest default.
            // `InsufficientReceipts` is a burn-path reason; on the mint path
            // it proves nothing, like an undecodable revert.
            OrchestratorRevertReason::NonceReplayed { .. }
            | OrchestratorRevertReason::InsufficientReceipts { .. }
            | OrchestratorRevertReason::Unknown => {
                MintFailureClassification::Unclassified
            }
        },
        _ => MintFailureClassification::Unclassified,
    }
}

/// Failure history preserved across a `MintRetryStarted` transition
/// (`MintingFailed` -> `Minting`). Without it a failed retry would restart
/// the automatic-retry schedule at attempt 1 and lose the
/// `TxSubmitted` predecessor that the retry `external_tx_id` is
/// derived from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MintRetryContext {
    attempts: u32,
    failed_from: Box<Mint>,
    /// Legacy receipt-triggered retries could carry an on-chain transaction
    /// hash even though replay moved the aggregate back to `Minting`. Preserve
    /// it so a later manual retry cannot mistake that state for a transaction-
    /// free prepare failure.
    #[serde(default)]
    tx_hash: Option<B256>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AutomaticRetryDecision {
    Ready,
    Wait(std::time::Duration),
    Exhausted,
    NotRecoverable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManualRecoveryDecision {
    Eligible,
    AlreadyTerminal,
    Unrecoverable,
}

impl Mint {
    pub(crate) const MAX_AUTOMATIC_MINT_RETRY_ATTEMPT: u32 = 4;
    const RETRY_EXTERNAL_TX_MARKER: &'static str = "-retry-";

    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::Initiated { .. } => "Initiated",
            Self::JournalConfirmed { .. } => "JournalConfirmed",
            Self::JournalRejected { .. } => "JournalRejected",
            Self::Minting { .. } => "Minting",
            Self::TxIntended { .. } => "MintIntended",
            Self::TxSubmitted { .. } => "TxSubmitted",
            Self::CallbackPending { .. } => "CallbackPending",
            Self::MintingFailed { .. } => "MintingFailed",
            Self::Completed { .. } => "Completed",
            Self::Closed { .. } => "Closed",
        }
    }

    /// Traverses the `failed_from` chain to find the last state before
    /// any failure. A `Minting` state that resumed from a failure delegates
    /// to the preserved chain so retry derivations (attempt number, known
    /// Fireblocks tx) survive the retry transition. If this mint carries no
    /// failure history, returns `self`.
    fn non_failed_predecessor(&self) -> &Self {
        match self {
            Self::MintingFailed { failed_from, .. } => {
                failed_from.non_failed_predecessor()
            }
            Self::Minting { retry: Some(context), .. } => {
                context.failed_from.non_failed_predecessor()
            }
            _ => self,
        }
    }

    /// The `external_tx_id` of the latest persisted `FireblocksSubmitted`
    /// predecessor, traversing any failure chain. `None` when no submission
    /// ever reached the signing backend.
    fn latest_known_external_tx_id(&self) -> Option<String> {
        match self.non_failed_predecessor() {
            Self::TxSubmitted { external_tx_id, .. } => {
                Some(external_tx_id.clone())
            }
            _ => None,
        }
    }

    /// Live prepared mint identity for rebroadcast / classification.
    ///
    /// Resolves through `MintingFailed` / `Minting { retry }` via
    /// [`Self::non_failed_predecessor`], and returns prepared bytes from
    /// `TxIntended` **or** `TxSubmitted` (when present). Without the
    /// `TxSubmitted` arm, a post-submit failure always looked like "no
    /// intent" and `SubmitMintJob` prepared a **new** hash — the double-mint
    /// amplifier for uncertain confirmation recovery.
    pub(super) fn pending_prepared_tx(&self) -> Option<PreparedMintTx> {
        match self.non_failed_predecessor() {
            Self::TxIntended { prepared_tx, .. } => Some(prepared_tx.clone()),
            Self::TxSubmitted { prepared_tx: Some(prepared_tx), .. } => {
                Some(prepared_tx.clone())
            }
            _ => None,
        }
    }

    /// Post-intent predecessor that cannot supply prepared bytes for
    /// classification (legacy `TxSubmitted { prepared_tx: None }`).
    ///
    /// Free-preparing a replacement in this state risks double-minting: a
    /// submission or intent already existed on-chain, but we cannot rebroadcast
    /// or prove death without the signed envelope. Recovery must inventory /
    /// confirm-poll only.
    pub(super) fn has_unclassifiable_post_intent_identity(&self) -> bool {
        matches!(
            self.non_failed_predecessor(),
            Self::TxSubmitted { prepared_tx: None, .. }
        )
    }

    /// Backend `tx_id` of the latest `TxSubmitted` predecessor, if any.
    pub(super) fn latest_known_tx_id(&self) -> Option<TxId> {
        match self.non_failed_predecessor() {
            Self::TxSubmitted { tx_id, .. } => Some(tx_id.clone()),
            _ => None,
        }
    }

    fn base_mint_external_tx_id(
        issuer_request_id: &IssuerMintRequestId,
    ) -> String {
        format!("mint-{issuer_request_id}")
    }

    fn retry_mint_external_tx_id(
        issuer_request_id: &IssuerMintRequestId,
        attempt: u32,
    ) -> String {
        format!(
            "{}{}{}",
            Self::base_mint_external_tx_id(issuer_request_id),
            Self::RETRY_EXTERNAL_TX_MARKER,
            attempt,
        )
    }

    fn retry_attempt_from_external_tx_id(external_tx_id: &str) -> Option<u32> {
        external_tx_id
            .rsplit_once(Self::RETRY_EXTERNAL_TX_MARKER)
            .and_then(|(_, attempt)| attempt.parse().ok())
    }

    /// Attempt number for the *next* retry's `external_tx_id`, derived from the
    /// latest persisted `TxSubmitted` predecessor. Stays unchanged when
    /// a submission fails before the tx is accepted (no new `TxSubmitted`
    /// persisted), so the same attempt number — and therefore the same
    /// `external_tx_id` — is reused on the next try. The delay/exhaustion
    /// schedule uses the separate `MintingFailed::attempts` counter instead.
    fn next_retry_attempt(&self) -> u32 {
        self.latest_known_external_tx_id()
            .and_then(|external_tx_id| {
                Self::retry_attempt_from_external_tx_id(&external_tx_id)
            })
            .unwrap_or(0)
            + 1
    }

    /// `external_tx_id` override for a durable `SubmitMintJob`: `Some(retry-N)`
    /// when this `Minting` state resumed from a failure (so Fireblocks does not
    /// dedupe the retry against the failed submission), `None` for a first
    /// submission (the backend derives the base deterministic id).
    pub(super) fn retry_submission_external_tx_id(
        &self,
    ) -> Option<MintExternalTxId> {
        let Self::Minting { issuer_request_id, retry: Some(_), .. } = self
        else {
            return None;
        };

        Some(MintExternalTxId::from_string(Self::retry_mint_external_tx_id(
            issuer_request_id,
            self.next_retry_attempt(),
        )))
    }

    const fn automatic_retry_delay(attempt: u32) -> Option<ChronoDuration> {
        match attempt {
            1 => Some(ChronoDuration::minutes(1)),
            2 => Some(ChronoDuration::minutes(10)),
            3 => Some(ChronoDuration::minutes(30)),
            4 => Some(ChronoDuration::hours(1)),
            _ => None,
        }
    }

    pub(crate) fn automatic_retry_decision(
        &self,
        now: DateTime<Utc>,
    ) -> AutomaticRetryDecision {
        let Self::MintingFailed { failed_at, attempts, .. } = self else {
            return match self {
                Self::JournalConfirmed { .. }
                | Self::Minting { .. }
                | Self::TxIntended { .. }
                | Self::TxSubmitted { .. }
                | Self::CallbackPending { .. } => AutomaticRetryDecision::Ready,
                _ => AutomaticRetryDecision::NotRecoverable,
            };
        };

        if *attempts > Self::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT {
            return AutomaticRetryDecision::Exhausted;
        }

        let Some(delay) = Self::automatic_retry_delay(*attempts) else {
            return AutomaticRetryDecision::Exhausted;
        };
        let retry_at = *failed_at + delay;
        if now >= retry_at {
            return AutomaticRetryDecision::Ready;
        }

        (retry_at - now)
            .to_std()
            .map_or(AutomaticRetryDecision::Ready, AutomaticRetryDecision::Wait)
    }

    pub(crate) const fn manual_recovery_decision(
        &self,
    ) -> ManualRecoveryDecision {
        match self {
            Self::Completed { .. } | Self::Closed { .. } => {
                ManualRecoveryDecision::AlreadyTerminal
            }
            Self::Initiated { .. } | Self::JournalRejected { .. } => {
                ManualRecoveryDecision::Unrecoverable
            }
            // MintingFailed stays eligible even when automatic retries are
            // exhausted: the cap bounds UNATTENDED retrying, while a manual
            // reprocess is the operator explicitly authorizing one more
            // attempt (driven directly, not through the capped loop).
            Self::JournalConfirmed { .. }
            | Self::Minting { .. }
            | Self::TxIntended { .. }
            | Self::TxSubmitted { .. }
            | Self::CallbackPending { .. }
            | Self::MintingFailed { .. } => ManualRecoveryDecision::Eligible,
        }
    }

    pub(crate) const fn network(&self) -> Option<Network> {
        match self {
            Self::Initiated { network, .. }
            | Self::JournalConfirmed { network, .. }
            | Self::JournalRejected { network, .. }
            | Self::Minting { network, .. }
            | Self::TxIntended { network, .. }
            | Self::TxSubmitted { network, .. }
            | Self::CallbackPending { network, .. }
            | Self::MintingFailed { network, .. }
            | Self::Completed { network, .. } => Some(*network),
            Self::Closed { .. } => None,
        }
    }

    pub(crate) const fn tokenization_request_id(
        &self,
    ) -> Option<&TokenizationRequestId> {
        match self {
            Self::Initiated { tokenization_request_id, .. }
            | Self::JournalConfirmed { tokenization_request_id, .. }
            | Self::JournalRejected { tokenization_request_id, .. }
            | Self::Minting { tokenization_request_id, .. }
            | Self::TxIntended { tokenization_request_id, .. }
            | Self::TxSubmitted { tokenization_request_id, .. }
            | Self::CallbackPending { tokenization_request_id, .. }
            | Self::MintingFailed { tokenization_request_id, .. }
            | Self::Completed { tokenization_request_id, .. } => {
                Some(tokenization_request_id)
            }
            Self::Closed { .. } => None,
        }
    }

    /// The mode anchored on `Initiated` — the only source mode-dependent
    /// steps derive from.
    pub(crate) const fn mint_mode(&self) -> Option<VaultMode> {
        match self {
            Self::Initiated { mint_mode, .. }
            | Self::JournalConfirmed { mint_mode, .. }
            | Self::JournalRejected { mint_mode, .. }
            | Self::Minting { mint_mode, .. }
            | Self::TxIntended { mint_mode, .. }
            | Self::TxSubmitted { mint_mode, .. }
            | Self::CallbackPending { mint_mode, .. }
            | Self::MintingFailed { mint_mode, .. }
            | Self::Completed { mint_mode, .. } => Some(*mint_mode),
            Self::Closed { .. } => None,
        }
    }

    pub(crate) const fn underlying(&self) -> Option<&UnderlyingSymbol> {
        match self {
            Self::Initiated { underlying, .. }
            | Self::JournalConfirmed { underlying, .. }
            | Self::JournalRejected { underlying, .. }
            | Self::Minting { underlying, .. }
            | Self::TxIntended { underlying, .. }
            | Self::TxSubmitted { underlying, .. }
            | Self::CallbackPending { underlying, .. }
            | Self::MintingFailed { underlying, .. }
            | Self::Completed { underlying, .. } => Some(underlying),
            Self::Closed { .. } => None,
        }
    }

    /// The recipient wallet — the `to` a mint authorization must be signed
    /// over.
    pub(crate) const fn wallet(&self) -> Option<Address> {
        match self {
            Self::Initiated { wallet, .. }
            | Self::JournalConfirmed { wallet, .. }
            | Self::JournalRejected { wallet, .. }
            | Self::Minting { wallet, .. }
            | Self::TxIntended { wallet, .. }
            | Self::TxSubmitted { wallet, .. }
            | Self::CallbackPending { wallet, .. }
            | Self::MintingFailed { wallet, .. }
            | Self::Completed { wallet, .. } => Some(*wallet),
            Self::Closed { .. } => None,
        }
    }

    pub(crate) const fn quantity(&self) -> Option<&Quantity> {
        match self {
            Self::Initiated { quantity, .. }
            | Self::JournalConfirmed { quantity, .. }
            | Self::JournalRejected { quantity, .. }
            | Self::Minting { quantity, .. }
            | Self::TxIntended { quantity, .. }
            | Self::TxSubmitted { quantity, .. }
            | Self::CallbackPending { quantity, .. }
            | Self::MintingFailed { quantity, .. }
            | Self::Completed { quantity, .. } => Some(quantity),
            Self::Closed { .. } => None,
        }
    }

    /// Whether this mint's lifecycle state can still accept a recipient
    /// authorization — the exact states [`Self::handle_authorize_mint`]
    /// destructures (keep the two in sync). The tokenization-id lookup uses
    /// this to prefer a live mint over stale same-id duplicates.
    pub(crate) const fn mint_authorization(
        &self,
    ) -> Option<&MintAuthorization> {
        match self {
            Self::Initiated { mint_authorization, .. }
            | Self::JournalConfirmed { mint_authorization, .. }
            | Self::JournalRejected { mint_authorization, .. }
            | Self::Minting { mint_authorization, .. }
            | Self::TxIntended { mint_authorization, .. }
            | Self::TxSubmitted { mint_authorization, .. }
            | Self::CallbackPending { mint_authorization, .. }
            | Self::MintingFailed { mint_authorization, .. }
            | Self::Completed { mint_authorization, .. } => {
                mint_authorization.as_ref()
            }
            Self::Closed { .. } => None,
        }
    }

    pub(crate) const fn accepts_mint_authorization(&self) -> bool {
        matches!(
            self,
            Self::Initiated { .. }
                | Self::JournalConfirmed { .. }
                | Self::Minting { .. }
        )
    }

    /// Associates the liquidity bot's validated authorization with this mint
    /// without changing the lifecycle state.
    ///
    /// Valid only before a transaction is intended: once `PrepareMint` signs,
    /// the nonce is baked into the persisted bytes, so a late delivery could
    /// not change what gets submitted. Idempotent on redelivery of an
    /// identical authorization; a conflicting one is rejected so the nonce
    /// can never be swapped mid-flight. On-chain validation (signer,
    /// `nonceUsed`) happens at the endpoint before this command — the
    /// aggregate enforces only mode and lifecycle invariants. The accepted
    /// states below must stay in sync with
    /// [`Self::accepts_mint_authorization`].
    fn handle_authorize_mint(
        &self,
        provided_id: IssuerMintRequestId,
        mint_authorization: MintAuthorization,
    ) -> Result<Vec<MintEvent>, MintError> {
        let (Self::Initiated {
            issuer_request_id: expected_id,
            underlying,
            mint_mode,
            mint_authorization: existing,
            ..
        }
        | Self::JournalConfirmed {
            issuer_request_id: expected_id,
            underlying,
            mint_mode,
            mint_authorization: existing,
            ..
        }
        | Self::Minting {
            issuer_request_id: expected_id,
            underlying,
            mint_mode,
            mint_authorization: existing,
            ..
        }) = self
        else {
            return Err(MintError::AuthorizationNotAcceptable {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        if matches!(mint_mode, VaultMode::VaultDirect) {
            return Err(MintError::AuthorizationForVaultDirectAsset {
                underlying: underlying.clone(),
            });
        }

        match existing {
            Some(existing) if *existing == mint_authorization => Ok(vec![]),
            Some(_) => Err(MintError::ConflictingMintAuthorization),
            None => Ok(vec![MintEvent::MintAuthorizationReceived {
                issuer_request_id: provided_id,
                mint_authorization,
                received_at: Utc::now(),
            }]),
        }
    }

    /// Sets the delivered authorization on the current state; the lifecycle
    /// position is untouched.
    /// Mirrors [`Self::handle_authorize_mint`]'s accepted states: the event
    /// is only ever emitted from these three, so any other state is an
    /// impossible replay and applies as a no-op — a hypothetical late
    /// emission must never attach a nonce after the transaction is signed
    /// (states past intent still CARRY the authorization; they receive it
    /// through the state-transition applies, not through this event).
    fn apply_mint_authorization_received(
        &mut self,
        authorization: MintAuthorization,
    ) {
        match self {
            Self::Initiated { mint_authorization, .. }
            | Self::JournalConfirmed { mint_authorization, .. }
            | Self::Minting { mint_authorization, .. } => {
                *mint_authorization = Some(authorization);
            }
            Self::JournalRejected { .. }
            | Self::TxIntended { .. }
            | Self::TxSubmitted { .. }
            | Self::CallbackPending { .. }
            | Self::MintingFailed { .. }
            | Self::Completed { .. }
            | Self::Closed { .. } => {}
        }
    }

    fn handle_confirm_journal(
        &self,
        provided_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::JournalConfirmed {
            issuer_request_id: provided_id,
            confirmed_at: now,
        }])
    }

    fn handle_reject_journal(
        &self,
        provided_id: IssuerMintRequestId,
        reason: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::JournalRejected {
            issuer_request_id: provided_id,
            reason,
            rejected_at: now,
        }])
    }

    fn validate_issuer_request_id(
        expected: &IssuerMintRequestId,
        provided: &IssuerMintRequestId,
    ) -> Result<(), MintError> {
        if provided != expected {
            return Err(MintError::IssuerMintRequestIdMismatch {
                expected: expected.clone(),
                provided: provided.clone(),
            });
        }
        Ok(())
    }

    /// Records the intent to mint by transitioning from `JournalConfirmed`
    /// to `Minting` state. Pure state transition — no network call.
    fn handle_deposit(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::JournalConfirmed { issuer_request_id: expected_id, .. } =
            self
        else {
            return Err(MintError::NotInJournalConfirmedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        Ok(vec![MintEvent::MintingStarted {
            issuer_request_id,
            started_at: Utc::now(),
        }])
    }

    /// Records a successful on-chain mint submission reported by a durable
    /// `SubmitMintJob`. Pure — emits `MintTxSubmitted` from the payload.
    /// Accepts `Minting` and legacy `TxIntended` (pre-jobs prepare path the
    /// submit job can still resume). Idempotent: a no-op once the mint has
    /// advanced past those states, so an at-least-once job re-run cannot
    /// double-record the submission.
    fn handle_record_tx_submitted(
        &self,
        issuer_request_id: IssuerMintRequestId,
        external_tx_id: MintExternalTxId,
        tx_id: TxId,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Minting { issuer_request_id: expected_id, .. }
            | Self::TxIntended { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::MintTxSubmitted {
                    issuer_request_id,
                    external_tx_id: external_tx_id.into_string(),
                    tx_id,
                    submitted_at: Utc::now(),
                }])
            }
            Self::TxSubmitted { .. }
            | Self::CallbackPending { .. }
            | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    fn handle_record_tx_intended(
        &self,
        issuer_request_id: IssuerMintRequestId,
        prepared_tx: PreparedMintTx,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Minting { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;
                Ok(vec![MintEvent::MintTxIntended {
                    issuer_request_id,
                    prepared_tx,
                    intended_at: Utc::now(),
                }])
            }
            Self::TxIntended { .. }
            | Self::TxSubmitted { .. }
            | Self::CallbackPending { .. }
            | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records a confirmed on-chain mint reported by a durable `ConfirmMintJob`.
    /// Pure — emits `TokensMinted` from the payload. Idempotent: a no-op once
    /// the mint has advanced past `TxSubmitted`. Rejects a report
    /// whose `tx_id` does not match the stored submission, so a
    /// stale confirm job re-driven after a retry cannot record an old
    /// transaction's result against a newer submission.
    fn handle_record_tokens_minted(
        &self,
        issuer_request_id: IssuerMintRequestId,
        confirmed: ConfirmedMint,
    ) -> Result<Vec<MintEvent>, MintError> {
        let ConfirmedMint {
            tx_id,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
        } = confirmed;

        match self {
            Self::TxSubmitted {
                issuer_request_id: expected_id,
                tx_id: stored_tx_id,
                mint_mode,
                ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                // A vault-direct result can never complete an orchestrator
                // mint: the recorded audit data (receipt id vs nonce) and the
                // receipt-custody implications differ.
                if let VaultMode::Orchestrator { .. } = mint_mode {
                    return Err(MintError::MintModeMismatch {
                        expected: mint_mode.kind(),
                        found: VaultModeKind::VaultDirect,
                    });
                }

                if stored_tx_id != &tx_id {
                    return Err(MintError::TxIdMismatch {
                        expected: stored_tx_id.clone(),
                        provided: tx_id,
                    });
                }

                Ok(vec![MintEvent::TokensMinted {
                    issuer_request_id,
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used,
                    block_number,
                    minted_at: Utc::now(),
                }])
            }
            Self::CallbackPending { .. } | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInSubmittedState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records a successful orchestrator mint confirmation reported by a
    /// durable `ConfirmMintJob`. Pure — emits `OrchestratorTokensMinted`.
    /// Idempotent: a no-op once the mint has advanced past `TxSubmitted`.
    fn handle_record_orchestrator_tokens_minted(
        &self,
        issuer_request_id: IssuerMintRequestId,
        confirmed: ConfirmedOrchestratorMint,
    ) -> Result<Vec<MintEvent>, MintError> {
        let ConfirmedOrchestratorMint {
            tx_id,
            tx_hash,
            nonce,
            shares_minted,
            gas_used,
            block_number,
        } = confirmed;

        match self {
            Self::TxSubmitted {
                issuer_request_id: expected_id,
                tx_id: stored_tx_id,
                mint_mode,
                mint_authorization,
                quantity,
                ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                if matches!(mint_mode, VaultMode::VaultDirect) {
                    return Err(MintError::MintModeMismatch {
                        expected: mint_mode.kind(),
                        found: VaultModeKind::Orchestrator,
                    });
                }

                if stored_tx_id != &tx_id {
                    return Err(MintError::TxIdMismatch {
                        expected: stored_tx_id.clone(),
                        provided: tx_id,
                    });
                }

                // The chain-reported values must equal what this mint was
                // authorized and prepared with — the orchestrator emits the
                // `Minted` fields from our own calldata, so a divergence
                // means a contract anomaly and must fail loudly rather than
                // become the persisted audit record.
                let authorization = mint_authorization
                    .as_ref()
                    .ok_or(MintError::MissingMintAuthorization)?;
                if nonce != authorization.nonce {
                    return Err(MintError::MintedNonceMismatch {
                        expected: authorization.nonce,
                        actual: nonce,
                    });
                }
                let authorized_shares = quantity
                    .to_u256_with_18_decimals()
                    .map_err(|error| MintError::QuantityConversion {
                        message: error.to_string(),
                    })?;
                if shares_minted != authorized_shares {
                    return Err(MintError::MintedSharesMismatch {
                        expected: authorized_shares,
                        actual: shares_minted,
                    });
                }

                Ok(vec![MintEvent::OrchestratorTokensMinted {
                    issuer_request_id,
                    tx_hash,
                    nonce,
                    shares_minted,
                    gas_used,
                    block_number,
                    minted_at: Utc::now(),
                }])
            }
            Self::CallbackPending { .. } | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInSubmittedState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records an orchestrator mint proven landed by the full-match
    /// `Minted`-log lookup after a `NonceReplayed` revert. Pure — emits
    /// `OrchestratorMintRecovered`. Idempotent: a no-op once the mint has
    /// advanced past `TxSubmitted`.
    fn handle_record_orchestrator_mint_recovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        nonce: B256,
        shares_minted: U256,
        block_number: u64,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::TxSubmitted {
                issuer_request_id: expected_id,
                mint_mode,
                ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                if matches!(mint_mode, VaultMode::VaultDirect) {
                    return Err(MintError::MintModeMismatch {
                        expected: mint_mode.kind(),
                        found: VaultModeKind::Orchestrator,
                    });
                }

                Ok(vec![MintEvent::OrchestratorMintRecovered {
                    issuer_request_id,
                    tx_hash,
                    nonce,
                    shares_minted,
                    block_number,
                    recovered_at: Utc::now(),
                }])
            }
            Self::CallbackPending { .. } | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInSubmittedState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records a sent Alpaca callback reported by a durable `SendCallbackJob`.
    /// Pure — emits `MintCompleted`. Idempotent: a no-op once the mint is
    /// already `Completed`.
    fn handle_record_callback_sent(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::CallbackPending {
                issuer_request_id: expected_id, ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::MintCompleted {
                    issuer_request_id,
                    completed_at: Utc::now(),
                }])
            }
            Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInCallbackPendingState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records a mint side-effect failure reported by a durable submission or
    /// confirmation job. Pure — emits `MintingFailed` from the payload.
    /// Idempotent and lenient: a stale failure report for a mint that already
    /// failed or advanced is ignored, so an at-least-once job re-run is safe.
    fn handle_record_mint_failed(
        &self,
        issuer_request_id: IssuerMintRequestId,
        error: String,
        classification: MintFailureClassification,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Minting { issuer_request_id: expected_id, .. }
            | Self::TxIntended { issuer_request_id: expected_id, .. }
            | Self::TxSubmitted { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::MintingFailed {
                    issuer_request_id,
                    error,
                    failed_at: Utc::now(),
                    classification,
                }])
            }
            _ => Ok(vec![]),
        }
    }

    /// Retries a failed mint by transitioning `MintingFailed` -> `Minting`,
    /// advancing the automatic-retry attempt counter. Pure — emits
    /// `MintRetryStarted`. Recovery sends this before re-enqueuing a
    /// `SubmitMintJob`. Idempotent: a no-op if the mint already left
    /// `MintingFailed` (e.g. a concurrent retry already started).
    fn handle_retry_mint(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::MintingFailed { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::MintRetryStarted {
                    issuer_request_id,
                    tx_hash: None,
                    manual_retry_id: None,
                    started_at: Utc::now(),
                }])
            }
            _ => Ok(vec![]),
        }
    }

    /// Mints initiated before this instant (2026-07-31T00:00:00Z) predate the
    /// current job-based submit flow and cannot prove from state alone that a
    /// `Minting`-predecessor failure never broadcast a transaction — a mined
    /// legacy transaction whose receipt was later redeemed leaves no trace in
    /// inventory. Under the current flow a broadcast from `Minting` is
    /// impossible: `SubmitMintJob` signs, persists `MintTxIntended`, and only
    /// then broadcasts, so any crash mid-submission lands in `TxIntended` or
    /// `TxSubmitted`, never `Minting`.
    const MANUAL_RETRY_PROVENANCE_SINCE_EPOCH: i64 = 1_785_456_000;

    /// The operator-authorized retry, gated on failure provenance where the
    /// aggregate state is authoritative (unlike the admin endpoint, which
    /// loads a snapshot that may be stale by execution time).
    ///
    /// A `Minting` predecessor on a modern-flow mint proves the failure
    /// happened at prepare or signing, before any transaction was persisted
    /// or broadcast, so a fresh submission cannot double-mint. A `TxIntended`
    /// or `TxSubmitted` predecessor holds a transaction that may still mine;
    /// the automatic retry path handles those by REUSING the persisted signed
    /// transaction, and this command refuses them rather than submit fresh
    /// bytes over an unresolved prior attempt. Pre-cutover mints are refused
    /// outright — their provenance cannot be proven from event history.
    fn handle_manual_retry_mint(
        &self,
        issuer_request_id: IssuerMintRequestId,
        manual_retry_id: Uuid,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::MintingFailed {
                issuer_request_id: expected_id,
                failed_from,
                initiated_at,
                ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                if initiated_at.timestamp()
                    < Self::MANUAL_RETRY_PROVENANCE_SINCE_EPOCH
                {
                    return Err(MintError::PreProvenanceCutoverMint {
                        initiated_at: *initiated_at,
                    });
                }

                if !matches!(failed_from.as_ref(), Self::Minting { .. })
                    || failed_from.has_transaction_provenance()
                {
                    return Err(MintError::AmbiguousRetryPredecessor {
                        predecessor: failed_from.state_name().to_string(),
                    });
                }

                Ok(vec![MintEvent::MintRetryStarted {
                    issuer_request_id,
                    tx_hash: None,
                    manual_retry_id: Some(manual_retry_id),
                    started_at: Utc::now(),
                }])
            }
            _ => Ok(vec![]),
        }
    }

    /// Records a mint whose on-chain transaction already succeeded (a receipt
    /// exists), reported by the `SubmitMintJob` before it re-submitted. Pure —
    /// emits `ExistingMintRecovered`, advancing to `CallbackPending` without
    /// re-minting. Idempotent: a no-op once the mint is already minted.
    fn handle_record_existing_mint(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Minting { issuer_request_id: expected_id, .. }
            | Self::TxIntended { issuer_request_id: expected_id, .. }
            | Self::TxSubmitted { issuer_request_id: expected_id, .. }
            | Self::MintingFailed { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                // A vault receipt is a vault-direct proof: the bot never
                // custodies a receipt for an orchestrator mint, so this path
                // should be unreachable for one — the guard mirrors the
                // other record handlers as defence in depth.
                if let Some(VaultMode::Orchestrator { .. }) = self.mint_mode() {
                    return Err(MintError::MintModeMismatch {
                        expected: VaultModeKind::Orchestrator,
                        found: VaultModeKind::VaultDirect,
                    });
                }

                Ok(vec![MintEvent::ExistingMintRecovered {
                    issuer_request_id,
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    block_number,
                    recovered_at: Utc::now(),
                }])
            }
            Self::CallbackPending { .. } | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    fn apply_journal_confirmed(&mut self, confirmed_at: DateTime<Utc>) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at: confirmed_at,
        };
    }

    fn apply_journal_rejected(
        &mut self,
        reason: String,
        rejected_at: DateTime<Utc>,
    ) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalRejected {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            reason,
            rejected_at,
        };
    }

    fn apply_minting_started(&mut self, started_at: DateTime<Utc>) {
        let Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            minting_started_at: started_at,
            retry: None,
        };
    }

    fn apply_mint_submitted(
        &mut self,
        external_tx_id: String,
        tx_id: TxId,
        _submitted_at: DateTime<Utc>,
    ) {
        // Rebroadcast after RetryMint submits from `Minting { retry }` without a
        // new `MintTxIntended`. Preserve the live prepared identity from the
        // predecessor chain so recovery can keep classifying/rebroadcasting the
        // same hash (dropping it here re-opened the double-mint hole).
        let prepared_tx = match self {
            Self::TxIntended { prepared_tx, .. } => Some(prepared_tx.clone()),
            Self::Minting { .. } => self.pending_prepared_tx(),
            _ => return,
        };

        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            minting_started_at,
            retry: _,
        }
        | Self::TxIntended {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            minting_started_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::TxSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            minting_started_at,
            prepared_tx,
            external_tx_id,
            tx_id,
        };
    }

    fn apply_mint_intended(&mut self, prepared_tx: PreparedMintTx) {
        let Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            minting_started_at,
            retry: _,
        } = self.clone()
        else {
            return;
        };

        *self = Self::TxIntended {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            minting_started_at,
            prepared_tx,
        };
    }

    fn apply_tokens_minted(&mut self, audit: MintedAudit) {
        let MintedAudit {
            tx_hash,
            receipt_id,
            mint_nonce,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        } = audit;
        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxIntended {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            mint_nonce,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        };
    }

    fn apply_minting_failed(
        &mut self,
        error: String,
        failed_at: DateTime<Utc>,
        classification: MintFailureClassification,
    ) {
        // Re-failure while already in MintingFailed (e.g. a recovery confirm
        // or resubmission attempt failed again): refresh error/failed_at and
        // advance the attempt counter so the delay schedule escalates and
        // eventually exhausts, but keep the original failed_from chain (and the
        // external_tx_id derived from it) so resubmission stays idempotent.
        if let Self::MintingFailed {
            error: existing_error,
            failed_at: existing_failed_at,
            classification: existing_classification,
            attempts,
            ..
        } = self
        {
            *existing_error = error;
            *existing_failed_at = failed_at;
            *existing_classification = classification;
            // An orchestrator-wide halt does not advance the per-mint
            // counter (SPEC "Failure States"): it resolves by upgrade, and
            // advancing here would exhaust every in-flight mint's budget
            // during one halt window.
            if !classification.is_environment_halt() {
                *attempts += 1;
            }
            return;
        }

        // First failure from a live state: seed the attempt counter from the
        // TxSubmitted predecessor's retry number when present, else 1
        // (a submission that failed before the tx was accepted). A retrying
        // Minting state keeps escalating the preserved failure history and
        // its predecessor chain instead of restarting from scratch.
        let (attempts, failed_from) = match self {
            Self::TxSubmitted { external_tx_id, .. } => (
                Self::retry_attempt_from_external_tx_id(external_tx_id)
                    .unwrap_or(0)
                    + 1,
                Box::new(self.clone()),
            ),
            Self::TxIntended { prepared_tx, .. } => (
                Self::retry_attempt_from_external_tx_id(
                    &prepared_tx.external_tx_id,
                )
                .unwrap_or(0)
                    + 1,
                Box::new(self.clone()),
            ),
            // A retry that failed to an environment-wide halt keeps the
            // preserved counter (SPEC "Failure States" — halts never advance
            // it); any other failure escalates the schedule. Keep the retry
            // wrapper, not only its older predecessor: the wrapper may carry
            // transaction-hash provenance that a manual retry must never
            // flatten away. `non_failed_predecessor` already traverses this
            // chain for automatic retry identity.
            Self::Minting { retry: Some(context), .. } => (
                if classification.is_environment_halt() {
                    context.attempts
                } else {
                    context.attempts + 1
                },
                Box::new(self.clone()),
            ),
            _ => (1, Box::new(self.clone())),
        };

        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxIntended {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            error,
            failed_at,
            classification,
            attempts,
            failed_from,
        };
    }

    fn apply_mint_completed(&mut self, completed_at: DateTime<Utc>) {
        let Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            mint_nonce,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::Completed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            mint_nonce,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
            completed_at,
        };
    }

    fn apply_existing_mint_recorded(
        &mut self,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    ) {
        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxIntended {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            tx_hash,
            receipt_id: Some(receipt_id),
            mint_nonce: None,
            shares_minted,
            gas_used: None,
            block_number,
            minted_at: recovered_at,
        };
    }

    /// Mirrors `apply_existing_mint_recorded` for an orchestrator mint whose
    /// landing was proven by the full-match `Minted`-log lookup: same
    /// lifecycle landing (`CallbackPending`), orchestrator-shaped audit data
    /// (`mint_nonce` in place of `receipt_id`), and no `gas_used` — a bare
    /// log does not expose it.
    fn apply_orchestrator_mint_recovered(
        &mut self,
        tx_hash: B256,
        nonce: B256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    ) {
        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxIntended {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::TxSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }
        | Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            tx_hash,
            receipt_id: None,
            mint_nonce: Some(nonce),
            shares_minted,
            gas_used: None,
            block_number,
            minted_at: recovered_at,
        };
    }

    fn has_transaction_provenance(&self) -> bool {
        match self {
            Self::TxIntended { .. }
            | Self::TxSubmitted { .. }
            | Self::CallbackPending { .. }
            | Self::Completed { .. } => true,
            Self::Minting { retry: Some(retry), .. } => {
                retry.tx_hash.is_some()
                    || retry.failed_from.has_transaction_provenance()
            }
            Self::MintingFailed { failed_from, .. } => {
                failed_from.has_transaction_provenance()
            }
            Self::Initiated { .. }
            | Self::JournalConfirmed { .. }
            | Self::JournalRejected { .. }
            | Self::Minting { retry: None, .. }
            | Self::Closed { .. } => false,
        }
    }

    fn apply_mint_retry_started(
        &mut self,
        tx_hash: Option<B256>,
        started_at: DateTime<Utc>,
    ) {
        let Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            attempts,
            failed_from,
            ..
        } = self.clone()
        else {
            return;
        };

        *self = Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode,
            mint_authorization,
            journal_confirmed_at,
            minting_started_at: started_at,
            retry: Some(MintRetryContext { attempts, failed_from, tx_hash }),
        };
    }
    fn handle_close_mint(
        &self,
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        acknowledged_unresolved_mint_tx_hash: Option<B256>,
    ) -> Result<Vec<MintEvent>, MintError> {
        if matches!(self, Self::Completed { .. } | Self::Closed { .. }) {
            return Err(MintError::NotRecoverable {
                current_state: self.state_name().to_string(),
            });
        }

        // A legacy `TxSubmitted { prepared_tx: None }` carries no prepared
        // bytes, but its submission is already on the wire and recovery still
        // enqueues confirm for the stored `tx_id`. Closing it terminal without
        // an acknowledgement would mark a live deposit resolved, so fall back
        // to the stored submission identity when there are no prepared bytes.
        let unresolved_mint_tx_hash = self
            .pending_prepared_tx()
            .map(|prepared_tx| prepared_tx.hash)
            .or_else(|| {
                self.latest_known_tx_id().and_then(|tx_id| tx_id.to_hash())
            });
        let acknowledged_unresolved_mint_tx_hash = match (
            unresolved_mint_tx_hash,
            acknowledged_unresolved_mint_tx_hash,
        ) {
            (Some(mint_tx_hash), acknowledgement) => {
                Some(Self::require_unresolved_mint_acknowledgement(
                    mint_tx_hash,
                    acknowledgement,
                )?)
            }
            (None, Some(provided)) => {
                return Err(
                    MintError::UnexpectedUnresolvedMintAcknowledgement {
                        provided,
                    },
                );
            }
            (None, None) => None,
        };

        Ok(vec![MintEvent::MintClosed {
            issuer_request_id,
            reason,
            acknowledged_unresolved_mint_tx_hash,
            closed_at: Utc::now(),
        }])
    }

    fn require_unresolved_mint_acknowledgement(
        persisted_mint_hash: B256,
        acknowledged_unresolved_mint_tx_hash: Option<B256>,
    ) -> Result<B256, MintError> {
        let acknowledged_hash = acknowledged_unresolved_mint_tx_hash.ok_or(
            MintError::UnresolvedMintRequiresAcknowledgement {
                mint_tx_hash: persisted_mint_hash,
            },
        )?;
        if acknowledged_hash != persisted_mint_hash {
            return Err(MintError::UnresolvedMintAcknowledgementMismatch {
                expected: persisted_mint_hash,
                provided: acknowledged_hash,
            });
        }

        Ok(acknowledged_hash)
    }
}

#[async_trait]
impl EventSourced for Mint {
    type Id = IssuerMintRequestId;
    type Event = MintEvent;
    type Command = MintCommand;
    type Error = MintError;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "Mint";
    const PROJECTION: Table = Table("mint_view");
    const SCHEMA_VERSION: u64 = 4;

    // Snapshots are disabled: the pre-migration wiring never wrote snapshots,
    // and event-sorcery hardwires snapshot-every-N with no off switch, so
    // usize::MAX makes the next-snapshot threshold unreachable. The proper
    // fix is for event-sorcery to take the snapshot policy explicitly from
    // the consumer, including the option to disable snapshotting entirely.
    const SNAPSHOT_SIZE: usize = usize::MAX;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            MintEvent::Initiated {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at,
                mint_mode,
            } => Some(Self::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: *network,
                client_id: *client_id,
                wallet: *wallet,
                initiated_at: *initiated_at,
                mint_mode: *mint_mode,
                // The authorization arrives out-of-band strictly after
                // `Initiated` is persisted; a fresh mint never has one.
                mint_authorization: None,
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
            MintCommand::Initiate {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                mint_mode,
            } => Ok(vec![MintEvent::Initiated {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
                mint_mode,
            }]),
            MintCommand::ConfirmJournal { .. }
            | MintCommand::RejectJournal { .. } => {
                Err(MintError::NotInInitiatedState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::AuthorizeMint { .. } => {
                Err(MintError::AuthorizationNotAcceptable {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::Deposit { .. } => {
                Err(MintError::NotInJournalConfirmedState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordTxIntended { .. }
            | MintCommand::RecordTxSubmitted { .. }
            | MintCommand::RecordExistingMint { .. } => {
                Err(MintError::NotInMintingState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordTokensMinted { .. } => {
                Err(MintError::NotInSubmittedState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordCallbackSent { .. } => {
                Err(MintError::NotInCallbackPendingState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordMintFailed { .. }
            | MintCommand::RetryMint { .. }
            | MintCommand::RecordOrchestratorTokensMinted { .. }
            | MintCommand::RecordOrchestratorMintRecovered { .. }
            | MintCommand::ManualRetryMint { .. } => Ok(vec![]),
            MintCommand::CloseMint { .. } => Err(MintError::NotRecoverable {
                current_state: "Uninitialized".to_string(),
            }),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            MintCommand::Initiate {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                mint_mode,
            } => {
                if matches!(self, Self::Initiated { .. }) {
                    Err(MintError::AlreadyInitiated {
                        tokenization_request_id: tokenization_request_id.0,
                    })
                } else {
                    Ok(vec![MintEvent::Initiated {
                        issuer_request_id,
                        tokenization_request_id,
                        quantity,
                        underlying,
                        token,
                        network,
                        client_id,
                        wallet,
                        initiated_at: Utc::now(),
                        mint_mode,
                    }])
                }
            }
            MintCommand::AuthorizeMint {
                issuer_request_id,
                mint_authorization,
            } => self
                .handle_authorize_mint(issuer_request_id, mint_authorization),
            MintCommand::ConfirmJournal { issuer_request_id } => {
                self.handle_confirm_journal(issuer_request_id)
            }
            MintCommand::RejectJournal { issuer_request_id, reason } => {
                self.handle_reject_journal(issuer_request_id, reason)
            }
            MintCommand::Deposit { issuer_request_id } => {
                self.handle_deposit(issuer_request_id)
            }
            MintCommand::RecordTxIntended {
                issuer_request_id,
                prepared_tx,
            } => self.handle_record_tx_intended(issuer_request_id, prepared_tx),
            MintCommand::RecordTxSubmitted {
                issuer_request_id,
                external_tx_id,
                tx_id,
            } => self.handle_record_tx_submitted(
                issuer_request_id,
                external_tx_id,
                tx_id,
            ),
            MintCommand::RecordTokensMinted {
                issuer_request_id,
                tx_id,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            } => self.handle_record_tokens_minted(
                issuer_request_id,
                ConfirmedMint {
                    tx_id,
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used,
                    block_number,
                },
            ),
            MintCommand::RecordCallbackSent { issuer_request_id } => {
                self.handle_record_callback_sent(issuer_request_id)
            }
            MintCommand::RecordMintFailed {
                issuer_request_id,
                error,
                classification,
            } => self.handle_record_mint_failed(
                issuer_request_id,
                error,
                classification,
            ),
            MintCommand::RecordOrchestratorTokensMinted {
                issuer_request_id,
                tx_id,
                tx_hash,
                nonce,
                shares_minted,
                gas_used,
                block_number,
            } => self.handle_record_orchestrator_tokens_minted(
                issuer_request_id,
                ConfirmedOrchestratorMint {
                    tx_id,
                    tx_hash,
                    nonce,
                    shares_minted,
                    gas_used,
                    block_number,
                },
            ),
            MintCommand::RecordOrchestratorMintRecovered {
                issuer_request_id,
                tx_hash,
                nonce,
                shares_minted,
                block_number,
            } => self.handle_record_orchestrator_mint_recovered(
                issuer_request_id,
                tx_hash,
                nonce,
                shares_minted,
                block_number,
            ),
            MintCommand::RetryMint { issuer_request_id } => {
                self.handle_retry_mint(issuer_request_id)
            }
            MintCommand::ManualRetryMint {
                issuer_request_id,
                manual_retry_id,
            } => self
                .handle_manual_retry_mint(issuer_request_id, manual_retry_id),
            MintCommand::RecordExistingMint {
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
            } => self.handle_record_existing_mint(
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
            ),
            MintCommand::CloseMint {
                issuer_request_id,
                reason,
                acknowledged_unresolved_mint_tx_hash,
            } => self.handle_close_mint(
                issuer_request_id,
                reason,
                acknowledged_unresolved_mint_tx_hash,
            ),
        }
    }
}

impl Mint {
    fn apply_event(&mut self, event: MintEvent) {
        match event {
            MintEvent::Initiated {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at,
                mint_mode,
            } => {
                *self = Self::Initiated {
                    issuer_request_id,
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at,
                    mint_mode,
                    // The authorization arrives out-of-band strictly after
                    // `Initiated`; a fresh mint never has one.
                    mint_authorization: None,
                };
            }
            MintEvent::MintAuthorizationReceived {
                issuer_request_id: _,
                mint_authorization,
                received_at: _,
            } => self.apply_mint_authorization_received(mint_authorization),
            MintEvent::JournalConfirmed {
                issuer_request_id: _,
                confirmed_at,
            } => self.apply_journal_confirmed(confirmed_at),
            MintEvent::JournalRejected {
                issuer_request_id: _,
                reason,
                rejected_at,
            } => self.apply_journal_rejected(reason, rejected_at),
            MintEvent::MintingStarted { issuer_request_id: _, started_at } => {
                self.apply_minting_started(started_at);
            }
            MintEvent::MintTxIntended {
                issuer_request_id: _,
                prepared_tx,
                intended_at: _,
            } => self.apply_mint_intended(prepared_tx),
            MintEvent::TokensMinted {
                issuer_request_id: _,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
                minted_at,
            } => self.apply_tokens_minted(MintedAudit {
                tx_hash,
                receipt_id: Some(receipt_id),
                mint_nonce: None,
                shares_minted,
                gas_used: Some(gas_used),
                block_number,
                minted_at,
            }),
            MintEvent::OrchestratorTokensMinted {
                issuer_request_id: _,
                tx_hash,
                nonce,
                shares_minted,
                gas_used,
                block_number,
                minted_at,
            } => self.apply_tokens_minted(MintedAudit {
                tx_hash,
                receipt_id: None,
                mint_nonce: Some(nonce),
                shares_minted,
                gas_used: Some(gas_used),
                block_number,
                minted_at,
            }),
            MintEvent::OrchestratorMintRecovered {
                issuer_request_id: _,
                tx_hash,
                nonce,
                shares_minted,
                block_number,
                recovered_at,
            } => self.apply_orchestrator_mint_recovered(
                tx_hash,
                nonce,
                shares_minted,
                block_number,
                recovered_at,
            ),
            MintEvent::MintingFailed {
                issuer_request_id: _,
                error,
                failed_at,
                classification,
            } => self.apply_minting_failed(error, failed_at, classification),
            MintEvent::MintCompleted { issuer_request_id: _, completed_at } => {
                self.apply_mint_completed(completed_at);
            }
            MintEvent::ExistingMintRecovered {
                issuer_request_id: _,
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
                recovered_at,
            } => self.apply_existing_mint_recorded(
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
                recovered_at,
            ),
            MintEvent::MintTxSubmitted {
                issuer_request_id: _,
                external_tx_id,
                tx_id,
                submitted_at,
            } => {
                self.apply_mint_submitted(external_tx_id, tx_id, submitted_at);
            }
            MintEvent::MintRetryStarted {
                issuer_request_id: _,
                tx_hash,
                started_at,
                ..
            } => {
                self.apply_mint_retry_started(tx_hash, started_at);
            }
            MintEvent::MintClosed {
                issuer_request_id,
                reason,
                acknowledged_unresolved_mint_tx_hash,
                closed_at,
            } => {
                *self = Self::Closed {
                    issuer_request_id,
                    reason,
                    acknowledged_unresolved_mint_tx_hash,
                    closed_at,
                };
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum MintError {
    #[error(
        "Mint already initiated for tokenization request: {tokenization_request_id}"
    )]
    AlreadyInitiated { tokenization_request_id: String },
    #[error(
        "Mint authorization delivered for vault-direct mint of {underlying}: nothing will ever consume it"
    )]
    AuthorizationForVaultDirectAsset { underlying: UnderlyingSymbol },
    #[error(
        "A different mint authorization is already recorded for this mint; the nonce cannot be swapped mid-flight"
    )]
    ConflictingMintAuthorization,
    #[error(
        "Mint authorization cannot be accepted in state {current_state}: authorizations are only accepted before the mint transaction is prepared"
    )]
    AuthorizationNotAcceptable { current_state: String },
    #[error(
        "Mint result mode mismatch: the mint's persisted anchor is {expected}, the recorded result is {found}"
    )]
    MintModeMismatch { expected: VaultModeKind, found: VaultModeKind },
    #[error("Orchestrator mint has no recorded authorization to check against")]
    MissingMintAuthorization,
    #[error(
        "Minted nonce diverges from the recorded authorization: expected {expected}, chain reported {actual}"
    )]
    MintedNonceMismatch { expected: B256, actual: B256 },
    #[error(
        "Minted shares diverge from the authorized amount: expected {expected}, chain reported {actual}"
    )]
    MintedSharesMismatch { expected: U256, actual: U256 },
    #[error("Mint not in Initiated state. Current state: {current_state}")]
    NotInInitiatedState { current_state: String },
    #[error(
        "Mint not in JournalConfirmed state. Current state: {current_state}"
    )]
    NotInJournalConfirmedState { current_state: String },
    #[error(
        "Mint not in CallbackPending state. Current state: {current_state}"
    )]
    NotInCallbackPendingState { current_state: String },
    #[error("Mint not in TxSubmitted state. Current state: {current_state}")]
    NotInSubmittedState { current_state: String },
    #[error(
        "Issuer request ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    IssuerMintRequestIdMismatch {
        expected: IssuerMintRequestId,
        provided: IssuerMintRequestId,
    },
    #[error("Mint not in Minting state. Current state: {current_state}")]
    NotInMintingState { current_state: String },
    #[error("Mint not in MintIntended state. Current state: {current_state}")]
    NotInMintIntendedState { current_state: String },
    #[error(
        "Mint not in Minting or MintingFailed state. Current state: {current_state}"
    )]
    NotInMintingOrMintingFailedState { current_state: String },
    #[error("Mint not in recoverable state. Current state: {current_state}")]
    NotRecoverable { current_state: String },
    #[error("Automatic mint retry is not due until {retry_at}")]
    RetryNotDue { retry_at: DateTime<Utc> },
    #[error("Automatic mint retries exhausted after {attempts} attempts")]
    AutomaticRetriesExhausted { attempts: u32 },
    #[error(
        "the mint's failure chain contains a prepared or submitted \
         transaction ({predecessor}); a fresh manual submission could \
         double-mint — refuse and investigate the prior transaction instead"
    )]
    AmbiguousRetryPredecessor { predecessor: String },
    #[error(
        "the mint was initiated at {initiated_at}, before the job-based \
         submit flow whose event history proves a Minting-predecessor \
         failure never broadcast; a fresh manual submission could \
         double-mint — verify the mint against on-chain history instead"
    )]
    PreProvenanceCutoverMint { initiated_at: DateTime<Utc> },
    #[error("Retry delay out of range")]
    RetryDelayOutOfRange,
    #[error(
        "Transaction ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    TxIdMismatch { expected: TxId, provided: TxId },
    #[error("Network {network} is not configured for mint")]
    NetworkNotConfigured { network: Network },
    #[error(
        "Vault returned transaction metadata that differs from mint intent"
    )]
    SubmittedTransactionMismatch,
    #[error(
        "Asset not found for underlying: {underlying} on network: {network}"
    )]
    AssetNotFound { underlying: UnderlyingSymbol, network: Network },
    #[error("Quantity conversion: {message}")]
    QuantityConversion { message: String },
    #[error(transparent)]
    AssetView(#[from] TokenizedAssetViewFailure),
    #[error("database query failed: {message}")]
    Database { message: String },
    #[error("Alpaca: {message}")]
    Alpaca { message: String },
    #[error("Receipt lookup: {message}")]
    ReceiptLookup { message: String },
    #[error("No receipt found for mint {issuer_request_id}")]
    ReceiptNotFound { issuer_request_id: IssuerMintRequestId },
    #[error(
        "Cannot prepare a new mint while another wallet intent is unresolved"
    )]
    PendingWalletIntent,
    #[error("Vault: {message}")]
    Vault { message: String },
    #[error(
        "Unresolved prepared mint {mint_tx_hash:?} requires explicit operator acknowledgement"
    )]
    UnresolvedMintRequiresAcknowledgement { mint_tx_hash: B256 },
    #[error(
        "Unresolved mint acknowledgement mismatch: expected {expected:?}, provided {provided:?}"
    )]
    UnresolvedMintAcknowledgementMismatch { expected: B256, provided: B256 },
    #[error(
        "Unresolved mint acknowledgement {provided:?} was provided, but this mint has no persisted prepared deposit"
    )]
    UnexpectedUnresolvedMintAcknowledgement { provided: B256 },
}

impl From<TokenizedAssetViewError> for MintError {
    fn from(error: TokenizedAssetViewError) -> Self {
        Self::AssetView(error.into())
    }
}

impl From<UnconfiguredNetworkError> for MintError {
    fn from(error: UnconfiguredNetworkError) -> Self {
        Self::NetworkNotConfigured { network: error.network }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::{Address, B256, Bytes, address, b256, uint};
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use cqrs_es::DomainEvent;
    use event_sorcery::{LifecycleError, StoreBuilder, TestHarness, replay};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use tracing::Level;
    use tracing_test::traced_test;
    use uuid::{Uuid, uuid};

    use super::{
        AutomaticRetryDecision, ClientId, IssuerMintRequestId,
        ManualRecoveryDecision, Mint, MintCommand, MintError, MintEvent,
        MintExternalTxId, MintFailureClassification, Network, Quantity,
        TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
        has_unresolved_signer_intent, orchestrator_mint_failure_classification,
    };
    use crate::config::VaultMode;
    use crate::prepare_event_sourced_startup;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        AssetKey, TokenizedAsset, TokenizedAssetCommand,
    };
    use crate::vault::{
        MintAuthorization, OrchestratorRevertReason, PreparedMintTx, TxId,
        VaultError,
    };

    pub(super) const VAULT: Address =
        address!("0xcccccccccccccccccccccccccccccccccccccccc");

    pub(super) const BOT: Address =
        address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    async fn insert_raw_event(
        pool: &Pool<Sqlite>,
        aggregate_type: &str,
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
            VALUES (?, ?, ?, ?, '1.0', ?, '{}')
            ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn unresolved_mint_intents_only_block_the_same_signer_network() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations should run");
        let aggregate_id = IssuerMintRequestId::random().to_string();

        for (sequence, event_type, payload) in [
            (1, "MintEvent::Initiated", r#"{"Initiated":{"network":"base"}}"#),
            (2, "MintEvent::MintTxIntended", "{}"),
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
                VALUES ('Mint', ?, ?, ?, '1.0', ?, '{}')
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
            "an unresolved intent must keep the same network's gate closed"
        );
        assert!(
            !has_unresolved_signer_intent(&pool, Network::Ethereum, None)
                .await
                .expect("intent query should succeed"),
            "a Base intent must not block an independent Ethereum signer"
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
                'Mint',
                'orphaned-intent',
                1,
                'MintEvent::MintTxIntended',
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
            orphaned_error.contains("requires one Initiated event"),
            "the validation trigger must name the missing origin, got: \
             {orphaned_error}"
        );
    }

    #[tokio::test]
    async fn signer_intent_migration_backfills_and_rejects_ambiguous_history() {
        const INIT: &str =
            include_str!("../../migrations/20251016210348_init.sql");
        const GUARD: &str = include_str!(
            "../../migrations/20260801095000_enforce_active_signer_intents.sql"
        );

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::raw_sql(INIT).execute(&pool).await.unwrap();
        insert_raw_event(
            &pool,
            "Mint",
            "existing-mint",
            1,
            "MintEvent::Initiated",
            r#"{"Initiated":{"network":"base"}}"#,
        )
        .await
        .unwrap();
        insert_raw_event(
            &pool,
            "Mint",
            "existing-mint",
            2,
            "MintEvent::MintTxIntended",
            "{}",
        )
        .await
        .unwrap();

        sqlx::raw_sql(GUARD).execute(&pool).await.unwrap();
        let active: (String, String, String) = sqlx::query_as(
            "SELECT network, aggregate_type, aggregate_id \
             FROM active_signer_intents",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            active,
            (
                "base".to_string(),
                "Mint".to_string(),
                "existing-mint".to_string(),
            )
        );

        let malformed = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::raw_sql(INIT).execute(&malformed).await.unwrap();
        insert_raw_event(
            &malformed,
            "Mint",
            "missing-network",
            1,
            "MintEvent::Initiated",
            r#"{"Initiated":{}}"#,
        )
        .await
        .unwrap();
        insert_raw_event(
            &malformed,
            "Mint",
            "missing-network",
            2,
            "MintEvent::MintTxIntended",
            "{}",
        )
        .await
        .unwrap();

        let malformed_error = sqlx::raw_sql(GUARD)
            .execute(&malformed)
            .await
            .expect_err(
                "migration must fail closed when an active intent has no \
                 signer domain",
            )
            .to_string();
        assert!(
            malformed_error.contains("NOT NULL constraint failed"),
            "the malformed origin must abort on the NOT NULL network \
             constraint specifically, got: {malformed_error}"
        );

        // The core double-signing hazard the table exists to prevent: TWO
        // historical aggregates left unresolved intents on the same network.
        // The backfill must abort on the PRIMARY KEY rather than pick a
        // winner — remediation is resolving one aggregate, never guessing.
        let conflicted = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::raw_sql(INIT).execute(&conflicted).await.unwrap();
        for aggregate_id in ["first-unresolved", "second-unresolved"] {
            insert_raw_event(
                &conflicted,
                "Mint",
                aggregate_id,
                1,
                "MintEvent::Initiated",
                r#"{"Initiated":{"network":"base"}}"#,
            )
            .await
            .unwrap();
            insert_raw_event(
                &conflicted,
                "Mint",
                aggregate_id,
                2,
                "MintEvent::MintTxIntended",
                "{}",
            )
            .await
            .unwrap();
        }
        let conflicted_error = sqlx::raw_sql(GUARD)
            .execute(&conflicted)
            .await
            .expect_err(
                "migration must abort on two unresolved intents sharing a \
                 network instead of silently choosing one",
            )
            .to_string();
        assert!(
            conflicted_error.contains("UNIQUE constraint failed"),
            "the historical conflict must abort on the network PRIMARY KEY \
             specifically, got: {conflicted_error}"
        );

        // Duplicate origin events make the signer domain ambiguous; the
        // backfill must fail closed exactly like the live validation trigger
        // instead of trusting whichever duplicate a LIMIT picks.
        let duplicated = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::raw_sql(INIT).execute(&duplicated).await.unwrap();
        for (sequence, event_type, payload) in [
            (1, "MintEvent::Initiated", r#"{"Initiated":{"network":"base"}}"#),
            (
                2,
                "MintEvent::Initiated",
                r#"{"Initiated":{"network":"ethereum"}}"#,
            ),
            (3, "MintEvent::MintTxIntended", "{}"),
        ] {
            insert_raw_event(
                &duplicated,
                "Mint",
                "duplicated-origin",
                sequence,
                event_type,
                payload,
            )
            .await
            .unwrap();
        }
        let duplicated_error = sqlx::raw_sql(GUARD)
            .execute(&duplicated)
            .await
            .expect_err(
                "migration must fail closed on duplicate origin events \
                 instead of guessing which network is authoritative",
            )
            .to_string();
        assert!(
            duplicated_error.contains("NOT NULL constraint failed"),
            "duplicate origins must abort on the NOT NULL network constraint \
             specifically, got: {duplicated_error}"
        );
    }

    #[tokio::test]
    async fn event_store_atomically_rejects_a_second_same_network_intent() {
        // A single connection: each pooled connection to ":memory:" opens
        // its OWN empty database, so a second connection would see neither
        // the schema nor the seeded events.
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
                "mint-a",
                1,
                "MintEvent::Initiated",
                r#"{"Initiated":{"network":"base"}}"#,
            ),
            ("mint-a", 2, "MintEvent::MintTxIntended", "{}"),
            (
                "mint-b",
                1,
                "MintEvent::Initiated",
                r#"{"Initiated":{"network":"base"}}"#,
            ),
        ] {
            insert_raw_event(
                &pool,
                "Mint",
                aggregate_id,
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("test history should insert");
        }

        let competing = insert_raw_event(
            &pool,
            "Mint",
            "mint-b",
            2,
            "MintEvent::MintTxIntended",
            "{}",
        )
        .await;
        let competing_error = competing
            .expect_err(
                "the intent append must atomically reject a competing signer \
                 nonce",
            )
            .to_string();
        assert!(
            competing_error.contains("signer network already reserved"),
            "the cross-instance rejection must carry the explicit \
             reservation message, not an implicit unique violation that \
             upstream layers misreport as a same-aggregate conflict; got: \
             {competing_error}"
        );

        insert_raw_event(
            &pool,
            "Mint",
            "mint-missing-network",
            1,
            "MintEvent::Initiated",
            r#"{"Initiated":{}}"#,
        )
        .await
        .expect("malformed origin should seed for boundary testing");
        let missing_error = insert_raw_event(
            &pool,
            "Mint",
            "mint-missing-network",
            2,
            "MintEvent::MintTxIntended",
            "{}",
        )
        .await
        .expect_err(
            "missing mint network metadata must fail closed before intent \
             commit",
        )
        .to_string();
        assert!(
            missing_error.contains("requires network metadata"),
            "the validation trigger must name the missing metadata, got: \
             {missing_error}"
        );

        for (sequence, event_type, payload) in [
            (
                1,
                "MintEvent::Initiated",
                r#"{"Initiated":{"network":"ethereum"}}"#,
            ),
            (2, "MintEvent::MintTxIntended", "{}"),
        ] {
            insert_raw_event(
                &pool,
                "Mint",
                "mint-ethereum",
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("an independent signer network must remain available");
        }

        insert_raw_event(
            &pool,
            "Redemption",
            "redemption-base",
            1,
            "RedemptionEvent::Detected",
            r#"{"Detected":{"network":"base"}}"#,
        )
        .await
        .expect("redemption origin should insert");
        let competing_burn = insert_raw_event(
            &pool,
            "Redemption",
            "redemption-base",
            2,
            "RedemptionEvent::BurnIntended",
            "{}",
        )
        .await;
        let competing_burn_error = competing_burn
            .expect_err(
                "mint and burn intents must share one per-network nonce domain",
            )
            .to_string();
        assert!(
            competing_burn_error.contains("signer network already reserved"),
            "a burn competing with an unresolved mint must carry the explicit \
             reservation message, got: {competing_burn_error}"
        );

        insert_raw_event(
            &pool,
            "Mint",
            "mint-a",
            3,
            "MintEvent::MintTxSubmitted",
            "{}",
        )
        .await
        .expect("terminal submission should release the signer domain");
        insert_raw_event(
            &pool,
            "Mint",
            "mint-b",
            2,
            "MintEvent::MintTxIntended",
            "{}",
        )
        .await
        .expect("a resolved intent must release the next same-network append");
    }

    /// An operator close is valid from any non-terminal state, including
    /// TxIntended. It must release the network's signer reservation — a
    /// close that left the row behind would permanently reject every later
    /// mint and burn on that network with no self-healing path.
    #[tokio::test]
    async fn admin_close_releases_the_signer_reservation() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations should run");

        for (sequence, event_type, payload) in [
            (1, "MintEvent::Initiated", r#"{"Initiated":{"network":"base"}}"#),
            (2, "MintEvent::MintTxIntended", "{}"),
        ] {
            insert_raw_event(
                &pool,
                "Mint",
                "mint-closed",
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("test history should insert");
        }
        assert!(
            has_unresolved_signer_intent(&pool, Network::Base, None)
                .await
                .expect("intent query should succeed"),
            "the intent must reserve the network before the close"
        );

        insert_raw_event(
            &pool,
            "Mint",
            "mint-closed",
            3,
            "MintEvent::MintClosed",
            "{}",
        )
        .await
        .expect("the admin close event should insert");

        assert!(
            !has_unresolved_signer_intent(&pool, Network::Base, None)
                .await
                .expect("intent query should succeed"),
            "an admin close must release the network's signer reservation"
        );
        insert_raw_event(
            &pool,
            "Mint",
            "mint-after-close",
            1,
            "MintEvent::Initiated",
            r#"{"Initiated":{"network":"base"}}"#,
        )
        .await
        .expect("successor origin should insert");
        insert_raw_event(
            &pool,
            "Mint",
            "mint-after-close",
            2,
            "MintEvent::MintTxIntended",
            "{}",
        )
        .await
        .expect("a closed mint must not block the next same-network intent");
    }

    /// An orchestrator mint's landing can be recorded while the intent's
    /// reservation is still held (recovery proving the landing without a
    /// `MintTxSubmitted` ever committing). Both orchestrator resolutions
    /// must release the network's reservation, or the row strands and every
    /// later mint AND burn on that network is rejected until an operator
    /// close.
    #[tokio::test]
    async fn orchestrator_resolutions_release_the_signer_reservation() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations should run");

        for (aggregate_id, resolution) in [
            ("mint-recovered", "MintEvent::OrchestratorMintRecovered"),
            ("mint-orch-minted", "MintEvent::OrchestratorTokensMinted"),
        ] {
            for (sequence, event_type, payload) in [
                (
                    1,
                    "MintEvent::Initiated",
                    r#"{"Initiated":{"network":"base"}}"#,
                ),
                (2, "MintEvent::MintTxIntended", "{}"),
            ] {
                insert_raw_event(
                    &pool,
                    "Mint",
                    aggregate_id,
                    sequence,
                    event_type,
                    payload,
                )
                .await
                .expect("test history should insert");
            }
            assert!(
                has_unresolved_signer_intent(&pool, Network::Base, None)
                    .await
                    .expect("intent query should succeed"),
                "the intent must reserve the network before {resolution}"
            );

            insert_raw_event(&pool, "Mint", aggregate_id, 3, resolution, "{}")
                .await
                .expect("the orchestrator resolution event should insert");

            assert!(
                !has_unresolved_signer_intent(&pool, Network::Base, None)
                    .await
                    .expect("intent query should succeed"),
                "{resolution} must release the network's signer reservation"
            );
        }
    }

    /// The validation trigger's third branch: a network value outside the
    /// known set must abort the intent append, not reserve an ambiguous
    /// signer domain.
    #[tokio::test]
    async fn unknown_network_metadata_rejects_the_intent_append() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("in-memory database should connect");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations should run");

        insert_raw_event(
            &pool,
            "Mint",
            "mint-unknown-network",
            1,
            "MintEvent::Initiated",
            r#"{"Initiated":{"network":"solana"}}"#,
        )
        .await
        .expect("unknown-network origin should seed for boundary testing");

        let unknown_error = insert_raw_event(
            &pool,
            "Mint",
            "mint-unknown-network",
            2,
            "MintEvent::MintTxIntended",
            "{}",
        )
        .await
        .expect_err(
            "an unknown mint network must fail closed before intent commit",
        )
        .to_string();
        assert!(
            unknown_error.contains("has an unknown network"),
            "the validation trigger must name the unknown-network branch, \
             got: {unknown_error}"
        );
    }

    fn minting_events_for_retry(
        issuer_request_id: &IssuerMintRequestId,
        external_tx_id: String,
        failed_at: DateTime<Utc>,
    ) -> Vec<MintEvent> {
        vec![
            MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: failed_at,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: failed_at,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: failed_at,
            },
            MintEvent::MintTxSubmitted {
                issuer_request_id: issuer_request_id.clone(),
                external_tx_id,
                tx_id: TxId::Legacy("fb-failed".to_string()),
                submitted_at: failed_at,
            },
            MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "terminal transaction failure".to_string(),
                failed_at,
                classification: MintFailureClassification::Unclassified,
            },
        ]
    }

    pub(super) fn events_through_minting(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        vec![
            MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: now,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: now,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: now,
            },
        ]
    }

    pub(super) fn events_through_tx_submitted(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_minting(issuer_request_id);
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: "ext-1".to_string(),
            tx_id: TxId::Legacy("fb-1".to_string()),
            submitted_at: Utc::now(),
        });
        events
    }

    pub(super) fn events_through_tokens_minted(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_tx_submitted(issuer_request_id);
        events.push(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: uint!(42_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: 21_000,
            block_number: 1_000,
            minted_at: Utc::now(),
        });
        events
    }

    fn events_through_tx_intended(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_minting(issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: PreparedMintTx::valid_for_test(
                1,
                format!("mint-{issuer_request_id}"),
            ),
            intended_at: Utc::now(),
        });
        events
    }

    fn events_through_completed(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_tokens_minted(issuer_request_id);
        events.push(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at: Utc::now(),
        });
        events
    }

    #[test]
    fn manual_recovery_decision_rejects_nonrecoverable_states() {
        let issuer_request_id = IssuerMintRequestId::random();
        let minting = events_through_minting(&issuer_request_id);
        let initiated =
            replay::<Mint>(vec![minting[0].clone()]).unwrap().unwrap();
        let journal_confirmed =
            replay::<Mint>(minting[..2].to_vec()).unwrap().unwrap();
        let completed =
            replay::<Mint>(events_through_completed(&issuer_request_id))
                .unwrap()
                .unwrap();
        let failed_at = Utc::now() - chrono::Duration::hours(2);
        let mut exhausted_events =
            events_through_tx_submitted(&issuer_request_id);
        for _ in 0..5 {
            exhausted_events.push(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
                classification: MintFailureClassification::Unclassified,
            });
        }
        let exhausted = replay::<Mint>(exhausted_events).unwrap().unwrap();

        assert_eq!(
            initiated.manual_recovery_decision(),
            ManualRecoveryDecision::Unrecoverable
        );
        assert_eq!(
            journal_confirmed.manual_recovery_decision(),
            ManualRecoveryDecision::Eligible
        );
        assert_eq!(
            completed.manual_recovery_decision(),
            ManualRecoveryDecision::AlreadyTerminal
        );
        // Exhaustion caps AUTOMATIC retries only: a manual reprocess is the
        // operator explicitly authorizing another attempt, so an exhausted
        // MintingFailed stays eligible for manual recovery.
        assert_eq!(
            exhausted.manual_recovery_decision(),
            ManualRecoveryDecision::Eligible
        );
    }

    /// A modern-flow mint that failed at prepare (a `Minting` predecessor,
    /// so provably no transaction was ever persisted or broadcast) is the
    /// case the manual retry exists for.
    #[tokio::test]
    async fn manual_retry_starts_a_retry_for_a_modern_prepare_failure() {
        let issuer_request_id = IssuerMintRequestId::random();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "signing rejected".to_string(),
            failed_at: Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });

        let emitted = TestHarness::<Mint>::with(())
            .given(events)
            .when(MintCommand::ManualRetryMint {
                issuer_request_id: issuer_request_id.clone(),
                manual_retry_id: Uuid::new_v4(),
            })
            .await
            .events();

        assert!(matches!(
            emitted.as_slice(),
            [MintEvent::MintRetryStarted { .. }]
        ));
    }

    /// A replayed legacy retry can carry a transaction hash while its enum
    /// state is `Minting`; a later prepare-looking failure must retain and
    /// reject that provenance instead of authorizing another mint transaction.
    #[tokio::test]
    async fn manual_retry_refuses_transaction_provenance_hidden_by_retry_state()
    {
        let issuer_request_id = IssuerMintRequestId::random();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "first attempt failed".to_string(),
            failed_at: Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });
        events.push(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: Some(B256::random()),
            manual_retry_id: None,
            started_at: Utc::now(),
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "retry failed".to_string(),
            failed_at: Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });

        let error = TestHarness::<Mint>::with(())
            .given(events)
            .when(MintCommand::ManualRetryMint {
                issuer_request_id: issuer_request_id.clone(),
                manual_retry_id: Uuid::new_v4(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(
                    MintError::AmbiguousRetryPredecessor { .. }
                )
            ),
            "transaction-bearing retry provenance must block a fresh mint, got {error:?}"
        );
    }

    /// A mint initiated before the job-based submit flow cannot prove its
    /// `Minting`-predecessor failure never broadcast a transaction, so the
    /// manual retry refuses it.
    #[tokio::test]
    async fn manual_retry_refuses_a_pre_cutover_mint() {
        let issuer_request_id = IssuerMintRequestId::random();
        let mut events = events_through_minting(&issuer_request_id);
        let MintEvent::Initiated { initiated_at, .. } = &mut events[0] else {
            panic!("first event must be Initiated");
        };
        *initiated_at = Utc::now() - chrono::Duration::days(90);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "signing rejected".to_string(),
            failed_at: Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });

        let error = TestHarness::<Mint>::with(())
            .given(events)
            .when(MintCommand::ManualRetryMint {
                issuer_request_id: issuer_request_id.clone(),
                manual_retry_id: Uuid::new_v4(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(
                    MintError::PreProvenanceCutoverMint { .. }
                )
            ),
            "a pre-cutover mint must be refused, got {error:?}"
        );
    }

    #[test]
    fn pending_prepared_tx_resolves_from_tx_intended() {
        let issuer_request_id = IssuerMintRequestId::random();
        let mint =
            replay::<Mint>(events_through_tx_intended(&issuer_request_id))
                .unwrap()
                .unwrap();
        let prepared =
            mint.pending_prepared_tx().expect("TxIntended holds prepared");
        assert_eq!(prepared.nonce, 1);
        assert_eq!(
            prepared.external_tx_id,
            format!("mint-{issuer_request_id}")
        );
    }

    #[test]
    fn pending_prepared_tx_resolves_from_tx_submitted_after_intended() {
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared_tx = PreparedMintTx::valid_for_test(
            7,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared_tx.hash;
        let mut events = events_through_tx_intended(&issuer_request_id);
        // Rewrite intended with known prepared, then submit so apply copies it.
        if let MintEvent::MintTxIntended { prepared_tx: stored, .. } =
            &mut events[3]
        {
            *stored = prepared_tx;
        }
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(expected_hash),
            submitted_at: Utc::now(),
        });
        let mint = replay::<Mint>(events).unwrap().unwrap();
        let prepared = mint
            .pending_prepared_tx()
            .expect("TxSubmitted must retain prepared_tx from intended");
        assert_eq!(prepared.hash, expected_hash);
        assert_eq!(prepared.nonce, 7);
    }

    #[test]
    fn pending_prepared_tx_resolves_failed_from_tx_submitted_chain() {
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared_tx = PreparedMintTx::valid_for_test(
            9,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared_tx.hash;
        let mut events = events_through_tx_intended(&issuer_request_id);
        if let MintEvent::MintTxIntended { prepared_tx: stored, .. } =
            &mut events[3]
        {
            *stored = prepared_tx;
        }
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(expected_hash),
            submitted_at: Utc::now(),
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "reverted".to_string(),
            failed_at: Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });
        let failed = replay::<Mint>(events).unwrap().unwrap();
        let prepared = failed
            .pending_prepared_tx()
            .expect("MintingFailed must resolve prepared via failed_from");
        assert_eq!(prepared.hash, expected_hash);

        // After RetryMint the Minting{retry} chain must still see the same hash.
        let mut after_retry = failed;
        after_retry.apply_event(MintEvent::MintRetryStarted {
            issuer_request_id,
            tx_hash: None,
            manual_retry_id: None,
            started_at: Utc::now(),
        });
        let prepared_after_retry = after_retry
            .pending_prepared_tx()
            .expect("retry Minting must resolve prepared via failed_from");
        assert_eq!(prepared_after_retry.hash, expected_hash);
    }

    #[test]
    fn pending_prepared_tx_none_for_first_minting_attempt() {
        let issuer_request_id = IssuerMintRequestId::random();
        let mint = replay::<Mint>(events_through_minting(&issuer_request_id))
            .unwrap()
            .unwrap();
        assert!(
            mint.pending_prepared_tx().is_none(),
            "first Minting attempt has no prepared identity; prepare is allowed"
        );
    }

    #[tokio::test]
    async fn record_tx_intended_from_minting_emits_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared_tx = PreparedMintTx::valid_for_test(
            1,
            format!("mint-{issuer_request_id}"),
        );

        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::RecordTxIntended {
                issuer_request_id,
                prepared_tx,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [MintEvent::MintTxIntended { .. }]
        ));
    }

    pub(super) const ORCHESTRATOR: Address =
        address!("0xdddddddddddddddddddddddddddddddddddddddd");

    pub(super) fn test_mint_authorization() -> MintAuthorization {
        MintAuthorization {
            nonce: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            signature: Bytes::from(vec![0x42; 65]),
        }
    }

    /// `events_through_minting` with the `Initiated` mode anchor flipped to
    /// orchestrator — no authorization delivered yet.
    pub(super) fn orchestrator_events_through_minting(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_minting(issuer_request_id);
        if let Some(MintEvent::Initiated { mint_mode, .. }) = events.first_mut()
        {
            *mint_mode = VaultMode::Orchestrator { address: ORCHESTRATOR };
        }
        events
    }

    pub(super) fn orchestrator_events_through_minting_authorized(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = orchestrator_events_through_minting(issuer_request_id);
        events.insert(
            1,
            MintEvent::MintAuthorizationReceived {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: test_mint_authorization(),
                received_at: Utc::now(),
            },
        );
        events
    }

    pub(super) fn orchestrator_events_through_tx_submitted(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events =
            orchestrator_events_through_minting_authorized(issuer_request_id);
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: "ext-1".to_string(),
            tx_id: TxId::Legacy("fb-1".to_string()),
            submitted_at: Utc::now(),
        });
        events
    }

    #[tokio::test]
    async fn record_tx_submitted_from_minting_emits_event() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::RecordTxSubmitted {
                issuer_request_id: issuer_request_id.clone(),
                external_tx_id: MintExternalTxId::from_string(
                    "ext-1".to_string(),
                ),
                tx_id: TxId::Legacy("fb-1".to_string()),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MintEvent::MintTxSubmitted { tx_id, .. }
                if tx_id == &TxId::Legacy("fb-1".to_string())
        ));
    }

    /// `SubmitMintJob` can resume a legacy `TxIntended` mint; recording the
    /// submission must not reject with `NotInMintingState`.
    #[tokio::test]
    async fn record_tx_submitted_from_tx_intended_emits_event() {
        let issuer_request_id = IssuerMintRequestId::random();

        let recorded = TestHarness::<Mint>::with(())
            .given(events_through_tx_intended(&issuer_request_id))
            .when(MintCommand::RecordTxSubmitted {
                issuer_request_id: issuer_request_id.clone(),
                external_tx_id: MintExternalTxId::from_string(
                    "ext-1".to_string(),
                ),
                tx_id: TxId::Legacy("fb-1".to_string()),
            })
            .await
            .events();

        assert_eq!(recorded.len(), 1);
        assert!(matches!(
            &recorded[0],
            MintEvent::MintTxSubmitted { tx_id, .. }
                if tx_id == &TxId::Legacy("fb-1".to_string())
        ));
    }

    #[test]
    fn minting_failed_from_tx_intended_replays_to_failed() {
        let issuer_request_id = IssuerMintRequestId::random();
        let mut mint =
            replay::<Mint>(events_through_tx_intended(&issuer_request_id))
                .unwrap()
                .unwrap();

        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id,
            error: "legacy broadcast failed".to_string(),
            failed_at: Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });

        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "a persisted failure must replay to MintingFailed, got {}",
            mint.state_name()
        );
    }

    #[tokio::test]
    async fn record_existing_mint_from_tx_intended_emits_event() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tx_intended(&issuer_request_id))
            .when(MintCommand::RecordExistingMint {
                issuer_request_id,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                receipt_id: uint!(7_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                block_number: 1_234,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [MintEvent::ExistingMintRecovered { .. }]
        ));
    }

    #[tokio::test]
    async fn record_tx_submitted_is_idempotent_once_submitted() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordTxSubmitted {
                issuer_request_id,
                external_tx_id: MintExternalTxId::from_string(
                    "ext-1".to_string(),
                ),
                tx_id: TxId::Legacy("fb-1".to_string()),
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "re-recording an already-submitted mint must be a no-op"
        );
    }

    #[tokio::test]
    async fn record_tokens_minted_from_tx_submitted_emits_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let expected_tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordTokensMinted {
                issuer_request_id,
                tx_id: TxId::Legacy("fb-1".to_string()),
                tx_hash: expected_tx_hash,
                receipt_id: uint!(7_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 21_000,
                block_number: 1_234,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let MintEvent::TokensMinted {
            issuer_request_id: _,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at: _,
        } = &events[0]
        else {
            panic!("Expected TokensMinted, got {:?}", events[0]);
        };
        assert_eq!(*tx_hash, expected_tx_hash);
        assert_eq!(*receipt_id, uint!(7_U256));
        assert_eq!(*shares_minted, uint!(100_000000000000000000_U256));
        assert_eq!(*gas_used, 21_000);
        assert_eq!(*block_number, 1_234);
    }

    #[tokio::test]
    async fn record_tokens_minted_is_idempotent_once_minted() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tokens_minted(&issuer_request_id))
            .when(MintCommand::RecordTokensMinted {
                issuer_request_id,
                tx_id: TxId::Legacy("fb-1".to_string()),
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                receipt_id: uint!(7_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 21_000,
                block_number: 1_234,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "re-recording an already-minted mint must be a no-op"
        );
    }

    #[tokio::test]
    async fn record_tokens_minted_rejects_mismatched_tx_id() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given(events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordTokensMinted {
                issuer_request_id,
                tx_id: TxId::Legacy("fb-stale".to_string()),
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                receipt_id: uint!(7_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 21_000,
                block_number: 1_234,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::TxIdMismatch { .. })
            ),
            "a confirm report for a different signing-backend tx must be \
             rejected, got {error:?}"
        );
    }

    #[test]
    fn retry_transition_preserves_attempts_and_predecessor_chain() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();

        let mut mint =
            replay::<Mint>(events_through_minting(&issuer_request_id))
                .unwrap()
                .unwrap();

        mint.apply_event(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: Mint::base_mint_external_tx_id(&issuer_request_id),
            tx_id: TxId::Legacy("fb-1".to_string()),
            submitted_at: now,
        });
        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "terminal signer failure".to_string(),
            failed_at: now,
            classification: MintFailureClassification::Unclassified,
        });

        mint.apply_event(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            manual_retry_id: None,
            started_at: now,
        });

        assert!(
            matches!(mint.non_failed_predecessor(), Mint::TxSubmitted { .. }),
            "the retry transition must preserve the failed predecessor chain"
        );
        assert_eq!(
            mint.retry_submission_external_tx_id(),
            Some(MintExternalTxId::from_string(format!(
                "mint-{issuer_request_id}-retry-1"
            ))),
            "a retry submission must not reuse the failed submission's \
             external_tx_id (the signing backend would dedupe it away)"
        );

        // The retry's own submission failing must escalate the schedule,
        // not restart it at attempt 1.
        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id,
            error: "retry submission rejected".to_string(),
            failed_at: now,
            classification: MintFailureClassification::Unclassified,
        });

        let Mint::MintingFailed { attempts, failed_from, .. } = &mint else {
            panic!("Expected MintingFailed after a failed retry, got {mint:?}");
        };
        assert_eq!(
            *attempts, 2,
            "the attempt counter must survive the retry transition"
        );
        assert!(
            matches!(failed_from.as_ref(), Mint::Minting { .. })
                && matches!(
                    failed_from.non_failed_predecessor(),
                    Mint::TxSubmitted { .. }
                ),
            "the retry wrapper and its predecessor chain must both survive a \
             failed retry"
        );
    }

    #[test]
    fn retry_failure_after_persisted_intent_keeps_escalating_attempts() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();
        let mut mint = replay::<Mint>(minting_events_for_retry(
            &issuer_request_id,
            "mint-retry-seed".to_string(),
            now - chrono::Duration::hours(2),
        ))
        .unwrap()
        .unwrap();
        mint.apply_event(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            manual_retry_id: None,
            started_at: now,
        });
        mint.apply_event(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: PreparedMintTx::valid_for_test(
                1,
                format!("mint-{issuer_request_id}-retry-1"),
            ),
            intended_at: now,
        });
        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id,
            error: "retry submission outcome unknown".to_string(),
            failed_at: now,
            classification: MintFailureClassification::Unclassified,
        });

        let Mint::MintingFailed { attempts, .. } = mint else {
            panic!("expected MintingFailed");
        };
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn record_callback_sent_from_callback_pending_completes() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tokens_minted(&issuer_request_id))
            .when(MintCommand::RecordCallbackSent {
                issuer_request_id: issuer_request_id.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], MintEvent::MintCompleted { .. }));
    }

    #[tokio::test]
    async fn record_callback_sent_is_idempotent_once_completed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_completed(&issuer_request_id))
            .when(MintCommand::RecordCallbackSent { issuer_request_id })
            .await
            .events();

        assert!(
            events.is_empty(),
            "re-recording the callback for a completed mint must be a no-op"
        );
    }

    #[tokio::test]
    async fn record_mint_failed_from_minting_emits_failed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::RecordMintFailed {
                issuer_request_id,
                error: "submission rejected".to_string(),
                classification: MintFailureClassification::Unclassified,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MintEvent::MintingFailed { error, .. }
                if error == "submission rejected"
        ));
    }

    #[tokio::test]
    async fn record_mint_failed_from_tx_submitted_emits_failed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordMintFailed {
                issuer_request_id,
                error: "confirmation failed".to_string(),
                classification: MintFailureClassification::Unclassified,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MintEvent::MintingFailed { error, .. }
                if error == "confirmation failed"
        ));
    }

    #[tokio::test]
    async fn record_mint_failed_is_ignored_once_completed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_completed(&issuer_request_id))
            .when(MintCommand::RecordMintFailed {
                issuer_request_id,
                error: "stale failure report".to_string(),
                classification: MintFailureClassification::Unclassified,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "a stale failure report for a completed mint must be ignored"
        );
    }

    #[tokio::test]
    async fn retry_mint_from_minting_failed_emits_retry_started() {
        let issuer_request_id = IssuerMintRequestId::random();
        let failed_at = Utc::now() - chrono::Duration::hours(2);

        let events = TestHarness::<Mint>::with(())
            .given(minting_events_for_retry(
                &issuer_request_id,
                "ext-1".to_string(),
                failed_at,
            ))
            .when(MintCommand::RetryMint { issuer_request_id })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MintEvent::MintRetryStarted { tx_hash: None, .. }
        ));
    }

    #[tokio::test]
    async fn retry_mint_is_idempotent_when_not_failed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::RetryMint { issuer_request_id })
            .await
            .events();

        assert!(
            events.is_empty(),
            "retrying a mint that is not in MintingFailed must be a no-op"
        );
    }

    prop_compose! {
        pub(crate) fn arb_issuer_request_id()(bytes in any::<[u8; 16]>()) -> IssuerMintRequestId {
            IssuerMintRequestId::new(Uuid::from_bytes(bytes))
        }
    }

    #[tokio::test]
    async fn test_initiate_mint_creates_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<Mint>::with(())
            .given_no_previous_events()
            .when(MintCommand::Initiate {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network,
                client_id,
                wallet,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        match &events[0] {
            MintEvent::Initiated {
                issuer_request_id: event_issuer_id,
                tokenization_request_id: event_tokenization_id,
                quantity: event_quantity,
                underlying: event_underlying,
                token: event_token,
                network: event_network,
                client_id: event_client_id,
                wallet: event_wallet,
                initiated_at,
                mint_mode,
            } => {
                assert_eq!(event_issuer_id, &issuer_request_id);
                assert_eq!(event_tokenization_id, &tokenization_request_id);
                assert_eq!(event_quantity, &quantity);
                assert_eq!(event_underlying, &underlying);
                assert_eq!(event_token, &token);
                assert_eq!(event_network, &network);
                assert_eq!(event_client_id, &client_id);
                assert_eq!(event_wallet, &wallet);
                assert!(initiated_at.timestamp() > 0);
                assert_eq!(mint_mode, &VaultMode::VaultDirect);
            }
            MintEvent::JournalConfirmed { .. }
            | MintEvent::JournalRejected { .. }
            | MintEvent::MintingStarted { .. }
            | MintEvent::MintTxIntended { .. }
            | MintEvent::MintTxSubmitted { .. }
            | MintEvent::TokensMinted { .. }
            | MintEvent::MintingFailed { .. }
            | MintEvent::MintCompleted { .. }
            | MintEvent::ExistingMintRecovered { .. }
            | MintEvent::MintRetryStarted { .. }
            | MintEvent::MintAuthorizationReceived { .. }
            | MintEvent::OrchestratorTokensMinted { .. }
            | MintEvent::OrchestratorMintRecovered { .. }
            | MintEvent::MintClosed { .. } => {
                panic!("Expected MintInitiated event, got {:?}", &events[0])
            }
        }
    }

    /// Historic `Initiated` events predate `mint_mode` entirely; they must
    /// replay as `VaultDirect` — the only mode that existed when they were
    /// written.
    #[test]
    fn initiated_event_without_mint_mode_replays_as_vault_direct() {
        let historic = serde_json::json!({
            "Initiated": {
                "issuer_request_id": IssuerMintRequestId::random().to_string(),
                "tokenization_request_id": "alp-legacy-1",
                "quantity": "100",
                "underlying": "AAPL",
                "token": "tAAPL",
                "network": "base",
                "client_id": ClientId::new(),
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "initiated_at": "2025-01-01T00:00:00Z"
            }
        });

        let event: MintEvent = serde_json::from_value(historic)
            .expect("historic Initiated event must deserialize");
        let MintEvent::Initiated { mint_mode, .. } = &event else {
            panic!("expected Initiated, got {event:?}");
        };
        assert_eq!(mint_mode, &VaultMode::VaultDirect);

        let mint = replay::<Mint>(vec![event])
            .expect("historic event must replay")
            .expect("mint must exist");
        assert!(matches!(
            mint,
            Mint::Initiated { mint_mode: VaultMode::VaultDirect, .. }
        ));
    }

    /// Pre-orchestrator aggregate snapshots lack `mint_mode`; deserializing
    /// them must default to `VaultDirect` rather than fail.
    #[test]
    fn state_snapshot_without_mint_mode_deserializes_as_vault_direct() {
        let mint = Mint::Initiated {
            mint_authorization: None,
            issuer_request_id: IssuerMintRequestId::random(),
            tokenization_request_id: TokenizationRequestId::new("alp-snap-1"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: Utc::now(),
            mint_mode: VaultMode::Orchestrator {
                address: address!("0x00000000000000000000000000000000000000aa"),
            },
        };

        let mut old_snapshot =
            serde_json::to_value(&mint).expect("state must serialize");
        old_snapshot
            .pointer_mut("/Initiated")
            .and_then(serde_json::Value::as_object_mut)
            .expect("Initiated snapshot object")
            .remove("mint_mode");

        let restored: Mint = serde_json::from_value(old_snapshot)
            .expect("old snapshot must deserialize");
        assert!(matches!(
            restored,
            Mint::Initiated { mint_mode: VaultMode::VaultDirect, .. }
        ));
    }

    /// The mode anchored on `Initiated` flows through every lifecycle
    /// transition: replay derives it from the mint's own event history alone
    /// (live config is not an input to replay), so a mint initiated while its
    /// asset was orchestrator-mode stays orchestrator-mode through
    /// journal-confirm, minting, intent, submission, and failure — regardless
    /// of what the asset's configured vault_mode says later.
    #[test]
    fn mint_mode_anchor_survives_replay_through_lifecycle() {
        let orchestrator = VaultMode::Orchestrator {
            address: address!("0x00000000000000000000000000000000000000aa"),
        };
        let issuer_request_id = IssuerMintRequestId::random();
        let base_events = vec![
            MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new(
                    "alp-anchor-1",
                ),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: Utc::now(),
                mint_mode: orchestrator,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: Utc::now(),
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: Utc::now(),
            },
            MintEvent::MintTxIntended {
                issuer_request_id: issuer_request_id.clone(),
                prepared_tx: PreparedMintTx::default(),
                intended_at: Utc::now(),
            },
            MintEvent::MintTxSubmitted {
                issuer_request_id: issuer_request_id.clone(),
                external_tx_id: "mint-anchor-1".to_string(),
                tx_id: TxId::Hash(B256::repeat_byte(0x22)),
                submitted_at: Utc::now(),
            },
        ];

        let submitted = replay::<Mint>(base_events.clone())
            .expect("lifecycle must replay")
            .expect("mint must exist");
        assert!(
            matches!(
                &submitted,
                Mint::TxSubmitted { mint_mode, .. } if *mint_mode == orchestrator
            ),
            "TxSubmitted must carry the orchestrator anchor, got {submitted:?}"
        );

        let mut failed_events = base_events;
        failed_events.push(MintEvent::MintingFailed {
            issuer_request_id,
            error: "boom".to_string(),
            failed_at: Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });
        let failed = replay::<Mint>(failed_events)
            .expect("failed lifecycle must replay")
            .expect("mint must exist");
        assert!(
            matches!(
                &failed,
                Mint::MintingFailed { mint_mode, .. }
                    if *mint_mode == orchestrator
            ),
            "MintingFailed must carry the orchestrator anchor, got {failed:?}"
        );
    }

    /// The anchor also survives the transitions the lifecycle test does not
    /// reach: the recovery retry (`MintingFailed -> Minting`), the recovered
    /// and terminal success states (`CallbackPending`, `Completed`), and the
    /// journal-rejection terminal. Retry preservation is what keeps a
    /// recovered orchestrator mint on the orchestrator path.
    #[test]
    fn mint_mode_anchor_survives_retry_rejection_and_terminal_states() {
        let orchestrator = VaultMode::Orchestrator {
            address: address!("0x00000000000000000000000000000000000000aa"),
        };
        let issuer_request_id = IssuerMintRequestId::random();
        let initiated = MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("alp-anchor-2"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: Utc::now(),
            mint_mode: orchestrator,
        };

        let mut events = vec![
            initiated.clone(),
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: Utc::now(),
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: Utc::now(),
            },
            MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "boom".to_string(),
                failed_at: Utc::now(),
                classification: MintFailureClassification::Unclassified,
            },
            MintEvent::MintRetryStarted {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: None,
                started_at: Utc::now(),
                manual_retry_id: None,
            },
        ];
        let retried = replay::<Mint>(events.clone())
            .expect("retry lifecycle must replay")
            .expect("mint must exist");
        assert!(
            matches!(
                &retried,
                Mint::Minting { mint_mode, .. } if *mint_mode == orchestrator
            ),
            "the retry transition must preserve the anchor, got {retried:?}"
        );

        events.push(MintEvent::ExistingMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: B256::repeat_byte(0x33),
            receipt_id: uint!(1_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            block_number: 1000,
            recovered_at: Utc::now(),
        });
        let recovered = replay::<Mint>(events.clone())
            .expect("recovered lifecycle must replay")
            .expect("mint must exist");
        assert!(
            matches!(
                &recovered,
                Mint::CallbackPending { mint_mode, .. }
                    if *mint_mode == orchestrator
            ),
            "CallbackPending must carry the anchor, got {recovered:?}"
        );

        events.push(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at: Utc::now(),
        });
        let completed = replay::<Mint>(events)
            .expect("completed lifecycle must replay")
            .expect("mint must exist");
        assert!(
            matches!(
                &completed,
                Mint::Completed { mint_mode, .. }
                    if *mint_mode == orchestrator
            ),
            "Completed must carry the anchor, got {completed:?}"
        );

        let rejected = replay::<Mint>(vec![
            initiated,
            MintEvent::JournalRejected {
                issuer_request_id,
                reason: "insufficient shares".to_string(),
                rejected_at: Utc::now(),
            },
        ])
        .expect("rejected lifecycle must replay")
        .expect("mint must exist");
        assert!(
            matches!(
                &rejected,
                Mint::JournalRejected { mint_mode, .. }
                    if *mint_mode == orchestrator
            ),
            "JournalRejected must carry the anchor, got {rejected:?}"
        );
    }

    fn orchestrator_initiated_event(
        issuer_request_id: &IssuerMintRequestId,
        mint_mode: VaultMode,
    ) -> MintEvent {
        MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("alp-auth-1"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: Utc::now(),
            mint_mode,
        }
    }

    fn test_authorization() -> MintAuthorization {
        MintAuthorization {
            nonce: B256::repeat_byte(0x07),
            signature: Bytes::from_static(&[0xaa; 65]),
        }
    }

    /// `MintAuthorizationReceived` applies only in the states
    /// `handle_authorize_mint` emits from: a hypothetical late emission must
    /// never swap the nonce after the transaction is signed — past intent it
    /// replays as a no-op and the originally bound authorization stays.
    #[test]
    fn late_authorization_event_replays_as_a_noop_past_intent() {
        let issuer_request_id = IssuerMintRequestId::random();
        let orchestrator = VaultMode::Orchestrator {
            address: address!("0x00000000000000000000000000000000000000aa"),
        };

        let history = vec![
            orchestrator_initiated_event(&issuer_request_id, orchestrator),
            MintEvent::MintAuthorizationReceived {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: test_authorization(),
                received_at: Utc::now(),
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: Utc::now(),
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: Utc::now(),
            },
            MintEvent::MintTxIntended {
                issuer_request_id: issuer_request_id.clone(),
                prepared_tx: PreparedMintTx::valid_for_test(
                    1,
                    "mint-late-auth".to_string(),
                ),
                intended_at: Utc::now(),
            },
            // No command emits this past intent; if one ever does, it must
            // not rebind the nonce the signed transaction already carries.
            MintEvent::MintAuthorizationReceived {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: MintAuthorization {
                    nonce: B256::repeat_byte(0x08),
                    signature: Bytes::from_static(&[0xbb; 65]),
                },
                received_at: Utc::now(),
            },
        ];

        let mint = replay::<Mint>(history)
            .expect("history must replay")
            .expect("mint must exist");

        assert!(
            matches!(
                &mint,
                Mint::TxIntended { mint_authorization: Some(authorization), .. }
                    if *authorization == test_authorization()
            ),
            "the late event must not rebind the authorization, got {mint:?}"
        );
    }

    /// `AuthorizeMint` records the authorization on an orchestrator-mode mint
    /// from every pre-intent state — without changing the lifecycle position.
    #[tokio::test]
    async fn authorize_mint_records_authorization_without_lifecycle_change() {
        let issuer_request_id = IssuerMintRequestId::random();
        let orchestrator = VaultMode::Orchestrator {
            address: address!("0x00000000000000000000000000000000000000aa"),
        };
        let initiated =
            orchestrator_initiated_event(&issuer_request_id, orchestrator);
        let journal_confirmed = MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at: Utc::now(),
        };
        let minting_started = MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: Utc::now(),
        };

        let cases: Vec<(Vec<MintEvent>, &str)> = vec![
            (vec![initiated.clone()], "Initiated"),
            (
                vec![initiated.clone(), journal_confirmed.clone()],
                "JournalConfirmed",
            ),
            (vec![initiated, journal_confirmed, minting_started], "Minting"),
        ];

        for (given, expected_state) in cases {
            let events = TestHarness::<Mint>::with(())
                .given(given.clone())
                .when(MintCommand::AuthorizeMint {
                    issuer_request_id: issuer_request_id.clone(),
                    mint_authorization: test_authorization(),
                })
                .await
                .events();

            assert_eq!(events.len(), 1, "from {expected_state}");
            let MintEvent::MintAuthorizationReceived {
                mint_authorization, ..
            } = &events[0]
            else {
                panic!(
                    "expected MintAuthorizationReceived from {expected_state}, \
                     got {:?}",
                    events[0]
                );
            };
            assert_eq!(mint_authorization, &test_authorization());

            // Applying the event sets the authorization but leaves the
            // lifecycle position untouched.
            let mut history = given;
            history.push(events[0].clone());
            let mint = replay::<Mint>(history)
                .expect("history must replay")
                .expect("mint must exist");
            assert_eq!(mint.state_name(), expected_state);
            assert!(matches!(
                &mint,
                Mint::Initiated {
                    mint_authorization: Some(authorization),
                    ..
                } | Mint::JournalConfirmed {
                    mint_authorization: Some(authorization),
                    ..
                } | Mint::Minting {
                    mint_authorization: Some(authorization),
                    ..
                } if *authorization == test_authorization()
            ));
        }
    }

    /// An authorization for a vault-direct mint is meaningless — nothing will
    /// ever consume it — so delivery is rejected and never stored.
    #[tokio::test]
    async fn authorize_mint_rejects_vault_direct_mint() {
        let issuer_request_id = IssuerMintRequestId::random();
        let error = TestHarness::<Mint>::with(())
            .given(vec![orchestrator_initiated_event(
                &issuer_request_id,
                VaultMode::VaultDirect,
            )])
            .when(MintCommand::AuthorizeMint {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: test_authorization(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                &error,
                LifecycleError::Apply(
                    MintError::AuthorizationForVaultDirectAsset { .. }
                )
            ),
            "expected AuthorizationForVaultDirectAsset, got {error:?}"
        );
    }

    /// Redelivery of the identical authorization is a no-op (the liquidity
    /// bot retries deliveries); a different one is rejected so the nonce can
    /// never be swapped mid-flight.
    #[tokio::test]
    async fn authorize_mint_is_idempotent_and_rejects_conflicts() {
        let issuer_request_id = IssuerMintRequestId::random();
        let orchestrator = VaultMode::Orchestrator {
            address: address!("0x00000000000000000000000000000000000000aa"),
        };
        let given = vec![
            orchestrator_initiated_event(&issuer_request_id, orchestrator),
            MintEvent::MintAuthorizationReceived {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: test_authorization(),
                received_at: Utc::now(),
            },
        ];

        let events = TestHarness::<Mint>::with(())
            .given(given.clone())
            .when(MintCommand::AuthorizeMint {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: test_authorization(),
            })
            .await
            .events();
        assert!(
            events.is_empty(),
            "identical redelivery must be a no-op, got {events:?}"
        );

        let conflicting = MintAuthorization {
            nonce: B256::repeat_byte(0x08),
            signature: Bytes::from_static(&[0xbb; 65]),
        };
        let error = TestHarness::<Mint>::with(())
            .given(given)
            .when(MintCommand::AuthorizeMint {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: conflicting,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                &error,
                LifecycleError::Apply(MintError::ConflictingMintAuthorization)
            ),
            "expected ConflictingMintAuthorization, got {error:?}"
        );
    }

    /// Once `PrepareMint` signs, the nonce is baked into the persisted bytes;
    /// a late delivery could not change what gets submitted and is rejected.
    #[tokio::test]
    async fn authorize_mint_rejected_after_transaction_intent() {
        let issuer_request_id = IssuerMintRequestId::random();
        let orchestrator = VaultMode::Orchestrator {
            address: address!("0x00000000000000000000000000000000000000aa"),
        };
        let mut given = vec![
            orchestrator_initiated_event(&issuer_request_id, orchestrator),
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: Utc::now(),
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: Utc::now(),
            },
            MintEvent::MintTxIntended {
                issuer_request_id: issuer_request_id.clone(),
                prepared_tx: PreparedMintTx::default(),
                intended_at: Utc::now(),
            },
        ];

        for expected_state in ["MintIntended", "TxSubmitted"] {
            let error = TestHarness::<Mint>::with(())
                .given(given.clone())
                .when(MintCommand::AuthorizeMint {
                    issuer_request_id: issuer_request_id.clone(),
                    mint_authorization: test_authorization(),
                })
                .await
                .then_expect_error();

            assert!(
                matches!(
                    &error,
                    LifecycleError::Apply(
                        MintError::AuthorizationNotAcceptable {
                            current_state,
                        }
                    ) if current_state == expected_state
                ),
                "expected AuthorizationNotAcceptable from {expected_state}, \
                 got {error:?}"
            );

            given.push(MintEvent::MintTxSubmitted {
                issuer_request_id: issuer_request_id.clone(),
                external_tx_id: "mint-auth-late".to_string(),
                tx_id: TxId::Hash(B256::repeat_byte(0x22)),
                submitted_at: Utc::now(),
            });
        }
    }

    #[tokio::test]
    async fn test_initiate_mint_when_already_initiated_returns_error() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network,
                client_id,
                wallet,
                initiated_at: chrono::Utc::now(),
            }])
            .when(MintCommand::Initiate {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::AlreadyInitiated { .. })
            ),
            "Expected AlreadyInitiated error, got {error:?}"
        );
    }

    #[test]
    fn test_apply_initiated_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(50));
        let underlying = UnderlyingSymbol::new("TSLA").unwrap();
        let token = TokenSymbol::new("tTSLA");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = chrono::Utc::now();

        let mint = replay::<Mint>(vec![MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
        }])
        .unwrap()
        .unwrap();

        let Mint::Initiated {
            mint_authorization: _,
            mint_mode: _,
            issuer_request_id: applied_issuer_id,
            tokenization_request_id: applied_tokenization_id,
            quantity: applied_quantity,
            underlying: applied_underlying,
            token: applied_token,
            network: applied_network,
            client_id: applied_client_id,
            wallet: applied_wallet,
            initiated_at: applied_initiated_at,
        } = mint
        else {
            panic!("Expected Initiated, got Uninitialized")
        };

        assert_eq!(applied_issuer_id, issuer_request_id);
        assert_eq!(applied_tokenization_id, tokenization_request_id);
        assert_eq!(applied_quantity, quantity);
        assert_eq!(applied_underlying, underlying);
        assert_eq!(applied_token, token);
        assert_eq!(applied_network, network);
        assert_eq!(applied_client_id, client_id);
        assert_eq!(applied_wallet, wallet);
        assert_eq!(applied_initiated_at, initiated_at);
    }

    #[test]
    fn test_apply_journal_confirmed_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut mint = Mint::Initiated {
            mint_authorization: None,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
        };

        let confirmed_at = Utc::now();

        mint.apply_event(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        });

        let Mint::JournalConfirmed {
            issuer_request_id: state_issuer_id,
            journal_confirmed_at,
            ..
        } = mint
        else {
            panic!("Expected JournalConfirmed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(journal_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_apply_journal_rejected_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut mint = Mint::Initiated {
            mint_authorization: None,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
        };

        let rejected_at = Utc::now();
        let reason = "Insufficient funds".to_string();

        mint.apply_event(MintEvent::JournalRejected {
            issuer_request_id: issuer_request_id.clone(),
            reason: reason.clone(),
            rejected_at,
        });

        let Mint::JournalRejected {
            issuer_request_id: state_issuer_id,
            reason: state_reason,
            rejected_at: state_rejected_at,
            ..
        } = mint
        else {
            panic!("Expected JournalRejected state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_reason, reason);
        assert_eq!(state_rejected_at, rejected_at);
    }

    #[tokio::test]
    async fn test_confirm_journal_produces_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::ConfirmJournal { issuer_request_id })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], MintEvent::JournalConfirmed { .. }));
    }

    #[tokio::test]
    async fn test_confirm_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given_no_previous_events()
            .when(MintCommand::ConfirmJournal { issuer_request_id })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(MintError::NotInInitiatedState { .. })
        ));
    }

    #[tokio::test]
    async fn test_confirm_journal_for_already_confirmed_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![
                MintEvent::Initiated {
                    mint_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at: Utc::now(),
                },
                MintEvent::JournalConfirmed {
                    issuer_request_id: issuer_request_id.clone(),
                    confirmed_at: Utc::now(),
                },
            ])
            .when(MintCommand::ConfirmJournal { issuer_request_id })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(MintError::NotInInitiatedState { .. })
        ));
    }

    #[tokio::test]
    async fn test_reject_journal_produces_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let reason = "Insufficient funds";

        let events = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::RejectJournal {
                issuer_request_id,
                reason: reason.to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let MintEvent::JournalRejected { reason: event_reason, .. } =
            &events[0]
        else {
            panic!("Expected JournalRejected event, got {:?}", &events[0]);
        };

        assert_eq!(event_reason, reason);
    }

    #[tokio::test]
    async fn test_reject_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given_no_previous_events()
            .when(MintCommand::RejectJournal {
                issuer_request_id,
                reason: "Test reason".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(MintError::NotInInitiatedState { .. })
        ));
    }

    #[tokio::test]
    async fn test_confirm_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id = IssuerMintRequestId::random();
        let wrong_issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: correct_issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::ConfirmJournal {
                issuer_request_id: wrong_issuer_request_id,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(
                    MintError::IssuerMintRequestIdMismatch { .. }
                )
            ),
            "Expected IssuerMintRequestIdMismatch error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn test_reject_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id = IssuerMintRequestId::random();
        let wrong_issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: correct_issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::RejectJournal {
                issuer_request_id: wrong_issuer_request_id,
                reason: "Test reason".to_string(),
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(
                    MintError::IssuerMintRequestIdMismatch { .. }
                )
            ),
            "Expected IssuerMintRequestIdMismatch error, got {error:?}"
        );
    }

    #[test]
    fn test_apply_minting_started_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut mint = Mint::JournalConfirmed {
            mint_authorization: None,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
            journal_confirmed_at,
        };

        let minting_started_at = Utc::now();
        mint.apply_event(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: minting_started_at,
        });

        let Mint::Minting {
            mint_authorization: _,
            mint_mode: _,
            issuer_request_id: state_issuer_id,
            tokenization_request_id: state_tok_id,
            quantity: state_quantity,
            underlying: state_underlying,
            token: state_token,
            network: state_network,
            client_id: state_client_id,
            wallet: state_wallet,
            initiated_at: state_initiated_at,
            journal_confirmed_at: state_journal_confirmed_at,
            minting_started_at: state_minting_started_at,
            retry: state_retry,
        } = mint
        else {
            panic!("Expected Minting state, got {mint:?}");
        };
        assert!(
            state_retry.is_none(),
            "a first MintingStarted must not carry retry history"
        );

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tok_id, tokenization_request_id);
        assert_eq!(state_quantity, quantity);
        assert_eq!(state_underlying, underlying);
        assert_eq!(state_token, token);
        assert_eq!(state_network, network);
        assert_eq!(state_client_id, client_id);
        assert_eq!(state_wallet, wallet);
        assert_eq!(state_initiated_at, initiated_at);
        assert_eq!(state_journal_confirmed_at, journal_confirmed_at);
        assert_eq!(state_minting_started_at, minting_started_at);
    }

    #[test]
    fn test_apply_tokens_minted_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut mint = Mint::Minting {
            mint_authorization: None,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
            journal_confirmed_at,
            minting_started_at,
            retry: None,
        };

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        mint.apply_event(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        });

        let Mint::CallbackPending {
            issuer_request_id: state_issuer_id,
            tx_hash: state_tx_hash,
            receipt_id: state_receipt_id,
            shares_minted: state_shares_minted,
            gas_used: state_gas_used,
            block_number: state_block_number,
            minted_at: state_minted_at,
            ..
        } = mint
        else {
            panic!("Expected CallbackPending state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_receipt_id, Some(receipt_id));
        assert_eq!(state_shares_minted, shares_minted);
        assert_eq!(state_gas_used, Some(gas_used));
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_minted_at, minted_at);
    }

    #[test]
    fn test_apply_minting_failed_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut mint = Mint::Minting {
            mint_authorization: None,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
            journal_confirmed_at,
            minting_started_at,
            retry: None,
        };

        let error_message = "Transaction failed: insufficient gas";
        let failed_at = Utc::now();

        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: error_message.to_string(),
            failed_at,
            classification: MintFailureClassification::Unclassified,
        });

        let Mint::MintingFailed {
            issuer_request_id: state_issuer_id,
            error: state_error,
            failed_at: state_failed_at,
            failed_from,
            ..
        } = &mint
        else {
            panic!("Expected MintingFailed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, &issuer_request_id);
        assert_eq!(state_error, error_message);
        assert_eq!(state_failed_at, &failed_at);
        assert!(
            matches!(**failed_from, Mint::Minting { .. }),
            "Expected failed_from to be Minting, got {:?}",
            failed_from.state_name()
        );
        assert!(
            matches!(mint.non_failed_predecessor(), Mint::Minting { .. }),
            "Expected non_failed_predecessor to be Minting"
        );
    }

    #[test]
    fn test_apply_mint_completed_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        let mut mint = Mint::CallbackPending {
            mint_authorization: None,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
            journal_confirmed_at,
            tx_hash,
            receipt_id: Some(receipt_id),
            mint_nonce: None,
            shares_minted,
            gas_used: Some(gas_used),
            block_number,
            minted_at,
        };

        let completed_at = Utc::now();

        mint.apply_event(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at,
        });

        let Mint::Completed {
            issuer_request_id: state_issuer_id,
            tx_hash: state_tx_hash,
            receipt_id: state_receipt_id,
            shares_minted: state_shares_minted,
            gas_used: state_gas_used,
            block_number: state_block_number,
            minted_at: state_minted_at,
            completed_at: state_completed_at,
            ..
        } = mint
        else {
            panic!("Expected Completed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_receipt_id, Some(receipt_id));
        assert_eq!(state_shares_minted, shares_minted);
        assert_eq!(state_gas_used, Some(gas_used));
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_minted_at, minted_at);
        assert_eq!(state_completed_at, completed_at);
    }

    #[test]
    fn test_full_mint_flow_to_completed() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-flow-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();
        let completed_at = Utc::now();

        let mut mint = replay::<Mint>(vec![MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            mint_mode: VaultMode::VaultDirect,
        }])
        .unwrap()
        .unwrap();

        assert!(
            matches!(mint, Mint::Initiated { .. }),
            "Expected Initiated state, got {mint:?}"
        );

        mint.apply_event(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        });

        assert!(
            matches!(mint, Mint::JournalConfirmed { .. }),
            "Expected JournalConfirmed state, got {mint:?}"
        );

        let minting_started_at = Utc::now();
        mint.apply_event(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: minting_started_at,
        });

        assert!(
            matches!(mint, Mint::Minting { .. }),
            "Expected Minting state, got {mint:?}"
        );

        mint.apply_event(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        });

        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "Expected CallbackPending state, got {mint:?}"
        );

        mint.apply_event(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at,
        });

        let Mint::Completed {
            issuer_request_id: final_id,
            completed_at: final_completed_at,
            ..
        } = mint
        else {
            panic!("Expected Completed state, got {mint:?}");
        };

        assert_eq!(final_id, issuer_request_id);
        assert_eq!(final_completed_at, completed_at);
    }

    /// Regression: pre-event-sorcery snapshot payloads (`{"Completed": ...}`)
    /// must be cleared by schema reconciliation before projection catch-up
    /// calls `load_with_context`, which deserializes into `Lifecycle<Mint>`.
    #[traced_test]
    #[tokio::test]
    async fn mint_pre_lifecycle_snapshot_cleared_before_projection_catch_up() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let mint_id = IssuerMintRequestId::new(uuid!(
            "550e8400-e29b-41d4-a716-446655440000"
        ));
        let mint_id_str = mint_id.to_string();
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
                "VersionUpdated": { "name": "Mint", "version": 1 }
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
                'Mint',
                ?,
                1,
                'MintEvent::Initiated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(mint_id_str.as_str())
        .bind(
            serde_json::json!({
                "Initiated": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
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
                'Mint',
                ?,
                1,
                0,
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ",
        )
        .bind(mint_id_str.as_str())
        .bind(
            serde_json::json!({
                "Completed": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
                    "journal_confirmed_at": now,
                    "tx_hash": "0xbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00d",
                    "receipt_id": "1",
                    "shares_minted": "1000000000000000000",
                    "gas_used": 21000,
                    "block_number": 1,
                    "minted_at": now,
                    "completed_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        let (asset_store, _asset_projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        asset_store
            .send(
                &AssetKey::new(
                    UnderlyingSymbol::new("AAPL").unwrap(),
                    Network::Base,
                ),
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    vault: VAULT,
                },
            )
            .await
            .unwrap();

        prepare_event_sourced_startup::<Mint>(&pool).await.unwrap();
        StoreBuilder::<Mint>::new(pool.clone()).build(()).await.unwrap();

        assert!(logs_contain_at!(
            Level::INFO,
            &["Cleared stale snapshots", "Mint"]
        ));

        let stale_snapshot_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM snapshots
            WHERE aggregate_type = 'Mint'
              AND aggregate_id = ?
            ",
        )
        .bind(mint_id_str.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            stale_snapshot_count, 0,
            "Schema reconciliation must delete incompatible Mint snapshots"
        );
    }

    async fn seed_pre_lifecycle_mint_view_row(
        pool: &Pool<Sqlite>,
        mint_id: &str,
        version: i64,
        payload: &serde_json::Value,
    ) {
        sqlx::query(
            "
            INSERT INTO mint_view (view_id, version, payload)
            VALUES (?, ?, ?)
            ",
        )
        .bind(mint_id)
        .bind(version)
        .bind(payload.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    /// Regression: pre-event-sorcery `mint_view` payloads (`{"Completed": ...}`)
    /// must be cleared on schema version change before projection catch-up calls
    /// `load_with_context`, which deserializes into `Lifecycle<Mint>`.
    #[tokio::test]
    async fn pre_lifecycle_mint_view_cleared_before_projection_catch_up() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let mint_id = IssuerMintRequestId::new(uuid!(
            "550e8400-e29b-41d4-a716-446655440000"
        ));
        let mint_id_str = mint_id.to_string();
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
                "VersionUpdated": { "name": "Mint", "version": 1 }
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
                'Mint',
                ?,
                1,
                'MintEvent::Initiated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(mint_id_str.as_str())
        .bind(
            serde_json::json!({
                "Initiated": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale-view",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        seed_pre_lifecycle_mint_view_row(
            &pool,
            &mint_id_str,
            1,
            &serde_json::json!({
                "Completed": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale-view",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
                    "journal_confirmed_at": now,
                    "tx_hash": "0xbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00d",
                    "receipt_id": "1",
                    "shares_minted": "1000000000000000000",
                    "gas_used": 21000,
                    "block_number": 1,
                    "minted_at": now,
                    "completed_at": now,
                }
            }),
        )
        .await;

        let (asset_store, _asset_projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        asset_store
            .send(
                &AssetKey::new(
                    UnderlyingSymbol::new("AAPL").unwrap(),
                    Network::Base,
                ),
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    vault: VAULT,
                },
            )
            .await
            .unwrap();

        prepare_event_sourced_startup::<Mint>(&pool).await.unwrap();
        StoreBuilder::<Mint>::new(pool.clone()).build(()).await.unwrap();

        let view_payload: String = sqlx::query_scalar(
            "
            SELECT payload
            FROM mint_view
            WHERE view_id = ?
            ",
        )
        .bind(mint_id_str.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();

        let payload: serde_json::Value =
            serde_json::from_str(&view_payload).unwrap();
        assert!(
            payload
                .get("Live")
                .and_then(|live| live.get("Initiated"))
                .is_some(),
            "Projection catch-up must rebuild mint_view with Lifecycle payload, got {payload}"
        );
    }

    #[test]
    fn test_issuer_request_id_display() {
        let uuid = Uuid::new_v4();
        let id = IssuerMintRequestId::new(uuid);
        assert_eq!(format!("{id}"), uuid.to_string());
    }

    #[test]
    fn test_tokenization_request_id_display() {
        let id = TokenizationRequestId::new("alp-456");
        assert_eq!(format!("{id}"), "alp-456");
    }

    #[tokio::test]
    async fn close_mint_without_prepared_identity_needs_no_acknowledgement() {
        let issuer_request_id = IssuerMintRequestId::random();
        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::CloseMint {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator closed pre-prepare".to_string(),
                acknowledged_unresolved_mint_tx_hash: None,
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [MintEvent::MintClosed {
                issuer_request_id: closed_id,
                acknowledged_unresolved_mint_tx_hash: None,
                ..
            }] if closed_id == &issuer_request_id
        ));
    }

    /// A legacy `TxSubmitted { prepared_tx: None }` has no prepared bytes, but
    /// a hash `tx_id` is still a broadcast identity recovery keeps polling.
    /// Closing it must not be cheaper than closing an intended mint.
    #[tokio::test]
    async fn close_mint_with_legacy_submitted_hash_requires_acknowledgement() {
        let issuer_request_id = IssuerMintRequestId::random();
        let submitted_hash = B256::random();
        let mut seed = events_through_minting(&issuer_request_id);
        seed.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: "ext-legacy".to_string(),
            tx_id: TxId::Hash(submitted_hash),
            submitted_at: Utc::now(),
        });

        let missing = TestHarness::<Mint>::with(())
            .given(seed.clone())
            .when(MintCommand::CloseMint {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator closed legacy submitted".to_string(),
                acknowledged_unresolved_mint_tx_hash: None,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                missing,
                LifecycleError::Apply(
                    MintError::UnresolvedMintRequiresAcknowledgement {
                        mint_tx_hash
                    }
                ) if mint_tx_hash == submitted_hash
            ),
            "legacy submitted close without ack must fail, got {missing:?}"
        );

        let wrong_hash = B256::random();
        let wrong = TestHarness::<Mint>::with(())
            .given(seed.clone())
            .when(MintCommand::CloseMint {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator closed legacy submitted".to_string(),
                acknowledged_unresolved_mint_tx_hash: Some(wrong_hash),
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                wrong,
                LifecycleError::Apply(
                    MintError::UnresolvedMintAcknowledgementMismatch {
                        expected,
                        provided,
                    }
                ) if expected == submitted_hash && provided == wrong_hash
            ),
            "mismatched ack must fail, got {wrong:?}"
        );

        let events = TestHarness::<Mint>::with(())
            .given(seed)
            .when(MintCommand::CloseMint {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator closed legacy submitted".to_string(),
                acknowledged_unresolved_mint_tx_hash: Some(submitted_hash),
            })
            .await
            .events();
        assert!(matches!(
            events.as_slice(),
            [MintEvent::MintClosed {
                acknowledged_unresolved_mint_tx_hash: Some(acknowledged),
                ..
            }] if acknowledged == &submitted_hash
        ));
    }

    #[tokio::test]
    async fn close_mint_with_prepared_identity_requires_matching_hash() {
        let issuer_request_id = IssuerMintRequestId::random();
        let seed = events_through_tx_intended(&issuer_request_id);
        let prepared_hash = match &seed[3] {
            MintEvent::MintTxIntended { prepared_tx, .. } => prepared_tx.hash,
            other => panic!("expected MintTxIntended, got {other:?}"),
        };

        let missing = TestHarness::<Mint>::with(())
            .given(seed.clone())
            .when(MintCommand::CloseMint {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator closed intended".to_string(),
                acknowledged_unresolved_mint_tx_hash: None,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                missing,
                LifecycleError::Apply(
                    MintError::UnresolvedMintRequiresAcknowledgement {
                        mint_tx_hash
                    }
                ) if mint_tx_hash == prepared_hash
            ),
            "missing ack must fail, got {missing:?}"
        );

        let wrong = TestHarness::<Mint>::with(())
            .given(seed.clone())
            .when(MintCommand::CloseMint {
                issuer_request_id: issuer_request_id.clone(),
                reason: "wrong hash".to_string(),
                acknowledged_unresolved_mint_tx_hash: Some(b256!(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                )),
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                wrong,
                LifecycleError::Apply(
                    MintError::UnresolvedMintAcknowledgementMismatch {
                        expected,
                        ..
                    }
                ) if expected == prepared_hash
            ),
            "mismatched ack must fail, got {wrong:?}"
        );

        let events = TestHarness::<Mint>::with(())
            .given(seed)
            .when(MintCommand::CloseMint {
                issuer_request_id: issuer_request_id.clone(),
                reason: "operator closed intended".to_string(),
                acknowledged_unresolved_mint_tx_hash: Some(prepared_hash),
            })
            .await
            .events();
        assert!(matches!(
            events.as_slice(),
            [MintEvent::MintClosed {
                issuer_request_id: closed_id,
                acknowledged_unresolved_mint_tx_hash: Some(ack),
                ..
            }] if closed_id == &issuer_request_id && *ack == prepared_hash
        ));
    }

    #[tokio::test]
    async fn close_mint_rejects_unexpected_acknowledgement() {
        let issuer_request_id = IssuerMintRequestId::random();
        let unexpected = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        let error = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::CloseMint {
                issuer_request_id,
                reason: "ack without intent".to_string(),
                acknowledged_unresolved_mint_tx_hash: Some(unexpected),
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(
                    MintError::UnexpectedUnresolvedMintAcknowledgement {
                        provided
                    }
                ) if provided == unexpected
            ),
            "unexpected ack must fail, got {error:?}"
        );
    }

    /// Submission failures that produce no TxSubmitted event (pre-acceptance)
    /// must still escalate the retry schedule and eventually exhaust, while the
    /// external_tx_id stays at retry-1 so resubmission is idempotent.
    #[test]
    fn pre_acceptance_failures_escalate_attempts_but_reuse_external_id() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();
        let mut mint = replay::<Mint>(vec![
            MintEvent::Initiated {
                mint_mode: VaultMode::VaultDirect,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: now,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: now,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: now,
            },
        ])
        .unwrap()
        .unwrap();

        // failed_at far in the past so every retry window has elapsed; the
        // decision then turns only on the escalating attempt counter.
        let failed_at = now - ChronoDuration::hours(3);

        // First submission failure from Minting: attempts = 1, retryable.
        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "submission rejected".to_string(),
            failed_at,
            classification: MintFailureClassification::Unclassified,
        });
        assert_eq!(
            mint.automatic_retry_decision(Utc::now()),
            AutomaticRetryDecision::Ready
        );

        // Three more pre-acceptance failures push attempts to 4 (still the last
        // retryable attempt), then a fifth exhausts the automatic schedule —
        // proving the counter escalates without a TxSubmitted record.
        for _ in 0..3 {
            mint.apply_event(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
                classification: MintFailureClassification::Unclassified,
            });
        }
        assert_eq!(
            mint.automatic_retry_decision(Utc::now()),
            AutomaticRetryDecision::Ready
        );

        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id,
            error: "submission rejected".to_string(),
            failed_at,
            classification: MintFailureClassification::Unclassified,
        });
        assert_eq!(
            mint.automatic_retry_decision(Utc::now()),
            AutomaticRetryDecision::Exhausted
        );

        // The external_tx_id attempt never advanced — retries reuse retry-1.
        assert_eq!(mint.next_retry_attempt(), 1);
    }

    /// An environment-wide halt (`VaultLogicMismatch`/`ReceiptLogicMismatch`)
    /// must not advance the attempt counter (SPEC "Failure States"): the halt
    /// resolves by upgrade, so re-failures during the halt window leave the
    /// per-mint budget untouched, while an unclassified failure still
    /// escalates the schedule.
    #[test]
    fn halt_classified_failures_do_not_advance_attempts() {
        let issuer_request_id = IssuerMintRequestId::random();
        let failed_at = Utc::now();
        let mut mint = replay::<Mint>(
            orchestrator_events_through_tx_submitted(&issuer_request_id),
        )
        .expect("orchestrator mint must replay")
        .expect("mint must exist");

        for _ in 0..6 {
            mint.apply_event(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "vault logic mismatch".to_string(),
                failed_at,
                classification: MintFailureClassification::VaultLogicMismatch,
            });
        }
        let Mint::MintingFailed { attempts, .. } = &mint else {
            panic!("expected MintingFailed, got {mint:?}");
        };
        assert_eq!(
            *attempts, 1,
            "halt re-failures must leave the attempt counter untouched"
        );

        // A retry cycle that fails back to a halt keeps the preserved
        // counter too.
        mint.apply_event(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            started_at: failed_at,
            manual_retry_id: None,
        });
        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "vault logic mismatch".to_string(),
            failed_at,
            classification: MintFailureClassification::VaultLogicMismatch,
        });
        let Mint::MintingFailed { attempts, .. } = &mint else {
            panic!("expected MintingFailed, got {mint:?}");
        };
        assert_eq!(
            *attempts, 1,
            "a retry that failed back to a halt must not escalate"
        );

        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id,
            error: "timeout".to_string(),
            failed_at,
            classification: MintFailureClassification::Unclassified,
        });
        let Mint::MintingFailed { attempts, .. } = &mint else {
            panic!("expected MintingFailed, got {mint:?}");
        };
        assert_eq!(
            *attempts, 2,
            "an unclassified failure must still escalate the schedule"
        );
    }

    /// Every deterministic orchestrator revert decodes to its typed
    /// classification; only the reasons that prove nothing about a retry
    /// (`NonceReplayed` without an authorization to full-match,
    /// `InsufficientReceipts`, `Unknown`) stay `Unclassified`.
    #[test]
    fn orchestrator_revert_reasons_classify_deterministic_failures() {
        let cases = [
            (
                OrchestratorRevertReason::VaultLogicMismatch,
                MintFailureClassification::VaultLogicMismatch,
            ),
            (
                OrchestratorRevertReason::ReceiptLogicMismatch,
                MintFailureClassification::ReceiptLogicMismatch,
            ),
            (
                OrchestratorRevertReason::BadRecipientSignature,
                MintFailureClassification::BadRecipientSignature,
            ),
            (
                OrchestratorRevertReason::RecipientCallbackRejected {
                    recipient: Address::ZERO,
                },
                MintFailureClassification::RecipientCallbackRejected,
            ),
            (
                OrchestratorRevertReason::VaultAmountMismatch {
                    expected: uint!(2_U256),
                    actual: uint!(1_U256),
                },
                MintFailureClassification::VaultAmountMismatch,
            ),
            (
                OrchestratorRevertReason::NonceReplayed {
                    to: Address::ZERO,
                    nonce: B256::ZERO,
                },
                MintFailureClassification::Unclassified,
            ),
            (
                OrchestratorRevertReason::InsufficientReceipts {
                    token: Address::ZERO,
                    shortfall: uint!(1_U256),
                },
                MintFailureClassification::Unclassified,
            ),
            (
                OrchestratorRevertReason::Unknown,
                MintFailureClassification::Unclassified,
            ),
        ];

        for (reason, expected) in cases {
            let error = VaultError::OrchestratorReverted {
                tx_hash: B256::ZERO,
                reason,
            };
            assert_eq!(
                orchestrator_mint_failure_classification(&error),
                expected,
                "revert reason {reason:?} must classify as {expected:?}"
            );
        }
    }

    /// A vault-direct confirmation result can never complete an orchestrator
    /// mint, and vice versa — the record handlers enforce the persisted
    /// mode anchor.
    #[tokio::test]
    async fn record_handlers_reject_cross_mode_results() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given(orchestrator_events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordTokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_id: TxId::Legacy("fb-1".to_string()),
                tx_hash: B256::ZERO,
                receipt_id: uint!(1_U256),
                shares_minted: uint!(1_U256),
                gas_used: 1,
                block_number: 1,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::MintModeMismatch { .. })
            ),
            "a vault-direct result must not complete an orchestrator mint, \
             got {error:?}"
        );

        let error = TestHarness::<Mint>::with(())
            .given(events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordOrchestratorTokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_id: TxId::Legacy("fb-1".to_string()),
                tx_hash: B256::ZERO,
                nonce: B256::ZERO,
                shares_minted: uint!(1_U256),
                gas_used: 1,
                block_number: 1,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::MintModeMismatch { .. })
            ),
            "an orchestrator result must not complete a vault-direct mint, \
             got {error:?}"
        );
    }

    /// A re-delivered orchestrator confirmation after the mint already
    /// advanced is a no-op, mirroring the vault-direct record handlers.
    #[tokio::test]
    async fn record_orchestrator_tokens_minted_noop_past_tx_submitted() {
        let issuer_request_id = IssuerMintRequestId::random();
        let mut events =
            orchestrator_events_through_tx_submitted(&issuer_request_id);
        events.push(MintEvent::OrchestratorTokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: B256::ZERO,
            nonce: test_mint_authorization().nonce,
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: 21_000,
            block_number: 1_000,
            minted_at: Utc::now(),
        });

        let events = TestHarness::<Mint>::with(())
            .given(events)
            .when(MintCommand::RecordOrchestratorTokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_id: TxId::Legacy("fb-1".to_string()),
                tx_hash: B256::ZERO,
                nonce: test_mint_authorization().nonce,
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 21_000,
                block_number: 1_000,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "a re-delivered orchestrator confirmation must be a no-op"
        );
    }

    /// The chain-reported `(nonce, shares)` must equal the recorded
    /// authorization and journaled quantity — a divergence is a contract
    /// anomaly that fails loudly instead of becoming the audit record.
    #[tokio::test]
    async fn record_orchestrator_tokens_minted_rejects_diverging_values() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given(orchestrator_events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordOrchestratorTokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_id: TxId::Legacy("fb-1".to_string()),
                tx_hash: B256::ZERO,
                nonce: B256::repeat_byte(0xEE),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 21_000,
                block_number: 1_000,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::MintedNonceMismatch { .. })
            ),
            "a diverging nonce must be rejected, got {error:?}"
        );

        let error = TestHarness::<Mint>::with(())
            .given(orchestrator_events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordOrchestratorTokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_id: TxId::Legacy("fb-1".to_string()),
                tx_hash: B256::ZERO,
                nonce: test_mint_authorization().nonce,
                shares_minted: uint!(1_U256),
                gas_used: 21_000,
                block_number: 1_000,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::MintedSharesMismatch { .. })
            ),
            "diverging shares must be rejected, got {error:?}"
        );
    }

    /// The recovered-mint path enforces its guards through the command
    /// handler, not just apply: a vault-direct mint rejects the cross-mode
    /// completion and a mint already past `TxSubmitted` no-ops.
    #[tokio::test]
    async fn record_orchestrator_mint_recovered_guards_via_command() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given(events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordOrchestratorMintRecovered {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: B256::ZERO,
                nonce: B256::ZERO,
                shares_minted: uint!(1_U256),
                block_number: 1,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::MintModeMismatch { .. })
            ),
            "an orchestrator recovery must not complete a vault-direct mint, \
             got {error:?}"
        );

        let mut events =
            orchestrator_events_through_tx_submitted(&issuer_request_id);
        events.push(MintEvent::OrchestratorMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: B256::ZERO,
            nonce: test_mint_authorization().nonce,
            shares_minted: uint!(100_000000000000000000_U256),
            block_number: 777,
            recovered_at: Utc::now(),
        });
        let events = TestHarness::<Mint>::with(())
            .given(events)
            .when(MintCommand::RecordOrchestratorMintRecovered {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: B256::ZERO,
                nonce: test_mint_authorization().nonce,
                shares_minted: uint!(100_000000000000000000_U256),
                block_number: 777,
            })
            .await
            .events();
        assert!(events.is_empty(), "a re-delivered recovery must be a no-op");
    }

    /// A vault receipt is a vault-direct proof: the bot never custodies a
    /// receipt for an orchestrator mint, so an existing-receipt record for
    /// one is a cross-mode anomaly the handler must refuse.
    #[tokio::test]
    async fn record_existing_mint_rejects_orchestrator_anchored_mint() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given(orchestrator_events_through_tx_submitted(&issuer_request_id))
            .when(MintCommand::RecordExistingMint {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: B256::ZERO,
                receipt_id: uint!(1_U256),
                shares_minted: uint!(1_U256),
                block_number: 1,
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::MintModeMismatch { .. })
            ),
            "a vault receipt record must not complete an orchestrator mint, \
             got {error:?}"
        );
    }

    /// `OrchestratorTokensMinted` and `OrchestratorMintRecovered` replay into
    /// `CallbackPending` with the nonce as audit data and no receipt id.
    #[test]
    fn orchestrator_mint_events_replay_to_callback_pending() {
        let issuer_request_id = IssuerMintRequestId::random();
        let nonce = test_mint_authorization().nonce;

        let mut minted_events =
            orchestrator_events_through_tx_submitted(&issuer_request_id);
        minted_events.push(MintEvent::OrchestratorTokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: B256::ZERO,
            nonce,
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: 21_000,
            block_number: 1_000,
            minted_at: Utc::now(),
        });
        let minted = replay::<Mint>(minted_events)
            .expect("orchestrator mint must replay")
            .expect("mint must exist");
        assert!(matches!(
            &minted,
            Mint::CallbackPending {
                receipt_id: None,
                mint_nonce: Some(event_nonce),
                gas_used: Some(21_000),
                ..
            } if *event_nonce == nonce
        ));

        let mut recovered_events =
            orchestrator_events_through_tx_submitted(&issuer_request_id);
        recovered_events.push(MintEvent::OrchestratorMintRecovered {
            issuer_request_id,
            tx_hash: B256::ZERO,
            nonce,
            shares_minted: uint!(100_000000000000000000_U256),
            block_number: 777,
            recovered_at: Utc::now(),
        });
        let recovered = replay::<Mint>(recovered_events)
            .expect("recovered orchestrator mint must replay")
            .expect("mint must exist");
        assert!(matches!(
            &recovered,
            Mint::CallbackPending {
                receipt_id: None,
                mint_nonce: Some(event_nonce),
                gas_used: None,
                block_number: 777,
                ..
            } if *event_nonce == nonce
        ));
    }

    /// Historic `MintingFailed` events predate `classification`; they must
    /// replay as `Unclassified`.
    #[test]
    fn minting_failed_without_classification_replays_as_unclassified() {
        let historic = serde_json::json!({
            "MintingFailed": {
                "issuer_request_id": IssuerMintRequestId::random().to_string(),
                "error": "timeout",
                "failed_at": "2025-01-01T00:00:00Z"
            }
        });

        let event: MintEvent = serde_json::from_value(historic).unwrap();

        assert!(matches!(
            event,
            MintEvent::MintingFailed {
                classification: MintFailureClassification::Unclassified,
                ..
            }
        ));
    }

    /// The classifications' wire strings are permanent event schema
    /// (embedded in persisted `MintingFailed` payloads) — pin every variant
    /// against literals so a rename fails here instead of silently
    /// re-shaping history (a renamed halt variant would replay historic
    /// failures as `Unclassified` and re-enter the retry schedule).
    #[test]
    fn every_classification_pins_its_wire_string() {
        for (classification, wire) in [
            (MintFailureClassification::Unclassified, "Unclassified"),
            (
                MintFailureClassification::VaultLogicMismatch,
                "VaultLogicMismatch",
            ),
            (
                MintFailureClassification::ReceiptLogicMismatch,
                "ReceiptLogicMismatch",
            ),
            (
                MintFailureClassification::NonceConsumedByOtherMint,
                "NonceConsumedByOtherMint",
            ),
            (
                MintFailureClassification::NonceReplayUnresolved,
                "NonceReplayUnresolved",
            ),
            (
                MintFailureClassification::BadRecipientSignature,
                "BadRecipientSignature",
            ),
            (
                MintFailureClassification::RecipientCallbackRejected,
                "RecipientCallbackRejected",
            ),
            (
                MintFailureClassification::VaultAmountMismatch,
                "VaultAmountMismatch",
            ),
        ] {
            assert_eq!(
                serde_json::to_value(classification).unwrap(),
                serde_json::json!(wire)
            );
            assert_eq!(
                serde_json::from_value::<MintFailureClassification>(
                    serde_json::json!(wire)
                )
                .unwrap(),
                classification
            );
        }
    }

    /// The new orchestrator events' on-disk shape is a permanent event
    /// schema — pin it against literal JSON (a serde round-trip can never
    /// catch a field rename) along with the permanent `event_type` names.
    #[test]
    fn orchestrator_mint_events_pin_wire_shape() {
        let issuer_request_id = IssuerMintRequestId::random();
        let timestamp: DateTime<Utc> = "2026-01-02T03:04:05Z".parse().unwrap();
        let nonce = B256::repeat_byte(0x07);

        let minted = MintEvent::OrchestratorTokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: B256::ZERO,
            nonce,
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: 21_000,
            block_number: 1_000,
            minted_at: timestamp,
        };
        assert_eq!(minted.event_type(), "MintEvent::OrchestratorTokensMinted");
        let minted_wire = serde_json::json!({
            "OrchestratorTokensMinted": {
                "issuer_request_id": issuer_request_id.to_string(),
                "tx_hash": format!("{:?}", B256::ZERO),
                "nonce": format!("{nonce:?}"),
                "shares_minted": "0x56bc75e2d63100000",
                "gas_used": 21_000,
                "block_number": 1_000,
                "minted_at": "2026-01-02T03:04:05Z",
            }
        });
        assert_eq!(serde_json::to_value(&minted).unwrap(), minted_wire);
        assert_eq!(
            serde_json::from_value::<MintEvent>(minted_wire).unwrap(),
            minted
        );

        let recovered = MintEvent::OrchestratorMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: B256::ZERO,
            nonce,
            shares_minted: uint!(100_000000000000000000_U256),
            block_number: 777,
            recovered_at: timestamp,
        };
        assert_eq!(
            recovered.event_type(),
            "MintEvent::OrchestratorMintRecovered"
        );
        let recovered_wire = serde_json::json!({
            "OrchestratorMintRecovered": {
                "issuer_request_id": issuer_request_id.to_string(),
                "tx_hash": format!("{:?}", B256::ZERO),
                "nonce": format!("{nonce:?}"),
                "shares_minted": "0x56bc75e2d63100000",
                "block_number": 777,
                "recovered_at": "2026-01-02T03:04:05Z",
            }
        });
        assert_eq!(serde_json::to_value(&recovered).unwrap(), recovered_wire);
        assert_eq!(
            serde_json::from_value::<MintEvent>(recovered_wire).unwrap(),
            recovered
        );
    }
}
