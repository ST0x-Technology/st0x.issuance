use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use super::{BurnExternalTxId, IssuerRedemptionRequestId};
use crate::Quantity;
use crate::config::VaultMode;
use crate::mint::TokenizationRequestId;
use crate::redemption::event::BurnFailureClassification;
use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
use crate::vault::{MultiBurnEntry, TxId};

/// Mode-specific burn parameters carried by `IntendBurn` and `BurnTokens`.
///
/// The handler cross-checks the variant against the redemption's persisted
/// `burn_mode` anchor and rejects a mismatch, so a caller can never drive a
/// vault-direct redemption down the orchestrator path or vice versa.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum BurnParams {
    VaultDirect {
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
    },
    Orchestrator {
        /// The vault contract, which is also the ERC-20 share token.
        token: Address,
        /// Burn amount in 18-decimal share-wei (dust stays in the bot
        /// wallet).
        amount: U256,
        owner: Address,
    },
}

/// Mode-specific proof of an existing on-chain burn carried by
/// `RecordExistingBurn`. Cross-checked against the redemption's persisted
/// `burn_mode` anchor by the command handler. `dust_retained` is supplied by
/// the caller (derived from the redemption's persisted `dust_quantity`)
/// because the `Failed` state does not retain it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ExistingBurnProof {
    VaultDirect {
        burns: Vec<super::BurnRecord>,
    },
    Orchestrator {
        shares_burned: U256,
        /// `(firstReceiptId, nextBurnReceiptIdAfter)` from the `Burned` event.
        burn_range: (U256, U256),
        dust_retained: U256,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionCommand {
    Detect {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        /// The asset's `VaultMode` resolved from config at detection time.
        /// Persisted on `Detected` to anchor mode derivation for the whole
        /// redemption lifecycle. Deliberately no `#[serde(default)]`:
        /// commands are not persisted, so a default here would only mask a
        /// caller that forgot to resolve the mode.
        burn_mode: VaultMode,
    },
    RecordAlpacaCall {
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        /// Quantity sent to Alpaca (truncated to 9 decimals)
        alpaca_quantity: Quantity,
        /// Dust quantity to be returned to user
        dust_quantity: Quantity,
    },
    RecordAlpacaFailure {
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
    },
    ConfirmAlpacaComplete {
        issuer_request_id: IssuerRedemptionRequestId,
    },
    MarkFailed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
    },
    /// Broadcasts the exact transaction persisted by `IntendBurn`.
    /// Produces `BurnTxSubmitted` (vault-direct) or
    /// `OrchestratorBurnSubmitted` (orchestrator) on success, or the caller
    /// records failure.
    BurnTokens {
        issuer_request_id: IssuerRedemptionRequestId,
        params: BurnParams,
        /// Optional deterministic transaction `externalTxId` override.
        /// Used when retrying a replacement burn after a prior accepted
        /// transaction burn terminally failed.
        #[serde(default)]
        external_tx_id: Option<BurnExternalTxId>,
    },

    /// Confirms a previously submitted burn transaction.
    /// Polls the signing backend and produces `TokensBurned` or error.
    ConfirmBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_id: TxId,
        dust_shares: U256,
    },
    RecordBurnFailure {
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
        tx_id: Option<TxId>,
        /// Planned burns at the time of failure.
        planned_burns: Vec<super::BurnRecord>,
        /// Typed failure classification persisted on `BurningFailed`.
        /// Deliberately no `#[serde(default)]` — see `Detect::burn_mode`.
        classification: BurnFailureClassification,
    },
    /// Resets a failed redemption back to Detected state for reprocessing.
    /// Only valid from `Failed` state — post-Alpaca states have dedicated
    /// recovery paths and resetting them would cause duplicate Alpaca calls.
    /// Metadata is provided by the API layer (extracted from the event
    /// store) since the Failed state does not preserve it.
    Reprocess {
        issuer_request_id: IssuerRedemptionRequestId,
        metadata: super::RedemptionMetadata,
    },
    /// Records an existing on-chain burn discovered via tx lookup.
    /// Only valid from `Failed` state. Used when the transaction
    /// succeeded on-chain but the bot timed out before recording it. The
    /// `proof` variant is cross-checked against the redemption's persisted
    /// `burn_mode` anchor, so a vault-direct redemption can never be recorded
    /// with an orchestrator proof or vice versa.
    RecordExistingBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_id: TxId,
        tx_hash: B256,
        proof: ExistingBurnProof,
        block_number: u64,
    },
    /// Admin-closes a redemption that cannot be automatically recovered.
    /// Valid from `Failed`, `Burning`, or `BurnSubmitted`. The honest terminal
    /// path for a redemption whose burn is not verifiable on-chain.
    CloseRedemption {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        /// Exact persisted signed burn hash the operator has reconciled and is
        /// explicitly acknowledging may still land.
        #[serde(default)]
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    },
    /// Admin-terminalizes a redemption stuck in
    /// `Burning`/`BurnIntended`/`BurnSubmitted`/`Failed` whose burn already
    /// landed on-chain. The admin layer verifies `burn_tx_hash` before
    /// issuing this command; the aggregate records it as proof and
    /// transitions to `Completed`. A legacy `Failed` redemption with no
    /// persisted signed transaction has no hash to bind, so the caller's
    /// on-chain verification of the planned burns is the entire proof there.
    ForceCompleteBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        block_number: u64,
        reason: String,
        /// Exact persisted signed burn hash the operator has reconciled when
        /// `burn_tx_hash` proves a different transaction.
        #[serde(default)]
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    },
    /// Resumes a post-Alpaca failed redemption directly to Burning state.
    /// Only valid from `Failed` state when Alpaca was already called and
    /// the journal has since completed on Alpaca's side.
    /// Metadata is provided by the API layer (extracted from the event store).
    ResumeBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        metadata: super::RedemptionMetadata,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        called_at: chrono::DateTime<chrono::Utc>,
        /// Alpaca's `updated_at` for the completed journal.
        alpaca_journal_completed_at: chrono::DateTime<chrono::Utc>,
        /// Optional deterministic transaction `externalTxId` for the next burn
        /// submission. Persisted through `BurnResumed` so a retry submission
        /// that fails before transaction accepts it can be retried idempotently.
        #[serde(default)]
        external_tx_id: Option<BurnExternalTxId>,
    },
    IntendBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        params: BurnParams,
        /// Optional deterministic `externalTxId` override.
        #[serde(default)]
        external_tx_id: Option<BurnExternalTxId>,
    },
    RecordBurnRecoveryAttempt {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        nonce: u64,
        action: super::BurnRecoveryAction,
    },
    RecordBurnPreparationRecoveryAttempt {
        issuer_request_id: IssuerRedemptionRequestId,
        attempt: u32,
    },
    RecordBurnRecoveryExhausted {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        nonce: u64,
        attempts: u32,
    },
    RecordBurnPreparationRecoveryExhausted {
        issuer_request_id: IssuerRedemptionRequestId,
        attempts: u32,
    },
    ReplaceDeadBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        owner: Address,
    },
}
