use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use super::{BurnExternalTxId, IssuerRedemptionRequestId};
use crate::Quantity;
use crate::mint::TokenizationRequestId;
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
use crate::vault::{MultiBurnEntry, TxId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionCommand {
    Detect {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
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
    /// Submits burn transaction to the signing backend.
    /// Produces `BurnTxSubmitted` on success, or the caller records failure.
    BurnTokens {
        issuer_request_id: IssuerRedemptionRequestId,
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
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
    /// succeeded on-chain but the bot timed out before recording it.
    RecordExistingBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_id: TxId,
        tx_hash: B256,
        planned_burns: Vec<super::BurnRecord>,
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
    /// `Burning`/`BurnIntended`/`BurnSubmitted` whose burn already landed
    /// on-chain. The admin layer verifies `burn_tx_hash` before issuing this
    /// command; the aggregate records it as proof and transitions to
    /// `Completed`.
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
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
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
