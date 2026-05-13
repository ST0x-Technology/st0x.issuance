use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use super::IssuerRedemptionRequestId;
use crate::Quantity;
use crate::mint::TokenizationRequestId;
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
use crate::vault::MultiBurnEntry;

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
    /// Produces `BurnFireblocksSubmitted` on success, or the caller records failure.
    BurnTokens {
        issuer_request_id: IssuerRedemptionRequestId,
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
    },

    /// Confirms a previously submitted burn transaction.
    /// Polls the signing backend and produces `TokensBurned` or error.
    ConfirmBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        fireblocks_tx_id: String,
        dust_shares: U256,
    },
    RecordBurnFailure {
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
        /// Fireblocks transaction ID, if the burn was submitted via Fireblocks.
        fireblocks_tx_id: Option<String>,
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
    /// Records an existing on-chain burn discovered via Fireblocks tx lookup.
    /// Only valid from `Failed` state. Used when the Fireblocks transaction
    /// succeeded on-chain but the bot timed out before recording it.
    RecordExistingBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        fireblocks_tx_id: String,
        tx_hash: B256,
        planned_burns: Vec<super::BurnRecord>,
        block_number: u64,
    },
    /// Admin-closes a failed redemption that cannot be automatically recovered.
    /// Only valid from `Failed` state.
    CloseRedemption {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
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
    },
}
