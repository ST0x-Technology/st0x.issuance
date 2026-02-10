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
    /// Burns tokens on-chain from one or more receipts, returns dust to user.
    /// Uses multicall to atomically execute all burns in a single transaction.
    BurnTokens {
        issuer_request_id: IssuerRedemptionRequestId,
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
    },
    RecordBurnFailure {
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
    },
    /// Retries a failed burn operation.
    /// Only valid from Failed state after a BurningFailed event.
    RetryBurn {
        issuer_request_id: IssuerRedemptionRequestId,
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
        /// Wallet to return dust to (from view metadata)
        user_wallet: Address,
    },
}
