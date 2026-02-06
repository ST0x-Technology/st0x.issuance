use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use crate::Quantity;
use crate::mint::{IssuerRequestId, TokenizationRequestId};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
use crate::vault::{MultiBurnEntry, ReceiptInformation};

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
        /// Quantity sent to Alpaca (truncated to 9 decimals)
        alpaca_quantity: Quantity,
        /// Dust quantity to be returned to user
        dust_quantity: Quantity,
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
    /// Burns tokens on-chain from one or more receipts, returns dust to user.
    /// Uses multicall to atomically execute all burns in a single transaction.
    BurnTokens {
        issuer_request_id: IssuerRequestId,
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
        receipt_info: ReceiptInformation,
    },
    RecordBurnFailure {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
    /// Retries a failed burn operation.
    /// Only valid from Failed state after a BurningFailed event.
    RetryBurn {
        issuer_request_id: IssuerRequestId,
        vault: Address,
        /// Burns to execute (receipt_id + amount for each)
        burns: Vec<MultiBurnEntry>,
        /// Dust to return to user
        dust_shares: U256,
        owner: Address,
        receipt_info: ReceiptInformation,
        /// Wallet to return dust to (from view metadata)
        user_wallet: Address,
    },
}
