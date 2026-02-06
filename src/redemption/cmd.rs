use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use crate::Quantity;
use crate::mint::{IssuerRequestId, TokenizationRequestId};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
use crate::vault::ReceiptInformation;

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
    /// Burns tokens on-chain and returns dust to user.
    /// This command calls the vault service and emits TokensBurned on success.
    BurnTokens {
        issuer_request_id: IssuerRequestId,
        vault: Address,
        burn_shares: U256,
        dust_shares: U256,
        receipt_id: U256,
        owner: Address,
        receipt_info: ReceiptInformation,
    },
    RecordBurnFailure {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
    /// Retries a failed burn operation.
    /// Only valid from Failed state after a BurningFailed event.
    /// Uses view data to reconstruct burn parameters since Failed state loses metadata.
    RetryBurn {
        issuer_request_id: IssuerRequestId,
        vault: Address,
        burn_shares: U256,
        dust_shares: U256,
        receipt_id: U256,
        owner: Address,
        receipt_info: ReceiptInformation,
        /// Wallet to return dust to (from view metadata)
        user_wallet: Address,
    },
}
