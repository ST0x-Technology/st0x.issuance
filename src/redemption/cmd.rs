use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

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
    RecordBurnSuccess {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_burned: U256,
        gas_used: u64,
        block_number: u64,
    },
    RecordBurnFailure {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
}
