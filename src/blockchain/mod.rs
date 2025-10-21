use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::mint::{
    IssuerRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
};

mod service;

#[cfg(test)]
pub(crate) mod mock;

#[async_trait]
pub(crate) trait BlockchainService: Send + Sync {
    async fn mint_tokens(
        &self,
        assets: U256,
        receiver: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<MintResult, BlockchainError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MintResult {
    pub(crate) tx_hash: B256,
    pub(crate) receipt_id: U256,
    pub(crate) shares_minted: U256,
    pub(crate) gas_used: u64,
    pub(crate) block_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReceiptInformation {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) quantity: Quantity,
    pub(crate) operation_type: OperationType,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OperationType {
    Mint,
    Redeem,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockchainError {
    #[error("Transaction failed: {reason}")]
    TransactionFailed { reason: String },

    #[error("Invalid receipt")]
    InvalidReceipt,

    #[error("Gas estimation failed")]
    GasEstimationFailed,

    #[error("RPC error: {message}")]
    RpcError { message: String },

    #[error("Event not found in transaction: {tx_hash}")]
    EventNotFound { tx_hash: String },
}
