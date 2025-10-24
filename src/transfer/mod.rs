use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;

use crate::mint::IssuerRequestId;
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

pub(crate) struct Transfer {
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) from: Address,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) amount: U256,
    pub(crate) tx_hash: B256,
    pub(crate) block_number: u64,
}

#[async_trait]
pub(crate) trait TransferService: Send + Sync {
    async fn watch(&self) -> Result<Vec<Transfer>, TransferError>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferError {
    #[error("Failed to connect to blockchain: {0}")]
    ConnectionFailed(String),

    #[error("Failed to fetch transfers: {0}")]
    FetchFailed(String),

    #[error("Invalid transfer data: {0}")]
    InvalidData(String),
}
