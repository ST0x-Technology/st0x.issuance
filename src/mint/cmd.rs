use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use super::{
    ClientId, IssuerRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MintCommand {
    Initiate {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
    },
    ConfirmJournal {
        issuer_request_id: IssuerRequestId,
    },
    RejectJournal {
        issuer_request_id: IssuerRequestId,
        reason: String,
    },
    StartMinting {
        issuer_request_id: IssuerRequestId,
    },
    CompleteMinting {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
    },
    FailMinting {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
    CompleteCallback {
        issuer_request_id: IssuerRequestId,
    },
    /// Recovers a mint that already succeeded on-chain during recovery.
    ///
    /// Used when recovering from `Minting` or `MintingFailed` state and we find
    /// that the mint actually succeeded on-chain (receipt exists in inventory).
    RecoverExistingMint {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
    },
    /// Retries a mint that failed or was interrupted before completing on-chain.
    ///
    /// Used during recovery when no receipt exists for a mint stuck in
    /// `Minting` or `MintingFailed` state.
    RetryMint {
        issuer_request_id: IssuerRequestId,
    },
}
