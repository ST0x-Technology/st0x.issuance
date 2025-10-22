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
    RecordMintSuccess {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
    },
    RecordMintFailure {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
    RecordCallback {
        issuer_request_id: IssuerRequestId,
    },
}
