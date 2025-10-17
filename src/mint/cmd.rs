use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use super::{
    ClientId, Network, Quantity, TokenSymbol, TokenizationRequestId,
    UnderlyingSymbol,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MintCommand {
    Initiate {
        tokenization_request_id: TokenizationRequestId,
        qty: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
    },
}
