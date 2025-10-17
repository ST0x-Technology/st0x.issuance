use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedAssetCommand {
    Add {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault_address: Address,
    },
}
