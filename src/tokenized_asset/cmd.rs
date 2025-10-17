use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultAddress(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedAssetCommand {
    AddAsset {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault_address: VaultAddress,
    },
}
