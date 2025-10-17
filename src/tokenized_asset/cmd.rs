use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol, VaultAddress};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedAssetCommand {
    AddAsset {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault_address: VaultAddress,
    },
}
