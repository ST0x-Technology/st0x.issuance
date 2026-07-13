use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol};

/// Listing lifecycle commands. Corporate-action freeze/unfreeze address the
/// underlying-keyed `Underlying` aggregate (`crate::underlying`), not a
/// per-network listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedAssetCommand {
    Add {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault: Address,
    },
}
