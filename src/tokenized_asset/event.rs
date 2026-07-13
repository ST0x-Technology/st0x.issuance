use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol};

/// Listing lifecycle events. The pre-multichain `Frozen`/`Unfrozen` variants
/// live on the underlying-keyed `Underlying` aggregate now — the
/// aggregate-rekey migration re-types shipped rows onto it, so no
/// `TokenizedAsset` stream carries them after migration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedAssetEvent {
    Added {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault: Address,
        added_at: DateTime<Utc>,
    },
    VaultAddressUpdated {
        vault: Address,
        previous_vault: Address,
        updated_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedAssetEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Added { .. } => "TokenizedAssetEvent::Added".to_string(),
            Self::VaultAddressUpdated { .. } => {
                "TokenizedAssetEvent::VaultAddressUpdated".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
