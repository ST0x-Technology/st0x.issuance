use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol};
use super::cmd::VaultAddress;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedAssetEvent {
    AssetAdded {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault_address: VaultAddress,
        added_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedAssetEvent {
    fn event_type(&self) -> String {
        match self {
            Self::AssetAdded { .. } => "AssetAdded".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
