use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedAssetEvent {
    Added {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault: Address,
        added_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedAssetEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Added { .. } => "TokenizedAssetEvent::Added".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
