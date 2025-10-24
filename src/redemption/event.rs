use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum RedemptionEvent {
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
}

impl DomainEvent for RedemptionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Detected { .. } => "RedemptionEvent::Detected".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
