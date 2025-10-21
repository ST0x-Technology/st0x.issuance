use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{
    ClientId, IssuerRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintEvent {
    Initiated {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
    },
    JournalConfirmed {
        issuer_request_id: IssuerRequestId,
        confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerRequestId,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
}

impl DomainEvent for MintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initiated { .. } => "MintEvent::Initiated".to_string(),
            Self::JournalConfirmed { .. } => {
                "MintEvent::JournalConfirmed".to_string()
            }
            Self::JournalRejected { .. } => {
                "MintEvent::JournalRejected".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
