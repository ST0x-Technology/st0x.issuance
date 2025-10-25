use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{AlpacaAccountNumber, ClientId, Email};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum AccountEvent {
    Linked {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
        wallet: Address,
        linked_at: DateTime<Utc>,
    },
}

impl DomainEvent for AccountEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Linked { .. } => "AccountEvent::Linked".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
