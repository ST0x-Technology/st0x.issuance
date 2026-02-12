use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{AlpacaAccountNumber, ClientId, Email};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum AccountEvent {
    Registered {
        client_id: ClientId,
        email: Email,
        registered_at: DateTime<Utc>,
    },
    LinkedToAlpaca {
        alpaca_account: AlpacaAccountNumber,
        linked_at: DateTime<Utc>,
    },
    WalletWhitelisted {
        wallet: Address,
        whitelisted_at: DateTime<Utc>,
    },
    WalletUnwhitelisted {
        wallet: Address,
        unwhitelisted_at: DateTime<Utc>,
    },
}

impl DomainEvent for AccountEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Registered { .. } => "AccountEvent::Registered".to_string(),
            Self::LinkedToAlpaca { .. } => {
                "AccountEvent::LinkedToAlpaca".to_string()
            }
            Self::WalletWhitelisted { .. } => {
                "AccountEvent::WalletWhitelisted".to_string()
            }
            Self::WalletUnwhitelisted { .. } => {
                "AccountEvent::WalletUnwhitelisted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
