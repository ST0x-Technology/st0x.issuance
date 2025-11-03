use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
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
    AlpacaCalled {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        called_at: DateTime<Utc>,
    },
    AlpacaCallFailed {
        issuer_request_id: IssuerRequestId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    AlpacaJournalCompleted {
        issuer_request_id: IssuerRequestId,
        alpaca_completed_at: DateTime<Utc>,
    },
    RedemptionFailed {
        issuer_request_id: IssuerRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for RedemptionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Detected { .. } => "RedemptionEvent::Detected".to_string(),
            Self::AlpacaCalled { .. } => {
                "RedemptionEvent::AlpacaCalled".to_string()
            }
            Self::AlpacaCallFailed { .. } => {
                "RedemptionEvent::AlpacaCallFailed".to_string()
            }
            Self::AlpacaJournalCompleted { .. } => {
                "RedemptionEvent::AlpacaJournalCompleted".to_string()
            }
            Self::RedemptionFailed { .. } => {
                "RedemptionEvent::RedemptionFailed".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[test]
    fn test_alpaca_journal_completed_event_type() {
        let event = RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: IssuerRequestId::new("red-test-123"),
            alpaca_completed_at: Utc::now(),
        };

        assert_eq!(
            event.event_type(),
            "RedemptionEvent::AlpacaJournalCompleted"
        );
    }
}
