use alloy::primitives::{Address, B256, U256};
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
        alpaca_journal_completed_at: DateTime<Utc>,
    },
    RedemptionFailed {
        issuer_request_id: IssuerRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    TokensBurned {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_burned: U256,
        gas_used: u64,
        block_number: u64,
        burned_at: DateTime<Utc>,
    },
    BurningFailed {
        issuer_request_id: IssuerRequestId,
        error: String,
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
            Self::TokensBurned { .. } => {
                "RedemptionEvent::TokensBurned".to_string()
            }
            Self::BurningFailed { .. } => {
                "RedemptionEvent::BurningFailed".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{b256, uint};
    use chrono::Utc;

    use super::*;

    #[test]
    fn test_alpaca_journal_completed_event_type() {
        let event = RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: IssuerRequestId::new("red-test-123"),
            alpaca_journal_completed_at: Utc::now(),
        };

        assert_eq!(
            event.event_type(),
            "RedemptionEvent::AlpacaJournalCompleted"
        );
    }

    #[test]
    fn test_tokens_burned_event_type() {
        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-burned-456"),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            receipt_id: uint!(42_U256),
            shares_burned: uint!(100_000000000000000000_U256),
            gas_used: 50000,
            block_number: 1000,
            burned_at: Utc::now(),
        };

        assert_eq!(event.event_type(), "RedemptionEvent::TokensBurned");
        assert_eq!(event.event_version(), "1.0");
    }

    #[test]
    fn test_tokens_burned_serialization() {
        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-ser-456"),
            tx_hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            receipt_id: uint!(7_U256),
            shares_burned: uint!(250_500000000000000000_U256),
            gas_used: 75000,
            block_number: 2000,
            burned_at: Utc::now(),
        };

        let serialized = serde_json::to_string(&event).unwrap();
        let deserialized: RedemptionEvent =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_burning_failed_event_type() {
        let event = RedemptionEvent::BurningFailed {
            issuer_request_id: IssuerRequestId::new("red-fail-789"),
            error: "Blockchain error: timeout".to_string(),
            failed_at: Utc::now(),
        };

        assert_eq!(event.event_type(), "RedemptionEvent::BurningFailed");
        assert_eq!(event.event_version(), "1.0");
    }

    #[test]
    fn test_burning_failed_serialization() {
        let event = RedemptionEvent::BurningFailed {
            issuer_request_id: IssuerRequestId::new("red-ser-789"),
            error: "Network timeout".to_string(),
            failed_at: Utc::now(),
        };

        let serialized = serde_json::to_string(&event).unwrap();
        let deserialized: RedemptionEvent =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(event, deserialized);
    }
}
