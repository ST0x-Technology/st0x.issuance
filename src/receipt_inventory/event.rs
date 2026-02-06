use alloy::primitives::B256;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{ReceiptId, Shares};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptInventoryEvent {
    ReceiptDiscovered {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: B256,
    },
    ReceiptBurned {
        receipt_id: ReceiptId,
        amount_burned: Shares,
        new_balance: Shares,
    },
    ReceiptDepleted {
        receipt_id: ReceiptId,
    },
}

impl DomainEvent for ReceiptInventoryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::ReceiptDiscovered { .. } => {
                "ReceiptInventoryEvent::ReceiptDiscovered".to_string()
            }
            Self::ReceiptBurned { .. } => {
                "ReceiptInventoryEvent::ReceiptBurned".to_string()
            }
            Self::ReceiptDepleted { .. } => {
                "ReceiptInventoryEvent::ReceiptDepleted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
