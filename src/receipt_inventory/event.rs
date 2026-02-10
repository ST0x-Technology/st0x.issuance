use alloy::primitives::TxHash;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{ReceiptId, Shares};
use crate::mint::IssuerMintRequestId;
use crate::vault::ReceiptInformation;

/// Identifies how a receipt was created.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptSource {
    /// Receipt from Alpaca Instant Tokenization Network (ITN).
    /// The issuer_request_id links this receipt to the mint operation that created it.
    Itn { issuer_request_id: IssuerMintRequestId },
    /// External receipt for mints not performed by this service
    External,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptInventoryEvent {
    Discovered {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        source: ReceiptSource,
        #[serde(default)]
        receipt_info: Option<ReceiptInformation>,
    },
    Burned {
        receipt_id: ReceiptId,
        amount_burned: Shares,
        new_balance: Shares,
    },
    Depleted {
        receipt_id: ReceiptId,
    },
    BackfillCheckpoint {
        block_number: u64,
    },
}

impl DomainEvent for ReceiptInventoryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Discovered { .. } => {
                "ReceiptInventoryEvent::Discovered".to_string()
            }
            Self::Burned { .. } => "ReceiptInventoryEvent::Burned".to_string(),
            Self::Depleted { .. } => {
                "ReceiptInventoryEvent::Depleted".to_string()
            }
            Self::BackfillCheckpoint { .. } => {
                "ReceiptInventoryEvent::BackfillCheckpoint".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
