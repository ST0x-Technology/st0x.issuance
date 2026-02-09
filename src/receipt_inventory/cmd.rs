use alloy::primitives::TxHash;
use serde::{Deserialize, Serialize};

use super::event::ReceiptSource;
use super::{ReceiptId, Shares};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ReceiptInventoryCommand {
    DiscoverReceipt {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        source: ReceiptSource,
    },
    BurnShares {
        receipt_id: ReceiptId,
        amount: Shares,
    },
    AdvanceBackfillCheckpoint {
        block_number: u64,
    },
}
