use alloy::primitives::TxHash;
use serde::{Deserialize, Serialize};

use super::event::ReceiptSource;
use super::{ReceiptId, Shares};
use crate::vault::ReceiptInformation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ReceiptInventoryCommand {
    DiscoverReceipt {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        source: ReceiptSource,
        receipt_info: Option<ReceiptInformation>,
    },
    ReconcileBalance {
        receipt_id: ReceiptId,
        on_chain_balance: Shares,
    },
    AdvanceBackfillCheckpoint {
        block_number: u64,
    },
}
