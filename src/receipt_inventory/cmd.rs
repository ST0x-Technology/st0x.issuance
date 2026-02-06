use alloy::primitives::B256;
use serde::{Deserialize, Serialize};

use super::{ReceiptId, Shares};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ReceiptInventoryCommand {
    DiscoverReceipt {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: B256,
    },
    BurnShares {
        receipt_id: ReceiptId,
        amount: Shares,
    },
}
