use alloy::primitives::{Bytes, TxHash};
use serde::{Deserialize, Serialize};

use super::event::ReceiptSource;
use super::{ReceiptId, Shares};
use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
use crate::vault::ReceiptInformation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ReceiptInventoryCommand {
    DiscoverReceipt {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        source: ReceiptSource,
        receipt_info: Option<Box<ReceiptInformation>>,
        receipt_info_bytes: Option<Bytes>,
    },
    ReconcileBalance {
        receipt_id: ReceiptId,
        on_chain_balance: Shares,
    },
    ReserveBurn {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
        burns: Vec<BurnRecord>,
    },
    ReleaseBurn {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    },
    SettleBurn {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    },
}
