use alloy::primitives::B256;
use serde::{Deserialize, Serialize};

use super::{BurnExcessPath, ExcessBurnBind, FundingTransferId};
use crate::vault::{SendableTxWithHash, TxId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum BurnExcessCommand {
    /// Path B only: record a verified funding Transfer so the poller skips it.
    RecordFundingExclusion {
        bind: ExcessBurnBind,
        funding_log_id: FundingTransferId,
        reason: String,
        incident_id: Option<String>,
    },
    /// Persist the exact signed burn before broadcast.
    ///
    /// Path A may originate the stream; Path B requires
    /// [`Self::RecordFundingExclusion`] first.
    ///
    /// `receipt_id`, `shares`, and issuer wallet (owner) come from `bind` —
    /// do not duplicate them here.
    IntendExcessBurn {
        bind: ExcessBurnBind,
        path: BurnExcessPath,
        funding_log_id: Option<FundingTransferId>,
        reason: String,
        incident_id: Option<String>,
        sendable_tx: SendableTxWithHash,
    },
    RecordExcessBurnSubmitted {
        tx_id: TxId,
        burn_tx_hash: B256,
    },
    CompleteExcessBurn {
        burn_tx_hash: B256,
        block_number: u64,
    },
    CloseExcessBurn {
        reason: String,
    },
}
