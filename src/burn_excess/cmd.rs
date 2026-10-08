use alloy::primitives::B256;
use serde::{Deserialize, Serialize};

use super::{
    BurnExcessPath, ExcessBurnBind, FundingTransferId, HeldTransferRedemption,
};
use crate::vault::{SendableTxWithHash, TxId};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BurnExcessCloseProof {
    #[default]
    Unsigned,
    FinalizedReverted,
    ProvablyDead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum BurnExcessCommand {
    /// Path B, live route: record the funding Transfer the stream expects
    /// before it is broadcast, so the poller holds it rather than redeem it.
    ExpectFunding {
        bind: ExcessBurnBind,
        reason: String,
        incident_id: Option<String>,
    },
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
        /// Persisted atomically with the signed intent so a crash cannot lose
        /// the AP attribution the balance proof relied on.
        held_redemptions: Vec<HeldTransferRedemption>,
    },
    /// Anchor genuine AP Transfers held with Path B funding before the signed
    /// excess burn can be broadcast.
    AnchorHeldRedemptions {
        held_redemptions: Vec<HeldTransferRedemption>,
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
        proof: BurnExcessCloseProof,
        release_through_block: Option<u64>,
    },
}
