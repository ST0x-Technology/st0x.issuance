use alloy::primitives::B256;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{
    BurnExcessCloseProof, BurnExcessPath, ExcessBurnBind, FundingTransferId,
    HeldTransferRedemption,
};
use crate::vault::{SendableTxWithHash, TxId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum BurnExcessEvent {
    /// Path B, live route: the funding Transfer this stream will burn is
    /// expected from `bind.original_recipient` to `bind.issuer_wallet` for
    /// exactly `bind.shares`, so the poller holds a matching log instead of
    /// opening a Redemption for it.
    FundingExpected {
        bind: ExcessBurnBind,
        reason: String,
        incident_id: Option<String>,
        expected_at: DateTime<Utc>,
    },
    FundingExclusionRecorded {
        bind: ExcessBurnBind,
        funding_log_id: FundingTransferId,
        reason: String,
        incident_id: Option<String>,
        excluded_at: DateTime<Utc>,
    },
    ExcessBurnIntended {
        bind: ExcessBurnBind,
        path: BurnExcessPath,
        funding_log_id: Option<FundingTransferId>,
        reason: String,
        incident_id: Option<String>,
        sendable_tx: SendableTxWithHash,
        intended_at: DateTime<Utc>,
    },
    /// Genuine AP Transfers held with Path B funding, durably attributed
    /// before the excess burn can be broadcast.
    HeldRedemptionsAnchored {
        held_redemptions: Vec<HeldTransferRedemption>,
        anchored_at: DateTime<Utc>,
    },
    ExcessBurnSubmitted {
        tx_id: TxId,
        burn_tx_hash: B256,
        submitted_at: DateTime<Utc>,
    },
    ExcessBurnCompleted {
        burn_tx_hash: B256,
        block_number: u64,
        completed_at: DateTime<Utc>,
    },
    ExcessBurnClosed {
        reason: String,
        #[serde(default)]
        proof: BurnExcessCloseProof,
        #[serde(default)]
        release_through_block: Option<u64>,
        closed_at: DateTime<Utc>,
    },
}

impl BurnExcessEvent {
    /// Stored `event_type` values shared with raw SQL index rebuilds and
    /// lifecycle queries. Bound here so a renamed variant is a compile error
    /// rather than a query that silently matches nothing.
    pub(crate) const FUNDING_EXPECTED: &'static str =
        "BurnExcessEvent::FundingExpected";
    pub(crate) const FUNDING_EXCLUSION_RECORDED: &'static str =
        "BurnExcessEvent::FundingExclusionRecorded";
    pub(crate) const HELD_REDEMPTIONS_ANCHORED: &'static str =
        "BurnExcessEvent::HeldRedemptionsAnchored";
    pub(crate) const EXCESS_BURN_INTENDED: &'static str =
        "BurnExcessEvent::ExcessBurnIntended";
    pub(crate) const EXCESS_BURN_SUBMITTED: &'static str =
        "BurnExcessEvent::ExcessBurnSubmitted";
    pub(crate) const EXCESS_BURN_COMPLETED: &'static str =
        "BurnExcessEvent::ExcessBurnCompleted";
    pub(crate) const EXCESS_BURN_CLOSED: &'static str =
        "BurnExcessEvent::ExcessBurnClosed";
}

impl DomainEvent for BurnExcessEvent {
    fn event_type(&self) -> String {
        match self {
            Self::FundingExpected { .. } => Self::FUNDING_EXPECTED.to_string(),
            Self::FundingExclusionRecorded { .. } => {
                Self::FUNDING_EXCLUSION_RECORDED.to_string()
            }
            Self::ExcessBurnIntended { .. } => {
                Self::EXCESS_BURN_INTENDED.to_string()
            }
            Self::HeldRedemptionsAnchored { .. } => {
                Self::HELD_REDEMPTIONS_ANCHORED.to_string()
            }
            Self::ExcessBurnSubmitted { .. } => {
                Self::EXCESS_BURN_SUBMITTED.to_string()
            }
            Self::ExcessBurnCompleted { .. } => {
                Self::EXCESS_BURN_COMPLETED.to_string()
            }
            Self::ExcessBurnClosed { .. } => {
                Self::EXCESS_BURN_CLOSED.to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
