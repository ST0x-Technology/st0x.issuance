use alloy::primitives::B256;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{BurnExcessPath, ExcessBurnBind, FundingTransferId};
use crate::vault::{SendableTxWithHash, TxId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum BurnExcessEvent {
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
        closed_at: DateTime<Utc>,
    },
}

impl BurnExcessEvent {
    /// Stored `event_type` values, shared with the raw SQL that filters on
    /// them: the wallet intent gate (`has_unresolved_excess_burn_intent`) and
    /// the exclusion index rebuild (`rebuild_funding_exclusion_index`). Bound
    /// here so a renamed variant is a compile error rather than a query that
    /// silently matches nothing — a gate that returns zero rows is a gate that
    /// is off.
    pub(crate) const FUNDING_EXCLUSION_RECORDED: &'static str =
        "BurnExcessEvent::FundingExclusionRecorded";
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
            Self::FundingExclusionRecorded { .. } => {
                Self::FUNDING_EXCLUSION_RECORDED.to_string()
            }
            Self::ExcessBurnIntended { .. } => {
                Self::EXCESS_BURN_INTENDED.to_string()
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
