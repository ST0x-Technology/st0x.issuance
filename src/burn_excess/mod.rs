//! Administrative burn of excess shares from a proven duplicate mint.
//!
//! See SPEC.md "Burn excess shares". Path A (`internal`) burns when the issuer
//! already holds the excess; Path B (`external`) records a funding-transfer
//! exclusion then burns. Never Alpaca; never a `Redemption` aggregate.

pub(crate) mod api;
pub(crate) mod cli;
mod cmd;
pub(crate) mod engine;
mod event;
pub(crate) mod exclusion;
pub(crate) mod expectation;
pub(crate) mod proof;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EventSourced, Nil};
use serde::{Deserialize, Serialize};
use st0x_issuance_dto::AcknowledgedInboundTransfer;

use crate::account::{AlpacaAccountNumber, ClientId};
use crate::config::VaultMode;
use crate::mint::IssuerMintRequestId;
use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
use crate::vault::{SendableTxWithHash, TxId};

pub(crate) use cmd::{BurnExcessCloseProof, BurnExcessCommand};
pub(crate) use event::BurnExcessEvent;

/// Operator / aggregate path after selection (persisted on first progress).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BurnExcessPath {
    Internal,
    External,
}

impl std::fmt::Display for BurnExcessPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal => formatter.write_str("internal"),
            Self::External => formatter.write_str("external"),
        }
    }
}

/// Verified identity of a Path B funding Transfer log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FundingTransferId {
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) tx_hash: B256,
    pub(crate) log_index: u64,
    pub(crate) from: Address,
    pub(crate) to: Address,
    pub(crate) amount: U256,
}

/// Durable attribution for a genuine AP Transfer held by a live Path B
/// funding expectation. The account snapshot is anchored before the excess
/// burn is broadcast, so a later wallet unlink cannot strand this already
/// mined redemption when the expectation releases it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HeldTransferRedemption {
    pub(crate) transfer: FundingTransferId,
    /// Block containing the held log. Zero is the legacy compatibility value.
    #[serde(default)]
    pub(crate) block_number: u64,
    pub(crate) client_id: ClientId,
    pub(crate) alpaca_account: AlpacaAccountNumber,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) burn_mode: VaultMode,
}

/// Proven deposit bind shared across the burn-excess stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExcessBurnBind {
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) deposit_tx_hash: B256,
    pub(crate) receipt_id: U256,
    pub(crate) shares: U256,
    pub(crate) original_recipient: Address,
    pub(crate) vault: Address,
    pub(crate) network: Network,
    /// RPC chain proven when the bind was first recorded. Zero is the
    /// compatibility sentinel for pre-field history: signed resumes verify the
    /// decoded envelope chain, while unsigned resumes use the request chain.
    #[serde(default)]
    pub(crate) chain_id: u64,
    pub(crate) issuer_wallet: Address,
}

/// Aggregate id is the deposit transaction hash that created the excess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct BurnExcessId(B256);

impl BurnExcessId {
    #[must_use]
    pub(crate) const fn new(deposit_tx_hash: B256) -> Self {
        Self(deposit_tx_hash)
    }

    #[must_use]
    pub(crate) const fn deposit_tx_hash(self) -> B256 {
        self.0
    }
}

impl std::fmt::Display for BurnExcessId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#x}", self.0)
    }
}

impl std::str::FromStr for BurnExcessId {
    type Err = alloy::hex::FromHexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse::<B256>().map(Self)
    }
}

/// Lifecycle of one excess-burn recovery stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum BurnExcess {
    /// Path B, live route: the funding Transfer is expected but not yet
    /// proven; the poller holds a matching log. The hold on Transfers of that
    /// shape outlives this state until the stream completes or closes, while
    /// the funding log itself is skipped once excluded.
    AwaitingFunding {
        bind: ExcessBurnBind,
        reason: String,
        incident_id: Option<String>,
        expected_at: DateTime<Utc>,
    },
    FundingExcluded {
        bind: ExcessBurnBind,
        funding_log_id: FundingTransferId,
        reason: String,
        incident_id: Option<String>,
        excluded_at: DateTime<Utc>,
    },
    Intended {
        bind: ExcessBurnBind,
        path: BurnExcessPath,
        funding_log_id: Option<FundingTransferId>,
        reason: String,
        incident_id: Option<String>,
        sendable_tx: SendableTxWithHash,
        held_redemptions: Vec<HeldTransferRedemption>,
        held_redemptions_anchored: bool,
        #[serde(default)]
        acknowledged_inflows: Vec<AcknowledgedInboundTransfer>,
        intended_at: DateTime<Utc>,
    },
    Submitted {
        bind: ExcessBurnBind,
        path: BurnExcessPath,
        funding_log_id: Option<FundingTransferId>,
        reason: String,
        incident_id: Option<String>,
        sendable_tx: SendableTxWithHash,
        held_redemptions: Vec<HeldTransferRedemption>,
        held_redemptions_anchored: bool,
        #[serde(default)]
        acknowledged_inflows: Vec<AcknowledgedInboundTransfer>,
        tx_id: TxId,
        burn_tx_hash: B256,
        intended_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
    },
    Completed {
        bind: ExcessBurnBind,
        path: BurnExcessPath,
        funding_log_id: Option<FundingTransferId>,
        burn_tx_hash: B256,
        block_number: u64,
        completed_at: DateTime<Utc>,
    },
    Closed {
        bind: ExcessBurnBind,
        path: BurnExcessPath,
        funding_log_id: Option<FundingTransferId>,
        reason: String,
        release_through_block: Option<u64>,
        closed_at: DateTime<Utc>,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error,
)]
pub(crate) enum BurnExcessError {
    #[error("invalid state: expected {expected}, found {found}")]
    InvalidState { expected: String, found: String },

    #[error(
        "external path requires FundingExcluded before IntendExcessBurn; \
         got {found}"
    )]
    ExternalRequiresExclusion { found: String },

    #[error("internal path must not record a funding exclusion")]
    InternalMustNotExclude,

    #[error(
        "IntendExcessBurn path {command_path} conflicts with stream path \
         {stream_path}"
    )]
    PathConflict { stream_path: BurnExcessPath, command_path: BurnExcessPath },

    #[error(
        "funding log on intend does not match recorded exclusion: \
         command={command:?}, recorded={recorded:?}"
    )]
    FundingMismatch {
        command: Option<Box<FundingTransferId>>,
        recorded: Box<FundingTransferId>,
    },

    #[error("deposit bind does not match the stream bind")]
    BindMismatch,

    #[error("held-redemption attribution does not match the persisted set")]
    HeldRedemptionsMismatch,
}

impl BurnExcess {
    pub(crate) const fn path(&self) -> BurnExcessPath {
        match self {
            Self::AwaitingFunding { .. } | Self::FundingExcluded { .. } => {
                BurnExcessPath::External
            }
            Self::Intended { path, .. }
            | Self::Submitted { path, .. }
            | Self::Completed { path, .. }
            | Self::Closed { path, .. } => *path,
        }
    }

    pub(crate) const fn bind(&self) -> &ExcessBurnBind {
        match self {
            Self::AwaitingFunding { bind, .. }
            | Self::FundingExcluded { bind, .. }
            | Self::Intended { bind, .. }
            | Self::Submitted { bind, .. }
            | Self::Completed { bind, .. }
            | Self::Closed { bind, .. } => bind,
        }
    }

    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::AwaitingFunding { .. } => "AwaitingFunding",
            Self::FundingExcluded { .. } => "FundingExcluded",
            Self::Intended { .. } => "Intended",
            Self::Submitted { .. } => "Submitted",
            Self::Completed { .. } => "Completed",
            Self::Closed { .. } => "Closed",
        }
    }

    pub(crate) const fn funding_log_id(&self) -> Option<&FundingTransferId> {
        match self {
            Self::AwaitingFunding { .. } => None,
            Self::FundingExcluded { funding_log_id, .. } => {
                Some(funding_log_id)
            }
            Self::Intended { funding_log_id, .. }
            | Self::Submitted { funding_log_id, .. }
            | Self::Completed { funding_log_id, .. }
            | Self::Closed { funding_log_id, .. } => funding_log_id.as_ref(),
        }
    }

    fn apply_event(&mut self, event: BurnExcessEvent) {
        match event {
            BurnExcessEvent::FundingExpected {
                bind,
                reason,
                incident_id,
                expected_at,
            } => {
                *self = Self::AwaitingFunding {
                    bind,
                    reason,
                    incident_id,
                    expected_at,
                };
            }
            BurnExcessEvent::FundingExclusionRecorded {
                bind,
                funding_log_id,
                reason,
                incident_id,
                excluded_at,
            } => {
                *self = Self::FundingExcluded {
                    bind,
                    funding_log_id,
                    reason,
                    incident_id,
                    excluded_at,
                };
            }
            BurnExcessEvent::ExcessBurnIntended {
                bind,
                path,
                funding_log_id,
                reason,
                incident_id,
                sendable_tx,
                acknowledged_inflows,
                intended_at,
            } => {
                *self = Self::Intended {
                    bind,
                    path,
                    funding_log_id,
                    reason,
                    incident_id,
                    sendable_tx,
                    acknowledged_inflows,
                    intended_at,
                    held_redemptions: Vec::new(),
                    held_redemptions_anchored: false,
                };
            }
            BurnExcessEvent::HeldRedemptionsAnchored {
                held_redemptions,
                ..
            } => match self {
                Self::Intended {
                    held_redemptions: anchored,
                    held_redemptions_anchored,
                    ..
                }
                | Self::Submitted {
                    held_redemptions: anchored,
                    held_redemptions_anchored,
                    ..
                } => {
                    *anchored = held_redemptions;
                    *held_redemptions_anchored = true;
                }
                Self::AwaitingFunding { .. }
                | Self::FundingExcluded { .. }
                | Self::Completed { .. }
                | Self::Closed { .. } => {}
            },
            BurnExcessEvent::ExcessBurnSubmitted {
                tx_id,
                burn_tx_hash,
                submitted_at,
            } => {
                let Self::Intended {
                    bind,
                    path,
                    funding_log_id,
                    reason,
                    incident_id,
                    sendable_tx,
                    held_redemptions,
                    acknowledged_inflows,
                    held_redemptions_anchored,
                    intended_at,
                } = self.clone()
                else {
                    return;
                };
                *self = Self::Submitted {
                    bind,
                    path,
                    funding_log_id,
                    reason,
                    incident_id,
                    sendable_tx,
                    held_redemptions,
                    acknowledged_inflows,
                    held_redemptions_anchored,
                    tx_id,
                    burn_tx_hash,
                    intended_at,
                    submitted_at,
                };
            }
            BurnExcessEvent::ExcessBurnCompleted {
                burn_tx_hash,
                block_number,
                completed_at,
            } => {
                let (bind, path, funding_log_id) = match self {
                    Self::Intended { bind, path, funding_log_id, .. }
                    | Self::Submitted { bind, path, funding_log_id, .. } => {
                        (bind.clone(), *path, funding_log_id.clone())
                    }
                    Self::FundingExcluded { bind, funding_log_id, .. } => (
                        bind.clone(),
                        BurnExcessPath::External,
                        Some(funding_log_id.clone()),
                    ),
                    Self::AwaitingFunding { .. }
                    | Self::Completed { .. }
                    | Self::Closed { .. } => return,
                };
                *self = Self::Completed {
                    bind,
                    path,
                    funding_log_id,
                    burn_tx_hash,
                    block_number,
                    completed_at,
                };
            }
            BurnExcessEvent::ExcessBurnClosed {
                reason,
                release_through_block,
                closed_at,
                ..
            } => {
                let (bind, path, funding_log_id) = match self {
                    Self::AwaitingFunding { bind, .. } => {
                        (bind.clone(), BurnExcessPath::External, None)
                    }
                    Self::FundingExcluded { bind, funding_log_id, .. } => (
                        bind.clone(),
                        BurnExcessPath::External,
                        Some(funding_log_id.clone()),
                    ),
                    Self::Intended { bind, path, funding_log_id, .. }
                    | Self::Submitted { bind, path, funding_log_id, .. } => {
                        (bind.clone(), *path, funding_log_id.clone())
                    }
                    Self::Completed { .. } | Self::Closed { .. } => return,
                };
                *self = Self::Closed {
                    bind,
                    path,
                    funding_log_id,
                    reason,
                    release_through_block,
                    closed_at,
                };
            }
        }
    }

    fn handle_record_funding_exclusion(
        bind: ExcessBurnBind,
        funding_log_id: FundingTransferId,
        reason: String,
        incident_id: Option<String>,
    ) -> Vec<BurnExcessEvent> {
        vec![BurnExcessEvent::FundingExclusionRecorded {
            bind,
            funding_log_id,
            reason,
            incident_id,
            excluded_at: Utc::now(),
        }]
    }

    fn handle_intend(
        &self,
        command: BurnExcessCommand,
    ) -> Result<Vec<BurnExcessEvent>, BurnExcessError> {
        let BurnExcessCommand::IntendExcessBurn {
            bind: command_bind,
            path,
            funding_log_id,
            reason,
            incident_id,
            sendable_tx,
            held_redemptions,
            acknowledged_inflows,
        } = command
        else {
            return Err(BurnExcessError::InvalidState {
                expected: "IntendExcessBurn".to_string(),
                found: self.state_name().to_string(),
            });
        };

        match self {
            Self::FundingExcluded {
                bind, funding_log_id: recorded, ..
            } => {
                if path != BurnExcessPath::External {
                    return Err(BurnExcessError::PathConflict {
                        stream_path: BurnExcessPath::External,
                        command_path: path,
                    });
                }
                if command_bind != *bind {
                    return Err(BurnExcessError::BindMismatch);
                }
                match &funding_log_id {
                    Some(command_funding) if command_funding == recorded => {}
                    other => {
                        return Err(BurnExcessError::FundingMismatch {
                            command: other.clone().map(Box::new),
                            recorded: Box::new(recorded.clone()),
                        });
                    }
                }
                let intended_at = Utc::now();
                Ok(vec![
                    BurnExcessEvent::ExcessBurnIntended {
                        bind: command_bind,
                        path,
                        funding_log_id,
                        reason,
                        incident_id,
                        sendable_tx,
                        acknowledged_inflows,
                        intended_at,
                    },
                    BurnExcessEvent::HeldRedemptionsAnchored {
                        held_redemptions,
                        anchored_at: intended_at,
                    },
                ])
            }
            Self::AwaitingFunding { .. } => {
                Err(BurnExcessError::ExternalRequiresExclusion {
                    found: self.state_name().to_string(),
                })
            }
            other => Err(BurnExcessError::InvalidState {
                expected:
                    "FundingExcluded (external) or uninitialized (internal)"
                        .to_string(),
                found: other.state_name().to_string(),
            }),
        }
    }
}

#[async_trait]
impl EventSourced for BurnExcess {
    type Id = BurnExcessId;
    type Event = BurnExcessEvent;
    type Command = BurnExcessCommand;
    type Error = BurnExcessError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "BurnExcess";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;
    const SNAPSHOT_SIZE: usize = usize::MAX;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            BurnExcessEvent::FundingExpected {
                bind,
                reason,
                incident_id,
                expected_at,
            } => Some(Self::AwaitingFunding {
                bind: bind.clone(),
                reason: reason.clone(),
                incident_id: incident_id.clone(),
                expected_at: *expected_at,
            }),
            BurnExcessEvent::FundingExclusionRecorded {
                bind,
                funding_log_id,
                reason,
                incident_id,
                excluded_at,
            } => Some(Self::FundingExcluded {
                bind: bind.clone(),
                funding_log_id: funding_log_id.clone(),
                reason: reason.clone(),
                incident_id: incident_id.clone(),
                excluded_at: *excluded_at,
            }),
            BurnExcessEvent::ExcessBurnIntended {
                bind,
                path,
                funding_log_id,
                reason,
                incident_id,
                sendable_tx,
                acknowledged_inflows,
                intended_at,
            } => Some(Self::Intended {
                bind: bind.clone(),
                path: *path,
                funding_log_id: funding_log_id.clone(),
                reason: reason.clone(),
                incident_id: incident_id.clone(),
                sendable_tx: sendable_tx.clone(),
                acknowledged_inflows: acknowledged_inflows.clone(),
                held_redemptions: Vec::new(),
                held_redemptions_anchored: false,
                intended_at: *intended_at,
            }),
            _ => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        let mut next = entity.clone();
        next.apply_event(event.clone());
        Ok(Some(next))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BurnExcessCommand::ExpectFunding { bind, reason, incident_id } => {
                Ok(vec![BurnExcessEvent::FundingExpected {
                    bind,
                    reason,
                    incident_id,
                    expected_at: Utc::now(),
                }])
            }
            BurnExcessCommand::RecordFundingExclusion {
                bind,
                funding_log_id,
                reason,
                incident_id,
            } => Ok(Self::handle_record_funding_exclusion(
                bind,
                funding_log_id,
                reason,
                incident_id,
            )),
            BurnExcessCommand::IntendExcessBurn {
                bind,
                path,
                funding_log_id,
                reason,
                incident_id,
                sendable_tx,
                held_redemptions,
                acknowledged_inflows,
            } => {
                if path != BurnExcessPath::Internal {
                    return Err(BurnExcessError::ExternalRequiresExclusion {
                        found: "Uninitialized".to_string(),
                    });
                }
                if funding_log_id.is_some() {
                    return Err(BurnExcessError::InternalMustNotExclude);
                }
                let intended_at = Utc::now();
                Ok(vec![
                    BurnExcessEvent::ExcessBurnIntended {
                        bind,
                        path,
                        funding_log_id: None,
                        reason,
                        incident_id,
                        sendable_tx,
                        acknowledged_inflows,
                        intended_at,
                    },
                    BurnExcessEvent::HeldRedemptionsAnchored {
                        held_redemptions,
                        anchored_at: intended_at,
                    },
                ])
            }
            BurnExcessCommand::AnchorHeldRedemptions { .. }
            | BurnExcessCommand::RecordExcessBurnSubmitted { .. }
            | BurnExcessCommand::CompleteExcessBurn { .. }
            | BurnExcessCommand::CloseExcessBurn { .. } => {
                Err(BurnExcessError::InvalidState {
                    expected: "Intended or later".to_string(),
                    found: "Uninitialized".to_string(),
                })
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BurnExcessCommand::ExpectFunding { bind: command_bind, .. } => {
                match self {
                    // Re-expecting the same funding is a no-op, so a retried
                    // request cannot fail on a hold it already placed.
                    Self::AwaitingFunding { bind, .. }
                        if *bind == command_bind =>
                    {
                        Ok(vec![])
                    }
                    Self::AwaitingFunding { .. } => {
                        Err(BurnExcessError::BindMismatch)
                    }
                    other => Err(BurnExcessError::InvalidState {
                        expected: "Uninitialized or AwaitingFunding"
                            .to_string(),
                        found: other.state_name().to_string(),
                    }),
                }
            }
            BurnExcessCommand::RecordFundingExclusion {
                bind: command_bind,
                funding_log_id,
                reason,
                incident_id,
            } => match self {
                Self::AwaitingFunding { bind, .. } if *bind == command_bind => {
                    Ok(Self::handle_record_funding_exclusion(
                        command_bind,
                        funding_log_id,
                        reason,
                        incident_id,
                    ))
                }
                Self::AwaitingFunding { .. } => {
                    Err(BurnExcessError::BindMismatch)
                }
                other => Err(BurnExcessError::InvalidState {
                    expected: "Uninitialized or AwaitingFunding".to_string(),
                    found: other.state_name().to_string(),
                }),
            },
            command @ BurnExcessCommand::IntendExcessBurn { .. } => {
                self.handle_intend(command)
            }
            BurnExcessCommand::AnchorHeldRedemptions { held_redemptions } => {
                match self {
                    Self::Intended {
                        held_redemptions_anchored: false, ..
                    }
                    | Self::Submitted {
                        held_redemptions_anchored: false,
                        ..
                    } => Ok(vec![BurnExcessEvent::HeldRedemptionsAnchored {
                        held_redemptions,
                        anchored_at: Utc::now(),
                    }]),
                    Self::Intended { held_redemptions: anchored, .. }
                    | Self::Submitted { held_redemptions: anchored, .. }
                        if *anchored == held_redemptions =>
                    {
                        Ok(vec![])
                    }
                    Self::Intended { .. } | Self::Submitted { .. } => {
                        Err(BurnExcessError::HeldRedemptionsMismatch)
                    }
                    other => Err(BurnExcessError::InvalidState {
                        expected: "Intended or Submitted".to_string(),
                        found: other.state_name().to_string(),
                    }),
                }
            }
            BurnExcessCommand::RecordExcessBurnSubmitted {
                tx_id,
                burn_tx_hash,
            } => match self {
                Self::Intended { held_redemptions_anchored: true, .. } => {
                    Ok(vec![BurnExcessEvent::ExcessBurnSubmitted {
                        tx_id,
                        burn_tx_hash,
                        submitted_at: Utc::now(),
                    }])
                }
                other => Err(BurnExcessError::InvalidState {
                    expected: "Intended".to_string(),
                    found: other.state_name().to_string(),
                }),
            },
            BurnExcessCommand::CompleteExcessBurn {
                burn_tx_hash,
                block_number,
            } => match self {
                Self::Submitted { held_redemptions_anchored: true, .. }
                | Self::Intended { held_redemptions_anchored: true, .. } => {
                    Ok(vec![BurnExcessEvent::ExcessBurnCompleted {
                        burn_tx_hash,
                        block_number,
                        completed_at: Utc::now(),
                    }])
                }
                other => Err(BurnExcessError::InvalidState {
                    expected: "Intended or Submitted".to_string(),
                    found: other.state_name().to_string(),
                }),
            },
            BurnExcessCommand::CloseExcessBurn {
                reason,
                proof,
                release_through_block,
            } => {
                let allowed = matches!(
                    (self, proof, release_through_block),
                    (
                        Self::AwaitingFunding { .. }
                            | Self::FundingExcluded { .. },
                        BurnExcessCloseProof::Unsigned,
                        Some(_)
                    ) | (
                        Self::Intended { .. } | Self::Submitted { .. },
                        BurnExcessCloseProof::FinalizedReverted
                            | BurnExcessCloseProof::ProvablyDead,
                        Some(_)
                    )
                );
                if !allowed {
                    return Err(BurnExcessError::InvalidState {
                        expected: "closable stream with release boundary and \
                                   state-compatible safety proof"
                            .to_string(),
                        found: format!("{} with {proof:?}", self.state_name()),
                    });
                }
                Ok(vec![BurnExcessEvent::ExcessBurnClosed {
                    reason,
                    proof,
                    release_through_block,
                    closed_at: Utc::now(),
                }])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address, b256};
    use event_sorcery::{LifecycleError, TestHarness};
    use sqlx::sqlite::SqlitePoolOptions;
    use uuid::Uuid;

    use super::*;
    use crate::account::{AlpacaAccountNumber, ClientId};
    use crate::mint::IssuerMintRequestId;

    fn issuer_request() -> IssuerMintRequestId {
        IssuerMintRequestId::new(
            Uuid::parse_str("d3042b2f-4845-4acd-9a67-92d743e4e58c").unwrap(),
        )
    }

    fn sample_bind() -> ExcessBurnBind {
        ExcessBurnBind {
            issuer_request_id: issuer_request(),
            deposit_tx_hash: b256!(
                "0x1bb6afc590e58095099373a8fea2242017b31acc7940bcd0d6b68820ebeb8ebd"
            ),
            receipt_id: U256::from(7u64),
            shares: U256::from(750_000_000_000_000_000u64),
            original_recipient: address!(
                "0xA9C16673F65AE808688cB18952AFE3d9658C808f"
            ),
            vault: address!("0x1111111111111111111111111111111111111111"),
            network: Network::Base,
            chain_id: 8453,
            issuer_wallet: address!(
                "0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"
            ),
        }
    }

    fn funding_id() -> FundingTransferId {
        FundingTransferId {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            log_index: 3,
            from: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            to: address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"),
            amount: U256::from(750_000_000_000_000_000u64),
        }
    }

    fn held_redemption() -> HeldTransferRedemption {
        HeldTransferRedemption {
            transfer: FundingTransferId {
                tx_hash: b256!(
                    "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                ),
                log_index: 4,
                ..funding_id()
            },
            block_number: 100,
            client_id: ClientId::new(),
            alpaca_account: AlpacaAccountNumber("account".into()),
            underlying: UnderlyingSymbol::new("PTY").unwrap(),
            token: TokenSymbol::new("tPTY"),
            burn_mode: VaultMode::VaultDirect,
        }
    }

    fn sample_sendable() -> SendableTxWithHash {
        SendableTxWithHash {
            tx: vec![0xde, 0xad],
            hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            nonce: 7,
            signed_at: Utc::now(),
            dust_shares: U256::ZERO,
        }
    }

    #[tokio::test]
    async fn internal_intend_without_exclusion() {
        let bind = sample_bind();
        let sendable = sample_sendable();

        let events = TestHarness::<BurnExcess>::with(())
            .given_no_previous_events()
            .when(BurnExcessCommand::IntendExcessBurn {
                bind: bind.clone(),
                path: BurnExcessPath::Internal,
                funding_log_id: None,
                reason: "duplicate mint".into(),
                incident_id: Some("inc-1".into()),
                sendable_tx: sendable.clone(),
                held_redemptions: Vec::new(),
                acknowledged_inflows: Vec::new(),
            })
            .await
            .events();

        assert_eq!(events.len(), 2);
        let BurnExcessEvent::ExcessBurnIntended {
            path,
            funding_log_id,
            bind: event_bind,
            sendable_tx,
            ..
        } = &events[0]
        else {
            panic!("expected ExcessBurnIntended, got {:?}", events[0]);
        };
        assert_eq!(*path, BurnExcessPath::Internal);
        assert!(funding_log_id.is_none());
        assert_eq!(event_bind, &bind);
        assert_eq!(sendable_tx, &sendable);
        assert!(matches!(
            &events[1],
            BurnExcessEvent::HeldRedemptionsAnchored {
                held_redemptions,
                ..
            } if held_redemptions.is_empty()
        ));
    }

    #[tokio::test]
    async fn external_requires_exclusion_before_intend() {
        let bind = sample_bind();
        let err = TestHarness::<BurnExcess>::with(())
            .given_no_previous_events()
            .when(BurnExcessCommand::IntendExcessBurn {
                bind: bind.clone(),
                path: BurnExcessPath::External,
                funding_log_id: Some(funding_id()),
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                held_redemptions: Vec::new(),
                acknowledged_inflows: Vec::new(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(
                BurnExcessError::ExternalRequiresExclusion { .. }
            )
        ));
    }

    #[tokio::test]
    async fn external_exclusion_then_intend() {
        let bind = sample_bind();
        let funding = funding_id();
        let held = held_redemption();
        let excluded_at = Utc::now();

        let events = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::FundingExclusionRecorded {
                bind: bind.clone(),
                funding_log_id: funding.clone(),
                reason: "duplicate mint".into(),
                incident_id: None,
                excluded_at,
            }])
            .when(BurnExcessCommand::IntendExcessBurn {
                bind: bind.clone(),
                path: BurnExcessPath::External,
                funding_log_id: Some(funding.clone()),
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                held_redemptions: vec![held.clone()],
                acknowledged_inflows: Vec::new(),
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [
                BurnExcessEvent::ExcessBurnIntended {
                    path: BurnExcessPath::External,
                    funding_log_id: Some(event_funding),
                    ..
                },
                BurnExcessEvent::HeldRedemptionsAnchored {
                    held_redemptions,
                    ..
                },
            ] if *event_funding == funding && held_redemptions == &[held]
        ));
    }

    fn funding_expected(bind: &ExcessBurnBind) -> BurnExcessEvent {
        BurnExcessEvent::FundingExpected {
            bind: bind.clone(),
            reason: "duplicate mint".into(),
            incident_id: None,
            expected_at: Utc::now(),
        }
    }

    fn other_bind() -> ExcessBurnBind {
        ExcessBurnBind { shares: U256::from(1u64), ..sample_bind() }
    }

    /// The expectation names the stream's bind; the exclusion that ends the
    /// hold must be for that same bind, or a different recovery could release
    /// the hold this one placed.
    #[tokio::test]
    async fn awaiting_funding_records_the_exclusion_only_for_its_own_bind() {
        let bind = sample_bind();

        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![funding_expected(&bind)])
            .when(BurnExcessCommand::RecordFundingExclusion {
                bind: other_bind(),
                funding_log_id: funding_id(),
                reason: "duplicate mint".into(),
                incident_id: None,
            })
            .await
            .then_expect_error();
        assert!(matches!(
            err,
            LifecycleError::Apply(BurnExcessError::BindMismatch)
        ));

        let events = TestHarness::<BurnExcess>::with(())
            .given(vec![funding_expected(&bind)])
            .when(BurnExcessCommand::RecordFundingExclusion {
                bind: bind.clone(),
                funding_log_id: funding_id(),
                reason: "duplicate mint".into(),
                incident_id: None,
            })
            .await
            .events();
        assert!(matches!(
            events.as_slice(),
            [BurnExcessEvent::FundingExclusionRecorded { funding_log_id, .. }]
                if *funding_log_id == funding_id()
        ));
    }

    /// An expectation is not an exclusion: nothing may sign a burn until the
    /// funding Transfer is proven and excluded.
    #[tokio::test]
    async fn awaiting_funding_refuses_intend_before_the_exclusion() {
        let bind = sample_bind();
        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![funding_expected(&bind)])
            .when(BurnExcessCommand::IntendExcessBurn {
                bind,
                path: BurnExcessPath::External,
                funding_log_id: Some(funding_id()),
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                held_redemptions: Vec::new(),
                acknowledged_inflows: Vec::new(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(
                BurnExcessError::ExternalRequiresExclusion { .. }
            )
        ));
    }

    /// A retried expect request must not fail on the hold it already placed,
    /// but it cannot re-point that hold at a different bind.
    #[tokio::test]
    async fn re_expecting_funding_is_a_no_op_only_for_the_same_bind() {
        let bind = sample_bind();

        let events = TestHarness::<BurnExcess>::with(())
            .given(vec![funding_expected(&bind)])
            .when(BurnExcessCommand::ExpectFunding {
                bind: bind.clone(),
                reason: "retry".into(),
                incident_id: None,
            })
            .await
            .events();
        assert!(events.is_empty());

        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![funding_expected(&bind)])
            .when(BurnExcessCommand::ExpectFunding {
                bind: other_bind(),
                reason: "retry".into(),
                incident_id: None,
            })
            .await
            .then_expect_error();
        assert!(matches!(
            err,
            LifecycleError::Apply(BurnExcessError::BindMismatch)
        ));
    }

    /// Closing an expectation that was never funded (or should redeem after
    /// all) is the way to release its hold.
    #[tokio::test]
    async fn awaiting_funding_can_be_closed() {
        let events = TestHarness::<BurnExcess>::with(())
            .given(vec![funding_expected(&sample_bind())])
            .when(BurnExcessCommand::CloseExcessBurn {
                reason: "funding never sent".into(),
                proof: BurnExcessCloseProof::Unsigned,
                release_through_block: Some(100),
            })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [BurnExcessEvent::ExcessBurnClosed { .. }]
        ));
    }

    #[tokio::test]
    async fn path_conflict_on_intend_with_wrong_path() {
        let bind = sample_bind();
        let funding = funding_id();
        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::FundingExclusionRecorded {
                bind: bind.clone(),
                funding_log_id: funding,
                reason: "duplicate mint".into(),
                incident_id: None,
                excluded_at: Utc::now(),
            }])
            .when(BurnExcessCommand::IntendExcessBurn {
                bind,
                path: BurnExcessPath::Internal,
                funding_log_id: None,
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                held_redemptions: Vec::new(),
                acknowledged_inflows: Vec::new(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(BurnExcessError::PathConflict {
                stream_path: BurnExcessPath::External,
                command_path: BurnExcessPath::Internal,
            })
        ));
    }

    #[tokio::test]
    async fn bind_mismatch_on_intend_with_wrong_bind() {
        let bind = sample_bind();
        let mut wrong_bind = bind.clone();
        wrong_bind.receipt_id = U256::from(999u64);
        let funding = funding_id();
        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::FundingExclusionRecorded {
                bind: bind.clone(),
                funding_log_id: funding.clone(),
                reason: "duplicate mint".into(),
                incident_id: None,
                excluded_at: Utc::now(),
            }])
            .when(BurnExcessCommand::IntendExcessBurn {
                bind: wrong_bind,
                path: BurnExcessPath::External,
                funding_log_id: Some(funding),
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                held_redemptions: Vec::new(),
                acknowledged_inflows: Vec::new(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(BurnExcessError::BindMismatch)
        ));
    }

    #[tokio::test]
    async fn funding_mismatch_on_intend_with_wrong_funding() {
        let bind = sample_bind();
        let funding = funding_id();
        let mut wrong_funding = funding.clone();
        wrong_funding.log_index = 99;
        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::FundingExclusionRecorded {
                bind: bind.clone(),
                funding_log_id: funding,
                reason: "duplicate mint".into(),
                incident_id: None,
                excluded_at: Utc::now(),
            }])
            .when(BurnExcessCommand::IntendExcessBurn {
                bind,
                path: BurnExcessPath::External,
                funding_log_id: Some(wrong_funding),
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                held_redemptions: Vec::new(),
                acknowledged_inflows: Vec::new(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(BurnExcessError::FundingMismatch { .. })
        ));
    }

    /// `handle_intend` accepts only `FundingExcluded`. On an already-`Intended`
    /// stream the fall-through arm is what stops a second signed transaction
    /// against the same issuer wallet nonce, so an identical retry of the
    /// command must be refused rather than re-signed.
    #[tokio::test]
    async fn intend_refuses_a_second_intent_on_an_intended_stream() {
        let bind = sample_bind();
        let funding = funding_id();
        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![
                BurnExcessEvent::FundingExclusionRecorded {
                    bind: bind.clone(),
                    funding_log_id: funding.clone(),
                    reason: "duplicate mint".into(),
                    incident_id: None,
                    excluded_at: Utc::now(),
                },
                BurnExcessEvent::ExcessBurnIntended {
                    bind: bind.clone(),
                    path: BurnExcessPath::External,
                    funding_log_id: Some(funding.clone()),
                    reason: "duplicate mint".into(),
                    incident_id: None,
                    sendable_tx: sample_sendable(),
                    acknowledged_inflows: Vec::new(),
                    intended_at: Utc::now(),
                },
            ])
            .when(BurnExcessCommand::IntendExcessBurn {
                bind,
                path: BurnExcessPath::External,
                funding_log_id: Some(funding),
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                held_redemptions: Vec::new(),
                acknowledged_inflows: Vec::new(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                err,
                LifecycleError::Apply(BurnExcessError::InvalidState { .. })
            ),
            "a second IntendExcessBurn must not re-sign against a live intent"
        );
    }

    /// The recorded exclusion is what the poller skips on, so a live stream
    /// must not be able to swap in a second, different funding log.
    #[tokio::test]
    async fn record_funding_exclusion_refuses_a_second_funding_log() {
        let bind = sample_bind();
        let other_funding = FundingTransferId {
            log_index: funding_id().log_index + 1,
            ..funding_id()
        };

        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::FundingExclusionRecorded {
                bind: bind.clone(),
                funding_log_id: funding_id(),
                reason: "duplicate mint".into(),
                incident_id: None,
                excluded_at: Utc::now(),
            }])
            .when(BurnExcessCommand::RecordFundingExclusion {
                bind,
                funding_log_id: other_funding,
                reason: "duplicate mint".into(),
                incident_id: None,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                err,
                LifecycleError::Apply(BurnExcessError::InvalidState { .. })
            ),
            "a second funding log on a live stream must be refused, got {err:?}"
        );
    }

    #[tokio::test]
    async fn signed_intent_cannot_be_closed() {
        let bind = sample_bind();
        let err = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::ExcessBurnIntended {
                bind,
                path: BurnExcessPath::Internal,
                funding_log_id: None,
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                acknowledged_inflows: Vec::new(),
                intended_at: Utc::now(),
            }])
            .when(BurnExcessCommand::CloseExcessBurn {
                reason: "abandoned".into(),
                proof: BurnExcessCloseProof::Unsigned,
                release_through_block: Some(100),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(BurnExcessError::InvalidState { .. })
        ));
    }

    #[tokio::test]
    async fn submit_and_complete_lifecycle() {
        let bind = sample_bind();
        let intended_at = Utc::now();
        let sendable = sample_sendable();

        let submitted = TestHarness::<BurnExcess>::with(())
            .given(vec![
                BurnExcessEvent::ExcessBurnIntended {
                    bind: bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "duplicate mint".into(),
                    incident_id: None,
                    sendable_tx: sendable.clone(),
                    acknowledged_inflows: Vec::new(),
                    intended_at,
                },
                BurnExcessEvent::HeldRedemptionsAnchored {
                    held_redemptions: Vec::new(),
                    anchored_at: Utc::now(),
                },
            ])
            .when(BurnExcessCommand::RecordExcessBurnSubmitted {
                tx_id: TxId::from(sendable.hash),
                burn_tx_hash: sendable.hash,
            })
            .await
            .events();
        assert!(matches!(
            &submitted[0],
            BurnExcessEvent::ExcessBurnSubmitted { burn_tx_hash, .. }
                if *burn_tx_hash == sendable.hash
        ));

        let completed = TestHarness::<BurnExcess>::with(())
            .given(vec![
                BurnExcessEvent::ExcessBurnIntended {
                    bind: bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "duplicate mint".into(),
                    incident_id: None,
                    sendable_tx: sendable.clone(),
                    acknowledged_inflows: Vec::new(),
                    intended_at,
                },
                BurnExcessEvent::HeldRedemptionsAnchored {
                    held_redemptions: Vec::new(),
                    anchored_at: Utc::now(),
                },
                BurnExcessEvent::ExcessBurnSubmitted {
                    tx_id: TxId::from(sendable.hash),
                    burn_tx_hash: sendable.hash,
                    submitted_at: Utc::now(),
                },
            ])
            .when(BurnExcessCommand::CompleteExcessBurn {
                burn_tx_hash: sendable.hash,
                block_number: 99,
            })
            .await
            .events();
        assert!(matches!(
            &completed[0],
            BurnExcessEvent::ExcessBurnCompleted {
                burn_tx_hash,
                block_number: 99,
                ..
            } if *burn_tx_hash == sendable.hash
        ));
    }

    #[tokio::test]
    async fn legacy_submitted_stream_can_anchor_empty_before_completion() {
        let bind = sample_bind();
        let sendable = sample_sendable();
        let legacy_events = vec![
            BurnExcessEvent::ExcessBurnIntended {
                bind,
                path: BurnExcessPath::Internal,
                funding_log_id: None,
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sendable.clone(),
                acknowledged_inflows: Vec::new(),
                intended_at: Utc::now(),
            },
            BurnExcessEvent::ExcessBurnSubmitted {
                tx_id: TxId::from(sendable.hash),
                burn_tx_hash: sendable.hash,
                submitted_at: Utc::now(),
            },
        ];
        let anchored = TestHarness::<BurnExcess>::with(())
            .given(legacy_events.clone())
            .when(BurnExcessCommand::AnchorHeldRedemptions {
                held_redemptions: Vec::new(),
            })
            .await
            .events();
        assert!(matches!(
            anchored.as_slice(),
            [BurnExcessEvent::HeldRedemptionsAnchored {
                held_redemptions,
                ..
            }] if held_redemptions.is_empty()
        ));

        let completed = TestHarness::<BurnExcess>::with(())
            .given(
                legacy_events.into_iter().chain(anchored).collect::<Vec<_>>(),
            )
            .when(BurnExcessCommand::CompleteExcessBurn {
                burn_tx_hash: sendable.hash,
                block_number: 99,
            })
            .await
            .events();
        assert!(matches!(
            completed.as_slice(),
            [BurnExcessEvent::ExcessBurnCompleted { .. }]
        ));
    }

    #[test]
    fn serde_round_trip_path_and_events() {
        let intended = BurnExcessEvent::ExcessBurnIntended {
            bind: sample_bind(),
            path: BurnExcessPath::External,
            funding_log_id: Some(funding_id()),
            reason: "r".into(),
            incident_id: Some("i".into()),
            sendable_tx: sample_sendable(),
            acknowledged_inflows: Vec::new(),
            intended_at: Utc::now(),
        };
        let json = serde_json::to_string(&intended).unwrap();
        let back: BurnExcessEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, intended);

        let path_json =
            serde_json::to_string(&BurnExcessPath::Internal).unwrap();
        assert_eq!(path_json, "\"internal\"");
    }

    #[test]
    fn burn_excess_id_display_parse() {
        let hash = b256!(
            "0x1bb6afc590e58095099373a8fea2242017b31acc7940bcd0d6b68820ebeb8ebd"
        );
        let id = BurnExcessId::new(hash);
        let parsed: BurnExcessId = id.to_string().parse().unwrap();
        assert_eq!(parsed.deposit_tx_hash(), hash);
    }

    #[tokio::test]
    async fn independent_stores_cannot_open_one_burn_excess_network() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let first_store =
            event_sorcery::test_store::<BurnExcess>(pool.clone(), ());
        let second_store =
            event_sorcery::test_store::<BurnExcess>(pool.clone(), ());
        let first_bind = sample_bind();
        let first_id = BurnExcessId::new(first_bind.deposit_tx_hash);
        first_store
            .send(
                &first_id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: first_bind,
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "first".into(),
                    incident_id: None,
                    sendable_tx: sample_sendable(),
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();

        let second_bind = ExcessBurnBind {
            deposit_tx_hash: b256!(
                "0x2bb6afc590e58095099373a8fea2242017b31acc7940bcd0d6b68820ebeb8ebd"
            ),
            ..sample_bind()
        };
        let second_id = BurnExcessId::new(second_bind.deposit_tx_hash);
        let competing_error = second_store
            .send(
                &second_id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: second_bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "second".into(),
                    incident_id: None,
                    sendable_tx: sample_sendable(),
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(
            format!("{competing_error:?}")
                .contains("another unresolved burn-excess stream"),
            "database arbitration must expose the stream conflict: \
             {competing_error:?}"
        );
        assert!(second_store.load(&second_id).await.unwrap().is_none());
        let burn_tx_hash = sample_sendable().hash;
        first_store
            .send(
                &first_id,
                BurnExcessCommand::CompleteExcessBurn {
                    burn_tx_hash,
                    block_number: 1,
                },
            )
            .await
            .unwrap();
        second_store
            .send(
                &second_id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: second_bind,
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "second".into(),
                    incident_id: None,
                    sendable_tx: sample_sendable(),
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn signer_reservation_migration_leaves_unsigned_exclusion_unreserved()
    {
        const MIGRATION: &str = include_str!(
            "../../migrations/20261007193733_reserve_burn_excess_signer_intents.sql"
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "
            CREATE TABLE events (
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                sequence INTEGER,
                payload JSON NOT NULL
            );
            CREATE TABLE burn_excess_funding_expectations (
                deposit_tx_hash TEXT PRIMARY KEY,
                network TEXT NOT NULL,
                vault TEXT NOT NULL,
                from_address TEXT NOT NULL,
                to_address TEXT NOT NULL,
                amount TEXT NOT NULL
            );
            CREATE TABLE tokenized_asset_vault_owners (
                network TEXT NOT NULL,
                vault TEXT NOT NULL,
                aggregate_id TEXT NOT NULL
            );
            CREATE TABLE active_signer_intents (
                network TEXT NOT NULL PRIMARY KEY,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                UNIQUE (aggregate_type, aggregate_id)
            );
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        let bind = sample_bind();
        let aggregate_id = BurnExcessId::new(bind.deposit_tx_hash).to_string();
        let payload =
            serde_json::to_string(&BurnExcessEvent::FundingExclusionRecorded {
                bind,
                funding_log_id: funding_id(),
                reason: "duplicate mint".into(),
                incident_id: None,
                excluded_at: Utc::now(),
            })
            .unwrap();
        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                event_type,
                payload
            )
            VALUES (
                'BurnExcess',
                ?,
                'BurnExcessEvent::FundingExclusionRecorded',
                ?
            )
            ",
        )
        .bind(&aggregate_id)
        .bind(payload)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO active_signer_intents (
                network,
                aggregate_type,
                aggregate_id
            )
            VALUES ('base', 'Redemption', 'existing-redemption')
            ",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(MIGRATION).execute(&pool).await.unwrap();
        let reservation: (String, String, String) = sqlx::query_as(
            "
            SELECT network, aggregate_type, aggregate_id
            FROM active_signer_intents
            ",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            reservation,
            ("base".into(), "Redemption".into(), "existing-redemption".into())
        );

        let invalid_close = sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                event_type,
                payload
            )
            VALUES (
                'BurnExcess',
                ?,
                'BurnExcessEvent::ExcessBurnClosed',
                '{\"ExcessBurnClosed\":{\"reason\":\"unsafe\"}}'
            )
            ",
        )
        .bind(&aggregate_id)
        .execute(&pool)
        .await;
        assert!(invalid_close.is_err());
        let reservation_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM active_signer_intents
            WHERE aggregate_type = 'BurnExcess'
              AND aggregate_id = ?
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(reservation_count, 0);

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                event_type,
                payload
            )
            VALUES (
                'BurnExcess',
                ?,
                'BurnExcessEvent::ExcessBurnClosed',
                '{\"ExcessBurnClosed\":{
                    \"reason\":\"safe\",
                    \"proof\":\"unsigned\",
                    \"release_through_block\":100
                }}'
            )
            ",
        )
        .bind(&aggregate_id)
        .execute(&pool)
        .await
        .unwrap();
        let reservation_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM active_signer_intents
            WHERE aggregate_type = 'BurnExcess'
              AND aggregate_id = ?
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(reservation_count, 0);
    }

    #[tokio::test]
    async fn signer_reservation_migration_reports_burn_excess_collision() {
        const MIGRATION: &str = include_str!(
            "../../migrations/20261007193733_reserve_burn_excess_signer_intents.sql"
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "
            CREATE TABLE events (
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                sequence INTEGER,
                payload JSON NOT NULL
            );
            CREATE TABLE burn_excess_funding_expectations (
                deposit_tx_hash TEXT PRIMARY KEY,
                network TEXT NOT NULL,
                vault TEXT NOT NULL,
                from_address TEXT NOT NULL,
                to_address TEXT NOT NULL,
                amount TEXT NOT NULL
            );
            CREATE TABLE tokenized_asset_vault_owners (
                network TEXT NOT NULL,
                vault TEXT NOT NULL,
                aggregate_id TEXT NOT NULL
            );
            CREATE TABLE active_signer_intents (
                network TEXT NOT NULL PRIMARY KEY,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                UNIQUE (aggregate_type, aggregate_id)
            );
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                event_type,
                payload
            )
            VALUES
                (
                    'BurnExcess',
                    'first',
                    'BurnExcessEvent::FundingExclusionRecorded',
                    '{\"FundingExclusionRecorded\":{\"bind\":{\"network\":\"base\"}}}'
                ),
                (
                    'BurnExcess',
                    'second',
                    'BurnExcessEvent::FundingExclusionRecorded',
                    '{\"FundingExclusionRecorded\":{\"bind\":{\"network\":\"base\"}}}'
                );
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        let error = sqlx::raw_sql(MIGRATION).execute(&pool).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("burn-excess network backfill collision"),
            "migration must identify conflicting unresolved streams: {error}"
        );
    }

    #[tokio::test]
    async fn signer_reservation_migration_releases_legacy_signed_close() {
        const MIGRATION: &str = include_str!(
            "../../migrations/20261007193733_reserve_burn_excess_signer_intents.sql"
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "
            CREATE TABLE events (
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                sequence INTEGER,
                payload JSON NOT NULL
            );
            CREATE TABLE burn_excess_funding_expectations (
                deposit_tx_hash TEXT PRIMARY KEY,
                network TEXT NOT NULL,
                vault TEXT NOT NULL,
                from_address TEXT NOT NULL,
                to_address TEXT NOT NULL,
                amount TEXT NOT NULL
            );
            CREATE TABLE tokenized_asset_vault_owners (
                network TEXT NOT NULL,
                vault TEXT NOT NULL,
                aggregate_id TEXT NOT NULL
            );
            CREATE TABLE active_signer_intents (
                network TEXT NOT NULL PRIMARY KEY,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                UNIQUE (aggregate_type, aggregate_id)
            );
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        let bind = sample_bind();
        let aggregate_id = BurnExcessId::new(bind.deposit_tx_hash).to_string();
        let intended =
            serde_json::to_string(&BurnExcessEvent::ExcessBurnIntended {
                bind,
                path: BurnExcessPath::Internal,
                funding_log_id: None,
                reason: "legacy signed close".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                acknowledged_inflows: Vec::new(),
                intended_at: Utc::now(),
            })
            .unwrap();
        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                event_type,
                payload
            )
            VALUES
                (
                    'BurnExcess',
                    ?,
                    'BurnExcessEvent::ExcessBurnIntended',
                    ?
                ),
                (
                    'BurnExcess',
                    ?,
                    'BurnExcessEvent::ExcessBurnClosed',
                    '{\"ExcessBurnClosed\":{\"reason\":\"legacy\"}}'
                )
            ",
        )
        .bind(&aggregate_id)
        .bind(intended)
        .bind(&aggregate_id)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::raw_sql(MIGRATION).execute(&pool).await.unwrap();
        let reservation_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM active_signer_intents
            WHERE aggregate_type = 'BurnExcess'
              AND aggregate_id = ?
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(reservation_count, 0);
    }

    #[test]
    fn path_and_funding_accessors() {
        let excluded = BurnExcess::FundingExcluded {
            bind: sample_bind(),
            funding_log_id: funding_id(),
            reason: "r".into(),
            incident_id: None,
            excluded_at: Utc::now(),
        };
        assert_eq!(excluded.path(), BurnExcessPath::External);
        assert_eq!(excluded.funding_log_id(), Some(&funding_id()));
        assert_eq!(excluded.state_name(), "FundingExcluded");

        let intended = BurnExcess::Intended {
            bind: sample_bind(),
            path: BurnExcessPath::Internal,
            funding_log_id: None,
            reason: "r".into(),
            incident_id: None,
            sendable_tx: sample_sendable(),
            held_redemptions: Vec::new(),
            held_redemptions_anchored: true,
            acknowledged_inflows: Vec::new(),
            intended_at: Utc::now(),
        };
        assert_eq!(intended.path(), BurnExcessPath::Internal);
        assert!(intended.funding_log_id().is_none());
    }
}
