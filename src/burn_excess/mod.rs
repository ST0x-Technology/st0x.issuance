//! Administrative burn of excess shares from a proven duplicate mint.
//!
//! See SPEC.md "Burn excess shares". Path A (`internal`) burns when the issuer
//! already holds the excess; Path B (`external`) records a funding-transfer
//! exclusion then burns. Never Alpaca; never a `Redemption` aggregate.

pub(crate) mod cli;
mod cmd;
pub(crate) mod engine;
mod event;
pub(crate) mod exclusion;
pub(crate) mod proof;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EventSourced, Nil};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use crate::mint::IssuerMintRequestId;
use crate::tokenized_asset::Network;
use crate::vault::{SendableTxWithHash, TxId};

pub(crate) use cmd::BurnExcessCommand;
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
        intended_at: DateTime<Utc>,
    },
    Submitted {
        bind: ExcessBurnBind,
        path: BurnExcessPath,
        funding_log_id: Option<FundingTransferId>,
        reason: String,
        incident_id: Option<String>,
        sendable_tx: SendableTxWithHash,
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
}

impl BurnExcess {
    pub(crate) const fn path(&self) -> BurnExcessPath {
        match self {
            Self::FundingExcluded { .. } => BurnExcessPath::External,
            Self::Intended { path, .. }
            | Self::Submitted { path, .. }
            | Self::Completed { path, .. }
            | Self::Closed { path, .. } => *path,
        }
    }

    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::FundingExcluded { .. } => "FundingExcluded",
            Self::Intended { .. } => "Intended",
            Self::Submitted { .. } => "Submitted",
            Self::Completed { .. } => "Completed",
            Self::Closed { .. } => "Closed",
        }
    }

    pub(crate) const fn funding_log_id(&self) -> Option<&FundingTransferId> {
        match self {
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
                intended_at,
            } => {
                *self = Self::Intended {
                    bind,
                    path,
                    funding_log_id,
                    reason,
                    incident_id,
                    sendable_tx,
                    intended_at,
                };
            }
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
                    Self::Completed { .. } | Self::Closed { .. } => return,
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
            BurnExcessEvent::ExcessBurnClosed { reason, closed_at } => {
                let (bind, path, funding_log_id) = match self {
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
                Ok(vec![BurnExcessEvent::ExcessBurnIntended {
                    bind: command_bind,
                    path,
                    funding_log_id,
                    reason,
                    incident_id,
                    sendable_tx,
                    intended_at: Utc::now(),
                }])
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

/// Returns whether any excess-burn stream is an unresolved recovery in
/// progress, without a terminal complete/close.
///
/// `Intended` / `Submitted` hold a signed nonce. `FundingExcluded` holds no
/// signed transaction yet, but its exclusion write is already permanent and the
/// stream will sign against the same issuer wallet, so it counts too: a Path B
/// recovery abandoned before intend must be resumed or `--close`d, never raced
/// by a second recovery or by mint/redemption burn prepare.
pub(crate) async fn has_unresolved_excess_burn_intent(
    pool: &Pool<Sqlite>,
    excluding: Option<&BurnExcessId>,
) -> Result<bool, sqlx::Error> {
    let excluding = excluding.map(ToString::to_string).unwrap_or_default();
    let exists = sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM events AS intent
            WHERE intent.aggregate_type = 'BurnExcess'
              AND intent.event_type IN (?, ?, ?)
              AND intent.aggregate_id != ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM events AS terminal
                  WHERE terminal.aggregate_type = intent.aggregate_type
                    AND terminal.aggregate_id = intent.aggregate_id
                    AND terminal.event_type IN (?, ?)
              )
        )
        ",
    )
    .bind(BurnExcessEvent::FUNDING_EXCLUSION_RECORDED)
    .bind(BurnExcessEvent::EXCESS_BURN_INTENDED)
    .bind(BurnExcessEvent::EXCESS_BURN_SUBMITTED)
    .bind(excluding)
    .bind(BurnExcessEvent::EXCESS_BURN_COMPLETED)
    .bind(BurnExcessEvent::EXCESS_BURN_CLOSED)
    .fetch_one(pool)
    .await?;

    Ok(exists)
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
                intended_at,
            } => Some(Self::Intended {
                bind: bind.clone(),
                path: *path,
                funding_log_id: funding_log_id.clone(),
                reason: reason.clone(),
                incident_id: incident_id.clone(),
                sendable_tx: sendable_tx.clone(),
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
            } => {
                if path != BurnExcessPath::Internal {
                    return Err(BurnExcessError::ExternalRequiresExclusion {
                        found: "Uninitialized".to_string(),
                    });
                }
                if funding_log_id.is_some() {
                    return Err(BurnExcessError::InternalMustNotExclude);
                }
                Ok(vec![BurnExcessEvent::ExcessBurnIntended {
                    bind,
                    path,
                    funding_log_id: None,
                    reason,
                    incident_id,
                    sendable_tx,
                    intended_at: Utc::now(),
                }])
            }
            BurnExcessCommand::RecordExcessBurnSubmitted { .. }
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
            BurnExcessCommand::RecordFundingExclusion { .. } => {
                Err(BurnExcessError::InvalidState {
                    expected: "Uninitialized".to_string(),
                    found: self.state_name().to_string(),
                })
            }
            command @ BurnExcessCommand::IntendExcessBurn { .. } => {
                self.handle_intend(command)
            }
            BurnExcessCommand::RecordExcessBurnSubmitted {
                tx_id,
                burn_tx_hash,
            } => match self {
                Self::Intended { .. } => {
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
                Self::Submitted { .. } | Self::Intended { .. } => {
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
            BurnExcessCommand::CloseExcessBurn { reason } => match self {
                Self::FundingExcluded { .. }
                | Self::Intended { .. }
                | Self::Submitted { .. } => {
                    Ok(vec![BurnExcessEvent::ExcessBurnClosed {
                        reason,
                        closed_at: Utc::now(),
                    }])
                }
                other => Err(BurnExcessError::InvalidState {
                    expected: "FundingExcluded, Intended, or Submitted"
                        .to_string(),
                    found: other.state_name().to_string(),
                }),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address, b256};
    use event_sorcery::{LifecycleError, TestHarness};
    use uuid::Uuid;

    use super::*;
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
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
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
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let BurnExcessEvent::ExcessBurnIntended {
            path, funding_log_id, ..
        } = &events[0]
        else {
            panic!("expected ExcessBurnIntended");
        };
        assert_eq!(*path, BurnExcessPath::External);
        assert_eq!(funding_log_id.as_ref(), Some(&funding));
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
    async fn close_from_intended() {
        let bind = sample_bind();
        let intended_at = Utc::now();
        let events = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::ExcessBurnIntended {
                bind: bind.clone(),
                path: BurnExcessPath::Internal,
                funding_log_id: None,
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sample_sendable(),
                intended_at,
            }])
            .when(BurnExcessCommand::CloseExcessBurn {
                reason: "abandoned".into(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            BurnExcessEvent::ExcessBurnClosed { reason, .. }
                if reason == "abandoned"
        ));
    }

    #[tokio::test]
    async fn submit_and_complete_lifecycle() {
        let bind = sample_bind();
        let intended_at = Utc::now();
        let sendable = sample_sendable();

        let submitted = TestHarness::<BurnExcess>::with(())
            .given(vec![BurnExcessEvent::ExcessBurnIntended {
                bind: bind.clone(),
                path: BurnExcessPath::Internal,
                funding_log_id: None,
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: sendable.clone(),
                intended_at,
            }])
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
                    intended_at,
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

    #[test]
    fn serde_round_trip_path_and_events() {
        let intended = BurnExcessEvent::ExcessBurnIntended {
            bind: sample_bind(),
            path: BurnExcessPath::External,
            funding_log_id: Some(funding_id()),
            reason: "r".into(),
            incident_id: Some("i".into()),
            sendable_tx: sample_sendable(),
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
    async fn unresolved_excess_burn_intent_tracks_intended_and_submitted() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        assert!(!has_unresolved_excess_burn_intent(&pool, None).await.unwrap());

        let store = event_sorcery::test_store::<BurnExcess>(pool.clone(), ());
        let bind = sample_bind();
        let id = BurnExcessId::new(bind.deposit_tx_hash);
        store
            .send(
                &id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "dup".into(),
                    incident_id: None,
                    sendable_tx: sample_sendable(),
                },
            )
            .await
            .unwrap();

        assert!(has_unresolved_excess_burn_intent(&pool, None).await.unwrap());
        assert!(
            !has_unresolved_excess_burn_intent(&pool, Some(&id)).await.unwrap()
        );

        store
            .send(
                &id,
                BurnExcessCommand::CloseExcessBurn { reason: "done".into() },
            )
            .await
            .unwrap();
        assert!(!has_unresolved_excess_burn_intent(&pool, None).await.unwrap());
    }

    #[tokio::test]
    async fn unresolved_excess_burn_intent_tracks_funding_excluded() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let store = event_sorcery::test_store::<BurnExcess>(pool.clone(), ());
        let bind = sample_bind();
        let id = BurnExcessId::new(bind.deposit_tx_hash);
        store
            .send(
                &id,
                BurnExcessCommand::RecordFundingExclusion {
                    bind: bind.clone(),
                    funding_log_id: funding_id(),
                    reason: "dup".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();

        // Path B before intend holds no signed nonce, but the exclusion write
        // is permanent and the stream still owns the issuer wallet.
        assert!(has_unresolved_excess_burn_intent(&pool, None).await.unwrap());
        assert!(
            !has_unresolved_excess_burn_intent(&pool, Some(&id)).await.unwrap()
        );

        store
            .send(
                &id,
                BurnExcessCommand::CloseExcessBurn {
                    reason: "abandoned".into(),
                },
            )
            .await
            .unwrap();
        assert!(!has_unresolved_excess_burn_intent(&pool, None).await.unwrap());
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
            intended_at: Utc::now(),
        };
        assert_eq!(intended.path(), BurnExcessPath::Internal);
        assert!(intended.funding_log_id().is_none());
    }
}
