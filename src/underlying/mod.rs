//! Per-underlying corporate-action state.
//!
//! A scheduled corporate action (dividend record date, split) is an event on
//! the underlying equity, so it applies to every tokenization of that equity
//! on every network. Keying the freeze by bare [`UnderlyingSymbol`] makes a
//! cross-network freeze divergence unrepresentable — per-network listing state
//! (vault, token symbol) lives on `TokenizedAsset` instead.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use event_sorcery::{EventSourced, Never, Table};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::collections::BTreeSet;

use st0x_issuance_dto::TokenizedAssetStatus;
pub use st0x_issuance_dto::UnderlyingSymbol;

/// Whether an underlying accepts new mints, on any network.
///
/// `Frozen` gates *only* new minting — every listing of a frozen underlying
/// stays supported and in the `list_enabled_assets()` set so in-flight
/// redemptions still detect and complete. This is orthogonal to listing; see
/// the freeze invariant in SPEC.md. Serializes to the bare strings
/// `"Enabled"` / `"Frozen"`, which the view queries match against
/// `$.Live.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AssetStatus {
    Enabled,
    Frozen,
}

impl AssetStatus {
    /// Whether new mints are currently rejected for this underlying.
    pub(crate) const fn is_frozen(self) -> bool {
        matches!(self, Self::Frozen)
    }
}

/// Maps the domain freeze state onto its wire representation. The two enums are
/// kept distinct (domain vs API contract) but always move in lock-step.
impl From<AssetStatus> for TokenizedAssetStatus {
    fn from(status: AssetStatus) -> Self {
        match status {
            AssetStatus::Enabled => Self::Enabled,
            AssetStatus::Frozen => Self::Frozen,
        }
    }
}

/// A validated interval during which a corporate action owns the supply gate.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct FreezeWindow {
    freeze_at: DateTime<Utc>,
    unfreeze_at: DateTime<Utc>,
}

impl FreezeWindow {
    pub(crate) fn new(
        freeze_at: DateTime<Utc>,
        unfreeze_at: DateTime<Utc>,
    ) -> Option<Self> {
        (freeze_at < unfreeze_at).then_some(Self { freeze_at, unfreeze_at })
    }

    pub(crate) const fn freeze_at(&self) -> DateTime<Utc> {
        self.freeze_at
    }

    pub(crate) const fn unfreeze_at(&self) -> DateTime<Utc> {
        self.unfreeze_at
    }
}

/// Stable identity of one independent requirement to keep an underlying frozen.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct FreezeHoldId(FreezeHoldSource);

impl FreezeHoldId {
    pub(crate) const fn operator() -> Self {
        Self(FreezeHoldSource::Operator)
    }

    pub(crate) const fn corporate_action(window: FreezeWindow) -> Self {
        Self(FreezeHoldSource::CorporateAction(window))
    }

    pub(crate) fn is_expired_at(self, now: DateTime<Utc>) -> bool {
        match self.0 {
            FreezeHoldSource::Operator => false,
            FreezeHoldSource::CorporateAction(window) => {
                window.unfreeze_at() <= now
            }
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
enum FreezeHoldSource {
    Operator,
    CorporateAction(FreezeWindow),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct ActiveFreezeHolds(BTreeSet<FreezeHoldId>);

impl ActiveFreezeHolds {
    fn one(hold_id: FreezeHoldId) -> Self {
        Self(BTreeSet::from([hold_id]))
    }

    fn legacy_operator() -> Self {
        Self::one(FreezeHoldId::operator())
    }

    fn contains(&self, hold_id: &FreezeHoldId) -> bool {
        self.0.contains(hold_id)
    }

    fn with(&self, hold_id: FreezeHoldId) -> Self {
        let mut holds = self.0.clone();
        holds.insert(hold_id);
        Self(holds)
    }

    fn without(&self, hold_id: &FreezeHoldId) -> Option<Self> {
        let mut holds = self.0.clone();
        holds.remove(hold_id);
        (!holds.is_empty()).then_some(Self(holds))
    }
}

impl<'de> Deserialize<'de> for ActiveFreezeHolds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let holds = BTreeSet::<FreezeHoldId>::deserialize(deserializer)?;
        if holds.is_empty() {
            return Err(serde::de::Error::custom(
                "a frozen underlying must have at least one active freeze hold",
            ));
        }
        Ok(Self(holds))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status")]
enum FreezeState {
    Enabled,
    Frozen {
        #[serde(default = "ActiveFreezeHolds::legacy_operator")]
        active_freeze_holds: ActiveFreezeHolds,
    },
}

impl FreezeState {
    const fn status(&self) -> AssetStatus {
        match self {
            Self::Enabled => AssetStatus::Enabled,
            Self::Frozen { .. } => AssetStatus::Frozen,
        }
    }

    fn contains(&self, hold_id: &FreezeHoldId) -> bool {
        match self {
            Self::Enabled => false,
            Self::Frozen { active_freeze_holds } => {
                active_freeze_holds.contains(hold_id)
            }
        }
    }

    fn acquire(&self, hold_id: FreezeHoldId) -> Option<Self> {
        match self {
            Self::Enabled => Some(Self::Frozen {
                active_freeze_holds: ActiveFreezeHolds::one(hold_id),
            }),
            Self::Frozen { active_freeze_holds }
                if !active_freeze_holds.contains(&hold_id) =>
            {
                Some(Self::Frozen {
                    active_freeze_holds: active_freeze_holds.with(hold_id),
                })
            }
            Self::Frozen { .. } => None,
        }
    }

    fn release(&self, hold_id: &FreezeHoldId) -> Option<Self> {
        let Self::Frozen { active_freeze_holds } = self else {
            return None;
        };
        if !active_freeze_holds.contains(hold_id) {
            return None;
        }
        Some(
            active_freeze_holds
                .without(hold_id)
                .map_or(Self::Enabled, |active_freeze_holds| Self::Frozen {
                    active_freeze_holds,
                }),
        )
    }
}

/// Corporate-action state of one underlying equity, across all networks.
///
/// A stream originates on the first legacy `Frozen` or new
/// `FreezeHoldAcquired` event; an underlying with no stream is `Enabled` by
/// definition (see [`load_freeze_status`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Underlying {
    #[serde(flatten)]
    freeze_state: FreezeState,
}

impl Underlying {
    fn has_freeze_hold(&self, hold_id: &FreezeHoldId) -> bool {
        self.freeze_state.contains(hold_id)
    }

    pub(crate) const fn freeze_status(&self) -> AssetStatus {
        self.freeze_state.status()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum UnderlyingCommand {
    /// Acquire the operator hold for this underlying.
    Freeze { underlying: UnderlyingSymbol },
    /// Release only the operator hold.
    Unfreeze { underlying: UnderlyingSymbol },
    /// Acquire one independently owned freeze hold.
    AcquireFreezeHold {
        underlying: UnderlyingSymbol,
        hold_id: FreezeHoldId,
        acquired_at: DateTime<Utc>,
    },
    /// Release only the named freeze hold.
    ReleaseFreezeHold {
        underlying: UnderlyingSymbol,
        hold_id: FreezeHoldId,
        released_at: DateTime<Utc>,
    },
}

/// The payloads must stay wire-compatible with the pre-multichain
/// `TokenizedAssetEvent::Frozen`/`Unfrozen` events: the aggregate-rekey
/// migration re-types shipped rows onto this aggregate without touching their
/// JSON payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UnderlyingEvent {
    Frozen { frozen_at: DateTime<Utc> },
    Unfrozen { unfrozen_at: DateTime<Utc> },
    FreezeHoldAcquired { hold_id: FreezeHoldId, acquired_at: DateTime<Utc> },
    FreezeHoldReleased { hold_id: FreezeHoldId, released_at: DateTime<Utc> },
}

impl DomainEvent for UnderlyingEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Frozen { .. } => "UnderlyingEvent::Frozen".to_string(),
            Self::Unfrozen { .. } => "UnderlyingEvent::Unfrozen".to_string(),
            Self::FreezeHoldAcquired { .. } => {
                "UnderlyingEvent::FreezeHoldAcquired".to_string()
            }
            Self::FreezeHoldReleased { .. } => {
                "UnderlyingEvent::FreezeHoldReleased".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[async_trait]
impl EventSourced for Underlying {
    type Id = UnderlyingSymbol;
    type Event = UnderlyingEvent;
    type Command = UnderlyingCommand;
    type Error = Never;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "Underlying";
    const PROJECTION: Table = Table("underlying_view");
    const SCHEMA_VERSION: u64 = 2;

    // Snapshots are disabled: event-sorcery hardwires snapshot-every-N with no
    // off switch, so usize::MAX makes the next-snapshot threshold unreachable.
    // Freeze streams are a handful of events, so replay is trivially cheap.
    const SNAPSHOT_SIZE: usize = usize::MAX;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            UnderlyingEvent::Frozen { .. } => Some(Self {
                freeze_state: FreezeState::Frozen {
                    active_freeze_holds: ActiveFreezeHolds::legacy_operator(),
                },
            }),
            UnderlyingEvent::FreezeHoldAcquired { hold_id, .. } => Some(Self {
                freeze_state: FreezeState::Frozen {
                    active_freeze_holds: ActiveFreezeHolds::one(*hold_id),
                },
            }),
            // `Unfreeze` on a stream-less underlying is a no-op (already
            // `Enabled` by definition), so `Unfrozen` never starts a stream.
            UnderlyingEvent::Unfrozen { .. }
            | UnderlyingEvent::FreezeHoldReleased { .. } => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        match event {
            UnderlyingEvent::Frozen { .. } => Ok(Some(Self {
                freeze_state: entity
                    .freeze_state
                    .acquire(FreezeHoldId::operator())
                    .unwrap_or_else(|| entity.freeze_state.clone()),
            })),
            UnderlyingEvent::Unfrozen { .. } => Ok(Some(Self {
                freeze_state: entity
                    .freeze_state
                    .release(&FreezeHoldId::operator())
                    .unwrap_or_else(|| entity.freeze_state.clone()),
            })),
            UnderlyingEvent::FreezeHoldAcquired { hold_id, .. } => {
                Ok(Some(Self {
                    freeze_state: entity
                        .freeze_state
                        .acquire(*hold_id)
                        .unwrap_or_else(|| entity.freeze_state.clone()),
                }))
            }
            UnderlyingEvent::FreezeHoldReleased { hold_id, .. } => {
                Ok(Some(Self {
                    freeze_state: entity
                        .freeze_state
                        .release(hold_id)
                        .unwrap_or_else(|| entity.freeze_state.clone()),
                }))
            }
        }
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            UnderlyingCommand::Freeze { underlying } => {
                tracing::info!(target: "asset", underlying = %underlying,
                    "Freezing underlying across all networks"
                );
                Ok(vec![UnderlyingEvent::FreezeHoldAcquired {
                    hold_id: FreezeHoldId::operator(),
                    acquired_at: Utc::now(),
                }])
            }

            // No stream means the underlying was never frozen — `Enabled` by
            // definition, so there is nothing to unfreeze.
            UnderlyingCommand::Unfreeze { underlying } => {
                tracing::debug!(target: "asset", underlying = %underlying,
                    "Underlying was never frozen, skipping unfreeze"
                );
                Ok(vec![])
            }

            UnderlyingCommand::AcquireFreezeHold {
                underlying,
                hold_id,
                acquired_at,
            } => {
                if hold_id.is_expired_at(acquired_at) {
                    tracing::info!(target: "asset",
                        underlying = %underlying,
                        ?hold_id,
                        "Skipping expired underlying freeze hold"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset",
                    underlying = %underlying,
                    ?hold_id,
                    "Acquiring underlying freeze hold"
                );
                Ok(vec![UnderlyingEvent::FreezeHoldAcquired {
                    hold_id,
                    acquired_at,
                }])
            }

            UnderlyingCommand::ReleaseFreezeHold {
                underlying,
                hold_id,
                ..
            } => {
                tracing::debug!(target: "asset",
                    underlying = %underlying,
                    ?hold_id,
                    "Freeze hold already absent, skipping"
                );
                Ok(vec![])
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            UnderlyingCommand::Freeze { underlying } => {
                let hold_id = FreezeHoldId::operator();
                if self.has_freeze_hold(&hold_id) {
                    tracing::debug!(target: "asset", underlying = %underlying,
                        "Operator freeze hold already active, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset", underlying = %underlying,
                    "Freezing underlying across all networks"
                );
                Ok(vec![UnderlyingEvent::FreezeHoldAcquired {
                    hold_id,
                    acquired_at: Utc::now(),
                }])
            }

            UnderlyingCommand::Unfreeze { underlying } => {
                let hold_id = FreezeHoldId::operator();
                if !self.has_freeze_hold(&hold_id) {
                    tracing::debug!(target: "asset", underlying = %underlying,
                        "Operator freeze hold already absent, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset", underlying = %underlying,
                    "Unfreezing underlying across all networks"
                );
                Ok(vec![UnderlyingEvent::FreezeHoldReleased {
                    hold_id,
                    released_at: Utc::now(),
                }])
            }

            UnderlyingCommand::AcquireFreezeHold {
                underlying,
                hold_id,
                acquired_at,
            } => {
                if hold_id.is_expired_at(acquired_at) {
                    tracing::info!(target: "asset",
                        underlying = %underlying,
                        ?hold_id,
                        "Skipping expired underlying freeze hold"
                    );
                    return Ok(vec![]);
                }
                if self.has_freeze_hold(&hold_id) {
                    tracing::debug!(target: "asset",
                        underlying = %underlying,
                        ?hold_id,
                        "Freeze hold already active, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset",
                    underlying = %underlying,
                    ?hold_id,
                    "Acquiring underlying freeze hold"
                );
                Ok(vec![UnderlyingEvent::FreezeHoldAcquired {
                    hold_id,
                    acquired_at,
                }])
            }

            UnderlyingCommand::ReleaseFreezeHold {
                underlying,
                hold_id,
                released_at,
            } => {
                if !self.has_freeze_hold(&hold_id) {
                    tracing::debug!(target: "asset",
                        underlying = %underlying,
                        ?hold_id,
                        "Freeze hold already absent, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset",
                    underlying = %underlying,
                    ?hold_id,
                    "Releasing underlying freeze hold"
                );
                Ok(vec![UnderlyingEvent::FreezeHoldReleased {
                    hold_id,
                    released_at,
                }])
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UnderlyingViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error(
        "underlying {underlying} has a non-live (null `$.Live`) projection row"
    )]
    NonLiveRow { underlying: UnderlyingSymbol },
}

/// Reads the freeze status of an underlying from the `underlying_view`
/// projection.
///
/// An absent row means the underlying was never frozen — `Enabled` by
/// definition. Existence of the underlying itself (does it have any listing?)
/// is deliberately NOT this function's concern: callers gate on the
/// `tokenized_asset_view` listing lookup first and 404 unknown assets, so an
/// `Enabled` answer here never turns an unknown asset into a mintable one.
pub(crate) async fn load_freeze_status(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<AssetStatus, UnderlyingViewError> {
    let row = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM underlying_view
        WHERE view_id = ?
        "#,
        underlying.as_str()
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(AssetStatus::Enabled);
    };

    let Some(live) = row.live else {
        return Err(UnderlyingViewError::NonLiveRow {
            underlying: underlying.clone(),
        });
    };

    let view: Underlying = serde_json::from_str(&live)?;

    Ok(view.freeze_state.status())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};
    use event_sorcery::TestHarness;
    use tracing_test::traced_test;

    use super::{
        AssetStatus, FreezeHoldId, FreezeWindow, Underlying, UnderlyingCommand,
        UnderlyingEvent, UnderlyingSymbol, UnderlyingViewError,
        load_freeze_status,
    };
    use crate::prepare_event_sourced_startup;
    use crate::test_utils::logs_contain_at;

    fn aapl() -> UnderlyingSymbol {
        UnderlyingSymbol::new("AAPL").unwrap()
    }

    fn corporate_action_hold(
        freeze_at: chrono::DateTime<Utc>,
        unfreeze_at: chrono::DateTime<Utc>,
    ) -> FreezeHoldId {
        FreezeHoldId::corporate_action(
            FreezeWindow::new(freeze_at, unfreeze_at).unwrap(),
        )
    }

    #[traced_test]
    #[tokio::test]
    async fn test_freeze_never_frozen_underlying_acquires_operator_hold() {
        let events = TestHarness::<Underlying>::with(())
            .given_no_previous_events()
            .when(UnderlyingCommand::Freeze { underlying: aapl() })
            .await
            .events();

        assert_eq!(events.len(), 1, "Expected exactly one event");

        let UnderlyingEvent::FreezeHoldAcquired { hold_id, acquired_at } =
            &events[0]
        else {
            panic!("Expected FreezeHoldAcquired event, got: {:?}", events[0])
        };
        assert_eq!(*hold_id, FreezeHoldId::operator());
        assert!(acquired_at.timestamp() > 0);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing underlying across all networks", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_freeze_already_frozen_is_idempotent() {
        TestHarness::<Underlying>::with(())
            .given(vec![UnderlyingEvent::Frozen {
                frozen_at: chrono::Utc::now(),
            }])
            .when(UnderlyingCommand::Freeze { underlying: aapl() })
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Operator freeze hold already active, skipping", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unfreeze_frozen_underlying_releases_operator_hold() {
        let events = TestHarness::<Underlying>::with(())
            .given(vec![UnderlyingEvent::Frozen {
                frozen_at: chrono::Utc::now(),
            }])
            .when(UnderlyingCommand::Unfreeze { underlying: aapl() })
            .await
            .events();

        assert_eq!(events.len(), 1, "Expected exactly one event");

        let UnderlyingEvent::FreezeHoldReleased { hold_id, released_at } =
            &events[0]
        else {
            panic!("Expected FreezeHoldReleased event, got: {:?}", events[0])
        };
        assert_eq!(*hold_id, FreezeHoldId::operator());
        assert!(released_at.timestamp() > 0);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Unfreezing underlying across all networks", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unfreeze_never_frozen_underlying_is_noop() {
        TestHarness::<Underlying>::with(())
            .given_no_previous_events()
            .when(UnderlyingCommand::Unfreeze { underlying: aapl() })
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Underlying was never frozen, skipping unfreeze", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unfreeze_already_enabled_is_idempotent() {
        TestHarness::<Underlying>::with(())
            .given(vec![
                UnderlyingEvent::Frozen { frozen_at: chrono::Utc::now() },
                UnderlyingEvent::Unfrozen { unfrozen_at: chrono::Utc::now() },
            ])
            .when(UnderlyingCommand::Unfreeze { underlying: aapl() })
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Operator freeze hold already absent, skipping", "AAPL"]
        ));
    }

    #[tokio::test]
    async fn overlapping_holds_keep_underlying_frozen_until_final_release() {
        let now = Utc::now();
        let first_hold = corporate_action_hold(
            now + ChronoDuration::hours(1),
            now + ChronoDuration::hours(3),
        );
        let second_hold = corporate_action_hold(
            now + ChronoDuration::hours(2),
            now + ChronoDuration::hours(4),
        );

        let underlying = TestHarness::<Underlying>::with(())
            .given(vec![
                UnderlyingEvent::FreezeHoldAcquired {
                    hold_id: first_hold,
                    acquired_at: now + ChronoDuration::hours(1),
                },
                UnderlyingEvent::FreezeHoldAcquired {
                    hold_id: second_hold,
                    acquired_at: now + ChronoDuration::hours(2),
                },
                UnderlyingEvent::FreezeHoldReleased {
                    hold_id: first_hold,
                    released_at: now + ChronoDuration::hours(3),
                },
            ])
            .when(UnderlyingCommand::ReleaseFreezeHold {
                underlying: aapl(),
                hold_id: second_hold,
                released_at: now + ChronoDuration::hours(4),
            })
            .await
            .events();

        assert_eq!(
            underlying,
            vec![UnderlyingEvent::FreezeHoldReleased {
                hold_id: second_hold,
                released_at: now + ChronoDuration::hours(4),
            }]
        );
    }

    #[tokio::test]
    async fn releasing_one_hold_does_not_release_another() {
        let now = Utc::now();
        let scheduled_hold =
            corporate_action_hold(now, now + ChronoDuration::hours(1));
        let underlying = event_sorcery::replay::<Underlying>(vec![
            UnderlyingEvent::FreezeHoldAcquired {
                hold_id: FreezeHoldId::operator(),
                acquired_at: now,
            },
            UnderlyingEvent::FreezeHoldAcquired {
                hold_id: scheduled_hold,
                acquired_at: now,
            },
            UnderlyingEvent::FreezeHoldReleased {
                hold_id: scheduled_hold,
                released_at: now + ChronoDuration::hours(1),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(underlying.freeze_state.status(), AssetStatus::Frozen);
        assert!(underlying.has_freeze_hold(&FreezeHoldId::operator()));
    }

    #[tokio::test]
    async fn expired_hold_is_not_acquired() {
        let now = Utc::now();
        let expired_hold = corporate_action_hold(
            now - ChronoDuration::hours(2),
            now - ChronoDuration::hours(1),
        );

        TestHarness::<Underlying>::with(())
            .given_no_previous_events()
            .when(UnderlyingCommand::AcquireFreezeHold {
                underlying: aapl(),
                hold_id: expired_hold,
                acquired_at: now,
            })
            .await
            .then_expect_events(&[]);
    }

    #[test]
    fn legacy_frozen_projection_defaults_to_operator_hold() {
        let underlying: Underlying =
            serde_json::from_value(serde_json::json!({ "status": "Frozen" }))
                .unwrap();

        assert!(underlying.has_freeze_hold(&FreezeHoldId::operator()));
        let serialized = serde_json::to_value(underlying).unwrap();
        assert_eq!(serialized["status"], "Frozen");
        assert_eq!(
            serialized["active_freeze_holds"],
            serde_json::json!(["Operator"])
        );
    }

    #[test]
    fn frozen_projection_rejects_empty_hold_set() {
        let result = serde_json::from_value::<Underlying>(serde_json::json!({
            "status": "Frozen",
            "active_freeze_holds": [],
        }));

        assert!(result.is_err());
    }

    #[traced_test]
    #[tokio::test]
    async fn test_freeze_event_payload_matches_shipped_wire_shape() {
        // The rekey migration re-types shipped `TokenizedAssetEvent::Frozen`
        // rows onto this aggregate without rewriting payloads, so this enum
        // must keep deserializing the exact pre-multichain JSON.
        let event: UnderlyingEvent = serde_json::from_str(
            r#"{"Frozen":{"frozen_at":"2026-06-15T12:00:00Z"}}"#,
        )
        .unwrap();
        assert!(matches!(event, UnderlyingEvent::Frozen { .. }));

        let event: UnderlyingEvent = serde_json::from_str(
            r#"{"Unfrozen":{"unfrozen_at":"2026-06-16T12:00:00Z"}}"#,
        )
        .unwrap();
        assert!(matches!(event, UnderlyingEvent::Unfrozen { .. }));
    }

    #[tokio::test]
    async fn test_load_freeze_status_defaults_to_enabled_without_stream() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        prepare_event_sourced_startup::<Underlying>(&pool).await.unwrap();

        let status = load_freeze_status(&pool, &aapl()).await.unwrap();

        assert_eq!(status, AssetStatus::Enabled);
    }

    // A row whose `$.Live` is null is a non-live lifecycle state (known but
    // indeterminate), distinct from an absent row. Only reachable via external
    // DB manipulation — the projection always writes a live `$.Live` payload.
    #[tokio::test]
    async fn test_load_freeze_status_errors_on_non_live_row() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        prepare_event_sourced_startup::<Underlying>(&pool).await.unwrap();

        sqlx::query(
            "
            INSERT INTO underlying_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{\"Live\": null}')
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = load_freeze_status(&pool, &aapl()).await;

        assert!(
            matches!(
                result.unwrap_err(),
                UnderlyingViewError::NonLiveRow { underlying }
                    if underlying == aapl()
            ),
            "null $.Live must surface NonLiveRow, not default to Enabled"
        );
    }

    #[tokio::test]
    async fn test_load_freeze_status_reflects_freeze_and_unfreeze() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        prepare_event_sourced_startup::<Underlying>(&pool).await.unwrap();
        let (store, _projection) =
            event_sorcery::StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        store
            .send(&aapl(), UnderlyingCommand::Freeze { underlying: aapl() })
            .await
            .unwrap();
        assert_eq!(
            load_freeze_status(&pool, &aapl()).await.unwrap(),
            AssetStatus::Frozen
        );

        store
            .send(&aapl(), UnderlyingCommand::Unfreeze { underlying: aapl() })
            .await
            .unwrap();
        assert_eq!(
            load_freeze_status(&pool, &aapl()).await.unwrap(),
            AssetStatus::Enabled
        );
    }
}
