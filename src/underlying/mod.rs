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

/// Corporate-action state of one underlying equity, across all networks.
///
/// A stream originates on the first `Frozen` event; an underlying with no
/// stream is `Enabled` by definition (see [`load_freeze_status`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Underlying {
    status: AssetStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum UnderlyingCommand {
    /// Stop accepting new mints for every listing of this underlying.
    /// Idempotent — freezing a frozen underlying is a no-op.
    Freeze { underlying: UnderlyingSymbol },
    /// Resume accepting mints. Idempotent; a no-op when the underlying was
    /// never frozen (no stream exists).
    Unfreeze { underlying: UnderlyingSymbol },
}

/// The payloads must stay wire-compatible with the pre-multichain
/// `TokenizedAssetEvent::Frozen`/`Unfrozen` events: the aggregate-rekey
/// migration re-types shipped rows onto this aggregate without touching their
/// JSON payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UnderlyingEvent {
    Frozen { frozen_at: DateTime<Utc> },
    Unfrozen { unfrozen_at: DateTime<Utc> },
}

impl DomainEvent for UnderlyingEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Frozen { .. } => "UnderlyingEvent::Frozen".to_string(),
            Self::Unfrozen { .. } => "UnderlyingEvent::Unfrozen".to_string(),
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
    const SCHEMA_VERSION: u64 = 1;

    // Snapshots are disabled: event-sorcery hardwires snapshot-every-N with no
    // off switch, so usize::MAX makes the next-snapshot threshold unreachable.
    // Freeze streams are a handful of events, so replay is trivially cheap.
    const SNAPSHOT_SIZE: usize = usize::MAX;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            UnderlyingEvent::Frozen { .. } => {
                Some(Self { status: AssetStatus::Frozen })
            }
            // `Unfreeze` on a stream-less underlying is a no-op (already
            // `Enabled` by definition), so `Unfrozen` never starts a stream.
            UnderlyingEvent::Unfrozen { .. } => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        let _ = entity;
        match event {
            UnderlyingEvent::Frozen { .. } => {
                Ok(Some(Self { status: AssetStatus::Frozen }))
            }
            UnderlyingEvent::Unfrozen { .. } => {
                Ok(Some(Self { status: AssetStatus::Enabled }))
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
                Ok(vec![UnderlyingEvent::Frozen { frozen_at: Utc::now() }])
            }

            // No stream means the underlying was never frozen — `Enabled` by
            // definition, so there is nothing to unfreeze.
            UnderlyingCommand::Unfreeze { underlying } => {
                tracing::debug!(target: "asset", underlying = %underlying,
                    "Underlying was never frozen, skipping unfreeze"
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
                if self.status == AssetStatus::Frozen {
                    tracing::debug!(target: "asset", underlying = %underlying,
                        "Underlying already frozen, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset", underlying = %underlying,
                    "Freezing underlying across all networks"
                );
                Ok(vec![UnderlyingEvent::Frozen { frozen_at: Utc::now() }])
            }

            UnderlyingCommand::Unfreeze { underlying } => {
                if self.status == AssetStatus::Enabled {
                    tracing::debug!(target: "asset", underlying = %underlying,
                        "Underlying already enabled, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset", underlying = %underlying,
                    "Unfreezing underlying across all networks"
                );
                Ok(vec![UnderlyingEvent::Unfrozen { unfrozen_at: Utc::now() }])
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

    Ok(view.status)
}

#[cfg(test)]
mod tests {
    use event_sorcery::TestHarness;
    use tracing_test::traced_test;

    use super::{
        AssetStatus, Underlying, UnderlyingCommand, UnderlyingEvent,
        UnderlyingSymbol, UnderlyingViewError, load_freeze_status,
    };
    use crate::prepare_event_sourced_startup;
    use crate::test_utils::logs_contain_at;

    fn aapl() -> UnderlyingSymbol {
        UnderlyingSymbol::new("AAPL").unwrap()
    }

    #[traced_test]
    #[tokio::test]
    async fn test_freeze_never_frozen_underlying_emits_frozen() {
        let events = TestHarness::<Underlying>::with(())
            .given_no_previous_events()
            .when(UnderlyingCommand::Freeze { underlying: aapl() })
            .await
            .events();

        assert_eq!(events.len(), 1, "Expected exactly one event");

        let UnderlyingEvent::Frozen { frozen_at } = &events[0] else {
            panic!("Expected Frozen event, got: {:?}", events[0])
        };
        assert!(frozen_at.timestamp() > 0);

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
            &["Underlying already frozen, skipping", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unfreeze_frozen_underlying_emits_unfrozen() {
        let events = TestHarness::<Underlying>::with(())
            .given(vec![UnderlyingEvent::Frozen {
                frozen_at: chrono::Utc::now(),
            }])
            .when(UnderlyingCommand::Unfreeze { underlying: aapl() })
            .await
            .events();

        assert_eq!(events.len(), 1, "Expected exactly one event");

        let UnderlyingEvent::Unfrozen { unfrozen_at } = &events[0] else {
            panic!("Expected Unfrozen event, got: {:?}", events[0])
        };
        assert!(unfrozen_at.timestamp() > 0);

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
            &["Underlying already enabled, skipping", "AAPL"]
        ));
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
