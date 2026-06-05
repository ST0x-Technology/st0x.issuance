use cqrs_es::persist::{
    EventUpcaster, GenericQuery, PersistenceError, QueryReplay,
};
use cqrs_es::{Aggregate, AggregateError, View};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository};
use std::sync::{Arc, Mutex};
use tracing::{debug, error};

/// Errors raised while rebuilding a view by replaying committed events.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplayError<E: std::error::Error + 'static> {
    /// The event stream itself failed to open. Unreachable with the pinned
    /// sqlite-es (which always returns `Ok` from `stream_all_events` and routes
    /// read failures through the per-event handler as [`Self::EventsDropped`]);
    /// reserved for backends that surface stream-open errors eagerly.
    #[error(transparent)]
    Aggregate(#[from] AggregateError<E>),

    /// `dropped` is a lower bound, not an exact count. A row-level stream
    /// read error (e.g. an unreadable column or non-JSON payload) closes the
    /// event stream, so any events after the failed row are skipped without
    /// ever reaching a handler and are not counted here. View-dispatch and
    /// deserialization failures that do not close the stream are counted
    /// exactly.
    #[error(
        "replay dropped at least {dropped} event(s) while rebuilding the view; first error: {first}"
    )]
    EventsDropped { dropped: usize, first: PersistenceError },
}

/// Accumulates per-event replay failures surfaced by the cqrs-es error
/// handlers. Only the count and the first error are retained — full per-event
/// detail goes to the DEBUG logs — so a systemic failure (e.g. a missing view
/// table that fails every event's dispatch) cannot grow memory without bound.
#[derive(Default)]
struct DroppedEvents {
    count: usize,
    first: Option<PersistenceError>,
}

/// Replays all committed events of aggregate `A` into the view managed by
/// `view_repo`, failing fast if any per-event error would otherwise be silently
/// dropped.
///
/// `cqrs-es` delivers per-event failures (event deserialization, stream reads,
/// view writes) to optional error handlers on both [`QueryReplay`] and the
/// [`GenericQuery`]. With no handler installed those failures are discarded and
/// `replay_all` still returns `Ok(())`, so a single malformed or unupcastable
/// historical event can leave a projection partially rebuilt while startup
/// reports success (verified in `cqrs-es-0.4.12`: `persist/replay.rs:103-107`
/// and `persist/generic_query.rs:125-129`). Both handlers return `()` and
/// cannot abort the stream, so this collects every dropped error and converts a
/// non-empty collection into a typed failure once the replay completes.
///
/// `upcasters` must match those registered on the aggregate's live event store,
/// otherwise legacy event versions that the live store upcasts successfully
/// would deserialize-fail here and abort startup. Pass an empty `Vec` for
/// aggregates whose store registers none.
pub(crate) async fn replay_all_or_fail<V, A>(
    event_repo: SqliteEventRepository,
    view_repo: Arc<SqliteViewRepository<V, A>>,
    upcasters: Vec<Box<dyn EventUpcaster>>,
) -> Result<(), ReplayError<A::Error>>
where
    V: View<A>,
    A: Aggregate,
    A::Error: 'static,
{
    let dropped_errors: Arc<Mutex<DroppedEvents>> =
        Arc::new(Mutex::new(DroppedEvents::default()));

    let mut query = GenericQuery::new(view_repo);
    let dispatch_errors = dropped_errors.clone();
    query.use_error_handler(Box::new(move |error| {
        debug!(target: "startup", %error, "dropped event during view dispatch");
        if let Ok(mut dropped) = dispatch_errors.lock() {
            dropped.count += 1;
            dropped.first.get_or_insert(error);
        }
    }));

    let mut replay =
        QueryReplay::new(event_repo, query).with_upcasters(upcasters);
    let stream_errors = dropped_errors.clone();
    replay.use_error_handler(Box::new(move |error| {
        debug!(target: "startup", %error, "dropped event during stream read");
        if let Ok(mut dropped) = stream_errors.lock() {
            dropped.count += 1;
            dropped.first.get_or_insert(error);
        }
    }));

    // With the pinned sqlite-es, `stream_all_events` always returns `Ok` and
    // delivers DB/stream failures through the stream channel to the handler
    // above (surfacing as `EventsDropped`), so this `?` does not fire on a
    // missing or unreadable event store. It is kept as a forward-compatible
    // guard: if a future event-store backend returns the stream-open error
    // eagerly, it maps to `ReplayError::Aggregate` rather than being swallowed.
    replay.replay_all().await?;

    let dropped = std::mem::take(
        &mut *dropped_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );

    if let Some(first) = dropped.first {
        let dropped = dropped.count;
        error!(
            target: "startup",
            dropped,
            %first,
            "view replay dropped events; aborting startup so the incomplete projection is never served"
        );

        return Err(ReplayError::EventsDropped { dropped, first });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use tracing_test::traced_test;

    use super::*;
    use crate::redemption::upcaster::create_tokens_burned_upcaster;
    use crate::redemption::{Redemption, RedemptionView};
    use crate::test_utils::logs_contain_at;

    /// A legacy v1.0 `TokensBurned` payload — top-level `receipt_id`/
    /// `shares_burned` with no `burns` array. It only deserializes into the
    /// current `RedemptionEvent` after `create_tokens_burned_upcaster` runs.
    const LEGACY_TOKENS_BURNED_V1: &str = r#"{
        "TokensBurned": {
            "issuer_request_id": "red-abcd1234",
            "tx_hash": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "receipt_id": "0x42",
            "shares_burned": "0x100",
            "dust_returned": "0x0",
            "gas_used": 1000,
            "block_number": 5,
            "burned_at": "2025-01-01T00:00:00Z"
        }
    }"#;

    async fn migrated_pool() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    async fn seed_event(
        pool: &Pool<Sqlite>,
        aggregate_type: &str,
        event_type: &str,
        event_version: &str,
        payload: &str,
    ) {
        seed_event_at(
            pool,
            aggregate_type,
            "agg-1",
            1,
            event_type,
            event_version,
            payload,
        )
        .await;
    }

    async fn seed_event_at(
        pool: &Pool<Sqlite>,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        event_version: &str,
        payload: &str,
    ) {
        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, '{}')
            ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(event_version)
        .bind(payload)
        .execute(pool)
        .await
        .expect("Failed to seed event");
    }

    #[traced_test]
    #[tokio::test]
    async fn replay_fails_fast_on_malformed_event() {
        let pool = migrated_pool().await;

        // A Redemption event whose payload cannot deserialize into
        // RedemptionEvent, so the stream delivers a per-event error the handler
        // must surface.
        seed_event(
            &pool,
            "Redemption",
            "RedemptionEvent::Garbage",
            "1.0",
            r#"{"NonexistentVariant":{}}"#,
        )
        .await;

        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let event_repo = SqliteEventRepository::new(pool);

        let error = replay_all_or_fail(event_repo, view_repo, vec![])
            .await
            .expect_err("replay should fail on a malformed event");

        let ReplayError::EventsDropped { dropped, first } = &error else {
            panic!("expected EventsDropped, got {error:?}");
        };
        assert_eq!(*dropped, 1);
        assert!(
            first.to_string().contains("NonexistentVariant"),
            "first error should name the bad variant, got: {first}"
        );

        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["view replay dropped events", "aborting startup"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["dropped event during stream read"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn replay_fails_fast_on_view_write_error() {
        let pool = migrated_pool().await;

        // A valid (post-upcast) event so the stream yields Ok and dispatch runs.
        seed_event(
            &pool,
            "Redemption",
            "RedemptionEvent::TokensBurned",
            "1.0",
            LEGACY_TOKENS_BURNED_V1,
        )
        .await;

        // Remove the target view table so the view-write path inside dispatch
        // fails, exercising the GenericQuery error handler.
        sqlx::query("DROP TABLE redemption_view")
            .execute(&pool)
            .await
            .expect("Failed to drop redemption_view");

        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let event_repo = SqliteEventRepository::new(pool);

        let error = replay_all_or_fail(
            event_repo,
            view_repo,
            vec![create_tokens_burned_upcaster()],
        )
        .await
        .expect_err("replay should fail when the view table is missing");

        assert!(
            matches!(error, ReplayError::EventsDropped { dropped: 1, .. }),
            "expected EventsDropped, got {error:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["dropped event during view dispatch"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["view replay dropped events"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn replay_upcasts_legacy_events_instead_of_dropping_them() {
        let pool = migrated_pool().await;

        // Without the upcaster this v1.0 payload deserialize-fails and would be
        // dropped; with it, the replay must rebuild the view cleanly.
        seed_event(
            &pool,
            "Redemption",
            "RedemptionEvent::TokensBurned",
            "1.0",
            LEGACY_TOKENS_BURNED_V1,
        )
        .await;

        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let event_repo = SqliteEventRepository::new(pool.clone());

        replay_all_or_fail(
            event_repo,
            view_repo,
            vec![create_tokens_burned_upcaster()],
        )
        .await
        .expect("legacy event should upcast and replay without being dropped");

        let payload: String =
            sqlx::query_scalar("SELECT payload FROM redemption_view")
                .fetch_one(&pool)
                .await
                .expect("redemption_view should hold the rebuilt row");
        // The upcast event must actually project its fields, not just insert an
        // empty row — assert a value carried by LEGACY_TOKENS_BURNED_V1 landed
        // in the rebuilt view.
        assert!(
            payload.contains("red-abcd1234"),
            "rebuilt view should carry the upcast event's issuer_request_id, got: {payload}"
        );
    }

    #[tokio::test]
    async fn replay_succeeds_on_empty_store() {
        let pool = migrated_pool().await;

        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let event_repo = SqliteEventRepository::new(pool.clone());

        replay_all_or_fail(event_repo, view_repo, vec![])
            .await
            .expect("an empty event store is not a replay failure");

        let rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM redemption_view")
                .fetch_one(&pool)
                .await
                .expect("redemption_view should be queryable");
        assert_eq!(
            rows, 0,
            "a replay over no events must leave the view empty"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn replay_drops_legacy_event_without_upcaster() {
        let pool = migrated_pool().await;

        // The same v1.0 payload the upcast test replays cleanly — but with no
        // upcaster it cannot deserialize, proving the positive upcast test
        // detects the upcaster's work rather than an event that happens to
        // deserialize regardless.
        seed_event(
            &pool,
            "Redemption",
            "RedemptionEvent::TokensBurned",
            "1.0",
            LEGACY_TOKENS_BURNED_V1,
        )
        .await;

        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let event_repo = SqliteEventRepository::new(pool);

        let error =
            replay_all_or_fail(event_repo, view_repo, vec![]).await.expect_err(
                "a legacy event with no upcaster must not be dropped silently",
            );

        assert!(
            matches!(error, ReplayError::EventsDropped { dropped: 1, .. }),
            "expected EventsDropped, got {error:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["view replay dropped events"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["dropped event during stream read"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn replay_counts_every_dropped_event() {
        let pool = migrated_pool().await;

        // Two events under distinct aggregates so each dispatches separately;
        // dropping the view table fails every dispatch, so the count must
        // reflect both, not just the first.
        for aggregate_id in ["agg-1", "agg-2"] {
            seed_event_at(
                &pool,
                "Redemption",
                aggregate_id,
                1,
                "RedemptionEvent::TokensBurned",
                "1.0",
                LEGACY_TOKENS_BURNED_V1,
            )
            .await;
        }

        sqlx::query("DROP TABLE redemption_view")
            .execute(&pool)
            .await
            .expect("Failed to drop redemption_view");

        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let event_repo = SqliteEventRepository::new(pool);

        let error = replay_all_or_fail(
            event_repo,
            view_repo,
            vec![create_tokens_burned_upcaster()],
        )
        .await
        .expect_err("replay should fail when the view table is missing");

        let ReplayError::EventsDropped { dropped, .. } = &error else {
            panic!("expected EventsDropped, got {error:?}");
        };
        assert_eq!(
            *dropped, 2,
            "both dispatch failures must be counted, not just the first"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["view replay dropped events"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["dropped event during view dispatch"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn replay_fails_fast_when_event_store_is_unreadable() {
        let pool = migrated_pool().await;

        // Drop the event store entirely. sqlite-es surfaces the resulting DB
        // error through the stream as a single per-event read failure, so it
        // reaches the QueryReplay handler and becomes EventsDropped rather than
        // silently rebuilding an empty view — a catastrophic store failure
        // must still abort startup, never report a clean rebuild.
        sqlx::query("DROP TABLE events")
            .execute(&pool)
            .await
            .expect("Failed to drop events");

        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let event_repo = SqliteEventRepository::new(pool);

        let error = replay_all_or_fail(event_repo, view_repo, vec![])
            .await
            .expect_err("an unreadable event store must abort the replay");

        assert!(
            matches!(error, ReplayError::EventsDropped { dropped: 1, .. }),
            "expected EventsDropped {{ dropped: 1 }}, got {error:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["view replay dropped events"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["dropped event during stream read"]
        ));
    }
}
