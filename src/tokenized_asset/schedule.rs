//! Scheduled dividend freeze/unfreeze for tokenized assets.
//!
//! Around an ex-date the supply freeze must flip on and off at known
//! instants. This module automates the trigger the issuer CLI fires by hand:
//! a [`FreezeScheduler`] enqueues two durable apalis jobs per corporate-action
//! window — a `Freeze` before the ex-date and an `Unfreeze` after — and the
//! worker dispatches the exact same underlying-scoped commands the CLI does.
//! Only the trigger changes; the mint gate reacts to the status flip identically
//! however it was dispatched, across every network listing.
//!
//! Durability and idempotency: apalis persists the due time, so a scheduled
//! transition survives restarts, and the idempotency key (underlying + the
//! scheduled instant) collapses re-submissions of the same window. The
//! `Freeze`/`Unfreeze` commands are themselves no-ops when the asset is
//! already in the target state, so overlapping schedules are safe.

use chrono::{DateTime, Utc};
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::UnderlyingSymbol;
use crate::jobs::{Job, JobQueue, QueuePushError};
use crate::underlying::{Underlying, UnderlyingCommand};

/// Which side of the freeze window a scheduled job applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum FreezeTransition {
    Freeze,
    Unfreeze,
}

/// Durable job applying one scheduled freeze transition to one asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ApplyFreezeTransition {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) transition: FreezeTransition,
    /// The instant this transition was scheduled for. Part of the
    /// idempotency key, and recorded for audit in worker logs.
    pub(crate) scheduled_for: DateTime<Utc>,
}

/// Runtime dependencies for [`ApplyFreezeTransition::perform`].
pub(crate) struct FreezeScheduleCtx {
    pub(crate) underlying_store: Arc<Store<Underlying>>,
}

/// Error surfaced by a freeze-transition job. Command dispatch is the only
/// fallible step; `Freeze`/`Unfreeze` on an already-transitioned asset is a
/// no-op, not an error.
#[derive(Debug, thiserror::Error)]
#[error(
    "failed to dispatch scheduled {transition:?} for {underlying}: {source}"
)]
pub(crate) struct FreezeTransitionError {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) transition: FreezeTransition,
    #[source]
    pub(crate) source: Box<AggregateError<LifecycleError<Underlying>>>,
}

impl Job<FreezeScheduleCtx> for ApplyFreezeTransition {
    type Output = ();
    type Error = FreezeTransitionError;

    async fn perform(
        &self,
        ctx: &FreezeScheduleCtx,
    ) -> Result<Self::Output, Self::Error> {
        let command = match self.transition {
            FreezeTransition::Freeze => UnderlyingCommand::Freeze {
                underlying: self.underlying.clone(),
            },
            FreezeTransition::Unfreeze => UnderlyingCommand::Unfreeze {
                underlying: self.underlying.clone(),
            },
        };

        ctx.underlying_store.send(&self.underlying, command).await.map_err(
            |source| FreezeTransitionError {
                underlying: self.underlying.clone(),
                transition: self.transition,
                source: Box::new(source),
            },
        )
    }
}

/// Why a freeze window could not be scheduled.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FreezeScheduleError {
    #[error(
        "freeze window is inverted: freeze_at {freeze_at} must precede \
         unfreeze_at {unfreeze_at}"
    )]
    InvertedWindow { freeze_at: DateTime<Utc>, unfreeze_at: DateTime<Utc> },
    #[error(
        "freeze window already elapsed: unfreeze_at {unfreeze_at} is not \
         after now ({now})"
    )]
    ElapsedWindow { unfreeze_at: DateTime<Utc>, now: DateTime<Utc> },
    #[error(transparent)]
    Push(#[from] QueuePushError),
}

/// Schedules freeze/unfreeze job pairs for corporate-action windows.
///
/// This is the API RAI-style automation (or an operator via the admin
/// endpoint) calls to arm a window; the schedule source — manual today,
/// Alpaca corporate-actions later — only decides who calls it.
#[derive(Clone)]
pub(crate) struct FreezeScheduler {
    queue: JobQueue<ApplyFreezeTransition>,
}

impl FreezeScheduler {
    pub(crate) fn new(apalis_pool: &apalis_sqlite::SqlitePool) -> Self {
        Self { queue: JobQueue::new(apalis_pool) }
    }

    /// Arms one freeze window for `underlying`: a `Freeze` at `freeze_at`
    /// and an `Unfreeze` at `unfreeze_at`.
    ///
    /// A `freeze_at` already in the past (window in progress) schedules the
    /// freeze immediately; a fully elapsed window is rejected rather than
    /// flapping the asset. Both jobs are idempotency-keyed by underlying and
    /// scheduled instant, so re-arming the same window is a no-op.
    pub(crate) async fn schedule_window(
        &mut self,
        underlying: &UnderlyingSymbol,
        freeze_at: DateTime<Utc>,
        unfreeze_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), FreezeScheduleError> {
        if freeze_at >= unfreeze_at {
            return Err(FreezeScheduleError::InvertedWindow {
                freeze_at,
                unfreeze_at,
            });
        }

        if unfreeze_at <= now {
            return Err(FreezeScheduleError::ElapsedWindow {
                unfreeze_at,
                now,
            });
        }

        let freeze_delay =
            (freeze_at - now).to_std().unwrap_or(std::time::Duration::ZERO);
        let unfreeze_delay =
            (unfreeze_at - now).to_std().unwrap_or(std::time::Duration::ZERO);

        self.queue
            .push_scheduled_batch([
                (
                    ApplyFreezeTransition {
                        underlying: underlying.clone(),
                        transition: FreezeTransition::Freeze,
                        scheduled_for: freeze_at,
                    },
                    format!("freeze:{underlying}:{}", freeze_at.timestamp()),
                    freeze_delay,
                ),
                (
                    ApplyFreezeTransition {
                        underlying: underlying.clone(),
                        transition: FreezeTransition::Unfreeze,
                        scheduled_for: unfreeze_at,
                    },
                    format!(
                        "unfreeze:{underlying}:{}",
                        unfreeze_at.timestamp()
                    ),
                    unfreeze_delay,
                ),
            ])
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};
    use event_sorcery::StoreBuilder;

    use super::{
        ApplyFreezeTransition, FreezeScheduleCtx, FreezeScheduleError,
        FreezeScheduler, FreezeTransition,
    };
    use crate::jobs::Job;
    use crate::mint::test_utils::TestHarness;
    use crate::tokenized_asset::UnderlyingSymbol;
    use crate::underlying::{AssetStatus, Underlying, load_freeze_status};

    // The job dispatches the same command path the CLI uses: perform on a
    // Freeze transition flips the asset to Frozen, Unfreeze flips it back,
    // and re-performing either is an idempotent no-op.
    #[tokio::test]
    async fn perform_applies_freeze_then_unfreeze_idempotently() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let pool = harness.pool.clone();
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let ctx = FreezeScheduleCtx { underlying_store };

        let freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            transition: FreezeTransition::Freeze,
            scheduled_for: Utc::now(),
        };
        freeze.perform(&ctx).await.unwrap();
        freeze.perform(&ctx).await.unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );

        let unfreeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            transition: FreezeTransition::Unfreeze,
            scheduled_for: Utc::now(),
        };
        unfreeze.perform(&ctx).await.unwrap();
        unfreeze.perform(&ctx).await.unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
    }

    #[tokio::test]
    async fn schedule_window_rejects_inverted_window() {
        let harness = TestHarness::new().await;
        let mut scheduler = FreezeScheduler::new(&harness.apalis_pool);
        let now = Utc::now();

        let error = scheduler
            .schedule_window(
                &UnderlyingSymbol::new("AAPL").unwrap(),
                now + ChronoDuration::hours(2),
                now + ChronoDuration::hours(1),
                now,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, FreezeScheduleError::InvertedWindow { .. }));
    }

    #[tokio::test]
    async fn schedule_window_rejects_elapsed_window() {
        let harness = TestHarness::new().await;
        let mut scheduler = FreezeScheduler::new(&harness.apalis_pool);
        let now = Utc::now();

        let error = scheduler
            .schedule_window(
                &UnderlyingSymbol::new("AAPL").unwrap(),
                now - ChronoDuration::hours(2),
                now - ChronoDuration::hours(1),
                now,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, FreezeScheduleError::ElapsedWindow { .. }));
    }

    // An in-progress window (freeze_at already past, unfreeze_at ahead) is
    // accepted: the freeze fires immediately rather than being dropped.
    #[tokio::test]
    async fn schedule_window_accepts_in_progress_window() {
        let harness = TestHarness::new().await;
        let mut scheduler = FreezeScheduler::new(&harness.apalis_pool);
        let now = Utc::now();

        scheduler
            .schedule_window(
                &UnderlyingSymbol::new("AAPL").unwrap(),
                now - ChronoDuration::minutes(5),
                now + ChronoDuration::hours(1),
                now,
            )
            .await
            .unwrap();
    }

    // Re-arming the identical window twice must not error: the idempotency
    // key collapses the duplicate enqueues.
    #[tokio::test]
    async fn schedule_window_is_idempotent_for_the_same_window() {
        let harness = TestHarness::new().await;
        let mut scheduler = FreezeScheduler::new(&harness.apalis_pool);
        let now = Utc::now();
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let freeze_at = now + ChronoDuration::hours(1);
        let unfreeze_at = now + ChronoDuration::hours(3);

        scheduler
            .schedule_window(&underlying, freeze_at, unfreeze_at, now)
            .await
            .unwrap();
        scheduler
            .schedule_window(&underlying, freeze_at, unfreeze_at, now)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn schedule_window_does_not_persist_half_a_window_when_unfreeze_insert_fails()
     {
        let harness = TestHarness::new().await;
        let mut scheduler = FreezeScheduler::new(&harness.apalis_pool);
        let now = Utc::now();
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();

        sqlx::query(
            r"
            CREATE TRIGGER reject_unfreeze_job
            BEFORE INSERT ON Jobs
            WHEN NEW.idempotency_key LIKE 'unfreeze:%'
            BEGIN
                SELECT RAISE(ABORT, 'injected unfreeze enqueue failure');
            END
            ",
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        let error = scheduler
            .schedule_window(
                &underlying,
                now + ChronoDuration::hours(1),
                now + ChronoDuration::hours(3),
                now,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, FreezeScheduleError::Push(_)));
        let persisted_transitions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE ?",
        )
        .bind("%:AAPL:%")
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(persisted_transitions, 0);
    }
}
