//! Scheduled dividend freeze/unfreeze for tokenized assets.
//!
//! Around an ex-date the supply freeze must flip on and off at known
//! instants. This module automates the trigger the issuer CLI fires by hand:
//! a [`FreezeScheduler`] enqueues two durable apalis jobs per corporate-action
//! window — acquire that window's hold before the ex-date and release it after.
//! The aggregate stays frozen while any operator or corporate-action hold is
//! active on the underlying, so one window cannot release another window's
//! cross-network freeze.
//!
//! Durability and idempotency: apalis persists the due time, so a scheduled
//! transition survives restarts, and the idempotency key (underlying + both
//! window boundaries) collapses re-submissions of the same window while its
//! jobs are still pending or running. Terminal rows (done, killed, or out of
//! retries) release their keys when the same window is re-armed, so an
//! infrastructure failure never permanently blocks a window. Acquiring or
//! releasing the same hold twice is a no-op.

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, warn};

use super::UnderlyingSymbol;
use super::view::{TokenizedAssetViewError, underlying_has_listing};
use crate::ApalisSqlitePool;
use crate::jobs::{Job, JobQueue, QueuePushError, ScheduledTask, job_type};
use crate::notifications::{
    FreezeTransitionKind, LifecycleNotification, LifecycleNotifier,
    SendLifecycleNotification,
};
use crate::underlying::{
    FreezeHoldId, FreezeWindow, Underlying, UnderlyingCommand, UnderlyingEvent,
    persisted_event_changed_freeze_status,
};

/// Which side of the freeze window a scheduled job applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum FreezeTransition {
    Freeze,
    Unfreeze,
}

impl FreezeTransition {
    /// Prefix naming this transition inside a job idempotency key, so the
    /// key and the job payload cannot disagree about which side of the
    /// window a row belongs to.
    const fn key_prefix(self) -> &'static str {
        match self {
            Self::Freeze => "freeze",
            Self::Unfreeze => "unfreeze",
        }
    }

    const fn lifecycle_kind(self) -> FreezeTransitionKind {
        match self {
            Self::Freeze => FreezeTransitionKind::Freeze,
            Self::Unfreeze => FreezeTransitionKind::Unfreeze,
        }
    }

    const fn persisted_event(
        self,
        hold_id: FreezeHoldId,
        transitioned_at: DateTime<Utc>,
    ) -> UnderlyingEvent {
        match self {
            Self::Freeze => UnderlyingEvent::FreezeHoldAcquired {
                hold_id,
                acquired_at: transitioned_at,
            },
            Self::Unfreeze => UnderlyingEvent::FreezeHoldReleased {
                hold_id,
                released_at: transitioned_at,
            },
        }
    }

    const fn applied_notification(
        self,
        underlying: UnderlyingSymbol,
    ) -> LifecycleNotification {
        match self {
            Self::Freeze => LifecycleNotification::FreezeApplied { underlying },
            Self::Unfreeze => {
                LifecycleNotification::UnfreezeApplied { underlying }
            }
        }
    }
}

/// Durable job applying one scheduled freeze transition to one asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ApplyFreezeTransition {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) hold_id: FreezeHoldId,
    pub(crate) transition: FreezeTransition,
    /// The instant this transition was scheduled for. Part of the
    /// idempotency key, and recorded for audit in worker logs.
    pub(crate) scheduled_for: DateTime<Utc>,
}

/// Runtime dependencies for [`ApplyFreezeTransition::perform`].
pub(crate) struct FreezeScheduleCtx {
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) apalis_pool: ApalisSqlitePool,
    pub(crate) underlying_store: Arc<Store<Underlying>>,
    pub(crate) notifier: Arc<dyn LifecycleNotifier>,
    #[cfg(test)]
    pub(crate) before_dispatch_barriers:
        Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>,
}

/// Error surfaced by a freeze-transition job.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FreezeTransitionError {
    #[error(
        "failed to dispatch scheduled {transition:?} for {underlying}: {source}"
    )]
    Dispatch {
        underlying: UnderlyingSymbol,
        transition: FreezeTransition,
        #[source]
        source: Box<AggregateError<LifecycleError<Underlying>>>,
    },
}

impl Job<FreezeScheduleCtx> for ApplyFreezeTransition {
    type Output = ();
    type Error = FreezeTransitionError;

    async fn perform(
        &self,
        ctx: &FreezeScheduleCtx,
    ) -> Result<Self::Output, Self::Error> {
        let transitioned_at = Utc::now();
        let command = match self.transition {
            FreezeTransition::Freeze => UnderlyingCommand::AcquireFreezeHold {
                underlying: self.underlying.clone(),
                hold_id: self.hold_id,
                acquired_at: transitioned_at,
            },
            FreezeTransition::Unfreeze => {
                UnderlyingCommand::ReleaseFreezeHold {
                    underlying: self.underlying.clone(),
                    hold_id: self.hold_id,
                    released_at: transitioned_at,
                }
            }
        };

        #[cfg(test)]
        if let Some((reached, proceed)) = &ctx.before_dispatch_barriers {
            reached.wait().await;
            proceed.wait().await;
        }

        if let Err(source) =
            ctx.underlying_store.send(&self.underlying, command).await
        {
            warn!(target: "asset", underlying = %self.underlying,
                transition = ?self.transition,
                scheduled_for = %self.scheduled_for,
                error = %source,
                "Scheduled freeze transition dispatch failed; apalis will \
                 retry until its attempt budget is exhausted"
            );
            let notification_key = format!(
                "notify:freeze-transition-failed:{}:{}:{}",
                self.underlying,
                self.hold_id,
                self.transition.key_prefix()
            );
            let mut notification_queue = JobQueue::new(&ctx.apalis_pool);
            if let Err(notification_error) = notification_queue
                .push_with_idempotency_key(
                    SendLifecycleNotification {
                        notification:
                            LifecycleNotification::FreezeTransitionFailed {
                                underlying: self.underlying.clone(),
                                transition: self.transition.lifecycle_kind(),
                            },
                    },
                    notification_key,
                )
                .await
            {
                warn!(target: "asset", underlying = %self.underlying,
                    transition = ?self.transition,
                    error = %notification_error,
                    "Failed to queue freeze-transition failure notification"
                );
            }
            return Err(FreezeTransitionError::Dispatch {
                underlying: self.underlying.clone(),
                transition: self.transition,
                source: Box::new(source),
            });
        }

        let persisted_event =
            self.transition.persisted_event(self.hold_id, transitioned_at);
        let changes_state = match persisted_event_changed_freeze_status(
            &ctx.pool,
            &self.underlying,
            &persisted_event,
        )
        .await
        {
            Ok(changed_state) => changed_state,
            Err(source) => {
                error!(target: "asset",
                    underlying = %self.underlying,
                    transition = ?self.transition,
                    error = %source,
                    "Could not inspect the persisted freeze transition outcome"
                );
                return Ok(());
            }
        };
        if changes_state {
            ctx.notifier
                .notify(
                    &self
                        .transition
                        .applied_notification(self.underlying.clone()),
                )
                .await;
        }

        Ok(())
    }
}

/// Why a freeze window could not be scheduled.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FreezeScheduleError {
    #[error("underlying {underlying} has no listing on any network")]
    UnknownUnderlying { underlying: UnderlyingSymbol },
    #[error(
        "freeze window is inverted: freeze_at {freeze_at} must precede \
         unfreeze_at {unfreeze_at}"
    )]
    InvertedWindow { freeze_at: DateTime<Utc>, unfreeze_at: DateTime<Utc> },
    #[error(
        "freeze window too short: freeze_at {freeze_at} and unfreeze_at \
         {unfreeze_at} must be at least one second apart (apalis schedules \
         at second granularity, so a sub-second window has no defined \
         execution order)"
    )]
    WindowTooShort { freeze_at: DateTime<Utc>, unfreeze_at: DateTime<Utc> },
    #[error(
        "freeze window already elapsed: unfreeze_at {unfreeze_at} is not \
         after now ({now})"
    )]
    ElapsedWindow { unfreeze_at: DateTime<Utc>, now: DateTime<Utc> },
    #[error(transparent)]
    Push(#[from] QueuePushError),
    #[error(transparent)]
    View(#[from] TokenizedAssetViewError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// Schedules freeze/unfreeze job pairs for corporate-action windows.
///
/// This is the API RAI-style automation (or an operator via the admin
/// endpoint) calls to arm a window; the schedule source — manual today,
/// Alpaca corporate-actions later — only decides who calls it.
#[derive(Clone)]
pub(crate) struct FreezeScheduler {
    queue: JobQueue<ApplyFreezeTransition>,
    /// Event-store pool for the terminal-row release; both pools address the
    /// same SQLite file (see `crate::jobs`).
    pool: Pool<Sqlite>,
}

impl FreezeScheduler {
    pub(crate) fn new(
        apalis_pool: &apalis_sqlite::SqlitePool,
        pool: Pool<Sqlite>,
    ) -> Self {
        Self { queue: JobQueue::new(apalis_pool), pool }
    }

    /// Arms one freeze window for `underlying`: acquire its hold at `freeze_at`
    /// and release that hold at `unfreeze_at`.
    ///
    /// A `freeze_at` already in the past (window in progress) schedules the
    /// freeze immediately; a fully elapsed window is rejected rather than
    /// flapping the asset. Both jobs are idempotency-keyed by underlying and
    /// both window boundaries, so re-arming the same window while it is
    /// pending or running is a no-op — but a terminal row (done, killed, or
    /// out of retries) releases its key first, so re-arming after an
    /// infrastructure failure enqueues fresh jobs instead of silently deduping
    /// against the dead ones. Acquiring or releasing the same hold twice is
    /// an idempotent no-op at the command level.
    pub(crate) async fn schedule_window(
        &mut self,
        underlying: &UnderlyingSymbol,
        freeze_at: DateTime<Utc>,
        unfreeze_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), FreezeScheduleError> {
        // The Underlying commands succeed for any symbol (they originate a
        // fresh stream), so scheduling for a symbol that was never listed
        // would silently arm a freeze that gates nothing. Gate here, not at
        // each caller, so no schedule source can skip the check.
        if !underlying_has_listing(&self.pool, underlying).await? {
            return Err(FreezeScheduleError::UnknownUnderlying {
                underlying: underlying.clone(),
            });
        }

        let Some(window) = FreezeWindow::new(freeze_at, unfreeze_at) else {
            return Err(FreezeScheduleError::InvertedWindow {
                freeze_at,
                unfreeze_at,
            });
        };

        // apalis stores due times at second granularity; two jobs due in the
        // same second have no defined execution order, so a sub-second window
        // could unfreeze before it freezes.
        if unfreeze_at - freeze_at < ChronoDuration::seconds(1) {
            return Err(FreezeScheduleError::WindowTooShort {
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
        let hold_id = FreezeHoldId::corporate_action(window);
        let window_key = format!(
            "{underlying}:{}:{}",
            window.freeze_at().to_rfc3339_opts(SecondsFormat::Nanos, true),
            window.unfreeze_at().to_rfc3339_opts(SecondsFormat::Nanos, true)
        );

        let freeze_key =
            format!("{}:{window_key}", FreezeTransition::Freeze.key_prefix());
        let unfreeze_key =
            format!("{}:{window_key}", FreezeTransition::Unfreeze.key_prefix());

        self.release_terminal_window_jobs([&freeze_key, &unfreeze_key]).await?;

        self.queue
            .push_scheduled_batch([
                ScheduledTask {
                    task: ApplyFreezeTransition {
                        underlying: underlying.clone(),
                        hold_id,
                        transition: FreezeTransition::Freeze,
                        scheduled_for: freeze_at,
                    },
                    idempotency_key: freeze_key,
                    run_after: freeze_delay,
                },
                ScheduledTask {
                    task: ApplyFreezeTransition {
                        underlying: underlying.clone(),
                        hold_id,
                        transition: FreezeTransition::Unfreeze,
                        scheduled_for: unfreeze_at,
                    },
                    idempotency_key: unfreeze_key,
                    run_after: unfreeze_delay,
                },
            ])
            .await?;

        Ok(())
    }

    /// Frees this window's idempotency keys from terminal rows so the enqueue
    /// below cannot dedup against a job that will never run again. apalis's
    /// `ON CONFLICT DO NOTHING` matches any row sharing the key — including
    /// `Done`/`Killed`/exhausted-`Failed` ones — so without this release a
    /// window whose job died of an infrastructure failure could never be
    /// re-armed. Pending and running rows are left untouched; deduping
    /// against those is the intended idempotency. Rows that died without
    /// applying (killed or out of retries) are surfaced at ERROR before their
    /// only record is deleted.
    async fn release_terminal_window_jobs(
        &self,
        idempotency_keys: [&str; 2],
    ) -> Result<(), sqlx::Error> {
        for idempotency_key in idempotency_keys {
            log_dead_freeze_jobs(&self.pool, Some(idempotency_key)).await?;

            sqlx::query(
                "
                DELETE FROM Jobs
                WHERE
                    job_type = ?
                    AND idempotency_key = ?
                    AND (
                        status IN ('Done', 'Killed')
                        OR (status = 'Failed' AND max_attempts <= attempts)
                    )
                ",
            )
            .bind(job_type::<ApplyFreezeTransition>())
            .bind(idempotency_key)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }
}

/// Logs at ERROR every [`ApplyFreezeTransition`] row that reached a dead
/// terminal state (killed, or `Failed` with its attempt budget exhausted) —
/// meaning a scheduled freeze/unfreeze never applied. Called before the
/// cleanup paths delete such rows, because the row is the only durable record
/// that the transition was ever armed. Scoped to one idempotency key when
/// releasing a window for re-arm, or unscoped for the startup vacuum.
async fn log_dead_freeze_jobs(
    pool: &Pool<Sqlite>,
    idempotency_key: Option<&str>,
) -> Result<(), sqlx::Error> {
    let dead_jobs: Vec<String> = if let Some(idempotency_key) = idempotency_key
    {
        sqlx::query_scalar(
            "
            SELECT idempotency_key
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key = ?
                AND (
                    status = 'Killed'
                    OR (status = 'Failed' AND max_attempts <= attempts)
                )
            ",
        )
        .bind(job_type::<ApplyFreezeTransition>())
        .bind(idempotency_key)
        .fetch_all(pool)
        .await?
    } else {
        let unfreeze_key_pattern =
            format!("{}:%", FreezeTransition::Unfreeze.key_prefix());
        sqlx::query_scalar(
            "
            SELECT idempotency_key
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key NOT LIKE ?
                AND (
                    status = 'Killed'
                    OR (status = 'Failed' AND max_attempts <= attempts)
                )
            ",
        )
        .bind(job_type::<ApplyFreezeTransition>())
        .bind(unfreeze_key_pattern)
        .fetch_all(pool)
        .await?
    };

    if !dead_jobs.is_empty() {
        error!(target: "asset", jobs = ?dead_jobs,
            "Scheduled freeze transitions died without applying; removing \
             their terminal rows"
        );
    }

    Ok(())
}

/// Flips recoverable freeze-schedule jobs back to `Pending`, clearing their
/// lock columns (`lock_at`, `lock_by`).
///
/// At startup no worker from this process is running yet, so any `Running` row
/// is an orphan from the previous process. Terminal unfreeze rows are also
/// requeued: deleting one after its hold was acquired would strand the named
/// hold forever, while replaying the release is safe because it is idempotent.
/// Terminal freeze rows remain terminal and are vacuumed separately because an
/// elapsed acquisition must not refreeze the asset. Scoped to this job type so
/// other apalis jobs sharing the table keep their own recovery policy.
pub(crate) async fn reset_orphaned_freeze_schedule_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    let result = sqlx::query(
        "
        UPDATE Jobs
        SET
            status = 'Pending',
            lock_at = NULL,
            lock_by = NULL
        WHERE
            job_type = ?
            AND status = 'Running'
        ",
    )
    .bind(job_type::<ApplyFreezeTransition>())
    .execute(pool)
    .await?;
    if result.rows_affected() > 0 {
        warn!(target: "asset", recovered_jobs = result.rows_affected(),
            "Requeued orphaned freeze-schedule jobs"
        );
    }
    requeue_terminal_unfreeze_jobs(pool).await?;

    Ok(())
}

async fn requeue_terminal_unfreeze_jobs(
    pool: &Pool<Sqlite>,
) -> Result<Vec<String>, sqlx::Error> {
    let unfreeze_key_pattern =
        format!("{}:%", FreezeTransition::Unfreeze.key_prefix());
    let requeued: Vec<String> = sqlx::query_scalar(
        "
        UPDATE Jobs
        SET
            status = 'Pending',
            attempts = 0,
            lock_at = NULL,
            lock_by = NULL
        WHERE
            job_type = ?
            AND idempotency_key LIKE ?
            AND (
                status = 'Killed'
                OR (status = 'Failed' AND max_attempts <= attempts)
            )
        RETURNING idempotency_key
        ",
    )
    .bind(job_type::<ApplyFreezeTransition>())
    .bind(unfreeze_key_pattern)
    .fetch_all(pool)
    .await?;

    if !requeued.is_empty() {
        warn!(target: "asset", recovered_jobs = requeued.len(), jobs = ?requeued,
            "Requeued terminal unfreeze jobs"
        );
    }

    Ok(requeued)
}

/// Requeues terminal unfreeze jobs every `interval` until shutdown.
///
/// Startup already performs one recovery pass, so the initial interval tick is
/// consumed and the first runtime pass occurs after one full interval. The loop
/// exits both for an explicit shutdown signal and when the sender is dropped.
pub(crate) async fn run_terminal_unfreeze_recovery(
    pool: Pool<Sqlite>,
    interval: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(error) = requeue_terminal_unfreeze_jobs(&pool).await {
                    warn!(target: "asset", error = %error,
                        "Failed to requeue terminal unfreeze jobs"
                    );
                }
            }
            _ = shutdown.changed() => break,
        }
    }
}

/// Deletes terminal apalis rows for [`ApplyFreezeTransition`] jobs, mirroring
/// the mint stack's terminal-job vacuums: it bounds the `Jobs` table across
/// restarts and frees idempotency keys held by concluded windows. Only
/// terminal rows are removed, so orphaned `Pending`/`Running` jobs apalis will
/// re-pick are left untouched. Completed unfreeze rows are safe to remove;
/// killed and exhausted unfreeze rows remain durable until startup or runtime
/// recovery requeues their compensating releases. Runs on the event-store pool
/// because both pools address the same SQLite file.
pub(crate) async fn vacuum_terminal_freeze_schedule_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    log_dead_freeze_jobs(pool, None).await?;
    let unfreeze_key_pattern =
        format!("{}:%", FreezeTransition::Unfreeze.key_prefix());

    sqlx::query(
        "
        DELETE FROM Jobs
        WHERE
            job_type = ?
            AND (
                (
                    idempotency_key NOT LIKE ?
                    AND (
                        status IN ('Done', 'Killed')
                        OR (status = 'Failed' AND max_attempts <= attempts)
                    )
                )
                OR (
                    idempotency_key LIKE ?
                    AND status = 'Done'
                )
            )
        ",
    )
    .bind(job_type::<ApplyFreezeTransition>())
    .bind(&unfreeze_key_pattern)
    .bind(&unfreeze_key_pattern)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};
    use event_sorcery::StoreBuilder;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        ApplyFreezeTransition, FreezeScheduleCtx, FreezeScheduleError,
        FreezeScheduler, FreezeTransition, reset_orphaned_freeze_schedule_jobs,
        vacuum_terminal_freeze_schedule_jobs,
    };
    use crate::jobs::{Job, JobQueue, ScheduledTask, job_type};
    use crate::mint::test_utils::TestHarness;
    use crate::notifications::{
        CapturingLifecycleNotifier, LifecycleNotification,
        SendLifecycleNotification,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::UnderlyingSymbol;
    use crate::underlying::{
        AssetStatus, FreezeHoldId, FreezeWindow, Underlying, UnderlyingCommand,
        load_freeze_status,
    };

    fn scheduler_for(harness: &TestHarness) -> FreezeScheduler {
        FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone())
    }

    fn hold_id(
        freeze_at: chrono::DateTime<Utc>,
        unfreeze_at: chrono::DateTime<Utc>,
    ) -> FreezeHoldId {
        FreezeHoldId::corporate_action(
            FreezeWindow::new(freeze_at, unfreeze_at).unwrap(),
        )
    }

    // A window owns one stable hold: acquiring it freezes the asset, releasing
    // it enables the asset only if no other holds remain, and repeating either
    // transition is an idempotent no-op.
    #[traced_test]
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
        let freeze_at = Utc::now();
        let unfreeze_at = freeze_at + ChronoDuration::hours(1);
        let hold_id = hold_id(freeze_at, unfreeze_at);
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let ctx = FreezeScheduleCtx {
            pool: pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };

        let freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id,
            transition: FreezeTransition::Freeze,
            scheduled_for: freeze_at,
        };
        freeze.perform(&ctx).await.unwrap();
        freeze.perform(&ctx).await.unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Acquiring underlying freeze hold", underlying.as_str()]
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Freeze hold already active", underlying.as_str()]
        ));

        let unfreeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id,
            transition: FreezeTransition::Unfreeze,
            scheduled_for: unfreeze_at,
        };
        unfreeze.perform(&ctx).await.unwrap();
        unfreeze.perform(&ctx).await.unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Releasing underlying freeze hold", underlying.as_str()]
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Freeze hold already absent", underlying.as_str()]
        ));
        assert_eq!(
            notifier.notifications(),
            vec![
                LifecycleNotification::FreezeApplied {
                    underlying: underlying.clone(),
                },
                LifecycleNotification::UnfreezeApplied { underlying },
            ]
        );
    }

    // A symbol that was never listed must be rejected before any job is
    // enqueued: the Underlying commands succeed for any symbol, so a typo'd
    // underlying would otherwise arm a freeze that gates nothing.
    #[tokio::test]
    async fn schedule_window_rejects_unlisted_underlying() {
        let harness = TestHarness::new().await;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();

        let error = scheduler
            .schedule_window(
                &UnderlyingSymbol::new("MSFT").unwrap(),
                now + ChronoDuration::hours(1),
                now + ChronoDuration::hours(3),
                now,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, FreezeScheduleError::UnknownUnderlying { .. }));

        let window_jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE ?",
        )
        .bind("%:MSFT:%")
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(window_jobs, 0);
    }

    #[traced_test]
    #[tokio::test]
    async fn repeated_transition_failures_queue_one_notification() {
        let store_harness = TestHarness::new().await;
        let underlying =
            store_harness.setup_account_and_asset().await.underlying;
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(store_harness.pool.clone())
                .build(())
                .await
                .unwrap();
        store_harness.pool.close().await;
        let notification_harness = TestHarness::new().await;
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let ctx = FreezeScheduleCtx {
            pool: notification_harness.pool.clone(),
            apalis_pool: notification_harness.apalis_pool.clone(),
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };
        let freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: hold_id(Utc::now(), Utc::now() + ChronoDuration::hours(1)),
            transition: FreezeTransition::Freeze,
            scheduled_for: Utc::now(),
        };

        freeze.perform(&ctx).await.unwrap_err();
        freeze.perform(&ctx).await.unwrap_err();

        assert!(notifier.notifications().is_empty());
        let queued_notifications: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<SendLifecycleNotification>())
                .fetch_one(&notification_harness.pool)
                .await
                .unwrap();
        assert_eq!(queued_notifications, 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn replay_read_failure_after_commit_does_not_retry_the_transition() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(harness.pool.clone())
                .build(())
                .await
                .unwrap();
        let closed_pool =
            SqlitePoolOptions::new().connect("sqlite::memory:").await.unwrap();
        closed_pool.close().await;
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let ctx = FreezeScheduleCtx {
            pool: closed_pool,
            apalis_pool: harness.apalis_pool.clone(),
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };
        let now = Utc::now();
        let transition = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: hold_id(now, now + ChronoDuration::hours(1)),
            transition: FreezeTransition::Freeze,
            scheduled_for: now,
        };

        transition.perform(&ctx).await.unwrap();

        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );
        assert!(notifier.notifications().is_empty());
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Could not inspect the persisted freeze transition outcome",
                "AAPL"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn overlapping_windows_remain_frozen_until_the_last_window_releases()
    {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let pool = harness.pool.clone();
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let ctx = FreezeScheduleCtx {
            pool: pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };
        let now = Utc::now();
        let first_hold = hold_id(
            now + ChronoDuration::hours(1),
            now + ChronoDuration::hours(3),
        );
        let second_hold = hold_id(
            now + ChronoDuration::hours(2),
            now + ChronoDuration::hours(4),
        );

        let first_freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: first_hold,
            transition: FreezeTransition::Freeze,
            scheduled_for: now + ChronoDuration::hours(1),
        };
        let second_freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: second_hold,
            transition: FreezeTransition::Freeze,
            scheduled_for: now + ChronoDuration::hours(2),
        };
        let first_unfreeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: first_hold,
            transition: FreezeTransition::Unfreeze,
            scheduled_for: now + ChronoDuration::hours(3),
        };
        let second_unfreeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: second_hold,
            transition: FreezeTransition::Unfreeze,
            scheduled_for: now + ChronoDuration::hours(4),
        };

        first_freeze.perform(&ctx).await.unwrap();
        second_freeze.perform(&ctx).await.unwrap();
        first_unfreeze.perform(&ctx).await.unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );

        second_unfreeze.perform(&ctx).await.unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Acquiring underlying freeze hold", "AAPL"]
        ));
        assert!(logs_contain_at!(
            Level::INFO,
            &["Releasing underlying freeze hold", "AAPL"]
        ));
        assert_eq!(
            notifier.notifications(),
            vec![
                LifecycleNotification::FreezeApplied {
                    underlying: underlying.clone(),
                },
                LifecycleNotification::UnfreezeApplied { underlying },
            ]
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn delayed_freeze_after_its_window_ends_does_not_refreeze_the_asset()
    {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let pool = harness.pool.clone();
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let ctx = FreezeScheduleCtx {
            pool: pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };
        let now = Utc::now();
        let expired_hold = hold_id(
            now - ChronoDuration::hours(2),
            now - ChronoDuration::hours(1),
        );

        ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: expired_hold,
            transition: FreezeTransition::Freeze,
            scheduled_for: now - ChronoDuration::hours(2),
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Skipping expired underlying freeze hold", "AAPL"]
        ));
        assert!(notifier.notifications().is_empty());
    }

    #[traced_test]
    #[tokio::test]
    async fn persisted_outcome_does_not_notify_for_a_concurrent_operator_hold()
    {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let pool = harness.pool.clone();
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let mut ctx = FreezeScheduleCtx {
            pool: pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };
        let freeze_at = Utc::now();
        let unfreeze_at = freeze_at + ChronoDuration::hours(1);
        let hold_id = hold_id(freeze_at, unfreeze_at);

        ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id,
            transition: FreezeTransition::Freeze,
            scheduled_for: freeze_at,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let proceed = Arc::new(tokio::sync::Barrier::new(2));
        ctx.before_dispatch_barriers = Some((reached.clone(), proceed.clone()));
        let unfreeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id,
            transition: FreezeTransition::Unfreeze,
            scheduled_for: unfreeze_at,
        };
        let acquire_operator_hold = async {
            reached.wait().await;
            let result = ctx
                .underlying_store
                .send(
                    &underlying,
                    UnderlyingCommand::Freeze {
                        underlying: underlying.clone(),
                    },
                )
                .await;
            proceed.wait().await;
            result
        };

        let (unfreeze_result, operator_result) =
            tokio::join!(unfreeze.perform(&ctx), acquire_operator_hold);
        operator_result.unwrap();
        unfreeze_result.unwrap();

        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );
        assert_eq!(
            notifier.notifications(),
            vec![LifecycleNotification::FreezeApplied {
                underlying: underlying.clone(),
            }],
            "the concurrent operator hold keeps the scheduled release from changing status"
        );

        ctx.underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Unfreeze { underlying: underlying.clone() },
            )
            .await
            .unwrap();
        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Releasing underlying freeze hold", "AAPL"]
        ));
        assert!(logs_contain_at!(
            Level::INFO,
            &["Unfreezing underlying across all networks", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn startup_requeues_terminal_unfreeze_and_releases_acquired_hold() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let pool = harness.pool.clone();
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let ctx = FreezeScheduleCtx {
            pool: pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
            underlying_store,
            notifier: Arc::new(CapturingLifecycleNotifier::default()),
            before_dispatch_barriers: None,
        };
        let now = Utc::now();
        let freeze_at = now - ChronoDuration::hours(1);
        let unfreeze_at = now + ChronoDuration::milliseconds(50);
        let hold_id = hold_id(freeze_at, unfreeze_at);

        ctx.underlying_store
            .send(
                &underlying,
                UnderlyingCommand::AcquireFreezeHold {
                    underlying: underlying.clone(),
                    hold_id,
                    acquired_at: freeze_at,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut queue = JobQueue::new(&harness.apalis_pool);
        queue
            .push_scheduled_batch([ScheduledTask {
                task: ApplyFreezeTransition {
                    underlying: underlying.clone(),
                    hold_id,
                    transition: FreezeTransition::Unfreeze,
                    scheduled_for: unfreeze_at,
                },
                idempotency_key: "unfreeze:AAPL:terminal-recovery".to_string(),
                run_after: std::time::Duration::ZERO,
            }])
            .await
            .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = ?",
        )
        .bind("unfreeze:AAPL:terminal-recovery")
        .execute(&harness.pool)
        .await
        .unwrap();

        reset_orphaned_freeze_schedule_jobs(&harness.pool).await.unwrap();
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "Requeued terminal unfreeze jobs",
                "recovered_jobs=1",
                "unfreeze:AAPL:terminal-recovery"
            ]
        ));

        let status: String = sqlx::query_scalar(
            "SELECT status FROM Jobs WHERE idempotency_key = ?",
        )
        .bind("unfreeze:AAPL:terminal-recovery")
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(status, "Pending");

        ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id,
            transition: FreezeTransition::Unfreeze,
            scheduled_for: unfreeze_at,
        }
        .perform(&ctx)
        .await
        .unwrap();
        assert_eq!(
            load_freeze_status(&pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn runtime_recovery_pass_requeues_terminal_unfreeze_without_resetting_running_jobs()
     {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let now = Utc::now();
        let freeze_at = now - ChronoDuration::hours(1);
        let unfreeze_at = now + ChronoDuration::hours(1);
        let terminal_key = "unfreeze:AAPL:runtime-terminal";
        let running_key = "unfreeze:AAPL:runtime-running";
        let mut queue = JobQueue::new(&harness.apalis_pool);
        queue
            .push_scheduled_batch([
                ScheduledTask {
                    task: ApplyFreezeTransition {
                        underlying: underlying.clone(),
                        hold_id: hold_id(freeze_at, unfreeze_at),
                        transition: FreezeTransition::Unfreeze,
                        scheduled_for: unfreeze_at,
                    },
                    idempotency_key: terminal_key.to_string(),
                    run_after: std::time::Duration::ZERO,
                },
                ScheduledTask {
                    task: ApplyFreezeTransition {
                        underlying,
                        hold_id: hold_id(freeze_at, unfreeze_at),
                        transition: FreezeTransition::Unfreeze,
                        scheduled_for: unfreeze_at,
                    },
                    idempotency_key: running_key.to_string(),
                    run_after: std::time::Duration::ZERO,
                },
            ])
            .await
            .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = ?",
        )
        .bind(terminal_key)
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Running' WHERE idempotency_key = ?",
        )
        .bind(running_key)
        .execute(&harness.pool)
        .await
        .unwrap();

        super::requeue_terminal_unfreeze_jobs(&harness.pool).await.unwrap();

        let statuses: Vec<(String, String)> = sqlx::query_as(
            "SELECT idempotency_key, status FROM Jobs WHERE idempotency_key IN (?, ?) ORDER BY idempotency_key",
        )
        .bind(running_key)
        .bind(terminal_key)
        .fetch_all(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            statuses,
            vec![
                (running_key.to_string(), "Running".to_string()),
                (terminal_key.to_string(), "Pending".to_string()),
            ]
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "Requeued terminal unfreeze jobs",
                "recovered_jobs=1",
                terminal_key
            ]
        ));
    }

    #[tokio::test]
    async fn vacuum_preserves_terminal_unfreeze_without_a_successful_reset() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let now = Utc::now();
        let freeze_at = now - ChronoDuration::hours(1);
        let unfreeze_at = now + ChronoDuration::hours(1);
        let key = "unfreeze:AAPL:reset-failed";
        let mut queue = JobQueue::new(&harness.apalis_pool);
        queue
            .push_scheduled_batch([ScheduledTask {
                task: ApplyFreezeTransition {
                    underlying,
                    hold_id: hold_id(freeze_at, unfreeze_at),
                    transition: FreezeTransition::Unfreeze,
                    scheduled_for: unfreeze_at,
                },
                idempotency_key: key.to_string(),
                run_after: std::time::Duration::ZERO,
            }])
            .await
            .unwrap();
        let updated = sqlx::query(
            "UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = ?",
        )
        .bind(key)
        .execute(&harness.pool)
        .await
        .unwrap();
        assert_eq!(updated.rows_affected(), 1);

        vacuum_terminal_freeze_schedule_jobs(&harness.pool).await.unwrap();

        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
        )
        .bind(key)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(remaining, 1);
    }

    #[tokio::test]
    async fn vacuum_removes_completed_unfreeze_and_preserves_terminal_failure()
    {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let now = Utc::now();
        let freeze_at = now - ChronoDuration::hours(1);
        let unfreeze_at = now + ChronoDuration::hours(1);
        let completed_key = "unfreeze:AAPL:completed";
        let terminal_key = "unfreeze:AAPL:terminal";
        let mut queue = JobQueue::new(&harness.apalis_pool);
        queue
            .push_scheduled_batch([
                ScheduledTask {
                    task: ApplyFreezeTransition {
                        underlying: underlying.clone(),
                        hold_id: hold_id(freeze_at, unfreeze_at),
                        transition: FreezeTransition::Unfreeze,
                        scheduled_for: unfreeze_at,
                    },
                    idempotency_key: completed_key.to_string(),
                    run_after: std::time::Duration::ZERO,
                },
                ScheduledTask {
                    task: ApplyFreezeTransition {
                        underlying,
                        hold_id: hold_id(freeze_at, unfreeze_at),
                        transition: FreezeTransition::Unfreeze,
                        scheduled_for: unfreeze_at,
                    },
                    idempotency_key: terminal_key.to_string(),
                    run_after: std::time::Duration::ZERO,
                },
            ])
            .await
            .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Done' WHERE idempotency_key = ?",
        )
        .bind(completed_key)
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = ?",
        )
        .bind(terminal_key)
        .execute(&harness.pool)
        .await
        .unwrap();

        vacuum_terminal_freeze_schedule_jobs(&harness.pool).await.unwrap();

        let remaining_keys: Vec<String> = sqlx::query_scalar(
            "SELECT idempotency_key FROM Jobs WHERE idempotency_key IN (?, ?) ORDER BY idempotency_key",
        )
        .bind(completed_key)
        .bind(terminal_key)
        .fetch_all(&harness.pool)
        .await
        .unwrap();
        assert_eq!(remaining_keys, vec![terminal_key.to_string()]);
    }

    #[tokio::test]
    async fn schedule_window_rejects_inverted_window() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();

        let error = scheduler
            .schedule_window(
                &underlying,
                now + ChronoDuration::hours(2),
                now + ChronoDuration::hours(1),
                now,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, FreezeScheduleError::InvertedWindow { .. }));
    }

    // apalis schedules at second granularity, so a window narrower than one
    // second has no defined freeze-before-unfreeze order and is rejected.
    #[tokio::test]
    async fn schedule_window_rejects_sub_second_window() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();
        let freeze_at = now + ChronoDuration::hours(1);

        let error = scheduler
            .schedule_window(
                &underlying,
                freeze_at,
                freeze_at + ChronoDuration::milliseconds(500),
                now,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, FreezeScheduleError::WindowTooShort { .. }));
    }

    #[tokio::test]
    async fn schedule_window_rejects_elapsed_window() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();

        let error = scheduler
            .schedule_window(
                &underlying,
                now - ChronoDuration::hours(2),
                now - ChronoDuration::hours(1),
                now,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, FreezeScheduleError::ElapsedWindow { .. }));
    }

    // An in-progress window (freeze_at already past, unfreeze_at ahead) is
    // accepted, and the freeze job is due immediately rather than waiting for
    // the already-elapsed freeze_at while the unfreeze keeps its future due
    // time.
    #[tokio::test]
    async fn schedule_window_accepts_in_progress_window() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();

        scheduler
            .schedule_window(
                &underlying,
                now - ChronoDuration::minutes(5),
                now + ChronoDuration::hours(1),
                now,
            )
            .await
            .unwrap();

        let freeze_run_at: i64 = sqlx::query_scalar(
            "SELECT run_at FROM Jobs WHERE idempotency_key LIKE 'freeze:%'",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        let unfreeze_run_at: i64 = sqlx::query_scalar(
            "SELECT run_at FROM Jobs WHERE idempotency_key LIKE 'unfreeze:%'",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();

        assert!(
            freeze_run_at <= now.timestamp() + 1,
            "in-progress window must schedule the freeze immediately, \
             got run_at {freeze_run_at} vs now {}",
            now.timestamp()
        );
        assert!(
            unfreeze_run_at >= now.timestamp() + 3590,
            "unfreeze must keep its future due time, got run_at \
             {unfreeze_run_at} vs now {}",
            now.timestamp()
        );
    }

    // Re-arming the identical window twice must not error, and the
    // idempotency key must actually collapse the duplicate enqueues: exactly
    // one freeze and one unfreeze row exist afterwards.
    #[tokio::test]
    async fn schedule_window_is_idempotent_for_the_same_window() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();
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

        let freeze_jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs \
             WHERE job_type = ? AND idempotency_key LIKE 'freeze:%'",
        )
        .bind(job_type::<ApplyFreezeTransition>())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        let unfreeze_jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs \
             WHERE job_type = ? AND idempotency_key LIKE 'unfreeze:%'",
        )
        .bind(job_type::<ApplyFreezeTransition>())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            freeze_jobs, 1,
            "re-arming the same window must dedup the freeze job"
        );
        assert_eq!(
            unfreeze_jobs, 1,
            "re-arming the same window must dedup the unfreeze job"
        );
    }

    // A window whose job died terminally (e.g. exhausted retries after an
    // infrastructure failure) must be re-armable: the terminal row's
    // idempotency key is released and a fresh Pending job replaces it.
    #[tokio::test]
    async fn schedule_window_rearms_after_killed_job() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();
        let freeze_at = now + ChronoDuration::hours(1);
        let unfreeze_at = now + ChronoDuration::hours(3);

        scheduler
            .schedule_window(&underlying, freeze_at, unfreeze_at, now)
            .await
            .unwrap();

        sqlx::query(
            "
            UPDATE Jobs
            SET status = 'Killed'
            WHERE idempotency_key LIKE 'freeze:%'
            ",
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        scheduler
            .schedule_window(&underlying, freeze_at, unfreeze_at, now)
            .await
            .unwrap();

        let freeze_statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM Jobs WHERE idempotency_key LIKE 'freeze:%'",
        )
        .fetch_all(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            freeze_statuses,
            vec!["Pending".to_string()],
            "re-arming must replace the terminal freeze job with a fresh \
             Pending one"
        );
    }

    #[tokio::test]
    async fn startup_reset_and_vacuum_preserve_live_schedule_jobs() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();
        scheduler
            .schedule_window(
                &underlying,
                now + ChronoDuration::hours(1),
                now + ChronoDuration::hours(3),
                now,
            )
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES ('dead-freeze-worker', ?, 'SqliteStorage')",
        )
        .bind(job_type::<ApplyFreezeTransition>())
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_at = strftime('%s', 'now'), lock_by = 'dead-freeze-worker' WHERE idempotency_key LIKE 'freeze:%'",
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        reset_orphaned_freeze_schedule_jobs(&harness.pool).await.unwrap();
        let (status, lock_at, lock_by):
            (String, Option<i64>, Option<String>) = sqlx::query_as(
                "SELECT status, lock_at, lock_by FROM Jobs WHERE idempotency_key LIKE 'freeze:%'",
            )
            .fetch_one(&harness.pool)
            .await
            .unwrap();
        assert_eq!(status, "Pending");
        assert!(lock_at.is_none() && lock_by.is_none());

        sqlx::query(
            "UPDATE Jobs SET status = 'Done' WHERE idempotency_key LIKE 'freeze:%'",
        )
        .execute(&harness.pool)
        .await
        .unwrap();
        vacuum_terminal_freeze_schedule_jobs(&harness.pool).await.unwrap();

        let remaining_keys: Vec<String> = sqlx::query_scalar(
            "SELECT idempotency_key FROM Jobs ORDER BY idempotency_key",
        )
        .fetch_all(&harness.pool)
        .await
        .unwrap();
        assert_eq!(remaining_keys.len(), 1);
        assert!(remaining_keys[0].starts_with("unfreeze:"));
    }

    // Two windows in the same UTC second must not share an idempotency key:
    // truncating to `timestamp()` would collide and drop one schedule.
    #[tokio::test]
    async fn schedule_window_keeps_subsecond_idempotency_keys_distinct() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();
        let freeze_a = now + ChronoDuration::hours(1);
        let freeze_b = freeze_a + ChronoDuration::milliseconds(250);
        let unfreeze_a = freeze_a + ChronoDuration::hours(2);
        let unfreeze_b = freeze_b + ChronoDuration::hours(2);

        scheduler
            .schedule_window(&underlying, freeze_a, unfreeze_a, now)
            .await
            .unwrap();
        scheduler
            .schedule_window(&underlying, freeze_b, unfreeze_b, now)
            .await
            .unwrap();

        let freeze_keys: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE ?",
        )
        .bind("freeze:AAPL:%")
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            freeze_keys, 2,
            "subsecond-distinct freeze schedules must not collide"
        );
    }

    #[tokio::test]
    async fn distinct_subsecond_windows_get_distinct_jobs() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();
        let freeze_at = now + ChronoDuration::hours(1);
        let unfreeze_at = now + ChronoDuration::hours(3);

        scheduler
            .schedule_window(&underlying, freeze_at, unfreeze_at, now)
            .await
            .unwrap();
        scheduler
            .schedule_window(
                &underlying,
                freeze_at + ChronoDuration::nanoseconds(1),
                unfreeze_at + ChronoDuration::nanoseconds(1),
                now,
            )
            .await
            .unwrap();

        let persisted_transitions: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(persisted_transitions, 4);
    }

    #[tokio::test]
    async fn schedule_window_does_not_persist_half_a_window_when_unfreeze_insert_fails()
     {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = scheduler_for(&harness);
        let now = Utc::now();

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
