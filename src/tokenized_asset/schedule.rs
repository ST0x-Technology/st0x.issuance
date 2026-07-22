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

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use cqrs_es::AggregateError;
use event_sorcery::{EventSourced, LifecycleError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, Pool, Sqlite};
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::Barrier;
use tokio::sync::{Mutex, MutexGuard};
use tracing::{error, warn};

use super::view::{TokenizedAssetViewError, underlying_has_listing};
use super::{CorporateActionEventId, CorporateActionId, UnderlyingSymbol};
use crate::jobs::{Job, JobQueue, QueuePushError, ScheduledTask, job_type};
use crate::notifications::{
    FreezeTransitionKind, LifecycleNotification, LifecycleNotifier,
    SendLifecycleNotification,
};
use crate::underlying::{
    AssetStatus, FreezeHoldId, FreezeWindow, Underlying, UnderlyingCommand,
    UnderlyingEvent,
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

    fn matches_event(
        self,
        event: &UnderlyingEvent,
        expected_hold_id: &FreezeHoldId,
        transitioned_at: DateTime<Utc>,
    ) -> bool {
        match (self, event) {
            (
                Self::Freeze,
                UnderlyingEvent::FreezeHoldAcquired { hold_id, acquired_at },
            ) => hold_id == expected_hold_id && *acquired_at == transitioned_at,
            (
                Self::Unfreeze,
                UnderlyingEvent::FreezeHoldReleased { hold_id, released_at },
            ) => hold_id == expected_hold_id && *released_at == transitioned_at,
            _ => false,
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
    pub(crate) underlying_store: Arc<Store<Underlying>>,
    pub(crate) notifier: Arc<dyn LifecycleNotifier>,
    #[cfg(test)]
    pub(crate) before_dispatch_barriers:
        Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>,
}

/// Durable alignment of one Alpaca-owned hold to its latest projected
/// corporate-action revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AlignCorporateActionFreeze {
    pub(crate) action_id: CorporateActionId,
    pub(crate) expected_event_id: CorporateActionEventId,
}

#[cfg(test)]
pub(crate) struct RevisionReadTestHook {
    pub(crate) observed: Arc<Barrier>,
    pub(crate) release: Arc<Barrier>,
}

static CORPORATE_ACTION_REVISION_GUARD: Mutex<()> = Mutex::const_new(());

/// Serializes projection commits with the action-owned hold effects authorized
/// by their expected revision. The issuer is a single-writer service, so one
/// process-wide guard closes this boundary without a distributed lease.
pub(crate) async fn acquire_corporate_action_revision_guard()
-> MutexGuard<'static, ()> {
    CORPORATE_ACTION_REVISION_GUARD.lock().await
}

const CORPORATE_ACTION_ALIGNMENT_MAX_ATTEMPTS: u32 = 10;

/// Rows apalis will never run again. Recovery, logging, notification, and
/// cleanup share this predicate so a terminal job cannot fall between paths.
const DEAD_JOB_PREDICATE: &str = "
    status = 'Killed'
    OR (status = 'Failed' AND max_attempts <= attempts)
";

pub(crate) struct CorporateActionFreezeCtx {
    pub(crate) underlying_store: Arc<Store<Underlying>>,
    pub(crate) pool: Pool<Sqlite>,
    #[cfg(test)]
    pub(crate) revision_read_test_hook: Option<RevisionReadTestHook>,
}

#[derive(Clone)]
pub(crate) struct CorporateActionFreezeScheduler {
    queue: JobQueue<AlignCorporateActionFreeze>,
    notification_queue: JobQueue<SendLifecycleNotification>,
    pool: Pool<Sqlite>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CorporateActionScheduleState {
    Active,
    Deleted,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionScheduleError {
    #[error("underlying {underlying} has no listing on any network")]
    UnknownUnderlying { underlying: UnderlyingSymbol },
    #[error(transparent)]
    Push(#[from] QueuePushError),
    #[error(transparent)]
    View(#[from] TokenizedAssetViewError),
    #[error("corporate-action ex-date {0} has no following day")]
    InvalidExDate(NaiveDate),
}

impl CorporateActionFreezeScheduler {
    pub(crate) fn new(
        apalis_pool: &apalis_sqlite::SqlitePool,
        pool: Pool<Sqlite>,
    ) -> Self {
        Self {
            queue: JobQueue::new(apalis_pool),
            notification_queue: JobQueue::new(apalis_pool),
            pool,
        }
    }

    pub(crate) async fn schedule_revision(
        &mut self,
        action_id: &CorporateActionId,
        event_id: &CorporateActionEventId,
        underlying: &UnderlyingSymbol,
        ex_date: NaiveDate,
        state: CorporateActionScheduleState,
        now: DateTime<Utc>,
    ) -> Result<(), CorporateActionScheduleError> {
        if state == CorporateActionScheduleState::Active
            && !underlying_has_listing(&self.pool, underlying).await?
        {
            return Err(CorporateActionScheduleError::UnknownUnderlying {
                underlying: underlying.clone(),
            });
        }

        let key_prefix = format!("corporate-action:{action_id}:{event_id}");
        let immediate = ScheduledTask {
            task: AlignCorporateActionFreeze {
                action_id: action_id.clone(),
                expected_event_id: event_id.clone(),
            },
            idempotency_key: format!("{key_prefix}:immediate"),
            run_after: std::time::Duration::ZERO,
            max_attempts: Some(CORPORATE_ACTION_ALIGNMENT_MAX_ATTEMPTS),
        };
        if state == CorporateActionScheduleState::Deleted {
            self.queue.push_scheduled_batch([immediate]).await?;
            return Ok(());
        }

        // The feed is restricted to the US market. UTC midnight begins before
        // the US/Eastern session opens and the following UTC midnight ends
        // after it closes, so this window contains the full ex-date session.
        let freeze_at = ex_date
            .and_hms_opt(0, 0, 0)
            .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
            .ok_or(CorporateActionScheduleError::InvalidExDate(ex_date))?;
        let unfreeze_at = ex_date
            .succ_opt()
            .and_then(|value| value.and_hms_opt(0, 0, 0))
            .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
            .ok_or(CorporateActionScheduleError::InvalidExDate(ex_date))?;
        let freeze_delay =
            (freeze_at - now).to_std().unwrap_or(std::time::Duration::ZERO);
        let unfreeze_delay =
            (unfreeze_at - now).to_std().unwrap_or(std::time::Duration::ZERO);
        if unfreeze_at <= now {
            self.queue.push_scheduled_batch([immediate]).await?;
            return Ok(());
        }

        self.queue
            .push_scheduled_batch([
                immediate,
                ScheduledTask {
                    task: AlignCorporateActionFreeze {
                        action_id: action_id.clone(),
                        expected_event_id: event_id.clone(),
                    },
                    idempotency_key: format!("{key_prefix}:freeze"),
                    run_after: freeze_delay,
                    max_attempts: Some(CORPORATE_ACTION_ALIGNMENT_MAX_ATTEMPTS),
                },
                ScheduledTask {
                    task: AlignCorporateActionFreeze {
                        action_id: action_id.clone(),
                        expected_event_id: event_id.clone(),
                    },
                    idempotency_key: format!("{key_prefix}:unfreeze"),
                    run_after: unfreeze_delay,
                    max_attempts: Some(CORPORATE_ACTION_ALIGNMENT_MAX_ATTEMPTS),
                },
            ])
            .await?;

        self.notification_queue
            .push_with_idempotency_key(
                SendLifecycleNotification {
                    notification:
                        LifecycleNotification::CorporateActionScheduled {
                            underlying: underlying.clone(),
                            ex_date,
                            freeze_at,
                            unfreeze_at,
                        },
                },
                format!(
                    "notify:corporate-action:{underlying}:{ex_date}:{}:{}",
                    freeze_at.timestamp(),
                    unfreeze_at.timestamp()
                ),
            )
            .await?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionFreezeError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    View(#[from] TokenizedAssetViewError),
    #[error("invalid stored corporate-action underlying {0}")]
    InvalidUnderlying(String),
    #[error("invalid stored corporate-action ex-date {0}")]
    InvalidExDate(String),
    #[error(transparent)]
    Aggregate(#[from] Box<AggregateError<LifecycleError<Underlying>>>),
}

impl Job<CorporateActionFreezeCtx> for AlignCorporateActionFreeze {
    type Output = ();
    type Error = CorporateActionFreezeError;

    async fn perform(
        &self,
        ctx: &CorporateActionFreezeCtx,
    ) -> Result<Self::Output, Self::Error> {
        let _revision_guard = acquire_corporate_action_revision_guard().await;
        let Some((event_id, underlying, ex_date, deleted)) =
            sqlx::query_as::<_, (String, String, String, i64)>(
                "
                SELECT event_id, underlying, ex_date, deleted
                FROM corporate_action_schedule
                WHERE action_id = ?
                ",
            )
            .bind(self.action_id.as_str())
            .fetch_optional(&ctx.pool)
            .await?
        else {
            return Ok(());
        };

        if event_id != self.expected_event_id.as_str() {
            return Ok(());
        }

        #[cfg(test)]
        if let Some(hook) = &ctx.revision_read_test_hook {
            hook.observed.wait().await;
            hook.release.wait().await;
        }

        let underlying_symbol =
            UnderlyingSymbol::new(&underlying).map_err(|_| {
                CorporateActionFreezeError::InvalidUnderlying(
                    underlying.clone(),
                )
            })?;
        let ex_date =
            NaiveDate::parse_from_str(&ex_date, "%Y-%m-%d").map_err(|_| {
                CorporateActionFreezeError::InvalidExDate(ex_date.clone())
            })?;
        let freeze_at = ex_date
            .and_hms_opt(0, 0, 0)
            .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| {
                CorporateActionFreezeError::InvalidExDate(ex_date.to_string())
            })?;
        let unfreeze_at = ex_date
            .succ_opt()
            .and_then(|value| value.and_hms_opt(0, 0, 0))
            .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| {
                CorporateActionFreezeError::InvalidExDate(ex_date.to_string())
            })?;
        let now = Utc::now();
        let hold_id =
            FreezeHoldId::alpaca_corporate_action(self.action_id.clone());
        let should_hold = deleted == 0
            && underlying_has_listing(&ctx.pool, &underlying_symbol).await?
            && now >= freeze_at
            && now < unfreeze_at;
        let observed_underlyings: Vec<String> = sqlx::query_scalar(
            "
            SELECT DISTINCT underlying
            FROM corporate_action_mutations
            WHERE action_id = ? AND underlying IS NOT NULL
            ",
        )
        .bind(self.action_id.as_str())
        .fetch_all(&ctx.pool)
        .await?;

        if should_hold {
            ctx.underlying_store
                .send(
                    &underlying_symbol,
                    UnderlyingCommand::AcquireFreezeHold {
                        underlying: underlying_symbol.clone(),
                        hold_id: hold_id.clone(),
                        acquired_at: now,
                    },
                )
                .await
                .map_err(|source| {
                    CorporateActionFreezeError::Aggregate(Box::new(source))
                })?;
        }

        for observed_underlying in observed_underlyings {
            let observed_underlying =
                UnderlyingSymbol::new(&observed_underlying).map_err(|_| {
                    CorporateActionFreezeError::InvalidUnderlying(
                        observed_underlying.clone(),
                    )
                })?;
            if should_hold && observed_underlying == underlying_symbol {
                continue;
            }
            ctx.underlying_store
                .send(
                    &observed_underlying,
                    UnderlyingCommand::ReleaseFreezeHold {
                        underlying: observed_underlying.clone(),
                        hold_id: hold_id.clone(),
                        released_at: now,
                    },
                )
                .await
                .map_err(|source| {
                    CorporateActionFreezeError::Aggregate(Box::new(source))
                })?;
        }

        Ok(())
    }
}

/// Error surfaced by a freeze-transition job. Command dispatch is the only
/// fallible step; `Freeze`/`Unfreeze` on an already-transitioned asset is a
/// no-op, not an error.
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
    #[error(
        "failed to inspect persisted scheduled {transition:?} outcome for {underlying}: {source}"
    )]
    PersistedOutcome {
        underlying: UnderlyingSymbol,
        transition: FreezeTransition,
        #[source]
        source: PersistedFreezeTransitionOutcomeError,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PersistedFreezeTransitionOutcomeError {
    #[error("failed to load the underlying event stream")]
    Database(#[from] sqlx::Error),
    #[error("failed to decode underlying event at sequence {sequence}")]
    Decode {
        sequence: i64,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "underlying event at sequence {sequence} cannot originate a stream"
    )]
    CannotOriginate { sequence: i64 },
    #[error("underlying event at sequence {sequence} cannot evolve the stream")]
    CannotEvolve { sequence: i64 },
}

/// Replays the retained Underlying stream through the exact event produced by
/// this attempt. Matching both the hold and transition timestamp distinguishes
/// this command from an interleaved operator or overlapping-window command.
async fn persisted_transition_changed_state(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
    hold_id: &FreezeHoldId,
    transition: FreezeTransition,
    transitioned_at: DateTime<Utc>,
) -> Result<bool, PersistedFreezeTransitionOutcomeError> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT sequence, payload
        FROM events
        WHERE aggregate_type = ? AND aggregate_id = ?
        ORDER BY sequence
        ",
    )
    .bind(Underlying::AGGREGATE_TYPE)
    .bind(underlying.to_string())
    .fetch_all(pool)
    .await?;

    let mut state: Option<Underlying> = None;
    let mut changed_state = None;
    for (sequence, payload) in rows {
        let event: UnderlyingEvent =
            serde_json::from_str(&payload).map_err(|source| {
                PersistedFreezeTransitionOutcomeError::Decode {
                    sequence,
                    source,
                }
            })?;
        let before = state
            .as_ref()
            .map_or(AssetStatus::Enabled, Underlying::freeze_status);
        state = match state.as_ref() {
            None => Some(Underlying::originate(&event).ok_or(
                PersistedFreezeTransitionOutcomeError::CannotOriginate {
                    sequence,
                },
            )?),
            Some(current) => match Underlying::evolve(current, &event) {
                Ok(Some(next)) => Some(next),
                Ok(None) => {
                    return Err(
                        PersistedFreezeTransitionOutcomeError::CannotEvolve {
                            sequence,
                        },
                    );
                }
                Err(never) => match never {},
            },
        };

        if transition.matches_event(&event, hold_id, transitioned_at) {
            let after = state
                .as_ref()
                .map_or(AssetStatus::Enabled, Underlying::freeze_status);
            changed_state = Some(before != after);
        }
    }

    Ok(changed_state.unwrap_or(false))
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
                hold_id: self.hold_id.clone(),
                acquired_at: transitioned_at,
            },
            FreezeTransition::Unfreeze => {
                UnderlyingCommand::ReleaseFreezeHold {
                    underlying: self.underlying.clone(),
                    hold_id: self.hold_id.clone(),
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
            ctx.notifier
                .notify(&LifecycleNotification::FreezeTransitionFailed {
                    underlying: self.underlying.clone(),
                    transition: self.transition.lifecycle_kind(),
                })
                .await;
            return Err(FreezeTransitionError::Dispatch {
                underlying: self.underlying.clone(),
                transition: self.transition,
                source: Box::new(source),
            });
        }

        let changes_state = persisted_transition_changed_state(
            &ctx.pool,
            &self.underlying,
            &self.hold_id,
            self.transition,
            transitioned_at,
        )
        .await
        .map_err(|source| FreezeTransitionError::PersistedOutcome {
            underlying: self.underlying.clone(),
            transition: self.transition,
            source,
        })?;
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
            window
                .freeze_at()
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            window
                .unfreeze_at()
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
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
                        hold_id: hold_id.clone(),
                        transition: FreezeTransition::Freeze,
                        scheduled_for: freeze_at,
                    },
                    idempotency_key: freeze_key,
                    run_after: freeze_delay,
                    max_attempts: None,
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
                    max_attempts: None,
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

            let query = format!(
                "
                DELETE FROM Jobs
                WHERE
                    job_type = ?
                    AND idempotency_key = ?
                    AND (
                        status = 'Done'
                        OR ({DEAD_JOB_PREDICATE})
                    )
                "
            );
            sqlx::query(AssertSqlSafe(query))
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
        let query = format!(
            "
            SELECT idempotency_key
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key = ?
                AND ({DEAD_JOB_PREDICATE})
            "
        );
        sqlx::query_scalar(AssertSqlSafe(query))
            .bind(job_type::<ApplyFreezeTransition>())
            .bind(idempotency_key)
            .fetch_all(pool)
            .await?
    } else {
        let query = format!(
            "
            SELECT idempotency_key
            FROM Jobs
            WHERE
                job_type = ?
                AND ({DEAD_JOB_PREDICATE})
            "
        );
        sqlx::query_scalar(AssertSqlSafe(query))
            .bind(job_type::<ApplyFreezeTransition>())
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum FreezeScheduleRecoveryError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Push(#[from] QueuePushError),
}

/// Flips freeze-schedule jobs left `Running` by a dead process back to
/// `Pending`, clearing their lock columns (`lock_at`, `lock_by`). Dead
/// corporate-action alignment jobs (`Killed` or exhausted `Failed`) are also
/// re-armed from attempt zero: their commands are idempotent, and leaving one
/// terminal would strand the latest projected freeze state permanently because
/// the projection has already marked that revision reconciled.
///
/// At startup no worker from this process is running yet, so any `Running`
/// row is an orphan from the previous process; without this reset a
/// crashed-mid-run transition waits for apalis's orphan re-enqueue timeout
/// (default ~300s) — a long delay next to an ex-date deadline. Scoped to the
/// two freeze-schedule job types so `Running` rows of other apalis job
/// types sharing the `Jobs` table are left for their own recovery. Runs on the
/// event-store pool because both pools address the same SQLite file.
pub(crate) async fn reset_orphaned_freeze_schedule_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE Jobs
        SET
            status = 'Pending',
            lock_at = NULL,
            lock_by = NULL
        WHERE
            job_type IN (?, ?)
            AND status = 'Running'
        ",
    )
    .bind(job_type::<ApplyFreezeTransition>())
    .bind(job_type::<AlignCorporateActionFreeze>())
    .execute(pool)
    .await?;

    let dead_alignment_query = format!(
        "
        SELECT idempotency_key
        FROM Jobs
        WHERE
            job_type = ?
            AND ({DEAD_JOB_PREDICATE})
        "
    );
    let dead_alignments: Vec<String> =
        sqlx::query_scalar(AssertSqlSafe(dead_alignment_query))
            .bind(job_type::<AlignCorporateActionFreeze>())
            .fetch_all(pool)
            .await?;
    if !dead_alignments.is_empty() {
        error!(target: "asset", jobs = ?dead_alignments,
            "Re-arming dead corporate-action freeze alignments"
        );
    }

    let rearm_query = format!(
        "
        UPDATE Jobs
        SET
            status = 'Pending',
            attempts = 0,
            lock_at = NULL,
            lock_by = NULL,
            done_at = NULL,
            last_result = NULL
        WHERE
            job_type = ?
            AND ({DEAD_JOB_PREDICATE})
        "
    );
    sqlx::query(AssertSqlSafe(rearm_query))
        .bind(job_type::<AlignCorporateActionFreeze>())
        .execute(pool)
        .await?;

    Ok(())
}

/// Re-arms orphaned and terminal schedule jobs and durably alerts once per
/// alignment key that exhausted its bounded retry budget. The alert key stays
/// stable across later restarts, so a permanently malformed revision cannot
/// page on every boot.
pub(crate) async fn reset_orphaned_freeze_schedule_jobs_and_notify(
    pool: &Pool<Sqlite>,
    apalis_pool: &apalis_sqlite::SqlitePool,
) -> Result<(), FreezeScheduleRecoveryError> {
    let dead_alignment_query = format!(
        "
        SELECT idempotency_key
        FROM Jobs
        WHERE
            job_type = ?
            AND ({DEAD_JOB_PREDICATE})
        "
    );
    let dead_alignments: Vec<String> =
        sqlx::query_scalar(AssertSqlSafe(dead_alignment_query))
            .bind(job_type::<AlignCorporateActionFreeze>())
            .fetch_all(pool)
            .await?;

    let mut notification_queue =
        JobQueue::<SendLifecycleNotification>::new(apalis_pool);
    for idempotency_key in dead_alignments {
        notification_queue
            .push_with_idempotency_key(
                SendLifecycleNotification {
                    notification:
                        LifecycleNotification::CorporateActionsSyncFailed,
                },
                format!(
                    "notify:corporate-action-alignment-dead:{idempotency_key}"
                ),
            )
            .await?;
    }

    // Enqueue every durable alert before clearing the terminal evidence. If a
    // queue write fails, the dead alignment remains discoverable on restart;
    // already-enqueued alerts deduplicate by their stable key.
    reset_orphaned_freeze_schedule_jobs(pool).await?;

    Ok(())
}

/// Deletes terminal apalis rows for concluded freeze-schedule work, mirroring
/// the mint stack's terminal-job vacuums: it bounds the `Jobs` table across
/// restarts and frees idempotency keys held by concluded windows. Terminal
/// [`ApplyFreezeTransition`] rows are removed after logging rows that died
/// without applying. Done [`AlignCorporateActionFreeze`] rows are removed too;
/// dead alignment rows are re-armed by the startup call to
/// [`reset_orphaned_freeze_schedule_jobs`] instead of vacuumed because the
/// latest projected freeze state still needs to be applied. Only terminal rows
/// are removed, so orphaned `Pending`/`Running`
/// jobs apalis will re-pick are left untouched. Runs on the event-store pool
/// because both pools address the same SQLite file.
pub(crate) async fn vacuum_terminal_freeze_schedule_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    log_dead_freeze_jobs(pool, None).await?;

    let delete_query = format!(
        "
        DELETE FROM Jobs
        WHERE
            job_type = ?
            AND (
                status = 'Done'
                OR ({DEAD_JOB_PREDICATE})
            )
        "
    );
    sqlx::query(AssertSqlSafe(delete_query))
        .bind(job_type::<ApplyFreezeTransition>())
        .execute(pool)
        .await?;

    sqlx::query("DELETE FROM Jobs WHERE job_type = ? AND status = 'Done'")
        .bind(job_type::<AlignCorporateActionFreeze>())
        .execute(pool)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use chrono::{Duration as ChronoDuration, Utc};
    use event_sorcery::StoreBuilder;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Barrier;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        AlignCorporateActionFreeze, ApplyFreezeTransition,
        CorporateActionFreezeCtx, CorporateActionFreezeScheduler,
        CorporateActionScheduleState, FreezeScheduleCtx, FreezeScheduleError,
        FreezeScheduler, FreezeTransition, RevisionReadTestHook,
        reset_orphaned_freeze_schedule_jobs,
        reset_orphaned_freeze_schedule_jobs_and_notify,
        vacuum_terminal_freeze_schedule_jobs,
    };
    use crate::jobs::{Job, job_type};
    use crate::mint::test_utils::TestHarness;
    use crate::notifications::{
        CapturingLifecycleNotifier, FreezeTransitionKind,
        LifecycleNotification, SendLifecycleNotification,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::corporate_action_feed::{
        CorporateActionMutation, CorporateActionMutationKind,
        DividendCorporateAction, apply_mutation,
    };
    use crate::tokenized_asset::{
        AssetKey, CorporateActionEventId, CorporateActionId, Network,
        TokenSymbol, TokenizedAssetCommand, UnderlyingSymbol,
    };
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

    fn corporate_action(
        event_id: &str,
        kind: CorporateActionMutationKind,
        action_id: &str,
        underlying: &UnderlyingSymbol,
        ex_date: chrono::NaiveDate,
    ) -> CorporateActionMutation {
        CorporateActionMutation {
            event_id: CorporateActionEventId::new(event_id).unwrap(),
            kind,
            action: DividendCorporateAction {
                id: CorporateActionId::new(action_id).unwrap(),
                underlying: underlying.clone(),
                ex_date,
            },
        }
    }

    async fn corporate_action_context(
        harness: &TestHarness,
    ) -> CorporateActionFreezeCtx {
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(harness.pool.clone())
                .build(())
                .await
                .unwrap();
        CorporateActionFreezeCtx {
            underlying_store,
            pool: harness.pool.clone(),
            revision_read_test_hook: None,
        }
    }

    #[tokio::test]
    async fn corporate_action_revision_schedules_immediate_and_boundary_jobs() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );
        let now = Utc::now();
        let ex_date = now.date_naive() + ChronoDuration::days(1);

        scheduler
            .schedule_revision(
                &CorporateActionId::new("ca-1").unwrap(),
                &CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2")
                    .unwrap(),
                &underlying,
                ex_date,
                CorporateActionScheduleState::Active,
                now,
            )
            .await
            .unwrap();

        let (jobs, min_attempts): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), MIN(max_attempts) FROM Jobs WHERE job_type = ?",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(jobs, 3);
        assert_eq!(
            min_attempts,
            i64::from(super::CORPORATE_ACTION_ALIGNMENT_MAX_ATTEMPTS)
        );
        let run_at: Vec<(String, i64)> = sqlx::query_as(
            "SELECT idempotency_key, run_at FROM Jobs WHERE job_type = ? ORDER BY idempotency_key",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .fetch_all(&harness.pool)
        .await
        .unwrap();
        let freeze_at = ex_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
        let unfreeze_at =
            ex_date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc();
        assert!(run_at.iter().any(|(key, scheduled_at)| {
            key.ends_with(":immediate") && *scheduled_at <= now.timestamp() + 1
        }));
        assert!(run_at.iter().any(|(key, scheduled_at)| {
            key.ends_with(":freeze") && *scheduled_at == freeze_at.timestamp()
        }));
        assert!(run_at.iter().any(|(key, scheduled_at)| {
            key.ends_with(":unfreeze")
                && *scheduled_at == unfreeze_at.timestamp()
        }));

        scheduler
            .schedule_revision(
                &CorporateActionId::new("ca-1").unwrap(),
                &CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2")
                    .unwrap(),
                &underlying,
                ex_date,
                CorporateActionScheduleState::Active,
                now,
            )
            .await
            .unwrap();

        let notification_jobs: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<SendLifecycleNotification>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(
            notification_jobs, 1,
            "re-scheduling the same revision must dedup to one \
             corporate-action-scheduled notification"
        );
    }

    #[tokio::test]
    async fn elapsed_corporate_action_revision_schedules_only_alignment() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );
        let now = Utc::now();

        scheduler
            .schedule_revision(
                &CorporateActionId::new("ca-elapsed").unwrap(),
                &CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2")
                    .unwrap(),
                &underlying,
                now.date_naive() - ChronoDuration::days(2),
                CorporateActionScheduleState::Active,
                now,
            )
            .await
            .unwrap();

        let jobs: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<AlignCorporateActionFreeze>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(jobs, 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn corporate_action_alignment_acquires_the_action_owned_hold() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mutation = corporate_action(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            &underlying,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &mutation).await.unwrap();

        AlignCorporateActionFreeze {
            action_id: mutation.action.id,
            expected_event_id: mutation.event_id,
        }
        .perform(&corporate_action_context(&harness).await)
        .await
        .unwrap();

        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Acquiring underlying freeze hold", underlying.as_str()]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn corporate_action_update_releases_the_superseded_active_window() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let ctx = corporate_action_context(&harness).await;
        let insert = corporate_action(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            &underlying,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &insert).await.unwrap();
        AlignCorporateActionFreeze {
            action_id: insert.action.id.clone(),
            expected_event_id: insert.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let update = corporate_action(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Update,
            "ca-1",
            &underlying,
            Utc::now().date_naive() + ChronoDuration::days(1),
        );
        apply_mutation(&harness.pool, &update).await.unwrap();
        AlignCorporateActionFreeze {
            action_id: update.action.id,
            expected_event_id: update.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Releasing underlying freeze hold", underlying.as_str()]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn corporate_action_update_moves_its_hold_to_the_revised_underlying()
    {
        let harness = TestHarness::new().await;
        let aapl = harness.setup_account_and_asset().await.underlying;
        let msft = UnderlyingSymbol::new("MSFT").unwrap();
        harness
            .asset_store
            .send(
                &AssetKey::new(msft.clone(), Network::Base),
                TokenizedAssetCommand::Add {
                    underlying: msft.clone(),
                    token: TokenSymbol::new("tMSFT"),
                    network: Network::Base,
                    vault: address!(
                        "0x2234567890abcdef1234567890abcdef12345678"
                    ),
                },
            )
            .await
            .unwrap();
        let ctx = corporate_action_context(&harness).await;
        let insert = corporate_action(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            &aapl,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &insert).await.unwrap();
        AlignCorporateActionFreeze {
            action_id: insert.action.id,
            expected_event_id: insert.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();
        let update = corporate_action(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Update,
            "ca-1",
            &msft,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &update).await.unwrap();

        AlignCorporateActionFreeze {
            action_id: update.action.id,
            expected_event_id: update.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            load_freeze_status(&harness.pool, &aapl).await.unwrap(),
            AssetStatus::Enabled
        );
        assert_eq!(
            load_freeze_status(&harness.pool, &msft).await.unwrap(),
            AssetStatus::Frozen
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Acquiring underlying freeze hold", msft.as_str()]
        ));
        assert!(logs_contain_at!(
            Level::INFO,
            &["Releasing underlying freeze hold", aapl.as_str()]
        ));
    }

    #[tokio::test]
    async fn revision_update_waits_for_inflight_action_alignment() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let insert = corporate_action(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            &underlying,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &insert).await.unwrap();

        let observed = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(harness.pool.clone())
                .build(())
                .await
                .unwrap();
        let ctx = Arc::new(CorporateActionFreezeCtx {
            underlying_store,
            pool: harness.pool.clone(),
            revision_read_test_hook: Some(RevisionReadTestHook {
                observed: observed.clone(),
                release: release.clone(),
            }),
        });
        let alignment = AlignCorporateActionFreeze {
            action_id: insert.action.id.clone(),
            expected_event_id: insert.event_id,
        };
        let alignment_ctx = ctx.clone();
        let alignment_task =
            tokio::spawn(
                async move { alignment.perform(&alignment_ctx).await },
            );
        observed.wait().await;

        let update = corporate_action(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Update,
            "ca-1",
            &underlying,
            Utc::now().date_naive() + ChronoDuration::days(1),
        );
        let update_pool = harness.pool.clone();
        let mut update_task =
            tokio::spawn(
                async move { apply_mutation(&update_pool, &update).await },
            );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut update_task)
                .await
                .is_err(),
            "a newer revision must not commit between the old revision check and its hold effects"
        );

        release.wait().await;
        alignment_task.await.unwrap().unwrap();
        update_task.await.unwrap().unwrap();
    }

    #[traced_test]
    #[tokio::test]
    async fn stale_corporate_action_alignment_is_a_noop() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let ctx = corporate_action_context(&harness).await;
        let insert = corporate_action(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            &underlying,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &insert).await.unwrap();
        AlignCorporateActionFreeze {
            action_id: insert.action.id.clone(),
            expected_event_id: insert.event_id.clone(),
        }
        .perform(&ctx)
        .await
        .unwrap();
        let update = corporate_action(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Update,
            "ca-1",
            &underlying,
            Utc::now().date_naive() + ChronoDuration::days(1),
        );
        apply_mutation(&harness.pool, &update).await.unwrap();
        AlignCorporateActionFreeze {
            action_id: update.action.id,
            expected_event_id: update.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        AlignCorporateActionFreeze {
            action_id: insert.action.id,
            expected_event_id: insert.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Releasing underlying freeze hold", underlying.as_str()]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn corporate_action_delete_preserves_the_operator_hold() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let ctx = corporate_action_context(&harness).await;
        ctx.underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Freeze { underlying: underlying.clone() },
            )
            .await
            .unwrap();
        let insert = corporate_action(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            &underlying,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &insert).await.unwrap();
        AlignCorporateActionFreeze {
            action_id: insert.action.id.clone(),
            expected_event_id: insert.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();
        let deletion = corporate_action(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Delete,
            "ca-1",
            &underlying,
            Utc::now().date_naive(),
        );
        apply_mutation(&harness.pool, &deletion).await.unwrap();

        AlignCorporateActionFreeze {
            action_id: deletion.action.id,
            expected_event_id: deletion.event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Frozen
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Releasing underlying freeze hold", underlying.as_str()]
        ));
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
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };

        let freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: hold_id.clone(),
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

    #[tokio::test]
    async fn perform_notifies_when_the_transition_fails() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(harness.pool.clone())
                .build(())
                .await
                .unwrap();
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let ctx = FreezeScheduleCtx {
            pool: harness.pool.clone(),
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };
        harness.pool.close().await;
        let freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: hold_id(Utc::now(), Utc::now() + ChronoDuration::hours(1)),
            transition: FreezeTransition::Freeze,
            scheduled_for: Utc::now(),
        };

        freeze.perform(&ctx).await.unwrap_err();

        assert_eq!(
            notifier.notifications(),
            vec![LifecycleNotification::FreezeTransitionFailed {
                underlying,
                transition: FreezeTransitionKind::Freeze,
            }]
        );
    }

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
            hold_id: first_hold.clone(),
            transition: FreezeTransition::Freeze,
            scheduled_for: now + ChronoDuration::hours(1),
        };
        let second_freeze = ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: second_hold.clone(),
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
        assert!(notifier.notifications().is_empty());
    }

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
            underlying_store,
            notifier: notifier.clone(),
            before_dispatch_barriers: None,
        };
        let freeze_at = Utc::now();
        let unfreeze_at = freeze_at + ChronoDuration::hours(1);
        let hold_id = hold_id(freeze_at, unfreeze_at);

        ApplyFreezeTransition {
            underlying: underlying.clone(),
            hold_id: hold_id.clone(),
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

        let window_jobs: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<ApplyFreezeTransition>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(
            window_jobs, 2,
            "re-arming the same window must dedup to one freeze and one \
             unfreeze job"
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

    #[tokio::test]
    async fn startup_reset_and_vacuum_cover_corporate_action_alignments() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );
        let now = Utc::now();
        scheduler
            .schedule_revision(
                &CorporateActionId::new("ca-1").unwrap(),
                &CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2")
                    .unwrap(),
                &underlying,
                now.date_naive() + ChronoDuration::days(1),
                CorporateActionScheduleState::Active,
                now,
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES ('dead-corporate-action-worker', ?, 'SqliteStorage')",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_at = strftime('%s', 'now'), lock_by = 'dead-corporate-action-worker' WHERE job_type = ?",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .execute(&harness.pool)
        .await
        .unwrap();

        reset_orphaned_freeze_schedule_jobs(&harness.pool).await.unwrap();

        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending' AND lock_at IS NULL AND lock_by IS NULL",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(pending, 3);

        sqlx::query(
            "UPDATE Jobs SET status = 'Failed', attempts = max_attempts WHERE job_type = ?",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .execute(&harness.pool)
        .await
        .unwrap();
        reset_orphaned_freeze_schedule_jobs_and_notify(
            &harness.pool,
            &harness.apalis_pool,
        )
        .await
        .unwrap();
        let rearmed_failed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending' AND attempts = 0",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(rearmed_failed, 3);
        let alignment_alerts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<SendLifecycleNotification>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(alignment_alerts, 4);

        sqlx::query("UPDATE Jobs SET status = 'Killed' WHERE job_type = ?")
            .bind(job_type::<AlignCorporateActionFreeze>())
            .execute(&harness.pool)
            .await
            .unwrap();
        reset_orphaned_freeze_schedule_jobs_and_notify(
            &harness.pool,
            &harness.apalis_pool,
        )
        .await
        .unwrap();
        let rearmed_killed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending' AND attempts = 0",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(rearmed_killed, 3);
        let deduplicated_alignment_alerts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<SendLifecycleNotification>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(deduplicated_alignment_alerts, 4);

        sqlx::query("UPDATE Jobs SET status = 'Done' WHERE job_type = ?")
            .bind(job_type::<AlignCorporateActionFreeze>())
            .execute(&harness.pool)
            .await
            .unwrap();
        vacuum_terminal_freeze_schedule_jobs(&harness.pool).await.unwrap();

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<AlignCorporateActionFreeze>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(remaining, 0);
    }

    #[tokio::test]
    async fn startup_recovery_preserves_dead_alignment_until_alert_is_durable()
    {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );
        let now = Utc::now();
        scheduler
            .schedule_revision(
                &CorporateActionId::new("ca-alert-failure").unwrap(),
                &CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2")
                    .unwrap(),
                &underlying,
                now.date_naive() + ChronoDuration::days(1),
                CorporateActionScheduleState::Active,
                now,
            )
            .await
            .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Failed', attempts = max_attempts WHERE job_type = ?",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .execute(&harness.pool)
        .await
        .unwrap();
        harness.apalis_pool.close().await;

        assert!(
            reset_orphaned_freeze_schedule_jobs_and_notify(
                &harness.pool,
                &harness.apalis_pool,
            )
            .await
            .is_err(),
            "startup must fail when the durable alert cannot be enqueued"
        );
        let still_dead: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Failed' AND attempts = max_attempts",
        )
        .bind(job_type::<AlignCorporateActionFreeze>())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            still_dead, 3,
            "terminal evidence must remain available for the next restart"
        );
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
