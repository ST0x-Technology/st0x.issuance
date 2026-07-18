//! Sources the freeze schedule from Alpaca dividend corporate actions.
//!
//! A periodic sync pass lists upcoming dividend announcements (ex-dates) from
//! Alpaca's Corporate Actions Announcements API and arms a freeze window per
//! supported asset per ex-date through [`FreezeScheduler`] — the same
//! scheduler the manual `POST /admin/freeze-schedules` endpoint drives. The
//! scheduler's idempotency keys (underlying + both window boundaries) make the
//! pass safe to repeat: every sync re-arms the same windows as no-ops until
//! the announcement leaves the horizon.
//!
//! Window policy: the full UTC ex-date day — freeze at ex-date 00:00 UTC,
//! unfreeze at 00:00 UTC the following day. Both instants are conservative
//! with respect to US/Eastern trading hours: the freeze lands the evening
//! before the ex-date session opens, and the unfreeze lands after that
//! session closes. The issuer CLI owns an independent operator hold, so manual
//! unfreeze cannot release a corporate-action window.

use chrono::{DateTime, Days, NaiveDate, NaiveTime, Utc};
use sqlx::{Pool, Sqlite};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tracing::debug;

use super::UnderlyingSymbol;
use super::schedule::{FreezeScheduleError, FreezeScheduler};
use super::view::{TokenizedAssetViewError, list_enabled_assets};
use crate::alpaca::{AlpacaError, AlpacaService, DividendAnnouncement};

/// How far ahead each sync pass looks for ex-dates. Alpaca documents both
/// bounds as inclusive and caps the range at 90 days, so an 88-day offset
/// keeps the inclusive range one day below the cap:
/// <https://docs.alpaca.markets/us/v1.4.2/reference/getcorporateannouncements>
const ANNOUNCEMENT_HORIZON_DAYS: u64 = 88;

/// Interval between sync passes. Announcements surface in Alpaca's API around
/// the day after declaration, and declaration precedes the ex-date by days to
/// weeks, so a few passes per day is ample slack; each pass is one API call.
const CORPORATE_ACTIONS_SYNC_INTERVAL: Duration =
    Duration::from_secs(6 * 60 * 60);

/// Periodic sync arming freeze windows from Alpaca dividend announcements.
pub(crate) struct CorporateActionsSync {
    alpaca: Arc<dyn AlpacaService>,
    scheduler: FreezeScheduler,
    pool: Pool<Sqlite>,
}

/// What one sync pass did, for the caller to log or assert on.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SyncSummary {
    /// Windows armed (or idempotently re-armed) this pass.
    pub(crate) armed: Vec<(UnderlyingSymbol, NaiveDate)>,
    /// Announcements for symbols we do not tokenize.
    pub(crate) skipped_unsupported: usize,
    /// Announcements whose ex-date is not set yet upstream.
    pub(crate) skipped_undated: usize,
    /// Announcements whose whole window already elapsed.
    pub(crate) skipped_elapsed: usize,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionsSyncError {
    #[error("the ex-date {ex_date} cannot form a full UTC-day freeze window")]
    ExDateWindowOverflow { ex_date: NaiveDate },
    #[error(
        "adding the {days}-day announcement horizon to {since} exceeds the \
         representable date range"
    )]
    AnnouncementHorizonOverflow { since: NaiveDate, days: u64 },
    #[error(transparent)]
    Alpaca(#[from] AlpacaError),
    #[error(transparent)]
    View(#[from] TokenizedAssetViewError),
    #[error(transparent)]
    Schedule(#[from] FreezeScheduleError),
}

impl CorporateActionsSync {
    pub(crate) fn new(
        alpaca: Arc<dyn AlpacaService>,
        scheduler: FreezeScheduler,
        pool: Pool<Sqlite>,
    ) -> Self {
        Self { alpaca, scheduler, pool }
    }

    /// Runs one sync pass: fetch upcoming dividend ex-dates and arm a freeze
    /// window for each supported asset.
    ///
    /// Skips (rather than fails on) announcements that are undated, for
    /// unsupported symbols, or already fully elapsed; any other error aborts
    /// the pass so the next interval retries it whole — the scheduler's
    /// idempotency keys make the partial progress of an aborted pass safe.
    pub(crate) async fn sync_once(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<SyncSummary, CorporateActionsSyncError> {
        let log_failure = |error: &CorporateActionsSyncError| {
            debug!(target: "tokenized_asset",
                error = %error,
                "Corporate-actions freeze sync pass failed; retrying at the next interval"
            );
        };
        let supported: HashSet<UnderlyingSymbol> =
            list_enabled_assets(&self.pool)
                .await
                .map_err(CorporateActionsSyncError::from)
                .inspect_err(&log_failure)?
                .into_iter()
                .map(|asset| asset.underlying)
                .collect();

        let since = now.date_naive();
        let until = since
            .checked_add_days(Days::new(ANNOUNCEMENT_HORIZON_DAYS))
            .ok_or(CorporateActionsSyncError::AnnouncementHorizonOverflow {
                since,
                days: ANNOUNCEMENT_HORIZON_DAYS,
            })
            .inspect_err(&log_failure)?;

        let announcements = self
            .alpaca
            .list_dividend_announcements(since, until)
            .await
            .map_err(CorporateActionsSyncError::from)
            .inspect_err(&log_failure)?;

        let mut summary = SyncSummary::default();

        for announcement in announcements {
            let DividendAnnouncement { initiating_symbol, ex_date } =
                announcement;

            let Some(ex_date) = ex_date else {
                debug!(target: "tokenized_asset", underlying = %initiating_symbol,
                    "Skipping dividend announcement with no ex-date"
                );
                summary.skipped_undated += 1;
                continue;
            };

            if !supported.contains(&initiating_symbol) {
                debug!(target: "tokenized_asset", underlying = %initiating_symbol,
                    %ex_date,
                    "Skipping dividend announcement for unsupported symbol"
                );
                summary.skipped_unsupported += 1;
                continue;
            }

            let Some((freeze_at, unfreeze_at)) = ex_date_window(ex_date) else {
                let error =
                    CorporateActionsSyncError::ExDateWindowOverflow { ex_date };
                log_failure(&error);
                return Err(error);
            };

            match self
                .scheduler
                .schedule_window(
                    &initiating_symbol,
                    freeze_at,
                    unfreeze_at,
                    now,
                )
                .await
            {
                Ok(()) => {
                    debug!(target: "tokenized_asset",
                        underlying = %initiating_symbol,
                        %ex_date,
                        %freeze_at,
                        %unfreeze_at,
                        "Armed dividend freeze window"
                    );
                    summary.armed.push((initiating_symbol, ex_date));
                }
                Err(FreezeScheduleError::ElapsedWindow { .. }) => {
                    debug!(target: "tokenized_asset",
                        underlying = %initiating_symbol,
                        %ex_date,
                        "Skipping dividend announcement whose freeze window \
                         already elapsed"
                    );
                    summary.skipped_elapsed += 1;
                }
                Err(error) => {
                    let error = error.into();
                    log_failure(&error);
                    return Err(error);
                }
            }
        }

        debug!(target: "tokenized_asset",
            armed = summary.armed.len(),
            skipped_unsupported = summary.skipped_unsupported,
            skipped_undated = summary.skipped_undated,
            skipped_elapsed = summary.skipped_elapsed,
            "Corporate-actions freeze sync pass complete"
        );
        Ok(summary)
    }
}

/// Spawns the periodic corporate-actions sync loop. A failed pass is logged
/// and retried at the next interval; the scheduler's idempotency keys make
/// repeated passes over the same announcements no-ops.
pub(crate) fn spawn_corporate_actions_sync(
    mut sync: CorporateActionsSync,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(CORPORATE_ACTIONS_SYNC_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = ticker.tick() => {
                    let _sync_result = sync.sync_once(Utc::now()).await;
                }
            }
        }
        debug!(target: "tokenized_asset", reason = "shutdown_signal",
            "Corporate-actions sync loop stopped"
        );
    })
}

/// The freeze window for an ex-date: the full UTC calendar day (see the
/// module docs for why that brackets the US/Eastern session). `None` only if
/// the day after `ex_date` is unrepresentable.
fn ex_date_window(
    ex_date: NaiveDate,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let freeze_at = ex_date.and_time(NaiveTime::MIN).and_utc();
    let unfreeze_at = ex_date
        .checked_add_days(Days::new(1))?
        .and_time(NaiveTime::MIN)
        .and_utc();

    Some((freeze_at, unfreeze_at))
}

#[cfg(test)]
mod tests {
    use chrono::{Days, NaiveDate, Utc};
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        CorporateActionsSync, ex_date_window, spawn_corporate_actions_sync,
    };
    use crate::alpaca::{DividendAnnouncement, mock::MockAlpacaService};
    use crate::mint::test_utils::TestHarness;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::UnderlyingSymbol;
    use crate::tokenized_asset::schedule::FreezeScheduler;

    #[test]
    fn ex_date_window_brackets_the_utc_day() {
        let ex_date = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();

        let (freeze_at, unfreeze_at) = ex_date_window(ex_date).unwrap();

        assert_eq!(freeze_at.to_rfc3339(), "2026-08-14T00:00:00+00:00");
        assert_eq!(unfreeze_at.to_rfc3339(), "2026-08-15T00:00:00+00:00");
    }

    #[test]
    fn announcement_horizon_stays_one_day_below_alpaca_limit() {
        let since = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        let until = since
            .checked_add_days(Days::new(super::ANNOUNCEMENT_HORIZON_DAYS))
            .unwrap();

        assert_eq!((until - since).num_days() + 1, 89);
    }

    // One pass over a mixed announcement set: a supported dated announcement
    // arms a window; unsupported, undated, and fully-elapsed announcements
    // are skipped without failing the pass.
    #[traced_test]
    #[tokio::test]
    async fn sync_once_arms_supported_and_skips_the_rest() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let scheduler =
            FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone());
        let now = Utc::now();
        let upcoming_ex_date =
            now.date_naive().checked_add_days(Days::new(7)).unwrap();
        let elapsed_ex_date =
            now.date_naive().checked_sub_days(Days::new(7)).unwrap();

        let alpaca = Arc::new(
            MockAlpacaService::new_success().with_announcements(vec![
                DividendAnnouncement {
                    initiating_symbol: underlying.clone(),
                    ex_date: Some(upcoming_ex_date),
                },
                DividendAnnouncement {
                    initiating_symbol: UnderlyingSymbol::new("UNSUPP").unwrap(),
                    ex_date: Some(upcoming_ex_date),
                },
                DividendAnnouncement {
                    initiating_symbol: underlying.clone(),
                    ex_date: None,
                },
                DividendAnnouncement {
                    initiating_symbol: underlying.clone(),
                    ex_date: Some(elapsed_ex_date),
                },
            ]),
        );

        let mut sync =
            CorporateActionsSync::new(alpaca, scheduler, harness.pool);

        let summary = sync.sync_once(now).await.unwrap();

        assert_eq!(summary.armed, vec![(underlying, upcoming_ex_date)]);
        assert_eq!(summary.skipped_unsupported, 1);
        assert_eq!(summary.skipped_undated, 1);
        assert_eq!(summary.skipped_elapsed, 1);
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Armed dividend freeze window", "AAPL"]
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["unsupported symbol", "UNSUPP"]
        ));
        assert!(logs_contain_at!(Level::DEBUG, &["no ex-date", "AAPL"]));
        assert!(logs_contain_at!(Level::DEBUG, &["already elapsed", "AAPL"]));
    }

    // Re-running the identical pass must not error: the scheduler's
    // idempotency keys collapse the duplicate window enqueues.
    #[traced_test]
    #[tokio::test]
    async fn sync_once_is_idempotent_across_passes() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let scheduler =
            FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone());
        let now = Utc::now();
        let ex_date = now.date_naive().checked_add_days(Days::new(7)).unwrap();

        let alpaca =
            Arc::new(MockAlpacaService::new_success().with_announcements(
                vec![DividendAnnouncement {
                    initiating_symbol: underlying.clone(),
                    ex_date: Some(ex_date),
                }],
            ));

        let mut sync =
            CorporateActionsSync::new(alpaca, scheduler, harness.pool);

        let first = sync.sync_once(now).await.unwrap();
        let second = sync.sync_once(now).await.unwrap();

        assert_eq!(first.armed, vec![(underlying.clone(), ex_date)]);
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Armed dividend freeze window", underlying.as_str()]
        ));
        assert_eq!(second.armed, vec![(underlying, ex_date)]);
    }

    #[traced_test]
    #[tokio::test]
    async fn sync_once_rejects_an_unrepresentable_announcement_horizon() {
        let harness = TestHarness::new().await;
        let scheduler =
            FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone());
        let alpaca = Arc::new(MockAlpacaService::new_success());
        let mut sync =
            CorporateActionsSync::new(alpaca, scheduler, harness.pool);
        let now = NaiveDate::MAX.and_hms_opt(0, 0, 0).unwrap().and_utc();

        assert!(matches!(
            sync.sync_once(now).await.unwrap_err(),
            super::CorporateActionsSyncError::AnnouncementHorizonOverflow {
                since: NaiveDate::MAX,
                days: super::ANNOUNCEMENT_HORIZON_DAYS,
            }
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "sync_once_rejects_an_unrepresentable_announcement_horizon",
                "Corporate-actions freeze sync pass failed",
                "exceeds the representable date range"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn sync_once_rejects_an_unrepresentable_ex_date_window() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let scheduler =
            FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone());
        let alpaca =
            Arc::new(MockAlpacaService::new_success().with_announcements(
                vec![DividendAnnouncement {
                    initiating_symbol: underlying,
                    ex_date: Some(NaiveDate::MAX),
                }],
            ));
        let mut sync =
            CorporateActionsSync::new(alpaca, scheduler, harness.pool);

        assert!(matches!(
            sync.sync_once(Utc::now()).await.unwrap_err(),
            super::CorporateActionsSyncError::ExDateWindowOverflow {
                ex_date: NaiveDate::MAX
            }
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "sync_once_rejects_an_unrepresentable_ex_date_window",
                "Corporate-actions freeze sync pass failed",
                "cannot form a full UTC-day freeze window"
            ]
        ));
    }

    // A fetch failure aborts the pass with the Alpaca error so the worker
    // loop logs it and retries next interval.
    #[traced_test]
    #[tokio::test]
    async fn sync_once_propagates_fetch_failures() {
        let harness = TestHarness::new().await;
        harness.setup_account_and_asset().await;
        let scheduler =
            FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone());

        let alpaca = Arc::new(MockAlpacaService::new_failure("api down"));

        let mut sync =
            CorporateActionsSync::new(alpaca, scheduler, harness.pool);

        let error = sync.sync_once(Utc::now()).await.unwrap_err();

        assert!(matches!(error, super::CorporateActionsSyncError::Alpaca(_)));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "sync_once_propagates_fetch_failures",
                "Corporate-actions freeze sync pass failed",
                "api down"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn sync_loop_stops_on_shutdown() {
        let harness = TestHarness::new().await;
        let scheduler =
            FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone());
        let sync = CorporateActionsSync::new(
            Arc::new(MockAlpacaService::new_success()),
            scheduler,
            harness.pool,
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let sync_task = spawn_corporate_actions_sync(sync, shutdown_rx);

        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(std::time::Duration::from_millis(100), sync_task)
            .await
            .expect("shutdown must stop the corporate-actions sync loop")
            .unwrap();
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "sync_loop_stops_on_shutdown",
                "Corporate-actions sync loop stopped",
                "reason=\"shutdown_signal\""
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn sync_once_logs_when_a_pass_fails() {
        let harness = TestHarness::new().await;
        let scheduler =
            FreezeScheduler::new(&harness.apalis_pool, harness.pool.clone());
        let alpaca = Arc::new(MockAlpacaService::new_failure("api down"));
        let mut sync =
            CorporateActionsSync::new(alpaca, scheduler, harness.pool);

        let result = sync.sync_once(Utc::now()).await;

        assert!(result.is_err());
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Corporate-actions freeze sync pass failed"]
        ));
    }
}
