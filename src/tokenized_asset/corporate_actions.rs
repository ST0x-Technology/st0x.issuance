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
use tracing::{debug, warn};

use super::UnderlyingSymbol;
use super::schedule::{FreezeScheduleError, FreezeScheduler};
use super::view::{TokenizedAssetViewError, list_enabled_assets};
use crate::alpaca::{AlpacaError, AlpacaService, DividendAnnouncement};

/// How far ahead each sync pass looks for ex-dates. Alpaca caps the
/// announcements date range at 90 days; staying one day inside the cap avoids
/// tripping an inclusive-bounds rejection.
const ANNOUNCEMENT_HORIZON_DAYS: u64 = 89;

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
        let supported: HashSet<UnderlyingSymbol> =
            list_enabled_assets(&self.pool)
                .await?
                .into_iter()
                .map(|asset| asset.underlying)
                .collect();

        let since = now.date_naive();
        let until = since
            .checked_add_days(Days::new(ANNOUNCEMENT_HORIZON_DAYS))
            .unwrap_or(since);

        let announcements =
            self.alpaca.list_dividend_announcements(since, until).await?;

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

            // `ex_date_window` only fails when `checked_add_days` overflows —
            // the announcement IS dated, so counting it as undated would lie
            // in the pass summary. Log and move on instead.
            let Some((freeze_at, unfreeze_at)) = ex_date_window(ex_date) else {
                warn!(target: "tokenized_asset",
                    underlying = %initiating_symbol,
                    %ex_date,
                    "Failed to compute freeze window for ex-date"
                );
                continue;
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
                Err(error) => return Err(error.into()),
            }
        }

        Ok(summary)
    }
}

/// Spawns the periodic corporate-actions sync loop. A failed pass is logged
/// and retried at the next interval; the scheduler's idempotency keys make
/// repeated passes over the same announcements no-ops.
pub(crate) fn spawn_corporate_actions_sync(mut sync: CorporateActionsSync) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(CORPORATE_ACTIONS_SYNC_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;

            match sync.sync_once(Utc::now()).await {
                Ok(summary) => {
                    debug!(target: "tokenized_asset",
                        armed = summary.armed.len(),
                        skipped_unsupported = summary.skipped_unsupported,
                        skipped_undated = summary.skipped_undated,
                        skipped_elapsed = summary.skipped_elapsed,
                        "Corporate-actions freeze sync pass complete"
                    );
                }
                Err(error) => {
                    debug!(target: "tokenized_asset",
                        error = %error,
                        "Corporate-actions freeze sync pass failed; retrying \
                         at the next interval"
                    );
                }
            }
        }
    });
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

    use super::{CorporateActionsSync, ex_date_window};
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
        assert_eq!(second.armed, vec![(underlying, ex_date)]);
    }

    // A fetch failure aborts the pass with the Alpaca error so the worker
    // loop logs it and retries next interval.
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
    }
}
