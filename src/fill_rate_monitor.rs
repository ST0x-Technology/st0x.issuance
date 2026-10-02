//! Alerts when the issuer stops taking fills.
//!
//! A fill is a request the issuer accepted: a `Mint` `Initiated` event or a
//! `Redemption` `Detected` event. Intake is what stops when Alpaca can no
//! longer reach the issuer or the transfer poller is silently stuck, and
//! nothing else alerts on that, so the [`FillRateMonitor`] counts fills in a
//! trailing window straight from the event store and alerts when the count is
//! below the configured average.
//!
//! Fills follow the US extended-hours trading day, so the monitor only speaks
//! while the [`session`] is open and only once the session has been open for
//! the whole window. Outside that it leaves the alert state alone.
//!
//! Spam control mirrors the gas monitor: alert once when the count drops below
//! the minimum, again at most once per [`FILL_RATE_REALERT_INTERVAL`] while it
//! stays low, and only log recovery. A failed alert delivery or count query
//! leaves the alert state unchanged, so the former retries on the next poll and
//! the latter neither fires nor clears an alert.

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{Pool, Sqlite};
use std::num::TryFromIntError;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::config::FillRateAlertConfig;
use crate::notifications::{LifecycleNotification, LifecycleNotifier};

mod session;

use session::SessionState;
pub(crate) use session::{CalendarError, NyseCalendar, SESSION_HOURS};

/// Interval between fill counts. The window is hours long, so a shortfall is
/// worth noticing within minutes but never needs sub-minute resolution.
pub(crate) const FILL_RATE_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// Minimum time between repeated alerts while the rate stays below the minimum.
const FILL_RATE_REALERT_INTERVAL: Duration = Duration::from_secs(3600);

/// The fill rate monitor loop.
pub(crate) struct FillRateMonitor {
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) alert: FillRateAlertConfig,
    pub(crate) calendar: NyseCalendar,
    pub(crate) poll_interval: Duration,
    pub(crate) notifier: Arc<dyn LifecycleNotifier>,
}

impl FillRateMonitor {
    /// Runs the polling loop forever. Never returns; the spawn site pairs it
    /// with the shutdown channel in a `select!`, like the gas monitor.
    pub(crate) async fn run(&self) {
        debug!(
            target: "fill_rate",
            min_fills_per_hour = self.alert.min_fills_per_hour,
            window_hours = self.alert.window_hours,
            "Starting fill rate monitor"
        );

        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut last_alerted = None;
        loop {
            interval.tick().await;
            last_alerted =
                self.poll_once(last_alerted, Utc::now(), Instant::now()).await;
        }
    }

    /// Counts the window's fills, acts on the outcome, and returns the next
    /// dedup state: when the last alert went out, or `None` while healthy.
    async fn poll_once(
        &self,
        last_alerted: Option<Instant>,
        now: DateTime<Utc>,
        instant: Instant,
    ) -> Option<Instant> {
        let window_start = match self
            .calendar
            .session_state(now, self.alert.window_hours)
        {
            Ok(SessionState::WindowFull { window_start }) => window_start,
            Ok(SessionState::Closed) => {
                debug!(
                    target: "fill_rate",
                    "Outside the session; fill rate not evaluated"
                );
                return last_alerted;
            }
            Ok(SessionState::WarmingUp) => {
                debug!(
                    target: "fill_rate",
                    window_hours = self.alert.window_hours,
                    "The session window is not yet full; fill rate not \
                     evaluated"
                );
                return last_alerted;
            }
            Err(session_error) => {
                warn!(
                    target: "fill_rate",
                    error = %session_error,
                    "Cannot place this poll in the session, so the fill rate \
                     is not evaluated; check the holiday list"
                );
                return last_alerted;
            }
        };

        let required_fills = u64::from(self.alert.min_fills_per_hour.get())
            * u64::from(self.alert.window_hours.get());

        let fills = match count_fills(&self.pool, window_start).await {
            Ok(fills) => fills,
            Err(count_error) => {
                warn!(
                    target: "fill_rate",
                    error = %count_error,
                    "Failed to count fills in the trailing window"
                );
                return last_alerted;
            }
        };

        debug!(
            target: "fill_rate",
            fills,
            required_fills,
            window_hours = self.alert.window_hours,
            "Counted fills in the trailing window"
        );

        match evaluate(last_alerted, fills < required_fills, instant) {
            PollOutcome::StillHealthy => None,
            PollOutcome::Suppressed => last_alerted,
            PollOutcome::Recovered => {
                info!(
                    target: "fill_rate",
                    fills,
                    required_fills,
                    window_hours = self.alert.window_hours,
                    "Fill rate recovered to the configured minimum"
                );
                None
            }
            PollOutcome::Alert => {
                error!(
                    target: "fill_rate",
                    fills,
                    required_fills,
                    window_hours = self.alert.window_hours,
                    "Fill rate is below the configured minimum"
                );
                self.deliver_alert(fills, required_fills, last_alerted, instant)
                    .await
            }
        }
    }

    /// Delivers the alert and returns the next dedup state. A failed delivery
    /// keeps the prior state: advancing it would suppress every retry for a
    /// full [`FILL_RATE_REALERT_INTERVAL`] and leave the operator unpaged.
    async fn deliver_alert(
        &self,
        fills: u64,
        required_fills: u64,
        last_alerted: Option<Instant>,
        instant: Instant,
    ) -> Option<Instant> {
        let delivery = self
            .notifier
            .deliver(&LifecycleNotification::LowFillRate {
                fills,
                required_fills,
                window_hours: self.alert.window_hours,
            })
            .await;

        match delivery {
            Ok(()) => Some(instant),
            Err(delivery_error) => {
                warn!(
                    target: "fill_rate",
                    error = %delivery_error,
                    "Low fill rate alert delivery failed; retrying on the \
                     next poll"
                );
                last_alerted
            }
        }
    }
}

/// What one poll observed relative to the previous alert state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    StillHealthy,
    Alert,
    Suppressed,
    Recovered,
}

/// Pure alert transition: `last_alerted` is when the current shortfall last
/// alerted, `None` while the rate was healthy at the previous poll.
fn evaluate(
    last_alerted: Option<Instant>,
    is_low: bool,
    now: Instant,
) -> PollOutcome {
    match (last_alerted, is_low) {
        (None, false) => PollOutcome::StillHealthy,
        (Some(_), false) => PollOutcome::Recovered,
        (None, true) => PollOutcome::Alert,
        (Some(alerted_at), true) => {
            if now.saturating_duration_since(alerted_at)
                >= FILL_RATE_REALERT_INTERVAL
            {
                PollOutcome::Alert
            } else {
                PollOutcome::Suppressed
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CountFillsError {
    #[error("database error")]
    Database(#[from] sqlx::Error),
    #[error("fill count is out of range")]
    Count(#[from] TryFromIntError),
}

/// Counts accepted mint and redemption requests whose event timestamp is at or
/// after `since`.
///
/// Timestamps are compared as Julian days because the persisted RFC 3339
/// strings vary in fractional digits, which makes a text comparison wrong.
async fn count_fills(
    pool: &Pool<Sqlite>,
    since: DateTime<Utc>,
) -> Result<u64, CountFillsError> {
    let since = since.to_rfc3339_opts(SecondsFormat::Micros, true);

    let fills: i64 = sqlx::query_scalar(
        "
        SELECT COUNT(*)
        FROM events
        WHERE (
            aggregate_type = 'Mint'
            AND event_type = 'MintEvent::Initiated'
            AND julianday(json_extract(payload, '$.Initiated.initiated_at'))
                >= julianday(?)
        ) OR (
            aggregate_type = 'Redemption'
            AND event_type = 'RedemptionEvent::Detected'
            AND julianday(json_extract(payload, '$.Detected.detected_at'))
                >= julianday(?)
        )
        ",
    )
    .bind(&since)
    .bind(&since)
    .fetch_one(pool)
    .await?;

    Ok(u64::try_from(fills)?)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;
    use async_trait::async_trait;
    use chrono::{DateTime, TimeDelta, TimeZone, Utc};
    use chrono_tz::America::New_York;
    use cqrs_es::DomainEvent;
    use parking_lot::Mutex;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::session::NyseCalendar;
    use super::{
        FILL_RATE_REALERT_INTERVAL, FillRateMonitor, PollOutcome, count_fills,
        evaluate,
    };
    use crate::config::{FillRateAlertConfig, VaultMode};
    use crate::mint::{
        ClientId, IssuerMintRequestId, MintEvent, Quantity, TokenSymbol,
        TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::notifications::{
        LifecycleNotification, LifecycleNotificationError, LifecycleNotifier,
    };
    use crate::redemption::{IssuerRedemptionRequestId, RedemptionEvent};
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::Network;

    /// Notifier that fails its first `fail_first` deliveries, then succeeds,
    /// recording only the deliveries that succeed.
    struct FlakyNotifier {
        fail_remaining: Mutex<usize>,
        delivered: Mutex<Vec<LifecycleNotification>>,
    }

    impl FlakyNotifier {
        fn new(fail_first: usize) -> Arc<Self> {
            Arc::new(Self {
                fail_remaining: Mutex::new(fail_first),
                delivered: Mutex::new(Vec::new()),
            })
        }

        fn delivered(&self) -> Vec<LifecycleNotification> {
            self.delivered.lock().clone()
        }
    }

    #[async_trait]
    impl LifecycleNotifier for FlakyNotifier {
        async fn deliver(
            &self,
            notification: &LifecycleNotification,
        ) -> Result<(), LifecycleNotificationError> {
            let should_fail = {
                let mut remaining = self.fail_remaining.lock();
                let fail = *remaining > 0;
                if fail {
                    *remaining -= 1;
                }
                fail
            };
            if should_fail {
                return Err(LifecycleNotificationError::new(
                    std::io::Error::other("telegram unavailable"),
                ));
            }
            self.delivered.lock().push(notification.clone());
            Ok(())
        }
    }

    fn three_per_hour_over_six() -> FillRateAlertConfig {
        FillRateAlertConfig {
            min_fills_per_hour: NonZeroU32::new(3).unwrap(),
            window_hours: NonZeroU32::new(6).unwrap(),
        }
    }

    async fn migrated_pool() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn monitor(
        pool: Pool<Sqlite>,
        notifier: Arc<dyn LifecycleNotifier>,
    ) -> FillRateMonitor {
        FillRateMonitor {
            pool,
            alert: three_per_hour_over_six(),
            calendar: NyseCalendar::embedded().unwrap(),
            poll_interval: Duration::from_secs(300),
            notifier,
        }
    }

    /// New York wall-clock time as the instant it names.
    fn new_york(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
    ) -> DateTime<Utc> {
        New_York
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc)
    }

    /// A Monday, 11:00 New York time: the session has been open for seven
    /// hours, so the six hour window is full and starts at 05:00.
    fn full_window_now() -> DateTime<Utc> {
        new_york(2026, 10, 5, 11, 0)
    }

    async fn insert_event(
        pool: &Pool<Sqlite>,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
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
            VALUES (?, ?, 1, ?, '1.0', ?, '{}')
            ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_mint_initiated(pool: &Pool<Sqlite>, at: DateTime<Utc>) {
        let issuer_request_id = IssuerMintRequestId::random();
        let event = MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("alp-fill"),
            quantity: Quantity::default(),
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: alloy::primitives::Address::ZERO,
            initiated_at: at,
            mint_mode: VaultMode::VaultDirect,
        };

        insert_event(
            pool,
            "Mint",
            &issuer_request_id.to_string(),
            &event.event_type(),
            &serde_json::to_string(&event).unwrap(),
        )
        .await;
    }

    async fn insert_redemption_detected(
        pool: &Pool<Sqlite>,
        at: DateTime<Utc>,
    ) {
        let issuer_request_id = IssuerRedemptionRequestId::new(B256::random());
        let event = RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            wallet: alloy::primitives::Address::ZERO,
            quantity: Quantity::default(),
            tx_hash: B256::random(),
            block_number: 1,
            detected_at: at,
            burn_mode: VaultMode::VaultDirect,
        };

        insert_event(
            pool,
            "Redemption",
            &issuer_request_id.to_string(),
            &event.event_type(),
            &serde_json::to_string(&event).unwrap(),
        )
        .await;
    }

    async fn seed_mints(pool: &Pool<Sqlite>, count: usize, at: DateTime<Utc>) {
        for _ in 0..count {
            insert_mint_initiated(pool, at).await;
        }
    }

    #[test]
    fn healthy_rate_stays_healthy() {
        assert_eq!(
            evaluate(None, false, Instant::now()),
            PollOutcome::StillHealthy
        );
    }

    #[test]
    fn shortfall_alerts_once_then_suppresses() {
        let now = Instant::now();

        assert_eq!(evaluate(None, true, now), PollOutcome::Alert);
        assert_eq!(
            evaluate(Some(now), true, now + Duration::from_secs(300)),
            PollOutcome::Suppressed
        );
    }

    #[test]
    fn sustained_shortfall_realerts_after_the_interval() {
        let alerted_at = Instant::now();

        assert_eq!(
            evaluate(
                Some(alerted_at),
                true,
                alerted_at + FILL_RATE_REALERT_INTERVAL
            ),
            PollOutcome::Alert
        );
    }

    #[test]
    fn recovery_clears_the_dedup_state() {
        let alerted_at = Instant::now();

        assert_eq!(
            evaluate(Some(alerted_at), false, alerted_at),
            PollOutcome::Recovered
        );
        assert_eq!(
            evaluate(None, true, alerted_at + Duration::from_secs(60)),
            PollOutcome::Alert,
            "a shortfall after recovery must page immediately"
        );
    }

    #[tokio::test]
    async fn counts_mints_and_redemptions_inside_the_window_only() {
        let pool = migrated_pool().await;
        let now = Utc::now();
        let inside = now - TimeDelta::hours(5);
        let outside = now - TimeDelta::hours(7);

        insert_mint_initiated(&pool, inside).await;
        insert_mint_initiated(&pool, outside).await;
        insert_redemption_detected(&pool, inside).await;
        insert_redemption_detected(&pool, outside).await;

        let fills =
            count_fills(&pool, now - TimeDelta::hours(6)).await.unwrap();

        assert_eq!(fills, 2, "one mint and one redemption are in the window");
    }

    #[tokio::test]
    async fn counts_only_intake_events() {
        let pool = migrated_pool().await;
        let now = Utc::now();

        // A later event of the same aggregate carries its own timestamp field
        // but is not a new fill.
        insert_event(
            &pool,
            "Mint",
            "mint-later",
            "MintEvent::JournalConfirmed",
            &format!(
                r#"{{"JournalConfirmed":{{"confirmed_at":"{}"}}}}"#,
                now.to_rfc3339()
            ),
        )
        .await;

        let fills =
            count_fills(&pool, now - TimeDelta::hours(6)).await.unwrap();

        assert_eq!(fills, 0);
    }

    #[tokio::test]
    async fn window_boundary_compares_timestamps_not_text() {
        let pool = migrated_pool().await;
        let now = DateTime::parse_from_rfc3339("2026-10-02T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Persisted timestamps vary in fractional digits; the second one
        // sorts after the cutoff as text but is a half second before it.
        insert_event(
            &pool,
            "Mint",
            "mint-fractional",
            "MintEvent::Initiated",
            r#"{"Initiated":{"initiated_at":"2026-10-02T05:59:59.500Z"}}"#,
        )
        .await;
        insert_event(
            &pool,
            "Mint",
            "mint-whole",
            "MintEvent::Initiated",
            r#"{"Initiated":{"initiated_at":"2026-10-02T06:00:00Z"}}"#,
        )
        .await;

        let fills =
            count_fills(&pool, now - TimeDelta::hours(6)).await.unwrap();

        assert_eq!(fills, 1, "only the fill at the cutoff is in the window");
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_below_minimum_alerts_with_count_and_window() {
        let pool = migrated_pool().await;
        let now = full_window_now();
        seed_mints(&pool, 17, now - TimeDelta::hours(1)).await;
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());

        let state = monitor.poll_once(None, now, Instant::now()).await;

        assert!(state.is_some(), "a delivered alert must start the timer");
        assert_eq!(
            notifier.delivered(),
            vec![LifecycleNotification::LowFillRate {
                fills: 17,
                required_fills: 18,
                window_hours: NonZeroU32::new(6).unwrap(),
            }]
        );
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Fill rate is below the configured minimum",
                "fills=17",
                "required_fills=18"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_at_minimum_is_healthy() {
        let pool = migrated_pool().await;
        let now = full_window_now();
        seed_mints(&pool, 18, now - TimeDelta::hours(1)).await;
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());

        let state = monitor.poll_once(None, now, Instant::now()).await;

        assert_eq!(state, None);
        assert!(notifier.delivered().is_empty());
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Counted fills", "fills=18", "required_fills=18"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_recovery_logs_without_notifying() {
        let pool = migrated_pool().await;
        let now = full_window_now();
        seed_mints(&pool, 20, now - TimeDelta::hours(1)).await;
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());

        let state =
            monitor.poll_once(Some(Instant::now()), now, Instant::now()).await;

        assert_eq!(state, None);
        assert!(notifier.delivered().is_empty());
        assert!(logs_contain_at!(
            Level::INFO,
            &["Fill rate recovered", "fills=20", "required_fills=18"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn failed_alert_retries_on_the_next_poll() {
        let pool = migrated_pool().await;
        let now = full_window_now();
        let notifier = FlakyNotifier::new(1);
        let monitor = monitor(pool, notifier.clone());

        let state = monitor.poll_once(None, now, Instant::now()).await;

        assert_eq!(state, None, "a failed delivery must not start the timer");
        assert!(notifier.delivered().is_empty());
        assert!(logs_contain_at!(
            Level::WARN,
            &["Low fill rate alert delivery failed"]
        ));

        let state = monitor.poll_once(state, now, Instant::now()).await;

        assert!(state.is_some());
        assert_eq!(
            notifier.delivered(),
            vec![LifecycleNotification::LowFillRate {
                fills: 0,
                required_fills: 18,
                window_hours: NonZeroU32::new(6).unwrap(),
            }]
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn count_failure_keeps_state_and_does_not_alert() {
        let pool = migrated_pool().await;
        sqlx::query("DROP TABLE events").execute(&pool).await.unwrap();
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());
        let alerted_at = Some(Instant::now());

        let state = monitor
            .poll_once(alerted_at, full_window_now(), Instant::now())
            .await;

        assert_eq!(state, alerted_at);
        assert!(notifier.delivered().is_empty());
        assert!(logs_contain_at!(
            Level::WARN,
            &["Failed to count fills in the trailing window"]
        ));
    }

    /// Polls at `now` with a pending alert and with none, over a database
    /// with no fills at all, and asserts the poll neither counts, alerts, nor
    /// touches the state.
    async fn assert_poll_is_silent(now: DateTime<Utc>, label: &str) {
        let pool = migrated_pool().await;
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());
        let pending = Some(Instant::now());

        let healthy_state = monitor.poll_once(None, now, Instant::now()).await;
        let pending_state =
            monitor.poll_once(pending, now, Instant::now()).await;

        assert_eq!(healthy_state, None, "{label}: state must be unchanged");
        assert_eq!(pending_state, pending, "{label}: state must be unchanged");
        assert!(
            notifier.delivered().is_empty(),
            "{label}: a shortfall must not alert"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn weekend_polls_are_silent() {
        // 2026-10-03 is a Saturday, 2026-10-04 a Sunday.
        assert_poll_is_silent(new_york(2026, 10, 3, 12, 0), "Saturday").await;
        assert_poll_is_silent(new_york(2026, 10, 4, 12, 0), "Sunday").await;
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Outside the session", "fill rate not evaluated"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn overnight_polls_are_silent() {
        assert_poll_is_silent(new_york(2026, 10, 5, 3, 0), "before the open")
            .await;
        assert_poll_is_silent(new_york(2026, 10, 5, 20, 0), "at the close")
            .await;
        assert_poll_is_silent(new_york(2026, 10, 6, 2, 0), "after midnight")
            .await;
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Outside the session", "fill rate not evaluated"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn holiday_polls_are_silent() {
        // Thanksgiving 2026, a Thursday, at a time that is mid-session on any
        // ordinary weekday.
        assert_poll_is_silent(new_york(2026, 11, 26, 14, 0), "Thanksgiving")
            .await;
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Outside the session", "fill rate not evaluated"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn a_session_open_for_less_than_the_window_is_silent() {
        // Monday 09:59: nine fewer minutes than the six hour window needs.
        assert_poll_is_silent(new_york(2026, 10, 5, 9, 59), "warming up").await;
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["window is not yet full", "fill rate not evaluated"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn the_first_poll_with_a_full_window_can_alert() {
        let pool = migrated_pool().await;
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());

        let state = monitor
            .poll_once(None, new_york(2026, 10, 5, 10, 0), Instant::now())
            .await;

        assert!(state.is_some(), "10:00 is the first page of a session");
        assert_eq!(notifier.delivered().len(), 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn fridays_fills_do_not_count_toward_mondays_window() {
        let pool = migrated_pool().await;
        // Plenty of fills on Friday evening, none yet on Monday.
        seed_mints(&pool, 40, new_york(2026, 10, 2, 18, 0)).await;
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());

        let state = monitor
            .poll_once(None, new_york(2026, 10, 5, 10, 0), Instant::now())
            .await;

        assert!(state.is_some());
        assert_eq!(
            notifier.delivered(),
            vec![LifecycleNotification::LowFillRate {
                fills: 0,
                required_fills: 18,
                window_hours: NonZeroU32::new(6).unwrap(),
            }]
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn a_date_outside_the_holiday_list_is_silent_and_says_why() {
        let pool = migrated_pool().await;
        let notifier = FlakyNotifier::new(0);
        let monitor = monitor(pool, notifier.clone());
        let pending = Some(Instant::now());

        // Midday on a weekday in a year the list does not cover.
        let state = monitor
            .poll_once(pending, new_york(2029, 7, 4, 12, 0), Instant::now())
            .await;

        assert_eq!(state, pending);
        assert!(notifier.delivered().is_empty());
        assert!(logs_contain_at!(Level::WARN, &["holiday list", "2029-07-04"]));
    }
}
