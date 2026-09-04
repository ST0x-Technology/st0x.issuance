//! Per chain native gas balance monitoring for the issuer wallet.
//!
//! Signed transactions (mints, burns, receipt moves) spend the chain's native
//! token from the single issuer wallet: ETH on Base and Ethereum, HYPE on
//! HyperEVM. An empty wallet halts issuance on that chain, so one
//! [`GasMonitor`] per configured chain polls `eth_getBalance` for the issuer
//! wallet and alerts before the wallet runs dry.
//!
//! Spam control is time based dedup, not a hysteresis band: the monitor alerts
//! once when the balance drops below the threshold, alerts again at most once
//! per [`GAS_REALERT_INTERVAL`] while it stays low, and only logs recovery.
//! A throttled realert requires a delivered alert: if delivery fails the poll
//! keeps the prior alert state (Normal on the first drop), so the next low
//! balance poll retries the alert immediately rather than waiting out the
//! interval. Recovery clears the dedup state, so a later drop below the
//! threshold pages immediately; the interval only throttles repeated alerts
//! while the balance stays continuously low. A failed balance read leaves the
//! alert state unchanged: a transient RPC blip must neither fire nor clear
//! alerts.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::transports::{RpcError, TransportErrorKind};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::network_telemetry::NetworkTelemetry;
use crate::notifications::{LifecycleNotification, LifecycleNotifier};
use crate::tokenized_asset::Network;

/// Interval between balance polls. One `eth_getBalance` per chain per minute
/// is negligible RPC cost while catching a draining wallet well before the
/// next signed transaction fails.
pub(crate) const GAS_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Minimum time between repeated alerts for a wallet that stays below the
/// threshold.
const GAS_REALERT_INTERVAL: Duration = Duration::from_secs(3600);

/// One chain's gas balance monitor loop.
pub(crate) struct GasMonitor<P> {
    pub(crate) network: Network,
    pub(crate) provider: P,
    pub(crate) wallet: Address,
    pub(crate) threshold: U256,
    pub(crate) poll_interval: Duration,
    pub(crate) notifier: Arc<dyn LifecycleNotifier>,
    pub(crate) telemetry: Arc<NetworkTelemetry>,
}

impl<P: Provider> GasMonitor<P> {
    /// Runs the polling loop forever. Never returns; the spawn site pairs it
    /// with the shutdown channel in a `select!`, like the transfer poller.
    pub(crate) async fn run(&self) {
        debug!(
            target: "gas",
            network = %self.network,
            wallet = %self.wallet,
            threshold = %self.threshold,
            "Starting gas balance monitor"
        );

        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut state = AlertState::Normal;
        loop {
            interval.tick().await;
            state = self.poll_once(state, Instant::now()).await;
        }
    }

    async fn poll_once(&self, state: AlertState, now: Instant) -> AlertState {
        let balance = match self.provider.get_balance(self.wallet).await {
            Ok(balance) => balance,
            Err(read_error) => {
                // The raw error's Display can carry the provider URL and its
                // embedded API key, so only a bounded, non-secret category
                // reaches the log and the telemetry surface.
                let reason = classify_balance_read_error(&read_error);
                warn!(
                    target: "gas",
                    network = %self.network,
                    wallet = %self.wallet,
                    reason,
                    "Failed to read the issuer wallet's native balance"
                );
                self.telemetry
                    .record_gas_read_failure(self.network, reason.to_owned());
                return state;
            }
        };

        self.telemetry.record_gas_reading(
            self.network,
            balance,
            self.threshold,
        );

        let (next_state, outcome) =
            evaluate(state, balance, self.threshold, GAS_REALERT_INTERVAL, now);

        if self.act_on_outcome(outcome, balance).await {
            next_state
        } else {
            // The alert did not reach the operator, so keep the prior dedup
            // state: advancing `last_alerted` here would suppress every retry
            // for a full `GAS_REALERT_INTERVAL`, silently leaving a low wallet
            // unpaged. Retaining it re-alerts on the next poll instead.
            state
        }
    }

    /// Acts on one poll's outcome and reports whether the alert transition may
    /// stand. Returns `true` when nothing needed delivery or delivery
    /// succeeded, and `false` when an alert was attempted but its delivery
    /// failed, so the caller retains the prior deduplication state.
    async fn act_on_outcome(
        &self,
        outcome: PollOutcome,
        balance: U256,
    ) -> bool {
        match outcome {
            PollOutcome::DroppedBelow | PollOutcome::StillLowRealert => {
                error!(
                    target: "gas",
                    network = %self.network,
                    wallet = %self.wallet,
                    balance = %balance,
                    threshold = %self.threshold,
                    "Issuer wallet native balance is below the low gas \
                     threshold"
                );
                match self
                    .notifier
                    .deliver(&LifecycleNotification::LowGasBalance {
                        network: self.network,
                        wallet: self.wallet,
                        balance,
                        threshold: self.threshold,
                    })
                    .await
                {
                    Ok(()) => true,
                    Err(delivery_error) => {
                        warn!(
                            target: "gas",
                            network = %self.network,
                            wallet = %self.wallet,
                            error = %delivery_error,
                            "Low gas alert delivery failed; retrying on the \
                             next poll"
                        );
                        false
                    }
                }
            }
            PollOutcome::Recovered => {
                info!(
                    target: "gas",
                    network = %self.network,
                    wallet = %self.wallet,
                    balance = %balance,
                    threshold = %self.threshold,
                    "Issuer wallet native balance recovered above the \
                     low gas threshold"
                );
                true
            }
            PollOutcome::StillHealthy | PollOutcome::StillLowSuppressed => true,
        }
    }
}

/// Alert dedup state: either the balance was above the threshold at the last
/// poll, or it was below and `last_alerted` timestamps the most recent alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlertState {
    Normal,
    Low { last_alerted: Instant },
}

/// What one poll observed relative to the previous state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    StillHealthy,
    DroppedBelow,
    StillLowSuppressed,
    StillLowRealert,
    Recovered,
}

/// Pure alert transition: given the previous state and the freshly read
/// balance, decides the next state and whether this poll alerts.
fn evaluate(
    state: AlertState,
    balance: U256,
    threshold: U256,
    realert_interval: Duration,
    now: Instant,
) -> (AlertState, PollOutcome) {
    let is_low = balance < threshold;

    match (state, is_low) {
        (AlertState::Normal, false) => {
            (AlertState::Normal, PollOutcome::StillHealthy)
        }
        (AlertState::Normal, true) => {
            (AlertState::Low { last_alerted: now }, PollOutcome::DroppedBelow)
        }
        (AlertState::Low { .. }, false) => {
            (AlertState::Normal, PollOutcome::Recovered)
        }
        (AlertState::Low { last_alerted }, true) => {
            if now.saturating_duration_since(last_alerted) >= realert_interval {
                (
                    AlertState::Low { last_alerted: now },
                    PollOutcome::StillLowRealert,
                )
            } else {
                (
                    AlertState::Low { last_alerted },
                    PollOutcome::StillLowSuppressed,
                )
            }
        }
    }
}

/// Maps a native-balance read error to a bounded, non-secret category. The
/// raw error's `Display` can carry the provider URL with its embedded API
/// key, so it must never reach the WARN log or the telemetry surface; only
/// this fixed classification does.
const fn classify_balance_read_error(
    error: &RpcError<TransportErrorKind>,
) -> &'static str {
    match error {
        RpcError::ErrorResp(_) => "rpc error response",
        RpcError::NullResp => "null response",
        RpcError::UnsupportedFeature(_) => "unsupported feature",
        RpcError::LocalUsageError(_) => "local usage error",
        RpcError::SerError(_) => "serialization error",
        RpcError::DeserError { .. } => "deserialization error",
        RpcError::Transport(_) => "transport error",
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{U256, address};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::signers::local::PrivateKeySigner;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{AlertState, GasMonitor, PollOutcome, evaluate};
    use crate::network_telemetry::NetworkTelemetry;
    use crate::notifications::{
        LifecycleNotification, LifecycleNotificationError, LifecycleNotifier,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::Network;

    const REALERT: Duration = Duration::from_secs(3600);

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

    fn monitor(
        asserter: &Asserter,
        threshold: U256,
        notifier: Arc<dyn LifecycleNotifier>,
        telemetry: Arc<NetworkTelemetry>,
    ) -> GasMonitor<impl alloy::providers::Provider> {
        GasMonitor {
            network: Network::Base,
            provider: ProviderBuilder::new()
                .wallet(EthereumWallet::from(PrivateKeySigner::random()))
                .connect_mocked_client(asserter.clone()),
            wallet: address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
            threshold,
            poll_interval: Duration::from_secs(60),
            notifier,
            telemetry,
        }
    }

    #[test]
    fn healthy_balance_stays_normal() {
        let (state, outcome) = evaluate(
            AlertState::Normal,
            U256::from(100),
            U256::from(100),
            REALERT,
            Instant::now(),
        );

        assert_eq!(state, AlertState::Normal);
        assert_eq!(outcome, PollOutcome::StillHealthy);
    }

    #[test]
    fn drop_below_threshold_alerts_once() {
        let now = Instant::now();

        let (state, outcome) = evaluate(
            AlertState::Normal,
            U256::from(99),
            U256::from(100),
            REALERT,
            now,
        );
        assert_eq!(state, AlertState::Low { last_alerted: now });
        assert_eq!(outcome, PollOutcome::DroppedBelow);

        let (state, outcome) = evaluate(
            state,
            U256::from(99),
            U256::from(100),
            REALERT,
            now + Duration::from_secs(60),
        );
        assert_eq!(state, AlertState::Low { last_alerted: now });
        assert_eq!(outcome, PollOutcome::StillLowSuppressed);
    }

    #[test]
    fn sustained_low_realerts_after_the_interval() {
        let alerted_at = Instant::now();
        let realert_due = alerted_at + REALERT;

        let (state, outcome) = evaluate(
            AlertState::Low { last_alerted: alerted_at },
            U256::from(1),
            U256::from(100),
            REALERT,
            realert_due,
        );

        assert_eq!(state, AlertState::Low { last_alerted: realert_due });
        assert_eq!(outcome, PollOutcome::StillLowRealert);
    }

    #[test]
    fn recovery_returns_to_normal_and_resets_dedup() {
        let alerted_at = Instant::now();

        let (state, outcome) = evaluate(
            AlertState::Low { last_alerted: alerted_at },
            U256::from(100),
            U256::from(100),
            REALERT,
            alerted_at + Duration::from_secs(60),
        );
        assert_eq!(state, AlertState::Normal);
        assert_eq!(outcome, PollOutcome::Recovered);

        // The next drop alerts immediately: recovery cleared the dedup state.
        let (_, outcome) = evaluate(
            state,
            U256::from(1),
            U256::from(100),
            REALERT,
            alerted_at + Duration::from_secs(120),
        );
        assert_eq!(outcome, PollOutcome::DroppedBelow);
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_below_threshold_notifies_and_records_telemetry() {
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(5));
        let notifier = FlakyNotifier::new(0);
        let telemetry = Arc::new(NetworkTelemetry::new([Network::Base]));
        let monitor = monitor(
            &asserter,
            U256::from(100),
            notifier.clone(),
            telemetry.clone(),
        );

        let state = monitor.poll_once(AlertState::Normal, Instant::now()).await;

        assert!(matches!(state, AlertState::Low { .. }));
        assert_eq!(
            notifier.delivered(),
            vec![LifecycleNotification::LowGasBalance {
                network: Network::Base,
                wallet: monitor.wallet,
                balance: U256::from(5),
                threshold: U256::from(100),
            }]
        );
        let snapshot = serde_json::to_value(telemetry.snapshot()).unwrap();
        assert_eq!(snapshot[0]["gas"]["status"], "low");
        assert_eq!(snapshot[0]["gas"]["balance_wei"], "5");
        assert!(logs_contain_at!(
            Level::ERROR,
            &["below the low gas", "base", "balance=5", "threshold=100"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_read_failure_keeps_state_and_degrades_telemetry() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("rpc down");
        let notifier = FlakyNotifier::new(0);
        let telemetry = Arc::new(NetworkTelemetry::new([Network::Base]));
        let monitor = monitor(
            &asserter,
            U256::from(100),
            notifier.clone(),
            telemetry.clone(),
        );
        let low_state = AlertState::Low { last_alerted: Instant::now() };

        let state = monitor.poll_once(low_state, Instant::now()).await;

        // The read failure must neither clear the low state nor alert again.
        assert_eq!(state, low_state);
        assert!(notifier.delivered().is_empty());
        let snapshot = serde_json::to_value(telemetry.snapshot()).unwrap();
        assert_eq!(snapshot[0]["gas"]["status"], "unavailable");
        // The raw error text (which could carry the provider URL and key) must
        // not reach telemetry; only the bounded classification does.
        let reported = snapshot[0]["gas"]["error"].as_str().unwrap();
        assert!(
            !reported.contains("rpc down"),
            "raw error leaked into telemetry: {reported}"
        );
        assert!(
            ["transport error", "rpc error response", "deserialization error"]
                .contains(&reported),
            "unexpected classification: {reported}"
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &["Failed to read the issuer wallet's native balance", "base"]
        ));
        // The raw error must not leak into the log either.
        assert!(!logs_contain_at!(Level::WARN, &["rpc down"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_recovery_logs_without_notifying() {
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200));
        let notifier = FlakyNotifier::new(0);
        let telemetry = Arc::new(NetworkTelemetry::new([Network::Base]));
        let monitor = monitor(
            &asserter,
            U256::from(100),
            notifier.clone(),
            telemetry.clone(),
        );
        let low_state = AlertState::Low { last_alerted: Instant::now() };

        let state = monitor.poll_once(low_state, Instant::now()).await;

        assert_eq!(state, AlertState::Normal);
        assert!(notifier.delivered().is_empty());
        let snapshot = serde_json::to_value(telemetry.snapshot()).unwrap();
        assert_eq!(snapshot[0]["gas"]["status"], "ok");
        assert!(logs_contain_at!(
            Level::INFO,
            &["recovered above", "base", "balance=200"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn failed_alert_retries_on_the_next_poll() {
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(5));
        asserter.push_success(&U256::from(5));
        // The first delivery fails, the second succeeds.
        let notifier = FlakyNotifier::new(1);
        let telemetry = Arc::new(NetworkTelemetry::new([Network::Base]));
        let monitor = monitor(
            &asserter,
            U256::from(100),
            notifier.clone(),
            telemetry.clone(),
        );

        // First poll: balance is low but the alert fails to deliver, so the
        // dedup state must stay Normal rather than advancing to Low; otherwise
        // the retry would be suppressed for a full GAS_REALERT_INTERVAL.
        let state = monitor.poll_once(AlertState::Normal, Instant::now()).await;
        assert_eq!(state, AlertState::Normal);
        assert!(notifier.delivered().is_empty());
        assert!(logs_contain_at!(
            Level::WARN,
            &["Low gas alert delivery failed", "base"]
        ));

        // Second poll: still low, delivery now succeeds, so the alert lands
        // and the state advances to Low.
        let state = monitor.poll_once(state, Instant::now()).await;
        assert!(matches!(state, AlertState::Low { .. }));
        assert_eq!(
            notifier.delivered(),
            vec![LifecycleNotification::LowGasBalance {
                network: Network::Base,
                wallet: monitor.wallet,
                balance: U256::from(5),
                threshold: U256::from(100),
            }]
        );
    }
}
