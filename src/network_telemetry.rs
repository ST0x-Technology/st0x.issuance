//! Per network operational telemetry.
//!
//! An in memory registry, one entry per configured network, aggregating what
//! the long running per network loops report: transfer poller passes, periodic
//! receipt backfill passes (each with block lag), and the gas monitor's latest
//! native balance reading. `GET /admin/network-telemetry` serves snapshots of
//! it. Deliberately not persisted: it describes the running process, and the
//! durable signals (checkpoints, event store) already survive restarts.

use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;
use tracing::warn;

use crate::tokenized_asset::Network;

/// Registry of per network loop and gas balance health. Shared as an `Arc`
/// between the recording loops and the admin read surface.
pub(crate) struct NetworkTelemetry {
    networks: HashMap<Network, Mutex<NetworkStats>>,
}

impl NetworkTelemetry {
    pub(crate) fn new(networks: impl IntoIterator<Item = Network>) -> Self {
        Self {
            networks: networks
                .into_iter()
                .map(|network| (network, Mutex::new(NetworkStats::default())))
                .collect(),
        }
    }

    /// Records a transfer poller pass that made progress. `lag_blocks` is the
    /// worst per vault distance between the chain head and the vault's cursor
    /// at the start of the pass.
    pub(crate) fn record_transfer_poll_success(
        &self,
        network: Network,
        lag_blocks: u64,
    ) {
        self.with_stats(network, |stats| {
            stats.transfer_poller.record_success(lag_blocks);
        });
    }

    /// Records a transfer poller pass where nothing progressed: the asset view
    /// read or head fetch failed, or every vault failed.
    pub(crate) fn record_transfer_poll_failure(&self, network: Network) {
        self.with_stats(network, |stats| {
            stats.transfer_poller.record_failure();
        });
    }

    /// Records a periodic receipt backfill pass that made progress.
    /// `lag_blocks` is the worst per vault distance between the chain head and
    /// the vault's receipt backfill checkpoint at the start of the pass.
    pub(crate) fn record_receipt_backfill_success(
        &self,
        network: Network,
        lag_blocks: u64,
    ) {
        self.with_stats(network, |stats| {
            stats.receipt_backfill.record_success(lag_blocks);
        });
    }

    /// Records a periodic receipt backfill pass where nothing progressed: the
    /// asset list or head fetch failed, or every vault failed.
    pub(crate) fn record_receipt_backfill_failure(&self, network: Network) {
        self.with_stats(network, |stats| {
            stats.receipt_backfill.record_failure();
        });
    }

    /// Records a successful gas balance reading for the issuer wallet.
    pub(crate) fn record_gas_reading(
        &self,
        network: Network,
        balance: U256,
        threshold: U256,
    ) {
        self.with_stats(network, |stats| {
            stats.gas =
                GasReading::Read { balance, threshold, checked_at: Utc::now() };
        });
    }

    /// Records a failed gas balance read; the previous reading is replaced so
    /// a stale balance cannot masquerade as current.
    pub(crate) fn record_gas_read_failure(
        &self,
        network: Network,
        error: String,
    ) {
        self.with_stats(network, |stats| {
            stats.gas = GasReading::Unavailable { error };
        });
    }

    /// Snapshots every network's stats, sorted by network wire name so the
    /// admin response is deterministic.
    pub(crate) fn snapshot(&self) -> Vec<NetworkTelemetrySnapshot> {
        self.networks
            .iter()
            .map(|(network, stats)| {
                let stats = stats.lock();
                NetworkTelemetrySnapshot {
                    network: *network,
                    transfer_poller: stats.transfer_poller.snapshot(),
                    receipt_backfill: stats.receipt_backfill.snapshot(),
                    gas: stats.gas.snapshot(),
                }
            })
            .sorted_by_key(|snapshot| snapshot.network.as_str())
            .collect()
    }

    fn with_stats(
        &self,
        network: Network,
        update: impl FnOnce(&mut NetworkStats),
    ) {
        let Some(stats) = self.networks.get(&network) else {
            warn!(
                target: "network_telemetry",
                %network,
                "Dropping telemetry record for an unconfigured network"
            );
            return;
        };

        update(&mut stats.lock());
    }
}

#[derive(Default)]
struct NetworkStats {
    transfer_poller: PassStats,
    receipt_backfill: PassStats,
    gas: GasReading,
}

/// Cumulative pass counters for one long running loop on one network.
/// Counters are `u32`: saturation at ~4 billion passes is centuries away at
/// the loops' poll intervals, and `f64::from(u32)` is lossless where a
/// `u64` cast would not be.
#[derive(Default)]
struct PassStats {
    passes: u32,
    failures: u32,
    consecutive_failures: u32,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    lag_blocks: Option<u64>,
}

impl PassStats {
    fn record_success(&mut self, lag_blocks: u64) {
        self.passes = self.passes.saturating_add(1);
        self.consecutive_failures = 0;
        self.last_success_at = Some(Utc::now());
        self.lag_blocks = Some(lag_blocks);
    }

    fn record_failure(&mut self) {
        self.passes = self.passes.saturating_add(1);
        self.failures = self.failures.saturating_add(1);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_failure_at = Some(Utc::now());
    }

    fn snapshot(&self) -> PassStatsSnapshot {
        let failure_rate = (self.passes > 0)
            .then(|| f64::from(self.failures) / f64::from(self.passes));

        PassStatsSnapshot {
            passes: self.passes,
            failures: self.failures,
            consecutive_failures: self.consecutive_failures,
            failure_rate,
            last_success_at: self.last_success_at,
            last_failure_at: self.last_failure_at,
            lag_blocks: self.lag_blocks,
        }
    }
}

/// The gas monitor's latest observation for one network. `low` versus `ok` is
/// derived at snapshot time from the stored balance and threshold, so the two
/// can never contradict.
#[derive(Default)]
enum GasReading {
    #[default]
    Unmonitored,
    Unavailable {
        error: String,
    },
    Read {
        balance: U256,
        threshold: U256,
        checked_at: DateTime<Utc>,
    },
}

impl GasReading {
    fn snapshot(&self) -> GasStatusSnapshot {
        match self {
            Self::Unmonitored => GasStatusSnapshot::Unmonitored,
            Self::Unavailable { error } => {
                GasStatusSnapshot::Unavailable { error: error.clone() }
            }
            Self::Read { balance, threshold, checked_at }
                if balance < threshold =>
            {
                GasStatusSnapshot::Low {
                    balance_wei: balance.to_string(),
                    threshold_wei: threshold.to_string(),
                    checked_at: *checked_at,
                }
            }
            Self::Read { balance, threshold, checked_at } => {
                GasStatusSnapshot::Ok {
                    balance_wei: balance.to_string(),
                    threshold_wei: threshold.to_string(),
                    checked_at: *checked_at,
                }
            }
        }
    }
}

/// One network's row in the `GET /admin/network-telemetry` response.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct NetworkTelemetrySnapshot {
    network: Network,
    transfer_poller: PassStatsSnapshot,
    receipt_backfill: PassStatsSnapshot,
    gas: GasStatusSnapshot,
}

/// Serialized pass counters for one loop. `passes` is every completed pass,
/// successes and failures alike, so successes are `passes - failures`.
/// `failure_rate` is `failures / passes`, absent until the first pass
/// completes; `lag_blocks` is absent until the first successful pass records
/// one.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct PassStatsSnapshot {
    passes: u32,
    failures: u32,
    consecutive_failures: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    last_success_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    last_failure_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag_blocks: Option<u64>,
}

/// Serialized gas status for one network: `ok`/`low` carry the reading,
/// `unavailable` the read error, `unmonitored` no fields. Wei amounts are
/// decimal strings.
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case", tag = "status")]
pub(crate) enum GasStatusSnapshot {
    Ok {
        balance_wei: String,
        threshold_wei: String,
        #[schema(value_type = String)]
        checked_at: DateTime<Utc>,
    },
    Low {
        balance_wei: String,
        threshold_wei: String,
        #[schema(value_type = String)]
        checked_at: DateTime<Utc>,
    },
    Unavailable {
        error: String,
    },
    Unmonitored,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::NetworkTelemetry;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::Network;

    fn snapshot_json(telemetry: &NetworkTelemetry) -> serde_json::Value {
        serde_json::to_value(telemetry.snapshot()).unwrap()
    }

    #[test]
    fn fresh_registry_reports_zero_counters_and_unmonitored_gas() {
        let telemetry =
            NetworkTelemetry::new([Network::HyperEvm, Network::Base]);

        let snapshot = snapshot_json(&telemetry);

        // Sorted by wire name: base before hyperevm regardless of insertion.
        assert_eq!(snapshot[0]["network"], "base");
        assert_eq!(snapshot[1]["network"], "hyperevm");
        assert_eq!(snapshot[0]["transfer_poller"]["passes"], 0);
        assert_eq!(snapshot[0]["transfer_poller"].get("failure_rate"), None);
        assert_eq!(snapshot[0]["transfer_poller"].get("lag_blocks"), None);
        assert_eq!(snapshot[0]["receipt_backfill"]["passes"], 0);
        assert_eq!(snapshot[0]["gas"]["status"], "unmonitored");
    }

    #[test]
    fn pass_counters_track_failure_rate_and_consecutive_failures() {
        let telemetry = NetworkTelemetry::new([Network::Base]);

        telemetry.record_transfer_poll_failure(Network::Base);
        telemetry.record_transfer_poll_failure(Network::Base);
        telemetry.record_transfer_poll_success(Network::Base, 7);
        telemetry.record_transfer_poll_failure(Network::Base);

        let poller = &snapshot_json(&telemetry)[0]["transfer_poller"];
        assert_eq!(poller["passes"], 4);
        assert_eq!(poller["failures"], 3);
        assert_eq!(poller["consecutive_failures"], 1);
        assert_eq!(poller["failure_rate"], 0.75);
        // The last successful pass's lag survives later failures.
        assert_eq!(poller["lag_blocks"], 7);
        assert!(poller["last_success_at"].is_string());
        assert!(poller["last_failure_at"].is_string());
    }

    #[test]
    fn backfill_counters_are_independent_of_the_poller() {
        let telemetry = NetworkTelemetry::new([Network::Base]);

        telemetry.record_receipt_backfill_success(Network::Base, 3);

        let snapshot = snapshot_json(&telemetry);
        assert_eq!(snapshot[0]["receipt_backfill"]["passes"], 1);
        assert_eq!(snapshot[0]["receipt_backfill"]["lag_blocks"], 3);
        assert_eq!(snapshot[0]["transfer_poller"]["passes"], 0);
    }

    #[test]
    fn gas_reading_derives_low_from_balance_and_threshold() {
        let telemetry = NetworkTelemetry::new([Network::Base]);

        telemetry.record_gas_reading(
            Network::Base,
            U256::from(5),
            U256::from(10),
        );
        let snapshot = snapshot_json(&telemetry);
        assert_eq!(snapshot[0]["gas"]["status"], "low");
        assert_eq!(snapshot[0]["gas"]["balance_wei"], "5");
        assert_eq!(snapshot[0]["gas"]["threshold_wei"], "10");

        telemetry.record_gas_reading(
            Network::Base,
            U256::from(10),
            U256::from(10),
        );
        assert_eq!(snapshot_json(&telemetry)[0]["gas"]["status"], "ok");

        telemetry
            .record_gas_read_failure(Network::Base, "rpc down".to_string());
        let snapshot = snapshot_json(&telemetry);
        assert_eq!(snapshot[0]["gas"]["status"], "unavailable");
        assert_eq!(snapshot[0]["gas"]["error"], "rpc down");
    }

    #[traced_test]
    #[test]
    fn record_for_unconfigured_network_warns_and_is_dropped() {
        let telemetry = NetworkTelemetry::new([Network::Base]);

        telemetry.record_transfer_poll_success(Network::Ethereum, 1);

        let snapshot = snapshot_json(&telemetry);
        assert_eq!(snapshot.as_array().unwrap().len(), 1);
        assert_eq!(snapshot[0]["transfer_poller"]["passes"], 0);
        assert!(logs_contain_at!(
            Level::WARN,
            &["Dropping telemetry record", "ethereum"]
        ));
    }
}
