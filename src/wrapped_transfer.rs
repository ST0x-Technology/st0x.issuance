//! Inbound wrapped-token transfer detection for the issuer wallet.
//!
//! The redemption transfer poller watches each asset's vault, i.e. the
//! unwrapped share token. A redemption sent as the ERC-4626 wrapped token
//! lands in the issuer wallet without ever being detected or redeemed. This
//! module is the issuance backstop: per network, it watches the configured
//! wrapped-token contracts for `Transfer` events to the issuer wallet, records
//! each one durably, raises an operator alert, and exposes the recorded
//! transfers to the admin API. Recovery itself is a manual operation.
//!
//! Alert dedup is durable: the lifecycle notification is queued under an
//! idempotency key derived from the log identity, exactly as the
//! corporate-action notifications are, so a restart or a re-scan never
//! re-alerts a transfer whose alert was delivered, while a dead delivery is
//! released and retried.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use chrono::{DateTime, Utc};
use sqlx::{Pool, Sqlite};
use st0x_issuance_dto::{NetworkParseError, UnderlyingSymbolError};
use std::collections::HashMap;
use std::num::TryFromIntError;
use std::time::Duration;
use tracing::{debug, error, trace, warn};

use crate::bindings;
use crate::jobs::{JobQueue, QueuePushError};
use crate::notifications::{
    LifecycleNotification, SendLifecycleNotification,
    release_dead_lifecycle_notification_job,
};
use crate::poll_checkpoint::{
    CheckpointError, advance_checkpoint_block, load_checkpoint_block,
};
use crate::redemption::poller::{BLOCK_CHUNK_SIZE, block_ranges};
use crate::tokenized_asset::{Network, UnderlyingSymbol};

/// Interval between polling passes once a watcher is caught up. Inbound
/// wrapped-token transfers are rare and the alert is not latency critical, so
/// one `eth_getLogs` per token per minute is plenty.
pub(crate) const WRAPPED_TRANSFER_POLL_INTERVAL: Duration =
    Duration::from_secs(60);

/// Interval between retries when a polling pass fails (e.g. RPC error).
const RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Wrapped-token contract addresses to watch, per network, each mapped to the
/// underlying it wraps. Built from the `[wrapped_tokens.<network>]` tables of
/// the TOML config file; existence implies the entries passed validation.
#[derive(Debug, Clone, Default)]
pub struct WrappedTokenConfig {
    per_network: HashMap<Network, HashMap<Address, UnderlyingSymbol>>,
}

/// One `[wrapped_tokens.<network>]` entry: `underlying = "<token>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedTokenEntry {
    pub network: Network,
    pub underlying: UnderlyingSymbol,
    pub token: Address,
}

/// One wrapped token a network's watcher scans, with the underlying the alert
/// names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchedWrappedToken {
    pub(crate) token: Address,
    pub(crate) underlying: UnderlyingSymbol,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WrappedTokenConfigError {
    #[error(
        "wrapped token address for {underlying} on {network} is the zero \
         address"
    )]
    ZeroAddress { network: Network, underlying: UnderlyingSymbol },
    #[error(
        "wrapped token {token} on {network} is configured for both {first} \
         and {second}; one address cannot wrap two underlyings on one network"
    )]
    AddressCollision {
        network: Network,
        token: Address,
        first: UnderlyingSymbol,
        second: UnderlyingSymbol,
    },
    #[error("{underlying} on {network} has more than one wrapped token entry")]
    DuplicateUnderlying { network: Network, underlying: UnderlyingSymbol },
}

impl WrappedTokenConfig {
    /// Validates the entries: no zero address, and on each network one
    /// address wraps one underlying and one underlying has one address.
    ///
    /// # Errors
    ///
    /// Returns the first entry that violates one of those rules.
    pub fn new(
        entries: impl IntoIterator<Item = WrappedTokenEntry>,
    ) -> Result<Self, WrappedTokenConfigError> {
        let mut per_network: HashMap<
            Network,
            HashMap<Address, UnderlyingSymbol>,
        > = HashMap::new();

        for WrappedTokenEntry { network, underlying, token } in entries {
            if token.is_zero() {
                return Err(WrappedTokenConfigError::ZeroAddress {
                    network,
                    underlying,
                });
            }

            let tokens = per_network.entry(network).or_default();
            if tokens.values().any(|existing| existing == &underlying) {
                return Err(WrappedTokenConfigError::DuplicateUnderlying {
                    network,
                    underlying,
                });
            }

            if let Some(first) = tokens.insert(token, underlying.clone()) {
                return Err(WrappedTokenConfigError::AddressCollision {
                    network,
                    token,
                    first,
                    second: underlying,
                });
            }
        }

        Ok(Self { per_network })
    }

    /// The tokens to watch on `network`, sorted by address so a pass scans
    /// them in a deterministic order. Empty when the network has no entries.
    pub(crate) fn watched_on(
        &self,
        network: Network,
    ) -> Vec<WatchedWrappedToken> {
        let mut watched: Vec<WatchedWrappedToken> = self
            .per_network
            .get(&network)
            .into_iter()
            .flatten()
            .map(|(token, underlying)| WatchedWrappedToken {
                token: *token,
                underlying: underlying.clone(),
            })
            .collect();
        watched.sort_unstable_by_key(|watched| watched.token);
        watched
    }

    /// Every network that has at least one entry, so startup can reject a
    /// table for a chain that has no configuration.
    pub(crate) fn networks(&self) -> impl Iterator<Item = Network> + '_ {
        self.per_network
            .iter()
            .filter(|(_, tokens)| !tokens.is_empty())
            .map(|(network, _)| *network)
    }
}

/// One recorded inbound wrapped-token transfer, as stored in
/// `inbound_wrapped_transfers`. `amount` is the raw ERC-20 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InboundWrappedTransfer {
    pub(crate) network: Network,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: Address,
    pub(crate) from: Address,
    pub(crate) amount: U256,
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
    pub(crate) block_number: u64,
    pub(crate) detected_at: DateTime<Utc>,
}

/// One network's watcher loop over its configured wrapped tokens. Mirrors the
/// redemption transfer poller: per token checkpoints in `poll_checkpoints`,
/// chunked `eth_getLogs`, and a checkpoint that only advances once every log
/// in the chunk is recorded and its alert queued.
pub(crate) struct WrappedTransferMonitor<P> {
    pub(crate) network: Network,
    pub(crate) provider: P,
    pub(crate) bot_wallet: Address,
    pub(crate) backfill_start_block: u64,
    pub(crate) watched: Vec<WatchedWrappedToken>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) apalis_pool: ApalisSqlitePool,
    pub(crate) poll_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WrappedTransferPollError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Checkpoint error: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("Checkpoint overflow: last_processed_block={last_processed_block}")]
    CheckpointOverflow { last_processed_block: u64 },
    #[error("Failed to queue the inbound wrapped-token transfer alert: {0}")]
    QueuePush(#[from] QueuePushError),
    #[error("all {total} wrapped tokens failed the poll pass")]
    AllTokensFailed { total: usize },
}

impl<P: Provider> WrappedTransferMonitor<P> {
    /// Runs the polling loop forever. Never returns; the spawn site pairs it
    /// with the shutdown channel in a `select!`, like the transfer poller.
    pub(crate) async fn run(&self) {
        debug!(
            target: "wrapped_transfer",
            network = %self.network,
            watched = ?self.watched,
            "Starting inbound wrapped-token transfer watcher"
        );

        loop {
            if let Err(error) = self.poll_once().await {
                warn!(
                    target: "wrapped_transfer",
                    network = %self.network,
                    error = %error,
                    retry_after_secs = RETRY_INTERVAL.as_secs(),
                    "Wrapped-token transfer poll pass failed; will retry from \
                     the last checkpoint"
                );
                tokio::time::sleep(RETRY_INTERVAL).await;
                continue;
            }

            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// One pass: scan every watched token from its checkpoint to one shared
    /// chain head. A per token failure does not starve the others; a pass
    /// where every token failed propagates, since that is indistinguishable
    /// from the watcher being offline.
    async fn poll_once(&self) -> Result<(), WrappedTransferPollError> {
        todo!()
    }
}

/// Records `transfer` unless its `(network, tx_hash, log_index)` identity is
/// already stored. Returns whether the row was newly inserted.
pub(crate) async fn record_inbound_wrapped_transfer(
    pool: &Pool<Sqlite>,
    transfer: &InboundWrappedTransfer,
) -> Result<bool, sqlx::Error> {
    let _ = (pool, transfer);
    todo!()
}

/// Every recorded inbound wrapped-token transfer, highest block first.
pub(crate) async fn list_inbound_wrapped_transfers(
    pool: &Pool<Sqlite>,
) -> Result<Vec<InboundWrappedTransfer>, InboundWrappedTransferReadError> {
    let _ = pool;
    todo!()
}

/// A stored row failed to parse back into its typed form.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InboundWrappedTransferReadError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("stored network is not a known network: {0}")]
    Network(#[from] NetworkParseError),
    #[error("stored underlying symbol is invalid: {0}")]
    UnderlyingSymbol(#[from] UnderlyingSymbolError),
    #[error("stored address or hash is invalid: {0}")]
    Hex(#[from] alloy::hex::FromHexError),
    #[error("stored amount is not a decimal integer: {0}")]
    Amount(#[from] alloy::primitives::ruint::ParseError),
    #[error("stored timestamp is not RFC 3339: {0}")]
    Timestamp(#[from] chrono::ParseError),
    #[error("stored block number or log index is negative: {0}")]
    Int(#[from] TryFromIntError),
}

/// Checkpoint name for one network's scan of one wrapped token, in the same
/// per-(network, address) shape as the transfer poller's.
pub(crate) fn checkpoint_name(network: Network, token: Address) -> String {
    format!("wrapped_transfer_poll:{network}:{token:#x}")
}

/// Durable idempotency key of the alert for one transfer log.
fn alert_idempotency_key(
    network: Network,
    tx_hash: TxHash,
    log_index: u64,
) -> String {
    format!("notify:wrapped-transfer:{network}:{tx_hash:#x}:{log_index}")
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Address, TxHash, U256, address, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::Log;
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use std::time::Duration;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        InboundWrappedTransfer, WatchedWrappedToken, WrappedTokenConfig,
        WrappedTokenConfigError, WrappedTokenEntry, WrappedTransferMonitor,
        WrappedTransferPollError, alert_idempotency_key, checkpoint_name,
        list_inbound_wrapped_transfers, record_inbound_wrapped_transfer,
    };
    use crate::jobs::job_type;
    use crate::mint::test_utils::TestHarness;
    use crate::notifications::SendLifecycleNotification;
    use crate::poll_checkpoint::load_checkpoint_block;
    use crate::redemption::test_utils::create_transfer_log_with_index;
    use crate::test_utils::{log_count_at, logs_contain_at};
    use crate::tokenized_asset::{Network, UnderlyingSymbol};

    fn symbol(value: &str) -> UnderlyingSymbol {
        UnderlyingSymbol::new(value).unwrap()
    }

    fn entry(
        network: Network,
        underlying: &str,
        token: Address,
    ) -> WrappedTokenEntry {
        WrappedTokenEntry { network, underlying: symbol(underlying), token }
    }

    const TOKEN_A: Address =
        address!("0x00000000000000000000000000000000000000aa");
    const TOKEN_B: Address =
        address!("0x00000000000000000000000000000000000000bb");
    const BOT_WALLET: Address =
        address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
    const SENDER: Address =
        address!("0x9999999999999999999999999999999999999999");
    const TX_HASH: TxHash = b256!(
        "0x1111111111111111111111111111111111111111111111111111111111111111"
    );

    /// The same address may wrap the same underlying on two chains
    /// (deterministic deploys), and each network's watch list is sorted by
    /// address regardless of entry order.
    #[test]
    fn watched_tokens_are_per_network_and_sorted_by_address() {
        let config = WrappedTokenConfig::new([
            entry(Network::Base, "AAPL", TOKEN_B),
            entry(Network::Base, "RKLB", TOKEN_A),
            entry(Network::Ethereum, "RKLB", TOKEN_A),
        ])
        .unwrap();

        assert_eq!(
            config.watched_on(Network::Base),
            vec![
                WatchedWrappedToken {
                    token: TOKEN_A,
                    underlying: symbol("RKLB")
                },
                WatchedWrappedToken {
                    token: TOKEN_B,
                    underlying: symbol("AAPL")
                },
            ]
        );
        assert_eq!(
            config.watched_on(Network::Ethereum),
            vec![WatchedWrappedToken {
                token: TOKEN_A,
                underlying: symbol("RKLB")
            }]
        );
        assert!(config.watched_on(Network::HyperEvm).is_empty());

        let mut networks: Vec<Network> = config.networks().collect();
        networks.sort_unstable_by_key(Network::as_str);
        assert_eq!(networks, vec![Network::Base, Network::Ethereum]);
    }

    #[test]
    fn zero_address_is_rejected() {
        let error = WrappedTokenConfig::new([entry(
            Network::Base,
            "RKLB",
            Address::ZERO,
        )])
        .unwrap_err();

        assert_eq!(
            error,
            WrappedTokenConfigError::ZeroAddress {
                network: Network::Base,
                underlying: symbol("RKLB"),
            }
        );
    }

    /// One address cannot wrap two underlyings on one network: an inbound
    /// transfer of it could not be attributed to an asset.
    #[test]
    fn one_address_for_two_underlyings_on_a_network_is_rejected() {
        let error = WrappedTokenConfig::new([
            entry(Network::Base, "RKLB", TOKEN_A),
            entry(Network::Base, "AAPL", TOKEN_A),
        ])
        .unwrap_err();

        assert_eq!(
            error,
            WrappedTokenConfigError::AddressCollision {
                network: Network::Base,
                token: TOKEN_A,
                first: symbol("RKLB"),
                second: symbol("AAPL"),
            }
        );
    }

    #[test]
    fn two_addresses_for_one_underlying_on_a_network_is_rejected() {
        let error = WrappedTokenConfig::new([
            entry(Network::Base, "RKLB", TOKEN_A),
            entry(Network::Base, "RKLB", TOKEN_B),
        ])
        .unwrap_err();

        assert_eq!(
            error,
            WrappedTokenConfigError::DuplicateUnderlying {
                network: Network::Base,
                underlying: symbol("RKLB"),
            }
        );
    }

    fn monitor(
        harness: &TestHarness,
        asserter: &Asserter,
        watched: Vec<WatchedWrappedToken>,
    ) -> WrappedTransferMonitor<impl alloy::providers::Provider> {
        WrappedTransferMonitor {
            network: Network::Base,
            provider: ProviderBuilder::new()
                .wallet(EthereumWallet::from(PrivateKeySigner::random()))
                .connect_mocked_client(asserter.clone()),
            bot_wallet: BOT_WALLET,
            backfill_start_block: 0,
            watched,
            pool: harness.pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
            poll_interval: Duration::from_secs(60),
        }
    }

    fn watch_aapl() -> Vec<WatchedWrappedToken> {
        vec![WatchedWrappedToken { token: TOKEN_A, underlying: symbol("AAPL") }]
    }

    fn inbound_log(from: Address, amount: U256, log_index: u64) -> Log {
        create_transfer_log_with_index(
            TOKEN_A, from, BOT_WALLET, amount, TX_HASH, 120, log_index,
        )
    }

    async fn alert_job_count(harness: &TestHarness, key: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND idempotency_key = ?",
        )
        .bind(job_type::<SendLifecycleNotification>())
        .bind(key)
        .fetch_one(&harness.pool)
        .await
        .unwrap()
    }

    async fn checkpoint(harness: &TestHarness) -> Option<u64> {
        load_checkpoint_block(
            &harness.pool,
            &checkpoint_name(Network::Base, TOKEN_A),
        )
        .await
        .unwrap()
    }

    /// The core guarantee: a wrapped-token transfer into the issuer wallet is
    /// recorded, logged at ERROR with chain, asset, amount and tx, queued as
    /// a durable alert, and the token's checkpoint moves to the head.
    #[traced_test]
    #[tokio::test]
    async fn poll_records_alerts_and_checkpoints_an_inbound_transfer() {
        let harness = TestHarness::new().await;
        let amount = U256::from(5_000_000_000_000_000_000_u128);
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(SENDER, amount, 3)]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        monitor.poll_once().await.unwrap();

        let recorded =
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap();
        assert_eq!(recorded.len(), 1, "exactly one row: {recorded:?}");
        let detected_at = recorded[0].detected_at;
        assert!(
            (Utc::now() - detected_at).num_seconds().abs() < 60,
            "detected_at must be now-ish: {detected_at}"
        );
        assert_eq!(
            recorded[0],
            InboundWrappedTransfer {
                network: Network::Base,
                underlying: symbol("AAPL"),
                token: TOKEN_A,
                from: SENDER,
                amount,
                tx_hash: TX_HASH,
                log_index: 3,
                block_number: 120,
                detected_at,
            }
        );
        assert_eq!(checkpoint(&harness).await, Some(200));
        assert_eq!(
            alert_job_count(
                &harness,
                &alert_idempotency_key(Network::Base, TX_HASH, 3)
            )
            .await,
            1,
            "one durable alert job keyed by the log identity"
        );
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Inbound wrapped-token transfer",
                "network=base",
                "underlying=AAPL",
                &format!("token={TOKEN_A}"),
                &format!("from={SENDER}"),
                "amount=5000000000000000000",
                &format!("tx_hash={TX_HASH}"),
            ]
        ));
    }

    /// A re-scan (here: the head moved and the same log came back) must
    /// neither duplicate the row nor queue a second alert, and must not page
    /// the operator again: the repeat is DEBUG, not ERROR.
    #[traced_test]
    #[tokio::test]
    async fn rescanning_a_recorded_transfer_duplicates_neither_row_nor_alert() {
        let harness = TestHarness::new().await;
        let amount = U256::from(1u64);
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(SENDER, amount, 0)]);
        asserter.push_success(&U256::from(300u64));
        asserter.push_success(&vec![inbound_log(SENDER, amount, 0)]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        monitor.poll_once().await.unwrap();
        monitor.poll_once().await.unwrap();

        let recorded =
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap();
        assert_eq!(recorded.len(), 1, "one row after a re-scan: {recorded:?}");
        assert_eq!(
            alert_job_count(
                &harness,
                &alert_idempotency_key(Network::Base, TX_HASH, 0)
            )
            .await,
            1
        );
        assert_eq!(checkpoint(&harness).await, Some(300));
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[
                    "Inbound wrapped-token transfer",
                    &format!("tx_hash={TX_HASH}")
                ]
            ),
            1,
            "the operator is paged once per transfer"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["already recorded", &format!("tx_hash={TX_HASH}")]
        ));
    }

    /// A wrapper `deposit` with the issuer wallet as receiver mints straight
    /// into the wallet: `from` is the zero address, and it is just as lost as
    /// a plain transfer, so it must be recorded rather than skipped as a mint.
    #[tokio::test]
    async fn a_wrapper_deposit_straight_to_the_issuer_wallet_is_recorded() {
        let harness = TestHarness::new().await;
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(
            Address::ZERO,
            U256::from(2u64),
            0,
        )]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        monitor.poll_once().await.unwrap();

        let recorded =
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].from, Address::ZERO);
    }

    /// A log without a transaction hash cannot be identified, so it cannot be
    /// recorded or deduplicated; it is dropped with a WARN summary and the
    /// checkpoint still advances rather than freezing on it forever.
    #[traced_test]
    #[tokio::test]
    async fn an_unidentifiable_log_is_dropped_with_a_warn_and_the_checkpoint_advances()
     {
        let harness = TestHarness::new().await;
        let mut log = inbound_log(SENDER, U256::from(1u64), 0);
        log.transaction_hash = None;
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![log]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        monitor.poll_once().await.unwrap();

        assert!(
            list_inbound_wrapped_transfers(&harness.pool)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(checkpoint(&harness).await, Some(200));
        assert!(logs_contain_at!(
            Level::WARN,
            &["Dropped unidentifiable wrapped-token transfer logs", "count=1"]
        ));
    }

    /// A failed `eth_getLogs` must leave the token's checkpoint alone so the
    /// range is retried, and a pass where every token failed fails the pass.
    #[tokio::test]
    async fn a_failed_log_fetch_fails_the_pass_and_holds_the_checkpoint() {
        let harness = TestHarness::new().await;
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_failure_msg("simulated eth_getLogs failure");
        let monitor = monitor(&harness, &asserter, watch_aapl());

        let result = monitor.poll_once().await;

        assert!(
            matches!(
                result,
                Err(WrappedTransferPollError::AllTokensFailed { total: 1 })
            ),
            "got: {result:?}"
        );
        assert_eq!(checkpoint(&harness).await, None);
    }

    /// If the alert cannot be queued, the transfer is still recorded (that
    /// insert happens first) but the checkpoint must not advance: the next
    /// pass re-scans the range and queues the alert, so it is never lost.
    #[tokio::test]
    async fn a_failed_alert_enqueue_holds_the_checkpoint_for_a_retry() {
        let harness = TestHarness::new().await;
        sqlx::query("DROP TABLE Jobs").execute(&harness.pool).await.unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(SENDER, U256::from(1u64), 0)]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        let result = monitor.poll_once().await;

        assert!(
            matches!(
                result,
                Err(WrappedTransferPollError::AllTokensFailed { total: 1 })
            ),
            "got: {result:?}"
        );
        assert_eq!(
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap().len(),
            1
        );
        assert_eq!(checkpoint(&harness).await, None);
    }

    /// With nothing configured for the network the pass makes no RPC call at
    /// all: the asserter has no queued responses, so any call would fail.
    #[tokio::test]
    async fn nothing_watched_makes_no_rpc_calls() {
        let harness = TestHarness::new().await;
        let asserter = Asserter::new();
        let monitor = monitor(&harness, &asserter, Vec::new());

        monitor.poll_once().await.unwrap();
    }

    #[tokio::test]
    async fn listing_orders_newest_block_first() {
        let harness = TestHarness::new().await;
        let older = InboundWrappedTransfer {
            network: Network::Base,
            underlying: symbol("AAPL"),
            token: TOKEN_A,
            from: SENDER,
            amount: U256::from(1u64),
            tx_hash: TX_HASH,
            log_index: 0,
            block_number: 100,
            detected_at: Utc::now(),
        };
        let newer = InboundWrappedTransfer {
            block_number: 200,
            tx_hash: b256!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            ..older.clone()
        };

        assert!(
            record_inbound_wrapped_transfer(&harness.pool, &older)
                .await
                .unwrap()
        );
        assert!(
            record_inbound_wrapped_transfer(&harness.pool, &newer)
                .await
                .unwrap()
        );
        assert!(
            !record_inbound_wrapped_transfer(&harness.pool, &older)
                .await
                .unwrap(),
            "re-recording the same log identity is a no-op"
        );

        let listed =
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap();
        let blocks: Vec<u64> =
            listed.iter().map(|transfer| transfer.block_number).collect();
        assert_eq!(blocks, vec![200, 100]);
    }
}
