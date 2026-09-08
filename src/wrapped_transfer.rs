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

/// Wrapped-token contract addresses to watch, per network.
///
/// Each address maps to the underlying it wraps. Built from the
/// `[wrapped_tokens.<network>]` tables of the TOML config file; existence
/// implies the entries passed validation.
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
        if self.watched.is_empty() {
            return Ok(());
        }

        // One head for the whole pass so every token scans to a consistent
        // block.
        let head = self.provider.get_block_number().await?;

        // A per token failure must not starve the others: each token owns
        // its checkpoint and resumes next pass. A pass where EVERY token
        // failed is indistinguishable from the watcher being offline, so it
        // propagates and `run` backs off.
        let mut failed_tokens: Vec<Address> = Vec::new();
        for watched in &self.watched {
            if let Err(error) = self.poll_token(watched, head).await {
                debug!(
                    target: "wrapped_transfer",
                    network = %self.network,
                    token = %watched.token,
                    error = %error,
                    "Failed to poll wrapped token; will retry next pass from \
                     its checkpoint"
                );
                failed_tokens.push(watched.token);
            }
        }

        if !failed_tokens.is_empty() {
            warn!(
                target: "wrapped_transfer",
                network = %self.network,
                failed_token_count = failed_tokens.len(),
                failed_tokens = ?failed_tokens,
                total_tokens = self.watched.len(),
                "Wrapped-token transfer poll pass completed with token \
                 failures; each resumes from its checkpoint next pass"
            );

            if failed_tokens.len() == self.watched.len() {
                return Err(WrappedTransferPollError::AllTokensFailed {
                    total: self.watched.len(),
                });
            }
        }

        Ok(())
    }

    /// Scans one token from its checkpoint (or `backfill_start_block` when
    /// it has none) up to `head`, handling each log and advancing the
    /// checkpoint per chunk.
    async fn poll_token(
        &self,
        watched: &WatchedWrappedToken,
        head: u64,
    ) -> Result<(), WrappedTransferPollError> {
        let name = checkpoint_name(self.network, watched.token);

        let cursor = match load_checkpoint_block(&self.pool, &name).await? {
            None => self.backfill_start_block,
            Some(last_processed) => {
                let next = last_processed.checked_add(1).ok_or(
                    WrappedTransferPollError::CheckpointOverflow {
                        last_processed_block: last_processed,
                    },
                )?;
                next.max(self.backfill_start_block)
            }
        };

        if cursor > head {
            trace!(
                target: "wrapped_transfer",
                network = %self.network,
                token = %watched.token,
                cursor,
                head,
                "Wrapped token caught up; skipping"
            );
            return Ok(());
        }

        debug!(
            target: "wrapped_transfer",
            network = %self.network,
            token = %watched.token,
            from_block = cursor,
            to_block = head,
            "Polling wrapped token for inbound transfers"
        );

        for (chunk_from, chunk_to) in
            block_ranges(cursor, head, BLOCK_CHUNK_SIZE)
        {
            let logs = self
                .fetch_transfer_logs(watched.token, chunk_from, chunk_to)
                .await?;

            let mut dropped = 0_usize;
            for log in &logs {
                match self.handle_log(watched, log).await? {
                    HandledLog::Dropped => dropped += 1,
                    HandledLog::Recorded | HandledLog::Ignored => {}
                }
            }

            advance_checkpoint_block(&self.pool, &name, chunk_to).await?;

            // The advance above makes the drop permanent, and the per log
            // detail is DEBUG (loop-body rule), so this per chunk summary is
            // the operator's only signal.
            if dropped > 0 {
                warn!(
                    target: "wrapped_transfer",
                    network = %self.network,
                    token = %watched.token,
                    count = dropped,
                    chunk_from,
                    chunk_to,
                    "Dropped unidentifiable wrapped-token transfer logs; they \
                     cannot be recorded or alerted"
                );
            }
        }

        Ok(())
    }

    /// Fetches `Transfer` logs of `token` whose `to` is the issuer wallet.
    /// Every ERC-20 emits the same `Transfer(address,address,uint256)`, so
    /// the vault binding's event matches and decodes the wrapper's logs too.
    async fn fetch_transfer_logs(
        &self,
        token: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, WrappedTransferPollError> {
        let filter = Filter::new()
            .address(token)
            .event_signature(
                bindings::OffchainAssetReceiptVault::Transfer::SIGNATURE_HASH,
            )
            .topic2(self.bot_wallet.into_word())
            .from_block(from_block)
            .to_block(to_block);

        Ok(self.provider.get_logs(&filter).await?)
    }

    /// Records the log and queues its alert. Both steps are idempotent on the
    /// log identity, so a chunk retried after a partial failure can neither
    /// double-record nor double-alert. A log that cannot be identified (no tx
    /// hash, block number, or log index, or an undecodable payload) is
    /// `Dropped`: retrying it would freeze the checkpoint forever.
    async fn handle_log(
        &self,
        watched: &WatchedWrappedToken,
        log: &Log,
    ) -> Result<HandledLog, WrappedTransferPollError> {
        let Some(transfer) =
            identify_transfer(self.network, self.bot_wallet, watched, log)
        else {
            debug!(
                target: "wrapped_transfer",
                network = %self.network,
                token = %watched.token,
                tx_hash = ?log.transaction_hash,
                log_index = ?log.log_index,
                "Skipping unidentifiable wrapped-token transfer log"
            );
            return Ok(HandledLog::Dropped);
        };

        // ERC-20 requires a zero-value transfer to emit `Transfer` like any
        // other (EIP-20, "Transfer event"), so an unauthenticated sender can
        // emit one to the issuer wallet for the price of gas. Nothing moved,
        // so there is nothing to recover and nothing to page about.
        if transfer.amount.is_zero() {
            debug!(
                target: "wrapped_transfer",
                network = %transfer.network,
                token = %transfer.token,
                from = %transfer.from,
                tx_hash = %transfer.tx_hash,
                log_index = transfer.log_index,
                "Ignoring zero-value wrapped-token transfer to the issuer \
                 wallet; it moves nothing and needs no recovery"
            );
            return Ok(HandledLog::Ignored);
        }

        if record_inbound_wrapped_transfer(&self.pool, &transfer).await? {
            error!(
                target: "wrapped_transfer",
                network = %transfer.network,
                underlying = %transfer.underlying,
                token = %transfer.token,
                from = %transfer.from,
                amount = %transfer.amount,
                tx_hash = %transfer.tx_hash,
                log_index = transfer.log_index,
                block_number = transfer.block_number,
                "Inbound wrapped-token transfer to the issuer wallet; it \
                 cannot be redeemed automatically and needs manual recovery"
            );
        } else {
            debug!(
                target: "wrapped_transfer",
                network = %transfer.network,
                tx_hash = %transfer.tx_hash,
                log_index = transfer.log_index,
                "Inbound wrapped-token transfer already recorded; re-queueing \
                 its alert is a no-op"
            );
        }

        let key = alert_idempotency_key(
            transfer.network,
            transfer.tx_hash,
            transfer.log_index,
        );
        release_dead_lifecycle_notification_job(&self.pool, &key).await?;
        JobQueue::<SendLifecycleNotification>::new(&self.apalis_pool)
            .push_with_idempotency_key(
                SendLifecycleNotification {
                    notification:
                        LifecycleNotification::InboundWrappedTransfer {
                            network: transfer.network,
                            underlying: transfer.underlying,
                            token: transfer.token,
                            from: transfer.from,
                            amount: transfer.amount,
                            tx_hash: transfer.tx_hash,
                        },
                },
                key,
            )
            .await?;

        Ok(HandledLog::Recorded)
    }
}

/// Outcome of handling one log: recorded (and its alert queued), ignored
/// because it moves nothing, or dropped because the log cannot be identified.
enum HandledLog {
    Recorded,
    Ignored,
    Dropped,
}

/// Decodes one `Transfer` log into a record, or `None` when the log lacks
/// the fields that make up its identity, does not decode as a `Transfer`, or
/// is not a transfer of `watched.token` to the issuer wallet.
///
/// The emitter and recipient are constrained by the `eth_getLogs` filter
/// already, so re-checking them here is defense in depth: the row is labelled
/// with `watched.underlying`, which is only true of a log this token emitted,
/// and `detect_transfer` re-derives `log.address()` the same way.
fn identify_transfer(
    network: Network,
    bot_wallet: Address,
    watched: &WatchedWrappedToken,
    log: &Log,
) -> Option<InboundWrappedTransfer> {
    if log.address() != watched.token {
        return None;
    }

    let event =
        bindings::OffchainAssetReceiptVault::Transfer::decode_log(&log.inner)
            .ok()?;

    if event.to != bot_wallet {
        return None;
    }

    Some(InboundWrappedTransfer {
        network,
        underlying: watched.underlying.clone(),
        token: watched.token,
        from: event.from,
        amount: event.value,
        tx_hash: log.transaction_hash?,
        log_index: log.log_index?,
        block_number: log.block_number?,
        detected_at: Utc::now(),
    })
}

/// Records `transfer` unless its `(network, tx_hash, log_index)` identity is
/// already stored. Returns whether the row was newly inserted.
pub(crate) async fn record_inbound_wrapped_transfer(
    pool: &Pool<Sqlite>,
    transfer: &InboundWrappedTransfer,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "
        INSERT INTO inbound_wrapped_transfers (
            network,
            tx_hash,
            log_index,
            token,
            underlying,
            from_address,
            amount,
            block_number,
            detected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(network, tx_hash, log_index) DO NOTHING
        ",
    )
    .bind(transfer.network.as_str())
    .bind(format!("{:#x}", transfer.tx_hash))
    .bind(integer_column(transfer.log_index)?)
    .bind(format!("{:#x}", transfer.token))
    .bind(transfer.underlying.as_str())
    .bind(format!("{:#x}", transfer.from))
    .bind(transfer.amount.to_string())
    .bind(integer_column(transfer.block_number)?)
    .bind(transfer.detected_at.to_rfc3339())
    .execute(pool)
    .await?;

    Ok(result.rows_affected() == 1)
}

/// Converts a block number or log index into a SQLite INTEGER bind. Reported
/// as an encode error: the value is on its way into a bind parameter, so a
/// decode error would point an operator at the read path.
fn integer_column(value: u64) -> Result<i64, sqlx::Error> {
    i64::try_from(value).map_err(|error| {
        sqlx::Error::Encode(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )))
    })
}

/// Every recorded inbound wrapped-token transfer, highest block first.
pub(crate) async fn list_inbound_wrapped_transfers(
    pool: &Pool<Sqlite>,
) -> Result<Vec<InboundWrappedTransfer>, InboundWrappedTransferReadError> {
    let rows = sqlx::query_as::<
        _,
        (String, String, i64, String, String, String, String, i64, String),
    >(
        "
        SELECT
            network,
            tx_hash,
            log_index,
            token,
            underlying,
            from_address,
            amount,
            block_number,
            detected_at
        FROM inbound_wrapped_transfers
        ORDER BY block_number DESC, log_index DESC
        ",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(
            |(
                network,
                tx_hash,
                log_index,
                token,
                underlying,
                from_address,
                amount,
                block_number,
                detected_at,
            )| {
                Ok(InboundWrappedTransfer {
                    network: network.parse()?,
                    underlying: UnderlyingSymbol::new(underlying)?,
                    token: token.parse()?,
                    from: from_address.parse()?,
                    amount: amount.parse()?,
                    tx_hash: tx_hash.parse()?,
                    log_index: u64::try_from(log_index)?,
                    block_number: u64::try_from(block_number)?,
                    detected_at: DateTime::parse_from_rfc3339(&detected_at)?
                        .with_timezone(&Utc),
                })
            },
        )
        .collect()
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

    /// Every test in this module logs into one process-wide buffer that the
    /// log assertions scan, so a test asserting on its own lines needs a
    /// transaction hash that no sibling test emits.
    fn tx_hash(seed: u8) -> TxHash {
        TxHash::repeat_byte(seed)
    }

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

    fn inbound_log(
        tx_hash: TxHash,
        from: Address,
        amount: U256,
        log_index: u64,
    ) -> Log {
        create_transfer_log_with_index(
            TOKEN_A, from, BOT_WALLET, amount, tx_hash, 120, log_index,
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
        let tx = tx_hash(0xa1);
        let amount = U256::from(5_000_000_000_000_000_000_u128);
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(tx, SENDER, amount, 3)]);
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
                tx_hash: tx,
                log_index: 3,
                block_number: 120,
                detected_at,
            }
        );
        assert_eq!(checkpoint(&harness).await, Some(200));
        assert_eq!(
            alert_job_count(
                &harness,
                &alert_idempotency_key(Network::Base, tx, 3)
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
                &format!("tx_hash={tx}"),
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
        let tx = tx_hash(0xa2);
        let amount = U256::from(1u64);
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(tx, SENDER, amount, 0)]);
        asserter.push_success(&U256::from(300u64));
        asserter.push_success(&vec![inbound_log(tx, SENDER, amount, 0)]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        monitor.poll_once().await.unwrap();
        monitor.poll_once().await.unwrap();

        let recorded =
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap();
        assert_eq!(recorded.len(), 1, "one row after a re-scan: {recorded:?}");
        assert_eq!(
            alert_job_count(
                &harness,
                &alert_idempotency_key(Network::Base, tx, 0)
            )
            .await,
            1
        );
        assert_eq!(checkpoint(&harness).await, Some(300));
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &["Inbound wrapped-token transfer", &format!("tx_hash={tx}")]
            ),
            1,
            "the operator is paged once per transfer"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["already recorded", &format!("tx_hash={tx}")]
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
            tx_hash(0xa3),
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

    /// ERC-20 mandates that a zero-value transfer emits `Transfer` like any
    /// other, so anyone can emit one to the issuer wallet for the price of
    /// gas. Nothing moved and nothing needs recovery, so it must not reach
    /// the operator or the table.
    #[traced_test]
    #[tokio::test]
    async fn a_zero_value_transfer_is_ignored() {
        let harness = TestHarness::new().await;
        let tx = tx_hash(0xa6);
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(tx, SENDER, U256::ZERO, 0)]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        monitor.poll_once().await.unwrap();

        assert!(
            list_inbound_wrapped_transfers(&harness.pool)
                .await
                .unwrap()
                .is_empty(),
            "a zero-value transfer must not be recorded"
        );
        assert_eq!(
            alert_job_count(
                &harness,
                &alert_idempotency_key(Network::Base, tx, 0)
            )
            .await,
            0,
            "a zero-value transfer must not page the operator"
        );
        assert_eq!(checkpoint(&harness).await, Some(200));
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &["Inbound wrapped-token transfer", &format!("tx_hash={tx}")]
            ),
            0
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["zero-value", &format!("tx_hash={tx}")]
        ));
    }

    /// The `to` and emitter constraints live in the `eth_getLogs` filter, so
    /// a filter the provider ignores, a wrong topic index, or a stale RPC
    /// would hand back logs that belong to nobody here. Re-checking both
    /// against the log itself keeps such a log out of the table, the way the
    /// redemption poller re-derives `log.address()` before it acts.
    #[traced_test]
    #[tokio::test]
    async fn a_log_from_another_token_or_to_another_wallet_is_dropped() {
        let harness = TestHarness::new().await;
        let other_wallet =
            address!("0x1234123412341234123412341234123412341234");
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![
            create_transfer_log_with_index(
                TOKEN_B,
                SENDER,
                BOT_WALLET,
                U256::from(1u64),
                tx_hash(0xa7),
                120,
                0,
            ),
            create_transfer_log_with_index(
                TOKEN_A,
                SENDER,
                other_wallet,
                U256::from(1u64),
                tx_hash(0xa8),
                120,
                1,
            ),
        ]);
        let monitor = monitor(&harness, &asserter, watch_aapl());

        monitor.poll_once().await.unwrap();

        assert!(
            list_inbound_wrapped_transfers(&harness.pool)
                .await
                .unwrap()
                .is_empty(),
            "neither log belongs to the watched token and wallet"
        );
        assert_eq!(checkpoint(&harness).await, Some(200));
        assert!(logs_contain_at!(
            Level::WARN,
            &["Dropped unidentifiable wrapped-token transfer logs", "count=2"]
        ));
    }

    /// A log without a transaction hash cannot be identified, so it cannot be
    /// recorded or deduplicated; it is dropped with a WARN summary and the
    /// checkpoint still advances rather than freezing on it forever.
    #[traced_test]
    #[tokio::test]
    async fn an_unidentifiable_log_is_dropped_with_a_warn_and_the_checkpoint_advances()
     {
        let harness = TestHarness::new().await;
        let mut log = inbound_log(tx_hash(0xa4), SENDER, U256::from(1u64), 0);
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
        asserter.push_success(&vec![inbound_log(
            tx_hash(0xa5),
            SENDER,
            U256::from(1u64),
            0,
        )]);
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
