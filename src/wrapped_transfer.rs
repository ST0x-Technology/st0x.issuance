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
//! The scan follows the chain head with no confirmation depth, like the
//! redemption transfer poller: a log read from a block that is later reorged
//! out leaves a recorded row and a delivered page for a transfer that no
//! longer exists, and a transaction re-mined above the advanced checkpoint
//! with a different log index is recorded again under its new identity. Both
//! are false positives an operator resolves by looking at the chain, which is
//! the right way round for a backstop whose failure mode must not be a missed
//! transfer.
//!
//! Alert dedup is durable: the lifecycle notification is queued under an
//! idempotency key derived from the log identity, exactly as the
//! corporate-action notifications are, so a restart or a re-scan never
//! re-alerts a transfer whose alert was delivered. Unlike the recurring
//! producers that pattern comes from, this one derives each key once: the
//! dead-job release before the push only covers a chunk retried because it
//! failed before its checkpoint advanced. Past that point the log is never
//! handled again, so a delivery that exhausts its retries is not re-queued
//! and the recorded row, the ERROR log, and `GET /admin/wrapped-transfers`
//! are what remains of it.

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
use std::collections::hash_map::Entry;
use std::num::TryFromIntError;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

use crate::bindings;
use crate::jobs::{JobQueue, QueuePushError};
use crate::network_telemetry::NetworkTelemetry;
use crate::notifications::{
    LifecycleNotification, SendLifecycleNotification,
    release_dead_lifecycle_notification_job,
};
use crate::poll_checkpoint::{
    CheckpointError, advance_checkpoint_block, load_checkpoint_block,
};
use crate::redemption::poller::{BLOCK_CHUNK_SIZE, block_ranges};
use crate::tokenized_asset::view::list_enabled_assets;
use crate::tokenized_asset::{Network, UnderlyingSymbol};

/// Interval between polling passes once a watcher is caught up. Inbound
/// wrapped-token transfers are rare and the alert is not latency critical, so
/// one `eth_getLogs` per token per minute is plenty.
pub(crate) const WRAPPED_TRANSFER_POLL_INTERVAL: Duration =
    Duration::from_secs(60);

/// Interval between retries when a polling pass fails (e.g. RPC error).
const RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Consecutive failed poll passes before the per-pass WARN escalates to an
/// ERROR alarm, matching the transfer poller. A blip retries quietly; a
/// sustained failure means the backstop is offline and every inbound
/// wrapped-token transfer in the gap goes unseen until it recovers.
const MAX_POLL_FAILURES_BEFORE_ALARM: usize = 3;

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

            match tokens.entry(token) {
                Entry::Occupied(occupied) => {
                    return Err(WrappedTokenConfigError::AddressCollision {
                        network,
                        token,
                        first: occupied.get().clone(),
                        second: underlying,
                    });
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(underlying);
                }
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
        self.per_network.keys().copied()
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
    /// Never empty: a chain with nothing left to watch runs no monitor.
    pub(crate) watched: Vec<WatchedWrappedToken>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) apalis_pool: ApalisSqlitePool,
    pub(crate) telemetry: Arc<NetworkTelemetry>,
    pub(crate) poll_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WrappedTransferPollError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Failed to record the inbound wrapped-token transfer: {0}")]
    Record(#[from] RecordInboundWrappedTransferError),
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

        let mut consecutive_failures = 0_usize;
        loop {
            match self.poll_once().await {
                Err(error) => {
                    consecutive_failures += 1;
                    log_poll_failure(
                        self.network,
                        &error,
                        consecutive_failures,
                    );
                    self.telemetry
                        .record_wrapped_transfer_poll_failure(self.network);
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }
                Ok(lag_blocks) => {
                    self.telemetry.record_wrapped_transfer_poll_success(
                        self.network,
                        lag_blocks,
                    );
                }
            }

            consecutive_failures = 0;
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// One pass: scan every watched token from its checkpoint to one shared
    /// chain head. A per token failure does not starve the others, since each
    /// token owns its checkpoint and resumes next pass; a pass where every
    /// token failed propagates, since that is indistinguishable from the
    /// watcher being offline.
    ///
    /// Returns the worst per token distance between the head and the token's
    /// checkpoint at the start of the pass, which telemetry reports as the
    /// watcher's block lag.
    async fn poll_once(&self) -> Result<u64, WrappedTransferPollError> {
        // One head for the whole pass so every token scans to a consistent
        // block.
        let head = self.provider.get_block_number().await?;

        let mut failed_tokens: Vec<Address> = Vec::new();
        let mut lag_blocks = 0_u64;
        for watched in &self.watched {
            match self.poll_token(watched, head).await {
                Ok(token_lag) => lag_blocks = lag_blocks.max(token_lag),
                Err(error) => {
                    debug!(
                        target: "wrapped_transfer",
                        network = %self.network,
                        token = %watched.token,
                        error = %error,
                        "Failed to poll wrapped token; will retry next pass \
                         from its checkpoint"
                    );
                    failed_tokens.push(watched.token);
                }
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

        Ok(lag_blocks)
    }

    /// Scans one token from its checkpoint (or `backfill_start_block` when
    /// it has none) up to `head`, handling each log and advancing the
    /// checkpoint per chunk.
    async fn poll_token(
        &self,
        watched: &WatchedWrappedToken,
        head: u64,
    ) -> Result<u64, WrappedTransferPollError> {
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
            return Ok(0);
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

            let mut dropped_tx_hashes: Vec<Option<TxHash>> = Vec::new();
            for log in &logs {
                match self.handle_log(watched, log).await? {
                    HandledLog::Dropped { tx_hash } => {
                        dropped_tx_hashes.push(tx_hash);
                    }
                    HandledLog::Recorded | HandledLog::Ignored => {}
                }
            }

            advance_checkpoint_block(&self.pool, &name, chunk_to).await?;

            // The advance above makes the drop permanent, and the per log
            // detail is DEBUG (loop-body rule), so this per chunk summary is
            // the operator's only signal.
            if !dropped_tx_hashes.is_empty() {
                warn!(
                    target: "wrapped_transfer",
                    network = %self.network,
                    token = %watched.token,
                    count = dropped_tx_hashes.len(),
                    tx_hashes = ?dropped_tx_hashes,
                    chunk_from,
                    chunk_to,
                    "Dropped unidentifiable wrapped-token transfer logs; they \
                     cannot be recorded or alerted"
                );
            }
        }

        Ok(head.saturating_sub(cursor))
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
        let transfer = match identify_transfer(
            self.network,
            self.bot_wallet,
            watched,
            log,
        ) {
            Ok(transfer) => transfer,
            Err(reason) => {
                debug!(
                    target: "wrapped_transfer",
                    network = %self.network,
                    token = %watched.token,
                    tx_hash = ?log.transaction_hash,
                    log_index = ?log.log_index,
                    reason = %reason,
                    "Skipping unidentifiable wrapped-token transfer log"
                );
                return Ok(HandledLog::Dropped {
                    tx_hash: log.transaction_hash,
                });
            }
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
        // Only a chunk that failed before its checkpoint advanced brings the
        // same key back here; releasing a dead delivery lets that retry queue
        // the alert instead of colliding with the corpse of the first one.
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
/// A dropped log carries what identity it had, since the drop is permanent
/// once the chunk is checkpointed and the summary WARN is all the operator
/// gets.
enum HandledLog {
    Recorded,
    Ignored,
    Dropped { tx_hash: Option<TxHash> },
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
) -> Result<InboundWrappedTransfer, UnidentifiableLog> {
    if log.address() != watched.token {
        return Err(UnidentifiableLog::OtherEmitter { emitter: log.address() });
    }

    let event =
        bindings::OffchainAssetReceiptVault::Transfer::decode_log(&log.inner)?;

    if event.to != bot_wallet {
        return Err(UnidentifiableLog::OtherRecipient { recipient: event.to });
    }

    Ok(InboundWrappedTransfer {
        network,
        underlying: watched.underlying.clone(),
        token: watched.token,
        from: event.from,
        amount: event.value,
        tx_hash: log.transaction_hash.ok_or(UnidentifiableLog::NoTxHash)?,
        log_index: log.log_index.ok_or(UnidentifiableLog::NoLogIndex)?,
        block_number: log
            .block_number
            .ok_or(UnidentifiableLog::NoBlockNumber)?,
        detected_at: Utc::now(),
    })
}

/// Why a log cannot become an [`InboundWrappedTransfer`]. Each one is
/// permanent for that log, so the reason is what the operator gets instead of
/// a retry.
#[derive(Debug, thiserror::Error)]
enum UnidentifiableLog {
    #[error("log was not emitted by the watched token but by {emitter}")]
    OtherEmitter { emitter: Address },
    #[error("recipient is not the issuer wallet but {recipient}")]
    OtherRecipient { recipient: Address },
    #[error("log does not decode as an ERC-20 Transfer: {0}")]
    Undecodable(#[from] alloy::sol_types::Error),
    #[error("log has no transaction hash")]
    NoTxHash,
    #[error("log has no log index")]
    NoLogIndex,
    #[error("log has no block number")]
    NoBlockNumber,
}

/// Records `transfer` unless its `(network, tx_hash, log_index)` identity is
/// already stored. Returns whether the row was newly inserted.
pub(crate) async fn record_inbound_wrapped_transfer(
    pool: &Pool<Sqlite>,
    transfer: &InboundWrappedTransfer,
) -> Result<bool, RecordInboundWrappedTransferError> {
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
    .bind(i64::try_from(transfer.log_index)?)
    .bind(format!("{:#x}", transfer.token))
    .bind(transfer.underlying.as_str())
    .bind(format!("{:#x}", transfer.from))
    .bind(transfer.amount.to_string())
    .bind(i64::try_from(transfer.block_number)?)
    .bind(transfer.detected_at.to_rfc3339())
    .execute(pool)
    .await?;

    Ok(result.rows_affected() == 1)
}

/// A transfer could not be written to `inbound_wrapped_transfers`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordInboundWrappedTransferError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("block number or log index exceeds SQLite INTEGER range: {0}")]
    Int(#[from] TryFromIntError),
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
fn checkpoint_name(network: Network, token: Address) -> String {
    format!("wrapped_transfer_poll:{network}:{token:#x}")
}

/// The tokens a network's watcher may actually scan, announced at INFO so an
/// operator can see what is watched without reading the config file.
///
/// A wrapped-token address that is also an enabled asset's vault on this
/// network would make every genuine redemption transfer look like an
/// un-redeemable inbound one, so it is refused rather than watched. A failed
/// asset read leaves the list alone: an unverified watch still detects real
/// transfers, while dropping the list would disable the backstop over a
/// database blip.
pub(crate) async fn watchable_tokens(
    pool: &Pool<Sqlite>,
    network: Network,
    watched: Vec<WatchedWrappedToken>,
) -> Vec<WatchedWrappedToken> {
    let vaults = match list_enabled_assets(pool).await {
        Ok(assets) => assets
            .into_iter()
            .filter(|asset| asset.network == network)
            .map(|asset| asset.vault)
            .collect(),
        Err(error) => {
            warn!(
                target: "wrapped_transfer",
                %network,
                error = %error,
                "Could not read the enabled assets to check the configured \
                 wrapped tokens against their vaults; watching them unchecked"
            );
            Vec::new()
        }
    };

    let watchable: Vec<WatchedWrappedToken> = watched
        .into_iter()
        .filter(|candidate| {
            if vaults.contains(&candidate.token) {
                error!(
                    target: "wrapped_transfer",
                    %network,
                    token = %candidate.token,
                    underlying = %candidate.underlying,
                    "Configured wrapped token is an enabled asset's vault on \
                     this network; refusing to watch it, since every \
                     redemption transfer would be recorded and paged as an \
                     un-redeemable inbound transfer"
                );
                return false;
            }

            true
        })
        .collect();

    info!(
        target: "wrapped_transfer",
        %network,
        tokens = ?watchable,
        "Watching wrapped tokens for inbound transfers to the issuer wallet"
    );

    watchable
}

/// Emits the log for a failed poll pass: WARN while the failure may still be
/// a blip, escalating to ERROR once `consecutive_failures` reaches
/// [`MAX_POLL_FAILURES_BEFORE_ALARM`], where the backstop is offline and an
/// inbound wrapped-token transfer can land unseen.
fn log_poll_failure(
    network: Network,
    error: &WrappedTransferPollError,
    consecutive_failures: usize,
) {
    if consecutive_failures >= MAX_POLL_FAILURES_BEFORE_ALARM {
        error!(
            target: "wrapped_transfer",
            %network,
            error = %error,
            consecutive_failures,
            retry_after_secs = RETRY_INTERVAL.as_secs(),
            "Wrapped-token transfer poll pass has failed repeatedly; inbound \
             wrapped-token transfers are undetected until it recovers"
        );
    } else {
        warn!(
            target: "wrapped_transfer",
            %network,
            error = %error,
            consecutive_failures,
            retry_after_secs = RETRY_INTERVAL.as_secs(),
            "Wrapped-token transfer poll pass failed; will retry from the \
             last checkpoint"
        );
    }
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
    use alloy::providers::mock::Asserter;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::types::Log;
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        InboundWrappedTransfer, InboundWrappedTransferReadError,
        WatchedWrappedToken, WrappedTokenConfig, WrappedTokenConfigError,
        WrappedTokenEntry, WrappedTransferMonitor, WrappedTransferPollError,
        alert_idempotency_key, checkpoint_name, list_inbound_wrapped_transfers,
        record_inbound_wrapped_transfer, watchable_tokens,
    };
    use crate::jobs::job_type;
    use crate::mint::test_utils::TestHarness;
    use crate::network_telemetry::NetworkTelemetry;
    use crate::notifications::SendLifecycleNotification;
    use crate::poll_checkpoint::load_checkpoint_block;
    use crate::redemption::test_utils::create_transfer_log_with_index;
    use crate::test_utils::{log_count_at, logs_contain_at};
    use crate::tokenized_asset::view::list_enabled_assets;
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
    ) -> WrappedTransferMonitor<impl Provider> {
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
            telemetry: Arc::new(NetworkTelemetry::new([Network::Base])),
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
        token_checkpoint(harness, TOKEN_A).await
    }

    async fn token_checkpoint(
        harness: &TestHarness,
        token: Address,
    ) -> Option<u64> {
        load_checkpoint_block(
            &harness.pool,
            &checkpoint_name(Network::Base, token),
        )
        .await
        .unwrap()
    }

    /// The checkpoint moves to the pass head rather than the log's block:
    /// every log in the chunk was handled, so re-scanning the gap could only
    /// re-derive rows the identity key already holds.
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
            &[
                "Dropped unidentifiable wrapped-token transfer logs",
                "count=2",
                &format!("{:?}", tx_hash(0xa7)),
                &format!("{:?}", tx_hash(0xa8)),
            ]
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["reason=", "log was not emitted by the watched token"]
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["reason=", "recipient is not the issuer wallet"]
        ));
    }

    /// The escalation policy for consecutive pass failures: WARN below
    /// `MAX_POLL_FAILURES_BEFORE_ALARM` (the loop retries quietly), ERROR at
    /// the threshold, where the backstop is offline and every inbound
    /// wrapped-token transfer in the gap goes unseen until it recovers.
    #[traced_test]
    #[test]
    fn log_poll_failure_escalates_from_warn_to_error_at_the_alarm_threshold() {
        let error = WrappedTransferPollError::AllTokensFailed { total: 1 };

        super::log_poll_failure(Network::Base, &error, 1);
        super::log_poll_failure(Network::Base, &error, 2);

        assert_eq!(
            log_count_at!(
                Level::WARN,
                &["will retry from the last checkpoint"]
            ),
            2,
            "each below-threshold failure must WARN"
        );
        assert!(
            !logs_contain_at!(Level::ERROR, &["failed repeatedly"]),
            "no ERROR before the alarm threshold is reached"
        );

        super::log_poll_failure(Network::Base, &error, 3);

        assert!(
            logs_contain_at!(
                Level::ERROR,
                &["failed repeatedly", "consecutive_failures=3"]
            ),
            "the third consecutive failure must escalate to ERROR"
        );
    }

    /// A vault address configured as a wrapped token would turn every genuine
    /// redemption transfer into an un-redeemable inbound one: recorded, paged,
    /// and impossible to clear. The watcher refuses to watch it.
    #[traced_test]
    #[tokio::test]
    async fn an_enabled_vault_is_refused_as_a_wrapped_token() {
        let harness = TestHarness::new().await;
        harness.setup_account_and_asset().await;
        let vault = list_enabled_assets(&harness.pool).await.unwrap()[0].vault;

        let watchable = watchable_tokens(
            &harness.pool,
            Network::Base,
            vec![
                WatchedWrappedToken {
                    token: vault,
                    underlying: symbol("AAPL"),
                },
                WatchedWrappedToken {
                    token: TOKEN_A,
                    underlying: symbol("RKLB"),
                },
            ],
        )
        .await;

        assert_eq!(
            watchable,
            vec![WatchedWrappedToken {
                token: TOKEN_A,
                underlying: symbol("RKLB")
            }],
            "the vault must not be watched"
        );
        assert!(logs_contain_at!(
            Level::ERROR,
            &["is an enabled asset's vault", &format!("token={vault}")]
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
    #[traced_test]
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
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "poll pass completed with token failures",
                "failed_token_count=1",
                "total_tokens=1",
            ]
        ));
    }

    /// If the alert cannot be queued, the transfer is still recorded (that
    /// insert happens first) but the checkpoint must not advance: the next
    /// pass re-scans the range and queues the alert, so it is never lost.
    #[traced_test]
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
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "poll pass completed with token failures",
                "failed_token_count=1",
            ]
        ));
    }

    /// One token's `eth_getLogs` failing must not starve its siblings: the
    /// healthy token still records and checkpoints, the pass stays `Ok`, and
    /// only the failing token resumes from its own checkpoint next pass.
    #[traced_test]
    #[tokio::test]
    async fn one_failing_token_leaves_the_others_polling() {
        let harness = TestHarness::new().await;
        let tx = tx_hash(0xa9);
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![inbound_log(
            tx,
            SENDER,
            U256::from(3u64),
            0,
        )]);
        asserter.push_failure_msg("simulated eth_getLogs failure");
        let monitor = monitor(
            &harness,
            &asserter,
            vec![
                WatchedWrappedToken {
                    token: TOKEN_A,
                    underlying: symbol("AAPL"),
                },
                WatchedWrappedToken {
                    token: TOKEN_B,
                    underlying: symbol("RKLB"),
                },
            ],
        );

        monitor.poll_once().await.unwrap();

        assert_eq!(
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap().len(),
            1,
            "the healthy token still records its transfer"
        );
        assert_eq!(token_checkpoint(&harness, TOKEN_A).await, Some(200));
        assert_eq!(
            token_checkpoint(&harness, TOKEN_B).await,
            None,
            "the failing token holds its checkpoint for a retry"
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "poll pass completed with token failures",
                "failed_token_count=1",
                "total_tokens=2",
            ]
        ));
    }

    #[tokio::test]
    async fn listing_orders_by_block_then_log_index_and_repeats_are_no_ops() {
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
            log_index: 1,
            // The largest value an ERC-20 can move, to prove the decimal
            // string in the TEXT column round-trips without loss.
            amount: U256::MAX,
            tx_hash: b256!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            ..older.clone()
        };
        // Same block as `newer`, lower log index: several inbound transfers
        // in one block is exactly what the tie-break orders.
        let same_block = InboundWrappedTransfer {
            block_number: 200,
            log_index: 0,
            tx_hash: b256!(
                "0x3333333333333333333333333333333333333333333333333333333333333333"
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
            record_inbound_wrapped_transfer(&harness.pool, &same_block)
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
        let block_and_log_index: Vec<(u64, u64)> = listed
            .iter()
            .map(|transfer| (transfer.block_number, transfer.log_index))
            .collect();
        assert_eq!(block_and_log_index, vec![(200, 1), (200, 0), (100, 0)]);
        assert_eq!(listed[0].amount, U256::MAX, "amount round-trips exactly");
    }

    /// A row that cannot be parsed back fails the whole listing rather than
    /// being skipped: the endpoint exists to show an operator every transfer
    /// awaiting manual recovery, and a listing silently missing one is worse
    /// than a listing that reports it is broken.
    #[tokio::test]
    async fn one_unparsable_row_fails_the_whole_listing() {
        let harness = TestHarness::new().await;
        sqlx::query(
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
            VALUES ('mars', ?, 0, ?, 'AAPL', ?, '1', 100, ?)
            ",
        )
        .bind(format!("{TX_HASH:#x}"))
        .bind(format!("{TOKEN_A:#x}"))
        .bind(format!("{SENDER:#x}"))
        .bind(Utc::now().to_rfc3339())
        .execute(&harness.pool)
        .await
        .unwrap();

        let error =
            list_inbound_wrapped_transfers(&harness.pool).await.unwrap_err();

        assert!(
            matches!(error, InboundWrappedTransferReadError::Network(_)),
            "got: {error:?}"
        );
    }
}
