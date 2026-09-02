use alloy::primitives::{Address, TxHash};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use event_sorcery::Store;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, trace, warn};

use super::{
    IssuerRedemptionRequestId, Redemption,
    burn_manager::BurnManager,
    journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
    transfer::{
        RedemptionFlowCtx, TransferOutcome, TransferProcessingError,
        detect_transfer, drive_redemption_flow,
    },
};
use crate::bindings;
use crate::config::VaultModeConfig;
use crate::network_telemetry::NetworkTelemetry;
use crate::poll_checkpoint::{
    self, CheckpointError, TRANSFER_POLL, advance_transfer_poll,
    load_transfer_poll,
};
use crate::tokenized_asset::Network;
use crate::tokenized_asset::TokenizedAssetView;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};

/// Interval between polling cycles when the poller is caught up to the chain
/// head. 5 seconds is a reasonable trade-off: negligible latency (downstream
/// flows take minutes) and predictable RPC cost (~$3/month on dRPC).
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum number of blocks to query in a single `eth_getLogs` call.
/// RPCs typically limit response sizes, so we chunk large ranges.
const BLOCK_CHUNK_SIZE: u64 = 2000;

/// Interval between retries when a polling pass fails (e.g., RPC error).
const RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Consecutive failed poll passes before the per-pass WARN escalates to an ERROR
/// alarm. A single transient failure retries quietly; sustained failures mean
/// redemption detection is offline and must be operator-visible rather than
/// hidden among routine WARNs. At `RETRY_INTERVAL` (10s) this is ~30s of
/// continuous failure.
const MAX_POLL_FAILURES_BEFORE_ALARM: usize = 3;

/// Continuously polls `eth_getLogs` for Transfer events across all vaults on
/// one network.
///
/// Replaces the old dual architecture of per-vault `eth_subscribe` detectors
/// (which could silently die) and one-shot backfillers (which ran once and
/// exited). One poller runs per configured network, each with its own RPC
/// provider and its own vault set. Each polling loop:
///
/// - Covers all of its network's vaults in one RPC call per block range chunk
/// - Never misses events (every block is explicitly scanned; on error the
///   checkpoint does not advance, so the chunk is retried next pass)
/// - Fails visibly (if the call fails, we know and retry)
/// - Persists progress to the `poll_checkpoints` SQL table under the
///   per-network key `transfer_poll:{network}` (e.g. `transfer_poll:ethereum`).
///   The Base poller additionally falls back to the legacy single-chain
///   `transfer_poll` key when its per-network key is absent (see
///   `load_transfer_poll`), so upgrades resume instead of re-scanning history.
///
/// **Dynamic vaults:** the monitored set is re-read from the tokenized-asset
/// view on every pass (see [`enabled_vaults`]), so an asset added or
/// re-pointed at runtime is covered on the next poll without a restart. Each
/// vault carries its OWN checkpoint ([`poll_checkpoint::transfer_poll_name`]),
/// so a vault appearing for the first time — a runtime add, or a re-point to a
/// vault that already has on-chain history — is scanned from
/// `backfill_start_block`, catching every redemption on it rather than
/// inheriting a global cursor already past its history (which would silently
/// drop those redemptions). Vaults already monitored under the legacy global
/// checkpoint are seeded from it once at startup (see
/// [`Self::seed_per_vault_checkpoints`]) so a deploy does not re-scan them.
pub(crate) struct TransferPoller<P> {
    network: Network,
    provider: P,
    bot_wallet: Address,
    backfill_start_block: u64,
    store: Arc<Store<Redemption>>,
    pool: Pool<Sqlite>,
    redeem_call_manager: Arc<RedeemCallManager>,
    journal_manager: Arc<JournalManager>,
    burn_manager: Arc<BurnManager>,
    vault_mode_config: VaultModeConfig,
    telemetry: Arc<NetworkTelemetry>,
}

/// Configuration for constructing a [`TransferPoller`].
pub(crate) struct TransferPollerConfig<P> {
    pub(crate) network: Network,
    pub(crate) provider: P,
    pub(crate) bot_wallet: Address,
    pub(crate) backfill_start_block: u64,
    pub(crate) store: Arc<Store<Redemption>>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) redeem_call_manager: Arc<RedeemCallManager>,
    pub(crate) journal_manager: Arc<JournalManager>,
    pub(crate) burn_manager: Arc<BurnManager>,
    pub(crate) vault_mode_config: VaultModeConfig,
    pub(crate) telemetry: Arc<NetworkTelemetry>,
}

impl<P> TransferPoller<P> {
    pub(crate) fn new(config: TransferPollerConfig<P>) -> Self {
        Self {
            network: config.network,
            provider: config.provider,
            bot_wallet: config.bot_wallet,
            backfill_start_block: config.backfill_start_block,
            store: config.store,
            pool: config.pool,
            redeem_call_manager: config.redeem_call_manager,
            journal_manager: config.journal_manager,
            burn_manager: config.burn_manager,
            vault_mode_config: config.vault_mode_config,
            telemetry: config.telemetry,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferPollError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Transfer processing error: {0}")]
    TransferProcessing(#[from] TransferProcessingError),
    #[error("Checkpoint error: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("Checkpoint overflow: last_processed_block={last_processed_block}")]
    CheckpointOverflow { last_processed_block: u64 },
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error("all {total} vaults failed the poll pass")]
    AllVaultsFailed { total: usize },
}

impl<P> TransferPoller<P>
where
    P: Provider + Clone + Send + Sync,
{
    /// Runs the polling loop forever. Never returns under normal operation.
    ///
    /// On error, logs the failure and retries after `RETRY_INTERVAL`. Each
    /// vault's cursor is persisted, so no blocks are re-scanned unnecessarily.
    pub(crate) async fn run(&self) {
        // One-time migration from the legacy global checkpoint to per-vault
        // checkpoints. Non-fatal: without it a vault simply re-scans from
        // `backfill_start_block`, which is safe (redemption detection is
        // idempotent), just slower on the first deploy.
        if let Err(error) = self.seed_per_vault_checkpoints().await {
            warn!(
                target: "redemption",
                error = %error,
                "Failed to seed per-vault transfer checkpoints; affected vaults \
                 will re-scan from backfill_start_block"
            );
        }

        // A single transient poll failure is WARN-level — the loop retries from
        // the last checkpoint. But a sustained failure keeps all redemption
        // detection offline, so once failures persist past
        // `MAX_POLL_FAILURES_BEFORE_ALARM` escalate to ERROR; otherwise a long
        // outage is indistinguishable from a single blip in the logs.
        let mut consecutive_failures = 0_usize;
        loop {
            match self.poll_once().await {
                Err(error) => {
                    consecutive_failures += 1;
                    log_poll_failure(&error, consecutive_failures);
                    self.telemetry.record_transfer_poll_failure(self.network);
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }
                Ok(lag_blocks) => {
                    self.telemetry
                        .record_transfer_poll_success(self.network, lag_blocks);
                }
            }

            consecutive_failures = 0;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Seeds the per-vault transfer checkpoints from the legacy global
    /// [`TRANSFER_POLL`] checkpoint, for the legacy vaults the old single-cursor
    /// poller scanned up to that block, CONSUMING the legacy row so the
    /// migration runs at most once. Runs at startup so a deploy resumes from
    /// the global block instead of re-scanning every vault from
    /// `backfill_start_block`.
    ///
    /// Consuming the legacy row is what makes this a one-shot migration:
    /// without it, a vault added at runtime and killed before its first
    /// per-vault checkpoint write would be mistaken for a legacy vault on the
    /// next startup and seeded to the global block, silently skipping every
    /// transfer between `backfill_start_block` and that block. A crash between
    /// the delete and the seeds only costs the legacy vaults a re-scan from
    /// `backfill_start_block`, which is safe (detection is idempotent) but
    /// slower.
    ///
    /// No-op when the global checkpoint is unset (a fresh deploy, or a prior
    /// run already consumed it). Only seeds a vault that has NO per-vault
    /// checkpoint yet; a vault that already holds one — a runtime-added vault
    /// mid-scan — is skipped, so its (possibly partial) checkpoint is never
    /// jumped forward to the global block, which would silently drop the
    /// redemptions in between.
    async fn seed_per_vault_checkpoints(
        &self,
    ) -> Result<(), TransferPollError> {
        let Some(global) =
            poll_checkpoint::load_checkpoint_block(&self.pool, TRANSFER_POLL)
                .await?
        else {
            return Ok(());
        };

        let assets = list_enabled_assets(&self.pool)
            .await?
            .into_iter()
            .filter(|asset| asset.network == self.network)
            .collect::<Vec<_>>();

        // Delete the legacy row BEFORE seeding: once it is gone, no later
        // startup can re-apply the migration to vaults that did not exist at
        // migration time.
        poll_checkpoint::remove(&self.pool, TRANSFER_POLL).await?;

        for vault in enabled_vaults(&assets) {
            let name = poll_checkpoint::transfer_poll_name(self.network, vault);
            // Only migrate vaults with NO per-vault checkpoint yet — the legacy
            // vaults the old single-cursor poller scanned under the global. A
            // vault that already holds its own checkpoint (a runtime-added vault
            // mid-scan) must be left untouched: it may carry a PARTIAL
            // checkpoint below the legacy global, and `advance`'s monotonic
            // forward-jump would silently skip every redemption between its
            // cursor and the global block.
            if poll_checkpoint::load_checkpoint_block(&self.pool, &name)
                .await?
                .is_some()
            {
                continue;
            }

            poll_checkpoint::advance_checkpoint_block(
                &self.pool, &name, global,
            )
            .await?;
        }

        Ok(())
    }

    /// Runs a single poll pass: re-read this network's enabled asset set,
    /// then scan each of its vaults from its own checkpoint to the chain
    /// head. Returns the pass's lag: the worst per vault distance between the
    /// chain head and the vault's start cursor, measured before each scan so a
    /// vault whose scan fails still contributes its backlog.
    ///
    /// The asset list is loaded ONCE per pass and that same snapshot drives
    /// both the vault set and per-log asset attribution
    /// ([`super::transfer::detect_transfer`]): one snapshot per pass means a
    /// re-point mid-pass cannot turn an already-fetched log into a
    /// non-transient skip (the vault its log came from is always present in
    /// the snapshot the pass was built from).
    async fn poll_once(&self) -> Result<u64, TransferPollError> {
        // Re-read the monitored asset set every pass so assets added or
        // re-pointed at runtime are covered without a restart — scoped to
        // this poller's network, so no pass scans (or checkpoints) another
        // chain's vault addresses against this chain's RPC.
        let assets = list_enabled_assets(&self.pool)
            .await?
            .into_iter()
            .filter(|asset| asset.network == self.network)
            .collect::<Vec<_>>();
        let vaults = enabled_vaults(&assets);
        if vaults.is_empty() {
            return Ok(0);
        }

        // One `head` for the whole pass so every vault scans to a consistent
        // block.
        let head = self.provider.get_block_number().await?;

        // A per-vault failure must not starve the other vaults: each vault owns
        // its checkpoint, so a failed vault simply resumes from where it left
        // off next pass. Log per-vault failures at DEBUG and emit a single WARN
        // summary — the same resilience the receipt-backfill loop uses. But a
        // pass where EVERY vault failed is indistinguishable from redemption
        // detection being offline, so it propagates as a pass failure and
        // counts toward the WARN→ERROR escalation and `RETRY_INTERVAL` backoff
        // in `run()`. (Pass-level failures above — the view read and the head
        // fetch — still propagate, since they block every vault.)
        let mut failed_vaults: Vec<Address> = Vec::new();
        let total_vaults = vaults.len();
        let mut lag_blocks = 0_u64;

        for vault in vaults {
            // Read the start cursor before scanning so a vault that fails its
            // scan still surfaces its backlog as lag, instead of vanishing
            // when another vault succeeds and a permanently failing vault
            // reporting zero lag forever.
            let cursor = match self.start_cursor(vault).await {
                Ok(cursor) => cursor,
                Err(error) => {
                    debug!(
                        target: "redemption",
                        %vault,
                        error = %error,
                        "Failed to read vault checkpoint; will retry next pass"
                    );
                    failed_vaults.push(vault);
                    continue;
                }
            };

            lag_blocks =
                lag_blocks.max(head.saturating_add(1).saturating_sub(cursor));

            if let Err(error) =
                self.poll_vault(&assets, vault, head, cursor).await
            {
                debug!(
                    target: "redemption",
                    %vault,
                    error = %error,
                    "Failed to poll vault; will retry next pass from its \
                     checkpoint"
                );
                failed_vaults.push(vault);
            }
        }

        if !failed_vaults.is_empty() {
            warn!(
                target: "redemption",
                failed_vault_count = failed_vaults.len(),
                failed_vaults = ?failed_vaults,
                total_vaults,
                "Transfer poll pass completed with vault failures; each resumes \
                 from its checkpoint next pass"
            );

            if failed_vaults.len() == total_vaults {
                return Err(TransferPollError::AllVaultsFailed {
                    total: total_vaults,
                });
            }
        }

        Ok(lag_blocks)
    }

    /// Computes the block a vault's scan starts from: one past its persisted
    /// checkpoint, floored at `backfill_start_block` (a first-seen vault scans
    /// its full history rather than inheriting a global cursor already past
    /// it). Read up front by `poll_once` so a vault's backlog counts toward
    /// pass lag even when its scan later fails.
    async fn start_cursor(
        &self,
        vault: Address,
    ) -> Result<u64, TransferPollError> {
        match load_transfer_poll(&self.pool, self.network, vault).await? {
            None => Ok(self.backfill_start_block),
            Some(last_processed) => {
                let next = last_processed.checked_add(1).ok_or(
                    TransferPollError::CheckpointOverflow {
                        last_processed_block: last_processed,
                    },
                )?;
                Ok(next.max(self.backfill_start_block))
            }
        }
    }

    /// Scans one vault from `cursor` (its start block, from [`Self::start_cursor`])
    /// up to `head`, processing each Transfer and advancing the vault's
    /// checkpoint per chunk.
    async fn poll_vault(
        &self,
        assets: &[TokenizedAssetView],
        vault: Address,
        head: u64,
        cursor: u64,
    ) -> Result<(), TransferPollError> {
        if cursor > head {
            trace!(
                target: "redemption",
                %vault,
                network = %self.network,
                cursor,
                head,
                "Vault caught up; skipping"
            );
            return Ok(());
        }

        debug!(
            target: "redemption",
            %vault,
            network = %self.network,
            from_block = cursor,
            to_block = head,
            "Polling vault for transfer events"
        );

        for (chunk_from, chunk_to) in
            block_ranges(cursor, head, BLOCK_CHUNK_SIZE)
        {
            let logs =
                self.fetch_transfer_logs(vault, chunk_from, chunk_to).await?;

            trace!(
                target: "redemption",
                %vault,
                network = %self.network,
                chunk_from,
                chunk_to,
                logs_found = logs.len(),
                "Processed block range"
            );

            let mut dropped_tx_hashes: Vec<Option<TxHash>> = Vec::new();
            for log in &logs {
                if let ProcessedLog::DroppedNonTransient { tx_hash } =
                    self.process_log(assets, log).await?
                {
                    dropped_tx_hashes.push(tx_hash);
                }
            }

            advance_transfer_poll(&self.pool, self.network, vault, chunk_to)
                .await?;

            // The advance above moved the cursor past these transfers, making
            // the skip permanent: real user tokens in the redemption wallet
            // that will never be redeemed automatically. The per-log detail is
            // DEBUG (loop-body rule), so this per-chunk summary is the
            // operator's only signal — emitted here, not after the loop, so a
            // transient error in a later chunk cannot swallow it. Any earlier
            // `?` abort leaves this chunk's checkpoint unadvanced, so its
            // drops are re-detected on the next pass.
            if !dropped_tx_hashes.is_empty() {
                warn!(
                    target: "redemption",
                    count = dropped_tx_hashes.len(),
                    tx_hashes = ?dropped_tx_hashes,
                    "Permanently skipped non-retryable transfer logs; these transfers will not be redeemed automatically"
                );
            }
        }

        Ok(())
    }

    /// Fetches Transfer logs for one vault where topic2 (to) == bot_wallet.
    async fn fetch_transfer_logs(
        &self,
        vault: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, TransferPollError> {
        let filter = alloy::rpc::types::Filter::new()
            .address(vault)
            .event_signature(
                bindings::OffchainAssetReceiptVault::Transfer::SIGNATURE_HASH,
            )
            .topic2(self.bot_wallet.into_word())
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.provider.get_logs(&filter).await?;

        Ok(logs)
    }

    /// Processes a single Transfer log: detect, then drive the redemption
    /// flow.
    ///
    /// Returns `Err` only for transient failures (DB/RPC errors that may
    /// succeed on retry). Non-transient failures (decode errors, missing
    /// fields, no matching asset) are reported as
    /// [`ProcessedLog::DroppedNonTransient`] and skipped — retrying them
    /// would freeze the checkpoint permanently.
    async fn process_log(
        &self,
        assets: &[TokenizedAssetView],
        log: &Log,
    ) -> Result<ProcessedLog, TransferPollError> {
        let vault = log.address();

        let outcome = match detect_transfer(
            log,
            vault,
            self.network,
            assets,
            &self.store,
            &self.pool,
            &self.vault_mode_config,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(err) if err.is_non_transient() => {
                debug!(
                    target: "redemption",
                    error = %err,
                    tx_hash = ?log.transaction_hash,
                    "Skipping non-retryable transfer log"
                );
                return Ok(ProcessedLog::DroppedNonTransient {
                    tx_hash: log.transaction_hash,
                });
            }
            Err(err) => return Err(err.into()),
        };

        match outcome {
            TransferOutcome::Detected {
                issuer_request_id,
                client_id,
                alpaca_account,
            } => {
                let flow_issuer_request_id = issuer_request_id.clone();
                let flow = tokio::spawn(drive_redemption_flow(
                    issuer_request_id,
                    client_id,
                    alpaca_account,
                    RedemptionFlowCtx {
                        store: self.store.clone(),
                        redeem_call_manager: self.redeem_call_manager.clone(),
                        journal_manager: self.journal_manager.clone(),
                        burn_manager: self.burn_manager.clone(),
                    },
                ));

                tokio::spawn(watch_redemption_flow(
                    flow,
                    flow_issuer_request_id,
                    log.transaction_hash,
                ));
            }
            TransferOutcome::AlreadyDetected
            | TransferOutcome::SkippedMint
            | TransferOutcome::SkippedNoAccount
            | TransferOutcome::SkippedAdminRecovery => {}
        }

        Ok(ProcessedLog::Handled)
    }
}

/// Outcome of processing a single Transfer log: either handled (including
/// benign skips like already-detected) or permanently dropped because of a
/// non-transient decode/detection failure.
enum ProcessedLog {
    Handled,
    DroppedNonTransient { tx_hash: Option<TxHash> },
}

/// Watches a spawned redemption-flow task. A dropped `JoinHandle` swallows
/// task panics silently, so a panic must surface in logs — with the redemption
/// identity, so the operator does not have to correlate by timestamp — instead
/// of only manifesting as a stuck aggregate the next recovery sweep has to
/// clean up. Cancellation (runtime shutdown, abort) is expected teardown noise
/// and logged at DEBUG so it cannot masquerade as a panic.
async fn watch_redemption_flow(
    flow: JoinHandle<()>,
    issuer_request_id: IssuerRedemptionRequestId,
    tx_hash: Option<TxHash>,
) {
    if let Err(err) = flow.await {
        if err.is_panic() {
            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                tx_hash = ?tx_hash,
                error = ?err,
                "drive_redemption_flow task panicked"
            );
        } else {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                tx_hash = ?tx_hash,
                error = ?err,
                "drive_redemption_flow task cancelled"
            );
        }
    }
}

/// Extracts the deduped vault addresses from a per-pass asset snapshot.
/// Frozen assets stay in the set so in-flight redemptions on them are still
/// detected. Deduped so a vault shared by two enabled assets is polled once,
/// not once per asset. (Sharing a vault is a misconfiguration that
/// `find_matching_asset` rejects, but avoid the duplicate `eth_getLogs` work
/// regardless.)
fn enabled_vaults(assets: &[TokenizedAssetView]) -> Vec<Address> {
    let mut vaults: Vec<Address> =
        assets.iter().map(|asset| asset.vault).collect();
    vaults.sort_unstable();
    vaults.dedup();
    vaults
}

/// Emits the log for a failed poll pass: WARN while the failure may still be
/// a blip (the loop retries from the last checkpoint), escalating to ERROR
/// once `consecutive_failures` reaches [`MAX_POLL_FAILURES_BEFORE_ALARM`] —
/// sustained failure means redemption detection is offline and must be
/// operator-visible rather than hidden among routine WARNs.
fn log_poll_failure(error: &TransferPollError, consecutive_failures: usize) {
    if consecutive_failures >= MAX_POLL_FAILURES_BEFORE_ALARM {
        error!(
            target: "redemption",
            error = %error,
            consecutive_failures,
            retry_after_secs = RETRY_INTERVAL.as_secs(),
            "Transfer poll pass has failed repeatedly; redemption \
             detection is offline until it recovers"
        );
    } else {
        warn!(
            target: "redemption",
            error = %error,
            consecutive_failures,
            retry_after_secs = RETRY_INTERVAL.as_secs(),
            "Transfer poll pass failed; will retry from last \
             checkpoint"
        );
    }
}

// ---------------------------------------------------------------------------
// Block range chunking
// ---------------------------------------------------------------------------

/// Generates inclusive block ranges of at most `chunk_size` blocks.
fn block_ranges(
    from: u64,
    to: u64,
    chunk_size: u64,
) -> impl Iterator<Item = (u64, u64)> {
    debug_assert!(chunk_size > 0, "chunk_size must be positive");

    std::iter::successors(Some(from), move |&start| {
        let next = start + chunk_size;
        if next <= to { Some(next) } else { None }
    })
    .map(move |start| (start, (start + chunk_size - 1).min(to)))
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Address, U256, address, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::Log;
    use alloy::signers::local::PrivateKeySigner;
    use event_sorcery::{Store, StoreBuilder, test_store};
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{TransferPollError, watch_redemption_flow};
    use crate::alpaca::mock::MockAlpacaService;
    use crate::config::VaultModeConfig;
    use crate::network_telemetry::NetworkTelemetry;
    use crate::notifications::NoopLifecycleNotifier;
    use crate::poll_checkpoint::{
        self, TRANSFER_POLL, advance_transfer_poll, load_transfer_poll,
    };
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptInventory, ReceiptService,
    };
    use crate::redemption::test_utils::{
        create_transfer_log, setup_test_db_with_asset,
    };
    use crate::redemption::{
        IssuerRedemptionRequestId, Redemption, RedemptionServices,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, log_count_at, logs_contain_at};
    use crate::tokenized_asset::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;
    use st0x_issuance_dto::AssetKey;

    /// `pool` must already have migrations applied — the stores write to the
    /// `events` table on first command dispatch.
    fn setup_test_store(
        pool: &SqlitePool,
    ) -> (Arc<Store<Redemption>>, Arc<dyn ReceiptService>) {
        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        let vault_service: Arc<dyn crate::vault::VaultService> =
            Arc::new(MockVaultService::new_success());
        let store = Arc::new(test_store::<Redemption>(
            pool.clone(),
            RedemptionServices::with_single_vault(Network::Base, vault_service),
        ));
        let receipt_service: Arc<dyn ReceiptService> =
            Arc::new(CqrsReceiptService::new(receipt_store));

        (store, receipt_service)
    }

    struct TestPollerSetup<P: alloy::providers::Provider + Clone> {
        poller: super::TransferPoller<P>,
        pool: SqlitePool,
    }

    async fn setup_test_poller(
        vault: Address,
        bot_wallet: Address,
        ap_wallet: Option<Address>,
        asserter: &Asserter,
        backfill_start_block: u64,
    ) -> TestPollerSetup<impl alloy::providers::Provider + Clone> {
        let pool = setup_test_db_with_asset(vault, ap_wallet).await;
        build_poller(bot_wallet, asserter, backfill_start_block, pool)
    }

    fn build_poller(
        bot_wallet: Address,
        asserter: &Asserter,
        backfill_start_block: u64,
        pool: SqlitePool,
    ) -> TestPollerSetup<impl alloy::providers::Provider + Clone> {
        build_poller_on_network(
            Network::Base,
            bot_wallet,
            asserter,
            backfill_start_block,
            pool,
        )
    }

    fn build_poller_on_network(
        network: Network,
        bot_wallet: Address,
        asserter: &Asserter,
        backfill_start_block: u64,
        pool: SqlitePool,
    ) -> TestPollerSetup<impl alloy::providers::Provider + Clone> {
        let (store, receipt_service) = setup_test_store(&pool);

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(
            crate::redemption::redeem_call_manager::RedeemCallManager::new(
                alpaca_service.clone(),
                store.clone(),
                pool.clone(),
                Arc::new(NoopLifecycleNotifier),
            ),
        );
        let journal_manager =
            Arc::new(crate::redemption::journal_manager::JournalManager::new(
                alpaca_service,
                store.clone(),
                pool.clone(),
            ));

        let vault_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let burn_manager = Arc::new(
            crate::redemption::burn_manager::BurnManager::new_for_tests(
                vault_service,
                pool.clone(),
                store.clone(),
                receipt_service,
                bot_wallet,
                ANVIL_CHAIN_ID,
            ),
        );

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter.clone());

        let poller = super::TransferPoller::new(super::TransferPollerConfig {
            network,
            provider,
            bot_wallet,
            backfill_start_block,
            store,
            pool: pool.clone(),
            redeem_call_manager,
            journal_manager,
            burn_manager,
            vault_mode_config: VaultModeConfig::default(),
            telemetry: Arc::new(NetworkTelemetry::new([network])),
        });

        TestPollerSetup { poller, pool }
    }

    /// Adds a second enabled asset (MSFT/tMSFT) bound to `vault`, alongside
    /// the AAPL asset `setup_test_db_with_asset` seeds.
    async fn add_second_asset(pool: &SqlitePool, vault: Address) {
        let (asset_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let msft = UnderlyingSymbol::new("MSFT").unwrap();
        asset_store
            .send(
                &AssetKey::new(msft.clone(), Network::Base),
                TokenizedAssetCommand::Add {
                    underlying: msft.clone(),
                    token: TokenSymbol::new("tMSFT"),
                    network: Network::Base,
                    vault,
                },
            )
            .await
            .unwrap();
    }

    /// Adds an enabled Ethereum-network asset (TSLA/tTSLA) bound to `vault`,
    /// alongside the Base AAPL asset `setup_test_db_with_asset` seeds.
    async fn add_ethereum_asset(pool: &SqlitePool, vault: Address) {
        let (asset_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let tsla = UnderlyingSymbol::new("TSLA").unwrap();
        asset_store
            .send(
                &AssetKey::new(tsla.clone(), Network::Ethereum),
                TokenizedAssetCommand::Add {
                    underlying: tsla.clone(),
                    token: TokenSymbol::new("tTSLA"),
                    network: Network::Ethereum,
                    vault,
                },
            )
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // Block range tests
    // -----------------------------------------------------------------------

    #[test]
    fn block_ranges_single_chunk() {
        let ranges: Vec<_> = super::block_ranges(0, 100, 2000).collect();
        assert_eq!(ranges, vec![(0, 100)]);
    }

    #[test]
    fn block_ranges_exact_multiple() {
        let ranges: Vec<_> = super::block_ranges(0, 3999, 2000).collect();
        assert_eq!(ranges, vec![(0, 1999), (2000, 3999)]);
    }

    #[test]
    fn block_ranges_with_remainder() {
        let ranges: Vec<_> = super::block_ranges(0, 5000, 2000).collect();
        assert_eq!(ranges, vec![(0, 1999), (2000, 3999), (4000, 5000)]);
    }

    #[test]
    fn block_ranges_from_nonzero() {
        let ranges: Vec<_> = super::block_ranges(1000, 4500, 2000).collect();
        assert_eq!(ranges, vec![(1000, 2999), (3000, 4500)]);
    }

    #[test]
    fn block_ranges_single_block() {
        let ranges: Vec<_> = super::block_ranges(100, 100, 2000).collect();
        assert_eq!(ranges, vec![(100, 100)]);
    }

    // -----------------------------------------------------------------------
    // poll_once tests
    // -----------------------------------------------------------------------

    #[traced_test]
    #[tokio::test]
    async fn poll_detects_transfer_to_bot_wallet() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let transfer_log = create_transfer_log(
            vault, ap_wallet, bot_wallet, value, tx_hash, 100,
        );

        let asserter = Asserter::new();
        // eth_blockNumber
        asserter.push_success(&U256::from(200u64));
        // eth_getLogs
        asserter.push_success(&vec![transfer_log]);

        let setup =
            setup_test_poller(vault, bot_wallet, Some(ap_wallet), &asserter, 0)
                .await;

        setup.poller.poll_once().await.unwrap();

        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Base, vault)
                .await
                .unwrap(),
            Some(200)
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Polling vault for transfer events"]
        ));
    }

    /// Each per-network poller must scan only its OWN network's vaults: with
    /// one Base and one Ethereum asset listed, a Base pass polls exactly the
    /// Base vault and writes no checkpoint for the Ethereum vault. (The
    /// asserter deliberately queues a getLogs response for BOTH vaults — the
    /// regression this pins would consume the second one and checkpoint the
    /// Ethereum vault under the base network.)
    #[traced_test]
    #[tokio::test]
    async fn poll_once_skips_other_networks_vaults() {
        // Vault addresses unique to this test: the log buffer is global
        // across concurrently running tests, so an address another test
        // polls (on any network) could satisfy or poison the line
        // assertions below.
        let base_vault = address!("0x5555555555555555555555555555555555555555");
        let eth_vault = address!("0x6666666666666666666666666666666666666666");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let pool = setup_test_db_with_asset(base_vault, None).await;
        add_ethereum_asset(&pool, eth_vault).await;

        let asserter = Asserter::new();
        // eth_blockNumber
        asserter.push_success(&U256::from(200u64));
        // One eth_getLogs per vault the pass COULD scan.
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&Vec::<Log>::new());

        let setup = build_poller(bot_wallet, &asserter, 0, pool);
        setup.poller.poll_once().await.unwrap();

        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Base, base_vault)
                .await
                .unwrap(),
            Some(200),
            "the Base poller must checkpoint its own network's vault"
        );
        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Base, eth_vault)
                .await
                .unwrap(),
            None,
            "the Base poller must not poll or checkpoint the Ethereum vault"
        );

        // The `base` network snippet pins the line assertions to a Base
        // pass, so the claim stays "never polled on Base" rather than
        // "never polled anywhere".
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Polling vault for transfer events",
                &base_vault.to_string(),
                "base",
            ]
        ));
        assert!(
            !logs_contain_at!(
                tracing::Level::DEBUG,
                &[
                    "Polling vault for transfer events",
                    &eth_vault.to_string(),
                    "base",
                ]
            ),
            "the Ethereum vault must never appear in a Base polling pass"
        );
    }

    /// The mirror of `poll_once_skips_other_networks_vaults`: an Ethereum
    /// pass polls exactly the Ethereum vault — so the network scoping is a
    /// real filter on `self.network`, not something that happens to hold for
    /// Base.
    #[traced_test]
    #[tokio::test]
    async fn poll_once_scopes_the_ethereum_poller_to_its_own_vault() {
        // Vault addresses deliberately disjoint from the mirror test's: the
        // log buffer is global across concurrently running tests, so shared
        // addresses would let one test's polling lines satisfy (or poison)
        // the other's log assertions.
        let base_vault = address!("0x3333333333333333333333333333333333333333");
        let eth_vault = address!("0x4444444444444444444444444444444444444444");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let pool = setup_test_db_with_asset(base_vault, None).await;
        add_ethereum_asset(&pool, eth_vault).await;

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(300u64));
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&Vec::<Log>::new());

        let setup = build_poller_on_network(
            Network::Ethereum,
            bot_wallet,
            &asserter,
            0,
            pool,
        );
        setup.poller.poll_once().await.unwrap();

        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Ethereum, eth_vault)
                .await
                .unwrap(),
            Some(300),
            "the Ethereum poller must checkpoint its own network's vault"
        );
        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Ethereum, base_vault)
                .await
                .unwrap(),
            None,
            "the Ethereum poller must not poll or checkpoint the Base vault"
        );

        // Scoped by the `ethereum` network snippet for the same reason the
        // mirror test scopes by `base`.
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Polling vault for transfer events",
                &eth_vault.to_string(),
                "ethereum",
            ]
        ));
        assert!(
            !logs_contain_at!(
                tracing::Level::DEBUG,
                &[
                    "Polling vault for transfer events",
                    &base_vault.to_string(),
                    "ethereum",
                ]
            ),
            "the Base vault must never appear in an Ethereum polling pass"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_skips_mint_events() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let mint_log = create_transfer_log(
            vault,
            Address::ZERO,
            bot_wallet,
            value,
            tx_hash,
            100,
        );

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![mint_log]);

        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 0).await;

        setup.poller.poll_once().await.unwrap();

        // Checkpoint still advances even with no detections
        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Base, vault)
                .await
                .unwrap(),
            Some(200)
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Polling vault for transfer events"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_resumes_from_checkpoint() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        // eth_blockNumber returns 200 (same as checkpoint)
        asserter.push_success(&U256::from(200u64));

        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 50).await;

        // Pre-seed the vault's checkpoint at 200
        advance_transfer_poll(&setup.pool, Network::Base, vault, 200)
            .await
            .unwrap();

        // Should skip since cursor (201) > head (200)
        setup.poller.poll_once().await.unwrap();

        assert!(logs_contain_at!(tracing::Level::TRACE, &["Vault caught up"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_skips_unwhitelisted_wallets() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let unknown_wallet =
            address!("0x1111111111111111111111111111111111111111");

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );

        let transfer_log = create_transfer_log(
            vault,
            unknown_wallet,
            bot_wallet,
            value,
            tx_hash,
            100,
        );

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![transfer_log]);

        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 0).await;

        setup.poller.poll_once().await.unwrap();

        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Base, vault)
                .await
                .unwrap(),
            Some(200)
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_no_op_with_empty_vaults() {
        let asserter = Asserter::new();
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        // No tokenized asset is seeded, so the dynamic vault lookup finds none
        // and the pass short-circuits before any RPC call.
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let setup = build_poller(bot_wallet, &asserter, 0, pool);

        setup.poller.poll_once().await.unwrap();

        // No checkpoint advanced — assert the whole table is empty, not just the
        // dead global key (which is vacuously None regardless of what poll_once
        // did, since nothing writes it anymore).
        let checkpoint_rows = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM poll_checkpoints",
        )
        .fetch_one(&setup.pool)
        .await
        .unwrap();
        assert_eq!(
            checkpoint_rows, 0,
            "an empty-vault pass must not advance any per-vault checkpoint"
        );
    }

    /// A failed enabled-vault view read aborts the whole pass with
    /// `TokenizedAssetView` — the outer `run` loop then WARNs and retries — so it
    /// must surface as that error, never be swallowed or panic. Dropping the view
    /// table makes the read fail the way a corrupt or unavailable DB would.
    #[tokio::test]
    async fn poll_once_errors_when_the_asset_view_read_fails() {
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let asserter = Asserter::new();
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        // The enabled-vault read targets tokenized_asset_view; dropping it makes
        // list_enabled_assets fail like a corrupt or unavailable database.
        sqlx::query("DROP TABLE tokenized_asset_view")
            .execute(&pool)
            .await
            .unwrap();

        let setup = build_poller(bot_wallet, &asserter, 0, pool);

        let result = setup.poller.poll_once().await;

        assert!(
            matches!(result, Err(TransferPollError::TokenizedAssetView(_))),
            "a failed asset-view read must abort the pass with \
             TokenizedAssetView, got: {result:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_persists_checkpoint_after_success() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&Vec::<Log>::new());

        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 50).await;

        setup.poller.poll_once().await.unwrap();

        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Base, vault)
                .await
                .unwrap(),
            Some(200)
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Polling vault for transfer events",
                "from_block=50",
                "to_block=200"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_skips_when_checkpoint_ahead_of_chain() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(100u64));

        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 200).await;

        setup.poller.poll_once().await.unwrap();

        // No checkpoint saved since we skipped
        assert_eq!(
            load_transfer_poll(&setup.pool, Network::Base, vault)
                .await
                .unwrap(),
            None
        );

        assert!(logs_contain_at!(tracing::Level::TRACE, &["Vault caught up"]));
    }

    /// A vault with no per-vault checkpoint scans from `backfill_start_block`,
    /// NOT from the legacy global checkpoint — this is what makes a re-pointed
    /// or late-attached vault catch its pre-checkpoint redemption history
    /// instead of silently skipping it.
    #[traced_test]
    #[tokio::test]
    async fn poll_vault_scans_from_backfill_start_ignoring_global_checkpoint() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(300u64));
        asserter.push_success(&Vec::<Log>::new());

        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 50).await;

        // A leftover global checkpoint from before the per-vault migration.
        // The vault has no per-vault checkpoint, so poll_once must ignore the
        // global value and scan from backfill_start_block (50).
        poll_checkpoint::advance_checkpoint_block(
            &setup.pool,
            TRANSFER_POLL,
            200,
        )
        .await
        .unwrap();

        setup.poller.poll_once().await.unwrap();

        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &["Polling vault for transfer events", "from_block=50"]
            ),
            "a first-seen vault must scan from backfill_start_block, not the \
             global checkpoint"
        );
        assert_eq!(
            poll_checkpoint::load_checkpoint_block(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(Network::Base, vault),
            )
            .await
            .unwrap(),
            Some(300)
        );
    }

    /// The startup seed migrates a vault already monitored under the legacy
    /// global checkpoint to its per-vault checkpoint, so a deploy resumes from
    /// the global block rather than re-scanning from `backfill_start_block`.
    #[tokio::test]
    async fn seed_migrates_existing_vault_from_global_checkpoint() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        // Seed (run()'s migration step) makes no RPC calls.
        let asserter = Asserter::new();
        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 0).await;

        poll_checkpoint::advance_checkpoint_block(
            &setup.pool,
            TRANSFER_POLL,
            200,
        )
        .await
        .unwrap();

        setup.poller.seed_per_vault_checkpoints().await.unwrap();

        assert_eq!(
            poll_checkpoint::load_checkpoint_block(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(Network::Base, vault),
            )
            .await
            .unwrap(),
            Some(200),
            "an existing vault must inherit the global checkpoint, not re-scan"
        );
    }

    /// Regression for the silent-redemption-loss bug: a vault that already holds
    /// its OWN partial checkpoint (a runtime-added vault that scanned partway,
    /// then the service restarted) must NOT be advanced to the legacy global.
    /// Seeding it forward would skip every redemption between its partial cursor
    /// and the global block, since `advance` only ever moves a checkpoint
    /// forward.
    #[tokio::test]
    async fn seed_does_not_advance_a_vault_with_an_existing_checkpoint() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 0).await;

        // The vault scanned partway (block 100) before the restart...
        poll_checkpoint::advance_checkpoint_block(
            &setup.pool,
            &poll_checkpoint::transfer_poll_name(Network::Base, vault),
            100,
        )
        .await
        .unwrap();
        // ...and a stale legacy global sits far ahead at block 5000.
        poll_checkpoint::advance_checkpoint_block(
            &setup.pool,
            TRANSFER_POLL,
            5000,
        )
        .await
        .unwrap();

        setup.poller.seed_per_vault_checkpoints().await.unwrap();

        assert_eq!(
            poll_checkpoint::load_checkpoint_block(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(Network::Base, vault),
            )
            .await
            .unwrap(),
            Some(100),
            "a vault with an existing partial checkpoint must NOT be jumped \
             forward to the legacy global — that would drop redemptions in the \
             gap"
        );
    }

    /// A per-vault failure (here a failing `eth_getLogs`) must NOT abort the
    /// pass — `poll_once` records it and continues, so a single flaky vault does
    /// not starve the others: the healthy vault still advances its checkpoint
    /// to head, while the failed vault keeps no checkpoint advance and simply
    /// retries next pass from where it left off.
    #[traced_test]
    #[tokio::test]
    async fn poll_once_continues_past_a_failing_vault() {
        let vault_a = address!("0x1111111111111111111111111111111111111111");
        let vault_b = address!("0x2222222222222222222222222222222222222222");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(300u64));
        // Vaults are polled in sorted address order: vault_a's `eth_getLogs`
        // fails, vault_b's succeeds.
        asserter.push_failure_msg("simulated eth_getLogs failure");
        asserter.push_success(&Vec::<Log>::new());

        let setup =
            setup_test_poller(vault_a, bot_wallet, None, &asserter, 0).await;
        add_second_asset(&setup.pool, vault_b).await;

        // The failing vault must not propagate — the pass completes Ok.
        setup.poller.poll_once().await.unwrap();

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Transfer poll pass completed with vault failures"]
        ));

        assert_eq!(
            poll_checkpoint::load_checkpoint_block(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(Network::Base, vault_b),
            )
            .await
            .unwrap(),
            Some(300),
            "the healthy vault must advance its checkpoint to head"
        );
        assert_eq!(
            poll_checkpoint::load_checkpoint_block(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(Network::Base, vault_a),
            )
            .await
            .unwrap(),
            None,
            "a failed vault must not advance its checkpoint"
        );
    }

    /// A pass where EVERY vault failed is indistinguishable from redemption
    /// detection being offline, so `poll_once` must propagate it as a pass
    /// failure — feeding `run()`'s WARN→ERROR escalation and RETRY_INTERVAL
    /// backoff — instead of quietly returning Ok.
    #[traced_test]
    #[tokio::test]
    async fn poll_once_fails_when_every_vault_fails() {
        let vault_a = address!("0x1111111111111111111111111111111111111111");
        let vault_b = address!("0x2222222222222222222222222222222222222222");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(300u64));
        asserter.push_failure_msg("simulated eth_getLogs failure for vault_a");
        asserter.push_failure_msg("simulated eth_getLogs failure for vault_b");

        let setup =
            setup_test_poller(vault_a, bot_wallet, None, &asserter, 0).await;
        add_second_asset(&setup.pool, vault_b).await;

        let result = setup.poller.poll_once().await;

        assert!(
            matches!(
                result,
                Err(TransferPollError::AllVaultsFailed { total: 2 })
            ),
            "a pass where every vault failed must fail the pass, got: \
             {result:?}"
        );
    }

    /// The escalation policy for consecutive pass failures: WARN below
    /// `MAX_POLL_FAILURES_BEFORE_ALARM` (the loop retries quietly), ERROR at
    /// the threshold (redemption detection is offline and must be
    /// operator-visible).
    #[traced_test]
    #[test]
    fn log_poll_failure_escalates_from_warn_to_error_at_the_alarm_threshold() {
        let error = TransferPollError::AllVaultsFailed { total: 1 };

        super::log_poll_failure(&error, 1);
        super::log_poll_failure(&error, 2);

        assert_eq!(
            log_count_at!(
                tracing::Level::WARN,
                &["will retry from last checkpoint"]
            ),
            2,
            "each below-threshold failure must WARN"
        );
        assert!(
            !logs_contain_at!(tracing::Level::ERROR, &["failed repeatedly"]),
            "no ERROR before the alarm threshold is reached"
        );

        super::log_poll_failure(&error, 3);

        assert!(
            logs_contain_at!(tracing::Level::ERROR, &["failed repeatedly"]),
            "the third consecutive failure must escalate to ERROR"
        );
    }

    /// Regression for the one-shot-migration guarantee: seeding must CONSUME
    /// the legacy global checkpoint. A vault enabled AFTER the migration ran —
    /// e.g. added at runtime and killed before its first per-vault checkpoint
    /// write — must NOT be mistaken for a legacy vault on the next startup and
    /// seeded to the global block; it must scan from `backfill_start_block`.
    #[tokio::test]
    async fn seed_consumes_the_global_checkpoint() {
        let vault_a = address!("0x1111111111111111111111111111111111111111");
        let vault_b = address!("0x2222222222222222222222222222222222222222");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        let setup =
            setup_test_poller(vault_a, bot_wallet, None, &asserter, 0).await;

        poll_checkpoint::advance_checkpoint_block(
            &setup.pool,
            TRANSFER_POLL,
            200,
        )
        .await
        .unwrap();

        setup.poller.seed_per_vault_checkpoints().await.unwrap();

        assert_eq!(
            poll_checkpoint::load_checkpoint_block(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(Network::Base, vault_a),
            )
            .await
            .unwrap(),
            Some(200),
            "the legacy vault must inherit the global checkpoint"
        );
        assert_eq!(
            poll_checkpoint::load_checkpoint_block(&setup.pool, TRANSFER_POLL)
                .await
                .unwrap(),
            None,
            "the migration must consume the legacy global row"
        );

        // The crash-window case: a vault enabled after the first seeding run,
        // then a restart re-runs the seed.
        add_second_asset(&setup.pool, vault_b).await;
        setup.poller.seed_per_vault_checkpoints().await.unwrap();

        assert_eq!(
            poll_checkpoint::load_checkpoint_block(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(Network::Base, vault_b),
            )
            .await
            .unwrap(),
            None,
            "a vault added after the migration must have no checkpoint — it \
             scans from backfill_start_block, not the stale global block"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn watch_redemption_flow_logs_panic_with_identity() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let id_string = issuer_request_id.to_string();
        let flow = tokio::spawn(async { panic!("redemption flow blew up") });

        watch_redemption_flow(flow, issuer_request_id, None).await;

        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &["drive_redemption_flow task panicked", id_string.as_str()]
            ),
            "a flow panic must be logged at WARN with the redemption identity"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn watch_redemption_flow_logs_cancellation_at_debug() {
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let id_string = issuer_request_id.to_string();
        let flow = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        flow.abort();

        watch_redemption_flow(flow, issuer_request_id, None).await;

        // Scope both assertions by this test's redemption id: the log buffer
        // is global across concurrently running tests, so an unscoped negative
        // match would trip on the sibling panic test's WARN line.
        assert!(
            !logs_contain_at!(
                tracing::Level::WARN,
                &["drive_redemption_flow task panicked", id_string.as_str()]
            ),
            "cancellation must not be reported as a panic"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &["drive_redemption_flow task cancelled", id_string.as_str()]
            ),
            "cancellation must be logged at DEBUG"
        );
    }
}
