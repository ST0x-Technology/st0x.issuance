use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use event_sorcery::Store;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, trace, warn};

use super::{
    Redemption,
    burn_manager::BurnManager,
    journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
    transfer::{
        RedemptionFlowCtx, TransferOutcome, TransferProcessingError,
        detect_transfer, drive_redemption_flow,
    },
};
use crate::bindings;
use crate::poll_checkpoint::{self, CheckpointError, TRANSFER_POLL};
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

/// Continuously polls `eth_getLogs` for Transfer events across all vaults.
///
/// Replaces the old dual architecture of per-vault `eth_subscribe` detectors
/// (which could silently die) and one-shot backfillers (which ran once and
/// exited). This single polling loop:
///
/// - Covers all vaults in one RPC call per block range chunk
/// - Never misses events (every block is explicitly scanned; on error the
///   checkpoint does not advance, so the chunk is retried next pass)
/// - Fails visibly (if the call fails, we know and retry)
/// - Persists progress to the `poll_checkpoints` SQL table
///
/// **Dynamic vaults:** the monitored set is re-read from the tokenized-asset
/// view on every pass (see [`Self::enabled_vaults`]), so an asset added or
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
    provider: P,
    bot_wallet: Address,
    backfill_start_block: u64,
    store: Arc<Store<Redemption>>,
    pool: Pool<Sqlite>,
    redeem_call_manager: Arc<RedeemCallManager>,
    journal_manager: Arc<JournalManager>,
    burn_manager: Arc<BurnManager>,
}

/// Configuration for constructing a [`TransferPoller`].
pub(crate) struct TransferPollerConfig<P> {
    pub(crate) provider: P,
    pub(crate) bot_wallet: Address,
    pub(crate) backfill_start_block: u64,
    pub(crate) store: Arc<Store<Redemption>>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) redeem_call_manager: Arc<RedeemCallManager>,
    pub(crate) journal_manager: Arc<JournalManager>,
    pub(crate) burn_manager: Arc<BurnManager>,
}

impl<P> TransferPoller<P> {
    pub(crate) fn new(config: TransferPollerConfig<P>) -> Self {
        Self {
            provider: config.provider,
            bot_wallet: config.bot_wallet,
            backfill_start_block: config.backfill_start_block,
            store: config.store,
            pool: config.pool,
            redeem_call_manager: config.redeem_call_manager,
            journal_manager: config.journal_manager,
            burn_manager: config.burn_manager,
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
            if let Err(error) = self.poll_once().await {
                consecutive_failures += 1;
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
                tokio::time::sleep(RETRY_INTERVAL).await;
                continue;
            }

            consecutive_failures = 0;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Seeds the per-vault transfer checkpoints from the legacy global
    /// [`TRANSFER_POLL`] checkpoint, for the legacy vaults the old single-cursor
    /// poller scanned up to that block. Runs once at startup so a deploy resumes
    /// from the global block instead of re-scanning every vault from
    /// `backfill_start_block`.
    ///
    /// No-op when the global checkpoint is unset (a fresh deploy). Only seeds a
    /// vault that has NO per-vault checkpoint yet; a vault that already holds one
    /// — a runtime-added vault mid-scan, or one already migrated — is skipped, so
    /// its (possibly partial) checkpoint is never jumped forward to the global
    /// block, which would silently drop the redemptions in between.
    async fn seed_per_vault_checkpoints(
        &self,
    ) -> Result<(), TransferPollError> {
        let Some(global) =
            poll_checkpoint::load(&self.pool, TRANSFER_POLL).await?
        else {
            return Ok(());
        };

        for vault in self.enabled_vaults().await? {
            let name = poll_checkpoint::transfer_poll_name(vault);
            // Only migrate vaults with NO per-vault checkpoint yet — the legacy
            // vaults the old single-cursor poller scanned under the global. A
            // vault that already holds its own checkpoint (a runtime-added vault
            // mid-scan, or one already migrated) must be left untouched: it may
            // carry a PARTIAL checkpoint below the legacy global, and
            // `advance`'s monotonic forward-jump would silently skip every
            // redemption between its cursor and the global block.
            if poll_checkpoint::load(&self.pool, &name).await?.is_some() {
                continue;
            }

            poll_checkpoint::advance(&self.pool, &name, global).await?;
        }

        Ok(())
    }

    /// Runs a single poll pass: re-read the enabled vault set, then scan each
    /// vault from its own checkpoint to the chain head.
    async fn poll_once(&self) -> Result<(), TransferPollError> {
        // Re-read the monitored vault set every pass so assets added or
        // re-pointed at runtime are covered without a restart.
        let vaults = self.enabled_vaults().await?;
        if vaults.is_empty() {
            return Ok(());
        }

        // One `head` for the whole pass so every vault scans to a consistent
        // block.
        let head = self.provider.get_block_number().await?;

        // A per-vault failure must not starve the other vaults: each vault owns
        // its checkpoint, so a failed vault simply resumes from where it left
        // off next pass. Log per-vault failures at DEBUG and emit a single WARN
        // summary — the same resilience the receipt-backfill loop uses. (Pass-
        // level failures above — the view read and the head fetch — still
        // propagate, since they block every vault.)
        let mut failed_vaults: Vec<Address> = Vec::new();
        let total_vaults = vaults.len();

        for vault in vaults {
            if let Err(error) = self.poll_vault(vault, head).await {
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
        }

        Ok(())
    }

    /// Scans one vault from its per-vault checkpoint (or `backfill_start_block`
    /// when it has none — a runtime-added or re-pointed vault) up to `head`,
    /// processing each Transfer and advancing the vault's checkpoint per chunk.
    /// Per-vault checkpoints are why a first-seen vault scans its full history
    /// instead of inheriting a global cursor already past it.
    async fn poll_vault(
        &self,
        vault: Address,
        head: u64,
    ) -> Result<(), TransferPollError> {
        let checkpoint_name = poll_checkpoint::transfer_poll_name(vault);
        let last_processed =
            poll_checkpoint::load(&self.pool, &checkpoint_name).await?;

        let cursor = match last_processed {
            None => self.backfill_start_block,
            Some(last_processed) => {
                let next = last_processed.checked_add(1).ok_or(
                    TransferPollError::CheckpointOverflow {
                        last_processed_block: last_processed,
                    },
                )?;
                next.max(self.backfill_start_block)
            }
        };

        if cursor > head {
            trace!(
                target: "redemption",
                %vault,
                cursor,
                head,
                "Vault caught up; skipping"
            );
            return Ok(());
        }

        debug!(
            target: "redemption",
            %vault,
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
                chunk_from,
                chunk_to,
                logs_found = logs.len(),
                "Processed block range"
            );

            for log in &logs {
                self.process_log(log).await?;
            }

            poll_checkpoint::advance(&self.pool, &checkpoint_name, chunk_to)
                .await?;
        }

        Ok(())
    }

    /// Reads the currently-enabled (and frozen) vault addresses from the
    /// tokenized-asset view. Frozen assets stay in the set so in-flight
    /// redemptions on them are still detected.
    async fn enabled_vaults(&self) -> Result<Vec<Address>, TransferPollError> {
        let mut vaults: Vec<Address> = list_enabled_assets(&self.pool)
            .await?
            .into_iter()
            .map(|asset| asset.vault)
            .collect();
        // Dedupe so a vault shared by two enabled assets is polled once, not
        // once per asset. (Sharing a vault is a misconfiguration that
        // `find_matching_asset` rejects, but avoid the duplicate `eth_getLogs`
        // work regardless.)
        vaults.sort_unstable();
        vaults.dedup();
        Ok(vaults)
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
    /// fields, no matching asset) are logged and skipped — retrying them
    /// would freeze the checkpoint permanently.
    async fn process_log(&self, log: &Log) -> Result<(), TransferPollError> {
        let vault = log.address();

        let outcome =
            match detect_transfer(log, vault, &self.store, &self.pool).await {
                Ok(outcome) => outcome,
                Err(err) if err.is_non_transient() => {
                    warn!(
                        target: "redemption",
                        error = %err,
                        tx_hash = ?log.transaction_hash,
                        "Skipping non-retryable transfer log"
                    );
                    return Ok(());
                }
                Err(err) => return Err(err.into()),
            };

        match outcome {
            TransferOutcome::Detected {
                issuer_request_id,
                client_id,
                alpaca_account,
                network,
            } => {
                tokio::spawn(drive_redemption_flow(
                    issuer_request_id,
                    client_id,
                    alpaca_account,
                    network,
                    RedemptionFlowCtx {
                        store: self.store.clone(),
                        redeem_call_manager: self.redeem_call_manager.clone(),
                        journal_manager: self.journal_manager.clone(),
                        burn_manager: self.burn_manager.clone(),
                    },
                ));
            }
            TransferOutcome::AlreadyDetected
            | TransferOutcome::SkippedMint
            | TransferOutcome::SkippedNoAccount => {}
        }

        Ok(())
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
    use event_sorcery::{Store, test_store};
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::TransferPollError;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::poll_checkpoint::{self, TRANSFER_POLL};
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptInventory, ReceiptService,
    };
    use crate::redemption::Redemption;
    use crate::redemption::test_utils::{
        create_transfer_log, setup_test_db_with_asset,
    };
    use crate::test_utils::logs_contain_at;
    use crate::vault::mock::MockVaultService;

    /// `pool` must already have migrations applied — the stores write to the
    /// `events` table on first command dispatch.
    fn setup_test_store(
        pool: &SqlitePool,
    ) -> (Arc<Store<Redemption>>, Arc<dyn ReceiptService>) {
        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        let vault_service: Arc<dyn crate::vault::VaultService> =
            Arc::new(MockVaultService::new_success());
        let store =
            Arc::new(test_store::<Redemption>(pool.clone(), vault_service));
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
        let (store, receipt_service) = setup_test_store(&pool);

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(
            crate::redemption::redeem_call_manager::RedeemCallManager::new(
                alpaca_service.clone(),
                store.clone(),
                pool.clone(),
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
        let burn_manager =
            Arc::new(crate::redemption::burn_manager::BurnManager::new(
                vault_service,
                pool.clone(),
                store.clone(),
                receipt_service,
                bot_wallet,
            ));

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter.clone());

        let poller = super::TransferPoller::new(super::TransferPollerConfig {
            provider,
            bot_wallet,
            backfill_start_block,
            store,
            pool: pool.clone(),
            redeem_call_manager,
            journal_manager,
            burn_manager,
        });

        TestPollerSetup { poller, pool }
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
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
            )
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
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
            )
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
        poll_checkpoint::advance(
            &setup.pool,
            &poll_checkpoint::transfer_poll_name(vault),
            200,
        )
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
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
            )
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
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
            )
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
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
            )
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
        poll_checkpoint::advance(&setup.pool, TRANSFER_POLL, 200)
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
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
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

        poll_checkpoint::advance(&setup.pool, TRANSFER_POLL, 200)
            .await
            .unwrap();

        setup.poller.seed_per_vault_checkpoints().await.unwrap();

        assert_eq!(
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
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
        poll_checkpoint::advance(
            &setup.pool,
            &poll_checkpoint::transfer_poll_name(vault),
            100,
        )
        .await
        .unwrap();
        // ...and a stale legacy global sits far ahead at block 5000.
        poll_checkpoint::advance(&setup.pool, TRANSFER_POLL, 5000)
            .await
            .unwrap();

        setup.poller.seed_per_vault_checkpoints().await.unwrap();

        assert_eq!(
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
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
    /// not starve the others. The failed vault keeps no checkpoint advance, so
    /// it simply retries next pass from where it left off.
    #[traced_test]
    #[tokio::test]
    async fn poll_once_continues_past_a_failing_vault() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(300u64));
        asserter.push_failure_msg("simulated eth_getLogs failure");

        let setup =
            setup_test_poller(vault, bot_wallet, None, &asserter, 0).await;

        // The failing vault must not propagate — the pass completes Ok.
        setup.poller.poll_once().await.unwrap();

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Transfer poll pass completed with vault failures"]
        ));

        assert_eq!(
            poll_checkpoint::load(
                &setup.pool,
                &poll_checkpoint::transfer_poll_name(vault),
            )
            .await
            .unwrap(),
            None,
            "a failed vault must not advance its checkpoint"
        );
    }
}
