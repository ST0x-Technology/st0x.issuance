use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, trace, warn};

use super::{
    Redemption,
    burn_manager::BurnManager,
    journal_manager::JournalManager,
    poll_checkpoint::{
        TransferPollCheckpoint, TransferPollCheckpointCommand,
        TransferPollCheckpointError,
    },
    redeem_call_manager::RedeemCallManager,
    transfer::{
        RedemptionFlowCtx, TransferOutcome, TransferProcessingError,
        detect_transfer, drive_redemption_flow,
    },
};
use crate::bindings;

/// Interval between polling cycles when the poller is caught up to the chain
/// head. 5 seconds is a reasonable trade-off: negligible latency (downstream
/// flows take minutes) and predictable RPC cost (~$3/month on dRPC).
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum number of blocks to query in a single `eth_getLogs` call.
/// RPCs typically limit response sizes, so we chunk large ranges.
const BLOCK_CHUNK_SIZE: u64 = 2000;

/// Interval between retries when a polling pass fails (e.g., RPC error).
const RETRY_INTERVAL: Duration = Duration::from_secs(10);

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
/// - Checkpoints progress via the `TransferPollCheckpoint` aggregate
///
/// **Operational note — adding new vaults:** The global checkpoint means a
/// newly-added vault will only be scanned from the current checkpoint
/// forward, not from `backfill_start_block`. When adding a new vault,
/// either reset the checkpoint manually or restart with a lower
/// `backfill_start_block` to cover historic blocks.
pub(crate) struct TransferPoller<P, RedemptionStore, CheckpointStore>
where
    RedemptionStore: EventStore<Redemption>,
    CheckpointStore: EventStore<TransferPollCheckpoint>,
{
    provider: P,
    bot_wallet: Address,
    vaults: Vec<Address>,
    backfill_start_block: u64,
    cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
    event_store: Arc<RedemptionStore>,
    pool: Pool<Sqlite>,
    checkpoint_cqrs:
        Arc<CqrsFramework<TransferPollCheckpoint, CheckpointStore>>,
    checkpoint_store: Arc<CheckpointStore>,
    redeem_call_manager: Arc<RedeemCallManager<RedemptionStore>>,
    journal_manager: Arc<JournalManager<RedemptionStore>>,
    burn_manager: Arc<BurnManager<RedemptionStore>>,
}

/// Configuration for constructing a [`TransferPoller`].
pub(crate) struct TransferPollerConfig<P, RedemptionStore, CheckpointStore>
where
    RedemptionStore: EventStore<Redemption>,
    CheckpointStore: EventStore<TransferPollCheckpoint>,
{
    pub(crate) provider: P,
    pub(crate) bot_wallet: Address,
    pub(crate) vaults: Vec<Address>,
    pub(crate) backfill_start_block: u64,
    pub(crate) cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
    pub(crate) event_store: Arc<RedemptionStore>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) checkpoint_cqrs:
        Arc<CqrsFramework<TransferPollCheckpoint, CheckpointStore>>,
    pub(crate) checkpoint_store: Arc<CheckpointStore>,
    pub(crate) redeem_call_manager: Arc<RedeemCallManager<RedemptionStore>>,
    pub(crate) journal_manager: Arc<JournalManager<RedemptionStore>>,
    pub(crate) burn_manager: Arc<BurnManager<RedemptionStore>>,
}

impl<P, RedemptionStore, CheckpointStore>
    TransferPoller<P, RedemptionStore, CheckpointStore>
where
    RedemptionStore: EventStore<Redemption>,
    CheckpointStore: EventStore<TransferPollCheckpoint>,
{
    pub(crate) fn new(
        config: TransferPollerConfig<P, RedemptionStore, CheckpointStore>,
    ) -> Self {
        Self {
            provider: config.provider,
            bot_wallet: config.bot_wallet,
            vaults: config.vaults,
            backfill_start_block: config.backfill_start_block,
            cqrs: config.cqrs,
            event_store: config.event_store,
            pool: config.pool,
            checkpoint_cqrs: config.checkpoint_cqrs,
            checkpoint_store: config.checkpoint_store,
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
    #[error("Checkpoint aggregate error: {0}")]
    Checkpoint(#[from] AggregateError<TransferPollCheckpointError>),
    #[error("Checkpoint overflow: last_processed_block={last_processed_block}")]
    CheckpointOverflow { last_processed_block: u64 },
}

impl<P, RedemptionStore, CheckpointStore>
    TransferPoller<P, RedemptionStore, CheckpointStore>
where
    P: Provider + Clone + Send + Sync,
    RedemptionStore: EventStore<Redemption> + 'static,
    RedemptionStore::AC: Send,
    CheckpointStore: EventStore<TransferPollCheckpoint>,
{
    /// Runs the polling loop forever. Never returns under normal operation.
    ///
    /// On error, logs the failure and retries after `RETRY_INTERVAL`. The
    /// cursor is persisted, so no blocks are re-scanned unnecessarily.
    pub(crate) async fn run(&self) {
        loop {
            if let Err(error) = self.poll_once().await {
                warn!(
                    target: "redemption",
                    error = %error,
                    retry_after_secs = RETRY_INTERVAL.as_secs(),
                    "Transfer poll pass failed; will retry from last checkpoint"
                );
                tokio::time::sleep(RETRY_INTERVAL).await;
                continue;
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Runs a single poll pass: scan from cursor to chain head, process logs,
    /// advance checkpoint.
    async fn poll_once(&self) -> Result<(), TransferPollError> {
        if self.vaults.is_empty() {
            return Ok(());
        }

        let checkpoint = self
            .checkpoint_store
            .load_aggregate(TransferPollCheckpoint::AGGREGATE_ID)
            .await?;

        let cursor = match checkpoint.aggregate().last_processed_block() {
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

        let head = self.provider.get_block_number().await?;

        if cursor > head {
            trace!(
                target: "redemption",
                cursor,
                head,
                "Transfer poller caught up; sleeping"
            );
            return Ok(());
        }

        debug!(
            target: "redemption",
            from_block = cursor,
            to_block = head,
            vault_count = self.vaults.len(),
            "Polling for transfer events"
        );

        for (chunk_from, chunk_to) in
            block_ranges(cursor, head, BLOCK_CHUNK_SIZE)
        {
            let logs = self.fetch_transfer_logs(chunk_from, chunk_to).await?;

            trace!(
                target: "redemption",
                chunk_from,
                chunk_to,
                logs_found = logs.len(),
                "Processed block range"
            );

            for log in &logs {
                self.process_log(log).await?;
            }

            self.checkpoint_cqrs
                .execute(
                    TransferPollCheckpoint::AGGREGATE_ID,
                    TransferPollCheckpointCommand::Advance {
                        block_number: chunk_to,
                    },
                )
                .await?;
        }

        Ok(())
    }

    /// Fetches Transfer logs for all vaults in a single RPC call.
    async fn fetch_transfer_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, TransferPollError> {
        // Build a filter matching Transfer events across all vault addresses
        // where topic2 (to) == bot_wallet.
        let filter = alloy::rpc::types::Filter::new()
            .address(self.vaults.clone())
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
            match detect_transfer(log, vault, &self.cqrs, &self.pool).await {
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
                        event_store: self.event_store.clone(),
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
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use crate::alpaca::mock::MockAlpacaService;
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptInventory, ReceiptService,
    };
    use crate::redemption::Redemption;
    use crate::redemption::poll_checkpoint::{
        TransferPollCheckpoint, TransferPollCheckpointCommand,
    };
    use crate::redemption::test_utils::{
        create_transfer_log, setup_test_db_with_asset,
    };
    use crate::test_utils::logs_contain_at;
    use crate::vault::mock::MockVaultService;

    type TestRedemptionStore = MemStore<Redemption>;
    type TestRedemptionCqrs =
        Arc<CqrsFramework<Redemption, TestRedemptionStore>>;
    type TestCheckpointStore = MemStore<TransferPollCheckpoint>;
    type TestCheckpointCqrs =
        Arc<CqrsFramework<TransferPollCheckpoint, TestCheckpointStore>>;

    fn setup_test_cqrs() -> (
        TestRedemptionCqrs,
        Arc<TestRedemptionStore>,
        Arc<dyn ReceiptService>,
        TestCheckpointCqrs,
        Arc<TestCheckpointStore>,
    ) {
        let store = Arc::new(MemStore::default());
        let receipt_inventory_store: Arc<MemStore<ReceiptInventory>> =
            Arc::new(MemStore::default());
        let vault_service: Arc<dyn crate::vault::VaultService> =
            Arc::new(MockVaultService::new_success());
        let cqrs = Arc::new(CqrsFramework::new(
            (*store).clone(),
            vec![],
            vault_service,
        ));
        let receipt_inventory_cqrs = Arc::new(CqrsFramework::new(
            (*receipt_inventory_store).clone(),
            vec![],
            (),
        ));
        let receipt_service: Arc<dyn ReceiptService> =
            Arc::new(CqrsReceiptService::new(
                receipt_inventory_store,
                receipt_inventory_cqrs,
            ));

        let checkpoint_store = Arc::new(MemStore::default());
        let checkpoint_cqrs = Arc::new(CqrsFramework::new(
            (*checkpoint_store).clone(),
            vec![],
            (),
        ));

        (cqrs, store, receipt_service, checkpoint_cqrs, checkpoint_store)
    }

    async fn load_test_checkpoint(store: &TestCheckpointStore) -> Option<u64> {
        let context = store
            .load_aggregate(TransferPollCheckpoint::AGGREGATE_ID)
            .await
            .unwrap();
        context.aggregate().last_processed_block()
    }

    struct TestPollerSetup<P: alloy::providers::Provider + Clone> {
        poller:
            super::TransferPoller<P, TestRedemptionStore, TestCheckpointStore>,
        checkpoint_store: Arc<TestCheckpointStore>,
        checkpoint_cqrs: TestCheckpointCqrs,
    }

    async fn setup_test_poller(
        vault: Address,
        bot_wallet: Address,
        ap_wallet: Option<Address>,
        asserter: &Asserter,
        backfill_start_block: u64,
    ) -> TestPollerSetup<impl alloy::providers::Provider + Clone> {
        let pool = setup_test_db_with_asset(vault, ap_wallet).await;
        build_poller(vault, bot_wallet, asserter, backfill_start_block, pool)
    }

    fn build_poller(
        vault: Address,
        bot_wallet: Address,
        asserter: &Asserter,
        backfill_start_block: u64,
        pool: SqlitePool,
    ) -> TestPollerSetup<impl alloy::providers::Provider + Clone> {
        let (cqrs, store, receipt_service, checkpoint_cqrs, checkpoint_store) =
            setup_test_cqrs();

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(
            crate::redemption::redeem_call_manager::RedeemCallManager::new(
                alpaca_service.clone(),
                cqrs.clone(),
                store.clone(),
                pool.clone(),
            ),
        );
        let journal_manager =
            Arc::new(crate::redemption::journal_manager::JournalManager::new(
                alpaca_service,
                cqrs.clone(),
                store.clone(),
                pool.clone(),
            ));

        let vault_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let burn_manager =
            Arc::new(crate::redemption::burn_manager::BurnManager::new(
                vault_service,
                pool.clone(),
                cqrs.clone(),
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
            vaults: vec![vault],
            backfill_start_block,
            cqrs,
            event_store: store,
            pool,
            checkpoint_cqrs: checkpoint_cqrs.clone(),
            checkpoint_store: checkpoint_store.clone(),
            redeem_call_manager,
            journal_manager,
            burn_manager,
        });

        TestPollerSetup { poller, checkpoint_store, checkpoint_cqrs }
    }

    // -----------------------------------------------------------------------
    // Checkpoint aggregate tests (unit tests live in poll_checkpoint.rs;
    // these verify integration with the poller's setup_test_cqrs helper)
    // -----------------------------------------------------------------------

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
            load_test_checkpoint(&setup.checkpoint_store).await,
            Some(200)
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Polling for transfer events"]
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
            load_test_checkpoint(&setup.checkpoint_store).await,
            Some(200)
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Polling for transfer events"]
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

        // Pre-seed checkpoint at 200
        setup
            .checkpoint_cqrs
            .execute(
                TransferPollCheckpoint::AGGREGATE_ID,
                TransferPollCheckpointCommand::Advance { block_number: 200 },
            )
            .await
            .unwrap();

        // Should skip since cursor (201) > head (200)
        setup.poller.poll_once().await.unwrap();

        assert!(logs_contain_at!(
            tracing::Level::TRACE,
            &["Transfer poller caught up"]
        ));
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
            load_test_checkpoint(&setup.checkpoint_store).await,
            Some(200)
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn poll_no_op_with_empty_vaults() {
        let asserter = Asserter::new();
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let pool = setup_test_db_with_asset(vault, None).await;

        let (cqrs, store, receipt_service, checkpoint_cqrs, checkpoint_store) =
            setup_test_cqrs();

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(
            crate::redemption::redeem_call_manager::RedeemCallManager::new(
                alpaca_service.clone(),
                cqrs.clone(),
                store.clone(),
                pool.clone(),
            ),
        );
        let journal_manager =
            Arc::new(crate::redemption::journal_manager::JournalManager::new(
                alpaca_service,
                cqrs.clone(),
                store.clone(),
                pool.clone(),
            ));
        let vault_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let burn_manager =
            Arc::new(crate::redemption::burn_manager::BurnManager::new(
                vault_service,
                pool.clone(),
                cqrs.clone(),
                store.clone(),
                receipt_service,
                bot_wallet,
            ));

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let poller = super::TransferPoller::new(super::TransferPollerConfig {
            provider,
            bot_wallet,
            vaults: vec![],
            backfill_start_block: 0,
            cqrs,
            event_store: store,
            pool,
            checkpoint_cqrs,
            checkpoint_store: checkpoint_store.clone(),
            redeem_call_manager,
            journal_manager,
            burn_manager,
        });

        poller.poll_once().await.unwrap();

        // No checkpoint saved, no RPC calls made
        assert_eq!(load_test_checkpoint(&checkpoint_store).await, None);
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
            load_test_checkpoint(&setup.checkpoint_store).await,
            Some(200)
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Polling for transfer events", "from_block=50", "to_block=200"]
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
        assert_eq!(load_test_checkpoint(&setup.checkpoint_store).await, None);

        assert!(logs_contain_at!(
            tracing::Level::TRACE,
            &["Transfer poller caught up"]
        ));
    }
}
