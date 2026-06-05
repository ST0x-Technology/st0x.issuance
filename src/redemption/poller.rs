use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use event_sorcery::Store;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, trace, warn};

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
/// - Persists progress to the `poll_checkpoints` SQL table
///
/// **Operational note — adding new vaults:** The global checkpoint means a
/// newly-added vault will only be scanned from the current checkpoint
/// forward, not from `backfill_start_block`. When adding a new vault,
/// either reset the checkpoint manually or restart with a lower
/// `backfill_start_block` to cover historic blocks.
pub(crate) struct TransferPoller<P> {
    provider: P,
    bot_wallet: Address,
    vaults: Vec<Address>,
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
    pub(crate) vaults: Vec<Address>,
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
            vaults: config.vaults,
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
}

impl<P> TransferPoller<P>
where
    P: Provider + Clone + Send + Sync,
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

        let last_processed =
            poll_checkpoint::load(&self.pool, TRANSFER_POLL).await?;

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

            poll_checkpoint::advance(&self.pool, TRANSFER_POLL, chunk_to)
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
        build_poller(vault, bot_wallet, asserter, backfill_start_block, pool)
    }

    fn build_poller(
        vault: Address,
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
            vaults: vec![vault],
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
            poll_checkpoint::load(&setup.pool, TRANSFER_POLL).await.unwrap(),
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
            poll_checkpoint::load(&setup.pool, TRANSFER_POLL).await.unwrap(),
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
        poll_checkpoint::advance(&setup.pool, TRANSFER_POLL, 200)
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
            poll_checkpoint::load(&setup.pool, TRANSFER_POLL).await.unwrap(),
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
            .connect_mocked_client(asserter);

        let poller = super::TransferPoller::new(super::TransferPollerConfig {
            provider,
            bot_wallet,
            vaults: vec![],
            backfill_start_block: 0,
            store,
            pool: pool.clone(),
            redeem_call_manager,
            journal_manager,
            burn_manager,
        });

        poller.poll_once().await.unwrap();

        // No checkpoint saved, no RPC calls made
        assert_eq!(
            poll_checkpoint::load(&pool, TRANSFER_POLL).await.unwrap(),
            None
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
            poll_checkpoint::load(&setup.pool, TRANSFER_POLL).await.unwrap(),
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
        assert_eq!(
            poll_checkpoint::load(&setup.pool, TRANSFER_POLL).await.unwrap(),
            None
        );

        assert!(logs_contain_at!(
            tracing::Level::TRACE,
            &["Transfer poller caught up"]
        ));
    }
}
