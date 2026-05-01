use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, info};

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

/// Backfills Transfer events on a single vault by scanning historic blocks.
///
/// Detects transfers to the bot wallet that occurred while the service was down.
/// For each qualifying transfer (non-mint, from a whitelisted wallet), triggers
/// the full redemption flow. Idempotent via `IssuerRedemptionRequestId` derived
/// from the transaction hash — the Redemption aggregate rejects duplicates.
pub(crate) struct TransferBackfiller<Node, RedemptionStore>
where
    RedemptionStore: EventStore<Redemption>,
{
    pub(crate) provider: Node,
    pub(crate) bot_wallet: Address,
    pub(crate) cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
    pub(crate) event_store: Arc<RedemptionStore>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) redeem_call_manager: Arc<RedeemCallManager<RedemptionStore>>,
    pub(crate) journal_manager: Arc<JournalManager<RedemptionStore>>,
    pub(crate) burn_manager: Arc<BurnManager<RedemptionStore>>,
}

#[derive(Debug)]
pub(crate) struct TransferBackfillResult {
    pub(crate) detected_count: u64,
    pub(crate) skipped_mint: u64,
    pub(crate) skipped_no_account: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferBackfillError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),
    #[error(
        "Backfill checkpoint overflow for vault {vault}: {last_processed_block}"
    )]
    CheckpointOverflow { vault: Address, last_processed_block: u64 },
    #[error("Transfer processing error: {0}")]
    TransferProcessing(#[from] TransferProcessingError),
}

impl<Node, RedemptionStore> TransferBackfiller<Node, RedemptionStore>
where
    Node: Provider + Clone + Send + Sync,
    RedemptionStore: EventStore<Redemption> + 'static,
    RedemptionStore::AC: Send,
{
    /// Scans historic Transfer events to detect redemptions that occurred
    /// while the service was down.
    ///
    /// For each Transfer where `to=bot_wallet` and `from != Address::ZERO`:
    /// 1. Looks up the tokenized asset matching this vault
    /// 2. Creates a `RedemptionCommand::Detect` (idempotent via tx hash)
    /// 3. Looks up the sender's account info
    /// 4. Triggers Alpaca redeem call, journal polling, and burn
    ///
    /// Queries are chunked to avoid RPC response size limits.
    pub(crate) async fn backfill_transfers(
        &self,
        vault: Address,
        backfill_start_block: u64,
    ) -> Result<TransferBackfillResult, TransferBackfillError> {
        let current_block = self.provider.get_block_number().await?;
        let from_block = backfill_start_block_for_vault(
            &self.pool,
            vault,
            backfill_start_block,
        )
        .await?;

        if from_block > current_block {
            info!(
                %vault,
                from_block,
                current_block,
                "Transfer backfill skipped: from_block is ahead of current block"
            );

            return Ok(TransferBackfillResult {
                detected_count: 0,
                skipped_mint: 0,
                skipped_no_account: 0,
            });
        }

        info!(
            %vault,
            bot_wallet = %self.bot_wallet,
            from_block,
            to_block = current_block,
            "Starting transfer backfill"
        );

        let mut detected_count = 0u64;
        let mut skipped_mint = 0u64;
        let mut skipped_no_account = 0u64;

        for (chunk_from, chunk_to) in
            block_ranges(from_block, current_block, BLOCK_CHUNK_SIZE)
        {
            let logs =
                self.fetch_transfer_logs(vault, chunk_from, chunk_to).await?;

            debug!(
                chunk_from,
                chunk_to,
                logs_found = logs.len(),
                "Processed block range"
            );

            for log in &logs {
                let outcome =
                    detect_transfer(log, vault, &self.cqrs, &self.pool).await?;

                match outcome {
                    TransferOutcome::Detected {
                        issuer_request_id,
                        client_id,
                        alpaca_account,
                        network,
                    } => {
                        drive_redemption_flow(
                            issuer_request_id,
                            client_id,
                            alpaca_account,
                            network,
                            RedemptionFlowCtx {
                                event_store: self.event_store.clone(),
                                redeem_call_manager: self
                                    .redeem_call_manager
                                    .clone(),
                                journal_manager: self.journal_manager.clone(),
                                burn_manager: self.burn_manager.clone(),
                            },
                        )
                        .await;
                        detected_count += 1;
                    }
                    TransferOutcome::AlreadyDetected => detected_count += 1,
                    TransferOutcome::SkippedMint => skipped_mint += 1,
                    TransferOutcome::SkippedNoAccount => {
                        skipped_no_account += 1;
                    }
                }
            }
        }

        save_backfill_checkpoint(&self.pool, vault, current_block).await?;

        info!(
            %vault,
            detected_count,
            skipped_mint,
            skipped_no_account,
            checkpoint_block = current_block,
            "Transfer backfill complete"
        );

        Ok(TransferBackfillResult {
            detected_count,
            skipped_mint,
            skipped_no_account,
        })
    }

    async fn fetch_transfer_logs(
        &self,
        vault: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, TransferBackfillError> {
        let vault_contract =
            bindings::OffchainAssetReceiptVault::new(vault, &self.provider);

        let filter = vault_contract
            .Transfer_filter()
            .topic2(self.bot_wallet)
            .from_block(from_block)
            .to_block(to_block)
            .filter;

        let logs = self.provider.get_logs(&filter).await?;

        Ok(logs)
    }
}

async fn backfill_start_block_for_vault(
    pool: &Pool<Sqlite>,
    vault: Address,
    configured_start_block: u64,
) -> Result<u64, TransferBackfillError> {
    load_backfill_checkpoint(pool, vault).await?.map_or(
        Ok(configured_start_block),
        |last_processed_block| {
            next_transfer_backfill_block(
                vault,
                last_processed_block,
                configured_start_block,
            )
        },
    )
}

fn next_transfer_backfill_block(
    vault: Address,
    last_processed_block: u64,
    configured_start_block: u64,
) -> Result<u64, TransferBackfillError> {
    let next_unprocessed_block = last_processed_block.checked_add(1).ok_or(
        TransferBackfillError::CheckpointOverflow {
            vault,
            last_processed_block,
        },
    )?;

    Ok(next_unprocessed_block.max(configured_start_block))
}

async fn load_backfill_checkpoint(
    pool: &Pool<Sqlite>,
    vault: Address,
) -> Result<Option<u64>, TransferBackfillError> {
    let row = sqlx::query_as::<_, (i64,)>(
        "
        SELECT last_processed_block
        FROM redemption_backfill_checkpoints
        WHERE vault = ?
        ",
    )
    .bind(vault.to_string())
    .fetch_optional(pool)
    .await?;

    row.map(|(last_processed_block,)| u64::try_from(last_processed_block))
        .transpose()
        .map_err(TransferBackfillError::IntConversion)
}

async fn save_backfill_checkpoint(
    pool: &Pool<Sqlite>,
    vault: Address,
    last_processed_block: u64,
) -> Result<(), TransferBackfillError> {
    let last_processed_block = i64::try_from(last_processed_block)?;

    sqlx::query(
        "
        INSERT INTO redemption_backfill_checkpoints (
            vault,
            last_processed_block
        )
        VALUES (?, ?)
        ON CONFLICT(vault) DO UPDATE SET
            last_processed_block = MAX(
                excluded.last_processed_block,
                redemption_backfill_checkpoints.last_processed_block
            ),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        ",
    )
    .bind(vault.to_string())
    .bind(last_processed_block)
    .execute(pool)
    .await?;

    Ok(())
}

/// Maximum number of blocks to query in a single get_logs call.
const BLOCK_CHUNK_SIZE: u64 = 2000;

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
    use alloy::signers::local::PrivateKeySigner;
    use cqrs_es::{CqrsFramework, mem_store::MemStore};
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{
        TransferBackfillError, TransferBackfiller,
        backfill_start_block_for_vault, load_backfill_checkpoint,
        next_transfer_backfill_block, save_backfill_checkpoint,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptInventory, ReceiptService,
    };
    use crate::redemption::Redemption;
    use crate::redemption::test_utils::{
        create_transfer_log, setup_test_db_with_asset,
    };
    use crate::test_utils::logs_contain_at;
    use crate::vault::mock::MockVaultService;

    type TestRedemptionStore = MemStore<Redemption>;
    type TestRedemptionCqrs =
        Arc<CqrsFramework<Redemption, TestRedemptionStore>>;

    fn setup_test_cqrs()
    -> (TestRedemptionCqrs, Arc<TestRedemptionStore>, Arc<dyn ReceiptService>)
    {
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
        (cqrs, store, receipt_service)
    }

    async fn setup_and_run_backfill(
        vault: Address,
        bot_wallet: Address,
        ap_wallet: Option<Address>,
        asserter: &Asserter,
        from_block: u64,
    ) -> Result<super::TransferBackfillResult, super::TransferBackfillError>
    {
        let pool = setup_test_db_with_asset(vault, ap_wallet).await;

        run_backfill_with_pool(vault, bot_wallet, asserter, from_block, pool)
            .await
    }

    async fn run_backfill_with_pool(
        vault: Address,
        bot_wallet: Address,
        asserter: &Asserter,
        from_block: u64,
        pool: SqlitePool,
    ) -> Result<super::TransferBackfillResult, super::TransferBackfillError>
    {
        let (cqrs, store, receipt_service) = setup_test_cqrs();

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

        let backfiller = TransferBackfiller {
            provider,
            bot_wallet,
            cqrs,
            event_store: store.clone(),
            pool,
            redeem_call_manager,
            journal_manager,
            burn_manager,
        };

        backfiller.backfill_transfers(vault, from_block).await
    }

    async fn load_backfill_checkpoint_updated_at(
        pool: &SqlitePool,
        vault: Address,
    ) -> Option<String> {
        sqlx::query_scalar::<_, String>(
            "
            SELECT updated_at
            FROM redemption_backfill_checkpoints
            WHERE vault = ?
            ",
        )
        .bind(vault.to_string())
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    #[traced_test]
    #[tokio::test]
    async fn backfill_detects_transfer_to_bot_wallet() {
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
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![transfer_log]);

        let result = setup_and_run_backfill(
            vault,
            bot_wallet,
            Some(ap_wallet),
            &asserter,
            0,
        )
        .await
        .unwrap();

        assert_eq!(result.detected_count, 1);
        assert_eq!(result.skipped_mint, 0);
        assert_eq!(result.skipped_no_account, 0);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Starting transfer backfill"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Transfer backfill complete", "detected_count=1"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn backfill_skips_mint_events() {
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

        let result =
            setup_and_run_backfill(vault, bot_wallet, None, &asserter, 0)
                .await
                .unwrap();

        assert_eq!(result.detected_count, 0);
        assert_eq!(result.skipped_mint, 1);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Starting transfer backfill"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Transfer backfill complete", "skipped_mint=1"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn backfill_succeeds_on_repeated_input() {
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

        let asserter1 = Asserter::new();
        asserter1.push_success(&U256::from(200u64));
        asserter1.push_success(&vec![transfer_log.clone()]);

        let result1 = setup_and_run_backfill(
            vault,
            bot_wallet,
            Some(ap_wallet),
            &asserter1,
            0,
        )
        .await
        .unwrap();
        assert_eq!(result1.detected_count, 1);

        // Second run with same transfer — independent store, verifies no crash
        let asserter2 = Asserter::new();
        asserter2.push_success(&U256::from(200u64));
        asserter2.push_success(&vec![transfer_log]);

        let result2 = setup_and_run_backfill(
            vault,
            bot_wallet,
            Some(ap_wallet),
            &asserter2,
            0,
        )
        .await;
        assert!(
            result2.is_ok(),
            "Backfill should succeed on repeated input, got: {result2:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Transfer backfill complete"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn backfill_skips_transfers_from_unwhitelisted_wallets() {
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

        let result =
            setup_and_run_backfill(vault, bot_wallet, None, &asserter, 0)
                .await
                .unwrap();

        assert_eq!(result.detected_count, 0);
        assert_eq!(result.skipped_no_account, 1);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Transfer backfill complete", "skipped_no_account=1"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn backfill_skips_when_from_block_ahead_of_chain() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(100u64));

        let result =
            setup_and_run_backfill(vault, bot_wallet, None, &asserter, 200)
                .await
                .unwrap();

        assert_eq!(result.detected_count, 0);
        assert_eq!(result.skipped_mint, 0);
        assert_eq!(result.skipped_no_account, 0);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Transfer backfill skipped", "from_block is ahead"]
        ));
    }

    #[tokio::test]
    async fn backfill_start_block_uses_configured_start_without_checkpoint() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let pool = setup_test_db_with_asset(vault, None).await;

        let start_block =
            backfill_start_block_for_vault(&pool, vault, 50).await.unwrap();

        assert_eq!(start_block, 50);
    }

    #[tokio::test]
    async fn backfill_start_block_resumes_after_checkpoint() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let pool = setup_test_db_with_asset(vault, None).await;

        save_backfill_checkpoint(&pool, vault, 80).await.unwrap();

        let start_block =
            backfill_start_block_for_vault(&pool, vault, 50).await.unwrap();

        assert_eq!(start_block, 81);
    }

    #[tokio::test]
    async fn backfill_start_block_respects_configured_floor() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let pool = setup_test_db_with_asset(vault, None).await;

        save_backfill_checkpoint(&pool, vault, 20).await.unwrap();

        let start_block =
            backfill_start_block_for_vault(&pool, vault, 50).await.unwrap();

        assert_eq!(start_block, 50);
    }

    #[tokio::test]
    async fn backfill_start_block_fails_on_checkpoint_overflow() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let error =
            next_transfer_backfill_block(vault, u64::MAX, 50).unwrap_err();

        assert!(matches!(
            error,
            TransferBackfillError::CheckpointOverflow { .. }
        ));
    }

    #[tokio::test]
    async fn save_backfill_checkpoint_is_monotonic() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let pool = setup_test_db_with_asset(vault, None).await;

        save_backfill_checkpoint(&pool, vault, 100).await.unwrap();
        save_backfill_checkpoint(&pool, vault, 80).await.unwrap();

        assert_eq!(
            load_backfill_checkpoint(&pool, vault).await.unwrap(),
            Some(100)
        );
    }

    #[tokio::test]
    async fn skipped_backfill_does_not_touch_checkpoint_timestamp() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let pool = setup_test_db_with_asset(vault, None).await;

        save_backfill_checkpoint(&pool, vault, 200).await.unwrap();
        let updated_at_before =
            load_backfill_checkpoint_updated_at(&pool, vault).await.unwrap();

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));

        run_backfill_with_pool(vault, bot_wallet, &asserter, 50, pool.clone())
            .await
            .unwrap();

        assert_eq!(
            load_backfill_checkpoint_updated_at(&pool, vault).await.unwrap(),
            updated_at_before
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn backfill_persists_checkpoint_after_success() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let pool = setup_test_db_with_asset(vault, None).await;

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        let result = run_backfill_with_pool(
            vault,
            bot_wallet,
            &asserter,
            50,
            pool.clone(),
        )
        .await
        .unwrap();

        assert_eq!(result.detected_count, 0);
        assert_eq!(
            load_backfill_checkpoint(&pool, vault).await.unwrap(),
            Some(200)
        );

        assert_eq!(
            backfill_start_block_for_vault(&pool, vault, 50).await.unwrap(),
            201
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Starting transfer backfill", "from_block=50", "to_block=200"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Transfer backfill complete", "checkpoint_block=200"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn backfill_restart_skips_already_checkpointed_range() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let pool = setup_test_db_with_asset(vault, None).await;

        save_backfill_checkpoint(&pool, vault, 200).await.unwrap();

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));

        let result = run_backfill_with_pool(
            vault,
            bot_wallet,
            &asserter,
            50,
            pool.clone(),
        )
        .await
        .unwrap();

        assert_eq!(result.detected_count, 0);
        assert_eq!(
            load_backfill_checkpoint(&pool, vault).await.unwrap(),
            Some(200)
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Transfer backfill skipped",
                "from_block is ahead",
                "from_block=201",
                "current_block=200"
            ]
        ));
    }
}
