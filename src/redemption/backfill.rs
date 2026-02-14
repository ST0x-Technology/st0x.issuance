use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::Log;
use alloy::transports::{RpcError, TransportErrorKind};
use apalis::prelude::Data;
use cqrs_es::{CqrsFramework, EventStore};
use serde::{Deserialize, Serialize};
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
use crate::{
    RedemptionCqrs, RedemptionEventStore, RedemptionManagers, VaultConfigs,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TransferBackfillJob;

pub(crate) struct TransferBackfillCtx {
    pub(crate) provider: DynProvider,
    pub(crate) bot_wallet: Address,
    pub(crate) cqrs: RedemptionCqrs,
    pub(crate) event_store: RedemptionEventStore,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) managers: RedemptionManagers,
    pub(crate) backfill_start_block: u64,
}

impl TransferBackfillJob {
    pub(crate) async fn run(
        self,
        ctx: Data<Arc<TransferBackfillCtx>>,
        vault_configs: Data<VaultConfigs>,
    ) -> Result<(), anyhow::Error> {
        let configs = {
            let guard = vault_configs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.clone()
        };

        let Some(configs) = configs else {
            anyhow::bail!(
                "Transfer backfill skipped: vault configs not available \
                 (receipt backfill may have failed)"
            );
        };

        if configs.is_empty() {
            info!("No enabled vaults, skipping transfer backfill");
            return Ok(());
        }

        let backfiller = TransferBackfiller {
            provider: ctx.provider.clone(),
            bot_wallet: ctx.bot_wallet,
            cqrs: ctx.cqrs.clone(),
            event_store: ctx.event_store.clone(),
            pool: ctx.pool.clone(),
            redeem_call_manager: ctx.managers.redeem_call.clone(),
            journal_manager: ctx.managers.journal.clone(),
            burn_manager: ctx.managers.burn.clone(),
        };

        info!(
            vault_count = configs.len(),
            "Running transfer backfill for all vaults"
        );

        let mut total_detected = 0u64;
        let mut total_skipped_mint = 0u64;
        let mut total_skipped_no_account = 0u64;
        let mut failed_vaults = 0u64;

        for config in &configs {
            match backfiller
                .backfill_transfers(config.vault, ctx.backfill_start_block)
                .await
            {
                Ok(result) => {
                    debug!(
                        vault = %config.vault,
                        detected = result.detected_count,
                        skipped_mint = result.skipped_mint,
                        skipped_no_account = result.skipped_no_account,
                        "Transfer backfill complete for vault"
                    );
                    total_detected += result.detected_count;
                    total_skipped_mint += result.skipped_mint;
                    total_skipped_no_account += result.skipped_no_account;
                }
                Err(err) => {
                    debug!(
                        vault = %config.vault,
                        error = %err,
                        "Transfer backfill failed for vault"
                    );
                    failed_vaults += 1;
                }
            }
        }

        info!(
            vault_count = configs.len(),
            total_detected,
            total_skipped_mint,
            total_skipped_no_account,
            failed_vaults,
            "Transfer backfill complete"
        );

        Ok(())
    }
}

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
        from_block: u64,
    ) -> Result<TransferBackfillResult, TransferBackfillError> {
        let current_block = self.provider.get_block_number().await?;

        if from_block > current_block {
            info!(
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

        let result = TransferBackfillResult {
            detected_count,
            skipped_mint,
            skipped_no_account,
        };

        info!(
            detected_count = result.detected_count,
            skipped_mint = result.skipped_mint,
            skipped_no_account = result.skipped_no_account,
            "Transfer backfill complete"
        );

        Ok(result)
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
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::TransferBackfiller;
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
        let (cqrs, store, receipt_service) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, ap_wallet).await;

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
}
