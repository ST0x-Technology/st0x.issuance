use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{AggregateContext, CqrsFramework, EventStore};
use futures::{StreamExt, TryStreamExt, stream};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{
    IssuerRedemptionRequestId, Redemption, RedemptionCommand, RedemptionError,
    burn_manager::BurnManager, journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
};
use crate::Quantity;
use crate::QuantityConversionError;
use crate::account::view::{AccountViewError, find_by_wallet};
use crate::account::{AccountView, AlpacaAccountNumber, ClientId};
use crate::bindings;
use crate::receipt_inventory::ReceiptInventory;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAssetView, UnderlyingSymbol,
};

/// Maximum number of blocks to query in a single get_logs call.
const BLOCK_CHUNK_SIZE: u64 = 2000;

/// Generates inclusive block ranges of at most `chunk_size` blocks.
fn block_ranges(
    from: u64,
    to: u64,
    chunk_size: u64,
) -> impl Iterator<Item = (u64, u64)> {
    std::iter::successors(Some(from), move |&start| {
        let next = start + chunk_size;
        if next <= to { Some(next) } else { None }
    })
    .map(move |start| (start, (start + chunk_size - 1).min(to)))
}

/// Backfills Transfer events on a single vault by scanning historic blocks.
///
/// Detects transfers to the bot wallet that occurred while the service was down.
/// For each qualifying transfer (non-mint, from a whitelisted wallet), triggers
/// the full redemption flow. Idempotent via `IssuerRedemptionRequestId` derived
/// from the transaction hash — the Redemption aggregate rejects duplicates.
pub(crate) struct TransferBackfiller<
    ProviderType,
    RedemptionStore,
    ReceiptInventoryStore,
> where
    RedemptionStore: EventStore<Redemption>,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    pub(crate) provider: ProviderType,
    pub(crate) bot_wallet: Address,
    pub(crate) cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
    pub(crate) event_store: Arc<RedemptionStore>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) redeem_call_manager: Arc<RedeemCallManager<RedemptionStore>>,
    pub(crate) journal_manager: Arc<JournalManager<RedemptionStore>>,
    pub(crate) burn_manager:
        Arc<BurnManager<RedemptionStore, ReceiptInventoryStore>>,
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
    #[error("Failed to decode Transfer event: {0}")]
    SolTypes(#[from] alloy::sol_types::Error),
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("CQRS error: {0}")]
    Aggregate(#[from] cqrs_es::AggregateError<RedemptionError>),
    #[error("Account view error: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error("No asset found for vault {vault}")]
    NoMatchingAsset { vault: Address },
}

impl<ProviderType, RedemptionStore, ReceiptInventoryStore>
    TransferBackfiller<ProviderType, RedemptionStore, ReceiptInventoryStore>
where
    ProviderType: Provider + Clone + Send + Sync,
    RedemptionStore: EventStore<Redemption> + 'static,
    ReceiptInventoryStore: EventStore<ReceiptInventory> + 'static,
    RedemptionStore::AC: Send,
    ReceiptInventoryStore::AC: Send,
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

        let all_logs: Vec<Log> = stream::iter(block_ranges(
            from_block,
            current_block,
            BLOCK_CHUNK_SIZE,
        ))
        .then(|(chunk_from, chunk_to)| async move {
            let logs =
                self.fetch_transfer_logs(vault, chunk_from, chunk_to).await?;
            debug!(
                chunk_from,
                chunk_to,
                logs_found = logs.len(),
                "Processed block range"
            );
            Ok::<_, TransferBackfillError>(logs)
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect();

        let mut detected_count = 0u64;
        let mut skipped_mint = 0u64;
        let mut skipped_no_account = 0u64;

        for log in &all_logs {
            match self.process_transfer(vault, log).await? {
                TransferOutcome::Detected => detected_count += 1,
                TransferOutcome::SkippedMint => skipped_mint += 1,
                TransferOutcome::SkippedNoAccount => {
                    skipped_no_account += 1;
                }
            }
        }

        info!(
            detected_count,
            skipped_mint, skipped_no_account, "Transfer backfill complete"
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

    async fn process_transfer(
        &self,
        vault: Address,
        log: &Log,
    ) -> Result<TransferOutcome, TransferBackfillError> {
        let transfer_event =
            bindings::OffchainAssetReceiptVault::Transfer::decode_log(
                &log.inner,
            )?;

        if transfer_event.from == Address::ZERO {
            debug!(
                to = %transfer_event.to,
                value = %transfer_event.value,
                "Skipping mint event (from=0x0)"
            );
            return Ok(TransferOutcome::SkippedMint);
        }

        let tx_hash =
            log.transaction_hash.ok_or(TransferBackfillError::MissingTxHash)?;

        let block_number = log
            .block_number
            .ok_or(TransferBackfillError::MissingBlockNumber)?;

        let account_view =
            find_by_wallet(&self.pool, &transfer_event.from).await?;

        let Some(AccountView::LinkedToAlpaca {
            client_id, alpaca_account, ..
        }) = account_view
        else {
            debug!(
                from = %transfer_event.from,
                tx_hash = %tx_hash,
                "Skipping transfer from unwhitelisted wallet"
            );
            return Ok(TransferOutcome::SkippedNoAccount);
        };

        let (underlying, token, network) =
            self.find_matching_asset(vault).await?;

        let issuer_request_id = IssuerRedemptionRequestId::new(tx_hash);
        let quantity =
            Quantity::from_u256_with_18_decimals(transfer_event.value)?;

        let command = RedemptionCommand::Detect {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token,
            wallet: transfer_event.from,
            quantity,
            tx_hash,
            block_number,
        };

        if let Err(err) =
            self.cqrs.execute(&issuer_request_id.to_string(), command).await
        {
            debug!(
                %issuer_request_id,
                error = ?err,
                "Skipping already-detected transfer"
            );
            return Ok(TransferOutcome::Detected);
        }

        info!(
            %issuer_request_id,
            from = %transfer_event.from,
            "Backfill detected redemption transfer"
        );

        self.trigger_redemption_flow(
            issuer_request_id,
            client_id,
            alpaca_account,
            network,
        )
        .await;

        Ok(TransferOutcome::Detected)
    }

    async fn find_matching_asset(
        &self,
        vault: Address,
    ) -> Result<(UnderlyingSymbol, TokenSymbol, Network), TransferBackfillError>
    {
        let assets = list_enabled_assets(&self.pool).await?;

        assets
            .into_iter()
            .find_map(|view| match view {
                TokenizedAssetView::Asset {
                    underlying,
                    token,
                    network,
                    vault: addr,
                    ..
                } if addr == vault => Some((underlying, token, network)),
                _ => None,
            })
            .ok_or(TransferBackfillError::NoMatchingAsset { vault })
    }

    async fn trigger_redemption_flow(
        &self,
        issuer_request_id: IssuerRedemptionRequestId,
        client_id: ClientId,
        alpaca_account: AlpacaAccountNumber,
        network: Network,
    ) {
        let aggregate_ctx = match self
            .event_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => {
                warn!(
                    %issuer_request_id,
                    error = ?err,
                    "Failed to load aggregate after detection"
                );
                return;
            }
        };

        if let Err(err) = self
            .redeem_call_manager
            .handle_redemption_detected(
                &alpaca_account,
                &issuer_request_id,
                aggregate_ctx.aggregate(),
                client_id,
                network,
            )
            .await
        {
            warn!(
                %issuer_request_id,
                error = ?err,
                "handle_redemption_detected failed during backfill"
            );
            return;
        }

        let aggregate_ctx = match self
            .event_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => {
                warn!(
                    %issuer_request_id,
                    error = ?err,
                    "Failed to load aggregate after redeem call"
                );
                return;
            }
        };

        let Redemption::AlpacaCalled { tokenization_request_id, .. } =
            aggregate_ctx.aggregate()
        else {
            return;
        };

        let journal_manager = self.journal_manager.clone();
        let burn_manager = self.burn_manager.clone();
        let event_store = self.event_store.clone();
        let issuer_request_id_cloned = issuer_request_id.clone();
        let tokenization_request_id_cloned = tokenization_request_id.clone();

        tokio::spawn(async move {
            if let Err(err) = journal_manager
                .handle_alpaca_called(
                    &alpaca_account,
                    issuer_request_id_cloned.clone(),
                    tokenization_request_id_cloned,
                )
                .await
            {
                warn!(
                    error = ?err,
                    "handle_alpaca_called (journal polling) failed during backfill"
                );
                return;
            }

            let aggregate_ctx = match event_store
                .load_aggregate(&issuer_request_id_cloned.to_string())
                .await
            {
                Ok(ctx) => ctx,
                Err(err) => {
                    warn!(
                        issuer_request_id = %issuer_request_id_cloned,
                        error = ?err,
                        "Failed to load aggregate after journal completion"
                    );
                    return;
                }
            };

            if matches!(aggregate_ctx.aggregate(), Redemption::Burning { .. }) {
                if let Err(err) = burn_manager
                    .handle_burning_started(
                        &issuer_request_id_cloned,
                        aggregate_ctx.aggregate(),
                    )
                    .await
                {
                    warn!(
                        issuer_request_id = %issuer_request_id_cloned,
                        error = ?err,
                        "handle_burning_started failed during backfill"
                    );
                }
            }
        });
    }
}

enum TransferOutcome {
    Detected,
    SkippedMint,
    SkippedNoAccount,
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{
        Address, B256, Bytes, Log as PrimitiveLog, LogData, U256, address, b256,
    };
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::Log;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolEvent;
    use cqrs_es::{CqrsFramework, mem_store::MemStore, persist::GenericQuery};
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::SqlitePool;
    use std::sync::Arc;

    use super::TransferBackfiller;
    use crate::account::{
        Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
        view::AccountView,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptInventory, ReceiptService,
    };
    use crate::redemption::Redemption;
    use crate::tokenized_asset::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol, view::TokenizedAssetView,
    };
    use crate::vault::mock::MockVaultService;

    type TestRedemptionStore = MemStore<Redemption>;
    type TestReceiptInventoryStore = MemStore<ReceiptInventory>;
    type TestRedemptionCqrs =
        Arc<CqrsFramework<Redemption, TestRedemptionStore>>;
    type TestReceiptInventoryCqrs =
        Arc<CqrsFramework<ReceiptInventory, TestReceiptInventoryStore>>;

    fn setup_test_cqrs() -> (
        TestRedemptionCqrs,
        Arc<TestRedemptionStore>,
        Arc<dyn ReceiptService>,
        TestReceiptInventoryCqrs,
    ) {
        let store = Arc::new(MemStore::default());
        let receipt_inventory_store: Arc<TestReceiptInventoryStore> =
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
                receipt_inventory_cqrs.clone(),
            ));
        (cqrs, store, receipt_service, receipt_inventory_cqrs)
    }

    async fn setup_test_db_with_asset(
        vault: Address,
        ap_wallet: Option<Address>,
    ) -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let asset_view_repo = Arc::new(SqliteViewRepository::<
            TokenizedAssetView,
            TokenizedAsset,
        >::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));
        let asset_query = GenericQuery::new(asset_view_repo);
        let asset_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(asset_query)], ());

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");

        asset_cqrs
            .execute(
                &underlying.0,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token,
                    network,
                    vault,
                },
            )
            .await
            .unwrap();

        if let Some(wallet) = ap_wallet {
            let account_view_repo =
                Arc::new(SqliteViewRepository::<AccountView, Account>::new(
                    pool.clone(),
                    "account_view".to_string(),
                ));

            let account_query = GenericQuery::new(account_view_repo);
            let account_cqrs =
                sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

            let client_id = ClientId::new();
            let email = Email::new("test@example.com".to_string()).unwrap();

            account_cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::Register { client_id, email },
                )
                .await
                .unwrap();

            account_cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::LinkToAlpaca {
                        alpaca_account: AlpacaAccountNumber(
                            "ALPACA123".to_string(),
                        ),
                    },
                )
                .await
                .unwrap();

            account_cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::WhitelistWallet { wallet },
                )
                .await
                .unwrap();
        }

        pool
    }

    fn create_transfer_log(
        from: Address,
        to: Address,
        value: U256,
        tx_hash: B256,
        block_number: u64,
    ) -> Log {
        let topics = vec![
            OffchainAssetReceiptVault::Transfer::SIGNATURE_HASH,
            B256::left_padding_from(&from[..]),
            B256::left_padding_from(&to[..]),
        ];

        let data_bytes = value.to_be_bytes::<32>();

        Log {
            inner: PrimitiveLog {
                address: address!("0x0000000000000000000000000000000000000000"),
                data: LogData::new_unchecked(topics, Bytes::from(data_bytes)),
            },
            block_hash: Some(b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    async fn setup_and_run_backfill(
        vault: Address,
        bot_wallet: Address,
        ap_wallet: Option<Address>,
        asserter: &Asserter,
        from_block: u64,
    ) -> Result<super::TransferBackfillResult, super::TransferBackfillError>
    {
        let (cqrs, store, receipt_service, receipt_inventory_cqrs) =
            setup_test_cqrs();
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
                receipt_inventory_cqrs,
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

    #[tokio::test]
    async fn backfill_detects_transfer_to_bot_wallet() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let transfer_log =
            create_transfer_log(ap_wallet, bot_wallet, value, tx_hash, 100);

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
    }

    #[tokio::test]
    async fn backfill_skips_mint_events() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let mint_log =
            create_transfer_log(Address::ZERO, bot_wallet, value, tx_hash, 100);

        let asserter = Asserter::new();
        asserter.push_success(&U256::from(200u64));
        asserter.push_success(&vec![mint_log]);

        let result =
            setup_and_run_backfill(vault, bot_wallet, None, &asserter, 0)
                .await
                .unwrap();

        assert_eq!(result.detected_count, 0);
        assert_eq!(result.skipped_mint, 1);
    }

    #[tokio::test]
    async fn backfill_is_idempotent() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let transfer_log =
            create_transfer_log(ap_wallet, bot_wallet, value, tx_hash, 100);

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

        // Second run with same transfer — should not error (idempotent)
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
            "Backfill should be idempotent, got: {result2:?}"
        );
    }

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
    }

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
    }
}
