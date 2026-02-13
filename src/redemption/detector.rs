use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{CqrsFramework, EventStore};
use futures::StreamExt;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{info, warn};
use url::Url;

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
use crate::receipt_inventory::ReceiptInventory;

/// Configuration parameters for the redemption detector.
pub(crate) struct RedemptionDetectorConfig {
    pub(crate) rpc_url: Url,
    pub(crate) vault: Address,
    pub(crate) bot_wallet: Address,
}

/// Orchestrates the WebSocket monitoring process for redemption detection.
///
/// The detector subscribes to Transfer events on the vault contract, filtering for
/// transfers to the redemption wallet. When a transfer is detected, it creates a
/// RedemptionCommand::Detect to record the redemption in the aggregate.
pub(crate) struct RedemptionDetector<RedemptionStore, ReceiptInventoryStore>
where
    RedemptionStore: EventStore<Redemption>,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    rpc_url: Url,
    vault: Address,
    bot_wallet: Address,
    cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
    event_store: Arc<RedemptionStore>,
    pool: Pool<Sqlite>,
    redeem_call_manager: Arc<RedeemCallManager<RedemptionStore>>,
    journal_manager: Arc<JournalManager<RedemptionStore>>,
    burn_manager: Arc<BurnManager<RedemptionStore, ReceiptInventoryStore>>,
}

impl<RedemptionStore, ReceiptInventoryStore>
    RedemptionDetector<RedemptionStore, ReceiptInventoryStore>
where
    RedemptionStore: EventStore<Redemption> + 'static,
    ReceiptInventoryStore: EventStore<ReceiptInventory> + 'static,
    RedemptionStore::AC: Send,
    ReceiptInventoryStore::AC: Send,
{
    /// Creates a new redemption detector.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration parameters (RPC URL, addresses)
    /// * `cqrs` - CQRS framework for executing commands on the Redemption aggregate
    /// * `event_store` - Event store for loading aggregates
    /// * `pool` - Database connection pool for querying tokenized assets
    /// * `redeem_call_manager` - Manager for handling Alpaca redeem API calls
    /// * `journal_manager` - Manager for polling Alpaca journal status
    /// * `burn_manager` - Manager for handling token burning after Alpaca journal completes
    pub(crate) fn new(
        config: RedemptionDetectorConfig,
        cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
        event_store: Arc<RedemptionStore>,
        pool: Pool<Sqlite>,
        redeem_call_manager: Arc<RedeemCallManager<RedemptionStore>>,
        journal_manager: Arc<JournalManager<RedemptionStore>>,
        burn_manager: Arc<BurnManager<RedemptionStore, ReceiptInventoryStore>>,
    ) -> Self {
        Self {
            rpc_url: config.rpc_url,
            vault: config.vault,
            bot_wallet: config.bot_wallet,
            cqrs,
            event_store,
            pool,
            redeem_call_manager,
            journal_manager,
            burn_manager,
        }
    }

    /// Runs the monitoring loop with automatic reconnection on errors.
    ///
    /// This method never returns under normal operation. If a WebSocket error occurs,
    /// it logs the error and reconnects after 5 seconds.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn run(&self) {
        loop {
            if let Err(err) = self.monitor_once().await {
                warn!(
                    "WebSocket monitoring error: {err}. Reconnecting in 5s..."
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

    /// Monitors for redemptions once, establishing a WebSocket connection and processing events.
    ///
    /// Returns an error if the connection fails, subscription fails, or the stream ends.
    async fn monitor_once(&self) -> Result<(), RedemptionMonitorError> {
        info!("Connecting to WebSocket at {}", self.rpc_url);

        let provider =
            ProviderBuilder::new().connect(self.rpc_url.as_str()).await?;

        let vault =
            bindings::OffchainAssetReceiptVault::new(self.vault, &provider);

        let filter = vault.Transfer_filter().topic2(self.bot_wallet).filter;

        info!(
            "Subscribing to Transfer events for bot wallet {}",
            self.bot_wallet
        );

        let sub = provider.subscribe_logs(&filter).await?;

        let mut stream = sub.into_stream();

        info!("WebSocket subscription active, monitoring for redemptions");

        while let Some(log) = stream.next().await {
            if let Err(err) = self.process_transfer_log(&log).await {
                warn!("Failed to process transfer log: {err}");
            }
        }

        Err(RedemptionMonitorError::StreamEnded)
    }

    /// Processes a single Transfer event log.
    ///
    /// Decodes the event, looks up the corresponding tokenized asset, converts the quantity,
    /// and executes a RedemptionCommand::Detect.
    #[tracing::instrument(skip(self, log), fields(
        tx_hash = ?log.transaction_hash,
        block_number = ?log.block_number
    ))]
    async fn process_transfer_log(
        &self,
        log: &alloy::rpc::types::Log,
    ) -> Result<Option<IssuerRedemptionRequestId>, RedemptionMonitorError> {
        let outcome =
            detect_transfer(log, self.vault, &self.cqrs, &self.pool).await?;

        match outcome {
            TransferOutcome::Detected {
                issuer_request_id,
                client_id,
                alpaca_account,
                network,
            } => {
                drive_redemption_flow(
                    issuer_request_id.clone(),
                    client_id,
                    alpaca_account,
                    network,
                    RedemptionFlowCtx {
                        event_store: self.event_store.clone(),
                        redeem_call_manager: self.redeem_call_manager.clone(),
                        journal_manager: self.journal_manager.clone(),
                        burn_manager: self.burn_manager.clone(),
                    },
                )
                .await;

                Ok(Some(issuer_request_id))
            }
            TransferOutcome::AlreadyDetected
            | TransferOutcome::SkippedMint
            | TransferOutcome::SkippedNoAccount => Ok(None),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RedemptionMonitorError {
    #[error("RPC error")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Transfer processing error: {0}")]
    TransferProcessing(#[from] TransferProcessingError),
    #[error("Stream ended unexpectedly")]
    StreamEnded,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{
        Address, B256, Bytes, Log as PrimitiveLog, LogData, U256, address, b256,
    };
    use alloy::rpc::types::Log;
    use alloy::sol_types::SolEvent;
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
        persist::GenericQuery,
    };
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::SqlitePool;
    use std::sync::Arc;

    use super::{
        BurnManager, JournalManager, RedeemCallManager, RedemptionDetector,
        RedemptionDetectorConfig,
    };
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

    type TestCqrs = CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;
    type TestReceiptInventoryCqrs =
        CqrsFramework<ReceiptInventory, MemStore<ReceiptInventory>>;

    fn setup_test_cqrs() -> (
        Arc<TestCqrs>,
        Arc<TestStore>,
        Arc<dyn ReceiptService>,
        Arc<TestReceiptInventoryCqrs>,
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
        vault_address: Address,
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
                address: vault_address,
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

    #[tokio::test]
    async fn test_process_transfer_log_success() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let (cqrs, store, receipt_service, receipt_inventory_cqrs) =
            setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        ));
        let journal_manager = Arc::new(JournalManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        ));

        let vault_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let burn_manager = Arc::new(BurnManager::new(
            vault_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            bot_wallet,
        ));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault,
            bot_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs.clone(),
            store.clone(),
            pool,
            redeem_call_manager,
            journal_manager,
            burn_manager,
        );

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        let log = create_transfer_log(
            vault,
            ap_wallet,
            bot_wallet,
            value,
            tx_hash,
            block_number,
        );

        let result = detector.process_transfer_log(&log).await;

        let issuer_request_id =
            result.expect("Expected success").expect("Expected Some(id)");

        let context =
            store.load_aggregate(&issuer_request_id.to_string()).await.unwrap();

        assert!(
            matches!(context.aggregate(), Redemption::AlpacaCalled { .. }),
            "Expected AlpacaCalled state, got {:?}",
            context.aggregate()
        );
    }

    #[tokio::test]
    async fn test_process_transfer_log_ignores_mint_events() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let (cqrs, store, receipt_service, receipt_inventory_cqrs) =
            setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, None).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        ));
        let journal_manager = Arc::new(JournalManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        ));

        let vault_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let burn_manager = Arc::new(BurnManager::new(
            vault_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            bot_wallet,
        ));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault,
            bot_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs,
            store.clone(),
            pool,
            redeem_call_manager,
            journal_manager,
            burn_manager,
        );

        let log = create_transfer_log(
            vault,
            Address::ZERO,
            bot_wallet,
            U256::from_str_radix("100000000000000000000", 10).unwrap(),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );

        let result = detector.process_transfer_log(&log).await;

        assert!(
            result.is_ok(),
            "Mint events should be silently ignored, got {result:?}"
        );

        assert!(
            result.unwrap().is_none(),
            "Mint events should return None (no redemption created)"
        );
    }

    #[tokio::test]
    async fn test_process_transfer_log_duplicate_creates_separate_redemptions()
    {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let (cqrs, store, receipt_service, receipt_inventory_cqrs) =
            setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        ));
        let journal_manager = Arc::new(JournalManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        ));

        let vault_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let burn_manager = Arc::new(BurnManager::new(
            vault_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            bot_wallet,
        ));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault,
            bot_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs,
            store,
            pool,
            redeem_call_manager,
            journal_manager,
            burn_manager,
        );

        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            vault,
            address!("0x9999999999999999999999999999999999999999"),
            bot_wallet,
            U256::from_str_radix("100000000000000000000", 10).unwrap(),
            tx_hash,
            12345,
        );

        let first_result = detector.process_transfer_log(&log).await;
        assert!(
            first_result.is_ok(),
            "First detection should succeed, got {first_result:?}"
        );

        // With idempotent detection, duplicate returns Ok(None) instead of error
        let second_result = detector.process_transfer_log(&log).await;
        assert!(
            second_result.is_ok(),
            "Second detection should be handled idempotently, got {second_result:?}"
        );
        assert!(
            second_result.unwrap().is_none(),
            "Duplicate detection should return None"
        );
    }
}
