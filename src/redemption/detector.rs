use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::sol_types::SolEvent;
use alloy::transports::RpcError;
use cqrs_es::{AggregateContext, CqrsFramework, EventStore};
use futures::StreamExt;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{info, warn};
use url::Url;

use super::{
    Redemption, RedemptionCommand, RedemptionError,
    journal_manager::JournalManager, redeem_call_manager::RedeemCallManager,
};
use crate::account::{
    ClientId,
    view::{AccountViewError, find_by_wallet},
};
use crate::bindings;
use crate::mint::IssuerRequestId;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAssetView, UnderlyingSymbol,
};
use crate::{Quantity, QuantityConversionError};

/// Configuration parameters for the redemption detector.
pub(crate) struct RedemptionDetectorConfig {
    pub(crate) rpc_url: Url,
    pub(crate) vault_address: Address,
    pub(crate) redemption_wallet: Address,
}

/// Orchestrates the WebSocket monitoring process for redemption detection.
///
/// The detector subscribes to Transfer events on the vault contract, filtering for
/// transfers to the redemption wallet. When a transfer is detected, it creates a
/// RedemptionCommand::Detect to record the redemption in the aggregate.
pub(crate) struct RedemptionDetector<ES: EventStore<Redemption>> {
    rpc_url: Url,
    vault_address: Address,
    redemption_wallet: Address,
    cqrs: Arc<CqrsFramework<Redemption, ES>>,
    event_store: Arc<ES>,
    pool: Pool<Sqlite>,
    redeem_call_manager: Arc<RedeemCallManager<ES>>,
    journal_manager: Arc<JournalManager<ES>>,
}

impl<ES: EventStore<Redemption> + 'static> RedemptionDetector<ES>
where
    ES::AC: Send,
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
    pub(crate) fn new(
        config: RedemptionDetectorConfig,
        cqrs: Arc<CqrsFramework<Redemption, ES>>,
        event_store: Arc<ES>,
        pool: Pool<Sqlite>,
        redeem_call_manager: Arc<RedeemCallManager<ES>>,
        journal_manager: Arc<JournalManager<ES>>,
    ) -> Self {
        Self {
            rpc_url: config.rpc_url,
            vault_address: config.vault_address,
            redemption_wallet: config.redemption_wallet,
            cqrs,
            event_store,
            pool,
            redeem_call_manager,
            journal_manager,
        }
    }

    /// Runs the monitoring loop with automatic reconnection on errors.
    ///
    /// This method never returns under normal operation. If a WebSocket error occurs,
    /// it logs the error and reconnects after 5 seconds.
    pub(crate) async fn run(&self) {
        loop {
            if let Err(e) = self.monitor_once().await {
                warn!("WebSocket monitoring error: {e}. Reconnecting in 5s...");
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

        let vault = bindings::OffchainAssetReceiptVault::new(
            self.vault_address,
            &provider,
        );

        let filter =
            vault.Transfer_filter().topic2(self.redemption_wallet).filter;

        info!(
            "Subscribing to Transfer events for redemption wallet {}",
            self.redemption_wallet
        );

        let sub = provider.subscribe_logs(&filter).await?;

        let mut stream = sub.into_stream();

        info!("WebSocket subscription active, monitoring for redemptions");

        while let Some(log) = stream.next().await {
            if let Err(e) = self.process_transfer_log(&log).await {
                warn!("Failed to process transfer log: {e}");
            }
        }

        Err(RedemptionMonitorError::StreamEnded)
    }

    /// Processes a single Transfer event log.
    ///
    /// Decodes the event, looks up the corresponding tokenized asset, converts the quantity,
    /// and executes a RedemptionCommand::Detect.
    async fn process_transfer_log(
        &self,
        log: &alloy::rpc::types::Log,
    ) -> Result<(), RedemptionMonitorError> {
        let transfer_event =
            bindings::OffchainAssetReceiptVault::Transfer::decode_log(
                &log.inner,
            )?;

        info!(
            from = %transfer_event.from,
            to = %transfer_event.to,
            value = %transfer_event.value,
            "Transfer event decoded"
        );

        let (underlying, token, network) = self.find_matching_asset().await?;

        let issuer_request_id = self
            .create_redemption_detection(
                &transfer_event,
                log,
                underlying,
                token,
            )
            .await?;

        let client_id =
            self.get_account_client_id(&transfer_event.from).await?;

        self.handle_alpaca_and_polling(issuer_request_id, client_id, network)
            .await?;

        Ok(())
    }

    async fn find_matching_asset(
        &self,
    ) -> Result<(UnderlyingSymbol, TokenSymbol, Network), RedemptionMonitorError>
    {
        let assets = list_enabled_assets(&self.pool).await?;

        assets
            .into_iter()
            .find_map(|view| match view {
                TokenizedAssetView::Asset {
                    underlying,
                    token,
                    network,
                    vault_address: addr,
                    ..
                } if addr == self.vault_address => {
                    Some((underlying, token, network))
                }
                _ => None,
            })
            .ok_or(RedemptionMonitorError::NoMatchingAsset {
                vault_address: self.vault_address,
            })
    }

    async fn create_redemption_detection(
        &self,
        transfer_event: &bindings::OffchainAssetReceiptVault::Transfer,
        log: &alloy::rpc::types::Log,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    ) -> Result<IssuerRequestId, RedemptionMonitorError> {
        let tx_hash = log
            .transaction_hash
            .ok_or(RedemptionMonitorError::MissingTxHash)?;

        let issuer_request_id = crate::mint::IssuerRequestId::new(format!(
            "red-{}",
            &tx_hash.to_string()[2..10]
        ));

        let quantity =
            Quantity::from_u256_with_18_decimals(transfer_event.value)?;

        let block_number = log
            .block_number
            .ok_or(RedemptionMonitorError::MissingBlockNumber)?;

        let command = RedemptionCommand::Detect {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token,
            wallet: transfer_event.from,
            quantity,
            tx_hash,
            block_number,
        };

        self.cqrs.execute(&issuer_request_id.0, command).await?;

        info!(
            issuer_request_id = %issuer_request_id.0,
            "Redemption detection recorded successfully"
        );

        Ok(issuer_request_id)
    }

    async fn get_account_client_id(
        &self,
        wallet: &Address,
    ) -> Result<ClientId, RedemptionMonitorError> {
        let account_view = find_by_wallet(&self.pool, wallet).await?.ok_or(
            RedemptionMonitorError::AccountNotFound { wallet: *wallet },
        )?;

        let crate::account::AccountView::Account { client_id, .. } =
            account_view
        else {
            return Err(RedemptionMonitorError::AccountNotLinked {
                wallet: *wallet,
            });
        };

        Ok(client_id)
    }

    async fn handle_alpaca_and_polling(
        &self,
        issuer_request_id: IssuerRequestId,
        client_id: ClientId,
        network: Network,
    ) -> Result<(), RedemptionMonitorError> {
        let aggregate_ctx =
            self.event_store.load_aggregate(&issuer_request_id.0).await?;

        if let Err(e) = self
            .redeem_call_manager
            .handle_redemption_detected(
                &issuer_request_id,
                aggregate_ctx.aggregate(),
                client_id,
                network,
            )
            .await
        {
            warn!(
                issuer_request_id = %issuer_request_id.0,
                error = ?e,
                "handle_redemption_detected failed"
            );
            return Ok(());
        }

        let aggregate_ctx =
            self.event_store.load_aggregate(&issuer_request_id.0).await?;

        let Redemption::AlpacaCalled { tokenization_request_id, .. } =
            aggregate_ctx.aggregate()
        else {
            return Ok(());
        };

        let journal_manager = self.journal_manager.clone();
        let issuer_request_id_cloned = issuer_request_id.clone();
        let tokenization_request_id_cloned = tokenization_request_id.clone();

        tokio::spawn(async move {
            if let Err(e) = journal_manager
                .handle_alpaca_called(
                    issuer_request_id_cloned,
                    tokenization_request_id_cloned,
                )
                .await
            {
                warn!(
                    error = ?e,
                    "handle_alpaca_called (journal polling) failed"
                );
            }
        });

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RedemptionMonitorError {
    #[error("WebSocket connection failed: {0}")]
    Connection(#[from] RpcError<alloy::transports::TransportErrorKind>),
    #[error("Failed to decode Transfer event: {0}")]
    EventDecode(#[from] alloy::sol_types::Error),
    #[error("Failed to list assets: {0}")]
    ListAssets(#[from] TokenizedAssetViewError),
    #[error("No asset found for vault address {vault_address}")]
    NoMatchingAsset { vault_address: Address },
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error("Failed to convert quantity: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("Failed to record redemption detection: {0}")]
    CqrsExecution(#[from] cqrs_es::AggregateError<RedemptionError>),
    #[error("Failed to query account: {0}")]
    AccountQuery(#[from] AccountViewError),
    #[error("No account found for wallet {wallet}")]
    AccountNotFound { wallet: Address },
    #[error("Account not linked for wallet {wallet}")]
    AccountNotLinked { wallet: Address },
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
    use chrono::Utc;
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use sqlx::SqlitePool;
    use std::sync::Arc;

    use super::{
        JournalManager, RedeemCallManager, RedemptionDetector,
        RedemptionDetectorConfig, RedemptionMonitorError,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::mint::IssuerRequestId;
    use crate::redemption::Redemption;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    type TestCqrs = CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn setup_test_db_with_asset(vault_address: Address) -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let added_at = Utc::now();

        let asset_view_json = serde_json::json!({
            "Asset": {
                "underlying": underlying,
                "token": token,
                "network": network,
                "vault_address": vault_address,
                "enabled": true,
                "added_at": added_at
            }
        });

        sqlx::query(
            "INSERT INTO tokenized_asset_view (view_id, version, payload) VALUES (?, ?, ?)",
        )
        .bind(&underlying.0)
        .bind(1_i64)
        .bind(asset_view_json.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let ap_wallet = address!("0x9999999999999999999999999999999999999999");
        let account_view_json = serde_json::json!({
            "Account": {
                "client_id": "test-client-123",
                "email": "test@example.com",
                "alpaca_account": "ALPACA123",
                "wallet": ap_wallet,
                "status": "Active",
                "linked_at": Utc::now()
            }
        });

        sqlx::query(
            "INSERT INTO account_view (view_id, version, payload) VALUES (?, ?, ?)",
        )
        .bind("test-client-123")
        .bind(1_i64)
        .bind(account_view_json.to_string())
        .execute(&pool)
        .await
        .unwrap();

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

    #[tokio::test]
    async fn test_process_transfer_log_success() {
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");
        let redemption_wallet =
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault_address).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
        ));
        let journal_manager =
            Arc::new(JournalManager::new(alpaca_service, cqrs.clone()));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault_address,
            redemption_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs.clone(),
            store.clone(),
            pool,
            redeem_call_manager,
            journal_manager,
        );

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        let log = create_transfer_log(
            ap_wallet,
            redemption_wallet,
            value,
            tx_hash,
            block_number,
        );

        let result = detector.process_transfer_log(&log).await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        let issuer_request_id = IssuerRequestId::new(format!(
            "red-{}",
            &tx_hash.to_string()[2..10]
        ));

        let context = store.load_aggregate(&issuer_request_id.0).await.unwrap();
        let aggregate = context.aggregate();

        assert!(
            matches!(aggregate, Redemption::AlpacaCalled { .. }),
            "Expected AlpacaCalled state, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_process_transfer_log_missing_tx_hash() {
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");
        let redemption_wallet =
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault_address).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
        ));
        let journal_manager =
            Arc::new(JournalManager::new(alpaca_service, cqrs.clone()));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault_address,
            redemption_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs,
            store,
            pool,
            redeem_call_manager,
            journal_manager,
        );

        let mut log = create_transfer_log(
            address!("0x9999999999999999999999999999999999999999"),
            redemption_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );

        log.transaction_hash = None;

        let result = detector.process_transfer_log(&log).await;

        assert!(
            matches!(result, Err(RedemptionMonitorError::MissingTxHash)),
            "Expected MissingTxHash error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_process_transfer_log_missing_block_number() {
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");
        let redemption_wallet =
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault_address).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
        ));
        let journal_manager =
            Arc::new(JournalManager::new(alpaca_service, cqrs.clone()));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault_address,
            redemption_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs,
            store,
            pool,
            redeem_call_manager,
            journal_manager,
        );

        let mut log = create_transfer_log(
            address!("0x9999999999999999999999999999999999999999"),
            redemption_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );

        log.block_number = None;

        let result = detector.process_transfer_log(&log).await;

        assert!(
            matches!(result, Err(RedemptionMonitorError::MissingBlockNumber)),
            "Expected MissingBlockNumber error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_process_transfer_log_no_matching_asset() {
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");
        let wrong_vault_address =
            address!("0x9876543210fedcba9876543210fedcba98765432");
        let redemption_wallet =
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(wrong_vault_address).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
        ));
        let journal_manager =
            Arc::new(JournalManager::new(alpaca_service, cqrs.clone()));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault_address,
            redemption_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs,
            store,
            pool,
            redeem_call_manager,
            journal_manager,
        );

        let log = create_transfer_log(
            address!("0x9999999999999999999999999999999999999999"),
            redemption_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );

        let result = detector.process_transfer_log(&log).await;

        assert!(
            matches!(
                result,
                Err(RedemptionMonitorError::NoMatchingAsset { .. })
            ),
            "Expected NoMatchingAsset error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_process_transfer_log_duplicate_detection_fails() {
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");
        let redemption_wallet =
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault_address).await;

        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let redeem_call_manager = Arc::new(RedeemCallManager::new(
            alpaca_service.clone(),
            cqrs.clone(),
        ));
        let journal_manager =
            Arc::new(JournalManager::new(alpaca_service, cqrs.clone()));

        let config = RedemptionDetectorConfig {
            rpc_url: "wss://fake.url".parse().unwrap(),
            vault_address,
            redemption_wallet,
        };

        let detector = RedemptionDetector::new(
            config,
            cqrs,
            store,
            pool,
            redeem_call_manager,
            journal_manager,
        );

        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            address!("0x9999999999999999999999999999999999999999"),
            redemption_wallet,
            U256::from_str_radix("100000000000000000000", 10).unwrap(),
            tx_hash,
            12345,
        );

        let first_result = detector.process_transfer_log(&log).await;
        assert!(
            first_result.is_ok(),
            "First detection should succeed, got {first_result:?}"
        );

        let second_result = detector.process_transfer_log(&log).await;
        assert!(
            matches!(
                second_result,
                Err(RedemptionMonitorError::CqrsExecution(_))
            ),
            "Second detection should fail with CQRS error, got {second_result:?}"
        );
    }
}
