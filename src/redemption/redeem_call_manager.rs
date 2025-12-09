use alloy::primitives::Address;
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::{
    IssuerRequestId, Redemption, RedemptionCommand, RedemptionError,
    RedemptionViewError, find_detected,
};
use crate::account::view::{AccountViewError, find_by_wallet};
use crate::account::{AccountView, AlpacaAccountNumber, ClientId};
use crate::alpaca::{AlpacaError, AlpacaService, RedeemRequest};
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};
use crate::tokenized_asset::{Network, TokenizedAssetView, UnderlyingSymbol};

pub(crate) struct RedeemCallManager<ES: EventStore<Redemption>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Redemption, ES>>,
    event_store: Arc<ES>,
    pool: Pool<Sqlite>,
}

impl<ES: EventStore<Redemption>> RedeemCallManager<ES> {
    pub(crate) fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        cqrs: Arc<CqrsFramework<Redemption, ES>>,
        event_store: Arc<ES>,
        pool: Pool<Sqlite>,
    ) -> Self {
        Self { alpaca_service, cqrs, event_store, pool }
    }

    pub(crate) async fn recover_detected_redemptions(&self) {
        debug!("Starting recovery of Detected redemptions");

        let stuck_redemptions = match find_detected(&self.pool).await {
            Ok(redemptions) => redemptions,
            Err(e) => {
                error!(error = %e, "Failed to query for stuck Detected redemptions");
                return;
            }
        };

        if stuck_redemptions.is_empty() {
            debug!("No Detected redemptions to recover");
            return;
        }

        info!(
            count = stuck_redemptions.len(),
            "Recovering stuck Detected redemptions"
        );

        for (issuer_request_id, _view) in stuck_redemptions {
            if let Err(e) =
                self.recover_single_detected(&issuer_request_id).await
            {
                warn!(
                    issuer_request_id = %issuer_request_id.0,
                    error = %e,
                    "Failed to recover Detected redemption"
                );
            }
        }

        debug!("Completed recovery of Detected redemptions");
    }

    async fn recover_single_detected(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<(), RedeemCallManagerError> {
        let aggregate_ctx =
            self.event_store.load_aggregate(&issuer_request_id.0).await?;

        let aggregate = aggregate_ctx.aggregate();

        let Redemption::Detected { metadata } = aggregate else {
            debug!(
                issuer_request_id = %issuer_request_id.0,
                "Redemption no longer in Detected state, skipping"
            );
            return Ok(());
        };

        let (client_id, alpaca_account) =
            self.lookup_account_for_recovery(&metadata.wallet).await?;

        let network =
            self.lookup_network_for_asset(&metadata.underlying).await?;

        info!(
            issuer_request_id = %issuer_request_id.0,
            "Recovering Detected redemption - calling Alpaca"
        );

        self.handle_redemption_detected(
            &alpaca_account,
            issuer_request_id,
            aggregate,
            client_id,
            network,
        )
        .await?;

        Ok(())
    }

    async fn lookup_account_for_recovery(
        &self,
        wallet: &alloy::primitives::Address,
    ) -> Result<(ClientId, AlpacaAccountNumber), RedeemCallManagerError> {
        let account_view = find_by_wallet(&self.pool, wallet).await?.ok_or(
            RedeemCallManagerError::AccountNotFound { wallet: *wallet },
        )?;

        match account_view {
            AccountView::LinkedToAlpaca {
                client_id, alpaca_account, ..
            } => Ok((client_id, alpaca_account)),
            _ => Err(RedeemCallManagerError::AccountNotLinked {
                wallet: *wallet,
            }),
        }
    }

    async fn lookup_network_for_asset(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<Network, RedeemCallManagerError> {
        let assets = list_enabled_assets(&self.pool).await?;

        for view in assets {
            if let TokenizedAssetView::Asset {
                underlying: u, network, ..
            } = view
            {
                if &u == underlying {
                    return Ok(network);
                }
            }
        }

        Err(RedeemCallManagerError::AssetNotFound {
            underlying: underlying.clone(),
        })
    }

    #[tracing::instrument(skip(self, aggregate), fields(
        issuer_request_id = %issuer_request_id.0,
        client_id = %client_id
    ))]
    pub(crate) async fn handle_redemption_detected(
        &self,
        alpaca_account: &AlpacaAccountNumber,
        issuer_request_id: &IssuerRequestId,
        aggregate: &Redemption,
        client_id: ClientId,
        network: Network,
    ) -> Result<(), RedeemCallManagerError> {
        let Redemption::Detected { metadata } = aggregate else {
            return Err(RedeemCallManagerError::InvalidAggregateState {
                current_state: aggregate.state_name().to_string(),
            });
        };

        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;
        let UnderlyingSymbol(underlying_str) = &metadata.underlying;

        info!(
            issuer_request_id = %issuer_request_id_str,
            underlying = %underlying_str,
            quantity = %metadata.quantity.0,
            wallet = %metadata.wallet,
            "Calling Alpaca redeem endpoint"
        );

        let request = RedeemRequest {
            issuer_request_id: issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            client_id,
            quantity: metadata.quantity.clone(),
            network,
            wallet: metadata.wallet,
            tx_hash: metadata.detected_tx_hash,
        };

        match self.alpaca_service.call_redeem_endpoint(request).await {
            Ok(response) => {
                info!(
                    issuer_request_id = %response.issuer_request_id.0,
                    tokenization_request_id = %response.tokenization_request_id.0,
                    r#type = ?response.r#type,
                    status = ?response.status,
                    created_at = %response.created_at,
                    issuer = %response.issuer,
                    underlying = %response.underlying.0,
                    token = %response.token.0,
                    quantity = %response.quantity.0,
                    network = %response.network.0,
                    wallet = %response.wallet,
                    tx_hash = %response.tx_hash,
                    fees = %response.fees.0,
                    "Alpaca redeem API call succeeded"
                );

                self.cqrs
                    .execute(
                        issuer_request_id_str,
                        RedemptionCommand::RecordAlpacaCall {
                            issuer_request_id: issuer_request_id.clone(),
                            tokenization_request_id: response
                                .tokenization_request_id,
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordAlpacaCall command executed successfully"
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_request_id_str,
                    error = %e,
                    "Alpaca redeem API call failed"
                );

                self.cqrs
                    .execute(
                        issuer_request_id_str,
                        RedemptionCommand::RecordAlpacaFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: e.to_string(),
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordAlpacaFailure command executed successfully"
                );

                Err(RedeemCallManagerError::Alpaca(e))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RedeemCallManagerError {
    #[error("Alpaca error: {0}")]
    Alpaca(#[from] AlpacaError),
    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<RedemptionError>),
    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },
    #[error("View error: {0}")]
    View(#[from] RedemptionViewError),
    #[error("Account view error: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Account not found for wallet: {wallet}")]
    AccountNotFound { wallet: Address },
    #[error("Account not linked for wallet: {wallet}")]
    AccountNotLinked { wallet: Address },
    #[error("Asset view error: {0}")]
    AssetView(#[from] TokenizedAssetViewError),
    #[error("Asset not found for underlying: {underlying}")]
    AssetNotFound { underlying: UnderlyingSymbol },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use cqrs_es::{AggregateContext, EventStore, mem_store::MemStore};
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::{RedeemCallManager, RedeemCallManagerError};
    use crate::account::{AccountView, AlpacaAccountNumber, ClientId, Email};
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::{IssuerRequestId, Quantity};
    use crate::redemption::{
        Redemption, RedemptionCommand, RedemptionView, UnderlyingSymbol,
    };
    use crate::tokenized_asset::{Network, TokenSymbol, TokenizedAssetView};

    type TestCqrs = cqrs_es::CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

    fn test_alpaca_account() -> AlpacaAccountNumber {
        AlpacaAccountNumber("test-account".to_string())
    }

    async fn setup_test_cqrs()
    -> (Arc<TestCqrs>, Arc<TestStore>, sqlx::Pool<sqlx::Sqlite>) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let store = Arc::new(MemStore::default());
        let cqrs =
            Arc::new(cqrs_es::CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store, pool)
    }

    async fn create_test_redemption_in_detected_state(
        cqrs: &TestCqrs,
        store: &TestStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Redemption {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        cqrs.execute(
            &issuer_request_id.0,
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
            },
        )
        .await
        .unwrap();

        load_aggregate(store, issuer_request_id).await
    }

    async fn load_aggregate(
        store: &TestStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Redemption {
        let context = store.load_aggregate(&issuer_request_id.0).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_redemption_detected_with_success() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        let issuer_request_id = IssuerRequestId::new("red-success-123");
        let aggregate = create_test_redemption_in_detected_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let client_id = ClientId::new();
        let network = Network::new("base");

        let result = manager
            .handle_redemption_detected(
                &test_alpaca_account(),
                &issuer_request_id,
                &aggregate,
                client_id,
                network,
            )
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        assert_eq!(alpaca_service_mock.get_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::AlpacaCalled { .. }),
            "Expected AlpacaCalled state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_redemption_detected_with_alpaca_failure() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock =
            Arc::new(MockAlpacaService::new_failure("API timeout"));
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        let issuer_request_id = IssuerRequestId::new("red-failure-456");
        let aggregate = create_test_redemption_in_detected_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let client_id = ClientId::new();
        let network = Network::new("base");

        let result = manager
            .handle_redemption_detected(
                &test_alpaca_account(),
                &issuer_request_id,
                &aggregate,
                client_id,
                network,
            )
            .await;

        assert!(
            matches!(result, Err(RedeemCallManagerError::Alpaca(_))),
            "Expected Alpaca error, got {result:?}"
        );

        assert_eq!(alpaca_service_mock.get_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("API timeout"),
            "Expected error message to contain 'API timeout', got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_handle_redemption_detected_with_wrong_state_fails() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager =
            RedeemCallManager::new(alpaca_service, cqrs.clone(), store, pool);

        let issuer_request_id = IssuerRequestId::new("red-wrong-state-789");
        let aggregate = Redemption::Uninitialized;

        let client_id = ClientId::new();
        let network = Network::new("base");

        let result = manager
            .handle_redemption_detected(
                &test_alpaca_account(),
                &issuer_request_id,
                &aggregate,
                client_id,
                network,
            )
            .await;

        assert!(
            matches!(
                result,
                Err(RedeemCallManagerError::InvalidAggregateState { .. })
            ),
            "Expected InvalidAggregateState error, got {result:?}"
        );
    }

    async fn insert_account_view(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        wallet: Address,
        client_id: &ClientId,
        alpaca_account: &AlpacaAccountNumber,
    ) {
        let view = AccountView::LinkedToAlpaca {
            client_id: *client_id,
            email: Email::new("test@example.com".to_string()).unwrap(),
            alpaca_account: alpaca_account.clone(),
            whitelisted_wallets: vec![wallet],
            registered_at: chrono::Utc::now(),
            linked_at: chrono::Utc::now(),
        };
        let payload = serde_json::to_string(&view).unwrap();
        let view_id = format!("{wallet:?}");
        sqlx::query!(
            r#"
            INSERT INTO account_view (view_id, version, payload)
            VALUES ($1, 1, $2)
            "#,
            view_id,
            payload
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_asset_view(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        underlying: &UnderlyingSymbol,
        network: &Network,
    ) {
        let view = TokenizedAssetView::Asset {
            underlying: underlying.clone(),
            token: TokenSymbol::new(format!("t{}", underlying.0)),
            network: network.clone(),
            vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            enabled: true,
            added_at: chrono::Utc::now(),
        };
        let payload = serde_json::to_string(&view).unwrap();
        sqlx::query!(
            r#"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ($1, 1, $2)
            "#,
            underlying.0,
            payload
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_redemption_view(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        view_id: &str,
        view: &RedemptionView,
    ) {
        let payload = serde_json::to_string(view).unwrap();
        sqlx::query!(
            r#"
            INSERT INTO redemption_view (view_id, version, payload)
            VALUES ($1, 1, $2)
            "#,
            view_id,
            payload
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_lookup_account_for_recovery_success() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager =
            RedeemCallManager::new(alpaca_service, cqrs, store, pool.clone());

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let client_id = ClientId::new();
        let alpaca_account = AlpacaAccountNumber("acc-123".to_string());

        insert_account_view(&pool, wallet, &client_id, &alpaca_account).await;

        let result = manager.lookup_account_for_recovery(&wallet).await;

        assert!(result.is_ok(), "Expected success, got {result:?}");
        let (found_client_id, found_alpaca_account) = result.unwrap();
        assert_eq!(found_client_id, client_id);
        assert_eq!(found_alpaca_account, alpaca_account);
    }

    #[tokio::test]
    async fn test_lookup_account_for_recovery_not_found() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, cqrs, store, pool);

        let wallet = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let result = manager.lookup_account_for_recovery(&wallet).await;

        assert!(
            matches!(
                result,
                Err(RedeemCallManagerError::AccountNotFound { .. })
            ),
            "Expected AccountNotFound, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_lookup_network_for_asset_success() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager =
            RedeemCallManager::new(alpaca_service, cqrs, store, pool.clone());

        let underlying = UnderlyingSymbol::new("AAPL");
        let network = Network::new("base");

        insert_asset_view(&pool, &underlying, &network).await;

        let result = manager.lookup_network_for_asset(&underlying).await;

        assert!(result.is_ok(), "Expected success, got {result:?}");
        assert_eq!(result.unwrap(), network);
    }

    #[tokio::test]
    async fn test_lookup_network_for_asset_not_found() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, cqrs, store, pool);

        let underlying = UnderlyingSymbol::new("UNKNOWN");

        let result = manager.lookup_network_for_asset(&underlying).await;

        assert!(
            matches!(result, Err(RedeemCallManagerError::AssetNotFound { .. })),
            "Expected AssetNotFound, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_detected_redemptions_empty() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, cqrs, store, pool);

        manager.recover_detected_redemptions().await;

        assert_eq!(
            alpaca_service_mock.get_call_count(),
            0,
            "Should not call Alpaca when no redemptions"
        );
    }

    #[tokio::test]
    async fn test_recover_detected_redemptions_with_valid_redemption() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        );

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let client_id = ClientId::new();
        let alpaca_account = AlpacaAccountNumber("acc-recovery".to_string());
        let underlying = UnderlyingSymbol::new("AAPL");
        let network = Network::new("base");

        insert_account_view(&pool, wallet, &client_id, &alpaca_account).await;
        insert_asset_view(&pool, &underlying, &network).await;

        let issuer_request_id = IssuerRequestId::new("red-recovery-1");
        create_test_redemption_in_detected_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let view = RedemptionView::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token: TokenSymbol::new("tAAPL"),
            wallet,
            quantity: Quantity::new(Decimal::from(100)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: chrono::Utc::now(),
        };
        insert_redemption_view(&pool, &issuer_request_id.0, &view).await;

        manager.recover_detected_redemptions().await;

        assert_eq!(
            alpaca_service_mock.get_call_count(),
            1,
            "Should call Alpaca once for the detected redemption"
        );

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::AlpacaCalled { .. }),
            "Expected AlpacaCalled state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_single_detected_missing_account() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        );

        let issuer_request_id = IssuerRequestId::new("red-no-account");
        create_test_redemption_in_detected_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager.recover_single_detected(&issuer_request_id).await;

        assert!(
            matches!(
                result,
                Err(RedeemCallManagerError::AccountNotFound { .. })
            ),
            "Expected AccountNotFound, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_single_detected_missing_asset() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        );

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let client_id = ClientId::new();
        let alpaca_account = AlpacaAccountNumber("acc-no-asset".to_string());
        insert_account_view(&pool, wallet, &client_id, &alpaca_account).await;

        let issuer_request_id = IssuerRequestId::new("red-no-asset");
        create_test_redemption_in_detected_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager.recover_single_detected(&issuer_request_id).await;

        assert!(
            matches!(result, Err(RedeemCallManagerError::AssetNotFound { .. })),
            "Expected AssetNotFound, got {result:?}"
        );
    }
}
