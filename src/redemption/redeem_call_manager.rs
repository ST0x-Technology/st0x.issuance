use alloy::primitives::Address;
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::{
    IssuerRequestId, Redemption, RedemptionCommand, RedemptionError,
    RedemptionViewError, find_detected,
};
use crate::QuantityConversionError;
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
            Err(err) => {
                error!(error = %err, "Failed to query for stuck Detected redemptions");
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
            if let Err(err) =
                self.recover_single_detected(&issuer_request_id).await
            {
                warn!(
                    issuer_request_id = %issuer_request_id.as_str(),
                    error = %err,
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
            self.event_store.load_aggregate(issuer_request_id.as_str()).await?;

        let aggregate = aggregate_ctx.aggregate();

        let Redemption::Detected { metadata } = aggregate else {
            debug!(
                issuer_request_id = %issuer_request_id.as_str(),
                "Redemption no longer in Detected state, skipping"
            );
            return Ok(());
        };

        let (client_id, alpaca_account) =
            self.lookup_account_for_recovery(&metadata.wallet).await?;

        let network =
            self.lookup_network_for_asset(&metadata.underlying).await?;

        info!(
            issuer_request_id = %issuer_request_id.as_str(),
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
        issuer_request_id = %issuer_request_id.as_str(),
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

        // Truncate to 9 decimals for Alpaca - they don't support 18 decimal precision
        let (alpaca_quantity, dust_quantity) =
            metadata.quantity.truncate_for_alpaca()?;

        info!(
            issuer_request_id = %issuer_request_id,
            underlying = %metadata.underlying,
            original_quantity = %metadata.quantity.0,
            alpaca_quantity = %alpaca_quantity.0,
            dust_quantity = %dust_quantity.0,
            wallet = %metadata.wallet,
            "Calling Alpaca redeem endpoint"
        );

        let request = RedeemRequest {
            issuer_request_id: issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            client_id,
            quantity: alpaca_quantity.clone(),
            network,
            wallet: metadata.wallet,
            tx_hash: metadata.detected_tx_hash,
        };

        match self.alpaca_service.call_redeem_endpoint(request).await {
            Ok(response) => {
                info!(
                    issuer_request_id = %response.issuer_request_id.as_str(),
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
                    fees = ?response.fees.as_ref().map(|f| f.0),
                    "Alpaca redeem API call succeeded"
                );

                self.cqrs
                    .execute(
                        issuer_request_id.as_str(),
                        RedemptionCommand::RecordAlpacaCall {
                            issuer_request_id: issuer_request_id.clone(),
                            tokenization_request_id: response
                                .tokenization_request_id,
                            alpaca_quantity,
                            dust_quantity,
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id,
                    "RecordAlpacaCall command executed successfully"
                );

                Ok(())
            }
            Err(err) => {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Alpaca redeem API call failed"
                );

                self.cqrs
                    .execute(
                        issuer_request_id.as_str(),
                        RedemptionCommand::RecordAlpacaFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: err.to_string(),
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id,
                    "RecordAlpacaFailure command executed successfully"
                );

                Err(RedeemCallManagerError::Alpaca(err))
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
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use cqrs_es::persist::{GenericQuery, PersistedEventStore};
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use rust_decimal::Decimal;
    use sqlite_es::{
        SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::{RedeemCallManager, RedeemCallManagerError};
    use crate::account::{
        Account, AccountCommand, AccountView, AlpacaAccountNumber, ClientId,
        Email,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::{IssuerRequestId, Quantity};
    use crate::redemption::{
        Redemption, RedemptionCommand, RedemptionView, UnderlyingSymbol,
    };
    use crate::tokenized_asset::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetView,
    };
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    type TestCqrs = cqrs_es::CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

    fn test_alpaca_account() -> AlpacaAccountNumber {
        AlpacaAccountNumber("test-account".to_string())
    }

    type RedemptionStore =
        PersistedEventStore<SqliteEventRepository, Redemption>;

    struct TestHarness {
        pool: sqlx::Pool<sqlx::Sqlite>,
        redemption_store: Arc<RedemptionStore>,
        redemption_cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
        account_cqrs: SqliteCqrs<Account>,
        asset_cqrs: SqliteCqrs<TokenizedAsset>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let redemption_store =
                Arc::new(PersistedEventStore::new_event_store(
                    SqliteEventRepository::new(pool.clone()),
                ));
            let redemption_view_repo = Arc::new(SqliteViewRepository::<
                RedemptionView,
                Redemption,
            >::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
            let redemption_query = GenericQuery::new(redemption_view_repo);
            let vault_service = Arc::new(MockVaultService::new_success());
            let redemption_cqrs = Arc::new(CqrsFramework::new(
                PersistedEventStore::new_event_store(
                    SqliteEventRepository::new(pool.clone()),
                ),
                vec![Box::new(redemption_query)],
                vault_service,
            ));

            let account_view_repo =
                Arc::new(SqliteViewRepository::<AccountView, Account>::new(
                    pool.clone(),
                    "account_view".to_string(),
                ));
            let account_query = GenericQuery::new(account_view_repo);
            let account_cqrs =
                sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

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

            Self {
                pool,
                redemption_store,
                redemption_cqrs,
                account_cqrs,
                asset_cqrs,
            }
        }

        async fn register_and_link_account(
            &self,
            client_id: ClientId,
            email: &str,
            alpaca_account: &AlpacaAccountNumber,
            wallet: Address,
        ) {
            let email = Email::new(email.to_string()).unwrap();

            self.account_cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::Register { client_id, email },
                )
                .await
                .expect("Failed to register account");

            self.account_cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::LinkToAlpaca {
                        alpaca_account: alpaca_account.clone(),
                    },
                )
                .await
                .expect("Failed to link to Alpaca");

            self.account_cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::WhitelistWallet { wallet },
                )
                .await
                .expect("Failed to whitelist wallet");
        }

        async fn add_asset(
            &self,
            underlying: &UnderlyingSymbol,
            network: &Network,
        ) {
            self.asset_cqrs
                .execute(
                    &underlying.0,
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!("t{}", underlying.0)),
                        network: network.clone(),
                        vault: address!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                    },
                )
                .await
                .expect("Failed to add asset");
        }

        async fn detect_redemption(
            &self,
            issuer_request_id: &IssuerRequestId,
            underlying: &UnderlyingSymbol,
            wallet: Address,
        ) {
            self.redemption_cqrs
                .execute(
                    issuer_request_id.as_str(),
                    RedemptionCommand::Detect {
                        issuer_request_id: issuer_request_id.clone(),
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!("t{}", underlying.0)),
                        wallet,
                        quantity: Quantity::new(Decimal::from(100)),
                        tx_hash: b256!(
                            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                        ),
                        block_number: 12345,
                    },
                )
                .await
                .expect("Failed to detect redemption");
        }

        fn create_manager(
            &self,
            alpaca_service: Arc<dyn crate::alpaca::AlpacaService>,
        ) -> RedeemCallManager<RedemptionStore> {
            RedeemCallManager::new(
                alpaca_service,
                self.redemption_cqrs.clone(),
                self.redemption_store.clone(),
                self.pool.clone(),
            )
        }
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
        let vault_service: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success());
        let cqrs = Arc::new(cqrs_es::CqrsFramework::new(
            (*store).clone(),
            vec![],
            vault_service,
        ));
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
            issuer_request_id.as_str(),
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
        let context =
            store.load_aggregate(issuer_request_id.as_str()).await.unwrap();
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

    #[tokio::test]
    async fn test_lookup_account_for_recovery_success() {
        let harness = TestHarness::new().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = harness.create_manager(alpaca_service);

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let client_id = ClientId::new();
        let alpaca_account = AlpacaAccountNumber("acc-123".to_string());

        harness
            .register_and_link_account(
                client_id,
                "test@example.com",
                &alpaca_account,
                wallet,
            )
            .await;

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
        let harness = TestHarness::new().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = harness.create_manager(alpaca_service);

        let underlying = UnderlyingSymbol::new("AAPL");
        let network = Network::new("base");

        harness.add_asset(&underlying, &network).await;

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
        let harness = TestHarness::new().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = harness.create_manager(alpaca_service);

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let client_id = ClientId::new();
        let alpaca_account = AlpacaAccountNumber("acc-recovery".to_string());
        let underlying = UnderlyingSymbol::new("AAPL");
        let network = Network::new("base");

        harness
            .register_and_link_account(
                client_id,
                "test@example.com",
                &alpaca_account,
                wallet,
            )
            .await;
        harness.add_asset(&underlying, &network).await;

        let issuer_request_id = IssuerRequestId::new("red-recovery-1");
        harness
            .detect_redemption(&issuer_request_id, &underlying, wallet)
            .await;

        manager.recover_detected_redemptions().await;

        assert_eq!(
            alpaca_service_mock.get_call_count(),
            1,
            "Should call Alpaca once for the detected redemption"
        );

        let context = harness
            .redemption_store
            .load_aggregate(issuer_request_id.as_str())
            .await
            .unwrap();
        let updated_aggregate = context.aggregate();
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
        let harness = TestHarness::new().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = harness.create_manager(alpaca_service);

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let client_id = ClientId::new();
        let alpaca_account = AlpacaAccountNumber("acc-no-asset".to_string());
        let underlying = UnderlyingSymbol::new("AAPL");

        harness
            .register_and_link_account(
                client_id,
                "test@example.com",
                &alpaca_account,
                wallet,
            )
            .await;

        let issuer_request_id = IssuerRequestId::new("red-no-asset");
        harness
            .detect_redemption(&issuer_request_id, &underlying, wallet)
            .await;

        let result = manager.recover_single_detected(&issuer_request_id).await;

        assert!(
            matches!(result, Err(RedeemCallManagerError::AssetNotFound { .. })),
            "Expected AssetNotFound, got {result:?}"
        );
    }
}
