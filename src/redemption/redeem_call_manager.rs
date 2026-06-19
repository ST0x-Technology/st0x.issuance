use alloy::primitives::Address;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::{
    IssuerRedemptionRequestId, Redemption, RedemptionCommand,
    RedemptionViewError, find_detected,
};
use crate::QuantityConversionError;
use crate::account::view::{AccountViewError, find_by_wallet};
use crate::account::{AccountView, AlpacaAccountNumber, ClientId};
use crate::alpaca::{AlpacaError, AlpacaService, RedeemRequest};
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};
use crate::tokenized_asset::{Network, UnderlyingSymbol};

pub(crate) struct RedeemCallManager {
    alpaca_service: Arc<dyn AlpacaService>,
    store: Arc<Store<Redemption>>,
    pool: Pool<Sqlite>,
}

impl RedeemCallManager {
    pub(crate) fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        store: Arc<Store<Redemption>>,
        pool: Pool<Sqlite>,
    ) -> Self {
        Self { alpaca_service, store, pool }
    }

    pub(crate) async fn recover_detected_redemptions(&self) {
        let stuck_redemptions = match find_detected(&self.pool).await {
            Ok(redemptions) => redemptions,
            Err(err) => {
                error!(target: "redemption", error = %err, "Failed to query for stuck Detected redemptions");
                return;
            }
        };

        if stuck_redemptions.is_empty() {
            debug!(target: "redemption", "No Detected redemptions to recover");
            return;
        }

        info!(target: "redemption", count = stuck_redemptions.len(),
            "Recovering stuck Detected redemptions"
        );

        let mut recovered = 0u32;
        let mut auto_failed = 0u32;
        let mut failed = 0u32;

        for (issuer_request_id, _view) in &stuck_redemptions {
            match self.recover_single_detected(issuer_request_id).await {
                Ok(()) => {
                    recovered += 1;
                }
                Err(
                    RedeemCallManagerError::AccountNotFound { .. }
                    | RedeemCallManagerError::AccountNotLinked { .. },
                ) => {
                    let command = RedemptionCommand::MarkFailed {
                        issuer_request_id: issuer_request_id.clone(),
                        reason: "No linked account found for redemption wallet"
                            .to_string(),
                    };

                    if let Err(err) =
                        self.store.send(issuer_request_id, command).await
                    {
                        debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                            error = %err,
                            "Failed to mark unrecoverable Detected redemption as failed"
                        );
                        failed += 1;
                    } else {
                        debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                            "Auto-failed Detected redemption with no linked account"
                        );
                        auto_failed += 1;
                    }
                }
                Err(err) => {
                    debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                        error = %err,
                        "Failed to recover Detected redemption"
                    );
                    failed += 1;
                }
            }
        }

        info!(target: "redemption", total = stuck_redemptions.len(),
            recovered,
            auto_failed,
            failed,
            "Detected redemption recovery complete"
        );
    }

    async fn recover_single_detected(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<(), RedeemCallManagerError> {
        let Some(aggregate) = self.store.load(issuer_request_id).await? else {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "Redemption not found, skipping"
            );
            return Ok(());
        };

        let Redemption::Detected { metadata } = &aggregate else {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "Redemption no longer in Detected state, skipping"
            );
            return Ok(());
        };

        let (client_id, alpaca_account) =
            self.lookup_account_for_recovery(&metadata.wallet).await?;

        let network =
            self.lookup_network_for_asset(&metadata.underlying).await?;

        debug!(target: "redemption", issuer_request_id = %issuer_request_id,
            "Recovering Detected redemption - calling Alpaca"
        );

        self.handle_redemption_detected(
            &alpaca_account,
            issuer_request_id,
            &aggregate,
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
            AccountView::Registered { .. } => {
                Err(RedeemCallManagerError::AccountNotLinked {
                    wallet: *wallet,
                })
            }
        }
    }

    async fn lookup_network_for_asset(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<Network, RedeemCallManagerError> {
        let assets = list_enabled_assets(&self.pool).await?;

        assets
            .into_iter()
            .find(|asset| &asset.underlying == underlying)
            .map(|asset| asset.network)
            .ok_or_else(|| RedeemCallManagerError::AssetNotFound {
                underlying: underlying.clone(),
            })
    }

    #[tracing::instrument(skip(self, aggregate), fields(
        issuer_request_id = %issuer_request_id,
        client_id = %client_id
    ))]
    pub(crate) async fn handle_redemption_detected(
        &self,
        alpaca_account: &AlpacaAccountNumber,
        issuer_request_id: &IssuerRedemptionRequestId,
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

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
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
                info!(target: "redemption", issuer_request_id = %response.issuer_request_id,
                    tokenization_request_id = %response.tokenization_request_id.0,
                    r#type = ?response.r#type,
                    status = ?response.status,
                    created_at = %response.created_at,
                    issuer = %response.issuer,
                    underlying = %response.underlying.0,
                    token = %response.token.0,
                    quantity = %response.quantity.0,
                    network = %response.network,
                    wallet = %response.wallet,
                    tx_hash = %response.tx_hash,
                    fees = ?response.fees.as_ref().map(|f| f.0),
                    "Alpaca redeem API call succeeded"
                );

                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordAlpacaCall {
                            issuer_request_id: issuer_request_id.clone(),
                            tokenization_request_id: response
                                .tokenization_request_id,
                            alpaca_quantity,
                            dust_quantity,
                        },
                    )
                    .await?;

                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "RecordAlpacaCall command executed successfully"
                );

                Ok(())
            }
            Err(err) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Alpaca redeem API call failed"
                );

                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordAlpacaFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: err.to_string(),
                        },
                    )
                    .await?;

                info!(target: "redemption", issuer_request_id = %issuer_request_id,
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
    Cqrs(Box<AggregateError<LifecycleError<Redemption>>>),
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

// `AggregateError<LifecycleError<Redemption>>` is large (it can carry a full
// Redemption aggregate), so it's boxed to keep `RedeemCallManagerError` small.
impl From<AggregateError<LifecycleError<Redemption>>>
    for RedeemCallManagerError
{
    fn from(error: AggregateError<LifecycleError<Redemption>>) -> Self {
        Self::Cqrs(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use chrono::Utc;
    use event_sorcery::{Store, StoreBuilder};
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{RedeemCallManager, RedeemCallManagerError};
    use crate::account::{
        Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::Quantity;
    use crate::redemption::view::RedemptionViewReactor;
    use crate::redemption::{
        IssuerRedemptionRequestId, Redemption, RedemptionCommand,
        UnderlyingSymbol,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    };
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    fn test_alpaca_account() -> AlpacaAccountNumber {
        AlpacaAccountNumber("test-account".to_string())
    }

    /// Builds the full set of event-sorcery stores backed by one in-memory
    /// SQLite pool: a [`Store<Redemption>`] wired with the
    /// [`RedemptionViewReactor`] (so the recovery sweeps that read
    /// `redemption_view` via `find_detected` see redemptions), plus account and
    /// asset stores whose projections back the recovery lookups.
    struct TestHarness {
        pool: sqlx::Pool<sqlx::Sqlite>,
        redemption_store: Arc<Store<Redemption>>,
        account_store: Arc<Store<Account>>,
        asset_store: Arc<Store<TokenizedAsset>>,
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

            let vault_service: Arc<dyn VaultService> =
                Arc::new(MockVaultService::new_success());
            let redemption_store =
                StoreBuilder::<Redemption>::new(pool.clone())
                    .with(Arc::new(RedemptionViewReactor::new(pool.clone())))
                    .build(vault_service)
                    .await
                    .expect("Failed to build redemption store");

            let (account_store, _account_projection) =
                StoreBuilder::<Account>::new(pool.clone())
                    .build(())
                    .await
                    .expect("Failed to build account store");

            let (asset_store, _asset_projection) =
                StoreBuilder::<TokenizedAsset>::new(pool.clone())
                    .build(())
                    .await
                    .expect("Failed to build tokenized asset store");

            Self { pool, redemption_store, account_store, asset_store }
        }

        async fn register_and_link_account(
            &self,
            client_id: ClientId,
            email: &str,
            alpaca_account: &AlpacaAccountNumber,
            wallet: Address,
        ) {
            let email = Email::new(email).unwrap();

            self.account_store
                .send(&client_id, AccountCommand::Register { client_id, email })
                .await
                .expect("Failed to register account");

            self.account_store
                .send(
                    &client_id,
                    AccountCommand::LinkToAlpaca {
                        alpaca_account: alpaca_account.clone(),
                    },
                )
                .await
                .expect("Failed to link to Alpaca");

            self.account_store
                .send(&client_id, AccountCommand::WhitelistWallet { wallet })
                .await
                .expect("Failed to whitelist wallet");
        }

        async fn add_asset(
            &self,
            underlying: &UnderlyingSymbol,
            network: &Network,
        ) {
            self.asset_store
                .send(
                    underlying,
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
            issuer_request_id: &IssuerRedemptionRequestId,
            underlying: &UnderlyingSymbol,
            wallet: Address,
        ) {
            self.redemption_store
                .send(
                    issuer_request_id,
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
        ) -> RedeemCallManager {
            RedeemCallManager::new(
                alpaca_service,
                self.redemption_store.clone(),
                self.pool.clone(),
            )
        }
    }

    async fn setup_test_store()
    -> (Arc<Store<Redemption>>, sqlx::Pool<sqlx::Sqlite>) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let vault_service: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success());
        let store = StoreBuilder::<Redemption>::new(pool.clone())
            .with(Arc::new(RedemptionViewReactor::new(pool.clone())))
            .build(vault_service)
            .await
            .expect("Failed to build redemption store");

        (store, pool)
    }

    async fn create_test_redemption_in_detected_state(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Redemption {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        store
            .send(
                issuer_request_id,
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

        store.load(issuer_request_id).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn test_handle_redemption_detected_with_success() {
        let (store, pool) = setup_test_store().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager =
            RedeemCallManager::new(alpaca_service, store.clone(), pool);

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate = create_test_redemption_in_detected_state(
            &store,
            &issuer_request_id,
        )
        .await;

        let client_id = ClientId::new();
        let network = Network::Base;

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
            store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(updated_aggregate, Redemption::AlpacaCalled { .. }),
            "Expected AlpacaCalled state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_redemption_detected_with_alpaca_failure() {
        let (store, pool) = setup_test_store().await;
        let alpaca_service_mock =
            Arc::new(MockAlpacaService::new_failure("API timeout"));
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager =
            RedeemCallManager::new(alpaca_service, store.clone(), pool);

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate = create_test_redemption_in_detected_state(
            &store,
            &issuer_request_id,
        )
        .await;

        let client_id = ClientId::new();
        let network = Network::Base;

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
            store.load(&issuer_request_id).await.unwrap().unwrap();

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
        let (store, pool) = setup_test_store().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, store, pool);

        let issuer_request_id = IssuerRedemptionRequestId::random();
        // A non-Detected aggregate exercises the InvalidAggregateState guard.
        let aggregate = Redemption::Completed {
            issuer_request_id: issuer_request_id.clone(),
            burn_tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            completed_at: Utc::now(),
        };

        let client_id = ClientId::new();
        let network = Network::Base;

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
        let (store, pool) = setup_test_store().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, store, pool);

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
        let network = Network::Base;

        harness.add_asset(&underlying, &network).await;

        let result = manager.lookup_network_for_asset(&underlying).await;

        assert!(result.is_ok(), "Expected success, got {result:?}");
        assert_eq!(result.unwrap(), network);
    }

    #[tokio::test]
    async fn test_lookup_network_for_asset_not_found() {
        let (store, pool) = setup_test_store().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, store, pool);

        let underlying = UnderlyingSymbol::new("UNKNOWN");

        let result = manager.lookup_network_for_asset(&underlying).await;

        assert!(
            matches!(result, Err(RedeemCallManagerError::AssetNotFound { .. })),
            "Expected AssetNotFound, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_detected_redemptions_empty() {
        let (store, pool) = setup_test_store().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, store, pool);

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
        let network = Network::Base;

        harness
            .register_and_link_account(
                client_id,
                "test@example.com",
                &alpaca_account,
                wallet,
            )
            .await;
        harness.add_asset(&underlying, &network).await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(&issuer_request_id, &underlying, wallet)
            .await;

        manager.recover_detected_redemptions().await;

        assert_eq!(
            alpaca_service_mock.get_call_count(),
            1,
            "Should call Alpaca once for the detected redemption"
        );

        let updated_aggregate = harness
            .redemption_store
            .load(&issuer_request_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(updated_aggregate, Redemption::AlpacaCalled { .. }),
            "Expected AlpacaCalled state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_single_detected_missing_account() {
        let (store, pool) = setup_test_store().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager =
            RedeemCallManager::new(alpaca_service, store.clone(), pool.clone());

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_detected_state(&store, &issuer_request_id)
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

    #[traced_test]
    #[tokio::test]
    async fn test_recover_detected_missing_account_marks_failed() {
        let harness = TestHarness::new().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = harness.create_manager(alpaca_service);

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let underlying = UnderlyingSymbol::new("AAPL");

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(&issuer_request_id, &underlying, wallet)
            .await;

        // No account registered for this wallet — recovery should auto-fail
        manager.recover_detected_redemptions().await;

        let updated_aggregate = harness
            .redemption_store
            .load(&issuer_request_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(updated_aggregate, Redemption::Failed { .. }),
            "Expected Detected redemption with missing account to be auto-failed, \
             got {updated_aggregate:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Auto-failed Detected redemption", "no linked account"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Detected redemption recovery complete", "auto_failed=1"]
        ));
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
