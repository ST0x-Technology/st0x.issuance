use std::sync::Arc;

use alloy::primitives::Address;
use chrono::Utc;
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use tracing::{debug, error, info, warn};

use crate::tokenized_asset::UnderlyingSymbol;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, find_vault_by_underlying,
};
use crate::vault::{
    MintResult, OperationType, ReceiptInformation, VaultError, VaultService,
};

use super::view::{MintViewError, find_journal_confirmed};
use super::{
    IssuerRequestId, Mint, MintCommand, MintError, QuantityConversionError,
};

/// Orchestrates the on-chain minting process in response to JournalConfirmed events.
///
/// The manager bridges ES/CQRS aggregates with external blockchain services. It reacts to
/// JournalConfirmed events by calling the blockchain service to mint tokens, then records
/// the result (success or failure) back into the Mint aggregate via commands.
///
/// This pattern keeps aggregates pure (no side effects in command handlers) while enabling
/// integration with external systems.
pub(crate) struct MintManager<ES: EventStore<Mint>> {
    blockchain_service: Arc<dyn VaultService>,
    cqrs: Arc<CqrsFramework<Mint, ES>>,
    event_store: Arc<ES>,
    pool: Pool<Sqlite>,
    bot: Address,
}

impl<ES: EventStore<Mint>> MintManager<ES> {
    /// Creates a new mint manager.
    ///
    /// # Arguments
    ///
    /// * `blockchain_service` - Service for on-chain minting operations
    /// * `cqrs` - CQRS framework for executing commands on the Mint aggregate
    /// * `event_store` - Event store for loading aggregates
    /// * `pool` - Database pool for querying views
    /// * `bot` - Bot's address that receives ERC1155 receipts
    pub(crate) fn new(
        blockchain_service: Arc<dyn VaultService>,
        cqrs: Arc<CqrsFramework<Mint, ES>>,
        event_store: Arc<ES>,
        pool: Pool<Sqlite>,
        bot: Address,
    ) -> Self {
        Self { blockchain_service, cqrs, event_store, pool, bot }
    }

    /// Handles a JournalConfirmed event by minting tokens on-chain.
    ///
    /// This method orchestrates the complete on-chain minting flow:
    /// 1. Validates the aggregate is in JournalConfirmed state
    /// 2. Converts quantity to U256 with 18 decimals
    /// 3. Calls blockchain service to mint tokens
    /// 4. Records success (RecordMintSuccess) or failure (RecordMintFailure) via commands
    ///
    /// # Arguments
    ///
    /// * `issuer_request_id` - ID of the mint request
    /// * `aggregate` - Current state of the Mint aggregate (must be JournalConfirmed)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if minting succeeded and RecordMintSuccess command was executed.
    /// Returns `Err(MintManagerError::Blockchain)` if minting failed (RecordMintFailure
    /// command is still executed to record the failure).
    ///
    /// # Errors
    ///
    /// * `MintManagerError::InvalidAggregateState` - Aggregate is not in JournalConfirmed state
    /// * `MintManagerError::QuantityConversion` - Quantity cannot be converted to U256
    /// * `MintManagerError::Blockchain` - Blockchain transaction failed
    /// * `MintManagerError::Cqrs` - Command execution failed
    #[tracing::instrument(skip(self, aggregate), fields(
        issuer_request_id = %issuer_request_id.0
    ))]
    pub(crate) async fn handle_journal_confirmed(
        &self,
        issuer_request_id: &IssuerRequestId,
        aggregate: &Mint,
    ) -> Result<(), MintManagerError> {
        let Mint::JournalConfirmed {
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            ..
        } = aggregate
        else {
            return Err(MintManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(aggregate).to_string(),
            });
        };

        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;
        let UnderlyingSymbol(underlying_str) = underlying;

        let vault = find_vault_by_underlying(&self.pool, underlying)
            .await?
            .ok_or_else(|| MintManagerError::AssetNotFound {
                underlying: underlying_str.clone(),
            })?;

        info!(
            issuer_request_id = %issuer_request_id_str,
            underlying = %underlying_str,
            quantity = %quantity.0,
            wallet = %wallet,
            vault = %vault,
            "Starting on-chain minting process"
        );

        // Transition to Minting state before blockchain call to prevent duplicate mints
        // on recovery. Once in Minting state, this mint won't be picked up by
        // `find_journal_confirmed()` if we crash before completing.
        self.cqrs
            .execute(
                issuer_request_id_str,
                MintCommand::StartMinting {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await?;

        debug!(
            issuer_request_id = %issuer_request_id_str,
            "StartMinting command executed, proceeding with blockchain call"
        );

        let assets = quantity.to_u256_with_18_decimals()?;

        let receipt_info = ReceiptInformation {
            tokenization_request_id: tokenization_request_id.clone(),
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            quantity: quantity.clone(),
            operation_type: OperationType::Mint,
            timestamp: Utc::now(),
            notes: None,
        };

        match self
            .blockchain_service
            .mint_and_transfer_shares(
                vault,
                assets,
                self.bot,
                *wallet,
                receipt_info,
            )
            .await
        {
            Ok(result) => {
                self.record_mint_success(issuer_request_id, result).await
            }
            Err(e) => self.record_mint_failure(issuer_request_id, e).await,
        }
    }

    async fn record_mint_success(
        &self,
        issuer_request_id: &IssuerRequestId,
        result: MintResult,
    ) -> Result<(), MintManagerError> {
        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;

        info!(
            issuer_request_id = %issuer_request_id_str,
            tx_hash = %result.tx_hash,
            receipt_id = %result.receipt_id,
            shares_minted = %result.shares_minted,
            gas_used = result.gas_used,
            block_number = result.block_number,
            "On-chain minting succeeded"
        );

        self.cqrs
            .execute(
                issuer_request_id_str,
                MintCommand::RecordMintSuccess {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: result.tx_hash,
                    receipt_id: result.receipt_id,
                    shares_minted: result.shares_minted,
                    gas_used: result.gas_used,
                    block_number: result.block_number,
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id_str,
            "RecordMintSuccess command executed successfully"
        );

        Ok(())
    }

    async fn record_mint_failure(
        &self,
        issuer_request_id: &IssuerRequestId,
        error: VaultError,
    ) -> Result<(), MintManagerError> {
        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;

        warn!(
            issuer_request_id = %issuer_request_id_str,
            error = %error,
            "On-chain minting failed"
        );

        self.cqrs
            .execute(
                issuer_request_id_str,
                MintCommand::RecordMintFailure {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error.to_string(),
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id_str,
            "RecordMintFailure command executed successfully"
        );

        Err(MintManagerError::Blockchain(error))
    }

    /// Recovers mints stuck in JournalConfirmed state after a restart.
    ///
    /// This method queries for all mints in JournalConfirmed state and attempts to
    /// process them by calling `handle_journal_confirmed` for each. Errors during
    /// processing are logged but do not stop recovery of other mints.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn recover_journal_confirmed_mints(&self) {
        debug!("Starting recovery of JournalConfirmed mints");

        let stuck_mints = match find_journal_confirmed(&self.pool).await {
            Ok(mints) => mints,
            Err(e) => {
                error!(error = %e, "Failed to query for stuck JournalConfirmed mints");
                return;
            }
        };

        if stuck_mints.is_empty() {
            debug!("No JournalConfirmed mints to recover");
            return;
        }

        info!(
            count = stuck_mints.len(),
            "Recovering stuck JournalConfirmed mints"
        );

        for (issuer_request_id, _view) in stuck_mints {
            let aggregate = match self
                .event_store
                .load_aggregate(&issuer_request_id.0)
                .await
            {
                Ok(context) => context.aggregate().clone(),
                Err(e) => {
                    error!(
                        issuer_request_id = %issuer_request_id.0,
                        error = %e,
                        "Failed to load aggregate for recovery"
                    );
                    continue;
                }
            };

            match self
                .handle_journal_confirmed(&issuer_request_id, &aggregate)
                .await
            {
                Ok(()) => {
                    info!(
                        issuer_request_id = %issuer_request_id.0,
                        "Successfully recovered JournalConfirmed mint"
                    );
                }
                Err(e) => {
                    error!(
                        issuer_request_id = %issuer_request_id.0,
                        error = %e,
                        "Failed to recover JournalConfirmed mint"
                    );
                }
            }
        }

        debug!("Completed recovery of JournalConfirmed mints");
    }
}

const fn aggregate_state_name(aggregate: &Mint) -> &'static str {
    match aggregate {
        Mint::Uninitialized => "Uninitialized",
        Mint::Initiated { .. } => "Initiated",
        Mint::JournalConfirmed { .. } => "JournalConfirmed",
        Mint::JournalRejected { .. } => "JournalRejected",
        Mint::Minting { .. } => "Minting",
        Mint::CallbackPending { .. } => "CallbackPending",
        Mint::MintingFailed { .. } => "MintingFailed",
        Mint::Completed { .. } => "Completed",
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintManagerError {
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] VaultError),
    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<MintError>),
    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("Mint view error: {0}")]
    MintView(#[from] MintViewError),
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error("Asset not found for underlying: {underlying}")]
    AssetNotFound { underlying: String },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::{Address, address};
    use cqrs_es::persist::{GenericQuery, PersistedEventStore};
    use cqrs_es::{AggregateContext, EventStore, mem_store::MemStore};
    use rust_decimal::Decimal;
    use sqlite_es::{
        SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
    };
    use sqlx::sqlite::SqlitePoolOptions;

    use crate::mint::{
        ClientId, IssuerRequestId, Mint, MintCommand, MintView, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, TokenizedAssetView,
    };
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    use super::{MintManager, MintManagerError};

    type TestMintCqrs = cqrs_es::CqrsFramework<Mint, MemStore<Mint>>;
    type TestMintStore = MemStore<Mint>;

    struct TestHarness {
        cqrs: Arc<TestMintCqrs>,
        store: Arc<TestMintStore>,
        pool: sqlx::Pool<sqlx::Sqlite>,
        asset_cqrs: SqliteCqrs<TokenizedAsset>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let store = Arc::new(MemStore::default());
            let cqrs = Arc::new(cqrs_es::CqrsFramework::new(
                (*store).clone(),
                vec![],
                (),
            ));

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let tokenized_asset_view_repo = Arc::new(SqliteViewRepository::<
                TokenizedAssetView,
                TokenizedAsset,
            >::new(
                pool.clone(),
                "tokenized_asset_view".to_string(),
            ));
            let tokenized_asset_query =
                GenericQuery::new(tokenized_asset_view_repo);
            let asset_cqrs = sqlite_cqrs(
                pool.clone(),
                vec![Box::new(tokenized_asset_query)],
                (),
            );

            Self { cqrs, store, pool, asset_cqrs }
        }

        async fn add_asset(
            &self,
            underlying: &UnderlyingSymbol,
            vault: Address,
        ) {
            self.asset_cqrs
                .execute(
                    &underlying.0,
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!("t{}", underlying.0)),
                        network: Network::new("base"),
                        vault,
                    },
                )
                .await
                .expect("Failed to add tokenized asset");
        }
    }

    async fn create_test_mint_in_journal_confirmed_state(
        cqrs: &TestMintCqrs,
        store: &TestMintStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Mint {
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        cqrs.execute(
            &issuer_request_id.0,
            MintCommand::Initiate {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id,
                wallet,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &issuer_request_id.0,
            MintCommand::ConfirmJournal {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        load_aggregate(store, issuer_request_id).await
    }

    async fn load_aggregate(
        store: &TestMintStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Mint {
        let context = store.load_aggregate(&issuer_request_id.0).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_success() {
        let harness = TestHarness::new().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let TestHarness { cqrs, store, pool, .. } = harness;

        let blockchain_service_mock = Arc::new(MockVaultService::new_success());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(
            blockchain_service,
            cqrs.clone(),
            store.clone(),
            pool,
            bot,
        );

        let issuer_request_id = IssuerRequestId::new("iss-success-123");
        let aggregate = create_test_mint_in_journal_confirmed_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_journal_confirmed(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        assert_eq!(blockchain_service_mock.get_call_count(), 1);

        let last_call = blockchain_service_mock
            .get_last_call()
            .expect("Expected a mint call");
        assert_eq!(last_call.vault, vault, "Expected vault address to match");

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Mint::CallbackPending { .. }),
            "Expected CallbackPending state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_blockchain_failure() {
        let harness = TestHarness::new().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let TestHarness { cqrs, store, pool, .. } = harness;

        let blockchain_service_mock =
            Arc::new(MockVaultService::new_failure("Network error: timeout"));
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(
            blockchain_service,
            cqrs.clone(),
            store.clone(),
            pool,
            bot,
        );

        let issuer_request_id = IssuerRequestId::new("iss-failure-456");
        let aggregate = create_test_mint_in_journal_confirmed_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_journal_confirmed(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(MintManagerError::Blockchain(_))),
            "Expected blockchain error, got {result:?}"
        );

        assert_eq!(blockchain_service_mock.get_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        let Mint::MintingFailed { error, .. } = updated_aggregate else {
            panic!("Expected MintingFailed state, got {updated_aggregate:?}");
        };

        assert!(
            error.contains("Network error: timeout"),
            "Expected error message to contain 'Network error: timeout', got: {error}"
        );
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_wrong_state_fails() {
        let harness = TestHarness::new().await;
        let TestHarness { cqrs, store, pool, .. } = &harness;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(
            blockchain_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
            bot,
        );

        let issuer_request_id = IssuerRequestId::new("iss-wrong-state-789");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        cqrs.execute(
            &issuer_request_id.0,
            MintCommand::Initiate {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            },
        )
        .await
        .unwrap();

        let aggregate = load_aggregate(&store, &issuer_request_id).await;

        let result = manager
            .handle_journal_confirmed(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(
                result,
                Err(MintManagerError::InvalidAggregateState { .. })
            ),
            "Expected InvalidAggregateState error, got {result:?}"
        );
    }

    #[test]
    fn test_quantity_to_u256_conversion() {
        let quantity = Quantity::new(Decimal::new(1005, 1));
        let result = quantity.to_u256_with_18_decimals().unwrap();

        let expected = alloy::primitives::U256::from_str_radix(
            "100500000000000000000",
            10,
        )
        .unwrap();

        assert_eq!(result, expected, "Expected {expected}, got {result}");
    }

    #[test]
    fn test_quantity_to_u256_conversion_with_fractional_error() {
        let quantity = Quantity::new(Decimal::new(10001, 19));
        let result = quantity.to_u256_with_18_decimals();

        assert!(
            result.is_err(),
            "Expected error for fractional value, got {result:?}"
        );
    }

    #[test]
    fn test_quantity_to_u256_conversion_overflow() {
        let quantity = Quantity::new(Decimal::MAX);
        let result = quantity.to_u256_with_18_decimals();

        assert!(result.is_err(), "Expected error for overflow, got {result:?}");
    }

    async fn setup_recovery_test_env() -> (
        sqlx::Pool<sqlx::Sqlite>,
        Arc<
            cqrs_es::CqrsFramework<
                Mint,
                PersistedEventStore<SqliteEventRepository, Mint>,
            >,
        >,
        Arc<PersistedEventStore<SqliteEventRepository, Mint>>,
        SqliteCqrs<TokenizedAsset>,
    ) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);
        let mint_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ()));

        let mint_event_repo = SqliteEventRepository::new(pool.clone());
        let mint_event_store = Arc::new(PersistedEventStore::<
            SqliteEventRepository,
            Mint,
        >::new_event_store(
            mint_event_repo
        ));

        let tokenized_asset_view_repo = Arc::new(SqliteViewRepository::<
            TokenizedAssetView,
            TokenizedAsset,
        >::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));
        let tokenized_asset_query =
            GenericQuery::new(tokenized_asset_view_repo);
        let tokenized_asset_cqrs = sqlite_cqrs(
            pool.clone(),
            vec![Box::new(tokenized_asset_query)],
            (),
        );

        (pool, mint_cqrs, mint_event_store, tokenized_asset_cqrs)
    }

    #[tokio::test]
    async fn test_recover_journal_confirmed_mints_processes_stuck_mints() {
        let (pool, mint_cqrs, mint_event_store, asset_cqrs) =
            setup_recovery_test_env().await;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        asset_cqrs
            .execute(
                &underlying.0,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    vault,
                },
            )
            .await
            .expect("Failed to add asset");

        let client_id = ClientId::new();
        let issuer_request_id = IssuerRequestId::new("iss-recovery-test-1");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        mint_cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::Initiate {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-1",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    client_id,
                    wallet,
                },
            )
            .await
            .expect("Failed to initiate mint");

        mint_cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::ConfirmJournal {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .expect("Failed to confirm journal");

        let blockchain_mock = Arc::new(MockVaultService::new_success());
        let manager = MintManager::new(
            blockchain_mock.clone() as Arc<dyn VaultService>,
            mint_cqrs.clone(),
            mint_event_store.clone(),
            pool.clone(),
            bot,
        );

        manager.recover_journal_confirmed_mints().await;

        assert_eq!(
            blockchain_mock.get_call_count(),
            1,
            "Expected blockchain service to be called once"
        );

        let results =
            crate::mint::view::find_journal_confirmed(&pool).await.unwrap();
        assert!(
            results.is_empty(),
            "Expected no JournalConfirmed mints after recovery"
        );
    }

    #[tokio::test]
    async fn test_recover_journal_confirmed_mints_does_nothing_when_empty() {
        let (pool, mint_cqrs, mint_event_store, _asset_cqrs) =
            setup_recovery_test_env().await;

        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let blockchain_mock = Arc::new(MockVaultService::new_success());
        let manager = MintManager::new(
            blockchain_mock.clone() as Arc<dyn VaultService>,
            mint_cqrs,
            mint_event_store,
            pool,
            bot,
        );

        manager.recover_journal_confirmed_mints().await;

        assert_eq!(
            blockchain_mock.get_call_count(),
            0,
            "Expected no blockchain calls when no stuck mints"
        );
    }

    #[tokio::test]
    async fn test_recover_journal_confirmed_continues_on_individual_failure() {
        let (pool, mint_cqrs, mint_event_store, asset_cqrs) =
            setup_recovery_test_env().await;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        asset_cqrs
            .execute(
                &underlying.0,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    vault,
                },
            )
            .await
            .expect("Failed to add asset");

        let client_id = ClientId::new();
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        for i in 1..=2 {
            let issuer_request_id =
                IssuerRequestId::new(format!("iss-recovery-multi-{i}"));
            let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

            mint_cqrs
                .execute(
                    &issuer_request_id.0,
                    MintCommand::Initiate {
                        issuer_request_id: issuer_request_id.clone(),
                        tokenization_request_id: TokenizationRequestId::new(
                            format!("tok-{i}"),
                        ),
                        quantity: Quantity::new(Decimal::from(100)),
                        underlying: UnderlyingSymbol::new("AAPL"),
                        token: TokenSymbol::new("tAAPL"),
                        network: Network::new("base"),
                        client_id,
                        wallet,
                    },
                )
                .await
                .unwrap();

            mint_cqrs
                .execute(
                    &issuer_request_id.0,
                    MintCommand::ConfirmJournal {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                )
                .await
                .unwrap();
        }

        let blockchain_mock =
            Arc::new(MockVaultService::new_failure("Network error"));
        let manager = MintManager::new(
            blockchain_mock.clone() as Arc<dyn VaultService>,
            mint_cqrs,
            mint_event_store,
            pool,
            bot,
        );

        manager.recover_journal_confirmed_mints().await;

        assert_eq!(
            blockchain_mock.get_call_count(),
            2,
            "Expected both mints to be attempted"
        );
    }
}
