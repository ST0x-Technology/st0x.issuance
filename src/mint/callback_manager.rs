use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::view::{MintView, MintViewError, find_callback_pending};
use super::{IssuerRequestId, Mint, MintCommand, MintError};
use crate::account::view::find_by_client_id;
use crate::account::{AccountView, AlpacaAccountNumber, ClientId};
use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};

/// Orchestrates the Alpaca callback process in response to TokensMinted events.
///
/// The manager bridges ES/CQRS aggregates with external Alpaca services. It reacts to
/// TokensMinted events by calling the Alpaca service to send the mint callback, then records
/// the result back into the Mint aggregate via commands.
///
/// This pattern keeps aggregates pure (no side effects in command handlers) while enabling
/// integration with external systems.
pub(crate) struct CallbackManager<ES: EventStore<Mint>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Mint, ES>>,
    event_store: Arc<ES>,
    pool: Pool<Sqlite>,
}

impl<ES: EventStore<Mint>> CallbackManager<ES> {
    /// Creates a new callback manager.
    ///
    /// # Arguments
    ///
    /// * `alpaca_service` - Service for Alpaca API operations
    /// * `cqrs` - CQRS framework for executing commands on the Mint aggregate
    /// * `event_store` - Event store for loading aggregates
    /// * `pool` - Database pool for querying views
    pub(crate) fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        cqrs: Arc<CqrsFramework<Mint, ES>>,
        event_store: Arc<ES>,
        pool: Pool<Sqlite>,
    ) -> Self {
        Self { alpaca_service, cqrs, event_store, pool }
    }

    /// Handles a TokensMinted event by sending a callback to Alpaca.
    ///
    /// This method orchestrates the complete callback flow:
    /// 1. Validates the aggregate is in CallbackPending state
    /// 2. Builds MintCallbackRequest from aggregate data
    /// 3. Calls AlpacaService to send the callback (with retries for transient failures)
    /// 4. Records success (RecordCallback) via command
    ///
    /// # Arguments
    ///
    /// * `alpaca_account` - The user's Alpaca account number for API calls
    /// * `issuer_request_id` - ID of the mint request
    /// * `aggregate` - Current state of the Mint aggregate (must be CallbackPending)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if callback succeeded and RecordCallback command was executed.
    ///
    /// # Errors
    ///
    /// * `CallbackManagerError::InvalidAggregateState` - Aggregate is not in CallbackPending state
    /// * `CallbackManagerError::Alpaca` - Alpaca API call failed
    /// * `CallbackManagerError::Cqrs` - Command execution failed
    #[tracing::instrument(skip(self, aggregate), fields(
        issuer_request_id = %issuer_request_id.0
    ))]
    pub(crate) async fn handle_tokens_minted(
        &self,
        alpaca_account: &AlpacaAccountNumber,
        issuer_request_id: &IssuerRequestId,
        aggregate: &Mint,
    ) -> Result<(), CallbackManagerError> {
        let Mint::CallbackPending {
            tokenization_request_id,
            client_id,
            wallet,
            tx_hash,
            network,
            ..
        } = aggregate
        else {
            return Err(CallbackManagerError::InvalidAggregateState {
                current_state: aggregate.state_name().to_string(),
            });
        };

        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;

        info!(
            issuer_request_id = %issuer_request_id_str,
            tx_hash = %tx_hash,
            "Starting Alpaca callback process"
        );

        let callback_request = MintCallbackRequest {
            tokenization_request_id: tokenization_request_id.clone(),
            client_id: *client_id,
            wallet_address: *wallet,
            tx_hash: *tx_hash,
            network: network.clone(),
        };

        match self.alpaca_service.send_mint_callback(callback_request).await {
            Ok(()) => {
                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "Alpaca callback succeeded"
                );

                self.cqrs
                    .execute(
                        issuer_request_id_str,
                        MintCommand::RecordCallback {
                            issuer_request_id: issuer_request_id.clone(),
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordCallback command executed successfully"
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_request_id_str,
                    error = %e,
                    "Alpaca callback failed"
                );

                Err(CallbackManagerError::Alpaca(e))
            }
        }
    }

    /// Recovers mints stuck in CallbackPending state after a restart.
    ///
    /// This method queries for all mints in CallbackPending state and attempts to
    /// send callbacks by calling `handle_tokens_minted` for each. Errors during
    /// processing are logged but do not stop recovery of other mints.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn recover_callback_pending_mints(&self) {
        debug!("Starting recovery of CallbackPending mints");

        let stuck_mints = match find_callback_pending(&self.pool).await {
            Ok(mints) => mints,
            Err(e) => {
                error!(error = %e, "Failed to query for stuck CallbackPending mints");
                return;
            }
        };

        if stuck_mints.is_empty() {
            debug!("No CallbackPending mints to recover");
            return;
        }

        info!(
            count = stuck_mints.len(),
            "Recovering stuck CallbackPending mints"
        );

        for (issuer_request_id, view) in stuck_mints {
            let MintView::CallbackPending { client_id, .. } = &view else {
                error!(
                    issuer_request_id = %issuer_request_id.0,
                    "Unexpected view state during CallbackPending recovery"
                );
                continue;
            };
            let client_id = *client_id;

            let alpaca_account = match self
                .lookup_alpaca_account_for_recovery(client_id)
                .await
            {
                Ok(account) => account,
                Err(e) => {
                    error!(
                        issuer_request_id = %issuer_request_id.0,
                        client_id = %client_id,
                        error = %e,
                        "Failed to lookup Alpaca account for recovery"
                    );
                    continue;
                }
            };

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
                .handle_tokens_minted(
                    &alpaca_account,
                    &issuer_request_id,
                    &aggregate,
                )
                .await
            {
                Ok(()) => {
                    info!(
                        issuer_request_id = %issuer_request_id.0,
                        "Recovered CallbackPending mint"
                    );
                }
                Err(e) => {
                    error!(
                        issuer_request_id = %issuer_request_id.0,
                        error = %e,
                        "Failed to recover CallbackPending mint"
                    );
                }
            }
        }

        debug!("Completed recovery of CallbackPending mints");
    }

    async fn lookup_alpaca_account_for_recovery(
        &self,
        client_id: ClientId,
    ) -> Result<AlpacaAccountNumber, CallbackManagerError> {
        let account_view = find_by_client_id(&self.pool, &client_id)
            .await
            .map_err(CallbackManagerError::AccountView)?
            .ok_or(CallbackManagerError::AccountNotFound { client_id })?;

        match account_view {
            AccountView::LinkedToAlpaca { alpaca_account, .. } => {
                Ok(alpaca_account)
            }
            _ => Err(CallbackManagerError::AccountNotLinked { client_id }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CallbackManagerError {
    #[error("Alpaca error: {0}")]
    Alpaca(#[from] AlpacaError),
    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<MintError>),
    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },
    #[error("View error: {0}")]
    View(#[from] MintViewError),
    #[error("Account view error: {0}")]
    AccountView(#[from] crate::account::view::AccountViewError),
    #[error("Account not found for client_id: {client_id}")]
    AccountNotFound { client_id: ClientId },
    #[error("Account not linked to Alpaca for client_id: {client_id}")]
    AccountNotLinked { client_id: ClientId },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address, b256};
    use cqrs_es::persist::{GenericQuery, PersistedEventStore};
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use rust_decimal::Decimal;
    use sqlite_es::{SqliteEventRepository, SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::{CallbackManager, CallbackManagerError};
    use crate::account::{
        Account, AccountCommand, AccountView, AlpacaAccountNumber, Email,
    };
    use crate::alpaca::{AlpacaService, mock::MockAlpacaService};
    use crate::mint::{
        ClientId, IssuerRequestId, Mint, MintCommand, MintView, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };

    type TestCqrs = CqrsFramework<Mint, MemStore<Mint>>;
    type TestStore = MemStore<Mint>;

    fn test_alpaca_account() -> AlpacaAccountNumber {
        AlpacaAccountNumber("test-account".to_string())
    }

    async fn setup_test_cqrs()
    -> (Arc<TestCqrs>, Arc<TestStore>, sqlx::Pool<sqlx::Sqlite>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        (cqrs, store, pool)
    }

    async fn create_test_mint_in_callback_pending_state(
        cqrs: &TestCqrs,
        store: &TestStore,
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

        cqrs.execute(
            &issuer_request_id.0,
            MintCommand::StartMinting {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id =
            alloy::primitives::U256::from_str_radix("123", 10).unwrap();
        let shares_minted = alloy::primitives::U256::from_str_radix(
            "100000000000000000000",
            10,
        )
        .unwrap();

        cqrs.execute(
            &issuer_request_id.0,
            MintCommand::RecordMintSuccess {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used: 50000,
                block_number: 12345,
            },
        )
        .await
        .unwrap();

        load_aggregate(store, issuer_request_id).await
    }

    async fn load_aggregate(
        store: &TestStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Mint {
        let context = store.load_aggregate(&issuer_request_id.0).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_tokens_minted_with_success() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service =
            alpaca_service_mock.clone() as Arc<dyn AlpacaService>;
        let manager = CallbackManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        let issuer_request_id =
            IssuerRequestId::new("iss-callback-success-123");
        let aggregate = create_test_mint_in_callback_pending_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_tokens_minted(
                &test_alpaca_account(),
                &issuer_request_id,
                &aggregate,
            )
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        assert_eq!(alpaca_service_mock.get_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Mint::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_tokens_minted_with_alpaca_failure() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service_mock =
            Arc::new(MockAlpacaService::new_failure("API error: 500"));
        let alpaca_service =
            alpaca_service_mock.clone() as Arc<dyn AlpacaService>;
        let manager = CallbackManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        let issuer_request_id = IssuerRequestId::new("iss-callback-fail-456");
        let aggregate = create_test_mint_in_callback_pending_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_tokens_minted(
                &test_alpaca_account(),
                &issuer_request_id,
                &aggregate,
            )
            .await;

        assert!(
            matches!(result, Err(CallbackManagerError::Alpaca(_))),
            "Expected Alpaca error, got {result:?}"
        );

        assert_eq!(alpaca_service_mock.get_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Mint::CallbackPending { .. }),
            "Expected state to remain CallbackPending after failure, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_tokens_minted_with_wrong_state_fails() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn AlpacaService>;
        let manager = CallbackManager::new(
            alpaca_service,
            cqrs.clone(),
            store.clone(),
            pool,
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
            .handle_tokens_minted(
                &test_alpaca_account(),
                &issuer_request_id,
                &aggregate,
            )
            .await;

        assert!(
            matches!(
                result,
                Err(CallbackManagerError::InvalidAggregateState { .. })
            ),
            "Expected InvalidAggregateState error, got {result:?}"
        );
    }

    async fn setup_recovery_test_env() -> (
        sqlx::Pool<sqlx::Sqlite>,
        Arc<
            CqrsFramework<
                Mint,
                PersistedEventStore<SqliteEventRepository, Mint>,
            >,
        >,
        Arc<PersistedEventStore<SqliteEventRepository, Mint>>,
        CqrsFramework<
            Account,
            PersistedEventStore<SqliteEventRepository, Account>,
        >,
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

        let account_view_repo =
            Arc::new(SqliteViewRepository::<AccountView, Account>::new(
                pool.clone(),
                "account_view".to_string(),
            ));
        let account_query = GenericQuery::new(account_view_repo);
        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        (pool, mint_cqrs, mint_event_store, account_cqrs)
    }

    async fn create_linked_account(
        account_cqrs: &CqrsFramework<
            Account,
            PersistedEventStore<SqliteEventRepository, Account>,
        >,
        client_id: ClientId,
    ) {
        let email =
            Email::new("test@example.com".to_string()).expect("Valid email");

        account_cqrs
            .execute(
                &client_id.to_string(),
                AccountCommand::Register { client_id, email },
            )
            .await
            .expect("Failed to register account");

        account_cqrs
            .execute(
                &client_id.to_string(),
                AccountCommand::LinkToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "TEST-ALPACA-123".to_string(),
                    ),
                },
            )
            .await
            .expect("Failed to link account");
    }

    #[tokio::test]
    async fn test_recover_callback_pending_mints_processes_stuck_mints() {
        let (pool, mint_cqrs, mint_event_store, account_cqrs) =
            setup_recovery_test_env().await;

        let client_id = ClientId::new();
        create_linked_account(&account_cqrs, client_id).await;

        let issuer_request_id = IssuerRequestId::new("iss-recovery-test-1");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

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

        mint_cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::StartMinting {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .expect("Failed to start minting");

        mint_cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::RecordMintSuccess {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: b256!(
                        "0x1111111111111111111111111111111111111111111111111111111111111111"
                    ),
                    receipt_id: U256::from(1),
                    shares_minted: U256::from(100),
                    gas_used: 21000,
                    block_number: 12345,
                },
            )
            .await
            .expect("Failed to record mint success");

        let alpaca_mock = Arc::new(MockAlpacaService::new_success());
        let manager = CallbackManager::new(
            alpaca_mock.clone(),
            mint_cqrs.clone(),
            mint_event_store.clone(),
            pool.clone(),
        );

        manager.recover_callback_pending_mints().await;

        assert_eq!(
            alpaca_mock.get_call_count(),
            1,
            "Expected Alpaca callback to be called once"
        );

        let results =
            crate::mint::view::find_callback_pending(&pool).await.unwrap();
        assert!(
            results.is_empty(),
            "Expected no CallbackPending mints after recovery"
        );
    }

    #[tokio::test]
    async fn test_recover_callback_pending_mints_does_nothing_when_empty() {
        let (pool, mint_cqrs, mint_event_store, _account_cqrs) =
            setup_recovery_test_env().await;

        let alpaca_mock = Arc::new(MockAlpacaService::new_success());
        let manager = CallbackManager::new(
            alpaca_mock.clone(),
            mint_cqrs,
            mint_event_store,
            pool,
        );

        manager.recover_callback_pending_mints().await;

        assert_eq!(
            alpaca_mock.get_call_count(),
            0,
            "Expected no Alpaca calls when no stuck mints"
        );
    }

    #[tokio::test]
    async fn test_recover_callback_pending_continues_on_individual_failure() {
        let (pool, mint_cqrs, mint_event_store, account_cqrs) =
            setup_recovery_test_env().await;

        let client_id = ClientId::new();
        create_linked_account(&account_cqrs, client_id).await;

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

            mint_cqrs
                .execute(
                    &issuer_request_id.0,
                    MintCommand::StartMinting {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                )
                .await
                .unwrap();

            mint_cqrs
                .execute(
                    &issuer_request_id.0,
                    MintCommand::RecordMintSuccess {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_hash: b256!(
                            "0x2222222222222222222222222222222222222222222222222222222222222222"
                        ),
                        receipt_id: U256::from(i),
                        shares_minted: U256::from(100),
                        gas_used: 21000,
                        block_number: 12345,
                    },
                )
                .await
                .unwrap();
        }

        let alpaca_mock = Arc::new(MockAlpacaService::new_failure("API error"));
        let manager = CallbackManager::new(
            alpaca_mock.clone(),
            mint_cqrs,
            mint_event_store,
            pool,
        );

        manager.recover_callback_pending_mints().await;

        assert_eq!(
            alpaca_mock.get_call_count(),
            2,
            "Expected both mints to be attempted"
        );
    }
}
