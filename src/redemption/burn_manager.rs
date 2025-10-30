use alloy::primitives::U256;
use chrono::Utc;
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{info, warn};

use super::{Redemption, RedemptionCommand, RedemptionError};
use crate::mint::{IssuerRequestId, QuantityConversionError};
use crate::receipt_inventory::view::{
    ReceiptInventoryViewError, find_receipt_with_balance,
};
use crate::tokenized_asset::UnderlyingSymbol;
use crate::vault::{
    OperationType, ReceiptInformation, VaultError, VaultService,
};

/// Orchestrates the on-chain burning process in response to `AlpacaJournalCompleted` events.
///
/// The manager bridges ES/CQRS aggregates with external blockchain services. It reacts to
/// `AlpacaJournalCompleted` events by querying for a suitable receipt, calling the blockchain
/// service to burn tokens, then records the result (success or failure) back into the
/// Redemption aggregate via commands.
///
/// This pattern keeps aggregates pure (no side effects in command handlers) while enabling
/// integration with external systems.
pub(crate) struct BurnManager<ES: EventStore<Redemption>> {
    blockchain_service: Arc<dyn VaultService>,
    receipt_query_pool: Pool<Sqlite>,
    cqrs: Arc<CqrsFramework<Redemption, ES>>,
}

impl<ES: EventStore<Redemption>> BurnManager<ES> {
    /// Creates a new burn manager.
    ///
    /// # Arguments
    ///
    /// * `blockchain_service` - Service for on-chain burning operations
    /// * `receipt_query_pool` - Database pool for querying receipt inventory
    /// * `cqrs` - CQRS framework for executing commands on the Redemption aggregate
    pub(crate) fn new(
        blockchain_service: Arc<dyn VaultService>,
        receipt_query_pool: Pool<Sqlite>,
        cqrs: Arc<CqrsFramework<Redemption, ES>>,
    ) -> Self {
        Self { blockchain_service, receipt_query_pool, cqrs }
    }

    /// Handles a `Burning` state by burning tokens on-chain.
    ///
    /// This method orchestrates the complete on-chain burning flow:
    /// 1. Validates the aggregate is in `Burning` state
    /// 2. Converts quantity to U256 with 18 decimals
    /// 3. Queries for a suitable receipt with sufficient balance
    /// 4. Calls blockchain service to burn tokens
    /// 5. Records success (`RecordBurnSuccess`) or failure (`RecordBurnFailure`) via commands
    ///
    /// # Arguments
    ///
    /// * `issuer_request_id` - ID of the redemption request
    /// * `aggregate` - Current state of the Redemption aggregate (must be `Burning`)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if burning succeeded and `RecordBurnSuccess` command was executed.
    /// Returns `Err(BurnManagerError::Blockchain)` if burning failed (`RecordBurnFailure`
    /// command is still executed to record the failure).
    ///
    /// # Errors
    ///
    /// * `BurnManagerError::InvalidAggregateState` - Aggregate is not in `Burning` state
    /// * `BurnManagerError::QuantityConversion` - Quantity cannot be converted to U256
    /// * `BurnManagerError::InsufficientBalance` - No receipt with sufficient balance found
    /// * `BurnManagerError::Blockchain` - Blockchain transaction failed
    /// * `BurnManagerError::Cqrs` - Command execution failed
    /// * `BurnManagerError::Database` - Receipt query failed
    pub(crate) async fn handle_burning_started(
        &self,
        issuer_request_id: &IssuerRequestId,
        aggregate: &Redemption,
    ) -> Result<(), BurnManagerError> {
        let Redemption::Burning {
            tokenization_request_id,
            underlying,
            quantity,
            wallet,
            ..
        } = aggregate
        else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(aggregate).to_string(),
            });
        };

        info!(
            issuer_request_id = %issuer_request_id,
            underlying = %underlying,
            quantity = %quantity,
            wallet = %wallet,
            "Starting on-chain burning process"
        );

        let shares = quantity.to_u256_with_18_decimals()?;

        let receipt_id = self
            .select_receipt_for_burning(issuer_request_id, underlying, shares)
            .await?;

        let receipt_info = ReceiptInformation {
            tokenization_request_id: tokenization_request_id.clone(),
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            quantity: quantity.clone(),
            operation_type: OperationType::Redeem,
            timestamp: Utc::now(),
            notes: None,
        };

        self.execute_burn_and_record_result(
            issuer_request_id,
            shares,
            receipt_id,
            *wallet,
            receipt_info,
        )
        .await
    }

    async fn select_receipt_for_burning(
        &self,
        issuer_request_id: &IssuerRequestId,
        underlying: &UnderlyingSymbol,
        shares: U256,
    ) -> Result<U256, BurnManagerError> {
        let receipt_view = find_receipt_with_balance(
            &self.receipt_query_pool,
            underlying,
            shares,
        )
        .await?;

        let Some(receipt_view) = receipt_view else {
            return self
                .handle_no_receipt_found(issuer_request_id, underlying, shares)
                .await;
        };

        let crate::receipt_inventory::view::ReceiptInventoryView::Active {
            receipt_id,
            ..
        } = receipt_view
        else {
            return self
                .handle_receipt_not_active(
                    issuer_request_id,
                    underlying,
                    shares,
                )
                .await;
        };

        info!(
            issuer_request_id = %issuer_request_id,
            receipt_id = %receipt_id,
            "Selected receipt for burning"
        );

        Ok(receipt_id)
    }

    async fn handle_no_receipt_found(
        &self,
        issuer_request_id: &IssuerRequestId,
        underlying: &UnderlyingSymbol,
        shares: U256,
    ) -> Result<U256, BurnManagerError> {
        let error_msg = format!(
            "No receipt found with sufficient balance for {shares} shares of {underlying}"
        );

        warn!(
            issuer_request_id = %issuer_request_id,
            required_shares = %shares,
            underlying = %underlying,
            "{error_msg}"
        );

        self.cqrs
            .execute(
                &issuer_request_id.0,
                RedemptionCommand::RecordBurnFailure {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error_msg.clone(),
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id,
            "RecordBurnFailure command executed successfully"
        );

        Err(BurnManagerError::InsufficientBalance {
            required: shares,
            available: U256::ZERO,
        })
    }

    async fn handle_receipt_not_active(
        &self,
        issuer_request_id: &IssuerRequestId,
        underlying: &UnderlyingSymbol,
        shares: U256,
    ) -> Result<U256, BurnManagerError> {
        let error_msg = format!(
            "Receipt found but not in Active state for {shares} shares of {underlying}"
        );

        warn!(
            issuer_request_id = %issuer_request_id,
            "{error_msg}"
        );

        self.cqrs
            .execute(
                &issuer_request_id.0,
                RedemptionCommand::RecordBurnFailure {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error_msg.clone(),
                },
            )
            .await?;

        Err(BurnManagerError::InsufficientBalance {
            required: shares,
            available: U256::ZERO,
        })
    }

    async fn execute_burn_and_record_result(
        &self,
        issuer_request_id: &IssuerRequestId,
        shares: U256,
        receipt_id: U256,
        wallet: alloy::primitives::Address,
        receipt_info: ReceiptInformation,
    ) -> Result<(), BurnManagerError> {
        match self
            .blockchain_service
            .burn_tokens(shares, receipt_id, wallet, receipt_info)
            .await
        {
            Ok(result) => {
                info!(
                    issuer_request_id = %issuer_request_id,
                    tx_hash = %result.tx_hash,
                    receipt_id = %result.receipt_id,
                    shares_burned = %result.shares_burned,
                    gas_used = result.gas_used,
                    block_number = result.block_number,
                    "On-chain burning succeeded"
                );

                self.cqrs
                    .execute(
                        &issuer_request_id.0,
                        RedemptionCommand::RecordBurnSuccess {
                            issuer_request_id: issuer_request_id.clone(),
                            tx_hash: result.tx_hash,
                            receipt_id: result.receipt_id,
                            shares_burned: result.shares_burned,
                            gas_used: result.gas_used,
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id,
                    "RecordBurnSuccess command executed successfully"
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    error = %e,
                    "On-chain burning failed"
                );

                self.cqrs
                    .execute(
                        &issuer_request_id.0,
                        RedemptionCommand::RecordBurnFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: e.to_string(),
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id,
                    "RecordBurnFailure command executed successfully"
                );

                Err(BurnManagerError::Blockchain(e))
            }
        }
    }
}

const fn aggregate_state_name(aggregate: &Redemption) -> &'static str {
    match aggregate {
        Redemption::Uninitialized => "Uninitialized",
        Redemption::Detected { .. } => "Detected",
        Redemption::AlpacaCalled { .. } => "AlpacaCalled",
        Redemption::Burning { .. } => "Burning",
        Redemption::Failed { .. } => "Failed",
        Redemption::Completed { .. } => "Completed",
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnManagerError {
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] VaultError),

    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<RedemptionError>),

    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },

    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),

    #[error("Insufficient balance: required {required}, available {available}")]
    InsufficientBalance { required: U256, available: U256 },

    #[error("Receipt inventory error: {0}")]
    ReceiptInventory(#[from] ReceiptInventoryViewError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use chrono::Utc;
    use cqrs_es::{AggregateContext, EventStore, mem_store::MemStore};
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::{BurnManager, BurnManagerError, Redemption, RedemptionCommand};
    use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
    use crate::receipt_inventory::view::ReceiptInventoryView;
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
    use crate::vault::mock::MockVaultService;

    type TestCqrs = cqrs_es::CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs =
            Arc::new(cqrs_es::CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn setup_test_db() -> sqlx::Pool<sqlx::Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    async fn seed_receipt_with_balance(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        issuer_request_id: &str,
        receipt_id: alloy::primitives::U256,
        underlying: &str,
        token: &str,
        balance: alloy::primitives::U256,
    ) {
        let receipt_view = ReceiptInventoryView::Active {
            receipt_id,
            underlying: UnderlyingSymbol::new(underlying),
            token: TokenSymbol::new(token),
            initial_amount: balance,
            current_balance: balance,
            minted_at: Utc::now(),
        };

        let payload = serde_json::to_string(&receipt_view)
            .expect("Failed to serialize view");

        sqlx::query!(
            r"
            INSERT INTO receipt_inventory_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            issuer_request_id,
            payload
        )
        .execute(pool)
        .await
        .expect("Failed to insert receipt view");
    }

    async fn create_test_redemption_in_burning_state(
        cqrs: &TestCqrs,
        store: &TestStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Redemption {
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burn-456");
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

        cqrs.execute(
            &issuer_request_id.0,
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &issuer_request_id.0,
            RedemptionCommand::ConfirmAlpacaComplete {
                issuer_request_id: issuer_request_id.clone(),
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
    async fn test_handle_burning_started_with_success() {
        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db().await;
        let blockchain_service_mock = Arc::new(MockVaultService::new_success());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager =
            BurnManager::new(blockchain_service, pool.clone(), cqrs.clone());

        let issuer_request_id = IssuerRequestId::new("red-burn-success-123");

        seed_receipt_with_balance(
            &pool,
            "mint-for-burn",
            uint!(42_U256),
            "AAPL",
            "tAAPL",
            uint!(100_000000000000000000_U256),
        )
        .await;

        let aggregate = create_test_redemption_in_burning_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        assert_eq!(blockchain_service_mock.get_burn_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_blockchain_failure() {
        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db().await;
        let blockchain_service_mock =
            Arc::new(MockVaultService::new_failure("Network error: timeout"));
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager =
            BurnManager::new(blockchain_service, pool.clone(), cqrs.clone());

        let issuer_request_id = IssuerRequestId::new("red-burn-failure-456");

        seed_receipt_with_balance(
            &pool,
            "mint-for-fail",
            uint!(7_U256),
            "AAPL",
            "tAAPL",
            uint!(100_000000000000000000_U256),
        )
        .await;

        let aggregate = create_test_redemption_in_burning_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::Blockchain(_))),
            "Expected blockchain error, got {result:?}"
        );

        assert_eq!(blockchain_service_mock.get_burn_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("Network error: timeout"),
            "Expected error message to contain 'Network error: timeout', got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_insufficient_balance() {
        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db().await;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager =
            BurnManager::new(blockchain_service, pool.clone(), cqrs.clone());

        let issuer_request_id = IssuerRequestId::new("red-insufficient-789");

        let aggregate = create_test_redemption_in_burning_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::InsufficientBalance { .. })),
            "Expected InsufficientBalance error, got {result:?}"
        );

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("No receipt found with sufficient balance"),
            "Expected error message about no receipt, got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_wrong_state_fails() {
        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db().await;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(blockchain_service, pool, cqrs.clone());

        let issuer_request_id = IssuerRequestId::new("red-wrong-state-999");
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;

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

        let aggregate = load_aggregate(&store, &issuer_request_id).await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(
                result,
                Err(BurnManagerError::InvalidAggregateState { .. })
            ),
            "Expected InvalidAggregateState error, got {result:?}"
        );
    }
}
