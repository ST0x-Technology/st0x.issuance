use alloy::primitives::{Address, U256};
use chrono::Utc;
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::view::{
    RedemptionView, RedemptionViewError, find_burn_failed, find_burning,
};
use super::{IssuerRequestId, Redemption, RedemptionCommand, RedemptionError};
use crate::mint::QuantityConversionError;
use crate::receipt_inventory::{
    BurnPlan, BurnTrackingError, ReceiptId, ReceiptInventory,
    ReceiptInventoryCommand, ReceiptInventoryError, ReceiptService, Shares,
};
use crate::tokenized_asset::UnderlyingSymbol;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, find_vault_by_underlying,
};
use crate::vault::{
    MultiBurnEntry, OperationType, ReceiptInformation, VaultError, VaultService,
};

/// Orchestrates the on-chain burning process in response to `AlpacaJournalCompleted` events.
///
/// The manager reacts to `AlpacaJournalCompleted` events by querying for a suitable receipt,
/// then issues a `BurnTokens` command to the Redemption aggregate. The aggregate's command
/// handler calls the vault service to perform the actual burn operation.
///
/// On burn failure, the manager issues a `RecordBurnFailure` command to record the error.
pub(crate) struct BurnManager<RedemptionStore, ReceiptInventoryStore>
where
    RedemptionStore: EventStore<Redemption>,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    /// Used only for balance queries during recovery (not for burns - those go through aggregate)
    vault_service: Arc<dyn VaultService>,
    view_pool: Pool<Sqlite>,
    cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
    store: Arc<RedemptionStore>,
    receipt_service: Arc<dyn ReceiptService>,
    receipt_inventory_cqrs:
        Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
    bot_wallet: Address,
}

impl<RedemptionStore, ReceiptInventoryStore>
    BurnManager<RedemptionStore, ReceiptInventoryStore>
where
    RedemptionStore: EventStore<Redemption>,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    /// Creates a new burn manager.
    ///
    /// # Arguments
    ///
    /// * `vault_service` - Vault service for balance queries during recovery
    /// * `view_pool` - Database pool for querying views
    /// * `cqrs` - CQRS framework for executing commands on the Redemption aggregate
    /// * `store` - Event store for loading aggregate state during recovery
    /// * `receipt_service` - Service for finding receipts to burn
    /// * `receipt_inventory_cqrs` - CQRS framework for updating ReceiptInventory after burns
    /// * `bot_wallet` - Bot's wallet address that owns both shares and receipts
    pub(crate) fn new(
        vault_service: Arc<dyn VaultService>,
        view_pool: Pool<Sqlite>,
        cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
        store: Arc<RedemptionStore>,
        receipt_service: Arc<dyn ReceiptService>,
        receipt_inventory_cqrs: Arc<
            CqrsFramework<ReceiptInventory, ReceiptInventoryStore>,
        >,
        bot_wallet: Address,
    ) -> Self {
        Self {
            vault_service,
            view_pool,
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            bot_wallet,
        }
    }

    /// Recovers redemptions stuck in the `Burning` state at startup.
    ///
    /// Queries the view for all redemptions in `Burning` state and resumes
    /// the burn process for each. This handles cases where the bot crashed
    /// after Alpaca journal completion but before burn was executed.
    pub(crate) async fn recover_burning_redemptions(&self) {
        debug!("Starting recovery of Burning redemptions");

        let stuck_redemptions = match find_burning(&self.view_pool).await {
            Ok(redemptions) => redemptions,
            Err(err) => {
                error!(error = %err, "Failed to query for stuck Burning redemptions");
                return;
            }
        };

        if stuck_redemptions.is_empty() {
            debug!("No Burning redemptions to recover");
            return;
        }

        info!(
            count = stuck_redemptions.len(),
            "Recovering stuck Burning redemptions"
        );

        for (issuer_request_id, _view) in stuck_redemptions {
            if let Err(err) =
                self.recover_single_burning(&issuer_request_id).await
            {
                warn!(
                    issuer_request_id = %issuer_request_id.as_str(),
                    error = %err,
                    "Failed to recover Burning redemption"
                );
            }
        }

        debug!("Completed recovery of Burning redemptions");
    }

    /// Recovers redemptions stuck in the `BurnFailed` state at startup.
    ///
    /// Queries the view for all redemptions where burn failed and retries
    /// the burn process for each using metadata preserved in the view.
    pub(crate) async fn recover_burn_failed_redemptions(&self) {
        debug!("Starting recovery of BurnFailed redemptions");

        let failed_redemptions = match find_burn_failed(&self.view_pool).await {
            Ok(redemptions) => redemptions,
            Err(err) => {
                error!(error = %err, "Failed to query for BurnFailed redemptions");
                return;
            }
        };

        if failed_redemptions.is_empty() {
            debug!("No BurnFailed redemptions to recover");
            return;
        }

        info!(
            count = failed_redemptions.len(),
            "Recovering BurnFailed redemptions"
        );

        for (issuer_request_id, view) in failed_redemptions {
            if let Err(err) =
                self.recover_single_burn_failed(&issuer_request_id, &view).await
            {
                warn!(
                    issuer_request_id = %issuer_request_id.as_str(),
                    error = %err,
                    "Failed to recover BurnFailed redemption"
                );
            }
        }

        debug!("Completed recovery of BurnFailed redemptions");
    }

    async fn recover_single_burn_failed(
        &self,
        issuer_request_id: &IssuerRequestId,
        view: &RedemptionView,
    ) -> Result<(), BurnManagerError> {
        let RedemptionView::BurnFailed {
            underlying,
            wallet,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            ..
        } = view
        else {
            debug!(
                issuer_request_id = %issuer_request_id.as_str(),
                "View not in BurnFailed state, skipping"
            );
            return Ok(());
        };

        let vault = find_vault_by_underlying(&self.view_pool, underlying)
            .await?
            .ok_or_else(|| BurnManagerError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        let burn_shares = alpaca_quantity.to_u256_with_18_decimals()?;
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

        let total_shares = burn_shares
            .checked_add(dust_shares)
            .ok_or(BurnManagerError::SharesOverflow)?;

        // Check on-chain balance before attempting burn. If the bot has insufficient
        // shares, the burn likely already succeeded on-chain but we crashed before
        // recording it (e.g., RPC timeout via VaultError::PendingTransaction).
        // Skip this redemption to avoid double-burning. Manual intervention required.
        let on_chain_balance = self
            .vault_service
            .get_share_balance(vault, self.bot_wallet)
            .await?;

        if on_chain_balance < total_shares {
            warn!(
                issuer_request_id = %issuer_request_id.as_str(),
                on_chain_balance = %on_chain_balance,
                burn_shares = %burn_shares,
                dust_shares = %dust_shares,
                total_shares = %total_shares,
                "MANUAL INTERVENTION REQUIRED: On-chain balance insufficient for BurnFailed recovery. \
                 Burn likely already succeeded but was not recorded. \
                 Skipping to avoid double-burning."
            );
            return Ok(());
        }

        let plan = self
            .plan_burn(
                issuer_request_id,
                vault,
                underlying,
                burn_shares,
                dust_shares,
            )
            .await?;

        let burns: Vec<MultiBurnEntry> = plan
            .allocations
            .into_iter()
            .map(|allocation| MultiBurnEntry {
                receipt_id: allocation.receipt.receipt_id.inner(),
                burn_shares: allocation.burn_amount.inner(),
            })
            .collect();

        let receipt_info = ReceiptInformation {
            tokenization_request_id: tokenization_request_id.clone(),
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            quantity: alpaca_quantity.clone(),
            operation_type: OperationType::Redeem,
            timestamp: Utc::now(),
            notes: Some("Retry after burn failure".to_string()),
        };

        info!(
            issuer_request_id = %issuer_request_id.as_str(),
            burn_shares = %burn_shares,
            dust_shares = %dust_shares,
            num_receipts = burns.len(),
            "Retrying burn for BurnFailed redemption"
        );

        self.cqrs
            .execute(
                issuer_request_id.as_str(),
                RedemptionCommand::RetryBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns,
                    dust_shares: plan.dust.inner(),
                    owner: self.bot_wallet,
                    receipt_info,
                    user_wallet: *wallet,
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id.as_str(),
            "Successfully retried burn"
        );

        Ok(())
    }

    async fn recover_single_burning(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<(), BurnManagerError> {
        let context =
            self.store.load_aggregate(issuer_request_id.as_str()).await?;

        let aggregate = context.aggregate();

        let Redemption::Burning {
            metadata,
            alpaca_quantity,
            dust_quantity,
            ..
        } = aggregate
        else {
            debug!(
                issuer_request_id = %issuer_request_id.as_str(),
                "Redemption no longer in Burning state, skipping"
            );
            return Ok(());
        };

        let vault =
            find_vault_by_underlying(&self.view_pool, &metadata.underlying)
                .await?
                .ok_or_else(|| BurnManagerError::AssetNotFound {
                    underlying: metadata.underlying.clone(),
                })?;

        // We need to burn alpaca_quantity and transfer dust_quantity
        let burn_shares = alpaca_quantity.to_u256_with_18_decimals()?;
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;
        let total_shares_needed = burn_shares
            .checked_add(dust_shares)
            .ok_or(BurnManagerError::SharesOverflow)?;

        // Check on-chain balance before attempting burn. If the bot has insufficient
        // shares, the burn likely already succeeded on-chain but we crashed before
        // recording it. Skip this redemption to avoid recording a false failure.
        // Manual intervention required to resolve.
        // TODO: Implement automatic recovery - see #88
        let on_chain_balance = self
            .vault_service
            .get_share_balance(vault, self.bot_wallet)
            .await?;

        if on_chain_balance < total_shares_needed {
            warn!(
                issuer_request_id = %issuer_request_id.as_str(),
                on_chain_balance = %on_chain_balance,
                burn_shares = %burn_shares,
                dust_shares = %dust_shares,
                total_shares_needed = %total_shares_needed,
                "MANUAL INTERVENTION REQUIRED: On-chain balance insufficient for burn recovery. \
                 Burn likely already succeeded but was not recorded. \
                 Skipping to avoid recording false failure."
            );
            return Ok(());
        }

        info!(
            issuer_request_id = %issuer_request_id.as_str(),
            "Recovering Burning redemption - resuming burn"
        );

        self.handle_burning_started(issuer_request_id, aggregate).await
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
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            ..
        } = aggregate
        else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(aggregate).to_string(),
            });
        };

        let Some(vault) =
            find_vault_by_underlying(&self.view_pool, &metadata.underlying)
                .await?
        else {
            let error_msg = format!(
                "No vault configured for underlying asset {}",
                metadata.underlying
            );

            warn!(
                issuer_request_id = %issuer_request_id,
                underlying = %metadata.underlying,
                "{error_msg}"
            );

            self.cqrs
                .execute(
                    issuer_request_id.as_str(),
                    RedemptionCommand::RecordBurnFailure {
                        issuer_request_id: issuer_request_id.clone(),
                        error: error_msg,
                    },
                )
                .await?;

            return Err(BurnManagerError::AssetNotFound {
                underlying: metadata.underlying.clone(),
            });
        };

        // Convert quantities to U256 for on-chain operations
        let burn_shares = alpaca_quantity.to_u256_with_18_decimals()?;
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

        info!(
            issuer_request_id = %issuer_request_id,
            underlying = %metadata.underlying,
            alpaca_quantity = %alpaca_quantity,
            dust_quantity = %dust_quantity,
            burn_shares = %burn_shares,
            dust_shares = %dust_shares,
            wallet = %metadata.wallet,
            vault = %vault,
            "Starting on-chain burning process with dust handling"
        );

        let plan = self
            .plan_burn(
                issuer_request_id,
                vault,
                &metadata.underlying,
                burn_shares,
                dust_shares,
            )
            .await?;

        let receipt_info = ReceiptInformation {
            tokenization_request_id: tokenization_request_id.clone(),
            issuer_request_id: issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            quantity: alpaca_quantity.clone(),
            operation_type: OperationType::Redeem,
            timestamp: Utc::now(),
            notes: None,
        };

        self.execute_burn_and_record_result(
            issuer_request_id,
            vault,
            plan,
            receipt_info,
        )
        .await
    }

    async fn plan_burn(
        &self,
        issuer_request_id: &IssuerRequestId,
        vault: Address,
        underlying: &UnderlyingSymbol,
        burn_shares: U256,
        dust_shares: U256,
    ) -> Result<BurnPlan, BurnManagerError> {
        let plan = self
            .receipt_service
            .for_burn(vault, Shares::new(burn_shares), Shares::new(dust_shares))
            .await;

        match plan {
            Ok(plan) => {
                info!(
                    issuer_request_id = %issuer_request_id,
                    num_receipts = plan.allocations.len(),
                    total_burn = %plan.total_burn,
                    dust = %plan.dust,
                    "Planned multi-receipt burn"
                );
                Ok(plan)
            }
            Err(BurnTrackingError::InsufficientBalance {
                required,
                available,
            }) => {
                self.handle_insufficient_balance(
                    issuer_request_id,
                    underlying,
                    required,
                    available,
                )
                .await
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn handle_insufficient_balance(
        &self,
        issuer_request_id: &IssuerRequestId,
        underlying: &UnderlyingSymbol,
        required: Shares,
        available: Shares,
    ) -> Result<BurnPlan, BurnManagerError> {
        let error_msg = format!(
            "Insufficient balance for {underlying}: required {required}, available {available}"
        );

        warn!(
            issuer_request_id = %issuer_request_id,
            %required,
            %available,
            underlying = %underlying,
            "{error_msg}"
        );

        self.cqrs
            .execute(
                issuer_request_id.as_str(),
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

        Err(BurnManagerError::InsufficientBalance { required, available })
    }

    async fn execute_burn_and_record_result(
        &self,
        issuer_request_id: &IssuerRequestId,
        vault: Address,
        plan: BurnPlan,
        receipt_info: ReceiptInformation,
    ) -> Result<(), BurnManagerError> {
        // Save burn info for inventory updates before consuming plan
        let inventory_updates: Vec<(ReceiptId, Shares)> = plan
            .allocations
            .iter()
            .map(|alloc| (alloc.receipt.receipt_id, alloc.burn_amount))
            .collect();

        let burns: Vec<MultiBurnEntry> = plan
            .allocations
            .into_iter()
            .map(|alloc| MultiBurnEntry {
                receipt_id: alloc.receipt.receipt_id.inner(),
                burn_shares: alloc.burn_amount.inner(),
            })
            .collect();

        let burn_result = self
            .cqrs
            .execute(
                issuer_request_id.as_str(),
                RedemptionCommand::BurnTokens {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns,
                    dust_shares: plan.dust.inner(),
                    owner: self.bot_wallet,
                    receipt_info,
                },
            )
            .await;

        match burn_result {
            Ok(()) => {
                info!(
                    issuer_request_id = %issuer_request_id,
                    "BurnTokens command executed successfully"
                );

                // Update ReceiptInventory to reflect burned shares
                self.update_receipt_inventory_after_burn(
                    vault,
                    &inventory_updates,
                )
                .await;

                Ok(())
            }
            Err(AggregateError::UserError(RedemptionError::BurnFailed(
                err,
            ))) => {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "On-chain burning failed"
                );

                self.cqrs
                    .execute(
                        issuer_request_id.as_str(),
                        RedemptionCommand::RecordBurnFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: err.to_string(),
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id,
                    "RecordBurnFailure command recorded"
                );

                Err(BurnManagerError::Blockchain(err))
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Updates the ReceiptInventory aggregate after a successful burn.
    ///
    /// Emits `BurnShares` commands for each receipt that was burned. Failures
    /// are logged but don't fail the overall operation since the on-chain burn
    /// already succeeded.
    async fn update_receipt_inventory_after_burn(
        &self,
        vault: Address,
        burns: &[(ReceiptId, Shares)],
    ) {
        let aggregate_id = vault.to_string();

        for (receipt_id, amount) in burns {
            if let Err(err) = self
                .receipt_inventory_cqrs
                .execute(
                    &aggregate_id,
                    ReceiptInventoryCommand::BurnShares {
                        receipt_id: *receipt_id,
                        amount: *amount,
                    },
                )
                .await
            {
                warn!(
                    vault = %vault,
                    receipt_id = %receipt_id,
                    amount = %amount,
                    error = %err,
                    "Failed to update ReceiptInventory after burn"
                );
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
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<RedemptionError>),
    #[error("Receipt inventory error: {0}")]
    ReceiptInventory(#[from] AggregateError<ReceiptInventoryError>),
    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("Insufficient balance: required {required}, available {available}")]
    InsufficientBalance { required: Shares, available: Shares },
    #[error("Receipt inventory error: {0}")]
    BurnTracking(#[from] BurnTrackingError),
    #[error("Redemption view error: {0}")]
    RedemptionView(#[from] RedemptionViewError),
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error("Asset not found for underlying: {underlying}")]
    AssetNotFound { underlying: UnderlyingSymbol },
    #[error("Arithmetic overflow when computing total shares needed")]
    SharesOverflow,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address, b256, uint};
    use cqrs_es::{
        AggregateContext, EventStore,
        persist::{GenericQuery, PersistedEventStore},
    };
    use rust_decimal::Decimal;
    use sqlite_es::{
        SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::{BurnManager, BurnManagerError, Redemption, RedemptionCommand};
    use crate::mint::{
        IssuerRequestId, Network, Quantity, TokenizationRequestId,
    };
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptService, ReceiptSource, Shares,
    };
    use crate::redemption::view::RedemptionView;
    use crate::tokenized_asset::view::TokenizedAssetView;
    use crate::tokenized_asset::{
        TokenSymbol, TokenizedAsset, TokenizedAssetCommand, UnderlyingSymbol,
    };
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    const TEST_WALLET: Address =
        address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

    type TestCqrs = SqliteCqrs<Redemption>;
    type TestStore = PersistedEventStore<SqliteEventRepository, Redemption>;

    struct TestHarness {
        cqrs: Arc<TestCqrs>,
        store: Arc<TestStore>,
        receipt_service: Arc<dyn ReceiptService>,
        receipt_inventory_cqrs: Arc<SqliteCqrs<ReceiptInventory>>,
        pool: sqlx::Pool<sqlx::Sqlite>,
        asset_cqrs: SqliteCqrs<TokenizedAsset>,
    }

    impl TestHarness {
        async fn new() -> Self {
            Self::with_vault_mock(Arc::new(MockVaultService::new_success()))
                .await
        }

        async fn with_vault_mock(vault_mock: Arc<MockVaultService>) -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let redemption_view_repo = Arc::new(SqliteViewRepository::<
                RedemptionView,
                Redemption,
            >::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));

            let redemption_query = GenericQuery::new(redemption_view_repo);

            let vault_service: Arc<dyn crate::vault::VaultService> =
                vault_mock.clone();
            let cqrs = Arc::new(sqlite_cqrs(
                pool.clone(),
                vec![Box::new(redemption_query)],
                vault_service,
            ));

            let repo = SqliteEventRepository::new(pool.clone());
            let store = Arc::new(PersistedEventStore::new_event_store(repo));

            let receipt_inventory_repo =
                SqliteEventRepository::new(pool.clone());
            let receipt_inventory_store = Arc::new(
                PersistedEventStore::new_event_store(receipt_inventory_repo),
            );

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

            let receipt_inventory_cqrs =
                Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

            let receipt_service =
                Arc::new(CqrsReceiptService::new(receipt_inventory_store));

            Self {
                cqrs,
                store,
                receipt_service,
                receipt_inventory_cqrs,
                pool,
                asset_cqrs,
            }
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

        async fn discover_receipt(
            &self,
            vault: Address,
            receipt_id: U256,
            balance: U256,
        ) {
            self.receipt_inventory_cqrs
                .execute(
                    &vault.to_string(),
                    ReceiptInventoryCommand::DiscoverReceipt {
                        receipt_id: ReceiptId::from(receipt_id),
                        balance: Shares::from(balance),
                        block_number: 1,
                        tx_hash: b256!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                        source: ReceiptSource::External,
                    },
                )
                .await
                .expect("Failed to discover receipt");
        }
    }

    async fn setup_test_environment() -> TestHarness {
        TestHarness::new().await
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
            issuer_request_id.as_str(),
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            issuer_request_id.as_str(),
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                alpaca_quantity: quantity,
                dust_quantity: Quantity::new(Decimal::ZERO),
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            issuer_request_id.as_str(),
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
        let context =
            store.load_aggregate(issuer_request_id.as_str()).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_success() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-burn-success-123");

        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_blockchain_failure() {
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-burn-failure-456");

        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
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

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("Invalid receipt"),
            "Expected error message to contain 'Invalid receipt', got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_insufficient_balance() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-insufficient-789");

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
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

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("Insufficient balance"),
            "Expected error message about insufficient balance, got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_wrong_state_fails() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

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

        let aggregate = load_aggregate(store, &issuer_request_id).await;

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

    #[tokio::test]
    async fn test_complete_redemption_with_burn() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-complete-001");

        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_partial_burn_receipt_remains_active() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying_symbol = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying_symbol, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-partial-002");

        harness
            .discover_receipt(
                vault,
                uint!(43_U256),
                uint!(200_000000000000000000_U256),
            )
            .await;

        let tokenization_request_id =
            TokenizationRequestId::new("alp-partial-burn");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let block_number = 22222;

        cqrs.execute(
            issuer_request_id.as_str(),
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            issuer_request_id.as_str(),
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                alpaca_quantity: quantity,
                dust_quantity: Quantity::new(Decimal::ZERO),
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            issuer_request_id.as_str(),
            RedemptionCommand::ConfirmAlpacaComplete {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        let aggregate = load_aggregate(store, &issuer_request_id).await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_burn_depletes_receipt() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-depletes-003");

        harness
            .discover_receipt(
                vault,
                uint!(44_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_burn_updates_receipt_inventory() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service =
            Arc::new(MockVaultService::new_success()) as Arc<dyn VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-inv-update-001");
        let receipt_id = uint!(99_U256);
        let initial_balance = uint!(200_000000000000000000_U256);

        harness.discover_receipt(vault, receipt_id, initial_balance).await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        // Verify the receipt inventory was updated to reflect the burn.
        // The redemption burns 100 shares (100_000000000000000000 in 18 decimals),
        // so 100 should remain from the initial 200.
        let inventory_store = SqliteEventRepository::new(pool.clone());
        let inventory_event_store: PersistedEventStore<
            SqliteEventRepository,
            ReceiptInventory,
        > = PersistedEventStore::new_event_store(inventory_store);

        let context = inventory_event_store
            .load_aggregate(&vault.to_string())
            .await
            .unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1, "Expected 1 receipt remaining");
        assert_eq!(
            receipts[0].available_balance.inner(),
            uint!(100_000000000000000000_U256),
            "Expected 100 shares remaining after burning 100 from 200"
        );
    }

    #[tokio::test]
    async fn test_burn_with_multiple_receipts() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-multiple-004");

        harness
            .discover_receipt(
                vault,
                uint!(45_U256),
                uint!(50_000000000000000000_U256),
            )
            .await;

        harness
            .discover_receipt(
                vault,
                uint!(46_U256),
                uint!(200_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_insufficient_balance_scenario() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-insufficient-005");

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
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

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("Insufficient balance"),
            "Expected error message about insufficient balance, got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_recover_burning_redemptions_empty() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        manager.recover_burning_redemptions().await;
    }

    #[tokio::test]
    async fn test_recover_burning_redemptions_with_valid_redemption() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-recovery-001");

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        manager.recover_burning_redemptions().await;

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state after recovery, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burning_skips_when_balance_insufficient() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return balance less than required (100 shares = 100e18)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id =
            IssuerRequestId::new("red-recovery-insufficient-balance");

        // Create a redemption in Burning state (needs 100 shares)
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        // Recovery should skip this redemption without attempting burn
        manager.recover_burning_redemptions().await;

        // No burn should have been attempted
        assert_eq!(
            blockchain_service_mock.get_multi_burn_call_count(),
            0,
            "Should not call burn when on-chain balance is insufficient"
        );

        // Redemption should stay in Burning state (not move to Failed)
        let aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(aggregate, Redemption::Burning { .. }),
            "Expected Burning state unchanged when balance insufficient, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burning_skips_non_burning_state() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;
        let blockchain_service_mock = Arc::new(MockVaultService::new_success());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRequestId::new("red-recovery-skip-001");

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

        manager.recover_burning_redemptions().await;

        assert_eq!(
            blockchain_service_mock.get_multi_burn_call_count(),
            0,
            "Should not call burn for Detected state"
        );

        let aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(aggregate, Redemption::Detected { .. }),
            "Expected Detected state unchanged, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burn_failed_redemptions() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id =
            IssuerRequestId::new("red-burn-failed-recovery");

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        // Create redemption and progress to Burning state
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        // Record burn failure to transition to Failed/BurnFailed
        cqrs.execute(
            issuer_request_id.as_str(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "Initial burn failed".to_string(),
            },
        )
        .await
        .expect("Failed to record burn failure");

        // Verify aggregate is in Failed state
        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state, got {aggregate:?}"
        );

        // Recovery should find the BurnFailed view and retry
        manager.recover_burn_failed_redemptions().await;

        // Burn should have been retried
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "Should have retried the burn"
        );

        // Aggregate should now be Completed
        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state after recovery, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burn_failed_skips_when_balance_insufficient() {
        let harness = setup_test_environment().await;
        let TestHarness {
            cqrs,
            store,
            receipt_service,
            receipt_inventory_cqrs,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return balance less than required (100 shares = 100e18)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let blockchain_service =
            blockchain_service_mock.clone() as Arc<dyn VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            receipt_inventory_cqrs.clone(),
            TEST_WALLET,
        );

        let issuer_request_id =
            IssuerRequestId::new("red-burn-failed-insufficient");

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        cqrs.execute(
            issuer_request_id.as_str(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "RPC timeout".to_string(),
            },
        )
        .await
        .expect("Failed to record burn failure");

        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state before recovery, got {aggregate:?}"
        );

        // Recovery should skip due to insufficient balance
        manager.recover_burn_failed_redemptions().await;

        // No burn should have been attempted
        assert_eq!(
            blockchain_service_mock.get_multi_burn_call_count(),
            0,
            "Should not call burn when on-chain balance is insufficient"
        );

        // Aggregate should stay in Failed state (not re-fail or change)
        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Failed { .. }),
            "Expected Failed state unchanged when balance insufficient, got {updated_aggregate:?}"
        );
    }
}
