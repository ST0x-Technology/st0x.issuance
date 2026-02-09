use std::sync::Arc;

use alloy::primitives::{Address, B256, U256};
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use tracing::{debug, error, info, warn};

use crate::receipt_inventory::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, ReceiptSource, Shares,
};
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, find_vault_by_underlying,
};
use crate::vault::{MintResult, ReceiptInformation, VaultError, VaultService};

use super::view::{
    MintView, MintViewError, find_incomplete_mints, find_journal_confirmed,
};
use super::{
    IssuerRequestId, Mint, MintCommand, MintError, Quantity,
    QuantityConversionError,
};

/// Orchestrates the on-chain minting process in response to JournalConfirmed events.
///
/// The manager bridges ES/CQRS aggregates with external blockchain services. It reacts to
/// JournalConfirmed events by calling the blockchain service to mint tokens, then records
/// the result (success or failure) back into the Mint aggregate via commands.
///
/// This pattern keeps aggregates pure (no side effects in command handlers) while enabling
/// integration with external systems.
pub(crate) struct MintManager<MintStore, ReceiptInventoryStore>
where
    MintStore: EventStore<Mint>,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    blockchain_service: Arc<dyn VaultService>,
    cqrs: Arc<CqrsFramework<Mint, MintStore>>,
    event_store: Arc<MintStore>,
    pool: Pool<Sqlite>,
    bot: Address,
    receipt_inventory_cqrs:
        Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
    receipt_inventory_event_store: Arc<ReceiptInventoryStore>,
}

impl<MintStore, ReceiptInventoryStore>
    MintManager<MintStore, ReceiptInventoryStore>
where
    MintStore: EventStore<Mint>,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    /// Creates a new mint manager.
    ///
    /// # Arguments
    ///
    /// * `blockchain_service` - Service for on-chain minting operations
    /// * `cqrs` - CQRS framework for executing commands on the Mint aggregate
    /// * `event_store` - Event store for loading aggregates
    /// * `pool` - Database pool for querying views
    /// * `bot` - Bot's address that receives ERC1155 receipts
    /// * `receipt_inventory_cqrs` - CQRS framework for registering receipts in ReceiptInventory
    /// * `receipt_inventory_event_store` - Event store for loading ReceiptInventory aggregates
    pub(crate) fn new(
        blockchain_service: Arc<dyn VaultService>,
        cqrs: Arc<CqrsFramework<Mint, MintStore>>,
        event_store: Arc<MintStore>,
        pool: Pool<Sqlite>,
        bot: Address,
        receipt_inventory_cqrs: Arc<
            CqrsFramework<ReceiptInventory, ReceiptInventoryStore>,
        >,
        receipt_inventory_event_store: Arc<ReceiptInventoryStore>,
    ) -> Self {
        Self {
            blockchain_service,
            cqrs,
            event_store,
            pool,
            bot,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
        }
    }

    /// Handles a JournalConfirmed event by minting tokens on-chain.
    ///
    /// This method orchestrates the complete on-chain minting flow:
    /// 1. Validates the aggregate is in JournalConfirmed state
    /// 2. Converts quantity to U256 with 18 decimals
    /// 3. Calls blockchain service to mint tokens
    /// 4. Records success (CompleteMinting) or failure (FailMinting) via commands
    ///
    /// # Arguments
    ///
    /// * `issuer_request_id` - ID of the mint request
    /// * `aggregate` - Current state of the Mint aggregate (must be JournalConfirmed)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if minting succeeded and CompleteMinting command was executed.
    /// Returns `Err(MintManagerError::Blockchain)` if minting failed (FailMinting
    /// command is still executed to record the failure).
    ///
    /// # Errors
    ///
    /// * `MintManagerError::InvalidAggregateState` - Aggregate is not in JournalConfirmed state
    /// * `MintManagerError::QuantityConversion` - Quantity cannot be converted to U256
    /// * `MintManagerError::Blockchain` - Blockchain transaction failed
    /// * `MintManagerError::Cqrs` - Command execution failed
    #[tracing::instrument(skip(self, aggregate), fields(
        issuer_request_id = %issuer_request_id.as_str()
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
            journal_confirmed_at,
            ..
        } = aggregate
        else {
            return Err(MintManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(aggregate).to_string(),
            });
        };

        let vault = find_vault_by_underlying(&self.pool, underlying)
            .await?
            .ok_or_else(|| MintManagerError::AssetNotFound {
                underlying: underlying.0.clone(),
            })?;

        info!(
            issuer_request_id = %issuer_request_id,
            underlying = %underlying,
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
                issuer_request_id.as_str(),
                MintCommand::StartMinting {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await?;

        debug!(
            issuer_request_id = %issuer_request_id,
            "StartMinting command executed, proceeding with blockchain call"
        );

        let receipt_info = ReceiptInformation::mint(
            tokenization_request_id.clone(),
            issuer_request_id.clone(),
            underlying.clone(),
            quantity.clone(),
            *journal_confirmed_at,
            None,
        );

        self.execute_mint(
            issuer_request_id,
            vault,
            quantity,
            *wallet,
            receipt_info,
        )
        .await
    }

    async fn complete_minting(
        &self,
        issuer_request_id: &IssuerRequestId,
        vault: Address,
        result: MintResult,
    ) -> Result<(), MintManagerError> {
        info!(
            issuer_request_id = %issuer_request_id,
            tx_hash = %result.tx_hash,
            receipt_id = %result.receipt_id,
            shares_minted = %result.shares_minted,
            gas_used = result.gas_used,
            block_number = result.block_number,
            "On-chain minting succeeded"
        );

        self.cqrs
            .execute(
                issuer_request_id.as_str(),
                MintCommand::CompleteMinting {
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
            issuer_request_id = %issuer_request_id,
            "CompleteMinting command executed successfully"
        );

        // Register the receipt in ReceiptInventory for burn planning
        self.register_receipt(
            vault,
            result.receipt_id,
            result.shares_minted,
            result.block_number,
            result.tx_hash,
            issuer_request_id.clone(),
        )
        .await;

        Ok(())
    }

    async fn fail_minting(
        &self,
        issuer_request_id: &IssuerRequestId,
        error: VaultError,
    ) -> Result<(), MintManagerError> {
        warn!(
            issuer_request_id = %issuer_request_id,
            error = %error,
            "On-chain minting failed"
        );

        self.cqrs
            .execute(
                issuer_request_id.as_str(),
                MintCommand::FailMinting {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error.to_string(),
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id,
            "FailMinting command executed successfully"
        );

        Err(MintManagerError::Blockchain(error))
    }

    /// Registers a newly minted receipt in the ReceiptInventory aggregate.
    ///
    /// This enables the burn planning to include this receipt when calculating
    /// which receipts to burn for redemptions. The aggregate handles idempotency,
    /// so duplicate registrations are safe.
    async fn register_receipt(
        &self,
        vault: Address,
        receipt_id: U256,
        shares: U256,
        block_number: u64,
        tx_hash: B256,
        issuer_request_id: IssuerRequestId,
    ) {
        if let Err(e) = self
            .receipt_inventory_cqrs
            .execute(
                &vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(receipt_id),
                    balance: Shares::from(shares),
                    block_number,
                    tx_hash,
                    source: ReceiptSource::Itn { issuer_request_id },
                },
            )
            .await
        {
            warn!(
                receipt_id = %receipt_id,
                vault = %vault,
                error = %e,
                "Failed to register receipt in ReceiptInventory"
            );
        } else {
            info!(
                receipt_id = %receipt_id,
                vault = %vault,
                shares = %shares,
                "Receipt registered in ReceiptInventory"
            );
        }
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
                .load_aggregate(issuer_request_id.as_str())
                .await
            {
                Ok(context) => context.aggregate().clone(),
                Err(e) => {
                    error!(
                        issuer_request_id = %issuer_request_id.as_str(),
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
                        issuer_request_id = %issuer_request_id.as_str(),
                        "Successfully recovered JournalConfirmed mint"
                    );
                }
                Err(e) => {
                    error!(
                        issuer_request_id = %issuer_request_id.as_str(),
                        error = %e,
                        "Failed to recover JournalConfirmed mint"
                    );
                }
            }
        }

        debug!("Completed recovery of JournalConfirmed mints");
    }

    /// Recovers mints stuck in Minting or MintingFailed state after a restart.
    ///
    /// This method:
    /// 1. Queries for all mints in Minting or MintingFailed state
    /// 2. For each, checks receipt inventory for a receipt with matching issuer_request_id
    /// 3. If receipt found: issues RecoverExistingMint (mint succeeded on-chain)
    /// 4. If no receipt: issues RetryMint and attempts blockchain mint
    #[tracing::instrument(skip(self))]
    pub(crate) async fn recover_incomplete_mints(&self) {
        debug!("Starting recovery of incomplete mints (Minting/MintingFailed)");

        let incomplete_mints = match find_incomplete_mints(&self.pool).await {
            Ok(mints) => mints,
            Err(e) => {
                error!(error = %e, "Failed to query for incomplete mints");
                return;
            }
        };

        if incomplete_mints.is_empty() {
            debug!("No incomplete mints to recover");
            return;
        }

        info!(count = incomplete_mints.len(), "Recovering incomplete mints");

        for (issuer_request_id, view) in incomplete_mints {
            if let Err(e) = self
                .recover_single_incomplete_mint(&issuer_request_id, &view)
                .await
            {
                error!(
                    issuer_request_id = %issuer_request_id.as_str(),
                    error = %e,
                    "Failed to recover incomplete mint"
                );
            }
        }

        debug!("Completed recovery of incomplete mints");
    }

    async fn recover_single_incomplete_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        view: &MintView,
    ) -> Result<(), MintManagerError> {
        let (MintView::Minting { underlying, .. }
        | MintView::MintingFailed { underlying, .. }) = view
        else {
            return Err(MintManagerError::InvalidViewState {
                issuer_request_id: issuer_request_id.clone(),
                current_state: view.state_name().to_string(),
            });
        };

        let vault = find_vault_by_underlying(&self.pool, underlying)
            .await?
            .ok_or_else(|| MintManagerError::AssetNotFound {
                underlying: underlying.0.clone(),
            })?;

        if let Some(receipt_id) = receipt_inventory.find_by_issuer_request_id(issuer_request_id) {
            // Mint succeeded on-chain, record it
            info!(
                issuer_request_id = %issuer_request_id.as_str(),
                receipt_id = %receipt_id.inner(),
                "Found existing receipt for incomplete mint, recording"
            );

            self.recover_existing_mint_from_receipt(
                issuer_request_id,
                vault,
                &receipt_inventory,
                receipt_id,
            )
            .await
        } else {
            // No receipt found, need to retry the mint
            info!(
                issuer_request_id = %issuer_request_id.as_str(),
                "No receipt found for incomplete mint, retrying"
            );

            self.retry_incomplete_mint(issuer_request_id, view).await
        }
    }

    async fn recover_existing_mint_from_receipt(
        &self,
        issuer_request_id: &IssuerRequestId,
        vault: Address,
        receipt_inventory: &ReceiptInventory,
        receipt_id: ReceiptId,
    ) -> Result<(), MintManagerError> {
        // Get receipt details from inventory
        let receipt = receipt_inventory
            .receipts_with_balance()
            .into_iter()
            .find(|candidate| candidate.receipt_id == receipt_id);

        let Some(receipt) = receipt else {
            return Err(MintManagerError::ReceiptInventoryInconsistent {
                issuer_request_id: issuer_request_id.clone(),
                receipt_id,
            });
        };

        self.cqrs
            .execute(
                issuer_request_id.as_str(),
                MintCommand::RecoverExistingMint {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: receipt.tx_hash,
                    receipt_id: receipt_id.inner(),
                    shares_minted: receipt.available_balance.inner(),
                    block_number: receipt.block_number,
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id.as_str(),
            "RecoverExistingMint command executed"
        );

        // Register the receipt if not already registered (idempotent)
        self.register_receipt(
            vault,
            receipt_id.inner(),
            receipt.available_balance.inner(),
            receipt.block_number,
            receipt.tx_hash,
            issuer_request_id.clone(),
        )
        .await;

        Ok(())
    }

    async fn retry_incomplete_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        view: &MintView,
    ) -> Result<(), MintManagerError> {
        let (MintView::Minting {
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        }
        | MintView::MintingFailed {
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        }) = view
        else {
            return Err(MintManagerError::InvalidViewState {
                issuer_request_id: issuer_request_id.clone(),
                current_state: view.state_name().to_string(),
            });
        };

        let vault = find_vault_by_underlying(&self.pool, underlying)
            .await?
            .ok_or_else(|| MintManagerError::AssetNotFound {
                underlying: underlying.0.clone(),
            })?;

        // Issue RetryMint - aggregate handles state validation (idempotent if already Minting)
        self.cqrs
            .execute(
                issuer_request_id.as_str(),
                MintCommand::RetryMint {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id.as_str(),
            "RetryMint command executed"
        );

        let receipt_info = ReceiptInformation::mint(
            tokenization_request_id.clone(),
            issuer_request_id.clone(),
            underlying.clone(),
            quantity.clone(),
            *journal_confirmed_at,
            Some("Recovery mint".to_string()),
        );

        self.execute_mint(
            issuer_request_id,
            vault,
            quantity,
            *wallet,
            receipt_info,
        )
        .await
    }

    async fn execute_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        vault: Address,
        quantity: &Quantity,
        wallet: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<(), MintManagerError> {
        let assets = quantity.to_u256_with_18_decimals()?;

        match self
            .blockchain_service
            .mint_and_transfer_shares(
                vault,
                assets,
                self.bot,
                wallet,
                receipt_info,
            )
            .await
        {
            Ok(result) => {
                self.complete_minting(issuer_request_id, vault, result).await
            }
            Err(e) => self.fail_minting(issuer_request_id, e).await,
        }
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
    #[error("Receipt inventory CQRS error: {0}")]
    ReceiptInventoryCqrs(#[from] AggregateError<ReceiptInventoryError>),
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

    use alloy::primitives::{Address, U256, address};
    use cqrs_es::persist::{GenericQuery, PersistedEventStore};
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use rust_decimal::Decimal;
    use sqlite_es::{
        SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
    };
    use sqlx::sqlite::SqlitePoolOptions;

    use crate::mint::{
        ClientId, IssuerRequestId, Mint, MintCommand, MintView, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::receipt_inventory::ReceiptInventory;
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, TokenizedAssetView,
    };
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    use super::{MintManager, MintManagerError};

    type TestMintCqrs = CqrsFramework<Mint, MemStore<Mint>>;
    type TestMintStore = MemStore<Mint>;
    type TestReceiptInventoryStore = MemStore<ReceiptInventory>;
    type TestReceiptInventoryCqrs =
        CqrsFramework<ReceiptInventory, TestReceiptInventoryStore>;

    struct TestHarness {
        cqrs: Arc<TestMintCqrs>,
        store: Arc<TestMintStore>,
        pool: sqlx::Pool<sqlx::Sqlite>,
        asset_cqrs: SqliteCqrs<TokenizedAsset>,
        receipt_inventory_cqrs: Arc<TestReceiptInventoryCqrs>,
        receipt_inventory_store: Arc<TestReceiptInventoryStore>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let store = Arc::new(MemStore::default());
            let cqrs =
                Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));

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

            let receipt_inventory_store: Arc<TestReceiptInventoryStore> =
                Arc::new(MemStore::default());
            let receipt_inventory_cqrs = Arc::new(CqrsFramework::new(
                (*receipt_inventory_store).clone(),
                vec![],
                (),
            ));

            Self {
                cqrs,
                store,
                pool,
                asset_cqrs,
                receipt_inventory_cqrs,
                receipt_inventory_store,
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
            issuer_request_id.as_str(),
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
            issuer_request_id.as_str(),
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
        let context =
            store.load_aggregate(issuer_request_id.as_str()).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_success() {
        let harness = TestHarness::new().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let TestHarness {
            cqrs,
            store,
            pool,
            receipt_inventory_cqrs,
            receipt_inventory_store,
            ..
        } = harness;

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
            receipt_inventory_cqrs,
            receipt_inventory_store,
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
    async fn test_handle_journal_confirmed_registers_receipt_in_inventory() {
        let harness = TestHarness::new().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let TestHarness {
            cqrs,
            store,
            pool,
            receipt_inventory_cqrs,
            receipt_inventory_store,
            ..
        } = harness;

        let blockchain_service =
            Arc::new(MockVaultService::new_success()) as Arc<dyn VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(
            blockchain_service,
            cqrs.clone(),
            store.clone(),
            pool,
            bot,
            receipt_inventory_cqrs,
            receipt_inventory_store.clone(),
        );

        let issuer_request_id = IssuerRequestId::new("iss-receipt-reg-123");
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

        // Verify receipt was registered in ReceiptInventory
        let context = receipt_inventory_store
            .load_aggregate(&vault.to_string())
            .await
            .unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1, "Expected 1 receipt to be registered");

        // MockVaultService returns receipt_id = 1
        assert_eq!(
            receipts[0].receipt_id.inner(),
            U256::from(1),
            "Expected receipt_id to match mock result"
        );
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_blockchain_failure() {
        let harness = TestHarness::new().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let TestHarness {
            cqrs,
            store,
            pool,
            receipt_inventory_cqrs,
            receipt_inventory_store,
            ..
        } = harness;

        let blockchain_service_mock = Arc::new(MockVaultService::new_failure());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(
            blockchain_service,
            cqrs.clone(),
            store.clone(),
            pool,
            bot,
            receipt_inventory_cqrs,
            receipt_inventory_store,
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
            error.contains("Invalid receipt"),
            "Expected error message to contain 'Invalid receipt', got: {error}"
        );
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_wrong_state_fails() {
        let harness = TestHarness::new().await;
        let TestHarness {
            cqrs,
            store,
            pool,
            receipt_inventory_cqrs,
            receipt_inventory_store,
            ..
        } = &harness;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(
            blockchain_service,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
            bot,
            receipt_inventory_cqrs.clone(),
            receipt_inventory_store.clone(),
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
            issuer_request_id.as_str(),
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

        let aggregate = load_aggregate(store, &issuer_request_id).await;

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

    type SqliteReceiptInventoryCqrs = CqrsFramework<
        ReceiptInventory,
        PersistedEventStore<SqliteEventRepository, ReceiptInventory>,
    >;
    type SqliteReceiptInventoryEventStore =
        Arc<PersistedEventStore<SqliteEventRepository, ReceiptInventory>>;

    async fn setup_recovery_test_env() -> (
        sqlx::Pool<sqlx::Sqlite>,
        Arc<
            CqrsFramework<
                Mint,
                PersistedEventStore<SqliteEventRepository, Mint>,
            >,
        >,
        Arc<PersistedEventStore<SqliteEventRepository, Mint>>,
        SqliteCqrs<TokenizedAsset>,
        Arc<SqliteReceiptInventoryCqrs>,
        SqliteReceiptInventoryEventStore,
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

        let receipt_inventory_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
        let receipt_inventory_event_repo =
            SqliteEventRepository::new(pool.clone());
        let receipt_inventory_event_store = Arc::new(PersistedEventStore::<
            SqliteEventRepository,
            ReceiptInventory,
        >::new_event_store(
            receipt_inventory_event_repo,
        ));

        (
            pool,
            mint_cqrs,
            mint_event_store,
            tokenized_asset_cqrs,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
        )
    }

    #[tokio::test]
    async fn test_recover_journal_confirmed_mints_processes_stuck_mints() {
        let (
            pool,
            mint_cqrs,
            mint_event_store,
            asset_cqrs,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
        ) = setup_recovery_test_env().await;

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
                issuer_request_id.as_str(),
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
                issuer_request_id.as_str(),
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
            receipt_inventory_cqrs.clone(),
            receipt_inventory_event_store,
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
        let (
            pool,
            mint_cqrs,
            mint_event_store,
            _asset_cqrs,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
        ) = setup_recovery_test_env().await;

        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let blockchain_mock = Arc::new(MockVaultService::new_success());
        let manager = MintManager::new(
            blockchain_mock.clone() as Arc<dyn VaultService>,
            mint_cqrs,
            mint_event_store,
            pool,
            bot,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
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
        let (
            pool,
            mint_cqrs,
            mint_event_store,
            asset_cqrs,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
        ) = setup_recovery_test_env().await;

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
                    issuer_request_id.as_str(),
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
                    issuer_request_id.as_str(),
                    MintCommand::ConfirmJournal {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                )
                .await
                .unwrap();
        }

        let blockchain_mock = Arc::new(MockVaultService::new_failure());
        let manager = MintManager::new(
            blockchain_mock.clone() as Arc<dyn VaultService>,
            mint_cqrs,
            mint_event_store,
            pool,
            bot,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
        );

        manager.recover_journal_confirmed_mints().await;

        assert_eq!(
            blockchain_mock.get_call_count(),
            2,
            "Expected both mints to be attempted"
        );
    }
}
