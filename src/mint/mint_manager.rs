use std::sync::Arc;

use alloy::primitives::Address;
use chrono::Utc;
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use tracing::{info, warn};

use crate::lifecycle::{Lifecycle, Never};
use crate::tokenized_asset::UnderlyingSymbol;
use crate::vault::{
    OperationType, ReceiptInformation, VaultError, VaultService,
};

use super::{
    IssuerRequestId, Mint, MintCommand, MintError, MintState,
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
pub(crate) struct MintManager<ES: EventStore<Lifecycle<Mint, Never>>> {
    blockchain_service: Arc<dyn VaultService>,
    cqrs: Arc<CqrsFramework<Lifecycle<Mint, Never>, ES>>,
    bot: Address,
}

impl<ES: EventStore<Lifecycle<Mint, Never>>> MintManager<ES> {
    /// Creates a new mint manager.
    ///
    /// # Arguments
    ///
    /// * `blockchain_service` - Service for on-chain minting operations
    /// * `cqrs` - CQRS framework for executing commands on the Mint aggregate
    /// * `bot` - Bot's address that receives ERC1155 receipts
    pub(crate) const fn new(
        blockchain_service: Arc<dyn VaultService>,
        cqrs: Arc<CqrsFramework<Lifecycle<Mint, Never>, ES>>,
        bot: Address,
    ) -> Self {
        Self { blockchain_service, cqrs, bot }
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
        aggregate: &Lifecycle<Mint, Never>,
    ) -> Result<(), MintManagerError> {
        let inner = aggregate.live().map_err(|_| {
            MintManagerError::InvalidAggregateState {
                current_state: "Uninitialized".to_string(),
            }
        })?;

        let MintState::JournalConfirmed { .. } = &inner.state else {
            return Err(MintManagerError::InvalidAggregateState {
                current_state: inner.state_name().to_string(),
            });
        };

        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;
        let UnderlyingSymbol(underlying_str) = &inner.request.underlying;

        info!(
            issuer_request_id = %issuer_request_id_str,
            underlying = %underlying_str,
            quantity = %inner.request.quantity.0,
            wallet = %inner.request.wallet,
            "Starting on-chain minting process"
        );

        let assets = inner.request.quantity.to_u256_with_18_decimals()?;

        let receipt_info = ReceiptInformation {
            tokenization_request_id: inner
                .request
                .tokenization_request_id
                .clone(),
            issuer_request_id: issuer_request_id.clone(),
            underlying: inner.request.underlying.clone(),
            quantity: inner.request.quantity.clone(),
            operation_type: OperationType::Mint,
            timestamp: Utc::now(),
            notes: None,
        };

        match self
            .blockchain_service
            .mint_and_transfer_shares(
                assets,
                self.bot,
                inner.request.wallet,
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
        result: crate::vault::MintResult,
    ) -> Result<(), MintManagerError> {
        info!(
            issuer_request_id = %issuer_request_id.0,
            tx_hash = %result.tx_hash,
            receipt_id = %result.receipt_id,
            shares_minted = %result.shares_minted,
            gas_used = result.gas_used,
            block_number = result.block_number,
            "On-chain minting succeeded"
        );

        self.cqrs
            .execute(
                &issuer_request_id.0,
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
            issuer_request_id = %issuer_request_id.0,
            "RecordMintSuccess command executed successfully"
        );

        Ok(())
    }

    async fn record_mint_failure(
        &self,
        issuer_request_id: &IssuerRequestId,
        error: VaultError,
    ) -> Result<(), MintManagerError> {
        warn!(
            issuer_request_id = %issuer_request_id.0,
            error = %error,
            "On-chain minting failed"
        );

        self.cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::RecordMintFailure {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error.to_string(),
                },
            )
            .await?;

        info!(
            issuer_request_id = %issuer_request_id.0,
            "RecordMintFailure command executed successfully"
        );

        Err(MintManagerError::Blockchain(error))
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::address;
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use rust_decimal::Decimal;

    use crate::lifecycle::{Lifecycle, Never};
    use crate::mint::{
        ClientId, IssuerRequestId, Mint, MintCommand, MintState, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;

    use super::{MintManager, MintManagerError};

    type TestCqrs =
        CqrsFramework<Lifecycle<Mint, Never>, MemStore<Lifecycle<Mint, Never>>>;
    type TestStore = MemStore<Lifecycle<Mint, Never>>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn create_test_mint_in_journal_confirmed_state(
        cqrs: &TestCqrs,
        store: &TestStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Lifecycle<Mint, Never> {
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
        store: &TestStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Lifecycle<Mint, Never> {
        let context = store.load_aggregate(&issuer_request_id.0).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_success() {
        let (cqrs, store) = setup_test_cqrs();
        let blockchain_service_mock = Arc::new(MockVaultService::new_success());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(blockchain_service, cqrs.clone(), bot);

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

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        let inner = updated_aggregate.live().expect("Expected Live state");

        assert!(
            matches!(inner.state, MintState::CallbackPending { .. }),
            "Expected CallbackPending state, got {:?}",
            inner.state
        );
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_blockchain_failure() {
        let (cqrs, store) = setup_test_cqrs();
        let blockchain_service_mock =
            Arc::new(MockVaultService::new_failure("Network error: timeout"));
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(blockchain_service, cqrs.clone(), bot);

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

        let inner = updated_aggregate.live().expect("Expected Live state");

        let MintState::MintingFailed { error, .. } = &inner.state else {
            panic!("Expected MintingFailed state, got {:?}", inner.state);
        };

        assert!(
            error.contains("Network error: timeout"),
            "Expected error message to contain 'Network error: timeout', got: {error}"
        );
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_wrong_state_fails() {
        let (cqrs, store) = setup_test_cqrs();
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let manager = MintManager::new(blockchain_service, cqrs.clone(), bot);

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
}
