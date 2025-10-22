use std::sync::Arc;

use chrono::Utc;
use cqrs_es::{CqrsFramework, EventStore};
use tracing::{info, warn};

use crate::blockchain::{
    BlockchainError, BlockchainService, OperationType, ReceiptInformation,
};
use crate::tokenized_asset::UnderlyingSymbol;

use super::{IssuerRequestId, Mint, MintCommand, QuantityConversionError};

/// Orchestrates the on-chain minting process in response to JournalConfirmed events.
///
/// The manager bridges ES/CQRS aggregates with external blockchain services. It reacts to
/// JournalConfirmed events by calling the blockchain service to mint tokens, then records
/// the result (success or failure) back into the Mint aggregate via commands.
///
/// This pattern keeps aggregates pure (no side effects in command handlers) while enabling
/// integration with external systems.
pub(crate) struct MintManager<ES: EventStore<Mint>> {
    blockchain_service: Arc<dyn BlockchainService>,
    cqrs: Arc<CqrsFramework<Mint, ES>>,
}

impl<ES: EventStore<Mint>> MintManager<ES> {
    /// Creates a new mint manager.
    ///
    /// # Arguments
    ///
    /// * `blockchain_service` - Service for on-chain minting operations
    /// * `cqrs` - CQRS framework for executing commands on the Mint aggregate
    pub(crate) const fn new(
        blockchain_service: Arc<dyn BlockchainService>,
        cqrs: Arc<CqrsFramework<Mint, ES>>,
    ) -> Self {
        Self { blockchain_service, cqrs }
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

        info!(
            issuer_request_id = %issuer_request_id_str,
            underlying = %underlying_str,
            quantity = %quantity.0,
            wallet = %wallet,
            "Starting on-chain minting process"
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
            .mint_tokens(assets, *wallet, receipt_info)
            .await
        {
            Ok(result) => {
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
                    .await
                    .map_err(|e| MintManagerError::Cqrs(e.to_string()))?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordMintSuccess command executed successfully"
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_request_id_str,
                    error = %e,
                    "On-chain minting failed"
                );

                self.cqrs
                    .execute(
                        issuer_request_id_str,
                        MintCommand::RecordMintFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: e.to_string(),
                        },
                    )
                    .await
                    .map_err(|err| MintManagerError::Cqrs(err.to_string()))?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordMintFailure command executed successfully"
                );

                Err(MintManagerError::Blockchain(e))
            }
        }
    }
}

const fn aggregate_state_name(aggregate: &Mint) -> &'static str {
    match aggregate {
        Mint::Uninitialized => "Uninitialized",
        Mint::Initiated { .. } => "Initiated",
        Mint::JournalConfirmed { .. } => "JournalConfirmed",
        Mint::JournalRejected { .. } => "JournalRejected",
        Mint::CallbackPending { .. } => "CallbackPending",
        Mint::MintingFailed { .. } => "MintingFailed",
        Mint::Completed { .. } => "Completed",
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintManagerError {
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),

    #[error("CQRS error: {0}")]
    Cqrs(String),

    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },

    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::address;
    use cqrs_es::{AggregateContext, EventStore, mem_store::MemStore};
    use rust_decimal::Decimal;

    use crate::{
        blockchain::mock::MockBlockchainService,
        mint::{
            ClientId, IssuerRequestId, Mint, MintCommand, Network, Quantity,
            TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
        },
    };

    use super::{MintManager, MintManagerError};

    type TestCqrs = cqrs_es::CqrsFramework<Mint, MemStore<Mint>>;
    type TestStore = MemStore<Mint>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs =
            Arc::new(cqrs_es::CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn create_test_mint_in_journal_confirmed_state(
        cqrs: &TestCqrs,
        store: &TestStore,
        issuer_request_id: &IssuerRequestId,
    ) -> Mint {
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
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
                client_id: client_id.clone(),
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
    ) -> Mint {
        let context = store.load_aggregate(&issuer_request_id.0).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_success() {
        let (cqrs, store) = setup_test_cqrs();
        let blockchain_service_mock =
            Arc::new(MockBlockchainService::new_success());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::blockchain::BlockchainService>;
        let manager = MintManager::new(blockchain_service, cqrs.clone());

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

        assert!(
            matches!(updated_aggregate, Mint::CallbackPending { .. }),
            "Expected CallbackPending state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_journal_confirmed_with_blockchain_failure() {
        let (cqrs, store) = setup_test_cqrs();
        let blockchain_service_mock = Arc::new(
            MockBlockchainService::new_failure("Network error: timeout"),
        );
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::blockchain::BlockchainService>;
        let manager = MintManager::new(blockchain_service, cqrs.clone());

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
        let (cqrs, store) = setup_test_cqrs();
        let blockchain_service = Arc::new(MockBlockchainService::new_success())
            as Arc<dyn crate::blockchain::BlockchainService>;
        let manager = MintManager::new(blockchain_service, cqrs.clone());

        let issuer_request_id = IssuerRequestId::new("iss-wrong-state-789");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
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
