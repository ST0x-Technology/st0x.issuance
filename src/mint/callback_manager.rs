use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use std::sync::Arc;
use tracing::{info, warn};

use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};
use crate::lifecycle::{Lifecycle, Never};

use super::{IssuerRequestId, Mint, MintCommand, MintError, MintState};

/// Orchestrates the Alpaca callback process in response to TokensMinted events.
///
/// The manager bridges ES/CQRS aggregates with external Alpaca services. It reacts to
/// TokensMinted events by calling the Alpaca service to send the mint callback, then records
/// the result back into the Mint aggregate via commands.
///
/// This pattern keeps aggregates pure (no side effects in command handlers) while enabling
/// integration with external systems.
pub(crate) struct CallbackManager<ES: EventStore<Lifecycle<Mint, Never>>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Lifecycle<Mint, Never>, ES>>,
}

impl<ES: EventStore<Lifecycle<Mint, Never>>> CallbackManager<ES> {
    /// Creates a new callback manager.
    ///
    /// # Arguments
    ///
    /// * `alpaca_service` - Service for Alpaca API operations
    /// * `cqrs` - CQRS framework for executing commands on the Mint aggregate
    pub(crate) const fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        cqrs: Arc<CqrsFramework<Lifecycle<Mint, Never>, ES>>,
    ) -> Self {
        Self { alpaca_service, cqrs }
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
        issuer_request_id: &IssuerRequestId,
        aggregate: &Lifecycle<Mint, Never>,
    ) -> Result<(), CallbackManagerError> {
        let inner = aggregate.live().map_err(|_| {
            CallbackManagerError::InvalidAggregateState {
                current_state: "Uninitialized".to_string(),
            }
        })?;

        let MintState::CallbackPending { tx_hash, .. } = &inner.state else {
            return Err(CallbackManagerError::InvalidAggregateState {
                current_state: inner.state_name().to_string(),
            });
        };

        info!(
            issuer_request_id = %issuer_request_id.0,
            tx_hash = %tx_hash,
            "Starting Alpaca callback process"
        );

        let callback_request = MintCallbackRequest {
            tokenization_request_id: inner
                .request
                .tokenization_request_id
                .clone(),
            client_id: inner.request.client_id,
            wallet_address: inner.request.wallet,
            tx_hash: *tx_hash,
            network: inner.request.network.clone(),
        };

        match self.alpaca_service.send_mint_callback(callback_request).await {
            Ok(()) => {
                info!(
                    issuer_request_id = %issuer_request_id.0,
                    "Alpaca callback succeeded"
                );

                self.cqrs
                    .execute(
                        &issuer_request_id.0,
                        MintCommand::RecordCallback {
                            issuer_request_id: issuer_request_id.clone(),
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id.0,
                    "RecordCallback command executed successfully"
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_request_id.0,
                    error = %e,
                    "Alpaca callback failed"
                );

                Err(CallbackManagerError::Alpaca(e))
            }
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
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use rust_decimal::Decimal;
    use std::sync::Arc;

    use super::{CallbackManager, CallbackManagerError};
    use crate::alpaca::{AlpacaService, mock::MockAlpacaService};
    use crate::lifecycle::{Lifecycle, Never};
    use crate::mint::{
        ClientId, IssuerRequestId, Mint, MintCommand, MintState, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };

    type TestCqrs =
        CqrsFramework<Lifecycle<Mint, Never>, MemStore<Lifecycle<Mint, Never>>>;
    type TestStore = MemStore<Lifecycle<Mint, Never>>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn create_test_mint_in_callback_pending_state(
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
    ) -> Lifecycle<Mint, Never> {
        let context = store.load_aggregate(&issuer_request_id.0).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_tokens_minted_with_success() {
        let (cqrs, store) = setup_test_cqrs();
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service =
            alpaca_service_mock.clone() as Arc<dyn AlpacaService>;
        let manager = CallbackManager::new(alpaca_service, cqrs.clone());

        let issuer_request_id =
            IssuerRequestId::new("iss-callback-success-123");
        let aggregate = create_test_mint_in_callback_pending_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result =
            manager.handle_tokens_minted(&issuer_request_id, &aggregate).await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        assert_eq!(alpaca_service_mock.get_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        let inner = updated_aggregate.live().expect("Expected Live state");

        assert!(
            matches!(inner.state, MintState::Completed { .. }),
            "Expected Completed state, got {:?}",
            inner.state
        );
    }

    #[tokio::test]
    async fn test_handle_tokens_minted_with_alpaca_failure() {
        let (cqrs, store) = setup_test_cqrs();
        let alpaca_service_mock =
            Arc::new(MockAlpacaService::new_failure("API error: 500"));
        let alpaca_service =
            alpaca_service_mock.clone() as Arc<dyn AlpacaService>;
        let manager = CallbackManager::new(alpaca_service, cqrs.clone());

        let issuer_request_id = IssuerRequestId::new("iss-callback-fail-456");
        let aggregate = create_test_mint_in_callback_pending_state(
            &cqrs,
            &store,
            &issuer_request_id,
        )
        .await;

        let result =
            manager.handle_tokens_minted(&issuer_request_id, &aggregate).await;

        assert!(
            matches!(result, Err(CallbackManagerError::Alpaca(_))),
            "Expected Alpaca error, got {result:?}"
        );

        assert_eq!(alpaca_service_mock.get_call_count(), 1);

        let updated_aggregate =
            load_aggregate(&store, &issuer_request_id).await;

        let inner = updated_aggregate.live().expect("Expected Live state");

        assert!(
            matches!(inner.state, MintState::CallbackPending { .. }),
            "Expected state to remain CallbackPending after failure, got {:?}",
            inner.state
        );
    }

    #[tokio::test]
    async fn test_handle_tokens_minted_with_wrong_state_fails() {
        let (cqrs, store) = setup_test_cqrs();
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn AlpacaService>;
        let manager = CallbackManager::new(alpaca_service, cqrs.clone());

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

        let result =
            manager.handle_tokens_minted(&issuer_request_id, &aggregate).await;

        assert!(
            matches!(
                result,
                Err(CallbackManagerError::InvalidAggregateState { .. })
            ),
            "Expected InvalidAggregateState error, got {result:?}"
        );
    }
}
