use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use std::sync::Arc;
use tracing::{info, warn};

use super::{IssuerRequestId, Redemption, RedemptionCommand, RedemptionError};
use crate::account::ClientId;
use crate::alpaca::{AlpacaError, AlpacaService, RedeemRequest};
use crate::tokenized_asset::{Network, UnderlyingSymbol};

pub(crate) struct RedeemCallManager<ES: EventStore<Redemption>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Redemption, ES>>,
}

impl<ES: EventStore<Redemption>> RedeemCallManager<ES> {
    pub(crate) const fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        cqrs: Arc<CqrsFramework<Redemption, ES>>,
    ) -> Self {
        Self { alpaca_service, cqrs }
    }

    #[tracing::instrument(skip(self, aggregate), fields(
        issuer_request_id = %issuer_request_id.0,
        client_id = %client_id
    ))]
    pub(crate) async fn handle_redemption_detected(
        &self,
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

        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;
        let UnderlyingSymbol(underlying_str) = &metadata.underlying;

        info!(
            issuer_request_id = %issuer_request_id_str,
            underlying = %underlying_str,
            quantity = %metadata.quantity.0,
            wallet = %metadata.wallet,
            "Calling Alpaca redeem endpoint"
        );

        let request = RedeemRequest {
            issuer_request_id: issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            client_id,
            quantity: metadata.quantity.clone(),
            network,
            wallet: metadata.wallet,
            tx_hash: metadata.detected_tx_hash,
        };

        match self.alpaca_service.call_redeem_endpoint(request).await {
            Ok(response) => {
                info!(
                    issuer_request_id = %response.issuer_request_id.0,
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
                    fees = %response.fees.0,
                    "Alpaca redeem API call succeeded"
                );

                self.cqrs
                    .execute(
                        issuer_request_id_str,
                        RedemptionCommand::RecordAlpacaCall {
                            issuer_request_id: issuer_request_id.clone(),
                            tokenization_request_id: response
                                .tokenization_request_id,
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordAlpacaCall command executed successfully"
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_request_id_str,
                    error = %e,
                    "Alpaca redeem API call failed"
                );

                self.cqrs
                    .execute(
                        issuer_request_id_str,
                        RedemptionCommand::RecordAlpacaFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: e.to_string(),
                        },
                    )
                    .await?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordAlpacaFailure command executed successfully"
                );

                Err(RedeemCallManagerError::Alpaca(e))
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
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use cqrs_es::{AggregateContext, EventStore, mem_store::MemStore};
    use rust_decimal::Decimal;
    use std::sync::Arc;

    use super::{RedeemCallManager, RedeemCallManagerError};
    use crate::account::ClientId;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::{IssuerRequestId, Quantity};
    use crate::redemption::{Redemption, RedemptionCommand, UnderlyingSymbol};
    use crate::tokenized_asset::{Network, TokenSymbol};

    type TestCqrs = cqrs_es::CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs =
            Arc::new(cqrs_es::CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
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
    async fn test_handle_redemption_detected_with_success() {
        let (cqrs, store) = setup_test_cqrs();
        let alpaca_service_mock = Arc::new(MockAlpacaService::new_success());
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, cqrs.clone());

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
        let (cqrs, store) = setup_test_cqrs();
        let alpaca_service_mock =
            Arc::new(MockAlpacaService::new_failure("API timeout"));
        let alpaca_service = alpaca_service_mock.clone()
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, cqrs.clone());

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
        let (cqrs, _store) = setup_test_cqrs();
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn crate::alpaca::AlpacaService>;
        let manager = RedeemCallManager::new(alpaca_service, cqrs.clone());

        let issuer_request_id = IssuerRequestId::new("red-wrong-state-789");
        let aggregate = Redemption::Uninitialized;

        let client_id = ClientId::new();
        let network = Network::new("base");

        let result = manager
            .handle_redemption_detected(
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
}
