use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

use super::{IssuerRequestId, Redemption, RedemptionCommand, RedemptionError};
use crate::alpaca::{AlpacaError, AlpacaService, RedeemRequestStatus};
use crate::mint::TokenizationRequestId;

pub(crate) struct JournalManager<ES: EventStore<Redemption>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Redemption, ES>>,
    store: Arc<ES>,
    max_duration: Duration,
    initial_poll_interval: Duration,
    max_interval: Duration,
}

impl<ES: EventStore<Redemption>> JournalManager<ES> {
    pub(crate) fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        cqrs: Arc<CqrsFramework<Redemption, ES>>,
        store: Arc<ES>,
    ) -> Self {
        Self {
            alpaca_service,
            cqrs,
            store,
            max_duration: Duration::from_secs(3600),
            initial_poll_interval: Duration::from_millis(250),
            max_interval: Duration::from_secs(30),
        }
    }

    pub(crate) async fn handle_alpaca_called(
        &self,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
    ) -> Result<(), JournalManagerError> {
        let IssuerRequestId(issuer_id_str) = &issuer_request_id;
        let TokenizationRequestId(tok_id_str) = &tokenization_request_id;

        info!(
            issuer_request_id = %issuer_id_str,
            tokenization_request_id = %tok_id_str,
            "Starting journal polling for redemption"
        );

        let start_time = Instant::now();
        let max_duration = self.max_duration;
        let mut poll_interval = self.initial_poll_interval;
        let max_interval = self.max_interval;

        loop {
            if start_time.elapsed() >= max_duration {
                return self
                    .handle_timeout(&issuer_request_id, start_time.elapsed())
                    .await;
            }

            info!(
                issuer_request_id = %issuer_id_str,
                tokenization_request_id = %tok_id_str,
                poll_interval = ?poll_interval,
                elapsed = ?start_time.elapsed(),
                "Polling Alpaca for journal status"
            );

            let request_result = self
                .alpaca_service
                .poll_request_status(&tokenization_request_id)
                .await;

            let should_continue = self
                .handle_poll_result(
                    request_result,
                    &issuer_request_id,
                    &tokenization_request_id,
                    start_time.elapsed(),
                    poll_interval,
                )
                .await?;

            if !should_continue {
                return Ok(());
            }

            sleep(poll_interval).await;
            poll_interval = (poll_interval * 2).min(max_interval);
        }
    }

    async fn validate_request_fields(
        &self,
        request: &crate::alpaca::TokenizationRequest,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<(), JournalManagerError> {
        use cqrs_es::Aggregate;

        let events =
            self.store.load_events(&issuer_request_id.0).await.map_err(
                |e| JournalManagerError::ValidationFailed {
                    issuer_request_id: issuer_request_id.0.clone(),
                    reason: format!("Failed to load events: {e}"),
                },
            )?;

        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }

        let Some((
            expected_issuer_request_id,
            expected_underlying,
            expected_token,
            expected_quantity,
            expected_wallet,
            expected_tx_hash,
        )) = aggregate.validation_fields()
        else {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.0.clone(),
                reason: format!(
                    "Redemption not in expected state for validation: {aggregate:?}"
                ),
            });
        };

        if &request.issuer_request_id != expected_issuer_request_id {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.0.clone(),
                reason: format!(
                    "Issuer request ID mismatch: expected {}, got {}",
                    expected_issuer_request_id.0, request.issuer_request_id.0
                ),
            });
        }

        if &request.underlying != expected_underlying {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.0.clone(),
                reason: format!(
                    "Underlying symbol mismatch: expected {}, got {}",
                    expected_underlying.0, request.underlying.0
                ),
            });
        }

        if &request.token != expected_token {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.0.clone(),
                reason: format!(
                    "Token symbol mismatch: expected {}, got {}",
                    expected_token.0, request.token.0
                ),
            });
        }

        if &request.quantity != expected_quantity {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.0.clone(),
                reason: format!(
                    "Quantity mismatch: expected {}, got {}",
                    expected_quantity.0, request.quantity.0
                ),
            });
        }

        if &request.wallet != expected_wallet {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.0.clone(),
                reason: format!(
                    "Wallet mismatch: expected {}, got {}",
                    expected_wallet, request.wallet
                ),
            });
        }

        if &request.tx_hash != expected_tx_hash {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.0.clone(),
                reason: format!(
                    "Transaction hash mismatch: expected {}, got {}",
                    expected_tx_hash, request.tx_hash
                ),
            });
        }

        Ok(())
    }

    async fn handle_timeout(
        &self,
        issuer_request_id: &IssuerRequestId,
        elapsed: Duration,
    ) -> Result<(), JournalManagerError> {
        let IssuerRequestId(issuer_id_str) = issuer_request_id;

        warn!(
            issuer_request_id = %issuer_id_str,
            elapsed = ?elapsed,
            "Polling timeout reached (1 hour), marking redemption as failed"
        );

        self.cqrs
            .execute(
                issuer_id_str,
                RedemptionCommand::MarkFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: "Alpaca journal polling timeout (1 hour)"
                        .to_string(),
                },
            )
            .await?;

        Err(JournalManagerError::Timeout {
            issuer_request_id: issuer_id_str.clone(),
            elapsed,
        })
    }

    async fn handle_poll_result(
        &self,
        request_result: Result<crate::alpaca::TokenizationRequest, AlpacaError>,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
        elapsed: Duration,
        poll_interval: Duration,
    ) -> Result<bool, JournalManagerError> {
        let IssuerRequestId(issuer_id_str) = issuer_request_id;
        let TokenizationRequestId(tok_id_str) = tokenization_request_id;

        match request_result {
            Ok(request) => {
                self.validate_request_fields(&request, issuer_request_id)
                    .await?;

                match request.status {
                    RedeemRequestStatus::Completed => {
                        info!(
                            issuer_request_id = %issuer_id_str,
                            tokenization_request_id = %tok_id_str,
                            elapsed = ?elapsed,
                            "Alpaca journal completed, confirming completion"
                        );

                        self.cqrs
                            .execute(
                                issuer_id_str,
                                RedemptionCommand::ConfirmAlpacaComplete {
                                    issuer_request_id: issuer_request_id
                                        .clone(),
                                },
                            )
                            .await?;

                        info!(
                            issuer_request_id = %issuer_id_str,
                            "ConfirmAlpacaComplete command executed successfully"
                        );

                        Ok(false)
                    }
                    RedeemRequestStatus::Rejected => {
                        warn!(
                            issuer_request_id = %issuer_id_str,
                            tokenization_request_id = %tok_id_str,
                            "Alpaca journal rejected, marking redemption as failed"
                        );

                        self.cqrs
                            .execute(
                                issuer_id_str,
                                RedemptionCommand::MarkFailed {
                                    issuer_request_id: issuer_request_id
                                        .clone(),
                                    reason: "Alpaca journal rejected"
                                        .to_string(),
                                },
                            )
                            .await?;

                        Err(JournalManagerError::Rejected {
                            issuer_request_id: issuer_id_str.clone(),
                        })
                    }
                    RedeemRequestStatus::Pending => {
                        info!(
                            issuer_request_id = %issuer_id_str,
                            tokenization_request_id = %tok_id_str,
                            status = "pending",
                            next_poll_in = ?poll_interval,
                            "Journal still pending, will retry"
                        );
                        Ok(true)
                    }
                }
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_id_str,
                    tokenization_request_id = %tok_id_str,
                    error = %e,
                    next_poll_in = ?poll_interval,
                    "Polling error, will retry"
                );
                Ok(true)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum JournalManagerError {
    #[error("Alpaca error: {0}")]
    Alpaca(#[from] AlpacaError),
    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<RedemptionError>),
    #[error(
        "Polling timeout for redemption {issuer_request_id} after {elapsed:?}"
    )]
    Timeout { issuer_request_id: String, elapsed: Duration },
    #[error("Alpaca journal rejected for redemption {issuer_request_id}")]
    Rejected { issuer_request_id: String },
    #[error("Validation failed for redemption {issuer_request_id}: {reason}")]
    ValidationFailed { issuer_request_id: String, reason: String },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use async_trait::async_trait;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::{Aggregate, EventStore};
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::{JournalManager, JournalManagerError};
    use crate::alpaca::{AlpacaError, AlpacaService, RedeemRequestStatus};
    use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
    use crate::redemption::{Redemption, RedemptionCommand};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    type TestCqrs = cqrs_es::CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs =
            Arc::new(cqrs_es::CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn create_test_redemption_in_alpaca_called_state(
        cqrs: &TestCqrs,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
    ) {
        cqrs.execute(
            &issuer_request_id.0,
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(100)),
                tx_hash: b256!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ),
                block_number: 12345,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &issuer_request_id.0,
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
            },
        )
        .await
        .unwrap();
    }

    struct StatefulMockAlpacaService {
        call_count: Mutex<usize>,
        responses: Vec<Result<RedeemRequestStatus, AlpacaError>>,
        issuer_request_id: IssuerRequestId,
    }

    impl StatefulMockAlpacaService {
        fn new(
            responses: Vec<Result<RedeemRequestStatus, AlpacaError>>,
            issuer_request_id: IssuerRequestId,
        ) -> Self {
            Self { call_count: Mutex::new(0), responses, issuer_request_id }
        }

        fn create_mock_request(
            &self,
            tokenization_request_id: &TokenizationRequestId,
            status: RedeemRequestStatus,
        ) -> crate::alpaca::TokenizationRequest {
            crate::alpaca::TokenizationRequest {
                id: tokenization_request_id.clone(),
                issuer_request_id: self.issuer_request_id.clone(),
                r#type: crate::alpaca::TokenizationRequestType::Redeem,
                status,
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                quantity: Quantity::new(Decimal::from(100)),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                tx_hash: b256!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ),
            }
        }
    }

    #[async_trait]
    impl AlpacaService for StatefulMockAlpacaService {
        async fn send_mint_callback(
            &self,
            _request: crate::alpaca::MintCallbackRequest,
        ) -> Result<(), AlpacaError> {
            Ok(())
        }

        async fn call_redeem_endpoint(
            &self,
            _request: crate::alpaca::RedeemRequest,
        ) -> Result<crate::alpaca::RedeemResponse, AlpacaError> {
            unreachable!()
        }

        async fn poll_request_status(
            &self,
            tokenization_request_id: &TokenizationRequestId,
        ) -> Result<crate::alpaca::TokenizationRequest, AlpacaError> {
            let index = {
                let mut count = self.call_count.lock().unwrap();
                let index = *count;
                *count += 1;
                index
            };

            let response = if index < self.responses.len() {
                &self.responses[index]
            } else {
                self.responses.last().unwrap()
            };

            match response {
                Ok(status) => Ok(self.create_mock_request(
                    tokenization_request_id,
                    status.clone(),
                )),
                Err(e) => Err(e.clone()),
            }
        }
    }

    #[tokio::test]
    async fn test_poll_completes_successfully() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-poll-success-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-poll-success-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![Ok(RedeemRequestStatus::Completed)],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(issuer_request_id, tokenization_request_id)
            .await;

        assert!(result.is_ok(), "Expected Ok, got {result:?}");
    }

    #[tokio::test]
    async fn test_poll_pending_then_completed() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-pending-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-pending-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Completed),
            ],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(issuer_request_id, tokenization_request_id)
            .await;

        assert!(result.is_ok(), "Expected Ok, got {result:?}");
    }

    #[tokio::test]
    async fn test_poll_rejected_marks_as_failed() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-rejected-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-rejected-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Rejected),
            ],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                issuer_request_id.clone(),
                tokenization_request_id,
            )
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::Rejected { .. })),
            "Expected Rejected error, got {result:?}"
        );

        let events = store.load_events(&issuer_request_id.0).await.unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_poll_error_retries() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-error-retry-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-error-retry-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                Err(AlpacaError::Http { message: "Network error".to_string() }),
                Err(AlpacaError::Http { message: "Network error".to_string() }),
                Ok(RedeemRequestStatus::Completed),
            ],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(issuer_request_id, tokenization_request_id)
            .await;

        assert!(result.is_ok(), "Expected Ok after retries, got {result:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_poll_timeout_marks_as_failed() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-timeout-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-timeout-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![Ok(RedeemRequestStatus::Pending)],
            issuer_request_id.clone(),
        ));

        let mut manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        manager.max_duration = std::time::Duration::from_millis(100);
        manager.initial_poll_interval = std::time::Duration::from_millis(10);

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                issuer_request_id.clone(),
                tokenization_request_id,
            )
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::Timeout { .. })),
            "Expected Timeout error, got {result:?}"
        );

        let events = store.load_events(&issuer_request_id.0).await.unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_validation_fails_on_issuer_request_id_mismatch() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-validation-1");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-validation-1");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![Ok(RedeemRequestStatus::Completed)],
            IssuerRequestId::new("wrong-issuer-id"),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                issuer_request_id.clone(),
                tokenization_request_id,
            )
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::ValidationFailed { .. })),
            "Expected ValidationFailed error, got {result:?}"
        );

        if let Err(JournalManagerError::ValidationFailed { reason, .. }) =
            result
        {
            assert!(
                reason.contains("Issuer request ID mismatch"),
                "Expected issuer_request_id mismatch error, got: {reason}"
            );
        }
    }

    #[tokio::test]
    async fn test_validation_fails_on_quantity_mismatch() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-validation-2");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-validation-2");

        struct QuantityMismatchMock {
            issuer_request_id: IssuerRequestId,
        }

        #[async_trait]
        impl AlpacaService for QuantityMismatchMock {
            async fn send_mint_callback(
                &self,
                _request: crate::alpaca::MintCallbackRequest,
            ) -> Result<(), AlpacaError> {
                Ok(())
            }

            async fn call_redeem_endpoint(
                &self,
                _request: crate::alpaca::RedeemRequest,
            ) -> Result<crate::alpaca::RedeemResponse, AlpacaError>
            {
                unreachable!()
            }

            async fn poll_request_status(
                &self,
                tokenization_request_id: &TokenizationRequestId,
            ) -> Result<crate::alpaca::TokenizationRequest, AlpacaError>
            {
                Ok(crate::alpaca::TokenizationRequest {
                    id: tokenization_request_id.clone(),
                    issuer_request_id: self.issuer_request_id.clone(),
                    r#type: crate::alpaca::TokenizationRequestType::Redeem,
                    status: RedeemRequestStatus::Completed,
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    quantity: Quantity::new(Decimal::from(999)),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                    tx_hash: b256!(
                        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    ),
                })
            }
        }

        let mock = Arc::new(QuantityMismatchMock {
            issuer_request_id: issuer_request_id.clone(),
        });

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(issuer_request_id, tokenization_request_id)
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::ValidationFailed { .. })),
            "Expected ValidationFailed error, got {result:?}"
        );

        if let Err(JournalManagerError::ValidationFailed { reason, .. }) =
            result
        {
            assert!(
                reason.contains("Quantity mismatch"),
                "Expected quantity mismatch error, got: {reason}"
            );
        }
    }

    #[tokio::test]
    async fn test_exponential_backoff_intervals() {
        let (cqrs, store) = setup_test_cqrs();

        let issuer_request_id = IssuerRequestId::new("red-backoff-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-backoff-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Pending),
                Ok(RedeemRequestStatus::Completed),
            ],
            issuer_request_id.clone(),
        ));

        let mut manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
        );

        manager.initial_poll_interval = std::time::Duration::from_millis(10);
        manager.max_interval = std::time::Duration::from_millis(100);

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let start = std::time::Instant::now();

        let result = manager
            .handle_alpaca_called(issuer_request_id, tokenization_request_id)
            .await;

        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Expected Ok, got {result:?}");
        assert!(
            elapsed >= std::time::Duration::from_millis(10 + 20 + 40 + 80),
            "Expected exponential backoff delays, got {elapsed:?}"
        );
    }
}
