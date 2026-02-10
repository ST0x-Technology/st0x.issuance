use alloy::primitives::Address;
use cqrs_es::{Aggregate, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info, warn};

use super::{
    IssuerRedemptionRequestId, Redemption, RedemptionCommand, RedemptionError,
    RedemptionView, find_alpaca_called,
};
use crate::account::view::{AccountViewError, find_by_wallet};
use crate::account::{AccountView, AlpacaAccountNumber};
use crate::alpaca::{
    AlpacaError, AlpacaService, RedeemRequestStatus, TokenizationRequest,
};
use crate::mint::TokenizationRequestId;

pub(crate) struct JournalManager<ES: EventStore<Redemption>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Redemption, ES>>,
    store: Arc<ES>,
    pool: Pool<Sqlite>,
    max_duration: Duration,
    initial_poll_interval: Duration,
    max_interval: Duration,
}

impl<ES: EventStore<Redemption>> JournalManager<ES> {
    pub(crate) fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        cqrs: Arc<CqrsFramework<Redemption, ES>>,
        store: Arc<ES>,
        pool: Pool<Sqlite>,
    ) -> Self {
        Self {
            alpaca_service,
            cqrs,
            store,
            pool,
            max_duration: Duration::from_secs(3600),
            initial_poll_interval: Duration::from_millis(250),
            max_interval: Duration::from_secs(30),
        }
    }

    pub(crate) async fn recover_alpaca_called_redemptions(&self) {
        debug!("Starting recovery of AlpacaCalled redemptions");

        let stuck_redemptions = match find_alpaca_called(&self.pool).await {
            Ok(redemptions) => redemptions,
            Err(err) => {
                error!(error = %err, "Failed to query for stuck AlpacaCalled redemptions");
                return;
            }
        };

        if stuck_redemptions.is_empty() {
            debug!("No AlpacaCalled redemptions to recover");
            return;
        }

        info!(
            count = stuck_redemptions.len(),
            "Recovering stuck AlpacaCalled redemptions"
        );

        for (issuer_request_id, view) in stuck_redemptions {
            if let Err(err) = self
                .recover_single_alpaca_called(&issuer_request_id, &view)
                .await
            {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to recover AlpacaCalled redemption"
                );
            }
        }

        debug!("Completed recovery of AlpacaCalled redemptions");
    }

    async fn recover_single_alpaca_called(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        view: &RedemptionView,
    ) -> Result<(), JournalManagerError> {
        let RedemptionView::AlpacaCalled {
            tokenization_request_id,
            wallet,
            ..
        } = view
        else {
            debug!(
                issuer_request_id = %issuer_request_id,
                "Redemption no longer in AlpacaCalled state, skipping"
            );
            return Ok(());
        };

        let alpaca_account = self.lookup_account_for_recovery(wallet).await?;

        info!(
            issuer_request_id = %issuer_request_id,
            "Recovering AlpacaCalled redemption - resuming polling"
        );

        self.handle_alpaca_called(
            &alpaca_account,
            issuer_request_id.clone(),
            tokenization_request_id.clone(),
        )
        .await?;

        Ok(())
    }

    async fn lookup_account_for_recovery(
        &self,
        wallet: &Address,
    ) -> Result<AlpacaAccountNumber, JournalManagerError> {
        let account_view = find_by_wallet(&self.pool, wallet)
            .await?
            .ok_or(JournalManagerError::AccountNotFound { wallet: *wallet })?;

        match account_view {
            AccountView::LinkedToAlpaca { alpaca_account, .. } => {
                Ok(alpaca_account)
            }
            _ => Err(JournalManagerError::AccountNotLinked { wallet: *wallet }),
        }
    }

    #[tracing::instrument(skip(self), fields(
        issuer_request_id = %issuer_request_id,
        tokenization_request_id = %tokenization_request_id.0
    ))]
    pub(crate) async fn handle_alpaca_called(
        &self,
        alpaca_account: &AlpacaAccountNumber,
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
    ) -> Result<(), JournalManagerError> {
        info!(
            issuer_request_id = %issuer_request_id,
            tokenization_request_id = %tokenization_request_id,
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

            debug!(
                issuer_request_id = %issuer_request_id,
                tokenization_request_id = %tokenization_request_id.0,
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
        request: &TokenizationRequest,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<(), JournalManagerError> {
        let TokenizationRequest::Redeem {
            issuer_request_id: req_issuer_id,
            underlying: req_underlying,
            token: req_token,
            quantity: req_quantity,
            wallet: req_wallet,
            tx_hash: req_tx_hash,
            ..
        } = request
        else {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: "Expected Redeem entry, got Mint".to_string(),
            });
        };

        let aggregate_id = issuer_request_id.to_string();
        let events =
            self.store.load_events(&aggregate_id).await.map_err(|error| {
                JournalManagerError::ValidationFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: format!("Failed to load events: {error}"),
                }
            })?;

        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }

        let Some(metadata) = aggregate.metadata() else {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Redemption not in expected state for validation: {aggregate:?}"
                ),
            });
        };

        if *req_issuer_id != metadata.issuer_request_id {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Issuer request ID mismatch: expected {}, got {}",
                    metadata.issuer_request_id, req_issuer_id
                ),
            });
        }

        if *req_underlying != metadata.underlying {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Underlying symbol mismatch: expected {}, got {}",
                    metadata.underlying.0, req_underlying.0
                ),
            });
        }

        if *req_token != metadata.token {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Token symbol mismatch: expected {}, got {}",
                    metadata.token.0, req_token.0
                ),
            });
        }

        let Some(alpaca_quantity) = aggregate.alpaca_quantity() else {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Redemption not in AlpacaCalled state for validation: {aggregate:?}"
                ),
            });
        };

        if req_quantity != alpaca_quantity {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Quantity mismatch: expected {}, got {}",
                    alpaca_quantity.0, req_quantity.0
                ),
            });
        }

        if *req_wallet != metadata.wallet {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Wallet mismatch: expected {}, got {}",
                    metadata.wallet, req_wallet
                ),
            });
        }

        if *req_tx_hash != Some(metadata.detected_tx_hash) {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Transaction hash mismatch: expected {}, got {:?}",
                    metadata.detected_tx_hash, req_tx_hash
                ),
            });
        }

        Ok(())
    }

    async fn handle_timeout(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        elapsed: Duration,
    ) -> Result<(), JournalManagerError> {
        warn!(
            issuer_request_id = %issuer_request_id,
            elapsed = ?elapsed,
            max_duration = ?self.max_duration,
            "Polling timeout reached, marking redemption as failed"
        );

        let aggregate_id = issuer_request_id.to_string();
        self.cqrs
            .execute(
                &aggregate_id,
                RedemptionCommand::MarkFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: format!(
                        "Alpaca journal polling timeout after {:?}",
                        self.max_duration
                    ),
                },
            )
            .await?;

        Err(JournalManagerError::Timeout {
            issuer_request_id: issuer_request_id.clone(),
            elapsed,
        })
    }

    async fn handle_poll_result(
        &self,
        request_result: Result<TokenizationRequest, AlpacaError>,
        issuer_request_id: &IssuerRedemptionRequestId,
        tokenization_request_id: &TokenizationRequestId,
        elapsed: Duration,
        poll_interval: Duration,
    ) -> Result<bool, JournalManagerError> {
        match request_result {
            Ok(request) => {
                self.validate_request_fields(&request, issuer_request_id)
                    .await?;

                let status = match &request {
                    TokenizationRequest::Redeem { status, .. } => status,
                    TokenizationRequest::Mint { .. } => {
                        return Err(JournalManagerError::ValidationFailed {
                            issuer_request_id: issuer_request_id.clone(),
                            reason: "Expected Redeem entry, got Mint"
                                .to_string(),
                        });
                    }
                };

                match status {
                    RedeemRequestStatus::Completed => {
                        info!(
                            issuer_request_id = %issuer_request_id,
                            tokenization_request_id = %tokenization_request_id,
                            elapsed = ?elapsed,
                            "Alpaca journal completed, confirming completion"
                        );

                        self.cqrs
                            .execute(
                                &issuer_request_id.to_string(),
                                RedemptionCommand::ConfirmAlpacaComplete {
                                    issuer_request_id: issuer_request_id
                                        .clone(),
                                },
                            )
                            .await?;

                        info!(
                            issuer_request_id = %issuer_request_id,
                            "ConfirmAlpacaComplete command executed successfully"
                        );

                        Ok(false)
                    }
                    RedeemRequestStatus::Rejected => {
                        warn!(
                            issuer_request_id = %issuer_request_id,
                            tokenization_request_id = %tokenization_request_id,
                            "Alpaca journal rejected, marking redemption as failed"
                        );

                        self.cqrs
                            .execute(
                                &issuer_request_id.to_string(),
                                RedemptionCommand::MarkFailed {
                                    issuer_request_id: issuer_request_id
                                        .clone(),
                                    reason: "Alpaca journal rejected"
                                        .to_string(),
                                },
                            )
                            .await?;

                        Err(JournalManagerError::Rejected {
                            issuer_request_id: issuer_request_id.clone(),
                        })
                    }
                    RedeemRequestStatus::Pending => {
                        info!(
                            issuer_request_id = %issuer_request_id,
                            tokenization_request_id = %tokenization_request_id,
                            status = "pending",
                            next_poll_in = ?poll_interval,
                            "Journal still pending, will retry"
                        );
                        Ok(true)
                    }
                }
            }
            Err(err) => {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    tokenization_request_id = %tokenization_request_id,
                    error = %err,
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
    Timeout { issuer_request_id: IssuerRedemptionRequestId, elapsed: Duration },
    #[error("Alpaca journal rejected for redemption {issuer_request_id}")]
    Rejected { issuer_request_id: IssuerRedemptionRequestId },
    #[error("Validation failed for redemption {issuer_request_id}: {reason}")]
    ValidationFailed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
    },
    #[error("View error: {0}")]
    View(#[from] super::RedemptionViewError),
    #[error("Account view error: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Account not found for wallet: {wallet}")]
    AccountNotFound { wallet: Address },
    #[error("Account not linked for wallet: {wallet}")]
    AccountNotLinked { wallet: Address },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, address, b256};
    use async_trait::async_trait;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::{Aggregate, EventStore};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tracing_test::traced_test;

    use super::{JournalManager, JournalManagerError};
    use crate::account::{AccountView, AlpacaAccountNumber, ClientId, Email};
    use crate::alpaca::{
        AlpacaError, AlpacaService, RedeemRequestStatus, TokenizationRequest,
    };
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::redemption::{
        Redemption, RedemptionCommand, RedemptionView, UnderlyingSymbol,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::TokenSymbol;
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    type TestCqrs = cqrs_es::CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

    fn new_redemption_id() -> IssuerRedemptionRequestId {
        IssuerRedemptionRequestId::new(B256::random())
    }

    fn test_alpaca_account() -> AlpacaAccountNumber {
        AlpacaAccountNumber("test-account".to_string())
    }

    async fn setup_test_cqrs()
    -> (Arc<TestCqrs>, Arc<TestStore>, sqlx::Pool<sqlx::Sqlite>) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let store = Arc::new(MemStore::default());
        let vault_service: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success());
        let cqrs = Arc::new(cqrs_es::CqrsFramework::new(
            (*store).clone(),
            vec![],
            vault_service,
        ));
        (cqrs, store, pool)
    }

    async fn create_test_redemption_in_alpaca_called_state(
        cqrs: &TestCqrs,
        issuer_request_id: &IssuerRedemptionRequestId,
        tokenization_request_id: &TokenizationRequestId,
    ) {
        cqrs.execute(
            &issuer_request_id.to_string(),
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
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                alpaca_quantity: Quantity::new(Decimal::from(100)),
                dust_quantity: Quantity::new(Decimal::ZERO),
            },
        )
        .await
        .unwrap();
    }

    enum MockResponse {
        Success(RedeemRequestStatus),
        Error { status_code: u16, body: String },
    }

    struct StatefulMockAlpacaService {
        call_count: Mutex<usize>,
        responses: Vec<MockResponse>,
        issuer_request_id: IssuerRedemptionRequestId,
    }

    impl StatefulMockAlpacaService {
        fn new(
            responses: Vec<MockResponse>,
            issuer_request_id: IssuerRedemptionRequestId,
        ) -> Self {
            Self { call_count: Mutex::new(0), responses, issuer_request_id }
        }

        fn create_mock_request(
            &self,
            status: RedeemRequestStatus,
        ) -> TokenizationRequest {
            TokenizationRequest::Redeem {
                id: TokenizationRequestId::new("mock-tok"),
                issuer_request_id: self.issuer_request_id.clone(),
                status,
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                quantity: Quantity::new(Decimal::from(100)),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                tx_hash: Some(b256!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                )),
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
            _tokenization_request_id: &TokenizationRequestId,
        ) -> Result<TokenizationRequest, AlpacaError> {
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
                MockResponse::Success(status) => {
                    Ok(self.create_mock_request(status.clone()))
                }
                MockResponse::Error { status_code, body } => {
                    Err(AlpacaError::Api {
                        status_code: *status_code,
                        body: body.clone(),
                    })
                }
            }
        }
    }

    #[tokio::test]
    async fn test_poll_completes_successfully() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-poll-success-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                &test_alpaca_account(),
                issuer_request_id,
                tokenization_request_id,
            )
            .await;

        assert!(result.is_ok(), "Expected Ok, got {result:?}");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_poll_pending_then_completed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-pending-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Completed),
            ],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                &test_alpaca_account(),
                issuer_request_id,
                tokenization_request_id,
            )
            .await;

        assert!(result.is_ok(), "Expected Ok, got {result:?}");

        assert!(
            logs_contain_at(
                tracing::Level::DEBUG,
                &[
                    "test_poll_pending_then_completed",
                    "Polling Alpaca for journal status",
                ]
            ),
            "Polling log should be at DEBUG level, not INFO"
        );
        assert!(
            !logs_contain_at(
                tracing::Level::INFO,
                &[
                    "test_poll_pending_then_completed",
                    "Polling Alpaca for journal status",
                ]
            ),
            "Polling log must not appear at INFO level (loop body noise)"
        );
    }

    #[tokio::test]
    async fn test_poll_rejected_marks_as_failed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-rejected-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Rejected),
            ],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                &test_alpaca_account(),
                issuer_request_id.clone(),
                tokenization_request_id,
            )
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::Rejected { .. })),
            "Expected Rejected error, got {result:?}"
        );

        let events =
            store.load_events(&issuer_request_id.to_string()).await.unwrap();
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
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-error-retry-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                MockResponse::Error {
                    status_code: 500,
                    body: "Network error".to_string(),
                },
                MockResponse::Error {
                    status_code: 500,
                    body: "Network error".to_string(),
                },
                MockResponse::Success(RedeemRequestStatus::Completed),
            ],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                &test_alpaca_account(),
                issuer_request_id,
                tokenization_request_id,
            )
            .await;

        assert!(result.is_ok(), "Expected Ok after retries, got {result:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_poll_timeout_marks_as_failed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-timeout-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Pending)],
            issuer_request_id.clone(),
        ));

        let mut manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool,
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
                &test_alpaca_account(),
                issuer_request_id.clone(),
                tokenization_request_id,
            )
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::Timeout { .. })),
            "Expected Timeout error, got {result:?}"
        );

        let events =
            store.load_events(&issuer_request_id.to_string()).await.unwrap();
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
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-validation-1");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            new_redemption_id(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool,
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                &test_alpaca_account(),
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
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-validation-2");

        struct QuantityMismatchMock {
            issuer_request_id: IssuerRedemptionRequestId,
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
                _tokenization_request_id: &TokenizationRequestId,
            ) -> Result<TokenizationRequest, AlpacaError> {
                Ok(TokenizationRequest::Redeem {
                    id: TokenizationRequestId::new("mock-tok"),
                    issuer_request_id: self.issuer_request_id.clone(),
                    status: RedeemRequestStatus::Completed,
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    quantity: Quantity::new(Decimal::from(999)),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                    tx_hash: Some(b256!(
                        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    )),
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
            pool,
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let result = manager
            .handle_alpaca_called(
                &test_alpaca_account(),
                issuer_request_id,
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
                reason.contains("Quantity mismatch"),
                "Expected quantity mismatch error, got: {reason}"
            );
        }
    }

    #[tokio::test]
    async fn test_exponential_backoff_intervals() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-backoff-456");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Pending),
                MockResponse::Success(RedeemRequestStatus::Completed),
            ],
            issuer_request_id.clone(),
        ));

        let mut manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool,
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
            .handle_alpaca_called(
                &test_alpaca_account(),
                issuer_request_id,
                tokenization_request_id,
            )
            .await;

        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Expected Ok, got {result:?}");
        assert!(
            elapsed >= std::time::Duration::from_millis(10 + 20 + 40 + 80),
            "Expected exponential backoff delays, got {elapsed:?}"
        );
    }

    async fn insert_account_view(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        wallet: Address,
        alpaca_account: &AlpacaAccountNumber,
    ) {
        let view = AccountView::LinkedToAlpaca {
            client_id: ClientId::new(),
            email: Email::new("test@example.com".to_string()).unwrap(),
            alpaca_account: alpaca_account.clone(),
            whitelisted_wallets: vec![wallet],
            registered_at: chrono::Utc::now(),
            linked_at: chrono::Utc::now(),
        };
        let payload = serde_json::to_string(&view).unwrap();
        let view_id = format!("{wallet:?}");
        sqlx::query!(
            r#"
            INSERT INTO account_view (view_id, version, payload)
            VALUES ($1, 1, $2)
            "#,
            view_id,
            payload
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_redemption_view(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        view_id: &str,
        view: &RedemptionView,
    ) {
        let payload = serde_json::to_string(view).unwrap();
        sqlx::query!(
            r#"
            INSERT INTO redemption_view (view_id, version, payload)
            VALUES ($1, 1, $2)
            "#,
            view_id,
            payload
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_lookup_account_for_recovery_success() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            new_redemption_id(),
        ));
        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs,
            store,
            pool.clone(),
        );

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let alpaca_account = AlpacaAccountNumber("acc-lookup-123".to_string());

        insert_account_view(&pool, wallet, &alpaca_account).await;

        let result = manager.lookup_account_for_recovery(&wallet).await;

        assert!(result.is_ok(), "Expected success, got {result:?}");
        assert_eq!(result.unwrap(), alpaca_account);
    }

    #[tokio::test]
    async fn test_lookup_account_for_recovery_not_found() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            new_redemption_id(),
        ));
        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs,
            store,
            pool,
        );

        let wallet = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let result = manager.lookup_account_for_recovery(&wallet).await;

        assert!(
            matches!(result, Err(JournalManagerError::AccountNotFound { .. })),
            "Expected AccountNotFound, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_alpaca_called_redemptions_empty() {
        let (cqrs, store, pool) = setup_test_cqrs().await;
        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            new_redemption_id(),
        ));
        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs,
            store,
            pool,
        );

        manager.recover_alpaca_called_redemptions().await;
    }

    #[tokio::test]
    async fn test_recover_alpaca_called_redemptions_with_valid_redemption() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let alpaca_account = AlpacaAccountNumber("acc-recovery".to_string());
        insert_account_view(&pool, wallet, &alpaca_account).await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id = TokenizationRequestId::new("tok-recover");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            issuer_request_id.clone(),
        ));
        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let quantity = Quantity::new(dec!(100));
        let view = RedemptionView::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet,
            quantity: quantity.clone(),
            alpaca_quantity: quantity.clone(),
            dust_quantity: Quantity::new(dec!(0)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: chrono::Utc::now(),
            called_at: chrono::Utc::now(),
        };
        insert_redemption_view(&pool, &issuer_request_id.to_string(), &view)
            .await;

        manager.recover_alpaca_called_redemptions().await;

        let events =
            store.load_events(&issuer_request_id.to_string()).await.unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::Burning { .. }),
            "Expected Burning state after recovery, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_single_alpaca_called_missing_account() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = new_redemption_id();
        let tokenization_request_id =
            TokenizationRequestId::new("tok-no-account");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            issuer_request_id.clone(),
        ));
        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store.clone(),
            pool.clone(),
        );

        create_test_redemption_in_alpaca_called_state(
            &cqrs,
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        let quantity = Quantity::new(dec!(100));
        let view = RedemptionView::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: quantity.clone(),
            alpaca_quantity: quantity.clone(),
            dust_quantity: Quantity::new(dec!(0)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: chrono::Utc::now(),
            called_at: chrono::Utc::now(),
        };

        let result = manager
            .recover_single_alpaca_called(&issuer_request_id, &view)
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::AccountNotFound { .. })),
            "Expected AccountNotFound, got {result:?}"
        );
    }
}
