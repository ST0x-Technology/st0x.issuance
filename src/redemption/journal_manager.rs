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
        let stuck_redemptions = match find_alpaca_called(&self.pool).await {
            Ok(redemptions) => redemptions,
            Err(err) => {
                error!(target: "redemption", error = %err, "Failed to query for stuck AlpacaCalled redemptions");
                return;
            }
        };

        if stuck_redemptions.is_empty() {
            debug!(target: "redemption", "No AlpacaCalled redemptions to recover");
            return;
        }

        info!(target: "redemption", count = stuck_redemptions.len(),
            "Recovering stuck AlpacaCalled redemptions"
        );

        for (issuer_request_id, view) in stuck_redemptions {
            if let Err(err) = self
                .recover_single_alpaca_called(&issuer_request_id, &view)
                .await
            {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to recover AlpacaCalled redemption"
                );
            }
        }
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
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "Redemption no longer in AlpacaCalled state, skipping"
            );
            return Ok(());
        };

        let alpaca_account = self.lookup_account_for_recovery(wallet).await?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
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
            AccountView::Registered { .. } => {
                Err(JournalManagerError::AccountNotLinked { wallet: *wallet })
            }
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
        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            tokenization_request_id = %tokenization_request_id,
            "Starting journal polling for redemption"
        );

        let start_time = Instant::now();
        let max_duration = self.max_duration;
        let mut poll_interval = self.initial_poll_interval;
        let max_interval = self.max_interval;
        let mut not_found_warned = false;

        loop {
            if start_time.elapsed() >= max_duration {
                return self
                    .handle_timeout(&issuer_request_id, start_time.elapsed())
                    .await;
            }

            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                %tokenization_request_id,
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
                    &mut not_found_warned,
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
    ) -> Result<RedeemRequestStatus, JournalManagerError> {
        let TokenizationRequest::Redeem {
            issuer_request_id: req_issuer_id,
            underlying: req_underlying,
            token: req_token,
            quantity: req_quantity,
            wallet: req_wallet,
            tx_hash: req_tx_hash,
            status,
            ..
        } = request
        else {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: "Expected Redeem entry, got Mint".to_string(),
            });
        };

        let aggregate = self.load_aggregate(issuer_request_id).await?;

        let metadata = aggregate.metadata().ok_or_else(|| {
            JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "Redemption not in expected state for validation: {aggregate:?}"
                ),
            }
        })?;

        let alpaca_quantity =
            aggregate.alpaca_quantity().ok_or_else(|| {
                JournalManagerError::ValidationFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: format!(
                        "Redemption not in AlpacaCalled state for validation: {aggregate:?}"
                    ),
                }
            })?;

        Self::check_field_match(
            issuer_request_id,
            "Issuer request ID",
            &metadata.issuer_request_id,
            req_issuer_id,
        )?;
        Self::check_field_match(
            issuer_request_id,
            "Underlying symbol",
            &metadata.underlying,
            req_underlying,
        )?;
        Self::check_field_match(
            issuer_request_id,
            "Token symbol",
            &metadata.token,
            req_token,
        )?;
        Self::check_field_match(
            issuer_request_id,
            "Quantity",
            alpaca_quantity,
            req_quantity,
        )?;
        Self::check_field_match(
            issuer_request_id,
            "Wallet",
            &metadata.wallet,
            req_wallet,
        )?;
        Self::check_field_match(
            issuer_request_id,
            "Transaction hash",
            &Some(metadata.detected_tx_hash),
            req_tx_hash,
        )?;

        Ok(status.clone())
    }

    async fn load_aggregate(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<Redemption, JournalManagerError> {
        let aggregate_id = issuer_request_id.to_string();
        let events =
            self.store.load_events(&aggregate_id).await.map_err(|source| {
                JournalManagerError::StoreLoad {
                    issuer_request_id: issuer_request_id.clone(),
                    source: Box::new(source),
                }
            })?;

        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }

        Ok(aggregate)
    }

    fn check_field_match<T: PartialEq + std::fmt::Debug>(
        issuer_request_id: &IssuerRedemptionRequestId,
        field_name: &str,
        expected: &T,
        actual: &T,
    ) -> Result<(), JournalManagerError> {
        if *expected != *actual {
            return Err(JournalManagerError::ValidationFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: format!(
                    "{field_name} mismatch: expected {expected:?}, got {actual:?}"
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
        warn!(target: "redemption", issuer_request_id = %issuer_request_id,
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
        not_found_warned: &mut bool,
    ) -> Result<bool, JournalManagerError> {
        match request_result {
            Ok(request) => {
                let status = match self
                    .validate_request_fields(&request, issuer_request_id)
                    .await
                {
                    Ok(status) => status,
                    Err(err @ JournalManagerError::StoreLoad { .. }) => {
                        // Transient store error — do not terminalize the redemption.
                        // This polling session exits; the background recovery job
                        // re-picks it on its next run.
                        warn!(target: "redemption",
                            issuer_request_id = %issuer_request_id,
                            tokenization_request_id = %tokenization_request_id,
                            error = %err,
                            "Transient store error during validation, will retry"
                        );
                        return Err(err);
                    }
                    Err(err) => {
                        warn!(target: "redemption",
                            issuer_request_id = %issuer_request_id,
                            tokenization_request_id = %tokenization_request_id,
                            error = %err,
                            "Validation failed for Alpaca response, marking redemption as failed"
                        );
                        if let Err(mark_err) = self
                            .cqrs
                            .execute(
                                &issuer_request_id.to_string(),
                                RedemptionCommand::MarkFailed {
                                    issuer_request_id: issuer_request_id
                                        .clone(),
                                    reason: err.to_string(),
                                },
                            )
                            .await
                        {
                            error!(target: "redemption",
                                issuer_request_id = %issuer_request_id,
                                error = %mark_err,
                                "Failed to mark redemption as failed after validation failure"
                            );
                        }
                        return Err(err);
                    }
                };

                match status {
                    RedeemRequestStatus::Completed => {
                        info!(target: "redemption", issuer_request_id = %issuer_request_id,
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

                        info!(target: "redemption", issuer_request_id = %issuer_request_id,
                            "ConfirmAlpacaComplete command executed successfully"
                        );

                        Ok(false)
                    }
                    RedeemRequestStatus::Rejected => {
                        warn!(target: "redemption", issuer_request_id = %issuer_request_id,
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
                        info!(target: "redemption", issuer_request_id = %issuer_request_id,
                            tokenization_request_id = %tokenization_request_id,
                            status = "pending",
                            next_poll_in = ?poll_interval,
                            "Journal still pending, will retry"
                        );
                        Ok(true)
                    }
                }
            }
            Err(AlpacaError::RequestNotFound { ref id, .. }) => {
                if *not_found_warned {
                    debug!(target: "redemption",
                        issuer_request_id = %issuer_request_id,
                        tokenization_request_id = %id,
                        next_poll_in = ?poll_interval,
                        "Request not found at Alpaca keyed endpoint, will retry"
                    );
                } else {
                    warn!(target: "redemption",
                        issuer_request_id = %issuer_request_id,
                        tokenization_request_id = %id,
                        next_poll_in = ?poll_interval,
                        "Request not found at Alpaca keyed endpoint (assumed transient), will retry"
                    );
                    *not_found_warned = true;
                }
                Ok(true)
            }
            Err(err @ AlpacaError::ResponseIdMismatch { .. }) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    tokenization_request_id = %tokenization_request_id,
                    error = %err,
                    "Response id mismatch from Alpaca, marking redemption as failed (non-retryable)"
                );

                if let Err(mark_err) = self
                    .cqrs
                    .execute(
                        &issuer_request_id.to_string(),
                        RedemptionCommand::MarkFailed {
                            issuer_request_id: issuer_request_id.clone(),
                            reason: format!(
                                "Alpaca response id mismatch: {err}"
                            ),
                        },
                    )
                    .await
                {
                    error!(target: "redemption",
                        issuer_request_id = %issuer_request_id,
                        error = %mark_err,
                        "Failed to mark redemption as failed after response id mismatch"
                    );
                }

                Err(JournalManagerError::Alpaca(err))
            }
            Err(err) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
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
    #[error(
        "Failed to load events for redemption {issuer_request_id}: {source}"
    )]
    StoreLoad {
        issuer_request_id: IssuerRedemptionRequestId,
        #[source]
        source: Box<AggregateError<RedemptionError>>,
    },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use async_trait::async_trait;
    use cqrs_es::mem_store::{MemStore, MemStoreAggregateContext};
    use cqrs_es::{Aggregate, AggregateError, EventStore};
    use event_sorcery::StoreBuilder;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracing_test::traced_test;

    use super::{JournalManager, JournalManagerError};
    use crate::account::{
        Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
    };
    use crate::alpaca::{
        AlpacaError, AlpacaService, RedeemRequestStatus, TokenizationRequest,
    };
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::redemption::RedemptionError;
    use crate::redemption::{
        Redemption, RedemptionCommand, RedemptionView, UnderlyingSymbol,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::TokenSymbol;
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    type TestCqrs = cqrs_es::CqrsFramework<Redemption, MemStore<Redemption>>;
    type TestStore = MemStore<Redemption>;

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

    async fn create_test_redemption_in_alpaca_called_state<
        ES: EventStore<Redemption>,
    >(
        cqrs: &cqrs_es::CqrsFramework<Redemption, ES>,
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
        NotFound,
        ResponseIdMismatch { returned_id: String },
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

        fn call_count(&self) -> usize {
            *self.call_count.lock().unwrap()
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
                updated_at: Some(chrono::Utc::now()),
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
                MockResponse::NotFound => Err(AlpacaError::RequestNotFound {
                    id: tokenization_request_id.clone(),
                    body: String::new(),
                }),
                MockResponse::ResponseIdMismatch { returned_id } => {
                    Err(AlpacaError::ResponseIdMismatch {
                        requested: tokenization_request_id.clone(),
                        returned: TokenizationRequestId::new(returned_id),
                    })
                }
            }
        }
    }

    #[tokio::test]
    async fn test_poll_completes_successfully() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
            logs_contain_at!(
                tracing::Level::DEBUG,
                &["Polling Alpaca for journal status"]
            ),
            "Polling log should be at DEBUG level, not INFO"
        );
        assert!(
            !logs_contain_at!(
                tracing::Level::INFO,
                &["Polling Alpaca for journal status"]
            ),
            "Polling log must not appear at INFO level (loop body noise)"
        );
    }

    #[tokio::test]
    async fn test_poll_rejected_marks_as_failed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
    #[traced_test]
    async fn test_poll_timeout_marks_as_failed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &["Polling timeout reached, marking redemption as failed"]
            ),
            "Expected WARN log for polling timeout"
        );
    }

    #[tokio::test]
    async fn test_validation_fails_on_issuer_request_id_mismatch() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-validation-1");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            IssuerRedemptionRequestId::random(),
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
                    updated_at: Some(chrono::Utc::now()),
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

    /// When Alpaca returns a Mint variant (or any response that fails
    /// validation), the redemption must be terminalized as Failed so it does
    /// not remain stuck in AlpacaCalled.
    #[tokio::test]
    #[traced_test]
    async fn test_validation_failure_terminates_as_failed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-mint-response");

        struct MintVariantMock;

        #[async_trait]
        impl AlpacaService for MintVariantMock {
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
                Ok(TokenizationRequest::Mint {})
            }
        }

        let mock = Arc::new(MintVariantMock);

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

        let events =
            store.load_events(&issuer_request_id.to_string()).await.unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state after Mint variant response, got {aggregate:?}"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Validation failed for Alpaca response, marking redemption as failed"
                ]
            ),
            "Expected WARN log for validation failure termination"
        );
    }

    /// When Alpaca returns a response id that doesn't match the requested id,
    /// the redemption must be terminalized as Failed immediately (non-retryable).
    #[tokio::test]
    #[traced_test]
    async fn test_response_id_mismatch_terminates_as_failed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-tok-requested");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::ResponseIdMismatch {
                returned_id: "alp-tok-different".to_string(),
            }],
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
            matches!(
                result,
                Err(JournalManagerError::Alpaca(
                    AlpacaError::ResponseIdMismatch { .. }
                ))
            ),
            "Expected Alpaca(ResponseIdMismatch) error, got {result:?}"
        );

        let events =
            store.load_events(&issuer_request_id.to_string()).await.unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state after ResponseIdMismatch, got {aggregate:?}"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Response id mismatch from Alpaca, marking redemption as failed"
                ]
            ),
            "Expected WARN log for response id mismatch termination"
        );
    }

    #[tokio::test]
    async fn test_exponential_backoff_intervals() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let client_id = ClientId::new();

        account_store
            .send(
                &client_id,
                AccountCommand::Register {
                    client_id,
                    email: Email::new("test@example.com".to_string()).unwrap(),
                },
            )
            .await
            .unwrap();

        account_store
            .send(
                &client_id,
                AccountCommand::LinkToAlpaca {
                    alpaca_account: alpaca_account.clone(),
                },
            )
            .await
            .unwrap();

        account_store
            .send(&client_id, AccountCommand::WhitelistWallet { wallet })
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
            IssuerRedemptionRequestId::random(),
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
            IssuerRedemptionRequestId::random(),
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
            IssuerRedemptionRequestId::random(),
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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

    #[tokio::test]
    #[traced_test]
    async fn test_poll_request_not_found_then_completed() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-not-found-then-ok");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![
                MockResponse::NotFound,
                MockResponse::NotFound,
                MockResponse::Success(RedeemRequestStatus::Completed),
            ],
            issuer_request_id.clone(),
        ));
        let mock_ref = Arc::clone(&mock);

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
            result.is_ok(),
            "Expected Ok after 404s then Completed, got {result:?}"
        );
        // First NotFound logs at WARN; subsequent ones at DEBUG.
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Request not found at Alpaca keyed endpoint (assumed transient), will retry",
                    "alp-not-found-then-ok"
                ]
            ),
            "Expected WARN log for first RequestNotFound"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &[
                    "Request not found at Alpaca keyed endpoint, will retry",
                    "alp-not-found-then-ok"
                ]
            ),
            "Expected DEBUG log for subsequent RequestNotFound"
        );

        let events =
            store.load_events(&issuer_request_id.to_string()).await.unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::Burning { .. }),
            "Expected Burning state after ConfirmAlpacaComplete, got {aggregate:?}"
        );
        assert_eq!(
            mock_ref.call_count(),
            3,
            "Expected exactly 3 poll calls (2 NotFound + 1 Completed)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn test_poll_request_not_found_until_timeout() {
        let (cqrs, store, pool) = setup_test_cqrs().await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-not-found-timeout");

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::NotFound],
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
            "Expected Timeout error on perpetual 404, got {result:?}"
        );

        let events =
            store.load_events(&issuer_request_id.to_string()).await.unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state after 404 timeout, got {aggregate:?}"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Request not found at Alpaca keyed endpoint (assumed transient), will retry"
                ]
            ),
            "Expected WARN log for first RequestNotFound"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &["Polling timeout reached, marking redemption as failed"]
            ),
            "Expected WARN log for polling timeout"
        );
    }

    /// An `EventStore` wrapper that proxies all operations to an inner `MemStore`
    /// but returns an error from `load_events` whenever `fail_load` is set.
    #[derive(Clone)]
    struct FailingStore {
        inner: MemStore<Redemption>,
        fail_load: Arc<AtomicBool>,
    }

    impl FailingStore {
        fn new(
            inner: MemStore<Redemption>,
            fail_load: Arc<AtomicBool>,
        ) -> Self {
            Self { inner, fail_load }
        }
    }

    #[async_trait]
    impl EventStore<Redemption> for FailingStore {
        type AC = MemStoreAggregateContext<Redemption>;

        async fn load_events(
            &self,
            aggregate_id: &str,
        ) -> Result<
            Vec<cqrs_es::EventEnvelope<Redemption>>,
            AggregateError<RedemptionError>,
        > {
            if self.fail_load.load(Ordering::SeqCst) {
                return Err(AggregateError::DatabaseConnectionError(
                    "simulated store IO error".into(),
                ));
            }
            self.inner.load_events(aggregate_id).await
        }

        async fn load_aggregate(
            &self,
            aggregate_id: &str,
        ) -> Result<
            MemStoreAggregateContext<Redemption>,
            AggregateError<RedemptionError>,
        > {
            self.inner.load_aggregate(aggregate_id).await
        }

        async fn commit(
            &self,
            events: Vec<crate::redemption::RedemptionEvent>,
            context: MemStoreAggregateContext<Redemption>,
            metadata: HashMap<String, String>,
        ) -> Result<
            Vec<cqrs_es::EventEnvelope<Redemption>>,
            AggregateError<RedemptionError>,
        > {
            self.inner.commit(events, context, metadata).await
        }
    }

    /// Verifies that a transient store-load failure during polling does NOT
    /// permanently terminalize the redemption (no `MarkFailed` command).
    /// The error should propagate for retry, leaving the redemption intact.
    #[tokio::test]
    #[traced_test]
    async fn test_store_load_error_does_not_mark_redemption_failed() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        // FailingStore shares its inner MemStore so both cqrs and the
        // separate store Arc see the same committed events.
        let fail_load = Arc::new(AtomicBool::new(false));
        let failing_store =
            FailingStore::new(MemStore::default(), Arc::clone(&fail_load));

        let vault_service: Arc<dyn crate::vault::VaultService> =
            Arc::new(crate::vault::mock::MockVaultService::new_success());
        let cqrs = Arc::new(cqrs_es::CqrsFramework::new(
            failing_store.clone(),
            vec![],
            vault_service,
        ));
        let store_arc = Arc::new(failing_store);

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-store-fail-test");

        create_test_redemption_in_alpaca_called_state(
            cqrs.as_ref(),
            &issuer_request_id,
            &tokenization_request_id,
        )
        .await;

        // After seeding events, make subsequent load_events calls fail
        // to simulate a transient SQLite/IO error during polling.
        fail_load.store(true, Ordering::SeqCst);

        let mock = Arc::new(StatefulMockAlpacaService::new(
            vec![MockResponse::Success(RedeemRequestStatus::Completed)],
            issuer_request_id.clone(),
        ));

        let manager = JournalManager::new(
            mock as Arc<dyn AlpacaService>,
            cqrs.clone(),
            store_arc.clone(),
            pool,
        );

        let result = manager
            .handle_alpaca_called(
                &test_alpaca_account(),
                issuer_request_id.clone(),
                tokenization_request_id,
            )
            .await;

        assert!(
            matches!(result, Err(JournalManagerError::StoreLoad { .. })),
            "Expected StoreLoad error, not permanent failure, got {result:?}"
        );

        // Disable the failure so we can read events back for assertion.
        fail_load.store(false, Ordering::SeqCst);
        let events = store_arc
            .load_events(&issuer_request_id.to_string())
            .await
            .unwrap();
        let mut aggregate = Redemption::default();
        for event in events {
            aggregate.apply(event.payload);
        }
        assert!(
            matches!(aggregate, Redemption::AlpacaCalled { .. }),
            "Redemption must remain in AlpacaCalled state (not Failed) after a transient store error; got {aggregate:?}"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &["Transient store error during validation, will retry"]
            ),
            "Expected WARN log for transient store error"
        );
    }
}
