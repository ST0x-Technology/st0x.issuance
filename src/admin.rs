use chrono::{DateTime, Utc};
use cqrs_es::{AggregateError, EventStore};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::Quantity;
use crate::alpaca::{AlpacaService, RedeemRequestStatus, TokenizationRequest};
use crate::auth::InternalAuth;
use crate::mint::{
    IssuerMintRequestId, MintCommand, MintView, TokenizationRequestId,
    find_all_recoverable_mints, find_by_issuer_request_id,
};
use crate::redemption::{
    BurnRecord, IssuerRedemptionRequestId, RedemptionCommand, RedemptionError,
    RedemptionEvent, RedemptionMetadata, RedemptionView, find_stuck,
};
use crate::vault::{FireblocksTxStatus, VaultService};
use crate::{MintCqrs, RedemptionCqrs, RedemptionEventStore};

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AggregateKind {
    Mint,
    Redemption,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReprocessResponse {
    aggregate_type: AggregateKind,
    aggregate_id: String,
    previous_state: String,
    message: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct StuckAggregate {
    aggregate_type: AggregateKind,
    aggregate_id: String,
    state: String,
    detail: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct StuckResponse {
    stuck: Vec<StuckAggregate>,
}

/// Data extracted from an AlpacaCalled event in the event history.
struct AlpacaCalledData {
    tokenization_request_id: TokenizationRequestId,
    alpaca_quantity: Quantity,
    dust_quantity: Quantity,
    called_at: DateTime<Utc>,
}

/// Data extracted from a BurningFailed event in the event history.
struct BurningFailedData {
    fireblocks_tx_id: Option<String>,
    planned_burns: Vec<BurnRecord>,
}

struct ReprocessContext {
    metadata: RedemptionMetadata,
    /// Data from the AlpacaCalled event, if one exists.
    /// Present means Alpaca was already called — reprocessing back to Detected
    /// would cause a duplicate call. Absent means safe to reprocess.
    alpaca_called: Option<AlpacaCalledData>,
    /// Data from the most recent BurningFailed event, if one exists.
    burning_failed: Option<BurningFailedData>,
}

/// Loads all events for a redemption and extracts:
/// 1. The original `RedemptionMetadata` from the first `Detected` event
/// 2. AlpacaCalled data if any post-Alpaca event exists
///
/// This uses the event store as the authoritative source, not the view,
/// because the view's `Failed` state collapses pre-Alpaca and post-Alpaca
/// failures into the same variant.
async fn load_reprocess_context(
    event_store: &RedemptionEventStore,
    aggregate_id: &str,
) -> Result<ReprocessContext, Status> {
    let events =
        event_store.load_events(aggregate_id).await.map_err(|err| {
            error!(
                aggregate_id = aggregate_id,
                error = %err,
                "Failed to load redemption events"
            );
            Status::InternalServerError
        })?;

    if events.is_empty() {
        return Err(Status::NotFound);
    }

    let mut metadata = None;
    let mut alpaca_called = None;
    let mut burning_failed = None;

    for event in &events {
        match &event.payload {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            } => {
                if metadata.is_none() {
                    metadata = Some(RedemptionMetadata {
                        issuer_request_id: issuer_request_id.clone(),
                        underlying: underlying.clone(),
                        token: token.clone(),
                        wallet: *wallet,
                        quantity: quantity.clone(),
                        detected_tx_hash: *tx_hash,
                        block_number: *block_number,
                        detected_at: *detected_at,
                    });
                }
            }
            RedemptionEvent::AlpacaCalled {
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                ..
            } => {
                alpaca_called = Some(AlpacaCalledData {
                    tokenization_request_id: tokenization_request_id.clone(),
                    alpaca_quantity: alpaca_quantity.clone(),
                    dust_quantity: dust_quantity.clone(),
                    called_at: *called_at,
                });
            }
            RedemptionEvent::BurningFailed {
                fireblocks_tx_id,
                planned_burns,
                ..
            } => {
                burning_failed = Some(BurningFailedData {
                    fireblocks_tx_id: fireblocks_tx_id.clone(),
                    planned_burns: planned_burns.clone(),
                });
            }
            _ => {}
        }
    }

    let Some(metadata) = metadata else {
        error!(
            aggregate_id = aggregate_id,
            "No Detected event found in redemption event history"
        );
        return Err(Status::InternalServerError);
    };

    Ok(ReprocessContext { metadata, alpaca_called, burning_failed })
}

/// Unified recovery endpoint for stuck redemptions.
///
/// Auto-detects the right recovery path from the event history:
/// - **Pre-Alpaca failures**: Resets to `Detected` so `RedeemCallManager` re-calls Alpaca.
/// - **Post-Alpaca failures**: Polls Alpaca to verify the journal completed, then
///   resumes to `Burning` so `BurnManager` can execute the on-chain burn.
///   Refuses if Alpaca's journal hasn't completed (to avoid burning without backing).
#[tracing::instrument(skip(
    _auth,
    cqrs,
    event_store,
    alpaca_service,
    vault_service
))]
#[post("/admin/recover/redemption/<issuer_request_id>")]
pub(crate) async fn recover_redemption(
    _auth: InternalAuth,
    cqrs: &rocket::State<RedemptionCqrs>,
    event_store: &rocket::State<RedemptionEventStore>,
    alpaca_service: &rocket::State<Arc<dyn AlpacaService>>,
    vault_service: &rocket::State<Arc<dyn VaultService>>,
    issuer_request_id: IssuerRedemptionRequestId,
) -> Result<Json<ReprocessResponse>, Status> {
    let aggregate_id = issuer_request_id.to_string();

    let context =
        load_reprocess_context(event_store.inner(), &aggregate_id).await?;

    let Some(alpaca_data) = context.alpaca_called else {
        // Pre-Alpaca failure: safe to reset to Detected and re-call Alpaca.
        return recover_pre_alpaca(
            cqrs,
            &aggregate_id,
            issuer_request_id,
            context.metadata,
        )
        .await;
    };

    // Post-Alpaca failure: verify with Alpaca before burning.
    recover_post_alpaca(
        cqrs,
        alpaca_service,
        vault_service.inner(),
        PostAlpacaRecoveryInput {
            aggregate_id,
            issuer_request_id,
            metadata: context.metadata,
            alpaca_data,
            burning_failed: context.burning_failed,
        },
    )
    .await
}

async fn recover_pre_alpaca(
    cqrs: &RedemptionCqrs,
    aggregate_id: &str,
    issuer_request_id: IssuerRedemptionRequestId,
    metadata: RedemptionMetadata,
) -> Result<Json<ReprocessResponse>, Status> {
    cqrs.execute(
        aggregate_id,
        RedemptionCommand::Reprocess {
            issuer_request_id: issuer_request_id.clone(),
            metadata,
        },
    )
    .await
    .map_err(|err| {
        error!(
            aggregate_id = %aggregate_id,
            error = %err,
            "Failed to recover redemption (pre-Alpaca)"
        );
        map_redemption_error(&err)
    })?;

    info!(
        aggregate_id = %aggregate_id,
        "Redemption recovered from Failed to Detected"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id: aggregate_id.to_string(),
        previous_state: "Failed".to_string(),
        message:
            "Recovered to Detected — RedeemCallManager will re-call Alpaca"
                .to_string(),
    }))
}

/// All data needed to recover a post-Alpaca failed redemption.
struct PostAlpacaRecoveryInput {
    aggregate_id: String,
    issuer_request_id: IssuerRedemptionRequestId,
    metadata: RedemptionMetadata,
    alpaca_data: AlpacaCalledData,
    burning_failed: Option<BurningFailedData>,
}

async fn recover_post_alpaca(
    cqrs: &RedemptionCqrs,
    alpaca_service: &Arc<dyn AlpacaService>,
    vault_service: &Arc<dyn VaultService>,
    input: PostAlpacaRecoveryInput,
) -> Result<Json<ReprocessResponse>, Status> {
    let PostAlpacaRecoveryInput {
        aggregate_id,
        issuer_request_id,
        metadata,
        alpaca_data,
        burning_failed,
    } = input;
    // Verify journal status with Alpaca before resuming to Burning.
    // Burning without a completed journal would destroy on-chain tokens
    // without receiving the underlying shares.
    let request = alpaca_service
        .poll_request_status(&alpaca_data.tokenization_request_id)
        .await
        .map_err(|err| {
            error!(
                aggregate_id = %aggregate_id,
                tokenization_request_id = %alpaca_data.tokenization_request_id.0,
                error = %err,
                "Failed to poll Alpaca for journal status"
            );
            Status::BadGateway
        })?;

    let (status, alpaca_updated_at) = match &request {
        TokenizationRequest::Redeem {
            status,
            issuer_request_id: req_issuer_id,
            underlying: req_underlying,
            token: req_token,
            quantity: req_quantity,
            wallet: req_wallet,
            updated_at,
            ..
        } => {
            // Validate Alpaca's response matches our records — defense-in-depth
            // against data corruption or misrouted requests.
            if req_issuer_id != &metadata.issuer_request_id
                || req_underlying != &metadata.underlying
                || req_token != &metadata.token
                || req_quantity != &alpaca_data.alpaca_quantity
                || req_wallet != &metadata.wallet
            {
                error!(
                    aggregate_id = %aggregate_id,
                    "Alpaca response fields do not match redemption metadata"
                );
                return Err(Status::InternalServerError);
            }
            (status, updated_at)
        }
        TokenizationRequest::Mint { .. } => {
            error!(
                aggregate_id = %aggregate_id,
                "Alpaca returned Mint request for a redemption tokenization_request_id"
            );
            return Err(Status::InternalServerError);
        }
    };

    match status {
        RedeemRequestStatus::Completed => {}
        RedeemRequestStatus::Pending => {
            info!(
                aggregate_id = %aggregate_id,
                tokenization_request_id = %alpaca_data.tokenization_request_id.0,
                "Cannot recover: Alpaca journal still pending"
            );
            return Err(Status::UnprocessableEntity);
        }
        RedeemRequestStatus::Rejected => {
            info!(
                aggregate_id = %aggregate_id,
                tokenization_request_id = %alpaca_data.tokenization_request_id.0,
                "Cannot recover: Alpaca journal was rejected"
            );
            return Err(Status::UnprocessableEntity);
        }
    }

    // If we have a Fireblocks tx ID from a previous BurningFailed event,
    // check whether the burn actually succeeded on-chain.
    if let Some(ref bf_data) = burning_failed {
        if let Some(ref fb_tx_id) = bf_data.fireblocks_tx_id {
            match vault_service.check_fireblocks_tx(fb_tx_id).await {
                Ok(Some(FireblocksTxStatus::Completed {
                    tx_hash,
                    block_number,
                })) => {
                    if bf_data.planned_burns.is_empty() {
                        warn!(
                            aggregate_id = %aggregate_id,
                            fireblocks_tx_id = %fb_tx_id,
                            tx_hash = ?tx_hash,
                            "Pre-enrichment BurningFailed event has no planned_burns — \
                             burn records will be empty. Manual receipt inventory \
                             reconciliation may be needed after recovery."
                        );
                    }

                    info!(
                        aggregate_id = %aggregate_id,
                        fireblocks_tx_id = %fb_tx_id,
                        tx_hash = ?tx_hash,
                        "Fireblocks tx already completed on-chain, recording existing burn"
                    );

                    cqrs.execute(
                        &aggregate_id,
                        RedemptionCommand::RecordExistingBurn {
                            issuer_request_id: issuer_request_id.clone(),
                            fireblocks_tx_id: fb_tx_id.clone(),
                            tx_hash,
                            planned_burns: bf_data.planned_burns.clone(),
                            block_number,
                        },
                    )
                    .await
                    .map_err(|err| {
                        error!(
                            aggregate_id = %aggregate_id,
                            error = %err,
                            "Failed to record existing burn"
                        );
                        map_redemption_error(&err)
                    })?;

                    return Ok(Json(ReprocessResponse {
                        aggregate_type: AggregateKind::Redemption,
                        aggregate_id: aggregate_id.to_string(),
                        previous_state: "Failed".to_string(),
                        message:
                            "Existing on-chain burn recorded via Fireblocks tx lookup"
                                .to_string(),
                    }));
                }
                Ok(Some(FireblocksTxStatus::Pending)) => {
                    info!(
                        aggregate_id = %aggregate_id,
                        fireblocks_tx_id = %fb_tx_id,
                        "Fireblocks tx still pending, cannot recover yet"
                    );
                    return Err(Status::UnprocessableEntity);
                }
                Ok(Some(FireblocksTxStatus::Failed { status: fb_status })) => {
                    info!(
                        aggregate_id = %aggregate_id,
                        fireblocks_tx_id = %fb_tx_id,
                        fireblocks_status = %fb_status,
                        "Fireblocks tx failed, proceeding with ResumeBurn"
                    );
                    // Fall through to ResumeBurn below
                }
                Ok(None) => {
                    // Non-Fireblocks backend, fall through to ResumeBurn
                }
                Err(err) => {
                    error!(
                        aggregate_id = %aggregate_id,
                        fireblocks_tx_id = %fb_tx_id,
                        error = %err,
                        "Failed to check Fireblocks tx status"
                    );
                    return Err(Status::BadGateway);
                }
            }
        }
    }

    let alpaca_journal_completed_at = match alpaca_updated_at {
        Some(ts) => *ts,
        None => {
            error!(
                aggregate_id = %aggregate_id,
                "Alpaca returned completed status but updated_at is null"
            );
            return Err(Status::BadGateway);
        }
    };

    cqrs.execute(
        &aggregate_id,
        RedemptionCommand::ResumeBurn {
            issuer_request_id: issuer_request_id.clone(),
            metadata,
            tokenization_request_id: alpaca_data.tokenization_request_id,
            alpaca_quantity: alpaca_data.alpaca_quantity,
            dust_quantity: alpaca_data.dust_quantity,
            called_at: alpaca_data.called_at,
            alpaca_journal_completed_at,
        },
    )
    .await
    .map_err(|err| {
        error!(
            aggregate_id = %aggregate_id,
            error = %err,
            "Failed to recover redemption (post-Alpaca)"
        );
        map_redemption_error(&err)
    })?;

    info!(
        aggregate_id = %aggregate_id,
        "Redemption recovered from Failed to Burning"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id: aggregate_id.to_string(),
        previous_state: "Failed".to_string(),
        message:
            "Recovered to Burning — burn will execute on next service restart"
                .to_string(),
    }))
}

const fn map_redemption_error(err: &AggregateError<RedemptionError>) -> Status {
    match err {
        AggregateError::UserError(RedemptionError::AlreadyCompleted {
            ..
        }) => Status::Conflict,
        AggregateError::UserError(_) => Status::UnprocessableEntity,
        _ => Status::InternalServerError,
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CloseRedemptionRequest {
    reason: String,
}

/// Admin endpoint to close a failed redemption that cannot be automatically recovered.
///
/// Only valid from `Failed` state. Closed redemptions do not appear in stuck queries.
#[tracing::instrument(skip(_auth, cqrs))]
#[post(
    "/admin/close/redemption/<issuer_request_id>",
    format = "json",
    data = "<body>"
)]
pub(crate) async fn close_redemption(
    _auth: InternalAuth,
    cqrs: &rocket::State<RedemptionCqrs>,
    issuer_request_id: IssuerRedemptionRequestId,
    body: Json<CloseRedemptionRequest>,
) -> Result<Json<ReprocessResponse>, Status> {
    let aggregate_id = issuer_request_id.to_string();

    cqrs.execute(
        &aggregate_id,
        RedemptionCommand::CloseRedemption {
            issuer_request_id: issuer_request_id.clone(),
            reason: body.into_inner().reason,
        },
    )
    .await
    .map_err(|err| {
        error!(
            aggregate_id = %aggregate_id,
            error = %err,
            "Failed to close redemption"
        );
        map_redemption_error(&err)
    })?;

    info!(
        aggregate_id = %aggregate_id,
        "Redemption closed"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id,
        previous_state: "Failed".to_string(),
        message: "Redemption closed by admin".to_string(),
    }))
}

#[tracing::instrument(skip(_auth, cqrs, pool))]
#[post("/admin/reprocess/mint/<aggregate_id>")]
pub(crate) async fn reprocess_mint(
    _auth: InternalAuth,
    cqrs: &rocket::State<MintCqrs>,
    pool: &rocket::State<Pool<Sqlite>>,
    aggregate_id: &str,
) -> Result<Json<ReprocessResponse>, Status> {
    let issuer_request_id: IssuerMintRequestId = aggregate_id
        .parse::<uuid::Uuid>()
        .map(IssuerMintRequestId::new)
        .map_err(|_| Status::BadRequest)?;

    // Check current state via view
    let current_state =
        match find_by_issuer_request_id(pool.inner(), &issuer_request_id).await
        {
            Ok(Some(view)) => {
                let state = match &view {
                    MintView::NotFound => return Err(Status::NotFound),
                    MintView::Completed { .. } => return Err(Status::Conflict),
                    MintView::Initiated { .. } => "Initiated",
                    MintView::JournalConfirmed { .. } => "JournalConfirmed",
                    MintView::JournalRejected { .. } => "JournalRejected",
                    MintView::Minting { .. } => "Minting",
                    MintView::CallbackPending { .. } => "CallbackPending",
                    MintView::MintingFailed { .. } => "MintingFailed",
                };
                state.to_string()
            }
            Ok(None) => return Err(Status::NotFound),
            Err(err) => {
                error!(error = %err, "Failed to query mint view");
                return Err(Status::InternalServerError);
            }
        };

    cqrs.execute(
        aggregate_id,
        MintCommand::Recover { issuer_request_id: issuer_request_id.clone() },
    )
    .await
    .map_err(|err| {
        error!(
            aggregate_id = aggregate_id,
            error = %err,
            "Failed to reprocess mint"
        );
        match err {
            AggregateError::UserError(_) => Status::UnprocessableEntity,
            _ => Status::InternalServerError,
        }
    })?;

    info!(
        aggregate_id = aggregate_id,
        previous_state = %current_state,
        "Mint reprocessed successfully"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Mint,
        aggregate_id: aggregate_id.to_string(),
        previous_state: current_state,
        message: "Recovery initiated".to_string(),
    }))
}

#[tracing::instrument(skip(_auth, pool))]
#[get("/admin/stuck")]
pub(crate) async fn list_stuck(
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
) -> Result<Json<StuckResponse>, Status> {
    let mut stuck = Vec::new();

    // Stuck redemptions (Failed and BurnFailed)
    let stuck_redemptions = find_stuck(pool.inner()).await.map_err(|err| {
        error!(error = %err, "Failed to query stuck redemptions");
        Status::InternalServerError
    })?;

    for (id, view) in stuck_redemptions {
        let (state, detail) = match &view {
            RedemptionView::Failed { reason, .. } => {
                ("Failed".to_string(), reason.clone())
            }
            RedemptionView::BurnFailed { error, .. } => {
                ("BurnFailed".to_string(), error.clone())
            }
            _ => continue,
        };

        stuck.push(StuckAggregate {
            aggregate_type: AggregateKind::Redemption,
            aggregate_id: id.to_string(),
            state,
            detail,
        });
    }

    // Stuck mints (recoverable states)
    let recoverable_mints =
        find_all_recoverable_mints(pool.inner()).await.map_err(|err| {
            error!(error = %err, "Failed to query recoverable mints");
            Status::InternalServerError
        })?;

    for (id, view) in recoverable_mints {
        let (state, detail) = match &view {
            MintView::JournalConfirmed { .. } => (
                "JournalConfirmed".to_string(),
                "Waiting for deposit".to_string(),
            ),
            MintView::Minting { .. } => {
                ("Minting".to_string(), "Deposit in progress".to_string())
            }
            MintView::MintingFailed { error, .. } => {
                ("MintingFailed".to_string(), error.clone())
            }
            MintView::CallbackPending { .. } => (
                "CallbackPending".to_string(),
                "Waiting for callback".to_string(),
            ),
            _ => continue,
        };

        stuck.push(StuckAggregate {
            aggregate_type: AggregateKind::Mint,
            aggregate_id: id.to_string(),
            state,
            detail,
        });
    }

    Ok(Json(StuckResponse { stuck }))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use async_trait::async_trait;
    use chrono::Utc;
    use cqrs_es::persist::{GenericQuery, PersistedEventStore};
    use rocket::http::Status;
    use rust_decimal::Decimal;
    use sqlite_es::{
        SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        AlpacaCalledData, PostAlpacaRecoveryInput, load_reprocess_context,
        recover_post_alpaca,
    };
    use crate::alpaca::{
        AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
        RedeemRequestStatus, RedeemResponse, TokenizationRequest,
    };
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::{
        IssuerRedemptionRequestId, Redemption, RedemptionCommand,
        RedemptionMetadata, RedemptionView,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    fn mock_vault_service() -> Arc<dyn VaultService> {
        Arc::new(MockVaultService::new_success())
    }

    /// Configurable poll response for the test mock.
    enum PollResponse {
        Ok(TokenizationRequest),
        Error(AlpacaError),
    }

    /// Mock AlpacaService that returns a configurable response for
    /// `poll_request_status`. Other methods are unused in these tests.
    struct PollMockAlpaca {
        response: PollResponse,
    }

    fn redeem_response(
        status: RedeemRequestStatus,
        metadata: &RedemptionMetadata,
        alpaca_data: &AlpacaCalledData,
    ) -> TokenizationRequest {
        TokenizationRequest::Redeem {
            id: alpaca_data.tokenization_request_id.clone(),
            issuer_request_id: metadata.issuer_request_id.clone(),
            status,
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            quantity: alpaca_data.alpaca_quantity.clone(),
            wallet: metadata.wallet,
            tx_hash: None,
            updated_at: Some(Utc::now()),
        }
    }

    #[async_trait]
    impl AlpacaService for PollMockAlpaca {
        async fn send_mint_callback(
            &self,
            _request: MintCallbackRequest,
        ) -> Result<(), AlpacaError> {
            unimplemented!("not used in recover_post_alpaca tests")
        }

        async fn call_redeem_endpoint(
            &self,
            _request: RedeemRequest,
        ) -> Result<RedeemResponse, AlpacaError> {
            unimplemented!("not used in recover_post_alpaca tests")
        }

        async fn poll_request_status(
            &self,
            _tokenization_request_id: &TokenizationRequestId,
        ) -> Result<TokenizationRequest, AlpacaError> {
            match &self.response {
                PollResponse::Ok(request) => Ok(request.clone()),
                PollResponse::Error(err) => Err(match err {
                    AlpacaError::Api { status_code, body } => {
                        AlpacaError::Api {
                            status_code: *status_code,
                            body: body.clone(),
                        }
                    }
                    AlpacaError::RequestNotFound(id) => {
                        AlpacaError::RequestNotFound(id.clone())
                    }
                    AlpacaError::Auth(msg) => AlpacaError::Auth(msg.clone()),
                    _ => {
                        AlpacaError::Auth("unsupported mock error".to_string())
                    }
                }),
            }
        }
    }

    fn test_metadata() -> RedemptionMetadata {
        RedemptionMetadata {
            issuer_request_id: IssuerRedemptionRequestId::random(),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(100)),
            detected_tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        }
    }

    fn test_alpaca_data() -> AlpacaCalledData {
        AlpacaCalledData {
            tokenization_request_id: TokenizationRequestId::new("tok-test-1"),
            alpaca_quantity: Quantity::new(Decimal::from(100)),
            dust_quantity: Quantity::new(Decimal::ZERO),
            called_at: Utc::now(),
        }
    }

    type TestEventStore =
        Arc<PersistedEventStore<SqliteEventRepository, Redemption>>;

    async fn setup_pool() -> sqlx::Pool<sqlx::Sqlite> {
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

    fn setup_cqrs(
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> (Arc<SqliteCqrs<Redemption>>, TestEventStore) {
        let view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let query = GenericQuery::new(view_repo);
        let vault_service = Arc::new(MockVaultService::new_success());
        let cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(query)],
            vault_service,
        ));
        let event_store = Arc::new(PersistedEventStore::new_event_store(
            SqliteEventRepository::new(pool.clone()),
        ));
        (cqrs, event_store)
    }

    /// Sets up an in-memory SQLite CQRS framework with a redemption in Failed
    /// state (post-Alpaca, i.e. with AlpacaCalled event in history).
    async fn setup_failed_redemption() -> (
        Arc<SqliteCqrs<Redemption>>,
        TestEventStore,
        RedemptionMetadata,
        AlpacaCalledData,
    ) {
        let pool = setup_pool().await;
        let (cqrs, event_store) = setup_cqrs(&pool);

        let metadata = test_metadata();
        let alpaca_data = test_alpaca_data();
        let aggregate_id = metadata.issuer_request_id.to_string();

        // Drive aggregate to Failed state (post-Alpaca)
        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::Detect {
                issuer_request_id: metadata.issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
            },
        )
        .await
        .expect("Detect failed");

        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: metadata.issuer_request_id.clone(),
                tokenization_request_id: alpaca_data
                    .tokenization_request_id
                    .clone(),
                alpaca_quantity: alpaca_data.alpaca_quantity.clone(),
                dust_quantity: alpaca_data.dust_quantity.clone(),
            },
        )
        .await
        .expect("RecordAlpacaCall failed");

        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::MarkFailed {
                issuer_request_id: metadata.issuer_request_id.clone(),
                reason: "Journal timed out".to_string(),
            },
        )
        .await
        .expect("MarkFailed failed");

        (cqrs, event_store, metadata, alpaca_data)
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_completed_succeeds() {
        let (cqrs, _event_store, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });

        let result = recover_post_alpaca(
            &cqrs,
            &alpaca,
            &mock_vault_service(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata: metadata.clone(),
                alpaca_data,
                burning_failed: None,
            },
        )
        .await;

        let response = result.expect("Expected Ok response");
        assert_eq!(response.previous_state, "Failed");
        assert!(response.message.contains("Burning"));
        assert!(logs_contain_at!(Level::INFO, &["recovered", "Burning"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_pending_returns_422() {
        let (cqrs, _event_store, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Pending,
                &metadata,
                &alpaca_data,
            )),
        });

        let result = recover_post_alpaca(
            &cqrs,
            &alpaca,
            &mock_vault_service(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::UnprocessableEntity);
        assert!(logs_contain_at!(Level::INFO, &["journal still pending"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_rejected_returns_422() {
        let (cqrs, _event_store, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Rejected,
                &metadata,
                &alpaca_data,
            )),
        });

        let result = recover_post_alpaca(
            &cqrs,
            &alpaca,
            &mock_vault_service(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::UnprocessableEntity);
        assert!(logs_contain_at!(Level::INFO, &["journal was rejected"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_api_error_returns_502() {
        let (cqrs, _event_store, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Error(AlpacaError::Api {
                status_code: 500,
                body: "Internal Server Error".to_string(),
            }),
        });

        let result = recover_post_alpaca(
            &cqrs,
            &alpaca,
            &mock_vault_service(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::BadGateway);
        assert!(logs_contain_at!(Level::ERROR, &["Failed to poll Alpaca"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_mint_type_returns_500() {
        let (cqrs, _event_store, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(TokenizationRequest::Mint {}),
        });

        let result = recover_post_alpaca(
            &cqrs,
            &alpaca,
            &mock_vault_service(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::InternalServerError);
        assert!(logs_contain_at!(
            Level::ERROR,
            &["Mint request", "redemption"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_field_mismatch_returns_500() {
        let (cqrs, _event_store, metadata, alpaca_data) =
            setup_failed_redemption().await;

        // Return a response with a different underlying symbol
        let mut mismatched = redeem_response(
            RedeemRequestStatus::Completed,
            &metadata,
            &alpaca_data,
        );
        if let TokenizationRequest::Redeem { ref mut underlying, .. } =
            mismatched
        {
            *underlying = UnderlyingSymbol::new("WRONG");
        }

        let alpaca: Arc<dyn AlpacaService> =
            Arc::new(PollMockAlpaca { response: PollResponse::Ok(mismatched) });

        let result = recover_post_alpaca(
            &cqrs,
            &alpaca,
            &mock_vault_service(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::InternalServerError);
        assert!(logs_contain_at!(
            Level::ERROR,
            &["do not match redemption metadata"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_load_reprocess_context_not_found_for_unknown_aggregate() {
        let pool = setup_pool().await;
        let (_cqrs, event_store) = setup_cqrs(&pool);

        let result =
            load_reprocess_context(&event_store, "nonexistent-id").await;

        assert_eq!(result.err(), Some(Status::NotFound));
    }

    /// Build a minimal Rocket instance with the recovery endpoint and all
    /// required managed state for endpoint-level tests.
    async fn test_rocket(
        alpaca: Arc<dyn AlpacaService>,
    ) -> (
        rocket::Rocket<rocket::Build>,
        Arc<SqliteCqrs<Redemption>>,
        TestEventStore,
    ) {
        use crate::alpaca::service::AlpacaConfig;
        use crate::auth::{FailedAuthRateLimiter, test_auth_config};
        use crate::config::{Config, LogLevel};
        use crate::fireblocks::SignerConfig;
        use alloy::primitives::B256;
        use url::Url;

        let config = Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").unwrap(),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
            subgraph_url: Url::parse("http://localhost:0/subgraph").unwrap(),
        };

        let pool = setup_pool().await;
        let (cqrs, event_store) = setup_cqrs(&pool);

        let rocket = rocket::build()
            .manage(config)
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(cqrs.clone())
            .manage(event_store.clone())
            .manage(alpaca)
            .manage(mock_vault_service())
            .mount("/", rocket::routes![super::recover_redemption]);

        (rocket, cqrs, event_store)
    }

    /// Drives a redemption to Failed state with only a Detected event
    /// (pre-Alpaca failure path).
    async fn setup_pre_alpaca_failure(
        cqrs: &Arc<SqliteCqrs<Redemption>>,
    ) -> RedemptionMetadata {
        let metadata = test_metadata();
        let aggregate_id = metadata.issuer_request_id.to_string();

        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::Detect {
                issuer_request_id: metadata.issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
            },
        )
        .await
        .expect("Detect failed");

        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::MarkFailed {
                issuer_request_id: metadata.issuer_request_id.clone(),
                reason: "Pre-Alpaca failure".to_string(),
            },
        )
        .await
        .expect("MarkFailed failed");

        metadata
    }

    /// Drives a redemption to Failed state after AlpacaCalled (post-Alpaca).
    async fn setup_post_alpaca_failure(
        cqrs: &Arc<SqliteCqrs<Redemption>>,
    ) -> (RedemptionMetadata, AlpacaCalledData) {
        let metadata = test_metadata();
        let alpaca_data = test_alpaca_data();
        let aggregate_id = metadata.issuer_request_id.to_string();

        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::Detect {
                issuer_request_id: metadata.issuer_request_id.clone(),
                underlying: metadata.underlying.clone(),
                token: metadata.token.clone(),
                wallet: metadata.wallet,
                quantity: metadata.quantity.clone(),
                tx_hash: metadata.detected_tx_hash,
                block_number: metadata.block_number,
            },
        )
        .await
        .expect("Detect failed");

        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: metadata.issuer_request_id.clone(),
                tokenization_request_id: alpaca_data
                    .tokenization_request_id
                    .clone(),
                alpaca_quantity: alpaca_data.alpaca_quantity.clone(),
                dust_quantity: alpaca_data.dust_quantity.clone(),
            },
        )
        .await
        .expect("RecordAlpacaCall failed");

        cqrs.execute(
            &aggregate_id,
            RedemptionCommand::MarkFailed {
                issuer_request_id: metadata.issuer_request_id.clone(),
                reason: "Journal timed out".to_string(),
            },
        )
        .await
        .expect("MarkFailed failed");

        (metadata, alpaca_data)
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_pre_alpaca_recovery_resets_to_detected() {
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &test_metadata(),
                &test_alpaca_data(),
            )),
        });

        let (rocket, cqrs, _event_store) = test_rocket(alpaca).await;
        let metadata = setup_pre_alpaca_failure(&cqrs).await;

        let client =
            rocket::local::asynchronous::Client::tracked(rocket).await.unwrap();

        let response = client
            .post(format!(
                "/admin/recover/redemption/{}",
                metadata.issuer_request_id
            ))
            .header(rocket::http::Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.unwrap();
        assert!(body.contains("Detected"));
        assert!(logs_contain_at!(Level::INFO, &["recovered", "Detected"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_post_alpaca_recovery_resumes_to_burning() {
        // Must create a no-op mock initially; we replace the state after
        // setup. Instead, use a single pool/cqrs and build the mock from
        // the actual metadata produced by setup.
        let pool = setup_pool().await;
        let (cqrs, event_store) = setup_cqrs(&pool);
        let (metadata, alpaca_data) = setup_post_alpaca_failure(&cqrs).await;

        // Now build the mock with the real metadata so field validation passes.
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });

        let rocket = {
            use crate::alpaca::service::AlpacaConfig;
            use crate::auth::{FailedAuthRateLimiter, test_auth_config};
            use crate::config::{Config, LogLevel};
            use crate::fireblocks::SignerConfig;
            use alloy::primitives::B256;
            use url::Url;

            let config = Config {
                database_url: "sqlite::memory:".to_string(),
                database_max_connections: 5,
                rpc_url: Url::parse("wss://localhost:8545").unwrap(),
                chain_id: crate::test_utils::ANVIL_CHAIN_ID,
                signer: SignerConfig::Local(B256::ZERO),
                backfill_start_block: 0,
                auth: test_auth_config().unwrap(),
                log_level: LogLevel::Debug,
                hyperdx: None,
                alpaca: AlpacaConfig::test_default(),
                subgraph_url: Url::parse("http://localhost:0/subgraph")
                    .unwrap(),
            };

            rocket::build()
                .manage(config)
                .manage(FailedAuthRateLimiter::new().unwrap())
                .manage(cqrs)
                .manage(event_store)
                .manage(alpaca)
                .manage(mock_vault_service())
                .mount("/", rocket::routes![super::recover_redemption])
        };

        let client =
            rocket::local::asynchronous::Client::tracked(rocket).await.unwrap();

        let response = client
            .post(format!(
                "/admin/recover/redemption/{}",
                metadata.issuer_request_id
            ))
            .header(rocket::http::Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.unwrap();
        assert!(body.contains("Burning"));
        assert!(logs_contain_at!(Level::INFO, &["recovered", "Burning"]));
    }
}
