use cqrs_es::{AggregateError, EventStore};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use serde::Serialize;
use sqlx::{Pool, Sqlite};
use tracing::{error, info};

use crate::auth::InternalAuth;
use crate::mint::{
    IssuerMintRequestId, MintCommand, MintView, find_all_recoverable_mints,
    find_by_issuer_request_id,
};
use crate::redemption::{
    IssuerRedemptionRequestId, RedemptionCommand, RedemptionError,
    RedemptionEvent, RedemptionMetadata, RedemptionView, find_stuck,
};
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

struct ReprocessContext {
    metadata: RedemptionMetadata,
    /// Whether Alpaca was ever called for this redemption.
    /// If true, reprocessing would cause a duplicate Alpaca redeem call.
    alpaca_was_called: bool,
}

/// Loads all events for a redemption and extracts:
/// 1. The original `RedemptionMetadata` from the first `Detected` event
/// 2. Whether any post-Alpaca event exists (AlpacaCalled, AlpacaJournalCompleted)
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
    let mut alpaca_was_called = false;

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
            RedemptionEvent::AlpacaCalled { .. }
            | RedemptionEvent::AlpacaJournalCompleted { .. } => {
                alpaca_was_called = true;
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

    Ok(ReprocessContext { metadata, alpaca_was_called })
}

#[tracing::instrument(skip(_auth, cqrs, event_store))]
#[post("/admin/reprocess/redemption/<issuer_request_id>")]
pub(crate) async fn reprocess_redemption(
    _auth: InternalAuth,
    cqrs: &rocket::State<RedemptionCqrs>,
    event_store: &rocket::State<RedemptionEventStore>,
    issuer_request_id: IssuerRedemptionRequestId,
) -> Result<Json<ReprocessResponse>, Status> {
    let aggregate_id = issuer_request_id.to_string();

    let context =
        load_reprocess_context(event_store.inner(), &aggregate_id).await?;

    // Guard: reject post-Alpaca redemptions. The aggregate's Failed state
    // collapses pre-Alpaca (AlpacaCallFailed) and post-Alpaca (BurningFailed,
    // MarkFailed after burn) failures. Only pre-Alpaca failures are safe to
    // reprocess — post-Alpaca failures would cause a duplicate Alpaca call.
    // The event store is the authoritative source, not the view.
    if context.alpaca_was_called {
        error!(
            aggregate_id = %aggregate_id,
            "Cannot reprocess: Alpaca was already called for this redemption"
        );
        return Err(Status::UnprocessableEntity);
    }

    // Reprocess is only valid from Failed state (enforced by the aggregate).
    cqrs.execute(
        &aggregate_id,
        RedemptionCommand::Reprocess {
            issuer_request_id: issuer_request_id.clone(),
            metadata: context.metadata,
        },
    )
    .await
    .map_err(|err| {
        error!(
            aggregate_id = %aggregate_id,
            error = %err,
            "Failed to reprocess redemption"
        );
        match err {
            AggregateError::UserError(RedemptionError::AlreadyCompleted {
                ..
            }) => Status::Conflict,
            AggregateError::UserError(_) => Status::UnprocessableEntity,
            _ => Status::InternalServerError,
        }
    })?;

    info!(
        aggregate_id = %aggregate_id,
        "Redemption reprocessed from Failed to Detected"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id,
        previous_state: "Failed".to_string(),
        message: "Reprocessed to Detected state — will be picked up by recovery on next restart".to_string(),
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
