use alloy::primitives::B256;
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
    IssuerMintRequestId, MintCommand, MintEvent, MintView,
    TokenizationRequestId, find_all_recoverable_mints,
    find_by_issuer_request_id, recovery::spawn_scheduled_mint_recovery,
};
use crate::redemption::{
    BurnRecord, IssuerRedemptionRequestId, RedemptionCommand, RedemptionError,
    RedemptionEvent, RedemptionMetadata, RedemptionView, find_stuck,
};
use crate::tokenized_asset::UnderlyingSymbol;
use crate::vault::{FireblocksTxStatus, VaultService};
use crate::{MintCqrs, MintEventStore, RedemptionCqrs, RedemptionEventStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    tokenization_request_id: Option<TokenizationRequestId>,
    state: String,
    detail: String,
    timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    underlying: Option<UnderlyingSymbol>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quantity: Option<Quantity>,
    /// Primary on-chain transaction hash for this aggregate, when known.
    /// For redemptions this is the detected transfer tx hash. For mints this
    /// is the successful mint tx hash, or a Fireblocks network hash when the
    /// signing backend exposes one.
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_hash: Option<B256>,
    /// Fireblocks transaction ID associated with this aggregate's current
    /// stuck step. For mints, sourced from the most recent
    /// `FireblocksSubmitted` event. For redemptions, populated only on
    /// `BurnFailed` (the view carries it for that variant).
    #[serde(skip_serializing_if = "Option::is_none")]
    fireblocks_tx_id: Option<String>,
    /// Live status of the Fireblocks transaction (subStatus, network tx
    /// hashes). Best-effort: omitted when `fireblocks_tx_id` is missing,
    /// the backend is non-Fireblocks, or the lookup itself fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    fireblocks_status: Option<FireblocksTxStatus>,
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
            error!(target: "admin", aggregate_id = aggregate_id,
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
        error!(target: "admin", aggregate_id = aggregate_id,
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
        error!(target: "admin", aggregate_id = %aggregate_id,
            error = %err,
            "Failed to recover redemption (pre-Alpaca)"
        );
        map_redemption_error(&err)
    })?;

    info!(target: "admin", aggregate_id = %aggregate_id,
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
            error!(target: "admin", aggregate_id = %aggregate_id,
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
                error!(target: "admin", aggregate_id = %aggregate_id,
                    "Alpaca response fields do not match redemption metadata"
                );
                return Err(Status::InternalServerError);
            }
            (status, updated_at)
        }
        TokenizationRequest::Mint { .. } => {
            error!(target: "admin", aggregate_id = %aggregate_id,
                "Alpaca returned Mint request for a redemption tokenization_request_id"
            );
            return Err(Status::InternalServerError);
        }
    };

    match status {
        RedeemRequestStatus::Completed => {}
        RedeemRequestStatus::Pending => {
            info!(target: "admin", aggregate_id = %aggregate_id,
                tokenization_request_id = %alpaca_data.tokenization_request_id.0,
                "Cannot recover: Alpaca journal still pending"
            );
            return Err(Status::UnprocessableEntity);
        }
        RedeemRequestStatus::Rejected => {
            info!(target: "admin", aggregate_id = %aggregate_id,
                tokenization_request_id = %alpaca_data.tokenization_request_id.0,
                "Cannot recover: Alpaca journal was rejected"
            );
            return Err(Status::UnprocessableEntity);
        }
    }

    // If we have a Fireblocks tx ID from a previous BurningFailed event,
    // check whether the burn actually succeeded on-chain.
    if let Some(ref bf_data) = burning_failed
        && let Some(ref fb_tx_id) = bf_data.fireblocks_tx_id
    {
        match vault_service.check_fireblocks_tx(fb_tx_id).await {
            Ok(Some(FireblocksTxStatus::Completed {
                tx_hash,
                block_number,
            })) => {
                if bf_data.planned_burns.is_empty() {
                    warn!(target: "admin", aggregate_id = %aggregate_id,
                        fireblocks_tx_id = %fb_tx_id,
                        tx_hash = ?tx_hash,
                        "Pre-enrichment BurningFailed event has no planned_burns — \
                         burn records will be empty. Manual receipt inventory \
                         reconciliation may be needed after recovery."
                    );
                }

                info!(target: "admin", aggregate_id = %aggregate_id,
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
                    error!(target: "admin", aggregate_id = %aggregate_id,
                        error = %err,
                        "Failed to record existing burn"
                    );
                    map_redemption_error(&err)
                })?;

                return Ok(Json(ReprocessResponse {
                    aggregate_type: AggregateKind::Redemption,
                    aggregate_id: aggregate_id.clone(),
                    previous_state: "Failed".to_string(),
                    message:
                        "Existing on-chain burn recorded via Fireblocks tx lookup"
                            .to_string(),
                }));
            }
            Ok(Some(FireblocksTxStatus::Pending)) => {
                info!(target: "admin", aggregate_id = %aggregate_id,
                    fireblocks_tx_id = %fb_tx_id,
                    "Fireblocks tx still pending, cannot recover yet"
                );
                return Err(Status::UnprocessableEntity);
            }
            Ok(Some(FireblocksTxStatus::Failed {
                detail: fb_detail,
                sub_status: fb_sub_status,
                network_tx_hashes: _,
            })) => {
                info!(target: "admin", aggregate_id = %aggregate_id,
                    fireblocks_tx_id = %fb_tx_id,
                    fireblocks_status = %fb_detail,
                    fireblocks_sub_status = ?fb_sub_status,
                    "Fireblocks tx failed, proceeding with ResumeBurn"
                );
                // Fall through to ResumeBurn below
            }
            Ok(None) => {
                // Non-Fireblocks backend, fall through to ResumeBurn
            }
            Err(err) => {
                error!(target: "admin", aggregate_id = %aggregate_id,
                    fireblocks_tx_id = %fb_tx_id,
                    error = %err,
                    "Failed to check Fireblocks tx status"
                );
                return Err(Status::BadGateway);
            }
        }
    }

    let Some(alpaca_journal_completed_at) = alpaca_updated_at else {
        error!(target: "admin", aggregate_id = %aggregate_id,
            "Alpaca returned completed status but updated_at is null"
        );
        return Err(Status::BadGateway);
    };
    let alpaca_journal_completed_at = *alpaca_journal_completed_at;

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
        error!(target: "admin", aggregate_id = %aggregate_id,
            error = %err,
            "Failed to recover redemption (post-Alpaca)"
        );
        map_redemption_error(&err)
    })?;

    info!(target: "admin", aggregate_id = %aggregate_id,
        "Redemption recovered from Failed to Burning"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id: aggregate_id.clone(),
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
        error!(target: "admin", aggregate_id = %aggregate_id,
            error = %err,
            "Failed to close redemption"
        );
        map_redemption_error(&err)
    })?;

    info!(target: "admin", aggregate_id = %aggregate_id,
        "Redemption closed"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id,
        previous_state: "Failed".to_string(),
        message: "Redemption closed by admin".to_string(),
    }))
}

#[tracing::instrument(skip(_auth, cqrs, event_store, pool))]
#[post("/admin/reprocess/mint/<aggregate_id>")]
pub(crate) async fn reprocess_mint(
    _auth: InternalAuth,
    cqrs: &rocket::State<MintCqrs>,
    event_store: &rocket::State<MintEventStore>,
    pool: &rocket::State<Pool<Sqlite>>,
    aggregate_id: &str,
) -> Result<Json<ReprocessResponse>, Status> {
    let issuer_request_id: IssuerMintRequestId = aggregate_id
        .parse::<uuid::Uuid>()
        .map(IssuerMintRequestId::new)
        .map_err(|_| Status::BadRequest)?;

    // Check current state via view
    let current_state = match find_by_issuer_request_id(
        pool.inner(),
        &issuer_request_id,
    )
    .await
    {
        Ok(Some(view)) => {
            let state = match &view {
                MintView::NotFound => return Err(Status::NotFound),
                MintView::Completed { .. } | MintView::Closed { .. } => {
                    return Err(Status::Conflict);
                }
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
            error!(target: "admin", error = %err, "Failed to query mint view");
            return Err(Status::InternalServerError);
        }
    };

    cqrs.execute(
        aggregate_id,
        MintCommand::Recover {
            issuer_request_id: issuer_request_id.clone(),
            mode: crate::mint::MintRecoveryMode::Manual,
        },
    )
    .await
    .map_err(|err| {
        error!(target: "admin", aggregate_id = aggregate_id,
            error = %err,
            "Failed to reprocess mint"
        );
        match err {
            AggregateError::UserError(_) => Status::UnprocessableEntity,
            _ => Status::InternalServerError,
        }
    })?;

    info!(target: "admin", aggregate_id = aggregate_id,
        previous_state = %current_state,
        "Mint reprocessed successfully"
    );

    // A single manual Recover advances the mint by one step (e.g. submits the
    // next retry, leaving it in FireblocksSubmitted). Hand it to a background
    // scheduled-recovery task so it is driven to completion instead of
    // stalling until the next restart or another manual reprocess.
    spawn_scheduled_mint_recovery(
        cqrs.inner().clone(),
        event_store.inner().clone(),
        issuer_request_id,
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Mint,
        aggregate_id: aggregate_id.to_string(),
        previous_state: current_state,
        message: "Recovery initiated".to_string(),
    }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CloseMintRequest {
    reason: String,
}

/// Admin endpoint to close a mint that cannot be automatically recovered.
///
/// Valid from any non-terminal state. Closed mints do not appear in stuck queries.
#[tracing::instrument(skip(_auth, cqrs))]
#[post("/admin/close/mint/<aggregate_id>", format = "json", data = "<body>")]
pub(crate) async fn close_mint(
    _auth: InternalAuth,
    cqrs: &rocket::State<MintCqrs>,
    aggregate_id: &str,
    body: Json<CloseMintRequest>,
) -> Result<Json<ReprocessResponse>, Status> {
    let issuer_request_id: IssuerMintRequestId = aggregate_id
        .parse::<uuid::Uuid>()
        .map(IssuerMintRequestId::new)
        .map_err(|_| Status::BadRequest)?;

    cqrs.execute(
        aggregate_id,
        MintCommand::CloseMint {
            issuer_request_id: issuer_request_id.clone(),
            reason: body.into_inner().reason,
        },
    )
    .await
    .map_err(|err| {
        error!(target: "admin", aggregate_id = %aggregate_id,
            error = %err,
            "Failed to close mint"
        );
        match err {
            AggregateError::UserError(_) => Status::UnprocessableEntity,
            _ => Status::InternalServerError,
        }
    })?;

    info!(target: "admin", aggregate_id = %aggregate_id, "Mint closed");

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Mint,
        aggregate_id: aggregate_id.to_string(),
        previous_state: "Unknown".to_string(),
        message: "Mint closed by admin".to_string(),
    }))
}

#[tracing::instrument(skip(
    _auth,
    pool,
    mint_event_store,
    redemption_event_store,
    vault_service
))]
#[get("/admin/stuck")]
pub(crate) async fn list_stuck(
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
    mint_event_store: &rocket::State<MintEventStore>,
    redemption_event_store: &rocket::State<RedemptionEventStore>,
    vault_service: &rocket::State<Arc<dyn VaultService>>,
) -> Result<Json<StuckResponse>, Status> {
    let mut stuck = Vec::new();

    // Stuck redemptions (Failed and BurnFailed)
    let stuck_redemptions = find_stuck(pool.inner()).await.map_err(|err| {
        error!(target: "admin", error = %err, "Failed to query stuck redemptions");
        Status::InternalServerError
    })?;

    for (issuer_redemption_request_id, view) in stuck_redemptions {
        let history = redemption_history_summary(
            redemption_event_store.inner(),
            &issuer_redemption_request_id.to_string(),
        )
        .await;

        if let Some(entry) = stuck_redemption_entry(
            &issuer_redemption_request_id,
            &view,
            history,
        ) {
            stuck.push(entry);
        }
    }

    // Stuck mints (recoverable states)
    let recoverable_mints =
        find_all_recoverable_mints(pool.inner()).await.map_err(|err| {
            error!(target: "admin", error = %err, "Failed to query recoverable mints");
            Status::InternalServerError
        })?;

    for (issuer_mint_request_id, view) in recoverable_mints {
        let Some((tokenization_request_id, state, detail, timestamp)) =
            mint_view_summary(&view)
        else {
            continue;
        };

        let (underlying, quantity) = mint_view_asset(&view);
        let mint_history = mint_history_summary(
            mint_event_store.inner(),
            &issuer_mint_request_id.to_string(),
        )
        .await;

        stuck.push(StuckAggregate {
            aggregate_type: AggregateKind::Mint,
            aggregate_id: issuer_mint_request_id.to_string(),
            tokenization_request_id,
            state,
            detail,
            timestamp,
            underlying,
            quantity,
            tx_hash: mint_history.tx_hash,
            fireblocks_tx_id: mint_history.fireblocks_tx_id,
            fireblocks_status: None,
        });
    }

    enrich_with_fireblocks_status(&mut stuck, vault_service.inner().as_ref())
        .await;

    Ok(Json(StuckResponse { stuck }))
}

/// Best-effort enrichment: for every stuck entry with a `fireblocks_tx_id`,
/// query the vault service for the live Fireblocks status (subStatus + network
/// records) and attach it to the entry. Lookup failures and non-Fireblocks
/// backends leave `fireblocks_status` as `None`; this is purely additive
/// information for operators triaging stuck transactions.
async fn enrich_with_fireblocks_status(
    stuck: &mut [StuckAggregate],
    vault_service: &dyn VaultService,
) {
    for entry in stuck.iter_mut() {
        let Some(tx_id) = entry.fireblocks_tx_id.as_deref() else {
            continue;
        };

        match vault_service.check_fireblocks_tx(tx_id).await {
            Ok(Some(status)) => {
                if entry.tx_hash.is_none() {
                    entry.tx_hash = tx_hash_from_fireblocks_status(&status);
                }
                entry.fireblocks_status = Some(status);
            }
            Ok(None) => {}
            Err(err) => {
                warn!(target: "admin",
                    aggregate_id = %entry.aggregate_id,
                    fireblocks_tx_id = %tx_id,
                    error = %err,
                    "Failed to enrich stuck entry with Fireblocks status"
                );
            }
        }
    }
}

fn stuck_redemption_entry(
    issuer_redemption_request_id: &IssuerRedemptionRequestId,
    view: &RedemptionView,
    history: RedemptionHistorySummary,
) -> Option<StuckAggregate> {
    let (
        tokenization_request_id,
        state,
        detail,
        timestamp,
        underlying,
        quantity,
        tx_hash,
        fireblocks_tx_id,
    ) = match view {
        RedemptionView::Failed { reason, failed_at, .. } => (
            history.tokenization_request_id,
            "Failed".to_string(),
            reason.clone(),
            *failed_at,
            history.underlying,
            history.quantity,
            history.tx_hash,
            history.fireblocks_tx_id,
        ),
        RedemptionView::BurnFailed {
            tokenization_request_id,
            underlying,
            quantity,
            tx_hash,
            error,
            failed_at,
            fireblocks_tx_id,
            ..
        } => (
            Some(tokenization_request_id.clone()),
            "BurnFailed".to_string(),
            error.clone(),
            *failed_at,
            Some(underlying.clone()),
            Some(quantity.clone()),
            Some(*tx_hash),
            fireblocks_tx_id.clone().or(history.fireblocks_tx_id),
        ),
        // find_stuck only returns Failed/BurnFailed; surface any other
        // variant as a real mismatch rather than fabricating placeholder
        // data that could mislead operators.
        _ => return None,
    };

    Some(StuckAggregate {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id: issuer_redemption_request_id.to_string(),
        tokenization_request_id,
        state,
        detail,
        timestamp,
        underlying,
        quantity,
        tx_hash,
        fireblocks_tx_id,
        fireblocks_status: None,
    })
}

#[derive(Debug, Default)]
struct RedemptionHistorySummary {
    tokenization_request_id: Option<TokenizationRequestId>,
    underlying: Option<UnderlyingSymbol>,
    quantity: Option<Quantity>,
    tx_hash: Option<B256>,
    fireblocks_tx_id: Option<String>,
}

async fn redemption_history_summary(
    event_store: &crate::SqliteEventStore<crate::redemption::Redemption>,
    aggregate_id: &str,
) -> RedemptionHistorySummary {
    let events = match event_store.load_events(aggregate_id).await {
        Ok(events) => events,
        Err(err) => {
            warn!(
                target: "admin",
                aggregate_id = aggregate_id,
                error = %err,
                "Failed to load redemption events for stuck metadata lookup"
            );
            return RedemptionHistorySummary::default();
        }
    };

    let mut summary = RedemptionHistorySummary::default();
    for event in events {
        match event.payload {
            RedemptionEvent::Detected {
                underlying, quantity, tx_hash, ..
            }
            | RedemptionEvent::Reprocessed {
                underlying,
                quantity,
                tx_hash,
                ..
            }
            | RedemptionEvent::BurnResumed {
                underlying,
                quantity,
                tx_hash,
                ..
            } => {
                summary.underlying = Some(underlying);
                summary.quantity = Some(quantity);
                summary.tx_hash = Some(tx_hash);
            }
            RedemptionEvent::AlpacaCalled {
                tokenization_request_id, ..
            } => {
                summary.tokenization_request_id = Some(tokenization_request_id);
            }
            RedemptionEvent::BurnFireblocksSubmitted {
                fireblocks_tx_id,
                ..
            }
            | RedemptionEvent::ExistingBurnRecovered {
                fireblocks_tx_id, ..
            }
            | RedemptionEvent::BurningFailed {
                fireblocks_tx_id: Some(fireblocks_tx_id),
                ..
            } => {
                summary.fireblocks_tx_id = Some(fireblocks_tx_id);
            }
            _ => {}
        }
    }

    summary
}

fn mint_view_summary(
    view: &MintView,
) -> Option<(Option<TokenizationRequestId>, String, String, DateTime<Utc>)> {
    match view {
        MintView::JournalConfirmed {
            tokenization_request_id,
            journal_confirmed_at,
            ..
        } => Some((
            Some(tokenization_request_id.clone()),
            "JournalConfirmed".to_string(),
            "Waiting for deposit".to_string(),
            *journal_confirmed_at,
        )),
        MintView::Minting {
            tokenization_request_id,
            minting_started_at,
            ..
        } => Some((
            Some(tokenization_request_id.clone()),
            "Minting".to_string(),
            "Deposit in progress".to_string(),
            *minting_started_at,
        )),
        MintView::MintingFailed {
            tokenization_request_id,
            error,
            failed_at,
            ..
        } => Some((
            Some(tokenization_request_id.clone()),
            "MintingFailed".to_string(),
            error.clone(),
            *failed_at,
        )),
        MintView::CallbackPending {
            tokenization_request_id,
            minted_at,
            ..
        } => Some((
            Some(tokenization_request_id.clone()),
            "CallbackPending".to_string(),
            "Waiting for callback".to_string(),
            *minted_at,
        )),
        _ => None,
    }
}

fn mint_view_asset(
    view: &MintView,
) -> (Option<UnderlyingSymbol>, Option<Quantity>) {
    match view {
        MintView::JournalConfirmed { underlying, quantity, .. }
        | MintView::Minting { underlying, quantity, .. }
        | MintView::MintingFailed { underlying, quantity, .. }
        | MintView::CallbackPending { underlying, quantity, .. } => {
            (Some(underlying.clone()), Some(quantity.clone()))
        }
        _ => (None, None),
    }
}

#[derive(Debug, Default)]
struct MintHistorySummary {
    tx_hash: Option<B256>,
    fireblocks_tx_id: Option<String>,
}

/// Returns the latest useful transaction hints from this mint's history.
async fn mint_history_summary(
    event_store: &crate::SqliteEventStore<crate::mint::Mint>,
    aggregate_id: &str,
) -> MintHistorySummary {
    let events = match event_store.load_events(aggregate_id).await {
        Ok(events) => events,
        Err(err) => {
            warn!(
                target: "admin",
                aggregate_id = aggregate_id,
                error = %err,
                "Failed to load mint events for fireblocks_tx_id lookup"
            );
            return MintHistorySummary::default();
        }
    };

    let mut summary = MintHistorySummary::default();
    for event in events {
        match event.payload {
            MintEvent::TokensMinted { tx_hash, .. }
            | MintEvent::ExistingMintRecovered { tx_hash, .. }
            | MintEvent::MintRetryStarted { tx_hash: Some(tx_hash), .. } => {
                summary.tx_hash = Some(tx_hash);
            }
            MintEvent::FireblocksSubmitted { fireblocks_tx_id, .. } => {
                summary.fireblocks_tx_id = Some(fireblocks_tx_id);
            }
            _ => {}
        }
    }

    summary
}

fn tx_hash_from_fireblocks_status(status: &FireblocksTxStatus) -> Option<B256> {
    match status {
        FireblocksTxStatus::Completed { tx_hash, .. } => Some(*tx_hash),
        FireblocksTxStatus::Failed { network_tx_hashes, .. } => {
            network_tx_hashes.iter().find_map(|tx_hash| match tx_hash.parse() {
                Ok(parsed) => Some(parsed),
                Err(err) => {
                    warn!(target: "admin",
                        network_tx_hash = %tx_hash,
                        error = %err,
                        "Skipping malformed Fireblocks network_tx_hash"
                    );
                    None
                }
            })
        }
        FireblocksTxStatus::Pending => None,
    }
}

/// Response for the Fireblocks transaction status lookup endpoint.
#[derive(Debug, Serialize)]
pub(crate) struct FireblocksTxResponse {
    fireblocks_tx_id: String,
    #[serde(flatten)]
    status: FireblocksTxStatus,
}

/// Admin endpoint to look up a Fireblocks transaction status.
///
/// Useful for checking orphaned transactions that were submitted but never
/// recorded in the event store (e.g. due to recovery timeout).
#[tracing::instrument(skip(_auth, vault_service))]
#[get("/admin/fireblocks/tx/<fireblocks_tx_id>")]
pub(crate) async fn check_fireblocks_tx(
    _auth: InternalAuth,
    vault_service: &rocket::State<Arc<dyn VaultService>>,
    fireblocks_tx_id: &str,
) -> Result<Json<FireblocksTxResponse>, Status> {
    let result = vault_service
        .check_fireblocks_tx(fireblocks_tx_id)
        .await
        .map_err(|err| {
            error!(target: "admin",
                fireblocks_tx_id = %fireblocks_tx_id,
                error = %err,
                "Failed to check Fireblocks transaction"
            );
            Status::BadGateway
        })?;

    let Some(fb_status) = result else {
        // Non-Fireblocks backend — check_fireblocks_tx returns None.
        return Err(Status::NotFound);
    };

    let response = FireblocksTxResponse {
        fireblocks_tx_id: fireblocks_tx_id.to_string(),
        status: fb_status,
    };

    Ok(Json(response))
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
    use crate::vault::mock::MockVaultService;
    use crate::vault::{FireblocksTxStatus, VaultService};

    use super::{AggregateKind, StuckAggregate};

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

    #[test]
    fn failed_redemption_stuck_entry_uses_history_metadata() {
        let metadata = test_metadata();
        let tokenization_request_id = TokenizationRequestId::new("tok-red-1");
        let fireblocks_tx_id = "fb-redemption-1".to_string();
        let failed_at = Utc::now();
        let view = RedemptionView::Failed {
            issuer_request_id: metadata.issuer_request_id.clone(),
            reason: "Fireblocks burn confirmation failed".to_string(),
            failed_at,
        };
        let history = super::RedemptionHistorySummary {
            tokenization_request_id: Some(tokenization_request_id.clone()),
            underlying: Some(metadata.underlying.clone()),
            quantity: Some(metadata.quantity.clone()),
            tx_hash: Some(metadata.detected_tx_hash),
            fireblocks_tx_id: Some(fireblocks_tx_id.clone()),
        };

        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            &view,
            history,
        )
        .expect("failed redemption should produce stuck entry");

        assert_eq!(entry.aggregate_type, AggregateKind::Redemption);
        assert_eq!(entry.aggregate_id, metadata.issuer_request_id.to_string());
        assert_eq!(
            entry.tokenization_request_id,
            Some(tokenization_request_id)
        );
        assert_eq!(entry.underlying, Some(metadata.underlying));
        assert_eq!(entry.quantity, Some(metadata.quantity));
        assert_eq!(entry.tx_hash, Some(metadata.detected_tx_hash));
        assert_eq!(entry.fireblocks_tx_id, Some(fireblocks_tx_id));
        assert_eq!(entry.timestamp, failed_at);
    }

    #[test]
    fn burn_failed_stuck_entry_prefers_view_metadata() {
        let metadata = test_metadata();
        let tokenization_request_id = TokenizationRequestId::new("tok-red-2");
        let fireblocks_tx_id = "fb-burn-failed".to_string();
        let failed_at = Utc::now();
        let view = RedemptionView::BurnFailed {
            issuer_request_id: metadata.issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            alpaca_quantity: metadata.quantity.clone(),
            dust_quantity: Quantity::default(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at: metadata.detected_at,
            called_at: Utc::now(),
            alpaca_journal_completed_at: Utc::now(),
            error: "burn failed".to_string(),
            failed_at,
            fireblocks_tx_id: Some(fireblocks_tx_id.clone()),
            planned_burns: vec![],
        };

        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            &view,
            super::RedemptionHistorySummary::default(),
        )
        .expect("burn failed redemption should produce stuck entry");

        assert_eq!(
            entry.tokenization_request_id,
            Some(tokenization_request_id)
        );
        assert_eq!(entry.tx_hash, Some(metadata.detected_tx_hash));
        assert_eq!(entry.fireblocks_tx_id, Some(fireblocks_tx_id));
        assert_eq!(entry.underlying, Some(metadata.underlying));
        assert_eq!(entry.quantity, Some(metadata.quantity));
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

    /// Stub VaultService that returns a pre-canned `FireblocksTxStatus` for a
    /// specific tx id and `None` for everything else. Lets us drive
    /// `enrich_with_fireblocks_status` without spinning up Fireblocks.
    struct StubFireblocksVault {
        tx_id: String,
        response: FireblocksTxStatus,
    }

    #[async_trait]
    impl VaultService for StubFireblocksVault {
        async fn submit_mint(
            &self,
            _vault: alloy::primitives::Address,
            _assets: alloy::primitives::U256,
            _bot: alloy::primitives::Address,
            _user: alloy::primitives::Address,
            _receipt_info: crate::vault::ReceiptInformation,
            _external_tx_id: Option<String>,
        ) -> Result<crate::vault::SubmittedTx, crate::vault::VaultError>
        {
            unimplemented!("not used in enrichment tests")
        }

        async fn confirm_mint(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<crate::vault::MintResult, crate::vault::VaultError>
        {
            unimplemented!("not used in enrichment tests")
        }

        async fn get_share_balance(
            &self,
            _vault: alloy::primitives::Address,
            _owner: alloy::primitives::Address,
        ) -> Result<alloy::primitives::U256, crate::vault::VaultError> {
            unimplemented!("not used in enrichment tests")
        }

        async fn check_fireblocks_tx(
            &self,
            fireblocks_tx_id: &str,
        ) -> Result<Option<FireblocksTxStatus>, crate::vault::VaultError>
        {
            if fireblocks_tx_id == self.tx_id {
                Ok(Some(self.response.clone()))
            } else {
                Ok(None)
            }
        }

        async fn submit_burn(
            &self,
            _params: crate::vault::MultiBurnParams,
        ) -> Result<crate::vault::SubmittedTx, crate::vault::VaultError>
        {
            unimplemented!("not used in enrichment tests")
        }

        async fn confirm_burn(
            &self,
            _fireblocks_tx_id: &str,
            _dust_shares: alloy::primitives::U256,
        ) -> Result<crate::vault::MultiBurnResult, crate::vault::VaultError>
        {
            unimplemented!("not used in enrichment tests")
        }
    }

    fn stuck_entry(id: &str, fireblocks_tx_id: Option<&str>) -> StuckAggregate {
        StuckAggregate {
            aggregate_type: AggregateKind::Mint,
            aggregate_id: id.to_string(),
            tokenization_request_id: None,
            state: "MintingFailed".to_string(),
            detail: "Fireblocks transaction X reached terminal status: Failed"
                .to_string(),
            timestamp: Utc::now(),
            underlying: None,
            quantity: None,
            tx_hash: None,
            fireblocks_tx_id: fireblocks_tx_id.map(str::to_owned),
            fireblocks_status: None,
        }
    }

    #[tokio::test]
    async fn enrich_populates_status_for_matching_tx_id() {
        let tx_id = "a29e5027-1e44-4a66-b78b-b579e55757db";
        let stub = StubFireblocksVault {
            tx_id: tx_id.to_string(),
            response: FireblocksTxStatus::Failed {
                detail: "Failed".to_string(),
                sub_status: Some("INSUFFICIENT_FUNDS".to_string()),
                network_tx_hashes: vec![
                    "0xabc0000000000000000000000000000000000000000000000000000000000001"
                        .to_string(),
                ],
            },
        };

        let mut entries = vec![stuck_entry("mint-1", Some(tx_id))];
        super::enrich_with_fireblocks_status(&mut entries, &stub).await;

        match entries[0].fireblocks_status.as_ref().expect(
            "matching tx id should produce a populated fireblocks_status",
        ) {
            FireblocksTxStatus::Failed {
                detail,
                sub_status,
                network_tx_hashes,
            } => {
                assert_eq!(detail, "Failed");
                assert_eq!(sub_status.as_deref(), Some("INSUFFICIENT_FUNDS"),);
                assert_eq!(network_tx_hashes.len(), 1);
            }
            other => panic!("unexpected status: {other:?}"),
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn enrich_warns_on_malformed_network_tx_hash() {
        let tx_id = "a29e5027-1e44-4a66-b78b-b579e55757db";
        let stub = StubFireblocksVault {
            tx_id: tx_id.to_string(),
            response: FireblocksTxStatus::Failed {
                detail: "Failed".to_string(),
                sub_status: Some("REJECTED_BY_BLOCKCHAIN".to_string()),
                network_tx_hashes: vec!["not-a-hash".to_string()],
            },
        };

        let mut entries = vec![stuck_entry("mint-1", Some(tx_id))];
        super::enrich_with_fireblocks_status(&mut entries, &stub).await;

        // The unparseable hash must not be silently dropped: tx_hash stays
        // None and the bad value is surfaced in a warning.
        assert!(entries[0].tx_hash.is_none());
        assert!(logs_contain_at!(
            Level::WARN,
            &["malformed Fireblocks network_tx_hash", "not-a-hash"]
        ));
    }

    #[tokio::test]
    async fn enrich_leaves_entries_without_tx_id_untouched() {
        let stub = StubFireblocksVault {
            tx_id: "any".to_string(),
            response: FireblocksTxStatus::Pending,
        };

        let mut entries = vec![
            stuck_entry("redemption-no-tx", None),
            stuck_entry("redemption-unknown-tx", Some("unrelated-tx")),
        ];

        super::enrich_with_fireblocks_status(&mut entries, &stub).await;

        // Entry without a tx id is skipped entirely.
        assert!(entries[0].fireblocks_status.is_none());
        // Entry with a tx id the stub doesn't recognise gets `Ok(None)` back
        // from the vault service, which maps to a left-as-None status.
        assert!(entries[1].fireblocks_status.is_none());
    }

    #[test]
    fn failed_status_serializes_with_sub_status_and_network_hashes() {
        let status = FireblocksTxStatus::Failed {
            detail: "Failed".to_string(),
            sub_status: Some("BLOCKED_BY_POLICY".to_string()),
            network_tx_hashes: vec!["0xdeadbeef".to_string()],
        };

        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["status"], "failed");
        assert_eq!(json["detail"], "Failed");
        assert_eq!(json["sub_status"], "BLOCKED_BY_POLICY");
        assert_eq!(json["network_tx_hashes"][0], "0xdeadbeef");
    }
}
