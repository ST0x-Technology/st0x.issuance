use alloy::network::ReceiptResponse;
use alloy::primitives::B256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::AggregateError;
use event_sorcery::{EventSourced, LifecycleError, Store};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::Quantity;
use crate::alpaca::{
    AlpacaError, AlpacaService, RedeemRequestStatus, TokenizationRequest,
};
use crate::auth::InternalAuth;
use crate::mint::{
    DriveOutcome, IssuerMintRequestId, Mint, MintCommand, MintEvent, MintView,
    TokenizationRequestId, find_by_issuer_request_id,
    find_stuck as find_stuck_mints, recover_mint_manually,
};
use crate::redemption::Redemption;
use crate::redemption::burn_manager::{
    BurnManager, BurnManagerError, MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
    RecoveryOutcome,
};
use crate::redemption::{
    BurnExternalTxId, BurnRecord, IssuerRedemptionRequestId, RedemptionCommand,
    RedemptionError, RedemptionEvent, RedemptionMetadata, RedemptionView,
    find_stuck as find_stuck_redemptions,
    next_burn_retry_external_tx_id_from_history,
};
use crate::tokenized_asset::UnderlyingSymbol;
use crate::vault::{BurnVerification, TxId, VaultError, VaultService};

#[async_trait]
pub(crate) trait RedemptionBurnRecovery: Send + Sync {
    async fn execute_recovered_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<RecoveryOutcome, BurnManagerError>;

    /// Terminalizes a redemption whose persisted exact burn already landed
    /// on-chain, after binding and verifying `burn_tx_hash` against the chain.
    /// Returns the on-chain verification for the admin response.
    async fn force_complete_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        reason: String,
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    ) -> Result<BurnVerification, BurnManagerError>;
}

#[async_trait]
impl RedemptionBurnRecovery for BurnManager {
    async fn execute_recovered_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        self.recover_burning_redemption(issuer_request_id).await
    }

    async fn force_complete_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        reason: String,
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    ) -> Result<BurnVerification, BurnManagerError> {
        self.force_complete_burn(
            issuer_request_id,
            burn_tx_hash,
            reason,
            acknowledged_unresolved_burn_tx_hash,
        )
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AggregateKind {
    Mint,
    Redemption,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct ReprocessResponse {
    aggregate_type: AggregateKind,
    aggregate_id: String,
    previous_state: String,
    message: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct StuckAggregate {
    aggregate_type: AggregateKind,
    aggregate_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    tokenization_request_id: Option<TokenizationRequestId>,
    state: String,
    detail: String,
    #[schema(value_type = String)]
    timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    underlying: Option<UnderlyingSymbol>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    quantity: Option<Quantity>,
    /// Primary on-chain transaction hash for this aggregate, when known.
    /// For redemptions this is the detected transfer tx hash. For mints this
    /// is the successful mint tx hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    tx_hash: Option<B256>,
    /// Transaction ID associated with this aggregate's current
    /// stuck step. For mints, sourced from the most recent
    /// `MintTxSubmitted` event. For redemptions, populated from the persisted
    /// burn intent or a later submission/failure event.
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_id: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
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
    tx_id: Option<TxId>,
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
    /// Replacement externalTxId for a retry burn, when event
    /// history shows a prior accepted burn or an unaccepted retry attempt.
    burn_retry_external_tx_id: Option<BurnExternalTxId>,
    burn_recovery_exhausted: bool,
}

/// Loads all events for a redemption and extracts:
/// 1. The original `RedemptionMetadata` from the first `Detected` event
/// 2. AlpacaCalled data if any post-Alpaca event exists
///
/// This uses the event store as the authoritative source, not the view,
/// because the view's `Failed` state collapses pre-Alpaca and post-Alpaca
/// failures into the same variant.
async fn load_reprocess_context(
    pool: &Pool<Sqlite>,
    aggregate_id: &IssuerRedemptionRequestId,
) -> Result<ReprocessContext, Status> {
    let aggregate_id_str = aggregate_id.to_string();
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
        ORDER BY sequence
        "#,
        aggregate_id_str
    )
    .fetch_all(pool)
    .await
    .map_err(|err| {
        error!(target: "admin", aggregate_id = %aggregate_id_str,
            error = %err,
            "Failed to load redemption events"
        );
        Status::InternalServerError
    })?;

    if rows.is_empty() {
        return Err(Status::NotFound);
    }

    let events: Vec<RedemptionEvent> = rows
        .iter()
        .map(|row| serde_json::from_str(&row.payload))
        .collect::<Result<_, _>>()
        .map_err(|err: serde_json::Error| {
            error!(target: "admin", aggregate_id = %aggregate_id_str,
                error = %err,
                "Failed to deserialize redemption events"
            );
            Status::InternalServerError
        })?;

    let mut metadata = None;
    let mut alpaca_called = None;
    let mut burning_failed = None;

    for event in &events {
        match event {
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
            RedemptionEvent::BurningFailed { tx_id, planned_burns, .. } => {
                burning_failed = Some(BurningFailedData {
                    tx_id: tx_id.clone(),
                    planned_burns: planned_burns.clone(),
                });
            }
            _ => {}
        }
    }

    let Some(metadata) = metadata else {
        error!(target: "admin", aggregate_id = %aggregate_id_str,
            "No Detected event found in redemption event history"
        );
        return Err(Status::InternalServerError);
    };

    let burn_retry_external_tx_id =
        next_burn_retry_external_tx_id_from_history(
            &metadata.detected_tx_hash,
            events.iter(),
        )
        .map_err(|error| {
            error!(target: "admin", aggregate_id = %aggregate_id_str,
                %error,
                "Failed to compute next burn retry external tx id"
            );
            Status::InternalServerError
        })?;
    let burn_recovery_exhausted = events.iter().any(|event| {
        matches!(
            event,
            RedemptionEvent::BurnRecoveryExhausted { .. }
                | RedemptionEvent::BurnPreparationRecoveryExhausted { .. }
        )
    }) || events
        .iter()
        .filter(|event| {
            matches!(
                event,
                RedemptionEvent::BurnRecoveryAttempted { .. }
                    | RedemptionEvent::BurnPreparationRecoveryAttempted { .. }
            )
        })
        .count()
        >= usize::try_from(MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS).map_err(
            |error| {
                error!(target: "admin", aggregate_id = %aggregate_id_str,
                    %error,
                    "Burn recovery limit does not fit this platform"
                );
                Status::InternalServerError
            },
        )?;

    Ok(ReprocessContext {
        metadata,
        alpaca_called,
        burning_failed,
        burn_retry_external_tx_id,
        burn_recovery_exhausted,
    })
}

/// Unified recovery endpoint for stuck redemptions.
///
/// Auto-detects the right recovery path from the event history:
/// - **Pre-Alpaca failures**: Resets to `Detected` so `RedeemCallManager` re-calls Alpaca.
/// - **Post-Alpaca failures**: Polls Alpaca to verify the journal completed, then
///   resumes to `Burning` and invokes burn recovery immediately.
///   Refuses if Alpaca's journal hasn't completed (to avoid burning without backing).
#[utoipa::path(
    post,
    path = "/admin/recover/redemption/{issuer_request_id}",
    tag = "admin",
    params(
        ("issuer_request_id" = String, Path,
            description = "Issuer redemption request id of the stuck redemption")
    ),
    responses(
        (status = 200, description = "Recovery initiated; describes the path taken",
            body = ReprocessResponse),
        (status = 404, description = "No redemption found for this id"),
        (status = 409, description = "Redemption already completed"),
        (status = 422,
            description = "Cannot recover: Alpaca journal pending/rejected, \
                prior burn not confirmed reverted, or invalid aggregate state"),
        (status = 502,
            description = "Alpaca poll or burn execution failed"),
        (status = 500, description = "Event load/deserialize or internal failure")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(
    _auth,
    store,
    pool,
    alpaca_service,
    vault_service,
    burn_recovery
))]
#[post("/admin/recover/redemption/<issuer_request_id>")]
pub(crate) async fn recover_redemption(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<Redemption>>>,
    pool: &rocket::State<Pool<Sqlite>>,
    alpaca_service: &rocket::State<Arc<dyn AlpacaService>>,
    vault_service: &rocket::State<Arc<dyn VaultService>>,
    burn_recovery: &rocket::State<Arc<dyn RedemptionBurnRecovery>>,
    issuer_request_id: IssuerRedemptionRequestId,
) -> Result<Json<ReprocessResponse>, Status> {
    let aggregate_id = issuer_request_id.to_string();

    let context =
        load_reprocess_context(pool.inner(), &issuer_request_id).await?;

    if context.burn_recovery_exhausted {
        warn!(target: "admin", aggregate_id = %aggregate_id,
            "Refusing to re-arm a redemption with exhausted automatic burn recovery"
        );
        return Err(Status::UnprocessableEntity);
    }

    let Some(alpaca_data) = context.alpaca_called else {
        // Pre-Alpaca failure: safe to reset to Detected and re-call Alpaca.
        return recover_pre_alpaca(
            store,
            &aggregate_id,
            issuer_request_id,
            context.metadata,
        )
        .await;
    };

    // Post-Alpaca failure: verify with Alpaca before burning.
    recover_post_alpaca(
        store,
        alpaca_service,
        vault_service.inner(),
        burn_recovery.inner(),
        PostAlpacaRecoveryInput {
            aggregate_id,
            issuer_request_id,
            metadata: context.metadata,
            alpaca_data,
            burning_failed: context.burning_failed,
            burn_retry_external_tx_id: context.burn_retry_external_tx_id,
        },
    )
    .await
}

async fn recover_pre_alpaca(
    store: &Store<Redemption>,
    aggregate_id: &str,
    issuer_request_id: IssuerRedemptionRequestId,
    metadata: RedemptionMetadata,
) -> Result<Json<ReprocessResponse>, Status> {
    store
        .send(
            &issuer_request_id,
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
    burn_retry_external_tx_id: Option<BurnExternalTxId>,
}

async fn recover_post_alpaca(
    store: &Store<Redemption>,
    alpaca_service: &Arc<dyn AlpacaService>,
    vault_service: &Arc<dyn VaultService>,
    burn_recovery: &Arc<dyn RedemptionBurnRecovery>,
    input: PostAlpacaRecoveryInput,
) -> Result<Json<ReprocessResponse>, Status> {
    let PostAlpacaRecoveryInput {
        aggregate_id,
        issuer_request_id,
        metadata,
        alpaca_data,
        burning_failed,
        burn_retry_external_tx_id,
        ..
    } = input;
    // Verify journal status with Alpaca before resuming to Burning.
    // Burning without a completed journal would destroy on-chain tokens
    // without receiving the underlying shares.
    let request = alpaca_service
        .poll_request_status(&alpaca_data.tokenization_request_id)
        .await
        .map_err(|err| {
            let (status, msg) = match &err {
                AlpacaError::RequestNotFound { .. } => (
                    Status::NotFound,
                    "Tokenization request not found at Alpaca (404)",
                ),
                AlpacaError::ResponseIdMismatch { .. } => (
                    Status::BadGateway,
                    "Alpaca returned a mismatched tokenization request id",
                ),
                AlpacaError::Reqwest(_)
                | AlpacaError::Parse { .. }
                | AlpacaError::Auth(_)
                | AlpacaError::Api { .. } => (
                    Status::BadGateway,
                    "Failed to poll Alpaca for journal status",
                ),
            };
            error!(target: "admin", aggregate_id = %aggregate_id,
                tokenization_request_id = %alpaca_data.tokenization_request_id,
                error = %err,
                status = status.code,
                "{msg}"
            );
            status
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
                tokenization_request_id = %alpaca_data.tokenization_request_id,
                "Cannot recover: Alpaca journal still pending"
            );
            return Err(Status::UnprocessableEntity);
        }
        RedeemRequestStatus::Rejected => {
            info!(target: "admin", aggregate_id = %aggregate_id,
                tokenization_request_id = %alpaca_data.tokenization_request_id,
                "Cannot recover: Alpaca journal was rejected"
            );
            return Err(Status::UnprocessableEntity);
        }
    }

    // If a transaction ID was recorded on a previous BurningFailed event,
    // inspect it before deciding whether to record the existing burn or resume.
    let burn_retry_external_tx_id = match inspect_prior_burn(
        store,
        vault_service,
        &aggregate_id,
        &issuer_request_id,
        &metadata.detected_tx_hash,
        burning_failed.as_ref(),
        burn_retry_external_tx_id,
    )
    .await?
    {
        PriorBurnDisposition::AlreadyRecorded(response) => {
            return Ok(Json(response));
        }
        PriorBurnDisposition::ResumeWith(external_tx_id) => external_tx_id,
    };

    let Some(alpaca_journal_completed_at) = alpaca_updated_at else {
        error!(target: "admin", aggregate_id = %aggregate_id,
            "Alpaca returned completed status but updated_at is null"
        );
        return Err(Status::BadGateway);
    };
    let alpaca_journal_completed_at = *alpaca_journal_completed_at;

    store
        .send(
            &issuer_request_id,
            RedemptionCommand::ResumeBurn {
                issuer_request_id: issuer_request_id.clone(),
                metadata,
                tokenization_request_id: alpaca_data.tokenization_request_id,
                alpaca_quantity: alpaca_data.alpaca_quantity,
                dust_quantity: alpaca_data.dust_quantity,
                called_at: alpaca_data.called_at,
                alpaca_journal_completed_at,
                external_tx_id: burn_retry_external_tx_id,
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

    let outcome = burn_recovery
        .execute_recovered_burn(&issuer_request_id)
        .await
        .map_err(|err| {
            error!(target: "admin", aggregate_id = %aggregate_id,
                error = %err,
                "Failed to execute recovered redemption burn"
            );
            Status::BadGateway
        })?;

    let message = report_recovery_outcome(outcome, &aggregate_id);

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id: aggregate_id.clone(),
        previous_state: "Failed".to_string(),
        message: message.to_string(),
    }))
}

/// Outcome of inspecting a prior burn on a failed redemption.
enum PriorBurnDisposition {
    /// The prior burn already completed on-chain and was recorded; the caller
    /// should return this response directly.
    AlreadyRecorded(ReprocessResponse),
    /// No prior burn exists, or the prior burn conclusively reverted; resume
    /// with this (possibly fallback) retry `externalTxId`.
    ResumeWith(Option<BurnExternalTxId>),
}

/// Inspects the prior tx (if any) from a previous `BurningFailed` event to
/// decide whether the on-chain burn already succeeded (record it), conclusively
/// reverted (resume with a fresh replacement `externalTxId`), or remains
/// ambiguous (require manual intervention).
async fn inspect_prior_burn(
    store: &Store<Redemption>,
    vault_service: &Arc<dyn VaultService>,
    aggregate_id: &str,
    issuer_request_id: &IssuerRedemptionRequestId,
    detected_tx_hash: &B256,
    burning_failed: Option<&BurningFailedData>,
    burn_retry_external_tx_id: Option<BurnExternalTxId>,
) -> Result<PriorBurnDisposition, Status> {
    let Some(bf_data) = burning_failed else {
        return Ok(PriorBurnDisposition::ResumeWith(burn_retry_external_tx_id));
    };
    let Some(tx_id) = bf_data.tx_id.as_ref() else {
        return Ok(PriorBurnDisposition::ResumeWith(burn_retry_external_tx_id));
    };

    match vault_service.check_tx(tx_id).await {
        Ok(receipt) => {
            let Some(block_number) = receipt.block_number() else {
                error!(target: "admin", aggregate_id = %aggregate_id,
                    tx_hash = ?tx_id,
                    "Completed burn transaction receipt is missing block number"
                );
                return Err(Status::InternalServerError);
            };

            if bf_data.planned_burns.is_empty() {
                warn!(target: "admin", aggregate_id = %aggregate_id,
                    tx_hash = ?tx_id,
                    "BurningFailed event has no planned_burns — \
                     burn records will be empty. Manual receipt inventory \
                     reconciliation may be needed after recovery."
                );
            }

            info!(target: "admin", aggregate_id = %aggregate_id,
                tx_hash = ?tx_id,
                "Transaction already completed on-chain, recording existing burn"
            );

            store
                .send(
                    issuer_request_id,
                    RedemptionCommand::RecordExistingBurn {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_id: tx_id.clone(),
                        tx_hash: receipt.transaction_hash(),
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

            Ok(PriorBurnDisposition::AlreadyRecorded(ReprocessResponse {
                aggregate_type: AggregateKind::Redemption,
                aggregate_id: aggregate_id.to_string(),
                previous_state: "Failed".to_string(),
                message: "Existing on-chain burn recorded via tx lookup"
                    .to_string(),
            }))
        }
        Err(VaultError::Reverted { .. }) => {
            // The terminally failed tx permanently reserves its externalTxId,
            // so the replacement burn must never reuse the base id. When event
            // history has no recorded retry id, fall back to retry-1 — mirror
            // of the startup recovery path in BurnManager.
            let retry_external_tx_id =
                burn_retry_external_tx_id.or_else(|| {
                    Some(Redemption::retry_burn_external_tx_id_typed(
                        detected_tx_hash,
                        1,
                    ))
                });

            info!(target: "admin", aggregate_id = %aggregate_id,
                tx_hash = %tx_id,
                retry_external_tx_id = ?retry_external_tx_id,
                "Transaction reverted onchain, proceeding with ResumeBurn"
            );

            Ok(PriorBurnDisposition::ResumeWith(retry_external_tx_id))
        }
        Err(VaultError::MissingBlockNumber { tx_hash }) => {
            error!(target: "admin", aggregate_id = %aggregate_id,
                %tx_hash,
                "Completed burn transaction receipt is missing block number"
            );
            Err(Status::InternalServerError)
        }
        Err(error) => {
            warn!(target: "admin", aggregate_id = %aggregate_id,
                issuer_request_id = %issuer_request_id,
                %tx_id,
                error = %error,
                "Prior burn outcome is ambiguous; manual intervention required"
            );
            Err(Status::UnprocessableEntity)
        }
    }
}

/// Logs each recovery outcome at a severity matching what actually happened and
/// returns the operator-facing message. Only `SkippedManualIntervention` leaves
/// the redemption unresolved, so it alone warns; the other outcomes describe
/// their distinct resolutions without ever claiming a burn that didn't run.
fn report_recovery_outcome(
    outcome: RecoveryOutcome,
    aggregate_id: &str,
) -> &'static str {
    match outcome {
        RecoveryOutcome::Executed => {
            info!(target: "admin", aggregate_id = %aggregate_id, outcome = ?outcome,
                "Recovered redemption and executed burn immediately"
            );
            "Recovered from Failed and executed burn immediately"
        }
        RecoveryOutcome::ExistingBurnRecorded => {
            info!(target: "admin", aggregate_id = %aggregate_id, outcome = ?outcome,
                "Recovered redemption by recording a previously submitted on-chain burn"
            );
            "Recovered from Failed and recorded a previously submitted on-chain burn"
        }
        RecoveryOutcome::SkippedManualIntervention => {
            warn!(target: "admin", aggregate_id = %aggregate_id, outcome = ?outcome,
                "Recovered redemption to Burning but burn was skipped; manual intervention required"
            );
            "Recovered to Burning but burn skipped: on-chain balance \
             insufficient, manual intervention required"
        }
        RecoveryOutcome::AlreadyAdvanced => {
            info!(target: "admin", aggregate_id = %aggregate_id, outcome = ?outcome,
                "Redemption had already advanced past Burning; no burn executed"
            );
            "Redemption had already advanced past Burning; no burn executed"
        }
    }
}

const fn map_redemption_error(
    err: &AggregateError<LifecycleError<Redemption>>,
) -> Status {
    match err {
        AggregateError::UserError(LifecycleError::Apply(
            RedemptionError::AlreadyCompleted { .. },
        )) => Status::Conflict,
        AggregateError::UserError(_) => Status::UnprocessableEntity,
        _ => Status::InternalServerError,
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(crate) struct CloseRedemptionRequest {
    reason: String,
    #[schema(value_type = Option<String>)]
    acknowledged_unresolved_burn_tx_hash: Option<B256>,
}

/// Admin endpoint to close a redemption that cannot be automatically recovered.
///
/// Valid from `Failed`, `Burning`, or `BurnSubmitted` — the honest terminal
/// path for a redemption whose burn is not verifiable on-chain (e.g. a
/// `Failed -> Burning` recovery regression, or an ambiguous case pending
/// off-chain reconciliation). For a redemption whose burn *did* land on-chain,
/// use `/admin/force-complete/redemption` instead. Closed redemptions do not
/// appear in stuck queries.
#[utoipa::path(
    post,
    path = "/admin/close/redemption/{issuer_request_id}",
    tag = "admin",
    params(
        ("issuer_request_id" = String, Path,
            description = "Issuer redemption request id to close")
    ),
    request_body = CloseRedemptionRequest,
    responses(
        (status = 200, description = "Redemption closed by admin",
            body = ReprocessResponse),
        (status = 409, description = "Redemption already completed"),
        (status = 422, description = "Invalid state transition for close"),
        (status = 500, description = "Event load/deserialize or internal failure")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, store, pool))]
#[post(
    "/admin/close/redemption/<issuer_request_id>",
    format = "json",
    data = "<body>"
)]
pub(crate) async fn close_redemption(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<Redemption>>>,
    pool: &rocket::State<Pool<Sqlite>>,
    issuer_request_id: IssuerRedemptionRequestId,
    body: Json<CloseRedemptionRequest>,
) -> Result<Json<ReprocessResponse>, Status> {
    let aggregate_id = issuer_request_id.to_string();
    let CloseRedemptionRequest { reason, acknowledged_unresolved_burn_tx_hash } =
        body.into_inner();

    store
        .send(
            &issuer_request_id,
            RedemptionCommand::CloseRedemption {
                issuer_request_id: issuer_request_id.clone(),
                reason,
                acknowledged_unresolved_burn_tx_hash,
            },
        )
        .await
        .map_err(|err| {
            error!(target: "admin", aggregate_id = %aggregate_id,
                acknowledged_unresolved_burn_tx_hash = ?acknowledged_unresolved_burn_tx_hash,
                error = %err,
                "Failed to close redemption"
            );
            map_redemption_error(&err)
        })?;

    let previous_state =
        redemption_state_before_last_event(pool.inner(), &aggregate_id).await?;

    info!(target: "admin", aggregate_id = %aggregate_id,
        previous_state = %previous_state,
        acknowledged_unresolved_burn_tx_hash = ?acknowledged_unresolved_burn_tx_hash,
        "Redemption closed"
    );

    let message = acknowledged_unresolved_burn_tx_hash.map_or_else(
        || "Redemption closed by admin".to_string(),
        |acknowledged_hash| {
            format!(
                "Redemption closed by admin after acknowledging unresolved burn {acknowledged_hash:#x}"
            )
        },
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id,
        previous_state,
        message,
    }))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(crate) struct ForceCompleteRedemptionRequest {
    /// On-chain transaction hash that burned the redemption's shares. Verified
    /// against the chain before the redemption is terminalized.
    #[schema(value_type = String)]
    burn_tx_hash: B256,
    /// Operator-supplied audit reason recorded with the terminal event.
    reason: String,
    #[schema(value_type = Option<String>)]
    acknowledged_unresolved_burn_tx_hash: Option<B256>,
}

/// Admin endpoint to terminalize a redemption stuck in `Burning`/`BurnSubmitted`
/// whose burn already landed on-chain but was never recorded.
///
/// The supplied `burn_tx_hash` is verified on-chain (the receipt must have
/// succeeded and contain a real `Transfer(bot_wallet -> 0x0)` of the vault's
/// shares) before the redemption is moved to `Completed`, recording the proving
/// tx hash for audit. Ambiguous cases with no verifiable on-chain burn are
/// rejected (`422`) — use `/admin/close/redemption` for those.
#[utoipa::path(
    post,
    path = "/admin/force-complete/redemption/{issuer_request_id}",
    tag = "admin",
    params(
        ("issuer_request_id" = String, Path,
            description = "Issuer redemption request id to force-complete")
    ),
    request_body = ForceCompleteRedemptionRequest,
    responses(
        (status = 200, description = "Burn verified on-chain; redemption completed",
            body = ReprocessResponse),
        (status = 422,
            description = "Supplied hash is not a verifiable burn, or invalid \
                aggregate state"),
        (status = 502, description = "On-chain/RPC verification failure"),
        (status = 500, description = "Event load/deserialize or internal failure")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, pool, burn_recovery))]
#[post(
    "/admin/force-complete/redemption/<issuer_request_id>",
    format = "json",
    data = "<body>"
)]
pub(crate) async fn force_complete_redemption(
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
    burn_recovery: &rocket::State<Arc<dyn RedemptionBurnRecovery>>,
    issuer_request_id: IssuerRedemptionRequestId,
    body: Json<ForceCompleteRedemptionRequest>,
) -> Result<Json<ReprocessResponse>, Status> {
    let aggregate_id = issuer_request_id.to_string();
    let ForceCompleteRedemptionRequest {
        burn_tx_hash,
        reason,
        acknowledged_unresolved_burn_tx_hash,
    } = body.into_inner();

    let verification = burn_recovery
        .force_complete_burn(
            &issuer_request_id,
            burn_tx_hash,
            reason,
            acknowledged_unresolved_burn_tx_hash,
        )
        .await
        .map_err(|err| {
            error!(target: "admin", aggregate_id = %aggregate_id,
                burn_tx_hash = ?burn_tx_hash,
                acknowledged_unresolved_burn_tx_hash = ?acknowledged_unresolved_burn_tx_hash,
                error = %err,
                "Failed to force-complete redemption"
            );
            map_burn_manager_error(&err)
        })?;

    let previous_state =
        redemption_state_before_last_event(pool.inner(), &aggregate_id).await?;

    info!(target: "admin", aggregate_id = %aggregate_id,
        burn_tx_hash = ?burn_tx_hash,
        acknowledged_unresolved_burn_tx_hash = ?acknowledged_unresolved_burn_tx_hash,
        block_number = verification.block_number,
        previous_state = %previous_state,
        "Redemption force-completed"
    );

    Ok(Json(ReprocessResponse {
        aggregate_type: AggregateKind::Redemption,
        aggregate_id,
        previous_state,
        message: acknowledged_unresolved_burn_tx_hash.map_or_else(
            || {
                format!(
                    "Force-completed: burn verified on-chain at block {} ({} shares)",
                    verification.block_number, verification.shares_burned
                )
            },
            |acknowledged_hash| format!(
                "Force-completed: burn {burn_tx_hash:#x} verified on-chain at block {} ({} shares) after acknowledging unresolved burn {acknowledged_hash:#x}",
                verification.block_number, verification.shares_burned
            ),
        ),
    }))
}

/// Reconstructs a redemption's state immediately *before* its most recent event
/// — the state the just-applied terminal command transitioned from — for admin
/// response/audit reporting.
///
/// Read *after* the terminal command commits, not as a preflight load, so it
/// cannot report a state the command never observed: background recovery and the
/// transfer poller can advance the aggregate between a preflight read and the
/// command. Both terminal admin commands (`CloseRedemption`, `ForceCompleteBurn`)
/// append exactly one event, so replaying every event except the last yields the
/// pre-transition state. An aggregate with no prior events resolves to
/// `Uninitialized`.
async fn redemption_state_before_last_event(
    pool: &Pool<Sqlite>,
    aggregate_id: &str,
) -> Result<String, Status> {
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
        ORDER BY sequence
        "#,
        aggregate_id
    )
    .fetch_all(pool)
    .await
    .map_err(|err| {
        error!(target: "admin", aggregate_id = %aggregate_id,
            error = %err,
            "Failed to load redemption events"
        );
        Status::InternalServerError
    })?;

    let pre_terminal = rows.len().saturating_sub(1);
    let mut prior: Option<Redemption> = None;
    for row in rows.into_iter().take(pre_terminal) {
        let event: RedemptionEvent = serde_json::from_str(&row.payload)
            .map_err(|err| {
                error!(target: "admin", aggregate_id = %aggregate_id,
                    error = %err,
                    "Failed to deserialize redemption event"
                );
                Status::InternalServerError
            })?;
        prior = match prior {
            None => Redemption::originate(&event),
            Some(state) => {
                Redemption::evolve(&state, &event).map_err(|err| {
                    error!(target: "admin", aggregate_id = %aggregate_id,
                        error = %err,
                        "Failed to evolve redemption state"
                    );
                    Status::InternalServerError
                })?
            }
        };
    }

    Ok(prior.map_or_else(
        || "Uninitialized".to_string(),
        |state| state.state_name().to_string(),
    ))
}

/// Maps a burn-manager failure to an HTTP status for the force-complete
/// endpoint. A bad operator-supplied hash or wrong aggregate state is a client
/// error (`422`); an on-chain/RPC failure is `502`; anything else is `500`.
const fn map_burn_manager_error(err: &BurnManagerError) -> Status {
    match err {
        BurnManagerError::Vault(
            VaultError::InvalidReceipt
            | VaultError::NotABurn { .. }
            | VaultError::Reverted { .. },
        )
        | BurnManagerError::InvalidAggregateState { .. }
        | BurnManagerError::Redemption(_)
        | BurnManagerError::AlternateBurnSemanticsMismatch { .. }
        | BurnManagerError::Cqrs(AggregateError::UserError(_)) => {
            Status::UnprocessableEntity
        }
        BurnManagerError::Vault(_) => Status::BadGateway,
        _ => Status::InternalServerError,
    }
}

#[utoipa::path(
    post,
    path = "/admin/reprocess/mint/{aggregate_id}",
    tag = "admin",
    params(
        ("aggregate_id" = String, Path,
            description = "Issuer mint request id (UUID) to reprocess")
    ),
    responses(
        (status = 200, description = "Recovery initiated", body = ReprocessResponse),
        (status = 400, description = "Aggregate id is not a valid UUID"),
        (status = 404, description = "No mint found for this id"),
        (status = 409, description = "Mint already completed or closed"),
        (status = 422, description = "Invalid state transition for recovery"),
        (status = 500, description = "View query or internal failure")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, store, vault_service, pool))]
#[post("/admin/reprocess/mint/<aggregate_id>")]
pub(crate) async fn reprocess_mint(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<Mint>>>,
    vault_service: &rocket::State<Arc<dyn VaultService>>,
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
                MintView::MintIntended { .. } => "MintIntended",
                MintView::MintTxSubmitted { .. } => "MintTxSubmitted",
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

    let outcome = recover_mint_manually(
        store.inner().as_ref(),
        vault_service.inner(),
        issuer_request_id,
    )
    .await;
    if !matches!(outcome, DriveOutcome::Done) {
        error!(target: "admin", aggregate_id = aggregate_id,
            ?outcome,
            "Failed to drive manual mint recovery to completion"
        );
        return Err(Status::UnprocessableEntity);
    }

    info!(target: "admin", aggregate_id = aggregate_id,
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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(crate) struct CloseMintRequest {
    reason: String,
}

/// Admin endpoint to close a mint that cannot be automatically recovered.
///
/// Valid from any non-terminal state. Closed mints do not appear in stuck queries.
#[utoipa::path(
    post,
    path = "/admin/close/mint/{aggregate_id}",
    tag = "admin",
    params(
        ("aggregate_id" = String, Path,
            description = "Issuer mint request id (UUID) to close")
    ),
    request_body = CloseMintRequest,
    responses(
        (status = 200, description = "Mint closed by admin", body = ReprocessResponse),
        (status = 400, description = "Aggregate id is not a valid UUID"),
        (status = 422, description = "Invalid state transition for close"),
        (status = 500, description = "Internal failure")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, store))]
#[post("/admin/close/mint/<aggregate_id>", format = "json", data = "<body>")]
pub(crate) async fn close_mint(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<Mint>>>,
    aggregate_id: &str,
    body: Json<CloseMintRequest>,
) -> Result<Json<ReprocessResponse>, Status> {
    let issuer_request_id: IssuerMintRequestId = aggregate_id
        .parse::<uuid::Uuid>()
        .map(IssuerMintRequestId::new)
        .map_err(|_| Status::BadRequest)?;

    store
        .send(
            &issuer_request_id,
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

/// In-progress states that haven't transitioned in this long are reported as
/// stuck. Most state transitions take seconds, so anything older than this
/// either deadlocked or was silently skipped by recovery (e.g.
/// `RecoveryOutcome::SkippedManualIntervention` leaves a redemption in
/// `Burning` indefinitely with no terminal event).
const STUCK_THRESHOLD: chrono::Duration = chrono::Duration::hours(1);

#[utoipa::path(
    get,
    path = "/admin/stuck",
    tag = "admin",
    responses(
        (status = 200, description = "Stuck mints and redemptions", body = StuckResponse),
        (status = 500, description = "Failed to query stuck aggregates")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, pool, _vault_service))]
#[get("/admin/stuck")]
pub(crate) async fn list_stuck(
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
    _vault_service: &rocket::State<Arc<dyn VaultService>>,
) -> Result<Json<StuckResponse>, Status> {
    let now = Utc::now();
    let mut stuck = Vec::new();

    let stuck_redemptions =
        find_stuck_redemptions(pool.inner()).await.map_err(|err| {
            error!(target: "admin", error = %err, "Failed to query stuck redemptions");
            Status::InternalServerError
        })?;

    for (issuer_redemption_request_id, view) in stuck_redemptions {
        let Some((class, timestamp)) = redemption_stuck_info(&view) else {
            continue;
        };
        if !is_stuck(class, timestamp, now) {
            continue;
        }

        let history = redemption_history_summary(
            pool.inner(),
            &issuer_redemption_request_id,
        )
        .await;

        if let Some(entry) =
            stuck_redemption_entry(&issuer_redemption_request_id, view, history)
        {
            stuck.push(entry);
        }
    }

    let stuck_mints = find_stuck_mints(pool.inner()).await.map_err(|err| {
        error!(target: "admin", error = %err, "Failed to query stuck mints");
        Status::InternalServerError
    })?;

    for (issuer_mint_request_id, view) in stuck_mints {
        let Some(summary) = mint_view_summary(&view) else {
            continue;
        };
        if !is_stuck(summary.class, summary.timestamp, now) {
            continue;
        }

        let (underlying, quantity) = mint_view_asset(&view);
        let mint_history =
            mint_history_summary(pool.inner(), &issuer_mint_request_id).await;

        stuck.push(StuckAggregate {
            aggregate_type: AggregateKind::Mint,
            aggregate_id: issuer_mint_request_id.to_string(),
            tokenization_request_id: summary.tokenization_request_id,
            state: summary.state,
            detail: summary.detail,
            timestamp: summary.timestamp,
            underlying,
            quantity,
            tx_hash: mint_history.tx_hash,
            tx_id: mint_history.tx_id.map(|tx_id| tx_id.to_string()),
        });
    }

    Ok(Json(StuckResponse { stuck }))
}

/// Classification of a non-terminal view used to decide whether it counts as
/// stuck right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StuckClass {
    /// Aggregate is mid-flow. Only counts as stuck once it sits in this state
    /// longer than [`STUCK_THRESHOLD`].
    InProgress,
    /// Aggregate landed in a terminal-failure state. Always counts as stuck
    /// regardless of age — these never self-resolve.
    TerminalFail,
}

fn is_stuck(
    class: StuckClass,
    timestamp: DateTime<Utc>,
    now: DateTime<Utc>,
) -> bool {
    match class {
        StuckClass::TerminalFail => true,
        StuckClass::InProgress => {
            now.signed_duration_since(timestamp) >= STUCK_THRESHOLD
        }
    }
}

/// Returns the stuck-classification and state-entered timestamp for a view
/// the operator may need to act on, or `None` for terminal/Unavailable
/// variants that never appear in `/admin/stuck`. Fusing the classification
/// and timestamp into a single `Option` keeps the type system enforcing
/// that callers never observe a timestamp without a class, eliminating the
/// possibility of a sentinel value on a financial admin path.
const fn redemption_stuck_info(
    view: &RedemptionView,
) -> Option<(StuckClass, DateTime<Utc>)> {
    use StuckClass::{InProgress, TerminalFail};
    match view {
        RedemptionView::Detected { detected_entered_at, .. } => {
            Some((InProgress, *detected_entered_at))
        }
        RedemptionView::AlpacaCalled { called_at, .. } => {
            Some((InProgress, *called_at))
        }
        RedemptionView::Burning { burning_entered_at, .. } => {
            Some((InProgress, *burning_entered_at))
        }
        RedemptionView::Failed { failed_at, .. }
        | RedemptionView::BurnFailed { failed_at, .. } => {
            Some((TerminalFail, *failed_at))
        }
        RedemptionView::Unavailable
        | RedemptionView::Completed { .. }
        | RedemptionView::Closed { .. } => None,
    }
}

fn stuck_redemption_entry(
    issuer_redemption_request_id: &IssuerRedemptionRequestId,
    view: RedemptionView,
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
        tx_id,
    ) = match view {
        RedemptionView::Detected {
            underlying,
            quantity,
            tx_hash,
            detected_entered_at,
            ..
        } => (
            None,
            "Detected".to_string(),
            "Waiting to call Alpaca".to_string(),
            detected_entered_at,
            Some(underlying),
            Some(quantity),
            Some(tx_hash),
            history.tx_id,
        ),
        RedemptionView::AlpacaCalled {
            tokenization_request_id,
            underlying,
            quantity,
            tx_hash,
            called_at,
            ..
        } => (
            Some(tokenization_request_id),
            "AlpacaCalled".to_string(),
            "Waiting for Alpaca journal".to_string(),
            called_at,
            Some(underlying),
            Some(quantity),
            Some(tx_hash),
            history.tx_id,
        ),
        RedemptionView::Burning {
            tokenization_request_id,
            underlying,
            quantity,
            tx_hash,
            burning_entered_at,
            ..
        } => {
            // BurnTxSubmitted intentionally leaves the view in
            // Burning. The detail string is what operators read first, so
            // distinguish pre- vs post-submission by whether a tx id has
            // been recorded in event history.
            let detail = if history.tx_id.is_some() {
                "Waiting for burn confirmation".to_string()
            } else {
                "Waiting for burn submission".to_string()
            };
            (
                Some(tokenization_request_id),
                "Burning".to_string(),
                detail,
                burning_entered_at,
                Some(underlying),
                Some(quantity),
                Some(tx_hash),
                history.tx_id,
            )
        }
        RedemptionView::Failed { reason, failed_at, .. } => (
            history.tokenization_request_id,
            "Failed".to_string(),
            reason,
            failed_at,
            history.underlying,
            history.quantity,
            history.tx_hash,
            history.tx_id,
        ),
        RedemptionView::BurnFailed {
            tokenization_request_id,
            underlying,
            quantity,
            tx_hash,
            error,
            failed_at,
            tx_id,
            ..
        } => (
            Some(tokenization_request_id),
            "BurnFailed".to_string(),
            error,
            failed_at,
            Some(underlying),
            Some(quantity),
            Some(tx_hash),
            tx_id.or(history.tx_id),
        ),
        // Terminal/Unavailable variants never reach here — list_stuck gates
        // on redemption_stuck_info which returns None for them.
        RedemptionView::Unavailable
        | RedemptionView::Completed { .. }
        | RedemptionView::Closed { .. } => return None,
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
        tx_id: tx_id.map(|tx_id| tx_id.to_string()),
    })
}

#[derive(Debug, Default)]
struct RedemptionHistorySummary {
    tokenization_request_id: Option<TokenizationRequestId>,
    underlying: Option<UnderlyingSymbol>,
    quantity: Option<Quantity>,
    tx_hash: Option<B256>,
    tx_id: Option<TxId>,
}

/// Summarizes a redemption's history for the `/admin/stuck` metadata lookup.
///
/// Reads the raw `RedemptionEvent` payloads straight from the `events` table.
/// `event_sorcery::Store` exposes no event-log read, so the history is loaded
/// with a direct query rather than through the aggregate store.
async fn redemption_history_summary(
    pool: &Pool<Sqlite>,
    aggregate_id: &IssuerRedemptionRequestId,
) -> RedemptionHistorySummary {
    let aggregate_id_str = aggregate_id.to_string();
    let rows = match sqlx::query!(
        r#"
        SELECT payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
        ORDER BY sequence
        "#,
        aggregate_id_str
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            warn!(
                target: "admin",
                aggregate_id = %aggregate_id_str,
                error = %err,
                "Failed to load redemption events for stuck metadata lookup"
            );
            return RedemptionHistorySummary::default();
        }
    };

    let events = match rows
        .into_iter()
        .map(|row| serde_json::from_str::<RedemptionEvent>(&row.payload))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(events) => events,
        Err(err) => {
            warn!(
                target: "admin",
                aggregate_id = %aggregate_id_str,
                error = %err,
                "Failed to deserialize redemption events for stuck metadata lookup"
            );
            return RedemptionHistorySummary::default();
        }
    };

    redemption_history_summary_from_events(events)
}

/// Pure reduce: builds a `RedemptionHistorySummary` from an ordered sequence
/// of `RedemptionEvent`s. Split out from `redemption_history_summary` so the
/// branching (especially the Reprocess/Resume tx-id reset) is unit-testable
/// without an event store.
fn redemption_history_summary_from_events(
    events: impl IntoIterator<Item = RedemptionEvent>,
) -> RedemptionHistorySummary {
    let mut summary = RedemptionHistorySummary::default();
    for event in events {
        match event {
            RedemptionEvent::Detected {
                underlying, quantity, tx_hash, ..
            } => {
                summary.underlying = Some(underlying);
                summary.quantity = Some(quantity);
                summary.tx_hash = Some(tx_hash);
            }
            RedemptionEvent::Reprocessed {
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
                // Reprocess/Resume starts a fresh attempt — any prior
                // tx submission belongs to the previous attempt
                // and must not bleed into the current Burning row's
                // operator-facing detail. A subsequent
                // `BurnTxSubmitted` re-sets the field.
                summary.tx_id = None;
            }
            RedemptionEvent::AlpacaCalled {
                tokenization_request_id, ..
            } => {
                summary.tokenization_request_id = Some(tokenization_request_id);
            }
            RedemptionEvent::BurnTxSubmitted { tx_id, .. }
            | RedemptionEvent::ExistingBurnRecovered { tx_id, .. }
            | RedemptionEvent::BurningFailed { tx_id: Some(tx_id), .. } => {
                summary.tx_id = Some(tx_id);
            }
            RedemptionEvent::BurnIntended { sendable_tx, .. }
                if !sendable_tx.tx.is_empty() =>
            {
                summary.tx_id = Some(TxId::Hash(sendable_tx.hash));
            }
            _ => {}
        }
    }

    summary
}

/// Projection of a non-terminal `MintView` used to populate a `StuckAggregate`.
/// Two adjacent `String` slots (`state`, `detail`) would be position-swappable
/// in a tuple; the named struct prevents that.
#[derive(Debug)]
struct MintStuckSummary {
    class: StuckClass,
    tokenization_request_id: Option<TokenizationRequestId>,
    state: String,
    detail: String,
    timestamp: DateTime<Utc>,
}

fn mint_view_summary(view: &MintView) -> Option<MintStuckSummary> {
    use StuckClass::{InProgress, TerminalFail};
    match view {
        MintView::Initiated {
            tokenization_request_id, initiated_at, ..
        } => Some(MintStuckSummary {
            class: InProgress,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "Initiated".to_string(),
            detail: "Waiting for journal confirmation".to_string(),
            timestamp: *initiated_at,
        }),
        MintView::JournalConfirmed {
            tokenization_request_id,
            journal_confirmed_at,
            ..
        } => Some(MintStuckSummary {
            class: InProgress,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "JournalConfirmed".to_string(),
            detail: "Waiting for deposit".to_string(),
            timestamp: *journal_confirmed_at,
        }),
        MintView::JournalRejected {
            tokenization_request_id,
            reason,
            rejected_at,
            ..
        } => Some(MintStuckSummary {
            class: TerminalFail,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "JournalRejected".to_string(),
            detail: reason.clone(),
            timestamp: *rejected_at,
        }),
        MintView::Minting {
            tokenization_request_id,
            minting_started_at,
            ..
        } => Some(MintStuckSummary {
            class: InProgress,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "Minting".to_string(),
            detail: "Deposit in progress".to_string(),
            timestamp: *minting_started_at,
        }),
        MintView::MintIntended {
            tokenization_request_id,
            minting_started_at,
            ..
        } => Some(MintStuckSummary {
            class: InProgress,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "MintIntended".to_string(),
            detail: "Signed transaction awaiting broadcast".to_string(),
            timestamp: *minting_started_at,
        }),
        MintView::MintTxSubmitted {
            tokenization_request_id,
            minting_started_at,
            ..
        } => Some(MintStuckSummary {
            class: InProgress,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "MintTxSubmitted".to_string(),
            detail: "Awaiting on-chain confirmation".to_string(),
            timestamp: *minting_started_at,
        }),
        MintView::MintingFailed {
            tokenization_request_id,
            error,
            failed_at,
            ..
        } => Some(MintStuckSummary {
            class: TerminalFail,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "MintingFailed".to_string(),
            detail: error.clone(),
            timestamp: *failed_at,
        }),
        MintView::CallbackPending {
            tokenization_request_id,
            minted_at,
            ..
        } => Some(MintStuckSummary {
            class: InProgress,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            state: "CallbackPending".to_string(),
            detail: "Waiting for callback".to_string(),
            timestamp: *minted_at,
        }),
        MintView::NotFound
        | MintView::Completed { .. }
        | MintView::Closed { .. } => None,
    }
}

fn mint_view_asset(
    view: &MintView,
) -> (Option<UnderlyingSymbol>, Option<Quantity>) {
    match view {
        MintView::Initiated { underlying, quantity, .. }
        | MintView::JournalConfirmed { underlying, quantity, .. }
        | MintView::JournalRejected { underlying, quantity, .. }
        | MintView::Minting { underlying, quantity, .. }
        | MintView::MintIntended { underlying, quantity, .. }
        | MintView::MintTxSubmitted { underlying, quantity, .. }
        | MintView::MintingFailed { underlying, quantity, .. }
        | MintView::CallbackPending { underlying, quantity, .. } => {
            (Some(underlying.clone()), Some(quantity.clone()))
        }
        MintView::NotFound
        | MintView::Completed { .. }
        | MintView::Closed { .. } => (None, None),
    }
}

#[derive(Debug, Default)]
struct MintHistorySummary {
    tx_hash: Option<B256>,
    tx_id: Option<TxId>,
}

/// Returns the latest useful transaction hints from this mint's history.
///
/// Reads the raw `MintEvent` payloads straight from the `events` table.
/// `event_sorcery::Store` exposes no event-log read, so the history is loaded
/// with a direct query rather than through the aggregate store.
async fn mint_history_summary(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
) -> MintHistorySummary {
    let aggregate_id = issuer_request_id.to_string();
    let rows = match sqlx::query!(
        r#"
        SELECT payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Mint' AND aggregate_id = ?
        ORDER BY sequence
        "#,
        aggregate_id
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            warn!(
                target: "admin",
                aggregate_id = %aggregate_id,
                error = %err,
                "Failed to load mint events for tx_id lookup"
            );
            return MintHistorySummary::default();
        }
    };

    let events = match rows
        .into_iter()
        .map(|row| serde_json::from_str::<MintEvent>(&row.payload))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(events) => events,
        Err(err) => {
            warn!(
                target: "admin",
                aggregate_id = %aggregate_id,
                error = %err,
                "Failed to deserialize mint events for tx_id lookup"
            );
            return MintHistorySummary::default();
        }
    };

    mint_history_summary_from_events(events)
}

/// Pure reduce: builds a [`MintHistorySummary`] from an ordered sequence of
/// `MintEvent`s. Split out from `mint_history_summary` so the fold is
/// unit-testable without an event store or database.
fn mint_history_summary_from_events(
    events: impl IntoIterator<Item = MintEvent>,
) -> MintHistorySummary {
    let mut summary = MintHistorySummary::default();
    for event in events {
        match event {
            MintEvent::TokensMinted { tx_hash, .. }
            | MintEvent::ExistingMintRecovered { tx_hash, .. }
            | MintEvent::MintRetryStarted { tx_hash: Some(tx_hash), .. } => {
                summary.tx_hash = Some(tx_hash);
            }
            MintEvent::MintTxSubmitted { tx_id, .. } => {
                summary.tx_id = Some(tx_id);
            }
            _ => {}
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{
        Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
    };
    use alloy::primitives::{Address, B256, Bloom, Bytes, U256, address, b256};
    use alloy::rpc::types::TransactionReceipt;
    use async_trait::async_trait;
    use chrono::Utc;
    use event_sorcery::{Store, StoreBuilder, test_store};
    use rocket::http::Status;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing::Level;
    use tracing_test::traced_test;
    use url::Url;

    use super::{
        AggregateKind, MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS, StuckAggregate,
    };
    use super::{
        AlpacaCalledData, PostAlpacaRecoveryInput, load_reprocess_context,
        recover_post_alpaca,
    };
    use crate::admin::BurningFailedData;
    use crate::alpaca::{
        AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
        RedeemRequestStatus, RedeemResponse, TokenizationRequest,
        service::AlpacaConfig,
    };
    use crate::auth::{FailedAuthRateLimiter, test_auth_config};
    use crate::config::{Config, Environment, LogLevel};
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptService, ReceiptSource, Shares,
    };
    use crate::redemption::BurnExternalTxId;
    use crate::redemption::{
        BurnRecord, BurnRecoveryAction, IssuerRedemptionRequestId, Redemption,
        RedemptionCommand, RedemptionEvent, RedemptionMetadata, RedemptionView,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        MultiBurnEntry, SendableTxWithHash, TxId, VaultService, VerifiedBurn,
    };
    use crate::wallet::SignerConfig;

    fn mock_vault_service() -> Arc<dyn VaultService> {
        Arc::new(MockVaultService::new_success())
    }

    fn checked_burn_receipt(
        transaction_hash: B256,
        block_number: Option<u64>,
        succeeded: bool,
    ) -> TransactionReceipt {
        let consensus_receipt = Receipt {
            status: Eip658Value::Eip658(succeeded),
            cumulative_gas_used: 21_000,
            logs: Vec::new(),
        };

        TransactionReceipt {
            transaction_hash,
            transaction_index: Some(0),
            block_hash: None,
            block_number,
            from: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            to: Some(address!("0xcccccccccccccccccccccccccccccccccccccccc")),
            gas_used: 21_000,
            effective_gas_price: 1,
            contract_address: None,
            blob_gas_used: None,
            blob_gas_price: None,
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                consensus_receipt,
                Bloom::default(),
            )),
        }
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

    /// Configurable result for `MockBurnRecovery`. `Fails` exercises the
    /// endpoint's error path; the concrete `BurnManagerError` is irrelevant
    /// since every error maps to `502`.
    #[derive(Clone, Copy)]
    enum MockBurnResult {
        Succeeds(super::RecoveryOutcome),
        Fails,
    }

    /// Configurable result for `MockBurnRecovery::force_complete_burn`.
    #[derive(Clone, Copy)]
    enum MockForceResult {
        /// On-chain verification succeeded; report this block number.
        Verified,
        /// The operator-supplied hash is not a verifiable burn.
        NotABurn,
    }

    struct MockBurnRecovery {
        calls: AtomicUsize,
        result: MockBurnResult,
        force_calls: AtomicUsize,
        force_result: MockForceResult,
    }

    impl Default for MockBurnRecovery {
        fn default() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                result: MockBurnResult::Succeeds(
                    super::RecoveryOutcome::Executed,
                ),
                force_calls: AtomicUsize::new(0),
                force_result: MockForceResult::Verified,
            }
        }
    }

    impl MockBurnRecovery {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn force_calls(&self) -> usize {
            self.force_calls.load(Ordering::Relaxed)
        }
    }

    struct ReservationTrackingBurnRecovery {
        calls: AtomicUsize,
        receipt_inventory_store: Arc<Store<ReceiptInventory>>,
        vault: Address,
    }

    impl ReservationTrackingBurnRecovery {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl super::RedemptionBurnRecovery for ReservationTrackingBurnRecovery {
        async fn execute_recovered_burn(
            &self,
            issuer_request_id: &IssuerRedemptionRequestId,
        ) -> Result<super::RecoveryOutcome, super::BurnManagerError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.receipt_inventory_store
                .send(
                    &self.vault,
                    ReceiptInventoryCommand::ReleaseBurn {
                        redemption_issuer_request_id: issuer_request_id.clone(),
                    },
                )
                .await
                .expect("test recovery should release the reservation");
            Ok(super::RecoveryOutcome::Executed)
        }

        async fn force_complete_burn(
            &self,
            _issuer_request_id: &IssuerRedemptionRequestId,
            _burn_tx_hash: alloy::primitives::B256,
            _reason: String,
            _acknowledged_unresolved_burn_tx_hash: Option<
                alloy::primitives::B256,
            >,
        ) -> Result<super::BurnVerification, super::BurnManagerError> {
            unimplemented!("not used by redemption recovery route tests")
        }
    }

    #[async_trait]
    impl super::RedemptionBurnRecovery for MockBurnRecovery {
        async fn execute_recovered_burn(
            &self,
            _issuer_request_id: &IssuerRedemptionRequestId,
        ) -> Result<super::RecoveryOutcome, super::BurnManagerError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.result {
                MockBurnResult::Succeeds(outcome) => Ok(outcome),
                MockBurnResult::Fails => {
                    Err(super::BurnManagerError::SharesOverflow)
                }
            }
        }

        async fn force_complete_burn(
            &self,
            _issuer_request_id: &IssuerRedemptionRequestId,
            burn_tx_hash: alloy::primitives::B256,
            _reason: String,
            _acknowledged_unresolved_burn_tx_hash: Option<
                alloy::primitives::B256,
            >,
        ) -> Result<super::BurnVerification, super::BurnManagerError> {
            self.force_calls.fetch_add(1, Ordering::Relaxed);
            match self.force_result {
                MockForceResult::Verified => Ok(super::BurnVerification {
                    block_number: 45_989_009,
                    nonce: 0,
                    shares_burned: alloy::primitives::U256::from(17u64),
                    burns: vec![],
                    share_transfers: vec![],
                }),
                MockForceResult::NotABurn => {
                    Err(super::BurnManagerError::Vault(
                        super::VaultError::NotABurn { tx_hash: burn_tx_hash },
                    ))
                }
            }
        }
    }

    fn mock_burn_recovery() -> Arc<dyn super::RedemptionBurnRecovery> {
        Arc::new(MockBurnRecovery::default())
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
                    AlpacaError::RequestNotFound { id, body } => {
                        AlpacaError::RequestNotFound {
                            id: id.clone(),
                            body: body.clone(),
                        }
                    }
                    AlpacaError::ResponseIdMismatch { requested, returned } => {
                        AlpacaError::ResponseIdMismatch {
                            requested: requested.clone(),
                            returned: returned.clone(),
                        }
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
    fn stuck_aggregate_serializes_tx_id_as_documented_string() {
        let cases = [
            TxId::Hash(b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            )),
            TxId::Legacy("07bdef3c-5314-4d1d-94f7-f3f346cd4c2f".to_string()),
        ];

        for tx_id in cases {
            let expected = tx_id.to_string();
            let entry = StuckAggregate {
                aggregate_type: AggregateKind::Mint,
                aggregate_id: "mint-id".to_string(),
                tokenization_request_id: None,
                state: "minting_failed".to_string(),
                detail: "mint failed".to_string(),
                timestamp: Utc::now(),
                underlying: None,
                quantity: None,
                tx_hash: None,
                tx_id: Some(expected.clone()),
            };

            let response = serde_json::to_value(entry).unwrap();

            assert_eq!(response["tx_id"], expected);
        }
    }

    #[test]
    fn failed_redemption_stuck_entry_uses_history_metadata() {
        let metadata = test_metadata();
        let tokenization_request_id = TokenizationRequestId::new("tok-red-1");
        let tx_id = TxId::random();
        let failed_at = Utc::now();
        let view = RedemptionView::Failed {
            issuer_request_id: metadata.issuer_request_id.clone(),
            reason: "Transaction burn confirmation failed".to_string(),
            failed_at,
        };
        let history = super::RedemptionHistorySummary {
            tokenization_request_id: Some(tokenization_request_id.clone()),
            underlying: Some(metadata.underlying.clone()),
            quantity: Some(metadata.quantity.clone()),
            tx_hash: Some(metadata.detected_tx_hash),
            tx_id: Some(tx_id.clone()),
        };

        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            view,
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
        assert_eq!(entry.tx_id, Some(tx_id.to_string()));
        assert_eq!(entry.timestamp, failed_at);
    }

    #[test]
    fn burn_failed_stuck_entry_prefers_view_metadata() {
        let metadata = test_metadata();
        let tokenization_request_id = TokenizationRequestId::new("tok-red-2");
        let tx_id = TxId::random();
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
            tx_id: Some(tx_id.clone()),
            planned_burns: vec![],
        };

        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            view,
            super::RedemptionHistorySummary::default(),
        )
        .expect("burn failed redemption should produce stuck entry");

        assert_eq!(
            entry.tokenization_request_id,
            Some(tokenization_request_id)
        );
        assert_eq!(entry.tx_hash, Some(metadata.detected_tx_hash));
        assert_eq!(entry.tx_id, Some(tx_id.to_string()));
        assert_eq!(entry.underlying, Some(metadata.underlying));
        assert_eq!(entry.quantity, Some(metadata.quantity));
    }

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

    /// Builds an event-sorcery [`Store<Redemption>`] backed by the given pool,
    /// wired with a success vault mock. No reactors are registered, so the
    /// `redemption_view` projection stays empty — the admin paths under test
    /// read the `events` table directly, not the view.
    fn setup_store(pool: &sqlx::Pool<sqlx::Sqlite>) -> Arc<Store<Redemption>> {
        let vault_service: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success());
        setup_store_with_vault(pool, vault_service)
    }

    fn setup_store_with_vault(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        vault_service: Arc<dyn VaultService>,
    ) -> Arc<Store<Redemption>> {
        Arc::new(test_store::<Redemption>(pool.clone(), vault_service))
    }

    /// Sets up an in-memory redemption store with a redemption in Failed
    /// state (post-Alpaca, i.e. with AlpacaCalled event in history).
    async fn setup_failed_redemption() -> (
        Arc<Store<Redemption>>,
        sqlx::Pool<sqlx::Sqlite>,
        RedemptionMetadata,
        AlpacaCalledData,
    ) {
        let pool = setup_pool().await;
        let store = setup_store(&pool);

        let metadata = test_metadata();
        let alpaca_data = test_alpaca_data();

        // Drive aggregate to Failed state (post-Alpaca)
        store
            .send(
                &metadata.issuer_request_id,
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

        store
            .send(
                &metadata.issuer_request_id,
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

        store
            .send(
                &metadata.issuer_request_id,
                RedemptionCommand::MarkFailed {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    reason: "Journal timed out".to_string(),
                },
            )
            .await
            .expect("MarkFailed failed");

        (store, pool, metadata, alpaca_data)
    }

    #[tokio::test]
    async fn test_load_reprocess_context_derives_retry_id_from_history() {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let tx_id = TxId::random();

        // Drive the redemption through a real burn submission that then fails,
        // so event history carries a BurnTxSubmitted event the
        // derivation must scan — rather than injecting the retry id directly.
        let (metadata, _) = setup_burn_failure(&store, tx_id.clone()).await;

        let context =
            load_reprocess_context(&pool, &metadata.issuer_request_id)
                .await
                .expect("load_reprocess_context failed");

        // The base BurnTxSubmitted in history must advance the derived
        // id to retry-1, proving the derivation is wired into
        // load_reprocess_context rather than the retry id being injected.
        assert_eq!(
            context.burn_retry_external_tx_id,
            Some(Redemption::retry_burn_external_tx_id_typed(
                &metadata.detected_tx_hash,
                1
            ))
        );

        let burning_failed = context
            .burning_failed
            .expect("expected BurningFailed data in context");
        assert_eq!(burning_failed.tx_id, Some(tx_id));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_completed_succeeds() {
        let (store, _pool, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let burn_recovery = Arc::new(MockBurnRecovery::default());
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();

        let result = recover_post_alpaca(
            &store,
            &alpaca,
            &mock_vault_service(),
            &burn_recovery_state,
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata: metadata.clone(),
                alpaca_data,
                burning_failed: None,
                burn_retry_external_tx_id: None,
            },
        )
        .await;

        let response = result.expect("Expected Ok response");
        assert_eq!(response.previous_state, "Failed");
        assert!(response.message.contains("Recovered from Failed"));
        assert!(response.message.contains("executed burn"));
        assert_eq!(burn_recovery.calls(), 1);
        assert!(logs_contain_at!(Level::INFO, &["recovered", "Burning"]));
        assert!(logs_contain_at!(Level::INFO, &["burn", "executed"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_persists_retry_external_tx_id() {
        let (store, _pool, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let retry_external_tx_id =
            BurnExternalTxId::from_string("burn-0xabc-retry-1".to_string());

        let result = recover_post_alpaca(
            &store,
            &alpaca,
            &mock_vault_service(),
            &mock_burn_recovery(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata: metadata.clone(),
                alpaca_data,
                burning_failed: None,
                burn_retry_external_tx_id: Some(retry_external_tx_id.clone()),
            },
        )
        .await;

        result.expect("Expected Ok response");

        let redemption = store
            .load(&metadata.issuer_request_id)
            .await
            .expect("Expected aggregate to load")
            .expect("Expected redemption to be initialized");

        let Redemption::Burning { external_tx_id, .. } = &redemption else {
            panic!("Expected Burning state, got {redemption:?}");
        };

        assert_eq!(external_tx_id, &Some(retry_external_tx_id));

        assert!(logs_contain_at!(Level::INFO, &["recovered", "Burning"]));
        assert!(logs_contain_at!(Level::INFO, &["burn", "executed"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_uses_retry_id_fallback_on_terminal_failure()
     {
        let (store, _pool, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let prior_tx_id = TxId::random();
        let vault: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success().with_checked_tx_receipt(
                checked_burn_receipt(
                    prior_tx_id.to_hash().unwrap(),
                    Some(12_345),
                    false,
                ),
            ));

        let result = recover_post_alpaca(
            &store,
            &alpaca,
            &vault,
            &mock_burn_recovery(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata: metadata.clone(),
                alpaca_data,
                burning_failed: Some(BurningFailedData {
                    tx_id: Some(prior_tx_id),
                    planned_burns: vec![],
                }),
                burn_retry_external_tx_id: None,
            },
        )
        .await;

        result.expect("Expected Ok response");

        let redemption = store
            .load(&metadata.issuer_request_id)
            .await
            .expect("Expected aggregate to load")
            .expect("Expected redemption to be initialized");

        let Redemption::Burning { external_tx_id, .. } = &redemption else {
            panic!("Expected Burning state, got {redemption:?}");
        };

        // The terminally failed transaction permanently blocks the base
        // externalTxId, so recovery must fall back to retry-1 rather than
        // reuse it.
        assert_eq!(
            external_tx_id,
            &Some(Redemption::retry_burn_external_tx_id_typed(
                &metadata.detected_tx_hash,
                1
            ))
        );

        assert!(logs_contain_at!(
            Level::INFO,
            &["Transaction reverted onchain", "ResumeBurn"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_pending_returns_422() {
        let (store, _pool, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Pending,
                &metadata,
                &alpaca_data,
            )),
        });

        let result = recover_post_alpaca(
            &store,
            &alpaca,
            &mock_vault_service(),
            &mock_burn_recovery(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
                burn_retry_external_tx_id: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::UnprocessableEntity);
        assert!(logs_contain_at!(Level::INFO, &["journal still pending"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_rejected_returns_422() {
        let (store, _pool, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Rejected,
                &metadata,
                &alpaca_data,
            )),
        });

        let result = recover_post_alpaca(
            &store,
            &alpaca,
            &mock_vault_service(),
            &mock_burn_recovery(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
                burn_retry_external_tx_id: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::UnprocessableEntity);
        assert!(logs_contain_at!(Level::INFO, &["journal was rejected"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_api_error_returns_502() {
        let (store, _pool, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Error(AlpacaError::Api {
                status_code: 500,
                body: "Internal Server Error".to_string(),
            }),
        });

        let result = recover_post_alpaca(
            &store,
            &alpaca,
            &mock_vault_service(),
            &mock_burn_recovery(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
                burn_retry_external_tx_id: None,
            },
        )
        .await;

        assert_eq!(result.unwrap_err(), Status::BadGateway);
        assert!(logs_contain_at!(Level::ERROR, &["Failed to poll Alpaca"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_post_alpaca_mint_type_returns_500() {
        let (store, _pool, metadata, alpaca_data) =
            setup_failed_redemption().await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(TokenizationRequest::Mint {}),
        });

        let result = recover_post_alpaca(
            &store,
            &alpaca,
            &mock_vault_service(),
            &mock_burn_recovery(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
                burn_retry_external_tx_id: None,
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
        let (store, _pool, metadata, alpaca_data) =
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
            &store,
            &alpaca,
            &mock_vault_service(),
            &mock_burn_recovery(),
            PostAlpacaRecoveryInput {
                aggregate_id: metadata.issuer_request_id.to_string(),
                issuer_request_id: metadata.issuer_request_id.clone(),
                metadata,
                alpaca_data,
                burning_failed: None,
                burn_retry_external_tx_id: None,
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

        let result =
            load_reprocess_context(&pool, &IssuerRedemptionRequestId::random())
                .await;

        assert_eq!(result.err(), Some(Status::NotFound));
    }

    /// Build a minimal Rocket instance with the recovery endpoint and all
    /// required managed state for endpoint-level tests.
    async fn test_rocket(
        alpaca: Arc<dyn AlpacaService>,
    ) -> (
        rocket::Rocket<rocket::Build>,
        Arc<Store<Redemption>>,
        sqlx::Pool<sqlx::Sqlite>,
    ) {
        use crate::alpaca::service::AlpacaConfig;
        use crate::auth::{FailedAuthRateLimiter, test_auth_config};
        use crate::config::{Config, Environment, LogLevel};
        use crate::wallet::SignerConfig;
        use alloy::primitives::B256;
        use url::Url;

        let config = Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").unwrap(),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            environment: Environment::Development,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
            subgraph_url: Url::parse("http://localhost:0/subgraph").unwrap(),
        };

        let pool = setup_pool().await;
        let store = setup_store(&pool);

        let rocket = rocket::build()
            .manage(config)
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store.clone())
            .manage(pool.clone())
            .manage(alpaca)
            .manage(mock_vault_service())
            .manage(mock_burn_recovery())
            .mount("/", rocket::routes![super::recover_redemption]);

        (rocket, store, pool)
    }

    /// Drives a redemption to Failed state with only a Detected event
    /// (pre-Alpaca failure path).
    async fn setup_pre_alpaca_failure(
        store: &Store<Redemption>,
    ) -> RedemptionMetadata {
        let metadata = test_metadata();

        store
            .send(
                &metadata.issuer_request_id,
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

        store
            .send(
                &metadata.issuer_request_id,
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
        store: &Store<Redemption>,
    ) -> (RedemptionMetadata, AlpacaCalledData) {
        let metadata = test_metadata();
        let alpaca_data = test_alpaca_data();

        store
            .send(
                &metadata.issuer_request_id,
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

        store
            .send(
                &metadata.issuer_request_id,
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

        store
            .send(
                &metadata.issuer_request_id,
                RedemptionCommand::MarkFailed {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    reason: "Journal timed out".to_string(),
                },
            )
            .await
            .expect("MarkFailed failed");

        (metadata, alpaca_data)
    }

    /// Drives a redemption through a submitted burn that then fails, leaving
    /// the transaction ID in event history for the admin recovery route.
    async fn setup_burn_failure(
        store: &Store<Redemption>,
        tx_id: TxId,
    ) -> (RedemptionMetadata, AlpacaCalledData) {
        let metadata = setup_burning(store).await;
        let alpaca_data = test_alpaca_data();
        let burns = vec![MultiBurnEntry {
            receipt_id: U256::from(99),
            burn_shares: U256::from(100),
            receipt_info: None,
            receipt_info_bytes: None,
        }];
        let external_tx_id =
            Some(BurnExternalTxId::base(&metadata.detected_tx_hash));

        store
            .send(
                &metadata.issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    vault: address!(
                        "0xcccccccccccccccccccccccccccccccccccccccc"
                    ),
                    burns: burns.clone(),
                    dust_shares: U256::ZERO,
                    owner: Address::ZERO,
                    external_tx_id: external_tx_id.clone(),
                },
            )
            .await
            .expect("IntendBurn failed");

        store
            .send(
                &metadata.issuer_request_id,
                RedemptionCommand::BurnTokens {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    vault: address!(
                        "0xcccccccccccccccccccccccccccccccccccccccc"
                    ),
                    burns,
                    dust_shares: U256::ZERO,
                    owner: Address::ZERO,
                    external_tx_id,
                },
            )
            .await
            .expect("BurnTokens failed");

        store
            .send(
                &metadata.issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    error: "burn terminally failed".to_string(),
                    tx_id: Some(tx_id),
                    planned_burns: vec![BurnRecord {
                        receipt_id: U256::from(99),
                        shares_burned: U256::from(100),
                    }],
                },
            )
            .await
            .expect("RecordBurnFailure failed");

        (metadata, alpaca_data)
    }

    async fn seed_receipt_reservation(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> (Arc<Store<ReceiptInventory>>, Address) {
        let store = Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");

        store
            .send(
                &vault,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(U256::from(99)),
                    balance: Shares::from(U256::from(100)),
                    block_number: 12_345,
                    tx_hash: b256!(
                        "0xa4f500a4f500a4f500a4f500a4f500a4f500a4f500a4f500a4f500a4f500a4f5"
                    ),
                    source: ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .expect("receipt discovery should succeed");

        store
            .send(
                &vault,
                ReceiptInventoryCommand::ReserveBurn {
                    redemption_issuer_request_id: issuer_request_id.clone(),
                    burns: vec![BurnRecord {
                        receipt_id: U256::from(99),
                        shares_burned: U256::from(100),
                    }],
                },
            )
            .await
            .expect("receipt reservation should succeed");

        let inventory = store
            .load(&vault)
            .await
            .expect("receipt inventory should load")
            .expect("receipt inventory should exist");
        assert_eq!(
            inventory.reserved_redemptions(),
            vec![issuer_request_id.clone()]
        );

        (store, vault)
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

        let (rocket, store, _pool) = test_rocket(alpaca).await;
        let metadata = setup_pre_alpaca_failure(&store).await;

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

    fn post_alpaca_rocket(
        store: Arc<Store<Redemption>>,
        pool: sqlx::Pool<sqlx::Sqlite>,
        alpaca: Arc<dyn AlpacaService>,
        vault_service: Arc<dyn VaultService>,
        burn_recovery: Arc<dyn super::RedemptionBurnRecovery>,
    ) -> rocket::Rocket<rocket::Build> {
        use crate::alpaca::service::AlpacaConfig;
        use crate::auth::{FailedAuthRateLimiter, test_auth_config};
        use crate::config::{Config, Environment, LogLevel};
        use crate::wallet::SignerConfig;
        use alloy::primitives::B256;
        use url::Url;

        let config = Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").unwrap(),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            environment: Environment::Development,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
            subgraph_url: Url::parse("http://localhost:0/subgraph").unwrap(),
        };

        rocket::build()
            .manage(config)
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store)
            .manage(pool)
            .manage(alpaca)
            .manage(vault_service)
            .manage(burn_recovery)
            .mount("/", rocket::routes![super::recover_redemption])
    }

    async fn dispatch_recover_redemption(
        rocket: rocket::Rocket<rocket::Build>,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> (Status, String) {
        let client =
            rocket::local::asynchronous::Client::tracked(rocket).await.unwrap();

        let response = client
            .post(format!("/admin/recover/redemption/{issuer_request_id}"))
            .header(rocket::http::Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        let status = response.status();
        let body = response.into_string().await.unwrap();
        (status, body)
    }

    #[traced_test]
    #[tokio::test]
    async fn endpoint_rejects_burn_receipt_without_block_number() {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let tx_id = TxId::random();
        let (metadata, alpaca_data) =
            setup_burn_failure(&store, tx_id.clone()).await;
        let aggregate_id = metadata.issuer_request_id.to_string();
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let vault_service: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success().with_checked_tx_receipt(
                checked_burn_receipt(tx_id.to_hash().unwrap(), None, true),
            ));
        let burn_recovery = Arc::new(MockBurnRecovery::default());
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();
        let rocket = post_alpaca_rocket(
            store.clone(),
            pool.clone(),
            alpaca,
            vault_service,
            burn_recovery_state,
        );

        let (status, _body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(status, Status::InternalServerError);
        assert_eq!(burn_recovery.calls(), 0);
        let redemption =
            store.load(&metadata.issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(redemption, Redemption::Failed { .. }));
        let recovered_events: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::ExistingBurnRecovered'
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(recovered_events, 0);
        assert!(logs_contain_at!(
            Level::ERROR,
            &[&aggregate_id, "missing block number"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn endpoint_records_burn_receipt_block_number() {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let tx_id = TxId::random();
        let (metadata, alpaca_data) =
            setup_burn_failure(&store, tx_id.clone()).await;
        let aggregate_id = metadata.issuer_request_id.to_string();
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let vault_service: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success().with_checked_tx_receipt(
                checked_burn_receipt(
                    tx_id.to_hash().unwrap(),
                    Some(12_345),
                    true,
                ),
            ));
        let burn_recovery = Arc::new(MockBurnRecovery::default());
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();
        let rocket = post_alpaca_rocket(
            store.clone(),
            pool.clone(),
            alpaca,
            vault_service,
            burn_recovery_state,
        );

        let (status, body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(status, Status::Ok);
        assert!(body.contains("Existing on-chain burn recorded"));
        assert_eq!(burn_recovery.calls(), 0);
        let redemption =
            store.load(&metadata.issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(redemption, Redemption::Completed { .. }));
        let payload: String = sqlx::query_scalar(
            "
            SELECT payload
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::ExistingBurnRecovered'
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let event: crate::redemption::RedemptionEvent =
            serde_json::from_str(&payload).unwrap();
        let crate::redemption::RedemptionEvent::ExistingBurnRecovered {
            block_number,
            ..
        } = event
        else {
            panic!("expected existing burn recovery event");
        };
        assert_eq!(block_number, 12_345);
        assert!(logs_contain_at!(
            Level::INFO,
            &[&aggregate_id, "recording existing burn"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn endpoint_refuses_ambiguous_prior_burns() {
        let incomplete_revert_tx_id = TxId::random();
        let mismatched_success_tx_id = TxId::random();
        let mismatched_revert_tx_id = TxId::random();
        let cases = [
            (
                "pending",
                TxId::random(),
                MockVaultService::new_success().with_pending_checked_tx(),
            ),
            (
                "rpc_failed",
                TxId::random(),
                MockVaultService::new_success().with_rpc_checked_tx_error(),
            ),
            (
                "unknown",
                TxId::random(),
                MockVaultService::new_success().with_invalid_checked_tx(),
            ),
            (
                "legacy",
                TxId::Legacy("legacy-fireblocks-id".to_string()),
                MockVaultService::new_success().with_invalid_checked_tx(),
            ),
            (
                "incomplete_revert",
                incomplete_revert_tx_id.clone(),
                MockVaultService::new_success().with_checked_tx_receipt(
                    checked_burn_receipt(
                        incomplete_revert_tx_id.to_hash().unwrap(),
                        None,
                        false,
                    ),
                ),
            ),
            (
                "mismatched_success",
                mismatched_success_tx_id,
                MockVaultService::new_success().with_checked_tx_receipt(
                    checked_burn_receipt(B256::ZERO, Some(12_345), true),
                ),
            ),
            (
                "mismatched_revert",
                mismatched_revert_tx_id,
                MockVaultService::new_success().with_checked_tx_receipt(
                    checked_burn_receipt(B256::ZERO, Some(12_345), false),
                ),
            ),
        ];

        for (case, tx_id, vault_service) in cases {
            let pool = setup_pool().await;
            let store = setup_store(&pool);
            let (metadata, alpaca_data) =
                setup_burn_failure(&store, tx_id.clone()).await;
            let aggregate_id = metadata.issuer_request_id.to_string();
            let (receipt_inventory_store, vault) =
                seed_receipt_reservation(&pool, &metadata.issuer_request_id)
                    .await;
            let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
                response: PollResponse::Ok(redeem_response(
                    RedeemRequestStatus::Completed,
                    &metadata,
                    &alpaca_data,
                )),
            });
            let vault_service: Arc<dyn VaultService> = Arc::new(vault_service);
            let burn_recovery = Arc::new(ReservationTrackingBurnRecovery {
                calls: AtomicUsize::new(0),
                receipt_inventory_store: receipt_inventory_store.clone(),
                vault,
            });
            let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
                burn_recovery.clone();
            let rocket = post_alpaca_rocket(
                store.clone(),
                pool.clone(),
                alpaca,
                vault_service,
                burn_recovery_state,
            );

            let (status, _body) = dispatch_recover_redemption(
                rocket,
                &metadata.issuer_request_id,
            )
            .await;

            assert_eq!(status, Status::UnprocessableEntity, "case: {case}");
            assert_eq!(burn_recovery.calls(), 0, "case: {case}");
            let redemption =
                store.load(&metadata.issuer_request_id).await.unwrap().unwrap();
            assert!(
                matches!(redemption, Redemption::Failed { .. }),
                "case: {case}"
            );
            let context =
                load_reprocess_context(&pool, &metadata.issuer_request_id)
                    .await
                    .unwrap();
            let planned_burns = context.burning_failed.unwrap().planned_burns;
            assert_eq!(planned_burns.len(), 1, "case: {case}");
            let planned_burn = planned_burns.first().unwrap();
            assert_eq!(planned_burn.receipt_id, U256::from(99));
            assert_eq!(planned_burn.shares_burned, U256::from(100));
            let receipt_inventory =
                receipt_inventory_store.load(&vault).await.unwrap().unwrap();
            assert_eq!(
                receipt_inventory.reserved_redemptions(),
                vec![metadata.issuer_request_id.clone()],
                "case: {case}"
            );
            let advancing_events: i64 = sqlx::query_scalar(
                "
                SELECT COUNT(*)
                FROM events
                WHERE aggregate_type = 'Redemption'
                  AND aggregate_id = ?
                  AND event_type IN (
                      'RedemptionEvent::BurnResumed',
                      'RedemptionEvent::ExistingBurnRecovered'
                  )
                ",
            )
            .bind(&aggregate_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(advancing_events, 0, "case: {case}");
            assert!(
                logs_contain_at!(
                    Level::WARN,
                    &[&aggregate_id, "manual intervention"]
                ),
                "case: {case}"
            );
        }
    }

    async fn assert_endpoint_refuses_exhausted_burn_recovery(
        with_marker: bool,
    ) {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let tx_id = TxId::random();
        let tx_hash = tx_id.to_hash().unwrap();
        let (metadata, alpaca_data) = setup_burn_failure(&store, tx_id).await;
        let aggregate_id = metadata.issuer_request_id.to_string();
        let annotations = if with_marker {
            vec![RedemptionEvent::BurnRecoveryExhausted {
                issuer_request_id: metadata.issuer_request_id.clone(),
                tx_hash,
                nonce: 4,
                attempts: MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
                exhausted_at: Utc::now(),
            }]
        } else {
            (0..MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS)
                .map(|_| RedemptionEvent::BurnRecoveryAttempted {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    tx_hash,
                    nonce: 4,
                    action: BurnRecoveryAction::Rebroadcast,
                    attempted_at: Utc::now(),
                })
                .collect()
        };
        for annotation in annotations {
            let event_type = match annotation {
                RedemptionEvent::BurnRecoveryAttempted { .. } => {
                    "RedemptionEvent::BurnRecoveryAttempted"
                }
                RedemptionEvent::BurnRecoveryExhausted { .. } => {
                    "RedemptionEvent::BurnRecoveryExhausted"
                }
                _ => unreachable!(),
            };
            sqlx::query(
                "
                INSERT INTO events (
                    aggregate_type,
                    aggregate_id,
                    sequence,
                    event_type,
                    event_version,
                    payload,
                    metadata
                )
                SELECT
                    'Redemption',
                    ?,
                    COALESCE(MAX(sequence), 0) + 1,
                    ?,
                    '1.0',
                    ?,
                    '{}'
                FROM events
                WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
                ",
            )
            .bind(&aggregate_id)
            .bind(event_type)
            .bind(serde_json::to_string(&annotation).unwrap())
            .bind(&aggregate_id)
            .execute(&pool)
            .await
            .unwrap();
        }

        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let burn_recovery = Arc::new(MockBurnRecovery::default());
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();
        let rocket = post_alpaca_rocket(
            store,
            pool.clone(),
            alpaca,
            mock_vault_service(),
            burn_recovery_state,
        );

        let (status, _body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(status, Status::UnprocessableEntity);
        assert_eq!(burn_recovery.calls(), 0);
        let resumed_events: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnResumed'
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(resumed_events, 0);
        assert!(logs_contain_at!(
            Level::WARN,
            &[&aggregate_id, "exhausted automatic burn recovery"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn endpoint_refuses_marked_exhausted_burn_recovery() {
        assert_endpoint_refuses_exhausted_burn_recovery(true).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn endpoint_refuses_count_only_exhausted_burn_recovery() {
        assert_endpoint_refuses_exhausted_burn_recovery(false).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn endpoint_replaces_only_confirmed_reverted_prior_burn() {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let tx_id = TxId::random();
        let tx_hash = tx_id.to_hash().unwrap();
        let (metadata, alpaca_data) =
            setup_burn_failure(&store, tx_id.clone()).await;
        let aggregate_id = metadata.issuer_request_id.to_string();
        let (receipt_inventory_store, vault) =
            seed_receipt_reservation(&pool, &metadata.issuer_request_id).await;
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let vault_service: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success().with_checked_tx_receipt(
                checked_burn_receipt(tx_hash, Some(12_345), false),
            ));
        let burn_recovery = Arc::new(ReservationTrackingBurnRecovery {
            calls: AtomicUsize::new(0),
            receipt_inventory_store: receipt_inventory_store.clone(),
            vault,
        });
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();
        let rocket = post_alpaca_rocket(
            store.clone(),
            pool.clone(),
            alpaca,
            vault_service,
            burn_recovery_state,
        );

        let (status, body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(status, Status::Ok);
        assert!(body.contains("executed burn"));
        assert_eq!(burn_recovery.calls(), 1);
        let redemption =
            store.load(&metadata.issuer_request_id).await.unwrap().unwrap();
        let Redemption::Burning { external_tx_id, .. } = redemption else {
            panic!("expected Burning state, got {redemption:?}");
        };
        assert_eq!(
            external_tx_id,
            Some(Redemption::retry_burn_external_tx_id_typed(
                &metadata.detected_tx_hash,
                1,
            ))
        );
        let receipt_inventory =
            receipt_inventory_store.load(&vault).await.unwrap().unwrap();
        assert!(receipt_inventory.reserved_redemptions().is_empty());
        let resumed_events: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnResumed'
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(resumed_events, 1);
        assert!(logs_contain_at!(
            Level::INFO,
            &[&aggregate_id, "Transaction reverted onchain", "ResumeBurn"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_post_alpaca_recovery_resumes_to_burning() {
        // Build the Alpaca mock from the actual metadata produced by setup so
        // the endpoint's field validation passes.
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let (metadata, alpaca_data) = setup_post_alpaca_failure(&store).await;

        // Now build the mock with the real metadata so field validation passes.
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        let burn_recovery = Arc::new(MockBurnRecovery::default());
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();

        let rocket = post_alpaca_rocket(
            store,
            pool,
            alpaca,
            mock_vault_service(),
            burn_recovery_state,
        );

        let (status, body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(status, Status::Ok);
        assert!(body.contains("Recovered from Failed"));
        assert!(body.contains("executed burn"));
        assert_eq!(burn_recovery.calls(), 1);
        assert!(logs_contain_at!(Level::INFO, &["recovered", "Burning"]));
        assert!(logs_contain_at!(Level::INFO, &["burn", "executed"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_post_alpaca_recovery_reports_skipped_burn() {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let (metadata, alpaca_data) = setup_post_alpaca_failure(&store).await;

        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        // The burn was skipped (e.g. insufficient on-chain balance), so the
        // endpoint must NOT claim the burn executed.
        let burn_recovery = Arc::new(MockBurnRecovery {
            result: MockBurnResult::Succeeds(
                super::RecoveryOutcome::SkippedManualIntervention,
            ),
            ..Default::default()
        });
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();

        let rocket = post_alpaca_rocket(
            store,
            pool,
            alpaca,
            mock_vault_service(),
            burn_recovery_state,
        );

        let (status, body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(status, Status::Ok);
        assert!(body.contains("manual intervention required"));
        assert!(!body.contains("executed burn"));
        assert_eq!(burn_recovery.calls(), 1);
        assert!(logs_contain_at!(
            Level::WARN,
            &["burn was skipped", "manual intervention required"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_post_alpaca_recovery_burn_failure_returns_502() {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let (metadata, alpaca_data) = setup_post_alpaca_failure(&store).await;

        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Ok(redeem_response(
                RedeemRequestStatus::Completed,
                &metadata,
                &alpaca_data,
            )),
        });
        // The burn execution itself fails, which must surface as a 502 so the
        // operator knows the recovery did not complete.
        let burn_recovery = Arc::new(MockBurnRecovery {
            result: MockBurnResult::Fails,
            ..Default::default()
        });
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();

        let rocket = post_alpaca_rocket(
            store,
            pool,
            alpaca,
            mock_vault_service(),
            burn_recovery_state,
        );

        let (status, _body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(status, Status::BadGateway);
        assert_eq!(burn_recovery.calls(), 1);
        assert!(logs_contain_at!(
            Level::ERROR,
            &["Failed to execute recovered redemption burn"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_post_alpaca_recovery_request_not_found_returns_404()
    {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let (metadata, _alpaca_data) = setup_post_alpaca_failure(&store).await;

        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Error(AlpacaError::RequestNotFound {
                id: TokenizationRequestId::new("tok-test-1"),
                body: "not found".to_string(),
            }),
        });
        let burn_recovery = Arc::new(MockBurnRecovery::default());
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();

        let rocket = post_alpaca_rocket(
            store,
            pool,
            alpaca,
            mock_vault_service(),
            burn_recovery_state,
        );

        let (status, _body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(
            status,
            Status::NotFound,
            "RequestNotFound from Alpaca must return 404, not 502"
        );
        assert_eq!(
            burn_recovery.calls(),
            0,
            "Burn recovery must not be called"
        );
        assert!(
            logs_contain_at!(Level::ERROR, &["Tokenization request not found"]),
            "Expected error log for poll failure"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_post_alpaca_recovery_response_id_mismatch_returns_502()
     {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let (metadata, _alpaca_data) = setup_post_alpaca_failure(&store).await;

        let requested = TokenizationRequestId::new("tok-test-1");
        let returned = TokenizationRequestId::new("tok-other-id");
        let alpaca: Arc<dyn AlpacaService> = Arc::new(PollMockAlpaca {
            response: PollResponse::Error(AlpacaError::ResponseIdMismatch {
                requested: requested.clone(),
                returned: returned.clone(),
            }),
        });
        let burn_recovery = Arc::new(MockBurnRecovery::default());
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();

        let rocket = post_alpaca_rocket(
            store,
            pool,
            alpaca,
            mock_vault_service(),
            burn_recovery_state,
        );

        let (status, _body) =
            dispatch_recover_redemption(rocket, &metadata.issuer_request_id)
                .await;

        assert_eq!(
            status,
            Status::BadGateway,
            "ResponseIdMismatch from Alpaca must return 502"
        );
        assert_eq!(
            burn_recovery.calls(),
            0,
            "Burn recovery must not be called"
        );
        assert!(
            logs_contain_at!(
                Level::ERROR,
                &["mismatched tokenization request id"]
            ),
            "Expected error log for response id mismatch"
        );
    }

    /// Drives a redemption to `Burning` state (post-journal, pre-burn) — the
    /// state stuck redemptions wedge in.
    async fn setup_burning(store: &Store<Redemption>) -> RedemptionMetadata {
        let metadata = test_metadata();
        let alpaca_data = test_alpaca_data();

        store
            .send(
                &metadata.issuer_request_id,
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

        store
            .send(
                &metadata.issuer_request_id,
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

        store
            .send(
                &metadata.issuer_request_id,
                RedemptionCommand::ConfirmAlpacaComplete {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                },
            )
            .await
            .expect("ConfirmAlpacaComplete failed");

        metadata
    }

    async fn setup_burn_intended(
        store: &Store<Redemption>,
        persisted_tx: &SendableTxWithHash,
        vault: Address,
    ) -> RedemptionMetadata {
        setup_burn_intended_with_dust(store, persisted_tx, vault, U256::ZERO)
            .await
    }

    async fn setup_burn_intended_with_dust(
        store: &Store<Redemption>,
        persisted_tx: &SendableTxWithHash,
        vault: Address,
        dust_shares: U256,
    ) -> RedemptionMetadata {
        let metadata = setup_burning(store).await;
        store
            .send(
                &metadata.issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: metadata.issuer_request_id.clone(),
                    vault,
                    burns: vec![MultiBurnEntry {
                        receipt_id: U256::from(42),
                        burn_shares: U256::from(17),
                        receipt_info: None,
                        receipt_info_bytes: None,
                    }],
                    dust_shares,
                    owner: persisted_tx.signer_for_test(),
                    external_tx_id: None,
                },
            )
            .await
            .expect("IntendBurn failed");

        metadata
    }

    fn admin_test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").unwrap(),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            environment: Environment::Development,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
            subgraph_url: Url::parse("http://localhost:0/subgraph").unwrap(),
            receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
        }
    }

    fn close_redemption_rocket(
        store: Arc<Store<Redemption>>,
        pool: sqlx::Pool<sqlx::Sqlite>,
    ) -> rocket::Rocket<rocket::Build> {
        rocket::build()
            .manage(admin_test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store)
            .manage(pool)
            .mount("/", rocket::routes![super::close_redemption])
    }

    async fn dispatch_close_redemption(
        rocket: rocket::Rocket<rocket::Build>,
        issuer_request_id: &IssuerRedemptionRequestId,
        body: &str,
    ) -> (Status, String) {
        let client =
            rocket::local::asynchronous::Client::tracked(rocket).await.unwrap();
        let response = client
            .post(format!("/admin/close/redemption/{issuer_request_id}"))
            .header(rocket::http::ContentType::JSON)
            .header(rocket::http::Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(body)
            .dispatch()
            .await;

        let status = response.status();
        let body = response.into_string().await.unwrap();
        (status, body)
    }

    #[traced_test]
    #[tokio::test]
    async fn close_endpoint_rejects_unacknowledged_persisted_burn() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let vault_service: Arc<dyn VaultService> = Arc::new(
            MockVaultService::new_success()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let store = setup_store_with_vault(&pool, vault_service);
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;

        let rocket = close_redemption_rocket(store.clone(), pool);
        let (status, _) = dispatch_close_redemption(
            rocket,
            &metadata.issuer_request_id,
            r#"{"reason":"operator reconciled externally"}"#,
        )
        .await;

        assert_eq!(status, Status::UnprocessableEntity);
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::BurnIntended { .. })
        ));
        let persisted_hash = format!("{:?}", persisted_tx.hash);
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Failed to close redemption",
                "requires explicit operator acknowledgement",
                &persisted_hash,
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn close_endpoint_records_and_reports_burn_acknowledgement() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let vault_service: Arc<dyn VaultService> = Arc::new(
            MockVaultService::new_success()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let store = setup_store_with_vault(&pool, vault_service);
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;
        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        receipt_store
            .send(
                &vault,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(U256::from(42)),
                    balance: Shares::new(U256::from(17)),
                    block_number: 1,
                    tx_hash: B256::random(),
                    source: ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .expect("receipt discovery should succeed");
        let receipt_service = CqrsReceiptService::new(receipt_store);
        receipt_service
            .reserve_burn(
                vault,
                metadata.issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: U256::from(42),
                    shares_burned: U256::from(17),
                }],
            )
            .await
            .expect("reservation should seed");

        let rocket = close_redemption_rocket(store.clone(), pool);
        let body = format!(
            r#"{{"reason":"operator reconciled externally","acknowledged_unresolved_burn_tx_hash":"{:#x}"}}"#,
            persisted_tx.hash
        );
        let (status, response_body) = dispatch_close_redemption(
            rocket,
            &metadata.issuer_request_id,
            &body,
        )
        .await;

        assert_eq!(status, Status::Ok);
        assert!(
            response_body.contains(&format!("{:#x}", persisted_tx.hash)),
            "body: {response_body}"
        );
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::Closed { .. })
        ));
        assert_eq!(
            receipt_service.reserved_redemptions(vault).await.unwrap(),
            vec![metadata.issuer_request_id.clone()],
            "acknowledged close must retain the unresolved reservation"
        );
        let persisted_hash = format!("{:?}", persisted_tx.hash);
        assert!(logs_contain_at!(
            Level::INFO,
            &[
                "Redemption closed",
                "acknowledged_unresolved_burn_tx_hash",
                &persisted_hash,
            ]
        ));
    }

    fn force_complete_rocket(
        pool: sqlx::Pool<sqlx::Sqlite>,
        burn_recovery: Arc<dyn super::RedemptionBurnRecovery>,
    ) -> rocket::Rocket<rocket::Build> {
        rocket::build()
            .manage(admin_test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .manage(burn_recovery)
            .mount("/", rocket::routes![super::force_complete_redemption])
    }

    async fn real_burn_recovery(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        store: Arc<Store<Redemption>>,
        vault_service: Arc<dyn VaultService>,
        vault: Address,
        owner: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> (Arc<dyn super::RedemptionBurnRecovery>, Arc<CqrsReceiptService>) {
        let (asset_store, _asset_projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("tokenized asset store should build");
        let underlying = UnderlyingSymbol::new("AAPL");
        asset_store
            .send(
                &underlying,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    vault,
                },
            )
            .await
            .expect("tokenized asset should seed");

        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        receipt_store
            .send(
                &vault,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(U256::from(42)),
                    balance: Shares::new(U256::from(17)),
                    block_number: 1,
                    tx_hash: B256::random(),
                    source: ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .expect("receipt should seed");
        let receipt_service = Arc::new(CqrsReceiptService::new(receipt_store));
        receipt_service
            .reserve_burn(
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: U256::from(42),
                    shares_burned: U256::from(17),
                }],
            )
            .await
            .expect("reservation should seed");
        let burn_recovery: Arc<dyn super::RedemptionBurnRecovery> =
            Arc::new(super::BurnManager::new(
                vault_service,
                pool.clone(),
                store,
                receipt_service.clone(),
                owner,
            ));

        (burn_recovery, receipt_service)
    }

    async fn dispatch_force_complete(
        rocket: rocket::Rocket<rocket::Build>,
        issuer_request_id: &IssuerRedemptionRequestId,
        body: &str,
    ) -> (Status, String) {
        let client =
            rocket::local::asynchronous::Client::tracked(rocket).await.unwrap();

        let response = client
            .post(format!(
                "/admin/force-complete/redemption/{issuer_request_id}"
            ))
            .header(rocket::http::ContentType::JSON)
            .header(rocket::http::Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(body)
            .dispatch()
            .await;

        let status = response.status();
        let body = response.into_string().await.unwrap();
        (status, body)
    }

    fn stuck_rocket(
        pool: sqlx::Pool<sqlx::Sqlite>,
        vault_service: Arc<dyn VaultService>,
    ) -> rocket::Rocket<rocket::Build> {
        rocket::build()
            .manage(admin_test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .manage(vault_service)
            .mount("/", rocket::routes![super::list_stuck])
    }

    #[tokio::test]
    async fn stuck_endpoint_exposes_persisted_burn_intent_hash() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let vault_service = Arc::new(
            MockVaultService::new_success()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let store = setup_store_with_vault(&pool, vault_service.clone());
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;
        let alpaca = test_alpaca_data();
        let old_timestamp = Utc::now() - chrono::Duration::hours(2);
        let view = RedemptionView::Burning {
            issuer_request_id: metadata.issuer_request_id.clone(),
            tokenization_request_id: alpaca.tokenization_request_id,
            underlying: metadata.underlying,
            token: metadata.token,
            wallet: metadata.wallet,
            quantity: metadata.quantity,
            alpaca_quantity: alpaca.alpaca_quantity,
            dust_quantity: alpaca.dust_quantity,
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at: metadata.detected_at,
            called_at: old_timestamp,
            alpaca_journal_completed_at: old_timestamp,
            burning_entered_at: old_timestamp,
        };
        let payload = serde_json::to_string(&view).unwrap();
        sqlx::query(
            "
            INSERT INTO redemption_view (
                view_id,
                version,
                payload
            )
            VALUES (?, 4, ?)
            ",
        )
        .bind(metadata.issuer_request_id.to_string())
        .bind(payload)
        .execute(&pool)
        .await
        .unwrap();

        let client = rocket::local::asynchronous::Client::tracked(
            stuck_rocket(pool, vault_service),
        )
        .await
        .unwrap();
        let response = client
            .get("/admin/stuck")
            .header(rocket::http::Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body = response.into_string().await.unwrap();
        assert!(
            body.contains(&format!("{:#x}", persisted_tx.hash)),
            "body: {body}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_force_complete_records_verified_burn() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burn(45_989_009, U256::from(17))
                .with_prepared_tx(persisted_tx.clone()),
        );
        let vault_service: Arc<dyn VaultService> = vault_mock.clone();
        let store = setup_store_with_vault(&pool, vault_service.clone());
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;
        let (burn_recovery, receipt_service) = real_burn_recovery(
            &pool,
            store.clone(),
            vault_service,
            vault,
            persisted_tx.signer_for_test(),
            &metadata.issuer_request_id,
        )
        .await;

        let rocket = force_complete_rocket(pool, burn_recovery);
        let body = format!(
            r#"{{"burn_tx_hash":"{:#x}","reason":"burn confirmed on-chain"}}"#,
            persisted_tx.hash
        );

        let (status, body) =
            dispatch_force_complete(rocket, &metadata.issuer_request_id, &body)
                .await;

        assert_eq!(status, Status::Ok);
        // Response reports the proven block and the true previous state.
        assert!(body.contains("Force-completed"), "body: {body}");
        assert!(body.contains("45989009"), "body: {body}");
        assert!(body.contains("BurnIntended"), "body: {body}");
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::Completed { .. })
        ));
        assert!(
            receipt_service
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(vault_mock.verify_burn_call_count(), 1);
        assert!(logs_contain_at!(Level::INFO, &["Redemption force-completed"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn force_complete_endpoint_passes_and_reports_burn_acknowledgement() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let proving_burn_tx_hash = B256::random();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns(
                    45_989_009,
                    persisted_tx.nonce,
                    vec![VerifiedBurn {
                        sender: persisted_tx.signer_for_test(),
                        receiver: test_metadata().wallet,
                        receipt_id: U256::from(42),
                        shares_burned: U256::from(17),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let vault_service: Arc<dyn VaultService> = vault_mock.clone();
        let store = setup_store_with_vault(&pool, vault_service.clone());
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;
        let (burn_recovery, receipt_service) = real_burn_recovery(
            &pool,
            store.clone(),
            vault_service,
            vault,
            persisted_tx.signer_for_test(),
            &metadata.issuer_request_id,
        )
        .await;
        let rocket = force_complete_rocket(pool, burn_recovery);
        let body = format!(
            r#"{{"burn_tx_hash":"{proving_burn_tx_hash:#x}","reason":"alternate burn reconciled","acknowledged_unresolved_burn_tx_hash":"{:#x}"}}"#,
            persisted_tx.hash
        );

        let (status, response_body) =
            dispatch_force_complete(rocket, &metadata.issuer_request_id, &body)
                .await;

        assert_eq!(status, Status::Ok);
        assert!(
            response_body.contains(&format!("{proving_burn_tx_hash:#x}")),
            "body: {response_body}"
        );
        assert!(
            response_body.contains(&format!("{:#x}", persisted_tx.hash)),
            "body: {response_body}"
        );
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::Completed { burn_tx_hash, .. })
                if burn_tx_hash == proving_burn_tx_hash
        ));
        assert!(
            receipt_service
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(vault_mock.verify_burn_call_count(), 1);
        let proving_hash = format!("{proving_burn_tx_hash:?}");
        let persisted_hash = format!("{:?}", persisted_tx.hash);
        assert!(logs_contain_at!(
            Level::INFO,
            &[
                "Redemption force-completed",
                "acknowledged_unresolved_burn_tx_hash",
                &proving_hash,
                &persisted_hash,
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn force_complete_endpoint_rejects_alternate_burn_missing_dust() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let dust_shares = U256::from(3);
        let mut persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        persisted_tx.dust_shares = dust_shares;
        let proving_burn_tx_hash = B256::random();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns(
                    45_989_009,
                    persisted_tx.nonce,
                    vec![VerifiedBurn {
                        sender: persisted_tx.signer_for_test(),
                        receiver: test_metadata().wallet,
                        receipt_id: U256::from(42),
                        shares_burned: U256::from(17),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let vault_service: Arc<dyn VaultService> = vault_mock.clone();
        let store = setup_store_with_vault(&pool, vault_service.clone());
        let metadata = setup_burn_intended_with_dust(
            &store,
            &persisted_tx,
            vault,
            dust_shares,
        )
        .await;
        let (burn_recovery, receipt_service) = real_burn_recovery(
            &pool,
            store.clone(),
            vault_service,
            vault,
            persisted_tx.signer_for_test(),
            &metadata.issuer_request_id,
        )
        .await;
        let body = format!(
            r#"{{"burn_tx_hash":"{proving_burn_tx_hash:#x}","reason":"alternate burn omitted dust","acknowledged_unresolved_burn_tx_hash":"{:#x}"}}"#,
            persisted_tx.hash
        );

        let (status, _) = dispatch_force_complete(
            force_complete_rocket(pool, burn_recovery),
            &metadata.issuer_request_id,
            &body,
        )
        .await;

        assert_eq!(status, Status::UnprocessableEntity);
        assert_eq!(vault_mock.verify_burn_call_count(), 1);
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::BurnIntended { .. })
        ));
        assert_eq!(
            receipt_service.reserved_redemptions(vault).await.unwrap(),
            vec![metadata.issuer_request_id]
        );
        let proving_hash = format!("{proving_burn_tx_hash:?}");
        let persisted_hash = format!("{:?}", persisted_tx.hash);
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Failed to force-complete redemption",
                "Alternate proving transaction does not match",
                "expected nonce 7",
                "dust 3",
                &proving_hash,
                &persisted_hash,
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn force_complete_endpoint_rejects_mismatched_aggregate_burn_total() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let proving_burn_tx_hash = B256::random();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns_and_total(
                    45_989_009,
                    persisted_tx.nonce,
                    U256::from(18),
                    vec![VerifiedBurn {
                        sender: persisted_tx.signer_for_test(),
                        receiver: test_metadata().wallet,
                        receipt_id: U256::from(42),
                        shares_burned: U256::from(17),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let vault_service: Arc<dyn VaultService> = vault_mock.clone();
        let store = setup_store_with_vault(&pool, vault_service.clone());
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;
        let (burn_recovery, receipt_service) = real_burn_recovery(
            &pool,
            store.clone(),
            vault_service,
            vault,
            persisted_tx.signer_for_test(),
            &metadata.issuer_request_id,
        )
        .await;
        let body = format!(
            r#"{{"burn_tx_hash":"{proving_burn_tx_hash:#x}","reason":"alternate burn has mismatched total","acknowledged_unresolved_burn_tx_hash":"{:#x}"}}"#,
            persisted_tx.hash
        );

        let (status, _) = dispatch_force_complete(
            force_complete_rocket(pool, burn_recovery),
            &metadata.issuer_request_id,
            &body,
        )
        .await;

        assert_eq!(status, Status::UnprocessableEntity);
        assert_eq!(vault_mock.verify_burn_call_count(), 1);
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::BurnIntended { .. })
        ));
        assert_eq!(
            receipt_service.reserved_redemptions(vault).await.unwrap(),
            vec![metadata.issuer_request_id]
        );
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Failed to force-complete redemption",
                "expected nonce 7",
                "total shares 17",
                "verified nonce 7",
                "total shares 18",
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn force_complete_endpoint_rejects_wrong_burn_acknowledgement() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns(
                    45_989_009,
                    persisted_tx.nonce,
                    vec![VerifiedBurn {
                        sender: persisted_tx.signer_for_test(),
                        receiver: test_metadata().wallet,
                        receipt_id: U256::from(42),
                        shares_burned: U256::from(17),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let vault_service: Arc<dyn VaultService> = vault_mock.clone();
        let store = setup_store_with_vault(&pool, vault_service.clone());
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;
        let (burn_recovery, receipt_service) = real_burn_recovery(
            &pool,
            store.clone(),
            vault_service,
            vault,
            persisted_tx.signer_for_test(),
            &metadata.issuer_request_id,
        )
        .await;
        let proving_hash = B256::random();
        let wrong_acknowledgement = B256::random();
        let body = format!(
            r#"{{"burn_tx_hash":"{proving_hash:#x}","reason":"wrong acknowledgement","acknowledged_unresolved_burn_tx_hash":"{wrong_acknowledgement:#x}"}}"#
        );

        let (status, _) = dispatch_force_complete(
            force_complete_rocket(pool, burn_recovery),
            &metadata.issuer_request_id,
            &body,
        )
        .await;

        assert_eq!(status, Status::UnprocessableEntity);
        assert_eq!(vault_mock.verify_burn_call_count(), 0);
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::BurnIntended { .. })
        ));
        assert_eq!(
            receipt_service.reserved_redemptions(vault).await.unwrap(),
            vec![metadata.issuer_request_id]
        );
        let persisted_hash = format!("{:?}", persisted_tx.hash);
        let wrong_hash = format!("{wrong_acknowledgement:?}");
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Failed to force-complete redemption",
                &persisted_hash,
                &wrong_hash,
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn force_complete_endpoint_rejects_redundant_burn_acknowledgement() {
        let pool = setup_pool().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns(
                    45_989_009,
                    persisted_tx.nonce,
                    vec![VerifiedBurn {
                        sender: persisted_tx.signer_for_test(),
                        receiver: test_metadata().wallet,
                        receipt_id: U256::from(42),
                        shares_burned: U256::from(17),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let vault_service: Arc<dyn VaultService> = vault_mock.clone();
        let store = setup_store_with_vault(&pool, vault_service.clone());
        let metadata = setup_burn_intended(&store, &persisted_tx, vault).await;
        let (burn_recovery, receipt_service) = real_burn_recovery(
            &pool,
            store.clone(),
            vault_service,
            vault,
            persisted_tx.signer_for_test(),
            &metadata.issuer_request_id,
        )
        .await;
        let body = format!(
            r#"{{"burn_tx_hash":"{0:#x}","reason":"persisted burn verified","acknowledged_unresolved_burn_tx_hash":"{0:#x}"}}"#,
            persisted_tx.hash
        );

        let (status, _) = dispatch_force_complete(
            force_complete_rocket(pool, burn_recovery),
            &metadata.issuer_request_id,
            &body,
        )
        .await;

        assert_eq!(status, Status::UnprocessableEntity);
        assert_eq!(vault_mock.verify_burn_call_count(), 0);
        assert!(matches!(
            store.load(&metadata.issuer_request_id).await.unwrap(),
            Some(Redemption::BurnIntended { .. })
        ));
        assert_eq!(
            receipt_service.reserved_redemptions(vault).await.unwrap(),
            vec![metadata.issuer_request_id]
        );
        let persisted_hash = format!("{:?}", persisted_tx.hash);
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Failed to force-complete redemption",
                "redundant because the proving burn is the persisted signed burn",
                &persisted_hash,
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_endpoint_force_complete_unverifiable_returns_422() {
        let pool = setup_pool().await;
        let store = setup_store(&pool);
        let metadata = setup_burning(&store).await;

        // Verification fails: the operator-supplied hash is not a burn.
        let burn_recovery = Arc::new(MockBurnRecovery {
            force_result: MockForceResult::NotABurn,
            ..Default::default()
        });
        let burn_recovery_state: Arc<dyn super::RedemptionBurnRecovery> =
            burn_recovery.clone();

        let rocket = force_complete_rocket(pool, burn_recovery_state);
        let body = r#"{"burn_tx_hash":"0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf","reason":"not actually a burn"}"#;

        let (status, _body) =
            dispatch_force_complete(rocket, &metadata.issuer_request_id, body)
                .await;

        assert_eq!(status, Status::UnprocessableEntity);
        assert_eq!(burn_recovery.force_calls(), 1);
        assert!(logs_contain_at!(
            Level::ERROR,
            &["Failed to force-complete redemption"]
        ));
    }

    #[test]
    fn map_burn_manager_error_classifies_statuses() {
        use super::map_burn_manager_error;

        let tx = alloy::primitives::B256::ZERO;

        assert_eq!(
            map_burn_manager_error(&super::BurnManagerError::Vault(
                super::VaultError::NotABurn { tx_hash: tx }
            )),
            Status::UnprocessableEntity
        );
        assert_eq!(
            map_burn_manager_error(&super::BurnManagerError::Vault(
                super::VaultError::Reverted { tx_hash: tx }
            )),
            Status::UnprocessableEntity
        );
        assert_eq!(
            map_burn_manager_error(
                &super::BurnManagerError::InvalidAggregateState {
                    current_state: "Completed".to_string()
                }
            ),
            Status::UnprocessableEntity
        );
        assert_eq!(
            map_burn_manager_error(&super::BurnManagerError::Redemption(
                crate::redemption::RedemptionError::UnresolvedBurnAcknowledgementMismatch {
                    expected: tx,
                    provided: B256::random(),
                },
            )),
            Status::UnprocessableEntity
        );
        // A hash that resolves to no receipt (or one that doesn't prove a burn)
        // is an unverifiable operator-supplied input, not an upstream failure.
        assert_eq!(
            map_burn_manager_error(&super::BurnManagerError::Vault(
                super::VaultError::InvalidReceipt
            )),
            Status::UnprocessableEntity
        );
        assert_eq!(
            map_burn_manager_error(&super::BurnManagerError::SharesOverflow),
            Status::InternalServerError
        );
    }

    #[test]
    fn is_stuck_terminal_fail_always_true_regardless_of_age() {
        let now = Utc::now();
        let just_now = now;
        let long_ago = now - chrono::Duration::days(30);

        assert!(super::is_stuck(
            super::StuckClass::TerminalFail,
            just_now,
            now
        ));
        assert!(super::is_stuck(
            super::StuckClass::TerminalFail,
            long_ago,
            now
        ));
    }

    #[test]
    fn is_stuck_in_progress_uses_one_hour_threshold() {
        let now = Utc::now();

        // Just under threshold — not stuck yet.
        let fresh = now - chrono::Duration::minutes(59);
        assert!(!super::is_stuck(super::StuckClass::InProgress, fresh, now));

        // Exactly at threshold — stuck.
        let at_threshold = now - super::STUCK_THRESHOLD;
        assert!(super::is_stuck(
            super::StuckClass::InProgress,
            at_threshold,
            now
        ));

        // Older than threshold — stuck.
        let old = now - chrono::Duration::hours(13);
        assert!(super::is_stuck(super::StuckClass::InProgress, old, now));
    }

    #[test]
    fn redemption_stuck_info_classifies_and_timestamps_each_variant() {
        use super::RedemptionView::{
            AlpacaCalled, BurnFailed, Burning, Closed, Completed, Detected,
            Failed, Unavailable,
        };
        use super::StuckClass::{InProgress, TerminalFail};

        let metadata = test_metadata();
        let issuer = metadata.issuer_request_id.clone();
        let underlying = metadata.underlying.clone();
        let token = metadata.token.clone();
        let quantity = metadata.quantity.clone();
        let tx_hash = metadata.detected_tx_hash;
        let block_number = metadata.block_number;
        let detected_at = metadata.detected_at;
        let called_at = Utc::now();
        let burning_entered_at = Utc::now();
        let failed_at = Utc::now();
        let burn_failed_at = Utc::now();
        let tok_id = TokenizationRequestId::new("tok-1");

        // Detected → InProgress with detected_entered_at. Use a distinct
        // (later) value than detected_at so the projection unambiguously
        // selects detected_entered_at — this is the post-reprocess clock.
        let detected_entered_at = detected_at + chrono::Duration::days(7);
        let detected = Detected {
            issuer_request_id: issuer.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: metadata.wallet,
            quantity: quantity.clone(),
            tx_hash,
            block_number,
            detected_at,
            detected_entered_at,
        };
        assert_eq!(
            super::redemption_stuck_info(&detected),
            Some((InProgress, detected_entered_at))
        );

        // AlpacaCalled → InProgress with called_at.
        let alpaca_called = AlpacaCalled {
            issuer_request_id: issuer.clone(),
            tokenization_request_id: tok_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: metadata.wallet,
            quantity: quantity.clone(),
            alpaca_quantity: quantity.clone(),
            dust_quantity: Quantity::default(),
            tx_hash,
            block_number,
            detected_at,
            called_at,
        };
        assert_eq!(
            super::redemption_stuck_info(&alpaca_called),
            Some((InProgress, called_at))
        );

        // Burning → InProgress with burning_entered_at (NOT
        // alpaca_journal_completed_at, which would lag for resumed
        // redemptions). Use a distinct journal-completion time to make the
        // distinction observable.
        let burning = Burning {
            issuer_request_id: issuer.clone(),
            tokenization_request_id: tok_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: metadata.wallet,
            alpaca_quantity: quantity.clone(),
            quantity: quantity.clone(),
            dust_quantity: Quantity::default(),
            tx_hash,
            block_number,
            detected_at,
            called_at,
            alpaca_journal_completed_at: detected_at,
            burning_entered_at,
        };
        assert_eq!(
            super::redemption_stuck_info(&burning),
            Some((InProgress, burning_entered_at))
        );

        // Failed → TerminalFail with failed_at.
        let failed = Failed {
            issuer_request_id: issuer.clone(),
            reason: "x".to_string(),
            failed_at,
        };
        assert_eq!(
            super::redemption_stuck_info(&failed),
            Some((TerminalFail, failed_at))
        );

        // BurnFailed → TerminalFail with failed_at. Exercises the exact
        // variant that motivated this PR (the original red-79631d72 /
        // red-742f9f3a incident).
        let burn_failed = BurnFailed {
            issuer_request_id: issuer.clone(),
            tokenization_request_id: tok_id,
            underlying,
            token,
            wallet: metadata.wallet,
            quantity: quantity.clone(),
            alpaca_quantity: quantity,
            dust_quantity: Quantity::default(),
            tx_hash,
            block_number,
            detected_at,
            called_at,
            alpaca_journal_completed_at: detected_at,
            error: "burn failed".to_string(),
            failed_at: burn_failed_at,
            tx_id: None,
            planned_burns: vec![],
        };
        assert_eq!(
            super::redemption_stuck_info(&burn_failed),
            Some((TerminalFail, burn_failed_at))
        );

        // Terminal/Unavailable do not appear.
        assert_eq!(super::redemption_stuck_info(&Unavailable), None);
        assert_eq!(
            super::redemption_stuck_info(&Completed {
                issuer_request_id: issuer.clone(),
                burn_tx_hash: tx_hash,
                block_number,
                completed_at: failed_at,
            }),
            None
        );
        assert_eq!(
            super::redemption_stuck_info(&Closed {
                issuer_request_id: issuer,
                reason: "x".to_string(),
                closed_at: failed_at,
            }),
            None
        );
    }

    #[test]
    fn stuck_redemption_entry_handles_in_progress_variants() {
        let metadata = test_metadata();
        let detected_at = metadata.detected_at;

        // Use distinct timestamps so the projection unambiguously selects
        // detected_entered_at (the post-reprocess clock) over detected_at.
        let detected_entered_at = detected_at + chrono::Duration::days(3);
        let detected_view = RedemptionView::Detected {
            issuer_request_id: metadata.issuer_request_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at,
            detected_entered_at,
        };

        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            detected_view,
            super::RedemptionHistorySummary::default(),
        )
        .expect("Detected view should produce a stuck entry");

        assert_eq!(entry.aggregate_type, AggregateKind::Redemption);
        assert_eq!(entry.state, "Detected");
        assert_eq!(entry.detail, "Waiting to call Alpaca");
        assert_eq!(entry.timestamp, detected_entered_at);
        assert_eq!(entry.tokenization_request_id, None);
        assert_eq!(entry.underlying, Some(metadata.underlying.clone()));
        assert_eq!(entry.quantity, Some(metadata.quantity.clone()));
        assert_eq!(entry.tx_hash, Some(metadata.detected_tx_hash));

        let called_at = Utc::now();
        let tok_id = TokenizationRequestId::new("tok-progress");
        let alpaca_called_view = RedemptionView::AlpacaCalled {
            issuer_request_id: metadata.issuer_request_id.clone(),
            tokenization_request_id: tok_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            alpaca_quantity: metadata.quantity.clone(),
            dust_quantity: Quantity::default(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at,
            called_at,
        };
        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            alpaca_called_view,
            super::RedemptionHistorySummary::default(),
        )
        .expect("AlpacaCalled view should produce a stuck entry");
        assert_eq!(entry.state, "AlpacaCalled");
        assert_eq!(entry.detail, "Waiting for Alpaca journal");
        assert_eq!(entry.timestamp, called_at);
        assert_eq!(entry.tokenization_request_id, Some(tok_id.clone()));

        // Use distinct timestamps so the projection unambiguously selects
        // burning_entered_at (the post-resume clock) over
        // alpaca_journal_completed_at.
        let journal_completed_at = Utc::now() - chrono::Duration::days(7);
        let burning_entered_at = Utc::now();
        let burning_view = RedemptionView::Burning {
            issuer_request_id: metadata.issuer_request_id.clone(),
            tokenization_request_id: tok_id.clone(),
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            alpaca_quantity: metadata.quantity.clone(),
            dust_quantity: Quantity::default(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at,
            called_at,
            alpaca_journal_completed_at: journal_completed_at,
            burning_entered_at,
        };
        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            burning_view,
            super::RedemptionHistorySummary::default(),
        )
        .expect("Burning view should produce a stuck entry");
        assert_eq!(entry.state, "Burning");
        // No prior submission recorded → pre-submission detail.
        assert_eq!(entry.detail, "Waiting for burn submission");
        assert_eq!(entry.timestamp, burning_entered_at);
        assert_eq!(entry.tokenization_request_id, Some(tok_id.clone()));

        // Same Burning view, but history shows a prior transaction
        // submission — the detail should flip to "Waiting for burn
        // confirmation" so operators don't see misleading "submission"
        // text on a post-submission row. BurnTxSubmitted leaves
        // the view in Burning, so this branch is the only signal.
        let burning_view_with_history = RedemptionView::Burning {
            issuer_request_id: metadata.issuer_request_id.clone(),
            tokenization_request_id: tok_id,
            underlying: metadata.underlying.clone(),
            token: metadata.token.clone(),
            wallet: metadata.wallet,
            quantity: metadata.quantity.clone(),
            alpaca_quantity: metadata.quantity.clone(),
            dust_quantity: Quantity::default(),
            tx_hash: metadata.detected_tx_hash,
            block_number: metadata.block_number,
            detected_at,
            called_at,
            alpaca_journal_completed_at: journal_completed_at,
            burning_entered_at,
        };

        let tx_id = TxId::random();
        let history_with_fb = super::RedemptionHistorySummary {
            tx_id: Some(tx_id.clone()),
            ..super::RedemptionHistorySummary::default()
        };
        let entry = super::stuck_redemption_entry(
            &metadata.issuer_request_id,
            burning_view_with_history,
            history_with_fb,
        )
        .expect("Burning view should produce a stuck entry");
        assert_eq!(entry.state, "Burning");
        assert_eq!(entry.detail, "Waiting for burn confirmation");
        assert_eq!(entry.tx_id, Some(tx_id.to_string()));
    }

    #[test]
    fn mint_view_summary_classifies_and_timestamps_each_variant() {
        use super::StuckClass::{InProgress, TerminalFail};
        use crate::mint::{ClientId, MintView, Network};

        let issuer_request_id = crate::mint::IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-stuck-1");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let quantity = crate::mint::Quantity::new(Decimal::from(100));
        let initiated_at = Utc::now() - chrono::Duration::hours(3);
        let journal_confirmed_at = Utc::now() - chrono::Duration::hours(2);
        let rejected_at = Utc::now() - chrono::Duration::hours(1);
        let minting_started_at = Utc::now() - chrono::Duration::minutes(45);
        let minted_at = Utc::now() - chrono::Duration::minutes(30);
        let failed_at = Utc::now() - chrono::Duration::minutes(15);

        let initiated = MintView::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
        };
        let s = super::mint_view_summary(&initiated)
            .expect("Initiated should produce a summary");
        assert_eq!(s.class, InProgress);
        assert_eq!(s.state, "Initiated");
        assert_eq!(s.detail, "Waiting for journal confirmation");
        assert_eq!(s.timestamp, initiated_at);
        assert_eq!(
            s.tokenization_request_id,
            Some(tokenization_request_id.clone())
        );

        let confirmed = MintView::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };
        let s = super::mint_view_summary(&confirmed)
            .expect("JournalConfirmed should produce a summary");
        assert_eq!(s.class, InProgress);
        assert_eq!(s.state, "JournalConfirmed");
        assert_eq!(s.detail, "Waiting for deposit");
        assert_eq!(s.timestamp, journal_confirmed_at);

        let rejected = MintView::JournalRejected {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
            reason: "Alpaca rejected".to_string(),
            rejected_at,
        };
        let s = super::mint_view_summary(&rejected)
            .expect("JournalRejected should produce a summary");
        assert_eq!(s.class, TerminalFail);
        assert_eq!(s.state, "JournalRejected");
        assert_eq!(s.detail, "Alpaca rejected");
        assert_eq!(s.timestamp, rejected_at);

        let minting = MintView::Minting {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at,
        };
        let s = super::mint_view_summary(&minting).unwrap();
        assert_eq!(s.class, InProgress);
        assert_eq!(s.state, "Minting");
        assert_eq!(s.detail, "Deposit in progress");
        assert_eq!(s.timestamp, minting_started_at);

        let callback_pending = MintView::CallbackPending {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: U256::from(1u64),
            shares_minted: U256::from(100u64),
            gas_used: None,
            block_number: 1,
            minted_at,
        };
        let s = super::mint_view_summary(&callback_pending).unwrap();
        assert_eq!(s.class, InProgress);
        assert_eq!(s.state, "CallbackPending");
        assert_eq!(s.detail, "Waiting for callback");
        assert_eq!(s.timestamp, minted_at);

        let minting_failed = MintView::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            error: "deposit failed".to_string(),
            failed_at,
        };
        let s = super::mint_view_summary(&minting_failed).unwrap();
        assert_eq!(s.class, TerminalFail);
        assert_eq!(s.state, "MintingFailed");
        assert_eq!(s.detail, "deposit failed");
        assert_eq!(s.timestamp, failed_at);

        // Terminal/NotFound do not produce a summary.
        assert!(super::mint_view_summary(&MintView::NotFound).is_none());
        assert!(
            super::mint_view_summary(&MintView::Completed {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("alp-completed"),
                quantity: crate::mint::Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                initiated_at: failed_at,
                journal_confirmed_at: failed_at,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                receipt_id: U256::from(2u64),
                shares_minted: U256::from(100u64),
                gas_used: None,
                block_number: 1,
                minted_at: failed_at,
                completed_at: failed_at,
            })
            .is_none(),
            "Completed must not produce a stuck summary"
        );
        assert!(
            super::mint_view_summary(&MintView::Closed {
                issuer_request_id,
                reason: "closed by admin".to_string(),
                closed_at: failed_at,
            })
            .is_none()
        );
    }

    /// Regression: an iter-3 fix branches the Burning detail string on
    /// `history.tx_id.is_some()`. Without the iter-4 fix to
    /// `redemption_history_summary`, a previously-failed transaction
    /// would survive across a `BurnResumed` and mislabel the freshly
    /// resumed (but not-yet-submitted) Burning row as "Waiting for burn
    /// confirmation".
    #[test]
    fn redemption_history_summary_clears_tx_id_on_burn_resumed() {
        use crate::redemption::{BurnExternalTxId, RedemptionEvent};

        let issuer = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let now = Utc::now();
        let tok_id = TokenizationRequestId::new("tok-resume-1");
        let tx_id = TxId::random();

        // Sequence: a full Burning attempt that failed during transaction
        // confirmation, then operator-initiated BurnResumed putting us
        // back into Burning with NO new submission yet.
        let events = vec![
            RedemptionEvent::Detected {
                issuer_request_id: issuer.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number: 1,
                detected_at: now,
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer.clone(),
                tokenization_request_id: tok_id.clone(),
                alpaca_quantity: quantity.clone(),
                dust_quantity: Quantity::default(),
                called_at: now,
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer.clone(),
                alpaca_journal_completed_at: now,
            },
            RedemptionEvent::BurnTxSubmitted {
                issuer_request_id: issuer.clone(),
                external_tx_id: BurnExternalTxId::from_string(format!(
                    "burn-{tx_hash}"
                )),
                tx_id: tx_id.clone(),
                planned_burns: vec![],
                submitted_at: now,
            },
            RedemptionEvent::BurningFailed {
                issuer_request_id: issuer.clone(),
                error: "tx failed".to_string(),
                failed_at: now,
                tx_id: Some(tx_id),
                planned_burns: vec![],
            },
            RedemptionEvent::RedemptionFailed {
                issuer_request_id: issuer.clone(),
                reason: "burn failed".to_string(),
                failed_at: now,
            },
            // Operator resumes the burn — no new BurnTxSubmitted
            // has happened yet.
            RedemptionEvent::BurnResumed {
                issuer_request_id: issuer,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number: 1,
                detected_at: now,
                tokenization_request_id: tok_id.clone(),
                alpaca_quantity: Quantity::default(),
                dust_quantity: Quantity::default(),
                called_at: now,
                alpaca_journal_completed_at: now,
                external_tx_id: None,
                resumed_at: now,
            },
        ];

        let summary = super::redemption_history_summary_from_events(events);

        // BurnResumed must clear the prior failed transaction id so the
        // stuck-row detail string reflects "Waiting for burn submission"
        // (not "Waiting for burn confirmation") for the new attempt.
        assert_eq!(
            summary.tx_id, None,
            "BurnResumed must clear prior tx_id from summary"
        );
        // tokenization_request_id is preserved (it doesn't reset).
        assert_eq!(summary.tokenization_request_id, Some(tok_id));
    }

    #[test]
    fn redemption_history_summary_exposes_persisted_burn_intent_hash() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let sendable_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let summary = super::redemption_history_summary_from_events([
            RedemptionEvent::BurnIntended {
                issuer_request_id: IssuerRedemptionRequestId::random(),
                sendable_tx: sendable_tx.clone(),
                planned_burns: vec![],
            },
        ]);

        assert_eq!(summary.tx_id, Some(TxId::Hash(sendable_tx.hash)));
    }
}
