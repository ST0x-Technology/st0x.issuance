use alloy::primitives::{Address, B256};
use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use rocket::http::{ContentType, Status};
use rocket::post;
use rocket::response::{self, Responder};
use rocket::serde::json::Json;
use sqlx::{Pool, Sqlite};
use st0x_issuance_dto::{MintAuthorizationRequest, MintAuthorizationResponse};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use super::ErrorResponse;
use crate::auth::InternalAuth;
use crate::config::VaultMode;
use crate::mint::recovery::enqueue_authorized_mint_recovery;
use crate::mint::view::{
    find_issuer_id_by_tokenization_request_id, find_mints_holding_nonce,
};
use crate::mint::{
    IssuerMintRequestId, Mint, MintCommand, MintError, TokenizationRequestId,
};
use crate::tokenized_asset::view::find_vault;
use crate::vault::{
    MintAuthorization, MintedLogQuery, NetworkVaultServices, VaultError,
    VaultService,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintAuthorizationApiError {
    #[error("No mint found for tokenization request {0}")]
    UnknownTokenizationRequest(TokenizationRequestId),

    #[error("Mint {0} is vault-direct; it never consumes an authorization")]
    VaultDirectMint(TokenizationRequestId),

    #[error("Authorization not acceptable: mint is in state {0}")]
    NotAcceptable(String),

    #[error("A different authorization is already recorded for this mint")]
    ConflictingAuthorization,

    #[error("Nonce {0} is already held by another mint for this recipient")]
    NonceHeldByAnotherMint(B256),

    #[error("The mint was modified concurrently; retry the delivery")]
    DeliveryRace,

    #[error("Invalid authorization: {0}")]
    InvalidAuthorization(VaultError),

    #[error("On-chain validation failed: {0}")]
    OnChainValidationFailed(VaultError),

    #[error("On-chain validation timed out")]
    OnChainValidationTimedOut,

    #[error("Internal error")]
    Internal,
}

/// Upper bound on the on-chain reads validating one authorization (up to
/// four sequential RPCs: `nonceUsed`, `mintAuthDigest`, `eth_getCode`, and
/// ERC-1271 `isValidSignature`). The HTTP provider has no request timeout of
/// its own, so without this deadline an unresponsive RPC node would hold the
/// liquidity bot's delivery request open indefinitely.
#[cfg(not(test))]
const ON_CHAIN_VALIDATION_TIMEOUT: Duration = Duration::from_secs(30);
const AUTHORIZATION_RECORD_ATTEMPTS: usize = 3;

/// Unit tests hang the mock validation forever to exercise the deadline, so
/// the bound must stay short enough to keep that test fast in real time. A
/// paused tokio clock is not an option here — auto-advance also fires sqlx's
/// pool-acquire timers, failing the request with `PoolTimedOut` before
/// validation is ever reached whenever an acquire has to wait.
///
/// It must also stay long enough for the tests that PARK a delivery inside
/// validation (`with_mint_auth_gate`) while they drive the mint through
/// several store commands: the deadline runs while the delivery is parked.
/// At 50ms those commands outran the deadline under full-suite load, so the
/// parked delivery answered `502` instead of the outcome under test. One
/// second leaves a wide margin for the parked work and still keeps the hang
/// test under a second of real time.
#[cfg(test)]
const ON_CHAIN_VALIDATION_TIMEOUT: Duration = Duration::from_secs(1);

impl<'r> Responder<'r, 'static> for MintAuthorizationApiError {
    fn respond_to(
        self,
        _req: &'r rocket::Request<'_>,
    ) -> response::Result<'static> {
        let (status, message) = match &self {
            Self::UnknownTokenizationRequest(_) => {
                (Status::NotFound, self.to_string())
            }
            Self::VaultDirectMint(_) | Self::InvalidAuthorization(_) => {
                (Status::UnprocessableEntity, self.to_string())
            }
            Self::NotAcceptable(_)
            | Self::ConflictingAuthorization
            | Self::NonceHeldByAnotherMint(_)
            | Self::DeliveryRace => (Status::Conflict, self.to_string()),
            // Unlike the deliberately descriptive 422s, the read-failure
            // body stays generic: the underlying `VaultError` can carry
            // transport/provider detail (RPC endpoints, connection errors)
            // that must not leave the process. The full error is already
            // ERROR-logged where this variant is constructed.
            Self::OnChainValidationFailed(_)
            | Self::OnChainValidationTimedOut => (
                Status::BadGateway,
                "On-chain validation is currently unavailable".to_string(),
            ),
            Self::Internal => (
                Status::InternalServerError,
                "Internal server error".to_string(),
            ),
        };

        let body = serde_json::to_string(&ErrorResponse { error: message })
            .map_err(|_| Status::InternalServerError)?;
        rocket::Response::build()
            .status(status)
            .header(ContentType::JSON)
            .sized_body(body.len(), std::io::Cursor::new(body))
            .ok()
    }
}

/// Receives the liquidity bot's `MintAuthV1` for one orchestrator-mode mint,
/// validates it on-chain against the mint's own persisted facts
/// (`token`, `to`, `amount` — the bot MUST mint with exactly the signed
/// values), and records it on the aggregate. Delivered out-of-band from the
/// Alpaca flow: Alpaca cannot carry the authorization, so it arrives on the
/// same internal channel the liquidity bot already uses for the asset-status
/// endpoint.
#[utoipa::path(
    post,
    path = "/internal/mints/{tokenization_request_id}/authorization",
    tag = "internal",
    params(
        ("tokenization_request_id" = String, Path,
            description = "Alpaca tokenization request id the mint was initiated with")
    ),
    request_body = MintAuthorizationRequest,
    responses(
        (status = 200, description = "Authorization validated and recorded (idempotent)",
            body = MintAuthorizationResponse),
        (status = 404, description = "No mint for this tokenization request"),
        (status = 409,
            description = "Conflicting authorization, a nonce another mint already holds for \
                this recipient (open, completed, or closed without releasing it — a \
                completed mint's nonce is refused here, not as a 422), or the mint \
                already signed its transaction"),
        (status = 422,
            description = "Vault-direct mint, invalid or malformed signer, \
                empty signature for an EOA recipient, or a nonce the chain reports \
                consumed by a mint this issuer does not hold"),
        (status = 502, description = "On-chain validation read failure")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, mint_store, pool, vault_services, request))]
#[post(
    "/internal/mints/<tokenization_request_id>/authorization",
    format = "json",
    data = "<request>"
)]
pub(crate) async fn authorize_mint(
    _auth: InternalAuth,
    tokenization_request_id: &str,
    mint_store: &rocket::State<Arc<Store<Mint>>>,
    pool: &rocket::State<Pool<Sqlite>>,
    apalis_pool: &rocket::State<ApalisSqlitePool>,
    vault_services: &rocket::State<NetworkVaultServices>,
    request: Json<MintAuthorizationRequest>,
) -> Result<Json<MintAuthorizationResponse>, MintAuthorizationApiError> {
    let tokenization_request_id =
        TokenizationRequestId(tokenization_request_id.to_string());
    let request = request.into_inner();

    let issuer_request_id = find_issuer_id_by_tokenization_request_id(
        pool.inner(),
        &tokenization_request_id,
    )
    .await
    .map_err(|err| {
        error!(target: "mint", error = %err,
            "Failed to look up mint by tokenization request id"
        );
        MintAuthorizationApiError::Internal
    })?
    .ok_or_else(|| {
        warn!(target: "mint",
            tokenization_request_id = %tokenization_request_id,
            "Authorization delivered for an unknown tokenization request"
        );
        MintAuthorizationApiError::UnknownTokenizationRequest(
            tokenization_request_id.clone(),
        )
    })?;

    let mint = mint_store
        .load(&issuer_request_id)
        .await
        .map_err(|err| {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %err, "Failed to load mint aggregate"
            );
            MintAuthorizationApiError::Internal
        })?
        .ok_or_else(|| {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint aggregate missing for known tokenization request"
            );
            MintAuthorizationApiError::Internal
        })?;

    // A mint with no live mode (`Closed`) is not a vault-direct mint — it
    // cannot accept an authorization in any mode, so it must not take the
    // vault-direct rejection below with its untrue cause.
    let Some(mode) = mint.mint_mode() else {
        return Err(MintAuthorizationApiError::NotAcceptable(
            mint.state_name().to_string(),
        ));
    };

    // The orchestrator address comes from the mint's own persisted
    // `mint_mode` anchor — never live config. A vault-direct mint has no
    // orchestrator to validate against and never consumes an authorization.
    let VaultMode::Orchestrator { address: orchestrator } = mode else {
        warn!(target: "mint", issuer_request_id = %issuer_request_id,
            "Authorization delivered for a vault-direct mint; rejecting"
        );
        return Err(MintAuthorizationApiError::VaultDirectMint(
            tokenization_request_id.clone(),
        ));
    };

    let (Some(network), Some(underlying), Some(to), Some(quantity)) =
        (mint.network(), mint.underlying(), mint.wallet(), mint.quantity())
    else {
        return Err(MintAuthorizationApiError::NotAcceptable(
            mint.state_name().to_string(),
        ));
    };

    let amount = quantity.to_u256_with_18_decimals().map_err(|err| {
        error!(target: "mint", issuer_request_id = %issuer_request_id,
            error = %err, "Persisted mint quantity failed share conversion"
        );
        MintAuthorizationApiError::Internal
    })?;

    let vault = find_vault(pool.inner(), underlying, &network)
        .await
        .map_err(|err| {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %err, "Vault lookup failed"
            );
            MintAuthorizationApiError::Internal
        })?
        .ok_or_else(|| {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                underlying = %underlying.as_str(),
                "No vault for the mint's asset"
            );
            MintAuthorizationApiError::Internal
        })?;

    let vault_service = vault_services.service(network).map_err(|err| {
        error!(target: "mint", issuer_request_id = %issuer_request_id,
            error = %err,
            "No vault service for the mint's network"
        );
        MintAuthorizationApiError::Internal
    })?;

    let authorization = MintAuthorization {
        nonce: request.nonce,
        signature: request.signature,
    };

    // An identical redelivery is the common retry case (a delivery whose
    // response was lost): answer from the recorded state without re-running
    // the on-chain validation — the recorded authorization already passed
    // it, and re-validating burns up to four RPC reads per retry.
    if mint.accepts_mint_authorization()
        && mint.mint_authorization() == Some(&authorization)
    {
        info!(target: "mint", issuer_request_id = %issuer_request_id,
            tokenization_request_id = %tokenization_request_id,
            "Identical mint authorization already recorded; redelivery is a \
             no-op"
        );
        // The first delivery's wake may have failed after the authorization
        // was recorded, leaving the mint parked; the bot's redelivery is the
        // caller-driven repair vector, and the enqueue's idempotency key
        // collapses duplicates when the first wake did land.
        wake_mint_recovery(
            pool.inner(),
            apalis_pool.inner(),
            &issuer_request_id,
        )
        .await;
        return Ok(Json(MintAuthorizationResponse {
            issuer_request_id: issuer_request_id.to_string(),
            status: "authorized".to_string(),
        }));
    }

    // A mint past the accepting states is refused on its STATE, ahead of
    // whatever else is also wrong with the delivery. The state is the cause
    // the bot can act on — it must stop delivering to this mint — whereas the
    // nonce refusal below would only send it back with a fresh nonce for the
    // same dead end. Refusing here also spares the validation RPCs. The
    // aggregate stays the authority (`AuthorizeMint` below re-checks, which
    // is what catches a mint that advances past this point mid-request); this
    // is the endpoint reporting the more actionable of two true causes. The
    // `stage` field is what tells the two refusals apart in the log.
    if !mint.accepts_mint_authorization() {
        warn!(target: "mint", issuer_request_id = %issuer_request_id,
            stage = NonceCheckStage::Gate.as_str(),
            current_state = %mint.state_name(),
            "Authorization rejected: mint state does not accept one"
        );
        return Err(MintAuthorizationApiError::NotAcceptable(
            mint.state_name().to_string(),
        ));
    }

    // Refusing here spares the validation RPCs below on a duplicate the
    // on-chain reads could never have caught anyway.
    refuse_duplicate_nonce(
        pool.inner(),
        &issuer_request_id,
        to,
        authorization.nonce,
        NonceCheckStage::Gate,
    )
    .await?;

    validate_on_chain(
        vault_service,
        &issuer_request_id,
        MintedLogQuery {
            orchestrator,
            token: vault,
            to,
            amount,
            nonce: authorization.nonce,
            // Validation reads on-chain state and never scans logs.
            lookback_blocks: None,
        },
        &authorization,
    )
    .await?;

    record_under_admission(
        mint_store.inner(),
        pool.inner(),
        &issuer_request_id,
        &tokenization_request_id,
        to,
        &authorization,
    )
    .await?;

    info!(target: "mint", issuer_request_id = %issuer_request_id,
        tokenization_request_id = %tokenization_request_id,
        "Mint authorization validated and recorded"
    );

    // The authorization's arrival is what unblocks a mint that deferred its
    // submission waiting for it, so wake recovery now.
    wake_mint_recovery(pool.inner(), apalis_pool.inner(), &issuer_request_id)
        .await;

    Ok(Json(MintAuthorizationResponse {
        issuer_request_id: issuer_request_id.to_string(),
        status: "authorized".to_string(),
    }))
}

/// Records a validated authorization under the admission lock.
///
/// The lock is held from the record-stage check through the send that claims
/// the pair. The check is a plain read of `mint_view` and the send is a commit
/// on a DIFFERENT aggregate from any competing delivery's, so nothing else
/// orders the two: without this guard two deliveries for two mints could both
/// read "no holder" and both record, which is the double claim the check
/// exists to refuse. Validation stays outside it — the guard covers only the
/// read-then-commit, not the RPC round-trips.
async fn record_under_admission(
    mint_store: &Arc<Store<Mint>>,
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
    tokenization_request_id: &TokenizationRequestId,
    to: Address,
    authorization: &MintAuthorization,
) -> Result<(), MintAuthorizationApiError> {
    let admission = AUTHORIZATION_ADMISSION.lock().await;
    let mut attempt = 1;
    loop {
        // Re-read on EVERY attempt, not once before the loop. The validation
        // before this spent RPC round-trips, and the conflict retry below
        // yields; both hand a concurrent delivery the chance to claim the
        // pair before this send lands.
        refuse_duplicate_nonce(
            pool,
            issuer_request_id,
            to,
            authorization.nonce,
            NonceCheckStage::Record,
        )
        .await?;

        let result = mint_store
            .send(
                issuer_request_id,
                MintCommand::AuthorizeMint {
                    issuer_request_id: issuer_request_id.clone(),
                    mint_authorization: authorization.clone(),
                },
            )
            .await;
        match result {
            Ok(()) => break,
            Err(AggregateError::AggregateConflict)
                if attempt < AUTHORIZATION_RECORD_ATTEMPTS =>
            {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    attempt,
                    max_attempts = AUTHORIZATION_RECORD_ATTEMPTS,
                    "Mint changed while recording authorization; retrying"
                );
                attempt += 1;
                tokio::task::yield_now().await;
            }
            Err(error) => {
                return Err(map_authorize_command_error(
                    issuer_request_id,
                    tokenization_request_id,
                    &error,
                ));
            }
        }
    }

    // Still under the lock, so a lost view write is caught before any other
    // delivery can read the view (see `confirm_claim_visible`).
    confirm_claim_visible(pool, issuer_request_id, to, authorization.nonce)
        .await?;
    drop(admission);
    Ok(())
}

/// Validates one delivery against the chain under a deadline, mapping the
/// outcomes onto actionable responses.
///
/// Validating on receipt makes a bad delivery a failure the liquidity bot
/// sees on this internal call, rather than a post-journal surprise at the
/// on-chain step. The typed rejections become a descriptive 422; anything
/// else is a read failure the caller should retry, and its detail stays in
/// the log rather than the response body.
async fn validate_on_chain(
    vault_service: &Arc<dyn VaultService>,
    issuer_request_id: &IssuerMintRequestId,
    query: MintedLogQuery,
    authorization: &MintAuthorization,
) -> Result<(), MintAuthorizationApiError> {
    timeout(
        ON_CHAIN_VALIDATION_TIMEOUT,
        vault_service.validate_mint_authorization(query, authorization),
    )
    .await
    .map_err(|_| {
        error!(target: "mint", issuer_request_id = %issuer_request_id,
            timeout_secs = ON_CHAIN_VALIDATION_TIMEOUT.as_secs(),
            "On-chain mint-authorization validation timed out"
        );
        MintAuthorizationApiError::OnChainValidationTimedOut
    })?
    .map_err(|err| match &err {
        VaultError::MintAuthSignerMismatch { .. }
        | VaultError::MintAuthNonceUsed { .. }
        | VaultError::MintAuthRejectedByContract { .. }
        | VaultError::MintAuthEmptySignatureForEoa { .. }
        | VaultError::Signature(_) => {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %err, "Rejected invalid mint authorization"
            );
            MintAuthorizationApiError::InvalidAuthorization(err)
        }
        _ => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %err,
                "On-chain mint-authorization validation failed"
            );
            MintAuthorizationApiError::OnChainValidationFailed(err)
        }
    })
}

/// Serializes the record-stage duplicate check with the `AuthorizeMint` send
/// that follows it. The issuer service has one writer process, so a static
/// mutex is the whole ordering: a delivery holds it from its last read of
/// the holders to the commit that makes it one, and no other delivery can
/// read in between. Modeled on the freeze admission in `crate::underlying`.
static AUTHORIZATION_ADMISSION: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

/// Where in the delivery a refusal was decided. The two are not
/// interchangeable — a `Gate` refusal answers what the endpoint saw when the
/// request arrived, while a `Record` refusal is a genuine concurrent change
/// (a pair claimed, or a mint advanced, while the request was in flight) —
/// and the log field is what tells an operator which happened, so the two
/// values are a closed set rather than a free string.
#[derive(Debug, Clone, Copy)]
enum NonceCheckStage {
    /// Before the on-chain validation, refusing cheaply on the state as
    /// loaded.
    Gate,
    /// At the record step, catching a change made meanwhile.
    Record,
}

impl NonceCheckStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Gate => "gate",
            Self::Record => "record",
        }
    }
}

/// Refuses an authorization whose `(recipient, nonce)` pair another mint
/// already holds — open, completed, or closed without releasing it.
///
/// The orchestrator keys `nonceUsed` on the recipient across every token, so
/// one pair yields at most one landing. Two mints holding it would both
/// full-match that landing and both complete — one AP's tokens for two
/// journaled positions. The on-chain `nonceUsed` read cannot catch this,
/// because at delivery time neither mint has landed and the pair still reads
/// free. This refusal is therefore also what keeps
/// `SubmitMintJob::recover_landed_orchestrator_mint`'s full-match
/// unambiguous.
///
/// Called at two `stage`s: `gate` refuses before the validation RPCs are spent
/// on a duplicate, and `record` re-reads before every send attempt, so a pair
/// claimed while the RPCs were in flight — or while a conflict retry yielded —
/// is still refused. The `record` read runs under `AUTHORIZATION_ADMISSION`,
/// held through the send, so two deliveries cannot both read "no holder" and
/// both commit. The `stage` field keeps the two cases apart for whoever reads
/// the log: a `gate` refusal is an ordinary reused nonce, while a `record`
/// refusal is a genuine concurrent race.
async fn refuse_duplicate_nonce(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
    recipient: Address,
    nonce: B256,
    stage: NonceCheckStage,
) -> Result<(), MintAuthorizationApiError> {
    let stage = stage.as_str();
    let holders = find_mints_holding_nonce(pool, recipient, nonce)
        .await
        .map_err(|err| {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                stage, error = %err,
                "Failed to check the authorization nonce against open mints"
            );
            MintAuthorizationApiError::Internal
        })?;

    // This guard keeps the count at one from here on, so anything higher is
    // a pre-existing collision that predates it — report the true size rather
    // than only the first holder, or an operator reading this line would
    // under-count the mints needing reconciliation.
    let others: Vec<&IssuerMintRequestId> =
        holders.iter().filter(|holder| *holder != issuer_request_id).collect();
    let Some(holder) = others.first() else {
        return Ok(());
    };

    warn!(target: "mint", issuer_request_id = %issuer_request_id,
        stage, held_by = %holder, holders = others.len(),
        recipient = %recipient, nonce = %nonce,
        "Rejected an authorization whose nonce another mint holds"
    );
    Err(MintAuthorizationApiError::NonceHeldByAnotherMint(nonce))
}

/// Checks that `mint_view` shows this mint holding the pair it just recorded.
///
/// `refuse_duplicate_nonce` reads holders only from `mint_view`, and the
/// projection that writes this claim logs and returns `Ok` when its load or
/// save fails or its retries run out. The event is committed either way, so a
/// lost view write leaves the claim invisible to the next delivery's check
/// until the next startup rebuild. This check cannot block that delivery —
/// the view is the only thing it reads — but it makes the lost write loud: a
/// missing claim is reported as an ERROR and answered `500`, so the liquidity
/// bot does not treat the delivery as a clean success.
async fn confirm_claim_visible(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
    recipient: Address,
    nonce: B256,
) -> Result<(), MintAuthorizationApiError> {
    let holders = find_mints_holding_nonce(pool, recipient, nonce)
        .await
        .map_err(|err| {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %err,
                "Failed to confirm the recorded authorization nonce in mint_view"
            );
            MintAuthorizationApiError::Internal
        })?;

    if holders.contains(issuer_request_id) {
        return Ok(());
    }

    error!(target: "mint", issuer_request_id = %issuer_request_id,
        recipient = %recipient, nonce = %nonce,
        "Authorization recorded, but mint_view does not show this mint \
         holding its nonce: the view write was lost, so the duplicate-nonce \
         guard cannot see this claim until the next startup rebuild. \
         Restart the service to rebuild mint_view"
    );
    Err(MintAuthorizationApiError::Internal)
}

/// Wakes mint recovery for a recorded authorization. Needed because the
/// periodic reconciler dedups against a terminal recovery row, so a mint
/// whose recovery job already exhausted its no-progress budget would stay
/// parked until the next restart without this kick. An enqueue failure is
/// tolerable — the authorization is recorded, the bot's redelivery re-drives
/// this wake, and the startup re-scan is the last-resort fallback.
async fn wake_mint_recovery(
    pool: &Pool<Sqlite>,
    apalis_pool: &ApalisSqlitePool,
    issuer_request_id: &IssuerMintRequestId,
) {
    if let Err(error) = enqueue_authorized_mint_recovery(
        pool,
        apalis_pool,
        issuer_request_id.clone(),
    )
    .await
    {
        warn!(target: "mint", issuer_request_id = %issuer_request_id,
            error = %error,
            "Failed to enqueue mint recovery after recording the authorization"
        );
    }
}

fn map_authorize_command_error(
    issuer_request_id: &IssuerMintRequestId,
    tokenization_request_id: &TokenizationRequestId,
    error: &AggregateError<LifecycleError<Mint>>,
) -> MintAuthorizationApiError {
    // A lost optimistic-lock race is the bot's cue to retry, not a server
    // fault — map it to 409 like the sibling endpoints do.
    if matches!(error, AggregateError::AggregateConflict) {
        warn!(target: "mint", issuer_request_id = %issuer_request_id,
            "Concurrent mint modification during authorization delivery"
        );
        return MintAuthorizationApiError::DeliveryRace;
    }

    if let AggregateError::UserError(LifecycleError::Apply(mint_error)) = error
    {
        match mint_error {
            MintError::AuthorizationForVaultDirectAsset { .. } => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    "Authorization rejected for vault-direct mint"
                );
                // The wire error names the tokenization id — the only mint
                // identifier the caller supplied and can correlate on — the
                // same id the endpoint's own mode-anchor rejection reports.
                return MintAuthorizationApiError::VaultDirectMint(
                    tokenization_request_id.clone(),
                );
            }
            MintError::ConflictingMintAuthorization => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    "Conflicting mint authorization rejected"
                );
                return MintAuthorizationApiError::ConflictingAuthorization;
            }
            // The endpoint already refused on the state it loaded, so reaching
            // this arm means the mint advanced past intent WHILE the delivery
            // was in flight — `stage` is what separates the two in the log.
            MintError::AuthorizationNotAcceptable { current_state } => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    stage = NonceCheckStage::Record.as_str(),
                    current_state = %current_state,
                    "Authorization rejected: mint state does not accept one"
                );
                return MintAuthorizationApiError::NotAcceptable(
                    current_state.clone(),
                );
            }
            _ => {}
        }
    }

    error!(target: "mint", issuer_request_id = %issuer_request_id,
        error = %error, "Failed to record mint authorization"
    );
    MintAuthorizationApiError::Internal
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, address};
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use rust_decimal::Decimal;
    use std::any::type_name;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{
        MintAuthorizationApiError, authorize_mint, confirm_claim_visible,
    };
    use crate::auth::FailedAuthRateLimiter;
    use crate::config::VaultMode;
    use crate::mint::api::test_utils::{TestHarness, test_config};
    use crate::mint::recovery::MintRecoveryJob;
    use crate::mint::view::find_mints_holding_nonce;
    use crate::mint::{
        ClientId, IssuerMintRequestId, Mint, MintCommand,
        MintFailureClassification, Network, Quantity, TokenSymbol,
        TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, logs_contain_at};
    use crate::tokenized_asset::{AssetKey, TokenizedAssetCommand};
    use crate::vault::mock::{MockMintAuthFailure, MockVaultService};
    use crate::vault::{
        MintAuthorization, NetworkVaultServices, PreparedMintTx, VaultService,
    };

    const ORCHESTRATOR: Address =
        address!("0x00000000000000000000000000000000000000aa");
    const RECIPIENT: Address =
        address!("0x1234567890abcdef1234567890abcdef12345678");
    const API_KEY: &str = "test-key-12345678901234567890123456";

    async fn seed_mint(
        harness: &TestHarness,
        tokenization_request_id: &str,
        mint_mode: VaultMode,
    ) -> IssuerMintRequestId {
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let network = Network::Base;
        let vault = address!("0x9999999999999999999999999999999999999999");
        // Adding twice is idempotent per test DB; ignore the duplicate error
        // when a test seeds two mints for the same asset.
        let _ = harness
            .asset_store
            .send(
                &AssetKey::new(underlying.clone(), network),
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tAAPL"),
                    network,
                    vault,
                },
            )
            .await;

        let issuer_request_id = IssuerMintRequestId::random();
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Initiate {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        tokenization_request_id,
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying,
                    token: TokenSymbol::new("tAAPL"),
                    network,
                    client_id: ClientId::new(),
                    wallet: RECIPIENT,
                    mint_mode,
                },
            )
            .await
            .expect("mint must initiate");
        issuer_request_id
    }

    fn authorization_rocket(
        harness: &TestHarness,
        vault_mock: Arc<dyn VaultService>,
    ) -> rocket::Rocket<rocket::Build> {
        let vault_services = NetworkVaultServices::with_single_vault(
            Network::Base,
            ANVIL_CHAIN_ID,
            vault_mock,
        );
        rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(harness.mint_store.clone())
            .manage(harness.pool.clone())
            .manage(harness.apalis_pool.clone())
            .manage(vault_services)
            .mount("/", routes![authorize_mint])
    }

    /// Delivers an authorization with the internal API key attached — the
    /// authenticated shape every test but the auth-rejection one uses.
    async fn deliver<'client>(
        client: &'client rocket::local::asynchronous::Client,
        tokenization_request_id: &str,
        nonce: B256,
        signature: &str,
    ) -> rocket::local::asynchronous::LocalResponse<'client> {
        delivery_request(client, tokenization_request_id, nonce, signature)
            .header(Header::new("X-API-KEY", API_KEY))
            .dispatch()
            .await
    }

    /// Delivers an authorization WITHOUT the API key, for asserting the
    /// authentication rejection.
    async fn deliver_without_key<'client>(
        client: &'client rocket::local::asynchronous::Client,
        tokenization_request_id: &str,
        nonce: B256,
        signature: &str,
    ) -> rocket::local::asynchronous::LocalResponse<'client> {
        delivery_request(client, tokenization_request_id, nonce, signature)
            .dispatch()
            .await
    }

    fn delivery_request<'client>(
        client: &'client rocket::local::asynchronous::Client,
        tokenization_request_id: &str,
        nonce: B256,
        signature: &str,
    ) -> rocket::local::asynchronous::LocalRequest<'client> {
        client
            .post(format!(
                "/internal/mints/{tokenization_request_id}/authorization"
            ))
            .header(ContentType::JSON)
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(
                serde_json::json!({
                    "nonce": nonce,
                    "signature": signature,
                })
                .to_string(),
            )
    }

    /// A valid delivery (including an EMPTY "0x" signature — the bridge
    /// recipient shape) is validated, recorded, and idempotent on redelivery.
    #[traced_test]
    #[tokio::test]
    async fn records_valid_authorization_and_redelivery_is_idempotent() {
        let harness = TestHarness::new().await;
        let issuer_request_id = seed_mint(
            &harness,
            "tok-auth-1",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let nonce = B256::repeat_byte(0x07);
        for _ in 0..2 {
            let response = deliver(&client, "tok-auth-1", nonce, "0x").await;
            assert_eq!(response.status(), Status::Ok);
            let body: serde_json::Value =
                response.into_json().await.expect("response must be JSON");
            assert_eq!(
                body["issuer_request_id"],
                issuer_request_id.to_string()
            );
            assert_eq!(body["status"], "authorized");
        }

        // Validated once, recorded once: the identical redelivery answers
        // from the recorded state without re-running the on-chain reads.
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 1);
        let mint = harness
            .mint_store
            .load(&issuer_request_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(matches!(
            mint,
            Mint::Initiated {
                mint_authorization: Some(authorization),
                ..
            } if authorization.nonce == nonce
                && authorization.signature.is_empty()
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Mint authorization validated and recorded", "tok-auth-1"]
        ));

        // The recorded authorization must wake recovery: a mint whose
        // deferred submission already exhausted its recovery job would
        // otherwise stay parked until restart (the reconciler dedups
        // against terminal rows). The redelivery short-circuit re-drives
        // the wake (repairing a first delivery whose enqueue failed), and
        // the idempotency key collapses the duplicate down to one row.
        let recovery_jobs: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key = ?
            ",
        )
        .bind(type_name::<MintRecoveryJob>())
        .bind(format!("{issuer_request_id}:authorization"))
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            recovery_jobs, 1,
            "recording the authorization must enqueue exactly one mint \
             recovery job"
        );
    }

    /// The bot can redeliver the SAME authorization to the SAME mint while
    /// the first delivery is still inside its on-chain reads. Both clear the
    /// identical-redelivery short-circuit (nothing is recorded yet), the
    /// second records, and the first's record-stage check then finds the
    /// pair held — by the very mint it is delivering to. That holder must be
    /// excluded, or the mint's own nonce would be refused as a duplicate.
    #[traced_test]
    #[tokio::test]
    async fn an_identical_concurrent_redelivery_is_not_a_duplicate() {
        let harness = TestHarness::new().await;
        let issuer_request_id = seed_mint(
            &harness,
            "tok-redeliver-race",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let vault_mock =
            Arc::new(MockVaultService::new_success().with_mint_auth_gate());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = Arc::new(
            rocket::local::asynchronous::Client::tracked(rocket)
                .await
                .expect("valid rocket"),
        );

        let nonce = B256::repeat_byte(0x1b);
        let parked_client = Arc::clone(&client);
        let parked = tokio::spawn(async move {
            deliver(&parked_client, "tok-redeliver-race", nonce, "0x")
                .await
                .status()
        });

        // The first delivery is parked in validation; the identical second
        // one runs through and records the pair on this same mint.
        vault_mock.wait_for_mint_auth_validation().await;
        assert_eq!(
            deliver(&client, "tok-redeliver-race", nonce, "0x").await.status(),
            Status::Ok
        );

        vault_mock.release_mint_auth_validation();
        assert_eq!(
            parked.await.expect("parked delivery must finish"),
            Status::Ok,
            "the mint's own recorded pair must not refuse its redelivery"
        );

        let mint = harness
            .mint_store
            .load(&issuer_request_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(
            matches!(
                &mint,
                Mint::Initiated {
                    mint_authorization: Some(authorization),
                    ..
                } if authorization.nonce == nonce
            ),
            "the authorization must be recorded once, got {mint:?}"
        );
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 2);
        assert!(
            !logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Rejected an authorization whose nonce another mint holds",
                    &issuer_request_id.to_string(),
                ]
            ),
            "a mint must never be refused its own nonce"
        );
    }

    /// `nonceUsed` is keyed on `(recipient, nonce)` across all tokens, so two
    /// open mints holding one pair would both full-match the single landing
    /// it can produce — one AP's tokens for two journaled positions. The
    /// on-chain read cannot see this (neither has landed yet), so the second
    /// delivery is refused here, before any RPC is spent on it.
    #[traced_test]
    #[tokio::test]
    async fn rejects_a_nonce_another_open_mint_already_holds() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        let holder_id =
            seed_mint(&harness, "tok-nonce-dup-1", orchestrator).await;
        let duplicate_id =
            seed_mint(&harness, "tok-nonce-dup-2", orchestrator).await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let nonce = B256::repeat_byte(0x11);
        assert_eq!(
            deliver(&client, "tok-nonce-dup-1", nonce, "0x").await.status(),
            Status::Ok
        );

        let response = deliver(&client, "tok-nonce-dup-2", nonce, "0x").await;
        assert_eq!(response.status(), Status::Conflict);
        let body: serde_json::Value =
            response.into_json().await.expect("response must be JSON");
        assert!(
            body["error"]
                .as_str()
                .expect("error must be a string")
                .contains(&nonce.to_string()),
            "the refusal must name the rejected nonce, got {body}"
        );

        // Refused before the on-chain reads: only the first delivery spent
        // the validation RPCs.
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 1);
        let duplicate = harness
            .mint_store
            .load(&duplicate_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(
            matches!(
                duplicate,
                Mint::Initiated { mint_authorization: None, .. }
            ),
            "the refused mint must record no authorization, got {duplicate:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Rejected an authorization whose nonce another mint holds",
                "gate",
                &holder_id.to_string(),
                &duplicate_id.to_string(),
            ]
        ));
    }

    /// The gate check runs before the validation RPCs, so a pair claimed
    /// WHILE they are in flight would slip past it. The re-check immediately
    /// before recording is what refuses that delivery — held here by parking
    /// the first request inside its validation until the second has recorded
    /// the pair.
    #[traced_test]
    #[tokio::test]
    async fn rejects_a_nonce_claimed_while_validation_was_in_flight() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        let parked_id =
            seed_mint(&harness, "tok-nonce-race-1", orchestrator).await;
        let winner_id =
            seed_mint(&harness, "tok-nonce-race-2", orchestrator).await;
        let vault_mock =
            Arc::new(MockVaultService::new_success().with_mint_auth_gate());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = Arc::new(
            rocket::local::asynchronous::Client::tracked(rocket)
                .await
                .expect("valid rocket"),
        );

        let nonce = B256::repeat_byte(0x13);
        let parked_client = Arc::clone(&client);
        let parked = tokio::spawn(async move {
            deliver(&parked_client, "tok-nonce-race-1", nonce, "0x")
                .await
                .status()
        });

        // The first delivery is now past its gate check and inside the
        // validation, exactly where a concurrent claim is invisible to it.
        vault_mock.wait_for_mint_auth_validation().await;
        assert_eq!(
            deliver(&client, "tok-nonce-race-2", nonce, "0x").await.status(),
            Status::Ok
        );

        vault_mock.release_mint_auth_validation();
        assert_eq!(
            parked.await.expect("parked delivery must finish"),
            Status::Conflict,
            "the pair was claimed during validation; recording must be \
             refused"
        );

        let parked_mint = harness
            .mint_store
            .load(&parked_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(
            matches!(
                parked_mint,
                Mint::Initiated { mint_authorization: None, .. }
            ),
            "the refused mint must record no authorization, got \
             {parked_mint:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Rejected an authorization whose nonce another mint holds",
                "record",
                &parked_id.to_string(),
                &winner_id.to_string(),
            ]
        ));
    }

    /// Deliveries for DIFFERENT mints carrying one pair, all in flight at
    /// once — the shape two workers picking the same next nonce produce. The
    /// record-stage read alone does not order them: each is a plain read of
    /// `mint_view`, and each send commits a different aggregate, so without
    /// the admission guard several could read "no holder" and all record.
    /// With it, exactly one delivery claims the pair, however many race.
    #[traced_test]
    #[tokio::test]
    async fn concurrent_deliveries_of_one_pair_admit_exactly_one() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        const RACERS: usize = 4;
        for index in 0..RACERS {
            seed_mint(
                &harness,
                &format!("tok-nonce-storm-{index}"),
                orchestrator,
            )
            .await;
        }
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock);
        let client = Arc::new(
            rocket::local::asynchronous::Client::tracked(rocket)
                .await
                .expect("valid rocket"),
        );

        let nonce = B256::repeat_byte(0x1a);
        let deliveries: Vec<_> = (0..RACERS)
            .map(|index| {
                let client = Arc::clone(&client);
                tokio::spawn(async move {
                    deliver(
                        &client,
                        &format!("tok-nonce-storm-{index}"),
                        nonce,
                        "0x",
                    )
                    .await
                    .status()
                })
            })
            .collect();
        let mut statuses = Vec::with_capacity(RACERS);
        for delivery in deliveries {
            statuses.push(delivery.await.expect("delivery must finish"));
        }

        let admitted =
            statuses.iter().filter(|status| **status == Status::Ok).count();
        let refused = statuses
            .iter()
            .filter(|status| **status == Status::Conflict)
            .count();
        assert_eq!(
            (admitted, refused),
            (1, RACERS - 1),
            "exactly one delivery may claim the pair, got {statuses:?}"
        );
        let holders = find_mints_holding_nonce(&harness.pool, RECIPIENT, nonce)
            .await
            .expect("holder lookup must succeed");
        assert_eq!(holders.len(), 1, "one pair, one holder, got {holders:?}");
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Rejected an authorization whose nonce another mint holds",
                &nonce.to_string(),
            ]
        ));
    }

    /// Data written before this guard existed can hold one pair across
    /// several open mints. The refusal must report how many, not just the
    /// first: an operator reading one holder would under-count the mints
    /// needing reconciliation. Seeded through the aggregate, since the
    /// endpoint itself can no longer produce this state.
    #[traced_test]
    #[tokio::test]
    async fn a_pre_existing_collision_reports_every_holder() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        let nonce = B256::repeat_byte(0x16);

        for index in 0..2 {
            let issuer_request_id = seed_mint(
                &harness,
                &format!("tok-legacy-dup-{index}"),
                orchestrator,
            )
            .await;
            harness
                .mint_store
                .send(
                    &issuer_request_id,
                    MintCommand::AuthorizeMint {
                        issuer_request_id: issuer_request_id.clone(),
                        mint_authorization: MintAuthorization {
                            nonce,
                            signature: Bytes::new(),
                        },
                    },
                )
                .await
                .expect("legacy authorization must record");
        }

        let newcomer_id =
            seed_mint(&harness, "tok-legacy-dup-new", orchestrator).await;

        // Precondition: the legacy state the endpoint can no longer create.
        let seeded = find_mints_holding_nonce(&harness.pool, RECIPIENT, nonce)
            .await
            .expect("holder lookup must succeed");
        assert_eq!(
            seeded.len(),
            2,
            "both seeded mints must hold the pair, got {seeded:?}"
        );

        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        assert_eq!(
            deliver(&client, "tok-legacy-dup-new", nonce, "0x").await.status(),
            Status::Conflict
        );
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 0);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Rejected an authorization whose nonce another mint holds",
                &newcomer_id.to_string(),
                "holders=2",
            ]
        ));
    }

    /// A mint whose journal was rejected never submitted anything, so its
    /// nonce was never consumed on-chain and is genuinely free again. The
    /// guard must let a later mint take the pair rather than burning it.
    #[traced_test]
    #[tokio::test]
    async fn a_nonce_released_by_its_mint_is_deliverable_again() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        let holder_id =
            seed_mint(&harness, "tok-nonce-free-1", orchestrator).await;
        let successor_id =
            seed_mint(&harness, "tok-nonce-free-2", orchestrator).await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let nonce = B256::repeat_byte(0x12);
        assert_eq!(
            deliver(&client, "tok-nonce-free-1", nonce, "0x").await.status(),
            Status::Ok
        );

        harness
            .mint_store
            .send(
                &holder_id,
                MintCommand::RejectJournal {
                    issuer_request_id: holder_id.clone(),
                    reason: "journal failed".to_string(),
                },
            )
            .await
            .expect("journal rejection must record");

        assert_eq!(
            deliver(&client, "tok-nonce-free-2", nonce, "0x").await.status(),
            Status::Ok
        );
        let successor = harness
            .mint_store
            .load(&successor_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(
            matches!(
                &successor,
                Mint::Initiated { mint_authorization: Some(authorization), .. }
                    if authorization.nonce == nonce
            ),
            "the successor must take the released pair, got {successor:?}"
        );
        // The observable worth pinning: the successor's delivery was
        // RECORDED, not refused.
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Mint authorization validated and recorded",
                &successor_id.to_string(),
            ]
        ));
    }

    /// Closing is NOT proof the nonce is free. `handle_close_mint` demands
    /// only that the operator echo the persisted transaction hash, which
    /// acknowledges an UNRESOLVED transaction rather than a dead one. If the
    /// close released the pair, a successor could take it, the acknowledged
    /// transaction could then land, and that successor's `Minted`-log
    /// full-match would complete it too — one AP's tokens for two journaled
    /// positions, which is the outcome this guard exists to prevent. The
    /// release half of that rule is the next test.
    #[traced_test]
    #[tokio::test]
    async fn an_ordinary_close_keeps_its_nonce() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        let holder_id =
            seed_mint(&harness, "tok-nonce-closed-1", orchestrator).await;
        let successor_id =
            seed_mint(&harness, "tok-nonce-closed-2", orchestrator).await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let nonce = B256::repeat_byte(0x17);
        assert_eq!(
            deliver(&client, "tok-nonce-closed-1", nonce, "0x").await.status(),
            Status::Ok
        );

        // An ordinary close: no nonce acknowledgement, so nothing states the
        // nonce is free.
        harness
            .mint_store
            .send(
                &holder_id,
                MintCommand::CloseMint {
                    issuer_request_id: holder_id.clone(),
                    reason: "operator close".to_string(),
                    acknowledged_unresolved_mint_tx_hash: None,
                    acknowledged_unresolved_mint_nonce: None,
                },
            )
            .await
            .expect("close must record");

        let response =
            deliver(&client, "tok-nonce-closed-2", nonce, "0x").await;
        assert_eq!(response.status(), Status::Conflict);
        let successor = harness
            .mint_store
            .load(&successor_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(
            matches!(
                successor,
                Mint::Initiated { mint_authorization: None, .. }
            ),
            "a closed mint's pair must not pass to a successor"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Rejected an authorization whose nonce another mint holds",
                &holder_id.to_string(),
                &successor_id.to_string(),
            ]
        ));
    }

    /// The one close that DOES release, driven through the command: the
    /// nonce acknowledgement is accepted only from
    /// `MintingFailed { NonceReplayUnresolved }`, and a close carrying it
    /// records the operator's claim that the nonce is free. The guard must
    /// then let a successor take the pair — this is the deliberate hand-off
    /// path, the one place a pair passes on after a submission.
    #[traced_test]
    #[tokio::test]
    async fn a_close_acknowledging_the_nonce_releases_it() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        let holder_id =
            seed_mint(&harness, "tok-nonce-acked-1", orchestrator).await;
        let successor_id =
            seed_mint(&harness, "tok-nonce-acked-2", orchestrator).await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let nonce = B256::repeat_byte(0x1c);
        assert_eq!(
            deliver(&client, "tok-nonce-acked-1", nonce, "0x").await.status(),
            Status::Ok
        );

        // The holder submits, then parks under the one verdict the nonce
        // acknowledgement applies to.
        for command in [
            MintCommand::ConfirmJournal {
                issuer_request_id: holder_id.clone(),
            },
            MintCommand::Deposit { issuer_request_id: holder_id.clone() },
            MintCommand::RecordTxIntended {
                issuer_request_id: holder_id.clone(),
                prepared_tx: PreparedMintTx::valid_for_test(
                    1,
                    format!("mint-{holder_id}"),
                ),
            },
            MintCommand::RecordMintFailed {
                issuer_request_id: holder_id.clone(),
                error: "nonce consumed with no log at the pair".to_string(),
                classification:
                    MintFailureClassification::NonceReplayUnresolved,
            },
        ] {
            harness
                .mint_store
                .send(&holder_id, command)
                .await
                .expect("holder must reach the unresolved replay");
        }
        harness
            .mint_store
            .send(
                &holder_id,
                MintCommand::CloseMint {
                    issuer_request_id: holder_id.clone(),
                    reason: "verified free on an independent chain view"
                        .to_string(),
                    acknowledged_unresolved_mint_tx_hash: None,
                    acknowledged_unresolved_mint_nonce: Some(nonce),
                },
            )
            .await
            .expect("the acknowledging close must record");

        let holders = find_mints_holding_nonce(&harness.pool, RECIPIENT, nonce)
            .await
            .expect("holder lookup must succeed");
        assert!(
            holders.is_empty(),
            "an acknowledged close must release the pair, got {holders:?}"
        );

        assert_eq!(
            deliver(&client, "tok-nonce-acked-2", nonce, "0x").await.status(),
            Status::Ok
        );
        let successor = harness
            .mint_store
            .load(&successor_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(
            matches!(
                &successor,
                Mint::Initiated {
                    mint_authorization: Some(authorization),
                    ..
                } if authorization.nonce == nonce
            ),
            "the successor must take the released pair, got {successor:?}"
        );
        assert!(
            !logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Rejected an authorization whose nonce another mint holds",
                    &successor_id.to_string(),
                ]
            ),
            "a released pair must not be refused to its successor"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn unknown_tokenization_request_is_not_found() {
        let harness = TestHarness::new().await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let response =
            deliver(&client, "tok-nope", B256::repeat_byte(0x07), "0x").await;

        assert_eq!(response.status(), Status::NotFound);
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 0);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["unknown tokenization request", "tok-nope"]
        ));
    }

    /// A vault-direct mint never consumes an authorization: rejected
    /// actionably, never stored, and never validated on-chain.
    #[traced_test]
    #[tokio::test]
    async fn vault_direct_mint_delivery_is_rejected_actionably() {
        let harness = TestHarness::new().await;
        let issuer_request_id =
            seed_mint(&harness, "tok-direct-1", VaultMode::VaultDirect).await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let response =
            deliver(&client, "tok-direct-1", B256::repeat_byte(0x07), "0x")
                .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
        let body: serde_json::Value =
            response.into_json().await.expect("error body must be JSON");
        assert!(
            body["error"]
                .as_str()
                .expect("error string")
                .contains("vault-direct"),
            "the rejection must name the cause, got {body}"
        );
        assert_eq!(
            vault_mock.mint_auth_validation_call_count(),
            0,
            "a vault-direct delivery must never reach on-chain validation"
        );
        let mint = harness
            .mint_store
            .load(&issuer_request_id)
            .await
            .expect("aggregate must load")
            .expect("aggregate must exist");
        assert!(
            matches!(mint, Mint::Initiated { mint_authorization: None, .. }),
            "a rejected authorization must never be stored"
        );
        assert!(logs_contain_at!(tracing::Level::WARN, &["vault-direct mint"]));
    }

    /// A closed mint is unreachable by delivery: `Closed` carries no
    /// tokenization request id, so the lookup 404s — and must NOT surface
    /// the vault-direct 422, whose cause would be untrue for it. (The
    /// endpoint's no-live-mode guard covers the load-after-close race the
    /// lookup cannot.)
    #[tokio::test]
    async fn closed_mint_delivery_is_not_found_never_vault_direct() {
        let harness = TestHarness::new().await;
        let issuer_request_id = seed_mint(
            &harness,
            "tok-closed-1",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::CloseMint {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: "operator close".to_string(),
                    acknowledged_unresolved_mint_tx_hash: None,
                    acknowledged_unresolved_mint_nonce: None,
                },
            )
            .await
            .expect("mint must close");

        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let response =
            deliver(&client, "tok-closed-1", B256::repeat_byte(0x07), "0x")
                .await;

        assert_eq!(response.status(), Status::NotFound);
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 0);
    }

    /// Invalid authorizations (wrong signer / consumed nonce) map to
    /// distinct actionable 422s on this internal call — not a post-journal
    /// surprise at the on-chain step.
    #[traced_test]
    #[tokio::test]
    async fn invalid_authorization_is_unprocessable() {
        for (failure, expected_snippet) in [
            (MockMintAuthFailure::SignerMismatch, "recovered"),
            (MockMintAuthFailure::NonceUsed, "already consumed"),
            (MockMintAuthFailure::EmptySignatureForEoa, "has no code"),
        ] {
            let harness = TestHarness::new().await;
            let issuer_request_id = seed_mint(
                &harness,
                "tok-invalid-1",
                VaultMode::Orchestrator { address: ORCHESTRATOR },
            )
            .await;
            let vault_mock = Arc::new(
                MockVaultService::new_success().with_mint_auth_failure(failure),
            );
            let rocket = authorization_rocket(&harness, vault_mock.clone());
            let client = rocket::local::asynchronous::Client::tracked(rocket)
                .await
                .expect("valid rocket");

            let response = deliver(
                &client,
                "tok-invalid-1",
                B256::repeat_byte(0x07),
                "0xaaaa",
            )
            .await;

            assert_eq!(response.status(), Status::UnprocessableEntity);
            let body: serde_json::Value =
                response.into_json().await.expect("error body must be JSON");
            assert!(
                body["error"]
                    .as_str()
                    .expect("error string")
                    .contains(expected_snippet),
                "{failure:?} must surface its cause, got {body}"
            );
            let mint = harness
                .mint_store
                .load(&issuer_request_id)
                .await
                .expect("aggregate must load")
                .expect("aggregate must exist");
            assert!(
                matches!(
                    mint,
                    Mint::Initiated { mint_authorization: None, .. }
                ),
                "an invalid authorization must never be stored"
            );
            assert!(
                logs_contain_at!(
                    tracing::Level::WARN,
                    &["Rejected invalid mint authorization", expected_snippet]
                ),
                "{failure:?} must WARN with its cause"
            );
        }
    }

    /// A conflicting second authorization is rejected with 409 — the nonce
    /// can never be swapped mid-flight.
    #[traced_test]
    #[tokio::test]
    async fn conflicting_authorization_is_a_conflict() {
        let harness = TestHarness::new().await;
        seed_mint(
            &harness,
            "tok-conflict-1",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let first =
            deliver(&client, "tok-conflict-1", B256::repeat_byte(0x07), "0x")
                .await;
        assert_eq!(first.status(), Status::Ok);

        let second =
            deliver(&client, "tok-conflict-1", B256::repeat_byte(0x08), "0x")
                .await;
        assert_eq!(second.status(), Status::Conflict);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Conflicting mint authorization rejected"]
        ));
    }

    /// A non-authorization on-chain read failure surfaces as 502 with a
    /// generic body — never a 422 rejection, and never the underlying
    /// transport/provider detail.
    #[traced_test]
    #[tokio::test]
    async fn on_chain_read_failure_is_a_bad_gateway() {
        let harness = TestHarness::new().await;
        seed_mint(
            &harness,
            "tok-502-1",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_mint_auth_failure(MockMintAuthFailure::ReadFailed),
        );
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let response =
            deliver(&client, "tok-502-1", B256::repeat_byte(0x07), "0x").await;

        assert_eq!(response.status(), Status::BadGateway);
        let body: serde_json::Value =
            response.into_json().await.expect("error body must be JSON");
        assert_eq!(
            body["error"], "On-chain validation is currently unavailable",
            "the 502 body must stay generic, got {body}"
        );
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 1);
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["On-chain mint-authorization validation failed"]
        ));
    }

    /// Once the mint is past intent, its signed transaction already binds a
    /// nonce: a late delivery is a 409 through the HTTP layer, naming the
    /// state. The mint was already past intent when the delivery arrived, so
    /// the endpoint's own check answers it — `stage=gate`.
    #[traced_test]
    #[tokio::test]
    async fn post_intent_delivery_is_a_conflict() {
        let harness = TestHarness::new().await;
        let issuer_request_id = seed_mint(
            &harness,
            "tok-late-1",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;

        let authorization = MintAuthorization {
            nonce: B256::repeat_byte(0x07),
            signature: Bytes::new(),
        };
        for command in [
            MintCommand::AuthorizeMint {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: authorization,
            },
            MintCommand::ConfirmJournal {
                issuer_request_id: issuer_request_id.clone(),
            },
            MintCommand::Deposit {
                issuer_request_id: issuer_request_id.clone(),
            },
            MintCommand::RecordTxIntended {
                issuer_request_id: issuer_request_id.clone(),
                prepared_tx: PreparedMintTx::valid_for_test(
                    1,
                    format!("mint-{issuer_request_id}"),
                ),
            },
        ] {
            harness
                .mint_store
                .send(&issuer_request_id, command)
                .await
                .expect("mint must advance to intent");
        }

        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let response =
            deliver(&client, "tok-late-1", B256::repeat_byte(0x07), "0x").await;

        assert_eq!(response.status(), Status::Conflict);
        let body: serde_json::Value =
            response.into_json().await.expect("error body must be JSON");
        assert!(
            body["error"]
                .as_str()
                .expect("error string")
                .contains("MintIntended"),
            "the conflict must name the rejecting state, got {body}"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Authorization rejected: mint state does not accept one",
                "stage=\"gate\"",
                &issuer_request_id.to_string(),
                "MintIntended",
            ]
        ));
    }

    /// A mint past the accepting states that ALSO carries a duplicate nonce
    /// has two true refusals. The state is the one the bot can act on — it
    /// must stop delivering to this mint — whereas a nonce refusal would only
    /// send it back with a fresh nonce for the same dead end. The state wins,
    /// and it wins before the validation RPCs are spent.
    #[traced_test]
    #[tokio::test]
    async fn post_intent_delivery_is_refused_on_state_not_nonce() {
        let harness = TestHarness::new().await;
        let orchestrator = VaultMode::Orchestrator { address: ORCHESTRATOR };
        seed_mint(&harness, "tok-order-holder", orchestrator).await;
        let intended_id =
            seed_mint(&harness, "tok-order-intended", orchestrator).await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        // The holder takes the pair through the endpoint and stays open.
        let nonce = B256::repeat_byte(0x14);
        assert_eq!(
            deliver(&client, "tok-order-holder", nonce, "0x").await.status(),
            Status::Ok
        );

        // The second mint advances past intent under a nonce of its own.
        for command in [
            MintCommand::AuthorizeMint {
                issuer_request_id: intended_id.clone(),
                mint_authorization: MintAuthorization {
                    nonce: B256::repeat_byte(0x15),
                    signature: Bytes::new(),
                },
            },
            MintCommand::ConfirmJournal {
                issuer_request_id: intended_id.clone(),
            },
            MintCommand::Deposit { issuer_request_id: intended_id.clone() },
            MintCommand::RecordTxIntended {
                issuer_request_id: intended_id.clone(),
                prepared_tx: PreparedMintTx::valid_for_test(
                    1,
                    format!("mint-{intended_id}"),
                ),
            },
        ] {
            harness
                .mint_store
                .send(&intended_id, command)
                .await
                .expect("mint must advance to intent");
        }

        let response =
            deliver(&client, "tok-order-intended", nonce, "0x").await;

        assert_eq!(response.status(), Status::Conflict);
        let body: serde_json::Value =
            response.into_json().await.expect("error body must be JSON");
        let message = body["error"].as_str().expect("error string");
        assert!(
            message.contains("MintIntended"),
            "the state must be the reported cause, got {body}"
        );
        assert!(
            !message.contains(&nonce.to_string()),
            "the nonce refusal must not pre-empt the state, got {body}"
        );

        // Only the holder's delivery reached the chain.
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 1);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Authorization rejected: mint state does not accept one",
                "stage=\"gate\"",
                &intended_id.to_string(),
                "MintIntended",
            ]
        ));
    }

    /// The endpoint's state check answers a mint that was already past intent
    /// when the delivery arrived. A mint that advances past intent WHILE the
    /// delivery is inside its on-chain reads is caught by the aggregate
    /// instead, through the `AuthorizationNotAcceptable` mapping, and the log
    /// must tell the two apart: `stage=gate` is the endpoint's early answer,
    /// `stage=record` the aggregate's.
    #[traced_test]
    #[tokio::test]
    async fn a_mint_advancing_past_intent_mid_delivery_is_refused_at_record() {
        let harness = TestHarness::new().await;
        let issuer_request_id = seed_mint(
            &harness,
            "tok-late-race",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let vault_mock =
            Arc::new(MockVaultService::new_success().with_mint_auth_gate());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = Arc::new(
            rocket::local::asynchronous::Client::tracked(rocket)
                .await
                .expect("valid rocket"),
        );

        let parked_client = Arc::clone(&client);
        let parked = tokio::spawn(async move {
            let response = deliver(
                &parked_client,
                "tok-late-race",
                B256::repeat_byte(0x18),
                "0x",
            )
            .await;
            let status = response.status();
            let body: serde_json::Value =
                response.into_json().await.expect("error body must be JSON");
            (status, body)
        });
        vault_mock.wait_for_mint_auth_validation().await;

        // The mint advances past intent under a nonce of its own while the
        // delivery is parked in validation.
        for command in [
            MintCommand::AuthorizeMint {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: MintAuthorization {
                    nonce: B256::repeat_byte(0x19),
                    signature: Bytes::new(),
                },
            },
            MintCommand::ConfirmJournal {
                issuer_request_id: issuer_request_id.clone(),
            },
            MintCommand::Deposit {
                issuer_request_id: issuer_request_id.clone(),
            },
            MintCommand::RecordTxIntended {
                issuer_request_id: issuer_request_id.clone(),
                prepared_tx: PreparedMintTx::valid_for_test(
                    1,
                    format!("mint-{issuer_request_id}"),
                ),
            },
        ] {
            harness
                .mint_store
                .send(&issuer_request_id, command)
                .await
                .expect("mint must advance to intent");
        }

        vault_mock.release_mint_auth_validation();
        let (status, body) = parked.await.expect("parked delivery must finish");
        assert_eq!(status, Status::Conflict, "body: {body}");
        assert!(
            body["error"]
                .as_str()
                .expect("error string")
                .contains("MintIntended"),
            "the conflict must name the rejecting state, got {body}"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Authorization rejected: mint state does not accept one",
                "stage=\"record\"",
                &issuer_request_id.to_string(),
                "MintIntended",
            ]
        ));
        assert!(
            !logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Authorization rejected: mint state does not accept one",
                    "stage=\"gate\"",
                    &issuer_request_id.to_string(),
                ]
            ),
            "the endpoint saw an accepting state; only the aggregate refused"
        );
    }

    /// The duplicate-nonce guard reads only `mint_view`, and the projection
    /// that writes a claim swallows its own failures. So after recording,
    /// the claim must be confirmed visible there: a visible claim passes, and
    /// a lost view write is refused with an ERROR rather than reported as a
    /// clean success.
    #[traced_test]
    #[tokio::test]
    async fn a_recorded_claim_missing_from_the_view_is_refused_loudly() {
        let harness = TestHarness::new().await;
        let issuer_request_id = seed_mint(
            &harness,
            "tok-lost-view",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let nonce = B256::repeat_byte(0x21);
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::AuthorizeMint {
                    issuer_request_id: issuer_request_id.clone(),
                    mint_authorization: MintAuthorization {
                        nonce,
                        signature: Bytes::new(),
                    },
                },
            )
            .await
            .expect("authorization must record");

        confirm_claim_visible(
            &harness.pool,
            &issuer_request_id,
            RECIPIENT,
            nonce,
        )
        .await
        .expect("a claim the view received must pass");

        // Simulate the lost view write: the event stays committed, the
        // view row does not show it.
        sqlx::query("DELETE FROM mint_view WHERE view_id = ?")
            .bind(issuer_request_id.to_string())
            .execute(&harness.pool)
            .await
            .expect("view row must delete");

        let refused = confirm_claim_visible(
            &harness.pool,
            &issuer_request_id,
            RECIPIENT,
            nonce,
        )
        .await;

        assert!(
            matches!(refused, Err(MintAuthorizationApiError::Internal)),
            "a claim missing from the view must be refused, got {refused:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &[
                "mint_view does not show this mint holding its nonce",
                &issuer_request_id.to_string(),
            ]
        ));
    }

    /// An unresponsive provider must fail the delivery with a 502 at the
    /// validation deadline, not hold the request open indefinitely. The
    /// mock's validation hangs forever; the test-profile deadline is
    /// milliseconds, so the timeout fires in real time.
    #[traced_test]
    #[tokio::test]
    async fn unresponsive_provider_times_out_as_bad_gateway() {
        let harness = TestHarness::new().await;
        seed_mint(
            &harness,
            "tok-hang-1",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let rocket = authorization_rocket(
            &harness,
            Arc::new(MockVaultService::new_success().with_mint_auth_hang()),
        );
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let response =
            deliver(&client, "tok-hang-1", B256::repeat_byte(0x07), "0x").await;

        assert_eq!(response.status(), Status::BadGateway);
        let body: serde_json::Value =
            response.into_json().await.expect("error body must be JSON");
        assert_eq!(
            body["error"], "On-chain validation is currently unavailable",
            "the timeout body must stay generic like other 502s"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["validation timed out", "timeout_secs"]
        ));
    }

    #[tokio::test]
    async fn missing_api_key_is_unauthorized() {
        let harness = TestHarness::new().await;
        seed_mint(
            &harness,
            "tok-noauth-1",
            VaultMode::Orchestrator { address: ORCHESTRATOR },
        )
        .await;
        let vault_mock = Arc::new(MockVaultService::new_success());
        let rocket = authorization_rocket(&harness, vault_mock.clone());
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket");

        let response = deliver_without_key(
            &client,
            "tok-noauth-1",
            B256::repeat_byte(0x07),
            "0x",
        )
        .await;

        assert_eq!(response.status(), Status::Unauthorized);
        assert_eq!(vault_mock.mint_auth_validation_call_count(), 0);
    }
}
