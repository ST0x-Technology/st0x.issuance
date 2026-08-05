use alloy::primitives::{B256, Bytes};
use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use rocket::http::{ContentType, Status};
use rocket::post;
use rocket::response::{self, Responder};
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{error, info, warn};

use super::ErrorResponse;
use crate::auth::InternalAuth;
use crate::config::VaultMode;
use crate::mint::recovery::enqueue_scheduled_mint_recovery;
use crate::mint::view::find_issuer_id_by_tokenization_request_id;
use crate::mint::{
    IssuerMintRequestId, Mint, MintCommand, MintError, TokenizationRequestId,
};
use crate::tokenized_asset::view::find_vault;
use crate::vault::{
    MintAuthorization, MintedLogQuery, NetworkVaultServices, VaultError,
};

/// Wire shape of the liquidity bot's mint-authorization delivery
/// (RAI-1243). The signature is opaque bytes; `"0x"` (empty) is valid input
/// for recipients authorized via the orchestrator's `authorizeMint`
/// callback.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(crate) struct MintAuthorizationRequest {
    /// Recipient-chosen random 32-byte nonce, hex-encoded.
    #[schema(value_type = String)]
    pub(crate) nonce: B256,
    /// EIP-712 `MintAuthV1` signature over `(token, to, amount, nonce)` by
    /// the recipient wallet key, hex-encoded; may be `"0x"`.
    #[schema(value_type = String)]
    pub(crate) signature: Bytes,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(crate) struct MintAuthorizationResponse {
    #[schema(value_type = String)]
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) status: String,
}

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

/// Unit tests hang the mock validation forever to exercise the deadline; a
/// millisecond-scale bound keeps that test fast in real time. A paused
/// tokio clock is not an option here — auto-advance also fires sqlx's
/// pool-acquire timers, failing the request with `PoolTimedOut` before
/// validation is ever reached whenever an acquire has to wait.
#[cfg(test)]
const ON_CHAIN_VALIDATION_TIMEOUT: Duration = Duration::from_millis(50);

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
            description = "Conflicting authorization, or the mint already signed its transaction"),
        (status = 422,
            description = "Vault-direct mint, invalid or malformed signer, \
                empty signature for an EOA recipient, or consumed nonce"),
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
            issuer_request_id,
            status: "authorized".to_string(),
        }));
    }

    // Validate on receipt so a bad delivery is an actionable failure on this
    // internal call, not a post-journal surprise at the on-chain step.
    timeout(
        ON_CHAIN_VALIDATION_TIMEOUT,
        vault_service.validate_mint_authorization(
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
        ),
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
    })?;

    mint_store
        .send(
            &issuer_request_id,
            MintCommand::AuthorizeMint {
                issuer_request_id: issuer_request_id.clone(),
                mint_authorization: authorization,
            },
        )
        .await
        .map_err(|err| {
            map_authorize_command_error(
                &issuer_request_id,
                &tokenization_request_id,
                &err,
            )
        })?;

    info!(target: "mint", issuer_request_id = %issuer_request_id,
        tokenization_request_id = %tokenization_request_id,
        "Mint authorization validated and recorded"
    );

    // The authorization's arrival is what unblocks a mint that deferred its
    // submission waiting for it, so wake recovery now.
    wake_mint_recovery(pool.inner(), apalis_pool.inner(), &issuer_request_id)
        .await;

    Ok(Json(MintAuthorizationResponse {
        issuer_request_id,
        status: "authorized".to_string(),
    }))
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
    if let Err(error) = enqueue_scheduled_mint_recovery(
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
            MintError::AuthorizationNotAcceptable { current_state } => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
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

    use super::authorize_mint;
    use crate::auth::FailedAuthRateLimiter;
    use crate::config::VaultMode;
    use crate::mint::api::test_utils::{TestHarness, test_config};
    use crate::mint::recovery::MintRecoveryJob;
    use crate::mint::{
        ClientId, IssuerMintRequestId, Mint, MintCommand, Network, Quantity,
        TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
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
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            recovery_jobs, 1,
            "recording the authorization must enqueue exactly one mint \
             recovery job"
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
    }

    /// Once the mint is past intent, its signed transaction already binds a
    /// nonce: a late delivery is a 409 through the HTTP layer, naming the
    /// state.
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
