//! Breakglass HTTP routes for the internal- and external-path excess-share
//! burns.
//!
//! `internal` never touches the redemption transfer poller. `external` burns
//! shares that arrived through a funding Transfer the live poller would
//! otherwise read as an AP redemption, so it runs in two steps: `expect-funding`
//! records the Transfer the stream expects before the operator broadcasts it,
//! and the poller holds a matching log until `external` excludes it (and any
//! other log of that shape until the burn completes). Both burn
//! routes sign through the running service's vault service, so the burn shares
//! the wallet lock and nonce manager every live mint and redemption burn uses,
//! and both self-gate on wallet quiescence under that lock (an unresolved
//! mint/redemption burn intent on the network refuses with `Conflict`).

use alloy::primitives::B256;
use alloy::providers::ProviderBuilder;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{State, post};
use serde::Serialize;
use sqlx::{Pool, Sqlite};
use st0x_issuance_dto::{
    BurnExcessCommon, BurnExcessExpectFundingRequest,
    BurnExcessExternalRequest, BurnExcessInternalRequest,
};
use std::time::Duration;
use tracing::{error, warn};

use super::engine::{
    BurnExcessEngineError, BurnExcessOutcome, BurnExcessRequest, PollerGuard,
    run_burn_excess,
};
use super::proof::BurnExcessMode;
use crate::auth::BreakglassOps;
use crate::chain::{ChainConfig, rpc_client};
use crate::config::Config;
use crate::mint::IssuerMintRequestId;
use crate::vault::NetworkVaultServices;

/// Caps how long a burn-excess route runs the engine. The burn routes hold the
/// network's wallet lock across signing; a hung provider or vault call must not
/// block live signing until the process restarts. On elapse the run is
/// abandoned, releasing the wallet lock, and the route returns 504. Chosen
/// above the expected broadcast-and-confirm window.
const BURN_EXCESS_TIMEOUT: Duration = Duration::from_secs(120);

/// Builds the engine request from a route body. The operator-supplied chain
/// id is validated against the network's configured chain entry by
/// `resolve_chain`, which can see the config; this function cannot, so it is
/// infallible. Every route runs beside the live poller, so a Path B stream
/// relies on its funding expectation rather than a stopped service.
fn into_request(
    common: BurnExcessCommon,
    mode: BurnExcessMode,
    funding_tx_hash: Option<B256>,
) -> BurnExcessRequest {
    BurnExcessRequest {
        mode,
        issuer_request_id: IssuerMintRequestId::new(common.issuer_request_id),
        deposit_tx_hash: common.deposit_tx_hash,
        funding_tx_hash,
        receipt_id: common.receipt_id,
        shares: common.shares.to_u256(),
        reason: common.reason,
        incident_id: common.incident_id,
        network: common.network,
        chain_id: common.chain_id,
        execute: common.execute,
        close: common.close,
        poller_guard: PollerGuard::FundingExpected,
    }
}

#[derive(Serialize)]
pub(crate) struct BurnExcessResponse {
    /// Whether a mutation was requested (`execute`); a dry-run reports `false`.
    executed: bool,
    /// The proven plan or terminal report: what a dry-run would burn, or what
    /// an execute committed.
    outcome: BurnExcessOutcome,
}

/// Resolves and validates the configured chain before a route performs any
/// side effect.
fn resolve_chain<'config>(
    config: &'config Config,
    request: &BurnExcessRequest,
    path: &'static str,
) -> Result<&'config ChainConfig, Status> {
    let chain = config
        .chains
        .iter()
        .find(|candidate| candidate.network == request.network)
        .ok_or_else(|| {
            error!(target: "admin", network = %request.network, path,
                issuer_request_id = %request.issuer_request_id,
                "No chain configuration for network"
            );
            Status::InternalServerError
        })?;

    if request.chain_id != chain.chain_id {
        warn!(target: "admin", network = %request.network, path,
            issuer_request_id = %request.issuer_request_id,
            chain_id = request.chain_id, configured = chain.chain_id,
            "burn-excess chain_id does not match the configured chain"
        );
        return Err(Status::UnprocessableEntity);
    }

    Ok(chain)
}

/// Runs the engine request for both routes through the running service's
/// per-network vault service, so the burn takes the same wallet lock and nonce
/// manager as every live mint and redemption burn, and maps its outcome or
/// error onto the HTTP response. `path` names the route for the failure log.
///
/// The read provider's RPC endpoint comes from the service's startup-verified
/// chain configuration, never from a request-time environment read, so a
/// deployment that supplies its RPC as a flag rather than an env var serves
/// these routes too. That configuration was RPC-verified when the service
/// started, so no per-request chain-id round trip is needed either.
async fn run_burn_excess_ops(
    pool: &Pool<Sqlite>,
    config: &Config,
    chain: &ChainConfig,
    vault_services: &NetworkVaultServices,
    request: BurnExcessRequest,
    path: &'static str,
) -> Result<Json<BurnExcessResponse>, Status> {
    // On every line below, so the client's Cloud Logging link (which searches
    // for the command's issuer request id) finds the run's setup failure,
    // plan, and outcome.
    let issuer_request_id = request.issuer_request_id.clone();
    let vault_service =
        vault_services.service(request.network).map_err(|error| {
            warn!(target: "admin", %error, path,
                issuer_request_id = %issuer_request_id, "burn-excess refused"
            );
            Status::UnprocessableEntity
        })?;
    let issuer_wallet = config.signer.address().map_err(|error| {
        error!(target: "admin", %error, path,
            issuer_request_id = %issuer_request_id,
            "burn-excess signer address unavailable"
        );
        Status::InternalServerError
    })?;
    let rpc = rpc_client(&chain.rpc).map_err(|error| {
        error!(target: "admin", %error, path,
            issuer_request_id = %issuer_request_id,
            "burn-excess RPC unavailable"
        );
        Status::InternalServerError
    })?;
    let read_provider = ProviderBuilder::new().connect_client(rpc);
    let executed = request.execute;

    // Bound the run so a hung provider or vault call cannot hold the wallet
    // lock indefinitely.
    // On elapse the future is dropped, releasing the wallet lock; the burn
    // stream is persisted before broadcast, so a re-invocation resumes it.
    let outcome = tokio::time::timeout(
        BURN_EXCESS_TIMEOUT,
        run_burn_excess(
            pool,
            vault_service.as_ref(),
            &read_provider,
            issuer_wallet,
            request,
            |plan: &str| {
                // The CLI operator reads this plan before authorizing the
                // burn; here the request's `execute` stood in for that answer,
                // so the plan is recorded instead, next to reason and incident.
                warn!(target: "admin", plan, path,
                    issuer_request_id = %issuer_request_id,
                    "burn-excess auto-approving operator-confirmed plan"
                );
                Ok::<bool, std::io::Error>(true)
            },
        ),
    )
    .await
    .map_err(|_| {
        error!(target: "admin", path, issuer_request_id = %issuer_request_id,
            "burn-excess timed out"
        );
        Status::GatewayTimeout
    })?
    .map_err(|error| {
        error!(target: "admin", %error, path,
            issuer_request_id = %issuer_request_id, "burn-excess failed"
        );
        map_burn_excess_error(&error)
    })?;

    Ok(Json(BurnExcessResponse { executed, outcome }))
}

/// Breakglass-tier internal excess-share burn. Above debug because it signs and
/// broadcasts a burn on the issuer wallet. The operator's `execute` field is the
/// confirmation, so the engine's interactive prompt is auto-approved here.
#[post(
    "/ops/breakglass/burn-excess/internal",
    format = "json",
    data = "<body>"
)]
#[tracing::instrument(
    target = "auth",
    name = "operator",
    skip_all,
    fields(subject = %auth.0)
)]
pub(crate) async fn burn_excess_internal_ops(
    auth: BreakglassOps,
    pool: &State<Pool<Sqlite>>,
    config: &State<Config>,
    vault_services: &State<NetworkVaultServices>,
    body: Json<BurnExcessInternalRequest>,
) -> Result<Json<BurnExcessResponse>, Status> {
    let request =
        into_request(body.into_inner().common, BurnExcessMode::Internal, None);
    let chain = resolve_chain(config.inner(), &request, "internal")?;
    run_burn_excess_ops(
        pool.inner(),
        config.inner(),
        chain,
        vault_services,
        request,
        "internal",
    )
    .await
}

/// Breakglass-tier first step of the external-path burn, run before the
/// funding Transfer is broadcast. Proves the deposit bind and, on `execute`,
/// records the funding Transfer the stream expects, so the live poller holds
/// it rather than open a Redemption for shares being burned. With `close`, it
/// closes the stream instead and releases the hold. Signs nothing.
#[post(
    "/ops/breakglass/burn-excess/expect-funding",
    format = "json",
    data = "<body>"
)]
#[tracing::instrument(
    target = "auth",
    name = "operator",
    skip_all,
    fields(subject = %auth.0)
)]
pub(crate) async fn burn_excess_expect_funding_ops(
    auth: BreakglassOps,
    pool: &State<Pool<Sqlite>>,
    config: &State<Config>,
    vault_services: &State<NetworkVaultServices>,
    body: Json<BurnExcessExpectFundingRequest>,
) -> Result<Json<BurnExcessResponse>, Status> {
    let request = into_request(
        body.into_inner().common,
        BurnExcessMode::ExpectFunding,
        None,
    );
    let chain = resolve_chain(config.inner(), &request, "expect-funding")?;
    run_burn_excess_ops(
        pool.inner(),
        config.inner(),
        chain,
        vault_services,
        request,
        "expect-funding",
    )
    .await
}

/// Breakglass-tier external-path excess-share burn. Unlike `internal`, the
/// excess shares arrived via an on-chain Transfer into the wallet the
/// redemption transfer poller watches. The stream must already be expecting
/// that Transfer (`expect-funding`, before it was broadcast), so the poller
/// has held it; a stream that is not is refused with `Conflict`. The run then
/// records the exclusion before it releases the expectation, so the poller
/// skips the funding log rather than redeem it.
#[post(
    "/ops/breakglass/burn-excess/external",
    format = "json",
    data = "<body>"
)]
#[tracing::instrument(
    target = "auth",
    name = "operator",
    skip_all,
    fields(subject = %auth.0)
)]
pub(crate) async fn burn_excess_external_ops(
    auth: BreakglassOps,
    pool: &State<Pool<Sqlite>>,
    config: &State<Config>,
    vault_services: &State<NetworkVaultServices>,
    body: Json<BurnExcessExternalRequest>,
) -> Result<Json<BurnExcessResponse>, Status> {
    let body = body.into_inner();
    let funding_tx_hash = body.funding_tx_hash;
    let request = into_request(
        body.common,
        BurnExcessMode::External,
        Some(funding_tx_hash),
    );
    let chain = resolve_chain(config.inner(), &request, "external")?;
    run_burn_excess_ops(
        pool.inner(),
        config.inner(),
        chain,
        vault_services,
        request,
        "external",
    )
    .await
}

/// Maps a burn-excess failure to an HTTP status. An absent mint is a 404; a
/// wallet not quiesced, an external run without its funding expectation, or
/// another stream's expectation open on the vault is a 409; a bad proof or
/// input is a 422; an on-chain/RPC fault is a 502; anything else (including a
/// burn that landed but whose bookkeeping failed) is a 500 for operator
/// intervention.
const fn map_burn_excess_error(error: &BurnExcessEngineError) -> Status {
    use BurnExcessEngineError::{
        AmbiguousDepositTx, AmbiguousShareTransferOut, ChainBehindProvenPlan,
        Contract, DeadBurnIntent, DepositTxInvalid, FundingNotExpected,
        FundingTxInvalid, HeldTransferReceiptMissing, MintMissingAsset,
        MintNetworkMismatch, MintNotFound, Proof, Provider,
        UnresolvedExcessBurnIntent, UnresolvedSignerIntent, Vault,
        VaultNotListed,
    };

    match error {
        MintNotFound { .. } => Status::NotFound,
        UnresolvedSignerIntent { .. }
        | UnresolvedExcessBurnIntent
        | FundingNotExpected => Status::Conflict,
        Proof(_)
        | MintMissingAsset { .. }
        | MintNetworkMismatch { .. }
        | VaultNotListed { .. }
        | DepositTxInvalid { .. }
        | AmbiguousDepositTx { .. }
        | AmbiguousShareTransferOut { .. }
        | FundingTxInvalid { .. }
        | DeadBurnIntent { .. } => Status::UnprocessableEntity,
        Provider(_)
        | Contract(_)
        | Vault(_)
        | HeldTransferReceiptMissing { .. }
        | ChainBehindProvenPlan { .. } => Status::BadGateway,
        _ => Status::InternalServerError,
    }
}
