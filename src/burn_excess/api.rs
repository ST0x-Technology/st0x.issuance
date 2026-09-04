//! Breakglass HTTP route for the internal-path excess-share burn.
//!
//! The external path stays offline (`issuer burn-excess external`) until the
//! transfer poller can be paused from a running service; internal never touches
//! the poller. The engine self-gates on wallet quiescence (an unresolved
//! mint/redemption burn intent on the network refuses with `Conflict`), so this
//! is safe to run against a live service.

use alloy::primitives::{B256, U256};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{State, post};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::{error, warn};

use super::cli::{parse_shares, run_burn_excess_request};
use super::engine::{BurnExcessEngineError, BurnExcessRequest};
use super::proof::BurnExcessMode;
use crate::auth::BreakglassOps;
use crate::config::Config;
use crate::mint::IssuerMintRequestId;
use crate::tokenized_asset::Network;

/// Operator inputs for an internal-path excess burn, mirroring the
/// `burn-excess internal` CLI flags. `shares` is an 18-decimal fixed-point
/// amount as a decimal string (e.g. `"0.750"`).
#[derive(Deserialize)]
pub(crate) struct BurnExcessInternalRequest {
    issuer_request_id: IssuerMintRequestId,
    deposit_tx_hash: B256,
    receipt_id: U256,
    shares: String,
    reason: String,
    #[serde(default)]
    incident_id: Option<String>,
    network: Network,
    chain_id: u64,
    /// Perform the mutation (sign/broadcast/exclusion). Default is a dry-run
    /// that proves the plan and logs it without touching chain or state.
    #[serde(default)]
    execute: bool,
    /// Close a dead intended/submitted stream instead of burning.
    #[serde(default)]
    close: bool,
}

#[derive(Serialize)]
pub(crate) struct BurnExcessResponse {
    /// Whether a mutation was requested (`execute`); a dry-run reports `false`.
    executed: bool,
}

/// Breakglass-tier internal excess-share burn. Above debug because it signs and
/// broadcasts a burn on the issuer wallet. The operator's `execute` field is the
/// confirmation, so the engine's interactive prompt is auto-approved here.
#[post(
    "/ops/breakglass/burn-excess/internal",
    format = "json",
    data = "<body>"
)]
pub(crate) async fn burn_excess_internal_ops(
    _auth: BreakglassOps,
    pool: &State<Pool<Sqlite>>,
    config: &State<Config>,
    body: Json<BurnExcessInternalRequest>,
) -> Result<Json<BurnExcessResponse>, Status> {
    let body = body.into_inner();

    let shares = parse_shares(&body.shares).map_err(|error| {
        warn!(target: "admin", error = %error, "Invalid burn-excess shares");
        Status::UnprocessableEntity
    })?;

    if body.chain_id != body.network.chain_id() {
        warn!(target: "admin", network = %body.network, chain_id = body.chain_id,
            "burn-excess chain_id does not match network"
        );
        return Err(Status::UnprocessableEntity);
    }

    let request = BurnExcessRequest {
        mode: BurnExcessMode::Internal,
        issuer_request_id: body.issuer_request_id,
        deposit_tx_hash: body.deposit_tx_hash,
        funding_tx_hash: None,
        receipt_id: body.receipt_id,
        shares,
        reason: body.reason,
        incident_id: body.incident_id,
        network: body.network,
        chain_id: body.chain_id,
        execute: body.execute,
        close: body.close,
    };
    let executed = request.execute;

    run_burn_excess_request(
        pool.inner(),
        &config.signer,
        request,
        |_: &str| Ok::<bool, std::io::Error>(true),
    )
    .await
    .map_err(|error| {
        error!(target: "admin", %error, "burn-excess (internal) failed");
        error
            .downcast_ref::<BurnExcessEngineError>()
            .map_or(Status::InternalServerError, map_burn_excess_error)
    })?;

    Ok(Json(BurnExcessResponse { executed }))
}

/// Maps a burn-excess failure to an HTTP status. An absent mint is a 404; a
/// wallet not quiesced is a 409; a bad proof or input is a 422; an on-chain/RPC
/// fault is a 502; anything else (including a burn that landed but whose
/// bookkeeping failed) is a 500 for operator intervention.
const fn map_burn_excess_error(error: &BurnExcessEngineError) -> Status {
    use BurnExcessEngineError::{
        AmbiguousDepositTx, AmbiguousShareTransferOut, Contract,
        DeadBurnIntent, DepositTxInvalid, FundingTxInvalid, MintMissingAsset,
        MintNetworkMismatch, MintNotFound, Proof, Provider,
        UnresolvedExcessBurnIntent, UnresolvedSignerIntent, Vault,
        VaultNotListed,
    };

    match error {
        MintNotFound { .. } => Status::NotFound,
        UnresolvedSignerIntent { .. } | UnresolvedExcessBurnIntent => {
            Status::Conflict
        }
        Proof(_)
        | MintMissingAsset { .. }
        | MintNetworkMismatch { .. }
        | VaultNotListed { .. }
        | DepositTxInvalid { .. }
        | AmbiguousDepositTx { .. }
        | AmbiguousShareTransferOut { .. }
        | FundingTxInvalid { .. }
        | DeadBurnIntent { .. } => Status::UnprocessableEntity,
        Provider(_) | Contract(_) | Vault(_) => Status::BadGateway,
        _ => Status::InternalServerError,
    }
}
