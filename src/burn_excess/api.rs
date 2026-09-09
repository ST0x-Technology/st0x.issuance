//! Breakglass HTTP route for the internal-path excess-share burn.
//!
//! The external path stays offline (`issuer burn-excess external`) until the
//! transfer poller can be paused from a running service; internal never touches
//! the poller. The route signs through the running service's vault service, so
//! the burn shares the wallet lock and nonce manager every live mint and
//! redemption burn uses, and the engine self-gates on wallet quiescence under
//! that lock (an unresolved mint/redemption burn intent on the network refuses
//! with `Conflict`), so this is safe to run against a live service.

use alloy::primitives::{B256, U256};
use alloy::providers::ProviderBuilder;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{State, post};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::{error, warn};

use super::cli::parse_shares;
use super::engine::{
    BurnExcessEngineError, BurnExcessOutcome, BurnExcessRequest,
    run_burn_excess,
};
use super::proof::BurnExcessMode;
use crate::auth::BreakglassOps;
use crate::config::{Config, configured_rpc_url, wss_to_http};
use crate::mint::IssuerMintRequestId;
use crate::tokenized_asset::Network;
use crate::vault::NetworkVaultServices;

/// An 18-decimal fixed-point share amount parsed from its decimal-string wire
/// form at deserialize time, so an invalid or over-precise quantity is refused
/// before the handler runs. Private inner; the [`Deserialize`] impl (via
/// [`parse_shares`]) is the only constructor, so a `Shares` that exists is a
/// valid on-chain amount.
pub(crate) struct Shares(U256);

impl Shares {
    const fn into_u256(self) -> U256 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Shares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        parse_shares(&raw).map(Self).map_err(de::Error::custom)
    }
}

/// Operator inputs for an internal-path excess burn, mirroring the
/// `burn-excess internal` CLI flags. `shares` is an 18-decimal fixed-point
/// amount as a decimal string (e.g. `"0.750"`).
#[derive(Deserialize)]
pub(crate) struct BurnExcessInternalRequest {
    issuer_request_id: IssuerMintRequestId,
    deposit_tx_hash: B256,
    receipt_id: U256,
    shares: Shares,
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
    /// The proven plan or terminal report: what a dry-run would burn, or what
    /// an execute committed.
    outcome: BurnExcessOutcome,
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
    let body = body.into_inner();

    if body.chain_id != body.network.chain_id() {
        warn!(target: "admin", network = %body.network, chain_id = body.chain_id,
            "burn-excess chain_id does not match network"
        );
        return Err(Status::UnprocessableEntity);
    }

    let vault_service =
        vault_services.service(body.network).map_err(|error| {
            warn!(target: "admin", %error, "burn-excess refused");
            Status::UnprocessableEntity
        })?;
    let issuer_wallet = config.signer.address().map_err(|error| {
        error!(target: "admin", %error, "burn-excess signer address unavailable");
        Status::InternalServerError
    })?;
    let rpc_url = configured_rpc_url(body.network).map_err(|error| {
        error!(target: "admin", %error, "burn-excess RPC unavailable");
        Status::InternalServerError
    })?;
    let http_url = wss_to_http(&rpc_url).map_err(|error| {
        error!(target: "admin", %error, "burn-excess RPC unavailable");
        Status::InternalServerError
    })?;
    let read_provider = ProviderBuilder::new().connect_http(http_url);

    let request = BurnExcessRequest {
        mode: BurnExcessMode::Internal,
        issuer_request_id: body.issuer_request_id,
        deposit_tx_hash: body.deposit_tx_hash,
        funding_tx_hash: None,
        receipt_id: body.receipt_id,
        shares: body.shares.into_u256(),
        reason: body.reason,
        incident_id: body.incident_id,
        network: body.network,
        chain_id: body.chain_id,
        execute: body.execute,
        close: body.close,
    };
    let executed = request.execute;

    let outcome = run_burn_excess(
        pool.inner(),
        vault_service.as_ref(),
        &read_provider,
        issuer_wallet,
        request,
        |plan: &str| {
            // The CLI operator reads this plan before authorizing the burn;
            // here the request's `execute` stood in for that answer, so the
            // plan is recorded instead, next to the reason and incident.
            warn!(target: "admin", plan,
                "burn-excess (internal) auto-approving operator-confirmed plan"
            );
            Ok::<bool, std::io::Error>(true)
        },
    )
    .await
    .map_err(|error| {
        error!(target: "admin", %error, "burn-excess (internal) failed");
        map_burn_excess_error(&error)
    })?;

    Ok(Json(BurnExcessResponse { executed, outcome }))
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
