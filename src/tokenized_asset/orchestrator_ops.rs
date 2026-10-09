//! Role-gated HTTP routes for the orchestrator cutover verbs, reusing the
//! on-chain onboarding helpers directly (the CLI wrappers only add stdout
//! reporting and file-based config resolution). Inputs come from the running
//! service: the orchestrator address from `[orchestrator.addresses]`, the
//! wallet from the Turnkey signer, the RPC from the per-network environment.

use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use rocket::http::Status;
use rocket::request::FromParam;
use rocket::serde::json::Json;
use rocket::{State, get, post};
use serde::Serialize;
use sqlx::{Pool, Sqlite};
use std::str::FromStr;
use tokio::time::Instant;
use tracing::{error, info, warn};

use super::api::UnderlyingParam;
use super::cli::preflight_assets;
use super::view::find_vault;
use super::{Network, UnderlyingSymbol};
use crate::auth::{CapitalOps, DebugOps, ReadOps};
use crate::chain::{RpcEndpoint, rpc_client};
use crate::config::Config;
use crate::mint::has_unresolved_signer_intent;
use crate::vault::onboarding::{
    APPROVAL_DEADLINE, APPROVAL_RECEIPT_POLL, ApprovalOutcome, OnboardingError,
    OrchestratorReadiness, check_orchestrator_readiness,
    ensure_unlimited_approval, prove_signing_shapes,
};
use crate::vault::{NetworkVaultServices, VaultService};
use crate::wallet::SignerConfig;
use crate::wallet::turnkey::{TurnkeyConfig, resolve_turnkey_signer};

/// Read-tier pre-cutover readiness gate: on-chain roles, `vaultLogicIsExpected`,
/// and per-asset allowances for `network`. Pure reads; signs nothing.
#[get("/ops/read/orchestrator-preflight/<network>?<asset>")]
pub(crate) async fn orchestrator_preflight_ops(
    _auth: ReadOps,
    pool: &State<Pool<Sqlite>>,
    config: &State<Config>,
    network: &str,
    asset: Vec<String>,
) -> Result<Json<PreflightResponse>, Status> {
    let network = parse_network(network)?;
    let OrchestratorContext { orchestrator, bot, rpc, .. } =
        resolve_orchestrator_context(config.inner(), network)?;

    let filter = parse_assets(&asset)?;
    let assets = preflight_assets(
        pool.inner(),
        network,
        &filter,
        &config.vault_mode_config,
    )
    .await
    .map_err(|error| {
        warn!(target: "asset", %network, %error,
            "Orchestrator preflight asset scope failed"
        );
        Status::UnprocessableEntity
    })?;

    let provider = ProviderBuilder::new().connect_client(
        rpc_client(&rpc).map_err(|error| {
            error!(target: "asset", %network, %error,
                "Orchestrator preflight could not connect to RPC"
            );
            Status::BadGateway
        })?,
    );

    let report =
        check_orchestrator_readiness(&provider, orchestrator, bot, &assets)
            .await
            .map_err(|error| {
                error!(target: "asset", %orchestrator, %error,
                    "Orchestrator readiness reads failed"
                );
                map_onboarding_error(&error)
            })?;

    info!(target: "asset", %orchestrator, ready = report.is_ready(),
        "Orchestrator preflight reported"
    );
    Ok(Json(report.into()))
}

/// Debug-tier proof that the Turnkey policy signs every orchestrator shape.
/// Signs (never broadcasts) one transaction per required shape; a policy
/// denial surfaces here rather than during the first live mint.
#[post("/ops/debug/orchestrator-verify-signing/<network>/<underlying>")]
#[tracing::instrument(
    target = "auth",
    name = "operator",
    skip_all,
    fields(subject = %auth.0)
)]
pub(crate) async fn orchestrator_verify_signing_ops(
    auth: DebugOps,
    pool: &State<Pool<Sqlite>>,
    config: &State<Config>,
    network: &str,
    underlying: UnderlyingParam,
) -> Result<Json<VerifySigningResponse>, Status> {
    let network = parse_network(network)?;
    let UnderlyingParam(symbol) = underlying;
    let OrchestratorContext { orchestrator, bot, rpc, chain_id, turnkey } =
        resolve_orchestrator_context(config.inner(), network)?;

    let vault = find_vault(pool.inner(), &symbol, &network)
        .await
        .map_err(|error| {
            error!(target: "asset", %error, "Failed to look up vault");
            Status::InternalServerError
        })?
        .ok_or(Status::NotFound)?;

    let resolved =
        resolve_turnkey_signer(turnkey, chain_id).await.map_err(|error| {
            error!(target: "asset", %error, "Failed to resolve Turnkey signer");
            Status::InternalServerError
        })?;

    let provider = ProviderBuilder::new().connect_client(
        rpc_client(&rpc).map_err(|error| {
            error!(target: "asset", %error,
                "verify-signing could not connect to RPC"
            );
            Status::BadGateway
        })?,
    );

    let proofs = prove_signing_shapes(
        &provider,
        &resolved.wallet,
        orchestrator,
        vault,
        bot,
    )
    .await
    .map_err(|error| {
        error!(target: "asset", %orchestrator, %error,
            "Orchestrator signing proof failed"
        );
        map_onboarding_error(&error)
    })?;

    info!(target: "asset", %orchestrator, shapes = proofs.len(),
        "Orchestrator signing verified"
    );
    Ok(Json(VerifySigningResponse {
        orchestrator: orchestrator.to_string(),
        bot: bot.to_string(),
        shapes: proofs
            .iter()
            .map(|proof| SigningShapeResponse {
                label: proof.label.to_string(),
                to: proof.to.to_string(),
                tx_hash: proof.tx_hash.to_string(),
            })
            .collect(),
    }))
}

/// Capital-tier one-time unlimited approval of an asset's vault shares to the
/// orchestrator, signed and broadcast through Turnkey. Idempotent: an already
/// unlimited allowance sends nothing. Refuses if the configured address does
/// not verify as a healthy orchestrator. The whole request runs against one
/// [`APPROVAL_DEADLINE`] from its start: running out before the broadcast
/// answers 503 with nothing sent, and an approval signed but not confirmed in
/// time answers 202 with its hash, so the operator reconciles it on chain
/// rather than sending a second approval.
#[post("/ops/capital/orchestrator-approve/<network>/<underlying>")]
#[tracing::instrument(
    target = "auth",
    name = "operator",
    skip_all,
    fields(subject = %auth.0)
)]
pub(crate) async fn orchestrator_approve_ops(
    auth: CapitalOps,
    pool: &State<Pool<Sqlite>>,
    config: &State<Config>,
    vault_services: &State<NetworkVaultServices>,
    network: &str,
    underlying: UnderlyingParam,
) -> Result<(Status, Json<ApproveResponse>), Status> {
    let deadline = Instant::now()
        .checked_add(APPROVAL_DEADLINE)
        .ok_or(Status::InternalServerError)?;
    let network = parse_network(network)?;
    let UnderlyingParam(symbol) = underlying;
    let OrchestratorContext { orchestrator, bot, rpc, chain_id, turnkey } =
        resolve_orchestrator_context(config.inner(), network)?;

    let vault = before_deadline(
        deadline,
        "vault lookup",
        find_vault(pool.inner(), &symbol, &network),
    )
    .await?
    .map_err(|error| {
        error!(target: "asset", %error, "Failed to look up vault");
        Status::InternalServerError
    })?
    .ok_or(Status::NotFound)?;

    let resolved = before_deadline(
        deadline,
        "turnkey signer",
        resolve_turnkey_signer(turnkey, chain_id),
    )
    .await?
    .map_err(|error| {
        error!(target: "asset", %error, "Failed to resolve Turnkey signer");
        Status::InternalServerError
    })?;

    // A remote RPC client polls receipts every 7 s by default, which would
    // spend most of the approval's deadline between two looks at a receipt
    // that landed in the first block or two.
    let rpc_client = rpc_client(&rpc).map_err(|error| {
        error!(target: "asset", %error, "approve could not connect to RPC");
        Status::BadGateway
    })?;
    let provider = ProviderBuilder::new()
        .with_chain_id(chain_id)
        .wallet(resolved.wallet)
        .connect_client(rpc_client.with_poll_interval(APPROVAL_RECEIPT_POLL));

    // The spender is about to receive an unlimited allowance from the
    // production wallet, so prove the address is a healthy orchestrator first:
    // a stale or typo'd entry fails these reads or reports vault logic false.
    let readiness = before_deadline(
        deadline,
        "orchestrator readiness",
        check_orchestrator_readiness(&provider, orchestrator, bot, &[]),
    )
    .await?
    .map_err(|error| {
        error!(target: "asset", %orchestrator, %error,
            "Refusing approve: orchestrator could not be verified"
        );
        map_onboarding_error(&error)
    })?;
    if !readiness.vault_logic_expected {
        warn!(target: "asset", %orchestrator,
            "Refusing approve: vaultLogicIsExpected() is false"
        );
        return Err(Status::UnprocessableEntity);
    }

    let vault_service = vault_services.service(network).map_err(|error| {
        warn!(target: "asset", %error, "Refusing approve");
        Status::UnprocessableEntity
    })?;
    let outcome = approve_under_wallet_lock(
        vault_service.as_ref(),
        pool.inner(),
        network,
        deadline,
        async {
            Box::pin(ensure_unlimited_approval(
                &provider,
                vault,
                orchestrator,
                bot,
                deadline,
            ))
            .await
            .map_err(|error| {
                log_approval_failure(&error, orchestrator, vault);
                map_onboarding_error(&error)
            })
        },
    )
    .await?;

    let (status, response) = approve_reply(outcome);
    info!(target: "asset", %orchestrator, %vault, outcome = response.outcome,
        tx_hash = ?response.tx_hash, "Orchestrator approval settled"
    );
    Ok((status, Json(response)))
}

/// Logs why the approval did not settle: running out of time before the
/// broadcast is the same refusal the pre-broadcast steps log (WARN, nothing
/// sent); anything else is a failed approval (ERROR, with the node's or
/// chain's reason).
fn log_approval_failure(
    error: &OnboardingError,
    orchestrator: Address,
    vault: Address,
) {
    if matches!(error, OnboardingError::DeadlineBeforeBroadcast) {
        warn!(target: "asset", step = "approval signing", %orchestrator, %vault,
            "Refusing approve: deadline passed before broadcast; nothing sent"
        );
    } else {
        error!(target: "asset", %orchestrator, %vault, %error,
            "Orchestrator approval failed"
        );
    }
}

/// The approval's HTTP answer: 200 for a verified or already-unlimited
/// allowance, 202 for one signed but not confirmed, which may still land.
fn approve_reply(outcome: ApprovalOutcome) -> (Status, ApproveResponse) {
    match outcome {
        ApprovalOutcome::AlreadyUnlimited => (
            Status::Ok,
            ApproveResponse { outcome: "already_unlimited", tx_hash: None },
        ),
        ApprovalOutcome::Approved { tx_hash } => (
            Status::Ok,
            ApproveResponse {
                outcome: "approved",
                tx_hash: Some(tx_hash.to_string()),
            },
        ),
        ApprovalOutcome::SubmittedUnconfirmed { tx_hash } => (
            Status::Accepted,
            ApproveResponse {
                outcome: "submitted_unconfirmed",
                tx_hash: Some(tx_hash.to_string()),
            },
        ),
    }
}

/// Runs a pre-broadcast `step` of the approval against its deadline: one that
/// would overrun it answers 503, since nothing has been sent yet and a retry
/// is safe. `step` names it in the log.
async fn before_deadline<T>(
    deadline: Instant,
    step: &'static str,
    work: impl Future<Output = T>,
) -> Result<T, Status> {
    tokio::time::timeout_at(deadline, work).await.map_err(|_| {
        warn!(target: "asset", step,
            "Refusing approve: deadline passed before broadcast; nothing sent"
        );
        Status::ServiceUnavailable
    })
}

/// Runs `approval` under the network's service wallet lock, the lock every
/// live mint and redemption burn holds while it checks intents and signs. The
/// lock is taken before the intent check so no live flow can persist a signed
/// nonce between the check and the broadcast; while it is held no live flow
/// fills a nonce either, so the pending count the route's provider reads is
/// the one to use. Waiting for the lock and the intent check count against
/// the approval's deadline (503 on elapse, nothing sent). The lock is released
/// when `approval` returns, at the latest at the deadline. An approval still
/// pending then keeps its nonce: the service's nonce manager fills the next
/// live flow after it from the node's pending count, so until it mines (or the
/// operator replaces it) later mints and burns queue behind it. Refuses with
/// 409 while an unresolved mint or redemption signer intent already holds a
/// signed nonce on this network: both would fill from the same pending nonce
/// and one would fail nonce-too-low, leaving the bot's recovery to reconcile a
/// submission it did not make.
async fn approve_under_wallet_lock(
    vault_service: &dyn VaultService,
    pool: &Pool<Sqlite>,
    network: Network,
    deadline: Instant,
    approval: impl Future<Output = Result<ApprovalOutcome, Status>>,
) -> Result<ApprovalOutcome, Status> {
    let _wallet_guard =
        before_deadline(deadline, "wallet lock", vault_service.lock_wallet())
            .await?;

    let intent_held = before_deadline(
        deadline,
        "signer intent check",
        has_unresolved_signer_intent(pool, network, None),
    )
    .await?
    .map_err(|error| {
        error!(target: "asset", %network, %error,
            "Failed to check signer intents before approve"
        );
        Status::InternalServerError
    })?;
    if intent_held {
        warn!(target: "asset", %network,
            "Refusing approve: an unresolved signer intent holds the wallet nonce"
        );
        return Err(Status::Conflict);
    }

    approval.await
}

#[derive(Serialize)]
pub(crate) struct AssetReadinessResponse {
    underlying: String,
    vault: String,
    allowance: String,
    unlimited: bool,
    deposit_role_granted: bool,
    withdraw_role_granted: bool,
}

#[derive(Serialize)]
pub(crate) struct OrchestratorRoles {
    mint_role_granted: bool,
    burn_role_granted: bool,
    vault_logic_expected: bool,
}

#[derive(Serialize)]
pub(crate) struct PreflightResponse {
    ready: bool,
    orchestrator: String,
    bot: String,
    roles: OrchestratorRoles,
    assets: Vec<AssetReadinessResponse>,
}

impl From<OrchestratorReadiness> for PreflightResponse {
    fn from(report: OrchestratorReadiness) -> Self {
        let assets = report
            .assets
            .iter()
            .map(|asset| AssetReadinessResponse {
                underlying: asset.underlying.to_string(),
                vault: asset.vault.to_string(),
                allowance: asset.allowance.to_string(),
                unlimited: asset.is_unlimited(),
                deposit_role_granted: asset.deposit_role_granted,
                withdraw_role_granted: asset.withdraw_role_granted,
            })
            .collect();
        Self {
            ready: report.is_ready(),
            orchestrator: report.orchestrator.to_string(),
            bot: report.bot.to_string(),
            roles: OrchestratorRoles {
                mint_role_granted: report.mint_role_granted,
                burn_role_granted: report.burn_role_granted,
                vault_logic_expected: report.vault_logic_expected,
            },
            assets,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct SigningShapeResponse {
    label: String,
    to: String,
    tx_hash: String,
}

#[derive(Serialize)]
pub(crate) struct VerifySigningResponse {
    orchestrator: String,
    bot: String,
    shapes: Vec<SigningShapeResponse>,
}

#[derive(Serialize)]
pub(crate) struct ApproveResponse {
    outcome: &'static str,
    tx_hash: Option<String>,
}

/// Resolved inputs for the orchestrator ops: the orchestrator address, the
/// Turnkey bot wallet, and the network's configured RPC endpoint, chain id, and
/// Turnkey config that the signing verbs pass to [`resolve_turnkey_signer`].
struct OrchestratorContext<'a> {
    orchestrator: Address,
    bot: Address,
    rpc: RpcEndpoint,
    chain_id: u64,
    turnkey: &'a TurnkeyConfig,
}

/// Resolves the orchestrator context for `network`, or the HTTP status to fail
/// with.
fn resolve_orchestrator_context(
    config: &Config,
    network: Network,
) -> Result<OrchestratorContext<'_>, Status> {
    let orchestrator = config
        .vault_mode_config
        .orchestrator_address_for(network)
        .ok_or_else(|| {
            warn!(target: "asset", %network,
                "No orchestrator address configured for network"
            );
            Status::UnprocessableEntity
        })?;

    let SignerConfig::Turnkey(turnkey) = &config.signer else {
        warn!(target: "asset",
            "Orchestrator ops require the Turnkey signer configuration"
        );
        return Err(Status::UnprocessableEntity);
    };
    let bot = turnkey.settings.address;

    let chain = config
        .chains
        .iter()
        .find(|candidate| candidate.network == network)
        .ok_or_else(|| {
            error!(target: "asset", %network,
                "No chain configuration for network"
            );
            Status::InternalServerError
        })?;
    let rpc = chain.rpc.clone();
    let chain_id = chain.chain_id;

    Ok(OrchestratorContext { orchestrator, bot, rpc, chain_id, turnkey })
}

fn parse_network(network: &str) -> Result<Network, Status> {
    Network::from_str(network).map_err(|error| {
        warn!(target: "asset", network, %error, "Invalid network");
        Status::UnprocessableEntity
    })
}

/// The `?asset=` filter values are operator-supplied symbols like the path
/// segment, so they take the same normalisation; a malformed one is a 422.
fn parse_assets(assets: &[String]) -> Result<Vec<UnderlyingSymbol>, Status> {
    assets
        .iter()
        .map(|asset| {
            UnderlyingParam::from_param(asset)
                .map(|UnderlyingParam(symbol)| symbol)
                .map_err(|_| Status::UnprocessableEntity)
        })
        .collect()
}

/// Maps an onboarding failure to an HTTP status. A Turnkey policy denial found
/// by the sign-only proof and a broadcast the node refused (nothing pending;
/// its reason is in the log) are 422s the operator fixes before retrying; a
/// deadline that passed before the broadcast is a 503 with nothing sent; a
/// filled but unsigned transaction is the bot's own fault (500); every other
/// failure, including the approval's own signing failing inside the fill, is
/// an on-chain, RPC, or signer fault (502).
const fn map_onboarding_error(error: &OnboardingError) -> Status {
    use OnboardingError::{
        ApprovalNotEffective, ApprovalReverted, BroadcastRejected, Contract,
        DeadlineBeforeBroadcast, SigningRejected, Transport, Unsigned,
    };

    match error {
        SigningRejected { .. } | BroadcastRejected { .. } => {
            Status::UnprocessableEntity
        }
        DeadlineBeforeBroadcast => Status::ServiceUnavailable,
        Unsigned => Status::InternalServerError,
        Contract(_)
        | Transport(_)
        | ApprovalReverted { .. }
        | ApprovalNotEffective { .. } => Status::BadGateway,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, address};
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::transports::RpcError;
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use rocket::http::Status;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::time::Instant;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        approve_reply, approve_under_wallet_lock, log_approval_failure,
        map_onboarding_error,
    };
    use crate::account::ClientId;
    use crate::mint::{IssuerMintRequestId, MintEvent, TokenizationRequestId};
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
    use crate::vault::PreparedMintTx;
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;
    use crate::vault::onboarding::{ApprovalOutcome, OnboardingError};
    use crate::{Quantity, VaultMode};

    async fn migrated_pool() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    async fn insert_mint_event(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        sequence: i64,
        event: &MintEvent,
    ) {
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
            VALUES ('Mint', ?, ?, ?, '1.0', ?, '{}')
            ",
        )
        .bind(issuer_request_id.to_string())
        .bind(sequence)
        .bind(event.event_type())
        .bind(serde_json::to_string(event).unwrap())
        .execute(pool)
        .await
        .unwrap();
    }

    /// Persists the `MintTxIntended` a mint job records while holding the
    /// wallet, which reserves the network's signer for that mint.
    async fn seed_mint_holding_the_signer(pool: &Pool<Sqlite>) {
        let issuer_request_id = IssuerMintRequestId::random();
        insert_mint_event(
            pool,
            &issuer_request_id,
            1,
            &MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok"),
                quantity: Quantity::new(Decimal::new(1, 0)),
                underlying: UnderlyingSymbol::new("PTY").unwrap(),
                token: TokenSymbol::new("tPTY"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
                initiated_at: Utc::now(),
                mint_mode: VaultMode::VaultDirect,
            },
        )
        .await;
        insert_mint_event(
            pool,
            &issuer_request_id,
            2,
            &MintEvent::MintTxIntended {
                issuer_request_id: issuer_request_id.clone(),
                prepared_tx: PreparedMintTx::valid_for_test(
                    1,
                    format!("mint-{issuer_request_id}"),
                ),
                intended_at: Utc::now(),
            },
        )
        .await;
    }

    fn deadline_after(budget: Duration) -> Instant {
        Instant::now().checked_add(budget).unwrap()
    }

    /// The approval must take the service wallet lock before it checks for
    /// signer intents: a mint that signs while the approval is queued on the
    /// lock persists its intent under that lock, and the approval, once it
    /// acquires the lock, must see it and refuse rather than broadcast on the
    /// nonce the mint just took.
    #[tokio::test]
    async fn approve_checks_intents_only_once_it_holds_the_wallet_lock() {
        let pool = migrated_pool().await;
        let vault_service =
            Arc::new(MockVaultService::new_wallet_lock_blocked());
        let broadcast = Arc::new(AtomicBool::new(false));

        let approval = tokio::spawn({
            let vault_service = Arc::clone(&vault_service);
            let pool = pool.clone();
            let broadcast = Arc::clone(&broadcast);
            async move {
                approve_under_wallet_lock(
                    vault_service.as_ref(),
                    &pool,
                    Network::Base,
                    deadline_after(Duration::from_secs(25)),
                    async {
                        broadcast.store(true, Ordering::SeqCst);
                        Ok(ApprovalOutcome::AlreadyUnlimited)
                    },
                )
                .await
            }
        });

        // Queued on the wallet: a mint holding it signs and persists its intent.
        tokio::time::timeout(
            Duration::from_secs(5),
            vault_service.wait_for_wallet_lock_attempt(),
        )
        .await
        .expect(
            "the approval must contend for the wallet before anything else",
        );
        assert!(
            !approval.is_finished(),
            "the approval must not run its intent check before it holds the wallet"
        );
        seed_mint_holding_the_signer(&pool).await;
        vault_service.release_wallet_lock();

        assert_eq!(
            approval.await.unwrap(),
            Err(Status::Conflict),
            "the intent check must see the mint that signed while the approval waited"
        );
        assert!(
            !broadcast.load(Ordering::SeqCst),
            "a refused approval must not have run"
        );
    }

    /// The approval runs while the wallet lock is held, so no live mint or
    /// burn can fill a nonce between its intent check and its broadcast.
    #[tokio::test]
    async fn approval_runs_while_the_wallet_lock_is_held() {
        let pool = migrated_pool().await;
        let vault_service = MockVaultService::new_success();

        let outcome = approve_under_wallet_lock(
            &vault_service,
            &pool,
            Network::Base,
            deadline_after(Duration::from_secs(25)),
            async {
                let competing_got_the_wallet = tokio::time::timeout(
                    Duration::from_millis(50),
                    vault_service.lock_wallet(),
                )
                .await
                .is_ok();
                assert!(
                    !competing_got_the_wallet,
                    "a live flow must not get the wallet while the approval runs"
                );
                Ok(ApprovalOutcome::AlreadyUnlimited)
            },
        )
        .await;

        assert_eq!(outcome, Ok(ApprovalOutcome::AlreadyUnlimited));
    }

    /// Waiting for the wallet counts against the approval's deadline: a lock
    /// held past it answers 503, before anything was signed or sent.
    #[traced_test]
    #[tokio::test]
    async fn approve_queued_on_the_wallet_past_its_deadline_is_refused() {
        let pool = migrated_pool().await;
        let vault_service = MockVaultService::new_wallet_lock_blocked();

        let refused = tokio::time::timeout(
            Duration::from_secs(5),
            approve_under_wallet_lock(
                &vault_service,
                &pool,
                Network::Base,
                deadline_after(Duration::from_millis(100)),
                async { Ok(ApprovalOutcome::AlreadyUnlimited) },
            ),
        )
        .await
        .expect("the lock wait must end at the approval's deadline");

        assert_eq!(refused, Err(Status::ServiceUnavailable));
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "Refusing approve: deadline passed before broadcast",
                "nothing sent",
                "wallet lock"
            ]
        ));
    }

    /// The intent check's database wait counts against the deadline too: a
    /// pool with no free connection answers 503 rather than holding the wallet
    /// lock, and the request, past it.
    #[traced_test]
    #[tokio::test]
    async fn approve_with_the_database_stalled_past_its_deadline_is_refused() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let _held = pool.acquire().await.unwrap();
        let vault_service = MockVaultService::new_success();

        let refused = tokio::time::timeout(
            Duration::from_secs(5),
            approve_under_wallet_lock(
                &vault_service,
                &pool,
                Network::Base,
                deadline_after(Duration::from_millis(100)),
                async { Ok(ApprovalOutcome::AlreadyUnlimited) },
            ),
        )
        .await
        .expect("the intent check must end at the approval's deadline");

        assert_eq!(refused, Err(Status::ServiceUnavailable));
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "Refusing approve: deadline passed before broadcast",
                "nothing sent",
                "signer intent check"
            ]
        ));
    }

    /// The status is the script-visible half of the answer: only a verified
    /// or already-unlimited allowance is 200; one signed but unconfirmed is
    /// 202 with the hash to reconcile.
    #[test]
    fn approval_outcomes_map_to_their_status_and_body() {
        let tx_hash = B256::repeat_byte(0xab);

        let (status, body) =
            approve_reply(ApprovalOutcome::SubmittedUnconfirmed { tx_hash });
        assert_eq!(status, Status::Accepted);
        assert_eq!(body.outcome, "submitted_unconfirmed");
        assert_eq!(body.tx_hash, Some(tx_hash.to_string()));

        let (status, body) =
            approve_reply(ApprovalOutcome::Approved { tx_hash });
        assert_eq!(status, Status::Ok);
        assert_eq!(body.outcome, "approved");
        assert_eq!(body.tx_hash, Some(tx_hash.to_string()));

        let (status, body) = approve_reply(ApprovalOutcome::AlreadyUnlimited);
        assert_eq!(status, Status::Ok);
        assert_eq!(body.outcome, "already_unlimited");
        assert_eq!(body.tx_hash, None);
    }

    /// A broadcast the node refused is definitive, nothing is pending, so it
    /// is a 422 the client reports as a failure, not a 502 it reads as an
    /// unknown outcome. Running out of time while signing is the same 503 the
    /// pre-broadcast steps answer, logged as a WARN refusal, not an ERROR.
    #[traced_test]
    #[test]
    fn refusals_map_to_definitive_statuses_and_logs() {
        let refused = OnboardingError::BroadcastRejected {
            tx_hash: B256::repeat_byte(0xcd),
            source: RpcError::ErrorResp(ErrorPayload {
                code: -32003,
                message: "insufficient funds for gas * price + value".into(),
                data: None,
            }),
        };
        assert_eq!(map_onboarding_error(&refused), Status::UnprocessableEntity);

        let late = OnboardingError::DeadlineBeforeBroadcast;
        assert_eq!(map_onboarding_error(&late), Status::ServiceUnavailable);
        log_approval_failure(
            &late,
            Address::repeat_byte(1),
            Address::repeat_byte(2),
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "Refusing approve: deadline passed before broadcast",
                "approval signing"
            ]
        ));
        assert!(!logs_contain_at!(
            Level::ERROR,
            &["Orchestrator approval failed"]
        ));
    }
}
