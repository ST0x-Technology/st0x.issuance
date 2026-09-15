//! Role-gated HTTP routes for the orchestrator cutover verbs, reusing the
//! on-chain onboarding helpers directly (the CLI wrappers only add stdout
//! reporting and file-based config resolution). Inputs come from the running
//! service: the orchestrator address from `[orchestrator.addresses]`, the
//! wallet from the Turnkey signer, the RPC from the per-network environment.

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use rocket::http::Status;
use rocket::request::FromParam;
use rocket::serde::json::Json;
use rocket::{State, get, post};
use serde::Serialize;
use sqlx::{Pool, Sqlite};
use std::str::FromStr;
use tracing::{error, info, warn};
use url::Url;

use super::api::UnderlyingParam;
use super::cli::preflight_assets;
use super::view::find_vault;
use super::{Network, UnderlyingSymbol};
use crate::auth::{CapitalOps, DebugOps, ReadOps};
use crate::config::Config;
use crate::mint::has_unresolved_signer_intent;
use crate::vault::onboarding::{
    ApprovalOutcome, OnboardingError, OrchestratorReadiness,
    check_orchestrator_readiness, ensure_unlimited_approval,
    prove_signing_shapes,
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
    let OrchestratorContext { orchestrator, bot, rpc_url, .. } =
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

    let provider = ProviderBuilder::new()
        .connect(rpc_url.as_str())
        .await
        .map_err(|error| {
            error!(target: "asset", %network, %error,
                "Orchestrator preflight could not connect to RPC"
            );
            Status::BadGateway
        })?;

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
    let OrchestratorContext { orchestrator, bot, rpc_url, chain_id, turnkey } =
        resolve_orchestrator_context(config.inner(), network)?;

    let vault = find_vault(pool.inner(), &symbol, &network)
        .await
        .map_err(|error| {
            error!(target: "asset", %error, "Failed to look up vault");
            Status::InternalServerError
        })?
        .ok_or(Status::NotFound)?;

    let resolved =
        resolve_turnkey_signer(turnkey, chain_id).map_err(|error| {
            error!(target: "asset", %error, "Failed to resolve Turnkey signer");
            Status::InternalServerError
        })?;

    let provider = ProviderBuilder::new()
        .connect(rpc_url.as_str())
        .await
        .map_err(|error| {
            error!(target: "asset", %error,
                "verify-signing could not connect to RPC"
            );
            Status::BadGateway
        })?;

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
/// not verify as a healthy orchestrator.
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
) -> Result<Json<ApproveResponse>, Status> {
    let network = parse_network(network)?;
    let UnderlyingParam(symbol) = underlying;
    let OrchestratorContext { orchestrator, bot, rpc_url, chain_id, turnkey } =
        resolve_orchestrator_context(config.inner(), network)?;

    let vault = find_vault(pool.inner(), &symbol, &network)
        .await
        .map_err(|error| {
            error!(target: "asset", %error, "Failed to look up vault");
            Status::InternalServerError
        })?
        .ok_or(Status::NotFound)?;

    let resolved =
        resolve_turnkey_signer(turnkey, chain_id).map_err(|error| {
            error!(target: "asset", %error, "Failed to resolve Turnkey signer");
            Status::InternalServerError
        })?;

    let provider = ProviderBuilder::new()
        .with_chain_id(chain_id)
        .wallet(resolved.wallet)
        .connect(rpc_url.as_str())
        .await
        .map_err(|error| {
            error!(target: "asset", %error, "approve could not connect to RPC");
            Status::BadGateway
        })?;

    // The spender is about to receive an unlimited allowance from the
    // production wallet, so prove the address is a healthy orchestrator first:
    // a stale or typo'd entry fails these reads or reports vault logic false.
    let readiness =
        check_orchestrator_readiness(&provider, orchestrator, bot, &[])
            .await
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
        &provider,
        vault,
        orchestrator,
        bot,
    )
    .await?;

    let response = match outcome {
        ApprovalOutcome::AlreadyUnlimited => {
            ApproveResponse { outcome: "already_unlimited", tx_hash: None }
        }
        ApprovalOutcome::Approved { tx_hash } => ApproveResponse {
            outcome: "approved",
            tx_hash: Some(tx_hash.to_string()),
        },
    };
    info!(target: "asset", %orchestrator, %vault, outcome = response.outcome,
        "Orchestrator approval settled"
    );
    Ok(Json(response))
}

/// Broadcasts the approval from the production wallet under the network's
/// service wallet lock, the one every live mint and redemption burn holds
/// while it checks intents and signs. The lock is taken before the intent
/// check so no live flow can persist a signed nonce between the check and
/// this broadcast; while it is held no live flow fills a nonce either, so the
/// pending count this route's provider reads is the one to use. The lock is
/// held until the receipt is in, since the service's nonce manager resyncs
/// from the chain's pending count and must not observe this transaction only
/// halfway through. Refuses with 409 while an unresolved mint or redemption
/// signer intent already holds a signed nonce on this network: both would
/// fill from the same pending nonce and one would fail nonce-too-low, leaving
/// the bot's recovery to reconcile a submission it did not make.
async fn approve_under_wallet_lock<P: Provider>(
    vault_service: &dyn VaultService,
    pool: &Pool<Sqlite>,
    network: Network,
    provider: &P,
    vault: Address,
    orchestrator: Address,
    bot: Address,
) -> Result<ApprovalOutcome, Status> {
    let _wallet_guard = vault_service.lock_wallet().await;

    if has_unresolved_signer_intent(pool, network, None).await.map_err(
        |error| {
            error!(target: "asset", %network, %error,
                "Failed to check signer intents before approve"
            );
            Status::InternalServerError
        },
    )? {
        warn!(target: "asset", %network,
            "Refusing approve: an unresolved signer intent holds the wallet nonce"
        );
        return Err(Status::Conflict);
    }

    ensure_unlimited_approval(provider, vault, orchestrator, bot).await.map_err(
        |error| {
            error!(target: "asset", %orchestrator, %vault, %error,
                "Orchestrator approval failed"
            );
            map_onboarding_error(&error)
        },
    )
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
    rpc_url: Url,
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
    let rpc_url = chain.rpc_url.clone();
    let chain_id = chain.chain_id;

    Ok(OrchestratorContext { orchestrator, bot, rpc_url, chain_id, turnkey })
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

/// Maps an onboarding failure to an HTTP status. A Turnkey policy denial is a
/// 422 the operator fixes in the policy; every other failure is an on-chain or
/// RPC fault (502).
const fn map_onboarding_error(error: &OnboardingError) -> Status {
    use OnboardingError::{
        ApprovalNotEffective, ApprovalReverted, Contract, PendingTransaction,
        SigningRejected, Transport,
    };

    match error {
        SigningRejected { .. } => Status::UnprocessableEntity,
        Contract(_)
        | PendingTransaction(_)
        | Transport(_)
        | ApprovalReverted { .. }
        | ApprovalNotEffective { .. } => Status::BadGateway,
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Address, U256, address};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use rocket::http::Status;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use std::sync::Arc;
    use std::time::Duration;

    use super::approve_under_wallet_lock;
    use crate::account::ClientId;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::mint::{IssuerMintRequestId, MintEvent, TokenizationRequestId};
    use crate::test_utils::LocalEvm;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
    use crate::vault::PreparedMintTx;
    use crate::vault::mock::MockVaultService;
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

    /// The approval must take the service wallet lock before it checks for
    /// signer intents: a mint that signs while the approval is queued on the
    /// lock persists its intent under that lock, and the approval, once it
    /// acquires the lock, must see it and refuse rather than broadcast on the
    /// nonce the mint just took.
    #[tokio::test]
    async fn approve_checks_intents_only_once_it_holds_the_wallet_lock() {
        let evm = LocalEvm::new().await.unwrap();
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let pool = migrated_pool().await;
        let vault_service =
            Arc::new(MockVaultService::new_wallet_lock_blocked());
        let orchestrator = Address::repeat_byte(0x42);

        let approval = tokio::spawn({
            let vault_service = Arc::clone(&vault_service);
            let pool = pool.clone();
            let provider = provider.clone();
            let vault = evm.vault_address;
            let bot = evm.wallet_address;
            async move {
                approve_under_wallet_lock(
                    vault_service.as_ref(),
                    &pool,
                    Network::Base,
                    &provider,
                    vault,
                    orchestrator,
                    bot,
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

        let error = approval.await.unwrap().unwrap_err();
        assert_eq!(
            error,
            Status::Conflict,
            "the intent check must see the mint that signed while the approval waited"
        );
        let allowance =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider)
                .allowance(evm.wallet_address, orchestrator)
                .call()
                .await
                .unwrap();
        assert_eq!(
            allowance,
            U256::ZERO,
            "a refused approval must not have broadcast"
        );
    }

    /// With the wallet free and no intent outstanding the approval lands.
    #[tokio::test]
    async fn approve_broadcasts_under_a_free_wallet() {
        let evm = LocalEvm::new().await.unwrap();
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let pool = migrated_pool().await;
        let vault_service = MockVaultService::new_success();
        let orchestrator = Address::repeat_byte(0x42);

        approve_under_wallet_lock(
            &vault_service,
            &pool,
            Network::Base,
            &provider,
            evm.vault_address,
            orchestrator,
            evm.wallet_address,
        )
        .await
        .unwrap();

        assert_eq!(vault_service.get_wallet_lock_call_count(), 1);
        let allowance =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider)
                .allowance(evm.wallet_address, orchestrator)
                .call()
                .await
                .unwrap();
        assert_eq!(allowance, U256::MAX);
    }
}
