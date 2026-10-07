//! Orchestrates dual-path burn-excess recovery (D0.5).
//!
//! Dry-run proves and prints; `--execute` persists exclusion (Path B), then
//! signs/intends/submits/confirms the vault redeem and updates inventory. The
//! live route's `expect-funding` step records the funding Transfer a Path B
//! stream expects before it is broadcast.

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log, TransactionReceipt};
use alloy::sol_types::SolEvent;
use chrono::{DateTime, Utc};
use event_sorcery::{Store, StoreBuilder};
use serde::Serialize;
use sqlx::{Pool, Sqlite};
use std::io;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::exclusion::{
    FundingExclusionReactor, is_excluded_funding_log,
    rebuild_funding_exclusion_index, record_funding_exclusion,
};
use super::expectation::{
    FundingExpectationReactor, clear_funding_expectation,
    has_other_funding_expectation, is_expected_funding,
    record_funding_expectation,
};
use super::proof::{
    BurnExcessMode, BurnExcessProofError, DepositProof,
    FundingTransferCandidate, FundingTransferExpectation, PathResolution,
    bind_deposit_proof, decode_receipt_information_strict,
    require_exact_issuer_share_balance, require_funding_hash_match,
    require_issuer_receipt_balance, resolve_path, select_funding_transfer,
};
use super::{
    BurnExcess, BurnExcessCommand, BurnExcessId, BurnExcessPath,
    ExcessBurnBind, FundingTransferId, has_unresolved_excess_burn_intent,
};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::mint::{IssuerMintRequestId, Mint, has_unresolved_signer_intent};
use crate::poll_checkpoint::{CheckpointError, load_transfer_poll};
use crate::receipt_inventory::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand, Shares,
    load_inventory, send_receipt_inventory_command,
};
use crate::redemption::IssuerRedemptionRequestId;
use crate::redemption::poller::{BLOCK_CHUNK_SIZE, block_ranges};
use crate::tokenized_asset::view::find_vault;
use crate::tokenized_asset::{Network, UnderlyingSymbol};
use crate::vault::{
    BurnRequestOrigin, BurnTxStatus, MultiBurnEntry, MultiBurnParams,
    VaultService, WalletNonceGuard,
};

/// Operator inputs after clap parse (mode keyword already selected).
#[derive(Debug, Clone)]
pub(crate) struct BurnExcessRequest {
    pub(crate) mode: BurnExcessMode,
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) deposit_tx_hash: B256,
    /// Required on `external`; must be `None` on `internal`.
    pub(crate) funding_tx_hash: Option<B256>,
    pub(crate) receipt_id: U256,
    pub(crate) shares: U256,
    pub(crate) reason: String,
    pub(crate) incident_id: Option<String>,
    pub(crate) network: Network,
    pub(crate) chain_id: u64,
    pub(crate) execute: bool,
    pub(crate) close: bool,
    pub(crate) poller_guard: PollerGuard,
}

/// What keeps the redemption poller from opening a Redemption for a Path B
/// funding Transfer, which decides what a fresh `external` run requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PollerGuard {
    /// Offline CLI: the whole service, poller included, is stopped from
    /// before the funding Transfer is broadcast until the run finishes.
    ServiceStopped,
    /// Live route: the poller runs, so the stream must have recorded its
    /// funding expectation before the funding Transfer was broadcast.
    FundingExpected,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnExcessEngineError {
    #[error(transparent)]
    Proof(#[from] BurnExcessProofError),

    #[error(transparent)]
    Aggregate(Box<super::BurnExcessError>),

    #[error(transparent)]
    Store(Box<event_sorcery::SendError<BurnExcess>>),

    #[error(transparent)]
    MintStore(Box<event_sorcery::SendError<Mint>>),

    #[error(transparent)]
    Inventory(Box<event_sorcery::SendError<ReceiptInventory>>),

    #[error(transparent)]
    Vault(#[from] crate::vault::VaultError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error("failed to reconcile event schema: {0}")]
    Reconcile(#[from] event_sorcery::ReconcileError),

    #[error("failed to build the {aggregate} store: {message}")]
    StoreBuild { aggregate: &'static str, message: String },

    #[error(transparent)]
    TokenizedAsset(
        #[from] crate::tokenized_asset::view::TokenizedAssetViewError,
    ),

    #[error(transparent)]
    Provider(
        Box<alloy::transports::RpcError<alloy::transports::TransportErrorKind>>,
    ),

    #[error(transparent)]
    Contract(Box<alloy::contract::Error>),

    #[error("mint {issuer_request_id} has no event history in this database")]
    MintNotFound { issuer_request_id: IssuerMintRequestId },

    #[error(
        "mint {issuer_request_id} does not carry underlying/network \
         (state={state}); burn-excess needs a mint that still has asset fields"
    )]
    MintMissingAsset { issuer_request_id: IssuerMintRequestId, state: String },

    #[error(
        "mint {issuer_request_id} is on network {mint_network}, not \
         --network {requested}"
    )]
    MintNetworkMismatch {
        issuer_request_id: IssuerMintRequestId,
        mint_network: Network,
        requested: Network,
    },

    #[error(
        "no vault listing for underlying {underlying} on network {network}"
    )]
    VaultNotListed { underlying: UnderlyingSymbol, network: Network },

    #[error(
        "deposit transaction {tx_hash} is missing, reverted, or not a vault \
         Deposit on the listed vault"
    )]
    DepositTxInvalid { tx_hash: B256 },

    #[error(
        "deposit transaction {tx_hash} has multiple Deposit logs on the listed \
         vault; refuse ambiguous receipt selection"
    )]
    AmbiguousDepositTx { tx_hash: B256 },

    #[error(
        "deposit transaction {tx_hash} has multiple outbound share Transfers \
         matching the deposit; refuse ambiguous original recipient"
    )]
    AmbiguousShareTransferOut { tx_hash: B256 },

    #[error(
        "funding transaction {tx_hash} is missing or reverted on this chain"
    )]
    FundingTxInvalid { tx_hash: B256 },

    #[error(
        "an unresolved mint or redemption burn intent holds a signed wallet \
         nonce on {network}; clear it before burn-excess"
    )]
    UnresolvedSignerIntent { network: Network },

    #[error(
        "another excess-burn recovery is unresolved (funding excluded, \
         intended, or submitted); resume or --close it first"
    )]
    UnresolvedExcessBurnIntent,

    #[error(
        "the live external route needs `burn-excess expect-funding --execute` \
         recorded before the funding Transfer is broadcast, and this stream \
         has none. Do not record one now if the Transfer was already sent: \
         the running poller may have detected it as a redemption. If no \
         Redemption exists for the funding transaction, stop the issuer \
         service and finish with the offline `issuer burn-excess external`; \
         if one exists, burn-excess will refuse it and that redemption is \
         the incident to handle"
    )]
    FundingNotExpected,

    #[error(
        "another excess-burn recovery already expects its funding Transfer on \
         vault {vault}; finish or --close it first, since the issuer wallet \
         can hold only one stream's funding at a time"
    )]
    AnotherFundingExpected { vault: Address },

    #[error(
        "funding expectation index missing after ExpectFunding for deposit \
         {deposit_tx_hash:?}; do not broadcast the funding Transfer"
    )]
    FundingExpectationIndexMissing { deposit_tx_hash: B256 },

    #[error("operator aborted")]
    Aborted,

    #[error("I/O error during operator confirmation: {0}")]
    Io(#[from] io::Error),

    #[error(
        "persisted burn intent is {status:?}; use `burn-excess … --close \
         --execute` to clear the wallet nonce gate for this deposit. Closed is \
         report-only terminal for this stream — it does not unlock a \
         replacement intend on the same deposit_tx_hash; follow the ops \
         runbook if a new signed burn is required"
    )]
    DeadBurnIntent { status: BurnTxStatus },

    #[error(
        "burn result did not match planned withdrawal: expected receipt \
         {expected_receipt} shares {expected_shares}, on-chain {onchain:?}"
    )]
    BurnDeltaMismatch {
        expected_receipt: U256,
        expected_shares: U256,
        onchain: Vec<(U256, U256)>,
    },

    #[error(
        "funding exclusion index missing after RecordFundingExclusion for \
         tx {tx_hash:?} log_index={log_index}; refusing to prepare burn"
    )]
    FundingExclusionIndexMissing { tx_hash: B256, log_index: u64 },

    #[error(
        "post-burn inventory reconcile failed for receipt {receipt_id} \
         (on-chain burn already completed): {source}"
    )]
    InventoryReconcileFailed {
        receipt_id: U256,
        #[source]
        source: Box<event_sorcery::SendError<ReceiptInventory>>,
    },

    #[error(transparent)]
    RebuildFundingExclusion(
        #[from] super::exclusion::RebuildFundingExclusionError,
    ),

    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
}

impl From<event_sorcery::SendError<BurnExcess>> for BurnExcessEngineError {
    fn from(error: event_sorcery::SendError<BurnExcess>) -> Self {
        Self::Store(Box::new(error))
    }
}

impl From<event_sorcery::SendError<Mint>> for BurnExcessEngineError {
    fn from(error: event_sorcery::SendError<Mint>) -> Self {
        Self::MintStore(Box::new(error))
    }
}

impl From<event_sorcery::SendError<ReceiptInventory>>
    for BurnExcessEngineError
{
    fn from(error: event_sorcery::SendError<ReceiptInventory>) -> Self {
        Self::Inventory(Box::new(error))
    }
}

impl From<super::BurnExcessError> for BurnExcessEngineError {
    fn from(error: super::BurnExcessError) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

impl From<alloy::transports::RpcError<alloy::transports::TransportErrorKind>>
    for BurnExcessEngineError
{
    fn from(
        error: alloy::transports::RpcError<
            alloy::transports::TransportErrorKind,
        >,
    ) -> Self {
        Self::Provider(Box::new(error))
    }
}

impl From<alloy::contract::Error> for BurnExcessEngineError {
    fn from(error: alloy::contract::Error) -> Self {
        Self::Contract(Box::new(error))
    }
}

/// Builds the `BurnExcess` store with the funding exclusion and expectation
/// reactors attached.
///
/// Rebuilds the funding exclusion index from events first: custom reactors on
/// `Nil`-materialized aggregates are not catch_up'd by `StoreBuilder`. That
/// rebuild only inserts, so it is safe while the poller and other requests
/// run. The expectation index is not rebuilt here: its rebuild deletes rows,
/// and run per request it could drop an expectation whose stream has just
/// committed `FundingExclusionRecorded` but not yet written the exclusion,
/// leaving the poller neither guard. Service startup rebuilds it before the
/// pollers spawn, and the offline CLI runs with the service stopped.
pub(crate) async fn burn_excess_store(
    pool: Pool<Sqlite>,
) -> Result<Arc<Store<BurnExcess>>, BurnExcessEngineError> {
    crate::prepare_event_sourced_startup::<BurnExcess>(&pool).await?;
    rebuild_funding_exclusion_index(&pool).await?;
    Ok(StoreBuilder::<BurnExcess>::new(pool.clone())
        .with(Arc::new(FundingExclusionReactor::new(pool.clone())))
        .with(Arc::new(FundingExpectationReactor::new(pool)))
        .build(())
        .await?)
}

/// Full dry-run / execute orchestration for one deposit stream.
pub(crate) async fn run_burn_excess<P: Provider>(
    pool: &Pool<Sqlite>,
    vault_service: &dyn VaultService,
    provider: &P,
    issuer_wallet: Address,
    request: BurnExcessRequest,
    confirm: impl Fn(&str) -> io::Result<bool> + Send + Sync,
) -> Result<BurnExcessOutcome, BurnExcessEngineError> {
    let aggregate_id = BurnExcessId::new(request.deposit_tx_hash);
    let store = burn_excess_store(pool.clone()).await?;

    let state = store.load(&aggregate_id).await?;
    let path_resolution = resolve_path(request.mode, state.as_ref())?;

    match path_resolution {
        PathResolution::ReportOnly(path) => {
            if request.execute {
                release_terminal_expectation(pool, state.as_ref()).await?;
            }
            Ok(BurnExcessOutcome::Terminal(terminal_view(path, state.as_ref())))
        }
        PathResolution::Start(path) | PathResolution::Resume(path) => {
            if request.close {
                let view = close_stream(
                    &store,
                    &aggregate_id,
                    state.as_ref(),
                    path,
                    &request.reason,
                    request.execute,
                    &confirm,
                )
                .await?;
                // Dual-write: closing ends any funding expectation, and the
                // poller must stop holding a Transfer for a stream that will
                // never exclude it.
                if !view.dry_run {
                    clear_funding_expectation(pool, request.deposit_tx_hash)
                        .await?;
                }
                return Ok(BurnExcessOutcome::Close(view));
            }

            if request.mode == BurnExcessMode::ExpectFunding {
                return expect_funding(
                    ExpectFundingCtx {
                        pool,
                        vault_service,
                        provider,
                        issuer_wallet,
                        store: &store,
                    },
                    &request,
                    state.as_ref(),
                    &confirm,
                )
                .await;
            }

            // The live poller reads the funding Transfer as soon as it is
            // mined, so only an expectation recorded before then keeps it
            // from being redeemed; without one there is nothing to rely on.
            if path == BurnExcessPath::External
                && request.poller_guard == PollerGuard::FundingExpected
                && state.is_none()
            {
                return Err(BurnExcessEngineError::FundingNotExpected);
            }

            let plan = prove_plan(
                pool,
                vault_service,
                provider,
                issuer_wallet,
                &request,
                state.as_ref(),
                path,
            )
            .await?;

            let view = plan_view(&plan, request.execute, request.poller_guard);

            if !request.execute {
                return Ok(BurnExcessOutcome::Plan(Box::new(view)));
            }

            execute_plan(
                MutationCtx {
                    pool,
                    vault_service,
                    provider,
                    store: &store,
                    aggregate_id: &aggregate_id,
                    plan: &plan,
                    request: &request,
                },
                state.as_ref(),
                &confirm,
            )
            .await?;

            // The expectation held same-shape Transfers through the burn; a
            // completed stream releases them to the redemption flow.
            let finished = store.load(&aggregate_id).await?;
            release_terminal_expectation(pool, finished.as_ref()).await?;

            Ok(BurnExcessOutcome::Plan(Box::new(view)))
        }
    }
}

/// Drops the funding expectation of a terminal stream. A live Path B stream
/// keeps its expectation through the burn so other Transfers of the same shape
/// stay held (see `require_issuer_share_balance`); once the stream completes
/// or closes there is nothing left to hold. Also heals a row left by a clear
/// that failed, or put back by a retried `expect-funding` that raced the run.
/// A closed stream releases its hold by definition; a completed one only once
/// its exclusion row is in, so the funding log always has one guard.
async fn release_terminal_expectation(
    pool: &Pool<Sqlite>,
    state: Option<&BurnExcess>,
) -> Result<(), BurnExcessEngineError> {
    let releasable = match state {
        Some(BurnExcess::Closed { bind, .. }) => Some(bind),
        Some(BurnExcess::Completed {
            bind,
            funding_log_id: Some(funding),
            ..
        }) if is_excluded_funding_log(
            pool,
            funding.network,
            funding.vault,
            funding.tx_hash,
            funding.log_index,
        )
        .await? =>
        {
            Some(bind)
        }
        _ => None,
    };

    if let (Some(bind), Some(terminal)) = (releasable, state)
        && clear_funding_expectation(pool, bind.deposit_tx_hash).await?
    {
        info!(
            target: "burn_excess",
            deposit_tx_hash = %bind.deposit_tx_hash,
            vault = %bind.vault,
            state = terminal.state_name(),
            "Released the funding expectation of a terminal stream; Transfers \
             it held are now detected as ordinary redemptions unless excluded"
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ProvenPlan {
    path: BurnExcessPath,
    bind: ExcessBurnBind,
    deposit_proof: DepositProof,
    funding_log_id: Option<FundingTransferId>,
    /// From `FundingExclusionRecorded.excluded_at` when resuming Path B, so
    /// index repair re-inserts the event timestamp rather than `Utc::now()`.
    exclusion_excluded_at: Option<DateTime<Utc>>,
    underlying: UnderlyingSymbol,
    freeze_advisory: Option<&'static str>,
    resume_note: Option<&'static str>,
}

/// Serializable outcome of a burn-excess run: the structured plan or terminal
/// report the engine used to only print. Returned to the CLI (which renders it)
/// and the breakglass HTTP route (which serializes it into the response), so a
/// dry-run shows exactly what would burn instead of only logging it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BurnExcessOutcome {
    /// A proven start/resume plan (a dry-run, or the plan that was executed).
    Plan(Box<BurnExcessPlanView>),
    /// A stream already in a terminal state; report only.
    Terminal(BurnExcessTerminalView),
    /// A close of a dead intended/submitted stream.
    Close(BurnExcessCloseView),
}

/// Serializable view of a proven [`ProvenPlan`]: the exact effect an operator
/// reviews before committing with `execute=true`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BurnExcessPlanView {
    pub(crate) path: BurnExcessPath,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) bind: ExcessBurnBind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) funding_log: Option<FundingTransferId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resume_note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) freeze_advisory: Option<String>,
    /// Path B only: what must hold for the burn to be safe from the redemption
    /// poller: offline, that the issuer service is stopped; on the live
    /// route, that the funding expectation was recorded before the funding
    /// Transfer was broadcast; for `expect-funding`, the exact Transfer to
    /// broadcast next.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) precondition: Option<String>,
    /// A dry-run proves and returns this plan without writing events, signing,
    /// or recording an exclusion.
    pub(crate) dry_run: bool,
}

/// Serializable view of a terminal [`BurnExcess`] stream for the report-only
/// path.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum BurnExcessTerminalView {
    Completed {
        path: BurnExcessPath,
        burn_tx_hash: B256,
        block_number: u64,
        completed_at: DateTime<Utc>,
        #[serde(skip_serializing_if = "Option::is_none")]
        funding_log: Option<FundingTransferId>,
    },
    Closed {
        path: BurnExcessPath,
        reason: String,
        closed_at: DateTime<Utc>,
        #[serde(skip_serializing_if = "Option::is_none")]
        funding_log: Option<FundingTransferId>,
    },
    Other {
        path: BurnExcessPath,
        state: String,
    },
}

/// Serializable view of a close plan for a dead intended/submitted stream.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BurnExcessCloseView {
    pub(crate) path: BurnExcessPath,
    pub(crate) state: String,
    pub(crate) reason: String,
    pub(crate) dry_run: bool,
}

async fn prove_plan<P: Provider>(
    pool: &Pool<Sqlite>,
    vault_service: &dyn VaultService,
    provider: &P,
    issuer_wallet: Address,
    request: &BurnExcessRequest,
    state: Option<&BurnExcess>,
    path: BurnExcessPath,
) -> Result<ProvenPlan, BurnExcessEngineError> {
    require_wallet_intent_gates(pool, request.network, request.deposit_tx_hash)
        .await?;

    let ProvenBind { bind, deposit_proof, underlying } =
        prove_bind(pool, provider, issuer_wallet, request).await?;
    let listed_vault = bind.vault;

    // Path A safety: internal only when the deposit left shares with the issuer.
    // A non-issuer original recipient means shares need a funding Transfer first.
    if path == BurnExcessPath::Internal
        && deposit_proof.original_recipient != issuer_wallet
    {
        return Err(BurnExcessProofError::InternalRequiresIssuerAsRecipient {
            original_recipient: deposit_proof.original_recipient,
            issuer_wallet,
        }
        .into());
    }

    let (funding_log_id, exclusion_excluded_at, resume_note) =
        match (path, state) {
            (BurnExcessPath::Internal, _) => {
                let balance = vault_service
                    .get_share_balance(listed_vault, issuer_wallet)
                    .await?;
                require_exact_issuer_share_balance(balance, request.shares, 0)?;
                let note = match state {
                    Some(
                        BurnExcess::Intended { .. }
                        | BurnExcess::Submitted { .. },
                    ) => Some("resume; burn already intended/submitted"),
                    _ => None,
                };
                (None, None, note)
            }
            (
                BurnExcessPath::External,
                Some(BurnExcess::FundingExcluded {
                    funding_log_id,
                    excluded_at,
                    ..
                }),
            ) => {
                let funding_hash_arg = request
                    .funding_tx_hash
                    .ok_or(BurnExcessProofError::FundingTxHashRequired)?;
                require_funding_hash_match(funding_hash_arg, funding_log_id)?;
                require_issuer_share_balance(
                    pool,
                    provider,
                    vault_service,
                    &bind,
                    Some(funding_log_id),
                )
                .await?;
                (
                    Some(funding_log_id.clone()),
                    Some(*excluded_at),
                    Some("resume; exclusion already recorded"),
                )
            }
            (
                BurnExcessPath::External,
                Some(
                    BurnExcess::Intended {
                        funding_log_id: Some(funding_log_id),
                        ..
                    }
                    | BurnExcess::Submitted {
                        funding_log_id: Some(funding_log_id),
                        ..
                    },
                ),
            ) => {
                let funding_hash_arg = request
                    .funding_tx_hash
                    .ok_or(BurnExcessProofError::FundingTxHashRequired)?;
                require_funding_hash_match(funding_hash_arg, funding_log_id)?;
                require_issuer_share_balance(
                    pool,
                    provider,
                    vault_service,
                    &bind,
                    Some(funding_log_id),
                )
                .await?;
                (
                    Some(funding_log_id.clone()),
                    None,
                    Some("resume; burn already intended/submitted"),
                )
            }
            (BurnExcessPath::External, _) => {
                let funding_tx_hash = request
                    .funding_tx_hash
                    .ok_or(BurnExcessProofError::FundingTxHashRequired)?;
                let funding_log_id = prove_funding_transfer(
                    pool,
                    provider,
                    FundingTransferExpectation {
                        network: request.network,
                        vault: listed_vault,
                        tx_hash: funding_tx_hash,
                        from: deposit_proof.original_recipient,
                        to: issuer_wallet,
                        amount: request.shares,
                    },
                )
                .await?;
                require_issuer_share_balance(
                    pool,
                    provider,
                    vault_service,
                    &bind,
                    Some(&funding_log_id),
                )
                .await?;
                (Some(funding_log_id), None, None)
            }
        };

    let freeze_advisory = match crate::underlying::load_freeze_status(
        pool,
        &underlying,
    )
    .await
    {
        Ok(crate::underlying::AssetStatus::Frozen) => Some(
            "advisory: underlying is frozen (freeze is not a burn-excess gate)",
        ),
        Ok(crate::underlying::AssetStatus::Enabled) => None,
        Err(error) => {
            warn!(
                target: "burn_excess",
                error = %error,
                %underlying,
                "Failed to load freeze status; continuing without advisory"
            );
            None
        }
    };

    Ok(ProvenPlan {
        path,
        bind,
        deposit_proof,
        funding_log_id,
        exclusion_excluded_at,
        underlying,
        freeze_advisory,
        resume_note,
    })
}

/// A deposit bind proven against the chain: the facts every path burns by.
struct ProvenBind {
    bind: ExcessBurnBind,
    deposit_proof: DepositProof,
    underlying: UnderlyingSymbol,
}

/// Proves the duplicate deposit and that the issuer holds its receipt, and
/// binds the stream to them. Shared by every path and by `expect-funding`,
/// which needs the bind (who funds, how much, into which vault) before any
/// funding Transfer exists.
async fn prove_bind<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    issuer_wallet: Address,
    request: &BurnExcessRequest,
) -> Result<ProvenBind, BurnExcessEngineError> {
    let (underlying, mint_network) =
        load_mint_asset(pool, &request.issuer_request_id).await?;
    if mint_network != request.network {
        return Err(BurnExcessEngineError::MintNetworkMismatch {
            issuer_request_id: request.issuer_request_id.clone(),
            mint_network,
            requested: request.network,
        });
    }

    let listed_vault =
        find_vault(pool, &underlying, &request.network).await?.ok_or_else(
            || BurnExcessEngineError::VaultNotListed {
                underlying: underlying.clone(),
                network: request.network,
            },
        )?;

    let deposit_proof =
        fetch_deposit_proof(provider, request.deposit_tx_hash, listed_vault)
            .await?;
    bind_deposit_proof(
        &request.issuer_request_id,
        request.receipt_id,
        request.shares,
        &deposit_proof,
    )?;

    let receipt_contract =
        receipt_contract_address(provider, listed_vault).await?;
    let receipt_balance = receipt_balance_of(
        provider,
        receipt_contract,
        issuer_wallet,
        request.receipt_id,
    )
    .await?;
    require_issuer_receipt_balance(
        request.receipt_id,
        receipt_balance,
        request.shares,
    )?;

    let bind = ExcessBurnBind {
        issuer_request_id: request.issuer_request_id.clone(),
        deposit_tx_hash: request.deposit_tx_hash,
        receipt_id: request.receipt_id,
        shares: request.shares,
        original_recipient: deposit_proof.original_recipient,
        vault: listed_vault,
        network: request.network,
        issuer_wallet,
    };

    Ok(ProvenBind { bind, deposit_proof, underlying })
}

fn plan_view(
    plan: &ProvenPlan,
    execute: bool,
    poller_guard: PollerGuard,
) -> BurnExcessPlanView {
    let precondition = match (plan.path, poller_guard) {
        (BurnExcessPath::External, PollerGuard::ServiceStopped) => Some(
            "the issuer service must be STOPPED; a running transfer poller can \
             open a Redemption for the funding Transfer first and steer this \
             recovery onto the Alpaca path"
                .to_string(),
        ),
        (BurnExcessPath::External, PollerGuard::FundingExpected) => Some(
            "the funding expectation must have been recorded before the \
             funding Transfer was broadcast; the poller holds the Transfer \
             only while it is expected, and one it redeemed before refuses \
             this run"
                .to_string(),
        ),
        (BurnExcessPath::Internal, _) => None,
    };

    BurnExcessPlanView {
        path: plan.path,
        underlying: plan.underlying.clone(),
        bind: plan.bind.clone(),
        funding_log: plan.funding_log_id.clone(),
        resume_note: plan.resume_note.map(str::to_string),
        freeze_advisory: plan.freeze_advisory.map(str::to_string),
        precondition,
        dry_run: !execute,
    }
}

/// Shared handles for the `expect-funding` step (avoids a long arg list).
struct ExpectFundingCtx<'a, P> {
    pool: &'a Pool<Sqlite>,
    vault_service: &'a dyn VaultService,
    provider: &'a P,
    issuer_wallet: Address,
    store: &'a Store<BurnExcess>,
}

/// The live route's first Path B step: proves the deposit bind and, on
/// execute, records the funding Transfer the stream expects (from the
/// deposit's original recipient to the issuer wallet, exactly the excess
/// shares) so the poller holds it instead of opening a Redemption. The
/// operator broadcasts the funding Transfer only after this succeeds.
async fn expect_funding<P: Provider>(
    ctx: ExpectFundingCtx<'_, P>,
    request: &BurnExcessRequest,
    state: Option<&BurnExcess>,
    confirm: &(impl Fn(&str) -> io::Result<bool> + Send + Sync),
) -> Result<BurnExcessOutcome, BurnExcessEngineError> {
    let ExpectFundingCtx {
        pool,
        vault_service,
        provider,
        issuer_wallet,
        store,
    } = ctx;
    let resume_note = match state {
        None => None,
        Some(BurnExcess::AwaitingFunding { .. }) => {
            Some("funding expectation already recorded")
        }
        Some(other) => {
            // Past the expectation: the exclusion already covers the funding
            // Transfer, so there is nothing to hold; continue with `external`.
            return Ok(BurnExcessOutcome::Terminal(terminal_view(
                BurnExcessPath::External,
                Some(other),
            )));
        }
    };

    // Another unresolved recovery would refuse every `external` run on this
    // stream until it finishes, while the poller holds this vault's funding
    // Transfer; refuse before the operator is asked to send it.
    let this_stream = BurnExcessId::new(request.deposit_tx_hash);
    if has_unresolved_excess_burn_intent(pool, Some(&this_stream)).await? {
        return Err(BurnExcessEngineError::UnresolvedExcessBurnIntent);
    }

    let ProvenBind { bind, underlying, .. } =
        prove_bind(pool, provider, issuer_wallet, request).await?;

    // Shares minted to the issuer are already there: that deposit is
    // `internal`. An expectation would lock it to a path whose funding cannot
    // exist, leaving its excess unburnable in the wallet.
    if bind.original_recipient == issuer_wallet {
        return Err(
            BurnExcessProofError::FundingFromIssuer { issuer_wallet }.into()
        );
    }

    require_no_competing_funding(pool, &bind).await?;

    let view = BurnExcessPlanView {
        path: BurnExcessPath::External,
        underlying,
        bind: bind.clone(),
        funding_log: None,
        resume_note: resume_note.map(str::to_string),
        freeze_advisory: None,
        precondition: Some(format!(
            "broadcast the funding Transfer only after this expectation is \
             recorded: exactly {} shares (raw) from {} to {} on vault {}",
            bind.shares,
            bind.original_recipient,
            bind.issuer_wallet,
            bind.vault
        )),
        dry_run: !request.execute,
    };

    if !request.execute {
        return Ok(BurnExcessOutcome::Plan(Box::new(view)));
    }

    if !confirm(&format!(
        "Record the funding expectation for deposit {:#x} (path B external) \
         so the redemption poller holds its funding Transfer?",
        request.deposit_tx_hash
    ))? {
        return Err(BurnExcessEngineError::Aborted);
    }

    // Re-checked under the wallet lock, which every other expect-funding and
    // external run takes before writing, so two concurrent requests cannot
    // both pass the checks above and both record.
    let _wallet_guard = vault_service.lock_wallet().await;
    require_no_competing_funding(pool, &bind).await?;

    let aggregate_id = BurnExcessId::new(request.deposit_tx_hash);
    let expected_at = Utc::now();
    store
        .send(
            &aggregate_id,
            BurnExcessCommand::ExpectFunding {
                bind: bind.clone(),
                reason: request.reason.clone(),
                incident_id: request.incident_id.clone(),
            },
        )
        .await?;

    // Dual-write, as for the exclusion: the reactor cannot fail the command,
    // and the operator broadcasts as soon as this answers, so the hold must
    // already be durable.
    record_funding_expectation(pool, &bind, expected_at).await?;

    // A retried request sends a no-op command, so `external` or `--close`
    // may have moved the stream on, and a terminal stream cleared its
    // expectation, before the write above put it back. Re-read the stream
    // after the write: past `AwaitingFunding`, the row is the stream's own
    // until it completes or closes, so release it only then (see
    // `release_terminal_expectation`) and report the stream as it is.
    let current = store.load(&aggregate_id).await?;
    if !matches!(current, Some(BurnExcess::AwaitingFunding { .. })) {
        release_terminal_expectation(pool, current.as_ref()).await?;
        return Ok(BurnExcessOutcome::Terminal(terminal_view(
            BurnExcessPath::External,
            current.as_ref(),
        )));
    }

    if !is_expected_funding(
        pool,
        bind.network,
        bind.vault,
        bind.original_recipient,
        bind.issuer_wallet,
        bind.shares,
    )
    .await?
    {
        return Err(BurnExcessEngineError::FundingExpectationIndexMissing {
            deposit_tx_hash: request.deposit_tx_hash,
        });
    }

    info!(
        target: "burn_excess",
        deposit = %aggregate_id,
        from = %bind.original_recipient,
        amount = %bind.shares,
        "Recorded funding expectation"
    );

    Ok(BurnExcessOutcome::Plan(Box::new(view)))
}

/// Refuses an expectation the issuer wallet could not settle: another
/// stream's unresolved recovery refuses every `external` run on this one until
/// it finishes, and another open expectation on the same vault would put two
/// fundings in the wallet, so neither `external` run could see the exact share
/// balance it burns. In both cases the poller would hold this vault's funding
/// Transfer with no way to release it but `--close`.
async fn require_no_competing_funding(
    pool: &Pool<Sqlite>,
    bind: &ExcessBurnBind,
) -> Result<(), BurnExcessEngineError> {
    let this_stream = BurnExcessId::new(bind.deposit_tx_hash);
    if has_unresolved_excess_burn_intent(pool, Some(&this_stream)).await? {
        return Err(BurnExcessEngineError::UnresolvedExcessBurnIntent);
    }
    if has_other_funding_expectation(
        pool,
        bind.network,
        bind.vault,
        bind.deposit_tx_hash,
    )
    .await?
    {
        return Err(BurnExcessEngineError::AnotherFundingExpected {
            vault: bind.vault,
        });
    }
    Ok(())
}

fn terminal_view(
    path: BurnExcessPath,
    state: Option<&BurnExcess>,
) -> BurnExcessTerminalView {
    match state {
        Some(
            terminal @ BurnExcess::Completed {
                burn_tx_hash,
                block_number,
                completed_at,
                ..
            },
        ) => BurnExcessTerminalView::Completed {
            path: terminal.path(),
            burn_tx_hash: *burn_tx_hash,
            block_number: *block_number,
            completed_at: *completed_at,
            funding_log: terminal.funding_log_id().cloned(),
        },
        Some(terminal @ BurnExcess::Closed { reason, closed_at, .. }) => {
            BurnExcessTerminalView::Closed {
                path: terminal.path(),
                reason: reason.clone(),
                closed_at: *closed_at,
                funding_log: terminal.funding_log_id().cloned(),
            }
        }
        other => BurnExcessTerminalView::Other {
            path,
            state: other.map_or_else(
                || "Uninitialized".to_string(),
                |stream| stream.state_name().to_string(),
            ),
        },
    }
}

async fn close_stream(
    store: &Store<BurnExcess>,
    aggregate_id: &BurnExcessId,
    state: Option<&BurnExcess>,
    path: BurnExcessPath,
    reason: &str,
    execute: bool,
    confirm: &(impl Fn(&str) -> io::Result<bool> + Send + Sync),
) -> Result<BurnExcessCloseView, BurnExcessEngineError> {
    const CLOSABLE: &str =
        "AwaitingFunding, FundingExcluded, Intended, or Submitted";
    let state = state.ok_or_else(|| super::BurnExcessError::InvalidState {
        expected: CLOSABLE.to_string(),
        found: "Uninitialized".to_string(),
    })?;

    match state {
        BurnExcess::AwaitingFunding { .. }
        | BurnExcess::FundingExcluded { .. }
        | BurnExcess::Intended { .. }
        | BurnExcess::Submitted { .. } => {}
        other => {
            return Err(super::BurnExcessError::InvalidState {
                expected: CLOSABLE.to_string(),
                found: other.state_name().to_string(),
            }
            .into());
        }
    }

    if !execute {
        return Ok(BurnExcessCloseView {
            path,
            state: state.state_name().to_string(),
            reason: reason.to_string(),
            dry_run: true,
        });
    }

    let effect = if matches!(state, BurnExcess::AwaitingFunding { .. }) {
        "releases the funding-Transfer hold; a funding Transfer already sent \
         will then be detected as an ordinary redemption"
    } else {
        "clears wallet gates only"
    };
    if !confirm(&format!(
        "Close dead excess-burn stream {aggregate_id} (path={path}; {effect})?"
    ))? {
        return Err(BurnExcessEngineError::Aborted);
    }

    store
        .send(
            aggregate_id,
            BurnExcessCommand::CloseExcessBurn { reason: reason.to_string() },
        )
        .await?;

    // The stream is now `Closed`; report that rather than the pre-close state.
    Ok(BurnExcessCloseView {
        path,
        state: "Closed".to_string(),
        reason: reason.to_string(),
        dry_run: false,
    })
}

/// Shared handles for burn-excess mutation steps (avoids long arg lists).
struct MutationCtx<'a, P> {
    pool: &'a Pool<Sqlite>,
    vault_service: &'a dyn VaultService,
    provider: &'a P,
    store: &'a Store<BurnExcess>,
    aggregate_id: &'a BurnExcessId,
    plan: &'a ProvenPlan,
    request: &'a BurnExcessRequest,
}

struct ConfirmCtx<'a, P> {
    pool: &'a Pool<Sqlite>,
    vault_service: &'a dyn VaultService,
    provider: &'a P,
    store: &'a Store<BurnExcess>,
    aggregate_id: &'a BurnExcessId,
    tx_id: crate::vault::TxId,
    dust_shares: U256,
    receipt_id: U256,
    shares: U256,
    bind: &'a ExcessBurnBind,
    owner: Address,
    chain_id: u64,
}

async fn execute_plan<P: Provider>(
    mutation: MutationCtx<'_, P>,
    state: Option<&BurnExcess>,
    confirm: &(impl Fn(&str) -> io::Result<bool> + Send + Sync),
) -> Result<(), BurnExcessEngineError> {
    match state {
        None | Some(BurnExcess::AwaitingFunding { .. }) => {
            let prompt = match mutation.plan.path {
                BurnExcessPath::Internal => format!(
                    "Sign and persist excess burn for deposit {:#x} (path A \
                     internal), then broadcast?",
                    mutation.request.deposit_tx_hash
                ),
                BurnExcessPath::External => format!(
                    "Record funding exclusion for deposit {:#x} (path B \
                     external), then sign/broadcast the excess burn?",
                    mutation.request.deposit_tx_hash
                ),
            };
            if !confirm(&prompt)? {
                return Err(BurnExcessEngineError::Aborted);
            }

            // Held from before the exclusion through the persisted intent, so
            // two runs on different streams cannot both record an exclusion
            // and then refuse each other at the sign boundary.
            let wallet_guard = mutation.vault_service.lock_wallet().await;
            if mutation.plan.path == BurnExcessPath::External {
                require_wallet_intent_gates(
                    mutation.pool,
                    mutation.request.network,
                    mutation.request.deposit_tx_hash,
                )
                .await?;
                let funding = mutation
                    .plan
                    .funding_log_id
                    .clone()
                    .ok_or(BurnExcessProofError::FundingTxHashRequired)?;
                // Irreversible boundary: re-check race vs redemption poller.
                if redemption_exists_for_tx(mutation.pool, funding.tx_hash)
                    .await?
                {
                    return Err(BurnExcessProofError::FundingAlreadyRedeemed {
                        tx_hash: funding.tx_hash,
                        log_index: funding.log_index,
                    }
                    .into());
                }
                record_exclusion(
                    mutation.pool,
                    mutation.store,
                    mutation.aggregate_id,
                    mutation.plan.bind.clone(),
                    funding,
                    &mutation.request.reason,
                    mutation.request.incident_id.clone(),
                )
                .await?;
            }

            intend_submit_confirm(&mutation, wallet_guard).await
        }
        Some(BurnExcess::FundingExcluded { funding_log_id, .. }) => {
            if !confirm(&format!(
                "Sign and persist excess burn for deposit {:#x} (exclusion \
                 already recorded log_index={})?",
                mutation.request.deposit_tx_hash, funding_log_id.log_index
            ))? {
                return Err(BurnExcessEngineError::Aborted);
            }
            let wallet_guard = mutation.vault_service.lock_wallet().await;
            intend_submit_confirm(&mutation, wallet_guard).await
        }
        Some(BurnExcess::Intended { sendable_tx, bind, .. }) => {
            if !confirm(&format!(
                "Resume broadcast of persisted excess burn \
                 sendable_tx.hash={:#x} for deposit {:#x}?",
                sendable_tx.hash, mutation.request.deposit_tx_hash
            ))? {
                return Err(BurnExcessEngineError::Aborted);
            }
            resume_from_intended(
                &mutation,
                bind.issuer_wallet,
                sendable_tx.clone(),
                bind.receipt_id,
                bind.shares,
                bind,
            )
            .await
        }
        Some(BurnExcess::Submitted { sendable_tx, tx_id, bind, .. }) => {
            if !confirm(&format!(
                "Resume confirmation of submitted excess burn \
                 sendable_tx.hash={:#x} for deposit {:#x}?",
                sendable_tx.hash, mutation.request.deposit_tx_hash
            ))? {
                return Err(BurnExcessEngineError::Aborted);
            }
            resume_from_submitted(
                &mutation,
                bind.issuer_wallet,
                sendable_tx.clone(),
                tx_id.clone(),
                bind.receipt_id,
                bind.shares,
                bind,
            )
            .await
        }
        Some(BurnExcess::Completed { .. } | BurnExcess::Closed { .. }) => {
            Ok(())
        }
    }
}

async fn record_exclusion(
    pool: &Pool<Sqlite>,
    store: &Store<BurnExcess>,
    aggregate_id: &BurnExcessId,
    bind: ExcessBurnBind,
    funding_log_id: FundingTransferId,
    reason: &str,
    incident_id: Option<String>,
) -> Result<(), BurnExcessEngineError> {
    let excluded_at = Utc::now();
    store
        .send(
            aggregate_id,
            BurnExcessCommand::RecordFundingExclusion {
                bind,
                funding_log_id: funding_log_id.clone(),
                reason: reason.to_string(),
                incident_id,
            },
        )
        .await?;

    // Dual-write: reactor is best-effort (Never cannot fail the command). The
    // engine must own durability before any prepare/sign/broadcast.
    record_funding_exclusion(
        pool,
        &funding_log_id,
        aggregate_id.deposit_tx_hash(),
        excluded_at,
    )
    .await?;

    if !is_excluded_funding_log(
        pool,
        funding_log_id.network,
        funding_log_id.vault,
        funding_log_id.tx_hash,
        funding_log_id.log_index,
    )
    .await?
    {
        return Err(BurnExcessEngineError::FundingExclusionIndexMissing {
            tx_hash: funding_log_id.tx_hash,
            log_index: funding_log_id.log_index,
        });
    }

    info!(
        target: "burn_excess",
        deposit = %aggregate_id,
        funding_tx = %format!("{:#x}", funding_log_id.tx_hash),
        log_index = funding_log_id.log_index,
        "Recorded funding exclusion"
    );
    eprintln!(
        "Recorded funding exclusion tx={:#x} log_index={}",
        funding_log_id.tx_hash, funding_log_id.log_index
    );
    Ok(())
}

async fn require_wallet_intent_gates(
    pool: &Pool<Sqlite>,
    network: Network,
    deposit_tx_hash: B256,
) -> Result<(), BurnExcessEngineError> {
    // The reservation is keyed by network alone, so this single check covers
    // both an unresolved mint and an unresolved redemption burn holding a
    // signed nonce on the signer this excess burn would use. BurnExcess is not
    // tracked in that table, so its own intents need the separate check below.
    if has_unresolved_signer_intent(pool, network, None).await? {
        return Err(BurnExcessEngineError::UnresolvedSignerIntent { network });
    }
    let excluding = Some(&BurnExcessId::new(deposit_tx_hash));
    if has_unresolved_excess_burn_intent(pool, excluding).await? {
        return Err(BurnExcessEngineError::UnresolvedExcessBurnIntent);
    }
    Ok(())
}

async fn ensure_path_b_exclusion_indexed(
    pool: &Pool<Sqlite>,
    plan: &ProvenPlan,
) -> Result<(), BurnExcessEngineError> {
    let Some(funding) = plan.funding_log_id.as_ref() else {
        return Ok(());
    };
    // Idempotent repair: the event is the source of truth, the SQL index is a
    // derived read model. Re-write it before refusing so a dual-write failure
    // after RecordFundingExclusion cannot permanently brick the stream.
    // Prefer the event's excluded_at so the index timestamp matches history.
    let excluded_at = plan.exclusion_excluded_at.unwrap_or_else(Utc::now);
    record_funding_exclusion(
        pool,
        funding,
        plan.bind.deposit_tx_hash,
        excluded_at,
    )
    .await?;
    if is_excluded_funding_log(
        pool,
        funding.network,
        funding.vault,
        funding.tx_hash,
        funding.log_index,
    )
    .await?
    {
        return Ok(());
    }
    Err(BurnExcessEngineError::FundingExclusionIndexMissing {
        tx_hash: funding.tx_hash,
        log_index: funding.log_index,
    })
}

/// Live-state gates re-read at the irreversible sign boundary.
///
/// `prove_plan` reads these before `print_plan` and the operator confirm
/// prompt, which blocks on stdin for an unbounded time. Balances can move in
/// that window, so a plan proven minutes ago must not be the last word before
/// `prepare_burn_tx` signs and fixes a nonce. The deposit proof and the funding
/// Transfer are mined history and cannot change, so only balances are re-read.
async fn require_issuer_balances<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    vault_service: &dyn VaultService,
    bind: &ExcessBurnBind,
    funding: Option<&FundingTransferId>,
) -> Result<(), BurnExcessEngineError> {
    let receipt_contract =
        receipt_contract_address(provider, bind.vault).await?;
    let receipt_balance = receipt_balance_of(
        provider,
        receipt_contract,
        bind.issuer_wallet,
        bind.receipt_id,
    )
    .await?;
    require_issuer_receipt_balance(
        bind.receipt_id,
        receipt_balance,
        bind.shares,
    )?;

    require_issuer_share_balance(pool, provider, vault_service, bind, funding)
        .await
}

/// The issuer must hold exactly the excess, plus the shares of the other
/// Transfers this stream's open funding expectation holds with `funding`.
/// The expectation matches by shape, so a genuine AP redemption of exactly the
/// excess from the original recipient is held with the funding Transfer until
/// the stream completes; its shares are in the wallet and will be redeemed
/// then, so they are counted. Any other surplus still refuses.
async fn require_issuer_share_balance<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    vault_service: &dyn VaultService,
    bind: &ExcessBurnBind,
    funding: Option<&FundingTransferId>,
) -> Result<(), BurnExcessEngineError> {
    let balance =
        vault_service.get_share_balance(bind.vault, bind.issuer_wallet).await?;
    if balance == bind.shares {
        return Ok(());
    }

    let held = held_same_shape_transfers(pool, provider, bind, funding).await?;
    require_exact_issuer_share_balance(balance, bind.shares, held.len())?;

    let held_tx_hashes: Vec<B256> =
        held.iter().map(|transfer| transfer.tx_hash).collect();
    info!(
        target: "burn_excess",
        deposit_tx_hash = %bind.deposit_tx_hash,
        vault = %bind.vault,
        held_transfers = held.len(),
        held_tx_hashes = ?held_tx_hashes,
        %balance,
        "Counted same-shape Transfers held with the funding Transfer toward \
         the issuer share balance; they are redeemed once the stream completes"
    );
    Ok(())
}

/// Transfers the open funding expectation of `bind`'s shape holds besides
/// `funding`: on the vault, from the original recipient to the issuer wallet,
/// of exactly the excess amount, past the vault's poll checkpoint (the poller
/// stops it before the first held Transfer, so every held one is past it),
/// neither excluded nor already a `Redemption`. With no such expectation (the
/// offline CLI, Path A) the poller holds nothing, and with no checkpoint it
/// has not read the vault yet; either way none are counted.
async fn held_same_shape_transfers<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    bind: &ExcessBurnBind,
    funding: Option<&FundingTransferId>,
) -> Result<Vec<FundingTransferId>, BurnExcessEngineError> {
    if !is_expected_funding(
        pool,
        bind.network,
        bind.vault,
        bind.original_recipient,
        bind.issuer_wallet,
        bind.shares,
    )
    .await?
    {
        return Ok(Vec::new());
    }
    let Some(from_block) = load_transfer_poll(pool, bind.network, bind.vault)
        .await?
        .and_then(|checkpoint| checkpoint.checked_add(1))
    else {
        return Ok(Vec::new());
    };
    let head_block = provider.get_block_number().await?;
    if from_block > head_block {
        return Ok(Vec::new());
    }

    let mut held_transfers = Vec::new();
    for (chunk_from, chunk_to) in
        block_ranges(from_block, head_block, BLOCK_CHUNK_SIZE)
    {
        let filter = Filter::new()
            .address(bind.vault)
            .event_signature(
                OffchainAssetReceiptVault::Transfer::SIGNATURE_HASH,
            )
            .topic1(bind.original_recipient.into_word())
            .topic2(bind.issuer_wallet.into_word())
            .from_block(chunk_from)
            .to_block(chunk_to);
        for log in provider.get_logs(&filter).await? {
            let Some(transfer) = same_shape_transfer(bind, &log) else {
                continue;
            };
            let is_funding = funding.is_some_and(|funding| {
                funding.tx_hash == transfer.tx_hash
                    && funding.log_index == transfer.log_index
            });
            if is_funding
                || is_excluded_funding_log(
                    pool,
                    transfer.network,
                    transfer.vault,
                    transfer.tx_hash,
                    transfer.log_index,
                )
                .await?
                || redemption_exists_for_tx(pool, transfer.tx_hash).await?
            {
                continue;
            }
            held_transfers.push(transfer);
        }
    }
    Ok(held_transfers)
}

/// The log as a Transfer of exactly `bind`'s funding shape, or `None` when it
/// is not one or lacks the identity the poller holds it by.
fn same_shape_transfer(
    bind: &ExcessBurnBind,
    log: &Log,
) -> Option<FundingTransferId> {
    let decoded =
        log.log_decode::<OffchainAssetReceiptVault::Transfer>().ok()?;
    let data = decoded.data();
    let same_shape = log.address() == bind.vault
        && data.from == bind.original_recipient
        && data.to == bind.issuer_wallet
        && data.value == bind.shares;
    same_shape.then_some(FundingTransferId {
        network: bind.network,
        vault: bind.vault,
        tx_hash: log.transaction_hash?,
        log_index: log.log_index?,
        from: data.from,
        to: data.to,
        amount: data.value,
    })
}

async fn intend_submit_confirm<P: Provider>(
    ctx: &MutationCtx<'_, P>,
    wallet_guard: WalletNonceGuard,
) -> Result<(), BurnExcessEngineError> {
    // Irreversible sign boundary: re-check gates, balances, and Path B
    // exclusion index. The caller holds the wallet from before this intent
    // check through broadcast because the intent check is what makes the
    // nonce safe: a concurrent mint, redemption burn, or excess burn on
    // another deposit runs its own check under the same lock, so it cannot
    // pass between this check and the persisted intent and sign a second
    // transaction on the nonce `prepare_burn_tx` fixes here.
    require_wallet_intent_gates(
        ctx.pool,
        ctx.request.network,
        ctx.request.deposit_tx_hash,
    )
    .await?;
    require_issuer_balances(
        ctx.pool,
        ctx.provider,
        ctx.vault_service,
        &ctx.plan.bind,
        ctx.plan.funding_log_id.as_ref(),
    )
    .await?;
    if ctx.plan.path == BurnExcessPath::External {
        ensure_path_b_exclusion_indexed(ctx.pool, ctx.plan).await?;
    }

    let params = multi_burn_params(ctx.plan);
    let sendable_tx = ctx.vault_service.prepare_burn_tx(&params).await?;

    ctx.store
        .send(
            ctx.aggregate_id,
            BurnExcessCommand::IntendExcessBurn {
                bind: ctx.plan.bind.clone(),
                path: ctx.plan.path,
                funding_log_id: ctx.plan.funding_log_id.clone(),
                reason: ctx.request.reason.clone(),
                incident_id: ctx.request.incident_id.clone(),
                sendable_tx: sendable_tx.clone(),
            },
        )
        .await?;
    eprintln!(
        "Persisted IntendExcessBurn hash={:#x} nonce={}",
        sendable_tx.hash, sendable_tx.nonce
    );

    let submitted =
        ctx.vault_service.submit_burn(params, sendable_tx.clone()).await?;
    ctx.store
        .send(
            ctx.aggregate_id,
            BurnExcessCommand::RecordExcessBurnSubmitted {
                tx_id: submitted.tx_id.clone(),
                burn_tx_hash: sendable_tx.hash,
            },
        )
        .await?;
    eprintln!("Submitted excess burn tx={:#x}", sendable_tx.hash);
    drop(wallet_guard);

    confirm_and_complete(&ConfirmCtx {
        pool: ctx.pool,
        vault_service: ctx.vault_service,
        provider: ctx.provider,
        store: ctx.store,
        aggregate_id: ctx.aggregate_id,
        tx_id: submitted.tx_id,
        dust_shares: sendable_tx.dust_shares,
        receipt_id: ctx.plan.bind.receipt_id,
        shares: ctx.plan.bind.shares,
        bind: &ctx.plan.bind,
        owner: ctx.plan.bind.issuer_wallet,
        chain_id: ctx.request.chain_id,
    })
    .await
}

async fn resume_from_intended<P: Provider>(
    ctx: &MutationCtx<'_, P>,
    owner: Address,
    sendable_tx: crate::vault::SendableTxWithHash,
    receipt_id: U256,
    shares: U256,
    bind: &ExcessBurnBind,
) -> Result<(), BurnExcessEngineError> {
    let status =
        ctx.vault_service.classify_burn_tx(owner, &sendable_tx).await?;
    match status {
        BurnTxStatus::Mined => {
            confirm_and_complete(&ConfirmCtx {
                pool: ctx.pool,
                vault_service: ctx.vault_service,
                provider: ctx.provider,
                store: ctx.store,
                aggregate_id: ctx.aggregate_id,
                tx_id: sendable_tx.hash.into(),
                dust_shares: sendable_tx.dust_shares,
                receipt_id,
                shares,
                bind,
                owner,
                chain_id: ctx.request.chain_id,
            })
            .await
        }
        BurnTxStatus::StillMineable => {
            // Rebroadcast persisted bytes; MultiBurnParams only supplies
            // external_tx_id metadata (deposit-scoped placeholders).
            let submitted = ctx
                .vault_service
                .submit_burn(
                    multi_burn_params_from_bind(bind, &Bytes::new(), None),
                    sendable_tx.clone(),
                )
                .await?;
            ctx.store
                .send(
                    ctx.aggregate_id,
                    BurnExcessCommand::RecordExcessBurnSubmitted {
                        tx_id: submitted.tx_id.clone(),
                        burn_tx_hash: sendable_tx.hash,
                    },
                )
                .await?;
            confirm_and_complete(&ConfirmCtx {
                pool: ctx.pool,
                vault_service: ctx.vault_service,
                provider: ctx.provider,
                store: ctx.store,
                aggregate_id: ctx.aggregate_id,
                tx_id: submitted.tx_id,
                dust_shares: sendable_tx.dust_shares,
                receipt_id,
                shares,
                bind,
                owner,
                chain_id: ctx.request.chain_id,
            })
            .await
        }
        BurnTxStatus::Reverted
        | BurnTxStatus::FinalizedReverted
        | BurnTxStatus::ProvablyDead => {
            Err(BurnExcessEngineError::DeadBurnIntent { status })
        }
    }
}

async fn resume_from_submitted<P: Provider>(
    ctx: &MutationCtx<'_, P>,
    owner: Address,
    sendable_tx: crate::vault::SendableTxWithHash,
    tx_id: crate::vault::TxId,
    receipt_id: U256,
    shares: U256,
    bind: &ExcessBurnBind,
) -> Result<(), BurnExcessEngineError> {
    let status =
        ctx.vault_service.classify_burn_tx(owner, &sendable_tx).await?;
    match status {
        BurnTxStatus::Mined | BurnTxStatus::StillMineable => {
            confirm_and_complete(&ConfirmCtx {
                pool: ctx.pool,
                vault_service: ctx.vault_service,
                provider: ctx.provider,
                store: ctx.store,
                aggregate_id: ctx.aggregate_id,
                tx_id,
                dust_shares: sendable_tx.dust_shares,
                receipt_id,
                shares,
                bind,
                owner,
                chain_id: ctx.request.chain_id,
            })
            .await
        }
        BurnTxStatus::Reverted
        | BurnTxStatus::FinalizedReverted
        | BurnTxStatus::ProvablyDead => {
            Err(BurnExcessEngineError::DeadBurnIntent { status })
        }
    }
}

async fn confirm_and_complete<P: Provider>(
    ctx: &ConfirmCtx<'_, P>,
) -> Result<(), BurnExcessEngineError> {
    let result =
        ctx.vault_service.confirm_burn(&ctx.tx_id, ctx.dust_shares).await?;

    let onchain: Vec<(U256, U256)> = result
        .burns
        .iter()
        .map(|burn| (burn.receipt_id, burn.shares_burned))
        .collect();
    if onchain.as_slice() != [(ctx.receipt_id, ctx.shares)] {
        return Err(BurnExcessEngineError::BurnDeltaMismatch {
            expected_receipt: ctx.receipt_id,
            expected_shares: ctx.shares,
            onchain,
        });
    }

    // Complete after on-chain verify so the stream is not stuck Intended if
    // inventory reconcile fails. Reconcile failures still fail the CLI so ops
    // re-run report-only + manual inventory.
    ctx.store
        .send(
            ctx.aggregate_id,
            BurnExcessCommand::CompleteExcessBurn {
                burn_tx_hash: result.tx_hash,
                block_number: result.block_number,
            },
        )
        .await?;

    eprintln!(
        "Completed excess burn tx={:#x} block={} receipt_id={} shares={}",
        result.tx_hash, result.block_number, ctx.receipt_id, ctx.shares
    );

    // Before reconcile, which fails the CLI on error: the operator most needs
    // the final balance in exactly that case — burn landed, Complete
    // persisted, inventory read model now stale.
    let share_balance =
        ctx.vault_service.get_share_balance(ctx.bind.vault, ctx.owner).await?;
    eprintln!(
        "post-burn: issuer_share_balance={share_balance} (delta expected -{})",
        ctx.shares
    );

    reconcile_inventory_after_burn(
        ctx.pool,
        ctx.provider,
        ctx.chain_id,
        ctx.bind.vault,
        ctx.owner,
        ctx.receipt_id,
    )
    .await?;

    Ok(())
}

async fn reconcile_inventory_after_burn<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    chain_id: u64,
    vault: Address,
    owner: Address,
    receipt_id: U256,
) -> Result<(), BurnExcessEngineError> {
    // The CLI is its own process and never runs the service startup that
    // reconciles this aggregate, so do it here. Reaching `build()` on a stale
    // schema fails the reconcile after `CompleteExcessBurn` is already
    // persisted — the one window where the burn is done and the read model is
    // not.
    crate::prepare_event_sourced_startup::<ReceiptInventory>(pool).await?;
    let inventory_store = StoreBuilder::<ReceiptInventory>::new(pool.clone())
        .build(())
        .await
        .map_err(|error| BurnExcessEngineError::StoreBuild {
            aggregate: "ReceiptInventory",
            message: error.to_string(),
        })?;

    let inventory = load_inventory(&inventory_store, chain_id, &vault).await?;
    let tracked = inventory
        .receipts_with_balance()
        .into_iter()
        .any(|row| row.receipt_id.inner() == receipt_id);

    if !tracked {
        eprintln!(
            "inventory: receipt {receipt_id} not tracked; skipping reconcile \
             (no Discover / no ReserveBurn)"
        );
        return Ok(());
    }

    let receipt_contract = receipt_contract_address(provider, vault).await?;
    let on_chain =
        receipt_balance_of(provider, receipt_contract, owner, receipt_id)
            .await?;

    if let Err(error) = send_receipt_inventory_command(
        &inventory_store,
        chain_id,
        &vault,
        ReceiptInventoryCommand::ReconcileBalance {
            receipt_id: ReceiptId::from(receipt_id),
            on_chain_balance: Shares::from(on_chain),
            observed_wallet: owner,
        },
    )
    .await
    {
        error!(
            target: "burn_excess",
            %receipt_id,
            error = %error,
            "Inventory reconcile after excess burn failed; on-chain burn \
             completed — re-run inventory reconcile / report-only"
        );
        return Err(BurnExcessEngineError::InventoryReconcileFailed {
            receipt_id,
            source: Box::new(error),
        });
    }

    eprintln!(
        "inventory: reconciled receipt {receipt_id} to on-chain balance \
         {on_chain}"
    );
    Ok(())
}

/// MultiBurnParams placeholders: reuse vault redeem path. `detected_tx_hash`
/// is deposit-scoped (not a Redemption aggregate).
fn multi_burn_params(plan: &ProvenPlan) -> MultiBurnParams {
    multi_burn_params_from_bind(
        &plan.bind,
        &plan.deposit_proof.receipt_info_bytes,
        Some(plan.deposit_proof.receipt_info.clone()),
    )
}

fn multi_burn_params_from_bind(
    bind: &ExcessBurnBind,
    receipt_info_bytes: &Bytes,
    receipt_info: Option<crate::vault::ReceiptInformation>,
) -> MultiBurnParams {
    MultiBurnParams {
        vault: bind.vault,
        burns: vec![MultiBurnEntry {
            receipt_id: bind.receipt_id,
            burn_shares: bind.shares,
            receipt_info,
            receipt_info_bytes: Some(receipt_info_bytes.clone()),
        }],
        dust_shares: U256::ZERO,
        owner: bind.issuer_wallet,
        user: bind.issuer_wallet,
        origin: BurnRequestOrigin::ExcessRecovery(BurnExcessId::new(
            bind.deposit_tx_hash,
        )),
        detected_tx_hash: bind.deposit_tx_hash,
        external_tx_id: None,
    }
}

async fn load_mint_asset(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<(UnderlyingSymbol, Network), BurnExcessEngineError> {
    crate::prepare_event_sourced_startup::<Mint>(pool).await?;
    let (store, _projection) = StoreBuilder::<Mint>::new(pool.clone())
        .build(())
        .await
        .map_err(|error| BurnExcessEngineError::StoreBuild {
            aggregate: "Mint",
            message: error.to_string(),
        })?;
    let mint = store.load(issuer_request_id).await?.ok_or_else(|| {
        BurnExcessEngineError::MintNotFound {
            issuer_request_id: issuer_request_id.clone(),
        }
    })?;

    match mint {
        Mint::Initiated { underlying, network, .. }
        | Mint::JournalConfirmed { underlying, network, .. }
        | Mint::JournalRejected { underlying, network, .. }
        | Mint::Minting { underlying, network, .. }
        | Mint::TxIntended { underlying, network, .. }
        | Mint::TxSubmitted { underlying, network, .. }
        | Mint::MintingFailed { underlying, network, .. }
        | Mint::CallbackPending { underlying, network, .. }
        | Mint::Completed { underlying, network, .. } => {
            Ok((underlying, network))
        }
        closed @ Mint::Closed { .. } => {
            Err(BurnExcessEngineError::MintMissingAsset {
                issuer_request_id: issuer_request_id.clone(),
                state: closed.state_name().to_string(),
            })
        }
    }
}

async fn fetch_deposit_proof<P: Provider>(
    provider: &P,
    deposit_tx_hash: B256,
    expected_vault: Address,
) -> Result<DepositProof, BurnExcessEngineError> {
    let receipt = provider
        .get_transaction_receipt(deposit_tx_hash)
        .await?
        .ok_or(BurnExcessEngineError::DepositTxInvalid {
            tx_hash: deposit_tx_hash,
        })?;
    if !receipt.status() {
        return Err(BurnExcessEngineError::DepositTxInvalid {
            tx_hash: deposit_tx_hash,
        });
    }

    parse_deposit_proof(&receipt, expected_vault, deposit_tx_hash)
}

fn parse_deposit_proof(
    receipt: &TransactionReceipt,
    expected_vault: Address,
    deposit_tx_hash: B256,
) -> Result<DepositProof, BurnExcessEngineError> {
    let mut deposit: Option<(U256, U256, Bytes, Address)> = None;
    let mut share_transfer_out: Option<Address> = None;

    for log in receipt.inner.logs() {
        if log.address() != expected_vault {
            continue;
        }

        if let Ok(decoded) =
            log.log_decode::<OffchainAssetReceiptVault::Deposit>()
        {
            if deposit.is_some() {
                return Err(BurnExcessEngineError::AmbiguousDepositTx {
                    tx_hash: deposit_tx_hash,
                });
            }
            let data = decoded.data();
            deposit = Some((
                data.id,
                data.shares,
                data.receiptInformation.clone(),
                data.owner,
            ));
            continue;
        }

        if let Ok(decoded) =
            log.log_decode::<OffchainAssetReceiptVault::Transfer>()
        {
            let transfer = decoded.data();
            // Production mint multicall: deposit(to=issuer) then
            // transfer(user, shares). The outbound Transfer after mint is the
            // original share recipient for Path B funding proofs.
            if let Some((_, shares, _, owner)) = deposit.as_ref()
                && transfer.from == *owner
                && transfer.to != Address::ZERO
                && transfer.value == *shares
            {
                if share_transfer_out.is_some() {
                    return Err(
                        BurnExcessEngineError::AmbiguousShareTransferOut {
                            tx_hash: deposit_tx_hash,
                        },
                    );
                }
                share_transfer_out = Some(transfer.to);
            }
        }
    }

    let Some((receipt_id, shares, receipt_info_bytes, deposit_owner)) = deposit
    else {
        return Err(BurnExcessEngineError::DepositTxInvalid {
            tx_hash: deposit_tx_hash,
        });
    };

    let receipt_info = decode_receipt_information_strict(&receipt_info_bytes)?;
    let original_recipient = share_transfer_out.unwrap_or(deposit_owner);

    Ok(DepositProof {
        receipt_id,
        shares,
        receipt_info,
        receipt_info_bytes,
        original_recipient,
        vault: expected_vault,
    })
}

async fn prove_funding_transfer<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    expectation: FundingTransferExpectation,
) -> Result<FundingTransferId, BurnExcessEngineError> {
    let receipt = provider
        .get_transaction_receipt(expectation.tx_hash)
        .await?
        .ok_or(BurnExcessEngineError::FundingTxInvalid {
            tx_hash: expectation.tx_hash,
        })?;
    if !receipt.status() {
        return Err(BurnExcessEngineError::FundingTxInvalid {
            tx_hash: expectation.tx_hash,
        });
    }

    // Tx-scoped race check before log candidates exist — no log_index yet.
    if redemption_exists_for_tx(pool, expectation.tx_hash).await? {
        return Err(BurnExcessProofError::FundingAlreadyRedeemedTx {
            tx_hash: expectation.tx_hash,
        }
        .into());
    }

    let mut candidates = Vec::new();
    for log in receipt.inner.logs() {
        let Ok(decoded) =
            log.log_decode::<OffchainAssetReceiptVault::Transfer>()
        else {
            continue;
        };
        let Some(log_index) = log.log_index else {
            return Err(BurnExcessProofError::FundingLogIndexMissing {
                tx_hash: expectation.tx_hash,
            }
            .into());
        };
        let data = decoded.data();
        candidates.push(FundingTransferCandidate {
            log_index,
            vault: log.address(),
            from: data.from,
            to: data.to,
            amount: data.value,
        });
    }

    Ok(select_funding_transfer(&expectation, &candidates)?)
}

async fn redemption_exists_for_tx(
    pool: &Pool<Sqlite>,
    tx_hash: B256,
) -> Result<bool, sqlx::Error> {
    let aggregate_id = IssuerRedemptionRequestId::new(tx_hash).to_string();
    let exists = sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
        )
        ",
    )
    .bind(aggregate_id)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

async fn receipt_contract_address<P: Provider>(
    provider: &P,
    vault: Address,
) -> Result<Address, BurnExcessEngineError> {
    let vault_contract = OffchainAssetReceiptVault::new(vault, provider);
    Ok(Address::from(vault_contract.receipt().call().await?.0))
}

async fn receipt_balance_of<P: Provider>(
    provider: &P,
    receipt_contract: Address,
    owner: Address,
    receipt_id: U256,
) -> Result<U256, BurnExcessEngineError> {
    let contract = Receipt::new(receipt_contract, provider);
    Ok(contract.balanceOf(owner, receipt_id).call().await?)
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Bytes, U256, address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, NonceFiller,
    };
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use event_sorcery::StoreBuilder;
    use parking_lot::Mutex;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use std::time::Duration;
    use tracing_test::traced_test;

    use super::*;
    use crate::account::ClientId;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::burn_excess::exclusion::is_excluded_funding_log;
    use crate::burn_excess::proof::BurnExcessMode;
    use crate::mint::{MintEvent, TokenizationRequestId};
    use crate::poll_checkpoint::load_transfer_poll;
    use crate::receipt_inventory::{
        ReceiptSource, ReceiptVaultKey, send_receipt_inventory_command,
    };
    use crate::redemption::test_utils::{
        link_ap_wallet, transfer_poller_for_tests,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm};
    use crate::tokenized_asset::{
        AssetKey, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::vault::ReceiptInformation;
    use crate::vault::mock::MockVaultService;
    use crate::vault::service::{RealBlockchainService, ResyncNonceManager};
    use crate::vault::{
        BurnTxStatus, MultiBurnResult, MultiBurnResultEntry, PreparedMintTx,
        SendableTxWithHash, VaultError,
    };
    use crate::{Quantity, VaultMode};

    const SHARES_RAW: u64 = 750_000_000_000_000_000;

    async fn pool() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn excess_shares() -> U256 {
        U256::from(SHARES_RAW)
    }

    fn sample_receipt_info(
        issuer_request_id: IssuerMintRequestId,
    ) -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-excess"),
            issuer_request_id,
            UnderlyingSymbol::new("PTY").unwrap(),
            Quantity::new(Decimal::new(750, 3)),
            Utc::now(),
            None,
        )
    }

    async fn seed_listing(
        pool: &Pool<Sqlite>,
        vault: Address,
    ) -> UnderlyingSymbol {
        let underlying = UnderlyingSymbol::new("PTY").unwrap();
        let (store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let key = AssetKey::new(underlying.clone(), Network::Base);
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tPTY"),
                    network: Network::Base,
                    vault,
                },
            )
            .await
            .unwrap();
        underlying
    }

    async fn seed_mint_initiated(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        underlying: &UnderlyingSymbol,
    ) {
        let initiated = MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("tok-excess"),
            quantity: Quantity::new(Decimal::new(750, 3)),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tPTY"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            initiated_at: Utc::now(),
            mint_mode: VaultMode::VaultDirect,
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
            VALUES ('Mint', ?, 1, ?, '1.0', ?, '{}')
            ",
        )
        .bind(issuer_request_id.to_string())
        .bind(initiated.event_type())
        .bind(serde_json::to_string(&initiated).unwrap())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn prepared_evm() -> (
        LocalEvm,
        RealBlockchainService,
        impl Provider + Clone,
        PrivateKeySigner,
    ) {
        let evm = LocalEvm::new().await.unwrap();
        evm.grant_deposit_role(evm.wallet_address).await.unwrap();
        evm.grant_withdraw_role(evm.wallet_address).await.unwrap();
        evm.grant_certify_role(evm.wallet_address).await.unwrap();
        evm.certify_vault(U256::MAX).await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let nonce_manager = ResyncNonceManager::default();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller::default())
            .filler(NonceFiller::new(nonce_manager.clone()))
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer.clone()))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let service =
            RealBlockchainService::new(provider.clone(), nonce_manager);
        (evm, service, provider, signer)
    }

    async fn mint_with_info(
        evm: &LocalEvm,
        to: Address,
        issuer_request_id: &IssuerMintRequestId,
    ) -> (U256, U256, Bytes, B256) {
        let info = sample_receipt_info(issuer_request_id.clone());
        let encoded = info.encode().unwrap();
        let (receipt_id, shares, bytes) = evm
            .mint_directly_with_info(excess_shares(), to, encoded)
            .await
            .unwrap();
        // LocalEvm doesn't return tx hash; fetch latest from chain via
        // balance-bearing deposit by scanning is hard — re-deposit path uses
        // mint_directly_with_info which returns shares. Read the last
        // transaction from the block.
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let block = provider
            .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
            .await
            .unwrap()
            .unwrap();
        let tx_hash = *block
            .transactions
            .as_hashes()
            .and_then(|hashes| hashes.last())
            .expect("deposit tx hash");
        assert_eq!(shares, excess_shares());
        (receipt_id, shares, bytes, tx_hash)
    }

    fn request(
        mode: BurnExcessMode,
        issuer_request_id: IssuerMintRequestId,
        deposit_tx_hash: B256,
        receipt_id: U256,
        funding_tx_hash: Option<B256>,
        execute: bool,
    ) -> BurnExcessRequest {
        BurnExcessRequest {
            mode,
            issuer_request_id,
            deposit_tx_hash,
            funding_tx_hash,
            receipt_id,
            shares: excess_shares(),
            reason: "duplicate mint recovery".into(),
            incident_id: Some("rai-1632-test".into()),
            network: Network::Base,
            chain_id: ANVIL_CHAIN_ID,
            execute,
            close: false,
            poller_guard: PollerGuard::ServiceStopped,
        }
    }

    #[tokio::test]
    async fn internal_dry_run_does_not_mutate() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;

        let (receipt_id, _, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;

        let before_share = service
            .get_share_balance(evm.vault_address, evm.wallet_address)
            .await
            .unwrap();

        let outcome = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let after_share = service
            .get_share_balance(evm.vault_address, evm.wallet_address)
            .await
            .unwrap();
        assert_eq!(before_share, after_share);

        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none()
        );

        // The deliverable: a dry-run returns the structured plan (what the
        // breakglass response serializes) instead of only logging it.
        let plan = match outcome {
            BurnExcessOutcome::Plan(plan) => plan,
            other => panic!("expected a dry-run plan, got: {other:?}"),
        };
        assert_eq!(plan.path, BurnExcessPath::Internal);
        assert!(plan.dry_run);
        assert_eq!(plan.bind.receipt_id, receipt_id);
        assert!(plan.funding_log.is_none());

        let body = serde_json::to_value(BurnExcessOutcome::Plan(plan)).unwrap();
        assert_eq!(body["plan"]["path"], "internal");
        assert_eq!(body["plan"]["dry_run"], true);
    }

    #[tokio::test]
    async fn internal_execute_burns_exact_shares_and_receipt() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;

        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;

        // Numeric shape: receipt id is sequential; assert shares match 0.750e18.
        assert_eq!(shares, excess_shares());

        let receipt_contract =
            receipt_contract_address(&provider, evm.vault_address)
                .await
                .unwrap();
        let receipt_before = receipt_balance_of(
            &provider,
            receipt_contract,
            evm.wallet_address,
            receipt_id,
        )
        .await
        .unwrap();
        assert!(receipt_before >= shares);

        // Track inventory + custody so post-burn reconcile can drop the row.
        let inventory_store =
            StoreBuilder::<ReceiptInventory>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        send_receipt_inventory_command(
            &inventory_store,
            ANVIL_CHAIN_ID,
            &evm.vault_address,
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: receipt_id.into(),
                balance: Shares::from(shares),
                block_number: 1,
                tx_hash: deposit_tx,
                source: ReceiptSource::External,
                receipt_info: None,
                receipt_info_bytes: None,
            },
        )
        .await
        .unwrap();
        send_receipt_inventory_command(
            &inventory_store,
            ANVIL_CHAIN_ID,
            &evm.vault_address,
            ReceiptInventoryCommand::ConfirmCustody {
                holder: evm.wallet_address,
            },
        )
        .await
        .unwrap();

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let share_after = service
            .get_share_balance(evm.vault_address, evm.wallet_address)
            .await
            .unwrap();
        assert_eq!(share_after, U256::ZERO);

        let receipt_after = receipt_balance_of(
            &provider,
            receipt_contract,
            evm.wallet_address,
            receipt_id,
        )
        .await
        .unwrap();
        assert_eq!(receipt_after, receipt_before - shares);

        let inventory = inventory_store
            .load(&ReceiptVaultKey::new(ANVIL_CHAIN_ID, evm.vault_address))
            .await
            .unwrap()
            .unwrap();
        assert!(
            inventory.receipts_with_balance().iter().all(|row| row
                .receipt_id
                .inner()
                != receipt_id
                || row.available_balance.is_zero()),
            "burned receipt must not remain available: {:?}",
            inventory.receipts_with_balance()
        );

        let store = burn_excess_store(pool.clone()).await.unwrap();
        let state =
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap();
        assert!(matches!(
            state,
            BurnExcess::Completed { path: BurnExcessPath::Internal, .. }
        ));
    }

    #[tokio::test]
    async fn external_fund_exclude_burn_and_poller_skips_only_that_log() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            funding_log_index,
            ..
        } = setup_external_path(&pool, &evm).await;

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::External,
                issuer_request_id.clone(),
                deposit_tx,
                receipt_id,
                Some(funding_tx),
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        assert_eq!(
            service
                .get_share_balance(evm.vault_address, evm.wallet_address)
                .await
                .unwrap(),
            U256::ZERO
        );

        assert!(
            is_excluded_funding_log(
                &pool,
                Network::Base,
                evm.vault_address,
                funding_tx,
                funding_log_index,
            )
            .await
            .unwrap(),
            "the proven funding log must be excluded"
        );

        // "Only that log": a neighbour in the same transaction stays eligible,
        // so the exclusion is a single log identity and not a tx-wide skip.
        assert!(
            !is_excluded_funding_log(
                &pool,
                Network::Base,
                evm.vault_address,
                funding_tx,
                funding_log_index.saturating_add(1),
            )
            .await
            .unwrap(),
            "a neighbouring log in the same tx must remain eligible"
        );

        // Terminal stream is report-only (no PathConflict); exclusion stays.
        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        assert!(
            is_excluded_funding_log(
                &pool,
                Network::Base,
                evm.vault_address,
                funding_tx,
                funding_log_index,
            )
            .await
            .unwrap(),
            "funding exclusion remains permanent after complete"
        );
    }

    #[tokio::test]
    async fn path_conflict_when_switching_mode_mid_stream() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (receipt_id, _, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;

        // Start internal by intending only: execute full burn then we can't
        // switch — use FundingExcluded seed for external lock instead.
        let store = burn_excess_store(pool.clone()).await.unwrap();
        let bind = ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash: deposit_tx,
            receipt_id,
            shares: excess_shares(),
            original_recipient: evm.wallet_address,
            vault: evm.vault_address,
            network: Network::Base,
            issuer_wallet: evm.wallet_address,
        };
        store
            .send(
                &BurnExcessId::new(deposit_tx),
                BurnExcessCommand::IntendExcessBurn {
                    bind,
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "mid".into(),
                    incident_id: None,
                    sendable_tx: crate::vault::SendableTxWithHash::default(),
                },
            )
            .await
            .unwrap();

        let err = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                Some(B256::ZERO),
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            BurnExcessEngineError::Proof(BurnExcessProofError::PathConflict {
                locked: BurnExcessPath::Internal,
                requested: BurnExcessMode::External,
            })
        ));
    }

    /// Shared Path B setup: deposit multicall (issuer deposit + transfer to
    /// recipient) and funding transfer back to issuer.
    /// Named rather than a tuple: callers pick out two or three fields each,
    /// and a positional mix-up here previously produced a wrong
    /// `funding_log_index`.
    struct ExternalPathFixture {
        issuer_request_id: IssuerMintRequestId,
        receipt_id: U256,
        deposit_tx: B256,
        funding_tx: B256,
        recipient: Address,
        funding_log_index: u64,
    }

    async fn setup_external_path(
        pool: &Pool<Sqlite>,
        evm: &LocalEvm,
    ) -> ExternalPathFixture {
        let deposit = setup_external_deposit(pool, evm).await;
        let recipient = deposit.recipient.address();
        let (funding_tx, funding_log_index) =
            send_external_funding(evm, deposit.recipient).await;

        ExternalPathFixture {
            issuer_request_id: deposit.issuer_request_id,
            receipt_id: deposit.receipt_id,
            deposit_tx: deposit.deposit_tx,
            funding_tx,
            recipient,
            funding_log_index,
        }
    }

    /// The Path B deposit alone: the excess shares sit with the recipient and
    /// the funding Transfer back to the issuer is not sent yet.
    struct ExternalDeposit {
        issuer_request_id: IssuerMintRequestId,
        receipt_id: U256,
        deposit_tx: B256,
        recipient: PrivateKeySigner,
    }

    async fn setup_external_deposit(
        pool: &Pool<Sqlite>,
        evm: &LocalEvm,
    ) -> ExternalDeposit {
        let underlying = seed_listing(pool, evm.vault_address).await;
        deposit_to_recipient(pool, evm, &underlying).await
    }

    /// A duplicate deposit on an already listed `underlying` whose excess
    /// shares went to a fresh recipient.
    async fn deposit_to_recipient(
        pool: &Pool<Sqlite>,
        evm: &LocalEvm,
        underlying: &UnderlyingSymbol,
    ) -> ExternalDeposit {
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(pool, &issuer_request_id, underlying).await;

        let recipient = PrivateKeySigner::random();
        let recipient_address = recipient.address();

        let issuer_signer =
            PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let issuer_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(issuer_signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let _ = issuer_provider
            .send_transaction(
                alloy::rpc::types::TransactionRequest::default()
                    .to(recipient_address)
                    .value(U256::from(10u64).pow(U256::from(18u64))),
            )
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let info = sample_receipt_info(issuer_request_id.clone());
        let encoded = info.encode().unwrap();
        let vault_issuer =
            OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider);
        let ratio = U256::from(10).pow(U256::from(18));
        let shares = excess_shares();
        let deposit_call = vault_issuer
            .deposit(shares, evm.wallet_address, ratio, encoded)
            .calldata()
            .clone();
        let transfer_call =
            vault_issuer.transfer(recipient_address, shares).calldata().clone();
        let multicall_receipt = vault_issuer
            .multicall(vec![deposit_call, transfer_call])
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let deposit_tx = multicall_receipt.transaction_hash;
        let receipt_id = multicall_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Deposit>()
                    .ok()
                    .map(|decoded| decoded.data().id)
            })
            .unwrap();

        ExternalDeposit { issuer_request_id, receipt_id, deposit_tx, recipient }
    }

    /// Sends the Path B funding Transfer (the excess shares back from the
    /// recipient to the issuer) and returns its hash and log index.
    async fn send_external_funding(
        evm: &LocalEvm,
        recipient: PrivateKeySigner,
    ) -> (B256, u64) {
        let recipient_address = recipient.address();
        let shares = excess_shares();
        let recipient_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(recipient))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let vault_recipient = OffchainAssetReceiptVault::new(
            evm.vault_address,
            recipient_provider,
        );
        let funding_receipt = vault_recipient
            .transfer(evm.wallet_address, shares)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let funding_tx = funding_receipt.transaction_hash;
        let funding_log_index = funding_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                let decoded = log
                    .log_decode::<OffchainAssetReceiptVault::Transfer>()
                    .ok()?;
                let data = decoded.data();
                if data.from == recipient_address
                    && data.to == evm.wallet_address
                    && data.value == shares
                {
                    log.log_index
                } else {
                    None
                }
            })
            .expect("funding transfer log index");

        (funding_tx, funding_log_index)
    }

    #[tokio::test]
    async fn external_dry_run_does_not_mutate() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            funding_log_index,
            ..
        } = setup_external_path(&pool, &evm).await;

        let before_share = service
            .get_share_balance(evm.vault_address, evm.wallet_address)
            .await
            .unwrap();

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                Some(funding_tx),
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let after_share = service
            .get_share_balance(evm.vault_address, evm.wallet_address)
            .await
            .unwrap();
        assert_eq!(before_share, after_share);

        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none(),
            "dry-run must not open a BurnExcess stream"
        );
        assert!(
            !is_excluded_funding_log(
                &pool,
                Network::Base,
                evm.vault_address,
                funding_tx,
                funding_log_index,
            )
            .await
            .unwrap(),
            "dry-run must not write an exclusion row"
        );
    }

    #[tokio::test]
    async fn external_execute_has_burn_excess_not_redemption() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            funding_log_index,
            ..
        } = setup_external_path(&pool, &evm).await;

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                Some(funding_tx),
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let redemption_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
            ",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            redemption_count, 0,
            "Path B must never open a Redemption for funding/deposit txs"
        );

        let store = burn_excess_store(pool.clone()).await.unwrap();
        let state =
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap();
        assert!(matches!(
            state,
            BurnExcess::Completed { path: BurnExcessPath::External, .. }
        ));
        assert!(
            is_excluded_funding_log(
                &pool,
                Network::Base,
                evm.vault_address,
                funding_tx,
                funding_log_index,
            )
            .await
            .unwrap()
        );
    }

    /// The live route runs with the service up, so the poller may scan the
    /// funding Transfer's block before the burn-excess request arrives. With
    /// the expectation recorded, that scan must open no Redemption (the
    /// recipient is a linked AP wallet, so an unprotected scan would) and
    /// must hold the vault there; the live external burn must then complete,
    /// after which the poller moves past the excluded funding log.
    #[tokio::test]
    async fn poller_scanning_the_funding_block_first_opens_no_redemption() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalDeposit {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            recipient,
        } = setup_external_deposit(&pool, &evm).await;
        link_ap_wallet(&pool, recipient.address()).await;
        let live = |mode, funding_tx_hash| BurnExcessRequest {
            poller_guard: PollerGuard::FundingExpected,
            ..request(
                mode,
                issuer_request_id.clone(),
                deposit_tx,
                receipt_id,
                funding_tx_hash,
                true,
            )
        };

        // The operator sequence: record the expectation, then broadcast.
        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            live(BurnExcessMode::ExpectFunding, None),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let (funding_tx, _) = send_external_funding(&evm, recipient).await;

        let poller = transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await;
        poller.poll_once().await.unwrap();

        assert_eq!(
            redemption_event_count(&pool).await,
            0,
            "the poller must not open a Redemption for the funding Transfer"
        );
        let head = provider.get_block_number().await.unwrap();
        let held_at =
            load_transfer_poll(&pool, Network::Base, evm.vault_address)
                .await
                .unwrap();
        assert!(
            held_at.is_none_or(|block| block < head),
            "the poller must hold the vault before the funding block: \
             checkpoint {held_at:?}, head {head}"
        );

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            live(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap(),
            BurnExcess::Completed { path: BurnExcessPath::External, .. }
        ));

        poller.poll_once().await.unwrap();
        assert_eq!(redemption_event_count(&pool).await, 0);
        let head = provider.get_block_number().await.unwrap();
        assert_eq!(
            load_transfer_poll(&pool, Network::Base, evm.vault_address)
                .await
                .unwrap(),
            Some(head),
            "once excluded, the funding log no longer holds the vault"
        );
    }

    /// An AP redemption mined on the held vault after the funding Transfer
    /// puts its shares in the issuer wallet, so `external` cannot prove the
    /// exact balance it needs until that redemption burns. Were the
    /// redemption held behind the funding log, it would never burn, the
    /// exclusion would never be recorded, and the hold would never clear. The
    /// poller must detect it while holding only the funding log; `external`
    /// then completes once the balance is exact again.
    #[tokio::test]
    async fn a_redemption_after_the_held_funding_is_detected_while_held() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalDeposit {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            recipient,
        } = setup_external_deposit(&pool, &evm).await;
        let recipient_address = recipient.address();
        link_ap_wallet(&pool, recipient_address).await;

        let redeemed = U256::from(1_000_000_000_000_000_000u64);
        let issuer_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(
                PrivateKeySigner::from_bytes(&evm.private_key).unwrap(),
            ))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let vault_issuer =
            OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider);
        let deposit_call = vault_issuer
            .deposit(
                redeemed,
                evm.wallet_address,
                U256::from(10).pow(U256::from(18)),
                sample_receipt_info(IssuerMintRequestId::random())
                    .encode()
                    .unwrap(),
            )
            .calldata()
            .clone();
        let transfer_call = vault_issuer
            .transfer(recipient_address, redeemed)
            .calldata()
            .clone();
        vault_issuer
            .multicall(vec![deposit_call, transfer_call])
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let live = |mode, funding_tx_hash| BurnExcessRequest {
            poller_guard: PollerGuard::FundingExpected,
            ..request(
                mode,
                issuer_request_id.clone(),
                deposit_tx,
                receipt_id,
                funding_tx_hash,
                true,
            )
        };
        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            live(BurnExcessMode::ExpectFunding, None),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let recipient_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(recipient.clone()))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let (funding_tx, _) = send_external_funding(&evm, recipient).await;
        let redemption_tx = OffchainAssetReceiptVault::new(
            evm.vault_address,
            &recipient_provider,
        )
        .transfer(evm.wallet_address, redeemed)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap()
        .transaction_hash;

        let poller = transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await;
        poller.poll_once().await.unwrap();

        let redemptions = redemption_aggregate_ids(&pool).await;
        assert_eq!(
            redemptions,
            vec![IssuerRedemptionRequestId::new(redemption_tx).to_string()],
            "only the later redemption opens; the funding Transfer stays held"
        );

        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            live(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error,
                BurnExcessEngineError::Proof(
                    BurnExcessProofError::IssuerShareBalanceNotExact { .. }
                )
            ),
            "the redemption's shares are still in the issuer wallet: \
             {error:?}"
        );

        // Stand-in for the detected redemption's burn, which the redemption
        // flow runs once Alpaca journals it: the issuer wallet no longer
        // holds the redeemed shares.
        vault_issuer
            .transfer(Address::random(), redeemed)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            live(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap(),
            BurnExcess::Completed { path: BurnExcessPath::External, .. }
        ));

        poller.poll_once().await.unwrap();
        assert_eq!(redemption_aggregate_ids(&pool).await, redemptions);
        let head = provider.get_block_number().await.unwrap();
        assert_eq!(
            load_transfer_poll(&pool, Network::Base, evm.vault_address)
                .await
                .unwrap(),
            Some(head),
            "once excluded, the funding log no longer holds the vault"
        );
    }

    /// The expectation matches by shape, so a genuine AP redemption of exactly
    /// the excess from the original recipient is held with the funding
    /// Transfer, and both sets of shares sit in the issuer wallet. `external`
    /// must count the held redemption's shares instead of refusing for a
    /// balance that is not exactly the excess, which left only `--close`
    /// (redeeming the funding Transfer through Alpaca). Once the burn
    /// completes, the held redemption is detected as an ordinary one and the
    /// funding Transfer never is.
    #[traced_test]
    #[tokio::test]
    async fn a_same_shape_redemption_held_with_the_funding_is_counted() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalDeposit {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            recipient,
        } = setup_external_deposit(&pool, &evm).await;
        let recipient_address = recipient.address();
        link_ap_wallet(&pool, recipient_address).await;

        // A second deposit gives the recipient another excess amount, so it
        // can redeem exactly that much on its own account.
        let shares = excess_shares();
        let issuer_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(
                PrivateKeySigner::from_bytes(&evm.private_key).unwrap(),
            ))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let vault_issuer =
            OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider);
        let deposit_call = vault_issuer
            .deposit(
                shares,
                evm.wallet_address,
                U256::from(10).pow(U256::from(18)),
                sample_receipt_info(IssuerMintRequestId::random())
                    .encode()
                    .unwrap(),
            )
            .calldata()
            .clone();
        let transfer_call =
            vault_issuer.transfer(recipient_address, shares).calldata().clone();
        vault_issuer
            .multicall(vec![deposit_call, transfer_call])
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let live = |mode, funding_tx_hash| BurnExcessRequest {
            poller_guard: PollerGuard::FundingExpected,
            ..request(
                mode,
                issuer_request_id.clone(),
                deposit_tx,
                receipt_id,
                funding_tx_hash,
                true,
            )
        };
        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            live(BurnExcessMode::ExpectFunding, None),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let recipient_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(recipient.clone()))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let (funding_tx, _) = send_external_funding(&evm, recipient).await;
        let redemption_tx = OffchainAssetReceiptVault::new(
            evm.vault_address,
            &recipient_provider,
        )
        .transfer(evm.wallet_address, shares)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap()
        .transaction_hash;

        let poller = transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await;
        poller.poll_once().await.unwrap();
        assert!(
            redemption_aggregate_ids(&pool).await.is_empty(),
            "both Transfers match the expectation, so both are held"
        );

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            live(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap(),
            BurnExcess::Completed { path: BurnExcessPath::External, .. }
        ));
        let deposit_key = deposit_tx.to_string();
        logs_assert(|lines: &[&str]| {
            lines
                .iter()
                .any(|line| {
                    line.contains(" INFO ")
                        && line.contains(
                            "Counted same-shape Transfers held with the \
                             funding Transfer",
                        )
                        && line.contains(&deposit_key)
                        && line.contains("held_transfers=1")
                })
                .then_some(())
                .ok_or_else(|| "no INFO counting the held redemption".into())
        });

        poller.poll_once().await.unwrap();
        assert_eq!(
            redemption_aggregate_ids(&pool).await,
            vec![IssuerRedemptionRequestId::new(redemption_tx).to_string()],
            "only the held redemption opens once the stream completes"
        );
        let head = provider.get_block_number().await.unwrap();
        assert_eq!(
            load_transfer_poll(&pool, Network::Base, evm.vault_address)
                .await
                .unwrap(),
            Some(head)
        );
    }

    async fn redemption_aggregate_ids(pool: &Pool<Sqlite>) -> Vec<String> {
        sqlx::query_scalar(
            "
            SELECT DISTINCT aggregate_id
            FROM events
            WHERE aggregate_type = 'Redemption'
            ORDER BY aggregate_id
            ",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// Every route request opens the store. Rebuilding the expectation index
    /// there deleted rows not backed by a latest `FundingExpected` event, so a
    /// request opening the store just after another stream committed its
    /// exclusion event, but before that stream wrote the exclusion row, left
    /// the poller with neither guard. Opening the store must leave existing
    /// expectations alone.
    #[tokio::test]
    async fn opening_the_store_leaves_funding_expectations_in_place() {
        let pool = pool().await;
        let bind = ExcessBurnBind {
            issuer_request_id: IssuerMintRequestId::random(),
            deposit_tx_hash: B256::random(),
            receipt_id: U256::from(7u64),
            shares: U256::from(750_000_000_000_000_000u64),
            original_recipient: Address::random(),
            vault: Address::random(),
            network: Network::Base,
            issuer_wallet: Address::random(),
        };
        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();

        burn_excess_store(pool.clone()).await.unwrap();

        assert!(
            is_expected_funding(
                &pool,
                bind.network,
                bind.vault,
                bind.original_recipient,
                bind.issuer_wallet,
                bind.shares,
            )
            .await
            .unwrap(),
            "opening the store must not drop a funding expectation"
        );
    }

    /// The live route cannot rely on a stopped service, so an external run on
    /// a stream that never recorded its expectation is refused before any
    /// mutation: nothing guarded the funding Transfer from the poller.
    #[tokio::test]
    async fn live_external_without_an_expectation_is_refused() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            ..
        } = setup_external_path(&pool, &evm).await;

        let err = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            BurnExcessRequest {
                poller_guard: PollerGuard::FundingExpected,
                ..request(
                    BurnExcessMode::External,
                    issuer_request_id,
                    deposit_tx,
                    receipt_id,
                    Some(funding_tx),
                    true,
                )
            },
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, BurnExcessEngineError::FundingNotExpected));
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none()
        );
    }

    /// Closing an expectation releases the poller's hold: the funding Transfer
    /// it held is then an ordinary redemption, which is what close means.
    #[tokio::test]
    async fn closing_an_expectation_releases_the_held_transfer() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            recipient,
            ..
        } = setup_external_path(&pool, &evm).await;
        link_ap_wallet(&pool, recipient).await;
        let expect = |close| BurnExcessRequest {
            poller_guard: PollerGuard::FundingExpected,
            close,
            ..request(
                BurnExcessMode::ExpectFunding,
                issuer_request_id.clone(),
                deposit_tx,
                receipt_id,
                None,
                true,
            )
        };

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            expect(false),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let poller = transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await;
        poller.poll_once().await.unwrap();
        assert_eq!(redemption_event_count(&pool).await, 0);

        let prompt = Mutex::new(String::new());
        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            expect(true),
            |text| {
                text.clone_into(&mut prompt.lock());
                Ok(true)
            },
        )
        .await
        .unwrap();
        let prompt = prompt.into_inner();
        assert!(
            prompt.contains("releases the funding-Transfer hold")
                && !prompt.contains("wallet gates only"),
            "the confirmation must say what closing an expectation does: \
             {prompt}"
        );
        poller.poll_once().await.unwrap();

        assert!(
            redemption_event_count(&pool).await > 0,
            "a closed expectation must stop holding the funding Transfer"
        );
    }

    async fn redemption_event_count(pool: &Pool<Sqlite>) -> i64 {
        sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
            ",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn funding_already_redeemed_refuses_external() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            ..
        } = setup_external_path(&pool, &evm).await;

        // Seed a Redemption aggregate for the funding tx (poller race).
        let redemption_id =
            IssuerRedemptionRequestId::new(funding_tx).to_string();
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
            VALUES (
                'Redemption',
                ?,
                1,
                'RedemptionEvent::Detected',
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .bind(&redemption_id)
        .execute(&pool)
        .await
        .unwrap();

        let err = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                Some(funding_tx),
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                BurnExcessEngineError::Proof(
                    BurnExcessProofError::FundingAlreadyRedeemedTx { .. }
                )
            ),
            "expected FundingAlreadyRedeemedTx, got {err:?}"
        );
    }

    #[tokio::test]
    async fn internal_refuses_when_original_recipient_is_not_issuer() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            recipient,
            ..
        } = setup_external_path(&pool, &evm).await;
        assert_ne!(recipient, evm.wallet_address);

        // Shares sit at issuer after funding, but original recipient is not
        // issuer — Path A must refuse and direct ops to external.
        let err = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                BurnExcessEngineError::Proof(
                    BurnExcessProofError::InternalRequiresIssuerAsRecipient { .. }
                )
            ),
            "expected InternalRequiresIssuerAsRecipient, got {err:?}"
        );
        assert!(
            err.to_string().contains("burn-excess external"),
            "error should direct ops to external mode: {err}"
        );
    }

    #[tokio::test]
    async fn path_conflict_funding_excluded_then_internal() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            ..
        } = setup_external_path(&pool, &evm).await;

        let store = burn_excess_store(pool.clone()).await.unwrap();
        let funding = FundingTransferId {
            network: Network::Base,
            vault: evm.vault_address,
            tx_hash: funding_tx,
            log_index: 0,
            from: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            to: evm.wallet_address,
            amount: excess_shares(),
        };
        let bind = ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash: deposit_tx,
            receipt_id,
            shares: excess_shares(),
            original_recipient: funding.from,
            vault: evm.vault_address,
            network: Network::Base,
            issuer_wallet: evm.wallet_address,
        };
        store
            .send(
                &BurnExcessId::new(deposit_tx),
                BurnExcessCommand::RecordFundingExclusion {
                    bind,
                    funding_log_id: funding,
                    reason: "seed".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();

        let err = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            BurnExcessEngineError::Proof(BurnExcessProofError::PathConflict {
                locked: BurnExcessPath::External,
                requested: BurnExcessMode::Internal,
            })
        ));
    }

    #[tokio::test]
    async fn path_b_resume_repairs_missing_exclusion_index_row() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            recipient,
            funding_log_index,
        } = setup_external_path(&pool, &evm).await;

        let store = burn_excess_store(pool.clone()).await.unwrap();
        let funding = FundingTransferId {
            network: Network::Base,
            vault: evm.vault_address,
            tx_hash: funding_tx,
            log_index: funding_log_index,
            from: recipient,
            to: evm.wallet_address,
            amount: excess_shares(),
        };
        let bind = ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash: deposit_tx,
            receipt_id,
            shares: excess_shares(),
            original_recipient: recipient,
            vault: evm.vault_address,
            network: Network::Base,
            issuer_wallet: evm.wallet_address,
        };
        store
            .send(
                &BurnExcessId::new(deposit_tx),
                BurnExcessCommand::RecordFundingExclusion {
                    bind: bind.clone(),
                    funding_log_id: funding.clone(),
                    reason: "seed".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();

        // Simulate dual-write gap / truncated index: event exists, row gone,
        // and the expectation the exclusion should have cleared is still
        // there.
        sqlx::query("DELETE FROM burn_excess_funding_exclusions")
            .execute(&pool)
            .await
            .unwrap();
        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();
        assert!(
            !is_excluded_funding_log(
                &pool,
                Network::Base,
                evm.vault_address,
                funding_tx,
                funding_log_index,
            )
            .await
            .unwrap()
        );

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                Some(funding_tx),
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        assert!(
            is_excluded_funding_log(
                &pool,
                Network::Base,
                evm.vault_address,
                funding_tx,
                funding_log_index,
            )
            .await
            .unwrap(),
            "resume must re-insert the exclusion index from the event"
        );
        assert!(
            !is_expected_funding(
                &pool,
                bind.network,
                bind.vault,
                bind.original_recipient,
                bind.issuer_wallet,
                bind.shares,
            )
            .await
            .unwrap(),
            "resume must clear a stale expectation once the exclusion is in"
        );
        let state =
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap();
        assert!(matches!(
            state,
            BurnExcess::Completed { path: BurnExcessPath::External, .. }
        ));
    }

    /// The confirm prompt blocks on stdin for an unbounded time, so balances
    /// proven before it can move before the burn is signed. The re-check at the
    /// sign boundary must catch that and refuse, leaving nothing intended.
    #[tokio::test]
    async fn balance_moving_at_the_confirm_prompt_refuses_before_signing() {
        let pool = pool().await;
        let (evm, _real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;

        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;

        // Proven balance is exact, so `prove_plan` and the prompt both pass.
        let mock = MockVaultService::new_success().with_share_balance(shares);

        let error = run_burn_excess(
            &pool,
            &mock,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                true,
            ),
            |_| {
                // The operator's balance moves while the prompt is open.
                mock.set_share_balance(shares + U256::from(1u64));
                Ok(true)
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                BurnExcessEngineError::Proof(
                    BurnExcessProofError::IssuerShareBalanceNotExact { .. }
                )
            ),
            "a balance that moved at the prompt must fail the re-check, got: \
             {error:?}"
        );
        assert_eq!(
            mock.burn_preparation_call_count(),
            0,
            "the re-check must refuse before anything is signed"
        );

        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none(),
            "a refused re-check must leave no BurnExcess stream behind"
        );
    }

    /// Inserts the `MintTxIntended` a mint job persists while holding the
    /// wallet, which reserves the network's signer for that mint.
    async fn seed_mint_tx_intended(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
    ) {
        let intended = MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: PreparedMintTx::valid_for_test(
                1,
                format!("mint-{issuer_request_id}"),
            ),
            intended_at: Utc::now(),
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
            VALUES ('Mint', ?, 2, ?, '1.0', ?, '{}')
            ",
        )
        .bind(issuer_request_id.to_string())
        .bind(intended.event_type())
        .bind(serde_json::to_string(&intended).unwrap())
        .execute(pool)
        .await
        .unwrap();
    }

    /// Runs an execute request on its own task so the test can observe it
    /// parked on the wallet while another signer holds it.
    fn spawn_burn(
        pool: &Pool<Sqlite>,
        mock: &Arc<MockVaultService>,
        provider: &(impl Provider + Clone + 'static),
        issuer_wallet: Address,
        request: BurnExcessRequest,
    ) -> tokio::task::JoinHandle<Result<BurnExcessOutcome, BurnExcessEngineError>>
    {
        let pool = pool.clone();
        let mock = Arc::clone(mock);
        let provider = provider.clone();
        tokio::spawn(async move {
            run_burn_excess(
                &pool,
                mock.as_ref(),
                &provider,
                issuer_wallet,
                request,
                |_| Ok(true),
            )
            .await
        })
    }

    async fn wait_for_wallet_lock_calls(
        mock: &MockVaultService,
        expected: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while mock.get_wallet_lock_call_count() < expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the burn must contend for the wallet before its intent check");
    }

    /// Two excess burns on different deposits share one wallet. The second
    /// must not run its intent check until the first has persisted its intent:
    /// otherwise both checks pass and both sign on the same nonce.
    #[tokio::test]
    async fn a_second_excess_burn_waits_for_the_first_to_persist_its_intent() {
        let pool = pool().await;
        let (evm, _real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let first_id = IssuerMintRequestId::random();
        let second_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &first_id, &underlying).await;
        seed_mint_initiated(&pool, &second_id, &underlying).await;
        let (first_receipt, shares, _, first_deposit) =
            mint_with_info(&evm, evm.wallet_address, &first_id).await;
        let (second_receipt, _, _, second_deposit) =
            mint_with_info(&evm, evm.wallet_address, &second_id).await;

        let mock = Arc::new(
            MockVaultService::new_prepare_burn_blocked()
                .with_share_balance(shares),
        );

        let first = spawn_burn(
            &pool,
            &mock,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                first_id,
                first_deposit,
                first_receipt,
                None,
                true,
            ),
        );
        // Past its intent check, inside signing, intent not yet persisted:
        // the window a second burn must not be able to check inside.
        mock.wait_for_burn_preparation().await;

        let second = spawn_burn(
            &pool,
            &mock,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                second_id,
                second_deposit,
                second_receipt,
                None,
                true,
            ),
        );
        wait_for_wallet_lock_calls(&mock, 2).await;
        assert!(
            !second.is_finished(),
            "the second burn must wait for the wallet, not check intents \
             inside the first burn's window"
        );
        assert_eq!(mock.burn_preparation_call_count(), 1);

        mock.release_burn_preparation();
        let first_error = first.await.unwrap().unwrap_err();
        assert!(
            matches!(
                first_error,
                BurnExcessEngineError::Vault(
                    VaultError::ConfirmationPending { .. }
                )
            ),
            "the first burn must submit and stay unresolved, got: \
             {first_error:?}"
        );
        let second_error = second.await.unwrap().unwrap_err();
        assert!(
            matches!(
                second_error,
                BurnExcessEngineError::UnresolvedExcessBurnIntent
            ),
            "the second burn's intent check must see the first burn's \
             persisted intent, got: {second_error:?}"
        );
        assert_eq!(
            mock.burn_preparation_call_count(),
            1,
            "only the first burn may sign while its intent is unresolved"
        );

        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store.load(&BurnExcessId::new(first_deposit)).await.unwrap(),
            Some(BurnExcess::Submitted { .. })
        ));
        assert!(
            store
                .load(&BurnExcessId::new(second_deposit))
                .await
                .unwrap()
                .is_none(),
            "a refused second burn must leave no stream behind"
        );
    }

    /// An excess burn racing normal issuance. The mint job holds the wallet
    /// from its own intent check through signing and its persisted intent, so
    /// the burn must not check for intents until that intent is on record.
    #[tokio::test]
    async fn an_excess_burn_waits_behind_a_mint_holding_the_wallet() {
        let pool = pool().await;
        let (evm, _real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;
        let concurrent_mint = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &concurrent_mint, &underlying).await;

        let mock = Arc::new(
            MockVaultService::new_success().with_share_balance(shares),
        );
        // The mint job takes the wallet before its own intent check.
        let mint_guard = mock.lock_wallet().await;

        let burn = spawn_burn(
            &pool,
            &mock,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                true,
            ),
        );
        wait_for_wallet_lock_calls(&mock, 2).await;
        assert!(
            !burn.is_finished(),
            "the burn must wait for the wallet, not check intents past a \
             mint that holds it"
        );
        assert_eq!(mock.burn_preparation_call_count(), 0);

        // The mint signs and persists its intent, still holding the wallet.
        seed_mint_tx_intended(&pool, &concurrent_mint).await;
        drop(mint_guard);

        let error = burn.await.unwrap().unwrap_err();
        assert!(
            matches!(
                error,
                BurnExcessEngineError::UnresolvedSignerIntent {
                    network: Network::Base
                }
            ),
            "the burn's intent check must see the mint's persisted intent, \
             got: {error:?}"
        );
        assert_eq!(
            mock.burn_preparation_call_count(),
            0,
            "the burn must not sign behind a mint's live nonce"
        );

        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none(),
            "a refused burn must leave no stream behind"
        );
    }

    #[tokio::test]
    async fn resume_intended_mined_completes_without_second_prepare() {
        let pool = pool().await;
        let (evm, _real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;

        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;

        let sendable = SendableTxWithHash {
            tx: vec![0xde, 0xad],
            hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            nonce: 7,
            signed_at: Utc::now(),
            dust_shares: U256::ZERO,
        };
        let bind = ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash: deposit_tx,
            receipt_id,
            shares,
            original_recipient: evm.wallet_address,
            vault: evm.vault_address,
            network: Network::Base,
            issuer_wallet: evm.wallet_address,
        };
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &BurnExcessId::new(deposit_tx),
                BurnExcessCommand::IntendExcessBurn {
                    bind: bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "resume".into(),
                    incident_id: None,
                    sendable_tx: sendable.clone(),
                },
            )
            .await
            .unwrap();

        let mock = MockVaultService::new_success()
            .with_share_balance(shares)
            .with_burn_tx_status(BurnTxStatus::Mined)
            .with_pending_burn_result(MultiBurnResult {
                tx_hash: sendable.hash,
                burns: vec![MultiBurnResultEntry {
                    receipt_id,
                    shares_burned: shares,
                }],
                dust_returned: U256::ZERO,
                gas_used: 50_000,
                block_number: 99,
            });

        run_burn_excess(
            &pool,
            &mock,
            &provider,
            evm.wallet_address,
            request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                receipt_id,
                None,
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        assert_eq!(
            mock.burn_preparation_call_count(),
            0,
            "Intended+Mined resume must not re-prepare"
        );
        assert_eq!(mock.burn_classification_call_count(), 1);

        let state =
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap();
        assert!(matches!(state, BurnExcess::Completed { .. }));
    }

    /// `--close` never proves a plan, so it reaches no chain and no signer.
    /// Constructing the provider without a live node keeps these tests off
    /// Anvil and makes the "no I/O on the close path" property structural.
    fn offline_provider() -> impl Provider {
        ProviderBuilder::new()
            .connect_http("http://127.0.0.1:1".parse().unwrap())
    }

    fn close_request(
        mode: BurnExcessMode,
        issuer_request_id: IssuerMintRequestId,
        deposit_tx_hash: B256,
        receipt_id: U256,
        funding_tx_hash: Option<B256>,
        execute: bool,
    ) -> BurnExcessRequest {
        BurnExcessRequest {
            close: true,
            ..request(
                mode,
                issuer_request_id,
                deposit_tx_hash,
                receipt_id,
                funding_tx_hash,
                execute,
            )
        }
    }

    fn test_bind(
        issuer_request_id: &IssuerMintRequestId,
        deposit_tx_hash: B256,
    ) -> ExcessBurnBind {
        ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash,
            receipt_id: U256::from(7u64),
            shares: excess_shares(),
            original_recipient: address!(
                "0xA9C16673F65AE808688cB18952AFE3d9658C808f"
            ),
            vault: address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            network: Network::Base,
            issuer_wallet: address!(
                "0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"
            ),
        }
    }

    fn test_funding_log(bind: &ExcessBurnBind) -> FundingTransferId {
        FundingTransferId {
            network: bind.network,
            vault: bind.vault,
            tx_hash: b256!(
                "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1"
            ),
            log_index: 2,
            from: bind.original_recipient,
            to: bind.issuer_wallet,
            amount: bind.shares,
        }
    }

    /// Seeds an abandoned Path B recovery: the exclusion is permanent, but no
    /// transaction is signed. This is the state `--close` exists to release.
    async fn seed_funding_excluded(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        deposit_tx: B256,
    ) -> Arc<Store<BurnExcess>> {
        let bind = test_bind(issuer_request_id, deposit_tx);
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &BurnExcessId::new(deposit_tx),
                BurnExcessCommand::RecordFundingExclusion {
                    funding_log_id: test_funding_log(&bind),
                    bind,
                    reason: "abandoned path b".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        store
    }

    /// Another stream's unresolved recovery refuses every `external` run on
    /// this one, so recording this stream's expectation then would hold its
    /// funding Transfer, and the vault's checkpoint, until that other stream
    /// finishes. `expect-funding` must refuse first, before the operator is
    /// asked to broadcast.
    #[tokio::test]
    async fn expect_funding_refuses_while_another_recovery_is_unresolved() {
        let pool = pool().await;
        seed_funding_excluded(
            &pool,
            &IssuerMintRequestId::random(),
            B256::random(),
        )
        .await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let bind = test_bind(&issuer_request_id, deposit_tx);

        let error = run_burn_excess(
            &pool,
            &MockVaultService::new_success(),
            &offline_provider(),
            bind.issuer_wallet,
            BurnExcessRequest {
                poller_guard: PollerGuard::FundingExpected,
                ..request(
                    BurnExcessMode::ExpectFunding,
                    issuer_request_id,
                    deposit_tx,
                    bind.receipt_id,
                    None,
                    true,
                )
            },
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, BurnExcessEngineError::UnresolvedExcessBurnIntent),
            "got: {error:?}"
        );
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none()
        );
        assert!(
            !is_expected_funding(
                &pool,
                bind.network,
                bind.vault,
                bind.original_recipient,
                bind.issuer_wallet,
                bind.shares,
            )
            .await
            .unwrap()
        );
    }

    /// A Path B stream keeps its expectation until it completes or closes, so
    /// other Transfers of the same shape stay held while its burn is pending.
    /// A completed stream releases it only once its exclusion row is in, so
    /// the funding log always has one guard.
    #[tokio::test]
    async fn only_a_terminal_stream_releases_its_expectation() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let store =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;
        let bind = test_bind(&issuer_request_id, deposit_tx);
        let funding = test_funding_log(&bind);
        let expected = || {
            is_expected_funding(
                &pool,
                bind.network,
                bind.vault,
                bind.original_recipient,
                bind.issuer_wallet,
                bind.shares,
            )
        };
        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();

        let excluded =
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap();
        release_terminal_expectation(&pool, excluded.as_ref()).await.unwrap();
        assert!(
            expected().await.unwrap(),
            "a stream whose burn is pending must keep holding same-shape \
             Transfers"
        );

        let completed = BurnExcess::Completed {
            bind: bind.clone(),
            path: BurnExcessPath::External,
            funding_log_id: Some(funding.clone()),
            burn_tx_hash: B256::random(),
            block_number: 1,
            completed_at: Utc::now(),
        };
        sqlx::query("DELETE FROM burn_excess_funding_exclusions")
            .execute(&pool)
            .await
            .unwrap();
        release_terminal_expectation(&pool, Some(&completed)).await.unwrap();
        assert!(
            expected().await.unwrap(),
            "without its exclusion row the expectation is the only guard"
        );

        record_funding_exclusion(&pool, &funding, deposit_tx, Utc::now())
            .await
            .unwrap();
        release_terminal_expectation(&pool, Some(&completed)).await.unwrap();
        assert!(!expected().await.unwrap());
    }

    /// Two open expectations on one vault leave the issuer wallet holding
    /// both fundings, so neither `external` run can ever see an exact share
    /// balance and both holds stall the vault. A second `expect-funding` on a
    /// vault that already has one open is refused before anything is written.
    #[tokio::test]
    async fn a_second_expectation_on_one_vault_is_refused() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let ExternalDeposit {
            issuer_request_id, receipt_id, deposit_tx, ..
        } = setup_external_deposit(&pool, &evm).await;
        let open = ExcessBurnBind {
            vault: evm.vault_address,
            ..test_bind(&IssuerMintRequestId::random(), B256::random())
        };
        record_funding_expectation(&pool, &open, Utc::now()).await.unwrap();

        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            BurnExcessRequest {
                poller_guard: PollerGuard::FundingExpected,
                ..request(
                    BurnExcessMode::ExpectFunding,
                    issuer_request_id,
                    deposit_tx,
                    receipt_id,
                    None,
                    true,
                )
            },
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                BurnExcessEngineError::AnotherFundingExpected { vault }
                    if vault == evm.vault_address
            ),
            "got: {error:?}"
        );
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none()
        );
        assert!(
            !clear_funding_expectation(&pool, deposit_tx).await.unwrap(),
            "a refused expectation must write no index row"
        );
    }

    /// A deposit minted to the issuer wallet already left the excess there:
    /// it is `internal`. Expecting a funding Transfer for it would lock the
    /// stream to a path whose funding cannot exist, so `expect-funding`
    /// refuses before anything is written, dry run included.
    #[tokio::test]
    async fn expect_funding_refuses_a_deposit_minted_to_the_issuer() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (receipt_id, _, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;

        for execute in [false, true] {
            let error = run_burn_excess(
                &pool,
                &service,
                &provider,
                evm.wallet_address,
                BurnExcessRequest {
                    poller_guard: PollerGuard::FundingExpected,
                    ..request(
                        BurnExcessMode::ExpectFunding,
                        issuer_request_id.clone(),
                        deposit_tx,
                        receipt_id,
                        None,
                        execute,
                    )
                },
                |_| Ok(true),
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error,
                    BurnExcessEngineError::Proof(
                        BurnExcessProofError::FundingFromIssuer { .. }
                    )
                ),
                "execute={execute}: {error:?}"
            );
        }
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none()
        );
        assert!(!clear_funding_expectation(&pool, deposit_tx).await.unwrap());
    }

    /// Two `external` runs on different streams that both passed their
    /// planning gates must not both record their exclusion: each would then
    /// see the other's unresolved stream at the sign boundary and refuse on
    /// every retry. Whichever takes the wallet first records and signs; the
    /// other waits for the wallet before its exclusion and is refused there,
    /// writing nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_external_runs_record_only_one_exclusion() {
        let pool = pool().await;
        let (evm, _real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let mock = Arc::new(
            MockVaultService::new_prepare_burn_blocked()
                .with_share_balance(excess_shares()),
        );
        // Both runs reach the confirm prompt, i.e. both passed `prove_plan`,
        // before either may go on to its exclusion.
        let planned = Arc::new(std::sync::Barrier::new(2));

        let mut deposits = Vec::new();
        let mut runs = Vec::new();
        for _ in 0..2 {
            let deposit = deposit_to_recipient(&pool, &evm, &underlying).await;
            let (funding_tx, _) =
                send_external_funding(&evm, deposit.recipient.clone()).await;
            deposits.push(deposit.deposit_tx);
            let external = request(
                BurnExcessMode::External,
                deposit.issuer_request_id,
                deposit.deposit_tx,
                deposit.receipt_id,
                Some(funding_tx),
                true,
            );
            let (pool, mock, provider, planned) = (
                pool.clone(),
                Arc::clone(&mock),
                provider.clone(),
                Arc::clone(&planned),
            );
            let issuer_wallet = evm.wallet_address;
            runs.push(tokio::spawn(async move {
                run_burn_excess(
                    &pool,
                    mock.as_ref(),
                    &provider,
                    issuer_wallet,
                    external,
                    |_| {
                        planned.wait();
                        Ok(true)
                    },
                )
                .await
            }));
        }

        // One run signs (and holds the wallet); the other contends for it.
        // Bounded: when both record their exclusion, each refuses the other
        // at the sign boundary and neither ever signs.
        tokio::time::timeout(
            Duration::from_secs(10),
            mock.wait_for_burn_preparation(),
        )
        .await
        .expect("one of the two runs must reach signing");
        wait_for_wallet_lock_calls(&mock, 2).await;
        let store = burn_excess_store(pool.clone()).await.unwrap();
        let mut streams = Vec::new();
        for deposit in &deposits {
            streams
                .push(store.load(&BurnExcessId::new(*deposit)).await.unwrap());
        }
        assert_eq!(
            streams.iter().filter(|stream| stream.is_some()).count(),
            1,
            "only the run holding the wallet may have recorded its \
             exclusion: {streams:?}"
        );

        mock.release_burn_preparation();
        let mut refusals = 0;
        for run in runs {
            if matches!(
                run.await.unwrap(),
                Err(BurnExcessEngineError::UnresolvedExcessBurnIntent)
            ) {
                refusals += 1;
            }
        }
        assert_eq!(refusals, 1, "the waiting run must be refused");
        let mut submitted = 0;
        let mut absent = 0;
        for deposit in &deposits {
            match store.load(&BurnExcessId::new(*deposit)).await.unwrap() {
                Some(BurnExcess::Submitted { .. }) => submitted += 1,
                None => absent += 1,
                other => panic!("unexpected stream: {other:?}"),
            }
        }
        assert_eq!((submitted, absent), (1, 1));
    }

    #[tokio::test]
    async fn close_dry_run_records_no_event_and_keeps_the_gate_closed() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let store =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;

        let dry_run_outcome = run_burn_excess(
            &pool,
            &MockVaultService::new_success(),
            &offline_provider(),
            test_bind(&issuer_request_id, deposit_tx).issuer_wallet,
            close_request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                U256::from(7u64),
                Some(B256::random()),
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let dry_run_close = match dry_run_outcome {
            BurnExcessOutcome::Close(close) => close,
            other => panic!("expected a close outcome, got: {other:?}"),
        };
        assert!(dry_run_close.dry_run);
        assert_eq!(dry_run_close.state, "FundingExcluded");

        assert!(
            matches!(
                store.load(&BurnExcessId::new(deposit_tx)).await.unwrap(),
                Some(BurnExcess::FundingExcluded { .. })
            ),
            "a dry-run close must not advance the stream"
        );
        assert!(
            has_unresolved_excess_burn_intent(&pool, None).await.unwrap(),
            "a dry-run close must leave the wallet gate held"
        );
    }

    #[tokio::test]
    async fn close_execute_releases_the_wallet_gate() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let store =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;
        assert!(
            has_unresolved_excess_burn_intent(&pool, None).await.unwrap(),
            "an abandoned Path B recovery must hold the gate before close"
        );

        let execute_outcome = run_burn_excess(
            &pool,
            &MockVaultService::new_success(),
            &offline_provider(),
            test_bind(&issuer_request_id, deposit_tx).issuer_wallet,
            close_request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                U256::from(7u64),
                Some(B256::random()),
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let execute_close = match execute_outcome {
            BurnExcessOutcome::Close(close) => close,
            other => panic!("expected a close outcome, got: {other:?}"),
        };
        assert!(!execute_close.dry_run);
        assert_eq!(execute_close.state, "Closed");

        assert!(matches!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap(),
            Some(BurnExcess::Closed { .. })
        ));
        assert!(
            !has_unresolved_excess_burn_intent(&pool, None).await.unwrap(),
            "close is the only escape from a stuck stream: it must release \
             the gate that blocks every mint and redemption burn"
        );
    }

    #[tokio::test]
    async fn close_aborts_when_the_operator_declines() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let store =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;

        let error = run_burn_excess(
            &pool,
            &MockVaultService::new_success(),
            &offline_provider(),
            test_bind(&issuer_request_id, deposit_tx).issuer_wallet,
            close_request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                U256::from(7u64),
                Some(B256::random()),
                true,
            ),
            |_| Ok(false),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, BurnExcessEngineError::Aborted),
            "a declined confirmation must abort, got: {error:?}"
        );
        assert!(matches!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap(),
            Some(BurnExcess::FundingExcluded { .. })
        ));
    }

    #[tokio::test]
    async fn close_refuses_a_stream_that_was_never_started() {
        let pool = pool().await;
        let deposit_tx = B256::random();

        let error = run_burn_excess(
            &pool,
            &MockVaultService::new_success(),
            &offline_provider(),
            address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"),
            close_request(
                BurnExcessMode::Internal,
                IssuerMintRequestId::random(),
                deposit_tx,
                U256::from(7u64),
                None,
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                &error,
                BurnExcessEngineError::Aggregate(inner)
                    if matches!(
                        **inner,
                        super::super::BurnExcessError::InvalidState { .. }
                    )
            ),
            "closing an uninitialized stream must refuse, got: {error:?}"
        );
    }

    /// `ReportOnly` is matched before `close`, so `--close` on a terminal
    /// stream reports instead of erroring — re-running an ops command must be
    /// safe — and an executed re-run releases an expectation whose clear
    /// failed after the close committed.
    #[traced_test]
    #[tokio::test]
    async fn close_on_a_closed_stream_is_report_only() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let store =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;
        store
            .send(
                &BurnExcessId::new(deposit_tx),
                BurnExcessCommand::CloseExcessBurn {
                    reason: "already closed".into(),
                },
            )
            .await
            .unwrap();
        let bind = test_bind(&issuer_request_id, deposit_tx);
        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();

        run_burn_excess(
            &pool,
            &MockVaultService::new_success(),
            &offline_provider(),
            test_bind(&issuer_request_id, deposit_tx).issuer_wallet,
            close_request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                U256::from(7u64),
                Some(B256::random()),
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        assert!(matches!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap(),
            Some(BurnExcess::Closed { .. })
        ));
        assert!(
            !is_expected_funding(
                &pool,
                bind.network,
                bind.vault,
                bind.original_recipient,
                bind.issuer_wallet,
                bind.shares,
            )
            .await
            .unwrap(),
            "a closed stream's re-run must release its stale expectation"
        );
        let deposit_key = deposit_tx.to_string();
        logs_assert(|lines: &[&str]| {
            lines
                .iter()
                .any(|line| {
                    line.contains(" INFO ")
                        && line.contains(
                            "Released the funding expectation of a terminal \
                             stream",
                        )
                        && line.contains(&deposit_key)
                        && line.contains("Closed")
                })
                .then_some(())
                .ok_or_else(|| "no release INFO for the closed stream".into())
        });
    }
}
