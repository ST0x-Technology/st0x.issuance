//! Orchestrates dual-path burn-excess recovery (D0.5).
//!
//! Dry-run proves and prints; `--execute` persists exclusion (Path B), then
//! signs/intends/submits/confirms the vault redeem and updates inventory. The
//! live route's `expect-funding` step records the funding Transfer a Path B
//! stream expects before it is broadcast.

use alloy::consensus::Transaction;
use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log, TransactionReceipt};
use alloy::sol_types::SolEvent;
use chrono::{DateTime, Utc};
use event_sorcery::{Store, StoreBuilder};
use futures::{StreamExt, TryStreamExt, stream};
use serde::Serialize;
use sqlx::{Pool, Sqlite};
use st0x_issuance_dto::AcknowledgedInboundTransfer;
use std::io;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::exclusion::{
    FundingExclusionReactor, address_key, hash_key, is_excluded_funding_log,
    is_funding_log_excluded_for_another_deposit, log_index_key,
    rebuild_funding_exclusion_index, record_funding_exclusion,
};
use super::expectation::{
    FundingExpectationReactor, FundingTransferIndexError,
    competes_for_redemption_key, ensure_held_redemptions_compatible,
    has_other_funding_expectation, is_expected_funding, log_release,
    record_funding_expectation, record_held_redemptions,
    release_funding_expectation,
};
use super::proof::{
    BurnExcessMode, BurnExcessProofError, DepositProof,
    FundingTransferCandidate, FundingTransferExpectation, PathResolution,
    bind_deposit_proof, decode_receipt_information_strict,
    require_exact_issuer_share_balance, require_funding_hash_match,
    require_issuer_receipt_balance, resolve_path, select_funding_transfer,
};
use super::{
    BurnExcess, BurnExcessCloseProof, BurnExcessCommand, BurnExcessId,
    BurnExcessPath, ExcessBurnBind, FundingTransferId, HeldTransferRedemption,
};
use crate::account::AccountView;
use crate::account::view::{AccountViewError, find_by_wallet};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::mint::{IssuerMintRequestId, Mint};
use crate::poll_checkpoint::CheckpointError;
use crate::receipt_inventory::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand, Shares,
    load_inventory, send_receipt_inventory_command,
};
use crate::redemption::{IssuerRedemptionRequestId, RedemptionEvent};
use crate::tokenized_asset::view::find_vault;
use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
use crate::vault::{
    BurnRequestOrigin, BurnTxStatus, MultiBurnEntry, MultiBurnParams,
    VaultService, WalletNonceGuard,
};

/// `eth_getLogs` chunks fetched at once while counting held same-shape
/// Transfers, so a hold that has lasted days stays inside the route's budget.
const HELD_SCAN_BLOCK_CHUNK_SIZE: u64 = 2_000;
const HELD_SCAN_BLOCK_CHUNK_STEP: usize = 2_000;
const HELD_SCAN_CONCURRENCY: usize = 8;

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
    pub(crate) acknowledged_inflows: Vec<AcknowledgedInboundTransfer>,
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

    #[error(transparent)]
    FundingTransferIndex(#[from] FundingTransferIndexError),

    #[error("failed to reconcile event schema: {0}")]
    Reconcile(#[from] event_sorcery::ReconcileError),

    #[error("failed to build the {aggregate} store: {message}")]
    StoreBuild { aggregate: &'static str, message: String },

    #[error(transparent)]
    TokenizedAsset(
        #[from] crate::tokenized_asset::view::TokenizedAssetViewError,
    ),

    #[error(transparent)]
    AccountView(#[from] AccountViewError),

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
        "the RPC returned held Transfer log in {tx_hash:?} but no receipt for \
         its transaction; retry once the node is consistent"
    )]
    HeldTransferReceiptMissing { tx_hash: B256 },

    #[error(
        "issuer share Transfer log is missing identity: tx_hash={tx_hash:?}, \
         log_index={log_index:?}"
    )]
    IssuerShareTransferMissingIdentity {
        tx_hash: Option<B256>,
        log_index: Option<u64>,
    },

    #[error(
        "issuer share Transfer {tx_hash:?}:{log_index} is not a completed \
         redemption or completed burn-excess funding log"
    )]
    UnresolvedIssuerShareTransfer { tx_hash: B256, log_index: u64 },

    #[error(
        "issuer share Transfer log could not be decoded: \
         tx_hash={tx_hash:?}, log_index={log_index:?}"
    )]
    IssuerShareTransferMalformed {
        tx_hash: Option<B256>,
        log_index: Option<u64>,
    },

    #[error(
        "the RPC head {block} is behind block {proven_block} the plan's share \
         balance was proven at; retry once the node catches up"
    )]
    ChainBehindProvenPlan { proven_block: u64, block: u64 },
    #[error(
        "the RPC head {block} is behind duplicate deposit block \
         {deposit_block}; retry once the node catches up"
    )]
    ChainBehindDeposit { deposit_block: u64, block: u64 },

    #[error("RPC did not return block {block} while proving issuer balances")]
    ChainSnapshotBlockMissing { block: u64 },

    #[error(
        "RPC returned a disconnected chain snapshot at block {block}: \
         expected parent {expected_parent:?}, got {actual_parent:?}"
    )]
    ChainSnapshotDisconnected {
        block: u64,
        expected_parent: B256,
        actual_parent: B256,
    },

    #[error(
        "transaction receipt {tx_hash:?} does not match its hash-pinned \
         Transfer log block"
    )]
    HeldTransferReceiptInconsistent { tx_hash: B256 },
    #[error(
        "acknowledged issuer-wallet inbound Transfer {tx_hash:?} \
         log_index={log_index} was not found as an unresolved inflow"
    )]
    AcknowledgedInboundNotFound { tx_hash: B256, log_index: u64 },

    #[error(
        "held redemption set changed after plan proof; \
         proven={proven:?}, current={current:?}"
    )]
    HeldRedemptionsChanged {
        proven: Vec<HeldTransferRedemption>,
        current: Vec<HeldTransferRedemption>,
    },

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Quantity(#[from] crate::QuantityConversionError),

    #[error(
        "an unresolved mint or redemption burn intent holds a signed wallet \
         nonce on {network}; clear it before burn-excess"
    )]
    UnresolvedSignerIntent { network: Network },
    #[error(
        "persisted burn intent is {status:?}; --close requires a finalized \
         reverted transaction or a provably dead nonce"
    )]
    CloseRequiresDeadBurnIntent { status: BurnTxStatus },
    #[error(
        "cannot close unsigned excess-burn expectation for vault \
         {bound_vault:?} after {underlying} on {network} was repointed to \
         {listed_vault:?}; resume the external burn against the persisted \
         vault or restore its listing before closing"
    )]
    UnsignedCloseVaultRepointed {
        underlying: UnderlyingSymbol,
        network: Network,
        bound_vault: Address,
        listed_vault: Option<Address>,
    },
    #[error(
        "cannot record or release a burn-excess expectation because original \
         recipient {wallet:?} is not uniquely linked to an Alpaca account"
    )]
    ExpectationSenderNotLinked { wallet: Address },

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
         vault {vault}; finish or --close it first"
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
        "persisted burn intent is {status:?}; an unfinalized revert must be \
         retried after finality. Once exact-hash classification returns \
         FinalizedReverted or ProvablyDead, use `burn-excess … --close \
         --execute` to clear the wallet nonce gate. Closed is report-only \
         terminal for this stream and does not unlock a replacement intend on \
         the same deposit_tx_hash"
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
/// run. The expectation index is not rebuilt here: its rebuild deletes rows
/// before re-inserting them, so run beside a request it could drop an
/// expectation that request had just written, leaving a held Transfer
/// unguarded. Service startup rebuilds it before the pollers spawn, and the
/// offline CLI runs with the service stopped.
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
    if let Some(state) = state.as_ref() {
        require_request_matches_state(state, issuer_wallet, &request)?;
    }
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
                    CloseCtx {
                        pool,
                        vault_service,
                        provider,
                        store: &store,
                        aggregate_id: &aggregate_id,
                    },
                    state.as_ref(),
                    path,
                    &request.reason,
                    request.execute,
                    &confirm,
                )
                .await?;
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

/// Releases a terminal stream's funding expectation into a guarded catch-up
/// handoff. The expectation stays configuration-locked until the poller scans
/// through the terminal event's persisted chain boundary.
async fn release_terminal_expectation(
    pool: &Pool<Sqlite>,
    state: Option<&BurnExcess>,
) -> Result<(), BurnExcessEngineError> {
    let (terminal, deposit_tx_hash, release_through_block) = match state {
        Some(
            closed @ BurnExcess::Closed { bind, release_through_block, .. },
        ) => (closed, bind.deposit_tx_hash, *release_through_block),
        Some(completed @ BurnExcess::Completed { bind, block_number, .. }) => {
            (completed, bind.deposit_tx_hash, Some(*block_number))
        }
        _ => return Ok(()),
    };
    if let Some(funding) = terminal.funding_log_id() {
        require_funding_log_unclaimed(pool, funding, deposit_tx_hash).await?;
        record_funding_exclusion(pool, funding, deposit_tx_hash, Utc::now())
            .await?;
        if !is_excluded_funding_log(
            pool,
            funding.network,
            funding.vault,
            funding.tx_hash,
            funding.log_index,
        )
        .await?
        {
            return Err(BurnExcessEngineError::FundingExclusionIndexMissing {
                tx_hash: funding.tx_hash,
                log_index: funding.log_index,
            });
        }
    }
    let released = release_funding_expectation(
        pool,
        deposit_tx_hash,
        release_through_block,
    )
    .await?;
    log_release(deposit_tx_hash, terminal.state_name(), released);
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
    /// Same-shape Transfers this stream's expectation holds, counted toward
    /// the issuer share balance.
    held_transfers: Vec<FundingTransferId>,
    /// Durable account attribution for `held_transfers`, persisted before the
    /// signed excess burn can be broadcast.
    held_redemptions: Vec<HeldTransferRedemption>,
    acknowledged_inflows: Vec<AcknowledgedInboundTransfer>,
    /// Hash-pinned chain snapshot used by an unsigned Path B proof.
    share_balance_snapshot: Option<ChainSnapshot>,
    token: TokenSymbol,
    burn_mode: crate::config::VaultMode,
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
    /// A close of an unsigned AwaitingFunding/FundingExcluded stream.
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
    /// Path B, live route: other Transfers of exactly the funding shape the
    /// expectation holds, counted toward the issuer share balance and redeemed
    /// as ordinary redemptions once the burn completes.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) held_transfers: Vec<FundingTransferId>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) acknowledged_inflows: Vec<AcknowledgedInboundTransfer>,
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

/// Serializable view of a close plan for an unsigned recovery stream.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BurnExcessCloseView {
    pub(crate) path: BurnExcessPath,
    pub(crate) state: String,
    pub(crate) reason: String,
    pub(crate) dry_run: bool,
}

async fn prove_plan<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    issuer_wallet: Address,
    request: &BurnExcessRequest,
    state: Option<&BurnExcess>,
    path: BurnExcessPath,
) -> Result<ProvenPlan, BurnExcessEngineError> {
    require_wallet_intent_gates(pool, request.network, request.deposit_tx_hash)
        .await?;

    let ProvenBind { bind, deposit_proof, underlying, token, burn_mode } =
        prove_bind(pool, provider, issuer_wallet, request, state).await?;
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
                    request.deposit_tx_hash,
                )
                .await?;
                (Some(funding_log_id), None, None)
            }
        };

    // Once a burn has durable signed bytes, resume from its durable held set:
    // the transaction may have landed before the next event, consuming the
    // wallet's receipt and shares. Released pre-anchor streams used an exact
    // balance proof and could not count held redemptions, so both legacy
    // Intended and Submitted states resume with the empty compatibility set.
    let acknowledged_inflows = match state {
        Some(
            BurnExcess::Intended { acknowledged_inflows, .. }
            | BurnExcess::Submitted { acknowledged_inflows, .. },
        ) => acknowledged_inflows.clone(),
        _ => request.acknowledged_inflows.clone(),
    };

    let anchored = match state {
        Some(
            BurnExcess::Intended {
                held_redemptions,
                held_redemptions_anchored: true,
                ..
            }
            | BurnExcess::Submitted {
                held_redemptions,
                held_redemptions_anchored: true,
                ..
            },
        ) => Some(held_redemptions.clone()),
        Some(
            BurnExcess::Intended { held_redemptions_anchored: false, .. }
            | BurnExcess::Submitted { held_redemptions_anchored: false, .. },
        ) => Some(Vec::new()),
        _ => None,
    };
    let (held_redemptions, share_balance_snapshot) =
        if let Some(held_redemptions) = anchored {
            (held_redemptions, None)
        } else {
            require_issuer_receipt_balance_for(provider, &bind).await?;
            let scan = HeldRedemptionScan {
                funding: funding_log_id.as_ref(),
                asset: HeldRedemptionAsset {
                    underlying: &underlying,
                    token: &token,
                    burn_mode,
                },
                scan_after: None,
                acknowledged_inflows: &acknowledged_inflows,
            };
            let proof =
                require_issuer_share_balance(pool, provider, &bind, scan)
                    .await?;
            (proof.held_redemptions, Some(proof.snapshot))
        };
    let held_transfers =
        held_redemptions.iter().map(|held| held.transfer.clone()).collect();

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
        held_transfers,
        held_redemptions,
        acknowledged_inflows,
        share_balance_snapshot,
        token,
        burn_mode,
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
    token: TokenSymbol,
    burn_mode: crate::config::VaultMode,
}

/// Proves the duplicate deposit and binds the stream to it. Shared by every
/// path and by `expect-funding`, which needs the bind (who funds, how much,
/// into which vault) before any funding Transfer exists. Mined history only:
/// live balances are gated separately, because a resumed signed burn that
/// already landed no longer holds them.
async fn prove_bind<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    issuer_wallet: Address,
    request: &BurnExcessRequest,
    state: Option<&BurnExcess>,
) -> Result<ProvenBind, BurnExcessEngineError> {
    let (underlying, token, mint_network, burn_mode) =
        load_mint_asset(pool, &request.issuer_request_id).await?;
    if mint_network != request.network {
        return Err(BurnExcessEngineError::MintNetworkMismatch {
            issuer_request_id: request.issuer_request_id.clone(),
            mint_network,
            requested: request.network,
        });
    }

    let persisted_bind = state.map(BurnExcess::bind);
    if let Some(bind) = persisted_bind {
        require_request_matches_bind(bind, issuer_wallet, request)?;
    }
    let vault = match persisted_bind {
        Some(bind) => bind.vault,
        None => find_vault(pool, &underlying, &request.network)
            .await?
            .ok_or_else(|| BurnExcessEngineError::VaultNotListed {
                underlying: underlying.clone(),
                network: request.network,
            })?,
    };

    let deposit_proof =
        fetch_deposit_proof(provider, request.deposit_tx_hash, vault).await?;
    bind_deposit_proof(
        &request.issuer_request_id,
        request.receipt_id,
        request.shares,
        &deposit_proof,
    )?;

    let bind = ExcessBurnBind {
        issuer_request_id: request.issuer_request_id.clone(),
        deposit_tx_hash: request.deposit_tx_hash,
        receipt_id: request.receipt_id,
        shares: request.shares,
        original_recipient: deposit_proof.original_recipient,
        vault,
        network: request.network,
        chain_id: persisted_bind.map_or(request.chain_id, |bind| bind.chain_id),
        issuer_wallet,
    };
    if persisted_bind.is_some_and(|persisted| persisted != &bind) {
        return Err(super::BurnExcessError::BindMismatch.into());
    }

    Ok(ProvenBind { bind, deposit_proof, underlying, token, burn_mode })
}
fn require_request_matches_state(
    state: &BurnExcess,
    issuer_wallet: Address,
    request: &BurnExcessRequest,
) -> Result<(), BurnExcessEngineError> {
    let bind = state.bind();
    require_request_matches_bind(bind, issuer_wallet, request)?;
    if bind.chain_id == 0 {
        let sendable_tx = match state {
            BurnExcess::Intended { sendable_tx, .. }
            | BurnExcess::Submitted { sendable_tx, .. } => Some(sendable_tx),
            _ => None,
        };
        if let Some(sendable_tx) = sendable_tx {
            let envelope =
                sendable_tx.validate_for_owner(bind.issuer_wallet)?;
            if envelope.chain_id() != Some(request.chain_id) {
                return Err(super::BurnExcessError::BindMismatch.into());
            }
        }
    }
    Ok(())
}

fn require_request_matches_bind(
    bind: &ExcessBurnBind,
    issuer_wallet: Address,
    request: &BurnExcessRequest,
) -> Result<(), BurnExcessEngineError> {
    if bind.issuer_request_id != request.issuer_request_id
        || bind.deposit_tx_hash != request.deposit_tx_hash
        || bind.receipt_id != request.receipt_id
        || bind.shares != request.shares
        || bind.network != request.network
        || (bind.chain_id != 0 && bind.chain_id != request.chain_id)
        || bind.issuer_wallet != issuer_wallet
    {
        return Err(super::BurnExcessError::BindMismatch.into());
    }
    Ok(())
}

const fn effective_chain_id(
    bind: &ExcessBurnBind,
    request: &BurnExcessRequest,
) -> u64 {
    if bind.chain_id == 0 { request.chain_id } else { bind.chain_id }
}

fn plan_view(
    plan: &ProvenPlan,
    execute: bool,
    poller_guard: PollerGuard,
) -> BurnExcessPlanView {
    let guard_precondition = match (plan.path, poller_guard) {
        (BurnExcessPath::External, PollerGuard::ServiceStopped) => Some(
            "the issuer service must be STOPPED; a running transfer poller can \
             open a Redemption for the funding Transfer first and steer this \
             recovery onto the Alpaca path",
        ),
        (BurnExcessPath::External, PollerGuard::FundingExpected) => Some(
            "the funding expectation must have been recorded before the \
             funding Transfer was broadcast; the poller holds the Transfer \
             only while it is expected, and one it redeemed before refuses \
             this run",
        ),
        (BurnExcessPath::Internal, _) => None,
    };
    // Either route counts held Transfers while an expectation of the funding
    // shape is open (for instance one recorded live before finishing offline).
    let precondition = guard_precondition.map(|guard| {
        if plan.held_transfers.is_empty() {
            guard.to_string()
        } else {
            format!(
                "{guard}; {} other Transfer(s) of exactly this shape are held \
                 and counted toward the issuer balance (see held_transfers): \
                 confirm funding_log is the Transfer that was broadcast as \
                 funding; this stream releases the held ones to the \
                 redemption poller once its burn completes",
                plan.held_transfers.len()
            )
        }
    });

    BurnExcessPlanView {
        path: plan.path,
        underlying: plan.underlying.clone(),
        bind: plan.bind.clone(),
        funding_log: plan.funding_log_id.clone(),
        resume_note: plan.resume_note.map(str::to_string),
        freeze_advisory: plan.freeze_advisory.map(str::to_string),
        precondition,
        held_transfers: plan.held_transfers.clone(),
        acknowledged_inflows: plan.acknowledged_inflows.clone(),
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
            // Past `AwaitingFunding`: nothing to record. A stream that moved
            // on to `external` has excluded its funding log and keeps any
            // expectation until it closes or completes with its exclusion row
            // in; a closed or Path A stream holds nothing. Report it as it is.
            return Ok(BurnExcessOutcome::Terminal(terminal_view(
                BurnExcessPath::External,
                Some(other),
            )));
        }
    };

    // Another unresolved signer operation would refuse every `external` run
    // on this stream while the poller holds its funding Transfer. Refuse
    // before the operator is asked to send it.
    require_wallet_intent_gates(pool, request.network, request.deposit_tx_hash)
        .await?;

    let ProvenBind { bind, underlying, .. } =
        prove_bind(pool, provider, issuer_wallet, request, state).await?;
    require_expectation_sender_linked(pool, bind.original_recipient).await?;
    require_issuer_receipt_balance_for(provider, &bind).await?;

    // Shares minted to the issuer are already there: that deposit is
    // `internal`. An expectation would lock it to a path whose funding cannot
    // exist, leaving its excess unburnable in the wallet.
    if bind.original_recipient == issuer_wallet {
        return Err(
            BurnExcessProofError::FundingFromIssuer { issuer_wallet }.into()
        );
    }

    require_no_other_funding_expectation(pool, &bind).await?;

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
        held_transfers: Vec::new(),
        acknowledged_inflows: Vec::new(),
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

    // The confirmation prompt is unbounded, so re-check the wallet intent and
    // vault-expectation gates under the signer lock immediately before write.
    let _wallet_guard = vault_service.lock_wallet().await;
    require_wallet_intent_gates(pool, request.network, request.deposit_tx_hash)
        .await?;
    require_no_other_funding_expectation(pool, &bind).await?;

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

async fn require_no_other_funding_expectation(
    pool: &Pool<Sqlite>,
    bind: &ExcessBurnBind,
) -> Result<(), BurnExcessEngineError> {
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

struct CloseCtx<'a, P> {
    pool: &'a Pool<Sqlite>,
    vault_service: &'a dyn VaultService,
    provider: &'a P,
    store: &'a Store<BurnExcess>,
    aggregate_id: &'a BurnExcessId,
}

async fn close_stream<P: Provider>(
    ctx: CloseCtx<'_, P>,
    state: Option<&BurnExcess>,
    path: BurnExcessPath,
    reason: &str,
    execute: bool,
    confirm: &(impl Fn(&str) -> io::Result<bool> + Send + Sync),
) -> Result<BurnExcessCloseView, BurnExcessEngineError> {
    let CloseCtx { pool, vault_service, provider, store, aggregate_id } = ctx;
    const CLOSABLE: &str = "AwaitingFunding, FundingExcluded, or finalized-dead Intended/Submitted";
    let state = state.ok_or_else(|| super::BurnExcessError::InvalidState {
        expected: CLOSABLE.to_string(),
        found: "Uninitialized".to_string(),
    })?;
    if matches!(
        state,
        BurnExcess::AwaitingFunding { .. } | BurnExcess::FundingExcluded { .. }
    ) {
        prove_unsigned_close_release(pool, state).await?;
    }

    let proof = match state {
        BurnExcess::AwaitingFunding { .. }
        | BurnExcess::FundingExcluded { .. } => BurnExcessCloseProof::Unsigned,
        BurnExcess::Intended { bind, sendable_tx, .. }
        | BurnExcess::Submitted { bind, sendable_tx, .. } => {
            match vault_service
                .classify_burn_tx(bind.issuer_wallet, sendable_tx)
                .await?
            {
                BurnTxStatus::FinalizedReverted => {
                    BurnExcessCloseProof::FinalizedReverted
                }
                BurnTxStatus::ProvablyDead => {
                    BurnExcessCloseProof::ProvablyDead
                }
                status => {
                    return Err(
                        BurnExcessEngineError::CloseRequiresDeadBurnIntent {
                            status,
                        },
                    );
                }
            }
        }
        other => {
            return Err(super::BurnExcessError::InvalidState {
                expected: CLOSABLE.to_string(),
                found: other.state_name().to_string(),
            }
            .into());
        }
    };

    if !execute {
        return Ok(BurnExcessCloseView {
            path,
            state: state.state_name().to_string(),
            reason: reason.to_string(),
            dry_run: true,
        });
    }

    let (effect, pending_precondition) = if matches!(
        state,
        BurnExcess::AwaitingFunding { .. }
    ) {
        (
            "releases the funding-Transfer hold; a funding Transfer already \
                 mined will be detected as an ordinary redemption during the \
                 guarded catch-up pass",
            "; confirm no funding or same-shape Transfer remains pending",
        )
    } else if state.funding_log_id().is_some() {
        (
            "clears wallet gates and releases any other mined Transfer of \
                 the funding shape for ordinary redemption during the guarded \
                 catch-up pass; the excluded funding Transfer stays skipped",
            "; confirm no same-shape Transfer remains pending",
        )
    } else {
        ("clears wallet gates only", "")
    };
    if !confirm(&format!(
        "Close excess-burn stream {aggregate_id} \
         (path={path}; {effect}{pending_precondition})?"
    ))? {
        return Err(BurnExcessEngineError::Aborted);
    }

    if let BurnExcess::Intended { held_redemptions, .. }
    | BurnExcess::Submitted { held_redemptions, .. } = state
    {
        record_held_redemptions(
            pool,
            aggregate_id.deposit_tx_hash(),
            held_redemptions,
        )
        .await?;
    }
    if let Some(funding) = state.funding_log_id() {
        require_funding_log_unclaimed(
            pool,
            funding,
            aggregate_id.deposit_tx_hash(),
        )
        .await?;
        let excluded_at = match state {
            BurnExcess::FundingExcluded { excluded_at, .. } => *excluded_at,
            _ => Utc::now(),
        };
        record_funding_exclusion(
            pool,
            funding,
            aggregate_id.deposit_tx_hash(),
            excluded_at,
        )
        .await?;
    }
    let release_through_block = Some(provider.get_block_number().await?);

    store
        .send(
            aggregate_id,
            BurnExcessCommand::CloseExcessBurn {
                reason: reason.to_string(),
                proof,
                release_through_block,
            },
        )
        .await?;
    if release_through_block.is_some() {
        release_funding_expectation(
            pool,
            aggregate_id.deposit_tx_hash(),
            release_through_block,
        )
        .await?;
    }

    // The stream is now `Closed`; report that rather than the pre-close state.
    Ok(BurnExcessCloseView {
        path,
        state: "Closed".to_string(),
        reason: reason.to_string(),
        dry_run: false,
    })
}
async fn require_expectation_sender_linked(
    pool: &Pool<Sqlite>,
    wallet: Address,
) -> Result<(), BurnExcessEngineError> {
    if matches!(
        find_by_wallet(pool, &wallet).await?,
        Some(AccountView::LinkedToAlpaca { .. })
    ) {
        return Ok(());
    }

    Err(BurnExcessEngineError::ExpectationSenderNotLinked { wallet })
}

async fn prove_unsigned_close_release(
    pool: &Pool<Sqlite>,
    state: &BurnExcess,
) -> Result<(), BurnExcessEngineError> {
    let bind = state.bind();
    require_expectation_sender_linked(pool, bind.original_recipient).await?;
    let (underlying, _token, network, _burn_mode) =
        load_mint_asset(pool, &bind.issuer_request_id).await?;
    let listed_vault = find_vault(pool, &underlying, &network).await?;
    if listed_vault == Some(bind.vault) {
        return Ok(());
    }

    Err(BurnExcessEngineError::UnsignedCloseVaultRepointed {
        underlying,
        network,
        bound_vault: bind.vault,
        listed_vault,
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
                let funding = mutation
                    .plan
                    .funding_log_id
                    .clone()
                    .ok_or(BurnExcessProofError::FundingTxHashRequired)?;
                // Irreversible boundary. Under the wallet lock, so a second
                // stream claiming the same log cannot pass these checks while
                // this one records its exclusion: once recorded, this stream
                // is an unresolved intent and its row owns the log.
                require_wallet_intent_gates(
                    mutation.pool,
                    mutation.request.network,
                    mutation.request.deposit_tx_hash,
                )
                .await?;
                if redemption_exists_for_tx(mutation.pool, funding.tx_hash)
                    .await?
                {
                    return Err(BurnExcessProofError::FundingAlreadyRedeemed {
                        tx_hash: funding.tx_hash,
                        log_index: funding.log_index,
                    }
                    .into());
                }
                require_funding_log_unclaimed(
                    mutation.pool,
                    &funding,
                    mutation.request.deposit_tx_hash,
                )
                .await?;
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
    // The insert keeps an existing owner, so existence alone would also pass
    // for a row another deposit wrote.
    require_funding_log_unclaimed(
        pool,
        &funding_log_id,
        aggregate_id.deposit_tx_hash(),
    )
    .await?;

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
    let aggregate_id = BurnExcessId::new(deposit_tx_hash).to_string();
    let competing_signer = sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM active_signer_intents
            WHERE network = ?
              AND aggregate_type != 'BurnExcess'
        )
        ",
    )
    .bind(network.as_str())
    .fetch_one(pool)
    .await?;
    if competing_signer {
        return Err(BurnExcessEngineError::UnresolvedSignerIntent { network });
    }
    let competing_excess = sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM events AS intent
            WHERE intent.aggregate_type = 'BurnExcess'
              AND intent.aggregate_id != ?
              AND intent.event_type IN (
                  'BurnExcessEvent::FundingExclusionRecorded',
                  'BurnExcessEvent::ExcessBurnIntended'
              )
              AND COALESCE(
                  json_extract(
                      intent.payload,
                      '$.FundingExclusionRecorded.bind.network'
                  ),
                  json_extract(
                      intent.payload,
                      '$.ExcessBurnIntended.bind.network'
                  )
              ) = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM events AS terminal
                  WHERE terminal.aggregate_type = intent.aggregate_type
                    AND terminal.aggregate_id = intent.aggregate_id
                    AND terminal.event_type IN (
                        'BurnExcessEvent::ExcessBurnCompleted',
                        'BurnExcessEvent::ExcessBurnClosed'
                    )
              )
        )
        ",
    )
    .bind(aggregate_id)
    .bind(network.as_str())
    .fetch_one(pool)
    .await?;
    if competing_excess {
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
    // Under the wallet lock: the insert below keeps an existing row's owner,
    // so a log another stream claimed after this plan was proven must refuse
    // here rather than pass the existence check as if it were this stream's.
    require_funding_log_unclaimed(pool, funding, plan.bind.deposit_tx_hash)
        .await?;
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

async fn require_funding_log_unclaimed(
    pool: &Pool<Sqlite>,
    funding: &FundingTransferId,
    deposit_tx_hash: B256,
) -> Result<(), BurnExcessEngineError> {
    if is_funding_log_excluded_for_another_deposit(
        pool,
        funding,
        deposit_tx_hash,
    )
    .await?
    {
        return Err(
            BurnExcessProofError::FundingLogExcludedForAnotherDeposit {
                tx_hash: funding.tx_hash,
                log_index: funding.log_index,
            }
            .into(),
        );
    }
    Ok(())
}

/// Live-state gates re-read at the irreversible sign boundary.
///
/// `prove_plan` reads these before `print_plan` and the operator confirm
/// prompt, which blocks on stdin for an unbounded time. Balances and the set of
/// held same-shape Transfers can both change in that window. A Path B plan
/// therefore re-reads both at one block no older than its proof and requires
/// the exact anchored set to remain unchanged before signing.
async fn require_issuer_balances<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    vault_service: &dyn VaultService,
    plan: &ProvenPlan,
) -> Result<(), BurnExcessEngineError> {
    let bind = &plan.bind;
    require_issuer_receipt_balance_for(provider, bind).await?;

    let (share_balance, held_redemptions) = match plan.share_balance_snapshot {
        Some(proven_snapshot) => {
            let snapshot = chain_snapshot(provider).await?;
            if snapshot.number < proven_snapshot.number {
                return Err(BurnExcessEngineError::ChainBehindProvenPlan {
                    proven_block: proven_snapshot.number,
                    block: snapshot.number,
                });
            }
            let asset = HeldRedemptionAsset {
                underlying: &plan.underlying,
                token: &plan.token,
                burn_mode: plan.burn_mode,
            };
            require_snapshot_block(
                provider,
                proven_snapshot.number,
                proven_snapshot.hash,
            )
            .await?;
            let mut held_redemptions = plan.held_redemptions.clone();
            if snapshot.number > proven_snapshot.number {
                let additions = held_same_shape_redemptions(
                    pool,
                    provider,
                    bind,
                    snapshot,
                    HeldRedemptionScan {
                        funding: plan.funding_log_id.as_ref(),
                        asset,
                        scan_after: Some(proven_snapshot.number),
                        acknowledged_inflows: &[],
                    },
                )
                .await?;
                held_redemptions.extend(additions);
            }
            (
                issuer_share_balance_at(provider, bind, snapshot).await?,
                held_redemptions,
            )
        }
        None => (
            vault_service
                .get_share_balance(bind.vault, bind.issuer_wallet)
                .await?,
            plan.held_redemptions.clone(),
        ),
    };
    if held_redemptions != plan.held_redemptions {
        return Err(BurnExcessEngineError::HeldRedemptionsChanged {
            proven: plan.held_redemptions.clone(),
            current: held_redemptions,
        });
    }
    require_exact_issuer_share_balance(
        share_balance,
        bind.shares,
        plan.held_redemptions.len(),
    )?;
    Ok(())
}

/// The issuer must still hold enough of the deposit's receipt to burn the
/// excess against it.
async fn require_issuer_receipt_balance_for<P: Provider>(
    provider: &P,
    bind: &ExcessBurnBind,
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
    Ok(())
}

/// An unsigned excess-burn share-balance proof: attributed same-shape
/// redemptions counted for Path B, and the hash-pinned chain snapshot both the
/// balance and complete issuer-wallet liability set were read from.
struct ShareBalanceProof {
    held_redemptions: Vec<HeldTransferRedemption>,
    snapshot: ChainSnapshot,
}

#[derive(Debug, Clone, Copy)]
struct ChainSnapshot {
    number: u64,
    hash: B256,
}

#[derive(Clone, Copy)]
struct HeldRedemptionAsset<'a> {
    underlying: &'a UnderlyingSymbol,
    token: &'a TokenSymbol,
    burn_mode: crate::config::VaultMode,
}

#[derive(Clone, Copy)]
struct HeldRedemptionScan<'a> {
    funding: Option<&'a FundingTransferId>,
    asset: HeldRedemptionAsset<'a>,
    scan_after: Option<u64>,
    acknowledged_inflows: &'a [AcknowledgedInboundTransfer],
}

/// The issuer must hold exactly the excess, plus same-shape redemptions an
/// external stream will durably anchor. Every inbound share Transfer since the
/// duplicate deposit is reconciled for both paths, so unrelated shares cannot
/// numerically offset an outbound while the wallet's net balance looks exact.
/// The balance and liability set are read at one hash-pinned chain snapshot.
async fn require_issuer_share_balance<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    bind: &ExcessBurnBind,
    scan: HeldRedemptionScan<'_>,
) -> Result<ShareBalanceProof, BurnExcessEngineError> {
    let snapshot = chain_snapshot(provider).await?;
    let balance = issuer_share_balance_at(provider, bind, snapshot).await?;

    let held_redemptions =
        held_same_shape_redemptions(pool, provider, bind, snapshot, scan)
            .await?;
    require_exact_issuer_share_balance(
        balance,
        bind.shares,
        held_redemptions.len(),
    )?;

    if !held_redemptions.is_empty() {
        let held_tx_hashes: Vec<B256> =
            held_redemptions.iter().map(|held| held.transfer.tx_hash).collect();
        info!(
            target: "burn_excess",
            deposit_tx_hash = %bind.deposit_tx_hash,
            vault = %bind.vault,
            held_transfers = held_redemptions.len(),
            held_tx_hashes = ?held_tx_hashes,
            %balance,
            block = snapshot.number,
            block_hash = %snapshot.hash,
            "Counted same-shape Transfers held with the funding Transfer toward \
             the issuer share balance; this stream releases them once it completes"
        );
    }
    Ok(ShareBalanceProof { held_redemptions, snapshot })
}

async fn issuer_share_balance_at<P: Provider>(
    provider: &P,
    bind: &ExcessBurnBind,
    snapshot: ChainSnapshot,
) -> Result<U256, BurnExcessEngineError> {
    Ok(OffchainAssetReceiptVault::new(bind.vault, provider)
        .balanceOf(bind.issuer_wallet)
        .block(BlockId::hash_canonical(snapshot.hash))
        .call()
        .await?)
}

async fn chain_snapshot<P: Provider>(
    provider: &P,
) -> Result<ChainSnapshot, BurnExcessEngineError> {
    let number = provider.get_block_number().await?;
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(number))
        .await?
        .ok_or(BurnExcessEngineError::ChainSnapshotBlockMissing {
            block: number,
        })?;
    Ok(ChainSnapshot { number, hash: block.header.hash })
}

async fn require_snapshot_block<P: Provider>(
    provider: &P,
    number: u64,
    expected_hash: B256,
) -> Result<(), BurnExcessEngineError> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(number))
        .await?
        .ok_or(BurnExcessEngineError::ChainSnapshotBlockMissing {
            block: number,
        })?;
    if block.header.hash != expected_hash {
        return Err(BurnExcessEngineError::ChainSnapshotDisconnected {
            block: number,
            expected_parent: expected_hash,
            actual_parent: block.header.hash,
        });
    }
    Ok(())
}

/// Transfers into the issuer wallet since the duplicate deposit. Every inflow
/// except that deposit's one authorized mint leg must either be the exact Path
/// B funding log, a same-shape redemption this stream will anchor, a fully
/// forwarded net-zero mint, or have a durable successful burn. Scanning both
/// paths and all senders prevents an unrelated inbound liability from
/// numerically offsetting an outbound while the wallet's net balance looks
/// exact.
struct InboundTransferScan {
    from_block: u64,
    deposit_transaction_index: u64,
    chunks: Vec<Vec<Log>>,
}

async fn issuer_inbound_transfer_scan<P: Provider>(
    provider: &P,
    bind: &ExcessBurnBind,
    snapshot: ChainSnapshot,
    scan_after: Option<u64>,
) -> Result<InboundTransferScan, BurnExcessEngineError> {
    let deposit_receipt = provider
        .get_transaction_receipt(bind.deposit_tx_hash)
        .await?
        .ok_or(BurnExcessEngineError::DepositTxInvalid {
            tx_hash: bind.deposit_tx_hash,
        })?;
    if !deposit_receipt.status() {
        return Err(BurnExcessEngineError::DepositTxInvalid {
            tx_hash: bind.deposit_tx_hash,
        });
    }
    let deposit_proof = parse_deposit_proof(
        &deposit_receipt,
        bind.vault,
        bind.deposit_tx_hash,
    )?;
    bind_deposit_proof(
        &bind.issuer_request_id,
        bind.receipt_id,
        bind.shares,
        &deposit_proof,
    )?;
    if deposit_proof.original_recipient != bind.original_recipient {
        return Err(super::BurnExcessError::BindMismatch.into());
    }
    let from_block = deposit_receipt.block_number.ok_or(
        BurnExcessEngineError::DepositTxInvalid {
            tx_hash: bind.deposit_tx_hash,
        },
    )?;
    let deposit_block_hash = deposit_receipt.block_hash.ok_or(
        BurnExcessEngineError::DepositTxInvalid {
            tx_hash: bind.deposit_tx_hash,
        },
    )?;
    let deposit_transaction_index = deposit_receipt.transaction_index.ok_or(
        BurnExcessEngineError::DepositTxInvalid {
            tx_hash: bind.deposit_tx_hash,
        },
    )?;
    if from_block > snapshot.number {
        return Err(BurnExcessEngineError::ChainBehindDeposit {
            deposit_block: from_block,
            block: snapshot.number,
        });
    }
    require_snapshot_block(provider, from_block, deposit_block_hash).await?;
    require_snapshot_block(provider, snapshot.number, snapshot.hash).await?;

    // An open expectation can keep the poll checkpoint behind while unrelated
    // transfers continue. Range queries reconcile every issuer inflow since
    // the duplicate deposit on the initial proof, and only blocks newer than
    // the planned snapshot at the sign boundary. Re-reading both boundary
    // hashes after the scan rejects a reorg that raced the range queries.
    let scan_from =
        scan_after.and_then(|block| block.checked_add(1)).unwrap_or(from_block);
    let ranges =
        (scan_from..=snapshot.number).step_by(HELD_SCAN_BLOCK_CHUNK_STEP);
    let chunks = stream::iter(ranges)
        .map(|chunk_from| {
            let chunk_to = chunk_from
                .saturating_add(HELD_SCAN_BLOCK_CHUNK_SIZE - 1)
                .min(snapshot.number);
            let filter = Filter::new()
                .address(bind.vault)
                .event_signature(
                    OffchainAssetReceiptVault::Transfer::SIGNATURE_HASH,
                )
                .topic2(bind.issuer_wallet.into_word())
                .from_block(chunk_from)
                .to_block(chunk_to);
            async move { provider.get_logs(&filter).await }
        })
        .buffered(HELD_SCAN_CONCURRENCY)
        .try_collect()
        .await?;
    require_snapshot_block(provider, from_block, deposit_block_hash).await?;
    require_snapshot_block(provider, snapshot.number, snapshot.hash).await?;

    Ok(InboundTransferScan { from_block, deposit_transaction_index, chunks })
}

async fn held_same_shape_redemptions<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    bind: &ExcessBurnBind,
    snapshot: ChainSnapshot,
    scan: HeldRedemptionScan<'_>,
) -> Result<Vec<HeldTransferRedemption>, BurnExcessEngineError> {
    let HeldRedemptionScan { funding, asset, scan_after, acknowledged_inflows } =
        scan;
    let InboundTransferScan { from_block, deposit_transaction_index, chunks } =
        issuer_inbound_transfer_scan(provider, bind, snapshot, scan_after)
            .await?;

    let mut held_transfers = Vec::new();
    let mut funding_seen = funding.is_none() || scan_after.is_some();
    let mut duplicate_deposit_mint_seen = scan_after.is_some();
    let mut acknowledgements_seen = vec![false; acknowledged_inflows.len()];
    for logs in &chunks {
        for log in logs {
            let Some(block_hash) = log.block_hash else {
                return Err(
                    BurnExcessEngineError::IssuerShareTransferMissingIdentity {
                        tx_hash: log.transaction_hash,
                        log_index: log.log_index,
                    },
                );
            };
            let transfer = issuer_inbound_transfer(bind, log)?;
            let is_duplicate_deposit_mint = transfer.tx_hash
                == bind.deposit_tx_hash
                && transfer.from == Address::ZERO
                && transfer.amount == bind.shares;
            if is_duplicate_deposit_mint {
                if duplicate_deposit_mint_seen {
                    return Err(BurnExcessEngineError::DepositTxInvalid {
                        tx_hash: bind.deposit_tx_hash,
                    });
                }
                duplicate_deposit_mint_seen = true;
                continue;
            }
            let funding_key_matches = funding.is_some_and(|funding| {
                funding.tx_hash == transfer.tx_hash
                    && funding.log_index == transfer.log_index
            });
            if funding_key_matches {
                let funding =
                    funding.ok_or(BurnExcessEngineError::FundingTxInvalid {
                        tx_hash: transfer.tx_hash,
                    })?;
                if &transfer != funding {
                    return Err(BurnExcessEngineError::FundingTxInvalid {
                        tx_hash: transfer.tx_hash,
                    });
                }
                let (Some(block_number), Some(transaction_index)) =
                    (log.block_number, log.transaction_index)
                else {
                    return Err(BurnExcessEngineError::FundingTxInvalid {
                        tx_hash: transfer.tx_hash,
                    });
                };
                if (block_number, transaction_index)
                    <= (from_block, deposit_transaction_index)
                {
                    return Err(BurnExcessEngineError::FundingTxInvalid {
                        tx_hash: transfer.tx_hash,
                    });
                }
                funding_seen = true;
                continue;
            }
            let receipt = provider
                .get_transaction_receipt(transfer.tx_hash)
                .await?
                .ok_or(BurnExcessEngineError::HeldTransferReceiptMissing {
                    tx_hash: transfer.tx_hash,
                })?;
            if receipt.block_hash != Some(block_hash) {
                return Err(
                    BurnExcessEngineError::HeldTransferReceiptInconsistent {
                        tx_hash: transfer.tx_hash,
                    },
                );
            }
            if transfer.from == Address::ZERO
                && issuer_mint_fully_forwarded(
                    receipt.inner.logs(),
                    bind.vault,
                    bind.issuer_wallet,
                )
            {
                continue;
            }
            let redemption_exists =
                redemption_exists_for_tx(pool, transfer.tx_hash).await?;
            let redemption_is_this_transfer = redemption_exists
                && redemption_is_own(pool, receipt.inner.logs(), &transfer)
                    .await?;
            let redemption_completed = redemption_is_this_transfer
                && redemption_burn_completed(pool, transfer.tx_hash).await?;
            let exclusion_completed =
                excluded_burn_completed(pool, &transfer).await?;
            if redemption_completed || exclusion_completed {
                continue;
            }
            if let Some((index, _)) = acknowledged_inflows
                .iter()
                .enumerate()
                .find(|(_, acknowledged)| {
                    acknowledged.tx_hash == transfer.tx_hash
                        && acknowledged.log_index == transfer.log_index
                })
            {
                acknowledgements_seen[index] = true;
                continue;
            }
            let same_shape = funding.is_some_and(|funding| {
                transfer.from == funding.from
                    && transfer.amount == funding.amount
            });

            if !same_shape {
                return Err(
                    BurnExcessEngineError::UnresolvedIssuerShareTransfer {
                        tx_hash: transfer.tx_hash,
                        log_index: transfer.log_index,
                    },
                );
            }
            if is_excluded_funding_log(
                pool,
                transfer.network,
                transfer.vault,
                transfer.tx_hash,
                transfer.log_index,
            )
            .await?
            {
                return Err(
                    BurnExcessEngineError::UnresolvedIssuerShareTransfer {
                        tx_hash: transfer.tx_hash,
                        log_index: transfer.log_index,
                    },
                );
            }
            if competes_for_redemption_key(
                pool,
                receipt.inner.logs(),
                &transfer,
                funding,
            )
            .await?
            {
                return Err(
                    BurnExcessProofError::HeldTransfersShareTransaction {
                        tx_hash: transfer.tx_hash,
                    }
                    .into(),
                );
            }
            if redemption_exists {
                if redemption_completed {
                    continue;
                }
                if redemption_is_this_transfer {
                    return Err(
                        BurnExcessEngineError::UnresolvedIssuerShareTransfer {
                            tx_hash: transfer.tx_hash,
                            log_index: transfer.log_index,
                        },
                    );
                }
                return Err(
                    BurnExcessProofError::HeldTransfersShareTransaction {
                        tx_hash: transfer.tx_hash,
                    }
                    .into(),
                );
            }
            let block_number = log.block_number.ok_or(
                BurnExcessEngineError::IssuerShareTransferMalformed {
                    tx_hash: log.transaction_hash,
                    log_index: log.log_index,
                },
            )?;
            held_transfers.push((transfer, block_number));
        }
    }
    require_acknowledgements_seen(
        acknowledged_inflows,
        &acknowledgements_seen,
    )?;
    if !duplicate_deposit_mint_seen {
        return Err(BurnExcessEngineError::DepositTxInvalid {
            tx_hash: bind.deposit_tx_hash,
        });
    }
    if let Some(funding) = funding.filter(|_| !funding_seen) {
        return Err(BurnExcessEngineError::FundingTxInvalid {
            tx_hash: funding.tx_hash,
        });
    }

    materialize_held_redemptions(pool, bind, &asset, held_transfers).await
}
fn require_acknowledgements_seen(
    acknowledged_inflows: &[AcknowledgedInboundTransfer],
    acknowledgements_seen: &[bool],
) -> Result<(), BurnExcessEngineError> {
    let Some((_, acknowledged)) = acknowledged_inflows
        .iter()
        .enumerate()
        .find(|(index, _)| !acknowledgements_seen[*index])
    else {
        return Ok(());
    };
    Err(BurnExcessEngineError::AcknowledgedInboundNotFound {
        tx_hash: acknowledged.tx_hash,
        log_index: acknowledged.log_index,
    })
}

async fn materialize_held_redemptions(
    pool: &Pool<Sqlite>,
    bind: &ExcessBurnBind,
    asset: &HeldRedemptionAsset<'_>,
    held_transfers: Vec<(FundingTransferId, u64)>,
) -> Result<Vec<HeldTransferRedemption>, BurnExcessEngineError> {
    if held_transfers.is_empty() {
        return Ok(Vec::new());
    }
    // Counted shares must be redeemed once released. Anchor the linked AP
    // account now so a later wallet unlink cannot revoke admission for tokens
    // already mined and relied upon by the excess burn.
    let Some(AccountView::LinkedToAlpaca { client_id, alpaca_account, .. }) =
        find_by_wallet(pool, &bind.original_recipient).await?
    else {
        return Err(BurnExcessProofError::HeldTransferSenderNotLinked {
            wallet: bind.original_recipient,
        }
        .into());
    };
    Ok(held_transfers
        .into_iter()
        .map(|(transfer, block_number)| HeldTransferRedemption {
            transfer,
            block_number,
            client_id,
            alpaca_account: alpaca_account.clone(),
            underlying: asset.underlying.clone(),
            token: asset.token.clone(),
            burn_mode: asset.burn_mode,
        })
        .collect())
}

fn issuer_inbound_transfer(
    bind: &ExcessBurnBind,
    log: &Log,
) -> Result<FundingTransferId, BurnExcessEngineError> {
    let decoded = log
        .log_decode::<OffchainAssetReceiptVault::Transfer>()
        .map_err(|_| BurnExcessEngineError::IssuerShareTransferMalformed {
            tx_hash: log.transaction_hash,
            log_index: log.log_index,
        })?;
    let data = decoded.data();
    if log.address() != bind.vault || data.to != bind.issuer_wallet {
        return Err(BurnExcessEngineError::IssuerShareTransferMalformed {
            tx_hash: log.transaction_hash,
            log_index: log.log_index,
        });
    }
    let (Some(tx_hash), Some(log_index)) =
        (log.transaction_hash, log.log_index)
    else {
        return Err(
            BurnExcessEngineError::IssuerShareTransferMissingIdentity {
                tx_hash: log.transaction_hash,
                log_index: log.log_index,
            },
        );
    };
    Ok(FundingTransferId {
        network: bind.network,
        vault: bind.vault,
        tx_hash,
        log_index,
        from: data.from,
        to: data.to,
        amount: data.value,
    })
}

fn issuer_mint_fully_forwarded(
    receipt_logs: &[Log],
    vault: Address,
    issuer_wallet: Address,
) -> bool {
    let (minted, forwarded) = receipt_logs
        .iter()
        .filter(|log| log.address() == vault)
        .filter_map(|log| {
            log.log_decode::<OffchainAssetReceiptVault::Transfer>().ok()
        })
        .fold(
            (Some(U256::ZERO), Some(U256::ZERO)),
            |(minted, forwarded), decoded| {
                let data = decoded.data();
                let minted =
                    if data.from == Address::ZERO && data.to == issuer_wallet {
                        minted.and_then(|total| total.checked_add(data.value))
                    } else {
                        minted
                    };
                let forwarded = if data.from == issuer_wallet
                    && data.to != Address::ZERO
                {
                    forwarded.and_then(|total| total.checked_add(data.value))
                } else {
                    forwarded
                };
                (minted, forwarded)
            },
        );
    matches!(
        (minted, forwarded),
        (Some(minted), Some(forwarded))
            if !minted.is_zero() && minted == forwarded
    )
}

async fn redemption_burn_completed(
    pool: &Pool<Sqlite>,
    detected_tx_hash: B256,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type IN (
                  'RedemptionEvent::TokensBurned',
                  'RedemptionEvent::OrchestratorTokensBurned',
                  'RedemptionEvent::ExistingBurnRecovered',
                  'RedemptionEvent::OrchestratorBurnRecovered',
                  'RedemptionEvent::BurnForceCompleted'
              )
        )
        ",
    )
    .bind(IssuerRedemptionRequestId::new(detected_tx_hash).to_string())
    .fetch_one(pool)
    .await
}

async fn excluded_burn_completed(
    pool: &Pool<Sqlite>,
    transfer: &FundingTransferId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM burn_excess_funding_exclusions AS exclusion
            JOIN events AS terminal
              ON terminal.aggregate_type = 'BurnExcess'
             AND terminal.aggregate_id = exclusion.deposit_tx_hash
             AND terminal.event_type =
                 'BurnExcessEvent::ExcessBurnCompleted'
            WHERE exclusion.network = ?
              AND exclusion.vault = ?
              AND exclusion.tx_hash = ?
              AND exclusion.log_index = ?
        )
        ",
    )
    .bind(transfer.network.as_str())
    .bind(address_key(transfer.vault))
    .bind(hash_key(transfer.tx_hash))
    .bind(log_index_key(transfer.log_index)?)
    .fetch_one(pool)
    .await
}

/// Whether the Redemption keyed by `transfer`'s transaction can only have
/// been detected from this exact log. Historical `Detected` events do not
/// carry the emitting vault or log index, so the event's sender and amount
/// must identify exactly one inbound Transfer in the receipt; ambiguity fails
/// closed even when the other vault is no longer configured.
async fn redemption_is_own(
    pool: &Pool<Sqlite>,
    receipt_logs: &[Log],
    transfer: &FundingTransferId,
) -> Result<bool, BurnExcessEngineError> {
    let payload = sqlx::query_scalar::<_, String>(
        "
        SELECT payload
        FROM events
        WHERE aggregate_type = 'Redemption'
          AND aggregate_id = ?
          AND event_type = 'RedemptionEvent::Detected'
        ",
    )
    .bind(IssuerRedemptionRequestId::new(transfer.tx_hash).to_string())
    .fetch_optional(pool)
    .await?;
    let Some(payload) = payload else {
        return Ok(false);
    };
    let RedemptionEvent::Detected { wallet, quantity, .. } =
        serde_json::from_str(&payload)?
    else {
        return Ok(false);
    };
    let amount = quantity.to_u256_with_18_decimals()?;
    let mut matching_logs = receipt_logs.iter().filter_map(|log| {
        let decoded =
            log.log_decode::<OffchainAssetReceiptVault::Transfer>().ok()?;
        let data = decoded.data();
        (data.from == wallet && data.to == transfer.to && data.value == amount)
            .then_some((log.address(), log.log_index))
    });
    let Some(candidate) = matching_logs.next() else {
        return Ok(false);
    };
    if matching_logs.next().is_some() {
        return Ok(false);
    }
    Ok(candidate == (transfer.vault, Some(transfer.log_index)))
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
    ensure_held_redemptions_compatible(
        ctx.pool,
        ctx.aggregate_id.deposit_tx_hash(),
        &ctx.plan.held_redemptions,
    )
    .await?;
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
        ctx.plan,
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
                held_redemptions: ctx.plan.held_redemptions.clone(),
                acknowledged_inflows: ctx.plan.acknowledged_inflows.clone(),
            },
        )
        .await?;
    anchor_held_redemptions(ctx).await?;
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
        chain_id: effective_chain_id(&ctx.plan.bind, ctx.request),
    })
    .await
}

async fn anchor_held_redemptions<P: Provider>(
    ctx: &MutationCtx<'_, P>,
) -> Result<(), BurnExcessEngineError> {
    ctx.store
        .send(
            ctx.aggregate_id,
            BurnExcessCommand::AnchorHeldRedemptions {
                held_redemptions: ctx.plan.held_redemptions.clone(),
            },
        )
        .await?;
    // Dual-write: the event is source of truth; the exact-log index is what
    // lets the poller use this admission after the shape expectation clears.
    record_held_redemptions(
        ctx.pool,
        ctx.aggregate_id.deposit_tx_hash(),
        &ctx.plan.held_redemptions,
    )
    .await?;
    Ok(())
}

async fn resume_from_intended<P: Provider>(
    ctx: &MutationCtx<'_, P>,
    owner: Address,
    sendable_tx: crate::vault::SendableTxWithHash,
    receipt_id: U256,
    shares: U256,
    bind: &ExcessBurnBind,
) -> Result<(), BurnExcessEngineError> {
    anchor_held_redemptions(ctx).await?;
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
                chain_id: effective_chain_id(bind, ctx.request),
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
                chain_id: effective_chain_id(bind, ctx.request),
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
    anchor_held_redemptions(ctx).await?;
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
                tx_id,
                dust_shares: sendable_tx.dust_shares,
                receipt_id,
                shares,
                bind,
                owner,
                chain_id: effective_chain_id(bind, ctx.request),
            })
            .await
        }
        BurnTxStatus::StillMineable => {
            let submitted = ctx
                .vault_service
                .submit_burn(
                    multi_burn_params_from_bind(bind, &Bytes::new(), None),
                    sendable_tx.clone(),
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
                chain_id: effective_chain_id(bind, ctx.request),
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
) -> Result<
    (UnderlyingSymbol, TokenSymbol, Network, crate::config::VaultMode),
    BurnExcessEngineError,
> {
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
        Mint::Initiated { underlying, token, network, mint_mode, .. }
        | Mint::JournalConfirmed {
            underlying,
            token,
            network,
            mint_mode,
            ..
        }
        | Mint::JournalRejected {
            underlying, token, network, mint_mode, ..
        }
        | Mint::Minting { underlying, token, network, mint_mode, .. }
        | Mint::TxIntended { underlying, token, network, mint_mode, .. }
        | Mint::TxSubmitted { underlying, token, network, mint_mode, .. }
        | Mint::MintingFailed {
            underlying, token, network, mint_mode, ..
        }
        | Mint::CallbackPending {
            underlying, token, network, mint_mode, ..
        }
        | Mint::Completed { underlying, token, network, mint_mode, .. } => {
            Ok((underlying, token, network, mint_mode))
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

/// Proves the funding Transfer in `expectation.tx_hash`. Among identical
/// funding-shape logs it takes the lowest one no other deposit has claimed,
/// so two streams funded in one batched transaction each find their own.
async fn prove_funding_transfer<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: &P,
    expectation: FundingTransferExpectation,
    deposit_tx_hash: B256,
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

    // Only funding-shape logs can be claimed; another deposit's exclusion of
    // a log of a different shape is not this stream's concern.
    let mut unclaimed = Vec::with_capacity(candidates.len());
    let mut first_claimed = None;
    for candidate in candidates {
        let candidate_id = FundingTransferId {
            network: expectation.network,
            vault: candidate.vault,
            tx_hash: expectation.tx_hash,
            log_index: candidate.log_index,
            from: candidate.from,
            to: candidate.to,
            amount: candidate.amount,
        };
        let funding_shape = candidate.vault == expectation.vault
            && candidate.from == expectation.from
            && candidate.to == expectation.to
            && candidate.amount == expectation.amount;
        if funding_shape
            && is_funding_log_excluded_for_another_deposit(
                pool,
                &candidate_id,
                deposit_tx_hash,
            )
            .await?
        {
            first_claimed.get_or_insert(candidate_id);
        } else {
            unclaimed.push(candidate);
        }
    }

    match (select_funding_transfer(&expectation, &unclaimed), first_claimed) {
        (Ok(funding), _) => Ok(funding),
        (Err(_), Some(claimed)) => {
            Err(BurnExcessProofError::FundingLogExcludedForAnotherDeposit {
                tx_hash: claimed.tx_hash,
                log_index: claimed.log_index,
            }
            .into())
        }
        (Err(error), None) => Err(error.into()),
    }
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
    use alloy::providers::mock::Asserter;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::types::TransactionRequest;
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
    use crate::account::{
        Account, AccountCommand, AccountEvent, AlpacaAccountNumber, ClientId,
        Email,
    };
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::burn_excess::BurnExcessEvent;
    use crate::burn_excess::exclusion::is_excluded_funding_log;
    use crate::burn_excess::expectation::{
        FundingTransferStatus, classify_funding_transfer,
        clear_funding_expectation, clear_held_redemption,
        held_redemption_vaults, rebuild_funding_expectation_index,
    };
    use crate::burn_excess::proof::BurnExcessMode;
    use crate::mint::{MintEvent, TokenizationRequestId};
    use crate::poll_checkpoint::{
        advance_transfer_poll_observed, load_transfer_poll,
    };
    use crate::receipt_inventory::{
        ReceiptSource, ReceiptVaultKey, send_receipt_inventory_command,
    };
    use crate::redemption::test_utils::{
        link_ap_wallet, transfer_poller_for_tests,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm, logs_contain_at};
    use crate::tokenized_asset::{
        AssetKey, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::vault::ReceiptInformation;
    use crate::vault::mock::MockVaultService;
    use crate::vault::service::{RealBlockchainService, ResyncNonceManager};
    use crate::vault::{
        BurnTxStatus, MultiBurnResult, MultiBurnResultEntry, PreparedMintTx,
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

    async fn link_ap_wallet_pair(
        pool: &Pool<Sqlite>,
        first: Address,
        second: Address,
    ) {
        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();
        let client_id = ClientId::new();
        account_store
            .send(
                &client_id,
                AccountCommand::Register {
                    client_id,
                    email: Email::new("paired@example.com").unwrap(),
                },
            )
            .await
            .unwrap();
        account_store
            .send(
                &client_id,
                AccountCommand::LinkToAlpaca {
                    alpaca_account: AlpacaAccountNumber("PAIRED".into()),
                },
            )
            .await
            .unwrap();
        for wallet in [first, second] {
            account_store
                .send(&client_id, AccountCommand::WhitelistWallet { wallet })
                .await
                .unwrap();
        }
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
            acknowledged_inflows: Vec::new(),
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
            chain_id: ANVIL_CHAIN_ID,
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
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
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

        tokio::time::timeout(
            Duration::from_secs(10),
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
            ),
        )
        .await
        .expect("fresh external execute must not deadlock")
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

    /// A live Path B stream with its expectation recorded and an AP-linked
    /// original recipient that also holds `own_shares` of its own to redeem.
    struct LiveHoldFixture {
        issuer_request_id: IssuerMintRequestId,
        receipt_id: U256,
        deposit_tx: B256,
        recipient: PrivateKeySigner,
    }

    impl LiveHoldFixture {
        fn request(
            &self,
            mode: BurnExcessMode,
            funding_tx_hash: Option<B256>,
        ) -> BurnExcessRequest {
            BurnExcessRequest {
                poller_guard: PollerGuard::FundingExpected,
                ..request(
                    mode,
                    self.issuer_request_id.clone(),
                    self.deposit_tx,
                    self.receipt_id,
                    funding_tx_hash,
                    true,
                )
            }
        }
    }

    async fn live_hold_fixture(
        pool: &Pool<Sqlite>,
        evm: &LocalEvm,
        service: &dyn VaultService,
        provider: &impl Provider,
        own_shares: U256,
    ) -> LiveHoldFixture {
        let ExternalDeposit {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            recipient,
        } = setup_external_deposit(pool, evm).await;
        link_ap_wallet(pool, recipient.address()).await;

        let issuer_provider = issuer_provider(evm).await;
        let vault_issuer =
            OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider);
        let deposit_call = vault_issuer
            .deposit(
                own_shares,
                evm.wallet_address,
                U256::from(10).pow(U256::from(18)),
                sample_receipt_info(IssuerMintRequestId::random())
                    .encode()
                    .unwrap(),
            )
            .calldata()
            .clone();
        let transfer_call = vault_issuer
            .transfer(recipient.address(), own_shares)
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

        let fixture = LiveHoldFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            recipient,
        };
        run_burn_excess(
            pool,
            service,
            provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::ExpectFunding, None),
            |_| Ok(true),
        )
        .await
        .unwrap();
        fixture
    }

    async fn issuer_provider(evm: &LocalEvm) -> impl Provider {
        ProviderBuilder::new()
            .wallet(EthereumWallet::from(
                PrivateKeySigner::from_bytes(&evm.private_key).unwrap(),
            ))
            .connect(&evm.endpoint)
            .await
            .unwrap()
    }

    /// Sends `amounts` from `sender` to the issuer wallet in ONE transaction
    /// (a vault multicall when there is more than one) and returns its hash.
    async fn transfer_to_issuer(
        evm: &LocalEvm,
        sender: &PrivateKeySigner,
        amounts: &[U256],
    ) -> B256 {
        let sender_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(sender.clone()))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let vault =
            OffchainAssetReceiptVault::new(evm.vault_address, &sender_provider);
        let calls = amounts
            .iter()
            .map(|amount| {
                vault.transfer(evm.wallet_address, *amount).calldata().clone()
            })
            .collect();
        vault
            .multicall(calls)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap()
            .transaction_hash
    }

    async fn record_redemption_burn_completed(
        pool: &Pool<Sqlite>,
        tx_hash: B256,
    ) {
        let issuer_request_id = IssuerRedemptionRequestId::new(tx_hash);
        let event = RedemptionEvent::BurnForceCompleted {
            issuer_request_id: issuer_request_id.clone(),
            burn_tx_hash: B256::random(),
            block_number: 1,
            reason: "test burn completed".into(),
            acknowledged_unresolved_burn_tx_hash: None,
            completed_at: Utc::now(),
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
                '2.0',
                ?,
                '{}'
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
            ",
        )
        .bind(issuer_request_id.to_string())
        .bind(event.event_type())
        .bind(serde_json::to_string(&event).unwrap())
        .bind(issuer_request_id.to_string())
        .execute(pool)
        .await
        .unwrap();
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
        let redeemed = U256::from(1_000_000_000_000_000_000u64);
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, redeemed).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        let redemption_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[redeemed]).await;

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
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error,
                BurnExcessEngineError::UnresolvedIssuerShareTransfer {
                    tx_hash,
                    ..
                } if tx_hash == redemption_tx
            ),
            "the redemption's shares are still in the issuer wallet: \
             {error:?}"
        );

        // Stand-in for the detected redemption's burn, which the redemption
        // flow runs once Alpaca journals it: the issuer wallet no longer
        // holds the redeemed shares.
        let issuer_provider = issuer_provider(&evm).await;
        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), redeemed)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        record_redemption_burn_completed(&pool, redemption_tx).await;

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store
                .load(&BurnExcessId::new(fixture.deposit_tx))
                .await
                .unwrap()
                .unwrap(),
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
    /// (redeeming the funding Transfer through Alpaca), and must show the
    /// operator what it counted. Once the burn completes, the held redemption
    /// is detected with its anchored AP attribution even if the wallet is
    /// unlinked before the next poll; the funding Transfer never is.
    #[traced_test]
    #[tokio::test]
    async fn a_counted_held_redemption_survives_wallet_unlink() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        let redemption_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[shares]).await;

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

        let outcome = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let BurnExcessOutcome::Plan(view) = outcome else {
            panic!("expected the executed plan, got {outcome:?}");
        };
        assert_eq!(
            view.held_transfers
                .iter()
                .map(|held| held.tx_hash)
                .collect::<Vec<_>>(),
            vec![redemption_tx],
            "the plan must name the Transfer the balance gate counted"
        );
        assert!(
            view.precondition
                .as_deref()
                .is_some_and(|text| text.contains("1 other Transfer(s)")),
            "{:?}",
            view.precondition
        );
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store
                .load(&BurnExcessId::new(fixture.deposit_tx))
                .await
                .unwrap()
                .unwrap(),
            BurnExcess::Completed { path: BurnExcessPath::External, .. }
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Counted same-shape Transfers held with the funding Transfer",
                &fixture.deposit_tx.to_string(),
                "held_transfers=1",
            ]
        ));

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Anchored held-redemption account attribution",
                &fixture.deposit_tx.to_string(),
                "held_redemptions=1",
            ]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Released the funding expectation of a terminal stream",
                &fixture.deposit_tx.to_string(),
                "Completed",
            ]
        ));
        sqlx::query("DELETE FROM account_view").execute(&pool).await.unwrap();

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
    #[tokio::test]
    async fn held_redemption_cannot_be_hidden_by_offsetting_outbound_shares() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        transfer_to_issuer(&evm, &fixture.recipient, &[shares]).await;
        let poller = transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await;
        poller.poll_once().await.unwrap();
        let issuer_provider = issuer_provider(&evm).await;
        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), shares)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let mut request =
            fixture.request(BurnExcessMode::External, Some(funding_tx));
        request.execute = false;
        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            request,
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            BurnExcessEngineError::Proof(
                BurnExcessProofError::IssuerShareBalanceNotExact {
                    held_transfers: 1,
                    ..
                }
            )
        ));
    }

    #[tokio::test]
    async fn sign_boundary_without_checkpoint_refuses_offsetting_held_set_change()
     {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        let request =
            fixture.request(BurnExcessMode::External, Some(funding_tx));
        let store = burn_excess_store(pool.clone()).await.unwrap();
        let aggregate_id = BurnExcessId::new(fixture.deposit_tx);
        let state = store.load(&aggregate_id).await.unwrap();
        let plan = prove_plan(
            &pool,
            &provider,
            evm.wallet_address,
            &request,
            state.as_ref(),
            BurnExcessPath::External,
        )
        .await
        .unwrap();
        assert!(plan.held_redemptions.is_empty());

        transfer_to_issuer(&evm, &fixture.recipient, &[shares]).await;
        let issuer_provider = issuer_provider(&evm).await;
        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), shares)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let error = require_issuer_balances(&pool, &provider, &service, &plan)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BurnExcessEngineError::HeldRedemptionsChanged {
                proven,
                current,
            } if proven.is_empty() && current.len() == 1
        ));
    }

    /// Records the expectation, funds the stream, batches `amounts` from the
    /// recipient to the issuer in one transaction, polls once, then runs
    /// `external`. Returns the stream, the batch transaction, and the refusal.
    async fn run_after_a_batched_transfer(
        pool: &Pool<Sqlite>,
        amounts: &[U256],
    ) -> (LiveHoldFixture, B256, BurnExcessEngineError) {
        let (evm, service, provider, _) = prepared_evm().await;
        let own_shares =
            amounts.iter().fold(U256::ZERO, |total, amount| total + *amount);
        let fixture =
            live_hold_fixture(pool, &evm, &service, &provider, own_shares)
                .await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        let batched_tx =
            transfer_to_issuer(&evm, &fixture.recipient, amounts).await;
        transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await
        .poll_once()
        .await
        .unwrap();

        let error = run_burn_excess(
            pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        (fixture, batched_tx, error)
    }

    fn is_shared_transaction_refusal(
        error: &BurnExcessEngineError,
        batched_tx: B256,
    ) -> bool {
        matches!(
            error,
            BurnExcessEngineError::Proof(
                BurnExcessProofError::HeldTransfersShareTransaction { tx_hash }
            ) if *tx_hash == batched_tx
        )
    }

    /// A Redemption is keyed by its transaction, so two held same-shape logs
    /// in one transaction could only ever be redeemed once. Counting both
    /// would burn the excess and strand the other's shares; `external` must
    /// refuse before anything is recorded.
    #[tokio::test]
    async fn two_held_redemptions_in_one_transaction_refuse_the_burn() {
        let pool = pool().await;
        let shares = excess_shares();
        let (fixture, batched_tx, error) =
            run_after_a_batched_transfer(&pool, &[shares, shares]).await;

        assert!(
            is_shared_transaction_refusal(&error, batched_tx),
            "got: {error:?}"
        );
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store
                .load(&BurnExcessId::new(fixture.deposit_tx))
                .await
                .unwrap()
                .unwrap(),
            BurnExcess::AwaitingFunding { .. }
        ));
    }

    /// Identical funding-shape logs in the funding transaction itself are
    /// interchangeable: the lowest is the funding log, and the other is a held
    /// redemption counted toward the balance. The run must complete rather
    /// than refuse the transaction as ambiguous, which left only `--close`.
    #[tokio::test]
    async fn a_funding_transaction_with_two_identical_transfers_completes() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let funding_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[shares, shares])
                .await;
        let poller = transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await;
        poller.poll_once().await.unwrap();

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
        poller.poll_once().await.unwrap();

        assert_eq!(
            redemption_aggregate_ids(&pool).await,
            vec![IssuerRedemptionRequestId::new(funding_tx).to_string()],
            "the second identical log is redeemed; the funding log is not"
        );
    }

    /// The reason the expectation outlives the exclusion: a run that recorded
    /// the exclusion and then failed before signing resumes from
    /// `FundingExcluded`, and that resume must still find the same-shape
    /// redemption held, or it would refuse for a balance it cannot explain
    /// while the redemption, detected and journaled, waits behind this
    /// stream's unresolved intent.
    #[tokio::test]
    async fn a_funding_excluded_resume_still_counts_the_held_redemption() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        let redemption_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[shares]).await;
        let poller = transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await;
        poller.poll_once().await.unwrap();

        // The exclusion is recorded, then signing fails.
        run_burn_excess(
            &pool,
            &MockVaultService::new_prepare_tx_failure()
                .with_share_balance(shares * U256::from(2u64)),
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            store
                .load(&BurnExcessId::new(fixture.deposit_tx))
                .await
                .unwrap()
                .unwrap(),
            BurnExcess::FundingExcluded { .. }
        ));
        poller.poll_once().await.unwrap();
        assert!(
            redemption_aggregate_ids(&pool).await.is_empty(),
            "the exclusion must not release the same-shape redemption"
        );

        let outcome = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
        let BurnExcessOutcome::Plan(view) = outcome else {
            panic!("expected the executed plan, got {outcome:?}");
        };
        assert_eq!(
            view.held_transfers
                .iter()
                .map(|held| held.tx_hash)
                .collect::<Vec<_>>(),
            vec![redemption_tx]
        );

        poller.poll_once().await.unwrap();
        assert_eq!(
            redemption_aggregate_ids(&pool).await,
            vec![IssuerRedemptionRequestId::new(redemption_tx).to_string()]
        );
    }

    /// A permanent funding exclusion arbitrates only burn-excess streams until
    /// bytes are signed. A different-shape redemption detected afterward must
    /// still be able to reserve the signer, burn its shares, and let the
    /// `FundingExcluded` stream reprove and finish.
    #[traced_test]
    #[tokio::test]
    async fn funding_excluded_yields_to_a_later_redemption_burn() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let redemption_shares = U256::from(1_000_000_000_000_000_000u64);
        let fixture = live_hold_fixture(
            &pool,
            &evm,
            &service,
            &provider,
            redemption_shares,
        )
        .await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;

        run_burn_excess(
            &pool,
            &MockVaultService::new_prepare_tx_failure()
                .with_share_balance(excess_shares()),
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        let burn_excess_store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(matches!(
            burn_excess_store
                .load(&BurnExcessId::new(fixture.deposit_tx))
                .await
                .unwrap(),
            Some(BurnExcess::FundingExcluded { .. })
        ));
        assert!(
            !excess_reservation_exists(&pool).await,
            "an unsigned exclusion must not reserve the signer nonce"
        );

        let redemption_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[redemption_shares])
                .await;
        transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await
        .poll_once()
        .await
        .unwrap();
        assert_eq!(
            redemption_aggregate_ids(&pool).await,
            vec![IssuerRedemptionRequestId::new(redemption_tx).to_string()]
        );

        let redemption_id =
            IssuerRedemptionRequestId::new(redemption_tx).to_string();

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
                'RedemptionEvent::BurnIntended',
                '1.0',
                '{}',
                '{}'
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
            ",
        )
        .bind(&redemption_id)
        .bind(&redemption_id)
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            crate::redemption::has_unresolved_signer_intent(
                &pool,
                Network::Base,
                None,
            )
            .await
            .unwrap()
        );

        let issuer_provider = issuer_provider(&evm).await;
        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), redemption_shares)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        record_redemption_burn_completed(&pool, redemption_tx).await;

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
        assert!(matches!(
            burn_excess_store
                .load(&BurnExcessId::new(fixture.deposit_tx))
                .await
                .unwrap(),
            Some(BurnExcess::Completed { path: BurnExcessPath::External, .. })
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Recorded funding exclusion", &fixture.deposit_tx.to_string()]
        ));
    }

    /// A held same-shape log batched with another Transfer that the poller
    /// detected shares that Redemption's transaction key, so it could never
    /// be redeemed on its own; `external` must refuse rather than leave it
    /// uncounted and the balance unexplained.
    #[tokio::test]
    async fn a_held_transfer_batched_with_a_detected_one_refuses_the_burn() {
        let pool = pool().await;
        let other = U256::from(1_000_000_000_000_000_000u64);
        let (_, batched_tx, error) =
            run_after_a_batched_transfer(&pool, &[excess_shares(), other])
                .await;

        assert_eq!(
            redemption_aggregate_ids(&pool).await,
            vec![IssuerRedemptionRequestId::new(batched_tx).to_string()],
            "the other-shape log in the batch is detected"
        );
        assert!(
            is_shared_transaction_refusal(&error, batched_tx),
            "got: {error:?}"
        );
    }

    /// The exclusion insert keeps an existing row's owner, so a stream proving
    /// a funding log another deposit already excluded would burn without ever
    /// owning its exclusion, and its expectation could never be released.
    #[tokio::test]
    async fn external_refuses_a_funding_log_another_deposit_excluded() {
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
        let funding = FundingTransferId {
            network: Network::Base,
            vault: evm.vault_address,
            tx_hash: funding_tx,
            log_index: funding_log_index,
            from: recipient,
            to: evm.wallet_address,
            amount: excess_shares(),
        };
        record_funding_exclusion(&pool, &funding, B256::random(), Utc::now())
            .await
            .unwrap();

        let error = run_burn_excess(
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
        .unwrap_err();

        assert!(
            matches!(
                error,
                BurnExcessEngineError::Proof(
                    BurnExcessProofError::FundingLogExcludedForAnotherDeposit { .. }
                )
            ),
            "got: {error:?}"
        );
        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none()
        );
    }

    /// Two same-shape streams funded in one batched transaction: once the
    /// first claimed the lowest log, the second must take the next one rather
    /// than refuse a funding transaction it can never name otherwise.
    #[tokio::test]
    async fn a_batched_funding_log_claimed_by_another_stream_is_skipped() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let funding_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[shares, shares])
                .await;
        let receipt = provider
            .get_transaction_receipt(funding_tx)
            .await
            .unwrap()
            .unwrap();
        let log_indexes: Vec<u64> = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Transfer>().is_ok()
            })
            .filter_map(|log| log.log_index)
            .collect();
        let [claimed_index, own_index] = log_indexes[..] else {
            panic!("expected two Transfer logs, got {log_indexes:?}");
        };

        // Another stream already excluded the lower log and burned its
        // excess, so only this stream's excess is left in the issuer wallet.
        let claimed = FundingTransferId {
            network: Network::Base,
            vault: evm.vault_address,
            tx_hash: funding_tx,
            log_index: claimed_index,
            from: fixture.recipient.address(),
            to: evm.wallet_address,
            amount: shares,
        };
        let claimed_deposit = B256::random();
        record_funding_exclusion(&pool, &claimed, claimed_deposit, Utc::now())
            .await
            .unwrap();
        let completed = BurnExcessEvent::ExcessBurnCompleted {
            burn_tx_hash: B256::random(),
            block_number: 1,
            completed_at: Utc::now(),
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
            VALUES ('BurnExcess', ?, 1, ?, '1.0', ?, '{}')
            ",
        )
        .bind(BurnExcessId::new(claimed_deposit).to_string())
        .bind(completed.event_type())
        .bind(serde_json::to_string(&completed).unwrap())
        .execute(&pool)
        .await
        .unwrap();
        let issuer_provider = issuer_provider(&evm).await;
        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), shares)
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
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let store = burn_excess_store(pool.clone()).await.unwrap();
        let state = store
            .load(&BurnExcessId::new(fixture.deposit_tx))
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                &state,
                BurnExcess::Completed {
                    funding_log_id: Some(funding),
                    ..
                } if funding.log_index == own_index
            ),
            "{state:?}"
        );
    }

    /// A same-shape Transfer the poller detected as a redemption before the
    /// expectation existed is the only inbound Transfer in its transaction:
    /// its Redemption is its own, its shares leave through that redemption's
    /// burn, and it must not be mistaken for a held log sharing a transaction.
    #[tokio::test]
    async fn a_same_shape_transfer_already_redeemed_on_its_own_is_not_held() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let redeemed_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[shares]).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await
        .poll_once()
        .await
        .unwrap();

        // As if the poller had detected it before the expectation.
        let detected = RedemptionEvent::Detected {
            issuer_request_id: IssuerRedemptionRequestId::new(redeemed_tx),
            underlying: UnderlyingSymbol::new("PTY").unwrap(),
            token: TokenSymbol::new("tPTY"),
            network: Network::Base,
            wallet: fixture.recipient.address(),
            quantity: Quantity::from_u256_with_18_decimals(shares).unwrap(),
            tx_hash: redeemed_tx,
            block_number: 0,
            detected_at: Utc::now(),
            burn_mode: VaultMode::VaultDirect,
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
            VALUES (
                'Redemption',
                ?,
                1,
                'RedemptionEvent::Detected',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(IssuerRedemptionRequestId::new(redeemed_tx).to_string())
        .bind(serde_json::to_string(&detected).unwrap())
        .execute(&pool)
        .await
        .unwrap();

        // Until that redemption burns, its shares are unexplained surplus:
        // the run must refuse as a balance to wait out, not as an incident.
        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error,
                BurnExcessEngineError::UnresolvedIssuerShareTransfer {
                    tx_hash,
                    ..
                } if tx_hash == redeemed_tx
            ),
            "got: {error:?}"
        );

        // The redemption burns its shares; the run then completes.
        let issuer_provider = issuer_provider(&evm).await;
        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), shares)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        record_redemption_burn_completed(&pool, redeemed_tx).await;

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap();
    }

    /// Net balance alone cannot distinguish an outbound loss from a different
    /// AP's inbound redemption of the same amount. The run refuses until the
    /// operator names that exact inflow; the signed intent then persists the
    /// acknowledgement for audit.
    #[tokio::test]
    async fn unrelated_inflow_requires_an_audited_acknowledgement() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let own_shares = shares.checked_mul(U256::from(2u64)).unwrap();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, own_shares)
                .await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;

        let other_sender = PrivateKeySigner::random();
        link_ap_wallet(&pool, other_sender.address()).await;
        let issuer_provider = issuer_provider(&evm).await;
        issuer_provider
            .send_transaction(
                TransactionRequest::default()
                    .to(other_sender.address())
                    .value(U256::from(10u64).pow(U256::from(18u64))),
            )
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let recipient_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(fixture.recipient.clone()))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        OffchainAssetReceiptVault::new(evm.vault_address, &recipient_provider)
            .transfer(other_sender.address(), shares)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let unrelated_tx =
            transfer_to_issuer(&evm, &other_sender, &[shares]).await;

        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), shares)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        assert_eq!(
            service
                .get_share_balance(evm.vault_address, evm.wallet_address)
                .await
                .unwrap(),
            shares,
            "the unrelated inbound exactly masks the outbound"
        );

        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            BurnExcessEngineError::UnresolvedIssuerShareTransfer {
                tx_hash,
                ..
            } if tx_hash == unrelated_tx
        ));
        let unrelated_receipt = provider
            .get_transaction_receipt(unrelated_tx)
            .await
            .unwrap()
            .unwrap();
        let log_index = unrelated_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                let transfer = log
                    .log_decode::<OffchainAssetReceiptVault::Transfer>()
                    .ok()?;
                (transfer.data().to == evm.wallet_address)
                    .then_some(log.log_index)
                    .flatten()
            })
            .unwrap();
        let unknown_tx_hash = B256::random();
        let mut unknown_acknowledgement =
            fixture.request(BurnExcessMode::External, Some(funding_tx));
        unknown_acknowledgement.acknowledged_inflows = vec![
            AcknowledgedInboundTransfer { tx_hash: unrelated_tx, log_index },
            AcknowledgedInboundTransfer { tx_hash: unknown_tx_hash, log_index },
        ];
        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            unknown_acknowledgement,
            |_| Ok(true),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            BurnExcessEngineError::AcknowledgedInboundNotFound {
                tx_hash,
                log_index: found_log_index,
            } if tx_hash == unknown_tx_hash && found_log_index == log_index
        ));

        let mut acknowledged_request =
            fixture.request(BurnExcessMode::External, Some(funding_tx));
        acknowledged_request.acknowledged_inflows =
            vec![AcknowledgedInboundTransfer {
                tx_hash: unrelated_tx,
                log_index,
            }];

        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            acknowledged_request,
            |_| Ok(true),
        )
        .await
        .unwrap();
        let (payload, event_version): (String, String) = sqlx::query_as(
            "
            SELECT payload, event_version
            FROM events
            WHERE aggregate_type = 'BurnExcess'
              AND aggregate_id = ?
              AND event_type = ?
            ",
        )
        .bind(fixture.deposit_tx.to_string())
        .bind(BurnExcessEvent::EXCESS_BURN_INTENDED)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_version, "2.0");
        let BurnExcessEvent::ExcessBurnIntended {
            acknowledged_inflows, ..
        } = serde_json::from_str(&payload).unwrap()
        else {
            panic!("expected ExcessBurnIntended")
        };
        assert_eq!(
            acknowledged_inflows,
            vec![AcknowledgedInboundTransfer {
                tx_hash: unrelated_tx,
                log_index
            }]
        );
    }

    /// The poller redeems only Transfers from a linked AP wallet. Held
    /// Transfers from a sender that is not one would be skipped once released
    /// and their shares stranded, so they must not count toward the balance.
    #[tokio::test]
    async fn held_transfers_from_an_unlinked_sender_refuse_the_burn() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        transfer_to_issuer(&evm, &fixture.recipient, &[shares]).await;
        transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await
        .poll_once()
        .await
        .unwrap();
        // The recipient is no longer whitelisted on any linked account.
        sqlx::query("DELETE FROM account_view").execute(&pool).await.unwrap();

        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                BurnExcessEngineError::Proof(
                    BurnExcessProofError::HeldTransferSenderNotLinked {
                        wallet
                    }
                ) if wallet == fixture.recipient.address()
            ),
            "got: {error:?}"
        );
    }

    /// A Redemption that exists for a held log's transaction but was detected
    /// from another Transfer (whose sender or vault the poller no longer
    /// treats as redeemable today) is not the held log's own: it can never be
    /// redeemed on its own, so the run must refuse as an incident.
    #[tokio::test]
    async fn a_redemption_detected_from_another_transfer_is_not_the_held_logs()
    {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let held_tx =
            transfer_to_issuer(&evm, &fixture.recipient, &[shares]).await;
        let (funding_tx, _) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        transfer_poller_for_tests(
            Network::Base,
            provider.clone(),
            evm.wallet_address,
            0,
            pool.clone(),
        )
        .await
        .poll_once()
        .await
        .unwrap();
        let foreign = RedemptionEvent::Detected {
            issuer_request_id: IssuerRedemptionRequestId::new(held_tx),
            underlying: UnderlyingSymbol::new("PTY").unwrap(),
            token: TokenSymbol::new("tPTY"),
            network: Network::Base,
            wallet: Address::random(),
            quantity: Quantity::from_u256_with_18_decimals(U256::from(
                1_000_000_000_000_000_000u64,
            ))
            .unwrap(),
            tx_hash: held_tx,
            block_number: 0,
            detected_at: Utc::now(),
            burn_mode: VaultMode::VaultDirect,
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
            VALUES (
                'Redemption',
                ?,
                1,
                'RedemptionEvent::Detected',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(IssuerRedemptionRequestId::new(held_tx).to_string())
        .bind(serde_json::to_string(&foreign).unwrap())
        .execute(&pool)
        .await
        .unwrap();

        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(
            is_shared_transaction_refusal(&error, held_tx),
            "got: {error:?}"
        );
    }

    /// The funding log is claimed under the wallet lock: a stream that proved
    /// it, then waited at the lock while another stream recorded the same
    /// log's exclusion, must refuse without appending an exclusion it would
    /// not own.
    #[tokio::test]
    async fn a_funding_log_claimed_while_waiting_for_the_wallet_refuses() {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let shares = excess_shares();
        let fixture =
            live_hold_fixture(&pool, &evm, &service, &provider, shares).await;
        let (funding_tx, funding_log_index) =
            send_external_funding(&evm, fixture.recipient.clone()).await;
        let mock = Arc::new(
            MockVaultService::new_wallet_lock_blocked()
                .with_share_balance(shares),
        );

        let parked = spawn_burn(
            &pool,
            &mock,
            &provider,
            evm.wallet_address,
            fixture.request(BurnExcessMode::External, Some(funding_tx)),
        );
        mock.wait_for_wallet_lock_attempt().await;

        // Another stream records its exclusion of the same funding log.
        let other_deposit = B256::random();
        let funding = FundingTransferId {
            network: Network::Base,
            vault: evm.vault_address,
            tx_hash: funding_tx,
            log_index: funding_log_index,
            from: fixture.recipient.address(),
            to: evm.wallet_address,
            amount: shares,
        };
        let store = burn_excess_store(pool.clone()).await.unwrap();
        let other_bind = ExcessBurnBind {
            deposit_tx_hash: other_deposit,
            ..test_bind(&IssuerMintRequestId::random(), other_deposit)
        };
        store
            .send(
                &BurnExcessId::new(other_deposit),
                BurnExcessCommand::RecordFundingExclusion {
                    bind: other_bind,
                    funding_log_id: funding,
                    reason: "other stream".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        mock.release_wallet_lock();

        let error = parked.await.unwrap().unwrap_err();
        assert!(
            matches!(
                error,
                BurnExcessEngineError::UnresolvedExcessBurnIntent
                    | BurnExcessEngineError::Proof(
                        BurnExcessProofError::FundingLogExcludedForAnotherDeposit {
                            ..
                        }
                    )
            ),
            "got: {error:?}"
        );
        assert!(matches!(
            store
                .load(&BurnExcessId::new(fixture.deposit_tx))
                .await
                .unwrap()
                .unwrap(),
            BurnExcess::AwaitingFunding { .. }
        ));
    }

    fn transfer_log(
        vault: Address,
        from: Address,
        to: Address,
        value: U256,
        tx_hash: B256,
        log_index: u64,
    ) -> Log {
        let data = OffchainAssetReceiptVault::Transfer { from, to, value }
            .encode_log_data();
        Log {
            inner: alloy::primitives::Log { address: vault, data },
            transaction_hash: Some(tx_hash),
            log_index: Some(log_index),
            ..Log::default()
        }
    }

    /// `Detected` historically stores sender and amount but not vault/log
    /// identity. Two equal-shape logs in one receipt are therefore ambiguous
    /// even if one vault is no longer configured; the held log must not be
    /// mistaken for the existing transaction-keyed Redemption.
    #[tokio::test]
    async fn equal_shape_cross_vault_logs_make_redemption_ownership_ambiguous()
    {
        let pool = pool().await;
        let tx_hash = B256::random();
        let issuer = Address::random();
        let sender = Address::random();
        let amount = excess_shares();
        let old_vault = Address::random();
        let held_vault = Address::random();
        let held = FundingTransferId {
            network: Network::Base,
            vault: held_vault,
            tx_hash,
            log_index: 1,
            from: sender,
            to: issuer,
            amount,
        };
        let detected = RedemptionEvent::Detected {
            issuer_request_id: IssuerRedemptionRequestId::new(tx_hash),
            underlying: UnderlyingSymbol::new("PTY").unwrap(),
            token: TokenSymbol::new("tPTY"),
            network: Network::Base,
            wallet: sender,
            quantity: Quantity::from_u256_with_18_decimals(amount).unwrap(),
            tx_hash,
            block_number: 1,
            detected_at: Utc::now(),
            burn_mode: VaultMode::VaultDirect,
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
            VALUES (
                'Redemption',
                ?,
                1,
                'RedemptionEvent::Detected',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(IssuerRedemptionRequestId::new(tx_hash).to_string())
        .bind(serde_json::to_string(&detected).unwrap())
        .execute(&pool)
        .await
        .unwrap();
        let logs = vec![
            transfer_log(old_vault, sender, issuer, amount, tx_hash, 0),
            transfer_log(held_vault, sender, issuer, amount, tx_hash, 1),
        ];

        assert!(!redemption_is_own(&pool, &logs, &held).await.unwrap());
    }

    /// A Redemption is keyed by the transaction alone, so every non-mint
    /// inbound Transfer competes even when its vault is not configured or its
    /// sender is not linked yet. Both admission inputs may change before the
    /// held log is released.
    #[tokio::test]
    async fn any_other_inbound_transfer_takes_the_redemption_key() {
        let pool = pool().await;
        let held_vault = Address::random();
        let other_vault = Address::random();
        let issuer = Address::random();
        let sender = Address::random();
        let tx_hash = B256::random();
        let shares = excess_shares();
        let held = FundingTransferId {
            network: Network::Base,
            vault: held_vault,
            tx_hash,
            log_index: 0,
            from: sender,
            to: issuer,
            amount: shares,
        };
        let funding =
            FundingTransferId { tx_hash: B256::random(), ..held.clone() };
        let logs = vec![
            transfer_log(held_vault, sender, issuer, shares, tx_hash, 0),
            transfer_log(
                other_vault,
                Address::random(),
                issuer,
                U256::from(1u64),
                tx_hash,
                1,
            ),
        ];

        assert!(
            competes_for_redemption_key(&pool, &logs, &held, Some(&funding))
                .await
                .unwrap()
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

    /// Every route request opens the store. The expectation rebuild deletes
    /// rows before re-inserting them, so run beside a request it could drop
    /// an expectation that request had just written; it runs only at startup
    /// before the pollers spawn. Opening the store must leave existing
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
            chain_id: ANVIL_CHAIN_ID,
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
            chain_id: ANVIL_CHAIN_ID,
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
            chain_id: ANVIL_CHAIN_ID,
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
        // and the stream's own expectation still in place.
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
            "a completed stream releases its expectation"
        );
        let state =
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap();
        assert!(matches!(
            state,
            BurnExcess::Completed { path: BurnExcessPath::External, .. }
        ));
    }

    /// Balances can move after proof but before signing. The irreversible
    /// boundary re-check must refuse the stale plan before preparing a burn.
    #[tokio::test]
    async fn balance_moving_after_plan_refuses_before_signing() {
        let pool = pool().await;
        let (evm, _real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;
        let request = request(
            BurnExcessMode::Internal,
            issuer_request_id,
            deposit_tx,
            receipt_id,
            None,
            true,
        );
        let mock = MockVaultService::new_success().with_share_balance(shares);
        let plan = prove_plan(
            &pool,
            &provider,
            evm.wallet_address,
            &request,
            None,
            BurnExcessPath::Internal,
        )
        .await
        .unwrap();

        let issuer_provider = issuer_provider(&evm).await;
        OffchainAssetReceiptVault::new(evm.vault_address, &issuer_provider)
            .transfer(Address::random(), U256::from(1u64))
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let error = require_issuer_balances(&pool, &provider, &mock, &plan)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                BurnExcessEngineError::Proof(
                    BurnExcessProofError::IssuerShareBalanceNotExact { .. }
                )
            ),
            "a balance that moved after proof must fail the re-check, got: \
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

    #[tokio::test]
    async fn durable_held_attribution_conflict_refuses_before_intent() {
        let pool = pool().await;
        let (evm, _real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;
        let request = request(
            BurnExcessMode::Internal,
            issuer_request_id,
            deposit_tx,
            receipt_id,
            None,
            true,
        );
        let service =
            MockVaultService::new_success().with_share_balance(shares);
        let mut plan = prove_plan(
            &pool,
            &provider,
            evm.wallet_address,
            &request,
            None,
            BurnExcessPath::Internal,
        )
        .await
        .unwrap();

        let indexed_deposit = B256::random();
        let mut indexed_bind = plan.bind.clone();
        indexed_bind.deposit_tx_hash = indexed_deposit;
        let indexed_funding = test_funding_log(&indexed_bind);
        let indexed = HeldTransferRedemption {
            transfer: FundingTransferId {
                tx_hash: B256::random(),
                log_index: 8,
                ..indexed_funding.clone()
            },
            block_number: 100,
            client_id: ClientId::new(),
            alpaca_account: AlpacaAccountNumber("first".into()),
            underlying: plan.underlying.clone(),
            token: TokenSymbol::new("tRKLB"),
            burn_mode: VaultMode::VaultDirect,
        };
        let anchor_service = MockVaultService::new_success();
        let anchor_sendable = anchor_service
            .prepare_burn_tx(&multi_burn_params_from_bind(
                &indexed_bind,
                &Bytes::new(),
                None,
            ))
            .await
            .unwrap();
        link_ap_wallet(&pool, indexed_bind.original_recipient).await;
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &BurnExcessId::new(indexed_deposit),
                BurnExcessCommand::ExpectFunding {
                    bind: indexed_bind.clone(),
                    reason: "first stream".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &BurnExcessId::new(indexed_deposit),
                BurnExcessCommand::RecordFundingExclusion {
                    bind: indexed_bind.clone(),
                    funding_log_id: indexed_funding.clone(),
                    reason: "first stream".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &BurnExcessId::new(indexed_deposit),
                BurnExcessCommand::IntendExcessBurn {
                    bind: indexed_bind,
                    path: BurnExcessPath::External,
                    funding_log_id: Some(indexed_funding),
                    reason: "first stream".into(),
                    incident_id: None,
                    sendable_tx: anchor_sendable,
                    held_redemptions: vec![indexed.clone()],
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();
        clear_held_redemption(&pool, &indexed.transfer).await.unwrap();

        let requested = HeldTransferRedemption {
            client_id: ClientId::new(),
            alpaca_account: AlpacaAccountNumber("second".into()),
            ..indexed
        };
        plan.held_redemptions = vec![requested];
        let aggregate_id = BurnExcessId::new(deposit_tx);
        let error = intend_submit_confirm(
            &MutationCtx {
                pool: &pool,
                vault_service: &service,
                provider: &provider,
                store: &store,
                aggregate_id: &aggregate_id,
                plan: &plan,
                request: &request,
            },
            service.lock_wallet().await,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            BurnExcessEngineError::FundingTransferIndex(
                FundingTransferIndexError::HeldAttributionConflict { .. }
            )
        ));
        assert_eq!(service.burn_preparation_call_count(), 0);
        assert!(store.load(&aggregate_id).await.unwrap().is_none());
        rebuild_funding_expectation_index(&pool).await.unwrap();
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
    async fn submitted_still_mineable_rebroadcasts_persisted_bytes() {
        let pool = pool().await;
        let (evm, _, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;
        let bind = ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash: deposit_tx,
            receipt_id,
            shares,
            original_recipient: evm.wallet_address,
            vault: evm.vault_address,
            network: Network::Base,
            chain_id: ANVIL_CHAIN_ID,
            issuer_wallet: evm.wallet_address,
        };
        let service = MockVaultService::new_success()
            .with_burn_tx_status(BurnTxStatus::StillMineable);
        let sendable = service
            .prepare_burn_tx(&multi_burn_params_from_bind(
                &bind,
                &Bytes::new(),
                None,
            ))
            .await
            .unwrap();
        let aggregate_id = BurnExcessId::new(deposit_tx);
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &aggregate_id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "resume submitted".into(),
                    incident_id: None,
                    sendable_tx: sendable.clone(),
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &aggregate_id,
                BurnExcessCommand::RecordExcessBurnSubmitted {
                    tx_id: sendable.hash.into(),
                    burn_tx_hash: sendable.hash,
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

        assert_eq!(service.submitted_burn_txs(), vec![sendable]);
        assert_eq!(service.burn_preparation_call_count(), 1);
        assert!(matches!(
            store.load(&aggregate_id).await.unwrap(),
            Some(BurnExcess::Completed { .. })
        ));
    }

    #[tokio::test]
    async fn signed_resume_uses_persisted_vault_after_listing_repoint() {
        let pool = pool().await;
        let (evm, _, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;
        let bind = ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash: deposit_tx,
            receipt_id,
            shares,
            original_recipient: evm.wallet_address,
            vault: evm.vault_address,
            network: Network::Base,
            chain_id: ANVIL_CHAIN_ID,
            issuer_wallet: evm.wallet_address,
        };
        let preparing_service = MockVaultService::new_success();
        let sendable = preparing_service
            .prepare_burn_tx(&multi_burn_params_from_bind(
                &bind,
                &Bytes::new(),
                None,
            ))
            .await
            .unwrap();
        let service = preparing_service
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
        let aggregate_id = BurnExcessId::new(deposit_tx);
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &aggregate_id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "resume old vault".into(),
                    incident_id: None,
                    sendable_tx: sendable,
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();
        seed_listing(&pool, Address::random()).await;

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

        assert_eq!(service.burn_classification_call_count(), 1);
        assert_eq!(service.burn_preparation_call_count(), 1);
        assert!(matches!(
            store.load(&aggregate_id).await.unwrap(),
            Some(BurnExcess::Completed { bind: completed_bind, .. })
                if completed_bind.vault == evm.vault_address
        ));
    }

    #[tokio::test]
    async fn legacy_intended_mined_repairs_anchor_and_completes() {
        assert_legacy_mined_resume(false).await;
    }

    #[tokio::test]
    async fn legacy_submitted_mined_repairs_anchor_and_completes() {
        assert_legacy_mined_resume(true).await;
    }

    /// A pre-anchor signed burn may land before either the Submitted or
    /// anchor event is persisted. Resume must repair the compatibility anchor,
    /// classify the signed transaction, and complete without live balances.
    async fn assert_legacy_mined_resume(record_submitted: bool) {
        let pool = pool().await;
        let (evm, real_service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;

        let (receipt_id, shares, _, deposit_tx) =
            mint_with_info(&evm, evm.wallet_address, &issuer_request_id).await;

        let bind = ExcessBurnBind {
            issuer_request_id: issuer_request_id.clone(),
            deposit_tx_hash: deposit_tx,
            receipt_id,
            shares,
            original_recipient: evm.wallet_address,
            vault: evm.vault_address,
            network: Network::Base,
            chain_id: ANVIL_CHAIN_ID,
            issuer_wallet: evm.wallet_address,
        };
        let params = multi_burn_params_from_bind(&bind, &Bytes::new(), None);
        let sendable = real_service.prepare_burn_tx(&params).await.unwrap();
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
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();
        let submitted =
            real_service.submit_burn(params, sendable.clone()).await.unwrap();
        if record_submitted {
            store
                .send(
                    &BurnExcessId::new(deposit_tx),
                    BurnExcessCommand::RecordExcessBurnSubmitted {
                        tx_id: submitted.tx_id.clone(),
                        burn_tx_hash: sendable.hash,
                    },
                )
                .await
                .unwrap();
        }
        real_service
            .confirm_burn(&submitted.tx_id, sendable.dust_shares)
            .await
            .unwrap();
        let aggregate_id = BurnExcessId::new(deposit_tx).to_string();
        sqlx::query(
            "
            DELETE FROM events
            WHERE aggregate_type = 'BurnExcess'
              AND aggregate_id = ?
              AND event_type = ?
            ",
        )
        .bind(&aggregate_id)
        .bind(BurnExcessEvent::HELD_REDEMPTIONS_ANCHORED)
        .execute(&pool)
        .await
        .unwrap();
        if record_submitted {
            sqlx::query(
                "
                UPDATE events
                SET sequence = 2
                WHERE aggregate_type = 'BurnExcess'
                  AND aggregate_id = ?
                  AND event_type = ?
                ",
            )
            .bind(&aggregate_id)
            .bind(BurnExcessEvent::EXCESS_BURN_SUBMITTED)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "
            DELETE FROM snapshots
            WHERE aggregate_type = 'BurnExcess'
              AND aggregate_id = ?
            ",
        )
        .bind(&aggregate_id)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            real_service
                .get_share_balance(evm.vault_address, evm.wallet_address)
                .await
                .unwrap(),
            U256::ZERO,
            "the landed burn took the excess out of the issuer wallet"
        );

        // The classification and confirmation the resume runs, against the
        // burn that landed above.
        let mock = MockVaultService::new_success()
            .with_share_balance(U256::ZERO)
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
            "legacy mined resume must not re-prepare"
        );
        assert_eq!(mock.burn_classification_call_count(), 1);

        let state =
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().unwrap();
        assert!(matches!(state, BurnExcess::Completed { .. }));
        let anchored = sqlx::query_scalar::<_, bool>(
            "
            SELECT EXISTS (
                SELECT 1
                FROM events
                WHERE aggregate_type = 'BurnExcess'
                  AND aggregate_id = ?
                  AND event_type = ?
            )
            ",
        )
        .bind(aggregate_id)
        .bind(BurnExcessEvent::HELD_REDEMPTIONS_ANCHORED)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(anchored, "resume must persist the compatibility anchor");
    }

    /// Unsigned `--close` never proves a plan or classifies a transaction.
    /// Constructing the provider without a live node keeps those tests off
    /// Anvil and makes their no-chain-I/O property structural.
    fn head_provider(head: u64) -> impl Provider {
        let asserter = Asserter::new();
        asserter.push_success(&U256::from(head));
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

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
            chain_id: ANVIL_CHAIN_ID,
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

    async fn excess_reservation_exists(pool: &Pool<Sqlite>) -> bool {
        sqlx::query_scalar::<_, bool>(
            "
            SELECT EXISTS (
                SELECT 1
                FROM active_signer_intents
                WHERE aggregate_type = 'BurnExcess'
            )
            ",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Seeds an abandoned Path B recovery: the exclusion is permanent, but no
    /// transaction is signed. This is the state `--close` exists to release.
    async fn seed_funding_excluded(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        deposit_tx: B256,
    ) -> Arc<Store<BurnExcess>> {
        let bind = test_bind(issuer_request_id, deposit_tx);
        let underlying = seed_listing(pool, bind.vault).await;
        link_ap_wallet(pool, bind.original_recipient).await;
        seed_mint_initiated(pool, issuer_request_id, &underlying).await;
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &BurnExcessId::new(deposit_tx),
                BurnExcessCommand::ExpectFunding {
                    bind: bind.clone(),
                    reason: "abandoned path b".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
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

    #[tokio::test]
    async fn funding_expectation_event_rejects_a_vault_repoint_race() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let bind = test_bind(&issuer_request_id, B256::random());
        let underlying = seed_listing(&pool, bind.vault).await;
        link_ap_wallet(&pool, bind.original_recipient).await;
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (asset_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        asset_store
            .send(
                &AssetKey::new(underlying.clone(), Network::Base),
                TokenizedAssetCommand::Add {
                    underlying,
                    token: TokenSymbol::new("tPTY"),
                    network: Network::Base,
                    vault: Address::random(),
                },
            )
            .await
            .unwrap();
        let store = burn_excess_store(pool.clone()).await.unwrap();

        let result = store
            .send(
                &BurnExcessId::new(bind.deposit_tx_hash),
                BurnExcessCommand::ExpectFunding {
                    bind,
                    reason: "race".into(),
                    incident_id: None,
                },
            )
            .await;

        assert!(result.is_err());
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM burn_excess_expectation_guards",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn funding_expectation_event_rejects_an_unlink_race() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let bind = test_bind(&issuer_request_id, B256::random());
        let underlying = seed_listing(&pool, bind.vault).await;
        link_ap_wallet(&pool, bind.original_recipient).await;
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let (account_id, sequence): (String, i64) = sqlx::query_as(
            "
            SELECT aggregate_id, sequence
            FROM events
            WHERE aggregate_type = 'Account'
              AND event_type = 'AccountEvent::WalletWhitelisted'
            ",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let unlinked = AccountEvent::WalletUnwhitelisted {
            wallet: bind.original_recipient,
            unwhitelisted_at: Utc::now(),
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
            VALUES ('Account', ?, ?, ?, '1.0', ?, '{}')
            ",
        )
        .bind(account_id)
        .bind(sequence + 1)
        .bind(unlinked.event_type())
        .bind(serde_json::to_string(&unlinked).unwrap())
        .execute(&pool)
        .await
        .unwrap();
        let store = burn_excess_store(pool.clone()).await.unwrap();

        let result = store
            .send(
                &BurnExcessId::new(bind.deposit_tx_hash),
                BurnExcessCommand::ExpectFunding {
                    bind,
                    reason: "race".into(),
                    incident_id: None,
                },
            )
            .await;

        assert!(result.is_err());
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM burn_excess_expectation_guards",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
    }

    /// One vault can expose only one expected funding shape at a time. A
    /// second stream would otherwise make the poller hold both shapes while
    /// either stream refuses to proceed because the other is unresolved.
    #[tokio::test]
    async fn a_second_expectation_on_one_vault_is_refused() {
        let pool = pool().await;
        let first_id = IssuerMintRequestId::random();
        let second_id = IssuerMintRequestId::random();
        let first = test_bind(&first_id, B256::random());
        let second = ExcessBurnBind {
            shares: first.shares + U256::from(1u64),
            ..test_bind(&second_id, B256::random())
        };
        let underlying = seed_listing(&pool, first.vault).await;
        link_ap_wallet(&pool, first.original_recipient).await;
        seed_mint_initiated(&pool, &first_id, &underlying).await;
        seed_mint_initiated(&pool, &second_id, &underlying).await;
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &BurnExcessId::new(first.deposit_tx_hash),
                BurnExcessCommand::ExpectFunding {
                    bind: first,
                    reason: "first recovery".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();

        let result = store
            .send(
                &BurnExcessId::new(second.deposit_tx_hash),
                BurnExcessCommand::ExpectFunding {
                    bind: second,
                    reason: "second recovery".into(),
                    incident_id: None,
                },
            )
            .await;

        assert!(result.is_err());
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM burn_excess_expectation_guards",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn expect_funding_reports_another_expectation_for_a_different_shape()
    {
        let pool = pool().await;
        let (evm, service, provider, _) = prepared_evm().await;
        let underlying = seed_listing(&pool, evm.vault_address).await;
        let first = deposit_to_recipient(&pool, &evm, &underlying).await;
        let second = deposit_to_recipient(&pool, &evm, &underlying).await;
        link_ap_wallet_pair(
            &pool,
            first.recipient.address(),
            second.recipient.address(),
        )
        .await;
        let expectation = |deposit: &ExternalDeposit| BurnExcessRequest {
            poller_guard: PollerGuard::FundingExpected,
            ..request(
                BurnExcessMode::ExpectFunding,
                deposit.issuer_request_id.clone(),
                deposit.deposit_tx,
                deposit.receipt_id,
                None,
                true,
            )
        };
        run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            expectation(&first),
            |_| Ok(true),
        )
        .await
        .unwrap();

        let error = run_burn_excess(
            &pool,
            &service,
            &provider,
            evm.wallet_address,
            expectation(&second),
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
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Recorded funding expectation", &first.deposit_tx.to_string()]
        ));
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
        let expectation_exists: bool = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM burn_excess_funding_expectations
                WHERE deposit_tx_hash = ?
            )
            ",
        )
        .bind(deposit_tx.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!expectation_exists);
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
            !expected().await.unwrap(),
            "terminal release must repair the exclusion before opening catch-up"
        );
        assert!(
            is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                funding.tx_hash,
                funding.log_index,
            )
            .await
            .unwrap()
        );

        release_terminal_expectation(&pool, Some(&completed)).await.unwrap();
        assert!(!expected().await.unwrap());
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
        link_ap_wallet(&pool, evm.wallet_address).await;
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

    #[tokio::test]
    async fn close_dry_run_records_no_event_and_keeps_stream_open() {
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
            !excess_reservation_exists(&pool).await,
            "an unsigned recovery must not reserve the signer nonce"
        );
    }

    #[tokio::test]
    async fn close_execute_releases_the_unsigned_stream() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let store =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;
        let bind = test_bind(&issuer_request_id, deposit_tx);
        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();
        advance_transfer_poll_observed(&pool, bind.network, bind.vault, 250)
            .await
            .unwrap();
        assert!(
            !excess_reservation_exists(&pool).await,
            "an unsigned Path B recovery must not reserve the signer nonce"
        );
        let prompt = Mutex::new(String::new());

        let provider = head_provider(200);
        let execute_outcome = run_burn_excess(
            &pool,
            &MockVaultService::new_success(),
            &provider,
            test_bind(&issuer_request_id, deposit_tx).issuer_wallet,
            close_request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                U256::from(7u64),
                Some(B256::random()),
                true,
            ),
            |text| {
                text.clone_into(&mut prompt.lock());
                Ok(true)
            },
        )
        .await
        .unwrap();

        let execute_close = match execute_outcome {
            BurnExcessOutcome::Close(close) => close,
            other => panic!("expected a close outcome, got: {other:?}"),
        };
        assert!(!execute_close.dry_run);
        let prompt = prompt.into_inner();
        assert!(
            prompt.contains(
                "releases any other mined Transfer of the funding shape"
            ) && prompt.contains("no same-shape Transfer remains pending")
                && !prompt.contains("wallet gates only"),
            "closing an excluded live stream also ends its hold on same-shape \
             Transfers: {prompt}"
        );
        assert_eq!(execute_close.state, "Closed");

        assert!(matches!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap(),
            Some(BurnExcess::Closed { .. })
        ));
        assert!(
            !excess_reservation_exists(&pool).await,
            "closing an unsigned stream must not create a signer reservation"
        );
        let (released, release_through_block): (i64, i64) = sqlx::query_as(
            "
                SELECT released, release_through_block
                FROM burn_excess_funding_expectations
                WHERE deposit_tx_hash = ?
                ",
        )
        .bind(deposit_tx.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(released, 1);
        assert_eq!(release_through_block, 250);
    }

    #[tokio::test]
    async fn unsigned_expectation_atomically_blocks_vault_repoint() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let stream =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;
        let bind = test_bind(&issuer_request_id, deposit_tx);
        let (asset_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let underlying = UnderlyingSymbol::new("PTY").unwrap();

        let result = asset_store
            .send(
                &AssetKey::new(underlying.clone(), Network::Base),
                TokenizedAssetCommand::Add {
                    underlying,
                    token: TokenSymbol::new("tPTY"),
                    network: Network::Base,
                    vault: Address::random(),
                },
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            stream.load(&BurnExcessId::new(deposit_tx)).await.unwrap(),
            Some(BurnExcess::FundingExcluded { .. })
        ));
        assert!(!excess_reservation_exists(&pool).await);
        assert_eq!(
            find_vault(
                &pool,
                &UnderlyingSymbol::new("PTY").unwrap(),
                &Network::Base,
            )
            .await
            .unwrap(),
            Some(bind.vault)
        );
    }

    #[tokio::test]
    async fn finalized_dead_close_repairs_held_attribution_before_release() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let aggregate_id = BurnExcessId::new(deposit_tx);
        let bind = test_bind(&issuer_request_id, deposit_tx);
        let funding = test_funding_log(&bind);
        let held = HeldTransferRedemption {
            transfer: FundingTransferId {
                tx_hash: B256::random(),
                log_index: 8,
                ..funding.clone()
            },
            block_number: 100,
            client_id: ClientId::new(),
            alpaca_account: AlpacaAccountNumber("anchored".into()),
            underlying: UnderlyingSymbol::new("RKLB").unwrap(),
            token: TokenSymbol::new("tRKLB"),
            burn_mode: VaultMode::VaultDirect,
        };
        let service = MockVaultService::new_success()
            .with_burn_tx_status(BurnTxStatus::ProvablyDead);
        let sendable = service
            .prepare_burn_tx(&multi_burn_params_from_bind(
                &bind,
                &Bytes::new(),
                None,
            ))
            .await
            .unwrap();
        let underlying = seed_listing(&pool, bind.vault).await;
        link_ap_wallet(&pool, bind.original_recipient).await;
        seed_mint_initiated(&pool, &issuer_request_id, &underlying).await;
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &aggregate_id,
                BurnExcessCommand::ExpectFunding {
                    bind: bind.clone(),
                    reason: "dead transaction".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &aggregate_id,
                BurnExcessCommand::RecordFundingExclusion {
                    bind: bind.clone(),
                    funding_log_id: funding.clone(),
                    reason: "dead transaction".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &aggregate_id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: bind.clone(),
                    path: BurnExcessPath::External,
                    funding_log_id: Some(funding.clone()),
                    reason: "dead transaction".into(),
                    incident_id: None,
                    sendable_tx: sendable,
                    held_redemptions: vec![held.clone()],
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();
        sqlx::query(
            "
            DELETE FROM burn_excess_funding_exclusions
            WHERE network = ?
              AND vault = ?
              AND tx_hash = ?
              AND log_index = ?
            ",
        )
        .bind(funding.network.as_str())
        .bind(format!("{:#x}", funding.vault))
        .bind(funding.tx_hash.to_string())
        .bind(i64::try_from(funding.log_index).unwrap())
        .execute(&pool)
        .await
        .unwrap();
        clear_held_redemption(&pool, &held.transfer).await.unwrap();
        assert_eq!(
            classify_funding_transfer(&pool, &held.transfer).await.unwrap(),
            FundingTransferStatus::Expected
        );
        assert!(
            held_redemption_vaults(&pool, held.transfer.network)
                .await
                .unwrap()
                .is_empty()
        );

        let provider = head_provider(200);
        run_burn_excess(
            &pool,
            &service,
            &provider,
            bind.issuer_wallet,
            close_request(
                BurnExcessMode::External,
                issuer_request_id,
                deposit_tx,
                bind.receipt_id,
                Some(funding.tx_hash),
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

        assert_eq!(
            classify_funding_transfer(&pool, &held.transfer).await.unwrap(),
            FundingTransferStatus::Attributed(Box::new(held))
        );
        assert!(
            is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                funding.tx_hash,
                funding.log_index,
            )
            .await
            .unwrap(),
            "close must repair the exact funding exclusion before release"
        );
        assert!(matches!(
            store.load(&aggregate_id).await.unwrap(),
            Some(BurnExcess::Closed { .. })
        ));
    }

    #[tokio::test]
    async fn close_refuses_a_still_mineable_signed_intent() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let aggregate_id = BurnExcessId::new(deposit_tx);
        let bind = test_bind(&issuer_request_id, deposit_tx);
        let service = MockVaultService::new_success()
            .with_burn_tx_status(BurnTxStatus::StillMineable);
        let sendable = service
            .prepare_burn_tx(&multi_burn_params_from_bind(
                &bind,
                &Bytes::new(),
                None,
            ))
            .await
            .unwrap();
        let store = burn_excess_store(pool.clone()).await.unwrap();
        store
            .send(
                &aggregate_id,
                BurnExcessCommand::IntendExcessBurn {
                    bind: bind.clone(),
                    path: BurnExcessPath::Internal,
                    funding_log_id: None,
                    reason: "live transaction".into(),
                    incident_id: None,
                    sendable_tx: sendable,
                    held_redemptions: Vec::new(),
                    acknowledged_inflows: Vec::new(),
                },
            )
            .await
            .unwrap();

        let error = run_burn_excess(
            &pool,
            &service,
            &offline_provider(),
            bind.issuer_wallet,
            close_request(
                BurnExcessMode::Internal,
                issuer_request_id,
                deposit_tx,
                bind.receipt_id,
                None,
                true,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            BurnExcessEngineError::CloseRequiresDeadBurnIntent {
                status: BurnTxStatus::StillMineable
            }
        ));
        assert!(matches!(
            store.load(&aggregate_id).await.unwrap(),
            Some(BurnExcess::Intended { .. })
        ));
        assert!(excess_reservation_exists(&pool).await);
        assert_eq!(service.burn_classification_call_count(), 1);
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
                    proof: BurnExcessCloseProof::Unsigned,
                    release_through_block: Some(100),
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
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Released the funding expectation of a terminal stream",
                &deposit_tx.to_string(),
                "Closed",
            ]
        ));
    }
}
