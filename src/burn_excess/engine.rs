//! Orchestrates dual-path burn-excess recovery (D0.5).
//!
//! Dry-run proves and prints; `--execute` persists exclusion (Path B), then
//! signs/intends/submits/confirms the vault redeem and updates inventory.

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use chrono::{DateTime, Utc};
use event_sorcery::{Store, StoreBuilder};
use sqlx::{Pool, Sqlite};
use std::io;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::exclusion::{
    FundingExclusionReactor, is_excluded_funding_log,
    rebuild_funding_exclusion_index, record_funding_exclusion,
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
use crate::receipt_inventory::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand, Shares,
    load_inventory, send_receipt_inventory_command,
};
use crate::redemption::IssuerRedemptionRequestId;
use crate::tokenized_asset::view::find_vault;
use crate::tokenized_asset::{Network, UnderlyingSymbol};
use crate::vault::{
    BurnRequestOrigin, BurnTxStatus, MultiBurnEntry, MultiBurnParams,
    VaultService,
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

/// Builds the `BurnExcess` store with the funding-exclusion reactor attached.
///
/// Rebuilds the funding-exclusion SQL index from events first: custom reactors
/// on `Nil`-materialized aggregates are not catch_up'd by `StoreBuilder`.
pub(crate) async fn burn_excess_store(
    pool: Pool<Sqlite>,
) -> Result<Arc<Store<BurnExcess>>, BurnExcessEngineError> {
    crate::prepare_event_sourced_startup::<BurnExcess>(&pool).await?;
    rebuild_funding_exclusion_index(&pool).await?;
    Ok(StoreBuilder::<BurnExcess>::new(pool.clone())
        .with(Arc::new(FundingExclusionReactor::new(pool)))
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
) -> Result<(), BurnExcessEngineError> {
    let aggregate_id = BurnExcessId::new(request.deposit_tx_hash);
    let store = burn_excess_store(pool.clone()).await?;

    let state = store.load(&aggregate_id).await?;
    let path_resolution = resolve_path(request.mode, state.as_ref())?;

    match path_resolution {
        PathResolution::ReportOnly(path) => {
            print_terminal_report(path, state.as_ref());
            Ok(())
        }
        PathResolution::Start(path) | PathResolution::Resume(path) => {
            if request.close {
                return close_stream(
                    &store,
                    &aggregate_id,
                    state.as_ref(),
                    path,
                    &request.reason,
                    request.execute,
                    &confirm,
                )
                .await;
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

            print_plan(&plan, request.execute);

            if !request.execute {
                return Ok(());
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
            .await
        }
    }
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

    let (funding_log_id, exclusion_excluded_at, resume_note) =
        match (path, state) {
            (BurnExcessPath::Internal, _) => {
                let balance = vault_service
                    .get_share_balance(listed_vault, issuer_wallet)
                    .await?;
                require_exact_issuer_share_balance(balance, request.shares)?;
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
                let balance = vault_service
                    .get_share_balance(listed_vault, issuer_wallet)
                    .await?;
                require_exact_issuer_share_balance(balance, request.shares)?;
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
                let balance = vault_service
                    .get_share_balance(listed_vault, issuer_wallet)
                    .await?;
                require_exact_issuer_share_balance(balance, request.shares)?;
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
                let balance = vault_service
                    .get_share_balance(listed_vault, issuer_wallet)
                    .await?;
                require_exact_issuer_share_balance(balance, request.shares)?;
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

fn print_plan(plan: &ProvenPlan, execute: bool) {
    let mode = match plan.path {
        BurnExcessPath::Internal => "internal",
        BurnExcessPath::External => "external",
    };
    let path_letter = match plan.path {
        BurnExcessPath::Internal => "A",
        BurnExcessPath::External => "B",
    };

    match plan.path {
        BurnExcessPath::Internal => {
            if let Some(note) = plan.resume_note {
                println!(
                    "mode={mode} path={path_letter} ({note}; issuer holds exact \
                     shares; no funding exclusion)"
                );
            } else {
                println!(
                    "mode={mode} path={path_letter} (issuer must hold exact shares; \
                     no funding exclusion)"
                );
            }
        }
        BurnExcessPath::External => {
            let funding = plan.funding_log_id.as_ref().map_or_else(
                || "unknown".into(),
                |log| format!("{:#x}", log.tx_hash),
            );
            if let Some(note) = plan.resume_note {
                let log_index = plan.funding_log_id.as_ref().map_or_else(
                    || "?".into(),
                    |log| log.log_index.to_string(),
                );
                println!(
                    "mode={mode} path={path_letter} ({note}; funding_tx_hash={funding}; \
                     log_index={log_index})"
                );
            } else {
                println!(
                    "mode={mode} path={path_letter} (funding_tx_hash={funding}; will \
                     exclude that Transfer log then burn)"
                );
            }
        }
    }

    println!(
        "bind: underlying={} network={} vault={:#x} issuer_request={} \
         deposit={:#x} receipt_id={} shares={} original_recipient={:#x} \
         issuer_wallet={:#x}",
        plan.underlying,
        plan.bind.network,
        plan.bind.vault,
        plan.bind.issuer_request_id,
        plan.bind.deposit_tx_hash,
        plan.bind.receipt_id,
        plan.bind.shares,
        plan.bind.original_recipient,
        plan.bind.issuer_wallet,
    );
    if let Some(funding) = &plan.funding_log_id {
        println!(
            "funding_log: tx={:#x} log_index={} from={:#x} to={:#x} amount={}",
            funding.tx_hash,
            funding.log_index,
            funding.from,
            funding.to,
            funding.amount,
        );
    }
    if let Some(advisory) = plan.freeze_advisory {
        println!("{advisory}");
    }
    // The exclusion write only beats the transfer poller to the funding log if
    // the poller is not running. `redemption_exists_for_tx` re-checks right
    // before the write, but it is a check, not a lock across processes.
    if plan.path == BurnExcessPath::External {
        println!(
            "precondition: the issuer service must be STOPPED; a running \
             transfer poller can open a Redemption for the funding Transfer \
             first and steer this recovery onto the Alpaca path"
        );
    }
    if execute {
        println!("mode=execute (mutations will run after confirmation)");
    } else {
        println!("mode=dry-run (no events, no sign, no exclusion write)");
    }
}

fn print_terminal_report(path: BurnExcessPath, state: Option<&BurnExcess>) {
    match state {
        Some(
            state @ BurnExcess::Completed {
                burn_tx_hash,
                block_number,
                completed_at,
                ..
            },
        ) => {
            println!(
                "stream completed path={} burn_tx={burn_tx_hash:#x} \
                 block={block_number} at={completed_at}",
                state.path()
            );
            if let Some(funding) = state.funding_log_id() {
                println!(
                    "funding exclusion remains permanent: tx={:#x} log_index={}",
                    funding.tx_hash, funding.log_index
                );
            }
        }
        Some(state @ BurnExcess::Closed { reason, closed_at, .. }) => {
            println!(
                "stream closed path={} reason={reason} at={closed_at}",
                state.path()
            );
            if let Some(funding) = state.funding_log_id() {
                println!(
                    "funding exclusion remains permanent: tx={:#x} log_index={}",
                    funding.tx_hash, funding.log_index
                );
            }
        }
        other => {
            println!("stream terminal path={path} state={other:?}");
        }
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
) -> Result<(), BurnExcessEngineError> {
    let state = state.ok_or_else(|| super::BurnExcessError::InvalidState {
        expected: "FundingExcluded, Intended, or Submitted".to_string(),
        found: "Uninitialized".to_string(),
    })?;

    match state {
        BurnExcess::FundingExcluded { .. }
        | BurnExcess::Intended { .. }
        | BurnExcess::Submitted { .. } => {}
        other => {
            return Err(super::BurnExcessError::InvalidState {
                expected: "FundingExcluded, Intended, or Submitted".to_string(),
                found: other.state_name().to_string(),
            }
            .into());
        }
    }

    println!(
        "close plan: path={path} state={} reason={reason} \
         (clears wallet nonce gate only; Closed is report-only for this deposit)",
        state.state_name()
    );
    if !execute {
        println!("mode=dry-run (no close event)");
        return Ok(());
    }

    if !confirm(&format!(
        "Close dead excess-burn stream {aggregate_id} (path={path}; clears \
         wallet gates only)?"
    ))? {
        return Err(BurnExcessEngineError::Aborted);
    }

    store
        .send(
            aggregate_id,
            BurnExcessCommand::CloseExcessBurn { reason: reason.to_string() },
        )
        .await?;
    println!(
        "Closed excess-burn stream {aggregate_id} (wallet gate cleared; stream \
         is report-only terminal)"
    );
    Ok(())
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
        None => {
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

            if mutation.plan.path == BurnExcessPath::External {
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

            intend_submit_confirm(&mutation).await
        }
        Some(BurnExcess::FundingExcluded { funding_log_id, .. }) => {
            if !confirm(&format!(
                "Sign and persist excess burn for deposit {:#x} (exclusion \
                 already recorded log_index={})?",
                mutation.request.deposit_tx_hash, funding_log_id.log_index
            ))? {
                return Err(BurnExcessEngineError::Aborted);
            }
            intend_submit_confirm(&mutation).await
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
    println!(
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
    provider: &P,
    vault_service: &dyn VaultService,
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

    let share_balance =
        vault_service.get_share_balance(bind.vault, bind.issuer_wallet).await?;
    require_exact_issuer_share_balance(share_balance, bind.shares)?;

    Ok(())
}

async fn intend_submit_confirm<P: Provider>(
    ctx: &MutationCtx<'_, P>,
) -> Result<(), BurnExcessEngineError> {
    // Irreversible sign boundary: re-check gates, balances, and Path B
    // exclusion index.
    require_wallet_intent_gates(
        ctx.pool,
        ctx.request.network,
        ctx.request.deposit_tx_hash,
    )
    .await?;
    require_issuer_balances(ctx.provider, ctx.vault_service, &ctx.plan.bind)
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
    println!(
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
    println!("Submitted excess burn tx={:#x}", sendable_tx.hash);

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
        BurnTxStatus::Reverted | BurnTxStatus::ProvablyDead => {
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
        BurnTxStatus::Reverted | BurnTxStatus::ProvablyDead => {
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

    println!(
        "Completed excess burn tx={:#x} block={} receipt_id={} shares={}",
        result.tx_hash, result.block_number, ctx.receipt_id, ctx.shares
    );

    // Before reconcile, which fails the CLI on error: the operator most needs
    // the final balance in exactly that case — burn landed, Complete
    // persisted, inventory read model now stale.
    let share_balance =
        ctx.vault_service.get_share_balance(ctx.bind.vault, ctx.owner).await?;
    println!(
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
        println!(
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

    println!(
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
    use alloy::providers::fillers::{BlobGasFiller, ChainIdFiller};
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use event_sorcery::StoreBuilder;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::*;
    use crate::Quantity;
    use crate::account::ClientId;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::burn_excess::exclusion::is_excluded_funding_log;
    use crate::burn_excess::proof::BurnExcessMode;
    use crate::mint::{MintEvent, TokenizationRequestId};
    use crate::receipt_inventory::{
        ReceiptSource, ReceiptVaultKey, send_receipt_inventory_command,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm};
    use crate::tokenized_asset::{
        AssetKey, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::vault::ReceiptInformation;
    use crate::vault::mock::MockVaultService;
    use crate::vault::rain_meta::OaSchemaCache;
    use crate::vault::service::RealBlockchainService;
    use crate::vault::{
        BurnTxStatus, MultiBurnResult, MultiBurnResultEntry, SendableTxWithHash,
    };

    const TEST_OA_SCHEMA: &str =
        "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm";
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
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller)
            .with_simple_nonce_management()
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer.clone()))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let service = RealBlockchainService::new(
            provider.clone(),
            Arc::new(OaSchemaCache::fixed(TEST_OA_SCHEMA)),
        );
        (evm, service, provider, signer)
    }

    async fn mint_with_info(
        evm: &LocalEvm,
        to: Address,
        issuer_request_id: &IssuerMintRequestId,
    ) -> (U256, U256, Bytes, B256) {
        let info = sample_receipt_info(issuer_request_id.clone());
        let encoded = info.encode(Some(TEST_OA_SCHEMA)).unwrap();
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

        let after_share = service
            .get_share_balance(evm.vault_address, evm.wallet_address)
            .await
            .unwrap();
        assert_eq!(before_share, after_share);

        let store = burn_excess_store(pool.clone()).await.unwrap();
        assert!(
            store.load(&BurnExcessId::new(deposit_tx)).await.unwrap().is_none()
        );
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
        let underlying = seed_listing(pool, evm.vault_address).await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_initiated(pool, &issuer_request_id, &underlying).await;

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
        let encoded = info.encode(Some(TEST_OA_SCHEMA)).unwrap();
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

        ExternalPathFixture {
            issuer_request_id,
            receipt_id,
            deposit_tx,
            funding_tx,
            recipient: recipient_address,
            funding_log_index,
        }
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
                    bind,
                    funding_log_id: funding.clone(),
                    reason: "seed".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();

        // Simulate dual-write gap / truncated index: event exists, row gone.
        sqlx::query("DELETE FROM burn_excess_funding_exclusions")
            .execute(&pool)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn close_dry_run_records_no_event_and_keeps_the_gate_closed() {
        let pool = pool().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let deposit_tx = B256::random();
        let store =
            seed_funding_excluded(&pool, &issuer_request_id, deposit_tx).await;

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
                false,
            ),
            |_| Ok(true),
        )
        .await
        .unwrap();

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
    /// safe.
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
    }
}
