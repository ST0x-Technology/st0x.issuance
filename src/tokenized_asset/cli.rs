use alloy::primitives::{Address, B256, Bytes, U256, address};
use alloy::providers::{Provider, ProviderBuilder};
use clap::{Args, Parser, Subcommand};
use event_sorcery::{
    AggregateError, LifecycleError, ReconcileError, Store, StoreBuilder,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use super::view::{
    TokenizedAssetViewError, find_vault, list_enabled_assets,
    underlying_has_listing,
};
use super::{TokenizedAsset, UnderlyingSymbol};
use crate::Network;
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::burn_excess::cli::{
    BurnExcessCommand as BurnExcessCliCommand, run_burn_excess_cli,
};
use crate::config::{
    DEFAULT_DATABASE_MAX_CONNECTIONS, DEFAULT_DATABASE_URL, LogLevel,
    VaultMode, VaultModeConfig, load_vault_mode_config, setup_tracing,
};
use crate::fireblocks::auth_probe::probe_auth_pair;
use crate::fireblocks::{
    Environment, FireblocksEnv, FireblocksVaultService, fetch_vault_address,
};
use crate::prepare_event_sourced_startup;
use crate::receipt_inventory::migration::ensure_holder_quiescent;
use crate::redemption::IssuerRedemptionRequestId;
use crate::redemption::force_complete::{
    VerifiedCompletion, ensure_burn_unclaimed, landed_burn_evidence,
    terminalize_and_settle, verify_landed_burn,
};
use crate::underlying::{
    AssetStatus, Underlying, UnderlyingCommand, UnderlyingViewError,
    load_freeze_status,
};
use crate::vault::onboarding::{
    ApprovalOutcome, check_orchestrator_readiness, ensure_unlimited_approval,
    prove_signing_shapes,
};
use crate::wallet::turnkey::resolve_turnkey_signer;
use crate::wallet::{SignerConfig, SignerEnv};

/// Parses and runs the issuer-host CLI end to end. The `issuer` binary is a thin
/// wrapper over this entry point.
///
/// # Errors
///
/// Returns an error if argument parsing fails, the store cannot be opened, the
/// asset is not supported, the operator aborts a mutation, or the command
/// dispatch fails.
pub async fn run_issuer_cli() -> anyhow::Result<()> {
    setup_tracing(&LogLevel::Info);
    IssuerCli::parse().dispatch().await
}

#[derive(Parser)]
#[command(
    name = "issuer",
    version,
    about = "Issuer-host admin CLI for st0x.issuance"
)]
struct IssuerCli {
    #[command(subcommand)]
    command: IssuerCommand,
}

/// Freeze, unfreeze, and status address the underlying-keyed `Underlying`
/// aggregate: a corporate action applies to every listing of the underlying on
/// every network, so these subcommands deliberately take no network argument.
#[derive(Subcommand)]
enum IssuerCommand {
    /// Freeze an underlying on all networks: reject new mints (in-flight
    /// redemptions still complete).
    Freeze(AssetArgs),
    /// Unfreeze an underlying: resume accepting new mints on all networks.
    Unfreeze(AssetArgs),
    /// Print an underlying's current freeze status.
    Status(AssetArgs),
    /// Terminalize a Failed redemption whose burn already landed on-chain.
    /// For legacy custodian-era burns whose backend transaction id the
    /// current signing backend cannot look up: the operator supplies the
    /// on-chain transaction hash, and everything else is verified — the
    /// transaction must be a successful burn on the redemption's vault whose
    /// per-receipt withdrawals match the persisted burn plan exactly, and no
    /// other redemption may already claim it. Completes the redemption and
    /// settles its receipt reservation like a normal burn confirmation.
    ForceCompleteRedemption(Box<ForceCompleteRedemptionArgs>),
    /// Sweep the legacy holdings the receipt-custody migration could not see
    /// out of the Fireblocks wallet: five ERC-1155 receipts inherited from the
    /// pre-Fireblocks issuer wallet in March 2026 (they predate the service's
    /// inventory tracking, so `migrate-receipts` never listed them), plus the
    /// stranded 0.2 tCOIN ERC-20 redemption, forwarded to the liquidity bot
    /// wallet for re-sending. Every value is pinned in a hardcoded table and
    /// cross-checked against live on-chain balances before anything is
    /// submitted; entries already swept are skipped, so re-running is safe.
    ///
    /// Temporary, Base-only, thrown away with the Fireblocks integration.
    SweepLegacyReceipts(Box<SweepLegacyReceiptsArgs>),
    /// A/B-probe Fireblocks API authentication and print the raw responses.
    ///
    /// Sends the same authenticated vault-address GET twice, varying only the
    /// JWT expiry window: once compliant with Fireblocks' documented
    /// `exp < iat + 30s` bound, once with the SDK's out-of-spec `iat + 55`.
    /// Prints each response's status and verbatim body — the diagnostic the
    /// SDK swallows on 401. A split verdict proves the platform enforces the
    /// documented bound; identical rejections carry the server's own error
    /// code for diagnosis. Submits nothing and reads only the vault address.
    FireblocksAuthProbe(Box<FireblocksAuthProbeArgs>),
    /// Administrative supply correction: burn excess shares from a proven
    /// duplicate deposit. Path is a required mode keyword
    /// (`internal` | `external`); never Alpaca, never a Redemption aggregate.
    /// Default is dry-run inspection; pass `--execute` to mutate.
    #[command(subcommand)]
    BurnExcess(Box<BurnExcessCliCommand>),
    /// On-chain read-only pre-cutover gate for the orchestrator rollout:
    /// verifies via on-chain reads that the Turnkey bot wallet holds
    /// MINT_ROLE and BURN_ROLE on the orchestrator, that
    /// vaultLogicIsExpected() reports healthy, that the orchestrator holds
    /// DEPOSIT and WITHDRAW on each vault's authorizer, and that each
    /// checked asset's one-time unlimited ERC-20 approval to the
    /// orchestrator has been executed. The orchestrator address comes from
    /// the TOML config file — never typed. Exits non-zero unless every check
    /// passes, so runbook steps can gate on it. Submits nothing and signs
    /// nothing on-chain; locally it does touch the operator's database —
    /// connecting runs any pending migrations and projection catch-up before
    /// the asset lookup.
    OrchestratorPreflight(Box<OrchestratorPreflightArgs>),
    /// Executes one asset's one-time unlimited ERC-20 approval (Turnkey bot
    /// wallet -> orchestrator, on the vault share token) through the Turnkey
    /// signer. Idempotent: an already-unlimited allowance sends nothing, so
    /// re-runs and per-asset batches are safe. Approvals are inert until the
    /// asset's vault_mode flips to orchestrator, and approving through
    /// Turnkey is itself the live proof that the Turnkey signing policy
    /// allows `approve` on this token. The orchestrator address comes from
    /// the TOML config file — never typed.
    ApproveOrchestrator(Box<ApproveOrchestratorArgs>),
    /// Signs — WITHOUT broadcasting — one transaction per shape the Turnkey
    /// signing policy must allow before an asset's cutover: mint and burn on
    /// the orchestrator, approve on the vault share token, and ERC-1155
    /// safeBatchTransferFrom to the orchestrator (the receipt-migration step
    /// needs it). A policy gap surfaces here as a named signing refusal
    /// instead of during the pilot's first live mint. On-chain read-only:
    /// nothing is submitted and no chain state changes. Locally it does
    /// touch the operator's database — connecting runs any pending
    /// migrations and projection catch-up before the vault lookup.
    VerifyOrchestratorSigning(Box<VerifyOrchestratorSigningArgs>),
}

#[derive(Args)]
struct FireblocksAuthProbeArgs {
    #[clap(flatten)]
    fireblocks: FireblocksEnv,
}

#[derive(Args)]
struct SweepLegacyReceiptsArgs {
    /// RPC endpoint for Base — the service's own `RPC_URL`.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,

    /// Chain the sweep must run against, cross-checked against the chain the
    /// RPC reports. The sweep table is Base-only, so anything but 8453 is
    /// refused.
    #[arg(long)]
    chain_id: u64,

    /// Attempt counter salted into every Fireblocks externalTxId. Fireblocks
    /// treats an externalTxId as spent once a transaction under it completes
    /// — including a transaction that completed on the platform but REVERTED
    /// on-chain, which a plain rerun would recover forever instead of
    /// resubmitting. After the sweep reports a reverted transaction, rerun
    /// with the next attempt number to submit fresh.
    #[arg(long, default_value_t = 1)]
    attempt: u32,

    #[clap(flatten)]
    fireblocks: FireblocksEnv,
}

/// The retired Fireblocks issuer wallet every sweep entry moves out of.
const LEGACY_HOLDER: Address =
    address!("0x1c66D6708914C40239D54919320b4C48cAE3D1A9");

/// The Turnkey issuer wallet that now custodies every receipt. The running
/// service discovers inbound receipt transfers to this wallet on its next
/// backfill pass, so swept receipts enter tracked inventory with no restart.
const TURNKEY_RECIPIENT: Address =
    address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE");

/// The liquidity bot wallet. The stranded redemption goes back here rather
/// than straight to the redemption wallet because redemption detection skips
/// transfers from senders it cannot attribute; the bot re-sends it as a
/// normal redemption.
const LIQUIDITY_BOT: Address =
    address!("0xa9c16673F65AE808688cB18952AFE3d9658C808f");

/// The tCOIN vault, whose ERC-20 shares include the stranded redemption.
const TCOIN_VAULT: Address =
    address!("0x626757e6F50675D17fcAd312E82f989aE7A23d38");

/// The 0.2 tCOIN redemption that landed at the retired wallet on 2026-07-29.
const STRANDED_TCOIN: u128 = 200_000_000_000_000_000;

/// A Base block mined before this tool existed (2026-07-31, early UTC). The
/// stranded-tCOIN forward can only have been submitted by this tool, so any
/// proof of it lives at or after this block; scanning starts here.
const SWEEP_ERA_START_BLOCK: u64 = 49_330_000;

/// Proves the stranded tCOIN actually reached the liquidity bot wallet by
/// finding the exact legacy-to-bot `Transfer` on the tCOIN contract. A
/// drained source with no such event means the tokens went somewhere else,
/// which is an error, not a skip.
async fn find_stranded_forward_event<P: Provider + Clone>(
    provider: &P,
) -> anyhow::Result<B256> {
    let vault = OffchainAssetReceiptVault::new(TCOIN_VAULT, provider);
    let expected = U256::from(STRANDED_TCOIN);
    let latest = provider.get_block_number().await?;

    // Public Base RPC caps eth_getLogs at 10k blocks per query.
    let mut from_block = SWEEP_ERA_START_BLOCK;
    while from_block <= latest {
        let to_block = from_block.saturating_add(9_999).min(latest);
        let events = vault
            .Transfer_filter()
            .from_block(from_block)
            .to_block(to_block)
            .query()
            .await?;

        for (event, log) in events {
            if event.from == LEGACY_HOLDER
                && event.to == LIQUIDITY_BOT
                && event.value == expected
            {
                return log.transaction_hash.ok_or_else(|| {
                    anyhow::anyhow!(
                        "the stranded-tCOIN forward Transfer event carries \
                         no transaction hash"
                    )
                });
            }
        }
        from_block = to_block.saturating_add(1);
    }

    anyhow::bail!(
        "stranded tCOIN: the legacy wallet is drained but no Transfer of \
         {expected} from {LEGACY_HOLDER} to {LIQUIDITY_BOT} exists on \
         {TCOIN_VAULT} since block {SWEEP_ERA_START_BLOCK} — the tokens \
         went somewhere else; investigate before rerunning"
    )
}

/// One receipt contract's worth of legacy holdings to sweep.
struct LegacyReceiptSweep {
    label: &'static str,
    external_tx_id: &'static str,
    receipt_contract: Address,
    /// `(receipt id, expected balance)` — the sweep refuses to move a balance
    /// that no longer matches, so a stale table cannot move the wrong amount.
    holdings: &'static [(u64, u128)],
}

/// The five receipts the March 2026 issuer-wallet handover left at the
/// Fireblocks wallet. They were inherited from the previous issuer wallet
/// (0xe70d821f3462a074e63b42d0aac6523faae1d611) before this service's
/// inventory tracking began, so `migrate-receipts` — which enumerates tracked
/// holdings — never saw them. Balances verified on-chain 2026-07-31.
const LEGACY_RECEIPT_SWEEPS: [LegacyReceiptSweep; 4] = [
    LegacyReceiptSweep {
        label: "tCOIN receipts",
        external_tx_id: "legacy-receipt-sweep-tcoin",
        receipt_contract: address!(
            "0xBA1B8836A5510815e96103F067715b7CCC7c2E0E"
        ),
        holdings: &[(19, 1_000_000_000_000_000_000)],
    },
    LegacyReceiptSweep {
        label: "tCRCL receipts",
        external_tx_id: "legacy-receipt-sweep-tcrcl",
        receipt_contract: address!(
            "0xd508B97975fBE04E62bFf18959549b046bD8FA78"
        ),
        holdings: &[(4, 11_048_599_999_999_999_980)],
    },
    LegacyReceiptSweep {
        label: "tMSTR receipts",
        external_tx_id: "legacy-receipt-sweep-tmstr",
        receipt_contract: address!(
            "0x1c1fEF6f7b8e576219554b1d11c8aF29D00C0cEC"
        ),
        holdings: &[(5, 8_000_000_000_000_000_000)],
    },
    LegacyReceiptSweep {
        label: "tSPYM receipts",
        external_tx_id: "legacy-receipt-sweep-tspym",
        receipt_contract: address!(
            "0x957056dD6e2E594742E36675e8AA5A567163E5bd"
        ),
        holdings: &[
            (10, 28_401_765_980_495_899_205),
            (12, 90_000_000_000_000_000_000),
        ],
    },
];

async fn run_sweep_legacy_receipts(
    args: SweepLegacyReceiptsArgs,
    confirm: fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    let fireblocks_config = args.fireblocks.into_config()?;
    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    anyhow::ensure!(
        chain_id == 8453,
        "the legacy sweep table is Base-only (chain 8453); the RPC reports \
         chain {chain_id}"
    );

    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;
    let fireblocks_wallet = fetch_vault_address(&fireblocks_config).await?;
    anyhow::ensure!(
        fireblocks_wallet == LEGACY_HOLDER,
        "Fireblocks credentials resolve to {fireblocks_wallet}, not the \
         legacy holder {LEGACY_HOLDER} the sweep table was written for"
    );

    println!("Sweeping legacy holdings out of {LEGACY_HOLDER}:");
    for sweep in &LEGACY_RECEIPT_SWEEPS {
        for (receipt_id, amount) in sweep.holdings {
            println!(
                "  {} id {receipt_id}: {amount} -> {TURNKEY_RECIPIENT}",
                sweep.label
            );
        }
    }
    println!(
        "  stranded tCOIN redemption: {STRANDED_TCOIN} -> {LIQUIDITY_BOT}"
    );

    if !confirm(
        "Submit these transfers through Fireblocks (console approval may be \
         required)?",
    )? {
        anyhow::bail!("aborted by operator");
    }

    let service = FireblocksVaultService::new(
        &fireblocks_config,
        provider.clone(),
        chain_id,
    )?;

    for sweep in &LEGACY_RECEIPT_SWEEPS {
        sweep_legacy_receipt_contract(&service, &provider, sweep, args.attempt)
            .await?;
    }
    sweep_stranded_tcoin(&service, &provider, args.attempt).await?;

    println!("Legacy sweep complete.");
    Ok(())
}

/// Requires the Fireblocks-completed transaction to have SUCCEEDED on-chain.
///
/// Fireblocks reports "Completed" for a transaction that was mined but
/// REVERTED, and a completed transaction's externalTxId is spent forever — a
/// rerun under the same id recovers the reverted transaction instead of
/// submitting fresh. Refusing here keeps a revert from being reported as
/// success, and the error names the `--attempt` bump that mints fresh ids.
async fn require_onchain_success<P: Provider>(
    provider: &P,
    tx_hash: B256,
    attempt: u32,
) -> anyhow::Result<()> {
    let onchain =
        provider.get_transaction_receipt(tx_hash).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "no on-chain receipt found for {tx_hash} although Fireblocks \
                 reported it completed"
            )
        })?;

    anyhow::ensure!(
        onchain.status(),
        "{tx_hash} REVERTED on-chain although Fireblocks reported it \
         completed; nothing moved, and its externalTxId is spent — rerun \
         with --attempt {}",
        attempt.saturating_add(1)
    );
    Ok(())
}

/// Sweeps the still-held subset of one contract's legacy receipts, refusing
/// on any balance that matches neither the table nor zero. Success requires
/// the transaction to have succeeded on-chain, the legacy wallet drained, and
/// the Turnkey wallet to have gained exactly the swept amounts.
async fn sweep_legacy_receipt_contract<P: Provider + Clone>(
    service: &FireblocksVaultService<P>,
    provider: &P,
    sweep: &LegacyReceiptSweep,
    attempt: u32,
) -> anyhow::Result<()> {
    let receipt = Receipt::new(sweep.receipt_contract, provider);

    let mut ids = Vec::new();
    let mut amounts = Vec::new();
    let mut recipient_before = Vec::new();
    for (receipt_id, amount) in sweep.holdings {
        let receipt_id = U256::from(*receipt_id);
        let expected = U256::from(*amount);
        let held = receipt.balanceOf(LEGACY_HOLDER, receipt_id).call().await?;

        if held.is_zero() {
            // A drained source only proves a sweep if the receipt actually
            // sits at the Turnkey wallet; anything else means it went
            // somewhere it should not have.
            let at_recipient =
                receipt.balanceOf(TURNKEY_RECIPIENT, receipt_id).call().await?;
            // The destination may have held this receipt before the sweep.
            // Drained source custody is corroborated when it holds at least the
            // swept amount, matching the migration path's rerun semantics.
            anyhow::ensure!(
                at_recipient >= expected,
                "{}: id {receipt_id} is gone from the legacy wallet but \
                 {TURNKEY_RECIPIENT} holds {at_recipient}, less than the \
                 expected swept amount {expected} — it was NOT fully swept \
                 here; investigate before rerunning",
                sweep.label
            );
            continue;
        }
        anyhow::ensure!(
            held == expected,
            "{}: id {receipt_id} holds {held} at the legacy wallet, but the \
             sweep table expects {expected}; refusing to move an unexpected \
             balance",
            sweep.label
        );
        let before =
            receipt.balanceOf(TURNKEY_RECIPIENT, receipt_id).call().await?;
        ids.push(receipt_id);
        amounts.push(expected);
        recipient_before.push(before);
    }

    if ids.is_empty() {
        println!(
            "{}: already swept and verified at {TURNKEY_RECIPIENT}, skipping",
            sweep.label
        );
        return Ok(());
    }

    let calldata = receipt
        .safeBatchTransferFrom(
            LEGACY_HOLDER,
            TURNKEY_RECIPIENT,
            ids.clone(),
            amounts.clone(),
            Bytes::new(),
        )
        .calldata()
        .clone();

    // The retiring wallet may have a broadcast-but-unmined transaction from
    // an earlier attempt or another process; submitting over it would create
    // a second in-flight transfer without knowing the first one's outcome.
    // Same guard as every custody-transfer submission path.
    ensure_holder_quiescent(provider, LEGACY_HOLDER).await?;

    let external_tx_id = format!("{}-a{attempt}", sweep.external_tx_id);
    let tx_hash = service
        .submit_contract_call_to_completion(
            sweep.receipt_contract,
            &calldata,
            sweep.label,
            &external_tx_id,
        )
        .await?;

    require_onchain_success(provider, tx_hash, attempt).await?;

    for ((receipt_id, amount), before) in
        ids.iter().zip(&amounts).zip(&recipient_before)
    {
        let remaining =
            receipt.balanceOf(LEGACY_HOLDER, *receipt_id).call().await?;
        anyhow::ensure!(
            remaining.is_zero(),
            "{}: id {receipt_id} still holds {remaining} at the legacy \
             wallet after {tx_hash}",
            sweep.label
        );

        let gained = receipt
            .balanceOf(TURNKEY_RECIPIENT, *receipt_id)
            .call()
            .await?
            .checked_sub(*before)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "{}: id {receipt_id} balance at {TURNKEY_RECIPIENT} \
                     DECREASED across {tx_hash}",
                    sweep.label
                )
            })?;
        anyhow::ensure!(
            gained == *amount,
            "{}: id {receipt_id} gained {gained} at {TURNKEY_RECIPIENT} \
             across {tx_hash}, expected {amount}",
            sweep.label
        );
    }

    println!("{}: swept in {tx_hash}", sweep.label);
    Ok(())
}

/// Forwards the stranded 0.2 tCOIN redemption to the liquidity bot wallet,
/// with the same on-chain success and recipient-gain verification as the
/// receipt sweeps.
async fn sweep_stranded_tcoin<P: Provider + Clone>(
    service: &FireblocksVaultService<P>,
    provider: &P,
    attempt: u32,
) -> anyhow::Result<()> {
    let vault = OffchainAssetReceiptVault::new(TCOIN_VAULT, provider);
    let expected = U256::from(STRANDED_TCOIN);
    let held = vault.balanceOf(LEGACY_HOLDER).call().await?;

    if held.is_zero() {
        // The bot wallet holds tCOIN transiently during its own operations,
        // so its balance proves nothing about where the stranded tokens
        // went. Only the exact legacy-to-bot Transfer event does.
        let forwarded_in = find_stranded_forward_event(provider).await?;
        println!(
            "stranded tCOIN: already forwarded in {forwarded_in}, skipping"
        );
        return Ok(());
    }
    anyhow::ensure!(
        held == expected,
        "stranded tCOIN: the legacy wallet holds {held}, but the sweep \
         expects {expected}; refusing to move an unexpected balance"
    );
    let recipient_before = vault.balanceOf(LIQUIDITY_BOT).call().await?;

    let calldata = vault.transfer(LIQUIDITY_BOT, expected).calldata().clone();

    // Same guard as the receipt sweeps: never submit over an in-flight
    // transaction from the retiring wallet.
    ensure_holder_quiescent(provider, LEGACY_HOLDER).await?;

    let external_tx_id = format!("legacy-sweep-stranded-tcoin-a{attempt}");
    let tx_hash = service
        .submit_contract_call_to_completion(
            TCOIN_VAULT,
            &calldata,
            "stranded tCOIN redemption forward",
            &external_tx_id,
        )
        .await?;

    require_onchain_success(provider, tx_hash, attempt).await?;

    let remaining = vault.balanceOf(LEGACY_HOLDER).call().await?;
    anyhow::ensure!(
        remaining.is_zero(),
        "stranded tCOIN: the legacy wallet still holds {remaining} after \
         {tx_hash}"
    );

    let gained = vault
        .balanceOf(LIQUIDITY_BOT)
        .call()
        .await?
        .checked_sub(recipient_before)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "stranded tCOIN: balance at {LIQUIDITY_BOT} DECREASED across \
                 {tx_hash}"
            )
        })?;
    anyhow::ensure!(
        gained == expected,
        "stranded tCOIN: {LIQUIDITY_BOT} gained {gained} across {tx_hash}, \
         expected {expected}"
    );

    println!("stranded tCOIN: forwarded in {tx_hash}");
    Ok(())
}

#[derive(Args)]
struct ForceCompleteRedemptionArgs {
    /// Issuer redemption request id of the Failed redemption.
    issuer_request_id: IssuerRedemptionRequestId,

    /// On-chain hash of the transaction the operator asserts is this
    /// redemption's landed burn. Verified against the persisted burn plan
    /// before anything is recorded.
    #[arg(long)]
    burn_tx_hash: B256,

    /// Network the redemption was detected on, cross-checked against the
    /// event history and the RPC endpoint.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

    /// RPC endpoint for the network — the service's own `RPC_URL`.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,

    /// Chain this must run against. Deliberately redundant with `--network`:
    /// the command refuses unless both name the same chain and the RPC
    /// reports it, so a destructive terminalization needs two independent
    /// statements of where it runs.
    #[arg(long)]
    chain_id: u64,

    /// Why the redemption is being force-completed; recorded on the terminal
    /// event for the audit trail.
    #[arg(long)]
    reason: String,

    /// Exact persisted signed burn hash the operator has reconciled and
    /// explicitly acknowledges may still land, when the redemption carries an
    /// unresolved signed transaction different from `--burn-tx-hash`.
    #[arg(long)]
    acknowledged_unresolved_burn_tx_hash: Option<B256>,

    #[arg(
        long = "database-url",
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL,
        value_parser = parse_sqlite_url
    )]
    database_url: String,
    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    database_max_connections: u32,
}

#[derive(Args)]
struct OrchestratorPreflightArgs {
    /// TOML config file whose `[orchestrator].address` names the orchestrator
    /// to check — the config file is the single source of truth for the
    /// address, matching what the deployed service resolves.
    #[arg(long, env = "CONFIG")]
    config: PathBuf,

    /// Network whose asset listings to check.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

    /// Restrict the per-asset allowance checks to these underlyings
    /// (repeatable). Defaults to every enabled asset on the network.
    /// Upper-cased like [`AssetArgs`].
    #[arg(long = "asset", value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    assets: Vec<UnderlyingSymbol>,

    /// RPC endpoint for the network — the service's own `RPC_URL`.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,

    /// Chain this must run against, cross-checked against the chain the RPC
    /// reports.
    #[arg(long)]
    chain_id: u64,

    #[clap(flatten)]
    signer: SignerEnv,

    #[arg(
        long = "database-url",
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL,
        value_parser = parse_sqlite_url
    )]
    database_url: String,
    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    database_max_connections: u32,
}

#[derive(Args)]
struct ApproveOrchestratorArgs {
    /// Underlying symbol, e.g. RKLB. Upper-cased like [`AssetArgs`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    underlying: UnderlyingSymbol,

    /// TOML config file whose `[orchestrator].address` names the approval's
    /// spender — the config file is the single source of truth for the
    /// address, matching what the deployed service resolves.
    #[arg(long, env = "CONFIG")]
    config: PathBuf,

    /// Network whose vault to approve on.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

    /// RPC endpoint for the network — the service's own `RPC_URL`.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,

    /// Chain this must run against, cross-checked against the chain the RPC
    /// reports.
    #[arg(long)]
    chain_id: u64,

    #[clap(flatten)]
    signer: SignerEnv,

    #[arg(
        long = "database-url",
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL,
        value_parser = parse_sqlite_url
    )]
    database_url: String,
    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    database_max_connections: u32,
}

#[derive(Args)]
struct VerifyOrchestratorSigningArgs {
    /// Underlying symbol whose vault the token-scoped shapes (approve,
    /// receipt transfer) are built against, e.g. RKLB. Upper-cased like
    /// [`AssetArgs`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    underlying: UnderlyingSymbol,

    /// TOML config file whose `[orchestrator].address` names the orchestrator
    /// the shapes target — the config file is the single source of truth for
    /// the address, matching what the deployed service resolves.
    #[arg(long, env = "CONFIG")]
    config: PathBuf,

    /// Network whose vault the shapes are built against.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

    /// RPC endpoint for the network — the service's own `RPC_URL`.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,

    /// Chain this must run against, cross-checked against the chain the RPC
    /// reports.
    #[arg(long)]
    chain_id: u64,

    #[clap(flatten)]
    signer: SignerEnv,

    #[arg(
        long = "database-url",
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL,
        value_parser = parse_sqlite_url
    )]
    database_url: String,
    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    database_max_connections: u32,
}

#[derive(Args)]
struct AssetArgs {
    /// Underlying symbol, e.g. SGOV. Upper-cased so `"sgov"` resolves to the
    /// stored `SGOV` (assets are keyed by their upper-case symbol). Whitespace
    /// trimming is handled by [`UnderlyingSymbol::new`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    underlying: UnderlyingSymbol,
    #[arg(
        long = "database-url",
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL,
        value_parser = parse_sqlite_url
    )]
    database_url: String,
    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    database_max_connections: u32,
}

impl IssuerCli {
    async fn dispatch(self) -> anyhow::Result<()> {
        match self.command {
            IssuerCommand::Freeze(args) => {
                run_asset_command(AssetAction::Freeze, &args).await
            }
            IssuerCommand::Unfreeze(args) => {
                run_asset_command(AssetAction::Unfreeze, &args).await
            }
            IssuerCommand::Status(args) => {
                run_asset_command(AssetAction::Status, &args).await
            }
            IssuerCommand::ForceCompleteRedemption(args) => {
                run_force_complete_redemption(*args, prompt_confirm).await
            }
            IssuerCommand::FireblocksAuthProbe(args) => {
                run_fireblocks_auth_probe(*args).await
            }
            IssuerCommand::SweepLegacyReceipts(args) => {
                run_sweep_legacy_receipts(*args, prompt_confirm).await
            }
            IssuerCommand::BurnExcess(command) => {
                run_burn_excess_cli(*command, prompt_confirm).await
            }
            IssuerCommand::OrchestratorPreflight(args) => {
                run_orchestrator_preflight(*args).await
            }
            IssuerCommand::ApproveOrchestrator(args) => {
                run_approve_orchestrator(*args, prompt_confirm).await
            }
            IssuerCommand::VerifyOrchestratorSigning(args) => {
                run_verify_orchestrator_signing(*args).await
            }
        }
    }
}

/// Verifies an operator-supplied transaction as a Failed redemption's landed
/// burn, then terminalizes the redemption and settles its receipt
/// reservation.
///
/// The hash is the only thing the operator asserts; the binding is proven,
/// not trusted: the transaction must be a successful burn on the vault the
/// redemption's asset resolves to, its per-receipt withdrawals must match the
/// persisted burn plan exactly, and no other redemption's history may mention
/// it. The withdrawal owner is recovered from the transaction's own
/// signature, so a burn by any unrelated wallet cannot match unless it
/// consumed exactly the receipts this redemption reserved.
async fn run_force_complete_redemption(
    args: ForceCompleteRedemptionArgs,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    if args.chain_id != args.network.chain_id() {
        anyhow::bail!(
            "--network {} is chain {} but --chain-id is {}",
            args.network,
            args.network.chain_id(),
            args.chain_id
        );
    }

    println!("Using database: {}", args.database_url);
    let admin =
        AssetAdmin::connect(&args.database_url, args.database_max_connections)
            .await?;

    let evidence =
        landed_burn_evidence(&admin.pool, &args.issuer_request_id).await?;
    if evidence.network != args.network {
        anyhow::bail!(
            "redemption {} was detected on {}, not --network {}",
            args.issuer_request_id,
            evidence.network,
            args.network
        );
    }
    let vault =
        find_vault(&admin.pool, &evidence.underlying, &evidence.network)
            .await?
            .ok_or_else(|| AssetAdminError::NotFound {
                underlying: evidence.underlying.clone(),
            })?;

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;

    let landed = verify_landed_burn(
        &provider,
        vault,
        args.burn_tx_hash,
        &evidence.planned_burns,
    )
    .await?;
    ensure_burn_unclaimed(
        &admin.pool,
        &args.issuer_request_id,
        args.burn_tx_hash,
    )
    .await?;

    println!(
        "{} redemption {}: transaction {} at block {} burned {} share(s) of \
         vault {vault} across {} receipt(s), signed by {}, matching the \
         persisted burn plan exactly",
        evidence.underlying,
        args.issuer_request_id,
        args.burn_tx_hash,
        landed.verification.block_number,
        landed.verification.shares_burned,
        landed.verification.burns.len(),
        landed.owner,
    );

    if !confirm(&format!(
        "Force-complete redemption {} with this verified burn and settle its \
         receipt reservation?",
        args.issuer_request_id
    ))? {
        anyhow::bail!("aborted by operator");
    }

    terminalize_and_settle(
        &admin.pool,
        chain_id,
        vault,
        &args.issuer_request_id,
        VerifiedCompletion {
            burn_tx_hash: args.burn_tx_hash,
            block_number: landed.verification.block_number,
            reason: args.reason.clone(),
            acknowledged_unresolved_burn_tx_hash: args
                .acknowledged_unresolved_burn_tx_hash,
        },
    )
    .await?;

    println!(
        "Force-completed redemption {} and settled its reservation.",
        args.issuer_request_id
    );

    Ok(())
}

/// The read-only pre-cutover readiness gate: resolves the orchestrator from
/// the TOML config, the bot wallet from the Turnkey configuration, and the
/// vaults from the listing view, then reads roles, orchestrator health, and
/// per-asset allowances on-chain. Prints the report and fails unless every
/// check passes, so the cutover runbook can gate on the exit code. No prompt:
/// nothing is signed or submitted.
async fn run_orchestrator_preflight(
    args: OrchestratorPreflightArgs,
) -> anyhow::Result<()> {
    if args.chain_id != args.network.chain_id() {
        anyhow::bail!(
            "--network {} is chain {} but --chain-id is {}",
            args.network,
            args.network.chain_id(),
            args.chain_id
        );
    }

    let vault_modes = load_vault_mode_config(&args.config)?;
    let orchestrator = orchestrator_address_from(&vault_modes, &args.config)?;

    // Only the wallet address is used — the readiness facts (roles,
    // approvals) are keyed to the Turnkey bot wallet, and requiring the full
    // Turnkey configuration keeps that address sourced from the service's own
    // environment instead of an operator-typed value.
    let SignerConfig::Turnkey(turnkey_config) = args.signer.into_config()?
    else {
        anyhow::bail!(
            "orchestrator-preflight requires the Turnkey signer configuration"
        );
    };
    let bot = turnkey_config.settings.address;

    println!("Using database: {}", args.database_url);
    let admin =
        AssetAdmin::connect(&args.database_url, args.database_max_connections)
            .await?;
    let assets =
        preflight_assets(&admin.pool, args.network, &args.assets, &vault_modes)
            .await?;

    verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;
    let report =
        check_orchestrator_readiness(&provider, orchestrator, bot, &assets)
            .await?;

    println!("{report}");

    if !report.is_ready() {
        anyhow::bail!("orchestrator preflight FAILED — see the report above");
    }

    Ok(())
}

/// Executes one asset's one-time unlimited approval through the Turnkey
/// signer, after an explicit confirmation naming the asset, vault,
/// orchestrator, and wallet. The mutation itself is idempotent
/// ([`ensure_unlimited_approval`]), so a re-run after a completed approval
/// reports "already unlimited" instead of sending again.
async fn run_approve_orchestrator(
    args: ApproveOrchestratorArgs,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    if args.chain_id != args.network.chain_id() {
        anyhow::bail!(
            "--network {} is chain {} but --chain-id is {}",
            args.network,
            args.network.chain_id(),
            args.chain_id
        );
    }

    let orchestrator = required_orchestrator_address(&args.config)?;

    let SignerConfig::Turnkey(turnkey_config) = args.signer.into_config()?
    else {
        anyhow::bail!(
            "approve-orchestrator requires the Turnkey signer configuration"
        );
    };
    let bot = turnkey_config.settings.address;

    println!("Using database: {}", args.database_url);
    let admin =
        AssetAdmin::connect(&args.database_url, args.database_max_connections)
            .await?;
    let vault = find_vault(&admin.pool, &args.underlying, &args.network)
        .await?
        .ok_or_else(|| AssetAdminError::NotFound {
            underlying: args.underlying.clone(),
        })?;

    if !confirm(&format!(
        "Approve UNLIMITED spending of {} vault {vault} shares by \
         orchestrator {orchestrator}, from Turnkey wallet {bot}?",
        args.underlying
    ))? {
        anyhow::bail!("aborted by operator");
    }

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let resolved = resolve_turnkey_signer(&turnkey_config, chain_id)?;
    let provider = ProviderBuilder::new()
        .with_chain_id(chain_id)
        .wallet(resolved.wallet)
        .connect(args.rpc_url.as_str())
        .await?;

    // The spender is about to receive an UNLIMITED allowance from the
    // production bot wallet, so prove the configured address actually IS an
    // orchestrator before approving: these reads only answer on a contract
    // implementing the orchestrator interface (a typo'd or stale TOML entry
    // fails them), and a false version lock means the address is a stale
    // deployment nothing should be approved for.
    let readiness =
        check_orchestrator_readiness(&provider, orchestrator, bot, &[])
            .await
            .map_err(|error| {
            anyhow::anyhow!(
                "refusing to approve: {orchestrator} cannot be verified \
                     as an orchestrator (readiness reads failed: {error})"
            )
        })?;
    if !readiness.vault_logic_expected {
        anyhow::bail!(
            "refusing to approve: {orchestrator} reports \
             vaultLogicIsExpected() = false — the configured address is a \
             stale or mismatched orchestrator deployment"
        );
    }

    match ensure_unlimited_approval(&provider, vault, orchestrator, bot).await?
    {
        ApprovalOutcome::AlreadyUnlimited => println!(
            "Already unlimited: {} vault {vault} needs no approval \
             transaction.",
            args.underlying
        ),
        ApprovalOutcome::Approved { tx_hash } => println!(
            "Approved: unlimited allowance for orchestrator {orchestrator} \
             on {} vault {vault} in {tx_hash}.",
            args.underlying
        ),
    }
    println!("Run orchestrator-preflight to verify overall readiness.");

    Ok(())
}

/// Signs the pre-cutover policy-proof shapes with Turnkey — never
/// broadcasting — and reports each signed shape. A Turnkey signing-policy
/// denial fails here, naming the refused shape, which is the entire point:
/// the alternative discovery site is the pilot's first live transaction. No
/// prompt: nothing is submitted and no state changes.
async fn run_verify_orchestrator_signing(
    args: VerifyOrchestratorSigningArgs,
) -> anyhow::Result<()> {
    if args.chain_id != args.network.chain_id() {
        anyhow::bail!(
            "--network {} is chain {} but --chain-id is {}",
            args.network,
            args.network.chain_id(),
            args.chain_id
        );
    }

    let orchestrator = required_orchestrator_address(&args.config)?;

    let SignerConfig::Turnkey(turnkey_config) = args.signer.into_config()?
    else {
        anyhow::bail!(
            "verify-orchestrator-signing requires the Turnkey signer \
             configuration — proving a local key's signing capability says \
             nothing about the Turnkey policy"
        );
    };
    let bot = turnkey_config.settings.address;

    println!("Using database: {}", args.database_url);
    let admin =
        AssetAdmin::connect(&args.database_url, args.database_max_connections)
            .await?;
    let vault = find_vault(&admin.pool, &args.underlying, &args.network)
        .await?
        .ok_or_else(|| AssetAdminError::NotFound {
            underlying: args.underlying.clone(),
        })?;

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let resolved = resolve_turnkey_signer(&turnkey_config, chain_id)?;
    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;

    let proofs = prove_signing_shapes(
        &provider,
        &resolved.wallet,
        orchestrator,
        vault,
        bot,
    )
    .await?;

    for proof in &proofs {
        println!(
            "Signed (not broadcast): {} -> {} as {}",
            proof.label, proof.to, proof.tx_hash
        );
    }
    println!(
        "Turnkey policy allows all {} orchestrator shapes for {} from {bot}.",
        proofs.len(),
        args.underlying
    );

    Ok(())
}

/// Resolves the orchestrator address from the TOML config file — the single
/// source the onboarding subcommands accept, never a typed argument. A
/// missing section is an actionable error rather than a default, and a zero
/// address is refused: the config may stay dark (no asset needs
/// `vault_mode = "orchestrator"`) and still carry the address these commands
/// work against.
fn required_orchestrator_address(config: &Path) -> anyhow::Result<Address> {
    let vault_modes = load_vault_mode_config(config)?;
    orchestrator_address_from(&vault_modes, config)
}

fn orchestrator_address_from(
    vault_modes: &VaultModeConfig,
    config: &Path,
) -> anyhow::Result<Address> {
    let Some(orchestrator) = vault_modes.orchestrator_address() else {
        anyhow::bail!(
            "{} has no [orchestrator].address; add the section — the config \
             may stay dark (no asset needs vault_mode = \"orchestrator\")",
            config.display()
        );
    };

    // No zero-address guard here: `load_vault_mode_config` already refuses a
    // zero `[orchestrator].address` at parse time, for the service and these
    // commands alike.
    Ok(orchestrator)
}

/// Resolves which assets the preflight checks. `--asset` narrows to the
/// named symbols; without it the scope defaults to the enabled listings
/// whose configured `vault_mode` resolves to orchestrator — the assets the
/// rollout is actually cutting over — so the incremental one-asset-at-a-time
/// cutover never demands approvals for assets that stay vault-direct. A
/// filter symbol with no enabled listing on the network is an error rather
/// than silently skipped — a typo'd symbol must not produce a READY verdict
/// that never checked the intended asset.
async fn preflight_assets(
    pool: &Pool<Sqlite>,
    network: Network,
    filter: &[UnderlyingSymbol],
    vault_modes: &VaultModeConfig,
) -> anyhow::Result<Vec<(UnderlyingSymbol, Address)>> {
    let listed: Vec<(UnderlyingSymbol, Address)> = list_enabled_assets(pool)
        .await?
        .into_iter()
        .filter(|asset| asset.network == network)
        .map(|asset| (asset.underlying, asset.vault))
        .collect();

    if filter.is_empty() {
        if listed.is_empty() {
            anyhow::bail!("no enabled assets listed on {network}");
        }
        let orchestrator_scoped: Vec<(UnderlyingSymbol, Address)> = listed
            .into_iter()
            .filter(|(underlying, _)| {
                matches!(
                    vault_modes.mode_for(underlying),
                    VaultMode::Orchestrator { .. }
                )
            })
            .collect();
        if orchestrator_scoped.is_empty() {
            anyhow::bail!(
                "no enabled asset on {network} is configured for \
                 orchestrator mode; pass --asset to check an asset ahead of \
                 its cutover"
            );
        }
        return Ok(orchestrator_scoped);
    }

    let missing = filter
        .iter()
        .filter(|wanted| {
            listed.iter().all(|(underlying, _)| underlying != *wanted)
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        anyhow::bail!("not enabled on {network}: {}", missing.join(", "));
    }

    Ok(listed
        .into_iter()
        .filter(|(underlying, _)| filter.contains(underlying))
        .collect())
}

/// Runs the authentication A/B probe against the environment's API host and
/// prints each attempt's status and verbatim body.
async fn run_fireblocks_auth_probe(
    args: FireblocksAuthProbeArgs,
) -> anyhow::Result<()> {
    let config = args.fireblocks.into_config()?;
    let base_url = match config.environment {
        Environment::Production => "https://api.fireblocks.io",
        Environment::Sandbox => "https://sandbox-api.fireblocks.io",
    };

    println!("Probing {base_url} with the service's credential pair");
    for report in probe_auth_pair(base_url, &config).await? {
        println!(
            "exp = iat + {}s -> HTTP {}",
            report.expiry_seconds, report.status
        );
        println!("{}", report.body);
    }

    Ok(())
}

/// Connects to the RPC and cross-checks the chain it reports against
/// `--chain-id`.
///
/// Reached only once the operator has confirmed intent, so an `--rpc-url` typo
/// cannot surface before they have decided. The RPC is the authority on which
/// chain the transfer lands on, but only the operator knows which one they
/// meant; disagreement means the vault address, the inventory key, or the
/// endpoint is wrong — and a deterministic deployment can put the same vault
/// address on both chains, so reaching the contract proves nothing.
async fn verified_chain_id(
    rpc_url: &Url,
    expected_chain_id: u64,
) -> anyhow::Result<u64> {
    let chain_id = ProviderBuilder::new()
        .connect(rpc_url.as_str())
        .await?
        .get_chain_id()
        .await?;

    if chain_id != expected_chain_id {
        anyhow::bail!(
            "--chain-id is {expected_chain_id} but {rpc_url} reports chain \
             {chain_id}"
        );
    }

    Ok(chain_id)
}

enum AssetAction {
    Freeze,
    Unfreeze,
    Status,
}

/// Connects to the store, prints the resolved database so the operator can
/// confirm they are acting on the intended store, and runs the action with the
/// real stdin confirmation prompt.
async fn run_asset_command(
    action: AssetAction,
    args: &AssetArgs,
) -> anyhow::Result<()> {
    println!("Using database: {}", args.database_url);

    let admin =
        AssetAdmin::connect(&args.database_url, args.database_max_connections)
            .await?;

    execute(&admin, action, &args.underlying, prompt_confirm).await
}

/// Orchestrates a single action against an already-connected admin. The
/// confirmation is injected so the abort/confirm branches are unit-testable
/// without driving real stdin. Aborting a mutation returns an error (non-zero
/// exit) so automation can distinguish "operator declined" from "done".
async fn execute(
    admin: &AssetAdmin,
    action: AssetAction,
    underlying: &UnderlyingSymbol,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    // Display the current status for the operator to confirm against, and reject
    // an underlying with no listing up front. This snapshot only drives the
    // prompt and the not-found check — the freeze/unfreeze decision is NOT
    // derived from it (see `freeze`/`unfreeze`), so a concurrent write landing
    // in the confirmation window can never leave the underlying in the wrong
    // persisted state.
    let report = admin.status(underlying).await?.ok_or_else(|| {
        AssetAdminError::NotFound { underlying: underlying.clone() }
    })?;
    println!("{report}");

    match action {
        AssetAction::Status => Ok(()),
        AssetAction::Freeze => {
            if !confirm(&format!("Freeze {underlying} on all networks?"))? {
                anyhow::bail!("aborted by operator");
            }
            match admin.freeze(underlying).await? {
                FreezeOutcome::Froze => {
                    println!("Froze {underlying} on all networks.");
                }
                FreezeOutcome::AlreadyFrozen => {
                    println!("{underlying} was already frozen.");
                }
            }
            Ok(())
        }
        AssetAction::Unfreeze => {
            if !confirm(&format!("Unfreeze {underlying} on all networks?"))? {
                anyhow::bail!("aborted by operator");
            }
            match admin.unfreeze(underlying).await? {
                UnfreezeOutcome::Unfroze => {
                    println!("Unfroze {underlying} on all networks.");
                }
                UnfreezeOutcome::AlreadyEnabled => {
                    println!("{underlying} was already enabled.");
                }
            }
            Ok(())
        }
    }
}

/// Issuer-host admin for freezing/unfreezing supported underlyings.
///
/// Opens the same SQLite event store the server uses and dispatches the CQRS
/// `Freeze` / `Unfreeze` commands through the event-sorcery `Store` — never
/// writing the `events` table directly.
pub(crate) struct AssetAdmin {
    store: Arc<Store<Underlying>>,
    pool: Pool<Sqlite>,
}

/// Outcome of a freeze request, so the caller can report an idempotent no-op
/// distinctly from an actual state change. An underlying with no listing is an
/// `AssetAdminError::NotFound`, not an outcome: `execute` rejects unknown
/// underlyings up front, so `freeze` only runs against one that exists.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FreezeOutcome {
    Froze,
    AlreadyFrozen,
}

/// Outcome of an unfreeze request. An underlying with no listing is an
/// `AssetAdminError::NotFound`, not an outcome (see `FreezeOutcome`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UnfreezeOutcome {
    Unfroze,
    AlreadyEnabled,
}

/// An underlying's freeze status, formatted for the CLI.
#[derive(Debug)]
pub(crate) struct AssetStatusReport {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) status: AssetStatus,
}

impl std::fmt::Display for AssetStatusReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self.status {
            AssetStatus::Frozen => "frozen",
            AssetStatus::Enabled => "enabled",
        };
        write!(f, "{} is {state} (applies to all networks)", self.underlying)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AssetAdminError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("failed to read asset view: {0}")]
    View(#[from] TokenizedAssetViewError),
    #[error("failed to read underlying freeze view: {0}")]
    UnderlyingView(#[from] UnderlyingViewError),
    #[error("event store reconcile error: {0}")]
    Reconcile(#[from] ReconcileError),
    #[error("aggregate error: {0}")]
    Aggregate(Box<AggregateError<LifecycleError<Underlying>>>),
    #[error("{underlying} is not a supported tokenized asset on any network")]
    NotFound { underlying: UnderlyingSymbol },
}

// `Store::send` yields an un-boxed `AggregateError`; box it on conversion so the
// enum variant stays small (the error is large) while `?` still works at the
// call site without a hand-rolled `.map_err(Box::new)`.
impl From<AggregateError<LifecycleError<Underlying>>> for AssetAdminError {
    fn from(error: AggregateError<LifecycleError<Underlying>>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

impl AssetAdmin {
    /// Connects to the SQLite store at `db`, applying migrations so the command
    /// can run standalone on the issuer host. The 5s busy timeout pins sqlx's
    /// default: it makes SQLite wait on `SQLITE_BUSY` while the server holds the
    /// write lock instead of failing immediately. It does NOT cover an
    /// event-sorcery optimistic-concurrency conflict (a UNIQUE collision on the
    /// events PK), which `Store::send` surfaces as an error the operator re-runs.
    pub(crate) async fn connect(
        db: &str,
        max_connections: u32,
    ) -> Result<Self, AssetAdminError> {
        let connect_options = SqliteConnectOptions::from_str(db)?
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(connect_options)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        // The server heals `tokenized_asset_view` at startup, but this CLI
        // runs while the service is deliberately stopped — and production
        // carried the view empty for weeks without the running service's
        // read paths noticing. Run the same startup hygiene the server does
        // (schema reconciliation clears snapshots and stale canonical
        // projections on a version change) before the builds, so a CLI run
        // after a schema bump cannot advance the recorded version while
        // leaving incompatible view rows behind; the builds then run the
        // same catch-up, so every CLI read of the view sees the listings the
        // event log actually holds.
        prepare_event_sourced_startup::<TokenizedAsset>(&pool).await?;
        prepare_event_sourced_startup::<Underlying>(&pool).await?;

        let (_listing_store, _listing_projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone()).build(()).await?;

        let (store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone()).build(()).await?;

        Ok(Self { store, pool })
    }

    /// Reads the current freeze status, or `None` if the underlying has no
    /// listing on any network.
    pub(crate) async fn status(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<Option<AssetStatusReport>, AssetAdminError> {
        if !underlying_has_listing(&self.pool, underlying).await? {
            return Ok(None);
        }

        let status = load_freeze_status(&self.pool, underlying).await?;

        Ok(Some(AssetStatusReport { underlying: underlying.clone(), status }))
    }

    /// Freezes the underlying on all networks. Always dispatches `Freeze`
    /// through the store so the aggregate — the source of truth — decides the
    /// final state; an already-frozen underlying is a zero-event no-op there,
    /// so it is guaranteed frozen afterwards even if a concurrent writer
    /// changed it since the operator's status read. The returned
    /// `FreezeOutcome` only labels the message from a status read taken
    /// immediately before dispatch: it is best-effort under a concurrent
    /// write, but the persisted state is always correct. Deriving the label
    /// from the live store (not a snapshot passed in by the caller) is what
    /// closes the read-then-confirm-then-dispatch TOCTOU where a stale
    /// "already frozen" read would otherwise skip the dispatch.
    pub(crate) async fn freeze(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<FreezeOutcome, AssetAdminError> {
        let already_frozen = matches!(
            self.status(underlying).await?.map(|report| report.status),
            Some(AssetStatus::Frozen)
        );

        self.store
            .send(
                underlying,
                UnderlyingCommand::Freeze { underlying: underlying.clone() },
            )
            .await?;

        Ok(if already_frozen {
            FreezeOutcome::AlreadyFrozen
        } else {
            FreezeOutcome::Froze
        })
    }

    /// Unfreezes the underlying. Always dispatches `Unfreeze` through the store
    /// so the aggregate decides the final state; an already-enabled underlying
    /// is a zero-event no-op there. The returned `UnfreezeOutcome` labels the
    /// message from a pre-dispatch status read (best-effort under a concurrent
    /// write); the persisted state is always correct. See `freeze` for why the
    /// label is derived from the live store rather than a caller-supplied
    /// snapshot.
    pub(crate) async fn unfreeze(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<UnfreezeOutcome, AssetAdminError> {
        let already_enabled = matches!(
            self.status(underlying).await?.map(|report| report.status),
            Some(AssetStatus::Enabled)
        );

        self.store
            .send(
                underlying,
                UnderlyingCommand::Unfreeze { underlying: underlying.clone() },
            )
            .await?;

        Ok(if already_enabled {
            UnfreezeOutcome::AlreadyEnabled
        } else {
            UnfreezeOutcome::Unfroze
        })
    }
}

/// Validates the database URL uses the `sqlite:` scheme so a wrong env value
/// (e.g. an `http://` URL) fails fast with a clear message rather than an opaque
/// driver error deep inside sqlx. Returns the string unchanged so both the CLI
/// and the server hand sqlx identical bytes.
fn parse_sqlite_url(value: &str) -> Result<String, String> {
    if value.starts_with("sqlite:") {
        Ok(value.to_string())
    } else {
        Err(format!("database URL must use the sqlite: scheme, got: {value}"))
    }
}

fn prompt_confirm(prompt: &str) -> io::Result<bool> {
    print!("{prompt} [y/N] ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(parse_confirmation(&input))
}

/// Confirmation accepts `y`/`yes` case-insensitively (after trimming);
/// everything else — including empty input and EOF — declines.
fn parse_confirmation(input: &str) -> bool {
    let trimmed = input.trim();
    trimmed.eq_ignore_ascii_case("y") || trimmed.eq_ignore_ascii_case("yes")
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{U256, address, b256};
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing_test::traced_test;

    use super::*;
    use crate::Quantity;
    use crate::redemption::{
        BurnFailureClassification, BurnRecord, RedemptionEvent,
    };
    use crate::test_utils::{LocalEvm, logs_contain_at};
    use crate::tokenized_asset::{
        AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetEvent,
    };

    const TEST_SIGNER_KEY: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000001";

    /// The one vault every CLI test seeds. `seed_custody_at` keys the
    /// aggregate with this same vault (and Base's chain id), so the seeded
    /// custody can never silently desynchronise from the listing the CLI
    /// resolves.
    const TEST_VAULT: Address =
        address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    /// Seeds one listing per given network for `underlying`, then hands back an
    /// admin over the same pool — mirroring an issuer host where the server
    /// maintains the listing view and the CLI acts on the freeze store.
    async fn admin_with_asset(
        underlying: &str,
        networks: &[Network],
    ) -> AssetAdmin {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (listing_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let underlying = UnderlyingSymbol::new(underlying).unwrap();
        for network in networks {
            let key = AssetKey::new(underlying.clone(), *network);
            listing_store
                .send(
                    &key,
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!("t{underlying}")),
                        network: *network,
                        vault: TEST_VAULT,
                    },
                )
                .await
                .expect("Failed to add asset");
        }

        let (store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build underlying store");

        AssetAdmin { store, pool }
    }

    #[traced_test]
    #[tokio::test]
    async fn freeze_then_unfreeze_round_trip() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        let report =
            admin.status(&underlying).await.unwrap().expect("asset exists");
        assert_eq!(report.status, AssetStatus::Enabled);
        assert_eq!(
            format!("{report}"),
            "SGOV is enabled (applies to all networks)"
        );

        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::Froze
        );
        let frozen = admin.status(&underlying).await.unwrap().expect("exists");
        assert_eq!(frozen.status, AssetStatus::Frozen);
        assert_eq!(
            format!("{frozen}"),
            "SGOV is frozen (applies to all networks)"
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing underlying across all networks", "SGOV"]
        ));

        assert_eq!(
            admin.unfreeze(&underlying).await.unwrap(),
            UnfreezeOutcome::Unfroze
        );
        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Unfreezing underlying across all networks", "SGOV"]
        ));
    }

    // One freeze covers every listing of the underlying: with listings on two
    // networks, a single freeze is what the status (and the mint gate, which
    // reads the same view) reports for both.
    #[tokio::test]
    async fn freeze_covers_every_network_listing() {
        let admin =
            admin_with_asset("SGOV", &[Network::Base, Network::Ethereum]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::Froze
        );

        assert_eq!(
            load_freeze_status(&admin.pool, &underlying).await.unwrap(),
            AssetStatus::Frozen,
            "the underlying-scoped status applies to all network listings"
        );
    }

    #[tokio::test]
    async fn freeze_and_unfreeze_report_idempotent_no_ops() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        // A second freeze of an already-frozen underlying (and a second
        // unfreeze of an already-enabled one) is a zero-event no-op the
        // aggregate dedups, and is reported as the AlreadyFrozen /
        // AlreadyEnabled label.
        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::Froze
        );
        assert_eq!(
            admin.freeze(&underlying).await.unwrap(),
            FreezeOutcome::AlreadyFrozen
        );

        assert_eq!(
            admin.unfreeze(&underlying).await.unwrap(),
            UnfreezeOutcome::Unfroze
        );
        assert_eq!(
            admin.unfreeze(&underlying).await.unwrap(),
            UnfreezeOutcome::AlreadyEnabled
        );
    }

    #[tokio::test]
    async fn status_is_none_for_unknown_asset() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        assert!(
            admin
                .status(&UnderlyingSymbol::new("UNKNOWN").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn execute_rejects_unknown_asset() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let unknown = UnderlyingSymbol::new("UNKNOWN").unwrap();

        // The not-found rejection is `execute`'s entry-point behavior for all
        // three subcommands; assert the operator-facing message.
        let err = execute(&admin, AssetAction::Freeze, &unknown, |_| Ok(true))
            .await
            .expect_err("an unknown asset must be rejected");
        assert!(
            err.to_string().contains("is not a supported tokenized asset"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn execute_freeze_aborts_without_dispatching_when_declined() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        let result =
            execute(&admin, AssetAction::Freeze, &underlying, |_| Ok(false))
                .await;

        assert!(result.is_err(), "declined freeze must return an error");
        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled,
            "a declined freeze must not change state"
        );
    }

    #[tokio::test]
    async fn execute_freeze_dispatches_when_confirmed() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        execute(&admin, AssetAction::Freeze, &underlying, |_| Ok(true))
            .await
            .expect("confirmed freeze succeeds");

        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Frozen,
            "a confirmed freeze must change state"
        );
    }

    #[tokio::test]
    async fn execute_unfreeze_aborts_without_dispatching_when_declined() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();
        admin.freeze(&underlying).await.expect("freeze succeeds");

        let result =
            execute(&admin, AssetAction::Unfreeze, &underlying, |_| Ok(false))
                .await;

        assert!(result.is_err(), "declined unfreeze must return an error");
        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Frozen,
            "a declined unfreeze must not change state"
        );
    }

    #[tokio::test]
    async fn execute_unfreeze_dispatches_when_confirmed() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();
        admin.freeze(&underlying).await.expect("freeze succeeds");

        execute(&admin, AssetAction::Unfreeze, &underlying, |_| Ok(true))
            .await
            .expect("confirmed unfreeze succeeds");

        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled,
            "a confirmed unfreeze must change state"
        );
    }

    #[tokio::test]
    async fn execute_status_never_prompts_or_mutates() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let underlying = UnderlyingSymbol::new("SGOV").unwrap();

        execute(&admin, AssetAction::Status, &underlying, |_| {
            panic!("status must not prompt for confirmation")
        })
        .await
        .expect("status succeeds");

        assert_eq!(
            admin.status(&underlying).await.unwrap().expect("exists").status,
            AssetStatus::Enabled
        );
    }

    #[test]
    fn parse_confirmation_accepts_yes_case_insensitively() {
        for affirmative in ["y", "Y", "yes", "Yes", "YES", " y ", "yEs\n"] {
            assert!(
                parse_confirmation(affirmative),
                "{affirmative:?} should confirm"
            );
        }

        for decline in ["", "n", "N", "no", "yep", "  ", "\n"] {
            assert!(!parse_confirmation(decline), "{decline:?} should decline");
        }
    }

    #[test]
    fn issuer_cli_uppercases_underlying_and_rejects_blank() {
        let IssuerCli { command: IssuerCommand::Freeze(args) } =
            IssuerCli::try_parse_from(["issuer", "freeze", " sgov "]).unwrap()
        else {
            panic!("expected freeze command");
        };
        assert_eq!(
            args.underlying,
            UnderlyingSymbol::new("SGOV").unwrap(),
            "input is trimmed and upper-cased to the stored symbol"
        );

        for blank in ["", "   "] {
            assert!(
                IssuerCli::try_parse_from(["issuer", "freeze", blank]).is_err(),
                "{blank:?} must be rejected at parse time"
            );
        }
    }

    #[test]
    fn issuer_cli_rejects_non_sqlite_url() {
        assert!(
            IssuerCli::try_parse_from([
                "issuer",
                "freeze",
                "SGOV",
                "--database-url",
                "http://example.com/db",
            ])
            .is_err(),
            "non-sqlite database URL must be rejected at parse time"
        );
    }

    /// Seeds a listing into a file-backed store, since the commands under
    /// test open their own pool from the URL and so cannot share an
    /// in-memory one.
    async fn seed_listing_at(database_url: &str, underlying: &str) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
            .expect("Failed to open database");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (listing_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let underlying = UnderlyingSymbol::new(underlying).unwrap();
        listing_store
            .send(
                &AssetKey::new(underlying.clone(), Network::Base),
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new(format!("t{underlying}")),
                    network: Network::Base,
                    vault: TEST_VAULT,
                },
            )
            .await
            .expect("Failed to add asset");
        pool.close().await;
    }

    /// Production carried `tokenized_asset_view` empty for six weeks: a
    /// migration dropped and recreated the table, and only the running
    /// server's startup healed views — so listings written by an earlier
    /// binary exist as events with no view rows, and this CLI (which runs
    /// while the service is deliberately stopped) read an empty view.
    /// Connecting must heal the listing view from the event log.
    #[tokio::test]
    async fn connect_heals_an_empty_listing_view_from_events() {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("empty-view.db").display()
        );

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let underlying = UnderlyingSymbol::new("AMAT").unwrap();
        let event = TokenizedAssetEvent::Added {
            underlying: underlying.clone(),
            token: TokenSymbol::new("tAMAT"),
            network: Network::Base,
            vault: TEST_VAULT,
            added_at: Utc::now(),
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
            VALUES ('TokenizedAsset', ?, 1, ?, '1.0', ?, '{}')
            ",
        )
        .bind(AssetKey::new(underlying.clone(), Network::Base).to_string())
        .bind(event.event_type())
        .bind(serde_json::to_string(&event).unwrap())
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let admin = AssetAdmin::connect(&database_url, 1).await.unwrap();

        let vault =
            find_vault(&admin.pool, &underlying, &Network::Base).await.unwrap();
        assert_eq!(
            vault,
            Some(TEST_VAULT),
            "a listing present only in the event log must be readable after \
             connect"
        );
    }

    /// Seeds a redemption detected on Base, so a force-complete invocation
    /// naming another network contradicts the event history.
    async fn seed_detected_redemption_at(
        database_url: &str,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
            .expect("Failed to open database");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let events = [
            RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("AMAT").unwrap(),
                token: TokenSymbol::new("tAMAT"),
                network: Network::Base,
                wallet: address!("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
                quantity: Quantity::new(Decimal::new(4, 2)),
                tx_hash: b256!(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                ),
                block_number: 30_000_000,
                detected_at: Utc::now(),
                burn_mode: crate::VaultMode::VaultDirect,
            },
            RedemptionEvent::BurningFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "burn transaction polling timed out".to_string(),
                failed_at: Utc::now(),
                tx_id: None,
                planned_burns: vec![BurnRecord {
                    receipt_id: U256::from(3),
                    shares_burned: U256::from(40_000_000_000_000_000_u64),
                }],
                classification: BurnFailureClassification::Unclassified,
            },
        ];
        for (index, event) in events.iter().enumerate() {
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
                VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(issuer_request_id.to_string())
            .bind(i64::try_from(index).unwrap() + 1)
            .bind(event.event_type())
            .bind(serde_json::to_string(event).unwrap())
            .execute(&pool)
            .await
            .expect("Failed to seed redemption event");
        }
    }

    fn force_complete_args(
        issuer_request_id: &str,
        network: &str,
        chain_id: &str,
        database_url: &str,
    ) -> ForceCompleteRedemptionArgs {
        let cli = IssuerCli::try_parse_from([
            "issuer",
            "force-complete-redemption",
            issuer_request_id,
            "--burn-tx-hash",
            "0x5555555555555555555555555555555555555555555555555555555555555555",
            "--network",
            network,
            "--rpc-url",
            "http://127.0.0.1:1",
            "--chain-id",
            chain_id,
            "--reason",
            "operator verified the landed burn on-chain",
            "--database-url",
            database_url,
        ])
        .expect("arguments parse");

        let IssuerCommand::ForceCompleteRedemption(args) = cli.command else {
            panic!("expected the force-complete-redemption subcommand")
        };

        *args
    }

    /// The two independent statements of where the command runs must agree
    /// before anything is touched — the database and RPC endpoint are
    /// unreachable on purpose.
    #[tokio::test]
    async fn force_complete_refuses_a_chain_id_that_contradicts_the_network() {
        let args = force_complete_args(
            "red-00000001",
            "base",
            "1",
            "sqlite:unreachable.db",
        );

        let error = run_force_complete_redemption(args, |_| Ok(true))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("--chain-id is 1"),
            "a contradicting chain id must be refused, got {error}"
        );
    }

    /// A force-complete naming a network other than the one the redemption
    /// was detected on must be refused from the event history alone, before
    /// any chain access — the RPC endpoint is unreachable on purpose.
    #[tokio::test]
    async fn force_complete_refuses_a_network_that_contradicts_the_history() {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("history.db").display()
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_detected_redemption_at(&database_url, &issuer_request_id).await;

        let args = force_complete_args(
            &issuer_request_id.to_string(),
            "ethereum",
            "1",
            &database_url,
        );

        let error = run_force_complete_redemption(args, |_| Ok(true))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("was detected on"),
            "a network contradicting the history must be refused, got {error}"
        );
    }

    /// Mirrors [`seed_listing_at`] but at a caller-chosen vault, so the
    /// preflight end-to-end test can list the vault Anvil actually deployed.
    async fn seed_listing_at_vault(
        database_url: &str,
        underlying: &str,
        vault: Address,
    ) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
            .expect("Failed to open database");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (listing_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let underlying = UnderlyingSymbol::new(underlying).unwrap();
        listing_store
            .send(
                &AssetKey::new(underlying.clone(), Network::Base),
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new(format!("t{underlying}")),
                    network: Network::Base,
                    vault,
                },
            )
            .await
            .expect("Failed to add asset");
        pool.close().await;
    }

    #[test]
    fn orchestrator_preflight_parses_and_uppercases_asset_filters() {
        let cli = IssuerCli::try_parse_from([
            "issuer",
            "orchestrator-preflight",
            "--config",
            "issuance-config.toml",
            "--network",
            "base",
            "--asset",
            "rklb",
            "--asset",
            "SGOV",
            "--chain-id",
            "8453",
            "--rpc-url",
            "http://127.0.0.1:1",
            "--turnkey-org-id",
            "org-id",
            "--turnkey-api-private-key",
            "api-key",
            "--turnkey-address",
            "0x00000000000000000000000000000000000000cc",
            "--database-url",
            "sqlite::memory:",
        ])
        .expect("arguments parse");

        let IssuerCommand::OrchestratorPreflight(args) = cli.command else {
            panic!("expected the orchestrator-preflight subcommand")
        };

        assert_eq!(args.config, PathBuf::from("issuance-config.toml"));
        assert_eq!(args.network, Network::Base);
        assert_eq!(
            args.assets,
            vec![
                UnderlyingSymbol::new("RKLB").unwrap(),
                UnderlyingSymbol::new("SGOV").unwrap()
            ]
        );
    }

    #[test]
    fn orchestrator_preflight_requires_a_config_file() {
        let Err(error) = IssuerCli::try_parse_from([
            "issuer",
            "orchestrator-preflight",
            "--network",
            "base",
            "--chain-id",
            "8453",
            "--rpc-url",
            "http://127.0.0.1:1",
            "--turnkey-org-id",
            "org-id",
            "--turnkey-api-private-key",
            "api-key",
            "--turnkey-address",
            "0x00000000000000000000000000000000000000cc",
            "--database-url",
            "sqlite::memory:",
        ]) else {
            panic!(
                "omitting --config must fail at parse time — the orchestrator \
                 address has no other source"
            )
        };

        assert!(
            error.to_string().contains("--config"),
            "the parse failure must name the missing --config, got {error}"
        );
    }

    /// A vault-modes config resolving every asset to orchestrator mode, so
    /// the default preflight scope covers all listings.
    fn all_orchestrator_modes() -> VaultModeConfig {
        VaultModeConfig::new(
            std::collections::HashMap::new(),
            VaultMode::Orchestrator { address: Address::repeat_byte(0xdd) },
        )
    }

    #[tokio::test]
    async fn preflight_assets_lists_only_the_requested_network() {
        let admin =
            admin_with_asset("SGOV", &[Network::Base, Network::Ethereum]).await;

        let assets = preflight_assets(
            &admin.pool,
            Network::Base,
            &[],
            &all_orchestrator_modes(),
        )
        .await
        .unwrap();

        assert_eq!(
            assets,
            vec![(UnderlyingSymbol::new("SGOV").unwrap(), TEST_VAULT)]
        );
    }

    /// The default scope is the assets configured for orchestrator mode —
    /// the incremental cutover must not demand approvals for assets staying
    /// vault-direct, and a scope that matches nothing is an error, never a
    /// vacuous READY.
    #[tokio::test]
    async fn preflight_assets_default_scope_is_orchestrator_configured() {
        let admin =
            admin_with_asset("SGOV", &[Network::Base, Network::Ethereum]).await;

        let sgov_only = VaultModeConfig::new(
            std::collections::HashMap::from([(
                "SGOV".to_string(),
                VaultMode::Orchestrator { address: Address::repeat_byte(0xdd) },
            )]),
            VaultMode::VaultDirect,
        );
        let assets =
            preflight_assets(&admin.pool, Network::Base, &[], &sgov_only)
                .await
                .unwrap();
        assert_eq!(
            assets,
            vec![(UnderlyingSymbol::new("SGOV").unwrap(), TEST_VAULT)]
        );

        let all_vault_direct = VaultModeConfig::new(
            std::collections::HashMap::new(),
            VaultMode::VaultDirect,
        );
        let error = preflight_assets(
            &admin.pool,
            Network::Base,
            &[],
            &all_vault_direct,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no enabled asset on base is configured"),
            "an all-vault-direct config must refuse the default scope, got \
             {error}"
        );
    }

    #[tokio::test]
    async fn preflight_assets_filter_narrows_and_rejects_unknown_symbols() {
        let admin = admin_with_asset("SGOV", &[Network::Base]).await;
        let rklb_vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let (listing_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(admin.pool.clone())
                .build(())
                .await
                .unwrap();
        let rklb = UnderlyingSymbol::new("RKLB").unwrap();
        listing_store
            .send(
                &AssetKey::new(rklb.clone(), Network::Base),
                TokenizedAssetCommand::Add {
                    underlying: rklb.clone(),
                    token: TokenSymbol::new("tRKLB".to_string()),
                    network: Network::Base,
                    vault: rklb_vault,
                },
            )
            .await
            .unwrap();

        let narrowed = preflight_assets(
            &admin.pool,
            Network::Base,
            std::slice::from_ref(&rklb),
            &all_orchestrator_modes(),
        )
        .await
        .unwrap();
        assert_eq!(narrowed, vec![(rklb, rklb_vault)]);

        let error = preflight_assets(
            &admin.pool,
            Network::Base,
            &[UnderlyingSymbol::new("TSLA").unwrap()],
            &all_orchestrator_modes(),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("TSLA"),
            "the error must name the unknown symbol, got {error}"
        );
    }

    #[tokio::test]
    async fn preflight_assets_errors_when_nothing_is_listed_on_the_network() {
        let admin = admin_with_asset("SGOV", &[Network::Ethereum]).await;

        let error = preflight_assets(
            &admin.pool,
            Network::Base,
            &[],
            &all_orchestrator_modes(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("no enabled assets"),
            "an empty listing must be an error, got {error}"
        );
    }

    /// The full preflight glue against a real orchestrator on Anvil: NOT
    /// READY (non-zero exit) while the approval is missing, READY after it is
    /// executed. Turnkey credentials are parse-only stand-ins — the preflight
    /// reads the wallet address from the configuration and never signs.
    #[tokio::test]
    async fn orchestrator_preflight_end_to_end_gates_on_readiness() {
        let evm = LocalEvm::with_chain_id(8453).await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();

        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("preflight.db").display()
        );
        seed_listing_at_vault(&database_url, "RKLB", evm.vault_address).await;

        let config_path = directory.path().join("issuance-config.toml");
        // RKLB is configured for orchestrator mode so the default scope —
        // the assets actually cutting over — covers it.
        std::fs::write(
            &config_path,
            format!(
                "[orchestrator]\naddress = \"{orchestrator}\"\n\n\
                 [assets.RKLB]\nvault_mode = \"orchestrator\"\n"
            ),
        )
        .unwrap();

        let parse_args = |database_url: &str| {
            let cli = IssuerCli::try_parse_from([
                "issuer",
                "orchestrator-preflight",
                "--config",
                config_path.to_str().unwrap(),
                "--network",
                "base",
                "--chain-id",
                "8453",
                "--rpc-url",
                &evm.endpoint,
                "--turnkey-org-id",
                "org-id",
                "--turnkey-api-private-key",
                "api-key",
                "--turnkey-address",
                &evm.wallet_address.to_string(),
                "--database-url",
                database_url,
            ])
            .expect("arguments parse");
            let IssuerCommand::OrchestratorPreflight(args) = cli.command else {
                panic!("expected the orchestrator-preflight subcommand")
            };
            *args
        };

        let error = run_orchestrator_preflight(parse_args(&database_url))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("preflight FAILED"),
            "a missing approval must fail the preflight, got {error}"
        );

        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        OffchainAssetReceiptVault::new(evm.vault_address, &provider)
            .approve(orchestrator, U256::MAX)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        run_orchestrator_preflight(parse_args(&database_url))
            .await
            .expect("preflight must pass once the approval is unlimited");
    }

    fn approve_orchestrator_args(
        config_path: &str,
        database_url: &str,
        signer_flags: &[&str],
    ) -> ApproveOrchestratorArgs {
        let mut command_line = vec![
            "issuer",
            "approve-orchestrator",
            "rklb",
            "--config",
            config_path,
            "--network",
            "base",
            "--chain-id",
            "8453",
            "--rpc-url",
            "http://127.0.0.1:1",
            "--database-url",
            database_url,
        ];
        command_line.extend_from_slice(signer_flags);

        let cli =
            IssuerCli::try_parse_from(command_line).expect("arguments parse");
        let IssuerCommand::ApproveOrchestrator(args) = cli.command else {
            panic!("expected the approve-orchestrator subcommand")
        };
        *args
    }

    const TURNKEY_FLAGS: [&str; 6] = [
        "--turnkey-org-id",
        "org-id",
        "--turnkey-api-private-key",
        "api-key",
        "--turnkey-address",
        "0x00000000000000000000000000000000000000cc",
    ];

    fn write_orchestrator_config(directory: &std::path::Path) -> PathBuf {
        let config_path = directory.join("issuance-config.toml");
        std::fs::write(
            &config_path,
            "[orchestrator]\n\
             address = \"0x1234567890abcdef1234567890abcdef12345678\"\n",
        )
        .unwrap();
        config_path
    }

    #[test]
    fn approve_orchestrator_parses_and_uppercases_the_underlying() {
        let args = approve_orchestrator_args(
            "issuance-config.toml",
            "sqlite::memory:",
            &TURNKEY_FLAGS,
        );

        assert_eq!(args.underlying, UnderlyingSymbol::new("RKLB").unwrap());
        assert_eq!(args.network, Network::Base);
        assert_eq!(args.config, PathBuf::from("issuance-config.toml"));
    }

    /// A declined confirmation must abort before any network call. The
    /// unreachable `--rpc-url` is the assertion: if the prompt were bypassed,
    /// or the chain-id round-trip ran first, this would fail on the endpoint
    /// rather than on the operator's decision.
    #[tokio::test]
    async fn approve_orchestrator_aborts_without_touching_the_chain_when_declined()
     {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("approve-decline.db").display()
        );
        seed_listing_at(&database_url, "RKLB").await;
        let config_path = write_orchestrator_config(directory.path());

        let args = approve_orchestrator_args(
            config_path.to_str().unwrap(),
            &database_url,
            &TURNKEY_FLAGS,
        );

        let error =
            run_approve_orchestrator(args, |_| Ok(false)).await.unwrap_err();

        assert!(
            error.to_string().contains("aborted by operator"),
            "a declined confirmation must abort before any RPC, got {error}"
        );
    }

    /// The approval must come from the Turnkey bot wallet — the wallet whose
    /// allowance production burns will spend — so a local key is refused
    /// outright instead of approving from an address production never uses.
    #[tokio::test]
    async fn approve_orchestrator_refuses_a_non_turnkey_signer() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = write_orchestrator_config(directory.path());

        let args = approve_orchestrator_args(
            config_path.to_str().unwrap(),
            "sqlite::memory:",
            &["--evm-private-key", TEST_SIGNER_KEY],
        );

        let error = run_approve_orchestrator(args, |_| {
            panic!("must refuse the signer before prompting")
        })
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("Turnkey signer configuration"),
            "the refusal must name the Turnkey requirement, got {error}"
        );
    }

    fn verify_signing_args(
        config_path: &str,
        signer_flags: &[&str],
    ) -> VerifyOrchestratorSigningArgs {
        let mut command_line = vec![
            "issuer",
            "verify-orchestrator-signing",
            "rklb",
            "--config",
            config_path,
            "--network",
            "base",
            "--chain-id",
            "8453",
            "--rpc-url",
            "http://127.0.0.1:1",
            "--database-url",
            "sqlite::memory:",
        ];
        command_line.extend_from_slice(signer_flags);

        let cli =
            IssuerCli::try_parse_from(command_line).expect("arguments parse");
        let IssuerCommand::VerifyOrchestratorSigning(args) = cli.command else {
            panic!("expected the verify-orchestrator-signing subcommand")
        };
        *args
    }

    #[test]
    fn verify_orchestrator_signing_parses_and_uppercases_the_underlying() {
        let args = verify_signing_args("issuance-config.toml", &TURNKEY_FLAGS);

        assert_eq!(args.underlying, UnderlyingSymbol::new("RKLB").unwrap());
        assert_eq!(args.network, Network::Base);
        assert_eq!(args.config, PathBuf::from("issuance-config.toml"));
    }

    /// Signing with a local key would trivially succeed and prove nothing
    /// about the Turnkey policy — the command must refuse rather than report
    /// a hollow pass.
    #[tokio::test]
    async fn verify_orchestrator_signing_refuses_a_non_turnkey_signer() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = write_orchestrator_config(directory.path());

        let args = verify_signing_args(
            config_path.to_str().unwrap(),
            &["--evm-private-key", TEST_SIGNER_KEY],
        );

        let error = run_verify_orchestrator_signing(args).await.unwrap_err();

        assert!(
            error.to_string().contains("Turnkey signer"),
            "the refusal must name the Turnkey requirement, got {error}"
        );
    }

    #[test]
    fn required_orchestrator_address_refuses_missing_and_zero() {
        let directory = tempfile::tempdir().unwrap();

        let dark = directory.path().join("dark.toml");
        std::fs::write(&dark, "# dark: no [orchestrator] section\n").unwrap();
        let error = required_orchestrator_address(&dark).unwrap_err();
        assert!(
            error.to_string().contains("[orchestrator].address"),
            "the refusal must name the missing key, got {error}"
        );

        // The zero address is refused by the config loader itself (before
        // this helper's own checks), with the same strict parse the service
        // applies — pinned here so it can never reach these commands.
        let zeroed = directory.path().join("zero.toml");
        std::fs::write(
            &zeroed,
            format!("[orchestrator]\naddress = \"{}\"\n", Address::ZERO),
        )
        .unwrap();
        let error = required_orchestrator_address(&zeroed).unwrap_err();
        assert!(
            error.to_string().contains("not a valid EVM address"),
            "a zero address must be refused by the config loader, got {error}"
        );
    }

    #[tokio::test]
    async fn verify_orchestrator_signing_requires_an_orchestrator_address() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("dark-config.toml");
        std::fs::write(&config_path, "# dark: no [orchestrator] section\n")
            .unwrap();

        let args =
            verify_signing_args(config_path.to_str().unwrap(), &TURNKEY_FLAGS);

        let error = run_verify_orchestrator_signing(args).await.unwrap_err();

        assert!(
            error.to_string().contains("[orchestrator].address"),
            "the refusal must name the missing address, got {error}"
        );
    }
}
