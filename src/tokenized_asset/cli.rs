use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use clap::{Args, Parser, Subcommand};
use event_sorcery::{
    AggregateError, LifecycleError, ReconcileError, Store, StoreBuilder,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::io::{self, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use super::view::{
    TokenizedAssetViewError, find_vault, underlying_has_listing,
};
use super::{TokenizedAsset, UnderlyingSymbol};
use crate::Network;
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::config::{
    DEFAULT_DATABASE_MAX_CONNECTIONS, DEFAULT_DATABASE_URL, LogLevel,
    setup_tracing,
};
use crate::fireblocks::auth_probe::probe_auth_pair;
use crate::fireblocks::{
    Environment, FireblocksEnv, FireblocksVaultService, fetch_vault_address,
};
use crate::prepare_event_sourced_startup;
use crate::receipt_inventory::migration::{
    CorroboratedRecipient, MigrationOutcome, VaultIdentity,
    confirm_custody_holder, migrate_vault_receipts,
    migrate_vault_receipts_via_fireblocks, recorded_custody_holder,
    recorded_migration_origin, rollback_gas_reserve, verify_rollback_signing,
};
use crate::receipt_inventory::{ReceiptInventory, load_inventory};
use crate::redemption::IssuerRedemptionRequestId;
use crate::redemption::force_complete::{
    VerifiedCompletion, ensure_burn_unclaimed, landed_burn_evidence,
    terminalize_and_settle, verify_landed_burn,
};
use crate::underlying::{
    AssetStatus, Underlying, UnderlyingCommand, UnderlyingViewError,
    load_freeze_status,
};
use crate::wallet::turnkey::{TurnkeyConfig, resolve_turnkey_signer};
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
    /// Move a vault's deposit receipts between the Fireblocks wallet and the
    /// Turnkey wallet, with the direction resolved from recorded custody.
    ///
    /// Custody at the retiring wallet runs the forward leg, signed either by
    /// Fireblocks RAW signing (default) or an explicitly selected emergency
    /// local key; custody at the Turnkey wallet runs the rollback, signed by
    /// Turnkey back to the independently corroborated recorded origin. No
    /// wallet address is ever typed.
    ///
    /// Temporary, for the Turnkey cutover: the issuer burns against receipts
    /// held by its own signing address, so rotating the signing backend strands
    /// them until custody follows. Remove once every vault has migrated.
    ///
    /// Unlike the freeze subcommands this is network-scoped, because receipts
    /// live in a single vault on a single chain.
    MigrateReceipts(Box<MigrateReceiptsArgs>),
    /// Record which wallet holds a vault's receipts, after verifying on-chain
    /// that it holds exactly every tracked balance.
    ///
    /// The bootstrap for deployments whose history predates custody tracking:
    /// the reconciliation guard treats unobserved custody as "a zero balance
    /// means spent", so every vault's holder must be on record before any
    /// service starts against a rotated wallet. The holder is fetched from the
    /// Fireblocks API — never typed — and recorded only if it holds every
    /// tracked balance exactly. Submits no transaction.
    ConfirmCustody(Box<ConfirmCustodyArgs>),
    /// Prove both custodian connections, before anything moves.
    ///
    /// Fireblocks: authenticates, derives the wallet, and resolves the
    /// whitelisted Receipt contract — proving credentials and authorization
    /// without submitting anything. Turnkey: signs the exact rollback-shaped
    /// transaction — every tracked receipt back from the Turnkey wallet to
    /// the current holder — WITHOUT broadcasting it, exercising the
    /// credentials, organization, address, and signing policy end to end. The
    /// custodian signs the forward transfer outside this binary, so a broken
    /// Turnkey setup discovered after the move would strand custody with no
    /// way back; this makes that undiscoverable state impossible to enter.
    /// Also reports both wallets' gas balances. With `--smoke`, additionally
    /// submits a zero-amount transfer through the full Fireblocks path — the
    /// only live-transaction step, and the full proof of the custodian's own
    /// signing path.
    VerifyCustodians(Box<VerifyCustodiansArgs>),
    /// Terminalize a Failed redemption whose burn already landed on-chain.
    /// For legacy custodian-era burns whose backend transaction id the
    /// current signing backend cannot look up: the operator supplies the
    /// on-chain transaction hash, and everything else is verified — the
    /// transaction must be a successful burn on the redemption's vault whose
    /// per-receipt withdrawals match the persisted burn plan exactly, and no
    /// other redemption may already claim it. Completes the redemption and
    /// settles its receipt reservation like a normal burn confirmation.
    ForceCompleteRedemption(Box<ForceCompleteRedemptionArgs>),
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
}

#[derive(Args)]
struct FireblocksAuthProbeArgs {
    #[clap(flatten)]
    fireblocks: FireblocksEnv,
}

/// Which way the operator intends custody to move. Stated explicitly and
/// checked against the direction the recorded custody state resolves to, so a
/// re-run after a recorded forward move cannot silently become a rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum MigrationDirection {
    /// Fireblocks -> Turnkey.
    Forward,
    /// Turnkey -> the recorded Fireblocks origin.
    Rollback,
}

/// How the retiring custody wallet is controlled during a migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
enum OutgoingWalletControl {
    /// Build locally, RAW-sign through Fireblocks, and broadcast through RPC.
    #[default]
    FireblocksRaw,
    /// Last resort: sign directly with `CUSTODY_MIGRATION_PRIVATE_KEY`.
    LocalPrivateKey,
}

#[derive(Clone)]
struct CustodyMigrationPrivateKey(B256);

impl std::fmt::Debug for CustodyMigrationPrivateKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl CustodyMigrationPrivateKey {
    fn parse(value: &str) -> Result<Self, CustodyMigrationPrivateKeyError> {
        let key: B256 =
            value.parse().map_err(|_| CustodyMigrationPrivateKeyError)?;
        PrivateKeySigner::from_bytes(&key)
            .map_err(|_| CustodyMigrationPrivateKeyError)?;

        Ok(Self(key))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid custody migration private key")]
struct CustodyMigrationPrivateKeyError;

#[derive(Debug)]
enum ResolvedOutgoingWalletControl {
    FireblocksRaw(crate::fireblocks::FireblocksConfig),
    LocalPrivateKey(CustodyMigrationPrivateKey),
}

impl std::fmt::Display for MigrationDirection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Forward => "forward",
            Self::Rollback => "rollback",
        })
    }
}

#[derive(Args)]
struct MigrateReceiptsArgs {
    /// Underlying symbol, e.g. AMAT. Upper-cased like [`AssetArgs`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    underlying: UnderlyingSymbol,

    /// Network whose vault to migrate.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

    /// The direction this run is meant to move custody; refused if the
    /// recorded custody state resolves to the other one.
    #[arg(long, value_enum)]
    direction: MigrationDirection,

    /// How to control the retiring wallet. The private-key mode is an explicit
    /// last resort that bypasses Fireblocks entirely.
    #[arg(long, value_enum, default_value = "fireblocks-raw")]
    outgoing_wallet_control: OutgoingWalletControl,

    /// RPC endpoint for the network — the service's own `RPC_URL`.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,

    /// Chain the migration must run against, cross-checked against the chain
    /// the RPC reports. Stated explicitly because the same vault address can
    /// exist on more than one chain, so reaching the contract does not prove
    /// the endpoint is the intended one.
    #[arg(long)]
    chain_id: u64,

    #[clap(flatten)]
    signer: SignerEnv,

    #[clap(flatten)]
    fireblocks: FireblocksEnv,

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
struct VerifyCustodiansArgs {
    /// Underlying symbol, e.g. AMAT. Upper-cased like [`AssetArgs`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    underlying: UnderlyingSymbol,

    /// Network whose vault to verify against.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

    /// RPC endpoint for the network — the service's own `RPC_URL`.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,

    /// Chain this must run against, cross-checked against the chain the RPC
    /// reports.
    #[arg(long)]
    chain_id: u64,

    /// Additionally submit a zero-amount transfer through the full Fireblocks
    /// path — whitelisting, TAP rule, signing, and the vault's authorization
    /// gates — moving nothing. The strongest possible Fireblocks-side proof.
    #[arg(long)]
    smoke: bool,

    #[clap(flatten)]
    signer: SignerEnv,

    #[clap(flatten)]
    fireblocks: FireblocksEnv,

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
struct ConfirmCustodyArgs {
    /// Underlying symbol, e.g. AMAT. Upper-cased like [`AssetArgs`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    underlying: UnderlyingSymbol,

    /// Network whose vault to confirm.
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
    fireblocks: FireblocksEnv,

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
            IssuerCommand::MigrateReceipts(args) => {
                run_migrate_receipts(*args, prompt_confirm).await
            }
            IssuerCommand::ConfirmCustody(args) => {
                run_confirm_custody(*args, prompt_confirm).await
            }
            IssuerCommand::VerifyCustodians(args) => {
                run_verify_custodians(*args, prompt_confirm).await
            }
            IssuerCommand::ForceCompleteRedemption(args) => {
                run_force_complete_redemption(*args, prompt_confirm).await
            }
            IssuerCommand::FireblocksAuthProbe(args) => {
                run_fireblocks_auth_probe(*args).await
            }
        }
    }
}

/// Verifies and records a vault's custody holder. No transaction; the holder
/// is fetched from the Fireblocks API, its on-chain balances compared against
/// the tracked inventory, and only an exact match is recorded.
async fn run_confirm_custody(
    args: ConfirmCustodyArgs,
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
    let vault = find_vault(&admin.pool, &args.underlying, &args.network)
        .await?
        .ok_or_else(|| AssetAdminError::NotFound {
            underlying: args.underlying.clone(),
        })?;

    let fireblocks_config = args.fireblocks.into_config()?;
    let holder = fetch_vault_address(&fireblocks_config).await?;

    if !confirm(&format!(
        "Verify on-chain that the Fireblocks wallet {holder} holds every \
         tracked receipt of {} vault {vault} and record it as the custody \
         holder?",
        args.underlying
    ))? {
        anyhow::bail!("aborted by operator");
    }

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;
    let receipts = confirm_custody_holder(
        &admin.pool,
        provider,
        VaultIdentity { chain_id, vault, underlying: &args.underlying },
        holder,
    )
    .await?;

    println!(
        "Confirmed: {holder} holds all {receipts} tracked receipt(s) for \
         vault {vault}."
    );

    Ok(())
}

/// Proves both custodian connections before anything moves. Signs the exact
/// rollback-shaped transaction with Turnkey without broadcasting it,
/// authenticates against Fireblocks and resolves the whitelisted Receipt
/// contract, and reports gas balances. With `--smoke`, additionally submits a
/// zero-amount transfer through the full Fireblocks path.
async fn run_verify_custodians(
    args: VerifyCustodiansArgs,
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
    let vault = find_vault(&admin.pool, &args.underlying, &args.network)
        .await?
        .ok_or_else(|| AssetAdminError::NotFound {
            underlying: args.underlying.clone(),
        })?;

    let SignerConfig::Turnkey(turnkey_config) =
        args.signer.clone().into_config()?
    else {
        anyhow::bail!(
            "verify-custodians requires the Turnkey signer configuration"
        );
    };
    let turnkey = turnkey_config.settings.address;
    let fireblocks_config = args.fireblocks.clone().into_config()?;

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;

    // Fireblocks: authenticating and resolving the wallet proves the
    // credentials; resolving the whitelisted Receipt contract proves the
    // authorization work is in place before the window opens.
    let fireblocks_wallet = fetch_vault_address(&fireblocks_config).await?;
    println!("Fireblocks: authenticated; wallet {fireblocks_wallet}");

    let receipt_contract = OffchainAssetReceiptVault::new(vault, &provider)
        .receipt()
        .call()
        .await?
        .0
        .into();
    let fireblocks_service = FireblocksVaultService::new(
        &fireblocks_config,
        provider.clone(),
        chain_id,
    )?;
    fireblocks_service.resolve_contract_wallet(receipt_contract).await?;
    println!("Fireblocks: Receipt contract {receipt_contract} is whitelisted");

    // Turnkey: signing the exact rollback-shaped transaction (never
    // broadcast) proves the credentials, the organization, the address, and
    // the signing policy — before the forward move can become a one-way door.
    let resolved = resolve_turnkey_signer(&turnkey_config, chain_id)?;
    let proof = verify_rollback_signing(
        &admin.pool,
        &provider,
        &resolved.wallet,
        VaultIdentity { chain_id, vault, underlying: &args.underlying },
        fireblocks_wallet,
    )
    .await?;
    println!(
        "Turnkey: signed the rollback of {} receipt(s) to {} without \
         broadcasting",
        proof.receipts, proof.destination
    );
    println!(
        "Gas: turnkey {turnkey} holds {} wei; holder {} holds {} wei",
        proof.turnkey_gas, proof.destination, proof.holder_gas
    );
    require_rollback_gas(proof.turnkey_gas, turnkey)?;

    if args.smoke {
        // The one live transaction in the preflight: zero-amount, but still
        // submitted through the real custodian path and possibly waiting on
        // console approval — the operator opts in explicitly.
        if !confirm(&format!(
            "Submit a zero-amount smoke transfer of a {} receipt through the \
             full Fireblocks path (console approval may be required)?",
            args.underlying
        ))? {
            anyhow::bail!("aborted by operator");
        }

        run_fireblocks_smoke(
            &admin.pool,
            &fireblocks_service,
            chain_id,
            SmokeTarget { vault, receipt_contract, fireblocks_wallet, turnkey },
        )
        .await?;
    }

    println!("Both custodian connections verified.");

    Ok(())
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

/// The four distinct addresses the smoke transfer touches, under named
/// fields: they are all bare `Address`es, so as positional parameters a
/// transposed pair would compile silently and smoke-test the wrong transfer.
struct SmokeTarget {
    vault: Address,
    receipt_contract: Address,
    fireblocks_wallet: Address,
    turnkey: Address,
}

/// Submits a zero-amount `safeTransferFrom` of the first tracked receipt
/// through the full Fireblocks path. Exercises whitelisting, the TAP rule,
/// signing, and the vault's authorization gates while moving nothing — a
/// zero-amount transfer cannot create the inventory divergence a real dust
/// transfer would.
async fn run_fireblocks_smoke<P: Provider + Clone>(
    pool: &Pool<Sqlite>,
    fireblocks_service: &FireblocksVaultService<P>,
    chain_id: u64,
    target: SmokeTarget,
) -> anyhow::Result<()> {
    let SmokeTarget { vault, receipt_contract, fireblocks_wallet, turnkey } =
        target;
    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;
    let inventory = load_inventory(&store, chain_id, &vault).await?;
    let receipt_id = inventory
        .receipts_with_balance()
        .first()
        .map(|receipt| receipt.receipt_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "vault {vault} has no tracked receipts to smoke-test with"
            )
        })?;

    let calldata =
        Receipt::new(receipt_contract, fireblocks_service.read_provider())
            .safeTransferFrom(
                fireblocks_wallet,
                turnkey,
                receipt_id.inner(),
                U256::ZERO,
                Bytes::new(),
            )
            .calldata()
            .clone();

    // The same fresh-retry walk the migration transfer uses: a smoke run that
    // failed terminally (e.g. a TAP rule fixed minutes later) must not brick
    // the same-day smoke id.
    let date = chrono::Utc::now().format("%Y-%m-%d");
    let tx_hash = fireblocks_service
        .submit_contract_call_to_completion(
            receipt_contract,
            &calldata,
            "zero-amount custody-path smoke test",
            &format!("custody-smoke-{chain_id}-{vault:#x}-{date}"),
        )
        .await?;

    // Fireblocks can report `Completed` while the EVM transaction reverted —
    // the same discrepancy the migration's transfer path treats as failure. A
    // reverted smoke test proves the path is broken, so it must not print
    // success.
    let receipt = fireblocks_service
        .read_provider()
        .get_transaction_receipt(tx_hash)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "smoke transfer {tx_hash} has no receipt on-chain despite \
                 Fireblocks reporting completion"
            )
        })?;

    if !receipt.status() {
        anyhow::bail!(
            "smoke transfer {tx_hash} REVERTED on-chain despite Fireblocks \
             reporting completion — the custody path is not working"
        );
    }

    println!("Fireblocks smoke transfer completed (moved nothing): {tx_hash}");

    Ok(())
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

/// Moves one vault's receipt custody to a replacement wallet.
///
/// The wallet whose custody moves is whoever this command signs as, not a
/// separate argument: the two can then never disagree, and ERC-1155 only lets
/// the holder move its own balance anyway.
async fn run_migrate_receipts(
    args: MigrateReceiptsArgs,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    let emergency_key = match std::env::var("CUSTODY_MIGRATION_PRIVATE_KEY") {
        Ok(key) => Some(key),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!(
            "CUSTODY_MIGRATION_PRIVATE_KEY must be valid Unicode hex"
        ),
    };

    run_migrate_receipts_with_key(args, emergency_key.as_deref(), confirm).await
}

async fn run_migrate_receipts_with_key(
    args: MigrateReceiptsArgs,
    emergency_key: Option<&str>,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    // Checked first, before any database or network work: a network paired with
    // a chain it does not name is answerable from the arguments alone, and
    // every later step — vault resolution, the inventory key, the prompt the
    // operator confirms — would otherwise be derived from a premise already
    // known to be wrong.
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

    let vault = find_vault(&admin.pool, &args.underlying, &args.network)
        .await?
        .ok_or_else(|| AssetAdminError::NotFound {
            underlying: args.underlying.clone(),
        })?;

    // No wallet is an argument. Turnkey always supplies the incoming address
    // and rollback signer. The retiring wallet comes from the explicitly
    // selected control (Fireblocks RAW or emergency local key), and direction
    // comes from recorded custody — never from operator intent alone.
    let signer = args.signer.clone().into_config()?;
    let SignerConfig::Turnkey(turnkey_config) = signer else {
        anyhow::bail!(
            "migrate-receipts requires the Turnkey signer configuration: the \
             destination of the forward transfer and the signer of the \
             rollback both come from it."
        );
    };
    let turnkey = turnkey_config.settings.address;
    let outgoing_control =
        resolve_outgoing_wallet_control(&args, emergency_key)?;

    let recorded =
        recorded_custody_holder(&admin.pool, args.chain_id, vault).await?;

    match recorded {
        None => anyhow::bail!(
            "vault {vault} on chain {} has no recorded custody holder; run \
             `issuer confirm-custody` first",
            args.chain_id
        ),
        Some(holder) if holder == turnkey => {
            require_direction(args.direction, MigrationDirection::Rollback)?;
            run_rollback_transfer(
                &args,
                &admin,
                vault,
                &turnkey_config,
                &outgoing_control,
                confirm,
            )
            .await
        }
        Some(holder) => {
            require_direction(args.direction, MigrationDirection::Forward)?;
            run_forward_transfer(
                &args,
                &admin,
                vault,
                holder,
                &turnkey_config,
                &outgoing_control,
                confirm,
            )
            .await
        }
    }
}

/// Refuses when the operator's stated direction disagrees with the direction
/// the recorded custody state resolves to.
///
/// The command is state-driven and therefore reversible: once a forward move
/// is recorded, custody sits at Turnkey and a mechanical re-run of the same
/// command would resolve to a rollback. Requiring the direction to be stated
/// turns that surprise into a refusal before any prompt or network work.
fn resolve_outgoing_wallet_control(
    args: &MigrateReceiptsArgs,
    emergency_key: Option<&str>,
) -> anyhow::Result<ResolvedOutgoingWalletControl> {
    match (args.outgoing_wallet_control, emergency_key) {
        (OutgoingWalletControl::FireblocksRaw, Some(_)) => anyhow::bail!(
            "CUSTODY_MIGRATION_PRIVATE_KEY was supplied, but \
             --outgoing-wallet-control is fireblocks-raw; select \
             local-private-key explicitly or remove the emergency key"
        ),
        (OutgoingWalletControl::FireblocksRaw, None) => {
            Ok(ResolvedOutgoingWalletControl::FireblocksRaw(
                args.fireblocks.clone().into_config()?,
            ))
        }
        (OutgoingWalletControl::LocalPrivateKey, Some(key)) => {
            Ok(ResolvedOutgoingWalletControl::LocalPrivateKey(
                CustodyMigrationPrivateKey::parse(key)?,
            ))
        }
        (OutgoingWalletControl::LocalPrivateKey, None) => anyhow::bail!(
            "CUSTODY_MIGRATION_PRIVATE_KEY is required when \
             --outgoing-wallet-control is local-private-key"
        ),
    }
}

fn require_outgoing_wallet_matches(
    recorded: Address,
    derived: Address,
) -> anyhow::Result<()> {
    if recorded != derived {
        anyhow::bail!(
            "recorded retiring-wallet custody is {recorded}, but the selected \
             outgoing-wallet control derives {derived}; refusing before \
             signing. Check the selected backend and its configuration"
        );
    }

    Ok(())
}

async fn derive_outgoing_wallet(
    control: &ResolvedOutgoingWalletControl,
) -> anyhow::Result<Address> {
    match control {
        ResolvedOutgoingWalletControl::FireblocksRaw(config) => {
            Ok(fetch_vault_address(config).await?)
        }
        ResolvedOutgoingWalletControl::LocalPrivateKey(key) => {
            Ok(SignerConfig::Local(key.0).address()?)
        }
    }
}

fn require_direction(
    stated: MigrationDirection,
    resolved: MigrationDirection,
) -> anyhow::Result<()> {
    if stated != resolved {
        anyhow::bail!(
            "recorded custody state resolves this command to a {resolved} \
             transfer, but --direction says {stated}; if a previous run \
             already moved custody, a re-run is a {resolved} — state the \
             direction you actually intend"
        );
    }

    Ok(())
}

/// The rollback: Turnkey signs custody back to the recorded origin, which is
/// independently derived through the selected outgoing-wallet control. A wrong
/// Fireblocks workspace or emergency key therefore cannot redirect rollback.
/// No address is typed anywhere, and the migration engine's ownership checks
/// are the verification: the source must hold exactly every tracked balance
/// before, and the recipient's gain must match exactly after.
async fn run_rollback_transfer(
    args: &MigrateReceiptsArgs,
    admin: &AssetAdmin,
    vault: Address,
    turnkey_config: &TurnkeyConfig,
    outgoing_control: &ResolvedOutgoingWalletControl,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    let turnkey = turnkey_config.settings.address;
    let outgoing_wallet = derive_outgoing_wallet(outgoing_control).await?;
    let recorded_origin =
        recorded_migration_origin(&admin.pool, args.chain_id, vault).await?;
    let recipient =
        Recipient::for_rollback(recorded_origin, outgoing_wallet, turnkey)?;

    println!(
        "{} on {} (chain {}) vault {vault}: rolling receipt custody back from \
         {turnkey} to the recorded Fireblocks origin {recipient}",
        args.underlying, args.network, args.chain_id
    );

    if !confirm(&format!(
        "The issuer service MUST be stopped before this runs. Roll {} receipt \
         custody back from {turnkey} to {recipient}?",
        args.underlying
    ))? {
        anyhow::bail!("aborted by operator");
    }

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let resolved = resolve_turnkey_signer(turnkey_config, chain_id)?;
    let provider = ProviderBuilder::new()
        .with_chain_id(chain_id)
        .wallet(resolved.wallet)
        .connect(args.rpc_url.as_str())
        .await?;

    let recipient =
        CorroboratedRecipient::verify(&provider, recipient.address()).await?;

    let outcome = migrate_vault_receipts(
        &admin.pool,
        provider,
        VaultIdentity { chain_id, vault, underlying: &args.underlying },
        turnkey,
        recipient,
    )
    .await?;
    report_outcome(&outcome, recipient.address(), vault);

    Ok(())
}

/// The forward leg: build the exact transfer in this binary, then sign it
/// either through Fireblocks RAW signing or directly with the explicitly
/// selected emergency key. Both paths broadcast through the configured RPC and
/// run the same migration gates and post-condition checks.
///
/// Re-running after a completed move records it instead of transferring again.
async fn run_forward_transfer(
    args: &MigrateReceiptsArgs,
    admin: &AssetAdmin,
    vault: Address,
    holder: Address,
    turnkey_config: &TurnkeyConfig,
    outgoing_control: &ResolvedOutgoingWalletControl,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    let turnkey = turnkey_config.settings.address;
    let recipient = Recipient::for_holder(turnkey, holder)?;
    let signing_description = match outgoing_control {
        ResolvedOutgoingWalletControl::FireblocksRaw(_) => {
            "Fireblocks RAW signing"
        }
        ResolvedOutgoingWalletControl::LocalPrivateKey(_) => {
            "the emergency local private key"
        }
    };

    println!(
        "{} on {} (chain {}) vault {vault}: moving receipt custody from \
         {holder} to {recipient} via {signing_description}",
        args.underlying, args.network, args.chain_id
    );

    // The operator is warned here because this is the last point of no return
    // and it is the one precondition nothing downstream can detect: an issuer
    // service still running with the outgoing signer reconciles the moved
    // receipts as depletions and drops them from the aggregate. The prompt
    // deliberately runs before any network call, so declining costs nothing.
    if !confirm(&format!(
        "The issuer service MUST be stopped before this runs. Sign with \
         {signing_description} and move {} receipt custody from {holder} to \
         {recipient}?",
        args.underlying
    ))? {
        anyhow::bail!("aborted by operator");
    }

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;
    let outgoing_wallet = derive_outgoing_wallet(outgoing_control).await?;
    require_outgoing_wallet_matches(holder, outgoing_wallet)?;

    // The forward transfer is a one-way door unless Turnkey can sign the way
    // back. Re-proven here, immediately before the irreversible submission,
    // rather than trusted from an earlier `verify-custodians` run that may be
    // stale or skipped: the exact rollback-shaped transaction — every tracked
    // receipt back from the Turnkey wallet to the current holder — is signed
    // without being broadcast, and the Turnkey wallet must hold the rollback
    // gas reserve.
    let resolved = resolve_turnkey_signer(turnkey_config, chain_id)?;
    let proof = verify_rollback_signing(
        &admin.pool,
        &provider,
        &resolved.wallet,
        VaultIdentity { chain_id, vault, underlying: &args.underlying },
        outgoing_wallet,
    )
    .await?;
    println!(
        "Turnkey rollback re-proven: signed the return of {} receipt(s) to \
         {} without broadcasting",
        proof.receipts, proof.destination
    );
    require_rollback_gas(proof.turnkey_gas, turnkey)?;

    let recipient =
        CorroboratedRecipient::verify(&provider, recipient.address()).await?;
    let identity =
        VaultIdentity { chain_id, vault, underlying: &args.underlying };
    let outcome = match outgoing_control {
        ResolvedOutgoingWalletControl::FireblocksRaw(fireblocks_config) => {
            migrate_vault_receipts_via_fireblocks(
                &admin.pool,
                provider,
                fireblocks_config,
                identity,
                outgoing_wallet,
                recipient,
            )
            .await?
        }
        ResolvedOutgoingWalletControl::LocalPrivateKey(key) => {
            let resolved =
                crate::wallet::local::resolve_local_signer(&key.0, chain_id)?;
            let signing_provider = ProviderBuilder::new()
                .with_chain_id(chain_id)
                .wallet(resolved.wallet)
                .connect(args.rpc_url.as_str())
                .await?;

            migrate_vault_receipts(
                &admin.pool,
                signing_provider,
                identity,
                outgoing_wallet,
                recipient,
            )
            .await?
        }
    };
    report_outcome(&outcome, recipient.address(), vault);

    Ok(())
}

/// Requires the Turnkey wallet to hold the rollback gas reserve.
///
/// One wei passes a bare non-zero check while leaving a real rollback
/// unbroadcastable, which would turn the forward move into a one-way door
/// discovered only when the way back is needed.
fn require_rollback_gas(
    turnkey_gas: U256,
    turnkey: Address,
) -> anyhow::Result<()> {
    let reserve = rollback_gas_reserve();
    if turnkey_gas < reserve {
        anyhow::bail!(
            "the Turnkey wallet {turnkey} holds {turnkey_gas} wei but the \
             rollback gas reserve is {reserve} wei; fund it before the \
             window — it cannot broadcast a rollback or operate the service \
             without gas"
        );
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

fn report_outcome(
    outcome: &MigrationOutcome,
    recipient: Address,
    vault: Address,
) {
    match outcome {
        MigrationOutcome::Migrated { transaction, receipts } => {
            println!(
                "Moved {receipts} receipt(s) to {recipient} in {transaction}."
            );
        }
        MigrationOutcome::AlreadyMigrated { receipts } => {
            println!(
                "Already migrated: {recipient} holds all {receipts} \
                 receipt(s) for vault {vault}."
            );
        }
    }
}

/// A destination that is safe to move custody to.
///
/// Existence implies validity: both constructors funnel through
/// [`Recipient::for_holder`]'s checks ([`Recipient::for_rollback`] adds the
/// recorded-origin cross-check on top), so a caller cannot hold one without
/// having rejected the two destinations that make an irreversible move
/// unrecoverable or pointless.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Recipient(Address);

impl Recipient {
    /// Rejects the zero address, which would burn the receipts, and the
    /// signing wallet itself, which would submit a transaction that moves
    /// nothing.
    fn for_holder(recipient: Address, holder: Address) -> anyhow::Result<Self> {
        if recipient.is_zero() {
            anyhow::bail!(
                "recipient is the zero address; custody would be lost"
            );
        }

        if recipient == holder {
            anyhow::bail!(
                "recipient {recipient} is already the signing wallet; nothing \
                 would move"
            );
        }

        Ok(Self(recipient))
    }

    /// The rollback destination: the recorded migration origin, cross-checked
    /// against the wallet derived through the selected outgoing-wallet
    /// control. The two are independent — one from custody history, one from
    /// Fireblocks or the emergency key — so a wrong control cannot redirect
    /// rollback to a wallet this vault's receipts never came from.
    fn for_rollback(
        recorded_origin: Address,
        derived_outgoing_wallet: Address,
        holder: Address,
    ) -> anyhow::Result<Self> {
        if derived_outgoing_wallet != recorded_origin {
            anyhow::bail!(
                "the selected outgoing-wallet control derives \
                 {derived_outgoing_wallet}, but custody was recorded as moved \
                 from {recorded_origin}; refusing to roll back to a wallet \
                 this vault's custody never came from — check the selected \
                 backend and its configuration"
            );
        }

        Self::for_holder(recorded_origin, holder)
    }

    const fn address(self) -> Address {
        self.0
    }
}

impl std::fmt::Display for Recipient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
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
    use alloy::primitives::{U256, address, b256};
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing_test::traced_test;

    use super::*;
    use crate::Quantity;
    use crate::redemption::{BurnRecord, RedemptionEvent};
    use crate::test_utils::logs_contain_at;
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

    /// Seeds a listing into a file-backed store, since `run_migrate_receipts`
    /// opens its own pool from the URL and so cannot share an in-memory one.
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

    /// Seeds a recorded custody holder the way the service does — through the
    /// aggregate — so direction dispatch is tested against real state.
    async fn seed_custody_at(database_url: &str, holder: Address) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
            .expect("Failed to open database");
        let store = StoreBuilder::<ReceiptInventory>::new(pool)
            .build(())
            .await
            .expect("Failed to build receipt inventory store");

        store
            .send(
                &crate::receipt_inventory::ReceiptVaultKey::new(
                    Network::Base.chain_id(),
                    TEST_VAULT,
                ),
                crate::receipt_inventory::ReceiptInventoryCommand::ConfirmCustody {
                    holder,
                },
            )
            .await
            .expect("Failed to seed custody");
    }

    fn turnkey_migrate_args(
        direction: &str,
        chain_id: &str,
        database_url: &str,
        secret_path: &str,
    ) -> MigrateReceiptsArgs {
        let cli = IssuerCli::try_parse_from([
            "issuer",
            "migrate-receipts",
            "AMAT",
            "--network",
            "base",
            "--direction",
            direction,
            "--chain-id",
            chain_id,
            "--turnkey-org-id",
            "org-id",
            "--turnkey-api-private-key",
            "api-key",
            "--turnkey-address",
            "0x00000000000000000000000000000000000000cc",
            "--fireblocks-api-user-id",
            "fb-user",
            "--fireblocks-secret-path",
            secret_path,
            "--rpc-url",
            "http://127.0.0.1:1",
            "--database-url",
            database_url,
        ])
        .expect("arguments parse");

        let IssuerCommand::MigrateReceipts(args) = cli.command else {
            panic!("expected the migrate-receipts subcommand")
        };

        *args
    }

    #[test]
    fn cli_does_not_accept_the_emergency_key_in_process_arguments() {
        let parsed = IssuerCli::try_parse_from([
            "issuer",
            "migrate-receipts",
            "AMAT",
            "--network",
            "base",
            "--direction",
            "forward",
            "--outgoing-wallet-control",
            "local-private-key",
            "--custody-migration-private-key",
            TEST_SIGNER_KEY,
            "--chain-id",
            "8453",
            "--turnkey-org-id",
            "org-id",
            "--turnkey-api-private-key",
            "api-key",
            "--turnkey-address",
            "0x00000000000000000000000000000000000000cc",
            "--rpc-url",
            "http://127.0.0.1:1",
        ]);

        let Err(error) = parsed else {
            panic!(
                "private key input must be environment-only, never an argv \
                 option"
            )
        };

        assert!(
            !error.to_string().contains(TEST_SIGNER_KEY),
            "argument errors must never echo private key material"
        );
    }

    #[test]
    fn custody_migration_private_key_rejects_an_invalid_signer_value() {
        let invalid = "0x0000000000000000000000000000000000000000000000000000000000000000";

        let error = CustodyMigrationPrivateKey::parse(invalid).unwrap_err();

        assert_eq!(error.to_string(), "invalid custody migration private key");
        assert!(
            !error.to_string().contains(invalid),
            "parse errors must never echo private key material"
        );
    }

    #[test]
    fn local_private_key_control_requires_the_emergency_key() {
        let mut args = turnkey_migrate_args(
            "forward",
            "8453",
            "sqlite:unused.db",
            "unused-fireblocks-secret",
        );
        args.outgoing_wallet_control = OutgoingWalletControl::LocalPrivateKey;

        let error = resolve_outgoing_wallet_control(&args, None).unwrap_err();

        assert!(
            error.to_string().contains("CUSTODY_MIGRATION_PRIVATE_KEY"),
            "the explicit local mode must name its missing key, got {error}"
        );
    }

    #[test]
    fn fireblocks_raw_control_refuses_an_unused_emergency_key() {
        let args = turnkey_migrate_args(
            "forward",
            "8453",
            "sqlite:unused.db",
            "unused-fireblocks-secret",
        );
        let error =
            resolve_outgoing_wallet_control(&args, Some(TEST_SIGNER_KEY))
                .unwrap_err();

        assert!(
            error.to_string().contains("local-private-key"),
            "a supplied emergency key must not be silently ignored, got {error}"
        );
        assert!(
            !error.to_string().contains(TEST_SIGNER_KEY),
            "configuration errors must never expose the private key"
        );
    }

    #[test]
    fn local_private_key_control_derives_the_retiring_wallet_and_redacts_key() {
        let mut args = turnkey_migrate_args(
            "forward",
            "8453",
            "sqlite:unused.db",
            "unused-fireblocks-secret",
        );
        args.outgoing_wallet_control = OutgoingWalletControl::LocalPrivateKey;

        let control =
            resolve_outgoing_wallet_control(&args, Some(TEST_SIGNER_KEY))
                .unwrap();
        let ResolvedOutgoingWalletControl::LocalPrivateKey(key) = control
        else {
            panic!("the explicit local mode must resolve a local key")
        };

        let derived = SignerConfig::Local(key.0).address().unwrap();
        assert_eq!(
            derived,
            address!("0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"),
            "the fallback must derive the retiring wallet from the key"
        );
        assert_eq!(
            format!("{key:?}"),
            "[REDACTED]",
            "debug output must never expose the private key"
        );
    }

    #[test]
    fn a_local_private_key_must_match_recorded_custody() {
        let recorded = address!("0x00000000000000000000000000000000000000bb");
        let derived = address!("0x00000000000000000000000000000000000000cc");

        let error =
            require_outgoing_wallet_matches(recorded, derived).unwrap_err();

        assert!(
            error.to_string().contains(&recorded.to_string())
                && error.to_string().contains(&derived.to_string()),
            "the refusal must name both public wallet addresses, got {error}"
        );
    }

    /// A declined confirmation must abort before any network call. The
    /// unreachable `--rpc-url` is the assertion: if the prompt were bypassed,
    /// or the chain-id round-trip still ran first, this would fail on the
    /// endpoint rather than on the operator's decision.
    #[tokio::test]
    async fn migrate_receipts_aborts_without_touching_the_chain_when_declined()
    {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("decline.db").display()
        );
        seed_listing_at(&database_url, "AMAT").await;
        // Custody sits with a wallet that is not Turnkey, so dispatch chooses
        // the forward (Fireblocks) leg — the one whose prompt is under test.
        seed_custody_at(
            &database_url,
            address!("00000000000000000000000000000000000000bb"),
        )
        .await;
        let secret = directory.path().join("fb-secret.pem");
        std::fs::write(&secret, b"test-secret").unwrap();

        let args = turnkey_migrate_args(
            "forward",
            "8453",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts_with_key(args, None, |_| Ok(false))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("aborted by operator"),
            "a declined confirmation must abort before any RPC, got {error}"
        );
    }

    /// The emergency local-key mode must not require Fireblocks credentials:
    /// the key itself derives and controls the recorded holder. Declining the
    /// prompt must therefore abort before the deliberately unreachable RPC,
    /// proving dispatch reached the local path without touching Fireblocks.
    #[tokio::test]
    async fn local_private_key_control_bypasses_fireblocks_before_confirmation()
    {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("local-fallback.db").display()
        );
        seed_listing_at(&database_url, "AMAT").await;
        let local_key =
            CustodyMigrationPrivateKey::parse(TEST_SIGNER_KEY).unwrap();
        let holder = SignerConfig::Local(local_key.0).address().unwrap();
        seed_custody_at(&database_url, holder).await;

        let cli = IssuerCli::try_parse_from([
            "issuer",
            "migrate-receipts",
            "AMAT",
            "--network",
            "base",
            "--direction",
            "forward",
            "--outgoing-wallet-control",
            "local-private-key",
            "--chain-id",
            "8453",
            "--turnkey-org-id",
            "org-id",
            "--turnkey-api-private-key",
            "api-key",
            "--turnkey-address",
            "0x00000000000000000000000000000000000000cc",
            "--rpc-url",
            "http://127.0.0.1:1",
            "--database-url",
            &database_url,
        ])
        .expect("arguments parse");
        let IssuerCommand::MigrateReceipts(args) = cli.command else {
            panic!("expected the migrate-receipts subcommand")
        };

        let error =
            run_migrate_receipts_with_key(*args, Some(TEST_SIGNER_KEY), |_| {
                Ok(false)
            })
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("aborted by operator"),
            "local control must bypass Fireblocks and reach its prompt, got {error}"
        );
    }

    /// Turnkey remains mandatory: it is both the forward destination and the
    /// rollback signer, regardless of how the retiring wallet signs forward.
    #[tokio::test]
    async fn migrate_receipts_refuses_a_non_turnkey_signer() {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("local.db").display()
        );
        seed_listing_at(&database_url, "AMAT").await;

        let cli = IssuerCli::try_parse_from([
            "issuer",
            "migrate-receipts",
            "AMAT",
            "--network",
            "base",
            "--direction",
            "forward",
            "--chain-id",
            "8453",
            "--evm-private-key",
            TEST_SIGNER_KEY,
            "--rpc-url",
            "http://127.0.0.1:1",
            "--database-url",
            &database_url,
        ])
        .expect("arguments parse");

        let IssuerCommand::MigrateReceipts(args) = cli.command else {
            panic!("expected the migrate-receipts subcommand")
        };

        let error = run_migrate_receipts_with_key(*args, None, |_| Ok(true))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("Turnkey signer configuration"),
            "a local key must be refused, got {error}"
        );
    }

    /// With no recorded custody holder there is nothing to resolve a direction
    /// from, and the command must demand the bootstrap rather than guess.
    #[tokio::test]
    async fn migrate_receipts_without_recorded_custody_demands_the_bootstrap() {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("unobserved.db").display()
        );
        seed_listing_at(&database_url, "AMAT").await;
        let secret = directory.path().join("fb-secret.pem");
        std::fs::write(&secret, b"test-secret").unwrap();

        let args = turnkey_migrate_args(
            "forward",
            "8453",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts_with_key(args, None, |_| Ok(true))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("no recorded custody holder"),
            "unobserved custody must demand confirm-custody, got {error}"
        );
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
            },
            RedemptionEvent::BurningFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "Fireblocks transaction polling timed out".to_string(),
                failed_at: Utc::now(),
                tx_id: None,
                planned_burns: vec![BurnRecord {
                    receipt_id: U256::from(3),
                    shares_burned: U256::from(40_000_000_000_000_000_u64),
                }],
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

    /// The rollback's destination comes from the Fireblocks API, so a broken
    /// Fireblocks configuration must stop the rollback before anything is
    /// derived or signed — never fall back to a guess.
    #[tokio::test]
    async fn a_rollback_with_a_broken_fireblocks_config_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("rollback.db").display()
        );
        seed_listing_at(&database_url, "AMAT").await;
        // The recorded holder IS the Turnkey wallet, so dispatch chooses the
        // rollback leg — whose destination derivation must fail closed on an
        // unusable Fireblocks credential.
        seed_custody_at(
            &database_url,
            address!("00000000000000000000000000000000000000cc"),
        )
        .await;
        let secret = directory.path().join("fb-secret.pem");
        std::fs::write(&secret, b"not-an-rsa-key").unwrap();

        let args = turnkey_migrate_args(
            "rollback",
            "8453",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts_with_key(args, None, |_| Ok(true))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("Fireblocks"),
            "a rollback without a working Fireblocks configuration must \
             refuse, got {error}"
        );
    }

    /// The command is state-driven: once a forward move is recorded, custody
    /// sits at Turnkey and a mechanical re-run resolves to a rollback. The
    /// stated direction must catch that before any prompt or network work.
    #[tokio::test]
    async fn a_rerun_after_a_recorded_forward_move_is_refused_as_a_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("rerun.db").display()
        );
        seed_listing_at(&database_url, "AMAT").await;
        // Custody recorded at the Turnkey wallet: the state a completed
        // forward move leaves behind.
        seed_custody_at(
            &database_url,
            address!("0x00000000000000000000000000000000000000cc"),
        )
        .await;
        let secret = directory.path().join("fb-secret.pem");
        std::fs::write(&secret, b"test-secret").unwrap();

        let args = turnkey_migrate_args(
            "forward",
            "8453",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts_with_key(args, None, |_| Ok(true))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("resolves this command to a rollback"),
            "a forward-stated re-run over rolled custody must refuse, got \
             {error}"
        );
    }

    /// A network paired with the wrong chain must be rejected from the
    /// arguments alone, before any database or RPC work, so nothing downstream
    /// is derived from a premise already known to be wrong.
    #[tokio::test]
    async fn migrate_receipts_rejects_a_network_bound_to_the_wrong_chain() {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            directory.path().join("mismatch.db").display()
        );
        let secret = directory.path().join("fb-secret.pem");
        std::fs::write(&secret, b"test-secret").unwrap();

        // The database is never seeded and the RPC is unreachable: reaching
        // either would mean the mismatch was not caught up front.
        let args = turnkey_migrate_args(
            "forward",
            "84532",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts_with_key(args, None, |_| Ok(true))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("84532")
                && error.to_string().contains("8453"),
            "the rejection must name both chains, got {error}"
        );
    }

    #[test]
    fn recipient_may_not_be_the_zero_address() {
        let holder = address!("00000000000000000000000000000000000000bb");

        let error = Recipient::for_holder(Address::ZERO, holder).unwrap_err();

        assert!(
            error.to_string().contains("zero address"),
            "burning the receipts is unrecoverable, got {error}"
        );
    }

    #[test]
    fn recipient_may_not_be_the_signing_wallet() {
        let holder = address!("00000000000000000000000000000000000000bb");

        let error = Recipient::for_holder(holder, holder).unwrap_err();

        assert!(
            error.to_string().contains("already the signing wallet"),
            "a self-transfer would submit a pointless transaction, got {error}"
        );
    }

    /// A wrong Fireblocks workspace resolves a wallet that is not the
    /// recorded migration origin; the rollback must refuse before anything is
    /// signed rather than move custody to a wallet this vault's receipts
    /// never came from.
    #[test]
    fn rollback_refuses_a_workspace_that_is_not_the_recorded_origin() {
        let recorded = address!("0x00000000000000000000000000000000000000aa");
        let configured = address!("0x00000000000000000000000000000000000000bb");
        let holder = address!("0x00000000000000000000000000000000000000cc");

        let error =
            Recipient::for_rollback(recorded, configured, holder).unwrap_err();

        assert!(
            error.to_string().contains("recorded as moved from"),
            "a workspace mismatch must name both derived wallets, got {error}"
        );
    }

    #[test]
    fn rollback_destination_is_the_recorded_origin_when_workspaces_agree() {
        let recorded = address!("0x00000000000000000000000000000000000000aa");
        let holder = address!("0x00000000000000000000000000000000000000cc");

        let recipient =
            Recipient::for_rollback(recorded, recorded, holder).unwrap();

        assert_eq!(recipient.address(), recorded);
    }

    #[test]
    fn recipient_accepts_a_distinct_address() {
        let holder = address!("00000000000000000000000000000000000000bb");
        let recipient = address!("00000000000000000000000000000000000000cc");

        assert_eq!(
            Recipient::for_holder(recipient, holder).unwrap().address(),
            recipient,
            "a valid recipient must carry the address it was built from"
        );
    }

    #[test]
    fn migrate_receipts_requires_an_explicit_chain_id() {
        let Err(error) = IssuerCli::try_parse_from([
            "issuer",
            "migrate-receipts",
            "RKLB",
            "--network",
            "base",
            "--direction",
            "forward",
            "--rpc-url",
            "http://localhost:8545",
            "--evm-private-key",
            TEST_SIGNER_KEY,
        ]) else {
            panic!(
                "omitting --chain-id must fail at parse time rather than \
                 trusting whatever chain the RPC happens to be"
            )
        };

        assert!(
            error.to_string().contains("--chain-id"),
            "the parse failure must name the missing --chain-id, got {error}"
        );
    }
}
