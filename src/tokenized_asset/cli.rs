use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
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

use super::UnderlyingSymbol;
use super::view::{
    TokenizedAssetViewError, find_vault, underlying_has_listing,
};
use crate::Network;
use crate::bindings::OffchainAssetReceiptVault;
use crate::config::{
    DEFAULT_DATABASE_MAX_CONNECTIONS, DEFAULT_DATABASE_URL, LogLevel,
    setup_tracing,
};
use crate::fireblocks::{
    FireblocksEnv, FireblocksVaultService, fetch_vault_address,
};
use crate::receipt_inventory::migration::{
    CorroboratedRecipient, MigrationOutcome, VaultIdentity,
    confirm_custody_holder, migrate_vault_receipts,
    migrate_vault_receipts_via_fireblocks, recorded_custody_holder,
    verify_rollback_signing,
};
use crate::receipt_inventory::{ReceiptInventory, load_inventory};
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
    /// Custody at the Fireblocks wallet runs the forward leg, submitted
    /// through the Fireblocks API (console approval may be required); custody
    /// at the Turnkey wallet runs the rollback, signed by Turnkey back to the
    /// API-derived Fireblocks wallet. Both custodians' configurations are
    /// required and no wallet address is ever typed.
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
}

#[derive(Args)]
struct MigrateReceiptsArgs {
    /// Underlying symbol, e.g. AMAT. Upper-cased like [`AssetArgs`].
    #[arg(value_parser = |value: &str| UnderlyingSymbol::new(value.to_ascii_uppercase()))]
    underlying: UnderlyingSymbol,

    /// Network whose vault to migrate.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

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
        turnkey,
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
    if proof.turnkey_gas.is_zero() {
        anyhow::bail!(
            "the Turnkey wallet holds no gas; fund it before the window — it \
             cannot broadcast a rollback or operate the service without it"
        );
    }

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
            vault,
            receipt_contract,
            fireblocks_wallet,
            turnkey,
        )
        .await?;
    }

    println!("Both custodian connections verified.");

    Ok(())
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
    vault: Address,
    receipt_contract: Address,
    fireblocks_wallet: Address,
    turnkey: Address,
) -> anyhow::Result<()> {
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

    let calldata = crate::bindings::Receipt::new(
        receipt_contract,
        fireblocks_service.read_provider(),
    )
    .safeTransferFrom(
        fireblocks_wallet,
        turnkey,
        receipt_id.inner(),
        U256::ZERO,
        alloy::primitives::Bytes::new(),
    )
    .calldata()
    .clone();

    let date = chrono::Utc::now().format("%Y-%m-%d");
    let fireblocks_tx_id = fireblocks_service
        .submit_contract_call(
            receipt_contract,
            &calldata,
            "zero-amount custody-path smoke test",
            &format!("custody-smoke-{chain_id}-{vault:#x}-{date}"),
        )
        .await?;
    let tx_hash =
        fireblocks_service.wait_for_completion(&fireblocks_tx_id).await?;

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

/// Freezes an underlying on all networks.
///
/// Freezing means "corporate action in progress" and is not part of the
/// custody-migration sequence — the migration gates on quiescence instead.
/// Public so end-to-end tests can drive the real freeze operations alongside
/// the migration and prove the two mechanisms stay independent.
///
/// # Errors
///
/// Returns an error if the store cannot be opened or the command dispatch
/// fails.
pub async fn freeze_underlying(
    database_url: &str,
    database_max_connections: u32,
    underlying: &UnderlyingSymbol,
) -> anyhow::Result<()> {
    let admin =
        AssetAdmin::connect(database_url, database_max_connections).await?;
    admin.freeze(underlying).await?;

    Ok(())
}

/// Unfreezes an underlying on all networks.
///
/// The counterpart to [`freeze_underlying`], so a caller driving the operator
/// sequence can leave the asset as it found it.
///
/// # Errors
///
/// Returns an error if the store cannot be opened or the command dispatch
/// fails.
pub async fn unfreeze_underlying(
    database_url: &str,
    database_max_connections: u32,
    underlying: &UnderlyingSymbol,
) -> anyhow::Result<()> {
    let admin =
        AssetAdmin::connect(database_url, database_max_connections).await?;
    admin.unfreeze(underlying).await?;

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

    // No wallet is an argument. Both custodians' configurations are required:
    // the forward transfer is signed by Fireblocks through its API, the
    // rollback by Turnkey, and which one runs is resolved from the inventory's
    // recorded custody — never from the operator's intent alone.
    let signer = args.signer.clone().into_config()?;
    let SignerConfig::Turnkey(turnkey_config) = signer else {
        anyhow::bail!(
            "migrate-receipts requires the Turnkey signer configuration: the \
             destination of the forward transfer and the signer of the \
             rollback both come from it."
        );
    };
    let turnkey = turnkey_config.settings.address;

    let recorded =
        recorded_custody_holder(&admin.pool, args.chain_id, vault).await?;

    match recorded {
        None => anyhow::bail!(
            "vault {vault} on chain {} has no recorded custody holder; run \
             `issuer confirm-custody` first",
            args.chain_id
        ),
        Some(holder) if holder == turnkey => {
            run_rollback_transfer(
                &args,
                &admin,
                vault,
                &turnkey_config,
                turnkey,
                confirm,
            )
            .await
        }
        Some(holder) => {
            run_forward_via_fireblocks(
                &args, &admin, vault, holder, turnkey, confirm,
            )
            .await
        }
    }
}

/// The rollback: Turnkey signs custody back to the Fireblocks wallet, fetched
/// from the Fireblocks API — the same derivation the forward leg uses. No
/// address is typed anywhere, and the migration engine's ownership checks are
/// the verification: the source must hold exactly every tracked balance
/// before, and the recipient's gain must match exactly after.
async fn run_rollback_transfer(
    args: &MigrateReceiptsArgs,
    admin: &AssetAdmin,
    vault: Address,
    turnkey_config: &TurnkeyConfig,
    turnkey: Address,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    let fireblocks_config = args.fireblocks.clone().into_config()?;
    let fireblocks_wallet = fetch_vault_address(&fireblocks_config).await?;
    let recipient = Recipient::for_holder(fireblocks_wallet, turnkey)?;

    println!(
        "{} on {} (chain {}) vault {vault}: rolling receipt custody back from \
         {turnkey} to the Fireblocks wallet {recipient}",
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

/// The forward leg: the transfer is signed by the retiring custodian, so it is
/// submitted through the Fireblocks API and may wait on approvals in the
/// Fireblocks console. Every migration gate still runs in this binary; only
/// the signing happens at the custodian.
///
/// Re-running after a completed move records it instead of transferring again,
/// which is also how a transfer executed manually in the console gets onto the
/// record a rollback later depends on.
async fn run_forward_via_fireblocks(
    args: &MigrateReceiptsArgs,
    admin: &AssetAdmin,
    vault: Address,
    holder: Address,
    turnkey: Address,
    confirm: impl Fn(&str) -> io::Result<bool>,
) -> anyhow::Result<()> {
    let fireblocks_config = args.fireblocks.clone().into_config()?;
    let recipient = Recipient::for_holder(turnkey, holder)?;

    println!(
        "{} on {} (chain {}) vault {vault}: moving receipt custody from \
         {holder} to {recipient} via Fireblocks",
        args.underlying, args.network, args.chain_id
    );

    // The operator is warned here because this is the last point of no return
    // and it is the one precondition nothing downstream can detect: an issuer
    // service still running with the outgoing signer reconciles the moved
    // receipts as depletions and drops them from the aggregate. The prompt
    // deliberately runs before any network call, so declining costs nothing.
    if !confirm(&format!(
        "The issuer service MUST be stopped before this runs. Submit the \
         transfer through Fireblocks (console approval may be required) and \
         move {} receipt custody from {holder} to {recipient}?",
        args.underlying
    ))? {
        anyhow::bail!("aborted by operator");
    }

    let chain_id = verified_chain_id(&args.rpc_url, args.chain_id).await?;
    let provider =
        ProviderBuilder::new().connect(args.rpc_url.as_str()).await?;

    // The wallet the configured Fireblocks workspace actually controls. The
    // recorded holder must be that wallet: anything else means the
    // configuration points at a different workspace or vault account than
    // the one holding custody, and the transfer this command would submit
    // could never move the recorded holder's receipts. Checked before any
    // submission, so a mismatch is a clean refusal.
    let fireblocks_wallet = fetch_vault_address(&fireblocks_config).await?;

    if holder != fireblocks_wallet {
        anyhow::bail!(
            "recorded custody holder {holder} is not the wallet the \
             configured Fireblocks workspace controls ({fireblocks_wallet}); \
             refusing to submit a transfer that cannot move the recorded \
             holder's receipts. Check FIREBLOCKS_* configuration."
        );
    }

    let recipient =
        CorroboratedRecipient::verify(&provider, recipient.address()).await?;

    let outcome = migrate_vault_receipts_via_fireblocks(
        &admin.pool,
        provider,
        &fireblocks_config,
        VaultIdentity { chain_id, vault, underlying: &args.underlying },
        fireblocks_wallet,
        recipient,
    )
    .await?;
    report_outcome(&outcome, recipient.address(), vault);

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
/// Existence implies validity: [`Recipient::for_holder`] is the only
/// constructor, so a caller cannot hold one without having rejected the two
/// destinations that make an irreversible move unrecoverable or pointless.
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
    use alloy::primitives::address;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    };

    const TEST_SIGNER_KEY: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000001";

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
                        vault: address!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
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
                    vault: address!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
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
                    8453,
                    address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                ),
                crate::receipt_inventory::ReceiptInventoryCommand::ConfirmCustody {
                    holder,
                },
            )
            .await
            .expect("Failed to seed custody");
    }

    fn turnkey_migrate_args(
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
            "8453",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error =
            run_migrate_receipts(args, |_| Ok(false)).await.unwrap_err();

        assert!(
            error.to_string().contains("aborted by operator"),
            "a declined confirmation must abort before any RPC, got {error}"
        );
    }

    /// The forward transfer is signed by the custodian through its API and the
    /// rollback by Turnkey; a local key can do neither, so the command refuses
    /// it outright instead of offering a transfer path production never has.
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

        let error =
            run_migrate_receipts(*args, |_| Ok(true)).await.unwrap_err();

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
            "8453",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts(args, |_| Ok(true)).await.unwrap_err();

        assert!(
            error.to_string().contains("no recorded custody holder"),
            "unobserved custody must demand confirm-custody, got {error}"
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
            "8453",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts(args, |_| Ok(true)).await.unwrap_err();

        assert!(
            error.to_string().contains("Fireblocks"),
            "a rollback without a working Fireblocks configuration must \
             refuse, got {error}"
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
            "84532",
            &database_url,
            secret.to_str().unwrap(),
        );

        let error = run_migrate_receipts(args, |_| Ok(true)).await.unwrap_err();

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
        assert!(
            IssuerCli::try_parse_from([
                "issuer",
                "migrate-receipts",
                "RKLB",
                "--network",
                "base",
                "--recipient",
                "0x00000000000000000000000000000000000000cc",
                "--rpc-url",
                "http://localhost:8545",
                "--evm-private-key",
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            ])
            .is_err(),
            "omitting --chain-id must fail at parse time rather than trusting \
             whatever chain the RPC happens to be"
        );
    }
}
