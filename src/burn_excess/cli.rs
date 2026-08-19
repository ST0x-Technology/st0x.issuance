//! Clap surface for `issuer burn-excess internal|external`.

use alloy::primitives::{B256, U256};
use alloy::providers::fillers::BlobGasFiller;
use alloy::providers::{Provider, ProviderBuilder};
use clap::{Args, Subcommand};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::io;
use std::str::FromStr;
use url::Url;

use super::engine::{BurnExcessRequest, run_burn_excess};
use super::proof::BurnExcessMode;
use crate::Quantity;
use crate::config::{
    DEFAULT_DATABASE_MAX_CONNECTIONS, DEFAULT_DATABASE_URL, configured_rpc_url,
    wss_to_http,
};
use crate::mint::IssuerMintRequestId;
use crate::tokenized_asset::Network;
use crate::vault::service::RealBlockchainService;
use crate::wallet::local::resolve_local_signer;
use crate::wallet::turnkey::resolve_turnkey_signer;
use crate::wallet::{SignerConfig, SignerEnv};

/// Parent command: path is a required mode subcommand (`internal` | `external`).
#[derive(Debug, Subcommand)]
pub(crate) enum BurnExcessCommand {
    /// Path A — excess shares already sit in the issuer wallet; no funding
    /// Transfer to exclude from the redemption poller.
    Internal(BurnExcessSharedArgs),
    /// Path B — prove a funding Transfer into the issuer, persist a poller
    /// exclusion for that log only, then burn.
    External(BurnExcessExternalArgs),
}

#[derive(Debug, Args)]
pub(crate) struct BurnExcessSharedArgs {
    /// Issuer mint request id bound by the deposit receiptInformation.
    #[arg(long)]
    issuer_request_id: IssuerMintRequestId,

    /// Deposit transaction that created the excess receipt/shares.
    #[arg(long)]
    deposit_tx_hash: B256,

    /// Excess receipt id from the deposit.
    #[arg(long)]
    receipt_id: U256,

    /// Excess share amount as a decimal (18-decimal fixed point on chain),
    /// e.g. `0.750`.
    #[arg(long, value_parser = parse_shares)]
    shares: U256,

    /// Why this recovery is being run; recorded on events.
    #[arg(long)]
    reason: String,

    /// Optional incident / ticket id for the audit trail.
    #[arg(long)]
    incident_id: Option<String>,

    /// Network of the vault listing (cross-checked with mint + chain-id).
    /// RPC is taken from the service environment for this network
    /// (`CHAIN_<NETWORK>_RPC_URL`, or legacy `RPC_URL` for Base) — same
    /// secrets as the long-running bot; not a CLI flag.
    #[arg(long, value_parser = Network::from_str)]
    network: Network,

    /// Chain id; must match `--network` and the configured RPC-reported chain.
    #[arg(long)]
    chain_id: u64,

    /// Perform mutations (exclusion / sign / broadcast). Default is dry-run.
    #[arg(long)]
    execute: bool,

    /// Close a dead Intended/Submitted/FundingExcluded stream instead of
    /// burning.
    #[arg(long)]
    close: bool,

    #[clap(flatten)]
    signer: SignerEnv,

    #[arg(
        long = "database-url",
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL
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

#[derive(Debug, Args)]
pub(crate) struct BurnExcessExternalArgs {
    #[clap(flatten)]
    shared: BurnExcessSharedArgs,

    /// Funding Transfer that moved excess shares into the issuer wallet.
    /// Required on `external`; not present on `internal`.
    #[arg(long)]
    funding_tx_hash: B256,
}

fn parse_shares(value: &str) -> Result<U256, String> {
    let decimal = rust_decimal::Decimal::from_str(value)
        .map_err(|error| format!("invalid shares decimal: {error}"))?;
    Quantity::new(decimal)
        .to_u256_with_18_decimals()
        .map_err(|error| error.to_string())
}

/// Dispatches `issuer burn-excess …` after clap has selected the mode keyword.
pub(crate) async fn run_burn_excess_cli(
    command: BurnExcessCommand,
    confirm: impl Fn(&str) -> io::Result<bool> + Send + Sync,
) -> anyhow::Result<()> {
    let (mode, shared, funding_tx_hash) = match command {
        BurnExcessCommand::Internal(shared) => {
            (BurnExcessMode::Internal, shared, None)
        }
        BurnExcessCommand::External(external) => (
            BurnExcessMode::External,
            external.shared,
            Some(external.funding_tx_hash),
        ),
    };

    if shared.chain_id != shared.network.chain_id() {
        anyhow::bail!(
            "--network {} is chain {} but --chain-id is {}",
            shared.network,
            shared.network.chain_id(),
            shared.chain_id
        );
    }

    // Same process environment the service loads (issuer bin calls dotenv).
    // Grouped CHAIN_* wins for Base when both forms are present.
    let rpc_url = configured_rpc_url(shared.network)?;

    println!("Using database: {}", shared.database_url);
    println!(
        "Using configured RPC for network {} (host {})",
        shared.network,
        rpc_url.host_str().unwrap_or("(none)")
    );
    let pool =
        connect_pool(&shared.database_url, shared.database_max_connections)
            .await?;

    let chain_id = verified_chain_id(&rpc_url, shared.chain_id).await?;
    let signer_config = shared.signer.into_config()?;
    let issuer_wallet = signer_config.address()?;

    let resolved = match &signer_config {
        SignerConfig::Local(key) => resolve_local_signer(key, chain_id)?,
        SignerConfig::Turnkey(config) => {
            resolve_turnkey_signer(config, chain_id)?
        }
    };

    let http_url = wss_to_http(&rpc_url)?;
    let signing_provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .with_gas_estimation()
        .filler(BlobGasFiller)
        .with_simple_nonce_management()
        .with_chain_id(chain_id)
        .wallet(resolved.wallet)
        .connect_http(http_url.clone());

    let vault_service = RealBlockchainService::new(signing_provider);

    let read_provider = ProviderBuilder::new().connect_http(http_url);

    let request = BurnExcessRequest {
        mode,
        issuer_request_id: shared.issuer_request_id,
        deposit_tx_hash: shared.deposit_tx_hash,
        funding_tx_hash,
        receipt_id: shared.receipt_id,
        shares: shared.shares,
        reason: shared.reason,
        incident_id: shared.incident_id,
        network: shared.network,
        chain_id,
        execute: shared.execute,
        close: shared.close,
    };

    run_burn_excess(
        &pool,
        &vault_service,
        &read_provider,
        issuer_wallet,
        request,
        confirm,
    )
    .await
    .map_err(Into::into)
}

async fn connect_pool(
    database_url: &str,
    max_connections: u32,
) -> anyhow::Result<Pool<Sqlite>> {
    if !database_url.starts_with("sqlite:") {
        anyhow::bail!(
            "database URL must use the sqlite: scheme, got: {database_url}"
        );
    }
    // `create_if_missing(false)` cannot guard an in-memory URL: sqlx sets
    // `in_memory` for `:memory:` and `mode=memory` and always passes
    // SQLITE_OPEN_MEMORY, so the open succeeds against a fresh empty database
    // with no event history — exactly the state the guard below exists to
    // prevent.
    if database_url.contains(":memory:") || database_url.contains("mode=memory")
    {
        anyhow::bail!(
            "burn-excess requires an existing on-disk issuance database, got \
             in-memory URL: {database_url}"
        );
    }
    // burn-excess is operational against an existing issuance DB — never create
    // an empty file that would look like a successful open with no history.
    let options =
        SqliteConnectOptions::from_str(database_url)?.create_if_missing(false);
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "failed to open database at {database_url} (file must already \
                 exist for burn-excess): {error}"
            )
        })
}

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
            "--chain-id is {expected_chain_id} but configured RPC reports \
             chain {chain_id}"
        );
    }

    Ok(chain_id)
}

#[cfg(test)]
mod tests {
    use super::connect_pool;

    #[tokio::test]
    async fn connect_pool_rejects_in_memory_urls() {
        for url in [
            "sqlite::memory:",
            "sqlite:file::memory:",
            "sqlite:file:shared?mode=memory&cache=shared",
        ] {
            let error = connect_pool(url, 1).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("existing on-disk issuance database"),
                "in-memory URL {url} must be refused, got: {error}"
            );
        }
    }
}
