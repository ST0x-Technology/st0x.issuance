use alloy::primitives::Address;
use alloy::providers::Provider;
use clap::{Args, Parser};
use st0x_issuance_dto::{UnderlyingSymbol, UnderlyingSymbolError};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{Level, warn};
use url::Url;

use crate::alpaca::service::AlpacaConfig;
use crate::auth::AuthConfig;
use crate::chain::{
    ChainConfig, ChainRegistry, ChainRegistryError, build_chain_registry,
};
use crate::telemetry::{HyperDxApiKey, HyperDxConfig};
use crate::tokenized_asset::Network;
use crate::wallet::{SignerConfig, SignerConfigError, SignerEnv};

/// How a specific tokenized asset's mint/burn is executed on-chain.
///
/// `VaultDirect` calls the `OffchainAssetReceiptVault` directly (existing
/// behaviour). `Orchestrator` routes through the `ST0xOrchestrator` contract
/// at the given address, which handles the EIP-712 mint-auth and the
/// receipt-walk for burns.
///
/// The serde representation (`"VaultDirect"` /
/// `{"Orchestrator":{"address":"0x…"}}`) is embedded in persisted redemption
/// events and is therefore a permanent event schema — it must never change.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum VaultMode {
    #[default]
    VaultDirect,
    Orchestrator {
        address: Address,
    },
}

/// Resolved per-asset vault-mode configuration loaded from the optional TOML
/// config file. Defaults to all-`VaultDirect` when no file is provided.
#[derive(Debug, Clone, Default)]
pub struct VaultModeConfig {
    /// Per-asset overrides keyed by the underlying symbol string (e.g. "AAPL").
    per_asset: HashMap<String, VaultMode>,
    /// Fallback used for any asset not listed in `per_asset`.
    default: VaultMode,
}

impl VaultModeConfig {
    #[must_use]
    pub const fn new(
        per_asset: HashMap<String, VaultMode>,
        default: VaultMode,
    ) -> Self {
        Self { per_asset, default }
    }

    /// Returns the `VaultMode` for the given underlying asset symbol.
    ///
    /// Uses the per-asset override from the TOML config if present, otherwise
    /// falls back to the configured default (which itself defaults to
    /// `VaultDirect` when no TOML file is provided).
    #[must_use]
    pub fn mode_for(&self, underlying: &UnderlyingSymbol) -> VaultMode {
        self.per_asset.get(underlying.as_str()).copied().unwrap_or(self.default)
    }
}

/// Default chain ID (Base mainnet)
pub const DEFAULT_CHAIN_ID: u64 = 8453;

/// Default SQLite database URL when `DATABASE_URL` is unset. Production overrides
/// it via the environment (see `docker-compose.template.yaml`). Single source of
/// truth shared by the server config and the issuer CLI.
pub(crate) const DEFAULT_DATABASE_URL: &str = "sqlite:data.db";

/// Default SQLite connection-pool size, shared by the server config and the
/// issuer CLI.
pub(crate) const DEFAULT_DATABASE_MAX_CONNECTIONS: u32 = 5;

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub database_max_connections: u32,
    pub rpc_url: Url,
    pub chain_id: u64,
    pub signer: SignerConfig,
    pub backfill_start_block: u64,
    /// Interval between periodic receipt-backfill passes. Defaults to
    /// `RECEIPT_POLL_INTERVAL` in production; tests lower it so they don't
    /// have to wait a full production interval for a reconciliation pass.
    pub receipt_poll_interval: Duration,
    pub auth: AuthConfig,
    pub log_level: LogLevel,
    pub environment: Environment,
    pub hyperdx: Option<HyperDxConfig>,
    pub alpaca: AlpacaConfig,
    pub subgraph_url: Url,
    /// All chain runtimes the registry is built from, Base first. Legacy flat
    /// env vars preserve the Base-only path; complete `CHAIN_<NETWORK>_*`
    /// groups override Base or append additional networks.
    pub chains: Vec<ChainConfig>,
    pub vault_mode_config: VaultModeConfig,
}

impl Config {
    /// Parses configuration from environment variables and command-line arguments.
    ///
    /// # Errors
    ///
    /// Returns an error if command-line arguments or environment variables are invalid.
    pub fn parse() -> Result<Self, ConfigError> {
        let env = Env::try_parse()?;
        env.into_config()
    }

    /// Builds a [`ChainRegistry`] from `chains`. Consumers route per-chain
    /// side effects through `registry.get(network)`; anything not yet
    /// migrated to network-aware routing pins itself to `registry.base()`
    /// at its own call site.
    pub(crate) async fn create_chain_registry(
        &self,
    ) -> Result<ChainRegistry<impl Provider + Clone + use<>>, ConfigError> {
        build_chain_registry(self.chains.clone(), &self.signer)
            .await
            .map_err(|error| ConfigError::ChainRegistry(Box::new(error)))
    }

    /// Returns the `VaultMode` for the given underlying asset symbol.
    #[must_use]
    pub fn vault_mode_for(&self, underlying: &UnderlyingSymbol) -> VaultMode {
        self.vault_mode_config.mode_for(underlying)
    }
}

#[derive(Parser, Clone)]
#[command(name = "st0x-issuance")]
#[command(about = "Issuance bot for tokenizing equities via Alpaca ITN")]
struct Env {
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = DEFAULT_DATABASE_URL,
        help = "SQLite database URL"
    )]
    database_url: String,

    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        help = "Maximum number of database connections in the pool"
    )]
    database_max_connections: u32,

    #[arg(
        long,
        env = "RPC_URL",
        required_unless_present = "chain_base_rpc_url",
        help = "WebSocket RPC endpoint URL (wss://...)"
    )]
    rpc_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_ID",
        default_value_t = DEFAULT_CHAIN_ID,
        help = "Chain ID for signing transactions (default: Base mainnet)"
    )]
    chain_id: u64,

    #[clap(flatten)]
    signer: SignerEnv,

    #[arg(
        long,
        env = "BACKFILL_START_BLOCK",
        default_value = "41704326",
        help = "Block number from which to start backfilling receipts"
    )]
    backfill_start_block: u64,

    #[clap(flatten)]
    auth: AuthConfig,

    #[clap(long, env, default_value = "debug")]
    log_level: LogLevel,

    #[arg(
        long,
        env = "ENVIRONMENT",
        default_value = "production",
        help = "Deployment environment; gates dev-only surfaces such as the \
                OpenAPI docs (one of: development, staging, production)"
    )]
    environment: Environment,

    #[clap(flatten)]
    hyperdx: HyperDxEnv,

    #[clap(flatten)]
    pub(crate) alpaca: AlpacaConfig,

    #[arg(
        long,
        env = "SUBGRAPH_URL",
        required_unless_present = "chain_base_rpc_url",
        help = "Goldsky subgraph URL for querying OA schema hashes"
    )]
    subgraph_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_BASE_RPC_URL",
        requires_all = [
            "chain_base_chain_id",
            "chain_base_subgraph_url",
            "chain_base_backfill_start_block"
        ],
        help = "Base RPC endpoint; setting it requires the full CHAIN_BASE_* \
                group and overrides the legacy flat Base variables"
    )]
    chain_base_rpc_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_BASE_CHAIN_ID",
        requires = "chain_base_rpc_url",
        help = "Chain ID for the Base group; must be Base's canonical 8453"
    )]
    chain_base_chain_id: Option<u64>,

    #[arg(
        long,
        env = "CHAIN_BASE_SUBGRAPH_URL",
        requires = "chain_base_rpc_url",
        help = "Goldsky subgraph URL for the Base group (http or https)"
    )]
    chain_base_subgraph_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_BASE_BACKFILL_START_BLOCK",
        requires = "chain_base_rpc_url",
        help = "Receipt-backfill start block for the Base group"
    )]
    chain_base_backfill_start_block: Option<u64>,

    #[arg(
        long,
        env = "CHAIN_ETHEREUM_RPC_URL",
        requires_all = [
            "chain_ethereum_chain_id",
            "chain_ethereum_subgraph_url",
            "chain_ethereum_backfill_start_block"
        ],
        help = "Ethereum RPC endpoint; setting it requires the full \
                CHAIN_ETHEREUM_* group and enables the Ethereum chain"
    )]
    chain_ethereum_rpc_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_ETHEREUM_CHAIN_ID",
        requires = "chain_ethereum_rpc_url",
        help = "Chain ID for the Ethereum group; must be Ethereum's canonical 1"
    )]
    chain_ethereum_chain_id: Option<u64>,

    #[arg(
        long,
        env = "CHAIN_ETHEREUM_SUBGRAPH_URL",
        requires = "chain_ethereum_rpc_url",
        help = "Goldsky subgraph URL for the Ethereum group (http or https)"
    )]
    chain_ethereum_subgraph_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_ETHEREUM_BACKFILL_START_BLOCK",
        requires = "chain_ethereum_rpc_url",
        help = "Receipt-backfill start block for the Ethereum group"
    )]
    chain_ethereum_backfill_start_block: Option<u64>,

    #[arg(
        long,
        env = "CHAIN_HYPEREVM_RPC_URL",
        requires_all = [
            "chain_hyperevm_chain_id",
            "chain_hyperevm_subgraph_url",
            "chain_hyperevm_backfill_start_block"
        ],
        help = "HyperEVM RPC endpoint; setting it requires the full \
                CHAIN_HYPEREVM_* group and enables the HyperEVM chain"
    )]
    chain_hyperevm_rpc_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_HYPEREVM_CHAIN_ID",
        requires = "chain_hyperevm_rpc_url",
        help = "Chain ID for the HyperEVM group; must be HyperEVM's \
                canonical 999"
    )]
    chain_hyperevm_chain_id: Option<u64>,

    #[arg(
        long,
        env = "CHAIN_HYPEREVM_SUBGRAPH_URL",
        requires = "chain_hyperevm_rpc_url",
        help = "Goldsky subgraph URL for the HyperEVM group (http or https)"
    )]
    chain_hyperevm_subgraph_url: Option<Url>,

    #[arg(
        long,
        env = "CHAIN_HYPEREVM_BACKFILL_START_BLOCK",
        requires = "chain_hyperevm_rpc_url",
        help = "Receipt-backfill start block for the HyperEVM group"
    )]
    chain_hyperevm_backfill_start_block: Option<u64>,

    #[arg(
        long,
        env = "CONFIG",
        help = "Path to TOML configuration file for orchestrator/vault-mode settings. \
                Omitting this arg (or providing a file with no [orchestrator] section) \
                keeps every asset in vault-direct mode (safe default)."
    )]
    config: Option<PathBuf>,
}

impl Env {
    fn into_config(self) -> Result<Config, ConfigError> {
        let log_level_tracing = (&self.log_level).into();
        let (base, chains) = self.chain_configs()?;
        let rpc_url = base.rpc_url.clone();
        let chain_id = base.chain_id;
        let subgraph_url = base.subgraph_url.clone();
        let backfill_start_block = base.backfill_start_block;
        let signer = self.signer.into_config()?;
        let hyperdx = self.hyperdx.into_config(log_level_tracing);
        let vault_mode_config = if let Some(config_path) = self.config {
            let content =
                std::fs::read_to_string(&config_path).map_err(|error| {
                    ConfigError::ConfigFileRead { path: config_path, error }
                })?;
            let toml_file: TomlFile = toml::from_str(&content)?;
            resolve_vault_modes(&toml_file)?
        } else {
            VaultModeConfig::default()
        };

        Ok(Config {
            database_url: self.database_url,
            database_max_connections: self.database_max_connections,
            rpc_url,
            chain_id,
            signer,
            backfill_start_block,
            receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
            auth: self.auth,
            log_level: self.log_level,
            environment: self.environment,
            hyperdx,
            alpaca: self.alpaca,
            subgraph_url,
            chains,
            vault_mode_config,
        })
    }

    /// Returns the Base runtime and the full chain list, Base first.
    ///
    /// Base is returned separately rather than left for the caller to pluck
    /// out of the vector: Base is always present, and recovering it by index
    /// would leave that guarantee resting on ordering convention with an
    /// unreachable error path standing in for a case the type already rules
    /// out.
    fn chain_configs(
        &self,
    ) -> Result<(ChainConfig, Vec<ChainConfig>), ConfigError> {
        let base = if let Some(base) = Self::optional_chain_config(
            Network::Base,
            &ChainGroupEnv {
                rpc_url: self.chain_base_rpc_url.as_ref(),
                chain_id: self.chain_base_chain_id,
                subgraph_url: self.chain_base_subgraph_url.as_ref(),
                backfill_start_block: self.chain_base_backfill_start_block,
            },
        )? {
            // Both forms set is a legitimate state — the legacy flat vars
            // stay in the environment for operator tooling — but the service
            // itself must not silently split-brain between them, so the
            // precedence is stated once, loudly.
            if self.rpc_url.is_some() || self.subgraph_url.is_some() {
                warn!(
                    target: "config",
                    rpc_url = %base.rpc_url,
                    subgraph_url = %base.subgraph_url,
                    chain_id = base.chain_id,
                    "Both legacy (RPC_URL/SUBGRAPH_URL) and grouped \
                     (CHAIN_BASE_*) Base configuration are set; the grouped \
                     form takes precedence for the service"
                );
            }
            base
        } else {
            // Clap's `required_unless_present = "chain_base_rpc_url"` on the
            // legacy pair makes these `ok_or`s unreachable from a parsed
            // `Env`; they stay as a defensive guard so a future edit to those
            // clap attributes degrades to a startup error instead of a panic.
            let rpc_url = self
                .rpc_url
                .clone()
                .ok_or(ConfigError::MissingBaseChainConfiguration)?;
            let subgraph_url = self
                .subgraph_url
                .clone()
                .ok_or(ConfigError::MissingBaseChainConfiguration)?;
            validate_subgraph_scheme("SUBGRAPH_URL", &subgraph_url)?;

            ChainConfig {
                network: Network::Base,
                chain_id: self.chain_id,
                rpc_url,
                subgraph_url,
                backfill_start_block: self.backfill_start_block,
            }
        };
        let ethereum = Self::optional_chain_config(
            Network::Ethereum,
            &ChainGroupEnv {
                rpc_url: self.chain_ethereum_rpc_url.as_ref(),
                chain_id: self.chain_ethereum_chain_id,
                subgraph_url: self.chain_ethereum_subgraph_url.as_ref(),
                backfill_start_block: self.chain_ethereum_backfill_start_block,
            },
        )?;
        let hyperevm = Self::optional_chain_config(
            Network::HyperEvm,
            &ChainGroupEnv {
                rpc_url: self.chain_hyperevm_rpc_url.as_ref(),
                chain_id: self.chain_hyperevm_chain_id,
                subgraph_url: self.chain_hyperevm_subgraph_url.as_ref(),
                backfill_start_block: self.chain_hyperevm_backfill_start_block,
            },
        )?;
        let mut chains = vec![base.clone()];
        chains.extend(ethereum);
        chains.extend(hyperevm);

        Ok((base, chains))
    }

    fn optional_chain_config(
        network: Network,
        group: &ChainGroupEnv<'_>,
    ) -> Result<Option<ChainConfig>, ConfigError> {
        let &ChainGroupEnv {
            rpc_url,
            chain_id,
            subgraph_url,
            backfill_start_block,
        } = group;

        match (rpc_url, chain_id, subgraph_url, backfill_start_block) {
            (None, None, None, None) => Ok(None),
            (
                Some(rpc_url),
                Some(chain_id),
                Some(subgraph_url),
                Some(backfill_start_block),
            ) => {
                validate_subgraph_scheme(
                    subgraph_variable(network),
                    subgraph_url,
                )?;

                // A network label bound to a chain it does not name is not a
                // recoverable misconfiguration: the receipt inventory is keyed
                // `{chain_id}:{vault}`, so starting `Network::Base` on a
                // testnet id silently orphans every existing Base receipt
                // aggregate while every other check still passes. The RPC
                // cross-check in `chain::build_chain_runtime` only proves the
                // endpoint agrees with the configured id, not that the id
                // belongs to the network.
                //
                // Enforced on the grouped configuration only. The legacy flat
                // variables are how local development points a Base runtime at
                // Anvil, and production uses the grouped form.
                if chain_id != network.chain_id() {
                    return Err(ConfigError::ChainIdNotForNetwork {
                        network,
                        configured: chain_id,
                        expected: network.chain_id(),
                    });
                }

                Ok(Some(ChainConfig {
                    network,
                    chain_id,
                    rpc_url: rpc_url.clone(),
                    subgraph_url: subgraph_url.clone(),
                    backfill_start_block,
                }))
            }
            _ => Err(ConfigError::ParseError(clap::Error::new(
                clap::error::ErrorKind::MissingRequiredArgument,
            ))),
        }
    }
}

/// One network's `CHAIN_<NETWORK>_*` group, as read from the environment.
///
/// Named fields rather than positional arguments because `rpc_url` and
/// `subgraph_url` are both `Option<&Url>` and `chain_id` and
/// `backfill_start_block` are both `Option<u64>`: transposing either pair would
/// compile silently and produce a runtime pointed at the wrong endpoint.
struct ChainGroupEnv<'env> {
    rpc_url: Option<&'env Url>,
    chain_id: Option<u64>,
    subgraph_url: Option<&'env Url>,
    backfill_start_block: Option<u64>,
}

/// Validates a subgraph URL's scheme, naming the variable it came from.
///
/// With several chains configured, an error that only ever names `SUBGRAPH_URL`
/// sends an operator to the wrong variable; `variable` is what makes the
/// message actionable.
fn validate_subgraph_scheme(
    variable: &'static str,
    subgraph_url: &Url,
) -> Result<(), ConfigError> {
    match subgraph_url.scheme() {
        "http" | "https" => Ok(()),
        scheme => Err(ConfigError::InvalidSubgraphScheme {
            variable,
            scheme: scheme.to_string(),
        }),
    }
}

/// The subgraph environment variable backing a network's chain group.
const fn subgraph_variable(network: Network) -> &'static str {
    match network {
        Network::Base => "CHAIN_BASE_SUBGRAPH_URL",
        Network::Ethereum => "CHAIN_ETHEREUM_SUBGRAPH_URL",
        Network::HyperEvm => "CHAIN_HYPEREVM_SUBGRAPH_URL",
    }
}

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for Level {
    fn from(log_level: LogLevel) -> Self {
        match log_level {
            LogLevel::Trace => Self::TRACE,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Info => Self::INFO,
            LogLevel::Warn => Self::WARN,
            LogLevel::Error => Self::ERROR,
        }
    }
}

impl From<&LogLevel> for Level {
    fn from(log_level: &LogLevel) -> Self {
        match log_level {
            LogLevel::Trace => Self::TRACE,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Info => Self::INFO,
            LogLevel::Warn => Self::WARN,
            LogLevel::Error => Self::ERROR,
        }
    }
}

/// Deployment environment. Gates developer-facing surfaces that must not be
/// exposed in production. Defaults to `Production` so an unset `ENVIRONMENT`
/// fails closed (docs hidden) rather than open.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Development,
    Staging,
    Production,
}

impl Environment {
    /// Whether the interactive OpenAPI docs (SwaggerUI + `/api-docs/openapi.json`)
    /// should be served. They expose the full internal/admin API surface, so they
    /// are served only outside production.
    pub(crate) const fn exposes_api_docs(self) -> bool {
        match self {
            Self::Development | Self::Staging => true,
            Self::Production => false,
        }
    }
}

#[derive(Args, Debug, Clone)]
struct HyperDxEnv {
    #[clap(long, env)]
    hyperdx_api_key: Option<String>,
    #[clap(long, env, default_value = "st0x-issuance")]
    hyperdx_service_name: String,
}

impl HyperDxEnv {
    fn into_config(self, log_level: Level) -> Option<HyperDxConfig> {
        self.hyperdx_api_key.map(|api_key| HyperDxConfig {
            api_key: HyperDxApiKey::new(api_key),
            service_name: self.hyperdx_service_name,
            log_level,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Signer configuration error")]
    SignerConfig(#[from] SignerConfigError),
    #[error("Failed to parse configuration: {0}")]
    ParseError(#[from] clap::Error),
    #[error("{variable} must use http or https scheme, got: {scheme}")]
    InvalidSubgraphScheme { variable: &'static str, scheme: String },

    #[error("Base chain configuration is required")]
    MissingBaseChainConfiguration,
    #[error(
        "CHAIN_{}_CHAIN_ID is {configured} but {network} is chain {expected}; \
         a network bound to the wrong chain would re-key the receipt inventory",
        network.as_str().to_uppercase()
    )]
    ChainIdNotForNetwork { network: Network, configured: u64, expected: u64 },
    #[error(
        "no RPC URL configured for {network}; set {hint} in the service \
         environment (deployment secrets / .env)"
    )]
    NetworkRpcNotConfigured { network: Network, hint: &'static str },
    #[error("configured RPC URL for {network} is not a valid URL: {source}")]
    InvalidConfiguredRpcUrl {
        network: Network,
        #[source]
        source: url::ParseError,
    },
    #[error("chain registry initialization failed: {0}")]
    ChainRegistry(#[source] Box<ChainRegistryError>),
    #[error("Failed to read config file '{path}': {error}")]
    ConfigFileRead {
        path: PathBuf,
        #[source]
        error: std::io::Error,
    },
    #[error("Failed to parse toml config file: {0}")]
    Toml(#[from] toml::de::Error),
    #[error(
        "[orchestrator].address is required when any asset resolves to \
         orchestrator mode"
    )]
    MissingOrchestratorAddress,
    #[error("Invalid [orchestrator].address '{0}': not a valid EVM address")]
    InvalidOrchestratorAddress(String),
    #[error("Invalid [assets] key '{symbol}': {error}")]
    InvalidAssetSymbol {
        symbol: String,
        #[source]
        error: UnderlyingSymbolError,
    },
    #[error(
        "multiple [assets] keys normalize to '{symbol}'; keep exactly one \
         entry per asset"
    )]
    DuplicateAssetSymbol { symbol: String },
}

// Sourced from the file given to `--config`.  All structs carry
// `deny_unknown_fields` so a typo in the config is a startup error rather than
// a silent no-op.

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlFile {
    orchestrator: Option<OrchestratorSection>,
    #[serde(default)]
    assets: HashMap<String, AssetSection>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OrchestratorSection {
    address: Option<String>,
    default_vault_mode: Option<VaultModeStr>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AssetSection {
    vault_mode: VaultModeStr,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum VaultModeStr {
    VaultDirect,
    Orchestrator,
}

/// Converts the raw TOML file into a validated `VaultModeConfig`.
///
/// Validation rules:
/// - `default_vault_mode = "orchestrator"` requires `[orchestrator].address`.
/// - Any `[assets.<X>].vault_mode = "orchestrator"` requires
///   `[orchestrator].address`.
/// - An unknown `vault_mode` string fails via serde (see `VaultModeStr`).
/// - `[assets.<X>]` keys are validated as underlying symbols and normalized
///   to upper case (matching how assets are keyed everywhere else), so
///   `[assets.rklb]` configures RKLB instead of silently configuring
///   nothing; `deny_unknown_fields` cannot catch map keys, so this is where
///   a typo'd key becomes a startup error instead of a silent no-op. Two
///   keys normalizing to the same symbol are rejected rather than one
///   silently winning.
fn resolve_vault_modes(
    toml: &TomlFile,
) -> Result<VaultModeConfig, ConfigError> {
    let orchestrator_address =
        match toml.orchestrator.as_ref().and_then(|o| o.address.as_ref()) {
            Some(addr_str) => {
                let address = addr_str.parse::<Address>().map_err(|_| {
                    ConfigError::InvalidOrchestratorAddress(addr_str.clone())
                })?;
                if address.is_zero() {
                    return Err(ConfigError::InvalidOrchestratorAddress(
                        addr_str.clone(),
                    ));
                }
                Some(address)
            }
            None => None,
        };

    let resolve_mode =
        |mode_str: &VaultModeStr| -> Result<VaultMode, ConfigError> {
            match mode_str {
                VaultModeStr::VaultDirect => Ok(VaultMode::VaultDirect),
                VaultModeStr::Orchestrator => {
                    let address = orchestrator_address
                        .ok_or(ConfigError::MissingOrchestratorAddress)?;
                    Ok(VaultMode::Orchestrator { address })
                }
            }
        };

    let default = match toml
        .orchestrator
        .as_ref()
        .and_then(|o| o.default_vault_mode.as_ref())
    {
        None => VaultMode::VaultDirect,
        Some(mode_str) => resolve_mode(mode_str)?,
    };

    let mut per_asset = HashMap::new();
    for (symbol, asset_section) in &toml.assets {
        let normalized = UnderlyingSymbol::new(symbol.to_ascii_uppercase())
            .map_err(|error| ConfigError::InvalidAssetSymbol {
                symbol: symbol.clone(),
                error,
            })?
            .as_str()
            .to_string();

        let mode = resolve_mode(&asset_section.vault_mode)?;
        if per_asset.insert(normalized.clone(), mode).is_some() {
            return Err(ConfigError::DuplicateAssetSymbol {
                symbol: normalized,
            });
        }
    }

    Ok(VaultModeConfig { per_asset, default })
}

/// RPC URL uses a scheme that cannot be mapped to HTTP.
#[derive(Debug, thiserror::Error)]
#[error("Cannot derive HTTP URL from RPC URL: {0}")]
pub struct InvalidRpcScheme(String);

/// Derives an HTTP URL from a WebSocket URL by replacing the scheme.
pub(crate) fn wss_to_http(url: &Url) -> Result<Url, InvalidRpcScheme> {
    let new_scheme = match url.scheme() {
        "wss" => "https",
        "ws" => "http",
        "http" | "https" => return Ok(url.clone()),
        other => return Err(InvalidRpcScheme(other.to_string())),
    };

    let mut http_url = url.clone();
    http_url
        .set_scheme(new_scheme)
        .map_err(|()| InvalidRpcScheme(url.scheme().to_string()))?;

    Ok(http_url)
}

/// Resolves the service RPC URL for `network` from process environment
/// (deployment secrets / `.env` — the same variables the long-running bot
/// loads). Operator CLIs call this instead of taking `--rpc-url`.
///
/// Precedence matches service config: for Base, `CHAIN_BASE_RPC_URL` wins over
/// legacy `RPC_URL`; other networks use only their `CHAIN_<NETWORK>_RPC_URL`.
pub(crate) fn configured_rpc_url(network: Network) -> Result<Url, ConfigError> {
    resolve_configured_rpc_url(network, |name| {
        std::env::var(name).ok().filter(|value| !value.is_empty())
    })
}

fn resolve_configured_rpc_url(
    network: Network,
    env_get: impl Fn(&str) -> Option<String>,
) -> Result<Url, ConfigError> {
    let (primary, fallback, hint) = match network {
        Network::Base => (
            "CHAIN_BASE_RPC_URL",
            Some("RPC_URL"),
            "CHAIN_BASE_RPC_URL or RPC_URL",
        ),
        Network::Ethereum => {
            ("CHAIN_ETHEREUM_RPC_URL", None, "CHAIN_ETHEREUM_RPC_URL")
        }
        Network::HyperEvm => {
            ("CHAIN_HYPEREVM_RPC_URL", None, "CHAIN_HYPEREVM_RPC_URL")
        }
    };

    let raw = env_get(primary)
        .or_else(|| fallback.and_then(&env_get))
        .ok_or(ConfigError::NetworkRpcNotConfigured { network, hint })?;

    Url::parse(&raw).map_err(|source| ConfigError::InvalidConfiguredRpcUrl {
        network,
        source,
    })
}

/// Domain target categories used in `target:` on all tracing macros.
/// The default `EnvFilter` must include these so logs are not silenced.
const DOMAIN_TARGETS: &[&str] = &[
    "startup",
    "mint",
    "redemption",
    "receipt",
    "account",
    "asset",
    "alpaca",
    "auth",
    "wallet",
    "admin",
    "vault",
];

/// Builds a default `EnvFilter` string that includes both the crate module
/// path and all custom domain targets at the given level.
pub(crate) fn default_log_filter(level: Level) -> String {
    let mut parts = vec![format!("st0x_issuance={level}")];

    for target in DOMAIN_TARGETS {
        parts.push(format!("{target}={level}"));
    }

    parts.join(",")
}

pub fn setup_tracing(log_level: &LogLevel) {
    let level: Level = log_level.into();
    let default_filter = default_log_filter(level);

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .try_init();
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use ipnetwork::IpNetwork;

    use super::*;
    use crate::auth::IpWhitelist;

    fn minimal_args() -> Vec<&'static str> {
        vec![
            "test-binary",
            "--rpc-url",
            "wss://localhost:8545",
            "--evm-private-key",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "--backfill-start-block",
            "12345678",
            "--issuer-api-key",
            "test-key-that-is-at-least-32-chars-long",
            "--alpaca-account-id",
            "test-alpaca-account-id",
            "--alpaca-api-key",
            "alpaca-test-key",
            "--alpaca-api-secret",
            "alpaca-test-secret",
            "--subgraph-url",
            "http://localhost:0/subgraph",
        ]
    }

    fn remove_argument(args: &mut Vec<&str>, argument: &str) {
        let position =
            args.iter().position(|candidate| *candidate == argument).unwrap();
        args.drain(position..=position + 1);
    }

    #[test]
    fn into_config_populates_base_chain_entry_from_legacy_env_vars() {
        let env = Env::try_parse_from(minimal_args()).unwrap();
        let config = env.into_config().unwrap();

        assert_eq!(config.chains.len(), 1);
        let chain = &config.chains[0];
        assert_eq!(chain.network, Network::Base);
        assert_eq!(chain.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(chain.rpc_url, config.rpc_url);
        assert_eq!(chain.subgraph_url, config.subgraph_url);
        assert_eq!(chain.backfill_start_block, config.backfill_start_block);
    }

    #[test]
    fn into_config_keeps_additional_chains_disabled_with_explicit_base_config()
    {
        let mut args = minimal_args();
        remove_argument(&mut args, "--rpc-url");
        remove_argument(&mut args, "--subgraph-url");
        args.extend_from_slice(&[
            "--chain-base-rpc-url",
            "wss://base.example",
            "--chain-base-chain-id",
            "8453",
            "--chain-base-subgraph-url",
            "https://base-subgraph.example",
            "--chain-base-backfill-start-block",
            "42000000",
        ]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        assert_eq!(config.chains.len(), 1);
        let base = &config.chains[0];
        assert_eq!(base.network, Network::Base);
        assert_eq!(base.chain_id, 8453);
        assert_eq!(base.rpc_url, Url::parse("wss://base.example").unwrap());
        assert_eq!(
            base.subgraph_url,
            Url::parse("https://base-subgraph.example").unwrap()
        );
        assert_eq!(base.backfill_start_block, 42_000_000);
    }

    /// The migration-window environment carries both forms — legacy
    /// `RPC_URL` for the operator CLI and the grouped `CHAIN_BASE_*` for the
    /// service — so both set must resolve deterministically to the grouped
    /// values, never a silent mix.
    #[test]
    fn grouped_base_config_takes_precedence_over_legacy_vars() {
        let mut args = minimal_args();
        args.extend_from_slice(&[
            "--chain-base-rpc-url",
            "wss://base-grouped.example",
            "--chain-base-chain-id",
            "8453",
            "--chain-base-subgraph-url",
            "https://base-grouped-subgraph.example",
            "--chain-base-backfill-start-block",
            "42000000",
        ]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        assert_eq!(config.chains.len(), 1);
        let base = &config.chains[0];
        assert_eq!(
            base.rpc_url,
            Url::parse("wss://base-grouped.example").unwrap(),
            "the grouped RPC endpoint must win over the legacy RPC_URL"
        );
        assert_eq!(
            base.subgraph_url,
            Url::parse("https://base-grouped-subgraph.example").unwrap(),
            "the grouped subgraph must win over the legacy SUBGRAPH_URL"
        );
        assert_eq!(base.chain_id, 8453);
    }

    /// The network label bound to a chain it does not name is refused: the
    /// receipt inventory is keyed `{chain_id}:{vault}`, so a mislabeled
    /// network would silently orphan every existing aggregate. The same
    /// refusal arriving through the real environment variables is pinned at
    /// the binary boundary in `tests/config.rs`
    /// (`ethereum_group_bound_to_the_wrong_chain_id_fails_validation`); this
    /// leaf test keeps the exhaustive error-shape assertion where the check
    /// lives.
    #[test]
    fn grouped_config_with_wrong_chain_id_is_refused() {
        let result = Env::optional_chain_config(
            Network::Ethereum,
            &ChainGroupEnv {
                rpc_url: Some(&Url::parse("wss://ethereum.example").unwrap()),
                chain_id: Some(8453),
                subgraph_url: Some(
                    &Url::parse("https://ethereum-subgraph.example").unwrap(),
                ),
                backfill_start_block: Some(22_000_000),
            },
        );

        assert!(
            matches!(
                result,
                Err(ConfigError::ChainIdNotForNetwork {
                    network: Network::Ethereum,
                    configured: 8453,
                    expected: 1,
                })
            ),
            "a Base chain id under the Ethereum label must be refused, got \
             {result:?}"
        );
    }

    #[test]
    fn into_config_adds_complete_ethereum_chain_config() {
        let mut args = minimal_args();
        args.extend_from_slice(&[
            "--chain-ethereum-rpc-url",
            "wss://ethereum.example",
            "--chain-ethereum-chain-id",
            "1",
            "--chain-ethereum-subgraph-url",
            "https://ethereum-subgraph.example",
            "--chain-ethereum-backfill-start-block",
            "22000000",
        ]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        assert_eq!(config.chains.len(), 2);
        let ethereum = &config.chains[1];
        assert_eq!(ethereum.network, Network::Ethereum);
        assert_eq!(ethereum.chain_id, 1);
        assert_eq!(
            ethereum.rpc_url,
            Url::parse("wss://ethereum.example").unwrap()
        );
        assert_eq!(
            ethereum.subgraph_url,
            Url::parse("https://ethereum-subgraph.example").unwrap()
        );
        assert_eq!(ethereum.backfill_start_block, 22_000_000);
    }

    #[test]
    fn into_config_adds_complete_hyperevm_chain_config() {
        let mut args = minimal_args();
        args.extend_from_slice(&[
            "--chain-hyperevm-rpc-url",
            "wss://hyperevm.example",
            "--chain-hyperevm-chain-id",
            "999",
            "--chain-hyperevm-subgraph-url",
            "https://hyperevm-subgraph.example",
            "--chain-hyperevm-backfill-start-block",
            "9000000",
        ]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        assert_eq!(config.chains.len(), 2);
        let hyperevm = &config.chains[1];
        assert_eq!(hyperevm.network, Network::HyperEvm);
        assert_eq!(hyperevm.chain_id, 999);
        assert_eq!(
            hyperevm.rpc_url,
            Url::parse("wss://hyperevm.example").unwrap()
        );
        assert_eq!(
            hyperevm.subgraph_url,
            Url::parse("https://hyperevm-subgraph.example").unwrap()
        );
        assert_eq!(hyperevm.backfill_start_block, 9_000_000);
    }

    /// HyperEVM's testnet id (998) one keystroke away from mainnet (999) is
    /// exactly the mislabeling the canonical-id check exists to refuse.
    #[test]
    fn hyperevm_group_with_testnet_chain_id_is_refused() {
        let result = Env::optional_chain_config(
            Network::HyperEvm,
            &ChainGroupEnv {
                rpc_url: Some(&Url::parse("wss://hyperevm.example").unwrap()),
                chain_id: Some(998),
                subgraph_url: Some(
                    &Url::parse("https://hyperevm-subgraph.example").unwrap(),
                ),
                backfill_start_block: Some(9_000_000),
            },
        );

        assert!(
            matches!(
                result,
                Err(ConfigError::ChainIdNotForNetwork {
                    network: Network::HyperEvm,
                    configured: 998,
                    expected: 999,
                })
            ),
            "a testnet chain id under the HyperEVM label must be refused, \
             got {result:?}"
        );
    }

    #[test]
    fn partial_ethereum_chain_config_is_rejected() {
        let mut args = minimal_args();
        args.extend_from_slice(&[
            "--chain-ethereum-rpc-url",
            "wss://ethereum.example",
        ]);

        let Err(error) = Env::try_parse_from(args) else {
            panic!("partial Ethereum chain configuration must fail");
        };

        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn test_empty_ip_ranges_default() {
        let args = minimal_args();
        let env = Env::try_parse_from(args).unwrap();

        assert_eq!(env.auth.alpaca_ip_ranges, IpWhitelist::AllowAll);
    }

    #[test]
    fn api_docs_exposed_only_outside_production() {
        assert!(Environment::Development.exposes_api_docs());
        assert!(Environment::Staging.exposes_api_docs());
        assert!(!Environment::Production.exposes_api_docs());
    }

    #[test]
    fn environment_defaults_to_production_and_parses_explicit_value() {
        // An unset ENVIRONMENT must fail closed to production (docs hidden).
        let default_env = Env::try_parse_from(minimal_args()).unwrap();
        assert_eq!(default_env.environment, Environment::Production);

        let mut args = minimal_args();
        args.extend_from_slice(&["--environment", "staging"]);
        let staging_env = Env::try_parse_from(args).unwrap();
        assert_eq!(staging_env.environment, Environment::Staging);
    }

    #[test]
    fn test_empty_string_ip_ranges() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", ""]);

        let env = Env::try_parse_from(args).unwrap();

        assert_eq!(env.auth.alpaca_ip_ranges, IpWhitelist::AllowAll);
    }

    #[test]
    fn test_single_ip_range() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", "192.168.1.0/24"]);

        let env = Env::try_parse_from(args).unwrap();
        let expected =
            IpWhitelist::single("192.168.1.0/24".parse::<IpNetwork>().unwrap());

        assert_eq!(env.auth.alpaca_ip_ranges, expected);
    }

    #[test]
    fn test_multiple_ip_ranges() {
        let mut args = minimal_args();
        args.extend_from_slice(&[
            "--alpaca-ip-ranges",
            "192.168.1.0/24,10.0.0.0/8,172.16.0.0/12",
        ]);

        let env = Env::try_parse_from(args).unwrap();
        let expected = IpWhitelist::from_ranges(&[
            "192.168.1.0/24".parse::<IpNetwork>().unwrap(),
            "10.0.0.0/8".parse::<IpNetwork>().unwrap(),
            "172.16.0.0/12".parse::<IpNetwork>().unwrap(),
        ]);

        assert_eq!(env.auth.alpaca_ip_ranges, expected);
    }

    #[test]
    fn test_invalid_ip_range_fails() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", "not-an-ip"]);

        assert!(Env::try_parse_from(args).is_err());
    }

    #[test]
    fn test_config_with_empty_ip_ranges() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", ""]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        assert_eq!(config.auth.alpaca_ip_ranges, IpWhitelist::AllowAll);
    }

    #[test]
    fn test_config_with_valid_ip_ranges() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", "10.0.0.0/8"]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        let expected =
            IpWhitelist::single("10.0.0.0/8".parse::<IpNetwork>().unwrap());

        assert_eq!(config.auth.alpaca_ip_ranges, expected);
    }

    #[test]
    fn test_short_api_key_rejected_at_parse_time() {
        let args = vec![
            "test-binary",
            "--rpc-url",
            "wss://localhost:8545",
            "--evm-private-key",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "--backfill-start-block",
            "12345678",
            "--issuer-api-key",
            "short-key", // Less than 32 characters
            "--alpaca-account-id",
            "test-alpaca-account-id",
            "--alpaca-api-key",
            "alpaca-test-key",
            "--alpaca-api-secret",
            "alpaca-test-secret",
            "--subgraph-url",
            "http://localhost:0/subgraph",
        ];

        let result = Env::try_parse_from(args);

        assert!(result.is_err());
    }

    #[test]
    fn test_wss_subgraph_url_rejected() {
        let args = vec![
            "test-binary",
            "--rpc-url",
            "wss://localhost:8545",
            "--evm-private-key",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "--backfill-start-block",
            "12345678",
            "--issuer-api-key",
            "test-key-that-is-at-least-32-chars-long",
            "--alpaca-account-id",
            "test-alpaca-account-id",
            "--alpaca-api-key",
            "alpaca-test-key",
            "--alpaca-api-secret",
            "alpaca-test-secret",
            "--subgraph-url",
            "wss://api.goldsky.com/api/public/project_xxx/subgraphs/test/1.0.0/gn",
        ];

        let env = Env::try_parse_from(args).unwrap();
        let result = env.into_config();

        assert!(matches!(
            result,
            Err(ConfigError::InvalidSubgraphScheme { variable, .. })
                if variable == "SUBGRAPH_URL"
        ));
    }

    #[test]
    fn configured_rpc_url_base_prefers_grouped_over_legacy() {
        let lookup = |name: &str| match name {
            "CHAIN_BASE_RPC_URL" => Some("wss://base-grouped.example".into()),
            "RPC_URL" => Some("wss://legacy.example".into()),
            _ => None,
        };

        let url = resolve_configured_rpc_url(Network::Base, lookup).unwrap();
        assert_eq!(url, Url::parse("wss://base-grouped.example").unwrap());
    }

    #[test]
    fn configured_rpc_url_base_falls_back_to_legacy() {
        let lookup = |name: &str| match name {
            "RPC_URL" => Some("wss://legacy.example".into()),
            _ => None,
        };

        let url = resolve_configured_rpc_url(Network::Base, lookup).unwrap();
        assert_eq!(url, Url::parse("wss://legacy.example").unwrap());
    }

    #[test]
    fn configured_rpc_url_ethereum_requires_grouped_var() {
        let lookup = |name: &str| match name {
            "RPC_URL" => Some("wss://legacy.example".into()),
            _ => None,
        };

        let err =
            resolve_configured_rpc_url(Network::Ethereum, lookup).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::NetworkRpcNotConfigured {
                network: Network::Ethereum,
                ..
            }
        ));
    }

    #[test]
    fn configured_rpc_url_rejects_invalid_url() {
        let lookup = |name: &str| match name {
            "RPC_URL" => Some("not a url".into()),
            _ => None,
        };

        let err =
            resolve_configured_rpc_url(Network::Base, lookup).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidConfiguredRpcUrl { network: Network::Base, .. }
        ));
    }

    #[tokio::test]
    async fn test_bot_wallet_derived_from_private_key() {
        let args = minimal_args();
        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        // Private key 0x...01 derives to this well-known address
        let expected = address!("7E5F4552091A69125d5DfCb7b8C2659029395Bdf");

        assert_eq!(config.signer.address().unwrap(), expected);
    }

    const ORCH_ADDR: &str = "0x1234567890abcdef1234567890abcdef12345678";

    fn orch_address() -> Address {
        ORCH_ADDR.parse().unwrap()
    }

    #[test]
    fn no_config_file_every_asset_vault_direct() {
        let cfg = VaultModeConfig::default();
        assert_eq!(cfg.default, VaultMode::VaultDirect);
        assert!(cfg.per_asset.is_empty());
    }

    // Pins the committed per-environment deploy configs (baked into the
    // systemd unit as CONFIG=<store path>, see nix/upgradeable-services.nix)
    // to the strict parser: they must always parse, and while the rollout is
    // dark they must resolve every asset to vault-direct.
    #[test]
    fn deploy_config_files_parse_and_stay_dark() {
        for (name, content) in [
            ("config.prod.toml", include_str!("../config.prod.toml")),
            ("config.staging.toml", include_str!("../config.staging.toml")),
        ] {
            let toml_file: TomlFile = toml::from_str(content)
                .unwrap_or_else(|error| panic!("{name} must parse: {error}"));

            let cfg = resolve_vault_modes(&toml_file)
                .unwrap_or_else(|error| panic!("{name} must resolve: {error}"));

            assert_eq!(cfg.default, VaultMode::VaultDirect, "{name} not dark");
            assert!(cfg.per_asset.is_empty(), "{name} has asset overrides");
        }
    }

    // Pins config.example.toml to the strict parser so the committed example
    // can never drift into something the bot would reject at startup.
    #[test]
    fn example_config_file_parses_and_resolves() {
        let toml_file: TomlFile =
            toml::from_str(include_str!("../config.example.toml")).unwrap();

        let cfg = resolve_vault_modes(&toml_file).unwrap();

        assert_eq!(
            cfg.per_asset.get("RKLB").copied(),
            Some(VaultMode::Orchestrator { address: orch_address() })
        );
        assert_eq!(cfg.default, VaultMode::VaultDirect);
    }

    #[test]
    fn per_asset_override_to_orchestrator() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: Some(ORCH_ADDR.to_string()),
                default_vault_mode: None,
            }),
            assets: HashMap::from([(
                "AAPL".to_string(),
                AssetSection { vault_mode: VaultModeStr::Orchestrator },
            )]),
        };

        let cfg = resolve_vault_modes(&toml).unwrap();

        assert_eq!(
            cfg.per_asset.get("AAPL").copied(),
            Some(VaultMode::Orchestrator { address: orch_address() })
        );
        assert_eq!(cfg.default, VaultMode::VaultDirect);
    }

    #[test]
    fn per_asset_override_to_vault_direct_ignores_default() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: Some(ORCH_ADDR.to_string()),
                default_vault_mode: Some(VaultModeStr::Orchestrator),
            }),
            assets: HashMap::from([(
                "TSLA".to_string(),
                AssetSection { vault_mode: VaultModeStr::VaultDirect },
            )]),
        };

        let cfg = resolve_vault_modes(&toml).unwrap();

        assert_eq!(
            cfg.per_asset.get("TSLA").copied(),
            Some(VaultMode::VaultDirect)
        );
        assert_eq!(
            cfg.default,
            VaultMode::Orchestrator { address: orch_address() }
        );
    }

    #[test]
    fn no_per_asset_override_uses_default_vault_mode() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: Some(ORCH_ADDR.to_string()),
                default_vault_mode: Some(VaultModeStr::Orchestrator),
            }),
            assets: HashMap::new(),
        };

        let cfg = resolve_vault_modes(&toml).unwrap();

        assert_eq!(
            cfg.default,
            VaultMode::Orchestrator { address: orch_address() }
        );
    }

    #[test]
    fn no_orchestrator_section_defaults_to_vault_direct() {
        let toml = TomlFile { orchestrator: None, assets: HashMap::new() };

        let cfg = resolve_vault_modes(&toml).unwrap();

        assert_eq!(cfg.default, VaultMode::VaultDirect);
        assert!(cfg.per_asset.is_empty());
    }

    #[test]
    fn orchestrator_asset_without_address_is_startup_error() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: None,
                default_vault_mode: None,
            }),
            assets: HashMap::from([(
                "AAPL".to_string(),
                AssetSection { vault_mode: VaultModeStr::Orchestrator },
            )]),
        };

        assert!(matches!(
            resolve_vault_modes(&toml),
            Err(ConfigError::MissingOrchestratorAddress)
        ));
    }

    #[test]
    fn default_orchestrator_without_address_is_startup_error() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: None,
                default_vault_mode: Some(VaultModeStr::Orchestrator),
            }),
            assets: HashMap::new(),
        };

        assert!(matches!(
            resolve_vault_modes(&toml),
            Err(ConfigError::MissingOrchestratorAddress)
        ));
    }

    #[test]
    fn invalid_orchestrator_address_is_startup_error() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: Some("not-an-address".to_string()),
                default_vault_mode: None,
            }),
            assets: HashMap::new(),
        };

        assert!(matches!(
            resolve_vault_modes(&toml),
            Err(ConfigError::InvalidOrchestratorAddress(_))
        ));
    }

    #[test]
    fn vault_mode_serde_wire_format_is_stable() {
        assert_eq!(
            serde_json::to_value(VaultMode::VaultDirect).unwrap(),
            serde_json::json!("VaultDirect")
        );
        assert_eq!(
            serde_json::to_value(VaultMode::Orchestrator {
                address: orch_address()
            })
            .unwrap(),
            serde_json::json!({"Orchestrator": {"address": ORCH_ADDR}})
        );
    }

    #[test]
    fn vault_mode_serde_round_trips() {
        for mode in [
            VaultMode::VaultDirect,
            VaultMode::Orchestrator { address: orch_address() },
        ] {
            let json = serde_json::to_value(mode).unwrap();
            assert_eq!(
                serde_json::from_value::<VaultMode>(json).unwrap(),
                mode
            );
        }
    }

    #[test]
    fn mode_for_prefers_per_asset_override_and_falls_back_to_default() {
        let cfg = VaultModeConfig::new(
            HashMap::from([(
                "AAPL".to_string(),
                VaultMode::Orchestrator { address: orch_address() },
            )]),
            VaultMode::VaultDirect,
        );

        assert_eq!(
            cfg.mode_for(&UnderlyingSymbol::new("AAPL").unwrap()),
            VaultMode::Orchestrator { address: orch_address() }
        );
        assert_eq!(
            cfg.mode_for(&UnderlyingSymbol::new("TSLA").unwrap()),
            VaultMode::VaultDirect
        );
    }

    #[test]
    fn zero_orchestrator_address_is_startup_error() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: Some(Address::ZERO.to_string()),
                default_vault_mode: None,
            }),
            assets: HashMap::from([(
                "AAPL".to_string(),
                AssetSection { vault_mode: VaultModeStr::Orchestrator },
            )]),
        };

        assert!(matches!(
            resolve_vault_modes(&toml),
            Err(ConfigError::InvalidOrchestratorAddress(message))
                if message == Address::ZERO.to_string()
        ));
    }

    // A lowercase key must configure the same asset the uppercase symbol
    // names — before normalization, `[assets.rklb]` parsed and resolved
    // cleanly but never matched the stored uppercase symbol, silently
    // leaving the asset vault-direct.
    #[test]
    fn lowercase_asset_key_normalizes_to_the_stored_symbol() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: Some(ORCH_ADDR.to_string()),
                default_vault_mode: None,
            }),
            assets: HashMap::from([(
                "rklb".to_string(),
                AssetSection { vault_mode: VaultModeStr::Orchestrator },
            )]),
        };

        let cfg = resolve_vault_modes(&toml).unwrap();

        assert_eq!(
            cfg.per_asset.get("RKLB").copied(),
            Some(VaultMode::Orchestrator { address: orch_address() })
        );
        assert!(!cfg.per_asset.contains_key("rklb"));
    }

    #[test]
    fn blank_asset_key_is_startup_error() {
        let toml = TomlFile {
            orchestrator: None,
            assets: HashMap::from([(
                "  ".to_string(),
                AssetSection { vault_mode: VaultModeStr::VaultDirect },
            )]),
        };

        assert!(matches!(
            resolve_vault_modes(&toml),
            Err(ConfigError::InvalidAssetSymbol { symbol, .. })
                if symbol == "  "
        ));
    }

    #[test]
    fn asset_keys_colliding_after_normalization_are_a_startup_error() {
        let toml = TomlFile {
            orchestrator: None,
            assets: HashMap::from([
                (
                    "rklb".to_string(),
                    AssetSection { vault_mode: VaultModeStr::VaultDirect },
                ),
                (
                    "RKLB".to_string(),
                    AssetSection { vault_mode: VaultModeStr::VaultDirect },
                ),
            ]),
        };

        assert!(matches!(
            resolve_vault_modes(&toml),
            Err(ConfigError::DuplicateAssetSymbol { symbol })
                if symbol == "RKLB"
        ));
    }

    #[test]
    fn unknown_vault_mode_string_in_toml_is_parse_error() {
        let bad_toml = r#"
            [orchestrator]
            address = "0x1234567890abcdef1234567890abcdef12345678"

            [assets.AAPL]
            vault_mode = "not_a_valid_mode"
        "#;

        let result = toml::from_str::<TomlFile>(bad_toml);
        assert!(result.is_err(), "expected parse error for unknown vault_mode");
    }

    #[test]
    fn unknown_toml_key_is_parse_error() {
        let bad_toml = r#"
            [orchestrator]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            unexpected_key = "oops"
        "#;

        let result = toml::from_str::<TomlFile>(bad_toml);
        assert!(result.is_err(), "expected parse error for unknown key");
    }

    #[test]
    fn vault_mode_for_uses_per_asset_override_then_default() {
        let toml = TomlFile {
            orchestrator: Some(OrchestratorSection {
                address: Some(ORCH_ADDR.to_string()),
                default_vault_mode: Some(VaultModeStr::Orchestrator),
            }),
            assets: HashMap::from([(
                "TSLA".to_string(),
                AssetSection { vault_mode: VaultModeStr::VaultDirect },
            )]),
        };
        let args = minimal_args();
        let env = Env::try_parse_from(args).unwrap();
        let mut config = env.into_config().unwrap();
        config.vault_mode_config = resolve_vault_modes(&toml).unwrap();

        // Explicit VaultDirect override wins over the orchestrator default
        assert_eq!(
            config.vault_mode_for(&UnderlyingSymbol::new("TSLA").unwrap()),
            VaultMode::VaultDirect
        );

        // Asset not in per_asset falls back to default (orchestrator)
        assert_eq!(
            config.vault_mode_for(&UnderlyingSymbol::new("AAPL").unwrap()),
            VaultMode::Orchestrator { address: orch_address() }
        );
    }
}
