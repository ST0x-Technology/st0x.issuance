use alloy::providers::Provider;
use clap::{Args, Parser};
use std::time::Duration;
use tracing::Level;
use url::Url;

use crate::alpaca::service::AlpacaConfig;
use crate::auth::AuthConfig;
use crate::chain::{
    ChainConfig, ChainRegistry, ChainRegistryError, build_chain_registry,
};
use crate::telemetry::{HyperDxApiKey, HyperDxConfig};
use crate::tokenized_asset::Network;
use crate::wallet::{SignerConfig, SignerConfigError, SignerEnv};

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
    /// All chain runtimes the registry is built from, Base first. The legacy
    /// flat env vars populate the single Base entry today; the multichain
    /// `CHAIN_<NETWORK>_*` config format extends this list (see the SPEC.md
    /// Multi-chain section, which also specifies the legacy-var sunset).
    pub chains: Vec<ChainConfig>,
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
        help = "WebSocket RPC endpoint URL (wss://...)"
    )]
    rpc_url: Url,

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
        help = "Goldsky subgraph URL for querying OA schema hashes"
    )]
    subgraph_url: Url,
}

impl Env {
    fn into_config(self) -> Result<Config, ConfigError> {
        let log_level_tracing = (&self.log_level).into();
        let hyperdx = self.hyperdx.into_config(log_level_tracing);
        let signer = self.signer.into_config()?;

        match self.subgraph_url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(ConfigError::InvalidSubgraphScheme(
                    scheme.to_string(),
                ));
            }
        }

        let chains = vec![ChainConfig {
            network: Network::Base,
            chain_id: self.chain_id,
            rpc_url: self.rpc_url.clone(),
            subgraph_url: self.subgraph_url.clone(),
            backfill_start_block: self.backfill_start_block,
        }];

        Ok(Config {
            database_url: self.database_url,
            database_max_connections: self.database_max_connections,
            rpc_url: self.rpc_url,
            chain_id: self.chain_id,
            signer,
            backfill_start_block: self.backfill_start_block,
            receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
            auth: self.auth,
            log_level: self.log_level,
            environment: self.environment,
            hyperdx,
            alpaca: self.alpaca,
            subgraph_url: self.subgraph_url,
            chains,
        })
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
    #[error("SUBGRAPH_URL must use http or https scheme, got: {0}")]
    InvalidSubgraphScheme(String),
    #[error("chain registry initialization failed: {0}")]
    ChainRegistry(#[source] Box<ChainRegistryError>),
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

        assert!(matches!(result, Err(ConfigError::InvalidSubgraphScheme(_))));
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
}
