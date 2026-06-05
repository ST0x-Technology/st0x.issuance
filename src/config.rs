use alloy::providers::{Provider, ProviderBuilder};
use alloy::transports::{RpcError, TransportErrorKind};
use clap::{Args, Parser};
use std::sync::Arc;
use std::time::Duration;
use tracing::Level;
use url::Url;

use crate::alpaca::service::AlpacaConfig;
use crate::auth::AuthConfig;
use crate::fireblocks::{
    FireblocksVaultService, SignerConfig, SignerConfigError, SignerEnv,
    resolve_local_signer,
};
use crate::telemetry::HyperDxConfig;
use crate::vault::rain_meta::OaSchemaCache;
use crate::vault::{VaultService, service::RealBlockchainService};

pub(crate) struct BlockchainSetup<HttpP> {
    pub(crate) vault_service: Arc<dyn VaultService>,
    /// HTTP provider — used for all non-subscription RPC calls (backfill,
    /// reconciliation, vault service balance checks, etc.). Receipt monitors
    /// and redemption detectors create their own WSS connections.
    pub(crate) http_provider: HttpP,
}

/// Default chain ID (Base mainnet)
pub const DEFAULT_CHAIN_ID: u64 = 8453;

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
    pub hyperdx: Option<HyperDxConfig>,
    pub alpaca: AlpacaConfig,
    pub subgraph_url: Url,
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

    /// Creates the HTTP provider and vault service.
    ///
    /// Initializes an HTTP provider for non-subscription RPC calls (backfill,
    /// reconciliation, vault service) and builds the appropriate VaultService
    /// (local signer or Fireblocks). Receipt monitors and redemption detectors
    /// create their own WSS connections independently.
    pub(crate) async fn create_blockchain_setup(
        &self,
    ) -> Result<BlockchainSetup<impl Provider + Clone + use<>>, ConfigError>
    {
        let http_url = wss_to_http(&self.rpc_url)?;
        let http_provider = ProviderBuilder::new().connect_http(http_url);

        let rpc_chain_id = http_provider.get_chain_id().await?;
        if rpc_chain_id != self.chain_id {
            return Err(ConfigError::ChainIdMismatch {
                configured: self.chain_id,
                from_rpc: rpc_chain_id,
            });
        }

        let oa_schema_cache =
            Arc::new(OaSchemaCache::new(self.subgraph_url.clone())?);

        let vault_service: Arc<dyn VaultService> = match &self.signer {
            SignerConfig::Local(key) => {
                let resolved = resolve_local_signer(key, self.chain_id)
                    .map_err(|err| ConfigError::SignerResolve(Box::new(err)))?;

                let signing_provider = ProviderBuilder::new()
                    .wallet(resolved.wallet)
                    .connect_http(wss_to_http(&self.rpc_url)?);

                Arc::new(RealBlockchainService::new(
                    signing_provider,
                    oa_schema_cache,
                ))
            }

            SignerConfig::Fireblocks(env) => {
                let service = FireblocksVaultService::new(
                    env,
                    http_provider.clone(),
                    self.chain_id,
                    oa_schema_cache,
                )
                .map_err(|err| ConfigError::FireblocksVault(Box::new(err)))?;

                Arc::new(service)
            }
        };

        Ok(BlockchainSetup { vault_service, http_provider })
    }
}

#[derive(Parser, Clone)]
#[command(name = "st0x-issuance")]
#[command(about = "Issuance bot for tokenizing equities via Alpaca ITN")]
struct Env {
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "sqlite:data.db",
        help = "SQLite database URL"
    )]
    database_url: String,

    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value = "5",
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
            hyperdx,
            alpaca: self.alpaca,
            subgraph_url: self.subgraph_url,
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
            api_key,
            service_name: self.hyperdx_service_name,
            log_level,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Signer configuration error")]
    SignerConfig(#[from] SignerConfigError),
    #[error("Failed to resolve signer: {0}")]
    SignerResolve(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("RPC error")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Failed to parse configuration: {0}")]
    ParseError(#[from] clap::Error),
    #[error("Fireblocks vault service initialization failed: {0}")]
    FireblocksVault(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("SUBGRAPH_URL must use http or https scheme, got: {0}")]
    InvalidSubgraphScheme(String),
    #[error(
        "Chain ID mismatch: configured {configured}, RPC returned {from_rpc}"
    )]
    ChainIdMismatch { configured: u64, from_rpc: u64 },
    #[error("Cannot derive HTTP URL from RPC URL: {0}")]
    InvalidRpcScheme(String),
}

/// Derives an HTTP URL from a WebSocket URL by replacing the scheme.
///
/// `wss://` → `https://`, `ws://` → `http://`. Leaves HTTP URLs unchanged.
fn wss_to_http(url: &Url) -> Result<Url, ConfigError> {
    let new_scheme = match url.scheme() {
        "wss" => "https",
        "ws" => "http",
        "http" | "https" => return Ok(url.clone()),
        other => return Err(ConfigError::InvalidRpcScheme(other.to_string())),
    };

    let mut http_url = url.clone();
    http_url.set_scheme(new_scheme).map_err(|()| {
        ConfigError::InvalidRpcScheme(url.scheme().to_string())
    })?;

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
    "fireblocks",
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
    fn test_empty_ip_ranges_default() {
        let args = minimal_args();
        let env = Env::try_parse_from(args).unwrap();

        assert_eq!(env.auth.alpaca_ip_ranges, IpWhitelist::AllowAll);
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

        assert_eq!(config.signer.address().await.unwrap(), expected);
    }
}
