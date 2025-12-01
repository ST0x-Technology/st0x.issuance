use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::RpcError;
use clap::{Args, Parser};
use std::sync::Arc;
use tracing::{Level, warn};
use url::Url;

use crate::alpaca::service::AlpacaConfig;
use crate::auth::IpWhitelist;
use crate::telemetry::HyperDxConfig;
use crate::vault::{VaultService, service::RealBlockchainService};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub database_max_connections: u32,
    pub rpc_url: Url,
    pub private_key: B256,
    pub vault: Address,
    pub bot: Address,
    pub issuer_api_key: String,
    pub alpaca_ip_ranges: IpWhitelist,
    pub log_level: LogLevel,
    pub hyperdx: Option<HyperDxConfig>,
    pub alpaca: AlpacaConfig,
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

    pub(crate) async fn create_blockchain_service(
        &self,
    ) -> Result<Arc<dyn VaultService>, ConfigError> {
        let signer = PrivateKeySigner::from_bytes(&self.private_key)?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(self.rpc_url.as_str())
            .await?;

        Ok(Arc::new(RealBlockchainService::new(provider, self.vault)))
    }
}

#[derive(Debug, Parser, Clone)]
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
        env = "PRIVATE_KEY",
        help = "Private key for signing blockchain transactions"
    )]
    private_key: B256,

    #[arg(
        long,
        env = "VAULT_ADDRESS",
        help = "OffchainAssetReceiptVault contract address"
    )]
    vault: Address,

    #[arg(
        long,
        env = "BOT_WALLET",
        help = "Bot's wallet address that controls minting and redemption"
    )]
    bot: Address,

    #[arg(
        long,
        env = "ISSUER_API_KEY",
        help = "API key for authenticating inbound requests from Alpaca"
    )]
    issuer_api_key: String,

    #[arg(
        long,
        env = "ALPACA_IP_RANGES",
        default_value = "",
        help = "Comma-separated list of IP ranges (CIDR notation) allowed to call issuer endpoints. Leave empty to disable IP filtering (not recommended for production)."
    )]
    alpaca_ip_ranges: IpWhitelist,

    #[clap(long, env, default_value = "debug")]
    log_level: LogLevel,

    #[clap(flatten)]
    hyperdx: HyperDxEnv,

    #[clap(flatten)]
    pub(crate) alpaca: AlpacaConfig,
}

impl Env {
    fn into_config(self) -> Result<Config, ConfigError> {
        if self.issuer_api_key.len() < 32 {
            return Err(ConfigError::InvalidIssuerApiKey(format!(
                "API key must be at least 32 characters, got {}",
                self.issuer_api_key.len()
            )));
        }

        if matches!(self.alpaca_ip_ranges, IpWhitelist::AllowAll) {
            warn!(
                "ALPACA_IP_RANGES not set - IP whitelisting is DISABLED. \
                 This is NOT RECOMMENDED for production. Any IP can access issuer endpoints \
                 if they have the API key."
            );
        }

        let log_level_tracing = (&self.log_level).into();
        let hyperdx = self.hyperdx.into_config(log_level_tracing);

        Ok(Config {
            database_url: self.database_url,
            database_max_connections: self.database_max_connections,
            rpc_url: self.rpc_url,
            private_key: self.private_key,
            vault: self.vault,
            bot: self.bot,
            issuer_api_key: self.issuer_api_key,
            alpaca_ip_ranges: self.alpaca_ip_ranges,
            log_level: self.log_level,
            hyperdx,
            alpaca: self.alpaca,
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
    #[error("Invalid private key")]
    InvalidPrivateKey(#[from] alloy::signers::local::LocalSignerError),
    #[error("Invalid private key format")]
    InvalidPrivateKeyFormat(#[from] alloy::signers::k256::ecdsa::Error),
    #[error("Failed to connect to RPC endpoint")]
    ConnectionFailed(#[from] RpcError<alloy::transports::TransportErrorKind>),
    #[error("Invalid issuer API key: {0}")]
    InvalidIssuerApiKey(String),
    #[error("Failed to parse configuration: {0}")]
    ParseError(#[from] clap::Error),
}

pub fn setup_tracing(log_level: &LogLevel) {
    let level: Level = log_level.into();
    let default_filter = format!("st0x_issuance={level}");

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .try_init();
}

#[cfg(test)]
mod tests {
    use ipnetwork::IpNetwork;

    use super::*;

    fn minimal_args() -> Vec<&'static str> {
        vec![
            "test-binary",
            "--rpc-url",
            "wss://localhost:8545",
            "--private-key",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "--vault",
            "0x1111111111111111111111111111111111111111",
            "--bot",
            "0x2222222222222222222222222222222222222222",
            "--issuer-api-key",
            "test-key-that-is-at-least-32-chars-long",
            "--alpaca-account-id",
            "alpaca-account-id",
            "--alpaca-api-key",
            "alpaca-test-key",
            "--alpaca-api-secret",
            "alpaca-test-secret",
        ]
    }

    #[test]
    fn test_empty_ip_ranges_default() {
        let args = minimal_args();
        let env = Env::try_parse_from(args).unwrap();
        assert_eq!(env.alpaca_ip_ranges, IpWhitelist::AllowAll);
    }

    #[test]
    fn test_empty_string_ip_ranges() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", ""]);

        let env = Env::try_parse_from(args).unwrap();
        assert_eq!(env.alpaca_ip_ranges, IpWhitelist::AllowAll);
    }

    #[test]
    fn test_single_ip_range() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", "192.168.1.0/24"]);

        let env = Env::try_parse_from(args).unwrap();
        let expected =
            IpWhitelist::single("192.168.1.0/24".parse::<IpNetwork>().unwrap());
        assert_eq!(env.alpaca_ip_ranges, expected);
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
        assert_eq!(env.alpaca_ip_ranges, expected);
    }

    #[test]
    fn test_invalid_ip_range_fails() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", "not-an-ip"]);

        assert!(Env::try_parse_from(args).is_err());
    }

    #[test]
    fn test_config_warns_about_empty_ip_ranges() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", ""]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        assert_eq!(config.alpaca_ip_ranges, IpWhitelist::AllowAll);
    }

    #[test]
    fn test_config_with_valid_ip_ranges() {
        let mut args = minimal_args();
        args.extend_from_slice(&["--alpaca-ip-ranges", "10.0.0.0/8"]);

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();

        let expected =
            IpWhitelist::single("10.0.0.0/8".parse::<IpNetwork>().unwrap());
        assert_eq!(config.alpaca_ip_ranges, expected);
    }
}
