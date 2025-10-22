use std::sync::Arc;

use alloy::network::EthereumWallet;
use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use clap::Parser;

use crate::alpaca::AlpacaConfig;
use crate::blockchain::{BlockchainService, service::RealBlockchainService};

#[derive(Debug, Parser)]
#[command(name = "st0x-issuance")]
#[command(about = "Issuance bot for tokenizing equities via Alpaca ITN")]
pub(crate) struct Config {
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "sqlite:data.db",
        help = "SQLite database URL"
    )]
    pub(crate) database_url: String,

    #[arg(
        long,
        env = "DATABASE_MAX_CONNECTIONS",
        default_value = "5",
        help = "Maximum number of database connections in the pool"
    )]
    pub(crate) database_max_connections: u32,

    #[arg(long, env = "RPC_URL", help = "Blockchain RPC endpoint URL")]
    rpc_url: Option<String>,

    #[arg(
        long,
        env = "PRIVATE_KEY",
        help = "Private key for signing blockchain transactions"
    )]
    private_key: Option<String>,

    #[arg(
        long,
        env = "VAULT_ADDRESS",
        help = "OffchainAssetReceiptVault contract address"
    )]
    vault_address: Option<String>,

    #[arg(long, env = "CHAIN_ID", help = "Blockchain network chain ID")]
    chain_id: Option<u64>,

    #[command(flatten)]
    pub(crate) alpaca: AlpacaConfig,
}

impl Config {
    pub(crate) fn create_blockchain_service(
        &self,
    ) -> Result<Arc<dyn BlockchainService>, ConfigError> {
        let rpc_url =
            self.rpc_url.as_ref().ok_or(ConfigError::MissingRpcUrl)?;

        let private_key =
            self.private_key.as_ref().ok_or(ConfigError::MissingPrivateKey)?;

        let vault_address_str = self
            .vault_address
            .as_ref()
            .ok_or(ConfigError::MissingVaultAddress)?;

        let _chain_id = self.chain_id.ok_or(ConfigError::MissingChainId)?;

        let signer = private_key
            .parse::<PrivateKeySigner>()
            .map_err(|e| ConfigError::InvalidPrivateKey(e.to_string()))?;
        let wallet = EthereumWallet::from(signer);

        let vault_address = vault_address_str
            .parse::<Address>()
            .map_err(|e| ConfigError::InvalidVaultAddress(e.to_string()))?;

        let provider = ProviderBuilder::new().wallet(wallet).connect_http(
            rpc_url
                .parse()
                .map_err(|e| ConfigError::InvalidRpcUrl(format!("{e}")))?,
        );

        Ok(Arc::new(RealBlockchainService::new(provider, vault_address)))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigError {
    #[error("RPC_URL is required")]
    MissingRpcUrl,

    #[error("PRIVATE_KEY is required")]
    MissingPrivateKey,

    #[error("VAULT_ADDRESS is required")]
    MissingVaultAddress,

    #[error("CHAIN_ID is required")]
    MissingChainId,

    #[error("Invalid private key: {0}")]
    InvalidPrivateKey(String),

    #[error("Invalid vault address: {0}")]
    InvalidVaultAddress(String),

    #[error("Invalid RPC URL: {0}")]
    InvalidRpcUrl(String),
}
