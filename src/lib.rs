use alloy::network::EthereumWallet;
use alloy::primitives::{Address, address};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use clap::Parser;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use rocket::routes;
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::sync::Arc;

use account::{Account, AccountView};
use alpaca::service::AlpacaConfig;
use blockchain::{BlockchainService, service::RealBlockchainService};
use mint::{CallbackManager, Mint, MintView, mint_manager::MintManager};
use tokenized_asset::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    TokenizedAssetView, UnderlyingSymbol,
};

pub mod account;
pub mod alpaca;
pub mod blockchain;
pub mod mint;
pub mod test_utils;
pub mod tokenized_asset;

mod bindings;

pub(crate) type AccountCqrs = SqliteCqrs<account::Account>;

#[cfg(test)]
pub(crate) type TokenizedAssetCqrs =
    SqliteCqrs<tokenized_asset::TokenizedAsset>;

pub(crate) type MintCqrs = Arc<SqliteCqrs<mint::Mint>>;
pub(crate) type MintEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, mint::Mint>>;

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

type TokenizedAssetCqrsInternal = SqliteCqrs<TokenizedAsset>;

/// Initializes and configures the Rocket web server with all necessary state.
///
/// Sets up database connections, CQRS infrastructure, service managers, and mounts
/// all HTTP endpoints. This is the main entry point for starting the application.
///
/// # Errors
///
/// Returns an error if:
/// - Database connection or migration fails
/// - Blockchain service configuration is invalid
/// - Alpaca service configuration is invalid
pub async fn initialize_rocket()
-> Result<rocket::Rocket<rocket::Build>, Box<dyn std::error::Error>> {
    let config = Config::parse();

    let pool = create_pool(&config).await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let account_view_repo =
        Arc::new(SqliteViewRepository::<AccountView, Account>::new(
            pool.clone(),
            "account_view".to_string(),
        ));

    let account_query = GenericQuery::new(account_view_repo);

    let account_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

    let tokenized_asset_view_repo = Arc::new(SqliteViewRepository::<
        TokenizedAssetView,
        TokenizedAsset,
    >::new(
        pool.clone(),
        "tokenized_asset_view".to_string(),
    ));

    let tokenized_asset_query = GenericQuery::new(tokenized_asset_view_repo);

    let tokenized_asset_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(tokenized_asset_query)], ());

    let mint_view_repo = Arc::new(SqliteViewRepository::<MintView, Mint>::new(
        pool.clone(),
        "mint_view".to_string(),
    ));

    let mint_query = GenericQuery::new(mint_view_repo);

    let mint_cqrs_raw =
        sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ());
    let mint_cqrs = Arc::new(mint_cqrs_raw);

    let mint_event_repo = SqliteEventRepository::new(pool.clone());
    let mint_event_store: MintEventStore =
        Arc::new(PersistedEventStore::new_event_store(mint_event_repo));

    seed_initial_assets(&tokenized_asset_cqrs).await?;

    let blockchain_service = config.create_blockchain_service()?;

    let mint_manager =
        Arc::new(MintManager::new(blockchain_service, mint_cqrs.clone()));

    let alpaca_service = config.alpaca.service()?;
    let callback_manager =
        Arc::new(CallbackManager::new(alpaca_service, mint_cqrs.clone()));

    Ok(rocket::build()
        .manage(account_cqrs)
        .manage(tokenized_asset_cqrs)
        .manage(mint_cqrs)
        .manage(mint_event_store)
        .manage(mint_manager)
        .manage(callback_manager)
        .manage(pool)
        .mount(
            "/",
            routes![
                account::connect_account,
                tokenized_asset::list_tokenized_assets,
                mint::initiate_mint,
                mint::confirm_journal
            ],
        ))
}

async fn seed_initial_assets(
    cqrs: &TokenizedAssetCqrsInternal,
) -> Result<(), Box<dyn std::error::Error>> {
    // dummy values
    let assets = vec![
        (
            "AAPL",
            "tAAPL",
            "base",
            address!("0x1234567890abcdef1234567890abcdef12345678"),
        ),
        (
            "TSLA",
            "tTSLA",
            "base",
            address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
        ),
        (
            "NVDA",
            "tNVDA",
            "base",
            address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc"),
        ),
    ];

    for (underlying, token, network, vault_address) in assets {
        let command = TokenizedAssetCommand::Add {
            underlying: UnderlyingSymbol::new(underlying),
            token: TokenSymbol::new(token),
            network: Network::new(network),
            vault_address,
        };

        match cqrs.execute(underlying, command).await {
            Ok(()) | Err(cqrs_es::AggregateError::AggregateConflict) => {}
            Err(e) => {
                return Err(Box::new(e));
            }
        }
    }

    Ok(())
}

async fn create_pool(config: &Config) -> Result<Pool<Sqlite>, sqlx::Error> {
    SqlitePoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await
}
