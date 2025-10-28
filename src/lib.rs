use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, address};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::RpcError;
use clap::Parser;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use rocket::routes;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::sync::Arc;
use tracing::info;
use url::Url;

use account::{Account, AccountView};
use alpaca::service::AlpacaConfig;
use mint::{CallbackManager, Mint, MintView, mint_manager::MintManager};
use receipt_inventory::ReceiptInventoryView;
use redemption::{
    Redemption, RedemptionView,
    detector::{RedemptionDetector, RedemptionDetectorConfig},
    journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
};
use tokenized_asset::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    TokenizedAssetView, UnderlyingSymbol,
};
use vault::{VaultService, service::RealBlockchainService};

pub mod account;
pub mod mint;
pub mod redemption;
pub mod test_utils;
pub mod tokenized_asset;

pub(crate) mod alpaca;
pub(crate) mod receipt_inventory;
pub(crate) mod vault;

mod bindings;

pub(crate) type AccountCqrs = SqliteCqrs<account::Account>;

#[cfg(test)]
pub(crate) type TokenizedAssetCqrs =
    SqliteCqrs<tokenized_asset::TokenizedAsset>;

pub(crate) type MintCqrs = Arc<SqliteCqrs<mint::Mint>>;
pub(crate) type MintEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, mint::Mint>>;

type RedemptionCqrs = Arc<SqliteCqrs<Redemption>>;
type RedemptionEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, Redemption>>;

struct AggregateCqrsSetup {
    mint_cqrs: MintCqrs,
    mint_event_store: MintEventStore,
    redemption_cqrs: RedemptionCqrs,
    redemption_event_store: RedemptionEventStore,
}

struct RedemptionManagers {
    redeem_call_manager: Arc<
        RedeemCallManager<
            PersistedEventStore<SqliteEventRepository, Redemption>,
        >,
    >,
    journal_manager: Arc<
        JournalManager<PersistedEventStore<SqliteEventRepository, Redemption>>,
    >,
}

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

    #[arg(
        long,
        env = "RPC_URL",
        help = "WebSocket RPC endpoint URL (wss://...)"
    )]
    rpc_url: Option<Url>,

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
    vault_address: Option<Address>,

    #[arg(
        long,
        env = "REDEMPTION_WALLET",
        help = "Address where APs send tokens to initiate redemption"
    )]
    redemption_wallet: Option<Address>,

    #[command(flatten)]
    pub(crate) alpaca: AlpacaConfig,
}

impl Config {
    pub(crate) async fn create_blockchain_service(
        &self,
    ) -> Result<Arc<dyn VaultService>, ConfigError> {
        let rpc_url =
            self.rpc_url.as_ref().ok_or(ConfigError::MissingRpcUrl)?;

        let private_key =
            self.private_key.as_ref().ok_or(ConfigError::MissingPrivateKey)?;

        let vault_address =
            self.vault_address.ok_or(ConfigError::MissingVaultAddress)?;

        let signer = private_key.parse::<PrivateKeySigner>()?;
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(rpc_url.as_str())
            .await?;

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
    #[error("Invalid private key")]
    InvalidPrivateKey(#[from] alloy::signers::local::LocalSignerError),
    #[error("Failed to connect to RPC endpoint")]
    ConnectionFailed(#[from] RpcError<alloy::transports::TransportErrorKind>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Quantity(pub(crate) Decimal);

impl Quantity {
    pub(crate) const fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub(crate) fn to_u256_with_18_decimals(
        &self,
    ) -> Result<U256, QuantityConversionError> {
        let Self(value) = self;

        if value.is_sign_negative() {
            return Err(QuantityConversionError::NegativeValue {
                value: *value,
            });
        }

        let multiplier = 10_u128.pow(18);
        let scaled = value
            .checked_mul(Decimal::from(multiplier))
            .ok_or(QuantityConversionError::Overflow)?;

        if scaled.fract() != Decimal::ZERO {
            return Err(QuantityConversionError::FractionalValue {
                value: scaled,
            });
        }

        let integer_part = scaled
            .to_u128()
            .ok_or(QuantityConversionError::U128OutOfRange { value: scaled })?;

        Ok(U256::from(integer_part))
    }

    pub(crate) fn from_u256_with_18_decimals(
        value: U256,
    ) -> Result<Self, QuantityConversionError> {
        let decimal: Decimal = value.to_string().parse()?;

        let divisor = Decimal::from(10_u128.pow(18));
        let quantity = decimal
            .checked_div(divisor)
            .ok_or(QuantityConversionError::Overflow)?;

        Ok(Self(quantity))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum QuantityConversionError {
    #[error("Arithmetic overflow during conversion")]
    Overflow,
    #[error("Fractional value after scaling: {value}")]
    FractionalValue { value: Decimal },
    #[error("Negative value: {value}")]
    NegativeValue { value: Decimal },
    #[error("Value out of range for u128: {value}")]
    U128OutOfRange { value: Decimal },
    #[error("Failed to parse decimal: {0}")]
    ParseFailed(#[from] rust_decimal::Error),
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

    let (account_cqrs, tokenized_asset_cqrs) = setup_basic_cqrs(&pool).await?;

    let AggregateCqrsSetup {
        mint_cqrs,
        mint_event_store,
        redemption_cqrs,
        redemption_event_store,
    } = setup_aggregate_cqrs(&pool);

    let (mint_manager, callback_manager) =
        setup_mint_managers(&config, &mint_cqrs).await?;

    let RedemptionManagers { redeem_call_manager, journal_manager } =
        setup_redemption_managers(
            &config,
            &redemption_cqrs,
            &redemption_event_store,
        )?;

    spawn_redemption_detector(
        &config,
        redemption_cqrs.clone(),
        redemption_event_store,
        pool.clone(),
        redeem_call_manager,
        journal_manager,
    );

    Ok(rocket::build()
        .manage(account_cqrs)
        .manage(tokenized_asset_cqrs)
        .manage(mint_cqrs)
        .manage(mint_event_store)
        .manage(mint_manager)
        .manage(callback_manager)
        .manage(redemption_cqrs)
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

async fn setup_basic_cqrs(
    pool: &Pool<Sqlite>,
) -> Result<(AccountCqrs, TokenizedAssetCqrsInternal), Box<dyn std::error::Error>>
{
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

    seed_initial_assets(&tokenized_asset_cqrs).await?;

    Ok((account_cqrs, tokenized_asset_cqrs))
}

fn setup_aggregate_cqrs(pool: &Pool<Sqlite>) -> AggregateCqrsSetup {
    let mint_view_repo = Arc::new(SqliteViewRepository::<MintView, Mint>::new(
        pool.clone(),
        "mint_view".to_string(),
    ));
    let mint_query = GenericQuery::new(mint_view_repo);

    let receipt_inventory_mint_repo =
        Arc::new(SqliteViewRepository::<ReceiptInventoryView, Mint>::new(
            pool.clone(),
            "receipt_inventory_view".to_string(),
        ));
    let receipt_inventory_mint_query =
        GenericQuery::new(receipt_inventory_mint_repo);

    let mint_cqrs = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(mint_query), Box::new(receipt_inventory_mint_query)],
        (),
    ));
    let mint_event_store = Arc::new(PersistedEventStore::new_event_store(
        SqliteEventRepository::new(pool.clone()),
    ));

    let redemption_view_repo =
        Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
            pool.clone(),
            "redemption_view".to_string(),
        ));
    let redemption_query = GenericQuery::new(redemption_view_repo);

    let receipt_inventory_redemption_repo = Arc::new(SqliteViewRepository::<
        ReceiptInventoryView,
        Redemption,
    >::new(
        pool.clone(),
        "receipt_inventory_view".to_string(),
    ));
    let receipt_inventory_redemption_query =
        GenericQuery::new(receipt_inventory_redemption_repo);

    let redemption_cqrs = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![
            Box::new(redemption_query),
            Box::new(receipt_inventory_redemption_query),
        ],
        (),
    ));
    let redemption_event_store =
        Arc::new(PersistedEventStore::new_event_store(
            SqliteEventRepository::new(pool.clone()),
        ));

    AggregateCqrsSetup {
        mint_cqrs,
        mint_event_store,
        redemption_cqrs,
        redemption_event_store,
    }
}

async fn setup_mint_managers(
    config: &Config,
    mint_cqrs: &MintCqrs,
) -> Result<
    (
        Arc<MintManager<PersistedEventStore<SqliteEventRepository, Mint>>>,
        Arc<CallbackManager<PersistedEventStore<SqliteEventRepository, Mint>>>,
    ),
    Box<dyn std::error::Error>,
> {
    let blockchain_service = config.create_blockchain_service().await?;
    let mint_manager =
        Arc::new(MintManager::new(blockchain_service, mint_cqrs.clone()));

    let alpaca_service = config.alpaca.service()?;
    let callback_manager =
        Arc::new(CallbackManager::new(alpaca_service, mint_cqrs.clone()));

    Ok((mint_manager, callback_manager))
}

fn setup_redemption_managers(
    config: &Config,
    redemption_cqrs: &RedemptionCqrs,
    redemption_event_store: &RedemptionEventStore,
) -> Result<RedemptionManagers, Box<dyn std::error::Error>> {
    let alpaca_service = config.alpaca.service()?;
    let redeem_call_manager = Arc::new(RedeemCallManager::new(
        alpaca_service.clone(),
        redemption_cqrs.clone(),
    ));
    let journal_manager = Arc::new(JournalManager::new(
        alpaca_service,
        redemption_cqrs.clone(),
        redemption_event_store.clone(),
    ));

    Ok(RedemptionManagers { redeem_call_manager, journal_manager })
}

fn spawn_redemption_detector(
    config: &Config,
    redemption_cqrs: Arc<SqliteCqrs<Redemption>>,
    redemption_event_store: Arc<
        PersistedEventStore<SqliteEventRepository, Redemption>,
    >,
    pool: Pool<Sqlite>,
    redeem_call_manager: Arc<
        RedeemCallManager<
            PersistedEventStore<SqliteEventRepository, Redemption>,
        >,
    >,
    journal_manager: Arc<
        JournalManager<PersistedEventStore<SqliteEventRepository, Redemption>>,
    >,
) {
    if let (Some(rpc_url), Some(redemption_wallet), Some(vault_address)) =
        (&config.rpc_url, config.redemption_wallet, config.vault_address)
    {
        info!(
            "WebSocket monitoring task spawned for redemption wallet {redemption_wallet}"
        );

        let detector_config = RedemptionDetectorConfig {
            rpc_url: rpc_url.clone(),
            vault_address,
            redemption_wallet,
        };

        let detector = RedemptionDetector::new(
            detector_config,
            redemption_cqrs,
            redemption_event_store,
            pool,
            redeem_call_manager,
            journal_manager,
        );

        tokio::spawn(async move {
            detector.run().await;
        });
    }
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
