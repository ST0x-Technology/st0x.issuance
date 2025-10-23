#[macro_use]
extern crate rocket;

mod account;
mod alpaca;
mod bindings;
mod blockchain;
mod config;
mod mint;
mod tokenized_asset;

use alloy::primitives::address;
use clap::Parser;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::sync::Arc;
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use account::{Account, AccountView};
use config::Config;
use mint::{CallbackManager, Mint, MintView, mint_manager::MintManager};
use tokenized_asset::{TokenizedAsset, TokenizedAssetView};

type AccountCqrs = SqliteCqrs<Account>;
type TokenizedAssetCqrs = SqliteCqrs<TokenizedAsset>;
type MintCqrs = Arc<SqliteCqrs<Mint>>;
type MintEventStore = Arc<PersistedEventStore<SqliteEventRepository, Mint>>;

#[launch]
async fn rocket() -> _ {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .init();

    match initialize_rocket().await {
        Ok(rocket) => rocket,
        Err(e) => {
            error!("Failed to initialize application: {e}");
            std::process::exit(1);
        }
    }
}

async fn initialize_rocket()
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
    cqrs: &TokenizedAssetCqrs,
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
        let command = tokenized_asset::TokenizedAssetCommand::Add {
            underlying: tokenized_asset::UnderlyingSymbol::new(underlying),
            token: tokenized_asset::TokenSymbol::new(token),
            network: tokenized_asset::Network::new(network),
            vault_address,
        };

        cqrs.execute(underlying, command).await?;
    }

    Ok(())
}

async fn create_pool(config: &Config) -> Result<Pool<Sqlite>, sqlx::Error> {
    SqlitePoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await
}
