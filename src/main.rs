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

use account::{Account, AccountView};
use config::Config;
use mint::{Mint, MintView, conductor::MintConductor};
use tokenized_asset::{TokenizedAsset, TokenizedAssetView};

type AccountCqrs = SqliteCqrs<Account>;
type TokenizedAssetCqrs = SqliteCqrs<TokenizedAsset>;
type MintCqrs = Arc<SqliteCqrs<Mint>>;
type MintConductorType =
    Arc<MintConductor<PersistedEventStore<SqliteEventRepository, Mint>>>;

#[launch]
async fn rocket() -> _ {
    let config = Config::parse();

    let pool = create_pool(&config).await.unwrap_or_else(|e| {
        eprintln!("Failed to create database pool: {e}");
        std::process::exit(1);
    });

    sqlx::migrate!("./migrations").run(&pool).await.unwrap_or_else(|e| {
        eprintln!("Failed to run database migrations: {e}");
        std::process::exit(1);
    });

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

    seed_initial_assets(&tokenized_asset_cqrs).await.unwrap_or_else(|e| {
        eprintln!("Failed to seed initial assets: {e}");
        std::process::exit(1);
    });

    let blockchain_service =
        config.create_blockchain_service().unwrap_or_else(|e| {
            eprintln!("Failed to create blockchain service: {e}");
            std::process::exit(1);
        });

    let mint_conductor =
        Arc::new(MintConductor::new(blockchain_service, mint_cqrs.clone()));

    rocket::build()
        .manage(account_cqrs)
        .manage(tokenized_asset_cqrs)
        .manage(mint_cqrs)
        .manage(mint_conductor)
        .manage(pool)
        .mount(
            "/",
            routes![
                account::connect_account,
                tokenized_asset::list_tokenized_assets,
                mint::initiate_mint,
                mint::confirm_journal
            ],
        )
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
