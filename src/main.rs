#[macro_use]
extern crate rocket;

mod account;
mod tokenized_asset;

use clap::Parser;
use cqrs_es::persist::GenericQuery;
use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::sync::Arc;

use account::{Account, AccountView};

type AccountCqrs = SqliteCqrs<Account>;

#[derive(Debug, Parser)]
#[command(name = "st0x-issuance")]
#[command(about = "Issuance bot for tokenizing equities via Alpaca ITN")]
struct Config {
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
}

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

    rocket::build().manage(account_cqrs).manage(pool).mount(
        "/",
        routes![
            account::connect_account,
            tokenized_asset::list_tokenized_assets
        ],
    )
}

async fn create_pool(config: &Config) -> Result<Pool<Sqlite>, sqlx::Error> {
    SqlitePoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await
}
