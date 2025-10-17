#[macro_use]
extern crate rocket;

mod account;

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

    let pool = create_pool(&config).await;

    let account_view_repo =
        Arc::new(SqliteViewRepository::<AccountView, Account>::new(
            pool.clone(),
            "account_view".to_string(),
        ));

    let account_query = GenericQuery::new(account_view_repo);

    let account_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

    rocket::build()
        .manage(account_cqrs)
        .manage(pool)
        .mount("/", routes![account::connect_account])
}

async fn create_pool(config: &Config) -> Pool<Sqlite> {
    SqlitePoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await
        .expect("Failed to create database pool")
}
