use alloy::primitives::{B256, address};
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository, sqlite_cqrs};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::Arc;
use url::Url;

use crate::account::{
    Account, AccountCommand, AccountView, AlpacaAccountNumber, ClientId, Email,
};
use crate::alpaca::AlpacaService;
use crate::alpaca::service::AlpacaConfig;
use crate::auth::test_auth_config;
use crate::config::{Config, LogLevel};
use crate::mint::{
    CallbackManager, Mint, MintView, Network, TokenSymbol, UnderlyingSymbol,
    mint_manager::MintManager,
};
use crate::tokenized_asset::{
    TokenizedAsset, TokenizedAssetCommand, TokenizedAssetView,
};
use crate::vault::VaultService;
use crate::vault::mock::MockVaultService;

pub(super) fn test_config() -> Config {
    Config {
        database_url: "sqlite::memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
        private_key: B256::ZERO,
        vault: address!("0x1111111111111111111111111111111111111111"),
        bot: address!("0x2222222222222222222222222222222222222222"),
        auth: test_auth_config().unwrap(),
        log_level: LogLevel::Debug,
        hyperdx: None,
        alpaca: AlpacaConfig::test_default(),
    }
}

pub(super) fn create_test_mint_manager(
    mint_cqrs: crate::MintCqrs,
) -> Arc<MintManager<PersistedEventStore<SqliteEventRepository, Mint>>> {
    let blockchain_service =
        Arc::new(MockVaultService::new_success()) as Arc<dyn VaultService>;
    let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    Arc::new(MintManager::new(blockchain_service, mint_cqrs, bot))
}

pub(super) fn create_test_callback_manager(
    mint_cqrs: crate::MintCqrs,
) -> Arc<CallbackManager<PersistedEventStore<SqliteEventRepository, Mint>>> {
    let alpaca_service =
        Arc::new(crate::alpaca::mock::MockAlpacaService::new_success())
            as Arc<dyn AlpacaService>;

    Arc::new(CallbackManager::new(alpaca_service, mint_cqrs))
}

pub(super) fn create_test_event_store(
    pool: &sqlx::Pool<sqlx::Sqlite>,
) -> Arc<PersistedEventStore<SqliteEventRepository, Mint>> {
    let event_repo = SqliteEventRepository::new(pool.clone());
    Arc::new(PersistedEventStore::new_event_store(event_repo))
}

pub(super) async fn setup_test_environment() -> (
    sqlx::Pool<sqlx::Sqlite>,
    crate::AccountCqrs,
    crate::TokenizedAssetCqrs,
    crate::MintCqrs,
) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .expect("Failed to create in-memory database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

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
    let mint_cqrs =
        Arc::new(sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ()));

    (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs)
}

pub(super) async fn setup_with_account_and_asset(
    account_cqrs: &crate::AccountCqrs,
    tokenized_asset_cqrs: &crate::TokenizedAssetCqrs,
) -> (ClientId, UnderlyingSymbol, TokenSymbol, Network) {
    let email =
        Email::new("test@placeholder.com".to_string()).expect("Valid email");
    let client_id = ClientId::new();

    let register_cmd =
        AccountCommand::Register { client_id, email: email.clone() };

    let aggregate_id = client_id.to_string();
    account_cqrs
        .execute(&aggregate_id, register_cmd)
        .await
        .expect("Failed to register account");

    let link_cmd = AccountCommand::LinkToAlpaca {
        alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
    };

    account_cqrs
        .execute(&aggregate_id, link_cmd)
        .await
        .expect("Failed to link account to Alpaca");

    let underlying = UnderlyingSymbol::new("AAPL");
    let token = TokenSymbol::new("tAAPL");
    let network = Network::new("base");
    let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

    let asset_cmd = TokenizedAssetCommand::Add {
        underlying: underlying.clone(),
        token: token.clone(),
        network: network.clone(),
        vault,
    };

    tokenized_asset_cqrs
        .execute(&underlying.0, asset_cmd)
        .await
        .expect("Failed to add asset");

    (client_id, underlying, token, network)
}
