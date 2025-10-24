use alloy::primitives::address;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository, sqlite_cqrs};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::Arc;

use crate::account::{
    Account, AccountCommand, AccountView, AlpacaAccountNumber, Email,
    view::find_by_email,
};
use crate::alpaca::AlpacaService;
use crate::mint::{
    CallbackManager, Mint, MintView, Network, TokenSymbol, UnderlyingSymbol,
    mint_manager::MintManager,
};
use crate::tokenized_asset::{
    TokenizedAsset, TokenizedAssetCommand, TokenizedAssetView,
};
use crate::vault::VaultService;
use crate::vault::mock::MockBlockchainService;

pub(super) fn create_test_mint_manager(
    mint_cqrs: crate::MintCqrs,
) -> Arc<MintManager<PersistedEventStore<SqliteEventRepository, Mint>>> {
    let blockchain_service = Arc::new(MockBlockchainService::new_success())
        as Arc<dyn VaultService>;

    Arc::new(MintManager::new(blockchain_service, mint_cqrs))
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
    pool: &sqlx::Pool<sqlx::Sqlite>,
    account_cqrs: &crate::AccountCqrs,
    tokenized_asset_cqrs: &crate::TokenizedAssetCqrs,
) -> (String, UnderlyingSymbol, TokenSymbol, Network) {
    let email =
        Email::new("test@placeholder.com".to_string()).expect("Valid email");

    let account_cmd = AccountCommand::Link {
        email: email.clone(),
        alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
    };

    account_cqrs
        .execute(email.as_str(), account_cmd)
        .await
        .expect("Failed to link account");

    let account_view = find_by_email(pool, &email)
        .await
        .expect("Failed to query account")
        .expect("Account should exist");

    let AccountView::Account { client_id, .. } = account_view else {
        panic!("Expected Account variant");
    };

    let underlying = UnderlyingSymbol::new("AAPL");
    let token = TokenSymbol::new("tAAPL");
    let network = Network::new("base");
    let vault_address = address!("0x1234567890abcdef1234567890abcdef12345678");

    let asset_cmd = TokenizedAssetCommand::Add {
        underlying: underlying.clone(),
        token: token.clone(),
        network: network.clone(),
        vault_address,
    };

    tokenized_asset_cqrs
        .execute(&underlying.0, asset_cmd)
        .await
        .expect("Failed to add asset");

    (client_id.0, underlying, token, network)
}
