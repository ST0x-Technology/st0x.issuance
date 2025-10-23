use alloy::primitives::address;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use rocket::routes;
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::Arc;

use crate::account::{Account, AccountView};
use crate::alpaca::MockAlpacaService;
use crate::blockchain::MockBlockchainService;
use crate::mint::mint_manager::MintManager;
use crate::mint::{CallbackManager, Mint, MintView};
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    TokenizedAssetView, UnderlyingSymbol,
};

/// Test environment containing a configured Rocket instance and mock services.
pub struct TestEnv {
    pub rocket: rocket::Rocket<rocket::Build>,
    pub blockchain_service: Arc<MockBlockchainService>,
    pub alpaca_service: Arc<MockAlpacaService>,
}

/// Sets up a test Rocket instance with in-memory database and mock services.
///
/// # Panics
/// Panics if database creation, migrations, or asset seeding fails.
pub async fn setup_test_rocket() -> TestEnv {
    // Create in-memory database
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(":memory:")
        .await
        .expect("Failed to create in-memory database");

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    // Setup Account CQRS
    let account_view_repo =
        Arc::new(SqliteViewRepository::<AccountView, Account>::new(
            pool.clone(),
            "account_view".to_string(),
        ));

    let account_query = GenericQuery::new(account_view_repo);
    let account_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

    // Setup TokenizedAsset CQRS
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

    // Setup Mint CQRS
    let mint_view_repo = Arc::new(SqliteViewRepository::<MintView, Mint>::new(
        pool.clone(),
        "mint_view".to_string(),
    ));

    let mint_query = GenericQuery::new(mint_view_repo);
    let mint_cqrs_raw =
        sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ());
    let mint_cqrs = Arc::new(mint_cqrs_raw);

    let mint_event_repo = SqliteEventRepository::new(pool.clone());
    let mint_event_store = Arc::new(PersistedEventStore::<
        SqliteEventRepository,
        Mint,
    >::new_event_store(mint_event_repo));

    // Seed initial assets
    seed_test_assets(&tokenized_asset_cqrs).await;

    // Create mock services
    let blockchain_service = Arc::new(MockBlockchainService::new_success());
    let alpaca_service = Arc::new(MockAlpacaService::new_success());

    // Create managers
    let mint_manager = Arc::new(MintManager::new(
        blockchain_service.clone(),
        mint_cqrs.clone(),
    ));

    let callback_manager = Arc::new(CallbackManager::new(
        alpaca_service.clone(),
        mint_cqrs.clone(),
    ));

    // Build rocket
    let rocket = rocket::build()
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
                crate::account::connect_account,
                crate::tokenized_asset::list_tokenized_assets,
                crate::mint::initiate_mint,
                crate::mint::confirm_journal
            ],
        );

    TestEnv { rocket, blockchain_service, alpaca_service }
}

async fn seed_test_assets(cqrs: &SqliteCqrs<TokenizedAsset>) {
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
    ];

    for (underlying, token, network, vault_address) in assets {
        let command = TokenizedAssetCommand::Add {
            underlying: UnderlyingSymbol::new(underlying),
            token: TokenSymbol::new(token),
            network: Network::new(network),
            vault_address,
        };

        cqrs.execute(underlying, command)
            .await
            .expect("Failed to seed test asset");
    }
}
