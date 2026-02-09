use alloy::primitives::{Address, B256, address};
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository, sqlite_cqrs};
use sqlx::Sqlite;
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
use crate::fireblocks::SignerConfig;
use crate::alpaca::mock::MockAlpacaService;
use crate::mint::{
    CallbackManager, CqrsReceiptQuery, Mint, MintServices, MintView, Network,
    TokenSymbol, UnderlyingSymbol, mint_manager::MintManager,
};
use crate::receipt_inventory::ReceiptInventory;
use crate::tokenized_asset::{
    TokenizedAsset, TokenizedAssetCommand, TokenizedAssetView,
};
use crate::vault::VaultService;
use crate::vault::mock::MockVaultService;
use crate::{
    AccountCqrs, MintCqrs, MintEventStore, ReceiptInventoryCqrs,
    ReceiptInventoryEventStore, SqliteEventStore, TokenizedAssetCqrs,
};

pub(crate) fn test_config() -> Config {
    Config {
        database_url: "sqlite::memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
        chain_id: crate::test_utils::ANVIL_CHAIN_ID,
        signer: SignerConfig::Local(B256::ZERO),
        vault: address!("0x1111111111111111111111111111111111111111"),
        deployment_block: 0,
        auth: test_auth_config().unwrap(),
        log_level: LogLevel::Debug,
        hyperdx: None,
        alpaca: AlpacaConfig::test_default(),
    }
}

pub(crate) fn create_test_mint_manager(
    mint_cqrs: MintCqrs,
    event_store: MintEventStore,
    pool: sqlx::Pool<Sqlite>,
    receipt_inventory_cqrs: ReceiptInventoryCqrs,
    receipt_inventory_event_store: ReceiptInventoryEventStore,
) -> Arc<
    MintManager<
        PersistedEventStore<SqliteEventRepository, Mint>,
        SqliteEventStore<ReceiptInventory>,
    >,
> {
    let blockchain_service =
        Arc::new(MockVaultService::new_success()) as Arc<dyn VaultService>;
    let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    Arc::new(MintManager::new(
        blockchain_service,
        mint_cqrs,
        event_store,
        pool,
        bot,
        receipt_inventory_cqrs,
        receipt_inventory_event_store,
    ))
}

pub(crate) fn create_test_callback_manager(
    mint_cqrs: crate::MintCqrs,
    event_store: crate::MintEventStore,
    pool: sqlx::Pool<sqlx::Sqlite>,
) -> Arc<CallbackManager<PersistedEventStore<SqliteEventRepository, Mint>>> {
    let alpaca_service =
        Arc::new(crate::alpaca::mock::MockAlpacaService::new_success())
            as Arc<dyn AlpacaService>;

    Arc::new(CallbackManager::new(alpaca_service, mint_cqrs, event_store, pool))
}

pub(crate) fn create_test_event_store(
    pool: &sqlx::Pool<sqlx::Sqlite>,
) -> Arc<PersistedEventStore<SqliteEventRepository, Mint>> {
    let event_repo = SqliteEventRepository::new(pool.clone());
    Arc::new(PersistedEventStore::new_event_store(event_repo))
}

pub(crate) struct TestHarness {
    pub(crate) pool: sqlx::Pool<sqlx::Sqlite>,
    pub(crate) account_cqrs: AccountCqrs,
    pub(crate) asset_cqrs: TokenizedAssetCqrs,
    pub(crate) mint_cqrs: MintCqrs,
    pub(crate) receipt_inventory_cqrs: ReceiptInventoryCqrs,
    pub(crate) receipt_inventory_event_store: ReceiptInventoryEventStore,
}

impl TestHarness {
    pub(crate) async fn new() -> Self {
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
        let tokenized_asset_query =
            GenericQuery::new(tokenized_asset_view_repo);
        let asset_cqrs = sqlite_cqrs(
            pool.clone(),
            vec![Box::new(tokenized_asset_query)],
            (),
        );

        let receipt_inventory_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

        let receipt_inventory_event_store = {
            let event_repo = SqliteEventRepository::new(pool.clone());
            Arc::new(PersistedEventStore::new_event_store(event_repo))
        };

        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let mint_services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            pool: pool.clone(),
            bot,
            receipts: Arc::new(CqrsReceiptQuery::new(
                receipt_inventory_event_store.clone(),
            )),
        };

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);
        let mint_cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(mint_query)],
            mint_services,
        ));

        Self {
            pool,
            account_cqrs,
            asset_cqrs,
            mint_cqrs,
            receipt_inventory_cqrs,
            receipt_inventory_event_store,
        }
    }
}

pub(crate) struct TestAccountAndAsset {
    pub(crate) client_id: ClientId,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) wallet: Address,
}

impl TestHarness {
    pub(crate) async fn setup_account_and_asset(&self) -> TestAccountAndAsset {
        let email = Email::new("test@placeholder.com".to_string())
            .expect("Valid email");
        let client_id = ClientId::new();

        let register_cmd =
            AccountCommand::Register { client_id, email: email.clone() };

        let aggregate_id = client_id.to_string();
        self.account_cqrs
            .execute(&aggregate_id, register_cmd)
            .await
            .expect("Failed to register account");

        let link_cmd = AccountCommand::LinkToAlpaca {
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
        };

        self.account_cqrs
            .execute(&aggregate_id, link_cmd)
            .await
            .expect("Failed to link account to Alpaca");

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_cmd = AccountCommand::WhitelistWallet { wallet };

        self.account_cqrs
            .execute(&aggregate_id, whitelist_cmd)
            .await
            .expect("Failed to whitelist wallet");

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

        self.asset_cqrs
            .execute(&underlying.0, asset_cmd)
            .await
            .expect("Failed to add asset");

        TestAccountAndAsset { client_id, underlying, token, network, wallet }
    }
}
