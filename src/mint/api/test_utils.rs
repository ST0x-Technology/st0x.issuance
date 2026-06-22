use alloy::primitives::{Address, B256, address};
use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use event_sorcery::{Store, StoreBuilder, test_store};
use sqlx::sqlite::{SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::account::{
    Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
};
use crate::alpaca::mock::MockAlpacaService;
use crate::alpaca::service::AlpacaConfig;
use crate::auth::test_auth_config;
use crate::config::{Config, Environment, LogLevel};
use crate::fireblocks::SignerConfig;
use crate::mint::{Mint, MintServices, Network, TokenSymbol, UnderlyingSymbol};
use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};
use crate::vault::VaultService;
use crate::vault::mock::MockVaultService;

pub(crate) fn test_config() -> Config {
    Config {
        database_url: "sqlite::memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
        chain_id: crate::test_utils::ANVIL_CHAIN_ID,
        signer: SignerConfig::Local(B256::ZERO),
        backfill_start_block: 0,
        receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
        auth: test_auth_config().unwrap(),
        log_level: LogLevel::Debug,
        environment: Environment::Development,
        hyperdx: None,
        alpaca: AlpacaConfig::test_default(),
        subgraph_url: Url::parse("http://localhost:0/subgraph")
            .expect("valid test URL"),
    }
}

pub(crate) struct TestHarness {
    pub(crate) pool: sqlx::Pool<sqlx::Sqlite>,
    /// The apalis-sqlite (sqlx 0.8) pool the confirm/admin routes require to
    /// enqueue recovery jobs. Shares the event-store pool's SQLite file so the
    /// `Jobs`/`Workers` tables our migrations create are visible to it (two
    /// private `:memory:` DBs would not share them).
    pub(crate) apalis_pool: ApalisSqlitePool,
    pub(crate) account_store: Arc<Store<Account>>,
    pub(crate) asset_store: Arc<Store<TokenizedAsset>>,
    pub(crate) mint_store: Arc<Store<Mint>>,
}

impl TestHarness {
    pub(crate) async fn new() -> Self {
        Self::new_with_mint_vault(Arc::new(MockVaultService::new_success()))
            .await
    }

    /// Like [`new`](Self::new), but with a caller-chosen vault so failure-path
    /// tests can drive a mint into `MintingFailed`.
    pub(crate) async fn new_with_mint_vault(
        vault: Arc<dyn VaultService>,
    ) -> Self {
        // Both pools must address the SAME SQLite file: the apalis-sqlite (0.8)
        // pool needs the `Jobs`/`Workers` tables our migrations create on the
        // event-store (0.9) pool, and the two sqlx majors do not share a private
        // `:memory:` database. WAL + busy_timeout mirror the production pools so
        // the two writers coexist on one file without lock flakes.
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let db_url =
            format!("sqlite:{}", temp_dir.path().join("issuance.db").display());

        // Persist the dir for the process rather than holding it as a drop
        // guard on the harness: callers routinely move fields out with
        // `let TestHarness { pool, apalis_pool, mint_store, .. } = harness`,
        // which would drop a guard field immediately and delete the database
        // while the extracted pools are still using it. The small dir is
        // reclaimed by the OS temp cleanup.
        let _persisted_db_dir = temp_dir.keep();

        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)
            .expect("valid sqlite url")
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("Failed to create database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build account store");

        let (asset_store, _asset_projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let receipt_inventory_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let mint_services = MintServices {
            vault,
            alpaca: Arc::new(MockAlpacaService::new_success()),
            pool: pool.clone(),
            bot,
            receipts: Arc::new(CqrsReceiptService::new(
                receipt_inventory_store,
            )),
        };

        let (mint_store, _mint_projection) =
            StoreBuilder::<Mint>::new(pool.clone())
                .build(mint_services)
                .await
                .expect("Failed to build mint store");

        let apalis_options =
            apalis_sqlite::SqliteConnectOptions::from_str(&db_url)
                .expect("valid sqlite url")
                .pragma("journal_mode", "WAL")
                .busy_timeout(Duration::from_secs(5));

        let apalis_pool = ApalisSqlitePool::connect_with(apalis_options)
            .await
            .expect("Failed to create apalis pool");

        Self { pool, apalis_pool, account_store, asset_store, mint_store }
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
        let email = Email::new("test@placeholder.com").expect("Valid email");
        let client_id = ClientId::new();

        let register_cmd =
            AccountCommand::Register { client_id, email: email.clone() };

        self.account_store
            .send(&client_id, register_cmd)
            .await
            .expect("Failed to register account");

        let link_cmd = AccountCommand::LinkToAlpaca {
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
        };

        self.account_store
            .send(&client_id, link_cmd)
            .await
            .expect("Failed to link account to Alpaca");

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_cmd = AccountCommand::WhitelistWallet { wallet };

        self.account_store
            .send(&client_id, whitelist_cmd)
            .await
            .expect("Failed to whitelist wallet");

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset_cmd = TokenizedAssetCommand::Add {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault,
        };

        self.asset_store
            .send(&underlying, asset_cmd)
            .await
            .expect("Failed to add asset");

        TestAccountAndAsset { client_id, underlying, token, network, wallet }
    }
}
