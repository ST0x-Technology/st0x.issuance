use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use cqrs_es::{AggregateContext, EventStore};
use rocket::routes;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::sync::Arc;
use tracing::info;

use crate::account::{Account, AccountView};
use crate::auth::FailedAuthRateLimiter;
use crate::mint::{
    CallbackManager, Mint, MintView, mint_manager::MintManager,
    replay_mint_view,
};
use crate::receipt_inventory::backfill::ReceiptBackfiller;
use crate::receipt_inventory::{
    CqrsReceiptService, ReceiptBurnsView, ReceiptInventory,
    ReceiptInventoryView, ReceiptMonitor, ReceiptMonitorConfig,
    burn_tracking::replay_receipt_burns_view,
};
use crate::redemption::{
    Redemption, RedemptionView,
    burn_manager::BurnManager,
    detector::{RedemptionDetector, RedemptionDetectorConfig},
    journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
    replay_redemption_view,
    upcaster::create_tokens_burned_upcaster,
};
use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetView};

pub mod account;
pub mod mint;
pub mod redemption;
pub mod test_utils;
pub mod tokenized_asset;

pub(crate) mod alpaca;
pub(crate) mod auth;
pub(crate) mod catchers;
pub(crate) mod config;
pub(crate) mod fireblocks;
pub mod receipt_inventory;
pub(crate) mod telemetry;
pub(crate) mod vault;

pub mod bindings;

pub use alpaca::AlpacaConfig;
pub use auth::{AuthConfig, IpWhitelist, IssuerApiKey};
pub use config::{Config, LogLevel, setup_tracing};
pub use fireblocks::SignerConfig;
pub use telemetry::TelemetryGuard;
pub use test_utils::ANVIL_CHAIN_ID;

pub(crate) type AccountCqrs = SqliteCqrs<Account>;
pub(crate) type TokenizedAssetCqrs = SqliteCqrs<TokenizedAsset>;

pub(crate) type MintCqrs = Arc<SqliteCqrs<Mint>>;
pub(crate) type MintEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, Mint>>;

type RedemptionCqrs = Arc<SqliteCqrs<Redemption>>;
type RedemptionEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, Redemption>>;
type ReceiptInventoryCqrs = Arc<SqliteCqrs<ReceiptInventory>>;
type ReceiptInventoryEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, ReceiptInventory>>;

pub(crate) type SqliteEventStore<A> =
    PersistedEventStore<SqliteEventRepository, A>;

pub(crate) type SqliteMintManager = Arc<
    MintManager<SqliteEventStore<Mint>, SqliteEventStore<ReceiptInventory>>,
>;
pub(crate) type SqliteCallbackManager =
    Arc<CallbackManager<SqliteEventStore<Mint>>>;

struct AggregateCqrsSetup {
    mint_cqrs: MintCqrs,
    mint_event_store: MintEventStore,
    redemption_cqrs: RedemptionCqrs,
    redemption_event_store: RedemptionEventStore,
    receipt_inventory: ReceiptInventoryDeps,
}

struct ReceiptInventoryDeps {
    cqrs: ReceiptInventoryCqrs,
    event_store: ReceiptInventoryEventStore,
}

struct MintManagers {
    mint: Arc<
        MintManager<SqliteEventStore<Mint>, SqliteEventStore<ReceiptInventory>>,
    >,
    callback: Arc<CallbackManager<SqliteEventStore<Mint>>>,
}

struct RedemptionManagers {
    redeem_call: Arc<RedeemCallManager<SqliteEventStore<Redemption>>>,
    journal: Arc<JournalManager<SqliteEventStore<Redemption>>>,
    burn: Arc<
        BurnManager<
            SqliteEventStore<Redemption>,
            SqliteEventStore<ReceiptInventory>,
        >,
    >,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Quantity(pub(crate) Decimal);

impl std::fmt::Display for Quantity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Maximum decimal places supported by Alpaca's tokenization API.
/// Quantities with more decimals must be truncated before sending to Alpaca.
pub(crate) const ALPACA_MAX_DECIMALS: u32 = 9;

impl Quantity {
    pub(crate) const fn new(value: Decimal) -> Self {
        Self(value)
    }

    /// Truncates quantity to the specified number of decimal places.
    ///
    /// Returns a tuple of `(truncated, dust)` where:
    /// - `truncated` is the quantity rounded down to `decimals` places
    /// - `dust` is the remainder (`original - truncated`)
    ///
    /// Invariant: `truncated + dust == original`
    ///
    /// # Errors
    ///
    /// Returns `QuantityConversionError::Overflow` if computing 10^decimals overflows.
    pub(crate) fn truncate_to_decimals(
        &self,
        decimals: u32,
    ) -> Result<(Self, Self), QuantityConversionError> {
        let power = 10_u64
            .checked_pow(decimals)
            .ok_or(QuantityConversionError::Overflow)?;
        let multiplier = Decimal::from(power);
        let truncated_scaled = (self.0 * multiplier).trunc();
        let truncated = truncated_scaled / multiplier;
        let dust = self.0 - truncated;
        Ok((Self(truncated), Self(dust)))
    }

    /// Truncates quantity to Alpaca's maximum supported precision (9 decimals).
    ///
    /// Returns a tuple of `(alpaca_quantity, dust_quantity)` where:
    /// - `alpaca_quantity` can be safely sent to Alpaca's API
    /// - `dust_quantity` should be returned to the user
    ///
    /// # Errors
    ///
    /// Returns `QuantityConversionError::Overflow` if computing the multiplier overflows.
    pub(crate) fn truncate_for_alpaca(
        &self,
    ) -> Result<(Self, Self), QuantityConversionError> {
        self.truncate_to_decimals(ALPACA_MAX_DECIMALS)
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
pub async fn initialize_rocket(
    config: Config,
) -> Result<rocket::Rocket<rocket::Build>, anyhow::Error> {
    let pool = create_pool(&config).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let (account_cqrs, tokenized_asset_cqrs) = setup_basic_cqrs(&pool);

    let blockchain_service = config.create_blockchain_service().await?;

    let AggregateCqrsSetup {
        mint_cqrs,
        mint_event_store,
        redemption_cqrs,
        redemption_event_store,
        receipt_inventory,
    } = setup_aggregate_cqrs(&pool, blockchain_service.clone());

    let bot_wallet = config.signer.address().await?;
    info!("Bot wallet address: {bot_wallet}");

    let MintManagers { mint: mint_manager, callback: callback_manager } =
        setup_mint_managers(
            &config,
            blockchain_service.clone(),
            &mint_cqrs,
            &mint_event_store,
            &receipt_inventory.cqrs,
            &pool,
            bot_wallet,
        )?;

    spawn_mint_recovery(mint_manager.clone(), callback_manager.clone());

    let managers = setup_redemption_managers(
        &config,
        blockchain_service,
        &redemption_cqrs,
        &redemption_event_store,
        &receipt_inventory,
        &pool,
        bot_wallet,
    )?;

    info!("Replaying views to ensure schema updates are applied");
    replay_mint_view(pool.clone()).await?;
    replay_redemption_view(pool.clone()).await?;
    replay_receipt_burns_view(pool.clone()).await?;

    // Create a single provider for all receipt operations
    let provider = config.create_provider().await?;

    let receipt_contract = run_receipt_backfill(
        &config,
        provider.clone(),
        &receipt_inventory.cqrs,
        &receipt_inventory.event_store,
        bot_wallet,
    )
    .await?;

    spawn_receipt_monitor(
        provider,
        &config,
        receipt_contract,
        receipt_inventory.cqrs.clone(),
        bot_wallet,
    );

    spawn_redemption_recovery(
        managers.redeem_call.clone(),
        managers.journal.clone(),
        managers.burn.clone(),
    );

    spawn_redemption_detector(
        &config,
        redemption_cqrs.clone(),
        redemption_event_store,
        pool.clone(),
        managers,
        bot_wallet,
    );

    let rate_limiter = FailedAuthRateLimiter::new()?;

    let figment = rocket::Config::figment()
        .merge(("address", "0.0.0.0"))
        .merge(("port", 8000))
        // Disable header-based IP detection (X-Real-IP/X-Forwarded-For) to prevent
        // IP spoofing. The app relies solely on TCP source address for client IP.
        // If deployed behind a reverse proxy, the proxy must preserve the original
        // client IP at the network layer (e.g., PROXY protocol) rather than headers.
        .merge(("ip_header", false));

    Ok(rocket::custom(figment)
        .manage(config)
        .manage(rate_limiter)
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
                account::register_account,
                account::connect_account,
                account::whitelist_wallet,
                tokenized_asset::list_tokenized_assets,
                tokenized_asset::add_tokenized_asset,
                mint::initiate_mint,
                mint::confirm_journal
            ],
        )
        .register("/", catchers::json_catchers()))
}

fn setup_basic_cqrs(pool: &Pool<Sqlite>) -> (AccountCqrs, TokenizedAssetCqrs) {
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

    (account_cqrs, tokenized_asset_cqrs)
}

fn setup_aggregate_cqrs(
    pool: &Pool<Sqlite>,
    vault_service: Arc<dyn vault::VaultService>,
) -> AggregateCqrsSetup {
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

    // ReceiptBurnsView tracks burns keyed by redemption aggregate_id (red-xxx).
    // Use GenericQuery since aggregate_id = view_id is correct for this view.
    // Available balance is computed at query time by joining with receipt_inventory_view.
    let receipt_burns_repo =
        Arc::new(SqliteViewRepository::<ReceiptBurnsView, Redemption>::new(
            pool.clone(),
            "receipt_burns_view".to_string(),
        ));
    let receipt_burns_query = GenericQuery::new(receipt_burns_repo);

    let redemption_cqrs = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(redemption_query), Box::new(receipt_burns_query)],
        vault_service,
    ));
    let redemption_event_store = Arc::new(
        PersistedEventStore::new_event_store(SqliteEventRepository::new(
            pool.clone(),
        ))
        .with_upcasters(vec![create_tokens_burned_upcaster()]),
    );

    let receipt_inventory_cqrs =
        Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

    let receipt_inventory_event_store =
        Arc::new(PersistedEventStore::new_event_store(
            SqliteEventRepository::new(pool.clone()),
        ));

    AggregateCqrsSetup {
        mint_cqrs,
        mint_event_store,
        redemption_cqrs,
        redemption_event_store,
        receipt_inventory: ReceiptInventoryDeps {
            cqrs: receipt_inventory_cqrs,
            event_store: receipt_inventory_event_store,
        },
    }
}

fn setup_mint_managers(
    config: &Config,
    blockchain_service: Arc<dyn vault::VaultService>,
    mint_cqrs: &MintCqrs,
    mint_event_store: &MintEventStore,
    receipt_inventory_cqrs: &ReceiptInventoryCqrs,
    pool: &Pool<Sqlite>,
    bot_wallet: Address,
) -> Result<MintManagers, anyhow::Error> {
    let mint = Arc::new(MintManager::new(
        blockchain_service,
        mint_cqrs.clone(),
        mint_event_store.clone(),
        pool.clone(),
        bot_wallet,
        receipt_inventory_cqrs.clone(),
    ));

    let alpaca_service = config.alpaca.service()?;
    let callback = Arc::new(CallbackManager::new(
        alpaca_service,
        mint_cqrs.clone(),
        mint_event_store.clone(),
        pool.clone(),
    ));

    Ok(MintManagers { mint, callback })
}

fn setup_redemption_managers(
    config: &Config,
    blockchain_service: Arc<dyn vault::VaultService>,
    redemption_cqrs: &RedemptionCqrs,
    redemption_event_store: &RedemptionEventStore,
    receipt_inventory: &ReceiptInventoryDeps,
    pool: &Pool<Sqlite>,
    bot_wallet: Address,
) -> Result<RedemptionManagers, anyhow::Error> {
    let alpaca_service = config.alpaca.service()?;
    let redeem_call = Arc::new(RedeemCallManager::new(
        alpaca_service.clone(),
        redemption_cqrs.clone(),
        redemption_event_store.clone(),
        pool.clone(),
    ));
    let journal = Arc::new(JournalManager::new(
        alpaca_service,
        redemption_cqrs.clone(),
        redemption_event_store.clone(),
        pool.clone(),
    ));

    let receipt_service = Arc::new(CqrsReceiptService::new(
        receipt_inventory.event_store.clone(),
    ));

    let burn = Arc::new(BurnManager::new(
        blockchain_service,
        pool.clone(),
        redemption_cqrs.clone(),
        redemption_event_store.clone(),
        receipt_service,
        receipt_inventory.cqrs.clone(),
        bot_wallet,
    ));

    Ok(RedemptionManagers { redeem_call, journal, burn })
}

fn spawn_redemption_detector(
    config: &Config,
    redemption_cqrs: Arc<SqliteCqrs<Redemption>>,
    redemption_event_store: Arc<
        PersistedEventStore<SqliteEventRepository, Redemption>,
    >,
    pool: Pool<Sqlite>,
    managers: RedemptionManagers,
    bot_wallet: Address,
) {
    info!("Spawning WebSocket monitoring task for bot wallet {bot_wallet}");

    let detector_config = RedemptionDetectorConfig {
        rpc_url: config.rpc_url.clone(),
        vault: config.vault,
        bot_wallet,
    };

    let detector = RedemptionDetector::new(
        detector_config,
        redemption_cqrs,
        redemption_event_store,
        pool,
        managers.redeem_call,
        managers.journal,
        managers.burn,
    );

    tokio::spawn(async move {
        detector.run().await;
    });
}

fn spawn_mint_recovery(
    mint_manager: Arc<
        MintManager<
            PersistedEventStore<SqliteEventRepository, Mint>,
            PersistedEventStore<SqliteEventRepository, ReceiptInventory>,
        >,
    >,
    callback_manager: Arc<
        CallbackManager<PersistedEventStore<SqliteEventRepository, Mint>>,
    >,
) {
    info!("Spawning mint recovery task");

    tokio::spawn(async move {
        mint_manager.recover_journal_confirmed_mints().await;
        callback_manager.recover_callback_pending_mints().await;
    });
}

fn spawn_redemption_recovery(
    redeem_call: Arc<
        RedeemCallManager<
            PersistedEventStore<SqliteEventRepository, Redemption>,
        >,
    >,
    journal: Arc<
        JournalManager<PersistedEventStore<SqliteEventRepository, Redemption>>,
    >,
    burn: Arc<
        BurnManager<
            PersistedEventStore<SqliteEventRepository, Redemption>,
            PersistedEventStore<SqliteEventRepository, ReceiptInventory>,
        >,
    >,
) {
    info!("Spawning redemption recovery task");

    tokio::spawn(async move {
        redeem_call.recover_detected_redemptions().await;
        journal.recover_alpaca_called_redemptions().await;
        burn.recover_burning_redemptions().await;
        burn.recover_burn_failed_redemptions().await;
    });
}

/// Runs receipt backfill and returns the receipt contract address for use by
/// the live monitor.
async fn run_receipt_backfill<P: alloy::providers::Provider + Clone>(
    config: &Config,
    provider: P,
    receipt_inventory_cqrs: &ReceiptInventoryCqrs,
    receipt_inventory_event_store: &ReceiptInventoryEventStore,
    bot_wallet: Address,
) -> Result<Address, anyhow::Error> {
    // Query the vault for its receipt contract address.
    // The vault uses a separate ERC-1155 contract for receipts which emits
    // the TransferSingle/TransferBatch events we need to discover.
    let vault_contract =
        bindings::OffchainAssetReceiptVault::new(config.vault, &provider);
    let receipt_contract =
        Address::from(vault_contract.receipt().call().await?.0);

    // Load aggregate to get last backfilled block
    let aggregate_context = receipt_inventory_event_store
        .load_aggregate(&config.vault.to_string())
        .await?;

    let from_block = aggregate_context
        .aggregate()
        .last_backfilled_block()
        .unwrap_or(config.deployment_block);

    info!(
        vault = %config.vault,
        receipt_contract = %receipt_contract,
        bot_wallet = %bot_wallet,
        from_block,
        "Running receipt backfill"
    );

    let backfiller = ReceiptBackfiller::new(
        provider,
        receipt_contract,
        bot_wallet,
        config.vault,
        receipt_inventory_cqrs.clone(),
    );

    let result = backfiller.backfill_receipts(from_block).await?;

    info!(
        processed = result.processed_count,
        skipped_zero_balance = result.skipped_zero_balance,
        "Receipt backfill complete"
    );

    Ok(receipt_contract)
}

fn spawn_receipt_monitor<P>(
    provider: P,
    config: &Config,
    receipt_contract: Address,
    receipt_inventory_cqrs: ReceiptInventoryCqrs,
    bot_wallet: Address,
) where
    P: Provider + Clone + Send + Sync + 'static,
{
    info!(
        vault = %config.vault,
        receipt_contract = %receipt_contract,
        bot_wallet = %bot_wallet,
        "Spawning receipt monitor"
    );

    let monitor_config = ReceiptMonitorConfig {
        vault: config.vault,
        receipt_contract,
        bot_wallet,
    };

    let monitor =
        ReceiptMonitor::new(provider, monitor_config, receipt_inventory_cqrs);

    tokio::spawn(async move {
        monitor.run().await;
    });
}

async fn create_pool(config: &Config) -> Result<Pool<Sqlite>, sqlx::Error> {
    SqlitePoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, uint};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    use super::{Quantity, QuantityConversionError};

    #[test]
    fn test_quantity_display() {
        let quantity = Quantity::new(Decimal::from(100));
        assert_eq!(format!("{quantity}"), "100");

        let quantity_with_decimals = Quantity::new(Decimal::new(12345, 2));
        assert_eq!(format!("{quantity_with_decimals}"), "123.45");
    }

    #[test]
    fn test_to_u256_with_18_decimals_whole_number() {
        let quantity = Quantity::new(Decimal::from(100));
        let result = quantity.to_u256_with_18_decimals().unwrap();
        assert_eq!(result, uint!(100_000000000000000000_U256));
    }

    #[test]
    fn test_to_u256_with_18_decimals_with_decimals() {
        let quantity = Quantity::new(Decimal::new(12345, 2));
        let result = quantity.to_u256_with_18_decimals().unwrap();
        assert_eq!(result, uint!(123_450000000000000000_U256));
    }

    #[test]
    fn test_to_u256_with_18_decimals_zero() {
        let quantity = Quantity::new(Decimal::ZERO);
        let result = quantity.to_u256_with_18_decimals().unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn test_to_u256_with_18_decimals_negative_fails() {
        let quantity = Quantity::new(Decimal::from(-10));
        let result = quantity.to_u256_with_18_decimals();
        assert!(matches!(
            result,
            Err(QuantityConversionError::NegativeValue { .. })
        ));
    }

    #[test]
    fn test_to_u256_with_18_decimals_fractional_beyond_18_decimals_fails() {
        let quantity = Quantity::new(Decimal::new(1, 19));
        let result = quantity.to_u256_with_18_decimals();
        assert!(matches!(
            result,
            Err(QuantityConversionError::FractionalValue { .. })
        ));
    }

    #[test]
    fn test_to_u256_with_18_decimals_max_18_decimals() {
        let quantity = Quantity::new(Decimal::new(123_456_789_012_345_678, 18));
        let result = quantity.to_u256_with_18_decimals().unwrap();
        assert_eq!(result, uint!(123456789012345678_U256));
    }

    #[test]
    fn test_from_u256_with_18_decimals_whole_number() {
        let u256_value = uint!(100_000000000000000000_U256);
        let quantity =
            Quantity::from_u256_with_18_decimals(u256_value).unwrap();
        assert_eq!(quantity.0, Decimal::from(100));
    }

    #[test]
    fn test_from_u256_with_18_decimals_with_decimals() {
        let u256_value = uint!(123_450000000000000000_U256);
        let quantity =
            Quantity::from_u256_with_18_decimals(u256_value).unwrap();
        assert_eq!(quantity.0, Decimal::new(12345, 2));
    }

    #[test]
    fn test_from_u256_with_18_decimals_zero() {
        let quantity =
            Quantity::from_u256_with_18_decimals(U256::ZERO).unwrap();
        assert_eq!(quantity.0, Decimal::ZERO);
    }

    #[test]
    fn test_from_u256_with_18_decimals_preserves_precision() {
        let u256_value = uint!(123456789012345678_U256);
        let quantity =
            Quantity::from_u256_with_18_decimals(u256_value).unwrap();
        assert_eq!(quantity.0, Decimal::new(123_456_789_012_345_678, 18));
    }

    #[test]
    fn test_round_trip_conversion() {
        let original = Quantity::new(Decimal::from(100));
        let u256_value = original.to_u256_with_18_decimals().unwrap();
        let round_trip =
            Quantity::from_u256_with_18_decimals(u256_value).unwrap();
        assert_eq!(original, round_trip);
    }

    #[test]
    fn test_round_trip_conversion_with_decimals() {
        let original = Quantity::new(Decimal::new(12345, 2));
        let u256_value = original.to_u256_with_18_decimals().unwrap();
        let round_trip =
            Quantity::from_u256_with_18_decimals(u256_value).unwrap();
        assert_eq!(original, round_trip);
    }

    #[test]
    fn test_truncate_to_decimals_with_dust() {
        // 0.450574852280275235 -> truncated to 9 decimals
        // Expected: truncated = 0.450574852, dust = 0.000000000280275235
        let original =
            Quantity::new(Decimal::from_str("0.450574852280275235").unwrap());

        let (truncated, dust) = original.truncate_to_decimals(9).unwrap();

        assert_eq!(truncated.0, Decimal::from_str("0.450574852").unwrap());
        assert_eq!(dust.0, Decimal::from_str("0.000000000280275235").unwrap());

        // Verify invariant: truncated + dust == original
        assert_eq!(truncated.0 + dust.0, original.0);
    }

    #[test]
    fn test_truncate_to_decimals_no_dust() {
        // Exactly 9 decimals - no dust
        let original = Quantity::new(Decimal::from_str("0.123456789").unwrap());

        let (truncated, dust) = original.truncate_to_decimals(9).unwrap();

        assert_eq!(truncated.0, original.0);
        assert_eq!(dust.0, Decimal::ZERO);
    }

    #[test]
    fn test_truncate_to_decimals_whole_number() {
        let original = Quantity::new(Decimal::from(100));

        let (truncated, dust) = original.truncate_to_decimals(9).unwrap();

        assert_eq!(truncated.0, Decimal::from(100));
        assert_eq!(dust.0, Decimal::ZERO);
    }

    #[test]
    fn test_truncate_to_decimals_zero() {
        let original = Quantity::new(Decimal::ZERO);

        let (truncated, dust) = original.truncate_to_decimals(9).unwrap();

        assert_eq!(truncated.0, Decimal::ZERO);
        assert_eq!(dust.0, Decimal::ZERO);
    }

    #[test]
    fn test_truncate_to_decimals_overflow() {
        let original = Quantity::new(Decimal::from(1));

        // 10^100 would overflow u64
        let result = original.truncate_to_decimals(100);

        assert!(matches!(result, Err(QuantityConversionError::Overflow)));
    }

    #[test]
    fn test_truncate_for_alpaca() {
        // Uses ALPACA_MAX_DECIMALS = 9
        let original =
            Quantity::new(Decimal::from_str("1.123456789123456789").unwrap());

        let (alpaca_qty, dust_qty) = original.truncate_for_alpaca().unwrap();

        assert_eq!(alpaca_qty.0, Decimal::from_str("1.123456789").unwrap());
        assert_eq!(
            dust_qty.0,
            Decimal::from_str("0.000000000123456789").unwrap()
        );

        // Verify invariant
        assert_eq!(alpaca_qty.0 + dust_qty.0, original.0);
    }

    #[test]
    fn test_truncate_preserves_u256_conversion_integrity() {
        // Ensure truncated values can still be converted to U256
        let original =
            Quantity::new(Decimal::from_str("0.450574852280275235").unwrap());

        let (truncated, dust) = original.truncate_for_alpaca().unwrap();

        // Both should convert to U256 without error
        let truncated_u256 = truncated.to_u256_with_18_decimals().unwrap();
        let dust_u256 = dust.to_u256_with_18_decimals().unwrap();
        let original_u256 = original.to_u256_with_18_decimals().unwrap();

        // Verify: truncated_u256 + dust_u256 == original_u256
        assert_eq!(truncated_u256 + dust_u256, original_u256);
    }
}
