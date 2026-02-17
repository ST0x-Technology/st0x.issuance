use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use cqrs_es::{AggregateContext, EventStore};
use futures::stream::{self, StreamExt, TryStreamExt};
use rocket::routes;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::sync::Arc;
use tracing::{debug, error, info};
use url::Url;

use crate::account::{Account, AccountView};
use crate::alpaca::AlpacaService;
use crate::auth::FailedAuthRateLimiter;
use crate::mint::{
    Mint, MintServices, MintView, find_all_recoverable_mints,
    recovery::{MintRecoveryHandler, recover_mint},
    replay_mint_view,
};
use crate::receipt_inventory::backfill::ReceiptBackfiller;
use crate::receipt_inventory::reconcile::run_startup_reconciliation;
use crate::receipt_inventory::{
    CqrsReceiptService, ItnReceiptHandler, ReceiptBurnsView, ReceiptInventory,
    ReceiptInventoryView, ReceiptMonitor, ReceiptMonitorConfig,
    burn_tracking::replay_receipt_burns_view,
};
use crate::redemption::{
    Redemption, RedemptionView,
    backfill::TransferBackfiller,
    burn_manager::BurnManager,
    detector::{RedemptionDetector, RedemptionDetectorConfig},
    journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
    replay_redemption_view,
    upcaster::create_tokens_burned_upcaster,
};
use crate::tokenized_asset::{
    TokenizedAsset, TokenizedAssetView, TokenizedAssetViewRepo,
    view::{list_enabled_assets, replay_tokenized_asset_view},
};

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

struct AggregateCqrsSetup {
    mint: MintDeps,
    redemption_cqrs: RedemptionCqrs,
    redemption_event_store: RedemptionEventStore,
    receipt_inventory: ReceiptInventoryDeps,
}

struct ReceiptInventoryDeps {
    cqrs: ReceiptInventoryCqrs,
    event_store: ReceiptInventoryEventStore,
}

struct MintDeps {
    cqrs: MintCqrs,
    event_store: MintEventStore,
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

    let BasicCqrsSetup {
        account_cqrs,
        tokenized_asset_cqrs,
        tokenized_asset_view_repo,
    } = setup_basic_cqrs(&pool);

    let blockchain_setup = config.create_blockchain_setup().await?;
    let alpaca_service = config.alpaca.service()?;
    let bot_wallet = config.signer.address().await?;
    info!("Bot wallet address: {bot_wallet}");

    let AggregateCqrsSetup {
        mint,
        redemption_cqrs,
        redemption_event_store,
        receipt_inventory,
    } = setup_aggregate_cqrs(
        &pool,
        blockchain_setup.vault_service.clone(),
        alpaca_service.clone(),
        bot_wallet,
    );

    let managers = setup_redemption_managers(
        &config,
        blockchain_setup.vault_service,
        &redemption_cqrs,
        &redemption_event_store,
        &receipt_inventory,
        &pool,
        bot_wallet,
    )?;

    // Reprojections must complete BEFORE recovery runs, so recovery queries
    // up-to-date views. Each replay clears its view table first to remove
    // stale/corrupt data, then rebuilds from the event store.
    info!("Rebuilding all views from events");
    replay_tokenized_asset_view(pool.clone()).await?;
    replay_mint_view(pool.clone()).await?;
    replay_redemption_view(pool.clone()).await?;
    replay_receipt_burns_view(pool.clone()).await?;

    // Receipt backfill must run before recovery so that recovery can check
    // receipt inventory to detect already-minted receipts (prevents double-mints).
    let vault_configs = run_all_receipt_backfills(
        &pool,
        blockchain_setup.provider.clone(),
        &receipt_inventory.cqrs,
        &receipt_inventory.event_store,
        bot_wallet,
        config.backfill_start_block,
    )
    .await?;

    // Reconcile receipt balances with on-chain state after backfill. This detects
    // external burns (manual burns by stakeholders) that the service didn't track.
    let vault_receipt_pairs: Vec<_> = vault_configs
        .iter()
        .map(|config| (config.vault, config.receipt_contract))
        .collect();

    run_startup_reconciliation(
        blockchain_setup.provider.clone(),
        &vault_receipt_pairs,
        &receipt_inventory.cqrs,
        receipt_inventory.event_store.as_ref(),
        bot_wallet,
    )
    .await?;

    // Transfer backfill must run after receipt backfill so that receipt inventory
    // is available for burn planning, and before live monitoring so that missed
    // transfers during downtime are detected before new ones come in.
    let transfer_backfiller = TransferBackfiller {
        provider: blockchain_setup.provider.clone(),
        bot_wallet,
        cqrs: redemption_cqrs.clone(),
        event_store: redemption_event_store.clone(),
        pool: pool.clone(),
        redeem_call_manager: managers.redeem_call.clone(),
        journal_manager: managers.journal.clone(),
        burn_manager: managers.burn.clone(),
    };

    run_all_transfer_backfills(
        &vault_configs,
        &transfer_backfiller,
        config.backfill_start_block,
    )
    .await?;

    spawn_all_receipt_monitors(
        blockchain_setup.provider,
        &vault_configs,
        &receipt_inventory.cqrs,
        bot_wallet,
        &MintRecoveryHandler::new(mint.cqrs.clone()),
    );

    // Recovery runs AFTER reprojections and backfill so it can query accurate state
    spawn_mint_recovery(pool.clone(), mint.cqrs.clone());
    spawn_redemption_recovery(
        managers.redeem_call.clone(),
        managers.journal.clone(),
        managers.burn.clone(),
    );

    spawn_all_redemption_detectors(
        &config.rpc_url,
        &vault_configs,
        &redemption_cqrs,
        &redemption_event_store,
        &pool,
        &managers,
        bot_wallet,
    );

    Ok(build_rocket(RocketState {
        rate_limiter: FailedAuthRateLimiter::new()?,
        config,
        pool,
        account_cqrs,
        tokenized_asset_cqrs,
        tokenized_asset_view_repo,
        mint,
        redemption_cqrs,
    }))
}

struct RocketState {
    config: Config,
    rate_limiter: FailedAuthRateLimiter,
    pool: Pool<Sqlite>,
    account_cqrs: AccountCqrs,
    tokenized_asset_cqrs: TokenizedAssetCqrs,
    tokenized_asset_view_repo: TokenizedAssetViewRepo,
    mint: MintDeps,
    redemption_cqrs: RedemptionCqrs,
}

fn build_rocket(state: RocketState) -> rocket::Rocket<rocket::Build> {
    let figment = rocket::Config::figment()
        .merge(("address", "0.0.0.0"))
        .merge(("port", 8000))
        // Disable header-based IP detection (X-Real-IP/X-Forwarded-For) to prevent
        // IP spoofing. The app relies solely on TCP source address for client IP.
        // If deployed behind a reverse proxy, the proxy must preserve the original
        // client IP at the network layer (e.g., PROXY protocol) rather than headers.
        .merge(("ip_header", false));

    rocket::custom(figment)
        .manage(state.config)
        .manage(state.rate_limiter)
        .manage(state.account_cqrs)
        .manage(state.tokenized_asset_cqrs)
        .manage(state.tokenized_asset_view_repo)
        .manage(state.mint.cqrs)
        .manage(state.mint.event_store)
        .manage(state.redemption_cqrs)
        .manage(state.pool)
        .mount(
            "/",
            routes![
                account::register_account,
                account::connect_account,
                account::whitelist_wallet,
                account::unwhitelist_wallet,
                tokenized_asset::list_tokenized_assets,
                tokenized_asset::get_tokenized_asset,
                tokenized_asset::add_tokenized_asset,
                mint::initiate_mint,
                mint::confirm_journal
            ],
        )
        .register("/", catchers::json_catchers())
}

struct BasicCqrsSetup {
    account_cqrs: AccountCqrs,
    tokenized_asset_cqrs: TokenizedAssetCqrs,
    tokenized_asset_view_repo: TokenizedAssetViewRepo,
}

fn setup_basic_cqrs(pool: &Pool<Sqlite>) -> BasicCqrsSetup {
    let account_view_repo =
        Arc::new(SqliteViewRepository::<AccountView, Account>::new(
            pool.clone(),
            "account_view".to_string(),
        ));
    let account_query = GenericQuery::new(account_view_repo);
    let account_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

    let tokenized_asset_view_repo: TokenizedAssetViewRepo =
        Arc::new(SqliteViewRepository::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));
    let tokenized_asset_query =
        GenericQuery::new(tokenized_asset_view_repo.clone());
    let tokenized_asset_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(tokenized_asset_query)], ());

    BasicCqrsSetup {
        account_cqrs,
        tokenized_asset_cqrs,
        tokenized_asset_view_repo,
    }
}

fn setup_aggregate_cqrs(
    pool: &Pool<Sqlite>,
    vault_service: Arc<dyn vault::VaultService>,
    alpaca_service: Arc<dyn AlpacaService>,
    bot_wallet: Address,
) -> AggregateCqrsSetup {
    // Create receipt inventory first since MintServices depends on it
    let receipt_inventory_cqrs =
        Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
    let receipt_inventory_event_store =
        Arc::new(PersistedEventStore::new_event_store(
            SqliteEventRepository::new(pool.clone()),
        ));

    // Create MintServices with all dependencies
    let receipt_service = Arc::new(CqrsReceiptService::new(
        receipt_inventory_event_store.clone(),
        receipt_inventory_cqrs.clone(),
    ));
    let mint_services = MintServices {
        vault: vault_service.clone(),
        alpaca: alpaca_service,
        pool: pool.clone(),
        bot: bot_wallet,
        receipts: receipt_service,
    };

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
        mint_services,
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

    AggregateCqrsSetup {
        mint: MintDeps { cqrs: mint_cqrs, event_store: mint_event_store },
        redemption_cqrs,
        redemption_event_store,
        receipt_inventory: ReceiptInventoryDeps {
            cqrs: receipt_inventory_cqrs,
            event_store: receipt_inventory_event_store,
        },
    }
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
        receipt_inventory.cqrs.clone(),
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

/// Spawns redemption detectors for ALL enabled vaults.
fn spawn_all_redemption_detectors(
    rpc_url: &Url,
    vault_configs: &[VaultBackfillConfig],
    redemption_cqrs: &Arc<SqliteCqrs<Redemption>>,
    redemption_event_store: &Arc<
        PersistedEventStore<SqliteEventRepository, Redemption>,
    >,
    pool: &Pool<Sqlite>,
    managers: &RedemptionManagers,
    bot_wallet: Address,
) {
    info!(
        vault_count = vault_configs.len(),
        "Spawning redemption detectors for all vaults"
    );

    for config in vault_configs {
        debug!(
            vault = %config.vault,
            bot_wallet = %bot_wallet,
            "Spawning redemption detector for vault"
        );

        spawn_redemption_detector(
            rpc_url.clone(),
            config.vault,
            redemption_cqrs.clone(),
            redemption_event_store.clone(),
            pool.clone(),
            RedemptionManagers {
                redeem_call: managers.redeem_call.clone(),
                journal: managers.journal.clone(),
                burn: managers.burn.clone(),
            },
            bot_wallet,
        );
    }
}

fn spawn_redemption_detector(
    rpc_url: Url,
    vault: Address,
    redemption_cqrs: Arc<SqliteCqrs<Redemption>>,
    redemption_event_store: Arc<
        PersistedEventStore<SqliteEventRepository, Redemption>,
    >,
    pool: Pool<Sqlite>,
    managers: RedemptionManagers,
    bot_wallet: Address,
) {
    info!(bot_wallet = %bot_wallet, "Spawning WebSocket monitoring task");

    let detector_config =
        RedemptionDetectorConfig { rpc_url, vault, bot_wallet };

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

fn spawn_mint_recovery(pool: Pool<Sqlite>, mint_cqrs: MintCqrs) {
    info!("Spawning mint recovery task");

    tokio::spawn(async move {
        let recoverable_mints = match find_all_recoverable_mints(&pool).await {
            Ok(mints) => mints,
            Err(err) => {
                error!(error = %err, "Failed to query recoverable mints");
                return;
            }
        };

        if recoverable_mints.is_empty() {
            debug!("No mints to recover");
            return;
        }

        info!(count = recoverable_mints.len(), "Recovering mints");

        for (issuer_request_id, _view) in recoverable_mints {
            recover_mint(&mint_cqrs, issuer_request_id).await;
        }

        debug!("Completed mint recovery");
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

/// Configuration for a single vault, extracted from TokenizedAssetView.
struct VaultBackfillConfig {
    vault: Address,
    receipt_contract: Address,
}

/// Runs receipt backfill for ALL enabled tokenized assets.
///
/// Returns the vault configurations needed for live monitoring.
async fn run_all_receipt_backfills<P: Provider + Clone>(
    pool: &Pool<Sqlite>,
    provider: P,
    receipt_inventory_cqrs: &ReceiptInventoryCqrs,
    receipt_inventory_event_store: &ReceiptInventoryEventStore,
    bot_wallet: Address,
    backfill_start_block: u64,
) -> Result<Vec<VaultBackfillConfig>, anyhow::Error> {
    let assets = list_enabled_assets(pool).await?;

    if assets.is_empty() {
        info!("No enabled tokenized assets found, skipping receipt backfill");
        return Ok(vec![]);
    }

    info!(
        asset_count = assets.len(),
        "Running receipt backfill for all enabled assets"
    );

    stream::iter(assets.into_iter().filter_map(|asset| match asset {
        TokenizedAssetView::Asset { vault, underlying, .. } => {
            Some((vault, underlying))
        }
        TokenizedAssetView::Unavailable => None,
    }))
    .then(|(vault, underlying)| {
        let provider = provider.clone();
        async move {
            run_single_vault_backfill(
                &provider,
                vault,
                backfill_start_block,
                &underlying.0,
                receipt_inventory_cqrs,
                receipt_inventory_event_store,
                bot_wallet,
            )
            .await
        }
    })
    .try_collect()
    .await
}

/// Runs receipt backfill for a single vault.
async fn run_single_vault_backfill<P: Provider + Clone>(
    provider: &P,
    vault: Address,
    backfill_start_block: u64,
    underlying: &str,
    receipt_inventory_cqrs: &ReceiptInventoryCqrs,
    receipt_inventory_event_store: &ReceiptInventoryEventStore,
    bot_wallet: Address,
) -> Result<VaultBackfillConfig, anyhow::Error> {
    let vault_contract =
        bindings::OffchainAssetReceiptVault::new(vault, provider);
    let receipt_contract =
        Address::from(vault_contract.receipt().call().await?.0);

    let aggregate_context = receipt_inventory_event_store
        .load_aggregate(&vault.to_string())
        .await?;

    let from_block = aggregate_context
        .aggregate()
        .last_backfilled_block()
        .unwrap_or(backfill_start_block);

    info!(
        underlying,
        vault = %vault,
        receipt_contract = %receipt_contract,
        bot_wallet = %bot_wallet,
        from_block,
        "Running receipt backfill for vault"
    );

    let backfiller = ReceiptBackfiller::new(
        provider.clone(),
        receipt_contract,
        bot_wallet,
        vault,
        receipt_inventory_cqrs.clone(),
    );

    let result = backfiller.backfill_receipts(from_block).await?;

    info!(
        underlying,
        vault = %vault,
        processed = result.processed_count,
        skipped_zero_balance = result.skipped_zero_balance,
        "Receipt backfill complete for vault"
    );

    Ok(VaultBackfillConfig { vault, receipt_contract })
}

/// Runs transfer backfill for ALL enabled vaults.
///
/// Scans historic Transfer events to detect redemptions that occurred while the
/// service was down. Must run after receipt backfill (for burn planning) and
/// before live monitoring (to avoid missing transfers in the gap).
async fn run_all_transfer_backfills<P: Provider + Clone>(
    vault_configs: &[VaultBackfillConfig],
    backfiller: &TransferBackfiller<
        P,
        SqliteEventStore<Redemption>,
        SqliteEventStore<ReceiptInventory>,
    >,
    backfill_start_block: u64,
) -> Result<(), anyhow::Error> {
    if vault_configs.is_empty() {
        info!("No enabled vaults, skipping transfer backfill");
        return Ok(());
    }

    info!(
        vault_count = vault_configs.len(),
        "Running transfer backfill for all vaults"
    );

    for config in vault_configs {
        let result = backfiller
            .backfill_transfers(config.vault, backfill_start_block)
            .await?;

        debug!(
            vault = %config.vault,
            detected = result.detected_count,
            skipped_mint = result.skipped_mint,
            skipped_no_account = result.skipped_no_account,
            "Transfer backfill complete for vault"
        );
    }

    Ok(())
}

/// Spawns receipt monitors for ALL enabled vaults.
fn spawn_all_receipt_monitors<P, ITN>(
    provider: P,
    vault_configs: &[VaultBackfillConfig],
    receipt_inventory_cqrs: &ReceiptInventoryCqrs,
    bot_wallet: Address,
    handler: &ITN,
) where
    P: Provider + Clone + Send + Sync + 'static,
    ITN: ItnReceiptHandler + Clone + 'static,
{
    info!(
        vault_count = vault_configs.len(),
        "Spawning receipt monitors for all vaults"
    );

    for config in vault_configs {
        info!(
            vault = %config.vault,
            receipt_contract = %config.receipt_contract,
            bot_wallet = %bot_wallet,
            "Spawning receipt monitor for vault"
        );

        let monitor_config = ReceiptMonitorConfig {
            vault: config.vault,
            receipt_contract: config.receipt_contract,
            bot_wallet,
        };

        let monitor = ReceiptMonitor::new(
            provider.clone(),
            monitor_config,
            receipt_inventory_cqrs.clone(),
            handler.clone(),
        );

        tokio::spawn(async move {
            monitor.run().await;
        });
    }
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
