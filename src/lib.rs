use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use event_sorcery::{Store, StoreBuilder};
use futures::stream::{self, StreamExt, TryStreamExt};
use rocket::routes;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::{sync::Arc, time::Duration};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, trace, warn};

use crate::account::Account;
use crate::alpaca::AlpacaService;
use crate::auth::FailedAuthRateLimiter;
use crate::mint::{
    Mint, MintServices, MintView, find_all_recoverable_mints,
    recovery::{
        DriveOutcome, MintRecoveryHandler, recover_mint,
        spawn_scheduled_mint_recovery,
    },
    replay_mint_view,
};
use crate::receipt_inventory::backfill::{NoOpItnHandler, ReceiptBackfiller};
use crate::receipt_inventory::reconcile::run_startup_reconciliation;
use crate::receipt_inventory::{
    CqrsReceiptService, ItnReceiptHandler, ReceiptBurnsView, ReceiptInventory,
    ReceiptInventoryView, burn_tracking::replay_receipt_burns_view,
    replay_receipt_inventory_view,
};
use crate::redemption::{
    Redemption, RedemptionView,
    burn_manager::BurnManager,
    journal_manager::JournalManager,
    poller::{TransferPoller, TransferPollerConfig},
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

pub(crate) mod admin;
pub(crate) mod alpaca;
pub(crate) mod auth;
pub(crate) mod catchers;
pub(crate) mod config;
pub(crate) mod fireblocks;
pub(crate) mod poll_checkpoint;
pub mod receipt_inventory;
pub(crate) mod replay;
pub(crate) mod telemetry;
pub(crate) mod vault;

pub mod bindings;

pub use alpaca::AlpacaConfig;
pub use auth::{AuthConfig, IpWhitelist, IssuerApiKey};
pub use config::{Config, LogLevel, setup_tracing};
pub use fireblocks::SignerConfig;
pub use telemetry::TelemetryGuard;
pub use test_utils::ANVIL_CHAIN_ID;

pub(crate) type TokenizedAssetCqrs = SqliteCqrs<TokenizedAsset>;

pub(crate) type MintCqrs = Arc<SqliteCqrs<Mint>>;
pub(crate) type MintEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, Mint>>;

pub(crate) type RedemptionCqrs = Arc<SqliteCqrs<Redemption>>;
pub(crate) type RedemptionEventStore =
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
    burn: Arc<BurnManager<SqliteEventStore<Redemption>>>,
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

/// Interval between periodic receipt-inventory reconciliation passes.
///
/// Receipts are registered directly by the mint flow on completion (see
/// `register_minted_receipt`); this periodic backfill is a reconciliation
/// safety-net that catches any the direct path missed. Receipts only change
/// on mint/burn, and each pass issues several `eth_getLogs` per vault, so a
/// tight interval wastes RPC budget. 60s keeps recovery latency low while
/// cutting steady-state dRPC cost ~12x versus the previous 5s. Redemption
/// detection latency is unaffected — that is driven by the separate transfer
/// poller (see `redemption::poller`), which is unchanged.
pub(crate) const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(60);

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

    let BasicCqrsSetup { tokenized_asset_cqrs, tokenized_asset_view_repo } =
        setup_basic_cqrs(&pool);

    let (account_store, _account_projection) =
        StoreBuilder::<Account>::new(pool.clone()).build(()).await?;

    let blockchain_setup = config.create_blockchain_setup().await?;
    let alpaca_service = config.alpaca.service()?;
    let bot_wallet = config.signer.address().await?;
    info!(target: "startup", "Bot wallet address: {bot_wallet}");

    let vault_service_for_rocket = blockchain_setup.vault_service.clone();

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
    debug!(target: "startup", "Rebuilding all views from events");
    replay_tokenized_asset_view(pool.clone()).await?;
    replay_mint_view(pool.clone()).await?;
    replay_receipt_inventory_view(pool.clone()).await?;
    replay_redemption_view(pool.clone()).await?;
    replay_receipt_burns_view(pool.clone()).await?;

    // Receipt backfill must run before recovery so that recovery can check
    // receipt inventory to detect already-minted receipts (prevents double-mints).
    let vault_configs = run_all_receipt_backfills(
        &pool,
        blockchain_setup.http_provider.clone(),
        &receipt_inventory.cqrs,
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

    if let Err(error) = run_startup_reconciliation(
        blockchain_setup.http_provider.clone(),
        &vault_receipt_pairs,
        &receipt_inventory.cqrs,
        receipt_inventory.event_store.as_ref(),
        bot_wallet,
    )
    .await
    {
        error!(
            target: "receipt",
            error = %error,
            "Startup reconciliation failed — receipt balances may be stale \
             until the next withdraw event triggers reconciliation"
        );
    }

    // The synchronous recovery pass runs with a timeout before the HTTP server
    // starts. Mints that need deferred or pending follow-up are handed to
    // detached background scheduled-recovery tasks that intentionally outlive
    // this timeout and run concurrently with request handling; their safety
    // rests on cqrs-es optimistic concurrency and Fireblocks externalTxId
    // idempotency, not on completing before the server is up. If the
    // synchronous pass hangs it is cancelled and any remaining stuck aggregates
    // are left for manual admin intervention.
    let vaults: Vec<Address> =
        vault_configs.iter().map(|config| config.vault).collect();

    run_recovery_with_timeout(
        &pool,
        &mint.cqrs,
        &mint.event_store,
        &managers,
        &vaults,
    )
    .await;

    spawn_periodic_receipt_backfills(PeriodicBackfillSpawn {
        pool: pool.clone(),
        provider: blockchain_setup.http_provider.clone(),
        vault_configs: vault_configs.clone(),
        receipt_inventory_cqrs: receipt_inventory.cqrs.clone(),
        bot_wallet,
        backfill_start_block: config.backfill_start_block,
        receipt_poll_interval: config.receipt_poll_interval,
        handler: MintRecoveryHandler::new(mint.cqrs.clone()),
    });

    {
        info!(
            target: "redemption",
            vault_count = vaults.len(),
            "Spawning transfer poller (eth_getLogs across all vaults)"
        );

        let poller = TransferPoller::new(TransferPollerConfig {
            provider: blockchain_setup.http_provider,
            bot_wallet,
            vaults,
            backfill_start_block: config.backfill_start_block,
            cqrs: redemption_cqrs.clone(),
            event_store: redemption_event_store.clone(),
            pool: pool.clone(),
            redeem_call_manager: managers.redeem_call.clone(),
            journal_manager: managers.journal.clone(),
            burn_manager: managers.burn.clone(),
        });

        tokio::spawn(async move {
            poller.run().await;
        });
    }

    Ok(build_rocket(RocketState {
        rate_limiter: FailedAuthRateLimiter::new()?,
        config,
        pool,
        account_store,
        tokenized_asset_cqrs,
        tokenized_asset_view_repo,
        mint,
        redemption_cqrs,
        redemption_event_store,
        alpaca_service,
        burn_recovery: managers.burn.clone()
            as Arc<dyn admin::RedemptionBurnRecovery>,
        vault_service: vault_service_for_rocket,
    }))
}

struct RocketState {
    config: Config,
    rate_limiter: FailedAuthRateLimiter,
    pool: Pool<Sqlite>,
    account_store: Arc<Store<Account>>,
    tokenized_asset_cqrs: TokenizedAssetCqrs,
    tokenized_asset_view_repo: TokenizedAssetViewRepo,
    mint: MintDeps,
    redemption_cqrs: RedemptionCqrs,
    redemption_event_store: RedemptionEventStore,
    alpaca_service: Arc<dyn AlpacaService>,
    burn_recovery: Arc<dyn admin::RedemptionBurnRecovery>,
    vault_service: Arc<dyn vault::VaultService>,
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
        .manage(state.account_store)
        .manage(state.tokenized_asset_cqrs)
        .manage(state.tokenized_asset_view_repo)
        .manage(state.mint.cqrs)
        .manage(state.mint.event_store)
        .manage(state.redemption_cqrs)
        .manage(state.redemption_event_store)
        .manage(state.alpaca_service)
        .manage(state.burn_recovery)
        .manage(state.vault_service)
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
                mint::confirm_journal,
                admin::recover_redemption,
                admin::close_redemption,
                admin::force_complete_redemption,
                admin::reprocess_mint,
                admin::close_mint,
                admin::list_stuck,
                admin::check_fireblocks_tx,
            ],
        )
        .register("/", catchers::json_catchers())
}

struct BasicCqrsSetup {
    tokenized_asset_cqrs: TokenizedAssetCqrs,
    tokenized_asset_view_repo: TokenizedAssetViewRepo,
}

fn setup_basic_cqrs(pool: &Pool<Sqlite>) -> BasicCqrsSetup {
    let tokenized_asset_view_repo: TokenizedAssetViewRepo =
        Arc::new(SqliteViewRepository::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));
    let tokenized_asset_query =
        GenericQuery::new(tokenized_asset_view_repo.clone());
    let tokenized_asset_cqrs =
        sqlite_cqrs(pool.clone(), vec![Box::new(tokenized_asset_query)], ());

    BasicCqrsSetup { tokenized_asset_cqrs, tokenized_asset_view_repo }
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

    // ReceiptBurnsView tracks burns keyed by redemption aggregate_id.
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
        bot_wallet,
    ));

    Ok(RedemptionManagers { redeem_call, journal, burn })
}

async fn run_mint_recovery(
    pool: &Pool<Sqlite>,
    mint_cqrs: &MintCqrs,
    mint_event_store: &MintEventStore,
) {
    info!(target: "mint", "Running mint recovery");

    let recoverable_mints = match find_all_recoverable_mints(pool).await {
        Ok(mints) => mints,
        Err(err) => {
            error!(target: "mint", error = %err, "Failed to query recoverable mints");
            return;
        }
    };

    if recoverable_mints.is_empty() {
        debug!(target: "mint", "No mints to recover");
        return;
    }

    let count = recoverable_mints.len();
    debug!(target: "mint", count, "Recovering mints");

    for (issuer_request_id, _view) in recoverable_mints {
        // Drive one synchronous pass so mints that can finish immediately
        // (e.g. an on-chain receipt already exists) complete before the HTTP
        // server starts. If the pass does not reach a terminal/exhausted state
        // — waiting on a retry window, a pending Fireblocks tx, or a transient
        // failure — hand the mint to a background scheduled-recovery task so it
        // keeps progressing while the service runs instead of waiting for the
        // next restart.
        match recover_mint(mint_cqrs, issuer_request_id.clone()).await {
            DriveOutcome::RetryNotDue
            | DriveOutcome::Pending
            | DriveOutcome::Failed => {
                spawn_scheduled_mint_recovery(
                    mint_cqrs.clone(),
                    mint_event_store.clone(),
                    issuer_request_id,
                );
            }
            DriveOutcome::Done | DriveOutcome::Exhausted => {}
        }
    }

    debug!(target: "mint", count, "Mint recovery complete");
}

async fn run_redemption_recovery(
    redeem_call: &RedeemCallManager<
        PersistedEventStore<SqliteEventRepository, Redemption>,
    >,
    journal: &JournalManager<
        PersistedEventStore<SqliteEventRepository, Redemption>,
    >,
    burn: &BurnManager<PersistedEventStore<SqliteEventRepository, Redemption>>,
    vaults: &[Address],
) {
    info!(target: "redemption", "Running redemption recovery");

    redeem_call.recover_detected_redemptions().await;
    journal.recover_alpaca_called_redemptions().await;
    burn.recover_burning_redemptions().await;
    burn.recover_burn_failed_redemptions().await;
    // Runs last, after the recovery passes above have re-confirmed in-flight
    // burns. It only SETTLES reservations whose redemption confirmed
    // (Completed) but whose settlement was missed; ambiguous/in-flight
    // reservations are left untouched, since releasing a burn that may have
    // landed would risk a duplicate burn.
    burn.recover_stuck_reservations(vaults).await;
}

/// Maximum time to wait for recovery before starting the HTTP server.
///
/// Recovery runs as a background task with a timeout. If it completes within
/// this window, no race condition is possible (recovery is done before any
/// HTTP request arrives). If it hangs (e.g., Fireblocks call), the task is
/// **cancelled** — not left running — so no concurrent side effects can
/// race with incoming HTTP requests. Stuck aggregates that weren't recovered
/// in time require manual intervention via `/admin/recover` or `/admin/close`.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs mint and redemption recovery with a timeout, then starts the HTTP
/// server. Recovery that completes within [`RECOVERY_TIMEOUT`] runs to
/// completion before Rocket serves requests. Recovery that hangs is
/// cancelled to prevent concurrent side effects.
async fn run_recovery_with_timeout(
    pool: &Pool<Sqlite>,
    mint_cqrs: &MintCqrs,
    mint_event_store: &MintEventStore,
    managers: &RedemptionManagers,
    vaults: &[Address],
) {
    let recovery = async {
        run_mint_recovery(pool, mint_cqrs, mint_event_store).await;
        run_redemption_recovery(
            &managers.redeem_call,
            &managers.journal,
            &managers.burn,
            vaults,
        )
        .await;
    };

    match tokio::time::timeout(RECOVERY_TIMEOUT, recovery).await {
        Ok(()) => {
            info!(target: "startup", "Recovery complete");
        }
        Err(_) => {
            warn!(
                target: "startup",
                timeout_secs = RECOVERY_TIMEOUT.as_secs(),
                "Recovery timed out — remaining stuck aggregates require \
                 manual recovery via /admin/recover or /admin/close endpoints"
            );
        }
    }
}

/// Configuration for a single vault, extracted from TokenizedAssetView.
#[derive(Clone, Copy)]
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
    bot_wallet: Address,
    backfill_start_block: u64,
) -> Result<Vec<VaultBackfillConfig>, anyhow::Error> {
    let assets = list_enabled_assets(pool).await?;

    if assets.is_empty() {
        info!(target: "receipt", "No enabled tokenized assets found, skipping receipt backfill");
        return Ok(vec![]);
    }

    info!(
        target: "receipt",
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
                pool,
                &provider,
                vault,
                backfill_start_block,
                &underlying.0,
                receipt_inventory_cqrs,
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
    pool: &Pool<Sqlite>,
    provider: &P,
    vault: Address,
    backfill_start_block: u64,
    underlying: &str,
    receipt_inventory_cqrs: &ReceiptInventoryCqrs,
    bot_wallet: Address,
) -> Result<VaultBackfillConfig, anyhow::Error> {
    let vault_contract =
        bindings::OffchainAssetReceiptVault::new(vault, provider);
    let receipt_contract =
        Address::from(vault_contract.receipt().call().await?.0);

    let last_block = crate::poll_checkpoint::load(
        pool,
        &crate::poll_checkpoint::receipt_backfill_name(vault),
    )
    .await?;
    let from_block =
        next_receipt_backfill_block(last_block, backfill_start_block)?;

    info!(
        target: "receipt",
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
        pool.clone(),
        NoOpItnHandler,
    );

    let head_block = provider.get_block_number().await?;
    let result = backfiller.backfill_receipts(from_block, head_block).await?;

    info!(
        target: "receipt",
        underlying,
        vault = %vault,
        processed = result.processed_count,
        skipped_zero_balance = result.skipped_zero_balance,
        "Receipt backfill complete for vault"
    );

    Ok(VaultBackfillConfig { vault, receipt_contract })
}

fn next_receipt_backfill_block(
    last_backfilled_block: Option<u64>,
    backfill_start_block: u64,
) -> Result<u64, anyhow::Error> {
    last_backfilled_block.map_or(Ok(backfill_start_block), |block| {
        block.checked_add(1).map_or_else(
            || Err(anyhow::anyhow!("Receipt backfill checkpoint overflow")),
            |next_block| Ok(next_block.max(backfill_start_block)),
        )
    })
}

/// Dependencies shared by every vault in a single periodic receipt-backfill
/// pass. Constant across the pass; only `config` varies per vault. `head_block`
/// is fetched once per pass and passed separately to each per-vault call.
struct PeriodicBackfillCtx<'a, P, H> {
    pool: &'a Pool<Sqlite>,
    provider: &'a P,
    receipt_inventory_cqrs: &'a ReceiptInventoryCqrs,
    bot_wallet: Address,
    backfill_start_block: u64,
    handler: &'a H,
}

async fn run_periodic_receipt_backfill_for_config<P, H>(
    ctx: &PeriodicBackfillCtx<'_, P, H>,
    config: VaultBackfillConfig,
    head_block: u64,
) -> Result<(), anyhow::Error>
where
    P: Provider + Clone,
    H: ItnReceiptHandler,
{
    let last_block = crate::poll_checkpoint::load(
        ctx.pool,
        &crate::poll_checkpoint::receipt_backfill_name(config.vault),
    )
    .await?;
    let from_block =
        next_receipt_backfill_block(last_block, ctx.backfill_start_block)?;

    trace!(
        target: "receipt",
        vault = %config.vault,
        receipt_contract = %config.receipt_contract,
        from_block,
        "Running periodic receipt backfill for vault"
    );

    let backfiller = ReceiptBackfiller::new(
        ctx.provider.clone(),
        config.receipt_contract,
        ctx.bot_wallet,
        config.vault,
        ctx.receipt_inventory_cqrs.clone(),
        ctx.pool.clone(),
        ctx.handler,
    );

    let result = backfiller.backfill_receipts(from_block, head_block).await?;

    trace!(
        target: "receipt",
        vault = %config.vault,
        processed = result.processed_count,
        skipped_zero_balance = result.skipped_zero_balance,
        reconciled = result.reconciled_count,
        "Periodic receipt backfill complete for vault"
    );

    Ok(())
}

/// Owned dependencies needed to spawn the periodic receipt-backfill task.
/// Bundled to keep the spawn signature within argument limits; the spawned
/// task moves these in and borrows them into a `PeriodicBackfillCtx` per pass.
struct PeriodicBackfillSpawn<P, H> {
    pool: Pool<Sqlite>,
    provider: P,
    vault_configs: Vec<VaultBackfillConfig>,
    receipt_inventory_cqrs: ReceiptInventoryCqrs,
    bot_wallet: Address,
    backfill_start_block: u64,
    receipt_poll_interval: Duration,
    handler: H,
}

fn spawn_periodic_receipt_backfills<P, H>(spawn: PeriodicBackfillSpawn<P, H>)
where
    P: Provider + Clone + Send + Sync + 'static,
    H: ItnReceiptHandler + 'static,
{
    let PeriodicBackfillSpawn {
        pool,
        provider,
        vault_configs,
        receipt_inventory_cqrs,
        bot_wallet,
        backfill_start_block,
        receipt_poll_interval,
        handler,
    } = spawn;

    // With no vaults there is nothing to reconcile, so avoid spawning a task
    // that would fetch the chain head every interval for no work.
    if vault_configs.is_empty() {
        debug!(
            target: "receipt",
            "No vaults configured; skipping periodic receipt backfill"
        );
        return;
    }

    tokio::spawn(async move {
        let ctx = PeriodicBackfillCtx {
            pool: &pool,
            provider: &provider,
            receipt_inventory_cqrs: &receipt_inventory_cqrs,
            bot_wallet,
            backfill_start_block,
            handler: &handler,
        };

        let mut interval = tokio::time::interval(receipt_poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Skip the first tick (fires immediately) — the startup backfill
        // already ran during initialization, so we wait for the first
        // real interval before polling again.
        interval.tick().await;

        loop {
            interval.tick().await;

            // Fetch the chain head once per pass and reuse it for every vault.
            // The receipt backfill scans the same block range across all
            // vaults, so a single `eth_blockNumber` per pass replaces one call
            // per vault.
            let head_block = match provider.get_block_number().await {
                Ok(head_block) => head_block,
                Err(error) => {
                    warn!(
                        target: "receipt",
                        error = %error,
                        "Failed to fetch chain head; skipping this receipt \
                         backfill pass"
                    );
                    continue;
                }
            };

            for config in &vault_configs {
                if let Err(error) = run_periodic_receipt_backfill_for_config(
                    &ctx, *config, head_block,
                )
                .await
                {
                    warn!(
                        target: "receipt",
                        error = %error,
                        vault = %config.vault,
                        "Periodic receipt backfill failed; next run will resume \
                         from the last checkpoint"
                    );
                }
            }
        }
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

    use super::{
        Quantity, QuantityConversionError, next_receipt_backfill_block,
    };

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

    #[test]
    fn test_next_receipt_backfill_block_fails_on_overflow() {
        let result = next_receipt_backfill_block(Some(u64::MAX), 50);

        assert!(result.is_err());
    }

    #[test]
    fn test_next_receipt_backfill_block_uses_configured_start_without_checkpoint()
     {
        let start_block = next_receipt_backfill_block(None, 50).unwrap();

        assert_eq!(start_block, 50);
    }

    #[test]
    fn test_next_receipt_backfill_block_resumes_after_checkpoint() {
        let start_block = next_receipt_backfill_block(Some(80), 50).unwrap();

        assert_eq!(start_block, 81);
    }

    #[test]
    fn test_next_receipt_backfill_block_respects_configured_floor() {
        let start_block = next_receipt_backfill_block(Some(20), 50).unwrap();

        assert_eq!(start_block, 50);
    }
}
