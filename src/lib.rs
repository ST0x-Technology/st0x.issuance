use alloy::primitives::{Address, U256};
use apalis::prelude::{
    Monitor, State, Storage, WorkerBuilder, WorkerFactoryFn,
};
use apalis_sql::sqlite::SqliteStorage;
use cqrs_es::persist::{GenericQuery, PersistedEventStore};
use rocket::routes;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlite_es::{
    SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::fmt::Debug;
use std::sync::Arc;
use task_supervisor::SupervisorHandle;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

use crate::account::view::replay_account_view;
use crate::account::{Account, AccountView};
use crate::alpaca::AlpacaService;
use crate::auth::FailedAuthRateLimiter;
use crate::job::{Job, Label};
use crate::mint::{
    Mint, MintServices, MintView,
    recovery::{MintRecoveryCtx, MintRecoveryJob},
};
use crate::receipt_inventory::{
    CqrsReceiptService, ReceiptBurnsView, ReceiptInventory,
    ReceiptInventoryView,
    backfill::{ReceiptBackfillCtx, ReceiptBackfillJob},
};
use crate::redemption::{
    Redemption, RedemptionRecoveryJob, RedemptionView,
    backfill::{TransferBackfillCtx, TransferBackfillJob},
    burn_manager::BurnManager,
    journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
    upcaster::create_tokens_burned_upcaster,
};
use crate::tokenized_asset::{
    TokenizedAsset, TokenizedAssetViewRepo, discover_vaults,
};
use crate::view_replay::ViewReplayJob;

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
pub(crate) mod job;
pub mod receipt_inventory;
pub(crate) mod telemetry;
pub(crate) mod vault;
mod view_replay;

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

pub(crate) type RedemptionCqrs = Arc<SqliteCqrs<Redemption>>;
pub(crate) type RedemptionEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, Redemption>>;
pub(crate) type ReceiptInventoryCqrs = Arc<SqliteCqrs<ReceiptInventory>>;
pub(crate) type ReceiptInventoryEventStore =
    Arc<PersistedEventStore<SqliteEventRepository, ReceiptInventory>>;

pub(crate) type SqliteEventStore<A> =
    PersistedEventStore<SqliteEventRepository, A>;

/// A `JoinHandle` wrapper that aborts the task when dropped.
///
/// Ensures spawned background tasks (like the apalis monitor) are cleaned up
/// when the owning service is dropped, preventing orphaned tasks from
/// interfering with subsequent service instances sharing the same database.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Maximum time to wait for a single startup job before treating it as stuck.
const STARTUP_JOB_TIMEOUT: Duration = Duration::from_secs(300);

enum JobStatus {
    Completed,
    Pending,
}

fn check_job_status(
    label: &Label,
    context: &apalis_sql::context::SqlContext,
) -> Result<JobStatus, anyhow::Error> {
    match context.status() {
        State::Done => {
            info!(job = %label, "Startup job completed");
            Ok(JobStatus::Completed)
        }
        State::Failed => {
            let error_msg = context
                .last_error()
                .clone()
                .unwrap_or_else(|| "unknown error".to_string());
            Err(anyhow::anyhow!("Startup job '{label}' failed: {error_msg}"))
        }
        State::Killed => {
            Err(anyhow::anyhow!("Startup job '{label}' was killed"))
        }
        _ => Ok(JobStatus::Pending),
    }
}

/// Pushes a job to the queue and polls until it completes.
///
/// Returns `Ok(())` if the job reaches `Done` state, or `Err` if it fails,
/// is killed, or exceeds the timeout.
async fn push_and_await<J: Job>(
    job_queue: &mut SqliteStorage<J>,
    job: J,
) -> Result<(), anyhow::Error> {
    let label = job.label();
    debug!(job = %label, "Pushing startup job");
    let parts = job_queue.push(job).await?;
    let task_id = parts.task_id;
    debug!(job = %label, %task_id, "Job queued, waiting for completion");

    await_task(job_queue, &label, &task_id).await
}

/// Pushes multiple jobs to the queue and polls until all complete.
///
/// All jobs are pushed first (so the worker can start processing them
/// concurrently), then polled together in a single loop.
async fn push_all_and_await<J: Job>(
    job_queue: &mut SqliteStorage<J>,
    jobs: impl IntoIterator<Item = J>,
) -> Result<(), anyhow::Error> {
    let mut pending: Vec<(Label, apalis::prelude::TaskId)> = Vec::new();

    for job in jobs {
        let label = job.label();
        debug!(job = %label, "Pushing startup job");
        let parts = job_queue.push(job).await?;
        debug!(job = %label, task_id = %parts.task_id, "Job queued");
        pending.push((label, parts.task_id));
    }

    if pending.is_empty() {
        return Ok(());
    }

    info!(count = pending.len(), "Waiting for startup jobs to complete");

    let deadline = tokio::time::Instant::now() + STARTUP_JOB_TIMEOUT;

    while !pending.is_empty() {
        sleep(Duration::from_millis(100)).await;

        if tokio::time::Instant::now() > deadline {
            let still_pending: Vec<_> =
                pending.iter().map(|(label, _)| label.to_string()).collect();
            return Err(anyhow::anyhow!(
                "Startup jobs timed out after {}s: {}",
                STARTUP_JOB_TIMEOUT.as_secs(),
                still_pending.join(", ")
            ));
        }

        let mut still_pending = Vec::new();

        for (label, task_id) in pending {
            let Some(request) = job_queue.fetch_by_id(&task_id).await? else {
                still_pending.push((label, task_id));
                continue;
            };

            match check_job_status(&label, &request.parts.context)? {
                JobStatus::Completed => {}
                JobStatus::Pending => {
                    still_pending.push((label, task_id));
                }
            }
        }

        pending = still_pending;
    }

    Ok(())
}

async fn await_task<J: Job>(
    job_queue: &mut SqliteStorage<J>,
    label: &Label,
    task_id: &apalis::prelude::TaskId,
) -> Result<(), anyhow::Error> {
    let deadline = tokio::time::Instant::now() + STARTUP_JOB_TIMEOUT;

    loop {
        sleep(Duration::from_millis(100)).await;

        if tokio::time::Instant::now() > deadline {
            return Err(anyhow::anyhow!(
                "Startup job '{label}' timed out after {}s",
                STARTUP_JOB_TIMEOUT.as_secs()
            ));
        }

        let Some(request) = job_queue.fetch_by_id(task_id).await? else {
            continue;
        };

        if matches!(
            check_job_status(label, &request.parts.context)?,
            JobStatus::Completed
        ) {
            return Ok(());
        }
    }
}

struct AggregateCqrsCtx {
    mint: MintCtx,
    redemption_cqrs: RedemptionCqrs,
    redemption_event_store: RedemptionEventStore,
    receipt_inventory: ReceiptInventoryCtx,
}

struct ReceiptInventoryCtx {
    cqrs: ReceiptInventoryCqrs,
    event_store: ReceiptInventoryEventStore,
}

struct MintCtx {
    cqrs: MintCqrs,
    event_store: MintEventStore,
}

#[derive(Clone)]
pub(crate) struct RedemptionManagers {
    pub(crate) redeem_call:
        Arc<RedeemCallManager<SqliteEventStore<Redemption>>>,
    pub(crate) journal: Arc<JournalManager<SqliteEventStore<Redemption>>>,
    pub(crate) burn: Arc<BurnManager<SqliteEventStore<Redemption>>>,
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

    // Both apalis and our code use sqlx migrations, which share a single
    // `_sqlx_migrations` table. Each migrator validates that all previously
    // applied migrations exist in its own migration set — so running either
    // migrator after the other will fail with `VersionMissing` unless both
    // use `set_ignore_missing(true)`.
    //
    // We cannot call `SqliteStorage::setup()` because it runs apalis
    // migrations without `ignore_missing`. Instead, we get the apalis
    // migrator directly and run both with `ignore_missing`.
    sqlx::query("PRAGMA journal_mode = 'WAL'").execute(&pool).await?;
    sqlx::query("PRAGMA busy_timeout = 5000").execute(&pool).await?;

    apalis_sql::sqlite::SqliteStorage::<()>::migrations()
        .set_ignore_missing(true)
        .run(&pool)
        .await?;

    sqlx::migrate!("./migrations").set_ignore_missing(true).run(&pool).await?;

    let BasicCqrsSetup { account_cqrs, tokenized_asset_view_repo } =
        setup_basic_cqrs(&pool);

    let blockchain_setup = config.create_blockchain_setup().await?;
    let alpaca_service = config.alpaca.service()?;
    let bot_wallet = config.signer.address().await?;
    info!("Bot wallet address: {bot_wallet}");

    let AggregateCqrsCtx {
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

    let receipt_backfill_ctx = Arc::new(ReceiptBackfillCtx {
        provider: blockchain_setup.provider.clone(),
        receipt_inventory_cqrs: receipt_inventory.cqrs.clone(),
        receipt_inventory_event_store: receipt_inventory.event_store.clone(),
        bot_wallet,
        backfill_start_block: config.backfill_start_block,
    });

    let transfer_backfill_ctx = Arc::new(TransferBackfillCtx {
        provider: blockchain_setup.provider.clone(),
        bot_wallet,
        cqrs: redemption_cqrs.clone(),
        event_store: redemption_event_store.clone(),
        pool: pool.clone(),
        managers: RedemptionManagers {
            redeem_call: managers.redeem_call.clone(),
            journal: managers.journal.clone(),
            burn: managers.burn.clone(),
        },
        backfill_start_block: config.backfill_start_block,
    });

    let mut view_replay_job_queue =
        SqliteStorage::<ViewReplayJob>::new(pool.clone());
    let mut receipt_backfill_job_queue =
        SqliteStorage::<ReceiptBackfillJob>::new(pool.clone());
    let mut transfer_backfill_job_queue =
        SqliteStorage::<TransferBackfillJob>::new(pool.clone());
    let mut mint_recovery_job_queue =
        SqliteStorage::<MintRecoveryJob>::new(pool.clone());
    let mut redemption_recovery_job_queue =
        SqliteStorage::<RedemptionRecoveryJob>::new(pool.clone());

    let monitor = Monitor::new()
        .register(
            WorkerBuilder::new("view-replay")
                .data(pool.clone())
                .backend(view_replay_job_queue.clone())
                .build_fn(ViewReplayJob::run),
        )
        .register(
            WorkerBuilder::new("receipt-backfill")
                .data(receipt_backfill_ctx)
                .backend(receipt_backfill_job_queue.clone())
                .build_fn(ReceiptBackfillJob::run),
        )
        .register(
            WorkerBuilder::new("transfer-backfill")
                .data(transfer_backfill_ctx)
                .backend(transfer_backfill_job_queue.clone())
                .build_fn(TransferBackfillJob::run),
        )
        .register(
            WorkerBuilder::new("mint-recovery")
                .data(MintRecoveryCtx {
                    pool: pool.clone(),
                    mint_cqrs: mint.cqrs.clone(),
                })
                .backend(mint_recovery_job_queue.clone())
                .build_fn(MintRecoveryJob::run),
        )
        .register(
            WorkerBuilder::new("redemption-recovery")
                .data(Arc::new(managers.clone()))
                .backend(redemption_recovery_job_queue.clone())
                .build_fn(RedemptionRecoveryJob::run),
        );

    let mut monitor_handle = tokio::spawn(async move { monitor.run().await });

    let startup_jobs = async {
        // View replay — rebuild read models before recovery queries them
        push_and_await(&mut view_replay_job_queue, ViewReplayJob).await?;

        // Discover vaults and run per-vault backfills
        let vault_ctxs =
            discover_vaults(&pool, &blockchain_setup.provider).await?;

        for vault_ctx in &vault_ctxs {
            push_and_await(
                &mut receipt_backfill_job_queue,
                ReceiptBackfillJob {
                    vault: vault_ctx.vault,
                    receipt_contract: vault_ctx.receipt_contract,
                },
            )
            .await?;
        }

        for vault_ctx in &vault_ctxs {
            push_and_await(
                &mut transfer_backfill_job_queue,
                TransferBackfillJob { vault: vault_ctx.vault },
            )
            .await?;
        }

        // Recovery — runs after backfills for accurate state
        push_and_await(&mut mint_recovery_job_queue, MintRecoveryJob).await?;

        push_all_and_await(
            &mut redemption_recovery_job_queue,
            vault_ctxs.iter().map(|vault_ctx| RedemptionRecoveryJob {
                underlying: vault_ctx.underlying.clone(),
            }),
        )
        .await?;

        Ok::<_, anyhow::Error>(vault_ctxs)
    };

    let vault_ctxs = tokio::select! {
        result = &mut monitor_handle => {
            match result {
                Ok(Err(err)) => return Err(anyhow::anyhow!(
                    "Startup job monitor failed: {err}"
                )),
                Ok(Ok(())) => return Err(anyhow::anyhow!(
                    "Startup job monitor exited unexpectedly during startup"
                )),
                Err(err) => return Err(anyhow::anyhow!(
                    "Startup job monitor panicked: {err}"
                )),
            }
        }
        result = startup_jobs => result?,
    };

    let apalis_handle = AbortOnDrop(monitor_handle);

    let (monitor_query, supervisor_handle) =
        tokenized_asset::monitor_query::TokenizedAssetMonitorQuery::new(
            tokenized_asset::monitor_query::MonitorQuerySetup {
                rpc_url: config.rpc_url.clone(),
                pool: pool.clone(),
                bot_wallet,
                vault_ctxs: &vault_ctxs,
                provider: blockchain_setup.provider,
                mint_cqrs: &mint.cqrs,
                receipt_inventory_cqrs: receipt_inventory.cqrs,
                redemption_cqrs: redemption_cqrs.clone(),
                redemption_event_store,
                managers,
                receipt_backfill_job_queue,
                transfer_backfill_job_queue,
            },
        );

    let tokenized_asset_query =
        GenericQuery::new(tokenized_asset_view_repo.clone());
    let tokenized_asset_cqrs = sqlite_cqrs(
        pool.clone(),
        vec![Box::new(tokenized_asset_query), Box::new(monitor_query)],
        (),
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
        supervisor_handle,
        apalis_handle,
    }))
}

struct RocketState {
    config: Config,
    rate_limiter: FailedAuthRateLimiter,
    pool: Pool<Sqlite>,
    account_cqrs: AccountCqrs,
    tokenized_asset_cqrs: TokenizedAssetCqrs,
    tokenized_asset_view_repo: TokenizedAssetViewRepo,
    mint: MintCtx,
    redemption_cqrs: RedemptionCqrs,
    supervisor_handle: SupervisorHandle,
    apalis_handle: AbortOnDrop<Result<(), std::io::Error>>,
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
        .manage(state.supervisor_handle)
        .manage(state.apalis_handle)
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

    BasicCqrsSetup { account_cqrs, tokenized_asset_view_repo }
}

fn setup_aggregate_cqrs(
    pool: &Pool<Sqlite>,
    vault_service: Arc<dyn vault::VaultService>,
    alpaca_service: Arc<dyn AlpacaService>,
    bot_wallet: Address,
) -> AggregateCqrsCtx {
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

    AggregateCqrsCtx {
        mint: MintCtx { cqrs: mint_cqrs, event_store: mint_event_store },
        redemption_cqrs,
        redemption_event_store,
        receipt_inventory: ReceiptInventoryCtx {
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
    receipt_inventory: &ReceiptInventoryCtx,
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
