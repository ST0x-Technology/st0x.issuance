use alloy::primitives::Address;
use apalis::prelude::AbortError;
use apalis_sqlite::SqlitePool;
use async_trait::async_trait;
use chrono::Utc;
use event_sorcery::{SendError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::job::{ConfirmMintJob, SendCallbackJob, SubmitMintJob};
use super::{
    AutomaticRetryDecision, IssuerMintRequestId, Mint, MintCommand, Network,
    UnderlyingSymbol, find_all_recoverable_mints,
};
use crate::jobs::{Job, JobQueue, QueuePushError, job_type};
use crate::receipt_inventory::{ItnReceiptHandler, ReceiptService};
use crate::tokenized_asset::view::{TokenizedAssetViewError, find_vault};
use crate::vault::{NetworkVaultServices, TxId, UnconfiguredNetworkError};

/// Dependencies the scheduled mint-recovery worker needs to re-drive a stuck or
/// failed mint by enqueuing the appropriate per-state job.
pub(crate) struct MintRecoveryContext {
    pub(crate) mint_store: Arc<Store<Mint>>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) vault_services: NetworkVaultServices,
    pub(crate) receipts: Arc<dyn ReceiptService>,
    pub(crate) submit_queue: JobQueue<SubmitMintJob>,
    pub(crate) confirm_queue: JobQueue<ConfirmMintJob>,
    pub(crate) callback_queue: JobQueue<SendCallbackJob>,
}

/// Production handler that triggers mint recovery when an ITN receipt is
/// discovered by the receipt monitor.
///
/// Enqueues the scheduled recovery job for faster pickup than the periodic
/// reconciler. On-chain evidence (`tx_hash`, receipt id, shares) is already
/// in inventory from the preceding `DiscoverReceipt`; the recovery /
/// `SubmitMintJob` path loads that receipt and records it via
/// `RecordExistingMint` (or re-submits only when no receipt exists — the
/// signer does not dedup on `external_tx_id`).
#[derive(Clone)]
pub(crate) struct MintRecoveryHandler {
    pool: Pool<Sqlite>,
    apalis_pool: SqlitePool,
}

impl MintRecoveryHandler {
    pub(crate) const fn new(
        pool: Pool<Sqlite>,
        apalis_pool: SqlitePool,
    ) -> Self {
        Self { pool, apalis_pool }
    }
}

#[async_trait]
impl ItnReceiptHandler for MintRecoveryHandler {
    async fn on_itn_receipt_discovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) {
        if let Err(error) = enqueue_scheduled_mint_recovery(
            &self.pool,
            &self.apalis_pool,
            issuer_request_id.clone(),
        )
        .await
        {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %error,
                "Failed to enqueue recovery for a discovered receipt"
            );
        }
    }
}

/// Fixed backoff applied when a scheduled recovery pass cannot make progress —
/// a transient error (e.g. RPC blip) occurred. Keeps the loop from spinning while waiting.
const SCHEDULED_RECOVERY_BACKOFF: Duration = Duration::from_secs(60);

/// Budget for consecutive transient-failure backoffs (e.g. RPC blips) before
/// giving up. Small: a persistent error should surface for investigation, not
/// be hammered indefinitely. The next restart re-picks the mint.
const MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS: usize = 8;

/// Budget for recovery polls that observe no state change. A slow-but-healthy
/// step (e.g. a submitted tx awaiting confirmation) is not something we
/// control, so this is sized generously — ~6h at
/// [`SCHEDULED_RECOVERY_BACKOFF`] — separate from the transient-failure
/// budget, so slow finalization is not abandoned prematurely.
const MAX_SCHEDULED_RECOVERY_NO_PROGRESS_POLLS: usize = 360;

/// Durable apalis job that resumes one mint's automatic recovery.
///
/// Replaces the old fire-and-forget `tokio::spawn`: the job row persists in the
/// apalis `Jobs` table, so a crash mid-recovery no longer drops the work. A job
/// left `Running` by a crash is not re-fetched directly (apalis's `fetch_next`
/// only picks `Pending`/`Failed` rows); instead apalis's orphan-reenqueue resets
/// it to `Pending` once the dead worker's heartbeat goes stale
/// (`reenqueue_orphaned_after`, default 300s) — which works only because each
/// process registers a unique worker id, so a restart never refreshes the dead
/// worker's `last_seen`. On a full process restart the synchronous startup
/// re-scan also drives any still-recoverable mint directly, independent of the
/// queue row. The job body is the unchanged
/// [`recover_mint_until_automatic_budget_exhausted`] budget loop, which is
/// idempotent (recovery commands the aggregate guards against double-applying),
/// so re-running is safe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MintRecoveryJob {
    issuer_request_id: IssuerMintRequestId,
}

impl Job<MintRecoveryContext> for MintRecoveryJob {
    type Output = ();
    type Error = AbortError;

    async fn perform(
        &self,
        ctx: &MintRecoveryContext,
    ) -> Result<(), AbortError> {
        match recover_mint_until_automatic_budget_exhausted(
            ctx,
            &self.issuer_request_id,
            SCHEDULED_RECOVERY_BACKOFF,
            MAX_SCHEDULED_RECOVERY_NO_PROGRESS_POLLS,
        )
        .await
        {
            RecoveryConclusion::Resolved => Ok(()),
            // The mint is still incomplete. Return `AbortError` so apalis records
            // the failure in `last_result` and marks the job `Killed` — not a
            // clean `Done` that hides a stuck mint, and not a retryable `Failed`
            // that would re-run the whole budget loop up to `max_attempts`. The
            // startup re-scan re-attempts it; the ERROR surfaces the stuck mint.
            RecoveryConclusion::Abandoned { reason } => {
                error!(target: "mint", issuer_request_id = %self.issuer_request_id,
                    reason = %reason,
                    "Scheduled mint recovery abandoned the mint while still incomplete"
                );

                Err(AbortError::new(format!(
                    "mint recovery abandoned: {reason}"
                )))
            }
        }
    }
}

/// A unique apalis worker id for one [`MintRecoveryJob`] worker registration.
///
/// A FRESH id per registration is load-bearing for crash recovery: apalis
/// re-enqueues a job orphaned in `Running` only once its locking worker's
/// heartbeat goes stale, so a constant id would let each restart — including the
/// in-process restart loop in `spawn_mint_recovery_worker` — refresh the dead
/// worker's `last_seen` and strand the orphaned job. The random `uuid` suffix
/// guarantees a registration never reuses a crashed worker's id, so
/// `reenqueue_orphaned` reclaims its jobs once the dead worker's heartbeat goes
/// stale (`reenqueue_orphaned_after`).
pub(crate) struct MintRecoveryWorkerId(Uuid);

impl MintRecoveryWorkerId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for MintRecoveryWorkerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "mint-recovery-{}", self.0)
    }
}

/// The `job_type` apalis-sqlite assigns to [`MintRecoveryJob`]: `SqliteStorage`
/// derives it from `std::any::type_name` of the task type (`SqliteStorage::new`
/// → `Config::new(type_name::<T>())`) and binds that on both push and fetch.
/// Terminal-job cleanup scopes its `DELETE`s to exactly this value so it never
/// reaps rows belonging to other apalis job types that share the `Jobs` table.
/// Deriving it the same way apalis does (rather than hardcoding the path) keeps
/// it in lockstep with whatever string apalis actually stores.
///
/// WARNING: renaming or moving [`MintRecoveryJob`] (or renaming this crate)
/// changes `type_name`'s output but NOT the `job_type` already persisted in the
/// `Jobs` table, so terminal-job cleanup would silently stop matching old rows.
/// [`pushed_job_type_matches_cleanup_scope`] catches the mismatch at test time;
/// fixing it then requires a data migration to update `Jobs.job_type` for
/// existing rows.
fn mint_recovery_job_type() -> &'static str {
    std::any::type_name::<MintRecoveryJob>()
}

/// How many times to attempt enqueuing a recovery job before giving up. The
/// apalis write can transiently fail when its pool cannot win the single-writer
/// SQLite lock within `busy_timeout`; a few bounded retries ride that out rather
/// than dropping the mint's automatic recovery until the next restart.
const ENQUEUE_ATTEMPTS: usize = 3;

/// Backoff between enqueue attempts.
const ENQUEUE_BACKOFF: Duration = Duration::from_millis(200);

/// Enqueues a scheduled recovery job for `issuer_request_id`. The worker
/// registered in `initialize_rocket` drains the queue and drives the mint to a
/// terminal or budget-exhausted state.
///
/// First releases this mint's idempotency key from any TERMINAL prior job
/// (`Done`/`Killed`, or an exhausted `Failed`) so a live re-trigger — an admin
/// reprocess of an already-abandoned mint, the startup re-scan, a receipt-driven
/// retry — actually re-enqueues. A still-ACTIVE (`Pending`/`Running`) job for
/// the mint is left in place, so the insert collapses against it via apalis's
/// `ON CONFLICT(job_type, idempotency_key) DO NOTHING` rather than queuing a
/// duplicate. Without the release a terminal row would hold the key until the
/// next restart's vacuum and silently drop the re-enqueue (the conflict is not
/// an error), stranding the mint while reporting success.
///
/// `pool` is the event-store (sqlx 0.9) pool; the release runs there because
/// both pools address the same SQLite file and apalis-sqlite exposes no query
/// API on its own (sqlx 0.8) pool here.
pub(crate) async fn enqueue_scheduled_mint_recovery(
    pool: &Pool<Sqlite>,
    apalis_pool: &SqlitePool,
    issuer_request_id: IssuerMintRequestId,
) -> Result<(), anyhow::Error> {
    release_terminal_recovery_job(pool, &issuer_request_id).await?;
    push_mint_recovery_job(apalis_pool, issuer_request_id).await
}

/// Pushes a [`MintRecoveryJob`] for the mint, retrying transient enqueue
/// failures with a bounded backoff. The idempotency key makes the insert a
/// no-op when ANY job already exists for the mint — including a TERMINAL
/// (`Killed`/`Done`) one — via apalis's
/// `ON CONFLICT(job_type, idempotency_key) DO NOTHING`.
///
/// This is the half of [`enqueue_scheduled_mint_recovery`] AFTER the terminal
/// release. Callers that want to re-enqueue an already-abandoned mint go through
/// `enqueue_scheduled_mint_recovery` (which frees the terminal key first); the
/// periodic reconciler calls this directly so a mint that merely lost its job is
/// re-enqueued while an abandoned (`Killed`) mint dedups against its terminal row
/// instead of being retried every pass.
pub(crate) async fn push_mint_recovery_job(
    apalis_pool: &SqlitePool,
    issuer_request_id: IssuerMintRequestId,
) -> Result<(), anyhow::Error> {
    let mut attempt = 0;
    // The queue handle is reusable across attempts
    // (`push_with_idempotency_key` takes `&mut self`); build it once rather
    // than reconstructing it on every retry.
    let mut queue = JobQueue::<MintRecoveryJob>::new(apalis_pool);

    loop {
        attempt += 1;

        match queue
            .push_with_idempotency_key(
                MintRecoveryJob {
                    issuer_request_id: issuer_request_id.clone(),
                },
                issuer_request_id.to_string(),
            )
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) if attempt < ENQUEUE_ATTEMPTS => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    attempt, error = %error,
                    "Failed to enqueue scheduled mint recovery; retrying after backoff"
                );
                tokio::time::sleep(ENQUEUE_BACKOFF).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Deletes a TERMINAL apalis job row for one mint and one `job_type` so its
/// idempotency key is free to re-enqueue, leaving any active (`Pending`/
/// `Running`) row so the re-enqueue still dedups against it. Runs on the
/// event-store pool (same SQLite file as the apalis pool).
async fn release_terminal_job(
    pool: &Pool<Sqlite>,
    job_type: &str,
    idempotency_key: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        DELETE FROM Jobs
        WHERE
            job_type = ?
            AND idempotency_key = ?
            AND (
                status IN ('Done', 'Killed')
                OR (status = 'Failed' AND max_attempts <= attempts)
            )
        ",
    )
    .bind(job_type)
    .bind(idempotency_key)
    .execute(pool)
    .await?;

    Ok(())
}

/// Frees a mint's [`MintRecoveryJob`] idempotency key from any terminal prior
/// job before re-enqueuing it (see [`release_terminal_job`]).
async fn release_terminal_recovery_job(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<(), sqlx::Error> {
    release_terminal_job(
        pool,
        mint_recovery_job_type(),
        &issuer_request_id.to_string(),
    )
    .await
}

/// Deletes terminal apalis recovery jobs (`Done`/`Killed`, and `Failed` rows
/// that exhausted their attempts) to reclaim the `Jobs` table at startup.
///
/// apalis only ever UPDATEs a job's status to a terminal value — it never
/// deletes, and its `vacuum()` is a manual call we otherwise never make — so
/// without this every restart's re-scan would leave another terminal row behind
/// forever. This runs once at startup; within a single long-running process,
/// [`release_terminal_recovery_job`] reaps a mint's terminal row whenever it is
/// re-enqueued, so the only rows that linger between restarts belong to mints
/// that concluded and are never re-triggered (bounded by recovery volume, which
/// is low — only failed mints ever enqueue). Runs on the event-store pool
/// because both pools address the
/// same SQLite file; it must run BEFORE the recovery re-scan so a still-stuck
/// mint's idempotency key is free and it can be re-enqueued. Only terminal rows
/// are removed, so orphaned `Pending`/`Running` jobs that apalis will re-pick
/// are left untouched, and the delete is scoped to [`mint_recovery_job_type`]
/// so terminal rows of any other apalis job type sharing the `Jobs` table
/// survive.
///
/// The exhausted-`Failed` clause mirrors apalis-sqlite's own `vacuum.sql`:
/// apalis marks an out-of-attempts job `Killed` (already covered above), so the
/// `Failed`-at-exhaustion case is a defensive guard against apalis status-model
/// drift, not a state the current version reaches.
pub(crate) async fn vacuum_terminal_recovery_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        DELETE FROM Jobs
        WHERE
            job_type = ?
            AND (
                status IN ('Done', 'Killed')
                OR (status = 'Failed' AND max_attempts <= attempts)
            )
        ",
    )
    .bind(mint_recovery_job_type())
    .execute(pool)
    .await?;

    Ok(())
}

/// Flips mint-recovery jobs left `Running` by a dead process back to `Pending`,
/// clearing their lock columns (`lock_at`, `lock_by`).
///
/// At startup no worker from this process is running yet, so any `Running` row
/// is an orphan from the previous process; without this reset a crashed-mid-run
/// recovery job blocks its mint until apalis's orphan re-enqueue timeout
/// (`reenqueue_orphaned_after`, default ~300s). Scoped to
/// [`mint_recovery_job_type`] so `Running` rows of other apalis job types
/// sharing the `Jobs` table are left for their own recovery. Runs on the
/// event-store pool because both pools address the same SQLite file (see
/// [`vacuum_terminal_recovery_jobs`]).
pub(crate) async fn reset_orphaned_recovery_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE Jobs
        SET
            status = 'Pending',
            lock_at = NULL,
            lock_by = NULL
        WHERE
            job_type = ?
            AND status = 'Running'
        ",
    )
    .bind(mint_recovery_job_type())
    .execute(pool)
    .await?;

    Ok(())
}

/// Deletes terminal apalis rows for the per-state mint side-effect jobs
/// (`SubmitMintJob` / `ConfirmMintJob` / `SendCallbackJob`), mirroring
/// [`vacuum_terminal_recovery_jobs`] for the recovery job. Two purposes: it
/// bounds the `Jobs` table across restarts, and it frees the per-state
/// idempotency keys (which are scoped to the mint's `issuer_request_id`) that a
/// completed normal-flow job would otherwise hold — so a restart-driven recovery
/// of a rolled-back mint can re-enqueue its side-effect jobs rather than collide
/// with the prior run's `Done` row. Only terminal rows are removed, so an
/// orphaned `Pending`/`Running` job apalis will re-pick is left untouched. Runs
/// on the event-store pool because both pools address the same SQLite file.
pub(crate) async fn vacuum_terminal_mint_side_effect_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    for side_effect_job_type in [
        job_type::<SubmitMintJob>(),
        job_type::<ConfirmMintJob>(),
        job_type::<SendCallbackJob>(),
    ] {
        sqlx::query(
            "
            DELETE FROM Jobs
            WHERE
                job_type = ?
                AND (
                    status IN ('Done', 'Killed')
                    OR (status = 'Failed' AND max_attempts <= attempts)
                )
            ",
        )
        .bind(side_effect_job_type)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Flips submit/confirm/callback jobs left `Running` by a dead process back to
/// `Pending`, clearing their lock columns. Mirrors
/// [`reset_orphaned_recovery_jobs`] for the per-state side-effect chain so a
/// crash mid-job does not strand recovery behind apalis's orphan timeout
/// (re-enqueue would otherwise dedupe against the orphaned `Running` row).
pub(crate) async fn reset_orphaned_mint_side_effect_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    for side_effect_job_type in [
        job_type::<SubmitMintJob>(),
        job_type::<ConfirmMintJob>(),
        job_type::<SendCallbackJob>(),
    ] {
        sqlx::query(
            "
            UPDATE Jobs
            SET
                status = 'Pending',
                lock_at = NULL,
                lock_by = NULL
            WHERE
                job_type = ?
                AND status = 'Running'
            ",
        )
        .bind(side_effect_job_type)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Deletes `Workers` rows for the mint-recovery worker type that no job
/// references via `Jobs.lock_by`, so the table stays bounded across restarts.
///
/// Every registration uses a fresh unique id (see [`MintRecoveryWorkerId`]),
/// and apalis only ever INSERTs into `Workers` — so without this cleanup each
/// restart (and each in-process worker restart) leaves another dead row behind
/// forever. apalis-sqlite stores the worker's queue —
/// `Config::new(type_name::<T>())`, the same string as
/// [`mint_recovery_job_type`] — in `Workers.worker_type`
/// (apalis-sqlite `register_worker.rs` binds `config.queue()` as the
/// `worker_type` column), so the delete is scoped to exactly our worker rows.
/// Rows still referenced by a job's `lock_by` are kept, preserving the
/// heartbeat trail apalis's orphan re-enqueue relies on.
pub(crate) async fn prune_unreferenced_recovery_workers(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        DELETE FROM Workers
        WHERE
            worker_type = ?
            AND id NOT IN (
                SELECT lock_by
                FROM Jobs
                WHERE lock_by IS NOT NULL
            )
        ",
    )
    .bind(mint_recovery_job_type())
    .execute(pool)
    .await?;

    Ok(())
}

/// Ensures every currently-recoverable mint has a recovery job. Pushes a job for
/// EVERY recoverable mint and leans on apalis's
/// `ON CONFLICT(job_type, idempotency_key) DO NOTHING` to dedup — the insert is
/// a silent no-op for any mint that already has a job row (`Pending`, `Running`,
/// `Done`, or `Killed`), so only a mint that genuinely lost its job gets a fresh
/// one. Unlike the startup re-scan (`run_mint_recovery` in `lib.rs`) this
/// neither vacuums nor drives synchronously and — crucially — pushes WITHOUT
/// releasing terminal jobs ([`push_mint_recovery_job`]), so a mint whose
/// recovery was deliberately abandoned (`Killed`) dedups against its terminal
/// row rather than being retried every pass; the worker drives the jobs this
/// re-enqueues.
pub(crate) async fn reconcile_recoverable_mints(
    pool: &Pool<Sqlite>,
    apalis_pool: &SqlitePool,
) {
    let recoverable_mints = match find_all_recoverable_mints(pool).await {
        Ok(mints) => mints,
        Err(err) => {
            // Degraded but self-recovering (the reconciler retries in
            // MINT_RECOVERY_RECONCILE_INTERVAL), so WARN, not ERROR — an
            // ERROR here would raise a false unrecoverable alert for a
            // transient, self-healing miss.
            warn!(target: "mint", error = %err,
                "Failed to query recoverable mints during reconcile"
            );
            return;
        }
    };

    if recoverable_mints.is_empty() {
        return;
    }

    let count = recoverable_mints.len();
    for (issuer_request_id, _view) in recoverable_mints {
        if let Err(error) =
            push_mint_recovery_job(apalis_pool, issuer_request_id.clone()).await
        {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %error,
                "Failed to re-enqueue recoverable mint during reconcile"
            );
        }
    }

    debug!(target: "mint", recoverable_mints = count,
        "Reconcile pass pushed an idempotent recovery job for each recoverable \
         mint; pushes for mints that already have a Pending, Running, Done, or \
         Killed job are silent no-ops"
    );
}

/// Why [`recover_mint_until_automatic_budget_exhausted`] abandoned a mint while
/// it was still incomplete. A closed set of causes, so callers match on the
/// variant rather than comparing free-text strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbandonReason {
    /// The mint could not be loaded after the maximum load-failure backoffs.
    FailedToLoadMint,
    /// Receipt inventory remained unreadable after transient backoffs.
    FailedToLoadReceipt,
    /// The aggregate's automatic-retry attempts ran out.
    AutomaticRetriesExhausted,
    /// The mint stayed in the same recoverable state across the maximum number
    /// of polls — the enqueued job is not making progress.
    NoProgressBudgetExhausted,
}

impl fmt::Display for AbandonReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::FailedToLoadMint => "failed to load mint",
            Self::FailedToLoadReceipt => "failed to load receipt inventory",
            Self::AutomaticRetriesExhausted => "automatic retries exhausted",
            Self::NoProgressBudgetExhausted => "no-progress budget exhausted",
        };

        formatter.write_str(text)
    }
}

/// Why [`recover_mint_until_automatic_budget_exhausted`] stopped, so the durable
/// [`MintRecoveryJob`] records a clean success versus an abandoned mint that is
/// still incomplete and needs surfacing.
enum RecoveryConclusion {
    /// Recovery reached a definitive conclusion: the mint completed, is
    /// genuinely non-recoverable, or no longer exists. Nothing more to do.
    Resolved,
    /// Recovery gave up while the mint was still incomplete — a budget was
    /// exhausted, automatic retries ran out, or the mint could not be loaded.
    Abandoned { reason: AbandonReason },
}

async fn minting_failed_receipt_exists_with_backoff(
    ctx: &MintRecoveryContext,
    mint: &Mint,
    issuer_request_id: &IssuerMintRequestId,
    backoff: Duration,
    failure_backoffs: &mut usize,
) -> Result<Option<bool>, AbandonReason> {
    match minting_failed_receipt_exists(ctx, mint, issuer_request_id).await {
        Ok(exists) => {
            *failure_backoffs = 0;
            Ok(Some(exists))
        }
        Err(error) => {
            *failure_backoffs += 1;
            if *failure_backoffs > MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %error,
                    "Failed to read receipt inventory after maximum backoffs"
                );
                return Err(AbandonReason::FailedToLoadReceipt);
            }

            debug!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %error,
                backoff_ms = backoff.as_millis(),
                "Failed to read receipt inventory; backing off"
            );
            tokio::time::sleep(backoff).await;
            Ok(None)
        }
    }
}

async fn recover_mint_until_automatic_budget_exhausted(
    ctx: &MintRecoveryContext,
    issuer_request_id: &IssuerMintRequestId,
    backoff: Duration,
    max_no_progress_polls: usize,
) -> RecoveryConclusion {
    let mut mint_load_failure_backoffs = 0;
    let mut receipt_load_failure_backoffs = 0;
    let mut no_progress_polls = 0;
    let mut last_state: Option<&'static str> = None;

    loop {
        let mint = match ctx.mint_store.load(issuer_request_id).await {
            Ok(Some(mint)) => {
                mint_load_failure_backoffs = 0;
                mint
            }
            Ok(None) => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    "Mint not found for scheduled recovery"
                );
                return RecoveryConclusion::Resolved;
            }
            // A load failure is transient (e.g. a SQLite blip): back off and
            // retry rather than killing the durable job over a single read error.
            Err(error) => {
                mint_load_failure_backoffs += 1;
                if mint_load_failure_backoffs
                    > MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS
                {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        error = %error,
                        max_failure_backoffs = MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS,
                        "Failed to load mint for scheduled recovery after maximum backoffs"
                    );
                    return RecoveryConclusion::Abandoned {
                        reason: AbandonReason::FailedToLoadMint,
                    };
                }

                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %error,
                    backoff_ms = backoff.as_millis(),
                    "Failed to load mint for scheduled recovery; backing off"
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
        };

        // Reset the no-progress budget whenever the mint advances to a new
        // state, so a healthy multi-step recovery is not abandoned for taking
        // several backoffs overall.
        let state = mint.state_name();
        if Some(state) != last_state {
            no_progress_polls = 0;
            last_state = Some(state);
        }

        match mint.automatic_retry_decision(Utc::now()) {
            AutomaticRetryDecision::NotRecoverable => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    current_state = state,
                    "Mint recovery complete"
                );
                return RecoveryConclusion::Resolved;
            }
            AutomaticRetryDecision::Exhausted => {
                let receipt_exists =
                    match minting_failed_receipt_exists_with_backoff(
                        ctx,
                        &mint,
                        issuer_request_id,
                        backoff,
                        &mut receipt_load_failure_backoffs,
                    )
                    .await
                    {
                        Ok(Some(exists)) => exists,
                        Ok(None) => continue,
                        Err(reason) => {
                            return RecoveryConclusion::Abandoned { reason };
                        }
                    };
                // A confirmed receipt outranks retry exhaustion: the mint
                // succeeded on-chain after attempts ran out, so keep driving
                // toward TokensMinted / callback instead of abandoning.
                if receipt_exists {
                    info!(target: "mint", issuer_request_id = %issuer_request_id,
                        "Automatic mint retries exhausted but on-chain receipt exists; continuing recovery"
                    );
                    no_progress_polls += 1;
                    if no_progress_polls > max_no_progress_polls {
                        warn!(target: "mint", issuer_request_id = %issuer_request_id,
                            max_no_progress_polls,
                            "Scheduled mint recovery stopped after maximum polls without progress"
                        );
                        return RecoveryConclusion::Abandoned {
                            reason: AbandonReason::NoProgressBudgetExhausted,
                        };
                    }

                    if let Err(error) =
                        drive_one_step(ctx, &mint, issuer_request_id).await
                    {
                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            error = %error,
                            "Scheduled recovery step failed after exhausted retries with receipt; backing off"
                        );
                    }

                    tokio::time::sleep(backoff).await;
                    continue;
                }

                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    "Automatic mint retries exhausted"
                );
                return RecoveryConclusion::Abandoned {
                    reason: AbandonReason::AutomaticRetriesExhausted,
                };
            }
            AutomaticRetryDecision::Wait(wait) => {
                let receipt_exists =
                    match minting_failed_receipt_exists_with_backoff(
                        ctx,
                        &mint,
                        issuer_request_id,
                        backoff,
                        &mut receipt_load_failure_backoffs,
                    )
                    .await
                    {
                        Ok(Some(exists)) => exists,
                        Ok(None) => continue,
                        Err(reason) => {
                            return RecoveryConclusion::Abandoned { reason };
                        }
                    };
                // The retry backoff only spaces out re-submissions. If a receipt
                // already exists, the mint actually succeeded on-chain, so there
                // is nothing to wait for — drive immediately to record the
                // existing mint (the per-state job's receipt check turns this
                // into a record, not a re-submission). Otherwise honour the
                // backoff so a genuinely failed mint is not hammered.
                if receipt_exists {
                    no_progress_polls += 1;
                    if no_progress_polls > max_no_progress_polls {
                        warn!(target: "mint", issuer_request_id = %issuer_request_id,
                            max_no_progress_polls,
                            "Scheduled mint recovery stopped after maximum polls without progress"
                        );
                        return RecoveryConclusion::Abandoned {
                            reason: AbandonReason::NoProgressBudgetExhausted,
                        };
                    }

                    if let Err(error) =
                        drive_one_step(ctx, &mint, issuer_request_id).await
                    {
                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            error = %error,
                            "Scheduled recovery step failed; backing off"
                        );
                    }

                    tokio::time::sleep(backoff).await;
                } else {
                    debug!(target: "mint", issuer_request_id = %issuer_request_id,
                        wait_ms = wait.as_millis(),
                        "Waiting for next automatic mint retry window"
                    );
                    tokio::time::sleep(wait).await;
                }
            }
            // The state is recoverable and due: enqueue the per-state job that
            // advances it, then poll for progress on the next iteration.
            AutomaticRetryDecision::Ready => {
                no_progress_polls += 1;
                if no_progress_polls > max_no_progress_polls {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        max_no_progress_polls,
                        "Scheduled mint recovery stopped after maximum polls without progress"
                    );
                    return RecoveryConclusion::Abandoned {
                        reason: AbandonReason::NoProgressBudgetExhausted,
                    };
                }

                if let Err(error) =
                    drive_one_step(ctx, &mint, issuer_request_id).await
                {
                    debug!(target: "mint", issuer_request_id = %issuer_request_id,
                        error = %error,
                        "Scheduled recovery step failed; backing off"
                    );
                }

                tokio::time::sleep(backoff).await;
            }
        }
    }
}

/// Why a [`drive_one_step`] call failed. Recovery treats these as transient and
/// re-polls; the durable jobs and outcome commands are idempotent, so a retried
/// step cannot double-apply.
#[derive(Debug, thiserror::Error)]
enum MintRecoveryStepError {
    #[error(transparent)]
    Store(#[from] SendError<Mint>),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    AssetView(#[from] TokenizedAssetViewError),
    #[error(transparent)]
    UnconfiguredNetwork(#[from] UnconfiguredNetworkError),
    #[error("no vault configured for {underlying} on {network}")]
    VaultNotFound { underlying: UnderlyingSymbol, network: Network },
}

#[derive(Clone, Copy)]
struct ResolvedVault {
    address: Address,
    chain_id: u64,
}

/// Advances a recoverable mint one step by enqueuing the appropriate per-state
/// job (or issuing the pure transition that precedes it). The durable jobs
/// perform the actual I/O; recovery only schedules them. The deterministic
/// `external_tx_id` keeps a re-submission double-mint-safe.
async fn drive_one_step(
    ctx: &MintRecoveryContext,
    mint: &Mint,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<(), MintRecoveryStepError> {
    match mint {
        // The deposit was never recorded: do the pure transition; the next poll
        // enqueues the submission.
        Mint::JournalConfirmed { .. } => {
            ctx.mint_store
                .send(
                    issuer_request_id,
                    MintCommand::Deposit {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                )
                .await?;
        }
        Mint::Minting { underlying, network, .. }
        | Mint::TxIntended { underlying, network, .. } => {
            let vault = resolve_vault(ctx, underlying, *network).await?;
            enqueue_submit(ctx, issuer_request_id, vault).await?;
        }
        // Retry a failed mint: transition back to Minting (advancing the attempt
        // counter), then enqueue a fresh submission.
        Mint::MintingFailed { underlying, network, .. } => {
            ctx.mint_store
                .send(
                    issuer_request_id,
                    MintCommand::RetryMint {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                )
                .await?;
            let vault = resolve_vault(ctx, underlying, *network).await?;
            enqueue_submit(ctx, issuer_request_id, vault).await?;
        }
        Mint::TxSubmitted { underlying, network, tx_id, .. } => {
            let vault = resolve_vault(ctx, underlying, *network).await?;
            enqueue_confirm(ctx, issuer_request_id, vault, tx_id.clone())
                .await?;
        }
        Mint::CallbackPending { .. } => {
            enqueue_callback(ctx, issuer_request_id).await?;
        }
        // Initiated / JournalRejected / Completed / Closed are not driven here
        // (NotRecoverable, or recovery doesn't start before journal confirm).
        _ => {}
    }

    Ok(())
}

async fn resolve_vault(
    ctx: &MintRecoveryContext,
    underlying: &UnderlyingSymbol,
    network: Network,
) -> Result<ResolvedVault, MintRecoveryStepError> {
    let address = find_vault(&ctx.pool, underlying, &network)
        .await?
        .ok_or_else(|| MintRecoveryStepError::VaultNotFound {
            underlying: underlying.clone(),
            network,
        })?;
    let chain_id = ctx.vault_services.chain_id(network)?;

    Ok(ResolvedVault { address, chain_id })
}

/// Whether a `MintingFailed` mint already has a discovered receipt — i.e. its
/// on-chain transaction actually succeeded after the mint was marked failed.
/// When true, recovery should record the existing mint now rather than wait out
/// the re-submission backoff. A non-`MintingFailed` state, an unresolved vault,
/// or a lookup error is treated as "no receipt" so recovery falls back to the
/// normal retry schedule.
async fn minting_failed_receipt_exists(
    ctx: &MintRecoveryContext,
    mint: &Mint,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<bool, crate::receipt_inventory::ReceiptLookupError> {
    let Mint::MintingFailed { underlying, network, .. } = mint else {
        return Ok(false);
    };

    let Ok(vault) = resolve_vault(ctx, underlying, *network).await else {
        return Ok(false);
    };

    ctx.receipts
        .find_by_issuer_request_id(
            vault.chain_id,
            &vault.address,
            issuer_request_id,
        )
        .await
        .map(|receipt| receipt.is_some())
}

/// Enqueues a per-state job for a recovering mint, first freeing the mint's
/// idempotency key from any TERMINAL prior job of the same type (see
/// [`release_terminal_job`]). Without the release, a normal-flow job that
/// already ran to `Done` would hold the key and apalis's
/// `ON CONFLICT(job_type, idempotency_key) DO NOTHING` would silently drop the
/// re-enqueue, stranding recovery. A still-active (`Pending`/`Running`) job is
/// left in place so the push still dedups against it.
async fn enqueue_submit(
    ctx: &MintRecoveryContext,
    issuer_request_id: &IssuerMintRequestId,
    vault: ResolvedVault,
) -> Result<(), MintRecoveryStepError> {
    release_terminal_job(
        &ctx.pool,
        job_type::<SubmitMintJob>(),
        &issuer_request_id.to_string(),
    )
    .await?;

    ctx.submit_queue
        .clone()
        .push_with_idempotency_key(
            SubmitMintJob {
                issuer_request_id: issuer_request_id.clone(),
                vault: vault.address,
                chain_id: vault.chain_id,
            },
            issuer_request_id.to_string(),
        )
        .await?;

    Ok(())
}

async fn enqueue_confirm(
    ctx: &MintRecoveryContext,
    issuer_request_id: &IssuerMintRequestId,
    vault: ResolvedVault,
    tx_id: TxId,
) -> Result<(), MintRecoveryStepError> {
    release_terminal_job(
        &ctx.pool,
        job_type::<ConfirmMintJob>(),
        &issuer_request_id.to_string(),
    )
    .await?;

    ctx.confirm_queue
        .clone()
        .push_with_idempotency_key(
            ConfirmMintJob {
                issuer_request_id: issuer_request_id.clone(),
                vault: vault.address,
                chain_id: vault.chain_id,
                tx_id,
            },
            issuer_request_id.to_string(),
        )
        .await?;

    Ok(())
}

async fn enqueue_callback(
    ctx: &MintRecoveryContext,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<(), MintRecoveryStepError> {
    release_terminal_job(
        &ctx.pool,
        job_type::<SendCallbackJob>(),
        &issuer_request_id.to_string(),
    )
    .await?;

    ctx.callback_queue
        .clone()
        .push_with_idempotency_key(
            SendCallbackJob { issuer_request_id: issuer_request_id.clone() },
            issuer_request_id.to_string(),
        )
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use chrono::Utc;
    use event_sorcery::test_store;
    use rust_decimal::Decimal;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::mint::api::test_utils::{
        TestAccountAndAsset, TestHarness, network_vault_services,
    };
    use crate::mint::tests::VAULT;
    use crate::mint::{
        ClientId, IssuerMintRequestId, MintEvent, Network, Quantity,
        TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
    use crate::test_utils::log_count_at;
    use crate::tokenized_asset::{AssetKey, TokenizedAssetCommand};

    /// Builds a real event-sorcery [`Store<Mint>`] over a file-backed SQLite
    /// pool shared with an apalis-sqlite pool (so the durable per-state queues
    /// see the `Jobs` table), seeding the AAPL -> [`VAULT`] mapping so the
    /// recovery vault lookup (`find_vault_by_underlying`) resolves. The `Mint`
    /// aggregate is pure (`Services = ()`); the recovery worker's I/O
    /// dependencies are assembled on demand via [`Self::context`].
    struct MintRecoveryFixture {
        mint_store: Arc<Store<Mint>>,
        receipt_store: Arc<Store<ReceiptInventory>>,
        pool: Pool<Sqlite>,
        apalis_pool: SqlitePool,
        vault_services: NetworkVaultServices,
    }

    impl MintRecoveryFixture {
        async fn new() -> Self {
            let harness = TestHarness::new().await;

            harness
                .asset_store
                .send(
                    &AssetKey::new(
                        UnderlyingSymbol::new("AAPL").unwrap(),
                        Network::Base,
                    ),
                    TokenizedAssetCommand::Add {
                        underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                        token: TokenSymbol::new("tAAPL"),
                        network: Network::Base,
                        vault: VAULT,
                    },
                )
                .await
                .unwrap();

            let receipt_store = Arc::new(test_store::<ReceiptInventory>(
                harness.pool.clone(),
                (),
            ));

            let TestHarness { pool, apalis_pool, mint_store, vault, .. } =
                harness;

            Self {
                mint_store,
                receipt_store,
                pool,
                apalis_pool,
                vault_services: network_vault_services(vault),
            }
        }

        /// Assembles the recovery worker's I/O dependencies against the shared
        /// pools: the event-store pool, a [`CqrsReceiptService`] over the
        /// fixture's receipt inventory, and the three per-state job queues on
        /// the apalis pool.
        fn context(&self) -> MintRecoveryContext {
            MintRecoveryContext {
                mint_store: self.mint_store.clone(),
                pool: self.pool.clone(),
                vault_services: self.vault_services.clone(),
                receipts: Arc::new(CqrsReceiptService::new(
                    self.receipt_store.clone(),
                )),
                submit_queue: JobQueue::new(&self.apalis_pool),
                confirm_queue: JobQueue::new(&self.apalis_pool),
                callback_queue: JobQueue::new(&self.apalis_pool),
            }
        }

        /// Seeds the event store with raw `Mint` events, putting the aggregate
        /// directly into the desired lifecycle state. Mirrors the e2e
        /// setup-phase pattern of writing rows to the `events` table; the
        /// running service then reacts to them during the scenario.
        async fn seed_mint_events(
            &self,
            issuer_request_id: &IssuerMintRequestId,
            events: Vec<MintEvent>,
        ) {
            seed_mint_events(&self.pool, issuer_request_id, events).await;
        }
    }

    async fn seed_mint_events(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        events: Vec<MintEvent>,
    ) {
        let aggregate_id = issuer_request_id.to_string();

        for (offset, event) in events.into_iter().enumerate() {
            let sequence = i64::try_from(offset).unwrap() + 1;
            let payload = serde_json::to_value(&event).unwrap();
            let variant = payload
                .as_object()
                .and_then(|map| map.keys().next())
                .expect("MintEvent serializes as an externally-tagged enum")
                .clone();
            let event_type = format!("MintEvent::{variant}");
            let payload_str = payload.to_string();

            sqlx::query(
                "
                INSERT INTO events (
                    aggregate_type,
                    aggregate_id,
                    sequence,
                    event_type,
                    event_version,
                    payload,
                    metadata
                )
                VALUES ('Mint', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(&aggregate_id)
            .bind(sequence)
            .bind(&event_type)
            .bind(&payload_str)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    fn tx_failed_events(
        issuer_request_id: &IssuerMintRequestId,
        failed_at: chrono::DateTime<Utc>,
    ) -> Vec<MintEvent> {
        let mut events = tx_submitted_events(issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "terminal signer failure".to_string(),
            failed_at,
        });

        events
    }

    fn test_issuer_request_id() -> IssuerMintRequestId {
        IssuerMintRequestId::new(
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
                .unwrap(),
        )
    }

    fn minting_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();

        vec![
            MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: now,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: now,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: now,
            },
        ]
    }

    fn tx_submitted_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_events(issuer_request_id);

        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::Legacy("fb-tx-123".to_string()),
            submitted_at: now,
        });

        events
    }

    /// The budget loop must sleep the backoff on every no-progress poll rather
    /// than spinning through its budget instantly, so total elapsed time is at
    /// least one backoff per poll — a spinning loop would finish in
    /// microseconds. Here a `MintingFailed` mint whose re-submission never
    /// completes (no worker drains the enqueued `SubmitMintJob`) keeps the loop
    /// driving until the no-progress budget is spent.
    #[traced_test]
    #[tokio::test]
    async fn scheduled_recovery_backs_off_without_progress() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_events(&issuer_request_id, failed_at);
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let backoff = Duration::from_millis(5);
        let max_no_progress_polls = 8;
        let start = tokio::time::Instant::now();
        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context(),
            &issuer_request_id,
            backoff,
            max_no_progress_polls,
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            matches!(
                conclusion,
                RecoveryConclusion::Abandoned {
                    reason: AbandonReason::NoProgressBudgetExhausted
                }
            ),
            "No-progress budget exhaustion must report Abandoned, not Resolved"
        );

        assert!(
            elapsed
                >= backoff
                    * (u32::try_from(max_no_progress_polls).unwrap() - 1),
            "Scheduled recovery must sleep the backoff on each no-progress \
             poll, elapsed={elapsed:?}"
        );

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "drive_one_step retries MintingFailed into Minting and re-enqueues \
             the submission, got: {}",
            mint.state_name()
        );

        let test = "scheduled_recovery_backs_off_without_progress";
        assert!(
            log_count_at!(
                Level::WARN,
                &[test, "stopped after maximum polls without progress"]
            ) >= 1,
            "Loop must stop with a warning once the no-progress budget is spent"
        );
    }

    /// A re-enqueue for the same mint while a job is already queued must collapse
    /// via the idempotency key instead of inserting a duplicate `Jobs` row —
    /// otherwise every restart's re-scan would pile up jobs for a stuck mint.
    #[tokio::test]
    async fn enqueue_dedups_per_mint_via_idempotency_key() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();

        enqueue_scheduled_mint_recovery(
            &harness.pool,
            &harness.apalis_pool,
            issuer_request_id.clone(),
        )
        .await
        .unwrap();
        enqueue_scheduled_mint_recovery(
            &harness.pool,
            &harness.apalis_pool,
            issuer_request_id.clone(),
        )
        .await
        .unwrap();

        let queued: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();

        assert_eq!(
            queued, 1,
            "re-enqueue for the same mint must collapse to one job row"
        );
    }

    /// The periodic reconciler pushes WITHOUT releasing terminal jobs, so a mint
    /// whose recovery was abandoned (a `Killed` job) must NOT be re-enqueued —
    /// the idempotency key collapses the insert — or the reconciler would retry
    /// a hopeless mint every pass. (Enqueue a real job first so its `job_type`
    /// matches apalis's, then abandon it.)
    #[tokio::test]
    async fn push_dedups_against_a_killed_job_so_abandoned_mints_are_not_requeued()
     {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();

        push_mint_recovery_job(&harness.apalis_pool, issuer_request_id.clone())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = ?",
        )
        .bind(issuer_request_id.to_string())
        .execute(&harness.pool)
        .await
        .unwrap();

        push_mint_recovery_job(&harness.apalis_pool, issuer_request_id.clone())
            .await
            .unwrap();

        let statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM Jobs WHERE idempotency_key = ?",
        )
        .bind(issuer_request_id.to_string())
        .fetch_all(&harness.pool)
        .await
        .unwrap();

        assert_eq!(
            statuses,
            vec!["Killed".to_string()],
            "push must dedup against the terminal Killed job, not re-enqueue \
             the abandoned mint"
        );
    }

    /// The startup vacuum must delete terminal jobs (so the table stays bounded
    /// and idempotency keys are freed) while leaving still-active and retryable
    /// jobs that apalis will re-pick.
    #[tokio::test]
    async fn vacuum_clears_terminal_jobs_but_keeps_active() {
        let harness = TestHarness::new().await;

        let rows = [
            ("done", "Done", 0, 25),
            ("killed", "Killed", 3, 25),
            ("failed_exhausted", "Failed", 25, 25),
            ("failed_retryable", "Failed", 1, 25),
            ("pending", "Pending", 0, 25),
            ("running", "Running", 1, 25),
        ];

        for (id, status, attempts, max_attempts) in rows {
            sqlx::query(
                "
                INSERT INTO Jobs
                    (job, id, job_type, status, attempts, max_attempts)
                VALUES (X'00', ?, ?, ?, ?, ?)
                ",
            )
            .bind(id)
            .bind(mint_recovery_job_type())
            .bind(status)
            .bind(attempts)
            .bind(max_attempts)
            .execute(&harness.pool)
            .await
            .unwrap();
        }

        // A terminal job belonging to a DIFFERENT apalis job type must survive:
        // the vacuum is scoped to MintRecoveryJob's job_type and must not reap
        // rows that share the Jobs table with other queues.
        sqlx::query(
            "
            INSERT INTO Jobs
                (job, id, job_type, status, attempts, max_attempts)
            VALUES (X'00', 'other_type_done', 'some::other::OtherJob', 'Done', 0, 25)
            ",
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        vacuum_terminal_recovery_jobs(&harness.pool).await.unwrap();

        let survivors: Vec<String> =
            sqlx::query_scalar("SELECT id FROM Jobs ORDER BY id")
                .fetch_all(&harness.pool)
                .await
                .unwrap();

        assert_eq!(
            survivors,
            vec![
                "failed_retryable".to_string(),
                "other_type_done".to_string(),
                "pending".to_string(),
                "running".to_string(),
            ],
            "vacuum must drop only MintRecoveryJob Done/Killed/exhausted-Failed \
             rows, keeping active jobs and every other job type's terminal rows"
        );
    }

    /// apalis-sqlite stores a job's `job_type` as `std::any::type_name` of the
    /// task type, and our terminal-job cleanup scopes its deletes to exactly
    /// [`mint_recovery_job_type`]. Pin that contract against a really-pushed job
    /// so a future apalis change to the derivation fails loudly here instead of
    /// silently stranding recovery jobs whose terminal rows never get reaped.
    #[tokio::test]
    async fn pushed_job_type_matches_cleanup_scope() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();

        push_mint_recovery_job(&harness.apalis_pool, issuer_request_id.clone())
            .await
            .unwrap();

        let job_type: String = sqlx::query_scalar(
            "SELECT job_type FROM Jobs WHERE idempotency_key = ?",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();

        assert_eq!(
            job_type,
            mint_recovery_job_type(),
            "apalis must store the job_type our terminal-job cleanup deletes by"
        );
    }

    /// The budget loop must report `Abandoned` — not a clean conclusion — when
    /// it gives up on a still-incomplete mint (here a re-submission that never
    /// completes, outlasting the no-progress budget), so the job is recorded as
    /// Killed rather than a success that hides a stuck mint.
    #[traced_test]
    #[tokio::test]
    async fn budget_loop_reports_abandoned_when_no_progress_budget_spent() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_events(&issuer_request_id, failed_at);
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context(),
            &issuer_request_id,
            Duration::from_millis(1),
            2,
        )
        .await;

        assert!(
            matches!(
                conclusion,
                RecoveryConclusion::Abandoned {
                    reason: AbandonReason::NoProgressBudgetExhausted
                }
            ),
            "a mint that outlasts its budget must abandon, not resolve"
        );

        let test =
            "budget_loop_reports_abandoned_when_no_progress_budget_spent";
        assert!(
            log_count_at!(
                Level::WARN,
                &[test, "stopped after maximum polls without progress"]
            ) >= 1,
            "abandoning on the no-progress budget must log the WARN"
        );
    }

    /// `MintRecoveryJob::perform` resolves cleanly (`Ok`) when there is nothing
    /// to recover — an absent mint — so apalis records the job as Done. Also
    /// exercises the job's dispatch into the budget loop.
    #[traced_test]
    #[tokio::test]
    async fn run_resolves_when_mint_absent() {
        let fixture = MintRecoveryFixture::new().await;
        let issuer_request_id = test_issuer_request_id();

        let result = MintRecoveryJob { issuer_request_id }
            .perform(&fixture.context())
            .await;

        assert!(
            result.is_ok(),
            "recovery of an absent mint must resolve cleanly"
        );

        let test = "run_resolves_when_mint_absent";
        assert!(
            log_count_at!(
                Level::DEBUG,
                &[test, "Mint not found for scheduled recovery"]
            ) >= 1,
            "an absent mint must log the not-found path"
        );
    }

    /// Exhausted automatic retries must not abandon a mint that already has an
    /// on-chain receipt — recovery should keep driving toward TokensMinted /
    /// callback instead of returning `AutomaticRetriesExhausted`.
    #[traced_test]
    #[tokio::test]
    async fn exhausted_retries_with_existing_receipt_keep_driving() {
        use crate::receipt_inventory::{
            BurnPlan, BurnTrackingError, MintedReceiptParams,
            ReceiptLookupError, ReceiptRegistrationError, ReceiptService,
            RecoveredReceipt, Shares,
        };
        use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
        use alloy::primitives::{B256, U256};
        use async_trait::async_trait;

        enum ReceiptLookupBehavior {
            Present,
            Fails,
        }

        struct ReceiptLookupStub(ReceiptLookupBehavior);

        #[async_trait]
        impl ReceiptService for ReceiptLookupStub {
            async fn register_minted_receipt(
                &self,
                _params: MintedReceiptParams,
            ) -> Result<(), ReceiptRegistrationError> {
                Ok(())
            }

            async fn for_burn(
                &self,
                _chain_id: u64,
                _vault: Address,
                _redemption_issuer_request_id: &IssuerRedemptionRequestId,
                _shares_to_burn: Shares,
                _dust: Shares,
            ) -> Result<BurnPlan, BurnTrackingError> {
                Ok(BurnPlan {
                    allocations: vec![],
                    total_burn: Shares::ZERO,
                    dust: Shares::ZERO,
                })
            }

            async fn reserve_burn(
                &self,
                _chain_id: u64,
                _vault: Address,
                _redemption_issuer_request_id: IssuerRedemptionRequestId,
                _burns: Vec<BurnRecord>,
            ) -> Result<(), ReceiptRegistrationError> {
                Ok(())
            }

            async fn release_burn(
                &self,
                _chain_id: u64,
                _vault: Address,
                _redemption_issuer_request_id: IssuerRedemptionRequestId,
            ) -> Result<(), ReceiptRegistrationError> {
                Ok(())
            }

            async fn settle_burn(
                &self,
                _chain_id: u64,
                _vault: Address,
                _redemption_issuer_request_id: IssuerRedemptionRequestId,
            ) -> Result<(), ReceiptRegistrationError> {
                Ok(())
            }

            async fn reserved_redemptions(
                &self,
                _chain_id: u64,
                _vault: Address,
            ) -> Result<Vec<IssuerRedemptionRequestId>, ReceiptLookupError>
            {
                Ok(vec![])
            }

            async fn find_by_issuer_request_id(
                &self,
                _chain_id: u64,
                _vault: &Address,
                _issuer_request_id: &IssuerMintRequestId,
            ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError>
            {
                match self.0 {
                    ReceiptLookupBehavior::Present => {
                        Ok(Some(RecoveredReceipt {
                            receipt_id: U256::from(1),
                            tx_hash: B256::ZERO,
                            shares: U256::from(100),
                            block_number: 1,
                        }))
                    }
                    ReceiptLookupBehavior::Fails => {
                        Err(ReceiptLookupError::Inconsistent {
                            issuer_request_id: _issuer_request_id.clone(),
                            receipt_id: U256::from(1).into(),
                        })
                    }
                }
            }
        }

        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::hours(2);
        let mut events = tx_submitted_events(&issuer_request_id);
        for _ in 0..5 {
            events.push(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
            });
        }
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mut ctx = fixture.context();
        ctx.receipts =
            Arc::new(ReceiptLookupStub(ReceiptLookupBehavior::Present));

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &ctx,
            &issuer_request_id,
            Duration::from_millis(1),
            3,
        )
        .await;

        assert!(
            !matches!(
                conclusion,
                RecoveryConclusion::Abandoned {
                    reason: AbandonReason::AutomaticRetriesExhausted
                }
            ),
            "an exhausted mint with a confirmed receipt must not be abandoned \
             for retry exhaustion"
        );

        let test = "exhausted_retries_with_existing_receipt_keep_driving";
        assert!(
            log_count_at!(
                Level::INFO,
                &[test, "on-chain receipt exists; continuing recovery"]
            ) >= 1,
            "must log that recovery continues because a receipt exists"
        );

        let lookup_failure_id = IssuerMintRequestId::random();
        let mut lookup_failure_events = tx_submitted_events(&lookup_failure_id);
        for _ in 0..5 {
            lookup_failure_events.push(MintEvent::MintingFailed {
                issuer_request_id: lookup_failure_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
            });
        }
        fixture
            .seed_mint_events(&lookup_failure_id, lookup_failure_events)
            .await;
        ctx.receipts =
            Arc::new(ReceiptLookupStub(ReceiptLookupBehavior::Fails));

        let lookup_failure = recover_mint_until_automatic_budget_exhausted(
            &ctx,
            &lookup_failure_id,
            Duration::from_millis(1),
            3,
        )
        .await;

        assert!(matches!(
            lookup_failure,
            RecoveryConclusion::Abandoned {
                reason: AbandonReason::FailedToLoadReceipt
            }
        ));
    }

    /// `MintRecoveryJob::perform` returns `Err(AbortError)` when recovery
    /// abandons a still-incomplete mint (here, automatic retries exhausted), so
    /// apalis marks the job Killed instead of a `Done` that would hide the stuck
    /// mint. This is the load-bearing Abandoned→Err arm; without it a stuck mint
    /// looks like a successful recovery.
    #[traced_test]
    #[tokio::test]
    async fn run_returns_abort_error_when_recovery_abandons() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::hours(2);
        let mut events = tx_submitted_events(&issuer_request_id);
        // Five pre-acceptance failures push `attempts` past
        // MAX_AUTOMATIC_MINT_RETRY_ATTEMPT (4), so the mint is exhausted and the
        // budget loop abandons it on the first pass with no backoff sleep.
        for _ in 0..5 {
            events.push(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
            });
        }
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let error = MintRecoveryJob { issuer_request_id }
            .perform(&fixture.context())
            .await
            .expect_err("an exhausted mint must abort, not resolve");

        assert!(
            error.to_string().contains("automatic retries exhausted"),
            "abort reason should name the abandonment cause, got: {error}"
        );

        let test = "run_returns_abort_error_when_recovery_abandons";
        assert!(
            log_count_at!(
                Level::ERROR,
                &[test, "abandoned the mint while still incomplete"]
            ) >= 1,
            "the abandon→abort path must log the ERROR for operators"
        );
    }

    /// Drives a mint to `JournalConfirmed` — a recoverable state — through the
    /// harness's command flow, so the `mint_view` projection the reconciler
    /// queries is populated (raw event seeding would leave the view empty).
    async fn seed_recoverable_mint(
        harness: &TestHarness,
        issuer_request_id: &IssuerMintRequestId,
    ) {
        let TestAccountAndAsset {
            client_id,
            underlying,
            token,
            network,
            wallet,
        } = harness.setup_account_and_asset().await;

        harness
            .mint_store
            .send(
                issuer_request_id,
                MintCommand::Initiate {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-reconcile",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                },
            )
            .await
            .unwrap();

        harness
            .mint_store
            .send(
                issuer_request_id,
                MintCommand::ConfirmJournal {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();
    }

    /// The reconciler must enqueue a job for a recoverable mint that has no
    /// job row at all — the "lost enqueue" case it exists to close.
    #[tokio::test]
    async fn reconcile_enqueues_job_for_recoverable_mint_without_row() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();
        seed_recoverable_mint(&harness, &issuer_request_id).await;

        reconcile_recoverable_mints(&harness.pool, &harness.apalis_pool).await;

        let queued: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key = ?
            ",
        )
        .bind(mint_recovery_job_type())
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();

        assert_eq!(
            queued, 1,
            "the reconciler must enqueue exactly one job for a recoverable \
             mint that lost its job row"
        );
    }

    /// The reconciler pushes WITHOUT releasing terminal jobs, so a mint whose
    /// recovery was deliberately abandoned (`Killed`) must dedup against its
    /// terminal row instead of being resurrected every pass.
    #[tokio::test]
    async fn reconcile_does_not_resurrect_killed_job() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();
        seed_recoverable_mint(&harness, &issuer_request_id).await;

        push_mint_recovery_job(&harness.apalis_pool, issuer_request_id.clone())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = ?",
        )
        .bind(issuer_request_id.to_string())
        .execute(&harness.pool)
        .await
        .unwrap();

        reconcile_recoverable_mints(&harness.pool, &harness.apalis_pool).await;

        let statuses: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM Jobs WHERE idempotency_key = ?",
        )
        .bind(issuer_request_id.to_string())
        .fetch_all(&harness.pool)
        .await
        .unwrap();

        assert_eq!(
            statuses,
            vec!["Killed".to_string()],
            "the reconciler must not resurrect a deliberately abandoned \
             mint's Killed job"
        );
    }

    /// The startup reset must flip a `Running` job orphaned by a dead process
    /// back to `Pending` with its lock columns cleared, so the fresh worker
    /// picks it up without waiting out apalis's orphan re-enqueue timeout.
    #[tokio::test]
    async fn reset_orphaned_recovery_jobs_flips_running_to_pending() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();

        push_mint_recovery_job(&harness.apalis_pool, issuer_request_id.clone())
            .await
            .unwrap();

        // A fake dead worker to hold the lock; a Workers row is required
        // because Jobs.lock_by carries a foreign key to Workers(id).
        sqlx::query(
            "
            INSERT INTO Workers (id, worker_type, storage_name)
            VALUES ('dead-worker', ?, 'SqliteStorage')
            ",
        )
        .bind(mint_recovery_job_type())
        .execute(&harness.pool)
        .await
        .unwrap();

        sqlx::query(
            "
            UPDATE Jobs
            SET
                status = 'Running',
                lock_at = strftime('%s', 'now'),
                lock_by = 'dead-worker'
            WHERE idempotency_key = ?
            ",
        )
        .bind(issuer_request_id.to_string())
        .execute(&harness.pool)
        .await
        .unwrap();

        reset_orphaned_recovery_jobs(&harness.pool).await.unwrap();

        let (status, lock_at, lock_by): (String, Option<i64>, Option<String>) =
            sqlx::query_as(
                "
                SELECT status, lock_at, lock_by
                FROM Jobs
                WHERE idempotency_key = ?
                ",
            )
            .bind(issuer_request_id.to_string())
            .fetch_one(&harness.pool)
            .await
            .unwrap();

        assert_eq!(
            status, "Pending",
            "an orphaned Running job must flip back to Pending"
        );
        assert!(
            lock_at.is_none() && lock_by.is_none(),
            "the reset must clear the lock columns, got lock_at={lock_at:?} \
             lock_by={lock_by:?}"
        );
    }
}
