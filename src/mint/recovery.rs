use alloy::primitives::TxHash;
use apalis::prelude::AbortError;
use apalis_sqlite::SqlitePool;
use async_trait::async_trait;
use chrono::Utc;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{
    AutomaticRetryDecision, IssuerMintRequestId, Mint, MintCommand, MintError,
    MintRecoveryMode, find_all_recoverable_mints,
};
use crate::jobs::{Job, JobQueue};
use crate::receipt_inventory::ItnReceiptHandler;
use crate::vault::VaultService;

/// Production handler that triggers mint recovery when an ITN receipt is
/// discovered by the receipt monitor.
#[derive(Clone)]
pub(crate) struct MintRecoveryHandler {
    mint_store: Arc<Store<Mint>>,
    pool: Pool<Sqlite>,
    apalis_pool: SqlitePool,
}

impl MintRecoveryHandler {
    pub(crate) const fn new(
        mint_store: Arc<Store<Mint>>,
        pool: Pool<Sqlite>,
        apalis_pool: SqlitePool,
    ) -> Self {
        Self { mint_store, pool, apalis_pool }
    }
}

#[async_trait]
impl ItnReceiptHandler for MintRecoveryHandler {
    async fn on_itn_receipt_discovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    ) {
        let mint_store = self.mint_store.clone();
        let pool = self.pool.clone();
        let apalis_pool = self.apalis_pool.clone();
        tokio::spawn(async move {
            let result = mint_store
                .send(
                    &issuer_request_id,
                    MintCommand::RecoverFromReceipt {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_hash,
                    },
                )
                .await;
            match result {
                Ok(()) => {
                    if let Err(error) = enqueue_scheduled_mint_recovery(
                        &pool,
                        &apalis_pool,
                        issuer_request_id.clone(),
                    )
                    .await
                    {
                        warn!(target: "mint", issuer_request_id = %issuer_request_id,
                            %error,
                            "Failed to enqueue receipt-triggered mint recovery"
                        );
                    }
                }
                Err(AggregateError::UserError(LifecycleError::Apply(
                    MintError::NotRecoverable { current_state },
                ))) => {
                    debug!(target: "mint", issuer_request_id = %issuer_request_id,
                        current_state,
                        "Receipt discovery ignored for current mint state"
                    );
                }
                Err(error) => {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        %error,
                        "Receipt-triggered mint recovery failed"
                    );
                }
            }
        });
    }
}

/// Why a [`drive_recovery`] pass stopped. Lets the scheduled recovery loop
/// decide whether to wait, back off, or give up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriveOutcome {
    /// Reached a terminal or non-recoverable state — no further work.
    Done,
    /// Paused until the next automatic retry window elapses.
    RetryNotDue,
    /// Automatic retry budget is exhausted.
    Exhausted,
    /// A command failed unexpectedly, or recovery did not converge.
    Failed,
}

#[derive(Clone, Copy)]
enum RetryPolicy {
    Enforce,
    Bypass,
}

#[derive(Clone, Copy)]
enum ClassifiedRecoveryOutcome {
    Done { current_state: &'static str },
    RetryNotDue { wait: Duration },
    Exhausted,
}

/// Drives a mint through recovery to completion using `MintCommand::Recover`.
pub(crate) async fn recover_mint(
    mint_store: &Store<Mint>,
    vault_service: &Arc<dyn VaultService>,
    issuer_request_id: IssuerMintRequestId,
) -> DriveOutcome {
    drive_recovery(
        mint_store,
        vault_service,
        issuer_request_id,
        RetryPolicy::Enforce,
        recovery_step_requires_wallet,
        |_, id, wallet_locked| {
            if wallet_locked {
                MintCommand::RecoverWalletStep {
                    issuer_request_id: id,
                    mode: MintRecoveryMode::Automatic,
                }
            } else {
                MintCommand::Recover {
                    issuer_request_id: id,
                    mode: MintRecoveryMode::Automatic,
                }
            }
        },
    )
    .await
}

/// Drives an operator-requested recovery, holding the shared wallet lock only
/// for transitions that may prepare or broadcast a transaction.
pub(crate) async fn recover_mint_manually(
    mint_store: &Store<Mint>,
    vault_service: &Arc<dyn VaultService>,
    issuer_request_id: IssuerMintRequestId,
) -> DriveOutcome {
    drive_recovery(
        mint_store,
        vault_service,
        issuer_request_id,
        RetryPolicy::Bypass,
        recovery_step_requires_wallet,
        |_, id, wallet_locked| {
            if wallet_locked {
                MintCommand::RecoverWalletStep {
                    issuer_request_id: id,
                    mode: MintRecoveryMode::Manual,
                }
            } else {
                MintCommand::Recover {
                    issuer_request_id: id,
                    mode: MintRecoveryMode::Manual,
                }
            }
        },
    )
    .await
}

/// Fixed backoff applied when a scheduled recovery pass cannot make progress —
/// a transient error (e.g. RPC blip) occurred. Keeps the loop from spinning while waiting.
const SCHEDULED_RECOVERY_BACKOFF: Duration = Duration::from_secs(60);

/// Budget for retry-window wakeups (`Wait` / `RetryNotDue`). The automatic
/// schedule already terminates healthy retries via `Exhausted` after the
/// attempt cap; this bounds the degenerate case where a mint keeps re-failing
/// at the same attempt (e.g. submission errors before tx acceptance),
/// so the task gives up and the next restart re-picks it instead of looping.
const MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS: usize =
    (Mint::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT as usize) * 2 + 4;

/// Budget for consecutive transient-failure backoffs (e.g. RPC blips) before
/// giving up. Small: a persistent error should surface for investigation, not
/// be hammered indefinitely. The next restart re-picks the mint.
const MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS: usize = 8;

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

/// Runtime dependencies injected into the [`MintRecoveryJob`] worker.
///
/// Bundled so the generic [`crate::jobs::work`] adapter can take a single
/// `Data<Arc<Ctx>>` while recovery still needs both the mint store and the
/// vault service for on-chain retry.
pub(crate) struct MintRecoveryJobCtx {
    pub(crate) mint_store: Arc<Store<Mint>>,
    pub(crate) vault_service: Arc<dyn VaultService>,
}

impl Job<MintRecoveryJobCtx> for MintRecoveryJob {
    type Output = ();
    type Error = AbortError;

    async fn perform(
        &self,
        ctx: &MintRecoveryJobCtx,
    ) -> Result<(), AbortError> {
        match recover_mint_until_automatic_budget_exhausted(
            &ctx.mint_store,
            &ctx.vault_service,
            &self.issuer_request_id,
            SCHEDULED_RECOVERY_BACKOFF,
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

/// Deletes a TERMINAL recovery job for one mint so its idempotency key is free
/// to re-enqueue, leaving any active (`Pending`/`Running`) job so the
/// re-enqueue still dedups against it. Runs on the event-store pool (same
/// SQLite file as the apalis pool).
async fn release_terminal_recovery_job(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
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
    .bind(mint_recovery_job_type())
    .bind(issuer_request_id.to_string())
    .execute(pool)
    .await?;

    Ok(())
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
    /// The aggregate's automatic-retry attempts ran out.
    AutomaticRetriesExhausted,
    /// The transient-failure backoff budget was spent.
    TransientFailureBudgetExhausted,
    /// The retry-wakeup budget was spent.
    RetryWakeupBudgetExhausted,
}

impl fmt::Display for AbandonReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::FailedToLoadMint => "failed to load mint",
            Self::AutomaticRetriesExhausted => "automatic retries exhausted",
            Self::TransientFailureBudgetExhausted => {
                "transient failure budget exhausted"
            }
            Self::RetryWakeupBudgetExhausted => "retry wakeup budget exhausted",
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

async fn recover_mint_until_automatic_budget_exhausted(
    mint_store: &Store<Mint>,
    vault_service: &Arc<dyn VaultService>,
    issuer_request_id: &IssuerMintRequestId,
    backoff: Duration,
) -> RecoveryConclusion {
    let mut retry_wakeups = 0;
    let mut failure_backoffs = 0;

    loop {
        let mint = match mint_store.load(issuer_request_id).await {
            Ok(Some(mint)) => mint,
            // A missing mint cannot be recovered — the same outcome the old
            // default (`Uninitialized`) aggregate produced via
            // `AutomaticRetryDecision::NotRecoverable`.
            Ok(None) => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    "Mint not found for scheduled recovery"
                );
                return RecoveryConclusion::Resolved;
            }
            // A load failure is transient (e.g. a SQLite blip): back off and
            // retry within the same budget as `DriveOutcome::Failed` rather than
            // abandoning the mint immediately and killing the durable job over a
            // single read error.
            Err(err) => {
                failure_backoffs += 1;
                if failure_backoffs > MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        error = %err,
                        max_failure_backoffs = MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS,
                        "Failed to load mint for scheduled recovery after maximum backoffs"
                    );
                    return RecoveryConclusion::Abandoned {
                        reason: AbandonReason::FailedToLoadMint,
                    };
                }

                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %err,
                    backoff_ms = backoff.as_millis(),
                    "Failed to load mint for scheduled recovery; backing off"
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
        };

        match mint.automatic_retry_decision(Utc::now()) {
            AutomaticRetryDecision::Ready => {
                match recover_mint(
                    mint_store,
                    vault_service,
                    issuer_request_id.clone(),
                )
                .await
                {
                    DriveOutcome::Done => {
                        return RecoveryConclusion::Resolved;
                    }
                    DriveOutcome::Exhausted => {
                        return RecoveryConclusion::Abandoned {
                            reason: AbandonReason::AutomaticRetriesExhausted,
                        };
                    }
                    // A transient error: back off and retry a bounded number of
                    // times so a persistent error surfaces rather than looping.
                    DriveOutcome::Failed => {
                        failure_backoffs += 1;
                        if failure_backoffs
                            > MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS
                        {
                            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                                max_failure_backoffs = MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS,
                                "Scheduled mint recovery stopped after maximum failure backoffs"
                            );
                            return RecoveryConclusion::Abandoned {
                                reason: AbandonReason::TransientFailureBudgetExhausted,
                            };
                        }

                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            backoff_ms = backoff.as_millis(),
                            "Scheduled recovery backing off after a transient failure"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    // The retry window passed between the decision and the
                    // submit-time re-check; sleep so a clock race does not spin
                    // the wakeup budget, then re-evaluate (next decision Waits).
                    DriveOutcome::RetryNotDue => {
                        retry_wakeups += 1;
                        if retry_wakeups > MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS
                        {
                            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                                max_retry_wakeups = MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS,
                                "Scheduled mint recovery stopped after maximum retry wakeups"
                            );
                            return RecoveryConclusion::Abandoned {
                                reason:
                                    AbandonReason::RetryWakeupBudgetExhausted,
                            };
                        }

                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            AutomaticRetryDecision::Wait(wait) => {
                retry_wakeups += 1;
                if retry_wakeups > MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        max_retry_wakeups = MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS,
                        "Scheduled mint recovery stopped after maximum retry wakeups"
                    );
                    return RecoveryConclusion::Abandoned {
                        reason: AbandonReason::RetryWakeupBudgetExhausted,
                    };
                }

                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    wait_ms = wait.as_millis(),
                    "Waiting for next automatic mint retry window"
                );
                tokio::time::sleep(wait).await;
            }
            AutomaticRetryDecision::Exhausted => {
                return RecoveryConclusion::Abandoned {
                    reason: AbandonReason::AutomaticRetriesExhausted,
                };
            }
            AutomaticRetryDecision::NotRecoverable => {
                return RecoveryConclusion::Resolved;
            }
        }
    }
}

const MAX_RECOVERY_ATTEMPTS: usize = 10;

/// Drives a mint through recovery to completion by repeatedly sending
/// commands built by `make_command` until the mint reaches a terminal state.
///
/// A single recovery command advances the mint by one step (e.g.,
/// `MintingFailed` -> `CallbackPending`). The aggregate state is classified
/// before each command and after every successful step so terminal states and
/// retry windows do not need to be discovered through expected command errors.
///
/// Bounded to [`MAX_RECOVERY_ATTEMPTS`] iterations to prevent infinite
/// spinning if a command returns `Ok(())` without advancing state.
async fn drive_recovery(
    mint_store: &Store<Mint>,
    vault_service: &Arc<dyn VaultService>,
    issuer_request_id: IssuerMintRequestId,
    retry_policy: RetryPolicy,
    requires_wallet: impl Fn(&Mint) -> bool,
    make_command: impl Fn(Option<&Mint>, IssuerMintRequestId, bool) -> MintCommand,
) -> DriveOutcome {
    for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
        let mut loaded_mint = match mint_store.load(&issuer_request_id).await {
            Ok(Some(mint)) => mint,
            Ok(None) => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    "Mint not found for recovery"
                );
                return DriveOutcome::Done;
            }
            Err(err) => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to load mint before recovery step"
                );
                return DriveOutcome::Failed;
            }
        };

        if let Some(outcome) = classify_recovery(&loaded_mint, retry_policy) {
            return finish_classified_recovery(&issuer_request_id, outcome);
        }

        let mut wallet_guard = None;
        if requires_wallet(&loaded_mint) {
            wallet_guard = vault_service.lock_wallet().await;
            loaded_mint = match mint_store.load(&issuer_request_id).await {
                Ok(Some(mint)) => mint,
                Ok(None) => {
                    debug!(target: "mint", issuer_request_id = %issuer_request_id,
                        "Mint not found after acquiring wallet lock for recovery"
                    );
                    return DriveOutcome::Done;
                }
                Err(err) => {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        error = %err,
                        "Failed to reload mint under wallet lock before recovery step"
                    );
                    return DriveOutcome::Failed;
                }
            };

            if let Some(outcome) = classify_recovery(&loaded_mint, retry_policy)
            {
                drop(wallet_guard);
                return finish_classified_recovery(&issuer_request_id, outcome);
            }

            if !requires_wallet(&loaded_mint) {
                drop(wallet_guard.take());
            }
        }

        let result = mint_store
            .send(
                &issuer_request_id,
                make_command(
                    Some(&loaded_mint),
                    issuer_request_id.clone(),
                    wallet_guard.is_some(),
                ),
            )
            .await;
        drop(wallet_guard);

        match result {
            Ok(()) => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    attempt,
                    "Recovery step succeeded, continuing"
                );

                if let Some(outcome) = recovery_outcome_after_step(
                    mint_store,
                    &issuer_request_id,
                    retry_policy,
                )
                .await
                {
                    return outcome;
                }
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::NotRecoverable { current_state },
            ))) => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    current_state,
                    "Mint recovery complete"
                );
                return DriveOutcome::Done;
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::RetryNotDue { retry_at },
            ))) => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    %retry_at,
                    "Mint recovery paused until retry window"
                );
                return DriveOutcome::RetryNotDue;
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::AutomaticRetriesExhausted { attempts },
            ))) => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    attempts,
                    "Automatic mint retries exhausted"
                );
                return DriveOutcome::Exhausted;
            }
            Err(err) => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Mint recovery failed"
                );
                return DriveOutcome::Failed;
            }
        }
    }

    error!(target: "mint", issuer_request_id = %issuer_request_id,
        aggregate_id = %issuer_request_id,
        max_attempts = MAX_RECOVERY_ATTEMPTS,
        "Mint recovery exceeded maximum attempts without reaching terminal state"
    );

    DriveOutcome::Failed
}

fn classify_recovery(
    mint: &Mint,
    retry_policy: RetryPolicy,
) -> Option<ClassifiedRecoveryOutcome> {
    match mint.automatic_retry_decision(Utc::now()) {
        AutomaticRetryDecision::Ready => None,
        AutomaticRetryDecision::Wait(wait)
            if matches!(retry_policy, RetryPolicy::Enforce) =>
        {
            Some(ClassifiedRecoveryOutcome::RetryNotDue { wait })
        }
        AutomaticRetryDecision::Exhausted
            if matches!(retry_policy, RetryPolicy::Enforce) =>
        {
            Some(ClassifiedRecoveryOutcome::Exhausted)
        }
        AutomaticRetryDecision::NotRecoverable => {
            Some(ClassifiedRecoveryOutcome::Done {
                current_state: mint.state_name(),
            })
        }
        AutomaticRetryDecision::Wait(_) | AutomaticRetryDecision::Exhausted => {
            None
        }
    }
}

fn finish_classified_recovery(
    issuer_request_id: &IssuerMintRequestId,
    outcome: ClassifiedRecoveryOutcome,
) -> DriveOutcome {
    match outcome {
        ClassifiedRecoveryOutcome::Done { current_state } => {
            info!(target: "mint", issuer_request_id = %issuer_request_id,
                current_state,
                "Mint recovery complete"
            );
            DriveOutcome::Done
        }
        ClassifiedRecoveryOutcome::RetryNotDue { wait } => {
            info!(target: "mint", issuer_request_id = %issuer_request_id,
                wait_ms = wait.as_millis(),
                "Mint recovery paused until retry window"
            );
            DriveOutcome::RetryNotDue
        }
        ClassifiedRecoveryOutcome::Exhausted => {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                "Automatic mint retries exhausted"
            );
            DriveOutcome::Exhausted
        }
    }
}

async fn recovery_outcome_after_step(
    mint_store: &Store<Mint>,
    issuer_request_id: &IssuerMintRequestId,
    retry_policy: RetryPolicy,
) -> Option<DriveOutcome> {
    let mint = match mint_store.load(issuer_request_id).await {
        Ok(Some(mint)) => mint,
        Ok(None) => {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint disappeared during recovery"
            );
            return Some(DriveOutcome::Failed);
        }
        Err(err) => {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %err,
                "Failed to load mint after recovery step"
            );
            return Some(DriveOutcome::Failed);
        }
    };

    classify_recovery(&mint, retry_policy)
        .map(|outcome| finish_classified_recovery(issuer_request_id, outcome))
}

const fn recovery_step_requires_wallet(mint: &Mint) -> bool {
    !matches!(
        mint,
        Mint::JournalRejected { .. }
            | Mint::CallbackPending { .. }
            | Mint::Completed { .. }
            | Mint::Closed { .. }
    )
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use chrono::{Duration as ChronoDuration, Utc};
    use event_sorcery::{StoreBuilder, test_store};
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::alpaca::AlpacaService;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::api::test_utils::{TestAccountAndAsset, TestHarness};
    use crate::mint::tests::{BOT, VAULT};
    use crate::mint::{
        ClientId, IssuerMintRequestId, MintEvent, MintServices, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptSource, ReceiptVaultKey, Shares,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, log_count_at, logs_contain_at};
    use crate::tokenized_asset::{
        AssetKey, TokenizedAsset, TokenizedAssetCommand,
    };
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        PreparedMintTx, ReceiptInformation, TxId, VaultService,
    };

    /// Builds a real event-sorcery [`Store<Mint>`] backed by an in-memory
    /// SQLite pool, wired with the same services the production recovery flow
    /// uses (vault, Alpaca, receipt inventory). The [`TokenizedAsset`]
    /// projection is populated with the AAPL -> [`VAULT`] mapping so the
    /// recovery vault lookup (`find_vault`) resolves.
    struct MintRecoveryFixture {
        mint_store: Arc<Store<Mint>>,
        receipt_store: Arc<Store<ReceiptInventory>>,
        pool: sqlx::SqlitePool,
        vault: Arc<dyn VaultService>,
    }

    impl MintRecoveryFixture {
        async fn new() -> Self {
            Self::new_with_vault(Arc::new(MockVaultService::new_success()))
                .await
        }

        async fn new_with_vault(vault: Arc<dyn VaultService>) -> Self {
            Self::new_with_services(
                vault,
                Arc::new(MockAlpacaService::new_success()),
            )
            .await
        }

        async fn new_with_services(
            vault: Arc<dyn VaultService>,
            alpaca: Arc<dyn AlpacaService>,
        ) -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .unwrap();

            sqlx::migrate!("./migrations").run(&pool).await.unwrap();

            let (asset_store, _asset_projection) =
                StoreBuilder::<TokenizedAsset>::new(pool.clone())
                    .build(())
                    .await
                    .unwrap();

            asset_store
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

            let receipt_store =
                Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

            let services = MintServices::with_single_vault(
                Network::Base,
                ANVIL_CHAIN_ID,
                vault.clone(),
                alpaca,
                Arc::new(CqrsReceiptService::new(receipt_store.clone())),
                pool.clone(),
                BOT,
            );

            let mint_store =
                Arc::new(test_store::<Mint>(pool.clone(), services));

            Self { mint_store, receipt_store, pool, vault }
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

    fn journal_confirmed_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = minting_events(issuer_request_id);
        events.pop();
        events
    }

    fn tx_submitted_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_events(issuer_request_id);

        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::random(),
            submitted_at: now,
        });

        events
    }

    fn mint_intended_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = minting_events(issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: PreparedMintTx::valid_for_test(
                1,
                format!("mint-{issuer_request_id}"),
            ),
            intended_at: Utc::now(),
        });

        events
    }

    fn minting_failed_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_events(issuer_request_id);

        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "timeout".to_string(),
            failed_at: now,
        });

        events
    }

    fn callback_pending_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_failed_events(issuer_request_id);

        events.push(MintEvent::ExistingMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: uint!(42_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            block_number: 1000,
            recovered_at: now,
        });

        events
    }

    fn completed_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = callback_pending_events(issuer_request_id);

        events.push(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at: now,
        });

        events
    }

    async fn setup_with_receipt_and_events(
        events: Vec<MintEvent>,
    ) -> MintRecoveryFixture {
        setup_with_receipt_and_events_and_vault(
            events,
            Arc::new(MockVaultService::new_success()),
        )
        .await
    }

    async fn setup_with_receipt_and_events_and_vault(
        events: Vec<MintEvent>,
        vault: Arc<dyn VaultService>,
    ) -> MintRecoveryFixture {
        let fixture = MintRecoveryFixture::new_with_vault(vault).await;
        setup_fixture_with_receipt_and_events(fixture, events).await
    }

    async fn setup_fixture_with_receipt_and_events(
        fixture: MintRecoveryFixture,
        events: Vec<MintEvent>,
    ) -> MintRecoveryFixture {
        let issuer_request_id = test_issuer_request_id();

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL").unwrap(),
            Quantity::new(Decimal::from(100)),
            Utc::now(),
            None,
        );

        fixture
            .receipt_store
            .send(
                &ReceiptVaultKey::new(ANVIL_CHAIN_ID, VAULT),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(uint!(42_U256)),
                    balance: Shares::from(
                        uint!(100_000000000000000000_U256),
                    ),
                    block_number: 1000,
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    source: ReceiptSource::Itn {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                    receipt_info: Some(Box::new(receipt_info)),
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();

        fixture.seed_mint_events(&issuer_request_id, events).await;

        fixture
    }

    #[traced_test]
    #[tokio::test]
    async fn receipt_scan_bypasses_retry_window_and_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        drive_recovery(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id.clone(),
            RetryPolicy::Bypass,
            recovery_step_requires_wallet,
            |_, id, _wallet_locked| MintCommand::Recover {
                issuer_request_id: id,
                mode: MintRecoveryMode::Automatic,
            },
        )
        .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            mint.state_name()
        );
        let test =
            "receipt_scan_bypasses_retry_window_and_recovers_to_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Mint recovery complete"]),
            1,
        );
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[test, "Command handler returned domain error"]
            ),
            0,
        );
        // Receipt recovery and callback delivery are separate durable steps,
        // so the wallet guard is released before the Alpaca request.
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            2,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn mint_intended_with_receipt_recovers_without_rebroadcast() {
        let issuer_request_id = test_issuer_request_id();
        let events = mint_intended_events(&issuer_request_id);
        let vault = Arc::new(MockVaultService::new_submit_failure());
        let fixture =
            setup_with_receipt_and_events_and_vault(events, vault.clone())
                .await;

        recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id.clone(),
        )
        .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Completed { .. }),
            "receipt proof must complete MintIntended, got {}",
            mint.state_name()
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "receipt-first recovery must not prepare a replacement transaction"
        );
        assert!(
            log_count_at!(
                Level::INFO,
                &["Found existing receipt, recording recovery"]
            ) > 0
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn callback_pending_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = callback_pending_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;
        let wallet_guard = fixture.vault.lock_wallet().await;

        tokio::time::timeout(
            Duration::from_secs(1),
            recover_mint(
                fixture.mint_store.as_ref(),
                &fixture.vault,
                issuer_request_id.clone(),
            ),
        )
        .await
        .expect("callback-only recovery must not wait for the wallet lock");
        drop(wallet_guard);

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            mint.state_name()
        );
        let test = "callback_pending_recovers_to_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Mint recovery complete"]),
            1,
        );
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            1,
        );
    }

    /// Recovery from `Minting` persists the receipt proof before delivering
    /// the callback in a second command.
    #[traced_test]
    #[tokio::test]
    async fn minting_receipt_recovery_persists_before_callback() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "Expected CallbackPending after receipt recovery, got: {}",
            mint.state_name()
        );

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let completed =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(completed, Mint::Completed { .. }));
        let test = "minting_receipt_recovery_persists_before_callback";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    /// The same durable boundary applies from `MintingFailed`.
    #[traced_test]
    #[tokio::test]
    async fn failed_mint_receipt_recovery_persists_before_callback() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "Expected CallbackPending after receipt recovery, got: {}",
            mint.state_name()
        );

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let completed =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(completed, Mint::Completed { .. }));
        let test = "failed_mint_receipt_recovery_persists_before_callback";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    /// Same invariant for the `TxSubmitted` starting state.
    #[traced_test]
    #[tokio::test]
    async fn tx_submitted_recovery_persists_before_callback() {
        let issuer_request_id = test_issuer_request_id();
        let events = tx_submitted_events(&issuer_request_id);
        // No pre-existing receipt — the mock confirm path produces TokensMinted.
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;
        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "Expected CallbackPending after one Recover, got: {}",
            mint.state_name()
        );

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let completed =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(completed, Mint::Completed { .. }));
        let test = "tx_submitted_recovery_persists_before_callback";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    /// Receipt-monitor recovery must durably record the receipt before making
    /// the Alpaca callback.
    #[traced_test]
    #[tokio::test]
    async fn receipt_command_persists_recovery_before_callback() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::RecoverFromReceipt {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                },
            )
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "Expected CallbackPending after receipt command, got: {}",
            mint.state_name()
        );
        assert_eq!(
            log_count_at!(
                Level::INFO,
                &[
                    "receipt_command_persists_recovery_before_callback",
                    "Alpaca callback succeeded"
                ]
            ),
            0,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn receipt_discovery_does_not_send_callback_from_callback_pending() {
        let issuer_request_id = test_issuer_request_id();
        let harness = TestHarness::new().await;
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;
        seed_mint_events(
            &pool,
            &issuer_request_id,
            callback_pending_events(&issuer_request_id),
        )
        .await;
        let handler = MintRecoveryHandler::new(
            mint_store.clone(),
            pool.clone(),
            apalis_pool,
        );

        handler
            .on_itn_receipt_discovered(
                issuer_request_id.clone(),
                b256!(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
            )
            .await;

        tokio::time::timeout(Duration::from_secs(1), async {
            while !logs_contain_at!(
                Level::DEBUG,
                &[
                    "receipt_discovery_does_not_send_callback_from_callback_pending",
                    "Receipt discovery ignored for current mint state",
                ]
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("receipt-triggered recovery should finish");

        let mint = mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(mint, Mint::CallbackPending { .. }));
        let queued: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(queued, 0);
    }

    #[traced_test]
    #[tokio::test]
    async fn completed_mint_returns_cleanly() {
        let issuer_request_id = test_issuer_request_id();
        let events = completed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id.clone(),
        )
        .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            mint.state_name()
        );

        let test = "completed_mint_returns_cleanly";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Mint recovery complete"]),
            1,
        );
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[test, "Command handler returned domain error"]
            ),
            0,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn journal_confirmed_recovery_waits_for_wallet_lock() {
        let issuer_request_id = test_issuer_request_id();
        let events = journal_confirmed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;
        let wallet_guard = fixture.vault.lock_wallet().await;

        let blocked = tokio::time::timeout(
            Duration::from_millis(100),
            recover_mint(
                fixture.mint_store.as_ref(),
                &fixture.vault,
                issuer_request_id.clone(),
            ),
        )
        .await;

        assert!(blocked.is_err(), "recovery must wait for the wallet lock");
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::JournalConfirmed { .. }),
            "recovery must not advance toward signing without the wallet lock"
        );

        drop(wallet_guard);
        recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id.clone(),
        )
        .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(mint, Mint::Completed { .. }));
        assert_eq!(
            log_count_at!(
                Level::INFO,
                &[
                    "journal_confirmed_recovery_waits_for_wallet_lock",
                    "Mint recovery complete",
                ]
            ),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn recovery_reloads_state_and_releases_obsolete_wallet_lock() {
        let issuer_request_id = test_issuer_request_id();
        let vault = Arc::new(MockVaultService::new_success());
        let alpaca =
            Arc::new(MockAlpacaService::new_success().with_callback_delay(500));
        let fixture = MintRecoveryFixture::new_with_services(
            vault.clone(),
            alpaca.clone(),
        )
        .await;
        let fixture = setup_fixture_with_receipt_and_events(
            fixture,
            minting_events(&issuer_request_id),
        )
        .await;
        let wallet_guard = fixture.vault.lock_wallet().await;
        let mint_store = fixture.mint_store.clone();
        let recovery_vault = fixture.vault.clone();
        let recovery_id = issuer_request_id.clone();
        let recovery = tokio::spawn(async move {
            recover_mint(mint_store.as_ref(), &recovery_vault, recovery_id)
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while vault.get_wallet_lock_call_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery must begin waiting for the wallet lock");

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .expect("concurrent receipt recovery should advance the mint");
        drop(wallet_guard);

        tokio::time::timeout(Duration::from_secs(1), async {
            while alpaca.get_call_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("callback must start after the aggregate reload");

        let available_guard = tokio::time::timeout(
            Duration::from_millis(100),
            fixture.vault.lock_wallet(),
        )
        .await
        .expect("callback delivery must not retain the obsolete wallet lock");
        drop(available_guard);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), recovery)
                .await
                .expect("recovery should finish after callback delivery")
                .expect("recovery task should not panic"),
            DriveOutcome::Done,
        );
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(mint, Mint::Completed { .. }));
        assert_eq!(
            log_count_at!(
                Level::INFO,
                &[
                    "recovery_reloads_state_and_releases_obsolete_wallet_lock",
                    "Mint recovery complete",
                ]
            ),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn wallet_recovery_step_defers_concurrent_callback_state() {
        let issuer_request_id = test_issuer_request_id();
        let vault = Arc::new(MockVaultService::new_success());
        let alpaca =
            Arc::new(MockAlpacaService::new_success().with_callback_delay(500));
        let fixture =
            MintRecoveryFixture::new_with_services(vault, alpaca.clone()).await;
        fixture
            .seed_mint_events(
                &issuer_request_id,
                callback_pending_events(&issuer_request_id),
            )
            .await;

        tokio::time::timeout(
            Duration::from_millis(100),
            fixture.mint_store.send(
                &issuer_request_id,
                MintCommand::RecoverWalletStep {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            ),
        )
        .await
        .expect("wallet recovery step must not call Alpaca")
        .expect("callback deferral should be a successful no-op");

        assert_eq!(alpaca.get_call_count(), 0);
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(mint, Mint::CallbackPending { .. }));
        assert_eq!(
            log_count_at!(
                Level::DEBUG,
                &[
                    "wallet_recovery_step_defers_concurrent_callback_state",
                    "Deferring callback from wallet-locked recovery step",
                ]
            ),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn wallet_recovery_noop_converges_on_next_unlocked_iteration() {
        let issuer_request_id = test_issuer_request_id();
        let vault = Arc::new(MockVaultService::new_success());
        let alpaca = Arc::new(MockAlpacaService::new_success());
        let fixture =
            MintRecoveryFixture::new_with_services(vault, alpaca.clone()).await;
        fixture
            .seed_mint_events(
                &issuer_request_id,
                callback_pending_events(&issuer_request_id),
            )
            .await;
        let classifications = Arc::new(AtomicUsize::new(0));
        let classifier_calls = classifications.clone();

        let outcome = drive_recovery(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id.clone(),
            RetryPolicy::Bypass,
            move |_| classifier_calls.fetch_add(1, Ordering::Relaxed) < 2,
            |_, id, wallet_locked| {
                if wallet_locked {
                    MintCommand::RecoverWalletStep {
                        issuer_request_id: id,
                        mode: MintRecoveryMode::Automatic,
                    }
                } else {
                    MintCommand::Recover {
                        issuer_request_id: id,
                        mode: MintRecoveryMode::Automatic,
                    }
                }
            },
        )
        .await;

        assert_eq!(outcome, DriveOutcome::Done);
        assert_eq!(alpaca.get_call_count(), 1);
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(mint, Mint::Completed { .. }));
        assert_eq!(
            log_count_at!(
                Level::DEBUG,
                &[
                    "wallet_recovery_noop_converges_on_next_unlocked_iteration",
                    "Deferring callback from wallet-locked recovery step",
                ]
            ),
            1,
        );
        assert_eq!(
            log_count_at!(
                Level::INFO,
                &[
                    "wallet_recovery_noop_converges_on_next_unlocked_iteration",
                    "Mint recovery complete",
                ]
            ),
            1,
        );
    }

    /// When a mint is in `TxSubmitted` and `confirm_mint` fails, the recovery
    /// pass emits `MintingFailed` (attempts=1) and succeeds. Post-step
    /// classification observes the 1-minute backoff window and returns
    /// `RetryNotDue` without sending another command.
    #[traced_test]
    #[tokio::test]
    async fn tx_submitted_confirm_failure_transitions_to_minting_failed_then_retry_not_due()
     {
        let issuer_request_id = test_issuer_request_id();
        let events = tx_submitted_events(&issuer_request_id);
        let fixture = MintRecoveryFixture::new_with_vault(Arc::new(
            MockVaultService::new_failure(),
        ))
        .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        // First pass: confirm_mint fails → MintingFailed emitted. The step
        // succeeds, then classification observes that the retry is not due.
        let outcome = recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id.clone(),
        )
        .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "Expected MintingFailed after confirm failure, got: {}",
            mint.state_name()
        );
        assert_eq!(
            outcome,
            DriveOutcome::RetryNotDue,
            "Expected RetryNotDue after immediate re-check of fresh failure"
        );
        assert!(
            log_count_at!(
                Level::WARN,
                &["On-chain deposit confirmation failed"]
            ) > 0,
            "Expected confirmation-failure warning"
        );
        let test = "tx_submitted_confirm_failure_transitions_to_minting_failed_then_retry_not_due";
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            1,
            "post-step classification must stop without a second command"
        );
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[test, "Command handler returned domain error"]
            ),
            0,
            "RetryNotDue is expected control flow, not a command error"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn initial_retry_wait_returns_without_sending_command() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome = recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id,
        )
        .await;

        assert_eq!(outcome, DriveOutcome::RetryNotDue);
        let test = "initial_retry_wait_returns_without_sending_command";
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            0,
        );
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[test, "Command handler returned domain error"]
            ),
            0,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn initial_retry_exhaustion_returns_without_sending_command() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - ChronoDuration::hours(24);
        let mut events = minting_events(&issuer_request_id);
        for _ in 0..=Mint::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT {
            events.push(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "timeout".to_string(),
                failed_at,
            });
        }
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome = recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id,
        )
        .await;

        assert_eq!(outcome, DriveOutcome::Exhausted);
        let test = "initial_retry_exhaustion_returns_without_sending_command";
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            0,
        );
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[test, "Command handler returned domain error"]
            ),
            0,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn failed_final_attempt_returns_exhausted_without_extra_command() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - ChronoDuration::hours(24);
        let mut events = minting_events(&issuer_request_id);
        for _ in 0..Mint::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT {
            events.push(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "timeout".to_string(),
                failed_at,
            });
        }
        let fixture = MintRecoveryFixture::new_with_vault(Arc::new(
            MockVaultService::new_prepare_tx_failure(),
        ))
        .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome = recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id,
        )
        .await;

        assert_eq!(outcome, DriveOutcome::Exhausted);
        let test =
            "failed_final_attempt_returns_exhausted_without_extra_command";
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            1,
        );
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[test, "Command handler returned domain error"]
            ),
            0,
        );
    }

    /// When a known `tx_id` is in the `MintingFailed` predecessor and the retry
    /// window has elapsed (`failed_at` old enough for `Ready`), a transient
    /// `confirm_mint` failure must NOT trigger a new submission — it must return
    /// `RetryNotDue` so the next recovery pass retries confirming the same tx.
    ///
    /// Without the fix, the old code called `submit_recovery_mint` unconditionally
    /// on any confirm error, which would submit a fresh tx while the original was
    /// still pending, creating duplicate backed tokens for one journal.
    #[traced_test]
    #[tokio::test]
    async fn minting_failed_with_known_tx_transient_confirm_error_does_not_resubmit()
     {
        let issuer_request_id = test_issuer_request_id();
        // new_failure() makes confirm_mint return Err(VaultError::InvalidReceipt),
        // a non-terminal error.
        let fixture = MintRecoveryFixture::new_with_vault(Arc::new(
            MockVaultService::new_failure(),
        ))
        .await;

        // Seed: Initiated → JournalConfirmed → MintingStarted → MintTxSubmitted
        //       → MintingFailed with failed_at 24h ago (retry window: Ready).
        let failed_at = Utc::now() - ChronoDuration::hours(24);
        let mut events = tx_submitted_events(&issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "timeout".to_string(),
            failed_at,
        });
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome = recover_mint(
            fixture.mint_store.as_ref(),
            &fixture.vault,
            issuer_request_id.clone(),
        )
        .await;

        // Must back off, not submit a new tx.
        assert_eq!(
            outcome,
            DriveOutcome::RetryNotDue,
            "transient confirm error must not submit a replacement tx"
        );

        // No new mint transaction was submitted.
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "Aggregate must stay in MintingFailed, got: {}",
            mint.state_name()
        );

        assert!(
            log_count_at!(
                Level::WARN,
                &["Mint confirm returned transient error"]
            ) > 0,
            "Expected transient-error warning"
        );
    }

    /// `recover_mint_until_automatic_budget_exhausted` stops and logs a warning
    /// after exceeding `MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS` consecutive
    /// `DriveOutcome::Failed` outcomes. This uses a mint whose underlying asset
    /// is not registered in the `TokenizedAsset` projection, causing every
    /// `Recover` command to fail with `AssetNotFound` (surfacing as `Failed`).
    #[traced_test]
    #[tokio::test]
    async fn scheduled_recovery_stops_after_max_failure_backoffs() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        // Intentionally skip TokenizedAsset registration so that every
        // Recover command fails with AssetNotFound → DriveOutcome::Failed.
        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        let vault: Arc<dyn VaultService> =
            Arc::new(MockVaultService::new_success());
        let services = MintServices::with_single_vault(
            Network::Base,
            ANVIL_CHAIN_ID,
            vault.clone(),
            Arc::new(MockAlpacaService::new_success()),
            Arc::new(CqrsReceiptService::new(receipt_store)),
            pool.clone(),
            BOT,
        );
        let mint_store = Arc::new(test_store::<Mint>(pool.clone(), services));

        let issuer_request_id = test_issuer_request_id();

        // Seed a MintingFailed with failed_at 24h in the past so the retry
        // window is always Ready, and attempts=0 so exhaustion is not hit.
        let failed_at = Utc::now() - ChronoDuration::hours(24);
        let mut events = minting_events(&issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "timeout".to_string(),
            failed_at,
        });

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
            .execute(&pool)
            .await
            .unwrap();
        }

        // Zero backoff so the test does not sleep.
        recover_mint_until_automatic_budget_exhausted(
            mint_store.as_ref(),
            &vault,
            &issuer_request_id,
            Duration::ZERO,
        )
        .await;

        assert!(
            log_count_at!(
                Level::WARN,
                &[
                    "Scheduled mint recovery stopped after maximum failure backoffs"
                ]
            ) > 0,
            "Expected exhaustion warning after max failure backoffs"
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

    /// `MintRecoveryJob::perform` resolves cleanly (`Ok`) when there is nothing
    /// to recover — an absent mint — so apalis records the job as Done. Also
    /// exercises the job's dispatch into the budget loop.
    #[traced_test]
    #[tokio::test]
    async fn run_resolves_when_mint_absent() {
        let fixture = MintRecoveryFixture::new().await;
        let issuer_request_id = test_issuer_request_id();

        let result = MintRecoveryJob { issuer_request_id }
            .perform(&MintRecoveryJobCtx {
                mint_store: fixture.mint_store.clone(),
                vault_service: fixture.vault.clone(),
            })
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
            .perform(&MintRecoveryJobCtx {
                mint_store: fixture.mint_store.clone(),
                vault_service: fixture.vault.clone(),
            })
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
