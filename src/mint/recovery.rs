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
    AutomaticRetryDecision, IssuerMintRequestId, Mint, MintCommand,
    MintFailureClassification, MintView, Network, UnderlyingSymbol,
    find_all_recoverable_mints,
};
use crate::config::VaultMode;
use crate::jobs::{Job, JobQueue, QueuePushError, job_type};
use crate::receipt_inventory::{ItnReceiptHandler, ReceiptService};
use crate::tokenized_asset::view::{TokenizedAssetViewError, find_vault};
use crate::vault::{
    MintTxStatus, MintedLogQuery, MintedLogScan, NetworkVaultServices, TxId,
    UnconfiguredNetworkError,
};

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
/// `RecordExistingMint` (or rebroadcasts / replaces only after burn-parity
/// classification — never relying on `external_tx_id` for double-mint safety;
/// the signer does not dedup on it).
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
    /// Set by the admin reprocess endpoint ([`enqueue_manual_mint_recovery`]):
    /// permits exactly one re-drive of a classified (`ManualOnly`) failure
    /// that the automatic loop refuses to touch. Defaults to `false` for
    /// every automatic producer (startup re-scan, reconciler, recovery
    /// kicks), so a deserialized pre-existing job row stays automatic.
    #[serde(default)]
    manual: bool,
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
            self.manual,
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

/// Enqueues the recovery pass triggered by a newly recorded authorization.
/// Its distinct idempotency key prevents a recovery job that is concurrently
/// retiring its pre-authorization snapshot from swallowing the wake-up.
pub(crate) async fn enqueue_authorized_mint_recovery(
    pool: &Pool<Sqlite>,
    apalis_pool: &SqlitePool,
    issuer_request_id: IssuerMintRequestId,
) -> Result<(), anyhow::Error> {
    let idempotency_key = format!("{issuer_request_id}:authorization");
    release_terminal_job(pool, mint_recovery_job_type(), &idempotency_key)
        .await?;
    push_recovery_job(
        apalis_pool,
        MintRecoveryJob {
            issuer_request_id: issuer_request_id.clone(),
            manual: false,
        },
        idempotency_key.clone(),
    )
    .await?;
    debug!(target: "mint", %issuer_request_id, %idempotency_key,
        "Enqueued mint recovery after recording authorization"
    );
    Ok(())
}

/// Admin-reprocess variant of [`enqueue_scheduled_mint_recovery`]: the pushed
/// job carries the `manual` flag, permitting exactly one re-drive of a
/// classified failure the automatic loop refuses. Its distinct idempotency key
/// prevents an active automatic recovery from swallowing the operator's
/// explicitly authorized re-drive.
pub(crate) async fn enqueue_manual_mint_recovery(
    pool: &Pool<Sqlite>,
    apalis_pool: &SqlitePool,
    issuer_request_id: IssuerMintRequestId,
) -> Result<(), anyhow::Error> {
    let idempotency_key = format!("{issuer_request_id}:manual");
    release_terminal_job(pool, mint_recovery_job_type(), &idempotency_key)
        .await?;
    push_recovery_job(
        apalis_pool,
        MintRecoveryJob {
            issuer_request_id: issuer_request_id.clone(),
            manual: true,
        },
        idempotency_key.clone(),
    )
    .await?;
    debug!(target: "mint", %issuer_request_id, %idempotency_key,
        "Enqueued operator-authorized mint recovery"
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ManualRetryOutcome {
    Enqueued,
    /// Another caller already moved the aggregate out of `MintingFailed`; this
    /// command emitted no event, so this caller must not mutate queue state.
    AlreadyHandled,
    /// The domain transition committed, but immediate queue dispatch failed.
    /// The periodic mint reconciler will discover the recoverable `Minting`
    /// state and retry dispatch without another operator command.
    DeferredToRecovery {
        error: String,
    },
}

/// Drives one operator-authorized retry of a `MintingFailed` mint, bypassing
/// the capped scheduled-recovery loop entirely.
///
/// The automatic retry budget bounds UNATTENDED retrying; it must not lock an
/// operator out of a mint whose external blocker (e.g. a signing policy) has
/// since been fixed. This sends `ManualRetryMint` — whose handler enforces
/// the double-mint provenance gate atomically against the CURRENT aggregate
/// state, refusing any failure chain containing a prepared or submitted
/// transaction — and enqueues a fresh `SubmitMintJob`, whose normal
/// submit -> confirm -> callback chain completes the mint (its first step
/// records an already-existing on-chain receipt instead of re-submitting).
/// Each call authorizes exactly one attempt: a mint that fails again parks
/// in `MintingFailed` until the operator reprocesses once more.
pub(crate) async fn manually_retry_failed_mint(
    mint_store: &Arc<Store<Mint>>,
    pool: &Pool<Sqlite>,
    vault_services: &NetworkVaultServices,
    apalis_pool: &SqlitePool,
    issuer_request_id: &IssuerMintRequestId,
    mint: &Mint,
) -> Result<ManualRetryOutcome, anyhow::Error> {
    let Mint::MintingFailed { underlying, network, .. } = mint else {
        anyhow::bail!(
            "manual retry requires a MintingFailed mint; current state is {}",
            mint.state_name()
        );
    };
    let network = *network;

    let address =
        find_vault(pool, underlying, &network).await?.ok_or_else(|| {
            anyhow::anyhow!("no vault found for {underlying} on {network}")
        })?;
    let chain_id = vault_services.chain_id(network)?;

    let manual_retry_id = Uuid::new_v4();
    mint_store
        .send(
            issuer_request_id,
            MintCommand::ManualRetryMint {
                issuer_request_id: issuer_request_id.clone(),
                manual_retry_id,
            },
        )
        .await?;

    let retry_committed = sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM events
            WHERE aggregate_type = 'Mint'
              AND aggregate_id = ?
              AND event_type = 'MintEvent::MintRetryStarted'
              AND json_extract(
                  payload,
                  '$.MintRetryStarted.manual_retry_id'
              ) = ?
        )
        ",
    )
    .bind(issuer_request_id.to_string())
    .bind(manual_retry_id.to_string())
    .fetch_one(pool)
    .await?;
    if !retry_committed {
        info!(target: "mint", issuer_request_id = %issuer_request_id,
            %manual_retry_id,
            "Manual retry was already handled; leaving queue state unchanged"
        );
        return Ok(ManualRetryOutcome::AlreadyHandled);
    }

    info!(target: "mint", issuer_request_id = %issuer_request_id,
        %underlying, %network,
        "Manually retrying failed mint past the automatic budget"
    );

    let dispatch = async {
        release_terminal_job(
            pool,
            job_type::<SubmitMintJob>(),
            &issuer_request_id.to_string(),
        )
        .await?;
        JobQueue::<SubmitMintJob>::new(apalis_pool)
            .push_with_idempotency_key(
                SubmitMintJob {
                    issuer_request_id: issuer_request_id.clone(),
                    vault: address,
                    chain_id,
                },
                issuer_request_id.to_string(),
            )
            .await?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    match dispatch {
        Ok(()) => Ok(ManualRetryOutcome::Enqueued),
        Err(error) => {
            // ManualRetryMint has already committed. Reporting a hard failure
            // would invite the operator to repeat a command whose effect was
            // accepted. Surface partial success instead; the periodic
            // reconciler re-drives every recoverable Minting state and its
            // enqueue path releases terminal submit-job keys before pushing.
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %error,
                "Manual retry committed; deferring queue dispatch to reconciler"
            );
            Ok(ManualRetryOutcome::DeferredToRecovery {
                error: error.to_string(),
            })
        }
    }
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
    let idempotency_key = issuer_request_id.to_string();
    push_recovery_job(
        apalis_pool,
        MintRecoveryJob { issuer_request_id, manual: false },
        idempotency_key,
    )
    .await
}

async fn push_recovery_job(
    apalis_pool: &SqlitePool,
    job: MintRecoveryJob,
    idempotency_key: String,
) -> Result<(), anyhow::Error> {
    let mut attempt = 0;
    // The queue handle is reusable across attempts
    // (`push_with_idempotency_key` takes `&mut self`); build it once rather
    // than reconstructing it on every retry.
    let mut queue = JobQueue::<MintRecoveryJob>::new(apalis_pool);
    let issuer_request_id = job.issuer_request_id.clone();

    loop {
        attempt += 1;

        match queue
            .push_with_idempotency_key(job.clone(), idempotency_key.clone())
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) if attempt < ENQUEUE_ATTEMPTS => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    %idempotency_key, attempt, error = %error,
                    "Failed to enqueue mint recovery; retrying after backoff"
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
///
/// Shared by recovery enqueue helpers and normal-flow jobs (e.g.
/// [`super::job::SubmitMintJob::enqueue_confirm`]) so a prior `Done` confirm
/// cannot silently drop a re-enqueue via apalis
/// `ON CONFLICT(job_type, idempotency_key) DO NOTHING`.
pub(crate) async fn release_terminal_job(
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
    for (issuer_request_id, view) in recoverable_mints {
        // An unresolved replay is reconciled on the NORMAL SCHEDULE (SPEC
        // "Nonce"): each pass re-runs one widened `Minted`-log query. Its
        // previous pass's job row is terminal by design, so this push must
        // release the terminal key first — the plain dedup push below would
        // silently park the mint after its first pass.
        let result = if matches!(
            view,
            MintView::MintingFailed {
                classification:
                    MintFailureClassification::NonceReplayUnresolved,
                ..
            }
        ) {
            enqueue_scheduled_mint_recovery(
                pool,
                apalis_pool,
                issuer_request_id.clone(),
            )
            .await
        } else {
            push_mint_recovery_job(apalis_pool, issuer_request_id.clone()).await
        };
        if let Err(error) = result {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %error,
                "Failed to re-enqueue recoverable mint during reconcile"
            );
        }
    }

    debug!(target: "mint", recoverable_mints = count,
        "Reconcile pass pushed an idempotent recovery job for each recoverable \
         mint; pushes for mints that already have a Pending, Running, Done, or \
         Killed job are silent no-ops (unresolved replays release their \
         terminal row first, so their reconciliation re-runs every pass)"
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
#[derive(Debug)]
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

#[allow(clippy::too_many_lines)]
async fn recover_mint_until_automatic_budget_exhausted(
    ctx: &MintRecoveryContext,
    issuer_request_id: &IssuerMintRequestId,
    backoff: Duration,
    max_no_progress_polls: usize,
    manual: bool,
) -> RecoveryConclusion {
    let mut mint_load_failure_backoffs = 0;
    let mut receipt_load_failure_backoffs = 0;
    let mut no_progress_polls = 0;
    let mut last_state: Option<&'static str> = None;
    // The operator's `manual` flag buys exactly one re-drive of a classified
    // failure: after it is spent, a re-failure that classifies again stops
    // at the `ManualOnly` arm like the automatic path does.
    let mut manual_redrive_available = manual;
    // An unresolved replay gets exactly one widened reconciliation query per
    // job run; the periodic reconciler re-enqueues the job each pass, which
    // IS the "normal schedule" its re-query runs on (SPEC "Nonce").
    let mut reconcile_attempted = false;

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
            // Typed classifications are never auto-retried: the scheduled
            // loop stops here, before any wakeup/budget bookkeeping, and the
            // admin re-drive is the only way forward (mirrors the burn
            // side's classified-skip). The admin path reaches this same
            // loop, so its job carries a `manual` flag that spends exactly
            // one re-drive here — without it the reprocess endpoint would
            // answer "recovery initiated" while nothing runs.
            AutomaticRetryDecision::ManualOnly(classification) => {
                // Deterministic recipient-authorization failures can never
                // succeed with the same stored authorization: re-driving one
                // resubmits the identical `mint()` call, which reverts the
                // same way and only burns gas and a wallet nonce. Per SPEC
                // ("BadRecipientSignature / RecipientCallbackRejected"), the
                // only resolution is `CloseMint` plus a fresh `Initiate`
                // carrying new authorization — so the operator re-drive is
                // refused outright rather than spent on a doomed submission.
                if matches!(
                    classification,
                    MintFailureClassification::BadRecipientSignature
                        | MintFailureClassification::RecipientCallbackRejected
                ) {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        classification = ?classification,
                        "Refusing re-drive of a deterministic \
                         recipient-authorization failure; close the mint and \
                         re-initiate with fresh authorization"
                    );
                    return RecoveryConclusion::Resolved;
                }

                if manual_redrive_available {
                    manual_redrive_available = false;
                    info!(target: "mint", issuer_request_id = %issuer_request_id,
                        classification = ?classification,
                        "Operator-requested re-drive of a classified mint \
                         failure"
                    );
                    // The operator's adjudication IS the resubmission
                    // authority here: the wallet-guard deferrals in
                    // `drive_minting_failed_step` protect UNATTENDED retries
                    // of vault-direct identities, while a classified failure
                    // is nonce-anchored — a replay of a consumed nonce can
                    // only revert, never double-mint.
                    let redrive = async {
                        let Mint::MintingFailed { underlying, network, .. } =
                            &mint
                        else {
                            return Ok(());
                        };
                        let vault =
                            resolve_vault(ctx, underlying, *network).await?;
                        authorize_resubmit(ctx, issuer_request_id, vault).await
                    };
                    if let Err(error) = redrive.await {
                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            error = %error,
                            "Manual re-drive step failed; backing off"
                        );
                    }
                    tokio::time::sleep(backoff).await;
                    continue;
                }

                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    classification = ?classification,
                    "Skipping non-retryable classified mint failure; manual \
                     recovery required"
                );
                return RecoveryConclusion::Resolved;
            }
            // The inconclusive replay is reconciled, never resubmitted: one
            // widened `Minted`-log re-query per pass. A full match advances
            // the mint and the next iteration keeps driving it forward; a
            // proven mismatch upgrades the classification (and the next
            // iteration parks it via the `ManualOnly` arm); still nothing
            // found retires this pass — the reconciler re-enqueues the next
            // one.
            AutomaticRetryDecision::ReconcileOnly => {
                if reconcile_attempted {
                    debug!(target: "mint", issuer_request_id = %issuer_request_id,
                        "Unresolved replay still has no Minted log at its pair; awaiting the next reconcile pass"
                    );
                    return RecoveryConclusion::Resolved;
                }
                reconcile_attempted = true;
                match reconcile_unresolved_replay(ctx, &mint, issuer_request_id)
                    .await
                {
                    // The mint changed state — re-loop immediately so the
                    // next iteration keeps driving it (the callback for a
                    // recovery, the manual-only skip for an upgrade).
                    Ok(ReconcileOutcome::Advanced) => {}
                    // Unchanged (or unprovable): nothing more this pass can
                    // do — the reconciler re-enqueues the next one.
                    Ok(ReconcileOutcome::Parked) => {
                        return RecoveryConclusion::Resolved;
                    }
                    Err(error) => {
                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            error = %error,
                            "Unresolved-replay reconciliation failed; awaiting the next pass"
                        );
                        return RecoveryConclusion::Resolved;
                    }
                }
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
                // Same inventory gate as Wait/Exhausted for MintingFailed: fail
                // closed while inventory is unreadable (may free-prepare), and
                // surface an existing receipt so SubmitMintJob records it.
                if matches!(&mint, Mint::MintingFailed { .. }) {
                    match minting_failed_receipt_exists_with_backoff(
                        ctx,
                        &mint,
                        issuer_request_id,
                        backoff,
                        &mut receipt_load_failure_backoffs,
                    )
                    .await
                    {
                        Ok(Some(true)) => {
                            // Inside the budget loop and falling through to
                            // another poll, so DEBUG for the same reason the
                            // fail-closed step below is DEBUG.
                            debug!(target: "mint", issuer_request_id = %issuer_request_id,
                                "MintingFailed ready for retry but on-chain receipt exists; driving recovery"
                            );
                        }
                        Ok(Some(false)) => {}
                        Ok(None) => continue,
                        Err(reason) => {
                            return RecoveryConclusion::Abandoned { reason };
                        }
                    }
                }

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
    #[error(transparent)]
    ReceiptLookup(#[from] crate::receipt_inventory::ReceiptLookupError),
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
/// perform the actual I/O; recovery only schedules them.
///
/// After a prepared identity exists, recovery **never** treats `external_tx_id`
/// as double-mint protection — only exact-hash classification / rebroadcast
/// (or inventory) is safe. Replacement requires `MinedReverted` or
/// `ProvablyDead` under the wallet guard.
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
        // The inconclusive replay is never resubmitted — the nonce is
        // consumed either way, so a resubmission can only revert. Its drive
        // IS the widened reconciliation query, whichever path reaches it
        // (the scheduled `ReconcileOnly` arm or an operator re-drive).
        Mint::MintingFailed {
            classification: MintFailureClassification::NonceReplayUnresolved,
            ..
        } => {
            reconcile_unresolved_replay(ctx, mint, issuer_request_id).await?;
        }
        // Failed mint: classify any live prepared identity under the wallet
        // guard before RetryMint. StillMineable / Uncertain never authorize a
        // fresh prepare; only terminal dead/revert may replace after recheck.
        Mint::MintingFailed { underlying, network, .. } => {
            drive_minting_failed_step(
                ctx,
                mint,
                issuer_request_id,
                underlying,
                *network,
            )
            .await?;
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

/// Recovery step for `MintingFailed`. Inventory hits are handled by the
/// budget loop / SubmitMintJob receipt check; here we gate RetryMint on
/// observation of any predecessor prepared identity.
async fn drive_minting_failed_step(
    ctx: &MintRecoveryContext,
    mint: &Mint,
    issuer_request_id: &IssuerMintRequestId,
    underlying: &UnderlyingSymbol,
    network: Network,
) -> Result<(), MintRecoveryStepError> {
    let vault = resolve_vault(ctx, underlying, network).await?;

    let Some(prepared) = mint.pending_prepared_tx() else {
        // Post-intent without prepared bytes (legacy TxSubmitted): never
        // free-prepare a replacement — that is the double-mint hole. Inventory
        // may still record an existing mint via SubmitMintJob; otherwise only
        // confirm-poll the known tx_id and leave MintingFailed for ops.
        if mint.has_unclassifiable_post_intent_identity() {
            let receipt_exists = match minting_failed_receipt_exists(
                ctx,
                mint,
                issuer_request_id,
            )
            .await
            {
                Ok(exists) => exists,
                Err(error) => {
                    // Lookup failure is not "no receipt": refuse free-prepare
                    // and fall through to confirm-poll / ops ERROR below.
                    warn!(
                        target: "mint",
                        issuer_request_id = %issuer_request_id,
                        error = %error,
                        "Inventory lookup failed for post-submit MintingFailed \
                         without prepared_tx; fail closed"
                    );
                    false
                }
            };
            if receipt_exists {
                info!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    "MintingFailed post-submit without prepared_tx but inventory \
                     has receipt; RetryMint so SubmitMintJob can record ExistingMint"
                );
                authorize_resubmit(ctx, issuer_request_id, vault).await?;
                return Ok(());
            }

            // `has_unclassifiable_post_intent_identity` and
            // `latest_known_tx_id` read the same `TxSubmitted` predecessor, so
            // the tx_id is present here and the confirm poll below always runs.
            // That makes this pass self-recovering, and this step repeats on
            // every recovery poll — an ERROR per pass would alarm on a system
            // that is still re-observing. ERROR stays for the accessors
            // disagreeing, where nothing is left to poll and only an operator
            // can resolve it.
            let Some(tx_id) = mint.latest_known_tx_id() else {
                error!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    "MintingFailed after post-submit identity without \
                     prepared_tx or a known tx_id; fail closed with nothing \
                     left to poll (never free-prepare)"
                );
                return Ok(());
            };

            debug!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                tx_id = ?tx_id,
                "MintingFailed after post-submit identity without prepared_tx; \
                 fail closed (confirm poll / inventory only; never free-prepare)"
            );
            enqueue_confirm(ctx, issuer_request_id, vault, tx_id).await?;
            return Ok(());
        }

        // True pre-intent failure (Minting only / never intended): no on-chain
        // identity — existing retry schedule may prepare a first mint.
        authorize_resubmit(ctx, issuer_request_id, vault).await?;
        return Ok(());
    };

    let vault_service = ctx.vault_services.service(network)?;
    let owner = match prepared.recover_signer() {
        Ok(owner) => owner,
        Err(error) => {
            // Corrupt identity: fail closed — cannot prove death without a
            // valid envelope; leave MintingFailed for operator/restart.
            warn!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                error = %error,
                "Cannot recover signer from prepared mint; fail closed"
            );
            return Ok(());
        }
    };

    let wallet_guard = vault_service.lock_wallet().await;
    let status = match vault_service.classify_mint_tx(owner, &prepared).await {
        Ok(status) => status,
        Err(error) => {
            drop(wallet_guard);
            warn!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                tx_hash = %prepared.hash,
                nonce = prepared.nonce,
                error = %error,
                "Mint recovery classification uncertain; not replacing"
            );
            return Ok(());
        }
    };

    match status {
        MintTxStatus::StillMineable => {
            // Mempool-live / unconfirmed: rebroadcast same prepared hash.
            // SubmitMintJob classifies again and rebroadcasts without preparing.
            info!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                tx_hash = %prepared.hash,
                nonce = prepared.nonce,
                status = ?status,
                "Mint recovery rebroadcasting existing identity"
            );
            drop(wallet_guard);
            authorize_resubmit(ctx, issuer_request_id, vault).await?;
        }
        MintTxStatus::MinedSuccess => {
            // Receipt exists: observe via confirm while still MintingFailed
            // (ConfirmMintJob::confirm_while_minting_failed → RecordExistingMint).
            // Do not re-enter submit — a mined deposit must not rebroadcast.
            info!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                tx_hash = %prepared.hash,
                nonce = prepared.nonce,
                "Mint recovery confirming mined success for existing identity"
            );
            drop(wallet_guard);
            enqueue_confirm(
                ctx,
                issuer_request_id,
                vault,
                TxId::from(prepared.hash),
            )
            .await?;
        }
        MintTxStatus::MinedReverted | MintTxStatus::ProvablyDead => {
            // TOCTOU recheck immediately before allowing replacement prepare.
            let recheck =
                match vault_service.classify_mint_tx(owner, &prepared).await {
                    Ok(status) => status,
                    Err(error) => {
                        drop(wallet_guard);
                        warn!(
                            target: "mint",
                            issuer_request_id = %issuer_request_id,
                            tx_hash = %prepared.hash,
                            error = %error,
                            "Mint recovery recheck uncertain; not replacing"
                        );
                        return Ok(());
                    }
                };
            if !matches!(
                recheck,
                MintTxStatus::MinedReverted | MintTxStatus::ProvablyDead
            ) {
                drop(wallet_guard);
                warn!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    tx_hash = %prepared.hash,
                    first = ?status,
                    recheck = ?recheck,
                    "Mint recovery recheck no longer terminal; not replacing"
                );
                return Ok(());
            }

            info!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                tx_hash = %prepared.hash,
                nonce = prepared.nonce,
                status = ?recheck,
                "Mint recovery authorizing replacement after terminal dead/revert"
            );
            drop(wallet_guard);
            authorize_resubmit(ctx, issuer_request_id, vault).await?;
        }
    }

    Ok(())
}

/// Widened backward window for reconciling an unresolved replay: the
/// default scan window was sized for the live recovery timeline, and its
/// coming up empty is exactly what parked the mint — the re-query must look
/// further back than whatever the original lookup covered (SPEC "Recipient
/// Authorization" -> "Nonce"). ~23 days on Base's 2s blocks.
const UNRESOLVED_REPLAY_LOOKBACK_BLOCKS: u64 = 1_000_000;

/// What one widened reconciliation attempt concluded, so the recovery loop
/// can react immediately: an `Advanced` mint changed state (recovered
/// forward, or upgraded to the proven mismatch) and the next iteration
/// keeps driving it without waiting out a backoff; a `Parked` mint is
/// unchanged until the reconciler's next pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileOutcome {
    Advanced,
    Parked,
}

/// One reconciliation attempt for a `NonceReplayUnresolved` mint: re-runs
/// the `Minted`-log full-match over [`UNRESOLVED_REPLAY_LOOKBACK_BLOCKS`].
/// A `FullMatch` resolves the mint forward
/// (`RecordOrchestratorMintRecovered` — the loop's next iteration drives
/// the callback); a `Mismatch` upgrades the classification to the proven
/// `NonceConsumedByOtherMint`; `NotFound` (and any read failure, which
/// proves nothing) leaves the mint parked for the next pass.
async fn reconcile_unresolved_replay(
    ctx: &MintRecoveryContext,
    mint: &Mint,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<ReconcileOutcome, MintRecoveryStepError> {
    let Mint::MintingFailed {
        underlying,
        network,
        wallet,
        quantity,
        mint_mode,
        mint_authorization,
        ..
    } = mint
    else {
        return Ok(ReconcileOutcome::Parked);
    };
    let (
        VaultMode::Orchestrator { address: orchestrator },
        Some(authorization),
    ) = (mint_mode, mint_authorization)
    else {
        // `NonceReplayUnresolved` is only ever recorded for an
        // orchestrator mint holding an authorization; anything else is an
        // impossible replay and there is nothing to reconcile.
        return Ok(ReconcileOutcome::Parked);
    };

    let vault = resolve_vault(ctx, underlying, *network).await?;
    let vault_service = ctx.vault_services.service(*network)?;
    let amount = match quantity.to_u256_with_18_decimals() {
        Ok(amount) => amount,
        Err(error) => {
            // Deterministic for the persisted quantity: reconciliation can
            // never build the full-match query, so the mint stays parked
            // and stuck-visible rather than looping a doomed conversion.
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %error,
                "Unresolved-replay quantity cannot convert to on-chain units"
            );
            return Ok(ReconcileOutcome::Parked);
        }
    };

    match vault_service
        .find_orchestrator_minted_log(MintedLogQuery {
            orchestrator: *orchestrator,
            to: *wallet,
            nonce: authorization.nonce,
            token: vault.address,
            amount,
            lookback_blocks: Some(UNRESOLVED_REPLAY_LOOKBACK_BLOCKS),
        })
        .await
    {
        Ok(MintedLogScan::FullMatch(minted)) => {
            info!(target: "mint", issuer_request_id = %issuer_request_id,
                tx_hash = %minted.tx_hash,
                nonce = %minted.nonce,
                shares_minted = %minted.shares_minted,
                "Widened reconciliation full-matched the unresolved replay; \
                 recovering the landed mint"
            );
            ctx.mint_store
                .send(
                    issuer_request_id,
                    MintCommand::RecordOrchestratorMintRecovered {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_hash: minted.tx_hash,
                        nonce: minted.nonce,
                        shares_minted: minted.shares_minted,
                        block_number: minted.block_number,
                    },
                )
                .await?;
            Ok(ReconcileOutcome::Advanced)
        }
        Ok(MintedLogScan::Mismatch) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                to = %wallet,
                nonce = %authorization.nonce,
                token = %vault.address,
                amount = %amount,
                "Widened reconciliation proved the nonce was consumed by a \
                 different mint; manual reconciliation required"
            );
            ctx.mint_store
                .send(
                    issuer_request_id,
                    MintCommand::RecordMintFailed {
                        issuer_request_id: issuer_request_id.clone(),
                        error: "widened Minted-log scan found the pair's \
                                landing disagreeing on token/amount"
                            .to_string(),
                        classification:
                            MintFailureClassification::NonceConsumedByOtherMint,
                    },
                )
                .await?;
            Ok(ReconcileOutcome::Advanced)
        }
        Ok(MintedLogScan::NotFound) => {
            debug!(target: "mint", issuer_request_id = %issuer_request_id,
                nonce = %authorization.nonce,
                "Widened reconciliation still found no Minted log at the \
                 pair; the mint stays parked for the next pass"
            );
            Ok(ReconcileOutcome::Parked)
        }
        Err(error) => {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                error = %error,
                "Unresolved-replay reconciliation lookup failed; the mint \
                 stays parked for the next pass"
            );
            Ok(ReconcileOutcome::Parked)
        }
    }
}

/// `RetryMint` + enqueue `SubmitMintJob` — shared by free-prepare-safe and
/// rebroadcast / replacement-authorized recovery paths under `MintingFailed`.
async fn authorize_resubmit(
    ctx: &MintRecoveryContext,
    issuer_request_id: &IssuerMintRequestId,
    vault: ResolvedVault,
) -> Result<(), MintRecoveryStepError> {
    ctx.mint_store
        .send(
            issuer_request_id,
            MintCommand::RetryMint {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await?;
    enqueue_submit(ctx, issuer_request_id, vault).await?;
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
/// the re-submission backoff. Only a non-`MintingFailed` state answers a
/// definite "no receipt"; an unresolved vault or an unreadable inventory is
/// "cannot tell" and must surface as an error. Answering `false` there would
/// abandon a mint whose deposit already succeeded, since retry exhaustion reads
/// `false` as proof that nothing was minted.
async fn minting_failed_receipt_exists(
    ctx: &MintRecoveryContext,
    mint: &Mint,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<bool, MintRecoveryStepError> {
    let Mint::MintingFailed { underlying, network, .. } = mint else {
        return Ok(false);
    };

    let vault = resolve_vault(ctx, underlying, *network).await?;

    ctx.receipts
        .find_by_issuer_request_id(
            vault.chain_id,
            &vault.address,
            issuer_request_id,
        )
        .await
        .map(|receipt| receipt.is_some())
        .map_err(Into::into)
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
    use alloy::primitives::{Address, B256, U256, address};
    use async_trait::async_trait;
    use chrono::Utc;
    use event_sorcery::test_store;
    use rust_decimal::Decimal;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::config::VaultMode;
    use crate::mint::api::test_utils::{
        TestAccountAndAsset, TestHarness, network_vault_services,
    };
    use crate::mint::tests::{
        VAULT, orchestrator_events_through_minting_authorized,
        orchestrator_events_through_tx_submitted, test_mint_authorization,
    };
    use crate::mint::{
        ClientId, IssuerMintRequestId, MintEvent, MintExternalTxId,
        MintFailureClassification, Network, Quantity, TokenSymbol,
        TokenizationRequestId, UnderlyingSymbol, has_unresolved_signer_intent,
    };
    use crate::receipt_inventory::{
        BurnPlan, BurnTrackingError, CqrsReceiptService, MintedReceiptParams,
        ReceiptInventory, ReceiptLookupError, ReceiptRegistrationError,
        RecoveredReceipt, Shares,
    };
    use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
    use crate::test_utils::{log_count_at, logs_contain_at};
    use crate::tokenized_asset::{AssetKey, TokenizedAssetCommand};
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        MintTxStatus, OrchestratorMintedLog, PreparedMintTx, VaultService,
    };

    /// Configurable `ReceiptService` stub for recovery tests. Only
    /// `find_by_issuer_request_id` is behavior-specific; other methods are
    /// no-ops so a future trait method needs one edit instead of three.
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
            issuer_request_id: &IssuerMintRequestId,
        ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError> {
            match self.0 {
                ReceiptLookupBehavior::Present => Ok(Some(RecoveredReceipt {
                    receipt_id: U256::from(1),
                    tx_hash: B256::ZERO,
                    shares: U256::from(100),
                    block_number: 1,
                })),
                ReceiptLookupBehavior::Fails => {
                    Err(ReceiptLookupError::Inconsistent {
                        issuer_request_id: issuer_request_id.clone(),
                        receipt_id: U256::from(1).into(),
                    })
                }
            }
        }
    }

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

        /// Override the vault service used by recovery (classification mocks).
        fn with_vault(self, vault: Arc<dyn VaultService>) -> Self {
            Self { vault_services: network_vault_services(vault), ..self }
        }

        /// Like [`Self::context`] but over a caller-configured vault mock —
        /// the reconciliation tests steer the `Minted`-log verdict.
        fn context_with_vault(
            &self,
            vault: Arc<dyn VaultService>,
        ) -> MintRecoveryContext {
            MintRecoveryContext {
                vault_services: network_vault_services(vault),
                ..self.context()
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

    /// True pre-intent failure (`Minting` only — never `MintTxIntended` /
    /// `MintTxSubmitted`). Recovery may free-prepare on RetryMint.
    fn pre_intent_failed_events(
        issuer_request_id: &IssuerMintRequestId,
        failed_at: chrono::DateTime<Utc>,
    ) -> Vec<MintEvent> {
        let mut events = minting_events(issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "prepare or pre-submit failure".to_string(),
            failed_at,
            classification: MintFailureClassification::Unclassified,
        });

        events
    }

    /// Legacy post-submit failure: `MintTxSubmitted` without a preceding
    /// `MintTxIntended`, so `prepared_tx` is `None` on the predecessor.
    /// Free-prepare is forbidden; recovery must fail closed.
    fn post_submit_without_prepared_failed_events(
        issuer_request_id: &IssuerMintRequestId,
        failed_at: chrono::DateTime<Utc>,
    ) -> Vec<MintEvent> {
        let mut events = tx_submitted_events(issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "terminal signer failure".to_string(),
            failed_at,
            classification: MintFailureClassification::Unclassified,
        });

        events
    }

    /// MintingFailed after TxIntended + TxSubmitted so `prepared_tx` is retained
    /// on the predecessor (load-bearing for rebroadcast vs fresh prepare).
    fn tx_failed_with_prepared_events(
        issuer_request_id: &IssuerMintRequestId,
        failed_at: chrono::DateTime<Utc>,
        prepared_tx: PreparedMintTx,
    ) -> Vec<MintEvent> {
        // failed_at must be after intent/submit so the fixture is causally
        // ordered (callers often pass Utc::now() - N minutes for backoff).
        let intended_at = failed_at - chrono::Duration::minutes(2);
        let submitted_at = failed_at - chrono::Duration::minutes(1);
        let mut events = minting_events(issuer_request_id);
        let hash = prepared_tx.hash;
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx,
            intended_at,
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(hash),
            submitted_at,
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "prior confirm uncertain or reverted".to_string(),
            failed_at,
            classification: MintFailureClassification::Unclassified,
        });
        events
    }

    async fn count_jobs_for_mint(
        pool: &Pool<Sqlite>,
        job_ty: &str,
        issuer_request_id: &IssuerMintRequestId,
    ) -> i64 {
        sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE job_type = ?
              AND idempotency_key = ?
            ",
        )
        .bind(job_ty)
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
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
                mint_mode: VaultMode::VaultDirect,
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
        let events = pre_intent_failed_events(&issuer_request_id, failed_at);
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
            false,
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
            "drive_one_step retries true pre-intent MintingFailed into Minting \
             and re-enqueues the submission, got: {}",
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

    #[traced_test]
    #[tokio::test]
    async fn manual_recovery_is_not_deduplicated_by_an_active_automatic_job() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();

        enqueue_scheduled_mint_recovery(
            &harness.pool,
            &harness.apalis_pool,
            issuer_request_id.clone(),
        )
        .await
        .unwrap();
        enqueue_manual_mint_recovery(
            &harness.pool,
            &harness.apalis_pool,
            issuer_request_id.clone(),
        )
        .await
        .unwrap();

        let idempotency_keys: Vec<String> = sqlx::query_scalar(
            "
            SELECT idempotency_key
            FROM Jobs
            WHERE job_type = ?
            ORDER BY idempotency_key
            ",
        )
        .bind(mint_recovery_job_type())
        .fetch_all(&harness.pool)
        .await
        .unwrap();

        assert_eq!(
            idempotency_keys,
            vec![
                issuer_request_id.to_string(),
                format!("{issuer_request_id}:manual"),
            ],
            "an active automatic recovery must not suppress the operator's \
             manual re-drive"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "manual_recovery_is_not_deduplicated_by_an_active_automatic_job",
                "Enqueued operator-authorized mint recovery",
                "idempotency_key",
                ":manual"
            ]
        ));
    }

    /// An authorization wake uses a distinct automatic key, so an already
    /// scheduled pass cannot swallow the signal that unblocks a waiting mint.
    #[traced_test]
    #[tokio::test]
    async fn authorization_recovery_is_distinct_and_remains_automatic() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();

        enqueue_scheduled_mint_recovery(
            &harness.pool,
            &harness.apalis_pool,
            issuer_request_id.clone(),
        )
        .await
        .unwrap();
        enqueue_authorized_mint_recovery(
            &harness.pool,
            &harness.apalis_pool,
            issuer_request_id.clone(),
        )
        .await
        .unwrap();

        let queued_jobs: Vec<(String, Vec<u8>)> = sqlx::query_as(
            "
            SELECT idempotency_key, job
            FROM Jobs
            WHERE job_type = ?
            ORDER BY idempotency_key
            ",
        )
        .bind(mint_recovery_job_type())
        .fetch_all(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            queued_jobs
                .iter()
                .map(|(idempotency_key, _)| idempotency_key)
                .collect::<Vec<_>>(),
            vec![
                &issuer_request_id.to_string(),
                &format!("{issuer_request_id}:authorization"),
            ]
        );

        let authorization_job: MintRecoveryJob =
            serde_json::from_slice(&queued_jobs[1].1).unwrap();
        assert!(
            !authorization_job.manual,
            "an authorization wake must remain an automatic recovery"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "authorization_recovery_is_distinct_and_remains_automatic",
                "Enqueued mint recovery after recording authorization",
                &format!("{issuer_request_id}:authorization")
            ]
        ));
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
        let events = pre_intent_failed_events(&issuer_request_id, failed_at);
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context(),
            &issuer_request_id,
            Duration::from_millis(1),
            2,
            false,
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

        let result = MintRecoveryJob { issuer_request_id, manual: false }
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
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::hours(2);
        let mut events = tx_submitted_events(&issuer_request_id);
        for _ in 0..5 {
            events.push(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
                classification: MintFailureClassification::Unclassified,
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
            false,
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
                classification: MintFailureClassification::Unclassified,
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
            false,
        )
        .await;

        assert!(matches!(
            lookup_failure,
            RecoveryConclusion::Abandoned {
                reason: AbandonReason::FailedToLoadReceipt
            }
        ));
    }

    /// A vault that cannot be resolved means "cannot tell whether a receipt
    /// exists", not "no receipt". Reporting `AutomaticRetriesExhausted` there
    /// would abandon a mint whose deposit may already have succeeded, and an
    /// abandoned mint is Killed, never requeued, and unrecoverable without an
    /// operator. The honest conclusion is the unreadable-inventory one.
    #[traced_test]
    #[tokio::test]
    async fn exhausted_retries_with_unresolvable_vault_do_not_report_exhausted()
    {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::hours(2);
        // The fixture registers a vault for AAPL/Base only, so a mint on an
        // unregistered underlying makes `resolve_vault` fail.
        let mut events = tx_submitted_events(&issuer_request_id);
        if let Some(MintEvent::Initiated { underlying, token, .. }) =
            events.first_mut()
        {
            *underlying = UnderlyingSymbol::new("MSFT").unwrap();
            *token = TokenSymbol::new("tMSFT");
        }
        for _ in 0..5 {
            events.push(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
                classification: MintFailureClassification::Unclassified,
            });
        }
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context(),
            &issuer_request_id,
            Duration::from_millis(1),
            3,
            false,
        )
        .await;

        assert!(
            matches!(
                conclusion,
                RecoveryConclusion::Abandoned {
                    reason: AbandonReason::FailedToLoadReceipt
                }
            ),
            "an unresolvable vault must abandon as unreadable inventory, not \
             as retry exhaustion"
        );

        let test =
            "exhausted_retries_with_unresolvable_vault_do_not_report_exhausted";
        assert!(
            log_count_at!(
                Level::WARN,
                &[
                    test,
                    "Failed to read receipt inventory after maximum backoffs"
                ]
            ) >= 1,
            "giving up on an unreadable inventory must log the WARN"
        );
        assert!(
            log_count_at!(
                Level::WARN,
                &[test, "Automatic mint retries exhausted"]
            ) == 0,
            "an unreadable inventory must never be reported as retry exhaustion"
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
                classification: MintFailureClassification::Unclassified,
            });
        }
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let error = MintRecoveryJob { issuer_request_id, manual: false }
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
                    mint_mode: VaultMode::VaultDirect,
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

    /// A prepare-stage failure (no transaction ever signed or broadcast) is
    /// the unambiguous case: the manual retry must transition the mint back
    /// to `Minting` and enqueue a fresh `SubmitMintJob`.
    #[traced_test]
    #[tokio::test]
    async fn manual_retry_drives_a_prepare_failed_mint_to_a_fresh_submit() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();
        seed_recoverable_mint(&harness, &issuer_request_id).await;

        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Deposit {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::RecordMintFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    error: "Turnkey HTTP response was not successful: 403"
                        .to_string(),
                    classification: MintFailureClassification::Unclassified,
                },
            )
            .await
            .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        let outcome = manually_retry_failed_mint(
            &harness.mint_store,
            &harness.pool,
            &network_vault_services(harness.vault.clone()),
            &harness.apalis_pool,
            &issuer_request_id,
            &mint,
        )
        .await
        .unwrap();

        assert_eq!(outcome, ManualRetryOutcome::Enqueued);
        let retried =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert_eq!(retried.state_name(), "Minting");

        let queued: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key = ?
            ",
        )
        .bind(job_type::<SubmitMintJob>())
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            queued, 1,
            "the manual retry must enqueue exactly one fresh submit job"
        );

        let test =
            "manual_retry_drives_a_prepare_failed_mint_to_a_fresh_submit";
        assert_eq!(
            log_count_at!(
                Level::INFO,
                &[
                    test,
                    "Manually retrying failed mint past the automatic budget"
                ]
            ),
            1
        );
    }

    /// Once `ManualRetryMint` commits, an immediate queue failure is partial
    /// success rather than a failed operator command: the durable state is now
    /// recoverable and the periodic reconciler owns retrying its dispatch.
    #[traced_test]
    #[tokio::test]
    async fn manual_retry_reports_deferred_dispatch_after_the_transition_commits()
     {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();
        seed_recoverable_mint(&harness, &issuer_request_id).await;

        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Deposit {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::RecordMintFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    error: "prepare failed".to_string(),
                    classification: MintFailureClassification::Unclassified,
                },
            )
            .await
            .unwrap();
        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        sqlx::query("DROP TABLE Jobs").execute(&harness.pool).await.unwrap();

        let outcome = manually_retry_failed_mint(
            &harness.mint_store,
            &harness.pool,
            &network_vault_services(harness.vault.clone()),
            &harness.apalis_pool,
            &issuer_request_id,
            &mint,
        )
        .await
        .unwrap();

        assert!(
            matches!(
                outcome,
                ManualRetryOutcome::DeferredToRecovery { ref error }
                    if error.contains("Jobs")
            ),
            "the committed retry must expose deferred dispatch, got {outcome:?}"
        );
        assert_eq!(
            harness
                .mint_store
                .load(&issuer_request_id)
                .await
                .unwrap()
                .unwrap()
                .state_name(),
            "Minting",
            "the operator command committed before queue dispatch failed"
        );

        let test = "manual_retry_reports_deferred_dispatch_after_the_transition_commits";
        assert!(
            log_count_at!(
                Level::WARN,
                &[
                    test,
                    "Manual retry committed; deferring queue dispatch to \
                     reconciler"
                ]
            ) >= 1,
            "a committed retry whose dispatch fails must log the deferral \
             for operators"
        );
    }

    /// A stale endpoint snapshot must not release and recreate queue state when
    /// another request already committed the manual retry transition.
    #[traced_test]
    #[tokio::test]
    async fn stale_manual_retry_does_not_resurrect_a_terminal_submit_job() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();
        seed_recoverable_mint(&harness, &issuer_request_id).await;
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Deposit {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::RecordMintFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    error: "prepare failed".to_string(),
                    classification: MintFailureClassification::Unclassified,
                },
            )
            .await
            .unwrap();
        let stale =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        JobQueue::<SubmitMintJob>::new(&harness.apalis_pool)
            .push_with_idempotency_key(
                SubmitMintJob {
                    issuer_request_id: issuer_request_id.clone(),
                    vault: VAULT,
                    chain_id: Network::Base.chain_id(),
                },
                issuer_request_id.to_string(),
            )
            .await
            .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Done' WHERE job_type = ? \
             AND idempotency_key = ?",
        )
        .bind(job_type::<SubmitMintJob>())
        .bind(issuer_request_id.to_string())
        .execute(&harness.pool)
        .await
        .unwrap();
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::ManualRetryMint {
                    issuer_request_id: issuer_request_id.clone(),
                    manual_retry_id: Uuid::new_v4(),
                },
            )
            .await
            .unwrap();

        let outcome = manually_retry_failed_mint(
            &harness.mint_store,
            &harness.pool,
            &network_vault_services(harness.vault.clone()),
            &harness.apalis_pool,
            &issuer_request_id,
            &stale,
        )
        .await
        .unwrap();

        assert_eq!(outcome, ManualRetryOutcome::AlreadyHandled);
        let status: String = sqlx::query_scalar(
            "SELECT status FROM Jobs WHERE job_type = ? AND idempotency_key = ?",
        )
        .bind(job_type::<SubmitMintJob>())
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            status, "Done",
            "a no-op retry command must not resurrect terminal queue state"
        );

        let test =
            "stale_manual_retry_does_not_resurrect_a_terminal_submit_job";
        assert!(
            log_count_at!(
                Level::INFO,
                &[
                    test,
                    "Manual retry was already handled; leaving queue state \
                     unchanged"
                ]
            ) >= 1,
            "a stale retry that lost the commit race must log the no-op \
             outcome for operators"
        );
    }

    /// A failure chain that contains a prepared or submitted transaction is
    /// ambiguous — the prior transaction may still mine — so the manual
    /// retry must refuse rather than risk a double mint.
    #[tokio::test]
    async fn manual_retry_refuses_a_chain_with_a_submitted_transaction() {
        let harness = TestHarness::new().await;
        let issuer_request_id = test_issuer_request_id();
        seed_recoverable_mint(&harness, &issuer_request_id).await;

        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Deposit {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::RecordTxSubmitted {
                    issuer_request_id: issuer_request_id.clone(),
                    external_tx_id: MintExternalTxId::from_string(format!(
                        "mint-{issuer_request_id}"
                    )),
                    tx_id: TxId::Legacy("fb-ambiguous".to_string()),
                },
            )
            .await
            .unwrap();
        harness
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::RecordMintFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    error: "confirmation timed out".to_string(),
                    classification: MintFailureClassification::Unclassified,
                },
            )
            .await
            .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        let error = manually_retry_failed_mint(
            &harness.mint_store,
            &harness.pool,
            &network_vault_services(harness.vault.clone()),
            &harness.apalis_pool,
            &issuer_request_id,
            &mint,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("could double-mint"),
            "expected the double-mint refusal, got: {error}"
        );
        assert_eq!(
            harness
                .mint_store
                .load(&issuer_request_id)
                .await
                .unwrap()
                .unwrap()
                .state_name(),
            "MintingFailed",
            "a refused manual retry must not advance the mint"
        );
    }
    /// Uncertain classify must not RetryMint — leave MintingFailed so a second
    /// deposit is never prepared under observation failure.
    #[traced_test]
    #[tokio::test]
    async fn drive_one_step_uncertain_classify_does_not_retry_mint() {
        let issuer_request_id = test_issuer_request_id();
        let prepared = PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_with_prepared_events(
            &issuer_request_id,
            failed_at,
            prepared,
        );

        // Replace the default success mock with one that fails classification.
        let vault = Arc::new(
            MockVaultService::new_success()
                .with_mint_tx_classification_failure(),
        );
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        drive_one_step(
            &fixture.context(),
            &fixture
                .mint_store
                .load(&issuer_request_id)
                .await
                .unwrap()
                .unwrap(),
            &issuer_request_id,
        )
        .await
        .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "uncertain classify must not RetryMint, got: {}",
            mint.state_name()
        );
        assert_eq!(
            vault.mint_classification_call_count(),
            1,
            "must classify under wallet guard before any retry"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "must not prepare a new mint under uncertain classification"
        );

        let test = "drive_one_step_uncertain_classify_does_not_retry_mint";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "classification uncertain", "not replacing"]
        ));
        // Prepared identity still recoverable for a later successful observe.
        assert_eq!(
            mint.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash)
        );
    }

    /// StillMineable rebroadcast path: RetryMint + submit reuses same hash
    /// (pending_prepared_tx), never prepare_mint_tx a different identity.
    #[traced_test]
    #[tokio::test]
    async fn drive_one_step_still_mineable_rebroadcasts_same_hash() {
        let issuer_request_id = test_issuer_request_id();
        let prepared = PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_with_prepared_events(
            &issuer_request_id,
            failed_at,
            prepared,
        );

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_mint_tx_status(MintTxStatus::StillMineable),
        );
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "StillMineable recovery RetryMints into Minting for rebroadcast, got: {}",
            mint.state_name()
        );
        assert_eq!(
            mint.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash),
            "rebroadcast path must keep the same prepared hash"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "drive_one_step must not prepare; SubmitMintJob rebroadcasts"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<SubmitMintJob>(),
                &issuer_request_id,
            )
            .await,
            1,
            "StillMineable rebroadcast must enqueue SubmitMintJob"
        );
    }

    /// ProvablyDead allows RetryMint; SubmitMintJob may prepare a replacement.
    #[tokio::test]
    async fn drive_one_step_provably_dead_allows_retry() {
        let issuer_request_id = test_issuer_request_id();
        let prepared = PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_with_prepared_events(
            &issuer_request_id,
            failed_at,
            prepared,
        );

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_mint_tx_status(MintTxStatus::ProvablyDead),
        );
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "ProvablyDead must allow RetryMint, got: {}",
            mint.state_name()
        );
        // Two classify calls: initial + TOCTOU recheck.
        assert_eq!(vault.mint_classification_call_count(), 2);
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<SubmitMintJob>(),
                &issuer_request_id,
            )
            .await,
            1,
            "ProvablyDead replacement path must enqueue SubmitMintJob"
        );
    }

    /// TOCTOU: first classify ProvablyDead, recheck StillMineable → no
    /// RetryMint (identity may still mine; never authorize replacement).
    #[traced_test]
    #[tokio::test]
    async fn drive_one_step_toctou_provably_dead_then_still_mineable_no_retry()
    {
        let issuer_request_id = test_issuer_request_id();
        let prepared = PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_with_prepared_events(
            &issuer_request_id,
            failed_at,
            prepared,
        );

        let vault = Arc::new(
            MockVaultService::new_success().with_mint_tx_status_sequence(vec![
                MintTxStatus::ProvablyDead,
                MintTxStatus::StillMineable,
            ]),
        );
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "TOCTOU recheck StillMineable must not RetryMint, got: {}",
            mint.state_name()
        );
        assert_eq!(
            vault.mint_classification_call_count(),
            2,
            "must classify then recheck under the wallet guard"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "TOCTOU abort must not prepare a replacement"
        );
        assert_eq!(
            mint.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash)
        );

        let test =
            "drive_one_step_toctou_provably_dead_then_still_mineable_no_retry";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "recheck no longer terminal", "not replacing"]
        ));
    }

    /// Restart mid-`TxSubmitted`: recovery re-enqueues confirm for the same
    /// prepared hash and never prepares a replacement.
    #[tokio::test]
    async fn drive_one_step_tx_submitted_restart_reuses_same_hash() {
        let issuer_request_id = test_issuer_request_id();
        let prepared = PreparedMintTx::valid_for_test(
            7,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let now = Utc::now();
        let mut events = minting_events(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: prepared,
            intended_at: now,
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(expected_hash),
            submitted_at: now,
        });

        let vault = Arc::new(MockVaultService::new_success());
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint_before, Mint::TxSubmitted { .. }),
            "fixture must start TxSubmitted, got {}",
            mint_before.state_name()
        );
        assert_eq!(
            mint_before.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash)
        );

        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "restart confirm enqueue must not change state, got {}",
            mint.state_name()
        );
        assert_eq!(
            mint.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash),
            "restart recovery must keep the same prepared hash"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "drive_one_step must only enqueue ConfirmMintJob, not prepare"
        );

        let confirm_jobs: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE job_type = ?
              AND idempotency_key = ?
            ",
        )
        .bind(job_type::<ConfirmMintJob>())
        .bind(issuer_request_id.to_string())
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(
            confirm_jobs, 1,
            "restart must enqueue exactly one confirm job"
        );
    }

    /// Post-submit identity without prepared bytes must never free-prepare:
    /// leave MintingFailed, log ERROR for ops, and enqueue confirm poll only.
    #[traced_test]
    #[tokio::test]
    async fn drive_one_step_post_submit_without_prepared_fails_closed() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = post_submit_without_prepared_failed_events(
            &issuer_request_id,
            failed_at,
        );

        let vault = Arc::new(MockVaultService::new_success());
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            mint_before.pending_prepared_tx().is_none(),
            "fixture is legacy TxSubmitted without prepared_tx"
        );
        assert!(
            mint_before.has_unclassifiable_post_intent_identity(),
            "fixture must be recognized as post-intent without prepared bytes"
        );

        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "must not RetryMint into free-prepare path, got: {}",
            mint.state_name()
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "must never prepare_mint_tx for unclassifiable post-submit identity"
        );

        let confirm_jobs: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE job_type = ?
              AND idempotency_key = ?
            ",
        )
        .bind(job_type::<ConfirmMintJob>())
        .bind(issuer_request_id.to_string())
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(
            confirm_jobs, 1,
            "fail-closed path must still enqueue confirm poll for known tx_id"
        );

        // DEBUG, not ERROR: this step repeats on every recovery poll and the
        // confirm poll above keeps re-observing, so the mint is not stuck
        // awaiting an operator.
        let test = "drive_one_step_post_submit_without_prepared_fails_closed";
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[test, "without prepared_tx", "never free-prepare"]
        ));
    }

    /// MinedSuccess under wallet-guard recovery keeps the same prepared hash
    /// and enqueues ConfirmMintJob (not submit) so ExistingMint can be recorded
    /// without rebroadcasting a mined deposit.
    #[tokio::test]
    async fn drive_one_step_mined_success_keeps_same_prepared_hash() {
        let issuer_request_id = test_issuer_request_id();
        let prepared = PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_with_prepared_events(
            &issuer_request_id,
            failed_at,
            prepared,
        );

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_mint_tx_status(MintTxStatus::MinedSuccess),
        );
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "MinedSuccess must stay MintingFailed for confirm re-observe, got: {}",
            mint.state_name()
        );
        assert_eq!(
            mint.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash),
            "must not prepare a different hash after MinedSuccess classify"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "drive_one_step must not call prepare_mint_tx"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<ConfirmMintJob>(),
                &issuer_request_id,
            )
            .await,
            1,
            "MinedSuccess must enqueue ConfirmMintJob, not SubmitMintJob"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<SubmitMintJob>(),
                &issuer_request_id,
            )
            .await,
            0,
            "must not enqueue submit after MinedSuccess"
        );
    }

    /// MinedReverted allows RetryMint + SubmitMintJob (replacement after
    /// terminal death), mirroring ProvablyDead.
    #[tokio::test]
    async fn drive_one_step_mined_reverted_allows_retry() {
        let issuer_request_id = test_issuer_request_id();
        let prepared = PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_with_prepared_events(
            &issuer_request_id,
            failed_at,
            prepared,
        );

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_mint_tx_status(MintTxStatus::MinedReverted),
        );
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "MinedReverted must allow RetryMint, got: {}",
            mint.state_name()
        );
        assert_eq!(
            vault.mint_classification_call_count(),
            2,
            "must classify then recheck under the wallet guard"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<SubmitMintJob>(),
                &issuer_request_id,
            )
            .await,
            1,
            "MinedReverted replacement path must enqueue SubmitMintJob"
        );
    }

    /// Post-submit MintingFailed with inventory receipt: RetryMint so
    /// SubmitMintJob can record ExistingMint (never free-prepare a new deposit).
    #[traced_test]
    #[tokio::test]
    async fn drive_one_step_post_submit_with_inventory_receipt_retries() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = post_submit_without_prepared_failed_events(
            &issuer_request_id,
            failed_at,
        );

        let vault = Arc::new(MockVaultService::new_success());
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mut ctx = fixture.context();
        ctx.receipts =
            Arc::new(ReceiptLookupStub(ReceiptLookupBehavior::Present));

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&ctx, &mint_before, &issuer_request_id).await.unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "inventory-present post-submit must RetryMint into Minting, got: {}",
            mint.state_name()
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "RetryMint must not prepare_mint_tx in recovery; SubmitMintJob records ExistingMint"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<SubmitMintJob>(),
                &issuer_request_id,
            )
            .await,
            1,
            "inventory-present path must enqueue SubmitMintJob"
        );

        let test = "drive_one_step_post_submit_with_inventory_receipt_retries";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "has receipt", "RetryMint"]
        ));
    }

    /// Inventory lookup Err on post-submit without prepared bytes: refuse
    /// free-prepare, stay MintingFailed, still enqueue confirm poll.
    #[traced_test]
    #[tokio::test]
    async fn drive_one_step_post_submit_inventory_err_fails_closed() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = post_submit_without_prepared_failed_events(
            &issuer_request_id,
            failed_at,
        );

        let vault = Arc::new(MockVaultService::new_success());
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mut ctx = fixture.context();
        ctx.receipts =
            Arc::new(ReceiptLookupStub(ReceiptLookupBehavior::Fails));

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&ctx, &mint_before, &issuer_request_id).await.unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "inventory Err must not RetryMint free-prepare, got: {}",
            mint.state_name()
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "must never prepare_mint_tx when inventory lookup fails"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<ConfirmMintJob>(),
                &issuer_request_id,
            )
            .await,
            1,
            "fail-closed path must still enqueue confirm poll for known tx_id"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<SubmitMintJob>(),
                &issuer_request_id,
            )
            .await,
            0,
            "must not enqueue submit when inventory is unreadable"
        );

        let test = "drive_one_step_post_submit_inventory_err_fails_closed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Inventory lookup failed", "fail closed"]
        ));
    }

    /// Corrupt prepared identity: recover_signer fails closed without RetryMint
    /// or enqueue (cannot prove death without a valid envelope).
    #[traced_test]
    #[tokio::test]
    async fn drive_one_step_recover_signer_failure_fails_closed() {
        let issuer_request_id = test_issuer_request_id();
        let mut prepared = PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        // Invalidate hash so validate()/recover_signer fails before RPC.
        prepared.hash = B256::ZERO;
        let expected_hash = prepared.hash;
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = tx_failed_with_prepared_events(
            &issuer_request_id,
            failed_at,
            prepared,
        );

        let vault = Arc::new(MockVaultService::new_success());
        let fixture =
            MintRecoveryFixture::new().await.with_vault(vault.clone());
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let mint_before =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        drive_one_step(&fixture.context(), &mint_before, &issuer_request_id)
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "recover_signer failure must leave MintingFailed, got: {}",
            mint.state_name()
        );
        assert_eq!(
            vault.mint_classification_call_count(),
            0,
            "must not classify when signer recovery fails"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "must not prepare on corrupt identity"
        );
        assert_eq!(
            count_jobs_for_mint(
                &fixture.pool,
                job_type::<SubmitMintJob>(),
                &issuer_request_id,
            )
            .await,
            0,
            "must not enqueue submit on corrupt prepared identity"
        );
        assert_eq!(
            mint.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash)
        );

        let test = "drive_one_step_recover_signer_failure_fails_closed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Cannot recover signer", "fail closed"]
        ));
    }

    /// Typed classifications are never auto-retried: the scheduled loop
    /// retires as `Resolved` before any wakeup/budget bookkeeping, leaves
    /// the classified failure untouched, and enqueues no per-state job. The
    /// `failed_at` is far in the past, so an `Unclassified` failure would be
    /// `Ready` — proving the skip comes from the classification, not the
    /// retry window (the no-progress test above is that contrast: same-age
    /// `Unclassified` failures keep driving).
    #[traced_test]
    #[tokio::test]
    async fn scheduled_recovery_skips_classified_mint_failure() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::hours(24);
        let mut events =
            orchestrator_events_through_tx_submitted(&issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "confirmation failed".to_string(),
            failed_at,
            classification: MintFailureClassification::VaultLogicMismatch,
        });
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context(),
            &issuer_request_id,
            Duration::from_millis(5),
            8,
            false,
        )
        .await;

        assert!(
            matches!(conclusion, RecoveryConclusion::Resolved),
            "a classified failure must retire the job as Resolved"
        );
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                mint,
                Mint::MintingFailed {
                    classification:
                        MintFailureClassification::VaultLogicMismatch,
                    ..
                }
            ),
            "the classified failure must be left untouched, got: {}",
            mint.state_name()
        );

        let queued_jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(
            queued_jobs, 0,
            "the classified skip must not enqueue any per-state job"
        );

        let test = "scheduled_recovery_skips_classified_mint_failure";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Skipping non-retryable classified mint failure"]
        ));
    }

    /// The admin reprocess path reaches the same recovery loop that skips
    /// classified failures, so its job carries the `manual` flag: one
    /// re-drive is spent on the classified mint (`RetryMint` fires, a
    /// submission job is enqueued) instead of retiring as a silent no-op —
    /// the endpoint's "recovery initiated" answer must mean something ran.
    #[traced_test]
    #[tokio::test]
    async fn manual_recovery_redrives_classified_mint_failure_once() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::hours(24);
        let mut events =
            orchestrator_events_through_tx_submitted(&issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "nonce consumed by another mint".to_string(),
            failed_at,
            classification: MintFailureClassification::NonceConsumedByOtherMint,
        });
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context(),
            &issuer_request_id,
            Duration::from_millis(5),
            8,
            true,
        )
        .await;

        // No worker drains the enqueued submission in this fixture, so the
        // loop exhausts its no-progress budget — the point is that the
        // re-drive actually ran, unlike the automatic path's silent skip.
        assert!(
            matches!(
                conclusion,
                RecoveryConclusion::Abandoned {
                    reason: AbandonReason::NoProgressBudgetExhausted
                }
            ),
            "expected the fixture loop to stop on its no-progress budget, \
             got {conclusion:?}"
        );
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "the operator re-drive must retry the classified failure back \
             into Minting, got: {}",
            mint.state_name()
        );

        let submit_jobs: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE job_type = ?
            ",
        )
        .bind(job_type::<SubmitMintJob>())
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(
            submit_jobs, 1,
            "the manual re-drive must enqueue exactly one submission job"
        );

        let test = "manual_recovery_redrives_classified_mint_failure_once";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "Operator-requested re-drive"]
        ));
    }

    /// A deterministic recipient-authorization failure resubmitted with the
    /// same stored authorization reverts identically — the re-drive must be
    /// refused outright (no `RetryMint`, no submission job, no wallet nonce
    /// burned), leaving `CloseMint` + a fresh `Initiate` as the only path.
    #[traced_test]
    #[tokio::test]
    async fn manual_recovery_refuses_deterministic_authorization_failure() {
        let issuer_request_id = test_issuer_request_id();
        let mut events =
            orchestrator_events_through_tx_submitted(&issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "BadRecipientSignature()".to_string(),
            failed_at: Utc::now() - chrono::Duration::hours(24),
            classification: MintFailureClassification::BadRecipientSignature,
        });
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context(),
            &issuer_request_id,
            Duration::from_millis(5),
            8,
            true,
        )
        .await;

        assert!(
            matches!(conclusion, RecoveryConclusion::Resolved),
            "the refused re-drive must retire the job cleanly, got \
             {conclusion:?}"
        );
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                mint,
                Mint::MintingFailed {
                    classification:
                        MintFailureClassification::BadRecipientSignature,
                    ..
                }
            ),
            "the deterministic failure must be left untouched, got: {}",
            mint.state_name()
        );

        let queued_jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(
            queued_jobs, 0,
            "a refused re-drive must not enqueue any submission"
        );

        let test =
            "manual_recovery_refuses_deterministic_authorization_failure";
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                test,
                "Refusing re-drive of a deterministic",
                "re-initiate with fresh authorization"
            ]
        ));
    }

    fn unresolved_replay_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events =
            orchestrator_events_through_tx_submitted(issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "nonce consumed with no log at the pair".to_string(),
            failed_at: Utc::now() - chrono::Duration::hours(24),
            classification: MintFailureClassification::NonceReplayUnresolved,
        });
        events
    }

    /// A widened reconciliation that full-matches resolves the unresolved
    /// replay forward: the landed mint is recorded and the loop keeps
    /// driving it (the callback job is enqueued) — no resubmission ever
    /// happens.
    #[traced_test]
    #[tokio::test]
    async fn reconciliation_resolves_unresolved_replay_forward() {
        let issuer_request_id = test_issuer_request_id();
        let fixture = MintRecoveryFixture::new().await;
        fixture
            .seed_mint_events(
                &issuer_request_id,
                unresolved_replay_events(&issuer_request_id),
            )
            .await;

        let vault = Arc::new(MockVaultService::new_success().with_minted_log(
            OrchestratorMintedLog {
                tx_hash: B256::repeat_byte(0xcc),
                nonce: test_mint_authorization().nonce,
                shares_minted: U256::from(100u64)
                    * U256::from(10u64).pow(U256::from(18u64)),
                block_number: 777,
            },
        ));

        recover_mint_until_automatic_budget_exhausted(
            &fixture.context_with_vault(vault.clone()),
            &issuer_request_id,
            Duration::from_millis(5),
            2,
            false,
        )
        .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "a full-matched reconciliation must resolve the mint forward, \
             got: {}",
            mint.state_name()
        );
        let callback_jobs: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE job_type = ?
            ",
        )
        .bind(job_type::<SendCallbackJob>())
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(
            callback_jobs, 1,
            "the resolved mint must be driven on to its callback"
        );

        let test = "reconciliation_resolves_unresolved_replay_forward";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "Widened reconciliation full-matched"]
        ));
    }

    /// Full-path regression for the release-trigger migration: a crash after
    /// `MintTxIntended` leaves the signer reservation held with no
    /// `MintTxSubmitted` ever committing; when reconciliation proves the
    /// landing and records `OrchestratorMintRecovered`, the reservation must
    /// release — otherwise every later mint and burn on the network is
    /// rejected until an operator close.
    #[traced_test]
    #[tokio::test]
    async fn reconciled_recovery_releases_the_held_signer_reservation() {
        let issuer_request_id = test_issuer_request_id();
        let fixture = MintRecoveryFixture::new().await;
        let mut events =
            orchestrator_events_through_minting_authorized(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: PreparedMintTx::valid_for_test(
                1,
                format!("mint-{issuer_request_id}"),
            ),
            intended_at: Utc::now(),
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "nonce consumed with no log at the pair".to_string(),
            failed_at: Utc::now() - chrono::Duration::hours(24),
            classification: MintFailureClassification::NonceReplayUnresolved,
        });
        fixture.seed_mint_events(&issuer_request_id, events).await;

        assert!(
            has_unresolved_signer_intent(&fixture.pool, Network::Base, None)
                .await
                .expect("intent query should succeed"),
            "the crash-after-intent history must hold the reservation"
        );

        let vault = Arc::new(MockVaultService::new_success().with_minted_log(
            OrchestratorMintedLog {
                tx_hash: B256::repeat_byte(0xcc),
                nonce: test_mint_authorization().nonce,
                shares_minted: U256::from(100u64)
                    * U256::from(10u64).pow(U256::from(18u64)),
                block_number: 777,
            },
        ));

        recover_mint_until_automatic_budget_exhausted(
            &fixture.context_with_vault(vault),
            &issuer_request_id,
            Duration::from_millis(5),
            2,
            false,
        )
        .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "the full-matched reconciliation must resolve the mint forward, \
             got: {}",
            mint.state_name()
        );
        assert!(
            !has_unresolved_signer_intent(&fixture.pool, Network::Base, None)
                .await
                .expect("intent query should succeed"),
            "OrchestratorMintRecovered must release the signer reservation"
        );
    }

    /// A widened reconciliation that finds the pair's landing disagreeing on
    /// amount upgrades the verdict to the PROVEN `NonceConsumedByOtherMint`
    /// — which then parks manual-only; nothing is ever resubmitted.
    #[traced_test]
    #[tokio::test]
    async fn reconciliation_upgrades_unresolved_replay_to_proven_mismatch() {
        let issuer_request_id = test_issuer_request_id();
        let fixture = MintRecoveryFixture::new().await;
        fixture
            .seed_mint_events(
                &issuer_request_id,
                unresolved_replay_events(&issuer_request_id),
            )
            .await;

        let vault = Arc::new(MockVaultService::new_success().with_minted_log(
            OrchestratorMintedLog {
                tx_hash: B256::repeat_byte(0xcc),
                nonce: test_mint_authorization().nonce,
                shares_minted: U256::from(1u8),
                block_number: 777,
            },
        ));

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context_with_vault(vault.clone()),
            &issuer_request_id,
            Duration::from_millis(5),
            4,
            false,
        )
        .await;

        assert!(
            matches!(conclusion, RecoveryConclusion::Resolved),
            "the upgraded verdict must retire via the manual-only skip, got \
             {conclusion:?}"
        );
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                mint,
                Mint::MintingFailed {
                    classification:
                        MintFailureClassification::NonceConsumedByOtherMint,
                    ..
                }
            ),
            "the mismatching landing must upgrade the classification, got: \
             {mint:?}"
        );
        let queued_jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(
            queued_jobs, 0,
            "an upgraded verdict must never enqueue a submission"
        );

        let test =
            "reconciliation_upgrades_unresolved_replay_to_proven_mismatch";
        assert!(logs_contain_at!(
            Level::ERROR,
            &[test, "proved the nonce was consumed by a different mint"]
        ));
    }

    /// A widened reconciliation that still finds nothing leaves the mint
    /// parked — one query per pass, no state change, no submission — for
    /// the reconciler's next pass.
    #[traced_test]
    #[tokio::test]
    async fn reconciliation_leaves_still_unresolved_replay_parked() {
        let issuer_request_id = test_issuer_request_id();
        let fixture = MintRecoveryFixture::new().await;
        fixture
            .seed_mint_events(
                &issuer_request_id,
                unresolved_replay_events(&issuer_request_id),
            )
            .await;

        let vault = Arc::new(MockVaultService::new_success());
        let conclusion = recover_mint_until_automatic_budget_exhausted(
            &fixture.context_with_vault(vault.clone()),
            &issuer_request_id,
            Duration::from_millis(5),
            4,
            false,
        )
        .await;

        assert!(
            matches!(conclusion, RecoveryConclusion::Resolved),
            "a still-unresolved replay must retire the pass cleanly, got \
             {conclusion:?}"
        );
        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                mint,
                Mint::MintingFailed {
                    classification:
                        MintFailureClassification::NonceReplayUnresolved,
                    ..
                }
            ),
            "a still-empty scan must leave the mint parked unchanged, got: \
             {mint:?}"
        );
        let queued_jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(
            queued_jobs, 0,
            "an unresolved replay must never enqueue a submission"
        );

        let test = "reconciliation_leaves_still_unresolved_replay_parked";
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[test, "still found no Minted log"]
        ));
    }
}
