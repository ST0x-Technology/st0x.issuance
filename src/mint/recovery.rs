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
    MintRecoveryMode,
};
use crate::jobs::{Job, JobQueue};
use crate::receipt_inventory::ItnReceiptHandler;

/// Production handler that triggers mint recovery when an ITN receipt is
/// discovered by the receipt monitor.
#[derive(Clone)]
pub(crate) struct MintRecoveryHandler {
    mint_store: Arc<Store<Mint>>,
}

impl MintRecoveryHandler {
    pub(crate) const fn new(mint_store: Arc<Store<Mint>>) -> Self {
        Self { mint_store }
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
        tokio::spawn(async move {
            drive_recovery(&mint_store, issuer_request_id, |id| {
                MintCommand::RecoverFromReceipt {
                    issuer_request_id: id,
                    tx_hash,
                }
            })
            .await;
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
    /// The previously submitted Fireblocks transaction is still pending.
    Pending,
    /// Automatic retry budget is exhausted.
    Exhausted,
    /// A command failed unexpectedly, or recovery did not converge.
    Failed,
}

/// Drives a mint through recovery to completion using `MintCommand::Recover`.
pub(crate) async fn recover_mint(
    mint_store: &Store<Mint>,
    issuer_request_id: IssuerMintRequestId,
) -> DriveOutcome {
    drive_recovery(mint_store, issuer_request_id, |id| MintCommand::Recover {
        issuer_request_id: id,
        mode: MintRecoveryMode::Automatic,
    })
    .await
}

/// Fixed backoff applied when a scheduled recovery pass cannot make progress —
/// the previously submitted Fireblocks tx is still pending, or a transient
/// error (e.g. RPC blip) occurred. Keeps the loop from spinning while waiting.
const SCHEDULED_RECOVERY_BACKOFF: Duration = Duration::from_secs(60);

/// Budget for retry-window wakeups (`Wait` / `RetryNotDue`). The automatic
/// schedule already terminates healthy retries via `Exhausted` after the
/// attempt cap; this bounds the degenerate case where a mint keeps re-failing
/// at the same attempt (e.g. submission errors before Fireblocks acceptance),
/// so the task gives up and the next restart re-picks it instead of looping.
const MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS: usize =
    (Mint::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT as usize) * 2 + 4;

/// Budget for consecutive transient-failure backoffs (e.g. RPC blips) before
/// giving up. Small: a persistent error should surface for investigation, not
/// be hammered indefinitely. The next restart re-picks the mint.
const MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS: usize = 8;

/// Budget for polling a still-pending Fireblocks tx. A pending tx is a healthy
/// state (queued, awaiting policy approval, broadcasting, confirming) that we do
/// not control, so this is sized generously — ~6h at [`SCHEDULED_RECOVERY_BACKOFF`]
/// — separate from the transient-failure budget, so a slow finalization is not
/// abandoned prematurely.
const MAX_SCHEDULED_RECOVERY_PENDING_POLLS: usize = 360;

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

impl Job<Store<Mint>> for MintRecoveryJob {
    type Output = ();
    type Error = AbortError;

    async fn perform(
        &self,
        mint_store: &Store<Mint>,
    ) -> Result<(), AbortError> {
        let issuer_request_id = self.issuer_request_id.clone();

        match recover_mint_until_automatic_budget_exhausted(
            mint_store,
            issuer_request_id.clone(),
            SCHEDULED_RECOVERY_BACKOFF,
            MAX_SCHEDULED_RECOVERY_PENDING_POLLS,
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
                error!(target: "mint", issuer_request_id = %issuer_request_id,
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

/// Why [`recover_mint_until_automatic_budget_exhausted`] abandoned a mint while
/// it was still incomplete. A closed set of causes, so callers match on the
/// variant rather than comparing free-text strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbandonReason {
    /// The mint could not be loaded after the maximum load-failure backoffs.
    FailedToLoadMint,
    /// The aggregate's automatic-retry attempts ran out.
    AutomaticRetriesExhausted,
    /// The pending-Fireblocks-tx poll budget was spent.
    PendingPollBudgetExhausted,
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
            Self::PendingPollBudgetExhausted => "pending poll budget exhausted",
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
    issuer_request_id: IssuerMintRequestId,
    backoff: Duration,
    max_pending_polls: usize,
) -> RecoveryConclusion {
    let mut retry_wakeups = 0;
    let mut failure_backoffs = 0;
    let mut pending_polls = 0;
    let mut polled_tx_id: Option<String> = None;

    loop {
        let mint = match mint_store.load(&issuer_request_id).await {
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

        // Give each distinct Fireblocks transaction its own pending-poll
        // budget: when recovery advances to a new submitted tx, reset the
        // counter so a healthy retry is not abandoned because a prior tx
        // already spent the budget.
        let current_tx_id = mint.pending_fireblocks_tx_id();
        if current_tx_id != polled_tx_id {
            pending_polls = 0;
            polled_tx_id = current_tx_id;
        }

        match mint.automatic_retry_decision(Utc::now()) {
            AutomaticRetryDecision::Ready => {
                match recover_mint(mint_store, issuer_request_id.clone()).await
                {
                    DriveOutcome::Done => {
                        return RecoveryConclusion::Resolved;
                    }
                    DriveOutcome::Exhausted => {
                        return RecoveryConclusion::Abandoned {
                            reason: AbandonReason::AutomaticRetriesExhausted,
                        };
                    }
                    // A still-pending Fireblocks tx is healthy; poll it on a
                    // generous budget separate from transient failures.
                    DriveOutcome::Pending => {
                        pending_polls += 1;
                        if pending_polls > max_pending_polls {
                            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                                max_pending_polls,
                                "Scheduled mint recovery stopped after maximum pending polls"
                            );
                            return RecoveryConclusion::Abandoned {
                                reason:
                                    AbandonReason::PendingPollBudgetExhausted,
                            };
                        }

                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            backoff_ms = backoff.as_millis(),
                            "Scheduled recovery waiting for pending Fireblocks transaction"
                        );
                        tokio::time::sleep(backoff).await;
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
/// `MintingFailed` -> `CallbackPending`). This function loops until
/// `NotRecoverable` is returned, which means the mint has either
/// completed or reached a state that cannot be recovered from.
///
/// Bounded to [`MAX_RECOVERY_ATTEMPTS`] iterations to prevent infinite
/// spinning if a command returns `Ok(())` without advancing state.
async fn drive_recovery(
    mint_store: &Store<Mint>,
    issuer_request_id: IssuerMintRequestId,
    make_command: impl Fn(IssuerMintRequestId) -> MintCommand,
) -> DriveOutcome {
    for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
        let result = mint_store
            .send(&issuer_request_id, make_command(issuer_request_id.clone()))
            .await;

        match result {
            Ok(()) => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    attempt,
                    "Recovery step succeeded, continuing"
                );
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
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::FireblocksTxStillPending { fireblocks_tx_id },
            ))) => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    fireblocks_tx_id,
                    "Mint recovery paused while Fireblocks transaction is pending"
                );
                return DriveOutcome::Pending;
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

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address, b256, uint};
    use chrono::Utc;
    use event_sorcery::{StoreBuilder, test_store};
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::api::test_utils::TestHarness;
    use crate::mint::tests::{BOT, VAULT};
    use crate::mint::{
        ClientId, IssuerMintRequestId, MintEvent, MintServices, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptSource, Shares,
    };
    use crate::test_utils::log_count_at;
    use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        BurnVerification, FireblocksTxStatus, MintResult, MultiBurnParams,
        MultiBurnResult, ReceiptInformation, SubmittedTx, VaultError,
        VaultService,
    };

    /// Builds a real event-sorcery [`Store<Mint>`] backed by an in-memory
    /// SQLite pool, wired with the same services the production recovery flow
    /// uses (vault, Alpaca, receipt inventory). The [`TokenizedAsset`]
    /// projection is populated with the AAPL -> [`VAULT`] mapping so the
    /// recovery vault lookup (`find_vault_by_underlying`) resolves.
    struct MintRecoveryFixture {
        mint_store: Arc<Store<Mint>>,
        receipt_store: Arc<Store<ReceiptInventory>>,
        pool: sqlx::SqlitePool,
    }

    impl MintRecoveryFixture {
        async fn new() -> Self {
            Self::new_with_vault(Arc::new(MockVaultService::new_success()))
                .await
        }

        async fn new_with_vault(vault: Arc<dyn VaultService>) -> Self {
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
                    &UnderlyingSymbol::new("AAPL"),
                    TokenizedAssetCommand::Add {
                        underlying: UnderlyingSymbol::new("AAPL"),
                        token: TokenSymbol::new("tAAPL"),
                        network: Network::Base,
                        vault: VAULT,
                    },
                )
                .await
                .unwrap();

            let receipt_store =
                Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

            let services = MintServices {
                vault,
                alpaca: Arc::new(MockAlpacaService::new_success()),
                receipts: Arc::new(CqrsReceiptService::new(
                    receipt_store.clone(),
                )),
                pool: pool.clone(),
                bot: BOT,
            };

            let mint_store =
                Arc::new(test_store::<Mint>(pool.clone(), services));

            Self { mint_store, receipt_store, pool }
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
                .execute(&self.pool)
                .await
                .unwrap();
            }
        }
    }

    /// Vault whose Fireblocks transaction never finalizes — every status check
    /// returns `Pending`. Used to exercise the scheduled-recovery backoff.
    struct PendingMintVault;

    #[async_trait]
    impl VaultService for PendingMintVault {
        async fn submit_mint(
            &self,
            _vault: Address,
            _assets: U256,
            _bot: Address,
            _user: Address,
            _receipt_info: ReceiptInformation,
            _external_tx_id: Option<String>,
        ) -> Result<SubmittedTx, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn confirm_mint(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<MintResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn get_share_balance(
            &self,
            _vault: Address,
            _owner: Address,
        ) -> Result<U256, VaultError> {
            Ok(U256::ZERO)
        }

        async fn check_fireblocks_tx(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<Option<FireblocksTxStatus>, VaultError> {
            Ok(Some(FireblocksTxStatus::Pending))
        }

        async fn submit_burn(
            &self,
            _params: MultiBurnParams,
        ) -> Result<SubmittedTx, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn confirm_burn(
            &self,
            _fireblocks_tx_id: &str,
            _expected_dust_shares: U256,
        ) -> Result<MultiBurnResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn verify_burn_tx(
            &self,
            _vault: Address,
            _owner: Address,
            _tx_hash: B256,
        ) -> Result<BurnVerification, VaultError> {
            Err(VaultError::InvalidReceipt)
        }
    }

    fn fireblocks_failed_events(
        issuer_request_id: &IssuerMintRequestId,
        failed_at: chrono::DateTime<Utc>,
    ) -> Vec<MintEvent> {
        let mut events = fireblocks_submitted_events(issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "terminal Fireblocks failure".to_string(),
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
                underlying: UnderlyingSymbol::new("AAPL"),
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

    fn fireblocks_submitted_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_events(issuer_request_id);

        events.push(MintEvent::FireblocksSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            fireblocks_tx_id: "fb-tx-123".to_string(),
            submitted_at: now,
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
        let issuer_request_id = test_issuer_request_id();
        let fixture = MintRecoveryFixture::new().await;

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity::new(Decimal::from(100)),
            Utc::now(),
            None,
        );

        fixture
            .receipt_store
            .send(
                &VAULT,
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
    async fn minting_failed_with_receipt_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        recover_mint(fixture.mint_store.as_ref(), issuer_request_id.clone())
            .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            mint.state_name()
        );
        let test = "minting_failed_with_receipt_recovers_to_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Mint recovery complete"]),
            1,
        );
        // A single Recover command advances MintingFailed → CallbackPending
        // → Completed atomically via advance_through_callback, so the
        // drive_recovery loop only needs one successful step.
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn callback_pending_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = callback_pending_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        recover_mint(fixture.mint_store.as_ref(), issuer_request_id.clone())
            .await;

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

    /// A single `Recover` command on a mint stuck in `Minting` must reach
    /// `Completed` in one execution when an on-chain receipt is already
    /// present — the deposit event and the callback must be committed
    /// together so the aggregate never lingers in `CallbackPending`.
    #[traced_test]
    #[tokio::test]
    async fn single_recover_command_from_minting_reaches_completed() {
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
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state after one Recover, got: {}",
            mint.state_name()
        );
        let test = "single_recover_command_from_minting_reaches_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    /// Same invariant for the `MintingFailed` starting state.
    #[traced_test]
    #[tokio::test]
    async fn single_recover_command_from_minting_failed_reaches_completed() {
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
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state after one Recover, got: {}",
            mint.state_name()
        );
        let test =
            "single_recover_command_from_minting_failed_reaches_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    /// Same invariant for the `FireblocksSubmitted` starting state — the
    /// mock vault's `confirm_mint` succeeds, so `TokensMinted` and
    /// `MintCompleted` must be emitted in one command.
    #[traced_test]
    #[tokio::test]
    async fn single_recover_command_from_fireblocks_submitted_reaches_completed()
     {
        let issuer_request_id = test_issuer_request_id();
        let events = fireblocks_submitted_events(&issuer_request_id);
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
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state after one Recover, got: {}",
            mint.state_name()
        );
        let test = "single_recover_command_from_fireblocks_submitted_reaches_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn completed_mint_returns_cleanly() {
        let issuer_request_id = test_issuer_request_id();
        let events = completed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        recover_mint(fixture.mint_store.as_ref(), issuer_request_id.clone())
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
    }

    #[tokio::test]
    async fn recover_mint_returns_pending_while_fireblocks_tx_pending() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = fireblocks_failed_events(&issuer_request_id, failed_at);
        let fixture =
            MintRecoveryFixture::new_with_vault(Arc::new(PendingMintVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome =
            recover_mint(fixture.mint_store.as_ref(), issuer_request_id).await;

        assert_eq!(outcome, DriveOutcome::Pending);
    }

    /// A pending Fireblocks transaction must make the scheduled loop back off
    /// between wakeups rather than spinning through its budget instantly. The
    /// loop sleeps the backoff on every pending wakeup, so total elapsed time
    /// must be at least one backoff per wakeup — a spinning loop would finish
    /// in microseconds.
    #[traced_test]
    #[tokio::test]
    async fn scheduled_recovery_backs_off_while_fireblocks_tx_pending() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = fireblocks_failed_events(&issuer_request_id, failed_at);
        let fixture =
            MintRecoveryFixture::new_with_vault(Arc::new(PendingMintVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let backoff = Duration::from_millis(5);
        let max_pending_polls = 8;
        let start = tokio::time::Instant::now();
        let conclusion = recover_mint_until_automatic_budget_exhausted(
            fixture.mint_store.as_ref(),
            issuer_request_id.clone(),
            backoff,
            max_pending_polls,
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            matches!(
                conclusion,
                RecoveryConclusion::Abandoned {
                    reason: AbandonReason::PendingPollBudgetExhausted
                }
            ),
            "Pending-poll budget exhaustion must report Abandoned, not Resolved"
        );

        assert!(
            elapsed
                >= backoff * (u32::try_from(max_pending_polls).unwrap() - 1),
            "Scheduled recovery must sleep the backoff on each pending \
             poll, elapsed={elapsed:?}"
        );

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "Mint stays MintingFailed while the tx remains pending, got: {}",
            mint.state_name()
        );

        let test = "scheduled_recovery_backs_off_while_fireblocks_tx_pending";
        assert!(
            log_count_at!(
                Level::WARN,
                &[test, "stopped after maximum pending polls"]
            ) >= 1,
            "Loop must stop with a warning once the pending-poll budget is spent"
        );
    }

    /// A mint already in `FireblocksSubmitted` whose tx is still pending must
    /// pause recovery (DriveOutcome::Pending) via the non-blocking pre-check
    /// rather than blocking in confirm_mint or flipping to MintingFailed.
    #[tokio::test]
    async fn fireblocks_submitted_pending_tx_pauses_recovery() {
        let issuer_request_id = test_issuer_request_id();
        let events = fireblocks_submitted_events(&issuer_request_id);
        let fixture =
            MintRecoveryFixture::new_with_vault(Arc::new(PendingMintVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome = recover_mint(
            fixture.mint_store.as_ref(),
            issuer_request_id.clone(),
        )
        .await;

        assert_eq!(outcome, DriveOutcome::Pending);

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::FireblocksSubmitted { .. }),
            "A pending tx must leave the mint in FireblocksSubmitted, got: {}",
            mint.state_name()
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

    /// The budget loop must report `Abandoned` — not a clean conclusion — when it
    /// gives up on a still-incomplete mint (here a pending Fireblocks tx that
    /// outlasts the poll budget), so the job is recorded as Killed rather than a
    /// success that hides a stuck mint.
    #[traced_test]
    #[tokio::test]
    async fn budget_loop_reports_abandoned_when_pending_budget_spent() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = fireblocks_failed_events(&issuer_request_id, failed_at);
        let fixture =
            MintRecoveryFixture::new_with_vault(Arc::new(PendingMintVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let conclusion = recover_mint_until_automatic_budget_exhausted(
            fixture.mint_store.as_ref(),
            issuer_request_id,
            Duration::from_millis(1),
            2,
        )
        .await;

        assert!(
            matches!(
                conclusion,
                RecoveryConclusion::Abandoned {
                    reason: AbandonReason::PendingPollBudgetExhausted
                }
            ),
            "a pending tx that outlasts its budget must abandon, not resolve"
        );

        let test = "budget_loop_reports_abandoned_when_pending_budget_spent";
        assert!(
            log_count_at!(
                Level::WARN,
                &[test, "stopped after maximum pending polls"]
            ) >= 1,
            "abandoning on the pending-poll budget must log the WARN"
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
            .perform(fixture.mint_store.as_ref())
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
        let mut events = fireblocks_submitted_events(&issuer_request_id);
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
            .perform(fixture.mint_store.as_ref())
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
}
