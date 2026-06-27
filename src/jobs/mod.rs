//! Interim durable-job runner for st0x.issuance.
//!
//! A small, reusable abstraction over apalis-backed persistent jobs: a
//! serializable [`Job`] impl is pushed into [`JobQueue`] and drained by an
//! apalis worker that calls [`Job::perform`] via the generic [`work`] handler.
//!
//! This is the interim shape ADR-0002 prescribes — mirroring the machinery the
//! `event-sorcery` library lifted from st0x.liquidity's `conductor::job` — and
//! is deleted at the phase-2 bump in favour of the library's stack. It ships
//! only the core that has a real consumer today ([`crate::mint::recovery`]);
//! the retry / fail-stop worker macros, circuit breaker, and test
//! failure-injection arrive with their first request→outcome consumers.

use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::{Data, TaskBuilder, TaskSink};
use apalis_codec::json::JsonCodec;
use apalis_core::backend::TaskSinkError;
use apalis_core::backend::poll_strategy::{
    BackoffConfig, IntervalStrategy, StrategyBuilder,
};
use apalis_sqlite::fetcher::SqliteFetcher;
use apalis_sqlite::{
    CompactType, Config, SqlitePool, SqliteStorage, SqlxError,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// apalis-sqlite storage specialised to JSON-encoded tasks. This is exactly the
/// concrete type `SqliteStorage::new` returns, so naming it here pins the same
/// storage the runtime already uses — switching nothing about the on-disk
/// `Jobs` encoding.
type Storage<Task> = SqliteStorage<Task, JsonCodec<CompactType>, SqliteFetcher>;

/// Handler-facing durable job queue backed by apalis `SqliteStorage`.
///
/// Constructed against the apalis-sqlite (sqlx 0.8) pool, distinct from the
/// event store's sqlx 0.9 pool but addressing the same SQLite file.
pub(crate) struct JobQueue<Task>(Storage<Task>);

/// Error returned by [`JobQueue::push_with_idempotency_key`]. Wrapping
/// [`TaskSinkError`] keeps the failure chain typed so callers can `#[from]` it.
#[derive(Debug, thiserror::Error)]
#[error("failed to enqueue apalis job: {0}")]
pub(crate) struct QueuePushError(#[from] pub(crate) TaskSinkError<SqlxError>);

impl<Task> Clone for JobQueue<Task> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Task: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static>
    JobQueue<Task>
{
    pub(crate) fn new(pool: &SqlitePool) -> Self {
        Self(SqliteStorage::new(pool))
    }

    /// Like [`new`](Self::new) but caps the worker poll interval at ~1s instead
    /// of apalis's default exponential backoff to 60s after an idle period. Use
    /// for the backend of a worker draining latency-sensitive jobs (the mint
    /// side-effect chain), where a freshly enqueued job must be picked up
    /// promptly. The `job_type` is unchanged, so it stays compatible with rows
    /// pushed through [`new`](Self::new).
    pub(crate) fn with_fast_poll(pool: &SqlitePool) -> Self {
        Self(SqliteStorage::new_with_config(pool, &build_poll_config::<Task>()))
    }

    /// Enqueues `task` keyed by `idempotency_key`. apalis collapses the insert
    /// against any existing row sharing `(job_type, idempotency_key)` via
    /// `ON CONFLICT DO NOTHING`, so a re-enqueue for a job that is still
    /// in-flight — or already terminal — is a silent no-op.
    pub(crate) async fn push_with_idempotency_key(
        &mut self,
        task: Task,
        idempotency_key: impl AsRef<str>,
    ) -> Result<(), QueuePushError> {
        let task = TaskBuilder::new(task)
            .with_idempotency_key(idempotency_key)
            .build();
        Ok(TaskSink::push_task(&mut self.0, task).await?)
    }

    /// Consumes the queue into the underlying `SqliteStorage` a worker drains.
    pub(crate) fn into_storage(self) -> Storage<Task> {
        self.0
    }
}

/// Worker poll strategy capped at ~1s (100ms base, 1s backoff cap), so a
/// freshly enqueued job is picked up promptly rather than after apalis's
/// default exponential backoff to 60s following an idle period.
fn build_poll_config<Task: 'static>() -> Config {
    let strategy = StrategyBuilder::new()
        .apply(
            IntervalStrategy::new(Duration::from_millis(100))
                .with_backoff(BackoffConfig::new(Duration::from_secs(1))),
        )
        .build();

    Config::new(std::any::type_name::<Task>()).with_poll_interval(strategy)
}

/// A persistent, durable unit of work backed by apalis storage.
///
/// Implementations are serializable structs carrying the data needed to process
/// a single job. `Ctx` bundles the runtime dependencies (stores, services,
/// config) injected into the worker via apalis `Data` and handed to
/// [`perform`](Job::perform) by reference.
pub(crate) trait Job<Ctx>:
    Serialize + DeserializeOwned + Send + 'static
where
    Ctx: Send + Sync + 'static,
{
    /// Value produced on successful completion.
    type Output: Send + 'static;

    /// Error returned by [`perform`](Job::perform).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Processes this job using the provided context.
    async fn perform(&self, ctx: &Ctx) -> Result<Self::Output, Self::Error>;
}

/// Generic apalis handler adapting a [`Job`] impl to apalis's function-based
/// worker API.
///
/// Returns the raw `J::Error` rather than boxing it: apalis maps a handler
/// error to a job status by downcasting it, and an
/// [`AbortError`](apalis::prelude::AbortError) returned by `perform` must stay
/// downcastable so the job is marked `Killed` (terminal, not re-fetched) instead
/// of a retryable `Failed`.
pub(crate) async fn work<Ctx, J>(
    job: J,
    ctx: Data<Arc<Ctx>>,
) -> Result<J::Output, J::Error>
where
    Ctx: Send + Sync + 'static,
    J: Job<Ctx> + Sync,
{
    job.perform(&ctx).await
}
