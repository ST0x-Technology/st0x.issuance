# Orchestration

## Two-layer architecture

The service uses two complementary tools for task management:

```
task-supervisor (outermost — runs indefinitely, restarts crashed tasks)
  ├── ReceiptMonitor (vault A)
  ├── ReceiptMonitor (vault B)
  ├── RedemptionDetector (vault A)
  └── RedemptionDetector (vault B)

apalis (work layer — finite jobs that complete during startup)
  ├── ViewReplayJob
  ├── ReceiptBackfillJob
  ├── TransferBackfillJob
  ├── MintRecoveryJob
  ├── RedemptionRecoveryJob (vault A)  -- concurrent
  └── RedemptionRecoveryJob (vault B)  -- concurrent
```

**Apalis** (v0.7.4, SQLite backend) handles finite startup/recovery work:
backfilling historic data, replaying views, and recovering in-progress
operations. It gives us retry with backoff, persistence across restarts, ordered
execution within a worker, and visibility into job state (pending, running,
failed, done) — all backed by the same SQLite database the rest of the system
uses. Jobs run to completion during the startup sequence.

**task-supervisor** handles long-running monitors that must survive transient
failures. Each monitor implements `SupervisedTask` and is registered with a
`SupervisorBuilder`. The supervisor restarts crashed tasks with configurable
backoff (unlimited restarts, 5s base delay). Monitors have their own internal
retry loops for transient WebSocket failures; the supervisor handles
catastrophic failures (panics, unexpected exits).

The startup sequence: apalis jobs run to completion first (producing vault
configs), then the supervisor takes over with the long-running monitors.

## Apalis migration ordering

Both apalis and our application use sqlx migrations, and both write to the same
`_sqlx_migrations` table. Each migrator validates that every previously applied
migration exists in its own migration set — so running either migrator after the
other fails with `VersionMissing` unless both use `set_ignore_missing(true)`.

We cannot use `SqliteStorage::setup()` because it runs apalis migrations without
`ignore_missing`. Instead, we get the apalis migrator directly:

```rust
SqliteStorage::<()>::migrations()
    .set_ignore_missing(true)
    .run(&pool)
    .await?;
sqlx::migrate!("./migrations")
    .set_ignore_missing(true)
    .run(&pool)
    .await?;
```

**CLI gotcha:** Running `sqlx migrate run` against a database that already has
apalis migrations will fail with `VersionMissing` because the CLI sees unknown
entries in `_sqlx_migrations`. Use `sqlx migrate run --ignore-missing` to work
around this.

## Apalis job pattern

Jobs implement the `Job` trait (`src/job.rs`), which defines associated types
for context (`Ctx`) and error (`Error`), plus a `label()` method returning a
`Label` newtype and an async `run()` method:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MintRecoveryJob;

impl Job for MintRecoveryJob {
    type Ctx = MintRecoveryCtx;
    type Error = MintViewError;

    fn label(&self) -> Label { Label::new("mint-recovery") }

    async fn run(self, ctx: Data<Self::Ctx>) -> Result<(), Self::Error> {
        // recovery logic
    }
}

// registration
Monitor::new().register(
    WorkerBuilder::new("mint-recovery")
        .data(MintRecoveryCtx { pool, mint_cqrs })
        .backend(storage)
        .build_fn(MintRecoveryJob::run),
);
```

Each job bundles its dependencies into a single `Ctx` type rather than using
multiple `Data<T>` extractors.

## Startup ordering

Startup jobs execute via two helpers:

- `push_and_await` — pushes a single job and polls until completion. Used for
  sequential jobs where ordering matters.
- `push_all_and_await` — pushes all jobs first, then polls all of them in a
  single loop until every job completes. Used when multiple independent jobs can
  run concurrently.

The ordering satisfies data dependencies — each step assumes the preceding steps
have completed:

1. **View replay** — rebuild read models from the event store so recovery jobs
   can query accurate state
2. **Receipt backfill** (one per vault) — sync historic on-chain receipts into
   the receipt inventory
3. **Transfer backfill** (one per vault) — sync historic redemption transfers.
   Runs after all receipt backfills because transfer detection depends on
   receipt inventory being populated.
4. **Mint recovery** — resume any in-progress mints
5. **Redemption recovery** (one per vault, concurrent) — resume any in-progress
   redemptions for each vault

Any failure aborts startup — the service will not accept requests until all jobs
succeed. All jobs are idempotent, so restarting retries safely.
