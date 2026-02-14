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
  └── RedemptionRecoveryJob
```

**Apalis** (v0.7.4, SQLite backend) handles finite startup/recovery work:
backfilling historic data, replaying views, and recovering in-progress
operations. It gives us retry with backoff, persistence across restarts, ordered
execution within a worker, and visibility into job state (pending, running,
failed, done) — all backed by the same SQLite database the rest of the system
uses. Jobs run to completion during the phased startup sequence.

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

Jobs are zero-size structs that implement the apalis job trait. The actual
dependencies come through `Data<T>` extractors in the `run` function, and jobs
are registered via `build_fn`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MintRecoveryJob;

impl MintRecoveryJob {
    async fn run(self, pool: Data<Pool<Sqlite>>, cqrs: Data<MintCqrs>) {
        // recovery logic
    }
}

// registration
Monitor::new().register(
    WorkerBuilder::new("mint-recovery")
        .data(pool)
        .data(cqrs)
        .backend(storage)
        .build_fn(MintRecoveryJob::run),
);
```

## Phase-based startup ordering

Startup jobs execute in a specific order to satisfy data dependencies. Each
phase completes before the next begins:

1. **View replay** — rebuild read models from the event store
2. **Receipt backfill** — sync historic on-chain receipts
3. **Transfer backfill** — sync historic redemption transfers
4. **Mint recovery** — resume any in-progress mints
5. **Redemption recovery** — resume any in-progress redemptions
