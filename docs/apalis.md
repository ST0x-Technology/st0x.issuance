# Apalis

## Why apalis

We use apalis (v0.7.4, SQLite backend) for startup and recovery jobs. It gives
us retry with backoff, persistence across restarts, ordered execution within a
worker, and visibility into job state (pending, running, failed, done) — all
backed by the same SQLite database the rest of the system uses.

## Migration ordering

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

## Job pattern

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

Recovery jobs execute in a specific order to satisfy data dependencies. Each
phase completes before the next begins:

1. **View replay** — rebuild read models from the event store
2. **Receipt backfill** — sync historic on-chain receipts
3. **Transfer backfill** — sync historic redemption transfers
4. **Mint recovery** — resume any in-progress mints
5. **Redemption recovery** — resume any in-progress redemptions

## What stays outside apalis

Continuous monitors — receipt monitors and redemption detectors — are **not**
apalis jobs. They run as long-lived tasks via `tokio::spawn` because they
maintain WebSocket subscriptions and process events indefinitely. Apalis is for
finite work that completes; `tokio::spawn` is for processes that run for the
lifetime of the service.
