-- Ship apalis-sqlite 1.0-rc's `Workers`/`Jobs` schema as consumer-owned DDL.
--
-- apalis-sqlite does not run its own migrator here: doing so competes with our
-- migrator over the shared `_sqlx_migrations` table. Instead we replay its final
-- schema (the state after all of apalis-sqlite 1.0.0-rc.8's own migrations) so
-- the version is pinned to our Cargo.lock and the crate's compile-time `query!`
-- checks resolve against a matching table. apalis-sqlite is built on sqlx 0.8
-- while the rest of the bot is on sqlx 0.9, so its storage uses its own pool
-- against this same SQLite file (see `initialize_rocket`).

-- No `IF NOT EXISTS`: the pinned apalis-sqlite integration depends on this exact
-- schema and column order, so a pre-existing or hand-created `Workers`/`Jobs`
-- table must fail this migration loudly rather than let drift surface later at
-- enqueue/drain time.
CREATE TABLE Workers (
    id TEXT NOT NULL UNIQUE,
    worker_type TEXT NOT NULL,
    storage_name TEXT NOT NULL,
    layers TEXT,
    last_seen INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    started_at INTEGER
);

CREATE INDEX IF NOT EXISTS Idx ON Workers(id);

CREATE INDEX IF NOT EXISTS WTIdx ON Workers(worker_type);

CREATE INDEX IF NOT EXISTS LSIdx ON Workers(last_seen);

CREATE TABLE Jobs (
    job BLOB NOT NULL,
    id TEXT NOT NULL UNIQUE,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 25,
    run_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    last_result TEXT,
    lock_at INTEGER,
    lock_by TEXT,
    done_at INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    metadata TEXT,
    idempotency_key TEXT,
    PRIMARY KEY(id),
    FOREIGN KEY(lock_by) REFERENCES Workers(id)
);

CREATE INDEX IF NOT EXISTS TIdx ON Jobs(id);

CREATE INDEX IF NOT EXISTS SIdx ON Jobs(status);

CREATE INDEX IF NOT EXISTS LIdx ON Jobs(lock_by);

CREATE INDEX IF NOT EXISTS JTIdx ON Jobs(job_type);

CREATE INDEX IF NOT EXISTS idx_jobs_job_type_status_run_at ON Jobs(job_type, status, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at ON Jobs(status, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_run_at_status ON Jobs(run_at, status);

CREATE INDEX IF NOT EXISTS idx_jobs_completed_done_at ON Jobs(status, done_at, run_at)
WHERE
    status IN ('Done', 'Failed', 'Killed')
    AND done_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_pending ON Jobs(run_at)
WHERE
    status = 'Pending';

CREATE INDEX IF NOT EXISTS idx_jobs_running ON Jobs(run_at)
WHERE
    status = 'Running';

CREATE INDEX IF NOT EXISTS idx_jobs_job_type_run_at ON Jobs(job_type, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_job_type_covering ON Jobs(job_type, status, run_at, done_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key ON Jobs(job_type, idempotency_key);
