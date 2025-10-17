# sqlite-es

SQLite implementation of `PersistedEventRepository` for the
[cqrs-es](https://crates.io/crates/cqrs-es) framework.

This crate provides a SQLite-backed event store for event sourcing applications,
following the same patterns as
[postgres-es](https://crates.io/crates/postgres-es) and
[mysql-es](https://crates.io/crates/mysql-es).

## Features

- Event storage with optimistic locking
- Snapshot support for performance optimization
- Event streaming capabilities
- Full integration with cqrs-es framework
- In-memory testing utilities

## Usage

### Basic Setup

```rust
use sqlite_es::{SqliteEventRepository, sqlite_cqrs};
use sqlx::{Pool, Sqlite};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let pool = Pool::<Sqlite>::connect("sqlite:events.db").await?;

    // Run migrations
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Create CQRS framework
    let cqrs = sqlite_cqrs(pool, vec![], ());

    Ok(())
}
```

### Custom Table Names

```rust
use sqlite_es::SqliteEventRepository;
use sqlx::{Pool, Sqlite};

let pool = Pool::<Sqlite>::connect("sqlite:events.db").await?;

let repo = SqliteEventRepository::with_tables(
    pool,
    "custom_events".to_string(),
    "custom_snapshots".to_string(),
);
```

### Testing

The crate provides testing utilities for in-memory databases:

```rust
use sqlite_es::testing::create_test_pool;

#[tokio::test]
async fn test_my_aggregate() {
    let pool = create_test_pool().await.unwrap();
    // ... test code
}
```

## Database Schema

The default schema uses two tables:

### Events Table

```sql
CREATE TABLE events (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    payload JSON NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);
```

### Snapshots Table

```sql
CREATE TABLE snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL,
    payload JSON NOT NULL,
    timestamp TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

## SQLite-Specific Considerations

SQLite differs from PostgreSQL/MySQL in several ways:

- **Parameter binding**: Uses `?` instead of `$1, $2, ...`
- **Concurrency**: Write operations are serialized at the database level
- **Transaction isolation**: Default is SERIALIZABLE
- **Type system**: More flexible but less strict typing

## Documentation

- [cqrs-es Documentation](https://docs.rs/cqrs-es)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [sqlx Documentation](https://docs.rs/sqlx)
