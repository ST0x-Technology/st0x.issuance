# Implementation Plan: Issue #8 - Implement SqliteEventRepository

## Overview

This plan implements a custom SQLite event store following the
postgres-es/mysql-es pattern. The implementation will provide the foundation for
event sourcing in the issuance bot by creating a `PersistedEventRepository`
trait implementation backed by SQLite.

## Research Summary

### Architecture Pattern

The cqrs-es framework uses a layered architecture:

- `PersistedEventStore` - High-level event store interface
- `PersistedEventRepository` - Trait for database operations (what we implement)
- Database-specific implementations (PostgresEventRepository,
  MysqlEventRepository, etc.)

### Key Components to Implement

Based on postgres-es and mysql-es structure:

1. **event_repository.rs** - Core `SqliteEventRepository` struct,
   `PersistedEventRepository` trait implementation, and repository-specific
   error types
2. **sql_query.rs** - SQL query generation factory for SQLite
3. **cqrs.rs** - CQRS framework integration and type aliases
4. **lib.rs** - Public API and re-exports
5. **testing.rs** - Test utilities and helpers
6. **view_repository.rs** - Stub for future view repository (issue #9)

### SQLite-Specific Considerations

SQLite differs from PostgreSQL/MySQL in several ways that affect implementation:

1. **Placeholder syntax**: Uses `?` instead of `$1, $2, ...` for parameter
   binding
2. **Type system**: More flexible but less strict typing
3. **Concurrency**: Write serialization handled at database level
4. **Transaction isolation**: Different default isolation levels
5. **JSON support**: Has JSON1 extension enabled by default in modern SQLite

## Design Decisions

### Database Schema

Following SPEC.md, we'll use these tables:

**events table:**

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

**snapshots table:**

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

### Crate Structure

Create a workspace member at `crates/sqlite-es` with:

- Similar API to postgres-es and mysql-es for consistency
- SQLite-specific optimizations where appropriate
- Full integration with cqrs-es framework
- Comprehensive error handling
- Minimal visibility levels (private by default, then `pub(crate)`, then `pub`
  only when necessary)

### Dependencies

Dependencies will be configured at the workspace level where appropriate:

**Workspace-level dependencies** (in root `Cargo.toml`):

- `cqrs-es` - Core CQRS/ES framework (used by main bot and sqlite-es)
- `async-trait`
- `serde` with features: derive
- `serde_json`
- `thiserror`
- `tracing`

**Crate-specific dependencies** (only in `crates/sqlite-es`):

- `sqlx` with features: sqlite, runtime-tokio-rustls - SQLite database adapter

All lints will be inherited from workspace level.

### Error Handling

Define errors inline in modules where they're needed using thiserror. Use
`#[from]` attribute to compose errors across module boundaries.

Repository errors will be defined in `event_repository.rs`:

1. **OptimisticLock** - Concurrent modification detected
2. **Connection** - Database connection issues (via `#[from] sqlx::Error`)
3. **Deserialization** - Event/snapshot deserialization failures (via
   `#[from] serde_json::Error`)

---

## Task 1. Setup Workspace and Crate Structure ✅

### Subtasks

- [x] Update root `Cargo.toml` to add workspace configuration
- [x] Add shared dependencies at workspace level
- [x] Configure workspace lints
- [x] Create `crates/sqlite-es` directory
- [x] Run `cargo init --lib` in `crates/sqlite-es`
- [x] Add sqlite-es to workspace members
- [x] Add crate-specific dependencies using `cargo add`
- [x] Configure crate to inherit workspace lints
- [x] Create `crates/sqlite-es/src` directory structure
- [x] Create basic `lib.rs` with module declarations

### Implementation Details

**Step 1: Add dependencies to get latest versions:**

```bash
# Add dependencies to root package - this gets latest versions
cargo add rocket
cargo add sqlx --features sqlite,chrono,runtime-tokio-rustls
cargo add cqrs-es
cargo add async-trait
cargo add serde --features derive
cargo add serde_json
cargo add thiserror
cargo add tracing
```

**Step 2: Manually update root Cargo.toml for workspace structure:**

Move the dependencies added in Step 1 from `[dependencies]` to
`[workspace.dependencies]`, then add workspace configuration and update root
package to use workspace dependencies:

```toml
[workspace]
members = [".", "crates/sqlite-es"]
resolver = "2"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
unwrap_used = "deny"
enum_glob_use = "deny"
pedantic = { level = "deny", priority = -1 }
nursery = { level = "deny", priority = -1 }
doc_markdown = "allow"
items_after_statements = "allow"
redundant_pub_crate = "allow"

[workspace.dependencies]
# Move dependencies added in Step 1 here

[dependencies]
rocket.workspace = true
sqlx.workspace = true
tracing.workspace = true

[lints]
workspace = true
```

**Step 3: Create and initialize sqlite-es crate:**

```bash
mkdir -p crates/sqlite-es
cd crates/sqlite-es
cargo init --lib
```

**Step 4: Add dependencies to sqlite-es:**

```bash
cd crates/sqlite-es
cargo add sqlx --features sqlite,runtime-tokio-rustls
```

**Step 5: Configure sqlite-es to use workspace dependencies:**

Edit `crates/sqlite-es/Cargo.toml` to add workspace dependencies and lints

```toml
[dependencies]
cqrs-es.workspace = true
async-trait.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing.workspace = true

[lints]
workspace = true
```

**Step 6: Create module structure:**

```bash
touch src/cqrs.rs
touch src/event_repository.rs
touch src/sql_query.rs
touch src/testing.rs
touch src/view_repository.rs
```

---

## Task 2. Implement SQL Query Factory ✅

### Subtasks

- [x] Create `crates/sqlite-es/src/sql_query.rs`
- [x] Implement `SqlQueryFactory` struct with configurable table names
- [x] Implement event table queries (select, insert, stream)
- [x] Implement snapshot table queries (select, insert, update)
- [x] Use SQLite `?` placeholders
- [x] Split long SELECT lists line-by-line

### Implementation Details

The `SqlQueryFactory` generates SQL queries for:

**Event queries:**

- `select_events` - Get all events for an aggregate
- `insert_event` - Insert a new event
- `all_events` - Stream all events of an aggregate type
- `get_last_events` - Get events after a specific sequence

**Snapshot queries:**

- `select_snapshot` - Get snapshot for an aggregate
- `insert_snapshot` - Create new snapshot
- `update_snapshot` - Update existing snapshot with optimistic locking

**SQLite-specific considerations:**

- Use `?` placeholders instead of `$1, $2, ...`
- Use `INSERT OR REPLACE` for snapshot updates
- Handle TEXT storage for BIGINT values

Example factory structure with proper SELECT formatting:

```rust
pub(crate) struct SqlQueryFactory {
    events_table: String,
    snapshots_table: String,
}

impl SqlQueryFactory {
    pub(crate) fn new(events_table: String, snapshots_table: String) -> Self {
        Self { events_table, snapshots_table }
    }

    pub(crate) fn select_events(&self) -> String {
        format!(
            "SELECT
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
             FROM {}
             WHERE aggregate_type = ? AND aggregate_id = ?
             ORDER BY sequence",
            self.events_table
        )
    }

    // ... more query methods
}
```

Note: Keep struct and methods private or `pub(crate)` - use minimal visibility.

---

## Task 3. Implement Event Repository Core

### Subtasks

- [ ] Create `crates/sqlite-es/src/event_repository.rs`
- [ ] Define `SqliteAggregateError` enum inline using thiserror
- [ ] Define `SqliteEventRepository` struct
- [ ] Implement repository constructor and builder methods
- [ ] Implement event retrieval methods
- [ ] Implement event persistence methods
- [ ] Implement snapshot methods
- [ ] Implement optimistic locking for concurrent writes
- [ ] Use minimal visibility levels

### Implementation Details

**Error types (defined inline in this module):**

```rust
#[derive(Debug, thiserror::Error)]
pub enum SqliteAggregateError {
    #[error("Optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,

    #[error("Database connection error: {0}")]
    Connection(#[from] sqlx::Error),

    #[error("Event deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}
```

The `SqliteEventRepository` struct:

```rust
pub struct SqliteEventRepository {
    pool: Pool<Sqlite>,
    query_factory: SqlQueryFactory,
    stream_channel_size: usize,
}
```

Key methods to implement:

1. **Constructor:**

```rust
pub fn new(pool: Pool<Sqlite>) -> Self
pub fn with_tables(pool: Pool<Sqlite>, events_table: String, snapshots_table: String) -> Self
```

2. **Event retrieval:**

```rust
async fn get_events<A: Aggregate>(
    &self,
    aggregate_id: &str,
) -> Result<Vec<EventEnvelope<A>>, SqliteAggregateError>

async fn get_last_events<A: Aggregate>(
    &self,
    aggregate_id: &str,
    last_sequence: usize,
) -> Result<Vec<EventEnvelope<A>>, SqliteAggregateError>
```

3. **Snapshot operations:**

```rust
async fn get_snapshot<A: Aggregate>(
    &self,
    aggregate_id: &str,
) -> Result<Option<SerializedSnapshot>, SqliteAggregateError>
```

4. **Event persistence:**

```rust
async fn persist<A: Aggregate>(
    &self,
    events: &[EventEnvelope<A>],
    snapshot_update: Option<(String, Value, usize)>,
) -> Result<(), SqliteAggregateError>
```

5. **Event streaming:**

```rust
fn stream_events<A: Aggregate>(
    &self,
    aggregate_id: &str,
) -> ReplayStream

fn stream_all_events<A: Aggregate>(
    &self,
    aggregate_type: &str,
) -> ReplayStream
```

**Optimistic locking strategy:**

- Use transactions for atomicity
- Check sequence numbers before insert
- Return `OptimisticLock` error on conflicts
- Let SQLite's SERIALIZABLE isolation handle concurrency

**Serialization:**

- Events stored as JSON in `payload` column
- Metadata stored as JSON in `metadata` column
- Use serde_json for serialization/deserialization
- Use `?` operator for error propagation

**Visibility:**

- Keep internal helpers private or `pub(crate)` - use minimal visibility
- Only expose what needs to be public for the trait implementation

---

## Task 4. Implement PersistedEventRepository Trait

### Subtasks

- [ ] Implement `PersistedEventRepository` trait for `SqliteEventRepository`
- [ ] Ensure all trait methods delegate to internal implementation
- [ ] Use `?` operator for error propagation (no boxing)
- [ ] Verify compatibility with cqrs-es framework

### Implementation Details

The trait implementation connects our SQLite repository to the cqrs-es
framework:

```rust
#[async_trait]
impl PersistedEventRepository for SqliteEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        self.get_events(aggregate_id).await?
    }

    // ... implement all other trait methods
}
```

All methods should:

- Delegate to internal implementation
- Use `?` operator for error propagation
- Use `#[from]` conversions defined in error types
- Maintain async semantics
- Preserve transaction boundaries

---

## Task 5. Implement CQRS Integration

### Subtasks

- [ ] Create `crates/sqlite-es/src/cqrs.rs`
- [ ] Define `SqliteCqrs<A>` type alias
- [ ] Implement helper functions for creating CQRS framework
- [ ] Add convenience constructors
- [ ] Use appropriate visibility levels

### Implementation Details

Provide type aliases and helpers for easy integration:

```rust
use cqrs_es::{Aggregate, CqrsFramework, PersistedEventStore};
use sqlx::{Pool, Sqlite};
use crate::SqliteEventRepository;

pub type SqliteCqrs<A> = CqrsFramework<A, PersistedEventStore<SqliteEventRepository, A>>;

pub fn sqlite_cqrs<A>(
    pool: Pool<Sqlite>,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> SqliteCqrs<A>
where
    A: Aggregate,
{
    let repo = SqliteEventRepository::new(pool);
    let store = PersistedEventStore::new_event_store(repo);
    CqrsFramework::new(store, query_processor, services)
}
```

This provides a clean API for users to create CQRS frameworks backed by SQLite.

---

## Task 6. Update Public API

### Subtasks

- [ ] Update `lib.rs` with public API
- [ ] Re-export necessary modules
- [ ] Add crate-level documentation

### Implementation Details

**lib.rs:**

```rust
//! SQLite implementation of PersistedEventRepository for cqrs-es
//!
//! This crate provides a SQLite-backed event store for use with the cqrs-es
//! framework. It follows the same pattern as postgres-es and mysql-es.

mod cqrs;
mod event_repository;
mod sql_query;
pub mod testing;

// Re-exports
pub use cqrs::*;
pub use event_repository::*;
```

---

## Task 7. Implement Testing Utilities

### Subtasks

- [ ] Create `crates/sqlite-es/src/testing.rs`
- [ ] Implement in-memory database helpers using `sqlx::migrate!()`
- [ ] Add test data builders
- [ ] Create helper functions for test setup
- [ ] Add example test demonstrating usage

### Implementation Details

Provide testing utilities that use migrations:

```rust
use sqlx::{Pool, Sqlite};

/// Creates an in-memory SQLite database with migrations applied
pub async fn create_test_pool() -> Result<Pool<Sqlite>, sqlx::Error> {
    let pool = Pool::<Sqlite>::connect(":memory:").await?;
    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}
```

Key points:

- Use `sqlx::migrate!()` to run migrations from the migrations folder
- Don't manually create tables in Rust code
- Keep visibility appropriate (likely `pub` for testing utilities)

---

## Task 8. Add View Repository Stub

### Subtasks

- [ ] Create `crates/sqlite-es/src/view_repository.rs`
- [ ] Add module stub with TODO comment referencing issue #9
- [ ] Export from lib.rs

### Implementation Details

Create a stub file that will be implemented in issue #9:

```rust
//! View repository implementation for SQLite
//!
//! TODO(#9): Implement SqliteViewRepository
//! This will provide view persistence for read models in the CQRS pattern.
```

Format: `TODO(#issue_number): description`

---

## Task 9. Populate Database Migration

### Subtasks

- [ ] Reset the database using `sqlx migrate reset -y`
- [ ] Populate the existing `migrations/20251016210348_init.sql` file
- [ ] Add events and snapshots tables with proper indexes
- [ ] Test migration with `sqlx migrate run`

### Implementation Details

Edit the existing `migrations/20251016210348_init.sql` file:

```sql
-- Events table: stores all domain events
CREATE TABLE IF NOT EXISTS events (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    payload JSON NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(aggregate_type);
CREATE INDEX IF NOT EXISTS idx_events_aggregate
    ON events(aggregate_id);

-- Snapshots table: aggregate state cache for performance
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL,
    payload JSON NOT NULL,
    timestamp TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

Then run:

```bash
sqlx migrate reset -y
sqlx migrate run
```

**Note:** This follows the exact schema from SPEC.md and uses the existing
migration file.

---

## Task 10. Integration Testing

### Subtasks

- [ ] Create integration tests in sqlite-es crate
- [ ] Test event persistence and retrieval
- [ ] Test snapshot creation and updates
- [ ] Test optimistic locking behavior
- [ ] Test error conditions
- [ ] Test concurrent access patterns
- [ ] Verify compatibility with cqrs-es framework

### Implementation Details

Add tests in `crates/sqlite-es/src/event_repository.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;

    #[tokio::test]
    async fn test_persist_and_load_events() {
        // Test: persist events, reload them, verify content
    }

    #[tokio::test]
    async fn test_optimistic_locking() {
        // Test: concurrent writes should trigger OptimisticLock error
    }

    #[tokio::test]
    async fn test_snapshot_operations() {
        // Test: create, update, and retrieve snapshots
    }

    #[tokio::test]
    async fn test_event_streaming() {
        // Test: stream events from repository
    }
}
```

Tests should:

- Use in-memory SQLite databases for isolation via `create_test_pool()`
- Use `sqlx::migrate!()` to apply migrations
- Cover happy paths and error conditions
- Verify serialization/deserialization
- Test concurrent access scenarios
- Validate optimistic locking

---

## Task 11. Documentation and Examples

### Subtasks

- [ ] Add rustdoc comments to all public APIs
- [ ] Create README.md for sqlite-es crate
- [ ] Add usage examples
- [ ] Document SQLite-specific considerations
- [ ] Link to cqrs-es documentation

### Implementation Details

**README.md structure:**

1. Overview of the crate
2. Installation instructions
3. Basic usage example
4. Configuration options
5. Testing utilities
6. Differences from postgres-es/mysql-es
7. Link to SPEC.md and cqrs-es docs

**Example usage:**

```rust
use sqlite_es::{SqliteEventRepository, SqliteCqrs};
use sqlx::{Pool, Sqlite};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let pool = Pool::<Sqlite>::connect("sqlite:issuance.db").await?;

    // Create repository
    let repo = SqliteEventRepository::new(pool);

    // Create CQRS framework
    let cqrs = SqliteCqrs::new(repo, queries, services);

    // Execute commands
    cqrs.execute("aggregate-id", MyCommand { /* ... */ }).await?;

    Ok(())
}
```

---

## Task 12. Final Integration and Testing

### Subtasks

- [ ] Run `cargo test -q` on sqlite-es crate
- [ ] Run `cargo clippy` with no warnings
- [ ] Run `cargo fmt` to format code
- [ ] Verify workspace builds successfully
- [ ] Test integration with main issuance bot project
- [ ] Run pre-commit hooks

### Implementation Details

Final verification checklist:

1. All tests pass in sqlite-es crate
2. No clippy warnings (with deny warnings enabled)
3. Code is properly formatted
4. Documentation builds without warnings
5. No dead code or unused imports
6. Integration with cqrs-es framework works correctly
7. Migration can be applied successfully

Run commands:

```bash
cd crates/sqlite-es
cargo test -q
cargo clippy --all-targets --all-features -- -D clippy::all
cargo fmt --check
cargo doc --no-deps

cd ../..
cargo test -q
cargo clippy --all-targets --all-features -- -D clippy::all
cargo fmt
```

---

## Success Criteria

The implementation is complete when:

1. ✅ Workspace configuration includes shared dependencies and lints
2. ✅ `crates/sqlite-es` crate is created with proper structure
3. ✅ `SqliteEventRepository` implements `PersistedEventRepository` trait
4. ✅ Events can be persisted and retrieved from SQLite
5. ✅ Snapshots can be created and updated
6. ✅ Optimistic locking prevents concurrent modification issues
7. ✅ Database migration populates existing init migration file
8. ✅ All tests pass without warnings
9. ✅ Code passes clippy with deny warnings
10. ✅ Code is properly formatted
11. ✅ Documentation is comprehensive and accurate
12. ✅ Integration with cqrs-es framework is verified
13. ✅ Testing utilities use `sqlx::migrate!()` for setup
14. ✅ Visibility levels are minimal (prefer `pub(crate)`)

---

## Dependencies

This issue depends on:

- [x] Issue #5 - Nix flake with Rust toolchain (assumed complete per ROADMAP)
- [x] Issue #6 - Rocket.rs + SQLite setup (assumed complete per ROADMAP)
- [x] Issue #7 - CI setup (assumed complete per ROADMAP)

This issue blocks:

- [ ] Issue #9 - Implement SqliteViewRepository
- [ ] Issue #10+ - All feature implementations (Phases 2-6)

---

## Notes

### SQLite vs PostgreSQL/MySQL Differences

Key differences to handle:

1. **Parameter binding**: `?` instead of `$1, $2, ...`
2. **Type flexibility**: SQLite's type system is more flexible
3. **Concurrency**: SQLite serializes writes at database level
4. **Transaction isolation**: Default is SERIALIZABLE
5. **JSON support**: Uses JSON1 extension (enabled by default)

### Design Rationale

**Why follow postgres-es/mysql-es pattern?**

- Consistency with existing implementations
- Proven architecture for event sourcing
- Easy for developers familiar with other *-es crates
- Reusable testing patterns

**Why separate crate?**

- Clear separation of concerns
- Reusable by other projects
- Independent versioning
- Focused testing

**Why SQLite?**

- Embedded database (no separate server)
- ACID compliance
- Good performance for single-writer scenarios
- Easy deployment and backup
- Perfect for serverless/edge deployments

### Future Enhancements (Out of Scope)

- Connection pooling optimizations
- Write-ahead logging (WAL) mode configuration
- Snapshot compression
- Event encryption at rest
- Performance benchmarking
- Multiple database file support

These will be considered in future iterations if needed.
