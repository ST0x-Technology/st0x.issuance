# Implementation Plan: SqliteViewRepository (Issue #9)

This plan outlines the implementation of `SqliteViewRepository` in
`crates/sqlite-es`, which provides SQLite-backed view persistence for the CQRS
framework.

## Background

The `cqrs-es` framework uses the `ViewRepository` trait to handle database
access for views (read models). Views are read-optimized projections built from
events that maintain queryable state. Each view table follows a standard
pattern:

- `view_id TEXT PRIMARY KEY` - unique identifier for the view instance
- `version BIGINT NOT NULL` - last event sequence applied (for optimistic
  locking)
- `payload JSON NOT NULL` - serialized view state as JSON

The `ViewRepository` trait (from `cqrs-es`) has three methods:

1. `load` - loads a view instance by ID
2. `load_with_context` - loads a view with version information for optimistic
   locking
3. `update_view` - persists an updated view with optimistic locking support

Our implementation will follow the same pattern as `SqliteEventRepository`
(completed in issue #8), using `sqlx` for database access and storing view state
as JSON.

## Design Decisions

### Generic Implementation vs. Table-Specific Repositories

**Decision:** Implement a generic `SqliteViewRepository<V, A>` that can work
with any view table, rather than creating separate repository implementations
for each view type.

**Reasoning:**

- All view tables follow the same schema pattern (view_id, version, payload)
- Generic implementation reduces code duplication
- Matches the pattern from `cqrs-es` ecosystem (postgres-es, mysql-es)
- Views are already parameterized by aggregate type, so generics are natural
- Table name is configurable via constructor, enabling reuse

### View Storage Format

**Decision:** Store view state as JSON in the `payload` column using
`serde_json`.

**Reasoning:**

- Views implement `Serialize + DeserializeOwned` trait bounds
- JSON storage is flexible and allows views to evolve
- Consistent with event storage approach (events are also stored as JSON)
- SQLite has good JSON support with json_extract for indexed queries
- Matches the database schema from SPEC.md

### Optimistic Locking Strategy

**Decision:** Use version-based optimistic locking where the version represents
the last event sequence applied to the view.

**Reasoning:**

- Prevents lost updates when multiple processes update the same view
- `GenericQuery` uses version to ensure views are updated in order
- Version increment on each update provides ordering guarantee
- Matches the approach used by `cqrs-es` framework

### Error Handling

**Decision:** Define a `SqliteViewError` enum similar to `SqliteAggregateError`,
with conversion to `PersistenceError`.

**Reasoning:**

- Provides structured error information for debugging
- Distinguishes between connection errors, serialization errors, and optimistic
  lock conflicts
- `From<SqliteViewError> for PersistenceError` enables easy error propagation
- Consistent with error handling in `SqliteEventRepository`

## Task 1. Create View Table SQL Query Factory

Extend `SqlQueryFactory` to generate SQL queries for view operations.

### Subtasks

- [ ] Add view table name field to `SqlQueryFactory` struct
- [ ] Update constructors (`new`, `with_tables`) to accept view table name
      parameter
- [ ] Implement `select_view()` method that generates query to load view by ID
- [ ] Implement `insert_or_update_view()` method that generates upsert query
- [ ] Add unit tests for new query generation methods
- [ ] Update existing `SqlQueryFactory` tests if needed

### Implementation Notes

The query factory should generate queries like:

```sql
-- select_view
SELECT view_id, version, payload FROM {view_table} WHERE view_id = ?

-- insert_or_update_view
INSERT OR REPLACE INTO {view_table} (view_id, version, payload)
VALUES (?, ?, ?)
```

The upsert query uses SQLite's `INSERT OR REPLACE` syntax to handle both initial
inserts and subsequent updates.

## Task 2. Implement SqliteViewError Type

Create error types specific to view repository operations.

### Subtasks

- [ ] Define `SqliteViewError` enum in a new `view_repository.rs` file
- [ ] Add error variants:
  - `OptimisticLock` - concurrent modification detected
  - `Connection` - database connection/query error
  - `Serialization` - view serialization error
  - `Deserialization` - view deserialization error
  - `TryFromInt` - integer conversion error
- [ ] Implement `Display` and `Error` traits via `thiserror`
- [ ] Implement `From<SqliteViewError> for PersistenceError` conversion
- [ ] Map error variants to appropriate `PersistenceError` types

### Implementation Notes

Error mapping:

- `OptimisticLock` → `PersistenceError::OptimisticLockError`
- `Connection` → `PersistenceError::ConnectionError`
- `Serialization` → `PersistenceError::UnknownError` (serialization should
  rarely fail for valid views)
- `Deserialization` → `PersistenceError::DeserializationError`
- `TryFromInt` → `PersistenceError::UnknownError`

## Task 3. Implement SqliteViewRepository Struct

Create the main repository struct with database connection and configuration.

### Subtasks

- [ ] Define `SqliteViewRepository<V, A>` struct with generic parameters
- [ ] Add fields:
  - `pool: Pool<Sqlite>` - database connection pool
  - `view_table: String` - name of the view table
  - `_phantom: PhantomData<(V, A)>` - zero-sized marker for generic parameters
- [ ] Implement constructor `new(pool: Pool<Sqlite>, view_table: String)`
- [ ] Add trait bounds `V: View<A>, A: Aggregate` to ensure type safety
- [ ] Document the struct and its generic parameters

### Implementation Notes

The struct uses `PhantomData` because it doesn't directly store values of type
`V` or `A`, but needs to be generic over them for the `ViewRepository` trait
implementation. The `view_table` parameter allows using different table names
for different view types (e.g., "mint_view", "redemption_view").

## Task 4. Implement ViewRepository::load Method

Implement loading a view instance by ID without version context.

### Subtasks

- [ ] Implement `load(&self, view_id: &str)` method
- [ ] Build SELECT query using table name
- [ ] Execute query with sqlx, binding view_id parameter
- [ ] Handle case when view doesn't exist (return `Ok(None)`)
- [ ] Deserialize JSON payload to view type `V`
- [ ] Map database/deserialization errors to `PersistenceError`
- [ ] Add comprehensive error handling

### Implementation Notes

This method is simpler than `load_with_context` as it only needs to deserialize
the payload, not extract version information. It's used when the caller only
needs the view data without needing to update it.

Query:

```sql
SELECT payload FROM {view_table} WHERE view_id = ?
```

## Task 5. Implement ViewRepository::load_with_context Method

Implement loading a view with version information for optimistic locking.

### Subtasks

- [ ] Implement `load_with_context(&self, view_id: &str)` method
- [ ] Build SELECT query to fetch both payload and version
- [ ] Execute query with sqlx
- [ ] Handle case when view doesn't exist (return `Ok(None)`)
- [ ] Extract version from database row (i64 type)
- [ ] Deserialize JSON payload to view type `V`
- [ ] Construct `ViewContext` with view_id and version
- [ ] Return tuple `(V, ViewContext)` wrapped in Option
- [ ] Map errors appropriately

### Implementation Notes

Query:

```sql
SELECT view_id, version, payload FROM {view_table} WHERE view_id = ?
```

The `ViewContext` contains:

- `view_instance_id` - the view_id (for identification)
- `version` - current version number (for optimistic locking)

## Task 6. Implement ViewRepository::update_view Method

Implement persisting an updated view with optimistic locking.

### Subtasks

- [ ] Implement `update_view(&self, view: V, context: ViewContext)` method
- [ ] Serialize view to JSON using `serde_json::to_value`
- [ ] Convert JSON Value to string for storage
- [ ] Increment version number (`context.version + 1`)
- [ ] Build INSERT OR REPLACE query
- [ ] Execute query with parameters: view_id, new_version, serialized_payload
- [ ] Handle serialization errors
- [ ] Handle database errors
- [ ] Map errors to `PersistenceError`

### Implementation Notes

SQLite's `INSERT OR REPLACE` handles both initial inserts (version 0 → 1) and
updates (version N → N+1). The version increment happens in the application
layer, and the new version is stored in the database.

The `GenericQuery` framework is responsible for:

1. Loading view with context (gets current version)
2. Applying events to update view state
3. Calling update_view with incremented version

If two processes try to update simultaneously with the same starting version,
one will succeed and the other should detect a conflict. However, SQLite's
`INSERT OR REPLACE` doesn't provide row-level locking like PostgreSQL's
`UPDATE ... WHERE version = ?` approach. We may need to implement explicit
version checking.

**Note:** Review whether SQLite's `INSERT OR REPLACE` provides sufficient
optimistic locking guarantees, or if we need to use a different approach (e.g.,
separate INSERT vs UPDATE paths with version checking).

## Task 7. Add Optimistic Locking Support

Ensure concurrent updates are detected and prevented.

### Subtasks

- [ ] Research SQLite optimistic locking patterns for JSON-based updates
- [ ] Determine if `INSERT OR REPLACE` is sufficient or if we need
      `UPDATE ... WHERE version = ?`
- [ ] Implement version-based conflict detection if needed
- [ ] Return `OptimisticLock` error when version mismatch detected
- [ ] Document the locking strategy in code comments
- [ ] Ensure behavior matches `cqrs-es` framework expectations

### Implementation Notes

Two possible approaches:

**Approach A: INSERT OR REPLACE (simpler but weaker guarantees)**

```sql
INSERT OR REPLACE INTO {view_table} (view_id, version, payload)
VALUES (?, ?, ?)
```

This always succeeds but doesn't detect conflicts.

**Approach B: UPDATE with version check (stronger guarantees)**

```sql
UPDATE {view_table}
SET version = ?, payload = ?
WHERE view_id = ? AND version = ?
```

Check affected rows; if 0, either view doesn't exist or version mismatch
occurred.

**Recommendation:** Start with Approach A for simplicity, add tests to verify
behavior, then enhance if needed. The `GenericQuery` framework may provide
additional safeguards at a higher level.

## Task 8. Write Comprehensive Tests

Implement thorough test coverage for all repository operations.

### Subtasks

- [ ] Create test module in `view_repository.rs`
- [ ] Define test view type implementing `View<TestAggregate>` trait
- [ ] Reuse `TestAggregate` from event_repository tests or create new one
- [ ] Write test: `test_load_nonexistent_view` - loading missing view returns
      None
- [ ] Write test: `test_load_view` - loading existing view returns correct data
- [ ] Write test: `test_load_with_context` - returns view and version correctly
- [ ] Write test: `test_update_view_insert` - first update creates view with
      version 1
- [ ] Write test: `test_update_view_increment_version` - subsequent updates
      increment version
- [ ] Write test: `test_view_serialization_roundtrip` - view survives
      serialize/deserialize
- [ ] Write test: `test_multiple_views` - multiple views can coexist in same
      table
- [ ] Write test: `test_optimistic_locking` (if implemented) - concurrent
      updates handled correctly
- [ ] Add integration tests with `GenericQuery` if feasible
- [ ] Ensure all tests use `create_test_pool()` for isolation

### Implementation Notes

Test view should be simple:

```rust
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
struct TestView {
    count: i64,
    values: Vec<String>,
}

impl View<TestAggregate> for TestView {
    fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
        match event.payload {
            TestEvent::Created => self.count += 1,
            TestEvent::Updated { value } => self.values.push(value.clone()),
        }
    }
}
```

All tests should create their own in-memory database using `create_test_pool()`
to ensure isolation.

## Task 9. Add View Table Migration

Create database migration for a generic view table structure.

### Subtasks

- [ ] Decide on migration approach: generic template vs. specific view tables
- [ ] If generic: create migration with parameterized table name (may not be
      directly supported)
- [ ] If specific: add migrations for known view types (mint_view,
      redemption_view, etc.)
- [ ] Create migration file with CREATE TABLE statement
- [ ] Add indexes for common query patterns (by view_id is covered by PRIMARY
      KEY)
- [ ] Update `testing.rs` if needed to apply new migrations
- [ ] Test migrations apply correctly with `sqlx migrate run`

### Implementation Notes

Since SPEC.md defines specific view tables (mint_view, redemption_view, etc.),
we should create individual migrations for each view type rather than a generic
template.

Example migration for mint_view:

```sql
CREATE TABLE IF NOT EXISTS mint_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_mint_view_status
    ON mint_view(json_extract(payload, '$.status'));
CREATE INDEX IF NOT EXISTS idx_mint_view_client
    ON mint_view(json_extract(payload, '$.client_id'));
```

**Note:** This task may be deferred to when actual view types are implemented in
later phases (Phase 2+). For now, focus on the repository implementation and
test with test-only view tables.

## Task 10. Update Module Exports and Documentation

Make the new repository available to library consumers.

### Subtasks

- [ ] Add `mod view_repository;` to `lib.rs`
- [ ] Export `SqliteViewRepository` from `lib.rs`
- [ ] Export `SqliteViewError` from `lib.rs`
- [ ] Re-export `ViewContext` from `cqrs_es::persist` if needed
- [ ] Add crate-level documentation about view repository usage
- [ ] Update README.md in `crates/sqlite-es` if one exists
- [ ] Add code example showing how to use `SqliteViewRepository` with
      `GenericQuery`

### Implementation Notes

The `lib.rs` should export:

```rust
pub use view_repository::{SqliteViewRepository, SqliteViewError};
```

Documentation should explain:

- How to create a view repository for a specific view type
- How to configure the table name
- How views are stored (JSON in payload column)
- Example usage with `GenericQuery`

## Task 11. Run Tests and Code Quality Checks

Ensure all tests pass and code meets quality standards.

### Subtasks

- [ ] Run `cargo test -q` and ensure all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all` and fix
      all warnings
- [ ] Run `cargo fmt` to format code
- [ ] Verify no unwrap() calls in non-test code
- [ ] Check that all public APIs have documentation comments
- [ ] Review error handling for completeness
- [ ] Ensure visibility levels are as restrictive as possible
- [ ] Check for any dead code or unused imports

### Implementation Notes

Pay special attention to:

- All numeric conversions use `try_into()` (no `as` casts)
- All database operations have proper error handling
- All serialization/deserialization is wrapped in Result
- Tests use `assert!` with `.unwrap()` pattern, not `.is_ok()` checks
- No `#[allow(clippy::*)]` attributes without explicit permission

## Dependencies

This task depends on:

- ✅ Issue #8 (SqliteEventRepository) - provides patterns and infrastructure to
  follow

This task is required for:

- ⏳ Issue #10 onwards - view repositories are needed for implementing features
  with read models

## Success Criteria

- [ ] `SqliteViewRepository<V, A>` implements `ViewRepository<V, A>` trait
- [ ] All three trait methods (`load`, `load_with_context`, `update_view`) are
      implemented
- [ ] Views are correctly serialized/deserialized to/from JSON
- [ ] Version-based optimistic locking is implemented (at minimum, version
      increments correctly)
- [ ] Comprehensive error handling with proper error types
- [ ] All tests pass with good coverage of success and error cases
- [ ] No clippy warnings
- [ ] Code follows project conventions (comments, visibility, error handling)
- [ ] Documentation explains usage patterns

## Open Questions

1. **Optimistic Locking:** Should we implement strong version-checking (UPDATE
   WHERE version = ?) or rely on INSERT OR REPLACE with version increments? Need
   to verify what `cqrs-es` expects.

2. **View Table Migrations:** Should view table migrations be part of this PR,
   or deferred until actual view types are implemented in later phases?

3. **Table Naming Convention:** Should we enforce a naming convention for view
   tables (e.g., must end with "_view") or allow arbitrary names?

## Testing Strategy

All tests will use in-memory SQLite databases created with `create_test_pool()`
for isolation. Test coverage should include:

- **Happy paths:** Loading views, updating views, version increments
- **Edge cases:** Non-existent views, empty views, large payloads
- **Error cases:** Serialization failures, database connection errors
- **Concurrency:** Optimistic locking (if implemented)
- **Round-trip:** View data integrity through serialize/deserialize cycle
