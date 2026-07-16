# CQRS/ES Patterns with cqrs-es

Quick reference for cqrs-es usage patterns in this codebase.

## Core Principle: Events Are Immutable

**Events are the source of truth and can NEVER be changed or deleted.**
Everything else - aggregates, commands, views - can be freely modified because
they're derived from events.

- **Commands**: Can add, remove, or change freely
- **Aggregates**: Can restructure, add fields, change logic freely
- **Views**: Can add, drop, restructure freely (just replay from events)
- **Events**: PERMANENT. Think carefully before adding new event types.

This is the power of event sourcing: unlimited flexibility in how you interpret
historical data, as long as you preserve the raw facts.

## Snapshots

Performance optimization that caches aggregate state to skip replaying old
events. **Not currently enabled** — all aggregates use `new_event_store`.

**Enabling:** Replace `new_event_store(repo)` with `new_snapshot_store(repo, N)`
where `N` is the snapshot frequency (events between snapshots). On load, replays
only events after the last snapshot. On commit, writes a new snapshot when the
event count crosses a frequency boundary. Switching between `new_event_store`
and `new_snapshot_store` is safe **only when** existing snapshots are compatible
with the current aggregate shape — if you've changed the aggregate's struct
layout (fields, variants) since the last snapshot was written, you must reset
snapshots first to avoid deserialization failures.

**Resetting:** Deleting snapshots is safe anytime — the next load replays all
events from the beginning. **Must** reset after changing an aggregate's struct
layout (fields, variants) since the serialized snapshot won't deserialize
against the new shape. Events are unaffected.

```sql
-- Example: reset snapshots for the Mint aggregate
DELETE FROM snapshots WHERE aggregate_type = 'Mint';
```

## Evolving Event Structure

event-sorcery has no upcaster layer. When an event's structure must change (a
field is added, or a legacy layout must still deserialize), normalize the old
and new shapes during deserialization with `#[serde(try_from = "Wire")]`:

```rust
/// The union of every wire shape this event has had.
#[derive(Deserialize)]
struct MyEventWire {
    new_field: Option<String>,
    legacy_field: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(try_from = "MyEventWire")]
struct MyEvent {
    new_field: String,
}

#[derive(Debug, thiserror::Error)]
#[error("MyEvent carries neither new_field nor legacy_field")]
struct MissingFieldError;

impl TryFrom<MyEventWire> for MyEvent {
    type Error = MissingFieldError;

    fn try_from(wire: MyEventWire) -> Result<Self, Self::Error> {
        // Normalize the legacy layout into the current one, or error if the
        // payload is unusable.
        wire.new_field
            .or(wire.legacy_field)
            .map(|new_field| Self { new_field })
            .ok_or(MissingFieldError)
    }
}
```

The transformation runs at load time during deserialization, so historical
events read back as the current shape. Reference implementation:
`TokensBurnedData` / `TokensBurnedDataWire` in `src/redemption/event.rs`.

When the wire shape changes, also bump `event_version()` for newly emitted
events of that variant so stored rows record which shape they carry —
`RedemptionEvent::event_version()` emits `"2.0"` for `TokensBurned` while the
other variants stay `"1.0"`.

**Events are immutable** — never rewrite stored payloads; evolve the in-memory
type and let `TryFrom<Wire>` bridge the old and new layouts.

## Views and GenericQuery

Views are read-optimized projections built from events. **Never query view
tables directly with raw SQL** - use `GenericQuery`:

```rust
use cqrs_es::persist::GenericQuery;
use sqlite_es::SqliteViewRepository;

// Create view repository and query
let view_repo = Arc::new(SqliteViewRepository::<MyView, MyAggregate>::new(
    pool.clone(),
    "my_view".to_string(),
));
let query = GenericQuery::new(view_repo.clone());

// Load a view by aggregate ID
let view: Option<MyView> = query.load(&aggregate_id).await;
```

Views implement the `View` trait:

```rust
impl View<MyAggregate> for MyView {
    fn update(&mut self, event: &EventEnvelope<MyAggregate>) {
        match &event.payload {
            MyEvent::Created { ... } => { /* update view state */ }
            MyEvent::Updated { ... } => { /* update view state */ }
        }
    }
}
```

## Re-projecting Views by Rebuilding

event-sorcery reactors only observe newly committed events, so a view added or
changed after events already exist must be rebuilt by replaying the historical
streams. How a view rebuilds depends on which kind it is:

- **Canonical `Table` projections** (one per aggregate, e.g. `mint_view`) are
  rebuilt through event-sorcery itself: `StoreBuilder::build` catches the
  projection up at startup, and `rebuild_all()` replays the full history
  (`mint_projection.rebuild_all()` in `src/lib.rs`). No hand-written SQL replay.
- **Secondary views maintained by explicit reactors** (`receipt_inventory_view`,
  `redemption_view`, `receipt_burns_view`) each own a `rebuild_<view>_view`
  function that queries the aggregate's events, clears the view table, and
  replays each event through the view's reactor:

```rust
pub(crate) async fn rebuild_my_view(
    pool: &Pool<Sqlite>,
) -> Result<(), MyViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            aggregate_id as "aggregate_id!: String",
            payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'MyAggregate'
        ORDER BY aggregate_id, sequence
        "#
    )
    .fetch_all(pool)
    .await?;

    // Parse every event before deleting anything, so a malformed row aborts
    // the rebuild while the existing (possibly stale) rows are still intact
    // rather than leaving the view table empty after the DELETE.
    let events = rows
        .into_iter()
        .map(|row| -> Result<_, MyViewError> {
            let aggregate_id: MyAggregateId = row.aggregate_id.parse()?;
            let event: MyEvent = serde_json::from_str(&row.payload)?;
            Ok((aggregate_id, event))
        })
        .collect::<Result<Vec<_>, _>>()?;

    sqlx::query!("DELETE FROM my_view").execute(pool).await?;

    let reactor = MyViewReactor::new(pool.clone());
    for (aggregate_id, event) in events {
        reactor.project(&aggregate_id, &event).await;
    }

    Ok(())
}
```

Ordering by `(aggregate_id, sequence)` groups each aggregate's events so the
reactor sees them in order. Reference implementation:
`rebuild_receipt_inventory_view` in `src/receipt_inventory/view.rs` (also
`rebuild_redemption_view` and `rebuild_receipt_burns_view`, which predate the
parse-before-delete safeguard).

**Call the rebuild functions at startup** so views reflect any schema changes.
The `events` table is the single source of truth, so a rebuild is idempotent:
re-running it reproduces the same view rows. Two failure modes temper that
guarantee: in the pattern above a deserialization error fails fast, aborting
before the `DELETE`, while a per-event projection write failure is logged and
skipped inside the reactor's `project()` (see
`ReceiptInventoryViewReactor::project`), so a rebuild that returns `Ok` can
still have skipped events — watch for the reactor's WARN logs after a rebuild.

## Services Pattern

Aggregates can depend on external services (APIs, blockchain, etc.) via the
`Services` associated type:

```rust
#[async_trait]
impl Aggregate for MyAggregate {
    type Command = MyCommand;
    type Event = MyEvent;
    type Error = MyError;
    type Services = Arc<dyn MyService>;  // or () if no services needed

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,  // injected by framework
    ) -> Result<Vec<Self::Event>, Self::Error> {
        // Use services in command handlers
        let result = services.do_something().await?;
        Ok(vec![MyEvent::SomethingDone { result }])
    }
}
```

Pass services when creating the CQRS framework:

```rust
let services: Arc<dyn MyService> = Arc::new(MyServiceImpl::new());
let cqrs = CqrsFramework::new(event_store, queries, services);
```

For aggregates that don't need services, use `type Services = ()`.

## Forbidden Patterns

1. **Never query the `events` table inside aggregate handlers or commands** -
   reconstruct state by loading the aggregate. Raw `events` reads are sanctioned
   only as a class: the `rebuild_<view>_view` startup functions (replaying
   history through a reactor) and read-side event-history inspection where
   `event_sorcery::Store` exposes no event-log read — the admin
   reprocess/history endpoints in `src/admin.rs` and `BurnManager`'s retry
   externalTxId derivation in `src/redemption/burn_manager.rs`.
2. **Never query view tables with raw SQL** - use `GenericQuery::load()`
3. **Never modify or delete events** - they're immutable historical facts
4. **Never worry about changing aggregates/views** - they're just
   interpretations
5. **Never add events you don't need yet** - YAGNI applies especially to events

## Testing Aggregates

Use the Given-When-Then pattern with in-memory stores:

```rust
use cqrs_es::mem_store::MemStore;

#[tokio::test]
async fn test_my_command() {
    let store = MemStore::<MyAggregate>::default();
    let cqrs = CqrsFramework::new(store, vec![], services);

    // Given: apply prior events
    cqrs.execute(&id, SetupCommand { ... }).await.unwrap();

    // When: execute command under test
    let result = cqrs.execute(&id, CommandUnderTest { ... }).await;

    // Then: verify result
    assert!(result.is_ok());
}
```

Or for more direct testing, use `AggregateContext`:

```rust
let ctx = store.load_aggregate(&id).await.unwrap();
let aggregate = ctx.aggregate();
// assert on aggregate state
```
