# AGENTS.md

This file provides guidance to AI agents working with code in this repository.

**CRITICAL: File Size Limit** - AGENTS.md must not exceed 40,000 characters.
When editing this file, check the character count (`wc -c AGENTS.md`). If over
the limit, condense explanations without removing any rules.

## Documentation

**Before doing any work**, read these two documents:

1. **[SPEC.md](SPEC.md)** — the north star. Describes what this service should
   be. All new features must be spec'ed here first. If your change contradicts
   the spec, either update the spec first (with user approval) or change your
   approach. Implementation is downstream from the spec.
2. **[docs/workflow.md](docs/workflow.md)** — the mandatory process for all
   changes. Describes how to get from current behavior to the desired behavior
   defined in the spec.

**Read when relevant** to your task:

- [docs/alloy.md](docs/alloy.md) - Alloy types, FixedBytes aliases,
  `::random()`, mocks, encoding, compile-time macros
- [docs/cqrs.md](docs/cqrs.md) - CQRS/ES patterns (upcasters, views, replay,
  services)
- [docs/fireblocks.md](docs/fireblocks.md) - Fireblocks integration
  (externalTxId, SDK error handling)

**Update at the end** (see "After completing a plan" checklist below):

- **ROADMAP.md** — mark completed issues, link PRs
- **SPEC.md** — if aggregates, commands, events, state machines, or APIs changed
- **README.md** — if project structure, features, commands, or architecture
  changed
- **AGENTS.md** — if new patterns or conventions were introduced

### While implementing

- **Work until done:** Don't stop until all tasks are complete or you need user
  input. Keep working through the task list autonomously.
  - The user manually reviews all git diffs, so changes must be minimal and
    focused
  - **Any diff not required to complete the task is a guideline violation** - no
    drive-by improvements, refactorings, or style changes unless explicitly
    included in the scope of the task or requested by the user
- **CRITICAL: Tasks must be ordered correctly in plans**
  - When creating implementation plans, ensure tasks are in the correct order
  - Earlier tasks MUST NOT depend on code from later tasks
  - All tests SHOULD pass at the end of each task whenever possible
  - Focused git diffs and passing tests make reviewing much easier than large
    changesets or verbose changelogs
- The code diffs themselves should be self-explanatory and easy to review

### After completing a plan

When all tasks are complete, perform this checklist **before** creating or
updating a PR:

1. **Update documentation**: See "Update at the end" list above for which docs
   to update and when. For ROADMAP.md specifically: mark completed issues as
   `[x]` with PR link, add `- **PR:** [#N](pr-url)`, and use `gh issue list` and
   `gh pr list` to verify.
2. **Verify GitHub state**:
   - Ensure related issues will be closed when PR merges (use "Closes #N" in PR
     description)
   - Check that no issues are marked complete in ROADMAP.md but still open on
     GitHub

Out-of-date documentation has negative value - it confuses more than it
clarifies.

## Project Overview

This is a Rust-based issuance bot that acts as the **Issuer** in Alpaca's
Instant Tokenization Network (ITN). The bot implements the Issuer-side endpoints
that Alpaca calls during mint/redeem operations, and coordinates with the Rain
`OffchainAssetReceiptVault` contracts to execute the on-chain minting and
burning of tokenized shares.

**This is general infrastructure** - any Authorized Participant (AP) can use it
to mint and redeem tokenized equities. The bot bridges traditional equity
holdings (at Alpaca) and on-chain (semi-fungible) tokenized representations
(Rain SFT contracts).

The system uses **Event Sourcing (ES)** and **CQRS** to maintain a complete
audit trail, enable time-travel debugging, and provide a single source of truth.

## Key Development Commands

### Building & Running

- `cargo build` - Build the project
- `cargo run` - Run the HTTP server

### Dependency Management

- **CRITICAL: Always use `cargo add` to add dependencies** - NEVER manually edit
  version numbers in Cargo.toml
  - `cargo add` automatically selects the latest compatible version
  - Once `cargo add` has determined the version, you can then modify Cargo.toml
    with that knowledge if needed
  - Example: `cargo add chrono` (NOT manually adding `chrono = "0.4.40"`)
  - For workspace dependencies: `cargo add --workspace chrono`, then add to
    package with `chrono.workspace = true`

### Testing

- `cargo test --workspace` - Run all tests (including crates/)
- `cargo test -q` - Run all tests quietly
- `cargo test -q --lib` - Run library tests only
- `cargo test -q <test_name>` - Run specific test

### Database Management

- **CRITICAL: Always use `sqlx migrate add` to create migrations** - NEVER
  manually create migration files
  - `sqlx migrate add` automatically generates the migration file with proper
    timestamp
  - Example: `sqlx migrate add create_account_view` (NOT manually creating
    `20251017000000_create_account_view.sql`)
  - After the file is created, edit it to add the SQL
- **CRITICAL: Fix migrations in place during development** - When working on a
  feature, if you discover a migration you added is incorrect, fix the original
  migration file directly. NEVER add a new migration to fix another migration
  added as part of the same task/feature. Only add fix migrations for issues in
  migrations that have already been merged to main.
- `sqlx db create` - Create the database
- `sqlx migrate run` - Apply database migrations
- `sqlx migrate revert` - Revert last migration
- `sqlx db reset -y` - Drop the database and re-run all migrations
- Database URL configured via `DATABASE_URL` environment variable
- **CRITICAL: the build is OFFLINE sqlx** — `query!` macros use the committed
  `.sqlx/` cache (`SQLX_OFFLINE=true`), so apalis-sqlite (sqlx 0.8) and the
  event store (sqlx 0.9) coexist with no live DB. Stale cache → regenerate (not
  reset DB): set `SQLX_OFFLINE="false"`, `sqlx db reset -y`,
  `cargo sqlx prepare --database-url "sqlite:///ABS/path/issuance.db" --
  --all-targets`,
  restore `"true"`, commit.

### Development Tools

- `cargo fmt --all -- --check` - Check code formatting
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` -
  Run Clippy with all warnings denied
- `cargo fmt` - Format code

### Nix Development Environment

- `nix develop` - Enter development shell with all dependencies

## Development Workflow Notes

- When running `git diff`, make sure to add `--no-pager` to avoid opening it in
  the interactive view, e.g. `git --no-pager diff`

## Architecture Overview

### Event Sourcing and CQRS

ES + CQRS are this bot's architectural foundation.

**Core Concepts:**

- **Aggregates**: Business entities encapsulating state and logic (`Mint`,
  `Redemption`, `Account`, `TokenizedAsset`)
- **Commands**: Requests representing user/system intent (`InitiateMint`,
  `ConfirmJournal`)
- **Events**: Immutable past-tense facts (`MintInitiated`, `JournalConfirmed`)
- **Event Store**: Single source of truth - append-only log of domain events in
  SQLite
- **Views**: Read-optimized projections built from events
- **Services**: External dependencies aggregates use (Alpaca client, blockchain
  client, monitoring)

**Key Flow:**

```
Command -> Aggregate.handle() -> Validate & Produce Events -> Persist Events
  -> Apply to Aggregate -> Update Views
```

**Critical Methods:**

- `handle(command) -> Result<Vec<Event>, Error>`: Business logic. Validates the
  command against current state and returns 0+ events (e.g. `ConfirmJournal` may
  produce both `JournalConfirmed` and `MintingStarted`).
- `apply(event)`: Deterministically updates state from events. Pure, never fails
  - events are historical facts that already occurred.

**Benefits:** complete audit trail; time-travel debugging (replay to any point);
testability via Given-When-Then; rebuild/add views by replaying events; multiple
projections from the same events; event store is the single source of truth, all
else derived.

**CRITICAL: Events Are Permanent:**

- **Events can NEVER be removed or changed** once committed
- Changing event schemas requires complex migration/upcasting
- **Only add events you actually need NOW** - adding more later is easy; events
  are forever. Aggregates, views, and commands can change freely.
- YAGNI applies especially to events: "Do I need this event for a feature I'm
  implementing right now?" If not, don't add it.

### Services and Trait Design in CQRS/ES

**Services represent business capabilities**, not implementation details. A
well-designed service trait:

- **Models domain capabilities**: Methods describe what the system can DO in
  domain terms, not how it's implemented. "Domain" is context-dependent - here,
  burning tokens is domain because 1:1 backing is the value proposition.
- **Plugs into command handlers**: When a command says "do X", the service
  provides the capability to do X; events are produced from what the service
  did.
- **Decouples aggregates from external systems**: Services abstract external
  integrations (blockchain, APIs, other aggregates) behind domain interfaces, so
  implementations swap without changing aggregates.
- **Enables clean testing**: Mock the trait to test aggregate logic without real
  external systems.

Command expresses intent → command handler uses services to execute → event
records what occurred (including service responses).

**Anti-patterns to avoid:**

- Single-method traits that just wrap CQRS commands (no domain logic added)
- Trait methods named after implementation ("record", "persist", "save")
- Inconsistent patterns (some aggregates use traits, others use CQRS directly)
- Traits that don't represent a coherent business capability
- External managers orchestrating complex command sequences across aggregates -
  commands should invoke business actions directly via services, not just record
  what external orchestrators did

**Domain context:** Financial infrastructure where tokens derive value from 1:1
backing. Minting creates backed tokens; burning redeems them. Internal mechanics
(receipt tracking, burn planning) are implementation details - the domain
concern is that tokens are properly backed and redeemable.

### Aggregates

- **Mint**: mint lifecycle, from request through journal confirmation to
  on-chain minting and callback.
- **Redemption**: redemption lifecycle, from detecting an on-chain transfer
  through calling Alpaca to burning tokens.
- **Account**: relationship between AP accounts and our system.
- **TokenizedAsset**: which assets are supported for tokenization.

See SPEC.md for detailed command/event mappings and state machines.

### Aggregates vs Views: Naming and Purpose

Both use enum-based type modeling but serve different purposes, reflected in
naming.

- **Aggregates**: entity state reconstructed by replaying events (O(n)); enforce
  business rules and invariants; naming reflects **entity lifecycle**
  (`NotLinked`/`Linked`, `NotAdded`/`Added`) - where the entity is in its
  lifecycle.
- **Views**: materialized projections optimized for reads, filtered access, and
  cross-entity queries; naming is **query-oriented** (`Unavailable` instead of
  `NotAdded`/`Removed`).

Why it matters: views can map multiple aggregate states to one view state;
query-oriented naming keeps the architectural distinction clear and prevents
confusing entity state (aggregate) with data availability (view).

**Avoid Option wrappers for views:** `GenericQuery.load()` already returns
`Option<V>`, so `Option<View(Option<Data>)>` creates confusing nested Options.
Use enum variants instead.

### CRITICAL: Reading Views with GenericQuery

**ALWAYS use `GenericQuery::load()` to read views. NEVER use raw SQL to parse
JSON from view tables.**

```rust
// CORRECT - Use GenericQuery
pub(crate) type MintViewQuery = GenericQuery<
    SqliteViewRepository<MintView, Mint>, MintView, Mint,
>;

pub(crate) async fn load_mint(query: &MintViewQuery, id: &IssuerMintRequestId) -> Option<MintView> {
    query.load(&Mint::aggregate_id(id)).await
}

// FORBIDDEN - Raw SQL with JSON parsing bypasses type safety
sqlx::query!(r#"SELECT json_extract(payload, '$.field') FROM view"#)
```

**For cross-aggregate queries** (e.g. "find all receipts for underlying X"):
create a dedicated read model with proper SQL columns (not JSON), maintain an
in-memory index updated by event handlers, or iterate known aggregate IDs via
`GenericQuery::load()`.

### HTTP Integration

**Our HTTP Server (Rocket.rs)** implements the Alpaca ITN Issuer endpoints
Alpaca calls (account linking, mint requests, journal confirmations), backed by
SQLite for event store and views, with an async runtime for coordination.

**Endpoints We Implement:**

1. `POST /accounts/connect` - Account linking
2. `GET /tokenized-assets` - List supported assets
3. `POST /inkind/issuance` - Mint request from Alpaca
4. `POST /inkind/issuance/confirm` - Journal confirmation from Alpaca

**Endpoints We Call (Alpaca):**

1. `POST /v1/accounts/{account_id}/tokenization/callback/mint` - Confirm mint
   completed
2. `POST /v1/accounts/{account_id}/tokenization/redeem` - Initiate redemption
3. `GET /v1/accounts/{account_id}/tokenization/requests` - List/poll requests

### Blockchain Integration

**Rain OffchainAssetReceiptVault Contract:** ERC-1155 receipts tracking deposit
IDs, ERC-20 shares representing vault ownership; `deposit()` mints, `withdraw()`
burns.

- **Contract Documentation**: Rain contracts are thoroughly documented with
  inline comments on parameters, behavior, and rationale. Always consult the
  Solidity source for authoritative behavior, parameter meanings, and formulas
  (contracts use 18-decimal fixed-point arithmetic for share ratios):
  - Primary: `lib/ethgild/src/concrete/vault/OffchainAssetReceiptVault.sol`
  - Base: `lib/ethgild/src/abstract/ReceiptVault.sol`

**Redemption Wallet:** on-chain address where APs send tokens to redeem; we
monitor it for incoming transfers.

**MonitorService:** watches the redemption wallet via a WebSocket subscription;
methods `watch_transfers()`, `get_transfer_details()`.

**Signing Backends (`src/fireblocks/`):** two mutually exclusive backends, both
implementing `VaultService`:

- **Local**: `EVM_PRIVATE_KEY` → `RealBlockchainService` (dev/test)
- **Fireblocks**: CONTRACT_CALL → `FireblocksVaultService` (prod, TAP policies)

Key files: `fireblocks/mod.rs` (SignerConfig), `fireblocks/vault_service.rs`
(FireblocksVaultService), `config.rs` (backend selection)

### Core Flows

**Mint Flow:**

1. AP requests mint -> Alpaca calls `/inkind/issuance`
2. We validate and respond with `issuer_request_id` (Command: `InitiateMint`,
   Event: `MintInitiated`)
3. Alpaca journals shares from AP to our custodian account
4. Alpaca confirms journal -> `/inkind/issuance/confirm` (Command:
   `ConfirmJournal`, Events: `JournalConfirmed`, `MintingStarted`)
5. We mint on-chain via `vault.multicall()`, atomically: `deposit()` (mints
   receipts + shares to bot wallet) + `transfer()` (transfers only shares to
   user) (Command: `RecordMintSuccess`, Event: `TokensMinted`)
6. We call Alpaca's callback (Command: `RecordCallback`, Events: `CallbackSent`,
   `MintCompleted`)

**Redemption Flow:**

1. AP sends tokens to our redemption wallet -> we detect transfer (Command:
   `DetectRedemption`, Event: `RedemptionDetected`)
2. We call Alpaca's redeem endpoint (Command: `RecordAlpacaCall`, Event:
   `AlpacaCalled`)
3. We poll for journal completion (Command: `ConfirmAlpacaComplete`, Event:
   `AlpacaJournalCompleted`)
4. We burn on-chain via `vault.withdraw()` (Command: `RecordBurnSuccess`, Event:
   `TokensBurned` - final success state)

### Configuration

Environment variables are defined across the deployment pipeline:

- **`.env.example`**: local-dev template listing all available env vars
- **`.github/workflows/deploy.yaml`**: sets secrets and populates `.env` during
  deployment
- **`docker-compose.template.yaml`**: container config template populated by the
  deployment workflow

### Logging Guidelines

- **Log levels**: ERROR = unrecoverable without intervention, WARN = degraded
  but continuing, INFO = significant business events and state transitions,
  DEBUG = diagnostic detail, TRACE = fine-grained internal steps
- **Structured logging**: Always use structured key-value fields
  (`info!(key = %value, "message")`), not string interpolation in messages
- **Loop body logs must be DEBUG or TRACE**: any log inside a loop or per-item
  iteration must not be INFO or higher - only the summary before/after the loop
- **Actionable context**: include enough structured fields (IDs, counts,
  addresses) to investigate without reproducing
- **No secrets in logs**: never log API keys, private keys, or credentials.
  Request/response bodies are generally fine (this is infrastructure), but use
  judgement - avoid payloads containing credentials or tokens

### Code Quality & Best Practices

- **CRITICAL: Package by Feature, Not by Layer**: organize by business
  feature/domain, never by language primitives or technical layers.
  - **FORBIDDEN**: `types.rs`, `error.rs`, `models.rs`, `utils.rs`,
    `helpers.rs`, `http.rs`, `dto.rs`, `entities.rs`, `services.rs` (as
    catch-all technical-layer modules)
  - **CORRECT**: `account.rs`, `mint.rs`, `redemption.rs` by domain, with
    submodules like `account/cmd.rs`, `account/event.rs` if needed
  - Each feature module contains ALL its code (types, errors, commands, events,
    aggregate, view, endpoints) so you can understand/modify it without jumping
    between files. E.g. `src/account/` holds everything for account linking.
- **Event-Driven Architecture**: Commands produce events which update views
- **SQLite Persistence**: Event store and view repositories backed by SQLite
- **Comprehensive Error Handling**: Custom error types with proper propagation
- **CRITICAL: Use Typed Values in Errors**: Error types must store typed values,
  not string representations. Never `format!("{value:?}")` or `.to_string()` to
  convert typed data (addresses, hashes, IDs) into error-field strings.
  - **FORBIDDEN**: `tx_hash: format!("{hash:?}")` or `address: addr.to_string()`
  - **CORRECT**: `tx_hash: B256` or `address: Address` directly in the struct
  - thiserror's `#[error(...)]` handles display formatting - that's where
    formatting belongs, not in the field types
- **Let the Compiler Guide Error Variants**: Write functions using `?` as if all
  required variants exist, then let the compiler tell you which `From` impls are
  missing. Add `#[from]` variants only for errors the compiler complains about.
  - **FORBIDDEN**: Verbose `.map_err()` calls - use `#[from]` instead
  - **FORBIDDEN**: `.map_err(|e| SomeError::Variant(e.to_string()))` -
    stringly-typed
  - **CORRECT**: Add `#[from]` to error variant, use `?` directly
- **CRITICAL: `#[from]` Variant Naming**: With thiserror's `#[from]`, variant
  names must be generic (mirroring the source error type), not claim what
  operation failed. The `?` operator auto-converts any matching error to the
  variant, so operation-specific claims become false when another operation
  produces the same error type.
  - **FORBIDDEN**: `ReadSecret(#[from] std::io::Error)` /
    `ParseConfig(#[from]
    serde_json::Error)` - claim a specific operation
    failed
  - **CORRECT**: `Io(#[from] std::io::Error)` /
    `Json(#[from] serde_json::Error)`
    - generic, truthful. Use context from where the error is handled.
- **CRITICAL: Make Invalid States Unrepresentable**: Use ADTs/enums to encode
  business rules and state transitions in types, not runtime validation.
  - **FORBIDDEN**: Types with all/most fields `Option`; nullable fields that can
    contradict; multiple `bool`s encoding one status (e.g.
    `{ enabled: bool, frozen: bool }` — four combos, only some valid); string
    status fields paired with Options present only for certain statuses
  - **CORRECT**: Enum variants for mutually exclusive states, with
    state-specific data inside each variant; newtypes to prevent mixing
    incompatible values. A status with N states is ONE enum of N variants, never
    N booleans (`enum AssetStatus { Enabled, Frozen }`)
  - Example: `enum Account { NotLinked, Linked { client_id, email, ... } }`, NOT
    `struct Account { client_id: Option<ClientId>, email: Option<Email>, ... }`
- **CRITICAL: Parse, Don't Validate**: If a value exists, it must be valid.
  Validation happens at construction through smart constructors, not a separate
  step callers might forget.
  - **FORBIDDEN**: Separate `validate()` methods; raw primitives (String, i64)
    for constrained domain values; public fields/constructors that bypass
    validation
  - **CORRECT**: Newtypes with private inner values and fallible smart
    constructors as the ONLY way to create the value (so existence ⇒ validity)
  - Applies to ALL constrained domain types: API keys, emails, quantities,
    addresses, IDs, etc.
    ```rust
    // existence ⇒ validity: private inner, fallible new() the only constructor
    pub struct ApiKey(String);
    impl ApiKey {
        pub fn new(value: String) -> Result<Self, ApiKeyError> {
            if value.len() < 32 { return Err(ApiKeyError::TooShort); }
            Ok(Self(value))
        }
    }
    ```
- **Schema Design**: Avoid database columns that can contradict each other. Use
  constraints and normalization for consistency at the database level. Align
  schemas with type modeling principles where possible.
- **SQL Formatting**: Long SQL queries in Rust must be broken across multiple
  lines, each clause and column list on its own line. Never use `\` line
  continuations in SQL strings — use actual newlines:
  ```rust
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
      VALUES (
          'TokenizedAsset',
          ?,
          1,
          'TokenizedAssetEvent::Added', '1.0', ?, '{}'
      )
      ",
  )
  .bind(&aggregate_id)
  .bind(&event_payload_str)
  .execute(&pool)
  .await?;
  ```
- **Functional Programming Patterns**: Favor FP and ADT patterns over OOP. Avoid
  unnecessary encapsulation, inheritance, or getters/setters that don't fit
  Rust's ADTs. Use pattern matching, combinators, and type-driven design.
- **Idiomatic Functional Programming**: Prefer iterator-based patterns over
  imperative loops unless it increases complexity. Use itertools to do more with
  iterators.
- **Comments**: Follow the commenting guidelines (see section below)
- **Spacing**: Leave an empty line between code blocks (for vim curly-brace
  jumping and readability)
- **FORBIDDEN**: Single-letter variable and argument names. All names must
  convey meaning without surrounding context.
- **CRITICAL: Import Organization**: Two-group pattern everywhere (including
  `#[cfg(test)] mod tests`):
  - **Group 1 - External**: external crates (`std`, `alloy`, `cqrs_es`, `serde`,
    `tokio`, …), no blank lines between them.
  - **One blank line** separating the groups.
  - **Group 2 - Internal**: our code via `crate::` / `super::`, no blank lines
    between them.
  - **FORBIDDEN**: Three+ groups; blank lines within a group; function-level
    imports. **Exceptions** (function-body `use` only): (1) enum variant imports
    (`use MyEnum::{A, B}`) — never at module level; (2) imports used solely
    inside `#[cfg(...)]`-gated code, where a module-scope import would warn as
    unused under the inverse config.
  - Module declarations (`mod foo;`) may appear between imports.
    ```rust
    use std::sync::Arc;
    use alloy::primitives::Address;

    use crate::account::ClientId;
    use super::{Mint, MintCommand};
    ```
- **Import Conventions**: Always import types and use them unqualified unless
  the name is genuinely ambiguous within the crate (e.g.
  `alloy::rpc::types::Log` vs `alloy::primitives::Log`). When ambiguous, use
  qualified imports (`rpc_types::Log`, `contract::Error`). Never use
  fully-qualified paths for non-ambiguous types - import them at the module top.
  - **FORBIDDEN**: `alloy::primitives::B256` inline - import `B256` at the top
  - **CORRECT**: `rpc_types::Log` when `Log` is ambiguous
- **CRITICAL: Zero Tolerance for Panics in Non-Test Code**: This is
  mission-critical financial software. ANY panic in production code is
  unacceptable.
  - **FORBIDDEN**: `unwrap()`, `expect()`, `panic!()`, `unreachable!()`,
    `unimplemented!()` in non-test code
  - **FORBIDDEN**: Panicking index operations (`vec[i]`) - use `.get(i)`;
    division without checking for zero; any operation that can panic at runtime
  - **REQUIRED**: `?` for propagation; `Result`/`Option` with explicit handling;
    all fallible operations return `Result` with descriptive errors
  - **Exception**: `unwrap()`/`expect()` are ONLY allowed in test code
    (`#[cfg(test)]` modules and `#[test]` functions)
  - Panics in production code are deployment-blocking bugs - fix immediately
- **Visibility Levels**: Keep visibility as restrictive as possible (prefer
  `pub(crate)` over `pub`, private over `pub(crate)`) to enable better dead-code
  detection and make the relevance scope explicit.

### CRITICAL: Financial Data Integrity

**NEVER** silently provide wrong values, hide conversion errors, or mask
failures. FORBIDDEN: defensive value capping that hides overflow/underflow;
fallback to defaults on conversion failure (`unwrap_or`, `unwrap_or_default`);
silent precision truncation; "graceful degradation" in conversion functions.

**All financial operations must use explicit error handling:**

```rust
// WRONG - Silent cap             | CORRECT - Explicit error
fn to_i64(v: u64) -> i64 {        | fn to_i64(v: u64) -> Result<i64, Error> {
    if v > i64::MAX as u64 {      |     v.try_into().map_err(|_| Error::TooLarge { v })
        i64::MAX // silent cap    | }
    } else { v as i64 }           |
}                                 |

// WRONG - Masks DB violation     | CORRECT - Let the constraint fail
let safe = amt.clamp(0, MAX);     | sqlx::query!("INSERT ...", amt).execute(pool)?;
sqlx::query!("...", safe)...      |
```

**Must fail fast:** numeric conversions (`try_into`), precision loss, range
violations, parse failures, arithmetic (use checked ops), DB constraints. **Fail
fast > corrupted data** - silent corruption leads to massive losses.

### CRITICAL: Security and Secrets Management

**NEVER read credential files without explicit user permission.** Common
credential files to avoid:

- `.env`, `.env.*`, `.env.local`, `.env.production` - API keys, secrets
- `credentials.json`, `secrets.json` - Credential storage
- `*.key`, `*.pem`, `*.p12`, `*.pfx` - Private keys and certificates

**When debugging config issues:** ask the user to verify env vars are set,
request sanitized output, or check `.env.example` instead of the real `.env`.

## Database Schema

SQLite with event sourcing. The event store is the single source of truth; views
are derived projections. See `migrations/` for complete schemas.

### Event Store

```sql
CREATE TABLE events (
    aggregate_type TEXT NOT NULL,  -- 'Mint', 'Redemption', 'Account', 'TokenizedAsset'
    aggregate_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,      -- Sequence number (starts at 1)
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    payload JSON NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);
```

A `snapshots` table caches aggregate state as a performance optimization (PK
`(aggregate_type, aggregate_id)`, plus `last_sequence`, `payload`, `timestamp`)
and can be deleted anytime - aggregates rebuild from events alone.

### Views

All views follow the same pattern — a table with `view_id TEXT PRIMARY KEY`,
`version BIGINT`, and `payload JSON` — implement the `View` trait, and are
updated by `GenericQuery` on event commit. See `migrations/` for all view table
definitions and indexes.

## Testing Strategy

### Testing Pyramid

More tests at lower levels, fewer at higher:

1. **Property tests** - Most numerous. Use proptest for invariant testing
2. **Unit tests** - Aggregate logic with MemStore, exhaustive edge cases
3. **Integration tests** - HTTP endpoints with mocked dependencies
4. **E2E tests** - Fewest, but essential for system orchestration

**The pyramid is about quantity, not avoidance.** Have MANY property/unit tests,
SOME integration tests, and a FEW e2e tests. E2e tests are still required for
full system orchestration:

- Multiple async processes coordinating (backfilling, monitoring, processing)
- Startup/shutdown behavior and recovery
- Flows spanning multiple aggregates AND external systems
- Handling events that occurred before the service started

**Example:** Receipt backfilling + live monitoring REQUIRE e2e because unit
tests verify backfiller and monitor logic in isolation, but only e2e verifies
(1) service starts, (2) backfills historic receipts, (3) monitors new receipts,
(4) redemption uses receipts from both sources. One well-designed e2e test
covers the orchestration; dozens of unit tests cover the component edge cases.

### Given-When-Then Aggregate Testing

ES/CQRS enables testable business logic: **Given** previous events -> **When**
command -> **Then** expect events (or error).

```rust
#[test]
fn test_journal_confirmed() {
    MintTestFramework::with(mock_services)
        .given(vec![MintInitiated { issuer_request_id: "iss-456", /* ... */ }])
        .when(ConfirmJournal { issuer_request_id: "iss-456" })
        .then_expect_events(vec![JournalConfirmed { /* ... */ }, MintingStarted { /* ... */ }]);
}
```

### Testing Infrastructure

- **In-memory event store**: `cqrs-es` MemStore for fast aggregate tests
- **Mock external systems**: `httpmock` for Alpaca API; mock blockchain for
  determinism
- **Database isolation**: in-memory SQLite per test

### End-to-End Tests: Strict Definition

**A test is ONLY e2e if it:**

1. Spins up the full HTTP service
2. Uses ONLY the public API as an external consumer would
3. Uses Anvil for local blockchain
4. Mocks only truly external systems (Alpaca API)
5. Asserts correctness via API responses, Anvil state, and mock interactions

**Setup phase exception:** Some edge-case scenarios (e.g. recovering from a
`MintingFailed` state) cannot be induced through the public API alone. The
**setup phase** may use direct SQL to seed the **event store only** (rows in the
`events` table). No other tables — including derived views and read models — may
be pre-populated via SQL; views must be rebuilt by the running service during
scenario execution. Once setup is complete, **scenario execution and
verification** must follow all e2e rules above — the service runs, reacts to
real events (blockchain, monitors), and correctness is asserted via API
responses, Anvil state, and mock interactions.

**A test is NOT e2e if it:**

- Touches implementation details for **verification** (querying aggregates
  directly, inspecting internal state after the scenario runs)
- Touches implementation details during **scenario execution** (calling CQRS
  commands to drive the flow instead of letting the service react)
- Requires access to internal types or functions outside the setup phase

**If a test requires touching implementation details beyond setup**, it belongs
in `src/` as a unit/integration test, NOT in `tests/`. E2E tests live in
`./tests/`, test complete production flows, happy paths only, with real
blockchain (Anvil).

### Testing Guidelines

- Write tests before changing logic. When writing tests for existing code, don't
  assume current behavior is correct - it may have bugs.
- Add context to failing `assert!` macros instead of temporary `println!`
- Never test language features - test business logic
- **Tests must verify both behavior and observability.** Every test that
  exercises business logic must also assert on expected log output (via
  `tracing-test`). Observability is not optional - if code should log something,
  the test must verify it does. Don't create separate test cases for logging;
  add log assertions alongside behavioral assertions in the same test.
- **Use `logs_contain_at` for log assertions.** The helper
  `logs_contain_at(level, &["snippet1", "snippet2"])` checks that a single log
  line at the given level contains all specified snippets, ensuring the right
  information appears together in one entry:

```rust
#[traced_test]
#[test]
fn ingestion_logs_progress() {
    // ... trigger ingestion ...
    assert!(logs_contain_at(Level::DEBUG, &["fetching", "BTC"]));
    assert!(logs_contain_at(Level::DEBUG, &["fetched", "1"]));
}
```

```rust
// Bad: Tests struct assignment, not our code
fn test_fields() {
    let r = MintRequest { qty: 100.into(), underlying: "AAPL".into() };
    assert_eq!(r.qty, 100.into());
}

// Good: Tests our validation logic
fn test_validates_quantity() {
    let result = Mint::default().handle(InitiateMint { qty: (-10).into(), .. });
    assert!(matches!(result, Err(MintError::InvalidQuantity)));
}
```

## Workflow Best Practices

**Workflow (TTDD - Type-driven TDD)** sequence:

1. **Types first**: Define types, traits, and method signatures that model the
   domain
2. **Failing tests**: Write tests that compile but fail (build errors don't
   count as failing tests)
3. **Implementation**: Write the logic to make tests pass

**`todo!()` macro:** Encouraged during the types-first stage to stub signatures.
Must be removed before completion - any `todo!()` in final code is unacceptable.

While developing, continuously run `cargo check` and `cargo test` to verify
types and behavior.

**CRITICAL: `cargo clippy` is the final polish — never run it until ALL
implementation tasks are complete.** Running clippy on incomplete work is
pointless; fix the logic first, then polish. Only after all tasks pass their
tests, run `cargo clippy` and fix all warnings. Finally `cargo fmt` before
committing.

**CRITICAL: Never use `cargo build` for verification.** Use `cargo check`
(faster) or `cargo test` (more useful). Only `cargo build` when you need the
binary.

- **Before handing over a piece of work**, run checks in this order:
  1. `cargo test --workspace` - All tests must pass
  2. `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` -
     Fix any linting issues (minimal if code is well-structured)
  3. `cargo fmt --all` - Format the code

### CRITICAL: Lint Policy

**NEVER add `#[allow(...)]` attributes or disable any lints without explicit
user permission.** Applies to ALL lint attributes (`clippy::*`, `dead_code`,
`unused_imports`, etc.).

**Fix the root cause instead of suppressing:**

1. Refactor to address why clippy flags the code (understand the lint first)
2. Break large functions into smaller, focused ones
3. Use proper error handling instead of suppressing warnings
4. Remove unused code instead of allowing `dead_code`/`unused_imports`
5. If you believe a lint is genuinely wrong, ask permission before suppressing,
   and document the reasoning if granted

**Exception — third-party macro-generated code:** When a macro like `sol!`
generates code from a contract ABI we don't control, lint suppression is
acceptable inside the macro invocation:

```rust
sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, "node_modules/@pythnetwork/pyth-sdk-solidity/abis/IPyth.json"
);
```

## CRITICAL: Evidence-Based Claims

**NEVER make claims about code behavior, external systems, or technical facts
without evidence from the actual source code or documentation.** This is
zero-tolerance; speculation presented as fact is unacceptable.

**FORBIDDEN:** "This is how ERC-20 works" without citing the contract code; "The
API expects X" without showing the docs/code that prove it; "This function does
Y" without reading it first; any claim about inheritance, call stacks, or
behavior without tracing the actual source files.

**REQUIRED:** Before any claim about behavior, READ the relevant source first;
cite exact file paths and line numbers; for inheritance/call chains trace each
step with file references; if you haven't read the code, say "I don't know" or
"let me check".

```text
// FORBIDDEN: "ERC-20 minting emits Transfer from zero address"
// REQUIRED: "Let me check..." [reads files] "Found it:
//   deposit() -> ReceiptVault._deposit() (ReceiptVault.sol:559) -> _mint()
//   (ReceiptVault.sol:587); ReceiptVault inherits ERC20Upgradeable, whose
//   _mint() emits Transfer(0, to, amt) (ERC20Upgradeable.sol:266)"
```

**When documenting non-obvious behavior in code comments**, include the full
reference chain. See `src/redemption/detector.rs` for an example.

## Commenting Guidelines

Code should be self-documenting. Comment only when adding context that code
structure cannot express.

**DO comment:** Complex business logic, algorithm rationale, external system
behavior, non-obvious constraints, test data context, workarounds.

**DON'T comment:** Self-explanatory code, restating what code does, obvious
assignments, test section markers, section dividers (e.g. `// ========`),
references to tasks/issues (reviewers won't have access to transient planning
artifacts).

```rust
// Good: Explains WHY (business rule)
// Alpaca requires journal confirmation before minting - otherwise we risk
// minting without backing shares.
let confirmed = wait_for_journal_confirmation(&mint_id).await?;

// Bad: Restates WHAT (obvious from code)
// Execute mint command
execute_mint_command(mint);
```

Use `///` doc comments for public APIs. Keep comments focused on "why" not
"what".

## Code Style

### Module Organization

Order by visibility: **public API first** -> **impl blocks after types** ->
**private helpers last**. This makes the public interface immediately visible.

```rust
pub(crate) struct MintRequest { /* fields */ }  // Public type first
impl MintRequest { pub(crate) async fn save(&self, ..) -> Result<..> { } }

pub(crate) async fn find_mints(..) -> Result<Vec<MintRequest>, Error> {
    let rows = query_by_status(..)?;  // Uses private helper below
    rows.into_iter().map(row_to_mint).collect()
}

async fn query_by_status(..) -> Result<Vec<MintRow>, Error> { }  // Private helper
fn row_to_mint(row: MintRow) -> Result<MintRequest, Error> { }   // Private helper
```

### Test assertions

Use `.unwrap()` directly - if unexpected, you see the value immediately. Prefer
`assert!(matches!(result.unwrap_err(), MintError::InvalidQuantity))` over a
separate `is_err()` check plus a `match`.

### Type modeling

**Make invalid states unrepresentable** (see the Code Quality rule above): use
enums with state-specific data, not contradictory `Option` fields (e.g.
`enum MintStatus { Pending, Completed { tx_hash, shares }, Failed { reason } }`),
and use newtypes (`TokenizationId`, `IssuerId`, `Symbol`) so incompatible values
can't be mixed up in function arguments.

### Avoid deep nesting

Use early returns and `let-else` for flat code instead of nested `if let` / `if`
pyramids:

```rust
fn validate(d: Option<&Data>) -> Res<()> {
    let d = d.ok_or(Error::NoData)?;
    if d.qty <= 0 { return Err(Error::Qty); }
    if !d.sym.ok() { return Err(Error::Sym); }
    Ok(())
}
```

### Struct field access

Prefer direct field access over unnecessary constructors/getters:

```rust
let req = MintRequest { qty: 100.into(), underlying: "AAPL".into(), .. };
println!("{}", req.qty); // direct access; don't add getters like
// `fn qty(&self) -> Decimal { self.qty }` that just forward to fields
}
```
