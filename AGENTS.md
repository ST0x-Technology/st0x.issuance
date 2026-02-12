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

- **README.md** — if project structure, features, commands, or architecture
  changed
- **ROADMAP.md** — mark completed issues, link PRs

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

1. **Update documentation** (see the Documentation section above for what each
   doc covers):
   - **ROADMAP.md**: Mark completed issues as `[x]` with PR link, add
     `- **PR:** [#N](pr-url)`. Use `gh issue list` and `gh pr list` to verify.
   - **SPEC.md**: If aggregates, commands, events, state machines, or APIs
     changed
   - **README.md**: If project structure, features, commands, or architecture
     changed
   - **AGENTS.md**: If new patterns or conventions were introduced
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
`OffchainAssetReceiptVault` contracts to execute the actual on-chain minting and
burning of tokenized shares.

**This is general infrastructure** - any Authorized Participant (AP) can use it
to mint and redeem tokenized equities. The issuance bot serves as the bridge
between traditional equity holdings (at Alpaca) and on-chain (semi-fungible)
tokenized representations (Rain SFT contracts).

The system uses **Event Sourcing (ES)** and **Command Query Responsibility
Segregation (CQRS)** patterns to maintain a complete audit trail, enable
time-travel debugging, and provide a single source of truth for all operations.

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
- **Fixing "unable to open database file" errors** - If `cargo build` fails with
  sqlx macro errors like
  `error returned from database: (code: 14) unable to
  open database file`, run
  `sqlx db reset -y` to recreate the database

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

The issuance bot uses **Event Sourcing (ES)** and **Command Query Responsibility
Segregation (CQRS)** patterns as its architectural foundation.

**Core Concepts:**

- **Aggregates**: Business entities that encapsulate state and business logic
  (e.g., `Mint`, `Redemption`, `Account`, `TokenizedAsset`)
- **Commands**: Requests to perform actions, representing user or system intent
  (e.g., `InitiateMint`, `ConfirmJournal`)
- **Events**: Immutable facts about what happened, always in past tense (e.g.,
  `MintInitiated`, `JournalConfirmed`)
- **Event Store**: Single source of truth - an append-only log of all domain
  events stored in SQLite
- **Views**: Read-optimized projections built from events for efficient querying
- **Services**: External dependencies that aggregates use (Alpaca API client,
  blockchain client, monitoring service)

**Key Flow:**

```
Command -> Aggregate.handle() -> Validate & Produce Events -> Persist Events
  -> Apply to Aggregate -> Update Views
```

**Critical Methods:**

- `handle(command) -> Result<Vec<Event>, Error>`: Business logic lives here.
  Validates the command against current aggregate state and returns a list of
  events (can be 0+ events). For example, `ConfirmJournal` might produce both
  `JournalConfirmed` and `MintingStarted` events.
- `apply(event)`: Deterministically updates aggregate state from events. This
  method is pure and should never fail - events are historical facts that have
  already occurred.

**Benefits:**

- **Complete Audit Trail**: Every state change is captured as an immutable event
- **Time Travel Debugging**: Replay events to reconstruct system state at any
  point in history
- **Testability**: Business logic tested via Given-When-Then pattern (given
  events, when command, then expect events)
- **Rebuild Views**: If a view becomes corrupted or a new projection is needed,
  simply replay all events
- **Multiple Projections**: Same events can feed different views (operational
  dashboard, analytics, Grafana metrics)
- **Single Source of Truth**: Event store is authoritative; all other data is
  derived

**CRITICAL: Events Are Permanent:**

- **Events can NEVER be removed or changed** once committed to the event store
- Changing event schemas requires complex migration/upcasting strategies
- Think carefully before adding new event types - they are a permanent addition
- **Only add events you actually need NOW** - you can always add more later
- You can change aggregates, views, and commands freely, but events are forever
- Ask yourself: "Do I need this event for a feature I'm implementing right now?"
  If not, don't add it yet
- YAGNI (You Aren't Gonna Need It) applies especially to events

### Services and Trait Design in CQRS/ES

**Services represent business capabilities**, not implementation details. A
well-designed service trait:

- **Models domain capabilities**: The trait methods should describe what the
  system can DO in domain terms, not how it's implemented. What counts as
  "domain" depends on context - in this financial infrastructure, burning tokens
  is a domain concern because 1:1 backing is the value proposition. In a video
  game, token burning might be an implementation detail.
- **Plugs into command handlers**: Aggregates use services to fulfill commands.
  When a command says "do X", the service provides the capability to actually do
  X. Events are then produced based on what the service did.
- **Decouples aggregates from external systems**: Services abstract external
  integrations (blockchain, APIs, other aggregates) behind domain-meaningful
  interfaces. This allows swapping implementations without changing aggregates.
- **Enables clean testing**: Mock the trait to test aggregate logic without real
  external systems. Tests verify business behavior, not integration details.

**Commands vs Events vs Services:**

```
Command: "I want the system to do X"
   v
Command Handler: Uses services to actually do X
   v
Event: "X happened" (captures the fact, including service responses)
```

The command expresses intent. The service executes. The event records what
occurred. This separation keeps aggregates focused on business logic while
services handle execution.

**Anti-patterns to avoid:**

- Single-method traits that just wrap CQRS commands (no domain logic added)
- Trait methods named after implementation ("record", "persist", "save")
- Inconsistent patterns (some aggregates use traits, others use CQRS directly)
- Traits that don't represent a coherent business capability
- External managers orchestrating complex sequences of commands across multiple
  aggregates - commands should invoke business actions directly, using services
  to fulfill them, rather than just recording what external orchestrators did

**This system's domain context:**

This is financial infrastructure where tokens derive value from 1:1 backing.
Partners and integrators care that minting creates backed tokens and burning
redeems them. Internal mechanics (how we track receipts, plan burns, etc.) are
implementation details - the domain concern is that tokens are properly backed
and redeemable.

### Aggregates

**Mint Aggregate**: Manages the complete lifecycle of a mint operation, from
initial request through journal confirmation to on-chain minting and callback.

**Redemption Aggregate**: Manages the redemption lifecycle, from detecting an
on-chain transfer through calling Alpaca to burning tokens.

**Account Aggregate**: Manages the relationship between AP accounts and our
system.

**TokenizedAsset Aggregate**: Manages which assets are supported for
tokenization.

See SPEC.md for detailed command/event mappings and state machines for each
aggregate.

### Aggregates vs Views: Naming and Purpose

Both aggregates and views use enum-based type modeling, but serve fundamentally
different purposes which should be reflected in their naming.

**Aggregates:**

- Represent entity state, reconstructed by replaying events (O(n) cost)
- Enforce business rules, validate commands, maintain domain invariants
- Naming reflects **entity lifecycle**: `NotLinked`/`Linked`,
  `NotAdded`/`Added`, etc.
- Names communicate where the entity is in its domain-specific lifecycle

**Views:**

- Materialized projections for efficient querying
- Optimized for read operations, filtered access, cross-entity queries
- Naming is done from the perspective of a query, e.g. `Unavailable` instead of
  `NotAdded` or `Removed`

**Why the distinction matters:**

- Views can map multiple aggregate states to a single view state
- Query-oriented naming makes it clear views serve a different architectural
  purpose
- Prevents confusion between entity state (aggregate) and data availability
  (view)

**Avoid Option wrappers for views:** The `cqrs-es` framework's
`GenericQuery.load()` already returns `Option<V>`, so
`Option<View(Option<Data>)>` creates confusing nested Options. Use enum variants
instead.

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

**For cross-aggregate queries** (e.g., "find all receipts for underlying X"):

- Create a dedicated read model with proper SQL columns (not JSON)
- Or maintain an in-memory index updated by event handlers
- Or iterate through known aggregate IDs using `GenericQuery::load()`

### HTTP Integration

**Our HTTP Server (Rocket.rs):**

- Implements Alpaca ITN Issuer endpoints that Alpaca calls
- Handles account linking, mint requests, and journal confirmations
- Built with Rust (Rocket.rs web framework)
- SQLite database for event store and views
- Async runtime for coordination

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

**Rain OffchainAssetReceiptVault Contract:**

- ERC-1155 receipts tracking individual deposit IDs
- ERC-20 shares representing vault ownership
- `deposit()` function for minting
- `withdraw()` function for burning
- **Contract Documentation**: The Rain contracts are thoroughly documented with
  extensive inline comments explaining parameters, behavior, and design
  rationale. When working with blockchain integration:
  - Primary contract:
    `lib/ethgild/src/concrete/vault/OffchainAssetReceiptVault.sol`
  - Base implementation: `lib/ethgild/src/abstract/ReceiptVault.sol`
  - Always consult the Solidity source for authoritative documentation on
    contract behavior, parameter meanings, and mathematical formulas
  - The contracts use 18-decimal fixed-point arithmetic for share ratios

**Redemption Wallet:**

- On-chain address where APs send tokens to redeem
- We monitor this address for incoming transfers

**MonitorService:**

- Watches redemption wallet for incoming transfers
- Methods: `watch_transfers()`, `get_transfer_details()`
- Uses a WebSocket subscription to detect redemption events

**Signing Backends (`src/fireblocks/`):**

Two mutually exclusive signing backends, both implementing `VaultService` trait:

- **Local**: `EVM_PRIVATE_KEY` → `RealBlockchainService` (dev/test)
- **Fireblocks**: CONTRACT_CALL → `FireblocksVaultService` (prod, TAP policies)

Key files: `fireblocks/mod.rs` (SignerConfig), `fireblocks/vault_service.rs`
(FireblocksVaultService), `config.rs` (backend selection)

### Core Flows

**Mint Flow:**

1. AP requests mint -> Alpaca calls our `/inkind/issuance` endpoint
2. We validate and respond with `issuer_request_id` (Command: `InitiateMint`,
   Event: `MintInitiated`)
3. Alpaca journals shares from AP to our custodian account
4. Alpaca confirms journal -> we receive `/inkind/issuance/confirm` (Command:
   `ConfirmJournal`, Events: `JournalConfirmed`, `MintingStarted`)
5. We mint tokens on-chain via `vault.multicall()` which atomically executes:
   - `deposit()` - Mints receipts + shares to bot's wallet
   - `transfer()` - Transfers only shares to user's wallet (Command:
     `RecordMintSuccess`, Event: `TokensMinted`)
6. We call Alpaca's callback endpoint (Command: `RecordCallback`, Events:
   `CallbackSent`, `MintCompleted`)

**Redemption Flow:**

1. AP sends tokens to our redemption wallet -> we detect transfer (Command:
   `DetectRedemption`, Event: `RedemptionDetected`)
2. We call Alpaca's redeem endpoint (Command: `RecordAlpacaCall`, Event:
   `AlpacaCalled`)
3. We poll for journal completion (Command: `ConfirmAlpacaComplete`, Event:
   `AlpacaJournalCompleted`)
4. We burn tokens on-chain via `vault.withdraw()` (Command: `RecordBurnSuccess`,
   Event: `TokensBurned` - final success state)

### Configuration

Environment variables are defined in multiple places across the deployment
pipeline:

- **`.env.example`**: Template for local development, lists all available env
  vars
- **`.github/workflows/deploy.yaml`**: GitHub Actions workflow that sets secrets
  and populates the `.env` file during deployment
- **`docker-compose.template.yaml`**: Container configuration template populated
  by the deployment workflow

### Logging Guidelines

- **Log levels**: ERROR = system cannot recover without intervention, WARN =
  degraded but continuing, INFO = significant business events and state
  transitions, DEBUG = diagnostic detail for troubleshooting, TRACE =
  fine-grained internal steps
- **Structured logging**: Always use structured key-value fields
  (`info!(key = %value, "message")`) not string interpolation in messages
- **Loop body logs must be DEBUG or TRACE**: Any log inside a loop or per-item
  iteration must not be INFO or higher - only the summary before/after the loop
- **Actionable context**: Include enough structured fields to investigate
  without needing to reproduce (IDs, counts, addresses)
- **No secrets in logs**: Never log API keys, private keys, or authentication
  credentials. Request/response bodies are generally fine to log for debugging
  (this is infrastructure, not a user-facing app), but exercise judgement -
  avoid logging payloads that contain credentials or tokens

### Code Quality & Best Practices

- **CRITICAL: Package by Feature, Not by Layer**: NEVER organize code by
  language primitives or technical layers. ALWAYS organize by business
  feature/domain.
  - **FORBIDDEN**: `types.rs`, `error.rs`, `models.rs`, `utils.rs`,
    `helpers.rs`, `http.rs`, `dto.rs`, `entities.rs`, `services.rs` (when used
    as catch-all technical layer modules)
  - **CORRECT**: `account.rs`, `mint.rs`, `redemption.rs` (organized by business
    domain), with submodules like `account/cmd.rs`, `account/event.rs` if needed
  - Each feature module should contain ALL related code: types, errors,
    commands, events, aggregates, views, and endpoints
  - This makes it easy to understand and modify a feature without jumping
    between unrelated files
  - Example: `src/account/` contains everything related to account linking -
    newtypes (Email, ClientId), commands (LinkAccount), events (AccountLinked),
    aggregate (Account), view (AccountView), errors (AccountError), and endpoint
    (connect_account)
- **Event-Driven Architecture**: Commands produce events which update views
- **SQLite Persistence**: Event store and view repositories backed by SQLite
- **Comprehensive Error Handling**: Custom error types with proper propagation
- **CRITICAL: Use Typed Values in Errors**: Error types must store typed values,
  not string representations. Never use `format!("{value:?}")` or `.to_string()`
  to convert typed data (addresses, hashes, IDs) into strings for error fields.
  - **FORBIDDEN**: `tx_hash: format!("{hash:?}")` or `address: addr.to_string()`
  - **CORRECT**: `tx_hash: B256` or `address: Address` directly in error struct
  - The `#[error(...)]` attribute in thiserror handles display formatting -
    that's where formatting belongs, not in the error field types
- **Let the Compiler Guide Error Variants**: Don't waste time figuring out which
  error variants you'll need ahead of time. Write functions using the `?`
  operator as if all required variants exist, then let the compiler tell you
  exactly which `From` impls are missing. Add `#[from]` variants only for errors
  the compiler complains about.
  - **FORBIDDEN**: Verbose `.map_err()` calls - use `#[from]` instead
  - **FORBIDDEN**: `.map_err(|e| SomeError::Variant(e.to_string()))` -
    stringly-typed
  - **CORRECT**: Add `#[from]` to error variant, use `?` directly
- **CRITICAL: `#[from]` Variant Naming**: When using thiserror's `#[from]`
  attribute, variant names must be generic (matching the source error type) and
  MUST NOT claim what operation failed. The `?` operator auto-converts any
  matching error type to the variant, so specific claims become false if another
  operation can produce the same error type.
  - **FORBIDDEN**: `ReadSecret(#[from] std::io::Error)` - claims secret reading
    failed, but any `?` on io::Error will use this variant
  - **CORRECT**: `Io(#[from] std::io::Error)` - generic, makes no false claims
  - **FORBIDDEN**: `ParseConfig(#[from] serde_json::Error)` - claims config
    parsing failed
  - **CORRECT**: `Json(#[from] serde_json::Error)` - generic, truthful
  - Rule: If `#[from]` is used, the variant name should mirror the error type,
    not the operation. Use context from where the error is handled, not where
    it's defined.
- **CRITICAL: Make Invalid States Unrepresentable**: This is a fundamental
  principle of type modeling in this codebase. Use algebraic data types (ADTs)
  and enums to encode business rules and state transitions directly in types
  rather than relying on runtime validation.
  - **FORBIDDEN**: Aggregates or domain types with all/most fields as `Option`
    (e.g., `struct Foo { a: Option<A>, b: Option<B>, c: Option<C> }`)
  - **FORBIDDEN**: Multiple nullable fields that can contradict each other
  - **FORBIDDEN**: String-based status fields with Option fields that should be
    present for certain statuses
  - **CORRECT**: Use enum variants to represent mutually exclusive states
  - **CORRECT**: Encode state-specific data within enum variants rather than
    using nullable fields
  - **CORRECT**: Use newtypes for domain concepts to prevent mixing incompatible
    values
  - Example: An aggregate that can be NotLinked or Linked should be
    `enum Account { NotLinked, Linked { client_id, email, ... } }`, NOT
    `struct Account { client_id: Option<ClientId>, email: Option<Email>, ... }`
  - Leverage the type system to enforce invariants at compile time
- **CRITICAL: Parse, Don't Validate**: If a value exists, it must be valid.
  Validation must happen at construction time through smart constructors, not as
  a separate step that callers might forget to call.
  - **FORBIDDEN**: Separate `validate()` methods that must be called after
    construction
  - **FORBIDDEN**: Raw primitive types (String, i64, etc.) for domain values
    that have constraints
  - **FORBIDDEN**: Public struct fields or constructors that bypass validation
  - **CORRECT**: Newtypes with private inner values and fallible smart
    constructors
  - **CORRECT**: The only way to create a value is through a function that
    validates
  - Example of **FORBIDDEN** pattern:
    ```rust
    // WRONG - validate() can be forgotten, value can exist invalid
    pub struct ApiKey(pub String);
    impl ApiKey {
        pub fn validate(&self) -> Result<(), Error> {
            if self.0.len() < 32 { return Err(Error::TooShort); }
            Ok(())
        }
    }
    ```
  - Example of **CORRECT** pattern:
    ```rust
    // CORRECT - if ApiKey exists, it's guaranteed valid
    pub struct ApiKey(String);  // Private inner value
    impl ApiKey {
        pub fn new(value: String) -> Result<Self, ApiKeyError> {
            if value.len() < 32 {
                return Err(ApiKeyError::TooShort { len: value.len() });
            }
            Ok(Self(value))
        }
        pub fn as_str(&self) -> &str { &self.0 }
    }
    ```
  - This principle applies to ALL domain types with constraints: API keys,
    emails, quantities, addresses, IDs, etc.
  - The smart constructor is the ONLY way to create the type - this guarantees
    that if you have a value, it passed validation
- **Schema Design**: Avoid database columns that can contradict each other. Use
  constraints and proper normalization to ensure data consistency at the
  database level. Align database schemas with type modeling principles where
  possible
- **Functional Programming Patterns**: Favor FP and ADT patterns over OOP
  patterns. Avoid unnecessary encapsulation, inheritance hierarchies, or
  getter/setter patterns that don't make sense with Rust's algebraic data types.
  Use pattern matching, combinators, and type-driven design
- **Idiomatic Functional Programming**: Prefer iterator-based functional
  programming patterns over imperative loops unless it increases complexity. Use
  itertools to be able to do more with iterators and functional programming in
  Rust
- **Comments**: Follow comprehensive commenting guidelines (see detailed section
  below)
- **Spacing**: Leave an empty line in between code blocks to allow vim curly
  braces jumping between blocks and for easier reading
- **FORBIDDEN**: Single-letter variable and argument names. All names must be
  descriptive enough to convey meaning without surrounding context
- **CRITICAL: Import Organization**: Follow a consistent two-group import
  pattern throughout the codebase:
  - **Group 1 - External imports**: All imports from external crates including
    `std`, `alloy`, `cqrs_es`, `serde`, `tokio`, etc. No empty lines between
    external imports.
  - **Empty line separating the groups**
  - **Group 2 - Internal imports**: All imports from our codebase using
    `crate::` and `super::`. No empty lines between internal imports.
  - **FORBIDDEN**: Three or more import groups, imports separated by empty lines
    within a group
  - **FORBIDDEN**: Function-level imports. Always use top-of-module imports.
    **Exception**: enum variant imports are allowed inside function bodies when
    it eliminates repetitive qualification (e.g., `use MyEnum::*;` or
    `use MyEnum::{A, B, C};`). This is the only case where function-level
    imports are permitted, and importing from enums is only allowed at function
    level (never at module level).
  - Module declarations (`mod foo;`) can appear between imports if needed
  - This pattern applies to ALL modules including test modules
    (`#[cfg(test)] mod tests`)
  - Example of correct import organization:
    ```rust
    use std::sync::Arc;
    use alloy::primitives::{Address, B256};
    use cqrs_es::{CqrsFramework, EventStore};
    use serde::{Deserialize, Serialize};

    use crate::account::ClientId;
    use crate::mint::TokenizationRequestId;
    use super::{Mint, MintCommand};
    ```
  - Example of **INCORRECT** import organization:
    ```rust
    // WRONG - Three groups, internal imports mixed with external
    use std::sync::Arc;

    use alloy::primitives::{Address, B256};
    use crate::account::ClientId;  // Internal import in wrong place
    use cqrs_es::CqrsFramework;

    use super::Mint;
    ```
- **Import Conventions**: Always import types and use them unqualified unless
  the name is genuinely ambiguous within the crate (e.g.,
  `alloy::rpc::types::Log` vs `alloy::primitives::Log`). When ambiguity exists,
  use qualified imports like `rpc_types::Log`. Never use fully qualified paths
  for non-ambiguous types - import them at the top of the module.
  - **FORBIDDEN**: `alloy::primitives::B256` inline - import `B256` at module
    top
  - **CORRECT**: `rpc_types::Log` when `Log` is ambiguous (multiple `Log` types)
  - **CORRECT**: `contract::Error` to disambiguate from other `Error` types
- **CRITICAL: Zero Tolerance for Panics in Non-Test Code**: This is a
  mission-critical financial application. ANY panic in production code is
  completely unacceptable and can lead to catastrophic failures.
  - **FORBIDDEN**: `unwrap()`, `expect()`, `panic!()`, `unreachable!()`,
    `unimplemented!()` in any non-test code
  - **FORBIDDEN**: Index operations that can panic (e.g., `vec[i]`), use
    `.get(i)` instead
  - **FORBIDDEN**: Division operations without checking for zero
  - **FORBIDDEN**: Any operation that can panic at runtime
  - **REQUIRED**: Use `?` operator for proper error propagation
  - **REQUIRED**: Use `Result` and `Option` with explicit error handling
  - **REQUIRED**: All fallible operations must return `Result` with descriptive
    errors
  - **Exception**: `unwrap()` and `expect()` are ONLY allowed in test code
    (`#[cfg(test)]` modules and `#[test]` functions)
  - Panics in production code are deployment-blocking bugs that must be fixed
    immediately
- **Visibility Levels**: Always keep visibility levels as restrictive as
  possible (prefer `pub(crate)` over `pub`, private over `pub(crate)`) to enable
  better dead code detection by the compiler and tooling. This makes the
  codebase easier to navigate and understand by making the relevance scope
  explicit

### CRITICAL: Financial Data Integrity

**NEVER** silently provide wrong values, hide conversion errors, or mask
failures. FORBIDDEN patterns:

- Defensive value capping hiding overflow/underflow
- Fallback to defaults on conversion failure (`unwrap_or`, `unwrap_or_default`)
- Silent precision truncation
- "Graceful degradation" in conversion functions

**All financial operations must use explicit error handling:**

```rust
// WRONG - Silent cap             | CORRECT - Explicit error
fn to_i64(v: u64) -> i64 {        | fn to_i64(v: u64) -> Result<i64, Error> {
    if v > i64::MAX as u64 {      |     v.try_into().map_err(|_| Error::TooLarge { v })
        i64::MAX // silent cap    | }
    } else { v as i64 }           |
}                                 |

// WRONG - Hides parse error      | CORRECT - Propagates error
fn parse(s: &str) -> f64 {        | fn parse(s: &str) -> Result<Decimal, ParseError> {
    s.parse().unwrap_or(0.0)      |     Decimal::from_str(s).map_err(Into::into)
}                                 | }

// WRONG - Masks DB violation     | CORRECT - Let constraint fail
let safe = amt.clamp(0, MAX);     | sqlx::query!("INSERT ...", amt).execute(pool)?;
sqlx::query!("...", safe)...      |
```

**Must fail fast:** numeric conversions (`try_into`), precision loss, range
violations, parse failures, arithmetic (use checked ops), DB constraints.

**Fail fast > corrupted data.** Silent corruption leads to massive losses.

### CRITICAL: Security and Secrets Management

**NEVER read credential files without explicit user permission.**

**Common credential files to avoid:**

- `.env`, `.env.*`, `.env.local`, `.env.production` - API keys, secrets
- `credentials.json`, `secrets.json` - Credential storage
- `*.key`, `*.pem`, `*.p12`, `*.pfx` - Private keys and certificates

**When debugging config issues:** Ask the user to verify env vars are set,
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

-- Snapshots: performance optimization caching aggregate state
CREATE TABLE snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL,
    payload JSON NOT NULL,
    timestamp TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

Snapshots can be deleted anytime - aggregates rebuild from events alone.

### Views

All views follow the same pattern (`view_id`, `version`, `payload` as JSON).
Views implement `View` trait and are updated by `GenericQuery` on event commit.

```sql
CREATE TABLE mint_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
```

See `migrations/` for all view table definitions and indexes.

## Testing Strategy

### Testing Pyramid

Follow the testing pyramid - more tests at lower levels, fewer at higher:

1. **Property tests** - Most numerous. Use proptest for invariant testing
2. **Unit tests** - Aggregate logic with MemStore, exhaustive edge cases
3. **Integration tests** - HTTP endpoints with mocked dependencies
4. **E2E tests** - Fewest, but essential for system orchestration

**The pyramid is about quantity, not avoidance.** You should have MANY
property/unit tests, SOME integration tests, and a FEW e2e tests. But e2e tests
are still required when testing full system orchestration.

**When e2e tests ARE required:**

- Testing that multiple async processes coordinate correctly (backfilling,
  monitoring, processing)
- Testing startup/shutdown behavior and recovery
- Testing flows that span multiple aggregates AND external systems
- Testing that the service handles events that occurred before it started

**Example:** Receipt backfilling and live monitoring REQUIRE e2e testing
because:

- Unit tests can verify backfiller logic in isolation
- Unit tests can verify monitor logic in isolation
- But only e2e can verify: (1) service starts, (2) backfills historic receipts,
  (3) monitors new receipts, (4) redemption uses receipts from both sources

A single well-designed e2e test can cover the orchestration, while dozens of
unit tests cover the edge cases in each component.

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

- **In-memory event store**: `cqrs-es` provides MemStore for fast aggregate
  tests
- **Mock external systems**: `httpmock` for Alpaca API, mock blockchain for
  determinism
- **Database isolation**: In-memory SQLite per test

### End-to-End Tests: Strict Definition

**A test is ONLY considered e2e if it:**

1. Spins up the full HTTP service
2. Uses ONLY the public API as an external consumer would
3. Uses Anvil for local blockchain
4. Mocks only truly external systems (Alpaca API)
5. Asserts correctness via API responses, Anvil state, and mock interactions

**Setup phase exception:** Some edge-case scenarios (e.g., recovering from a
`MintingFailed` state) cannot be induced through the public API alone. In these
cases, the **setup phase** may use direct SQL (e.g., inserting events into the
event store) to establish the initial state. Once setup is complete, the
**scenario execution and verification** must follow all e2e rules above — the
service runs, reacts to real events (blockchain, monitors), and correctness is
asserted via API responses, Anvil state, and mock interactions.

**A test is NOT e2e if it:**

- Touches implementation details for **verification** (e.g., querying aggregates
  directly, inspecting internal state after the scenario runs)
- Touches implementation details during the **scenario execution** (e.g.,
  calling CQRS commands to drive the flow instead of letting the service react)
- Requires access to internal types or functions outside the setup phase

**If a test requires touching implementation details beyond setup**, it belongs
in `src/` as a unit or integration test, NOT in `tests/`.

E2E tests live in `./tests/`. They test complete production flows, happy paths
only, with real blockchain (Anvil).

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
  line at the given level contains all specified snippets. This ensures you're
  testing that the right information appears together in one log entry:

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

**Workflow (TTDD - Type-driven TDD)**:

TTDD sequence:

1. **Types first**: Define types, traits, and method signatures that model the
   domain
2. **Failing tests**: Write tests that compile but fail (build errors don't
   count as failing tests)
3. **Implementation**: Write the logic to make tests pass

**`todo!()` macro:** Encouraged during TTDD types-first stage to stub
signatures. Must be removed before completion - any `todo!()` in final code is
unacceptable.

While developing, continuously run `cargo check` and `cargo test` to verify
types and behavior. Only after implementation is complete, run `cargo clippy`
and fix all warnings. Finally, `cargo fmt` before committing.

**CRITICAL: Never use `cargo build` for verification.** Use `cargo check`
(faster) or `cargo test` (more useful). Only use `cargo build` when you need the
binary.

- **Before handing over a piece of work**, run checks in this order:
  1. `cargo test --workspace` - All tests must pass
  2. `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` -
     Fix any linting issues (these should be minimal if code is well-structured)
  3. `cargo fmt --all` - Format the code

### CRITICAL: Lint Policy

**NEVER add `#[allow(...)]` attributes or disable any lints without explicit
user permission.** This applies to ALL lint attributes:

**Required approach for lint/warning issues:**

1. **Refactor the code** to address the root cause of the lint violation
2. **Break down large functions** into smaller, more focused functions
3. **Improve code structure** to meet clippy's standards
4. **Use proper error handling** instead of suppressing warnings
5. **Remove unused code** instead of allowing dead_code warnings

**Examples of FORBIDDEN practices:**

```rust
// NEVER DO THIS - Suppressing lints is forbidden
#[allow(clippy::too_many_lines)]
fn large_function() { /* ... */ }

#[allow(clippy::needless_continue)]
// NEVER DO THIS - Fix the code structure instead

#[allow(dead_code)]
struct Unused { /* ... */ }  // Remove the unused code instead

#[allow(unused_imports)]
use some_module::Thing;  // Remove the unused import instead
```

**Required approach:**

```rust
// CORRECT - Refactor to address the issue
fn process_data() -> Result<(), Error> {
    let data = get_data()?;
    validate_data(&data)?;
    save_data(&data)?;
    Ok(())
}

fn validate_data(data: &Data) -> Result<(), Error> {
    // Extracted validation logic
}

fn save_data(data: &Data) -> Result<(), Error> {
    // Extracted saving logic
}
```

**If you encounter a clippy issue:**

1. Understand WHY clippy is flagging the code
2. Refactor the code to address the underlying problem
3. If you believe a lint is incorrect, ask for permission before suppressing it
4. Document your reasoning if given permission to suppress a specific lint

**Exception for third-party macro-generated code:**

When using third-party macros, such as `sol!` to generate Rust code, lint
suppression is acceptable for issues that originate from the contract's function
signatures, which we cannot control.

For example, to deal with a function generated from a smart contract's ABI, we
can add `allow` inside the `sol!` macro invocation.

```rust
// CORRECT - Suppressing lint for third-party ABI generated code
sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, "node_modules/@pythnetwork/pyth-sdk-solidity/abis/IPyth.json"
);
```

This policy ensures code quality remains high and prevents technical debt
accumulation through lint suppression.

## CRITICAL: Evidence-Based Claims

**NEVER make claims about code behavior, external systems, or technical facts
without providing evidence from the actual source code or documentation.**

This is a zero-tolerance policy. Speculation presented as fact is unacceptable.

**FORBIDDEN:**

- "This is how ERC-20 works" without citing the actual contract code
- "The API expects X" without showing the documentation or code that proves it
- "This function does Y" without reading the function first
- Any claim about inheritance, call stacks, or behavior without tracing through
  the actual source files

**REQUIRED:**

- Before making any claim about behavior, READ the relevant source code first
- Cite exact file paths and line numbers that prove the claim
- For inheritance chains or call stacks, trace through each step with file
  references
- If you haven't read the code, say "I don't know" or "let me check"

**Example:**

```text
// FORBIDDEN: "ERC-20 minting emits Transfer from zero address"

// REQUIRED: "Let me check..." [reads files] "Found it:
// 1. deposit() -> ReceiptVault._deposit() (lib/.../ReceiptVault.sol:559)
// 2. _deposit() -> _mint() (lib/.../ReceiptVault.sol:587)
// 3. ReceiptVault inherits ERC20Upgradeable (lib/.../ReceiptVault.sol:5)
// 4. _mint() emits Transfer(0, to, amt) (lib/.../ERC20Upgradeable.sol:266)"
```

**When documenting non-obvious behavior in code comments**, include the full
reference chain. See `src/redemption/detector.rs` for an example.

## Commenting Guidelines

Code should be self-documenting. Comment only when adding context that code
structure cannot express.

**DO comment:** Complex business logic, algorithm rationale, external system
behavior, non-obvious constraints, test data context, workarounds.

**DON'T comment:** Self-explanatory code, restating what code does, obvious
assignments, test section markers, section dividers (e.g., `// ========`),
references to tasks/issues (reviewers and future maintainers won't have access
to transient planning artifacts).

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
**private helpers last**. This makes public interface immediately visible.

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

Use `.unwrap()` directly - if unexpected, you see the value immediately:

```rust
// Verbose                           | Concise
assert!(result.is_err());               | assert!(matches!(result.unwrap_err(),
assert!(matches!(result.unwrap_err(),   |     MintError::InvalidQuantity));
    MintError::InvalidQuantity));       |
```

### Type modeling

**Make invalid states unrepresentable** - use enums, not nullable fields:

```rust
// Bad: Options can contradict         | Good: Each state has its data
pub struct Mint {                         | pub enum MintStatus {
    pub status: String,                   |     Pending,
    pub tx_hash: Option<String>,          |     Completed { tx_hash: String, shares: u64 },
    pub error: Option<String>,            |     Failed { reason: String },
}                                         | }
```

**Use newtypes** to prevent mixing incompatible values:

```rust
// fn mint(tok_id: String, iss_id: String, sym: String) - easy to mix up
// fn mint(tok_id: TokenizationId, iss_id: IssuerId, sym: Symbol) - type-safe
```

### Avoid deep nesting

Use early returns and `let-else` for flat code:

```rust
// Nested                              | Flat with early returns
fn validate(d: Option<&Data>) -> Res<()> | fn validate(d: Option<&Data>) -> Res<()> {
{                                         |     let d = d.ok_or(Error::NoData)?;
    if let Some(d) = d {                  |     if d.qty <= 0 { return Err(Error::Qty); }
        if d.qty > 0 {                    |     if !d.sym.ok() { return Err(Error::Sym); }
            if d.sym.ok() { Ok(()) }      |     Ok(())
            else { Err(Error::Sym) }      | }
        } else { Err(Error::Qty) }        |
    } else { Err(Error::NoData) }         |
}                                         |
```

### Struct field access

Prefer direct field access over unnecessary constructors/getters:

```rust
// Use struct literals directly
let req = MintRequest { qty: 100.into(), underlying: "AAPL".into(), .. };
println!("{}", req.qty);  // Direct access

// Don't add getters/constructors that just forward to fields
impl MintRequest {
    pub fn qty(&self) -> Decimal { self.qty }  // Unnecessary
}
```
