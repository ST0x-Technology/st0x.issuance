# AGENTS.md

This file provides guidance to AI agents working with code in this repository.

Relevant docs:

- README.md
- ROADMAP.md
- SPEC.md

## Plan & Review

### Before starting work

- Write a comprehensive step-by-step plan to PLAN.md with each task having a
  corresponding section and a list of subtasks as checkboxes inside of it
- The task sections should follow the format `## Task N. <TASK NAME>`
- The plan should be a detailed implementation plan and the reasoning behind the
  design decisions
- Do not include timelines in the plan as they tend to be inaccurate
- Remain focused on the task at hand, do not include unrelated improvements or
  premature optimizations
- Once you write the plan, ask me to review it. Do not continue until I approve
  the plan.

### While implementing

- **CRITICAL: Complete tasks one at a time and wait for review**
  - When asked to complete a task from a plan, complete ONLY that task
  - Do NOT proceed to the next task until the user reviews and approves your
    changes
  - The user manually reviews all git diffs, so changes must be minimal and
    focused
  - **Any diff not required to complete the task is a guideline violation** - no
    drive-by improvements, refactorings, or style changes unless explicitly
    included in the scope of the task or requested by the user
  - Exception: If the user explicitly asks you to "complete the whole plan" or
    "complete the GitHub issue", you may work through multiple tasks
  - By default, always work one task at a time
- **CRITICAL: Tasks must be ordered correctly in plans**
  - When creating implementation plans, ensure tasks are in the correct order
  - Earlier tasks MUST NOT depend on code from later tasks
  - All checks (tests, clippy, fmt) SHOULD pass at the end of each task whenever
    possible
  - Focused git diffs and passing checks make reviewing much easier
- **CRITICAL: Keep PLAN.md in sync with implementation decisions**
  - If you change approach during implementation, immediately update PLAN.md to
    reflect the new approach
  - Plans are living documents during development - update them when you
    discover better solutions
  - Implementation and plan must always match - out-of-sync plans are worse than
    no plan
- Update PLAN.md every time you complete a task by marking checkboxes as `[x]`
- Keep PLAN.md concise - just tick off checkboxes, do not add "Changes Made"
  sections or verbose changelogs
- The code diffs themselves should be self-explanatory and easy to review

### Before creating a PR

- **CRITICAL**: Delete PLAN.md before submitting changes for review
- PLAN.md is a transient development file that should ONLY exist on development
  branches
- PLAN.md should NEVER appear in pull requests or be merged to main/master
- The plan is for development tracking only - final documentation goes in commit
  messages, docstrings, and permanent markdown documents
- **CRITICAL**: Update all documentation to reflect your changes
  - **ROADMAP.md**: Mark completed tasks as done with the PR link
    - When you complete a task that corresponds to an issue in ROADMAP.md,
      update the roadmap to mark it as complete `[x]` and add the PR link
    - Format: `- [x] [#N](issue-url) - Task description`
    - Add PR reference: `- **PR:** [#N](pr-url)`
    - This ensures the roadmap accurately reflects progress when the PR is
      merged
  - **README.md**: Review and update if your changes affect:
    - Project structure (new directories, modules)
    - Key features or capabilities
    - Development commands or workflows
    - API endpoints
    - Architecture overview
  - **SPEC.md**: Review and update if your changes affect:
    - Aggregates, commands, or events
    - State machines or flows
    - Data structures or APIs
    - Integration points with external systems
  - **AGENTS.md**: Update if you introduce new patterns, practices, or
    conventions that other developers should follow
  - Out-of-date documentation has negative value - it confuses more than it
    clarifies

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
  feature tracked in PLAN.md, if you discover a migration you added is
  incorrect, fix the original migration file directly. NEVER add a new migration
  to fix another migration added as part of the same task/feature. Only add fix
  migrations for issues in migrations that have already been merged to main.
- `sqlx db create` - Create the database
- `sqlx migrate run` - Apply database migrations
- `sqlx migrate revert` - Revert last migration
- `sqlx db reset -y` - Drop the database and re-run all migrations
- Database URL configured via `DATABASE_URL` environment variable

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
Command → Aggregate.handle() → Validate & Produce Events → Persist Events
  → Apply to Aggregate → Update Views
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

### Core Flows

**Mint Flow:**

1. AP requests mint → Alpaca calls our `/inkind/issuance` endpoint
2. We validate and respond with `issuer_request_id` (Command: `InitiateMint`,
   Event: `MintInitiated`)
3. Alpaca journals shares from AP to our custodian account
4. Alpaca confirms journal → we receive `/inkind/issuance/confirm` (Command:
   `ConfirmJournal`, Events: `JournalConfirmed`, `MintingStarted`)
5. We mint tokens on-chain via `vault.multicall()` which atomically executes:
   - `deposit()` - Mints receipts + shares to bot's wallet
   - `transfer()` - Transfers only shares to user's wallet (Command:
     `RecordMintSuccess`, Event: `TokensMinted`)
6. We call Alpaca's callback endpoint (Command: `RecordCallback`, Events:
   `CallbackSent`, `MintCompleted`)

**Redemption Flow:**

1. AP sends tokens to our redemption wallet → we detect transfer (Command:
   `DetectRedemption`, Event: `RedemptionDetected`)
2. We call Alpaca's redeem endpoint (Command: `RecordAlpacaCall`, Event:
   `AlpacaCalled`)
3. We poll for journal completion (Command: `ConfirmAlpacaComplete`, Event:
   `AlpacaJournalCompleted`)
4. We burn tokens on-chain via `vault.withdraw()` (Command: `RecordBurnSuccess`,
   Event: `TokensBurned` - final success state)

### Configuration

Environment variables (can be set via `.env` file):

- `DATABASE_URL`: SQLite database path
- `WS_RPC_URL`: WebSocket RPC endpoint for blockchain monitoring
- `CHAIN_ID`: Chain ID (e.g., 8453 for Base)
- `VAULT_ADDRESS`: OffchainAssetReceiptVault contract address
- `PRIVATE_KEY`: Bot's private key for signing blockchain transactions
- `BOT_WALLET_ADDRESS`: Bot's wallet address (derived from private key)
- `REDEMPTION_WALLET_ADDRESS`: Address where APs send tokens to redeem
- Alpaca API credentials and endpoints
- Server configuration (host, port, API key)

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
    // ❌ WRONG - Three groups, internal imports mixed with external
    use std::sync::Arc;

    use alloy::primitives::{Address, B256};
    use crate::account::ClientId;  // Internal import in wrong place
    use cqrs_es::CqrsFramework;

    use super::Mint;
    ```
- **Import Conventions**: Use qualified imports when they prevent ambiguity
  (e.g. `contract::Error` for `alloy::contract::Error`), but avoid them when the
  module is clear (e.g. use `info!` instead of `tracing::info!`)
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

**This is a mission-critical financial application. The following patterns are
STRICTLY FORBIDDEN and can result in catastrophic financial losses:**

**NEVER** write code that silently provides wrong values, hides conversion
errors, or masks failures in any way. This includes but is not limited to:

- Defensive value capping that hides overflow/underflow
- Fallback to default values on conversion failure
- Silent truncation of precision
- Using `unwrap_or(default_value)` on financial calculations
- Using `unwrap_or_default()` on monetary values
- Conversion functions that "gracefully degrade" instead of failing

**ALL financial operations must use explicit error handling with proper error
propagation. Here are examples of forbidden patterns and their correct
alternatives:**

#### Numeric Conversions

```rust
// ❌ CATASTROPHICALLY DANGEROUS - Silent data corruption
const fn shares_to_db_i64(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX  // WRONG: Silently caps at wrong value
    } else {
        value as i64
    }
}

// ✅ CORRECT - Explicit conversion with proper error handling
fn shares_to_db_i64(value: u64) -> Result<i64, ConversionError> {
    value.try_into()
        .map_err(|_| ConversionError::ValueTooLarge {
            value,
            max_allowed: i64::MAX as u64
        })
}
```

#### String Parsing

```rust
// ❌ DANGEROUS - Hides conversion errors
fn parse_price(input: &str) -> f64 {
    input.parse().unwrap_or(0.0)  // WRONG
}

// ✅ CORRECT - Parse with explicit error
fn parse_price(input: &str) -> Result<Decimal, ParseError> {
    Decimal::from_str(input).map_err(|e| ParseError::InvalidPrice { input: input.to_string(), source: e })
}
```

#### Precision-Critical Arithmetic

```rust
// ❌ DANGEROUS - Silent precision loss
fn convert_to_cents(dollars: f64) -> i64 {
    (dollars * 100.0) as i64  // WRONG: Truncates
}

// ✅ CORRECT - Checked arithmetic
fn convert_to_cents(dollars: Decimal) -> Result<i64, ArithmeticError> {
    let cents = dollars.checked_mul(Decimal::from(100)).ok_or(ArithmeticError::Overflow)?;
    if cents.fract() != Decimal::ZERO {
        return Err(ArithmeticError::FractionalCents { value: cents });
    }
    cents.to_i64().ok_or(ArithmeticError::ConversionFailed { value: cents })
}
```

#### Database Constraints

```rust
// ❌ DANGEROUS - Masks constraint violations
async fn save_amount(amount: Decimal, pool: &Pool) -> Result<(), Error> {
    let safe = amount.min(Decimal::MAX).max(Decimal::ZERO);  // WRONG
    sqlx::query!("INSERT INTO trades (amount) VALUES (?)", safe).execute(pool).await?;
    Ok(())
}

// ✅ CORRECT - Let constraints fail naturally
async fn save_amount(amount: Decimal, pool: &Pool) -> Result<(), Error> {
    sqlx::query!("INSERT INTO trades (amount) VALUES (?)", amount).execute(pool).await?;
    Ok(())
}
```

#### Error Categories That Must Fail Fast

1. **Numeric Conversions**: Any conversion between numeric types must use
   `try_into()` or equivalent
2. **Precision Loss**: Operations that could lose precision must be explicit
   about it
3. **Range Violations**: Values outside expected ranges must error, not clamp
4. **Parse Failures**: String-to-number parsing must propagate parse errors
5. **Arithmetic Operations**: Use checked arithmetic for all financial
   calculations
6. **Database Constraints**: Let database constraints fail rather than masking
   violations

#### Required Error Types

Every financial operation must have proper error types that preserve context:

```rust
#[derive(Debug, thiserror::Error)]
pub enum FinancialError {
    #[error("Value {value} exceeds maximum allowed {max_allowed}")]
    ValueTooLarge { value: u64, max_allowed: u64 },

    #[error("Arithmetic overflow in operation: {operation}")]
    ArithmeticOverflow { operation: String },

    #[error("Precision loss detected: {original} -> {converted}")]
    PrecisionLoss { original: String, converted: String },

    #[error("Invalid price format: '{input}'")]
    InvalidPrice { input: String, #[source] source: DecimalError },
}
```

**Remember: In financial applications, it is ALWAYS better for the system to
fail fast with a clear error than to continue with potentially corrupted data.
Silent data corruption in financial systems can lead to massive losses,
regulatory violations, and complete system failure.**

### CRITICAL: Security and Secrets Management

**⚠️ ABSOLUTE PROHIBITION: NEVER, UNDER ANY CIRCUMSTANCES, READ FILES CONTAINING
CREDENTIALS OR SECRETS WITHOUT EXPLICIT USER PERMISSION ⚠️**

**This is a ZERO-TOLERANCE policy. Violations are deployment-blocking.**

This project handles financial transactions and sensitive API credentials.
Unauthorized access to secrets can lead to:

- Account compromise and theft
- Financial losses (potentially millions of dollars)
- Security breaches
- Regulatory violations
- Complete system compromise

**DO NOT attempt to "help" by reading credential files. DO NOT make exceptions.
DO NOT read these files even if you think it would solve the user's problem.**

#### Files That Require Explicit Permission

The following files MUST NOT be read without explicit user permission:

- `.env` - Environment variables containing API keys, secrets, and credentials
- `.env.*` - Environment-specific configuration files (`.env.local`,
  `.env.production`, etc.)
- `credentials.json` - Credential storage files
- `*.key`, `*.pem` - Private keys and certificates
- `*.p12`, `*.pfx` - Certificate bundles
- Database files containing sensitive data (unless necessary for debugging with
  permission)
- Any file that may contain API keys, tokens, passwords, or other secrets

#### Required Practice

**Before reading any file that may contain secrets:**

1. **Ask the user explicitly** for permission to read the file
2. **Explain why** you need to read it
3. **Wait for confirmation** before proceeding

**Example of correct behavior:**

```
User: "Why isn't the bot connecting to Alpaca?"
Assistant: "I can help debug this. To check the configuration, I would need to
read your .env file which contains sensitive credentials. May I have permission
to read it?"
```

#### Alternative Approaches

When debugging configuration issues, prefer these approaches:

1. **Ask the user** to verify specific environment variables are set
2. **Request sanitized output** where sensitive values are redacted
3. **Check example files** like `.env.example` instead of the actual `.env`
4. **Review code** that uses the configuration rather than the configuration
   itself

**Remember: Protecting secrets is critical for application security. Always
respect the sensitivity of credential files and never access them without
explicit permission.**

## Database Schema

The database uses **SQLite** with an event sourcing architecture. The event
store is the single source of truth, and all other tables are read-optimized
views derived from events.

### Event Store Tables

These tables store the immutable event log that serves as the authoritative
source of truth.

```sql
-- Events table: stores all domain events
CREATE TABLE events (
    aggregate_type TEXT NOT NULL,      -- 'Mint', 'Redemption', 'Account', 'TokenizedAsset'
    aggregate_id TEXT NOT NULL,        -- Unique identifier for the aggregate instance
    sequence BIGINT NOT NULL,          -- Sequence number for this aggregate (starts at 1)
    event_type TEXT NOT NULL,          -- Event name (e.g., 'MintInitiated', 'TokensMinted')
    event_version TEXT NOT NULL,       -- Event schema version (e.g., '1.0')
    payload JSON NOT NULL,             -- Event data as JSON
    metadata JSON NOT NULL,            -- Correlation IDs, timestamps, user context, etc.
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

CREATE INDEX idx_events_type ON events(aggregate_type);
CREATE INDEX idx_events_aggregate ON events(aggregate_id);

-- Snapshots table: aggregate state cache for performance
CREATE TABLE snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL,    -- Last event sequence included in this snapshot
    payload JSON NOT NULL,             -- Serialized aggregate state
    timestamp TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

**Note on Snapshots**: The snapshots table is a performance optimization that
caches aggregate state at specific sequence numbers. When loading an aggregate,
the framework loads the latest snapshot (if any) and replays only events since
that snapshot, rather than replaying all events from the beginning. Snapshots
can be deleted at any time - aggregates can always be rebuilt from the event
store alone.

### View Tables

These tables are read-optimized projections built from events. They can be
rebuilt at any time by replaying events.

```sql
-- Mint view: current state of mint operations
CREATE TABLE mint_view (
    view_id TEXT PRIMARY KEY,         -- issuer_request_id
    version BIGINT NOT NULL,          -- Last event sequence applied to this view
    payload JSON NOT NULL             -- Current mint state as JSON
);

CREATE INDEX idx_mint_view_payload ON mint_view(json_extract(payload, '$.status'));
CREATE INDEX idx_mint_view_client ON mint_view(json_extract(payload, '$.client_id'));
CREATE INDEX idx_mint_view_symbol ON mint_view(json_extract(payload, '$.underlying'));

-- Redemption view: current state of redemption operations
CREATE TABLE redemption_view (
    view_id TEXT PRIMARY KEY,         -- issuer_request_id
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX idx_redemption_view_payload ON redemption_view(json_extract(payload, '$.status'));
CREATE INDEX idx_redemption_view_symbol ON redemption_view(json_extract(payload, '$.underlying'));

-- Account view: current account state
CREATE TABLE account_view (
    view_id TEXT PRIMARY KEY,         -- client_id
    version BIGINT NOT NULL,
    payload JSON NOT NULL             -- {email, alpaca_account, status, timestamps}
);

CREATE INDEX idx_account_view_email ON account_view(json_extract(payload, '$.email'));
CREATE INDEX idx_account_view_alpaca ON account_view(json_extract(payload, '$.alpaca_account'));
CREATE INDEX idx_account_view_status ON account_view(json_extract(payload, '$.status'));

-- Tokenized asset view: current supported assets
CREATE TABLE tokenized_asset_view (
    view_id TEXT PRIMARY KEY,         -- underlying symbol
    version BIGINT NOT NULL,
    payload JSON NOT NULL             -- {token, network, vault_address, enabled, timestamps}
);

CREATE INDEX idx_asset_view_enabled ON tokenized_asset_view(json_extract(payload, '$.enabled'));

-- Receipt inventory view: built from TokensMinted and TokensBurned events
CREATE TABLE receipt_inventory_view (
    view_id TEXT PRIMARY KEY,         -- receipt_id:vault_address
    version BIGINT NOT NULL,
    payload JSON NOT NULL             -- {receipt_id, vault_address, symbol, initial_amount, current_balance, timestamps}
);

CREATE INDEX idx_receipt_vault ON receipt_inventory_view(json_extract(payload, '$.vault_address'));
CREATE INDEX idx_receipt_symbol ON receipt_inventory_view(json_extract(payload, '$.symbol'));
```

**Note on Views**: All view tables follow the same pattern - `view_id` (primary
key), `version` (last event sequence applied), and `payload` (JSON containing
the view state). Views implement the `View` trait and are automatically updated
by `GenericQuery` processors when events are committed. If a view becomes
corrupted or a new projection is needed, simply drop the table and replay all
events to rebuild it.

## Testing Strategy

### Given-When-Then Aggregate Testing

ES/CQRS enables highly testable business logic through the Given-When-Then
pattern.

**Testing Approach:**

- **Given**: Set up initial aggregate state by providing previous events
- **When**: Execute a command
- **Then**: Assert expected events are produced (or expected error)

**Example Tests:**

```rust
// Happy path: mint initiated successfully
#[test]
fn test_initiate_mint() {
    MintTestFramework::with(mock_services)
        .given_no_previous_events()
        .when(InitiateMint {
            tokenization_request_id: "alp-123",
            qty: Decimal::from(100),
            // ...
        })
        .then_expect_events(vec![
            MintInitiated { /* ... */ }
        ]);
}

// Journal confirmed triggers minting
#[test]
fn test_journal_confirmed() {
    MintTestFramework::with(mock_services)
        .given(vec![
            MintInitiated { issuer_request_id: "iss-456", /* ... */ }
        ])
        .when(ConfirmJournal { issuer_request_id: "iss-456" })
        .then_expect_events(vec![
            JournalConfirmed { /* ... */ },
            MintingStarted { /* ... */ }
        ]);
}

// Error case: can't confirm journal for non-existent mint
#[test]
fn test_journal_confirmed_for_missing_mint() {
    MintTestFramework::with(mock_services)
        .given_no_previous_events()
        .when(ConfirmJournal { issuer_request_id: "unknown" })
        .then_expect_error("Mint not found or already completed");
}
```

### Testing Infrastructure

**In-Memory Event Store:**

- The `cqrs-es` crate provides an in-memory event store for testing aggregates
- Use this for testing aggregate logic when not testing database-specific
  behavior
- Fast and isolated tests without database overhead

**Mock External Systems:**

- `httpmock` crate for Alpaca API testing
- Mock blockchain interactions for deterministic testing

**Database Isolation:**

- In-memory SQLite databases for testing database-specific logic
- Each test gets its own isolated database

### End-to-End Tests

**End-to-end tests** reproduce the complete production flow in a controlled
environment. These tests simulate exactly what would happen in reality while
mocking third-party external APIs.

**Critical Requirements:**

- E2E tests in `./tests/` directory (not `src/`)
- Use **Anvil** (Foundry's local blockchain) for real on-chain transactions
- Deploy actual smart contracts (OffchainAssetReceiptVault)
- Send real blockchain transactions that trigger the system
- Use **httpmock** to mock third-party HTTP APIs (Alpaca API)
- Start the full HTTP server (Rocket) with real wiring - real CQRS framework,
  real managers (MintManager, CallbackManager), real service implementations
  (RealBlockchainService, RealAlpacaService)
- Use in-memory SQLite database (real database operations, not MemStore)
- Test the **complete happy path flow** from start to finish, not individual
  steps

**What to Mock (external systems only):**

- **Third-party APIs**: Alpaca HTTP endpoints via httpmock
- **Blockchain RPC**: Use Anvil (local blockchain) instead of real RPC providers

**What NOT to Mock (all internal code):**

- Internal service traits (VaultService, AlpacaService) - use real
  implementations
- Managers (MintManager, CallbackManager, etc.) - use real implementations
- CQRS framework - use real implementation with real event store

**Example E2E Test Flows:**

- **Mint**: Alpaca HTTP request → CQRS → Mint on-chain via `vault.deposit()` to
  Anvil → Callback to httpmock Alpaca server
- **Redemption**: Send tokens on-chain to redemption wallet on Anvil → Detector
  triggers → Call httpmock Alpaca redeem endpoint → Poll httpmock for completion
  → Burn tokens via `vault.withdraw()` on Anvil

**Test Coverage Strategy:**

E2E tests are slower than unit/integration tests, so focus them exclusively on:

- **Happy path flows only** - Verify primary use cases work correctly end-to-end
- **Complete flows** - Test the entire path from trigger to completion, not
  individual steps

Leave exhaustive edge case testing to faster, more focused tests:

- **Unit tests** (aggregate tests in `src/*/mod.rs`) - Exhaustive edge cases,
  validation rules, business logic in isolation with MemStore
- **Integration tests** (component tests in `src/*/api/*.rs`) - Individual HTTP
  endpoints, database operations, error handling with mocked dependencies

**Test Type Distinctions:**

- **Unit tests** (`src/mint/mod.rs`): Test CQRS aggregate logic in isolation
  with MemStore, no external dependencies (fast, exhaustive edge cases)
- **Integration tests** (`src/mint/api/*.rs`): Test individual HTTP endpoints or
  components with mocked service dependencies (thorough error scenarios)
- **End-to-end tests** (`tests/*.rs`): Test complete production flows from
  external trigger to completion with real blockchain + mock external APIs only
  (happy paths only)

**Public API Surface:**

End-to-end tests help shape what will become the public API for the Rust client
library. When exposing types for end-to-end tests, be intentional about what
will eventually be part of the client library interface. The progression is:

1. Expose minimal public API for end-to-end tests (#20)
2. Package exposed types into proper client library (#52)
3. Refactor end-to-end tests to use the client library

### General Testing Guidelines

- **Edge Case Coverage**: Comprehensive error scenario testing for all workflows
- **Debugging failing tests**: When debugging tests with failing assert! macros,
  add additional context to the assert! macro instead of adding temporary
  println! statements
- **Test Quality**: Never write tests that only exercise language features
  without testing our application logic. Tests should verify actual business
  logic, not just struct field assignments or basic language operations

#### Writing Meaningful Tests

Tests should verify our application logic, not just language features. Avoid
tests that only exercise struct construction or field access without testing any
business logic.

##### ❌ Bad: Testing language features instead of our code

```rust
#[test]
fn test_mint_request_fields() {
    let request = MintRequest {
        qty: Decimal::from(100),
        underlying: "AAPL".to_string(),
    };

    assert_eq!(request.qty, Decimal::from(100));
    assert_eq!(request.underlying, "AAPL");
}
```

This test creates a struct and verifies field assignments, but doesn't test any
of our code logic - it only tests Rust's struct field assignment mechanism.

##### ✅ Good: Testing actual business logic

```rust
#[test]
fn test_mint_validates_positive_quantity() {
    let command = InitiateMint {
        qty: Decimal::from(-10),
        // ...
    };

    let result = Mint::default().handle(command);

    assert!(matches!(result, Err(MintError::InvalidQuantity)));
}
```

This test verifies that our validation logic correctly rejects negative
quantities.

## Workflow Best Practices

- **Always run tests, clippy, and formatters before handing over a piece of
  work**
  - Run `cargo test --workspace` first, as changing tests can break clippy
  - Run
    `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
    next, as fixing linting errors can break formatting
  - Always run `cargo fmt` last to ensure clean code formatting

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
// ❌ NEVER DO THIS - Suppressing lints is forbidden
#[allow(clippy::too_many_lines)]
fn large_function() { /* ... */ }

#[allow(clippy::needless_continue)]
// ❌ NEVER DO THIS - Fix the code structure instead

#[allow(dead_code)]
struct Unused { /* ... */ }  // ❌ Remove the unused code instead

#[allow(unused_imports)]
use some_module::Thing;  // ❌ Remove the unused import instead
```

**Required approach:**

```rust
// ✅ CORRECT - Refactor to address the issue
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
// ✅ CORRECT - Suppressing lint for third-party ABI generated code
sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, "node_modules/@pythnetwork/pyth-sdk-solidity/abis/IPyth.json"
);
```

This policy ensures code quality remains high and prevents technical debt
accumulation through lint suppression.

## Commenting Guidelines

Code should be primarily self-documenting through clear naming, structure, and
type modeling. Comments should only be used when they add meaningful context
that cannot be expressed through code structure alone.

### When to Use Comments

#### ✅ DO comment when:

- **Complex business logic**: Explaining non-obvious domain-specific rules or
  calculations
- **Algorithm rationale**: Why a particular approach was chosen over
  alternatives
- **External system interactions**: Behavior that depends on external APIs or
  protocols
- **Non-obvious technical constraints**: Performance considerations, platform
  limitations
- **Test data context**: Explaining what mock values represent or test scenarios
- **Workarounds**: Temporary solutions with context about why they exist

#### ❌ DON'T comment when:

- The code is self-explanatory through naming and structure
- Restating what the code obviously does
- Describing function signatures (use doc comments instead)
- Adding obvious test setup descriptions
- Marking code sections that are clear from structure

### Good Comment Examples

```rust
// Alpaca requires the journal to be confirmed before we can mint on-chain.
// If we mint before confirmation, we risk minting without the backing shares.
let journal_confirmed = wait_for_journal_confirmation(&mint_id).await?;

// We need to get the corresponding AfterClear event as ClearV2 doesn't
// contain the amounts. So we query the same block number, filter out
// logs with index lower than the ClearV2 log index and with tx hashes
// that don't match the ClearV2 tx hash.
let after_clear_logs = provider.get_logs(/* ... */).await?;

// Test data representing 9 shares with 18 decimal places
alice_output: U256::from_str("9000000000000000000").unwrap(), // 9 shares (18 dps)

/// Helper that converts a fixed-decimal `U256` amount into an `f64` using
/// the provided number of decimals.
///
/// NOTE: Parsing should never fail but precision may be lost.
fn u256_to_f64(amount: U256, decimals: u8) -> Result<f64, ParseFloatError> {
```

### Bad Comment Examples

```rust
// ❌ Redundant - the function name says this
// Execute mint command
execute_mint_command(mint);

// ❌ Obvious from context
// Store mint request
let mint = MintRequest { /* ... */ };
mint.save(&pool).await?;

// ❌ Just restating the code
// Call Alpaca callback endpoint
call_alpaca_callback(&mint_id).await?;

// ❌ Test section markers that add no value
// 1. Test mint initiation
let result = initiate_mint(request).await;

// ❌ Explaining what the code obviously does
// Create a mint command
let command = InitiateMint { /* ... */ };

// ❌ Obvious variable assignments
// Create an aggregate
let aggregate = Mint::default();
```

### Function Documentation

Use Rust doc comments (`///`) for public APIs:

```rust
/// Executes a command against the aggregate and persists resulting events.
///
/// Returns `CommandError::AggregateNotFound` if the aggregate doesn't exist
/// and the command requires an existing aggregate.
pub async fn execute_command<C: Command>(
    aggregate_id: &str,
    command: C,
) -> Result<Vec<Event>, CommandError> {
```

### Comment Maintenance

- Remove comments when refactoring makes them obsolete
- Update comments when changing the logic they describe
- If a comment is needed to explain what code does, consider refactoring for
  clarity
- Keep comments concise and focused on the "why" rather than the "what"

## Code Style

### Module Organization

Organize code within modules by importance and visibility:

- **Public API first**: Place public functions, types, and traits at the top of
  the module where they are immediately visible to consumers
- **Private helpers below public code**: Place private helper functions, types,
  and traits immediately after the public code that uses them
- **Implementation blocks next to type definitions**: Place `impl` blocks after
  the type definition

This organization pattern makes the module's public interface clear at a glance
and keeps implementation details appropriately subordinate.

**Example of good module organization (note that comments are just for
illustration, in real code we wouldn't leave those):**

```rust
// Public struct definition
pub(crate) struct MintRequest {
    pub(crate) id: Option<i64>,
    pub(crate) issuer_request_id: String,
    pub(crate) qty: Decimal,
}

// Implementation block right after type definition
impl MintRequest {
    pub(crate) async fn save(&self, pool: &SqlitePool) -> Result<i64, Error> {
        // Implementation
    }
}

// Public function that uses helper functions
pub(crate) async fn find_mints_by_status(
    pool: &SqlitePool,
    status: MintStatus,
) -> Result<Vec<MintRequest>, Error> {
    let rows = query_by_status(pool, status.as_str()).await?;
    rows.into_iter().map(row_to_mint).collect()
}

// Private helper functions used by find_mints_by_status
async fn query_by_status(
    pool: &SqlitePool,
    status: &str,
) -> Result<Vec<MintRow>, sqlx::Error> {
    // SQL query implementation
}

fn row_to_mint(row: MintRow) -> Result<MintRequest, Error> {
    // Conversion logic
}
```

### Use `.unwrap` over boolean result assertions in tests

Instead of

```rust
assert!(result.is_err());
assert!(matches!(result.unwrap_err(), MintError::InvalidQuantity));
```

or

```rust
assert!(result.is_ok());
assert_eq!(result.unwrap(), expected_events);
```

Write

```rust
assert!(matches!(result.unwrap_err(), MintError::InvalidQuantity));
```

and

```rust
assert_eq!(result.unwrap(), expected_events);
```

so that if we get an unexpected result value, we immediately see the value.

### Type modeling examples

#### Make invalid states unrepresentable:

Instead of using multiple fields that can contradict each other:

```rust
// ❌ Bad: Multiple fields can be in invalid combinations
pub struct MintRequest {
    pub status: String,  // "pending", "completed", "failed"
    pub tx_hash: Option<String>,  // Some when completed, None when pending
    pub minted_at: Option<DateTime<Utc>>,  // Some when completed
    pub shares: Option<u64>,  // Some when completed
    pub error: Option<String>,  // Some when failed
}
```

Use enum variants to encode valid states:

```rust
// ✅ Good: Each state has exactly the data it needs
pub enum MintStatus {
    PendingJournal,
    JournalCompleted,
    Minting,
    CallbackPending,
    Completed {
        tx_hash: String,
        minted_at: DateTime<Utc>,
        shares: u64,
    },
    Failed {
        failed_at: DateTime<Utc>,
        reason: String,
    },
}
```

#### Use newtypes for domain concepts:

```rust
// ❌ Bad: Easy to mix up parameters of the same type
fn initiate_mint(
    tokenization_id: String,
    issuer_id: String,
    symbol: String,
    qty: String,
) { }

// ✅ Good: Type system prevents mixing incompatible values
#[derive(Debug, Clone)]
struct TokenizationRequestId(String);

#[derive(Debug, Clone)]
struct IssuerRequestId(String);

#[derive(Debug, Clone)]
struct Symbol(String);

#[derive(Debug)]
struct Quantity(Decimal);

fn initiate_mint(
    tokenization_id: TokenizationRequestId,
    issuer_id: IssuerRequestId,
    symbol: Symbol,
    qty: Quantity,
) { }
```

### Avoid deep nesting

Prefer flat code over deeply nested blocks to improve readability and
maintainability.

#### Use early returns:

Instead of

```rust
fn validate_mint(data: Option<&MintData>) -> Result<(), Error> {
    if let Some(data) = data {
        if data.qty > Decimal::ZERO {
            if data.symbol.is_supported() {
                Ok(())
            } else {
                Err(Error::UnsupportedSymbol)
            }
        } else {
            Err(Error::InvalidQuantity)
        }
    } else {
        Err(Error::NoData)
    }
}
```

Write

```rust
fn validate_mint(data: Option<&MintData>) -> Result<(), Error> {
    let data = data.ok_or(Error::NoData)?;

    if data.qty <= Decimal::ZERO {
        return Err(Error::InvalidQuantity);
    }

    if !data.symbol.is_supported() {
        return Err(Error::UnsupportedSymbol);
    }

    Ok(())
}
```

#### Use let-else pattern for guard clauses:

The let-else pattern (available since Rust 1.65) is excellent for reducing
nesting when you need to extract a value or return early:

Instead of

```rust
fn process_mint(mint: &Mint) -> Result<Receipt, Error> {
    if let Some(tx_hash) = mint.get_tx_hash() {
        if tx_hash.is_valid() {
            if let Some(receipt_id) = mint.extract_receipt_id() {
                Ok(Receipt::new(tx_hash, receipt_id))
            } else {
                Err(Error::NoReceiptId)
            }
        } else {
            Err(Error::InvalidTxHash)
        }
    } else {
        Err(Error::NoTxHash)
    }
}
```

Write

```rust
fn process_mint(mint: &Mint) -> Result<Receipt, Error> {
    let Some(tx_hash) = mint.get_tx_hash() else {
        return Err(Error::NoTxHash);
    };

    if !tx_hash.is_valid() {
        return Err(Error::InvalidTxHash);
    }

    let Some(receipt_id) = mint.extract_receipt_id() else {
        return Err(Error::NoReceiptId);
    };

    Ok(Receipt::new(tx_hash, receipt_id))
}
```

### Struct field access

Avoid creating unnecessary constructors or getters when they don't add logic
beyond setting/getting field values. Use public fields directly instead.

#### Prefer direct field access:

```rust
pub struct MintRequest {
    pub tokenization_request_id: String,
    pub issuer_request_id: String,
    pub qty: Decimal,
    pub underlying: String,
}

// Create with struct literal syntax
let request = MintRequest {
    tokenization_request_id: "alp-123".to_string(),
    issuer_request_id: "iss-456".to_string(),
    qty: Decimal::from(100),
    underlying: "AAPL".to_string(),
};

// Access fields directly
println!("Quantity: {}", request.qty);
```

#### Avoid unnecessary constructors and getters:

```rust
// Don't create these unless they add meaningful logic
impl MintRequest {
    // Unnecessary - just sets fields without additional logic
    pub fn new(tokenization_id: String, /* ... */) -> Self { /* ... */ }

    // Unnecessary - just returns field value
    pub fn qty(&self) -> Decimal { self.qty }
}
```

This preserves argument clarity and avoids losing information about what each
field represents.
