# Implementation Plan: Issue #11 - Implement LinkAccount Feature

This plan implements the complete LinkAccount feature using ES/CQRS
architecture, transforming the stub endpoint from issue #10 into a fully
functional account linking system.

## Context

Issue #10 created the endpoint stub with basic types. This issue implements the
full ES/CQRS backend:

- AccountLink aggregate with commands and events
- AccountLinkView for querying account links
- Database migrations
- CQRS framework wiring
- Comprehensive tests

## Task 1. Define Domain Types

Create the complete type system for account linking in `src/account.rs`:

- [ ] Add `AccountStatus` enum (Active, Suspended, Inactive)
- [ ] Add `AccountLink` aggregate state structure
- [ ] Define commands: `LinkAccount`, `UnlinkAccount`, `SuspendAccount`,
      `ReactivateAccount`
- [ ] Define events: `AccountLinked`, `AccountUnlinked`, `AccountSuspended`,
      `AccountReactivated`
- [ ] Add error type `AccountLinkError` for domain errors

**Design Notes:**

- Commands represent intent (imperative mood)
- Events represent facts (past tense)
- Aggregate state captures all information needed for business logic
- Status enum encodes valid account states
- Error type covers validation failures and state transition errors

## Task 2. Implement AccountLink Aggregate

Implement the `Aggregate` trait for `AccountLink` in `src/account.rs`:

- [ ] Implement `Aggregate::handle()` for each command:
  - `LinkAccount`: Validate email format, generate client_id, produce
    `AccountLinked` event
  - `UnlinkAccount`: Check if linked, produce `AccountUnlinked` event
  - `SuspendAccount`: Check if active, produce `AccountSuspended` event
  - `ReactivateAccount`: Check if suspended, produce `AccountReactivated` event
- [ ] Implement `Aggregate::apply()` for each event:
  - Update aggregate state deterministically from events
  - Never fail - events are historical facts
- [ ] Add validation logic:
  - Email format validation (basic check for @ symbol)
  - State transition validation (can't unlink non-existent account, etc.)
  - Client ID generation using UUID v4

**Design Notes:**

- `handle()` contains all business logic and validation
- `handle()` returns `Result<Vec<Event>, Error>` - can return multiple events
- `apply()` is pure state updates only, never fails
- Aggregate ID is the client_id
- For LinkAccount, generate a new UUID-based client_id

## Task 3. Create AccountLinkView

Implement the view projection in `src/account.rs`:

- [ ] Create `AccountLinkView` struct with fields matching the spec
- [ ] Implement `View` trait with `update()` method:
  - `AccountLinked`: Create new view entry
  - `AccountUnlinked`: Mark as inactive
  - `AccountSuspended`: Update status to suspended
  - `AccountReactivated`: Update status to active
- [ ] Add query methods:
  - `find_by_email()`: Look up account by email
  - `find_by_alpaca_account()`: Look up by Alpaca account number
  - `find_by_client_id()`: Look up by client_id

**Design Notes:**

- Views are denormalized read models
- Each event updates the view state
- Views enable efficient queries without replaying events
- Store as JSON in database for flexibility

## Task 4. Add Database Migrations

Create migration for the account_link_view table:

- [ ] Create migration file in `migrations/` directory
- [ ] Define `account_link_view` table schema:
  - `view_id TEXT PRIMARY KEY` (client_id)
  - `version BIGINT NOT NULL` (last event sequence applied)
  - `payload JSON NOT NULL` (view state as JSON)
- [ ] Add indexes:
  - `idx_account_link_email` on `json_extract(payload, '$.email')`
  - `idx_account_link_alpaca` on `json_extract(payload, '$.alpaca_account')`
  - `idx_account_link_status` on `json_extract(payload, '$.status')`

**Design Notes:**

- Follows the same pattern as other view tables (mint_view, redemption_view)
- JSON storage allows flexible schema evolution
- Indexes enable efficient lookups by email and Alpaca account

## Task 5. Wire Up CQRS Framework

Integrate the aggregate and view with the CQRS framework:

- [ ] Update `src/main.rs` to create database connection pool
- [ ] Create `SqliteEventRepository` for AccountLink aggregate
- [ ] Create `SqliteViewRepository` for AccountLinkView
- [ ] Wire up `CqrsFramework` with event store and views
- [ ] Pass framework to the endpoint handler via Rocket state

**Design Notes:**

- Use `sqlite_cqrs()` helper from sqlite-es crate
- Register AccountLinkView with framework
- Store framework in Rocket state for access from endpoints
- Framework handles command execution and view updates

## Task 6. Update Endpoint Implementation

Replace the stub endpoint with actual aggregate integration:

- [ ] Update `connect_account()` to accept framework from Rocket state
- [ ] Execute `LinkAccount` command via framework
- [ ] Handle errors and return appropriate HTTP status codes:
  - 200: Successful link
  - 404: Email not found (for now, we'll auto-create - spec unclear)
  - 409: Account already linked
- [ ] Query view to get the created account link
- [ ] Return `AccountLinkResponse` with the generated client_id

**Design Notes:**

- For now, we'll create accounts automatically (spec says 404 if email not
  found, but unclear how accounts are pre-registered)
- Check for duplicate links by querying view before executing command
- Use `cqrs.execute()` to execute commands
- Error handling should map domain errors to HTTP status codes

## Task 7. Add Aggregate Tests

Write Given-When-Then tests for the aggregate:

- [ ] Test `LinkAccount` command:
  - Happy path: Creates new account link
  - Error: Email format validation
- [ ] Test `UnlinkAccount` command:
  - Happy path: Unlinks existing account
  - Error: Cannot unlink non-existent account
- [ ] Test `SuspendAccount` command:
  - Happy path: Suspends active account
  - Error: Cannot suspend non-existent account
- [ ] Test `ReactivateAccount` command:
  - Happy path: Reactivates suspended account
  - Error: Cannot reactivate non-existent account

**Design Notes:**

- Use `cqrs_es::test::TestFramework` for aggregate testing
- Given-When-Then pattern makes tests clear and maintainable
- Test both happy paths and error cases
- Mock services if needed (though AccountLink likely doesn't need external
  services)

## Task 8. Add View Tests

Write tests for the view projection:

- [ ] Test view updates from each event type
- [ ] Test query methods (find_by_email, find_by_alpaca_account)
- [ ] Test that views are correctly built from event stream

**Design Notes:**

- Test view.update() method directly
- Verify view state after each event
- Test query methods return correct results

## Task 9. Add Integration Tests

Write end-to-end tests for the endpoint:

- [ ] Test successful account link
- [ ] Test duplicate account link (409 error)
- [ ] Test invalid email format (400 error)
- [ ] Test that events are persisted correctly
- [ ] Test that views are updated correctly

**Design Notes:**

- Use in-memory SQLite for isolated tests
- Test the full stack: endpoint → aggregate → events → views
- Verify that events are in the event store
- Verify that views reflect the events

## Task 10. Update Dependencies

Add required dependencies:

- [ ] Add `cqrs-es` to Cargo.toml
- [ ] Add `async-trait` if needed for async aggregate methods
- [ ] Add `uuid` for generating client IDs
- [ ] Add `chrono` for timestamps
- [ ] Ensure `sqlite-es` crate is properly linked

**Design Notes:**

- Use workspace dependencies where possible
- Follow existing patterns from other aggregates if any exist
- Check Cargo.toml for already included dependencies

## Implementation Order

1. Start with Task 1 (types) and Task 2 (aggregate) - these are the core domain
   logic
2. Then Task 3 (view) and Task 4 (migrations) - the read model
3. Then Task 5 (wiring) and Task 6 (endpoint) - the integration
4. Finally Task 7, 8, 9 (tests) - verify everything works
5. Task 10 (dependencies) as needed throughout

## Success Criteria

- [ ] POST /accounts/connect creates account link with generated client_id
- [ ] Events are persisted in the event store
- [ ] Views are updated from events
- [ ] Duplicate links return 409 error
- [ ] All tests pass
- [ ] Clippy passes
- [ ] Code is formatted
