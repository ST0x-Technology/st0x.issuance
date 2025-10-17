# Implementation Plan: POST /accounts/connect Endpoint Stub

## Objective

Implement a stub endpoint for `POST /accounts/connect` that returns a 200 status
with a dummy `client_id`. This is issue #10 and lays the foundation for the full
account linking feature (issue #11) by establishing the request/response type
definitions.

## Task 1. Define Newtype Wrappers

### Subtasks

- [ ] Create `src/types.rs` module for domain types
- [ ] Define `Email` newtype wrapping `String` with `Serialize`/`Deserialize`
- [ ] Define `AlpacaAccountNumber` newtype wrapping `String` with
      `Serialize`/`Deserialize`
- [ ] Define `ClientId` newtype wrapping `String` with `Serialize`/`Deserialize`
- [ ] Add `types` module to `src/main.rs`

### Design Rationale

Using newtypes prevents mixing incompatible string values at compile time. For
example, accidentally passing an email where a client_id is expected becomes a
type error rather than a runtime bug. This follows the project's principle of
"making invalid states unrepresentable."

For this stub implementation, we keep the types simple. We'll add validation
logic later when implementing the full feature in issue #11.

## Task 2. Define Request/Response Structures

### Subtasks

- [ ] Create `src/http/mod.rs` module for HTTP-related code
- [ ] Create `src/http/accounts.rs` for account-related endpoints
- [ ] Define `AccountLinkRequest` struct with `email` and `account` fields
- [ ] Define `AccountLinkResponse` struct with `client_id` field
- [ ] Derive `Serialize`/`Deserialize` for both structs
- [ ] Add `http` module to `src/main.rs`

### Design Rationale

We organize HTTP-related code into a dedicated `http` module, with sub-modules
for each endpoint category. This follows the project structure outlined in
README.md and prepares for additional endpoints to be added in later phases.

## Task 3. Implement Endpoint Stub

### Subtasks

- [ ] Add `serde` and `serde_json` dependencies to Cargo.toml
- [ ] Implement `connect_account` handler function in `src/http/accounts.rs`
- [ ] Return hard-coded `ClientId` value (e.g., "stub-client-id-123")
- [ ] Accept `Json<AccountLinkRequest>` as input
- [ ] Return `Json<AccountLinkResponse>` with 200 status
- [ ] Mount endpoint at `/accounts/connect` in `src/main.rs`

### Design Rationale

The stub endpoint provides a working HTTP contract that Alpaca could call while
we develop the full implementation. It accepts the correct request format and
returns the expected response format, allowing integration testing before the
business logic is implemented.

## Task 4. Write Tests

### Subtasks

- [ ] Add test that posts valid request and receives 200 with client_id
- [ ] Verify response structure matches `AccountLinkResponse`
- [ ] Verify response client_id is not empty

### Design Rationale

Tests verify the HTTP contract works as expected. We keep tests simple for the
stub - comprehensive business logic tests will be added in issue #11 when
implementing the full feature with ES/CQRS.

## Task 5. Quality Checks

### Subtasks

- [ ] Run `cargo test -q` and ensure all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all` and fix
      any issues
- [ ] Run `cargo fmt` to format code

### Design Rationale

Following the project workflow ensures code quality before committing. We run in
this specific order because: (1) test failures need to be fixed first, (2)
fixing clippy issues can change formatting, (3) fmt runs last to ensure clean
code.

## Implementation Notes

### Module Organization

Following the project's module organization principles:

- Public types first (newtypes, request/response structs)
- Implementation (endpoint handler) after type definitions
- Private helpers (if needed) below public code
- Tests at the end of each module

### Error Handling

For this stub, we only handle the happy path (200 response). Error cases (404
for email not found, 409 for duplicate links) will be implemented in issue #11
when we add the full business logic with the AccountLink aggregate.

### Dependencies

We already have `serde` in workspace dependencies. We just need to add it to the
main package dependencies with the `derive` feature.

## Future Work (Issue #11)

This stub lays the groundwork for issue #11, which will:

- Implement the `AccountLink` aggregate with ES/CQRS
- Add `LinkAccount` command and `AccountLinked` event
- Implement proper validation (email format, duplicate checking)
- Add error handling for 404/409 status codes
- Create `AccountLinkView` for querying linked accounts
- Add comprehensive tests with Given-When-Then pattern
