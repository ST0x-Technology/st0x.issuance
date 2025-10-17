# Implementation Plan: POST /accounts/connect Endpoint Stub

## Objective

Implement a stub endpoint for `POST /accounts/connect` that returns a 200 status
with a dummy `client_id`. This is issue #10 and lays the foundation for the full
account linking feature (issue #11) by establishing the request/response type
definitions.

## Task 1. Define Newtype Wrappers ✓

- [x] Create `src/account.rs` module for account linking feature
- [x] Define `Email` newtype wrapping `String` with `Serialize`/`Deserialize`
- [x] Define `AlpacaAccountNumber` newtype wrapping `String` with
      `Serialize`/`Deserialize`
- [x] Define `ClientId` newtype wrapping `String` with `Serialize`/`Deserialize`
- [x] Add `account` module to `src/main.rs`

## Task 2. Define Request/Response Structures ✓

- [x] Define `AccountLinkRequest` struct with `email` and `account` fields
- [x] Define `AccountLinkResponse` struct with `client_id` field
- [x] Derive `Serialize`/`Deserialize` for both structs

## Task 3. Implement Endpoint Stub ✓

- [x] Add `serde`, `serde_json`, and `rocket` json feature to Cargo.toml
- [x] Implement `connect_account` handler function in `src/account.rs`
- [x] Return hard-coded `ClientId` value ("stub-client-id-123")
- [x] Accept `Json<AccountLinkRequest>` as input
- [x] Return `Json<AccountLinkResponse>` with 200 status
- [x] Mount endpoint at `/accounts/connect` in `src/main.rs`
- [x] Remove placeholder index endpoint

## Task 4. Write Tests ✓

- [x] Add test that posts valid request and receives 200 with client_id
- [x] Verify response structure matches `AccountLinkResponse`
- [x] Verify response client_id is not empty

## Task 5. Quality Checks ✓

- [x] Run `cargo test -q` and ensure all tests pass
- [x] Run `cargo clippy --all-targets --all-features -- -D clippy::all` and fix
      any issues
- [x] Run `cargo fmt` to format code
