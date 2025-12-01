# Account Lifecycle Refactor Plan

## Overview

Implements the account lifecycle changes from SPEC.md: splitting account
creation into Registration (internal) and Alpaca Linking (via
`/accounts/connect`).

**Current → Target:**

- Aggregate: `NotLinked` → `Linked` **becomes** `NotRegistered` → `Registered` →
  `LinkedToAlpaca`
- Events: `Linked` **splits into** `Registered` + `LinkedToAlpaca`
- Commands: `Link` **splits into** `Register` + `LinkToAlpaca`
- API: `/accounts/connect` creates account **becomes** `POST /accounts`
  (register) + `/accounts/connect` (link existing)

## Task 1. Refactor Aggregate Core

Update events, commands, aggregate states, handlers, and aggregate tests as a
single unit (these are tightly coupled and must change together).

**`src/account/event.rs`:**

- [ ] Rename `Linked` to `Registered`, remove `alpaca_account`, rename
      `linked_at` to `registered_at`
- [ ] Add `LinkedToAlpaca { alpaca_account, linked_at }`
- [ ] Update `DomainEvent` impl for new event type strings

**`src/account/cmd.rs`:**

- [ ] Rename `Link` to `Register`, remove `alpaca_account`
- [ ] Add `LinkToAlpaca { alpaca_account }`

**`src/account/mod.rs` (aggregate):**

- [ ] Rename `NotLinked` to `NotRegistered`
- [ ] Add `Registered { client_id, email, registered_at }` state
- [ ] Rename `Linked` to `LinkedToAlpaca`, remove `status` field
- [ ] Remove `LinkedAccountStatus` enum
- [ ] Update `handle` for `Register`: `NotRegistered` → produces `Registered`
      event
- [ ] Add `handle` for `LinkToAlpaca`: `Registered` → produces `LinkedToAlpaca`
      event
- [ ] Update `handle` for `WhitelistWallet`: only allowed in `LinkedToAlpaca`
      state
- [ ] Update `apply` for all three events
- [ ] Update errors: rename `AccountAlreadyExists` to
      `AccountAlreadyRegistered`, add `NotRegistered`, add
      `AlreadyLinkedToAlpaca`

**`src/account/mod.rs` (tests):**

- [ ] Update `test_link_account_creates_new_account` →
      `test_register_creates_new_account`
- [ ] Update `test_link_account_when_already_linked_returns_error` →
      `test_register_when_already_registered_returns_error`
- [ ] Add `test_link_to_alpaca_on_registered_account`
- [ ] Add `test_link_to_alpaca_on_not_registered_returns_error`
- [ ] Add `test_link_to_alpaca_when_already_linked_returns_error`
- [ ] Update `test_apply_account_linked_updates_state` →
      `test_apply_registered_updates_state`
- [ ] Add `test_apply_linked_to_alpaca_updates_state`
- [ ] Update wallet whitelisting tests for new state names
- [ ] Run `cargo test -p issuance -- account` and `cargo clippy` and `cargo fmt`

## Task 2. Update View

Update `AccountView` to handle new events and states.

**`src/account/view.rs`:**

- [ ] Add `Registered { client_id, email, registered_at }` variant
- [ ] Rename `Account` to `LinkedToAlpaca`
- [ ] Update `View::update` for `Registered` event
- [ ] Update `View::update` for `LinkedToAlpaca` event (must handle transition
      from `Registered` state, carrying over `client_id` and `email`)
- [ ] Update `WalletWhitelisted` handling for `LinkedToAlpaca` variant
- [ ] Update `find_by_email` to query both `$.Registered.email` and
      `$.LinkedToAlpaca.email`
- [ ] Update `find_by_client_id` to query both states

**`src/account/view.rs` (tests):**

- [ ] Update `test_view_update_from_account_linked_event` →
      `test_view_update_from_registered_event`
- [ ] Add `test_view_update_from_linked_to_alpaca_event`
- [ ] Update `test_find_by_client_id_returns_view` for new state name
- [ ] Update `test_find_by_email_returns_view` for new state name
- [ ] Update wallet whitelisting view tests
- [ ] Run `cargo test -p issuance -- account::view` and `cargo clippy` and
      `cargo fmt`

## Task 3. Add Internal Auth Guard

Create a separate auth guard for internal endpoints that uses localhost IP
whitelist instead of Alpaca IP ranges. Refactor `src/auth.rs` into submodules
organized by domain concept (not by technical layer).

**`src/auth/mod.rs`:**

- [ ] Convert `src/auth.rs` to `src/auth/mod.rs`
- [ ] Keep guards here: `IssuerAuth`, `InternalAuth` (new)
- [ ] Keep `AuthError` here (tightly coupled to guards)
- [ ] Add `AuthConfig` struct with:
  - `issuer_api_key: String`
  - `alpaca_ip_ranges: IpWhitelist`
  - `internal_ip_ranges: IpWhitelist` (default: `127.0.0.1/8,::1/128`)
- [ ] Derive `clap::Args` for `AuthConfig`
- [ ] Create `InternalAuth` guard (IP-only check against `internal_ip_ranges`)
- [ ] Extract shared guard logic into helper functions
- [ ] Re-export public types from submodules

**`src/auth/ip_whitelist.rs`:**

- [ ] Move `IpWhitelist` and `IpWhitelistParseError` here
- [ ] Add tests for `IpWhitelist` parsing and `is_allowed` logic

**`src/auth/rate_limit.rs`:**

- [ ] Move `FailedAuthRateLimiter` and `RateLimiterError` here
- [ ] Move `test_rate_limit_11th_failed_attempt_returns_429` here
- [ ] Move `test_successful_auth_does_not_count_against_limit` here

**`src/config.rs`:**

- [ ] Remove `issuer_api_key` and `alpaca_ip_ranges` fields
- [ ] Add `#[clap(flatten)] pub auth: AuthConfig`
- [ ] Update any code that accesses `config.issuer_api_key` to use
      `config.auth.issuer_api_key`

**`src/auth/mod.rs` (tests):**

- [ ] Keep guard-related tests: `test_missing_api_key_header_returns_401`,
      `test_invalid_api_key_returns_401`, `test_empty_ip_ranges_allows_*`,
      `test_configured_ip_ranges_block_*`
- [ ] Add `test_internal_auth_allows_localhost`
- [ ] Add `test_internal_auth_blocks_external_ip`
- [ ] Update existing guard tests to use `AuthConfig`
- [ ] Run `cargo test -p issuance -- auth` and `cargo clippy` and `cargo fmt`

## Task 4. Add Registration Endpoint

Add internal `POST /accounts` endpoint for manual AP registration.

**`src/account/api.rs`:**

- [ ] Add `RegisterAccountRequest { email }` struct
- [ ] Add `POST /accounts` endpoint (`register_account` function):
  - Use `InternalAuth` guard (not `IssuerAuth`)
  - Check email not already registered via `find_by_email`
  - Generate `client_id`
  - Execute `Register` command
  - Return `RegisterAccountResponse { client_id }`
- [ ] Export `register_account` from `src/account/mod.rs`

**`src/main.rs`:**

- [ ] Mount `register_account` route

**`src/account/api.rs` (tests):**

- [ ] Add `test_register_account_returns_client_id`
- [ ] Add `test_register_duplicate_email_returns_409`
- [ ] Add `test_register_invalid_email_returns_422`
- [ ] Run `cargo test -p issuance -- account::api` and `cargo clippy` and
      `cargo fmt`

## Task 5. Update Connect Endpoint

Modify `/accounts/connect` to look up existing account by email instead of
creating a new one.

**`src/account/api.rs`:**

- [ ] Update `connect_account`:
  - Look up account by email via `find_by_email`
  - Return 404 if not found
  - Return 409 if already linked to Alpaca
  - Get `client_id` from existing `Registered` account
  - Execute `LinkToAlpaca` command using existing `client_id` as aggregate ID
  - Return `AccountLinkResponse { client_id }`
- [ ] Update `whitelist_wallet`:
  - Change from `IssuerAuth` to `InternalAuth` (internal endpoint)
  - Check for `LinkedToAlpaca` view state (not `Account`)

**`src/account/api.rs` (tests):**

- [ ] Update `test_connect_account_returns_client_id` to first register, then
      connect
- [ ] Add `test_connect_account_when_not_registered_returns_404`
- [ ] Update `test_duplicate_account_link_returns_409` for new flow (register →
      connect → connect again)
- [ ] Update `test_events_are_persisted_correctly` for new event names
- [ ] Update `test_views_are_updated_correctly` for new state names
- [ ] Run `cargo test -p issuance -- account::api` and `cargo clippy` and
      `cargo fmt`

## Task 6. Final Validation

- [ ] Run `cargo test --workspace`
- [ ] Run
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt --all -- --check`
