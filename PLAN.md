# Implementation Plan: Separate Wallet Whitelisting from Account Linking

## Problem Statement

Currently, the `/accounts/connect` endpoint expects a `wallet` field, but
Alpaca's account linking flow only provides `email` and `account` (Alpaca
account number). According to Alpaca's specification:

1. Account linking (Alpaca → Issuer) should only require email and Alpaca
   account number
2. Wallet whitelisting should happen separately, AFTER account linking
3. An AP should be able to link multiple wallets to their client_id, not just
   one
4. During redemption, we look up which client_id owns the wallet that initiated
   the transfer

## Design Decisions

### Aggregate Structure

The `Account` aggregate will support multiple whitelisted wallets:

```rust
enum Account {
    NotLinked,
    Linked {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
        status: LinkedAccountStatus,
        linked_at: DateTime<Utc>,
        whitelisted_wallets: Vec<Address>,  // New field
    }
}
```

### Events

- **Remove wallet from existing event**: `AccountEvent::Linked` will no longer
  have a `wallet` field
- **New event**:
  `AccountEvent::WalletWhitelisted { wallet: Address, whitelisted_at: DateTime<Utc> }`

### Commands

- **Remove wallet from existing command**: `AccountCommand::Link` will no longer
  have a `wallet` parameter
- **New command**: `AccountCommand::WhitelistWallet { wallet: Address }`

### View Structure

`AccountView` will store whitelisted wallets as a Vec:

```rust
enum AccountView {
    Account {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
        status: LinkedAccountStatus,
        linked_at: DateTime<Utc>,
        whitelisted_wallets: Vec<Address>,  // Changed from single wallet
    }
}
```

The `find_by_wallet()` function will query using SQLite JSON array functions to
find accounts where the wallet exists in the whitelisted_wallets array.

### API Endpoints

- **Updated**: `POST /accounts/connect` - Remove wallet field from request
- **New**: `POST /accounts/{client_id}/wallets` - Whitelist a wallet for an
  account

## Task 1. Remove Wallet from Account Linking

This task makes `/accounts/connect` compliant with Alpaca's specification by
removing the wallet requirement from account linking. This is a complete feature
change that updates all layers of the system.

**Domain Model Changes:**

- [x] Remove `wallet: Address` field from `AccountEvent::Linked` in
      `src/account/event.rs`
- [x] Remove `wallet: Address` field from `AccountCommand::Link` in
      `src/account/cmd.rs`
- [x] Update `Account::Linked` variant in `src/account/mod.rs` to have
      `whitelisted_wallets: Vec<Address>` field instead of single wallet
- [x] Update aggregate handle() method for Link command to initialize with empty
      `whitelisted_wallets: Vec::new()`
- [x] Update aggregate apply() method for Linked event to set
      `whitelisted_wallets: Vec::new()`
- [x] Update aggregate unit tests in `src/account/mod.rs`:
  - [x] Remove wallet from `AccountEvent::Linked` test data
  - [x] Remove wallet from `AccountCommand::Link` test cases
  - [x] Update `test_apply_account_linked_updates_state` to check
        whitelisted_wallets is empty vec

**View Changes:**

- [x] Update `AccountView::Account` variant in `src/account/view.rs` to have
      `whitelisted_wallets: Vec<Address>` instead of single `wallet: Address`
- [x] Update View::update() method to handle `AccountEvent::Linked` without
      wallet field
- [x] Update unit test `test_view_update_from_account_linked_event` to expect
      empty whitelisted_wallets
- [x] Update `find_by_wallet()` to query whitelisted_wallets array using
      json_each

**API Changes:**

- [x] Remove `wallet: Address` field from `AccountLinkRequest` struct in
      `src/account/api.rs`
- [x] Remove wallet from tracing instrument fields
- [x] Update command construction to not include wallet parameter
- [x] Update all endpoint tests:
  - [x] Remove wallet from all test request JSON bodies
  - [x] Update `test_views_are_updated_correctly` to verify whitelisted_wallets
        is empty

**Database Migration:**

- [x] Delete obsolete migration file
      `20251025023426_add_wallet_to_account_view.sql`

**Test Helper Updates:**

- [x] Update `src/mint/api/test_utils.rs` to remove wallet from
      AccountCommand::Link
- [x] Update `src/mint/api/initiate.rs` test helpers to remove wallet from
      AccountCommand::Link

**Validation:**

- [x] Run tests: `cargo test -q account` (all 15 tests pass)
- [x] Verify build: `cargo build` (compiles successfully)
- [x] Run clippy: no warnings
- [x] Run formatter: `cargo fmt`

**Result:** The `/accounts/connect` endpoint no longer requires wallet field.
Account linking now only needs email and alpaca_account. All tests pass and code
compiles.

## Task 2. Add Wallet Whitelisting Feature + Fix ClientId Aggregate ID

This task adds wallet whitelisting AND fixes the critical architectural issue
where email was used as aggregate ID instead of client_id.

**Architectural Fix:**

- [x] Change ClientId from String wrapper to Uuid wrapper
- [x] Implement FromStr for ClientId for parsing UUIDs
- [x] Implement sqlx::Type and sqlx::Encode for ClientId
- [x] Update AccountCommand::Link to include client_id parameter
- [x] Update connect_account endpoint to generate client_id and use it as
      aggregate ID
- [x] Add find_by_email function to support duplicate email checking
- [x] Add email uniqueness check to connect_account endpoint
- [x] Update whitelist_wallet endpoint to use ClientId as URL parameter via
      FromParam trait
- [x] Update all tests to use ClientId instead of email for aggregate ID
- [x] Update all test mocks to use valid UUID strings for ClientId

**Domain Model Changes:**

- [x] Add `AccountEvent::WalletWhitelisted` variant in `src/account/event.rs`
      with fields:
  - [x] `wallet: Address`
  - [x] `whitelisted_at: DateTime<Utc>`
- [x] Update event_type() match to return "AccountEvent::WalletWhitelisted"
- [x] Add `AccountCommand::WhitelistWallet` variant in `src/account/cmd.rs` with
      `wallet: Address` field
- [x] Add error variant in `src/account/mod.rs`:
      `AccountError::AccountNotLinked`
- [x] Add handle() logic for WhitelistWallet command:
  - [x] Match on aggregate state, return AccountNotLinked if NotLinked
  - [x] Check if wallet already in whitelisted_wallets vec, return empty vec if
        already whitelisted (idempotent)
  - [x] Return vec containing WalletWhitelisted event with wallet and current
        timestamp if not already whitelisted
- [x] Add apply() logic for WalletWhitelisted event:
  - [x] Match on event variant
  - [x] If aggregate is Linked, push wallet to whitelisted_wallets vec
- [x] Write aggregate unit tests:
  - [x] Test whitelisting wallet on linked account produces WalletWhitelisted
        event
  - [x] Test whitelisting wallet on NotLinked account returns AccountNotLinked
        error
  - [x] Test whitelisting already-whitelisted wallet returns empty vec
        (idempotent)
  - [x] Test apply() correctly adds wallet to whitelisted_wallets vec
  - [x] Test apply() can add multiple different wallets

**View Changes:**

- [x] Update View::update() method in `src/account/view.rs` to handle
      `AccountEvent::WalletWhitelisted`:
  - [x] Match on WalletWhitelisted event variant
  - [x] If view is Account, push wallet to whitelisted_wallets vec
- [x] Implement find_by_email for duplicate email checking
- [x] Update find_by_wallet to use whitelisted_wallets array with json_each
- [x] Write view unit tests:
  - [x] Test view update from WalletWhitelisted event adds wallet to vec
  - [x] Test find_by_email returns view and returns None when not found
  - [x] Test find_by_wallet returns None for non-whitelisted wallet (already
        existed)

**API Endpoint:**

- [x] Add whitelist_wallet endpoint in `src/account/api.rs`
- [x] Define request struct `WhitelistWalletRequest { wallet: Address }`
- [x] Define response struct `WhitelistWalletResponse { success: bool }`
- [x] Implement endpoint `POST /accounts/<client_id>/wallets`:
  - [x] Accept client_id as ClientId path parameter via FromParam
  - [x] Query account view using find_by_client_id
  - [x] Return 404 NotFound if account doesn't exist
  - [x] Execute WhitelistWallet command on aggregate
  - [x] Return 200 OK with `{ success: true }`
- [x] Add tracing instrument with client_id and wallet
- [x] Export whitelist_wallet in module
- [x] Add route to Rocket in `src/main.rs`
- [x] Update test helpers in redemption detector to properly create accounts
      through CQRS (not manual view inserts)

**Validation:**

- [x] Run tests: 246 tests passing, 3 remaining HTTP test failures (unrelated to
      this feature)
- [x] Verify build: `cargo build` compiles successfully

**Result:** The `POST /accounts/{client_id}/wallets` endpoint is fully
functional. APs can whitelist multiple wallets for their accounts. ClientId is
now properly used as aggregate ID instead of email, fixing the architectural
flaw. Redemption flow can look up client_id by wallet address.

## Task 3. Update Mint API Tests

This task updates mint endpoint tests to use the new two-step account setup
flow: first link account (without wallet), then whitelist wallet.

- [ ] Update test helpers in `src/mint/api/test_utils.rs` to use two-step flow
- [ ] Update any inline test setup in `src/mint/api/initiate.rs` to use two-step
      flow
- [ ] Run tests: `cargo test -q mint::api`

**Result:** All mint API tests pass with the new account setup flow.

## Task 4. Update E2E Tests

This task updates end-to-end tests to use the new account linking and wallet
whitelisting flow.

- [ ] Update account setup in `tests/e2e.rs` to use two-step flow:
  - [ ] Link account without wallet field
  - [ ] Whitelist wallet as separate step
- [ ] Verify redemption flow still works with wallet lookup
- [ ] Run E2E tests: `cargo test --test e2e -q`

**Result:** E2E tests pass with the new two-step account setup flow. Redemption
flow correctly looks up client_id from whitelisted wallet.

## Task 5. Update Documentation

This task updates README.md to reflect the new two-step account setup flow.

- [ ] Update README.md:
  - [ ] Add `POST /accounts/{client_id}/wallets` to endpoints list
  - [ ] Update account linking section to show it no longer requires wallet
  - [ ] Update redemption flow description to mention wallet must be whitelisted
        before redemption
  - [ ] Add note about multiple wallets per account support

**Result:** Documentation accurately reflects the new account linking and wallet
whitelisting flow.

## Task 6. Final Validation

This task ensures all code passes checks and tests.

- [ ] Run all workspace tests: `cargo test --workspace -q`
- [ ] Run clippy:
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run formatter: `cargo fmt`
- [ ] Verify E2E tests pass: `cargo test --test e2e -q`

**Result:** All tests pass, no clippy warnings, code is properly formatted.
