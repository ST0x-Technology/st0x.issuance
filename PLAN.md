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

## Task 2. Add Wallet Whitelisting Feature

This task adds the complete wallet whitelisting feature: domain model, view,
endpoint, and all tests. After this task, APs can whitelist multiple wallets for
their accounts.

**Domain Model Changes:**

- [ ] Add `AccountEvent::WalletWhitelisted` variant in `src/account/event.rs`
      with fields:
  - [ ] `wallet: Address`
  - [ ] `whitelisted_at: DateTime<Utc>`
- [ ] Update event_type() match to return "AccountEvent::WalletWhitelisted"
- [ ] Add `AccountCommand::WhitelistWallet` variant in `src/account/cmd.rs` with
      `wallet: Address` field
- [ ] Add error variant in `src/account/mod.rs`:
      `AccountError::AccountNotLinked`
- [ ] Add handle() logic for WhitelistWallet command:
  - [ ] Match on aggregate state, return AccountNotLinked if NotLinked
  - [ ] Check if wallet already in whitelisted_wallets vec, return empty vec if
        already whitelisted (idempotent)
  - [ ] Return vec containing WalletWhitelisted event with wallet and current
        timestamp if not already whitelisted
- [ ] Add apply() logic for WalletWhitelisted event:
  - [ ] Match on event variant
  - [ ] If aggregate is Linked, push wallet to whitelisted_wallets vec
- [ ] Write aggregate unit tests:
  - [ ] Test whitelisting wallet on linked account produces WalletWhitelisted
        event
  - [ ] Test whitelisting wallet on NotLinked account returns AccountNotLinked
        error
  - [ ] Test whitelisting already-whitelisted wallet returns empty vec
        (idempotent)
  - [ ] Test apply() correctly adds wallet to whitelisted_wallets vec
  - [ ] Test apply() can add multiple different wallets

**View Changes:**

- [ ] Update View::update() method in `src/account/view.rs` to handle
      `AccountEvent::WalletWhitelisted`:
  - [ ] Match on WalletWhitelisted event variant
  - [ ] If view is Account, push wallet to whitelisted_wallets vec
  - [ ] If view is Unavailable, do nothing (defensive, shouldn't happen)
- [ ] Update `find_by_wallet()` function to query wallets within JSON array:
  - [ ] Use `json_each` to expand the whitelisted_wallets array
  - [ ] Match against the expanded values
  - [ ] SQL pattern:
        `EXISTS(SELECT 1 FROM json_each(payload, '$.Account.whitelisted_wallets') WHERE value = ?)`
- [ ] Write view unit tests:
  - [ ] Test view update from WalletWhitelisted event adds wallet to vec
  - [ ] Test find_by_wallet finds account with whitelisted wallet
  - [ ] Test find_by_wallet returns None for non-whitelisted wallet
  - [ ] Test find_by_wallet works with multiple whitelisted wallets

**API Endpoint:**

- [ ] Create `src/account/api/whitelist_wallet.rs` module
- [ ] Define request struct `WhitelistWalletRequest { wallet: Address }`
- [ ] Define response struct `WhitelistWalletResponse { success: bool }`
- [ ] Implement endpoint `POST /accounts/<client_id>/wallets`:
  - [ ] Accept client_id as path parameter (String)
  - [ ] Parse ClientId from path parameter
  - [ ] Query account view using find_by_client_id to get aggregate_id (email)
  - [ ] Return 404 NotFound if account doesn't exist
  - [ ] Execute WhitelistWallet command on aggregate
  - [ ] Map AccountNotLinked error to 500 InternalServerError (defensive,
        shouldn't happen)
  - [ ] Return 200 OK with `{ success: true }` (idempotent - returns success
        even if wallet was already whitelisted)
- [ ] Add tracing instrument with client_id and wallet
- [ ] Export in `src/account/api.rs`: add `mod whitelist_wallet;` and
      `pub use whitelist_wallet::whitelist_wallet;`
- [ ] Add route to Rocket in `src/main.rs` (mount at
      `/accounts/<client_id>/wallets`)
- [ ] Write integration tests:
  - [ ] Test whitelisting wallet for existing client_id returns 200
  - [ ] Test whitelisting wallet for non-existent client_id returns 404
  - [ ] Test whitelisting same wallet twice returns 200 both times (idempotent)
  - [ ] Test whitelisting same wallet twice produces only one event (check
        events table)
  - [ ] Test view is updated (query account_view via find_by_wallet)
  - [ ] Test multiple different wallets can be whitelisted for same account

**Validation:**

- [ ] Run tests: `cargo test --workspace -q`
- [ ] Verify build: `cargo build`

**Result:** The `POST /accounts/{client_id}/wallets` endpoint is fully
functional. APs can whitelist multiple wallets for their accounts. Redemption
flow can look up client_id by wallet address. All tests pass and code compiles.

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
