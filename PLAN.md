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

## Task 1. Remove Wallet from Account Linking (Event, Command, Aggregate)

This task makes `/accounts/connect` compliant with Alpaca's specification by
removing the wallet requirement from account linking.

- [ ] Remove `wallet: Address` field from `AccountEvent::Linked` in
      `src/account/event.rs`
- [ ] Remove `wallet: Address` field from `AccountCommand::Link` in
      `src/account/cmd.rs`
- [ ] Update `Account::Linked` variant in `src/account/mod.rs` to have
      `whitelisted_wallets: Vec<Address>` field instead of single wallet
- [ ] Update aggregate handle() method for Link command to initialize with empty
      `whitelisted_wallets: Vec::new()`
- [ ] Update aggregate apply() method for Linked event to set
      `whitelisted_wallets: Vec::new()`
- [ ] Update existing unit tests in `src/account/mod.rs`:
  - [ ] Remove wallet from `AccountEvent::Linked` test data
  - [ ] Remove wallet from `AccountCommand::Link` test cases
  - [ ] Update `test_apply_account_linked_updates_state` to check
        whitelisted_wallets is empty vec
- [ ] Run tests: `cargo test -q --lib account`

## Task 2. Update Account View and Database Schema

This task updates the database schema and view logic to support multiple
whitelisted wallets.

- [ ] Update `AccountView::Account` variant in `src/account/view.rs` to have
      `whitelisted_wallets: Vec<Address>` instead of single `wallet: Address`
- [ ] Update View::update() method to handle `AccountEvent::Linked` with
      `whitelisted_wallets: Vec::new()`
- [ ] Update unit test `test_view_update_from_account_linked_event` to expect
      empty whitelisted_wallets
- [ ] Create database migration: `sqlx migrate add update_account_view_wallets`
- [ ] Edit migration file to:
  - [ ] Drop existing `wallet_indexed` column
  - [ ] Drop existing index `idx_account_view_wallet_indexed`
- [ ] Reset database: `sqlx db reset -y`
- [ ] Run tests: `cargo test -q --lib account::view`

## Task 3. Update /accounts/connect Endpoint

This task removes the wallet field from the account linking endpoint.

- [ ] Remove `wallet: Address` field from `AccountLinkRequest` struct in
      `src/account/api.rs`
- [ ] Remove wallet from tracing instrument fields
- [ ] Update command construction to not include wallet parameter
- [ ] Update all endpoint tests:
  - [ ] Remove wallet from all test request JSON bodies
  - [ ] Update `test_views_are_updated_correctly` to verify whitelisted_wallets
        is empty
- [ ] Run tests: `cargo test -q account::api`
- [ ] Run workspace tests to ensure nothing else broke:
      `cargo test --workspace -q`

## Task 4. Add Wallet Whitelisting (Event, Command, Aggregate)

This task adds the ability to whitelist wallets on an already-linked account.

- [ ] Add `AccountEvent::WalletWhitelisted` variant in `src/account/event.rs`
      with fields:
  - [ ] `wallet: Address`
  - [ ] `whitelisted_at: DateTime<Utc>`
- [ ] Update event_type() match to return "AccountEvent::WalletWhitelisted" for
      new variant
- [ ] Add `AccountCommand::WhitelistWallet` variant in `src/account/cmd.rs` with
      `wallet: Address` field
- [ ] Add error variant in `src/account/mod.rs`:
  - [ ] `AccountError::AccountNotLinked`
- [ ] Add handle() logic for WhitelistWallet command:
  - [ ] Match on aggregate state, return AccountNotLinked if NotLinked
  - [ ] Check if wallet already in whitelisted_wallets vec, return empty vec if
        already whitelisted (idempotent)
  - [ ] Return vec containing WalletWhitelisted event with wallet and current
        timestamp if not already whitelisted
- [ ] Add apply() logic for WalletWhitelisted event:
  - [ ] Match on event variant
  - [ ] If aggregate is Linked, push wallet to whitelisted_wallets vec
- [ ] Write unit tests:
  - [ ] Test whitelisting wallet on linked account produces WalletWhitelisted
        event
  - [ ] Test whitelisting wallet on NotLinked account returns AccountNotLinked
        error
  - [ ] Test whitelisting already-whitelisted wallet returns empty vec
        (idempotent)
  - [ ] Test apply() correctly adds wallet to whitelisted_wallets vec
  - [ ] Test apply() can add multiple different wallets
- [ ] Run tests: `cargo test -q --lib account`

## Task 5. Update Account View for Wallet Whitelisting

This task updates the view to handle wallet whitelisting events and enables
wallet-based lookup.

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
- [ ] Write unit tests:
  - [ ] Test view update from WalletWhitelisted event adds wallet to vec
  - [ ] Test find_by_wallet finds account with whitelisted wallet
  - [ ] Test find_by_wallet returns None for non-whitelisted wallet
  - [ ] Test find_by_wallet works with multiple whitelisted wallets
- [ ] Run tests: `cargo test -q --lib account::view`

## Task 6. Create Wallet Whitelisting Endpoint

This task creates the HTTP endpoint for whitelisting wallets.

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
- [ ] Write integration tests:
  - [ ] Test whitelisting wallet for existing client_id returns 200
  - [ ] Test whitelisting wallet for non-existent client_id returns 404
  - [ ] Test whitelisting same wallet twice returns 200 both times (idempotent)
  - [ ] Test whitelisting same wallet twice produces only one event (check
        events table)
  - [ ] Test view is updated (query account_view via find_by_wallet)
  - [ ] Test multiple different wallets can be whitelisted for same account
- [ ] Add route to Rocket in `src/main.rs` (mount at
      `/accounts/<client_id>/wallets`)
- [ ] Run tests: `cargo test -q --lib account::api::whitelist_wallet`
- [ ] Run all account tests: `cargo test -q account`

## Task 7. Update Mint API Tests

This task updates mint endpoint tests to use the new two-step account setup
flow.

- [ ] Find all test helper functions in `src/mint/api/test_utils.rs` or
      individual test files that link accounts
- [ ] Update account linking to remove wallet field
- [ ] Add wallet whitelisting step after account linking in test setup
- [ ] Run tests: `cargo test -q mint::api`

## Task 8. Update E2E Tests

This task updates end-to-end tests to use the new account linking and wallet
whitelisting flow.

- [ ] Find account setup code in `tests/e2e.rs`
- [ ] Remove wallet field from account linking HTTP request
- [ ] Add HTTP request to whitelist wallet after account is linked
- [ ] Verify redemption flow still works with whitelisted wallet lookup
- [ ] Run E2E tests: `cargo test --test e2e -q`

## Task 9. Update README Documentation

This task updates README.md to reflect the new two-step account setup flow.
(SPEC.md was already updated in Tasks 3 and 6)

- [ ] Update README.md:
  - [ ] Add `POST /accounts/{client_id}/wallets` to endpoints list
  - [ ] Update account linking section to show it no longer requires wallet
  - [ ] Update redemption flow description to mention wallet must be whitelisted
        before redemption
  - [ ] Add note about multiple wallets per account support

## Task 10. Final Validation

This task ensures all code passes checks and tests.

- [ ] Run all workspace tests: `cargo test --workspace -q`
- [ ] Run clippy:
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run formatter: `cargo fmt`
- [ ] Verify E2E tests pass: `cargo test --test e2e -q`
