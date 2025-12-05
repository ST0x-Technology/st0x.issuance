# Plan: Fix Alpaca Account ID Bug & Add Startup Recovery

## Problem Summary

1. **Bug: Wrong account ID in all Alpaca API calls** - The `AlpacaService` uses
   a static `ALPACA_ACCOUNT_ID` env var for all three methods:
   - `send_mint_callback` → should use user's Alpaca account
   - `call_redeem_endpoint` → should use user's Alpaca account
   - `poll_request_status` → should use user's Alpaca account

   This causes callbacks to fail, leaving mints stuck in `CallbackPending` and
   likely causing redemption issues too.

2. **Missing recovery mechanism** - If the bot restarts or an operation fails,
   mints and redemptions in intermediate states stay stuck forever.

## Task 1. Fix AlpacaService to accept account ID per-call

All three Alpaca API methods need the user's Alpaca account number passed in,
not a static config value.

### Changes Required

- [x] Add `alpaca_account: &AlpacaAccountNumber` parameter to
      `send_mint_callback`
- [x] Add `alpaca_account: &AlpacaAccountNumber` parameter to
      `call_redeem_endpoint`
- [x] Add `alpaca_account: &AlpacaAccountNumber` parameter to
      `poll_request_status`
- [x] Remove `account_id` field from `RealAlpacaService` struct
- [x] Remove `ALPACA_ACCOUNT_ID` from `AlpacaConfig`
- [x] Update `AlpacaService` trait signatures
- [x] Update `MockAlpacaService` to match new signatures
- [x] Update all tests for new signatures

### Verification

- [x] `cargo test --workspace` passes
- [x] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [x] `cargo fmt --all -- --check` passes

## Task 2-4. Add account lookups for Alpaca API calls (CONSOLIDATED)

Originally planned as 3 separate tasks, consolidated into one since the
implementation is cleaner when done together at the call sites rather than in
each manager.

### Design Decision

Instead of adding `pool` to each manager and doing lookups internally, we:

1. Add `client_id()` accessor to `Mint` aggregate
2. Look up account at call sites (`confirm.rs`, `detector.rs`)
3. Pass `alpaca_account` as parameter through the call chain

This keeps managers focused on their single responsibility and avoids
duplicating database access across multiple components.

### Changes Made

- [x] Add `Mint::client_id()` accessor method
- [x] In `confirm.rs`: pass `pool` to `process_journal_completion`, look up
      account using `find_by_client_id`, pass to `CallbackManager`
- [x] In `detector.rs`: update `get_account_info` to return both `client_id` and
      `alpaca_account`, pass through to managers
- [x] Update E2E test mock paths to use `USER123` (the actual account from test
      setup)

### Verification

- [x] `cargo test --workspace` passes (296 tests + E2E)
- [x] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [x] `cargo fmt --all -- --check` passes

## Task 5. Add startup recovery for stuck mints

When the bot starts, check for mints in recoverable states and process them.

### Design Decision

Instead of creating a separate `MintRecoveryService`, recovery methods are added
directly to the existing managers (`MintManager` and `CallbackManager`). This
keeps the code simpler and avoids duplicating dependencies. The managers now
store `pool` and `event_store` to support recovery operations.

### Changes Made

- [x] Add `find_journal_confirmed()` query function to `view.rs`
- [x] Add `find_callback_pending()` query function to `view.rs`
- [x] Add `pool` and `event_store` fields to `MintManager`
- [x] Add `pool` and `event_store` fields to `CallbackManager`
- [x] Add `recover_journal_confirmed_mints()` method to `MintManager`
- [x] Add `recover_callback_pending_mints()` method to `CallbackManager`
- [x] Add `lookup_alpaca_account_for_recovery()` helper to `CallbackManager`
- [x] Wire recovery into startup via `spawn_mint_recovery()` in `lib.rs`
- [x] Add tests for view query functions
- [x] Add tests for recovery methods in both managers

### Design Notes

- Recovery is idempotent - safe to run multiple times
- Failed recovery attempts log errors but don't crash the bot
- Each mint is processed independently; failures don't stop other recoveries

### Verification

- [x] `cargo test --workspace` passes
- [x] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [x] `cargo fmt --all -- --check` passes

## Task 6. Add startup recovery for stuck redemptions

Same pattern as Task 5 - add recovery methods directly to existing managers.

### Changes Required

- [ ] Add `find_detected()` query function to `redemption/view.rs`
- [ ] Add `find_alpaca_called()` query function to `redemption/view.rs`
- [ ] Add `find_burning()` query function to `redemption/view.rs`
- [ ] Add `pool` and `event_store` fields to `RedeemCallManager`
- [ ] Add `recover_detected_redemptions()` method to `RedeemCallManager`
- [ ] Add `pool` and `event_store` fields to `JournalManager`
- [ ] Add `recover_alpaca_called_redemptions()` method to `JournalManager`
- [ ] Add `pool` and `event_store` fields to `BurnManager`
- [ ] Add `recover_burning_redemptions()` method to `BurnManager`
- [ ] Wire recovery into startup via `spawn_redemption_recovery()` in `lib.rs`
- [ ] Add tests for view query functions
- [ ] Add tests for recovery methods in all three managers

### Design Notes

- Same idempotency and error handling approach as mint recovery
- Each manager handles recovery for its own state
- Each redemption is processed independently; failures don't stop other
  recoveries

### Verification

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [ ] `cargo fmt --all -- --check` passes

## Task 7. Manual recovery for existing stuck mints (operational)

After deploying, recover the 3 mints stuck in `CallbackPending`:

- `507a97b0-72a9-42de-8c6a-5a2688332ece`
- `5b8a1ebb-da38-4a57-83ca-cf08f3b47127`
- `7353025e-36ec-4b85-92f5-8261f935cbe8`

### Steps

- [ ] Deploy the fixed code
- [ ] Startup recovery (Task 5) should automatically retry these
- [ ] Verify callbacks succeeded by checking event store for `MintCompleted`
      events
- [ ] Communicate status to Alpaca team
