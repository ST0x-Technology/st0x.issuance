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

- [ ] Add `alpaca_account: &AlpacaAccountNumber` parameter to
      `send_mint_callback`
- [ ] Add `alpaca_account: &AlpacaAccountNumber` parameter to
      `call_redeem_endpoint`
- [ ] Add `alpaca_account: &AlpacaAccountNumber` parameter to
      `poll_request_status`
- [ ] Remove `account_id` field from `RealAlpacaService` struct
- [ ] Remove `ALPACA_ACCOUNT_ID` from `AlpacaConfig`
- [ ] Update `AlpacaService` trait signatures
- [ ] Update `MockAlpacaService` to match new signatures
- [ ] Update all tests for new signatures

### Verification

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [ ] `cargo fmt --all -- --check` passes

## Task 2. Update CallbackManager to look up user's Alpaca account

The `CallbackManager` needs to look up the user's Alpaca account from the
`AccountView` using the `client_id` stored in the mint aggregate.

### Changes Required

- [ ] Add `pool: Pool<Sqlite>` to `CallbackManager` constructor
- [ ] In `handle_tokens_minted`, look up `AccountView` by `client_id`
- [ ] Extract `alpaca_account` from `AccountView::LinkedToAlpaca`
- [ ] Pass `alpaca_account` to `alpaca_service.send_mint_callback()`
- [ ] Add error handling for account not found / not linked
- [ ] Update tests with mock account lookups
- [ ] Update wiring in `lib.rs` to pass pool to `CallbackManager`

### Verification

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [ ] `cargo fmt --all -- --check` passes

## Task 3. Update RedeemCallManager to look up user's Alpaca account

Same pattern as Task 2, but for the redemption flow.

### Changes Required

- [ ] Add `pool: Pool<Sqlite>` to `RedeemCallManager` constructor (if not
      already present)
- [ ] In the redeem call handler, look up `AccountView` by `client_id`
- [ ] Extract `alpaca_account` from `AccountView::LinkedToAlpaca`
- [ ] Pass `alpaca_account` to `alpaca_service.call_redeem_endpoint()`
- [ ] Update tests
- [ ] Update wiring in `lib.rs`

### Verification

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [ ] `cargo fmt --all -- --check` passes

## Task 4. Update JournalManager to look up user's Alpaca account

Same pattern for `poll_request_status`.

### Changes Required

- [ ] Add `pool: Pool<Sqlite>` to `JournalManager` constructor (if not already
      present)
- [ ] Look up `AccountView` by `client_id` before polling
- [ ] Pass `alpaca_account` to `alpaca_service.poll_request_status()`
- [ ] Update tests
- [ ] Update wiring in `lib.rs`

### Verification

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [ ] `cargo fmt --all -- --check` passes

## Task 5. Add startup recovery for stuck mints

When the bot starts, check for mints in recoverable states and process them.

### Changes Required

- [ ] Create `src/mint/recovery.rs` with `MintRecoveryService`
- [ ] Add `recover_stuck_mints()` method that:
  - Queries `mint_view` for mints in `JournalConfirmed` state
  - Queries `mint_view` for mints in `CallbackPending` state
  - For each `JournalConfirmed` mint, calls
    `MintManager::handle_journal_confirmed`
  - For each `CallbackPending` mint, calls
    `CallbackManager::handle_tokens_minted`
- [ ] Add structured logging for recovery attempts and outcomes
- [ ] Wire recovery service into startup sequence in `lib.rs` (run after Rocket
      launches)
- [ ] Add tests for recovery logic

### Design Notes

- Recovery is idempotent - safe to run multiple times
- Failed recovery attempts log errors but don't crash the bot
- Recovery runs sequentially with delays between attempts to avoid overwhelming
  services

### Verification

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
      passes
- [ ] `cargo fmt --all -- --check` passes

## Task 6. Add startup recovery for stuck redemptions

Same pattern as Task 5, but for redemptions.

### Changes Required

- [ ] Create `src/redemption/recovery.rs` with `RedemptionRecoveryService`
- [ ] Add `recover_stuck_redemptions()` method that:
  - Queries `redemption_view` for redemptions in `Detected` state → retry
    calling Alpaca
  - Queries `redemption_view` for redemptions in `AlpacaCalled` state → retry
    polling
  - Queries `redemption_view` for redemptions in `Burning` state → retry burn
    transaction
- [ ] Add structured logging for recovery attempts and outcomes
- [ ] Wire recovery service into startup sequence in `lib.rs`
- [ ] Add tests for recovery logic

### Design Notes

- Same idempotency and error handling approach as mint recovery
- Recovery requires access to: `RedeemCallManager`, `JournalManager`,
  `BurnManager`

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
