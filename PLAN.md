# Migration Plan: Lifecycle Pattern for Aggregates

## Overview

This plan migrates all aggregates in `st0x.issuance-b` to use the
`Lifecycle<T, E>` pattern from `st0x.liquidity-b`.

**Benefits:**

- Panic-free error handling in `apply()` - errors go to `Failed` state instead
  of silent early returns
- Clean separation: domain logic (`T`) vs infrastructure concerns (lifecycle
  tracking)
- Explicit state: `Lifecycle::Uninitialized` vs `Lifecycle::Live(T)` vs
  `Lifecycle::Failed`
- Reduced field duplication via struct + state enum pattern

## Current vs Target Pattern

**Current:**

```rust
enum Mint {
    Uninitialized,
    Initiated { /* all fields */ },
    JournalConfirmed { /* all fields duplicated + new */ },
}

impl Aggregate for Mint {
    fn apply(&mut self, event: Self::Event) {
        match event {
            MintEvent::JournalConfirmed { .. } => {
                let Self::Initiated { .. } = self.clone() else { return }; // silent failure
            }
        }
    }
}
```

**Target:**

```rust
struct Mint {
    request: MintRequest,  // immutable request data
    state: MintState,      // state-specific data
}

impl Aggregate for Lifecycle<Mint, Never> {
    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, Mint::apply_transition)
            .or_initialize(&event, Mint::from_event);
    }
}
```

## Design Decisions

### Error Type: `Never`

All current aggregates have no fallible arithmetic operations. We use `Never`
(an uninhabited enum) for the error type parameter, meaning
`LifecycleError::Custom(Never)` is unreachable.

### View Updates

Views implement `View<A>` where `A: Aggregate`. Changing from
`impl Aggregate for Mint` to `impl Aggregate for Lifecycle<Mint, Never>`
requires updating view trait bounds accordingly. The `ReceiptInventoryView` is a
cross-aggregate view implementing both `View<Mint>` and `View<Redemption>`.

---

## Task 1. Add Lifecycle Module

Copy the lifecycle infrastructure from `st0x.liquidity-b`.

### Subtasks

- [ ] Copy `src/lifecycle.rs` from `st0x.liquidity-b/src/lifecycle.rs`
- [ ] Add `mod lifecycle;` to `src/lib.rs`
- [ ] Re-export `Lifecycle`, `LifecycleError`, `Never` as `pub(crate)`
- [ ] Run
      `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings && cargo fmt`

---

## Task 2. Migrate TokenizedAsset Aggregate

TokenizedAsset is the simplest aggregate with only 2 states (`NotAdded`,
`Added`) and no downstream dependencies beyond its own view.

### Target Structure

```rust
struct TokenizedAsset {
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    vault: Address,
    enabled: bool,
    added_at: DateTime<Utc>,
}
// Single live state - no state enum needed
```

### Subtasks

- [ ] Create `TokenizedAsset` struct (replaces enum)
- [ ] Implement `TokenizedAsset::from_event()` for `Added` event
- [ ] Implement `TokenizedAsset::apply_transition()` (returns `Mismatch` for all
      events since no transitions exist)
- [ ] Change `impl Aggregate for TokenizedAsset` to
      `impl Aggregate for Lifecycle<TokenizedAsset, Never>`
- [ ] Update `handle()` to use `self.live()` for state checking
- [ ] Remove `TokenizedAssetView` enum - `Lifecycle<TokenizedAsset, Never>`
      implements `View<Lifecycle<TokenizedAsset, Never>>` instead
- [ ] Update `list_enabled_assets` query to work with
      `Lifecycle<TokenizedAsset, Never>` payload
- [ ] Update tests to use `TestFramework<Lifecycle<TokenizedAsset, Never>>`
- [ ] Run
      `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings && cargo fmt`

---

## Task 3. Migrate Account Aggregate

Account has 3 states (`NotRegistered`, `Registered`, `LinkedToAlpaca`) with
moderate field duplication.

### Target Structure

```rust
struct Account {
    client_id: ClientId,
    email: Email,
    registered_at: DateTime<Utc>,
    state: AccountState,
}

enum AccountState {
    Registered,
    LinkedToAlpaca {
        alpaca_account: AlpacaAccountNumber,
        whitelisted_wallets: Vec<Address>,
        linked_at: DateTime<Utc>,
    },
}
```

### Subtasks

- [ ] Create `Account` struct and `AccountState` enum
- [ ] Implement `Account::from_event()` for `Registered` event
- [ ] Implement `Account::apply_transition()` for `LinkedToAlpaca` and
      `WalletWhitelisted` events
- [ ] Change `impl Aggregate for Account` to
      `impl Aggregate for Lifecycle<Account, Never>`
- [ ] Update `handle()` to use `self.live()` for state checking
- [ ] Remove `AccountView` enum - `Lifecycle<Account, Never>` implements
      `View<Lifecycle<Account, Never>>` instead
- [ ] Update account queries to work with `Lifecycle<Account, Never>` payload
- [ ] Update tests to use `TestFramework<Lifecycle<Account, Never>>`
- [ ] Run
      `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings && cargo fmt`

---

## Task 4. Migrate Redemption Aggregate

Redemption has 6 states and already uses `RedemptionMetadata` to reduce
duplication. The `ReceiptInventoryView` depends on `Redemption` events.

### Target Structure

```rust
struct Redemption {
    metadata: RedemptionMetadata,  // keep existing struct
    state: RedemptionState,
}

enum RedemptionState {
    Detected,
    AlpacaCalled {
        tokenization_request_id: TokenizationRequestId,
        called_at: DateTime<Utc>,
    },
    Burning {
        tokenization_request_id: TokenizationRequestId,
        called_at: DateTime<Utc>,
        alpaca_journal_completed_at: DateTime<Utc>,
    },
    Completed {
        burn_tx_hash: B256,
        completed_at: DateTime<Utc>,
    },
    Failed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
}
```

### Subtasks

- [ ] Create `Redemption` struct and `RedemptionState` enum
- [ ] Implement `Redemption::from_event()` for `Detected` event
- [ ] Implement `Redemption::apply_transition()` for all transition events
- [ ] Change `impl Aggregate for Redemption` to
      `impl Aggregate for Lifecycle<Redemption, Never>`
- [ ] Update `handle()` to use `self.live()` for state checking
- [ ] Remove `RedemptionView` enum - `Lifecycle<Redemption, Never>` implements
      `View<Lifecycle<Redemption, Never>>` instead
- [ ] Update redemption queries to work with `Lifecycle<Redemption, Never>`
      payload
- [ ] Update managers (`burn_manager`, `detector`, `journal_manager`,
      `redeem_call_manager`) to work with `Lifecycle<Redemption, Never>`
- [ ] Update tests to use `TestFramework<Lifecycle<Redemption, Never>>`
- [ ] Run
      `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings && cargo fmt`

---

## Task 5. Migrate Mint Aggregate

Mint is the most complex aggregate with 7 states and heavy field duplication.
The `ReceiptInventoryView` and `MintView` depend on `Mint` events.

### Target Structure

```rust
struct MintRequest {
    issuer_request_id: IssuerRequestId,
    tokenization_request_id: TokenizationRequestId,
    quantity: Quantity,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    client_id: ClientId,
    wallet: Address,
    initiated_at: DateTime<Utc>,
}

struct Mint {
    request: MintRequest,
    state: MintState,
}

enum MintState {
    Initiated,
    JournalConfirmed {
        journal_confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    CallbackPending {
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        journal_confirmed_at: DateTime<Utc>,
        error: String,
        failed_at: DateTime<Utc>,
    },
    Completed {
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
}
```

### Subtasks

- [ ] Create `MintRequest` struct for immutable request data
- [ ] Create `Mint` struct and `MintState` enum
- [ ] Implement `Mint::from_event()` for `Initiated` event
- [ ] Implement `Mint::apply_transition()` for all transition events
- [ ] Change `impl Aggregate for Mint` to
      `impl Aggregate for Lifecycle<Mint, Never>`
- [ ] Update `handle()` to use `self.live()` for state checking
- [ ] Remove `MintView` enum - `Lifecycle<Mint, Never>` implements
      `View<Lifecycle<Mint, Never>>` instead
- [ ] Update mint queries to work with `Lifecycle<Mint, Never>` payload
- [ ] Update managers (`mint_manager`, `callback_manager`) to work with
      `Lifecycle<Mint, Never>`
- [ ] Update tests to use `TestFramework<Lifecycle<Mint, Never>>`
- [ ] Run
      `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings && cargo fmt`

---

## Task 6. Update ReceiptInventoryView

`ReceiptInventoryView` is a cross-aggregate view that implements `View<Mint>`
and `View<Redemption>` to track receipt lifecycle across both aggregates. Update
it to work with the new lifecycle-wrapped aggregate types.

### Subtasks

- [ ] Update `impl View<Mint> for ReceiptInventoryView` to
      `impl View<Lifecycle<Mint, Never>> for ReceiptInventoryView`
- [ ] Update `impl View<Redemption> for ReceiptInventoryView` to
      `impl View<Lifecycle<Redemption, Never>> for ReceiptInventoryView`
- [ ] Update `ReceiptInventoryMintQuery` and `ReceiptInventoryRedemptionQuery`
      in tests
- [ ] Run
      `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings && cargo fmt`

---

## Task 7. Update CQRS Wiring and Integration Points

Update the application wiring that creates aggregate instances and CQRS
frameworks.

### Subtasks

- [ ] Update `src/lib.rs` aggregate re-exports if needed
- [ ] Update any `CqrsFramework<Mint, ...>` to
      `CqrsFramework<Lifecycle<Mint, Never>, ...>`
- [ ] Update any `CqrsFramework<Redemption, ...>` to
      `CqrsFramework<Lifecycle<Redemption, Never>, ...>`
- [ ] Update any `CqrsFramework<Account, ...>` to
      `CqrsFramework<Lifecycle<Account, Never>, ...>`
- [ ] Update any `CqrsFramework<TokenizedAsset, ...>` to
      `CqrsFramework<Lifecycle<TokenizedAsset, Never>, ...>`
- [ ] Update API endpoint handlers that load aggregates
- [ ] Run full test suite including any E2E tests
- [ ] Run
      `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings && cargo fmt`
