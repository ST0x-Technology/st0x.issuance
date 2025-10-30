# Implementation Plan for Issue #25: Token Burning Feature

This plan implements the token burning functionality to complete the redemption
flow. The burning process occurs after Alpaca confirms the journal transfer,
allowing us to burn the tokenized shares on-chain via the vault's `withdraw()`
function.

## Overview

The burning feature is the final step in the redemption flow:

1. Tokens are detected when sent to redemption wallet (already implemented)
2. Alpaca API is called to initiate journal transfer (already implemented - #22)
3. Journal completion is polled and confirmed (implemented - #23)
4. **Tokens are burned on-chain** (this implementation)
5. End-to-end tests verify complete flow (future - #26)

## Key Design Decisions

### Receipt Selection Strategy

When burning tokens, we need to select which ERC-1155 receipt to burn from. The
`ReceiptInventoryView` already provides query methods for this purpose:

- **Primary strategy**: Use `find_receipt_with_balance()` to select the receipt
  with the highest balance that meets minimum requirements
- **Rationale**: This minimizes fragmentation and keeps receipts consolidated
- The view tracks `current_balance` which decrements as burns occur against each
  receipt

### Burn Event Structure

Following the existing pattern from `TokensMinted`, we'll add `receipt_id` and
`shares_burned` to the burn event. This allows:

- **Receipt tracking**: The `ReceiptInventoryView` can decrement the correct
  receipt's balance
- **Audit trail**: Complete on-chain to off-chain correlation
- **Depleted detection**: When `current_balance` reaches zero, transition
  receipt to `Depleted` state

### BurningStarted Event

Following the Mint flow pattern where `ConfirmJournal` emits both
`JournalConfirmed` and `MintingStarted` events, we need to ensure
`ConfirmAlpacaComplete` emits both `AlpacaJournalCompleted` and `BurningStarted`
events. This allows the `BurnManager` to react to the `BurningStarted` event to
orchestrate burning.

### Error Handling Philosophy

The burning process mirrors the minting approach:

- **Blockchain failures**: Record via `RecordBurnFailure` command, transition to
  `Failed` state
- **No retries in aggregate**: Manager layer can implement retry logic if needed
- **Receipt selection failures**: These should be rare (insufficient balance),
  recorded as burn failures

### State Transitions

The `Redemption::Burning` state indicates that:

- Alpaca has completed the journal transfer (shares are back in our custodian
  account)
- We now need to burn the corresponding on-chain tokens
- On success → `Completed` (final state)
- On failure → `Failed` (terminal error state)

## Task 1. Add Commands and Events

Add all new commands and events needed for the burning feature.

**Reasoning**: Commands and events form the API contract for the aggregate.
Adding them together ensures consistency and allows Task 2 to implement both
command handling and event application in one cohesive step.

**Implementation**:

- [ ] Add `RecordBurnSuccess` variant to `RedemptionCommand` enum in
      `src/redemption/cmd.rs`
  - Fields: `issuer_request_id: IssuerRequestId`, `tx_hash: B256`,
    `receipt_id: U256`, `shares_burned: U256`, `gas_used: u64`,
    `block_number: u64`
  - Similar structure to `RecordMintSuccess` for consistency
- [ ] Add `RecordBurnFailure` variant to `RedemptionCommand` enum in
      `src/redemption/cmd.rs`
  - Fields: `issuer_request_id: IssuerRequestId`, `error: String`
  - Similar structure to `RecordMintFailure`
- [ ] Add `BurningStarted` variant to `RedemptionEvent` enum in
      `src/redemption/event.rs`
  - Fields: `issuer_request_id: IssuerRequestId`,
    `burning_started_at: DateTime<Utc>`
  - Signals that burning should begin (emitted alongside
    `AlpacaJournalCompleted`)
- [ ] Add `TokensBurned` variant to `RedemptionEvent` enum in
      `src/redemption/event.rs`
  - Fields: `issuer_request_id: IssuerRequestId`, `tx_hash: B256`,
    `receipt_id: U256`, `shares_burned: U256`, `gas_used: u64`,
    `block_number: u64`, `burned_at: DateTime<Utc>`
  - Mirrors `MintEvent::TokensMinted` structure for consistency
- [ ] Add `BurningFailed` variant to `RedemptionEvent` enum in
      `src/redemption/event.rs`
  - Fields: `issuer_request_id: IssuerRequestId`, `error: String`,
    `failed_at: DateTime<Utc>`
  - Records blockchain burning failure
- [ ] Add `RedemptionCompleted` variant to `RedemptionEvent` enum in
      `src/redemption/event.rs`
  - Fields: `issuer_request_id: IssuerRequestId`, `completed_at: DateTime<Utc>`
  - Marks successful end of redemption flow
- [ ] Implement `DomainEvent` trait for new event variants in
      `src/redemption/event.rs`
  - Add event types: "RedemptionEvent::BurningStarted",
    "RedemptionEvent::TokensBurned", "RedemptionEvent::BurningFailed",
    "RedemptionEvent::RedemptionCompleted"
  - All events use version "1.0"
- [ ] Add unit tests for event serialization in `src/redemption/event.rs`
  - Test each new event can be serialized and deserialized
  - Test `event_type()` returns correct strings
  - Test `event_version()` returns "1.0"

**Files to modify**:

- `src/redemption/cmd.rs` - Add command variants
- `src/redemption/event.rs` - Add event variants, DomainEvent implementation,
  and tests

## Task 2. Extend Redemption Aggregate with Burn Logic and Tests

Update the `Redemption` aggregate to handle burning commands/events, add the
`Completed` state, and verify with comprehensive tests.

**Reasoning**: The aggregate is the authoritative source for business rules. It
validates state transitions and ensures only valid command sequences are
allowed. Tests are included in this task to ensure all checks pass at the end of
the task.

**Implementation**:

- [ ] Add `Completed` state to `Redemption` enum in `src/redemption/mod.rs`
  - Fields: `issuer_request_id: IssuerRequestId`, `completed_at: DateTime<Utc>`,
    and any other relevant fields from prior states that should be preserved
- [ ] Update `Redemption::state_name()` to handle `Completed` state
- [ ] Verify `ConfirmAlpacaComplete` command emits `BurningStarted` event
  - Check if current implementation emits both `AlpacaJournalCompleted` and
    `BurningStarted`
  - If not, update to emit both events (following `ConfirmJournal` pattern from
    Mint)
- [ ] Update `Redemption::handle()` to process `RecordBurnSuccess` command
  - Validate current state is `Burning`
  - Return `InvalidState` error if in wrong state
  - Emit `TokensBurned` event followed by `RedemptionCompleted` event (two
    events from one command)
  - Capture `Utc::now()` for timestamp fields
- [ ] Update `Redemption::handle()` to process `RecordBurnFailure` command
  - Validate current state is `Burning`
  - Return `InvalidState` error if in wrong state
  - Emit `BurningFailed` event
- [ ] Update `Redemption::apply()` to handle `BurningStarted` event
  - No state change needed (just a signal event)
  - Or transition from `AlpacaCalled` to `Burning` if this isn't already handled
    by `AlpacaJournalCompleted`
- [ ] Update `Redemption::apply()` to handle `TokensBurned` event
  - Store burn transaction details if needed
  - Remain in `Burning` state until `RedemptionCompleted` applies
- [ ] Update `Redemption::apply()` to handle `RedemptionCompleted` event
  - Transition from `Burning` to `Completed` with relevant fields
- [ ] Update `Redemption::apply()` to handle `BurningFailed` event
  - Transition to `Failed` state with error details
- [ ] Add comprehensive unit tests in `src/redemption/mod.rs`
  - Test `RecordBurnSuccess` from `Burning` state produces `TokensBurned` +
    `RedemptionCompleted` events
  - Test `RecordBurnSuccess` from wrong state returns `InvalidState` error
  - Test `RecordBurnFailure` from `Burning` state produces `BurningFailed` event
  - Test `RecordBurnFailure` from wrong state returns `InvalidState` error
  - Test `apply(TokensBurned)` correctly updates state
  - Test `apply(RedemptionCompleted)` transitions to `Completed` state
  - Test `apply(BurningFailed)` transitions to `Failed` state
  - Test complete redemption flow: `Detected` → `AlpacaCalled` →
    `AlpacaJournalCompleted` + `BurningStarted` → `TokensBurned` →
    `RedemptionCompleted`

**Files to modify**:

- `src/redemption/mod.rs` - Add `Completed` state, update `handle()` and
  `apply()`, add tests

## Task 3. Extend VaultService with burn_tokens() Method and Tests

Add the `burn_tokens()` method to the `VaultService` trait and implement it in
the real service and mock, with tests.

**Reasoning**: The vault service abstracts blockchain interactions. Adding
`burn_tokens()` mirrors the existing `mint_tokens()` pattern and enables both
real on-chain burns and mock burns for testing. Tests are included to ensure the
mock works correctly for downstream manager tests.

**Implementation**:

- [ ] Define `BurnResult` struct in `src/vault/mod.rs`
  - Fields: `tx_hash: B256`, `receipt_id: U256`, `shares_burned: U256`,
    `gas_used: u64`, `block_number: u64`
  - Place next to `MintResult` for consistency
  - Derive `Debug`, `Clone`, `PartialEq`, `Serialize`, `Deserialize`
- [ ] Add `burn_tokens()` method to `VaultService` trait in `src/vault/mod.rs`
  - Parameters: `shares: U256`, `receipt_id: U256`, `receiver: Address`,
    `receipt_info: ReceiptInformation`
  - Returns: `Result<BurnResult, VaultError>`
  - Document that this calls the vault's `withdraw()` function
- [ ] Implement `burn_tokens()` in real vault service (`src/vault/service.rs`)
  - Call vault contract's `withdraw()` function with correct parameters
  - Parse `Withdraw` event from transaction receipt to extract shares burned
  - Handle transaction failures and missing events gracefully
  - Return `BurnResult` on success
- [ ] Add success and failure modes to `MockVaultService` in `src/vault/mock.rs`
  - Implement `burn_tokens()` method that returns success or failure based on
    mock configuration
  - Track call count for test verification (add `burn_call_count` field)
  - Return deterministic test data (tx hash, receipt ID, shares, etc.)
- [ ] Add unit tests for mock behavior in `src/vault/mock.rs`
  - Test mock success mode returns expected `BurnResult`
  - Test mock failure mode returns `VaultError`
  - Test call count tracking works correctly

**Files to modify**:

- `src/vault/mod.rs` - Add `burn_tokens()` to trait, add `BurnResult` struct
- `src/vault/service.rs` - Implement real blockchain burning
- `src/vault/mock.rs` - Implement mock burning and add tests

## Task 4. Update ReceiptInventoryView to Handle Burn Events and Tests

Extend the `ReceiptInventoryView` to process `TokensBurned` events, track
balance decrements, and remove dead code annotations.

**Reasoning**: The view must reflect current receipt balances after burns. When
a receipt is fully burned (balance = 0), it should transition to `Depleted`
state for accurate inventory tracking. Tests verify the view correctly tracks
burns.

**Implementation**:

- [ ] Add helper method `with_tokens_burned` to `ReceiptInventoryView` in
      `src/receipt_inventory/view.rs`
  - Signature:
    `fn with_tokens_burned(self, receipt_id: U256, shares_burned: U256, burned_at: DateTime<Utc>) -> Self`
  - Match on `Self::Active` and check if `receipt_id` matches
  - Decrement `current_balance` by `shares_burned`
  - If `current_balance` reaches zero, transition to `Depleted` state with
    `depleted_at = burned_at`
  - If still has balance, remain in `Active` state with updated
    `current_balance`
  - For all other states/mismatches, return `self` unchanged
- [ ] Implement `View<Redemption>::update()` in `src/receipt_inventory/view.rs`
  - Handle `TokensBurned` event by calling `with_tokens_burned`
  - Extract `receipt_id`, `shares_burned`, and `burned_at` from event
  - All other redemption events leave view unchanged
- [ ] Remove `#[allow(dead_code)]` annotations from query methods in
      `src/receipt_inventory/view.rs`
  - Remove from `get_receipt()`
  - Remove from `list_active_receipts()`
  - Remove from `find_receipt_with_balance()`
  - Remove from `get_total_balance()`
- [ ] Add comprehensive unit tests in `src/receipt_inventory/view.rs`
  - Test `Active` receipt balance decrements correctly on `TokensBurned`
  - Test `Active` → `Depleted` transition when balance reaches zero
  - Test partial burns keep receipt `Active` with updated balance
  - Test burning from wrong receipt ID leaves view unchanged
  - Test burning when in `Pending` or `Unavailable` state leaves view unchanged
  - Test `View<Redemption>::update()` correctly processes `TokensBurned` events
  - Test other redemption events leave view unchanged
- [ ] Add database integration test in `src/receipt_inventory/view.rs`
  - Create in-memory database, seed with `Active` receipt
  - Apply `TokensBurned` event through view update
  - Verify persisted view has updated balance

**Files to modify**:

- `src/receipt_inventory/view.rs` - Implement burn event handling, remove
  dead_code annotations, add tests

## Task 5. Implement BurnManager Orchestrator with Tests

Create a manager to orchestrate the burning process, following the same pattern
as `MintManager`, with comprehensive tests.

**Reasoning**: Managers bridge ES/CQRS aggregates with external services. The
`BurnManager` reacts to `BurningStarted` events by selecting a receipt, calling
the blockchain service, and recording results. Tests are included to verify
orchestration logic.

**Implementation**:

- [ ] Create `src/redemption/burn_manager.rs` module
  - Add `pub(crate) mod burn_manager;` to `src/redemption/mod.rs`
- [ ] Define `BurnManagerError` enum in `burn_manager.rs`
  - Variants: `Blockchain(VaultError)`, `Cqrs(AggregateError<RedemptionError>)`,
    `InvalidAggregateState { current_state: String }`,
    `QuantityConversion(QuantityConversionError)`,
    `InsufficientBalance { required: U256, available: U256 }`,
    `Database(sqlx::Error)`
  - Derive `Debug`, `thiserror::Error`
  - Implement `From` traits for automatic error conversion where applicable
- [ ] Define `BurnManager` struct in `burn_manager.rs`
  - Fields: `blockchain_service: Arc<dyn VaultService>`,
    `receipt_query_pool: Pool<Sqlite>`,
    `cqrs: Arc<CqrsFramework<Redemption, ES>>`
  - Generic over `ES: EventStore<Redemption>` for flexibility
- [ ] Implement `BurnManager::new()` constructor
  - Accept blockchain service, database pool (for receipt queries), and CQRS
    framework
  - Return `Self`
- [ ] Implement `BurnManager::handle_burning_started()` method
  - Accept `issuer_request_id: &IssuerRequestId` and `aggregate: &Redemption`
  - Validate aggregate is in `Burning` state, return `InvalidAggregateState`
    error if not
  - Extract `underlying`, `quantity`, and `tokenization_request_id` from
    aggregate
  - Convert `quantity` to U256 shares (18 decimals) using
    `to_u256_with_18_decimals()`
  - Query `ReceiptInventoryView` using `find_receipt_with_balance()` to select
    receipt
  - If no receipt found with sufficient balance → execute `RecordBurnFailure`
    command and return error
  - Build `ReceiptInformation` with operation type `OperationType::Redeem`
  - Call `blockchain_service.burn_tokens()` with selected receipt
  - On success → execute `RecordBurnSuccess` command with transaction details
  - On failure → execute `RecordBurnFailure` command with error message
  - Add logging for start, success, and failure cases
- [ ] Add unit tests in `burn_manager.rs`
  - Test successful burn orchestration with mock vault service and in-memory
    CQRS
  - Test blockchain failure flow (error recorded in aggregate)
  - Test insufficient balance scenario
  - Test wrong aggregate state error
  - Test receipt selection logic with multiple receipts

**Files to create**:

- `src/redemption/burn_manager.rs`

**Files to modify**:

- `src/redemption/mod.rs` - Add `pub(crate) mod burn_manager;` and re-export
  `BurnManager`

## Task 6. Wire BurnManager into Application Orchestration

Integrate the `BurnManager` into the application's event processing pipeline to
automatically burn tokens when redemptions enter the `Burning` state.

**Reasoning**: The manager must react to `BurningStarted` events to execute
burns. This requires wiring into the CQRS query/view infrastructure and
potentially the event listener that processes redemption events.

**Implementation**:

- [ ] Examine existing manager wiring
  - Locate where `MintManager` is instantiated (likely in `main.rs` or app
    setup)
  - Understand the event listening mechanism used for minting
- [ ] Create `BurnManager` instance during application startup
  - Pass `vault_service`, `sqlite_pool`, and `redemption_cqrs` as dependencies
  - Store in application state or service container
- [ ] Wire manager to listen for `BurningStarted` events
  - Option A: Add to existing redemption event processor if one exists
  - Option B: Create new event processor specifically for burning
  - Option C: Implement as a query that triggers on `BurningStarted` event
  - Follow the same pattern used for `MintManager` orchestration
- [ ] Implement event processing logic
  - When `BurningStarted` event is received
  - Load current `Redemption` aggregate from event store
  - Call `burn_manager.handle_burning_started()` with aggregate
  - Log success/failure appropriately
- [ ] Add error handling and logging
  - Log when burn orchestration starts
  - Log when burn succeeds (with tx hash, receipt ID, shares)
  - Log when burn fails (with error details)
  - Handle transient errors gracefully (e.g., RPC timeouts)

**Note**: The exact wiring approach depends on the existing application
structure. This task may require exploration and adjustment based on how
`MintManager` is integrated.

**Files to modify**:

- `src/main.rs` (or wherever app initialization happens)
- Potentially create new event processor/query module if needed

## Task 7. Add Integration Tests for Complete Burn Flow

Add integration tests that exercise the complete burn flow with real database
and mock blockchain.

**Reasoning**: Integration tests verify that the burning feature works correctly
when all components are wired together, catching issues that unit tests might
miss.

**Implementation**:

- [ ] Add integration test for complete redemption with burn (happy path)
  - Setup: Initialize in-memory SQLite, run migrations
  - Create redemption aggregate in `Burning` state
  - Seed `ReceiptInventoryView` with active receipt (sufficient balance)
  - Execute: Call `burn_manager.handle_burning_started()`
  - Verify: Burn succeeds, aggregate in `Completed` state, receipt balance
    decremented in database view
- [ ] Add integration test for partial burn
  - Setup: Create receipt with balance larger than burn amount
  - Execute: Burn portion of shares
  - Verify: Receipt remains `Active` with correct updated balance
- [ ] Add integration test for burn that depletes receipt
  - Setup: Create receipt with exact balance needed for burn
  - Execute: Burn all shares
  - Verify: Receipt transitions to `Depleted` state
- [ ] Add integration test for burn with multiple receipts
  - Setup: Create multiple receipts with different balances
  - Execute: Burn amount requiring selection
  - Verify: Correct receipt selected (highest balance), others unchanged
- [ ] Add integration test for insufficient balance scenario
  - Setup: Create receipts with insufficient total balance
  - Execute: Attempt burn
  - Verify: Burn fails, aggregate in `Failed` state with appropriate error

**Files to create/modify**:

- `src/redemption/burn_manager.rs` - Add integration tests in
  `#[cfg(test)] mod integration_tests` section

## Task 8. Run All Quality Checks

Ensure code quality and correctness before submitting.

**Reasoning**: Running tests, linting, and formatting is critical to maintain
code quality and catch issues before code review. This ensures all checks pass
and the feature is complete.

**Implementation**:

- [ ] Run all tests: `cargo test --workspace`
  - Verify all new and existing tests pass
  - Fix any failing tests
- [ ] Run clippy:
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  - Address all clippy warnings
  - Refactor code if needed to satisfy lints (no `#[allow]` without permission)
- [ ] Run formatter: `cargo fmt`
  - Ensure consistent code formatting
- [ ] Verify no dead code warnings
  - Confirm all `#[allow(dead_code)]` annotations removed from receipt inventory
    view
  - Verify compiler reports no dead code warnings
  - Check that all new code is being used (commands, events, manager methods)
