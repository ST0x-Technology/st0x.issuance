# Implementation Plan: Issue #22 - Implement Alpaca Redeem Call Feature

## Overview

This task implements the Alpaca API integration for the redemption flow. When a
redemption is detected (via `RedemptionDetected` event), we need to call
Alpaca's redeem endpoint to initiate the journal transfer of shares from our
account to the AP's account. This is Step 2 in the redemption flow outlined in
SPEC.md.

## Task 1. Add AlpacaCalled, AlpacaCallFailed, and RedemptionFailed events

- [ ] Add `AlpacaCalled` event variant to `RedemptionEvent` enum in
      `src/redemption/event.rs`
  - Fields: `issuer_request_id`, `tokenization_request_id`,
    `called_at: DateTime<Utc>`
- [ ] Add `AlpacaCallFailed` event variant to `RedemptionEvent` enum
  - Fields: `issuer_request_id`, `error: String`, `failed_at: DateTime<Utc>`
- [ ] Add `RedemptionFailed` event variant to `RedemptionEvent` enum
  - Fields: `issuer_request_id`, `reason: String`, `failed_at: DateTime<Utc>`
  - Note: This is a general failure event for any redemption failures beyond
    AlpacaCallFailed
- [ ] Update `DomainEvent` impl to return correct event_type strings for all new
      events
- [ ] Add new aggregate states to `Redemption` enum in `src/redemption/mod.rs`:
  - `AlpacaCalled { issuer_request_id, tokenization_request_id, underlying, token, wallet, quantity, detected_tx_hash, block_number, detected_at, called_at }`
  - `Failed { issuer_request_id, reason, failed_at }`
- [ ] Update `Redemption::apply()` to handle `AlpacaCalled` event
  - Transition aggregate to `AlpacaCalled` state with tokenization_request_id
    and timestamp
- [ ] Update `Redemption::apply()` to handle `AlpacaCallFailed` event
  - Transition aggregate to `Failed` state with error and timestamp
- [ ] Update `Redemption::apply()` to handle `RedemptionFailed` event
  - Transition aggregate to `Failed` state with reason and timestamp

## Task 2. Add RecordAlpacaCall and RecordAlpacaFailure commands to Redemption aggregate

- [ ] Add `RecordAlpacaCall` command variant to `RedemptionCommand` enum in
      `src/redemption/cmd.rs`
  - Fields: `issuer_request_id`, `tokenization_request_id`
- [ ] Add `RecordAlpacaFailure` command variant to `RedemptionCommand` enum
  - Fields: `issuer_request_id`, `error: String`
- [ ] Add `RedemptionError::InvalidState` variant for state validation errors
- [ ] Update `Redemption::handle()` to process `RecordAlpacaCall` command
  - Validate aggregate is in `Detected` state
  - Return `AlpacaCalled` event with tokenization_request_id
- [ ] Update `Redemption::handle()` to process `RecordAlpacaFailure` command
  - Validate aggregate is in `Detected` state
  - Return `AlpacaCallFailed` event with error message
- [ ] Add aggregate tests for both new commands (happy path + error cases)

## Task 3. Extend AlpacaService with call_redeem_endpoint() method

- [ ] Add `RedeemRequest` struct to `src/alpaca/mod.rs`
  - Fields per SPEC.md: `issuer_request_id`, `underlying_symbol`,
    `token_symbol`, `client_id`, `qty`, `network`, `wallet_address`, `tx_hash`
  - Derive `Debug, Clone, Serialize`
  - Use serde field renaming for `underlying_symbol`, `token_symbol`,
    `wallet_address` (snake_case)
- [ ] Add `RedeemResponse` struct to `src/alpaca/mod.rs`
  - Fields per SPEC.md: `tokenization_request_id`, `issuer_request_id`,
    `created_at`, `request_type`, `status`, `underlying_symbol`, `token_symbol`,
    `qty`, `issuer`, `network`, `wallet_address`, `tx_hash`, `fees`
  - Derive `Debug, Clone, Deserialize`
  - Use serde field renaming for snake_case fields
- [ ] Add `RedeemRequestType` and `RedeemRequestStatus` enums per SPEC.md
- [ ] Add `Fees` newtype wrapper for decimal fees
- [ ] Add `call_redeem_endpoint()` method to `AlpacaService` trait
  - Signature:
    `async fn call_redeem_endpoint(&self, request: RedeemRequest) -> Result<RedeemResponse, AlpacaError>`
- [ ] Implement `call_redeem_endpoint()` in `RealAlpacaService`
  - URL: `POST /v1/accounts/{account_id}/tokenization/redeem`
  - Use basic auth (same as mint callback)
  - Parse JSON response to `RedeemResponse`
  - Apply same retry logic as mint callback (exponential backoff with jitter)
  - Handle auth errors (401/403), API errors (400/500), and HTTP errors
- [ ] Add comprehensive tests for `call_redeem_endpoint()`:
  - Success case (200 OK with valid response)
  - Unauthorized (401)
  - Forbidden (403)
  - API error (400/500)
  - Correct URL construction
  - Correct JSON serialization
  - Correct basic auth header

## Task 4. Extend MockAlpacaService for testing

- [ ] Add `call_redeem_endpoint()` implementation to `MockAlpacaService` in
      `src/alpaca/mock.rs`
  - Support both success and failure modes
  - Track call count
  - Return mock `RedeemResponse` with configurable tokenization_request_id
- [ ] Add tests for mock implementation

## Task 5. Create RedemptionManager orchestrator

- [ ] Create `src/redemption/alpaca_manager.rs` following the pattern from
      `MintManager`
- [ ] Implement `AlpacaManager` struct:
  - Fields: `alpaca_service: Arc<dyn AlpacaService>`,
    `cqrs: Arc<CqrsFramework<Redemption, ES>>`
  - Constructor: `new(alpaca_service, cqrs) -> Self`
- [ ] Implement `handle_redemption_detected()` method:
  - Accept `issuer_request_id` and `aggregate: &Redemption` parameters
  - Validate aggregate is in `Detected` state
  - Extract redemption details (underlying, token, wallet, quantity, tx_hash)
  - Look up `client_id` from wallet address (note: this requires AccountView
    integration - will be addressed when we have the view)
  - For now, use a placeholder `ClientId` or accept it as a parameter
  - Construct `RedeemRequest` from aggregate data
  - Call `alpaca_service.call_redeem_endpoint(request)`
  - On success: execute `RecordAlpacaCall` command with returned
    `tokenization_request_id`
  - On failure: execute `RecordAlpacaFailure` command with error message
  - Return `Result<(), AlpacaManagerError>`
- [ ] Define `AlpacaManagerError` enum:
  - `Alpaca(AlpacaError)` - API call failed
  - `Cqrs(AggregateError<RedemptionError>)` - Command execution failed
  - `InvalidAggregateState { current_state: String }` - Wrong state for
    operation
- [ ] Add comprehensive tests using `MemStore` and `MockAlpacaService`:
  - Success path: Detected → call API → RecordAlpacaCall → AlpacaCalled state
  - Failure path: Detected → API failure → RecordAlpacaFailure → Failed state
  - Invalid state: Cannot call from non-Detected state
- [ ] Export `AlpacaManager` from `src/redemption/mod.rs`

## Task 6. Wire orchestrator to listen for RedemptionDetected events

- [ ] Add `alpaca_manager` module declaration in `src/redemption/mod.rs`
- [ ] Update application wiring in `src/main.rs` (or wherever CQRS framework is
      instantiated):
  - Create `AlpacaManager` instance with alpaca_service and redemption_cqrs
  - Register event listener for `RedemptionDetected` events
  - Event listener should: load aggregate → call
    `alpaca_manager.handle_redemption_detected()`
  - Handle errors appropriately (log and potentially retry)
- [ ] Note: Full integration will be completed in issue #26 (end-to-end tests),
      this task focuses on the manager itself

## Implementation Notes

### Event Flow

```
RedemptionDetected event
  → Listener detects event
  → Loads Redemption aggregate
  → Calls AlpacaManager.handle_redemption_detected()
  → Manager calls AlpacaService.call_redeem_endpoint()
  → On success: RecordAlpacaCall command → AlpacaCalled event
  → On failure: RecordAlpacaFailure command → AlpacaCallFailed event
```

### State Transitions

```
Detected → (RecordAlpacaCall) → AlpacaCalled
Detected → (RecordAlpacaFailure) → Failed
```

### Task Ordering Rationale

**Why events before commands?**

- Events are data structures that must exist before command handlers can produce
  them
- The `handle()` method returns events, so events must be defined first
- Aggregate states correspond to events, so they're defined together with events
- Commands are processed by `handle()` to produce events, so they come after

This ordering ensures:

1. All tests pass after each task
2. No forward dependencies (earlier tasks don't depend on later tasks)
3. Logical progression: data structures → handlers → external services →
   orchestration

### Design Rationale

**Why separate AlpacaCalled and AlpacaCallFailed events?**

- `AlpacaCalled` indicates successful API call - we now have a
  `tokenization_request_id` to track
- `AlpacaCallFailed` is a terminal failure state - the API call failed and we
  cannot proceed
- This mirrors the mint flow's `JournalConfirmed`/`JournalRejected` pattern

**Why RedemptionFailed event in addition to AlpacaCallFailed?**

- `AlpacaCallFailed` is specifically for Alpaca API call failures at this stage
- `RedemptionFailed` provides a general failure event for other failure
  scenarios (e.g., future polling failures, burn failures could use `MarkFailed`
  command)
- Having both allows us to track specific failure reasons while maintaining a
  consistent failure state

**Client ID Lookup:**

- The `client_id` is required by Alpaca's redeem endpoint
- We need to map wallet address → client_id
- This requires AccountView which may not be implemented yet
- For this task, we'll accept `client_id` as a parameter or use a placeholder
- Full integration will be addressed when AccountView is available

### Testing Strategy

- Unit tests: Test aggregate command handling and event application in isolation
- Manager tests: Test AlpacaManager with mock services and MemStore
- Integration tests: End-to-end tests will be covered in issue #26

### Files to Create/Modify

**New files:**

- `src/redemption/alpaca_manager.rs` - Orchestrator for Alpaca API calls

**Modified files:**

- `src/redemption/event.rs` - Add new events (Task 1)
- `src/redemption/mod.rs` - Add new aggregate states (Task 1), export manager
  (Task 5)
- `src/redemption/cmd.rs` - Add new commands (Task 2)
- `src/alpaca/mod.rs` - Add redeem request/response types and trait method
  (Task 3)
- `src/alpaca/service.rs` - Implement call_redeem_endpoint (Task 3)
- `src/alpaca/mock.rs` - Add mock implementation (Task 4)
- `src/main.rs` - Wire manager (Task 6)

### Dependencies

This task depends on:

- Issue #21 (DetectRedemption feature) must be complete - provides
  `RedemptionDetected` event
- AlpacaService infrastructure from mint flow (already implemented)

This task is required for:

- Issue #23 (Alpaca journal polling) - needs `AlpacaCalled` state with
  `tokenization_request_id`
- Issue #26 (End-to-end redemption tests) - needs complete Alpaca call feature
