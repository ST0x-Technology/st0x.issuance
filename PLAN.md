# Implementation Plan: Issue #23 - Implement Alpaca Journal Polling Feature

## Overview

This task implements the journal status polling mechanism for the redemption
flow. After we call Alpaca's redeem endpoint (AlpacaCalled event), we need to
poll Alpaca's request status endpoint until the journal transfer is completed,
then record the completion in our system.

## Context

**Current State (after #22):**

- Redemption aggregate has `Detected` and `AlpacaCalled` states
- RedeemManager handles `RedemptionDetected` → calls Alpaca redeem endpoint →
  produces `AlpacaCalled` event
- AlpacaService has `call_redeem_endpoint()` method

**What's Missing:**

- Polling mechanism to check when Alpaca journal completes
- `ConfirmAlpacaComplete` command and `AlpacaJournalCompleted` event
- Aggregate state transition from `AlpacaCalled` to `Burning`
- Manager to orchestrate the polling and command execution

**State Machine Flow:**

```
Detected → (RedeemManager) → AlpacaCalled → (JournalManager) → Burning → (BurnManager) → Completed
```

## Design Decisions

### 1. Aggregate State Design

The Redemption aggregate transitions directly from `AlpacaCalled` to `Burning`
state when Alpaca's journal completes.

**Rationale:** Following the pattern from Mint aggregate where journal
confirmation immediately leads to the next operational phase:

- Mint: `Initiated` → `JournalConfirmed` → (manager starts minting) →
  `CallbackPending` → `Completed`
- Redemption: `Detected` → `AlpacaCalled` → (manager polls and confirms) →
  `Burning` → (manager burns) → `Completed`

We don't need an intermediate `AlpacaCompleted` state because burning should
start immediately after Alpaca journal completes - there's no waiting period or
decision point.

### 2. Event Design

Following SPEC.md, the `ConfirmAlpacaComplete` command produces a **single
event**: `AlpacaJournalCompleted`.

**Rationale:** Consistent with the established pattern throughout the codebase:

- **All commands in SPEC.md produce single events** - there are no
  multiple-event commands
- **Events represent business facts**: `AlpacaJournalCompleted` is a fact
  (Alpaca confirmed the journal), while "burning started" is orchestration state
- **Manager pattern handles orchestration**: A burn manager listens for
  `AlpacaJournalCompleted` events and orchestrates the burning, just like
  `MintManager` listens for `JournalConfirmed` events
- **No decision point**: There's no business logic or validation between "Alpaca
  done" and "start burning" - burning should always start immediately

The aggregate transitions directly to `Burning` state when applying the
`AlpacaJournalCompleted` event, signaling that the redemption is ready for
on-chain burning.

### 3. AlpacaService Polling Method

Add `poll_request_status()` method that calls
`GET /v1/accounts/{account_id}/tokenization/requests` and filters for the
specific request.

**Rationale:** According to SPEC.md (line 946), this endpoint returns a list of
requests and we need to poll it to check status. The method will:

- Accept `tokenization_request_id` to identify which request to check
- Return the current status (Pending, Completed, Rejected)
- Handle parsing and filtering the list response

### 4. Journal Monitor Design

Create a new `JournalManager` that:

- Takes `AlpacaCalled` events as input
- Polls Alpaca at intervals (250ms initially, exponential backoff to 30s)
- Executes `ConfirmAlpacaComplete` when status becomes "completed"
- Has a timeout (1 hour) after which it marks the redemption as failed

**Rationale:** Follows the manager pattern established in the codebase:

- `MintManager`: Orchestrates blockchain minting in response to
  `JournalConfirmed` events
- `RedeemManager` (redemption): Orchestrates Alpaca API call in response to
  `RedemptionDetected` events
- `JournalManager` (new): Orchestrates journal polling in response to
  `AlpacaCalled` events

The manager keeps the aggregate pure (no async/polling logic in command
handlers) while enabling integration with external systems.

**Alternative Considered:** Make the RedeemManager responsible for both calling
and polling. Rejected because:

- Violates single responsibility principle
- Polling is conceptually different from making the initial API call
- Makes testing more complex
- Doesn't follow the granular manager pattern in the codebase

### 5. Polling Strategy

**Initial interval:** 250ms **Maximum interval:** 30 seconds **Backoff
strategy:** Exponential (250ms → 500ms → 1s → 2s → 4s → 8s → 16s → 30s → 30s...)
**Timeout:** 1 hour

**Rationale:**

- Start with 250ms for very quick completion - the entire mint/redemption flow
  should complete within seconds when everything works well
- Back off exponentially to avoid hammering Alpaca's API if the journal takes
  longer
- Cap at 30s to maintain reasonable responsiveness
- 1 hour timeout handles edge cases where Alpaca never completes

### 6. Wiring Pattern

The polling manager will be spawned as a background task when `AlpacaCalled`
events are emitted. This follows the async orchestration pattern used for
callback manager.

**Rationale:** Polling is a long-running operation that shouldn't block the main
event processing flow. Background tasks allow:

- Non-blocking event handling
- Independent retry/timeout logic
- Clean separation of concerns

## Task 1. Extend Redemption Aggregate with Journal Completion

**Subtasks:**

- [x] Add `Burning` state variant to `Redemption` enum with all necessary fields
- [x] Add `AlpacaJournalCompleted` event to `RedemptionEvent` enum
- [x] Add `ConfirmAlpacaComplete` command to `RedemptionCommand` enum
- [x] Implement command handler for `ConfirmAlpacaComplete` that produces
      `AlpacaJournalCompleted` event
- [x] Implement `apply()` logic for `AlpacaJournalCompleted` event (transition
      to `Burning` state)
- [x] Update `state_name()` helper to include `Burning` state
- [x] Add unit tests for command handler (given AlpacaCalled, when
      ConfirmAlpacaComplete, then expect AlpacaJournalCompleted event)
- [x] Add unit tests for invalid state transitions (e.g., ConfirmAlpacaComplete
      from wrong state)
- [x] Add unit tests for event application (verify transition to Burning state)

**Files Modified:**

- `src/redemption/mod.rs` - Add state variant, handle command
- `src/redemption/cmd.rs` - Add command variant
- `src/redemption/event.rs` - Add event variant, implement `DomainEvent` trait

**Acceptance Criteria:**

- All aggregate tests pass
- `ConfirmAlpacaComplete` command produces exactly one event:
  `AlpacaJournalCompleted`
- Aggregate transitions from `AlpacaCalled` to `Burning` state
- Invalid state transitions return appropriate errors
- `Burning` state includes all necessary fields (issuer_request_id,
  tokenization_request_id, underlying, token, wallet, quantity, tx_hash,
  timestamps)

## Task 2. Extend AlpacaService with Polling Method

Add the `poll_request_status()` method to query Alpaca's request list endpoint.

**Subtasks:**

- [x] Add `RequestsListResponse` type to represent the list endpoint response
- [x] Add `poll_request_status()` method signature to `AlpacaService` trait
- [x] Implement method in `AlpacaHttpService` to call GET endpoint
- [x] Add filtering logic to find request by `tokenization_request_id`
- [x] Handle case where request is not found in list
- [x] Add mock implementation in `MockAlpacaService` for testing
- [x] Add unit tests for response parsing
- [x] Add unit tests for filtering logic
- [x] Add unit tests for not-found case

**Files Modified:**

- `src/alpaca/mod.rs` - Add trait method, add types
- `src/alpaca/service.rs` - Implement HTTP method
- `src/alpaca/mock.rs` - Add mock implementation

**Acceptance Criteria:**

- Method returns `RedeemRequestStatus` for the specified request
- Method handles list responses correctly (filters, parses)
- Method returns error when request not found
- Mock implementation supports configurable responses for testing
- All service tests pass

## Task 3. Implement Journal Monitor

Create a monitor that polls Alpaca and executes the `ConfirmAlpacaComplete`
command.

**Subtasks:**

- [x] Create `src/redemption/journal_manager.rs` file
- [x] Implement `JournalManager` struct with AlpacaService and CQRS dependencies
- [x] Implement `handle_alpaca_called()` method that spawns polling task
- [x] Implement polling loop with exponential backoff (250ms → 30s max)
- [x] Implement timeout handling (1 hour max)
- [x] Execute `ConfirmAlpacaComplete` command when status is "completed"
- [x] Handle "rejected" status by marking redemption as failed
- [x] Add comprehensive logging for debugging
- [x] Add unit tests for successful completion path
- [x] Add `MarkFailed` command to support timeout/failure scenarios

**Files Modified:**

- `src/redemption/journal_manager.rs` (new file) - JournalManager implementation
- `src/redemption/mod.rs` - Export journal_manager module, add `MarkFailed`
  command handler
- `src/redemption/cmd.rs` - Add `MarkFailed` command variant
- `src/redemption/event.rs` - `RedemptionFailed` event (already existed)

**Acceptance Criteria:**

- Manager successfully polls until completion ✓
- Exponential backoff works correctly (250ms → 500ms → 1s → ... → 30s) ✓
- Timeout triggers failure after 1 hour ✓
- `ConfirmAlpacaComplete` command executed on success ✓
- Rejected status marks redemption as failed ✓
- All manager tests pass ✓
- Comprehensive tracing logs for operational visibility ✓

## Task 4: Update RedemptionView to Handle New Event

Extend the view to process the new event and maintain query state.

**Subtasks:**

- [x] Add `alpaca_completed_at` field to view payload (records when Alpaca
      journal completed)
- [x] Add `Burning` status variant to view's status enum
- [x] Implement `update()` logic for `AlpacaJournalCompleted` event (transition
      to Burning status)
- [x] Add unit tests for view updates with new event
- [x] Verify view queries work correctly with Burning status

**Files Modified:**

- `src/redemption/view.rs`

**Acceptance Criteria:**

- View correctly updates status to `Burning` on `AlpacaJournalCompleted` event
- `alpaca_completed_at` timestamp is properly recorded
- All view tests pass
- Database queries handle Burning status value

## Task 5: Wire Up Journal Monitor in Main Application

Integrate the journal monitor into the event processing pipeline.

**Subtasks:**

- [x] Instantiate `JournalManager` in main application setup
- [x] Register event listener for `AlpacaCalled` events
- [x] Spawn background task when `AlpacaCalled` event occurs
- [x] Ensure proper error handling and logging
- [ ] Add integration test verifying end-to-end flow

**Files Modified:**

- `src/lib.rs` - Instantiate JournalManager and pass to RedemptionDetector,
  rename managers to better names
- `src/redemption/detector.rs` - Spawn polling task after AlpacaCalled event,
  refactor into smaller functions
- `src/redemption/journal_manager.rs` - Refactor into smaller functions to pass
  clippy
- `src/redemption/redeem_manager.rs` - Renamed from alpaca_manager.rs
- `src/redemption/mod.rs` - Updated module exports
- `src/alpaca/mod.rs` - Add allow(dead_code) to TokenizationRequest struct

**Acceptance Criteria:**

- Polling automatically starts when `AlpacaCalled` event is emitted ✓
- Background task doesn't block main event processing ✓
- Errors are logged appropriately ✓
- Integration test covers: Detect → Call Alpaca → Poll → Confirm Complete (not
  yet added)
- All tests pass ✓
- All clippy checks pass ✓

## Task 6: Add End-to-End Tests

Add comprehensive tests covering the entire journal polling flow.

**Subtasks:**

- [ ] Add test for happy path: AlpacaCalled → poll (pending x3) → poll
      (completed) → AlpacaJournalCompleted → Burning state
- [ ] Add test for rejection: AlpacaCalled → poll (pending) → poll (rejected) →
      Failed
- [ ] Add test for timeout: AlpacaCalled → poll (pending for 1+ hour) → Failed
- [ ] Add test verifying exponential backoff intervals
- [ ] Add test for concurrent polling of multiple redemptions
- [ ] Verify AlpacaJournalCompleted event is persisted correctly
- [ ] Verify view state transitions to Burning status

**Files Modified:**

- `tests/redemption_flow_test.rs` or similar end-to-end test file

**Acceptance Criteria:**

- All happy path scenarios covered
- All failure scenarios covered
- Tests use realistic timing (can be mocked/accelerated for test speed)
- Tests verify single event (AlpacaJournalCompleted) is produced and persisted
- Tests verify aggregate transitions to Burning state
- Tests verify view updates correctly
- All tests pass consistently

## Testing Strategy

### Unit Tests

- **Aggregate tests**: Command handling, event application, state transitions
- **Service tests**: Polling endpoint parsing, filtering, error handling
- **Manager tests**: Polling logic, backoff, timeout, command execution

### Integration Tests

- **Manager + CQRS tests**: Verify commands are executed correctly after polling
- **Service + HTTP tests**: Verify actual HTTP requests (using mocks)

### End-to-End Tests

- **Full flow tests**: Detect → Call → Poll → Complete
- **Failure scenarios**: Rejection, timeout, errors

## Dependencies

- Must be implemented **after** #22 (Alpaca redeem call) is complete
- Should be implemented **before** #25 (token burning) to maintain proper flow
- Does **not** depend on #24 (ReceiptInventoryView) - that's parallel work

## Rollout Considerations

- Polling is a background operation, so failures should be logged but not crash
  the system
- Consider adding metrics for:
  - Average polling duration
  - Number of polls required per redemption
  - Timeout rate
  - Alpaca API error rate
- Ensure polling doesn't overwhelm Alpaca's API (respect rate limits)

## Open Questions

None - all design decisions have been made based on existing patterns in the
codebase.
