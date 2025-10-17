# Implementation Plan: Issue #17 - Implement ConfirmJournal and RejectJournal Features

## Overview

Implement journal confirmation handling for the mint flow. This builds on the
endpoint stub from issue #16 (#48) and extends the Mint aggregate to handle both
successful and failed journal confirmations from Alpaca.

## Context

Currently, the Mint aggregate only supports:

- **Command**: `InitiateMint` → **Event**: `MintInitiated` → **State**:
  `Initiated`

According to SPEC.md, we need to extend this to handle journal confirmation:

- **ConfirmJournal** → produces `JournalConfirmed` event
- **RejectJournal** → produces `JournalRejected` event

This is the critical handoff point where Alpaca confirms or rejects the share
journal transfer, determining whether we can proceed to on-chain minting (in a
future issue) or if the mint failed.

## Design Decisions

### 1. Aggregate State Modeling

**Decision**: Extend the `Mint` enum with new states to represent the mint
lifecycle.

**Rationale**: Using enum variants (not Option fields) follows the "make invalid
states unrepresentable" principle and ensures type safety at compile time.

**New States**:

```rust
enum Mint {
    Uninitialized,
    Initiated { /* existing fields */ },
    JournalConfirmed {
        // All fields from Initiated
        // + journal_confirmed_at: DateTime<Utc>
    },
    JournalRejected {
        // All fields from Initiated
        // + reason: String
        // + rejected_at: DateTime<Utc>
    },
}
```

**Alternative Considered**: Adding optional timestamp fields to `Initiated`
variant. Rejected because it would allow invalid states like
`journal_confirmed_at.is_some()` while mint is still in `Initiated` state.

**Note on Failed State**: We're NOT adding a generic `Failed` state in this
issue. `JournalRejected` is a terminal state for this issue. Future issues will
add more terminal failure states as needed.

### 2. Command Design

**Decision**: Commands contain only the essential data needed to execute the
action.

**Commands**:

```rust
enum MintCommand {
    Initiate { /* existing */ },
    ConfirmJournal {
        issuer_request_id: IssuerRequestId,
    },
    RejectJournal {
        issuer_request_id: IssuerRequestId,
        reason: String,
    },
}
```

**Rationale**: The aggregate already has all mint details in its state. Commands
just trigger state transitions with minimal additional data.

### 3. Event Design

**Decision**: Events are facts about what happened and contain all data needed
to reconstruct state. One event per command for this issue.

**Events**:

```rust
enum MintEvent {
    Initiated { /* existing */ },
    JournalConfirmed {
        issuer_request_id: IssuerRequestId,
        confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerRequestId,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
}
```

**Rationale**:

- `ConfirmJournal` produces ONE event: `JournalConfirmed` (journal transfer
  confirmed is a fact)
- `RejectJournal` produces ONE event: `JournalRejected` (journal transfer
  rejected is a fact)
- We're NOT adding `MintingStarted` or `MintFailed` events yet - those will be
  added in future issues when we implement on-chain minting (#18) and failure
  handling
- Events are permanent, so we're only adding exactly what we need now
- Following YAGNI principle - don't add events until we have code that needs
  them

### 4. View Design

**Decision**: Extend `MintView` to mirror the aggregate states for efficient
querying.

**New View States**:

```rust
enum MintView {
    NotFound,
    Initiated { /* existing */ },
    JournalConfirmed {
        // All fields from Initiated
        // + journal_confirmed_at: DateTime<Utc>
    },
    JournalRejected {
        // All fields from Initiated
        // + reason: String
        // + rejected_at: DateTime<Utc>
    },
}
```

**Rationale**: Views are query-oriented and can be rebuilt, so we can include
whatever fields are useful for querying/filtering without worrying about
permanence. We mirror the aggregate states for efficient querying.

### 5. Error Handling

**Decision**: Add specific errors for invalid state transitions.

**New Errors**:

```rust
enum MintError {
    MintAlreadyInitiated { /* existing */ },
    MintNotFound,
    MintNotInInitiatedState { current_state: String },
}
```

**Rationale**:

- Can only confirm/reject journal for mints in `Initiated` state
- Clear error messages for debugging
- Prevents invalid state transitions at runtime

## Task Breakdown

### Task 1. Extend Mint Aggregate State

Add new enum variants to `Mint` aggregate to represent journal confirmation and
rejection states.

**File**: `src/mint/mod.rs`

**Steps**:

- [ ] Add `JournalConfirmed` variant with all fields from `Initiated` +
      `journal_confirmed_at`
- [ ] Add `JournalRejected` variant with all fields from `Initiated` + `reason`,
      `rejected_at`
- [ ] Keep existing `Uninitialized` and `Initiated` variants unchanged

**Acceptance Criteria**:

- Code compiles (will break `apply` method which we'll fix in Task 5)
- Each state has exactly the data it needs (no Option fields except where truly
  optional)

### Task 2. Add New Commands

Extend `MintCommand` enum with `ConfirmJournal` and `RejectJournal` commands.

**File**: `src/mint/cmd.rs`

**Steps**:

- [ ] Add `ConfirmJournal` variant with `issuer_request_id` field
- [ ] Add `RejectJournal` variant with `issuer_request_id` and `reason` fields
- [ ] Keep existing `Initiate` variant unchanged

**Acceptance Criteria**:

- Commands have minimal data (just what's needed to execute the action)
- Code compiles

### Task 3. Add New Events

Extend `MintEvent` enum with new events according to updated SPEC.md.

**File**: `src/mint/event.rs`

**Steps**:

- [ ] Add `JournalConfirmed` event with `issuer_request_id`, `confirmed_at`
- [ ] Add `JournalRejected` event with `issuer_request_id`, `reason`,
      `rejected_at`
- [ ] Update `event_type()` method in `DomainEvent` impl to handle new events
- [ ] Keep `event_version()` as "1.0" for all events

**Acceptance Criteria**:

- Each event is past tense (Confirmed, Rejected)
- Events contain timestamps for audit trail
- Code compiles

### Task 4. Implement Command Handlers

Add handlers for `ConfirmJournal` and `RejectJournal` in the `Mint` aggregate.

**File**: `src/mint/mod.rs`

**Steps**:

- [ ] Add `ConfirmJournal` handler in `handle()` method:
  - [ ] Validate mint is in `Initiated` state, return error otherwise
  - [ ] Return single event: `JournalConfirmed`
  - [ ] Use `Utc::now()` for timestamp
- [ ] Add `RejectJournal` handler in `handle()` method:
  - [ ] Validate mint is in `Initiated` state, return error otherwise
  - [ ] Return single event: `JournalRejected`
  - [ ] Use `Utc::now()` for timestamp
  - [ ] Include reason from command in event

**Acceptance Criteria**:

- Each command produces exactly 1 event as per updated SPEC.md
- Proper validation prevents invalid state transitions
- Descriptive errors returned for invalid commands

### Task 5. Implement Event Handlers (apply)

Update the `apply()` method to handle new events and update aggregate state.

**File**: `src/mint/mod.rs`

**Steps**:

- [ ] Add `JournalConfirmed` case:
  - [ ] Transition from `Initiated` to `JournalConfirmed`
  - [ ] Carry over all fields from `Initiated` + add `journal_confirmed_at`
- [ ] Add `JournalRejected` case:
  - [ ] Transition from `Initiated` to `JournalRejected`
  - [ ] Carry over all fields from `Initiated` + add `reason` and `rejected_at`

**Acceptance Criteria**:

- `apply()` never fails (events are historical facts)
- State transitions are deterministic
- All event data is properly applied to aggregate state

### Task 6. Add Error Types

Extend `MintError` enum with new error types for validation failures.

**File**: `src/mint/mod.rs`

**Steps**:

- [ ] Add `MintNotFound` error
- [ ] Add `MintNotInInitiatedState { current_state: String }` error
- [ ] Ensure all errors have descriptive messages using `#[error(...)]`
      attribute

**Acceptance Criteria**:

- Error messages are clear and actionable
- Errors contain context needed for debugging

### Task 7. Extend MintView

Update `MintView` to handle new events and add new state variants.

**File**: `src/mint/view.rs`

**Steps**:

- [ ] Add `JournalConfirmed` variant with all query-relevant fields
- [ ] Add `JournalRejected` variant with query-relevant fields (including
      reason, timestamps)
- [ ] Update `View::update()` method to handle new events:
  - [ ] `JournalConfirmed` → transition to `JournalConfirmed` state
  - [ ] `JournalRejected` → transition to `JournalRejected` state

**Acceptance Criteria**:

- Views can be queried efficiently for different mint statuses
- Rejected mints are queryable with reason and timestamps

### Task 8. Wire Endpoint to Commands

Connect the `confirm_journal` endpoint stub to execute appropriate commands
based on journal status.

**File**: `src/mint/api.rs`

**Steps**:

- [ ] Update `confirm_journal` endpoint to accept `MintCqrs` state
- [ ] Match on `JournalStatus`:
  - [ ] `Completed` → execute `ConfirmJournal` command
  - [ ] `Rejected` → execute `RejectJournal` command with reason "Journal
        rejected by Alpaca"
- [ ] Handle command execution errors appropriately
- [ ] Return 200 OK on success (acknowledgement only, no response body needed)
- [ ] Log errors but still return 200 OK (endpoint is acknowledgement-based)

**Acceptance Criteria**:

- Endpoint successfully executes commands via CQRS framework
- Errors are logged but don't fail the endpoint (Alpaca expects 200 OK)
- Events are persisted to event store

### Task 9. Unit Tests - Aggregate

Write Given-When-Then tests for the Mint aggregate command handlers.

**File**: `src/mint/mod.rs` (test module)

**Tests to Add**:

- [ ] `test_confirm_journal_produces_event` - verify `JournalConfirmed` event
- [ ] `test_confirm_journal_transitions_to_journal_confirmed_state` - verify
      state change via apply
- [ ] `test_confirm_journal_for_uninitialized_mint_fails` - error when mint not
      found
- [ ] `test_confirm_journal_for_already_confirmed_mint_fails` - idempotency
      check
- [ ] `test_reject_journal_produces_event` - verify `JournalRejected` event
- [ ] `test_reject_journal_transitions_to_rejected_state` - verify state change
      via apply
- [ ] `test_reject_journal_for_uninitialized_mint_fails` - error when mint not
      found
- [ ] `test_reject_journal_includes_reason_in_event` - verify reason is captured

**Acceptance Criteria**:

- All tests follow Given-When-Then pattern
- Tests verify events produced, not just state changes
- Tests cover happy paths and error cases
- Tests are readable and self-documenting

### Task 10. Unit Tests - View

Write tests for `MintView` update method with new events.

**File**: `src/mint/view.rs` (test module)

**Tests to Add**:

- [ ] `test_view_update_from_journal_confirmed_event` - transitions to
      `JournalConfirmed`
- [ ] `test_view_update_from_journal_rejected_event` - transitions to
      `JournalRejected` with reason

**Acceptance Criteria**:

- Views correctly update in response to all new events
- Rejected mints capture reason for querying

### Task 11. Integration Tests - Endpoint

Write end-to-end tests for the `confirm_journal` endpoint with real CQRS
execution.

**File**: `src/mint/api.rs` (test module)

**Tests to Add**:

- [ ] `test_confirm_journal_completed_executes_command` - verify command
      execution
- [ ] `test_confirm_journal_completed_persists_events` - verify events in event
      store
- [ ] `test_confirm_journal_completed_updates_view` - verify view updated to
      `JournalConfirmed`
- [ ] `test_confirm_journal_rejected_executes_command` - verify command
      execution
- [ ] `test_confirm_journal_rejected_persists_events` - verify events in event
      store
- [ ] `test_confirm_journal_rejected_updates_view` - verify view updated to
      `JournalRejected`
- [ ] `test_confirm_journal_for_nonexistent_mint_logs_error_returns_ok` -
      graceful handling

**Acceptance Criteria**:

- Tests use in-memory database with migrations
- Tests verify full CQRS flow (command → events → state → view)
- Tests verify endpoint returns 200 OK even on errors (per Alpaca API spec)
- Tests verify proper logging of errors

### Task 12. Run Test Suite

Ensure all tests pass and code quality checks succeed.

**Steps**:

- [ ] Run `cargo test --workspace` - all tests must pass
- [ ] Run
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` -
      no warnings
- [ ] Run `cargo fmt` - code properly formatted

**Acceptance Criteria**:

- All tests pass (including new and existing tests)
- No clippy warnings
- Code is formatted

## Testing Strategy

### Unit Tests (Aggregate)

- Use `cqrs_es::test::TestFramework` for Given-When-Then testing
- Test each command handler independently
- Verify events produced, not just final state
- Test error cases (invalid state transitions)

### Unit Tests (View)

- Test view updates for each new event
- Verify state transitions in views
- Ensure views are queryable after updates

### Integration Tests (Endpoint)

- Use in-memory SQLite database
- Test full CQRS flow end-to-end
- Verify events persisted to event store
- Verify views updated correctly
- Test both `completed` and `rejected` status paths

## Success Criteria

- [ ] All new commands, events, and states implemented
- [ ] Aggregate properly validates state transitions
- [ ] Events are correctly applied to aggregate state
- [ ] Views correctly updated in response to events
- [ ] Endpoint wired to execute commands via CQRS
- [ ] Comprehensive test coverage (unit + integration)
- [ ] All tests pass
- [ ] No clippy warnings
- [ ] Code formatted with rustfmt

## Out of Scope

The following are explicitly NOT part of this task (will be handled in future
issues):

- On-chain minting implementation (#18)
- Alpaca callback implementation (#19)
- Receipt inventory tracking
- Additional mint states beyond `JournalConfirmed` and `Failed`
- Monitoring/metrics
- Error alerting/notification

These will be implemented in subsequent issues as per the roadmap.
