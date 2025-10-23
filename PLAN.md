# Implementation Plan: End-to-End Mint Flow Tests (Issue #20)

## Overview

This plan implements comprehensive end-to-end tests for the complete mint flow,
from HTTP endpoint invocation through all state transitions to completion. The
tests will exercise the full integration of HTTP endpoints, CQRS
commands/events, managers, and mock external services.

## Context

The mint flow consists of:

1. **Initiate**: AP calls `POST /inkind/issuance` → `MintInitiated` event
2. **Confirm Journal**: Alpaca calls `POST /inkind/issuance/confirm` →
   `JournalConfirmed` event
3. **Mint On-Chain**: MintManager calls BlockchainService → `TokensMinted` event
4. **Callback**: CallbackManager calls AlpacaService → `MintCompleted` event

Issue #20 requires end-to-end tests for:

- Complete happy path flow from initiation to completion

**Note:** End-to-end tests are slower and more complex, so we focus on happy
path flows only. Error scenarios are already comprehensively covered by existing
unit tests (aggregates) and integration tests (endpoints):

- Journal rejection: Tested in `src/mint/api/confirm.rs` (3 integration tests)
- Blockchain failures: Tested in `src/mint/mint_manager.rs` (unit tests)
- Alpaca callback failures: Tested in `src/mint/callback_manager.rs` (unit
  tests)

## Design Decisions

### Test Organization

- Create `tests/` directory at project root for end-to-end tests
- Create `tests/e2e_mint_flow.rs` for complete mint flow tests
- End-to-end tests exercise the entire system: HTTP API → CQRS → Managers →
  External Services → Database
- This is a binary crate (main.rs), so tests go in project-level `tests/`
  directory

### Distinction from Existing Tests

- Existing tests in `src/mint/mod.rs` test CQRS aggregate logic with MemStore
  (no HTTP, no database, no managers)
- Existing tests in `src/mint/api/` test individual HTTP endpoints in isolation
  with mocked dependencies
- **New tests** verify the complete flow: HTTP requests → state transitions →
  database persistence → async processing
- New tests start the full Rocket server with all real wiring (but mock external
  services)

### Test Infrastructure

- Use Rocket's test client for HTTP requests
- Use in-memory SQLite database for isolation (real database, not MemStore)
- Each test gets its own isolated database and Rocket instance
- Use `tokio::time::sleep` with short delays to allow async processing to
  complete
- Create helper functions within the test file for common setup patterns

### Accessing Internal Types and Shaping Public API

Work on this issue will begin exposing types that will eventually form the
public API for the Rust client library (see #52). When exposing types:

- Be intentional - only expose what's truly needed for external consumers
- Re-export necessary types from appropriate modules (e.g.,
  `pub use crate::blockchain::mock::MockBlockchainService;` in
  `src/mint/mod.rs`)
- Don't make entire mock modules public - selectively re-export only what's
  needed
- Expose necessary types and functions that external clients will need
- Mark internal-only helpers as `pub(crate)` to limit scope
- Tests import from the crate: `use st0x_issuance::mint::MockBlockchainService;`

The progression is:

1. This issue (#20): Expose minimal public API surface via selective re-exports
   for end-to-end tests
2. Future issue (#52): Package exposed types into proper client library
3. Refactor these tests to use the client library (making them even more
   realistic)

## Tasks

## Task 1. Create end-to-end test file and expose necessary types

- [ ] Create `tests/` directory at project root (if it doesn't exist)
- [ ] Create `tests/e2e_mint_flow.rs` file with basic structure and imports
- [ ] Re-export mock services from `src/mint/mod.rs` (e.g.,
      `pub use crate::blockchain::mock::MockBlockchainService;`)
- [ ] Identify and expose necessary test utilities by re-exporting from
      `src/mint/mod.rs`
- [ ] Add helper functions in the test file:
  - Helper function for creating test Rocket instance with mock services
  - Helper function for setting up account and asset prerequisites
  - Helper function for waiting for async processing with timeout
  - Helper function for querying database to verify state and events

## Task 2. Implement happy path end-to-end test

Create test that exercises complete successful mint flow:

- [ ] Setup: Create account, asset, and Rocket instance with mock services
- [ ] Step 1: POST to /inkind/issuance, verify 200 response
- [ ] Step 2: POST to /inkind/issuance/confirm with status=completed
- [ ] Step 3: Wait for async processing (mint + callback) to complete
- [ ] Verify final state: Query mint_view, assert Completed state
- [ ] Verify events: Query events table, assert all 4 events exist in correct
      order
  - MintEvent::Initiated
  - MintEvent::JournalConfirmed
  - MintEvent::TokensMinted
  - MintEvent::MintCompleted
- [ ] Verify mock call counts: blockchain service called once, alpaca service
      called once
- [ ] Run `cargo test --workspace`,
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`,
      and `cargo fmt`

## Notes

- These tests complement existing unit tests by testing the full integration
- The tests use mock services, so they don't require actual blockchain or Alpaca
  connectivity
- Async processing in `confirm_journal` endpoint uses `tokio::spawn`, so tests
  need to wait for completion
- Each test should use unique issuer_request_id values to avoid conflicts
- Tests should clean up by using in-memory databases that are dropped after each
  test
