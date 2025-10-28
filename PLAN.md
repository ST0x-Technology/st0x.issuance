# Implementation Plan: ReceiptInventoryView

## Overview

Implement the `ReceiptInventoryView` to track receipt balances for the issuance
bot. This view enables the system to select which receipts to burn from during
redemption operations by maintaining an accurate inventory of all active
receipts and their current balances.

## Background

The Rain `OffchainAssetReceiptVault` uses an ERC-1155 model where each deposit
creates a unique receipt ID. When minting tokens, the vault returns a receipt ID
that represents that specific deposit. When burning tokens during redemption, we
must specify which receipt ID to burn from.

Currently, there's no systematic way to track which receipts exist and their
available balances. This view solves that problem by listening to mint and burn
events and maintaining a queryable inventory.

## Design Decisions

### View ID Format: `issuer_request_id`

We use `issuer_request_id` (the Mint aggregate's aggregate_id) as the view_id
because:

- Allows use of `GenericQuery` which automatically uses the aggregate_id as
  view_id
- The view can track state across the event sequence: Initiated →
  JournalConfirmed → TokensMinted
- `underlying` and `token` are available from `MintEvent::Initiated`, then
  stored in view state
- `receipt_id` and `shares_minted` become available from
  `MintEvent::TokensMinted`
- No need to modify events - all required data flows naturally through existing
  events
- Querying by underlying/token is handled via JSON indexes on the payload

### Tracking Both Initial Amount and Current Balance

The view maintains both `initial_amount` (shares minted originally) and
`current_balance` (remaining shares after burns):

- Provides audit trail showing how much has been burned from each receipt
- Enables queries like "show receipts that have been partially burned"
- Useful for operational metrics and debugging

### Cross-Aggregate Event Listening

This view listens to events from both Mint and Redemption aggregates:

- `TokensMinted` from Mint aggregate (create new receipt, set initial balance)
- Burn events from Redemption aggregate (decrement balance) - **not yet
  implemented, see issue #25**

The `cqrs-es` framework supports cross-aggregate listening through multiple
`View<T>` trait implementations. A single view struct can implement both
`View<Mint>` and `View<Redemption>`, with separate `GenericQuery` instances
registered to each aggregate's CQRS framework. Both queries update the same
database table through a shared `ViewRepository`.

**Current implementation:** Only `View<Mint>` is fully implemented. The
`View<Redemption>` trait is added with stub/TODO implementation, to be completed
when issue #25 adds burn events with receipt details.

### State Model

The view uses an enum to represent states as the mint progresses:

- `Unavailable` - No mint initiated yet (initial state)
- `Pending { underlying, token }` - Mint initiated, waiting for tokens to be
  minted
- `Active { receipt_id, underlying, token, initial_amount, current_balance, minted_at }` -
  Tokens minted, receipt exists with available balance
- `Depleted { receipt_id, underlying, token, initial_amount, depleted_at }` -
  Receipt fully burned (current_balance = 0)

The state progression:

1. `Unavailable` → `Pending` on `MintEvent::Initiated` (captures underlying and
   token)
2. `Pending` → `Active` on `MintEvent::TokensMinted` (captures receipt_id and
   shares_minted)
3. `Active` → `Depleted` when balance reaches zero (future: redemption burn
   events)

This makes invalid states unrepresentable and allows the view to accumulate data
across the event sequence without requiring event modifications.

## Tasks

## Task 1. Create Module Structure

Create the basic module structure following the project's package-by-feature
organization pattern.

- [x] Create `src/receipt_inventory/` directory
- [x] Create `src/receipt_inventory/mod.rs` with module exports
- [x] Create `src/receipt_inventory/view.rs` with view implementation skeleton
- [x] Add module declaration to `src/lib.rs`

## Task 2. Define View Types

Define the types that model receipt inventory state.

- [x] Update `ReceiptInventoryView` enum to include `Pending` state
- [x] Add `Pending { underlying, token }` variant for tracking data from
      Initiated event
- [x] Keep existing `Unavailable`, `Active`, and `Depleted` variants
- [x] Update helper methods: `is_active()`, `is_depleted()`,
      `has_sufficient_balance(amount)`, add `is_pending()`
- [x] Add state transition methods: `with_initiated_data()`,
      `with_tokens_minted()`, `mark_depleted()`

## Task 3. Implement View Traits

Implement both `View<Mint>` and `View<Redemption>` traits for cross-aggregate
listening.

- [x] Implement `View<Mint>` trait in `src/receipt_inventory/view.rs`
- [x] Add `update()` method for Mint that pattern matches on:
  - `MintEvent::Initiated`: Transition `Unavailable` → `Pending`, store
    `underlying` and `token`
  - `MintEvent::TokensMinted`: Transition `Pending` → `Active`, add `receipt_id`
    and `shares_minted`, set both `initial_amount` and `current_balance` to
    `shares_minted`
  - Other events: No-ops (JournalConfirmed, JournalRejected, MintingFailed,
    MintCompleted)
- [x] Implement `View<Redemption>` trait with stub update() method
- [x] Add TODO comments in Redemption update() explaining it will handle burn
      events when issue #25 is implemented
- [x] Make Redemption update() a no-op for all current RedemptionEvent variants

## Task 4. Create Database Migration

Create the SQLite migration for the receipt_inventory_view table.

- [x] Run `sqlx migrate add create_receipt_inventory_view`
- [x] Add `CREATE TABLE receipt_inventory_view` with view_id, version, payload
      columns
- [x] Add indexes on `json_extract(payload, '$.underlying')` for filtering by
      asset (created indexes for both Pending and Active states)
- [x] Add indexes on `json_extract(payload, '$.token')` for filtering by token
      symbol (created indexes for both Pending and Active states)
- [x] Add index on `json_extract(payload, '$.current_balance')` for finding
      available receipts
- [x] Test migration with `sqlx db reset -y`

## Task 5. Wire View into CQRS Framework

Register the view with both Mint and Redemption CQRS frameworks for
cross-aggregate listening.

- [x] Create shared `SqliteViewRepository` for `ReceiptInventoryView` in
      `src/receipt_inventory/view.rs`
- [x] Implement repository following existing patterns from other views
- [x] Create `GenericQuery<Repo, ReceiptInventoryView, Mint>` for Mint events in
      `src/main.rs`
- [x] Create `GenericQuery<Repo, ReceiptInventoryView, Redemption>` for
      Redemption events
- [x] Both queries should use the SAME repository instance (same database table)
- [x] Register Mint query with Mint CQRS framework's queries vector
- [x] Register Redemption query with Redemption CQRS framework's queries vector
- [x] Verify view receives and processes `TokensMinted` events when running the
      application
- [x] Add comment noting Redemption query currently does nothing (waiting for
      #25)

## Task 6. Add Query Methods

Implement query methods for finding receipts during redemption.

- [ ] Add `find_receipt_with_balance()` method to find a receipt with sufficient
      balance for a given underlying symbol
- [ ] Add `list_active_receipts()` method to list all active receipts for an
      underlying symbol
- [ ] Add `get_total_balance()` method to sum balances across all receipts for
      an underlying symbol
- [ ] Add `get_receipt()` method to retrieve specific receipt by
      issuer_request_id
- [ ] Add filtering options (by underlying, by token, by minimum balance)

Query methods should be implemented as associated functions that take a
`SqlitePool` parameter, following the pattern used in other views.

## Task 7. Write Unit Tests

Test the view's event handling logic in isolation.

- [ ] Test `Unavailable` → `Pending` transition on `MintEvent::Initiated`
- [ ] Test `Pending` → `Active` transition on `MintEvent::TokensMinted`
- [ ] Test that view correctly stores underlying/token from Initiated event
- [ ] Test that view correctly combines stored data with TokensMinted data
- [ ] Test helper methods (`is_active()`, `is_pending()`,
      `has_sufficient_balance()`)
- [ ] Test handling multiple mints for different underlying symbols

## Task 8. Write Integration Tests

Test the view with actual database operations.

- [ ] Test view persistence and loading from database
- [ ] Test event replay rebuilds view correctly
- [ ] Test querying receipts by underlying symbol
- [ ] Test querying receipts by token symbol
- [ ] Test finding receipt with sufficient balance for an underlying
- [ ] Test view version tracking (last event sequence)

## Task 9. Write End-to-End Test

Test the view in the context of the full mint flow.

- [ ] Create e2e test in `tests/` directory
- [ ] Set up test with mock services and in-memory database
- [ ] Execute complete mint flow (InitiateMint → ConfirmJournal →
      RecordMintSuccess)
- [ ] Verify receipt appears in inventory view after TokensMinted event
- [ ] Verify receipt has correct initial_amount and current_balance
- [ ] Verify receipt can be queried by underlying and token symbols
- [ ] Clean up test fixtures

## Task 10. Update Documentation

Update project documentation to reflect the new view.

- [ ] Add `ReceiptInventoryView` section to README.md (if it describes views)
- [ ] Verify SPEC.md already documents the view (it does, but check accuracy)
- [ ] Add code comments explaining receipt tracking logic
- [ ] Document query methods with usage examples in docstrings

## Notes

### View State Accumulation Strategy

The view accumulates data across multiple events without modifying event
schemas:

1. **MintEvent::Initiated provides**: `underlying`, `token` (stored in `Pending`
   state)
2. **MintEvent::TokensMinted provides**: `receipt_id`, `shares_minted` (combined
   with stored data to create `Active` state)

This approach:

- Avoids modifying permanent event schemas
- Uses existing event data that already flows through the system
- Leverages the guarantee that events arrive in order
- Follows standard ES patterns for views that aggregate data across events

The view_id is `issuer_request_id` (the Mint aggregate's aggregate_id), which:

- Enables use of `GenericQuery` from cqrs-es framework
- Naturally tracks one receipt per mint operation
- Allows querying by underlying/token via JSON payload indexes

### Future Work (Issue #25)

When token burning is implemented:

1. **Add burn event to Redemption aggregate**: Create a `TokensBurned` or
   `RedemptionCompleted` event that includes:
   - `issuer_request_id`: Which mint (and thus which receipt) was burned from
   - `receipt_id`: The receipt ID that was burned
   - `shares_burned`: Amount burned
   - Other burn transaction details (tx_hash, gas_used, etc.)

2. **Complete View<Redemption> implementation**: Update the stub implementation
   to:
   - Pattern match on the burn event
   - Use the `issuer_request_id` to look up the corresponding mint's receipt
   - Decrement `current_balance` by `shares_burned`
   - If balance reaches zero, transition to `Depleted` state

3. **Update tests**: Add test coverage for:
   - Burn event processing
   - Balance decrement logic
   - Transition to Depleted state
   - Multiple burns from same receipt
   - Attempting to burn more than available (error case)

Note: The relationship between redemptions and receipts needs design work - one
redemption may burn from a receipt created by a different mint operation, so we
need to track which receipt_id is being burned in the redemption event.

### Query Performance Considerations

The view uses JSON payload storage with indexed extracts for querying. For
better performance at scale, consider:

- Separate columns for frequently-queried fields (vault_address, symbol,
  balance)
- Composite indexes for common query patterns
- Caching frequently-accessed receipts in memory

These optimizations can be added later if query performance becomes an issue.
The current approach follows the project's established pattern and will work
well for expected transaction volumes.
