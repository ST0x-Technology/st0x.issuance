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

### View ID Format: `receipt_id:underlying`

We use a composite key format `{receipt_id}:{underlying}` as the view_id
because:

- Receipt IDs are only unique within a single vault contract
- Each underlying symbol maps to exactly one vault (1:1 relationship enforced by
  TokenizedAsset aggregate using underlying as aggregate_id)
- Using underlying instead of vault_address avoids needing to pass vault_address
  through the command chain (it's not currently available in RecordMintSuccess)
- We can derive vault_address from underlying via TokenizedAssetView when needed
  for queries
- The composite key ensures global uniqueness across all assets

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

The view uses an enum to represent three possible states:

- `Active { receipt_id, underlying, token, initial_amount, current_balance, minted_at }` -
  Receipt exists with available balance
- `Depleted { receipt_id, underlying, token, initial_amount, depleted_at }` -
  Receipt fully burned (current_balance = 0)
- Not present in view - Receipt doesn't exist

This makes invalid states unrepresentable and clearly distinguishes between
active and depleted receipts.

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

- [ ] Define `ReceiptInventoryView` enum with `Active` and `Depleted` variants
- [ ] Add all required fields (receipt_id, underlying, token, initial_amount,
      current_balance, timestamps)
- [ ] Implement `Serialize` and `Deserialize` derives
- [ ] Add helper methods: `is_active()`, `is_depleted()`,
      `has_sufficient_balance(amount)`
- [ ] Add constructor methods: `new_active()`, `mark_depleted()`

## Task 3. Extend TokensMinted Event

Add missing fields to `TokensMinted` event so the view has all required data.

- [ ] Update `MintEvent::TokensMinted` variant in `src/mint/event.rs` to
      include:
  - `underlying: UnderlyingSymbol`
  - `token: TokenSymbol`
- [ ] Update `handle_record_mint_success()` in Mint aggregate to extract
      underlying and token from the JournalConfirmed state and include them in
      the event
- [ ] Update all existing tests that use `TokensMinted` to include new fields
- [ ] Update `apply_tokens_minted()` method (no changes needed, just verify it
      ignores extra fields)
- [ ] Update `MintView::update()` to handle new fields in `TokensMinted` event
- [ ] Run `cargo test --workspace` to ensure no regressions

**Rationale:** Adding fields to events is safe in event sourcing - the apply
method already ignores fields it doesn't need via pattern matching. These fields
are already available in the aggregate's JournalConfirmed state when
TokensMinted is produced.

## Task 4. Implement View Traits

Implement both `View<Mint>` and `View<Redemption>` traits for cross-aggregate
listening.

- [ ] Implement `View<Mint>` trait in `src/receipt_inventory/view.rs`
- [ ] Add `update()` method for Mint that pattern matches on
      `MintEvent::TokensMinted`
- [ ] Extract all required fields from TokensMinted: receipt_id, shares_minted,
      underlying, token
- [ ] Generate composite view_id: `format!("{receipt_id}:{underlying}")`
- [ ] Create new `Active` receipt entry with initial_amount and current_balance
      both set to shares_minted
- [ ] Handle other MintEvent variants as no-ops
- [ ] Implement `View<Redemption>` trait with stub update() method
- [ ] Add TODO comments in Redemption update() explaining it will handle burn
      events when issue #25 is implemented
- [ ] Make Redemption update() a no-op for all current RedemptionEvent variants

## Task 5. Create Database Migration

Create the SQLite migration for the receipt_inventory_view table.

- [ ] Run `sqlx migrate add create_receipt_inventory_view`
- [ ] Add `CREATE TABLE receipt_inventory_view` with view_id, version, payload
      columns
- [ ] Add indexes on `json_extract(payload, '$.underlying')` for filtering by
      asset
- [ ] Add indexes on `json_extract(payload, '$.token')` for filtering by token
      symbol
- [ ] Add index on `json_extract(payload, '$.current_balance')` for finding
      available receipts
- [ ] Test migration with `sqlx db reset -y`

## Task 6. Wire View into CQRS Framework

Register the view with both Mint and Redemption CQRS frameworks for
cross-aggregate listening.

- [ ] Create shared `SqliteViewRepository` for `ReceiptInventoryView` in
      `src/receipt_inventory/view.rs`
- [ ] Implement repository following existing patterns from other views
- [ ] Create `GenericQuery<Repo, ReceiptInventoryView, Mint>` for Mint events in
      `src/main.rs`
- [ ] Create `GenericQuery<Repo, ReceiptInventoryView, Redemption>` for
      Redemption events
- [ ] Both queries should use the SAME repository instance (same database table)
- [ ] Register Mint query with Mint CQRS framework's queries vector
- [ ] Register Redemption query with Redemption CQRS framework's queries vector
- [ ] Verify view receives and processes `TokensMinted` events when running the
      application
- [ ] Add comment noting Redemption query currently does nothing (waiting for
      #25)

## Task 7. Add Query Methods

Implement query methods for finding receipts during redemption.

- [ ] Add `find_receipt_with_balance()` method to find a receipt with sufficient
      balance for a given underlying symbol
- [ ] Add `list_active_receipts()` method to list all active receipts for an
      underlying symbol
- [ ] Add `get_total_balance()` method to sum balances across all receipts for
      an underlying symbol
- [ ] Add `get_receipt()` method to retrieve specific receipt by composite ID
- [ ] Add filtering options (by underlying, by token, by minimum balance)

Query methods should be implemented as associated functions that take a
`SqlitePool` parameter, following the pattern used in other views.

## Task 8. Write Unit Tests

Test the view's event handling logic in isolation.

- [ ] Test creating new receipt from `TokensMinted` event
- [ ] Test receipt state after creation (verify all fields populated correctly)
- [ ] Test view_id generation (format: `receipt_id:underlying`)
- [ ] Test helper methods (`is_active()`, `has_sufficient_balance()`)
- [ ] Test handling multiple receipts for same underlying symbol
- [ ] Test handling receipts across different underlying symbols

## Task 9. Write Integration Tests

Test the view with actual database operations.

- [ ] Test view persistence and loading from database
- [ ] Test event replay rebuilds view correctly
- [ ] Test querying receipts by underlying symbol
- [ ] Test querying receipts by token symbol
- [ ] Test finding receipt with sufficient balance for an underlying
- [ ] Test view version tracking (last event sequence)

## Task 10. Write End-to-End Test

Test the view in the context of the full mint flow.

- [ ] Create e2e test in `tests/` directory
- [ ] Set up test with mock services and in-memory database
- [ ] Execute complete mint flow (InitiateMint → ConfirmJournal →
      RecordMintSuccess)
- [ ] Verify receipt appears in inventory view after TokensMinted event
- [ ] Verify receipt has correct initial_amount and current_balance
- [ ] Verify receipt can be queried by underlying and token symbols
- [ ] Clean up test fixtures

## Task 11. Update Documentation

Update project documentation to reflect the new view.

- [ ] Add `ReceiptInventoryView` section to README.md (if it describes views)
- [ ] Verify SPEC.md already documents the view (it does, but check accuracy)
- [ ] Add code comments explaining receipt tracking logic
- [ ] Document query methods with usage examples in docstrings

## Notes

### Event Extension Strategy

We're adding `underlying` and `token` fields to the `TokensMinted` event. This
is safe because:

1. **Adding fields is backward compatible** - The `apply` method uses pattern
   matching with `..` to ignore extra fields, so old code continues to work
2. **No event migration needed** - We're not changing existing fields, just
   adding new ones
3. **Data is already available** - When `RecordMintSuccess` command runs, the
   aggregate is in `JournalConfirmed` state which contains `underlying` and
   `token`
4. **Avoids architectural violations** - Views cannot access database or other
   views from the `update` method, so cross-view lookups are not feasible

We use `receipt_id:underlying` as the view_id instead of
`receipt_id:vault_address` because:

- MintManager doesn't have vault_address when calling RecordMintSuccess
- Each underlying maps to exactly one vault (1:1 enforced by TokenizedAsset
  aggregate)
- We can derive vault_address from underlying via TokenizedAssetView in query
  methods if needed

Alternative considered: Add vault_address to RecordMintSuccess command. Rejected
because the MintManager calls blockchain_service.mint_tokens() which abstracts
away the vault details - the manager doesn't know or care which specific vault
contract was used.

### Future Work (Issue #25)

When token burning is implemented:

1. **Add burn event to Redemption aggregate**: Create a `TokensBurned` or
   `RedemptionCompleted` event that includes:
   - `receipt_id`: Which receipt was burned from
   - `shares_burned`: Amount burned
   - `underlying`: Asset symbol
   - Other burn transaction details (tx_hash, gas_used, etc.)

2. **Complete View<Redemption> implementation**: Update the stub implementation
   to:
   - Pattern match on the burn event
   - Load the existing receipt from the view using receipt_id:underlying
   - Decrement `current_balance` by `shares_burned`
   - If balance reaches zero, transition to `Depleted` state
   - Save the updated view

3. **Update tests**: Add test coverage for:
   - Burn event processing
   - Balance decrement logic
   - Transition to Depleted state
   - Multiple burns from same receipt
   - Attempting to burn more than available (error case)

The cross-aggregate view pattern is already set up - only the Redemption event
handling logic needs to be completed.

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
