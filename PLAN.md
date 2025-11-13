# Implementation Plan: Receipt Custody Model with Multicall

## Overview

Implement the receipt custody model where the bot's wallet holds all ERC1155
receipts while users hold ERC20 shares. Uses the vault's `multicall()` function
to atomically execute deposit and transfer in a single transaction.

## Architecture Decision

**Atomic implementation using multicall** - No custom contracts needed:

- `vault.multicall()` executes multiple calls in single transaction
- `vault.deposit()` mints shares + receipts to bot
- `vault.transfer()` sends shares to user
- All succeed or all fail - true atomicity
- Works because 1:1 share ratio allows predicting transfer amount

---

## Task 1. Add Multicall Support to VaultService

**Goal:** Add method to VaultService that uses multicall for atomic
mint-and-transfer.

**Files to modify:**

- `src/vault/mod.rs`
- `src/vault/service.rs`
- `src/vault/mock.rs`

**Subtasks:**

- [x] Add `mint_and_transfer_shares()` to VaultService trait
- [x] Implement `mint_and_transfer_shares()` for RealBlockchainService
  - [x] Encode `deposit()` call with `receiver = bot_wallet`
  - [x] Encode `transfer()` call with `to = user_wallet, amount = assets`
  - [x] Call `vault.multicall([deposit_call, transfer_call])`
  - [x] Parse results to extract receipt_id, shares_minted from deposit event
  - [x] Return MintResult
- [x] Implement `mint_and_transfer_shares()` for MockVaultService
  - [x] Simulate both operations
  - [x] Store that bot has receipts and user has shares
  - [x] Return MintResult

**Implementation pseudocode:**

```rust
// VaultService trait
async fn mint_and_transfer_shares(
    &self,
    assets: U256,
    bot_wallet: Address,
    user_wallet: Address,
    receipt_info: ReceiptInformation,
) -> Result<MintResult, VaultError>;

// RealBlockchainService implementation
let deposit_call = vault.deposit(assets, bot_wallet, share_ratio, receipt_info_bytes).calldata();
let transfer_call = vault.transfer(user_wallet, assets).calldata();
let receipt = vault.multicall(vec![deposit_call, transfer_call]).send().await?.get_receipt().await?;
// Parse Deposit event to get receipt_id and shares_minted
```

---

## Task 2. Update MintManager to Use Multicall

**Goal:** Change MintManager to call new `mint_and_transfer_shares()` method.

**Files to modify:**

- `src/mint/mint_manager.rs`
- `src/lib.rs`

**Subtasks:**

- [x] Add `bot_wallet: Address` field to `MintManager` struct
- [x] Update `MintManager::new()` constructor to accept `bot_wallet` parameter
- [x] Update `handle_journal_confirmed()` to call `mint_and_transfer_shares()`
  - [x] Change from `mint_tokens(assets, *wallet, receipt_info)`
  - [x] Change to
        `mint_and_transfer_shares(assets, self.bot_wallet, *wallet, receipt_info)`
- [x] Update `src/lib.rs` to pass `config.bot_wallet` when creating MintManager

**Error handling:**

- If multicall fails, entire transaction reverts
- No partial state - either user gets shares OR nothing happens
- Standard error propagation through `?` operator

---

## Task 3. Update Configuration Documentation

**Goal:** Document that `bot_wallet` is the bot's wallet that holds receipts.

**Files to modify:**

- `src/config.rs`
- `AGENTS.md`

**Subtasks:**

- [ ] Add doc comment to `bot_wallet` field in Config explaining:
  - [ ] Holds all ERC1155 receipts from minting operations
  - [ ] Receives ERC20 shares from users initiating redemptions
  - [ ] Performs burns (requires both shares + receipts)
  - [ ] Same wallet controlled by `private_key`
  - [ ] Multicall atomically mints receipts to bot while transferring shares to
        users
- [ ] Update AGENTS.md configuration section to explain:
  - [ ] Receipt custody model
  - [ ] Why bot holds receipts
  - [ ] How multicall ensures atomicity

---

## Task 4. Verify BurnManager Works with Receipt Custody

**Goal:** Confirm BurnManager already works with receipt custody model (no
changes needed).

**Files to verify:**

- `src/redemption/burn_manager.rs`

**Subtasks:**

- [ ] Verify BurnManager has `bot_wallet: Address` field
- [ ] Verify it uses `self.bot_wallet` as owner when burning
- [ ] Verify owner will have both shares (from user transfer) + receipts (from
      mint)
- [ ] Confirm no code changes are needed

---

## Task 5. Verify E2E Test Passes

**Goal:** Ensure e2e test passes with multicall implementation.

**Files:**

- `tests/e2e.rs`

**Subtasks:**

- [ ] Run `cargo test -q test_tokenization_flow`
- [ ] Verify user receives shares after mint (from multicall transfer)
- [ ] Verify bot holds receipts (from multicall deposit)
- [ ] Verify user can transfer shares back to bot for redemption
- [ ] Verify bot successfully burns (has both shares + receipts)
- [ ] Consider adding unit tests for `mint_and_transfer_shares()` encoding
- [ ] Consider adding test to verify multicall calldata is correct

---

## Implementation Order

Follow this sequence to minimize risk:

1. Task 3 (documentation) - No code changes, can do first
2. Task 4 (verification) - Confirm BurnManager is ready
3. Task 1 (add multicall to VaultService) - Core functionality
4. Task 2 (update MintManager) - Use new functionality
5. Task 5 (test verification) - Validate everything works

## Success Criteria

- [ ] E2E test passes
- [ ] User receives shares after mint (via multicall)
- [ ] Bot holds receipts after mint (via multicall)
- [ ] Single transaction for mint-and-transfer (truly atomic)
- [ ] Redemption burn succeeds (bot has shares + receipts)
- [ ] All existing tests pass
- [ ] Clippy and fmt checks pass

## Key Assumptions

**CRITICAL:** This implementation relies on 1:1 share ratio (1 asset = 1 share).

**Verification:**

- ✅ We always pass `minShareRatio = 1e18` (src/vault/service.rs:61)
- ✅ Vault defaults to `1e18` ratio (ReceiptVault.sol:524)
- ✅ OffchainAssetReceiptVault doesn't override `_shareRatio()`
- ✅ Formula: `assets * 1e18 / 1e18 = assets`

**If ratio changes:** This approach breaks (can't predict transfer amount).
Would need custom wrapper contract instead.

## Risk Assessment

**Very Low Risk:**

- ✅ Uses existing vault functionality (multicall built-in)
- ✅ Truly atomic - no failure windows
- ✅ 1:1 share ratio is guaranteed by our code and vault defaults
- ✅ No custom contracts to audit/deploy
- ✅ Maintains complete audit trail

**Potential Issues:**

- Slightly higher gas cost (multicall overhead vs single call)
- Need to correctly encode calls for multicall
- Parsing multicall results requires understanding event structure

**Mitigation:**

- Test multicall encoding thoroughly
- Document the 1:1 ratio assumption clearly
- Add assertions to verify share ratio in tests

## Rollback Plan

If issues arise:

1. Revert Tasks 1-2 (multicall changes)
2. System returns to minting directly to user
3. Accept that redemption requires complex UX (transfer shares + receipts)

Rollback is simple because changes are isolated to VaultService and MintManager.
