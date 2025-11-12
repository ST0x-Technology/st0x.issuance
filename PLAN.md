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

## Task Breakdown

### Task 1: Add multicall Support to VaultService

**Goal:** Add method to VaultService that uses multicall for atomic
mint-and-transfer.

**Files to modify:**

- `src/vault/mod.rs` - Add `mint_and_transfer_shares()` to VaultService trait
- `src/vault/service.rs` - Implement `mint_and_transfer_shares()` for
  RealBlockchainService
- `src/vault/mock.rs` - Implement `mint_and_transfer_shares()` for
  MockVaultService

**VaultService trait addition:**

```rust
async fn mint_and_transfer_shares(
    &self,
    assets: U256,
    bot_wallet: Address,
    user_wallet: Address,
    receipt_info: ReceiptInformation,
) -> Result<MintResult, VaultError>;
```

**Implementation approach (RealBlockchainService):**

1. Encode `deposit()` call with `receiver = bot_wallet`
2. Encode `transfer()` call with `to = user_wallet, amount = assets` (1:1
   ratio!)
3. Call `vault.multicall([deposit_call, transfer_call])`
4. Parse results to extract receipt_id, shares_minted from deposit event
5. Return MintResult (existing struct, no changes needed)

**Key implementation details:**

```rust
// 1. Encode deposit call
let deposit_call = vault.deposit(assets, bot_wallet, share_ratio, receipt_info_bytes)
    .calldata();

// 2. Encode transfer call (same amount due to 1:1 ratio)
let transfer_call = vault.transfer(user_wallet, assets).calldata();

// 3. Execute atomically
let receipt = vault
    .multicall(vec![deposit_call, transfer_call])
    .send()
    .await?
    .get_receipt()
    .await?;

// 4. Parse Deposit event to get receipt_id and shares_minted
// (Transfer event is just confirmation, we don't need data from it)
```

**Mock implementation:**

- Simulate both operations
- Store that bot has receipts and user has shares
- Return MintResult as usual

**Result:** Atomic mint-and-transfer in single transaction.

---

### Task 2: Update MintManager to Use Multicall

**Goal:** Change MintManager to call new `mint_and_transfer_shares()` method.

**Files to modify:**

- `src/mint/mint_manager.rs`

**Changes:**

1. Add `bot_wallet: Address` field to `MintManager` struct
2. Update constructor to accept `bot_wallet` parameter
3. In `handle_journal_confirmed()`:
   - Change from: `mint_tokens(assets, *wallet, receipt_info)`
   - Change to:
     `mint_and_transfer_shares(assets, self.bot_wallet, *wallet, receipt_info)`
   - Store `user_wallet` (`*wallet`) in aggregate for tracking

4. Update `src/lib.rs` to pass bot_wallet when creating MintManager:
   - Use `config.redemption_wallet` as the bot's wallet

**Error handling:**

- If multicall fails, entire transaction reverts
- No partial state - either user gets shares OR nothing happens
- Standard error propagation through `?` operator

**Result:** Minting atomically gives receipts to bot and shares to user.

---

### Task 3: Update Configuration Documentation

**Goal:** Document that `redemption_wallet` is the bot's wallet that holds
receipts.

**Files to modify:**

- `src/config.rs` - Add doc comment to `redemption_wallet` field
- `AGENTS.md` - Update configuration section

**Changes:**

1. Add doc comment:
   ```rust
   /// Address of the bot's wallet. This wallet:
   /// - Holds all ERC1155 receipts from minting operations
   /// - Receives ERC20 shares from users initiating redemptions
   /// - Performs burns (requires both shares + receipts)
   ///
   /// Note: This is the same wallet controlled by `private_key`. Users send
   /// shares to this address to initiate redemptions.
   ///
   /// The bot uses multicall to atomically mint receipts to itself while
   /// transferring shares to users, maintaining the receipt custody model.
   #[arg(long, env = "REDEMPTION_WALLET")]
   redemption_wallet: Address,
   ```

2. Update AGENTS.md to explain:
   - Receipt custody model
   - Why bot holds receipts
   - How multicall ensures atomicity

**Result:** Clear documentation of architecture.

---

### Task 4: Verify BurnManager (No Changes Needed)

**Goal:** Confirm BurnManager already works with receipt custody model.

**Files to verify:**

- `src/redemption/burn_manager.rs`

**Current state:**

- ✅ BurnManager has `wallet: Address` field (bot's wallet)
- ✅ Uses `self.wallet` as owner when burning
- ✅ Owner has both shares (from user transfer) + receipts (from mint)

**Verification:**

- No code changes needed
- BurnManager is already correct for this model

**Result:** Redemption flow works as-is.

---

### Task 5: Verify E2E Test Passes

**Goal:** Ensure e2e test passes with multicall implementation.

**Files:**

- `tests/e2e.rs` - Already has documentation comments

**Verification steps:**

1. Run `cargo test -q test_tokenization_flow`
2. Verify:
   - User receives shares after mint (from multicall transfer)
   - Bot holds receipts (from multicall deposit)
   - User can transfer shares back to bot for redemption
   - Bot successfully burns (has both shares + receipts)

**Expected behavior:**

- Test passes without code changes
- Multicall is transparent to test logic
- Only implementation detail changes

**Additional test considerations:**

- May want unit tests for `mint_and_transfer_shares()` with mock vault
- May want to verify multicall encoding is correct

**Result:** Full e2e flow validated.

---

## Implementation Order

1. **Task 3** (documentation) - No code changes, can do first
2. **Task 4** (verification) - Confirm BurnManager is ready
3. **Task 1** (add multicall to VaultService) - Core functionality
4. **Task 2** (update MintManager) - Use new functionality
5. **Task 5** (test verification) - Validate everything works

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

## Testing Strategy

1. **Unit tests:**
   - Mock vault and test `mint_and_transfer_shares()` encoding
   - Verify correct calldata generation
   - Test error handling

2. **Integration tests:**
   - Test with real vault on Anvil
   - Verify multicall executes both operations
   - Confirm bot has receipts, user has shares

3. **E2E test:**
   - Full flow from mint request to redemption
   - Validates entire system works end-to-end

## Key Assumptions

**CRITICAL:** This implementation relies on 1:1 share ratio (1 asset = 1 share).

**Verification:**

- ✅ We always pass `minShareRatio = 1e18` (src/vault/service.rs:61)
- ✅ Vault defaults to `1e18` ratio (ReceiptVault.sol:524)
- ✅ OffchainAssetReceiptVault doesn't override `_shareRatio()`
- ✅ Formula: `assets * 1e18 / 1e18 = assets`

**If ratio changes:** This approach breaks (can't predict transfer amount).
Would need custom wrapper contract instead.

## Rollback Plan

If issues arise:

1. Revert Tasks 1-2 (multicall changes)
2. System returns to minting directly to user
3. Accept that redemption requires complex UX (transfer shares + receipts)

Rollback is simple because changes are isolated to VaultService and MintManager.

## Success Criteria

- [ ] E2E test passes
- [ ] User receives shares after mint (via multicall)
- [ ] Bot holds receipts after mint (via multicall)
- [ ] Single transaction for mint-and-transfer (truly atomic)
- [ ] Redemption burn succeeds (bot has shares + receipts)
- [ ] All existing tests pass
- [ ] Clippy and fmt checks pass
- [ ] Gas usage is acceptable (measure before/after)

## Gas Cost Analysis

Before implementing, measure gas cost:

- Current: `deposit()` = ~180k gas
- Proposed: `multicall([deposit(), transfer()])` = ~220k gas (estimate)
- Acceptable trade-off for atomicity guarantee

Will measure actual costs during testing.
