# Implementation Plan: Issue #18 - BlockchainService and On-Chain Minting

This plan outlines the implementation of blockchain integration for on-chain
token minting via the Rain OffchainAssetReceiptVault contract.

## Overview

Issue #18 completes the on-chain minting portion of the mint flow. After Alpaca
confirms the journal (JournalConfirmed event), we need to:

1. Mint tokens on-chain via vault.deposit()
2. Record the result (success/failure) as events
3. Update the Mint aggregate to track these new states
4. Wire the conductor to orchestrate the minting process

The implementation follows ES/CQRS patterns, with the blockchain service
injected via traits for testability. Each task includes its tests to ensure
checks pass after completion.

## Task 1. Define Core Types and BlockchainService Trait

Define the blockchain service abstraction and associated types for on-chain
operations.

- [ ] Create `src/blockchain/mod.rs` module with core types
  - [ ] Define `MintResult` struct:
    - Fields: `tx_hash: B256`, `receipt_id: U256`, `shares_minted: U256`,
      `gas_used: u64`, `block_number: u64`
    - Derive: `Debug`, `Clone`, `PartialEq`, `Serialize`, `Deserialize`
  - [ ] Define `ReceiptInformation` struct for on-chain metadata:
    - Fields: `tokenization_request_id: TokenizationRequestId`,
      `issuer_request_id: IssuerRequestId`, `underlying: UnderlyingSymbol`,
      `quantity: Quantity`, `operation_type: OperationType`,
      `timestamp: DateTime<Utc>`, `notes: Option<String>`
    - Derive: `Debug`, `Clone`, `Serialize`, `Deserialize`
  - [ ] Define `OperationType` enum with variants: `Mint`, `Redeem`
    - Derive: `Debug`, `Clone`, `Serialize`, `Deserialize`
- [ ] Define `BlockchainService` trait
  - [ ] Add method signature:
        `async fn mint_tokens(&self, assets: U256, receiver: Address, receipt_info: ReceiptInformation) -> Result<MintResult, BlockchainError>`
  - [ ] Add trait bounds: `Send + Sync`
- [ ] Define `BlockchainError` enum
  - [ ] Variants: `TransactionFailed { reason: String }`, `InvalidReceipt`,
        `GasEstimationFailed`, `RpcError { source: String }`,
        `EventNotFound { tx_hash: String }`
  - [ ] Derive `Debug` and `thiserror::Error` with proper error messages
- [ ] Re-export types in `src/lib.rs` or create appropriate module structure

**Design Rationale:**

- The trait abstraction allows injecting mock implementations for testing
- `MintResult` captures all on-chain transaction details needed for the
  TokensMinted event
- `ReceiptInformation` provides on-chain metadata for audit trails and is
  encoded as bytes for the contract call

**Tests:** None for this task (type definitions only)

## Task 2. Implement Mock BlockchainService

Create a mock implementation for use in aggregate and conductor tests.

- [ ] Create `src/blockchain/mock.rs`
  - [ ] Define `MockBlockchainService` struct:
    - [ ] Field `should_succeed: bool` - whether mint_tokens returns success or
          error
    - [ ] Field `failure_reason: Option<String>` - error message if
          should_succeed is false
    - [ ] Field `mint_delay_ms: u64` - simulated async delay
    - [ ] Field `call_count: Arc<Mutex<usize>>` - tracks number of mint_tokens
          calls
    - [ ] Field
          `last_call: Arc<Mutex<Option<(U256, Address, ReceiptInformation)>>>` -
          captures last call arguments
  - [ ] Implement `BlockchainService` trait
    - [ ] In `mint_tokens()`: sleep for `mint_delay_ms`, increment `call_count`,
          store arguments in `last_call`
    - [ ] On success: return deterministic `MintResult` with predictable values
          (fixed tx_hash, receipt_id=1, shares=assets, gas_used=21000,
          block_number=1000)
    - [ ] On failure: return
          `BlockchainError::TransactionFailed { reason: failure_reason.clone() }`
- [ ] Add constructor methods
  - [ ] `new_success()` - creates mock that always succeeds
  - [ ] `new_failure(reason: impl Into<String>)` - creates mock that always
        fails with given reason
  - [ ] `with_delay(mut self, delay_ms: u64) -> Self` - builder method to set
        delay
- [ ] Add test helper methods
  - [ ] `get_call_count(&self) -> usize` - returns number of mint_tokens calls
  - [ ] `get_last_call(&self) -> Option<(U256, Address, ReceiptInformation)>` -
        returns last call arguments
  - [ ] `reset(&self)` - resets call_count and last_call

**Design Rationale:**

- Mock service allows testing aggregate and conductor logic without blockchain
  infrastructure
- Configurable success/failure enables testing both happy and error paths
- Call tracking verifies the service is invoked correctly
- Delay simulation helps test async behavior

**Tests:**

- [ ] Write tests in `#[cfg(test)] mod tests` within `mock.rs`
  - [ ] Test `new_success()` returns `MintResult` with expected values
  - [ ] Test `new_failure()` returns error with correct reason
  - [ ] Test `get_call_count()` increments on each call
  - [ ] Test `get_last_call()` captures arguments correctly
  - [ ] Test `with_delay()` causes appropriate delay (use tokio::time::Instant)
  - [ ] Test `reset()` clears call_count and last_call

## Task 3. Extend Mint Aggregate with New Commands, Events, and States

Add new commands, events, and states to support the minting lifecycle. Write
tests alongside implementation.

### Commands (in `src/mint/cmd.rs`)

- [ ] Add `RecordMintSuccess` variant to `MintCommand` enum
  - [ ] Fields: `issuer_request_id: IssuerRequestId`, `tx_hash: B256`,
        `receipt_id: U256`, `shares_minted: U256`, `gas_used: u64`,
        `block_number: u64`
- [ ] Add `RecordMintFailure` variant to `MintCommand` enum
  - [ ] Fields: `issuer_request_id: IssuerRequestId`, `error: String`

### Events (in `src/mint/event.rs`)

- [ ] Add `TokensMinted` variant to `MintEvent` enum
  - [ ] Fields: `issuer_request_id: IssuerRequestId`, `tx_hash: B256`,
        `receipt_id: U256`, `shares_minted: U256`, `gas_used: u64`,
        `block_number: u64`, `minted_at: DateTime<Utc>`
  - [ ] Implement `DomainEvent` trait: event_type "MintEvent::TokensMinted",
        version "1.0"
- [ ] Add `MintingFailed` variant to `MintEvent` enum
  - [ ] Fields: `issuer_request_id: IssuerRequestId`, `error: String`,
        `failed_at: DateTime<Utc>`
  - [ ] Implement `DomainEvent` trait: event_type "MintEvent::MintingFailed",
        version "1.0"

### Aggregate States (in `src/mint/mod.rs`)

- [ ] Add `CallbackPending` variant to `Mint` enum
  - [ ] Carry forward all fields from `JournalConfirmed`
  - [ ] Add transaction details: `tx_hash: B256`, `receipt_id: U256`,
        `shares_minted: U256`, `gas_used: u64`, `block_number: u64`,
        `minted_at: DateTime<Utc>`
- [ ] Add `MintingFailed` variant to `Mint` enum (terminal failure state)
  - [ ] Carry forward all fields from `JournalConfirmed`
  - [ ] Add error details: `error: String`, `failed_at: DateTime<Utc>`

### Error Types

- [ ] Add `NotInJournalConfirmedState { current_state: String }` variant to
      `MintError` enum

**Design Rationale:**

- RecordMintSuccess/Failure commands are executed by the conductor after calling
  BlockchainService
- TokensMinted event captures all on-chain transaction details for audit trail
- We transition directly from JournalConfirmed to CallbackPending (success) or
  MintingFailed (failure)
- No intermediate "Minting" state needed - the conductor handles that
  coordination
- State variants follow enum pattern to make invalid states unrepresentable

**Tests:** Tests will be added in Task 4 alongside command handlers

## Task 4. Implement Command Handlers and Event Application

Add business logic to handle the new commands and update state on events. Write
comprehensive tests.

### Command Handlers (in `src/mint/mod.rs`)

- [ ] Implement `RecordMintSuccess` handler in `Mint::handle()`
  - [ ] Validate current state is `JournalConfirmed` (return
        `NotInJournalConfirmedState` error otherwise)
  - [ ] Validate `issuer_request_id` matches the aggregate's ID (return
        `IssuerRequestIdMismatch` error otherwise)
  - [ ] Generate `TokensMinted` event with provided transaction details
  - [ ] Set `minted_at` to current timestamp (`Utc::now()`)
- [ ] Implement `RecordMintFailure` handler in `Mint::handle()`
  - [ ] Validate current state is `JournalConfirmed`
  - [ ] Validate `issuer_request_id` matches
  - [ ] Generate `MintingFailed` event with error details
  - [ ] Set `failed_at` to current timestamp

### Event Application (in `src/mint/mod.rs`)

- [ ] Handle `TokensMinted` event in `Mint::apply()`
  - [ ] Extract current state fields from `JournalConfirmed` variant (use
        pattern matching)
  - [ ] Transition to `CallbackPending` state, carrying forward all previous
        fields plus transaction details
  - [ ] If current state is not `JournalConfirmed`, return early (graceful
        handling for event replay)
- [ ] Handle `MintingFailed` event in `Mint::apply()`
  - [ ] Extract current state fields from `JournalConfirmed` variant
  - [ ] Transition to `MintingFailed` state, carrying forward fields plus error
        details
  - [ ] If current state is not `JournalConfirmed`, return early
- [ ] Update `state_name()` helper method
  - [ ] Add matches for new states: `"CallbackPending"`, `"MintingFailed"`

**Design Rationale:**

- Command handlers only validate and produce events - they don't call external
  services
- The conductor will call BlockchainService and then execute
  RecordMintSuccess/Failure commands
- Validation ensures commands are only accepted in appropriate states
- `apply()` is deterministic and never fails - it just updates state
- Early returns in `apply()` handle event replay scenarios gracefully

**Tests:**

- [ ] Add tests to `#[cfg(test)] mod tests` in `src/mint/mod.rs`
  - [ ] `test_record_mint_success_from_journal_confirmed_state`
    - Given: Mint in JournalConfirmed state
    - When: RecordMintSuccess command
    - Then: TokensMinted event produced with correct fields
  - [ ] `test_record_mint_success_from_wrong_state_fails`
    - Given: Mint in Initiated state
    - When: RecordMintSuccess command
    - Then: Error NotInJournalConfirmedState
  - [ ] `test_record_mint_failure_from_journal_confirmed_state`
    - Given: Mint in JournalConfirmed state
    - When: RecordMintFailure command
    - Then: MintingFailed event produced with error
  - [ ] `test_record_mint_failure_from_wrong_state_fails`
    - Given: Mint in Initiated state
    - When: RecordMintFailure command
    - Then: Error NotInJournalConfirmedState
  - [ ] `test_apply_tokens_minted_event_updates_state`
    - Given: Mint in JournalConfirmed state
    - When: Apply TokensMinted event
    - Then: State transitions to CallbackPending with tx details
  - [ ] `test_apply_minting_failed_event_updates_state`
    - Given: Mint in JournalConfirmed state
    - When: Apply MintingFailed event
    - Then: State transitions to MintingFailed with error
  - [ ] `test_record_mint_success_with_mismatched_issuer_request_id_fails`
    - Given: Mint with issuer_request_id "iss-123"
    - When: RecordMintSuccess with issuer_request_id "iss-456"
    - Then: Error IssuerRequestIdMismatch

## Task 5. Update MintView to Handle New Events

Ensure the read model stays synchronized with new events. Use enum variants
matching the aggregate pattern.

### View Updates (in `src/mint/view.rs`)

- [ ] Add `CallbackPending` variant to `MintView` enum
  - [ ] Carry forward all fields from `JournalConfirmed`
  - [ ] Add transaction details: `tx_hash: B256`, `receipt_id: U256`,
        `shares_minted: U256`, `gas_used: u64`, `block_number: u64`,
        `minted_at: DateTime<Utc>`
- [ ] Add `MintingFailed` variant to `MintView` enum
  - [ ] Carry forward all fields from `JournalConfirmed`
  - [ ] Add error details: `error: String`, `failed_at: DateTime<Utc>`
- [ ] Update `MintView::update()` method
  - [ ] Handle `TokensMinted` event: extract fields from `JournalConfirmed`
        variant, transition to `CallbackPending` with transaction details
  - [ ] Handle `MintingFailed` event: extract fields from `JournalConfirmed`
        variant, transition to `MintingFailed` with error

**Design Rationale:**

- Views use enum variants to mirror aggregate states (makes invalid states
  unrepresentable)
- View must handle all events to stay consistent with aggregate state
- This allows querying mint operations by status, tx hash, etc.
- Essential for operational dashboards and debugging

**Tests:**

- [ ] Add tests to `#[cfg(test)] mod tests` in `src/mint/view.rs`
  - [ ] `test_view_update_from_tokens_minted_event`
    - Start with view in `JournalConfirmed` variant
    - Apply `TokensMinted` event
    - Verify view transitions to `CallbackPending`
    - Verify all transaction details are present
  - [ ] `test_view_update_from_minting_failed_event`
    - Start with view in `JournalConfirmed` variant
    - Apply `MintingFailed` event
    - Verify view transitions to `MintingFailed`
    - Verify error message is present

## Task 6. Setup Foundry and Generate Contract Bindings

Set up Foundry infrastructure and generate Rust bindings using Alloy's `sol!`
macro, following the pattern from st0x.liquidity-a.

### Add Foundry Project Structure

- [ ] Create contracts directory structure
  - [ ] Create `contracts/src/` directory
  - [ ] Create `contracts/foundry.toml` with basic configuration: `src = "src"`,
        `out = "out"`, `libs = ["../lib"]`
- [ ] Create OffchainAssetReceiptVault interface
  - [ ] Create `contracts/src/IOffchainAssetReceiptVault.sol`
  - [ ] Add SPDX license and pragma: `pragma solidity ^0.8.18;`
  - [ ] Import IERC1155 and IERC20 from forge-std or openzeppelin
  - [ ] Define interface extending both:
        `interface IOffchainAssetReceiptVault is IERC1155, IERC20`
  - [ ] Add `deposit()` function signature:
        `function deposit(uint256 assets, address receiver, bytes calldata receiptInformation) external returns (uint256 id, uint256 shares)`
  - [ ] Add `withdraw()` function signature (for future redemption support):
        `function withdraw(uint256 assets, address receiver, address owner, uint256 id, bytes calldata receiptInformation) external returns (uint256 shares)`
  - [ ] Add `Deposit` event:
        `event Deposit(address indexed sender, address indexed receiver, uint256 assets, uint256 shares, uint256 id, bytes receiptInformation)`
  - [ ] Add `Withdraw` event (for future use):
        `event Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares, uint256 id, bytes receiptInformation)`

### Setup Foundry Dependencies

- [ ] Add forge-std as git submodule
  - [ ] Run:
        `git submodule add https://github.com/foundry-rs/forge-std lib/forge-std`
  - [ ] Update `.gitmodules` file
  - [ ] Run: `git submodule update --init --recursive`

### Update Nix Flake

- [ ] Read current `flake.nix` to understand structure
- [ ] Add `prepSolArtifacts` task to `flake.nix` packages section:

  ```nix
  prepSolArtifacts = rainix.mkTask.${system} {
    name = "prep-sol-artifacts";
    additionalBuildInputs = rainix.sol-build-inputs.${system};
    body = ''
      set -euxo pipefail
      (cd contracts && forge build)
      (cd lib/forge-std && forge build)
    '';
  };
  ```

- [ ] Add `packages.prepSolArtifacts` to devShell buildInputs
- [ ] Check for CI workflow in `.github/workflows/` and update to run
      `prepSolArtifacts` before Rust builds (if CI exists)

### Generate Rust Bindings

- [ ] Create `src/blockchain/bindings.rs`
  - [ ] Use `sol!` macro to generate bindings:

    ```rust
    use alloy::sol;

    sol!(
        #![sol(all_derives = true, rpc)]
        #[allow(clippy::too_many_arguments)]
        #[derive(serde::Serialize, serde::Deserialize)]
        IOffchainAssetReceiptVault,
        "contracts/out/IOffchainAssetReceiptVault.sol/IOffchainAssetReceiptVault.json"
    );
    ```

  - [ ] Add re-exports: `pub use IOffchainAssetReceiptVault::*;`
- [ ] Add `mod bindings;` to `src/blockchain/mod.rs`
- [ ] Add utility functions in `src/blockchain/contract.rs`
  - [ ] `encode_receipt_information(info: &ReceiptInformation) -> Result<Bytes, serde_json::Error>` -
        JSON encode then convert to bytes
  - [ ] `parse_deposit_event(receipt: &TransactionReceipt) -> Result<(U256, U256), BlockchainError>` -
        extract receipt_id and shares_minted from Deposit event
  - [ ] Add error handling for missing events or malformed receipts

**Design Rationale:**

- Foundry provides reproducible builds via Nix for contract artifacts
- Creating an interface allows us to define the minimal ABI we need without
  requiring the full contract
- The `sol!` macro generates type-safe Rust bindings from the forge-built JSON
  artifacts
- This approach matches st0x.liquidity-a and ensures CI can build everything
  deterministically
- Interface can be updated later when we have access to the actual
  OffchainAssetReceiptVault contract
- Building contracts before Rust compilation ensures ABI artifacts are always
  available

**Tests:**

- [ ] Add tests in `src/blockchain/contract.rs`
  - [ ] `test_encode_receipt_information` - verify JSON encoding produces valid
        bytes
  - [ ] `test_parse_deposit_event_success` - mock TransactionReceipt with
        Deposit event, verify extraction
  - [ ] `test_parse_deposit_event_missing` - mock receipt without Deposit event,
        verify error

## Task 7. Implement Real BlockchainService

Implement the production blockchain service that interacts with the actual
on-chain vault contract.

### Service Implementation (in `src/blockchain/service.rs`)

- [ ] Define `RealBlockchainService` struct
  - [ ] Field `provider: Arc<RootProvider<Http<Client>>>` - Alloy RPC provider
  - [ ] Field `signer: PrivateKeySigner` - Private key for signing transactions
  - [ ] Field `vault_address: Address` - OffchainAssetReceiptVault contract
        address
  - [ ] Field `chain_id: u64` - Chain ID (e.g., 8453 for Base)
- [ ] Implement constructor
      `new(rpc_url: String, private_key: String, vault_address: Address, chain_id: u64) -> Result<Self, BlockchainError>`
  - [ ] Parse private_key into `PrivateKeySigner`
  - [ ] Create provider from RPC URL
  - [ ] Return configured service
- [ ] Implement `BlockchainService` trait
  - [ ] Implement `mint_tokens()` method:
    - [ ] Encode `ReceiptInformation` to bytes using
          `encode_receipt_information()`
    - [ ] Create contract instance using vault_address and provider
    - [ ] Build deposit transaction:
          `vault.deposit(assets, receiver, receipt_info_bytes)`
    - [ ] Estimate gas with buffer: `estimated_gas * 120 / 100` (1.2x)
    - [ ] Fetch current gas price and add 10% buffer
    - [ ] Send transaction with gas settings
    - [ ] Wait for transaction confirmation (1 confirmation)
    - [ ] Get transaction receipt
    - [ ] Parse receipt to extract Deposit event using `parse_deposit_event()`
    - [ ] Extract `receipt_id` and `shares_minted` from event
    - [ ] Return `MintResult` with tx_hash, receipt_id, shares_minted, gas_used,
          block_number
    - [ ] Map errors to `BlockchainError` variants (use `?` operator with proper
          error conversion)

**Design Rationale:**

- Real service provides production blockchain interaction
- Configuration injected via constructor for flexibility
- Gas estimation and buffering prevents transaction failures
- Event parsing extracts on-chain results for recording in events
- Error mapping provides clear error messages for debugging

**Tests:**

- [ ] Add integration tests in `src/blockchain/service.rs` (mark as `#[ignore]`
      by default)
  - [ ] `test_mint_tokens_success` - requires testnet RPC and funded account
  - [ ] For MVP, rely on manual testing with mock service
  - [ ] Add note: "Integration tests require RPC_URL and PRIVATE_KEY env vars"

## Task 8. Create Conductor for Mint Flow

Implement the conductor that listens to JournalConfirmed events and coordinates
the minting process.

### Conductor Implementation (in `src/mint/conductor.rs`)

- [ ] Define `MintConductor` struct
  - [ ] Field `blockchain_service: Arc<dyn BlockchainService>` - blockchain
        service for minting
  - [ ] Field `cqrs: Arc<CqrsFramework<Mint>>` - CQRS framework for executing
        commands
- [ ] Implement `new(blockchain_service, cqrs) -> Self` constructor
- [ ] Implement `handle_journal_confirmed()` async method
  - [ ] Parameters: `issuer_request_id: &IssuerRequestId`, `aggregate: &Mint`
  - [ ] Extract mint details from aggregate (quantity, receiver, symbol, etc.)
  - [ ] Convert quantity from `Decimal` to `U256` (handle 18 decimal places:
        multiply by 10^18)
  - [ ] Build `ReceiptInformation` struct with all required fields
  - [ ] Call `blockchain_service.mint_tokens(assets, receiver, receipt_info)`
  - [ ] On success: execute `RecordMintSuccess` command with MintResult details
  - [ ] On failure: execute `RecordMintFailure` command with error message
  - [ ] Log all steps with tracing (info for success, warn for failure)
  - [ ] Return `Result<(), ConductorError>` where ConductorError wraps both
        BlockchainError and CqrsError
- [ ] Define `ConductorError` enum
  - [ ] Variants: `Blockchain(BlockchainError)`, `Cqrs(String)`,
        `InvalidAggregateState { current_state: String }`
  - [ ] Derive `Debug`, `thiserror::Error`

**Design Rationale:**

- Conductor separates coordination logic from aggregate business logic
- It bridges the event-driven world (ES/CQRS) with external services
- Reacts to JournalConfirmed events (detected by caller) to trigger on-chain
  minting
- Executes RecordMintSuccess/Failure commands to record results back into the
  aggregate
- This pattern allows aggregates to remain pure and testable
- Decimal to U256 conversion must handle 18 decimal places for ERC20 standard

**Tests:**

- [ ] Add tests in `#[cfg(test)] mod tests` in `src/mint/conductor.rs`
  - [ ] `test_handle_journal_confirmed_with_success`
    - Use `MockBlockchainService::new_success()`
    - Create aggregate in JournalConfirmed state
    - Call `handle_journal_confirmed()`
    - Verify blockchain service was called (check call_count)
    - Verify RecordMintSuccess command was executed (load aggregate, check state
      is CallbackPending)
  - [ ] `test_handle_journal_confirmed_with_blockchain_failure`
    - Use `MockBlockchainService::new_failure("network error")`
    - Create aggregate in JournalConfirmed state
    - Call `handle_journal_confirmed()`
    - Verify RecordMintFailure command was executed (check state is
      MintingFailed)
  - [ ] `test_handle_journal_confirmed_with_wrong_state_fails`
    - Create aggregate in Initiated state (not JournalConfirmed)
    - Call `handle_journal_confirmed()`
    - Verify error `InvalidAggregateState`
  - [ ] `test_decimal_to_u256_conversion`
    - Test converting Decimal(100.5) to U256 with 18 decimals
    - Verify result is 100.5 * 10^18

## Task 9. Wire Conductor into Application

Integrate the conductor into the main application lifecycle.

### Application Wiring (in `src/main.rs`)

- [ ] Instantiate blockchain service
  - [ ] Load configuration from environment: `RPC_URL`, `PRIVATE_KEY`,
        `VAULT_ADDRESS`, `CHAIN_ID`
  - [ ] Create `RealBlockchainService::new()` with config
  - [ ] Wrap in `Arc<dyn BlockchainService>`
  - [ ] For development/testing, allow switching to
        `Arc::new(MockBlockchainService::new_success())` via feature flag or env
        var
- [ ] Instantiate MintConductor
  - [ ] Create conductor with blockchain service and mint CQRS framework
  - [ ] Store in application state (Rocket state or similar)
- [ ] Set up event listener for JournalConfirmed
  - [ ] After executing ConfirmJournal command in `confirm_journal` endpoint,
        check if JournalConfirmed event was produced
  - [ ] If so, load the aggregate and call
        `conductor.handle_journal_confirmed(issuer_request_id, &aggregate)`
  - [ ] Spawn as async task so endpoint returns quickly (don't block response)
  - [ ] Log any conductor errors but don't fail the endpoint response

**Design Rationale:**

- For MVP, trigger conductor synchronously after ConfirmJournal command
  execution
- Future iterations can use proper event bus for better decoupling
- Async task ensures endpoint remains responsive
- Environment-based configuration allows easy switching between real and mock
  services

**Tests:**

- [ ] Manual testing (documented in task 10)
- [ ] No automated tests for this wiring task (integration tests cover the flow)

## Task 10. Integration Tests

Write end-to-end integration tests for the complete mint flow.

### Integration Tests (in `tests/mint_flow_integration.rs` or in

`src/mint/mod.rs` tests module)

- [ ] `test_complete_mint_flow_with_mock_blockchain`
  - [ ] Setup: create in-memory database, CQRS framework, mock blockchain
        service (success)
  - [ ] Execute InitiateMint command → verify MintInitiated event
  - [ ] Execute ConfirmJournal command → verify JournalConfirmed event
  - [ ] Trigger conductor with aggregate
  - [ ] Verify blockchain service was called (check mock call_count)
  - [ ] Verify TokensMinted event produced (load events from event store)
  - [ ] Verify final aggregate state is CallbackPending
  - [ ] Verify view is updated to CallbackPending
- [ ] `test_mint_flow_with_blockchain_failure`
  - [ ] Setup with mock blockchain service configured for failure
  - [ ] Execute InitiateMint → ConfirmJournal
  - [ ] Trigger conductor
  - [ ] Verify MintingFailed event produced
  - [ ] Verify final aggregate state is MintingFailed with error message
  - [ ] Verify view reflects failure
- [ ] `test_conductor_not_triggered_for_non_journal_confirmed_events`
  - [ ] Execute InitiateMint only
  - [ ] Attempt to trigger conductor
  - [ ] Verify conductor returns error or handles gracefully

**Design Rationale:**

- Integration tests verify the complete flow end-to-end
- Using mock blockchain service keeps tests fast and deterministic
- Tests cover both success and failure paths
- Validates that events, aggregates, views, and conductor all work together

## Task 11. Update Documentation and Run Validation

Final cleanup, documentation, and validation before submitting.

- [ ] Update inline documentation
  - [ ] Add doc comments to `BlockchainService` trait and methods
  - [ ] Add doc comments to new command/event variants
  - [ ] Add doc comments to conductor struct and methods
  - [ ] Add module-level doc comments explaining the blockchain integration
- [ ] Run all tests
  - [ ] `cargo test --workspace` to verify all tests pass
  - [ ] Fix any failing tests
- [ ] Run clippy
  - [ ] `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  - [ ] Fix all warnings and errors (refactor code, don't add #[allow])
- [ ] Run formatter
  - [ ] `cargo fmt --all` to ensure consistent code style
- [ ] Manual end-to-end testing
  - [ ] Start the application with mock blockchain service
  - [ ] Use curl or API client to trigger a mint flow:
    - [ ] POST /inkind/issuance (InitiateMint)
    - [ ] POST /inkind/issuance/confirm (ConfirmJournal)
  - [ ] Verify conductor executes and events are produced (check logs)
  - [ ] Query mint view to see CallbackPending state
  - [ ] Test failure scenario with mock configured to fail
- [ ] Delete PLAN.md before creating PR
  - [ ] PLAN.md is development-only and should not be merged

**Design Rationale:**

- Documentation helps future maintainers understand the code
- Running validation tools ensures code quality and consistency
- Manual testing catches issues automated tests might miss
- PLAN.md deletion keeps the repository clean

## Implementation Notes

### Blockchain Service Dependency Injection

The `BlockchainService` is injected as a trait object, but the Mint aggregate
does not use services directly. Instead, the conductor uses the service and
executes commands on the aggregate.

The Mint aggregate's `Services` type remains `()` since it doesn't need services
in command handlers - all blockchain interaction happens in the conductor.

### Conductor Pattern

The conductor reacts to events and calls external services:

```
JournalConfirmed event produced by ConfirmJournal command
    ↓
confirm_journal endpoint detects event after command execution
    ↓
Conductor.handle_journal_confirmed() is called with aggregate
    ↓
Conductor loads aggregate state to get mint details
    ↓
Conductor calls blockchain_service.mint_tokens()
    ↓
Conductor executes RecordMintSuccess or RecordMintFailure command
    ↓
TokensMinted or MintingFailed event produced
```

This keeps the aggregate pure (no side effects in command handlers) while
enabling integration with external systems.

### State Transition Flow

```
Uninitialized → Initiated → JournalConfirmed → CallbackPending → (future: Completed)
                               ↓                     ↓
                         JournalRejected      MintingFailed
```

- `JournalRejected`: Alpaca rejected the journal transfer (off-chain failure
  before minting)
- `MintingFailed`: On-chain minting transaction failed (on-chain failure)
- We skip intermediate "Minting" state - conductor handles coordination, not the
  aggregate

### Decimal to U256 Conversion

ERC20 tokens use 18 decimal places. Converting Decimal to U256:

```rust
fn decimal_to_u256_with_decimals(value: Decimal, decimals: u8) -> Result<U256, ConversionError> {
    let multiplier = 10_u128.pow(decimals as u32);
    let scaled = value * Decimal::from(multiplier);

    // Ensure no fractional part remains
    if scaled.fract() != Decimal::ZERO {
        return Err(ConversionError::FractionalValue);
    }

    let integer_part = scaled.to_u128()
        .ok_or(ConversionError::Overflow)?;

    Ok(U256::from(integer_part))
}
```

### Future Work (Not in This Issue)

- Issue #19 will implement AlpacaService and callback (RecordCallback command)
- Issue #20 will add end-to-end tests for complete mint flow including callback
- Future: Add proper event bus for conductor instead of inline triggering
- Future: Add retry logic for transient blockchain failures
- Future: Add monitoring/metrics for conductor performance
- Future: Add database migration for new view fields
