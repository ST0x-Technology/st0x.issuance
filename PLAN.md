# Plan: Implement E2E Tests with Anvil

## Goal

Enable end-to-end testing of mint and redemption flows using Anvil (local
blockchain) with real on-chain transactions to the OffchainAssetReceiptVault
contract.

## Design Decisions

**Public API for testing**: E2E tests need to construct Config with test values
and pass it to initialize_rocket(). This requires making Config public and
changing initialize_rocket() to accept a Config parameter.

**LocalEvm lifecycle management**: LocalEvm owns the AnvilInstance, keeping the
local blockchain alive for the test duration. It encapsulates all deployment
complexity - tests simply call LocalEvm::new() and get a configured environment.

**Follow Solidity deployment pattern**: The Solidity tests in lib/ethgild/test/
are the canonical reference. We follow their exact deployment sequence to ensure
correct initialization of the factory/clone pattern.

**No VaultService exposure**: VaultService remains internal. Tests configure the
application via Config, and initialize_rocket() internally creates VaultService.
Tests interact only through the public HTTP API.

## Task 1. Expose Public API for E2E Tests

**Desired state**: E2E tests can construct Config and call initialize_rocket().

- [x] Make Config struct public
- [x] Make all Config fields public
- [x] Make alpaca module public and re-export AlpacaConfig
- [x] Make bindings module public
- [x] Change initialize_rocket() signature to accept Config parameter
- [x] Update src/main.rs to call Config::parse() and pass to initialize_rocket()
- [x] Verify compilation succeeds

## Task 2. Implement LocalEvm Deployment Infrastructure

**Desired state**: Tests call LocalEvm::new() to get a configured Anvil
environment with deployed vault contracts.

- [x] Add required imports to test_utils.rs
- [x] Define LocalEvmError enum
- [x] Define LocalEvm struct:
  - `_anvil: AnvilInstance`
  - `pub vault_address: Address`
  - `pub authorizer_address: Address`
  - `pub wallet_address: Address`
  - `pub private_key: B256`
  - `pub endpoint: String`
- [x] Implement LocalEvm::new() method
- [x] Implement LocalEvm::deploy_vault() private helper method
- [x] Implement LocalEvm::private_key_hex() method returning String
- [x] Implement LocalEvm::grant_deposit_role() method

### LocalEvm::new() Implementation

- [x] Spawn Anvil instance
- [x] Get WebSocket endpoint
- [x] Get first Anvil pre-funded key as B256
- [x] Create PrivateKeySigner from key
- [x] Get wallet address from signer
- [x] Create EthereumWallet from signer
- [x] Create provider with wallet attached
- [x] Call deploy_vault() with provider and wallet address
- [x] Return LocalEvm struct

### LocalEvm::deploy_vault() Implementation

Follow lib/ethgild/script/Deploy.sol and
lib/ethgild/test/lib/LibOffchainAssetVaultCreator.sol:

- [x] Deploy Receipt implementation
- [x] Deploy CloneFactory
- [x] Deploy OffchainAssetReceiptVault implementation with
      ReceiptVaultConstructionConfigV2
- [x] Deploy OffchainAssetReceiptVaultAuthorizerV1 implementation
- [x] Encode vault init data: (admin, (asset, name, symbol))
- [x] Call factory.clone(vault_impl, init_data).from(admin).send()
- [x] Parse NewClone event to get vault address
- [x] Encode authorizer init data: (admin)
- [x] Call factory.clone(authorizer_impl, init_data).from(admin).send()
- [x] Parse NewClone event to get authorizer address
- [x] Grant DEPOSIT role: authorizer.grantRole(keccak256("DEPOSIT"),
      admin).from(admin).send()
- [x] Set authorizer: vault.setAuthorizer(authorizer_address).from(admin).send()
- [x] Return vault and authorizer addresses

## Task 3. Verify Deployment and Authorization

**Desired state**: Vault deploys successfully with admin having DEPOSIT/WITHDRAW
roles, enabling on-chain minting.

- [x] Run e2e mint test
- [x] Verify deployment completes without errors
- [x] Verify deposit transaction succeeds
- [x] Verify Deposit event is emitted with correct data

## Task 4. Verify E2E Mint Flow

**Desired state**: E2E mint test passes with on-chain deposit executing
correctly.

- [x] Run cargo test --test e2e_mint_flow
- [x] Verify complete mint flow with non-zero shares balance

## Task 5. Verify E2E Redemption Flow

**Desired state**: E2E redemption test infrastructure is in place with WebSocket
monitoring working.

- [x] Run cargo test --test e2e_redemption_flow
- [x] Verify WebSocket connection and subscription work
- [x] Verify Transfer events are detected and decoded
- [ ] Fix vault address mismatch between LocalEvm and tokenized_asset seeds (out
      of scope for initial infrastructure setup)

**Known Issue**: Test currently fails because LocalEvm creates a vault with a
dynamic address, but tokenized_asset seeds use hardcoded addresses. The
RedemptionDetector correctly detects and decodes the Transfer event but can't
find the corresponding asset. This needs to be addressed in a follow-up task.

## Success Criteria

1. Config, initialize_rocket(), and bindings are publicly accessible
2. LocalEvm deploys vault contracts with proper authorization using WebSocket
3. E2E mint test passes with on-chain minting
4. Shares balance increases after successful mint
5. E2E redemption test demonstrates WebSocket monitoring works
