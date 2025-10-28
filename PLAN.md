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

- [ ] Make Config struct public
- [ ] Make all Config fields public
- [ ] Make alpaca module public and re-export AlpacaConfig
- [ ] Make bindings module public
- [ ] Change initialize_rocket() signature to accept Config parameter
- [ ] Update src/main.rs to call Config::parse() and pass to initialize_rocket()
- [ ] Verify compilation succeeds

## Task 2. Implement LocalEvm Deployment Infrastructure

**Desired state**: Tests call LocalEvm::new() to get a configured Anvil
environment with deployed vault contracts.

- [ ] Add required imports to test_utils.rs
- [ ] Define LocalEvmError enum
- [ ] Define LocalEvm struct:
  - `_anvil: AnvilInstance`
  - `pub vault_address: Address`
  - `pub wallet_address: Address`
  - `pub private_key: B256`
  - `pub endpoint: String`
- [ ] Implement LocalEvm::new() method
- [ ] Implement LocalEvm::deploy_vault() private helper method
- [ ] Implement LocalEvm::private_key_hex() method returning String

### LocalEvm::new() Implementation

- [ ] Spawn Anvil instance
- [ ] Get WebSocket endpoint
- [ ] Get first Anvil pre-funded key as B256
- [ ] Create PrivateKeySigner from key
- [ ] Get wallet address from signer
- [ ] Create EthereumWallet from signer
- [ ] Create provider with wallet attached
- [ ] Call deploy_vault() with provider and wallet address
- [ ] Return LocalEvm struct

### LocalEvm::deploy_vault() Implementation

Follow lib/ethgild/script/Deploy.sol and
lib/ethgild/test/lib/LibOffchainAssetVaultCreator.sol:

- [ ] Deploy Receipt implementation
- [ ] Deploy CloneFactory
- [ ] Deploy OffchainAssetReceiptVault implementation with
      ReceiptVaultConstructionConfigV2
- [ ] Deploy OffchainAssetReceiptVaultAuthorizerV1 implementation
- [ ] Encode vault init data: (admin, (asset, name, symbol))
- [ ] Call factory.clone(vault_impl, init_data).from(admin).send()
- [ ] Parse NewClone event to get vault address
- [ ] Encode authorizer init data: (admin)
- [ ] Call factory.clone(authorizer_impl, init_data).from(admin).send()
- [ ] Parse NewClone event to get authorizer address
- [ ] Grant DEPOSIT role: authorizer.grantRole(keccak256("DEPOSIT"),
      admin).from(admin).send()
- [ ] Grant WITHDRAW role: authorizer.grantRole(keccak256("WITHDRAW"),
      admin).from(admin).send()
- [ ] Set authorizer: vault.setAuthorizer(authorizer_address).from(admin).send()
- [ ] Return vault address

## Task 3. Verify Deployment and Authorization

**Desired state**: Vault deploys successfully with admin having DEPOSIT/WITHDRAW
roles, enabling on-chain minting.

- [ ] Run e2e mint test
- [ ] Verify deployment completes without errors
- [ ] Verify deposit transaction succeeds
- [ ] Verify Deposit event is emitted with correct data

## Task 4. Verify E2E Mint Flow

**Desired state**: E2E mint test passes with on-chain deposit executing
correctly.

- [ ] Run cargo test --test e2e_mint_flow
- [ ] Verify complete mint flow with non-zero shares balance

## Success Criteria

1. Config, initialize_rocket(), and bindings are publicly accessible
2. LocalEvm deploys vault contracts with proper authorization
3. E2E mint test passes with on-chain minting
4. Shares balance increases after successful mint
