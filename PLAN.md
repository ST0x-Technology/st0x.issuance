# Plan: Fix E2E Test Authorization Issue

## Problem Statement

The e2e mint test fails with `Unauthorized` error (0xfe720f78) from the vault's
authorizer contract. The error data shows `address(0)` as the transaction
sender, indicating the deposit transaction is not being sent from the wallet
that has the DEPOSIT role.

After reverting changes with `git checkout`, the LocalEvm deployment code was
lost from test_utils.rs, and several compilation errors exist due to missing
public APIs.

## Root Cause Analysis

The authorization error occurs because:

1. The vault's OffchainAssetReceiptVaultAuthorizerV1 enforces role-based access
   control
2. DEPOSIT and WITHDRAW roles must be granted to specific addresses
3. Transactions must be sent FROM an address that has these roles
4. Currently the transaction sender is showing as `address(0)`, which has no
   roles

The correct flow should be:

1. Deploy vault with admin wallet
2. Grant DEPOSIT/WITHDRAW roles to admin wallet
3. Configure application to use admin wallet's private key
4. Provider with wallet attached should automatically sign transactions from
   that wallet's address

## Design Decisions

**Why restore public API first**: E2E tests need to construct Config and call
initialize_rocket() with it, which requires making Config public and changing
initialize_rocket() signature.

**Why LocalEvm manages full deployment lifecycle**: Following Rust ownership
principles, LocalEvm owns the AnvilInstance, ensuring the local blockchain stays
alive for the test duration. It also encapsulates all deployment complexity.

**Why follow Solidity test deployment pattern exactly**: The Solidity tests in
lib/ethgild/test/ are the canonical reference for how to deploy these contracts.
Following them exactly ensures we deploy correctly with proper initialization.

**Why not modify RealBlockchainService**: The service is an implementation
detail that should work with any properly configured provider. The issue is with
how the provider is configured, not with the service itself. When a provider has
a wallet attached via `.wallet()`, Alloy automatically uses that wallet's
address as the transaction sender.

## Task 1. Restore Public API for E2E Tests

E2E tests need access to Config, initialize_rocket(), and contract bindings.

- [ ] Make Config struct public (change `pub(crate)` to `pub`)
- [ ] Make all Config fields public
- [ ] Change initialize_rocket() signature to accept Config parameter instead of
      parsing it internally
- [ ] Make alpaca module public and re-export AlpacaConfig
- [ ] Make vault module and VaultService trait visible to tests
- [ ] Make bindings module public so tests can use OffchainAssetReceiptVault
      type
- [ ] Update src/main.rs to call Config::parse() before passing to
      initialize_rocket()
- [ ] Verify compilation succeeds

**Reasoning**: E2E tests live in tests/ directory and cannot access pub(crate)
items. Tests need to construct Config with test-specific values (in-memory
database, mock Alpaca, local RPC endpoint, test private key). Making
initialize_rocket() accept Config allows dependency injection for testing while
keeping the production path simple (main.rs parses from env vars).

## Task 2. Implement LocalEvm Deployment Infrastructure

Provide a LocalEvm helper that spawns Anvil and deploys the complete vault
contract setup.

- [ ] Add required imports to test_utils.rs (alloy node_bindings, hex, signers,
      etc.)
- [ ] Define LocalEvmError enum with variants for all error cases
- [ ] Define LocalEvm struct with fields:
  - `_anvil: AnvilInstance` (underscore because it's held only to keep Anvil
    alive)
  - `pub vault_address: Address`
  - `pub wallet_address: Address`
  - `pub private_key: String` (hex string without 0x prefix)
  - `pub endpoint: String` (WebSocket URL)
- [ ] Implement LocalEvm::new() method
- [ ] Implement LocalEvm::deploy_vault() method
- [ ] Implement LocalEvm::vault_service() method for creating VaultService

**Reasoning**: LocalEvm encapsulates the complexity of spinning up a local
blockchain and deploying the full contract setup. Tests simply call
LocalEvm::new() and get a ready-to-use environment. The struct owns
AnvilInstance via `_anvil` field, so Anvil stays running until LocalEvm is
dropped. Exposing private_key and endpoint allows tests to configure the
application to connect to the same blockchain.

### LocalEvm::new() Implementation

- [ ] Spawn Anvil instance
- [ ] Get WebSocket endpoint from Anvil
- [ ] Get first Anvil pre-funded key
- [ ] Hex-encode the private key (without 0x prefix)
- [ ] Create PrivateKeySigner from the key
- [ ] Get wallet address from signer
- [ ] Create EthereumWallet from signer
- [ ] Create provider with wallet attached using ProviderBuilder
- [ ] Call deploy_vault() with provider and wallet address
- [ ] Return LocalEvm struct with all information

**Reasoning**: Anvil provides pre-funded test accounts. We use the first one as
the admin wallet that will deploy contracts and receive DEPOSIT/WITHDRAW roles.
The provider must have the wallet attached so all deployment transactions are
automatically signed.

### LocalEvm::deploy_vault() Implementation

Follow the exact pattern from
lib/ethgild/test/lib/LibOffchainAssetVaultCreator.sol:

- [ ] Deploy Receipt implementation contract
- [ ] Deploy CloneFactory
- [ ] Deploy OffchainAssetReceiptVault implementation with
      ReceiptVaultConstructionConfigV2
- [ ] Deploy OffchainAssetReceiptVaultAuthorizerV1 implementation
- [ ] Encode vault initialization data (admin, (asset, name, symbol))
- [ ] Call factory.clone() with vault implementation and init data, using
      .from(admin).send()
- [ ] Parse NewClone event from transaction logs to get vault address
- [ ] Encode authorizer initialization data (admin)
- [ ] Call factory.clone() with authorizer implementation and init data, using
      .from(admin).send()
- [ ] Parse NewClone event to get authorizer address
- [ ] Create authorizer contract instance
- [ ] Grant DEPOSIT role to admin using keccak256("DEPOSIT"),
      .from(admin).send()
- [ ] Grant WITHDRAW role to admin using keccak256("WITHDRAW"),
      .from(admin).send()
- [ ] Call vault.setAuthorizer(authorizer_address) using .from(admin).send()
- [ ] Return vault address

**Reasoning**: This follows the OpenZeppelin Initializable pattern.
Implementation contracts cannot be used directly - they must be cloned via
factory which calls initialize(). The factory.clone() function deploys a minimal
proxy that delegates to the implementation and calls initialize() in the same
transaction. We MUST use .from(admin).send() on every transaction to ensure the
admin wallet signs them. We parse the NewClone event because the vault address
is not predictable - the factory creates it. Roles must be granted BEFORE the
vault can be used. The DEPOSIT and WITHDRAW role IDs are keccak256 hashes of the
role names.

### LocalEvm::vault_service() Implementation

- [ ] Parse private_key string to PrivateKeySigner
- [ ] Create EthereumWallet from signer
- [ ] Create provider with wallet attached
- [ ] Connect to self.endpoint
- [ ] Create RealBlockchainService with provider and vault_address
- [ ] Wrap in Arc<dyn VaultService> and return

**Reasoning**: This creates a VaultService that connects to the deployed Anvil
contracts. The provider must have the wallet attached so deposit/withdraw
transactions are signed by the admin wallet which has the necessary roles.

## Task 3. Verify Authorization Works Correctly

Ensure the deployed vault configuration allows the admin wallet to perform
deposits.

- [ ] Add debug logging to deployment to print admin address and vault address
- [ ] Add check after granting roles to verify hasRole returns true
- [ ] Run e2e test and verify deployment succeeds
- [ ] Verify deposit transaction is sent from correct address (not address(0))
- [ ] Verify deposit transaction succeeds and emits Deposit event

**Reasoning**: We need to verify each step: deployment succeeds, roles are
granted correctly, and transactions use the correct sender. If we still see
address(0), it means the provider isn't using the wallet, which would indicate a
deeper Alloy configuration issue.

## Task 4. Run and Verify E2E Tests

- [ ] Run cargo test --test e2e_mint_flow
- [ ] Verify mint flow completes: account link → mint initiate → journal confirm
      → on-chain deposit → callback
- [ ] Verify shares balance is non-zero after mint
- [ ] Run cargo test --test e2e_redemption_flow
- [ ] Verify redemption flow completes: detect transfer → call Alpaca → poll
      status → burn tokens
- [ ] Verify shares balance decreases after redemption

**Reasoning**: Both tests exercise the full stack with real on-chain
transactions. Success proves the authorization is working correctly and the
application can interact with the vault contracts as intended.

## Success Criteria

1. All compilation errors resolved
2. LocalEvm successfully deploys vault with authorization configured
3. E2E mint test passes with on-chain deposit succeeding
4. E2E redemption test passes with on-chain burn succeeding
5. No authorization errors in logs
