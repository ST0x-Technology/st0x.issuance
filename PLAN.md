# Plan: Migrate Key Signing to Fireblocks

Port the Fireblocks signer module from `st0x.liquidity` to `st0x.issuance`, replacing the hardcoded `PRIVATE_KEY` with a runtime-selectable signer (Fireblocks or local).

## Task 1. Add fireblocks module

- [x] Add `fireblocks-sdk` dependency to Cargo.toml
- [x] Create `src/fireblocks/mod.rs` with `SignerEnv`, `SignerConfig`, `ResolvedSigner`, and associated errors (ported from liquidity)
- [x] Create `src/fireblocks/config.rs` with `FireblocksEnv` struct
- [x] Create `src/fireblocks/signer.rs` with `FireblocksSigner` implementing alloy `Signer` and `TxSigner` traits
- [x] Register `mod fireblocks` in `src/lib.rs`

## Task 2. Refactor Config to use SignerConfig

- [x] Replace `private_key: B256` field in `Config` with `signer: SignerConfig` (from the new fireblocks module)
- [x] Replace `private_key: B256` in `Env` with `#[clap(flatten)] signer: SignerEnv`
- [x] Update `Env::into_config()` to call `signer.into_config()?` (now fallible)
- [x] Update `Config::parse()` to handle the new error from signer config validation
- [x] Change `Config::bot_wallet()` to `async` — calls `self.signer.address().await`
- [x] Change `Config::create_blockchain_service()` to use `self.signer.resolve().await` for wallet+provider
- [x] Update `ConfigError` enum: remove `InvalidPrivateKey`/`InvalidPrivateKeyFormat`, add signer-related variants
- [x] Update all callers of `config.bot_wallet()` in `lib.rs` (already async context, just add `.await`)

## Task 3. Update test infrastructure

- [x] Update `test_config()` in `src/test_utils.rs`: use `SignerConfig::Local(B256::ZERO)` instead of `private_key: B256::ZERO`
- [x] Update E2E tests in `tests/e2e.rs`: construct `Config` with `signer: SignerConfig::Local(evm.private_key)`
- [x] Update config tests in `src/config.rs`: adjust `minimal_args()` to use `--evm-private-key` instead of `--private-key`
- [x] Update remaining test modules (account/api, tokenized_asset/api, auth, mint/api/test_utils)

## Design Decisions

- **Exact port from st0x.liquidity**: The fireblocks module is copied as-is to maintain consistency across our repos. Same `SignerEnv`/`SignerConfig`/`FireblocksSigner` pattern.
- **Env var rename**: `PRIVATE_KEY` → `EVM_PRIVATE_KEY` for consistency with st0x.liquidity and to make the mutual exclusivity with `FIREBLOCKS_API_KEY` clear.
- **`bot_wallet()` becomes async**: Because Fireblocks requires an API call to resolve the address. This is fine since all callers are already in async contexts.
- **ChainAssetIds**: Uses the updated `ChainAssetIds` mapping (e.g. "8453:BASECHAIN_ETH") instead of a single `asset_id` string, matching the latest st0x.liquidity.
