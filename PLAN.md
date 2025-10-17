# Implementation Plan: AddAsset Feature (Issue #13)

This plan implements the AddAsset feature with full ES/CQRS support, following
the same module structure as `src/account/`.

## Task 1. Create TokenizedAssetCommand

**File:** `src/tokenized_asset/cmd.rs`

- [x] Create `TokenizedAssetCommand` enum with `AddAsset` variant
- [x] Fields: `underlying: UnderlyingSymbol`, `token: TokenSymbol`,
      `network: Network`, `vault_address: VaultAddress`
- [x] Derive `Debug`, `Clone`, `Serialize`, `Deserialize`

## Task 2. Create TokenizedAssetEvent and DomainEvent Implementation

**File:** `src/tokenized_asset/event.rs`

- [x] Create `TokenizedAssetEvent` enum with `AssetAdded` variant
- [x] Fields: Same as command + `added_at: DateTime<Utc>`
- [x] Implement `DomainEvent` trait (event_type = "AssetAdded", event_version =
      "1.0")
- [x] Derive `Debug`, `Clone`, `Serialize`, `Deserialize`, `PartialEq`

## Task 3. Create VaultAddress Newtype and TokenizedAsset Aggregate

**File:** `src/tokenized_asset/mod.rs`

- [ ] Create `VaultAddress` newtype (String wrapper)
- [ ] Create `TokenizedAsset` enum:
  - `NotAdded` variant (initial state)
  - `Added` variant with fields: underlying, token, network, vault_address,
    enabled, added_at
- [ ] Implement `Default` trait (returns `NotAdded`)
- [ ] Implement `Aggregate` trait:
  - `aggregate_type()` returns "TokenizedAsset"
  - `handle()` validates and produces `AssetAdded` event
  - `apply()` transitions from `NotAdded` to `Added` (with enabled = true)
- [ ] Create `TokenizedAssetError` enum
- [ ] Add aggregate tests (Given-When-Then pattern)
- [ ] Update module declarations and re-exports

## Task 4. Create TokenizedAssetView

**File:** `src/tokenized_asset/view.rs`

- [ ] Create `TokenizedAssetView` struct with all asset fields
- [ ] Implement `Default` trait (placeholder values)
- [ ] Implement `View<TokenizedAsset>` trait with `update()` method
- [ ] Create
      `list_enabled_assets(pool: &Pool<Sqlite>) -> Result<Vec<TokenizedAssetView>>`
      query function
- [ ] Create `TokenizedAssetViewError` enum
- [ ] Add view tests (update from event, query functions)

## Task 5. Create Database Migration

**File:** `migrations/YYYYMMDDHHMMSS_create_tokenized_asset_view.sql`

- [ ] Run `sqlx migrate add create_tokenized_asset_view`
- [ ] Create `tokenized_asset_view` table (view_id, version, payload)
- [ ] Add indexes for enabled and network JSON fields

## Task 6. Update Endpoint to Query View

**File:** `src/tokenized_asset/endpoint.rs`

- [ ] Update `list_tokenized_assets()` to accept `State<Pool<Sqlite>>`
- [ ] Call `list_enabled_assets()` from view module
- [ ] Map `TokenizedAssetView` to response DTO
- [ ] Handle errors properly
- [ ] Update/add tests for database integration

## Task 7. Wire Up CQRS Framework in main.rs

**File:** `src/main.rs`

- [ ] Import TokenizedAsset, TokenizedAssetView
- [ ] Create type alias `TokenizedAssetCqrs`
- [ ] Create view repository, query, and CQRS instance
- [ ] Add to `.manage()` chain
- [ ] Implement `seed_initial_assets()` function in main.rs
- [ ] Call seeding after CQRS setup, before launching Rocket

## Task 8. Run All Checks

- [ ] Run `cargo test --workspace` - all tests pass
- [ ] Run
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` -
      no warnings
- [ ] Run `cargo fmt` - code properly formatted

## Design Decisions

### Module Structure

- Follow `src/account/` pattern exactly
- cmd.rs, event.rs, view.rs, endpoint.rs submodules
- Aggregate and main types in mod.rs
- Seeding logic in main.rs (simple startup function)

### Aggregate ID

- Use `underlying` symbol as aggregate_id
- Natural key for lookups

### Default Enabled Status

- New assets default to `enabled: true`
- Can add EnableAsset/DisableAsset commands later if needed

### Seeding

- Simple function in main.rs that executes AddAsset commands
- Idempotent - gracefully handles already-added assets
- Seeds AAPL, TSLA, NVDA on every startup

### Type Safety

- Simple newtypes (no phantom types)
- VaultAddress, UnderlyingSymbol, TokenSymbol, Network all independent types
