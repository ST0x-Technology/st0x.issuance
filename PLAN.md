# Implementation Plan for Issue #19: Implement AlpacaService and mint callback

This plan implements the Alpaca callback integration, which is the final piece
of the mint flow. After tokens are minted on-chain, we need to notify Alpaca by
calling their callback endpoint, then mark the mint as complete.

## Overview

According to SPEC.md, the complete mint flow is:

1. AP requests mint → Alpaca calls our `/inkind/issuance` endpoint ✅ (Issue
   #15)
2. We validate and respond with `issuer_request_id` ✅ (Issue #15)
3. Alpaca journals shares from AP to our custodian account (Alpaca's
   responsibility)
4. Alpaca confirms journal → we receive `/inkind/issuance/confirm` ✅ (Issue
   #17)
5. We mint tokens on-chain via `vault.deposit()` ✅ (Issue #18)
6. **We call Alpaca's callback endpoint** ⬅️ THIS ISSUE
7. **Mint flow completed** ⬅️ THIS ISSUE

## Architecture Pattern

Following the same pattern as `BlockchainService`:

- Define `AlpacaService` trait for testability
- Create mock implementation for testing
- Create real implementation using `reqwest` HTTP client (the `apca` crate does
  NOT support tokenization endpoints - it only covers trading API)
- Add `reqwest` dependency with JSON features
- Add `RecordCallback` command to `Mint` aggregate
- Add `MintCompleted` event (single event, not two)
- Create `CallbackManager` to orchestrate the callback flow
- Wire up event listening: `TokensMinted` → call `send_mint_callback()` →
  execute `RecordCallback`
- Add Alpaca configuration to `Config` struct following the blockchain service
  pattern

## Task 1. Define AlpacaService trait, types, and mock implementation

Add a new module `src/alpaca/mod.rs` following the pattern from
`src/blockchain/mod.rs`. This task sets up the complete service interface and
test infrastructure.

**Files to create:**

- `src/alpaca/mod.rs` - Module definition with trait, types, and re-exports
- `src/alpaca/mock.rs` - Mock implementation for testing

**AlpacaService trait:**

```rust
#[async_trait]
pub(crate) trait AlpacaService: Send + Sync {
    async fn send_mint_callback(
        &self,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError>;
}
```

**MintCallbackRequest:** According to SPEC.md Step 5, the callback request
contains:

```rust
pub(crate) struct MintCallbackRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) client_id: ClientId,
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
    pub(crate) network: Network,
}
```

**AlpacaError:**

```rust
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaError {
    #[error("HTTP request failed: {message}")]
    Http { message: String },

    #[error("Authentication failed: {reason}")]
    Auth { reason: String },

    #[error("API error: {status_code} - {message}")]
    Api { status_code: u16, message: String },
}
```

**MockAlpacaService:** Following the pattern from `src/blockchain/mock.rs`,
create a mock implementation that can be configured to succeed or fail.

```rust
pub(crate) struct MockAlpacaService {
    should_succeed: bool,
    error_message: Option<String>,
    call_count: Arc<Mutex<usize>>,
}

impl MockAlpacaService {
    pub(crate) fn new_success() -> Self { ... }
    pub(crate) fn new_failure(error_message: impl Into<String>) -> Self { ... }
    pub(crate) fn get_call_count(&self) -> usize { ... }
}
```

**Subtasks:**

- [ ] Add `reqwest` dependency via `cargo add`
- [ ] Create `src/alpaca/mod.rs` with trait definition and types
- [ ] Add `AlpacaError` enum with proper error variants
- [ ] Add `MintCallbackRequest` struct matching SPEC.md format
- [ ] Add serde rename attributes and custom serializers for Alloy types
- [ ] Create `src/alpaca/mock.rs` with `MockAlpacaService`
- [ ] Implement `new_success()` and `new_failure()` constructors
- [ ] Implement `AlpacaService` trait for mock
- [ ] Add call counting for verification in tests
- [ ] Add tests for mock service (success, failure, call tracking)
- [ ] Add tests for JSON serialization of `MintCallbackRequest`
- [ ] Update `src/main.rs` to include `mod alpaca;`

## Task 2. Add RecordCallback command

Extend the `Mint` aggregate to handle the callback recording.

**Command:**

```rust
RecordCallback {
    issuer_request_id: IssuerRequestId,
}
```

**Event mapping:**

- `RecordCallback` → produces `MintCompleted` (single event)
  - Callback success means the mint is complete - these are the same domain
    fact, not separate state transitions
  - Follows DDD/ES principle: one command handles one domain fact = one event

**Subtasks:**

- [x] Add `RecordCallback` variant to `MintCommand` enum in `src/mint/cmd.rs`
- [x] Add `MintCompleted` variant to `MintEvent` enum in `src/mint/event.rs`
- [x] Ensure event includes `issuer_request_id` and `completed_at` timestamp

## Task 3. Add Completed state to Mint aggregate

The mint flow needs a terminal success state after callback completes.

**New state:**

```rust
Completed {
    issuer_request_id: IssuerRequestId,
    tokenization_request_id: TokenizationRequestId,
    quantity: Quantity,
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    client_id: ClientId,
    wallet: Address,
    initiated_at: DateTime<Utc>,
    journal_confirmed_at: DateTime<Utc>,
    tx_hash: B256,
    receipt_id: U256,
    shares_minted: U256,
    gas_used: u64,
    block_number: u64,
    minted_at: DateTime<Utc>,
    callback_sent_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
}
```

**Subtasks:**

- [x] Add `Completed` variant to `Mint` enum in `src/mint/mod.rs`
- [x] Update `state_name()` method to include `Completed` state
- [x] Add `handle_record_callback()` method that validates state is
      `CallbackPending`
- [x] Implement event handler that produces `MintCompleted` event (single event)
- [x] Add `apply_mint_completed()` method that transitions `CallbackPending` →
      `Completed`
- [x] Update `Aggregate::handle()` match to call `handle_record_callback()`
- [x] Update `Aggregate::apply()` match to handle `MintCompleted` event

## Task 4. Add tests for RecordCallback command

Following TDD and the testing patterns from `src/mint/mod.rs::tests`.

**Test cases:**

- [x] `test_record_callback_from_callback_pending_state` - Happy path
- [x] `test_record_callback_from_wrong_state_fails` - Error case
- [x] `test_record_callback_with_mismatched_issuer_request_id_fails` -
      Validation
- [x] `test_apply_mint_completed_event_updates_state` - Event application
- [x] Integration test showing full flow: `Initiated` → `JournalConfirmed` →
      `CallbackPending` → `Completed`

## Task 5. Update MintView to handle new events

The view needs to reflect callback and completion status.

**View changes:**

- Listen to `MintCompleted` event - update status to "completed" and add
  timestamp

**Subtasks:**

- [x] Update `MintView::update()` in `src/mint/view.rs` to handle
      `MintCompleted`
- [x] Add `completed_at` timestamp field to view payload
- [x] Update tests in view module to verify event is processed correctly

## Task 6. Create CallbackManager

Following the pattern from `MintManager` in `src/mint/manager.rs`.

**Manager responsibilities:**

1. React to `TokensMinted` event (or `CallbackPending` state)
2. Extract necessary data from aggregate
3. Call `AlpacaService::send_mint_callback()`
4. Execute `RecordCallback` command to record result

**Implementation:**

```rust
pub(crate) struct CallbackManager<ES: EventStore<Mint>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Mint, ES>>,
}

impl<ES: EventStore<Mint>> CallbackManager<ES> {
    pub(crate) async fn handle_tokens_minted(
        &self,
        issuer_request_id: &IssuerRequestId,
        aggregate: &Mint,
    ) -> Result<(), ManagerError> {
        // 1. Validate aggregate is in CallbackPending state
        // 2. Build MintCallbackRequest from aggregate data
        // 3. Call alpaca_service.send_mint_callback()
        // 4. Execute RecordCallback command
        // 5. Handle errors with proper logging
    }
}
```

**Subtasks:**

- [x] Create `src/mint/callback_manager.rs`
- [x] Implement `CallbackManager` struct with `new()` and
      `handle_tokens_minted()` methods
- [x] Add proper error handling and logging (use `tracing::info!` and
      `tracing::warn!`)
- [x] Define `CallbackManagerError` enum (can reuse or extend `ManagerError`)
- [x] Update `src/mint/mod.rs` to
      `pub(crate) use callback_manager::CallbackManager;`

## Task 7. Add tests for CallbackManager

Following the testing pattern from `src/mint/manager.rs::tests`.

**Test cases:**

- [ ] `test_handle_tokens_minted_with_success` - Callback succeeds
- [ ] `test_handle_tokens_minted_with_alpaca_failure` - Callback fails but error
      is recorded
- [ ] `test_handle_tokens_minted_with_wrong_state_fails` - Validates state

**Test setup:**

- Use `MemStore` for in-memory event store
- Create mint in `CallbackPending` state (requires executing `Initiate`,
  `ConfirmJournal`, `RecordMintSuccess`)
- Use `MockAlpacaService` configured for success/failure
- Verify aggregate transitions to `Completed` state on success

## Task 8. Wire CallbackManager into main event loop

The manager needs to be invoked when `TokensMinted` events occur. This wiring
will happen in `main.rs` or a dedicated orchestration module.

**Integration points:**

- After `TokensMinted` event is committed, load the aggregate
- Call `CallbackManager::handle_tokens_minted()`
- Handle errors (retry logic can be added later)

**Note:** The exact wiring mechanism depends on how event listeners are
structured in the app. This may require:

- Setting up event subscribers/listeners
- Or polling for mints in `CallbackPending` state
- Or using the CQRS framework's query/view mechanism to trigger actions

**Subtasks:**

- [ ] Identify where to hook into event flow (likely in `main.rs` setup)
- [ ] Create `CallbackManager` instance with real/mock `AlpacaService`
- [ ] Wire up event listener or polling mechanism
- [ ] Add proper error handling and logging
- [ ] Document the wiring approach in code comments

## Task 9. Add Alpaca configuration to Config struct

Following the pattern from `create_blockchain_service()` in `src/config.rs`.

**Configuration fields to add:**

```rust
#[arg(
    long,
    env = "ALPACA_API_BASE_URL",
    default_value = "https://broker-api.alpaca.markets",
    help = "Alpaca API base URL"
)]
alpaca_api_base_url: String,

#[arg(
    long,
    env = "ALPACA_ACCOUNT_ID",
    help = "Alpaca tokenization account ID"
)]
alpaca_account_id: Option<String>,

#[arg(long, env = "ALPACA_API_KEY", help = "Alpaca API key ID")]
alpaca_api_key: Option<String>,

#[arg(long, env = "ALPACA_API_SECRET", help = "Alpaca API secret key")]
alpaca_api_secret: Option<String>,
```

**Factory method:**

```rust
pub(crate) fn create_alpaca_service(
    &self,
) -> Result<Arc<dyn AlpacaService>, ConfigError> {
    let account_id = self.alpaca_account_id.as_ref()
        .ok_or(ConfigError::MissingAlpacaAccountId)?;
    let api_key = self.alpaca_api_key.as_ref()
        .ok_or(ConfigError::MissingAlpacaApiKey)?;
    let api_secret = self.alpaca_api_secret.as_ref()
        .ok_or(ConfigError::MissingAlpacaApiSecret)?;

    Ok(Arc::new(RealAlpacaService::new(
        self.alpaca_api_base_url.clone(),
        account_id.clone(),
        api_key.clone(),
        api_secret.clone(),
    )))
}
```

**Subtasks:**

- [ ] Add Alpaca config fields to `Config` struct in `src/config.rs`
- [ ] Add corresponding `ConfigError` variants for missing Alpaca config
- [ ] Implement `create_alpaca_service()` factory method
- [ ] Update `.env.example` or documentation with required env vars

## Task 10. Implement RealAlpacaService

Concrete HTTP-based implementation using `reqwest`.

**Service structure:**

```rust
pub(crate) struct RealAlpacaService {
    client: reqwest::Client,
    base_url: String,
    account_id: String,
    api_key: String,
    api_secret: String,
}
```

**Implementation details:**

```rust
impl RealAlpacaService {
    pub(crate) fn new(
        base_url: String,
        account_id: String,
        api_key: String,
        api_secret: String,
    ) -> Self {
        let client = reqwest::Client::new();
        Self { client, base_url, account_id, api_key, api_secret }
    }
}

#[async_trait]
impl AlpacaService for RealAlpacaService {
    async fn send_mint_callback(
        &self,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        // Endpoint: POST /v1/accounts/{account_id}/tokenization/callback/mint
        let url = format!(
            "{}/v1/accounts/{}/tokenization/callback/mint",
            self.base_url,
            self.account_id
        );

        // Use Basic Auth with API key as username and secret as password
        // (Alpaca API pattern per their documentation)
        let response = self.client
            .post(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .json(&request)
            .send()
            .await
            .map_err(|e| AlpacaError::HttpError {
                message: e.to_string()
            })?;

        match response.status() {
            reqwest::StatusCode::OK => Ok(()),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                Err(AlpacaError::AuthError {
                    reason: "Authentication failed".to_string()
                })
            }
            status => {
                let message = response.text().await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(AlpacaError::ApiError {
                    status_code: status.as_u16(),
                    message,
                })
            }
        }
    }
}
```

**JSON serialization for MintCallbackRequest:**

Per SPEC.md, the JSON payload should be:

```json
{
  "tokenization_request_id": "12345-678-90AB",
  "client_id": "5505-1234-ABC-4G45",
  "wallet_address": "0x...",
  "tx_hash": "0x12345678",
  "network": "base"
}
```

Use serde rename to map Rust field names to JSON snake_case:

```rust
#[derive(Debug, Clone, Serialize)]
pub(crate) struct MintCallbackRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) client_id: ClientId,
    #[serde(serialize_with = "serialize_address")]
    pub(crate) wallet_address: Address,
    #[serde(serialize_with = "serialize_b256")]
    pub(crate) tx_hash: B256,
    pub(crate) network: Network,
}

// Custom serializers for alloy types
fn serialize_address<S>(addr: &Address, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&format!("{addr:#x}"))
}

fn serialize_b256<S>(hash: &B256, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&format!("{hash:#x}"))
}
```

**Subtasks:**

- [ ] Implement `RealAlpacaService` struct in `src/alpaca/service.rs`
- [ ] Add constructor taking base_url, account_id, api_key, api_secret
- [ ] Implement `send_mint_callback()` with POST request to correct endpoint
- [ ] Use Basic Auth with `basic_auth()` method
- [ ] Handle response status codes: 200 OK, 401/403 auth errors, other errors
- [ ] Add custom serializers for `Address` and `B256` types to format as hex
      strings
- [ ] Add unit tests using `httpmock` or similar for HTTP client testing
- [ ] Add integration test that verifies JSON serialization format matches
      SPEC.md

## Task 11. Integration test for complete mint flow

Create an end-to-end test that exercises the entire mint flow from initiation
through callback completion.

**Test flow:**

1. Execute `InitiateMint` command → `Initiated` state
2. Execute `ConfirmJournal` command → `JournalConfirmed` state
3. `MintManager` mints tokens → `CallbackPending` state
4. `CallbackManager` sends callback → `Completed` state
5. Verify all events were produced and view was updated

**Subtasks:**

- [ ] Create integration test in `src/mint/mod.rs::tests` or separate
      integration test file
- [ ] Use `MockBlockchainService` and `MockAlpacaService`
- [ ] Exercise full flow with both managers
- [ ] Verify final aggregate state is `Completed`
- [ ] Verify `MintView` shows status as "completed" with all timestamps

## Task 12. Update documentation

Ensure all changes are properly documented.

**Subtasks:**

- [ ] Add doc comments to `AlpacaService` trait and methods
- [ ] Add doc comments to `CallbackManager` and its methods
- [ ] Update SPEC.md if any implementation details diverge from spec
- [ ] Add inline comments explaining manager wiring

## Task 13. Run checks before completion

Ensure code quality and all tests pass.

**Subtasks:**

- [ ] Run `cargo test --workspace` - all tests pass
- [ ] Run
      `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` -
      no warnings
- [ ] Run `cargo fmt --all` - code properly formatted
- [ ] Review all changes for adherence to AGENTS.md guidelines
- [ ] Verify no panics (`unwrap()`, `expect()`) in non-test code
- [ ] Verify proper error handling throughout

## Dependencies and Order

Tasks must be completed in order:

- Task 1 (AlpacaService infrastructure including mock) before Task 6
  (CallbackManager)
- Tasks 2-4 (Aggregate changes and tests) before Task 6 (CallbackManager)
- Task 5 (View updates) can be done in parallel with Task 6
- Tasks 6-7 (CallbackManager implementation and tests) before Task 8 (wiring)
- Task 9 (Config) and Task 10 (RealAlpacaService) can be done after Task 1
- Task 8 (wiring) before Task 11 (integration test)
- Tasks 12-13 (docs and checks) at the end

## Technical Decisions

### HTTP Client: reqwest

The `apca` crate does NOT support Alpaca's tokenization endpoints - it only
covers the standard trading API (orders, positions, market data). We must use
`reqwest` directly.

**Rationale:**

- Tokenization endpoints are part of Alpaca's ITN (Instant Tokenization Network)
- The `apca` crate documentation shows no tokenization support
- We need: `POST /v1/accounts/{account_id}/tokenization/callback/mint`
- `reqwest` is the standard Rust HTTP client, well-maintained, async-compatible
  with tokio

**Version:** `reqwest = { version = "0.12", features = ["json"] }`

### Authentication: Basic Auth

Per Alpaca API patterns, use HTTP Basic Authentication:

- Username: API key ID
- Password: API secret key
- `reqwest` supports this via `.basic_auth(username, Some(password))`

### JSON Serialization

Custom serializers needed for alloy types:

- `Address` → hex string with `0x` prefix: `format!("{addr:#x}")`
- `B256` → hex string with `0x` prefix: `format!("{hash:#x}")`

All other fields serialize naturally via serde derives.

## Notes

- Follow the same architectural patterns as `BlockchainService` and
  `MintManager`
- Keep aggregates pure - no I/O in command handlers, all external interactions
  via managers
- Maintain complete test coverage for all business logic
- `RecordCallback` produces single `MintCompleted` event following DDD/ES
  principles (one command = one domain fact = one event)
- The `Completed` state is the terminal success state for the mint flow
- Error handling: callback failures should be logged but not crash the system
  (can add retry logic later)
- Use `reqwest::Client::new()` instead of building with timeout - can add
  timeout configuration later if needed
- Follow config pattern from `blockchain::service` for service creation
- Adhere to all guidelines in AGENTS.md, especially:
  - Zero tolerance for panics in non-test code
  - Package by feature (alpaca module for all Alpaca-related code)
  - Make invalid states unrepresentable (use enum states, not nullable fields)
  - Comprehensive error handling with Result types
