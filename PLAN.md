# HyperDX Telemetry Integration Implementation Plan

This plan implements OpenTelemetry distributed tracing with HyperDX as the
observability backend. The implementation follows the exact pattern from
`../st0x.liquidity-b/` which is known to work correctly.

## Task 1. Add OpenTelemetry dependencies

Add the required OpenTelemetry and tracing dependencies to enable distributed
tracing with HyperDX.

- [x] Add `opentelemetry = "0.30.0"` to package dependencies
- [x] Add `opentelemetry_sdk = { version = "0.30.0", features = ["rt-tokio"] }`
      to package dependencies
- [x] Add `opentelemetry-otlp = "0.30.0"` to package dependencies
- [x] Add `tracing-opentelemetry = "0.31.0"` to package dependencies
- [x] Add `blocking` feature to existing `reqwest` dependency (required for OTLP
      exporter)
- [x] Add `tracing-subscriber` with `env-filter` feature to package dependencies

**Design Rationale:**

- OpenTelemetry 0.30.0 is the latest stable version compatible with the tokio
  runtime
- The `rt-tokio` feature enables tokio-specific integrations for the SDK
- The `blocking` reqwest feature is critical - `BatchSpanProcessor` spawns
  background threads outside the tokio runtime that require a blocking HTTP
  client
- Workspace dependencies ensure version consistency across the project

## Task 2. Create telemetry module

Create `src/telemetry.rs` by copying the exact implementation from
`../st0x.liquidity-b/src/telemetry.rs` with minimal adaptations for the issuance
bot.

- [ ] Copy `src/telemetry.rs` from liquidity-b project
- [ ] Update service name references from "st0x-hedge" to "st0x-issuance"
- [ ] Update crate name references in default filters from
      "st0x_hedge,st0x_broker" to "st0x_issuance"
- [ ] Keep all other implementation details identical (batch config, OTLP
      endpoint, error types, guard)

**Design Rationale:**

- The liquidity-b telemetry module is battle-tested and implements best
  practices
- Critical features preserved:
  - Blocking HTTP client created in separate thread to avoid tokio runtime
    panics
  - Batch processor configuration (512 spans/batch, 2048 queue size, 3s
    interval)
  - Dual-layer filtering (console + telemetry with independent filters)
  - TelemetryGuard with explicit flush and shutdown on drop
  - gRPC protocol for efficient trace export to HyperDX
- Only service/crate name changes needed for issuance bot context

## Task 3. Create env module with Env/Config types

Create `src/env.rs` module following the liquidity-b pattern where `Env` handles
parsing and `Config` has properly typed fields.

- [ ] Create `src/env.rs` file
- [ ] Create `LogLevel` enum with `Trace`, `Debug`, `Info`, `Warn`, `Error`
      variants and `#[derive(clap::ValueEnum, Debug, Clone)]`
- [ ] Implement `From<LogLevel>` and `From<&LogLevel>` for `tracing::Level`
- [ ] Create `HyperDxEnv` struct with `#[derive(Parser, Debug, Clone)]`:
  - `hyperdx_api_key: Option<String>` field
  - `hyperdx_service_name: String` field with default "st0x-issuance"
- [ ] Implement
      `HyperDxEnv::into_config(self, log_level: tracing::Level) -> Option<HyperDxConfig>`
      that returns Some if api_key is present
- [ ] Create `Env` struct with `#[derive(Parser, Debug, Clone)]` containing:
  - All fields from current `Config` in lib.rs (database_url, rpc_url,
    private_key, vault_address, alpaca fields, etc.)
  - `log_level: LogLevel` field with default "debug"
  - `#[clap(flatten)] hyperdx: HyperDxEnv` field
- [ ] Create `Config` struct without `#[derive(Parser)]` containing:
  - All fields from `Env` (database_url, rpc_url, etc.)
  - `log_level: LogLevel`
  - `hyperdx: Option<HyperDxConfig>` field
- [ ] Implement `Env::into_config(self) -> Result<Config, ConfigError>` that
      constructs Config from Env
- [ ] In `into_config`, call
      `self.hyperdx.into_config((&self.log_level).into())` to get
      `Option<HyperDxConfig>`
- [ ] Move `create_blockchain_service` method from lib.rs Config to env.rs
      Config
- [ ] Move `ConfigError` enum from lib.rs to env.rs
- [ ] Add `pub fn setup_tracing(log_level: &LogLevel)` function for console-only
      tracing fallback

**Design Rationale:**

- **Make invalid states unrepresentable**: `Option<HyperDxConfig>` ensures if
  HyperDX is enabled, ALL required fields (api_key, service_name, log_level) are
  present
- **Modular structure**: `HyperDxEnv` is a separate, flattened struct that
  handles its own conversion logic
- Separation of concerns: `Env` handles environment/CLI parsing (clap's job),
  `Config` represents validated configuration (application's job)
- Type safety: Can't accidentally have `Some(api_key)` with missing service_name
- Follows exact pattern from liquidity-b for consistency across st0x projects
- `into_config()` is the validation boundary where we construct properly typed
  configuration
- Uses `ConfigError` for any validation errors (existing variants for blockchain
  config, can add more if needed)

## Task 4. Update lib.rs to export env types and remove old Config

Update `src/lib.rs` to use the new env module and remove the old Config
definition.

- [ ] Add `pub(crate) mod env;` declaration
- [ ] Add `pub(crate) use env::{Env, Config, setup_tracing};` exports
- [ ] Remove old `Config` struct definition from lib.rs
- [ ] Remove old `ConfigError` enum from lib.rs
- [ ] Keep all existing type aliases (AccountCqrs, MintCqrs, etc.)
- [ ] Keep all existing function signatures that use `&Config` parameter

**Design Rationale:**

- Centralizes configuration in env module
- Clean separation of concerns: lib.rs handles application logic, env.rs handles
  configuration
- Public (crate-level) exports enable main.rs to use configuration types
- Existing code using `Config` continues to work with no changes

## Task 5. Update main.rs to use Env → Config pattern

Update `src/main.rs` to parse `Env`, convert to `Config`, and initialize
telemetry following the exact pattern from
`../st0x.liquidity-b/src/bin/server.rs`.

- [ ] Add imports: `use st0x_issuance::{Env, setup_tracing, initialize_rocket};`
- [ ] Add import for telemetry: `use st0x_issuance::telemetry::TelemetryGuard;`
- [ ] Remove existing `tracing_subscriber::registry()` initialization code
- [ ] Parse environment: `let env = Env::parse();`
- [ ] Convert to Config: `let config = env.into_config()?;`
- [ ] Set up telemetry with exact pattern from liquidity-b:
  ```rust
  let telemetry_guard = if let Some(ref hyperdx) = config.hyperdx {
      match hyperdx.setup_telemetry() {
          Ok(guard) => Some(guard),
          Err(e) => {
              eprintln!("Failed to setup telemetry: {e}");
              setup_tracing(&config.log_level);
              None
          }
      }
  } else {
      setup_tracing(&config.log_level);
      None
  };
  ```
- [ ] Call `let result = initialize_rocket(config).await;`
- [ ] Explicitly drop telemetry guard: `drop(telemetry_guard);`
- [ ] Return result: `result?; Ok(())`

**Design Rationale:**

- Telemetry must be initialized before any logging/tracing occurs
- Graceful degradation: if telemetry setup fails, fall back to console logging
  and continue
- Explicit `drop(telemetry_guard)` ensures pending spans flush before
  application exit
- Background threads in BatchSpanProcessor need proper shutdown for clean
  termination
- Config parsed once in main and passed through to avoid duplication
- Exact same flow as liquidity-b/src/bin/server.rs

## Task 6. Update initialize_rocket() to accept Config

Modify `initialize_rocket()` in `src/lib.rs` to accept `Config` as a parameter
instead of parsing internally.

- [ ] Change signature from `initialize_rocket()` to
      `initialize_rocket(config: Config)`
- [ ] Remove `let config = Config::parse()` line inside function
- [ ] Update all uses of config to use the parameter
- [ ] Ensure all helper functions (`create_pool`, `setup_mint_managers`, etc.)
      still receive `&Config`

**Design Rationale:**

- Config must be parsed in main.rs before telemetry initialization
- Passing config as parameter avoids double-parsing and maintains single source
  of truth
- Clean separation: main.rs handles environment/CLI → validated config, lib.rs
  handles application logic

## Task 7. Add telemetry exports to lib.rs

Export telemetry types from lib.rs for use in main.rs.

- [ ] Add `pub(crate) mod telemetry;` declaration
- [ ] Export telemetry types:
      `pub(crate) use telemetry::{TelemetryError, TelemetryGuard};`
- [ ] Verify exports are visible in main.rs

**Design Rationale:**

- Public (crate-level) exports enable main.rs to use telemetry types
- Exporting only essential types (error + guard) maintains clean API surface
- `pub(crate)` visibility keeps telemetry implementation details internal
- Main.rs should be able to import everything it needs directly from the crate
  root

## Task 8. Update .env.example with HyperDX configuration

Add HyperDX configuration variables to `.env.example` following the liquidity-b
pattern.

- [ ] Add comment block explaining HyperDX is optional
- [ ] Add `HYPERDX_API_KEY=${HYPERDX_API_KEY}` variable
- [ ] Add comment with link to https://www.hyperdx.io/ for obtaining API key
- [ ] Add comment that bot runs with console-only logging if not set
- [ ] Add `HYPERDX_SERVICE_NAME` as optional with note about default
      "st0x-issuance"
- [ ] Add `LOG_LEVEL` as optional with note about default "debug"
- [ ] Add `RUST_LOG` variable with note that it overrides default filters

**Design Rationale:**

- Clear documentation helps users understand HyperDX is optional
- Environment variables follow standard naming conventions
- Default values minimize configuration burden for users
- Link to HyperDX signup makes it easy to get started with observability
- Matches liquidity-b .env.example structure

## Task 9. Instrument key flows with tracing spans

Add tracing instrumentation to mint and redemption flows following standard
tracing patterns.

- [ ] Add `#[tracing::instrument]` attribute to key mint functions:
  - `initiate_mint` endpoint handler
  - `confirm_journal` endpoint handler
  - `MintManager::start_minting`
  - `CallbackManager::send_callback`
- [ ] Add `#[tracing::instrument]` attribute to key redemption functions:
  - `RedemptionDetector::run`
  - `RedemptionDetector::handle_transfer`
  - `RedeemCallManager::call_alpaca_redeem`
  - `JournalManager::poll_for_completion`
  - Burn token functions
- [ ] Add manual spans for async contexts where `#[instrument]` doesn't work
      well
- [ ] Use `tracing::info!`, `tracing::warn!`, `tracing::error!` for contextual
      events within spans
- [ ] Add span fields for key identifiers (issuer_request_id, client_id,
      underlying symbol)

**Design Rationale:**

- Tracing instrumentation provides distributed tracing across async operations
- `#[instrument]` automatically creates spans with function arguments as fields
- Manual spans needed for long-running loops or complex async orchestration
- Span fields (issuer_request_id, etc.) enable filtering/searching in HyperDX UI
- Info/warn/error events provide additional context within traces
- Focus on business-critical flows (mint, redemption) rather than exhaustive
  instrumentation
- Start conservative - can always add more spans later based on observability
  needs

## Architecture Overview

### Env → Config Pattern with Flattened HyperDxEnv

Following the liquidity-b pattern for type-safe, modular configuration:

```rust
// HyperDxEnv: Handles HyperDX-specific parsing
#[derive(Parser)]
struct HyperDxEnv {
    #[clap(long, env)]
    hyperdx_api_key: Option<String>,
    #[clap(long, env, default_value = "st0x-issuance")]
    hyperdx_service_name: String,
}

impl HyperDxEnv {
    fn into_config(self, log_level: tracing::Level) -> Option<HyperDxConfig> {
        self.hyperdx_api_key.map(|api_key| HyperDxConfig {
            api_key,
            service_name: self.hyperdx_service_name,
            log_level,
        })
    }
}

// Env: Top-level parsing with flattened HyperDxEnv
#[derive(Parser)]
struct Env {
    #[clap(flatten)]
    hyperdx: HyperDxEnv,
    #[clap(long, env, default_value = "debug")]
    log_level: LogLevel,
    // ... other fields
}

// Config: Validated, properly typed
struct Config {
    hyperdx: Option<HyperDxConfig>,  // All-or-nothing
    log_level: LogLevel,
    // ... other fields
}

impl Env {
    fn into_config(self) -> Result<Config, ConfigError> {
        let log_level_tracing = (&self.log_level).into();
        let hyperdx = self.hyperdx.into_config(log_level_tracing);

        Ok(Config {
            hyperdx,
            log_level: self.log_level,
            // ... other fields
        })
    }
}
```

**Key Benefits:**

- **Type Safety**: `Option<HyperDxConfig>` makes invalid states unrepresentable
- **Modularity**: `HyperDxEnv` encapsulates HyperDX-specific parsing and
  conversion logic
- **Validation Boundary**: `into_config()` methods are where we enforce business
  rules
- **Separation of Concerns**: Parsing (clap) vs domain logic (Config)
- **Flattened Structure**: `#[clap(flatten)]` makes CLI/env parsing clean and
  intuitive

### Optional Integration Pattern

HyperDX telemetry is completely optional:

- If `HYPERDX_API_KEY` is set: Export traces to HyperDX + console logging
- If `HYPERDX_API_KEY` is not set: Console-only logging
- If telemetry setup fails: Graceful fallback to console-only logging

### Dual-Layer Filtering

Both console and telemetry layers share the same spans but can have independent
filters:

- Default filter: `st0x_issuance={log_level}` (prevents external crate spam)
- Can be overridden by `RUST_LOG` environment variable
- Prevents flooding HyperDX with traces from rocket, alloy, sqlx, etc.

### Critical Implementation Details

**Blocking HTTP Client Requirement:** The `BatchSpanProcessor` spawns background
threads that run outside the tokio runtime. These threads require a blocking
HTTP client (`reqwest::blocking`) rather than an async client. Using an async
client would panic with "no reactor running". The blocking client must be
created in a separate thread to avoid blocking the main tokio runtime during
initialization.

**TelemetryGuard Lifecycle:** The `TelemetryGuard` struct ensures clean
shutdown:

1. `force_flush()` blocks until all pending spans are exported (or timeout)
2. `shutdown()` terminates the BatchSpanProcessor's background threads
3. Explicit `drop(telemetry_guard)` in main ensures this happens before process
   exit

**Resource Configuration:**

- Service name: "st0x-issuance" (identifies this service in distributed traces)
- Deployment environment: "production" (resource attribute)
- Tracer name: "st0x-tracer" (instrumentation library identifier)

### Testing Strategy

- Manual testing with HyperDX API key to verify trace export
- Test console-only fallback by not setting HYPERDX_API_KEY
- Test error handling by providing invalid API key
- Verify spans appear correctly in HyperDX UI with expected fields
- Test telemetry guard flush by ensuring spans appear after shutdown
