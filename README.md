# ST0x Issuance Bot

A Rust-based issuance bot that acts as the **Issuer** in Alpaca's Instant
Tokenization Network (ITN). The bot implements the Issuer-side endpoints that
Alpaca calls during mint/redeem operations, and coordinates with the Rain
`OffchainAssetReceiptVault` contracts to execute the actual on-chain minting and
burning of tokenized shares.

## Overview

The issuance bot serves as the bridge between traditional equity holdings (at
Alpaca) and on-chain semi-fungible tokenized representations (Rain SFT
contracts). This is general infrastructure - any Authorized Participant (AP) can
use it to mint and redeem tokenized equities.

### Key Features

- **Account Linking**: Connect AP accounts to the system
- **Asset Management**: Configure which tokenized assets are supported
- **Minting**: Convert traditional equity holdings to on-chain tokens
- **Redemption**: Burn on-chain tokens and return underlying equity
- **Event Sourcing**: Complete audit trail with time-travel debugging
  capabilities
- **CQRS Architecture**: Separation of command and query responsibilities for
  scalability

## Architecture

### Event Sourcing & CQRS

The system uses **Event Sourcing (ES)** and **Command Query Responsibility
Segregation (CQRS)** patterns:

- **Commands**: Requests to perform actions (e.g., `InitiateMint`,
  `ConfirmJournal`)
- **Events**: Immutable facts about what happened (e.g., `MintInitiated`,
  `TokensMinted`)
- **Aggregates**: Business entities that process commands and produce events
  (`Mint`, `Redemption`, `Account`, `TokenizedAsset`)
- **Views**: Read-optimized projections built from events for efficient querying
- **Event Store**: Single source of truth - append-only log of all domain events
  in SQLite

### Core Components

- **HTTP Server**: Rocket.rs-based server implementing Alpaca ITN Issuer
  endpoints
- **Blockchain Client**: Alloy-based client for interacting with Rain vault
  contracts
- **Alpaca Integration**: Client for Alpaca's API endpoints
- **Monitor Service**: Watches redemption wallet for incoming token transfers
- **SQLite Database**: Event store and view repositories

## Development Setup

### Prerequisites

- [Nix](https://nixos.org/download.html) with flakes enabled

### Getting Started

1. **Clone the repository**:
   ```bash
   git clone https://github.com/ST0x-Technology/st0x.issuance.git
   cd st0x.issuance
   ```

2. **Enter development environment**:
   ```bash
   nix develop
   ```

3. **Set up environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Create and migrate database**:
   ```bash
   sqlx db create
   sqlx migrate run
   ```

5. **Run tests**:
   ```bash
   cargo test -q
   ```

6. **Start the server**:
   ```bash
   cargo run
   ```

## Authentication

Endpoints require API key authentication with IP whitelisting and rate limiting.

**Configuration:**

```bash
# Generate API key (min 32 chars)
ISSUER_API_KEY=$(openssl rand -hex 32)

# Configure IP whitelist (CIDR notation)
ALPACA_IP_RANGES="1.2.3.0/24,5.6.7.8/32"
```

**Request format:**

```bash
curl -X POST https://issuer.example.com/inkind/issuance \
  -H "X-API-KEY: <api-key>" \
  -H "Content-Type: application/json"
```

**Security:** API key constant-time comparison, 10 failed auth attempts/IP/min
rate limit

## Development Commands

### Building & Running

```bash
cargo build              # Build the project
cargo run                # Run the HTTP server
```

### Testing

```bash
cargo test --workspace   # Run all tests (including crates/)
cargo test -q            # Run all tests quietly
cargo test -q --lib      # Run library tests only
cargo test -q <name>     # Run specific test
```

### Database Management

```bash
sqlx db create           # Create the database
sqlx migrate run         # Apply migrations
sqlx migrate revert      # Revert last migration
sqlx migrate reset -y    # Drop DB and re-run all migrations
```

### Code Quality

```bash
cargo fmt                                                   # Format code
cargo fmt --all -- --check                                  # Check formatting
cargo clippy --workspace --all-targets --all-features \
  -- -D clippy::all -D warnings                             # Run linting
```

## Project Structure

```
st0x.issuance/
├── src/
│   ├── lib.rs               # Library entry point with rocket setup
│   ├── main.rs              # Binary entry point (minimal)
│   ├── config.rs            # Configuration types
│   ├── test_utils.rs        # Shared test utilities
│   ├── account/             # Account aggregate and endpoints
│   ├── mint/                # Mint aggregate and endpoints
│   ├── redemption/          # Redemption aggregate and managers
│   ├── tokenized_asset/     # TokenizedAsset aggregate
│   ├── receipt_inventory/   # Receipt tracking aggregate
│   ├── chain/               # ChainRegistry: per-chain RPC, vault, backfill config
│   ├── alpaca/              # Alpaca API service
│   ├── vault/               # On-chain vault service
│   ├── wallet/              # Signing backend (Turnkey / local)
│   └── auth/                # API key auth and IP whitelisting
├── tests/                   # End-to-end tests (Anvil + mocks)
├── crates/
│   └── sqlite-es/           # SQLite event store implementation
├── migrations/              # Database migrations
└── docs/                    # Developer documentation
```

**Note**: This project uses **package by feature** organization, not package by
layer. Each feature module (`account/`, `mint/`, `tokenized_asset/`) contains
all related code: types, errors, commands, events, aggregates, views, and
endpoints.

## API Endpoints

### Endpoints We Implement (for Alpaca)

- `POST /accounts/connect` - Link AP account to our system
- `POST /accounts/{client_id}/wallets` - Whitelist a wallet for an AP account
- `GET /tokenized-assets` - List supported tokenized assets
- `POST /inkind/issuance` - Receive mint request from Alpaca
- `POST /inkind/issuance/confirm` - Receive journal confirmation from Alpaca

### Endpoints We Call (Alpaca)

- `POST /v1/accounts/{account_id}/tokenization/callback/mint` - Confirm mint
  completed
- `POST /v1/accounts/{account_id}/tokenization/callback/redeem` - Initiate
  redemption
- `GET /v1/accounts/{account_id}/tokenization/requests/{tokenization_request_id}` -
  Poll request status

## Mint Flow

1. AP requests mint → Alpaca calls our `/inkind/issuance` endpoint
2. We validate and respond with `issuer_request_id`
3. Alpaca journals shares from AP to our custodian account
4. Alpaca confirms journal → we receive `/inkind/issuance/confirm`
5. We mint tokens on-chain (vault-direct: `vault.deposit()`; orchestrator:
   `ST0xOrchestrator.mint()` — see below)
6. We call Alpaca's callback endpoint

Each mint anchors its mode at initiation from the per-asset `vault_mode` config
(see SPEC.md "Orchestrator Migration"): vault-direct assets mint via the
`vault.deposit()` multicall with the receipt held by the bot; orchestrator
assets mint via a single `ST0xOrchestrator.mint()` — the orchestrator custodies
the receipt and forwards the shares to the recipient wallet.

An orchestrator-mode mint additionally requires a **recipient authorization**:
the liquidity bot signs the orchestrator's EIP-712 `MintAuthV1` digest over
`(token, to, amount, nonce)` with the recipient wallet's key and delivers
`{nonce, signature}` via the internal
`POST /internal/mints/<tokenization_request_id>/authorization` endpoint, which
validates the signer and nonce on-chain before recording it. Until the
authorization arrives the mint waits — it never falls back to vault-direct. The
consumed nonce is single-use on-chain, making it the mint's idempotency key:
recovery of a landed mint full-matches the orchestrator's `Minted` log against
`(to, nonce, token, amount)` and completes without resubmitting. A consumed
nonce that does not full-match takes one of two verdicts, never conflated: a log
at the pair disagreeing on token/amount is proof a different mint consumed it
(parked as `NonceConsumedByOtherMint` for manual reconciliation), while an empty
scan is an inconclusive chain view (parked as `NonceReplayUnresolved`,
re-queried by recovery over a widened window and resolved forward once the
landing becomes visible — closable only with an explicit operator
acknowledgement). The `vault_mode` field on
`GET /tokenized-assets/<underlying>/status` tells the liquidity bot which assets
will need authorizations for _new_ mints, but the authoritative requirement for
any given mint is its own persisted `mint_mode` anchor: a mint initiated before
a config flip keeps the mode it was initiated with, so the bot must answer the
mint it was asked about, not the asset's current config.

## Redemption Flow

1. AP sends tokens to our redemption wallet → we detect transfer
2. We call Alpaca's redeem endpoint
3. We poll for journal completion
4. We prepare and persist the exact signed burn transaction
5. We broadcast that transaction and confirm the on-chain burn

Each redemption anchors its burn mode at detection from the per-asset
`vault_mode` config (see SPEC.md "Orchestrator Migration"): vault-direct assets
burn via the vault multicall against bot-held receipts; orchestrator assets burn
via a single `ST0xOrchestrator.burn()` (the orchestrator walks its own receipts
on-chain, with no receipt planning or reservation on our side).

If the process stops after persisting or broadcasting a burn, startup and
periodic recovery classify the persisted hash before acting. Recovery confirms a
mined burn, re-broadcasts the same bytes while it can still land, and only signs
a fresh-nonce replacement after the previous transaction is provably dead. This
automatic recovery only reconciles the outcome of already-signed transactions —
burns that failed with a deterministic classification (`InsufficientReceipts`,
`AllowanceInsufficient`) are never auto-retried; an operator fixes the
underlying cause and re-drives them via the admin recovery endpoint
(`ResumeBurn`).

Recovery, on-chain burn verification, and the admin surface are all mode-aware:
each redemption's recovery and force-completion derive from its own persisted
burn mode (never live config), so vault-direct and orchestrator redemptions
recover side by side during the incremental per-asset cutover. Operators can see
which path each asset is on, and whether each orchestrator is healthy, via
`GET /admin/orchestrator-health`.

Receipt and redemption transfer backfills use durable per-vault checkpoints.
Periodic receipt backfill keeps receipt checkpoints current during long uptime,
and live receipt monitoring processes new events without advancing the durable
checkpoint out of block order. Redemption transfer backfill runs as background
startup work. Restarts resume after the last successfully processed block
instead of rescanning the full configured historical range.

## Per network monitoring

Every configured chain gets a gas balance monitor on the issuer wallet's native
balance (ETH on Base and Ethereum, HYPE on HyperEVM). Thresholds come from
`CHAIN_<NETWORK>_LOW_GAS_THRESHOLD` (or the flat `LOW_GAS_THRESHOLD` for the
legacy Base configuration) as decimal native token amounts; a balance below the
threshold raises an ERROR log and a Telegram lifecycle notification,
deduplicated to at most one repeat alert per hour. Thresholds are all or nothing
across configured chains; with none set, monitoring is disabled with a startup
WARN.

`GET /admin/network-telemetry` reports per network telemetry: transfer poller
and receipt backfill pass counters with failure rate and block lag, plus the gas
monitor's latest reading. See SPEC.md "Per network monitoring".

## Configuration

Configuration comes from two non-overlapping sources:

- **Environment variables** (see `.env.example`) — everything except vault
  modes: HTTP server settings, Alpaca API credentials, blockchain RPC endpoints,
  signing backend, database connection, and operational parameters (gas limits,
  poll intervals, etc.).
- **The optional TOML config file** (path via the `--config` flag or the
  `CONFIG` environment variable; see `config.example.toml`) — the only source of
  per-asset vault modes: the `[orchestrator]` section and per-asset `vault_mode`
  overrides. No environment variable sets vault modes. When the file is absent
  (or has no orchestrator entries), every asset runs vault-direct.

Because the sources are disjoint there is no precedence between them; each
setting has exactly one home.

## Testing Strategy

The project uses Given-When-Then testing for aggregate logic:

```rust
MintTestFramework::with(mock_services)
    .given(vec![MintInitiated { /* ... */ }])
    .when(ConfirmJournal { issuer_request_id: "123" })
    .then_expect_events(vec![
        JournalConfirmed { /* ... */ },
        MintingStarted { /* ... */ }
    ]);
```

This approach enables:

- Testing business logic in isolation
- Clear test intent and readability
- Complete coverage of state transitions
- Easy mocking of external services

### End-to-End Testing with Anvil

E2E tests in `tests/` use Anvil (local Ethereum blockchain) for realistic
on-chain testing:

- **LocalEvm**: Test infrastructure that deploys vault contracts to Anvil
- **Real blockchain interactions**: Tests execute actual on-chain deposits and
  transfers
- **WebSocket monitoring**: Tests verify event subscriptions and real-time
  detection
- **In-memory database**: Tests use SQLite in-memory for fast, isolated
  execution
- **Mock external APIs**: Alpaca API calls use httpmock for deterministic
  testing

E2E tests validate complete flows from HTTP request through CQRS to on-chain
execution.

## Documentation

- **[SPEC.md](SPEC.md)** - Detailed specification of the system
- **[ROADMAP.md](ROADMAP.md)** - Development roadmap and milestones
- **[AGENTS.md](AGENTS.md)** - Development guidelines for AI agents
- **[CLAUDE.md](CLAUDE.md)** - Instructions for Claude Code

## Contributing

This project follows strict development practices focused on code quality and
maintainability:

### Architecture & Design

- **Event Sourcing & CQRS**: All state changes captured as immutable events
- **Type-Driven Design**: Use algebraic data types to make invalid states
  unrepresentable
- **Functional Patterns**: Prefer functional programming patterns and iterators
  over imperative loops
- **Feature Development**: Implement complete vertical slices (HTTP → commands →
  events → views)

### Code Quality Standards

- **No Lint Suppression**: Never use `#[allow(clippy::*)]` without explicit
  permission - fix the underlying code instead
- **Financial Data Integrity**: All numeric conversions and financial operations
  must use explicit error handling - never silently cap, truncate, or provide
  default values
- **Error Handling**: Avoid `unwrap()` even after validation - use proper error
  propagation
- **Visibility Levels**: Keep visibility as restrictive as possible
  (`pub(crate)` over `pub`, private over `pub(crate)`)
- **Comments**: Only comment when adding context that cannot be expressed
  through code structure - avoid redundant comments

### Workflow

Before submitting changes, always run in order:

1. `cargo test -q` - Run all tests first
2. `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` -
   Fix all linting issues
3. `cargo fmt` - Format code last

For detailed architectural patterns and design decisions, see
[SPEC.md](SPEC.md).

## GCP release path

Alongside the DigitalOcean deploy-rs path, main builds an OCI image of the
`st0x-issuance` binary (`nix build .#bot-oci`, contract in
`nix/oci-image.nix`). `.github/workflows/build-oci.yml` pushes it to the
`s01-issuance` Artifact Registry, signs its attestation, and, once the
devops side has armed it, writes the digest into the GCP staging VM's
deploy state (a merge to main is a staging deploy). Pushing a `vX.Y.Z` tag
labels that commit's attested image (`release-tag.yml`); production is
promoted from that label on the devops side, never from this repo.
`nix run .#smoke-test-image -- <image>` runs the same startup check CI
does.
