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
  (`Mint`, `Redemption`, `AccountLink`, `TokenizedAsset`)
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

## Development Commands

### Building & Running

```bash
cargo build              # Build the project
cargo run                # Run the HTTP server
```

### Testing

```bash
cargo test -q            # Run all tests
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
cargo fmt                                                            # Format code
cargo clippy --all-targets --all-features -- -D clippy::all         # Run linting
rainix-rs-static                                                    # Run static analysis
```

## Project Structure

```
st0x.issuance/
├── src/
│   ├── aggregates/          # Domain aggregates (Mint, Redemption, etc.)
│   ├── commands/            # Command definitions
│   ├── events/              # Event definitions
│   ├── views/               # Read model projections
│   ├── services/            # External service clients (Alpaca, Blockchain)
│   ├── http/                # HTTP endpoints
│   └── main.rs              # Application entry point
├── crates/
│   └── sqlite-es/           # SQLite event store implementation
├── migrations/              # Database migrations
├── AGENTS.md                # AI agent development guidelines
├── CLAUDE.md                # Claude Code instructions
├── SPEC.md                  # Detailed specification
├── ROADMAP.md               # Development roadmap
└── README.md                # This file
```

## API Endpoints

### Endpoints We Implement (for Alpaca)

- `POST /accounts/connect` - Link AP account to our system
- `GET /tokenized-assets` - List supported tokenized assets
- `POST /inkind/issuance` - Receive mint request from Alpaca
- `POST /inkind/issuance/confirm` - Receive journal confirmation from Alpaca

### Endpoints We Call (Alpaca)

- `POST /v1/accounts/{account_id}/tokenization/callback/mint` - Confirm mint
  completed
- `POST /v1/accounts/{account_id}/tokenization/redeem` - Initiate redemption
- `GET /v1/accounts/{account_id}/tokenization/requests` - Poll request status

## Mint Flow

1. AP requests mint → Alpaca calls our `/inkind/issuance` endpoint
2. We validate and respond with `issuer_request_id`
3. Alpaca journals shares from AP to our custodian account
4. Alpaca confirms journal → we receive `/inkind/issuance/confirm`
5. We mint tokens on-chain via `vault.deposit()`
6. We call Alpaca's callback endpoint

## Redemption Flow

1. AP sends tokens to our redemption wallet → we detect transfer
2. We call Alpaca's redeem endpoint
3. We poll for journal completion
4. We burn tokens on-chain via `vault.withdraw()`

## Configuration

Configuration is managed through environment variables. See `.env.example` for
all available options.

Key configuration areas:

- HTTP server settings
- Alpaca API credentials
- Blockchain RPC endpoints
- Database connection
- Operational parameters (gas limits, poll intervals, etc.)

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
2. `cargo clippy --all-targets --all-features -- -D clippy::all` - Fix all
   linting issues
3. `cargo fmt` - Format code last

For detailed architectural patterns and design decisions, see
[SPEC.md](SPEC.md).
