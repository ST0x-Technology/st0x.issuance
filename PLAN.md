# Implementation Plan: DetectRedemption Feature and MonitorService

This plan implements issue #21, which establishes the foundation for the redemption flow by implementing redemption detection capabilities and the MonitorService infrastructure.

## Overview

This feature implements the first step of the redemption workflow: detecting when an Authorized Participant (AP) sends tokens to our redemption wallet. The system needs to:

1. Monitor on-chain transfers to our redemption wallet
2. Create redemption records in the event store when transfers are detected
3. Provide a MonitorService abstraction with both mock and real implementations
4. Wire everything into the CQRS framework

This is the beginning of the complete redemption flow (detect → call Alpaca → poll for journal → burn tokens). This task focuses only on the detection phase.

## Task 1. Create Redemption Module Structure with Minimal Stubs

Create the basic module structure with minimal stubs that compile.

**Files to Create:**
- `src/redemption/mod.rs` - Module entry point with minimal stub
- `src/redemption/cmd.rs` - Empty command enum stub
- `src/redemption/event.rs` - Empty event enum stub
- `src/redemption/view.rs` - Empty view enum stub

**`src/redemption/mod.rs`:**

```rust
mod cmd;
mod event;
mod view;

use serde::{Deserialize, Serialize};

pub(crate) use cmd::RedemptionCommand;
pub(crate) use event::RedemptionEvent;
pub(crate) use view::RedemptionView;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct Redemption;
```

**`src/redemption/cmd.rs`:**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionCommand {}
```

**`src/redemption/event.rs`:**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionEvent {}
```

**`src/redemption/view.rs`:**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) enum RedemptionView {}
```

**Update `src/lib.rs`:**
Add `pub mod redemption;` after the other module declarations (around line 27).

- [x] Create `src/redemption/` directory
- [x] Create `mod.rs` with minimal `Redemption` struct
- [x] Create `cmd.rs` with empty `RedemptionCommand` enum
- [x] Create `event.rs` with empty `RedemptionEvent` enum
- [x] Create `view.rs` with empty `RedemptionView` enum
- [x] Add `pub mod redemption;` to `src/lib.rs`
- [x] Run `cargo build` to verify module compiles
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 2. Define RedemptionEvent

Implement the event definition - events are the source of truth in event sourcing.

**Update `src/redemption/event.rs`:**

From SPEC.md line 248-249, the RedemptionDetected event captures all information about a detected transfer:

```rust
use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionEvent {
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
}
```

**Import Organization:**
- External crates first: `alloy`, `chrono`, `serde`
- Empty line
- Internal crates: `crate::mint`, `crate::tokenized_asset`

- [x] Update `event.rs` with `RedemptionEvent::Detected` variant
- [x] Add all necessary imports following two-group pattern
- [x] Run `cargo build` to verify compilation
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 3. Define RedemptionCommand

Implement the command definition - commands represent intent to produce events.

**Update `src/redemption/cmd.rs`:**

From SPEC.md line 233-234, the Detect command contains the same information as the event (before timestamp is added):

```rust
use alloy::primitives::{Address, B256};
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionCommand {
    Detect {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
    },
}
```

- [x] Update `cmd.rs` with `RedemptionCommand::Detect` variant
- [x] Add all necessary imports following two-group pattern
- [x] Run `cargo build` to verify compilation
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 4. Define Redemption Aggregate Enum

Now that we know what events we're applying, define the aggregate states.

**Update `src/redemption/mod.rs`:**

Replace the stub `Redemption` struct with the proper enum. The aggregate has two states:
- `Uninitialized` - No redemption detected yet
- `Detected` - Transfer detected, contains all event data

```rust
mod cmd;
mod event;
mod view;

use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub(crate) use cmd::RedemptionCommand;
pub(crate) use event::RedemptionEvent;
pub(crate) use view::RedemptionView;

use crate::mint::{IssuerRequestId, Quantity};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Redemption {
    Uninitialized,
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        detected_tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
}

impl Default for Redemption {
    fn default() -> Self {
        Self::Uninitialized
    }
}
```

**Note:** Field names match the event exactly, except `tx_hash` becomes `detected_tx_hash` for clarity since this is the transaction that triggered the redemption.

- [x] Replace `Redemption` struct with enum in `mod.rs`
- [x] Add `Uninitialized` and `Detected` variants
- [x] Implement `Default` trait
- [x] Add necessary imports following two-group pattern
- [x] Run `cargo build` to verify compilation
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 5. Implement Aggregate Trait for Redemption

Implement the command handling and event application logic.

**Update `src/redemption/mod.rs`:**

Add at the end of the file:

```rust
use async_trait::async_trait;
use cqrs_es::Aggregate;

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum RedemptionError {
    #[error("Redemption already detected for request: {issuer_request_id}")]
    AlreadyDetected { issuer_request_id: String },
}

#[async_trait]
impl Aggregate for Redemption {
    type Command = RedemptionCommand;
    type Event = RedemptionEvent;
    type Error = RedemptionError;
    type Services = ();

    fn aggregate_type() -> String {
        "Redemption".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            RedemptionCommand::Detect {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
            } => {
                if !matches!(self, Self::Uninitialized) {
                    return Err(RedemptionError::AlreadyDetected {
                        issuer_request_id: issuer_request_id.0.clone(),
                    });
                }

                let now = Utc::now();

                Ok(vec![RedemptionEvent::Detected {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                    detected_at: now,
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            } => {
                *self = Self::Detected {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    detected_tx_hash: tx_hash,
                    block_number,
                    detected_at,
                };
            }
        }
    }
}
```

**Add imports** to the top of `mod.rs`:
```rust
use async_trait::async_trait;
use chrono::Utc;
use cqrs_es::Aggregate;
```

- [x] Define `RedemptionError` enum
- [x] Implement `Aggregate` trait for `Redemption`
- [x] Implement `handle` method with validation and event production
- [x] Implement `apply` method for state transitions
- [x] Add necessary imports
- [x] Run `cargo build` to verify compilation
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 6. Implement RedemptionView

Create the view that maintains queryable state for redemptions.

**Update `src/redemption/view.rs`:**

Replace the stub with the actual view implementation:

```rust
use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity};
use crate::redemption::{Redemption, RedemptionEvent};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionView {
    Unavailable,
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
}

impl Default for RedemptionView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl View<Redemption> for RedemptionView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        match &event.payload {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            } => {
                *self = Self::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token: token.clone(),
                    wallet: *wallet,
                    quantity: quantity.clone(),
                    tx_hash: *tx_hash,
                    block_number: *block_number,
                    detected_at: *detected_at,
                };
            }
        }
    }
}
```

**Naming Decision:** View uses `Unavailable` instead of `Uninitialized` to reflect query perspective (data not available) vs entity lifecycle (entity not created).

- [x] Update `view.rs` with `RedemptionView` enum
- [x] Add `Unavailable` and `Detected` variants
- [x] Implement `Default` trait
- [x] Implement `View<Redemption>` trait with `update` method
- [x] Add necessary imports following two-group pattern
- [x] Run `cargo build` to verify compilation
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 7. Create Database Migration for redemption_view Table

Create a migration for the `redemption_view` table.

**Generate Migration:**

```bash
sqlx migrate add create_redemption_view
```

**Edit the generated file in `migrations/` with:**

```sql
-- Create redemption view table for tracking redemption state
CREATE TABLE redemption_view (
    view_id TEXT PRIMARY KEY,         -- issuer_request_id
    version BIGINT NOT NULL,          -- Last event sequence applied to this view
    payload JSON NOT NULL             -- Current redemption state as JSON
);

-- Index for querying redemptions by symbol
CREATE INDEX idx_redemption_view_symbol
    ON redemption_view(json_extract(payload, '$.underlying'));

-- Index for querying redemptions by transaction hash
CREATE INDEX idx_redemption_view_tx_hash
    ON redemption_view(json_extract(payload, '$.tx_hash'));
```

**Test Migration:**

```bash
sqlx db reset -y
```

This will drop the database, re-run all migrations (including the new one), and verify SQL is valid.

- [x] Run `sqlx migrate add create_redemption_view`
- [x] Edit generated migration file with CREATE TABLE statement
- [x] Add indexes for common queries
- [x] Run `sqlx db reset -y` to test migration
- [x] Verify database schema with `sqlite3 issuance.db ".schema redemption_view"`
- [x] Run `cargo build` to ensure sqlx is happy

## Task 8. Define TransferService Trait

Create the service trait for monitoring blockchain transfers to the redemption wallet.

**Rationale:** After analyzing the actual redemption flow, we determined that the blockchain services should be organized as:
- `blockchain/vault.rs` - `VaultService` trait for vault operations (deposit/withdraw)
- `blockchain/transfer.rs` - `TransferService` trait for watching ERC-20 transfers

The existing `BlockchainService` will be renamed to `VaultService` and moved to `vault.rs` in a future refactoring. For now, we add `TransferService` alongside it.

**Create `src/blockchain/transfer.rs`:**

```rust
use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;

use crate::mint::IssuerRequestId;
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

pub(crate) struct TransferDetected {
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) from: Address,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) amount: U256,
    pub(crate) tx_hash: B256,
    pub(crate) block_number: u64,
}

#[async_trait]
pub(crate) trait TransferService: Send + Sync {
    async fn watch_transfers(&self) -> Result<Vec<TransferDetected>, TransferError>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferError {
    #[error("Failed to connect to blockchain: {0}")]
    ConnectionFailed(String),

    #[error("Failed to fetch transfers: {0}")]
    FetchFailed(String),

    #[error("Invalid transfer data: {0}")]
    InvalidData(String),
}
```

**Update `src/blockchain/mod.rs`:**
Add `pub(crate) mod transfer;` after the existing module declarations.

**Design Notes:**

- `TransferDetected` represents an ERC-20 Transfer event to our redemption wallet
- Contains `from` address (the AP initiating redemption) and `amount` as U256
- Service returns `Vec<TransferDetected>` to support batch processing
- `watch_transfers()` is a pull-based API (manager polls for transfers)
- Lives in `blockchain/` module alongside vault operations (read/write separation)

- [ ] Create `transfer.rs` in `src/blockchain/`
- [ ] Define `TransferService` trait
- [ ] Define `TransferDetected` struct
- [ ] Define `TransferError` enum
- [ ] Add `pub(crate) mod transfer;` to `src/blockchain/mod.rs`
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

## Task 9. Implement MockTransferService

Create a mock implementation for testing.

**Create `src/blockchain/transfer_mock.rs`:**

```rust
use async_trait::async_trait;

use super::transfer::{TransferDetected, TransferError, TransferService};

pub(crate) struct MockTransferService {
    transfers: Vec<TransferDetected>,
    should_fail: bool,
}

impl MockTransferService {
    #[cfg(test)]
    pub(crate) fn new_with_transfers(transfers: Vec<TransferDetected>) -> Self {
        Self {
            transfers,
            should_fail: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_failing() -> Self {
        Self {
            transfers: vec![],
            should_fail: true,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_empty() -> Self {
        Self {
            transfers: vec![],
            should_fail: false,
        }
    }
}

#[async_trait]
impl TransferService for MockTransferService {
    async fn watch_transfers(&self) -> Result<Vec<TransferDetected>, TransferError> {
        if self.should_fail {
            return Err(TransferError::FetchFailed(
                "Mock failure".to_string(),
            ));
        }
        Ok(self.transfers.clone())
    }
}
```

**Update `src/blockchain/mod.rs`:**
Add `pub(crate) mod transfer_mock;` after `pub(crate) mod transfer;`.

**Design Notes:**

The mock enables testing different scenarios:
- `new_with_transfers()` - Simulate detected transfers
- `new_failing()` - Simulate service errors
- `new_empty()` - Simulate no transfers

All constructors are `#[cfg(test)]` since the mock is only for testing.

- [ ] Create `transfer_mock.rs` in `src/blockchain/`
- [ ] Implement `MockTransferService` struct
- [ ] Implement test constructors with `#[cfg(test)]`
- [ ] Implement `TransferService` trait
- [ ] Export mock from `mod.rs`
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

## Task 10. Wire Redemption CQRS Framework in lib.rs

Integrate redemption into the application's CQRS infrastructure.

**Update `src/lib.rs`:**

1. **Add imports** (around line 14):
```rust
use redemption::{Redemption, RedemptionView};
```

2. **Add type alias** (after `MintEventStore` around line 41):
```rust
pub(crate) type RedemptionCqrs = Arc<SqliteCqrs<redemption::Redemption>>;
```

3. **In `initialize_rocket()`**, add redemption setup after mint setup (around line 205):
```rust
let redemption_view_repo = Arc::new(SqliteViewRepository::<
    RedemptionView,
    Redemption,
>::new(
    pool.clone(),
    "redemption_view".to_string(),
));

let redemption_query = GenericQuery::new(redemption_view_repo);

let redemption_cqrs_raw =
    sqlite_cqrs(pool.clone(), vec![Box::new(redemption_query)], ());
let redemption_cqrs = Arc::new(redemption_cqrs_raw);
```

4. **Add to Rocket state** (around line 223):
```rust
.manage(redemption_cqrs)
```

**Test Wiring:**

Run the application to verify everything initializes correctly:
```bash
cargo run
```

You should see the server start without errors and the redemption view should be created in the database.

- [x] Add `redemption` imports to `lib.rs`
- [x] Add `RedemptionCqrs` type alias
- [x] Create redemption view repository in `initialize_rocket()`
- [x] Create `GenericQuery` for redemption
- [x] Create redemption CQRS framework instance
- [x] Add redemption CQRS to Rocket state
- [x] Run `cargo build` to verify wiring compiles
- [x] Run `cargo run` and verify application starts successfully
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 11. Add Aggregate Tests

Implement comprehensive tests for the Redemption aggregate.

**Add to `src/redemption/mod.rs`:**

```rust
#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use chrono::Utc;
    use cqrs_es::{Aggregate, test::TestFramework};
    use rust_decimal::Decimal;

    use super::{
        Redemption, RedemptionCommand, RedemptionError, RedemptionEvent,
    };
    use crate::mint::{IssuerRequestId, Quantity};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    type RedemptionTestFramework = TestFramework<Redemption>;

    #[test]
    fn test_detect_redemption_creates_event() {
        let issuer_request_id = IssuerRequestId::new("red-123");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        let validator = RedemptionTestFramework::with(())
            .given_no_previous_events()
            .when(RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    RedemptionEvent::Detected {
                        issuer_request_id: event_id,
                        underlying: event_underlying,
                        token: event_token,
                        wallet: event_wallet,
                        quantity: event_quantity,
                        tx_hash: event_tx_hash,
                        block_number: event_block_number,
                        detected_at,
                    } => {
                        assert_eq!(event_id, &issuer_request_id);
                        assert_eq!(event_underlying, &underlying);
                        assert_eq!(event_token, &token);
                        assert_eq!(event_wallet, &wallet);
                        assert_eq!(event_quantity, &quantity);
                        assert_eq!(event_tx_hash, &tx_hash);
                        assert_eq!(event_block_number, &block_number);
                        assert!(detected_at.timestamp() > 0);
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_detect_redemption_when_already_detected_returns_error() {
        let issuer_request_id = IssuerRequestId::new("red-456");
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;

        RedemptionTestFramework::with(())
            .given(vec![RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at: Utc::now(),
            }])
            .when(RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
            })
            .then_expect_error(RedemptionError::AlreadyDetected {
                issuer_request_id: issuer_request_id.0,
            });
    }

    #[test]
    fn test_apply_detected_event_updates_state() {
        let mut redemption = Redemption::default();

        assert!(matches!(redemption, Redemption::Uninitialized));

        let issuer_request_id = IssuerRequestId::new("red-789");
        let underlying = UnderlyingSymbol::new("NVDA");
        let token = TokenSymbol::new("tNVDA");
        let wallet = address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafed");
        let quantity = Quantity::new(Decimal::from(25));
        let tx_hash = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        let block_number = 99999;
        let detected_at = Utc::now();

        redemption.apply(RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet,
            quantity: quantity.clone(),
            tx_hash,
            block_number,
            detected_at,
        });

        let Redemption::Detected {
            issuer_request_id: state_id,
            underlying: state_underlying,
            token: state_token,
            wallet: state_wallet,
            quantity: state_quantity,
            detected_tx_hash: state_tx_hash,
            block_number: state_block_number,
            detected_at: state_detected_at,
        } = redemption
        else {
            panic!("Expected Detected state, got Uninitialized");
        };

        assert_eq!(state_id, issuer_request_id);
        assert_eq!(state_underlying, underlying);
        assert_eq!(state_token, token);
        assert_eq!(state_wallet, wallet);
        assert_eq!(state_quantity, quantity);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_detected_at, detected_at);
    }
}
```

- [x] Add `#[cfg(test)] mod tests` section to `mod.rs`
- [x] Implement `test_detect_redemption_creates_event`
- [x] Implement `test_detect_redemption_when_already_detected_returns_error`
- [x] Implement `test_apply_detected_event_updates_state`
- [x] Run `cargo test -q redemption` to verify tests pass
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 12. Add View Tests

Test the RedemptionView independently.

**Add to `src/redemption/view.rs`:**

```rust
#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use chrono::Utc;
    use cqrs_es::{EventEnvelope, View};
    use rust_decimal::Decimal;

    use super::RedemptionView;
    use crate::mint::{IssuerRequestId, Quantity};
    use crate::redemption::{Redemption, RedemptionEvent};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    #[test]
    fn test_view_starts_as_unavailable() {
        let view = RedemptionView::default();
        assert!(matches!(view, RedemptionView::Unavailable));
    }

    #[test]
    fn test_view_updates_on_detected_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-view-123");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;
        let detected_at = Utc::now();

        let event = EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: Default::default(),
        };

        view.update(&event);

        let RedemptionView::Detected {
            issuer_request_id: view_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
        } = view
        else {
            panic!("Expected Detected view, got Unavailable");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
    }
}
```

- [x] Add `#[cfg(test)] mod tests` section to `view.rs`
- [x] Implement `test_view_starts_as_unavailable`
- [x] Implement `test_view_updates_on_detected_event`
- [x] Run `cargo test -q redemption::view` to verify tests pass
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 13. Create RedemptionDetector Manager and Wire CQRS

Create the detector manager infrastructure. The real transfer monitoring will be added in Task 14.

**Create `src/redemption/detector.rs`:**

The detector manager bridges the TransferService with the Redemption aggregate by:
1. Using TransferService to get detected transfers
2. Converting Transfer data to RedemptionCommand::Detect
3. Executing commands via the CQRS framework

**Wire into application:**

Add CQRS and detector to application state with MockTransferService (real monitoring in Task 14).

- [x] Move `Quantity` and `QuantityConversionError` from `mint` to `lib.rs` (shared type)
- [x] Add `from_u256_with_18_decimals()` method to `Quantity`
- [x] Create `src/redemption/detector.rs`
- [x] Define `RedemptionDetector` struct
- [x] Implement `detect_transfers()` method using functional iterator chains
- [x] Add module to `src/redemption/mod.rs`
- [x] Add `RedemptionCqrs` type alias to `lib.rs`
- [x] Wire redemption CQRS in `initialize_rocket()`
- [x] Create detector with `MockTransferService::new_empty()` in `initialize_rocket()`
- [x] Add detector to Rocket state
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

## Task 14. Implement Transfer Monitoring via WebSocket Subscription

Implement real blockchain transfer monitoring using Alloy's WebSocket provider and event subscription patterns.

**Alloy WebSocket Pattern (from official docs):**
```rust
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::sol;

// 1. Connect via WebSocket
let ws = WsConnect::new("wss://eth-mainnet.example.com");
let provider = ProviderBuilder::new().connect_ws(ws).await?;

// 2. Create contract instance with sol! macro bindings
let contract = MyContractInstance::new(contract_address, &provider);

// 3. Create event filter and subscribe
let filter = contract.MyEvent_filter().filter;
let sub = provider.subscribe_logs(&filter).await?;
let mut stream = sub.into_stream();

// 4. Process events in loop
while let Some(log) = stream.next().await {
    // Decode and handle event
}
```

**Implementation:**

**Update `src/bindings.rs`:**
- The vault bindings should already have Transfer event from ERC-20
- Verify `Transfer(address indexed from, address indexed to, uint256 value)` event exists
- This is standard ERC-20, should be in OffchainAssetReceiptVault bindings

**Spawn monitoring task in `initialize_rocket()`:**

Instead of creating a service, directly spawn a background task that:
1. Connects to WebSocket: `ProviderBuilder::new().connect_ws(WsConnect::new(ws_url)).await?`
2. Creates vault contract instance: `VaultInstance::new(vault_address, &provider)`
3. Creates Transfer event filter with redemption wallet as `to` address
4. Subscribes: `provider.subscribe_logs(&filter).await?.into_stream()`
5. Loops forever processing events:
   ```rust
   while let Some(log) = stream.next().await {
       // Decode Transfer event
       // Build Transfer struct
       // Call detector.detect_transfers()
       // Log errors but keep running
   }
   ```
6. If connection drops, log error and exit (Rocket will handle restart)

**Update `src/lib.rs` Config:**
- Add `ws_rpc_url: String` field with env var `WS_RPC_URL`
- Add `redemption_wallet_address: Address` field with env var `REDEMPTION_WALLET_ADDRESS`
- `vault_address` already exists

**Remove TransferService abstraction:**
- Delete `src/transfer/service.rs` (not needed)
- Keep `TransferService` trait for `MockTransferService` in tests only
- Background task directly calls CQRS, no service layer needed

**Error handling:**
- Background task logs errors but keeps running
- Connection drops are fatal (task exits, relies on process restart)
- Individual event processing errors are logged but don't stop the stream

- [x] Verify Transfer event exists in vault bindings
- [x] Add WebSocket config fields to `Config` struct
- [x] Update `initialize_rocket()` to spawn WebSocket monitoring task
- [x] Implement event filter for redemption wallet address
- [x] Implement stream processing loop with error handling
- [x] Remove TransferService (keep trait for mock only)
- [x] Run `cargo build`
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [x] Run `cargo fmt`

## Task 15. Add RedemptionDetector Tests

Test the detector manager with mock transfer service.

- [ ] Add test module to `detector.rs`
- [ ] Test successful transfer detection
- [ ] Test transfer service failure handling
- [ ] Test empty transfer list
- [ ] Run `cargo test -q redemption::detector`
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

## Task 16. Documentation and Final Quality Checks

Ensure code quality and adherence to project guidelines.

**Quality Checklist:**

1. **All Tests Pass:**
   ```bash
   cargo test --workspace
   ```

2. **No Clippy Warnings:**
   ```bash
   cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings
   ```

3. **Code Formatted:**
   ```bash
   cargo fmt
   ```

4. **Application Runs:**
   ```bash
   cargo run
   ```
   Verify server starts and redemption infrastructure initializes.

**Code Review Checklist:**

- [ ] No panics in non-test code (no `unwrap()`, `expect()`, etc.)
- [ ] All imports follow two-group pattern (external, internal)
- [ ] Visibility levels as restrictive as possible (`pub(crate)` preferred)
- [ ] Enum states make invalid states unrepresentable
- [ ] Proper error handling with `Result` types
- [ ] No lint suppressions (`#[allow(...)]`)
- [ ] Comments only where adding non-obvious context
- [ ] Following SPEC.md command/event mappings

**Final Verification:**

- [ ] Run `cargo test --workspace` - all tests pass
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings` - no warnings
- [ ] Run `cargo fmt` - code formatted
- [ ] Run `cargo run` - application starts successfully
- [ ] Review code against guidelines checklist above
- [ ] Delete PLAN.md per project guidelines

## Notes

**Scope of This Task:**

This task implements ONLY the detection phase of redemption. Future tasks will add:
- Calling Alpaca's redeem endpoint (issue #22)
- Polling for journal completion (issue #23)
- ReceiptInventoryView for tracking receipt balances (issue #24)
- Token burning (issue #25)
- End-to-end tests (issue #26)

**Real MonitorService Implementation:**

The real `MonitorService` implementation (WebSocket-based blockchain monitoring) is deferred to a later task. This plan provides:
- The trait abstraction
- Mock implementation for testing
- Integration points for the real implementation

This is sufficient to develop and test dependent features. The real implementation will be added when needed for production deployment.

**TransferDetected Not Implemented as DomainEvent:**

The original issue mentioned implementing a `DomainEvent` trait. After analysis, this abstraction is not needed. The `TransferDetected` struct serves as a domain-specific data transfer object between the MonitorService and the manager that will execute commands. Adding a trait would introduce unnecessary complexity without clear benefits at this stage.
