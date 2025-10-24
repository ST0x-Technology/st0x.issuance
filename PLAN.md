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

- [ ] Update `event.rs` with `RedemptionEvent::Detected` variant
- [ ] Add all necessary imports following two-group pattern
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

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

- [ ] Update `cmd.rs` with `RedemptionCommand::Detect` variant
- [ ] Add all necessary imports following two-group pattern
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

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

- [ ] Replace `Redemption` struct with enum in `mod.rs`
- [ ] Add `Uninitialized` and `Detected` variants
- [ ] Implement `Default` trait
- [ ] Add necessary imports following two-group pattern
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

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

- [ ] Define `RedemptionError` enum
- [ ] Implement `Aggregate` trait for `Redemption`
- [ ] Implement `handle` method with validation and event production
- [ ] Implement `apply` method for state transitions
- [ ] Add necessary imports
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

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

- [ ] Update `view.rs` with `RedemptionView` enum
- [ ] Add `Unavailable` and `Detected` variants
- [ ] Implement `Default` trait
- [ ] Implement `View<Redemption>` trait with `update` method
- [ ] Add necessary imports following two-group pattern
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

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

- [ ] Run `sqlx migrate add create_redemption_view`
- [ ] Edit generated migration file with CREATE TABLE statement
- [ ] Add indexes for common queries
- [ ] Run `sqlx db reset -y` to test migration
- [ ] Verify database schema with `sqlite3 data.db ".schema redemption_view"`
- [ ] Run `cargo build` to ensure sqlx is happy

## Task 8. Define MonitorService Trait

Create the service trait for monitoring blockchain transfers.

**Create `src/monitor/mod.rs`:**

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
pub(crate) trait MonitorService: Send + Sync {
    async fn watch_transfers(&self) -> Result<Vec<TransferDetected>, MonitorError>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MonitorError {
    #[error("Failed to connect to blockchain: {0}")]
    ConnectionFailed(String),

    #[error("Failed to fetch transfers: {0}")]
    FetchFailed(String),

    #[error("Invalid transfer data: {0}")]
    InvalidData(String),
}
```

**Update `src/lib.rs`:**
Add `pub(crate) mod monitor;` after the other module declarations.

**Design Notes:**

- `TransferDetected` is the domain event representing a detected transfer
- Contains `from` address (wallet that sent tokens) and `amount` as U256
- Service returns `Vec<TransferDetected>` to support batch processing
- `watch_transfers()` is a pull-based API (manager polls for transfers)

- [ ] Create `src/monitor/` directory
- [ ] Create `mod.rs` with `MonitorService` trait
- [ ] Define `TransferDetected` struct
- [ ] Define `MonitorError` enum
- [ ] Add `pub(crate) mod monitor;` to `src/lib.rs`
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

## Task 9. Implement MockMonitorService

Create a mock implementation for testing.

**Create `src/monitor/mock.rs`:**

```rust
use async_trait::async_trait;

use super::{MonitorError, MonitorService, TransferDetected};

pub(crate) struct MockMonitorService {
    transfers: Vec<TransferDetected>,
    should_fail: bool,
}

impl MockMonitorService {
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
impl MonitorService for MockMonitorService {
    async fn watch_transfers(&self) -> Result<Vec<TransferDetected>, MonitorError> {
        if self.should_fail {
            return Err(MonitorError::FetchFailed(
                "Mock failure".to_string(),
            ));
        }
        Ok(self.transfers.clone())
    }
}
```

**Update `src/monitor/mod.rs`:**
Add `pub(crate) mod mock;` at the top of the file.

**Design Notes:**

The mock enables testing different scenarios:
- `new_with_transfers()` - Simulate detected transfers
- `new_failing()` - Simulate service errors
- `new_empty()` - Simulate no transfers

All constructors are `#[cfg(test)]` since the mock is only for testing.

- [ ] Create `mock.rs` in `src/monitor/`
- [ ] Implement `MockMonitorService` struct
- [ ] Implement test constructors with `#[cfg(test)]`
- [ ] Implement `MonitorService` trait
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

- [ ] Add `redemption` imports to `lib.rs`
- [ ] Add `RedemptionCqrs` type alias
- [ ] Create redemption view repository in `initialize_rocket()`
- [ ] Create `GenericQuery` for redemption
- [ ] Create redemption CQRS framework instance
- [ ] Add redemption CQRS to Rocket state
- [ ] Run `cargo build` to verify wiring compiles
- [ ] Run `cargo run` and verify application starts successfully
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

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

- [ ] Add `#[cfg(test)] mod tests` section to `mod.rs`
- [ ] Implement `test_detect_redemption_creates_event`
- [ ] Implement `test_detect_redemption_when_already_detected_returns_error`
- [ ] Implement `test_apply_detected_event_updates_state`
- [ ] Run `cargo test -q redemption` to verify tests pass
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

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

- [ ] Add `#[cfg(test)] mod tests` section to `view.rs`
- [ ] Implement `test_view_starts_as_unavailable`
- [ ] Implement `test_view_updates_on_detected_event`
- [ ] Run `cargo test -q redemption::view` to verify tests pass
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
- [ ] Run `cargo fmt`

## Task 13. Documentation and Final Quality Checks

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
