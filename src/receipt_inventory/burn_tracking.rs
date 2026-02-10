use alloy::primitives::TxHash;
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use cqrs_es::persist::{GenericQuery, QueryReplay};
use cqrs_es::{AggregateError, EventEnvelope, View};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;

use super::{ReceiptId, ReceiptInventoryError, Shares, SharesOverflow};
use crate::redemption::IssuerRedemptionRequestId;
use crate::vault::ReceiptInformation;
use crate::redemption::{
    BurnRecord, Redemption, RedemptionError, RedemptionEvent,
};

/// Tracks burn operations for a redemption.
///
/// This is a cqrs-es view keyed by redemption aggregate_id (red-xxx).
/// Each successful burn creates one record. Use GenericQuery to update.
///
/// To compute available receipt balance, join with receipt_inventory_view
/// on receipt_id and subtract sum of burns from initial_amount.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum ReceiptBurnsView {
    #[default]
    Unavailable,
    Burned {
        /// The redemption's issuer_request_id
        redemption_issuer_request_id: IssuerRedemptionRequestId,
        /// All burns performed in this redemption (may span multiple receipts)
        burns: Vec<BurnRecord>,
        /// When the burn occurred
        burned_at: DateTime<Utc>,
    },
}

impl View<Redemption> for ReceiptBurnsView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        if let RedemptionEvent::TokensBurned {
            issuer_request_id,
            burns,
            burned_at,
            ..
        } = &event.payload
        {
            *self = Self::Burned {
                redemption_issuer_request_id: issuer_request_id.clone(),
                burns: burns.clone(),
                burned_at: *burned_at,
            };
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnTrackingError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Replay error: {0}")]
    Replay(#[from] AggregateError<RedemptionError>),
    #[error("ReceiptInventory aggregate error: {0}")]
    ReceiptInventory(#[from] AggregateError<ReceiptInventoryError>),
    #[error(
        "Insufficient balance for burn: required={required}, available={available}"
    )]
    InsufficientBalance { required: Shares, available: Shares },
    #[error(
        "Arithmetic overflow during burn allocation: \
         remaining={remaining}, burn_from_receipt={burn_from_receipt}"
    )]
    AllocationOverflow { remaining: Shares, burn_from_receipt: Shares },
    #[error(transparent)]
    SharesOverflow(#[from] SharesOverflow),
}

/// Represents a receipt with its available balance for burn planning.
#[derive(Debug, Clone)]
pub(crate) struct ReceiptWithBalance {
    pub(crate) receipt_id: ReceiptId,
    pub(crate) available_balance: Shares,
    pub(crate) tx_hash: TxHash,
    pub(crate) block_number: u64,
    pub(crate) receipt_info: Option<ReceiptInformation>,
}

/// A single allocation within a multi-receipt burn plan.
///
/// Represents how much to burn from a specific receipt.
#[derive(Debug, Clone)]
pub(crate) struct BurnAllocation {
    /// The receipt to burn from
    pub(crate) receipt: ReceiptWithBalance,
    /// Amount to burn from this receipt (may be partial)
    pub(crate) burn_amount: Shares,
}

/// A plan for burning shares across multiple receipts.
///
/// Created by `plan_burn()` to determine which receipts
/// to use and how much to burn from each.
#[derive(Debug, Clone)]
pub(crate) struct BurnPlan {
    /// Ordered list of allocations (largest receipts first)
    pub(crate) allocations: Vec<BurnAllocation>,
    /// Total amount to burn across all allocations
    pub(crate) total_burn: Shares,
    /// Dust amount to return to user
    pub(crate) dust: Shares,
}

/// Plans a multi-receipt burn by selecting from available receipts.
///
/// Selects receipts in descending order of available balance until the
/// required burn amount is satisfied. The last receipt may be partially burned.
///
/// This is a pure function - the caller is responsible for loading receipts
/// with their available balances using proper CQRS patterns.
///
/// # Arguments
///
/// * `receipts` - Available receipts with their balances (will be sorted internally)
/// * `burn_amount` - Total amount of shares to burn
/// * `dust_amount` - Amount of dust to return to user
///
/// # Errors
///
/// Returns `BurnTrackingError::InsufficientBalance` if total available
/// balance across all receipts is less than the burn amount.
pub(crate) fn plan_burn(
    receipts: Vec<ReceiptWithBalance>,
    total_burn: Shares,
    dust: Shares,
) -> Result<BurnPlan, BurnTrackingError> {
    let receipts: Vec<_> = receipts
        .into_iter()
        .filter(|receipt| !receipt.available_balance.is_zero())
        .sorted_by(|a, b| {
            b.available_balance.inner().cmp(&a.available_balance.inner())
        })
        .collect();

    let total_available: Shares = receipts
        .iter()
        .map(|receipt| receipt.available_balance)
        .try_fold(Shares::new(U256::ZERO), |acc, balance| acc + balance)?;

    if total_available.inner() < total_burn.inner() {
        return Err(BurnTrackingError::InsufficientBalance {
            required: total_burn,
            available: total_available,
        });
    }

    let allocations: Vec<BurnAllocation> = receipts
        .into_iter()
        .scan(total_burn, |remaining, receipt| {
            if remaining.is_zero() {
                return None;
            }

            let burn_from_this = remaining.min(receipt.available_balance);

            match remaining.checked_sub(burn_from_this) {
                Some(new_remaining) => {
                    *remaining = new_remaining;
                    Some(Ok(BurnAllocation {
                        receipt,
                        burn_amount: burn_from_this,
                    }))
                }
                None => Some(Err(BurnTrackingError::AllocationOverflow {
                    remaining: *remaining,
                    burn_from_receipt: burn_from_this,
                })),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(BurnPlan { allocations, total_burn, dust })
}

/// Replays all `Redemption` events through the `receipt_burns_view`.
///
/// Uses `QueryReplay` to re-project the view from existing events in the event store.
/// This is used at startup to recover view state after the view schema was added.
///
/// # Errors
///
/// Returns `BurnTrackingError::Replay` if event replay fails.
pub(crate) async fn replay_receipt_burns_view(
    pool: Pool<Sqlite>,
) -> Result<(), BurnTrackingError> {
    let view_repo =
        Arc::new(SqliteViewRepository::<ReceiptBurnsView, Redemption>::new(
            pool.clone(),
            "receipt_burns_view".to_string(),
        ));
    let query = GenericQuery::new(view_repo);

    let event_repo = SqliteEventRepository::new(pool);
    let replay = QueryReplay::new(event_repo, query);
    replay.replay_all().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, U256, address, b256, uint};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;
    use uuid::Uuid;

    use super::*;
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    fn new_redemption_id() -> IssuerRedemptionRequestId {
        let uuid = Uuid::new_v4();
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(uuid.as_bytes());
        IssuerRedemptionRequestId::new(TxHash::from(bytes))
    }

    async fn setup_test_db() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    #[test]
    fn test_view_updates_on_tokens_burned() {
        let mut view = ReceiptBurnsView::default();
        assert!(matches!(view, ReceiptBurnsView::Unavailable));

        let issuer_request_id = new_redemption_id();
        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            burns: vec![BurnRecord {
                receipt_id: uint!(42_U256),
                shares_burned: uint!(100_000000000000000000_U256),
            }],
            dust_returned: U256::ZERO,
            gas_used: 50000,
            block_number: 1000,
            burned_at: Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "red-456".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let ReceiptBurnsView::Burned {
            redemption_issuer_request_id,
            burns,
            ..
        } = view
        else {
            panic!("Expected Burned variant");
        };

        assert_eq!(redemption_issuer_request_id, issuer_request_id);
        assert_eq!(burns.len(), 1);
        assert_eq!(burns[0].receipt_id, uint!(42_U256));
        assert_eq!(burns[0].shares_burned, uint!(100_000000000000000000_U256));
    }

    #[test]
    fn test_view_ignores_other_events() {
        let mut view = ReceiptBurnsView::default();

        let issuer_request_id = new_redemption_id();
        let events = vec![
            RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                wallet: address!("0x1111111111111111111111111111111111111111"),
                quantity: Quantity::new(dec!(10)),
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                block_number: 500,
                detected_at: Utc::now(),
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                alpaca_quantity: Quantity::new(dec!(10)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at: Utc::now(),
            },
            RedemptionEvent::BurningFailed {
                issuer_request_id,
                error: "test error".to_string(),
                failed_at: Utc::now(),
            },
        ];

        for event in events {
            let envelope = EventEnvelope {
                aggregate_id: "red-123".to_string(),
                sequence: 1,
                payload: event,
                metadata: HashMap::new(),
            };
            view.update(&envelope);
        }

        assert!(
            matches!(view, ReceiptBurnsView::Unavailable),
            "View should remain Unavailable for non-burn events"
        );
    }

    /// Tests re-projection of receipt_burns_view from existing TokensBurned events.
    ///
    /// This simulates a production scenario where the view table is empty but
    /// TokensBurned events exist in the event store. The re-projection should
    /// scan events and populate the view.
    #[tokio::test]
    async fn test_reproject_burns_from_events_populates_view() {
        let pool = setup_test_db().await;

        let aggregate_id = "red-reproject-123";
        let receipt_id = uint!(42_U256);
        let shares_burned = uint!(100_000000000000000000_U256);

        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: new_redemption_id(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            burns: vec![BurnRecord { receipt_id, shares_burned }],
            dust_returned: U256::ZERO,
            gas_used: 50000,
            block_number: 1000,
            burned_at: Utc::now(),
        };

        let payload = serde_json::to_string(&event).unwrap();
        sqlx::query!(
            r#"
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('Redemption', ?, 1, 'TokensBurned', '2.0', ?, '{}')
            "#,
            aggregate_id,
            payload
        )
        .execute(&pool)
        .await
        .unwrap();

        let view_repo = Arc::new(SqliteViewRepository::<
            ReceiptBurnsView,
            Redemption,
        >::new(
            pool.clone(),
            "receipt_burns_view".to_string(),
        ));
        let query = GenericQuery::new(view_repo);

        // Verify view is initially empty
        assert!(
            query.load(aggregate_id).await.is_none(),
            "View should be empty before re-projection"
        );

        // Run replay
        replay_receipt_burns_view(pool.clone())
            .await
            .expect("Replay should succeed");

        // Verify view is now populated via GenericQuery
        let view = query
            .load(aggregate_id)
            .await
            .expect("View should have been populated after replay");

        let ReceiptBurnsView::Burned { burns, .. } = view else {
            panic!("Expected Burned variant");
        };

        assert_eq!(burns.len(), 1);
        assert_eq!(burns[0].receipt_id, receipt_id);
        assert_eq!(burns[0].shares_burned, shares_burned);
    }

    /// Tests that re-projection handles multiple TokensBurned events correctly.
    #[tokio::test]
    async fn test_reproject_burns_handles_multiple_events() {
        let pool = setup_test_db().await;

        // Insert multiple TokensBurned events for different redemptions
        for i in 1_u64..=3 {
            let aggregate_id = format!("red-multi-{i}");
            let event = RedemptionEvent::TokensBurned {
                issuer_request_id: new_redemption_id(),
                tx_hash: b256!(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                burns: vec![BurnRecord {
                    receipt_id: U256::from(i),
                    shares_burned: U256::from(i * 100),
                }],
                dust_returned: U256::ZERO,
                gas_used: 50000,
                block_number: 1000 + i,
                burned_at: Utc::now(),
            };

            let payload = serde_json::to_string(&event).unwrap();
            sqlx::query!(
                r#"
                INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                VALUES ('Redemption', ?, 1, 'TokensBurned', '2.0', ?, '{}')
                "#,
                aggregate_id,
                payload
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        replay_receipt_burns_view(pool.clone())
            .await
            .expect("Replay should succeed");

        // Verify all three burns were projected via GenericQuery
        let view_repo = Arc::new(SqliteViewRepository::<
            ReceiptBurnsView,
            Redemption,
        >::new(
            pool.clone(),
            "receipt_burns_view".to_string(),
        ));
        let query = GenericQuery::new(view_repo);

        for i in 1_u64..=3 {
            let aggregate_id = format!("red-multi-{i}");
            let view = query.load(&aggregate_id).await.unwrap_or_else(|| {
                panic!("View should exist for {aggregate_id}")
            });

            let ReceiptBurnsView::Burned { burns, .. } = view else {
                panic!("Expected Burned variant for {aggregate_id}");
            };

            assert_eq!(burns.len(), 1);
            assert_eq!(burns[0].receipt_id, U256::from(i));
            assert_eq!(burns[0].shares_burned, U256::from(i * 100));
        }
    }

    /// Tests that re-projection is idempotent - running it twice doesn't duplicate entries.
    #[tokio::test]
    async fn test_reproject_burns_is_idempotent() {
        let pool = setup_test_db().await;

        let aggregate_id = "red-idempotent-123";
        let receipt_id = uint!(42_U256);
        let shares_burned = uint!(100_000000000000000000_U256);

        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: new_redemption_id(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            burns: vec![BurnRecord { receipt_id, shares_burned }],
            dust_returned: U256::ZERO,
            gas_used: 50000,
            block_number: 1000,
            burned_at: Utc::now(),
        };

        let payload = serde_json::to_string(&event).unwrap();
        sqlx::query!(
            r#"
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('Redemption', ?, 1, 'TokensBurned', '2.0', ?, '{}')
            "#,
            aggregate_id,
            payload
        )
        .execute(&pool)
        .await
        .unwrap();

        // Run replay twice
        replay_receipt_burns_view(pool.clone())
            .await
            .expect("First replay should succeed");

        replay_receipt_burns_view(pool.clone())
            .await
            .expect("Second replay should succeed");

        // Verify view still has correct data via GenericQuery
        let view_repo = Arc::new(SqliteViewRepository::<
            ReceiptBurnsView,
            Redemption,
        >::new(
            pool.clone(),
            "receipt_burns_view".to_string(),
        ));
        let query = GenericQuery::new(view_repo);

        let view = query
            .load(aggregate_id)
            .await
            .expect("View should exist after replay");

        let ReceiptBurnsView::Burned { burns, .. } = view else {
            panic!("Expected Burned variant");
        };

        assert_eq!(burns.len(), 1, "Re-projection should be idempotent");
        assert_eq!(burns[0].receipt_id, receipt_id);
        assert_eq!(burns[0].shares_burned, shares_burned);
    }

    /// When a single receipt has sufficient balance, plan should return 1 allocation.
    #[test]
    fn test_plan_single_receipt_sufficient_returns_one_allocation() {
        let receipts = vec![ReceiptWithBalance {
            receipt_id: ReceiptId::from(uint!(1_U256)),
            available_balance: Shares::new(uint!(100_000000000000000000_U256)),
            tx_hash: TxHash::ZERO,
            block_number: 0,
            receipt_info: None,
        }];

        let burn_amount = Shares::new(uint!(50_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let plan = plan_burn(receipts, burn_amount, dust_amount)
            .expect("Plan should succeed");

        assert_eq!(
            plan.allocations.len(),
            1,
            "Should return exactly 1 allocation"
        );
        assert_eq!(plan.total_burn, burn_amount);
        assert_eq!(plan.dust, dust_amount);
        assert_eq!(plan.allocations[0].burn_amount, burn_amount);
        assert_eq!(
            plan.allocations[0].receipt.receipt_id,
            ReceiptId::from(uint!(1_U256))
        );
    }

    /// When multiple receipts are needed to satisfy the burn, plan should return N allocations.
    #[test]
    fn test_plan_multiple_receipts_needed_returns_n_allocations() {
        let receipts = vec![
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(1_U256)),
                available_balance: Shares::new(uint!(
                    30_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(2_U256)),
                available_balance: Shares::new(uint!(
                    40_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(3_U256)),
                available_balance: Shares::new(uint!(
                    50_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
        ];

        let burn_amount = Shares::new(uint!(80_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let plan = plan_burn(receipts, burn_amount, dust_amount)
            .expect("Plan should succeed");

        // Should use 2 receipts (50 + 40 = 90 > 80)
        assert_eq!(
            plan.allocations.len(),
            2,
            "Should return 2 allocations, got {}",
            plan.allocations.len()
        );
        assert_eq!(plan.total_burn, burn_amount);

        // Total of all burn_amounts should equal burn_amount
        let total_allocated: Shares = plan
            .allocations
            .iter()
            .map(|alloc| alloc.burn_amount)
            .try_fold(Shares::new(U256::ZERO), |acc, shares| acc + shares)
            .unwrap();
        assert_eq!(
            total_allocated, burn_amount,
            "Sum of allocations must equal burn amount"
        );
    }

    /// When total available balance is insufficient, should return InsufficientBalance error.
    #[test]
    fn test_plan_insufficient_total_balance_returns_error() {
        let receipts = vec![ReceiptWithBalance {
            receipt_id: ReceiptId::from(uint!(1_U256)),
            available_balance: Shares::new(uint!(50_000000000000000000_U256)),
            tx_hash: TxHash::ZERO,
            block_number: 0,
            receipt_info: None,
        }];

        let burn_amount = Shares::new(uint!(100_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let result = plan_burn(receipts, burn_amount, dust_amount);

        let err =
            result.expect_err("Should return error for insufficient balance");
        assert!(
            matches!(err, BurnTrackingError::InsufficientBalance { required, available }
                if required == burn_amount && available == Shares::new(uint!(50_000000000000000000_U256))),
            "Expected InsufficientBalance error, got {err:?}"
        );
    }

    /// Plan should select largest receipts first to minimize number of allocations.
    #[test]
    fn test_plan_burns_from_largest_receipts_first() {
        // Receipts provided in non-sorted order to verify sorting
        let receipts = vec![
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(1_U256)),
                available_balance: Shares::new(uint!(
                    10_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(2_U256)),
                available_balance: Shares::new(uint!(
                    30_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(3_U256)),
                available_balance: Shares::new(uint!(
                    60_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
        ];

        let burn_amount = Shares::new(uint!(70_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let plan = plan_burn(receipts, burn_amount, dust_amount)
            .expect("Plan should succeed");

        assert_eq!(plan.allocations.len(), 2, "Should use exactly 2 receipts");

        // First allocation should be from largest receipt (id=3, 60 tokens)
        assert_eq!(
            plan.allocations[0].receipt.receipt_id,
            ReceiptId::from(uint!(3_U256)),
            "First allocation should be from largest receipt"
        );
        assert_eq!(
            plan.allocations[0].burn_amount,
            Shares::new(uint!(60_000000000000000000_U256)),
            "First allocation should burn full amount"
        );

        // Second allocation should be partial from medium receipt (id=2)
        assert_eq!(
            plan.allocations[1].receipt.receipt_id,
            ReceiptId::from(uint!(2_U256)),
            "Second allocation should be from medium receipt"
        );
        assert_eq!(
            plan.allocations[1].burn_amount,
            Shares::new(uint!(10_000000000000000000_U256)),
            "Second allocation should be partial (10 of 30)"
        );
    }

    /// Last receipt allocation should be partial (exact amount needed, not full receipt).
    #[test]
    fn test_plan_last_allocation_is_partial() {
        let receipts = vec![
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(1_U256)),
                available_balance: Shares::new(uint!(
                    100_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(2_U256)),
                available_balance: Shares::new(uint!(
                    50_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
        ];

        let burn_amount = Shares::new(uint!(120_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let plan = plan_burn(receipts, burn_amount, dust_amount)
            .expect("Plan should succeed");

        assert_eq!(plan.allocations.len(), 2, "Should use exactly 2 receipts");

        // First allocation should fully deplete the larger receipt
        assert_eq!(
            plan.allocations[0].burn_amount,
            Shares::new(uint!(100_000000000000000000_U256)),
            "First allocation should burn full 100 tokens"
        );

        // Last allocation should be partial - exactly what's needed
        let last = plan.allocations.last().unwrap();
        assert_eq!(
            last.burn_amount,
            Shares::new(uint!(20_000000000000000000_U256)),
            "Last allocation should burn exactly 20 tokens (not full 50)"
        );

        // Total should equal requested burn amount
        let total: Shares = plan
            .allocations
            .iter()
            .map(|alloc| alloc.burn_amount)
            .try_fold(Shares::new(U256::ZERO), |acc, shares| acc + shares)
            .unwrap();
        assert_eq!(
            total, burn_amount,
            "Total allocations must equal burn amount"
        );
    }

    /// Plan uses pre-computed available balances (caller handles burn deduction).
    #[test]
    fn test_plan_uses_provided_available_balances() {
        // Receipt 1: initial 100, already burned 60 → 40 available
        // Receipt 2: initial 50, no burns → 50 available
        let receipts = vec![
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(1_U256)),
                available_balance: Shares::new(uint!(
                    40_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(2_U256)),
                available_balance: Shares::new(uint!(
                    50_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
        ];

        // Available: 40 + 50 = 90, need 70
        let burn_amount = Shares::new(uint!(70_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let plan = plan_burn(receipts, burn_amount, dust_amount)
            .expect("Plan should succeed with adjusted balances");

        // Should use both receipts (50 from id=2, then 20 from id=1)
        assert_eq!(plan.allocations.len(), 2, "Should use 2 receipts");

        // Verify total matches burn amount
        let total: Shares = plan
            .allocations
            .iter()
            .map(|alloc| alloc.burn_amount)
            .try_fold(Shares::new(U256::ZERO), |acc, shares| acc + shares)
            .unwrap();
        assert_eq!(total, burn_amount);

        // First receipt should be id=2 (50 available > 40 available)
        assert_eq!(
            plan.allocations[0].receipt.receipt_id,
            ReceiptId::from(uint!(2_U256)),
            "Should select receipt with higher available balance first"
        );
        assert_eq!(
            plan.allocations[0].receipt.available_balance,
            Shares::new(uint!(50_000000000000000000_U256))
        );
    }

    /// Plan with empty receipt list returns InsufficientBalance error.
    #[test]
    fn test_plan_empty_receipts_returns_error() {
        let receipts: Vec<ReceiptWithBalance> = vec![];

        let burn_amount = Shares::new(uint!(50_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let result = plan_burn(receipts, burn_amount, dust_amount);

        let err = result.expect_err("Should fail with empty receipts");
        assert!(
            matches!(err, BurnTrackingError::InsufficientBalance { available, .. }
                if available.is_zero()),
            "Expected InsufficientBalance with zero available, got {err:?}"
        );
    }

    /// Plan should include dust amount in the result.
    #[test]
    fn test_plan_includes_dust_amount() {
        let receipts = vec![ReceiptWithBalance {
            receipt_id: ReceiptId::from(uint!(1_U256)),
            available_balance: Shares::new(uint!(100_000000000000000000_U256)),
            tx_hash: TxHash::ZERO,
            block_number: 0,
            receipt_info: None,
        }];

        let burn_amount = Shares::new(uint!(50_000000000000000000_U256));
        let dust_amount = Shares::new(uint!(123456789_U256));

        let plan = plan_burn(receipts, burn_amount, dust_amount)
            .expect("Plan should succeed");

        assert_eq!(plan.dust, dust_amount, "Plan should include dust amount");
        assert_eq!(plan.total_burn, burn_amount);
    }

    /// Plan filters out receipts with zero balance.
    #[test]
    fn test_plan_filters_zero_balance_receipts() {
        let receipts = vec![
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(1_U256)),
                available_balance: Shares::new(U256::ZERO),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(uint!(2_U256)),
                available_balance: Shares::new(uint!(
                    100_000000000000000000_U256
                )),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            },
        ];

        let burn_amount = Shares::new(uint!(50_000000000000000000_U256));
        let dust_amount = Shares::new(U256::ZERO);

        let plan = plan_burn(receipts, burn_amount, dust_amount)
            .expect("Plan should succeed");

        assert_eq!(
            plan.allocations.len(),
            1,
            "Should only use receipt with non-zero balance"
        );
        assert_eq!(
            plan.allocations[0].receipt.receipt_id,
            ReceiptId::from(uint!(2_U256))
        );
    }

    prop_compose! {
        fn arb_receipt_with_balance(max_balance: u64)(
            id in 1u64..1000,
            balance in 1u64..=max_balance,
        ) -> ReceiptWithBalance {
            ReceiptWithBalance {
                receipt_id: ReceiptId::from(U256::from(id)),
                available_balance: Shares::new(U256::from(balance)),
                tx_hash: TxHash::ZERO,
                block_number: 0,
                receipt_info: None,
            }
        }
    }

    prop_compose! {
        fn arb_receipts_vec(max_len: usize, max_balance: u64)(
            receipts in prop::collection::vec(arb_receipt_with_balance(max_balance), 1..=max_len)
        ) -> Vec<ReceiptWithBalance> {
            receipts
        }
    }

    proptest! {
        /// Sum of allocation burn amounts equals requested burn amount.
        #[test]
        fn prop_allocations_sum_equals_burn_amount(
            receipts in arb_receipts_vec(10, 1000),
        ) {
            let total_available: u64 = receipts
                .iter()
                .map(|receipt| receipt.available_balance.inner().to::<u64>())
                .sum();

            if total_available > 0 {
                let burn_amount = Shares::new(U256::from(total_available / 2));
                let dust = Shares::new(U256::ZERO);

                let plan = plan_burn(receipts, burn_amount, dust).unwrap();

                let allocated: Shares = plan.allocations.iter()
                    .map(|alloc| alloc.burn_amount)
                    .try_fold(Shares::new(U256::ZERO), |acc, shares| acc + shares)
                    .unwrap();
                prop_assert_eq!(allocated, burn_amount);
            }
        }

        /// No allocation burns more than the receipt's available balance.
        #[test]
        fn prop_no_allocation_exceeds_available(
            receipts in arb_receipts_vec(10, 1000),
        ) {
            let total_available: u64 = receipts
                .iter()
                .map(|receipt| receipt.available_balance.inner().to::<u64>())
                .sum();

            if total_available > 0 {
                let burn_amount = Shares::new(U256::from(total_available / 2));
                let dust = Shares::new(U256::ZERO);

                let plan = plan_burn(receipts, burn_amount, dust).unwrap();

                for alloc in &plan.allocations {
                    prop_assert!(
                        alloc.burn_amount.inner() <= alloc.receipt.available_balance.inner(),
                        "Burn {} exceeds available {}",
                        alloc.burn_amount,
                        alloc.receipt.available_balance
                    );
                }
            }
        }

        /// Allocations are sorted by available balance descending.
        #[test]
        fn prop_allocations_sorted_by_balance_desc(
            receipts in arb_receipts_vec(10, 1000),
        ) {
            let total_available: u64 = receipts
                .iter()
                .map(|receipt| receipt.available_balance.inner().to::<u64>())
                .sum();

            if total_available > 0 {
                let burn_amount = Shares::new(U256::from(total_available / 2));
                let dust = Shares::new(U256::ZERO);

                let plan = plan_burn(receipts, burn_amount, dust).unwrap();

                for window in plan.allocations.windows(2) {
                    prop_assert!(
                        window[0].receipt.available_balance.inner()
                            >= window[1].receipt.available_balance.inner(),
                        "Allocations not sorted: {} < {}",
                        window[0].receipt.available_balance,
                        window[1].receipt.available_balance
                    );
                }
            }
        }

        /// Insufficient balance returns error.
        #[test]
        fn prop_insufficient_balance_returns_error(
            receipts in arb_receipts_vec(5, 100),
        ) {
            let total_available: u64 = receipts
                .iter()
                .map(|receipt| receipt.available_balance.inner().to::<u64>())
                .sum();

            let burn_amount = Shares::new(U256::from(total_available + 1));
            let dust = Shares::new(U256::ZERO);

            let result = plan_burn(receipts, burn_amount, dust);
            prop_assert!(
                matches!(result, Err(BurnTrackingError::InsufficientBalance { .. })),
                "Expected InsufficientBalance error"
            );
        }

        /// Dust amount is preserved in the plan.
        #[test]
        fn prop_dust_preserved(
            receipts in arb_receipts_vec(5, 1000),
            dust_val in 0u64..1000,
        ) {
            let total_available: u64 = receipts
                .iter()
                .map(|receipt| receipt.available_balance.inner().to::<u64>())
                .sum();

            if total_available > 0 {
                let burn_amount = Shares::new(U256::from(total_available / 2));
                let dust = Shares::new(U256::from(dust_val));

                let plan = plan_burn(receipts, burn_amount, dust).unwrap();
                prop_assert_eq!(plan.dust, dust);
            }
        }
    }
}
