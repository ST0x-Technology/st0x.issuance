use alloy::primitives::{U256, ruint::ParseError};
use chrono::{DateTime, Utc};
use cqrs_es::persist::{GenericQuery, QueryReplay};
use cqrs_es::{AggregateError, EventEnvelope, View};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository};
use sqlx::{Pool, Sqlite};
use std::collections::HashMap;
use std::sync::Arc;

use crate::mint::IssuerRequestId;
use crate::redemption::{Redemption, RedemptionError, RedemptionEvent};
use crate::tokenized_asset::UnderlyingSymbol;

/// Tracks a single burn operation.
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
        /// The on-chain receipt ID that was burned from
        receipt_id: U256,
        /// The redemption's issuer_request_id
        redemption_issuer_request_id: IssuerRequestId,
        /// Number of shares burned
        shares_burned: U256,
        /// When the burn occurred
        burned_at: DateTime<Utc>,
    },
}

impl View<Redemption> for ReceiptBurnsView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        if let RedemptionEvent::TokensBurned { .. } = &event.payload {
            // TODO Task 8: Extract burns from v2.0 event and update view
            todo!(
                "Task 8: Update ReceiptBurnsView from TokensBurned v2.0 event"
            )
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnTrackingError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("JSON deserialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("Replay error: {0}")]
    Replay(#[from] AggregateError<RedemptionError>),
    #[error(
        "Burns exceed initial amount for receipt {receipt_id}: \
         initial_amount={initial_amount}, total_burned={total_burned}"
    )]
    BurnsExceedInitialAmount {
        receipt_id: U256,
        initial_amount: U256,
        total_burned: U256,
    },
    #[error(
        "Insufficient balance for burn: required={required}, available={available}"
    )]
    InsufficientBalance { required: U256, available: U256 },
}

/// Represents a receipt with its available balance (initial - burned).
#[derive(Debug, Clone)]
pub(crate) struct ReceiptWithBalance {
    pub(crate) receipt_id: U256,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) initial_amount: U256,
    pub(crate) total_burned: U256,
    pub(crate) available_balance: U256,
    /// The mint's issuer_request_id (for traceability)
    pub(crate) mint_issuer_request_id: IssuerRequestId,
}

/// A single allocation within a multi-receipt burn plan.
///
/// Represents how much to burn from a specific receipt.
#[derive(Debug, Clone)]
pub(crate) struct BurnAllocation {
    /// The receipt to burn from
    pub(crate) receipt: ReceiptWithBalance,
    /// Amount to burn from this receipt (may be partial)
    pub(crate) burn_amount: U256,
}

/// A plan for burning shares across multiple receipts.
///
/// Created by `plan_multi_receipt_burn()` to determine which receipts
/// to use and how much to burn from each.
#[derive(Debug, Clone)]
pub(crate) struct BurnPlan {
    /// Ordered list of allocations (largest receipts first)
    pub(crate) allocations: Vec<BurnAllocation>,
    /// Total amount to burn across all allocations
    pub(crate) total_burn: U256,
    /// Dust amount to return to user
    pub(crate) dust: U256,
}

/// Plans a multi-receipt burn by selecting receipts with available balance.
///
/// Selects receipts in descending order of available balance until the
/// required burn amount is satisfied. The last receipt may be partially burned.
///
/// # Arguments
///
/// * `pool` - Database connection pool
/// * `underlying` - The underlying asset symbol to filter receipts
/// * `burn_amount` - Total amount of shares to burn
/// * `dust_amount` - Amount of dust to return to user
///
/// # Returns
///
/// A `BurnPlan` with allocations for each receipt, or an error if
/// insufficient balance is available.
///
/// # Errors
///
/// Returns `BurnTrackingError::InsufficientBalance` if total available
/// balance across all receipts is less than the burn amount.
pub(crate) async fn plan_multi_receipt_burn(
    _pool: &Pool<Sqlite>,
    _underlying: &UnderlyingSymbol,
    _burn_amount: U256,
    _dust_amount: U256,
) -> Result<BurnPlan, BurnTrackingError> {
    todo!("Task 4: Implement multi-receipt burn planning")
}

fn parse_u256_hex(s: &str) -> Result<U256, ParseError> {
    U256::from_str_radix(s.trim_start_matches("0x"), 16)
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

/// Finds a receipt with sufficient available balance for burning.
///
/// Queries receipt_inventory_view and receipt_burns_view separately, then
/// computes available balance in Rust using proper U256 arithmetic.
/// Returns the receipt with the highest available balance that meets the
/// minimum requirement.
///
/// # Errors
///
/// Returns `BurnTrackingError::Database` if a database query fails.
/// Returns `BurnTrackingError::Parse` if a U256 hex value cannot be parsed.
pub(crate) async fn find_receipt_with_available_balance(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
    minimum_balance: U256,
) -> Result<Option<ReceiptWithBalance>, BurnTrackingError> {
    let underlying_str = &underlying.0;

    let receipt_rows = sqlx::query!(
        r#"
        SELECT
            view_id as "mint_issuer_request_id!: String",
            json_extract(payload, '$.Active.receipt_id') as "receipt_id!: String",
            json_extract(payload, '$.Active.initial_amount') as "initial_amount!: String"
        FROM receipt_inventory_view
        WHERE json_extract(payload, '$.Active.underlying') = ?
        "#,
        underlying_str
    )
    .fetch_all(pool)
    .await?;

    let burn_rows = sqlx::query!(
        r#"
        SELECT
            json_extract(payload, '$.Burned.receipt_id') as "receipt_id!: String",
            json_extract(payload, '$.Burned.shares_burned') as "shares_burned!: String"
        FROM receipt_burns_view
        WHERE json_extract(payload, '$.Burned') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    let burns_by_receipt: HashMap<String, U256> = burn_rows
        .into_iter()
        .map(|b| {
            parse_u256_hex(&b.shares_burned)
                .map(|shares| (b.receipt_id, shares))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .into_group_map()
        .into_iter()
        .map(|(receipt_id, burns)| (receipt_id, burns.into_iter().sum()))
        .collect();

    receipt_rows
        .into_iter()
        .map(|row| {
            let receipt_id = parse_u256_hex(&row.receipt_id)?;
            let initial_amount = parse_u256_hex(&row.initial_amount)?;
            let total_burned = burns_by_receipt
                .get(&row.receipt_id)
                .copied()
                .unwrap_or(U256::ZERO);
            let available_balance = initial_amount
                .checked_sub(total_burned)
                .ok_or(BurnTrackingError::BurnsExceedInitialAmount {
                    receipt_id,
                    initial_amount,
                    total_burned,
                })?;

            Ok(ReceiptWithBalance {
                receipt_id,
                underlying: underlying.clone(),
                initial_amount,
                total_burned,
                available_balance,
                mint_issuer_request_id: IssuerRequestId::new(
                    &row.mint_issuer_request_id,
                ),
            })
        })
        .collect::<Result<Vec<_>, BurnTrackingError>>()
        .map(|receipts| {
            receipts
                .into_iter()
                .filter(|r| r.available_balance >= minimum_balance)
                .max_by_key(|r| r.available_balance)
        })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::receipt_inventory::ReceiptInventoryView;
    use crate::redemption::BurnRecord;
    use crate::tokenized_asset::TokenSymbol;

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

    /// Tests that find_receipt_with_available_balance correctly handles
    /// U256 values that exceed i64::MAX (which is ~9.2 * 10^18).
    ///
    /// With 18 decimal places, even 10 tokens = 10 * 10^18 = 10^19 > i64::MAX.
    /// This test verifies the implementation uses proper U256 arithmetic.
    #[tokio::test]
    async fn test_find_receipt_handles_u256_values_exceeding_i64() {
        let pool = setup_test_db().await;

        let underlying = UnderlyingSymbol::new("AAPL");

        // 100 tokens with 18 decimals = 100 * 10^18 = 10^20, far exceeds i64::MAX
        let initial_amount = uint!(100_000000000000000000_U256);
        // 30 tokens burned
        let shares_burned = uint!(30_000000000000000000_U256);
        // Expected available: 70 tokens
        let expected_available = uint!(70_000000000000000000_U256);

        // Insert an active receipt
        let receipt_view = ReceiptInventoryView::Active {
            receipt_id: uint!(42_U256),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tAAPL"),
            initial_amount,
            current_balance: initial_amount, // Note: this field is stale, we compute from burns
            minted_at: Utc::now(),
        };
        let receipt_payload = serde_json::to_string(&receipt_view).unwrap();

        sqlx::query!(
            "INSERT INTO receipt_inventory_view (view_id, version, payload) VALUES (?, 1, ?)",
            "iss-123",
            receipt_payload
        )
        .execute(&pool)
        .await
        .unwrap();

        // Insert a burn record
        let burn_view = ReceiptBurnsView::Burned {
            receipt_id: uint!(42_U256),
            redemption_issuer_request_id: IssuerRequestId::new("red-456"),
            shares_burned,
            burned_at: Utc::now(),
        };
        let burn_payload = serde_json::to_string(&burn_view).unwrap();

        sqlx::query!(
            "INSERT INTO receipt_burns_view (view_id, version, payload) VALUES (?, 1, ?)",
            "red-456",
            burn_payload
        )
        .execute(&pool)
        .await
        .unwrap();

        // Query for a receipt with at least 50 tokens available
        let minimum = uint!(50_000000000000000000_U256);
        let result =
            find_receipt_with_available_balance(&pool, &underlying, minimum)
                .await
                .expect("Query should succeed");

        let receipt =
            result.expect("Should find a receipt with sufficient balance");

        assert_eq!(receipt.receipt_id, uint!(42_U256));
        assert_eq!(receipt.initial_amount, initial_amount);
        assert_eq!(receipt.total_burned, shares_burned);
        assert_eq!(
            receipt.available_balance, expected_available,
            "Available balance should be computed correctly with U256 arithmetic"
        );
    }

    #[test]
    fn test_view_updates_on_tokens_burned() {
        let mut view = ReceiptBurnsView::default();
        assert!(matches!(view, ReceiptBurnsView::Unavailable));

        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-123"),
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
            receipt_id,
            redemption_issuer_request_id,
            shares_burned,
            ..
        } = view
        else {
            panic!("Expected Burned variant");
        };

        assert_eq!(receipt_id, uint!(42_U256));
        assert_eq!(
            redemption_issuer_request_id,
            IssuerRequestId::new("red-123")
        );
        assert_eq!(shares_burned, uint!(100_000000000000000000_U256));
    }

    #[test]
    fn test_view_ignores_other_events() {
        let mut view = ReceiptBurnsView::default();

        let events = vec![
            RedemptionEvent::Detected {
                issuer_request_id: IssuerRequestId::new("red-123"),
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
                issuer_request_id: IssuerRequestId::new("red-123"),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                alpaca_quantity: Quantity::new(dec!(10)),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at: Utc::now(),
            },
            RedemptionEvent::BurningFailed {
                issuer_request_id: IssuerRequestId::new("red-123"),
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

        // Insert a TokensBurned event directly into the event store
        let aggregate_id = "red-reproject-123";
        let receipt_id = uint!(42_U256);
        let shares_burned = uint!(100_000000000000000000_U256);

        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("iss-mint-123"),
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

        // Verify view is initially empty
        let initial_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) as count FROM receipt_burns_view"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            initial_count, 0,
            "View should be empty before re-projection"
        );

        // Run replay
        replay_receipt_burns_view(pool.clone())
            .await
            .expect("Replay should succeed");

        // Verify view is now populated
        let row = sqlx::query!(
            r#"
            SELECT
                view_id as "view_id!: String",
                json_extract(payload, '$.Burned.receipt_id') as "receipt_id: String",
                json_extract(payload, '$.Burned.shares_burned') as "shares_burned: String"
            FROM receipt_burns_view
            WHERE view_id = ?
            "#,
            aggregate_id
        )
        .fetch_optional(&pool)
        .await
        .unwrap();

        let row = row.expect("View should have been populated");
        assert_eq!(
            parse_u256_hex(&row.receipt_id.unwrap()).unwrap(),
            receipt_id
        );
        assert_eq!(
            parse_u256_hex(&row.shares_burned.unwrap()).unwrap(),
            shares_burned
        );
    }

    /// Tests that re-projection handles multiple TokensBurned events correctly.
    #[tokio::test]
    async fn test_reproject_burns_handles_multiple_events() {
        let pool = setup_test_db().await;

        // Insert multiple TokensBurned events for different redemptions
        for i in 1_u64..=3 {
            let aggregate_id = format!("red-multi-{i}");
            let event = RedemptionEvent::TokensBurned {
                issuer_request_id: IssuerRequestId::new(format!(
                    "iss-mint-{i}"
                )),
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

        // Verify all three burns were projected
        let count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) as count FROM receipt_burns_view"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(count, 3, "All three burns should be in the view");
    }

    /// Tests that re-projection is idempotent - running it twice doesn't duplicate entries.
    #[tokio::test]
    async fn test_reproject_burns_is_idempotent() {
        let pool = setup_test_db().await;

        let aggregate_id = "red-idempotent-123";
        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("iss-mint-123"),
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

        // Verify only one entry exists
        let count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) as count FROM receipt_burns_view WHERE view_id = ?",
            aggregate_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(count, 1, "Re-projection should be idempotent");
    }

    #[tokio::test]
    async fn test_find_receipt_errors_when_burns_exceed_initial_amount() {
        let pool = setup_test_db().await;
        let underlying = UnderlyingSymbol::new("AAPL");

        let initial_amount = uint!(50_000000000000000000_U256);
        let shares_burned = uint!(100_000000000000000000_U256); // More than initial

        let receipt_view = ReceiptInventoryView::Active {
            receipt_id: uint!(42_U256),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tAAPL"),
            initial_amount,
            current_balance: initial_amount,
            minted_at: Utc::now(),
        };
        let receipt_payload = serde_json::to_string(&receipt_view).unwrap();

        sqlx::query!(
            "INSERT INTO receipt_inventory_view (view_id, version, payload) \
             VALUES (?, 1, ?)",
            "iss-123",
            receipt_payload
        )
        .execute(&pool)
        .await
        .unwrap();

        let burn_view = ReceiptBurnsView::Burned {
            receipt_id: uint!(42_U256),
            redemption_issuer_request_id: IssuerRequestId::new("red-456"),
            shares_burned,
            burned_at: Utc::now(),
        };
        let burn_payload = serde_json::to_string(&burn_view).unwrap();

        sqlx::query!(
            "INSERT INTO receipt_burns_view (view_id, version, payload) \
             VALUES (?, 1, ?)",
            "red-456",
            burn_payload
        )
        .execute(&pool)
        .await
        .unwrap();

        let result =
            find_receipt_with_available_balance(&pool, &underlying, U256::ZERO)
                .await;

        let err =
            result.expect_err("Should return error when burns exceed initial");
        assert!(
            matches!(err, BurnTrackingError::BurnsExceedInitialAmount { .. }),
            "Expected BurnsExceedInitialAmount, got {err:?}"
        );
    }
}
