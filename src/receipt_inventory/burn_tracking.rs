use alloy::primitives::{U256, ruint::ParseError};
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::collections::HashMap;

use crate::mint::IssuerRequestId;
use crate::redemption::{Redemption, RedemptionEvent};
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
        /// The mint's issuer_request_id (for traceability to original mint)
        mint_issuer_request_id: IssuerRequestId,
        /// Number of shares burned
        shares_burned: U256,
        /// When the burn occurred
        burned_at: DateTime<Utc>,
    },
}

impl View<Redemption> for ReceiptBurnsView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        if let RedemptionEvent::TokensBurned {
            issuer_request_id,
            receipt_id,
            shares_burned,
            burned_at,
            ..
        } = &event.payload
        {
            *self = Self::Burned {
                receipt_id: *receipt_id,
                mint_issuer_request_id: issuer_request_id.clone(),
                shares_burned: *shares_burned,
                burned_at: *burned_at,
            };
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BurnTrackingError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("JSON deserialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
}

/// Represents a receipt with its available balance (initial - burned).
#[derive(Debug, Clone)]
pub struct ReceiptWithBalance {
    pub receipt_id: U256,
    pub underlying: UnderlyingSymbol,
    pub initial_amount: U256,
    pub total_burned: U256,
    pub available_balance: U256,
    /// The mint's issuer_request_id (for traceability)
    pub mint_issuer_request_id: IssuerRequestId,
}

fn parse_u256_hex(s: &str) -> Result<U256, ParseError> {
    U256::from_str_radix(s.trim_start_matches("0x"), 16)
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
pub async fn find_receipt_with_available_balance(
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
            let available_balance = initial_amount.saturating_sub(total_burned);

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
    use alloy::primitives::uint;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;
    use crate::receipt_inventory::ReceiptInventoryView;
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
            mint_issuer_request_id: IssuerRequestId::new("iss-123"),
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
            issuer_request_id: IssuerRequestId::new("mint-123"),
            tx_hash: alloy::primitives::b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: uint!(42_U256),
            shares_burned: uint!(100_000000000000000000_U256),
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
            mint_issuer_request_id,
            shares_burned,
            ..
        } = view
        else {
            panic!("Expected Burned variant");
        };

        assert_eq!(receipt_id, uint!(42_U256));
        assert_eq!(mint_issuer_request_id, IssuerRequestId::new("mint-123"));
        assert_eq!(shares_burned, uint!(100_000000000000000000_U256));
    }

    #[test]
    fn test_view_ignores_other_events() {
        let mut view = ReceiptBurnsView::default();

        let events = vec![
            RedemptionEvent::Detected {
                issuer_request_id: IssuerRequestId::new("red-123"),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: crate::tokenized_asset::TokenSymbol::new("tAAPL"),
                wallet: alloy::primitives::address!(
                    "0x1111111111111111111111111111111111111111"
                ),
                quantity: crate::mint::Quantity::new(
                    rust_decimal::Decimal::from(10),
                ),
                tx_hash: alloy::primitives::b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                block_number: 500,
                detected_at: Utc::now(),
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: IssuerRequestId::new("red-123"),
                tokenization_request_id:
                    crate::mint::TokenizationRequestId::new("tok-123"),
                alpaca_quantity: crate::mint::Quantity::new(
                    rust_decimal::Decimal::from(10),
                ),
                dust_quantity: crate::mint::Quantity::new(
                    rust_decimal::Decimal::ZERO,
                ),
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
}
