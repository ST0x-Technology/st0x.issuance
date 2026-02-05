use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

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
pub(crate) enum BurnTrackingError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("JSON deserialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Parse error: {0}")]
    Parse(String),
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

/// Finds a receipt with sufficient available balance for burning.
///
/// Joins receipt_inventory_view with receipt_burns_view to compute
/// available balance at query time. Returns the receipt with the highest
/// available balance that meets the minimum requirement.
pub(crate) async fn find_receipt_with_available_balance(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
    minimum_balance: U256,
) -> Result<Option<ReceiptWithBalance>, BurnTrackingError> {
    let underlying_str = &underlying.0;
    let min_balance_hex = format!("{minimum_balance:#x}");

    // Single SQL query with LEFT JOIN to compute available balance
    // receipt_inventory_view: keyed by mint's issuer_request_id, has receipt_id in payload
    // receipt_burns_view: keyed by redemption_id, has receipt_id in payload
    let row = sqlx::query!(
        r#"
        SELECT
            riv.view_id as "mint_issuer_request_id!: String",
            json_extract(riv.payload, '$.Active.receipt_id') as "receipt_id!: String",
            json_extract(riv.payload, '$.Active.initial_amount') as "initial_amount!: String",
            COALESCE(SUM(
                CASE
                    WHEN rbv.payload IS NOT NULL
                    THEN CAST(json_extract(rbv.payload, '$.Burned.shares_burned') AS INTEGER)
                    ELSE 0
                END
            ), 0) as "total_burned!: i64"
        FROM receipt_inventory_view riv
        LEFT JOIN receipt_burns_view rbv
            ON json_extract(riv.payload, '$.Active.receipt_id')
                = json_extract(rbv.payload, '$.Burned.receipt_id')
        WHERE json_extract(riv.payload, '$.Active.underlying') = ?
        GROUP BY riv.view_id
        HAVING (
            CAST(json_extract(riv.payload, '$.Active.initial_amount') AS INTEGER)
            - COALESCE(SUM(
                CASE
                    WHEN rbv.payload IS NOT NULL
                    THEN CAST(json_extract(rbv.payload, '$.Burned.shares_burned') AS INTEGER)
                    ELSE 0
                END
            ), 0)
        ) >= CAST(? AS INTEGER)
        ORDER BY (
            CAST(json_extract(riv.payload, '$.Active.initial_amount') AS INTEGER)
            - COALESCE(SUM(
                CASE
                    WHEN rbv.payload IS NOT NULL
                    THEN CAST(json_extract(rbv.payload, '$.Burned.shares_burned') AS INTEGER)
                    ELSE 0
                END
            ), 0)
        ) DESC
        LIMIT 1
        "#,
        underlying_str,
        min_balance_hex
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let receipt_id =
        U256::from_str_radix(row.receipt_id.trim_start_matches("0x"), 16)
            .map_err(|e| BurnTrackingError::Parse(e.to_string()))?;

    let initial_amount =
        U256::from_str_radix(row.initial_amount.trim_start_matches("0x"), 16)
            .map_err(|e| BurnTrackingError::Parse(e.to_string()))?;

    let total_burned = U256::from(row.total_burned as u64);
    let available_balance = initial_amount.saturating_sub(total_burned);

    Ok(Some(ReceiptWithBalance {
        receipt_id,
        underlying: underlying.clone(),
        initial_amount,
        total_burned,
        available_balance,
        mint_issuer_request_id: IssuerRequestId::new(
            &row.mint_issuer_request_id,
        ),
    }))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::uint;
    use std::collections::HashMap;

    use super::*;

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
