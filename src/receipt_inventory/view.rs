use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use crate::mint::{IssuerRequestId, Mint, MintEvent};
use crate::redemption::Redemption;
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptInventoryViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptInventoryView {
    Unavailable,
    Pending {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    },
    Active {
        receipt_id: U256,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        initial_amount: U256,
        current_balance: U256,
        minted_at: DateTime<Utc>,
    },
    Depleted {
        receipt_id: U256,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        initial_amount: U256,
        depleted_at: DateTime<Utc>,
    },
}

impl Default for ReceiptInventoryView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl ReceiptInventoryView {
    fn with_initiated_data(
        self,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    ) -> Self {
        match self {
            Self::Unavailable => Self::Pending { underlying, token },
            other => other,
        }
    }

    fn with_tokens_minted(
        self,
        receipt_id: U256,
        shares_minted: U256,
        minted_at: DateTime<Utc>,
    ) -> Self {
        match self {
            Self::Pending { underlying, token } => Self::Active {
                receipt_id,
                underlying,
                token,
                initial_amount: shares_minted,
                current_balance: shares_minted,
                minted_at,
            },
            other => other,
        }
    }
}

impl View<Mint> for ReceiptInventoryView {
    fn update(&mut self, event: &EventEnvelope<Mint>) {
        match &event.payload {
            MintEvent::Initiated { underlying, token, .. } => {
                *self = self
                    .clone()
                    .with_initiated_data(underlying.clone(), token.clone());
            }
            MintEvent::TokensMinted {
                receipt_id,
                shares_minted,
                minted_at,
                ..
            } => {
                *self = self.clone().with_tokens_minted(
                    *receipt_id,
                    *shares_minted,
                    *minted_at,
                );
            }
            MintEvent::JournalConfirmed { .. }
            | MintEvent::JournalRejected { .. }
            | MintEvent::MintingFailed { .. }
            | MintEvent::MintCompleted { .. } => {}
        }
    }
}

impl View<Redemption> for ReceiptInventoryView {
    fn update(&mut self, _event: &EventEnvelope<Redemption>) {
        // TODO: This will be implemented when issue #25 adds burn events
        // with receipt details. The implementation will:
        // 1. Extract receipt_id and shares_burned from the burn event
        // 2. Decrement current_balance by shares_burned
        // 3. If balance reaches zero, transition to Depleted state
    }
}

pub(crate) async fn get_receipt(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerRequestId,
) -> Result<Option<ReceiptInventoryView>, ReceiptInventoryViewError> {
    let issuer_request_id_str = &issuer_request_id.0;
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM receipt_inventory_view
        WHERE view_id = ?
        "#,
        issuer_request_id_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: ReceiptInventoryView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

pub(crate) async fn list_active_receipts(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<Vec<ReceiptInventoryView>, ReceiptInventoryViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM receipt_inventory_view
        WHERE json_extract(payload, '$.Active.underlying') = ?
        "#,
        &underlying.0
    )
    .fetch_all(pool)
    .await?;

    let views: Result<Vec<ReceiptInventoryView>, serde_json::Error> = rows
        .into_iter()
        .map(|row| serde_json::from_str(&row.payload))
        .collect();

    Ok(views?)
}

pub(crate) async fn find_receipt_with_balance(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
    minimum_balance: U256,
) -> Result<Option<ReceiptInventoryView>, ReceiptInventoryViewError> {
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM receipt_inventory_view
        WHERE json_extract(payload, '$.Active.underlying') = ?
          AND CAST(json_extract(payload, '$.Active.current_balance') AS TEXT) >= ?
        ORDER BY CAST(json_extract(payload, '$.Active.current_balance') AS TEXT) DESC
        LIMIT 1
        "#,
        &underlying.0,
        minimum_balance.to_string()
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: ReceiptInventoryView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

pub(crate) async fn get_total_balance(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<U256, ReceiptInventoryViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM receipt_inventory_view
        WHERE json_extract(payload, '$.Active.underlying') = ?
        "#,
        &underlying.0
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter().try_fold(U256::ZERO, |acc, row| {
        let view: ReceiptInventoryView = serde_json::from_str(&row.payload)?;
        Ok(if let ReceiptInventoryView::Active { current_balance, .. } = view {
            acc + current_balance
        } else {
            acc
        })
    })
}
