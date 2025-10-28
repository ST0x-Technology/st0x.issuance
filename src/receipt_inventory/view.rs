use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};

use crate::mint::{Mint, MintEvent};
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
    pub(crate) fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub(crate) fn is_depleted(&self) -> bool {
        matches!(self, Self::Depleted { .. })
    }

    pub(crate) fn has_sufficient_balance(&self, amount: U256) -> bool {
        match self {
            Self::Active { current_balance, .. } => *current_balance >= amount,
            _ => false,
        }
    }

    pub(crate) fn new_active(
        receipt_id: U256,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        initial_amount: U256,
        minted_at: DateTime<Utc>,
    ) -> Self {
        Self::Active {
            receipt_id,
            underlying,
            token,
            initial_amount,
            current_balance: initial_amount,
            minted_at,
        }
    }

    pub(crate) fn mark_depleted(self, depleted_at: DateTime<Utc>) -> Self {
        match self {
            Self::Active {
                receipt_id,
                underlying,
                token,
                initial_amount,
                ..
            } => Self::Depleted {
                receipt_id,
                underlying,
                token,
                initial_amount,
                depleted_at,
            },
            other => other,
        }
    }
}

impl View<Mint> for ReceiptInventoryView {
    fn update(&mut self, event: &EventEnvelope<Mint>) {
        match &event.payload {
            MintEvent::TokensMinted { .. } => {
                // TODO: Implement in Task 4
            }
            _ => {}
        }
    }
}

impl View<Redemption> for ReceiptInventoryView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        match &event.payload {
            _ => {
                // TODO: This will be implemented when issue #25 adds burn events
                // with receipt details. The implementation will:
                // 1. Extract receipt_id and shares_burned from the burn event
                // 2. Decrement current_balance by shares_burned
                // 3. If balance reaches zero, transition to Depleted state
            }
        }
    }
}
