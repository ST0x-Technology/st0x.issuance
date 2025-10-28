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
    pub(crate) fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub(crate) fn is_pending(&self) -> bool {
        matches!(self, Self::Pending { .. })
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

    pub(crate) fn with_initiated_data(
        self,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    ) -> Self {
        match self {
            Self::Unavailable => Self::Pending { underlying, token },
            other => other,
        }
    }

    pub(crate) fn with_tokens_minted(
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
