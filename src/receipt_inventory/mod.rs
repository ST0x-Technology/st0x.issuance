pub mod burn_tracking;
mod cmd;
mod event;
pub(crate) mod view;

use alloy::primitives::U256;
use serde::{Deserialize, Serialize};

pub(crate) use burn_tracking::ReceiptBurnsView;
pub(crate) use cmd::ReceiptInventoryCommand;
pub(crate) use event::ReceiptInventoryEvent;
pub(crate) use view::ReceiptInventoryView;

/// Unique identifier for an ERC-1155 receipt within a vault.
///
/// Each mint operation creates a receipt with a unique ID that tracks
/// the deposited assets. Receipts are burned during redemption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct ReceiptId(U256);

impl ReceiptId {
    pub(crate) const fn new(id: U256) -> Self {
        Self(id)
    }

    pub(crate) const fn inner(&self) -> U256 {
        self.0
    }
}

impl From<U256> for ReceiptId {
    fn from(id: U256) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for ReceiptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Amount of vault shares (ERC-20 tokens with 18 decimals).
///
/// Shares represent ownership in the vault and are minted/burned
/// proportionally to deposited/withdrawn assets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Shares(U256);

impl Shares {
    pub(crate) const fn new(amount: U256) -> Self {
        Self(amount)
    }

    pub(crate) const fn inner(&self) -> U256 {
        self.0
    }

    pub(crate) const fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    pub(crate) fn checked_sub(&self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }

    pub(crate) fn saturating_sub(&self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }
}

impl From<U256> for Shares {
    fn from(amount: U256) -> Self {
        Self(amount)
    }
}

impl std::fmt::Display for Shares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Add for Shares {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl std::iter::Sum for Shares {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self(U256::ZERO), |acc, x| acc + x)
    }
}
