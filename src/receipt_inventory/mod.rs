pub(crate) mod burn_tracking;
mod cmd;
mod event;
pub(crate) mod view;

use alloy::primitives::U256;
use async_trait::async_trait;
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) use burn_tracking::{
    BurnPlan, BurnTrackingError, ReceiptBurnsView, ReceiptWithBalance,
    plan_burn,
};
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

    pub(crate) fn is_zero(&self) -> bool {
        self.0.is_zero()
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

/// Tracks all receipts with available balance for a vault.
///
/// This aggregate maintains a map of receipt IDs to their available balances.
/// Receipts with zero balance are removed from state (events remain for audit).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ReceiptInventory {
    receipts: HashMap<ReceiptId, Shares>,
}

impl ReceiptInventory {
    pub(crate) fn receipts_with_balance(&self) -> Vec<ReceiptWithBalance> {
        self.receipts
            .iter()
            .map(|(receipt_id, balance)| ReceiptWithBalance {
                receipt_id: *receipt_id,
                available_balance: *balance,
            })
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptInventoryError {
    #[error("Receipt {receipt_id} already discovered")]
    ReceiptAlreadyDiscovered { receipt_id: ReceiptId },

    #[error("Receipt {receipt_id} not found")]
    ReceiptNotFound { receipt_id: ReceiptId },

    #[error(
        "Insufficient balance for receipt {receipt_id}:\
         available={available}, requested={requested}"
    )]
    InsufficientBalance {
        receipt_id: ReceiptId,
        available: Shares,
        requested: Shares,
    },
}

#[async_trait]
impl Aggregate for ReceiptInventory {
    type Command = ReceiptInventoryCommand;
    type Event = ReceiptInventoryEvent;
    type Error = ReceiptInventoryError;
    type Services = ();

    fn aggregate_type() -> String {
        "ReceiptInventory".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id,
                balance,
                block_number,
                tx_hash,
            } => {
                if self.receipts.contains_key(&receipt_id) {
                    return Err(
                        ReceiptInventoryError::ReceiptAlreadyDiscovered {
                            receipt_id,
                        },
                    );
                }

                Ok(vec![ReceiptInventoryEvent::Discovered {
                    receipt_id,
                    balance,
                    block_number,
                    tx_hash,
                }])
            }

            ReceiptInventoryCommand::BurnShares { receipt_id, amount } => {
                let current_balance = self.receipts.get(&receipt_id).copied();

                let Some(available) = current_balance else {
                    return Err(ReceiptInventoryError::ReceiptNotFound {
                        receipt_id,
                    });
                };

                if available.inner() < amount.inner() {
                    return Err(ReceiptInventoryError::InsufficientBalance {
                        receipt_id,
                        available,
                        requested: amount,
                    });
                }

                let new_balance =
                    Shares::new(available.inner() - amount.inner());

                if new_balance.is_zero() {
                    Ok(vec![
                        ReceiptInventoryEvent::Burned {
                            receipt_id,
                            amount_burned: amount,
                            new_balance,
                        },
                        ReceiptInventoryEvent::Depleted { receipt_id },
                    ])
                } else {
                    Ok(vec![ReceiptInventoryEvent::Burned {
                        receipt_id,
                        amount_burned: amount,
                        new_balance,
                    }])
                }
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            ReceiptInventoryEvent::Discovered {
                receipt_id, balance, ..
            } => {
                self.receipts.insert(receipt_id, balance);
            }

            ReceiptInventoryEvent::Burned {
                receipt_id, new_balance, ..
            } => {
                if new_balance.is_zero() {
                    self.receipts.remove(&receipt_id);
                } else {
                    self.receipts.insert(receipt_id, new_balance);
                }
            }

            ReceiptInventoryEvent::Depleted { receipt_id } => {
                self.receipts.remove(&receipt_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;

    use super::*;

    fn make_receipt_id(n: u64) -> ReceiptId {
        ReceiptId::from(U256::from(n))
    }

    fn make_shares(n: u64) -> Shares {
        Shares::new(U256::from(n))
    }

    #[tokio::test]
    async fn test_discover_receipt_emits_discovered_event() {
        let aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = aggregate
            .handle(
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: make_receipt_id(42),
                    balance: make_shares(100),
                    block_number: 1000,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::Discovered {
                receipt_id,
                balance,
                block_number: 1000,
                ..
            } if *receipt_id == make_receipt_id(42) && *balance == make_shares(100)
        ));
    }

    #[tokio::test]
    async fn test_discover_already_discovered_receipt_returns_error() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // First discovery succeeds
        let events = aggregate
            .handle(
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: make_receipt_id(42),
                    balance: make_shares(100),
                    block_number: 1000,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply(event);
        }

        // Second discovery fails
        let result = aggregate
            .handle(
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: make_receipt_id(42),
                    balance: make_shares(200),
                    block_number: 2000,
                    tx_hash,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(ReceiptInventoryError::ReceiptAlreadyDiscovered { receipt_id })
            if receipt_id == make_receipt_id(42)
        ));
    }

    #[tokio::test]
    async fn test_burn_shares_emits_burned_event() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // Discover receipt first
        let events = aggregate
            .handle(
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: make_receipt_id(42),
                    balance: make_shares(100),
                    block_number: 1000,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply(event);
        }

        // Partial burn
        let events = aggregate
            .handle(
                ReceiptInventoryCommand::BurnShares {
                    receipt_id: make_receipt_id(42),
                    amount: make_shares(30),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::Burned {
                receipt_id,
                amount_burned,
                new_balance,
            } if *receipt_id == make_receipt_id(42)
                && *amount_burned == make_shares(30)
                && *new_balance == make_shares(70)
        ));
    }

    #[tokio::test]
    async fn test_burn_full_balance_emits_burned_and_depleted() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // Discover receipt
        let events = aggregate
            .handle(
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: make_receipt_id(42),
                    balance: make_shares(100),
                    block_number: 1000,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply(event);
        }

        // Full burn
        let events = aggregate
            .handle(
                ReceiptInventoryCommand::BurnShares {
                    receipt_id: make_receipt_id(42),
                    amount: make_shares(100),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::Burned { new_balance, .. }
            if new_balance.is_zero()
        ));
        assert!(matches!(
            &events[1],
            ReceiptInventoryEvent::Depleted { receipt_id }
            if *receipt_id == make_receipt_id(42)
        ));
    }

    #[tokio::test]
    async fn test_burn_nonexistent_receipt_returns_error() {
        let aggregate = ReceiptInventory::default();

        let result = aggregate
            .handle(
                ReceiptInventoryCommand::BurnShares {
                    receipt_id: make_receipt_id(99),
                    amount: make_shares(50),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(ReceiptInventoryError::ReceiptNotFound { receipt_id })
            if receipt_id == make_receipt_id(99)
        ));
    }

    #[tokio::test]
    async fn test_burn_insufficient_balance_returns_error() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // Discover receipt with 50 balance
        let events = aggregate
            .handle(
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: make_receipt_id(42),
                    balance: make_shares(50),
                    block_number: 1000,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply(event);
        }

        // Try to burn more than available
        let result = aggregate
            .handle(
                ReceiptInventoryCommand::BurnShares {
                    receipt_id: make_receipt_id(42),
                    amount: make_shares(100),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(ReceiptInventoryError::InsufficientBalance {
                receipt_id,
                available,
                requested,
            }) if receipt_id == make_receipt_id(42)
                && available == make_shares(50)
                && requested == make_shares(100)
        ));
    }

    #[test]
    fn test_apply_discovered_adds_to_state() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        aggregate.apply(ReceiptInventoryEvent::Discovered {
            receipt_id: make_receipt_id(42),
            balance: make_shares(100),
            block_number: 1000,
            tx_hash,
        });

        assert_eq!(aggregate.receipts.len(), 1);
        assert_eq!(
            aggregate.receipts.get(&make_receipt_id(42)),
            Some(&make_shares(100))
        );
    }

    #[test]
    fn test_apply_burned_updates_balance() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.receipts.insert(make_receipt_id(42), make_shares(100));

        aggregate.apply(ReceiptInventoryEvent::Burned {
            receipt_id: make_receipt_id(42),
            amount_burned: make_shares(30),
            new_balance: make_shares(70),
        });

        assert_eq!(
            aggregate.receipts.get(&make_receipt_id(42)),
            Some(&make_shares(70))
        );
    }

    #[test]
    fn test_apply_burned_with_zero_balance_removes_receipt() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.receipts.insert(make_receipt_id(42), make_shares(100));

        aggregate.apply(ReceiptInventoryEvent::Burned {
            receipt_id: make_receipt_id(42),
            amount_burned: make_shares(100),
            new_balance: make_shares(0),
        });

        assert!(!aggregate.receipts.contains_key(&make_receipt_id(42)));
    }

    #[test]
    fn test_apply_depleted_removes_receipt() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.receipts.insert(make_receipt_id(42), make_shares(0));

        aggregate.apply(ReceiptInventoryEvent::Depleted {
            receipt_id: make_receipt_id(42),
        });

        assert!(!aggregate.receipts.contains_key(&make_receipt_id(42)));
    }

    #[test]
    fn test_receipts_with_balance_returns_all_receipts() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.receipts.insert(make_receipt_id(1), make_shares(100));
        aggregate.receipts.insert(make_receipt_id(2), make_shares(200));

        let receipts = aggregate.receipts_with_balance();

        assert_eq!(receipts.len(), 2);

        let total: Shares =
            receipts.iter().map(|receipt| receipt.available_balance).sum();
        assert_eq!(total, make_shares(300));
    }

    #[test]
    fn test_receipts_with_balance_empty_aggregate() {
        let aggregate = ReceiptInventory::default();

        let receipts = aggregate.receipts_with_balance();

        assert!(receipts.is_empty());
    }
}
