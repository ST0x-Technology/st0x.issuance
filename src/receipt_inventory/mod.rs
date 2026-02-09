pub(crate) mod backfill;
pub(crate) mod burn_tracking;
mod cmd;
mod event;
pub(crate) mod monitor;
pub(crate) mod view;

pub(crate) use monitor::{ReceiptMonitor, ReceiptMonitorConfig};

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use cqrs_es::{Aggregate, AggregateContext, EventStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use burn_tracking::plan_burn;
pub(crate) use burn_tracking::{
    BurnPlan, BurnTrackingError, ReceiptBurnsView, ReceiptWithBalance,
};
pub(crate) use cmd::ReceiptInventoryCommand;
pub(crate) use event::{ReceiptInventoryEvent, ReceiptSource};
pub(crate) use view::ReceiptInventoryView;

use crate::mint::IssuerRequestId;

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

/// Provides the capability to find receipts for burning.
///
/// This trait abstracts the receipt inventory query for use by command handlers.
/// Implementation uses the ReceiptInventory aggregate to determine which
/// receipts can satisfy a burn request.
#[async_trait]
pub(crate) trait ReceiptService: Send + Sync {
    /// Returns a burn plan: which receipts to burn and how much from each.
    ///
    /// The plan satisfies the required burn amount by selecting from available
    /// receipts in descending order of balance.
    async fn for_burn(
        &self,
        vault: Address,
        shares_to_burn: Shares,
        dust: Shares,
    ) -> Result<BurnPlan, BurnTrackingError>;
}

/// Implementation of ReceiptService using the ReceiptInventory aggregate.
pub(crate) struct CqrsReceiptService<ES>
where
    ES: EventStore<ReceiptInventory>,
{
    store: Arc<ES>,
}

impl<ES> CqrsReceiptService<ES>
where
    ES: EventStore<ReceiptInventory>,
{
    pub(crate) const fn new(store: Arc<ES>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<ES> ReceiptService for CqrsReceiptService<ES>
where
    ES: EventStore<ReceiptInventory> + Send + Sync,
    ES::AC: Send,
{
    async fn for_burn(
        &self,
        vault: Address,
        shares_to_burn: Shares,
        dust: Shares,
    ) -> Result<BurnPlan, BurnTrackingError> {
        let context = self.store.load_aggregate(&vault.to_string()).await?;

        let receipts = context.aggregate().receipts_with_balance();
        plan_burn(receipts, shares_to_burn, dust)
    }
}

/// Tracks all receipts with available balance for a vault.
///
/// This aggregate maintains a map of receipt IDs to their available balances.
/// Receipts with zero balance are removed from state (events remain for audit).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ReceiptInventory {
    receipts: HashMap<ReceiptId, Shares>,
    /// Maps issuer_request_id to receipt_id for ITN mints.
    /// Used by mint recovery to check if a mint succeeded on-chain.
    itn_receipts: HashMap<IssuerRequestId, ReceiptId>,
    last_backfilled_block: Option<u64>,
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

    pub(crate) const fn last_backfilled_block(&self) -> Option<u64> {
        self.last_backfilled_block
    }

    /// Finds a receipt by its issuer_request_id (for ITN mints only).
    /// Returns None if no ITN receipt exists with this issuer_request_id.
    pub(crate) fn find_by_issuer_request_id(
        &self,
        _issuer_request_id: &IssuerRequestId,
    ) -> Option<ReceiptId> {
        todo!()
    }
}

/// Determines the source of a receipt by parsing the vault's receiptInformation.
///
/// If the receiptInformation contains a valid `issuer_request_id`, this returns
/// `ReceiptSource::Itn`; otherwise `ReceiptSource::External`.
pub(crate) fn determine_source(_receipt_id: ReceiptId) -> ReceiptSource {
    todo!("Task 6: parse receiptInformation to determine source")
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptInventoryError {
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
                source,
            } => {
                if self.receipts.contains_key(&receipt_id) {
                    return Ok(vec![]);
                }

                Ok(vec![ReceiptInventoryEvent::Discovered {
                    receipt_id,
                    balance,
                    block_number,
                    tx_hash,
                    source,
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

            ReceiptInventoryCommand::AdvanceBackfillCheckpoint {
                block_number,
            } => Ok(vec![ReceiptInventoryEvent::BackfillCheckpoint {
                block_number,
            }]),
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

            ReceiptInventoryEvent::BackfillCheckpoint { block_number } => {
                self.last_backfilled_block = Some(
                    self.last_backfilled_block
                        .map_or(block_number, |last| last.max(block_number)),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, address, b256};
    use cqrs_es::{CqrsFramework, mem_store::MemStore};
    use std::sync::Arc;

    use super::*;

    fn make_receipt_id(n: u64) -> ReceiptId {
        ReceiptId::from(U256::from(n))
    }

    fn make_shares(n: u64) -> Shares {
        Shares::new(U256::from(n))
    }

    fn discover_receipt_cmd(
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
    ) -> ReceiptInventoryCommand {
        ReceiptInventoryCommand::DiscoverReceipt {
            receipt_id,
            balance,
            block_number,
            tx_hash,
            source: ReceiptSource::External,
        }
    }

    #[tokio::test]
    async fn test_discover_receipt_emits_discovered_event() {
        let aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = aggregate
            .handle(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
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
    async fn test_discover_already_discovered_is_idempotent() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // First discovery succeeds
        let events = aggregate
            .handle(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        for event in events {
            aggregate.apply(event);
        }

        // Second discovery is idempotent - returns Ok with no events
        let events = aggregate
            .handle(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(200),
                    2000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        assert!(events.is_empty());
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
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
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
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
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
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(50),
                    1000,
                    tx_hash,
                ),
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

    #[tokio::test]
    async fn test_advance_backfill_checkpoint_emits_event() {
        let aggregate = ReceiptInventory::default();

        let events = aggregate
            .handle(
                ReceiptInventoryCommand::AdvanceBackfillCheckpoint {
                    block_number: 100,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BackfillCheckpoint { block_number: 100 }
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
            source: ReceiptSource::External,
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

    #[test]
    fn test_backfill_checkpoint_tracks_max_block_number() {
        let mut aggregate = ReceiptInventory::default();

        assert_eq!(aggregate.last_backfilled_block(), None);

        aggregate.apply(ReceiptInventoryEvent::BackfillCheckpoint {
            block_number: 500,
        });
        assert_eq!(aggregate.last_backfilled_block(), Some(500));

        aggregate.apply(ReceiptInventoryEvent::BackfillCheckpoint {
            block_number: 1000,
        });
        assert_eq!(aggregate.last_backfilled_block(), Some(1000));

        // Earlier block doesn't decrease the max
        aggregate.apply(ReceiptInventoryEvent::BackfillCheckpoint {
            block_number: 750,
        });
        assert_eq!(aggregate.last_backfilled_block(), Some(1000));
    }

    async fn setup_receipt_service_with_receipts(
        receipts: Vec<(u64, u64)>,
    ) -> CqrsReceiptService<MemStore<ReceiptInventory>> {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = CqrsFramework::new((*store).clone(), vec![], ());
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        for (i, (id, balance)) in receipts.into_iter().enumerate() {
            cqrs.execute(
                &address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .to_string(),
                discover_receipt_cmd(
                    make_receipt_id(id),
                    make_shares(balance),
                    1000 + i as u64,
                    tx_hash,
                ),
            )
            .await
            .unwrap();
        }

        CqrsReceiptService::new(store)
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_returns_plan_when_sufficient_balance()
     {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 100), (2, 200)]).await;

        let plan = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                make_shares(150),
                make_shares(0),
            )
            .await
            .unwrap();

        assert_eq!(plan.total_burn, make_shares(150));
        assert!(!plan.allocations.is_empty());
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_includes_dust_in_plan() {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 100), (2, 200)]).await;

        let plan = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                make_shares(100),
                make_shares(10),
            )
            .await
            .unwrap();

        assert_eq!(plan.total_burn, make_shares(100));
        assert_eq!(plan.dust, make_shares(10));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_returns_error_when_insufficient_balance()
     {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 50), (2, 30)]).await;

        let result = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                make_shares(100),
                make_shares(0),
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnTrackingError::InsufficientBalance { .. })
        ));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_with_empty_inventory_returns_error()
     {
        let service = setup_receipt_service_with_receipts(vec![]).await;

        let result = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                make_shares(100),
                make_shares(0),
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnTrackingError::InsufficientBalance { .. })
        ));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_with_unknown_vault_returns_error()
     {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;

        let result = service
            .for_burn(
                address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                make_shares(50),
                make_shares(0),
            )
            .await;

        // Unknown vault has no receipts, so insufficient balance
        assert!(matches!(
            result,
            Err(BurnTrackingError::InsufficientBalance { .. })
        ));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_exactly_available_succeeds() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;

        let plan = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                make_shares(100),
                make_shares(0),
            )
            .await
            .unwrap();

        assert_eq!(plan.total_burn, make_shares(100));
        assert_eq!(plan.allocations.len(), 1);
    }
}
