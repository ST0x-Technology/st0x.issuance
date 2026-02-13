pub(crate) mod backfill;
pub(crate) mod backfill_job;
pub(crate) mod burn_tracking;
mod cmd;
mod event;
pub(crate) mod monitor;
pub(crate) mod reconcile;
pub(crate) mod view;

use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use async_trait::async_trait;
use cqrs_es::{
    Aggregate, AggregateContext, AggregateError, CqrsFramework, EventStore,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::mint::IssuerMintRequestId;
use crate::vault::ReceiptInformation;
use burn_tracking::plan_burn;
pub(crate) use burn_tracking::{
    BurnPlan, BurnTrackingError, ReceiptBurnsView, ReceiptWithBalance,
};
pub(crate) use cmd::ReceiptInventoryCommand;
pub(crate) use event::{ReceiptInventoryEvent, ReceiptSource};
pub(crate) use monitor::{
    ItnReceiptHandler, ReceiptMonitor, ReceiptMonitorConfig,
};
pub(crate) use view::ReceiptInventoryView;

/// Receipt data recovered from the inventory for mint recovery.
#[derive(Debug, Clone)]
pub(crate) struct RecoveredReceipt {
    pub(crate) receipt_id: U256,
    pub(crate) tx_hash: B256,
    pub(crate) shares: U256,
    pub(crate) block_number: u64,
}

/// Errors that can occur when looking up a receipt.
#[derive(Debug, Error)]
pub(crate) enum ReceiptLookupError {
    #[error(transparent)]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
    #[error(
        "Receipt inventory inconsistent: index contains issuer_request_id {issuer_request_id} \
         -> receipt {receipt_id}, but receipt not found in receipts list"
    )]
    Inconsistent {
        issuer_request_id: IssuerMintRequestId,
        receipt_id: ReceiptId,
    },
}

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

    pub(crate) fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }

    pub(crate) fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
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

/// Overflow error for Shares arithmetic.
#[derive(Debug, thiserror::Error)]
#[error("Shares overflow: {lhs} + {rhs} exceeds U256::MAX")]
pub(crate) struct SharesOverflow {
    pub(crate) lhs: Shares,
    pub(crate) rhs: Shares,
}

impl std::ops::Add for Shares {
    type Output = Result<Self, SharesOverflow>;

    fn add(self, other: Self) -> Result<Self, SharesOverflow> {
        self.0
            .checked_add(other.0)
            .map(Self)
            .ok_or(SharesOverflow { lhs: self, rhs: other })
    }
}

/// Metadata about a receipt stored in the inventory.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReceiptMetadata {
    balance: Shares,
    tx_hash: TxHash,
    block_number: u64,
    #[serde(default)]
    receipt_info: Option<ReceiptInformation>,
}

/// Provides receipt inventory capabilities for command handlers.
///
/// Abstracts receipt discovery, querying, and burn planning.
/// Implementation uses the ReceiptInventory aggregate.
#[async_trait]
pub(crate) trait ReceiptService: Send + Sync {
    /// Registers a newly minted receipt in the inventory.
    ///
    /// Called after a successful on-chain deposit to make the receipt
    /// immediately available for burn planning. The monitor/backfill
    /// will also discover it eventually, but explicit registration
    /// avoids the timing gap.
    async fn register_minted_receipt(
        &self,
        vault: Address,
        receipt_id: ReceiptId,
        shares: Shares,
        block_number: u64,
        tx_hash: B256,
        receipt_info: ReceiptInformation,
    ) -> Result<(), ReceiptRegistrationError>;

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

    /// Finds a receipt by its issuer_request_id (for ITN mints only).
    ///
    /// Used by mint recovery to check if a mint succeeded on-chain.
    /// Returns None if no receipt exists with this issuer_request_id.
    async fn find_by_issuer_request_id(
        &self,
        vault: &Address,
        issuer_request_id: &IssuerMintRequestId,
    ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError>;
}

#[derive(Debug, Error)]
pub(crate) enum ReceiptRegistrationError {
    #[error(transparent)]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
}

/// Implementation of ReceiptService using the ReceiptInventory aggregate.
pub(crate) struct CqrsReceiptService<ES>
where
    ES: EventStore<ReceiptInventory>,
{
    store: Arc<ES>,
    cqrs: Arc<CqrsFramework<ReceiptInventory, ES>>,
}

impl<ES> CqrsReceiptService<ES>
where
    ES: EventStore<ReceiptInventory>,
{
    pub(crate) const fn new(
        store: Arc<ES>,
        cqrs: Arc<CqrsFramework<ReceiptInventory, ES>>,
    ) -> Self {
        Self { store, cqrs }
    }
}

#[async_trait]
impl<ES> ReceiptService for CqrsReceiptService<ES>
where
    ES: EventStore<ReceiptInventory> + Send + Sync,
    ES::AC: Send,
{
    async fn register_minted_receipt(
        &self,
        vault: Address,
        receipt_id: ReceiptId,
        shares: Shares,
        block_number: u64,
        tx_hash: B256,
        receipt_info: ReceiptInformation,
    ) -> Result<(), ReceiptRegistrationError> {
        let issuer_request_id = receipt_info.issuer_request_id.clone();

        self.cqrs
            .execute(
                &vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id,
                    balance: shares,
                    block_number,
                    tx_hash,
                    source: ReceiptSource::Itn { issuer_request_id },
                    receipt_info: Some(receipt_info),
                },
            )
            .await?;

        Ok(())
    }

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

    async fn find_by_issuer_request_id(
        &self,
        vault: &Address,
        issuer_request_id: &IssuerMintRequestId,
    ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError> {
        let receipt_inventory = self
            .store
            .load_aggregate(&vault.to_string())
            .await?
            .aggregate()
            .clone();

        let Some(receipt_id) =
            receipt_inventory.find_by_issuer_request_id(issuer_request_id)
        else {
            return Ok(None);
        };

        let receipt = receipt_inventory
            .receipts_with_balance()
            .into_iter()
            .find(|r| r.receipt_id == receipt_id)
            .ok_or(ReceiptLookupError::Inconsistent {
                issuer_request_id: issuer_request_id.clone(),
                receipt_id,
            })?;

        Ok(Some(RecoveredReceipt {
            receipt_id: receipt_id.inner(),
            tx_hash: receipt.tx_hash,
            shares: receipt.available_balance.inner(),
            block_number: receipt.block_number,
        }))
    }
}

/// Tracks all receipts with available balance for a vault.
///
/// This aggregate maintains a map of receipt IDs to their available balances.
/// Receipts with zero balance are removed from state (events remain for audit).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ReceiptInventory {
    receipts: HashMap<ReceiptId, ReceiptMetadata>,
    /// Maps issuer_request_id to receipt_id for ITN mints.
    /// Used by mint recovery to check if a mint succeeded on-chain.
    itn_receipts: HashMap<IssuerMintRequestId, ReceiptId>,
    last_backfilled_block: Option<u64>,
}

impl ReceiptInventory {
    pub(crate) fn receipts_with_balance(&self) -> Vec<ReceiptWithBalance> {
        self.receipts
            .iter()
            .map(|(receipt_id, metadata)| ReceiptWithBalance {
                receipt_id: *receipt_id,
                available_balance: metadata.balance,
                tx_hash: metadata.tx_hash,
                block_number: metadata.block_number,
                receipt_info: metadata.receipt_info.clone(),
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
        issuer_request_id: &IssuerMintRequestId,
    ) -> Option<ReceiptId> {
        self.itn_receipts.get(issuer_request_id).copied()
    }
}

/// Determines the source of a receipt by parsing the vault's receiptInformation.
///
/// If the receiptInformation contains a valid `issuer_request_id`, this returns
/// `ReceiptSource::Itn` with the parsed `ReceiptInformation`;
/// otherwise `ReceiptSource::External` with `None`.
pub(crate) fn determine_source(
    receipt_information: &Bytes,
) -> (ReceiptSource, Option<ReceiptInformation>) {
    if receipt_information.is_empty() {
        return (ReceiptSource::External, None);
    }

    serde_json::from_slice::<ReceiptInformation>(receipt_information)
        .ok()
        .map_or((ReceiptSource::External, None), |info| {
            let source = ReceiptSource::Itn {
                issuer_request_id: info.issuer_request_id.clone(),
            };
            (source, Some(info))
        })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptInventoryError {
    #[error(
        "Unexpected balance increase for receipt {receipt_id}: \
         aggregate_balance={aggregate_balance}, on_chain_balance={on_chain_balance}"
    )]
    UnexpectedBalanceIncrease {
        receipt_id: ReceiptId,
        aggregate_balance: Shares,
        on_chain_balance: Shares,
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
                receipt_info,
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
                    receipt_info,
                }])
            }

            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id,
                on_chain_balance,
            } => {
                let Some(metadata) = self.receipts.get(&receipt_id) else {
                    return Ok(vec![]);
                };

                let aggregate_balance = metadata.balance;
                if on_chain_balance == aggregate_balance {
                    return Ok(vec![]);
                }

                if aggregate_balance.checked_sub(on_chain_balance).is_none() {
                    return Err(
                        ReceiptInventoryError::UnexpectedBalanceIncrease {
                            receipt_id,
                            aggregate_balance,
                            on_chain_balance,
                        },
                    );
                }

                let reconciled = ReceiptInventoryEvent::BalanceReconciled {
                    receipt_id,
                    previous_balance: aggregate_balance,
                    on_chain_balance,
                };

                let depleted = on_chain_balance
                    .is_zero()
                    .then_some(ReceiptInventoryEvent::Depleted { receipt_id });

                Ok(std::iter::once(reconciled).chain(depleted).collect())
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
                receipt_id,
                balance,
                block_number,
                tx_hash,
                source,
                receipt_info,
            } => {
                self.receipts.insert(
                    receipt_id,
                    ReceiptMetadata {
                        balance,
                        tx_hash,
                        block_number,
                        receipt_info,
                    },
                );
                if let ReceiptSource::Itn { issuer_request_id } = source {
                    self.itn_receipts.insert(issuer_request_id, receipt_id);
                }
            }

            ReceiptInventoryEvent::BalanceReconciled {
                receipt_id,
                on_chain_balance,
                ..
            } => {
                if on_chain_balance.is_zero() {
                    self.receipts.remove(&receipt_id);
                    self.itn_receipts.retain(|_, indexed_receipt_id| {
                        *indexed_receipt_id != receipt_id
                    });
                } else if let Some(metadata) =
                    self.receipts.get_mut(&receipt_id)
                {
                    metadata.balance = on_chain_balance;
                }
            }

            ReceiptInventoryEvent::Depleted { receipt_id } => {
                self.receipts.remove(&receipt_id);
                self.itn_receipts.retain(|_, indexed_receipt_id| {
                    *indexed_receipt_id != receipt_id
                });
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
    use alloy::primitives::{Bytes, TxHash, address, b256};
    use cqrs_es::{CqrsFramework, EventStore, mem_store::MemStore};
    use std::sync::Arc;

    use super::*;

    fn make_receipt_id(n: u64) -> ReceiptId {
        ReceiptId::from(U256::from(n))
    }

    fn make_shares(n: u64) -> Shares {
        Shares::new(U256::from(n))
    }

    fn make_metadata(balance: u64) -> ReceiptMetadata {
        ReceiptMetadata {
            balance: make_shares(balance),
            tx_hash: TxHash::ZERO,
            block_number: 0,
            receipt_info: None,
        }
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
            receipt_info: None,
        }
    }

    fn discover_itn_receipt_cmd(
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        issuer_request_id: IssuerMintRequestId,
    ) -> ReceiptInventoryCommand {
        ReceiptInventoryCommand::DiscoverReceipt {
            receipt_id,
            balance,
            block_number,
            tx_hash,
            source: ReceiptSource::Itn { issuer_request_id },
            receipt_info: None,
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
            receipt_info: None,
        });

        assert_eq!(aggregate.receipts.len(), 1);
        assert_eq!(
            aggregate.receipts.get(&make_receipt_id(42)).map(|m| m.balance),
            Some(make_shares(100))
        );
    }

    #[test]
    fn test_apply_discovered_stores_receipt_info() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-test"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity(Decimal::new(100, 0)),
            Utc::now(),
            None,
        );

        aggregate.apply(ReceiptInventoryEvent::Discovered {
            receipt_id: make_receipt_id(42),
            balance: make_shares(100),
            block_number: 1000,
            tx_hash,
            source: ReceiptSource::Itn { issuer_request_id },
            receipt_info: Some(receipt_info.clone()),
        });

        let receipts = aggregate.receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].receipt_info, Some(receipt_info));
    }

    #[tokio::test]
    async fn test_discover_receipt_with_receipt_info_roundtrips() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = CqrsFramework::new((*store).clone(), vec![], ());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-roundtrip"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("TSLA"),
            Quantity(Decimal::new(5050, 2)),
            Utc::now(),
            Some("test notes".to_string()),
        );

        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: make_receipt_id(99),
                balance: make_shares(500),
                block_number: 2000,
                tx_hash,
                source: ReceiptSource::Itn { issuer_request_id },
                receipt_info: Some(receipt_info.clone()),
            },
        )
        .await
        .unwrap();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].receipt_info,
            Some(receipt_info),
            "receipt_info should be preserved through command -> event -> state"
        );
    }

    #[test]
    fn test_apply_depleted_removes_receipt() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.receipts.insert(make_receipt_id(42), make_metadata(0));

        aggregate.apply(ReceiptInventoryEvent::Depleted {
            receipt_id: make_receipt_id(42),
        });

        assert!(!aggregate.receipts.contains_key(&make_receipt_id(42)));
    }

    #[test]
    fn test_receipts_with_balance_returns_all_receipts() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.receipts.insert(make_receipt_id(1), make_metadata(100));
        aggregate.receipts.insert(make_receipt_id(2), make_metadata(200));

        let receipts = aggregate.receipts_with_balance();

        assert_eq!(receipts.len(), 2);

        let total: Shares = receipts
            .iter()
            .map(|receipt| receipt.available_balance)
            .try_fold(Shares::new(U256::ZERO), |acc, shares| acc + shares)
            .unwrap();
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

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_receipt_when_itn_receipt_exists()
     {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = CqrsFramework::new((*store).clone(), vec![], ());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();

        cqrs.execute(
            &vault.to_string(),
            discover_itn_receipt_cmd(
                make_receipt_id(42),
                make_shares(100),
                1000,
                tx_hash,
                issuer_request_id.clone(),
            ),
        )
        .await
        .unwrap();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let found =
            context.aggregate().find_by_issuer_request_id(&issuer_request_id);
        assert_eq!(found, Some(make_receipt_id(42)));
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_none_when_not_exists() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let issuer_request_id = IssuerMintRequestId::random();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let found =
            context.aggregate().find_by_issuer_request_id(&issuer_request_id);
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_none_for_external_receipts()
    {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = CqrsFramework::new((*store).clone(), vec![], ());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // Discover an external receipt (no issuer_request_id)
        cqrs.execute(
            &vault.to_string(),
            discover_receipt_cmd(
                make_receipt_id(42),
                make_shares(100),
                1000,
                tx_hash,
            ),
        )
        .await
        .unwrap();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let aggregate = context.aggregate();

        // External receipts should not be indexed by issuer_request_id
        let random_id = IssuerMintRequestId::random();
        assert_eq!(aggregate.find_by_issuer_request_id(&random_id), None);

        // But the receipt itself should exist
        assert_eq!(aggregate.receipts.len(), 1);
    }

    #[tokio::test]
    async fn test_itn_receipt_is_indexed_in_itn_receipts_map() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = CqrsFramework::new((*store).clone(), vec![], ());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();

        cqrs.execute(
            &vault.to_string(),
            discover_itn_receipt_cmd(
                make_receipt_id(99),
                make_shares(500),
                2000,
                tx_hash,
                issuer_request_id.clone(),
            ),
        )
        .await
        .unwrap();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let aggregate = context.aggregate();

        // Verify the receipt is in both maps
        assert_eq!(aggregate.receipts.len(), 1);
        assert_eq!(aggregate.itn_receipts.len(), 1);
        assert_eq!(
            aggregate.itn_receipts.get(&issuer_request_id),
            Some(&make_receipt_id(99))
        );
    }

    #[tokio::test]
    async fn test_external_receipt_not_in_itn_receipts_map() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = CqrsFramework::new((*store).clone(), vec![], ());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        cqrs.execute(
            &vault.to_string(),
            discover_receipt_cmd(
                make_receipt_id(77),
                make_shares(300),
                1500,
                tx_hash,
            ),
        )
        .await
        .unwrap();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let aggregate = context.aggregate();

        // Receipt exists but not in itn_receipts
        assert_eq!(aggregate.receipts.len(), 1);
        assert_eq!(aggregate.itn_receipts.len(), 0);
    }

    #[test]
    fn test_determine_source_returns_itn_when_receipt_info_has_issuer_request_id()
     {
        let expected_id = IssuerMintRequestId::random();
        let receipt_info = serde_json::json!({
            "tokenization_request_id": "tok-123",
            "issuer_request_id": expected_id.to_string(),
            "underlying": "AAPL",
            "quantity": "100.0",
            "operation_type": "Mint",
            "timestamp": "2024-01-01T00:00:00Z",
            "notes": null
        });

        let bytes = Bytes::from(serde_json::to_vec(&receipt_info).unwrap());
        let (source, info) = determine_source(&bytes);

        assert!(matches!(
            source,
            ReceiptSource::Itn { issuer_request_id } if issuer_request_id == expected_id
        ));
        assert!(info.is_some());
    }

    #[test]
    fn test_determine_source_returns_external_when_receipt_info_is_empty() {
        let bytes = Bytes::new();

        let (source, info) = determine_source(&bytes);

        assert!(matches!(source, ReceiptSource::External));
        assert!(info.is_none());
    }

    #[test]
    fn test_determine_source_returns_external_when_receipt_info_is_invalid_json()
     {
        let bytes = Bytes::from(vec![0x00, 0x01, 0x02]);

        let (source, info) = determine_source(&bytes);

        assert!(matches!(source, ReceiptSource::External));
        assert!(info.is_none());
    }

    async fn setup_receipt_service_with_receipts(
        receipts: Vec<(u64, u64)>,
    ) -> CqrsReceiptService<MemStore<ReceiptInventory>> {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
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

        CqrsReceiptService::new(store, cqrs)
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

    use chrono::{TimeZone, Utc};
    use proptest::prelude::*;
    use rust_decimal::Decimal;

    use crate::Quantity;
    use crate::mint::TokenizationRequestId;
    use crate::mint::tests::arb_issuer_request_id;
    use crate::test_utils::LocalEvm;
    use crate::tokenized_asset::UnderlyingSymbol;
    use crate::vault::ReceiptInformation;

    fn make_receipt_info(
        issuer_request_id: &IssuerMintRequestId,
    ) -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-test"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity(Decimal::new(100, 0)),
            Utc::now(),
            None,
        )
    }

    prop_compose! {
        fn arb_mint_receipt_information()(
            tok_id in "[a-zA-Z0-9_-]{1,64}",
            issuer_request_id in arb_issuer_request_id(),
            symbol in "[A-Z]{1,5}",
            qty in 0i64..1_000_000_000i64,
            timestamp_secs in 0i64..2_000_000_000i64,
            notes in proptest::option::of("[a-zA-Z0-9 ]{0,100}"),
        ) -> ReceiptInformation {
            ReceiptInformation::new(
                TokenizationRequestId::new(tok_id),
                issuer_request_id,
                UnderlyingSymbol::new(symbol),
                Quantity(Decimal::new(qty, 2)),
                Utc.timestamp_opt(timestamp_secs, 0).unwrap(),
                notes,
            )
        }
    }

    proptest! {
        #[test]
        fn encode_then_determine_source_preserves_issuer_request_id(
            receipt_info in arb_mint_receipt_information()
        ) {
            let encoded = receipt_info.encode().unwrap();
            let (source, parsed_info) = determine_source(&encoded);

            prop_assert!(
                matches!(&source, ReceiptSource::Itn { issuer_request_id }
                    if issuer_request_id == &receipt_info.issuer_request_id),
                "Expected Itn source with matching issuer_request_id, got {:?}", source
            );
            prop_assert!(parsed_info.is_some());
        }
    }

    #[tokio::test]
    async fn encode_then_determine_source_roundtrips_through_anvil() {
        let evm = LocalEvm::new().await.unwrap();

        evm.grant_deposit_role(evm.wallet_address).await.unwrap();
        evm.grant_certify_role(evm.wallet_address).await.unwrap();
        evm.certify_vault(U256::MAX).await.unwrap();

        let original_issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-anvil-test"),
            original_issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity(Decimal::new(10050, 2)),
            chrono::Utc::now(),
            Some("Anvil integration test".to_string()),
        );

        let encoded = receipt_info.encode().unwrap();
        let amount = U256::from(100) * U256::from(10).pow(U256::from(18));

        let (_receipt_id, _shares, returned_info) = evm
            .mint_directly_with_info(
                amount,
                evm.wallet_address,
                encoded.clone(),
            )
            .await
            .unwrap();

        assert_eq!(
            returned_info, encoded,
            "Returned receiptInformation should match what was sent"
        );

        let (source, parsed_info) = determine_source(&returned_info);
        assert!(parsed_info.is_some());
        match source {
            ReceiptSource::Itn { issuer_request_id } => {
                assert_eq!(
                    issuer_request_id.to_string(),
                    original_issuer_request_id.to_string(),
                    "Extracted issuer_request_id should match original"
                );
            }
            ReceiptSource::External => {
                panic!("Expected Itn source, got External");
            }
        }
    }

    #[tokio::test]
    async fn test_reconcile_matching_balance_emits_no_events() {
        let mut aggregate = ReceiptInventory::default();
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

        for event in events {
            aggregate.apply(event);
        }

        let events = aggregate
            .handle(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(100),
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.is_empty(),
            "Reconcile with matching balance should emit no events, got {events:?}"
        );
    }

    #[tokio::test]
    async fn test_reconcile_decreased_balance_emits_balance_reconciled() {
        let mut aggregate = ReceiptInventory::default();
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

        for event in events {
            aggregate.apply(event);
        }

        let events = aggregate
            .handle(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(50),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BalanceReconciled {
                receipt_id,
                previous_balance,
                on_chain_balance,
            } if *receipt_id == make_receipt_id(42)
                && *previous_balance == make_shares(100)
                && *on_chain_balance == make_shares(50)
        ));
    }

    #[tokio::test]
    async fn test_reconcile_zero_balance_emits_balance_reconciled_and_depleted()
    {
        let mut aggregate = ReceiptInventory::default();
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

        for event in events {
            aggregate.apply(event);
        }

        let events = aggregate
            .handle(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(0),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BalanceReconciled {
                on_chain_balance, ..
            } if on_chain_balance.is_zero()
        ));
        assert!(matches!(
            &events[1],
            ReceiptInventoryEvent::Depleted { receipt_id }
            if *receipt_id == make_receipt_id(42)
        ));
    }

    #[tokio::test]
    async fn test_reconcile_unknown_receipt_emits_no_events() {
        let aggregate = ReceiptInventory::default();

        let events = aggregate
            .handle(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(99),
                    on_chain_balance: make_shares(0),
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.is_empty(),
            "Reconcile for unknown receipt should emit no events, got {events:?}"
        );
    }

    #[tokio::test]
    async fn test_reconcile_increased_balance_returns_error() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

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

        let result = aggregate
            .handle(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(100),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(ReceiptInventoryError::UnexpectedBalanceIncrease { .. })
        ));
    }

    #[tokio::test]
    async fn test_register_minted_receipt_makes_receipt_available_for_burn() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        let service = CqrsReceiptService::new(store.clone(), cqrs);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let issuer_request_id = IssuerMintRequestId::random();

        service
            .register_minted_receipt(
                vault,
                make_receipt_id(1),
                make_shares(100),
                5000,
                tx_hash,
                make_receipt_info(&issuer_request_id),
            )
            .await
            .expect("Registration should succeed");

        let plan = service
            .for_burn(vault, make_shares(50), make_shares(0))
            .await
            .expect("Burn planning should succeed with registered receipt");

        assert_eq!(plan.total_burn, make_shares(50));
        assert_eq!(plan.allocations.len(), 1);
    }

    #[tokio::test]
    async fn test_register_minted_receipt_is_findable_by_issuer_request_id() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        let service = CqrsReceiptService::new(store.clone(), cqrs);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let issuer_request_id = IssuerMintRequestId::random();

        service
            .register_minted_receipt(
                vault,
                make_receipt_id(42),
                make_shares(200),
                6000,
                tx_hash,
                make_receipt_info(&issuer_request_id),
            )
            .await
            .expect("Registration should succeed");

        let found = service
            .find_by_issuer_request_id(&vault, &issuer_request_id)
            .await
            .expect("Lookup should succeed");

        let receipt =
            found.expect("Receipt should be found by issuer_request_id");
        assert_eq!(receipt.receipt_id, U256::from(42));
        assert_eq!(receipt.tx_hash, tx_hash);
        assert_eq!(receipt.shares, U256::from(200));
        assert_eq!(receipt.block_number, 6000);
    }

    #[tokio::test]
    async fn test_register_minted_receipt_is_idempotent() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        let service = CqrsReceiptService::new(store.clone(), cqrs);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        for _ in 0..2 {
            let issuer_request_id = IssuerMintRequestId::random();

            service
                .register_minted_receipt(
                    vault,
                    make_receipt_id(7),
                    make_shares(500),
                    7000,
                    tx_hash,
                    make_receipt_info(&issuer_request_id),
                )
                .await
                .expect("Registration should succeed (idempotent)");
        }

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(
            receipts.len(),
            1,
            "Duplicate registration should not create duplicate receipts"
        );
    }

    #[tokio::test]
    async fn test_register_minted_receipt_stores_receipt_info() {
        let store = Arc::new(MemStore::<ReceiptInventory>::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        let service = CqrsReceiptService::new(store.clone(), cqrs);
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = make_receipt_info(&issuer_request_id);

        service
            .register_minted_receipt(
                vault,
                make_receipt_id(42),
                make_shares(100),
                5000,
                tx_hash,
                receipt_info.clone(),
            )
            .await
            .expect("Registration should succeed");

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].receipt_info,
            Some(receipt_info),
            "receipt_info should be stored by register_minted_receipt"
        );
    }
}
