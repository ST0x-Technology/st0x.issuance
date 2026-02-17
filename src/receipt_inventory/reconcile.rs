use alloy::primitives::Address;
use alloy::providers::Provider;
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use futures::{StreamExt, stream};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, Shares,
};
use crate::bindings::Receipt;

/// Maximum concurrent RPC calls for balance checks.
const MAX_CONCURRENT_BALANCE_CHECKS: usize = 4;

#[derive(Debug)]
pub(crate) struct ReconcileResult {
    pub(crate) checked: usize,
    pub(crate) mismatches: usize,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReconcileError {
    #[error("Contract call error: {0}")]
    ContractCall(#[from] alloy::contract::Error),
    #[error("CQRS error: {0}")]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
    #[error("Persistence error: {0}")]
    Persistence(#[from] cqrs_es::persist::PersistenceError),
}

pub(crate) struct ReceiptReconciler<ProviderType, ReceiptInventoryStore>
where
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    provider: ProviderType,
    receipt_contract: Address,
    bot_wallet: Address,
    vault: Address,
    cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
}

impl<ProviderType, ReceiptInventoryStore>
    ReceiptReconciler<ProviderType, ReceiptInventoryStore>
where
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    pub(crate) const fn new(
        provider: ProviderType,
        receipt_contract: Address,
        bot_wallet: Address,
        vault: Address,
        cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
    ) -> Self {
        Self { provider, receipt_contract, bot_wallet, vault, cqrs }
    }
}

impl<Node, ReceiptInventoryStore> ReceiptReconciler<Node, ReceiptInventoryStore>
where
    Node: Provider + Clone + Send + Sync,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    /// Reconciles receipt inventory balances with on-chain state.
    ///
    /// Loads the aggregate to find receipts with positive balance, queries the
    /// actual on-chain balance for each, and executes ReconcileBalance commands
    /// for any mismatches (emitting ExternalBurnDetected events).
    pub(crate) async fn reconcile(
        &self,
        event_store: &ReceiptInventoryStore,
    ) -> Result<ReconcileResult, ReconcileError> {
        let aggregate_context =
            event_store.load_aggregate(&self.vault.to_string()).await?;

        let receipts: Vec<_> = aggregate_context
            .aggregate()
            .receipts_with_balance()
            .into_iter()
            .map(|receipt| (receipt.receipt_id, receipt.available_balance))
            .collect();

        if receipts.is_empty() {
            return Ok(ReconcileResult { checked: 0, mismatches: 0 });
        }

        info!(
            receipt_count = receipts.len(),
            vault = %self.vault,
            "Reconciling receipt balances with on-chain state"
        );

        let results: Vec<_> = stream::iter(receipts)
            .map(|(receipt_id, aggregate_balance)| {
                self.check_single_receipt(receipt_id, aggregate_balance)
            })
            .buffer_unordered(MAX_CONCURRENT_BALANCE_CHECKS)
            .collect()
            .await;

        let mut checked = 0usize;
        let mut mismatches = 0usize;

        for result in results {
            match result {
                Ok(true) => {
                    checked += 1;
                    mismatches += 1;
                }
                Ok(false) => {
                    checked += 1;
                }
                Err(err) => {
                    warn!(
                        vault = %self.vault,
                        error = %err,
                        "Failed to reconcile receipt"
                    );
                }
            }
        }

        info!(
            checked,
            mismatches,
            vault = %self.vault,
            "Receipt reconciliation complete"
        );

        Ok(ReconcileResult { checked, mismatches })
    }

    /// Returns true if a mismatch was detected and corrected.
    async fn check_single_receipt(
        &self,
        receipt_id: ReceiptId,
        aggregate_balance: Shares,
    ) -> Result<bool, ReconcileError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        let on_chain_balance = receipt_contract
            .balanceOf(self.bot_wallet, receipt_id.inner())
            .call()
            .await?;

        let on_chain_shares = Shares::from(on_chain_balance);

        if on_chain_shares == aggregate_balance {
            debug!(
                receipt_id = %receipt_id,
                balance = %aggregate_balance,
                "Receipt balance matches on-chain"
            );
            return Ok(false);
        }

        info!(
            receipt_id = %receipt_id,
            aggregate_balance = %aggregate_balance,
            on_chain_balance = %on_chain_shares,
            "Balance mismatch detected"
        );

        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id,
                    on_chain_balance: on_chain_shares,
                },
            )
            .await?;

        Ok(true)
    }
}

pub(crate) async fn run_startup_reconciliation<
    P: Provider + Clone,
    ES: EventStore<ReceiptInventory>,
>(
    provider: P,
    vault_receipt_contracts: &[(Address, Address)],
    cqrs: &Arc<CqrsFramework<ReceiptInventory, ES>>,
    event_store: &ES,
    bot_wallet: Address,
) -> Result<(), ReconcileError> {
    for &(vault, receipt_contract) in vault_receipt_contracts {
        let result = ReceiptReconciler::new(
            provider.clone(),
            receipt_contract,
            bot_wallet,
            vault,
            cqrs.clone(),
        )
        .reconcile(event_store)
        .await?;

        if result.mismatches > 0 {
            info!(
                vault = %vault,
                checked = result.checked,
                mismatches = result.mismatches,
                "Receipt reconciliation found mismatches"
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, b256, uint};
    use alloy::providers::ProviderBuilder;
    use alloy::sol_types::SolEvent;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::{AggregateContext, CqrsFramework, EventStore};
    use std::sync::Arc;

    use super::*;
    use crate::receipt_inventory::{
        ReceiptId, ReceiptInventory, ReceiptInventoryCommand, ReceiptSource,
        Shares,
    };
    use crate::test_utils::LocalEvm;

    type TestStore = MemStore<ReceiptInventory>;
    type TestCqrs = CqrsFramework<ReceiptInventory, TestStore>;

    fn setup_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn seed_receipt(
        cqrs: &TestCqrs,
        vault: Address,
        receipt_id: U256,
        balance: U256,
    ) {
        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: ReceiptId::from(receipt_id),
                balance: Shares::from(balance),
                block_number: 1,
                tx_hash: b256!(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                source: ReceiptSource::External,
                receipt_info: None,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_reconcile_empty_aggregate_returns_zero_counts() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_cqrs();

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
        let vault_contract = crate::bindings::OffchainAssetReceiptVault::new(
            evm.vault_address,
            &provider,
        );
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        let reconciler = ReceiptReconciler::new(
            provider,
            receipt_contract,
            evm.wallet_address,
            evm.vault_address,
            cqrs,
        );

        let result = reconciler.reconcile(store.as_ref()).await.unwrap();

        assert_eq!(result.checked, 0);
        assert_eq!(result.mismatches, 0);
    }

    #[tokio::test]
    async fn test_reconcile_detects_stale_receipt_with_zero_on_chain_balance() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_cqrs();

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
        let vault_contract = crate::bindings::OffchainAssetReceiptVault::new(
            evm.vault_address,
            &provider,
        );
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Seed aggregate with a receipt that doesn't exist on-chain (never minted on Anvil)
        let stale_receipt_id = uint!(0xff_U256);
        let stale_balance =
            U256::from(100) * U256::from(10).pow(U256::from(18));
        seed_receipt(&cqrs, evm.vault_address, stale_receipt_id, stale_balance)
            .await;

        let reconciler = ReceiptReconciler::new(
            provider,
            receipt_contract,
            evm.wallet_address,
            evm.vault_address,
            cqrs.clone(),
        );

        let result = reconciler.reconcile(store.as_ref()).await.unwrap();

        assert_eq!(result.checked, 1, "Should have checked 1 receipt");
        assert_eq!(result.mismatches, 1, "Should have detected 1 mismatch");

        // Verify the aggregate was updated — receipt should be depleted
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert!(
            receipts.is_empty(),
            "Stale receipt should be removed after reconciliation"
        );
    }

    #[tokio::test]
    async fn test_reconcile_matching_balance_reports_no_mismatch() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_cqrs();

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
        let vault_contract = crate::bindings::OffchainAssetReceiptVault::new(
            evm.vault_address,
            &provider,
        );
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Mint a real receipt on Anvil first, then seed aggregate with matching balance
        evm.grant_deposit_role(evm.wallet_address).await.unwrap();
        evm.grant_certify_role(evm.wallet_address).await.unwrap();
        evm.certify_vault(U256::MAX).await.unwrap();

        let one_share = U256::from(10).pow(U256::from(18));
        let deposit_amount = U256::from(50) * one_share;

        let signer = alloy::signers::local::PrivateKeySigner::from_bytes(
            &evm.private_key,
        )
        .unwrap();
        let wallet = alloy::network::EthereumWallet::from(signer);
        let bot_provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&evm.endpoint)
            .await
            .unwrap();

        let vault = crate::bindings::OffchainAssetReceiptVault::new(
            evm.vault_address,
            &bot_provider,
        );

        let receipt_result = vault
            .deposit(
                deposit_amount,
                evm.wallet_address,
                U256::ZERO,
                alloy::primitives::Bytes::new(),
            )
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Get the receipt ID from the Deposit event
        let deposit_log = receipt_result
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                crate::bindings::OffchainAssetReceiptVault::Deposit::decode_log(
                    &log.inner,
                )
                .ok()
            })
            .expect("Should find Deposit event");

        let receipt_id = deposit_log.id;
        let minted_shares = deposit_log.shares;

        // Seed aggregate with the same balance as on-chain
        seed_receipt(&cqrs, evm.vault_address, receipt_id, minted_shares).await;

        let reconciler = ReceiptReconciler::new(
            bot_provider,
            receipt_contract,
            evm.wallet_address,
            evm.vault_address,
            cqrs,
        );

        let result = reconciler.reconcile(store.as_ref()).await.unwrap();

        assert_eq!(result.checked, 1, "Should have checked 1 receipt");
        assert_eq!(
            result.mismatches, 0,
            "Balance matches, no mismatch expected"
        );
    }
}
