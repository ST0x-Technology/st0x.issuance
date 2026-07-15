use alloy::primitives::Address;
use alloy::providers::Provider;
use cqrs_es::AggregateError;
use event_sorcery::Store;
use futures::{StreamExt, stream};
use std::sync::Arc;
use tracing::{debug, info, trace};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, Shares, load_inventory,
    send_receipt_inventory_command,
};
use crate::bindings::Receipt;

/// Maximum concurrent RPC calls for balance checks.
const MAX_CONCURRENT_BALANCE_CHECKS: usize = 4;

#[derive(Debug)]
pub(crate) struct ReconcileResult {
    pub(crate) checked: usize,
    pub(crate) mismatches: usize,
    pub(crate) errors: usize,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReconcileError {
    #[error("Contract call error: {0}")]
    ContractCall(#[from] alloy::contract::Error),
    // Store operations surface SQLite failures as
    // `AggregateError::DatabaseConnectionError` inside this variant; there is
    // no separate persistence error path.
    #[error("CQRS error: {0}")]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
}

pub(crate) struct ReceiptReconciler<Node> {
    provider: Node,
    receipt_contract: Address,
    bot_wallet: Address,
    vault: Address,
    store: Arc<Store<ReceiptInventory>>,
}

impl<Node> ReceiptReconciler<Node> {
    pub(crate) const fn new(
        provider: Node,
        receipt_contract: Address,
        bot_wallet: Address,
        vault: Address,
        store: Arc<Store<ReceiptInventory>>,
    ) -> Self {
        Self { provider, receipt_contract, bot_wallet, vault, store }
    }
}

impl<Node> ReceiptReconciler<Node>
where
    Node: Provider + Clone + Send + Sync,
{
    /// Reconciles receipt inventory balances with on-chain state.
    ///
    /// Loads the aggregate to find receipts with positive balance, queries the
    /// actual on-chain balance for each, and executes ReconcileBalance commands
    /// for any mismatches (emitting BalanceReconciled, and Depleted when the
    /// on-chain balance is zero).
    pub(crate) async fn reconcile(
        &self,
    ) -> Result<ReconcileResult, ReconcileError> {
        let inventory = load_inventory(&self.store, &self.vault).await?;

        let receipts: Vec<_> = inventory
            .receipts_with_balance()
            .into_iter()
            .map(|receipt| (receipt.receipt_id, receipt.available_balance))
            .collect();

        if receipts.is_empty() {
            return Ok(ReconcileResult {
                checked: 0,
                mismatches: 0,
                errors: 0,
            });
        }

        info!(target: "receipt", receipt_count = receipts.len(),
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
        let mut errors = 0usize;

        for result in results {
            match result {
                Ok(true) => {
                    checked += 1;
                    mismatches += 1;
                }
                Ok(false) => {
                    checked += 1;
                }
                Err(error @ ReconcileError::Aggregate(_)) => {
                    return Err(error);
                }
                Err(err) => {
                    debug!(target: "receipt", vault = %self.vault,
                        error = %err,
                        "Failed to reconcile receipt"
                    );
                    errors += 1;
                }
            }
        }

        info!(target: "receipt", checked,
            mismatches,
            errors,
            vault = %self.vault,
            "Receipt reconciliation complete"
        );

        Ok(ReconcileResult { checked, mismatches, errors })
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
            trace!(target: "receipt", receipt_id = %receipt_id,
                balance = %aggregate_balance,
                "Receipt balance matches on-chain"
            );
            return Ok(false);
        }

        trace!(target: "receipt", receipt_id = %receipt_id,
            aggregate_balance = %aggregate_balance,
            on_chain_balance = %on_chain_shares,
            "Balance mismatch detected"
        );

        send_receipt_inventory_command(
            &self.store,
            &self.vault,
            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id,
                on_chain_balance: on_chain_shares,
            },
        )
        .await?;

        Ok(true)
    }
}

pub(crate) async fn run_startup_reconciliation<P: Provider + Clone>(
    provider: P,
    vault_receipt_contracts: &[(Address, Address)],
    store: &Arc<Store<ReceiptInventory>>,
    bot_wallet: Address,
) -> Result<(), ReconcileError> {
    let results = stream::iter(vault_receipt_contracts.iter().copied())
        .then(|(vault, receipt_contract)| {
            let provider = provider.clone();
            let store = store.clone();
            async move {
                let result = ReceiptReconciler::new(
                    provider,
                    receipt_contract,
                    bot_wallet,
                    vault,
                    store,
                )
                .reconcile()
                .await;

                (vault, receipt_contract, result)
            }
        })
        .collect::<Vec<_>>()
        .await;

    let mut total_checked = 0usize;
    let mut total_mismatches = 0usize;
    let mut total_errors = 0usize;
    let mut failed_vaults = 0usize;
    let mut first_error = None;

    for (vault, receipt_contract, result) in results {
        match result {
            Ok(result) => {
                total_checked += result.checked;
                total_mismatches += result.mismatches;
                total_errors += result.errors;

                if result.mismatches > 0 {
                    debug!(target: "receipt", vault = %vault,
                        checked = result.checked,
                        mismatches = result.mismatches,
                        "Receipt reconciliation found mismatches"
                    );
                }
            }
            Err(err) => {
                debug!(target: "receipt", vault = %vault,
                    receipt_contract = %receipt_contract,
                    error = %err,
                    "Receipt reconciliation failed for vault"
                );
                failed_vaults += 1;
                first_error.get_or_insert(err);
            }
        }
    }

    if total_mismatches > 0 || total_errors > 0 || failed_vaults > 0 {
        info!(target: "receipt", total_checked,
            total_mismatches, total_errors, failed_vaults,
            "Receipt reconciliation complete"
        );
    }

    first_error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, b256, uint};
    use alloy::providers::ProviderBuilder;
    use alloy::sol_types::SolEvent;
    use event_sorcery::test_store;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::*;
    use crate::receipt_inventory::{
        ReceiptId, ReceiptInventory, ReceiptInventoryCommand, ReceiptSource,
        Shares,
    };
    use crate::test_utils::{LocalEvm, logs_contain_at};

    async fn setup_store() -> Arc<Store<ReceiptInventory>> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");
        Arc::new(test_store::<ReceiptInventory>(pool, ()))
    }

    async fn seed_receipt(
        store: &Arc<Store<ReceiptInventory>>,
        vault: Address,
        receipt_id: U256,
        balance: U256,
    ) {
        store
            .send(
                &vault,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(receipt_id),
                    balance: Shares::from(balance),
                    block_number: 1,
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    source: ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_reconcile_empty_aggregate_returns_zero_counts() {
        let evm = LocalEvm::new().await.unwrap();
        let store = setup_store().await;

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
            store,
        );

        let result = reconciler.reconcile().await.unwrap();

        assert_eq!(result.checked, 0);
        assert_eq!(result.mismatches, 0);
        assert_eq!(result.errors, 0);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_reconcile_counts_errors_for_failing_balance_check() {
        let evm = LocalEvm::new().await.unwrap();
        let store = setup_store().await;

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        seed_receipt(
            &store,
            evm.vault_address,
            uint!(0xff_U256),
            U256::from(100) * U256::from(10).pow(U256::from(18)),
        )
        .await;

        // An address with no contract deployed: `balanceOf` returns no data,
        // so the call errors instead of reporting a balance. The receipt must
        // be counted as an error — not silently checked or mismatched.
        let missing_receipt_contract = Address::random();

        let reconciler = ReceiptReconciler::new(
            provider,
            missing_receipt_contract,
            evm.wallet_address,
            evm.vault_address,
            store,
        );

        let result = reconciler.reconcile().await.unwrap();

        assert_eq!(result.errors, 1, "balance-check failure must be counted");
        assert_eq!(result.checked, 0, "an errored receipt is not checked");
        assert_eq!(result.mismatches, 0);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Failed to reconcile receipt"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Receipt reconciliation complete", "errors=1"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_startup_reconciliation_accumulates_totals_across_vaults() {
        let evm = LocalEvm::new().await.unwrap();
        let store = setup_store().await;

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
        let vault_contract = crate::bindings::OffchainAssetReceiptVault::new(
            evm.vault_address,
            &provider,
        );
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // First vault: a stale receipt that was never minted on Anvil, so its
        // on-chain balance is zero -> one checked receipt with one mismatch.
        seed_receipt(
            &store,
            evm.vault_address,
            uint!(0xff_U256),
            U256::from(100) * U256::from(10).pow(U256::from(18)),
        )
        .await;

        // Second vault: its receipt contract address has no contract deployed,
        // so the balance check errors -> one per-receipt error, which must be
        // accumulated and must not fail the overall startup reconciliation.
        let vault_without_contract = Address::random();
        seed_receipt(
            &store,
            vault_without_contract,
            uint!(0x2_U256),
            U256::from(10) * U256::from(10).pow(U256::from(18)),
        )
        .await;

        let result = run_startup_reconciliation(
            provider,
            &[
                (evm.vault_address, receipt_contract),
                (vault_without_contract, Address::random()),
            ],
            &store,
            evm.wallet_address,
        )
        .await;

        assert!(
            result.is_ok(),
            "per-receipt errors must not fail startup reconciliation: {result:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Receipt reconciliation complete",
                "total_checked=1",
                "total_mismatches=1",
                "total_errors=1",
                "failed_vaults=0"
            ]
        ));
    }

    #[tokio::test]
    async fn test_reconcile_detects_stale_receipt_with_zero_on_chain_balance() {
        let evm = LocalEvm::new().await.unwrap();
        let store = setup_store().await;

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
        seed_receipt(
            &store,
            evm.vault_address,
            stale_receipt_id,
            stale_balance,
        )
        .await;

        let reconciler = ReceiptReconciler::new(
            provider,
            receipt_contract,
            evm.wallet_address,
            evm.vault_address,
            store.clone(),
        );

        let result = reconciler.reconcile().await.unwrap();

        assert_eq!(result.checked, 1, "Should have checked 1 receipt");
        assert_eq!(result.mismatches, 1, "Should have detected 1 mismatch");

        // Verify the aggregate was updated — receipt should be depleted
        let inventory =
            load_inventory(&store, &evm.vault_address).await.unwrap();
        let receipts = inventory.receipts_with_balance();
        assert!(
            receipts.is_empty(),
            "Stale receipt should be removed after reconciliation"
        );
    }

    #[tokio::test]
    async fn test_reconcile_matching_balance_reports_no_mismatch() {
        let evm = LocalEvm::new().await.unwrap();
        let store = setup_store().await;

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

        let deposit_amount = uint!(50_000000000000000000_U256);

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
        seed_receipt(&store, evm.vault_address, receipt_id, minted_shares)
            .await;

        let reconciler = ReceiptReconciler::new(
            bot_provider,
            receipt_contract,
            evm.wallet_address,
            evm.vault_address,
            store,
        );

        let result = reconciler.reconcile().await.unwrap();

        assert_eq!(result.checked, 1, "Should have checked 1 receipt");
        assert_eq!(
            result.mismatches, 0,
            "Balance matches, no mismatch expected"
        );
    }
}
