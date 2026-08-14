use alloy::primitives::Address;
use alloy::providers::Provider;
use cqrs_es::AggregateError;
use event_sorcery::Store;
use futures::{StreamExt, stream};
use std::sync::Arc;
use tracing::{debug, error, info, trace};

use super::{
    Custody, ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
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
    #[error(
        "Wallet {wallet} holds none of the {receipt_count} receipts the \
         inventory claims for vault {vault}; refusing to deplete them. The \
         receipts are most likely still held by the previous signing wallet — \
         point the service at the wallet that holds them, or use the approved \
         custody-migration procedure."
    )]
    CustodyDisplaced { vault: Address, wallet: Address, receipt_count: usize },
}

pub(crate) struct ReceiptReconciler<Node> {
    provider: Node,
    receipt_contract: Address,
    bot_wallet: Address,
    chain_id: u64,
    vault: Address,
    store: Arc<Store<ReceiptInventory>>,
}

impl<Node> ReceiptReconciler<Node> {
    pub(crate) const fn new(
        provider: Node,
        receipt_contract: Address,
        bot_wallet: Address,
        chain_id: u64,
        vault: Address,
        store: Arc<Store<ReceiptInventory>>,
    ) -> Self {
        Self { provider, receipt_contract, bot_wallet, chain_id, vault, store }
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
    ///
    /// A zero on-chain balance only means "spent" while the signing wallet is
    /// the one the inventory was built against. Every balance is therefore read
    /// before anything is written, so a wallet rotation can be recognised for
    /// what it is instead of being applied as mass depletion — see
    /// [`super::Custody`].
    pub(crate) async fn reconcile(
        &self,
    ) -> Result<ReconcileResult, ReconcileError> {
        let inventory =
            load_inventory(&self.store, self.chain_id, &self.vault).await?;

        let receipts: Vec<_> = inventory
            .receipts_with_balance()
            .into_iter()
            .map(|receipt| (receipt.receipt_id, receipt.available_balance))
            .collect();

        // An empty inventory proves nothing about which wallet holds
        // anything, so no custody is confirmed here — recording a holder on
        // no evidence would arm the guard around the wrong wallet.
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

        let readings: Vec<_> = stream::iter(receipts)
            .map(|(receipt_id, aggregate_balance)| async move {
                (
                    receipt_id,
                    aggregate_balance,
                    self.read_on_chain_balance(receipt_id).await,
                )
            })
            .buffer_unordered(MAX_CONCURRENT_BALANCE_CHECKS)
            .collect()
            .await;

        self.refuse_if_displaced(inventory.custody(), &readings)?;

        let mut checked = 0usize;
        let mut mismatches = 0usize;
        let mut errors = 0usize;

        for (receipt_id, aggregate_balance, reading) in readings {
            match reading {
                Ok(on_chain_balance) => {
                    checked += 1;

                    if self
                        .apply_reading(
                            receipt_id,
                            aggregate_balance,
                            on_chain_balance,
                        )
                        .await?
                    {
                        mismatches += 1;
                    }
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

        // A pass that could not read every balance has not proven the wallet
        // holds this vault's receipts, so custody stays as it was and the next
        // pass re-checks.
        if errors == 0 {
            self.confirm_custody().await?;
        }

        info!(target: "receipt", checked,
            mismatches,
            errors,
            vault = %self.vault,
            "Receipt reconciliation complete"
        );

        Ok(ReconcileResult { checked, mismatches, errors })
    }

    /// Fails the pass without writing anything when the signing wallet changed
    /// and does not hold receipts the inventory still claims.
    ///
    /// Those receipts are almost certainly sitting at the previous wallet:
    /// depleting them would erase inventory backed by real on-chain receipts,
    /// and the backfiller's checkpoint is already past the deposits that
    /// created them, so nothing would ever rediscover them.
    fn refuse_if_displaced(
        &self,
        custody: &Custody,
        readings: &[(ReceiptId, Shares, Result<Shares, ReconcileError>)],
    ) -> Result<(), ReconcileError> {
        // Unobserved custody is the pre-cutover baseline, and an unchanged
        // holder means a zero reading really is a spent receipt. Only a holder
        // that has moved makes the reading ambiguous.
        let Some(previous) =
            custody.holder().filter(|holder| *holder != self.bot_wallet)
        else {
            return Ok(());
        };

        let displaced = readings
            .iter()
            .filter(|(_, _, reading)| {
                matches!(reading, Ok(balance) if balance.is_zero())
            })
            .count();

        if displaced == 0 {
            return Ok(());
        }

        error!(target: "receipt", vault = %self.vault,
            wallet = %self.bot_wallet,
            previous_wallet = %previous,
            receipt_count = displaced,
            "Refusing to deplete receipts the signing wallet does not hold"
        );

        Err(ReconcileError::CustodyDisplaced {
            vault: self.vault,
            wallet: self.bot_wallet,
            receipt_count: displaced,
        })
    }

    async fn read_on_chain_balance(
        &self,
        receipt_id: ReceiptId,
    ) -> Result<Shares, ReconcileError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        let on_chain_balance = receipt_contract
            .balanceOf(self.bot_wallet, receipt_id.inner())
            .call()
            .await?;

        Ok(Shares::from(on_chain_balance))
    }

    /// Returns true if a mismatch was detected and corrected.
    async fn apply_reading(
        &self,
        receipt_id: ReceiptId,
        aggregate_balance: Shares,
        on_chain_balance: Shares,
    ) -> Result<bool, ReconcileError> {
        if on_chain_balance == aggregate_balance {
            trace!(target: "receipt", receipt_id = %receipt_id,
                balance = %aggregate_balance,
                "Receipt balance matches on-chain"
            );
            return Ok(false);
        }

        trace!(target: "receipt", receipt_id = %receipt_id,
            aggregate_balance = %aggregate_balance,
            on_chain_balance = %on_chain_balance,
            "Balance mismatch detected"
        );

        send_receipt_inventory_command(
            &self.store,
            self.chain_id,
            &self.vault,
            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id,
                on_chain_balance,
                observed_wallet: self.bot_wallet,
            },
        )
        .await?;

        Ok(true)
    }

    /// Records the wallet these balances were read against.
    ///
    /// The command is a no-op when custody is already this wallet, so the
    /// periodic reconciler does not append an event per pass.
    async fn confirm_custody(&self) -> Result<(), ReconcileError> {
        send_receipt_inventory_command(
            &self.store,
            self.chain_id,
            &self.vault,
            ReceiptInventoryCommand::ConfirmCustody { holder: self.bot_wallet },
        )
        .await?;

        Ok(())
    }
}

pub(crate) async fn run_startup_reconciliation<P: Provider + Clone>(
    provider: P,
    entries: &[(u64, Address, Address)],
    store: &Arc<Store<ReceiptInventory>>,
    bot_wallet: Address,
) -> Result<(), ReconcileError> {
    let results = stream::iter(entries.iter().copied())
        .then(|(chain_id, vault, receipt_contract)| {
            let provider = provider.clone();
            let store = store.clone();
            async move {
                let result = ReceiptReconciler::new(
                    provider,
                    receipt_contract,
                    bot_wallet,
                    chain_id,
                    vault,
                    store,
                )
                .reconcile()
                .await;

                (chain_id, vault, receipt_contract, result)
            }
        })
        .collect::<Vec<_>>()
        .await;

    let mut total_checked = 0usize;
    let mut total_mismatches = 0usize;
    let mut total_errors = 0usize;
    let mut failed_vaults = 0usize;
    let mut first_error = None;

    for (_chain_id, vault, receipt_contract, result) in results {
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
        ReceiptVaultKey, Shares,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm, logs_contain_at};

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
                &ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault),
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

    /// Mirrors the custody-confirmation bootstrap: depletion from a zero
    /// reading only applies once the holder is on record.
    async fn seed_custody(
        store: &Arc<Store<ReceiptInventory>>,
        vault: Address,
        holder: Address,
    ) {
        store
            .send(
                &ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault),
                ReceiptInventoryCommand::ConfirmCustody { holder },
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
            ANVIL_CHAIN_ID,
            evm.vault_address,
            store.clone(),
        );

        let result = reconciler.reconcile().await.unwrap();

        assert_eq!(result.checked, 0);
        assert_eq!(result.mismatches, 0);
        assert_eq!(result.errors, 0);

        // An empty inventory proves nothing about who holds anything, so the
        // pass must not record a holder.
        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &evm.vault_address)
                .await
                .unwrap();
        assert_eq!(*inventory.custody(), Custody::Unobserved);
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
            crate::test_utils::ANVIL_CHAIN_ID,
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
        seed_custody(&store, evm.vault_address, evm.wallet_address).await;

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
                (
                    crate::test_utils::ANVIL_CHAIN_ID,
                    evm.vault_address,
                    receipt_contract,
                ),
                (
                    crate::test_utils::ANVIL_CHAIN_ID,
                    vault_without_contract,
                    Address::random(),
                ),
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
        seed_custody(&store, evm.vault_address, evm.wallet_address).await;

        let reconciler = ReceiptReconciler::new(
            provider,
            receipt_contract,
            evm.wallet_address,
            ANVIL_CHAIN_ID,
            evm.vault_address,
            store.clone(),
        );

        let result = reconciler.reconcile().await.unwrap();

        assert_eq!(result.checked, 1, "Should have checked 1 receipt");
        assert_eq!(result.mismatches, 1, "Should have detected 1 mismatch");

        // Verify the aggregate was updated — receipt should be depleted
        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &evm.vault_address)
                .await
                .unwrap();
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
            ANVIL_CHAIN_ID,
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

    mod custody_displacement {
        use super::*;
        use crate::receipt_inventory::Custody;

        /// Establishes custody the way the service does — through the
        /// aggregate — so the guard is tested against real recorded state.
        async fn seed_custody(
            store: &Arc<Store<ReceiptInventory>>,
            vault: Address,
            holder: Address,
        ) {
            send_receipt_inventory_command(
                store,
                ANVIL_CHAIN_ID,
                &vault,
                ReceiptInventoryCommand::ConfirmCustody { holder },
            )
            .await
            .unwrap();
        }

        async fn custody_of(
            store: &Arc<Store<ReceiptInventory>>,
            vault: Address,
        ) -> Custody {
            load_inventory(store, ANVIL_CHAIN_ID, &vault)
                .await
                .unwrap()
                .custody()
                .clone()
        }

        /// The cutover hazard. The service restarts on the incoming signing
        /// wallet before that wallet holds the receipts, so every balance reads
        /// zero. Depleting on that reading would erase inventory whose receipts
        /// are sitting untouched at the outgoing wallet — and the backfiller
        /// has already checkpointed past the deposits that created them, so
        /// nothing would ever rediscover them.
        #[traced_test]
        #[tokio::test]
        async fn a_rotated_wallet_holding_nothing_is_refused_not_depleted() {
            let evm = LocalEvm::new().await.unwrap();
            let store = setup_store().await;

            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
            let vault_contract =
                crate::bindings::OffchainAssetReceiptVault::new(
                    evm.vault_address,
                    &provider,
                );
            let receipt_contract =
                Address::from(vault_contract.receipt().call().await.unwrap().0);

            let balance = U256::from(100) * U256::from(10).pow(U256::from(18));
            seed_receipt(&store, evm.vault_address, uint!(0xaa_U256), balance)
                .await;
            seed_receipt(&store, evm.vault_address, uint!(0xbb_U256), balance)
                .await;

            let outgoing = Address::random();
            seed_custody(&store, evm.vault_address, outgoing).await;

            let reconciler = ReceiptReconciler::new(
                provider,
                receipt_contract,
                evm.wallet_address,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                store.clone(),
            );

            let error = reconciler.reconcile().await.unwrap_err();

            assert!(
                matches!(
                    error,
                    ReconcileError::CustodyDisplaced { receipt_count: 2, .. }
                ),
                "expected both receipts reported as displaced, got: {error:?}"
            );

            let inventory =
                load_inventory(&store, ANVIL_CHAIN_ID, &evm.vault_address)
                    .await
                    .unwrap();
            assert_eq!(
                inventory.receipts_with_balance().len(),
                2,
                "a displaced inventory must survive the pass intact"
            );
            assert!(logs_contain_at!(
                tracing::Level::ERROR,
                &[
                    "Refusing to deplete receipts the signing wallet does not \
                     hold",
                    "receipt_count=2"
                ]
            ));
        }

        /// Only the depletion is refused, not the whole cutover: once custody
        /// has actually moved, the incoming wallet holds every receipt, the
        /// pass succeeds, and the newly recorded holder makes subsequent
        /// passes ordinary again.
        #[traced_test]
        #[tokio::test]
        async fn a_rotated_wallet_that_holds_the_receipts_is_accepted() {
            let evm = LocalEvm::new().await.unwrap();
            let store = setup_store().await;

            let (bot_provider, receipt_contract, receipt_id, minted_shares) =
                deposit_one_receipt(&evm).await;

            seed_receipt(&store, evm.vault_address, receipt_id, minted_shares)
                .await;

            seed_custody(&store, evm.vault_address, Address::random()).await;

            let reconciler = ReceiptReconciler::new(
                bot_provider,
                receipt_contract,
                evm.wallet_address,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                store.clone(),
            );

            let result = reconciler.reconcile().await.unwrap();

            assert_eq!(result.checked, 1);
            assert_eq!(result.mismatches, 0);

            assert_eq!(
                custody_of(&store, evm.vault_address).await.holder(),
                Some(evm.wallet_address),
                "a verified wallet must be recorded so later passes reconcile \
                 normally"
            );
            assert!(logs_contain_at!(
                tracing::Level::INFO,
                &["Receipt reconciliation complete", "errors=0"]
            ));
        }

        /// The guard must not cost us ordinary cleanup: with the wallet
        /// unchanged, a receipt that really was spent still gets depleted.
        #[traced_test]
        #[tokio::test]
        async fn an_unchanged_wallet_still_depletes_a_spent_receipt() {
            let evm = LocalEvm::new().await.unwrap();
            let store = setup_store().await;

            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
            let vault_contract =
                crate::bindings::OffchainAssetReceiptVault::new(
                    evm.vault_address,
                    &provider,
                );
            let receipt_contract =
                Address::from(vault_contract.receipt().call().await.unwrap().0);

            seed_receipt(
                &store,
                evm.vault_address,
                uint!(0xff_U256),
                U256::from(100) * U256::from(10).pow(U256::from(18)),
            )
            .await;

            seed_custody(&store, evm.vault_address, evm.wallet_address).await;

            let reconciler = ReceiptReconciler::new(
                provider,
                receipt_contract,
                evm.wallet_address,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                store.clone(),
            );

            let result = reconciler.reconcile().await.unwrap();

            assert_eq!(result.mismatches, 1);

            let inventory =
                load_inventory(&store, ANVIL_CHAIN_ID, &evm.vault_address)
                    .await
                    .unwrap();
            assert!(
                inventory.receipts_with_balance().is_empty(),
                "an unchanged wallet reading zero means the receipt was spent"
            );
            assert!(logs_contain_at!(
                tracing::Level::INFO,
                &["Receipt reconciliation complete", "mismatches=1"]
            ));
        }

        /// A pass that could not read every balance has proven nothing about
        /// where custody sits, so it must not record the wallet as verified —
        /// otherwise one flaky RPC call during the cutover window would
        /// silently disarm the guard for every later pass.
        #[traced_test]
        #[tokio::test]
        async fn a_pass_with_unreadable_balances_records_no_holder() {
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

            let reconciler = ReceiptReconciler::new(
                provider,
                Address::random(),
                evm.wallet_address,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                store.clone(),
            );

            let result = reconciler.reconcile().await.unwrap();
            assert_eq!(result.errors, 1);

            assert_eq!(
                custody_of(&store, evm.vault_address).await,
                Custody::Unobserved,
                "no holder may be recorded from an unverified pass"
            );
            assert!(logs_contain_at!(
                tracing::Level::DEBUG,
                &["Failed to reconcile receipt"]
            ));
        }

        async fn deposit_one_receipt(
            evm: &LocalEvm,
        ) -> (impl Provider + Clone, Address, U256, U256) {
            evm.grant_deposit_role(evm.wallet_address).await.unwrap();
            evm.grant_certify_role(evm.wallet_address).await.unwrap();
            evm.certify_vault(U256::MAX).await.unwrap();

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
            let receipt_contract =
                Address::from(vault.receipt().call().await.unwrap().0);

            let deposit_receipt = vault
                .deposit(
                    uint!(50_000000000000000000_U256),
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

            let deposit_log = deposit_receipt
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

            (bot_provider, receipt_contract, deposit_log.id, deposit_log.shares)
        }
    }
}
