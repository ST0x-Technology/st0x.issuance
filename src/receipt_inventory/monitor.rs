use alloy::primitives::{Address, B256, Bytes, TxHash};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use futures::StreamExt;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use task_supervisor::{SupervisedTask, TaskResult};
use tracing::{info, warn};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, ReceiptSource, Shares, determine_source,
};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::mint::IssuerMintRequestId;
use crate::tokenized_asset::UnderlyingSymbol;

/// Configuration parameters for the receipt monitor.
#[derive(Clone)]
pub(crate) struct ReceiptMonitorCtx {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) bot_wallet: Address,
    pub(crate) pool: Pool<Sqlite>,
}

impl ReceiptMonitorCtx {
    pub(crate) fn task_name(&self) -> String {
        format!("receipt-monitor-{}", self.underlying)
    }
}

/// Called when the receipt monitor discovers an ITN receipt (a receipt
/// with a valid `issuer_request_id` in its receiptInformation).
///
/// Production implementation triggers mint recovery for the associated mint.
/// The `tx_hash` is the on-chain transaction that created the receipt,
/// serving as compiler-enforced evidence that the mint succeeded on-chain.
#[async_trait]
pub(crate) trait ItnReceiptHandler: Send + Sync {
    async fn on_itn_receipt_discovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    );
}

/// Monitors for Deposit events on the vault in real-time.
///
/// This complements the backfiller by detecting deposits while the
/// service is running (e.g., manual operations).
/// When a deposit to the bot wallet is detected, it emits a `DiscoverReceipt`
/// command to register the receipt in the ReceiptInventory aggregate.
///
/// See `ReceiptBackfiller` for the assumption about all mints using
/// the bot wallet as depositor.
pub(crate) struct ReceiptMonitor<Node, ReceiptInventoryStore, Handler>
where
    Node: Provider + Clone,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
    Handler: ItnReceiptHandler + Clone,
{
    node: Node,
    underlying: UnderlyingSymbol,
    bot_wallet: Address,
    pool: Pool<Sqlite>,
    cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
    handler: Handler,
}

impl<Node, ReceiptInventoryStore, Handler> Clone
    for ReceiptMonitor<Node, ReceiptInventoryStore, Handler>
where
    Node: Provider + Clone,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
    Handler: ItnReceiptHandler + Clone,
{
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            underlying: self.underlying.clone(),
            bot_wallet: self.bot_wallet,
            pool: self.pool.clone(),
            cqrs: self.cqrs.clone(),
            handler: self.handler.clone(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptMonitorError {
    #[error("RPC transport error: {0}")]
    Transport(#[from] RpcError<TransportErrorKind>),
    #[error("Failed to decode event: {0}")]
    SolTypes(#[from] alloy::sol_types::Error),
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error("Contract call error: {0}")]
    ContractCall(#[from] alloy::contract::Error),
    #[error("Aggregate error: {0}")]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
    #[error("Vault discovery error: {0}")]
    VaultDiscovery(#[from] crate::tokenized_asset::view::VaultDiscoveryError),
    #[error("WebSocket stream ended unexpectedly")]
    StreamEnded,
}

impl<Node, ReceiptInventoryStore, Handler>
    ReceiptMonitor<Node, ReceiptInventoryStore, Handler>
where
    Node: Provider + Clone,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
    Handler: ItnReceiptHandler + Clone,
{
    pub(crate) fn new(
        node: Node,
        ctx: ReceiptMonitorCtx,
        cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
        handler: Handler,
    ) -> Self {
        Self {
            node,
            underlying: ctx.underlying,
            bot_wallet: ctx.bot_wallet,
            pool: ctx.pool,
            cqrs,
            handler,
        }
    }
}

impl<Node, ReceiptInventoryStore, Handler>
    ReceiptMonitor<Node, ReceiptInventoryStore, Handler>
where
    Node: Provider + Clone + Send + Sync + 'static,
    ReceiptInventoryStore: EventStore<ReceiptInventory> + 'static,
    ReceiptInventoryStore::AC: Send,
    Handler: ItnReceiptHandler + Clone,
{
    /// Runs the monitoring loop with automatic retry on errors.
    ///
    /// This method never returns under normal operation. If an error occurs,
    /// it logs the error and retries after 5 seconds.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn run_monitor(&self) {
        loop {
            if let Err(err) = self.monitor_once().await {
                warn!("Receipt monitor error: {err}. Retrying in 5s...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

    /// Monitors for Deposit and Withdraw events, subscribing and processing them.
    ///
    /// Looks up current vault details from the tokenized asset view at the
    /// start of each cycle, so restarts automatically pick up address changes.
    async fn monitor_once(&self) -> Result<(), ReceiptMonitorError> {
        let vault_ctx = crate::tokenized_asset::discover_vault(
            &self.pool,
            &self.node.root().clone().erased(),
            &self.underlying,
        )
        .await?;

        let vault_contract =
            OffchainAssetReceiptVault::new(vault_ctx.vault, &self.node);

        // Subscribe to all Deposit and Withdraw events on the vault.
        // We filter by owner == bot_wallet client-side since owner is not indexed.
        let deposit_filter = vault_contract.Deposit_filter().filter;
        let withdraw_filter = vault_contract.Withdraw_filter().filter;

        info!(
            underlying = %self.underlying,
            vault = %vault_ctx.vault,
            bot_wallet = %self.bot_wallet,
            "Subscribing to Deposit and Withdraw events"
        );

        let deposit_sub = self.node.subscribe_logs(&deposit_filter).await?;
        let withdraw_sub =
            self.provider.subscribe_logs(&withdraw_filter).await?;

        let mut deposit_stream = deposit_sub.into_stream();
        let mut withdraw_stream = withdraw_sub.into_stream();

        info!("Receipt monitor WebSocket subscription active");

        loop {
            tokio::select! {
                Some(log) = deposit_stream.next() => {
                    if let Err(err) = self.process_deposit(&log, vault_ctx.vault, vault_ctx.receipt_contract).await {
                        warn!("Failed to process Deposit: {err}");
                    }
                }
                Some(log) = withdraw_stream.next() => {
                    if let Err(err) = self.process_withdraw(&log).await {
                        warn!("Failed to process Withdraw: {err}");
                    }
                }
                else => {
                    return Err(ReceiptMonitorError::StreamEnded);
                }
            }
        }
    }

    /// Processes a Deposit event.
    async fn process_deposit(
        &self,
        log: &Log,
        vault: Address,
        receipt_contract: Address,
    ) -> Result<(), ReceiptMonitorError> {
        let event = OffchainAssetReceiptVault::Deposit::decode_log(&log.inner)?;
        let tx_hash =
            log.transaction_hash.ok_or(ReceiptMonitorError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(ReceiptMonitorError::MissingBlockNumber)?;

        // Filter: only process deposits where owner is bot_wallet
        if event.owner != self.bot_wallet {
            return Ok(());
        }

        info!(
            receipt_id = %event.id,
            sender = %event.sender,
            owner = %event.owner,
            shares = %event.shares,
            "Deposit detected"
        );

        self.discover_receipt_if_has_balance(
            ReceiptId::from(event.id),
            tx_hash,
            block_number,
            event.receiptInformation.clone(),
            vault,
            receipt_contract,
        )
        .await
    }

    /// Processes a Withdraw event by reconciling the receipt's on-chain balance.
    ///
    /// When a withdrawal from the bot wallet is detected, queries the current
    /// on-chain balance and issues a ReconcileBalance command to correct any
    /// mismatch with the aggregate state.
    async fn process_withdraw(
        &self,
        log: &Log,
    ) -> Result<(), ReceiptMonitorError> {
        let event =
            OffchainAssetReceiptVault::Withdraw::decode_log(&log.inner)?;

        if event.owner != self.bot_wallet {
            return Ok(());
        }

        let receipt_id = ReceiptId::from(event.id);

        info!(
            receipt_id = %receipt_id,
            sender = %event.sender,
            owner = %event.owner,
            shares = %event.shares,
            "Withdraw detected"
        );

        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        let on_chain_balance = receipt_contract
            .balanceOf(self.bot_wallet, receipt_id.inner())
            .call()
            .await?;

        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id,
                    on_chain_balance: Shares::from(on_chain_balance),
                },
            )
            .await?;

        info!(
            receipt_id = %receipt_id,
            on_chain_balance = %on_chain_balance,
            "Receipt balance reconciled after withdraw"
        );

        Ok(())
    }

    /// Checks on-chain balance and emits DiscoverReceipt if balance > 0.
    async fn discover_receipt_if_has_balance(
        &self,
        receipt_id: ReceiptId,
        tx_hash: B256,
        block_number: u64,
        receipt_information: Bytes,
        vault: Address,
        receipt_contract_addr: Address,
    ) -> Result<(), ReceiptMonitorError> {
        let receipt_contract = Receipt::new(receipt_contract_addr, &self.node);

        let current_balance = receipt_contract
            .balanceOf(self.bot_wallet, receipt_id.inner())
            .call()
            .await?;

        if current_balance.is_zero() {
            info!(
                receipt_id = %receipt_id,
                "Receipt has zero balance, skipping"
            );
            return Ok(());
        }

        let (source, receipt_info) = determine_source(&receipt_information);

        self.cqrs
            .execute(
                &vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id,
                    balance: Shares::from(current_balance),
                    block_number,
                    tx_hash,
                    source: source.clone(),
                    receipt_info,
                },
            )
            .await?;

        info!(
            receipt_id = %receipt_id,
            balance = %current_balance,
            "Receipt discovered via live monitoring"
        );

        if let ReceiptSource::Itn { issuer_request_id } = source {
            self.handler
                .on_itn_receipt_discovered(issuer_request_id, tx_hash)
                .await;
        }

        Ok(())
    }
}

#[async_trait]
impl<Node, ReceiptInventoryStore, Handler> SupervisedTask
    for ReceiptMonitor<Node, ReceiptInventoryStore, Handler>
where
    Node: Provider + Clone + Send + Sync + 'static,
    ReceiptInventoryStore: EventStore<ReceiptInventory> + Send + Sync + 'static,
    ReceiptInventoryStore::AC: Send,
    Handler: ItnReceiptHandler + Clone + 'static,
{
    async fn run(&mut self) -> TaskResult {
        Self::run_monitor(self).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, Bytes, U256, address, b256, uint};
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::types::Log as RpcLog;
    use alloy::sol_types::SolEvent;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::{AggregateContext, CqrsFramework};
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::{LocalEvm, logs_contain_at};

    type TestStore = MemStore<ReceiptInventory>;
    type TestCqrs = CqrsFramework<ReceiptInventory, TestStore>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    struct VaultEventLogParams {
        vault: Address,
        sender: Address,
        owner: Address,
        assets: U256,
        shares: U256,
        id: U256,
        receipt_information: Bytes,
        tx_hash: B256,
        block_number: u64,
    }

    fn create_deposit_log(params: VaultEventLogParams) -> RpcLog {
        let event = OffchainAssetReceiptVault::Deposit {
            sender: params.sender,
            owner: params.owner,
            assets: params.assets,
            shares: params.shares,
            id: params.id,
            receiptInformation: params.receipt_information,
        };

        RpcLog {
            inner: alloy::primitives::Log {
                address: params.vault,
                data: event.encode_log_data(),
            },
            block_hash: Some(b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            block_number: Some(params.block_number),
            block_timestamp: None,
            transaction_hash: Some(params.tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    #[test]
    fn test_deposit_event_decodes_correctly() {
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let sender = address!("0x3333333333333333333333333333333333333333");

        let receipt_info = serde_json::json!({
            "tokenization_request_id": "tok-123",
            "issuer_request_id": "00000000-0000-0000-0000-000000000456",
            "underlying": "AAPL",
            "quantity": "100.0",
            "operation_type": "Mint",
            "timestamp": "2024-01-01T00:00:00Z",
            "notes": null
        });
        let receipt_bytes =
            Bytes::from(serde_json::to_vec(&receipt_info).unwrap());

        let log = create_deposit_log(VaultEventLogParams {
            vault,
            sender,
            owner: bot_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(42_U256),
            receipt_information: receipt_bytes.clone(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 1000,
        });

        let event =
            OffchainAssetReceiptVault::Deposit::decode_log(&log.inner).unwrap();

        assert_eq!(event.sender, sender);
        assert_eq!(event.owner, bot_wallet);
        assert_eq!(event.id, uint!(42_U256));
        assert_eq!(event.receiptInformation, receipt_bytes);
    }

    #[test]
    fn test_deposit_event_filters_by_owner() {
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let sender = address!("0x3333333333333333333333333333333333333333");

        // Create deposit for bot_wallet
        let log_for_bot = create_deposit_log(VaultEventLogParams {
            vault,
            sender,
            owner: bot_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(1_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 1000,
        });

        // Create deposit for other wallet
        let log_for_other = create_deposit_log(VaultEventLogParams {
            vault,
            sender,
            owner: other_wallet,
            assets: uint!(200_U256),
            shares: uint!(200_U256),
            id: uint!(2_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 1001,
        });

        let event_for_bot =
            OffchainAssetReceiptVault::Deposit::decode_log(&log_for_bot.inner)
                .unwrap();
        let event_for_other = OffchainAssetReceiptVault::Deposit::decode_log(
            &log_for_other.inner,
        )
        .unwrap();

        // Verify we can filter by owner
        assert_eq!(event_for_bot.owner, bot_wallet);
        assert_eq!(event_for_other.owner, other_wallet);
        assert!(event_for_bot.owner == bot_wallet);
        assert!(event_for_other.owner != bot_wallet);
    }

    #[tokio::test]
    async fn test_discover_receipt_command_is_emitted() {
        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let receipt_id = ReceiptId::from(uint!(42_U256));
        let balance = Shares::from(uint!(100_000000000000000000_U256));
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );
        let block_number = 1000;

        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id,
                balance,
                block_number,
                tx_hash,
                source: ReceiptSource::External,
                receipt_info: None,
            },
        )
        .await
        .unwrap();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].receipt_id, receipt_id);
        assert_eq!(receipts[0].available_balance, balance);
    }

    #[tokio::test]
    async fn test_discover_receipt_is_idempotent() {
        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let receipt_id = ReceiptId::from(uint!(42_U256));
        let balance = Shares::from(uint!(100_000000000000000000_U256));
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );
        let block_number = 1000;

        for _ in 0..2 {
            cqrs.execute(
                &vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id,
                    balance,
                    block_number,
                    tx_hash,
                    source: ReceiptSource::External,
                    receipt_info: None,
                },
            )
            .await
            .unwrap();
        }

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
    }

    fn create_withdraw_log(params: VaultEventLogParams) -> RpcLog {
        let event = OffchainAssetReceiptVault::Withdraw {
            sender: params.sender,
            receiver: params.sender,
            owner: params.owner,
            assets: params.assets,
            shares: params.shares,
            id: params.id,
            receiptInformation: params.receipt_information,
        };

        RpcLog {
            inner: alloy::primitives::Log {
                address: params.vault,
                data: event.encode_log_data(),
            },
            block_hash: Some(b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            block_number: Some(params.block_number),
            block_timestamp: None,
            transaction_hash: Some(params.tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_withdraw_event_triggers_reconcile_balance() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Seed aggregate with a receipt that has 0 balance on-chain (never minted)
        let receipt_id = ReceiptId::from(uint!(0xff_U256));
        let stale_balance = Shares::from(uint!(100_000000000000000000_U256));

        cqrs.execute(
            &evm.vault_address.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id,
                balance: stale_balance,
                block_number: 1,
                tx_hash: b256!(
                    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                ),
                source: ReceiptSource::External,
                receipt_info: None,
            },
        )
        .await
        .unwrap();

        let config = ReceiptMonitorConfig {
            vault: evm.vault_address,
            receipt_contract,
            bot_wallet,
        };

        let monitor =
            ReceiptMonitor::new(provider, config, cqrs.clone(), NoOpHandler);

        let withdraw_log = create_withdraw_log(VaultEventLogParams {
            vault: evm.vault_address,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: uint!(100_000000000000000000_U256),
            shares: uint!(100_000000000000000000_U256),
            id: uint!(0xff_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            block_number: 100,
        });

        monitor.process_withdraw(&withdraw_log).await.unwrap();

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Withdraw detected", "receipt_id=255"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Receipt balance reconciled after withdraw",
                "on_chain_balance=0"
            ]
        ));

        // On-chain balance is 0 for this receipt (never minted on Anvil),
        // so ReconcileBalance should deplete it
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert!(
            receipts.is_empty(),
            "Receipt should be depleted after withdraw reconciliation"
        );
    }

    #[test]
    fn test_withdraw_event_decodes_correctly() {
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");

        let log = create_withdraw_log(VaultEventLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: uint!(50_U256),
            shares: uint!(50_U256),
            id: uint!(7_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 500,
        });

        let event = OffchainAssetReceiptVault::Withdraw::decode_log(&log.inner)
            .unwrap();

        assert_eq!(event.sender, bot_wallet);
        assert_eq!(event.owner, bot_wallet);
        assert_eq!(event.id, uint!(7_U256));
        assert_eq!(event.shares, uint!(50_U256));
    }

    #[test]
    fn test_withdraw_event_filters_by_owner() {
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");

        let log_for_bot = create_withdraw_log(VaultEventLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(1_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 1000,
        });

        let log_for_other = create_withdraw_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(200_U256),
            shares: uint!(200_U256),
            id: uint!(2_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 1001,
        });

        let event_for_bot =
            OffchainAssetReceiptVault::Withdraw::decode_log(&log_for_bot.inner)
                .unwrap();
        let event_for_other = OffchainAssetReceiptVault::Withdraw::decode_log(
            &log_for_other.inner,
        )
        .unwrap();

        assert_eq!(event_for_bot.owner, bot_wallet);
        assert!(event_for_other.owner != bot_wallet);
    }

    struct NoOpHandler;

    #[async_trait]
    impl ItnReceiptHandler for NoOpHandler {
        async fn on_itn_receipt_discovered(
            &self,
            _issuer_request_id: IssuerMintRequestId,
            _tx_hash: TxHash,
        ) {
        }
    }
}
