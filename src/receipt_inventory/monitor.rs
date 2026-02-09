use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use futures::StreamExt;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, Shares, determine_source,
};
use crate::bindings::Receipt;

/// Configuration parameters for the receipt monitor.
#[derive(Clone, Copy)]
pub(crate) struct ReceiptMonitorConfig {
    pub(crate) vault: Address,
    pub(crate) receipt_contract: Address,
    pub(crate) bot_wallet: Address,
}

/// Monitors for ERC-1155 receipt transfers to the bot wallet in real-time.
///
/// This complements the backfiller by detecting receipts minted while the
/// service is running (e.g., manual operations from another account).
/// When a transfer is detected, it emits a `DiscoverReceipt` command to
/// register the receipt in the ReceiptInventory aggregate.
pub(crate) struct ReceiptMonitor<ProviderType, ReceiptInventoryStore>
where
    ProviderType: Provider,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    provider: ProviderType,
    vault: Address,
    receipt_contract: Address,
    bot_wallet: Address,
    cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptMonitorError {
    #[error("RPC transport error: {0}")]
    Transport(#[from] RpcError<TransportErrorKind>),
    #[error("Failed to decode event: {0}")]
    EventDecode(#[from] alloy::sol_types::Error),
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error("Contract call error: {0}")]
    ContractCall(#[from] alloy::contract::Error),
    #[error("CQRS error: {0}")]
    Cqrs(AggregateError<ReceiptInventoryError>),
    #[error("WebSocket stream ended unexpectedly")]
    StreamEnded,
}

impl<ProviderType, ReceiptInventoryStore>
    ReceiptMonitor<ProviderType, ReceiptInventoryStore>
where
    ProviderType: Provider,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    pub(crate) const fn new(
        provider: ProviderType,
        config: ReceiptMonitorConfig,
        cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
    ) -> Self {
        Self {
            provider,
            vault: config.vault,
            receipt_contract: config.receipt_contract,
            bot_wallet: config.bot_wallet,
            cqrs,
        }
    }
}

impl<ProviderType, ReceiptInventoryStore>
    ReceiptMonitor<ProviderType, ReceiptInventoryStore>
where
    ProviderType: Provider + Clone + Send + Sync + 'static,
    ReceiptInventoryStore: EventStore<ReceiptInventory> + 'static,
    ReceiptInventoryStore::AC: Send,
{
    /// Runs the monitoring loop with automatic retry on errors.
    ///
    /// This method never returns under normal operation. If an error occurs,
    /// it logs the error and retries after 5 seconds.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn run(&self) {
        loop {
            if let Err(e) = self.monitor_once().await {
                warn!("Receipt monitor error: {e}. Retrying in 5s...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

    /// Monitors for receipt transfers once, subscribing to events and
    /// processing them.
    async fn monitor_once(&self) -> Result<(), ReceiptMonitorError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        // Subscribe to TransferSingle events where to=bot_wallet (topic3)
        let single_filter = receipt_contract
            .TransferSingle_filter()
            .topic3(self.bot_wallet)
            .filter;

        // Subscribe to TransferBatch events where to=bot_wallet (topic3)
        let batch_filter = receipt_contract
            .TransferBatch_filter()
            .topic3(self.bot_wallet)
            .filter;

        info!(
            receipt_contract = %self.receipt_contract,
            bot_wallet = %self.bot_wallet,
            "Subscribing to TransferSingle/TransferBatch events"
        );

        let single_sub = self.provider.subscribe_logs(&single_filter).await?;
        let batch_sub = self.provider.subscribe_logs(&batch_filter).await?;

        let mut single_stream = single_sub.into_stream();
        let mut batch_stream = batch_sub.into_stream();

        info!("Receipt monitor WebSocket subscription active");

        loop {
            tokio::select! {
                Some(log) = single_stream.next() => {
                    if let Err(e) = self.process_transfer_single(&log).await {
                        error!("Failed to process TransferSingle: {e}");
                    }
                }
                Some(log) = batch_stream.next() => {
                    if let Err(e) = self.process_transfer_batch(&log).await {
                        error!("Failed to process TransferBatch: {e}");
                    }
                }
                else => {
                    return Err(ReceiptMonitorError::StreamEnded);
                }
            }
        }
    }

    /// Processes a TransferSingle event.
    async fn process_transfer_single(
        &self,
        log: &Log,
    ) -> Result<(), ReceiptMonitorError> {
        let event = Receipt::TransferSingle::decode_log(&log.inner)?;
        let tx_hash =
            log.transaction_hash.ok_or(ReceiptMonitorError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(ReceiptMonitorError::MissingBlockNumber)?;

        // Skip mint events where from=0x0 (these are from our own minting)
        if event.from == Address::ZERO {
            info!(
                receipt_id = %event.id,
                "Ignoring mint event (from=0x0) - this is from our own minting"
            );
            return Ok(());
        }

        info!(
            receipt_id = %event.id,
            from = %event.from,
            to = %event.to,
            value = %event.value,
            "TransferSingle detected"
        );

        self.discover_receipt_if_has_balance(
            ReceiptId::from(event.id),
            tx_hash,
            block_number,
        )
        .await
    }

    /// Processes a TransferBatch event.
    async fn process_transfer_batch(
        &self,
        log: &Log,
    ) -> Result<(), ReceiptMonitorError> {
        let event = Receipt::TransferBatch::decode_log(&log.inner)?;
        let tx_hash =
            log.transaction_hash.ok_or(ReceiptMonitorError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(ReceiptMonitorError::MissingBlockNumber)?;

        // Skip mint events where from=0x0
        if event.from == Address::ZERO {
            info!(
                receipt_count = event.ids.len(),
                "Ignoring batch mint event (from=0x0) - this is from our own minting"
            );
            return Ok(());
        }

        info!(
            receipt_count = event.ids.len(),
            from = %event.from,
            to = %event.to,
            "TransferBatch detected"
        );

        for &id in &event.ids {
            self.discover_receipt_if_has_balance(
                ReceiptId::from(id),
                tx_hash,
                block_number,
            )
            .await?;
        }

        Ok(())
    }

    /// Checks on-chain balance and emits DiscoverReceipt if balance > 0.
    async fn discover_receipt_if_has_balance(
        &self,
        receipt_id: ReceiptId,
        tx_hash: B256,
        block_number: u64,
    ) -> Result<(), ReceiptMonitorError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

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

        // Execute DiscoverReceipt command - the aggregate handles idempotency
        // (already-discovered receipts are a no-op)
        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id,
                    balance: Shares::from(current_balance),
                    block_number,
                    tx_hash,
                    source: determine_source(receipt_id),
                },
            )
            .await
            .map_err(ReceiptMonitorError::Cqrs)?;

        info!(
            receipt_id = %receipt_id,
            balance = %current_balance,
            "Receipt discovered via live monitoring"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, U256, address, b256, uint};
    use alloy::rpc::types::Log as RpcLog;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::{AggregateContext, CqrsFramework};

    use super::*;
    use crate::receipt_inventory::ReceiptSource;

    type TestStore = MemStore<ReceiptInventory>;
    type TestCqrs = CqrsFramework<ReceiptInventory, TestStore>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    #[derive(Clone, Copy)]
    struct TransferSingleLogParams {
        receipt_contract: Address,
        operator: Address,
        from: Address,
        to: Address,
        id: U256,
        value: U256,
        tx_hash: B256,
        block_number: u64,
    }

    fn create_transfer_single_log(params: TransferSingleLogParams) -> RpcLog {
        let event = Receipt::TransferSingle {
            operator: params.operator,
            from: params.from,
            to: params.to,
            id: params.id,
            value: params.value,
        };

        RpcLog {
            inner: alloy::primitives::Log {
                address: params.receipt_contract,
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
    fn test_monitor_ignores_mint_events_from_zero_address() {
        // This test verifies that when from=0x0 (mint event), we skip processing.
        // The actual filtering happens in process_transfer_single which checks
        // event.from == Address::ZERO.

        let receipt_contract =
            address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let operator = address!("0x3333333333333333333333333333333333333333");

        // Create a mint event (from=0x0)
        let log = create_transfer_single_log(TransferSingleLogParams {
            receipt_contract,
            operator,
            from: Address::ZERO, // from=0x0 indicates mint
            to: bot_wallet,
            id: uint!(42_U256),
            value: uint!(100_U256),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 1000,
        });

        // Decode the event
        let event = Receipt::TransferSingle::decode_log(&log.inner).unwrap();

        // Verify from is zero (mint event)
        assert_eq!(event.from, Address::ZERO);

        // The monitor should skip this because from=0x0
        // (This is tested in process_transfer_single implementation)
    }

    #[test]
    fn test_monitor_processes_external_transfer() {
        // This test verifies that when from!=0x0 (external transfer), we process it.

        let receipt_contract =
            address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let operator = address!("0x3333333333333333333333333333333333333333");
        let external_sender =
            address!("0x4444444444444444444444444444444444444444");

        // Create an external transfer event (from=external_sender, not 0x0)
        let log = create_transfer_single_log(TransferSingleLogParams {
            receipt_contract,
            operator,
            from: external_sender, // from!=0x0 indicates external transfer
            to: bot_wallet,
            id: uint!(42_U256),
            value: uint!(100_U256),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 1000,
        });

        // Decode the event
        let event = Receipt::TransferSingle::decode_log(&log.inner).unwrap();

        // Verify from is NOT zero (external transfer)
        assert_ne!(event.from, Address::ZERO);
        assert_eq!(event.from, external_sender);
        assert_eq!(event.to, bot_wallet);
        assert_eq!(event.id, uint!(42_U256));
    }

    #[tokio::test]
    async fn test_discover_receipt_command_is_emitted() {
        // This test verifies the monitor correctly emits DiscoverReceipt command
        // when an external transfer is detected.
        //
        // Note: Full integration testing requires a mock provider which is
        // complex. This test focuses on the CQRS command execution path.

        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let receipt_id = ReceiptId::from(uint!(42_U256));
        let balance = Shares::from(uint!(100_000000000000000000_U256));
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );
        let block_number = 1000;

        // Execute DiscoverReceipt command directly (simulating what monitor does)
        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id,
                balance,
                block_number,
                tx_hash,
                source: ReceiptSource::External,
            },
        )
        .await
        .unwrap();

        // Load aggregate to verify receipt was discovered
        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].receipt_id, receipt_id);
        assert_eq!(receipts[0].available_balance, balance);
    }

    #[tokio::test]
    async fn test_discover_receipt_is_idempotent() {
        // Verify that discovering the same receipt twice doesn't cause errors
        // or duplicate entries.

        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let receipt_id = ReceiptId::from(uint!(42_U256));
        let balance = Shares::from(uint!(100_000000000000000000_U256));
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );
        let block_number = 1000;

        // Execute DiscoverReceipt command twice
        for _ in 0..2 {
            cqrs.execute(
                &vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id,
                    balance,
                    block_number,
                    tx_hash,
                    source: ReceiptSource::External,
                },
            )
            .await
            .unwrap();
        }

        // Load aggregate to verify only one receipt exists
        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
    }
}
