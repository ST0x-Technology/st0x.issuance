use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use futures::StreamExt;
use std::sync::Arc;
use tracing::{info, warn};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, Shares, determine_source,
};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};

/// Configuration parameters for the receipt monitor.
#[derive(Clone, Copy)]
pub(crate) struct ReceiptMonitorConfig {
    pub(crate) vault: Address,
    pub(crate) receipt_contract: Address,
    pub(crate) bot_wallet: Address,
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
    SolTypes(#[from] alloy::sol_types::Error),
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
            if let Err(err) = self.monitor_once().await {
                warn!("Receipt monitor error: {err}. Retrying in 5s...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

    /// Monitors for Deposit events once, subscribing and processing them.
    async fn monitor_once(&self) -> Result<(), ReceiptMonitorError> {
        let vault_contract =
            OffchainAssetReceiptVault::new(self.vault, &self.provider);

        // Subscribe to all Deposit events on the vault.
        // We filter by owner == bot_wallet client-side since owner is not indexed.
        let deposit_filter = vault_contract.Deposit_filter().filter;

        info!(
            vault = %self.vault,
            bot_wallet = %self.bot_wallet,
            "Subscribing to Deposit events"
        );

        let deposit_sub = self.provider.subscribe_logs(&deposit_filter).await?;
        let mut deposit_stream = deposit_sub.into_stream();

        info!("Receipt monitor WebSocket subscription active");

        loop {
            tokio::select! {
                Some(log) = deposit_stream.next() => {
                    if let Err(err) = self.process_deposit(&log).await {
                        warn!("Failed to process Deposit: {err}");
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
        )
        .await
    }

    /// Checks on-chain balance and emits DiscoverReceipt if balance > 0.
    async fn discover_receipt_if_has_balance(
        &self,
        receipt_id: ReceiptId,
        tx_hash: B256,
        block_number: u64,
        receipt_information: Bytes,
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

        let (source, receipt_info) =
            determine_source(&receipt_information);

        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id,
                    balance: Shares::from(current_balance),
                    block_number,
                    tx_hash,
                    source,
                    receipt_info,
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
    use alloy::primitives::{B256, Bytes, U256, address, b256, uint};
    use alloy::rpc::types::Log as RpcLog;
    use alloy::sol_types::SolEvent;
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

    struct DepositLogParams {
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

    fn create_deposit_log(params: DepositLogParams) -> RpcLog {
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

        let log = create_deposit_log(DepositLogParams {
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
        let log_for_bot = create_deposit_log(DepositLogParams {
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
        let log_for_other = create_deposit_log(DepositLogParams {
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
}
