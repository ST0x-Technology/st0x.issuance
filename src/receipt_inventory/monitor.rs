use alloy::primitives::{Address, B256, Bytes, TxHash};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use futures::StreamExt;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, ReceiptSource, Shares, determine_source,
};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::mint::IssuerMintRequestId;

/// Configuration parameters for the receipt monitor.
#[derive(Clone, Copy)]
pub(crate) struct ReceiptMonitorConfig {
    pub(crate) vault: Address,
    pub(crate) receipt_contract: Address,
    pub(crate) bot_wallet: Address,
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
pub(crate) struct ReceiptMonitor<ProviderType, ReceiptInventoryStore, Handler>
where
    ProviderType: Provider,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
    Handler: ItnReceiptHandler,
{
    provider: ProviderType,
    vault: Address,
    receipt_contract: Address,
    bot_wallet: Address,
    cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
    handler: Handler,
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
    #[error("WebSocket stream ended unexpectedly")]
    StreamEnded,
}

impl<ProviderType, ReceiptInventoryStore, Handler>
    ReceiptMonitor<ProviderType, ReceiptInventoryStore, Handler>
where
    ProviderType: Provider,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
    Handler: ItnReceiptHandler,
{
    pub(crate) const fn new(
        provider: ProviderType,
        config: ReceiptMonitorConfig,
        cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
        handler: Handler,
    ) -> Self {
        Self {
            provider,
            vault: config.vault,
            receipt_contract: config.receipt_contract,
            bot_wallet: config.bot_wallet,
            cqrs,
            handler,
        }
    }
}

impl<ProviderType, ReceiptInventoryStore, Handler>
    ReceiptMonitor<ProviderType, ReceiptInventoryStore, Handler>
where
    ProviderType: Provider + Clone + Send + Sync + 'static,
    ReceiptInventoryStore: EventStore<ReceiptInventory> + 'static,
    ReceiptInventoryStore::AC: Send,
    Handler: ItnReceiptHandler,
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

    /// Monitors for Deposit, Withdraw, TransferSingle, and TransferBatch events.
    async fn monitor_once(&self) -> Result<(), ReceiptMonitorError> {
        let vault_contract =
            OffchainAssetReceiptVault::new(self.vault, &self.provider);

        // Subscribe to Deposit and Withdraw events on the vault contract.
        // We filter by owner == bot_wallet client-side since owner is not indexed.
        let deposit_filter = vault_contract.Deposit_filter().filter;
        let withdraw_filter = vault_contract.Withdraw_filter().filter;

        // Subscribe to ERC-1155 TransferSingle and TransferBatch events on
        // the Receipt contract with indexed topic filtering for bot_wallet.
        // Separate inbound (to == bot_wallet) and outbound (from == bot_wallet)
        // subscriptions reduce WebSocket traffic by filtering at the node level.
        let transfer_single_inbound = transfer_single_filter(
            self.receipt_contract,
            self.bot_wallet,
            TransferDirection::Inbound,
        );
        let transfer_single_outbound = transfer_single_filter(
            self.receipt_contract,
            self.bot_wallet,
            TransferDirection::Outbound,
        );
        let transfer_batch_inbound = transfer_batch_filter(
            self.receipt_contract,
            self.bot_wallet,
            TransferDirection::Inbound,
        );
        let transfer_batch_outbound = transfer_batch_filter(
            self.receipt_contract,
            self.bot_wallet,
            TransferDirection::Outbound,
        );

        info!(
            vault = %self.vault,
            receipt_contract = %self.receipt_contract,
            bot_wallet = %self.bot_wallet,
            "Subscribing to Deposit, Withdraw, TransferSingle, and TransferBatch events"
        );

        let deposit_sub = self.provider.subscribe_logs(&deposit_filter).await?;
        let withdraw_sub =
            self.provider.subscribe_logs(&withdraw_filter).await?;
        let transfer_single_inbound_sub =
            self.provider.subscribe_logs(&transfer_single_inbound).await?;
        let transfer_single_outbound_sub =
            self.provider.subscribe_logs(&transfer_single_outbound).await?;
        let transfer_batch_inbound_sub =
            self.provider.subscribe_logs(&transfer_batch_inbound).await?;
        let transfer_batch_outbound_sub =
            self.provider.subscribe_logs(&transfer_batch_outbound).await?;

        let mut deposit_stream = deposit_sub.into_stream();
        let mut withdraw_stream = withdraw_sub.into_stream();
        let mut transfer_single_inbound_stream =
            transfer_single_inbound_sub.into_stream();
        let mut transfer_single_outbound_stream =
            transfer_single_outbound_sub.into_stream();
        let mut transfer_batch_inbound_stream =
            transfer_batch_inbound_sub.into_stream();
        let mut transfer_batch_outbound_stream =
            transfer_batch_outbound_sub.into_stream();

        info!("Receipt monitor WebSocket subscription active");

        loop {
            tokio::select! {
                Some(log) = deposit_stream.next() => {
                    if log.removed {
                        debug!("Skipping removed Deposit log (reorg)");
                        continue;
                    }
                    self.process_live_log(&log, "Deposit", |log| {
                        Box::pin(self.process_deposit(log))
                    }).await;
                }
                Some(log) = withdraw_stream.next() => {
                    if log.removed {
                        debug!("Skipping removed Withdraw log (reorg)");
                        continue;
                    }
                    self.process_live_log(&log, "Withdraw", |log| {
                        Box::pin(self.process_withdraw(log))
                    }).await;
                }
                Some(log) = transfer_single_inbound_stream.next() => {
                    if log.removed {
                        debug!("Skipping removed TransferSingle log (reorg)");
                        continue;
                    }
                    self.process_live_log(&log, "TransferSingle", |log| {
                        Box::pin(self.process_transfer_single(log))
                    }).await;
                }
                Some(log) = transfer_single_outbound_stream.next() => {
                    if log.removed {
                        debug!("Skipping removed TransferSingle log (reorg)");
                        continue;
                    }
                    self.process_live_log(&log, "TransferSingle", |log| {
                        Box::pin(self.process_transfer_single(log))
                    }).await;
                }
                Some(log) = transfer_batch_inbound_stream.next() => {
                    if log.removed {
                        debug!("Skipping removed TransferBatch log (reorg)");
                        continue;
                    }
                    self.process_live_log(&log, "TransferBatch", |log| {
                        Box::pin(self.process_transfer_batch(log))
                    }).await;
                }
                Some(log) = transfer_batch_outbound_stream.next() => {
                    if log.removed {
                        debug!("Skipping removed TransferBatch log (reorg)");
                        continue;
                    }
                    self.process_live_log(&log, "TransferBatch", |log| {
                        Box::pin(self.process_transfer_batch(log))
                    }).await;
                }
                else => {
                    return Err(ReceiptMonitorError::StreamEnded);
                }
            }
        }
    }

    async fn process_live_log<'a, FutureType>(
        &self,
        log: &'a Log,
        event_name: &'static str,
        process: impl FnOnce(&'a Log) -> FutureType,
    ) where
        FutureType:
            std::future::Future<Output = Result<(), ReceiptMonitorError>>,
    {
        let Some(block_number) = log.block_number else {
            // Without a block number we cannot prove where this live log fits
            // relative to the durable checkpoint. Skip live processing and let
            // periodic backfill rescan the range in block order.
            warn!(
                event_name,
                "Live receipt log missing block number; periodic backfill will \
                 rescan the range in block order"
            );
            return;
        };

        if let Err(err) = process(log).await {
            warn!(
                event_name,
                block_number,
                error = %err,
                "Failed to process live receipt log; periodic backfill will \
                 rescan from the durable checkpoint"
            );
        } else {
            debug!(
                event_name,
                block_number,
                "Processed live receipt log; periodic backfill owns durable \
                 checkpoint advancement"
            );
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

    /// Processes a TransferSingle event (ERC-1155 single token transfer).
    ///
    /// Handles both inbound transfers (to == bot_wallet, discovers receipt)
    /// and outbound transfers (from == bot_wallet, reconciles balance).
    /// Mint/burn transfers (from/to == address(0)) are skipped since those
    /// are already handled by Deposit/Withdraw events.
    async fn process_transfer_single(
        &self,
        log: &Log,
    ) -> Result<(), ReceiptMonitorError> {
        let event = Receipt::TransferSingle::decode_log(&log.inner)?;
        let tx_hash =
            log.transaction_hash.ok_or(ReceiptMonitorError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(ReceiptMonitorError::MissingBlockNumber)?;

        // Skip mint/burn transfers — already covered by Deposit/Withdraw
        if event.from.is_zero() || event.to.is_zero() {
            return Ok(());
        }

        let receipt_id = ReceiptId::from(event.id);

        if event.to == self.bot_wallet {
            debug!(
                receipt_id = %receipt_id,
                from = %event.from,
                value = %event.value,
                "Inbound TransferSingle detected"
            );

            self.discover_receipt_if_has_balance(
                receipt_id,
                tx_hash,
                block_number,
                Bytes::new(),
            )
            .await?;
        } else if event.from == self.bot_wallet {
            debug!(
                receipt_id = %receipt_id,
                to = %event.to,
                value = %event.value,
                "Outbound TransferSingle detected"
            );

            self.reconcile_receipt(receipt_id).await?;
        }

        Ok(())
    }

    /// Processes a TransferBatch event (ERC-1155 batch token transfer).
    ///
    /// Each (id, value) pair in the batch is handled identically to a
    /// TransferSingle event.
    async fn process_transfer_batch(
        &self,
        log: &Log,
    ) -> Result<(), ReceiptMonitorError> {
        let event = Receipt::TransferBatch::decode_log(&log.inner)?;
        let block_number =
            log.block_number.ok_or(ReceiptMonitorError::MissingBlockNumber)?;

        // Skip mint/burn transfers — already covered by Deposit/Withdraw
        if event.from.is_zero() || event.to.is_zero() {
            return Ok(());
        }

        let tx_hash =
            log.transaction_hash.ok_or(ReceiptMonitorError::MissingTxHash)?;

        if event.to == self.bot_wallet {
            for receipt_id_raw in &event.ids {
                let receipt_id = ReceiptId::from(*receipt_id_raw);

                debug!(
                    receipt_id = %receipt_id,
                    from = %event.from,
                    "Inbound TransferBatch item detected"
                );

                self.discover_receipt_if_has_balance(
                    receipt_id,
                    tx_hash,
                    block_number,
                    Bytes::new(),
                )
                .await?;
            }
        } else if event.from == self.bot_wallet {
            for receipt_id_raw in &event.ids {
                let receipt_id = ReceiptId::from(*receipt_id_raw);

                debug!(
                    receipt_id = %receipt_id,
                    to = %event.to,
                    "Outbound TransferBatch item detected"
                );

                self.reconcile_receipt(receipt_id).await?;
            }
        }

        Ok(())
    }

    /// Queries the on-chain balance of a receipt and reconciles the aggregate.
    async fn reconcile_receipt(
        &self,
        receipt_id: ReceiptId,
    ) -> Result<(), ReceiptMonitorError> {
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

        debug!(
            receipt_id = %receipt_id,
            on_chain_balance = %on_chain_balance,
            "Receipt balance reconciled after transfer"
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

        let (source, receipt_info) = determine_source(&receipt_information);

        // Always preserve the original on-chain bytes so redeem() can pass
        // them back exactly, regardless of whether we could parse them.
        let receipt_info_bytes = if receipt_information.is_empty() {
            None
        } else {
            Some(receipt_information.clone())
        };

        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id,
                    balance: Shares::from(current_balance),
                    block_number,
                    tx_hash,
                    source: source.clone(),
                    receipt_info: receipt_info.map(Box::new),
                    receipt_info_bytes,
                },
            )
            .await?;

        // For already-known receipts, DiscoverReceipt is a no-op. Reconcile
        // ensures the aggregate reflects the current on-chain balance (e.g.,
        // after an inbound transfer increases the balance).
        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id,
                    on_chain_balance: Shares::from(current_balance),
                },
            )
            .await?;

        info!(
            receipt_id = %receipt_id,
            balance = %current_balance,
            "Receipt discovered or reconciled via live monitoring"
        );

        if let ReceiptSource::Itn { issuer_request_id } = source {
            self.handler
                .on_itn_receipt_discovered(issuer_request_id, tx_hash)
                .await;
        }

        Ok(())
    }
}

/// Direction of token transfer relative to the bot wallet.
///
/// ERC-1155 TransferSingle/TransferBatch events have indexed `from` (topic2)
/// and `to` (topic3) fields. Using directional filters reduces RPC payload
/// by filtering at the node level rather than client-side.
#[derive(Clone, Copy)]
pub(crate) enum TransferDirection {
    /// Tokens received: `to == bot_wallet` (topic3)
    Inbound,
    /// Tokens sent: `from == bot_wallet` (topic2)
    Outbound,
}

/// Builds a log filter for ERC-1155 TransferSingle events on the given
/// contract, filtered by indexed topic for the specified direction.
pub(crate) fn transfer_single_filter(
    receipt_contract: Address,
    bot_wallet: Address,
    direction: TransferDirection,
) -> Filter {
    let base = Filter::new()
        .address(receipt_contract)
        .event_signature(Receipt::TransferSingle::SIGNATURE_HASH);

    match direction {
        TransferDirection::Inbound => base.topic3(bot_wallet.into_word()),
        TransferDirection::Outbound => base.topic2(bot_wallet.into_word()),
    }
}

/// Builds a log filter for ERC-1155 TransferBatch events on the given
/// contract, filtered by indexed topic for the specified direction.
pub(crate) fn transfer_batch_filter(
    receipt_contract: Address,
    bot_wallet: Address,
    direction: TransferDirection,
) -> Filter {
    let base = Filter::new()
        .address(receipt_contract)
        .event_signature(Receipt::TransferBatch::SIGNATURE_HASH);

    match direction {
        TransferDirection::Inbound => base.topic3(bot_wallet.into_word()),
        TransferDirection::Outbound => base.topic2(bot_wallet.into_word()),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, Bytes, U256, address, b256, uint};
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::types::{Filter, Log as RpcLog};
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

    /// Sets up Anvil vault roles and certifies for deposits. Call once
    /// per LocalEvm instance before calling `mint_directly`.
    async fn setup_vault_for_deposits(evm: &LocalEvm) {
        evm.grant_deposit_role(evm.wallet_address).await.unwrap();
        evm.grant_certify_role(evm.wallet_address).await.unwrap();
        evm.certify_vault(U256::MAX).await.unwrap();
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

    #[traced_test]
    #[tokio::test]
    async fn test_live_deposit_does_not_advance_receipt_checkpoint() {
        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let receipt_contract =
            address!("0x5555555555555555555555555555555555555555");

        let provider = ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new());

        let monitor = ReceiptMonitor::new(
            provider,
            ReceiptMonitorConfig { vault, receipt_contract, bot_wallet },
            cqrs.clone(),
            NoOpHandler,
        );

        let log = create_deposit_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(42_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 123,
        });

        monitor
            .process_live_log(&log, "Deposit", |log| {
                Box::pin(monitor.process_deposit(log))
            })
            .await;

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        assert_eq!(context.aggregate().last_backfilled_block(), None);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Processed live receipt log",
                "periodic backfill owns durable checkpoint advancement",
                "block_number=123"
            ]
        ));
        assert!(
            context.aggregate().receipts_with_balance().is_empty(),
            "Non-bot deposit should not register a receipt"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_failed_live_log_does_not_affect_later_live_processing() {
        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let receipt_contract =
            address!("0x5555555555555555555555555555555555555555");

        let provider = ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new());

        let monitor = ReceiptMonitor::new(
            provider,
            ReceiptMonitorConfig { vault, receipt_contract, bot_wallet },
            cqrs.clone(),
            NoOpHandler,
        );

        let mut failed_log = create_deposit_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(42_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 123,
        });
        failed_log.transaction_hash = None;

        monitor
            .process_live_log(&failed_log, "Deposit", |log| {
                Box::pin(monitor.process_deposit(log))
            })
            .await;

        let later_log = create_deposit_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(43_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 124,
        });

        monitor
            .process_live_log(&later_log, "Deposit", |log| {
                Box::pin(monitor.process_deposit(log))
            })
            .await;

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        assert_eq!(context.aggregate().last_backfilled_block(), None);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Failed to process live receipt log",
                "periodic backfill will rescan from the durable checkpoint",
                "error=Missing transaction hash in log"
            ]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Processed live receipt log",
                "periodic backfill owns durable checkpoint advancement",
                "block_number=124"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_periodic_backfill_remains_checkpoint_authority() {
        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let receipt_contract =
            address!("0x5555555555555555555555555555555555555555");

        let provider = ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new());

        let monitor = ReceiptMonitor::new(
            provider,
            ReceiptMonitorConfig { vault, receipt_contract, bot_wallet },
            cqrs.clone(),
            NoOpHandler,
        );

        let mut failed_log = create_deposit_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(42_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 123,
        });
        failed_log.transaction_hash = None;

        monitor
            .process_live_log(&failed_log, "Deposit", |log| {
                Box::pin(monitor.process_deposit(log))
            })
            .await;

        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::AdvanceBackfillCheckpoint {
                block_number: 123,
            },
        )
        .await
        .unwrap();

        let later_log = create_deposit_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(43_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 124,
        });

        monitor
            .process_live_log(&later_log, "Deposit", |log| {
                Box::pin(monitor.process_deposit(log))
            })
            .await;

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        assert_eq!(context.aggregate().last_backfilled_block(), Some(123));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Processed live receipt log",
                "periodic backfill owns durable checkpoint advancement",
                "block_number=124"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unpositioned_live_log_does_not_clear_existing_checkpoint() {
        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let receipt_contract =
            address!("0x5555555555555555555555555555555555555555");

        let provider = ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new());

        let monitor = ReceiptMonitor::new(
            provider,
            ReceiptMonitorConfig { vault, receipt_contract, bot_wallet },
            cqrs.clone(),
            NoOpHandler,
        );

        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::AdvanceBackfillCheckpoint {
                block_number: 123,
            },
        )
        .await
        .unwrap();

        let mut unpositioned_log = create_deposit_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(42_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 123,
        });
        unpositioned_log.block_number = None;

        monitor
            .process_live_log(&unpositioned_log, "Deposit", |log| {
                Box::pin(monitor.process_deposit(log))
            })
            .await;

        let later_log = create_deposit_log(VaultEventLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: uint!(100_U256),
            shares: uint!(100_U256),
            id: uint!(43_U256),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 124,
        });

        monitor
            .process_live_log(&later_log, "Deposit", |log| {
                Box::pin(monitor.process_deposit(log))
            })
            .await;

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        assert_eq!(context.aggregate().last_backfilled_block(), Some(123));
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Live receipt log missing block number",
                "periodic backfill will rescan the range in block order"
            ]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Processed live receipt log",
                "periodic backfill owns durable checkpoint advancement",
                "block_number=124"
            ]
        ));
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
                receipt_info_bytes: None,
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
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();
        }

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].receipt_id, receipt_id,
            "Should still be the same receipt after duplicate discover"
        );
        assert_eq!(
            receipts[0].available_balance, balance,
            "Balance should be unchanged after duplicate discover"
        );
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
                receipt_info_bytes: None,
            },
        )
        .await
        .unwrap();

        // Verify the receipt exists with stale balance BEFORE processing
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts_before = context.aggregate().receipts_with_balance();
        assert_eq!(
            receipts_before.len(),
            1,
            "Should have one receipt before withdraw"
        );
        assert_eq!(receipts_before[0].receipt_id, receipt_id);
        assert_eq!(
            receipts_before[0].available_balance, stale_balance,
            "Balance should be 100e18 before withdraw"
        );

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

    struct TransferBatchLogParams {
        receipt_contract: Address,
        operator: Address,
        from: Address,
        to: Address,
        ids: Vec<U256>,
        values: Vec<U256>,
        tx_hash: B256,
        block_number: u64,
    }

    fn create_transfer_batch_log(params: TransferBatchLogParams) -> RpcLog {
        let event = Receipt::TransferBatch {
            operator: params.operator,
            from: params.from,
            to: params.to,
            ids: params.ids,
            values: params.values,
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

    #[traced_test]
    #[tokio::test]
    async fn test_inbound_transfer_single_discovers_receipt() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");

        // Do a real deposit on Anvil so balanceOf returns a non-zero value
        let deposit_amount = uint!(75_000000000000000000_U256);
        setup_vault_for_deposits(&evm).await;
        let (receipt_id_raw, minted_shares) =
            evm.mint_directly(deposit_amount, bot_wallet).await.unwrap();

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        let config = ReceiptMonitorConfig {
            vault: evm.vault_address,
            receipt_contract,
            bot_wallet,
        };

        let monitor =
            ReceiptMonitor::new(provider, config, cqrs.clone(), NoOpHandler);

        // Inventory should be empty before processing the transfer
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        assert!(
            context.aggregate().receipts_with_balance().is_empty(),
            "Inventory should be empty before inbound transfer"
        );

        // Simulate an inbound transfer from other_wallet to bot_wallet
        let transfer_log = create_transfer_single_log(
            TransferSingleLogParams {
                receipt_contract,
                operator: other_wallet,
                from: other_wallet,
                to: bot_wallet,
                id: receipt_id_raw,
                value: minted_shares,
                tx_hash: b256!(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                block_number: 100,
            },
        );

        monitor.process_transfer_single(&transfer_log).await.unwrap();

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Inbound TransferSingle detected"]
        ));

        // Verify: receipt was discovered with the exact on-chain balance
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 1, "Should discover exactly one receipt");
        assert_eq!(
            receipts[0].receipt_id,
            ReceiptId::from(receipt_id_raw),
            "Discovered receipt ID should match the deposited receipt"
        );
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(minted_shares),
            "Discovered balance should match the on-chain balance"
        );
        assert!(
            receipts[0].receipt_info.is_none(),
            "Transfer-discovered receipts should have no parsed receipt info"
        );
        assert!(
            receipts[0].receipt_info_bytes.is_none(),
            "Transfer-discovered receipts should have no raw receipt info bytes"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_outbound_transfer_single_reconciles_balance() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Seed aggregate with a receipt that has stale balance (never
        // actually minted on-chain, so balanceOf will return 0)
        let receipt_id = ReceiptId::from(uint!(0xab_U256));
        let stale_balance = Shares::from(uint!(200_000000000000000000_U256));

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
                receipt_info_bytes: None,
            },
        )
        .await
        .unwrap();

        // Verify the aggregate has the receipt with stale balance BEFORE processing
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts_before = context.aggregate().receipts_with_balance();
        assert_eq!(
            receipts_before.len(),
            1,
            "Should have one receipt before outbound transfer"
        );
        assert_eq!(receipts_before[0].receipt_id, receipt_id);
        assert_eq!(
            receipts_before[0].available_balance, stale_balance,
            "Balance should be 200e18 before outbound transfer"
        );

        let config = ReceiptMonitorConfig {
            vault: evm.vault_address,
            receipt_contract,
            bot_wallet,
        };

        let monitor =
            ReceiptMonitor::new(provider, config, cqrs.clone(), NoOpHandler);

        let transfer_log = create_transfer_single_log(
            TransferSingleLogParams {
                receipt_contract,
                operator: bot_wallet,
                from: bot_wallet,
                to: other_wallet,
                id: uint!(0xab_U256),
                value: uint!(200_000000000000000000_U256),
                tx_hash: b256!(
                    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                ),
                block_number: 200,
            },
        );

        monitor.process_transfer_single(&transfer_log).await.unwrap();

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Outbound TransferSingle detected", "receipt_id=171"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Receipt balance reconciled after transfer",
                "on_chain_balance=0"
            ]
        ));

        // On-chain balance is 0 (never minted on Anvil), so reconciliation
        // should deplete the receipt from 200e18 → 0
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts_after = context.aggregate().receipts_with_balance();
        assert!(
            receipts_after.is_empty(),
            "Receipt should be depleted (balance 200e18 → 0) after outbound transfer reconciliation"
        );
    }

    /// Verifies the outbound-then-inbound scenario: a receipt that was
    /// reconciled downward after an outbound transfer gets its balance
    /// restored when an inbound transfer arrives for the same receipt ID.
    #[tokio::test]
    async fn test_inbound_transfer_reconciles_balance_upward_for_known_receipt()
    {
        let (cqrs, store) = setup_test_cqrs();
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let receipt_id = ReceiptId::from(uint!(42_U256));

        // 1. Discover receipt with balance 100
        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id,
                balance: Shares::from(uint!(100_U256)),
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

        // 2. Reconcile down to 50 (simulating outbound transfer)
        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id,
                on_chain_balance: Shares::from(uint!(50_U256)),
            },
        )
        .await
        .unwrap();

        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        assert_eq!(
            context.aggregate().receipts_with_balance()[0].available_balance,
            Shares::from(uint!(50_U256)),
        );

        // 3. Simulate inbound transfer: DiscoverReceipt is no-op (known),
        //    then ReconcileBalance updates balance upward to 80
        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id,
                balance: Shares::from(uint!(80_U256)),
                block_number: 2,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                source: ReceiptSource::External,
                receipt_info: None,
                receipt_info_bytes: None,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id,
                on_chain_balance: Shares::from(uint!(80_U256)),
            },
        )
        .await
        .unwrap();

        // 4. Verify balance is now 80 and receipt ID is unchanged
        let context = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].receipt_id, receipt_id);
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(uint!(80_U256)),
            "Balance should be reconciled upward after inbound transfer"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_transfer_single_skips_mint_transfers() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Seed a pre-existing receipt to verify it's unmodified after processing
        let existing_receipt_id = ReceiptId::from(uint!(99_U256));
        let existing_balance = Shares::from(uint!(500_U256));
        cqrs.execute(
            &evm.vault_address.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: existing_receipt_id,
                balance: existing_balance,
                block_number: 1,
                tx_hash: b256!(
                    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                ),
                source: ReceiptSource::External,
                receipt_info: None,
                receipt_info_bytes: None,
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

        // Mint transfer: from == address(0)
        let mint_log = create_transfer_single_log(TransferSingleLogParams {
            receipt_contract,
            operator: bot_wallet,
            from: Address::ZERO,
            to: bot_wallet,
            id: uint!(1_U256),
            value: uint!(100_U256),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 100,
        });

        // Route through monitor — should be skipped (no state changes)
        monitor.process_transfer_single(&mint_log).await.unwrap();

        // Verify: only the pre-existing receipt remains, unchanged
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(
            receipts.len(),
            1,
            "Mint transfer (from == 0) should not add new receipts"
        );
        assert_eq!(receipts[0].receipt_id, existing_receipt_id);
        assert_eq!(
            receipts[0].available_balance, existing_balance,
            "Pre-existing receipt balance should be unchanged"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_transfer_single_skips_burn_transfers() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Seed a pre-existing receipt to verify it's unmodified after processing
        let existing_receipt_id = ReceiptId::from(uint!(99_U256));
        let existing_balance = Shares::from(uint!(500_U256));
        cqrs.execute(
            &evm.vault_address.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: existing_receipt_id,
                balance: existing_balance,
                block_number: 1,
                tx_hash: b256!(
                    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                ),
                source: ReceiptSource::External,
                receipt_info: None,
                receipt_info_bytes: None,
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

        // Burn transfer: to == address(0)
        let burn_log = create_transfer_single_log(TransferSingleLogParams {
            receipt_contract,
            operator: bot_wallet,
            from: bot_wallet,
            to: Address::ZERO,
            id: uint!(1_U256),
            value: uint!(100_U256),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 200,
        });

        // Route through monitor — should be skipped (no state changes)
        monitor.process_transfer_single(&burn_log).await.unwrap();

        // Verify: only the pre-existing receipt remains, unchanged
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(
            receipts.len(),
            1,
            "Burn transfer (to == 0) should not affect inventory"
        );
        assert_eq!(receipts[0].receipt_id, existing_receipt_id);
        assert_eq!(
            receipts[0].available_balance, existing_balance,
            "Pre-existing receipt balance should be unchanged"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_inbound_transfer_batch_discovers_receipts() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");

        // Do three real deposits on Anvil so balanceOf returns non-zero
        // for each receipt ID (contract auto-assigns receipt IDs sequentially)
        let deposit_amounts = [
            uint!(100_000000000000000000_U256),
            uint!(200_000000000000000000_U256),
            uint!(300_000000000000000000_U256),
        ];

        setup_vault_for_deposits(&evm).await;

        let mut deposits = Vec::new();
        for amount in &deposit_amounts {
            let (receipt_id, shares) =
                evm.mint_directly(*amount, bot_wallet).await.unwrap();
            deposits.push((receipt_id, shares));
        }

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        let config = ReceiptMonitorConfig {
            vault: evm.vault_address,
            receipt_contract,
            bot_wallet,
        };

        let monitor =
            ReceiptMonitor::new(provider, config, cqrs.clone(), NoOpHandler);

        let batch_log = create_transfer_batch_log(TransferBatchLogParams {
            receipt_contract,
            operator: other_wallet,
            from: other_wallet,
            to: bot_wallet,
            ids: deposits.iter().map(|(id, _)| *id).collect(),
            values: deposits.iter().map(|(_, shares)| *shares).collect(),
            tx_hash: b256!(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            block_number: 300,
        });

        monitor.process_transfer_batch(&batch_log).await.unwrap();

        // Verify log output for each batch item
        for (receipt_id, _) in &deposits {
            assert!(
                logs_contain_at!(
                    tracing::Level::DEBUG,
                    &[
                        "Inbound TransferBatch item detected",
                        &format!("receipt_id={receipt_id}")
                    ]
                ),
                "Should log inbound batch detection for receipt {receipt_id}",
            );
        }

        // Verify: all three receipts discovered with exact on-chain balances
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 3, "Should discover all 3 batch receipts");

        for (receipt_id, minted_shares) in &deposits {
            let receipt = receipts
                .iter()
                .find(|r| r.receipt_id == ReceiptId::from(*receipt_id))
                .unwrap_or_else(|| {
                    panic!("Receipt {receipt_id} not found in inventory")
                });
            assert_eq!(
                receipt.available_balance,
                Shares::from(*minted_shares),
                "Balance for receipt {receipt_id} should match on-chain deposit",
            );
            assert!(
                receipt.receipt_info.is_none(),
                "Transfer-discovered receipt {receipt_id} should have no parsed receipt info"
            );
            assert!(
                receipt.receipt_info_bytes.is_none(),
                "Transfer-discovered receipt {receipt_id} should have no raw receipt info bytes"
            );
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_outbound_transfer_batch_reconciles_balances() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Seed aggregate with two receipts (never minted on-chain, so
        // balanceOf will return 0 for both — simulating full outbound transfer)
        let stale_balance = Shares::from(uint!(100_000000000000000000_U256));
        let receipt_ids = [uint!(30_U256), uint!(31_U256)];

        for receipt_id_raw in &receipt_ids {
            cqrs.execute(
                &evm.vault_address.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(*receipt_id_raw),
                    balance: stale_balance,
                    block_number: 1,
                    tx_hash: b256!(
                        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    ),
                    source: ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();
        }

        // Verify both receipts exist with expected balances BEFORE processing
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts_before = context.aggregate().receipts_with_balance();
        assert_eq!(
            receipts_before.len(),
            2,
            "Should have 2 receipts before outbound batch"
        );
        for receipt_id_raw in &receipt_ids {
            let receipt = receipts_before
                .iter()
                .find(|r| r.receipt_id == ReceiptId::from(*receipt_id_raw))
                .unwrap_or_else(|| {
                    panic!("Receipt {receipt_id_raw} not found before outbound transfer")
                });
            assert_eq!(
                receipt.available_balance, stale_balance,
                "Receipt {receipt_id_raw} should have balance 100e18 before outbound transfer"
            );
        }

        let config = ReceiptMonitorConfig {
            vault: evm.vault_address,
            receipt_contract,
            bot_wallet,
        };

        let monitor =
            ReceiptMonitor::new(provider, config, cqrs.clone(), NoOpHandler);

        let batch_log = create_transfer_batch_log(TransferBatchLogParams {
            receipt_contract,
            operator: bot_wallet,
            from: bot_wallet,
            to: other_wallet,
            ids: receipt_ids.to_vec(),
            values: vec![
                uint!(100_000000000000000000_U256),
                uint!(100_000000000000000000_U256),
            ],
            tx_hash: b256!(
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            block_number: 400,
        });

        monitor.process_transfer_batch(&batch_log).await.unwrap();

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Outbound TransferBatch item detected", "receipt_id=30"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Outbound TransferBatch item detected", "receipt_id=31"]
        ));

        // On-chain balance is 0 for both (never minted on Anvil), so
        // reconciliation should deplete both from 100e18 → 0
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts_after = context.aggregate().receipts_with_balance();
        assert!(
            receipts_after.is_empty(),
            "Both receipts should be depleted (100e18 → 0) after outbound batch transfer"
        );
    }

    /// Verifies that `transfer_single_filter` and `transfer_batch_filter`
    /// bind `bot_wallet` to the correct ERC-1155 indexed topic slot:
    ///   Inbound  → topic3 (to)
    ///   Outbound → topic2 (from)
    ///
    /// A flipped index would silently miss all relevant transfer events
    /// while still compiling, so this regression test is load-bearing.
    #[test]
    fn test_transfer_filters_use_correct_topic_indices() {
        let receipt_contract =
            address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let wallet_word = bot_wallet.into_word();

        // Helper: assert that exactly one of topic2/topic3 contains the
        // wallet and the other is empty.
        let assert_topic_placement =
            |filter: &Filter, expect_topic2: bool, label: &str| {
                let topics = &filter.topics;

                if expect_topic2 {
                    assert!(
                        topics[2].matches(&wallet_word),
                        "{label}: expected topic2 to contain bot_wallet"
                    );
                    assert!(
                        topics[3].is_empty(),
                        "{label}: expected topic3 to be empty"
                    );
                } else {
                    assert!(
                        topics[2].is_empty(),
                        "{label}: expected topic2 to be empty"
                    );
                    assert!(
                        topics[3].matches(&wallet_word),
                        "{label}: expected topic3 to contain bot_wallet"
                    );
                }

                // topic0 should always be the event signature
                assert!(
                    !topics[0].is_empty(),
                    "{label}: expected topic0 (event signature) to be set"
                );
            };

        // TransferSingle
        let single_in = transfer_single_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Inbound,
        );
        assert_topic_placement(&single_in, false, "TransferSingle Inbound");

        let single_out = transfer_single_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Outbound,
        );
        assert_topic_placement(&single_out, true, "TransferSingle Outbound");

        // TransferBatch
        let batch_in = transfer_batch_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Inbound,
        );
        assert_topic_placement(&batch_in, false, "TransferBatch Inbound");

        let batch_out = transfer_batch_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Outbound,
        );
        assert_topic_placement(&batch_out, true, "TransferBatch Outbound");

        // Verify event signatures differ between Single and Batch
        assert_ne!(
            single_in.topics[0], batch_in.topics[0],
            "TransferSingle and TransferBatch should have different event signatures"
        );

        // Verify event signatures match expected hashes
        assert!(
            single_in.topics[0]
                .matches(&Receipt::TransferSingle::SIGNATURE_HASH),
            "TransferSingle filter should use TransferSingle signature"
        );
        assert!(
            batch_in.topics[0].matches(&Receipt::TransferBatch::SIGNATURE_HASH),
            "TransferBatch filter should use TransferBatch signature"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_transfer_single_ignores_unrelated_wallets() {
        let evm = LocalEvm::new().await.unwrap();
        let (cqrs, store) = setup_test_cqrs();
        let bot_wallet = evm.wallet_address;
        let wallet_a = address!("0x3333333333333333333333333333333333333333");
        let wallet_b = address!("0x4444444444444444444444444444444444444444");

        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let vault_contract =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider);
        let receipt_contract =
            Address::from(vault_contract.receipt().call().await.unwrap().0);

        // Seed a pre-existing receipt to verify it's unmodified
        let existing_receipt_id = ReceiptId::from(uint!(77_U256));
        let existing_balance = Shares::from(uint!(250_U256));
        cqrs.execute(
            &evm.vault_address.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: existing_receipt_id,
                balance: existing_balance,
                block_number: 1,
                tx_hash: b256!(
                    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                ),
                source: ReceiptSource::External,
                receipt_info: None,
                receipt_info_bytes: None,
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

        // Transfer between two wallets that are NOT the bot wallet
        let log = create_transfer_single_log(TransferSingleLogParams {
            receipt_contract,
            operator: wallet_a,
            from: wallet_a,
            to: wallet_b,
            id: uint!(1_U256),
            value: uint!(100_U256),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 100,
        });

        // Route through monitor — should be a no-op (neither from nor to is bot_wallet)
        monitor.process_transfer_single(&log).await.unwrap();

        // Verify: pre-existing receipt is untouched, no new receipts added
        let context =
            store.load_aggregate(&evm.vault_address.to_string()).await.unwrap();
        let receipts = context.aggregate().receipts_with_balance();
        assert_eq!(
            receipts.len(),
            1,
            "Unrelated transfer should not add or remove receipts"
        );
        assert_eq!(receipts[0].receipt_id, existing_receipt_id);
        assert_eq!(
            receipts[0].available_balance, existing_balance,
            "Pre-existing receipt balance should be unchanged after unrelated transfer"
        );
    }
}
