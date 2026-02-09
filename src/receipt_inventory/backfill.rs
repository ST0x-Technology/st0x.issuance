use alloy::primitives::{Address, TxHash};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{CqrsFramework, EventStore};
use futures::future::join_all;
use itertools::Itertools;
use std::sync::Arc;
use tracing::info;

use super::{ReceiptId, ReceiptInventory, ReceiptInventoryCommand, Shares};
use crate::bindings::Receipt;

/// Backfills the ReceiptInventory aggregate by scanning historic ERC-1155
/// TransferSingle/TransferBatch events where the bot wallet received receipts.
///
/// This handles receipts minted outside our system (e.g., manual operations)
/// that the service needs to know about for burn planning.
pub(crate) struct ReceiptBackfiller<ProviderType, ReceiptInventoryStore>
where
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    provider: ProviderType,
    receipt_contract: Address,
    bot_wallet: Address,
    vault: Address,
    cqrs: Arc<CqrsFramework<ReceiptInventory, ReceiptInventoryStore>>,
}

#[derive(Debug)]
pub(crate) struct BackfillResult {
    pub(crate) processed_count: u64,
    pub(crate) skipped_zero_balance: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BackfillError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),

    #[error("Failed to decode TransferSingle event: {0}")]
    EventDecode(#[from] alloy::sol_types::Error),

    #[error("Missing transaction hash in log")]
    MissingTxHash,

    #[error("Missing block number in log")]
    MissingBlockNumber,

    #[error("Contract call error: {0}")]
    ContractCall(#[from] alloy::contract::Error),

    #[error("CQRS error: {0}")]
    Cqrs(cqrs_es::AggregateError<super::ReceiptInventoryError>),
}

impl<ProviderType, ReceiptInventoryStore>
    ReceiptBackfiller<ProviderType, ReceiptInventoryStore>
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

impl<ProviderType, ReceiptInventoryStore>
    ReceiptBackfiller<ProviderType, ReceiptInventoryStore>
where
    ProviderType: alloy::providers::Provider + Clone + Send + Sync,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    /// Scans historic TransferSingle and TransferBatch events to discover
    /// receipts the bot owns.
    ///
    /// For each receipt discovered:
    /// 1. Checks current on-chain balance (not just transfer amount, since
    ///    receipts may have been partially burned)
    /// 2. If balance > 0 and not already tracked, emits DiscoverReceipt command
    ///
    /// `from_block` is the block to start scanning from. On first run, pass
    /// the configured deployment block. On subsequent runs, pass
    /// `aggregate.last_discovered_block().unwrap_or(deployment_block)`.
    ///
    /// This is idempotent - running multiple times produces the same result.
    pub(crate) async fn backfill_receipts(
        &self,
        from_block: u64,
    ) -> Result<BackfillResult, BackfillError> {
        let start_block = from_block;

        info!(
            receipt_contract = %self.receipt_contract,
            bot_wallet = %self.bot_wallet,
            from_block = start_block,
            "Starting receipt backfill"
        );

        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        let single_filter = receipt_contract
            .TransferSingle_filter()
            .topic3(self.bot_wallet)
            .from_block(start_block)
            .filter;

        let batch_filter = receipt_contract
            .TransferBatch_filter()
            .topic3(self.bot_wallet)
            .from_block(start_block)
            .filter;

        let (single_logs, batch_logs) = futures::try_join!(
            self.provider.get_logs(&single_filter),
            self.provider.get_logs(&batch_filter),
        )?;

        let transfers = single_logs
            .iter()
            .map(Self::parse_transfer_single)
            .chain(batch_logs.iter().map(Self::parse_transfer_batch))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .unique_by(|transfer| transfer.receipt_id);

        let results =
            join_all(transfers.map(|transfer| self.process_transfer(transfer)))
                .await;

        let (processed_count, skipped_zero_balance) = results
            .into_iter()
            .try_fold((0u64, 0u64), |(processed, zero), result| {
                result.map(|outcome| match outcome {
                    ProcessOutcome::Processed => (processed + 1, zero),
                    ProcessOutcome::ZeroBalance => (processed, zero + 1),
                })
            })?;

        info!(
            processed_count,
            skipped_zero_balance, "Receipt backfill complete"
        );

        Ok(BackfillResult { processed_count, skipped_zero_balance })
    }

    fn parse_transfer_single(
        log: &Log,
    ) -> Result<Vec<TransferInfo>, BackfillError> {
        let event = Receipt::TransferSingle::decode_log(&log.inner)?;
        let tx_hash =
            log.transaction_hash.ok_or(BackfillError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(BackfillError::MissingBlockNumber)?;

        Ok(vec![TransferInfo {
            receipt_id: ReceiptId::from(event.id),
            tx_hash,
            block_number,
        }])
    }

    fn parse_transfer_batch(
        log: &Log,
    ) -> Result<Vec<TransferInfo>, BackfillError> {
        let event = Receipt::TransferBatch::decode_log(&log.inner)?;
        let tx_hash =
            log.transaction_hash.ok_or(BackfillError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(BackfillError::MissingBlockNumber)?;

        Ok(event
            .ids
            .iter()
            .map(|&id| TransferInfo {
                receipt_id: ReceiptId::from(id),
                tx_hash,
                block_number,
            })
            .collect())
    }

    async fn process_transfer(
        &self,
        transfer: TransferInfo,
    ) -> Result<ProcessOutcome, BackfillError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        let current_balance = receipt_contract
            .balanceOf(self.bot_wallet, transfer.receipt_id.inner())
            .call()
            .await?;

        if current_balance.is_zero() {
            return Ok(ProcessOutcome::ZeroBalance);
        }

        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: transfer.receipt_id,
                    balance: Shares::from(current_balance),
                    block_number: transfer.block_number,
                    tx_hash: transfer.tx_hash,
                },
            )
            .await
            .map_err(BackfillError::Cqrs)?;

        info!(
            receipt_id = %transfer.receipt_id,
            balance = %current_balance,
            "Processed receipt"
        );

        Ok(ProcessOutcome::Processed)
    }
}

struct TransferInfo {
    receipt_id: ReceiptId,
    tx_hash: TxHash,
    block_number: u64,
}

enum ProcessOutcome {
    Processed,
    ZeroBalance,
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Address, B256, U256, address, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::Log;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolEvent;
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use std::sync::Arc;

    use super::ReceiptBackfiller;
    use crate::bindings::Receipt;
    use crate::receipt_inventory::ReceiptInventory;

    type TestStore = MemStore<ReceiptInventory>;

    fn test_addresses() -> (Address, Address, Address) {
        let receipt_contract =
            address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let vault = address!("0x3333333333333333333333333333333333333333");
        (receipt_contract, bot_wallet, vault)
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

    fn create_transfer_single_log(params: TransferSingleLogParams) -> Log {
        let event = Receipt::TransferSingle {
            operator: params.operator,
            from: params.from,
            to: params.to,
            id: params.id,
            value: params.value,
        };

        Log {
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

    struct TransferBatchLogParams<'a> {
        receipt_contract: Address,
        operator: Address,
        from: Address,
        to: Address,
        ids: &'a [U256],
        values: &'a [U256],
        tx_hash: B256,
        block_number: u64,
    }

    fn create_transfer_batch_log(params: &TransferBatchLogParams<'_>) -> Log {
        let event = Receipt::TransferBatch {
            operator: params.operator,
            from: params.from,
            to: params.to,
            ids: params.ids.to_vec(),
            values: params.values.to_vec(),
        };

        Log {
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

    fn setup_cqrs() -> Arc<CqrsFramework<ReceiptInventory, TestStore>> {
        Arc::new(CqrsFramework::new(MemStore::default(), vec![], ()))
    }

    fn setup_cqrs_with_store()
    -> (Arc<CqrsFramework<ReceiptInventory, TestStore>>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    #[tokio::test]
    async fn backfill_discovers_receipt_from_historic_transfer_single() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let cqrs = setup_cqrs();

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let transfer_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: Address::ZERO,
                from: Address::ZERO,
                to: bot_wallet,
                id: receipt_id,
                value: balance,
                tx_hash,
                block_number: 100,
            });

        let asserter = Asserter::new();

        // eth_getLogs (TransferSingle filter)
        asserter.push_success(&vec![transfer_log]);

        // eth_getLogs (TransferBatch filter)
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        // eth_call (balanceOf) - must be ABI-encoded as 32-byte big-endian.
        // push_success accepts U256 but serializes it as JSON hex string ("0x3e8"),
        // not as ABI-encoded bytes that eth_call returns.
        asserter.push_success(&balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(
            provider,
            receipt_contract,
            bot_wallet,
            vault,
            cqrs,
        );

        let result = backfiller.backfill_receipts(0).await.unwrap();

        assert_eq!(result.processed_count, 1);
        assert_eq!(result.skipped_zero_balance, 0);
    }

    #[tokio::test]
    async fn backfill_is_idempotent() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let cqrs = setup_cqrs();

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        let transfer_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: Address::ZERO,
                from: Address::ZERO,
                to: bot_wallet,
                id: receipt_id,
                value: balance,
                tx_hash,
                block_number: 100,
            });

        // First run
        let asserter1 = Asserter::new();

        // eth_getLogs (TransferSingle filter)
        asserter1.push_success(&vec![transfer_log.clone()]);

        // eth_getLogs (TransferBatch filter)
        asserter1.push_success(&Vec::<alloy::rpc::types::Log>::new());

        // eth_call (balanceOf) - must be ABI-encoded as 32-byte big-endian.
        // push_success accepts U256 but serializes it as JSON hex string ("0x3e8"),
        // not as ABI-encoded bytes that eth_call returns.
        asserter1.push_success(&balance.to_be_bytes::<32>());

        let provider1 = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter1);

        let backfiller1 = ReceiptBackfiller::new(
            provider1,
            receipt_contract,
            bot_wallet,
            vault,
            cqrs.clone(),
        );

        let result1 = backfiller1.backfill_receipts(0).await.unwrap();
        assert_eq!(result1.processed_count, 1);

        // Second run - command is idempotent, succeeds without error
        let asserter2 = Asserter::new();

        // eth_getLogs (TransferSingle filter)
        asserter2.push_success(&vec![transfer_log]);

        // eth_getLogs (TransferBatch filter)
        asserter2.push_success(&Vec::<alloy::rpc::types::Log>::new());

        // eth_call (balanceOf) - ABI-encoded 32-byte big-endian
        asserter2.push_success(&balance.to_be_bytes::<32>());

        let provider2 = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter2);

        let backfiller2 = ReceiptBackfiller::new(
            provider2,
            receipt_contract,
            bot_wallet,
            vault,
            cqrs,
        );

        let result2 = backfiller2.backfill_receipts(0).await.unwrap();

        // Command succeeds but emits no events (idempotent)
        assert_eq!(result2.processed_count, 1);
    }

    #[tokio::test]
    async fn backfill_skips_zero_balance_receipts() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let cqrs = setup_cqrs();

        let receipt_id = U256::from(99);
        let transfer_amount = U256::from(500);
        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        let transfer_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: Address::ZERO,
                from: Address::ZERO,
                to: bot_wallet,
                id: receipt_id,
                value: transfer_amount,
                tx_hash,
                block_number: 200,
            });

        let asserter = Asserter::new();

        // eth_getLogs (TransferSingle filter)
        asserter.push_success(&vec![transfer_log]);

        // eth_getLogs (TransferBatch filter)
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        // eth_call (balanceOf) - current balance is zero (receipt was fully burned).
        // Must be ABI-encoded as 32-byte big-endian, not JSON hex.
        asserter.push_success(&U256::ZERO.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(
            provider,
            receipt_contract,
            bot_wallet,
            vault,
            cqrs,
        );

        let result = backfiller.backfill_receipts(0).await.unwrap();

        assert_eq!(result.processed_count, 0);
        assert_eq!(result.skipped_zero_balance, 1);
    }

    #[tokio::test]
    async fn backfill_uses_current_onchain_balance_not_transfer_amount() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, store) = setup_cqrs_with_store();

        let receipt_id = U256::from(77);
        let original_transfer_amount = U256::from(1000);
        let current_balance = U256::from(300); // Partially burned
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );

        let transfer_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: Address::ZERO,
                from: Address::ZERO,
                to: bot_wallet,
                id: receipt_id,
                value: original_transfer_amount,
                tx_hash,
                block_number: 300,
            });

        let asserter = Asserter::new();

        // eth_getLogs (TransferSingle filter)
        asserter.push_success(&vec![transfer_log]);

        // eth_getLogs (TransferBatch filter)
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        // eth_call (balanceOf) - returns current balance (300), not original
        // transfer amount (1000). Must be ABI-encoded as 32-byte big-endian.
        asserter.push_success(&current_balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(
            provider,
            receipt_contract,
            bot_wallet,
            vault,
            cqrs,
        );

        let result = backfiller.backfill_receipts(0).await.unwrap();

        assert_eq!(result.processed_count, 1);

        // Verify the aggregate has the current balance, not the transfer amount
        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = ctx.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].available_balance.inner(), current_balance);
    }

    #[tokio::test]
    async fn backfill_discovers_receipts_from_transfer_batch() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, store) = setup_cqrs_with_store();

        let ids = [U256::from(1), U256::from(2), U256::from(3)];
        let values = [U256::from(100), U256::from(200), U256::from(300)];
        let balances = [U256::from(50), U256::from(200), U256::from(300)]; // First partially burned
        let tx_hash = b256!(
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        );

        let batch_log = create_transfer_batch_log(&TransferBatchLogParams {
            receipt_contract,
            operator: Address::ZERO,
            from: Address::ZERO,
            to: bot_wallet,
            ids: &ids,
            values: &values,
            tx_hash,
            block_number: 500,
        });

        let asserter = Asserter::new();

        // eth_getLogs (TransferSingle filter) - returns empty
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        // eth_getLogs (TransferBatch filter) - returns the batch event
        asserter.push_success(&vec![batch_log]);

        // eth_call (balanceOf) for each receipt, in processing order.
        // Must be ABI-encoded as 32-byte big-endian, not JSON hex.
        asserter.push_success(&balances[0].to_be_bytes::<32>());
        asserter.push_success(&balances[1].to_be_bytes::<32>());
        asserter.push_success(&balances[2].to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(
            provider,
            receipt_contract,
            bot_wallet,
            vault,
            cqrs,
        );

        let result = backfiller.backfill_receipts(0).await.unwrap();

        assert_eq!(result.processed_count, 3);
        assert_eq!(result.skipped_zero_balance, 0);

        // Verify all receipts are in the aggregate with current balances
        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = ctx.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 3);
    }
}
