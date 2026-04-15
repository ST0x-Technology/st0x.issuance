use alloy::primitives::{Address, Bytes, TxHash};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use futures::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use std::sync::Arc;
use tracing::{debug, info};

use super::monitor::{transfer_batch_filter, transfer_single_filter};
use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, Shares, determine_source,
};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};

/// Maximum number of blocks to query in a single get_logs call.
/// RPCs typically limit response sizes, so we chunk large ranges.
const BLOCK_CHUNK_SIZE: u64 = 2000;

/// Maximum concurrent RPC calls for balance checks.
/// Limits parallelism to avoid overwhelming the RPC provider.
const MAX_CONCURRENT_BALANCE_CHECKS: usize = 4;

/// Generates inclusive block ranges of at most `chunk_size` blocks.
fn block_ranges(
    from: u64,
    to: u64,
    chunk_size: u64,
) -> impl Iterator<Item = (u64, u64)> {
    std::iter::successors(Some(from), move |&start| {
        let next = start + chunk_size;
        if next <= to { Some(next) } else { None }
    })
    .map(move |start| (start, (start + chunk_size - 1).min(to)))
}

/// Backfills the ReceiptInventory aggregate by scanning historic Deposit
/// events and ERC-1155 transfer events where the bot wallet received receipts.
///
/// This handles receipts minted outside our system (e.g., manual operations)
/// and receipts transferred to the bot wallet from other addresses.
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
    SolTypes(#[from] alloy::sol_types::Error),
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error("Contract call error: {0}")]
    ContractCall(#[from] alloy::contract::Error),
    #[error("CQRS error: {0}")]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
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
    /// Scans historic Deposit and ERC-1155 transfer events to discover
    /// receipts the bot owns.
    ///
    /// For each receipt discovered:
    /// 1. Checks current on-chain balance (not just transfer amount, since
    ///    receipts may have been partially burned)
    /// 2. If balance > 0 and not already tracked, emits DiscoverReceipt command
    ///
    /// `from_block` is the block to start scanning from. On first run, pass
    /// the configured backfill start block. On subsequent runs, pass
    /// `aggregate.last_backfilled_block().unwrap_or(backfill_start_block)`.
    ///
    /// Queries are chunked to avoid RPC response size limits.
    ///
    /// This is idempotent - running multiple times produces the same result.
    pub(crate) async fn backfill_receipts(
        &self,
        from_block: u64,
    ) -> Result<BackfillResult, BackfillError> {
        let current_block = self.provider.get_block_number().await?;

        if from_block > current_block {
            info!(
                from_block,
                current_block,
                "Backfill skipped: from_block is ahead of current block"
            );

            return Ok(BackfillResult {
                processed_count: 0,
                skipped_zero_balance: 0,
            });
        }

        info!(
            receipt_contract = %self.receipt_contract,
            bot_wallet = %self.bot_wallet,
            from_block,
            to_block = current_block,
            "Starting receipt backfill"
        );

        let all_logs: Vec<Log> = stream::iter(block_ranges(
            from_block,
            current_block,
            BLOCK_CHUNK_SIZE,
        ))
        .then(|(chunk_from, chunk_to)| async move {
            let logs = self.fetch_logs_for_range(chunk_from, chunk_to).await?;
            debug!(
                chunk_from,
                chunk_to,
                logs_found = logs.len(),
                "Processed block range"
            );
            Ok::<_, BackfillError>(logs)
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect();

        let discoveries = all_logs
            .iter()
            .map(Self::parse_log)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .unique_by(|discovery| discovery.receipt_id);

        let results: Vec<_> = stream::iter(discoveries)
            .map(|discovery| self.process_discovery(discovery))
            .buffer_unordered(MAX_CONCURRENT_BALANCE_CHECKS)
            .collect()
            .await;

        let (processed_count, skipped_zero_balance) = results
            .into_iter()
            .try_fold((0u64, 0u64), |(processed, zero), result| {
                result.map(|outcome| match outcome {
                    ProcessOutcome::Processed => (processed + 1, zero),
                    ProcessOutcome::ZeroBalance => (processed, zero + 1),
                })
            })?;

        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::AdvanceBackfillCheckpoint {
                    block_number: current_block,
                },
            )
            .await?;

        info!(
            processed_count,
            skipped_zero_balance,
            checkpoint_block = current_block,
            "Receipt backfill complete"
        );

        Ok(BackfillResult { processed_count, skipped_zero_balance })
    }

    async fn fetch_logs_for_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, BackfillError> {
        let vault_contract =
            OffchainAssetReceiptVault::new(self.vault, &self.provider);

        // Query Deposit events where owner (receiver) is the bot wallet.
        // Deposit event: (sender, owner, assets, shares, id, receiptInformation)
        // owner is not indexed, so we filter client-side after fetching.
        let deposit_filter = vault_contract
            .Deposit_filter()
            .from_block(from_block)
            .to_block(to_block)
            .filter;

        let deposit_logs = self.provider.get_logs(&deposit_filter).await?;

        let mut filtered: Vec<Log> = deposit_logs
            .into_iter()
            .filter_map(|log| {
                match OffchainAssetReceiptVault::Deposit::decode_log(&log.inner)
                {
                    Ok(event) if event.owner == self.bot_wallet => {
                        Some(Ok(log))
                    }
                    Ok(_) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Also scan ERC-1155 TransferSingle and TransferBatch events on the
        // Receipt contract for inbound transfers to the bot wallet.
        let transfer_single_logs = self
            .provider
            .get_logs(
                &transfer_single_filter(self.receipt_contract)
                    .from_block(from_block)
                    .to_block(to_block),
            )
            .await?;

        let transfer_batch_logs = self
            .provider
            .get_logs(
                &transfer_batch_filter(self.receipt_contract)
                    .from_block(from_block)
                    .to_block(to_block),
            )
            .await?;

        // Filter transfer events: keep only inbound (to == bot_wallet),
        // skip mint transfers (from == address(0)) since Deposit covers those.
        for log in transfer_single_logs {
            let event = Receipt::TransferSingle::decode_log(&log.inner)?;

            if event.to == self.bot_wallet && !event.from.is_zero() {
                filtered.push(log);
            }
        }

        for log in transfer_batch_logs {
            let event = Receipt::TransferBatch::decode_log(&log.inner)?;

            if event.to == self.bot_wallet && !event.from.is_zero() {
                filtered.push(log);
            }
        }

        Ok(filtered)
    }

    /// Parses a log into one or more `ReceiptDiscovery` entries.
    ///
    /// Deposit logs produce a single entry with receipt information.
    /// TransferSingle logs produce a single entry without receipt information.
    /// TransferBatch logs produce one entry per (id, value) pair.
    fn parse_log(log: &Log) -> Result<Vec<ReceiptDiscovery>, BackfillError> {
        let tx_hash =
            log.transaction_hash.ok_or(BackfillError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(BackfillError::MissingBlockNumber)?;

        // Try Deposit first, then TransferSingle, then TransferBatch
        if let Ok(event) =
            OffchainAssetReceiptVault::Deposit::decode_log(&log.inner)
        {
            return Ok(vec![ReceiptDiscovery {
                receipt_id: ReceiptId::from(event.id),
                tx_hash,
                block_number,
                receipt_information: event.receiptInformation.clone(),
            }]);
        }

        if let Ok(event) = Receipt::TransferSingle::decode_log(&log.inner) {
            return Ok(vec![ReceiptDiscovery {
                receipt_id: ReceiptId::from(event.id),
                tx_hash,
                block_number,
                receipt_information: Bytes::new(),
            }]);
        }

        if let Ok(event) = Receipt::TransferBatch::decode_log(&log.inner) {
            return Ok(event
                .ids
                .iter()
                .map(|id| ReceiptDiscovery {
                    receipt_id: ReceiptId::from(*id),
                    tx_hash,
                    block_number,
                    receipt_information: Bytes::new(),
                })
                .collect());
        }

        Err(BackfillError::SolTypes(alloy::sol_types::Error::custom(
            "Log did not match Deposit, TransferSingle, or TransferBatch",
        )))
    }

    async fn process_discovery(
        &self,
        discovery: ReceiptDiscovery,
    ) -> Result<ProcessOutcome, BackfillError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        let current_balance = receipt_contract
            .balanceOf(self.bot_wallet, discovery.receipt_id.inner())
            .call()
            .await?;

        if current_balance.is_zero() {
            return Ok(ProcessOutcome::ZeroBalance);
        }

        let (source, receipt_info) =
            determine_source(&discovery.receipt_information);
        let receipt_info_bytes = if discovery.receipt_information.is_empty() {
            None
        } else {
            Some(discovery.receipt_information.clone())
        };

        self.cqrs
            .execute(
                &self.vault.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: discovery.receipt_id,
                    balance: Shares::from(current_balance),
                    block_number: discovery.block_number,
                    tx_hash: discovery.tx_hash,
                    source,
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
                    receipt_id: discovery.receipt_id,
                    on_chain_balance: Shares::from(current_balance),
                },
            )
            .await?;

        debug!(
            receipt_id = %discovery.receipt_id,
            balance = %current_balance,
            "Processed receipt"
        );

        Ok(ProcessOutcome::Processed)
    }
}

struct ReceiptDiscovery {
    receipt_id: ReceiptId,
    tx_hash: TxHash,
    block_number: u64,
    receipt_information: Bytes,
}

enum ProcessOutcome {
    Processed,
    ZeroBalance,
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Address, B256, Bytes, U256, address, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::Log;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolEvent;
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::ReceiptBackfiller;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::receipt_inventory::{
        ReceiptId, ReceiptInventory, ReceiptInventoryCommand, ReceiptSource,
        Shares,
    };
    use crate::test_utils::logs_contain_at;

    type TestStore = MemStore<ReceiptInventory>;

    fn test_addresses() -> (Address, Address, Address) {
        let receipt_contract =
            address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let vault = address!("0x3333333333333333333333333333333333333333");
        (receipt_contract, bot_wallet, vault)
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

    fn create_deposit_log(params: DepositLogParams) -> Log {
        let event = OffchainAssetReceiptVault::Deposit {
            sender: params.sender,
            owner: params.owner,
            assets: params.assets,
            shares: params.shares,
            id: params.id,
            receiptInformation: params.receipt_information,
        };

        Log {
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

    fn setup_cqrs() -> Arc<CqrsFramework<ReceiptInventory, TestStore>> {
        Arc::new(CqrsFramework::new(MemStore::default(), vec![], ()))
    }

    /// Pushes empty responses for the TransferSingle and TransferBatch
    /// get_logs calls that the backfiller now makes.
    fn push_empty_transfer_logs(asserter: &Asserter) {
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&Vec::<Log>::new());
    }

    fn setup_cqrs_with_store()
    -> (Arc<CqrsFramework<ReceiptInventory, TestStore>>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    #[tokio::test]
    #[traced_test]
    async fn backfill_discovers_receipt_from_historic_deposit() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let cqrs = setup_cqrs();

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 100,
        });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_transfer_logs(&asserter);
        // eth_call (balanceOf)
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

        let test = "backfill_discovers_receipt_from_historic_deposit";
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &[test, "Processed block range"]
            ),
            "Expected DEBUG log for block range processing"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &[test, "Processed receipt"]
            ),
            "Expected DEBUG log for processed receipt"
        );
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

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 100,
        });

        // First run
        let asserter1 = Asserter::new();
        let current_block = 200u64;

        // eth_blockNumber
        asserter1.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter)
        asserter1.push_success(&vec![deposit_log.clone()]);
        push_empty_transfer_logs(&asserter1);
        // eth_call (balanceOf)
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

        // Second run - command is idempotent
        let asserter2 = Asserter::new();

        // eth_blockNumber
        asserter2.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter)
        asserter2.push_success(&vec![deposit_log]);
        push_empty_transfer_logs(&asserter2);
        // eth_call (balanceOf)
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
        assert_eq!(result2.processed_count, 1);
    }

    #[tokio::test]
    #[traced_test]
    async fn backfill_skips_zero_balance_receipts() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let cqrs = setup_cqrs();

        let receipt_id = U256::from(99);
        let deposit_amount = U256::from(500);
        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: deposit_amount,
            shares: deposit_amount,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 200,
        });

        let asserter = Asserter::new();
        let current_block = 300u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_transfer_logs(&asserter);
        // eth_call (balanceOf) - zero because receipt was fully burned
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

        let test = "backfill_skips_zero_balance_receipts";
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &[test, "Processed block range"]
            ),
            "Expected DEBUG log for block range processing"
        );
        assert!(
            !logs_contain_at!(
                tracing::Level::DEBUG,
                &[test, "Processed receipt"]
            ),
            "Should NOT log processed receipt when all receipts have zero balance"
        );
    }

    #[tokio::test]
    async fn backfill_uses_current_onchain_balance_not_deposit_amount() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, store) = setup_cqrs_with_store();

        let receipt_id = U256::from(77);
        let original_deposit = U256::from(1000);
        let current_balance = U256::from(300); // Partially burned
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: original_deposit,
            shares: original_deposit,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 300,
        });

        let asserter = Asserter::new();
        let checkpoint_block = 400u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(checkpoint_block));
        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_transfer_logs(&asserter);
        // eth_call (balanceOf) - returns current balance, not original deposit
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

        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = ctx.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].available_balance.inner(), current_balance);
    }

    #[tokio::test]
    async fn backfill_filters_deposits_by_owner() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let cqrs = setup_cqrs();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");

        let balance = U256::from(1000);

        // Deposit to bot_wallet (should be processed)
        let deposit_for_bot = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: U256::from(1),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 100,
        });

        // Deposit to other_wallet (should be filtered out)
        let deposit_for_other = create_deposit_log(DepositLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: balance,
            shares: balance,
            id: U256::from(2),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 101,
        });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter) - both deposits returned, filtered client-side
        asserter.push_success(&vec![deposit_for_bot, deposit_for_other]);
        push_empty_transfer_logs(&asserter);
        // eth_call (balanceOf) - only called for bot's receipt
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
    }

    #[tokio::test]
    #[traced_test]
    async fn backfill_emits_checkpoint_after_processing() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, store) = setup_cqrs_with_store();

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 100,
        });

        let asserter = Asserter::new();
        let current_block = 150u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_transfer_logs(&asserter);
        // eth_call (balanceOf)
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

        backfiller.backfill_receipts(0).await.unwrap();

        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        assert_eq!(
            ctx.aggregate().last_backfilled_block(),
            Some(current_block),
            "Backfill should checkpoint to current block after processing"
        );

        assert!(
            logs_contain_at!(
                tracing::Level::INFO,
                &[
                    "backfill_emits_checkpoint_after_processing",
                    "Receipt backfill complete"
                ]
            ),
            "Expected INFO log for backfill completion"
        );
    }

    #[test]
    fn block_ranges_single_chunk() {
        let ranges: Vec<_> = super::block_ranges(0, 100, 2000).collect();
        assert_eq!(ranges, vec![(0, 100)]);
    }

    #[test]
    fn block_ranges_exact_multiple() {
        let ranges: Vec<_> = super::block_ranges(0, 3999, 2000).collect();
        assert_eq!(ranges, vec![(0, 1999), (2000, 3999)]);
    }

    #[test]
    fn block_ranges_with_remainder() {
        let ranges: Vec<_> = super::block_ranges(0, 5000, 2000).collect();
        assert_eq!(ranges, vec![(0, 1999), (2000, 3999), (4000, 5000)]);
    }

    #[test]
    fn block_ranges_from_nonzero() {
        let ranges: Vec<_> = super::block_ranges(1000, 4500, 2000).collect();
        assert_eq!(ranges, vec![(1000, 2999), (3000, 4500)]);
    }

    #[test]
    fn block_ranges_empty_when_from_equals_to() {
        let ranges: Vec<_> = super::block_ranges(100, 100, 2000).collect();
        assert_eq!(ranges, vec![(100, 100)]);
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
        use crate::bindings::Receipt;

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

    #[tokio::test]
    #[traced_test]
    async fn backfill_discovers_receipt_from_inbound_transfer() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (cqrs, store) = setup_cqrs_with_store();

        let receipt_id = U256::from(55);
        let balance = U256::from(750);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let transfer_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: other_wallet,
                from: other_wallet,
                to: bot_wallet,
                id: receipt_id,
                value: balance,
                tx_hash,
                block_number: 150,
            });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter) - no deposits
        asserter.push_success(&Vec::<Log>::new());
        // eth_getLogs (TransferSingle filter) - one inbound transfer
        asserter.push_success(&vec![transfer_log]);
        // eth_getLogs (TransferBatch filter) - no batch transfers
        asserter.push_success(&Vec::<Log>::new());
        // eth_call (balanceOf)
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

        // Verify the receipt was actually discovered with correct ID and balance
        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = ctx.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 1, "Should discover exactly one receipt");
        assert_eq!(
            receipts[0].receipt_id,
            ReceiptId::from(receipt_id),
            "Discovered receipt ID should match the transfer"
        );
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(balance),
            "Balance should match the on-chain balanceOf response"
        );
    }

    #[tokio::test]
    async fn backfill_filters_outbound_and_mint_transfers() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (cqrs, store) = setup_cqrs_with_store();

        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        // Outbound transfer (from == bot_wallet) — should be filtered
        let outbound_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: bot_wallet,
                from: bot_wallet,
                to: other_wallet,
                id: U256::from(1),
                value: U256::from(100),
                tx_hash,
                block_number: 100,
            });

        // Mint transfer (from == address(0)) — should be filtered
        let mint_log = create_transfer_single_log(TransferSingleLogParams {
            receipt_contract,
            operator: bot_wallet,
            from: Address::ZERO,
            to: bot_wallet,
            id: U256::from(2),
            value: U256::from(200),
            tx_hash,
            block_number: 101,
        });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter) - no deposits
        asserter.push_success(&Vec::<Log>::new());
        // eth_getLogs (TransferSingle filter) - outbound + mint transfers
        asserter.push_success(&vec![outbound_log, mint_log]);
        // eth_getLogs (TransferBatch filter) - no batch transfers
        asserter.push_success(&Vec::<Log>::new());
        // No balanceOf calls since both transfers are filtered

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

        assert_eq!(
            result.processed_count, 0,
            "Outbound and mint transfers should not be processed"
        );
        assert_eq!(result.skipped_zero_balance, 0);

        // Verify: aggregate has no receipts at all
        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        assert!(
            ctx.aggregate().receipts_with_balance().is_empty(),
            "No receipts should be discovered from outbound/mint transfers"
        );
    }

    #[tokio::test]
    async fn backfill_deduplicates_deposit_and_transfer_for_same_receipt() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (cqrs, store) = setup_cqrs_with_store();

        let receipt_id = U256::from(88);
        let balance = U256::from(500);

        // Same receipt discovered via both Deposit and TransferSingle
        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 100,
        });

        let transfer_log = create_transfer_single_log(
            TransferSingleLogParams {
                receipt_contract,
                operator: other_wallet,
                from: other_wallet,
                to: bot_wallet,
                id: receipt_id,
                value: balance,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                block_number: 110,
            },
        );

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        // eth_getLogs (TransferSingle filter)
        asserter.push_success(&vec![transfer_log]);
        // eth_getLogs (TransferBatch filter)
        asserter.push_success(&Vec::<Log>::new());
        // eth_call (balanceOf) — only one call since deduplicated
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

        // Should only process once despite appearing in both Deposit and Transfer
        assert_eq!(result.processed_count, 1);

        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = ctx.aggregate().receipts_with_balance();
        assert_eq!(
            receipts.len(),
            1,
            "Should have exactly one receipt after deduplication"
        );
        assert_eq!(
            receipts[0].receipt_id,
            ReceiptId::from(receipt_id),
            "Receipt ID should match the deduplicated receipt"
        );
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(balance),
            "Balance should match the on-chain balanceOf response"
        );
    }

    /// Verifies that backfill reconciles an already-known receipt's balance
    /// upward when an inbound transfer increases the on-chain balance.
    #[tokio::test]
    async fn backfill_reconciles_known_receipt_balance_upward() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (cqrs, store) = setup_cqrs_with_store();

        let receipt_id_raw = U256::from(55);
        let stale_balance = U256::from(500);
        let new_on_chain_balance = U256::from(750);

        // Seed aggregate with a receipt at a stale lower balance
        cqrs.execute(
            &vault.to_string(),
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: ReceiptId::from(receipt_id_raw),
                balance: Shares::from(stale_balance),
                block_number: 50,
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

        // Inbound transfer log (someone sent tokens to bot_wallet)
        let transfer_log = create_transfer_single_log(
            TransferSingleLogParams {
                receipt_contract,
                operator: other_wallet,
                from: other_wallet,
                to: bot_wallet,
                id: receipt_id_raw,
                value: U256::from(250),
                tx_hash: b256!(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                block_number: 150,
            },
        );

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_blockNumber
        asserter.push_success(&U256::from(current_block));
        // eth_getLogs (Deposit filter) — no deposits
        asserter.push_success(&Vec::<Log>::new());
        // eth_getLogs (TransferSingle filter) — one inbound transfer
        asserter.push_success(&vec![transfer_log]);
        // eth_getLogs (TransferBatch filter) — none
        asserter.push_success(&Vec::<Log>::new());
        // eth_call (balanceOf) — returns the new higher balance
        asserter.push_success(&new_on_chain_balance.to_be_bytes::<32>());

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

        // Verify balance was reconciled upward from 500 to 750
        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = ctx.aggregate().receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(new_on_chain_balance),
            "Backfill should reconcile known receipt balance upward"
        );
    }
}
