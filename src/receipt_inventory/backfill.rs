use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use cqrs_es::{CqrsFramework, EventStore};
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
    pub(crate) discovered_count: u64,
    pub(crate) skipped_zero_balance: u64,
    pub(crate) skipped_already_tracked: u64,
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
}

impl<ProviderType, ReceiptInventoryStore>
    ReceiptBackfiller<ProviderType, ReceiptInventoryStore>
where
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    pub(crate) fn new(
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
    /// Scans historic TransferSingle events to discover receipts the bot owns.
    ///
    /// For each receipt discovered:
    /// 1. Checks current on-chain balance (not just transfer amount, since
    ///    receipts may have been partially burned)
    /// 2. If balance > 0 and not already tracked, emits DiscoverReceipt command
    ///
    /// This is idempotent - running multiple times produces the same result.
    pub(crate) async fn backfill_receipts(
        &self,
    ) -> Result<BackfillResult, BackfillError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{
        Address, B256, Bytes, Log as PrimitiveLog, LogData, U256, address, b256,
    };
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

    fn create_transfer_single_log(
        receipt_contract: Address,
        operator: Address,
        from: Address,
        to: Address,
        id: U256,
        value: U256,
        tx_hash: B256,
        block_number: u64,
    ) -> Log {
        let topics = vec![
            Receipt::TransferSingle::SIGNATURE_HASH,
            B256::left_padding_from(&operator[..]),
            B256::left_padding_from(&from[..]),
            B256::left_padding_from(&to[..]),
        ];

        let mut data = Vec::with_capacity(64);
        data.extend_from_slice(&id.to_be_bytes::<32>());
        data.extend_from_slice(&value.to_be_bytes::<32>());

        Log {
            inner: PrimitiveLog {
                address: receipt_contract,
                data: LogData::new_unchecked(topics, Bytes::from(data)),
            },
            block_hash: Some(b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    fn setup_cqrs()
    -> (Arc<CqrsFramework<ReceiptInventory, TestStore>>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    #[tokio::test]
    async fn backfill_discovers_receipt_from_historic_transfer_single() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, _store) = setup_cqrs();

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let transfer_log = create_transfer_single_log(
            receipt_contract,
            Address::ZERO,
            Address::ZERO,
            bot_wallet,
            receipt_id,
            balance,
            tx_hash,
            100,
        );

        let asserter = Asserter::new();

        // Mock eth_getLogs - returns the transfer event
        asserter.push_success(&vec![transfer_log]);

        // Mock balanceOf call - returns current balance
        let balance_hex =
            format!("0x{:0>64}", balance.to_string().trim_start_matches("0x"));
        asserter.push_success(&balance_hex);

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

        let result = backfiller.backfill_receipts().await.unwrap();

        assert_eq!(result.discovered_count, 1);
        assert_eq!(result.skipped_zero_balance, 0);
        assert_eq!(result.skipped_already_tracked, 0);
    }

    #[tokio::test]
    async fn backfill_is_idempotent_skips_already_discovered() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, _store) = setup_cqrs();

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        let transfer_log = create_transfer_single_log(
            receipt_contract,
            Address::ZERO,
            Address::ZERO,
            bot_wallet,
            receipt_id,
            balance,
            tx_hash,
            100,
        );

        // First run
        let asserter1 = Asserter::new();
        asserter1.push_success(&vec![transfer_log.clone()]);
        let balance_hex =
            format!("0x{:0>64}", balance.to_string().trim_start_matches("0x"));
        asserter1.push_success(&balance_hex);

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

        let result1 = backfiller1.backfill_receipts().await.unwrap();
        assert_eq!(result1.discovered_count, 1);

        // Second run - same receipt should be skipped
        let asserter2 = Asserter::new();
        asserter2.push_success(&vec![transfer_log]);
        asserter2.push_success(&balance_hex);

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

        let result2 = backfiller2.backfill_receipts().await.unwrap();

        assert_eq!(result2.discovered_count, 0);
        assert_eq!(result2.skipped_already_tracked, 1);
    }

    #[tokio::test]
    async fn backfill_skips_zero_balance_receipts() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, _store) = setup_cqrs();

        let receipt_id = U256::from(99);
        let transfer_amount = U256::from(500);
        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        let transfer_log = create_transfer_single_log(
            receipt_contract,
            Address::ZERO,
            Address::ZERO,
            bot_wallet,
            receipt_id,
            transfer_amount,
            tx_hash,
            200,
        );

        let asserter = Asserter::new();
        asserter.push_success(&vec![transfer_log]);

        // Current balance is zero (receipt was fully burned)
        let zero_balance = "0x0000000000000000000000000000000000000000000000000000000000000000";
        asserter.push_success(&zero_balance);

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

        let result = backfiller.backfill_receipts().await.unwrap();

        assert_eq!(result.discovered_count, 0);
        assert_eq!(result.skipped_zero_balance, 1);
        assert_eq!(result.skipped_already_tracked, 0);
    }

    #[tokio::test]
    async fn backfill_uses_current_onchain_balance_not_transfer_amount() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (cqrs, store) = setup_cqrs();

        let receipt_id = U256::from(77);
        let original_transfer_amount = U256::from(1000);
        let current_balance = U256::from(300); // Partially burned
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );

        let transfer_log = create_transfer_single_log(
            receipt_contract,
            Address::ZERO,
            Address::ZERO,
            bot_wallet,
            receipt_id,
            original_transfer_amount,
            tx_hash,
            300,
        );

        let asserter = Asserter::new();
        asserter.push_success(&vec![transfer_log]);

        // Return current balance (300), not original transfer amount (1000)
        let balance_hex = format!(
            "0x{:0>64}",
            current_balance.to_string().trim_start_matches("0x")
        );
        asserter.push_success(&balance_hex);

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

        let result = backfiller.backfill_receipts().await.unwrap();

        assert_eq!(result.discovered_count, 1);

        // Verify the aggregate has the current balance, not the transfer amount
        let ctx = store.load_aggregate(&vault.to_string()).await.unwrap();
        let receipts = ctx.aggregate().receipts_with_balance();

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].available_balance.inner(), current_balance);
    }
}
