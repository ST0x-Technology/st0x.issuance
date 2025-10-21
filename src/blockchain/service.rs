use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use async_trait::async_trait;

use super::{
    BlockchainError, BlockchainService, MintResult, ReceiptInformation,
};
use crate::bindings::OffchainAssetReceiptVault;

/// Alloy-based blockchain service that interacts with the Rain OffchainAssetReceiptVault
/// contract.
///
/// Generic over the provider type to support both production RPC providers and mock providers
/// for testing.
pub(crate) struct RealBlockchainService<P> {
    provider: P,
    vault_address: Address,
}

impl<P: Provider + Clone> RealBlockchainService<P> {
    /// Creates a new blockchain service instance.
    ///
    /// # Arguments
    ///
    /// * `provider` - Alloy provider for blockchain communication
    /// * `vault_address` - Address of the OffchainAssetReceiptVault contract
    pub(crate) const fn new(provider: P, vault_address: Address) -> Self {
        Self { provider, vault_address }
    }
}

#[async_trait]
impl<P: Provider + Clone + Send + Sync + 'static> BlockchainService
    for RealBlockchainService<P>
{
    async fn mint_tokens(
        &self,
        assets: U256,
        receiver: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<MintResult, BlockchainError> {
        let receipt_info_bytes =
            Bytes::from(serde_json::to_vec(&receipt_info).map_err(|e| {
                BlockchainError::RpcError {
                    message: format!(
                        "Failed to encode receipt information: {e}"
                    ),
                }
            })?);

        let vault =
            OffchainAssetReceiptVault::new(self.vault_address, &self.provider);

        // The third parameter to deposit() is depositMinShareRatio (an 18-decimal fixed-point number).
        // OffchainAssetReceiptVault inherits from ReceiptVault (OffchainAssetReceiptVault.sol:243).
        // The deposit() method is implemented in ReceiptVault.sol (lines 224-238).
        // It calls _calculateDeposit() (line 233) which uses the formula (line 440):
        //   shares = assets.fixedPointMul(shareRatio, Math.Rounding.Down)
        //   where shareRatio comes from _shareRatio() which defaults to 1e18 (line 305).
        // For 1:1 (one share per asset): depositMinShareRatio = 1e18.
        let share_ratio = U256::from(10).pow(U256::from(18));

        let receipt = vault
            .deposit(assets, receiver, share_ratio, receipt_info_bytes)
            .send()
            .await
            .map_err(|e| BlockchainError::TransactionFailed {
                reason: format!("Failed to send transaction: {e}"),
            })?
            .get_receipt()
            .await
            .map_err(|e| BlockchainError::RpcError {
                message: format!("Failed to get transaction receipt: {e}"),
            })?;

        let (receipt_id, shares_minted) = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Deposit>().ok().map(
                    |decoded| {
                        let event_data = decoded.data();
                        (event_data.id, event_data.shares)
                    },
                )
            })
            .ok_or_else(|| BlockchainError::EventNotFound {
                tx_hash: format!("{:?}", receipt.transaction_hash),
            })?;

        let gas_used = receipt.gas_used;
        let block_number =
            receipt.block_number.ok_or(BlockchainError::InvalidReceipt)?;

        Ok(MintResult {
            tx_hash: receipt.transaction_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{
        Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
    };
    use alloy::network::EthereumWallet;
    use alloy::primitives::{
        Address, Bloom, Bytes, IntoLogData, U256, address, fixed_bytes,
    };
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::{Block, FeeHistory, TransactionReceipt};
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use rust_decimal::Decimal;

    use crate::bindings::OffchainAssetReceiptVault;
    use crate::blockchain::{
        BlockchainService, OperationType, ReceiptInformation,
    };
    use crate::mint::{
        IssuerRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
    };

    use super::RealBlockchainService;

    fn test_receipt_info() -> ReceiptInformation {
        ReceiptInformation {
            tokenization_request_id: TokenizationRequestId::new("tok-123"),
            issuer_request_id: IssuerRequestId::new("iss-456"),
            underlying: UnderlyingSymbol::new("AAPL"),
            quantity: Quantity::new(Decimal::from(100)),
            operation_type: OperationType::Mint,
            timestamp: Utc::now(),
            notes: None,
        }
    }

    fn test_receiver() -> Address {
        address!("0000000000000000000000000000000000000001")
    }

    fn test_vault_address() -> Address {
        address!("0000000000000000000000000000000000000002")
    }

    #[tokio::test]
    async fn test_mint_tokens_success() {
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();
        let vault_address = test_vault_address();

        let tx_hash = fixed_bytes!(
            "0x1234567890123456789012345678901234567890123456789012345678901234"
        );
        let receipt_id = U256::from(42);
        let shares = U256::from(1000);

        let deposit_event = OffchainAssetReceiptVault::Deposit {
            sender: receiver,
            owner: receiver,
            assets,
            shares,
            id: receipt_id,
            receiptInformation: Bytes::new(),
        };

        let log_data = deposit_event.into_log_data();

        let log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: vault_address,
                data: log_data,
            },
            block_hash: Some(fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )),
            block_number: Some(0x3e8),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        };

        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 0x5208,
            logs: vec![log],
        };

        let receipt_with_bloom =
            ReceiptWithBloom::new(consensus_receipt, Bloom::default());

        let receipt = TransactionReceipt {
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: Some(fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )),
            block_number: Some(0x3e8),
            from: address!("1111111111111111111111111111111111111111"),
            to: Some(vault_address),
            gas_used: 0x5208,
            effective_gas_price: 0x3b9a_ca00,
            contract_address: None,
            blob_gas_used: None,
            blob_gas_price: None,
            inner: ReceiptEnvelope::Eip1559(receipt_with_bloom),
        };

        let fee_history = FeeHistory {
            base_fee_per_gas: vec![1_000_000_000],
            gas_used_ratio: vec![0.5],
            base_fee_per_blob_gas: vec![],
            blob_gas_used_ratio: vec![],
            oldest_block: 1000,
            reward: Some(vec![vec![10_000]]),
        };

        let block: Block = Block::default();

        let asserter = Asserter::new();

        // Mock eth_getTransactionCount (get nonce)
        asserter.push_success(&0u64);

        // Mock eth_feeHistory
        asserter.push_success(&fee_history);

        // Mock eth_getBlockByNumber (get latest block)
        asserter.push_success(&block);

        // Mock eth_chainId
        asserter.push_success(&1u64);

        // Mock eth_estimateGas
        asserter.push_success(&100_000_u64);

        // Mock eth_maxPriorityFeePerGas or another u64 call
        asserter.push_success(&1_000_000_000_u64);

        // Mock eth_getTransactionCount again (wallet's nonce manager)
        asserter.push_success(&0u64);

        // Mock eth_sendRawTransaction (returns pending tx hash)
        asserter.push_success(&tx_hash);

        // Mock eth_getTransactionReceipt for .get_receipt() polling (may poll multiple times)
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);

        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter);
        let service = RealBlockchainService::new(provider, vault_address);

        let result = service.mint_tokens(assets, receiver, receipt_info).await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let mint_result = result.unwrap();
        assert_eq!(mint_result.tx_hash, tx_hash);
        assert_eq!(mint_result.receipt_id, receipt_id);
        assert_eq!(mint_result.shares_minted, shares);
        assert_eq!(mint_result.gas_used, 0x5208);
        assert_eq!(mint_result.block_number, 0x3e8);
    }

    #[tokio::test]
    async fn test_mint_tokens_missing_deposit_event() {
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();
        let vault_address = test_vault_address();

        let tx_hash = fixed_bytes!(
            "0x1234567890123456789012345678901234567890123456789012345678901234"
        );

        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 0x5208,
            logs: vec![],
        };

        let receipt_with_bloom =
            ReceiptWithBloom::new(consensus_receipt, Bloom::default());

        let receipt = TransactionReceipt {
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: Some(fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )),
            block_number: Some(0x3e8),
            from: address!("1111111111111111111111111111111111111111"),
            to: Some(vault_address),
            gas_used: 0x5208,
            effective_gas_price: 0x3b9a_ca00,
            contract_address: None,
            blob_gas_used: None,
            blob_gas_price: None,
            inner: ReceiptEnvelope::Eip1559(receipt_with_bloom),
        };

        let fee_history = FeeHistory {
            base_fee_per_gas: vec![1_000_000_000],
            gas_used_ratio: vec![0.5],
            base_fee_per_blob_gas: vec![],
            blob_gas_used_ratio: vec![],
            oldest_block: 1000,
            reward: Some(vec![vec![10_000]]),
        };

        let block: Block = Block::default();

        let asserter = Asserter::new();

        // Mock eth_getTransactionCount (get nonce)
        asserter.push_success(&0u64);

        // Mock eth_feeHistory
        asserter.push_success(&fee_history);

        // Mock eth_getBlockByNumber (get latest block)
        asserter.push_success(&block);

        // Mock eth_chainId
        asserter.push_success(&1u64);

        // Mock eth_estimateGas
        asserter.push_success(&100_000_u64);

        // Mock eth_maxPriorityFeePerGas or another u64 call
        asserter.push_success(&1_000_000_000_u64);

        // Mock eth_getTransactionCount again (wallet's nonce manager)
        asserter.push_success(&0u64);

        // Mock eth_sendRawTransaction (returns pending tx hash)
        asserter.push_success(&tx_hash);

        // Mock eth_getTransactionReceipt for .get_receipt() polling (may poll multiple times)
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);

        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter);
        let service = RealBlockchainService::new(provider, vault_address);

        let result = service.mint_tokens(assets, receiver, receipt_info).await;

        assert!(result.is_err(), "Expected Err but got Ok: {result:?}");
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                crate::blockchain::BlockchainError::EventNotFound { .. }
            ),
            "Expected EventNotFound but got: {err:?}"
        );
    }
}
