use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use async_trait::async_trait;

use super::{
    BurnResult, MintResult, ReceiptInformation, VaultError, VaultService,
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
impl<P: Provider + Clone + Send + Sync + 'static> VaultService
    for RealBlockchainService<P>
{
    async fn mint_and_transfer_shares(
        &self,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<MintResult, VaultError> {
        let receipt_info_bytes =
            Bytes::from(serde_json::to_vec(&receipt_info).map_err(|e| {
                VaultError::RpcError {
                    message: format!(
                        "Failed to encode receipt information: {e}"
                    ),
                }
            })?);

        let vault =
            OffchainAssetReceiptVault::new(self.vault_address, &self.provider);

        let share_ratio = U256::from(10).pow(U256::from(18));

        // Encode deposit call - mints shares + receipts to bot
        let deposit_call = vault
            .deposit(assets, bot, share_ratio, receipt_info_bytes)
            .calldata()
            .clone();

        // Encode transfer call - transfers shares from bot to user
        // Due to 1:1 ratio (1e18), shares_minted == assets, so we can pre-calculate
        let transfer_call = vault.transfer(user, assets).calldata().clone();

        // Execute both operations atomically via multicall
        let receipt = vault
            .multicall(vec![deposit_call, transfer_call])
            .send()
            .await
            .map_err(|e| VaultError::TransactionFailed {
                reason: format!("Failed to send multicall transaction: {e}"),
            })?
            .get_receipt()
            .await
            .map_err(|e| VaultError::RpcError {
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
            .ok_or_else(|| VaultError::EventNotFound {
                tx_hash: format!("{:?}", receipt.transaction_hash),
            })?;

        let gas_used = receipt.gas_used;
        let block_number =
            receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

        Ok(MintResult {
            tx_hash: receipt.transaction_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
        })
    }

    async fn burn_tokens(
        &self,
        shares: U256,
        receipt_id: U256,
        owner: Address,
        receiver: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<BurnResult, VaultError> {
        let receipt_info_bytes =
            Bytes::from(serde_json::to_vec(&receipt_info).map_err(|e| {
                VaultError::RpcError {
                    message: format!(
                        "Failed to encode receipt information: {e}"
                    ),
                }
            })?);

        let vault =
            OffchainAssetReceiptVault::new(self.vault_address, &self.provider);

        // The vault's redeem() function burns shares and returns underlying assets to receiver.
        // Parameters: shares to burn, receiver address, owner address, receipt ID, metadata.
        // The owner parameter specifies whose shares are being burned, while receiver gets the assets.
        let receipt = vault
            .redeem(shares, receiver, owner, receipt_id, receipt_info_bytes)
            .send()
            .await
            .map_err(|e| VaultError::TransactionFailed {
                reason: format!("Failed to send transaction: {e}"),
            })?
            .get_receipt()
            .await
            .map_err(|e| VaultError::RpcError {
                message: format!("Failed to get transaction receipt: {e}"),
            })?;

        let (parsed_receipt_id, shares_burned) = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Withdraw>()
                    .ok()
                    .map(|decoded| {
                        let event_data = decoded.data();
                        (event_data.id, event_data.shares)
                    })
            })
            .ok_or_else(|| VaultError::EventNotFound {
                tx_hash: format!("{:?}", receipt.transaction_hash),
            })?;

        let gas_used = receipt.gas_used;
        let block_number =
            receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

        Ok(BurnResult {
            tx_hash: receipt.transaction_hash,
            receipt_id: parsed_receipt_id,
            shares_burned,
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

    use super::RealBlockchainService;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::mint::{
        IssuerRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::vault::{
        OperationType, ReceiptInformation, VaultError, VaultService,
    };

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
    async fn test_mint_and_transfer_shares_success() {
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x2222222222222222222222222222222222222222");
        let receipt_info = test_receipt_info();
        let vault_address = test_vault_address();

        let tx_hash = fixed_bytes!(
            "0x1234567890123456789012345678901234567890123456789012345678901234"
        );
        let receipt_id = U256::from(42);
        let shares = U256::from(1000);

        let deposit_event = OffchainAssetReceiptVault::Deposit {
            sender: bot_wallet,
            owner: bot_wallet,
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

        let result = service
            .mint_and_transfer_shares(
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
            )
            .await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let mint_result = result.unwrap();
        assert_eq!(mint_result.tx_hash, tx_hash);
        assert_eq!(mint_result.receipt_id, receipt_id);
        assert_eq!(mint_result.shares_minted, shares);
        assert_eq!(mint_result.gas_used, 0x5208);
        assert_eq!(mint_result.block_number, 0x3e8);
    }

    #[tokio::test]
    async fn test_mint_and_transfer_shares_missing_deposit_event() {
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x2222222222222222222222222222222222222222");
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

        let result = service
            .mint_and_transfer_shares(
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
            )
            .await;

        assert!(result.is_err(), "Expected Err but got Ok: {result:?}");
        let err = result.unwrap_err();
        assert!(
            matches!(err, VaultError::EventNotFound { .. }),
            "Expected EventNotFound but got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_burn_tokens_success() {
        let shares = U256::from(500);
        let receipt_id = U256::from(42);
        let receiver = test_receiver();
        let mut receipt_info = test_receipt_info();
        receipt_info.operation_type = OperationType::Redeem;
        let vault_address = test_vault_address();

        let tx_hash = fixed_bytes!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let withdraw_event = OffchainAssetReceiptVault::Withdraw {
            sender: receiver,
            receiver,
            owner: receiver,
            assets: shares,
            shares,
            id: receipt_id,
            receiptInformation: Bytes::new(),
        };

        let log_data = withdraw_event.into_log_data();

        let log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: vault_address,
                data: log_data,
            },
            block_hash: Some(fixed_bytes!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            )),
            block_number: Some(0x7d0),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        };

        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 0x6100,
            logs: vec![log],
        };

        let receipt_with_bloom =
            ReceiptWithBloom::new(consensus_receipt, Bloom::default());

        let receipt = TransactionReceipt {
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: Some(fixed_bytes!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            )),
            block_number: Some(0x7d0),
            from: address!("2222222222222222222222222222222222222222"),
            to: Some(vault_address),
            gas_used: 0x6100,
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
            oldest_block: 2000,
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

        let result = service
            .burn_tokens(shares, receipt_id, receiver, receiver, receipt_info)
            .await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let burn_result = result.unwrap();
        assert_eq!(burn_result.tx_hash, tx_hash);
        assert_eq!(burn_result.receipt_id, receipt_id);
        assert_eq!(burn_result.shares_burned, shares);
        assert_eq!(burn_result.gas_used, 0x6100);
        assert_eq!(burn_result.block_number, 0x7d0);
    }

    #[tokio::test]
    async fn test_burn_tokens_missing_withdraw_event() {
        let shares = U256::from(500);
        let receipt_id = U256::from(42);
        let owner = test_receiver();
        let receiver = test_receiver();
        let mut receipt_info = test_receipt_info();
        receipt_info.operation_type = OperationType::Redeem;
        let vault_address = test_vault_address();

        let tx_hash = fixed_bytes!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 0x6100,
            logs: vec![],
        };

        let receipt_with_bloom =
            ReceiptWithBloom::new(consensus_receipt, Bloom::default());

        let receipt = TransactionReceipt {
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: Some(fixed_bytes!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            )),
            block_number: Some(0x7d0),
            from: address!("2222222222222222222222222222222222222222"),
            to: Some(vault_address),
            gas_used: 0x6100,
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
            oldest_block: 2000,
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

        let result = service
            .burn_tokens(shares, receipt_id, owner, receiver, receipt_info)
            .await;

        assert!(result.is_err(), "Expected Err but got Ok: {result:?}");
        let err = result.unwrap_err();
        assert!(
            matches!(err, VaultError::EventNotFound { .. }),
            "Expected EventNotFound but got: {err:?}"
        );
    }
}
