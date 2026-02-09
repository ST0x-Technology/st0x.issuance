use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use async_trait::async_trait;

use super::{MintResult, ReceiptInformation, VaultError, VaultService};
use crate::bindings::OffchainAssetReceiptVault;

/// Alloy-based blockchain service that interacts with the Rain OffchainAssetReceiptVault
/// contract.
///
/// Generic over the provider type to support both production RPC providers and mock providers
/// for testing.
pub(crate) struct RealBlockchainService<P> {
    provider: P,
}

impl<P: Provider + Clone> RealBlockchainService<P> {
    /// Creates a new blockchain service instance.
    ///
    /// # Arguments
    ///
    /// * `provider` - Alloy provider for blockchain communication
    pub(crate) const fn new(provider: P) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P: Provider + Clone + Send + Sync + 'static> VaultService
    for RealBlockchainService<P>
{
    async fn mint_and_transfer_shares(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<MintResult, VaultError> {
        let receipt_info_bytes = receipt_info.encode()?;

        let vault_contract =
            OffchainAssetReceiptVault::new(vault, &self.provider);

        let share_ratio = U256::from(10).pow(U256::from(18));

        // Preview deposit to get the exact number shares that will be minted
        let shares =
            vault_contract.previewDeposit(assets, share_ratio).call().await?;

        // Encode deposit call - mints shares + receipts to bot
        let deposit_call = vault_contract
            .deposit(assets, bot, share_ratio, receipt_info_bytes)
            .calldata()
            .clone();

        // Encode transfer call - transfers exact shares from bot to user
        let transfer_call =
            vault_contract.transfer(user, shares).calldata().clone();

        // Execute both operations atomically via multicall
        let receipt = vault_contract
            .multicall(vec![deposit_call, transfer_call])
            .send()
            .await?
            .get_receipt()
            .await?;

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
                tx_hash: receipt.transaction_hash,
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

    async fn get_share_balance(
        &self,
        vault: Address,
        owner: Address,
    ) -> Result<U256, VaultError> {
        let vault_contract =
            OffchainAssetReceiptVault::new(vault, &self.provider);

        Ok(vault_contract.balanceOf(owner).call().await?)
    }

    async fn burn_multiple_receipts(
        &self,
        params: super::MultiBurnParams,
    ) -> Result<super::MultiBurnResult, VaultError> {
        let receipt_info_bytes = params.receipt_info.encode()?;

        let vault_contract =
            OffchainAssetReceiptVault::new(params.vault, &self.provider);

        // Build redeem calls for each burn
        let redeem_calls: Vec<Bytes> = params
            .burns
            .iter()
            .map(|burn| {
                vault_contract
                    .redeem(
                        burn.burn_shares,
                        params.user,
                        params.owner,
                        burn.receipt_id,
                        receipt_info_bytes.clone(),
                    )
                    .calldata()
                    .clone()
            })
            .collect();

        // Build multicall: all redeems, plus optional dust transfer
        let calls = if params.dust_shares > U256::ZERO {
            let transfer_call = vault_contract
                .transfer(params.user, params.dust_shares)
                .calldata()
                .clone();
            redeem_calls
                .into_iter()
                .chain(std::iter::once(transfer_call))
                .collect()
        } else {
            redeem_calls
        };

        let receipt =
            vault_contract.multicall(calls).send().await?.get_receipt().await?;

        // Parse all Withdraw events from the receipt
        let burns: Vec<super::MultiBurnResultEntry> = receipt
            .inner
            .logs()
            .iter()
            .filter_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Withdraw>()
                    .ok()
                    .map(|decoded| {
                        let event_data = decoded.data();
                        super::MultiBurnResultEntry {
                            receipt_id: event_data.id,
                            shares_burned: event_data.shares,
                        }
                    })
            })
            .collect();

        if burns.is_empty() {
            return Err(VaultError::EventNotFound {
                tx_hash: receipt.transaction_hash,
            });
        }

        let gas_used = receipt.gas_used;
        let block_number =
            receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

        Ok(super::MultiBurnResult {
            tx_hash: receipt.transaction_hash,
            burns,
            dust_returned: params.dust_shares,
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
        Address, B256, Bloom, Bytes, IntoLogData, U256, address, b256, bytes,
        fixed_bytes,
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
        MultiBurnEntry, MultiBurnParams, OperationType, ReceiptInformation,
        VaultError, VaultService,
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

    fn test_fee_history() -> FeeHistory {
        FeeHistory {
            base_fee_per_gas: vec![1_000_000_000],
            gas_used_ratio: vec![0.5],
            base_fee_per_blob_gas: vec![],
            blob_gas_used_ratio: vec![],
            oldest_block: 2000,
            reward: Some(vec![vec![10_000]]),
        }
    }

    fn setup_asserter_for_transaction(
        asserter: &Asserter,
        tx_hash: alloy::primitives::B256,
        receipt: &TransactionReceipt,
    ) {
        let block: Block<alloy::rpc::types::Transaction> = Block::default();
        asserter.push_success(&0u64); // eth_getTransactionCount
        asserter.push_success(&test_fee_history()); // eth_feeHistory
        asserter.push_success(&block); // eth_getBlockByNumber
        asserter.push_success(&1u64); // eth_chainId
        asserter.push_success(&100_000_u64); // eth_estimateGas
        asserter.push_success(&1_000_000_000_u64); // eth_maxPriorityFeePerGas
        asserter.push_success(&0u64); // eth_getTransactionCount again
        asserter.push_success(&tx_hash); // eth_sendRawTransaction
        asserter.push_success(receipt); // eth_getTransactionReceipt
        asserter.push_success(receipt); // eth_getTransactionReceipt (polling)
    }

    fn create_service_with_asserter(asserter: Asserter) -> impl VaultService {
        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter);
        RealBlockchainService::new(provider)
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

        // Mock previewDeposit call (eth_call returns shares as 32-byte hex string)
        asserter.push_success(&"0x00000000000000000000000000000000000000000000000000000000000003e8");

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
        let service = RealBlockchainService::new(provider);

        let result = service
            .mint_and_transfer_shares(
                vault_address,
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
        let vault_address = test_vault_address();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x2222222222222222222222222222222222222222");
        let receipt_info = test_receipt_info();

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

        // Mock previewDeposit call (eth_call returns shares as 32-byte hex string)
        asserter.push_success(&"0x00000000000000000000000000000000000000000000000000000000000003e8");

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
        let service = RealBlockchainService::new(provider);

        let result = service
            .mint_and_transfer_shares(
                vault_address,
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

    fn create_empty_receipt(
        vault_address: Address,
        tx_hash: alloy::primitives::B256,
    ) -> TransactionReceipt {
        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 0x6100,
            logs: vec![],
        };

        TransactionReceipt {
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
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                consensus_receipt,
                Bloom::default(),
            )),
        }
    }

    #[tokio::test]
    async fn test_mint_and_transfer_shares_produces_exact_calldata() {
        let vault_address = test_vault_address();
        let assets = U256::from(1000);
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let user = address!("0x2222222222222222222222222222222222222222");

        let fixed_timestamp =
            chrono::DateTime::parse_from_rfc3339("2025-01-15T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc);

        let receipt_info = ReceiptInformation {
            tokenization_request_id: TokenizationRequestId::new("tok-123"),
            issuer_request_id: IssuerRequestId::new("iss-456"),
            underlying: UnderlyingSymbol::new("AAPL"),
            quantity: Quantity::new(Decimal::from(100)),
            operation_type: OperationType::Mint,
            timestamp: fixed_timestamp,
            notes: None,
        };

        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect_anvil();

        let vault = OffchainAssetReceiptVault::new(vault_address, &provider);

        let receipt_info_bytes = receipt_info.encode().unwrap();
        let share_ratio = U256::from(10).pow(U256::from(18));

        let deposit_call = vault
            .deposit(assets, bot, share_ratio, receipt_info_bytes)
            .calldata()
            .clone();

        let transfer_call = vault.transfer(user, assets).calldata().clone();

        let multicall_calldata = vault
            .multicall(vec![deposit_call.clone(), transfer_call.clone()])
            .calldata()
            .clone();

        let expected_transfer = bytes!(
            "0xa9059cbb000000000000000000000000222222222222222222222222222222222222222200000000000000000000000000000000000000000000000000000000000003e8"
        );

        assert_eq!(
            transfer_call.as_ref(),
            expected_transfer.as_ref(),
            "Transfer calldata mismatch"
        );

        assert!(
            !deposit_call.is_empty()
                && deposit_call[0..4] == [0x14, 0x23, 0xfe, 0xba],
            "Deposit should start with correct function selector"
        );

        assert!(
            !multicall_calldata.is_empty()
                && multicall_calldata[0..4] == [0xac, 0x96, 0x50, 0xd8],
            "Multicall should start with correct function selector"
        );

        assert!(
            multicall_calldata.len() > deposit_call.len() + transfer_call.len(),
            "Multicall should contain both calls"
        );
    }

    fn create_multi_withdraw_receipt(
        vault_address: Address,
        tx_hash: B256,
        owner: Address,
        user: Address,
        burns: Vec<(U256, U256)>, // (receipt_id, shares)
    ) -> TransactionReceipt {
        let logs: Vec<alloy::rpc::types::Log> = burns
            .into_iter()
            .enumerate()
            .map(|(i, (receipt_id, shares))| {
                let withdraw_event = OffchainAssetReceiptVault::Withdraw {
                    sender: owner,
                    receiver: user,
                    owner,
                    assets: shares,
                    shares,
                    id: receipt_id,
                    receiptInformation: Bytes::new(),
                };

                alloy::rpc::types::Log {
                    inner: alloy::primitives::Log {
                        address: vault_address,
                        data: withdraw_event.into_log_data(),
                    },
                    block_hash: Some(b256!(
                        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    )),
                    block_number: Some(0x9c4),
                    block_timestamp: None,
                    transaction_hash: Some(tx_hash),
                    transaction_index: Some(0),
                    log_index: Some(i as u64),
                    removed: false,
                }
            })
            .collect();

        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 0x8000,
            logs,
        };

        TransactionReceipt {
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: Some(fixed_bytes!(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            )),
            block_number: Some(0x9c4),
            from: address!("2222222222222222222222222222222222222222"),
            to: Some(vault_address),
            gas_used: 0x8000,
            effective_gas_price: 0x3b9a_ca00,
            contract_address: None,
            blob_gas_used: None,
            blob_gas_price: None,
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                consensus_receipt,
                Bloom::default(),
            )),
        }
    }

    #[tokio::test]
    async fn test_burn_multiple_receipts_with_two_burns() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x3333333333333333333333333333333333333333");
        let mut receipt_info = test_receipt_info();
        receipt_info.operation_type = OperationType::Redeem;

        let tx_hash = fixed_bytes!(
            "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );

        let burns = vec![
            (U256::from(1), U256::from(100)),
            (U256::from(2), U256::from(50)),
        ];

        let receipt = create_multi_withdraw_receipt(
            vault_address,
            tx_hash,
            owner,
            user,
            burns.clone(),
        );

        let asserter = Asserter::new();
        setup_asserter_for_transaction(&asserter, tx_hash, &receipt);

        let service = create_service_with_asserter(asserter);

        let result = service
            .burn_multiple_receipts(MultiBurnParams {
                vault: vault_address,
                burns: burns
                    .iter()
                    .map(|(receipt_id, burn_shares)| MultiBurnEntry {
                        receipt_id: *receipt_id,
                        burn_shares: *burn_shares,
                    })
                    .collect(),
                dust_shares: U256::ZERO,
                owner,
                user,
                receipt_info,
            })
            .await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let multi_result = result.unwrap();

        assert_eq!(multi_result.tx_hash, tx_hash);
        assert_eq!(multi_result.burns.len(), 2, "Should have 2 burn results");
        assert_eq!(multi_result.burns[0].receipt_id, U256::from(1));
        assert_eq!(multi_result.burns[0].shares_burned, U256::from(100));
        assert_eq!(multi_result.burns[1].receipt_id, U256::from(2));
        assert_eq!(multi_result.burns[1].shares_burned, U256::from(50));
        assert_eq!(multi_result.dust_returned, U256::ZERO);
        assert_eq!(multi_result.gas_used, 0x8000);
        assert_eq!(multi_result.block_number, 0x9c4);
    }

    #[tokio::test]
    async fn test_burn_multiple_receipts_with_dust_transfer() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x4444444444444444444444444444444444444444");
        let mut receipt_info = test_receipt_info();
        receipt_info.operation_type = OperationType::Redeem;

        let tx_hash = fixed_bytes!(
            "0xfeedface feedface feedface feedface feedface feedface feedface feedface"
        );

        let burns = vec![(U256::from(42), U256::from(1000))];
        let dust_shares = U256::from(123);

        let receipt = create_multi_withdraw_receipt(
            vault_address,
            tx_hash,
            owner,
            user,
            burns.clone(),
        );

        let asserter = Asserter::new();
        setup_asserter_for_transaction(&asserter, tx_hash, &receipt);

        let service = create_service_with_asserter(asserter);

        let result = service
            .burn_multiple_receipts(MultiBurnParams {
                vault: vault_address,
                burns: burns
                    .iter()
                    .map(|(receipt_id, burn_shares)| MultiBurnEntry {
                        receipt_id: *receipt_id,
                        burn_shares: *burn_shares,
                    })
                    .collect(),
                dust_shares,
                owner,
                user,
                receipt_info,
            })
            .await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let multi_result = result.unwrap();

        assert_eq!(
            multi_result.dust_returned, dust_shares,
            "Dust should be returned"
        );
        assert_eq!(multi_result.burns.len(), 1);
    }

    #[tokio::test]
    async fn test_burn_multiple_receipts_parses_three_withdraw_events() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x5555555555555555555555555555555555555555");
        let mut receipt_info = test_receipt_info();
        receipt_info.operation_type = OperationType::Redeem;

        let tx_hash = fixed_bytes!(
            "0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"
        );

        let burns = vec![
            (U256::from(10), U256::from(500)),
            (U256::from(20), U256::from(300)),
            (U256::from(30), U256::from(200)),
        ];

        let receipt = create_multi_withdraw_receipt(
            vault_address,
            tx_hash,
            owner,
            user,
            burns.clone(),
        );

        let asserter = Asserter::new();
        setup_asserter_for_transaction(&asserter, tx_hash, &receipt);

        let service = create_service_with_asserter(asserter);

        let result = service
            .burn_multiple_receipts(MultiBurnParams {
                vault: vault_address,
                burns: burns
                    .iter()
                    .map(|(receipt_id, burn_shares)| MultiBurnEntry {
                        receipt_id: *receipt_id,
                        burn_shares: *burn_shares,
                    })
                    .collect(),
                dust_shares: U256::ZERO,
                owner,
                user,
                receipt_info,
            })
            .await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let multi_result = result.unwrap();

        assert_eq!(
            multi_result.burns.len(),
            3,
            "Should parse all 3 Withdraw events"
        );

        // Verify each burn was parsed correctly
        for (i, (expected_id, expected_shares)) in burns.iter().enumerate() {
            assert_eq!(
                multi_result.burns[i].receipt_id, *expected_id,
                "Burn {i} receipt_id mismatch"
            );
            assert_eq!(
                multi_result.burns[i].shares_burned, *expected_shares,
                "Burn {i} shares_burned mismatch"
            );
        }
    }

    #[tokio::test]
    async fn test_burn_multiple_receipts_returns_error_on_missing_events() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x6666666666666666666666666666666666666666");
        let mut receipt_info = test_receipt_info();
        receipt_info.operation_type = OperationType::Redeem;

        let tx_hash = fixed_bytes!(
            "0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff"
        );

        // Empty receipt with no Withdraw events - simulates failed transaction
        let receipt = create_empty_receipt(vault_address, tx_hash);

        let asserter = Asserter::new();
        setup_asserter_for_transaction(&asserter, tx_hash, &receipt);

        let service = create_service_with_asserter(asserter);

        let result = service
            .burn_multiple_receipts(MultiBurnParams {
                vault: vault_address,
                burns: vec![
                    MultiBurnEntry {
                        receipt_id: U256::from(1),
                        burn_shares: U256::from(100),
                    },
                    MultiBurnEntry {
                        receipt_id: U256::from(2),
                        burn_shares: U256::from(200),
                    },
                ],
                dust_shares: U256::ZERO,
                owner,
                user,
                receipt_info,
            })
            .await;

        assert!(result.is_err(), "Expected Err but got Ok: {result:?}");
        let err = result.unwrap_err();
        assert!(
            matches!(err, VaultError::EventNotFound { .. }),
            "Expected EventNotFound but got: {err:?}"
        );
    }
}
