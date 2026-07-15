use alloy::consensus::Transaction;
use alloy::eips::Encodable2718;
use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill,
    NonceFiller, SimpleNonceManager, WalletFiller,
};
use alloy::providers::{
    Identity, PendingTransactionBuilder, Provider, RootProvider,
};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::debug;

use super::rain_meta::OaSchemaCache;
use super::{
    BurnVerification, MintResult, MultiBurnResult, MultiBurnResultEntry,
    ReceiptInformation, SendableTxWithHash, SubmittedTx, TxId, VaultError,
    VaultService, WalletNonceGuard, verify_burn_in_receipt,
};
use crate::bindings::OffchainAssetReceiptVault;
use crate::redemption::BurnExternalTxId;

pub type RealBlockchainServiceProvider = FillProvider<
    JoinFill<
        JoinFill<
            JoinFill<
                JoinFill<JoinFill<Identity, GasFiller>, BlobGasFiller>,
                NonceFiller<SimpleNonceManager>,
            >,
            ChainIdFiller,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider,
>;

/// Alloy-based blockchain service that interacts with the Rain OffchainAssetReceiptVault
/// contract.
///
/// Generic over the provider type to support both production RPC providers and mock providers
/// for testing.
///
/// **Crash-safe idempotency:**
/// - **Mints**: handled at the application layer — the receipt backfiller runs at
///   startup before recovery, scans the chain for `Deposit` events, and populates
///   the receipt inventory by `issuer_request_id`. `recover_from_existing_receipt`
///   finds the existing deposit and emits `ExistingMintRecovered`, preventing a
///   double-mint.
/// - **Burns**: through `prepare_burn_tx()` which prepares a signed transaction
///   whose hash commits to the complete encoded transaction, including its
///   assigned nonce. On startup recovery, the exact persisted transaction is
///   re-broadcast before `confirm_burn` polls for its receipt, so a crash
///   between persistence, submission, and confirmation cannot cause a
///   replacement transaction to double-burn.
pub(crate) struct RealBlockchainService {
    provider: RealBlockchainServiceProvider,
    oa_schema_cache: Arc<OaSchemaCache>,
    wallet_nonce_lock: Arc<Mutex<()>>,
}

impl RealBlockchainService {
    /// Creates a new blockchain service instance.
    ///
    /// # Arguments
    ///
    /// * `provider` - Alloy provider for blockchain communication
    /// * `oa_schema_cache` - Cache for querying OA schema hashes from the subgraph
    pub(crate) fn new(
        provider: RealBlockchainServiceProvider,
        oa_schema_cache: Arc<OaSchemaCache>,
    ) -> Self {
        Self {
            provider,
            oa_schema_cache,
            wallet_nonce_lock: Arc::new(Mutex::new(())),
        }
    }
}

#[async_trait]
impl VaultService for RealBlockchainService {
    async fn submit_mint(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
        external_tx_id: Option<String>,
    ) -> Result<SubmittedTx, VaultError> {
        let oa_schema = self.oa_schema_cache.get(vault).await;
        let receipt_info_bytes = receipt_info.encode(oa_schema.as_deref())?;

        let vault_contract =
            OffchainAssetReceiptVault::new(vault, &self.provider);

        let share_ratio = U256::from(10).pow(U256::from(18));

        // Preview deposit to get the exact number shares that will be minted
        let shares =
            vault_contract.previewDeposit(assets, share_ratio).call().await?;

        // Encode deposit call - mints shares + receipts to bot
        let deposit_call = vault_contract
            .deposit(assets, bot, share_ratio, receipt_info_bytes.clone())
            .calldata()
            .clone();

        // Encode transfer call - transfers exact shares from bot to user
        let transfer_call =
            vault_contract.transfer(user, shares).calldata().clone();

        // Execute both operations atomically via multicall
        let wallet_guard = self.wallet_nonce_lock.clone().lock_owned().await;
        let pending_tx = vault_contract
            .multicall(vec![deposit_call, transfer_call])
            .send()
            .await?;
        drop(wallet_guard);

        let tx_id = TxId::from(*pending_tx.tx_hash());

        Ok(SubmittedTx {
            external_tx_id: external_tx_id.unwrap_or_else(|| {
                format!("mint-{}", receipt_info.issuer_request_id)
            }),
            tx_id,
        })
    }

    async fn confirm_mint(
        &self,
        tx_id: &TxId,
    ) -> Result<MintResult, VaultError> {
        // Fetch receipt from chain using tx hash
        debug!(target: "vault", tx_hash = %tx_id,
            "Getting mint tx from chain"
        );

        let tx_hash = tx_id.to_hash().ok_or(VaultError::InvalidReceipt)?;

        let receipt = PendingTransactionBuilder::new(
            self.provider.root().clone(),
            tx_hash,
        )
        .with_timeout(Some(Duration::from_secs(120)))
        .get_receipt()
        .await?;

        // A mined-but-reverted burn consumes no receipts, so it is a definitive
        // failure distinct from an anomalous missing-Withdraw parse error.
        if !receipt.status() {
            return Err(VaultError::Reverted { tx_hash });
        }

        let (receipt_id, shares_minted, receipt_info_bytes) = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Deposit>().ok().map(
                    |decoded| {
                        let event_data = decoded.data();
                        (
                            event_data.id,
                            event_data.shares,
                            event_data.receiptInformation.clone(),
                        )
                    },
                )
            })
            .ok_or_else(|| VaultError::EventNotFound { tx_hash })?;

        Ok(MintResult {
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used: receipt.gas_used,
            block_number: receipt
                .block_number
                .ok_or(VaultError::InvalidReceipt)?,
            receipt_info_bytes,
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

    async fn submit_burn(
        &self,
        params: super::MultiBurnParams,
        sendable_tx: SendableTxWithHash,
    ) -> Result<SubmittedTx, VaultError> {
        if let Err(error) =
            self.provider.send_raw_transaction(&sendable_tx.tx).await
        {
            if self
                .provider
                .get_transaction_by_hash(sendable_tx.hash)
                .await?
                .is_none()
            {
                return Err(error.into());
            }

            debug!(target: "vault", tx_hash = %sendable_tx.hash,
                "Burn broadcast errored but the node holds the persisted transaction"
            );
        }

        Ok(SubmittedTx {
            external_tx_id: params
                .external_tx_id
                .unwrap_or_else(|| {
                    BurnExternalTxId::base(&params.detected_tx_hash)
                })
                .into_string(),
            tx_id: sendable_tx.hash.into(),
        })
    }

    async fn confirm_burn(
        &self,
        tx_id: &TxId,
        dust_shares: U256,
    ) -> Result<MultiBurnResult, VaultError> {
        // Fetch receipt from chain using tx hash
        debug!(target: "vault", tx_hash = %tx_id,
            "Getting burn tx data from chain"
        );

        let tx_hash = tx_id.to_hash().ok_or(VaultError::InvalidReceipt)?;

        let receipt = PendingTransactionBuilder::new(
            self.provider.root().clone(),
            tx_hash,
        )
        .with_timeout(Some(Duration::from_secs(120)))
        .get_receipt()
        .await
        .map_err(|error| VaultError::ConfirmationPending {
            tx_id: TxId::Hash(tx_hash),
            message: error.to_string(),
        })?;

        // A mined-but-reverted burn consumes no receipts, so it is a definitive
        // failure distinct from an anomalous missing-Withdraw parse error.
        if !receipt.status() {
            return Err(VaultError::Reverted { tx_hash });
        }

        let burns: Vec<MultiBurnResultEntry> = receipt
            .inner
            .logs()
            .iter()
            .filter_map(|log| {
                log.log_decode::<OffchainAssetReceiptVault::Withdraw>()
                    .ok()
                    .map(|decoded| {
                        let event_data = decoded.data();
                        MultiBurnResultEntry {
                            receipt_id: event_data.id,
                            shares_burned: event_data.shares,
                        }
                    })
            })
            .collect();

        if burns.is_empty() {
            return Err(VaultError::EventNotFound { tx_hash });
        }

        Ok(MultiBurnResult {
            tx_hash,
            burns,
            dust_returned: dust_shares,
            gas_used: receipt.gas_used,
            block_number: receipt
                .block_number
                .ok_or(VaultError::InvalidReceipt)?,
        })
    }

    async fn verify_burn_tx(
        &self,
        vault: Address,
        owner: Address,
        tx_hash: B256,
    ) -> Result<BurnVerification, VaultError> {
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or(VaultError::InvalidReceipt)?;

        verify_burn_in_receipt(&receipt, vault, owner, tx_hash)
    }

    /// Prepares a signed tx that can be sent on-chain via eth_sendRawTransaction
    async fn prepare_burn_tx(
        &self,
        params: &super::MultiBurnParams,
    ) -> Result<SendableTxWithHash, VaultError> {
        let vault_contract =
            OffchainAssetReceiptVault::new(params.vault, &self.provider);

        let needs_encoding = params.burns.iter().any(|burn| {
            burn.receipt_info_bytes.is_none() && burn.receipt_info.is_some()
        });

        let oa_schema = if needs_encoding {
            self.oa_schema_cache.get(params.vault).await
        } else {
            None
        };

        let redeem_calls: Vec<Bytes> = params
            .burns
            .iter()
            .map(|burn| {
                let receipt_bytes = if let Some(raw) = &burn.receipt_info_bytes
                {
                    raw.clone()
                } else {
                    burn.receipt_info
                        .as_ref()
                        .map(|info| info.encode(oa_schema.as_deref()))
                        .transpose()?
                        .unwrap_or_default()
                };

                Ok(vault_contract
                    .redeem(
                        burn.burn_shares,
                        params.user,
                        params.owner,
                        burn.receipt_id,
                        receipt_bytes,
                    )
                    .calldata()
                    .clone())
            })
            .collect::<Result<Vec<_>, VaultError>>()?;

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

        let tx = vault_contract.multicall(calls).into_transaction_request();

        // Fill nonce, gas price, gas limit, chain_id from the provider
        let envelop = self
            .provider
            .fill(tx)
            .await?
            .try_into_envelope()
            .map_err(Box::new)?;
        let nonce = envelop.nonce();
        let hash = *envelop.tx_hash();
        let tx = envelop.encoded_2718();

        Ok(SendableTxWithHash {
            tx,
            hash,
            nonce,
            signed_at: Utc::now(),
            dust_shares: params.dust_shares,
        })
    }

    async fn check_tx(
        &self,
        tx_id: &TxId,
    ) -> Result<TransactionReceipt, VaultError> {
        let tx_hash = tx_id.to_hash().ok_or(VaultError::InvalidReceipt)?;

        let receipt = PendingTransactionBuilder::new(
            self.provider.root().clone(),
            tx_hash,
        )
        .with_timeout(Some(Duration::from_secs(30)))
        .get_receipt()
        .await?;

        // A mined-but-reverted burn consumes no receipts, so it is a definitive
        // failure distinct from an anomalous missing-Withdraw parse error.
        if !receipt.status() {
            return Err(VaultError::Reverted { tx_hash });
        }

        Ok(receipt)
    }

    async fn lock_wallet(&self) -> WalletNonceGuard {
        Some(self.wallet_nonce_lock.clone().lock_owned().await)
    }
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{
        Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
    };
    use alloy::network::EthereumWallet;
    use alloy::primitives::{
        Address, B256, Bloom, Bytes, IntoLogData, U256, address, b256,
        fixed_bytes,
    };
    use alloy::providers::ProviderBuilder;
    use alloy::providers::fillers::{BlobGasFiller, ChainIdFiller};
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::{Block, FeeHistory, TransactionReceipt};
    use alloy::signers::local::PrivateKeySigner;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::sync::Arc;

    use super::RealBlockchainService;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::mint::{
        IssuerMintRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::redemption::{BurnExternalTxId, IssuerRedemptionRequestId};
    use crate::vault::rain_meta::OaSchemaCache;
    use crate::vault::{
        MultiBurnEntry, MultiBurnParams, ReceiptInformation,
        SendableTxWithHash, VaultError, VaultService,
    };

    const TEST_OA_SCHEMA: &str =
        "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm";

    fn test_receipt_info() -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL"),
            Quantity::new(Decimal::from(100)),
            Utc::now(),
            None,
        )
    }

    fn test_issuer_redemption_id() -> IssuerRedemptionRequestId {
        IssuerRedemptionRequestId::new(b256!(
            "0xabababababababababababababababababababababababababababababababab"
        ))
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
        asserter.push_success(&tx_hash); // eth_sendRawTransaction
        asserter.push_success(receipt); // eth_getTransactionReceipt
        asserter.push_success(receipt); // eth_getTransactionReceipt (polling)
    }

    /// Sets up mock responses for `provider.fill(tx)`:
    /// ChainIdFiller → eth_chainId,
    /// GasFiller → eth_feeHistory / eth_getBlockByNumber / eth_estimateGas / eth_maxPriorityFeePerGas,
    /// NonceFiller → eth_getTransactionCount,
    /// WalletFiller → eth_chainId (signing uses chain_id for replay protection).
    fn setup_asserter_for_fill(asserter: &Asserter, nonce: u64) {
        let block: Block<alloy::rpc::types::Transaction> = Block::default();
        asserter.push_success(&1u64); // eth_chainId (ChainIdFiller)
        asserter.push_success(&test_fee_history()); // eth_feeHistory (GasFiller)
        asserter.push_success(&block); // eth_getBlockByNumber (GasFiller)
        asserter.push_success(&100_000_u64); // eth_estimateGas (GasFiller)
        asserter.push_success(&1_000_000_000_u64); // eth_maxPriorityFeePerGas (GasFiller)
        asserter.push_success(&nonce); // eth_getTransactionCount (NonceFiller)
        asserter.push_success(&1u64); // eth_chainId (WalletFiller, for EIP-155 signing)
    }

    fn create_service_with_asserter(asserter: Asserter) -> impl VaultService {
        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller)
            .with_simple_nonce_management()
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter);
        RealBlockchainService::new(
            provider,
            Arc::new(OaSchemaCache::fixed(TEST_OA_SCHEMA)),
        )
    }

    #[tokio::test]
    async fn test_submit_and_confirm_mint_success() {
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

        asserter.push_success(&"0x00000000000000000000000000000000000000000000000000000000000003e8");
        asserter.push_success(&0u64);
        asserter.push_success(&fee_history);
        asserter.push_success(&block);
        asserter.push_success(&1u64);
        asserter.push_success(&100_000_u64);
        asserter.push_success(&1_000_000_000_u64);
        asserter.push_success(&0u64);
        asserter.push_success(&tx_hash);
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);

        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller)
            .with_simple_nonce_management()
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter);
        let service = RealBlockchainService::new(
            provider,
            Arc::new(OaSchemaCache::fixed(TEST_OA_SCHEMA)),
        );

        let submitted = service
            .submit_mint(
                vault_address,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
                None,
            )
            .await;

        assert!(submitted.is_ok(), "Expected Ok but got: {submitted:?}");
        let submitted = submitted.unwrap();

        let result = service.confirm_mint(&submitted.tx_id).await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let mint_result = result.unwrap();
        assert_eq!(mint_result.tx_hash, tx_hash);
        assert_eq!(mint_result.receipt_id, receipt_id);
        assert_eq!(mint_result.shares_minted, shares);
        assert_eq!(mint_result.gas_used, 0x5208);
        assert_eq!(mint_result.block_number, 0x3e8);
    }

    #[tokio::test]
    async fn test_submit_mint_missing_deposit_event() {
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

        asserter.push_success(&"0x00000000000000000000000000000000000000000000000000000000000003e8");
        asserter.push_success(&0u64);
        asserter.push_success(&fee_history);
        asserter.push_success(&block);
        asserter.push_success(&1u64);
        asserter.push_success(&100_000_u64);
        asserter.push_success(&1_000_000_000_u64);
        asserter.push_success(&0u64);
        asserter.push_success(&tx_hash);
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);

        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller)
            .with_simple_nonce_management()
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter);
        let service = RealBlockchainService::new(
            provider,
            Arc::new(OaSchemaCache::fixed(TEST_OA_SCHEMA)),
        );

        let submitted = service
            .submit_mint(
                vault_address,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
                None,
            )
            .await;

        assert!(submitted.is_ok(), "Expected Ok but got: {submitted:?}");
        let submitted = submitted.unwrap();

        let result = service.confirm_mint(&submitted.tx_id).await;

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

    fn create_multi_withdraw_receipt(
        vault_address: Address,
        tx_hash: B256,
        owner: Address,
        user: Address,
        burns: Vec<(U256, U256)>,
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
    async fn test_submit_and_confirm_burn_two_burns() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x3333333333333333333333333333333333333333");

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

        let detected_tx_hash = b256!(
            "0xabababababababababababababababababababababababababababababababab"
        );
        let prepared_tx = SendableTxWithHash {
            tx: vec![],
            hash: tx_hash,
            nonce: 0,
            signed_at: Utc::now(),
            dust_shares: U256::ZERO,
        };
        let submitted = service
            .submit_burn(
                MultiBurnParams {
                    vault: vault_address,
                    burns: burns
                        .iter()
                        .map(|(receipt_id, burn_shares)| MultiBurnEntry {
                            receipt_id: *receipt_id,
                            burn_shares: *burn_shares,
                            receipt_info: None,
                            receipt_info_bytes: None,
                        })
                        .collect(),
                    dust_shares: U256::ZERO,
                    owner,
                    user,
                    issuer_request_id: test_issuer_redemption_id(),
                    detected_tx_hash,
                    external_tx_id: None,
                },
                prepared_tx,
            )
            .await;

        assert!(submitted.is_ok(), "Expected Ok but got: {submitted:?}");
        let submitted = submitted.unwrap();

        let result = service.confirm_burn(&submitted.tx_id, U256::ZERO).await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let multi_result = result.unwrap();

        assert_eq!(multi_result.tx_hash, tx_hash);
        assert_eq!(multi_result.burns.len(), 2);
        assert_eq!(multi_result.burns[0].receipt_id, U256::from(1));
        assert_eq!(multi_result.burns[0].shares_burned, U256::from(100));
        assert_eq!(multi_result.burns[1].receipt_id, U256::from(2));
        assert_eq!(multi_result.burns[1].shares_burned, U256::from(50));
    }

    #[tokio::test]
    async fn test_submit_burn_propagates_external_tx_id_override() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x3333333333333333333333333333333333333333");

        let tx_hash = fixed_bytes!(
            "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );

        let burns = vec![(U256::from(1), U256::from(100))];

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

        let detected_tx_hash = b256!(
            "0xabababababababababababababababababababababababababababababababab"
        );
        let override_id = "burn-0xabab-retry-2".to_string();

        let prepared_tx = SendableTxWithHash {
            tx: vec![],
            hash: tx_hash,
            nonce: 0,
            signed_at: Utc::now(),
            dust_shares: U256::ZERO,
        };
        let submitted = service
            .submit_burn(
                MultiBurnParams {
                    vault: vault_address,
                    burns: burns
                        .iter()
                        .map(|(receipt_id, burn_shares)| MultiBurnEntry {
                            receipt_id: *receipt_id,
                            burn_shares: *burn_shares,
                            receipt_info: None,
                            receipt_info_bytes: None,
                        })
                        .collect(),
                    dust_shares: U256::ZERO,
                    owner,
                    user,
                    issuer_request_id: test_issuer_redemption_id(),
                    detected_tx_hash,
                    external_tx_id: Some(BurnExternalTxId::from_string(
                        override_id.clone(),
                    )),
                },
                prepared_tx,
            )
            .await
            .expect("submit_burn should succeed");

        // A caller-provided externalTxId (used for replacement burn retries)
        // must propagate verbatim rather than be replaced by the local default.
        assert_eq!(submitted.external_tx_id, override_id);
    }

    #[tokio::test]
    async fn test_submit_burn_returns_error_on_missing_events() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x6666666666666666666666666666666666666666");

        let tx_hash = fixed_bytes!(
            "0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff"
        );

        let receipt = create_empty_receipt(vault_address, tx_hash);

        let asserter = Asserter::new();
        setup_asserter_for_transaction(&asserter, tx_hash, &receipt);

        let service = create_service_with_asserter(asserter);

        let prepared_tx = SendableTxWithHash {
            tx: vec![],
            hash: tx_hash,
            nonce: 0,
            signed_at: Utc::now(),
            dust_shares: U256::ZERO,
        };
        let submit_result = service
            .submit_burn(MultiBurnParams {
                vault: vault_address,
                burns: vec![MultiBurnEntry {
                    receipt_id: U256::from(1),
                    burn_shares: U256::from(100),
                    receipt_info: None,
                    receipt_info_bytes: None,
                }],
                dust_shares: U256::ZERO,
                owner,
                user,
                issuer_request_id: test_issuer_redemption_id(),
                detected_tx_hash: b256!(
                    "0xabababababababababababababababababababababababababababababababab"
                ),
                external_tx_id: None,
            }, prepared_tx)
            .await
            .expect("Expected a successfull tx submit, but failed1");
        let result =
            service.confirm_burn(&submit_result.tx_id, U256::ZERO).await;

        assert!(result.is_err(), "Expected Err but got Ok: {result:?}");
        assert!(matches!(
            result.unwrap_err(),
            VaultError::EventNotFound { .. }
        ));
    }

    #[tokio::test]
    async fn prepare_tx_returns_signed_tx_with_correct_nonce_and_dust_shares() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x3333333333333333333333333333333333333333");
        let detected_tx_hash = b256!(
            "0xabababababababababababababababababababababababababababababababab"
        );
        let expected_nonce = 7u64;
        let dust_shares = U256::from(500);

        let asserter = Asserter::new();
        setup_asserter_for_fill(&asserter, expected_nonce);

        let service = create_service_with_asserter(asserter);

        let params = MultiBurnParams {
            vault: vault_address,
            burns: vec![MultiBurnEntry {
                receipt_id: U256::from(1),
                burn_shares: U256::from(100),
                receipt_info: None,
                receipt_info_bytes: Some(Bytes::from(b"test".to_vec())),
            }],
            dust_shares,
            owner,
            user,
            issuer_request_id: test_issuer_redemption_id(),
            detected_tx_hash,
            external_tx_id: None,
        };

        let sendable = service
            .prepare_burn_tx(&params)
            .await
            .expect("expected SendableTxWithHash");

        assert_eq!(sendable.nonce, expected_nonce, "nonce must match mock");
        assert_eq!(
            sendable.dust_shares, dust_shares,
            "dust_shares must be propagated from params"
        );
        assert!(!sendable.tx.is_empty(), "encoded tx must be non-empty");
        assert_ne!(sendable.hash, B256::ZERO, "tx hash must be non-zero");
        assert!(sendable.signed_at.timestamp() > 0, "signed_at must be set");
    }

    #[tokio::test]
    async fn prepare_tx_zero_dust_shares_when_params_has_no_dust() {
        let vault_address = test_vault_address();
        let owner = test_receiver();
        let user = address!("0x3333333333333333333333333333333333333333");
        let detected_tx_hash = b256!(
            "0xabababababababababababababababababababababababababababababababab"
        );

        let asserter = Asserter::new();
        setup_asserter_for_fill(&asserter, 0);

        let service = create_service_with_asserter(asserter);

        let params = MultiBurnParams {
            vault: vault_address,
            burns: vec![MultiBurnEntry {
                receipt_id: U256::from(42),
                burn_shares: U256::from(200),
                receipt_info: None,
                receipt_info_bytes: Some(Bytes::from(b"receipt".to_vec())),
            }],
            dust_shares: U256::ZERO,
            owner,
            user,
            issuer_request_id: test_issuer_redemption_id(),
            detected_tx_hash,
            external_tx_id: None,
        };

        let sendable = service
            .prepare_burn_tx(&params)
            .await
            .expect("expected SendableTxWithHash");

        assert_eq!(sendable.dust_shares, U256::ZERO);
        assert!(!sendable.tx.is_empty());
    }
}
