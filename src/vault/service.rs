use alloy::consensus::Transaction;
use alloy::consensus::transaction::SignerRecoverable;
use alloy::eips::Encodable2718;
use alloy::network::{EthereumWallet, TransactionResponse};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill,
    NonceFiller, SimpleNonceManager, WalletFiller,
};
use alloy::providers::{
    Identity, PendingTransactionBuilder, Provider, RootProvider,
};
use alloy::rpc::json_rpc::ErrorPayload;
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::sol_types::SolInterface;
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::debug;

use super::rain_meta::OaSchemaCache;
use super::{
    BurnTxStatus, BurnVerification, MintResult, MintTxStatus, MultiBurnResult,
    MultiBurnResultEntry, OrchestratorBurnParams, OrchestratorBurnReadiness,
    OrchestratorBurnResult, OrchestratorRevertReason, PreparedMintTx,
    ReceiptInformation, SendableTxWithHash, SubmittedTx, TxId, VaultError,
    VaultService, WalletNonceGuard, classify_checked_receipt,
    verify_burn_in_receipt,
};
use crate::bindings::IST0xOrchestratorV1::IST0xOrchestratorV1Errors;
use crate::bindings::{IST0xOrchestratorV1, OffchainAssetReceiptVault};
use crate::redemption::BurnExternalTxId;
use crate::vault::orchestrator::BurnProofKind;

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
/// - **Mints**: `prepare_mint_tx()` returns exact signed bytes and their hash for
///   persistence before `submit_mint()` broadcasts them. Recovery rebroadcasts
///   or polls that same transaction; receipt backfill remains a second line of
///   defense for transactions that already mined.
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

    /// Decodes the typed revert reason of a mined-but-reverted orchestrator
    /// transaction by replaying it as an `eth_call` pinned at the parent of
    /// its mined block — the pre-transaction state a transaction in block N
    /// executes against is post-block-N-1 (ignoring same-block predecessors;
    /// the receipt itself carries no revert data). Best-effort: any lookup,
    /// replay, or decode uncertainty yields
    /// [`OrchestratorRevertReason::Unknown`], which downstream treats as an
    /// unclassified definitive revert.
    async fn decode_orchestrator_revert(
        &self,
        tx_hash: alloy::primitives::B256,
        block_number: u64,
    ) -> OrchestratorRevertReason {
        let Ok(Some(transaction)) =
            self.provider.get_transaction_by_hash(tx_hash).await
        else {
            return OrchestratorRevertReason::Unknown;
        };
        let Some(to) = transaction.to() else {
            return OrchestratorRevertReason::Unknown;
        };

        let request = TransactionRequest::default()
            .from(transaction.inner.signer())
            .to(to)
            .input(transaction.input().clone().into())
            .value(transaction.value());

        let Err(error) =
            self.provider.call(request).block(block_number.into()).await
        else {
            return OrchestratorRevertReason::Unknown;
        };

        error
            .as_error_resp()
            .and_then(ErrorPayload::as_revert_data)
            .and_then(|data| IST0xOrchestratorV1Errors::abi_decode(&data).ok())
            .map_or(OrchestratorRevertReason::Unknown, |decoded| {
                use IST0xOrchestratorV1Errors::{
                    InsufficientReceipts, ReceiptLogicMismatch,
                    VaultLogicMismatch,
                };
                match decoded {
                    InsufficientReceipts(error) => {
                        OrchestratorRevertReason::InsufficientReceipts {
                            token: error.token,
                            shortfall: error.shortfall,
                        }
                    }
                    VaultLogicMismatch(_) => {
                        OrchestratorRevertReason::VaultLogicMismatch
                    }
                    ReceiptLogicMismatch(_) => {
                        OrchestratorRevertReason::ReceiptLogicMismatch
                    }
                    _ => OrchestratorRevertReason::Unknown,
                }
            })
    }

    async fn try_broadcast_tx(
        &self,
        tx: &[u8],
        hash: B256,
    ) -> Result<Option<()>, VaultError> {
        match self.provider.send_raw_transaction(tx).await {
            Ok(pending_tx) => {
                let returned = *pending_tx.tx_hash();
                if returned != hash {
                    return Err(VaultError::BroadcastHashMismatch {
                        expected: hash,
                        returned,
                    });
                }
                Ok(Some(()))
            }
            Err(error) => {
                if self.provider.get_transaction_by_hash(hash).await?.is_none()
                {
                    return Err(error.into());
                }
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl VaultService for RealBlockchainService {
    async fn prepare_mint_tx(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
        external_tx_id: Option<String>,
    ) -> Result<PreparedMintTx, VaultError> {
        let external_tx_id = external_tx_id.unwrap_or_else(|| {
            format!("mint-{}", receipt_info.issuer_request_id)
        });
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

        let transaction = vault_contract
            .multicall(vec![deposit_call, transfer_call])
            .into_transaction_request();
        let envelope = self
            .provider
            .fill(transaction)
            .await?
            .try_into_envelope()
            .map_err(Box::new)?;
        let prepared_tx = PreparedMintTx {
            nonce: envelope.nonce(),
            hash: *envelope.tx_hash(),
            tx: envelope.encoded_2718(),
            signed_at: Utc::now(),
            external_tx_id,
        };
        prepared_tx.validate()?;

        Ok(prepared_tx)
    }

    async fn submit_mint(
        &self,
        prepared_tx: &PreparedMintTx,
    ) -> Result<SubmittedTx, VaultError> {
        prepared_tx.validate()?;
        if self
            .try_broadcast_tx(&prepared_tx.tx, prepared_tx.hash)
            .await?
            .is_none()
        {
            debug!(target: "vault", tx_hash = %prepared_tx.hash,
                "Mint broadcast errored but the node holds the persisted transaction"
            );
        }
        Ok(SubmittedTx {
            external_tx_id: prepared_tx.external_tx_id.clone(),
            tx_id: prepared_tx.hash.into(),
        })
    }

    async fn confirm_mint(
        &self,
        tx_id: &TxId,
    ) -> Result<MintResult, VaultError> {
        // Bounded Option poll of eth_getTransactionReceipt — never
        // PendingTransactionBuilder::get_receipt as the terminal classifier.
        // Uncertain outcomes surface as ConfirmationPending so jobs fail closed
        // instead of recording MintingFailed and authorizing a second deposit.
        debug!(target: "vault", tx_hash = %tx_id,
            "Getting mint tx from chain"
        );

        let tx_hash = tx_id.to_hash().ok_or(VaultError::InvalidReceipt)?;

        // Overall budget matches the historical 120s PendingTransactionBuilder
        // timeout; cadence is a simple fixed sleep (no multi-RPC failover).
        // Wrap the entire poll (including in-flight RPC) so a hanging provider
        // call cannot exceed the budget and hold an apalis worker slot.
        const CONFIRM_MINT_TIMEOUT: Duration = Duration::from_secs(120);
        const CONFIRM_MINT_POLL_INTERVAL: Duration = Duration::from_secs(2);

        let poll = async {
            loop {
                let receipt = match self
                    .provider
                    .get_transaction_receipt(tx_hash)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        // Transport / RPC blips are uncertain — never Reverted.
                        return Err(VaultError::ConfirmationPending {
                            tx_id: TxId::Hash(tx_hash),
                            message: error.to_string(),
                        });
                    }
                };

                let Some(receipt) = receipt else {
                    debug!(target: "vault",
                        tx_hash = %tx_hash,
                        "Mint receipt not yet available; polling"
                    );
                    tokio::time::sleep(CONFIRM_MINT_POLL_INTERVAL).await;
                    continue;
                };

                if receipt.transaction_hash != tx_hash
                    || receipt.block_number.is_none()
                {
                    return Err(VaultError::InvalidReceipt);
                }

                if !receipt.status() {
                    debug!(target: "vault",
                        tx_hash = %tx_hash,
                        "Mint transaction mined with status=0"
                    );
                    return Err(VaultError::Reverted { tx_hash });
                }

                let (receipt_id, shares_minted, receipt_info_bytes) = receipt
                    .inner
                    .logs()
                    .iter()
                    .find_map(|log| {
                        log.log_decode::<OffchainAssetReceiptVault::Deposit>()
                            .ok()
                            .map(|decoded| {
                                let event_data = decoded.data();
                                (
                                    event_data.id,
                                    event_data.shares,
                                    event_data.receiptInformation.clone(),
                                )
                            })
                    })
                    .ok_or_else(|| VaultError::EventNotFound { tx_hash })?;

                let block_number =
                    receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

                debug!(target: "vault",
                    tx_hash = %tx_hash,
                    block_number,
                    receipt_id = %receipt_id,
                    "Mint transaction confirmed with Deposit"
                );

                return Ok(MintResult {
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used: receipt.gas_used,
                    block_number,
                    receipt_info_bytes,
                });
            }
        };

        tokio::time::timeout(CONFIRM_MINT_TIMEOUT, poll).await.unwrap_or_else(
            |_| {
                Err(VaultError::ConfirmationPending {
                    tx_id: TxId::Hash(tx_hash),
                    message:
                        "receipt polling budget exhausted without a receipt"
                            .to_string(),
                })
            },
        )
    }

    async fn classify_mint_tx(
        &self,
        owner: Address,
        prepared_tx: &PreparedMintTx,
    ) -> Result<MintTxStatus, VaultError> {
        prepared_tx.validate_for_owner(owner)?;
        let status = if let Some(receipt) =
            self.provider.get_transaction_receipt(prepared_tx.hash).await?
        {
            if receipt.transaction_hash != prepared_tx.hash
                || receipt.block_number.is_none()
            {
                return Err(VaultError::InvalidReceipt);
            }

            if receipt.status() {
                MintTxStatus::MinedSuccess
            } else {
                MintTxStatus::MinedReverted
            }
        } else {
            let latest_nonce =
                self.provider.get_transaction_count(owner).latest().await?;
            let finalized_nonce =
                self.provider.get_transaction_count(owner).finalized().await?;
            let status = if finalized_nonce > prepared_tx.nonce {
                match self
                    .provider
                    .get_transaction_receipt(prepared_tx.hash)
                    .await?
                {
                    Some(receipt) => {
                        if receipt.transaction_hash != prepared_tx.hash
                            || receipt.block_number.is_none()
                        {
                            return Err(VaultError::InvalidReceipt);
                        }
                        if receipt.status() {
                            MintTxStatus::MinedSuccess
                        } else {
                            MintTxStatus::MinedReverted
                        }
                    }
                    // `ProvablyDead` unlocks `RecordMintFailed` and a
                    // replacement prepare, so it must not rest on a single
                    // absent receipt: a lagging or load-balanced RPC node can
                    // answer `None` for a receipt that exists. A node that has
                    // also forgotten the transaction corroborates the death; a
                    // node that still knows it is contradicting itself, which
                    // is uncertainty rather than proof.
                    None => match self
                        .provider
                        .get_transaction_by_hash(prepared_tx.hash)
                        .await?
                    {
                        None => MintTxStatus::ProvablyDead,
                        Some(_) => {
                            return Err(
                                VaultError::ContradictoryDeathSignals {
                                    tx_hash: prepared_tx.hash,
                                    nonce: prepared_tx.nonce,
                                },
                            );
                        }
                    },
                }
            } else {
                MintTxStatus::StillMineable
            };
            debug!(target: "vault",
                owner = %owner,
                tx_hash = %prepared_tx.hash,
                nonce = prepared_tx.nonce,
                latest_nonce,
                finalized_nonce,
                status = ?status,
                "Classified persisted mint transaction"
            );
            return Ok(status);
        };
        debug!(target: "vault",
            owner = %owner,
            tx_hash = %prepared_tx.hash,
            nonce = prepared_tx.nonce,
            status = ?status,
            "Classified persisted mint transaction"
        );
        Ok(status)
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
        sendable_tx.validate_for_owner(params.owner)?;
        if self
            .try_broadcast_tx(&sendable_tx.tx, sendable_tx.hash)
            .await?
            .is_none()
        {
            debug!(target: "vault", tx_hash = %sendable_tx.hash,
                "Burn broadcast errored but the node holds the persisted transaction"
            );
        }
        Ok(SubmittedTx {
            external_tx_id: params
                .external_tx_id
                .clone()
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
        expected_proof: BurnProofKind,
    ) -> Result<BurnVerification, VaultError> {
        let transaction = self
            .provider
            .get_transaction_by_hash(tx_hash)
            .await?
            .ok_or(VaultError::InvalidReceipt)?;
        if transaction.tx_hash() != tx_hash {
            return Err(VaultError::InvalidReceipt);
        }
        let recovered_signer = transaction.inner.inner().recover_signer()?;
        if recovered_signer != owner {
            return Err(VaultError::NotABurn { tx_hash });
        }
        if transaction.inner.signer() != recovered_signer {
            return Err(VaultError::InvalidReceipt);
        }
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or(VaultError::InvalidReceipt)?;
        if receipt.transaction_hash != tx_hash {
            return Err(VaultError::InvalidReceipt);
        }

        verify_burn_in_receipt(
            &receipt,
            vault,
            owner,
            tx_hash,
            transaction.nonce(),
            expected_proof,
        )
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

    async fn classify_burn_tx(
        &self,
        owner: Address,
        sendable_tx: &SendableTxWithHash,
    ) -> Result<BurnTxStatus, VaultError> {
        sendable_tx.validate_for_owner(owner)?;
        let status = if let Some(receipt) =
            self.provider.get_transaction_receipt(sendable_tx.hash).await?
        {
            if receipt.transaction_hash != sendable_tx.hash
                || receipt.block_number.is_none()
            {
                return Err(VaultError::InvalidReceipt);
            }

            if receipt.status() {
                BurnTxStatus::Mined
            } else {
                BurnTxStatus::Reverted
            }
        } else {
            let latest_nonce =
                self.provider.get_transaction_count(owner).latest().await?;
            let finalized_nonce =
                self.provider.get_transaction_count(owner).finalized().await?;
            let status = if finalized_nonce > sendable_tx.nonce {
                match self
                    .provider
                    .get_transaction_receipt(sendable_tx.hash)
                    .await?
                {
                    Some(receipt) => {
                        if receipt.transaction_hash != sendable_tx.hash
                            || receipt.block_number.is_none()
                        {
                            return Err(VaultError::InvalidReceipt);
                        }
                        if receipt.status() {
                            BurnTxStatus::Mined
                        } else {
                            BurnTxStatus::Reverted
                        }
                    }
                    None => BurnTxStatus::ProvablyDead,
                }
            } else {
                BurnTxStatus::StillMineable
            };
            debug!(target: "vault",
                owner = %owner,
                tx_hash = %sendable_tx.hash,
                nonce = sendable_tx.nonce,
                latest_nonce,
                finalized_nonce,
                status = ?status,
                "Classified persisted burn transaction"
            );
            return Ok(status);
        };
        debug!(target: "vault",
            owner = %owner,
            tx_hash = %sendable_tx.hash,
            nonce = sendable_tx.nonce,
            status = ?status,
            "Classified persisted burn transaction"
        );
        Ok(status)
    }

    async fn prepare_replacement_burn_tx(
        &self,
        owner: Address,
        sendable_tx: &SendableTxWithHash,
    ) -> Result<SendableTxWithHash, VaultError> {
        let envelope = sendable_tx.validate_for_owner(owner)?;
        let mut transaction = TransactionRequest::from_transaction(envelope);
        transaction.from = Some(owner);
        transaction.nonce =
            Some(self.provider.get_transaction_count(owner).pending().await?);
        transaction.gas = None;
        transaction.gas_price = None;
        transaction.max_fee_per_gas = None;
        transaction.max_priority_fee_per_gas = None;
        transaction.max_fee_per_blob_gas = None;

        let replacement = self
            .provider
            .fill(transaction)
            .await?
            .try_into_envelope()
            .map_err(Box::new)?;
        let replacement = SendableTxWithHash {
            tx: replacement.encoded_2718(),
            hash: *replacement.tx_hash(),
            nonce: replacement.nonce(),
            signed_at: Utc::now(),
            dust_shares: sendable_tx.dust_shares,
        };
        debug!(target: "vault",
            owner = %owner,
            previous_tx_hash = %sendable_tx.hash,
            previous_nonce = sendable_tx.nonce,
            replacement_tx_hash = %replacement.hash,
            replacement_nonce = replacement.nonce,
            "Prepared fresh-nonce burn replacement"
        );
        Ok(replacement)
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

        classify_checked_receipt(tx_hash, receipt)
    }

    async fn lock_wallet(&self) -> WalletNonceGuard {
        Some(self.wallet_nonce_lock.clone().lock_owned().await)
    }

    async fn check_orchestrator_burn_readiness(
        &self,
        orchestrator: Address,
        token: Address,
        owner: Address,
        amount: U256,
    ) -> Result<OrchestratorBurnReadiness, VaultError> {
        // Allowance first: an approval shortfall is an actionable ops failure
        // and must be reported even while the orchestrator is halted.
        let token_contract =
            OffchainAssetReceiptVault::new(token, &self.provider);
        let current =
            token_contract.allowance(owner, orchestrator).call().await?;
        if current < amount {
            return Ok(OrchestratorBurnReadiness::AllowanceInsufficient {
                required: amount,
                current,
            });
        }

        let orchestrator_contract =
            IST0xOrchestratorV1::new(orchestrator, &self.provider);
        if !orchestrator_contract.vaultLogicIsExpected().call().await? {
            return Ok(OrchestratorBurnReadiness::VaultLogicMismatch);
        }

        // Simulate the burn so a deterministic revert is classified before
        // anything is signed. This simulation is the classification
        // mechanism: gas estimation is a separate RPC step in the prepare
        // fill pipeline whose failure on a reverting burn surfaces only as
        // an unclassified preparation error, and a supplied gas limit would
        // skip estimation and sign a doomed transaction.
        let Err(error) = orchestrator_contract
            .burn(token, amount, Bytes::new())
            .from(owner)
            .call()
            .await
        else {
            return Ok(OrchestratorBurnReadiness::Ready);
        };

        // No revert data means the simulation itself failed (transport/RPC),
        // not that the burn reverted — propagate so the reconciler retries
        // the gate on its next pass.
        let Some(revert_data) = error.as_revert_data() else {
            return Err(error.into());
        };

        match IST0xOrchestratorV1Errors::abi_decode(&revert_data).ok() {
            Some(IST0xOrchestratorV1Errors::InsufficientReceipts(revert)) => {
                Ok(OrchestratorBurnReadiness::InsufficientReceipts {
                    shortfall: revert.shortfall,
                })
            }
            Some(
                IST0xOrchestratorV1Errors::VaultLogicMismatch(_)
                | IST0xOrchestratorV1Errors::ReceiptLogicMismatch(_),
            ) => Ok(OrchestratorBurnReadiness::VaultLogicMismatch),
            // A deterministic revert outside the classified set (another
            // orchestrator error, or a foreign revert from the vault's
            // transferFrom path): report Ready and let preparation fail
            // instead. Its gas estimation replays the same revert before
            // anything is signed, recording the failure as `Unclassified`
            // under the bounded preparation-retry budget — the
            // pre-simulation behavior. Erroring here would defer the
            // redemption forever without ever parking it in an
            // operator-visible BurnFailed state.
            Some(_) | None => Ok(OrchestratorBurnReadiness::Ready),
        }
    }

    async fn prepare_orchestrator_burn_tx(
        &self,
        params: &OrchestratorBurnParams,
    ) -> Result<SendableTxWithHash, VaultError> {
        let orchestrator_contract =
            IST0xOrchestratorV1::new(params.orchestrator, &self.provider);

        let tx = orchestrator_contract
            .burn(params.token, params.amount, Bytes::new())
            .into_transaction_request();

        let envelope = self
            .provider
            .fill(tx)
            .await?
            .try_into_envelope()
            .map_err(Box::new)?;

        Ok(SendableTxWithHash {
            nonce: envelope.nonce(),
            hash: *envelope.tx_hash(),
            tx: envelope.encoded_2718(),
            signed_at: Utc::now(),
            // The orchestrator burn retains dust in the bot wallet; there is
            // no on-chain dust return to encode.
            dust_shares: U256::ZERO,
        })
    }

    async fn submit_orchestrator_burn(
        &self,
        params: &OrchestratorBurnParams,
        sendable_tx: &SendableTxWithHash,
    ) -> Result<SubmittedTx, VaultError> {
        sendable_tx.validate_for_owner(params.owner)?;
        if self
            .try_broadcast_tx(&sendable_tx.tx, sendable_tx.hash)
            .await?
            .is_none()
        {
            debug!(target: "vault", tx_hash = %sendable_tx.hash,
                "Orchestrator burn broadcast errored but the node holds \
                    the persisted transaction"
            );
        }
        Ok(SubmittedTx {
            external_tx_id: params
                .external_tx_id
                .clone()
                .unwrap_or_else(|| {
                    BurnExternalTxId::base(&params.detected_tx_hash)
                })
                .into_string(),
            tx_id: sendable_tx.hash.into(),
        })
    }

    async fn confirm_orchestrator_burn(
        &self,
        tx_id: &TxId,
    ) -> Result<OrchestratorBurnResult, VaultError> {
        debug!(target: "vault", tx_hash = %tx_id,
            "Getting orchestrator burn tx data from chain"
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

        let block_number =
            receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

        // A mined-but-reverted orchestrator burn is a definitive failure;
        // decode its typed reason so the aggregate records the right
        // classification.
        if !receipt.status() {
            let reason = self
                .decode_orchestrator_revert(
                    tx_hash,
                    block_number.saturating_sub(1),
                )
                .await;
            return Err(VaultError::OrchestratorReverted { tx_hash, reason });
        }

        // Bind the decoded event to OUR burn before terminalizing
        // accounting: it must be emitted by the orchestrator this persisted
        // transaction targeted (`receipt.to`), for the transaction's own
        // sender as `caller` — a same-signature event emitted by any other
        // contract in the call tree must never be mistaken for it. Token and
        // amount are then bound transitively: the orchestrator emits exactly
        // one `Burned` per `burn()` call, with fields taken from our own
        // persisted calldata.
        let orchestrator = receipt.to.ok_or(VaultError::InvalidReceipt)?;
        let burned = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                if log.address() != orchestrator {
                    return None;
                }
                let decoded =
                    log.log_decode::<IST0xOrchestratorV1::Burned>().ok()?;
                (decoded.data().caller == receipt.from).then_some(decoded)
            })
            .ok_or(VaultError::EventNotFound { tx_hash })?;
        let burned = burned.data();

        Ok(OrchestratorBurnResult {
            tx_hash,
            shares_burned: burned.amount,
            burn_range: (burned.firstReceiptId, burned.nextBurnReceiptIdAfter),
            gas_used: receipt.gas_used,
            block_number,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::consensus::transaction::Recovered;
    use alloy::consensus::{
        Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom, Transaction,
        TxEnvelope,
    };
    use alloy::eips::{Decodable2718, Encodable2718};
    use alloy::network::EthereumWallet;
    use alloy::primitives::{
        Address, B256, Bloom, Bytes, IntoLogData, U256, address, b256,
        fixed_bytes,
    };
    use alloy::providers::fillers::{BlobGasFiller, ChainIdFiller};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::rpc::types::{
        Block, FeeHistory, Transaction as RpcTransaction, TransactionReceipt,
        TransactionRequest,
    };
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::{SolCall, SolError};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::{
        OrchestratorBurnParams, OrchestratorBurnReadiness,
        OrchestratorRevertReason, RealBlockchainService,
        RealBlockchainServiceProvider,
    };
    use crate::bindings::{IST0xOrchestratorV1, OffchainAssetReceiptVault};
    use crate::mint::{
        IssuerMintRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::redemption::{BurnExternalTxId, IssuerRedemptionRequestId};
    use crate::test_utils::{LocalEvm, logs_contain_at};
    use crate::vault::orchestrator::BurnProofKind;
    use crate::vault::rain_meta::OaSchemaCache;
    use crate::vault::{
        BurnRequestOrigin, BurnTxStatus, MintTxStatus, MultiBurnEntry,
        MultiBurnParams, PreparedMintTx, ReceiptInformation,
        SendableTxWithHash, TxId, VaultError, VaultService,
    };

    const TEST_OA_SCHEMA: &str =
        "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm";

    fn test_receipt_info() -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL").unwrap(),
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

    fn test_multi_burn_params(owner: Address) -> MultiBurnParams {
        MultiBurnParams {
            vault: test_vault_address(),
            burns: vec![MultiBurnEntry {
                receipt_id: U256::from(1),
                burn_shares: U256::from(100),
                receipt_info: None,
                receipt_info_bytes: None,
            }],
            dust_shares: U256::ZERO,
            owner,
            user: address!("0x3333333333333333333333333333333333333333"),
            origin: BurnRequestOrigin::Redemption(test_issuer_redemption_id()),
            detected_tx_hash: b256!(
                "0xabababababababababababababababababababababababababababababababab"
            ),
            external_tx_id: None,
        }
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
        create_service_with_signer(asserter, PrivateKeySigner::random())
    }

    fn create_service_with_signer(
        asserter: Asserter,
        signer: PrivateKeySigner,
    ) -> impl VaultService {
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

    async fn sign_test_transaction(
        provider: &RealBlockchainServiceProvider,
        transaction: TransactionRequest,
    ) -> SendableTxWithHash {
        let envelope = provider
            .fill(transaction)
            .await
            .expect("test transaction should fill")
            .try_into_envelope()
            .expect("test transaction should be signed");
        SendableTxWithHash {
            tx: envelope.encoded_2718(),
            hash: *envelope.tx_hash(),
            nonce: envelope.nonce(),
            signed_at: Utc::now(),
            dust_shares: U256::ZERO,
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn anvil_classifies_persisted_transactions_for_recovery() {
        let evm = LocalEvm::new().await.expect("Anvil should start");
        let signer = PrivateKeySigner::from_bytes(&evm.private_key)
            .expect("Anvil key should parse");
        let owner = signer.address();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller)
            .with_simple_nonce_management()
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .expect("provider should connect");
        let service = RealBlockchainService::new(
            provider.clone(),
            Arc::new(OaSchemaCache::fixed(TEST_OA_SCHEMA)),
        );
        let recipient = address!("0x3333333333333333333333333333333333333333");

        let mineable = sign_test_transaction(
            &provider,
            TransactionRequest::default()
                .from(owner)
                .to(recipient)
                .value(U256::from(1u8)),
        )
        .await;
        assert_eq!(
            service.classify_burn_tx(owner, &mineable).await.unwrap(),
            BurnTxStatus::StillMineable
        );

        provider
            .send_raw_transaction(&mineable.tx)
            .await
            .expect("persisted transaction should broadcast")
            .get_receipt()
            .await
            .expect("persisted transaction should mine");
        assert_eq!(
            service.classify_burn_tx(owner, &mineable).await.unwrap(),
            BurnTxStatus::Mined
        );

        let dead = sign_test_transaction(
            &provider,
            TransactionRequest::default()
                .from(owner)
                .to(recipient)
                .value(U256::from(2u8)),
        )
        .await;
        for nonce_offset in 0..3 {
            provider
                .send_transaction(
                    TransactionRequest::default()
                        .from(owner)
                        .to(recipient)
                        .nonce(dead.nonce + nonce_offset)
                        .value(U256::from(3u64 + nonce_offset)),
                )
                .await
                .expect("competing transaction should broadcast")
                .get_receipt()
                .await
                .expect("competing transaction should mine");
        }
        assert_eq!(
            service.classify_burn_tx(owner, &dead).await.unwrap(),
            BurnTxStatus::StillMineable,
            "Anvil's unfinalized nonce advance must not prove death"
        );

        let invalid_redeem =
            OffchainAssetReceiptVault::new(evm.vault_address, &provider)
                .redeem(U256::from(1u8), owner, owner, U256::MAX, Bytes::new())
                .calldata()
                .clone();
        let reverted = sign_test_transaction(
            &provider,
            TransactionRequest::default()
                .from(owner)
                .to(evm.vault_address)
                .input(invalid_redeem.into())
                .gas_limit(100_000),
        )
        .await;
        let receipt = provider
            .send_raw_transaction(&reverted.tx)
            .await
            .expect("reverting transaction should broadcast")
            .get_receipt()
            .await
            .expect("reverting transaction should mine");
        assert!(!receipt.status(), "test transaction must revert");
        assert_eq!(
            service.classify_burn_tx(owner, &reverted).await.unwrap(),
            BurnTxStatus::Reverted
        );
        for status in ["StillMineable", "Mined", "Reverted"] {
            assert!(logs_contain_at!(
                Level::DEBUG,
                &["Classified persisted burn transaction", status]
            ));
        }
    }

    #[tokio::test]
    async fn test_submit_and_confirm_mint_success() {
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x2222222222222222222222222222222222222222");
        let receipt_info = test_receipt_info();
        let expected_external_tx_id =
            format!("mint-{}", receipt_info.issuer_request_id);
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

        let mut receipt = TransactionReceipt {
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
        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller)
            .with_simple_nonce_management()
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter.clone());
        let service = RealBlockchainService::new(
            provider,
            Arc::new(OaSchemaCache::fixed(TEST_OA_SCHEMA)),
        );

        let prepared = service
            .prepare_mint_tx(
                vault_address,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
                None,
            )
            .await;

        assert!(prepared.is_ok(), "Expected Ok but got: {prepared:?}");
        let prepared = prepared.unwrap();
        assert!(!prepared.tx.is_empty());
        assert_eq!(prepared.external_tx_id, expected_external_tx_id);
        let decoded = TxEnvelope::decode_2718(&mut prepared.tx.as_slice())
            .expect("prepared mint must contain a valid EIP-2718 transaction");
        assert_eq!(decoded.nonce(), prepared.nonce);
        assert_eq!(*decoded.tx_hash(), prepared.hash);

        let mut malformed = prepared.clone();
        malformed.tx = vec![0x02];
        assert!(matches!(
            service.submit_mint(&malformed).await,
            Err(VaultError::Eip2718(_))
        ));

        let mut wrong_hash = prepared.clone();
        wrong_hash.hash = B256::ZERO;
        assert!(matches!(
            service.submit_mint(&wrong_hash).await,
            Err(VaultError::PreparedMintHashMismatch { .. })
        ));

        let mut wrong_nonce = prepared.clone();
        wrong_nonce.nonce = prepared.nonce + 1;
        assert!(matches!(
            service.submit_mint(&wrong_nonce).await,
            Err(VaultError::PreparedMintNonceMismatch { .. })
        ));

        receipt.transaction_hash = prepared.hash;
        asserter.push_success(&prepared.hash);
        asserter.push_success(&Some(receipt));
        let submitted = service.submit_mint(&prepared).await.unwrap();
        assert_eq!(submitted.tx_id, TxId::from(prepared.hash));

        let result = service.confirm_mint(&submitted.tx_id).await;

        assert!(result.is_ok(), "Expected Ok but got: {result:?}");
        let mint_result = result.unwrap();
        assert_eq!(mint_result.tx_hash, prepared.hash);
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

        let mut receipt = TransactionReceipt {
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
        let signer = PrivateKeySigner::random();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .with_gas_estimation()
            .filler(BlobGasFiller)
            .with_simple_nonce_management()
            .filler(ChainIdFiller::default())
            .wallet(EthereumWallet::from(signer))
            .connect_mocked_client(asserter.clone());
        let service = RealBlockchainService::new(
            provider,
            Arc::new(OaSchemaCache::fixed(TEST_OA_SCHEMA)),
        );

        let prepared = service
            .prepare_mint_tx(
                vault_address,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
                None,
            )
            .await;

        assert!(prepared.is_ok(), "Expected Ok but got: {prepared:?}");
        let prepared = prepared.unwrap();
        receipt.transaction_hash = prepared.hash;
        asserter.push_success(&prepared.hash);
        asserter.push_success(&Some(receipt));
        let submitted = service.submit_mint(&prepared).await.unwrap();

        let result = service.confirm_mint(&submitted.tx_id).await;

        assert!(result.is_err(), "Expected Err but got Ok: {result:?}");
        let err = result.unwrap_err();
        assert!(
            matches!(err, VaultError::EventNotFound { .. }),
            "Expected EventNotFound but got: {err:?}"
        );
    }

    fn persisted_mint_tx(nonce: u64) -> PreparedMintTx {
        PreparedMintTx::valid_for_test(nonce, format!("mint-test-{nonce}"))
    }

    #[tokio::test]
    async fn classify_mint_tx_reports_mined_and_reverted_receipts() {
        for (succeeded, expected) in [
            (true, MintTxStatus::MinedSuccess),
            (false, MintTxStatus::MinedReverted),
        ] {
            let persisted = persisted_mint_tx(7);
            let owner = persisted.signer_for_test();
            let mut receipt =
                create_empty_receipt(test_vault_address(), persisted.hash);
            receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                Receipt {
                    status: Eip658Value::Eip658(succeeded),
                    cumulative_gas_used: 0x6100,
                    logs: vec![],
                },
                Bloom::default(),
            ));
            let asserter = Asserter::new();
            asserter.push_success(&receipt);
            let service = create_service_with_asserter(asserter);

            let status = service
                .classify_mint_tx(owner, &persisted)
                .await
                .expect("receipt should classify");

            assert_eq!(status, expected);
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn classify_mint_tx_requires_finalized_nonce_to_prove_death() {
        let persisted = persisted_mint_tx(7);
        let owner = persisted.signer_for_test();

        for (latest_nonce, finalized_nonce, expected) in [
            (7, 7, MintTxStatus::StillMineable),
            (8, 7, MintTxStatus::StillMineable),
            (8, 8, MintTxStatus::ProvablyDead),
        ] {
            let asserter = Asserter::new();
            asserter.push_success(&Option::<TransactionReceipt>::None);
            asserter.push_success(&latest_nonce);
            asserter.push_success(&finalized_nonce);
            if finalized_nonce > persisted.nonce {
                asserter.push_success(&Option::<TransactionReceipt>::None);
                // Death needs the node to have forgotten the transaction too.
                asserter.push_success(&Option::<RpcTransaction>::None);
            }
            let service = create_service_with_asserter(asserter);

            let status = service
                .classify_mint_tx(owner, &persisted)
                .await
                .expect("missing receipt should classify by finalized nonce");

            assert_eq!(status, expected);
        }
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "Classified persisted mint transaction",
                "latest_nonce=8",
                "finalized_nonce=7",
                "StillMineable"
            ]
        ));
    }

    #[tokio::test]
    async fn classify_mint_tx_refuses_death_while_the_node_still_holds_the_tx()
    {
        let persisted = persisted_mint_tx(7);
        let owner = persisted.signer_for_test();
        let asserter = Asserter::new();
        asserter.push_success(&Option::<TransactionReceipt>::None);
        asserter.push_success(&8u64);
        asserter.push_success(&8u64);
        asserter.push_success(&Option::<TransactionReceipt>::None);
        // A finalized nonce past ours says the nonce is spent, but the node
        // answering with the transaction says it is not spent by another. One
        // node cannot have it both ways, so this is not a death proof.
        asserter.push_success(&Some(rpc_transaction(&persisted.tx, owner)));
        let service = create_service_with_asserter(asserter);

        let error = service
            .classify_mint_tx(owner, &persisted)
            .await
            .expect_err("contradictory node answers must not prove death");

        assert!(
            matches!(
                error,
                VaultError::ContradictoryDeathSignals { tx_hash, nonce }
                    if tx_hash == persisted.hash && nonce == persisted.nonce
            ),
            "expected ContradictoryDeathSignals, got {error:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn classify_mint_tx_rechecks_receipt_after_nonce_advances() {
        let persisted = persisted_mint_tx(7);
        let owner = persisted.signer_for_test();
        let receipt =
            create_empty_receipt(test_vault_address(), persisted.hash);
        let asserter = Asserter::new();
        asserter.push_success(&Option::<TransactionReceipt>::None);
        asserter.push_success(&8u64);
        asserter.push_success(&8u64);
        asserter.push_success(&Some(receipt));
        let service = create_service_with_asserter(asserter);

        let status = service
            .classify_mint_tx(owner, &persisted)
            .await
            .expect("the second receipt read should win the nonce race");

        assert_eq!(status, MintTxStatus::MinedSuccess);
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Classified persisted mint transaction", "MinedSuccess"]
        ));
    }

    #[tokio::test]
    async fn classify_mint_tx_rejects_unmined_receipt_shape() {
        let persisted = persisted_mint_tx(7);
        let owner = persisted.signer_for_test();
        let mut receipt =
            create_empty_receipt(test_vault_address(), persisted.hash);
        receipt.block_number = None;
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result = service.classify_mint_tx(owner, &persisted).await;

        assert!(matches!(result, Err(VaultError::InvalidReceipt)));
    }

    #[tokio::test]
    async fn classify_mint_tx_rejects_corrupt_persisted_identity_before_rpc() {
        let mut persisted = persisted_mint_tx(7);
        persisted.hash = B256::ZERO;
        let service = create_service_with_asserter(Asserter::new());

        let result =
            service.classify_mint_tx(test_receiver(), &persisted).await;

        assert!(matches!(
            result,
            Err(VaultError::PreparedMintHashMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn classify_mint_tx_rejects_a_different_signer_before_rpc() {
        let persisted = persisted_mint_tx(7);
        let service = create_service_with_asserter(Asserter::new());

        let result = service.classify_mint_tx(Address::ZERO, &persisted).await;

        assert!(matches!(
            result,
            Err(VaultError::PreparedMintSignerMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn confirm_mint_status_zero_is_reverted() {
        let persisted = persisted_mint_tx(3);
        let mut receipt =
            create_empty_receipt(test_vault_address(), persisted.hash);
        receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt {
                status: Eip658Value::Eip658(false),
                cumulative_gas_used: 0x6100,
                logs: vec![],
            },
            Bloom::default(),
        ));
        let asserter = Asserter::new();
        asserter.push_success(&Some(receipt));
        let service = create_service_with_asserter(asserter);

        let result = service.confirm_mint(&TxId::from(persisted.hash)).await;

        assert!(matches!(
            result,
            Err(VaultError::Reverted { tx_hash }) if tx_hash == persisted.hash
        ));
    }

    #[tokio::test]
    async fn confirm_mint_rpc_error_reports_confirmation_pending() {
        let persisted = persisted_mint_tx(3);
        let asserter = Asserter::new();
        // Transport/RPC blips must fail closed as ConfirmationPending (never
        // Reverted). Direct RPC failure on the first receipt poll avoids the
        // 120s empty-receipt budget in unit tests.
        asserter.push_failure_msg("forced transport blip");
        let service = create_service_with_asserter(asserter);

        let result = service.confirm_mint(&TxId::from(persisted.hash)).await;

        assert!(
            matches!(result, Err(VaultError::ConfirmationPending { .. })),
            "expected ConfirmationPending, got {result:?}"
        );
    }

    /// Empty receipts until the 120s poll budget expires must surface
    /// `ConfirmationPending` (never Reverted / MintingFailed). Uses Tokio's
    /// paused clock so the budget elapses without a real wall-clock wait.
    #[tokio::test(start_paused = true)]
    async fn confirm_mint_poll_budget_exhausted_reports_confirmation_pending() {
        let persisted = persisted_mint_tx(3);
        let asserter = Asserter::new();
        // 120s budget / 2s interval ≈ 60 polls; pad past the deadline.
        for _ in 0..80 {
            asserter.push_success(&Option::<TransactionReceipt>::None);
        }
        let service = create_service_with_asserter(asserter);

        let result = service.confirm_mint(&TxId::from(persisted.hash)).await;

        assert!(
            matches!(
                result,
                Err(VaultError::ConfirmationPending { ref message, .. })
                    if message.contains("budget exhausted")
            ),
            "expected ConfirmationPending budget exhausted, got {result:?}"
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

    fn rpc_transaction(
        encoded_tx: &[u8],
        reported_signer: Address,
    ) -> RpcTransaction {
        let mut encoded = encoded_tx;
        let envelope = TxEnvelope::decode_2718(&mut encoded)
            .expect("test transaction should decode");

        RpcTransaction {
            inner: Recovered::new_unchecked(envelope, reported_signer),
            block_hash: Some(fixed_bytes!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            )),
            block_number: Some(0x7d0),
            transaction_index: Some(0),
            effective_gas_price: Some(0x3b9a_ca00),
        }
    }

    #[tokio::test]
    async fn verify_burn_tx_returns_verified_nonce_from_matching_rpc_transaction()
     {
        let vault_address = test_vault_address();
        let persisted = SendableTxWithHash::valid_for_test(
            17,
            vault_address,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted.signer_for_test();
        let receiver = test_receiver();
        let transaction = rpc_transaction(&persisted.tx, owner);
        let receipt = create_multi_withdraw_receipt(
            vault_address,
            persisted.hash,
            owner,
            receiver,
            vec![(U256::from(7), U256::from(50))],
        );
        let asserter = Asserter::new();
        asserter.push_success(&transaction);
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let verification = service
            .verify_burn_tx(
                vault_address,
                owner,
                persisted.hash,
                BurnProofKind::VaultDirect,
            )
            .await
            .expect("matching transaction and receipt should verify");

        assert_eq!(verification.nonce, persisted.nonce);
        assert_eq!(verification.block_number, 0x9c4);
        assert_eq!(verification.shares_burned, U256::from(50));
        assert_eq!(verification.burns.len(), 1);
    }

    #[tokio::test]
    async fn verify_burn_tx_rejects_rpc_transaction_for_another_hash() {
        let persisted = persisted_burn_tx(17);
        let owner = persisted.signer_for_test();
        let transaction = rpc_transaction(&persisted.tx, owner);
        let requested_tx_hash = B256::random();
        let asserter = Asserter::new();
        asserter.push_success(&transaction);
        let service = create_service_with_asserter(asserter);

        let result = service
            .verify_burn_tx(
                test_vault_address(),
                owner,
                requested_tx_hash,
                BurnProofKind::VaultDirect,
            )
            .await;

        assert!(matches!(result, Err(VaultError::InvalidReceipt)));
    }

    #[tokio::test]
    async fn verify_burn_tx_rejects_rpc_transaction_with_inconsistent_from() {
        let persisted = persisted_burn_tx(17);
        let owner = persisted.signer_for_test();
        let transaction = rpc_transaction(&persisted.tx, Address::random());
        let asserter = Asserter::new();
        asserter.push_success(&transaction);
        let service = create_service_with_asserter(asserter);

        let result = service
            .verify_burn_tx(
                test_vault_address(),
                owner,
                persisted.hash,
                BurnProofKind::VaultDirect,
            )
            .await;

        assert!(matches!(result, Err(VaultError::InvalidReceipt)));
    }

    #[tokio::test]
    async fn verify_burn_tx_rejects_another_signature_when_rpc_reports_owner() {
        let persisted = persisted_burn_tx(17);
        let owner = Address::random();
        let transaction = rpc_transaction(&persisted.tx, owner);
        let asserter = Asserter::new();
        asserter.push_success(&transaction);
        let service = create_service_with_asserter(asserter);

        let result = service
            .verify_burn_tx(
                test_vault_address(),
                owner,
                persisted.hash,
                BurnProofKind::VaultDirect,
            )
            .await;

        assert!(matches!(
            result,
            Err(VaultError::NotABurn { tx_hash }) if tx_hash == persisted.hash
        ));
    }

    #[tokio::test]
    async fn verify_burn_tx_rejects_rpc_receipt_for_another_hash() {
        let persisted = persisted_burn_tx(17);
        let owner = persisted.signer_for_test();
        let transaction = rpc_transaction(&persisted.tx, owner);
        let receipt =
            create_empty_receipt(test_vault_address(), B256::random());
        let asserter = Asserter::new();
        asserter.push_success(&transaction);
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result = service
            .verify_burn_tx(
                test_vault_address(),
                owner,
                persisted.hash,
                BurnProofKind::VaultDirect,
            )
            .await;

        assert!(matches!(result, Err(VaultError::InvalidReceipt)));
    }

    fn persisted_burn_tx(nonce: u64) -> SendableTxWithHash {
        SendableTxWithHash::valid_for_test(
            nonce,
            test_vault_address(),
            Bytes::from_static(&[0xde, 0xad]),
        )
    }

    #[tokio::test]
    async fn classify_burn_tx_reports_mined_and_reverted_receipts() {
        for (succeeded, expected) in
            [(true, BurnTxStatus::Mined), (false, BurnTxStatus::Reverted)]
        {
            let persisted = persisted_burn_tx(7);
            let owner = persisted.signer_for_test();
            let mut receipt =
                create_empty_receipt(test_vault_address(), persisted.hash);
            receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                Receipt {
                    status: Eip658Value::Eip658(succeeded),
                    cumulative_gas_used: 0x6100,
                    logs: vec![],
                },
                Bloom::default(),
            ));
            let asserter = Asserter::new();
            asserter.push_success(&receipt);
            let service = create_service_with_asserter(asserter);

            let status = service
                .classify_burn_tx(owner, &persisted)
                .await
                .expect("receipt should classify");

            assert_eq!(status, expected);
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn classify_burn_tx_requires_finalized_nonce_to_prove_death() {
        let persisted = persisted_burn_tx(7);
        let owner = persisted.signer_for_test();

        for (latest_nonce, finalized_nonce, expected) in [
            (7, 7, BurnTxStatus::StillMineable),
            (8, 7, BurnTxStatus::StillMineable),
            (8, 8, BurnTxStatus::ProvablyDead),
        ] {
            let asserter = Asserter::new();
            asserter.push_success(&Option::<TransactionReceipt>::None);
            asserter.push_success(&latest_nonce);
            asserter.push_success(&finalized_nonce);
            if finalized_nonce > persisted.nonce {
                asserter.push_success(&Option::<TransactionReceipt>::None);
            }
            let service = create_service_with_asserter(asserter);

            let status = service
                .classify_burn_tx(owner, &persisted)
                .await
                .expect("missing receipt should classify by finalized nonce");

            assert_eq!(status, expected);
        }
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "Classified persisted burn transaction",
                "latest_nonce=8",
                "finalized_nonce=7",
                "StillMineable"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn classify_burn_tx_rechecks_receipt_after_nonce_advances() {
        let persisted = persisted_burn_tx(7);
        let owner = persisted.signer_for_test();
        let receipt =
            create_empty_receipt(test_vault_address(), persisted.hash);
        let asserter = Asserter::new();
        asserter.push_success(&Option::<TransactionReceipt>::None);
        asserter.push_success(&8u64);
        asserter.push_success(&8u64);
        asserter.push_success(&Some(receipt));
        let service = create_service_with_asserter(asserter);

        let status = service
            .classify_burn_tx(owner, &persisted)
            .await
            .expect("the second receipt read should win the nonce race");

        assert_eq!(status, BurnTxStatus::Mined);
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Classified persisted burn transaction", "Mined"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn replacement_uses_pending_nonce_to_avoid_live_transaction_collision()
     {
        let persisted = persisted_burn_tx(7);
        let owner = persisted.signer_for_test();
        let pending_nonce = 11u64;
        let asserter = Asserter::new();
        asserter.push_success(&pending_nonce);
        asserter.push_success(&100_000u64);
        asserter.push_success(&test_fee_history());
        asserter
            .push_success(&Block::<alloy::rpc::types::Transaction>::default());
        asserter.push_success(&1_000_000_000u64);
        asserter.push_success(&100_000u64);
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(1))
            .expect("test private key should be valid");
        let service = create_service_with_signer(asserter, signer);

        let replacement = service
            .prepare_replacement_burn_tx(owner, &persisted)
            .await
            .expect("replacement should use the pending wallet nonce");

        assert_eq!(replacement.nonce, pending_nonce);
        let previous_envelope =
            persisted.validate().expect("persisted tx should decode");
        let replacement_envelope =
            replacement.validate().expect("replacement should decode");
        assert_eq!(replacement_envelope.to(), previous_envelope.to());
        assert_eq!(replacement_envelope.value(), previous_envelope.value());
        assert_eq!(replacement_envelope.input(), previous_envelope.input());
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "Prepared fresh-nonce burn replacement",
                "previous_nonce=7",
                "replacement_nonce=11"
            ]
        ));
    }

    #[tokio::test]
    async fn classify_burn_tx_rejects_unmined_receipt_shape() {
        let persisted = persisted_burn_tx(7);
        let owner = persisted.signer_for_test();
        let mut receipt =
            create_empty_receipt(test_vault_address(), persisted.hash);
        receipt.block_number = None;
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result = service.classify_burn_tx(owner, &persisted).await;

        assert!(matches!(result, Err(VaultError::InvalidReceipt)));
    }

    #[tokio::test]
    async fn classify_burn_tx_rejects_corrupt_persisted_identity_before_rpc() {
        let mut persisted = persisted_burn_tx(7);
        persisted.hash = B256::ZERO;
        let service = create_service_with_asserter(Asserter::new());

        let result =
            service.classify_burn_tx(test_receiver(), &persisted).await;

        assert!(matches!(
            result,
            Err(VaultError::PreparedBurnHashMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn classify_burn_tx_rejects_a_different_signer_before_rpc() {
        let persisted = persisted_burn_tx(7);
        let service = create_service_with_asserter(Asserter::new());

        let result = service.classify_burn_tx(Address::ZERO, &persisted).await;

        assert!(matches!(
            result,
            Err(VaultError::PreparedBurnSignerMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn check_tx_rejects_reverted_receipt_without_block_number() {
        let vault_address = test_vault_address();
        let tx_hash = fixed_bytes!(
            "0x7070707070707070707070707070707070707070707070707070707070707070"
        );
        let mut receipt = create_empty_receipt(vault_address, tx_hash);
        receipt.block_hash = None;
        receipt.block_number = None;
        receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt {
                status: Eip658Value::Eip658(false),
                cumulative_gas_used: 0x6100,
                logs: vec![],
            },
            Bloom::default(),
        ));
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result = service.check_tx(&TxId::Hash(tx_hash)).await;

        assert!(
            matches!(result, Err(VaultError::InvalidReceipt)),
            "unexpected check result: {result:?}"
        );
    }

    #[tokio::test]
    async fn check_tx_rejects_successful_receipt_without_block_number() {
        let vault_address = test_vault_address();
        let tx_hash = fixed_bytes!(
            "0x7272727272727272727272727272727272727272727272727272727272727272"
        );
        let mut receipt = create_empty_receipt(vault_address, tx_hash);
        receipt.block_hash = None;
        receipt.block_number = None;
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result = service.check_tx(&TxId::Hash(tx_hash)).await;

        assert!(
            matches!(result, Err(VaultError::MissingBlockNumber { tx_hash: hash }) if hash == tx_hash),
            "unexpected check result: {result:?}"
        );
    }

    #[tokio::test]
    async fn check_tx_reports_mined_revert() {
        let vault_address = test_vault_address();
        let tx_hash = fixed_bytes!(
            "0x7171717171717171717171717171717171717171717171717171717171717171"
        );
        let mut receipt = create_empty_receipt(vault_address, tx_hash);
        receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt {
                status: Eip658Value::Eip658(false),
                cumulative_gas_used: 0x6100,
                logs: vec![],
            },
            Bloom::default(),
        ));
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result = service.check_tx(&TxId::Hash(tx_hash)).await;

        assert!(
            matches!(result, Err(VaultError::Reverted { tx_hash: hash }) if hash == tx_hash),
            "unexpected check result: {result:?}"
        );
    }

    #[tokio::test]
    async fn check_tx_rejects_mismatched_receipt_hash() {
        let vault_address = test_vault_address();
        let requested_tx_hash = fixed_bytes!(
            "0x7272727272727272727272727272727272727272727272727272727272727272"
        );
        let receipt_tx_hash = fixed_bytes!(
            "0x7373737373737373737373737373737373737373737373737373737373737373"
        );

        for succeeded in [true, false] {
            let mut receipt =
                create_empty_receipt(vault_address, receipt_tx_hash);
            receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                Receipt {
                    status: Eip658Value::Eip658(succeeded),
                    cumulative_gas_used: 0x6100,
                    logs: vec![],
                },
                Bloom::default(),
            ));
            let asserter = Asserter::new();
            asserter.push_success(&receipt);
            asserter.push_success(&receipt);
            let service = create_service_with_asserter(asserter);

            let result = service.check_tx(&TxId::Hash(requested_tx_hash)).await;

            assert!(
                matches!(result, Err(VaultError::InvalidReceipt)),
                "succeeded: {succeeded}, unexpected check result: {result:?}"
            );
        }
    }

    fn create_multi_withdraw_receipt(
        vault_address: Address,
        tx_hash: B256,
        owner: Address,
        user: Address,
        burns: Vec<(U256, U256)>,
    ) -> TransactionReceipt {
        let shares_burned =
            burns.iter().fold(U256::ZERO, |total, (_, shares)| total + shares);
        let transfer_event = OffchainAssetReceiptVault::Transfer {
            from: owner,
            to: Address::ZERO,
            value: shares_burned,
        };
        let mut logs = vec![alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: vault_address,
                data: transfer_event.into_log_data(),
            },
            block_hash: Some(b256!(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            )),
            block_number: Some(0x9c4),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }];
        logs.extend(burns.into_iter().enumerate().map(
            |(index, (receipt_id, shares))| {
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
                    log_index: Some(index as u64 + 1),
                    removed: false,
                }
            },
        ));

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
        let user = address!("0x3333333333333333333333333333333333333333");

        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            vault_address,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let tx_hash = prepared_tx.hash;
        let owner = prepared_tx.signer_for_test();

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
                    origin: BurnRequestOrigin::Redemption(
                        test_issuer_redemption_id(),
                    ),
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
        let user = address!("0x3333333333333333333333333333333333333333");

        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            vault_address,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let tx_hash = prepared_tx.hash;
        let owner = prepared_tx.signer_for_test();

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
                    origin: BurnRequestOrigin::Redemption(
                        test_issuer_redemption_id(),
                    ),
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
    async fn submit_burn_rejects_a_node_hash_for_different_bytes() {
        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            test_vault_address(),
            Bytes::from_static(&[0xde, 0xad]),
        );
        let returned = B256::random();
        let asserter = Asserter::new();
        asserter.push_success(&returned);
        let service = create_service_with_asserter(asserter);

        let result = service
            .submit_burn(
                test_multi_burn_params(prepared_tx.signer_for_test()),
                prepared_tx.clone(),
            )
            .await;

        assert!(matches!(
            result,
            Err(VaultError::BroadcastHashMismatch { expected, returned: actual })
                if expected == prepared_tx.hash && actual == returned
        ));
    }

    #[tokio::test]
    async fn test_submit_burn_returns_error_on_missing_events() {
        let vault_address = test_vault_address();
        let user = address!("0x6666666666666666666666666666666666666666");

        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            vault_address,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let tx_hash = prepared_tx.hash;
        let owner = prepared_tx.signer_for_test();

        let receipt = create_empty_receipt(vault_address, tx_hash);

        let asserter = Asserter::new();
        setup_asserter_for_transaction(&asserter, tx_hash, &receipt);

        let service = create_service_with_asserter(asserter);

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
                origin: BurnRequestOrigin::Redemption(test_issuer_redemption_id()),
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
            origin: BurnRequestOrigin::Redemption(test_issuer_redemption_id()),
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
            origin: BurnRequestOrigin::Redemption(test_issuer_redemption_id()),
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

    fn test_orchestrator_address() -> Address {
        address!("0x00000000000000000000000000000000000000aa")
    }

    fn test_orchestrator_burn_params(owner: Address) -> OrchestratorBurnParams {
        OrchestratorBurnParams {
            orchestrator: test_orchestrator_address(),
            token: test_vault_address(),
            amount: U256::from(1_000_000u64),
            owner,
            issuer_request_id: test_issuer_redemption_id(),
            detected_tx_hash: b256!(
                "0xabababababababababababababababababababababababababababababababab"
            ),
            external_tx_id: None,
        }
    }

    fn create_burned_receipt(
        tx_hash: B256,
        caller: Address,
        amount: U256,
        burn_range: (U256, U256),
        succeeded: bool,
    ) -> TransactionReceipt {
        create_burned_receipt_from(
            tx_hash,
            caller,
            amount,
            burn_range,
            succeeded,
            test_orchestrator_address(),
        )
    }

    /// Like [`create_burned_receipt`], with the `Burned` log's emitting
    /// contract chosen by the caller — for pinning that the confirm path
    /// binds the event to the orchestrator the transaction targeted.
    fn create_burned_receipt_from(
        tx_hash: B256,
        caller: Address,
        amount: U256,
        burn_range: (U256, U256),
        succeeded: bool,
        emitter: Address,
    ) -> TransactionReceipt {
        let burned_event = IST0xOrchestratorV1::Burned {
            caller,
            token: test_vault_address(),
            amount,
            firstReceiptId: burn_range.0,
            nextBurnReceiptIdAfter: burn_range.1,
        };

        let logs = if succeeded {
            vec![alloy::rpc::types::Log {
                inner: alloy::primitives::Log {
                    address: emitter,
                    data: burned_event.into_log_data(),
                },
                block_hash: Some(b256!(
                    "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                )),
                block_number: Some(0x9c4),
                block_timestamp: None,
                transaction_hash: Some(tx_hash),
                transaction_index: Some(0),
                log_index: Some(0),
                removed: false,
            }]
        } else {
            vec![]
        };

        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(succeeded),
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
            // The sender is the burn's `caller` — the confirm path binds the
            // decoded event's `caller` to `receipt.from`.
            from: caller,
            to: Some(test_orchestrator_address()),
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

    /// A same-signature `Burned` event emitted by a contract other than the
    /// orchestrator the transaction targeted must never terminalize the
    /// burn's accounting: the confirm path binds the log to `receipt.to` and
    /// reads anything else as `EventNotFound`.
    #[tokio::test]
    async fn confirm_orchestrator_burn_ignores_foreign_burned_logs() {
        let tx_hash = b256!(
            "0x7777777777777777777777777777777777777777777777777777777777777777"
        );
        let receipt = create_burned_receipt_from(
            tx_hash,
            address!("2222222222222222222222222222222222222222"),
            U256::from(5u8),
            (U256::ZERO, U256::ONE),
            true,
            test_vault_address(),
        );
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result =
            service.confirm_orchestrator_burn(&TxId::Hash(tx_hash)).await;

        assert!(
            matches!(
                result,
                Err(VaultError::EventNotFound { tx_hash: hash })
                    if hash == tx_hash
            ),
            "a foreign contract's Burned log must not confirm the burn, \
             got {result:?}"
        );
    }

    #[tokio::test]
    async fn orchestrator_burn_prepare_submit_confirm_round_trip() {
        let asserter = Asserter::new();
        setup_asserter_for_fill(&asserter, 7);
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let service = create_service_with_signer(asserter.clone(), signer);

        let params = test_orchestrator_burn_params(owner);

        let prepared = service
            .prepare_orchestrator_burn_tx(&params)
            .await
            .expect("expected SendableTxWithHash");

        assert_eq!(prepared.nonce, 7);
        assert_eq!(
            prepared.dust_shares,
            U256::ZERO,
            "orchestrator burns never encode a dust return"
        );
        let envelope = TxEnvelope::decode_2718(&mut prepared.tx.as_slice())
            .expect("prepared orchestrator burn must decode");
        assert_eq!(*envelope.tx_hash(), prepared.hash);
        assert_eq!(envelope.to(), Some(test_orchestrator_address()));
        let expected_calldata = IST0xOrchestratorV1::burnCall {
            token: params.token,
            amount: params.amount,
            burnInfo: Bytes::new(),
        }
        .abi_encode();
        assert_eq!(
            envelope.input().as_ref(),
            expected_calldata.as_slice(),
            "calldata must be burn(token, amount, empty burnInfo)"
        );

        let receipt = create_burned_receipt(
            prepared.hash,
            owner,
            params.amount,
            (U256::from(3u8), U256::from(6u8)),
            true,
        );
        setup_asserter_for_transaction(&asserter, prepared.hash, &receipt);

        let submitted = service
            .submit_orchestrator_burn(&params, &prepared)
            .await
            .expect("expected SubmittedTx");
        assert_eq!(submitted.tx_id, TxId::Hash(prepared.hash));
        assert_eq!(
            submitted.external_tx_id,
            format!("burn-{}", params.detected_tx_hash),
            "deterministic externalTxId must derive from the detected transfer"
        );

        let result = service
            .confirm_orchestrator_burn(&submitted.tx_id)
            .await
            .expect("expected OrchestratorBurnResult");
        assert_eq!(result.tx_hash, prepared.hash);
        assert_eq!(result.shares_burned, params.amount);
        assert_eq!(result.burn_range, (U256::from(3u8), U256::from(6u8)));
        assert_eq!(result.gas_used, 0x8000);
        assert_eq!(result.block_number, 0x9c4);
    }

    #[tokio::test]
    async fn confirm_orchestrator_burn_without_burned_event_is_not_found() {
        let tx_hash = b256!(
            "0x9999999999999999999999999999999999999999999999999999999999999999"
        );
        let mut receipt = create_burned_receipt(
            tx_hash,
            test_receiver(),
            U256::from(1u8),
            (U256::ZERO, U256::ZERO),
            true,
        );
        receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt {
                status: Eip658Value::Eip658(true),
                cumulative_gas_used: 0x8000,
                logs: vec![],
            },
            Bloom::default(),
        ));
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        let service = create_service_with_asserter(asserter);

        let result =
            service.confirm_orchestrator_burn(&TxId::Hash(tx_hash)).await;

        assert!(matches!(
            result,
            Err(VaultError::EventNotFound { tx_hash: hash }) if hash == tx_hash
        ));
    }

    /// A mined-but-reverted orchestrator burn replays the transaction as an
    /// `eth_call` at its mined block and decodes the typed revert reason.
    #[tokio::test]
    async fn confirm_orchestrator_burn_decodes_typed_revert_reasons() {
        let shortfall_error = IST0xOrchestratorV1::InsufficientReceipts {
            token: test_vault_address(),
            shortfall: U256::from(250u64),
        };
        let cases = [
            (
                Bytes::from(shortfall_error.abi_encode()),
                OrchestratorRevertReason::InsufficientReceipts {
                    token: test_vault_address(),
                    shortfall: U256::from(250u64),
                },
            ),
            (
                Bytes::from(
                    IST0xOrchestratorV1::VaultLogicMismatch {
                        expected: test_receiver(),
                        actual: test_vault_address(),
                    }
                    .abi_encode(),
                ),
                OrchestratorRevertReason::VaultLogicMismatch,
            ),
            (
                Bytes::from(
                    crate::bindings::IST0xOrchestratorV1::ReceiptLogicMismatch {
                        expected: test_receiver(),
                        actual: test_vault_address(),
                    }
                    .abi_encode(),
                ),
                super::OrchestratorRevertReason::ReceiptLogicMismatch,
            ),
            (
                Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
                OrchestratorRevertReason::Unknown,
            ),
        ];

        for (revert_data, expected_reason) in cases {
            let persisted = SendableTxWithHash::valid_for_test(
                17,
                test_orchestrator_address(),
                Bytes::from_static(&[0xde, 0xad]),
            );
            let owner = persisted.signer_for_test();
            let receipt = create_burned_receipt(
                persisted.hash,
                owner,
                U256::ZERO,
                (U256::ZERO, U256::ZERO),
                false,
            );
            let transaction = rpc_transaction(&persisted.tx, owner);

            let asserter = Asserter::new();
            asserter.push_success(&receipt); // eth_getTransactionReceipt
            asserter.push_success(&receipt); // eth_getTransactionReceipt (polling)
            asserter.push_success(&transaction); // eth_getTransactionByHash
            asserter.push_failure(ErrorPayload {
                code: 3,
                message: "execution reverted".into(),
                data: Some(
                    serde_json::value::to_raw_value(&format!("{revert_data}"))
                        .expect("revert data must serialize"),
                ),
            }); // eth_call replay
            let service = create_service_with_asserter(asserter);

            let result = service
                .confirm_orchestrator_burn(&TxId::Hash(persisted.hash))
                .await;

            assert!(
                matches!(
                    &result,
                    Err(VaultError::OrchestratorReverted { tx_hash, reason })
                        if *tx_hash == persisted.hash
                            && *reason == expected_reason
                ),
                "expected {expected_reason:?}, got {result:?}"
            );
        }
    }

    /// The allowance gate is evaluated before the orchestrator health gate,
    /// so an approval shortfall is reported even while the orchestrator is
    /// halted.
    #[tokio::test]
    async fn orchestrator_readiness_checks_allowance_before_health() {
        let owner = test_receiver();
        let amount = U256::from(1_000u64);

        // Allowance below the amount: readiness must report the shortfall
        // without ever querying vaultLogicIsExpected (a second eth_call
        // would fail the asserter with a missing response).
        let asserter = Asserter::new();
        asserter.push_success(&format!("0x{:064x}", 999u64));
        let service = create_service_with_asserter(asserter);
        let readiness = service
            .check_orchestrator_burn_readiness(
                test_orchestrator_address(),
                test_vault_address(),
                owner,
                amount,
            )
            .await
            .expect("readiness check should succeed");
        assert_eq!(
            readiness,
            OrchestratorBurnReadiness::AllowanceInsufficient {
                required: amount,
                current: U256::from(999u64),
            }
        );

        // Sufficient allowance but halted orchestrator.
        let asserter = Asserter::new();
        asserter.push_success(&format!("0x{}", "f".repeat(64)));
        asserter.push_success(&format!("0x{:064x}", 0u64));
        let service = create_service_with_asserter(asserter);
        let readiness = service
            .check_orchestrator_burn_readiness(
                test_orchestrator_address(),
                test_vault_address(),
                owner,
                amount,
            )
            .await
            .expect("readiness check should succeed");
        assert_eq!(readiness, OrchestratorBurnReadiness::VaultLogicMismatch);

        // Sufficient allowance, healthy orchestrator, burn simulation passes.
        let asserter = Asserter::new();
        asserter.push_success(&format!("0x{}", "f".repeat(64)));
        asserter.push_success(&format!("0x{:064x}", 1u64));
        asserter.push_success(&"0x"); // burn simulation (empty return)
        let service = create_service_with_asserter(asserter);
        let readiness = service
            .check_orchestrator_burn_readiness(
                test_orchestrator_address(),
                test_vault_address(),
                owner,
                amount,
            )
            .await
            .expect("readiness check should succeed");
        assert_eq!(readiness, OrchestratorBurnReadiness::Ready);

        // Simulation reverting with InsufficientReceipts classifies the
        // shortfall before anything is signed.
        {
            let revert_data = Bytes::from(
                IST0xOrchestratorV1::InsufficientReceipts {
                    token: test_vault_address(),
                    shortfall: U256::from(123u64),
                }
                .abi_encode(),
            );
            let asserter = Asserter::new();
            asserter.push_success(&format!("0x{}", "f".repeat(64)));
            asserter.push_success(&format!("0x{:064x}", 1u64));
            asserter.push_failure(ErrorPayload {
                code: 3,
                message: "execution reverted".into(),
                data: Some(
                    serde_json::value::to_raw_value(&format!("{revert_data}"))
                        .expect("revert data must serialize"),
                ),
            });
            let service = create_service_with_asserter(asserter);
            let readiness = service
                .check_orchestrator_burn_readiness(
                    test_orchestrator_address(),
                    test_vault_address(),
                    owner,
                    amount,
                )
                .await
                .expect("readiness check should succeed");
            assert_eq!(
                readiness,
                OrchestratorBurnReadiness::InsufficientReceipts {
                    shortfall: U256::from(123u64),
                }
            );
        }

        // A deterministic revert with undecodable data must fall through to
        // Ready — preparation replays the same revert and records it as
        // Unclassified — rather than erroring, which would defer the
        // redemption forever.
        {
            let asserter = Asserter::new();
            asserter.push_success(&format!("0x{}", "f".repeat(64)));
            asserter.push_success(&format!("0x{:064x}", 1u64));
            asserter.push_failure(ErrorPayload {
                code: 3,
                message: "execution reverted".into(),
                data: Some(
                    serde_json::value::to_raw_value("0xdeadbeef")
                        .expect("revert data must serialize"),
                ),
            });
            let service = create_service_with_asserter(asserter);
            let readiness = service
                .check_orchestrator_burn_readiness(
                    test_orchestrator_address(),
                    test_vault_address(),
                    owner,
                    amount,
                )
                .await
                .expect("an undecodable revert must not error the gate");
            assert_eq!(readiness, OrchestratorBurnReadiness::Ready);
        }

        // A simulation that fails without revert data is a transport fault,
        // not a burn revert: it must propagate so the reconciler retries.
        {
            let asserter = Asserter::new();
            asserter.push_success(&format!("0x{}", "f".repeat(64)));
            asserter.push_success(&format!("0x{:064x}", 1u64));
            asserter.push_failure(ErrorPayload {
                code: -32603,
                message: "internal error".into(),
                data: None,
            });
            let service = create_service_with_asserter(asserter);
            assert!(
                service
                    .check_orchestrator_burn_readiness(
                        test_orchestrator_address(),
                        test_vault_address(),
                        owner,
                        amount,
                    )
                    .await
                    .is_err(),
                "a transport failure must not be classified as a readiness outcome"
            );
        }
    }
}
