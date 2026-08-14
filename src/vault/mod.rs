use alloy::consensus::transaction::SignerRecoverable;
#[cfg(test)]
use alloy::consensus::{SignableTransaction, TxLegacy};
use alloy::consensus::{Transaction, TxEnvelope};
use alloy::eips::Decodable2718;
#[cfg(test)]
use alloy::eips::Encodable2718;
use alloy::hex::decode;
#[cfg(test)]
use alloy::primitives::TxKind;
use alloy::primitives::{Address, B256, Bytes, FixedBytes, U256};
use alloy::providers::SendableTxErr;
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
#[cfg(test)]
use alloy::signers::SignerSync;
#[cfg(test)]
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::bindings::OffchainAssetReceiptVault;
use crate::burn_excess::BurnExcessId;
use crate::mint::{
    IssuerMintRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
};
use crate::redemption::{BurnExternalTxId, IssuerRedemptionRequestId};

pub(crate) mod mock;
pub(crate) mod network_services;
pub(crate) mod orchestrator;
pub(crate) mod rain_meta;
pub(crate) mod service;

pub(crate) use network_services::{
    NetworkVault, NetworkVaultServices, UnconfiguredNetworkError,
};

pub(crate) use orchestrator::{
    BurnRange, OrchestratorBurnParams, OrchestratorBurnReadiness,
    OrchestratorBurnResult, OrchestratorRevertReason,
};

/// Service abstraction for vault operations.
///
/// This trait provides an interface for minting and burning tokenized assets on-chain via the
/// Rain OffchainAssetReceiptVault contract. Implementations can be real blockchain
/// services or mocks for testing.
#[async_trait]
pub(crate) trait VaultService: Send + Sync {
    /// Builds and signs a mint transaction without broadcasting it.
    ///
    /// The returned bytes and hash must be persisted before calling
    /// [`VaultService::submit_mint`].
    ///
    /// `external_tx_id` is a signing-backend correlation id (Fireblocks
    /// idempotency / Turnkey labeling). It is **not** vault-direct double-mint
    /// protection — after the first `MintTxIntended`, only exact-hash
    /// classification / rebroadcast of the persisted prepared bytes (or
    /// inventory) is safe.
    async fn prepare_mint_tx(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
        external_tx_id: Option<String>,
    ) -> Result<PreparedMintTx, VaultError>;

    /// Broadcasts the exact signed mint transaction that was persisted before
    /// this call. Repeated calls must rebroadcast the same bytes rather than
    /// prepare a replacement transaction.
    async fn submit_mint(
        &self,
        prepared_tx: &PreparedMintTx,
    ) -> Result<SubmittedTx, VaultError>;

    /// Confirms a previously submitted mint transaction.
    ///
    /// Bounded-polls `eth_getTransactionReceipt` as `Option` (never uses
    /// `PendingTransactionBuilder::get_receipt` as the terminal classifier).
    /// Terminal outcomes: mined success with a `Deposit` log, mined revert
    /// (`status=0`), or [`VaultError::ConfirmationPending`] when the poll
    /// budget is exhausted with no receipt. Provider uncertainty fails closed.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Backend transaction ID from [`SubmittedTx`]
    async fn confirm_mint(
        &self,
        tx_id: &TxId,
    ) -> Result<MintResult, VaultError>;

    /// Classifies whether a persisted signed mint transaction can still land.
    ///
    /// Implementations must check the exact hash receipt before comparing the
    /// owner's finalized nonce. Any provider uncertainty returns an error so
    /// callers fail closed and keep the persisted transaction live. `Uncertain`
    /// is expressed as `Err`, never as a success variant.
    async fn classify_mint_tx(
        &self,
        _owner: Address,
        _prepared_tx: &PreparedMintTx,
    ) -> Result<MintTxStatus, VaultError> {
        Ok(MintTxStatus::StillMineable)
    }

    /// Gets the ERC-20 share balance for an address.
    ///
    /// This queries the vault contract's balanceOf(address) to get the total
    /// share balance for the given address.
    ///
    /// # Arguments
    ///
    /// * `vault` - Address of the vault contract to query
    /// * `owner` - Address to check the balance for
    ///
    /// # Returns
    ///
    /// The share balance (with 18 decimals).
    async fn get_share_balance(
        &self,
        vault: Address,
        owner: Address,
    ) -> Result<U256, VaultError>;

    /// Submits a multi-receipt burn transaction to the signing backend.
    ///
    /// Encodes the multicall calldata (N redeems + optional dust transfer)
    /// and submits it. Returns a [`SubmittedTx`] for later confirmation.
    async fn submit_burn(
        &self,
        params: MultiBurnParams,
        sendable_tx: SendableTxWithHash,
    ) -> Result<SubmittedTx, VaultError>;

    /// Confirms a previously submitted burn transaction.
    ///
    /// Polls the signing backend until completion, then parses Withdraw events.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Backend transaction ID from [`SubmittedTx`]
    /// * `dust_shares` - Amount of dust to report as returned (passed through
    ///   from the original request since it cannot be derived from on-chain events)
    async fn confirm_burn(
        &self,
        tx_id: &TxId,
        dust_shares: U256,
    ) -> Result<MultiBurnResult, VaultError>;

    /// Verifies an operator-supplied burn transaction hash on-chain.
    ///
    /// Fetches the receipt for `tx_hash` and confirms it proves a burn of
    /// `vault` shares by `owner` — a successful transaction emitting at least
    /// one `Transfer(owner -> 0x0)` from the vault share token — and returns
    /// the signer nonce, owner's per-receipt `Withdraw` entries, and share
    /// transfers. Used by the admin
    /// force-complete path to bind an alternate proving transaction to the
    /// persisted burn plan before terminalizing the redemption.
    async fn verify_burn_tx(
        &self,
        vault: Address,
        owner: Address,
        tx_hash: B256,
    ) -> Result<BurnVerification, VaultError>;

    /// Prepares a signed raw transaction for `eth_sendRawTransaction`.
    async fn prepare_burn_tx(
        &self,
        _params: &MultiBurnParams,
    ) -> Result<SendableTxWithHash, VaultError>;

    /// Classifies whether a persisted signed burn transaction can still land.
    ///
    /// Implementations must check the exact hash receipt before comparing the
    /// owner's finalized nonce. Any provider uncertainty returns an error so
    /// callers fail closed and keep the persisted transaction live.
    async fn classify_burn_tx(
        &self,
        _owner: Address,
        _sendable_tx: &SendableTxWithHash,
    ) -> Result<BurnTxStatus, VaultError> {
        Ok(BurnTxStatus::StillMineable)
    }

    /// Re-signs the persisted burn's exact call at a fresh nonce.
    ///
    /// Callers may invoke this only after [`VaultService::classify_burn_tx`]
    /// proves the persisted hash dead. Implementations must preserve the
    /// destination, value, and calldata rather than reconstructing the burn
    /// from a lossy projection, and must assign the owner's pending account
    /// nonce rather than relying on a local nonce cache.
    async fn prepare_replacement_burn_tx(
        &self,
        _owner: Address,
        _sendable_tx: &SendableTxWithHash,
    ) -> Result<SendableTxWithHash, VaultError> {
        Err(VaultError::InvalidReceipt)
    }

    /// Fetches the on-chain receipt for `tx_hash`, returning an error if the
    /// transaction reverted.
    async fn check_tx(
        &self,
        _tx_id: &TxId,
    ) -> Result<TransactionReceipt, VaultError>;

    /// Serializes the local wallet's nonce assignment through broadcast.
    async fn lock_wallet(&self) -> WalletNonceGuard {
        None
    }

    /// Runs the pre-submit orchestrator burn gates: the ERC-20 allowance
    /// check, then `vaultLogicIsExpected()`. See
    /// [`OrchestratorBurnReadiness`] for the outcome semantics.
    ///
    /// The default implementation fails closed for implementations without
    /// orchestrator support (mirroring
    /// [`VaultService::prepare_replacement_burn_tx`]).
    #[allow(dead_code)]
    async fn check_orchestrator_burn_readiness(
        &self,
        _orchestrator: Address,
        _token: Address,
        _owner: Address,
        _amount: U256,
    ) -> Result<OrchestratorBurnReadiness, VaultError> {
        Err(VaultError::InvalidReceipt)
    }

    /// Builds and signs the `ST0xOrchestrator.burn()` transaction without
    /// broadcasting it. The returned bytes must be persisted (via
    /// `BurnIntended`) before [`VaultService::submit_orchestrator_burn`].
    async fn prepare_orchestrator_burn_tx(
        &self,
        _params: &OrchestratorBurnParams,
    ) -> Result<SendableTxWithHash, VaultError> {
        Err(VaultError::InvalidReceipt)
    }

    /// Broadcasts the exact signed orchestrator burn persisted before this
    /// call. Repeated calls must rebroadcast the same bytes rather than sign
    /// a replacement.
    async fn submit_orchestrator_burn(
        &self,
        _params: &OrchestratorBurnParams,
        _sendable_tx: &SendableTxWithHash,
    ) -> Result<SubmittedTx, VaultError> {
        Err(VaultError::InvalidReceipt)
    }

    /// Confirms a previously submitted orchestrator burn, parsing the
    /// orchestrator's `Burned` event. A mined-but-reverted burn fails with
    /// [`VaultError::OrchestratorReverted`] carrying the decoded typed
    /// reason.
    async fn confirm_orchestrator_burn(
        &self,
        _tx_id: &TxId,
    ) -> Result<OrchestratorBurnResult, VaultError> {
        Err(VaultError::InvalidReceipt)
    }
}

pub(crate) type WalletNonceGuard = Option<tokio::sync::OwnedMutexGuard<()>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BurnTxStatus {
    Mined,
    Reverted,
    StillMineable,
    ProvablyDead,
}

/// Observation of a persisted signed vault-direct mint transaction.
///
/// Parallel to [`BurnTxStatus`]. Provider/identity uncertainty is **not** a
/// variant — callers receive `Err` and must fail closed (no `MintingFailed`, no
/// replacement).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MintTxStatus {
    MinedSuccess,
    MinedReverted,
    StillMineable,
    ProvablyDead,
}

impl SendableTxWithHash {
    pub(crate) fn validate(&self) -> Result<TxEnvelope, VaultError> {
        let envelope = TxEnvelope::decode_2718_exact(&self.tx)?;
        let decoded_hash = *envelope.tx_hash();
        if decoded_hash != self.hash {
            return Err(VaultError::PreparedBurnHashMismatch {
                expected: self.hash,
                decoded: decoded_hash,
            });
        }
        let decoded_nonce = envelope.nonce();
        if decoded_nonce != self.nonce {
            return Err(VaultError::PreparedBurnNonceMismatch {
                expected: self.nonce,
                decoded: decoded_nonce,
            });
        }
        Ok(envelope)
    }

    pub(crate) fn validate_for_owner(
        &self,
        owner: Address,
    ) -> Result<TxEnvelope, VaultError> {
        let envelope = self.validate()?;
        let signer = envelope.recover_signer()?;
        if signer != owner {
            return Err(VaultError::PreparedBurnSignerMismatch {
                expected: owner,
                decoded: signer,
            });
        }
        Ok(envelope)
    }

    pub(crate) fn validate_replacement_for_owner(
        &self,
        previous: &Self,
        owner: Address,
    ) -> Result<(), VaultError> {
        let previous_envelope = previous.validate_for_owner(owner)?;
        let replacement_envelope = self.validate_for_owner(owner)?;
        if replacement_envelope.to() != previous_envelope.to() {
            return Err(VaultError::BurnReplacementDestinationMismatch {
                previous: previous_envelope.to(),
                replacement: replacement_envelope.to(),
            });
        }
        if replacement_envelope.value() != previous_envelope.value() {
            return Err(VaultError::BurnReplacementValueMismatch {
                previous: previous_envelope.value(),
                replacement: replacement_envelope.value(),
            });
        }
        if replacement_envelope.input() != previous_envelope.input() {
            return Err(VaultError::BurnReplacementInputMismatch {
                previous: previous_envelope.input().clone(),
                replacement: replacement_envelope.input().clone(),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn valid_for_test(
        nonce: u64,
        destination: Address,
        input: Bytes,
    ) -> Self {
        let transaction = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 1,
            gas_limit: 100_000,
            to: TxKind::Call(destination),
            value: U256::ZERO,
            input,
        };
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(1))
            .expect("test private key should be valid");
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .expect("test transaction should sign");
        let envelope = TxEnvelope::from(transaction.into_signed(signature));

        Self {
            tx: envelope.encoded_2718(),
            hash: *envelope.tx_hash(),
            nonce: envelope.nonce(),
            signed_at: Utc::now(),
            dust_shares: U256::ZERO,
        }
    }

    #[cfg(test)]
    pub(crate) fn signer_for_test(&self) -> Address {
        self.validate()
            .expect("test burn transaction should decode")
            .recover_signer()
            .expect("test burn transaction signature should recover")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct PreparedMintTx {
    pub(crate) tx: Vec<u8>,
    pub(crate) hash: FixedBytes<32>,
    pub(crate) nonce: u64,
    pub(crate) signed_at: DateTime<Utc>,
    pub(crate) external_tx_id: String,
}

impl PreparedMintTx {
    /// Verifies that the redundant persisted identity fields describe the
    /// exact signed envelope bytes.
    pub(crate) fn validate(&self) -> Result<TxEnvelope, VaultError> {
        let envelope = TxEnvelope::decode_2718_exact(&self.tx)?;
        let decoded_hash = *envelope.tx_hash();
        if decoded_hash != self.hash {
            return Err(VaultError::PreparedMintHashMismatch {
                expected: self.hash,
                decoded: decoded_hash,
            });
        }

        let decoded_nonce = envelope.nonce();
        if decoded_nonce != self.nonce {
            return Err(VaultError::PreparedMintNonceMismatch {
                expected: self.nonce,
                decoded: decoded_nonce,
            });
        }

        Ok(envelope)
    }

    /// Like [`Self::validate`], and requires the recovered signer to match
    /// `owner` (the bot wallet whose nonce is authoritative for death proof).
    pub(crate) fn validate_for_owner(
        &self,
        owner: Address,
    ) -> Result<TxEnvelope, VaultError> {
        let envelope = self.validate()?;
        let signer = envelope.recover_signer()?;
        if signer != owner {
            return Err(VaultError::PreparedMintSignerMismatch {
                expected: owner,
                decoded: signer,
            });
        }
        Ok(envelope)
    }

    /// Recovers the signing address from the persisted envelope after
    /// identity validation. Used when classify needs an owner and the job
    /// context does not carry the bot address.
    pub(crate) fn recover_signer(&self) -> Result<Address, VaultError> {
        Ok(self.validate()?.recover_signer()?)
    }

    #[cfg(test)]
    pub(crate) fn valid_for_test(nonce: u64, external_tx_id: String) -> Self {
        let transaction = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 1,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(1))
            .expect("test private key should be valid");
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .expect("test mint transaction should sign");
        let envelope = TxEnvelope::from(transaction.into_signed(signature));

        Self {
            tx: envelope.encoded_2718(),
            hash: *envelope.tx_hash(),
            nonce: envelope.nonce(),
            signed_at: Utc::now(),
            external_tx_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn signer_for_test(&self) -> Address {
        self.recover_signer()
            .expect("test mint transaction signature should recover")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct SendableTxWithHash {
    pub(crate) tx: Vec<u8>,
    pub(crate) hash: B256,
    pub(crate) nonce: u64,
    pub(crate) signed_at: DateTime<Utc>,
    pub(crate) dust_shares: U256,
}

/// Proof that a burn transaction landed on-chain, returned by
/// [`VaultService::verify_burn_tx`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BurnVerification {
    /// Block number the burn transaction was included in.
    pub(crate) block_number: u64,
    /// Signer nonce of the mined transaction. An alternate proof must replace
    /// the persisted transaction at this exact nonce.
    pub(crate) nonce: u64,
    /// Total shares burned by the owner in this transaction (sum of all
    /// matching `Transfer(owner -> 0x0)` events). Alternate-burn recovery
    /// requires this to equal the persisted burn plan's aggregate total.
    pub(crate) shares_burned: U256,
    /// Per-receipt withdrawals emitted for this owner. Admin recovery uses
    /// these to bind an alternate proving transaction to the redemption's
    /// persisted burn plan.
    pub(crate) burns: Vec<VerifiedBurn>,
    /// Non-burn share transfers sent by the owner in the same transaction.
    /// A redemption's dust return must match these exactly.
    pub(crate) share_transfers: Vec<VerifiedShareTransfer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedBurn {
    pub(crate) sender: Address,
    pub(crate) receiver: Address,
    pub(crate) receipt_id: U256,
    pub(crate) shares_burned: U256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedShareTransfer {
    pub(crate) recipient: Address,
    pub(crate) shares: U256,
}

/// Verifies that `receipt` proves `owner` burned shares of the `vault` share
/// token (one or more `Transfer(owner -> 0x0)` events emitted by `vault`).
///
/// A burn emits an ERC-20 `Transfer(owner, address(0), shares)` from the vault
/// share token. Reference chain: `redeem()` -> `ReceiptVault._withdraw()` ->
/// `_burn(owner, shares)` -> `ERC20Upgradeable._update(owner, 0x0, shares)`
/// emits `Transfer(owner, 0x0, shares)`. Mirrors the mint-skip check in
/// `redemption/transfer.rs`, which treats `from == 0x0` as a mint.
pub(crate) fn verify_burn_in_receipt(
    receipt: &TransactionReceipt,
    vault: Address,
    owner: Address,
    tx_hash: B256,
    nonce: u64,
) -> Result<BurnVerification, VaultError> {
    if !receipt.status() {
        return Err(VaultError::Reverted { tx_hash });
    }

    let mut shares_burned = U256::ZERO;
    let mut found_burn = false;
    let mut burns = Vec::new();
    let mut share_transfers = Vec::new();

    for log in receipt.inner.logs() {
        if log.address() != vault {
            continue;
        }

        if let Ok(decoded) =
            log.log_decode::<OffchainAssetReceiptVault::Transfer>()
        {
            let transfer = decoded.data();
            if transfer.from == owner && transfer.to == Address::ZERO {
                found_burn = true;
                shares_burned = shares_burned
                    .checked_add(transfer.value)
                    .ok_or(VaultError::InvalidReceipt)?;
            } else if transfer.from == owner {
                share_transfers.push(VerifiedShareTransfer {
                    recipient: transfer.to,
                    shares: transfer.value,
                });
            }
        }

        if let Ok(decoded) =
            log.log_decode::<OffchainAssetReceiptVault::Withdraw>()
        {
            let withdrawal = decoded.data();
            if withdrawal.owner == owner {
                burns.push(VerifiedBurn {
                    sender: withdrawal.sender,
                    receiver: withdrawal.receiver,
                    receipt_id: withdrawal.id,
                    shares_burned: withdrawal.shares,
                });
            }
        }
    }

    if !found_burn {
        return Err(VaultError::NotABurn { tx_hash });
    }

    let block_number =
        receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

    Ok(BurnVerification {
        block_number,
        nonce,
        shares_burned,
        burns,
        share_transfers,
    })
}

/// Result of a successful on-chain minting operation.
///
/// Contains all transaction details needed to track the mint in the Mint aggregate
/// and for audit trails.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MintResult {
    /// Transaction hash of the deposit transaction
    pub(crate) tx_hash: B256,
    /// ERC-1155 receipt ID issued by the vault
    pub(crate) receipt_id: U256,
    /// Number of ERC-20 shares minted (with 18 decimals)
    pub(crate) shares_minted: U256,
    /// Gas consumed by the transaction
    pub(crate) gas_used: u64,
    /// Block number where the transaction was included
    pub(crate) block_number: u64,
    /// The exact encoded bytes passed to deposit() on-chain.
    /// Preserved so that register_minted_receipt stores the same bytes
    /// that were committed on-chain, avoiding encoding mismatches.
    pub(crate) receipt_info_bytes: Bytes,
}

/// A single burn within a multi-receipt burn operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MultiBurnEntry {
    /// ERC-1155 receipt ID to burn from
    pub(crate) receipt_id: U256,
    /// Amount of shares to burn from this receipt
    pub(crate) burn_shares: U256,
    /// Original mint's receipt information (for on-chain audit trail).
    /// `None` for external receipts or receipts minted before this feature.
    pub(crate) receipt_info: Option<ReceiptInformation>,
    /// Original on-chain encoded bytes from the deposit event.
    /// When present, these exact bytes are passed to redeem() to preserve
    /// the original encoding (avoiding re-encoding legacy JSON as CBOR).
    /// Falls back to encoding `receipt_info` when absent (old events).
    #[serde(default)]
    pub(crate) receipt_info_bytes: Option<Bytes>,
}

/// Which domain flow asked for a burn.
///
/// The vault burn path serves both the redemption pipeline and the operator
/// excess-recovery CLI. Each owns its own aggregate keyspace, so the
/// correlation id must carry that keyspace with it: an excess recovery must
/// not be able to name a slot in the `Redemption` keyspace, even by accident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BurnRequestOrigin {
    Redemption(IssuerRedemptionRequestId),
    ExcessRecovery(BurnExcessId),
}

/// Parameters for a multi-receipt burn operation.
///
/// Atomically burns shares from multiple receipts in a single transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MultiBurnParams {
    /// Address of the vault contract
    pub(crate) vault: Address,
    /// List of burns to perform (receipt_id, burn_amount, per-entry receipt info)
    pub(crate) burns: Vec<MultiBurnEntry>,
    /// Amount of dust to return to user (can be zero)
    pub(crate) dust_shares: U256,
    /// Address that owns the shares being burned (typically bot wallet)
    pub(crate) owner: Address,
    /// User's address that will receive the dust
    pub(crate) user: Address,
    /// Domain flow this burn belongs to, and its correlation ID
    pub(crate) origin: BurnRequestOrigin,
    /// Full transaction hash that triggered this redemption, used for
    /// constructing a collision-resistant `externalTxId`.
    pub(crate) detected_tx_hash: B256,
    /// Optional deterministic `externalTxId` override for replacement burn
    /// retries after a previously accepted transaction failed.
    #[serde(default)]
    pub(crate) external_tx_id: Option<BurnExternalTxId>,
}

/// Result of a single burn within a multi-receipt burn operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MultiBurnResultEntry {
    /// ERC-1155 receipt ID that was burned from
    pub(crate) receipt_id: U256,
    /// Number of ERC-20 shares burned from this receipt
    pub(crate) shares_burned: U256,
}

/// Result of a successful multi-receipt burn operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MultiBurnResult {
    /// Transaction hash of the multicall transaction
    pub(crate) tx_hash: B256,
    /// Per-receipt burn results
    pub(crate) burns: Vec<MultiBurnResultEntry>,
    /// Amount of dust returned to user (with 18 decimals)
    pub(crate) dust_returned: U256,
    /// Gas consumed by the transaction
    pub(crate) gas_used: u64,
    /// Block number where the transaction was included
    pub(crate) block_number: u64,
}

/// Metadata emitted on-chain with each vault deposit and withdrawal.
///
/// Encoded as a Rain metadata v1 document (CBOR with magic prefix) and passed
/// as the `receiptInformation` parameter to `deposit()` and `redeem()`. The
/// contract emits this data in events, providing an on-chain audit trail
/// linking receipts to off-chain tokenization requests.
///
/// The encoding uses the `OA_STRUCTURE` magic number with deflated JSON payload,
/// matching the format expected by the h20.market UI.
///
/// Only constructed for mints (deposits). When burning (withdrawing),
/// the original mint's `ReceiptInformation` is passed back to the contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReceiptInformation {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) quantity: Quantity,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) notes: Option<String>,
}

impl ReceiptInformation {
    pub(crate) const fn new(
        tokenization_request_id: TokenizationRequestId,
        issuer_request_id: IssuerMintRequestId,
        underlying: UnderlyingSymbol,
        quantity: Quantity,
        timestamp: DateTime<Utc>,
        notes: Option<String>,
    ) -> Self {
        Self {
            tokenization_request_id,
            issuer_request_id,
            underlying,
            quantity,
            timestamp,
            notes,
        }
    }

    /// Encodes the receipt information as a Rain metadata v1 document.
    ///
    /// Format: rain meta prefix + CBOR map with deflated JSON payload,
    /// `OA_STRUCTURE` magic number, and optional `OA_SCHEMA` IPFS CID.
    pub(crate) fn encode(
        &self,
        oa_schema: Option<&str>,
    ) -> Result<Bytes, ReceiptEncodeError> {
        let json_bytes = serde_json::to_vec(self)?;
        let rain_meta = rain_meta::encode_receipt_meta(&json_bytes, oa_schema)?;
        Ok(Bytes::from(rain_meta))
    }
}

/// Result of submitting a transaction to the signing backend.
///
/// Returned by `submit_mint` and `submit_burn`. The `tx_id` is
/// persisted in an intermediate CQRS event so that `confirm_mint`/`confirm_burn`
/// can resume polling after a restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SubmittedTx {
    /// Deterministic ID used for idempotency (`externalTxId`).
    /// Format: `{operation}-{issuer_request_id}`.
    pub(crate) external_tx_id: String,
    /// Backend-specific transaction identifier.
    /// For local and Turnkey backends: the on-chain transaction hash.
    pub(crate) tx_id: TxId,
}

/// Errors that can occur when encoding receipt information.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptEncodeError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    RainMeta(#[from] rain_meta::RainMetaError),
}

/// Errors that can occur during vault operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultError {
    /// Transaction receipt is missing required data
    #[error("Invalid receipt")]
    InvalidReceipt,
    /// A transaction receipt was returned without proof of block inclusion.
    #[error("Transaction receipt is missing a block number: {tx_hash:?}")]
    MissingBlockNumber { tx_hash: B256 },
    /// Expected event (e.g., Deposit) not found in transaction logs
    #[error("Event not found in transaction: {tx_hash:?}")]
    EventNotFound { tx_hash: B256 },
    /// Transaction was mined but reverted on-chain (status == 0).
    ///
    /// A reverted burn consumes no receipts, so any inventory reservation
    /// held for it must be released.
    #[error("Transaction reverted on-chain: {tx_hash:?}")]
    Reverted { tx_hash: B256 },
    /// The node's own answers contradict each other: the sender's finalized
    /// nonce is past this transaction's, so the nonce is permanently spent,
    /// yet the node still knows the transaction and reports no receipt for
    /// it. A death proof cannot be read out of an inconsistent node, so this
    /// is uncertainty, not a terminal identity.
    #[error(
        "node reports nonce {nonce} finalized but still knows unmined \
         transaction {tx_hash:?}"
    )]
    ContradictoryDeathSignals { tx_hash: B256, nonce: u64 },
    /// An orchestrator transaction was mined but reverted, with the revert
    /// data decoded into a typed reason. Like [`VaultError::Reverted`], this
    /// is a definitive on-chain failure.
    #[error(
        "Orchestrator transaction reverted on-chain: {tx_hash:?} ({reason:?})"
    )]
    OrchestratorReverted { tx_hash: B256, reason: OrchestratorRevertReason },
    /// Transaction was mined and succeeded but does not prove the expected
    /// burn — it contains no `Transfer(owner -> 0x0)` of the vault's shares.
    /// The operator-supplied hash cannot terminalize the redemption.
    #[error("Transaction is not a burn of the expected shares: {tx_hash:?}")]
    NotABurn { tx_hash: B256 },
    /// The mined burn's `Burned` event names a token or amount different
    /// from the transaction's own decoded calldata. The hash pins our
    /// persisted bytes, so a divergence means the orchestrator emitted an
    /// event our burn never requested — accounting must not terminalize on
    /// it.
    #[error("Burned event diverges from the mined burn calldata: {tx_hash:?}")]
    BurnedEventMismatch { tx_hash: B256 },
    #[error(
        "Node returned transaction hash {returned:?} for persisted transaction {expected:?}"
    )]
    BroadcastHashMismatch { expected: B256, returned: B256 },
    #[error(
        "Persisted mint transaction hash {expected:?} does not match decoded hash {decoded:?}"
    )]
    PreparedMintHashMismatch { expected: B256, decoded: B256 },
    #[error(
        "Persisted mint transaction nonce {expected} does not match decoded nonce {decoded}"
    )]
    PreparedMintNonceMismatch { expected: u64, decoded: u64 },
    #[error(
        "Persisted mint transaction signer {decoded:?} does not match wallet {expected:?}"
    )]
    PreparedMintSignerMismatch { expected: Address, decoded: Address },
    #[error(
        "Persisted burn transaction hash {expected:?} does not match decoded hash {decoded:?}"
    )]
    PreparedBurnHashMismatch { expected: B256, decoded: B256 },
    #[error(
        "Persisted burn transaction nonce {expected} does not match decoded nonce {decoded}"
    )]
    PreparedBurnNonceMismatch { expected: u64, decoded: u64 },
    #[error(
        "Persisted burn transaction signer {decoded:?} does not match wallet {expected:?}"
    )]
    PreparedBurnSignerMismatch { expected: Address, decoded: Address },
    #[error(
        "Burn replacement destination {replacement:?} differs from persisted destination {previous:?}"
    )]
    BurnReplacementDestinationMismatch {
        previous: Option<Address>,
        replacement: Option<Address>,
    },
    #[error(
        "Burn replacement value {replacement} differs from persisted value {previous}"
    )]
    BurnReplacementValueMismatch { previous: U256, replacement: U256 },
    #[error("Burn replacement calldata differs from persisted calldata")]
    BurnReplacementInputMismatch { previous: Bytes, replacement: Bytes },
    #[error(transparent)]
    SignerRecovery(#[from] alloy::consensus::crypto::RecoveryError),
    #[error(transparent)]
    Eip2718(#[from] alloy::eips::eip2718::Eip2718Error),
    /// Contract call error
    #[error(transparent)]
    Contract(#[from] alloy::contract::Error),
    #[error(transparent)]
    ReceiptEncode(#[from] ReceiptEncodeError),
    /// Failed to get transaction receipt
    #[error(transparent)]
    PendingTransaction(#[from] alloy::providers::PendingTransactionError),
    /// The transaction may still be pending after receipt confirmation ended.
    /// Retrying with a newly signed transaction is unsafe until this exact
    /// transaction reaches a definitive terminal state.
    #[error("Transaction confirmation remains pending for {tx_id}: {message}")]
    ConfirmationPending { tx_id: TxId, message: String },
    /// RPC transport error (e.g., fetching receipt by tx hash during recovery)
    #[error(transparent)]
    Rpc(
        #[from]
        alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    ),
    #[error(transparent)]
    SendableTxErr(#[from] Box<SendableTxErr<TransactionRequest>>),
}

fn classify_checked_receipt(
    tx_hash: B256,
    receipt: TransactionReceipt,
) -> Result<TransactionReceipt, VaultError> {
    if receipt.transaction_hash != tx_hash {
        return Err(VaultError::InvalidReceipt);
    }
    if !receipt.status() {
        if receipt.block_number.is_none() {
            return Err(VaultError::InvalidReceipt);
        }
        return Err(VaultError::Reverted { tx_hash });
    }
    if receipt.block_number.is_none() {
        return Err(VaultError::MissingBlockNumber { tx_hash });
    }

    Ok(receipt)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TxId {
    Hash(B256),
    Legacy(String),
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum TaggedTxIdRef<'value> {
    Hash(&'value B256),
    Legacy(&'value str),
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum TaggedTxId {
    Hash(B256),
    Legacy(String),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PersistedTxId {
    Tagged(TaggedTxId),
    Flat(String),
}

impl TxId {
    pub(crate) const fn to_hash(&self) -> Option<B256> {
        if let Self::Hash(hash) = self {
            return Some(*hash);
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn random() -> Self {
        Self::Hash(B256::random())
    }
}

impl From<B256> for TxId {
    fn from(value: B256) -> Self {
        Self::Hash(value)
    }
}

impl std::fmt::Display for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hash(tx_hash) => write!(f, "{tx_hash:#x}"),
            Self::Legacy(id) => write!(f, "{id}"),
        }
    }
}

impl std::str::FromStr for TxId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(bytes) = decode(s)
            && bytes.len() == 32
        {
            return Ok(Self::Hash(B256::from_slice(&bytes)));
        }
        Ok(Self::Legacy(s.to_string()))
    }
}

impl utoipa::ToSchema for TxId {
    fn name() -> std::borrow::Cow<'static, str> {
        "TxId".into()
    }
}

impl utoipa::PartialSchema for TxId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::ObjectBuilder::new()
            .schema_type(utoipa::openapi::schema::Type::String)
            .description(Some(
                "On-chain transaction hash (0x-prefixed 32-byte hex) \
                    or legacy backend transaction ID",
            ))
            .into()
    }
}

impl Serialize for TxId {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match self {
            Self::Hash(hash) => TaggedTxIdRef::Hash(hash).serialize(serializer),
            Self::Legacy(value) => {
                TaggedTxIdRef::Legacy(value).serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for TxId {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        match PersistedTxId::deserialize(deserializer)? {
            PersistedTxId::Tagged(TaggedTxId::Hash(hash)) => {
                Ok(Self::Hash(hash))
            }
            PersistedTxId::Tagged(TaggedTxId::Legacy(value)) => {
                Ok(Self::Legacy(value))
            }
            PersistedTxId::Flat(value) => {
                value.parse().map_err(serde::de::Error::custom)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{
        Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
    };
    use alloy::primitives::{Bloom, Bytes, IntoLogData, address, b256};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::mint::{IssuerMintRequestId, Quantity, TokenizationRequestId};

    #[test]
    fn tx_id_tagged_persistence_roundtrips_variants() {
        let legacy_uuid =
            TxId::Legacy("07bdef3c-5314-4d1d-94f7-f3f346cd4c2f".to_string());
        let legacy_hex = TxId::Legacy(
            "1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
        );
        let hash = TxId::Hash(b256!(
            "2222222222222222222222222222222222222222222222222222222222222222"
        ));

        let cases =
            [(legacy_uuid, "legacy"), (legacy_hex, "legacy"), (hash, "hash")];

        for (tx_id, expected_tag) in cases {
            let serialized = serde_json::to_value(&tx_id).unwrap();
            let roundtripped: TxId =
                serde_json::from_value(serialized.clone()).unwrap();

            assert_eq!(roundtripped, tx_id);
            assert!(serialized.get(expected_tag).is_some());
        }
    }

    #[test]
    fn tx_id_deserializes_historical_flat_strings() {
        let legacy: TxId =
            serde_json::from_str(r#""07bdef3c-5314-4d1d-94f7-f3f346cd4c2f""#)
                .unwrap();
        let hash: TxId = serde_json::from_str(
            r#""0x2222222222222222222222222222222222222222222222222222222222222222""#,
        )
        .unwrap();

        assert_eq!(
            legacy,
            TxId::Legacy("07bdef3c-5314-4d1d-94f7-f3f346cd4c2f".to_string())
        );
        assert_eq!(
            hash,
            TxId::Hash(b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            ))
        );
    }

    const TEST_OA_SCHEMA: &str =
        "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm";

    fn sample_receipt_information() -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL").unwrap(),
            Quantity::new(dec!(100.5)),
            Utc::now(),
            Some("test mint".to_string()),
        )
    }

    #[test]
    fn encode_produces_rain_meta_with_valid_json_payload() {
        let info = sample_receipt_information();
        let encoded = info.encode(Some(TEST_OA_SCHEMA)).unwrap();

        assert!(rain_meta::is_rain_meta(&encoded));

        let json_bytes = rain_meta::decode_receipt_meta(&encoded).unwrap();
        let decoded: serde_json::Value =
            serde_json::from_slice(&json_bytes).unwrap();

        assert_eq!(
            decoded["tokenization_request_id"].as_str(),
            Some("tok-123")
        );
        assert_eq!(
            decoded["issuer_request_id"].as_str(),
            Some(info.issuer_request_id.to_string().as_str())
        );
        assert_eq!(decoded["underlying"].as_str(), Some("AAPL"));
        assert_eq!(decoded["quantity"].as_str(), Some("100.5"));
        assert_eq!(decoded["notes"].as_str(), Some("test mint"));
    }

    #[test]
    fn encode_roundtrips_through_rain_meta() {
        let original = sample_receipt_information();

        let encoded = original.encode(Some(TEST_OA_SCHEMA)).unwrap();
        let json_bytes = rain_meta::decode_receipt_meta(&encoded).unwrap();
        let decoded: ReceiptInformation =
            serde_json::from_slice(&json_bytes).unwrap();

        assert_eq!(decoded.issuer_request_id, original.issuer_request_id);
    }

    #[test]
    fn encode_handles_none_notes() {
        let info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL").unwrap(),
            Quantity::new(dec!(100.5)),
            Utc::now(),
            None,
        );

        let encoded = info.encode(Some(TEST_OA_SCHEMA)).unwrap();
        let json_bytes = rain_meta::decode_receipt_meta(&encoded).unwrap();
        let decoded: serde_json::Value =
            serde_json::from_slice(&json_bytes).unwrap();

        assert!(decoded["notes"].is_null());
    }

    const BOT_WALLET: Address = alloy::primitives::address!(
        "0x1111111111111111111111111111111111111111"
    );
    const VAULT: Address = alloy::primitives::address!(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    const BURN_TX: B256 = alloy::primitives::b256!(
        "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
    );

    /// Builds a receipt containing the given `(contract, from, to, value)`
    /// Transfer events, with the given on-chain success status.
    fn transfer_receipt(
        success: bool,
        block_number: u64,
        transfers: Vec<(Address, Address, Address, U256)>,
    ) -> TransactionReceipt {
        let logs: Vec<alloy::rpc::types::Log> = transfers
            .into_iter()
            .enumerate()
            .map(|(index, (contract, from, to, value))| {
                let transfer =
                    OffchainAssetReceiptVault::Transfer { from, to, value };

                alloy::rpc::types::Log {
                    inner: alloy::primitives::Log {
                        address: contract,
                        data: transfer.into_log_data(),
                    },
                    block_hash: None,
                    block_number: Some(block_number),
                    block_timestamp: None,
                    transaction_hash: Some(BURN_TX),
                    transaction_index: Some(0),
                    log_index: Some(index as u64),
                    removed: false,
                }
            })
            .collect();

        receipt_with_logs(success, block_number, logs)
    }

    fn receipt_with_logs(
        success: bool,
        block_number: u64,
        logs: Vec<alloy::rpc::types::Log>,
    ) -> TransactionReceipt {
        let consensus_receipt: Receipt<alloy::rpc::types::Log> = Receipt {
            status: Eip658Value::Eip658(success),
            cumulative_gas_used: 0x8000,
            logs,
        };

        TransactionReceipt {
            transaction_hash: BURN_TX,
            transaction_index: Some(0),
            block_hash: None,
            block_number: Some(block_number),
            from: BOT_WALLET,
            to: Some(VAULT),
            gas_used: 0x8000,
            effective_gas_price: 0,
            contract_address: None,
            blob_gas_used: None,
            blob_gas_price: None,
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                consensus_receipt,
                Bloom::default(),
            )),
        }
    }

    #[test]
    fn verify_burn_in_receipt_accepts_owner_to_zero_transfer() {
        let receipt = transfer_receipt(
            true,
            45_989_009,
            vec![(VAULT, BOT_WALLET, Address::ZERO, U256::from(17u64))],
        );

        let verification =
            verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX, 7)
                .unwrap();

        assert_eq!(verification.block_number, 45_989_009);
        assert_eq!(verification.shares_burned, U256::from(17u64));
    }

    #[test]
    fn verify_burn_in_receipt_reports_owner_withdrawals_by_receipt() {
        let transfer = OffchainAssetReceiptVault::Transfer {
            from: BOT_WALLET,
            to: Address::ZERO,
            value: U256::from(17u64),
        };
        let withdrawal = OffchainAssetReceiptVault::Withdraw {
            sender: BOT_WALLET,
            receiver: address!("0x3333333333333333333333333333333333333333"),
            owner: BOT_WALLET,
            assets: U256::from(17u64),
            shares: U256::from(17u64),
            id: U256::from(42u64),
            receiptInformation: Bytes::new(),
        };
        let dust_transfer = OffchainAssetReceiptVault::Transfer {
            from: BOT_WALLET,
            to: address!("0x3333333333333333333333333333333333333333"),
            value: U256::from(3u64),
        };
        let logs = [
            transfer.into_log_data(),
            withdrawal.into_log_data(),
            dust_transfer.into_log_data(),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, data)| alloy::rpc::types::Log {
            inner: alloy::primitives::Log { address: VAULT, data },
            block_hash: None,
            block_number: Some(45_989_009),
            block_timestamp: None,
            transaction_hash: Some(BURN_TX),
            transaction_index: Some(0),
            log_index: Some(index as u64),
            removed: false,
        })
        .collect();
        let receipt = receipt_with_logs(true, 45_989_009, logs);

        let verification =
            verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX, 7)
                .unwrap();

        assert_eq!(
            verification.burns,
            vec![VerifiedBurn {
                sender: BOT_WALLET,
                receiver: address!(
                    "0x3333333333333333333333333333333333333333"
                ),
                receipt_id: U256::from(42u64),
                shares_burned: U256::from(17u64),
            }]
        );
        assert_eq!(
            verification.share_transfers,
            vec![VerifiedShareTransfer {
                recipient: address!(
                    "0x3333333333333333333333333333333333333333"
                ),
                shares: U256::from(3u64),
            }]
        );
    }

    #[test]
    fn verify_burn_in_receipt_sums_multiple_burns_ignoring_noise() {
        let other = address!("0x2222222222222222222222222222222222222222");
        let other_vault =
            address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let receipt = transfer_receipt(
            true,
            100,
            vec![
                // A genuine burn by the bot on this vault.
                (VAULT, BOT_WALLET, Address::ZERO, U256::from(10u64)),
                // A second burn fragment by the bot on this vault.
                (VAULT, BOT_WALLET, Address::ZERO, U256::from(5u64)),
                // Burn by someone else — ignored.
                (VAULT, other, Address::ZERO, U256::from(99u64)),
                // Bot transfer to a user (not a burn) — ignored.
                (VAULT, BOT_WALLET, other, U256::from(99u64)),
                // Burn on a different vault — ignored.
                (other_vault, BOT_WALLET, Address::ZERO, U256::from(99u64)),
            ],
        );

        let verification =
            verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX, 7)
                .unwrap();

        assert_eq!(verification.shares_burned, U256::from(15u64));
    }

    #[test]
    fn verify_burn_in_receipt_rejects_reverted_tx() {
        let receipt = transfer_receipt(
            false,
            100,
            vec![(VAULT, BOT_WALLET, Address::ZERO, U256::from(17u64))],
        );

        let err =
            verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX, 7)
                .unwrap_err();

        assert!(matches!(err, VaultError::Reverted { .. }));
    }

    #[test]
    fn verify_burn_in_receipt_rejects_non_burn_tx() {
        let user = address!("0x3333333333333333333333333333333333333333");
        // Successful tx, but the bot only transferred to a user — no burn.
        let receipt = transfer_receipt(
            true,
            100,
            vec![(VAULT, BOT_WALLET, user, U256::from(17u64))],
        );

        let err =
            verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX, 7)
                .unwrap_err();

        assert!(matches!(err, VaultError::NotABurn { .. }));
    }

    const HASH_HEX: &str =
        "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf";

    fn known_hash() -> B256 {
        BURN_TX
    }

    #[test]
    fn tx_id_from_b256_is_hash_variant() {
        let tx_id = TxId::from(known_hash());
        assert_eq!(tx_id, TxId::Hash(known_hash()));
    }

    #[test]
    fn tx_id_to_hash_returns_inner_for_hash_variant() {
        let tx_id = TxId::Hash(known_hash());
        assert_eq!(tx_id.to_hash(), Some(known_hash()));
    }

    #[test]
    fn tx_id_to_hash_returns_none_for_legacy_variant() {
        let tx_id = TxId::Legacy("fb-tx-uuid-123".to_string());
        assert_eq!(tx_id.to_hash(), None);
    }

    #[test]
    fn tx_id_random_produces_hash_variant() {
        let tx_id = TxId::random();
        assert!(matches!(tx_id, TxId::Hash(_)));
    }

    #[test]
    fn tx_id_display_hash_emits_0x_hex() {
        let tx_id = TxId::Hash(known_hash());
        assert_eq!(tx_id.to_string(), HASH_HEX);
    }

    #[test]
    fn tx_id_display_legacy_emits_raw_string() {
        let raw = "fb-tx-uuid-123".to_string();
        let tx_id = TxId::Legacy(raw.clone());
        assert_eq!(tx_id.to_string(), raw);
    }

    #[test]
    fn tx_id_from_str_0x_hex_parses_as_hash() {
        let tx_id: TxId = HASH_HEX.parse().unwrap();
        assert_eq!(tx_id, TxId::Hash(known_hash()));
    }

    #[test]
    fn tx_id_from_str_non_hex_parses_as_legacy() {
        let raw = "fb-tx-uuid-123";
        let tx_id: TxId = raw.parse().unwrap();
        assert_eq!(tx_id, TxId::Legacy(raw.to_string()));
    }

    #[test]
    fn tx_id_from_str_short_hex_parses_as_legacy() {
        // Fewer than 32 bytes — not a full tx hash.
        let raw = "0xdeadbeef";
        let tx_id: TxId = raw.parse().unwrap();
        assert_eq!(tx_id, TxId::Legacy(raw.to_string()));
    }

    #[test]
    fn tx_id_from_str_roundtrips_hash_via_display() {
        let original = TxId::Hash(known_hash());
        let roundtripped: TxId = original.to_string().parse().unwrap();
        assert_eq!(roundtripped, original);
    }

    #[test]
    fn tx_id_from_str_roundtrips_legacy_via_display() {
        let original = TxId::Legacy("fb-tx-uuid-123".to_string());
        let roundtripped: TxId = original.to_string().parse().unwrap();
        assert_eq!(roundtripped, original);
    }

    #[test]
    fn tx_id_deserialize_0x_hex_string_as_hash() {
        let json = format!("\"{HASH_HEX}\"");
        let tx_id: TxId = serde_json::from_str(&json).unwrap();
        assert_eq!(tx_id, TxId::Hash(known_hash()));
    }

    #[test]
    fn tx_id_deserialize_legacy_string_as_legacy() {
        let raw = "fb-tx-uuid-123";
        let tx_id: TxId = serde_json::from_str(&format!("\"{raw}\"")).unwrap();
        assert_eq!(tx_id, TxId::Legacy(raw.to_string()));
    }

    #[test]
    fn tx_id_serde_hash_roundtrips() {
        let original = TxId::Hash(known_hash());
        let json = serde_json::to_string(&original).unwrap();
        let decoded: TxId = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn tx_id_serde_legacy_roundtrips() {
        let original = TxId::Legacy("fb-tx-uuid-123".to_string());
        let json = serde_json::to_string(&original).unwrap();
        let decoded: TxId = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn tx_id_openapi_schema_is_string_type() {
        use utoipa::PartialSchema;
        let schema_ref = TxId::schema();
        let json = serde_json::to_value(&schema_ref).unwrap();
        assert_eq!(json["type"], "string", "TxId schema must be type: string");
        assert!(
            json["description"].as_str().is_some(),
            "TxId schema must have a description"
        );
    }
}
