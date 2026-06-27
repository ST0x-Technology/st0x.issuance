use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::bindings::OffchainAssetReceiptVault;
use crate::mint::{
    IssuerMintRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
};
use crate::redemption::{BurnExternalTxId, IssuerRedemptionRequestId};

#[cfg(test)]
pub(crate) mod mock;
pub(crate) mod rain_meta;
pub(crate) mod service;

/// Service abstraction for vault operations.
///
/// This trait provides an interface for minting and burning tokenized assets on-chain via the
/// Rain OffchainAssetReceiptVault contract. Implementations can be real blockchain
/// services or mocks for testing.
#[async_trait]
pub(crate) trait VaultService: Send + Sync {
    /// Submits a mint transaction (deposit + share transfer) to the signing backend.
    ///
    /// Encodes the multicall calldata and submits it via the backend (Fireblocks
    /// CONTRACT_CALL or direct send). Returns a [`SubmittedTx`] whose
    /// `fireblocks_tx_id` is persisted in a CQRS event so that `confirm_mint`
    /// can resume polling after a restart.
    ///
    /// Uses a deterministic `external_tx_id` so that resubmitting the same mint
    /// after a crash triggers Fireblocks' duplicate rejection instead of a
    /// double-mint.
    async fn submit_mint(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
        external_tx_id: Option<String>,
    ) -> Result<SubmittedTx, VaultError>;

    /// Confirms a previously submitted mint transaction.
    ///
    /// Polls the signing backend until the transaction reaches a terminal state,
    /// then fetches the on-chain receipt and parses the Deposit event.
    ///
    /// # Arguments
    ///
    /// * `fireblocks_tx_id` - Backend transaction ID from [`SubmittedTx`]
    async fn confirm_mint(
        &self,
        fireblocks_tx_id: &str,
    ) -> Result<MintResult, VaultError>;

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

    /// Checks the status of a Fireblocks transaction by its ID.
    ///
    /// Returns `Ok(None)` for non-Fireblocks backends (no Fireblocks state to check).
    /// Returns `Ok(Some(status))` if the transaction was found on Fireblocks.
    ///
    /// This is a single-shot check, not a polling loop. Used by the admin recovery
    /// endpoint to detect burns that succeeded on-chain but weren't recorded.
    async fn check_fireblocks_tx(
        &self,
        _fireblocks_tx_id: &str,
    ) -> Result<Option<FireblocksTxStatus>, VaultError> {
        Ok(None)
    }

    /// Submits a multi-receipt burn transaction to the signing backend.
    ///
    /// Encodes the multicall calldata (N redeems + optional dust transfer)
    /// and submits it. Returns a [`SubmittedTx`] for later confirmation.
    async fn submit_burn(
        &self,
        params: MultiBurnParams,
    ) -> Result<SubmittedTx, VaultError>;

    /// Confirms a previously submitted burn transaction.
    ///
    /// Polls the signing backend until completion, then parses Withdraw events.
    ///
    /// # Arguments
    ///
    /// * `fireblocks_tx_id` - Backend transaction ID from [`SubmittedTx`]
    /// * `dust_shares` - Amount of dust to report as returned (passed through
    ///   from the original request since it cannot be derived from on-chain events)
    async fn confirm_burn(
        &self,
        fireblocks_tx_id: &str,
        dust_shares: U256,
    ) -> Result<MultiBurnResult, VaultError>;

    /// Verifies an operator-supplied burn transaction hash on-chain.
    ///
    /// Fetches the receipt for `tx_hash` and confirms it proves a burn of
    /// `vault` shares by `owner` — a successful transaction emitting at least
    /// one `Transfer(owner -> 0x0)` from the vault share token. Used by the
    /// admin force-complete path to terminalize a redemption stuck in `Burning`
    /// whose burn already landed on-chain but was never recorded.
    async fn verify_burn_tx(
        &self,
        vault: Address,
        owner: Address,
        tx_hash: B256,
    ) -> Result<BurnVerification, VaultError>;
}

/// Proof that a burn transaction landed on-chain, returned by
/// [`VaultService::verify_burn_tx`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BurnVerification {
    /// Block number the burn transaction was included in.
    pub(crate) block_number: u64,
    /// Total shares burned by the owner in this transaction (sum of all
    /// matching `Transfer(owner -> 0x0)` events). Reported for audit logging;
    /// the operator is responsible for confirming the amount off-chain.
    pub(crate) shares_burned: U256,
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
) -> Result<BurnVerification, VaultError> {
    if !receipt.status() {
        return Err(VaultError::Reverted { tx_hash });
    }

    let mut shares_burned = U256::ZERO;
    let mut found_burn = false;

    for log in receipt.inner.logs() {
        if log.address() != vault {
            continue;
        }

        let Ok(decoded) =
            log.log_decode::<OffchainAssetReceiptVault::Transfer>()
        else {
            continue;
        };

        let transfer = decoded.data();
        if transfer.from == owner && transfer.to == Address::ZERO {
            found_burn = true;
            shares_burned = shares_burned
                .checked_add(transfer.value)
                .ok_or(VaultError::InvalidReceipt)?;
        }
    }

    if !found_burn {
        return Err(VaultError::NotABurn { tx_hash });
    }

    let block_number =
        receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

    Ok(BurnVerification { block_number, shares_burned })
}

/// Status of a Fireblocks transaction, as returned by `check_fireblocks_tx`.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum FireblocksTxStatus {
    /// Transaction completed on-chain.
    Completed {
        #[schema(value_type = String)]
        tx_hash: B256,
        block_number: u64,
    },
    /// Transaction is still pending (submitted, confirming, etc.).
    Pending,
    /// Transaction reached a terminal failure status.
    Failed {
        /// Top-level Fireblocks `TransactionStatus` variant (debug-formatted).
        detail: String,
        /// Fireblocks `subStatus` (e.g. `INSUFFICIENT_FUNDS`,
        /// `BLOCKED_BY_POLICY`, `REJECTED_BY_BLOCKCHAIN`). This is what tells
        /// you *why* a transaction failed; `detail` alone just says it did.
        sub_status: Option<String>,
        /// Per-network transaction hashes Fireblocks recorded. Useful when a
        /// blockchain-level failure (revert, dropped tx) needs to be inspected
        /// against an explorer.
        network_tx_hashes: Vec<String>,
    },
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
    /// Redemption's issuer request ID (for Fireblocks notes/externalTxId)
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    /// Full transaction hash that triggered this redemption, used for
    /// constructing a collision-resistant Fireblocks `externalTxId`.
    pub(crate) detected_tx_hash: B256,
    /// Optional deterministic `externalTxId` override for replacement burn
    /// retries after a previously accepted Fireblocks transaction failed.
    #[serde(default)]
    pub(crate) external_tx_id: Option<BurnExternalTxId>,
}

/// Result of a single burn within a multi-receipt burn operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
/// Returned by `submit_mint` and `submit_burn`. The `fireblocks_tx_id` is
/// persisted in an intermediate CQRS event so that `confirm_mint`/`confirm_burn`
/// can resume polling after a restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SubmittedTx {
    /// Deterministic ID used for Fireblocks idempotency (`externalTxId`).
    /// Format: `{operation}-{issuer_request_id}`.
    pub(crate) external_tx_id: String,
    /// Backend-specific transaction identifier.
    /// For Fireblocks: the Fireblocks transaction ID.
    /// For local backends: the on-chain transaction hash.
    pub(crate) fireblocks_tx_id: String,
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
    /// Expected event (e.g., Deposit) not found in transaction logs
    #[error("Event not found in transaction: {tx_hash:?}")]
    EventNotFound { tx_hash: B256 },
    /// Transaction was mined but reverted on-chain (status == 0).
    ///
    /// A reverted burn consumes no receipts, so any inventory reservation
    /// held for it must be released.
    #[error("Transaction reverted on-chain: {tx_hash:?}")]
    Reverted { tx_hash: B256 },
    /// Transaction was mined and succeeded but does not prove the expected
    /// burn — it contains no `Transfer(owner -> 0x0)` of the vault's shares.
    /// The operator-supplied hash cannot terminalize the redemption.
    #[error("Transaction is not a burn of the expected shares: {tx_hash:?}")]
    NotABurn { tx_hash: B256 },
    /// Contract call error
    #[error(transparent)]
    Contract(#[from] alloy::contract::Error),
    #[error(transparent)]
    ReceiptEncode(#[from] ReceiptEncodeError),
    /// Failed to get transaction receipt
    #[error(transparent)]
    PendingTransaction(#[from] alloy::providers::PendingTransactionError),
    /// RPC transport error (e.g., fetching receipt by tx hash during recovery)
    #[error(transparent)]
    Rpc(
        #[from]
        alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    ),
    /// Fireblocks vault service error
    #[error(transparent)]
    Fireblocks(#[from] crate::fireblocks::FireblocksVaultError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::mint::{IssuerMintRequestId, Quantity, TokenizationRequestId};

    const TEST_OA_SCHEMA: &str =
        "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm";

    fn sample_receipt_information() -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL"),
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
            UnderlyingSymbol::new("AAPL"),
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
        use alloy::consensus::{
            Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
        };
        use alloy::primitives::{Bloom, IntoLogData};

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
            verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX)
                .unwrap();

        assert_eq!(verification.block_number, 45_989_009);
        assert_eq!(verification.shares_burned, U256::from(17u64));
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
            verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX)
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

        let err = verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX)
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

        let err = verify_burn_in_receipt(&receipt, VAULT, BOT_WALLET, BURN_TX)
            .unwrap_err();

        assert!(matches!(err, VaultError::NotABurn { .. }));
    }
}
