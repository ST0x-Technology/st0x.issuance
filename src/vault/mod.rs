use alloy::primitives::{Address, B256, Bytes, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::mint::{
    IssuerRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
};

pub(crate) mod mock;
pub(crate) mod service;

/// Service abstraction for vault operations.
///
/// This trait provides an interface for minting and burning tokenized assets on-chain via the
/// Rain OffchainAssetReceiptVault contract. Implementations can be real blockchain
/// services or mocks for testing.
#[async_trait]
pub(crate) trait VaultService: Send + Sync {
    /// Atomically mints tokens and transfers shares using the vault's multicall() function.
    ///
    /// This method implements the receipt custody model by:
    /// 1. Minting both ERC1155 receipts and ERC20 shares to the bot's wallet
    /// 2. Immediately transferring the ERC20 shares to the user's wallet
    ///
    /// Both operations succeed or fail atomically in a single transaction.
    ///
    /// # Arguments
    ///
    /// * `vault` - Address of the vault contract to interact with
    /// * `assets` - Amount of assets to deposit (18-decimal fixed-point)
    /// * `bot` - Bot's address that will hold the receipts
    /// * `user` - User's address that will receive the shares
    /// * `receipt_info` - Metadata about the mint operation for on-chain audit trail
    ///
    /// # Returns
    ///
    /// On success, returns [`MintResult`] containing transaction hash, receipt ID,
    /// shares minted, gas used, and block number.
    ///
    /// # Errors
    ///
    /// Returns [`VaultError`] if the multicall fails, events are missing,
    /// or RPC communication fails.
    ///
    /// # Implementation Note
    ///
    /// This relies on the 1:1 share ratio (1 asset = 1 share with 18 decimals).
    /// The transfer amount can be pre-calculated as equal to assets, allowing
    /// the multicall to be encoded before execution.
    async fn mint_and_transfer_shares(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
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

    /// Atomically burns tokens from multiple receipts and returns dust using multicall.
    ///
    /// This method handles redemptions that require burning from multiple receipts
    /// when no single receipt has sufficient balance. It uses multicall to atomically:
    /// 1. Execute N redeem() calls, one for each receipt
    /// 2. Transfer the dust back to the user's wallet (if dust > 0)
    ///
    /// All operations succeed or fail atomically in a single transaction.
    ///
    /// # Returns
    ///
    /// On success, returns [`MultiBurnResult`] containing transaction details
    /// and per-receipt burn amounts.
    async fn burn_multiple_receipts(
        &self,
        params: MultiBurnParams,
    ) -> Result<MultiBurnResult, VaultError>;
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
}

/// A single burn within a multi-receipt burn operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MultiBurnEntry {
    /// ERC-1155 receipt ID to burn from
    pub(crate) receipt_id: U256,
    /// Amount of shares to burn from this receipt
    pub(crate) burn_shares: U256,
}

/// Parameters for a multi-receipt burn operation.
///
/// Atomically burns shares from multiple receipts in a single transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MultiBurnParams {
    /// Address of the vault contract
    pub(crate) vault: Address,
    /// List of burns to perform (receipt_id, burn_amount)
    pub(crate) burns: Vec<MultiBurnEntry>,
    /// Amount of dust to return to user (can be zero)
    pub(crate) dust_shares: U256,
    /// Address that owns the shares being burned (typically bot wallet)
    pub(crate) owner: Address,
    /// User's address that will receive the dust
    pub(crate) user: Address,
    /// Metadata about the operation for on-chain audit trail
    pub(crate) receipt_info: ReceiptInformation,
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

/// On-chain metadata stored with each vault deposit or withdrawal.
///
/// This struct is serialized to JSON and stored as bytes in the vault's receiptInformation
/// field. It provides a complete audit trail linking on-chain receipts to off-chain
/// tokenization requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReceiptInformation {
    /// Alpaca's tokenization request identifier
    pub(crate) tokenization_request_id: TokenizationRequestId,
    /// Our internal issuer request identifier
    pub(crate) issuer_request_id: IssuerRequestId,
    /// Underlying asset symbol (e.g., "AAPL")
    pub(crate) underlying: UnderlyingSymbol,
    /// Quantity of underlying assets
    pub(crate) quantity: Quantity,
    /// Type of operation (Mint or Redeem)
    pub(crate) operation_type: OperationType,
    /// Timestamp when the operation was initiated
    pub(crate) timestamp: DateTime<Utc>,
    /// Optional notes for additional context
    pub(crate) notes: Option<String>,
}

/// Type of tokenization operation being performed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OperationType {
    /// Minting new tokenized shares (deposit to vault)
    Mint,
    /// Redeeming tokenized shares for underlying assets (withdrawal from vault)
    Redeem,
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
    /// Contract call error
    #[error(transparent)]
    Contract(#[from] alloy::contract::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    /// Failed to get transaction receipt
    #[error(transparent)]
    PendingTransaction(#[from] alloy::providers::PendingTransactionError),
    /// Fireblocks vault service error
    #[error(transparent)]
    Fireblocks(#[from] crate::fireblocks::FireblocksVaultError),
}
