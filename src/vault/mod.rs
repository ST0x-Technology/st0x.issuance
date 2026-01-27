use alloy::primitives::{Address, B256, U256};
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

    /// Burns tokens on-chain by calling the vault's withdraw() function.
    ///
    /// # Arguments
    ///
    /// * `vault` - Address of the vault contract to interact with
    /// * `shares` - Number of shares to burn (18-decimal fixed-point)
    /// * `receipt_id` - ERC-1155 receipt ID to burn from
    /// * `owner` - Address of the account whose shares are being burned
    /// * `receiver` - Address that will receive the underlying assets
    /// * `receipt_info` - Metadata about the burn operation for on-chain audit trail
    ///
    /// # Returns
    ///
    /// On success, returns [`BurnResult`] containing transaction hash, receipt ID,
    /// shares burned, gas used, and block number.
    ///
    /// # Errors
    ///
    /// Returns [`VaultError`] if the transaction fails, events are missing,
    /// or RPC communication fails.
    async fn burn_tokens(
        &self,
        vault: Address,
        shares: U256,
        receipt_id: U256,
        owner: Address,
        receiver: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<BurnResult, VaultError>;

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

/// Result of a successful on-chain burning operation.
///
/// Contains all transaction details needed to track the burn in the Redemption aggregate
/// and for audit trails.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BurnResult {
    /// Transaction hash of the withdraw transaction
    pub(crate) tx_hash: B256,
    /// ERC-1155 receipt ID that was burned from
    pub(crate) receipt_id: U256,
    /// Number of ERC-20 shares burned (with 18 decimals)
    pub(crate) shares_burned: U256,
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
    /// Transaction was sent but failed on-chain
    #[error("Transaction failed: {reason}")]
    TransactionFailed { reason: String },
    /// Transaction receipt is missing required data
    #[error("Invalid receipt")]
    InvalidReceipt,
    /// RPC communication error
    #[error("RPC error: {message}")]
    RpcError { message: String },
    /// Expected event (e.g., Deposit) not found in transaction logs
    #[error("Event not found in transaction: {tx_hash}")]
    EventNotFound { tx_hash: String },
    /// Contract call error
    #[error(transparent)]
    Contract(#[from] alloy::contract::Error),
}
