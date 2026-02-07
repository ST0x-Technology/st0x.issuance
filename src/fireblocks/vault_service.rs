use std::time::Duration;

use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use fireblocks_sdk::apis::transactions_api::{
    CreateTransactionError, CreateTransactionParams,
};
use fireblocks_sdk::models::{self, TransactionStatus};
use fireblocks_sdk::{Client, ClientBuilder};
use tracing::debug;

use super::config::{ChainAssetIds, Environment, FireblocksConfig};
use crate::bindings::OffchainAssetReceiptVault;
use crate::vault::{
    BurnParams, BurnWithDustResult, MintResult, ReceiptInformation, VaultError,
    VaultService,
};

/// Fireblocks-specific errors that can occur during vault operations.
#[derive(Debug, thiserror::Error)]
pub enum FireblocksVaultError {
    #[error("failed to read Fireblocks secret key from {path}")]
    ReadSecret {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Fireblocks SDK error")]
    Sdk(#[from] fireblocks_sdk::FireblocksError),
    #[error("Fireblocks API error")]
    Api(#[from] fireblocks_sdk::apis::Error<CreateTransactionError>),
    #[error("RPC error")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("no deposit address found for vault {vault_id}, asset {asset_id}")]
    NoAddress { vault_id: String, asset_id: String },
    #[error("invalid deposit address from Fireblocks: {address}")]
    InvalidAddress {
        address: String,
        #[source]
        source: alloy::hex::FromHexError,
    },
    #[error("Fireblocks response did not return a transaction ID")]
    MissingTransactionId,
    #[error(
        "Fireblocks transaction {tx_id} reached terminal status: {status:?}"
    )]
    TransactionFailed { tx_id: String, status: TransactionStatus },
    #[error(
        "Fireblocks transaction {tx_id} did not include a transaction hash"
    )]
    MissingTxHash { tx_id: String },
    #[error("invalid transaction hash from Fireblocks: {hash}")]
    InvalidTxHash {
        hash: String,
        #[source]
        source: alloy::hex::FromHexError,
    },
    #[error("no asset ID configured for chain {chain_id}")]
    UnknownChain { chain_id: u64 },
    #[error("transaction {tx_hash} has no receipt after confirmation")]
    MissingReceipt { tx_hash: TxHash },
}

/// Fetches the vault account address from Fireblocks.
///
/// This is used to derive the bot wallet address from the Fireblocks configuration.
/// It builds a temporary client, fetches the deposit address for the default asset,
/// and returns the address.
pub async fn fetch_vault_address(
    config: &FireblocksConfig,
) -> Result<Address, FireblocksVaultError> {
    let secret = std::fs::read(&config.secret_path).map_err(|e| {
        FireblocksVaultError::ReadSecret {
            path: config.secret_path.clone(),
            source: e,
        }
    })?;

    let mut builder = ClientBuilder::new(config.api_user_id.as_str(), &secret);
    if config.environment == Environment::Sandbox {
        builder = builder.use_sandbox();
    }
    let client = builder.build()?;

    let default_asset_id = config.chain_asset_ids.default_asset_id();

    let addresses = client
        .addresses(config.vault_account_id.as_str(), default_asset_id.as_str())
        .await?;

    let address_str = addresses
        .first()
        .and_then(|a| a.address.as_deref())
        .ok_or_else(|| FireblocksVaultError::NoAddress {
            vault_id: config.vault_account_id.as_str().to_string(),
            asset_id: default_asset_id.as_str().to_string(),
        })?;

    address_str.parse::<Address>().map_err(|e| {
        FireblocksVaultError::InvalidAddress {
            address: address_str.to_string(),
            source: e,
        }
    })
}

/// Vault service implementation that uses Fireblocks CONTRACT_CALL operation.
///
/// Unlike the RAW signing approach (which only signs hashes), CONTRACT_CALL
/// enables Fireblocks TAP policies to enforce contract/method whitelisting.
/// Fireblocks handles transaction building, signing, and broadcasting.
///
/// The service uses a read-only RPC provider for:
/// - Calling view functions (previewDeposit, balanceOf)
/// - Fetching transaction receipts to parse events
pub struct FireblocksVaultService<P> {
    client: Client,
    vault_account_id: String,
    chain_asset_ids: ChainAssetIds,
    read_provider: P,
    chain_id: u64,
}

impl<P: Provider + Clone> FireblocksVaultService<P> {
    /// Creates a new Fireblocks vault service.
    ///
    /// # Arguments
    ///
    /// * `config` - Fireblocks configuration
    /// * `read_provider` - Read-only RPC provider for view calls and receipt fetching
    /// * `chain_id` - The chain ID for transaction routing
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Fireblocks secret key cannot be read
    /// - The Fireblocks client cannot be built
    pub fn new(
        config: &FireblocksConfig,
        read_provider: P,
        chain_id: u64,
    ) -> Result<Self, FireblocksVaultError> {
        let secret = std::fs::read(&config.secret_path).map_err(|e| {
            FireblocksVaultError::ReadSecret {
                path: config.secret_path.clone(),
                source: e,
            }
        })?;

        let mut builder =
            ClientBuilder::new(config.api_user_id.as_str(), &secret);
        if config.environment == Environment::Sandbox {
            builder = builder.use_sandbox();
        }
        let client = builder.build()?;

        debug!(
            vault_account_id = %config.vault_account_id.as_str(),
            chain_asset_ids = ?config.chain_asset_ids,
            %chain_id,
            "Fireblocks vault service initialized"
        );

        Ok(Self {
            client,
            vault_account_id: config.vault_account_id.as_str().to_string(),
            chain_asset_ids: config.chain_asset_ids.clone(),
            read_provider,
            chain_id,
        })
    }

    /// Submits a CONTRACT_CALL transaction to Fireblocks.
    ///
    /// # Arguments
    ///
    /// * `contract_address` - The target contract address
    /// * `calldata` - The encoded function calldata
    /// * `note` - A descriptive note for the transaction
    ///
    /// # Returns
    ///
    /// The Fireblocks transaction ID.
    async fn submit_contract_call(
        &self,
        contract_address: Address,
        calldata: &Bytes,
        note: &str,
    ) -> Result<String, FireblocksVaultError> {
        let asset_id = self.chain_asset_ids.get(self.chain_id).ok_or(
            FireblocksVaultError::UnknownChain { chain_id: self.chain_id },
        )?;

        let tx_request = build_contract_call_request(
            asset_id.as_str(),
            &self.vault_account_id,
            contract_address,
            calldata,
            note,
        );

        let params = CreateTransactionParams::builder()
            .transaction_request(tx_request)
            .build();

        let create_response =
            self.client.transactions_api().create_transaction(params).await?;

        create_response.id.ok_or(FireblocksVaultError::MissingTransactionId)
    }

    /// Polls a Fireblocks transaction until completion.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The Fireblocks transaction ID to poll
    ///
    /// # Returns
    ///
    /// The on-chain transaction hash (B256).
    async fn wait_for_completion(
        &self,
        tx_id: &str,
    ) -> Result<B256, FireblocksVaultError> {
        debug!(fireblocks_tx_id = %tx_id, "Polling Fireblocks CONTRACT_CALL transaction...");

        let result = self
            .client
            .poll_transaction(
                tx_id,
                Duration::from_secs(300),
                Duration::from_millis(500),
                |tx| {
                    debug!(
                        fireblocks_tx_id = %tx_id,
                        status = ?tx.status,
                        "Polling Fireblocks transaction"
                    );
                },
            )
            .await?;

        if result.status != TransactionStatus::Completed {
            return Err(FireblocksVaultError::TransactionFailed {
                tx_id: tx_id.to_string(),
                status: result.status,
            });
        }

        let tx_hash_str = result.tx_hash.ok_or_else(|| {
            FireblocksVaultError::MissingTxHash { tx_id: tx_id.to_string() }
        })?;

        parse_tx_hash(&tx_hash_str)
    }

    /// Fetches a transaction receipt from the RPC provider.
    ///
    /// # Arguments
    ///
    /// * `tx_hash` - The transaction hash to fetch
    ///
    /// # Returns
    ///
    /// The transaction receipt.
    async fn fetch_receipt(
        &self,
        tx_hash: B256,
    ) -> Result<TransactionReceipt, FireblocksVaultError> {
        self.read_provider
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or(FireblocksVaultError::MissingReceipt { tx_hash })
    }
}

/// Builds a Fireblocks CONTRACT_CALL transaction request.
fn build_contract_call_request(
    asset_id: &str,
    vault_account_id: &str,
    contract_address: Address,
    calldata: &Bytes,
    note: &str,
) -> models::TransactionRequest {
    let extra_parameters = models::ExtraParameters {
        contract_call_data: Some(alloy::hex::encode(calldata)),
        raw_message_data: None,
        inputs_selection: None,
        node_controls: None,
        program_call_data: None,
    };

    models::TransactionRequest {
        operation: Some(models::TransactionOperation::ContractCall),
        asset_id: Some(asset_id.to_string()),
        source: Some(models::SourceTransferPeerPath {
            r#type: models::TransferPeerPathType::VaultAccount,
            id: Some(vault_account_id.to_string()),
            sub_type: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        destination: Some(models::DestinationTransferPeerPath {
            r#type: models::TransferPeerPathType::OneTimeAddress,
            one_time_address: Some(models::OneTimeAddress::new(format!(
                "{contract_address:?}"
            ))),
            sub_type: None,
            id: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        // Amount is "0" for contract calls that don't transfer value
        amount: Some(models::TransactionRequestAmount::String("0".to_string())),
        extra_parameters: Some(extra_parameters),
        external_tx_id: Some(uuid::Uuid::new_v4().to_string()),
        note: Some(note.to_string()),
        // Use default fee level (MEDIUM)
        fee_level: Some(models::transaction_request::FeeLevel::Medium),
        // Remaining fields default to None
        destinations: None,
        treat_as_gross_amount: None,
        force_sweep: None,
        fee: None,
        priority_fee: None,
        fail_on_low_fee: None,
        max_fee: None,
        gas_limit: None,
        gas_price: None,
        network_fee: None,
        replace_tx_by_hash: None,
        customer_ref_id: None,
        travel_rule_message: None,
        auto_staking: None,
        network_staking: None,
        cpu_staking: None,
        use_gasless: None,
    }
}

/// Parses a transaction hash string (with or without 0x prefix) into B256.
fn parse_tx_hash(tx_hash_str: &str) -> Result<B256, FireblocksVaultError> {
    let tx_hash_hex = tx_hash_str.strip_prefix("0x").unwrap_or(tx_hash_str);
    let tx_hash_bytes: [u8; 32] = alloy::hex::decode(tx_hash_hex)
        .map_err(|e| FireblocksVaultError::InvalidTxHash {
            hash: tx_hash_str.to_string(),
            source: e,
        })?
        .try_into()
        .map_err(|_| FireblocksVaultError::InvalidTxHash {
            hash: tx_hash_str.to_string(),
            source: alloy::hex::FromHexError::InvalidStringLength,
        })?;

    Ok(B256::from(tx_hash_bytes))
}

#[async_trait]
impl<P: Provider + Clone + Send + Sync + 'static> VaultService
    for FireblocksVaultService<P>
{
    async fn mint_and_transfer_shares(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        user: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<MintResult, VaultError> {
        let receipt_info_bytes =
            Bytes::from(serde_json::to_vec(&receipt_info)?);

        let vault_contract =
            OffchainAssetReceiptVault::new(vault, &self.read_provider);

        let share_ratio = U256::from(10).pow(U256::from(18));

        // Preview deposit to get the exact number of shares that will be minted
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

        // Encode multicall with both operations
        let multicall_calldata = vault_contract
            .multicall(vec![deposit_call, transfer_call])
            .calldata()
            .clone();

        // Submit CONTRACT_CALL to Fireblocks
        let note = format!(
            "Mint {} shares for {} (issuer_request_id: {})",
            assets,
            user,
            receipt_info.issuer_request_id.as_str()
        );

        let tx_id = self
            .submit_contract_call(vault, &multicall_calldata, &note)
            .await?;

        // Wait for Fireblocks to complete the transaction
        let tx_hash = self.wait_for_completion(&tx_id).await?;

        // Fetch the receipt from our RPC provider to parse events
        let receipt = self.fetch_receipt(tx_hash).await?;

        // Parse the Deposit event to get receipt_id and shares_minted
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
                tx_hash: format!("{tx_hash:?}"),
            })?;

        let gas_used = receipt.gas_used;
        let block_number =
            receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

        Ok(MintResult {
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
        })
    }

    async fn burn_and_return_dust(
        &self,
        params: BurnParams,
    ) -> Result<BurnWithDustResult, VaultError> {
        let receipt_info_bytes =
            Bytes::from(serde_json::to_vec(&params.receipt_info)?);

        let vault_contract =
            OffchainAssetReceiptVault::new(params.vault, &self.read_provider);

        // Build the redeem call for burning shares
        let redeem_call = vault_contract
            .redeem(
                params.burn_shares,
                params.owner,
                params.owner,
                params.receipt_id,
                receipt_info_bytes,
            )
            .calldata()
            .clone();

        // Build multicall: redeem + optional transfer for dust
        let calls = if params.dust_shares > U256::ZERO {
            let transfer_call = vault_contract
                .transfer(params.user, params.dust_shares)
                .calldata()
                .clone();
            vec![redeem_call, transfer_call]
        } else {
            vec![redeem_call]
        };

        let multicall_calldata =
            vault_contract.multicall(calls).calldata().clone();

        // Submit CONTRACT_CALL to Fireblocks
        let note = format!(
            "Burn {} shares from receipt {} (issuer_request_id: {})",
            params.burn_shares,
            params.receipt_id,
            params.receipt_info.issuer_request_id.as_str()
        );

        let tx_id = self
            .submit_contract_call(params.vault, &multicall_calldata, &note)
            .await?;

        // Wait for Fireblocks to complete the transaction
        let tx_hash = self.wait_for_completion(&tx_id).await?;

        // Fetch the receipt from our RPC provider to parse events
        let receipt = self.fetch_receipt(tx_hash).await?;

        // Parse the Withdraw event
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
                tx_hash: format!("{tx_hash:?}"),
            })?;

        let gas_used = receipt.gas_used;
        let block_number =
            receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

        Ok(BurnWithDustResult {
            tx_hash,
            receipt_id: parsed_receipt_id,
            shares_burned,
            dust_returned: params.dust_shares,
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
            OffchainAssetReceiptVault::new(vault, &self.read_provider);

        Ok(vault_contract.balanceOf(owner).call().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Unit Tests for build_contract_call_request ====================

    #[test]
    fn build_contract_call_request_has_correct_structure() {
        let asset_id = "BASECHAIN_ETH";
        let vault_account_id = "0";
        let contract_address = Address::ZERO;
        let calldata = Bytes::from(vec![0x12, 0x34, 0x56, 0x78]);
        let note = "Test transaction";

        let request = build_contract_call_request(
            asset_id,
            vault_account_id,
            contract_address,
            &calldata,
            note,
        );

        assert_eq!(
            request.operation,
            Some(models::TransactionOperation::ContractCall)
        );
        assert_eq!(request.asset_id, Some(asset_id.to_string()));
        assert_eq!(
            request.amount,
            Some(models::TransactionRequestAmount::String("0".to_string()))
        );
        assert!(request.extra_parameters.is_some());

        let extra = request.extra_parameters.as_ref().unwrap();
        assert_eq!(extra.contract_call_data, Some("12345678".to_string()));
        assert!(extra.raw_message_data.is_none());

        let source = request.source.as_ref().unwrap();
        assert_eq!(source.r#type, models::TransferPeerPathType::VaultAccount);
        assert_eq!(source.id, Some(vault_account_id.to_string()));

        let dest = request.destination.as_ref().unwrap();
        assert_eq!(dest.r#type, models::TransferPeerPathType::OneTimeAddress);
        assert!(dest.one_time_address.is_some());
    }

    #[test]
    fn build_contract_call_request_encodes_calldata_as_hex() {
        let calldata = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);

        let request = build_contract_call_request(
            "ETH",
            "0",
            Address::ZERO,
            &calldata,
            "test",
        );

        let extra = request.extra_parameters.unwrap();
        assert_eq!(extra.contract_call_data, Some("deadbeef".to_string()));
    }

    #[test]
    fn build_contract_call_request_formats_contract_address() {
        let contract_address =
            "0x1234567890123456789012345678901234567890".parse().unwrap();

        let request = build_contract_call_request(
            "ETH",
            "0",
            contract_address,
            &Bytes::new(),
            "test",
        );

        let dest = request.destination.unwrap();
        let one_time = dest.one_time_address.unwrap();
        assert!(
            one_time
                .address
                .contains("0x1234567890123456789012345678901234567890")
        );
    }

    #[test]
    fn build_contract_call_request_uses_medium_fee_level() {
        let request = build_contract_call_request(
            "ETH",
            "0",
            Address::ZERO,
            &Bytes::new(),
            "test",
        );

        assert_eq!(
            request.fee_level,
            Some(models::transaction_request::FeeLevel::Medium)
        );
    }

    // ==================== Unit Tests for parse_tx_hash ====================

    #[test]
    fn parse_tx_hash_with_0x_prefix() {
        let hash_str = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = parse_tx_hash(hash_str).unwrap();

        assert_eq!(
            result,
            B256::from([
                0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd,
                0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12,
                0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56,
                0x78, 0x90
            ])
        );
    }

    #[test]
    fn parse_tx_hash_without_0x_prefix() {
        let hash_str =
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = parse_tx_hash(hash_str).unwrap();

        assert_eq!(
            result,
            B256::from([
                0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd,
                0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12,
                0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56,
                0x78, 0x90
            ])
        );
    }

    #[test]
    fn parse_tx_hash_invalid_hex_characters() {
        let hash_str = "0xGGGGGG1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let result = parse_tx_hash(hash_str);

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }

    #[test]
    fn parse_tx_hash_too_short() {
        let hash_str = "0xabcdef";
        let result = parse_tx_hash(hash_str);

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }

    #[test]
    fn parse_tx_hash_too_long() {
        // 33 bytes instead of 32
        let hash_str = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890aa";
        let result = parse_tx_hash(hash_str);

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }

    #[test]
    fn parse_tx_hash_empty_string() {
        let result = parse_tx_hash("");

        assert!(
            matches!(result, Err(FireblocksVaultError::InvalidTxHash { .. })),
            "Expected InvalidTxHash error, got {result:?}"
        );
    }
}
