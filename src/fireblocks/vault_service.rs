use std::time::Duration;

use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use fireblocks_sdk::apis::transactions_api::{
    CreateTransactionError, CreateTransactionParams,
};
use fireblocks_sdk::apis::whitelisted_contracts_api::GetContractsError;
use fireblocks_sdk::models::{self, TransactionStatus};
use fireblocks_sdk::{Client, ClientBuilder};
use tracing::{debug, warn};

use super::config::{
    AssetId, ChainAssetIds, ContractWalletId, Environment, FireblocksConfig,
    FireblocksVaultAccountId,
};
use crate::bindings::OffchainAssetReceiptVault;
use crate::vault::{
    MintResult, MultiBurnParams, MultiBurnResult, MultiBurnResultEntry,
    ReceiptInformation, VaultError, VaultService,
};

/// Fireblocks-specific errors that can occur during vault operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FireblocksVaultError {
    #[error("Fireblocks SDK error: {0}")]
    Fireblocks(#[from] fireblocks_sdk::FireblocksError),
    #[error("Fireblocks API error: {0}")]
    Api(#[from] fireblocks_sdk::apis::Error<CreateTransactionError>),
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("no deposit address found for vault {}, asset {}", vault_id.as_str(), asset_id.as_str())]
    NoAddress { vault_id: FireblocksVaultAccountId, asset_id: AssetId },
    #[error("invalid address from Fireblocks: {0}")]
    FromHex(#[from] alloy::hex::FromHexError),
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
    #[error("Fireblocks contracts API error: {0}")]
    ContractsApi(#[from] fireblocks_sdk::apis::Error<GetContractsError>),
    #[error("contract {contract} is not whitelisted in Fireblocks")]
    ContractNotWhitelisted { contract: Address },
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
pub(crate) async fn fetch_vault_address(
    config: &FireblocksConfig,
) -> Result<Address, FireblocksVaultError> {
    let mut builder =
        ClientBuilder::new(config.api_user_id.as_str(), &config.secret);
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
            vault_id: config.vault_account_id.clone(),
            asset_id: default_asset_id.clone(),
        })?;

    Ok(address_str.parse::<Address>()?)
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
pub(crate) struct FireblocksVaultService<P> {
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
    /// * `config` - Fireblocks configuration (with secret already loaded)
    /// * `read_provider` - Read-only RPC provider for view calls and receipt fetching
    /// * `chain_id` - The chain ID for transaction routing
    ///
    /// # Errors
    ///
    /// Returns an error if the Fireblocks client cannot be built.
    pub fn new(
        config: &FireblocksConfig,
        read_provider: P,
        chain_id: u64,
    ) -> Result<Self, FireblocksVaultError> {
        let mut builder =
            ClientBuilder::new(config.api_user_id.as_str(), &config.secret);
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

    /// Resolves a contract address to its Fireblocks whitelisted wallet ID.
    ///
    /// Queries Fireblocks' `GET /contracts` API and finds the wallet whose
    /// asset entry matches both the configured chain asset ID and the given
    /// contract address. This ensures transactions go through TAP policy
    /// controls instead of bypassing them with `OneTimeAddress`.
    async fn resolve_contract_wallet(
        &self,
        contract: Address,
    ) -> Result<ContractWalletId, FireblocksVaultError> {
        let contract_address = contract.to_string().to_lowercase();
        let asset_id = self.chain_asset_ids.get(self.chain_id).ok_or(
            FireblocksVaultError::UnknownChain { chain_id: self.chain_id },
        )?;
        let expected_asset_id = asset_id.as_str();

        self.client
            .wallet_contract_api()
            .get_contracts()
            .await?
            .into_iter()
            .find_map(|wallet| {
                wallet
                    .assets
                    .iter()
                    .any(|asset| {
                        asset
                            .id
                            .as_ref()
                            .is_some_and(|id| id == expected_asset_id)
                            && asset.address.as_ref().is_some_and(|addr| {
                                addr.to_lowercase() == contract_address
                            })
                    })
                    .then_some(ContractWalletId::from(wallet.id))
            })
            .ok_or(FireblocksVaultError::ContractNotWhitelisted { contract })
    }

    /// Submits a CONTRACT_CALL transaction to Fireblocks.
    ///
    /// # Arguments
    ///
    /// * `contract_address` - The target contract address
    /// * `calldata` - The encoded function calldata
    /// * `note` - A descriptive note for the transaction
    /// * `external_tx_id` - Deterministic ID for idempotency across retries
    ///
    /// # Returns
    ///
    /// The Fireblocks transaction ID.
    async fn submit_contract_call(
        &self,
        contract_address: Address,
        calldata: &Bytes,
        note: &str,
        external_tx_id: &str,
    ) -> Result<String, FireblocksVaultError> {
        let asset_id = self.chain_asset_ids.get(self.chain_id).ok_or(
            FireblocksVaultError::UnknownChain { chain_id: self.chain_id },
        )?;

        let wallet_id = self.resolve_contract_wallet(contract_address).await?;

        let tx_request = build_contract_call_request(
            asset_id.as_str(),
            &self.vault_account_id,
            &wallet_id,
            calldata,
            note,
            external_tx_id,
        );

        let params = CreateTransactionParams::builder()
            .transaction_request(tx_request)
            .build();

        let create_response = self
            .client
            .transactions_api()
            .create_transaction(params)
            .await
            .map_err(|err| {
                // The SDK's Display impl only shows the status code, discarding
                // the response body which contains the actual error message and
                // code. Debug preserves the full ResponseContent including the
                // body and typed error entity.
                warn!(
                    error = ?err,
                    %contract_address,
                    %external_tx_id,
                    "Fireblocks create_transaction failed"
                );
                err
            })?;

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
                Duration::from_secs(600),
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
            if is_still_pending(result.status) {
                warn!(
                    fireblocks_tx_id = %tx_id,
                    status = ?result.status,
                    "Polling timed out but transaction may still confirm on-chain"
                );
            }

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
///
/// Uses `ExternalWallet` destination type with the resolved whitelisted
/// contract wallet ID, enabling Fireblocks TAP policy enforcement.
fn build_contract_call_request(
    asset_id: &str,
    vault_account_id: &str,
    wallet_id: &ContractWalletId,
    calldata: &Bytes,
    note: &str,
    external_tx_id: &str,
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
            r#type: models::TransferPeerPathType::ExternalWallet,
            id: Some(wallet_id.as_str().to_string()),
            one_time_address: None,
            sub_type: None,
            name: None,
            wallet_id: None,
            is_collateral: None,
        }),
        // Amount is "0" for contract calls that don't transfer value
        amount: Some(models::TransactionRequestAmount::String("0".to_string())),
        extra_parameters: Some(extra_parameters),
        external_tx_id: Some(external_tx_id.to_string()),
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

/// The type of vault operation, used to tag `externalTxId` for Fireblocks.
enum VaultOperation {
    Mint,
    Burn,
}

impl std::fmt::Display for VaultOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mint => write!(f, "mint"),
            Self::Burn => write!(f, "burn"),
        }
    }
}

impl VaultOperation {
    /// Generates a unique `externalTxId` for Fireblocks transactions.
    ///
    /// Format: `{ISO8601_compact}-{operation}-{issuer_request_id}`
    /// e.g., `20260210T083800Z-mint-5960be2e-a556-42e7-8def-ed3a354b66e6`
    ///
    /// Each call produces a unique ID (via timestamp), avoiding Fireblocks'
    /// permanent `externalTxId` rejection on retries. The operation prefix
    /// and issuer_request_id make the ID searchable in the Fireblocks dashboard.
    fn external_tx_id(
        &self,
        issuer_request_id: &(impl std::fmt::Display + ?Sized),
    ) -> String {
        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        format!("{timestamp}-{self}-{issuer_request_id}")
    }
}

/// Returns true if the transaction status indicates the transaction is still
/// in progress and may eventually confirm on-chain. Used to distinguish
/// "polling timed out but tx might still land" from "tx definitively failed."
const fn is_still_pending(status: TransactionStatus) -> bool {
    use TransactionStatus::*;

    match status {
        Submitted
        | PendingAmlScreening
        | PendingEnrichment
        | PendingAuthorization
        | Queued
        | PendingSignature
        | Pending3RdPartyManualApproval
        | Pending3RdParty
        | Broadcasting
        | Confirming
        | Cancelling => true,

        Completed | Cancelled | Blocked | Rejected | Failed => false,
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
        let receipt_info_bytes = receipt_info.encode()?;

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
            "Mint {assets} shares for {user} (issuer_request_id: {})",
            receipt_info.issuer_request_id,
        );

        let external_tx_id = VaultOperation::Mint
            .external_tx_id(&receipt_info.issuer_request_id);

        let tx_id = self
            .submit_contract_call(
                vault,
                &multicall_calldata,
                &note,
                &external_tx_id,
            )
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
            .ok_or_else(|| VaultError::EventNotFound { tx_hash })?;

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

    async fn burn_multiple_receipts(
        &self,
        params: MultiBurnParams,
    ) -> Result<MultiBurnResult, VaultError> {
        let vault_contract =
            OffchainAssetReceiptVault::new(params.vault, &self.read_provider);

        let redeem_calls: Vec<Bytes> = params
            .burns
            .iter()
            .map(|burn| {
                let receipt_info_bytes = burn
                    .receipt_info
                    .as_ref()
                    .map(ReceiptInformation::encode)
                    .transpose()?
                    .unwrap_or_default();

                Ok(vault_contract
                    .redeem(
                        burn.burn_shares,
                        params.user,
                        params.owner,
                        burn.receipt_id,
                        receipt_info_bytes,
                    )
                    .calldata()
                    .clone())
            })
            .collect::<Result<Vec<_>, VaultError>>()?;

        // Build multicall: all redeems, plus optional dust transfer
        let calls: Vec<Bytes> = if params.dust_shares > U256::ZERO {
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

        let multicall_calldata =
            vault_contract.multicall(calls).calldata().clone();

        // Submit CONTRACT_CALL to Fireblocks
        let total_burn: U256 = params.burns.iter().map(|b| b.burn_shares).sum();

        let note = format!(
            "Burn {total_burn} shares from {} receipts (issuer_request_id: {})",
            params.burns.len(),
            params.issuer_request_id,
        );

        let external_tx_id =
            VaultOperation::Burn.external_tx_id(&params.issuer_request_id);

        let tx_id = self
            .submit_contract_call(
                params.vault,
                &multicall_calldata,
                &note,
                &external_tx_id,
            )
            .await?;

        // Wait for Fireblocks to complete the transaction
        let tx_hash = self.wait_for_completion(&tx_id).await?;

        // Fetch the receipt from our RPC provider to parse events
        let receipt = self.fetch_receipt(tx_hash).await?;

        // Parse all Withdraw events from the receipt
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

        let gas_used = receipt.gas_used;
        let block_number =
            receipt.block_number.ok_or(VaultError::InvalidReceipt)?;

        Ok(MultiBurnResult {
            tx_hash,
            burns,
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
    use std::sync::LazyLock;

    use httpmock::MockServer;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use uuid::Uuid;

    use super::*;
    use crate::fireblocks::config::parse_chain_asset_ids;

    static TEST_RSA_PEM: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .as_bytes()
            .to_vec()
    });

    fn mock_client(server: &MockServer) -> Client {
        ClientBuilder::new("test-api-user", &TEST_RSA_PEM)
            .with_url(&server.base_url())
            .build()
            .unwrap()
    }

    fn build_test_service(
        client: Client,
    ) -> FireblocksVaultService<impl Provider + Clone + Send + Sync + 'static>
    {
        let chain_asset_ids =
            parse_chain_asset_ids("8453:BASECHAIN_ETH").unwrap();

        let read_provider = alloy::providers::RootProvider::new_http(
            "http://localhost:1".parse().unwrap(),
        );

        FireblocksVaultService {
            client,
            vault_account_id: "0".to_string(),
            chain_asset_ids,
            read_provider,
            chain_id: 8453,
        }
    }

    fn mock_whitelisted_contracts<'a>(
        server: &'a MockServer,
        contract_address: &str,
        asset_id: &str,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method("GET").path("/contracts");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([
                    {
                        "id": "contract-wallet-123",
                        "name": "Test Vault",
                        "assets": [
                            {
                                "id": asset_id,
                                "address": contract_address
                            }
                        ]
                    }
                ]));
        })
    }

    #[tokio::test]
    async fn resolve_contract_wallet_finds_whitelisted_contract() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        let mock = mock_whitelisted_contracts(
            &server,
            &contract.to_string().to_lowercase(),
            "BASECHAIN_ETH",
        );

        let service = build_test_service(mock_client(&server));

        let wallet_id =
            service.resolve_contract_wallet(contract).await.unwrap();

        assert_eq!(wallet_id.as_str(), "contract-wallet-123");
        mock.assert();
    }

    #[tokio::test]
    async fn resolve_contract_wallet_rejects_unknown_contract() {
        let contract = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        // Mock returns a wallet with a different address
        let mock = mock_whitelisted_contracts(
            &server,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "BASECHAIN_ETH",
        );

        let service = build_test_service(mock_client(&server));

        let result = service.resolve_contract_wallet(contract).await;

        assert!(
            matches!(
                result,
                Err(FireblocksVaultError::ContractNotWhitelisted { .. })
            ),
            "Expected ContractNotWhitelisted, got {result:?}"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn resolve_contract_wallet_rejects_wrong_asset_id() {
        let contract = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();

        let server = MockServer::start();
        // Mock returns matching address but wrong asset ID
        let mock = mock_whitelisted_contracts(
            &server,
            &contract.to_string().to_lowercase(),
            "ETH_TEST",
        );

        let service = build_test_service(mock_client(&server));

        let result = service.resolve_contract_wallet(contract).await;

        assert!(
            matches!(
                result,
                Err(FireblocksVaultError::ContractNotWhitelisted { .. })
            ),
            "Expected ContractNotWhitelisted, got {result:?}"
        );
        mock.assert();
    }

    #[test]
    fn build_contract_call_request_has_correct_structure() {
        let asset_id = "BASECHAIN_ETH";
        let vault_account_id = "0";
        let wallet_id = ContractWalletId::from("wallet-abc".to_string());
        let calldata = Bytes::from(vec![0x12, 0x34, 0x56, 0x78]);
        let note = "Test transaction";

        let request = build_contract_call_request(
            asset_id,
            vault_account_id,
            &wallet_id,
            &calldata,
            note,
            "test-id",
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
        assert_eq!(dest.r#type, models::TransferPeerPathType::ExternalWallet);
        assert_eq!(dest.id, Some("wallet-abc".to_string()));
        assert!(dest.one_time_address.is_none());
    }

    #[test]
    fn build_contract_call_request_encodes_calldata_as_hex() {
        let calldata = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);
        let wallet_id = ContractWalletId::from("w".to_string());

        let request = build_contract_call_request(
            "ETH", "0", &wallet_id, &calldata, "test", "test-id",
        );

        let extra = request.extra_parameters.unwrap();
        assert_eq!(extra.contract_call_data, Some("deadbeef".to_string()));
    }

    #[test]
    fn build_contract_call_request_uses_medium_fee_level() {
        let wallet_id = ContractWalletId::from("w".to_string());

        let request = build_contract_call_request(
            "ETH",
            "0",
            &wallet_id,
            &Bytes::new(),
            "test",
            "test-id",
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

    #[test]
    fn external_tx_id_contains_operation_and_issuer_request_id() {
        let id = VaultOperation::Mint
            .external_tx_id("5960be2e-a556-42e7-8def-ed3a354b66e6");
        assert!(
            id.contains("mint-5960be2e-a556-42e7-8def-ed3a354b66e6"),
            "Expected operation-issuer_request_id suffix, got {id}"
        );
    }

    #[test]
    fn external_tx_id_starts_with_iso8601_timestamp() {
        let id =
            VaultOperation::Burn.external_tx_id(&Uuid::new_v4().to_string());
        assert!(
            id.contains('T') && id.contains('Z'),
            "Expected ISO 8601 compact timestamp, got {id}"
        );
        let timestamp_part = id.split('-').next().unwrap();
        assert!(
            timestamp_part.ends_with('Z'),
            "Timestamp should end with Z, got {timestamp_part}"
        );
    }

    #[test]
    fn external_tx_id_is_unique_across_calls() {
        let issuer_request_id = Uuid::new_v4().to_string();
        let id1 = VaultOperation::Mint.external_tx_id(&issuer_request_id);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let id2 = VaultOperation::Mint.external_tx_id(&issuer_request_id);
        assert_ne!(id1, id2, "Consecutive calls should produce unique IDs");
    }

    #[test]
    fn is_still_pending_true_for_in_progress_statuses() {
        use TransactionStatus::*;

        assert!(is_still_pending(Submitted));
        assert!(is_still_pending(PendingAmlScreening));
        assert!(is_still_pending(PendingEnrichment));
        assert!(is_still_pending(PendingAuthorization));
        assert!(is_still_pending(Queued));
        assert!(is_still_pending(PendingSignature));
        assert!(is_still_pending(Pending3RdPartyManualApproval));
        assert!(is_still_pending(Pending3RdParty));
        assert!(is_still_pending(Broadcasting));
        assert!(is_still_pending(Confirming));
        assert!(is_still_pending(Cancelling));
    }

    #[test]
    fn is_still_pending_false_for_terminal_statuses() {
        use TransactionStatus::*;

        assert!(!is_still_pending(Completed));
        assert!(!is_still_pending(Failed));
        assert!(!is_still_pending(Cancelled));
        assert!(!is_still_pending(Blocked));
        assert!(!is_still_pending(Rejected));
    }
}
