use std::time::Duration;

use alloy::consensus::SignableTransaction;
use alloy::network::TxSigner;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::{Signature, Signer};
use async_trait::async_trait;
use fireblocks_sdk::apis::transactions_api::{
    CreateTransactionError, CreateTransactionParams,
};
use fireblocks_sdk::models;
use fireblocks_sdk::{Client, ClientBuilder, apis};
use tracing::debug;

use super::config::{
    AssetId, ChainAssetIds, Environment, FireblocksConfig, VaultAccountId,
};

#[derive(Debug, thiserror::Error)]
pub enum FireblocksError {
    #[error("failed to read Fireblocks secret key")]
    ReadSecret(#[from] std::io::Error),
    #[error("Fireblocks SDK error")]
    Sdk(#[from] fireblocks_sdk::FireblocksError),
    #[error("no deposit address found for vault {vault_id}, asset {asset_id}")]
    NoAddress { vault_id: String, asset_id: String },
    #[error("invalid deposit address from Fireblocks: {address}")]
    InvalidAddress {
        address: String,
        #[source]
        source: alloy::hex::FromHexError,
    },
    #[error("failed to create Fireblocks transaction")]
    CreateTransaction(#[from] apis::Error<CreateTransactionError>),
    #[error("Fireblocks transaction response missing ID")]
    MissingTransactionId,
    #[error("failed to poll Fireblocks transaction {tx_id}")]
    PollTransaction {
        tx_id: String,
        #[source]
        source: fireblocks_sdk::FireblocksError,
    },
    #[error(
        "Fireblocks transaction {tx_id} reached terminal status: {status:?}"
    )]
    TransactionFailed { tx_id: String, status: models::TransactionStatus },
    #[error("Fireblocks transaction {tx_id} returned no signed messages")]
    NoSignedMessages { tx_id: String },
    #[error("Fireblocks transaction {tx_id} returned signature without r/s/v")]
    IncompleteSignature { tx_id: String },
    #[error("failed to parse signature component from Fireblocks response")]
    ParseSignature(#[from] alloy::hex::FromHexError),
    #[error("no asset ID configured for chain {chain_id}")]
    UnknownChain { chain_id: u64 },
    #[error("chain_id not set on signer, cannot determine asset ID")]
    ChainIdNotSet,
}

/// Signer that delegates to the Fireblocks RAW signing API.
///
/// Constructed via `FireblocksSigner::new()`, which fetches the vault's deposit
/// address and caches it for `address()` calls.
#[derive(Clone)]
pub(crate) struct FireblocksSigner {
    client: Client,
    vault_account_id: VaultAccountId,
    chain_asset_ids: ChainAssetIds,
    address: Address,
    chain_id: Option<u64>,
}

impl std::fmt::Debug for FireblocksSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FireblocksSigner")
            .field("vault_account_id", &self.vault_account_id)
            .field("chain_asset_ids", &self.chain_asset_ids)
            .field("address", &self.address)
            .field("chain_id", &self.chain_id)
            .finish_non_exhaustive()
    }
}

impl FireblocksSigner {
    /// Create a new Fireblocks signer.
    ///
    /// Reads the RSA secret key from disk, builds the SDK client, and fetches
    /// the vault's deposit address to cache locally.
    pub(crate) async fn new(
        config: &FireblocksConfig,
    ) -> Result<Self, FireblocksError> {
        let secret = std::fs::read(&config.secret_path)?;

        let mut builder = ClientBuilder::new(config.api_key.as_str(), &secret);
        if config.environment == Environment::Sandbox {
            builder = builder.use_sandbox();
        }
        let client = builder.build()?;

        let default_asset_id = config.chain_asset_ids.default_asset_id();

        let addresses = client
            .addresses(
                config.vault_account_id.as_str(),
                default_asset_id.as_str(),
            )
            .await?;

        let address_str = addresses
            .first()
            .and_then(|a| a.address.as_deref())
            .ok_or_else(|| FireblocksError::NoAddress {
                vault_id: config.vault_account_id.as_str().to_string(),
                asset_id: default_asset_id.as_str().to_string(),
            })?;

        let address = address_str.parse::<Address>().map_err(|e| {
            FireblocksError::InvalidAddress {
                address: address_str.to_string(),
                source: e,
            }
        })?;

        debug!(
            vault_account_id = %config.vault_account_id.as_str(),
            chain_asset_ids = ?config.chain_asset_ids,
            %address,
            "Fireblocks signer initialized"
        );

        Ok(Self {
            client,
            vault_account_id: config.vault_account_id.clone(),
            chain_asset_ids: config.chain_asset_ids.clone(),
            address,
            chain_id: None,
        })
    }

    /// Sign a 32-byte hash via Fireblocks RAW signing.
    async fn sign_hash_inner(
        &self,
        hash: &B256,
    ) -> Result<Signature, FireblocksError> {
        let chain_id = self.chain_id.ok_or(FireblocksError::ChainIdNotSet)?;
        let asset_id = self
            .chain_asset_ids
            .get(chain_id)
            .ok_or(FireblocksError::UnknownChain { chain_id })?;

        let hash_hex = alloy::hex::encode(hash);
        let tx_request = Self::build_raw_signing_request(
            asset_id,
            &self.vault_account_id,
            &hash_hex,
        );

        let params = CreateTransactionParams::builder()
            .transaction_request(tx_request)
            .build();

        let create_response =
            self.client.transactions_api().create_transaction(params).await?;

        let tx_id =
            create_response.id.ok_or(FireblocksError::MissingTransactionId)?;

        debug!(fireblocks_tx_id = %tx_id, "Fireblocks RAW signing transaction created, polling...");

        let result = self
            .client
            .poll_transaction(
                &tx_id,
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
            .await
            .map_err(|e| FireblocksError::PollTransaction {
                tx_id: tx_id.clone(),
                source: e,
            })?;

        if result.status != models::TransactionStatus::Completed {
            return Err(FireblocksError::TransactionFailed {
                tx_id,
                status: result.status,
            });
        }

        Self::extract_signature(&tx_id, &result)
    }

    /// Build the Fireblocks RAW signing transaction request.
    ///
    /// For RAW signing operations, Fireblocks requires:
    /// - `operation`: Must be `RAW` for off-chain message signing
    /// - `asset_id`: Determines the key derivation path
    /// - `source`: VaultAccount with the vault ID containing the signing key
    /// - `extra_parameters.raw_message_data`: Contains the message(s) to sign
    ///   and the signing algorithm (ECDSA secp256k1 for EVM)
    ///
    /// All other fields (destination, amount, fee parameters, etc.) are for
    /// on-chain transfers and are not applicable to RAW signing.
    ///
    /// Reference: https://developers.fireblocks.com/docs/raw-signing
    fn build_raw_signing_request(
        asset_id: &AssetId,
        vault_account_id: &VaultAccountId,
        hash_hex: &str,
    ) -> models::TransactionRequest {
        let unsigned_msg = models::UnsignedMessage {
            content: hash_hex.to_string(),
            bip44address_index: None,
            bip44change: None,
            derivation_path: None,
            r#type: None,
            pre_hash: None,
        };

        let raw_message_data = models::ExtraParametersRawMessageData {
            messages: Some(vec![unsigned_msg]),
            algorithm: Some(
                models::extra_parameters_raw_message_data::Algorithm::MpcEcdsaSecp256K1,
            ),
        };

        let extra_parameters = models::ExtraParameters {
            raw_message_data: Some(raw_message_data),
            contract_call_data: None,
            inputs_selection: None,
            node_controls: None,
            program_call_data: None,
        };

        models::TransactionRequest {
            operation: Some(models::TransactionOperation::Raw),
            asset_id: Some(asset_id.as_str().to_string()),
            source: Some(models::SourceTransferPeerPath {
                r#type: models::TransferPeerPathType::VaultAccount,
                id: Some(vault_account_id.as_str().to_string()),
                sub_type: None,
                name: None,
                wallet_id: None,
                is_collateral: None,
            }),
            extra_parameters: Some(extra_parameters),
            external_tx_id: Some(uuid::Uuid::new_v4().to_string()),
            note: Some(format!("RAW sign: 0x{hash_hex}")),
            destination: None,
            destinations: None,
            amount: None,
            treat_as_gross_amount: None,
            force_sweep: None,
            fee_level: None,
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

    /// Extract an ECDSA signature from a completed Fireblocks transaction response.
    fn extract_signature(
        tx_id: &str,
        result: &models::TransactionResponse,
    ) -> Result<Signature, FireblocksError> {
        let incomplete = || FireblocksError::IncompleteSignature {
            tx_id: tx_id.to_string(),
        };

        let signed_msg = result
            .signed_messages
            .as_ref()
            .and_then(|msgs| msgs.first())
            .ok_or_else(|| FireblocksError::NoSignedMessages {
                tx_id: tx_id.to_string(),
            })?;

        let sig = signed_msg.signature.as_ref().ok_or_else(incomplete)?;

        let r_hex = sig.r.as_deref().ok_or_else(incomplete)?;
        let s_hex = sig.s.as_deref().ok_or_else(incomplete)?;
        let v = sig.v.as_ref().ok_or_else(incomplete)?;

        let r_bytes: [u8; 32] =
            alloy::hex::decode(r_hex)?.try_into().map_err(|_| incomplete())?;

        let s_bytes: [u8; 32] =
            alloy::hex::decode(s_hex)?.try_into().map_err(|_| incomplete())?;

        let y_parity =
            matches!(v, models::signed_message_signature::V::Variant1);

        let r = U256::from_be_bytes(r_bytes);
        let s = U256::from_be_bytes(s_bytes);

        Ok(Signature::new(r, s, y_parity))
    }
}

#[async_trait]
impl Signer for FireblocksSigner {
    async fn sign_hash(
        &self,
        hash: &B256,
    ) -> alloy::signers::Result<Signature> {
        self.sign_hash_inner(hash).await.map_err(alloy::signers::Error::other)
    }

    fn address(&self) -> Address {
        self.address
    }

    fn chain_id(&self) -> Option<u64> {
        self.chain_id
    }

    fn set_chain_id(&mut self, chain_id: Option<u64>) {
        self.chain_id = chain_id;
    }
}

#[async_trait]
impl TxSigner<Signature> for FireblocksSigner {
    fn address(&self) -> Address {
        self.address
    }

    async fn sign_transaction(
        &self,
        tx: &mut dyn SignableTransaction<Signature>,
    ) -> alloy::signers::Result<Signature> {
        let hash = tx.signature_hash();
        self.sign_hash(&hash).await
    }
}

#[cfg(test)]
mod tests {
    use fireblocks_sdk::models::{
        SignedMessage, SignedMessageSignature, TransactionResponse,
    };

    use super::*;

    #[test]
    fn build_raw_signing_request_sets_required_fields() {
        let asset_id: AssetId = "ETH".parse().unwrap();
        let vault_id: VaultAccountId = "123".to_string().into();
        let hash_hex = "abcd1234";

        let request = FireblocksSigner::build_raw_signing_request(
            &asset_id, &vault_id, hash_hex,
        );

        assert_eq!(request.operation, Some(models::TransactionOperation::Raw));
        assert_eq!(request.asset_id, Some("ETH".to_string()));

        let source = request.source.unwrap();
        assert_eq!(source.r#type, models::TransferPeerPathType::VaultAccount);
        assert_eq!(source.id, Some("123".to_string()));

        let extra = request.extra_parameters.unwrap();
        let raw_data = extra.raw_message_data.unwrap();
        assert_eq!(
            raw_data.algorithm,
            Some(models::extra_parameters_raw_message_data::Algorithm::MpcEcdsaSecp256K1)
        );

        let messages = raw_data.messages.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, hash_hex);
    }

    #[test]
    fn extract_signature_succeeds_with_valid_response() {
        let response = TransactionResponse {
            signed_messages: Some(vec![SignedMessage {
                signature: Some(SignedMessageSignature {
                    r: Some("a".repeat(64)),
                    s: Some("b".repeat(64)),
                    v: Some(models::signed_message_signature::V::Variant0),
                    full_sig: None,
                }),
                content: None,
                derivation_path: None,
                algorithm: None,
                public_key: None,
            }]),
            ..Default::default()
        };

        let sig =
            FireblocksSigner::extract_signature("tx-123", &response).unwrap();

        let expected_r = U256::from_be_bytes([0xaa; 32]);
        let expected_s = U256::from_be_bytes([0xbb; 32]);

        assert_eq!(sig.r(), expected_r);
        assert_eq!(sig.s(), expected_s);
        assert!(!sig.v());
    }

    #[test]
    fn extract_signature_fails_without_signed_messages() {
        let response =
            TransactionResponse { signed_messages: None, ..Default::default() };

        let result = FireblocksSigner::extract_signature("tx-123", &response);
        assert!(matches!(
            result,
            Err(FireblocksError::NoSignedMessages { .. })
        ));
    }

    #[test]
    fn extract_signature_fails_with_missing_r() {
        let response = TransactionResponse {
            signed_messages: Some(vec![SignedMessage {
                signature: Some(SignedMessageSignature {
                    r: None,
                    s: Some("b".repeat(64)),
                    v: Some(models::signed_message_signature::V::Variant0),
                    full_sig: None,
                }),
                content: None,
                derivation_path: None,
                algorithm: None,
                public_key: None,
            }]),
            ..Default::default()
        };

        let result = FireblocksSigner::extract_signature("tx-123", &response);
        assert!(matches!(
            result,
            Err(FireblocksError::IncompleteSignature { .. })
        ));
    }

    #[test]
    fn extract_signature_parses_v_variant1_as_odd_parity() {
        let response = TransactionResponse {
            signed_messages: Some(vec![SignedMessage {
                signature: Some(SignedMessageSignature {
                    r: Some("0".repeat(64)),
                    s: Some("0".repeat(64)),
                    v: Some(models::signed_message_signature::V::Variant1),
                    full_sig: None,
                }),
                content: None,
                derivation_path: None,
                algorithm: None,
                public_key: None,
            }]),
            ..Default::default()
        };

        let sig =
            FireblocksSigner::extract_signature("tx-123", &response).unwrap();
        assert!(sig.v());
    }

    #[test]
    fn extract_signature_parses_v_variant0_as_even_parity() {
        let response = TransactionResponse {
            signed_messages: Some(vec![SignedMessage {
                signature: Some(SignedMessageSignature {
                    r: Some("0".repeat(64)),
                    s: Some("0".repeat(64)),
                    v: Some(models::signed_message_signature::V::Variant0),
                    full_sig: None,
                }),
                content: None,
                derivation_path: None,
                algorithm: None,
                public_key: None,
            }]),
            ..Default::default()
        };

        let sig =
            FireblocksSigner::extract_signature("tx-123", &response).unwrap();
        assert!(!sig.v());
    }
}
