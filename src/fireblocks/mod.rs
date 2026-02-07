mod config;
pub(crate) mod vault_service;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use clap::{Args, Parser};

pub(crate) use config::{
    ChainAssetIds, Environment, FireblocksConfig, FireblocksConfigError,
    parse_chain_asset_ids,
};
pub(crate) use vault_service::{
    FireblocksVaultError, FireblocksVaultService, fetch_vault_address,
};

/// Resolved signer: an `EthereumWallet` and the corresponding address.
///
/// Only used for local signing. Fireblocks path uses `FireblocksVaultService` directly.
pub(crate) struct ResolvedSigner {
    pub(crate) wallet: EthereumWallet,
}

/// Command-line arguments for signer configuration.
///
/// Exactly one of the two signing backends must be configured:
/// - Local: `--evm-private-key` / `EVM_PRIVATE_KEY`
/// - Fireblocks: `--fireblocks-api-user-id` / `FIREBLOCKS_API_USER_ID`
#[derive(Parser, Debug, Clone)]
pub(crate) struct SignerEnv {
    #[clap(flatten)]
    local: LocalSignerEnv,

    #[clap(flatten)]
    fireblocks: FireblocksEnv,
}

/// Local EVM private key signer configuration.
#[derive(Args, Debug, Clone)]
#[group(id = "local_signer")]
struct LocalSignerEnv {
    /// Private key for signing EVM transactions (mutually exclusive with Fireblocks)
    #[clap(long, env)]
    evm_private_key: Option<B256>,
}

/// Fireblocks signer configuration.
#[derive(Args, Debug, Clone)]
#[group(id = "fireblocks_signer")]
struct FireblocksEnv {
    /// Fireblocks API User ID (mutually exclusive with local private key)
    #[clap(
        id = "fireblocks_api_user_id",
        long = "fireblocks-api-user-id",
        env = "FIREBLOCKS_API_USER_ID"
    )]
    api_user_id: Option<String>,

    /// Path to the RSA private key file for Fireblocks API authentication
    #[clap(
        id = "fireblocks_secret_path",
        long = "fireblocks-secret-path",
        env = "FIREBLOCKS_SECRET_PATH"
    )]
    secret_path: Option<std::path::PathBuf>,

    /// Fireblocks vault account ID containing the signing key (defaults to "0")
    #[clap(
        id = "fireblocks_vault_account_id",
        long = "fireblocks-vault-account-id",
        env = "FIREBLOCKS_VAULT_ACCOUNT_ID",
        default_value = "0"
    )]
    vault_account_id: String,

    /// Mapping of chain ID to Fireblocks asset ID, e.g. "1:ETH,8453:BASECHAIN_ETH"
    #[clap(id = "fireblocks_chain_asset_ids", long = "fireblocks-chain-asset-ids", env = "FIREBLOCKS_CHAIN_ASSET_IDS", default_value = "8453:BASECHAIN_ETH", value_parser = parse_chain_asset_ids)]
    chain_asset_ids: ChainAssetIds,

    /// Fireblocks environment (production or sandbox)
    #[clap(
        id = "fireblocks_environment",
        long = "fireblocks-environment",
        env = "FIREBLOCKS_ENVIRONMENT",
        default_value = "production",
        value_enum
    )]
    environment: Environment,
}

/// Validated signer configuration.
#[derive(Debug, Clone)]
pub enum SignerConfig {
    Fireblocks(FireblocksConfig),
    Local(B256),
}

#[derive(Debug, thiserror::Error)]
pub enum SignerConfigError {
    #[error(
        "exactly one of EVM_PRIVATE_KEY or FIREBLOCKS_API_USER_ID must be set"
    )]
    NeitherConfigured,
    #[error(
        "both EVM_PRIVATE_KEY and FIREBLOCKS_API_USER_ID are set; use only one"
    )]
    BothConfigured,
    #[error(
        "FIREBLOCKS_SECRET_PATH is required when FIREBLOCKS_API_USER_ID is set"
    )]
    MissingSecretPath,
    #[error(transparent)]
    FireblocksConfig(#[from] FireblocksConfigError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SignerResolveError {
    #[error(transparent)]
    FireblocksVault(#[from] FireblocksVaultError),
    #[error("invalid EVM private key")]
    InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
}

impl SignerEnv {
    pub(crate) fn into_config(self) -> Result<SignerConfig, SignerConfigError> {
        match (self.local.evm_private_key, self.fireblocks.api_user_id) {
            (Some(_), Some(_)) => Err(SignerConfigError::BothConfigured),
            (None, None) => Err(SignerConfigError::NeitherConfigured),
            (Some(key), None) => Ok(SignerConfig::Local(key)),
            (None, Some(api_user_id)) => {
                let secret_path = self
                    .fireblocks
                    .secret_path
                    .ok_or(SignerConfigError::MissingSecretPath)?;

                let config = FireblocksConfig::new(
                    api_user_id.into(),
                    &secret_path,
                    self.fireblocks.vault_account_id.into(),
                    self.fireblocks.chain_asset_ids,
                    self.fireblocks.environment,
                )?;

                Ok(SignerConfig::Fireblocks(config))
            }
        }
    }
}

/// Resolve a local private key into a wallet.
///
/// The chain_id is set on the signer for transaction signing.
pub(crate) fn resolve_local_signer(
    key: &B256,
    chain_id: u64,
) -> Result<ResolvedSigner, SignerResolveError> {
    let mut signer = PrivateKeySigner::from_bytes(key)?;
    signer.set_chain_id(Some(chain_id));
    let wallet = EthereumWallet::from(signer);
    Ok(ResolvedSigner { wallet })
}

impl SignerConfig {
    /// Derive the address from the signer configuration.
    ///
    /// For local keys this is synchronous. For Fireblocks, this requires an
    /// async API call to fetch the vault address.
    pub(crate) async fn address(&self) -> Result<Address, SignerResolveError> {
        match self {
            Self::Fireblocks(config) => {
                let address = fetch_vault_address(config).await?;
                Ok(address)
            }
            Self::Local(key) => {
                let signer = PrivateKeySigner::from_bytes(key)?;
                Ok(signer.address())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_chain_asset_ids() -> ChainAssetIds {
        parse_chain_asset_ids("1:ETH").unwrap()
    }

    #[test]
    fn local_key_produces_local_config() {
        let key = B256::from([1u8; 32]);

        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: Some(key) },
            fireblocks: FireblocksEnv {
                api_user_id: None,
                secret_path: None,
                vault_account_id: "0".to_string(),
                chain_asset_ids: default_chain_asset_ids(),
                environment: Environment::Production,
            },
        };

        let config = env.into_config().unwrap();
        assert!(
            matches!(config, SignerConfig::Local(k) if k == key),
            "Expected Local config, got {config:?}"
        );
    }

    #[test]
    fn neither_configured_fails() {
        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: None },
            fireblocks: FireblocksEnv {
                api_user_id: None,
                secret_path: None,
                vault_account_id: "0".to_string(),
                chain_asset_ids: default_chain_asset_ids(),
                environment: Environment::Production,
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::NeitherConfigured)),
            "Expected NeitherConfigured error, got {result:?}"
        );
    }

    #[test]
    fn both_configured_fails() {
        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: Some(B256::ZERO) },
            fireblocks: FireblocksEnv {
                api_user_id: Some("test-user-id".to_string()),
                secret_path: Some("/path/to/key".into()),
                vault_account_id: "0".to_string(),
                chain_asset_ids: default_chain_asset_ids(),
                environment: Environment::Production,
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::BothConfigured)),
            "Expected BothConfigured error, got {result:?}"
        );
    }

    #[test]
    fn fireblocks_missing_secret_path_fails() {
        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: None },
            fireblocks: FireblocksEnv {
                api_user_id: Some("test-user-id".to_string()),
                secret_path: None,
                vault_account_id: "0".to_string(),
                chain_asset_ids: default_chain_asset_ids(),
                environment: Environment::Production,
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::MissingSecretPath)),
            "Expected MissingSecretPath error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn local_signer_resolves_to_correct_address() {
        let mut key = B256::ZERO;
        key.0[31] = 1;
        let config = SignerConfig::Local(key);

        let address = config.address().await.unwrap();

        assert_eq!(
            address,
            "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
                .parse::<Address>()
                .unwrap()
        );
    }
}
