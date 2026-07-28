pub(crate) mod local;
pub(crate) mod turnkey;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use clap::{Args, Parser};
use serde::Deserialize;
use turnkey::{
    TurnkeyApiPrivateKey, TurnkeyConfig, TurnkeyError, TurnkeyOrganizationId,
};

/// Wallet backend discriminant. Deserialized from a `kind` field in wallet config sections
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum WalletKind {
    Local,
    Turnkey,
}

/// Resolved signer: an `EthereumWallet` and the corresponding address.
///
/// Both Turnkey and local use this path.
pub(crate) struct ResolvedSigner {
    pub(crate) kind: WalletKind,
    pub(crate) wallet: EthereumWallet,
}

/// Command-line arguments for signer configuration.
///
/// Exactly one of the two signing backends must be configured:
/// - Local: `--evm-private-key` / `EVM_PRIVATE_KEY`
/// - Turnkey: `--turnkey-org-id` / `TURNKEY_ORG_ID`
#[derive(Parser, Debug, Clone)]
pub(crate) struct SignerEnv {
    #[clap(flatten)]
    local: LocalSignerEnv,

    #[clap(flatten)]
    turnkey: TurnkeyEnv,
}

/// Local EVM private key signer configuration.
#[derive(Args, Debug, Clone)]
#[group(id = "local_signer")]
struct LocalSignerEnv {
    /// Private key for signing EVM transactions (mutually exclusive with Turnkey)
    #[clap(long, env)]
    evm_private_key: Option<B256>,
}

/// Turnkey signer configuration.
#[derive(Args, Debug, Clone)]
#[group(id = "turnkey_signer")]
struct TurnkeyEnv {
    /// Turnkey Org ID (mutually exclusive with local private key)
    #[clap(
        id = "turnkey_org_id",
        long = "turnkey-org-id",
        env = "TURNKEY_ORG_ID"
    )]
    org_id: Option<String>,

    /// Turnkey API private key
    #[clap(
        id = "turnkey_api_private_key",
        long = "turnkey-api-private-key",
        env = "TURNKEY_API_PRIVATE_KEY"
    )]
    api_private_key: Option<String>,

    /// Turnkey address
    #[clap(
        id = "turnkey_address",
        long = "turnkey-address",
        env = "TURNKEY_ADDRESS"
    )]
    address: Option<Address>,
}

/// Validated signer configuration.
#[derive(Debug, Clone)]
pub enum SignerConfig {
    Turnkey(TurnkeyConfig),
    Local(B256),
}

#[derive(Debug, thiserror::Error)]
pub enum SignerConfigError {
    #[error("exactly one of EVM_PRIVATE_KEY or TURNKEY_ORG_ID must be set")]
    NeitherConfigured,
    #[error("both EVM_PRIVATE_KEY and TURNKEY_ORG_ID are set; use only one")]
    BothConfigured,
    #[error("TURNKEY_API_PRIVATE_KEY is required when TURNKEY_ORG_ID is set")]
    MissingApiPrivateKey,
    #[error("TURNKEY_ADDRESS is required when TURNKEY_ORG_ID is set")]
    MissingAddress,
}

#[derive(Debug, thiserror::Error)]
pub enum SignerResolveError {
    #[error(transparent)]
    Turnkey(#[from] TurnkeyError),
    #[error("invalid EVM private key")]
    InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
}

impl SignerEnv {
    pub(crate) fn into_config(self) -> Result<SignerConfig, SignerConfigError> {
        match (self.local.evm_private_key, self.turnkey.org_id) {
            (Some(_), Some(_)) => Err(SignerConfigError::BothConfigured),
            (None, None) => Err(SignerConfigError::NeitherConfigured),
            (Some(key), None) => Ok(SignerConfig::Local(key)),
            (None, Some(org_id)) => signer_config_from_turnkey(
                org_id,
                self.turnkey.api_private_key,
                self.turnkey.address,
            ),
        }
    }
}

fn signer_config_from_turnkey(
    org_id: String,
    api_private_key: Option<String>,
    address: Option<Address>,
) -> Result<SignerConfig, SignerConfigError> {
    let api_private_key = TurnkeyApiPrivateKey::new(
        api_private_key.ok_or(SignerConfigError::MissingApiPrivateKey)?,
    );
    let address = address.ok_or(SignerConfigError::MissingAddress)?;

    Ok(SignerConfig::Turnkey(TurnkeyConfig::new(
        TurnkeyOrganizationId::new(org_id),
        api_private_key,
        address,
    )))
}

impl SignerConfig {
    /// Derive the address from the signer configuration.
    ///
    /// For local keys this is synchronous. For Turnkey, this is the env/cli arg.
    pub(crate) fn address(&self) -> Result<Address, SignerResolveError> {
        match self {
            Self::Turnkey(config) => Ok(config.settings.address),
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

    #[test]
    fn local_key_produces_local_config() {
        let key = B256::from([1u8; 32]);

        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: Some(key) },
            turnkey: TurnkeyEnv {
                org_id: None,
                api_private_key: None,
                address: None,
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
            turnkey: TurnkeyEnv {
                org_id: None,
                api_private_key: None,
                address: None,
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
            turnkey: TurnkeyEnv {
                org_id: Some("test-user-id".to_string()),
                api_private_key: Some("some-key".to_string()),
                address: Some(Address::random()),
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::BothConfigured)),
            "Expected BothConfigured error, got {result:?}"
        );
    }

    #[test]
    fn turnkey_missing_api_private_key_fails() {
        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: None },
            turnkey: TurnkeyEnv {
                org_id: Some("test-user-id".to_string()),
                api_private_key: None,
                address: Some(Address::random()),
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::MissingApiPrivateKey)),
            "Expected MissingApiPrivateKey error, got {result:?}"
        );
    }

    #[test]
    fn turnkey_missing_address_fails() {
        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: None },
            turnkey: TurnkeyEnv {
                org_id: Some("test-user-id".to_string()),
                api_private_key: Some("some-key".to_string()),
                address: None,
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::MissingAddress)),
            "Expected MissingAddress error, got {result:?}"
        );
    }

    #[test]
    fn turnkey_fully_configured_produces_turnkey_config_with_correct_address() {
        let expected_address = Address::from([0xaau8; 20]);

        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: None },
            turnkey: TurnkeyEnv {
                org_id: Some("org-abc123".to_string()),
                api_private_key: Some("some-api-key".to_string()),
                address: Some(expected_address),
            },
        };

        let config = env.into_config().unwrap();
        assert!(
            matches!(config, SignerConfig::Turnkey(_)),
            "Expected Turnkey config, got {config:?}"
        );
        assert_eq!(
            config.address().unwrap(),
            expected_address,
            "Turnkey config must carry the address supplied at construction"
        );
    }

    #[tokio::test]
    async fn local_signer_resolves_to_correct_address() {
        let mut key = B256::ZERO;
        key.0[31] = 1;
        let config = SignerConfig::Local(key);

        let address = config.address().unwrap();

        assert_eq!(
            address,
            "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
                .parse::<Address>()
                .unwrap()
        );
    }
}
