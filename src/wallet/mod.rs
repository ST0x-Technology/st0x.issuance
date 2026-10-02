pub(crate) mod gcp_kms_stamper;
pub(crate) mod local;
pub(crate) mod turnkey;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use clap::{Args, Parser};
use serde::Deserialize;
use turnkey::{
    InvalidKmsApiKey, TurnkeyApiPrivateKey, TurnkeyConfig, TurnkeyCredentials,
    TurnkeyError, TurnkeyKmsApiKey, TurnkeyOrganizationId,
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

    /// Cloud KMS key version whose public half is registered as the Turnkey
    /// API user (`projects/.../cryptoKeyVersions/N`). Requests are stamped
    /// by KMS under the runtime's ambient GCP identity, so no API private
    /// key is stored. Not a secret: it names an IAM-gated key. Exclusive
    /// with `TURNKEY_API_PRIVATE_KEY`.
    #[clap(
        id = "turnkey_kms_api_key",
        long = "turnkey-kms-api-key",
        env = "TURNKEY_KMS_API_KEY"
    )]
    kms_api_key: Option<String>,
}

/// Validated signer configuration.
#[derive(Debug, Clone)]
pub enum SignerConfig {
    Turnkey(TurnkeyConfig),
    Local(B256),
}

/// Errors during signer configuration validation.
#[derive(Debug, thiserror::Error)]
pub enum SignerConfigError {
    #[error("exactly one of EVM_PRIVATE_KEY or TURNKEY_ORG_ID must be set")]
    NeitherConfigured,
    #[error("both EVM_PRIVATE_KEY and TURNKEY_ORG_ID are set; use only one")]
    BothConfigured,
    #[error(
        "exactly one of TURNKEY_API_PRIVATE_KEY or TURNKEY_KMS_API_KEY is \
         required when TURNKEY_ORG_ID is set"
    )]
    MissingTurnkeyCredential,
    #[error(
        "both TURNKEY_API_PRIVATE_KEY and TURNKEY_KMS_API_KEY are set; use only one"
    )]
    AmbiguousTurnkeyCredential,
    #[error(transparent)]
    InvalidKmsApiKey(#[from] InvalidKmsApiKey),
    #[error("TURNKEY_ADDRESS is required when TURNKEY_ORG_ID is set")]
    MissingAddress,
}

/// Errors during signer resolution and initialization.
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
                self.turnkey.kms_api_key,
                self.turnkey.address,
            ),
        }
    }
}

fn signer_config_from_turnkey(
    org_id: String,
    api_private_key: Option<String>,
    kms_api_key: Option<String>,
    address: Option<Address>,
) -> Result<SignerConfig, SignerConfigError> {
    // An exported-but-empty variable (a `KEY=` line left behind during a
    // credential cutover) counts as unset, not as a second credential.
    let non_empty =
        |value: Option<String>| value.filter(|text| !text.is_empty());
    let credentials = match (non_empty(api_private_key), non_empty(kms_api_key))
    {
        (Some(key), None) => {
            TurnkeyCredentials::ApiKey(TurnkeyApiPrivateKey::new(key))
        }
        (None, Some(key_version)) => {
            TurnkeyCredentials::Kms(TurnkeyKmsApiKey::parse(key_version)?)
        }
        (Some(_), Some(_)) => {
            return Err(SignerConfigError::AmbiguousTurnkeyCredential);
        }
        (None, None) => {
            return Err(SignerConfigError::MissingTurnkeyCredential);
        }
    };
    let address = address.ok_or(SignerConfigError::MissingAddress)?;

    Ok(SignerConfig::Turnkey(TurnkeyConfig::new(
        TurnkeyOrganizationId::new(org_id),
        credentials,
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
                kms_api_key: None,
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
                kms_api_key: None,
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
                kms_api_key: None,
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::BothConfigured)),
            "Expected BothConfigured error, got {result:?}"
        );
    }

    #[test]
    fn turnkey_missing_credential_fails() {
        let env = SignerEnv {
            local: LocalSignerEnv { evm_private_key: None },
            turnkey: TurnkeyEnv {
                org_id: Some("test-user-id".to_string()),
                api_private_key: None,
                address: Some(Address::random()),
                kms_api_key: None,
            },
        };

        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::MissingTurnkeyCredential)),
            "Expected MissingTurnkeyCredential error, got {result:?}"
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
                kms_api_key: None,
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
                kms_api_key: None,
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

    const KMS_KEY: &str =
        "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1";

    fn turnkey_env(
        api_private_key: Option<&str>,
        kms_api_key: Option<&str>,
    ) -> SignerEnv {
        SignerEnv {
            local: LocalSignerEnv { evm_private_key: None },
            turnkey: TurnkeyEnv {
                org_id: Some("org-abc123".to_string()),
                api_private_key: api_private_key.map(str::to_string),
                address: Some(Address::from([0xbbu8; 20])),
                kms_api_key: kms_api_key.map(str::to_string),
            },
        }
    }

    #[test]
    fn turnkey_kms_api_key_produces_kms_credentials() {
        let config = turnkey_env(None, Some(KMS_KEY)).into_config().unwrap();

        let SignerConfig::Turnkey(turnkey) = config else {
            panic!("Expected Turnkey config, got {config:?}");
        };
        assert!(
            matches!(turnkey.credentials, TurnkeyCredentials::Kms(_)),
            "Expected KMS credentials, got {:?}",
            turnkey.credentials
        );
    }

    #[test]
    fn turnkey_both_credentials_fail() {
        let result = turnkey_env(Some("some-key"), Some(KMS_KEY)).into_config();

        assert!(
            matches!(
                result,
                Err(SignerConfigError::AmbiguousTurnkeyCredential)
            ),
            "Expected AmbiguousTurnkeyCredential error, got {result:?}"
        );
    }

    #[test]
    fn turnkey_empty_api_private_key_is_ignored_next_to_kms() {
        let config =
            turnkey_env(Some(""), Some(KMS_KEY)).into_config().unwrap();

        let SignerConfig::Turnkey(turnkey) = config else {
            panic!("Expected Turnkey config, got {config:?}");
        };
        assert!(
            matches!(turnkey.credentials, TurnkeyCredentials::Kms(_)),
            "Expected KMS credentials, got {:?}",
            turnkey.credentials
        );
    }

    #[test]
    fn turnkey_empty_credentials_fail_as_missing() {
        let result = turnkey_env(Some(""), Some("")).into_config();

        assert!(
            matches!(result, Err(SignerConfigError::MissingTurnkeyCredential)),
            "Expected MissingTurnkeyCredential error, got {result:?}"
        );
    }

    #[test]
    fn turnkey_kms_api_key_accepts_domain_scoped_project() {
        let key = "projects/example.com:my-project/locations/global/keyRings/r_1/cryptoKeys/k-1/cryptoKeyVersions/12";

        let config = turnkey_env(None, Some(key)).into_config().unwrap();

        assert!(
            matches!(
                config,
                SignerConfig::Turnkey(TurnkeyConfig {
                    credentials: TurnkeyCredentials::Kms(_),
                    ..
                })
            ),
            "Expected KMS credentials, got {config:?}"
        );
    }

    #[test]
    fn turnkey_malformed_kms_api_key_fails() {
        for malformed in [
            "projects/p/locations/l/keyRings/r/cryptoKeys/k",
            "projects//locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1",
            "//cloudkms.googleapis.com/v1/projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1",
            "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1#fragment",
            "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1 ",
            "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/latest",
            "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1/extra",
        ] {
            let result = turnkey_env(None, Some(malformed)).into_config();

            assert!(
                matches!(result, Err(SignerConfigError::InvalidKmsApiKey(_))),
                "Expected InvalidKmsApiKey for {malformed:?}, got {result:?}"
            );
        }
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
