//! Narrow slice of the retired Fireblocks integration, kept for the
//! receipt-custody migration only.
//!
//! The custody migration's forward leg is necessarily signed by the *retiring*
//! custodian: ERC-1155 only lets the holder move its own balance, and the
//! holder is the Fireblocks wallet. This module carries exactly the pieces
//! that submitting that one transfer requires — configuration, vault-address
//! lookup, whitelisted-contract resolution, `CONTRACT_CALL` submission with
//! deterministic `externalTxId` idempotency, and polling to a terminal status.
//! The retired service-side integration (the `VaultService` trait impl and
//! mint/burn paths) is gone for good.
//!
//! Temporary: leaves together with `migrate-receipts` once every vault has
//! migrated.

mod config;
pub mod vault_service;

use clap::Args;

pub use config::{
    ChainAssetIds, Environment, FireblocksConfig, FireblocksConfigError,
    parse_chain_asset_ids,
};
pub use vault_service::{
    FireblocksVaultError, FireblocksVaultService, fetch_vault_address,
};

/// Fireblocks credentials and settings for the migration CLI, using the same
/// environment variable names the retired integration used. The host wiring
/// supplies `FIREBLOCKS_SECRET_PATH` (the deploy activation installs the key
/// and exports the path); the remaining values come from the service
/// environment file.
#[derive(Args, Debug, Clone)]
pub(crate) struct FireblocksEnv {
    /// Fireblocks API User ID.
    #[clap(
        id = "fireblocks_api_user_id",
        long = "fireblocks-api-user-id",
        env = "FIREBLOCKS_API_USER_ID"
    )]
    api_user_id: Option<String>,

    /// Path to the RSA private key file for Fireblocks API authentication.
    #[clap(
        id = "fireblocks_secret_path",
        long = "fireblocks-secret-path",
        env = "FIREBLOCKS_SECRET_PATH"
    )]
    secret_path: Option<std::path::PathBuf>,

    /// Fireblocks vault account ID containing the signing key.
    #[clap(
        id = "fireblocks_vault_account_id",
        long = "fireblocks-vault-account-id",
        env = "FIREBLOCKS_VAULT_ACCOUNT_ID",
        default_value = "0"
    )]
    vault_account_id: String,

    /// Mapping of chain ID to Fireblocks asset ID, e.g.
    /// "1:ETH,8453:BASECHAIN_ETH".
    #[clap(
        id = "fireblocks_chain_asset_ids",
        long = "fireblocks-chain-asset-ids",
        env = "FIREBLOCKS_CHAIN_ASSET_IDS",
        default_value = "8453:BASECHAIN_ETH",
        value_parser = parse_chain_asset_ids
    )]
    chain_asset_ids: ChainAssetIds,

    /// Fireblocks environment (production or sandbox).
    #[clap(
        id = "fireblocks_environment",
        long = "fireblocks-environment",
        env = "FIREBLOCKS_ENVIRONMENT",
        default_value = "production",
        value_enum
    )]
    environment: Environment,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FireblocksEnvError {
    #[error("FIREBLOCKS_API_USER_ID is required to sign the forward transfer")]
    MissingApiUserId,
    #[error(
        "FIREBLOCKS_SECRET_PATH is required when FIREBLOCKS_API_USER_ID is set"
    )]
    MissingSecretPath,
    #[error(transparent)]
    Config(#[from] FireblocksConfigError),
}

impl FireblocksEnv {
    pub(crate) fn into_config(
        self,
    ) -> Result<FireblocksConfig, FireblocksEnvError> {
        let api_user_id =
            self.api_user_id.ok_or(FireblocksEnvError::MissingApiUserId)?;
        let secret_path =
            self.secret_path.ok_or(FireblocksEnvError::MissingSecretPath)?;

        Ok(FireblocksConfig::new(
            api_user_id.into(),
            &secret_path,
            self.vault_account_id.into(),
            self.chain_asset_ids,
            self.environment,
        )?)
    }
}
