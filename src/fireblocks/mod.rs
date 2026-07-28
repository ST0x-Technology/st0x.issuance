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

pub use config::{
    ChainAssetIds, Environment, FireblocksConfig, FireblocksConfigError,
    parse_chain_asset_ids,
};
pub use vault_service::{
    FireblocksVaultError, FireblocksVaultService, fetch_vault_address,
};
