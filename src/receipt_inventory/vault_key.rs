use std::fmt;
use std::str::FromStr;

use alloy::primitives::Address;

/// Receipt inventory aggregate id: `{chain_id}:{vault}`.
///
/// Pre-multichain rows used bare EIP-55 `{vault}` addresses; the
/// `rekey_receipt_inventory_aggregate_id` migration rewrites them to this
/// chain-qualified lowercase form, so runtime lookups never consult a legacy
/// key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ReceiptVaultKey(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReceiptVaultKeyError {
    #[error("receipt vault key cannot be empty")]
    Empty,
}

impl ReceiptVaultKey {
    pub(crate) fn new(chain_id: u64, vault: Address) -> Self {
        Self(format!("{chain_id}:{vault:#x}"))
    }
}

impl fmt::Display for ReceiptVaultKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for ReceiptVaultKey {
    type Err = ReceiptVaultKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(ReceiptVaultKeyError::Empty);
        }

        Ok(Self(value.to_string()))
    }
}
