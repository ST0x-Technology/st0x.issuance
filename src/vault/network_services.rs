//! Per-network vault service lookup, built once at startup and shared by
//! every consumer that dispatches on-chain work by network (mint and
//! redemption command handling, burn recovery, admin recovery/triage).
//!
//! Consolidates the previously-duplicated `HashMap<Network,
//! Arc<dyn VaultService>>` + get-or-error copies so the lookup and its error
//! cannot drift between consumers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::tokenized_asset::Network;
use crate::vault::VaultService;

/// The signing backend and chain id serving one configured network.
#[derive(Clone)]
pub(crate) struct NetworkVault {
    pub(crate) service: Arc<dyn VaultService>,
    pub(crate) chain_id: u64,
}

/// Per-network [`NetworkVault`] entries, built once at startup from the
/// chain registry and cloned into every consumer.
#[derive(Clone)]
pub(crate) struct NetworkVaultServices {
    entries: Arc<HashMap<Network, NetworkVault>>,
}

/// A network has no configured vault service. Consumers convert this into
/// their own domain error at the boundary.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[error("no vault service configured for network {network}")]
pub(crate) struct UnconfiguredNetworkError {
    pub(crate) network: Network,
}

impl NetworkVaultServices {
    pub(crate) fn new(entries: HashMap<Network, NetworkVault>) -> Self {
        Self { entries: Arc::new(entries) }
    }

    pub(crate) fn get(
        &self,
        network: Network,
    ) -> Result<&NetworkVault, UnconfiguredNetworkError> {
        self.entries.get(&network).ok_or(UnconfiguredNetworkError { network })
    }

    pub(crate) fn service(
        &self,
        network: Network,
    ) -> Result<&Arc<dyn VaultService>, UnconfiguredNetworkError> {
        self.get(network).map(|entry| &entry.service)
    }

    pub(crate) fn chain_id(
        &self,
        network: Network,
    ) -> Result<u64, UnconfiguredNetworkError> {
        self.get(network).map(|entry| entry.chain_id)
    }

    /// Test-only convenience wrapping one mock vault the way production
    /// Rocket state wires the real per-network map.
    pub(crate) fn with_single_vault(
        network: Network,
        chain_id: u64,
        service: Arc<dyn VaultService>,
    ) -> Self {
        Self::new(HashMap::from([(
            network,
            NetworkVault { service, chain_id },
        )]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::mock::MockVaultService;

    #[test]
    fn configured_network_resolves_service_and_chain_id() {
        let services = NetworkVaultServices::with_single_vault(
            Network::Base,
            8453,
            Arc::new(MockVaultService::new_success()),
        );

        services.service(Network::Base).unwrap();
        assert_eq!(services.chain_id(Network::Base).unwrap(), 8453);
    }

    #[test]
    fn unconfigured_network_fails_closed() {
        let services = NetworkVaultServices::with_single_vault(
            Network::Base,
            8453,
            Arc::new(MockVaultService::new_success()),
        );

        assert!(matches!(
            services.get(Network::Ethereum),
            Err(UnconfiguredNetworkError { network: Network::Ethereum })
        ));
    }
}
