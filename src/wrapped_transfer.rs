//! Inbound wrapped-token transfer detection for the issuer wallet.
//!
//! The redemption transfer poller watches each asset's vault, i.e. the
//! unwrapped share token. A redemption sent as the ERC-4626 wrapped token
//! lands in the issuer wallet without ever being detected or redeemed. This
//! module is the issuance backstop: per network, it watches the configured
//! wrapped-token contracts for `Transfer` events to the issuer wallet, records
//! each one durably, raises an operator alert, and exposes the recorded
//! transfers to the admin API. Recovery itself is a manual operation.

use alloy::primitives::Address;
use std::collections::HashMap;
use std::time::Duration;

use crate::tokenized_asset::{Network, UnderlyingSymbol};

/// Interval between polling passes once a watcher is caught up. Inbound
/// wrapped-token transfers are rare and the alert is not latency critical, so
/// one `eth_getLogs` per token per minute is plenty.
pub(crate) const WRAPPED_TRANSFER_POLL_INTERVAL: Duration =
    Duration::from_secs(60);

/// Wrapped-token contract addresses to watch, per network, each mapped to the
/// underlying it wraps. Built from the `[wrapped_tokens.<network>]` tables of
/// the TOML config file; existence implies the entries passed validation.
#[derive(Debug, Clone, Default)]
pub struct WrappedTokenConfig {
    per_network: HashMap<Network, HashMap<Address, UnderlyingSymbol>>,
}

/// One `[wrapped_tokens.<network>]` entry: `underlying = "<token>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedTokenEntry {
    pub network: Network,
    pub underlying: UnderlyingSymbol,
    pub token: Address,
}

/// One wrapped token a network's watcher scans, with the underlying the alert
/// names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchedWrappedToken {
    pub(crate) token: Address,
    pub(crate) underlying: UnderlyingSymbol,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WrappedTokenConfigError {
    #[error(
        "wrapped token address for {underlying} on {network} is the zero \
         address"
    )]
    ZeroAddress { network: Network, underlying: UnderlyingSymbol },
    #[error(
        "wrapped token {token} on {network} is configured for both {first} \
         and {second}; one address cannot wrap two underlyings on one network"
    )]
    AddressCollision {
        network: Network,
        token: Address,
        first: UnderlyingSymbol,
        second: UnderlyingSymbol,
    },
    #[error("{underlying} on {network} has more than one wrapped token entry")]
    DuplicateUnderlying { network: Network, underlying: UnderlyingSymbol },
}

impl WrappedTokenConfig {
    /// Validates the entries: no zero address, and on each network one
    /// address wraps one underlying and one underlying has one address.
    ///
    /// # Errors
    ///
    /// Returns the first entry that violates one of those rules.
    pub fn new(
        entries: impl IntoIterator<Item = WrappedTokenEntry>,
    ) -> Result<Self, WrappedTokenConfigError> {
        let mut per_network: HashMap<
            Network,
            HashMap<Address, UnderlyingSymbol>,
        > = HashMap::new();

        for WrappedTokenEntry { network, underlying, token } in entries {
            if token.is_zero() {
                return Err(WrappedTokenConfigError::ZeroAddress {
                    network,
                    underlying,
                });
            }

            let tokens = per_network.entry(network).or_default();
            if tokens.values().any(|existing| existing == &underlying) {
                return Err(WrappedTokenConfigError::DuplicateUnderlying {
                    network,
                    underlying,
                });
            }

            if let Some(first) = tokens.insert(token, underlying.clone()) {
                return Err(WrappedTokenConfigError::AddressCollision {
                    network,
                    token,
                    first,
                    second: underlying,
                });
            }
        }

        Ok(Self { per_network })
    }

    /// The tokens to watch on `network`, sorted by address so a pass scans
    /// them in a deterministic order. Empty when the network has no entries.
    pub(crate) fn watched_on(
        &self,
        network: Network,
    ) -> Vec<WatchedWrappedToken> {
        let mut watched: Vec<WatchedWrappedToken> = self
            .per_network
            .get(&network)
            .into_iter()
            .flatten()
            .map(|(token, underlying)| WatchedWrappedToken {
                token: *token,
                underlying: underlying.clone(),
            })
            .collect();
        watched.sort_unstable_by_key(|watched| watched.token);
        watched
    }

    /// Every network that has at least one entry, so startup can reject a
    /// table for a chain that has no configuration.
    pub(crate) fn networks(&self) -> impl Iterator<Item = Network> + '_ {
        self.per_network
            .iter()
            .filter(|(_, tokens)| !tokens.is_empty())
            .map(|(network, _)| *network)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};

    use super::{
        WatchedWrappedToken, WrappedTokenConfig, WrappedTokenConfigError,
        WrappedTokenEntry,
    };
    use crate::tokenized_asset::{Network, UnderlyingSymbol};

    fn symbol(value: &str) -> UnderlyingSymbol {
        UnderlyingSymbol::new(value).unwrap()
    }

    fn entry(
        network: Network,
        underlying: &str,
        token: Address,
    ) -> WrappedTokenEntry {
        WrappedTokenEntry { network, underlying: symbol(underlying), token }
    }

    const TOKEN_A: Address =
        address!("0x00000000000000000000000000000000000000aa");
    const TOKEN_B: Address =
        address!("0x00000000000000000000000000000000000000bb");

    /// The same address may wrap the same underlying on two chains
    /// (deterministic deploys), and each network's watch list is sorted by
    /// address regardless of entry order.
    #[test]
    fn watched_tokens_are_per_network_and_sorted_by_address() {
        let config = WrappedTokenConfig::new([
            entry(Network::Base, "AAPL", TOKEN_B),
            entry(Network::Base, "RKLB", TOKEN_A),
            entry(Network::Ethereum, "RKLB", TOKEN_A),
        ])
        .unwrap();

        assert_eq!(
            config.watched_on(Network::Base),
            vec![
                WatchedWrappedToken {
                    token: TOKEN_A,
                    underlying: symbol("RKLB")
                },
                WatchedWrappedToken {
                    token: TOKEN_B,
                    underlying: symbol("AAPL")
                },
            ]
        );
        assert_eq!(
            config.watched_on(Network::Ethereum),
            vec![WatchedWrappedToken {
                token: TOKEN_A,
                underlying: symbol("RKLB")
            }]
        );
        assert!(config.watched_on(Network::HyperEvm).is_empty());

        let mut networks: Vec<Network> = config.networks().collect();
        networks.sort_unstable_by_key(Network::as_str);
        assert_eq!(networks, vec![Network::Base, Network::Ethereum]);
    }

    #[test]
    fn zero_address_is_rejected() {
        let error = WrappedTokenConfig::new([entry(
            Network::Base,
            "RKLB",
            Address::ZERO,
        )])
        .unwrap_err();

        assert_eq!(
            error,
            WrappedTokenConfigError::ZeroAddress {
                network: Network::Base,
                underlying: symbol("RKLB"),
            }
        );
    }

    /// One address cannot wrap two underlyings on one network: an inbound
    /// transfer of it could not be attributed to an asset.
    #[test]
    fn one_address_for_two_underlyings_on_a_network_is_rejected() {
        let error = WrappedTokenConfig::new([
            entry(Network::Base, "RKLB", TOKEN_A),
            entry(Network::Base, "AAPL", TOKEN_A),
        ])
        .unwrap_err();

        assert_eq!(
            error,
            WrappedTokenConfigError::AddressCollision {
                network: Network::Base,
                token: TOKEN_A,
                first: symbol("RKLB"),
                second: symbol("AAPL"),
            }
        );
    }

    #[test]
    fn two_addresses_for_one_underlying_on_a_network_is_rejected() {
        let error = WrappedTokenConfig::new([
            entry(Network::Base, "RKLB", TOKEN_A),
            entry(Network::Base, "RKLB", TOKEN_B),
        ])
        .unwrap_err();

        assert_eq!(
            error,
            WrappedTokenConfigError::DuplicateUnderlying {
                network: Network::Base,
                underlying: symbol("RKLB"),
            }
        );
    }
}
