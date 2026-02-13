mod api;
mod cmd;
mod event;
pub(crate) mod view;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};

pub(crate) use api::{
    add_tokenized_asset, get_tokenized_asset, list_tokenized_assets,
};
pub(crate) use cmd::TokenizedAssetCommand;
pub(crate) use event::TokenizedAssetEvent;
pub(crate) use view::{TokenizedAssetView, TokenizedAssetViewRepo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnderlyingSymbol(pub(crate) String);

impl std::fmt::Display for UnderlyingSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl UnderlyingSymbol {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenSymbol(pub(crate) String);

impl std::fmt::Display for TokenSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TokenSymbol {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Network(pub(crate) String);

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Network {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedAsset {
    NotAdded,
    Added {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault: Address,
        enabled: bool,
        added_at: DateTime<Utc>,
    },
}

impl Default for TokenizedAsset {
    fn default() -> Self {
        Self::NotAdded
    }
}

#[async_trait]
impl Aggregate for TokenizedAsset {
    type Command = TokenizedAssetCommand;
    type Event = TokenizedAssetEvent;
    type Error = TokenizedAssetError;
    type Services = ();

    fn aggregate_type() -> String {
        "TokenizedAsset".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TokenizedAssetCommand::Add {
                underlying,
                token,
                network,
                vault,
            } => {
                if matches!(self, Self::Added { .. }) {
                    tracing::debug!(
                        underlying = %underlying.0,
                        "Asset already added, skipping"
                    );
                    return Ok(vec![]);
                }

                let now = Utc::now();

                Ok(vec![TokenizedAssetEvent::Added {
                    underlying,
                    token,
                    network,
                    vault,
                    added_at: now,
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            TokenizedAssetEvent::Added {
                underlying,
                token,
                network,
                vault,
                added_at,
            } => {
                *self = Self::Added {
                    underlying,
                    token,
                    network,
                    vault,
                    enabled: true,
                    added_at,
                };
            }
            TokenizedAssetEvent::VaultAddressUpdated { vault, .. } => {
                todo!("Apply VaultAddressUpdated: update vault to {vault}")
            }
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum TokenizedAssetError {}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::{Aggregate, test::TestFramework};

    use super::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetEvent, UnderlyingSymbol,
    };

    type TokenizedAssetTestFramework = TestFramework<TokenizedAsset>;

    #[test]
    fn test_add_asset_creates_new_asset() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = TokenizedAssetTestFramework::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::Add {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault,
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    TokenizedAssetEvent::Added {
                        underlying: event_underlying,
                        token: event_token,
                        network: event_network,
                        vault: event_vault,
                        added_at,
                    } => {
                        assert_eq!(event_underlying, &underlying);
                        assert_eq!(event_token, &token);
                        assert_eq!(event_network, &network);
                        assert_eq!(event_vault, &vault);
                        assert!(added_at.timestamp() > 0);
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_add_asset_when_already_added_is_idempotent() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        TokenizedAssetTestFramework::with(())
            .given(vec![TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault: address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::Add {
                underlying,
                token,
                network,
                vault,
            })
            .then_expect_events(vec![]);
    }

    #[test]
    fn test_apply_asset_added_updates_state() {
        let mut asset = TokenizedAsset::default();

        assert!(matches!(asset, TokenizedAsset::NotAdded));

        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let vault = address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc");
        let added_at = chrono::Utc::now();

        asset.apply(TokenizedAssetEvent::Added {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault,
            added_at,
        });

        match asset {
            TokenizedAsset::Added {
                underlying: added_underlying,
                token: added_token,
                network: added_network,
                vault: added_vault,
                enabled,
                added_at: added_at_timestamp,
            } => {
                assert_eq!(added_underlying, underlying);
                assert_eq!(added_token, token);
                assert_eq!(added_network, network);
                assert_eq!(added_vault, vault);
                assert!(enabled);
                assert_eq!(added_at_timestamp, added_at);
            }
            TokenizedAsset::NotAdded => panic!("Expected asset to be added"),
        }
    }

    #[test]
    fn test_underlying_symbol_display() {
        let symbol = UnderlyingSymbol::new("AAPL");
        assert_eq!(format!("{symbol}"), "AAPL");
    }

    #[test]
    fn test_token_symbol_display() {
        let symbol = TokenSymbol::new("tAAPL");
        assert_eq!(format!("{symbol}"), "tAAPL");
    }

    #[test]
    fn test_network_display() {
        let network = Network::new("base");
        assert_eq!(format!("{network}"), "base");
    }
}
