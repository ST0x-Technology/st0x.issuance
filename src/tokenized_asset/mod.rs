mod api;
mod cmd;
mod event;
mod view;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};

pub(crate) use api::list_tokenized_assets;
pub(crate) use cmd::TokenizedAssetCommand;
pub(crate) use event::TokenizedAssetEvent;
pub(crate) use view::TokenizedAssetView;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UnderlyingSymbol(pub(crate) String);

impl UnderlyingSymbol {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenSymbol(pub(crate) String);

impl TokenSymbol {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Network(pub(crate) String);

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
        vault_address: Address,
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
                vault_address,
            } => {
                if matches!(self, Self::Added { .. }) {
                    return Err(TokenizedAssetError::AssetAlreadyExists {
                        underlying: underlying.0,
                    });
                }

                let now = Utc::now();

                Ok(vec![TokenizedAssetEvent::Added {
                    underlying,
                    token,
                    network,
                    vault_address,
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
                vault_address,
                added_at,
            } => {
                *self = Self::Added {
                    underlying,
                    token,
                    network,
                    vault_address,
                    enabled: true,
                    added_at,
                };
            }
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum TokenizedAssetError {
    #[error("Asset already exists for underlying symbol: {underlying}")]
    AssetAlreadyExists { underlying: String },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::{Aggregate, test::TestFramework};

    use super::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetError, TokenizedAssetEvent, UnderlyingSymbol,
    };

    type TokenizedAssetTestFramework = TestFramework<TokenizedAsset>;

    #[test]
    fn test_add_asset_creates_new_asset() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = TokenizedAssetTestFramework::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::Add {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault_address,
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
                        vault_address: event_vault_address,
                        added_at,
                    } => {
                        assert_eq!(event_underlying, &underlying);
                        assert_eq!(event_token, &token);
                        assert_eq!(event_network, &network);
                        assert_eq!(event_vault_address, &vault_address);
                        assert!(added_at.timestamp() > 0);
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_add_asset_when_already_added_returns_error() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");

        TokenizedAssetTestFramework::with(())
            .given(vec![TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault_address: address!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ),
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::Add {
                underlying,
                token,
                network,
                vault_address,
            })
            .then_expect_error(TokenizedAssetError::AssetAlreadyExists {
                underlying: "AAPL".to_string(),
            });
    }

    #[test]
    fn test_apply_asset_added_updates_state() {
        let mut asset = TokenizedAsset::default();

        assert!(matches!(asset, TokenizedAsset::NotAdded));

        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let vault_address =
            address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc");
        let added_at = chrono::Utc::now();

        asset.apply(TokenizedAssetEvent::Added {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault_address,
            added_at,
        });

        match asset {
            TokenizedAsset::Added {
                underlying: added_underlying,
                token: added_token,
                network: added_network,
                vault_address: added_vault_address,
                enabled,
                added_at: added_at_timestamp,
            } => {
                assert_eq!(added_underlying, underlying);
                assert_eq!(added_token, token);
                assert_eq!(added_network, network);
                assert_eq!(added_vault_address, vault_address);
                assert!(enabled);
                assert_eq!(added_at_timestamp, added_at);
            }
            TokenizedAsset::NotAdded => panic!("Expected asset to be added"),
        }
    }
}
