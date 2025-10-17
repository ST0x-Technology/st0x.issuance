mod cmd;
mod endpoint;
mod event;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};

pub(crate) use cmd::TokenizedAssetCommand;
pub(crate) use endpoint::list_tokenized_assets;
pub(crate) use event::TokenizedAssetEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UnderlyingSymbol(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenSymbol(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Network(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VaultAddress(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedAsset {
    NotAdded,
    Added {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault_address: VaultAddress,
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
            TokenizedAssetCommand::AddAsset {
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

                Ok(vec![TokenizedAssetEvent::AssetAdded {
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
            TokenizedAssetEvent::AssetAdded {
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
    use super::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetError, TokenizedAssetEvent, UnderlyingSymbol,
        VaultAddress,
    };
    use cqrs_es::{Aggregate, test::TestFramework};

    type TokenizedAssetTestFramework = TestFramework<TokenizedAsset>;

    #[test]
    fn test_add_asset_creates_new_asset() {
        let underlying = UnderlyingSymbol("AAPL".to_string());
        let token = TokenSymbol("stAAPL".to_string());
        let network = Network("base".to_string());
        let vault_address = VaultAddress("0x1234567890abcdef".to_string());

        let validator = TokenizedAssetTestFramework::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::AddAsset {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault_address: vault_address.clone(),
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    TokenizedAssetEvent::AssetAdded {
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
        let underlying = UnderlyingSymbol("AAPL".to_string());
        let token = TokenSymbol("stAAPL".to_string());
        let network = Network("base".to_string());
        let vault_address = VaultAddress("0x1234567890abcdef".to_string());

        TokenizedAssetTestFramework::with(())
            .given(vec![TokenizedAssetEvent::AssetAdded {
                underlying: underlying.clone(),
                token: TokenSymbol("stAAPL".to_string()),
                network: Network("base".to_string()),
                vault_address: VaultAddress("0xabcdef".to_string()),
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::AddAsset {
                underlying: underlying.clone(),
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

        let underlying = UnderlyingSymbol("TSLA".to_string());
        let token = TokenSymbol("stTSLA".to_string());
        let network = Network("base".to_string());
        let vault_address = VaultAddress("0xfedcba".to_string());
        let added_at = chrono::Utc::now();

        asset.apply(TokenizedAssetEvent::AssetAdded {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault_address: vault_address.clone(),
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
