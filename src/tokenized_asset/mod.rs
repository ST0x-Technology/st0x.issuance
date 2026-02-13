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
            } => match self {
                Self::Added { vault: current_vault, .. }
                    if *current_vault == vault =>
                {
                    tracing::debug!(
                        underlying = %underlying.0,
                        "Asset already added with same vault, skipping"
                    );
                    Ok(vec![])
                }
                Self::Added { vault: current_vault, .. } => {
                    tracing::info!(
                        underlying = %underlying.0,
                        previous_vault = %current_vault,
                        new_vault = %vault,
                        "Updating vault address for asset"
                    );
                    Ok(vec![TokenizedAssetEvent::VaultAddressUpdated {
                        vault,
                        previous_vault: *current_vault,
                        updated_at: Utc::now(),
                    }])
                }
                Self::NotAdded => {
                    tracing::info!(
                        underlying = %underlying.0,
                        vault = %vault,
                        "Adding new tokenized asset"
                    );
                    Ok(vec![TokenizedAssetEvent::Added {
                        underlying,
                        token,
                        network,
                        vault,
                        added_at: Utc::now(),
                    }])
                }
            },
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
            TokenizedAssetEvent::VaultAddressUpdated {
                vault: new_vault,
                ..
            } => {
                if let Self::Added { vault, .. } = self {
                    *vault = new_vault;
                }
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
    use tracing_test::traced_test;

    use super::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetEvent, UnderlyingSymbol,
    };
    use crate::test_utils::logs_contain_at;

    type TokenizedAssetTestFramework = TestFramework<TokenizedAsset>;

    #[traced_test]
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

                let TokenizedAssetEvent::Added {
                    underlying: event_underlying,
                    token: event_token,
                    network: event_network,
                    vault: event_vault,
                    added_at,
                } = &events[0]
                else {
                    panic!("Expected Added event, got: {:?}", events[0])
                };

                assert_eq!(event_underlying, &underlying);
                assert_eq!(event_token, &token);
                assert_eq!(event_network, &network);
                assert_eq!(event_vault, &vault);
                assert!(added_at.timestamp() > 0);
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Adding new tokenized asset", "AAPL"]
        ));
    }

    #[traced_test]
    #[test]
    fn test_add_asset_when_already_added_with_same_vault_is_idempotent() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        TokenizedAssetTestFramework::with(())
            .given(vec![TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault,
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::Add {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault,
            })
            .then_expect_events(vec![]);

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Asset already added with same vault, skipping"]
        ));
    }

    #[traced_test]
    #[test]
    fn test_add_asset_with_different_vault_emits_vault_updated() {
        let vault_a = address!("0x1234567890abcdef1234567890abcdef12345678");
        let vault_b = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let validator = TokenizedAssetTestFramework::with(())
            .given(vec![TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault: vault_a,
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::Add {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault: vault_b,
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1, "Expected exactly one event");

                match &events[0] {
                    TokenizedAssetEvent::VaultAddressUpdated {
                        vault,
                        previous_vault,
                        ..
                    } => {
                        assert_eq!(*vault, vault_b);
                        assert_eq!(*previous_vault, vault_a);
                    }
                    other @ TokenizedAssetEvent::Added { .. } => {
                        panic!("Expected VaultAddressUpdated, got: {other:?}")
                    }
                }
            }
            Err(err) => panic!("Expected success, got error: {err}"),
        }

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Updating vault address for asset", "AAPL"]
        ));
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
