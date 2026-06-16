mod api;
mod cmd;
mod event;
pub(crate) mod view;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EventSourced, Never, Table};
use serde::{Deserialize, Serialize};

pub(crate) use api::{
    add_tokenized_asset, get_tokenized_asset, list_tokenized_assets,
};
pub(crate) use cmd::TokenizedAssetCommand;
pub(crate) use event::TokenizedAssetEvent;
pub(crate) use view::TokenizedAssetView;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnderlyingSymbol(pub(crate) String);

impl std::fmt::Display for UnderlyingSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for UnderlyingSymbol {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
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
pub(crate) struct TokenizedAsset {
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    vault: Address,
    enabled: bool,
    added_at: DateTime<Utc>,
}

#[async_trait]
impl EventSourced for TokenizedAsset {
    type Id = UnderlyingSymbol;
    type Event = TokenizedAssetEvent;
    type Command = TokenizedAssetCommand;
    type Error = Never;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "TokenizedAsset";
    const PROJECTION: Table = Table("tokenized_asset_view");
    const SCHEMA_VERSION: u64 = 1;

    // Snapshots are disabled: the pre-migration wiring never wrote snapshots,
    // and event-sorcery hardwires snapshot-every-N with no off switch, so
    // usize::MAX makes the next-snapshot threshold unreachable. The proper
    // fix is for event-sorcery to take the snapshot policy explicitly from
    // the consumer, including the option to disable snapshotting entirely.
    const SNAPSHOT_SIZE: usize = usize::MAX;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            TokenizedAssetEvent::Added {
                underlying,
                token,
                network,
                vault,
                added_at,
            } => Some(Self {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault: *vault,
                enabled: true,
                added_at: *added_at,
            }),
            TokenizedAssetEvent::VaultAddressUpdated { .. } => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        match event {
            TokenizedAssetEvent::VaultAddressUpdated { vault, .. } => {
                Ok(Some(Self { vault: *vault, ..entity.clone() }))
            }

            // A second `Added` re-adds the asset, overwriting in place — the
            // pre-migration `apply` did this silently. event-sorcery turns an
            // unhandled event (`Ok(None)`) into a permanent `Failed` lifecycle,
            // which startup `catch_up` would hit for any stream carrying a
            // duplicate `Added` (only reachable via direct event-store seeding),
            // so handle it explicitly rather than bricking the aggregate.
            TokenizedAssetEvent::Added { .. } => Ok(Self::originate(event)),
        }
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let TokenizedAssetCommand::Add { underlying, token, network, vault } =
            command;

        tracing::info!(target: "asset", underlying = %underlying.0,
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

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let TokenizedAssetCommand::Add { underlying, vault, .. } = command;

        if self.vault == vault {
            tracing::debug!(target: "asset", underlying = %underlying.0,
                "Asset already added with same vault, skipping"
            );
            return Ok(vec![]);
        }

        tracing::info!(target: "asset", underlying = %underlying.0,
            previous_vault = %self.vault,
            new_vault = %vault,
            "Updating vault address for asset"
        );
        Ok(vec![TokenizedAssetEvent::VaultAddressUpdated {
            vault,
            previous_vault: self.vault,
            updated_at: Utc::now(),
        }])
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use event_sorcery::{TestHarness, replay};
    use tracing_test::traced_test;

    use super::{
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetEvent, UnderlyingSymbol,
    };
    use crate::test_utils::logs_contain_at;

    #[traced_test]
    #[tokio::test]
    async fn test_add_asset_creates_new_asset() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<TokenizedAsset>::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::Add {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault,
            })
            .await
            .events();

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

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Adding new tokenized asset", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_add_asset_when_already_added_with_same_vault_is_idempotent() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        TestHarness::<TokenizedAsset>::with(())
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
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Asset already added with same vault, skipping"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_add_asset_with_different_vault_emits_vault_updated() {
        let vault_a = address!("0x1234567890abcdef1234567890abcdef12345678");
        let vault_b = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let events = TestHarness::<TokenizedAsset>::with(())
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
            })
            .await
            .events();

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

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Updating vault address for asset", "AAPL"]
        ));
    }

    #[test]
    fn test_apply_asset_added_updates_state() {
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let vault = address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc");
        let added_at = chrono::Utc::now();

        let asset =
            replay::<TokenizedAsset>(vec![TokenizedAssetEvent::Added {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault,
                added_at,
            }])
            .unwrap()
            .unwrap();

        let TokenizedAsset {
            underlying: added_underlying,
            token: added_token,
            network: added_network,
            vault: added_vault,
            enabled,
            added_at: added_at_timestamp,
        } = asset;

        assert_eq!(added_underlying, underlying);
        assert_eq!(added_token, token);
        assert_eq!(added_network, network);
        assert_eq!(added_vault, vault);
        assert!(enabled);
        assert_eq!(added_at_timestamp, added_at);
    }

    #[test]
    fn test_apply_vault_address_updated_changes_vault() {
        let vault_a = address!("0x1234567890abcdef1234567890abcdef12345678");
        let vault_b = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asset = replay::<TokenizedAsset>(vec![
            TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault: vault_a,
                added_at: chrono::Utc::now(),
            },
            TokenizedAssetEvent::VaultAddressUpdated {
                vault: vault_b,
                previous_vault: vault_a,
                updated_at: chrono::Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        let TokenizedAsset { vault, .. } = asset;
        assert_eq!(vault, vault_b);
    }

    // A duplicate `Added` mid-stream (only reachable via direct event-store
    // seeding) must overwrite state like the pre-migration `apply` did — not
    // fall through to `Ok(None)`, which event-sorcery escalates to a permanent
    // `Failed` lifecycle that startup `catch_up` would then hit.
    #[test]
    fn test_apply_duplicate_added_overwrites_state() {
        let vault_a = address!("0x1234567890abcdef1234567890abcdef12345678");
        let vault_b = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asset = replay::<TokenizedAsset>(vec![
            TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault: vault_a,
                added_at: chrono::Utc::now(),
            },
            TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL2"),
                network: Network::new("base"),
                vault: vault_b,
                added_at: chrono::Utc::now(),
            },
        ])
        .expect("duplicate Added must not fail the lifecycle")
        .expect("aggregate must stay live after a duplicate Added");

        assert_eq!(asset.vault, vault_b);
        assert_eq!(asset.token, TokenSymbol::new("tAAPL2"));
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
