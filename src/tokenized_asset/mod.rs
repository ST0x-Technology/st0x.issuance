pub(crate) mod api;
pub(crate) mod cli;
mod cmd;
mod event;
pub(crate) mod view;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EventSourced, Never, Table};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) use api::{
    add_tokenized_asset, get_tokenized_asset, get_tokenized_asset_status,
    list_tokenized_assets,
};
pub(crate) use cmd::TokenizedAssetCommand;
pub(crate) use event::TokenizedAssetEvent;
pub(crate) use view::TokenizedAssetView;

// The asset wire newtypes are defined once in the shared `st0x-issuance-dto`
// crate so the API DTOs, Rust clients, and the TypeScript dashboard all share a
// single definition.
pub(crate) use st0x_issuance_dto::AssetKey;
pub(crate) use st0x_issuance_dto::{Network, TokenSymbol};
pub use st0x_issuance_dto::{TokenizedAssetStatus, UnderlyingSymbol};

/// Whether an asset accepts new mints.
///
/// `Frozen` gates *only* new minting — a frozen asset stays supported and in the
/// `list_enabled_assets()` set so in-flight redemptions still detect and
/// complete. This is orthogonal to listing; see the freeze invariant in
/// SPEC.md. Serializes to the bare strings `"Enabled"` / `"Frozen"`, which the
/// view queries match against `$.Live.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AssetStatus {
    Enabled,
    Frozen,
}

impl AssetStatus {
    /// Whether new mints are currently rejected for this asset.
    pub(crate) const fn is_frozen(self) -> bool {
        matches!(self, Self::Frozen)
    }
}

/// Maps the domain freeze state onto its wire representation. The two enums are
/// kept distinct (domain vs API contract) but always move in lock-step.
impl From<AssetStatus> for TokenizedAssetStatus {
    fn from(status: AssetStatus) -> Self {
        match status {
            AssetStatus::Enabled => Self::Enabled,
            AssetStatus::Frozen => Self::Frozen,
        }
    }
}

/// Two enabled assets on different networks share one vault address.
///
/// Receipt inventory is keyed by `(chain_id, vault)`, so the streams no longer
/// merge, but a shared address across networks is still a misconfiguration:
/// transfer matching, admin tooling, and operator mental models assume a vault
/// address identifies one deployment. Reject at boot and at add time.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "vault address {vault} is configured on both {first} and {second}; \
     one address cannot serve two networks"
)]
pub(crate) struct CrossNetworkVaultCollision {
    pub(crate) vault: Address,
    pub(crate) first: Network,
    pub(crate) second: Network,
}

/// Rejects enabled-asset sets where the same vault address appears on more
/// than one network.
pub(crate) fn validate_no_cross_network_vault_collisions(
    assets: &[TokenizedAssetView],
) -> Result<(), CrossNetworkVaultCollision> {
    let mut vault_networks: HashMap<Address, Network> = HashMap::new();

    assets.iter().try_for_each(|asset| {
        match vault_networks.insert(asset.vault, asset.network) {
            Some(first) if first != asset.network => {
                Err(CrossNetworkVaultCollision {
                    vault: asset.vault,
                    first,
                    second: asset.network,
                })
            }
            _ => Ok(()),
        }
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TokenizedAsset {
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    vault: Address,
    status: AssetStatus,
    added_at: DateTime<Utc>,
}

#[async_trait]
impl EventSourced for TokenizedAsset {
    type Id = AssetKey;
    type Event = TokenizedAssetEvent;
    type Command = TokenizedAssetCommand;
    type Error = Never;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "TokenizedAsset";
    const PROJECTION: Table = Table("tokenized_asset_view");
    const SCHEMA_VERSION: u64 = 3;

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
                network: *network,
                vault: *vault,
                status: AssetStatus::Enabled,
                added_at: *added_at,
            }),
            // Vault updates and freeze/unfreeze are only reachable after an
            // `Added` genesis — they never start a stream.
            TokenizedAssetEvent::VaultAddressUpdated { .. }
            | TokenizedAssetEvent::Frozen { .. }
            | TokenizedAssetEvent::Unfrozen { .. } => None,
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

            TokenizedAssetEvent::Frozen { .. } => {
                Ok(Some(Self { status: AssetStatus::Frozen, ..entity.clone() }))
            }

            TokenizedAssetEvent::Unfrozen { .. } => Ok(Some(Self {
                status: AssetStatus::Enabled,
                ..entity.clone()
            })),

            // A second `Added` re-adds the asset, overwriting identity and vault
            // in place but preserving the current freeze `status` — re-adding
            // must not silently clear a freeze, since there would be no
            // `Unfrozen` event to explain the transition. event-sorcery turns an
            // unhandled event (`Ok(None)`) into a permanent `Failed` lifecycle,
            // which startup `catch_up` would hit for any stream carrying a
            // duplicate `Added` (only reachable via direct event-store seeding),
            // so handle it explicitly rather than bricking the aggregate.
            TokenizedAssetEvent::Added {
                underlying,
                token,
                network,
                vault,
                added_at,
            } => Ok(Some(Self {
                underlying: underlying.clone(),
                token: token.clone(),
                network: *network,
                vault: *vault,
                added_at: *added_at,
                // Preserve only the freeze `status`; everything else comes from
                // the authoritative `Added` event. Naming the single carried-over
                // field keeps the no-silent-unfreeze invariant local, and stops a
                // future field from being silently carried from stale state.
                status: entity.status,
            })),
        }
    }

    async fn initialize(
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
                tracing::info!(target: "asset", underlying = %underlying,
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

            // Freezing or unfreezing an asset that was never added is a no-op:
            // there is nothing to gate. Callers (the issuer CLI) check existence
            // first and report "not found" rather than silently dispatching.
            TokenizedAssetCommand::Freeze | TokenizedAssetCommand::Unfreeze => {
                tracing::warn!(target: "asset",
                    "Ignoring freeze/unfreeze for an asset that does not exist"
                );
                Ok(vec![])
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TokenizedAssetCommand::Add { underlying, vault, .. } => {
                if self.vault == vault {
                    tracing::debug!(target: "asset", underlying = %underlying,
                        "Asset already added with same vault, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset", underlying = %underlying,
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

            TokenizedAssetCommand::Freeze => {
                if self.status == AssetStatus::Frozen {
                    tracing::debug!(target: "asset",
                        underlying = %self.underlying,
                        "Asset already frozen, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset",
                    underlying = %self.underlying,
                    "Freezing tokenized asset"
                );
                Ok(vec![TokenizedAssetEvent::Frozen { frozen_at: Utc::now() }])
            }

            TokenizedAssetCommand::Unfreeze => {
                if self.status == AssetStatus::Enabled {
                    tracing::debug!(target: "asset",
                        underlying = %self.underlying,
                        "Asset already enabled, skipping"
                    );
                    return Ok(vec![]);
                }

                tracing::info!(target: "asset",
                    underlying = %self.underlying,
                    "Unfreezing tokenized asset"
                );
                Ok(vec![TokenizedAssetEvent::Unfrozen {
                    unfrozen_at: Utc::now(),
                }])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use chrono::Utc;
    use event_sorcery::{StoreBuilder, TestHarness, replay};
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing_test::traced_test;

    use super::{
        AssetStatus, Network, TokenSymbol, TokenizedAsset,
        TokenizedAssetCommand, TokenizedAssetEvent, TokenizedAssetView,
        UnderlyingSymbol, validate_no_cross_network_vault_collisions,
    };
    use crate::prepare_event_sourced_startup;
    use crate::test_utils::logs_contain_at;

    fn enabled_asset(
        underlying: &str,
        network: Network,
        vault: Address,
    ) -> TokenizedAssetView {
        TokenizedAssetView {
            underlying: UnderlyingSymbol::new(underlying).unwrap(),
            token: TokenSymbol::new(format!("t{underlying}")),
            network,
            vault,
            status: AssetStatus::Enabled,
            added_at: Utc::now(),
        }
    }

    #[test]
    fn cross_network_vault_address_collision_is_rejected() {
        let shared = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let assets = vec![
            enabled_asset("AAPL", Network::Base, shared),
            enabled_asset("MSFT", Network::Ethereum, shared),
        ];

        let error =
            validate_no_cross_network_vault_collisions(&assets).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("base"), "missing first network: {message}");
        assert!(
            message.contains("ethereum"),
            "missing second network: {message}"
        );
    }

    #[test]
    fn distinct_vault_addresses_across_networks_pass_validation() {
        let assets = vec![
            enabled_asset(
                "AAPL",
                Network::Base,
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ),
            enabled_asset(
                "AAPL",
                Network::Ethereum,
                address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            ),
        ];

        assert!(validate_no_cross_network_vault_collisions(&assets).is_ok());
    }

    #[traced_test]
    #[tokio::test]
    async fn test_add_asset_creates_new_asset() {
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<TokenizedAsset>::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::Add {
                underlying: underlying.clone(),
                token: token.clone(),
                network,
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
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                vault,
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::Add {
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
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
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                vault: vault_a,
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::Add {
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
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
            other => {
                panic!("Expected VaultAddressUpdated, got: {other:?}")
            }
        }

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Updating vault address for asset", "AAPL"]
        ));
    }

    fn added_event(vault: Address) -> TokenizedAssetEvent {
        TokenizedAssetEvent::Added {
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            vault,
            added_at: chrono::Utc::now(),
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_freeze_enabled_asset_emits_frozen() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<TokenizedAsset>::with(())
            .given(vec![added_event(vault)])
            .when(TokenizedAssetCommand::Freeze)
            .await
            .events();

        assert_eq!(events.len(), 1, "Expected exactly one event");

        let TokenizedAssetEvent::Frozen { frozen_at } = &events[0] else {
            panic!("Expected Frozen event, got: {:?}", events[0])
        };
        assert!(frozen_at.timestamp() > 0);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing tokenized asset", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_freeze_already_frozen_is_idempotent() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        TestHarness::<TokenizedAsset>::with(())
            .given(vec![
                added_event(vault),
                TokenizedAssetEvent::Frozen { frozen_at: chrono::Utc::now() },
            ])
            .when(TokenizedAssetCommand::Freeze)
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Asset already frozen, skipping", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unfreeze_frozen_asset_emits_unfrozen() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<TokenizedAsset>::with(())
            .given(vec![
                added_event(vault),
                TokenizedAssetEvent::Frozen { frozen_at: chrono::Utc::now() },
            ])
            .when(TokenizedAssetCommand::Unfreeze)
            .await
            .events();

        assert_eq!(events.len(), 1, "Expected exactly one event");

        let TokenizedAssetEvent::Unfrozen { unfrozen_at } = &events[0] else {
            panic!("Expected Unfrozen event, got: {:?}", events[0])
        };
        assert!(unfrozen_at.timestamp() > 0);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Unfreezing tokenized asset", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unfreeze_enabled_asset_is_idempotent() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        TestHarness::<TokenizedAsset>::with(())
            .given(vec![added_event(vault)])
            .when(TokenizedAssetCommand::Unfreeze)
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Asset already enabled, skipping", "AAPL"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_freeze_nonexistent_asset_is_noop() {
        TestHarness::<TokenizedAsset>::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::Freeze)
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Ignoring freeze/unfreeze for an asset that does not exist"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unfreeze_nonexistent_asset_is_noop() {
        TestHarness::<TokenizedAsset>::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::Unfreeze)
            .await
            .then_expect_events(&[]);

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Ignoring freeze/unfreeze for an asset that does not exist"]
        ));
    }

    #[test]
    fn test_apply_frozen_sets_status_frozen() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset = replay::<TokenizedAsset>(vec![
            added_event(vault),
            TokenizedAssetEvent::Frozen { frozen_at: chrono::Utc::now() },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(asset.status, AssetStatus::Frozen);
    }

    #[test]
    fn test_apply_unfrozen_sets_status_enabled() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset = replay::<TokenizedAsset>(vec![
            added_event(vault),
            TokenizedAssetEvent::Frozen { frozen_at: chrono::Utc::now() },
            TokenizedAssetEvent::Unfrozen { unfrozen_at: chrono::Utc::now() },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(asset.status, AssetStatus::Enabled);
    }

    #[test]
    fn test_apply_asset_added_updates_state() {
        let underlying = UnderlyingSymbol::new("TSLA").unwrap();
        let token = TokenSymbol::new("tTSLA");
        let network = Network::Base;
        let vault = address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc");
        let added_at = chrono::Utc::now();

        let asset =
            replay::<TokenizedAsset>(vec![TokenizedAssetEvent::Added {
                underlying: underlying.clone(),
                token: token.clone(),
                network,
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
            status,
            added_at: added_at_timestamp,
        } = asset;

        assert_eq!(added_underlying, underlying);
        assert_eq!(added_token, token);
        assert_eq!(added_network, network);
        assert_eq!(added_vault, vault);
        assert_eq!(status, AssetStatus::Enabled);
        assert_eq!(added_at_timestamp, added_at);
    }

    #[test]
    fn test_apply_vault_address_updated_changes_vault() {
        let vault_a = address!("0x1234567890abcdef1234567890abcdef12345678");
        let vault_b = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asset = replay::<TokenizedAsset>(vec![
            TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
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
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                vault: vault_a,
                added_at: chrono::Utc::now(),
            },
            TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL2"),
                network: Network::Base,
                vault: vault_b,
                added_at: chrono::Utc::now(),
            },
        ])
        .expect("duplicate Added must not fail the lifecycle")
        .expect("aggregate must stay live after a duplicate Added");

        assert_eq!(asset.vault, vault_b);
        assert_eq!(asset.token, TokenSymbol::new("tAAPL2"));
    }

    // A re-`Added` on a frozen asset must overwrite identity/vault but keep the
    // freeze `status` — there is no `Unfrozen` event to explain a transition back
    // to `Enabled`, so silently clearing the freeze would resume minting with no
    // audit trail.
    #[test]
    fn test_apply_duplicate_added_preserves_freeze_status() {
        let vault_a = address!("0x1234567890abcdef1234567890abcdef12345678");
        let vault_b = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let asset = replay::<TokenizedAsset>(vec![
            TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                vault: vault_a,
                added_at: chrono::Utc::now(),
            },
            TokenizedAssetEvent::Frozen { frozen_at: chrono::Utc::now() },
            TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                token: TokenSymbol::new("tAAPL2"),
                network: Network::Base,
                vault: vault_b,
                added_at: chrono::Utc::now(),
            },
        ])
        .expect("duplicate Added must not fail the lifecycle")
        .expect("aggregate must stay live after a duplicate Added");

        assert_eq!(asset.vault, vault_b);
        assert_eq!(asset.status, AssetStatus::Frozen);
    }

    #[test]
    fn test_underlying_symbol_display() {
        let symbol = UnderlyingSymbol::new("AAPL").unwrap();
        assert_eq!(format!("{symbol}"), "AAPL");
    }

    #[test]
    fn test_token_symbol_display() {
        let symbol = TokenSymbol::new("tAAPL");
        assert_eq!(format!("{symbol}"), "tAAPL");
    }

    #[test]
    fn test_network_display() {
        let network = Network::Base;
        assert_eq!(format!("{network}"), "base");
    }

    /// Executes the real migration file (via `include_str!`) so the test
    /// cannot drift from what production runs. Running the sequence twice
    /// proves both the rekey and its idempotency: legacy bare-ticker ids gain
    /// `:base` exactly once, already-rekeyed ids are untouched, and the view
    /// is cleared for projection catch-up.
    #[tokio::test]
    async fn rekey_migration_rekeys_legacy_ids_and_is_idempotent() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'TokenizedAsset',
                'AAPL',
                1,
                'TokenizedAssetEvent::Added',
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO snapshots (
                aggregate_type,
                aggregate_id,
                last_sequence,
                payload,
                timestamp
            )
            VALUES ('TokenizedAsset', 'AAPL', 1, '{}', 1)
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{}')
            ",
        )
        .execute(&pool)
        .await
        .unwrap();

        const REKEY_MIGRATION: &str = include_str!(
            "../../migrations/20260703235904_rekey_tokenized_asset_aggregate_id.sql"
        );

        for pass in 1..=2 {
            sqlx::raw_sql(REKEY_MIGRATION).execute(&pool).await.unwrap();

            let event_ids: Vec<(String,)> = sqlx::query_as(
                "
                SELECT aggregate_id
                FROM events
                WHERE aggregate_type = 'TokenizedAsset'
                ",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                event_ids,
                vec![("AAPL:base".to_string(),)],
                "event aggregate_id wrong after pass {pass}"
            );

            let snapshot_ids: Vec<(String,)> = sqlx::query_as(
                "
                SELECT aggregate_id
                FROM snapshots
                WHERE aggregate_type = 'TokenizedAsset'
                ",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                snapshot_ids,
                vec![("AAPL:base".to_string(),)],
                "snapshot aggregate_id wrong after pass {pass}"
            );

            let view_rows: Vec<(String,)> =
                sqlx::query_as("SELECT view_id FROM tokenized_asset_view")
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            assert!(
                view_rows.is_empty(),
                "view must be cleared after pass {pass}"
            );
        }
    }

    /// Precondition guard: unexpected aggregate ids must abort before any row
    /// is rekeyed. Each case seeds one violating id plus a legacy `AAPL` row;
    /// the migration must fail and leave both ids untouched.
    #[tokio::test]
    async fn rekey_migration_aborts_on_unexpected_aggregate_ids() {
        const REKEY_MIGRATION: &str = include_str!(
            "../../migrations/20260703235904_rekey_tokenized_asset_aggregate_id.sql"
        );

        for unexpected_id in ["", "AAPL:BASE", "NVDA:ethereum", ":base"] {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .unwrap();

            sqlx::migrate!().run(&pool).await.unwrap();

            for aggregate_id in ["AAPL", unexpected_id] {
                sqlx::query(
                    "
                    INSERT INTO events (
                        aggregate_type,
                        aggregate_id,
                        sequence,
                        event_type,
                        event_version,
                        payload,
                        metadata
                    )
                    VALUES (
                        'TokenizedAsset',
                        ?,
                        1,
                        'TokenizedAssetEvent::Added',
                        '1.0',
                        '{}',
                        '{}'
                    )
                    ",
                )
                .bind(aggregate_id)
                .execute(&pool)
                .await
                .unwrap();
            }

            let migration_result =
                sqlx::raw_sql(REKEY_MIGRATION).execute(&pool).await;
            assert!(
                migration_result.is_err(),
                "migration must abort when unexpected id {unexpected_id:?} is present"
            );

            let mut event_ids: Vec<String> = sqlx::query_as::<_, (String,)>(
                "
                SELECT aggregate_id
                FROM events
                WHERE aggregate_type = 'TokenizedAsset'
                ",
            )
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(aggregate_id,)| aggregate_id)
            .collect();
            event_ids.sort();
            let mut expected_ids =
                vec!["AAPL".to_string(), unexpected_id.to_string()];
            expected_ids.sort();
            assert_eq!(
                event_ids, expected_ids,
                "ids must be unchanged after aborted migration for {unexpected_id:?}"
            );
        }
    }

    /// Regression: pre-event-sorcery snapshot and `tokenized_asset_view` payloads
    /// must be cleared before `StoreBuilder::build` projection catch-up.
    #[tokio::test]
    async fn pre_lifecycle_snapshot_and_view_cleared_before_store_build() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let underlying = "AAPL";
        let aggregate_id = "AAPL:base";
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let now = Utc::now();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'SchemaRegistry',
                'schema',
                1,
                'SchemaRegistryEvent::VersionUpdated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(
            serde_json::json!({
                "VersionUpdated": { "name": "TokenizedAsset", "version": 1 }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'TokenizedAsset',
                ?,
                1,
                'TokenizedAssetEvent::Added',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(aggregate_id)
        .bind(
            serde_json::json!({
                "Added": {
                    "underlying": underlying,
                    "token": "tAAPL",
                    "network": "base",
                    "vault": vault,
                    "added_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        let stale = serde_json::json!({
            "underlying": underlying,
            "token": "tAAPL",
            "network": "base",
            "vault": vault,
            "status": "Enabled",
            "added_at": now,
        });

        sqlx::query(
            "
            INSERT INTO snapshots (
                aggregate_type,
                aggregate_id,
                last_sequence,
                snapshot_version,
                payload,
                timestamp
            )
            VALUES (
                'TokenizedAsset',
                ?,
                1,
                0,
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ",
        )
        .bind(aggregate_id)
        .bind(stale.to_string())
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
        )
        .bind(aggregate_id)
        .bind(stale.to_string())
        .execute(&pool)
        .await
        .unwrap();

        prepare_event_sourced_startup::<TokenizedAsset>(&pool).await.unwrap();
        StoreBuilder::<TokenizedAsset>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let stale_snapshot_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM snapshots
            WHERE aggregate_type = 'TokenizedAsset'
              AND aggregate_id = ?
            ",
        )
        .bind(aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            stale_snapshot_count, 0,
            "Startup must clear incompatible TokenizedAsset snapshots"
        );

        let view_payload: String = sqlx::query_scalar(
            "SELECT payload FROM tokenized_asset_view WHERE view_id = ?",
        )
        .bind(aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let payload: serde_json::Value =
            serde_json::from_str(&view_payload).unwrap();
        assert!(
            payload
                .get("Live")
                .and_then(|live| live.get("underlying"))
                .is_some(),
            "Projection catch-up must rebuild tokenized_asset_view with Lifecycle payload, got {payload}"
        );
    }
}
