pub(crate) mod api;
pub(crate) mod cli;
mod cmd;
mod event;
pub(crate) mod schedule;
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

/// Substring the vault ownership trigger's `RAISE(ABORT, ...)` messages carry
/// and `add_tokenized_asset` matches to return 422 on a concurrent claim
/// rejection. Keep this and the two RAISE sites in the migration in lockstep.
pub(crate) const VAULT_CLAIM_CONFLICT_MESSAGE: &str =
    "serves another underlying on this network";

/// Two enabled underlyings share one vault address on the same network.
///
/// Token deploys are deterministic (CREATE2), so one underlying legitimately
/// has the same vault address on every network it is deployed to. Receipt
/// inventory, vault lookup, and redemption transfer matching are all keyed by
/// `(network, vault)`, so those streams stay separate. Two underlyings behind
/// one `(network, vault)` is the real misconfiguration: a share transfer to
/// that vault cannot be attributed to one asset. Reject at boot and at add
/// time.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "vault address {vault} on {network} is configured for both {first} and \
     {second}; one vault cannot serve two underlyings on one network"
)]
pub(crate) struct VaultUnderlyingCollision {
    pub(crate) vault: Address,
    pub(crate) network: Network,
    pub(crate) first: UnderlyingSymbol,
    pub(crate) second: UnderlyingSymbol,
}

/// Rejects enabled-asset sets where one `(network, vault)` pair serves more
/// than one underlying. The same vault address on different networks is
/// allowed: deterministic deploys reuse the address across chains.
pub(crate) fn validate_one_underlying_per_network_vault(
    assets: &[TokenizedAssetView],
) -> Result<(), VaultUnderlyingCollision> {
    let mut owners: HashMap<(Network, Address), &UnderlyingSymbol> =
        HashMap::new();

    assets.iter().try_for_each(|asset| {
        match owners.insert((asset.network, asset.vault), &asset.underlying) {
            Some(first) if first != &asset.underlying => {
                Err(VaultUnderlyingCollision {
                    vault: asset.vault,
                    network: asset.network,
                    first: first.clone(),
                    second: asset.underlying.clone(),
                })
            }
            _ => Ok(()),
        }
    })
}

/// One per-network listing of an underlying: token symbol, vault address,
/// network. Corporate-action freeze status is a property of the underlying
/// equity, not of a listing, and lives on the underlying-keyed `Underlying`
/// aggregate (`crate::underlying`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TokenizedAsset {
    underlying: UnderlyingSymbol,
    token: TokenSymbol,
    network: Network,
    vault: Address,
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
    const SCHEMA_VERSION: u64 = 4;

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
                added_at: *added_at,
            }),
            // Vault updates are only reachable after an `Added` genesis —
            // they never start a stream.
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

            // A second `Added` re-adds the asset, overwriting the full listing
            // state from the authoritative event. event-sorcery turns an
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
                    network = %network,
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
        Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        TokenizedAssetEvent, TokenizedAssetView, UnderlyingSymbol,
        validate_one_underlying_per_network_vault,
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
            added_at: Utc::now(),
        }
    }

    /// Deterministic (CREATE2) deploys give one underlying the same vault
    /// address on every network, so a shared address across networks must
    /// pass validation.
    #[test]
    fn shared_vault_address_across_networks_passes_validation() {
        let shared = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let assets = vec![
            enabled_asset("RKLB", Network::Ethereum, shared),
            enabled_asset("RKLB", Network::HyperEvm, shared),
        ];

        assert!(validate_one_underlying_per_network_vault(&assets).is_ok());
    }

    /// Two underlyings behind one `(network, vault)` cannot be told apart by
    /// redemption transfer matching; that stays rejected.
    #[test]
    fn same_network_vault_serving_two_underlyings_is_rejected() {
        let shared = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let assets = vec![
            enabled_asset("AAPL", Network::Base, shared),
            enabled_asset("MSFT", Network::Base, shared),
        ];

        let error =
            validate_one_underlying_per_network_vault(&assets).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("base"), "missing network: {message}");
        assert!(
            message.contains("AAPL"),
            "missing first underlying: {message}"
        );
        assert!(
            message.contains("MSFT"),
            "missing second underlying: {message}"
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

        assert!(validate_one_underlying_per_network_vault(&assets).is_ok());
    }

    /// A live database predating this migration may hold two underlyings on one
    /// `(network, vault)` (the previous guard allowed it on one network). The
    /// ownership backfill must fail closed with an actionable message, not a
    /// bare `UNIQUE constraint failed` naming nothing.
    #[tokio::test]
    async fn vault_ownership_backfill_reports_ambiguous_listings() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("create in-memory database");

        sqlx::query(
            "
            CREATE TABLE events (
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                sequence BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                event_version TEXT NOT NULL,
                payload JSON NOT NULL,
                metadata JSON NOT NULL
            )
            ",
        )
        .execute(&pool)
        .await
        .expect("create events table");

        for underlying in ["FOO", "BAR"] {
            let payload = format!(
                r#"{{"Added":{{"underlying":"{underlying}","token":"t{underlying}","network":"base","vault":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","added_at":"2026-01-01T00:00:00Z"}}}}"#
            );
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
                    'TokenizedAsset', ?, 1,
                    'TokenizedAssetEvent::Added', '1.0', ?, '{}'
                )
                ",
            )
            .bind(format!("{underlying}:base"))
            .bind(payload)
            .execute(&pool)
            .await
            .expect("seed conflicting Added event");
        }

        let migration = include_str!(
            "../../migrations/20260825082953_enforce_tokenized_asset_vault_ownership.sql"
        );
        let error = sqlx::raw_sql(migration)
            .execute(&pool)
            .await
            .expect_err("backfill must abort on two underlyings per vault");

        assert!(
            error.to_string().contains("serves two underlyings"),
            "abort must name the conflict class, got: {error}"
        );
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

        let TokenizedAssetEvent::VaultAddressUpdated {
            vault,
            previous_vault,
            ..
        } = &events[0]
        else {
            panic!("Expected VaultAddressUpdated, got: {:?}", events[0])
        };
        assert_eq!(*vault, vault_b);
        assert_eq!(*previous_vault, vault_a);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Updating vault address for asset", "AAPL"]
        ));
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
            added_at: added_at_timestamp,
        } = asset;

        assert_eq!(added_underlying, underlying);
        assert_eq!(added_token, token);
        assert_eq!(added_network, network);
        assert_eq!(added_vault, vault);
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
    /// `:base` exactly once, already-rekeyed ids are untouched, shipped
    /// `Frozen`/`Unfrozen` events move to the underlying-keyed `Underlying`
    /// aggregate with contiguous resequencing on both streams, and the view
    /// is cleared for projection catch-up.
    #[tokio::test]
    async fn rekey_migration_rekeys_legacy_ids_and_is_idempotent() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        // A legacy stream shaped like production history: Added, then a
        // dividend freeze/unfreeze cycle, then a vault update. The freeze
        // events must split off to `Underlying` (resequenced 1..N) while the
        // listing events close ranks under the rekeyed AssetKey id.
        for (sequence, event_type, payload) in [
            (1, "TokenizedAssetEvent::Added", "{}"),
            (
                2,
                "TokenizedAssetEvent::Frozen",
                r#"{"Frozen":{"frozen_at":"2026-06-15T12:00:00Z"}}"#,
            ),
            (
                3,
                "TokenizedAssetEvent::Unfrozen",
                r#"{"Unfrozen":{"unfrozen_at":"2026-06-16T12:00:00Z"}}"#,
            ),
            (4, "TokenizedAssetEvent::VaultAddressUpdated", "{}"),
        ] {
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
                VALUES ('TokenizedAsset', 'AAPL', ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(sequence)
            .bind(event_type)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();
        }

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

            let listing_events: Vec<(String, i64, String)> = sqlx::query_as(
                "
                SELECT aggregate_id, sequence, event_type
                FROM events
                WHERE aggregate_type = 'TokenizedAsset'
                ORDER BY sequence
                ",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                listing_events,
                vec![
                    (
                        "AAPL:base".to_string(),
                        1,
                        "TokenizedAssetEvent::Added".to_string()
                    ),
                    (
                        "AAPL:base".to_string(),
                        2,
                        "TokenizedAssetEvent::VaultAddressUpdated".to_string()
                    ),
                ],
                "listing events wrong after pass {pass}"
            );

            let freeze_events: Vec<(String, i64, String, String)> =
                sqlx::query_as(
                    "
                    SELECT aggregate_id, sequence, event_type, payload
                    FROM events
                    WHERE aggregate_type = 'Underlying'
                    ORDER BY sequence
                    ",
                )
                .fetch_all(&pool)
                .await
                .unwrap();
            assert_eq!(
                freeze_events,
                vec![
                    (
                        "AAPL".to_string(),
                        1,
                        "UnderlyingEvent::Frozen".to_string(),
                        r#"{"Frozen":{"frozen_at":"2026-06-15T12:00:00Z"}}"#
                            .to_string(),
                    ),
                    (
                        "AAPL".to_string(),
                        2,
                        "UnderlyingEvent::Unfrozen".to_string(),
                        r#"{"Unfrozen":{"unfrozen_at":"2026-06-16T12:00:00Z"}}"#
                            .to_string(),
                    ),
                ],
                "freeze events wrong after pass {pass}"
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
            assert!(
                snapshot_ids.is_empty(),
                "stale TokenizedAsset snapshots must be dropped after pass {pass}"
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
