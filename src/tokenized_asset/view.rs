use alloy::primitives::Address;
use alloy::providers::DynProvider;
use chrono::{DateTime, Utc};
use cqrs_es::persist::{GenericQuery, QueryReplay, ViewRepository};
use cqrs_es::{EventEnvelope, View};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::info;

use super::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetError,
    TokenizedAssetEvent, UnderlyingSymbol, VaultCtx,
};
use crate::bindings::OffchainAssetReceiptVault;

pub(crate) type TokenizedAssetViewRepo =
    Arc<SqliteViewRepository<TokenizedAssetView, TokenizedAsset>>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizedAssetViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error("Persistence error: {0}")]
    Persistence(#[from] cqrs_es::persist::PersistenceError),
    #[error("Aggregate error: {0}")]
    Aggregate(#[from] cqrs_es::AggregateError<TokenizedAssetError>),
}

/// Replays all `TokenizedAsset` events through the `tokenized_asset_view`.
///
/// Uses `QueryReplay` to re-project the view from existing events in the event store.
/// This is used at startup to ensure the view is in sync with events after manual
/// event store modifications or schema changes.
pub(crate) async fn replay_tokenized_asset_view(
    pool: Pool<Sqlite>,
) -> Result<(), TokenizedAssetViewError> {
    info!("Replaying tokenized asset view from events");

    let view_repo = Arc::new(SqliteViewRepository::<
        TokenizedAssetView,
        TokenizedAsset,
    >::new(
        pool.clone(),
        "tokenized_asset_view".to_string(),
    ));
    let query = GenericQuery::new(view_repo);

    let event_repo = SqliteEventRepository::new(pool);
    let replay = QueryReplay::new(event_repo, query);
    replay.replay_all().await?;

    info!("Tokenized asset view replay complete");

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedAssetView {
    Unavailable,
    Asset {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault: Address,
        enabled: bool,
        added_at: DateTime<Utc>,
    },
}

impl Default for TokenizedAssetView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl View<TokenizedAsset> for TokenizedAssetView {
    fn update(&mut self, event: &EventEnvelope<TokenizedAsset>) {
        match &event.payload {
            TokenizedAssetEvent::Added {
                underlying,
                token,
                network,
                vault,
                added_at,
            } => {
                *self = Self::Asset {
                    underlying: underlying.clone(),
                    token: token.clone(),
                    network: network.clone(),
                    vault: *vault,
                    enabled: true,
                    added_at: *added_at,
                };
            }
            TokenizedAssetEvent::VaultAddressUpdated {
                vault: new_vault,
                ..
            } => {
                if let Self::Asset { vault, .. } = self {
                    *vault = *new_vault;
                }
            }
        }
    }
}

pub(crate) async fn list_enabled_assets(
    pool: &Pool<Sqlite>,
) -> Result<Vec<TokenizedAssetView>, TokenizedAssetViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM tokenized_asset_view
        WHERE json_extract(payload, '$.Asset.enabled') = 1
        "#
    )
    .fetch_all(pool)
    .await?;

    let views: Vec<TokenizedAssetView> = rows
        .into_iter()
        .map(|row| serde_json::from_str(&row.payload))
        .collect::<Result<_, _>>()?;

    Ok(views)
}

/// Loads the full tokenized asset view for a given underlying symbol.
///
/// Returns `Ok(Some(view))` if the asset exists, `Ok(None)` if not found,
/// or an error on database failure.
pub(crate) async fn load_asset_by_underlying(
    repo: &TokenizedAssetViewRepo,
    underlying: &UnderlyingSymbol,
) -> Result<Option<TokenizedAssetView>, TokenizedAssetViewError> {
    Ok(repo.load(&underlying.0).await?)
}

/// Finds the vault address for a given underlying symbol.
///
/// Returns `Ok(Some(vault))` if an enabled asset with that underlying exists,
/// `Ok(None)` if not found or disabled, or an error on database failure.
pub(crate) async fn find_vault_by_underlying(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<Option<Address>, TokenizedAssetViewError> {
    let repo = SqliteViewRepository::<TokenizedAssetView, TokenizedAsset>::new(
        pool.clone(),
        "tokenized_asset_view".to_string(),
    );

    let view = repo.load(&underlying.0).await?;

    match view {
        Some(TokenizedAssetView::Asset { vault, enabled: true, .. }) => {
            Ok(Some(vault))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultDiscoveryError {
    #[error(transparent)]
    View(#[from] TokenizedAssetViewError),
    #[error(transparent)]
    Contract(#[from] alloy::contract::Error),
}

pub(crate) async fn discover_vaults(
    pool: &Pool<Sqlite>,
    provider: &DynProvider,
) -> Result<Vec<VaultCtx>, VaultDiscoveryError> {
    let assets = list_enabled_assets(pool).await?;

    let vaults: Vec<_> = assets
        .into_iter()
        .filter_map(|asset| match asset {
            TokenizedAssetView::Asset { underlying, vault, .. } => {
                Some((underlying, vault))
            }
            TokenizedAssetView::Unavailable => None,
        })
        .collect();

    if vaults.is_empty() {
        info!("No enabled vaults found");
        return Ok(vec![]);
    }

    let configs: Vec<VaultCtx> = stream::iter(vaults)
        .then(|(underlying, vault)| async move {
            let vault_contract =
                OffchainAssetReceiptVault::new(vault, provider);
            let receipt_contract =
                Address::from(vault_contract.receipt().call().await?.0);

            Ok::<_, VaultDiscoveryError>(VaultCtx {
                underlying,
                vault,
                receipt_contract,
            })
        })
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    info!(vault_count = configs.len(), "Discovered vaults");

    Ok(configs)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::EventEnvelope;
    use cqrs_es::persist::GenericQuery;
    use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};

    struct TestHarness {
        pool: Pool<Sqlite>,
        cqrs: SqliteCqrs<TokenizedAsset>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let view_repo = Arc::new(SqliteViewRepository::<
                TokenizedAssetView,
                TokenizedAsset,
            >::new(
                pool.clone(),
                "tokenized_asset_view".to_string(),
            ));
            let query = GenericQuery::new(view_repo);
            let cqrs = sqlite_cqrs(pool.clone(), vec![Box::new(query)], ());

            Self { pool, cqrs }
        }

        async fn add_asset(&self, underlying: &str, vault: Address) {
            self.cqrs
                .execute(
                    underlying,
                    TokenizedAssetCommand::Add {
                        underlying: UnderlyingSymbol::new(underlying),
                        token: TokenSymbol::new(format!("t{underlying}")),
                        network: Network::new("base"),
                        vault,
                    },
                )
                .await
                .expect("Failed to add asset");
        }
    }

    #[test]
    fn test_view_update_from_asset_added_event() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let added_at = Utc::now();

        let event = TokenizedAssetEvent::Added {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault,
            added_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: underlying.0.clone(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = TokenizedAssetView::default();

        assert!(matches!(view, TokenizedAssetView::Unavailable));

        view.update(&envelope);

        let TokenizedAssetView::Asset {
            underlying: view_underlying,
            token: view_token,
            network: view_network,
            vault: view_vault,
            enabled,
            added_at: view_added_at,
        } = view
        else {
            panic!("Expected Asset, got Unavailable")
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_network, network);
        assert_eq!(view_vault, vault);
        assert!(enabled);
        assert_eq!(view_added_at, added_at);
    }

    #[test]
    fn test_view_update_from_vault_address_updated_event() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault_a = address!("0x1234567890abcdef1234567890abcdef12345678");
        let vault_b = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let added_at = Utc::now();
        let updated_at = Utc::now();

        let mut view = TokenizedAssetView::Asset {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault: vault_a,
            enabled: true,
            added_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: underlying.0.clone(),
            sequence: 2,
            payload: TokenizedAssetEvent::VaultAddressUpdated {
                vault: vault_b,
                previous_vault: vault_a,
                updated_at,
            },
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let TokenizedAssetView::Asset {
            underlying: view_underlying,
            token: view_token,
            network: view_network,
            vault: view_vault,
            enabled,
            added_at: view_added_at,
        } = view
        else {
            panic!("Expected Asset, got Unavailable")
        };

        assert_eq!(view_vault, vault_b, "Vault should be updated to vault B");
        assert_eq!(
            view_underlying, underlying,
            "Underlying should be preserved"
        );
        assert_eq!(view_token, token, "Token should be preserved");
        assert_eq!(view_network, network, "Network should be preserved");
        assert!(enabled, "Enabled should be preserved");
        assert_eq!(view_added_at, added_at, "Added-at should be preserved");
    }

    #[tokio::test]
    async fn test_list_enabled_assets_returns_added_assets() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        harness
            .add_asset(
                "AAPL",
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .await;
        harness
            .add_asset(
                "TSLA",
                address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            )
            .await;

        let result =
            list_enabled_assets(pool).await.expect("Query should succeed");

        assert_eq!(result.len(), 2);

        let underlyings: Vec<_> = result
            .iter()
            .filter_map(|v| match v {
                TokenizedAssetView::Asset { underlying, .. } => {
                    Some(underlying.0.as_str())
                }
                TokenizedAssetView::Unavailable => None,
            })
            .collect();

        assert!(underlyings.contains(&"AAPL"));
        assert!(underlyings.contains(&"TSLA"));
    }

    #[tokio::test]
    async fn test_list_enabled_assets_returns_empty_when_none() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let result =
            list_enabled_assets(pool).await.expect("Query should succeed");

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_vault_by_underlying_returns_vault_when_exists() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let expected_vault =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        harness.add_asset("AAPL", expected_vault).await;

        let result =
            find_vault_by_underlying(pool, &UnderlyingSymbol::new("AAPL"))
                .await
                .expect("Query should succeed");

        assert_eq!(result, Some(expected_vault));
    }

    #[tokio::test]
    async fn test_find_vault_by_underlying_returns_none_when_not_exists() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let result =
            find_vault_by_underlying(pool, &UnderlyingSymbol::new("AAPL"))
                .await
                .expect("Query should succeed");

        assert_eq!(result, None);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_replay_rebuilds_view_from_events() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        // Seed only the events table — no view row
        let event_payload =
            serde_json::to_string(&TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault,
                added_at: chrono::Utc::now(),
            })
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
            VALUES ('TokenizedAsset', 'AAPL', 1, 'TokenizedAssetEvent::Added', '1.0', ?, '{}')
            ",
        )
        .bind(&event_payload)
        .execute(&pool)
        .await
        .unwrap();

        // View should be empty before replay
        let before = list_enabled_assets(&pool).await.unwrap();
        assert!(before.is_empty(), "View should be empty before replay");

        // Replay rebuilds the view from events
        replay_tokenized_asset_view(pool.clone()).await.unwrap();

        let after = list_enabled_assets(&pool).await.unwrap();
        assert_eq!(after.len(), 1);

        match &after[0] {
            TokenizedAssetView::Asset {
                underlying, vault: view_vault, ..
            } => {
                assert_eq!(underlying.0, "AAPL");
                assert_eq!(*view_vault, vault);
            }
            TokenizedAssetView::Unavailable => {
                panic!("Expected Asset after replay, got Unavailable")
            }
        }

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Replaying tokenized asset view from events"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Tokenized asset view replay complete"]
        ));
    }
}
