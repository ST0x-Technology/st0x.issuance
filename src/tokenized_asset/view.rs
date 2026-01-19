use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetEvent, UnderlyingSymbol,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizedAssetViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
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

/// Finds the vault address for a given underlying symbol.
///
/// Returns `Ok(Some(vault))` if an enabled asset with that underlying exists,
/// `Ok(None)` if not found or disabled, or an error on database failure.
pub(crate) async fn find_vault_by_underlying(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<Option<Address>, TokenizedAssetViewError> {
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM tokenized_asset_view
        WHERE view_id = ?
          AND json_extract(payload, '$.Asset.enabled') = 1
        "#,
        underlying.0
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: TokenizedAssetView = serde_json::from_str(&row.payload)?;

    match view {
        TokenizedAssetView::Asset { vault, .. } => Ok(Some(vault)),
        TokenizedAssetView::Unavailable => Ok(None),
    }
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

    use super::*;
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
                _ => None,
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
}
