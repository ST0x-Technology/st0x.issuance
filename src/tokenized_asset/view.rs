use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetEvent,
    UnderlyingSymbol, VaultAddress,
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
        vault_address: VaultAddress,
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
            TokenizedAssetEvent::AssetAdded {
                underlying,
                token,
                network,
                vault_address,
                added_at,
            } => {
                *self = Self::Asset {
                    underlying: underlying.clone(),
                    token: token.clone(),
                    network: network.clone(),
                    vault_address: vault_address.clone(),
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

    Ok(views
        .into_iter()
        .filter(|view| {
            matches!(view, TokenizedAssetView::Asset { enabled: true, .. })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_es::EventEnvelope;
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;

    async fn setup_test_db() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    #[test]
    fn test_view_update_from_asset_added_event() {
        let underlying = UnderlyingSymbol("AAPL".to_string());
        let token = TokenSymbol("stAAPL".to_string());
        let network = Network("base".to_string());
        let vault_address = VaultAddress("0x1234567890abcdef".to_string());
        let added_at = Utc::now();

        let event = TokenizedAssetEvent::AssetAdded {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault_address: vault_address.clone(),
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
            vault_address: view_vault_address,
            enabled,
            added_at: view_added_at,
        } = view
        else {
            panic!("Expected Asset, got Unavailable")
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_network, network);
        assert_eq!(view_vault_address, vault_address);
        assert!(enabled);
        assert_eq!(view_added_at, added_at);
    }

    #[tokio::test]
    async fn test_list_enabled_assets_returns_only_enabled() {
        let pool = setup_test_db().await;

        let enabled_underlying = UnderlyingSymbol("AAPL".to_string());
        let enabled_view = TokenizedAssetView::Asset {
            underlying: enabled_underlying.clone(),
            token: TokenSymbol("stAAPL".to_string()),
            network: Network("base".to_string()),
            vault_address: VaultAddress("0xaaaa".to_string()),
            enabled: true,
            added_at: Utc::now(),
        };

        let disabled_underlying = UnderlyingSymbol("TSLA".to_string());
        let disabled_view = TokenizedAssetView::Asset {
            underlying: disabled_underlying.clone(),
            token: TokenSymbol("stTSLA".to_string()),
            network: Network("base".to_string()),
            vault_address: VaultAddress("0xbbbb".to_string()),
            enabled: false,
            added_at: Utc::now(),
        };

        let enabled_payload = serde_json::to_string(&enabled_view)
            .expect("Failed to serialize view");
        let disabled_payload = serde_json::to_string(&disabled_view)
            .expect("Failed to serialize view");

        sqlx::query!(
            r"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            enabled_underlying.0,
            enabled_payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert enabled view");

        sqlx::query!(
            r"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            disabled_underlying.0,
            disabled_payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert disabled view");

        let result =
            list_enabled_assets(&pool).await.expect("Query should succeed");

        assert_eq!(result.len(), 1);

        let TokenizedAssetView::Asset { underlying, enabled, .. } = &result[0]
        else {
            panic!("Expected Asset, got Unavailable")
        };

        assert_eq!(underlying, &enabled_underlying);
        assert!(enabled);
    }

    #[tokio::test]
    async fn test_list_enabled_assets_returns_empty_when_none() {
        let pool = setup_test_db().await;

        let result =
            list_enabled_assets(&pool).await.expect("Query should succeed");

        assert!(result.is_empty());
    }
}
