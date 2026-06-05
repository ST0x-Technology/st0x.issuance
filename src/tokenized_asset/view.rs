use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{Network, TokenSymbol, UnderlyingSymbol};

#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizedAssetViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

/// Read model for a tokenized asset, projected from the
/// `tokenized_asset_view` table.
///
/// `tokenized_asset_view` stores the event-sorcery
/// `Lifecycle<TokenizedAsset>` payload (`{"Live": {...}}`). The query helpers
/// extract the `$.Live` sub-object, which mirrors this struct field-for-field,
/// so it deserializes straight into `TokenizedAssetView`. Non-live rows have a
/// NULL `$.Live` and are treated as "not found".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TokenizedAssetView {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) enabled: bool,
    pub(crate) added_at: DateTime<Utc>,
}

pub(crate) async fn list_enabled_assets(
    pool: &Pool<Sqlite>,
) -> Result<Vec<TokenizedAssetView>, TokenizedAssetViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM tokenized_asset_view
        WHERE json_extract(payload, '$.Live.enabled') = 1
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .filter_map(|row| row.live)
        .map(|live| serde_json::from_str(&live).map_err(Into::into))
        .collect()
}

/// Loads the full tokenized asset view for a given underlying symbol.
///
/// Returns `Ok(Some(view))` if the asset exists, `Ok(None)` if not found,
/// or an error on database failure.
pub(crate) async fn load_asset_by_underlying(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<Option<TokenizedAssetView>, TokenizedAssetViewError> {
    let row = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM tokenized_asset_view
        WHERE view_id = ?
        "#,
        underlying.0
    )
    .fetch_optional(pool)
    .await?;

    let Some(live) = row.and_then(|row| row.live) else {
        return Ok(None);
    };

    let view: TokenizedAssetView = serde_json::from_str(&live)?;

    Ok(Some(view))
}

/// Finds the vault address for a given underlying symbol.
///
/// Returns `Ok(Some(vault))` if an enabled asset with that underlying exists,
/// `Ok(None)` if not found or disabled, or an error on database failure.
pub(crate) async fn find_vault_by_underlying(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<Option<Address>, TokenizedAssetViewError> {
    Ok(load_asset_by_underlying(pool, underlying)
        .await?
        .and_then(|asset| asset.enabled.then_some(asset.vault)))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use event_sorcery::{Store, StoreBuilder};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, TokenizedAssetEvent,
    };

    struct TestHarness {
        pool: Pool<Sqlite>,
        store: Arc<Store<TokenizedAsset>>,
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

            let (store, _projection) =
                StoreBuilder::<TokenizedAsset>::new(pool.clone())
                    .build(())
                    .await
                    .expect("Failed to build tokenized asset store");

            Self { pool, store }
        }

        async fn add_asset(&self, underlying: &str, vault: Address) {
            let underlying = UnderlyingSymbol::new(underlying);
            self.store
                .send(
                    &underlying,
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!("t{}", underlying.0)),
                        network: Network::new("base"),
                        vault,
                    },
                )
                .await
                .expect("Failed to add asset");
        }
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

        let underlyings: Vec<_> =
            result.iter().map(|asset| asset.underlying.0.as_str()).collect();

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

    // Proves the tokenized_asset_view-to-lifecycle migration is data-safe:
    // after the view table is cleared (as the migration's DROP TABLE does), a
    // fresh `StoreBuilder::build` rebuilds every row from the event log via
    // catch_up.
    #[traced_test]
    #[tokio::test]
    async fn build_rebuilds_tokenized_asset_view_from_events_after_view_cleared()
     {
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

        // Seed only the events table — no view row.
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

        // View should be empty before the store catches up.
        let before = list_enabled_assets(&pool).await.unwrap();
        assert!(before.is_empty(), "View should be empty before rebuild");

        // Building the store rebuilds the view from the event log via catch_up.
        let (_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        let after = list_enabled_assets(&pool).await.unwrap();
        assert_eq!(after.len(), 1);

        assert_eq!(after[0].underlying.0, "AAPL");
        assert_eq!(after[0].vault, vault);

        // The catch-up log is emitted by event-sorcery under target "cqrs"
        // (not this module's "asset" domain target); the macro's domain filter
        // matches because #[traced_test] prefixes each line with this test
        // function's span name, which contains "asset". Including "cqrs" in
        // the snippets pins the assertion to the actual emitter.
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["cqrs", "View caught up successfully", "AAPL"]
        ));
    }

    #[tokio::test]
    async fn test_load_asset_by_underlying_errors_on_malformed_payload() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        // Well-formed JSON whose `$.Live` value is not a valid
        // TokenizedAssetView — only reachable via external DB manipulation,
        // since the projection always writes the Lifecycle payload. The load
        // must surface a deserialization error rather than masking the row as
        // not-found.
        sqlx::query(
            "
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{\"Live\": {\"bad_field\": 1}}')
            ",
        )
        .execute(pool)
        .await
        .unwrap();

        let result =
            load_asset_by_underlying(pool, &UnderlyingSymbol::new("AAPL"))
                .await;

        assert!(matches!(
            result.unwrap_err(),
            TokenizedAssetViewError::Deserialization(_)
        ));
    }

    #[tokio::test]
    async fn test_find_vault_by_underlying_reflects_vault_update() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let vault_a = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let vault_b = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        harness.add_asset("AAPL", vault_a).await;
        // Re-adding with a different vault emits VaultAddressUpdated, which the
        // projection must apply to the view row.
        harness.add_asset("AAPL", vault_b).await;

        let result =
            find_vault_by_underlying(pool, &UnderlyingSymbol::new("AAPL"))
                .await
                .expect("Query should succeed");

        assert_eq!(
            result,
            Some(vault_b),
            "view must reflect the updated vault after VaultAddressUpdated"
        );
    }
}
