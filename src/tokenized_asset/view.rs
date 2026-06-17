use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{AssetStatus, Network, TokenSymbol, UnderlyingSymbol};

#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizedAssetViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error(
        "tokenized asset {underlying} has a non-live (null `$.Live`) projection row"
    )]
    NonLiveRow { underlying: UnderlyingSymbol },
}

/// Read model for a tokenized asset, projected from the
/// `tokenized_asset_view` table.
///
/// `tokenized_asset_view` stores the event-sorcery
/// `Lifecycle<TokenizedAsset>` payload (`{"Live": {...}}`). The query helpers
/// extract the `$.Live` sub-object, which mirrors this struct field-for-field,
/// so it deserializes straight into `TokenizedAssetView`. A non-live row has a
/// NULL `$.Live` and is surfaced as a `NonLiveRow` error (known but
/// indeterminate), distinct from an absent row ("not found").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TokenizedAssetView {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) status: AssetStatus,
    pub(crate) added_at: DateTime<Utc>,
}

/// Lists all supported assets, including frozen ones.
///
/// Freezing gates new minting only — a frozen asset stays in this set so
/// in-flight redemption detection (`src/redemption/`) and receipt backfilling
/// keep working. The filter matches `$.Live.status` against the supported
/// states rather than excluding `Frozen`, preserving the freeze invariant (see
/// SPEC.md).
pub(crate) async fn list_enabled_assets(
    pool: &Pool<Sqlite>,
) -> Result<Vec<TokenizedAssetView>, TokenizedAssetViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM tokenized_asset_view
        WHERE json_extract(payload, '$.Live.status') IN ('Enabled', 'Frozen')
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
/// Returns `Ok(Some(view))` for a live asset, `Ok(None)` if no row exists
/// (unknown asset), or an error — including
/// [`TokenizedAssetViewError::NonLiveRow`] when a row exists but its `$.Live`
/// is null (a non-live or corrupt projection: known but indeterminate, not
/// "unknown").
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

    let Some(row) = row else {
        return Ok(None);
    };

    // A row whose `$.Live` is NULL is a non-live lifecycle state (e.g. a
    // `Failed` lifecycle) or a corrupt projection: the asset is *known* but its
    // state is indeterminate, which is distinct from an unknown asset. Surface
    // it as an error so the status endpoint returns 500 ("indeterminate,
    // retry") rather than 404 ("unknown"). See SPEC.md's status-code semantics.
    let Some(live) = row.live else {
        return Err(TokenizedAssetViewError::NonLiveRow {
            underlying: underlying.clone(),
        });
    };

    let view: TokenizedAssetView = serde_json::from_str(&live)?;

    Ok(Some(view))
}

/// Finds the vault address for a given underlying symbol.
///
/// Delegates to [`load_asset_by_underlying`] and so inherits its contract:
/// `Ok(Some(vault))` if a supported asset with that underlying exists
/// (including a frozen one — in-flight redemptions and mints of a frozen asset
/// must still resolve their vault), `Ok(None)` only when no row exists (unknown
/// asset), or an error on database/deserialization failure — including
/// [`TokenizedAssetViewError::NonLiveRow`] when a row exists but its `$.Live`
/// is null (known but indeterminate, not "not found"). Callers in the mint and
/// redemption flows therefore see a propagated error, not `Ok(None)`, for a
/// non-live row.
pub(crate) async fn find_vault_by_underlying(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<Option<Address>, TokenizedAssetViewError> {
    Ok(load_asset_by_underlying(pool, underlying)
        .await?
        .map(|asset| asset.vault))
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
        AssetStatus, TokenizedAsset, TokenizedAssetCommand, TokenizedAssetEvent,
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
                        network: Network::Base,
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
                network: Network::Base,
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

    // Production regression (RAI-1083): the issuance bot crash-looped at startup
    // because projection catch-up asserted each aggregate's events form a
    // contiguous `1..N` run, aborting with a fatal `EventSequenceGap` otherwise.
    // Real production data violates that: `ReceiptInventory` aggregates were
    // written with a sequence counter shared across aggregates (sparse
    // per-aggregate sequences such as `24, 4559, ...`), and a heavily-retried
    // `Mint` carried a sequence hole. With the event-sorcery catch-up fix pinned,
    // startup must replay whatever events exist in `sequence` order and rebuild
    // the view rather than abort. `TokenizedAsset` exercises the same generic
    // Table-projection catch-up path that crashed the `Mint` aggregate in prod.
    #[traced_test]
    #[tokio::test]
    async fn build_rebuilds_view_despite_non_contiguous_event_sequences() {
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

        // A genesis `Added` at a non-1 sequence followed by a `Frozen` after a
        // large gap reproduces the sparse, shared-counter sequencing that aborted
        // catch-up in production.
        let added = serde_json::to_string(&TokenizedAssetEvent::Added {
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            vault,
            added_at: chrono::Utc::now(),
        })
        .unwrap();
        let frozen = serde_json::to_string(&TokenizedAssetEvent::Frozen {
            frozen_at: chrono::Utc::now(),
        })
        .unwrap();

        for (sequence, event_type, payload) in [
            (24_i64, "TokenizedAssetEvent::Added", added.as_str()),
            (4559_i64, "TokenizedAssetEvent::Frozen", frozen.as_str()),
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

        // Catch-up must not abort on the gap: it replays both events in sequence
        // order and rebuilds the view from scratch.
        StoreBuilder::<TokenizedAsset>::new(pool.clone())
            .build(())
            .await
            .expect("startup must tolerate non-contiguous event sequences");

        // `Frozen` applied on top of `Added` proves in-order replay across the
        // gap -- an out-of-order or dropped event could not yield `Frozen`.
        let asset =
            load_asset_by_underlying(&pool, &UnderlyingSymbol::new("AAPL"))
                .await
                .unwrap()
                .expect("asset rebuilt despite the sequence gap");
        assert_eq!(asset.status, AssetStatus::Frozen);
        assert_eq!(asset.vault, vault);

        // The view advances to the max event sequence, not the event count.
        let (version,): (i64,) = sqlx::query_as(
            "SELECT version FROM tokenized_asset_view WHERE view_id = 'AAPL'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(version, 4559, "view advances to the max event sequence");
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

    // A row whose `$.Live` is null is a non-live lifecycle state (known but
    // indeterminate), distinct from an absent row. The helper must surface it
    // as `NonLiveRow` rather than collapsing it into `Ok(None)`, so the status
    // endpoint can return 500 ("indeterminate, retry") instead of 404. Only
    // reachable via external DB manipulation — the projection always writes a
    // live `$.Live` payload.
    #[tokio::test]
    async fn test_load_asset_by_underlying_errors_on_non_live_row() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        sqlx::query(
            "
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{\"Live\": null}')
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
            TokenizedAssetViewError::NonLiveRow { underlying }
                if underlying.0 == "AAPL"
        ));
    }

    // `find_vault_by_underlying` delegates to `load_asset_by_underlying`, so a
    // non-live row must propagate as `NonLiveRow` rather than collapsing to
    // `Ok(None)` — the mint/redemption vault-lookup callers see an error, not
    // "asset not found", for a known-but-indeterminate row.
    #[tokio::test]
    async fn test_find_vault_by_underlying_errors_on_non_live_row() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        sqlx::query(
            "
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{\"Live\": null}')
            ",
        )
        .execute(pool)
        .await
        .unwrap();

        let result =
            find_vault_by_underlying(pool, &UnderlyingSymbol::new("AAPL"))
                .await;

        assert!(matches!(
            result.unwrap_err(),
            TokenizedAssetViewError::NonLiveRow { underlying }
                if underlying.0 == "AAPL"
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

    // Freezing must project `status: Frozen` into the view AND keep the asset in
    // `list_enabled_assets` — the freeze invariant that lets in-flight
    // redemptions of a frozen asset keep detecting (see SPEC.md).
    #[tokio::test]
    async fn test_freeze_projects_status_and_keeps_asset_listed() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, store } = &harness;

        let underlying = UnderlyingSymbol::new("AAPL");
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        harness.add_asset("AAPL", vault).await;

        let before = load_asset_by_underlying(pool, &underlying)
            .await
            .unwrap()
            .expect("asset exists before freeze");
        assert_eq!(before.status, AssetStatus::Enabled);

        store
            .send(&underlying, TokenizedAssetCommand::Freeze)
            .await
            .expect("Failed to freeze asset");

        let after = load_asset_by_underlying(pool, &underlying)
            .await
            .unwrap()
            .expect("asset exists after freeze");
        assert_eq!(after.status, AssetStatus::Frozen);

        let listed =
            list_enabled_assets(pool).await.expect("Query should succeed");
        assert_eq!(listed.len(), 1, "frozen asset must stay listed");
        assert_eq!(listed[0].status, AssetStatus::Frozen);

        // A frozen asset must still resolve its vault so in-flight redemptions
        // and mints can complete.
        let resolved = find_vault_by_underlying(pool, &underlying)
            .await
            .expect("Query should succeed");
        assert_eq!(resolved, Some(vault));
    }
}
