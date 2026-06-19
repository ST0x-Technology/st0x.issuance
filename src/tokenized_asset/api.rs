use alloy::primitives::Address;
use event_sorcery::{AggregateError, Store};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::error;

use super::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol, view::TokenizedAssetView,
};
use crate::auth::{InternalAuth, IssuerAuth};

#[derive(Debug, Serialize)]
pub(crate) struct TokenizedAssetDetailResponse {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) enabled: bool,
}

#[tracing::instrument(skip(_auth, pool))]
#[get("/tokenized-assets/<underlying>")]
pub(crate) async fn get_tokenized_asset(
    underlying: &str,
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
) -> Result<Json<TokenizedAssetDetailResponse>, Status> {
    let underlying_symbol = UnderlyingSymbol::new(underlying);

    let view = super::view::load_asset_by_underlying(
        pool.inner(),
        &underlying_symbol,
    )
    .await
    .map_err(|err| {
        error!(target: "asset", error = %err, "Failed to load tokenized asset");
        Status::InternalServerError
    })?;

    match view {
        Some(TokenizedAssetView {
            underlying,
            token,
            network,
            vault,
            status,
            ..
        }) => Ok(Json(TokenizedAssetDetailResponse {
            underlying,
            token,
            network,
            vault,
            enabled: status.is_listed(),
        })),
        None => Err(Status::NotFound),
    }
}

/// Per-asset freeze status, consumed by the liquidity rebalance guard
/// (RAI-1038) to skip frozen assets before starting a rebalancing flow.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenizedAssetStatusResponse {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) enabled: bool,
    pub(crate) frozen: bool,
}

#[tracing::instrument(skip(_auth, pool))]
#[get("/tokenized-assets/<underlying>/status")]
pub(crate) async fn get_tokenized_asset_status(
    underlying: &str,
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
) -> Result<Json<TokenizedAssetStatusResponse>, Status> {
    let underlying_symbol = UnderlyingSymbol::new(underlying);

    let view =
        super::view::load_asset_by_underlying(pool.inner(), &underlying_symbol)
            .await
            .map_err(|err| {
                error!(target: "asset", error = %err,
                    "Failed to load tokenized asset status"
                );
                Status::InternalServerError
            })?;

    match view {
        Some(TokenizedAssetView { underlying, status, .. }) => {
            Ok(Json(TokenizedAssetStatusResponse {
                underlying,
                enabled: status.is_listed(),
                frozen: status.is_frozen(),
            }))
        }
        None => Err(Status::NotFound),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenizedAssetResponse {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) networks: Vec<Network>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenizedAssetsListResponse {
    pub(crate) tokens: Vec<TokenizedAssetResponse>,
}

#[tracing::instrument(skip(_auth, pool))]
#[get("/tokenized-assets")]
pub(crate) async fn list_tokenized_assets(
    _auth: IssuerAuth,
    pool: &rocket::State<Pool<Sqlite>>,
) -> Result<Json<TokenizedAssetsListResponse>, rocket::http::Status> {
    let views =
        super::view::list_enabled_assets(pool.inner()).await.map_err(|e| {
            error!(target: "asset", "Failed to list enabled assets: {e}");
            rocket::http::Status::InternalServerError
        })?;

    let tokens = views
        .into_iter()
        .map(|TokenizedAssetView { underlying, token, network, .. }| {
            TokenizedAssetResponse {
                underlying,
                token,
                networks: vec![network],
            }
        })
        .collect();

    Ok(Json(TokenizedAssetsListResponse { tokens }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct AddTokenizedAssetRequest {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) vault: Address,
}

#[derive(Debug, Serialize)]
pub(crate) struct AddTokenizedAssetResponse {
    pub(crate) underlying: UnderlyingSymbol,
}

#[tracing::instrument(skip(_auth, store), fields(
    underlying = %request.underlying,
    token = %request.token,
    network = %request.network,
    vault = ?request.vault
))]
#[post("/tokenized-assets", format = "json", data = "<request>")]
pub(crate) async fn add_tokenized_asset(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<TokenizedAsset>>>,
    request: Json<AddTokenizedAssetRequest>,
) -> Result<(Status, Json<AddTokenizedAssetResponse>), Status> {
    let command = TokenizedAssetCommand::Add {
        underlying: request.underlying.clone(),
        token: request.token.clone(),
        network: request.network.clone(),
        vault: request.vault,
    };

    store
        .send(&request.underlying, command)
        .await
        .or_else(|err| match err {
            AggregateError::AggregateConflict => Ok(()),
            _ => Err(err),
        })
        .map_err(|err| {
            error!(target: "asset", "Failed to add tokenized asset: {err}");
            Status::InternalServerError
        })?;

    Ok((
        Status::Created,
        Json(AddTokenizedAssetResponse {
            underlying: request.underlying.clone(),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, address};
    use event_sorcery::StoreBuilder;
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use serde_json::{Value, json};
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing_test::traced_test;
    use url::Url;

    use super::*;
    use crate::alpaca::service::AlpacaConfig;
    use crate::auth::{FailedAuthRateLimiter, test_auth_config};
    use crate::config::{Config, LogLevel};
    use crate::fireblocks::SignerConfig;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};

    fn test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
            subgraph_url: Url::parse("http://localhost:0/subgraph")
                .expect("valid test URL"),
        }
    }

    #[tokio::test]
    async fn test_list_tokenized_assets_returns_added_assets() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let store = setup_tokenized_asset_store(&pool).await;

        store
            .send(
                &UnderlyingSymbol::new("AAPL"),
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    vault: address!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                },
            )
            .await
            .expect("Failed to add asset");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![list_tokenized_assets]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .get("/tokenized-assets")
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: TokenizedAssetsListResponse =
            response.into_json().await.expect("valid JSON response");

        assert_eq!(response_body.tokens.len(), 1);
        assert_eq!(
            response_body.tokens[0].underlying,
            UnderlyingSymbol::new("AAPL")
        );
        assert_eq!(response_body.tokens[0].token, TokenSymbol::new("tAAPL"));
        assert_eq!(
            response_body.tokens[0].networks,
            vec![Network::new("base")]
        );
    }

    #[tokio::test]
    async fn test_list_tokenized_assets_returns_empty_when_none() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![list_tokenized_assets]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .get("/tokenized-assets")
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: TokenizedAssetsListResponse =
            response.into_json().await.expect("valid JSON response");

        assert!(response_body.tokens.is_empty());
    }

    #[tokio::test]
    async fn test_list_tokenized_assets_without_auth_returns_401() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![list_tokenized_assets]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/tokenized-assets").dispatch().await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    async fn setup_tokenized_asset_store(
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Arc<Store<TokenizedAsset>> {
        let (store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build tokenized asset store");

        store
    }

    #[tokio::test]
    async fn test_add_new_asset_returns_201() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let store = setup_tokenized_asset_store(&pool).await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store)
            .manage(pool)
            .mount("/", routes![add_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "underlying": "AAPL",
            "token": "tAAPL",
            "network": "base",
            "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });

        let response = client
            .post("/tokenized-assets")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(
            response.status(),
            Status::Created,
            "Response body: {:?}",
            response.into_string().await
        );
    }

    #[tokio::test]
    async fn test_add_existing_asset_is_idempotent() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let store = setup_tokenized_asset_store(&pool).await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store)
            .manage(pool)
            .mount("/", routes![add_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "underlying": "AAPL",
            "token": "tAAPL",
            "network": "base",
            "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });

        let response = client
            .post("/tokenized-assets")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Created);

        let response = client
            .post("/tokenized-assets")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Created);
    }

    #[tokio::test]
    async fn test_concurrent_add_both_succeed() {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let store = setup_tokenized_asset_store(&pool).await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store)
            .manage(pool)
            .mount("/", routes![add_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "underlying": "AAPL",
            "token": "tAAPL",
            "network": "base",
            "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });

        let (response1, response2) = tokio::join!(
            client
                .post("/tokenized-assets")
                .header(ContentType::JSON)
                .header(Header::new(
                    "X-API-KEY",
                    "test-key-12345678901234567890123456",
                ))
                .remote("127.0.0.1:8000".parse().unwrap())
                .body(request_body.to_string())
                .dispatch(),
            client
                .post("/tokenized-assets")
                .header(ContentType::JSON)
                .header(Header::new(
                    "X-API-KEY",
                    "test-key-12345678901234567890123456",
                ))
                .remote("127.0.0.1:8000".parse().unwrap())
                .body(request_body.to_string())
                .dispatch()
        );

        assert_eq!(response1.status(), Status::Created);
        assert_eq!(response2.status(), Status::Created);
    }

    #[tokio::test]
    async fn test_add_asset_without_auth_returns_401() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let store = setup_tokenized_asset_store(&pool).await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store)
            .manage(pool)
            .mount("/", routes![add_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "underlying": "AAPL",
            "token": "tAAPL",
            "network": "base",
            "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });

        let response = client
            .post("/tokenized-assets")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    async fn migrated_in_memory_pool() -> sqlx::Pool<sqlx::Sqlite> {
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

    fn internal_api_key() -> Header<'static> {
        Header::new("X-API-KEY", "test-key-12345678901234567890123456")
    }

    #[traced_test]
    #[tokio::test]
    async fn test_get_status_reflects_freeze() {
        let pool = migrated_in_memory_pool().await;
        let store = setup_tokenized_asset_store(&pool).await;

        store
            .send(
                &UnderlyingSymbol::new("AAPL"),
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    vault: address!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                },
            )
            .await
            .expect("Failed to add asset");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset_status]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let before = client
            .get("/tokenized-assets/AAPL/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(before.status(), Status::Ok);

        // Assert the raw JSON body the external RAI-1038 guard parses — not a
        // round-trip through the producer struct, which would mask a serde
        // rename or a change to UnderlyingSymbol's wire encoding.
        let before_body: Value =
            before.into_json().await.expect("valid JSON response");
        assert_eq!(
            before_body,
            json!({ "underlying": "AAPL", "enabled": true, "frozen": false })
        );

        store
            .send(&UnderlyingSymbol::new("AAPL"), TokenizedAssetCommand::Freeze)
            .await
            .expect("Failed to freeze asset");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing tokenized asset", "AAPL"]
        ));

        let after = client
            .get("/tokenized-assets/AAPL/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(after.status(), Status::Ok);

        // A frozen asset stays enabled (listed) for redemptions; only `frozen`
        // flips to true.
        let after_body: Value =
            after.into_json().await.expect("valid JSON response");
        assert_eq!(
            after_body,
            json!({ "underlying": "AAPL", "enabled": true, "frozen": true })
        );

        // Unfreezing must flip `frozen` back to false — the other half of the
        // guard's lifecycle, exercised through the same HTTP + projection path.
        store
            .send(
                &UnderlyingSymbol::new("AAPL"),
                TokenizedAssetCommand::Unfreeze,
            )
            .await
            .expect("Failed to unfreeze asset");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Unfreezing tokenized asset", "AAPL"]
        ));

        let unfrozen = client
            .get("/tokenized-assets/AAPL/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(unfrozen.status(), Status::Ok);

        let unfrozen_body: Value =
            unfrozen.into_json().await.expect("valid JSON response");
        assert_eq!(
            unfrozen_body,
            json!({ "underlying": "AAPL", "enabled": true, "frozen": false })
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_get_status_db_error_returns_500() {
        let pool = migrated_in_memory_pool().await;

        // A row whose `$.Live` payload does not deserialize into
        // `TokenizedAssetView` forces the typed view load to error, exercising
        // the handler's error -> 500 mapping and the operator-facing log that
        // is the only signal for this failure mode.
        sqlx::query(
            r#"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{"Live": {"bad_field": 1}}')
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to insert malformed view row");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset_status]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .get("/tokenized-assets/AAPL/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::InternalServerError);

        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["Failed to load tokenized asset status"]
        ));
    }

    #[tokio::test]
    async fn test_get_status_unknown_asset_returns_404() {
        let pool = migrated_in_memory_pool().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset_status]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .get("/tokenized-assets/UNKNOWN/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::NotFound);

        // The 404 must not carry a parseable freeze status — the guard has to
        // tell "unknown" apart from "known and not frozen", so a regression that
        // returned a {enabled, frozen} body with 404 must fail here.
        let body = response.into_string().await.unwrap_or_default();
        assert!(
            !body.contains("\"frozen\""),
            "404 body must not expose a freeze status, got: {body}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_get_status_non_live_row_returns_500() {
        let pool = migrated_in_memory_pool().await;

        // A view row whose `$.Live` is null (a non-live lifecycle state) is a
        // known-but-indeterminate asset, not an unknown one. It must map to 500
        // ("indeterminate, retry"), NOT 404 — so the rebalance guard retries
        // rather than treating the asset as permanently absent.
        sqlx::query(
            r#"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{"Live": null}')
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to insert non-live view row");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset_status]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .get("/tokenized-assets/AAPL/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::InternalServerError);

        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["Failed to load tokenized asset status"]
        ));
    }

    // The detail endpoint shares `load_asset_by_underlying`, so a non-live
    // (`$.Live` null) row now flips it from 404 to 500 too — pin that changed
    // contract so the shared-helper behavior is not silently regressed.
    #[traced_test]
    #[tokio::test]
    async fn test_get_tokenized_asset_non_live_row_returns_500() {
        let pool = migrated_in_memory_pool().await;

        sqlx::query(
            r#"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{"Live": null}')
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to insert non-live view row");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .get("/tokenized-assets/AAPL")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::InternalServerError);

        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["Failed to load tokenized asset"]
        ));
    }

    #[tokio::test]
    async fn test_get_status_without_auth_returns_401() {
        let pool = migrated_in_memory_pool().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset_status]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response =
            client.get("/tokenized-assets/AAPL/status").dispatch().await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_get_status_wrong_api_key_returns_401() {
        let pool = migrated_in_memory_pool().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset_status]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        // A present-but-wrong key must be rejected, not just a missing header:
        // `InternalAuth` validates the key value, so knowing the header name is
        // not enough to reach the endpoint.
        let response = client
            .get("/tokenized-assets/AAPL/status")
            .header(Header::new(
                "X-API-KEY",
                "wrong-key-00000000000000000000000000",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Unauthorized);

        assert!(logs_contain_at!(tracing::Level::WARN, &["Invalid API key"]));
    }
}
