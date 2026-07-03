use event_sorcery::{AggregateError, Store};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use sqlx::{Pool, Sqlite};
use st0x_issuance_dto::{
    AddTokenizedAssetRequest, AddTokenizedAssetResponse, AssetKey,
    TokenizedAssetDetailResponse, TokenizedAssetResponse,
    TokenizedAssetStatusResponse, TokenizedAssetsListResponse,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::error;

use super::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol, view::TokenizedAssetView,
};
use crate::auth::{InternalAuth, IssuerAuth};

fn require_network_query(network: Option<&str>) -> Result<Network, Status> {
    let Some(wire) = network else {
        return Err(Status::UnprocessableEntity);
    };

    wire.parse().map_err(|_| Status::UnprocessableEntity)
}

fn merge_token_listing(
    views: Vec<TokenizedAssetView>,
) -> Vec<TokenizedAssetResponse> {
    let mut merged: BTreeMap<
        (UnderlyingSymbol, TokenSymbol),
        TokenizedAssetResponse,
    > = BTreeMap::new();

    for TokenizedAssetView { underlying, token, network, .. } in views {
        let row_key = (underlying.clone(), token.clone());
        merged
            .entry(row_key)
            .or_insert_with(|| TokenizedAssetResponse {
                underlying: underlying.clone(),
                token: token.clone(),
                networks: Vec::new(),
            })
            .networks
            .push(network);
    }

    let mut tokens: Vec<_> = merged.into_values().collect();
    for row in &mut tokens {
        row.networks.sort_by_key(|network| network.as_str().to_string());
    }
    tokens.sort_by(|left, right| left.underlying.0.cmp(&right.underlying.0));
    tokens
}

#[utoipa::path(
    get,
    path = "/tokenized-assets/{underlying}",
    tag = "tokenized-assets",
    params(
        ("underlying" = String, Path,
            description = "Underlying equity symbol, e.g. SGOV"),
        ("network" = String, Query,
            description = "Blockchain network wire value, e.g. base")
    ),
    responses(
        (status = 200, description = "Asset detail including freeze status",
            body = TokenizedAssetDetailResponse),
        (status = 404, description = "Unknown asset"),
        (status = 422, description = "Missing or unsupported `network` query parameter"),
        (status = 500, description = "View load or deserialization failure")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, pool))]
#[get("/tokenized-assets/<underlying>?<network>")]
pub(crate) async fn get_tokenized_asset(
    underlying: &str,
    network: Option<&str>,
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
) -> Result<Json<TokenizedAssetDetailResponse>, Status> {
    let network = require_network_query(network)?;
    let underlying_symbol = UnderlyingSymbol::new(underlying);

    let view = super::view::load_asset_by_network(
        pool.inner(),
        &underlying_symbol,
        &network,
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
            status: status.into(),
        })),
        None => Err(Status::NotFound),
    }
}

#[utoipa::path(
    get,
    path = "/tokenized-assets/{underlying}/status",
    tag = "tokenized-assets",
    params(
        ("underlying" = String, Path,
            description = "Underlying equity symbol, e.g. SGOV"),
        ("network" = String, Query,
            description = "Blockchain network wire value, e.g. base")
    ),
    responses(
        (status = 200, description = "Per-asset freeze status",
            body = TokenizedAssetStatusResponse),
        (status = 404, description = "Unknown asset"),
        (status = 422, description = "Missing or unsupported `network` query parameter"),
        (status = 500,
            description = "Indeterminate (view load failure); retry, do not treat as enabled")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, pool))]
#[get("/tokenized-assets/<underlying>/status?<network>")]
pub(crate) async fn get_tokenized_asset_status(
    underlying: &str,
    network: Option<&str>,
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
) -> Result<Json<TokenizedAssetStatusResponse>, Status> {
    let network = require_network_query(network)?;
    let underlying_symbol = UnderlyingSymbol::new(underlying);

    let view = super::view::load_asset_by_network(
        pool.inner(),
        &underlying_symbol,
        &network,
    )
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
                status: status.into(),
            }))
        }
        None => Err(Status::NotFound),
    }
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

    let tokens = merge_token_listing(views);

    Ok(Json(TokenizedAssetsListResponse { tokens }))
}

#[utoipa::path(
    post,
    path = "/tokenized-assets",
    tag = "tokenized-assets",
    request_body = AddTokenizedAssetRequest,
    responses(
        (status = 201, description = "Asset added (idempotent: also 201 if it already existed)",
            body = AddTokenizedAssetResponse),
        (status = 500, description = "Failed to add asset")
    ),
    security(("internal_api_key" = []))
)]
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

    let asset_key =
        AssetKey::new(request.underlying.clone(), request.network.clone());

    store
        .send(&asset_key, command)
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
    use chrono::Utc;
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
    use crate::config::{Config, Environment, LogLevel};
    use crate::fireblocks::SignerConfig;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        AssetKey, AssetStatus, Network, TokenSymbol, TokenizedAsset,
        TokenizedAssetCommand,
    };

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
            environment: Environment::Development,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
            subgraph_url: Url::parse("http://localhost:0/subgraph")
                .expect("valid test URL"),
            chains: Vec::new(),
        }
    }

    fn base_view(underlying: &str, token: &str) -> TokenizedAssetView {
        TokenizedAssetView {
            underlying: UnderlyingSymbol::new(underlying),
            token: TokenSymbol::new(token),
            network: Network::Base,
            vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            status: AssetStatus::Enabled,
            added_at: Utc::now(),
        }
    }

    // Only one `Network` variant exists today, so the multi-chain collapse is
    // exercised with two rows sharing the same (underlying, token) key: both
    // must accumulate into a single response row via the map entry. When a
    // second network variant lands, the projection produces exactly this shape
    // with distinct networks.
    #[test]
    fn merge_token_listing_merges_rows_sharing_underlying_and_token() {
        let merged = merge_token_listing(vec![
            base_view("AAPL", "tAAPL"),
            base_view("AAPL", "tAAPL"),
        ]);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].underlying, UnderlyingSymbol::new("AAPL"));
        assert_eq!(merged[0].token, TokenSymbol::new("tAAPL"));
        assert_eq!(merged[0].networks, vec![Network::Base, Network::Base]);
    }

    #[test]
    fn merge_token_listing_sorts_rows_by_underlying() {
        let merged = merge_token_listing(vec![
            base_view("MSFT", "tMSFT"),
            base_view("AAPL", "tAAPL"),
        ]);

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].underlying, UnderlyingSymbol::new("AAPL"));
        assert_eq!(merged[0].networks, vec![Network::Base]);
        assert_eq!(merged[1].underlying, UnderlyingSymbol::new("MSFT"));
        assert_eq!(merged[1].networks, vec![Network::Base]);
    }

    #[test]
    fn merge_token_listing_passes_single_asset_through() {
        let merged = merge_token_listing(vec![base_view("SGOV", "tSGOV")]);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].underlying, UnderlyingSymbol::new("SGOV"));
        assert_eq!(merged[0].token, TokenSymbol::new("tSGOV"));
        assert_eq!(merged[0].networks, vec![Network::Base]);
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

        let key = AssetKey::new(UnderlyingSymbol::new("AAPL"), Network::Base);
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
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

        // Assert the raw JSON the Alpaca/dashboard consumers see — not a
        // round-trip through the producer struct, which would mask a serde
        // rename or a change to the moved DTO/newtype wire encoding.
        let body: Value =
            response.into_json().await.expect("valid JSON response");
        assert_eq!(
            body,
            json!({
                "tokens": [{
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "networks": ["base"]
                }]
            })
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

        // Assert the raw wire shape rather than round-tripping through the DTO
        // struct: a wire-breaking rename (e.g. `tokens` -> `assets`) must fail
        // as a contract mismatch here, not get masked by a struct deserialize
        // error that never reaches the field assertion.
        let body: Value =
            response.into_json().await.expect("valid JSON response");

        assert_eq!(body, json!({ "tokens": [] }));
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

        assert_eq!(response.status(), Status::Created);

        // Pin the POST response body on the wire — the moved
        // AddTokenizedAssetResponse DTO encoded through Rocket's JSON responder.
        let body: Value =
            response.into_json().await.expect("valid JSON response");
        assert_eq!(body, json!({ "underlying": "AAPL" }));
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

        let key = AssetKey::new(UnderlyingSymbol::new("AAPL"), Network::Base);
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
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
            .get("/tokenized-assets/AAPL/status?network=base")
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
            json!({ "underlying": "AAPL", "status": "enabled" })
        );

        store
            .send(&key, TokenizedAssetCommand::Freeze)
            .await
            .expect("Failed to freeze asset");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing tokenized asset", "AAPL"]
        ));

        let after = client
            .get("/tokenized-assets/AAPL/status?network=base")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(after.status(), Status::Ok);

        // A frozen asset stays listed for redemptions; the status flips to
        // `frozen`.
        let after_body: Value =
            after.into_json().await.expect("valid JSON response");
        assert_eq!(
            after_body,
            json!({ "underlying": "AAPL", "status": "frozen" })
        );

        // Unfreezing must flip the status back to `enabled` — the other half of
        // the guard's lifecycle, exercised through the same HTTP + projection
        // path.
        store
            .send(&key, TokenizedAssetCommand::Unfreeze)
            .await
            .expect("Failed to unfreeze asset");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Unfreezing tokenized asset", "AAPL"]
        ));

        let unfrozen = client
            .get("/tokenized-assets/AAPL/status?network=base")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(unfrozen.status(), Status::Ok);

        let unfrozen_body: Value =
            unfrozen.into_json().await.expect("valid JSON response");
        assert_eq!(
            unfrozen_body,
            json!({ "underlying": "AAPL", "status": "enabled" })
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_get_detail_reflects_freeze() {
        let pool = migrated_in_memory_pool().await;
        let store = setup_tokenized_asset_store(&pool).await;

        let key = AssetKey::new(UnderlyingSymbol::new("AAPL"), Network::Base);
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
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
            .mount("/", routes![get_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let enabled = client
            .get("/tokenized-assets/AAPL?network=base")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(enabled.status(), Status::Ok);

        // Assert the raw JSON body rather than round-tripping the producer
        // struct, so a serde rename or a regression back to a lossy `enabled`
        // bool fails here.
        let enabled_body: Value =
            enabled.into_json().await.expect("valid JSON response");
        assert_eq!(
            enabled_body,
            json!({
                "underlying": "AAPL",
                "token": "tAAPL",
                "network": "base",
                "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "status": "enabled"
            })
        );

        store
            .send(&key, TokenizedAssetCommand::Freeze)
            .await
            .expect("Failed to freeze asset");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing tokenized asset", "AAPL"]
        ));

        let frozen = client
            .get("/tokenized-assets/AAPL?network=base")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(frozen.status(), Status::Ok);

        // A frozen asset must report `status: "frozen"`, not the old
        // `enabled: true` that conflated frozen with mint-accepting.
        let frozen_body: Value =
            frozen.into_json().await.expect("valid JSON response");
        assert_eq!(
            frozen_body,
            json!({
                "underlying": "AAPL",
                "token": "tAAPL",
                "network": "base",
                "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "status": "frozen"
            })
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
            VALUES ('AAPL:base', 1, '{"Live": {"bad_field": 1}}')
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
            .get("/tokenized-assets/AAPL/status?network=base")
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
            .get("/tokenized-assets/UNKNOWN/status?network=base")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::NotFound);

        // The 404 must not carry a parseable status — the guard has to tell
        // "unknown" apart from "known and enabled", so a regression that returned
        // a status body with 404 must fail here.
        let body = response
            .into_string()
            .await
            .expect("404 response body should be readable");
        assert!(
            !body.contains("\"status\""),
            "404 body must not expose a status, got: {body}"
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
            VALUES ('AAPL:base', 1, '{"Live": null}')
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
            .get("/tokenized-assets/AAPL/status?network=base")
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
            VALUES ('AAPL:base', 1, '{"Live": null}')
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
            .get("/tokenized-assets/AAPL?network=base")
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
    async fn test_get_status_missing_network_returns_422() {
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
            .get("/tokenized-assets/AAPL/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    // A present-but-unrecognised network wire value must map to 422 (the
    // documented contract), not 404 -- it is a malformed query, not a missing
    // asset.
    #[tokio::test]
    async fn test_get_status_unsupported_network_returns_422() {
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
            .get("/tokenized-assets/AAPL/status?network=solana")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    // The detail route binds `?<network>` independently of the status route, so
    // its 422 contract for an absent network parameter needs its own guard.
    #[tokio::test]
    async fn test_get_detail_missing_network_returns_422() {
        let pool = migrated_in_memory_pool().await;

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

        assert_eq!(response.status(), Status::UnprocessableEntity);
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
            .get("/tokenized-assets/AAPL/status?network=base")
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
