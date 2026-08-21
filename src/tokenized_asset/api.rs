use event_sorcery::{AggregateError, Store};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use sqlx::{Pool, Sqlite};
use st0x_issuance_dto::{
    AddTokenizedAssetRequest, AddTokenizedAssetResponse, AssetKey,
    TokenizedAssetDetailResponse, TokenizedAssetResponse,
    TokenizedAssetStatusResponse, TokenizedAssetsListResponse, VaultModeTag,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{error, warn};

use super::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol, view::TokenizedAssetView,
};
use crate::auth::{InternalAuth, IssuerAuth};
use crate::chain::ConfiguredNetworks;
use crate::config::{Config, VaultModeKind};
use crate::underlying::load_freeze_status;

impl From<VaultModeKind> for VaultModeTag {
    fn from(kind: VaultModeKind) -> Self {
        match kind {
            VaultModeKind::VaultDirect => Self::VaultDirect,
            // The tag deliberately drops the orchestrator address — the
            // liquidity bot only needs to know an authorization is required.
            VaultModeKind::Orchestrator => Self::Orchestrator,
        }
    }
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
            .or_insert(TokenizedAssetResponse {
                underlying,
                token,
                networks: Vec::new(),
            })
            .networks
            .push(network);
    }

    let mut tokens: Vec<_> = merged.into_values().collect();
    for row in &mut tokens {
        row.networks.sort_by_key(Network::as_str);
        row.networks.dedup();
    }
    tokens.sort_by(|left, right| {
        (left.underlying.as_str(), left.token.0.as_str())
            .cmp(&(right.underlying.as_str(), right.token.0.as_str()))
    });
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
    let Some(network) = network else {
        return Err(Status::UnprocessableEntity);
    };
    let network =
        network.parse::<Network>().map_err(|_| Status::UnprocessableEntity)?;
    let underlying = UnderlyingSymbol::new(underlying)
        .map_err(|_| Status::UnprocessableEntity)?;

    let view = super::view::load_asset_by_network(
        pool.inner(),
        &underlying,
        &network,
    )
    .await
    .map_err(|err| {
        error!(target: "asset", error = %err, "Failed to load tokenized asset");
        Status::InternalServerError
    })?;

    let Some(TokenizedAssetView { underlying, token, network, vault, .. }) =
        view
    else {
        return Err(Status::NotFound);
    };

    // Freeze status is a property of the underlying, not of this listing —
    // one freeze covers every network's listing of the underlying.
    let status =
        load_freeze_status(pool.inner(), &underlying).await.map_err(|err| {
            error!(target: "asset", error = %err,
                "Failed to load underlying freeze status"
            );
            Status::InternalServerError
        })?;

    Ok(Json(TokenizedAssetDetailResponse {
        underlying,
        token,
        network,
        vault,
        status: status.into(),
    }))
}

#[utoipa::path(
    get,
    path = "/tokenized-assets/{underlying}/status",
    tag = "tokenized-assets",
    params(
        ("underlying" = String, Path,
            description = "Underlying equity symbol, e.g. SGOV")
    ),
    responses(
        (status = 200, description = "Per-underlying freeze status",
            body = TokenizedAssetStatusResponse),
        (status = 404, description = "Underlying has no listing on any network"),
        (status = 500,
            description = "Indeterminate (view load failure); retry, do not treat as enabled")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, pool, config))]
#[get("/tokenized-assets/<underlying>/status")]
pub(crate) async fn get_tokenized_asset_status(
    underlying: &str,
    _auth: InternalAuth,
    pool: &rocket::State<Pool<Sqlite>>,
    config: &rocket::State<Config>,
) -> Result<Json<TokenizedAssetStatusResponse>, Status> {
    let underlying = UnderlyingSymbol::new(underlying)
        .map_err(|_| Status::UnprocessableEntity)?;

    // Freeze status is underlying-scoped (one corporate-action freeze covers
    // every network's listing), so no network parameter: the existence gate is
    // "does this underlying have any listing at all", preserving the
    // liquidity guard's fail-closed 404 handling for unknown assets.
    let has_listing =
        super::view::underlying_has_listing(pool.inner(), &underlying)
            .await
            .map_err(|err| {
                error!(target: "asset", error = %err,
                    "Failed to look up listings for underlying"
                );
                Status::InternalServerError
            })?;

    if !has_listing {
        return Err(Status::NotFound);
    }

    let status =
        load_freeze_status(pool.inner(), &underlying).await.map_err(|err| {
            error!(target: "asset", error = %err,
                "Failed to load underlying freeze status"
            );
            Status::InternalServerError
        })?;

    // Only the kind crosses the wire (the tag drops the address), so this
    // stays network-free: the mode is keyed by symbol alone.
    let vault_mode = config.vault_mode_kind_for(&underlying).into();

    Ok(Json(TokenizedAssetStatusResponse {
        underlying,
        status: status.into(),
        vault_mode,
    }))
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
        (status = 422, description = "Empty underlying symbol, network \
            without a chain configuration, or vault address already used on \
            another network"),
        (status = 500, description = "Failed to add asset")
    ),
    security(("internal_api_key" = []))
)]
#[tracing::instrument(skip(_auth, store, pool, configured_networks), fields(
    underlying = %request.underlying,
    token = %request.token,
    network = %request.network,
    vault = ?request.vault
))]
#[post("/tokenized-assets", format = "json", data = "<request>")]
pub(crate) async fn add_tokenized_asset(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<TokenizedAsset>>>,
    pool: &rocket::State<Pool<Sqlite>>,
    configured_networks: &rocket::State<ConfiguredNetworks>,
    request: Json<AddTokenizedAssetRequest>,
) -> Result<(Status, Json<AddTokenizedAssetResponse>), Status> {
    // An asset on a network with no chain config would make the next boot
    // abort in `validate_configured_asset_networks`, bricking the service
    // until the chain is configured or the asset removed. Reject upfront.
    // Non-Base networks are not fully wired for redemption/backfill until the
    // multichain redemption PR lands — do not register them in staging early.
    if !configured_networks.contains(request.network) {
        warn!(
            target: "asset",
            network = %request.network,
            "Rejected tokenized-asset registration for unconfigured network"
        );
        return Err(Status::UnprocessableEntity);
    }

    // Same guard as boot-time backfill: reject before the Add lands so a
    // shared vault across networks never waits until the next restart to fail.
    let mut assets = super::view::list_enabled_assets(pool.inner())
        .await
        .map_err(|error| {
            error!(
                target: "asset",
                error = %error,
                "Failed to list enabled assets before add"
            );
            Status::InternalServerError
        })?;
    assets.push(TokenizedAssetView {
        underlying: request.underlying.clone(),
        token: request.token.clone(),
        network: request.network,
        vault: request.vault,
        added_at: chrono::Utc::now(),
    });
    if let Err(collision) =
        super::validate_no_cross_network_vault_collisions(&assets)
    {
        warn!(
            target: "asset",
            vault = %collision.vault,
            first = %collision.first,
            second = %collision.second,
            "Rejected tokenized-asset registration: vault address already \
             used on another network"
        );
        return Err(Status::UnprocessableEntity);
    }

    let command = TokenizedAssetCommand::Add {
        underlying: request.underlying.clone(),
        token: request.token.clone(),
        network: request.network,
        vault: request.vault,
    };

    let asset_key = AssetKey::new(request.underlying.clone(), request.network);

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
    use std::collections::HashMap;
    use tracing_test::traced_test;
    use url::Url;

    use super::*;
    use crate::alpaca::service::AlpacaConfig;
    use crate::auth::{FailedAuthRateLimiter, test_auth_config};
    use crate::config::{Config, Environment, LogLevel, VaultModeConfig};
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    };
    use crate::underlying::{Underlying, UnderlyingCommand};
    use crate::wallet::SignerConfig;

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
            lifecycle_notifications:
                crate::LifecycleNotificationsConfig::disabled(),
            chains: Vec::new(),
            vault_mode_config: crate::config::VaultModeConfig::default(),
        }
    }

    fn base_view(underlying: &str, token: &str) -> TokenizedAssetView {
        TokenizedAssetView {
            underlying: UnderlyingSymbol::new(underlying).unwrap(),
            token: TokenSymbol::new(token),
            network: Network::Base,
            vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            added_at: Utc::now(),
        }
    }

    // Only one `Network` variant exists today, so the multi-chain collapse is
    // exercised with two rows sharing the same (underlying, token) key: both
    // must accumulate into a single response row whose `networks` is the
    // deduplicated union. When a second network variant lands, the projection
    // produces exactly this shape with distinct networks.
    #[test]
    fn merge_token_listing_merges_rows_sharing_underlying_and_token() {
        let merged = merge_token_listing(vec![
            base_view("AAPL", "tAAPL"),
            base_view("AAPL", "tAAPL"),
        ]);

        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].underlying,
            UnderlyingSymbol::new("AAPL").unwrap()
        );
        assert_eq!(merged[0].token, TokenSymbol::new("tAAPL"));
        assert_eq!(
            merged[0].networks,
            vec![Network::Base],
            "networks must be a deduplicated union, not repeated entries"
        );
    }

    #[test]
    fn merge_token_listing_sorts_rows_by_underlying() {
        let merged = merge_token_listing(vec![
            base_view("MSFT", "tMSFT"),
            base_view("AAPL", "tAAPL"),
        ]);

        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged[0].underlying,
            UnderlyingSymbol::new("AAPL").unwrap()
        );
        assert_eq!(merged[0].networks, vec![Network::Base]);
        assert_eq!(
            merged[1].underlying,
            UnderlyingSymbol::new("MSFT").unwrap()
        );
        assert_eq!(merged[1].networks, vec![Network::Base]);
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

        let key = AssetKey::new(
            UnderlyingSymbol::new("AAPL").unwrap(),
            Network::Base,
        );
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
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

    async fn setup_underlying_store(
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Arc<Store<Underlying>> {
        let (store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .expect("Failed to build underlying store");

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
            .manage(ConfiguredNetworks::from_iter([Network::Base]))
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
            .manage(ConfiguredNetworks::from_iter([Network::Base]))
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
            .manage(ConfiguredNetworks::from_iter([Network::Base]))
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
    async fn test_add_empty_underlying_returns_422() {
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
            .manage(ConfiguredNetworks::from_iter([Network::Base]))
            .mount("/", routes![add_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "underlying": "",
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

        assert_eq!(response.status(), Status::UnprocessableEntity);
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
            .manage(ConfiguredNetworks::from_iter([Network::Base]))
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

    #[traced_test]
    #[tokio::test]
    async fn test_add_asset_on_unconfigured_network_returns_422() {
        let pool = migrated_in_memory_pool().await;
        let store = setup_tokenized_asset_store(&pool).await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store.clone())
            .manage(pool)
            .manage(ConfiguredNetworks::from_iter([Network::Base]))
            .mount("/", routes![add_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "underlying": "AAPL",
            "token": "tAAPL",
            "network": "ethereum",
            "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });

        let response = client
            .post("/tokenized-assets")
            .header(ContentType::JSON)
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["unconfigured network", "ethereum"]
        ));

        // The rejected registration must not have written the aggregate — a
        // written-but-rejected asset would still brick the next boot.
        let key = AssetKey::new(
            UnderlyingSymbol::new("AAPL").unwrap(),
            Network::Ethereum,
        );
        let asset = store.load(&key).await.expect("load must succeed");
        assert!(asset.is_none(), "aggregate must not exist: {asset:?}");
    }

    #[traced_test]
    #[tokio::test]
    async fn test_add_asset_rejects_vault_shared_across_networks() {
        let pool = migrated_in_memory_pool().await;
        let store = setup_tokenized_asset_store(&pool).await;
        let shared_vault =
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        store
            .send(
                &AssetKey::new(
                    UnderlyingSymbol::new("AAPL").unwrap(),
                    Network::Base,
                ),
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    vault: shared_vault,
                },
            )
            .await
            .expect("base asset should add");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(store.clone())
            .manage(pool)
            .manage(ConfiguredNetworks::from_iter([
                Network::Base,
                Network::Ethereum,
            ]))
            .mount("/", routes![add_tokenized_asset]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "underlying": "MSFT",
            "token": "tMSFT",
            "network": "ethereum",
            "vault": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });

        let response = client
            .post("/tokenized-assets")
            .header(ContentType::JSON)
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["vault address already", "another network"]
        ));

        let key = AssetKey::new(
            UnderlyingSymbol::new("MSFT").unwrap(),
            Network::Ethereum,
        );
        let asset = store.load(&key).await.expect("load must succeed");
        assert!(asset.is_none(), "aggregate must not exist: {asset:?}");
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

        let key = AssetKey::new(
            UnderlyingSymbol::new("AAPL").unwrap(),
            Network::Base,
        );
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
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
            .manage(pool.clone())
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
            json!({
                "underlying": "AAPL",
                "status": "enabled",
                "vault_mode": "vault_direct"
            })
        );

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let underlying_store = setup_underlying_store(&pool).await;
        underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Freeze { underlying: underlying.clone() },
            )
            .await
            .expect("Failed to freeze underlying");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing underlying across all networks", "AAPL"]
        ));

        let after = client
            .get("/tokenized-assets/AAPL/status")
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
            json!({
                "underlying": "AAPL",
                "status": "frozen",
                "vault_mode": "vault_direct"
            })
        );

        // Unfreezing must flip the status back to `enabled` — the other half of
        // the guard's lifecycle, exercised through the same HTTP + projection
        // path.
        underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Unfreeze { underlying: underlying.clone() },
            )
            .await
            .expect("Failed to unfreeze underlying");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Unfreezing underlying across all networks", "AAPL"]
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
            json!({
                "underlying": "AAPL",
                "status": "enabled",
                "vault_mode": "vault_direct"
            })
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_get_detail_reflects_freeze() {
        let pool = migrated_in_memory_pool().await;
        let store = setup_tokenized_asset_store(&pool).await;

        let key = AssetKey::new(
            UnderlyingSymbol::new("AAPL").unwrap(),
            Network::Base,
        );
        store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
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
            .manage(pool.clone())
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

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let underlying_store = setup_underlying_store(&pool).await;
        underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Freeze { underlying: underlying.clone() },
            )
            .await
            .expect("Failed to freeze underlying");

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Freezing underlying across all networks", "AAPL"]
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

    /// Under a mixed config the status endpoint reports each asset's own
    /// mode tag: the per-asset orchestrator override for AAPL, the
    /// vault-direct default for MSFT — the liquidity bot's cue for which
    /// assets need a `MintAuthV1` before their mints can submit.
    #[tokio::test]
    async fn test_get_status_reports_vault_mode_per_asset_under_mixed_config() {
        let pool = migrated_in_memory_pool().await;
        let store = setup_tokenized_asset_store(&pool).await;

        for (underlying, token) in [("AAPL", "tAAPL"), ("MSFT", "tMSFT")] {
            let underlying = UnderlyingSymbol::new(underlying).unwrap();
            let key = AssetKey::new(underlying.clone(), Network::Base);
            store
                .send(
                    &key,
                    TokenizedAssetCommand::Add {
                        underlying,
                        token: TokenSymbol::new(token),
                        network: Network::Base,
                        vault: address!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                    },
                )
                .await
                .expect("Failed to add asset");
        }

        let config = Config {
            vault_mode_config: VaultModeConfig::new(
                HashMap::from([(
                    "AAPL".to_string(),
                    VaultModeKind::Orchestrator,
                )]),
                VaultModeKind::VaultDirect,
                HashMap::from([(
                    Network::Base,
                    address!("0xdddddddddddddddddddddddddddddddddddddddd"),
                )]),
            ),
            ..test_config()
        };

        let rocket = rocket::build()
            .manage(config)
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(pool)
            .mount("/", routes![get_tokenized_asset_status]);
        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        for (underlying, expected_mode) in
            [("AAPL", "orchestrator"), ("MSFT", "vault_direct")]
        {
            let response = client
                .get(format!("/tokenized-assets/{underlying}/status"))
                .header(internal_api_key())
                .remote("127.0.0.1:8000".parse().unwrap())
                .dispatch()
                .await;

            assert_eq!(response.status(), Status::Ok);
            let body: Value =
                response.into_json().await.expect("valid JSON response");
            assert_eq!(
                body,
                json!({
                    "underlying": underlying,
                    "status": "enabled",
                    "vault_mode": expected_mode
                }),
                "unexpected status body for {underlying}"
            );
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_get_status_db_error_returns_500() {
        let pool = migrated_in_memory_pool().await;

        // A listing row gates existence (any view_id under this underlying),
        // and a corrupt `underlying_view` row forces the freeze-status load to
        // error, exercising the handler's error -> 500 mapping and the
        // operator-facing log that is the only signal for this failure mode.
        sqlx::query(
            r#"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES ('AAPL:base', 1, '{"Live": {"bad_field": 1}}')
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to insert listing view row");

        sqlx::query(
            r#"
            INSERT INTO underlying_view (view_id, version, payload)
            VALUES ('AAPL', 1, '{"Live": {"bad_field": 1}}')
            "#,
        )
        .execute(&pool)
        .await
        .expect("Failed to insert malformed underlying view row");

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
            &["Failed to load underlying freeze status"]
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

    #[tokio::test]
    async fn test_get_status_non_live_listing_row_still_answers() {
        let pool = migrated_in_memory_pool().await;

        // A listing row whose `$.Live` is null is a known-but-indeterminate
        // LISTING, but the freeze answer comes from the separate
        // `underlying_view`, so the status endpoint still serves it: the row's
        // existence proves the underlying is known (not 404), and with no
        // freeze stream the underlying is enabled by definition.
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
            .get("/tokenized-assets/AAPL/status")
            .header(internal_api_key())
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body: Value =
            response.into_json().await.expect("valid JSON response");
        assert_eq!(
            body,
            json!({
                "underlying": "AAPL",
                "status": "enabled",
                "vault_mode": "vault_direct"
            })
        );
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
