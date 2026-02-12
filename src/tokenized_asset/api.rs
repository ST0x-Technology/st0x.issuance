use alloy::primitives::Address;
use cqrs_es::AggregateError;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::error;

use super::{
    Network, TokenSymbol, TokenizedAssetCommand, UnderlyingSymbol,
    view::TokenizedAssetView,
};
use crate::auth::{InternalAuth, IssuerAuth};

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
            error!("Failed to list enabled assets: {e}");
            rocket::http::Status::InternalServerError
        })?;

    let tokens = views
        .into_iter()
        .filter_map(|view| match view {
            TokenizedAssetView::Asset {
                underlying, token, network, ..
            } => Some(TokenizedAssetResponse {
                underlying,
                token,
                networks: vec![network],
            }),
            TokenizedAssetView::Unavailable => None,
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

#[tracing::instrument(skip(_auth, cqrs), fields(
    underlying = %request.underlying,
    token = %request.token,
    network = %request.network,
    vault = ?request.vault
))]
#[post("/tokenized-assets", format = "json", data = "<request>")]
pub(crate) async fn add_tokenized_asset(
    _auth: InternalAuth,
    cqrs: &rocket::State<crate::TokenizedAssetCqrs>,
    request: Json<AddTokenizedAssetRequest>,
) -> Result<(Status, Json<AddTokenizedAssetResponse>), Status> {
    let command = TokenizedAssetCommand::Add {
        underlying: request.underlying.clone(),
        token: request.token.clone(),
        network: request.network.clone(),
        vault: request.vault,
    };

    cqrs.execute(&request.underlying.0, command)
        .await
        .or_else(|err| match err {
            AggregateError::AggregateConflict => Ok(()),
            _ => Err(err),
        })
        .map_err(|err| {
            error!("Failed to add tokenized asset: {err}");
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
    use alloy::primitives::B256;
    use cqrs_es::persist::GenericQuery;
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use url::Url;

    use super::*;
    use crate::alpaca::service::AlpacaConfig;
    use crate::auth::{FailedAuthRateLimiter, test_auth_config};
    use crate::config::{Config, LogLevel};
    use crate::fireblocks::SignerConfig;
    use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};

    fn test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
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

        let cqrs = setup_tokenized_asset_cqrs(&pool);

        cqrs.execute(
            "AAPL",
            TokenizedAssetCommand::Add {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
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

    fn setup_tokenized_asset_cqrs(
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> crate::TokenizedAssetCqrs {
        let view_repo = Arc::new(SqliteViewRepository::<
            TokenizedAssetView,
            TokenizedAsset,
        >::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));

        let query = GenericQuery::new(view_repo);

        sqlite_cqrs(pool.clone(), vec![Box::new(query)], ())
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

        let cqrs = setup_tokenized_asset_cqrs(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(cqrs)
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

        let cqrs = setup_tokenized_asset_cqrs(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(cqrs)
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

        let cqrs = setup_tokenized_asset_cqrs(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(cqrs)
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

        let cqrs = setup_tokenized_asset_cqrs(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(cqrs)
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
}
