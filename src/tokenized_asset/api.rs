use rocket::get;
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::error;

use super::{Network, TokenSymbol, UnderlyingSymbol, view::TokenizedAssetView};
use crate::auth::IssuerAuth;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenizedAssetResponse {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
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
            } => Some(TokenizedAssetResponse { underlying, token, network }),
            TokenizedAssetView::Unavailable => None,
        })
        .collect();

    Ok(Json(TokenizedAssetsListResponse { tokens }))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use chrono::Utc;
    use rocket::http::{Header, Status};
    use rocket::routes;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;
    use crate::alpaca::service::AlpacaConfig;
    use crate::auth::{FailedAuthRateLimiter, IpWhitelist};
    use crate::config::{Config, LogLevel};

    fn test_config() -> Config {
        use alloy::primitives::{B256, address};
        use url::Url;

        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
            private_key: B256::ZERO,
            vault: address!("0x1111111111111111111111111111111111111111"),
            bot: address!("0x2222222222222222222222222222222222222222"),
            issuer_api_key: "test-key-12345678901234567890123456".to_string(),
            alpaca_ip_ranges: IpWhitelist::single(
                "127.0.0.1/32".parse().expect("Valid IP range"),
            ),
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
        }
    }

    #[tokio::test]
    async fn test_list_tokenized_assets_returns_enabled_assets() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let enabled_view = TokenizedAssetView::Asset {
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::new("base"),
            vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            enabled: true,
            added_at: Utc::now(),
        };

        let disabled_view = TokenizedAssetView::Asset {
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            network: Network::new("base"),
            vault: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            enabled: false,
            added_at: Utc::now(),
        };

        let enabled_payload =
            serde_json::to_string(&enabled_view).expect("Failed to serialize");
        let disabled_payload =
            serde_json::to_string(&disabled_view).expect("Failed to serialize");

        sqlx::query!(
            r"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            "AAPL",
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
            "TSLA",
            disabled_payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert disabled view");

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
            .header(Header::new("X-Real-IP", "127.0.0.1"))
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
        assert_eq!(response_body.tokens[0].network, Network::new("base"));
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
            .header(Header::new("X-Real-IP", "127.0.0.1"))
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
}
