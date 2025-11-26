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

#[tracing::instrument(skip(_auth, pool))]
#[get("/tokenized-assets")]
pub(crate) async fn list_tokenized_assets(
    _auth: IssuerAuth,
    pool: &rocket::State<Pool<Sqlite>>,
) -> Result<Json<Vec<TokenizedAssetResponse>>, rocket::http::Status> {
    let views =
        super::view::list_enabled_assets(pool.inner()).await.map_err(|e| {
            error!("Failed to list enabled assets: {e}");
            rocket::http::Status::InternalServerError
        })?;

    let assets = views
        .into_iter()
        .filter_map(|view| match view {
            TokenizedAssetView::Asset {
                underlying, token, network, ..
            } => Some(TokenizedAssetResponse { underlying, token, network }),
            TokenizedAssetView::Unavailable => None,
        })
        .collect();

    Ok(Json(assets))
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
    use crate::auth::FailedAuthRateLimiter;
    use crate::config::{Config, LogLevel};

    fn test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: None,
            private_key: None,
            vault_address: None,
            redemption_wallet: None,
            issuer_api_key: "test-key-12345678901234567890123456".to_string(),
            alpaca_ip_ranges: vec![
                "127.0.0.1/32".parse().expect("Valid IP range"),
            ],
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
                "Authorization",
                "Bearer test-key-12345678901234567890123456",
            ))
            .header(Header::new("X-Real-IP", "127.0.0.1"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let assets: Vec<TokenizedAssetResponse> = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].underlying, UnderlyingSymbol::new("AAPL"));
        assert_eq!(assets[0].token, TokenSymbol::new("tAAPL"));
        assert_eq!(assets[0].network, Network::new("base"));
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
                "Authorization",
                "Bearer test-key-12345678901234567890123456",
            ))
            .header(Header::new("X-Real-IP", "127.0.0.1"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let assets: Vec<TokenizedAssetResponse> = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert!(assets.is_empty());
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
