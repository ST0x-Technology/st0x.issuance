use rocket::get;
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::error;

use super::{Network, TokenSymbol, UnderlyingSymbol, view::TokenizedAssetView};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenizedAssetResponse {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
}

#[get("/tokenized-assets")]
pub(crate) async fn list_tokenized_assets(
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
    use rocket::http::Status;
    use rocket::routes;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

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
            vault_address: address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            enabled: true,
            added_at: Utc::now(),
        };

        let disabled_view = TokenizedAssetView::Asset {
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            network: Network::new("base"),
            vault_address: address!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
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
            .manage(pool)
            .mount("/", routes![list_tokenized_assets]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/tokenized-assets").dispatch().await;

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
            .manage(pool)
            .mount("/", routes![list_tokenized_assets]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/tokenized-assets").dispatch().await;

        assert_eq!(response.status(), Status::Ok);

        let assets: Vec<TokenizedAssetResponse> = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert!(assets.is_empty());
    }
}
