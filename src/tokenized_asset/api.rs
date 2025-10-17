use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};

use super::{Network, TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenizedAsset {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenizedAssetsResponse {
    pub(crate) assets: Vec<TokenizedAsset>,
}

#[get("/tokenized-assets")]
pub(crate) async fn list_tokenized_assets() -> Json<TokenizedAssetsResponse> {
    let assets = vec![
        TokenizedAsset {
            underlying: UnderlyingSymbol("AAPL".to_string()),
            token: TokenSymbol("stAAPL".to_string()),
            network: Network("base".to_string()),
        },
        TokenizedAsset {
            underlying: UnderlyingSymbol("TSLA".to_string()),
            token: TokenSymbol("stTSLA".to_string()),
            network: Network("base".to_string()),
        },
        TokenizedAsset {
            underlying: UnderlyingSymbol("NVDA".to_string()),
            token: TokenSymbol("stNVDA".to_string()),
            network: Network("base".to_string()),
        },
    ];

    Json(TokenizedAssetsResponse { assets })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::http::Status;

    #[tokio::test]
    async fn test_list_tokenized_assets_returns_hardcoded_list() {
        let rocket = rocket::build().mount("/", routes![list_tokenized_assets]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/tokenized-assets").dispatch().await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: TokenizedAssetsResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(response_body.assets.len(), 3);

        assert_eq!(
            response_body.assets[0].underlying,
            UnderlyingSymbol("AAPL".to_string())
        );
        assert_eq!(
            response_body.assets[0].token,
            TokenSymbol("stAAPL".to_string())
        );
        assert_eq!(
            response_body.assets[0].network,
            Network("base".to_string())
        );

        assert_eq!(
            response_body.assets[1].underlying,
            UnderlyingSymbol("TSLA".to_string())
        );
        assert_eq!(
            response_body.assets[1].token,
            TokenSymbol("stTSLA".to_string())
        );
        assert_eq!(
            response_body.assets[1].network,
            Network("base".to_string())
        );

        assert_eq!(
            response_body.assets[2].underlying,
            UnderlyingSymbol("NVDA".to_string())
        );
        assert_eq!(
            response_body.assets[2].token,
            TokenSymbol("stNVDA".to_string())
        );
        assert_eq!(
            response_body.assets[2].network,
            Network("base".to_string())
        );
    }
}
