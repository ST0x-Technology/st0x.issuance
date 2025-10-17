use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};

use super::{IssuerRequestId, Quantity, TokenizationRequestId};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MintRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) qty: Quantity,
    pub(crate) underlying: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MintResponse {
    pub(crate) issuer_request_id: IssuerRequestId,
}

#[post("/inkind/issuance", data = "<request>")]
pub(crate) async fn initiate_mint(
    request: Json<MintRequest>,
) -> Json<MintResponse> {
    let _request = request.into_inner();

    Json(MintResponse {
        issuer_request_id: IssuerRequestId::new("iss-stub-123"),
    })
}

#[cfg(test)]
mod tests {
    use rocket::http::{ContentType, Status};

    use super::*;

    #[tokio::test]
    async fn test_initiate_mint_returns_issuer_request_id() {
        let rocket = rocket::build().mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying": "AAPL"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            mint_response.issuer_request_id,
            IssuerRequestId::new("iss-stub-123")
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_malformed_request() {
        let rocket = rocket::build().mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "invalid_field": "value"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[tokio::test]
    async fn test_initiate_mint_accepts_decimal_quantity() {
        let rocket = rocket::build().mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-456",
            "qty": "99.999",
            "underlying": "TSLA"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            mint_response.issuer_request_id,
            IssuerRequestId::new("iss-stub-123")
        );
    }
}
