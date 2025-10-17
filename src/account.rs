use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Email(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AlpacaAccountNumber(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClientId(pub(crate) String);

#[derive(Debug, Deserialize)]
pub(crate) struct AccountLinkRequest {
    pub(crate) email: Email,
    pub(crate) account: AlpacaAccountNumber,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AccountLinkResponse {
    pub(crate) client_id: ClientId,
}

#[post("/accounts/connect", format = "json", data = "<_request>")]
pub(crate) fn connect_account(
    _request: Json<AccountLinkRequest>,
) -> Json<AccountLinkResponse> {
    Json(AccountLinkResponse {
        client_id: ClientId("stub-client-id-123".to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::AccountLinkResponse;
    use rocket::http::{ContentType, Status};
    use rocket::local::blocking::Client;

    fn rocket() -> rocket::Rocket<rocket::Build> {
        rocket::build().mount("/", routes![super::connect_account])
    }

    #[test]
    fn test_connect_account_returns_client_id() {
        let client = Client::tracked(rocket()).expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "customer@firm.com",
            "account": "alpaca-account-123"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch();

        assert_eq!(response.status(), Status::Ok);

        let response_body: AccountLinkResponse = serde_json::from_str(
            &response.into_string().expect("valid response body"),
        )
        .expect("valid JSON response");

        assert!(!response_body.client_id.0.is_empty());
    }
}
