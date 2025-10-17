mod cmd;
mod event;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) use cmd::AccountCommand;
pub(crate) use event::AccountEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Email(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlpacaAccountNumber(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClientId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LinkedAccountStatus {
    Active,
    Inactive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Account {
    NotLinked,
    Linked(LinkedAccount),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LinkedAccount {
    pub(crate) client_id: ClientId,
    pub(crate) email: Email,
    pub(crate) alpaca_account: AlpacaAccountNumber,
    pub(crate) status: LinkedAccountStatus,
    pub(crate) linked_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

impl Default for Account {
    fn default() -> Self {
        Self::NotLinked
    }
}

#[async_trait]
impl Aggregate for Account {
    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = AccountError;
    type Services = ();

    fn aggregate_type() -> String {
        "Account".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::LinkAccount { email, alpaca_account } => {
                if !is_valid_email(&email.0) {
                    return Err(AccountError::InvalidEmail { email: email.0 });
                }

                if matches!(self, Self::Linked(_)) {
                    return Err(AccountError::AccountAlreadyExists {
                        email: email.0,
                    });
                }

                let client_id = ClientId(Uuid::new_v4().to_string());
                let now = Utc::now();

                Ok(vec![AccountEvent::AccountLinked {
                    client_id,
                    email,
                    alpaca_account,
                    linked_at: now,
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            AccountEvent::AccountLinked {
                client_id,
                email,
                alpaca_account,
                linked_at,
            } => {
                *self = Self::Linked(LinkedAccount {
                    client_id,
                    email,
                    alpaca_account,
                    status: LinkedAccountStatus::Active,
                    linked_at,
                    updated_at: linked_at,
                });
            }
        }
    }
}

fn is_valid_email(email: &str) -> bool {
    email.contains('@')
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AccountError {
    #[error("Invalid email format: {email}")]
    InvalidEmail { email: String },

    #[error("Account already exists for email: {email}")]
    AccountAlreadyExists { email: String },
}

#[derive(Debug, Deserialize)]
pub(crate) struct AccountLinkRequest {
    #[serde(rename = "email")]
    pub(crate) _email: Email,
    #[serde(rename = "account")]
    pub(crate) _account: AlpacaAccountNumber,
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
