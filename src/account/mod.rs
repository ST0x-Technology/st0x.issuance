mod cmd;
mod event;
mod view;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) use cmd::AccountCommand;
pub(crate) use event::AccountEvent;
pub(crate) use view::{AccountView, find_by_email};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Email(String);

impl Email {
    pub(crate) fn new(email: String) -> Result<Self, AccountError> {
        if !email.contains('@') {
            return Err(AccountError::InvalidEmail { email });
        }
        Ok(Self(email))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Email {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

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
    Linked {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
        status: LinkedAccountStatus,
        linked_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    },
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
                if matches!(self, Self::Linked { .. }) {
                    return Err(AccountError::AccountAlreadyExists {
                        email: email.as_str().to_string(),
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
                *self = Self::Linked {
                    client_id,
                    email,
                    alpaca_account,
                    status: LinkedAccountStatus::Active,
                    linked_at,
                    updated_at: linked_at,
                };
            }
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum AccountError {
    #[error("Invalid email format: {email}")]
    InvalidEmail { email: String },

    #[error("Account already exists for email: {email}")]
    AccountAlreadyExists { email: String },
}

#[derive(Debug, Deserialize)]
pub(crate) struct AccountLinkRequest {
    pub(crate) email: Email,
    pub(crate) account: AlpacaAccountNumber,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AccountLinkResponse {
    pub(crate) client_id: ClientId,
}

#[post("/accounts/connect", format = "json", data = "<request>")]
pub(crate) async fn connect_account(
    cqrs: &rocket::State<crate::AccountCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<AccountLinkRequest>,
) -> Result<Json<AccountLinkResponse>, rocket::http::Status> {
    let command = AccountCommand::LinkAccount {
        email: request.email.clone(),
        alpaca_account: request.account.clone(),
    };

    let aggregate_id = request.email.as_str();

    cqrs.execute(aggregate_id, command)
        .await
        .map_err(|_| rocket::http::Status::Conflict)?;

    let account_view = find_by_email(pool.inner(), &request.email)
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?
        .ok_or(rocket::http::Status::InternalServerError)?;

    Ok(Json(AccountLinkResponse { client_id: account_view.client_id }))
}

#[cfg(test)]
mod tests {
    use super::{
        Account, AccountCommand, AccountError, AccountEvent,
        AccountLinkResponse, AlpacaAccountNumber, Email, LinkedAccountStatus,
    };
    use cqrs_es::{Aggregate, test::TestFramework};
    use rocket::http::{ContentType, Status};

    type AccountTestFramework = TestFramework<Account>;

    #[test]
    fn test_link_account_creates_new_account() {
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        let validator = AccountTestFramework::with(())
            .given_no_previous_events()
            .when(AccountCommand::LinkAccount {
                email: email.clone(),
                alpaca_account: alpaca_account.clone(),
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    AccountEvent::AccountLinked {
                        client_id,
                        email: event_email,
                        alpaca_account: event_alpaca,
                        linked_at,
                    } => {
                        assert!(!client_id.0.is_empty());
                        assert_eq!(event_email, &email);
                        assert_eq!(event_alpaca, &alpaca_account);
                        assert!(linked_at.timestamp() > 0);
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_email_smart_constructor_validates() {
        let result = Email::new("not-an-email".to_string());

        assert!(matches!(
            result,
            Err(AccountError::InvalidEmail { email }) if email == "not-an-email"
        ));

        let valid_result = Email::new("user@example.com".to_string());
        assert!(valid_result.is_ok());
    }

    #[test]
    fn test_link_account_when_already_linked_returns_error() {
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        AccountTestFramework::with(())
            .given(vec![AccountEvent::AccountLinked {
                client_id: super::ClientId("existing-client-id".to_string()),
                email: email.clone(),
                alpaca_account: AlpacaAccountNumber("ALPACA456".to_string()),
                linked_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::LinkAccount { email, alpaca_account })
            .then_expect_error(AccountError::AccountAlreadyExists {
                email: "user@example.com".to_string(),
            });
    }

    #[test]
    fn test_apply_account_linked_updates_state() {
        let mut account = Account::default();

        assert!(matches!(account, Account::NotLinked));

        let client_id = super::ClientId("test-client-123".to_string());
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = chrono::Utc::now();

        account.apply(AccountEvent::AccountLinked {
            client_id: client_id.clone(),
            email: email.clone(),
            alpaca_account: alpaca_account.clone(),
            linked_at,
        });

        match account {
            Account::Linked {
                client_id: linked_client_id,
                email: linked_email,
                alpaca_account: linked_alpaca,
                status,
                linked_at: linked_at_timestamp,
                updated_at,
            } => {
                assert_eq!(linked_client_id, client_id);
                assert_eq!(linked_email, email);
                assert_eq!(linked_alpaca, alpaca_account);
                assert_eq!(status, LinkedAccountStatus::Active);
                assert_eq!(linked_at_timestamp, linked_at);
                assert_eq!(updated_at, linked_at);
            }
            Account::NotLinked => panic!("Expected account to be linked"),
        }
    }

    #[tokio::test]
    async fn test_connect_account_returns_client_id() {
        use cqrs_es::persist::GenericQuery;
        use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
        use sqlx::sqlite::SqlitePoolOptions;
        use std::sync::Arc;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo =
            Arc::new(SqliteViewRepository::<super::AccountView, Account>::new(
                pool.clone(),
                "account_view".to_string(),
            ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![super::connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "customer@firm.com",
            "account": "alpaca-account-123"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: AccountLinkResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert!(!response_body.client_id.0.is_empty());
    }
}
