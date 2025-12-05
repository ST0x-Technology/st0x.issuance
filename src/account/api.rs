use alloy::primitives::Address;
use cqrs_es::AggregateError;
use rocket::Request;
use rocket::http::Status;
use rocket::post;
use rocket::request::FromParam;
use rocket::response::Responder;
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use tracing::error;
use uuid::Uuid;

use super::{
    AccountCommand, AccountError, AccountViewError, AlpacaAccountNumber,
    ClientId, Email, find_by_client_id, find_by_email,
};
use crate::auth::{InternalAuth, IssuerAuth};

impl<'a> FromParam<'a> for ClientId {
    type Error = uuid::Error;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        Uuid::parse_str(param).map(ClientId)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiError {
    #[error("Account not found")]
    AccountNotFound,
    #[error("Database error: {0}")]
    Database(#[from] AccountViewError),
    #[error("Command execution failed: {0}")]
    CommandFailed(#[from] AggregateError<AccountError>),
}

impl<'r> Responder<'r, 'static> for ApiError {
    fn respond_to(
        self,
        _: &'r Request<'_>,
    ) -> rocket::response::Result<'static> {
        let status = match self {
            Self::AccountNotFound => Status::NotFound,
            Self::Database(_) | Self::CommandFailed(_) => {
                Status::InternalServerError
            }
        };

        let message = self.to_string();
        error!("{message}");

        rocket::Response::build()
            .status(status)
            .sized_body(message.len(), std::io::Cursor::new(message))
            .ok()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct RegisterAccountRequest {
    pub(crate) email: Email,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterAccountResponse {
    pub client_id: ClientId,
}

#[tracing::instrument(skip(_auth, cqrs, pool), fields(email = %request.email.0))]
#[post("/accounts", format = "json", data = "<request>")]
pub(crate) async fn register_account(
    _auth: InternalAuth,
    cqrs: &rocket::State<crate::AccountCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<RegisterAccountRequest>,
) -> Result<Json<RegisterAccountResponse>, rocket::http::Status> {
    if find_by_email(pool.inner(), &request.email)
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?
        .is_some()
    {
        return Err(rocket::http::Status::Conflict);
    }

    let client_id = ClientId::new();
    let register_command =
        AccountCommand::Register { client_id, email: request.email.clone() };

    let aggregate_id = client_id.to_string();
    if let Err(e) = cqrs.execute(&aggregate_id, register_command).await {
        return Err(map_cqrs_error_to_status(&e));
    }

    Ok(Json(RegisterAccountResponse { client_id }))
}

fn map_cqrs_error_to_status(
    err: &AggregateError<super::AccountError>,
) -> Status {
    match err {
        AggregateError::DatabaseConnectionError(inner)
            if inner.to_string().contains("UNIQUE constraint failed") =>
        {
            Status::Conflict
        }
        AggregateError::UserError(_) => Status::BadRequest,
        AggregateError::AggregateConflict => Status::Conflict,
        _ => {
            error!("CQRS execute error: {err}");
            Status::InternalServerError
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AccountLinkRequest {
    pub(crate) email: Email,
    pub(crate) account: AlpacaAccountNumber,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountLinkResponse {
    pub client_id: ClientId,
}

#[tracing::instrument(skip(_auth, cqrs, pool), fields(
    email = %request.email.0,
    account = %request.account.0
))]
#[post("/accounts/connect", format = "json", data = "<request>")]
pub(crate) async fn connect_account(
    _auth: IssuerAuth,
    cqrs: &rocket::State<crate::AccountCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<AccountLinkRequest>,
) -> Result<Json<AccountLinkResponse>, rocket::http::Status> {
    let account = find_by_email(pool.inner(), &request.email)
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?
        .ok_or(rocket::http::Status::NotFound)?;

    if account.alpaca.is_some() {
        return Err(rocket::http::Status::Conflict);
    }

    let client_id = account.client_id;

    let link_command = AccountCommand::LinkToAlpaca {
        alpaca_account: request.account.clone(),
    };

    let aggregate_id = client_id.to_string();
    cqrs.execute(&aggregate_id, link_command)
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?;

    Ok(Json(AccountLinkResponse { client_id }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct WhitelistWalletRequest {
    pub(crate) wallet: Address,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WhitelistWalletResponse {
    pub success: bool,
}

#[tracing::instrument(skip(_auth, cqrs, pool), fields(
    client_id = %client_id,
    wallet = ?request.wallet
))]
#[post("/accounts/<client_id>/wallets", format = "json", data = "<request>")]
pub(crate) async fn whitelist_wallet(
    _auth: InternalAuth,
    cqrs: &rocket::State<crate::AccountCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    client_id: ClientId,
    request: Json<WhitelistWalletRequest>,
) -> Result<Json<WhitelistWalletResponse>, ApiError> {
    let account = find_by_client_id(pool.inner(), &client_id)
        .await?
        .ok_or(ApiError::AccountNotFound)?;

    if account.alpaca.is_none() {
        return Err(ApiError::AccountNotFound);
    }

    let command = AccountCommand::WhitelistWallet { wallet: request.wallet };

    let aggregate_id = client_id.0.to_string();
    cqrs.execute(&aggregate_id, command).await?;

    Ok(Json(WhitelistWalletResponse { success: true }))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, address};
    use cqrs_es::persist::GenericQuery;
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use url::Url;

    use super::*;
    use crate::account::Account;
    use crate::alpaca::service::AlpacaConfig;
    use crate::auth::{FailedAuthRateLimiter, test_auth_config};
    use crate::config::{Config, LogLevel};
    use crate::lifecycle::{Lifecycle, Never};

    fn test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
            private_key: B256::ZERO,
            vault: address!("0x1111111111111111111111111111111111111111"),
            bot: address!("0x2222222222222222222222222222222222222222"),
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
        }
    }

    async fn register_account(
        cqrs: &crate::AccountCqrs,
        email: &str,
    ) -> ClientId {
        let client_id = ClientId::new();
        let email =
            Email::new(email.to_string()).expect("Valid email for test");

        cqrs.execute(
            &client_id.to_string(),
            AccountCommand::Register { client_id, email },
        )
        .await
        .expect("Failed to register account");

        client_id
    }

    #[tokio::test]
    async fn test_connect_account_returns_client_id() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let email = "customer@firm.com";
        let client_id = register_account(&account_cqrs, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": email,
            "account": "alpaca-account-123"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: AccountLinkResponse =
            response.into_json().await.expect("valid JSON response");

        assert_eq!(response_body.client_id, client_id);
    }

    #[tokio::test]
    async fn test_duplicate_account_link_returns_409() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let email = "duplicate@example.com";
        register_account(&account_cqrs, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": email,
            "account": "ALPACA789"
        });

        let auth_header =
            Header::new("X-API-KEY", "test-key-12345678901234567890123456");

        let response1 = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(auth_header.clone())
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response1.status(), Status::Ok);

        let response2 = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(auth_header)
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response2.status(), Status::Conflict);
    }

    #[tokio::test]
    async fn test_connect_account_when_not_registered_returns_404() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "nonexistent@example.com",
            "account": "ALPACA999"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::NotFound);
    }

    #[tokio::test]
    async fn test_invalid_email_format_returns_400() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "not-an-email",
            "account": "ALPACA999"
        });

        let response = client
            .post("/accounts/connect")
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
    async fn test_events_are_persisted_correctly() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let email = "events@example.com";
        let client_id = register_account(&account_cqrs, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool.clone())
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": email,
            "account": "ALPACA001"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let client_id_str = client_id.to_string();

        let events = sqlx::query!(
            r"
            SELECT aggregate_id, event_type, sequence
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'Account'
            ORDER BY sequence
            ",
            client_id_str
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query events");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].aggregate_id, client_id_str);
        assert_eq!(events[0].event_type, "AccountEvent::Registered");
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].event_type, "AccountEvent::LinkedToAlpaca");
        assert_eq!(events[1].sequence, 2);
    }

    #[tokio::test]
    async fn test_views_are_updated_correctly() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let email = "view@example.com";
        let client_id = register_account(&account_cqrs, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool.clone())
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let alpaca_account = "ALPACA002";

        let request_body = serde_json::json!({
            "email": email,
            "account": alpaca_account
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let account = find_by_client_id(&pool, &client_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        assert_eq!(account.client_id, client_id);
        assert_eq!(account.email.as_str(), email);
        assert_eq!(
            account.alpaca.as_ref().expect("Should be linked").0,
            alpaca_account
        );
        assert!(account.whitelisted_wallets.is_empty());
    }

    #[tokio::test]
    async fn test_connect_account_without_auth_returns_401() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "customer@firm.com",
            "account": "alpaca-account-123",
            "wallet": "0x1111111111111111111111111111111111111111"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[tokio::test]
    async fn test_connect_account_with_wrong_ip_returns_403() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "customer@firm.com",
            "account": "alpaca-account-123",
            "wallet": "0x1111111111111111111111111111111111111111"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("8.8.8.8:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Forbidden);
    }

    #[tokio::test]
    async fn test_register_account_returns_client_id() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool.clone())
            .mount("/", routes![super::register_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let email = "newuser@example.com";
        let request_body = serde_json::json!({
            "email": email
        });

        let response = client
            .post("/accounts")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: RegisterAccountResponse =
            response.into_json().await.expect("valid JSON response");

        let account = find_by_client_id(&pool, &response_body.client_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        assert_eq!(account.client_id, response_body.client_id);
        assert_eq!(account.email.as_str(), email);
        assert!(account.alpaca.is_none(), "Should not be linked yet");
    }

    #[tokio::test]
    async fn test_register_duplicate_email_returns_409() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let email = "duplicate@example.com";
        register_account(&account_cqrs, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![super::register_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": email
        });

        let response = client
            .post("/accounts")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Conflict);
    }

    #[tokio::test]
    async fn test_register_invalid_email_returns_422() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let account_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<Account, Never>,
            Lifecycle<Account, Never>,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![super::register_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "not-an-email"
        });

        let response = client
            .post("/accounts")
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
}
