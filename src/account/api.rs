use alloy::primitives::Address;
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
    AlpacaAccountNumber, ClientId, Email, view::find_by_client_id,
    view::find_by_email,
};

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
    Database(#[from] super::view::AccountViewError),

    #[error("Command execution failed: {0}")]
    CommandFailed(#[from] cqrs_es::AggregateError<super::AccountError>),
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
pub(crate) struct AccountLinkRequest {
    pub(crate) email: Email,
    pub(crate) account: AlpacaAccountNumber,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountLinkResponse {
    pub client_id: ClientId,
}

#[tracing::instrument(skip(cqrs, pool), fields(
    email = %request.email.0,
    account = %request.account.0
))]
#[post("/accounts/connect", format = "json", data = "<request>")]
pub(crate) async fn connect_account(
    cqrs: &rocket::State<crate::AccountCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<AccountLinkRequest>,
) -> Result<Json<AccountLinkResponse>, rocket::http::Status> {
    if find_by_email(pool.inner(), &request.email)
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?
        .is_some()
    {
        return Err(rocket::http::Status::Conflict);
    }

    let client_id = ClientId::new();

    let command = super::AccountCommand::Link {
        client_id,
        email: request.email.clone(),
        alpaca_account: request.account.clone(),
    };

    let aggregate_id = client_id.to_string();
    cqrs.execute(&aggregate_id, command)
        .await
        .map_err(|_| rocket::http::Status::Conflict)?;

    let account_view = find_by_client_id(pool.inner(), &client_id)
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?
        .ok_or(rocket::http::Status::InternalServerError)?;

    let super::AccountView::Account { client_id, .. } = account_view else {
        return Err(rocket::http::Status::InternalServerError);
    };

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

#[tracing::instrument(skip(cqrs, pool), fields(
    client_id = %client_id,
    wallet = ?request.wallet
))]
#[post("/accounts/<client_id>/wallets", format = "json", data = "<request>")]
pub(crate) async fn whitelist_wallet(
    cqrs: &rocket::State<crate::AccountCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    client_id: ClientId,
    request: Json<WhitelistWalletRequest>,
) -> Result<Json<WhitelistWalletResponse>, ApiError> {
    let account_view = find_by_client_id(pool.inner(), &client_id)
        .await?
        .ok_or(ApiError::AccountNotFound)?;

    let super::AccountView::Account { .. } = account_view else {
        return Err(ApiError::AccountNotFound);
    };

    let command =
        super::AccountCommand::WhitelistWallet { wallet: request.wallet };

    let aggregate_id = client_id.0.to_string();
    cqrs.execute(&aggregate_id, command).await?;

    Ok(Json(WhitelistWalletResponse { success: true }))
}

#[cfg(test)]
mod tests {
    use cqrs_es::persist::GenericQuery;
    use rocket::http::{ContentType, Status};
    use rocket::routes;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::super::Account;
    use super::*;

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
            super::super::AccountView,
            Account,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

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

        let response_body: AccountLinkResponse =
            response.into_json().await.expect("valid JSON response");

        assert_ne!(response_body.client_id.0, Uuid::nil());
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
            super::super::AccountView,
            Account,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
            .manage(account_cqrs)
            .manage(pool)
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "email": "duplicate@example.com",
            "account": "ALPACA789"
        });

        let response1 = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response1.status(), Status::Ok);

        let response2 = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response2.status(), Status::Conflict);
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
            super::super::AccountView,
            Account,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
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
            super::super::AccountView,
            Account,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
            .manage(account_cqrs)
            .manage(pool.clone())
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let email = "events@example.com";
        let request_body = serde_json::json!({
            "email": email,
            "account": "ALPACA001"
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: AccountLinkResponse =
            response.into_json().await.expect("valid JSON response");

        let client_id_str = response_body.client_id.to_string();

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

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].aggregate_id, client_id_str);
        assert_eq!(events[0].event_type, "AccountEvent::Linked");
        assert_eq!(events[0].sequence, 1);
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
            super::super::AccountView,
            Account,
        >::new(
            pool.clone(),
            "account_view".to_string(),
        ));

        let account_query = GenericQuery::new(account_view_repo);

        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let rocket = rocket::build()
            .manage(account_cqrs)
            .manage(pool.clone())
            .mount("/", routes![connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let email = "view@example.com";
        let alpaca_account = "ALPACA002";

        let request_body = serde_json::json!({
            "email": email,
            "account": alpaca_account
        });

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let response_body: AccountLinkResponse =
            response.into_json().await.expect("valid JSON response");

        let view = find_by_client_id(&pool, &response_body.client_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        let super::super::AccountView::Account {
            client_id,
            email: view_email,
            alpaca_account: view_alpaca_account,
            whitelisted_wallets,
            status,
            ..
        } = view
        else {
            panic!("Expected Account, got Unavailable");
        };

        assert_eq!(client_id, response_body.client_id);
        assert_eq!(view_email.as_str(), email);
        assert_eq!(view_alpaca_account.0, alpaca_account);
        assert!(whitelisted_wallets.is_empty());
        assert_eq!(status, super::super::LinkedAccountStatus::Active);
    }
}
