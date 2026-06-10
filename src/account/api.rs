use alloy::primitives::Address;
use event_sorcery::{AggregateError, LifecycleError, Store};
use rocket::Request;
use rocket::http::Status;
use rocket::request::FromParam;
use rocket::response::Responder;
use rocket::serde::json::Json;
use rocket::{delete, post};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

use super::{
    Account, AccountCommand, AccountView, AlpacaAccountNumber, ClientId, Email,
    view::AccountViewError, view::find_by_client_id, view::find_by_email,
};
use crate::auth::{InternalAuth, IssuerAuth};

impl<'a> FromParam<'a> for ClientId {
    type Error = uuid::Error;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        Uuid::parse_str(param).map(ClientId)
    }
}

pub(crate) struct WalletParam(Address);

impl<'a> FromParam<'a> for WalletParam {
    type Error = alloy::hex::FromHexError;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        param.parse::<Address>().map(WalletParam)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiError {
    #[error("Account not found")]
    AccountNotFound,
    #[error("Account view error: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Aggregate error: {0}")]
    Aggregate(#[from] AggregateError<LifecycleError<Account>>),
}

impl<'r> Responder<'r, 'static> for ApiError {
    fn respond_to(
        self,
        _: &'r Request<'_>,
    ) -> rocket::response::Result<'static> {
        let status = match self {
            Self::AccountNotFound => Status::NotFound,
            Self::AccountView(_) | Self::Aggregate(_) => {
                Status::InternalServerError
            }
        };

        let message = self.to_string();
        error!(target: "account", "{message}");

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

#[tracing::instrument(skip(_auth, store, pool), fields(email = %request.email.0))]
#[post("/accounts", format = "json", data = "<request>")]
pub(crate) async fn register_account(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<Account>>>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<RegisterAccountRequest>,
) -> Result<Json<RegisterAccountResponse>, rocket::http::Status> {
    // Atomically claim the email before committing any event. The unique PK on
    // account_emails rejects a concurrent or repeat registration race-free,
    // unlike the account_view index whose violation the projection swallows
    // after the event has already committed.
    let claimed = sqlx::query!(
        "INSERT OR IGNORE INTO account_emails (email) VALUES (?)",
        request.email.0
    )
    .execute(pool.inner())
    .await
    .map_err(|_| rocket::http::Status::InternalServerError)?;

    if claimed.rows_affected() == 0 {
        return Err(rocket::http::Status::Conflict);
    }

    let client_id = ClientId::new();
    let register_command =
        AccountCommand::Register { client_id, email: request.email.clone() };

    if let Err(err) = store.send(&client_id, register_command).await {
        // Release the claim so a transient failure doesn't permanently block
        // re-registration of this email.
        if let Err(rollback_err) = sqlx::query!(
            "DELETE FROM account_emails WHERE email = ?",
            request.email.0
        )
        .execute(pool.inner())
        .await
        {
            error!(target: "account", email = %request.email.0,
                error = %rollback_err,
                "Failed to release the email claim after a failed \
                 registration; the email stays claimed (every retry gets 409) \
                 until its account_emails row is deleted manually"
            );
        }
        return Err(map_cqrs_error_to_status(&err));
    }

    Ok(Json(RegisterAccountResponse { client_id }))
}

fn map_cqrs_error_to_status(
    err: &AggregateError<LifecycleError<Account>>,
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
            error!(target: "account", "CQRS execute error: {err}");
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

#[tracing::instrument(skip(_auth, store, pool), fields(
    email = %request.email.0,
    account = %request.account.0
))]
#[post("/accounts/connect", format = "json", data = "<request>")]
pub(crate) async fn connect_account(
    _auth: IssuerAuth,
    store: &rocket::State<Arc<Store<Account>>>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<AccountLinkRequest>,
) -> Result<Json<AccountLinkResponse>, rocket::http::Status> {
    let account_view = find_by_email(pool.inner(), &request.email)
        .await
        .map_err(|_| rocket::http::Status::InternalServerError)?
        .ok_or(rocket::http::Status::NotFound)?;

    let client_id = match account_view {
        AccountView::Registered { client_id, .. } => client_id,
        AccountView::LinkedToAlpaca { .. } => {
            return Err(rocket::http::Status::Conflict);
        }
    };

    let link_command = AccountCommand::LinkToAlpaca {
        alpaca_account: request.account.clone(),
    };

    store
        .send(&client_id, link_command)
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

#[tracing::instrument(skip(_auth, store, pool), fields(
    client_id = %client_id,
    wallet = ?request.wallet
))]
#[post("/accounts/<client_id>/wallets", format = "json", data = "<request>")]
pub(crate) async fn whitelist_wallet(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<Account>>>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    client_id: ClientId,
    request: Json<WhitelistWalletRequest>,
) -> Result<Json<WhitelistWalletResponse>, ApiError> {
    let account_view = find_by_client_id(pool.inner(), &client_id)
        .await?
        .ok_or(ApiError::AccountNotFound)?;

    let AccountView::LinkedToAlpaca { .. } = account_view else {
        return Err(ApiError::AccountNotFound);
    };

    let command = AccountCommand::WhitelistWallet { wallet: request.wallet };

    store.send(&client_id, command).await?;

    Ok(Json(WhitelistWalletResponse { success: true }))
}

#[tracing::instrument(skip(_auth, store, pool), fields(
    client_id = %client_id,
    wallet = %wallet.0
))]
#[delete("/accounts/<client_id>/wallets/<wallet>")]
pub(crate) async fn unwhitelist_wallet(
    _auth: InternalAuth,
    store: &rocket::State<Arc<Store<Account>>>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    client_id: ClientId,
    wallet: WalletParam,
) -> Result<Json<WhitelistWalletResponse>, ApiError> {
    let account_view = find_by_client_id(pool.inner(), &client_id)
        .await?
        .ok_or(ApiError::AccountNotFound)?;

    let AccountView::LinkedToAlpaca { .. } = account_view else {
        return Err(ApiError::AccountNotFound);
    };

    let command = AccountCommand::UnwhitelistWallet { wallet: wallet.0 };

    store.send(&client_id, command).await?;

    Ok(Json(WhitelistWalletResponse { success: true }))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, address};
    use event_sorcery::StoreBuilder;
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing::Level;
    use tracing_test::traced_test;
    use url::Url;

    use super::*;
    use crate::account::Account;
    use crate::alpaca::service::AlpacaConfig;
    use crate::auth::{FailedAuthRateLimiter, test_auth_config};
    use crate::config::{Config, LogLevel};
    use crate::fireblocks::SignerConfig;
    use crate::test_utils::logs_contain_at;

    fn test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").expect("Valid URL"),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            receipt_poll_interval: crate::RECEIPT_POLL_INTERVAL,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
            subgraph_url: Url::parse("http://localhost:0/subgraph")
                .expect("valid test URL"),
        }
    }

    async fn register_account(
        store: &Arc<Store<Account>>,
        pool: &sqlx::Pool<sqlx::Sqlite>,
        email: &str,
    ) -> ClientId {
        let client_id = ClientId::new();

        let email = Email::new(email).expect("Valid email for test");

        sqlx::query!(
            "INSERT OR IGNORE INTO account_emails (email) VALUES (?)",
            email.0
        )
        .execute(pool)
        .await
        .expect("Failed to claim email");

        store
            .send(&client_id, AccountCommand::Register { client_id, email })
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let email = "customer@firm.com";
        let client_id = register_account(&account_store, &pool, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let email = "duplicate@example.com";
        register_account(&account_store, &pool, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let email = "events@example.com";
        let client_id = register_account(&account_store, &pool, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let email = "view@example.com";
        let client_id = register_account(&account_store, &pool, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

        let view = find_by_client_id(&pool, &client_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        let AccountView::LinkedToAlpaca {
            client_id: view_client_id,
            email: view_email,
            alpaca_account: view_alpaca_account,
            whitelisted_wallets,
            ..
        } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}");
        };

        assert_eq!(view_client_id, client_id);
        assert_eq!(view_email.to_string(), email);
        assert_eq!(view_alpaca_account.0, alpaca_account);
        assert!(whitelisted_wallets.is_empty());
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(account_store)
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(account_store)
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

        let view = find_by_client_id(&pool, &response_body.client_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        let AccountView::Registered { client_id, email: view_email, .. } = view
        else {
            panic!("Expected Registered, got {view:?}");
        };

        assert_eq!(client_id, response_body.client_id);
        assert_eq!(view_email.to_string(), email);
    }

    // When the Registered event fails to commit AND the compensating claim
    // release also fails, the handler must log the orphaned claim loudly —
    // the email stays 409-claimed until the row is deleted manually. The
    // event-store failure is induced by dropping the events table; the
    // release failure by a trigger blocking deletes on account_emails.
    #[traced_test]
    #[tokio::test]
    async fn test_register_account_logs_when_claim_release_fails() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        sqlx::query("DROP TABLE events")
            .execute(&pool)
            .await
            .expect("Failed to drop events table");
        sqlx::query(
            "
            CREATE TRIGGER block_email_release
            BEFORE DELETE ON account_emails
            BEGIN
                SELECT RAISE(ABORT, 'release blocked');
            END
            ",
        )
        .execute(&pool)
        .await
        .expect("Failed to create delete-blocking trigger");

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
            .manage(pool.clone())
            .mount("/", routes![super::register_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .post("/accounts")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(
                serde_json::json!({ "email": "orphan@example.com" })
                    .to_string(),
            )
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::InternalServerError);
        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Failed to release the email claim",
                "orphan@example.com",
                "deleted manually"
            ]
        ));
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let email = "duplicate@example.com";
        register_account(&account_store, &pool, email).await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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
    async fn test_register_duplicate_email_case_insensitive_returns_409() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        register_account(&account_store, &pool, "User@Example.com").await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
            .manage(pool)
            .mount("/", routes![super::register_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .post("/accounts")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(r#"{"email":"user@example.com"}"#)
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Conflict);
    }

    #[tokio::test]
    async fn test_connect_account_finds_email_case_insensitively() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let client_id =
            register_account(&account_store, &pool, "customer@firm.com").await;

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
            .manage(pool)
            .mount("/", routes![super::connect_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .post("/accounts/connect")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(r#"{"email":"Customer@Firm.COM","account":"ALPACA123"}"#)
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body: serde_json::Value =
            response.into_json().await.expect("response body");

        assert_eq!(
            body.get("client_id").and_then(|value| value.as_str()),
            Some(client_id.to_string().as_str())
        );
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

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
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

    #[tokio::test]
    async fn test_only_one_account_per_email_stored_in_view() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let email = "unique@example.com";
        let first_client_id =
            register_account(&account_store, &pool, email).await;

        let view = find_by_email(&pool, &Email::new(email).unwrap())
            .await
            .expect("Query should succeed")
            .expect("View should exist");

        let AccountView::Registered { client_id, .. } = view else {
            panic!("Expected Registered view")
        };

        assert_eq!(client_id, first_client_id);
    }

    #[tokio::test]
    async fn test_unwhitelist_wallet_succeeds() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let email = "unwhitelist@example.com";
        let client_id = register_account(&account_store, &pool, email).await;

        account_store
            .send(
                &client_id,
                AccountCommand::LinkToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                },
            )
            .await
            .expect("Failed to link to Alpaca");

        let wallet = address!("0x1111111111111111111111111111111111111111");
        account_store
            .send(&client_id, AccountCommand::WhitelistWallet { wallet })
            .await
            .expect("Failed to whitelist wallet");

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
            .manage(pool.clone())
            .mount("/", routes![super::unwhitelist_wallet]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .delete(format!("/accounts/{client_id}/wallets/{wallet}"))
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let view = find_by_client_id(&pool, &client_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        let AccountView::LinkedToAlpaca { whitelisted_wallets, .. } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert!(
            whitelisted_wallets.is_empty(),
            "Wallet should be removed from view after unwhitelisting"
        );
    }

    #[tokio::test]
    async fn test_unwhitelist_wallet_account_not_found_returns_404() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
            .manage(pool)
            .mount("/", routes![super::unwhitelist_wallet]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let fake_client_id = ClientId::new();
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let response = client
            .delete(format!("/accounts/{fake_client_id}/wallets/{wallet}"))
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::NotFound);
    }

    #[tokio::test]
    async fn test_register_rejected_when_email_already_claimed() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let (account_store, _account_projection) =
            StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

        // Simulate a concurrent registration that already claimed the email (its
        // account is not yet projected). The atomic claim must reject this
        // registration even though find_by_email would return no account.
        let email = "claimed@example.com";
        sqlx::query!("INSERT INTO account_emails (email) VALUES (?)", email)
            .execute(&pool)
            .await
            .expect("Failed to seed email claim");

        let rate_limiter = FailedAuthRateLimiter::new().unwrap();

        let rocket = rocket::build()
            .manage(test_config())
            .manage(rate_limiter)
            .manage(account_store)
            .manage(pool)
            .mount("/", routes![super::register_account]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .post("/accounts")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(serde_json::json!({ "email": email }).to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Conflict);
    }
}
