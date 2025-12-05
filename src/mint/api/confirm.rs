use cqrs_es::{AggregateContext, EventStore, persist::PersistedEventStore};
use rocket::{State, post, serde::json::Json};
use serde::Deserialize;
use sqlite_es::SqliteEventRepository;
use sqlx::SqlitePool;
use std::sync::Arc;
use tracing::{error, info};

use crate::MintEventStore;
use crate::account::{
    AccountView, AlpacaAccountNumber, ClientId,
    view::{AccountViewError, find_by_client_id},
};
use crate::auth::IssuerAuth;
use crate::mint::{
    CallbackManager, IssuerRequestId, Mint, MintCommand, TokenizationRequestId,
    mint_manager::MintManager,
};

#[derive(Debug, Deserialize)]
pub(crate) struct JournalConfirmationRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) status: JournalStatus,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum JournalStatus {
    Completed,
    Rejected,
}

#[tracing::instrument(skip(_auth, cqrs, event_store, mint_manager, callback_manager, pool), fields(
    tokenization_request_id = %request.tokenization_request_id.0,
    issuer_request_id = %request.issuer_request_id.0,
    status = ?request.status
))]
#[post("/inkind/issuance/confirm", format = "json", data = "<request>")]
pub(crate) async fn confirm_journal(
    _auth: IssuerAuth,
    cqrs: &State<crate::MintCqrs>,
    event_store: &State<MintEventStore>,
    mint_manager: &State<
        Arc<MintManager<PersistedEventStore<SqliteEventRepository, Mint>>>,
    >,
    callback_manager: &State<
        Arc<CallbackManager<PersistedEventStore<SqliteEventRepository, Mint>>>,
    >,
    pool: &State<SqlitePool>,
    request: Json<JournalConfirmationRequest>,
) -> rocket::http::Status {
    let JournalConfirmationRequest {
        tokenization_request_id,
        issuer_request_id,
        status,
        reason,
    } = request.into_inner();

    info!(
        "Received journal confirmation for issuer_request_id={}, \
         tokenization_request_id={}, status={:?}",
        issuer_request_id.0, tokenization_request_id.0, status
    );

    let mint_ctx = match event_store.load_aggregate(&issuer_request_id.0).await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            error!(
                "Failed to load mint aggregate for issuer_request_id={}: {}",
                issuer_request_id.0, e
            );
            return rocket::http::Status::InternalServerError;
        }
    };

    if let Some(expected_tokenization_id) =
        mint_ctx.aggregate().tokenization_request_id()
    {
        if &tokenization_request_id != expected_tokenization_id {
            error!(
                "Tokenization request ID mismatch for issuer_request_id={}. \
                 Expected: {}, provided: {}",
                issuer_request_id.0,
                expected_tokenization_id.0,
                tokenization_request_id.0
            );
            return rocket::http::Status::BadRequest;
        }
    }

    match status {
        JournalStatus::Rejected => {
            let command = MintCommand::RejectJournal {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.unwrap_or_else(|| {
                    "Journal rejected by Alpaca".to_string()
                }),
            };

            if let Err(e) = cqrs.execute(&issuer_request_id.0, command).await {
                error!(
                    "Failed to execute journal rejection command for \
                     issuer_request_id={}: {}",
                    issuer_request_id.0, e
                );
                return rocket::http::Status::InternalServerError;
            }
        }

        JournalStatus::Completed => {
            let command = MintCommand::ConfirmJournal {
                issuer_request_id: issuer_request_id.clone(),
            };

            if let Err(e) = cqrs.execute(&issuer_request_id.0, command).await {
                error!(
                    "Failed to execute journal confirmation command for \
                     issuer_request_id={}: {}",
                    issuer_request_id.0, e
                );
                return rocket::http::Status::InternalServerError;
            }

            rocket::tokio::spawn(process_journal_completion(
                event_store.inner().clone(),
                mint_manager.inner().clone(),
                callback_manager.inner().clone(),
                pool.inner().clone(),
                issuer_request_id,
            ));
        }
    }

    rocket::http::Status::Ok
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaAccountLookupError {
    #[error("Account not found for client_id {client_id}")]
    NotFound { client_id: ClientId },
    #[error("Account {client_id} not linked to Alpaca")]
    NotLinked { client_id: ClientId },
    #[error("Database error: {0}")]
    Database(#[from] AccountViewError),
}

pub(crate) async fn lookup_alpaca_account(
    pool: &SqlitePool,
    client_id: &ClientId,
) -> Result<AlpacaAccountNumber, AlpacaAccountLookupError> {
    match find_by_client_id(pool, client_id).await? {
        Some(AccountView::LinkedToAlpaca { alpaca_account, .. }) => {
            Ok(alpaca_account)
        }
        Some(_) => {
            Err(AlpacaAccountLookupError::NotLinked { client_id: *client_id })
        }
        None => {
            Err(AlpacaAccountLookupError::NotFound { client_id: *client_id })
        }
    }
}

#[tracing::instrument(skip(event_store, mint_manager, callback_manager, pool), fields(
    issuer_request_id = %issuer_request_id.0
))]
async fn process_journal_completion(
    event_store: MintEventStore,
    mint_manager: Arc<
        MintManager<PersistedEventStore<SqliteEventRepository, Mint>>,
    >,
    callback_manager: Arc<
        CallbackManager<PersistedEventStore<SqliteEventRepository, Mint>>,
    >,
    pool: SqlitePool,
    issuer_request_id: IssuerRequestId,
) {
    let mint_ctx = match event_store.load_aggregate(&issuer_request_id.0).await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            error!(
                issuer_request_id = %issuer_request_id.0,
                error = ?e,
                "Failed to load mint aggregate for mint manager"
            );
            return;
        }
    };

    if let Err(e) = mint_manager
        .handle_journal_confirmed(&issuer_request_id, mint_ctx.aggregate())
        .await
    {
        error!(
            issuer_request_id = %issuer_request_id.0,
            error = ?e,
            "handle_journal_confirmed failed"
        );
        return;
    }

    let mint_ctx = match event_store.load_aggregate(&issuer_request_id.0).await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            error!(
                issuer_request_id = %issuer_request_id.0,
                error = ?e,
                "Failed to reload mint aggregate for callback manager"
            );
            return;
        }
    };

    let Mint::CallbackPending { .. } = mint_ctx.aggregate() else {
        return;
    };

    let Some(client_id) = mint_ctx.aggregate().client_id() else {
        error!(
            issuer_request_id = %issuer_request_id.0,
            "Mint in CallbackPending state has no client_id"
        );
        return;
    };

    let alpaca_account = match lookup_alpaca_account(&pool, client_id).await {
        Ok(account) => account,
        Err(e) => {
            error!(
                issuer_request_id = %issuer_request_id.0,
                client_id = %client_id,
                error = %e,
                "Failed to look up Alpaca account"
            );
            return;
        }
    };

    if let Err(e) = callback_manager
        .handle_tokens_minted(
            &alpaca_account,
            &issuer_request_id,
            mint_ctx.aggregate(),
        )
        .await
    {
        error!(
            issuer_request_id = %issuer_request_id.0,
            error = ?e,
            "handle_tokens_minted failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use chrono::Utc;
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::{
        AlpacaAccountLookupError, confirm_journal, lookup_alpaca_account,
    };
    use crate::account::{AccountView, AlpacaAccountNumber, ClientId, Email};
    use crate::auth::FailedAuthRateLimiter;
    use crate::mint::api::test_utils::{
        create_test_callback_manager, create_test_event_store,
        create_test_mint_manager, setup_test_environment,
        setup_with_account_and_asset, test_config,
    };
    use crate::mint::{
        IssuerRequestId, MintCommand, MintView, Quantity,
        TokenizationRequestId, view::find_by_issuer_request_id,
    };

    #[tokio::test]
    async fn test_confirm_journal_completed_returns_ok() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(&account_cqrs, &tokenized_asset_cqrs)
                .await;

        let issuer_request_id = IssuerRequestId::new("iss-ok-test");
        let tokenization_request_id = TokenizationRequestId::new("alp-ok-test");

        let initiate_cmd = MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.0,
            "status": "completed"
        });

        let response = client
            .post("/inkind/issuance/confirm")
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
    }

    #[tokio::test]
    async fn test_confirm_journal_rejected_returns_ok() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(&account_cqrs, &tokenized_asset_cqrs)
                .await;

        let issuer_request_id = IssuerRequestId::new("iss-reject-ok-test");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-reject-ok-test");

        let initiate_cmd = MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.0,
            "status": "rejected"
        });

        let response = client
            .post("/inkind/issuance/confirm")
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
    }

    #[tokio::test]
    async fn test_confirm_journal_completed_executes_command_and_persists_events()
     {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(&account_cqrs, &tokenized_asset_cqrs)
                .await;

        let issuer_request_id = IssuerRequestId::new("iss-complete-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-complete-123");

        let initiate_cmd = MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.0,
            "status": "completed"
        });

        let response = client
            .post("/inkind/issuance/confirm")
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

        let events = sqlx::query!(
            r"
            SELECT event_type, sequence
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'Mint'
            ORDER BY sequence
            ",
            issuer_request_id.0
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query events");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "MintEvent::Initiated");
        assert_eq!(events[1].event_type, "MintEvent::JournalConfirmed");
    }

    #[tokio::test]
    async fn test_confirm_journal_completed_updates_view() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(&account_cqrs, &tokenized_asset_cqrs)
                .await;

        let issuer_request_id = IssuerRequestId::new("iss-view-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-view-123");

        let initiate_cmd = MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.0,
            "status": "completed"
        });

        let response = client
            .post("/inkind/issuance/confirm")
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

        let view = find_by_issuer_request_id(&pool, &issuer_request_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        assert!(matches!(view, MintView::JournalConfirmed { .. }));

        let MintView::JournalConfirmed {
            issuer_request_id: view_issuer_id,
            journal_confirmed_at,
            ..
        } = view
        else {
            panic!("Expected JournalConfirmed variant");
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert!(journal_confirmed_at.timestamp() > 0);
    }

    #[tokio::test]
    async fn test_confirm_journal_rejected_executes_command_and_persists_events()
     {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(&account_cqrs, &tokenized_asset_cqrs)
                .await;

        let issuer_request_id = IssuerRequestId::new("iss-reject-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-reject-123");

        let initiate_cmd = MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.0,
            "status": "rejected"
        });

        let response = client
            .post("/inkind/issuance/confirm")
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

        let events = sqlx::query!(
            r"
            SELECT event_type, sequence
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'Mint'
            ORDER BY sequence
            ",
            issuer_request_id.0
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query events");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "MintEvent::Initiated");
        assert_eq!(events[1].event_type, "MintEvent::JournalRejected");
    }

    #[tokio::test]
    async fn test_confirm_journal_rejected_updates_view() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(&account_cqrs, &tokenized_asset_cqrs)
                .await;

        let issuer_request_id = IssuerRequestId::new("iss-reject-view-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-reject-view-123");

        let initiate_cmd = MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.0,
            "status": "rejected"
        });

        let response = client
            .post("/inkind/issuance/confirm")
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

        let view = find_by_issuer_request_id(&pool, &issuer_request_id)
            .await
            .expect("Failed to query view")
            .expect("View should exist");

        assert!(matches!(view, MintView::JournalRejected { .. }));

        let MintView::JournalRejected {
            issuer_request_id: view_issuer_id,
            reason,
            rejected_at,
            ..
        } = view
        else {
            panic!("Expected JournalRejected variant");
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(reason, "Journal rejected by Alpaca");
        assert!(rejected_at.timestamp() > 0);
    }

    #[tokio::test]
    async fn test_confirm_journal_with_mismatched_tokenization_request_id_returns_bad_request()
     {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(&account_cqrs, &tokenized_asset_cqrs)
                .await;

        let issuer_request_id = IssuerRequestId::new("iss-mismatch-test");
        let correct_tokenization_request_id =
            TokenizationRequestId::new("alp-correct");
        let wrong_tokenization_request_id =
            TokenizationRequestId::new("alp-wrong");

        let initiate_cmd = MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: correct_tokenization_request_id.clone(),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": wrong_tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.0,
            "status": "completed"
        });

        let response = client
            .post("/inkind/issuance/confirm")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);
    }

    #[tokio::test]
    async fn test_confirm_journal_for_nonexistent_mint_returns_internal_server_error()
     {
        let (pool, _account_cqrs, _tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-nonexistent",
            "issuer_request_id": "iss-nonexistent",
            "status": "completed"
        });

        let response = client
            .post("/inkind/issuance/confirm")
            .header(ContentType::JSON)
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::InternalServerError);
    }

    #[tokio::test]
    async fn test_confirm_journal_without_auth_returns_401() {
        let (pool, _account_cqrs, _tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let event_store = create_test_event_store(&pool);
        let mint_manager = create_test_mint_manager(mint_cqrs.clone());
        let callback_manager = create_test_callback_manager(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(mint_manager)
            .manage(callback_manager)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "issuer_request_id": "iss-456",
            "status": "completed"
        });

        let response = client
            .post("/inkind/issuance/confirm")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    async fn setup_lookup_test_db() -> sqlx::SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    async fn insert_account_view(
        pool: &sqlx::SqlitePool,
        client_id: ClientId,
        view: &AccountView,
    ) {
        let payload = serde_json::to_string(view).unwrap();

        sqlx::query!(
            r"INSERT INTO account_view (view_id, version, payload) VALUES (?, 1, ?)",
            client_id,
            payload
        )
        .execute(pool)
        .await
        .expect("Failed to insert account view");
    }

    #[tokio::test]
    async fn test_lookup_alpaca_account_returns_account_when_linked() {
        let pool = setup_lookup_test_db().await;
        let client_id = ClientId::new();
        let expected_account = AlpacaAccountNumber("ALPACA-12345".to_string());

        let view = AccountView::LinkedToAlpaca {
            client_id,
            email: Email::new("test@example.com".to_string()).unwrap(),
            alpaca_account: expected_account.clone(),
            whitelisted_wallets: vec![],
            registered_at: Utc::now(),
            linked_at: Utc::now(),
        };

        insert_account_view(&pool, client_id, &view).await;

        let result = lookup_alpaca_account(&pool, &client_id).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_account);
    }

    #[tokio::test]
    async fn test_lookup_alpaca_account_returns_not_linked_when_only_registered()
     {
        let pool = setup_lookup_test_db().await;
        let client_id = ClientId::new();

        let view = AccountView::Registered {
            client_id,
            email: Email::new("test@example.com".to_string()).unwrap(),
            registered_at: Utc::now(),
        };

        insert_account_view(&pool, client_id, &view).await;

        let result = lookup_alpaca_account(&pool, &client_id).await;

        assert!(
            matches!(result, Err(AlpacaAccountLookupError::NotLinked { .. })),
            "Expected NotLinked error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_lookup_alpaca_account_returns_not_found_when_no_account() {
        let pool = setup_lookup_test_db().await;
        let client_id = ClientId::new();

        let result = lookup_alpaca_account(&pool, &client_id).await;

        assert!(
            matches!(result, Err(AlpacaAccountLookupError::NotFound { .. })),
            "Expected NotFound error, got {result:?}"
        );
    }
}
