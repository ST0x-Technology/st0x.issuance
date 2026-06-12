use cqrs_es::{AggregateContext, EventStore};
use rocket::{State, post, serde::json::Json};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::auth::IssuerAuth;
use crate::mint::{
    IssuerMintRequestId, Mint, MintCommand, TokenizationRequestId,
    recovery::spawn_scheduled_mint_recovery,
};
use crate::{MintCqrs, MintEventStore};

#[derive(Debug, Deserialize)]
pub(crate) struct JournalConfirmationRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) status: JournalStatus,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum JournalStatus {
    Completed,
    Rejected,
}

#[tracing::instrument(skip(_auth, cqrs, event_store), fields(
    tokenization_request_id = %request.tokenization_request_id.0,
    issuer_request_id = %request.issuer_request_id,
    status = ?request.status
))]
#[post("/inkind/issuance/confirm", format = "json", data = "<request>")]
pub(crate) async fn confirm_journal(
    _auth: IssuerAuth,
    cqrs: &State<MintCqrs>,
    event_store: &State<MintEventStore>,
    request: Json<JournalConfirmationRequest>,
) -> rocket::http::Status {
    let JournalConfirmationRequest {
        tokenization_request_id,
        issuer_request_id,
        status,
        reason,
    } = request.into_inner();

    info!(target: "mint", "Received journal confirmation for issuer_request_id={}, \
         tokenization_request_id={}, status={:?}",
        issuer_request_id, tokenization_request_id.0, status
    );

    let mut mint_ctx = match event_store
        .load_aggregate(&issuer_request_id.to_string())
        .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            error!(target: "mint", "Failed to load mint aggregate for issuer_request_id={}: {}",
                issuer_request_id, err
            );
            return rocket::http::Status::InternalServerError;
        }
    };

    if let Some(expected_tokenization_id) =
        mint_ctx.aggregate().tokenization_request_id()
        && &tokenization_request_id != expected_tokenization_id
    {
        error!(target: "mint", "Tokenization request ID mismatch for issuer_request_id={}. \
             Expected: {}, provided: {}",
            issuer_request_id,
            expected_tokenization_id.0,
            tokenization_request_id.0
        );
        return rocket::http::Status::BadRequest;
    }

    match status {
        JournalStatus::Rejected => {
            let command = MintCommand::RejectJournal {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.unwrap_or_else(|| {
                    "Journal rejected by Alpaca".to_string()
                }),
            };

            if let Err(err) =
                cqrs.execute(&issuer_request_id.to_string(), command).await
            {
                error!(target: "mint", "Failed to execute journal rejection command for \
                     issuer_request_id={}: {}",
                    issuer_request_id, err
                );
                return rocket::http::Status::InternalServerError;
            }
        }

        JournalStatus::Completed => {
            let command = MintCommand::ConfirmJournal {
                issuer_request_id: issuer_request_id.clone(),
            };

            if let Err(err) =
                cqrs.execute(&issuer_request_id.to_string(), command).await
            {
                error!(target: "mint", "Failed to execute journal confirmation command for \
                     issuer_request_id={}: {}",
                    issuer_request_id, err
                );
                return rocket::http::Status::InternalServerError;
            }

            let cqrs = cqrs.inner().clone();
            let store = event_store.inner().clone();
            rocket::tokio::spawn(process_journal_completion(
                cqrs,
                store,
                issuer_request_id,
            ));
        }
    }

    rocket::http::Status::Ok
}

#[tracing::instrument(skip(cqrs, event_store), fields(
    issuer_request_id = %issuer_request_id
))]
async fn process_journal_completion(
    cqrs: MintCqrs,
    event_store: MintEventStore,
    issuer_request_id: IssuerMintRequestId,
) {
    let aggregate_id = issuer_request_id.to_string();

    // Step 1: Record mint intent (Deposit → MintingStarted).
    // Persisted BEFORE the network call so a crash between here and
    // Step 2 leaves the aggregate in Minting (recoverable) rather
    // than JournalConfirmed (which would lose track of the submission).
    if let Err(err) = cqrs
        .execute(
            &aggregate_id,
            MintCommand::Deposit {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
    {
        error!(target: "mint", issuer_request_id = %issuer_request_id,
            error = ?err,
            "Deposit command failed"
        );
        return;
    }

    // Step 2: Submit mint transaction (SubmitMint → FireblocksSubmitted).
    if let Err(err) = cqrs
        .execute(
            &aggregate_id,
            MintCommand::SubmitMint {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
    {
        error!(target: "mint", issuer_request_id = %issuer_request_id,
            error = ?err,
            "SubmitMint command failed"
        );
        return;
    }

    // Step 3: Load the fireblocks_tx_id from the aggregate and confirm
    let mut context = match event_store.load_aggregate(&aggregate_id).await {
        Ok(ctx) => ctx,
        Err(err) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after SubmitMint"
            );
            return;
        }
    };

    if let Mint::FireblocksSubmitted { fireblocks_tx_id, .. } =
        context.aggregate()
    {
        if let Err(err) = cqrs
            .execute(
                &aggregate_id,
                MintCommand::ConfirmMint {
                    issuer_request_id: issuer_request_id.clone(),
                    fireblocks_tx_id: fireblocks_tx_id.clone(),
                },
            )
            .await
        {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = ?err,
                "ConfirmMint command failed"
            );
            return;
        }
    } else {
        let state = context.aggregate().state_name();
        match context.aggregate() {
            Mint::MintingFailed { .. } => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    %state,
                    "Mint submission failed — scheduling automatic recovery"
                );
                spawn_scheduled_mint_recovery(
                    cqrs,
                    event_store,
                    issuer_request_id,
                );
            }
            Mint::CallbackPending { .. } | Mint::Completed { .. } => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    %state,
                    "Aggregate already advanced by concurrent recovery — skipping"
                );
            }
            _ => {
                error!(target: "mint", issuer_request_id = %issuer_request_id,
                    %state,
                    "Unexpected aggregate state after SubmitMint — ConfirmMint skipped"
                );
            }
        }
        return;
    }

    let mut context = match event_store.load_aggregate(&aggregate_id).await {
        Ok(ctx) => ctx,
        Err(err) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after ConfirmMint"
            );
            return;
        }
    };

    match context.aggregate() {
        Mint::MintingFailed { .. } => {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint confirmation failed — scheduling automatic recovery"
            );
            spawn_scheduled_mint_recovery(cqrs, event_store, issuer_request_id);
            return;
        }
        Mint::CallbackPending { .. } => {}
        Mint::Completed { .. } => {
            info!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint already completed by recovery"
            );
            return;
        }
        state => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                state = %state.state_name(),
                "Unexpected aggregate state after ConfirmMint"
            );
            return;
        }
    }

    // Step 4: Send callback to Alpaca.
    if let Err(err) = cqrs
        .execute(
            &issuer_request_id.to_string(),
            MintCommand::SendCallback {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
    {
        error!(target: "mint", issuer_request_id = %issuer_request_id,
            error = ?err,
            "SendCallback command failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rocket::http::{ContentType, Header, Status};
    use rocket::routes;
    use rust_decimal::Decimal;

    use super::confirm_journal;
    use crate::auth::FailedAuthRateLimiter;
    use crate::mint::api::test_utils::{
        TestAccountAndAsset, TestHarness, create_test_event_store, test_config,
    };
    use crate::mint::{
        IssuerMintRequestId, MintCommand, MintView, Quantity,
        TokenizationRequestId, view::find_by_issuer_request_id,
    };

    #[tokio::test]
    async fn test_confirm_journal_completed_returns_ok() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;

        let TestHarness { pool, mint_cqrs, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
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
            .execute(&issuer_request_id.to_string(), initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.to_string(),
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
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, mint_cqrs, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
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
            .execute(&issuer_request_id.to_string(), initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.to_string(),
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
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, mint_cqrs, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
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
            .execute(&issuer_request_id.to_string(), initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.to_string(),
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

        let aggregate_id = issuer_request_id.to_string();
        let events = sqlx::query!(
            r"
            SELECT event_type, sequence
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'Mint'
            ORDER BY sequence
            ",
            aggregate_id
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
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, mint_cqrs, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
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
            .execute(&issuer_request_id.to_string(), initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.to_string(),
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
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, mint_cqrs, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
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
            .execute(&issuer_request_id.to_string(), initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.to_string(),
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

        let aggregate_id = issuer_request_id.to_string();
        let events = sqlx::query!(
            r"
            SELECT event_type, sequence
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'Mint'
            ORDER BY sequence
            ",
            aggregate_id
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
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, mint_cqrs, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
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
            .execute(&issuer_request_id.to_string(), initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool.clone())
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.to_string(),
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
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, mint_cqrs, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
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
            .execute(&issuer_request_id.to_string(), initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": wrong_tokenization_request_id.0,
            "issuer_request_id": issuer_request_id.to_string(),
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
        let TestHarness { pool, mint_cqrs, .. } = TestHarness::new().await;

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-nonexistent",
            "issuer_request_id": "00000000-0000-0000-0000-000000000000",
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
        let TestHarness { pool, mint_cqrs, .. } = TestHarness::new().await;

        let event_store = create_test_event_store(&pool);

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_cqrs)
            .manage(event_store)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "issuer_request_id": "00000000-0000-0000-0000-000000000456",
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
}
