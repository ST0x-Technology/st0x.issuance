use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use event_sorcery::Store;
use rocket::{post, serde::json::Json};
use serde::Deserialize;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::IssuerAuth;
use crate::mint::{
    IssuerMintRequestId, Mint, MintCommand, TokenizationRequestId,
    recovery::enqueue_scheduled_mint_recovery,
};

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

#[tracing::instrument(skip(_auth, mint_store, pool, apalis_pool), fields(
    tokenization_request_id = %request.tokenization_request_id.0,
    issuer_request_id = %request.issuer_request_id,
    status = ?request.status
))]
#[post("/inkind/issuance/confirm", format = "json", data = "<request>")]
pub(crate) async fn confirm_journal(
    _auth: IssuerAuth,
    mint_store: &rocket::State<Arc<Store<Mint>>>,
    pool: &rocket::State<Pool<Sqlite>>,
    apalis_pool: &rocket::State<ApalisSqlitePool>,
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

    let mint = match mint_store.load(&issuer_request_id).await {
        Ok(Some(mint)) => mint,
        Ok(None) => {
            error!(target: "mint", "Mint aggregate not found for issuer_request_id={}",
                issuer_request_id
            );
            return rocket::http::Status::InternalServerError;
        }
        Err(err) => {
            error!(target: "mint", "Failed to load mint aggregate for issuer_request_id={}: {}",
                issuer_request_id, err
            );
            return rocket::http::Status::InternalServerError;
        }
    };

    if let Some(expected_tokenization_id) = mint.tokenization_request_id()
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

            if let Err(err) = mint_store.send(&issuer_request_id, command).await
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

            if let Err(err) = mint_store.send(&issuer_request_id, command).await
            {
                error!(target: "mint", "Failed to execute journal confirmation command for \
                     issuer_request_id={}: {}",
                    issuer_request_id, err
                );
                return rocket::http::Status::InternalServerError;
            }

            let mint_store = mint_store.inner().clone();
            let pool = pool.inner().clone();
            let apalis_pool = apalis_pool.inner().clone();
            rocket::tokio::spawn(process_journal_completion(
                mint_store,
                pool,
                apalis_pool,
                issuer_request_id,
            ));
        }
    }

    rocket::http::Status::Ok
}

#[tracing::instrument(skip(mint_store, pool, apalis_pool), fields(
    issuer_request_id = %issuer_request_id
))]
async fn process_journal_completion(
    mint_store: Arc<Store<Mint>>,
    pool: Pool<Sqlite>,
    apalis_pool: ApalisSqlitePool,
    issuer_request_id: IssuerMintRequestId,
) {
    // Step 1: Record mint intent (Deposit → MintingStarted).
    // Persisted BEFORE the network call so a crash between here and
    // Step 2 leaves the aggregate in Minting (recoverable) rather
    // than JournalConfirmed (which would lose track of the submission).
    if let Err(err) = mint_store
        .send(
            &issuer_request_id,
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
    if let Err(err) = mint_store
        .send(
            &issuer_request_id,
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
    let mint = match mint_store.load(&issuer_request_id).await {
        Ok(Some(mint)) => mint,
        Ok(None) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint aggregate not found after SubmitMint"
            );
            return;
        }
        Err(err) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after SubmitMint"
            );
            return;
        }
    };

    if let Mint::FireblocksSubmitted { fireblocks_tx_id, .. } = &mint {
        if let Err(err) = mint_store
            .send(
                &issuer_request_id,
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
        let state = mint.state_name();
        match &mint {
            Mint::MintingFailed { .. } => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    %state,
                    "Mint submission failed — scheduling automatic recovery"
                );
                if let Err(error) = enqueue_scheduled_mint_recovery(
                    &pool,
                    &apalis_pool,
                    issuer_request_id.clone(),
                )
                .await
                {
                    // Reconciler re-enqueues recoverable mints, so a failed
                    // enqueue here delays recovery rather than losing it:
                    // degraded-but-continuing (WARN), not unrecoverable (ERROR).
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        error = %error,
                        "Failed to enqueue scheduled mint recovery"
                    );
                }
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

    let mint = match mint_store.load(&issuer_request_id).await {
        Ok(Some(mint)) => mint,
        Ok(None) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint aggregate not found after ConfirmMint"
            );
            return;
        }
        Err(err) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after ConfirmMint"
            );
            return;
        }
    };

    match &mint {
        Mint::MintingFailed { .. } => {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint confirmation failed — scheduling automatic recovery"
            );
            if let Err(error) = enqueue_scheduled_mint_recovery(
                &pool,
                &apalis_pool,
                issuer_request_id.clone(),
            )
            .await
            {
                // Reconciler re-enqueues recoverable mints, so a failed
                // enqueue here delays recovery rather than losing it:
                // degraded-but-continuing (WARN), not unrecoverable (ERROR).
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %error,
                    "Failed to enqueue scheduled mint recovery"
                );
            }
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
    if let Err(err) = mint_store
        .send(
            &issuer_request_id,
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
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::confirm_journal;
    use crate::auth::FailedAuthRateLimiter;
    use crate::mint::api::test_utils::{
        TestAccountAndAsset, TestHarness, test_config,
    };
    use crate::mint::{
        IssuerMintRequestId, MintCommand, MintView, Quantity,
        TokenizationRequestId, view::find_by_issuer_request_id,
    };
    use crate::test_utils::log_count_at;
    use crate::vault::mock::MockVaultService;

    #[tokio::test]
    async fn test_confirm_journal_completed_returns_ok() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;

        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

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

        mint_store
            .send(&issuer_request_id, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool)
            .manage(apalis_pool)
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
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

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

        mint_store
            .send(&issuer_request_id, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool)
            .manage(apalis_pool)
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
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

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

        mint_store
            .send(&issuer_request_id, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool.clone())
            .manage(apalis_pool)
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

    /// A confirm whose minting fails at submission drives the mint to
    /// `MintingFailed`, and `process_journal_completion` must enqueue a durable
    /// recovery job — the apalis integration's reason for existing. The handler
    /// does that work in a spawned task, so poll the shared `Jobs` table.
    #[traced_test]
    #[tokio::test]
    async fn confirm_enqueues_recovery_when_minting_fails() {
        let harness = TestHarness::new_with_mint_vault(Arc::new(
            MockVaultService::new_submit_failure(),
        ))
        .await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-fail-123");

        mint_store
            .send(
                &issuer_request_id,
                MintCommand::Initiate {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                },
            )
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool.clone())
            .manage(apalis_pool)
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

        // `process_journal_completion` runs in a spawned task, so poll for the
        // recovery job rather than asserting synchronously.
        let aggregate_id = issuer_request_id.to_string();
        let mut enqueued = 0;
        for _ in 0..100 {
            enqueued = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
            )
            .bind(&aggregate_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            if enqueued > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            enqueued, 1,
            "a mint that fails submission must enqueue exactly one recovery job"
        );

        assert_eq!(
            log_count_at!(
                Level::WARN,
                &["Mint submission failed — scheduling automatic recovery"]
            ),
            1,
            "the submission-failure path must log the scheduling of recovery"
        );
    }

    /// A confirm whose submission SUCCEEDS but whose on-chain confirmation
    /// reverts drives the mint to `MintingFailed` via the SECOND
    /// `process_journal_completion` branch (the one after `ConfirmMint`), which
    /// must also enqueue a durable recovery job.
    /// `confirm_enqueues_recovery_when_minting_fails` covers the first
    /// (submission-failure) branch; this covers the second so a future refactor
    /// cannot silently drop the confirmation-failure enqueue.
    #[traced_test]
    #[tokio::test]
    async fn confirm_enqueues_recovery_when_confirmation_reverts() {
        let harness = TestHarness::new_with_mint_vault(Arc::new(
            MockVaultService::new_confirm_revert(),
        ))
        .await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-confirm-revert-123");

        mint_store
            .send(
                &issuer_request_id,
                MintCommand::Initiate {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                },
            )
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool.clone())
            .manage(apalis_pool)
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

        // `process_journal_completion` runs in a spawned task, so poll for the
        // recovery job rather than asserting synchronously.
        let aggregate_id = issuer_request_id.to_string();
        let mut enqueued = 0;
        for _ in 0..100 {
            enqueued = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
            )
            .bind(&aggregate_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            if enqueued > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            enqueued, 1,
            "a mint whose confirmation reverts must enqueue exactly one recovery job"
        );

        assert_eq!(
            log_count_at!(
                Level::WARN,
                &["Mint confirmation failed — scheduling automatic recovery"]
            ),
            1,
            "the confirmation-failure path must log the scheduling of recovery"
        );
    }

    #[tokio::test]
    async fn test_confirm_journal_completed_updates_view() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

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

        mint_store
            .send(&issuer_request_id, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool.clone())
            .manage(apalis_pool)
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
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

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

        mint_store
            .send(&issuer_request_id, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool.clone())
            .manage(apalis_pool)
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
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

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

        mint_store
            .send(&issuer_request_id, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool.clone())
            .manage(apalis_pool)
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
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;

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

        mint_store
            .send(&issuer_request_id, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool)
            .manage(apalis_pool)
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
        let TestHarness { pool, apalis_pool, mint_store, .. } =
            TestHarness::new().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool)
            .manage(apalis_pool)
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
        let TestHarness { pool, apalis_pool, mint_store, .. } =
            TestHarness::new().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool)
            .manage(apalis_pool)
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
