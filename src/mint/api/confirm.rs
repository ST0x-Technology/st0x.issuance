use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use event_sorcery::Store;
use rocket::{post, serde::json::Json};
use serde::Deserialize;
use sqlx::{Pool, Sqlite};
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::IssuerAuth;
use crate::mint::{
    IssuerMintRequestId, Mint, MintCommand, TokenizationRequestId,
    recovery::enqueue_scheduled_mint_recovery,
};
use crate::vault::{NetworkVaultServices, VaultService};

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

#[tracing::instrument(skip(_auth, mint_store, vault_services, pool, apalis_pool), fields(
    tokenization_request_id = %request.tokenization_request_id.0,
    issuer_request_id = %request.issuer_request_id,
    status = ?request.status
))]
#[post("/inkind/issuance/confirm", format = "json", data = "<request>")]
pub(crate) async fn confirm_journal(
    _auth: IssuerAuth,
    mint_store: &rocket::State<Arc<Store<Mint>>>,
    vault_services: &rocket::State<NetworkVaultServices>,
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
            let Some(network) = mint.network() else {
                error!(target: "mint",
                    issuer_request_id = %issuer_request_id,
                    "Mint has no network — cannot select a vault service"
                );
                return rocket::http::Status::InternalServerError;
            };

            let vault_service = match vault_services.service(network) {
                Ok(vault_service) => vault_service.clone(),
                Err(error) => {
                    error!(target: "mint",
                        issuer_request_id = %issuer_request_id,
                        error = %error,
                        "Cannot confirm mint on an unconfigured network"
                    );
                    return rocket::http::Status::UnprocessableEntity;
                }
            };

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
                vault_service,
                pool,
                apalis_pool,
                issuer_request_id,
            ));
        }
    }

    rocket::http::Status::Ok
}

#[tracing::instrument(skip(mint_store, vault_service, pool, apalis_pool), fields(
    issuer_request_id = %issuer_request_id
))]
async fn process_journal_completion(
    mint_store: Arc<Store<Mint>>,
    vault_service: Arc<dyn VaultService>,
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

    // The shared wallet lock spans preparation, durable event persistence,
    // and initial broadcast. The real provider reads the pending chain nonce
    // while this guard is held, so a failed signing/event append cannot consume
    // a process-local nonce and concurrent wallet operations cannot prepare a
    // replacement before the exact intent is durable.
    let wallet_guard = vault_service.lock_wallet().await;

    // Step 2: Prepare and persist the exact signed transaction before any
    // broadcast (PrepareMint → MintIntended).
    if let Err(err) = mint_store
        .send(
            &issuer_request_id,
            MintCommand::PrepareMint {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
    {
        error!(target: "mint", issuer_request_id = %issuer_request_id,
            error = ?err,
            "PrepareMint command failed — scheduling recovery"
        );
        schedule_recovery(&pool, &apalis_pool, &issuer_request_id).await;
        return;
    }

    let Some(prepared) = prepared_mint_after_load(
        mint_store.load(&issuer_request_id).await,
        &pool,
        &apalis_pool,
        &issuer_request_id,
    )
    .await
    else {
        return;
    };

    if !matches!(prepared, Mint::TxIntended { .. }) {
        if matches!(prepared, Mint::MintingFailed { .. }) {
            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint preparation failed — scheduling automatic recovery"
            );
            schedule_recovery(&pool, &apalis_pool, &issuer_request_id).await;
        }
        return;
    }

    // Step 3: Broadcast only the persisted transaction.
    if let Err(err) = mint_store
        .send(
            &issuer_request_id,
            MintCommand::SubmitMint {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
    {
        warn!(target: "mint", issuer_request_id = %issuer_request_id,
            error = ?err,
            "SubmitMint command failed — keeping persisted intent for recovery"
        );
        schedule_recovery(&pool, &apalis_pool, &issuer_request_id).await;
        return;
    }
    drop(wallet_guard);

    // Step 4: Load the tx_id from the aggregate and confirm.
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

    if let Mint::TxSubmitted { tx_id, .. } = &mint {
        if let Err(err) = mint_store
            .send(
                &issuer_request_id,
                MintCommand::ConfirmMint {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_id: tx_id.clone(),
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
                schedule_recovery(&pool, &apalis_pool, &issuer_request_id)
                    .await;
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
            schedule_recovery(&pool, &apalis_pool, &issuer_request_id).await;
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

async fn prepared_mint_after_load<LoadError: Debug>(
    result: Result<Option<Mint>, LoadError>,
    pool: &Pool<Sqlite>,
    apalis_pool: &ApalisSqlitePool,
    issuer_request_id: &IssuerMintRequestId,
) -> Option<Mint> {
    match result {
        Ok(Some(mint)) => Some(mint),
        Ok(None) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                "Mint aggregate not found after PrepareMint"
            );
            None
        }
        Err(error) => {
            error!(target: "mint", issuer_request_id = %issuer_request_id,
                error = ?error,
                "Failed to load aggregate after PrepareMint — scheduling recovery"
            );
            schedule_recovery(pool, apalis_pool, issuer_request_id).await;
            None
        }
    }
}

async fn schedule_recovery(
    pool: &Pool<Sqlite>,
    apalis_pool: &ApalisSqlitePool,
    issuer_request_id: &IssuerMintRequestId,
) {
    if let Err(error) = enqueue_scheduled_mint_recovery(
        pool,
        apalis_pool,
        issuer_request_id.clone(),
    )
    .await
    {
        // The reconciler re-enqueues recoverable mints, so this delays
        // recovery rather than losing it: degraded but continuing.
        warn!(target: "mint", issuer_request_id = %issuer_request_id,
            error = %error,
            "Failed to enqueue scheduled mint recovery"
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

    use super::{
        confirm_journal, prepared_mint_after_load, process_journal_completion,
    };
    use crate::auth::FailedAuthRateLimiter;
    use crate::mint::api::test_utils::{
        TestAccountAndAsset, TestHarness, network_vault_services, test_config,
    };
    use crate::mint::{
        IssuerMintRequestId, Mint, MintCommand, MintView, Quantity,
        TokenizationRequestId, view::find_by_issuer_request_id,
    };
    use crate::test_utils::log_count_at;
    use crate::vault::VaultService;
    use crate::vault::mock::MockVaultService;

    #[traced_test]
    #[tokio::test]
    async fn prepared_mint_load_failure_enqueues_recovery() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, apalis_pool, .. } = harness;
        let issuer_request_id = IssuerMintRequestId::random();

        let prepared = prepared_mint_after_load(
            Err::<Option<Mint>, _>("transient load failure"),
            &pool,
            &apalis_pool,
            &issuer_request_id,
        )
        .await;

        assert!(prepared.is_none());
        let aggregate_id = issuer_request_id.to_string();
        let enqueued = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(enqueued, 1);
        assert_eq!(
            log_count_at!(
                Level::ERROR,
                &[
                    "Failed to load aggregate after PrepareMint — scheduling recovery",
                    &issuer_request_id.to_string(),
                ]
            ),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn concurrent_mints_share_the_wallet_preparation_lock() {
        let vault = Arc::new(MockVaultService::new_success().with_delay(500));
        let harness = TestHarness::new_with_mint_vault(vault.clone()).await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;
        let TestHarness { pool, apalis_pool, mint_store, .. } = harness;
        let first_id = IssuerMintRequestId::random();
        let second_id = IssuerMintRequestId::random();

        for (issuer_request_id, request_id) in [
            (first_id.clone(), "concurrent-first"),
            (second_id.clone(), "concurrent-second"),
        ] {
            mint_store
                .send(
                    &issuer_request_id,
                    MintCommand::Initiate {
                        issuer_request_id: issuer_request_id.clone(),
                        tokenization_request_id: TokenizationRequestId::new(
                            request_id,
                        ),
                        quantity: Quantity::new(Decimal::from(100)),
                        underlying: underlying.clone(),
                        token: token.clone(),
                        network,
                        client_id,
                        wallet: address!(
                            "0x1234567890abcdef1234567890abcdef12345678"
                        ),
                    },
                )
                .await
                .unwrap();
            mint_store
                .send(
                    &issuer_request_id,
                    MintCommand::ConfirmJournal {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                )
                .await
                .unwrap();
        }

        let initial_guard = vault.lock_wallet().await;
        let first = tokio::spawn(process_journal_completion(
            mint_store.clone(),
            vault.clone(),
            pool.clone(),
            apalis_pool.clone(),
            first_id.clone(),
        ));
        let second = tokio::spawn(process_journal_completion(
            mint_store.clone(),
            vault.clone(),
            pool,
            apalis_pool,
            second_id.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(vault.get_call_count(), 0);
        drop(initial_guard);
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert_eq!(
            vault.get_call_count(),
            1,
            "only one concurrent mint may prepare while the shared guard is held"
        );

        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(vault.get_call_count(), 2);
        assert!(matches!(
            mint_store.load(&first_id).await.unwrap(),
            Some(Mint::Completed { .. })
        ));
        assert!(matches!(
            mint_store.load(&second_id).await.unwrap(),
            Some(Mint::Completed { .. })
        ));
    }

    #[tokio::test]
    async fn test_confirm_journal_completed_returns_ok() {
        let harness = TestHarness::new().await;
        let TestAccountAndAsset {
            client_id, underlying, token, network, ..
        } = harness.setup_account_and_asset().await;

        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let enqueued = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let enqueued = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
                )
                .bind(&aggregate_id)
                .fetch_one(&pool)
                .await
                .unwrap();
                if enqueued > 0 {
                    break enqueued;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("job not enqueued within 5s");

        assert_eq!(
            enqueued, 1,
            "a mint that fails submission must enqueue exactly one recovery job"
        );

        assert_eq!(
            log_count_at!(
                Level::WARN,
                &[
                    "SubmitMint command failed — keeping persisted intent for recovery"
                ]
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let enqueued = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let enqueued = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?",
                )
                .bind(&aggregate_id)
                .fetch_one(&pool)
                .await
                .unwrap();
                if enqueued > 0 {
                    break enqueued;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("job not enqueued within 5s");

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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } = harness;

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
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } =
            TestHarness::new().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool)
            .manage(apalis_pool)
            .manage(network_vault_services(vault))
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
        let TestHarness { pool, apalis_pool, mint_store, vault, .. } =
            TestHarness::new().await;

        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .manage(mint_store)
            .manage(pool)
            .manage(apalis_pool)
            .manage(network_vault_services(vault))
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
