use alloy::primitives::Address;
use cqrs_es::{AggregateContext, EventStore, persist::PersistedEventStore};
use rocket::{State, serde::json::Json};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteEventRepository;
use tracing::{error, info};

use super::{
    ClientId, IssuerRequestId, MintCommand, MintView, Network, Quantity,
    TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    view::find_by_issuer_request_id,
};
use crate::account::{
    AccountView, LinkedAccountStatus, view::find_by_client_id,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum JournalStatus {
    Completed,
    Rejected,
}

#[derive(Debug, Deserialize)]
pub(crate) struct JournalConfirmationRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) status: JournalStatus,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MintRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    #[serde(rename = "qty")]
    pub(crate) quantity: Decimal,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) client_id: String,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Address,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MintResponse {
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ErrorResponse {
    pub(crate) error: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintApiError {
    #[error("Invalid quantity: must be greater than zero")]
    InvalidQuantity,

    #[error("Asset not available on network")]
    AssetNotAvailable,

    #[error("Client not eligible")]
    ClientNotEligible,

    #[error("Failed to query enabled assets")]
    AssetQueryFailed(
        #[source] crate::tokenized_asset::view::TokenizedAssetViewError,
    ),

    #[error("Failed to query account")]
    AccountQueryFailed(#[source] crate::account::view::AccountViewError),

    #[error("Failed to execute mint command")]
    CommandExecutionFailed(#[source] Box<dyn std::error::Error + Send>),

    #[error("Failed to query mint view")]
    MintViewQueryFailed(#[source] super::view::MintViewError),

    #[error("Mint view not found after creation")]
    MintViewNotFound,

    #[error("Unexpected mint state")]
    UnexpectedMintState,
}

impl<'r> rocket::response::Responder<'r, 'static> for MintApiError {
    fn respond_to(
        self,
        _: &'r rocket::Request<'_>,
    ) -> rocket::response::Result<'static> {
        let (status, message) = match self {
            Self::InvalidQuantity => (
                rocket::http::Status::BadRequest,
                "Failed Validation: Invalid data payload",
            ),
            Self::AssetNotAvailable => (
                rocket::http::Status::BadRequest,
                "Invalid Token: Token not available on the network",
            ),
            Self::ClientNotEligible => (
                rocket::http::Status::BadRequest,
                "Insufficient Eligibility: Client not eligible",
            ),
            Self::CommandExecutionFailed(_) => {
                (rocket::http::Status::Conflict, "Mint already initiated")
            }
            Self::AssetQueryFailed(_)
            | Self::AccountQueryFailed(_)
            | Self::MintViewQueryFailed(_)
            | Self::MintViewNotFound
            | Self::UnexpectedMintState => (
                rocket::http::Status::InternalServerError,
                "Internal server error",
            ),
        };

        let response = ErrorResponse { error: message.to_string() };

        rocket::response::Response::build()
            .status(status)
            .header(rocket::http::ContentType::JSON)
            .sized_body(
                None,
                std::io::Cursor::new(
                    serde_json::to_string(&response).unwrap_or_else(|_| {
                        r#"{"error":"Internal server error"}"#.to_string()
                    }),
                ),
            )
            .ok()
    }
}

#[post("/inkind/issuance", format = "json", data = "<request>")]
pub(crate) async fn initiate_mint(
    cqrs: &rocket::State<crate::MintCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<MintRequest>,
) -> Result<Json<MintResponse>, MintApiError> {
    let request = request.into_inner();

    if request.quantity <= Decimal::ZERO {
        return Err(MintApiError::InvalidQuantity);
    }

    validate_asset_exists(
        pool.inner(),
        &request.underlying,
        &request.token,
        &request.network,
    )
    .await?;

    let client_id = ClientId(request.client_id.clone());

    validate_client_eligible(pool.inner(), &client_id).await?;

    let issuer_request_id =
        IssuerRequestId::new(uuid::Uuid::new_v4().to_string());

    let command = MintCommand::Initiate {
        issuer_request_id: issuer_request_id.clone(),
        tokenization_request_id: request.tokenization_request_id,
        quantity: Quantity::new(request.quantity),
        underlying: request.underlying,
        token: request.token,
        network: request.network,
        client_id,
        wallet: request.wallet,
    };

    cqrs.execute(&issuer_request_id.0, command).await.map_err(|e| {
        error!("Failed to execute mint command: {e}");
        MintApiError::CommandExecutionFailed(Box::new(e))
    })?;

    let mint_view = find_by_issuer_request_id(pool.inner(), &issuer_request_id)
        .await
        .map_err(|e| {
            error!("Failed to find mint by issuer_request_id: {e}");
            MintApiError::MintViewQueryFailed(e)
        })?
        .ok_or(MintApiError::MintViewNotFound)?;

    let MintView::Initiated { issuer_request_id, .. } = mint_view else {
        return Err(MintApiError::UnexpectedMintState);
    };

    Ok(Json(MintResponse { issuer_request_id, status: "created".to_string() }))
}

async fn validate_asset_exists(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    underlying: &UnderlyingSymbol,
    token: &TokenSymbol,
    network: &Network,
) -> Result<(), MintApiError> {
    let underlying_str = &underlying.0;
    let token_str = &token.0;
    let network_str = &network.0;

    let count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count: i64"
        FROM tokenized_asset_view
        WHERE json_extract(payload, '$.Asset.underlying') = ?
            AND json_extract(payload, '$.Asset.token') = ?
            AND json_extract(payload, '$.Asset.network') = ?
            AND json_extract(payload, '$.Asset.enabled') = 1
        "#,
        underlying_str,
        token_str,
        network_str
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        error!("Failed to query asset: {e}");
        MintApiError::AssetQueryFailed(e.into())
    })?;

    if count == 0 {
        return Err(MintApiError::AssetNotAvailable);
    }

    Ok(())
}

async fn validate_client_eligible(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    client_id: &ClientId,
) -> Result<(), MintApiError> {
    let account_view =
        find_by_client_id(pool, client_id).await.map_err(|e| {
            error!("Failed to find account by client_id: {e}");
            MintApiError::AccountQueryFailed(e)
        })?;

    if let Some(AccountView::Account { status, .. }) = account_view {
        if status != LinkedAccountStatus::Active {
            return Err(MintApiError::ClientNotEligible);
        }
    } else {
        return Err(MintApiError::ClientNotEligible);
    }

    Ok(())
}

#[post("/inkind/issuance/confirm", format = "json", data = "<request>")]
pub(crate) async fn confirm_journal(
    cqrs: &State<crate::MintCqrs>,
    conductor: &State<crate::MintConductorType>,
    pool: &State<sqlx::Pool<sqlx::Sqlite>>,
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

    let command = match &status {
        JournalStatus::Completed => MintCommand::ConfirmJournal {
            issuer_request_id: issuer_request_id.clone(),
        },
        JournalStatus::Rejected => MintCommand::RejectJournal {
            issuer_request_id: issuer_request_id.clone(),
            reason: reason
                .unwrap_or_else(|| "Journal rejected by Alpaca".to_string()),
        },
    };

    if let Err(e) = cqrs.execute(&issuer_request_id.0, command).await {
        error!(
            "Failed to execute journal confirmation command for \
             issuer_request_id={}: {}",
            issuer_request_id.0, e
        );
        return rocket::http::Status::Ok;
    }

    if matches!(status, JournalStatus::Completed) {
        let conductor = conductor.inner().clone();
        let issuer_request_id_clone = issuer_request_id.clone();
        let pool_clone = pool.inner().clone();

        rocket::tokio::spawn(async move {
            info!(
                issuer_request_id = %issuer_request_id_clone.0,
                "Triggering conductor to handle journal confirmation"
            );

            let event_repo = SqliteEventRepository::new(pool_clone);
            let event_store = PersistedEventStore::new_event_store(event_repo);

            let aggregate_context = match event_store
                .load_aggregate(&issuer_request_id_clone.0)
                .await
            {
                Ok(ctx) => ctx,
                Err(e) => {
                    error!(
                        issuer_request_id = %issuer_request_id_clone.0,
                        error = %e,
                        "Failed to load aggregate"
                    );
                    return;
                }
            };

            let aggregate = aggregate_context.aggregate();

            if let Err(e) = conductor
                .handle_journal_confirmed(&issuer_request_id_clone, aggregate)
                .await
            {
                error!(
                    issuer_request_id = %issuer_request_id_clone.0,
                    error = %e,
                    "Conductor failed to handle journal confirmation"
                );
            } else {
                info!(
                    issuer_request_id = %issuer_request_id_clone.0,
                    "Conductor successfully handled journal confirmation"
                );
            }
        });
    }

    rocket::http::Status::Ok
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::persist::GenericQuery;
    use rocket::http::{ContentType, Status};
    use rust_decimal::Decimal;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::str::FromStr;
    use std::sync::Arc;
    use tracing::debug;

    use super::{ErrorResponse, MintResponse, confirm_journal, initiate_mint};
    use crate::account::{
        Account, AccountCommand, AccountView, AlpacaAccountNumber, Email,
        view::find_by_email,
    };
    use crate::mint::{
        Mint, MintView, Network, TokenSymbol, UnderlyingSymbol,
        view::find_by_issuer_request_id,
    };
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, TokenizedAssetView,
    };

    fn create_test_conductor(
        mint_cqrs: crate::MintCqrs,
    ) -> crate::MintConductorType {
        use crate::blockchain::mock::MockBlockchainService;
        use crate::mint::conductor::MintConductor;
        use std::sync::Arc;

        let blockchain_service = Arc::new(MockBlockchainService::new_success())
            as Arc<dyn crate::blockchain::BlockchainService>;

        Arc::new(MintConductor::new(blockchain_service, mint_cqrs))
    }

    async fn setup_test_environment() -> (
        sqlx::Pool<sqlx::Sqlite>,
        crate::AccountCqrs,
        crate::TokenizedAssetCqrs,
        crate::MintCqrs,
    ) {
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
            Arc::new(SqliteViewRepository::<AccountView, Account>::new(
                pool.clone(),
                "account_view".to_string(),
            ));
        let account_query = GenericQuery::new(account_view_repo);
        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let tokenized_asset_view_repo = Arc::new(SqliteViewRepository::<
            TokenizedAssetView,
            TokenizedAsset,
        >::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));
        let tokenized_asset_query =
            GenericQuery::new(tokenized_asset_view_repo);
        let tokenized_asset_cqrs = sqlite_cqrs(
            pool.clone(),
            vec![Box::new(tokenized_asset_query)],
            (),
        );

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);
        let mint_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ()));

        (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs)
    }

    async fn setup_with_account_and_asset(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        account_cqrs: &crate::AccountCqrs,
        tokenized_asset_cqrs: &crate::TokenizedAssetCqrs,
    ) -> (String, UnderlyingSymbol, TokenSymbol, Network) {
        let email = Email::new("test@placeholder.com".to_string())
            .expect("Valid email");

        let account_cmd = AccountCommand::Link {
            email: email.clone(),
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
        };

        account_cqrs
            .execute(email.as_str(), account_cmd)
            .await
            .expect("Failed to link account");

        let account_view = find_by_email(pool, &email)
            .await
            .expect("Failed to query account")
            .expect("Account should exist");

        let AccountView::Account { client_id, .. } = account_view else {
            panic!("Expected Account variant");
        };

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset_cmd = TokenizedAssetCommand::Add {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault_address,
        };

        tokenized_asset_cqrs
            .execute(&underlying.0, asset_cmd)
            .await
            .expect("Failed to add asset");

        (client_id.0, underlying, token, network)
    }

    #[tokio::test]
    async fn test_initiate_mint_returns_issuer_request_id() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let email = Email::new("test@placeholder.com".to_string())
            .expect("Valid email");
        let account_cmd = AccountCommand::Link {
            email: email.clone(),
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
        };
        account_cqrs
            .execute(email.as_str(), account_cmd)
            .await
            .expect("Failed to link account");

        let account_view = find_by_email(&pool, &email)
            .await
            .expect("Failed to query account")
            .expect("Account should exist");

        let AccountView::Account { client_id, .. } = account_view else {
            panic!("Expected Account variant");
        };

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset_cmd = TokenizedAssetCommand::Add {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault_address,
        };
        tokenized_asset_cqrs
            .execute(&underlying.0, asset_cmd)
            .await
            .expect("Failed to add asset");

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "network": "base",
            "client_id": client_id.0,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert!(!mint_response.issuer_request_id.0.is_empty());
        assert_eq!(mint_response.status, "created");
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_unknown_asset() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let email = Email::new("test@placeholder.com".to_string())
            .expect("Valid email");
        let account_cmd = AccountCommand::Link {
            email: email.clone(),
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
        };
        account_cqrs
            .execute(email.as_str(), account_cmd)
            .await
            .expect("Failed to link account");

        let account_view = find_by_email(&pool, &email)
            .await
            .expect("Failed to query account")
            .expect("Account should exist");

        let AccountView::Account { client_id, .. } = account_view else {
            panic!("Expected Account variant");
        };

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": "UNKNOWN",
            "token_symbol": "tUNKNOWN",
            "network": "base",
            "client_id": client_id.0,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Invalid Token: Token not available on the network"
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_negative_quantity() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "-10",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "network": "base",
            "client_id": "test",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Failed Validation: Invalid data payload"
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_unknown_client_id() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault_address =
            address!("0x1234567890abcdef1234567890abcdef12345678");

        let asset_cmd = TokenizedAssetCommand::Add {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault_address,
        };
        tokenized_asset_cqrs
            .execute(&underlying.0, asset_cmd)
            .await
            .expect("Failed to add asset");

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "network": "base",
            "client_id": "nonexistent-client-id",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Insufficient Eligibility: Client not eligible"
        );
    }

    #[tokio::test]
    async fn test_events_are_persisted_correctly() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool.clone())
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-events-test",
            "qty": "50.0",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": network.0,
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        let all_events = sqlx::query!("SELECT COUNT(*) as count FROM events")
            .fetch_one(&pool)
            .await
            .expect("Failed to count events");

        debug!("Total events in database: {}", all_events.count);

        let events = sqlx::query!(
            r"
            SELECT aggregate_id, event_type, sequence
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'Mint'
            ORDER BY sequence
            ",
            mint_response.issuer_request_id.0
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query events");

        debug!(
            "Events for mint {}: {}",
            mint_response.issuer_request_id.0,
            events.len()
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].aggregate_id, mint_response.issuer_request_id.0);
        assert_eq!(events[0].event_type, "MintEvent::Initiated");
        assert_eq!(events[0].sequence, 1);
    }

    #[tokio::test]
    async fn test_views_are_updated_correctly() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id_str, underlying, token, network) =
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool.clone())
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let tokenization_request_id = "alp-view-test";

        let request_body = serde_json::json!({
            "tokenization_request_id": tokenization_request_id,
            "qty": "75.5",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": network.0,
            "client_id": client_id_str,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let mint_response: MintResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        let view =
            find_by_issuer_request_id(&pool, &mint_response.issuer_request_id)
                .await
                .expect("Failed to query view")
                .expect("View should exist");

        let MintView::Initiated {
            issuer_request_id: view_issuer_id,
            tokenization_request_id: view_tokenization_id,
            quantity: view_quantity,
            underlying: view_underlying,
            token: view_token,
            network: view_network,
            client_id: view_client_id,
            wallet: view_wallet,
            ..
        } = view
        else {
            panic!("Expected Initiated variant");
        };

        assert_eq!(view_issuer_id, mint_response.issuer_request_id);
        assert_eq!(view_tokenization_id.0, tokenization_request_id);
        assert_eq!(view_quantity.0, Decimal::from_str("75.5").unwrap());
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_network, network);
        assert_eq!(view_client_id.0, client_id_str);
        assert_eq!(
            view_wallet,
            address!("0x1234567890abcdef1234567890abcdef12345678")
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_invalid_wallet_address() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": network.0,
            "client_id": client_id,
            "wallet_address": "invalid-address"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[tokio::test]
    async fn test_initiate_mint_rejects_wrong_network() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, _network) =
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(account_cqrs)
            .manage(tokenized_asset_cqrs)
            .manage(pool)
            .mount("/", routes![initiate_mint]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "qty": "100.5",
            "underlying_symbol": underlying.0,
            "token_symbol": token.0,
            "network": "ethereum",
            "client_id": client_id,
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
        });

        let response = client
            .post("/inkind/issuance")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);

        let error_response: ErrorResponse = serde_json::from_str(
            &response.into_string().await.expect("valid response body"),
        )
        .expect("valid JSON response");

        assert_eq!(
            error_response.error,
            "Invalid Token: Token not available on the network"
        );
    }

    #[tokio::test]
    async fn test_initiate_mint_with_duplicate_issuer_request_id() {
        let (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let (client_id, underlying, token, network) =
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let issuer_request_id = "test-issuer-request-id";

        let command = super::super::MintCommand::Initiate {
            issuer_request_id: super::super::IssuerRequestId::new(
                issuer_request_id,
            ),
            tokenization_request_id: super::super::TokenizationRequestId::new(
                "alp-123",
            ),
            quantity: super::super::Quantity::new(Decimal::from(100)),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id: super::super::ClientId(client_id.clone()),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(issuer_request_id, command.clone())
            .await
            .expect("First execution should succeed");

        let result = mint_cqrs.execute(issuer_request_id, command).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_confirm_journal_completed_returns_ok() {
        let (pool, _account_cqrs, _tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let conductor = create_test_conductor(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(conductor)
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

        assert_eq!(response.status(), Status::Ok);
    }

    #[tokio::test]
    async fn test_confirm_journal_rejected_returns_ok() {
        let (pool, _account_cqrs, _tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let conductor = create_test_conductor(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(conductor)
            .manage(pool)
            .mount("/", routes![confirm_journal]);

        let client = rocket::local::asynchronous::Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = serde_json::json!({
            "tokenization_request_id": "alp-123",
            "issuer_request_id": "iss-456",
            "status": "rejected"
        });

        let response = client
            .post("/inkind/issuance/confirm")
            .header(ContentType::JSON)
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
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let issuer_request_id =
            super::super::IssuerRequestId::new("iss-complete-123");
        let tokenization_request_id =
            super::super::TokenizationRequestId::new("alp-complete-123");

        let initiate_cmd = super::super::MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: super::super::Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id: super::super::ClientId(client_id),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let conductor = create_test_conductor(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(conductor)
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
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let issuer_request_id =
            super::super::IssuerRequestId::new("iss-view-123");
        let tokenization_request_id =
            super::super::TokenizationRequestId::new("alp-view-123");

        let initiate_cmd = super::super::MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: super::super::Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id: super::super::ClientId(client_id),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let conductor = create_test_conductor(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(conductor)
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
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let issuer_request_id =
            super::super::IssuerRequestId::new("iss-reject-123");
        let tokenization_request_id =
            super::super::TokenizationRequestId::new("alp-reject-123");

        let initiate_cmd = super::super::MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: super::super::Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id: super::super::ClientId(client_id),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let conductor = create_test_conductor(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(conductor)
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
            setup_with_account_and_asset(
                &pool,
                &account_cqrs,
                &tokenized_asset_cqrs,
            )
            .await;

        let issuer_request_id =
            super::super::IssuerRequestId::new("iss-reject-view-123");
        let tokenization_request_id =
            super::super::TokenizationRequestId::new("alp-reject-view-123");

        let initiate_cmd = super::super::MintCommand::Initiate {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: super::super::Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network,
            client_id: super::super::ClientId(client_id),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        };

        mint_cqrs
            .execute(&issuer_request_id.0, initiate_cmd)
            .await
            .expect("Failed to initiate mint");

        let conductor = create_test_conductor(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(conductor)
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
    async fn test_confirm_journal_for_nonexistent_mint_returns_ok() {
        let (pool, _account_cqrs, _tokenized_asset_cqrs, mint_cqrs) =
            setup_test_environment().await;

        let conductor = create_test_conductor(mint_cqrs.clone());

        let rocket = rocket::build()
            .manage(mint_cqrs)
            .manage(conductor)
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
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }
}
