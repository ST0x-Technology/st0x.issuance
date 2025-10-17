use alloy::primitives::Address;
use rocket::serde::json::Json;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::error;

use super::{
    ClientId, IssuerRequestId, MintCommand, MintView, Network, Quantity,
    TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    view::find_by_issuer_request_id,
};
use crate::account::{
    AccountView, LinkedAccountStatus, view::find_by_client_id,
};
use crate::tokenized_asset::{TokenizedAssetView, view::list_enabled_assets};

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

    let command = MintCommand::Initiate {
        tokenization_request_id: request.tokenization_request_id,
        quantity: Quantity::new(request.quantity),
        underlying: request.underlying,
        token: request.token,
        network: request.network,
        client_id,
        wallet: request.wallet,
    };

    let aggregate_id = uuid::Uuid::new_v4().to_string();

    cqrs.execute(&aggregate_id, command).await.map_err(|e| {
        error!("Failed to execute mint command: {e}");
        MintApiError::CommandExecutionFailed(Box::new(e))
    })?;

    let mint_view = find_by_issuer_request_id(
        pool.inner(),
        &IssuerRequestId::new(&aggregate_id),
    )
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
    let enabled_assets = list_enabled_assets(pool).await.map_err(|e| {
        error!("Failed to list enabled assets: {e}");
        MintApiError::AssetQueryFailed(e)
    })?;

    let asset_exists = enabled_assets.iter().any(|asset| match asset {
        TokenizedAssetView::Asset {
            underlying: asset_underlying,
            token: asset_token,
            network: asset_network,
            ..
        } => {
            *asset_underlying == *underlying
                && *asset_token == *token
                && *asset_network == *network
        }
        TokenizedAssetView::Unavailable => false,
    });

    if !asset_exists {
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

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::persist::GenericQuery;
    use rocket::http::{ContentType, Status};
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    use super::*;
    use crate::account::{
        Account, AccountCommand, AccountView, AlpacaAccountNumber, Email,
        view::find_by_email,
    };
    use crate::mint::Mint;
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, TokenizedAssetView,
    };

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
            sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ());

        (pool, account_cqrs, tokenized_asset_cqrs, mint_cqrs)
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
}
