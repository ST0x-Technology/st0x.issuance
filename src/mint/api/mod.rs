use rocket::http::{ContentType, Status};
use rocket::response::{self, Responder};
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use tracing::error;

use super::{
    ClientId, IssuerRequestId, Network, TokenSymbol, UnderlyingSymbol,
};
use crate::account::{AccountView, view::find_by_client_id};

mod confirm;
mod initiate;

#[cfg(test)]
pub(crate) mod test_utils;

pub(crate) use confirm::confirm_journal;
pub(crate) use initiate::initiate_mint;

#[derive(Debug, Serialize, Deserialize)]
pub struct MintResponse {
    pub issuer_request_id: IssuerRequestId,
    pub status: String,
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

impl<'r> Responder<'r, 'static> for MintApiError {
    fn respond_to(
        self,
        req: &'r rocket::Request<'_>,
    ) -> response::Result<'static> {
        let response = match self {
            Self::InvalidQuantity => MintErrorResponse::bad_request(
                "Failed Validation: Invalid data payload",
            ),
            Self::AssetNotAvailable => MintErrorResponse::bad_request(
                "Invalid Token: Token not available on the network",
            ),
            Self::ClientNotEligible => MintErrorResponse::bad_request(
                "Insufficient Eligibility: Client not eligible",
            ),
            Self::CommandExecutionFailed(e) => {
                Self::handle_command_execution_error(e.as_ref())
            }
            Self::AssetQueryFailed(e) => {
                Self::handle_query_error(&e, "Failed to query enabled assets")
            }
            Self::AccountQueryFailed(e) => {
                Self::handle_query_error(&e, "Failed to query account")
            }
            Self::MintViewQueryFailed(e) => {
                Self::handle_query_error(&e, "Failed to query mint view")
            }
            Self::MintViewNotFound => {
                error!("Mint view not found after creation");
                MintErrorResponse::internal_server_error(
                    "Internal server error",
                )
            }
            Self::UnexpectedMintState => {
                error!("Unexpected mint state");
                MintErrorResponse::internal_server_error(
                    "Internal server error",
                )
            }
        };

        response.respond_to(req)
    }
}

impl MintApiError {
    fn handle_command_execution_error(
        e: &(dyn std::error::Error + Send),
    ) -> MintErrorResponse {
        error!(error = %e, "Failed to execute mint command");

        let error_msg = e.to_string().to_lowercase();
        if error_msg.contains("already") || error_msg.contains("duplicate") {
            MintErrorResponse::conflict("Mint already initiated")
        } else {
            MintErrorResponse::internal_server_error(
                "Failed to execute mint command",
            )
        }
    }

    fn handle_query_error<E: std::fmt::Display>(
        e: &E,
        message: &str,
    ) -> MintErrorResponse {
        error!(error = %e, "{message}");
        MintErrorResponse::internal_server_error("Internal server error")
    }
}

struct MintErrorResponse {
    status: Status,
    body: Json<ErrorResponse>,
}

impl MintErrorResponse {
    fn bad_request(message: &str) -> Self {
        Self {
            status: Status::BadRequest,
            body: Json(ErrorResponse { error: message.to_string() }),
        }
    }

    fn conflict(message: &str) -> Self {
        Self {
            status: Status::Conflict,
            body: Json(ErrorResponse { error: message.to_string() }),
        }
    }

    fn internal_server_error(message: &str) -> Self {
        Self {
            status: Status::InternalServerError,
            body: Json(ErrorResponse { error: message.to_string() }),
        }
    }
}

impl<'r> Responder<'r, 'static> for MintErrorResponse {
    fn respond_to(
        self,
        req: &'r rocket::Request<'_>,
    ) -> response::Result<'static> {
        response::Response::build_from(self.body.respond_to(req)?)
            .status(self.status)
            .header(ContentType::JSON)
            .ok()
    }
}

pub(crate) async fn validate_asset_exists(
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

pub(crate) async fn validate_client_eligible(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    client_id: &ClientId,
) -> Result<(), MintApiError> {
    let account_view =
        find_by_client_id(pool, client_id).await.map_err(|e| {
            error!("Failed to find account by client_id: {e}");
            MintApiError::AccountQueryFailed(e)
        })?;

    let Some(AccountView::LinkedToAlpaca { .. }) = account_view else {
        return Err(MintApiError::ClientNotEligible);
    };

    Ok(())
}
