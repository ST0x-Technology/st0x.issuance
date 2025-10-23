use serde::{Deserialize, Serialize};
use tracing::error;

use super::{
    ClientId, IssuerRequestId, Network, TokenSymbol, UnderlyingSymbol,
};
use crate::account::{
    AccountView, LinkedAccountStatus, view::find_by_client_id,
};

mod confirm;
mod initiate;

#[cfg(test)]
mod test_utils;

pub(crate) use confirm::confirm_journal;
pub(crate) use initiate::initiate_mint;

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

    let Some(AccountView::Account { status, .. }) = account_view else {
        return Err(MintApiError::ClientNotEligible);
    };

    if status != LinkedAccountStatus::Active {
        return Err(MintApiError::ClientNotEligible);
    }

    Ok(())
}
