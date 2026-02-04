use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::account::ClientId;
use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

pub(crate) mod mock;
pub(crate) mod service;

pub use service::AlpacaConfig;

/// Service abstraction for Alpaca API operations.
///
/// This trait provides an interface for calling Alpaca's tokenization endpoints,
/// particularly the mint callback endpoint. Implementations can be real HTTP-based
/// services or mocks for testing.
#[async_trait]
pub(crate) trait AlpacaService: Send + Sync {
    /// Sends a mint callback to Alpaca to confirm mint completion.
    ///
    /// # Arguments
    ///
    /// * `request` - Callback request containing mint details
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success (200 OK response from Alpaca).
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if the HTTP request fails, authentication fails,
    /// or Alpaca returns an error response.
    async fn send_mint_callback(
        &self,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError>;

    /// Calls Alpaca's redeem endpoint to initiate a redemption.
    ///
    /// # Arguments
    ///
    /// * `request` - Redeem request containing redemption details
    async fn call_redeem_endpoint(
        &self,
        request: RedeemRequest,
    ) -> Result<RedeemResponse, AlpacaError>;

    /// Polls Alpaca's request list endpoint to retrieve a tokenization request.
    ///
    /// # Arguments
    ///
    /// * `tokenization_request_id` - ID of the tokenization request to poll
    ///
    /// # Returns
    ///
    /// Returns the full [`TokenizationRequest`] containing:
    /// - Request ID and issuer request ID
    /// - Request type (mint or redeem) and status (pending, completed, or rejected)
    /// - Asset details (underlying symbol, token symbol, quantity)
    /// - Transaction details (wallet address, transaction hash)
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if:
    /// - HTTP request fails (network error, timeout, etc.)
    /// - Authentication fails (401/403 response)
    /// - Request is not found in Alpaca's list
    /// - Alpaca returns an error response
    /// - Response deserialization or parsing fails
    async fn poll_request_status(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaError>;
}

/// Request payload for Alpaca's mint callback endpoint.
///
/// This struct is serialized to JSON and sent to:
/// `POST /v1/accounts/{account_id}/tokenization/callback/mint`
///
/// The JSON format uses snake_case field names per Alpaca's API convention.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct MintCallbackRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) client_id: ClientId,
    pub(crate) wallet_address: Address,
    pub(crate) tx_hash: B256,
    pub(crate) network: Network,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RedeemRequest {
    pub(crate) issuer_request_id: IssuerRequestId,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    pub(crate) client_id: ClientId,
    #[serde(rename = "qty")]
    pub(crate) quantity: Quantity,
    pub(crate) network: Network,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RedeemResponse {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) created_at: DateTime<Utc>,
    #[serde(rename = "type")]
    pub(crate) r#type: TokenizationRequestType,
    pub(crate) status: RedeemRequestStatus,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    #[serde(rename = "qty")]
    pub(crate) quantity: Quantity,
    pub(crate) issuer: String,
    pub(crate) network: Network,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
    pub(crate) fees: Option<Fees>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TokenizationRequestType {
    Mint,
    Redeem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RedeemRequestStatus {
    Pending,
    Completed,
    Rejected,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Fees(pub(crate) Decimal);

/// Individual tokenization request from the list endpoint.
///
/// This struct represents both mint and redeem requests returned by Alpaca's
/// request list endpoint. We validate all fields match to ensure we're processing
/// the correct request.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenizationRequest {
    #[serde(rename = "tokenization_request_id")]
    pub id: TokenizationRequestId,
    #[serde(rename = "issuer_request_id")]
    pub issuer_request_id: IssuerRequestId,
    #[serde(rename = "type")]
    pub r#type: TokenizationRequestType,
    pub status: RedeemRequestStatus,
    #[serde(rename = "underlying_symbol")]
    pub underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub token: TokenSymbol,
    #[serde(rename = "qty")]
    pub quantity: Quantity,
    #[serde(rename = "wallet_address")]
    pub wallet: Address,
    #[serde(rename = "tx_hash")]
    pub tx_hash: B256,
}

/// Errors that can occur during Alpaca API operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaError {
    #[error("Reqwest error")]
    Reqwest(#[from] reqwest::Error),
    /// Failed to parse response after 200 OK - NOT retryable
    #[error("Failed to parse response: {source}")]
    Parse {
        body: String,
        #[source]
        source: serde_json::Error,
    },
    /// Authentication failed (401/403 response)
    #[error("Authentication failed: {0}")]
    Auth(String),
    /// Alpaca API returned an error response
    #[error("API error {status_code}: {body}")]
    Api { status_code: u16, body: String },
    /// Tokenization request not found when polling status
    #[error("Tokenization request not found: {0}")]
    RequestNotFound(TokenizationRequestId),
}

impl AlpacaError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Reqwest(e) => !e.is_decode(),
            Self::Api { status_code, .. } => {
                matches!(status_code, 500..=599 | 429)
            }
            Self::Parse { .. } | Self::Auth(_) | Self::RequestNotFound(_) => {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use rust_decimal::Decimal;
    use serde_json::json;

    use crate::account::ClientId;
    use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    use super::{MintCallbackRequest, RedeemRequest};

    #[test]
    fn test_mint_callback_request_serialization() {
        let client_id = "55051234-0000-4abc-9000-4aabcdef0045".parse().unwrap();

        let request = MintCallbackRequest {
            tokenization_request_id: TokenizationRequestId::new(
                "12345-678-90AB",
            ),
            client_id,
            wallet_address: address!(
                "0x1234567890abcdef1234567890abcdef12345678"
            ),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            network: Network::new("base"),
        };

        let serialized = serde_json::to_value(&request).unwrap();

        assert_eq!(
            serialized["tokenization_request_id"],
            json!("12345-678-90AB")
        );
        assert_eq!(
            serialized["client_id"],
            json!("55051234-0000-4abc-9000-4aabcdef0045")
        );
        assert_eq!(
            serialized["wallet_address"],
            json!("0x1234567890abcdef1234567890abcdef12345678")
        );
        assert_eq!(
            serialized["tx_hash"],
            json!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            )
        );
        assert_eq!(serialized["network"], json!("base"));
    }

    #[test]
    fn test_redeem_request_serialization() {
        let client_id = "55051234-0000-4abc-9000-4aabcdef0045".parse().unwrap();

        let request = RedeemRequest {
            issuer_request_id: IssuerRequestId::new("red-abc123"),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            client_id,
            quantity: Quantity::new(Decimal::new(10050, 2)),
            network: Network::new("base"),
            wallet: address!("0x9999999999999999999999999999999999999999"),
            tx_hash: b256!(
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            ),
        };

        let serialized = serde_json::to_value(&request).unwrap();

        assert_eq!(serialized["issuer_request_id"], json!("red-abc123"));
        assert_eq!(serialized["underlying_symbol"], json!("AAPL"));
        assert_eq!(serialized["token_symbol"], json!("tAAPL"));
        assert_eq!(
            serialized["client_id"],
            json!("55051234-0000-4abc-9000-4aabcdef0045")
        );
        assert_eq!(serialized["qty"], json!("100.50"));
        assert_eq!(serialized["network"], json!("base"));
        assert_eq!(
            serialized["wallet_address"],
            json!("0x9999999999999999999999999999999999999999")
        );
        assert_eq!(
            serialized["tx_hash"],
            json!(
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )
        );
    }

    #[test]
    fn test_address_serialization_includes_0x_prefix() {
        let request = MintCallbackRequest {
            tokenization_request_id: TokenizationRequestId::new("test"),
            client_id: ClientId::new(),
            wallet_address: address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            network: Network::new("base"),
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(
            json.contains("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"")
        );
        assert!(json.contains("\"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\""));
    }
}
