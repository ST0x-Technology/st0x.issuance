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

    async fn call_redeem_endpoint(
        &self,
        request: RedeemRequest,
    ) -> Result<RedeemResponse, AlpacaError>;

    /// Polls Alpaca's request list endpoint to check the status of a tokenization request.
    ///
    /// # Arguments
    ///
    /// * `tokenization_request_id` - ID of the tokenization request to poll
    ///
    /// # Returns
    ///
    /// Returns the current status of the request (Pending, Completed, or Rejected).
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if:
    /// - The HTTP request fails
    /// - Authentication fails
    /// - The request is not found in the list
    /// - Alpaca returns an error response
    async fn poll_request_status(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<RedeemRequestStatus, AlpacaError>;
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
    #[serde(serialize_with = "serialize_address")]
    pub(crate) wallet_address: Address,
    #[serde(serialize_with = "serialize_b256")]
    pub(crate) tx_hash: B256,
    pub(crate) network: Network,
}

/// Serializes an Alloy `Address` as a hex string with `0x` prefix.
fn serialize_address<S>(addr: &Address, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&format!("{addr:#x}"))
}

/// Serializes an Alloy `B256` hash as a hex string with `0x` prefix.
fn serialize_b256<S>(hash: &B256, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&format!("{hash:#x}"))
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RedeemRequest {
    pub(crate) issuer_request_id: IssuerRequestId,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    pub(crate) client_id: ClientId,
    pub(crate) qty: Quantity,
    pub(crate) network: Network,
    #[serde(rename = "wallet_address", serialize_with = "serialize_address")]
    pub(crate) wallet: Address,
    #[serde(serialize_with = "serialize_b256")]
    pub(crate) tx_hash: B256,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RedeemResponse {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) created_at: DateTime<Utc>,
    #[serde(rename = "type")]
    pub(crate) request_type: RedeemRequestType,
    pub(crate) status: RedeemRequestStatus,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    pub(crate) qty: Quantity,
    pub(crate) issuer: String,
    pub(crate) network: Network,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
    pub(crate) fees: Fees,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RedeemRequestType {
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

/// Response from Alpaca's tokenization requests list endpoint.
///
/// This struct deserializes the JSON response from:
/// `GET /v1/accounts/{account_id}/tokenization/requests`
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RequestsListResponse {
    pub(crate) requests: Vec<TokenizationRequest>,
}

/// Individual tokenization request from the list endpoint.
///
/// This is similar to `RedeemResponse` but represents the generic format
/// returned by the list endpoint (which can include both mint and redeem requests).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TokenizationRequest {
    #[serde(rename = "tokenization_request_id")]
    pub(crate) id: TokenizationRequestId,
    #[serde(rename = "issuer_request_id")]
    pub(crate) _issuer_request_id: IssuerRequestId,
    #[serde(rename = "created_at")]
    pub(crate) _created_at: DateTime<Utc>,
    #[serde(rename = "type")]
    pub(crate) _request_type: RedeemRequestType,
    pub(crate) status: RedeemRequestStatus,
    #[serde(rename = "underlying_symbol")]
    pub(crate) _underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) _token: TokenSymbol,
    #[serde(rename = "qty")]
    pub(crate) _qty: Quantity,
    #[serde(rename = "issuer")]
    pub(crate) _issuer: String,
    #[serde(rename = "network")]
    pub(crate) _network: Network,
    #[serde(rename = "wallet_address")]
    pub(crate) _wallet: Address,
    #[serde(rename = "tx_hash")]
    pub(crate) _tx_hash: B256,
    #[serde(rename = "fees")]
    pub(crate) _fees: Fees,
}

/// Errors that can occur during Alpaca API operations.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum AlpacaError {
    /// HTTP request failed (network error, timeout, etc.)
    #[error("HTTP request failed: {message}")]
    Http { message: String },
    /// Authentication failed (401/403 response)
    #[error("Authentication failed: {reason}")]
    Auth { reason: String },
    /// Alpaca API returned an error response
    #[error("API error: {status_code} - {message}")]
    Api { status_code: u16, message: String },
    /// Tokenization request not found when polling status
    #[error("Tokenization request not found: {tokenization_request_id}")]
    RequestNotFound { tokenization_request_id: String },
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
        let request = MintCallbackRequest {
            tokenization_request_id: TokenizationRequestId::new(
                "12345-678-90AB",
            ),
            client_id: ClientId("5505-1234-ABC-4G45".to_string()),
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
        assert_eq!(serialized["client_id"], json!("5505-1234-ABC-4G45"));
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
        let request = RedeemRequest {
            issuer_request_id: IssuerRequestId::new("red-abc123"),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            client_id: ClientId("5505-1234-ABC-4G45".to_string()),
            qty: Quantity::new(Decimal::new(10050, 2)),
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
        assert_eq!(serialized["client_id"], json!("5505-1234-ABC-4G45"));
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
            client_id: ClientId("test".to_string()),
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
