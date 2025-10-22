use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use serde::Serialize;

use crate::account::ClientId;
use crate::mint::TokenizationRequestId;
use crate::tokenized_asset::Network;

#[cfg(test)]
pub(crate) mod mock;

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

/// Errors that can occur during Alpaca API operations.
#[derive(Debug, thiserror::Error)]
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
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use serde_json::json;

    use crate::account::ClientId;
    use crate::mint::TokenizationRequestId;
    use crate::tokenized_asset::Network;

    use super::MintCallbackRequest;

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
