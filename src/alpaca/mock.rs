use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{AlpacaError, AlpacaService, MintCallbackRequest};

/// Mock Alpaca service for testing.
///
/// Can be configured to either succeed or fail, and tracks the number of times
/// `send_mint_callback()` was called.
pub(crate) struct MockAlpacaService {
    should_succeed: bool,
    error_message: Option<String>,
    call_count: Arc<Mutex<usize>>,
}

impl MockAlpacaService {
    /// Creates a mock service that will succeed on all calls.
    pub(crate) fn new_success() -> Self {
        Self {
            should_succeed: true,
            error_message: None,
            call_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Creates a mock service that will fail on all calls.
    ///
    /// # Arguments
    ///
    /// * `error_message` - Error message to return in the failure
    pub(crate) fn new_failure(error_message: impl Into<String>) -> Self {
        Self {
            should_succeed: false,
            error_message: Some(error_message.into()),
            call_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Returns the number of times `send_mint_callback()` was called.
    pub(crate) fn get_call_count(&self) -> usize {
        *self
            .call_count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl AlpacaService for MockAlpacaService {
    async fn send_mint_callback(
        &self,
        _request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        *self
            .call_count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;

        if self.should_succeed {
            Ok(())
        } else {
            let message = self
                .error_message
                .clone()
                .unwrap_or_else(|| "Mock error".to_string());
            Err(AlpacaError::Http { message })
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};

    use crate::account::ClientId;
    use crate::alpaca::{AlpacaService, MintCallbackRequest};
    use crate::mint::TokenizationRequestId;
    use crate::tokenized_asset::Network;

    use super::MockAlpacaService;

    #[tokio::test]
    async fn test_mock_success_service() {
        let mock = MockAlpacaService::new_success();

        let request = MintCallbackRequest {
            tokenization_request_id: TokenizationRequestId::new("test-123"),
            client_id: ClientId("client-456".to_string()),
            wallet_address: address!(
                "0x1234567890abcdef1234567890abcdef12345678"
            ),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            network: Network::new("base"),
        };

        let result = mock.send_mint_callback(request).await;

        assert!(result.is_ok(), "Expected Ok, got {result:?}");
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_failure_service() {
        let mock = MockAlpacaService::new_failure("Network timeout");

        let request = MintCallbackRequest {
            tokenization_request_id: TokenizationRequestId::new("test-789"),
            client_id: ClientId("client-xyz".to_string()),
            wallet_address: address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            network: Network::new("base"),
        };

        let result = mock.send_mint_callback(request).await;

        assert!(result.is_err(), "Expected Err, got Ok");
        assert_eq!(mock.get_call_count(), 1);

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Network timeout"),
            "Expected error message to contain 'Network timeout', got: {err}"
        );
    }

    #[tokio::test]
    async fn test_mock_tracks_multiple_calls() {
        let mock = MockAlpacaService::new_success();

        let request = MintCallbackRequest {
            tokenization_request_id: TokenizationRequestId::new("test"),
            client_id: ClientId("client".to_string()),
            wallet_address: address!(
                "0x1234567890abcdef1234567890abcdef12345678"
            ),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            network: Network::new("base"),
        };

        mock.send_mint_callback(request.clone()).await.unwrap();
        mock.send_mint_callback(request.clone()).await.unwrap();
        mock.send_mint_callback(request).await.unwrap();

        assert_eq!(mock.get_call_count(), 3);
    }
}
