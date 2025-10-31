use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{AlpacaError, AlpacaService, MintCallbackRequest};

/// Mock Alpaca service for testing.
///
/// Can be configured to either succeed or fail, and tracks the number of times
/// `send_mint_callback()` was called.
///
/// This mock is NOT behind `#[cfg(test)]` because `setup_test_rocket()` (used by E2E tests
/// in `tests/`) needs to construct it. However, failure-related fields ARE behind
/// `#[cfg(test)]` because E2E tests only exercise the happy path and compile the library
/// without `#[cfg(test)]` enabled. Unit tests (inside the crate) can access `#[cfg(test)]`
/// code, so they get full mock functionality including failure scenarios.
pub(crate) struct MockAlpacaService {
    #[cfg(test)]
    should_succeed: bool,
    #[cfg(test)]
    error_message: Option<String>,
    call_count: Arc<AtomicUsize>,
}

impl MockAlpacaService {
    /// Creates a mock service that will succeed on all calls.
    #[must_use]
    pub(crate) fn new_success() -> Self {
        Self {
            #[cfg(test)]
            should_succeed: true,
            #[cfg(test)]
            error_message: None,
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Creates a mock service that will fail on all calls.
    ///
    /// # Arguments
    ///
    /// * `error_message` - Error message to return in the failure
    #[cfg(test)]
    pub(crate) fn new_failure(error_message: impl Into<String>) -> Self {
        Self {
            should_succeed: false,
            error_message: Some(error_message.into()),
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the number of times `send_mint_callback()` was called.
    #[cfg(test)]
    pub(crate) fn get_call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl AlpacaService for MockAlpacaService {
    async fn send_mint_callback(
        &self,
        _request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        {
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

        #[cfg(not(test))]
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};

    use super::MockAlpacaService;
    use crate::account::ClientId;
    use crate::alpaca::{AlpacaService, MintCallbackRequest};
    use crate::mint::TokenizationRequestId;
    use crate::tokenized_asset::Network;

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
