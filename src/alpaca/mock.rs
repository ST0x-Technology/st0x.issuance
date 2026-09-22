use async_trait::async_trait;
use st0x_alpaca::issuer::RedeemResponse;
use st0x_alpaca::issuer::mock::MockIssuerApi;

use super::{
    AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
    TokenizationRequest, TokenizationRequestId,
};

pub(crate) struct MockAlpacaService {
    inner: MockIssuerApi,
}

impl MockAlpacaService {
    pub(crate) fn new_success() -> Self {
        Self { inner: MockIssuerApi::new_success() }
    }

    #[cfg(test)]
    pub(crate) fn new_failure(error_message: impl Into<String>) -> Self {
        Self { inner: MockIssuerApi::new_failure(error_message) }
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn get_call_count(&self) -> usize {
        self.inner.get_call_count()
    }
}

#[async_trait]
impl AlpacaService for MockAlpacaService {
    async fn send_mint_callback(
        &self,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        self.inner.send_mint_callback(request).await
    }

    async fn call_redeem_endpoint(
        &self,
        request: RedeemRequest,
    ) -> Result<RedeemResponse, AlpacaError> {
        self.inner.call_redeem_endpoint(request).await
    }

    async fn poll_request_status(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaError> {
        self.inner.poll_request_status(tokenization_request_id).await
    }
}
