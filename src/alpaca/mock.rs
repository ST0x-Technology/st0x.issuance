use async_trait::async_trait;
use st0x_alpaca::issuer::RedeemResponse;
use st0x_alpaca::issuer::mock::MockIssuerApi;
use std::time::Duration;

use super::{
    AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
    TokenizationRequest, TokenizationRequestId,
};

pub(crate) struct MockAlpacaService {
    inner: MockIssuerApi,
    callback_delay: Duration,
}

impl MockAlpacaService {
    pub(crate) fn new_success() -> Self {
        Self {
            inner: MockIssuerApi::new_success(),
            callback_delay: Duration::ZERO,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_failure(error_message: impl Into<String>) -> Self {
        Self {
            inner: MockIssuerApi::new_failure(error_message),
            callback_delay: Duration::ZERO,
        }
    }

    #[cfg(test)]
    pub(crate) const fn with_callback_delay(
        mut self,
        delay_milliseconds: u64,
    ) -> Self {
        self.callback_delay = Duration::from_millis(delay_milliseconds);
        self
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
        if !self.callback_delay.is_zero() {
            tokio::time::sleep(self.callback_delay).await;
        }
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
