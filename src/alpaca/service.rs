use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use clap::Args;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use super::{
    AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
    TokenizationRequest, TokenizationRequestId,
};

pub(crate) const DEFAULT_CORPORATE_ACTIONS_STREAM_URL: &str = "https://stream.data.alpaca.markets/v1beta1/events/corporate-actions?type=cash_dividend_corporateaction_event,stock_dividend_corporateaction_event&region=us";

/// An operator-approved lower bound for an authenticated corporate-action feed
/// that has no durable cursor.
///
/// Parsing accepts non-future RFC3339 timestamps and normalizes them to UTC for
/// the Alpaca `since` query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorporateActionBootstrapSince(DateTime<Utc>);

impl CorporateActionBootstrapSince {
    pub(crate) fn try_from_instant(
        instant: DateTime<Utc>,
    ) -> Result<Self, CorporateActionBootstrapSinceError> {
        if instant > Utc::now() {
            return Err(CorporateActionBootstrapSinceError::Future(instant));
        }
        Ok(Self(instant))
    }

    pub(crate) fn query_value(&self) -> String {
        self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true)
    }
}

/// An error returned when validating a corporate-action bootstrap boundary.
#[derive(Debug, thiserror::Error)]
pub enum CorporateActionBootstrapSinceError {
    /// The configured value is not a valid RFC3339 timestamp.
    #[error("invalid corporate-action bootstrap timestamp")]
    Parse(#[from] chrono::ParseError),
    /// The configured timestamp is later than the current time.
    #[error("corporate-action bootstrap timestamp {0} is in the future")]
    Future(DateTime<Utc>),
}

impl FromStr for CorporateActionBootstrapSince {
    type Err = CorporateActionBootstrapSinceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let instant = DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc);
        Self::try_from_instant(instant)
    }
}

/// Configuration for Alpaca API integration including credentials and endpoints.
#[derive(Args, Clone)]
pub struct AlpacaConfig {
    #[arg(
        long = "alpaca-api-base-url",
        env = "ALPACA_API_BASE_URL",
        default_value = "https://broker-api.alpaca.markets",
        help = "Alpaca API base URL"
    )]
    pub api_base_url: String,

    #[arg(
        long = "alpaca-account-id",
        env = "ALPACA_ACCOUNT_ID",
        help = "Alpaca tokenization account ID"
    )]
    pub account_id: String,

    #[arg(
        long = "alpaca-api-key",
        env = "ALPACA_API_KEY",
        help = "Alpaca API key ID"
    )]
    pub api_key: String,

    #[arg(
        long = "alpaca-api-secret",
        env = "ALPACA_API_SECRET",
        help = "Alpaca API secret key"
    )]
    pub api_secret: String,

    #[arg(
        long = "alpaca-connect-timeout-secs",
        env = "ALPACA_CONNECT_TIMEOUT_SECS",
        default_value = "10",
        help = "Alpaca API connection timeout in seconds"
    )]
    pub connect_timeout_secs: u64,

    #[arg(
        long = "alpaca-request-timeout-secs",
        env = "ALPACA_REQUEST_TIMEOUT_SECS",
        default_value = "30",
        help = "Alpaca API request timeout in seconds"
    )]
    pub request_timeout_secs: u64,

    #[arg(
        long = "alpaca-corporate-actions-read-timeout-secs",
        env = "ALPACA_CORPORATE_ACTIONS_READ_TIMEOUT_SECS",
        default_value = "90",
        help = "Idle-read timeout for the Alpaca corporate-actions SSE stream"
    )]
    pub corporate_actions_read_timeout_secs: u64,

    #[arg(
        long = "alpaca-corporate-actions-stream-url",
        env = "ALPACA_CORPORATE_ACTIONS_STREAM_URL",
        default_value = DEFAULT_CORPORATE_ACTIONS_STREAM_URL,
        help = "Alpaca corporate-actions SSE stream URL"
    )]
    pub corporate_actions_stream_url: String,

    /// Enables one bounded replay from a validated, non-future RFC3339 instant
    /// when an authenticated corporate-action stream has no cursor. If absent,
    /// that first-install stream remains disabled without stopping issuance.
    #[arg(
        long = "alpaca-corporate-actions-bootstrap-since",
        env = "ALPACA_CORPORATE_ACTIONS_BOOTSTRAP_SINCE",
        help = "Explicit bounded-history bootstrap instant for an authenticated corporate-action stream with no cursor"
    )]
    pub corporate_actions_bootstrap_since:
        Option<CorporateActionBootstrapSince>,
}

impl std::fmt::Debug for AlpacaConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaConfig")
            .field("api_base_url", &self.api_base_url)
            .field("account_id", &self.account_id)
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field(
                "corporate_actions_read_timeout_secs",
                &self.corporate_actions_read_timeout_secs,
            )
            .field(
                "corporate_actions_stream_url",
                &self.corporate_actions_stream_url,
            )
            .field(
                "corporate_actions_bootstrap_since",
                &self.corporate_actions_bootstrap_since,
            )
            .finish()
    }
}

impl AlpacaConfig {
    pub(crate) fn service(
        &self,
    ) -> Result<Arc<dyn AlpacaService>, AlpacaError> {
        let client = st0x_alpaca::AlpacaClient::new(
            self.api_base_url.clone(),
            self.account_id.clone(),
            self.api_key.clone(),
            self.api_secret.clone(),
            Duration::from_secs(self.connect_timeout_secs),
            Duration::from_secs(self.request_timeout_secs),
        )?;
        Ok(Arc::new(InstrumentedAlpacaService { client }))
    }

    pub(crate) fn test_default() -> Self {
        Self {
            api_base_url: "https://example.com".to_string(),
            account_id: "test-account-id".to_string(),
            api_key: "test".to_string(),
            api_secret: "test".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            corporate_actions_read_timeout_secs: 90,
            corporate_actions_stream_url: DEFAULT_CORPORATE_ACTIONS_STREAM_URL
                .to_string(),
            corporate_actions_bootstrap_since: None,
        }
    }
}

struct InstrumentedAlpacaService {
    client: st0x_alpaca::AlpacaClient,
}

#[async_trait]
impl AlpacaService for InstrumentedAlpacaService {
    async fn send_mint_callback(
        &self,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        debug!(
            target: "alpaca",
            account_id = self.client.account_id(),
            method = "POST",
            "Sending mint callback to Alpaca"
        );
        self.client.send_mint_callback(request).await
    }

    async fn call_redeem_endpoint(
        &self,
        request: RedeemRequest,
    ) -> Result<st0x_alpaca::issuer::RedeemResponse, AlpacaError> {
        debug!(
            target: "alpaca",
            account_id = self.client.account_id(),
            method = "POST",
            "Calling Alpaca redeem endpoint"
        );
        self.client.call_redeem_endpoint(request).await
    }

    async fn poll_request_status(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaError> {
        debug!(
            target: "alpaca",
            account_id = self.client.account_id(),
            method = "GET",
            %tokenization_request_id,
            "Polling Alpaca request status"
        );
        self.client.poll_request_status(tokenization_request_id).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::{
        AlpacaConfig, CorporateActionBootstrapSince,
        CorporateActionBootstrapSinceError,
        DEFAULT_CORPORATE_ACTIONS_STREAM_URL,
    };

    #[test]
    fn debug_redacts_credentials_and_preserves_operational_configuration() {
        let config = AlpacaConfig::test_default();
        let debug = format!("{config:?}");

        assert!(!debug.contains("api_key: \"test\""));
        assert!(!debug.contains("api_secret: \"test\""));
        assert!(debug.contains(DEFAULT_CORPORATE_ACTIONS_STREAM_URL));
    }

    #[test]
    fn corporate_action_bootstrap_rejects_future_instants() {
        let result = CorporateActionBootstrapSince::try_from_instant(
            Utc::now() + Duration::minutes(1),
        );

        assert!(matches!(
            result,
            Err(CorporateActionBootstrapSinceError::Future(_))
        ));
    }
}
