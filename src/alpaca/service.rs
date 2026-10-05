use async_trait::async_trait;
use clap::Args;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

use super::{
    AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
    TokenizationRequest, TokenizationRequestId,
};

pub(crate) const DEFAULT_CORPORATE_ACTIONS_STREAM_URL: &str = "https://stream.data.alpaca.markets/v1beta1/events/corporate-actions?type=cash_dividend_corporateaction_event,stock_dividend_corporateaction_event&region=us";

pub use st0x_alpaca::corporate_actions::{
    CorporateActionBootstrapSince, CorporateActionBootstrapSinceError,
};

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
    pub api_key: Option<String>,

    #[arg(
        long = "alpaca-api-secret",
        env = "ALPACA_API_SECRET",
        help = "Alpaca API secret key"
    )]
    pub api_secret: Option<String>,

    #[arg(long = "alpaca-client-id", env = "ALPACA_CLIENT_ID")]
    pub client_id: Option<String>,

    #[arg(long = "alpaca-kms-key-version", env = "ALPACA_KMS_KEY_VERSION")]
    pub kms_key_version: Option<String>,

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
            .field("client_id", &self.client_id)
            .field("kms_key_version", &self.kms_key_version)
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

#[derive(Debug, thiserror::Error)]
pub enum AlpacaAuthConfigError {
    #[error(
        "configure exactly one complete Alpaca credential pair: API key/secret or client ID/KMS key version"
    )]
    CredentialPair,
    #[error(
        "Alpaca KMS key version must name projects/*/locations/*/keyRings/*/cryptoKeys/*/cryptoKeyVersions/<positive integer>"
    )]
    KmsKeyVersion,
    #[error("Alpaca KMS auth requires the live or sandbox broker HTTPS origin")]
    BrokerOrigin,
}

impl AlpacaConfig {
    pub(crate) fn auth(
        &self,
    ) -> Result<st0x_alpaca::AlpacaAuth, AlpacaAuthConfigError> {
        use st0x_alpaca::AlpacaAuth;
        match (
            &self.api_key,
            &self.api_secret,
            &self.client_id,
            &self.kms_key_version,
        ) {
            (Some(key), Some(secret), None, None)
                if !key.is_empty() && !secret.is_empty() =>
            {
                Ok(AlpacaAuth::Basic {
                    api_key: key.clone(),
                    api_secret: secret.clone(),
                })
            }
            (None, None, Some(client_id), Some(version))
                if !client_id.is_empty() && !version.is_empty() =>
            {
                let parts: Vec<_> = version.split('/').collect();
                if parts.len() != 10
                    || parts[0] != "projects"
                    || parts[2] != "locations"
                    || parts[4] != "keyRings"
                    || parts[6] != "cryptoKeys"
                    || parts[8] != "cryptoKeyVersions"
                    || [parts[1], parts[3], parts[5], parts[7]]
                        .iter()
                        .any(|p| p.is_empty())
                    || !parts[9].bytes().all(|c| c.is_ascii_digit())
                    || parts[9].parse::<u64>().map_or(true, |v| v == 0)
                {
                    return Err(AlpacaAuthConfigError::KmsKeyVersion);
                }
                Ok(AlpacaAuth::KmsJwt {
                    client_id: client_id.clone(),
                    kms_key_version: version.clone(),
                })
            }
            _ => Err(AlpacaAuthConfigError::CredentialPair),
        }
    }

    pub(crate) fn token_url(
        &self,
    ) -> Result<&'static str, AlpacaAuthConfigError> {
        if matches!(self.auth()?, st0x_alpaca::AlpacaAuth::Basic { .. }) {
            return Ok(st0x_alpaca::ALPACA_TOKEN_URL);
        }
        let url = reqwest::Url::parse(&self.api_base_url)
            .map_err(|_| AlpacaAuthConfigError::BrokerOrigin)?;
        if url.scheme() != "https"
            || url.port().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(AlpacaAuthConfigError::BrokerOrigin);
        }
        match url.host_str() {
            Some("broker-api.alpaca.markets") => {
                Ok(st0x_alpaca::ALPACA_TOKEN_URL)
            }
            Some("broker-api.sandbox.alpaca.markets") => {
                Ok(st0x_alpaca::ALPACA_SANDBOX_TOKEN_URL)
            }
            _ => Err(AlpacaAuthConfigError::BrokerOrigin),
        }
    }
    pub(crate) fn service(
        &self,
    ) -> Result<Arc<dyn AlpacaService>, AlpacaError> {
        self.service_with_auth(
            self.auth().map_err(|e| AlpacaError::Auth(e.to_string()))?,
            self.token_url().map_err(|e| AlpacaError::Auth(e.to_string()))?,
        )
    }

    pub(crate) fn service_with_auth(
        &self,
        auth: st0x_alpaca::AlpacaAuth,
        token_url: &str,
    ) -> Result<Arc<dyn AlpacaService>, AlpacaError> {
        let client = st0x_alpaca::AlpacaClient::with_auth(
            &self.api_base_url,
            self.account_id.clone(),
            auth,
            token_url,
            Duration::from_secs(self.connect_timeout_secs),
            Duration::from_secs(self.request_timeout_secs),
        )?;
        Ok(Arc::new(InstrumentedAlpacaService { client }))
    }

    pub(crate) fn test_default() -> Self {
        Self {
            api_base_url: "https://example.com".to_string(),
            account_id: "test-account-id".to_string(),
            api_key: Some("test".to_string()),
            api_secret: Some("test".to_string()),
            client_id: None,
            kms_key_version: None,
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            corporate_actions_read_timeout_secs: 90,
            corporate_actions_stream_url: DEFAULT_CORPORATE_ACTIONS_STREAM_URL
                .to_string(),
            corporate_actions_bootstrap_since: None,
        }
    }
}

pub(crate) fn alert_credential_rejection(error: &AlpacaError) -> bool {
    let rejected = match error {
        AlpacaError::Jwt(error) => error.is_deterministic(),
        AlpacaError::Auth(_) => true,
        _ => false,
    };
    if rejected {
        error!(target: "operational_alert", error = %error, "Alpaca credential rejected");
    }
    rejected
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
        let started_at = Instant::now();
        let result = self.client.send_mint_callback(request).await;
        debug!(target: "alpaca", elapsed = ?started_at.elapsed(), success = result.is_ok(),
            "Alpaca mint callback finished");
        result
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
        let started_at = Instant::now();
        let result = self.client.call_redeem_endpoint(request).await;
        debug!(target: "alpaca", elapsed = ?started_at.elapsed(), success = result.is_ok(),
            "Alpaca redeem call finished");
        if let Err(AlpacaError::Parse { body, source }) = &result {
            error!(target: "alpaca", %body, error = %source,
                "Failed to parse Alpaca redeem response");
        }
        result
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
        let started_at = Instant::now();
        let result =
            self.client.poll_request_status(tokenization_request_id).await;
        debug!(target: "alpaca", elapsed = ?started_at.elapsed(), success = result.is_ok(),
            "Alpaca request poll finished");
        match &result {
            Ok(TokenizationRequest::Redeem { id, .. }) => {
                debug!(target: "alpaca", tokenization_request_id = %id,
                    "Alpaca keyed request response received");
            }
            Ok(TokenizationRequest::Mint {}) => {
                warn!(target: "alpaca", %tokenization_request_id,
                    "Alpaca keyed request response received Mint variant (unexpected for redemption polling)");
            }
            Err(AlpacaError::Parse { body, source }) => {
                error!(target: "alpaca", %body, error = %source,
                    "Failed to parse Alpaca request response");
            }
            Err(_) => {}
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use clap::Parser;
    use httpmock::prelude::*;
    use tracing_test::traced_test;

    use crate::alpaca::{
        AlpacaError, TokenizationRequest, TokenizationRequestId,
    };
    use crate::test_utils::logs_contain_at;

    use super::{
        AlpacaConfig, CorporateActionBootstrapSince,
        CorporateActionBootstrapSinceError,
        DEFAULT_CORPORATE_ACTIONS_STREAM_URL,
    };

    #[derive(Debug, Parser)]
    struct AlpacaConfigTestCli {
        #[command(flatten)]
        alpaca: AlpacaConfig,
    }

    fn parse_config_with_bootstrap(
        bootstrap_since: &str,
    ) -> Result<AlpacaConfigTestCli, clap::Error> {
        AlpacaConfigTestCli::try_parse_from([
            "test",
            "--alpaca-account-id",
            "test-account",
            "--alpaca-api-key",
            "test-key",
            "--alpaca-api-secret",
            "test-secret",
            "--alpaca-corporate-actions-bootstrap-since",
            bootstrap_since,
        ])
    }

    #[test]
    fn accepts_and_normalizes_an_explicit_corporate_action_bootstrap_instant() {
        let config =
            parse_config_with_bootstrap("2026-08-30T21:00:00-03:00").unwrap();
        assert_eq!(
            config
                .alpaca
                .corporate_actions_bootstrap_since
                .unwrap()
                .query_value(),
            "2026-08-31T00:00:00Z"
        );
    }

    #[test]
    fn rejects_a_malformed_corporate_action_bootstrap_instant() {
        assert!(parse_config_with_bootstrap("yesterday").is_err());
    }

    #[test]
    fn rejects_a_future_corporate_action_bootstrap_instant() {
        assert!(parse_config_with_bootstrap("2999-01-01T00:00:00Z").is_err());
    }

    #[test]
    fn debug_redacts_credentials_and_preserves_operational_configuration() {
        let config = AlpacaConfig::test_default();
        let debug = format!("{config:?}");

        assert!(!debug.contains("api_key: \"test\""));
        assert!(!debug.contains("api_secret: \"test\""));
        assert!(debug.contains(DEFAULT_CORPORATE_ACTIONS_STREAM_URL));
    }

    #[test]
    fn debug_shows_kms_identifiers_and_redacts_basic_credentials() {
        let mut config = AlpacaConfig::test_default();
        config.client_id = Some("s01-client".into());
        config.kms_key_version = Some("projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1".into());
        let debug = format!("{config:?}");
        assert!(debug.contains("s01-client"));
        assert!(debug.contains("cryptoKeyVersions/1"));
        assert!(!debug.contains("Some(\"test\")"));
    }

    #[test]
    fn basic_auth_preserves_loopback_and_custom_broker_endpoints() {
        let mut config = AlpacaConfig::test_default();
        config.api_base_url = "http://127.0.0.1:8000".into();
        assert!(matches!(
            config.auth().unwrap(),
            st0x_alpaca::AlpacaAuth::Basic { .. }
        ));
        assert_eq!(config.token_url().unwrap(), st0x_alpaca::ALPACA_TOKEN_URL);
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

    fn service_for(
        server: &MockServer,
    ) -> std::sync::Arc<dyn super::AlpacaService> {
        let mut config = AlpacaConfig::test_default();
        config.api_base_url = server.base_url();
        config.account_id = "test-account".into();
        config.service().unwrap()
    }

    #[tokio::test]
    async fn issuer_service_uses_cached_bearer_without_apca_headers() {
        use p256::pkcs8::EncodePrivateKey;
        let server = MockServer::start();
        let token = server.mock(|when, then| {
            when.method(POST).path("/token");
            then.status(200).json_body(serde_json::json!({
                "access_token": "s01-issuer-bearer",
                "token_type": "Bearer", "expires_in": 900
            }));
        });
        let poll = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests/tok-bearer")
                .header("Authorization", "Bearer s01-issuer-bearer")
                .header_missing("APCA-API-KEY-ID")
                .header_missing("APCA-API-SECRET-KEY");
            then.status(200).json_body(serde_json::json!({"type":"mint"}));
        });
        let key = p256::SecretKey::from_slice(&[7_u8; 32]).unwrap();
        let mut config = AlpacaConfig::test_default();
        config.api_base_url = server.base_url();
        config.account_id = "test-account".into();
        let service = config
            .service_with_auth(
                st0x_alpaca::AlpacaAuth::PrivateKeyJwt {
                    client_id: "s01-test-client".into(),
                    private_key_pem: key
                        .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
                        .unwrap()
                        .to_string(),
                },
                &format!("{}/token", server.base_url()),
            )
            .unwrap();
        let request_id = TokenizationRequestId::new("tok-bearer");
        for _ in 0..2 {
            assert!(matches!(
                service.poll_request_status(&request_id).await.unwrap(),
                TokenizationRequest::Mint {}
            ));
        }
        token.assert_calls(1);
        poll.assert_calls(2);
    }

    #[traced_test]
    #[tokio::test]
    async fn keyed_poll_preserves_not_found_and_outcome_log() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-missing",
            );
            then.status(404).body("not found");
        });

        let error = service_for(&server)
            .poll_request_status(&TokenizationRequestId::new("tok-missing"))
            .await
            .unwrap_err();
        assert!(matches!(error, AlpacaError::RequestNotFound { .. }));
        assert!(!error.is_retryable());
        mock.assert_calls(1);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Alpaca request poll finished", "success=false"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn keyed_poll_warns_on_unexpected_mint_variant() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-mint-1",
            );
            then.status(200).json_body(serde_json::json!({"type": "mint"}));
        });

        let result = service_for(&server)
            .poll_request_status(&TokenizationRequestId::new("tok-mint-1"))
            .await
            .unwrap();
        assert!(matches!(result, TokenizationRequest::Mint {}));
        mock.assert_calls(1);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Alpaca keyed request response received Mint variant",
                "tok-mint-1"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn keyed_poll_accepts_omitted_transaction_hash_and_logs_receipt() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-456",
            );
            then.status(200).json_body(serde_json::json!({
                "type": "redeem",
                "tokenization_request_id": "tok-456",
                "issuer_request_id": "red-574378e0",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "50.00",
                "network": "base",
                "wallet_address": "0x9999999999999999999999999999999999999999",
                "updated_at": "2025-09-12T17:30:00-04:00"
            }));
        });

        let result = service_for(&server)
            .poll_request_status(&TokenizationRequestId::new("tok-456"))
            .await
            .unwrap();
        assert!(matches!(
            result,
            TokenizationRequest::Redeem { tx_hash: None, .. }
        ));
        mock.assert_calls(1);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Alpaca keyed request response received", "tok-456"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn keyed_poll_keeps_auth_failure_non_retryable() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-auth",
            );
            then.status(401).body("Unauthorized");
        });

        let error = service_for(&server)
            .poll_request_status(&TokenizationRequestId::new("tok-auth"))
            .await
            .unwrap_err();
        assert!(matches!(error, AlpacaError::Auth(_)));
        assert!(!error.is_retryable());
        mock.assert_calls(1);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Alpaca request poll finished", "success=false"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn keyed_poll_retries_transient_errors_and_logs_final_outcome() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-retry",
            );
            then.status(500).body("temporary failure");
        });

        let error = service_for(&server)
            .poll_request_status(&TokenizationRequestId::new("tok-retry"))
            .await
            .unwrap_err();
        assert!(matches!(error, AlpacaError::Api { status_code: 500, .. }));
        assert!(error.is_retryable());
        mock.assert_calls(6);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Alpaca request poll finished", "success=false"]
        ));
    }
}
