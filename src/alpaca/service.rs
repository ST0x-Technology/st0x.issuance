use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use clap::Args;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use super::{
    AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
    RedeemResponse, TokenizationRequest,
};
use crate::mint::TokenizationRequestId;

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
}

impl std::fmt::Debug for AlpacaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaConfig")
            .field("api_base_url", &self.api_base_url)
            .field("account_id", &self.account_id)
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .finish()
    }
}

impl AlpacaConfig {
    pub(crate) fn service(
        &self,
    ) -> Result<Arc<dyn AlpacaService>, AlpacaError> {
        let service = RealAlpacaService::new(
            self.api_base_url.clone(),
            self.account_id.clone(),
            self.api_key.clone(),
            self.api_secret.clone(),
            self.connect_timeout_secs,
            self.request_timeout_secs,
        )?;
        Ok(Arc::new(service))
    }

    pub(crate) fn test_default() -> Self {
        Self {
            api_base_url: "https://example.com".to_string(),
            account_id: "test-account-id".to_string(),
            api_key: "test".to_string(),
            api_secret: "test".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        }
    }
}

pub(crate) struct RealAlpacaService {
    client: reqwest::Client,
    base_url: String,
    account_id: String,
    api_key: String,
    api_secret: String,
    max_retries: usize,
}

impl RealAlpacaService {
    pub(crate) fn new(
        base_url: String,
        account_id: String,
        api_key: String,
        api_secret: String,
        connect_timeout_secs: u64,
        request_timeout_secs: u64,
    ) -> Result<Self, AlpacaError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .timeout(Duration::from_secs(request_timeout_secs))
            .build()?;
        Ok(Self {
            client,
            base_url,
            account_id,
            api_key,
            api_secret,
            max_retries: 5,
        })
    }

    #[cfg(test)]
    const fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }
}

#[async_trait]
impl AlpacaService for RealAlpacaService {
    async fn send_mint_callback(
        &self,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/callback/mint",
            self.base_url.trim_end_matches('/'),
            self.account_id
        );

        debug!(target: "alpaca", %url, method = "POST", "Sending mint callback to Alpaca");

        (|| async {
            let response = self
                .client
                .post(&url)
                .basic_auth(&self.api_key, Some(&self.api_secret))
                .header("APCA-API-KEY-ID", &self.api_key)
                .header("APCA-API-SECRET-KEY", &self.api_secret)
                .json(&request)
                .send()
                .await?;

            let status = response.status();

            match status {
                reqwest::StatusCode::OK => Ok(()),
                reqwest::StatusCode::UNAUTHORIZED
                | reqwest::StatusCode::FORBIDDEN => {
                    let body = response.text().await?;
                    Err(AlpacaError::Auth(body))
                }
                status => {
                    let body = response.text().await?;
                    Err(AlpacaError::Api { status_code: status.as_u16(), body })
                }
            }
        })
        .retry(
            ExponentialBuilder::default()
                .with_max_times(self.max_retries)
                .with_jitter(),
        )
        .when(|e: &AlpacaError| e.is_retryable())
        .notify(|err: &AlpacaError, dur: std::time::Duration| {
            tracing::debug!(
                target: "alpaca",
                "Alpaca API call failed with {err}, retrying after {dur:?}"
            );
        })
        .await
    }

    async fn call_redeem_endpoint(
        &self,
        request: RedeemRequest,
    ) -> Result<RedeemResponse, AlpacaError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/callback/redeem",
            self.base_url.trim_end_matches('/'),
            self.account_id
        );

        debug!(target: "alpaca", %url, method = "POST", "Calling Alpaca redeem endpoint");

        let response = self
            .client
            .post(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .json(&request)
            .send()
            .await?;

        let status = response.status();

        match status {
            reqwest::StatusCode::OK => {
                let body = response.text().await?;
                serde_json::from_str(&body).map_err(|e| {
                    tracing::error!(
                        target: "alpaca",
                        %body,
                        error = %e,
                        "Failed to parse Alpaca redeem response"
                    );
                    AlpacaError::Parse { body, source: e }
                })
            }
            reqwest::StatusCode::UNAUTHORIZED
            | reqwest::StatusCode::FORBIDDEN => {
                let body = response.text().await?;
                Err(AlpacaError::Auth(body))
            }
            status => {
                let body = response.text().await?;
                Err(AlpacaError::Api { status_code: status.as_u16(), body })
            }
        }
    }

    async fn poll_request_status(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaError> {
        // Alpaca tokenization_request_ids are server-generated UUIDs, so the
        // path segment needs no percent-encoding.
        let url = format!(
            "{}/v1/accounts/{}/tokenization/requests/{}",
            self.base_url.trim_end_matches('/'),
            self.account_id,
            tokenization_request_id
        );

        debug!(target: "alpaca", %url, method = "GET", %tokenization_request_id, "Polling Alpaca request status");

        (|| async {
            let response = self
                .client
                .get(&url)
                .basic_auth(&self.api_key, Some(&self.api_secret))
                .header("APCA-API-KEY-ID", &self.api_key)
                .header("APCA-API-SECRET-KEY", &self.api_secret)
                .send()
                .await?;

            let status = response.status();

            match status {
                reqwest::StatusCode::OK => {
                    let body = response.text().await?;
                    let request: TokenizationRequest =
                        serde_json::from_str(&body).map_err(|e| {
                            tracing::error!(
                                target: "alpaca",
                                %body,
                                error = %e,
                                "Failed to parse Alpaca request response"
                            );
                            AlpacaError::Parse { body, source: e }
                        })?;
                    // Keyed endpoint retrieves aged requests that no longer
                    // appear in the list endpoint (empirically verified
                    // 2026-06-12, see RAI-912).
                    match &request {
                        TokenizationRequest::Redeem { id, .. } => {
                            debug!(target: "alpaca",
                                tokenization_request_id = %id,
                                "Alpaca keyed request response received"
                            );
                            if *id != *tokenization_request_id {
                                return Err(AlpacaError::ResponseIdMismatch {
                                    requested: tokenization_request_id.clone(),
                                    returned: id.clone(),
                                });
                            }
                        }
                        TokenizationRequest::Mint {} => {
                            warn!(target: "alpaca",
                                %tokenization_request_id,
                                "Alpaca keyed request response received Mint variant (unexpected for redemption polling)"
                            );
                        }
                    }
                    Ok(request)
                }
                reqwest::StatusCode::NOT_FOUND => {
                    let body = response.text().await?;
                    Err(AlpacaError::RequestNotFound {
                        id: tokenization_request_id.clone(),
                        body,
                    })
                }
                reqwest::StatusCode::UNAUTHORIZED
                | reqwest::StatusCode::FORBIDDEN => {
                    let body = response.text().await?;
                    Err(AlpacaError::Auth(body))
                }
                status => {
                    let body = response.text().await?;
                    Err(AlpacaError::Api {
                        status_code: status.as_u16(),
                        body,
                    })
                }
            }
        })
        .retry(
            ExponentialBuilder::default()
                .with_max_times(self.max_retries)
                .with_jitter(),
        )
        .when(|e: &AlpacaError| e.is_retryable())
        .notify(|err: &AlpacaError, dur: std::time::Duration| {
            tracing::debug!(
                target: "alpaca",
                "Alpaca poll request status API call failed with {err}, retrying after {dur:?}"
            );
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use httpmock::prelude::*;
    use tracing_test::traced_test;

    use super::{
        AlpacaError, AlpacaService, MintCallbackRequest, RealAlpacaService,
        RedeemRequest, TokenizationRequest,
    };
    use crate::alpaca::{RedeemRequestStatus, TokenizationRequestType};
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    fn create_test_request() -> MintCallbackRequest {
        let client_id = "55051234-0000-4abc-9000-4aabcdef0045".parse().unwrap();

        MintCallbackRequest {
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
            network: Network::Base,
        }
    }

    #[tokio::test]
    async fn test_send_mint_callback_success() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint")
                .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret");
            then.status(200).body("");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_unauthorized() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint");
            then.status(401).body("Unauthorized");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "wrong-key".to_string(),
            "wrong-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(matches!(result, Err(AlpacaError::Auth(_))));
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_forbidden() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint");
            then.status(403).body("Forbidden");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(matches!(result, Err(AlpacaError::Auth(_))));
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint");
            then.status(400).body("Bad Request");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        match result {
            Err(AlpacaError::Api { status_code, body }) => {
                assert_eq!(status_code, 400);
                assert_eq!(body, "Bad Request");
            }
            _ => panic!("Expected AlpacaError::Api, got {result:?}"),
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_sends_correct_json() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint")
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "tokenization_request_id": "12345-678-90AB",
                    "client_id": "55051234-0000-4abc-9000-4aabcdef0045",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    "network": "base"
                }));
            then.status(200).body("");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_uses_legacy_auth() {
        let server = MockServer::start();

        // Legacy auth requires both Basic auth AND the APCA headers
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint")
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=")
                .header("APCA-API-KEY-ID", "mykey")
                .header("APCA-API-SECRET-KEY", "mysecret");
            then.status(200).body("");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "mykey".to_string(),
            "mysecret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_constructs_correct_url() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST).path(
                "/v1/accounts/my-special-account/tokenization/callback/mint",
            );
            then.status(200).body("");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "my-special-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    fn create_redeem_request() -> RedeemRequest {
        let client_id = "00000000-0000-0000-0000-000000000456".parse().unwrap();

        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        RedeemRequest {
            issuer_request_id: IssuerRedemptionRequestId::new(tx_hash),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            client_id,
            quantity: Quantity::new(rust_decimal::Decimal::from(100)),
            network: Network::Base,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
        }
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_success() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem")
                .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-456",
                "issuer_request_id": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.5"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.tokenization_request_id.0, "tok-456");
        assert_eq!(
            response.issuer_request_id,
            IssuerRedemptionRequestId::new(b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            )),
        );
        assert!(matches!(response.r#type, TokenizationRequestType::Redeem));
        assert!(matches!(response.status, RedeemRequestStatus::Pending));
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_success_without_fees() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-456",
                "issuer_request_id": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "redeem",
                "status": "rejected",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(result.is_ok(), "Expected Ok, got: {result:?}");
        let response = result.unwrap();
        assert_eq!(response.tokenization_request_id.0, "tok-456");
        assert!(matches!(response.status, RedeemRequestStatus::Rejected));
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_unauthorized() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(401).body("Unauthorized");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "wrong-key".to_string(),
            "wrong-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(matches!(result, Err(AlpacaError::Auth(_))));
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_forbidden() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(403).body("Forbidden");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(matches!(result, Err(AlpacaError::Auth(_))));
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(400).body("Invalid request");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        match result {
            Err(AlpacaError::Api { status_code, body }) => {
                assert_eq!(status_code, 400);
                assert_eq!(body, "Invalid request");
            }
            _ => panic!("Expected AlpacaError::Api, got {result:?}"),
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_constructs_correct_url() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/my-special-account/tokenization/callback/redeem");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-789",
                "issuer_request_id": "red-abcdefab",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.0"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "my-special-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_uses_legacy_auth() {
        let server = MockServer::start();

        // Legacy auth requires both Basic auth AND the APCA headers
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem")
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=")
                .header("APCA-API-KEY-ID", "mykey")
                .header("APCA-API-SECRET-KEY", "mysecret");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-001",
                "issuer_request_id": "red-abcdefab",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.0"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "mykey".to_string(),
            "mysecret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_sends_correct_json() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem")
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "issuer_request_id": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "client_id": "00000000-0000-0000-0000-000000000456",
                    "qty": "100",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                }));
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-002",
                "issuer_request_id": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.0"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_poll_request_status_success() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(
                    "/v1/accounts/test-account/tokenization/requests/tok-123",
                )
                .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-123",
                "issuer_request_id": "red-11223344",
                "type": "redeem",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "client_external_account_id": "1481094OM",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "updated_at": "2025-09-12T17:30:00.000000-04:00",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "network": "base",
                "issuer": "test-issuer",
                "fees": "0.5",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let tokenization_request_id = TokenizationRequestId::new("tok-123");
        let result =
            service.poll_request_status(&tokenization_request_id).await;

        let request = result.unwrap();
        assert!(matches!(
            request,
            TokenizationRequest::Redeem {
                status: RedeemRequestStatus::Completed,
                ..
            }
        ));
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &["Polling Alpaca request status"]
            ),
            "Expected DEBUG log for poll request status"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &["Alpaca keyed request response received", "tok-123"]
            ),
            "Expected DEBUG log with tokenization_request_id tok-123"
        );
        mock.assert();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_poll_request_status_not_found() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-NOT-FOUND",
            );
            then.status(404).body("not found");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let tokenization_request_id =
            TokenizationRequestId::new("tok-NOT-FOUND");
        let result =
            service.poll_request_status(&tokenization_request_id).await;

        let err = result.unwrap_err();
        assert!(
            matches!(err, AlpacaError::RequestNotFound { ref id, ref body } if id.0 == "tok-NOT-FOUND" && body == "not found"),
            "Expected RequestNotFound with correct id and body, got {err:?}"
        );
        assert!(!err.is_retryable(), "RequestNotFound must not be retryable");
        mock.assert_calls(1);
    }

    #[tokio::test]
    async fn test_poll_request_status_unauthorized() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-123",
            );
            then.status(401).body("Unauthorized");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "wrong-key".to_string(),
            "wrong-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(&TokenizationRequestId::new("tok-123"))
            .await;

        assert!(matches!(result, Err(AlpacaError::Auth(_))));
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-123",
            );
            then.status(500).body("Internal Server Error");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap()
        .with_max_retries(0);

        let result = service
            .poll_request_status(&TokenizationRequestId::new("tok-123"))
            .await;

        match result {
            Err(AlpacaError::Api { status_code, .. }) => {
                assert_eq!(status_code, 500);
            }
            _ => panic!("Expected Api error, got {result:?}"),
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_uses_legacy_auth() {
        let server = MockServer::start();

        // Legacy auth requires both Basic auth AND the APCA headers
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(
                    "/v1/accounts/test-account/tokenization/requests/tok-auth-test",
                )
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=")
                .header("APCA-API-KEY-ID", "mykey")
                .header("APCA-API-SECRET-KEY", "mysecret");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-auth-test",
                "issuer_request_id": "red-abcdefab",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "updated_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.0"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "mykey".to_string(),
            "mysecret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(&TokenizationRequestId::new("tok-auth-test"))
            .await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_constructs_correct_url() {
        let server = MockServer::start();

        // The mock only matches the exact path with account id and request id
        // as path segments — verifying the URL is constructed correctly.
        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/my-special-account/tokenization/requests/tok-url-test",
            );
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-url-test",
                "issuer_request_id": "red-11223344",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "updated_at": "2025-09-12T17:30:00.000000-04:00",
                "type": "redeem",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.0"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "my-special-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(&TokenizationRequestId::new("tok-url-test"))
            .await;

        assert!(result.is_ok(), "Expected Ok, got {result:?}");
        // mock.assert() verifies exactly this path was called — both account id
        // and tokenization_request_id appear as segments in the URL.
        mock.assert();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_poll_request_status_response_id_mismatch() {
        let server = MockServer::start();

        // Body returns tok-b but request was for tok-a — mismatch must be rejected.
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests/tok-a");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-b",
                "issuer_request_id": "red-11223344",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "updated_at": "2025-09-12T17:30:00.000000-04:00",
                "type": "redeem",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.0"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(&TokenizationRequestId::new("tok-a"))
            .await;

        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                AlpacaError::ResponseIdMismatch {
                    ref requested,
                    ref returned
                } if requested.0 == "tok-a" && returned.0 == "tok-b"
            ),
            "Expected ResponseIdMismatch, got {err:?}"
        );
        assert!(
            !err.is_retryable(),
            "ResponseIdMismatch must not be retryable"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::DEBUG,
                &["Alpaca keyed request response received", "tok-b"]
            ),
            "Expected DEBUG log with returned id tok-b"
        );
        mock.assert();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_poll_request_status_returns_mint_variant_on_200() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/accounts/test-account/tokenization/requests/tok-mint-1",
            );
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-mint-1",
                "issuer_request_id": "00000000-0000-4abc-9000-4aabcdef0045",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "mint",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "fees": "0.0"
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(&TokenizationRequestId::new("tok-mint-1"))
            .await;

        assert!(
            matches!(result, Ok(TokenizationRequest::Mint { .. })),
            "Expected Mint variant without Parse error, got {result:?}"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::WARN,
                &[
                    "Alpaca keyed request response received Mint variant",
                    "tok-mint-1"
                ]
            ),
            "Expected WARN log for Mint variant with tokenization_request_id"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn test_401_returns_non_retryable_auth_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(401).body("Unauthorized");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(matches!(result, Err(AlpacaError::Auth(_))));
        assert!(!result.unwrap_err().is_retryable());
        mock.assert_calls(1);
    }

    #[tokio::test]
    async fn test_400_returns_non_retryable_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(400).body("Bad Request");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(matches!(
            result,
            Err(AlpacaError::Api { status_code: 400, .. })
        ));
        assert!(!result.unwrap_err().is_retryable());
        mock.assert_calls(1);
    }

    #[tokio::test]
    async fn test_500_returns_retryable_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(500).body("Internal Server Error");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap()
        .with_max_retries(0);

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        assert!(matches!(
            result,
            Err(AlpacaError::Api { status_code: 500, .. })
        ));
        assert!(result.unwrap_err().is_retryable());
        mock.assert_calls(1);
    }

    #[tokio::test]
    async fn test_poll_request_status_parses_full_production_response() {
        let target_id = "00000000-0000-0000-0000-000000000001";
        let prod_json = r#"{"tokenization_request_id":"00000000-0000-0000-0000-000000000001","issuer_request_id":"0x1111111111111111111111111111111111111111111111111111111111111111","type":"redeem","status":"completed","underlying_symbol":"SPYM","token_symbol":"tSPYM","qty":"0.000064248","client_external_account_id":"00000000-0000-0000-0000-000000000002","created_at":"2026-06-11T00:02:27.467568Z","updated_at":"2026-06-11T04:02:33.530523Z","wallet_address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","network":"base","issuer":"st0x","fees":"0.01","tx_hash":"0x1111111111111111111111111111111111111111111111111111111111111111"}"#;

        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/test-account/tokenization/requests/{target_id}"
            ));
            then.status(200).body(prod_json);
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(&TokenizationRequestId::new(target_id))
            .await;

        let request = result.unwrap();
        match &request {
            TokenizationRequest::Redeem { id, status, .. } => {
                assert_eq!(id.0, target_id);
                assert!(matches!(status, RedeemRequestStatus::Completed));
            }
            other @ TokenizationRequest::Mint { .. } => {
                panic!("Expected Redeem variant, got {other:?}")
            }
        }
        mock.assert();
    }

    #[tokio::test]
    async fn test_200_with_invalid_json_returns_non_retryable_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(200).body("invalid json");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result = service.call_redeem_endpoint(request).await;

        let err = result.unwrap_err();
        assert!(matches!(err, AlpacaError::Parse { .. }));
        assert!(!err.is_retryable(), "Parse errors must NOT be retryable");
        mock.assert_calls(1);
    }
}
