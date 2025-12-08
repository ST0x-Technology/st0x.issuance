use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use clap::Args;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use super::{
    AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
    RedeemResponse, RequestsListResponse,
};
use crate::account::AlpacaAccountNumber;
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
    api_key: String,
    api_secret: String,
    max_retries: usize,
}

impl RealAlpacaService {
    pub(crate) fn new(
        base_url: String,
        api_key: String,
        api_secret: String,
        connect_timeout_secs: u64,
        request_timeout_secs: u64,
    ) -> Result<Self, AlpacaError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .timeout(Duration::from_secs(request_timeout_secs))
            .build()
            .map_err(|e| AlpacaError::Http {
                message: format!("Failed to build HTTP client: {e}"),
            })?;
        Ok(Self { client, base_url, api_key, api_secret, max_retries: 5 })
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
        alpaca_account: &AlpacaAccountNumber,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/callback/mint",
            self.base_url.trim_end_matches('/'),
            alpaca_account.as_str()
        );

        debug!(%url, method = "POST", "Sending mint callback to Alpaca");

        (|| async {
            let response = self
                .client
                .post(&url)
                .basic_auth(&self.api_key, Some(&self.api_secret))
                .header("APCA-API-KEY-ID", &self.api_key)
                .header("APCA-API-SECRET-KEY", &self.api_secret)
                .json(&request)
                .send()
                .await
                .map_err(|e| AlpacaError::Http { message: e.to_string() })?;

            let status = response.status();

            match status {
                reqwest::StatusCode::OK => Ok(()),
                reqwest::StatusCode::UNAUTHORIZED
                | reqwest::StatusCode::FORBIDDEN => {
                    let body =
                        response.text().await.unwrap_or_else(|_| String::new());
                    let snippet = body.chars().take(200).collect::<String>();
                    let reason = if snippet.is_empty() {
                        "Authentication failed".to_string()
                    } else {
                        format!("Authentication failed: {snippet}")
                    };
                    Err(AlpacaError::Auth { reason })
                }
                status => {
                    let message = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "Unknown error".to_string());
                    Err(AlpacaError::Api {
                        status_code: status.as_u16(),
                        message,
                    })
                }
            }
        })
        .retry(
            ExponentialBuilder::default()
                .with_max_times(self.max_retries)
                .with_jitter(),
        )
        .when(|e: &AlpacaError| {
            matches!(
                e,
                AlpacaError::Http { .. }
                    | AlpacaError::Api { status_code: 500..=599 | 429, .. }
            )
        })
        .notify(|err: &AlpacaError, dur: std::time::Duration| {
            tracing::warn!(
                "Alpaca API call failed with {err}, retrying after {dur:?}"
            );
        })
        .await
    }

    async fn call_redeem_endpoint(
        &self,
        alpaca_account: &AlpacaAccountNumber,
        request: RedeemRequest,
    ) -> Result<RedeemResponse, AlpacaError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/redeem",
            self.base_url.trim_end_matches('/'),
            alpaca_account.as_str()
        );

        debug!(%url, method = "POST", "Calling Alpaca redeem endpoint");

        (|| async {
            let response = self
                .client
                .post(&url)
                .basic_auth(&self.api_key, Some(&self.api_secret))
                .header("APCA-API-KEY-ID", &self.api_key)
                .header("APCA-API-SECRET-KEY", &self.api_secret)
                .json(&request)
                .send()
                .await
                .map_err(|e| AlpacaError::Http { message: e.to_string() })?;

            let status = response.status();

            match status {
                reqwest::StatusCode::OK => {
                    let redeem_response = response.json::<RedeemResponse>().await.map_err(
                        |e| AlpacaError::Http {
                            message: format!("Failed to parse response: {e}"),
                        },
                    )?;
                    Ok(redeem_response)
                }
                reqwest::StatusCode::UNAUTHORIZED
                | reqwest::StatusCode::FORBIDDEN => {
                    let body =
                        response.text().await.unwrap_or_else(|_| String::new());
                    let snippet = body.chars().take(200).collect::<String>();
                    let reason = if snippet.is_empty() {
                        "Authentication failed".to_string()
                    } else {
                        format!("Authentication failed: {snippet}")
                    };
                    Err(AlpacaError::Auth { reason })
                }
                status => {
                    let message = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "Unknown error".to_string());
                    Err(AlpacaError::Api {
                        status_code: status.as_u16(),
                        message,
                    })
                }
            }
        })
        .retry(
            ExponentialBuilder::default()
                .with_max_times(self.max_retries)
                .with_jitter(),
        )
        .when(|e: &AlpacaError| {
            matches!(
                e,
                AlpacaError::Http { .. }
                    | AlpacaError::Api { status_code: 500..=599 | 429, .. }
            )
        })
        .notify(|err: &AlpacaError, dur: std::time::Duration| {
            tracing::warn!(
                "Alpaca redeem API call failed with {err}, retrying after {dur:?}"
            );
        })
        .await
    }

    async fn poll_request_status(
        &self,
        alpaca_account: &AlpacaAccountNumber,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<super::TokenizationRequest, AlpacaError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/requests",
            self.base_url.trim_end_matches('/'),
            alpaca_account.as_str()
        );

        debug!(%url, method = "GET", "Polling Alpaca request status");

        (|| async {
            let response = self
                .client
                .get(&url)
                .basic_auth(&self.api_key, Some(&self.api_secret))
                .header("APCA-API-KEY-ID", &self.api_key)
                .header("APCA-API-SECRET-KEY", &self.api_secret)
                .send()
                .await
                .map_err(|e| AlpacaError::Http { message: e.to_string() })?;

            let status = response.status();

            match status {
                reqwest::StatusCode::OK => {
                    let list_response = response
                        .json::<RequestsListResponse>()
                        .await
                        .map_err(|e| AlpacaError::Http {
                            message: format!("Failed to parse response: {e}"),
                        })?;

                    let request = list_response
                        .requests
                        .into_iter()
                        .find(|req| {
                            &req.id == tokenization_request_id
                                && req.r#type == super::TokenizationRequestType::Redeem
                        })
                        .ok_or_else(|| AlpacaError::RequestNotFound {
                            tokenization_request_id: tokenization_request_id.0.clone(),
                        })?;

                    Ok(request)
                }
                reqwest::StatusCode::UNAUTHORIZED
                | reqwest::StatusCode::FORBIDDEN => {
                    let body =
                        response.text().await.unwrap_or_else(|_| String::new());
                    let snippet = body.chars().take(200).collect::<String>();
                    let reason = if snippet.is_empty() {
                        "Authentication failed".to_string()
                    } else {
                        format!("Authentication failed: {snippet}")
                    };
                    Err(AlpacaError::Auth { reason })
                }
                status => {
                    let message = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "Unknown error".to_string());
                    Err(AlpacaError::Api {
                        status_code: status.as_u16(),
                        message,
                    })
                }
            }
        })
        .retry(
            ExponentialBuilder::default()
                .with_max_times(self.max_retries)
                .with_jitter(),
        )
        .when(|e: &AlpacaError| {
            matches!(
                e,
                AlpacaError::Http { .. }
                    | AlpacaError::Api { status_code: 500..=599 | 429, .. }
            )
        })
        .notify(|err: &AlpacaError, dur: std::time::Duration| {
            tracing::warn!(
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

    use crate::account::AlpacaAccountNumber;
    use crate::alpaca::{RedeemRequestStatus, TokenizationRequestType};
    use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    use super::{
        AlpacaError, AlpacaService, MintCallbackRequest, RealAlpacaService,
        RedeemRequest,
    };

    fn test_alpaca_account() -> AlpacaAccountNumber {
        AlpacaAccountNumber("test-account".to_string())
    }

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
            network: Network::new("base"),
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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result =
            service.send_mint_callback(&test_alpaca_account(), request).await;

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
            "wrong-key".to_string(),
            "wrong-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result =
            service.send_mint_callback(&test_alpaca_account(), request).await;

        assert!(matches!(result, Err(AlpacaError::Auth { .. })));
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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result =
            service.send_mint_callback(&test_alpaca_account(), request).await;

        assert!(matches!(result, Err(AlpacaError::Auth { .. })));
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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result =
            service.send_mint_callback(&test_alpaca_account(), request).await;

        match result {
            Err(AlpacaError::Api { status_code, message }) => {
                assert_eq!(status_code, 400);
                assert_eq!(message, "Bad Request");
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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result =
            service.send_mint_callback(&test_alpaca_account(), request).await;

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
            "mykey".to_string(),
            "mysecret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_test_request();
        let result =
            service.send_mint_callback(&test_alpaca_account(), request).await;

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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let alpaca_account =
            AlpacaAccountNumber("my-special-account".to_string());
        let request = create_test_request();
        let result = service.send_mint_callback(&alpaca_account, request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    fn create_redeem_request() -> RedeemRequest {
        let client_id = "00000000-0000-0000-0000-000000000456".parse().unwrap();

        RedeemRequest {
            issuer_request_id: IssuerRequestId::new("red-123"),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            client_id,
            quantity: Quantity::new(rust_decimal::Decimal::from(100)),
            network: Network::new("base"),
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
                .path("/v1/accounts/test-account/tokenization/redeem")
                .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-456",
                "issuer_request_id": "red-123",
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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result =
            service.call_redeem_endpoint(&test_alpaca_account(), request).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.tokenization_request_id.0, "tok-456");
        assert_eq!(response.issuer_request_id.0, "red-123");
        assert!(matches!(response.r#type, TokenizationRequestType::Redeem));
        assert!(matches!(response.status, RedeemRequestStatus::Pending));
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_unauthorized() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/redeem");
            then.status(401).body("Unauthorized");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "wrong-key".to_string(),
            "wrong-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result =
            service.call_redeem_endpoint(&test_alpaca_account(), request).await;

        assert!(matches!(result, Err(AlpacaError::Auth { .. })));
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_forbidden() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/redeem");
            then.status(403).body("Forbidden");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result =
            service.call_redeem_endpoint(&test_alpaca_account(), request).await;

        assert!(matches!(result, Err(AlpacaError::Auth { .. })));
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/redeem");
            then.status(400).body("Invalid request");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result =
            service.call_redeem_endpoint(&test_alpaca_account(), request).await;

        match result {
            Err(AlpacaError::Api { status_code, message }) => {
                assert_eq!(status_code, 400);
                assert_eq!(message, "Invalid request");
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
                .path("/v1/accounts/my-special-account/tokenization/redeem");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-789",
                "issuer_request_id": "red-123",
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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let alpaca_account =
            AlpacaAccountNumber("my-special-account".to_string());
        let request = create_redeem_request();
        let result =
            service.call_redeem_endpoint(&alpaca_account, request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_uses_legacy_auth() {
        let server = MockServer::start();

        // Legacy auth requires both Basic auth AND the APCA headers
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/redeem")
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=")
                .header("APCA-API-KEY-ID", "mykey")
                .header("APCA-API-SECRET-KEY", "mysecret");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-001",
                "issuer_request_id": "red-123",
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
            "mykey".to_string(),
            "mysecret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result =
            service.call_redeem_endpoint(&test_alpaca_account(), request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_call_redeem_endpoint_sends_correct_json() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/redeem")
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "issuer_request_id": "red-123",
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
                "issuer_request_id": "red-123",
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
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let request = create_redeem_request();
        let result =
            service.call_redeem_endpoint(&test_alpaca_account(), request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_success() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests")
                .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret");
            then.status(200).json_body(serde_json::json!({
                "requests": [
                    {
                        "tokenization_request_id": "tok-123",
                        "issuer_request_id": "red-456",
                        "created_at": "2025-09-12T17:28:48.642437-04:00",
                        "type": "redeem",
                        "status": "completed",
                        "underlying_symbol": "AAPL",
                        "token_symbol": "tAAPL",
                        "qty": "100",
                        "issuer": "test-issuer",
                        "network": "base",
                        "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "fees": "0.5"
                    }
                ]
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let tokenization_request_id = TokenizationRequestId::new("tok-123");
        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &tokenization_request_id,
            )
            .await;

        assert!(result.is_ok());
        let request = result.unwrap();
        assert!(matches!(request.status, RedeemRequestStatus::Completed));
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_filters_correctly() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(200).json_body(serde_json::json!({
                "requests": [
                    {
                        "tokenization_request_id": "tok-111",
                        "issuer_request_id": "red-1",
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
                    },
                    {
                        "tokenization_request_id": "tok-222",
                        "issuer_request_id": "red-2",
                        "created_at": "2025-09-12T17:30:00.000000-04:00",
                        "type": "redeem",
                        "status": "completed",
                        "underlying_symbol": "TSLA",
                        "token_symbol": "tTSLA",
                        "qty": "50",
                        "issuer": "test-issuer",
                        "network": "base",
                        "wallet_address": "0x9876543210fedcba9876543210fedcba98765432",
                        "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                        "fees": "0.0"
                    },
                    {
                        "tokenization_request_id": "tok-333",
                        "issuer_request_id": "red-3",
                        "created_at": "2025-09-12T17:31:00.000000-04:00",
                        "type": "redeem",
                        "status": "rejected",
                        "underlying_symbol": "NVDA",
                        "token_symbol": "tNVDA",
                        "qty": "25",
                        "issuer": "test-issuer",
                        "network": "base",
                        "wallet_address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "tx_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                        "fees": "0.0"
                    }
                ]
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &TokenizationRequestId::new("tok-222"),
            )
            .await;

        assert!(result.is_ok());
        let request = result.unwrap();
        assert!(matches!(request.status, RedeemRequestStatus::Completed));
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_not_found() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(200).json_body(serde_json::json!({
                "requests": [
                    {
                        "tokenization_request_id": "tok-999",
                        "issuer_request_id": "red-999",
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
                    }
                ]
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let tokenization_request_id =
            TokenizationRequestId::new("tok-NOT-FOUND");
        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &tokenization_request_id,
            )
            .await;

        match result {
            Err(AlpacaError::RequestNotFound {
                tokenization_request_id: id,
            }) => {
                assert_eq!(id, "tok-NOT-FOUND");
            }
            _ => panic!("Expected RequestNotFound error, got {result:?}"),
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_empty_list() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(200).json_body(serde_json::json!({
                "requests": []
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &TokenizationRequestId::new("tok-123"),
            )
            .await;

        assert!(matches!(result, Err(AlpacaError::RequestNotFound { .. })));
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_unauthorized() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(401).body("Unauthorized");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "wrong-key".to_string(),
            "wrong-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &TokenizationRequestId::new("tok-123"),
            )
            .await;

        assert!(matches!(result, Err(AlpacaError::Auth { .. })));
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(500).body("Internal Server Error");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap()
        .with_max_retries(0);

        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &TokenizationRequestId::new("tok-123"),
            )
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
                .path("/v1/accounts/test-account/tokenization/requests")
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=")
                .header("APCA-API-KEY-ID", "mykey")
                .header("APCA-API-SECRET-KEY", "mysecret");
            then.status(200).json_body(serde_json::json!({
                "requests": [
                    {
                        "tokenization_request_id": "tok-auth-test",
                        "issuer_request_id": "red-123",
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
                    }
                ]
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "mykey".to_string(),
            "mysecret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &TokenizationRequestId::new("tok-auth-test"),
            )
            .await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_constructs_correct_url() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/my-special-account/tokenization/requests");
            then.status(200).json_body(serde_json::json!({
                "requests": []
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let alpaca_account =
            AlpacaAccountNumber("my-special-account".to_string());
        let _ = service
            .poll_request_status(
                &alpaca_account,
                &TokenizationRequestId::new("tok-123"),
            )
            .await;

        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_skips_legacy_mint_entries() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(200).json_body(serde_json::json!({
                "requests": [
                    {
                        "tokenization_request_id": "tok-mint-legacy",
                        "issuer_request_id": "mint-123",
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
                    },
                    {
                        "tokenization_request_id": "tok-redeem-valid",
                        "issuer_request_id": "red-456",
                        "created_at": "2025-09-12T17:30:00.000000-04:00",
                        "type": "redeem",
                        "status": "pending",
                        "underlying_symbol": "TSLA",
                        "token_symbol": "tTSLA",
                        "qty": "50",
                        "issuer": "test-issuer",
                        "network": "base",
                        "wallet_address": "0x9876543210fedcba9876543210fedcba98765432",
                        "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                        "fees": "0.0"
                    }
                ]
            }));
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-key".to_string(),
            "test-secret".to_string(),
            10,
            30,
        )
        .unwrap();

        let result = service
            .poll_request_status(
                &test_alpaca_account(),
                &TokenizationRequestId::new("tok-mint-legacy"),
            )
            .await;

        match result {
            Err(AlpacaError::RequestNotFound {
                tokenization_request_id: id,
            }) => {
                assert_eq!(id, "tok-mint-legacy");
            }
            _ => panic!(
                "Expected RequestNotFound for legacy mint entry, got {result:?}"
            ),
        }

        mock.assert();
    }
}
