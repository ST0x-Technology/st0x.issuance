use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use clap::Args;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

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
            tracing::warn!(
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
            "{}/v1/accounts/{}/tokenization/redeem",
            self.base_url.trim_end_matches('/'),
            self.account_id
        );

        debug!(%url, method = "POST", "Calling Alpaca redeem endpoint");

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
    ) -> Result<super::TokenizationRequest, AlpacaError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/requests",
            self.base_url.trim_end_matches('/'),
            self.account_id
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
                .await?;

            let status = response.status();

            match status {
                reqwest::StatusCode::OK => {
                    let body = response.text().await?;
                    let requests: Vec<TokenizationRequest> =
                        serde_json::from_str(&body).map_err(|e| {
                            tracing::error!(
                                %body,
                                error = %e,
                                "Failed to parse Alpaca requests list response"
                            );
                            AlpacaError::Parse { body: body.clone(), source: e }
                        })?;

                    let request = requests
                        .into_iter()
                        .find(|req| {
                            &req.id == tokenization_request_id
                                && req.r#type == super::TokenizationRequestType::Redeem
                        })
                        .ok_or_else(|| {
                            AlpacaError::RequestNotFound(tokenization_request_id.clone())
                        })?;

                    Ok(request)
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
            tracing::warn!(
                "Alpaca poll request status API call failed with {err}, retrying after {dur:?}"
            );
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::alpaca::{RedeemRequestStatus, TokenizationRequestType};
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
    use alloy::primitives::{address, b256};
    use httpmock::prelude::*;
    use serde_json::json;

    use super::{
        AlpacaError, AlpacaService, MintCallbackRequest, RealAlpacaService,
        RedeemRequest,
    };

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
                .path("/v1/accounts/test-account/tokenization/redeem");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-456",
                "issuer_request_id": "00000000-0000-0000-0000-000000000123",
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
                .path("/v1/accounts/test-account/tokenization/redeem");
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
                .path("/v1/accounts/test-account/tokenization/redeem");
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
                .path("/v1/accounts/test-account/tokenization/redeem");
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
                .path("/v1/accounts/my-special-account/tokenization/redeem");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-789",
                "issuer_request_id": "00000000-0000-0000-0000-000000000123",
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
                .path("/v1/accounts/test-account/tokenization/redeem")
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=")
                .header("APCA-API-KEY-ID", "mykey")
                .header("APCA-API-SECRET-KEY", "mysecret");
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-001",
                "issuer_request_id": "00000000-0000-0000-0000-000000000123",
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
                .path("/v1/accounts/test-account/tokenization/redeem")
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "issuer_request_id": "red-abcdefab",
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
    async fn test_poll_request_status_success() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests")
                .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret");
            then.status(200).json_body(serde_json::json!([
                {
                    "tokenization_request_id": "tok-123",
                    "issuer_request_id": "00000000-0000-0000-0000-000000000456",
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
            ]));
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
            then.status(200).json_body(serde_json::json!([
                {
                    "tokenization_request_id": "tok-111",
                    "issuer_request_id": "00000000-0000-0000-0000-000000000001",
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
                    "issuer_request_id": "00000000-0000-0000-0000-000000000002",
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
                    "issuer_request_id": "00000000-0000-0000-0000-000000000003",
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
            ]));
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
            .poll_request_status(&TokenizationRequestId::new("tok-222"))
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
            then.status(200).json_body(serde_json::json!([
                {
                    "tokenization_request_id": "tok-999",
                    "issuer_request_id": "00000000-0000-0000-0000-000000000999",
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
            ]));
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

        match result {
            Err(AlpacaError::RequestNotFound(id)) => {
                assert_eq!(id.0, "tok-NOT-FOUND");
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
            then.status(200).json_body(serde_json::json!([]));
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
            .poll_request_status(&TokenizationRequestId::new("tok-123"))
            .await;

        assert!(matches!(result, Err(AlpacaError::RequestNotFound(_))));
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
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
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
                .path("/v1/accounts/test-account/tokenization/requests")
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=")
                .header("APCA-API-KEY-ID", "mykey")
                .header("APCA-API-SECRET-KEY", "mysecret");
            then.status(200).json_body(serde_json::json!([
                {
                    "tokenization_request_id": "tok-auth-test",
                    "issuer_request_id": "00000000-0000-0000-0000-000000000123",
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
            ]));
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

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/my-special-account/tokenization/requests");
            then.status(200).json_body(serde_json::json!([]));
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

        let _ = service
            .poll_request_status(&TokenizationRequestId::new("tok-123"))
            .await;

        mock.assert();
    }

    #[tokio::test]
    async fn test_poll_request_status_skips_legacy_mint_entries() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(200).json_body(serde_json::json!([
                {
                    "tokenization_request_id": "tok-mint-legacy",
                    "issuer_request_id": "00000000-0000-0000-0000-0000000abc01",
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
                    "issuer_request_id": "00000000-0000-0000-0000-000000000456",
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
            ]));
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
            .poll_request_status(&TokenizationRequestId::new("tok-mint-legacy"))
            .await;

        match result {
            Err(AlpacaError::RequestNotFound(id)) => {
                assert_eq!(id.0, "tok-mint-legacy");
            }
            _ => panic!(
                "Expected RequestNotFound for legacy mint entry, got {result:?}"
            ),
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_401_returns_non_retryable_auth_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/redeem");
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
                .path("/v1/accounts/test-account/tokenization/redeem");
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
                .path("/v1/accounts/test-account/tokenization/redeem");
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

    /// Full production Alpaca response - exact payload from logs.
    /// Used to verify parsing handles all field variations in real responses.
    #[allow(clippy::too_many_lines)]
    fn production_tokenization_requests_json() -> String {
        serde_json::to_string(&json!([
            {
                "tokenization_request_id": "f466e81d-f7c9-4ea3-8b18-35b02775472e",
                "issuer_request_id": "514113fd-0ff9-413a-8e0c-e1212ddf9ecf",
                "type": "mint", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB", "qty": "1",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-01-16T15:37:31.205212Z",
                "updated_at": "2026-01-17T13:33:03.766747Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0.08",
                "tx_hash": "0xac5c7d090d50dac63bfde725480eb411ec60fa06f8ec00c0e6673c69dbd8febf"
            },
            {
                "tokenization_request_id": "65f62c42-0b14-494f-93d4-70379bc03592",
                "issuer_request_id": "00000000-0000-0000-0000-000000000000",
                "type": "mint", "status": "rejected",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB", "qty": "1",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-01-16T19:48:50.712619Z",
                "updated_at": "2026-01-16T19:48:50.712619Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0", "tx_hash": ""
            },
            {
                "tokenization_request_id": "f2354b82-89a1-41e7-86c8-75df9312d6fd",
                "issuer_request_id": "008ab414-0000-0000-0000-000000000000",
                "type": "redeem", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB", "qty": "1",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-01-26T15:39:37.472855Z",
                "updated_at": "2026-01-26T15:39:37.755401Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0.07",
                "tx_hash": "0x008ab414f0e0d0c32a743b865b4f7d7aa509fbff74b6899189ac3085d4a4e026"
            },
            {
                "tokenization_request_id": "b093448c-5871-4a68-ad8b-64611b9fffe7",
                "issuer_request_id": "d6e63f65-345d-4dcd-8b84-e9643987b35e",
                "type": "mint", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB", "qty": "3",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-01-26T16:17:15.136105Z",
                "updated_at": "2026-01-26T16:17:15.757746Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0.19",
                "tx_hash": "0x28680a47f1129c83b83fd9cfaffa37bf462553337b5b9a60c4920133ff10d95f"
            },
            {
                "tokenization_request_id": "f4a13943-ed97-4abc-8c70-505af8bc7d05",
                "issuer_request_id": "069071bb-6c78-42d4-8d6f-8fbcf755dbd3",
                "type": "mint", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB", "qty": "1",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-03T15:17:38.210539Z",
                "updated_at": "2026-02-03T15:17:39.632607Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0.06",
                "tx_hash": "0xbb9e45fb122019f33ba2b7a83ae4bae90045e3bc6cf57e0b3ece70b244ac33f3"
            },
            {
                "tokenization_request_id": "f2280c35-3a82-4460-8c0a-9c51a5526859",
                "issuer_request_id": "942e171e-0000-0000-0000-000000000000",
                "type": "redeem", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB", "qty": "1",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-03T15:27:01.815061Z",
                "updated_at": "2026-02-03T15:27:01.907492Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0.06",
                "tx_hash": "0x942e171ed76ab0286064209315e320ccd540d2c998a0a536ac5ac839370b4637"
            },
            {
                "tokenization_request_id": "0bdf6745-b513-469a-8ddd-874f5192cee8",
                "issuer_request_id": "22e66b2d-0000-0000-0000-000000000000",
                "type": "redeem", "status": "rejected",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB",
                "qty": "0.450574852280275235",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-03T23:59:27.63446Z",
                "updated_at": "2026-02-03T23:59:27.63446Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0",
                "tx_hash": "0x22e66b2dfa9635e63158ef85e665229f222f2a19a92555ddd1b0d09f621cbc60"
            },
            {
                "tokenization_request_id": "5319180c-d5b0-4db0-a4d3-346603b5b353",
                "issuer_request_id": "00000000-0000-0000-0000-000000000001",
                "type": "mint", "status": "rejected",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB",
                "qty": "3.1762222348598623822654992091",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-04T10:20:58.185982Z",
                "updated_at": "2026-02-04T10:20:58.185982Z",
                "wallet_address": "0xe75f9042728c3ad626b1aa283fd9a7fcaf63bf1d",
                "network": "base", "issuer": "st0x", "fees": "0", "tx_hash": ""
            },
            {
                "tokenization_request_id": "a87b2566-e297-4240-99e2-9846c988c400",
                "issuer_request_id": "1246cb25-25d9-42d1-bf04-f83d7bc9f615",
                "type": "mint", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB",
                "qty": "3.176222234",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-04T12:03:27.256796Z",
                "updated_at": "2026-02-04T12:03:29.873395Z",
                "wallet_address": "0xe75f9042728c3ad626b1aa283fd9a7fcaf63bf1d",
                "network": "base", "issuer": "st0x", "fees": "0.2",
                "tx_hash": "0xd391531167cae58f80a0984d1335b8e02b1d757417fdcaa5330fbf6ed024edb2"
            },
            {
                "tokenization_request_id": "adb7d2f6-1e2d-40d8-bcb9-3b6ac382332d",
                "issuer_request_id": "65bcf988-1907-4c87-916a-4b8808ffd26e",
                "type": "mint", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB",
                "qty": "1.588111117",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-04T12:47:38.92726Z",
                "updated_at": "2026-02-04T12:47:41.805323Z",
                "wallet_address": "0xe75f9042728c3ad626b1aa283fd9a7fcaf63bf1d",
                "network": "base", "issuer": "st0x", "fees": "0.1",
                "tx_hash": "0x1224289b2b8f901d74ea3287feb36bf9add03f22789068e9acfdd6902aa6ff73"
            },
            {
                "tokenization_request_id": "8e085fe9-933e-400e-9f7e-6f985c331be0",
                "issuer_request_id": "842331fb-0000-0000-0000-000000000000",
                "type": "redeem", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB",
                "qty": "4.764333351",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-04T14:17:41.396729Z",
                "updated_at": "2026-02-04T16:40:23.393243Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0.07",
                "tx_hash": "0x842331fbcff22069db2702aa3cb8183ec24ed88943cf5a6aa85721c4204046db"
            },
            {
                "tokenization_request_id": "8cd6c568-3de2-417a-8cea-312852e4a31c",
                "issuer_request_id": "7ba33782-0000-0000-0000-000000000000",
                "type": "redeem", "status": "completed",
                "underlying_symbol": "RKLB", "token_symbol": "tRKLB", "qty": "3",
                "account": "1481094OM",
                "issuer_account": "58ced8fa-0cff-4573-aebd-3d6a3cb9f901",
                "created_at": "2026-02-04T14:23:15.465284Z",
                "updated_at": "2026-02-04T16:40:23.460721Z",
                "wallet_address": "0xfcc6238ceb129c6308a567712edc8f8d36db2754",
                "network": "base", "issuer": "st0x", "fees": "0.04",
                "tx_hash": "0x7ba33782a0136bf449a2a6033deaeb0fa2b459d9c6d669e5d4b86d05b172cdca"
            },
        ]))
        .unwrap()
    }

    #[tokio::test]
    async fn test_poll_request_status_parses_full_production_response() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account/tokenization/requests");
            then.status(200).body(production_tokenization_requests_json());
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
            .poll_request_status(&TokenizationRequestId::new(
                "8e085fe9-933e-400e-9f7e-6f985c331be0",
            ))
            .await;

        assert!(result.is_ok(), "Expected Ok, got: {result:?}");
        let request = result.unwrap();
        assert_eq!(
            request.issuer_request_id,
            IssuerRedemptionRequestId::new(b256!(
                "0x842331fb00000000000000000000000000000000000000000000000000000000"
            )),
        );
        assert!(matches!(request.status, RedeemRequestStatus::Completed));
        mock.assert();
    }

    #[tokio::test]
    async fn test_200_with_invalid_json_returns_non_retryable_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/redeem");
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
