use std::sync::Arc;

use async_trait::async_trait;
use clap::Args;

use super::{AlpacaError, AlpacaService, MintCallbackRequest};

#[derive(Debug, Args)]
pub(crate) struct AlpacaConfig {
    #[arg(
        long,
        env,
        default_value = "https://broker-api.alpaca.markets",
        help = "Alpaca API base URL"
    )]
    api_base_url: String,

    #[arg(long, env, help = "Alpaca tokenization account ID")]
    account_id: String,

    #[arg(long, env, help = "Alpaca API key ID")]
    api_key: String,

    #[arg(long, env, help = "Alpaca API secret key")]
    api_secret: String,
}

impl AlpacaConfig {
    pub(crate) fn service(&self) -> Arc<dyn AlpacaService> {
        Arc::new(RealAlpacaService::new(
            self.api_base_url.clone(),
            self.account_id.clone(),
            self.api_key.clone(),
            self.api_secret.clone(),
        ))
    }
}

pub(crate) struct RealAlpacaService {
    client: reqwest::Client,
    base_url: String,
    account_id: String,
    api_key: String,
    api_secret: String,
}

impl RealAlpacaService {
    pub(crate) fn new(
        base_url: String,
        account_id: String,
        api_key: String,
        api_secret: String,
    ) -> Self {
        let client = reqwest::Client::new();
        Self { client, base_url, account_id, api_key, api_secret }
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
            self.base_url, self.account_id
        );

        let response = self
            .client
            .post(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .json(&request)
            .send()
            .await
            .map_err(|e| AlpacaError::Http { message: e.to_string() })?;

        match response.status() {
            reqwest::StatusCode::OK => Ok(()),
            reqwest::StatusCode::UNAUTHORIZED
            | reqwest::StatusCode::FORBIDDEN => Err(AlpacaError::Auth {
                reason: "Authentication failed".to_string(),
            }),
            status => {
                let message = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(AlpacaError::Api { status_code: status.as_u16(), message })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use httpmock::prelude::*;

    use crate::account::ClientId;
    use crate::mint::TokenizationRequestId;
    use crate::tokenized_asset::Network;

    use super::{
        AlpacaError, AlpacaService, MintCallbackRequest, RealAlpacaService,
    };

    fn create_test_request() -> MintCallbackRequest {
        MintCallbackRequest {
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
        }
    }

    #[tokio::test]
    async fn test_send_mint_callback_success() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint")
                .header("authorization", "Basic dGVzdC1rZXk6dGVzdC1zZWNyZXQ=");
            then.status(200).body("");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
        );

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
        );

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

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
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
        );

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(matches!(result, Err(AlpacaError::Auth { .. })));
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint");
            then.status(500).body("Internal Server Error");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
        );

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        match result {
            Err(AlpacaError::Api { status_code, message }) => {
                assert_eq!(status_code, 500);
                assert_eq!(message, "Internal Server Error");
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
                    "client_id": "5505-1234-ABC-4G45",
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
        );

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_mint_callback_uses_basic_auth() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/mint")
                .header("authorization", "Basic bXlrZXk6bXlzZWNyZXQ=");
            then.status(200).body("");
        });

        let service = RealAlpacaService::new(
            server.base_url(),
            "test-account".to_string(),
            "mykey".to_string(),
            "mysecret".to_string(),
        );

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
        );

        let request = create_test_request();
        let result = service.send_mint_callback(request).await;

        assert!(result.is_ok());
        mock.assert();
    }
}
