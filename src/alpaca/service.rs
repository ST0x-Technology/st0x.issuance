use async_trait::async_trait;
use clap::Args;
use serde_json::Value;
use st0x_alpaca::AlpacaClient;
use st0x_alpaca::issuer::RedeemResponse;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use super::{
    AlpacaError, AlpacaService, MintCallbackRequest, RedeemRequest,
    TokenizationRequest, TokenizationRequestId,
};

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
        let client = AlpacaClient::new(
            self.api_base_url.clone(),
            self.account_id.clone(),
            self.api_key.clone(),
            self.api_secret.clone(),
            Duration::from_secs(self.connect_timeout_secs),
            Duration::from_secs(self.request_timeout_secs),
        )?;
        Ok(Arc::new(SharedAlpacaService { client }))
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

struct SharedAlpacaService {
    client: AlpacaClient,
}

#[async_trait]
impl AlpacaService for SharedAlpacaService {
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
    ) -> Result<RedeemResponse, AlpacaError> {
        debug!(
            target: "alpaca",
            account_id = self.client.account_id(),
            method = "POST",
            "Calling Alpaca redeem endpoint"
        );
        self.client
            .with_retry(|| {
                let request = request.clone();
                async { self.client.call_redeem_endpoint(request).await }
            })
            .await
    }

    async fn poll_request_status(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/requests/{}",
            self.client.base_url(),
            self.client.account_id(),
            tokenization_request_id
        );

        debug!(
            target: "alpaca",
            %url,
            method = "GET",
            %tokenization_request_id,
            "Polling Alpaca request status"
        );

        self.client
            .with_retry(|| async {
                let response = self.client.get(&url).send().await?;
                let status_code = response.status().as_u16();

                match status_code {
                    200 => {
                        let body = response.text().await?;
                        let request = parse_tokenization_request(body)?;

                        match &request {
                            TokenizationRequest::Redeem { id, .. } => {
                                debug!(
                                    target: "alpaca",
                                    tokenization_request_id = %id,
                                    "Alpaca keyed request response received"
                                );
                                if id != tokenization_request_id {
                                    return Err(
                                        AlpacaError::ResponseIdMismatch {
                                            requested:
                                                tokenization_request_id.clone(),
                                            returned: id.clone(),
                                        },
                                    );
                                }
                            }
                            TokenizationRequest::Mint {} => {
                                warn!(
                                    target: "alpaca",
                                    %tokenization_request_id,
                                    "Alpaca keyed request response received Mint variant (unexpected for redemption polling)"
                                );
                            }
                        }

                        Ok(request)
                    }
                    404 => {
                        let body = response.text().await?;
                        Err(AlpacaError::RequestNotFound {
                            id: tokenization_request_id.clone(),
                            body,
                        })
                    }
                    401 | 403 => {
                        let body = response.text().await?;
                        Err(AlpacaError::Auth(body))
                    }
                    status_code => {
                        let body = response.text().await?;
                        Err(AlpacaError::Api { status_code, body })
                    }
                }
            })
            .await
    }
}

fn parse_tokenization_request(
    body: String,
) -> Result<TokenizationRequest, AlpacaError> {
    let mut payload: Value = serde_json::from_str(&body)
        .map_err(|source| AlpacaError::Parse { body: body.clone(), source })?;

    if let Some(object) = payload.as_object_mut()
        && object.get("tx_hash").is_none_or(Value::is_null)
    {
        object.insert("tx_hash".to_string(), Value::String(String::new()));
    }

    serde_json::from_value(payload)
        .map_err(|source| AlpacaError::Parse { body, source })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256};
    use httpmock::prelude::*;
    use std::time::Duration;
    use tracing_test::traced_test;

    use super::{SharedAlpacaService, parse_tokenization_request};
    use crate::Quantity;
    use crate::account::ClientId;
    use crate::alpaca::{
        AlpacaService, RedeemRequestInput, TokenizationRequest, redeem_request,
    };
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    #[tokio::test]
    #[traced_test]
    async fn redeem_retries_transient_errors() {
        let server = MockServer::start();
        let endpoint = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem");
            then.status(500).body("transient");
        });
        let client = st0x_alpaca::AlpacaClient::new(
            server.base_url(),
            "test-account".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap()
        .with_max_retries(1);
        let service = SharedAlpacaService { client };
        let tx_hash = B256::repeat_byte(0x11);
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let quantity = Quantity::new(rust_decimal::Decimal::ONE);
        let network = Network::Base;
        let request = redeem_request(RedeemRequestInput {
            issuer_request_id: &IssuerRedemptionRequestId::new(tx_hash),
            underlying: &underlying,
            token: &token,
            client_id: ClientId::new(),
            quantity: &quantity,
            network: &network,
            wallet: Address::repeat_byte(0x22),
            tx_hash,
        })
        .unwrap();

        let result = service.call_redeem_endpoint(request).await;

        assert!(matches!(
            result,
            Err(crate::alpaca::AlpacaError::Api { status_code: 500, .. })
        ));
        endpoint.assert_calls(2);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Calling Alpaca redeem endpoint", "test-account"]
        ));
    }

    #[test]
    fn pending_redeem_accepts_absent_null_and_empty_tx_hash() {
        let base = r#"{"tokenization_request_id":"00000000-0000-0000-0000-000000000001","issuer_request_id":"0x1111111111111111111111111111111111111111111111111111111111111111","type":"redeem","status":"pending","underlying_symbol":"SPYM","token_symbol":"tSPYM","qty":"0.1","wallet_address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","updated_at":"2026-06-11T04:02:33.530523Z""#;

        for tx_hash_field in ["", r#","tx_hash":null"#, r#","tx_hash":"""#] {
            let request =
                parse_tokenization_request(format!("{base}{tx_hash_field}}}"))
                    .unwrap();

            assert!(matches!(
                request,
                TokenizationRequest::Redeem { tx_hash: None, .. }
            ));
        }
    }
}
