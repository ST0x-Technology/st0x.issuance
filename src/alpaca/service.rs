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
    _client: reqwest::Client,
    _base_url: String,
    _account_id: String,
    _api_key: String,
    _api_secret: String,
}

impl RealAlpacaService {
    pub(crate) fn new(
        base_url: String,
        account_id: String,
        api_key: String,
        api_secret: String,
    ) -> Self {
        let client = reqwest::Client::new();
        Self {
            _client: client,
            _base_url: base_url,
            _account_id: account_id,
            _api_key: api_key,
            _api_secret: api_secret,
        }
    }
}

#[async_trait]
impl AlpacaService for RealAlpacaService {
    async fn send_mint_callback(
        &self,
        _request: MintCallbackRequest,
    ) -> Result<(), AlpacaError> {
        todo!("Implement in Task 9")
    }
}
