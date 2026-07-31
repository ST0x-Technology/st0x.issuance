//! Turnkey secure enclave wallet.
//!
//! `TurnkeyWallet` submits transactions via Turnkey's AWS Nitro secure
//! enclaves for low-latency signing (50-100ms). The only difference is
//! the signer: `TracingTurnkeySigner` (remote signing via Turnkey API) instead
//! of `PrivateKeySigner` (local key).

use alloy::consensus::{SignableTransaction, TxEnvelope};
use alloy::eips::eip2718::{Decodable2718, Eip2718Error};
use alloy::network::{EthereumWallet, TxSigner};
use alloy::primitives::{
    Address, B256, ChainId, Signature, SignatureError, hex, keccak256,
};
use alloy::signers::{Error as SignerError, Result as SignerResult};
use async_trait::async_trait;
use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde::Serialize;
use serde_json::error::Category as JsonErrorCategory;
use std::mem::Discriminant;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use tracing::{info, trace};
use turnkey_api_key_stamper::{
    Stamp, StampHeader, StamperError, TurnkeyP256ApiKey,
};
use turnkey_client::generated::{
    Activity, ActivityResponse, ActivityStatus, SignTransactionIntentV2,
    SignTransactionRequest,
    google::rpc::Status,
    immutable::activity::v1::{SignTransactionResult, result},
    immutable::common::v1::TransactionType,
};
use turnkey_client::{RetryConfig, TurnkeyClientError};

use crate::wallet::{ResolvedSigner, SignerResolveError, WalletKind};

/// Turnkey organization identifier (non-secret, lives in plaintext
/// config).
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub(crate) struct TurnkeyOrganizationId(String);

impl TurnkeyOrganizationId {
    pub const fn new(value: String) -> Self {
        Self(value)
    }
}

/// Hex-encoded P-256 API private key for Turnkey authentication
/// (secret, lives in encrypted config).
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub(crate) struct TurnkeyApiPrivateKey(String);

impl TurnkeyApiPrivateKey {
    pub const fn new(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Debug for TurnkeyApiPrivateKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Errors specific to the Turnkey signing backend.
#[derive(Debug, thiserror::Error)]
pub enum TurnkeyError {
    #[error("Turnkey signer error: {0}")]
    Signer(#[from] TracingTurnkeySignerError),
}

/// Non-secret Turnkey configuration: wallet address and organization ID.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TurnkeySettings {
    pub(crate) address: Address,
    pub(crate) organization_id: TurnkeyOrganizationId,
}

/// Secret Turnkey credential: the P-256 API private key.
#[derive(Clone, Deserialize)]
pub(crate) struct TurnkeyCredentials {
    pub(crate) api_private_key: TurnkeyApiPrivateKey,
}

impl std::fmt::Debug for TurnkeyCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnkeyCredentials")
            .field("api_private_key", &"[REDACTED]")
            .finish()
    }
}

/// Validated Turnkey configuration.
#[derive(Clone)]
pub struct TurnkeyConfig {
    pub(crate) settings: TurnkeySettings,
    pub(crate) credentials: TurnkeyCredentials,
}

impl std::fmt::Debug for TurnkeyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnkeyConfig")
            .field("organization_id", &self.settings.organization_id)
            .field("api_private_key", &"[REDACTED]")
            .field("address", &self.settings.address)
            .finish_non_exhaustive()
    }
}

impl TurnkeyConfig {
    pub(crate) const fn new(
        organization_id: TurnkeyOrganizationId,
        api_private_key: TurnkeyApiPrivateKey,
        address: Address,
    ) -> Self {
        Self {
            settings: TurnkeySettings { address, organization_id },
            credentials: TurnkeyCredentials { api_private_key },
        }
    }
}

/// Wallet that signs transactions via Turnkey's secure enclaves.
///
/// `TurnkeyWallet` provides a secure interface for signing Ethereum transactions
/// using private keys stored in Turnkey's secure enclaves. It wraps a `TurnkeySigner`
/// in an alloy `EthereumWallet` to enable seamless integration with alloy providers
/// and other Ethereum tooling.
///
/// # Security
///
/// Private keys never leave Turnkey's secure enclaves. All signing operations are
/// performed remotely via API calls authenticated with a P-256 API private key.
pub(crate) struct TurnkeyWallet {
    // A TurnkeySigner that is wrapped by an alloy `EthereumWallet`
    /// so it can be used by any alloy provider
    wallet: EthereumWallet,
    /// Wallet's address
    address: Address,
}

impl std::fmt::Debug for TurnkeyWallet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnkeyWallet")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl TurnkeyWallet {
    /// Creates a new `TurnkeyWallet` from a config containing API
    /// credentials, wallet address and chain ID.
    ///
    /// Constructs a `TurnkeySigner` using the P-256 API private key,
    /// wraps it in an `EthereumWallet`.
    pub(crate) fn new(
        config: &TurnkeyConfig,
        chain_id: u64,
    ) -> Result<Self, TurnkeyError> {
        let TurnkeySettings { address, organization_id } =
            config.settings.clone();
        let TurnkeyCredentials { api_private_key } = &config.credentials;
        let signer = TracingTurnkeySigner::from_api_key(
            api_private_key,
            organization_id,
            address,
            Some(chain_id),
        )
        .map_err(TurnkeyError::from)?;
        let wallet = EthereumWallet::from(signer);

        info!(target: "wallet", %address, "Turnkey wallet initialized");

        Ok(Self { wallet, address })
    }

    /// Creates a `TurnkeyWallet` from a pre-built `TurnkeyClient`.
    ///
    /// Used in tests to inject a client configured with a mock server
    /// base URL. Production code should use [`new`](Self::new).
    #[cfg(test)]
    fn from_client(
        client: TracingTurnkeyClient,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        chain_id: u64,
    ) -> Self {
        let signer = TracingTurnkeySigner::new(
            client,
            organization_id,
            address,
            Some(chain_id),
        );
        let wallet = EthereumWallet::from(signer);

        Self { wallet, address }
    }
}

impl From<TurnkeyWallet> for ResolvedSigner {
    fn from(value: TurnkeyWallet) -> Self {
        Self { wallet: value.wallet, kind: WalletKind::Turnkey }
    }
}

pub(crate) fn resolve_turnkey_signer(
    config: &TurnkeyConfig,
    chain_id: u64,
) -> Result<ResolvedSigner, SignerResolveError> {
    Ok(TurnkeyWallet::new(config, chain_id)?.into())
}

struct TracingTurnkeyClient {
    http: reqwest::Client,
    base_url: String,
    api_key: TurnkeyP256ApiKey,
    retry_config: RetryConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum TracingTurnkeyClientError {
    #[error(transparent)]
    Turnkey(#[from] TurnkeyClientError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Stamper(#[from] StamperError),
    #[error("Turnkey returned an unexpected activity result kind: {kind:?}")]
    UnexpectedInnerActivityResult { kind: Discriminant<result::Inner> },
    #[error(
        "Turnkey HTTP response was not successful: {status} ({body_len} response bytes redacted)"
    )]
    UnexpectedHttpStatus { status: StatusCode, body_len: usize },
    #[error(
        "Failed to decode {response_len} Turnkey response bytes ({category:?} at line {line}, column {column}; response redacted)"
    )]
    Decode {
        response_len: usize,
        category: JsonErrorCategory,
        line: usize,
        column: usize,
    },
}

impl std::fmt::Debug for TracingTurnkeyClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingTurnkeyClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl TracingTurnkeyClient {
    fn from_api_key(
        api_private_key: &TurnkeyApiPrivateKey,
    ) -> Result<Self, TracingTurnkeyClientError> {
        let TurnkeyApiPrivateKey(api_key_hex) = api_private_key;
        let api_key = TurnkeyP256ApiKey::from_strings(api_key_hex, None)?;
        Ok(Self::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("st0x-turnkey-client")
                .build()
                .map_err(TurnkeyClientError::ReqwestBuilder)?,
            "https://api.turnkey.com".to_string(),
            api_key,
            RetryConfig::default(),
        ))
    }

    #[cfg(test)]
    fn for_base_url(
        base_url: String,
        api_key: TurnkeyP256ApiKey,
    ) -> Result<Self, TracingTurnkeyClientError> {
        Ok(Self::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("st0x-turnkey-client")
                .build()
                .map_err(TurnkeyClientError::ReqwestBuilder)?,
            base_url,
            api_key,
            RetryConfig::default(),
        ))
    }

    const fn new(
        http: reqwest::Client,
        base_url: String,
        api_key: TurnkeyP256ApiKey,
        retry_config: RetryConfig,
    ) -> Self {
        Self { http, base_url, api_key, retry_config }
    }

    fn current_timestamp() -> Result<u128, TracingTurnkeySignerError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|source| TracingTurnkeySignerError::SystemTime { source })
    }

    async fn sign_transaction(
        &self,
        organization_id: TurnkeyOrganizationId,
        timestamp_ms: u128,
        params: SignTransactionIntentV2,
    ) -> Result<SignTransactionResult, TracingTurnkeyClientError> {
        let TurnkeyOrganizationId(organization_id) = organization_id;
        let request = SignTransactionRequest {
            r#type: "ACTIVITY_TYPE_SIGN_TRANSACTION_V2".to_string(),
            timestamp_ms: timestamp_ms.to_string(),
            parameters: Some(params),
            organization_id,
            generate_app_proofs: None,
        };
        let activity = self
            .process_activity(&request, "/public/v1/submit/sign_transaction")
            .await?;
        let inner = activity
            .result
            .ok_or(TurnkeyClientError::MissingResult)?
            .inner
            .ok_or(TurnkeyClientError::MissingInnerResult)?;

        match inner {
            result::Inner::SignTransactionResult(result) => Ok(result),
            other => {
                Err(TracingTurnkeyClientError::UnexpectedInnerActivityResult {
                    kind: std::mem::discriminant(&other),
                })
            }
        }
    }

    async fn process_activity<Request: Serialize + Sync>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Activity, TracingTurnkeyClientError> {
        let mut retry_count = 0;

        loop {
            let response: ActivityResponse =
                self.process_request(request, path).await?;
            let activity =
                response.activity.ok_or(TurnkeyClientError::MissingActivity)?;

            trace!(
                target: "wallet",
                activity_id = %activity.id,
                status = ?activity.status,
                "Turnkey activity response"
            );

            match activity.status {
                ActivityStatus::Completed => return Ok(activity),
                ActivityStatus::Pending => {
                    if retry_count >= self.retry_config.max_retries {
                        return Err(TurnkeyClientError::ExceededRetries(
                            retry_count,
                        )
                        .into());
                    }

                    retry_count += 1;
                    tokio::time::sleep(
                        self.retry_config.compute_delay(retry_count),
                    )
                    .await;
                }
                ActivityStatus::Failed => {
                    let failure = activity.failure.map(|failure| Status {
                        code: failure.code,
                        message: "<redacted Turnkey failure message>"
                            .to_string(),
                        details: Vec::new(),
                    });
                    return Err(
                        TurnkeyClientError::ActivityFailed(failure).into()
                    );
                }
                ActivityStatus::ConsensusNeeded => {
                    return Err(TurnkeyClientError::ActivityRequiresApproval(
                        activity.id,
                    )
                    .into());
                }
                ActivityStatus::Unspecified
                | ActivityStatus::Created
                | ActivityStatus::Rejected => {
                    return Err(TurnkeyClientError::UnexpectedActivityStatus(
                        activity.status.as_str_name().to_string(),
                    )
                    .into());
                }
            }
        }
    }

    async fn process_request<Request, Response>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Response, TracingTurnkeyClientError>
    where
        Request: Serialize + Sync,
        Response: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url, path);
        let post_body = serde_json::to_string(request)?;
        let StampHeader { name, value } =
            self.api_key.stamp(post_body.as_bytes())?;
        let response = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(name, value)
            .body(post_body)
            .send()
            .await?;
        let status = response.status();
        let content_type = response.headers().get(CONTENT_TYPE).cloned();
        // Read raw bytes and parse the success body with `serde_json::from_slice`
        // so invalid UTF-8 fails fast, matching the fail-fast convention the
        // Alpaca clients follow. Lossy decoding is used only for the trace line
        // and the error-body display.
        let bytes = response.bytes().await?;

        trace!(
            target: "wallet",
            method = "POST",
            status = %status,
            url = %url,
            "Turnkey API response received"
        );

        // Status is checked BEFORE content-type so that a 4xx/5xx response
        // carrying `text/html` (e.g. a CDN 401 or 429) surfaces as
        // `UnexpectedHttpStatus` with the body, not as `MissingContentTypeHeader`
        // or `UnexpectedMimeType`, which would hide the real error.
        if !status.is_success() {
            trace!(
                target: "wallet",
                status = %status,
                body_len = bytes.len(),
                "Turnkey API error response received"
            );
            return Err(TracingTurnkeyClientError::UnexpectedHttpStatus {
                status,
                body_len: bytes.len(),
            });
        }

        let content_type = content_type
            .ok_or(TurnkeyClientError::MissingContentTypeHeader)?
            .to_str()
            .map_err(|error| {
                TurnkeyClientError::HeaderToStrError(error.to_string())
            })?
            .parse::<mime::Mime>()
            .map_err(|error| {
                TurnkeyClientError::HeaderFromStrError(error.to_string())
            })?;

        // Compare only the MIME essence (type/subtype) so responses that carry
        // parameters such as `application/json; charset=utf-8` are accepted;
        // strict equality against `mime::APPLICATION_JSON` rejects them.
        if content_type.essence_str() != mime::APPLICATION_JSON.essence_str() {
            return Err(TurnkeyClientError::UnexpectedMimeType(
                content_type.to_string(),
            )
            .into());
        }

        serde_json::from_slice(&bytes).map_err(|error| {
            TracingTurnkeyClientError::Decode {
                response_len: bytes.len(),
                category: error.classify(),
                line: error.line(),
                column: error.column(),
            }
        })
    }
}

struct TracingTurnkeySigner {
    client: TracingTurnkeyClient,
    organization_id: TurnkeyOrganizationId,
    address: Address,
    chain_id: Option<ChainId>,
}

/// Errors that can occur when using the traced Turnkey signer.
#[derive(Debug, thiserror::Error)]
pub enum TracingTurnkeySignerError {
    #[error(transparent)]
    TurnkeyClient(#[from] TracingTurnkeyClientError),
    #[error("invalid hex string: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("invalid EIP-2718 signed transaction envelope: {0}")]
    Rlp(#[from] Eip2718Error),
    #[error("failed to recover signer address from signature: {0}")]
    SignatureRecovery(#[from] SignatureError),
    #[error(
        "Turnkey returned transaction content hash {returned}, expected {expected}"
    )]
    TransactionContentMismatch { expected: B256, returned: B256 },
    #[error(
        "Turnkey-returned signature recovers to {recovered}, expected signer {expected} -- \
         refusing to trust a signature over content we did not request"
    )]
    SignerAddressMismatch { expected: Address, recovered: Address },
    #[error("transaction is missing a chain id")]
    MissingTxChainId,
    #[error("system time is before UNIX epoch: {source}")]
    SystemTime { source: SystemTimeError },
}

impl std::fmt::Debug for TracingTurnkeySigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingTurnkeySigner")
            .field("organization_id", &self.organization_id)
            .field("address", &self.address)
            .field("chain_id", &self.chain_id)
            .finish_non_exhaustive()
    }
}

impl TracingTurnkeySigner {
    const fn new(
        client: TracingTurnkeyClient,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        chain_id: Option<ChainId>,
    ) -> Self {
        Self { client, organization_id, address, chain_id }
    }

    fn from_api_key(
        api_private_key: &TurnkeyApiPrivateKey,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        chain_id: Option<ChainId>,
    ) -> Result<Self, TracingTurnkeySignerError> {
        let client = TracingTurnkeyClient::from_api_key(api_private_key)?;
        Ok(Self::new(client, organization_id, address, chain_id))
    }

    fn parse_signature(
        response: &SignTransactionResult,
        expected_hash: B256,
        expected_signer: Address,
    ) -> Result<Signature, TracingTurnkeySignerError> {
        let signed_bytes = hex::decode(&response.signed_transaction)?;
        let envelope = TxEnvelope::decode_2718_exact(&signed_bytes)?;
        let returned_hash = envelope.signature_hash();
        if returned_hash != expected_hash {
            return Err(
                TracingTurnkeySignerError::TransactionContentMismatch {
                    expected: expected_hash,
                    returned: returned_hash,
                },
            );
        }
        let signature = envelope.signature().normalized_s();
        let recovered =
            signature.recover_address_from_prehash(&expected_hash)?;

        if recovered != expected_signer {
            return Err(TracingTurnkeySignerError::SignerAddressMismatch {
                expected: expected_signer,
                recovered,
            });
        }

        Ok(signature)
    }
}

#[async_trait]
impl TxSigner<Signature> for TracingTurnkeySigner {
    fn address(&self) -> Address {
        self.address
    }

    async fn sign_transaction(
        &self,
        tx: &mut dyn SignableTransaction<Signature>,
    ) -> SignerResult<Signature> {
        if let Some(chain_id) = self.chain_id
            && !tx.set_chain_id_checked(chain_id)
        {
            // `set_chain_id_checked` only returns false when the tx already
            // carries a (mismatching) chain id, so `tx.chain_id()` is always
            // Some here. The `MissingTxChainId` fallback is defensive (avoids
            // a panic the no-unwrap rule forbids) and is not expected to fire.
            let tx_chain_id = tx.chain_id().ok_or_else(|| {
                SignerError::other(TracingTurnkeySignerError::MissingTxChainId)
            })?;
            return Err(SignerError::TransactionChainIdMismatch {
                signer: chain_id,
                tx: tx_chain_id,
            });
        }

        let unsigned_rlp = tx.encoded_for_signing();
        let expected_hash = keccak256(&unsigned_rlp);
        let response = self
            .client
            .sign_transaction(
                self.organization_id.clone(),
                TracingTurnkeyClient::current_timestamp()
                    .map_err(SignerError::other)?,
                SignTransactionIntentV2 {
                    sign_with: self.address.to_string(),
                    unsigned_transaction: hex::encode(unsigned_rlp),
                    r#type: TransactionType::Ethereum,
                },
            )
            .await
            .map_err(|error| {
                SignerError::other(TracingTurnkeySignerError::TurnkeyClient(
                    error,
                ))
            })?;

        Self::parse_signature(&response, expected_hash, self.address)
            .map_err(SignerError::other)
    }
}

/// Builds an `EthereumWallet` whose Turnkey signer talks to `base_url`
/// instead of the real API, with a throwaway P-256 key. For tests outside
/// this module that need a Turnkey-shaped signing refusal (e.g. a
/// signing-policy denial) without exposing the private client machinery.
#[cfg(test)]
pub(crate) fn test_wallet_against(
    base_url: String,
    address: Address,
    chain_id: u64,
) -> EthereumWallet {
    let client = TracingTurnkeyClient::for_base_url(
        base_url,
        TurnkeyP256ApiKey::generate(),
    )
    .expect("mock Turnkey client must build");

    TurnkeyWallet::from_client(
        client,
        TurnkeyOrganizationId::new("org-test".to_string()),
        address,
        chain_id,
    )
    .wallet
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Signed, TxEip1559, TxLegacy};
    use alloy::eips::eip2718::Encodable2718;
    use alloy::eips::eip2930::AccessList;
    use alloy::node_bindings::AnvilInstance;
    use alloy::primitives::{Bytes, TxKind, U256, uint};
    use alloy::providers::ext::AnvilApi;
    use alloy::providers::{
        Provider, ProviderBuilder, WalletProvider, fillers::*,
    };
    use alloy::rpc::types::TransactionRequest;
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::MockServer;

    use super::*;
    use crate::test_utils::{logs_contain_at, test_anvil};

    /// Generate a fresh P-256 API key for testing.
    fn test_api_key() -> TurnkeyP256ApiKey {
        TurnkeyP256ApiKey::generate()
    }

    /// Build a Turnkey client that sends requests to the mock server.
    fn mock_client(server: &MockServer) -> TracingTurnkeyClient {
        TracingTurnkeyClient::for_base_url(server.base_url(), test_api_key())
            .unwrap()
    }

    /// Build a Turnkey client with a custom retry config so the retry
    /// loop can be exercised without real backoff delays.
    fn mock_client_with_retry(
        server: &MockServer,
        retry_config: RetryConfig,
    ) -> TracingTurnkeyClient {
        TracingTurnkeyClient::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap(),
            server.base_url(),
            test_api_key(),
            retry_config,
        )
    }

    fn eip1559_tx(chain_id: ChainId, to: Address) -> TxEip1559 {
        TxEip1559 {
            chain_id,
            nonce: 7,
            gas_limit: 21_000,
            max_fee_per_gas: 30_000_000_000,
            max_priority_fee_per_gas: 1_000_000_000,
            to: TxKind::Call(to),
            value: U256::ZERO,
            access_list: AccessList::default(),
            input: Bytes::new(),
        }
    }

    fn legacy_tx(chain_id: Option<ChainId>, to: Address) -> TxLegacy {
        TxLegacy {
            chain_id,
            nonce: 3,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value: U256::ZERO,
            input: Bytes::new(),
        }
    }

    async fn locally_signed_envelope_hex<Transaction>(
        signer: &PrivateKeySigner,
        mut transaction: Transaction,
    ) -> String
    where
        Transaction: SignableTransaction<Signature> + Clone,
        TxEnvelope: From<Signed<Transaction>>,
    {
        let signature =
            signer.sign_transaction(&mut transaction).await.unwrap();
        let envelope = TxEnvelope::from(transaction.into_signed(signature));
        hex::encode(envelope.encoded_2718())
    }

    fn completed_sign_transaction_body(
        signed_transaction_hex: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_COMPLETED",
                "type": "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
                "fingerprint": "fingerprint",
                "result": {
                    "signTransactionResult": {
                        "signedTransaction": signed_transaction_hex
                    }
                }
            }
        })
    }

    fn pending_activity_body() -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_PENDING",
                "type": "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
                "fingerprint": "fingerprint"
            }
        })
    }

    fn failed_activity_body(message: &str) -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_FAILED",
                "type": "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
                "fingerprint": "fingerprint",
                "failure": { "code": 7, "message": message }
            }
        })
    }

    #[tokio::test]
    async fn parse_signature_decodes_eip1559_transaction() {
        let signer = PrivateKeySigner::random();
        let mut transaction = eip1559_tx(8453, Address::random());
        let expected_hash = transaction.signature_hash();
        let signature =
            signer.sign_transaction(&mut transaction).await.unwrap();
        let response = SignTransactionResult {
            signed_transaction: hex::encode(
                TxEnvelope::from(transaction.into_signed(signature))
                    .encoded_2718(),
            ),
        };

        let parsed = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            signer.address(),
        )
        .unwrap();

        assert_eq!(parsed, signature);
    }

    #[tokio::test]
    async fn parse_signature_decodes_legacy_transaction() {
        let signer = PrivateKeySigner::random();
        let mut transaction = legacy_tx(Some(8453), Address::random());
        let expected_hash = transaction.signature_hash();
        let signature =
            signer.sign_transaction(&mut transaction).await.unwrap();
        let response = SignTransactionResult {
            signed_transaction: hex::encode(
                TxEnvelope::from(transaction.into_signed(signature))
                    .encoded_2718(),
            ),
        };

        let parsed = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            signer.address(),
        )
        .unwrap();

        assert_eq!(parsed, signature);
    }

    #[tokio::test]
    async fn parse_signature_normalizes_high_s_signature() {
        let signer = PrivateKeySigner::random();
        let mut transaction = eip1559_tx(8453, Address::random());
        let expected_hash = transaction.signature_hash();
        let signature =
            signer.sign_transaction(&mut transaction).await.unwrap();
        let curve_order = uint!(
            0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141_U256
        );
        let high_s_signature = Signature::new(
            signature.r(),
            curve_order - signature.s(),
            !signature.v(),
        );
        assert!(high_s_signature.normalize_s().is_some());
        let response = SignTransactionResult {
            signed_transaction: hex::encode(
                TxEnvelope::from(transaction.into_signed(high_s_signature))
                    .encoded_2718(),
            ),
        };

        let parsed = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            signer.address(),
        )
        .unwrap();

        assert_eq!(parsed, signature);
        assert!(parsed.normalize_s().is_none());
    }

    #[test]
    fn parse_signature_rejects_invalid_envelope() {
        let response =
            SignTransactionResult { signed_transaction: "not-hex".to_string() };

        let error = TracingTurnkeySigner::parse_signature(
            &response,
            B256::ZERO,
            Address::random(),
        )
        .unwrap_err();

        assert!(matches!(error, TracingTurnkeySignerError::Hex(_)));
    }

    #[tokio::test]
    async fn parse_signature_rejects_truncated_envelope() {
        let signer = PrivateKeySigner::random();
        let mut transaction = eip1559_tx(8453, Address::random());
        let expected_hash = transaction.signature_hash();
        let signature =
            signer.sign_transaction(&mut transaction).await.unwrap();
        let mut encoded =
            TxEnvelope::from(transaction.into_signed(signature)).encoded_2718();
        assert!(encoded.pop().is_some());
        let response =
            SignTransactionResult { signed_transaction: hex::encode(encoded) };

        let error = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            signer.address(),
        )
        .unwrap_err();

        assert!(matches!(error, TracingTurnkeySignerError::Rlp(_)));
    }

    #[tokio::test]
    async fn parse_signature_rejects_unrecoverable_signature() {
        let transaction = eip1559_tx(8453, Address::random());
        let expected_hash = transaction.signature_hash();
        let invalid_signature =
            Signature::new(U256::ZERO, U256::from(1), false);
        let response = SignTransactionResult {
            signed_transaction: hex::encode(
                TxEnvelope::from(transaction.into_signed(invalid_signature))
                    .encoded_2718(),
            ),
        };

        let error = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            Address::random(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::SignatureRecovery(_)
        ));
    }

    #[tokio::test]
    async fn parse_signature_rejects_wrong_signer() {
        let signer = PrivateKeySigner::random();
        let mut transaction = eip1559_tx(8453, Address::random());
        let expected_hash = transaction.signature_hash();
        let signature =
            signer.sign_transaction(&mut transaction).await.unwrap();
        let response = SignTransactionResult {
            signed_transaction: hex::encode(
                TxEnvelope::from(transaction.into_signed(signature))
                    .encoded_2718(),
            ),
        };
        let expected_signer = Address::random();

        let error = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            expected_signer,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::SignerAddressMismatch { expected, recovered }
                if expected == expected_signer && recovered == signer.address()
        ));
    }

    #[tokio::test]
    async fn parse_signature_rejects_different_transaction_content() {
        let signer = PrivateKeySigner::random();
        let mut expected_transaction = eip1559_tx(8453, Address::random());
        let returned_transaction = eip1559_tx(8453, Address::random());
        let expected_hash =
            keccak256(expected_transaction.encoded_for_signing());
        assert_ne!(
            expected_hash,
            keccak256(returned_transaction.encoded_for_signing())
        );
        let signature =
            signer.sign_transaction(&mut expected_transaction).await.unwrap();
        let response = SignTransactionResult {
            signed_transaction: hex::encode(
                TxEnvelope::from(returned_transaction.into_signed(signature))
                    .encoded_2718(),
            ),
        };

        let error = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            signer.address(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::TransactionContentMismatch {
                expected,
                returned,
            } if expected == expected_hash && returned != expected_hash
        ));
    }

    #[tokio::test]
    async fn sign_transaction_uses_turnkey_transaction_endpoint() {
        let key_signer = PrivateKeySigner::random();
        let address = key_signer.address();
        let chain_id = 8453;
        let mut transaction = eip1559_tx(chain_id, Address::random());
        let expected_unsigned = transaction.encoded_for_signing();
        let signed_transaction_hex =
            locally_signed_envelope_hex(&key_signer, transaction.clone()).await;
        let server = MockServer::start();
        let request = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_transaction")
                .json_body_includes(
                serde_json::json!({
                    "type": "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
                    "parameters": {
                        "signWith": address.to_string(),
                        "unsignedTransaction": hex::encode(expected_unsigned),
                        "type": "TRANSACTION_TYPE_ETHEREUM"
                    }
                })
                .to_string(),
            );
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(completed_sign_transaction_body(
                    &signed_transaction_hex,
                ));
        });
        let signer = TracingTurnkeySigner::new(
            mock_client(&server),
            TurnkeyOrganizationId::new("org-test".to_string()),
            address,
            Some(chain_id),
        );

        TxSigner::sign_transaction(&signer, &mut transaction).await.unwrap();

        request.assert();
    }

    #[tokio::test]
    async fn sign_transaction_exhausts_retries_while_activity_stays_pending() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_transaction");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(pending_activity_body());
        });

        // `RetryConfig::none()` caps retries at zero, so the first PENDING
        // response immediately exhausts retries with no backoff sleep.
        let client = mock_client_with_retry(&server, RetryConfig::none());

        let error = client
            .sign_transaction(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignTransactionIntentV2 {
                    sign_with: Address::random().to_string(),
                    unsigned_transaction: hex::encode(B256::ZERO),
                    r#type: TransactionType::Ethereum,
                },
            )
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(
            error,
            TracingTurnkeyClientError::Turnkey(
                TurnkeyClientError::ExceededRetries(0)
            )
        ));
    }

    #[tokio::test]
    async fn sign_transaction_redacts_failed_activity_message() {
        let server = MockServer::start();
        let sensitive_marker = "sensitive-turnkey-failure-message";
        server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_transaction");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(failed_activity_body(sensitive_marker));
        });

        let error = mock_client(&server)
            .sign_transaction(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignTransactionIntentV2 {
                    sign_with: Address::random().to_string(),
                    unsigned_transaction: hex::encode(B256::ZERO),
                    r#type: TransactionType::Ethereum,
                },
            )
            .await
            .unwrap_err();
        let error_message = error.to_string();

        assert!(error_message.contains("code: 7"));
        assert!(error_message.contains("redacted"));
        assert!(!error_message.contains(sensitive_marker));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn send_signing_failure() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_transaction");
            then.status(500)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "error": "internal server error"
                }));
        });

        let anvil = test_anvil().spawn();
        let client = mock_client(&server);

        let wallet = TurnkeyWallet::from_client(
            client,
            TurnkeyOrganizationId::new("org-test".to_string()),
            Address::random(),
            anvil.chain_id(),
        );
        let provider = ProviderBuilder::new()
            .wallet(wallet.wallet)
            .connect_http(anvil.endpoint_url());

        let tx = TransactionRequest::default()
            .to(Address::ZERO)
            .input(Bytes::new().into());
        let error = provider.send_transaction(tx).await.unwrap_err();

        // Turnkey signer errors surface as LocalUsageError(Signer(...)) because
        // signing fails before the transaction reaches the network.
        assert!(
            matches!(error, alloy::transports::RpcError::LocalUsageError(_)),
            "expected LocalUsageError wrapping signer failure, got: {error:?}"
        );
        let error_str = format!("{error:?}");
        assert!(
            error_str.contains("TurnkeyClient"),
            "expected TurnkeyClient error in chain, got: {error_str}"
        );
        assert!(logs_contain("Turnkey API error response received"));
        assert!(!logs_contain("internal server error"));
    }

    #[tokio::test]
    async fn process_request_sends_content_type_json_header() {
        let server = MockServer::start();

        // Require `Content-Type: application/json` on the request — strict
        // gateways reject or misroute bodies without it.
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/test")
                .header("content-type", "application/json");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({ "ok": true }));
        });

        let client = mock_client(&server);
        client
            .process_request::<_, serde_json::Value>(
                &serde_json::json!({}),
                "/public/v1/test",
            )
            .await
            .unwrap();

        mock.assert();
    }

    #[tokio::test]
    async fn process_request_accepts_json_content_type_with_charset_parameter()
    {
        let server = MockServer::start();

        // A `Content-Type` carrying a `charset` parameter must still be accepted:
        // the essence (`application/json`) is what matters, not exact equality.
        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "application/json; charset=utf-8")
                .json_body(serde_json::json!({ "turnkey_marker": "ok" }));
        });

        let client = mock_client(&server);
        let response: serde_json::Value = client
            .process_request(
                &serde_json::json!({"request": "body"}),
                "/public/v1/test",
            )
            .await
            .unwrap();

        assert_eq!(response["turnkey_marker"], "ok");
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn process_request_redacts_sensitive_value_on_decode_error() {
        let server = MockServer::start();
        let sensitive_status = "sensitive-unknown-activity-status";

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "activity": {
                        "status": sensitive_status
                    }
                }));
        });

        let client = mock_client(&server);
        let error = client
            .process_request::<_, ActivityResponse>(
                &serde_json::json!({"request": "body"}),
                "/public/v1/test",
            )
            .await
            .unwrap_err();
        let error_message = error.to_string();
        let error_debug = format!("{error:?}");

        assert!(error_message.contains("redacted"));
        assert!(!error_message.contains(sensitive_status));
        assert!(!error_debug.contains(sensitive_status));
        assert!(logs_contain_at!(
            tracing::Level::TRACE,
            &["Turnkey API response received", "/public/v1/test"]
        ));
        assert!(!logs_contain(sensitive_status));
    }

    #[tokio::test]
    async fn process_request_rejects_non_json_content_type() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "text/html")
                .body("<html></html>");
        });

        let client = mock_client(&server);
        let error = client
            .process_request::<_, serde_json::Value>(
                &serde_json::json!({"request": "body"}),
                "/public/v1/test",
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeyClientError::Turnkey(
                TurnkeyClientError::UnexpectedMimeType(mime)
            ) if mime == "text/html"
        ));
    }

    /// A 4xx response with a non-JSON body must surface its HTTP status without
    /// leaking the response body through the error or logs.
    #[tokio::test]
    async fn process_request_error_status_with_html_body_reports_http_status() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(401)
                .header("Content-Type", "text/html")
                .body("<html>Unauthorized</html>");
        });

        let client = mock_client(&server);
        let error = client
            .process_request::<_, serde_json::Value>(
                &serde_json::json!({"request": "body"}),
                "/public/v1/test",
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                TracingTurnkeyClientError::UnexpectedHttpStatus {
                    status: StatusCode::UNAUTHORIZED,
                    body_len: 25,
                }
            ),
            "expected typed Unauthorized status and body length, got: {error:?}"
        );
    }

    // --- Integration tests (real Turnkey API + local Anvil) ---------
    //
    // These tests sign via the real Turnkey API but submit to a local
    // Anvil instance (no testnet ETH needed -- Anvil funds the address
    // via `anvil_setBalance`).
    //
    // Required env vars (set in `.env`, loaded by direnv):
    //   TURNKEY_API_PRIVATE_KEY  -- hex-encoded P-256 API private key
    //   TURNKEY_ORG_ID           -- Turnkey organization ID
    //   TURNKEY_ADDRESS          -- Ethereum address managed by Turnkey
    //
    // See `.env.example` for the template.

    /// Returns `None` when `TURNKEY_*` env vars are absent, allowing
    /// tests to skip gracefully in environments without credentials.
    fn turnkey_env() -> Option<(String, String, Address)> {
        let api_key = std::env::var("TURNKEY_API_PRIVATE_KEY").ok()?;
        let org_id = std::env::var("TURNKEY_ORG_ID").ok()?;
        let address: Address = std::env::var("TURNKEY_ADDRESS")
            .ok()?
            .parse()
            .expect("TURNKEY_ADDRESS must be valid hex address");

        Some((api_key, org_id, address))
    }

    type TurnkeyWalletProvider = FillProvider<
        JoinFill<
            JoinFill<
                alloy::providers::Identity,
                JoinFill<
                    GasFiller,
                    JoinFill<
                        BlobGasFiller,
                        JoinFill<NonceFiller, ChainIdFiller>,
                    >,
                >,
            >,
            WalletFiller<EthereumWallet>,
        >,
        alloy::providers::RootProvider,
    >;

    /// Spin up Anvil, fund the Turnkey address, and return a
    /// `TurnkeyWallet` connected to the local node.
    async fn integration_wallet(
        api_key: String,
        org_id: String,
        address: Address,
    ) -> (TurnkeyWalletProvider, AnvilInstance) {
        let anvil = test_anvil().spawn();

        let wallet = TurnkeyWallet::new(
            &TurnkeyConfig {
                settings: TurnkeySettings {
                    address,
                    organization_id: TurnkeyOrganizationId::new(org_id),
                },
                credentials: TurnkeyCredentials {
                    api_private_key: TurnkeyApiPrivateKey::new(api_key),
                },
            },
            anvil.chain_id(),
        )
        .expect("failed to construct TurnkeyWallet from env vars");
        let provider = ProviderBuilder::new()
            .wallet(wallet.wallet)
            .connect_http(anvil.endpoint_url());
        // Fund the Turnkey-managed address on the local Anvil node.
        provider
            .anvil_set_balance(
                address,
                U256::from(10) * U256::from(10).pow(U256::from(18)),
            )
            .await
            .expect("anvil_setBalance should succeed");

        (provider, anvil)
    }

    #[ignore = "requires TURNKEY_* env vars -- run with `cargo test -- --ignored`"]
    #[tokio::test]
    async fn turnkey_integration() {
        let (api_key, org_id, address) = turnkey_env()
            .expect("TURNKEY_API_PRIVATE_KEY, TURNKEY_ORG_ID, and TURNKEY_ADDRESS must be set");

        let (wallet, _anvil) =
            integration_wallet(api_key, org_id, address).await;
        let self_address = wallet.default_signer_address();

        // Wallet address matches configured address.
        assert_eq!(wallet.default_signer_address(), address);

        // Sequential 0-value self-transfer: exercises the full Turnkey
        // signing round-trip.

        let tx = TransactionRequest::default()
            .to(self_address)
            .input(Bytes::new().into());
        let receipt = wallet
            .send_transaction(tx.clone())
            .await
            .expect("Turnkey signing and submission should succeed")
            .get_receipt()
            .await
            .unwrap();

        assert!(
            receipt.status(),
            "self-transfer should succeed, tx: {}",
            receipt.transaction_hash
        );
        assert_eq!(
            receipt.from, self_address,
            "transaction should be from the configured wallet"
        );

        // Two parallel 0-value self-transfers to verify nonce
        // management doesn't collide under concurrent signing.
        let tx = TransactionRequest::default()
            .to(self_address)
            .input(Bytes::new().into());
        let (receipt_a, receipt_b) = tokio::join!(
            wallet.send_transaction(tx.clone()).await.unwrap().get_receipt(),
            wallet.send_transaction(tx).await.unwrap().get_receipt(),
        );

        let receipt_a = receipt_a.expect("concurrent send A should succeed");
        let receipt_b = receipt_b.expect("concurrent send B should succeed");

        assert!(receipt_a.status(), "tx A should succeed");
        assert!(receipt_b.status(), "tx B should succeed");
        assert_ne!(
            receipt_a.transaction_hash, receipt_b.transaction_hash,
            "transactions must have different hashes"
        );
    }
}
