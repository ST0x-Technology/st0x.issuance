//! Turnkey secure enclave wallet.
//!
//! `TurnkeyWallet` submits transactions via Turnkey's AWS Nitro secure
//! enclaves for low-latency signing (50-100ms). The only difference is
//! the signer: `TracingTurnkeySigner` (remote signing via Turnkey API) instead
//! of `PrivateKeySigner` (local key).

use alloy::consensus::SignableTransaction;
use alloy::network::{EthereumWallet, TxSigner};
use alloy::primitives::{Address, B256, ChainId, Signature, U256, hex};
use alloy::signers::{Error as SignerError, Result as SignerResult, Signer};
use async_trait::async_trait;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde::Serialize;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use tracing::{info, trace};
use turnkey_api_key_stamper::{Stamp, StampHeader, TurnkeyP256ApiKey};
use turnkey_client::generated::{
    Activity, ActivityResponse, ActivityStatus, SignRawPayloadIntentV2,
    SignRawPayloadRequest,
    immutable::activity::v1::{SignRawPayloadResult, result},
    immutable::common::v1::{HashFunction, PayloadEncoding},
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

/// Component of an ECDSA signature returned by Turnkey.
#[derive(Debug, Clone, Copy)]
pub enum SignatureComponent {
    R,
    S,
    V,
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
    ) -> Result<Self, TurnkeyClientError> {
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
    ) -> Result<Self, TurnkeyClientError> {
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

    async fn sign_raw_payload(
        &self,
        organization_id: TurnkeyOrganizationId,
        timestamp_ms: u128,
        params: SignRawPayloadIntentV2,
    ) -> Result<SignRawPayloadResult, TurnkeyClientError> {
        let TurnkeyOrganizationId(organization_id) = organization_id;
        let request = SignRawPayloadRequest {
            r#type: "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2".to_string(),
            timestamp_ms: timestamp_ms.to_string(),
            parameters: Some(params),
            organization_id,
            generate_app_proofs: None,
        };
        let activity = self
            .process_activity(&request, "/public/v1/submit/sign_raw_payload")
            .await?;
        let inner = activity
            .result
            .ok_or(TurnkeyClientError::MissingResult)?
            .inner
            .ok_or(TurnkeyClientError::MissingInnerResult)?;

        match inner {
            result::Inner::SignRawPayloadResult(result) => Ok(result),
            other => Err(TurnkeyClientError::UnexpectedInnerActivityResult(
                serde_json::to_string(&other)?,
            )),
        }
    }

    async fn process_activity<Request: Serialize + Sync>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Activity, TurnkeyClientError> {
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
                        ));
                    }

                    retry_count += 1;
                    tokio::time::sleep(
                        self.retry_config.compute_delay(retry_count),
                    )
                    .await;
                }
                ActivityStatus::Failed => {
                    return Err(TurnkeyClientError::ActivityFailed(
                        activity.failure,
                    ));
                }
                ActivityStatus::ConsensusNeeded => {
                    return Err(TurnkeyClientError::ActivityRequiresApproval(
                        activity.id,
                    ));
                }
                ActivityStatus::Unspecified
                | ActivityStatus::Created
                | ActivityStatus::Rejected => {
                    return Err(TurnkeyClientError::UnexpectedActivityStatus(
                        activity.status.as_str_name().to_string(),
                    ));
                }
            }
        }
    }

    async fn process_request<Request, Response>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Response, TurnkeyClientError>
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
                body = %String::from_utf8_lossy(&bytes),
                "Turnkey API error response body"
            );
            return Err(TurnkeyClientError::UnexpectedHttpStatus(
                status.as_u16(),
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
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
            ));
        }

        serde_json::from_slice(&bytes).map_err(|error| {
            TurnkeyClientError::Decode(
                String::from_utf8_lossy(&bytes).into_owned(),
                error,
            )
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
    TurnkeyClient(#[from] TurnkeyClientError),
    #[error("invalid hex string: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("signature component {component:?} has invalid byte length: {len}")]
    BadComponentLength { component: SignatureComponent, len: usize },
    #[error("signature v value {value} is not a supported Turnkey recovery id")]
    UnnormalizableV { value: String },
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
        response: &SignRawPayloadResult,
    ) -> Result<Signature, TracingTurnkeySignerError> {
        let r = Self::parse_scalar(&response.r, SignatureComponent::R)?;
        let s = Self::parse_scalar(&response.s, SignatureComponent::S)?;
        let parity = Self::parse_recovery_id(&response.v)?;

        Ok(Signature::new(r, s, parity))
    }

    fn parse_scalar(
        value: &str,
        component: SignatureComponent,
    ) -> Result<U256, TracingTurnkeySignerError> {
        let hex_digits = Self::strip_hex_prefix(value);
        let byte_len = hex_digits.len().div_ceil(2);
        if byte_len == 0 || byte_len > 32 {
            return Err(TracingTurnkeySignerError::BadComponentLength {
                component,
                len: byte_len,
            });
        }

        let bytes = Self::decode_even_width_hex(hex_digits)?;
        Ok(B256::left_padding_from(&bytes).into())
    }

    fn parse_recovery_id(
        value: &str,
    ) -> Result<bool, TracingTurnkeySignerError> {
        let digits = Self::strip_hex_prefix(value);
        let byte_len = digits.len().div_ceil(2);
        if byte_len == 0 || byte_len > 1 {
            return Err(TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::V,
                len: byte_len,
            });
        }

        match digits {
            "0" | "00" | "1b" | "1B" => Ok(false),
            "1" | "01" | "1c" | "1C" => Ok(true),
            _ => Err(TracingTurnkeySignerError::UnnormalizableV {
                value: value.to_string(),
            }),
        }
    }

    fn strip_hex_prefix(value: &str) -> &str {
        value
            .strip_prefix("0x")
            .or_else(|| value.strip_prefix("0X"))
            .unwrap_or(value)
    }

    fn decode_even_width_hex(
        hex_digits: &str,
    ) -> Result<Vec<u8>, hex::FromHexError> {
        if hex_digits.len().is_multiple_of(2) {
            hex::decode(hex_digits)
        } else {
            hex::decode(format!("0{hex_digits}"))
        }
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
        if let Some(chain_id) = self.chain_id()
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

        self.sign_hash(&tx.signature_hash()).await
    }
}

#[async_trait]
impl Signer for TracingTurnkeySigner {
    async fn sign_hash(&self, hash: &B256) -> SignerResult<Signature> {
        let response = self
            .client
            .sign_raw_payload(
                self.organization_id.clone(),
                TracingTurnkeyClient::current_timestamp()
                    .map_err(SignerError::other)?,
                SignRawPayloadIntentV2 {
                    sign_with: self.address.to_string(),
                    payload: hex::encode(hash),
                    encoding: PayloadEncoding::Hexadecimal,
                    hash_function: HashFunction::NoOp,
                },
            )
            .await
            .map_err(|error| {
                SignerError::other(TracingTurnkeySignerError::TurnkeyClient(
                    error,
                ))
            })?;

        Self::parse_signature(&response).map_err(SignerError::other)
    }

    fn address(&self) -> Address {
        self.address
    }

    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id
    }

    fn set_chain_id(&mut self, chain_id: Option<ChainId>) {
        self.chain_id = chain_id;
    }
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{Bytes, U256};
    use alloy::providers::ext::AnvilApi;
    use alloy::providers::{
        Provider, ProviderBuilder, WalletProvider, fillers::*,
    };
    use alloy::rpc::types::TransactionRequest;
    use httpmock::MockServer;
    use std::sync::{Arc, RwLock};

    use super::*;

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

    /// 32-byte big-endian hex for the U256 value 1.
    const VALID_R_HEX: &str =
        "0000000000000000000000000000000000000000000000000000000000000001";
    /// 32-byte big-endian hex for the U256 value 2.
    const VALID_S_HEX: &str =
        "0000000000000000000000000000000000000000000000000000000000000002";

    fn signature_result(r: &str, s: &str, v: &str) -> SignRawPayloadResult {
        SignRawPayloadResult {
            r: r.to_string(),
            s: s.to_string(),
            v: v.to_string(),
        }
    }

    /// JSON body for a COMPLETED `sign_raw_payload` activity carrying the
    /// given signature components, shaped like a real Turnkey response.
    fn completed_sign_raw_payload_body(
        r: &str,
        s: &str,
        v: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_COMPLETED",
                "type": "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2",
                "fingerprint": "fingerprint",
                "result": {
                    "signRawPayloadResult": { "r": r, "s": s, "v": v }
                }
            }
        })
    }

    /// JSON body for a PENDING activity (no result yet).
    fn pending_activity_body() -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_PENDING",
                "type": "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2",
                "fingerprint": "fingerprint"
            }
        })
    }

    fn failed_activity_body() -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_FAILED",
                "type": "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2",
                "fingerprint": "fingerprint",
                "failure": { "code": 1, "message": "signing failed" }
            }
        })
    }

    fn consensus_needed_activity_body() -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "consensus-activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_CONSENSUS_NEEDED",
                "type": "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2",
                "fingerprint": "fingerprint"
            }
        })
    }

    #[test]
    fn parse_signature_decodes_valid_components() {
        let result = signature_result(VALID_R_HEX, VALID_S_HEX, "00");

        let signature = TracingTurnkeySigner::parse_signature(&result).unwrap();

        assert_eq!(signature.r(), U256::from(1));
        assert_eq!(signature.s(), U256::from(2));
        assert!(!signature.v());
    }

    #[test]
    fn parse_signature_decodes_sanitized_real_turnkey_result() {
        let result: SignRawPayloadResult = serde_json::from_str(include_str!(
            "fixtures/turnkey_sign_raw_payload_result.json"
        ))
        .unwrap();

        let signature = TracingTurnkeySigner::parse_signature(&result).unwrap();

        assert_eq!(
            signature.r(),
            U256::from_be_slice(
                &hex::decode(
                    "d80ea712806e483e220b81017e35be1aa1de6cd4e8bd3e293713cf3ccb2b8ca6"
                )
                .unwrap()
            )
        );
        assert_eq!(
            signature.s(),
            U256::from_be_slice(
                &hex::decode(
                    "3faae45047a55fb9092d36b000f3f77e423118086915eba94dee254933e64219"
                )
                .unwrap()
            )
        );
        assert!(signature.v());
    }

    #[test]
    fn parse_signature_normalizes_short_odd_width_components() {
        let result = signature_result("f", "0xabc", "1");

        let signature = TracingTurnkeySigner::parse_signature(&result).unwrap();

        assert_eq!(signature.r(), U256::from(15));
        assert_eq!(signature.s(), U256::from(0xabc));
        assert!(signature.v());
    }

    #[test]
    fn parse_signature_accepts_supported_recovery_id_encodings() {
        let cases = [
            ("0", false),
            ("00", false),
            ("0x0", false),
            ("1", true),
            ("01", true),
            ("0x01", true),
            ("1b", false),
            ("0x1B", false),
            ("1c", true),
            ("0X1C", true),
        ];

        for (recovery_id, expected_parity) in cases {
            let result =
                signature_result(VALID_R_HEX, VALID_S_HEX, recovery_id);

            let signature =
                TracingTurnkeySigner::parse_signature(&result).unwrap();

            assert_eq!(
                signature.v(),
                expected_parity,
                "unexpected parity for recovery id {recovery_id}"
            );
        }
    }

    #[test]
    fn parse_signature_rejects_oversized_r_component() {
        let result = signature_result(
            "001111111100111111110011111111001111111100111111110011111111111111",
            VALID_S_HEX,
            "00",
        );

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::R,
                len: 33
            }
        ));
    }

    #[test]
    fn parse_signature_rejects_oversized_v_component() {
        let result = signature_result(VALID_R_HEX, VALID_S_HEX, "0000");

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::V,
                len: 2
            }
        ));
    }

    #[test]
    fn parse_signature_rejects_unnormalizable_v() {
        // A raw secp256k1 signature only carries recovery id 0/1 (or the
        // equivalent Ethereum 27/28 form), never an EIP-155 transaction v.
        let result = signature_result(VALID_R_HEX, VALID_S_HEX, "02");

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::UnnormalizableV { value }
                if value == "02"
        ));
    }

    #[test]
    fn parse_signature_rejects_eip_155_transaction_v() {
        let result = signature_result(VALID_R_HEX, VALID_S_HEX, "25");

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::UnnormalizableV { value }
                if value == "25"
        ));
    }

    #[test]
    fn parse_signature_rejects_ambiguous_unprefixed_recovery_ids() {
        for recovery_id in ["27", "28"] {
            let result =
                signature_result(VALID_R_HEX, VALID_S_HEX, recovery_id);

            let error =
                TracingTurnkeySigner::parse_signature(&result).unwrap_err();

            assert!(
                matches!(
                    error,
                    TracingTurnkeySignerError::UnnormalizableV { ref value }
                        if value == recovery_id
                ),
                "ambiguous recovery id {recovery_id} must be rejected, got {error:?}"
            );
        }
    }

    #[test]
    fn parse_signature_rejects_empty_scalar() {
        let result = signature_result("0x", VALID_S_HEX, "00");

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::R,
                len: 0
            }
        ));
    }

    #[tokio::test]
    async fn sign_hash_returns_signature_from_completed_activity() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_raw_payload")
                .json_body_includes(r#"{"generateAppProofs":null}"#);
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(completed_sign_raw_payload_body(
                    VALID_R_HEX,
                    VALID_S_HEX,
                    "00",
                ));
        });

        let signer = TracingTurnkeySigner::new(
            mock_client(&server),
            TurnkeyOrganizationId::new("org-test".to_string()),
            Address::random(),
            Some(1),
        );

        let signature = signer.sign_hash(&B256::ZERO).await.unwrap();

        assert_eq!(signature.r(), U256::from(1));
        assert_eq!(signature.s(), U256::from(2));
        assert!(!signature.v());
    }

    #[tokio::test]
    async fn sign_raw_payload_exhausts_retries_while_activity_stays_pending() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_raw_payload");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(pending_activity_body());
        });

        // `RetryConfig::none()` caps retries at zero, so the first PENDING
        // response immediately exhausts retries with no backoff sleep.
        let client = mock_client_with_retry(&server, RetryConfig::none());

        let error = client
            .sign_raw_payload(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignRawPayloadIntentV2 {
                    sign_with: Address::random().to_string(),
                    payload: hex::encode(B256::ZERO),
                    encoding: PayloadEncoding::Hexadecimal,
                    hash_function: HashFunction::NoOp,
                },
            )
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(error, TurnkeyClientError::ExceededRetries(0)));
    }

    #[tokio::test]
    async fn sign_raw_payload_retries_pending_then_succeeds_on_completed() {
        let server = MockServer::start();

        let call_count = Arc::new(RwLock::new(0usize));
        let call_count_clone = Arc::clone(&call_count);

        let pending_mock = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_raw_payload")
                .is_true(move |_| {
                    let mut x = call_count_clone.write().unwrap();
                    let res = x.le(&1); // 1 retry means it needs to hit this twice
                    *x += 1;
                    res
                });
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(pending_activity_body());
        });
        server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_raw_payload");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(completed_sign_raw_payload_body(
                    VALID_R_HEX,
                    VALID_S_HEX,
                    "00",
                ));
        });

        // Allow 1 retry so the loop can reach the COMPLETED response.
        let client = mock_client_with_retry(
            &server,
            RetryConfig {
                max_retries: 1,
                initial_delay: Duration::ZERO,
                multiplier: 1.0,
                max_delay: Duration::ZERO,
            },
        );

        let result = client
            .sign_raw_payload(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignRawPayloadIntentV2 {
                    sign_with: Address::random().to_string(),
                    payload: hex::encode(B256::ZERO),
                    encoding: PayloadEncoding::Hexadecimal,
                    hash_function: HashFunction::NoOp,
                },
            )
            .await
            .unwrap();

        // PENDING mock served exactly once; COMPLETED provided the final result.
        assert_eq!(pending_mock.calls(), 1);
        assert_eq!(result.r, VALID_R_HEX);
        assert_eq!(result.s, VALID_S_HEX);
    }

    #[tokio::test]
    async fn sign_raw_payload_maps_failed_activity_to_error() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_raw_payload");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(failed_activity_body());
        });

        let error = mock_client(&server)
            .sign_raw_payload(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignRawPayloadIntentV2 {
                    sign_with: Address::random().to_string(),
                    payload: hex::encode(B256::ZERO),
                    encoding: PayloadEncoding::Hexadecimal,
                    hash_function: HashFunction::NoOp,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TurnkeyClientError::ActivityFailed(_)));
    }

    #[tokio::test]
    async fn sign_raw_payload_maps_consensus_needed_to_error() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_raw_payload");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(consensus_needed_activity_body());
        });

        let error = mock_client(&server)
            .sign_raw_payload(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignRawPayloadIntentV2 {
                    sign_with: Address::random().to_string(),
                    payload: hex::encode(B256::ZERO),
                    encoding: PayloadEncoding::Hexadecimal,
                    hash_function: HashFunction::NoOp,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TurnkeyClientError::ActivityRequiresApproval(ref id) if id == "consensus-activity-id"
        ));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn send_signing_failure() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_raw_payload");
            then.status(500)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "error": "internal server error"
                }));
        });

        let anvil = Anvil::new().spawn();
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
        assert!(logs_contain("Turnkey API error response body"));
        assert!(logs_contain("internal server error"));
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
            TurnkeyClientError::UnexpectedMimeType(mime) if mime == "text/html"
        ));
    }

    /// A 4xx response with a non-JSON body (e.g. a CDN 401 or a rate-limit 429
    /// rendered as HTML) must surface as `UnexpectedHttpStatus` with the
    /// response body, not as `MissingContentTypeHeader` or `UnexpectedMimeType`.
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
                TurnkeyClientError::UnexpectedHttpStatus(401, ref body)
                    if body.contains("Unauthorized")
            ),
            "expected UnexpectedHttpStatus(401, ...), got: {error:?}"
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
        let anvil = Anvil::new().spawn();

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
