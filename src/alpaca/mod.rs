use alloy::primitives::{Address, B256, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::account::ClientId;
use crate::mint::{Quantity, TokenizationRequestId};
use crate::redemption::IssuerRedemptionRequestId;
use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

pub(crate) mod itn;
#[cfg(test)]
pub(crate) mod mock;
pub(crate) mod service;

pub use service::AlpacaConfig;

/// Service abstraction for Alpaca API operations.
///
/// This trait provides an interface for calling Alpaca's tokenization endpoints,
/// particularly the mint callback endpoint. Implementations can be real HTTP-based
/// services or mocks for testing.
#[async_trait]
pub(crate) trait AlpacaService: Send + Sync {
    /// Sends a mint callback to Alpaca to confirm mint completion.
    ///
    /// # Arguments
    ///
    /// * `request` - Callback request containing mint details
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success (200 OK response from Alpaca).
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if the HTTP request fails, authentication fails,
    /// or Alpaca returns an error response.
    async fn send_mint_callback(
        &self,
        request: MintCallbackRequest,
    ) -> Result<(), AlpacaError>;

    /// Calls Alpaca's redeem endpoint to initiate a redemption.
    ///
    /// # Arguments
    ///
    /// * `request` - Redeem request containing redemption details
    async fn call_redeem_endpoint(
        &self,
        request: RedeemRequest,
    ) -> Result<RedeemResponse, AlpacaError>;

    /// Polls Alpaca's keyed request endpoint for a specific tokenization request.
    ///
    /// Calls `GET /v1/accounts/{account_id}/tokenization/requests/{tokenization_request_id}`
    /// and deserializes the single-object response as [`TokenizationRequest`].
    /// Maps HTTP 404 to [`AlpacaError::RequestNotFound`].
    async fn poll_request_status(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaError>;

    /// Lists dividend corporate-action announcements whose ex-date falls in
    /// `[since, until]` (both inclusive).
    ///
    /// Calls `GET /v1/corporate_actions/announcements` filtered to
    /// `ca_types=dividend` / `date_type=ex_date`. Alpaca caps the date range
    /// at 90 days; callers must keep `until - since` within that span.
    async fn list_dividend_announcements(
        &self,
        since: NaiveDate,
        until: NaiveDate,
    ) -> Result<Vec<DividendAnnouncement>, AlpacaError>;
}

/// A dividend corporate-action announcement from Alpaca's announcements API.
///
/// Only the fields the freeze scheduler consumes are modeled; the wire object
/// carries more (record/payable dates, cash per share, rates). For a cash
/// dividend the initiating and target symbols are the same company, so
/// `initiating_symbol` — the field Alpaca's `symbol` query filter matches — is
/// the asset the dividend applies to.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct DividendAnnouncement {
    pub(crate) initiating_symbol: UnderlyingSymbol,
    /// Nullable upstream: an announcement can be ingested before its ex-date
    /// is set.
    pub(crate) ex_date: Option<NaiveDate>,
}

/// Request payload for Alpaca's mint callback endpoint.
///
/// This struct is serialized to JSON and sent to:
/// `POST /v1/accounts/{account_id}/tokenization/callback/mint`
///
/// The JSON format uses snake_case field names per Alpaca's API convention.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct MintCallbackRequest {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) client_id: ClientId,
    pub(crate) wallet_address: Address,
    pub(crate) tx_hash: B256,
    pub(crate) network: Network,
}

/// Request payload for Alpaca's redeem callback endpoint.
///
/// Serialized to JSON and sent to:
/// `POST /v1/accounts/{account_id}/tokenization/callback/redeem`
///
/// The `network` field must be a `TokenizationNetwork` wire string per Alpaca
/// ITN OpenAPI: <https://docs.alpaca.markets/reference/posttokenizationredeem>
#[derive(Debug, Clone, Serialize)]
pub(crate) struct RedeemRequest {
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    pub(crate) client_id: ClientId,
    #[serde(rename = "qty")]
    pub(crate) quantity: Quantity,
    pub(crate) network: Network,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RedeemResponse {
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) created_at: DateTime<Utc>,
    #[serde(rename = "type")]
    pub(crate) r#type: TokenizationRequestType,
    pub(crate) status: RedeemRequestStatus,
    #[serde(rename = "underlying_symbol")]
    pub(crate) underlying: UnderlyingSymbol,
    #[serde(rename = "token_symbol")]
    pub(crate) token: TokenSymbol,
    #[serde(rename = "qty")]
    pub(crate) quantity: Quantity,
    pub(crate) issuer: String,
    pub(crate) network: Network,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
    pub(crate) fees: Option<Fees>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TokenizationRequestType {
    Mint,
    Redeem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RedeemRequestStatus {
    Pending,
    Completed,
    Rejected,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Fees(pub(crate) Decimal);

/// A tokenization request returned by Alpaca's keyed request endpoint.
///
/// The endpoint returns a single object. This enum deserializes both Mint and
/// Redeem variants via `#[serde(tag = "type")]`, with each variant carrying
/// the appropriate `issuer_request_id` type.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum TokenizationRequest {
    Mint {},
    Redeem {
        #[serde(rename = "tokenization_request_id")]
        id: TokenizationRequestId,
        #[serde(rename = "issuer_request_id")]
        issuer_request_id: IssuerRedemptionRequestId,
        status: RedeemRequestStatus,
        #[serde(rename = "underlying_symbol")]
        underlying: UnderlyingSymbol,
        #[serde(rename = "token_symbol")]
        token: TokenSymbol,
        #[serde(rename = "qty")]
        quantity: Quantity,
        network: Network,
        #[serde(rename = "wallet_address")]
        wallet: Address,
        #[serde(
            rename = "tx_hash",
            default,
            deserialize_with = "deserialize_optional_b256"
        )]
        tx_hash: Option<TxHash>,
        updated_at: Option<DateTime<Utc>>,
    },
}

fn deserialize_optional_b256<'de, D>(
    deserializer: D,
) -> Result<Option<TxHash>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Alpaca returns an empty string for a still-Pending redeem's tx_hash
    // (observed in production polling); JSON `null` and an omitted field
    // (handled by `#[serde(default)]` on the field) are tolerated defensively,
    // not observed behavior. Treat all three as `None`; only a non-empty
    // string is parsed as a hash. A type error here would surface as a
    // non-retryable `Parse` error and stall an otherwise-healthy redemption.
    let opt: Option<String> = serde::Deserialize::deserialize(deserializer)?;
    match opt.as_deref() {
        None | Some("") => Ok(None),
        Some(value) => {
            value.parse().map(Some).map_err(serde::de::Error::custom)
        }
    }
}

/// Errors that can occur during Alpaca API operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaError {
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    /// Failed to parse response after 200 OK - NOT retryable
    #[error("Failed to parse response: {source}")]
    Parse {
        body: String,
        #[source]
        source: serde_json::Error,
    },
    /// Authentication failed (401/403 response)
    #[error("Authentication failed: {0}")]
    Auth(String),
    /// Alpaca API returned an error response
    #[error("API error {status_code}: {body}")]
    Api { status_code: u16, body: String },
    /// HTTP 404 from the keyed endpoint. Treated as "definitively absent" for
    /// recovery purposes (empirically verified 2026-06-12, not a published
    /// Alpaca guarantee). `body` contains the raw 404 response for operator
    /// diagnostics.
    #[error("Tokenization request not found: {id}, body: {body}")]
    RequestNotFound { id: TokenizationRequestId, body: String },
    /// The keyed endpoint returned a request whose id differs from what was
    /// requested — non-retryable data integrity violation.
    #[error("Response id mismatch: requested {requested}, returned {returned}")]
    ResponseIdMismatch {
        requested: TokenizationRequestId,
        returned: TokenizationRequestId,
    },
    /// `network` is not listed in Alpaca ITN `TokenizationNetwork` OpenAPI.
    #[error(
        "Network {network} is not a published Alpaca TokenizationNetwork value — see {reference}"
    )]
    UnsupportedTokenizationNetwork { network: Network, reference: &'static str },
}

impl AlpacaError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Reqwest(e) => !e.is_decode(),
            Self::Api { status_code, .. } => {
                matches!(status_code, 500..=599 | 429)
            }
            Self::Parse { .. }
            | Self::Auth(_)
            | Self::RequestNotFound { .. }
            | Self::ResponseIdMismatch { .. }
            | Self::UnsupportedTokenizationNetwork { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use rust_decimal::Decimal;
    use serde_json::json;

    use crate::account::ClientId;
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    use super::{
        MintCallbackRequest, RedeemRequest, TokenizationRequest,
        itn::{REDEEM_CALLBACK_OPENAPI_REFERENCE, accepts_network_wire_string},
    };

    #[test]
    fn test_mint_callback_request_serialization() {
        let client_id = "55051234-0000-4abc-9000-4aabcdef0045".parse().unwrap();

        let request = MintCallbackRequest {
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
        };

        let serialized = serde_json::to_value(&request).unwrap();

        assert_eq!(
            serialized["tokenization_request_id"],
            json!("12345-678-90AB")
        );
        assert_eq!(
            serialized["client_id"],
            json!("55051234-0000-4abc-9000-4aabcdef0045")
        );
        assert_eq!(
            serialized["wallet_address"],
            json!("0x1234567890abcdef1234567890abcdef12345678")
        );
        assert_eq!(
            serialized["tx_hash"],
            json!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            )
        );
        assert_eq!(serialized["network"], json!("base"));
    }

    #[test]
    fn test_redeem_request_serialization() {
        let client_id = "55051234-0000-4abc-9000-4aabcdef0045".parse().unwrap();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        let request = RedeemRequest {
            issuer_request_id: IssuerRedemptionRequestId::new(tx_hash),
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            client_id,
            quantity: Quantity::new(Decimal::new(10050, 2)),
            network: Network::Base,
            wallet: address!("0x9999999999999999999999999999999999999999"),
            tx_hash,
        };

        let serialized = serde_json::to_value(&request).unwrap();

        assert_eq!(
            serialized["issuer_request_id"],
            json!(
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )
        );
        assert_eq!(serialized["underlying_symbol"], json!("AAPL"));
        assert_eq!(serialized["token_symbol"], json!("tAAPL"));
        assert_eq!(
            serialized["client_id"],
            json!("55051234-0000-4abc-9000-4aabcdef0045")
        );
        assert_eq!(serialized["qty"], json!("100.50"));
        assert_eq!(serialized["network"], json!("base"));
        assert_eq!(
            serialized["wallet_address"],
            json!("0x9999999999999999999999999999999999999999")
        );
        assert_eq!(
            serialized["tx_hash"],
            json!(
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )
        );
    }

    #[test]
    fn test_redeem_request_serialization_ethereum_network() {
        let client_id = "55051234-0000-4abc-9000-4aabcdef0045".parse().unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let request = RedeemRequest {
            issuer_request_id: IssuerRedemptionRequestId::new(tx_hash),
            underlying: UnderlyingSymbol::new("TSLA").unwrap(),
            token: TokenSymbol::new("tTSLA"),
            client_id,
            quantity: Quantity::new(Decimal::from(10)),
            network: Network::Ethereum,
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            tx_hash,
        };

        let serialized = serde_json::to_value(&request).unwrap();
        let wire = serialized["network"].as_str().unwrap();

        assert!(
            accepts_network_wire_string(wire),
            "redeem callback network must be a published Alpaca TokenizationNetwork \
             value — see {REDEEM_CALLBACK_OPENAPI_REFERENCE}"
        );
        assert_eq!(wire, "ethereum");
    }

    #[test]
    fn test_address_serialization_includes_0x_prefix() {
        let request = MintCallbackRequest {
            tokenization_request_id: TokenizationRequestId::new("test"),
            client_id: ClientId::new(),
            wallet_address: address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            network: Network::Base,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(
            json.contains("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"")
        );
        assert!(json.contains("\"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\""));
    }

    #[test]
    fn test_tokenization_request_deserializes_mint_variant() {
        let json = json!({
            "type": "mint",
            "tokenization_request_id": "tok-123",
            "issuer_request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.00",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        });

        let request: TokenizationRequest =
            serde_json::from_value(json).unwrap();
        assert!(matches!(request, TokenizationRequest::Mint { .. }));
    }

    #[test]
    fn test_tokenization_request_deserializes_redeem_variant() {
        let json = json!({
            "type": "redeem",
            "tokenization_request_id": "tok-456",
            "issuer_request_id": "red-574378e0",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "50.00",
            "network": "base",
            "wallet_address": "0x9999999999999999999999999999999999999999",
            "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "updated_at": "2025-09-12T17:30:00.000000-04:00"
        });

        let request: TokenizationRequest =
            serde_json::from_value(json).unwrap();
        assert!(matches!(request, TokenizationRequest::Redeem { .. }));
    }

    #[test]
    fn test_tokenization_request_list_deserializes_mixed_types() {
        let json = json!([
            {
                "type": "mint",
                "tokenization_request_id": "tok-mint-1",
                "issuer_request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "100.00",
                "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            },
            {
                "type": "redeem",
                "tokenization_request_id": "tok-red-2",
                "issuer_request_id": "red-574378e0",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "50.00",
                "network": "base",
                "wallet_address": "0x9999999999999999999999999999999999999999",
                "tx_hash": "",
                "updated_at": "2025-09-12T17:30:00.000000-04:00"
            }
        ]);

        let requests: Vec<TokenizationRequest> =
            serde_json::from_value(json).unwrap();
        assert_eq!(requests.len(), 2);
        assert!(matches!(requests[0], TokenizationRequest::Mint { .. }));
        assert!(matches!(requests[1], TokenizationRequest::Redeem { .. }));
    }

    #[test]
    fn test_redeem_deserializes_absent_null_and_empty_tx_hash_as_none() {
        // A still-Pending redeem's tx_hash arrives as an empty string
        // (observed); JSON null and an omitted field are tolerated
        // defensively. All three must deserialize to None rather than a
        // non-retryable Parse error that would stall the redemption.
        let base = r#"{"tokenization_request_id":"00000000-0000-0000-0000-000000000001","issuer_request_id":"0x1111111111111111111111111111111111111111111111111111111111111111","type":"redeem","status":"pending","underlying_symbol":"SPYM","token_symbol":"tSPYM","qty":"0.1","network":"base","wallet_address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","updated_at":"2026-06-11T04:02:33.530523Z""#;

        for tx_hash_field in ["", r#","tx_hash":null"#, r#","tx_hash":"""#] {
            let json = format!("{base}{tx_hash_field}}}");
            let parsed: TokenizationRequest = serde_json::from_str(&json)
                .unwrap_or_else(|err| {
                    panic!(
                        "failed to parse with tx_hash `{tx_hash_field}`: {err}"
                    )
                });

            match parsed {
                TokenizationRequest::Redeem { tx_hash, .. } => assert_eq!(
                    tx_hash, None,
                    "tx_hash `{tx_hash_field}` should deserialize to None"
                ),
                other @ TokenizationRequest::Mint { .. } => {
                    panic!("expected Redeem, got {other:?}")
                }
            }
        }
    }
}
