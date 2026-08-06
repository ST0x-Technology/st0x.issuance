//! Typed Rust client for the st0x.issuance HTTP API.
//!
//! Wraps the endpoints behind typed async methods using the shared
//! `st0x-issuance-dto` wire types, so consumers (e.g. the liquidity rebalance
//! guard, RAI-1038) don't hand-roll `reqwest` calls or re-declare the wire
//! shapes.
//!
//! ## Hand-written for now -- OpenAPI generation is the target
//!
//! The request paths below (e.g. `tokenized-assets/<underlying>/status`) are
//! written by hand and duplicate the server's Rocket route declarations. A
//! route change only surfaces as a mismatch at runtime -- a drift foot-gun. The
//! intended fix is to make the server emit an OpenAPI document (e.g. `utoipa`
//! over the routes + the shared DTO types) and generate this client from it
//! (e.g. `progenitor`), so the spec is the single source of truth and no path
//! is ever repeated. Deferred for now: most of the service is the Alpaca ITN
//! integration, so the client's non-Alpaca surface is small enough that the
//! codegen tooling isn't yet worth its weight. Revisit once that surface grows.

use reqwest::StatusCode;
use reqwest::redirect::Policy;
use st0x_issuance_dto::{
    MintAuthorizationRequest, MintAuthorizationResponse,
    TokenizedAssetStatusResponse, UnderlyingSymbol,
};
use std::time::Duration;
use url::Url;

const API_KEY_HEADER: &str = "X-API-KEY";

/// Async client for the issuance HTTP API, authenticating with an internal API
/// key.
pub struct IssuanceClient {
    base_url: Url,
    api_key: String,
    http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("failed to build HTTP client: {0}")]
    Build(reqwest::Error),
    #[error(
        "base URL cannot be a base (has no path segments to extend): {base}"
    )]
    NotABase { base: Url },
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to parse response body: {0}")]
    ParseResponse(reqwest::Error),
    #[error("unexpected status {status} from {url}")]
    Status { status: StatusCode, url: Url },
}

impl IssuanceClient {
    /// Creates a client targeting `base_url`, authenticating with `api_key`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Build`] if the HTTP client cannot be constructed
    /// (e.g. the system TLS backend fails to initialise).
    pub fn new(
        base_url: Url,
        api_key: impl Into<String>,
    ) -> Result<Self, ClientError> {
        // Timeouts are mandatory for a polling consumer (RAI-1038): reqwest sets
        // none by default, so without these a single unresponsive request would
        // hang the caller's task forever. Redirects are disabled because the
        // issuance API does not redirect, and reqwest forwards a custom header
        // like `X-API-KEY` to a cross-host redirect target by default — turning
        // them off keeps the credential from leaking to an unexpected host.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .redirect(Policy::none())
            .build()
            .map_err(ClientError::Build)?;

        Ok(Self { base_url, api_key: api_key.into(), http })
    }

    /// Reads the freeze status of `underlying` via
    /// `GET /tokenized-assets/<underlying>/status`, returning `Ok(None)` when
    /// the asset is unknown (404).
    ///
    /// Freeze status is a property of the underlying equity (one
    /// corporate-action freeze covers every network's listing), so the route
    /// takes no network parameter.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on URL/transport failures or an unexpected
    /// response status.
    pub async fn tokenized_asset_status(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<Option<TokenizedAssetStatusResponse>, ClientError> {
        // Build the URL by appending percent-encoded path segments to the base
        // rather than `Url::join`ing a relative string: join drops a base path
        // prefix (e.g. `/api`) when the base lacks a trailing slash, and a symbol
        // containing `/`, `?`, or `#` would corrupt the path. `path_segments_mut`
        // percent-encodes each segment and preserves the base prefix;
        // `pop_if_empty` drops the empty trailing segment a base like `host/api/`
        // leaves behind so we don't produce a `//`.
        let underlying_segment = underlying.to_string();
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|()| ClientError::NotABase {
                base: self.base_url.clone(),
            })?
            .pop_if_empty()
            .extend([
                "tokenized-assets",
                underlying_segment.as_str(),
                "status",
            ]);

        let response = self
            .http
            .get(url.clone())
            .header(API_KEY_HEADER, &self.api_key)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => Ok(Some(
                response.json().await.map_err(ClientError::ParseResponse)?,
            )),
            StatusCode::NOT_FOUND => Ok(None),
            // Carry the exact request URL (typed, no re-formatted path that can
            // drift from what was actually sent). The API key is a header, not
            // part of the URL, so this never leaks the credential.
            status => Err(ClientError::Status { status, url }),
        }
    }

    /// Delivers a signed `MintAuthV1` recipient authorization for the mint
    /// correlated by `tokenization_request_id`, via
    /// `POST /internal/mints/<tokenization_request_id>/authorization`
    /// (RAI-1243). A retried delivery MUST reuse the same
    /// `MintAuthorizationRequest` byte-identically — the nonce is fixed per
    /// mint, and the server treats an identical redelivery as an idempotent
    /// `200`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on URL/transport failures, and every non-`200`
    /// status as [`ClientError::Status`] for the caller to map. The server's
    /// contract: `404` — no mint exists yet for the tokenization request
    /// (retryable: the mint's Alpaca-side initiation may not have reached
    /// the issuance bot yet); `409` — a conflicting authorization is already
    /// recorded, or the mint has advanced past intent; `422` — the mint is
    /// vault-direct, the signer does not recover to the recipient, or the
    /// nonce is already consumed on-chain; `502` — the server's on-chain
    /// validation read failed (retryable).
    pub async fn deliver_mint_authorization(
        &self,
        tokenization_request_id: &str,
        authorization: &MintAuthorizationRequest,
    ) -> Result<MintAuthorizationResponse, ClientError> {
        // Segment-appended (never `Url::join`ed) for the same reasons as
        // `tokenized_asset_status`: preserve a base path prefix and
        // percent-encode a path-significant character in the id into one
        // segment instead of corrupting the route.
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|()| ClientError::NotABase {
                base: self.base_url.clone(),
            })?
            .pop_if_empty()
            .extend([
                "internal",
                "mints",
                tokenization_request_id,
                "authorization",
            ]);

        let response = self
            .http
            .post(url.clone())
            .header(API_KEY_HEADER, &self.api_key)
            .json(authorization)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {
                response.json().await.map_err(ClientError::ParseResponse)
            }
            // No `Ok(None)` mapping here, unlike `tokenized_asset_status`:
            // for a delivery, a 404 is a retryable race (the mint is not
            // initiated yet), not a benign "asset unknown" — collapsing it
            // into a non-error would hide the state the caller must retry
            // on. The URL never carries the API key (it is a header).
            status => Err(ClientError::Status { status, url }),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, Bytes};
    use httpmock::prelude::*;
    use serde_json::json;
    use st0x_issuance_dto::{TokenizedAssetStatus, VaultModeTag};

    use super::*;

    fn client_for(server: &MockServer) -> IssuanceClient {
        IssuanceClient::new(
            Url::parse(&server.base_url()).expect("valid mock URL"),
            "test-key",
        )
        .expect("client builds")
    }

    #[tokio::test]
    async fn tokenized_asset_status_parses_response() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/tokenized-assets/SGOV/status")
                .header(API_KEY_HEADER, "test-key");
            then.status(200).json_body(json!({
                "underlying": "SGOV",
                "status": "frozen",
                "vault_mode": "orchestrator"
            }));
        });

        let status = client_for(&server)
            .tokenized_asset_status(&UnderlyingSymbol::new("SGOV").unwrap())
            .await
            .expect("request succeeds")
            .expect("asset exists");

        mock.assert();
        assert_eq!(status.underlying, UnderlyingSymbol::new("SGOV").unwrap());
        assert_eq!(status.status, TokenizedAssetStatus::Frozen);
        // The mode tag is what st0x.liquidity switches its mint flow on —
        // this is the cross-repo path, so the client must surface it, not
        // merely tolerate it.
        assert_eq!(status.vault_mode, VaultModeTag::Orchestrator);
    }

    #[tokio::test]
    async fn tokenized_asset_status_is_none_for_unknown_asset() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/tokenized-assets/UNKNOWN/status")
                .header(API_KEY_HEADER, "test-key");
            then.status(404);
        });

        let status = client_for(&server)
            .tokenized_asset_status(&UnderlyingSymbol::new("UNKNOWN").unwrap())
            .await
            .expect("request succeeds");

        mock.assert();
        assert!(status.is_none());
    }

    #[tokio::test]
    async fn tokenized_asset_status_errors_on_unexpected_status() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/SGOV/status");
            then.status(500);
        });

        let err = client_for(&server)
            .tokenized_asset_status(&UnderlyingSymbol::new("SGOV").unwrap())
            .await
            .expect_err("a 500 must surface as an error, not None");

        mock.assert();
        assert!(
            matches!(err, ClientError::Status { status, .. } if status == StatusCode::INTERNAL_SERVER_ERROR),
            "expected Status(500), got {err:?}"
        );
    }

    #[tokio::test]
    async fn tokenized_asset_status_errors_on_malformed_body() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/SGOV/status");
            then.status(200).body("not json");
        });

        let err = client_for(&server)
            .tokenized_asset_status(&UnderlyingSymbol::new("SGOV").unwrap())
            .await
            .expect_err("a malformed 200 body must surface as a parse error");

        mock.assert();
        assert!(
            matches!(err, ClientError::ParseResponse(_)),
            "expected ParseResponse, got {err:?}"
        );
    }

    #[tokio::test]
    async fn tokenized_asset_status_preserves_base_path_prefix() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/api/tokenized-assets/SGOV/status");
            then.status(200).json_body(json!({
                "underlying": "SGOV",
                "status": "enabled"
            }));
        });

        // Base URL with a path prefix and a trailing slash: the prefix must be
        // preserved (Url::join would have dropped it).
        let base = Url::parse(&format!("{}/api/", server.base_url()))
            .expect("valid prefixed base URL");
        let client =
            IssuanceClient::new(base, "test-key").expect("client builds");

        let status = client
            .tokenized_asset_status(&UnderlyingSymbol::new("SGOV").unwrap())
            .await
            .expect("request succeeds")
            .expect("asset exists");

        mock.assert();
        assert_eq!(status.status, TokenizedAssetStatus::Enabled);
    }

    #[tokio::test]
    async fn tokenized_asset_status_errors_on_non_base_url() {
        // A cannot-be-a-base URL (e.g. `data:`) has no path segments to extend,
        // so the request must fail fast with NotABase rather than panic.
        let client = IssuanceClient::new(
            Url::parse("data:text/plain,not-a-base").expect("valid data URL"),
            "test-key",
        )
        .expect("client builds");

        let err = client
            .tokenized_asset_status(&UnderlyingSymbol::new("SGOV").unwrap())
            .await
            .expect_err("a cannot-be-a-base URL must error");

        assert!(
            matches!(err, ClientError::NotABase { .. }),
            "expected NotABase, got {err:?}"
        );
    }

    // The path is built with `path_segments_mut` (not `Url::join`/interpolation)
    // so a symbol containing a path-significant character is percent-encoded
    // into a single segment instead of corrupting the path. Pin it: a `/` must
    // arrive as `%2F` and stay one segment, not split into `.../FUND/A/...`
    // (which would 404 -> Ok(None), masking a real asset as unknown to the
    // rebalance guard).
    #[tokio::test]
    async fn tokenized_asset_status_percent_encodes_path_unsafe_symbols() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/FUND%2FA/status");
            then.status(200).json_body(json!({
                "underlying": "FUND/A",
                "status": "enabled"
            }));
        });

        let status = client_for(&server)
            .tokenized_asset_status(&UnderlyingSymbol::new("FUND/A").unwrap())
            .await
            .expect("request succeeds")
            .expect("asset exists");

        mock.assert();
        assert_eq!(status.status, TokenizedAssetStatus::Enabled);
    }

    fn test_authorization() -> MintAuthorizationRequest {
        MintAuthorizationRequest {
            nonce: B256::repeat_byte(0x07),
            signature: Bytes::from(vec![0xab, 0xcd]),
        }
    }

    #[tokio::test]
    async fn deliver_mint_authorization_posts_hex_body_and_parses_response() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/internal/mints/tok-123/authorization")
                .header(API_KEY_HEADER, "test-key")
                .json_body(json!({
                    "nonce": "0x0707070707070707070707070707070707070707070707070707070707070707",
                    "signature": "0xabcd"
                }));
            then.status(200).json_body(json!({
                "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "authorized"
            }));
        });

        let response = client_for(&server)
            .deliver_mint_authorization("tok-123", &test_authorization())
            .await
            .expect("delivery succeeds");

        mock.assert();
        assert_eq!(
            response.issuer_request_id,
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(response.status, "authorized");
    }

    /// An empty signature (`"0x"`) is a valid delivery for contract
    /// recipients — the client must send it as `"0x"`, not omit or reshape
    /// the field.
    #[tokio::test]
    async fn deliver_mint_authorization_sends_empty_signature_as_0x() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/internal/mints/tok-123/authorization")
                .json_body_includes(r#"{"signature":"0x"}"#);
            then.status(200).json_body(json!({
                "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "authorized"
            }));
        });

        client_for(&server)
            .deliver_mint_authorization(
                "tok-123",
                &MintAuthorizationRequest {
                    nonce: B256::repeat_byte(0x07),
                    signature: Bytes::new(),
                },
            )
            .await
            .expect("delivery succeeds");

        mock.assert();
    }

    // The server's delivery contract distinguishes retryable races (404: the
    // mint is not initiated yet; 502: on-chain validation read failed) from
    // terminal rejections (409 conflict, 422 invalid) purely by status. The
    // client must surface EVERY non-200 as `Status` with the exact code —
    // in particular 404 must NOT collapse into a benign `None` the way the
    // status endpoint's does, or the caller would lose the retry signal.
    #[tokio::test]
    async fn deliver_mint_authorization_surfaces_each_contract_status() {
        for code in [404u16, 409, 422, 502] {
            let server = MockServer::start_async().await;
            let mock = server.mock(|when, then| {
                when.method(POST).path("/internal/mints/tok-123/authorization");
                then.status(code);
            });

            let err = client_for(&server)
                .deliver_mint_authorization("tok-123", &test_authorization())
                .await
                .expect_err("a non-200 must surface as an error");

            mock.assert();
            assert!(
                matches!(
                    err,
                    ClientError::Status { status, .. } if status.as_u16() == code
                ),
                "expected Status({code}), got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn deliver_mint_authorization_errors_on_malformed_body() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST).path("/internal/mints/tok-123/authorization");
            then.status(200).body("not json");
        });

        let err = client_for(&server)
            .deliver_mint_authorization("tok-123", &test_authorization())
            .await
            .expect_err("a malformed 200 body must surface as a parse error");

        mock.assert();
        assert!(
            matches!(err, ClientError::ParseResponse(_)),
            "expected ParseResponse, got {err:?}"
        );
    }

    /// Same URL-building contract as the status endpoint: a base path prefix
    /// survives, and a path-significant character in the tokenization id is
    /// percent-encoded into a single segment instead of corrupting the
    /// route (which would 404 and masquerade as the initiation race).
    #[tokio::test]
    async fn deliver_mint_authorization_builds_prefixed_encoded_paths() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/api/internal/mints/tok%2F123/authorization");
            then.status(200).json_body(json!({
                "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "authorized"
            }));
        });

        let base = Url::parse(&format!("{}/api/", server.base_url()))
            .expect("valid prefixed base URL");
        let client =
            IssuanceClient::new(base, "test-key").expect("client builds");

        client
            .deliver_mint_authorization("tok/123", &test_authorization())
            .await
            .expect("delivery succeeds");

        mock.assert();
    }

    // The freeze-gating contract is "only 404 -> None; every other status ->
    // Err". Pin the auth/rate-limit codes the server can actually return so a
    // future refactor can't silently collapse them into Ok(None) and let the
    // rebalance guard treat a credential or rate-limit failure as "asset
    // unknown".
    #[tokio::test]
    async fn tokenized_asset_status_errors_on_auth_and_rate_limit_statuses() {
        for code in [401u16, 403, 429] {
            let server = MockServer::start_async().await;
            let mock = server.mock(|when, then| {
                when.method(GET).path("/tokenized-assets/SGOV/status");
                then.status(code);
            });

            let err = client_for(&server)
                .tokenized_asset_status(&UnderlyingSymbol::new("SGOV").unwrap())
                .await
                .expect_err("a non-404 error status must surface as an error");

            mock.assert();
            assert!(
                matches!(
                    err,
                    ClientError::Status { status, .. } if status.as_u16() == code
                ),
                "expected Status({code}), got {err:?}"
            );
        }
    }
}
