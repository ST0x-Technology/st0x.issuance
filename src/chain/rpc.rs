//! RPC endpoints and the single constructor for production chain clients.
//!
//! An endpoint is either an explicit URL from the environment or one derived
//! from `ALCHEMY_API_KEY`. A derived endpoint keeps the key out of its URL and
//! sends it as an `Authorization: Bearer` header instead, so no URL, log line,
//! span, or error built from it carries the key. Explicit URLs may still embed
//! a provider key in their path, so every client built here also strips the
//! URL from transport errors before they reach a caller.

use alloy::rpc::client::{ClientBuilder, RpcClient};
use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use alloy::transports::http::reqwest;
use alloy::transports::http::reqwest::header::{
    AUTHORIZATION, HeaderMap, HeaderValue,
};
use alloy::transports::{
    RpcError, TransportError, TransportErrorKind, TransportFut,
};
use std::fmt::{self, Debug, Formatter};
use std::task::{Context, Poll};
use tower::{Layer, Service};
use url::Url;

use crate::config::{InvalidRpcScheme, wss_to_http};
use crate::tokenized_asset::Network;

/// The Alchemy API key every derived endpoint authenticates with.
///
/// The only constructor validates the key, so building the `Authorization`
/// header from it cannot fail later. `Debug` never prints the key and there
/// is no `Display`.
#[derive(Clone)]
pub struct AlchemyApiKey(String);

/// `ALCHEMY_API_KEY` is empty, padded, or holds a character outside
/// `[A-Za-z0-9_-]`. The message never repeats the value.
#[derive(Debug, thiserror::Error)]
#[error(
    "ALCHEMY_API_KEY is invalid: it must be a non-empty run of letters, \
     digits, '-' or '_'"
)]
pub struct InvalidAlchemyApiKey;

impl AlchemyApiKey {
    pub(crate) fn new(key: String) -> Result<Self, InvalidAlchemyApiKey> {
        let valid = !key.is_empty()
            && key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
            });

        if valid { Ok(Self(key)) } else { Err(InvalidAlchemyApiKey) }
    }

    /// The Alchemy endpoint for `network`, or `None` for a network this key
    /// does not derive (BNB Smart Chain keeps an explicit URL).
    pub(crate) fn endpoint(&self, network: Network) -> Option<RpcEndpoint> {
        let host = match network {
            Network::Base => "base-mainnet.g.alchemy.com",
            Network::Ethereum => "eth-mainnet.g.alchemy.com",
            Network::HyperEvm => "hyperliquid-mainnet.g.alchemy.com",
            Network::Robinhood => "robinhood-mainnet.g.alchemy.com",
            Network::BnbSmartChain => return None,
        };

        let url = Url::parse(&format!("https://{host}/v2")).ok()?;
        Some(RpcEndpoint { url, bearer: Some(self.clone()) })
    }

    fn bearer_header(&self) -> Result<HeaderValue, RpcClientError> {
        let mut value = HeaderValue::from_str(&format!("Bearer {}", self.0))
            .map_err(|_| RpcClientError::InvalidBearer)?;
        value.set_sensitive(true);
        Ok(value)
    }
}

impl Debug for AlchemyApiKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Where a chain client sends its JSON-RPC requests, and the bearer key it
/// authenticates with, if any.
#[derive(Clone)]
pub struct RpcEndpoint {
    url: Url,
    bearer: Option<AlchemyApiKey>,
}

impl RpcEndpoint {
    #[must_use]
    pub const fn url(&self) -> &Url {
        &self.url
    }

    /// True when the key travels in the `Authorization` header.
    #[must_use]
    pub const fn uses_bearer_auth(&self) -> bool {
        self.bearer.is_some()
    }
}

/// An explicit URL, used as written and sent without an `Authorization`
/// header.
impl From<Url> for RpcEndpoint {
    fn from(url: Url) -> Self {
        Self { url, bearer: None }
    }
}

/// Prints the scheme and host only: an explicit URL may carry a provider key
/// in its path or query.
impl Debug for RpcEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcEndpoint")
            .field(
                "url",
                &format_args!(
                    "{}://{}",
                    self.url.scheme(),
                    self.url.host_str().unwrap_or("(none)")
                ),
            )
            .field("bearer", &self.bearer)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RpcClientError {
    #[error(transparent)]
    InvalidRpcScheme(#[from] InvalidRpcScheme),
    #[error("RPC bearer key is not a valid HTTP header value")]
    InvalidBearer,
    #[error("failed to build the RPC HTTP client: {0}")]
    HttpClient(#[source] reqwest::Error),
}

/// Builds the JSON-RPC client every production chain provider connects
/// through: `ws`/`wss` URLs are mapped to HTTP, a derived endpoint's key is
/// sent as a sensitive `Authorization: Bearer` header, no client follows a
/// redirect, and transport errors lose their URL. Hand the result to `ProviderBuilder::connect_client`.
pub(crate) fn rpc_client(
    endpoint: &RpcEndpoint,
) -> Result<RpcClient, RpcClientError> {
    let url = wss_to_http(&endpoint.url)?;

    // JSON-RPC never needs a redirect, and following one could carry the
    // bearer key, or a key in an explicit URL's path (through `Referer`), to
    // whatever host the redirect names.
    let mut builder =
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
    if let Some(key) = &endpoint.bearer {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, key.bearer_header()?);
        builder = builder.default_headers(headers);
    }

    let client = builder
        .build()
        .map_err(|error| RpcClientError::HttpClient(error.without_url()))?;

    Ok(ClientBuilder::default()
        .layer(StripUrlLayer)
        .http_with_client(client, url))
}

/// Replaces every reqwest error a transport returns with its URL-less copy.
///
/// reqwest's `Display` ends with ` for url (<full url>)`, and alloy passes
/// send and body-read failures through as `TransportErrorKind::Custom`, so
/// without this layer any logged RPC error quotes the endpoint. HTTP status
/// and deserialization errors carry no URL and pass through unchanged.
#[derive(Clone, Copy, Debug, Default)]
struct StripUrlLayer;

impl<S> Layer<S> for StripUrlLayer {
    type Service = StripUrl<S>;

    fn layer(&self, inner: S) -> Self::Service {
        StripUrl(inner)
    }
}

#[derive(Clone, Debug)]
struct StripUrl<S>(S);

impl<S> Service<RequestPacket> for StripUrl<S>
where
    S: Service<
            RequestPacket,
            Response = ResponsePacket,
            Error = TransportError,
            Future = TransportFut<'static>,
        >,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx).map_err(strip_url)
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        let response = self.0.call(request);
        Box::pin(async move { response.await.map_err(strip_url) })
    }
}

fn strip_url(error: TransportError) -> TransportError {
    match error {
        RpcError::Transport(TransportErrorKind::Custom(custom)) => {
            match custom.downcast::<reqwest::Error>() {
                Ok(reqwest_error) => {
                    TransportErrorKind::custom(reqwest_error.without_url())
                }
                Err(other) => {
                    RpcError::Transport(TransportErrorKind::Custom(other))
                }
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;
    use std::net::TcpListener;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::log_count_at;

    const SECRET: &str = "SECRETkey_123-abc";

    fn key() -> AlchemyApiKey {
        AlchemyApiKey::new(SECRET.to_string()).unwrap()
    }

    /// A loopback URL nothing listens on, with the key in its path the way an
    /// explicit Alchemy URL carries it.
    fn closed_port_url() -> Url {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Url::parse(&format!("http://127.0.0.1:{port}/v2/{SECRET}")).unwrap()
    }

    #[test]
    fn derives_the_alchemy_endpoint_for_each_supported_network() {
        for (network, expected) in [
            (Network::Base, "https://base-mainnet.g.alchemy.com/v2"),
            (Network::Ethereum, "https://eth-mainnet.g.alchemy.com/v2"),
            (Network::HyperEvm, "https://hyperliquid-mainnet.g.alchemy.com/v2"),
            (Network::Robinhood, "https://robinhood-mainnet.g.alchemy.com/v2"),
        ] {
            let endpoint = key().endpoint(network).unwrap();

            assert_eq!(endpoint.url().as_str(), expected, "{network}");
            assert!(endpoint.uses_bearer_auth(), "{network}");
            assert!(!endpoint.url().as_str().contains(SECRET), "{network}");
        }
    }

    #[test]
    fn bnb_smart_chain_is_not_derived() {
        assert!(key().endpoint(Network::BnbSmartChain).is_none());
    }

    #[test]
    fn rejects_malformed_keys() {
        for bad in
            ["", " key", "key ", "a/b", "a?b", "a#b", "a b", "ключ", "a\nb"]
        {
            let error = AlchemyApiKey::new(bad.to_string()).unwrap_err();
            if !bad.is_empty() {
                assert!(!error.to_string().contains(bad), "{bad:?}");
            }
        }
    }

    #[test]
    fn debug_output_never_shows_the_key() {
        assert_eq!(format!("{:?}", key()), "<redacted>");

        let derived = key().endpoint(Network::Base).unwrap();
        let explicit = RpcEndpoint::from(closed_port_url());

        for endpoint in [derived, explicit] {
            let debug = format!("{endpoint:?}");
            assert!(!debug.contains(SECRET), "{debug}");
            assert!(!debug.contains("/v2"), "{debug}");
        }
    }

    #[tokio::test]
    async fn derived_endpoint_sends_the_key_only_as_a_bearer_header() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/v2")
                    .header("authorization", format!("Bearer {SECRET}"));
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": "0x2105"
                }));
            })
            .await;
        let endpoint = RpcEndpoint {
            url: Url::parse(&server.url("/v2")).unwrap(),
            bearer: Some(key()),
        };

        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).unwrap());

        assert_eq!(provider.get_chain_id().await.unwrap(), 8453);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn no_endpoint_ever_follows_a_redirect() {
        let derived = |url: Url| RpcEndpoint { url, bearer: Some(key()) };
        let explicit = RpcEndpoint::from;

        for (path, build) in [
            ("/v2", &derived as &dyn Fn(Url) -> RpcEndpoint),
            (&*format!("/v2/{SECRET}"), &explicit),
        ] {
            let server = MockServer::start_async().await;
            let redirect = server
                .mock_async(|when, then| {
                    when.method(POST).path(path);
                    then.status(307)
                        .header("location", server.url("/elsewhere"));
                })
                .await;
            let elsewhere = server
                .mock_async(|when, then| {
                    when.path("/elsewhere");
                    then.status(200);
                })
                .await;
            let endpoint = build(Url::parse(&server.url(path)).unwrap());

            let provider = ProviderBuilder::new()
                .connect_client(rpc_client(&endpoint).unwrap());

            provider.get_chain_id().await.unwrap_err();
            redirect.assert_async().await;
            elsewhere.assert_calls_async(0).await;
        }
    }

    #[tokio::test]
    async fn a_websocket_url_is_sent_over_http_to_the_same_endpoint() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(POST).path("/rpc");
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": "0x1"
                }));
            })
            .await;
        let endpoint = RpcEndpoint::from(
            Url::parse(&format!("ws://{}/rpc", server.address())).unwrap(),
        );

        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).unwrap());

        assert_eq!(provider.get_chain_id().await.unwrap(), 1);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn explicit_endpoint_sends_no_authorization_header() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(POST).path("/rpc").header_missing("authorization");
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": "0x1"
                }));
            })
            .await;
        let endpoint =
            RpcEndpoint::from(Url::parse(&server.url("/rpc")).unwrap());

        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).unwrap());

        assert_eq!(provider.get_chain_id().await.unwrap(), 1);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn transport_errors_never_carry_the_url() {
        let endpoint = RpcEndpoint::from(closed_port_url());
        let read = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).unwrap());
        let signing = ProviderBuilder::new()
            .wallet(PrivateKeySigner::random())
            .connect_client(rpc_client(&endpoint).unwrap());

        let read_error = read.get_chain_id().await.unwrap_err();
        let signing_error = signing.get_chain_id().await.unwrap_err();

        for error in [read_error, signing_error] {
            let rendered = [
                error.to_string(),
                format!("{error:?}"),
                format!("{:#}", anyhow::Error::new(error)),
            ];
            for text in rendered {
                assert!(!text.contains(SECRET), "{text}");
                assert!(!text.contains("/v2/"), "{text}");
            }
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn a_logged_transport_error_does_not_leak_the_key() {
        let endpoint = RpcEndpoint::from(closed_port_url());
        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).unwrap());

        let error = provider.get_block_number().await.unwrap_err();
        tracing::warn!(target: "startup", error = %error, "RPC poll failed");

        assert_eq!(log_count_at!(Level::WARN, &["RPC poll failed"]), 1);
        // Scoped to the WARN event: alloy's own debug span names the URL,
        // which the default log filter never enables.
        assert_eq!(log_count_at!(Level::WARN, &[SECRET]), 0);
    }

    #[test]
    fn non_reqwest_errors_pass_through_unchanged() {
        let status = TransportErrorKind::http_error(429, "slow down".into());
        assert!(matches!(
            strip_url(status),
            RpcError::Transport(TransportErrorKind::HttpError(http))
                if http.status == 429 && http.body == "slow down"
        ));

        let custom = TransportErrorKind::custom_str("backend unavailable");
        assert_eq!(strip_url(custom).to_string(), "backend unavailable");

        let deser = TransportError::deser_err(
            serde_json::from_str::<u64>("x").unwrap_err(),
            "x",
        );
        assert!(matches!(strip_url(deser), RpcError::DeserError { .. }));
    }
}
