//! RPC endpoints and the single constructor for production chain clients.
//!
//! A bare Alchemy URL (`https://<network>.g.alchemy.com/v2`) is called with a
//! short-lived JWT that Cloud KMS signs, and any other Alchemy URL must be
//! an `https` `/v2/<key>` URL. Every other URL is called as written.
//! Every client is built on the `st0x-rpc` transport, which follows no
//! redirect and keeps the URL out of its errors, because an explicit URL may
//! embed a provider key in its path.

use alloy::rpc::client::RpcClient;
use st0x_rpc::{AlchemyJwtConfig, JwtEnv};
use std::fmt::{self, Debug, Formatter};
use url::Url;

use crate::config::{InvalidRpcScheme, wss_to_http};

/// `st0x-rpc` keeps its own name for "neither setting is set" private.
const BOTH_JWT_VARS: &str = "ALCHEMY_JWT_KMS_KEY_VERSION and ALCHEMY_JWT_KID";

/// Where a chain client sends its JSON-RPC requests, and how it
/// authenticates.
#[derive(Clone)]
pub struct RpcEndpoint {
    url: Url,
    auth: RpcAuth,
}

#[derive(Clone, Debug)]
enum RpcAuth {
    Plain,
    AlchemyJwt(AlchemyJwtConfig),
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidRpcEndpoint {
    /// A bare Alchemy URL has no other way to authenticate.
    #[error("a bare Alchemy URL needs {missing}")]
    AlchemyJwtNotConfigured { missing: &'static str },
    /// `st0x-rpc` refuses it at connect time: it is neither the bare JWT
    /// form nor a keyed URL Alchemy would accept.
    #[error(
        "an Alchemy URL must be the bare https://<network>.g.alchemy.com/v2 \
         form or an https /v2/<key> URL"
    )]
    MalformedAlchemyUrl,
    /// A keyed Alchemy URL carries its API key in the path, so it must not
    /// travel in cleartext.
    #[error("an Alchemy URL must use https or wss")]
    InsecureAlchemyUrl,
}

impl RpcEndpoint {
    /// Keeps `url` as configured. A bare Alchemy URL, also in its `wss://`
    /// form, is called with the JWT; any other URL is plain and ignores the
    /// settings.
    ///
    /// # Errors
    ///
    /// [`InvalidRpcEndpoint::AlchemyJwtNotConfigured`] when `url` is bare
    /// Alchemy and `jwt` lacks either setting, and
    /// [`InvalidRpcEndpoint::InsecureAlchemyUrl`] for a keyed Alchemy URL over
    /// `http://` or `ws://`, and [`InvalidRpcEndpoint::MalformedAlchemyUrl`]
    /// for any other Alchemy URL that is neither bare nor `https` with a
    /// `/v2/<key>` path (another port, a query, a trailing dot, no key).
    pub fn new(url: Url, jwt: &JwtEnv) -> Result<Self, InvalidRpcEndpoint> {
        let http = wss_to_http(&url).ok();
        let bare = http
            .as_ref()
            .is_some_and(|http| st0x_rpc::uses_alchemy_jwt(http.as_str()));

        if !bare {
            if let Some(http) =
                http.as_ref().filter(|http| is_alchemy_host(http))
            {
                if http.scheme() == "http" && has_path_key(http) {
                    return Err(InvalidRpcEndpoint::InsecureAlchemyUrl);
                }

                if http.scheme() != "https" || !has_path_key(http) {
                    return Err(InvalidRpcEndpoint::MalformedAlchemyUrl);
                }
            }

            return Ok(Self { url, auth: RpcAuth::Plain });
        }

        match jwt {
            JwtEnv::Both(config) => {
                Ok(Self { url, auth: RpcAuth::AlchemyJwt(config.clone()) })
            }
            JwtEnv::Partial { missing } => {
                Err(InvalidRpcEndpoint::AlchemyJwtNotConfigured { missing })
            }
            JwtEnv::Unset => Err(InvalidRpcEndpoint::AlchemyJwtNotConfigured {
                missing: BOTH_JWT_VARS,
            }),
        }
    }

    #[must_use]
    pub const fn url(&self) -> &Url {
        &self.url
    }

    #[must_use]
    pub const fn uses_alchemy_jwt(&self) -> bool {
        matches!(self.auth, RpcAuth::AlchemyJwt(_))
    }

    /// The settings `st0x-rpc` gets for this endpoint. A plain endpoint gets
    /// `Unset` so the crate does not warn about unused settings on every
    /// connect; config warns once at startup instead.
    fn jwt_env(&self) -> JwtEnv {
        match &self.auth {
            RpcAuth::Plain => JwtEnv::Unset,
            RpcAuth::AlchemyJwt(config) => JwtEnv::Both(config.clone()),
        }
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
            .field("auth", &self.auth)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RpcClientError {
    #[error(transparent)]
    InvalidRpcScheme(#[from] InvalidRpcScheme),
    #[error(transparent)]
    Rpc(#[from] st0x_rpc::Error),
}

/// Builds the JSON-RPC client every production chain provider connects
/// through: `ws`/`wss` URLs are mapped to HTTP, and a JWT endpoint is checked
/// with one signed `eth_chainId` call before it is returned. Hand the result
/// to `ProviderBuilder::connect_client`.
pub(crate) async fn rpc_client(
    endpoint: &RpcEndpoint,
) -> Result<RpcClient, RpcClientError> {
    let url = wss_to_http(&endpoint.url)?;

    Ok(st0x_rpc::connect_with(url.as_str(), endpoint.jwt_env()).await?)
}

/// With [`has_path_key`], mirrors the crate's private
/// `is_malformed_alchemy_jwt_url`, so a URL it would refuse at connect time
/// fails config instead.
fn is_alchemy_host(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.to_ascii_lowercase()
            .trim_end_matches('.')
            .ends_with(".g.alchemy.com")
    })
}

/// `/v2/<key>`, with an optional trailing slash.
fn has_path_key(url: &Url) -> bool {
    let path = url.path();
    path.strip_suffix('/')
        .unwrap_or(path)
        .strip_prefix("/v2/")
        .is_some_and(|key| !key.is_empty() && !key.contains('/'))
}

#[cfg(test)]
mod tests {
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;
    use st0x_rpc::{KEY_VERSION_VAR, KID_VAR};
    use std::net::TcpListener;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::log_count_at;

    const SECRET: &str = "SECRETkey_123-abc";

    fn url(raw: &str) -> Url {
        Url::parse(raw).unwrap()
    }

    fn both() -> JwtEnv {
        JwtEnv::Both(AlchemyJwtConfig {
            key_version: "projects/p/locations/l/keyRings/r/cryptoKeys/k/\
                          cryptoKeyVersions/1"
                .to_string(),
            kid: "test-kid".to_string(),
        })
    }

    fn plain(raw: &str) -> RpcEndpoint {
        RpcEndpoint::new(url(raw), &JwtEnv::Unset).unwrap()
    }

    /// A loopback URL nothing listens on, with the key in its path the way an
    /// explicit Alchemy URL carries it.
    fn closed_port_url() -> String {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        format!("http://127.0.0.1:{port}/v2/{SECRET}")
    }

    #[test]
    fn bare_alchemy_urls_use_the_jwt() {
        for raw in [
            "https://base-mainnet.g.alchemy.com/v2",
            "https://base-mainnet.g.alchemy.com/v2/",
            "https://BASE-MAINNET.G.ALCHEMY.COM/v2",
            "https://base-mainnet.g.alchemy.com:443/v2",
            "wss://base-mainnet.g.alchemy.com/v2",
        ] {
            let endpoint = RpcEndpoint::new(url(raw), &both()).unwrap();
            assert!(endpoint.uses_alchemy_jwt(), "{raw}");
        }
    }

    #[test]
    fn other_urls_are_plain_even_with_jwt_settings() {
        for raw in [
            "https://base-mainnet.g.alchemy.com/v2/abc123",
            "https://base-mainnet.g.alchemy.com/v2/abc123/",
            "wss://base-mainnet.g.alchemy.com/v2/abc123",
            "https://mainnet.base.org",
            "https://mainnet.base.org/v2",
        ] {
            let endpoint = RpcEndpoint::new(url(raw), &both()).unwrap();
            assert!(!endpoint.uses_alchemy_jwt(), "{raw}");
        }
    }

    #[test]
    fn an_alchemy_url_neither_bare_nor_keyed_is_refused() {
        for raw in [
            "http://base-mainnet.g.alchemy.com/v2",
            "ws://base-mainnet.g.alchemy.com/v2",
            "https://base-mainnet.g.alchemy.com:8443/v2",
            "https://base-mainnet.g.alchemy.com./v2",
            "https://base-mainnet.g.alchemy.com/v2?key=abc",
            "https://base-mainnet.g.alchemy.com",
            "https://base-mainnet.g.alchemy.com/V2",
            "https://base-mainnet.g.alchemy.com/v2//",
            "https://base-mainnet.g.alchemy.com/v2/abc123/extra",
        ] {
            let error = RpcEndpoint::new(url(raw), &both()).unwrap_err();
            assert!(
                matches!(error, InvalidRpcEndpoint::MalformedAlchemyUrl),
                "{raw}: {error:?}"
            );
        }
    }

    #[test]
    fn a_keyed_alchemy_url_over_cleartext_is_refused() {
        for raw in [
            "http://base-mainnet.g.alchemy.com/v2/abc123",
            "ws://base-mainnet.g.alchemy.com/v2/abc123",
            "http://BASE-MAINNET.G.ALCHEMY.COM./v2/abc123",
        ] {
            let error = RpcEndpoint::new(url(raw), &JwtEnv::Unset).unwrap_err();
            assert!(
                matches!(error, InvalidRpcEndpoint::InsecureAlchemyUrl),
                "{raw}: {error:?}"
            );
        }
    }

    #[test]
    fn the_url_is_kept_as_configured() {
        let endpoint = RpcEndpoint::new(
            url("wss://base-mainnet.g.alchemy.com/v2"),
            &both(),
        )
        .unwrap();

        assert_eq!(
            endpoint.url().as_str(),
            "wss://base-mainnet.g.alchemy.com/v2"
        );
    }

    #[test]
    fn a_bare_alchemy_url_without_both_settings_is_refused() {
        for (jwt, missing) in [
            (JwtEnv::Unset, BOTH_JWT_VARS),
            (JwtEnv::Partial { missing: KID_VAR }, KID_VAR),
            (JwtEnv::Partial { missing: KEY_VERSION_VAR }, KEY_VERSION_VAR),
        ] {
            let error = RpcEndpoint::new(
                url("https://eth-mainnet.g.alchemy.com/v2"),
                &jwt,
            )
            .unwrap_err();

            assert!(
                matches!(
                    error,
                    InvalidRpcEndpoint::AlchemyJwtNotConfigured { missing: got }
                        if got == missing
                ),
                "{jwt:?}: {error:?}"
            );
        }
    }

    #[test]
    fn debug_output_never_shows_the_path() {
        let jwt = RpcEndpoint::new(
            url("https://base-mainnet.g.alchemy.com/v2"),
            &both(),
        )
        .unwrap();
        let explicit = plain(&closed_port_url());

        for endpoint in [jwt, explicit] {
            let debug = format!("{endpoint:?}");
            assert!(!debug.contains(SECRET), "{debug}");
            assert!(!debug.contains("/v2"), "{debug}");
        }
    }

    #[test]
    fn only_a_jwt_endpoint_hands_the_settings_to_the_crate() {
        for raw in [
            "https://base-mainnet.g.alchemy.com/v2",
            "wss://base-mainnet.g.alchemy.com/v2",
        ] {
            let endpoint = RpcEndpoint::new(url(raw), &both()).unwrap();
            assert_eq!(endpoint.jwt_env(), both(), "{raw}");
        }

        let keyed = RpcEndpoint::new(
            url("https://base-mainnet.g.alchemy.com/v2/abc123"),
            &both(),
        )
        .unwrap();
        assert_eq!(keyed.jwt_env(), JwtEnv::Unset);
    }

    #[tokio::test]
    async fn a_plain_endpoint_connects_without_any_call() {
        rpc_client(&plain(&closed_port_url())).await.unwrap();
    }

    #[tokio::test]
    async fn no_endpoint_ever_follows_a_redirect() {
        let path = format!("/v2/{SECRET}");
        let server = MockServer::start_async().await;
        let redirect = server
            .mock_async(|when, then| {
                when.method(POST).path(&path);
                then.status(307).header("location", server.url("/elsewhere"));
            })
            .await;
        let elsewhere = server
            .mock_async(|when, then| {
                when.path("/elsewhere");
                then.status(200);
            })
            .await;
        let endpoint = plain(&server.url(&path));

        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).await.unwrap());

        provider.get_chain_id().await.unwrap_err();
        redirect.assert_async().await;
        elsewhere.assert_calls_async(0).await;
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
        let endpoint = plain(&format!("ws://{}/rpc", server.address()));

        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).await.unwrap());

        assert_eq!(provider.get_chain_id().await.unwrap(), 1);
        mock.assert_async().await;
    }

    #[traced_test]
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
            RpcEndpoint::new(url(&server.url("/rpc")), &both()).unwrap();

        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).await.unwrap());

        assert_eq!(provider.get_chain_id().await.unwrap(), 1);
        mock.assert_async().await;
        assert!(logs_contain("RPC: plain HTTP"));
        assert!(!logs_contain("Alchemy JWT settings"));
    }

    #[tokio::test]
    async fn transport_errors_never_carry_the_url() {
        let endpoint = plain(&closed_port_url());
        let read = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).await.unwrap());
        let signing = ProviderBuilder::new()
            .wallet(PrivateKeySigner::random())
            .connect_client(rpc_client(&endpoint).await.unwrap());

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
        let endpoint = plain(&closed_port_url());
        let provider = ProviderBuilder::new()
            .connect_client(rpc_client(&endpoint).await.unwrap());

        let error = provider.get_block_number().await.unwrap_err();
        tracing::warn!(target: "startup", error = %error, "RPC poll failed");

        assert_eq!(log_count_at!(Level::WARN, &["RPC poll failed"]), 1);
        assert!(!logs_contain(SECRET));
    }
}
