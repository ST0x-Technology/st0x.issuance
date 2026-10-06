//! Thin HTTP transport for the S01 issuance ops API: sends one tier-prefixed
//! route with the bearer ID token and maps the response to `TransportError`.
//! Holds no domain logic; the bot validates and decides everything.

use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use reqwest::redirect::Policy;
use reqwest::{Method, StatusCode};
use serde::Serialize;
use st0x_issuance_dto::{
    AddTokenizedAssetRequest, RegisterAccountRequest,
    ScheduleFreezeWindowRequest, WhitelistWalletRequest,
};
use std::time::Duration;
use url::Url;

/// Overall per-request bound, long enough for an orchestrator approval, which
/// waits for its on-chain receipt before answering.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Operator tier; the load balancer routes each prefix to its own IAP backend
/// and the bot pins that backend's audience.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Tier {
    Read,
    Debug,
    Capital,
}

impl Tier {
    const fn prefix(self) -> &'static str {
        match self {
            Self::Read => "/ops/read",
            Self::Debug => "/ops/debug",
            Self::Capital => "/ops/capital",
        }
    }
}

/// A request body: always one of the shared DTO types the bot deserializes, so
/// the client cannot drift from the bot's body shape.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum RouteBody {
    RegisterAccount(RegisterAccountRequest),
    WhitelistWallet(WhitelistWalletRequest),
    AddTokenizedAsset(AddTokenizedAssetRequest),
    ScheduleFreezeWindow(ScheduleFreezeWindowRequest),
}

/// One ops route: exactly what goes on the wire for one command.
#[derive(Debug)]
pub(crate) struct Route {
    pub(crate) method: Method,
    pub(crate) tier: Tier,
    /// Path below the tier prefix; every interpolated segment is already
    /// escaped with [`encode_segment`].
    pub(crate) path: String,
    pub(crate) query: Vec<(&'static str, String)>,
    pub(crate) body: Option<RouteBody>,
}

const SIGN_IN_AGAIN: &str = "Re-run the command to sign in again, or delete \
     st0x-issuance-client/oauth-<env>.json under your XDG config directory to \
     force it; in CI, mint a fresh ID token for this environment.";

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransportError {
    #[error(
        "the request to the S01 ops API at {url} failed: {}",
        error_chain(.source)
    )]
    Request {
        url: Url,
        #[source]
        source: reqwest::Error,
    },
    #[error(
        "IAP redirected to sign-in (HTTP {status}, location {location}): the \
         S01 Google identity was missing, expired, or not accepted.\n{}",
        SIGN_IN_AGAIN
    )]
    Redirect { status: StatusCode, location: String },
    #[error(
        "HTTP 401 Unauthorized: the S01 Google identity was missing, expired, \
         or rejected, including a token minted for another tier's \
         audience.\n{}{}",
        SIGN_IN_AGAIN,
        server_said(.body)
    )]
    Unauthorized { body: String },
    #[error(
        "HTTP 403 Forbidden: authenticated, but this S01 account is not in the \
         Workspace group IAP requires for this tier.{}",
        server_said(.body)
    )]
    Forbidden { body: String },
    #[error(
        "HTTP 404 Not Found: an unknown id or asset, or the ops API is not \
         mounted on this deployment.{}",
        server_said(.body)
    )]
    NotFound { body: String },
    #[error(
        "HTTP 503 Service Unavailable: the request never reached a handler \
         (for example the bot could not fetch Google's IAP keys); retrying is \
         safe.{}",
        server_said(.body)
    )]
    Unavailable { body: String },
    #[error("HTTP {status}: the request failed.{}", server_said(.body))]
    Http { status: StatusCode, body: String },
    #[error(
        "expected JSON but the endpoint returned {content_type} ({source}); \
         this usually means the ops API is not deployed at this path. Body \
         starts: {body_prefix}"
    )]
    Decode {
        content_type: String,
        body_prefix: String,
        #[source]
        source: serde_json::Error,
    },
}

impl TransportError {
    /// An authentication or authorization refusal, as opposed to a request
    /// the bot accepted and failed.
    pub(crate) const fn is_access_denied(&self) -> bool {
        matches!(
            self,
            Self::Redirect { .. }
                | Self::Unauthorized { .. }
                | Self::Forbidden { .. }
        )
    }

    /// Whether the S01 deployment answered, so its logs may explain the
    /// failure; a request that never connected left nothing there.
    pub(crate) const fn reached_server(&self) -> bool {
        !matches!(self, Self::Request { .. })
    }
}

/// An error and every cause beneath it on one line. reqwest's own message is
/// only "error sending request"; the cause (TLS verification, DNS, a refused
/// connection) is what an operator needs.
pub(crate) fn error_chain(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();

    while let Some(cause) = source {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        source = cause.source();
    }

    rendered
}

fn server_said(body: &str) -> String {
    let trimmed = body.trim();

    if trimmed.is_empty() {
        String::new()
    } else {
        format!("\nServer said: {trimmed}")
    }
}

/// Sends routes to one environment's ops API with one ID token.
pub(crate) struct Client {
    http: reqwest::Client,
    base_url: Url,
    token: String,
}

impl Client {
    pub(crate) fn new(
        base_url: Url,
        token: String,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;

        Ok(Self { http, base_url, token })
    }

    pub(crate) async fn send(
        &self,
        route: &Route,
    ) -> Result<serde_json::Value, TransportError> {
        let url = self.url(route);
        let request = self
            .http
            .request(route.method.clone(), url.clone())
            .bearer_auth(&self.token);

        // A bodiless write still declares its empty body, since some proxies
        // refuse a POST or DELETE without a length.
        let request = match &route.body {
            Some(body) => request.json(body),
            None if route.method == Method::GET => request,
            None => request.header(CONTENT_LENGTH, "0"),
        };

        let response = request.send().await.map_err(|source| {
            TransportError::Request { url: url.clone(), source }
        })?;

        classify(response, url).await
    }

    pub(crate) fn url(&self, route: &Route) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("{}{}", route.tier.prefix(), route.path));
        url.set_query(None);

        if !route.query.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &route.query {
                pairs.append_pair(key, value);
            }
        }

        url
    }
}

async fn classify(
    response: reqwest::Response,
    url: Url,
) -> Result<serde_json::Value, TransportError> {
    let status = response.status();

    // Redirects are never followed: IAP answers a missing or rejected
    // identity with a bounce to its sign-in page.
    if status.is_redirection() {
        return Err(TransportError::Redirect {
            status,
            location: header_text(&response, LOCATION),
        });
    }

    if status == StatusCode::NO_CONTENT {
        return Ok(serde_json::Value::Null);
    }

    let content_type = header_text(&response, CONTENT_TYPE);
    let body = response
        .text()
        .await
        .map_err(|source| TransportError::Request { url, source })?;

    if status.is_success() {
        return serde_json::from_str(&body).map_err(|source| {
            TransportError::Decode {
                content_type,
                body_prefix: body_prefix(&body),
                source,
            }
        });
    }

    Err(match status {
        StatusCode::UNAUTHORIZED => TransportError::Unauthorized { body },
        StatusCode::FORBIDDEN => TransportError::Forbidden { body },
        StatusCode::NOT_FOUND => TransportError::NotFound { body },
        StatusCode::SERVICE_UNAVAILABLE => TransportError::Unavailable { body },
        status => TransportError::Http { status, body },
    })
}

/// A header's value for display; absent or non-text headers render empty.
fn header_text(
    response: &reqwest::Response,
    name: reqwest::header::HeaderName,
) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map_or_else(String::new, str::to_owned)
}

/// A short, single-line preview of a response body, for error messages.
fn body_prefix(body: &str) -> String {
    body.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(120)
        .collect()
}

/// Percent-encodes one path segment so an interpolated value (an id, symbol,
/// or address) cannot inject extra `/` segments or a `?`/`#` that would change
/// routing. Keeps the RFC 3986 unreserved set; encodes everything else.
pub(crate) fn encode_segment(segment: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(segment.len());
    for &byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'.'
            | b'_'
            | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => {
                encoded.push('%');
                encoded.push(char::from(HEX[usize::from(byte >> 4)]));
                encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }

    encoded
}

#[cfg(test)]
mod tests {
    use alloy_primitives::address;
    use reqwest::Method;
    use serde_json::json;
    use st0x_issuance_dto::{Email, RegisterAccountRequest};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::{Receiver, channel};
    use url::Url;

    use super::{
        Client, Route, RouteBody, Tier, TransportError, encode_segment,
    };

    /// Serves one connection: captures the full raw request (headers plus a
    /// `Content-Length` body), hands it back over the channel, then writes
    /// `response`.
    fn serve(response: &'static str) -> (Url, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (sender, receiver) = channel();

        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            sender.send(read_request(&mut stream)).unwrap();
            stream.write_all(response.as_bytes()).unwrap();
        });

        (Url::parse(&format!("http://127.0.0.1:{port}")).unwrap(), receiver)
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut raw = Vec::new();
        let mut chunk = [0u8; 1024];

        loop {
            let count = stream.read(&mut chunk).unwrap();
            if count == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..count]);

            let Some(header_end) =
                raw.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers =
                String::from_utf8_lossy(&raw[..header_end]).to_lowercase();
            let length = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .map_or(0, |value| value.trim().parse::<usize>().unwrap());
            if raw.len() >= header_end + 4 + length {
                break;
            }
        }

        String::from_utf8(raw).unwrap()
    }

    fn header<'request>(
        request: &'request str,
        name: &str,
    ) -> Option<&'request str> {
        request.lines().skip(1).find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    fn body(request: &str) -> serde_json::Value {
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        serde_json::from_str(body).unwrap()
    }

    const OK_JSON: &str = "HTTP/1.1 200 OK\r\nContent-Type: \
        application/json\r\nContent-Length: 11\r\nConnection: \
        close\r\n\r\n{\"ok\":true}";

    fn route(method: Method, tier: Tier, path: &str) -> Route {
        Route {
            method,
            tier,
            path: path.to_owned(),
            query: Vec::new(),
            body: None,
        }
    }

    #[tokio::test]
    async fn a_get_goes_under_its_tier_prefix_with_the_bearer_token() {
        let (base, requests) = serve(OK_JSON);
        let client = Client::new(base, "id-token".to_owned()).unwrap();
        let mut read = route(Method::GET, Tier::Read, "/wrapped-transfers");
        read.query = vec![
            ("limit", "5".to_owned()),
            ("before_network", "a b".to_owned()),
        ];

        let value = client.send(&read).await.unwrap();
        let request = requests.recv().unwrap();

        assert_eq!(value, json!({ "ok": true }));
        assert!(
            request.starts_with(
                "GET /ops/read/wrapped-transfers?limit=5&before_network=a+b "
            ),
            "{request}"
        );
        assert_eq!(header(&request, "authorization"), Some("Bearer id-token"));
    }

    #[tokio::test]
    async fn a_typed_body_is_sent_as_json_under_its_tier() {
        let (base, requests) = serve(OK_JSON);
        let client = Client::new(base, "id-token".to_owned()).unwrap();
        let mut register = route(Method::POST, Tier::Debug, "/accounts");
        register.body =
            Some(RouteBody::RegisterAccount(RegisterAccountRequest {
                email: Email::new("ops@example.com").unwrap(),
            }));

        client.send(&register).await.unwrap();
        let request = requests.recv().unwrap();

        assert!(request.starts_with("POST /ops/debug/accounts "), "{request}");
        assert_eq!(header(&request, "content-type"), Some("application/json"));
        assert_eq!(body(&request), json!({ "email": "ops@example.com" }));
    }

    #[tokio::test]
    async fn a_bodiless_write_declares_an_empty_body() {
        let (base, requests) = serve(OK_JSON);
        let client = Client::new(base, "id-token".to_owned()).unwrap();
        let wallet = address!("0x1111111111111111111111111111111111111111");
        let unwhitelist = route(
            Method::DELETE,
            Tier::Debug,
            &format!("/accounts/client/wallets/{wallet}"),
        );

        client.send(&unwhitelist).await.unwrap();
        let request = requests.recv().unwrap();

        assert!(
            request.starts_with(&format!(
                "DELETE /ops/debug/accounts/client/wallets/{wallet} "
            )),
            "{request}"
        );
        assert_eq!(header(&request, "content-length"), Some("0"));
    }

    async fn failure(response: &'static str) -> TransportError {
        let (base, _requests) = serve(response);
        let client = Client::new(base, "id-token".to_owned()).unwrap();

        client
            .send(&route(Method::POST, Tier::Capital, "/freeze/AAPL"))
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn an_iap_sign_in_bounce_is_an_access_denial() {
        let error = failure(
            "HTTP/1.1 302 Found\r\nLocation: https://accounts.google.com/x\r\n\
             Content-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(matches!(error, TransportError::Redirect { .. }));
        assert!(error.is_access_denied());
    }

    #[tokio::test]
    async fn statuses_map_to_their_explanations() {
        let unauthorized = failure(
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: 4\r\n\
             Connection: close\r\n\r\nnope",
        )
        .await;
        assert!(
            matches!(&unauthorized, TransportError::Unauthorized { body } if body == "nope")
        );
        assert!(unauthorized.is_access_denied());
        assert!(unauthorized.to_string().contains("Server said: nope"));

        let forbidden = failure(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n",
        )
        .await;
        assert!(matches!(forbidden, TransportError::Forbidden { .. }));
        assert!(forbidden.is_access_denied());

        let not_found = failure(
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n",
        )
        .await;
        assert!(matches!(not_found, TransportError::NotFound { .. }));
        assert!(!not_found.is_access_denied());

        let unavailable = failure(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n",
        )
        .await;
        assert!(matches!(unavailable, TransportError::Unavailable { .. }));

        let conflict = failure(
            "HTTP/1.1 409 Conflict\r\nContent-Length: 4\r\n\
             Connection: close\r\n\r\nbusy",
        )
        .await;
        assert!(matches!(
            conflict,
            TransportError::Http { status, ref body }
                if status == reqwest::StatusCode::CONFLICT && body == "busy"
        ));
        assert!(conflict.reached_server());
    }

    #[tokio::test]
    async fn a_non_json_success_is_a_decode_error() {
        let error = failure(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: \
             15\r\nConnection: close\r\n\r\n<html>lb</html>",
        )
        .await;

        assert!(matches!(
            error,
            TransportError::Decode { ref content_type, .. } if content_type == "text/html"
        ));
    }

    #[tokio::test]
    async fn no_content_renders_as_null() {
        let (base, _requests) =
            serve("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
        let client = Client::new(base, "id-token".to_owned()).unwrap();

        let value = client
            .send(&route(Method::POST, Tier::Capital, "/freeze/AAPL"))
            .await
            .unwrap();

        assert_eq!(value, serde_json::Value::Null);
    }

    #[tokio::test]
    async fn an_unreachable_host_did_not_reach_the_server() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = Client::new(
            Url::parse(&format!("http://127.0.0.1:{port}")).unwrap(),
            "id-token".to_owned(),
        )
        .unwrap();

        let error = client
            .send(&route(Method::GET, Tier::Read, "/stuck"))
            .await
            .unwrap_err();

        assert!(matches!(error, TransportError::Request { .. }));
        assert!(!error.reached_server());
    }

    #[test]
    fn encode_segment_escapes_routing_characters() {
        assert_eq!(encode_segment("r/1?x#y z"), "r%2F1%3Fx%23y%20z");
        assert_eq!(encode_segment("AAPL-1.0_~"), "AAPL-1.0_~");
    }
}
