//! Thin HTTP transport for the S01 issuance ops API: sends one tier-prefixed
//! route with the bearer ID token and maps the response to `TransportError`.
//! Holds no domain logic; the bot validates and decides everything.

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use reqwest::redirect::Policy;
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::IgnoredAny;
use st0x_issuance_dto::{
    AddTokenizedAssetRequest, RegisterAccountRequest,
    ScheduleFreezeWindowRequest, WhitelistWalletRequest,
};
use std::time::Duration;
use url::Url;

/// Overall per-request bound. An orchestrator approval waits for its on-chain
/// receipt before answering and can outlast it, which surfaces as
/// `NoResponse`: the outcome is unknown and the logs say what happened.
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

const SIGN_IN_AGAIN: &str = "Re-running alone reuses the cached S01 sign-in. \
     To sign in again (for example as another S01 account), delete \
     st0x-issuance-client/oauth-<env>.json under your XDG config directory \
     and re-run; in CI, mint a fresh ID token for this environment.";

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransportError {
    #[error(
        "the request to the S01 ops API at {url} was not sent: {}",
        error_chain(.source)
    )]
    NotSent {
        url: Url,
        #[source]
        source: reqwest::Error,
    },
    #[error(
        "the S01 ops API at {url} did not answer: {}\nThe request may have \
         been applied; check the logs before retrying a write.",
        error_chain(.source)
    )]
    NoResponse {
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
        "HTTP 503 Service Unavailable: the deployment could not serve the \
         request (for example the bot could not fetch Google's IAP keys, or \
         the load balancer had no healthy backend). A read can be retried; \
         before retrying a write, check the logs for whether it was \
         applied.{}",
        server_said(.body)
    )]
    Unavailable { body: String },
    /// 502 or 504: something between the operator and the bot gave up waiting,
    /// either the load balancer's backend timeout or the bot's own wait on the
    /// chain, so a write may still have been applied.
    #[error(
        "HTTP {status}: a gateway gave up waiting for the bot, so the outcome \
         is unknown. A read can be retried; before retrying a write, check the \
         logs for whether it was applied.{}",
        server_said(.body)
    )]
    OutcomeUnknown { status: StatusCode, body: String },
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

    /// Whether the request may have reached the S01 deployment, so its logs
    /// may explain the failure; only a request never sent left nothing there.
    pub(crate) const fn reached_server(&self) -> bool {
        !matches!(self, Self::NotSent { .. })
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

/// The server's own words for an error: a JSON body in full, anything else (an
/// IAP or load balancer HTML page) as a one-line preview.
pub(crate) fn server_said(body: &str) -> String {
    let trimmed = body.trim();

    if trimmed.is_empty() {
        String::new()
    } else if serde_json::from_str::<IgnoredAny>(trimmed).is_ok() {
        format!("\nServer said: {trimmed}")
    } else {
        format!("\nServer said: {}", body_prefix(trimmed))
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
    ) -> Result<String, TransportError> {
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

        // Only a failure to build the request or to connect proves it never
        // left; a timeout or reset after that leaves the outcome unknown.
        let response = request.send().await.map_err(|source| {
            if source.is_connect() || source.is_builder() {
                TransportError::NotSent { url: url.clone(), source }
            } else {
                TransportError::NoResponse { url: url.clone(), source }
            }
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

/// Maps a response to its body, which must be JSON, or to the failure its
/// status means. The body is returned verbatim so the output is exactly what
/// the bot sent.
async fn classify(
    response: reqwest::Response,
    url: Url,
) -> Result<String, TransportError> {
    let status = response.status();

    // Redirects are never followed: IAP answers a missing or rejected
    // identity with a bounce to its sign-in page.
    if status.is_redirection() {
        return Err(TransportError::Redirect {
            status,
            location: header_text(&response, LOCATION),
        });
    }

    let content_type = header_text(&response, CONTENT_TYPE);
    // An error status already says what happened, so its body is only
    // diagnostic; a success whose body was cut off leaves the outcome unknown.
    let body = match response.text().await {
        Ok(body) => body,
        Err(_) if !status.is_success() => String::new(),
        Err(source) => return Err(TransportError::NoResponse { url, source }),
    };

    if status.is_success() {
        return match serde_json::from_str::<IgnoredAny>(&body) {
            Ok(IgnoredAny) => Ok(body),
            Err(source) => Err(TransportError::Decode {
                content_type,
                body_prefix: body_prefix(&body),
                source,
            }),
        };
    }

    Err(match status {
        StatusCode::UNAUTHORIZED => TransportError::Unauthorized { body },
        StatusCode::FORBIDDEN => TransportError::Forbidden { body },
        StatusCode::NOT_FOUND => TransportError::NotFound { body },
        StatusCode::SERVICE_UNAVAILABLE => TransportError::Unavailable { body },
        StatusCode::BAD_GATEWAY | StatusCode::GATEWAY_TIMEOUT => {
            TransportError::OutcomeUnknown { status, body }
        }
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

/// Everything outside the RFC 3986 unreserved set (`A-Z a-z 0-9 - . _ ~`).
const SEGMENT_RESERVED: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// Percent-encodes one path segment so an interpolated value (an id, symbol,
/// or address) cannot inject extra `/` segments or a `?`/`#` that would change
/// routing.
pub(crate) fn encode_segment(segment: &str) -> String {
    utf8_percent_encode(segment, SEGMENT_RESERVED).to_string()
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

    /// Keys out of alphabetical order, so a re-serialized body would differ.
    const OK_JSON: &str = "HTTP/1.1 200 OK\r\nContent-Type: \
        application/json\r\nContent-Length: 13\r\nConnection: \
        close\r\n\r\n{\"b\":1,\"a\":2}";

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

        assert_eq!(value, r#"{"b":1,"a":2}"#, "the body is passed through");
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

        // A gateway that gave up waiting (the load balancer's backend timeout,
        // or the bot's own RPC wait) may still see the write land, so these
        // must not read as a plain failure an operator would retry.
        for response in [
            "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n",
            "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n",
        ] {
            let gateway = failure(response).await;
            let message = gateway.to_string();
            assert!(message.contains("outcome is unknown"), "{message}");
            assert!(message.contains("check the logs"), "{message}");
            assert!(gateway.reached_server());
            assert!(!gateway.is_access_denied());
        }

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
    async fn a_body_cut_off_after_the_status_may_have_been_applied() {
        let error = failure(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
             Content-Length: 50\r\nConnection: close\r\n\r\n{\"ok\":",
        )
        .await;

        assert!(matches!(error, TransportError::NoResponse { .. }));
        assert!(error.reached_server(), "its logs may say what happened");
    }

    #[tokio::test]
    async fn a_connection_dropped_after_the_request_may_have_been_applied() {
        let error = failure("").await;

        assert!(matches!(error, TransportError::NoResponse { .. }));
        assert!(error.reached_server(), "its logs may say what happened");
    }

    #[tokio::test]
    async fn a_cut_off_error_body_keeps_the_known_denial() {
        let error = failure(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 50\r\n\
             Connection: close\r\n\r\npartial",
        )
        .await;

        assert!(matches!(error, TransportError::Forbidden { .. }));
        assert!(error.is_access_denied());
    }

    #[tokio::test]
    async fn an_html_error_page_is_previewed_on_one_line() {
        let page = format!(
            "<html>\n<body>\n{}\n</body>\n</html>",
            "no healthy upstream ".repeat(20)
        );
        let response = format!(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{page}",
            page.len()
        );

        let message = failure(response.leak()).await.to_string();
        let said = message.split_once("Server said: ").unwrap().1;

        assert!(!said.contains('\n'), "{said}");
        assert!(said.len() <= 120, "{said}");
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

        assert!(matches!(error, TransportError::NotSent { .. }));
        assert!(!error.reached_server());
    }

    #[test]
    fn encode_segment_escapes_routing_characters() {
        assert_eq!(encode_segment("r/1?x#y z"), "r%2F1%3Fx%23y%20z");
        assert_eq!(encode_segment("AAPL-1.0_~"), "AAPL-1.0_~");
    }
}
