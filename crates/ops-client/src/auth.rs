//! Obtains the S01 Google ID token the transport sends to IAP: either the one
//! CI minted through workload identity, or one from the interactive Desktop
//! OAuth flow (loopback + PKCE) whose refresh token is cached for silent reuse.

use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use url::Url;

use crate::target::{Env, Identity};
use crate::transport::{error_chain, server_said};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthError {
    #[error("sign-in I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("the sign-in listener task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("the request to Google failed: {}", error_chain(.0))]
    Http(#[from] reqwest::Error),
    #[error("could not decode Google's token response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid Google authorization endpoint: {0}")]
    Url(#[from] url::ParseError),
    #[error("Google's token endpoint returned {status}.{}", server_said(.body))]
    TokenEndpoint {
        status: reqwest::StatusCode,
        /// The response's OAuth 2.0 `error` code (RFC 6749 section 5.2), when
        /// it carried one.
        code: Option<String>,
        body: String,
    },
    #[error("Google's token response carried no id_token")]
    MissingIdToken,
    /// The sign-in redirect's OAuth 2.0 `error` code (RFC 6749 section
    /// 4.1.2.1).
    #[error("the sign-in did not complete: Google reported {code}")]
    Authorization { code: String },
    #[error("the sign-in redirect carried no authorization code")]
    MissingCode,
    /// Google does not redirect back for a broken client configuration (RFC
    /// 6749 section 4.1.2.1: an invalid client id or redirect URI); it shows
    /// the error in the browser instead, so the wait for the redirect is bounded.
    #[error(
        "the browser sign-in did not finish within {} minutes. If the browser \
         showed a Google error (for example deleted_client or \
         redirect_uri_mismatch), check S01_ISSUANCE_*_CLIENT_ID and the \
         Desktop OAuth client; otherwise re-run and complete the sign-in.",
        SIGN_IN_TIMEOUT.as_secs() / 60
    )]
    SignInTimedOut,
}

impl AuthError {
    /// Google rejected the grant itself (`invalid_grant`: a revoked or expired
    /// refresh token, or a bad authorization code). Only this calls for a new
    /// browser sign-in; a rate limit, an outage, or a misconfigured client
    /// does not.
    pub(crate) fn is_rejected_grant(&self) -> bool {
        matches!(
            self,
            Self::TokenEndpoint { code: Some(code), .. } if code == "invalid_grant"
        )
    }

    /// A refusal of the operator's identity: the operator declined at the
    /// consent screen (`access_denied`), or Google rejected the grant.
    pub(crate) fn is_access_denied(&self) -> bool {
        match self {
            Self::Authorization { code } => code == "access_denied",
            Self::TokenEndpoint { .. } => self.is_rejected_grant(),
            Self::Io(_)
            | Self::Join(_)
            | Self::Http(_)
            | Self::Json(_)
            | Self::Url(_)
            | Self::MissingIdToken
            | Self::MissingCode
            | Self::SignInTimedOut => false,
        }
    }

    /// Google rejected the Desktop OAuth client or the request it made (RFC
    /// 6749 sections 4.1.2.1 and 5.2), or never redirected back, which is how
    /// it reports a client id or redirect URI it cannot accept: a setup error
    /// no sign-in can repair.
    pub(crate) fn is_client_misconfigured(&self) -> bool {
        let code = match self {
            Self::SignInTimedOut => return true,
            Self::Authorization { code } => Some(code.as_str()),
            Self::TokenEndpoint { code, .. } => code.as_deref(),
            _ => None,
        };

        matches!(
            code,
            Some(
                "invalid_request"
                    | "invalid_client"
                    | "unauthorized_client"
                    | "unsupported_response_type"
                    | "unsupported_grant_type"
                    | "invalid_scope"
            )
        )
    }
}

/// Google OAuth 2.0 endpoints for the installed-application (Desktop) flow.
const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

/// Bounds each exchange with Google so a stalled endpoint cannot hang the
/// sign-in.
const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TOKEN_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounds the operator's browser step: long enough to sign in, short enough
/// that a client configuration Google refuses to redirect for fails rather
/// than hangs.
const SIGN_IN_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How often the loopback listener checks for a connection and the deadline.
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Bounds how long one loopback connection may take to send its whole request
/// head, so a connection that never sends one (a browser preconnect) or drips
/// it cannot stall the sign-in.
const REDIRECT_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The longest loopback request head accepted; a browser's redirect, headers
/// included, is far shorter, so a longer one is not the redirect.
const MAX_REQUEST_HEAD: usize = 16 * 1024;

/// Returns the ID token for `identity`. Workload identity already holds one;
/// the Desktop client silently refreshes a cached sign-in, or signs the
/// operator in through the browser once and caches the result.
pub(crate) async fn id_token(
    env: Env,
    identity: Identity,
) -> Result<String, AuthError> {
    match identity {
        Identity::WorkloadIdentity { id_token } => Ok(id_token),
        Identity::DesktopOauth { client_id, client_secret } => {
            desktop_id_token(env, &client_id, &client_secret).await
        }
    }
}

async fn desktop_id_token(
    env: Env,
    client_id: &str,
    client_secret: &str,
) -> Result<String, AuthError> {
    let http = reqwest::Client::builder()
        .timeout(TOKEN_REQUEST_TIMEOUT)
        .connect_timeout(TOKEN_CONNECT_TIMEOUT)
        .build()?;

    if let Some(refresh_token) = load_refresh_token(env, client_id) {
        match refresh_id_token(
            &http,
            TOKEN_ENDPOINT,
            client_id,
            client_secret,
            &refresh_token,
        )
        .await
        {
            Ok(id_token) => return Ok(id_token),
            // Only a rejected grant (`invalid_grant`: the refresh token was
            // revoked or expired) needs a new browser sign-in. Anything else
            // (a network failure, a rate limit, a 5xx, a bad client secret) is
            // reported instead: a sign-in would not fix it, so the operator is
            // not sent to the browser for it.
            Err(error) if error.is_rejected_grant() => {
                eprintln!(
                    "The cached S01 sign-in was rejected ({error}); signing in \
                     again."
                );
            }
            Err(error) => return Err(error),
        }
    }

    interactive_id_token(&http, env, client_id, client_secret).await
}

/// Runs the browser loopback + PKCE authorization once, exchanges the returned
/// code for an ID token, and caches the refresh token for silent reuse.
async fn interactive_id_token(
    http: &reqwest::Client,
    env: Env,
    client_id: &str,
    client_secret: &str,
) -> Result<String, AuthError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://127.0.0.1:{port}");
    let verifier = random_token(32);
    let challenge = code_challenge(&verifier);
    let state = random_token(16);

    let mut auth_url = Url::parse(AUTH_ENDPOINT)?;
    auth_url
        .query_pairs_mut()
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", "openid email")
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state)
        .append_pair("access_type", "offline")
        .append_pair("prompt", "consent");

    eprintln!(
        "Open this URL in your browser to sign in with your S01 Google \
         account:\n\n{auth_url}\n"
    );

    let code = tokio::task::spawn_blocking(move || {
        capture_code(&listener, &state, SIGN_IN_TIMEOUT)
    })
    .await??;

    let token = post_token(
        http,
        TOKEN_ENDPOINT,
        &[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("code_verifier", &verifier),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", &redirect_uri),
        ],
    )
    .await?;

    if let Some(refresh_token) =
        token.get("refresh_token").and_then(serde_json::Value::as_str)
    {
        store_refresh_token(env, client_id, refresh_token);
    }

    extract_id_token(&token)
}

/// Exchanges a cached refresh token for a fresh ID token, no browser needed.
async fn refresh_id_token(
    http: &reqwest::Client,
    endpoint: &str,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, AuthError> {
    let token = post_token(
        http,
        endpoint,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ],
    )
    .await?;

    extract_id_token(&token)
}

async fn post_token(
    http: &reqwest::Client,
    endpoint: &str,
    form: &[(&str, &str)],
) -> Result<serde_json::Value, AuthError> {
    let response = http.post(endpoint).form(form).send().await?;
    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        let code = serde_json::from_str::<TokenErrorBody>(&body)
            .ok()
            .map(|parsed| parsed.error);
        return Err(AuthError::TokenEndpoint { status, code, body });
    }

    Ok(serde_json::from_str(&body)?)
}

/// An OAuth 2.0 token error response (RFC 6749 section 5.2); only its `error`
/// code decides how a failure is handled.
#[derive(Deserialize)]
struct TokenErrorBody {
    error: String,
}

/// Pulls the `id_token` out of a token response; this JWT is what IAP checks.
/// Google returns it from both the authorization-code exchange and a
/// refresh-token exchange made with the Desktop client id and secret
/// (<https://cloud.google.com/iap/docs/authentication-howto>, "Authenticate a
/// user account" for a desktop app, "Refresh token").
fn extract_id_token(token: &serde_json::Value) -> Result<String, AuthError> {
    token
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or(AuthError::MissingIdToken)
}

/// Waits up to `timeout` for the loopback redirect, returns the authorization
/// code, and serves a small page telling the operator how the sign-in ended.
/// Only a request carrying this sign-in's own `state` counts (RFC 6749 section
/// 10.12): any other connection (a browser preconnect or favicon fetch, another
/// local process) is dropped and the wait continues. Blocking, so it runs on a
/// blocking task off the async runtime.
fn capture_code(
    listener: &std::net::TcpListener,
    expected_state: &str,
    timeout: Duration,
) -> Result<String, AuthError> {
    let deadline =
        Instant::now().checked_add(timeout).ok_or(AuthError::SignInTimedOut)?;
    listener.set_nonblocking(true)?;

    let (mut stream, params) = loop {
        if Instant::now() >= deadline {
            return Err(AuthError::SignInTimedOut);
        }

        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(ACCEPT_POLL_INTERVAL);
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        // An accepted socket inherits the listener's non-blocking mode on some
        // platforms (BSD, macOS), and the request read relies on blocking
        // reads with a timeout.
        stream.set_nonblocking(false)?;

        if let Some(params) = redirect_params(&stream, deadline)
            && params.get("state").map(String::as_str) == Some(expected_state)
        {
            break (stream, params);
        }
    };

    let completed =
        !params.contains_key("error") && params.contains_key("code");
    let page = if completed {
        "<html><body>Sign-in complete. You can close this tab and return to \
         the terminal.</body></html>"
    } else {
        "<html><body>Sign-in did not complete. Return to the terminal for the \
         reason.</body></html>"
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: \
         {}\r\nConnection: close\r\n\r\n{page}",
        page.len()
    );
    // The page is a courtesy: `params` already decides the outcome, so a tab
    // the operator closed early must not discard a captured code.
    let _ = std::io::Write::write_all(&mut stream, response.as_bytes());

    if let Some(code) = params.get("error") {
        return Err(AuthError::Authorization { code: code.clone() });
    }

    params.get("code").cloned().ok_or(AuthError::MissingCode)
}

/// The query parameters of the request on `stream`, or `None` when no request
/// line arrived before the connection's read deadline.
fn redirect_params(
    stream: &std::net::TcpStream,
    sign_in_deadline: Instant,
) -> Option<HashMap<String, String>> {
    let request_line = request_line(stream, sign_in_deadline)?;
    let query = request_line
        .split_whitespace()
        .nth(1)
        .and_then(|target| target.split_once('?'))
        .map_or("", |(_, query)| query);

    Some(url::form_urlencoded::parse(query.as_bytes()).into_owned().collect())
}

/// Reads the request head and returns its first line. The read stops at the
/// blank line ending the headers, at `MAX_REQUEST_HEAD` bytes, or at the
/// earlier of `REDIRECT_READ_TIMEOUT` and `sign_in_deadline`, so a connection
/// that stalls, drips, or floods cannot hold up the sign-in. Draining the head
/// lets the socket close with a FIN after the response rather than a reset for
/// unread data, so the browser shows the page; it is best effort, so a first
/// line already read is still returned when a bound stops the drain. `None`
/// when no complete first line arrived: the failure belongs to that
/// connection, never the sign-in.
fn request_line(
    mut stream: &std::net::TcpStream,
    sign_in_deadline: Instant,
) -> Option<String> {
    let deadline = Instant::now()
        .checked_add(REDIRECT_READ_TIMEOUT)?
        .min(sign_in_deadline);
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
        else {
            return first_line(&head);
        };

        if stream.set_read_timeout(Some(remaining)).is_err() {
            return first_line(&head);
        }

        let count = match stream.read(&mut chunk) {
            Ok(count) if count > 0 => count,
            _ => return first_line(&head),
        };
        head.extend_from_slice(chunk.get(..count)?);

        if head.windows(4).any(|window| window == b"\r\n\r\n")
            || head.len() > MAX_REQUEST_HEAD
        {
            return first_line(&head);
        }
    }
}

/// The bytes before the first `\n`, when one has arrived.
fn first_line(head: &[u8]) -> Option<String> {
    let end = head.iter().position(|byte| *byte == b'\n')?;

    String::from_utf8(head.get(..end)?.to_vec()).ok()
}

/// A URL-safe, unpadded base64 string of `bytes` random bytes, for the PKCE
/// verifier and the CSRF state.
fn random_token(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    rand::fill(buffer.as_mut_slice());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&buffer)
}

/// The S256 PKCE challenge for a verifier.
fn code_challenge(verifier: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(verifier.as_bytes()))
}

/// Path of the cached refresh token for one environment: under
/// `$XDG_CONFIG_HOME` (or `~/.config`) in this client's own directory, which
/// is separate from the T0 client's, keyed by environment. Per the XDG Base
/// Directory spec an empty or relative `$XDG_CONFIG_HOME` is ignored, so the
/// token never lands relative to the working directory.
fn refresh_token_path(env: Env) -> Option<PathBuf> {
    let base = absolute_dir("XDG_CONFIG_HOME")
        .or_else(|| absolute_dir("HOME").map(|home| home.join(".config")))?;

    Some(
        base.join("st0x-issuance-client")
            .join(format!("oauth-{}.json", env.cache_slug())),
    )
}

fn absolute_dir(variable: &str) -> Option<PathBuf> {
    std::env::var_os(variable)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

/// The cached sign-in: the refresh token and the Desktop client id it was
/// issued to, since Google refuses a refresh token from any other client.
#[derive(Serialize, Deserialize)]
struct CachedSignIn {
    client_id: String,
    refresh_token: String,
}

fn load_refresh_token(env: Env, client_id: &str) -> Option<String> {
    load_refresh_token_at(&refresh_token_path(env)?, client_id)
}

/// The cached refresh token, if one was issued to `client_id`; a cache from a
/// replaced client is ignored, so the operator signs in again. The cache is
/// locked down before it is read.
fn load_refresh_token_at(path: &Path, client_id: &str) -> Option<String> {
    lock_down(path).ok()?;
    let contents = std::fs::read_to_string(path).ok()?;
    let cached: CachedSignIn = serde_json::from_str(&contents).ok()?;

    (cached.client_id == client_id).then_some(cached.refresh_token)
}

/// Best effort: a cache write failure must not fail the command, only cost the
/// next run a sign-in, so it is reported rather than propagated.
fn store_refresh_token(env: Env, client_id: &str, refresh_token: &str) {
    if let Some(path) = refresh_token_path(env)
        && let Err(error) =
            store_refresh_token_at(&path, client_id, refresh_token)
    {
        eprintln!(
            "Could not cache the S01 sign-in at {} ({error}); the next run \
             will ask you to sign in again.",
            path.display()
        );
    }
}

/// Writes the cached sign-in to `path` in an owner-only (0700) directory and
/// an owner-only (0600) file, both pinned before the token is written.
#[cfg(unix)]
fn store_refresh_token_at(
    path: &Path,
    client_id: &str,
    refresh_token: &str,
) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

    if let Some(directory) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
    }

    let body = serde_json::to_string(&CachedSignIn {
        client_id: client_id.to_owned(),
        refresh_token: refresh_token.to_owned(),
    })?;

    // Created owner-only from the outset so the token is never briefly
    // world-readable.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    lock_down(path)?;
    file.write_all(body.as_bytes())
}

/// Without unix permission modes the cache cannot be made owner-only, so the
/// long-lived refresh token is never written and each run signs in.
#[cfg(not(unix))]
fn store_refresh_token_at(
    path: &Path,
    _client_id: &str,
    _refresh_token: &str,
) -> std::io::Result<()> {
    lock_down(path)
}

/// Pins this client's cache directory to 0700 and the cache file to 0600:
/// `mode` covers only what a call creates, so a directory or file left looser
/// (by hand, or by an older build) is tightened before a token is read from or
/// written to it.
#[cfg(unix)]
fn lock_down(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if let Some(directory) = path.parent() {
        std::fs::set_permissions(
            directory,
            std::fs::Permissions::from_mode(0o700),
        )?;
    }

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn lock_down(_path: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "owner-only file permissions are only enforced on unix",
    ))
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};

    use super::{
        AuthError, REDIRECT_READ_TIMEOUT, capture_code, extract_id_token,
        load_refresh_token_at, refresh_id_token, store_refresh_token_at,
    };

    #[test]
    fn extract_id_token_reads_the_jwt() {
        let token = serde_json::json!({ "id_token": "jwt-value" });

        assert_eq!(extract_id_token(&token).unwrap(), "jwt-value");
    }

    #[test]
    fn extract_id_token_rejects_a_missing_jwt() {
        let token = serde_json::json!({ "access_token": "no-id-here" });

        assert!(matches!(
            extract_id_token(&token),
            Err(AuthError::MissingIdToken)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn the_sign_in_cache_is_owner_only_and_keyed_to_its_client() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested").join("oauth.json");
        let cache_dir = path.parent().unwrap().to_owned();
        let mode = |path: &std::path::Path| {
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777
        };
        let loosen = || {
            std::fs::set_permissions(
                &path,
                std::fs::Permissions::from_mode(0o644),
            )
            .unwrap();
            std::fs::set_permissions(
                &cache_dir,
                std::fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        };

        store_refresh_token_at(&path, "cid-1", "rtok-1").unwrap();
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(&cache_dir), 0o700);
        assert_eq!(load_refresh_token_at(&path, "cid-1").unwrap(), "rtok-1");
        assert_eq!(
            load_refresh_token_at(&path, "cid-2"),
            None,
            "a replaced client signs in again"
        );

        loosen();
        assert_eq!(load_refresh_token_at(&path, "cid-1").unwrap(), "rtok-1");
        assert_eq!(mode(&path), 0o600, "locked down before it is read");
        assert_eq!(mode(&cache_dir), 0o700, "locked down before it is read");

        loosen();
        store_refresh_token_at(&path, "cid-1", "rtok-2").unwrap();
        assert_eq!(mode(&path), 0o600, "locked down before it is written");
        assert_eq!(mode(&cache_dir), 0o700, "locked down before it is written");
        assert_eq!(load_refresh_token_at(&path, "cid-1").unwrap(), "rtok-2");
    }

    /// One loopback connection that is not this sign-in's redirect.
    enum Stray {
        Send(Vec<u8>),
        /// A request line that never ends, sent until the listener drops it.
        Flood,
    }

    /// Drives `capture_code` (expecting state `xyz`) through `strays`, each its
    /// own loopback connection, then the redirect carrying `query`. Returns the
    /// result and the raw response served to the redirect.
    fn capture_after(
        strays: Vec<Stray>,
        query: &'static str,
    ) -> (Result<String, AuthError>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let sender = std::thread::spawn(move || {
            for stray in strays {
                let mut stream =
                    TcpStream::connect(("127.0.0.1", port)).unwrap();
                match stray {
                    Stray::Send(bytes) => stream.write_all(&bytes).unwrap(),
                    Stray::Flood => {
                        let chunk = [b'a'; 1024];
                        while stream.write_all(&chunk).is_ok() {}
                    }
                }
            }

            // Browser-sized: a real redirect carries ~1-3 KiB of headers, and
            // closing on unread bytes would reset the page read below.
            let mut redirect = TcpStream::connect(("127.0.0.1", port)).unwrap();
            let request = format!(
                "GET /?{query} HTTP/1.1\r\nHost: 127.0.0.1\r\nUser-Agent: \
                 {}\r\n\r\n",
                "a".repeat(3 * 1024)
            );
            redirect.write_all(request.as_bytes()).unwrap();
            let mut page = String::new();
            redirect.read_to_string(&mut page).unwrap();
            page
        });

        let result = capture_code(&listener, "xyz", Duration::from_secs(60));
        (result, sender.join().unwrap())
    }

    fn capture(query: &'static str) -> (Result<String, AuthError>, String) {
        capture_after(Vec::new(), query)
    }

    #[test]
    fn capture_code_returns_the_authorization_code() {
        let (result, page) = capture("code=abc&state=xyz");

        assert_eq!(result.unwrap(), "abc");
        assert!(page.contains("Sign-in complete"), "{page}");
    }

    #[test]
    fn capture_code_rejects_a_missing_code() {
        let (result, page) = capture("state=xyz");

        assert!(matches!(result, Err(AuthError::MissingCode)));
        assert!(page.contains("did not complete"), "{page}");
    }

    #[test]
    fn capture_code_surfaces_the_redirect_error() {
        let (result, page) = capture("error=access_denied&state=xyz");

        assert!(matches!(
            result,
            Err(AuthError::Authorization { code }) if code == "access_denied"
        ));
        assert!(page.contains("did not complete"), "{page}");
    }

    #[test]
    fn capture_code_waits_past_everything_but_its_own_redirect() {
        let strays = vec![
            Stray::Send(b"GET /favicon.ico HTTP/1.1\r\n\r\n".to_vec()),
            Stray::Send(Vec::new()),
            Stray::Flood,
            Stray::Send(b"GET /?error=access_denied HTTP/1.1\r\n\r\n".to_vec()),
            Stray::Send(
                b"GET /?code=forged&state=wrong HTTP/1.1\r\n\r\n".to_vec(),
            ),
        ];
        let started = Instant::now();

        let (result, _) = capture_after(strays, "code=abc&state=xyz");

        assert_eq!(result.unwrap(), "abc");
        assert!(
            started.elapsed() < REDIRECT_READ_TIMEOUT / 2,
            "the flood is cut off by size, not by the read deadline"
        );
    }

    #[test]
    fn capture_code_gives_up_when_no_redirect_arrives() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let started = Instant::now();

        let result = capture_code(&listener, "xyz", Duration::from_millis(300));

        assert!(matches!(result, Err(AuthError::SignInTimedOut)));
        assert!(result.unwrap_err().is_client_misconfigured());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn capture_code_keeps_its_deadline_while_a_connection_stalls() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _silent =
            TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let started = Instant::now();

        let result = capture_code(&listener, "xyz", Duration::from_millis(300));

        assert!(matches!(result, Err(AuthError::SignInTimedOut)));
        assert!(
            started.elapsed() < REDIRECT_READ_TIMEOUT / 2,
            "the stalled read is cut off by the sign-in deadline"
        );
    }

    #[test]
    fn capture_code_keeps_a_redirect_whose_head_exceeds_the_cap() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let sender = std::thread::spawn(move || {
            let mut redirect = TcpStream::connect(address).unwrap();
            let request = format!(
                "GET /?code=abc&state=xyz HTTP/1.1\r\nCookie: {}\r\n\r\n",
                "a".repeat(20 * 1024)
            );
            // The head is cut off at the cap, so the write and the page read
            // may meet a reset; only the captured code matters here.
            let _ = redirect.write_all(request.as_bytes());
            let _ = redirect.read_to_end(&mut Vec::new());
        });

        let result = capture_code(&listener, "xyz", Duration::from_secs(3));
        sender.join().unwrap();

        assert_eq!(result.unwrap(), "abc");
    }

    /// Serves one token-endpoint response over loopback and returns its URL.
    fn token_server(response: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 2048];
            let _ = stream.read(&mut buffer).unwrap();
            stream.write_all(response.as_bytes()).unwrap();
        });
        format!("http://127.0.0.1:{port}/token")
    }

    fn http(timeout: Duration) -> reqwest::Client {
        reqwest::Client::builder().timeout(timeout).build().unwrap()
    }

    #[tokio::test]
    async fn refresh_decodes_a_fresh_id_token() {
        let endpoint = token_server(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
             Connection: close\r\n\r\n{\"id_token\":\"fresh\"}",
        );

        let token = refresh_id_token(
            &http(Duration::from_secs(5)),
            &endpoint,
            "cid",
            "secret",
            "rtok",
        )
        .await
        .unwrap();

        assert_eq!(token, "fresh");
    }

    async fn refresh_failure(response: &'static str) -> AuthError {
        refresh_id_token(
            &http(Duration::from_secs(5)),
            &token_server(response),
            "cid",
            "secret",
            "rtok",
        )
        .await
        .unwrap_err()
    }

    #[tokio::test]
    async fn only_an_invalid_grant_calls_for_a_new_sign_in() {
        let revoked = refresh_failure(
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n\
             Connection: close\r\n\r\n{\"error\":\"invalid_grant\",\
             \"error_description\":\"Token has been expired or revoked.\"}",
        )
        .await;
        assert!(matches!(
            &revoked,
            AuthError::TokenEndpoint { status, .. } if *status == StatusCode::BAD_REQUEST
        ));
        assert!(revoked.is_rejected_grant());
        assert!(revoked.is_access_denied());

        let throttled = refresh_failure(
            "HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n\
             Rate Limit Exceeded",
        )
        .await;
        assert!(!throttled.is_rejected_grant(), "a rate limit is transient");
        assert!(!throttled.is_access_denied());

        let bad_secret = refresh_failure(
            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\n\
             Connection: close\r\n\r\n{\"error\":\"invalid_client\"}",
        )
        .await;
        assert!(!bad_secret.is_rejected_grant(), "a sign-in cannot fix it");
        assert!(bad_secret.is_client_misconfigured());
    }

    #[tokio::test]
    async fn refresh_times_out_on_a_stalled_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let _accepted = listener.accept();
            std::thread::sleep(Duration::from_secs(2));
        });

        let result = refresh_id_token(
            &http(Duration::from_millis(150)),
            &format!("http://127.0.0.1:{port}/token"),
            "cid",
            "secret",
            "rtok",
        )
        .await;

        assert!(matches!(result, Err(AuthError::Http(_))));
    }
}
