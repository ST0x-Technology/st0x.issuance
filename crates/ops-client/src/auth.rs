//! Obtains the S01 Google ID token the transport sends to IAP: either the one
//! CI minted through workload identity, or one from the interactive Desktop
//! OAuth flow (loopback + PKCE) whose refresh token is cached for silent reuse.

use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use url::Url;

use crate::target::{Env, Identity};
use crate::transport::error_chain;

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
    #[error("Google's token endpoint returned {status}: {body}")]
    TokenEndpoint { status: reqwest::StatusCode, body: String },
    #[error("Google's token response carried no id_token")]
    MissingIdToken,
    #[error("the sign-in was denied: {reason}")]
    Denied { reason: String },
    #[error(
        "the sign-in redirect state did not match; ignoring a possible forgery"
    )]
    StateMismatch,
    #[error("the sign-in redirect carried no authorization code")]
    MissingCode,
}

/// Google OAuth 2.0 endpoints for the installed-application (Desktop) flow.
const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

/// Bounds each exchange with Google so a stalled endpoint cannot hang the
/// sign-in; the operator's browser step itself is not bounded.
const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TOKEN_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounds how long one loopback connection may take to send its request line,
/// so a connection that never sends one (a browser preconnect) cannot stall
/// the sign-in.
const REDIRECT_READ_TIMEOUT: Duration = Duration::from_secs(10);

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

    if let Some(refresh_token) = load_refresh_token(env) {
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
            Err(error) => {
                eprintln!(
                    "The cached S01 sign-in could not be refreshed ({error}); \
                     signing in again."
                );
            }
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

    let code =
        tokio::task::spawn_blocking(move || capture_code(&listener, &state))
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
        store_refresh_token(env, refresh_token);
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
        return Err(AuthError::TokenEndpoint { status, body });
    }

    Ok(serde_json::from_str(&body)?)
}

/// Pulls the `id_token` out of a token response; this JWT is what IAP checks.
fn extract_id_token(token: &serde_json::Value) -> Result<String, AuthError> {
    token
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or(AuthError::MissingIdToken)
}

/// Waits for the loopback redirect, returns the authorization code, and serves
/// a small page telling the operator the sign-in is done. Connections carrying
/// no OAuth redirect parameters (a browser preconnect or favicon fetch, another
/// local process) are dropped and the wait continues. Blocking, so it runs on a
/// blocking task off the async runtime.
fn capture_code(
    listener: &std::net::TcpListener,
    expected_state: &str,
) -> Result<String, AuthError> {
    let (mut stream, params) = loop {
        let (stream, _) = listener.accept()?;

        if let Some(params) = redirect_params(&stream)? {
            break (stream, params);
        }
    };

    let page = "<html><body>Sign-in complete. You can close this tab and \
                return to the terminal.</body></html>";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: \
         {}\r\nConnection: close\r\n\r\n{page}",
        page.len()
    );
    std::io::Write::write_all(&mut stream, response.as_bytes())?;

    if let Some(reason) = params.get("error") {
        return Err(AuthError::Denied { reason: reason.clone() });
    }

    if params.get("state").map(String::as_str) != Some(expected_state) {
        return Err(AuthError::StateMismatch);
    }

    params.get("code").cloned().ok_or(AuthError::MissingCode)
}

/// The query parameters of the request on `stream`, or `None` when it is not
/// the OAuth redirect: no request line arrived in time, or it carried none of
/// `code`, `state`, or `error`.
fn redirect_params(
    stream: &std::net::TcpStream,
) -> Result<Option<HashMap<String, String>>, AuthError> {
    stream.set_read_timeout(Some(REDIRECT_READ_TIMEOUT))?;
    let mut reader = std::io::BufReader::new(stream);
    let mut request_line = String::new();

    // A timeout or reset belongs to this connection only, never the sign-in.
    if std::io::BufRead::read_line(&mut reader, &mut request_line).is_err() {
        return Ok(None);
    }

    let query = request_line
        .split_whitespace()
        .nth(1)
        .and_then(|target| target.split_once('?'))
        .map_or("", |(_, query)| query);
    let params: HashMap<String, String> =
        url::form_urlencoded::parse(query.as_bytes()).into_owned().collect();

    let is_redirect =
        ["code", "state", "error"].iter().any(|key| params.contains_key(*key));

    Ok(is_redirect.then_some(params))
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
/// is separate from the T0 client's, keyed by environment.
fn refresh_token_path(env: Env) -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from).or_else(
        || {
            std::env::var_os("HOME")
                .map(|home| PathBuf::from(home).join(".config"))
        },
    )?;

    Some(
        base.join("st0x-issuance-client")
            .join(format!("oauth-{}.json", env.cache_slug())),
    )
}

fn load_refresh_token(env: Env) -> Option<String> {
    load_refresh_token_at(&refresh_token_path(env)?)
}

fn load_refresh_token_at(path: &Path) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&contents).ok()?;

    parsed
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// Best effort: a cache write failure must not fail the command, only cost the
/// next run a sign-in, so it is reported rather than propagated.
fn store_refresh_token(env: Env, refresh_token: &str) {
    if let Some(path) = refresh_token_path(env)
        && let Err(error) = store_refresh_token_at(&path, refresh_token)
    {
        eprintln!(
            "Could not cache the S01 sign-in at {} ({error}); the next run \
             will ask you to sign in again.",
            path.display()
        );
    }
}

/// Writes the refresh token to `path`, creating parent directories and
/// pinning owner-only (0600) permissions on both new and existing files.
fn store_refresh_token_at(
    path: &Path,
    refresh_token: &str,
) -> std::io::Result<()> {
    if let Some(directory) = path.parent() {
        std::fs::create_dir_all(directory)?;
    }

    let body =
        serde_json::json!({ "refresh_token": refresh_token }).to_string();

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        // Created owner-only from the outset so the token is never briefly
        // world-readable; the explicit chmod also covers a pre-existing file,
        // whose mode `open` keeps.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(body.as_bytes())
    }

    #[cfg(not(unix))]
    {
        std::fs::write(path, body)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    use super::{
        AuthError, capture_code, extract_id_token, load_refresh_token_at,
        refresh_id_token, store_refresh_token_at,
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
    fn the_refresh_token_cache_is_owner_only_and_roundtrips() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested").join("oauth.json");
        let mode = |path: &std::path::Path| {
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777
        };

        store_refresh_token_at(&path, "rtok-1").unwrap();
        assert_eq!(mode(&path), 0o600);
        assert_eq!(load_refresh_token_at(&path).unwrap(), "rtok-1");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .unwrap();
        store_refresh_token_at(&path, "rtok-2").unwrap();
        assert_eq!(mode(&path), 0o600, "a loose existing file is locked down");
        assert_eq!(load_refresh_token_at(&path).unwrap(), "rtok-2");
    }

    /// Drives `capture_code` against one loopback redirect carrying `query`.
    fn capture(
        query: &'static str,
        expected_state: &str,
    ) -> Result<String, AuthError> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let sender = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            let request =
                format!("GET /?{query} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
            stream.write_all(request.as_bytes()).unwrap();
            let mut sink = Vec::new();
            stream.read_to_end(&mut sink).unwrap();
        });

        let result = capture_code(&listener, expected_state);
        sender.join().unwrap();
        result
    }

    #[test]
    fn capture_code_returns_the_authorization_code() {
        assert_eq!(capture("code=abc&state=xyz", "xyz").unwrap(), "abc");
    }

    #[test]
    fn capture_code_rejects_a_mismatched_state() {
        assert!(matches!(
            capture("code=abc&state=wrong", "xyz"),
            Err(AuthError::StateMismatch)
        ));
    }

    #[test]
    fn capture_code_rejects_a_missing_code() {
        assert!(matches!(
            capture("state=xyz", "xyz"),
            Err(AuthError::MissingCode)
        ));
    }

    #[test]
    fn capture_code_surfaces_a_denial() {
        assert!(matches!(
            capture("error=access_denied&state=xyz", "xyz"),
            Err(AuthError::Denied { reason }) if reason == "access_denied"
        ));
    }

    #[test]
    fn capture_code_skips_connections_that_are_not_the_redirect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let sender = std::thread::spawn(move || {
            let mut favicon = TcpStream::connect(("127.0.0.1", port)).unwrap();
            favicon.write_all(b"GET /favicon.ico HTTP/1.1\r\n\r\n").unwrap();
            drop(favicon);
            drop(TcpStream::connect(("127.0.0.1", port)).unwrap());

            let mut redirect = TcpStream::connect(("127.0.0.1", port)).unwrap();
            redirect
                .write_all(b"GET /?code=abc&state=xyz HTTP/1.1\r\n\r\n")
                .unwrap();
            let mut sink = Vec::new();
            redirect.read_to_end(&mut sink).unwrap();
        });

        assert_eq!(capture_code(&listener, "xyz").unwrap(), "abc");
        sender.join().unwrap();
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

    #[tokio::test]
    async fn refresh_fails_on_a_rejected_token_request() {
        let endpoint = token_server(
            "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\ninvalid_grant",
        );

        let result = refresh_id_token(
            &http(Duration::from_secs(5)),
            &endpoint,
            "cid",
            "secret",
            "rtok",
        )
        .await;

        assert!(matches!(
            result,
            Err(AuthError::TokenEndpoint { status, body })
                if status == reqwest::StatusCode::BAD_REQUEST
                    && body == "invalid_grant"
        ));
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
