mod ip_whitelist;
mod rate_limit;

use clap::Args;
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::response::{Responder, Response};
use std::io::Cursor;
use std::net::IpAddr;
use std::str::FromStr;
use subtle::ConstantTimeEq;
use tracing::{info, warn};

use crate::config::Config;
pub use ip_whitelist::IpWhitelist;
pub(crate) use rate_limit::FailedAuthRateLimiter;

#[derive(Clone)]
pub struct IssuerApiKey(String);

impl std::fmt::Debug for IssuerApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("IssuerApiKey").field(&"[REDACTED]").finish()
    }
}

impl IssuerApiKey {
    const MIN_LENGTH: usize = 32;

    fn new(value: String) -> Result<Self, IssuerApiKeyError> {
        if value.len() < Self::MIN_LENGTH {
            return Err(IssuerApiKeyError::TooShort { len: value.len() });
        }

        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for IssuerApiKey {
    type Err = IssuerApiKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IssuerApiKeyError {
    #[error("Issuer API key must be at least {min} characters, got {len}", min = IssuerApiKey::MIN_LENGTH)]
    TooShort { len: usize },
}

/// Authentication configuration for API guards.
#[derive(Args, Clone)]
pub struct AuthConfig {
    #[arg(
        long,
        env = "ISSUER_API_KEY",
        help = "API key for authenticating inbound requests from Alpaca"
    )]
    pub issuer_api_key: IssuerApiKey,

    #[arg(
        long,
        env = "ALPACA_IP_RANGES",
        default_value = "",
        help = "Comma-separated list of IP ranges (CIDR notation) allowed to call issuer endpoints. Leave empty to disable IP filtering (not recommended for production)."
    )]
    pub alpaca_ip_ranges: IpWhitelist,

    #[arg(
        long,
        env = "INTERNAL_IP_RANGES",
        default_value = "127.0.0.0/8,::1/128,172.16.0.0/12",
        help = "Comma-separated CIDR ranges for internal endpoints. \
                Default includes localhost and Docker networks."
    )]
    pub internal_ip_ranges: IpWhitelist,
}

/// Guard for Alpaca issuer endpoints.
///
/// Validates API key and checks IP against alpaca_ip_ranges whitelist.
pub(crate) struct IssuerAuth;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for IssuerAuth {
    type Error = AuthError;

    async fn from_request(
        request: &'r Request<'_>,
    ) -> Outcome<Self, Self::Error> {
        match authenticate_request(request, |auth| &auth.alpaca_ip_ranges) {
            Ok(client_ip) => {
                info!(
                    ip = %client_ip,
                    endpoint = %request.uri(),
                    "Issuer authentication success"
                );
                Outcome::Success(Self)
            }
            Err((status, error)) => Outcome::Error((status, error)),
        }
    }
}

/// Guard for internal endpoints.
///
/// Validates API key and checks IP against internal_ip_ranges whitelist.
pub(crate) struct InternalAuth;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for InternalAuth {
    type Error = AuthError;

    async fn from_request(
        request: &'r Request<'_>,
    ) -> Outcome<Self, Self::Error> {
        match authenticate_request(request, |auth| &auth.internal_ip_ranges) {
            Ok(client_ip) => {
                info!(
                    ip = %client_ip,
                    endpoint = %request.uri(),
                    "Internal authentication success"
                );
                Outcome::Success(Self)
            }
            Err((status, error)) => Outcome::Error((status, error)),
        }
    }
}

fn authenticate_request(
    request: &Request<'_>,
    get_whitelist: impl FnOnce(&AuthConfig) -> &IpWhitelist,
) -> Result<IpAddr, (Status, AuthError)> {
    let config = get_config(request)?;
    let rate_limiter = get_rate_limiter(request)?;

    check_api_key(request, config, rate_limiter)?;

    let client_ip = get_client_ip(request)?;
    check_ip_whitelist(
        request,
        config,
        rate_limiter,
        client_ip,
        get_whitelist,
    )?;

    Ok(client_ip)
}

fn get_config<'a>(
    request: &'a Request<'_>,
) -> Result<&'a Config, (Status, AuthError)> {
    request.rocket().state::<Config>().ok_or_else(|| {
        warn!("Config not found in Rocket state");
        (Status::InternalServerError, AuthError::ConfigMissing)
    })
}

fn get_rate_limiter<'a>(
    request: &'a Request<'_>,
) -> Result<&'a FailedAuthRateLimiter, (Status, AuthError)> {
    request.rocket().state::<FailedAuthRateLimiter>().ok_or_else(|| {
        warn!("Rate limiter not found in Rocket state");
        (Status::InternalServerError, AuthError::ConfigMissing)
    })
}

fn check_api_key(
    request: &Request<'_>,
    config: &Config,
    rate_limiter: &FailedAuthRateLimiter,
) -> Result<(), (Status, AuthError)> {
    let Some(api_key) = request.headers().get_one("X-API-KEY") else {
        warn!(
            endpoint = %request.uri(),
            "Missing X-API-KEY header"
        );
        return Err((Status::Unauthorized, AuthError::MissingApiKey));
    };

    let expected_key = config.auth.issuer_api_key.as_str();
    if validate_api_key(api_key, expected_key) {
        return Ok(());
    }

    if let Some(client_ip) = extract_client_ip(request) {
        check_rate_limit(request, rate_limiter, &client_ip)?;
    }

    warn!(
        endpoint = %request.uri(),
        "Invalid API key"
    );
    Err((Status::Unauthorized, AuthError::InvalidApiKey))
}

fn get_client_ip(request: &Request<'_>) -> Result<IpAddr, (Status, AuthError)> {
    extract_client_ip(request).ok_or_else(|| {
        warn!(
            endpoint = %request.uri(),
            "Could not determine client IP"
        );
        (Status::BadRequest, AuthError::NoClientIp)
    })
}

fn check_ip_whitelist(
    request: &Request<'_>,
    config: &Config,
    rate_limiter: &FailedAuthRateLimiter,
    client_ip: IpAddr,
    get_whitelist: impl FnOnce(&AuthConfig) -> &IpWhitelist,
) -> Result<(), (Status, AuthError)> {
    let whitelist = get_whitelist(&config.auth);
    if whitelist.is_allowed(&client_ip) {
        return Ok(());
    }

    check_rate_limit(request, rate_limiter, &client_ip)?;

    warn!(
        ip = %client_ip,
        endpoint = %request.uri(),
        "IP not whitelisted"
    );
    Err((Status::Forbidden, AuthError::UnauthorizedIp))
}

fn check_rate_limit(
    request: &Request<'_>,
    rate_limiter: &FailedAuthRateLimiter,
    client_ip: &IpAddr,
) -> Result<(), (Status, AuthError)> {
    if rate_limiter.check(client_ip) {
        return Ok(());
    }

    warn!(
        ip = %client_ip,
        endpoint = %request.uri(),
        "Rate limit exceeded for failed authentication"
    );
    Err((Status::TooManyRequests, AuthError::RateLimited))
}

fn validate_api_key(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

fn extract_client_ip(request: &Request<'_>) -> Option<IpAddr> {
    request.client_ip()
}

#[derive(Debug)]
pub enum AuthError {
    MissingApiKey,
    InvalidApiKey,
    UnauthorizedIp,
    NoClientIp,
    ConfigMissing,
    RateLimited,
}

impl<'r> Responder<'r, 'static> for AuthError {
    fn respond_to(
        self,
        _: &'r Request<'_>,
    ) -> rocket::response::Result<'static> {
        let (status, message) = match self {
            Self::MissingApiKey => (Status::Unauthorized, "Missing API key"),
            Self::InvalidApiKey => (Status::Unauthorized, "Invalid API key"),
            Self::UnauthorizedIp => {
                (Status::Forbidden, "IP address not authorized")
            }
            Self::NoClientIp => {
                (Status::BadRequest, "Could not determine client IP")
            }
            Self::ConfigMissing => {
                (Status::InternalServerError, "Server configuration error")
            }
            Self::RateLimited => (
                Status::TooManyRequests,
                "Too many failed authentication attempts",
            ),
        };

        Response::build()
            .status(status)
            .sized_body(Some(message.len()), Cursor::new(message))
            .ok()
    }
}

/// Creates test authentication configuration for use in tests.
///
/// # Errors
///
/// Returns an error if parsing fails (should not happen with hardcoded values).
pub fn test_auth_config() -> Result<AuthConfig, TestAuthConfigError> {
    Ok(AuthConfig {
        issuer_api_key: "test-key-12345678901234567890123456".parse()?,
        alpaca_ip_ranges: IpWhitelist::single("127.0.0.1/32".parse()?),
        internal_ip_ranges: "127.0.0.0/8,::1/128".parse()?,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum TestAuthConfigError {
    #[error("Invalid API key: {0}")]
    ApiKey(#[from] IssuerApiKeyError),
    #[error("Invalid IP network: {0}")]
    IpNetwork(#[from] ipnetwork::IpNetworkError),
    #[error("Invalid IP whitelist: {0}")]
    IpWhitelist(#[from] ip_whitelist::IpWhitelistParseError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;
    use rocket::http::Header;
    use rocket::local::asynchronous::Client;
    use url::Url;

    use super::*;
    use crate::alpaca::service::AlpacaConfig;
    use crate::config::LogLevel;
    use crate::fireblocks::SignerConfig;

    #[rocket::get("/issuer-test")]
    fn issuer_endpoint(_auth: IssuerAuth) -> &'static str {
        "issuer authenticated"
    }

    #[rocket::get("/internal-test")]
    fn internal_endpoint(_auth: InternalAuth) -> &'static str {
        "internal authenticated"
    }

    fn test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: Url::parse("wss://localhost:8545").unwrap(),
            chain_id: crate::test_utils::ANVIL_CHAIN_ID,
            signer: SignerConfig::Local(B256::ZERO),
            backfill_start_block: 0,
            auth: test_auth_config().unwrap(),
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
        }
    }

    #[tokio::test]
    async fn test_missing_api_key_header_returns_401() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![issuer_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();
        let response = client.get("/issuer-test").dispatch().await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[tokio::test]
    async fn test_invalid_api_key_returns_401() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![issuer_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();
        let response = client
            .get("/issuer-test")
            .header(Header::new("X-API-KEY", "wrong-key"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[tokio::test]
    async fn test_constant_time_comparison() {
        let key1 = "12345678901234567890123456789012";
        let key2 = "12345678901234567890123456789013";
        let key3 = "12345678901234567890123456789012";

        assert!(!validate_api_key(key1, key2));
        assert!(validate_api_key(key1, key3));
    }

    #[tokio::test]
    async fn test_empty_ip_ranges_allows_authenticated_requests() {
        let mut config = test_config();
        config.auth.alpaca_ip_ranges = IpWhitelist::AllowAll;

        let rocket = rocket::build()
            .manage(config)
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![issuer_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();
        let response = client
            .get("/issuer-test")
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("8.8.8.8:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    #[tokio::test]
    async fn test_configured_ip_ranges_block_unauthorized_ips() {
        let mut config = test_config();
        config.auth.alpaca_ip_ranges =
            IpWhitelist::single("10.0.0.0/24".parse().unwrap());

        let rocket = rocket::build()
            .manage(config)
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![issuer_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();
        let response = client
            .get("/issuer-test")
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("8.8.8.8:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Forbidden);
    }

    #[tokio::test]
    async fn test_rate_limit_11th_failed_attempt_returns_429() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![issuer_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();

        for i in 0..10 {
            let response = client
                .get("/issuer-test")
                .header(Header::new("X-API-KEY", "wrong-key"))
                .remote("127.0.0.1:8000".parse().unwrap())
                .dispatch()
                .await;

            assert_eq!(
                response.status(),
                Status::Unauthorized,
                "Request {} should return 401",
                i + 1
            );
        }

        let response = client
            .get("/issuer-test")
            .header(Header::new("X-API-KEY", "wrong-key"))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::TooManyRequests);
    }

    #[tokio::test]
    async fn test_successful_auth_does_not_count_against_limit() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![issuer_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();

        for _ in 0..15 {
            let response = client
                .get("/issuer-test")
                .header(Header::new(
                    "X-API-KEY",
                    "test-key-12345678901234567890123456",
                ))
                .remote("127.0.0.1:8000".parse().unwrap())
                .dispatch()
                .await;

            assert_eq!(response.status(), Status::Ok);
        }
    }

    #[tokio::test]
    async fn test_internal_auth_allows_localhost() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![internal_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();
        let response = client
            .get("/internal-test")
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("127.0.0.1:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    #[tokio::test]
    async fn test_internal_auth_allows_ipv6_localhost() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![internal_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();
        let response = client
            .get("/internal-test")
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("[::1]:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
    }

    #[tokio::test]
    async fn test_internal_auth_blocks_external_ip() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![internal_endpoint]);

        let client = Client::tracked(rocket).await.unwrap();
        let response = client
            .get("/internal-test")
            .header(Header::new(
                "X-API-KEY",
                "test-key-12345678901234567890123456",
            ))
            .remote("8.8.8.8:8000".parse().unwrap())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Forbidden);
    }

    #[test]
    fn test_issuer_api_key_accepts_exactly_32_chars() {
        let key = "12345678901234567890123456789012"; // exactly 32 chars
        assert_eq!(key.len(), 32);

        let result: Result<IssuerApiKey, _> = key.parse();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), key);
    }

    #[test]
    fn test_issuer_api_key_accepts_longer_than_32_chars() {
        let key = "this-is-a-very-long-api-key-that-exceeds-32-characters";
        assert!(key.len() > 32);

        let result: Result<IssuerApiKey, _> = key.parse();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), key);
    }

    #[test]
    fn test_issuer_api_key_rejects_shorter_than_32_chars() {
        let key = "short-key";
        assert!(key.len() < 32);

        let result: Result<IssuerApiKey, _> = key.parse();
        let err = result.unwrap_err();

        assert!(matches!(err, IssuerApiKeyError::TooShort { len: 9 }));
    }

    #[test]
    fn test_issuer_api_key_rejects_empty_string() {
        let result: Result<IssuerApiKey, _> = "".parse();
        let err = result.unwrap_err();

        assert!(matches!(err, IssuerApiKeyError::TooShort { len: 0 }));
    }
}
