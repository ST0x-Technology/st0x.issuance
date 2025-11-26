use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use ipnetwork::IpNetwork;
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::response::{Responder, Response};
use std::io::Cursor;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tracing::{info, warn};

use crate::config::Config;

#[derive(Debug, thiserror::Error)]
pub enum RateLimiterError {
    #[error("Rate limit value must be non-zero")]
    InvalidRateLimit,
}

pub(crate) struct FailedAuthRateLimiter(Arc<DefaultKeyedRateLimiter<IpAddr>>);

impl FailedAuthRateLimiter {
    pub(crate) fn new() -> Result<Self, RateLimiterError> {
        let rate_limit =
            NonZeroU32::new(10).ok_or(RateLimiterError::InvalidRateLimit)?;
        let quota = Quota::per_minute(rate_limit);
        Ok(Self(Arc::new(RateLimiter::keyed(quota))))
    }

    fn check(&self, ip: &IpAddr) -> bool {
        self.0.check_key(ip).is_ok()
    }
}

pub(crate) struct IssuerAuth;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for IssuerAuth {
    type Error = AuthError;

    async fn from_request(
        request: &'r Request<'_>,
    ) -> Outcome<Self, Self::Error> {
        let Some(config) = request.rocket().state::<Config>() else {
            warn!("Config not found in Rocket state");
            return Outcome::Error((
                Status::InternalServerError,
                AuthError::ConfigMissing,
            ));
        };

        let Some(rate_limiter) =
            request.rocket().state::<FailedAuthRateLimiter>()
        else {
            warn!("Rate limiter not found in Rocket state");
            return Outcome::Error((
                Status::InternalServerError,
                AuthError::ConfigMissing,
            ));
        };

        let Some(auth_header) = request.headers().get_one("Authorization")
        else {
            warn!(
                endpoint = %request.uri(),
                "Missing Authorization header"
            );
            return Outcome::Error((
                Status::Unauthorized,
                AuthError::MissingApiKey,
            ));
        };

        let Some(api_key) = auth_header.strip_prefix("Bearer ") else {
            warn!(
                endpoint = %request.uri(),
                "Malformed Authorization header (expected 'Bearer <key>')"
            );
            return Outcome::Error((
                Status::Unauthorized,
                AuthError::InvalidApiKey,
            ));
        };

        let expected_key = &config.issuer_api_key;
        if !validate_api_key(api_key, expected_key) {
            if let Some(client_ip) = extract_client_ip(request) {
                if !rate_limiter.check(&client_ip) {
                    warn!(
                        ip = %client_ip,
                        endpoint = %request.uri(),
                        "Rate limit exceeded for failed authentication"
                    );
                    return Outcome::Error((
                        Status::TooManyRequests,
                        AuthError::RateLimited,
                    ));
                }
            }

            warn!(
                endpoint = %request.uri(),
                "Invalid API key"
            );
            return Outcome::Error((
                Status::Unauthorized,
                AuthError::InvalidApiKey,
            ));
        }

        let Some(client_ip) = extract_client_ip(request) else {
            warn!(
                endpoint = %request.uri(),
                "Could not determine client IP"
            );
            return Outcome::Error((Status::BadRequest, AuthError::NoClientIp));
        };

        if !is_ip_whitelisted(&client_ip, &config.alpaca_ip_ranges) {
            if !rate_limiter.check(&client_ip) {
                warn!(
                    ip = %client_ip,
                    endpoint = %request.uri(),
                    "Rate limit exceeded for failed authentication"
                );
                return Outcome::Error((
                    Status::TooManyRequests,
                    AuthError::RateLimited,
                ));
            }

            warn!(
                ip = %client_ip,
                endpoint = %request.uri(),
                "IP not whitelisted"
            );
            return Outcome::Error((
                Status::Forbidden,
                AuthError::UnauthorizedIp,
            ));
        }

        info!(
            ip = %client_ip,
            endpoint = %request.uri(),
            "Issuer authentication success"
        );

        Outcome::Success(Self)
    }
}

fn validate_api_key(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

fn extract_client_ip(request: &Request<'_>) -> Option<IpAddr> {
    request.client_ip()
}

fn is_ip_whitelisted(ip: &IpAddr, allowed_ranges: &[IpNetwork]) -> bool {
    allowed_ranges.iter().any(|range| range.contains(*ip))
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

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::http::Header;
    use rocket::local::asynchronous::Client;

    use crate::alpaca::service::AlpacaConfig;
    use crate::config::LogLevel;

    #[rocket::get("/test")]
    fn test_endpoint(_auth: IssuerAuth) -> &'static str {
        "authenticated"
    }

    fn test_config() -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            database_max_connections: 5,
            rpc_url: None,
            private_key: None,
            vault_address: None,
            redemption_wallet: None,
            issuer_api_key: "test-key-12345678901234567890123456".to_string(),
            alpaca_ip_ranges: vec!["127.0.0.1/32".parse().unwrap()],
            log_level: LogLevel::Debug,
            hyperdx: None,
            alpaca: AlpacaConfig::test_default(),
        }
    }

    #[tokio::test]
    async fn test_missing_authorization_header_returns_401() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![test_endpoint]);

        let client =
            Client::tracked(rocket).await.expect("valid rocket instance");

        let response = client.get("/test").dispatch().await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[tokio::test]
    async fn test_malformed_authorization_header_returns_401() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![test_endpoint]);

        let client =
            Client::tracked(rocket).await.expect("valid rocket instance");

        let response = client
            .get("/test")
            .header(Header::new("Authorization", "InvalidFormat"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[tokio::test]
    async fn test_invalid_api_key_returns_401() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![test_endpoint]);

        let client =
            Client::tracked(rocket).await.expect("valid rocket instance");

        let response = client
            .get("/test")
            .header(Header::new("Authorization", "Bearer wrong-key"))
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
    async fn test_ip_whitelist_validation() {
        let ranges: Vec<IpNetwork> = vec![
            "10.0.0.0/24".parse().unwrap(),
            "192.168.1.100/32".parse().unwrap(),
        ];

        let ip1: IpAddr = "10.0.0.50".parse().unwrap();
        let ip2: IpAddr = "192.168.1.100".parse().unwrap();
        let ip3: IpAddr = "8.8.8.8".parse().unwrap();

        assert!(is_ip_whitelisted(&ip1, &ranges));
        assert!(is_ip_whitelisted(&ip2, &ranges));
        assert!(!is_ip_whitelisted(&ip3, &ranges));
    }

    #[tokio::test]
    async fn test_rate_limit_11th_failed_attempt_returns_429() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![test_endpoint]);

        let client =
            Client::tracked(rocket).await.expect("valid rocket instance");

        for i in 0..10 {
            let response = client
                .get("/test")
                .header(Header::new("Authorization", "Bearer wrong-key"))
                .header(Header::new("X-Real-IP", "127.0.0.1"))
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
            .get("/test")
            .header(Header::new("Authorization", "Bearer wrong-key"))
            .header(Header::new("X-Real-IP", "127.0.0.1"))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::TooManyRequests);
    }

    #[tokio::test]
    async fn test_successful_auth_does_not_count_against_limit() {
        let rocket = rocket::build()
            .manage(test_config())
            .manage(FailedAuthRateLimiter::new().unwrap())
            .mount("/", rocket::routes![test_endpoint]);

        let client =
            Client::tracked(rocket).await.expect("valid rocket instance");

        for _ in 0..15 {
            let response = client
                .get("/test")
                .header(Header::new(
                    "Authorization",
                    "Bearer test-key-12345678901234567890123456",
                ))
                .header(Header::new("X-Real-IP", "127.0.0.1"))
                .dispatch()
                .await;

            assert_eq!(response.status(), Status::Ok);
        }
    }
}
