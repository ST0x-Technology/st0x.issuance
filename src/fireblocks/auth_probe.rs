//! A/B probe for Fireblocks API authentication.
//!
//! Sends the same authenticated GET twice, varying only the JWT expiry
//! window, and surfaces the raw HTTP status and response body the platform
//! returns. Exists because the SDK both violates Fireblocks' documented
//! `exp < iat + 30s` bound (it signs `iat + 55`) and swallows 401 response
//! bodies, leaving a bare status code that cannot distinguish a rejected
//! expiry window from a disabled API user, an expired certificate, or a
//! permission change. The probe mirrors the SDK's JWT claims exactly and
//! reports what the server actually said.

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rand::Rng;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use super::config::FireblocksConfig;

/// What one probe request observed: the expiry window it signed with and the
/// verbatim platform response.
#[derive(Debug)]
pub struct AuthProbeReport {
    pub expiry_seconds: u64,
    pub status: u16,
    pub body: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthProbeError {
    #[error("failed to build the probe JWT: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("probe request failed before receiving a response: {0}")]
    Http(#[from] reqwest::Error),
    #[error("system clock error: {0}")]
    Clock(#[from] std::time::SystemTimeError),
    #[error("failed to format the body hash: {0}")]
    Fmt(#[from] std::fmt::Error),
}

/// The JWT claims Fireblocks authenticates, mirroring the SDK's shape
/// (`uri`, `nonce`, `iat`, `exp`, `sub`, `bodyHash`) so a probe failure is
/// attributable to the platform, not to a differently-shaped token.
#[derive(Debug, Serialize)]
struct ProbeClaims<'request> {
    uri: &'request str,
    nonce: u64,
    iat: u64,
    exp: u64,
    sub: &'request str,
    #[serde(rename = "bodyHash")]
    body_hash: String,
}

/// Sends one authenticated GET to `base_url` + `uri_path`, signing the JWT
/// with an expiry of `iat + expiry_seconds`, and returns the raw status and
/// body. Never logs or returns the key material.
///
/// # Errors
///
/// Returns an error if the JWT cannot be signed, the clock is unreadable, or
/// the request fails at the transport layer (a non-2xx response is a report,
/// not an error).
pub async fn probe_auth(
    base_url: &str,
    api_user_id: &str,
    rsa_key_pem: &[u8],
    uri_path: &str,
    expiry_seconds: u64,
) -> Result<AuthProbeReport, AuthProbeError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let body_hash = {
        let mut digest = Sha256::new();
        digest.update([]);
        let mut hex = String::new();
        for byte in digest.finalize() {
            write!(hex, "{byte:02x}")?;
        }
        hex
    };

    let claims = ProbeClaims {
        uri: uri_path,
        nonce: rand::thread_rng().r#gen(),
        iat: now,
        exp: now + expiry_seconds,
        sub: api_user_id,
        body_hash,
    };

    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::RS256),
        &claims,
        &EncodingKey::from_rsa_pem(rsa_key_pem)?,
    )?;

    let response = reqwest::Client::new()
        .get(format!("{base_url}{uri_path}"))
        .header("X-API-Key", api_user_id)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;

    let status = response.status().as_u16();
    let body = response.text().await?;

    Ok(AuthProbeReport { expiry_seconds, status, body })
}

/// Runs the A/B pair against the vault-address endpoint the service hits at
/// startup: once with a compliant `iat + 29` expiry, once with the SDK's
/// `iat + 55`.
///
/// A split verdict (29 accepted, 55 rejected) proves the platform began
/// enforcing its documented bound; two identical rejections carry the
/// server's own error body for diagnosis.
///
/// # Errors
///
/// Returns an error if either probe fails before receiving a response.
pub async fn probe_auth_pair(
    base_url: &str,
    config: &FireblocksConfig,
) -> Result<Vec<AuthProbeReport>, AuthProbeError> {
    let uri_path = format!(
        "/v1/vault/accounts/{}/{}/addresses_paginated",
        config.vault_account_id.as_str(),
        config.chain_asset_ids.default_asset_id().as_str(),
    );

    let mut reports = Vec::new();
    for expiry_seconds in [29, 55] {
        reports.push(
            probe_auth(
                base_url,
                config.api_user_id.as_str(),
                &config.secret,
                &uri_path,
                expiry_seconds,
            )
            .await?,
        );
    }

    Ok(reports)
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use jsonwebtoken::{Algorithm, DecodingKey, Validation};
    use rsa::pkcs8::EncodePrivateKey;
    use rsa::{RsaPrivateKey, pkcs1::EncodeRsaPublicKey};
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct DecodedClaims {
        uri: String,
        iat: u64,
        exp: u64,
        sub: String,
        #[serde(rename = "bodyHash")]
        body_hash: String,
    }

    fn test_rsa_key() -> (Vec<u8>, Vec<u8>) {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let private_pem = key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .as_bytes()
            .to_vec();
        let public_pem = key
            .to_public_key()
            .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .into_bytes();
        (private_pem, public_pem)
    }

    fn decode_bearer(authorization: &str, public_pem: &[u8]) -> DecodedClaims {
        let token = authorization
            .strip_prefix("Bearer ")
            .expect("Authorization must be a Bearer token");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_required_spec_claims::<&str>(&[]);
        validation.validate_exp = false;
        jsonwebtoken::decode::<DecodedClaims>(
            token,
            &DecodingKey::from_rsa_pem(public_pem).unwrap(),
            &validation,
        )
        .expect("probe JWT must verify against the signing key")
        .claims
    }

    /// The probe's whole value is fidelity: the JWT must carry exactly the
    /// requested expiry window, the request path as `uri`, the api user as
    /// `sub`, and the empty-body SHA-256 — signed by the supplied key.
    #[tokio::test]
    async fn probe_signs_the_requested_expiry_window() {
        let (private_pem, public_pem) = test_rsa_key();
        let captured: std::sync::Arc<std::sync::Mutex<Option<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let capture = captured.clone();

        let server = MockServer::start();
        server.mock(move |when, then| {
            when.method("GET").path("/v1/vault/accounts/0/ETH-TEST").is_true(
                move |request| {
                    let authorization = request
                        .headers_vec()
                        .iter()
                        .find(|(name, _)| {
                            name.eq_ignore_ascii_case("authorization")
                        })
                        .map(|(_, value)| value.clone());
                    *capture.lock().unwrap() = authorization;
                    true
                },
            );
            then.status(200)
                .header("content-type", "application/json")
                .body("{}");
        });

        let report = probe_auth(
            &server.base_url(),
            "api-user-test",
            &private_pem,
            "/v1/vault/accounts/0/ETH-TEST",
            29,
        )
        .await
        .unwrap();

        assert_eq!(report.status, 200);
        assert_eq!(report.expiry_seconds, 29);

        let authorization = captured
            .lock()
            .unwrap()
            .clone()
            .expect("the probe must send an Authorization header");
        let claims = decode_bearer(&authorization, &public_pem);

        assert_eq!(claims.exp - claims.iat, 29);
        assert_eq!(claims.uri, "/v1/vault/accounts/0/ETH-TEST");
        assert_eq!(claims.sub, "api-user-test");
        assert_eq!(
            claims.body_hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// The SDK swallows non-2xx bodies; the probe exists to surface them.
    /// A 401 with a diagnostic body must come back verbatim as a report,
    /// not as an error.
    #[tokio::test]
    async fn probe_surfaces_rejection_status_and_body_verbatim() {
        let (private_pem, _public_pem) = test_rsa_key();
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method("GET").path("/v1/vault/accounts/0/ETH-TEST");
            then.status(401)
                .header("content-type", "application/json")
                .body(r#"{"message":"Token expiry too far","code":-7}"#);
        });

        let report = probe_auth(
            &server.base_url(),
            "api-user-test",
            &private_pem,
            "/v1/vault/accounts/0/ETH-TEST",
            55,
        )
        .await
        .unwrap();

        assert_eq!(report.status, 401);
        assert!(
            report.body.contains(r#""code":-7"#),
            "the platform's error body must be preserved, got: {}",
            report.body
        );
    }
}
