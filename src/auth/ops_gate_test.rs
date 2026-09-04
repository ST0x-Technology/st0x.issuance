//! Route-level tests for the IAP-gated operator API: the tier guards on probe
//! routes (missing assertion, own-tier acceptance, cross-tier rejection) and
//! the real `/ops/*` handlers (gated when mounted, absent when unconfigured).

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL;
use event_sorcery::StoreBuilder;
use httpmock::prelude::*;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use p256::ecdsa::SigningKey;
use p256::pkcs8::EncodePrivateKey;
use rocket::http::Status;
use rocket::local::asynchronous::Client;
use serde::Serialize;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite};
use tracing::Level;
use tracing_test::traced_test;

use super::iap::ASSERTION_HEADER;
use super::{BreakglassOps, CapitalOps, DebugOps, OpsApiVerifiers, ReadOps};
use crate::config::OpsApiConfig;
use crate::test_utils::{logs_contain_at, setup_test_rocket};
use crate::underlying::Underlying;

const TEST_KID: &str = "test-key";
const IAP_ISSUER: &str = "https://cloud.google.com/iap";
const READ_AUDIENCE: &str = "aud-read";
const DEBUG_AUDIENCE: &str = "aud-debug";
const BREAKGLASS_AUDIENCE: &str = "aud-break";
const CAPITAL_AUDIENCE: &str = "aud-capital";

fn ops_config() -> OpsApiConfig {
    OpsApiConfig {
        read: READ_AUDIENCE.to_string(),
        debug: DEBUG_AUDIENCE.to_string(),
        capital: CAPITAL_AUDIENCE.to_string(),
        breakglass: BREAKGLASS_AUDIENCE.to_string(),
    }
}

#[rocket::get("/probe/read")]
fn read_probe(_auth: ReadOps) -> &'static str {
    "ok"
}

#[rocket::get("/probe/debug")]
fn debug_probe(_auth: DebugOps) -> &'static str {
    "ok"
}

#[rocket::get("/probe/breakglass")]
fn breakglass_probe(_auth: BreakglassOps) -> &'static str {
    "ok"
}

#[rocket::get("/probe/capital")]
fn capital_probe(_auth: CapitalOps) -> &'static str {
    "ok"
}

/// Probe rocket carrying only the verifiers the guards read; the probe handlers
/// need no other state, so this isolates the guard from the real handlers.
fn probe_rocket(verifiers: OpsApiVerifiers) -> rocket::Rocket<rocket::Build> {
    rocket::build().manage(verifiers).mount(
        "/",
        rocket::routes![
            read_probe,
            debug_probe,
            capital_probe,
            breakglass_probe
        ],
    )
}

#[derive(Serialize)]
struct TestClaims {
    sub: String,
    email: String,
    aud: String,
    iss: String,
    exp: u64,
}

/// A P-256 keypair standing in for Google's: the JWK halves for the mocked key
/// set, and the PEM that signs test tokens.
struct TestKey {
    signing_pem: Vec<u8>,
    x: String,
    y: String,
}

fn test_key() -> TestKey {
    let signing =
        SigningKey::from_bytes(&[7u8; 32].into()).expect("valid P-256 scalar");
    let public = signing.verifying_key().to_encoded_point(false);

    TestKey {
        signing_pem: signing
            .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
            .expect("PEM encodes")
            .as_bytes()
            .to_vec(),
        x: BASE64_URL.encode(public.x().expect("uncompressed point has x")),
        y: BASE64_URL.encode(public.y().expect("uncompressed point has y")),
    }
}

fn token(key: &TestKey, audience: &str) -> String {
    let exp = u64::try_from(
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after epoch")
                .as_secs(),
        )
        .expect("fits i64")
            + 300,
    )
    .expect("not before epoch");

    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(TEST_KID.to_string());

    encode(
        &header,
        &TestClaims {
            sub: "accounts.google.com:1234".to_string(),
            email: "operator@rainlang.xyz".to_string(),
            aud: audience.to_string(),
            iss: IAP_ISSUER.to_string(),
            exp,
        },
        &EncodingKey::from_ec_pem(&key.signing_pem).expect("PEM parses"),
    )
    .expect("token encodes")
}

fn jwks_server(key: &TestKey) -> MockServer {
    let server = MockServer::start();
    let body = serde_json::json!({
        "keys": [{
            "kid": TEST_KID,
            "kty": "EC",
            "crv": "P-256",
            "alg": "ES256",
            "x": key.x,
            "y": key.y,
        }]
    });

    server.mock(|when, then| {
        when.method(GET).path("/keys");
        then.status(200).json_body(body);
    });

    server
}

/// The VPC-internal case: a request that reached the bot without an IAP
/// assertion is refused by the app on every tier, not trusted for its network.
#[traced_test]
#[tokio::test]
async fn every_tier_refuses_a_missing_assertion() {
    let verifiers =
        OpsApiVerifiers::new(&ops_config(), &reqwest::Client::new());
    let client = Client::tracked(probe_rocket(verifiers)).await.unwrap();

    for path in
        ["/probe/read", "/probe/debug", "/probe/capital", "/probe/breakglass"]
    {
        let response = client.get(path).dispatch().await;
        assert_eq!(response.status(), Status::Unauthorized, "{path}");
    }

    assert!(logs_contain_at!(
        Level::WARN,
        &["Request carries no IAP assertion"]
    ));
}

/// Each tier's guard accepts an assertion minted for its own audience: the
/// happy path all the way through a real Rocket guard.
#[traced_test]
#[tokio::test]
async fn each_tier_accepts_an_assertion_for_its_own_audience() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client = Client::tracked(probe_rocket(verifiers)).await.unwrap();

    for (path, audience) in [
        ("/probe/read", READ_AUDIENCE),
        ("/probe/debug", DEBUG_AUDIENCE),
        ("/probe/capital", CAPITAL_AUDIENCE),
        ("/probe/breakglass", BREAKGLASS_AUDIENCE),
    ] {
        let response = client
            .get(path)
            .header(rocket::http::Header::new(
                ASSERTION_HEADER,
                token(&key, audience),
            ))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Ok, "{path}");
    }

    assert!(logs_contain_at!(Level::INFO, &["IAP assertion accepted"]));
}

/// The property the tiering rests on: IAP binds a token to the backend that
/// admitted it, so a debug-tier operator's assertion is refused on the
/// breakglass path even though the signature is valid. This is what stops a
/// debug operator force-completing or closing.
#[traced_test]
#[tokio::test]
async fn a_debug_tier_assertion_cannot_reach_the_breakglass_tier() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client = Client::tracked(probe_rocket(verifiers)).await.unwrap();

    let response = client
        .get("/probe/breakglass")
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, DEBUG_AUDIENCE),
        ))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Unauthorized);
    assert!(logs_contain_at!(
        Level::WARN,
        &["IAP assertion failed validation"]
    ));
}

/// The freeze tier: a debug-tier assertion is refused on the capital path, so a
/// debug operator cannot freeze (the issue's acceptance criterion).
#[traced_test]
#[tokio::test]
async fn a_debug_tier_assertion_cannot_reach_the_capital_tier() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client = Client::tracked(probe_rocket(verifiers)).await.unwrap();

    let response = client
        .get("/probe/capital")
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, DEBUG_AUDIENCE),
        ))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Unauthorized);
}

/// A read-tier assertion is likewise refused on the debug path: no tier's
/// token verifies against another tier's pinned audience.
#[traced_test]
#[tokio::test]
async fn a_read_tier_assertion_cannot_reach_the_debug_tier() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client = Client::tracked(probe_rocket(verifiers)).await.unwrap();

    let response = client
        .get("/probe/debug")
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, READ_AUDIENCE),
        ))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Unauthorized);
}

/// The real read- and debug-tier handlers, mounted on the full app state, are
/// refused without an IAP assertion: the gate is on the production routes, not
/// only on the probes.
#[traced_test]
#[tokio::test]
async fn real_ops_routes_require_an_iap_assertion() {
    let verifiers =
        OpsApiVerifiers::new(&ops_config(), &reqwest::Client::new());
    let rocket = setup_test_rocket()
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .mount(
            "/",
            rocket::routes![
                crate::admin::list_stuck_ops,
                crate::admin::reprocess_mint_ops,
                crate::admin::orchestrator_health_ops
            ],
        );
    let client = Client::tracked(rocket).await.unwrap();

    let read = client.get("/ops/read/stuck").dispatch().await;
    assert_eq!(read.status(), Status::Unauthorized);

    let debug = client
        .post("/ops/debug/reprocess/mint/00000000-0000-0000-0000-000000000000")
        .dispatch()
        .await;
    assert_eq!(debug.status(), Status::Unauthorized);

    let health = client.get("/ops/read/orchestrator-health").dispatch().await;
    assert_eq!(health.status(), Status::Unauthorized);

    assert!(logs_contain_at!(
        Level::WARN,
        &["Request carries no IAP assertion"]
    ));
}

/// Without configured audiences the `/ops/*` routes are not mounted at all: a
/// deployment with no load balancer serves 404, never a 401 that suggests the
/// path exists and wants credentials.
#[tokio::test]
async fn role_prefixes_are_absent_without_ops_api_config() {
    let rocket = setup_test_rocket().await.expect("test rocket builds");
    let client = Client::tracked(rocket).await.unwrap();

    let response = client.get("/ops/read/stuck").dispatch().await;
    assert_eq!(response.status(), Status::NotFound);
}

async fn migrated_pool() -> Pool<Sqlite> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("in-memory pool connects");
    sqlx::migrate!("./migrations").run(&pool).await.expect("migrations run");
    pool
}

/// Rocket carrying the verifiers plus the `Underlying` store and pool the
/// freeze/unfreeze/status handlers need, mounting only those three routes.
async fn underlying_ops_rocket(
    verifiers: OpsApiVerifiers,
) -> rocket::Rocket<rocket::Build> {
    let pool = migrated_pool().await;
    let (store, _projection) = StoreBuilder::<Underlying>::new(pool.clone())
        .build(())
        .await
        .expect("underlying store builds");
    rocket::build().manage(verifiers).manage(store).manage(pool).mount(
        "/",
        rocket::routes![
            crate::admin::asset_status_ops,
            crate::admin::freeze_underlying_ops,
            crate::admin::unfreeze_underlying_ops
        ],
    )
}

/// The freeze/unfreeze (capital) and status (read) routes are gated: without an
/// IAP assertion every one is refused inside the app.
#[traced_test]
#[tokio::test]
async fn underlying_ops_routes_require_an_iap_assertion() {
    let verifiers =
        OpsApiVerifiers::new(&ops_config(), &reqwest::Client::new());
    let client =
        Client::tracked(underlying_ops_rocket(verifiers).await).await.unwrap();

    let status = client.get("/ops/read/status/AAPL").dispatch().await;
    assert_eq!(status.status(), Status::Unauthorized);

    let freeze = client.post("/ops/capital/freeze/AAPL").dispatch().await;
    assert_eq!(freeze.status(), Status::Unauthorized);

    let unfreeze = client.post("/ops/capital/unfreeze/AAPL").dispatch().await;
    assert_eq!(unfreeze.status(), Status::Unauthorized);

    assert!(logs_contain_at!(
        Level::WARN,
        &["Request carries no IAP assertion"]
    ));
}

/// A read token admits the status route and the handler runs: an unlisted
/// underlying is a 404, proving the guard passed and the handler executed.
#[traced_test]
#[tokio::test]
async fn read_status_admits_its_token_and_reports_unlisted_as_not_found() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client =
        Client::tracked(underlying_ops_rocket(verifiers).await).await.unwrap();

    let response = client
        .get("/ops/read/status/UNLISTED")
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, READ_AUDIENCE),
        ))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::NotFound);
}

/// A capital token admits the freeze route and the handler runs; freezing an
/// unlisted underlying is refused (404) before any aggregate state is created.
#[traced_test]
#[tokio::test]
async fn capital_freeze_admits_its_token_and_refuses_an_unlisted_underlying() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client =
        Client::tracked(underlying_ops_rocket(verifiers).await).await.unwrap();

    let response = client
        .post("/ops/capital/freeze/UNLISTED")
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, CAPITAL_AUDIENCE),
        ))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::NotFound);
}
