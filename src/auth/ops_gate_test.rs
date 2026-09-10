//! Route-level tests for the IAP-gated operator API: the tier guards on probe
//! routes (missing assertion, own-tier acceptance, cross-tier rejection) and
//! the real `/ops/*` handlers (gated when mounted, absent when unconfigured).

use alloy::primitives::{Address, B256};
use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL;
use event_sorcery::{Store, StoreBuilder};
use httpmock::prelude::*;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use p256::ecdsa::SigningKey;
use p256::pkcs8::EncodePrivateKey;
use rocket::http::{ContentType, Method, Status};
use rocket::local::asynchronous::Client;
use serde::Serialize;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::Level;
use tracing_test::traced_test;
use url::Url;

use super::iap::ASSERTION_HEADER;
use super::{
    BreakglassOps, CapitalOps, DebugOps, FailedAuthRateLimiter,
    OpsApiVerifiers, ReadOps,
};
use crate::account::Account;
use crate::admin::RedemptionBurnRecovery;
use crate::alpaca::AlpacaService;
use crate::alpaca::mock::MockAlpacaService;
use crate::burn_excess::api::BurnExcessExternalRequest;
use crate::chain::{ChainConfig, ConfiguredNetworks};
use crate::config::{Config, OpsApiConfig};
use crate::mint::Mint;
use crate::network_telemetry::NetworkTelemetry;
use crate::receipt_inventory::{
    CqrsReceiptService, ReceiptInventory, ReceiptService,
};
use crate::redemption::burn_manager::{
    BurnManagerError, ManualBurnReplacementOutcome, RecoveryOutcome,
};
use crate::redemption::poller_pause::{
    PollerPause, PollerPauses, poller_pause,
};
use crate::redemption::{
    IssuerRedemptionRequestId, Redemption, RedemptionServices,
};
use crate::test_utils::{
    logs_contain_at, setup_test_rocket, setup_test_rocket_with_config,
    test_config,
};
use crate::tokenized_asset::schedule::FreezeScheduler;
use crate::tokenized_asset::{
    AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol,
};
use crate::underlying::Underlying;
use crate::vault::{BurnVerification, NetworkVaultServices};
use crate::wallet::SignerConfig;

const TEST_KID: &str = "test-key";
const IAP_ISSUER: &str = "https://cloud.google.com/iap";
const READ_AUDIENCE: &str = "aud-read";
const DEBUG_AUDIENCE: &str = "aud-debug";
const BREAKGLASS_AUDIENCE: &str = "aud-break";
const CAPITAL_AUDIENCE: &str = "aud-capital";

fn ops_config() -> OpsApiConfig {
    OpsApiConfig::new(
        Some(READ_AUDIENCE.to_string()),
        Some(DEBUG_AUDIENCE.to_string()),
        Some(CAPITAL_AUDIENCE.to_string()),
        Some(BREAKGLASS_AUDIENCE.to_string()),
    )
    .expect("valid ops config")
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
    iat: u64,
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after epoch")
        .as_secs();

    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(TEST_KID.to_string());

    encode(
        &header,
        &TestClaims {
            sub: "accounts.google.com:1234".to_string(),
            email: "operator@rainlang.xyz".to_string(),
            aud: audience.to_string(),
            iss: IAP_ISSUER.to_string(),
            iat: now,
            exp: now + 300,
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
        .manage(PollerPauses::new(HashMap::new()))
        .mount(
            "/",
            rocket::routes![
                crate::admin::list_stuck_ops,
                crate::admin::reprocess_mint_ops,
                crate::admin::orchestrator_health_ops,
                crate::burn_excess::api::burn_excess_internal_ops,
                crate::burn_excess::api::burn_excess_external_ops,
                crate::tokenized_asset::orchestrator_ops::orchestrator_preflight_ops,
                crate::tokenized_asset::orchestrator_ops::orchestrator_verify_signing_ops,
                crate::tokenized_asset::orchestrator_ops::orchestrator_approve_ops,
                crate::admin::aggregate_snapshot_ops
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

    let burn = client
        .post("/ops/breakglass/burn-excess/internal")
        .header(rocket::http::ContentType::JSON)
        .body("{}")
        .dispatch()
        .await;
    assert_eq!(burn.status(), Status::Unauthorized);

    let burn_external = client
        .post("/ops/breakglass/burn-excess/external")
        .header(rocket::http::ContentType::JSON)
        .body("{}")
        .dispatch()
        .await;
    assert_eq!(burn_external.status(), Status::Unauthorized);

    let preflight =
        client.get("/ops/read/orchestrator-preflight/base").dispatch().await;
    assert_eq!(preflight.status(), Status::Unauthorized);

    let verify = client
        .post("/ops/debug/orchestrator-verify-signing/base/AAPL")
        .dispatch()
        .await;
    assert_eq!(verify.status(), Status::Unauthorized);

    let approve = client
        .post("/ops/capital/orchestrator-approve/base/AAPL")
        .dispatch()
        .await;
    assert_eq!(approve.status(), Status::Unauthorized);

    let snapshot =
        client.get("/ops/read/snapshots/Mint/some-id").dispatch().await;
    assert_eq!(snapshot.status(), Status::Unauthorized);

    assert!(logs_contain_at!(
        Level::WARN,
        &["Request carries no IAP assertion"]
    ));
}

/// The headline breakglass property: a debug-tier operator cannot burn excess.
/// A debug token is refused on the breakglass burn-excess route before the
/// handler (and the signer) is ever reached.
#[traced_test]
#[tokio::test]
async fn a_debug_token_cannot_burn_excess() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let rocket = setup_test_rocket()
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .manage(PollerPauses::new(HashMap::new()))
        .mount(
            "/",
            rocket::routes![
                crate::burn_excess::api::burn_excess_internal_ops,
                crate::burn_excess::api::burn_excess_external_ops
            ],
        );
    let client = Client::tracked(rocket).await.unwrap();

    let internal = client
        .post("/ops/breakglass/burn-excess/internal")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, DEBUG_AUDIENCE),
        ))
        .body("{}")
        .dispatch()
        .await;
    assert_eq!(internal.status(), Status::Unauthorized);

    let external = client
        .post("/ops/breakglass/burn-excess/external")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, DEBUG_AUDIENCE),
        ))
        .body("{}")
        .dispatch()
        .await;
    assert_eq!(external.status(), Status::Unauthorized);
    assert!(logs_contain_at!(
        Level::WARN,
        &["IAP assertion failed validation"]
    ));
}

/// One representative route per tier. Each request matches its route's method
/// and format but carries no assertion, so 401 versus 404 is decided by
/// whether the tier is mounted and gated, nothing else.
const TIER_ROUTES: [(Method, &str); 4] = [
    (Method::Get, "/ops/read/stuck"),
    (Method::Post, "/ops/debug/recover/redemption/some-id"),
    (Method::Post, "/ops/capital/freeze/AAPL"),
    (Method::Post, "/ops/breakglass/close/mint/some-id"),
];

async fn probe_tier_route(
    client: &Client,
    method: Method,
    path: &str,
) -> Status {
    client.req(method, path).header(ContentType::JSON).dispatch().await.status()
}

/// The application rocket built by the same `build_rocket` production uses,
/// with `config` deciding whether the `/ops/*` routes are mounted. Every
/// managed state the ops routes declare is supplied (Rocket refuses to ignite
/// a route whose `State` is unmanaged); the gate refuses before any handler
/// runs, so none of it is exercised.
async fn app_rocket(config: Config) -> rocket::Rocket<rocket::Build> {
    let base = setup_test_rocket().await.expect("test rocket builds");
    let pool = base.state::<Pool<Sqlite>>().expect("pool managed").clone();
    let apalis_pool =
        base.state::<ApalisSqlitePool>().expect("apalis pool managed").clone();
    let vault_services = base
        .state::<NetworkVaultServices>()
        .expect("vault services managed")
        .clone();
    let account_store =
        base.state::<Arc<Store<Account>>>().expect("account store").clone();
    let tokenized_asset_store = base
        .state::<Arc<Store<TokenizedAsset>>>()
        .expect("tokenized asset store")
        .clone();
    let mint_store =
        base.state::<Arc<Store<Mint>>>().expect("mint store").clone();
    let redemption_store = StoreBuilder::<Redemption>::new(pool.clone())
        .build(RedemptionServices::new(vault_services.clone()))
        .await
        .expect("redemption store builds");
    let receipt_inventory = StoreBuilder::<ReceiptInventory>::new(pool.clone())
        .build(())
        .await
        .expect("receipt inventory store builds");
    let (underlying_store, _projection) =
        StoreBuilder::<Underlying>::new(pool.clone())
            .build(())
            .await
            .expect("underlying store builds");
    let (shutdown, _) = tokio::sync::watch::channel(false);

    crate::build_rocket(crate::RocketState {
        ops_verifiers: crate::build_ops_verifiers(&config)
            .expect("JWKS client builds"),
        freeze_scheduler: FreezeScheduler::new(&apalis_pool, pool.clone()),
        rate_limiter: FailedAuthRateLimiter::new().expect("rate limiter"),
        config,
        pool,
        apalis_pool,
        account_store,
        tokenized_asset_store,
        mint_store,
        redemption_store,
        alpaca_service: Arc::new(MockAlpacaService::new_success()),
        burn_recovery: Arc::new(UnreachableBurnRecovery),
        vault_services,
        configured_networks: ConfiguredNetworks::from_iter([Network::Base]),
        receipts: Arc::new(CqrsReceiptService::new(receipt_inventory)),
        network_telemetry: Arc::new(NetworkTelemetry::new([Network::Base])),
        underlying_store,
        poller_pauses: PollerPauses::new(HashMap::new()),
        background_tasks: crate::BackgroundTasks {
            shutdown,
            handles: Vec::new(),
        },
    })
}

/// With audiences configured the application mounts every tier under its
/// role prefix and gates it: a bare request reaches the guard and is refused
/// with 401, not 404. This drives the real mounting branch in `build_rocket`,
/// so a tier dropped from the route table or moved off its prefix fails here.
#[tokio::test]
async fn role_prefixes_are_mounted_and_gated_with_ops_api_config() {
    let config = Config {
        ops_api: Some(ops_config()),
        ..test_config().expect("test config builds")
    };
    let client = Client::tracked(app_rocket(config).await).await.unwrap();

    for (method, path) in TIER_ROUTES {
        assert_eq!(
            probe_tier_route(&client, method, path).await,
            Status::Unauthorized,
            "{path} must be mounted and gated"
        );
    }
}

/// Without configured audiences the `/ops/*` routes are not mounted at all: a
/// deployment with no load balancer serves 404, never a 401 that suggests the
/// path exists and wants credentials.
#[tokio::test]
async fn role_prefixes_are_absent_without_ops_api_config() {
    let config = test_config().expect("test config builds");
    let client = Client::tracked(app_rocket(config).await).await.unwrap();

    for (method, path) in TIER_ROUTES {
        assert_eq!(
            probe_tier_route(&client, method, path).await,
            Status::NotFound,
            "{path} must be absent"
        );
    }
}

/// Spawns a poller loop that counts its ticks, so a test can observe whether a
/// pause quiesced it. Ticks fast so a resume is visible within a short window.
fn spawn_counting_poller(
    mut pause: PollerPause,
    ticks: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            pause.wait_while_paused().await;
            ticks.fetch_add(1, Ordering::SeqCst);
            pause.interruptible_sleep(Duration::from_millis(10)).await;
        }
    })
}

/// A well-formed external burn body for `network`; the mint it names does not
/// exist in an empty store, so the engine errors once the handler runs.
fn external_body(network: Network) -> String {
    serde_json::json!({
        "issuer_request_id": "00000000-0000-0000-0000-000000000000",
        "deposit_tx_hash": format!("0x{}", "00".repeat(32)),
        "funding_tx_hash": format!("0x{}", "11".repeat(32)),
        "receipt_id": "0x1",
        "shares": "1.0",
        "reason": "excess share burn test",
        "network": network,
        "chain_id": network.chain_id(),
    })
    .to_string()
}

/// A network with no pause control is refused with 422 before any burn, and a
/// poller running for a different network is left untouched.
#[tokio::test]
async fn external_burn_without_a_pause_control_is_refused() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));

    let (ethereum_control, ethereum_pause) = poller_pause();
    let ethereum_parked = ethereum_control.parked_signal();
    let ticks = Arc::new(AtomicUsize::new(0));
    let poller = spawn_counting_poller(ethereum_pause, ticks.clone());
    let mut controls = HashMap::new();
    controls.insert(Network::Ethereum, ethereum_control);

    let rocket = setup_test_rocket()
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .manage(PollerPauses::new(controls))
        .mount(
            "/",
            rocket::routes![crate::burn_excess::api::burn_excess_external_ops],
        );
    let client = Client::tracked(rocket).await.unwrap();

    let body = external_body(Network::Base);
    assert!(
        serde_json::from_str::<BurnExcessExternalRequest>(&body).is_ok(),
        "the test body must be well-formed, so the 422 is the handler's \
         missing-control refusal, not a request-parse rejection"
    );

    let response = client
        .post("/ops/breakglass/burn-excess/external")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, BREAKGLASS_AUDIENCE),
        ))
        .body(body)
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::UnprocessableEntity);

    // The poller writes `parked` only on a real park, so a Base burn that
    // paused Ethereum even briefly and resumed before this line would have
    // moved the signal. It must not have.
    assert!(
        !ethereum_parked.has_changed().expect("Ethereum poller still running"),
        "a burn for another network must never park this poller"
    );

    poller.abort();
}

/// The pause guard resumes the poller on the handler's error paths, not only on
/// success: an erroring burn leaves the network's poller running.
#[tokio::test]
async fn external_burn_resumes_the_poller_after_an_error() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));

    let (control, pause) = poller_pause();
    let base_parked = control.parked_signal();
    let ticks = Arc::new(AtomicUsize::new(0));
    let poller = spawn_counting_poller(pause, ticks.clone());
    let mut controls = HashMap::new();
    controls.insert(Network::Base, control);

    // A configured Base chain and no CHAIN_BASE_RPC_URL in the environment:
    // the route must take its endpoint and chain id from here. The default
    // test signer is the zero key, which signer resolution refuses before the
    // engine runs; a real (nonzero) key lets the request reach the engine.
    let mut config = test_config().expect("test config builds");
    config.chains = vec![ChainConfig {
        network: Network::Base,
        chain_id: Network::Base.chain_id(),
        rpc_url: Url::parse("wss://localhost:8545").expect("valid url"),
        backfill_start_block: 0,
        low_gas_threshold: None,
    }];
    config.signer = SignerConfig::Local(B256::repeat_byte(7));
    let rocket = setup_test_rocket_with_config(config)
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .manage(PollerPauses::new(controls))
        .mount(
            "/",
            rocket::routes![crate::burn_excess::api::burn_excess_external_ops],
        );
    let client = Client::tracked(rocket).await.unwrap();

    let body = external_body(Network::Base);
    assert!(
        serde_json::from_str::<BurnExcessExternalRequest>(&body).is_ok(),
        "the test body must be well-formed, so the error is the handler's, \
         not a request-parse rejection"
    );

    let response = client
        .post("/ops/breakglass/burn-excess/external")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, BREAKGLASS_AUDIENCE),
        ))
        .body(body)
        .dispatch()
        .await;

    // The well-formed body ran the handler, which paused the poller then
    // errored in the engine; the guard must resume the poller on that error
    // path just as on success. The exact status matters: a broad `>= 400`
    // would also accept a 503 pause-acquisition failure, where no guard ever
    // exists. 404 is the engine refusing the absent mint, reachable only once
    // the pause succeeded and the engine ran - and only because the route
    // took the RPC and chain id from `config.chains` rather than the
    // environment (unset here) and dialed nothing first (no node listens).
    assert_eq!(response.status(), Status::NotFound);

    // The poller writes `parked` only on a real park: the handler must have
    // actually quiesced the poller before erroring, or "resumes" below proves
    // nothing.
    assert!(
        base_parked.has_changed().expect("Base poller still running"),
        "the handler must park the poller before the burn runs"
    );

    // The guard dropped on the error return, so the poller resumes ticking.
    let resumed_from = ticks.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        ticks.load(Ordering::SeqCst) > resumed_from,
        "the guard must resume the poller after the handler errors"
    );

    poller.abort();
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

/// A mutating route's records name the operator: the freeze log line carries
/// the assertion's subject id (never the email) via the handler's `operator`
/// span, so an audit can attribute the action from the service logs alone.
#[traced_test]
#[tokio::test]
async fn a_capital_freeze_is_attributed_to_the_assertion_subject() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let rocket = underlying_ops_rocket(verifiers).await;
    let pool = rocket.state::<Pool<Sqlite>>().expect("pool managed").clone();
    let (assets, _projection) =
        StoreBuilder::<TokenizedAsset>::new(pool.clone())
            .build(())
            .await
            .expect("tokenized asset store builds");
    let underlying = UnderlyingSymbol::new("AAPL").unwrap();
    assets
        .send(
            &AssetKey::new(underlying.clone(), Network::Base),
            TokenizedAssetCommand::Add {
                underlying,
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                vault: Address::repeat_byte(0x11),
            },
        )
        .await
        .expect("listing seeds");
    let client = Client::tracked(rocket).await.unwrap();

    let response = client
        .post("/ops/capital/freeze/AAPL")
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, CAPITAL_AUDIENCE),
        ))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Ok);
    logs_assert(|lines: &[&str]| {
        let freeze_lines: Vec<&&str> = lines
            .iter()
            .filter(|line| {
                line.contains("Freezing underlying across all networks")
            })
            .collect();
        if freeze_lines.is_empty() {
            return Err("no freeze record was logged".to_string());
        }
        for line in freeze_lines {
            if !line.contains("operator{subject=accounts.google.com:1234}") {
                return Err(format!(
                    "freeze record lacks the operator: {line}"
                ));
            }
            if line.contains("operator@rainlang.xyz") {
                return Err(format!("freeze record carries the email: {line}"));
            }
        }
        Ok(())
    });
}

/// Burn recovery that must never be reached: the IAP gate refuses every
/// request in these tests before a handler runs, so any call here is a gate
/// failure, not a recovery scenario.
struct UnreachableBurnRecovery;

#[async_trait]
impl RedemptionBurnRecovery for UnreachableBurnRecovery {
    async fn replace_exhausted_dead_burn(
        &self,
        _: &IssuerRedemptionRequestId,
    ) -> Result<ManualBurnReplacementOutcome, BurnManagerError> {
        unreachable!("the IAP gate refuses before the handler runs")
    }

    async fn execute_recovered_burn(
        &self,
        _: &IssuerRedemptionRequestId,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        unreachable!("the IAP gate refuses before the handler runs")
    }

    async fn force_complete_burn(
        &self,
        _: &IssuerRedemptionRequestId,
        _: B256,
        _: String,
        _: Option<B256>,
    ) -> Result<BurnVerification, BurnManagerError> {
        unreachable!("the IAP gate refuses before the handler runs")
    }
}

/// The full app rocket plus every piece of state the recovery, close,
/// force-complete, and freeze-schedule handlers declare, so all of them can be
/// mounted (Rocket refuses to ignite a route whose `State` is unmanaged). The
/// gate refuses before any handler runs, so none of this state is exercised.
async fn recovery_ops_rocket(
    verifiers: OpsApiVerifiers,
) -> rocket::Rocket<rocket::Build> {
    let rocket = setup_test_rocket().await.expect("test rocket builds");
    let pool = rocket.state::<Pool<Sqlite>>().expect("pool managed").clone();
    let vaults = rocket
        .state::<NetworkVaultServices>()
        .expect("vault services managed")
        .clone();
    let freeze_scheduler = FreezeScheduler::new(
        rocket.state::<ApalisSqlitePool>().expect("apalis pool managed"),
        pool.clone(),
    );

    let redemption_store = StoreBuilder::<Redemption>::new(pool.clone())
        .build(RedemptionServices::new(vaults))
        .await
        .expect("redemption store builds");
    let receipt_inventory = StoreBuilder::<ReceiptInventory>::new(pool)
        .build(())
        .await
        .expect("receipt inventory store builds");
    let receipts: Arc<dyn ReceiptService> =
        Arc::new(CqrsReceiptService::new(receipt_inventory));
    let burn_recovery: Arc<dyn RedemptionBurnRecovery> =
        Arc::new(UnreachableBurnRecovery);
    let alpaca: Arc<dyn AlpacaService> =
        Arc::new(MockAlpacaService::new_success());

    rocket
        .manage(verifiers)
        .manage(redemption_store)
        .manage(receipts)
        .manage(burn_recovery)
        .manage(alpaca)
        .manage(freeze_scheduler)
        .mount(
            "/",
            rocket::routes![
                crate::admin::recover_redemption_ops,
                crate::admin::close_redemption_ops,
                crate::admin::force_complete_redemption_ops,
                crate::admin::close_mint_ops,
                crate::admin::schedule_freeze_window_ops
            ],
        )
}

const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";

/// The recovery, close, force-complete, and freeze-schedule handlers, mounted
/// on the full app state, are refused without an IAP assertion.
#[traced_test]
#[tokio::test]
async fn recovery_ops_routes_require_an_iap_assertion() {
    let verifiers =
        OpsApiVerifiers::new(&ops_config(), &reqwest::Client::new());
    let client =
        Client::tracked(recovery_ops_rocket(verifiers).await).await.unwrap();

    let recover = client
        .post(format!("/ops/debug/recover/redemption/{REQUEST_ID}"))
        .dispatch()
        .await;
    assert_eq!(recover.status(), Status::Unauthorized);

    for path in [
        format!("/ops/breakglass/close/redemption/{REQUEST_ID}"),
        format!("/ops/breakglass/force-complete/redemption/{REQUEST_ID}"),
        format!("/ops/breakglass/close/mint/{REQUEST_ID}"),
        "/ops/capital/freeze-schedules".to_string(),
    ] {
        let response = client
            .post(&path)
            .header(rocket::http::ContentType::JSON)
            .body("{}")
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized, "{path}");
    }

    assert!(logs_contain_at!(
        Level::WARN,
        &["Request carries no IAP assertion"]
    ));
}

/// The done-when tier boundaries on the real handlers: a debug token cannot
/// force-complete, close, or schedule a freeze, and a read token cannot
/// recover. Each is refused before the handler runs.
#[traced_test]
#[tokio::test]
async fn lower_tier_tokens_cannot_recover_close_force_complete_or_freeze() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client =
        Client::tracked(recovery_ops_rocket(verifiers).await).await.unwrap();

    let recover = client
        .post(format!("/ops/debug/recover/redemption/{REQUEST_ID}"))
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, READ_AUDIENCE),
        ))
        .dispatch()
        .await;
    assert_eq!(recover.status(), Status::Unauthorized);

    for path in [
        format!("/ops/breakglass/close/redemption/{REQUEST_ID}"),
        format!("/ops/breakglass/force-complete/redemption/{REQUEST_ID}"),
        format!("/ops/breakglass/close/mint/{REQUEST_ID}"),
        "/ops/capital/freeze-schedules".to_string(),
    ] {
        let response = client
            .post(&path)
            .header(rocket::http::ContentType::JSON)
            .header(rocket::http::Header::new(
                ASSERTION_HEADER,
                token(&key, DEBUG_AUDIENCE),
            ))
            .body("{}")
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized, "{path}");
    }

    assert!(logs_contain_at!(
        Level::WARN,
        &["IAP assertion failed validation"]
    ));
}

/// The full app rocket plus the state the account, tokenized-asset, and
/// network-diagnostic twins declare beyond what [`setup_test_rocket`]
/// manages.
async fn onboarding_ops_rocket(
    verifiers: OpsApiVerifiers,
) -> rocket::Rocket<rocket::Build> {
    setup_test_rocket()
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .manage(ConfiguredNetworks::from_iter([Network::Base]))
        .manage(Arc::new(NetworkTelemetry::new([Network::Base])))
        .mount(
            "/",
            rocket::routes![
                crate::account::api::register_account_ops,
                crate::account::api::whitelist_wallet_ops,
                crate::account::api::unwhitelist_wallet_ops,
                crate::tokenized_asset::api::get_tokenized_asset_ops,
                crate::tokenized_asset::api::add_tokenized_asset_ops,
                crate::admin::network_telemetry_ops,
                crate::admin::list_wrapped_transfers_ops
            ],
        )
}

const WALLET: &str = "0x0000000000000000000000000000000000000001";

/// The account, tokenized-asset, and network-diagnostic twins, mounted on the
/// full app state, are refused without an IAP assertion.
#[traced_test]
#[tokio::test]
async fn onboarding_ops_routes_require_an_iap_assertion() {
    let verifiers =
        OpsApiVerifiers::new(&ops_config(), &reqwest::Client::new());
    let client =
        Client::tracked(onboarding_ops_rocket(verifiers).await).await.unwrap();

    for path in ["/ops/debug/accounts", "/ops/debug/tokenized-assets"] {
        let response = client
            .post(path)
            .header(rocket::http::ContentType::JSON)
            .body("{}")
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized, "{path}");
    }

    let whitelist = client
        .post(format!("/ops/debug/accounts/{REQUEST_ID}/wallets"))
        .header(rocket::http::ContentType::JSON)
        .body("{}")
        .dispatch()
        .await;
    assert_eq!(whitelist.status(), Status::Unauthorized);

    let unwhitelist = client
        .delete(format!("/ops/debug/accounts/{REQUEST_ID}/wallets/{WALLET}"))
        .dispatch()
        .await;
    assert_eq!(unwhitelist.status(), Status::Unauthorized);

    for path in [
        "/ops/debug/tokenized-assets/AAPL?network=base",
        "/ops/read/network-telemetry",
        "/ops/read/wrapped-transfers",
    ] {
        let response = client.get(path).dispatch().await;
        assert_eq!(response.status(), Status::Unauthorized, "{path}");
    }

    assert!(logs_contain_at!(
        Level::WARN,
        &["Request carries no IAP assertion"]
    ));
}

/// A read token is refused on every debug-tier onboarding route (account
/// registration, wallet whitelisting, asset listing all change what the bot
/// mints and to whom) but admitted on the read-tier diagnostics, which then
/// serve: the twins are wired to the real handlers, not only gated.
#[traced_test]
#[tokio::test]
async fn a_read_token_cannot_onboard_but_reads_network_diagnostics() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client =
        Client::tracked(onboarding_ops_rocket(verifiers).await).await.unwrap();
    let read_token = || {
        rocket::http::Header::new(ASSERTION_HEADER, token(&key, READ_AUDIENCE))
    };

    for path in ["/ops/debug/accounts", "/ops/debug/tokenized-assets"] {
        let response = client
            .post(path)
            .header(rocket::http::ContentType::JSON)
            .header(read_token())
            .body("{}")
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized, "{path}");
    }

    let whitelist = client
        .post(format!("/ops/debug/accounts/{REQUEST_ID}/wallets"))
        .header(rocket::http::ContentType::JSON)
        .header(read_token())
        .body("{}")
        .dispatch()
        .await;
    assert_eq!(whitelist.status(), Status::Unauthorized);

    let unwhitelist = client
        .delete(format!("/ops/debug/accounts/{REQUEST_ID}/wallets/{WALLET}"))
        .header(read_token())
        .dispatch()
        .await;
    assert_eq!(unwhitelist.status(), Status::Unauthorized);

    let detail = client
        .get("/ops/debug/tokenized-assets/AAPL?network=base")
        .header(read_token())
        .dispatch()
        .await;
    assert_eq!(detail.status(), Status::Unauthorized);

    let telemetry = client
        .get("/ops/read/network-telemetry")
        .header(read_token())
        .dispatch()
        .await;
    assert_eq!(telemetry.status(), Status::Ok);

    let transfers = client
        .get("/ops/read/wrapped-transfers")
        .header(read_token())
        .dispatch()
        .await;
    assert_eq!(transfers.status(), Status::Ok);

    assert!(logs_contain_at!(
        Level::WARN,
        &["IAP assertion failed validation"]
    ));
    assert!(logs_contain_at!(Level::INFO, &["IAP assertion accepted"]));
}
