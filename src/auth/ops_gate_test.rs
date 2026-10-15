//! Route-level tests for the IAP-gated operator API: the tier guards on probe
//! routes (missing assertion, own-tier acceptance, cross-tier rejection) and
//! the real `/ops/*` handlers (gated when mounted, absent when unconfigured).

use alloy::primitives::{Address, B256, address};
use apalis_sqlite::SqlitePool as ApalisSqlitePool;
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL;
use event_sorcery::{Store, StoreBuilder};
use httpmock::prelude::*;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use p256::ecdsa::SigningKey;
use p256::pkcs8::EncodePrivateKey;
use rocket::fairing::AdHoc;
use rocket::http::{Accept, ContentType, MediaType, Method, Status};
use rocket::local::asynchronous::Client;
use serde::Serialize;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite};
use st0x_alpaca::issuer::mock::MockIssuerApi;
use st0x_issuance_dto::{
    BurnExcessExternalRequest, TokenizedAssetDetailResponse,
    TokenizedAssetStatus,
};
use st0x_issuance_ops::test_support::{
    Client as OpsClient, CommandError, TransportError as OpsTransportError,
    command_cases, dispatch_args,
};
use st0x_rpc::JwtEnv;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex as StdMutex};
use tracing::Level;
use tracing_test::traced_test;
use url::Url;

use super::iap::ASSERTION_HEADER;
use super::{
    BreakglassOps, CapitalOps, DebugOps, FailedAuthRateLimiter,
    OpsApiVerifiers, ReadOps,
};
use crate::account::view::find_by_client_id;
use crate::account::{
    Account, AccountCommand, AccountView, AlpacaAccountNumber, ClientId,
    RegisterAccountResponse, WhitelistWalletResponse,
};
use crate::admin::RedemptionBurnRecovery;
use crate::chain::{ChainConfig, ConfiguredNetworks, RpcEndpoint};
use crate::config::{Config, OpsApiConfig};
use crate::mint::Mint;
use crate::network_telemetry::NetworkTelemetry;
use crate::operations_audit::OperationsAuditFairing;
use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
use crate::redemption::burn_manager::{
    BurnManagerError, ManualBurnReplacementOutcome, RecoveryOutcome,
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
    let now = unix_now();
    signed_token(key, audience, now, now + 300)
}

/// A token whose lifetime ended five minutes ago, past the verifier's leeway.
fn expired_token(key: &TestKey, audience: &str) -> String {
    let now = unix_now();
    signed_token(key, audience, now - 600, now - 300)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after epoch")
        .as_secs()
}

fn signed_token(
    key: &TestKey,
    audience: &str,
    issued_at: u64,
    expires_at: u64,
) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(TEST_KID.to_string());

    encode(
        &header,
        &TestClaims {
            sub: "accounts.google.com:1234".to_string(),
            email: "operator@rainlang.xyz".to_string(),
            aud: audience.to_string(),
            iss: IAP_ISSUER.to_string(),
            iat: issued_at,
            exp: expires_at,
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

/// One representative route per tier, which a deployment without ops API
/// audiences must not serve. Each request matches its route's method and
/// format, so a 404 means the tier is absent, not that the request missed.
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
/// with `config` deciding whether the `/ops/*` routes are mounted.
async fn app_rocket(config: Config) -> rocket::Rocket<rocket::Build> {
    let ops_verifiers =
        crate::build_ops_verifiers(&config).expect("JWKS client builds");
    app_rocket_with_verifiers(config, ops_verifiers).await
}

/// [`app_rocket`] with the ops verifiers supplied, so a test can point them at
/// a mocked key set. Every managed state the ops routes declare is supplied
/// (Rocket refuses to ignite a route whose `State` is unmanaged): the stores
/// `setup_test_rocket` manages, whose tokenized asset store lists AAPL and
/// TSLA on Base, and empty redemption, receipt inventory, and underlying
/// stores.
async fn app_rocket_with_verifiers(
    config: Config,
    ops_verifiers: Option<OpsApiVerifiers>,
) -> rocket::Rocket<rocket::Build> {
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
        ops_verifiers,
        freeze_scheduler: FreezeScheduler::new(&apalis_pool, pool.clone()),
        rate_limiter: FailedAuthRateLimiter::new().expect("rate limiter"),
        config,
        pool,
        apalis_pool,
        account_store,
        tokenized_asset_store,
        mint_store,
        redemption_store,
        alpaca_service: Arc::new(MockIssuerApi::new_success()),
        burn_recovery: Arc::new(AbsentRedemptionBurnRecovery),
        vault_services,
        configured_networks: ConfiguredNetworks::from_iter([Network::Base]),
        receipts: Arc::new(CqrsReceiptService::new(receipt_inventory)),
        network_telemetry: Arc::new(NetworkTelemetry::new([Network::Base])),
        underlying_store,
        store_guard: None,
        background_tasks: crate::BackgroundTasks {
            shutdown,
            handles: Vec::new(),
        },
    })
}

/// Without configured audiences the `/ops/*` routes are not mounted at all: a
/// deployment with no load balancer serves 404, never a 401 that suggests the
/// path exists and wants credentials. Checked over the whole route table, so a
/// route moved out of the configured mount fails here.
#[tokio::test]
async fn role_prefixes_are_absent_without_ops_api_config() {
    let config = test_config().expect("test config builds");
    let rocket = app_rocket(config).await;
    let exposed: Vec<String> = rocket
        .routes()
        .filter(|route| ops_tier_segment(route.uri.path()).is_some())
        .map(route_identity)
        .collect();
    assert!(exposed.is_empty(), "mounted without ops API config: {exposed:#?}");
    let client = Client::tracked(rocket).await.unwrap();

    for (method, path) in TIER_ROUTES {
        assert_eq!(
            probe_tier_route(&client, method, path).await,
            Status::NotFound,
            "{path} must be absent"
        );
    }
}

/// Every mutation route the application mounts is audited, which the echoed
/// `x-request-id` proves, unless it is one of the integration calls Alpaca or
/// the liquidity bot make as part of the mint and account-link flows. Drives
/// the real route table, so a new operator mutation mounted outside an
/// audited surface fails here. An accepted shared-key call proves the real
/// `InternalAuth` guard attributes its event.
#[traced_test]
#[tokio::test]
async fn every_mounted_operator_mutation_is_audited() {
    let config = Config {
        ops_api: Some(ops_config()),
        ..test_config().expect("test config builds")
    };
    let client = Client::tracked(app_rocket(config).await).await.unwrap();
    let mutations: Vec<(Method, String)> = client
        .rocket()
        .routes()
        .filter(|route| {
            matches!(
                route.method,
                Method::Post | Method::Put | Method::Patch | Method::Delete
            )
        })
        .map(|route| (route.method, concrete_target(route)))
        .collect();

    let mut unaudited = Vec::new();
    for (method, path) in mutations {
        let response = client
            .req(method, path.as_str())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        if response.headers().get_one("x-request-id").is_none() {
            unaudited.push(format!("{method} {path}"));
        }
    }
    unaudited.sort();

    assert_eq!(
        unaudited,
        [
            "POST /accounts/connect",
            "POST /inkind/issuance",
            "POST /inkind/issuance/confirm",
            "POST /internal/mints/00000000-0000-0000-0000-000000000000/authorization",
        ]
    );

    let accepted = client
        .post("/admin/reprocess/mint/not-a-mint")
        .header(rocket::http::Header::new("X-API-KEY", API_KEY))
        .header(rocket::http::Header::new(
            "x-request-id",
            "efefefef-efef-efef-efef-efefefefefef",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .dispatch()
        .await;
    assert_ne!(accepted.status(), Status::Unauthorized);
    assert_ne!(accepted.status(), Status::Forbidden);
    logs_assert(|lines| {
        lines
            .iter()
            .any(|line| {
                line.contains("operations_audit: Operations audit event")
                    && line.contains("efefefef-efef-efef-efef-efefefefefef")
                    && line.contains("principal=\"internal-api-key\"")
                    && line.contains("role=\"internal\"")
                    && line.contains(
                        "route=\"/admin/reprocess/mint/<aggregate_id>\"",
                    )
            })
            .then_some(())
            .ok_or_else(|| format!("missing internal audit event: {lines:?}"))
    });
}

/// With audiences configured the application mounts every tier under its
/// role prefix and gates it: a bare request reaches the guard and is refused
/// with 401, not 404. This drives the production chain from `Config` through
/// `build_ops_verifiers` to the mounting branch in `build_rocket`, which the
/// route walks below bypass to read a mocked key set.
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

/// Audience each tier's IAP backend stamps, keyed by the tier segment of its
/// `/ops/<tier>/` prefix.
const TIER_AUDIENCES: [(&str, &str); 4] = [
    ("read", READ_AUDIENCE),
    ("debug", DEBUG_AUDIENCE),
    ("capital", CAPITAL_AUDIENCE),
    ("breakglass", BREAKGLASS_AUDIENCE),
];

fn tier_audience(tier: &str) -> Option<&'static str> {
    TIER_AUDIENCES
        .iter()
        .find(|(name, _)| *name == tier)
        .map(|(_, audience)| *audience)
}

/// The segment after `/ops` in a route path, or `None` for a route outside
/// `/ops`.
fn ops_tier_segment(path: &str) -> Option<&str> {
    let mut segments = path.split('/').filter(|segment| !segment.is_empty());
    (segments.next() == Some("ops"))
        .then(|| segments.next().unwrap_or_default())
}

/// An `IssuerRedemptionRequestId`: the transaction hash that started the
/// redemption.
const REDEMPTION_ID: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";

/// A value each path or query parameter of the mounted routes parses as.
/// Rocket runs a route's request guards before its path and query guards, so
/// a value the route cannot parse still meets the auth guard but then
/// forwards instead of entering the handler, which the walks report. A new
/// parameter name fails here until it has a sample.
fn sample_parameter(name: &str) -> &str {
    match name {
        "aggregate_id" | "client_id" | "tokenization_request_id" => REQUEST_ID,
        "issuer_request_id" => REDEMPTION_ID,
        "underlying" | "asset" => "AAPL",
        "network" | "before_network" => "base",
        "wallet" => WALLET,
        "aggregate_type" => "Mint",
        "limit" | "before_block" | "before_log_index" => "1",
        // The swagger UI's catch all tail.
        "_.." => "index.html",
        other => panic!("no sample value for route parameter {other}"),
    }
}

/// The route's path and query with every parameter replaced by its sample.
fn concrete_target(route: &rocket::Route) -> String {
    let path = route
        .uri
        .path()
        .split('/')
        .map(|segment| {
            segment
                .strip_prefix('<')
                .and_then(|name| name.strip_suffix('>'))
                .map_or(segment, sample_parameter)
        })
        .collect::<Vec<_>>()
        .join("/");
    let Some(query) = route.uri.query() else {
        return path;
    };
    let query = query
        .split('&')
        .map(|parameter| {
            let name = parameter.trim_start_matches('<').trim_end_matches('>');
            format!("{name}={}", sample_parameter(name))
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{query}")
}

/// Names a route uniquely among the mounted ones: Rocket lets two routes share
/// a method and path when their formats or ranks differ.
fn route_identity(route: &rocket::Route) -> String {
    let format = route
        .format
        .as_ref()
        .map_or_else(|| "any".to_owned(), ToString::to_string);
    format!(
        "{} {} rank {} format {format}",
        route.method, route.uri, route.rank
    )
}

/// A mounted route a walk probes, with a request target its path and query
/// guards accept and the media type its format matches.
struct WalkedRoute {
    identity: String,
    method: Method,
    path: String,
    target: String,
    format: MediaType,
}

/// Answers a request a walked route forwarded because a path or query value
/// did not parse. Mounted below each walked route, so a forwarded probe is
/// answered here and records this route instead of the walked one.
#[derive(Clone)]
struct ForwardSentinel;

#[rocket::async_trait]
impl rocket::route::Handler for ForwardSentinel {
    async fn handle<'r>(
        &self,
        request: &'r rocket::Request<'_>,
        _: rocket::Data<'r>,
    ) -> rocket::route::Outcome<'r> {
        rocket::route::Outcome::from(request, Status::ImATeapot)
    }
}

/// Identity of the route that answered each request, `None` when the request
/// matched no route.
type AnsweredRoutes = Arc<StdMutex<Vec<Option<String>>>>;

/// Prepares `rocket` for a route walk over the routes `walk` selects: lists
/// them, mounts a [`ForwardSentinel`] below each, and records into `answered`
/// which route answered every request.
fn instrument(
    rocket: rocket::Rocket<rocket::Build>,
    answered: &AnsweredRoutes,
    walk: impl Fn(&rocket::Route) -> bool,
) -> (rocket::Rocket<rocket::Build>, Vec<WalkedRoute>) {
    let selected: Vec<rocket::Route> =
        rocket.routes().filter(|route| walk(route)).cloned().collect();
    let mut sentinels: Vec<rocket::Route> = Vec::new();
    for route in &selected {
        let duplicate = sentinels.iter().any(|sentinel| {
            sentinel.method == route.method
                && sentinel.uri.as_str() == route.uri.as_str()
                && sentinel.format == route.format
        });
        if !duplicate {
            let mut sentinel = rocket::Route::ranked(
                100,
                route.method,
                route.uri.as_str(),
                ForwardSentinel,
            );
            sentinel.format.clone_from(&route.format);
            sentinels.push(sentinel);
        }
    }
    let walked = selected
        .iter()
        .map(|route| WalkedRoute {
            identity: route_identity(route),
            method: route.method,
            path: route.uri.path().to_owned(),
            target: concrete_target(route),
            format: route.format.clone().unwrap_or(MediaType::JSON),
        })
        .collect();

    let recorder = Arc::clone(answered);
    let rocket = rocket.mount("/", sentinels).attach(AdHoc::on_response(
        "answering route recorder",
        move |request, _| {
            recorder
                .lock()
                .expect("route recorder is not poisoned")
                .push(request.route().map(route_identity));
            Box::pin(async {})
        },
    ));
    (rocket, walked)
}

/// The application rocket with every tier mounted, its verifiers reading the
/// key set `jwks` serves.
async fn gated_app_rocket(jwks: &MockServer) -> rocket::Rocket<rocket::Build> {
    let config = Config {
        ops_api: Some(ops_config()),
        ..test_config().expect("test config builds")
    };
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    app_rocket_with_verifiers(config, Some(verifiers)).await
}

/// Sends `route` its sample request from `remote` with `headers` added, in the
/// media type its format matches and with a `{}` body on a mutation. Returns
/// the status and the identity of the route that answered.
async fn probe(
    client: &Client,
    answered: &AnsweredRoutes,
    route: &WalkedRoute,
    remote: SocketAddr,
    headers: Vec<rocket::http::Header<'static>>,
) -> (Status, Option<String>) {
    let mut request = client
        .req(route.method, route.target.as_str())
        .remote(remote)
        .header(ContentType(route.format.clone()))
        .header(Accept::from(route.format.clone()));
    for header in headers {
        request = request.header(header);
    }
    if route.method != Method::Get {
        request = request.body("{}");
    }
    let status = request.dispatch().await.status();
    let answered_by = answered
        .lock()
        .expect("route recorder is not poisoned")
        .pop()
        .flatten();
    (status, answered_by)
}

fn assertion(token: String) -> Vec<rocket::http::Header<'static>> {
    vec![rocket::http::Header::new(ASSERTION_HEADER, token)]
}

/// The shared key `test_config` sets for `InternalAuth` and `IssuerAuth`.
const API_KEY: &str = "test-key-12345678901234567890123456";

const LOOPBACK: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000);

/// Walks the real route table: every route mounted under `/ops` sits under one
/// of the four tier prefixes, and its guard is that tier's. The tier's own
/// assertion passes the tier guard (only a guard answers 401), while a missing
/// assertion, an expired one, and each other tier's are refused with 401. A
/// handler taking another tier's guard than its prefix names fails here.
#[traced_test]
#[tokio::test]
async fn every_ops_route_admits_only_its_own_tier() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let answered = AnsweredRoutes::default();
    let (rocket, routes) =
        instrument(gated_app_rocket(&jwks).await, &answered, |route| {
            ops_tier_segment(route.uri.path()).is_some()
        });
    let client = Client::tracked(rocket).await.unwrap();

    for (tier, _) in TIER_AUDIENCES {
        assert!(
            routes
                .iter()
                .any(|route| ops_tier_segment(&route.path) == Some(tier)),
            "no route mounted under /ops/{tier}"
        );
    }

    let mut failures = Vec::new();
    for route in &routes {
        let identity = &route.identity;
        let Some(own) = ops_tier_segment(&route.path).and_then(tier_audience)
        else {
            failures.push(format!("{identity}: outside the tiers"));
            continue;
        };

        let mut probes = vec![
            ("its own assertion".to_owned(), assertion(token(&key, own)), true),
            ("no assertion".to_owned(), Vec::new(), false),
            (
                "an expired assertion".to_owned(),
                assertion(expired_token(&key, own)),
                false,
            ),
        ];
        probes.extend(
            TIER_AUDIENCES.iter().filter(|(_, audience)| *audience != own).map(
                |(other, audience)| {
                    (
                        format!("a {other} assertion"),
                        assertion(token(&key, audience)),
                        false,
                    )
                },
            ),
        );
        for (label, headers, admitted) in probes {
            let (status, answered_by) =
                probe(&client, &answered, route, LOOPBACK, headers).await;
            if answered_by.as_ref() != Some(identity) {
                failures.push(format!(
                    "{identity}: {label} was answered by {answered_by:?}"
                ));
            } else if admitted == (status == Status::Unauthorized) {
                failures.push(format!("{identity}: {label} got {status}"));
            }
        }
    }

    assert!(failures.is_empty(), "{failures:#?}");
    assert!(logs_contain_at!(Level::INFO, &["IAP assertion accepted"]));
    assert!(logs_contain_at!(
        Level::WARN,
        &["Request carries no IAP assertion"]
    ));
    assert!(logs_contain_at!(
        Level::WARN,
        &["IAP assertion failed validation"]
    ));
}

/// Walks the real route table: every route mounted outside `/ops`, read or
/// mutation, is guarded by the shared API key and an address check. Each is
/// `InternalAuth` except Alpaca's four calls, which are `IssuerAuth`, and the
/// API docs, which a development build serves to anyone (production does not
/// mount them). A route mounted with neither guard fails here.
#[traced_test]
#[tokio::test]
async fn every_route_outside_ops_is_internal_an_alpaca_call_or_the_api_docs() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let answered = AnsweredRoutes::default();
    let (rocket, routes) =
        instrument(gated_app_rocket(&jwks).await, &answered, |route| {
            ops_tier_segment(route.uri.path()).is_none()
        });
    let client = Client::tracked(rocket).await.unwrap();
    let api_key = || vec![rocket::http::Header::new("X-API-KEY", API_KEY)];
    // Inside the test config's internal range but outside its Alpaca range.
    let internal = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 1);
    assert!(!routes.is_empty());

    let mut issuer = Vec::new();
    let mut unguarded = Vec::new();
    let mut other = Vec::new();
    for (index, route) in (1u8..).zip(&routes) {
        // A distinct public address per route, so the failed authentication
        // rate limit never answers in place of the guard.
        let public =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, index)), 1);
        let mut statuses = [0; 4];
        for (slot, (remote, headers)) in statuses.iter_mut().zip([
            (LOOPBACK, Vec::new()),
            (internal, api_key()),
            (public, api_key()),
            (LOOPBACK, api_key()),
        ]) {
            let (status, answered_by) =
                probe(&client, &answered, route, remote, headers).await;
            if answered_by.as_ref() != Some(&route.identity) {
                other.push(format!(
                    "{}: from {remote} was answered by {answered_by:?}",
                    route.identity
                ));
            }
            *slot = status.code;
        }

        // 401 and 403 are the guards' refusals, 429 their rate limit.
        let passed = |status: u16| !matches!(status, 401 | 403 | 429);
        match statuses {
            // `InternalAuth`: the key passes from an internal address and is
            // refused from a public one.
            [401, internal, 403, _] if passed(internal) => {}
            // `IssuerAuth`: the key passes only from Alpaca's address.
            [401, 403, 403, alpaca] if passed(alpaca) => {
                issuer.push(format!("{} {}", route.method, route.target));
            }
            statuses if statuses.into_iter().all(passed) => {
                unguarded.push(format!("{} {}", route.method, route.target));
            }
            statuses => other.push(format!(
                "{}: without a key, from an internal, a public, and Alpaca's \
                 address: {statuses:?}",
                route.identity
            )),
        }
    }

    assert!(other.is_empty(), "{other:#?}");
    issuer.sort();
    assert_eq!(
        issuer,
        [
            "GET /tokenized-assets",
            "POST /accounts/connect",
            "POST /inkind/issuance",
            "POST /inkind/issuance/confirm",
        ]
    );
    unguarded.sort();
    assert_eq!(
        unguarded,
        ["GET /api-docs/openapi.json", "GET /swagger-ui/index.html"]
    );
    assert!(logs_contain_at!(Level::WARN, &["Missing X-API-KEY header"]));
    assert!(logs_contain_at!(Level::WARN, &["IP not whitelisted"]));
}

/// Serves `rocket` on an ephemeral loopback port behind a stand in for the
/// load balancer, which turns the client's bearer token into the IAP assertion
/// header as IAP does.
async fn serve_behind_iap(
    rocket: rocket::Rocket<rocket::Build>,
) -> (Url, tokio::task::JoinHandle<()>) {
    let (port_sender, port) = tokio::sync::oneshot::channel();
    let figment = rocket
        .figment()
        .clone()
        .merge((rocket::Config::ADDRESS, Ipv4Addr::LOCALHOST))
        .merge((rocket::Config::PORT, 0));
    let rocket = rocket
        .configure(figment)
        .attach(AdHoc::on_request("IAP stand in", |request, _| {
            let bearer = request
                .headers()
                .get_one("Authorization")
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::to_owned);
            if let Some(token) = bearer {
                request.add_header(rocket::http::Header::new(
                    ASSERTION_HEADER,
                    token,
                ));
            }
            Box::pin(async {})
        }))
        .attach(AdHoc::on_liftoff("report port", move |rocket| {
            // The server binds port 0 and records the port it got before
            // liftoff.
            port_sender.send(rocket.config().port).expect("test awaits port");
            Box::pin(async {})
        }));

    let server = tokio::spawn(async move {
        if let Err(error) = rocket.launch().await {
            panic!("test server failed: {error}");
        }
    });
    let port = port.await.expect("server lifts off");
    let base_url =
        Url::parse(&format!("http://127.0.0.1:{port}")).expect("valid URL");
    (base_url, server)
}

/// Query keys of `request` that the route answering it does not declare.
/// Rocket ignores such a key, so the handler runs with that parameter unset.
fn undeclared_query_keys(request: &rocket::Request<'_>) -> Vec<String> {
    let declared: BTreeSet<&str> = request
        .route()
        .and_then(|route| route.uri.query())
        .into_iter()
        .flat_map(|query| query.split('&'))
        .map(|parameter| {
            parameter.trim_start_matches('<').trim_end_matches('>')
        })
        .collect();
    request
        .uri()
        .query()
        .into_iter()
        .flat_map(|query| query.segments())
        .filter(|(key, _)| !declared.contains(key))
        .map(|(key, _)| key.to_owned())
        .collect()
}

/// Drives every `st0x-issuance-client` command through the real router over
/// HTTP. With its own tier's token each command is answered by a route under
/// its own `/ops/<tier>/` prefix that declares every query key it sends,
/// refused by no guard and forwarded by no path or query value, and with each
/// other tier's token it is refused. Together the commands cover every `/ops`
/// route exactly once, so a client path or parameter the server does not
/// serve, or a route no command calls, fails here.
#[traced_test]
#[tokio::test]
async fn every_client_command_reaches_a_route_of_its_own_tier() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let answered = AnsweredRoutes::default();
    let (rocket, routes) =
        instrument(gated_app_rocket(&jwks).await, &answered, |route| {
            ops_tier_segment(route.uri.path()).is_some()
        });
    let undeclared = Arc::new(StdMutex::new(Vec::<String>::new()));
    let recorder = Arc::clone(&undeclared);
    let rocket = rocket.attach(AdHoc::on_response(
        "undeclared query key recorder",
        move |request, _| {
            recorder
                .lock()
                .expect("query key recorder is not poisoned")
                .extend(undeclared_query_keys(request));
            Box::pin(async {})
        },
    ));
    let (base_url, server) = serve_behind_iap(rocket).await;
    let client = |audience: &str| {
        OpsClient::new(base_url.clone(), token(&key, audience))
            .expect("client builds")
    };
    let cases = command_cases();
    let mut covered = BTreeSet::new();

    for case in &cases {
        let own = tier_audience(case.tier)
            .unwrap_or_else(|| panic!("{case:?}: unknown tier"));
        answered.lock().expect("route recorder is not poisoned").clear();
        undeclared.lock().expect("query key recorder is not poisoned").clear();

        match dispatch_args(&client(own), &case.args).await {
            Ok(_)
            | Err(CommandError::Transport(
                OpsTransportError::NotFound { .. }
                | OpsTransportError::Unavailable { .. }
                | OpsTransportError::OutcomeUnknown { .. }
                | OpsTransportError::Http { .. }
                | OpsTransportError::Decode { .. },
            )) => {}
            Err(error) => panic!("{case:?} did not reach its handler: {error}"),
        }
        let requests = std::mem::take(
            &mut *answered.lock().expect("route recorder is not poisoned"),
        );
        let [Some(identity)] = requests.as_slice() else {
            panic!(
                "{case:?} must send one request to a route, sent {requests:?}"
            );
        };
        let ignored = std::mem::take(
            &mut *undeclared
                .lock()
                .expect("query key recorder is not poisoned"),
        );
        assert!(
            ignored.is_empty(),
            "{case:?} sent query keys {identity} does not declare: {ignored:?}"
        );
        let Some(route) =
            routes.iter().find(|route| route.identity == *identity)
        else {
            panic!("{case:?} was answered by {identity}, not an /ops handler");
        };
        assert_eq!(
            ops_tier_segment(&route.path),
            Some(case.tier),
            "{case:?} called {identity}"
        );
        assert!(
            covered.insert(identity.clone()),
            "{case:?} called {identity}, which another command calls"
        );

        for (other, audience) in TIER_AUDIENCES {
            if audience == own {
                continue;
            }
            match dispatch_args(&client(audience), &case.args).await {
                Err(CommandError::Transport(
                    OpsTransportError::Unauthorized { .. },
                )) => {}
                result => {
                    panic!(
                        "{case:?} must refuse a {other} token, got {result:?}"
                    )
                }
            }
        }
    }

    assert_eq!(
        covered,
        routes.iter().map(|route| route.identity.clone()).collect(),
        "commands must cover every /ops route"
    );
    server.abort();
    assert!(logs_contain_at!(Level::INFO, &["IAP assertion accepted"]));
    assert!(logs_contain_at!(
        Level::WARN,
        &["IAP assertion failed validation"]
    ));
}

/// A well-formed external burn body for `network`; the mint it names does not
/// exist in an empty store.
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

/// A request for the wrong configured chain is rejected before the external
/// route runs the engine, on a line the client's log link can find.
#[traced_test]
#[tokio::test]
async fn external_burn_rejects_a_mismatched_chain_id_and_logs_the_request() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));

    let mut config = test_config().expect("test config builds");
    config.chains = vec![ChainConfig {
        network: Network::Base,
        chain_id: 31337,
        rpc: RpcEndpoint::new(
            Url::parse("wss://localhost:8545").expect("valid url"),
            &JwtEnv::Unset,
        )
        .expect("plain url"),
        backfill_start_block: 0,
        low_gas_threshold: None,
    }];
    let rocket = setup_test_rocket_with_config(config)
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .mount(
            "/",
            rocket::routes![crate::burn_excess::api::burn_excess_external_ops],
        );
    let client = Client::tracked(rocket).await.unwrap();

    let response = client
        .post("/ops/breakglass/burn-excess/external")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, BREAKGLASS_AUDIENCE),
        ))
        .body(external_body(Network::Base))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::UnprocessableEntity);
    // The client's Cloud Logging link searches for the issuer request id, so
    // the refusal's cause must sit on a line that carries it.
    logs_assert(|lines: &[&str]| {
        if lines.iter().any(|line| {
            line.contains("WARN")
                && line.contains("admin:")
                && line.contains("chain_id does not match the configured chain")
                && line.contains(
                    "issuer_request_id=00000000-0000-0000-0000-000000000000",
                )
        }) {
            Ok(())
        } else {
            Err("no WARN line naming the chain mismatch and the request"
                .to_owned())
        }
    });
}

/// The live external route runs beside the redemption poller, so a stream
/// that never recorded its funding expectation has nothing keeping the poller
/// off the funding Transfer: it is refused with 409 before anything is read
/// from chain, and the log says what to do.
#[traced_test]
#[tokio::test]
async fn external_burn_without_a_funding_expectation_is_refused() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));

    // The default test signer is the zero key, which signer resolution
    // refuses before the engine runs; a real (nonzero) key lets the request
    // reach the engine. No node listens on the RPC, so a 409 also proves the
    // refusal comes before any chain read.
    let mut config = test_config().expect("test config builds");
    config.chains = vec![ChainConfig {
        network: Network::Base,
        chain_id: Network::Base.chain_id(),
        rpc: RpcEndpoint::new(
            Url::parse("wss://localhost:8545").expect("valid url"),
            &JwtEnv::Unset,
        )
        .expect("plain url"),
        backfill_start_block: 0,
        low_gas_threshold: None,
    }];
    config.signer = SignerConfig::Local(B256::repeat_byte(7));
    let rocket = setup_test_rocket_with_config(config)
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .mount(
            "/",
            rocket::routes![crate::burn_excess::api::burn_excess_external_ops],
        );
    let client = Client::tracked(rocket).await.unwrap();

    let body = external_body(Network::Base);
    assert!(
        serde_json::from_str::<BurnExcessExternalRequest>(&body).is_ok(),
        "the test body must be well-formed, so the 409 is the engine's \
         refusal, not a request-parse rejection"
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

    assert_eq!(response.status(), Status::Conflict);
    logs_assert(|lines: &[&str]| {
        if lines.iter().any(|line| {
            line.contains("ERROR")
                && line.contains("admin:")
                && line.contains("burn-excess failed")
                && line.contains("expect-funding")
                && line.contains(
                    "issuer_request_id=00000000-0000-0000-0000-000000000000",
                )
        }) {
            Ok(())
        } else {
            Err("no ERROR line naming the missing expectation and the request"
                .to_owned())
        }
    });
}

/// A legacy flat-Base deployment runs a non-canonical chain id (a local Anvil
/// here), so burn-excess validates the operator-supplied `chain_id` against the
/// configured chain entry, not the hardcoded `Network::chain_id()`. The request
/// matching the configured id reaches the engine (404 on the absent mint); the
/// one sending the canonical 8453, which the old canonical check accepted, is
/// now refused with 422.
#[traced_test]
#[tokio::test]
async fn internal_burn_validates_chain_id_against_the_configured_chain() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));

    let anvil_chain_id = 31337u64;
    let mut config = test_config().expect("test config builds");
    config.chains = vec![ChainConfig {
        network: Network::Base,
        chain_id: anvil_chain_id,
        rpc: RpcEndpoint::new(
            Url::parse("wss://localhost:8545").expect("valid url"),
            &JwtEnv::Unset,
        )
        .expect("plain url"),
        backfill_start_block: 0,
        low_gas_threshold: None,
    }];
    config.signer = SignerConfig::Local(B256::repeat_byte(7));
    let rocket = setup_test_rocket_with_config(config)
        .await
        .expect("test rocket builds")
        .manage(verifiers)
        .mount(
            "/",
            rocket::routes![crate::burn_excess::api::burn_excess_internal_ops],
        );
    let client = Client::tracked(rocket).await.unwrap();

    let internal_body = |chain_id: u64| {
        serde_json::json!({
            "issuer_request_id": "00000000-0000-0000-0000-000000000000",
            "deposit_tx_hash": format!("0x{}", "00".repeat(32)),
            "receipt_id": "0x1",
            "shares": "1.0",
            "reason": "chain id validation test",
            "network": Network::Base,
            "chain_id": chain_id,
        })
        .to_string()
    };

    // Matches the configured (Anvil) chain id: passes the chain gate and
    // reaches the engine, which 404s on the absent mint (no node listens).
    let matched = client
        .post("/ops/breakglass/burn-excess/internal")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, BREAKGLASS_AUDIENCE),
        ))
        .body(internal_body(anvil_chain_id))
        .dispatch()
        .await;
    assert_eq!(
        matched.status(),
        Status::NotFound,
        "a chain_id matching the configured chain must reach the engine"
    );

    // The canonical 8453 disagrees with the configured Anvil id, so it is
    // refused with 422; the old canonical check would have accepted it.
    let mismatched = client
        .post("/ops/breakglass/burn-excess/internal")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(&key, BREAKGLASS_AUDIENCE),
        ))
        .body(internal_body(Network::Base.chain_id()))
        .dispatch()
        .await;
    assert_eq!(mismatched.status(), Status::UnprocessableEntity);
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
/// span, and the audit event carries it as its principal, so an audit can
/// attribute the action from the service logs alone.
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
    let client =
        Client::tracked(rocket.attach(OperationsAuditFairing)).await.unwrap();

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

        let audited = lines.iter().any(|line| {
            line.contains("operations_audit: Operations audit event")
                && line.contains("principal=\"accounts.google.com:1234\"")
                && line.contains("role=\"capital\"")
                && line.contains("outcome=\"success\"")
        });
        if !audited {
            return Err(format!("missing attributed audit event: {lines:?}"));
        }
        if lines.iter().any(|line| {
            line.contains("Operations audit event")
                && line.contains("operator@rainlang.xyz")
        }) {
            return Err("audit event carries the email".to_string());
        }
        Ok(())
    });
}

/// Burn recovery over the empty redemption store. Only `force_complete_burn`
/// is reached for an unknown redemption, since the recover route answers 404
/// from the event store first; it refuses as `BurnManager::force_complete_burn`
/// does for a redemption that was never initialized, and the other two methods
/// return the same error.
struct AbsentRedemptionBurnRecovery;

fn uninitialized() -> BurnManagerError {
    BurnManagerError::InvalidAggregateState {
        current_state: "Uninitialized".to_string(),
    }
}

#[async_trait]
impl RedemptionBurnRecovery for AbsentRedemptionBurnRecovery {
    async fn replace_exhausted_dead_burn(
        &self,
        _: &IssuerRedemptionRequestId,
    ) -> Result<ManualBurnReplacementOutcome, BurnManagerError> {
        Err(uninitialized())
    }

    async fn execute_recovered_burn(
        &self,
        _: &IssuerRedemptionRequestId,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        Err(uninitialized())
    }

    async fn force_complete_burn(
        &self,
        _: &IssuerRedemptionRequestId,
        _: B256,
        _: String,
        _: Option<B256>,
    ) -> Result<BurnVerification, BurnManagerError> {
        Err(uninitialized())
    }
}

const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";

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

/// Registers the test operator account through the debug twin. A named
/// function rather than a closure because the response borrows the client and
/// a closure cannot spell that lifetime.
async fn register_operator_account<'client>(
    client: &'client Client,
    key: &TestKey,
) -> rocket::local::asynchronous::LocalResponse<'client> {
    client
        .post("/ops/debug/accounts")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            ASSERTION_HEADER,
            token(key, DEBUG_AUDIENCE),
        ))
        .body(r#"{"email":"operator@example.com"}"#)
        .dispatch()
        .await
}

/// A debug token runs the onboarding twins end to end, each step's effect
/// visible to the next: register an account (a repeat is refused, so the email
/// claim persisted), link it, whitelist and unwhitelist a wallet (the account
/// view carries then drops it), add an asset, and read its detail back with
/// the full response contract. The twins are wired to the real handlers.
#[traced_test]
#[tokio::test]
async fn a_debug_token_onboards_an_account_and_lists_an_asset() {
    let key = test_key();
    let jwks = jwks_server(&key);
    let verifiers =
        OpsApiVerifiers::with_jwks_url(&ops_config(), &jwks.url("/keys"));
    let client =
        Client::tracked(onboarding_ops_rocket(verifiers).await).await.unwrap();
    let debug_token = || {
        rocket::http::Header::new(ASSERTION_HEADER, token(&key, DEBUG_AUDIENCE))
    };
    let wallet: Address =
        address!("0x0000000000000000000000000000000000000001");
    let vault: Address = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    let registered = register_operator_account(&client, &key).await;
    assert_eq!(registered.status(), Status::Ok);
    let client_id: ClientId = registered
        .into_json::<RegisterAccountResponse>()
        .await
        .expect("registration response body")
        .client_id;

    // The email claim persisted: registering it again is refused.
    let repeat = register_operator_account(&client, &key).await;
    assert_eq!(repeat.status(), Status::Conflict);

    // Linking is Alpaca's ITN step, not an operator verb, so it happens through
    // the store; whitelisting requires a linked account.
    let rocket = client.rocket();
    let account_store =
        rocket.state::<Arc<Store<Account>>>().expect("account store managed");
    let pool = rocket.state::<Pool<Sqlite>>().expect("pool managed");
    account_store
        .send(
            &client_id,
            AccountCommand::LinkToAlpaca {
                alpaca_account: AlpacaAccountNumber("ALP-1".to_string()),
            },
        )
        .await
        .expect("link succeeds");

    let whitelisted = client
        .post(format!("/ops/debug/accounts/{client_id}/wallets"))
        .header(rocket::http::ContentType::JSON)
        .header(debug_token())
        .body(format!(r#"{{"wallet":"{wallet}"}}"#))
        .dispatch()
        .await;
    assert_eq!(whitelisted.status(), Status::Ok);
    assert!(
        whitelisted
            .into_json::<WhitelistWalletResponse>()
            .await
            .expect("whitelist response body")
            .success
    );
    let Some(AccountView::LinkedToAlpaca { whitelisted_wallets, .. }) =
        find_by_client_id(pool, &client_id).await.expect("view loads")
    else {
        panic!("expected a linked account view");
    };
    assert_eq!(whitelisted_wallets, vec![wallet]);

    let unwhitelisted = client
        .delete(format!("/ops/debug/accounts/{client_id}/wallets/{wallet}"))
        .header(debug_token())
        .dispatch()
        .await;
    assert_eq!(unwhitelisted.status(), Status::Ok);
    let Some(AccountView::LinkedToAlpaca { whitelisted_wallets, .. }) =
        find_by_client_id(pool, &client_id).await.expect("view loads")
    else {
        panic!("expected a linked account view");
    };
    assert!(whitelisted_wallets.is_empty(), "the wallet must be removed");

    let added = client
        .post("/ops/debug/tokenized-assets")
        .header(rocket::http::ContentType::JSON)
        .header(debug_token())
        .body(format!(
            r#"{{"underlying":"MSFT","token":"tMSFT","network":"base","vault":"{vault}"}}"#
        ))
        .dispatch()
        .await;
    assert_eq!(added.status(), Status::Created);

    // MSFT is not among the seeded assets, so this is a new listing rather
    // than a vault update of an existing one; the detail read proves it
    // persisted and carries every field.
    let detail = client
        .get("/ops/debug/tokenized-assets/MSFT?network=base")
        .header(debug_token())
        .dispatch()
        .await;
    assert_eq!(detail.status(), Status::Ok);
    let detail = detail
        .into_json::<TokenizedAssetDetailResponse>()
        .await
        .expect("detail response body");
    assert_eq!(detail.underlying, UnderlyingSymbol::new("MSFT").unwrap());
    assert_eq!(detail.token, TokenSymbol::new("tMSFT"));
    assert_eq!(detail.network, Network::Base);
    assert_eq!(detail.vault, vault);
    assert_eq!(detail.status, TokenizedAssetStatus::Enabled);

    assert!(logs_contain_at!(Level::INFO, &["IAP assertion accepted"]));
}
