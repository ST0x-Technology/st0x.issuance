//! Shared structured audit records for mutation-capable operator routes.

use chrono::{SecondsFormat, Utc};
use rocket::data::{self, Data, FromData};
use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::{Header, Method, Status};
use rocket::serde::json::{self, Json};
use rocket::{Request, Response, Route};
use serde::Deserialize;
use st0x_issuance_dto::{
    AddTokenizedAssetRequest, BurnExcessCommon, BurnExcessExpectFundingRequest,
    BurnExcessExternalRequest, BurnExcessInternalRequest, CloseMintRequest,
    CloseRedemptionRequest, ForceCompleteRedemptionRequest,
    RegisterAccountRequest, ScheduleFreezeWindowRequest,
    WhitelistWalletRequest,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::auth::OpsTier;

const AUDIT_SCHEMA: &str = "st0x.operations.audit.v1";
const SERVICE: &str = "issuance";
const REQUEST_ID_HEADER: &str = "x-request-id";
const UNAUTHENTICATED: &str = "unauthenticated";
const NOT_IDENTIFIED: &str = "not_identified";
const NOT_PROVIDED: &str = "not_provided";
const TARGET_VALUE_LIMIT: usize = 256;
const REASON_LIMIT: usize = 1024;

/// Operator surface an audited request reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuditRole {
    Ops(OpsTier),
    /// The shared-key (`InternalAuth`) twins of the operator mutations.
    Internal,
}

impl AuditRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ops(tier) => tier.as_str(),
            Self::Internal => "internal",
        }
    }
}

/// One versioned event shared with the liquidity bot.
#[derive(Debug)]
struct OperationsAuditEvent {
    principal: String,
    role: AuditRole,
    route: String,
    request_id: Uuid,
    target_id: String,
    reason: String,
    outcome: AuditOutcome,
    timestamp: String,
}

impl OperationsAuditEvent {
    fn new(
        request: &Request<'_>,
        role: AuditRole,
        unmatched_route: &'static str,
        status: Status,
    ) -> Self {
        let body = request.local_cache(AuditBody::default);
        let (route, mut target_parts) = request.route().map_or_else(
            || (unmatched_route.to_string(), Vec::new()),
            |matched| {
                (matched.uri.to_string(), path_parameters(matched, request))
            },
        );
        for target in &body.targets {
            if !target_parts.contains(target) {
                target_parts.push(target.clone());
            }
        }

        Self {
            principal: request
                .local_cache(|| AuditPrincipal(UNAUTHENTICATED.to_string()))
                .0
                .clone(),
            role,
            route,
            request_id: request_id(request),
            target_id: if target_parts.is_empty() {
                NOT_IDENTIFIED.to_string()
            } else {
                target_parts
                    .into_iter()
                    .map(|part| bounded(part, TARGET_VALUE_LIMIT))
                    .collect::<Vec<_>>()
                    .join(":")
            },
            reason: body.reason.clone().map_or_else(
                || NOT_PROVIDED.to_string(),
                |reason| bounded(reason, REASON_LIMIT),
            ),
            outcome: AuditOutcome::from_status(status.code),
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuditOutcome {
    Success,
    Denied,
    ValidationFailure,
    CommandFailure,
}

impl AuditOutcome {
    const fn from_status(status: u16) -> Self {
        match status {
            200..=299 => Self::Success,
            // 429 comes only from the failed-authentication rate limiter.
            401 | 403 | 429 => Self::Denied,
            400 | 404 | 405 | 413 | 415 | 422 => Self::ValidationFailure,
            _ => Self::CommandFailure,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Denied => "denied",
            Self::ValidationFailure => "validation_failure",
            Self::CommandFailure => "command_failure",
        }
    }
}

/// Writes the event to the `operations_audit` target, which the console
/// layer admits at INFO whatever `RUST_LOG` says (see
/// `telemetry::console_filter`). Every caller-influenced value is recorded as a
/// string value, so the text formatter quotes and escapes it and a newline in
/// a path segment or reason cannot start a forged record line.
fn record(event: &OperationsAuditEvent) {
    match event.outcome {
        AuditOutcome::Success => info!(
            target: "operations_audit",
            audit_schema = AUDIT_SCHEMA,
            service = SERVICE,
            principal = event.principal.as_str(),
            role = event.role.as_str(),
            route = event.route.as_str(),
            request_id = %event.request_id,
            target_id = event.target_id.as_str(),
            reason = event.reason.as_str(),
            outcome = event.outcome.as_str(),
            timestamp = %event.timestamp,
            "Operations audit event"
        ),
        AuditOutcome::Denied
        | AuditOutcome::ValidationFailure
        | AuditOutcome::CommandFailure => warn!(
            target: "operations_audit",
            audit_schema = AUDIT_SCHEMA,
            service = SERVICE,
            principal = event.principal.as_str(),
            role = event.role.as_str(),
            route = event.route.as_str(),
            request_id = %event.request_id,
            target_id = event.target_id.as_str(),
            reason = event.reason.as_str(),
            outcome = event.outcome.as_str(),
            timestamp = %event.timestamp,
            "Operations audit event"
        ),
    }
}

/// Keeps an audit value whole when it fits its limit and otherwise records an
/// explicit marker: an unbounded caller-supplied value could push the record
/// past the log entry size limit and lose the whole event.
fn bounded(value: String, limit: usize) -> String {
    if value.len() <= limit {
        value
    } else {
        format!("<oversized: {} bytes>", value.len())
    }
}

/// Identity verified by the route's authentication guard. A request no guard
/// accepted records `unauthenticated`.
struct AuditPrincipal(String);

/// Records the identity an authentication guard verified for this request.
pub(crate) fn record_principal(request: &Request<'_>, principal: String) {
    request.local_cache(|| AuditPrincipal(principal));
}

/// Entity identifiers and reason a mutation's typed request body carries.
/// Read only after the body parsed, so a field the command ignores never
/// reaches the audit record.
pub(crate) trait AuditedBody {
    /// Identifiers the body names beyond the route's path parameters.
    fn audit_targets(&self) -> Vec<String> {
        Vec::new()
    }

    /// The operator's stated reason, when the command records one.
    fn audit_reason(&self) -> Option<&str> {
        None
    }
}

#[derive(Default)]
struct AuditBody {
    targets: Vec<String>,
    reason: Option<String>,
}

/// JSON data guard for audited mutation routes. Parses exactly like [`Json`]
/// (same limit, same errors) and records the body's [`AuditedBody`] identity
/// for the request's audit event.
pub(crate) struct Audited<T>(pub(crate) T);

#[rocket::async_trait]
impl<'r, T> FromData<'r> for Audited<T>
where
    T: Deserialize<'r> + AuditedBody + Send,
{
    type Error = json::Error<'r>;

    async fn from_data(
        request: &'r Request<'_>,
        data: Data<'r>,
    ) -> data::Outcome<'r, Self> {
        Json::<T>::from_data(request, data).await.map(|Json(body)| {
            request.local_cache(|| AuditBody {
                targets: body.audit_targets(),
                reason: body.audit_reason().map(ToOwned::to_owned),
            });
            Self(body)
        })
    }
}

impl AuditedBody for AddTokenizedAssetRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.underlying.to_string(), self.network.to_string()]
    }
}

impl AuditedBody for RegisterAccountRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.email.to_string()]
    }
}

impl AuditedBody for WhitelistWalletRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.wallet.to_string()]
    }
}

impl AuditedBody for ScheduleFreezeWindowRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.underlying.to_string()]
    }
}

impl AuditedBody for CloseRedemptionRequest {
    fn audit_reason(&self) -> Option<&str> {
        Some(&self.reason)
    }
}

impl AuditedBody for ForceCompleteRedemptionRequest {
    fn audit_reason(&self) -> Option<&str> {
        Some(&self.reason)
    }
}

impl AuditedBody for CloseMintRequest {
    fn audit_reason(&self) -> Option<&str> {
        Some(&self.reason)
    }
}

impl AuditedBody for BurnExcessCommon {
    /// The mode follows the mint id: every burn-excess call defaults to a dry
    /// run, so a record without it could not tell a plan from a signed burn
    /// or a stream close.
    fn audit_targets(&self) -> Vec<String> {
        let mode = match (self.close, self.execute) {
            (false, false) => "dry-run",
            (false, true) => "execute",
            (true, false) => "close-dry-run",
            (true, true) => "close",
        };
        vec![self.issuer_request_id.to_string(), mode.to_string()]
    }

    fn audit_reason(&self) -> Option<&str> {
        Some(&self.reason)
    }
}

impl AuditedBody for BurnExcessInternalRequest {
    fn audit_targets(&self) -> Vec<String> {
        self.common.audit_targets()
    }

    fn audit_reason(&self) -> Option<&str> {
        self.common.audit_reason()
    }
}

impl AuditedBody for BurnExcessExpectFundingRequest {
    fn audit_targets(&self) -> Vec<String> {
        self.common.audit_targets()
    }

    fn audit_reason(&self) -> Option<&str> {
        self.common.audit_reason()
    }
}

impl AuditedBody for BurnExcessExternalRequest {
    fn audit_targets(&self) -> Vec<String> {
        self.common.audit_targets()
    }

    fn audit_reason(&self) -> Option<&str> {
        self.common.audit_reason()
    }
}

/// Audits every mutation request to an operator surface after its final
/// status is known, including guard, routing, and data-validation failures.
pub(crate) struct OperationsAuditFairing;

#[rocket::async_trait]
impl Fairing for OperationsAuditFairing {
    fn info(&self) -> Info {
        Info { name: "operations audit", kind: Kind::Response }
    }

    async fn on_response<'request>(
        &self,
        request: &'request Request<'_>,
        response: &mut Response<'request>,
    ) {
        let Some((role, unmatched_route)) = audited_surface(request) else {
            return;
        };

        let event = OperationsAuditEvent::new(
            request,
            role,
            unmatched_route,
            response.status(),
        );
        response.set_header(Header::new(
            REQUEST_ID_HEADER,
            event.request_id.to_string(),
        ));
        record(&event);
    }
}

/// The audited surface of a mutation request and the fixed route an unmatched
/// request records instead of its caller-chosen path. Classified from the
/// percent-decoded, empty-skipping segments Rocket routes on, so no spelling of
/// a mutation path reaches a handler without an audit event.
fn audited_surface(request: &Request<'_>) -> Option<(AuditRole, &'static str)> {
    if !matches!(
        request.method(),
        Method::Post | Method::Put | Method::Patch | Method::Delete
    ) {
        return None;
    }

    let segments = request.uri().path().segments();
    match (segments.get(0), segments.get(1), segments.get(2)) {
        (Some("ops"), Some("debug"), _) => {
            Some((AuditRole::Ops(OpsTier::Debug), "/ops/debug/<path..>"))
        }
        (Some("ops"), Some("capital"), _) => {
            Some((AuditRole::Ops(OpsTier::Capital), "/ops/capital/<path..>"))
        }
        (Some("ops"), Some("breakglass"), _) => Some((
            AuditRole::Ops(OpsTier::Breakglass),
            "/ops/breakglass/<path..>",
        )),
        (Some("admin"), _, _) => Some((AuditRole::Internal, "/admin/<path..>")),
        // `/accounts/connect` is Alpaca's account-link call, not an operator
        // mutation.
        (Some("accounts"), None, _)
        | (Some("accounts"), Some(_), Some("wallets")) => {
            Some((AuditRole::Internal, "/accounts/<path..>"))
        }
        (Some("tokenized-assets"), None, _) => {
            Some((AuditRole::Internal, "/tokenized-assets/<path..>"))
        }
        _ => None,
    }
}

fn request_id(request: &Request<'_>) -> Uuid {
    request
        .headers()
        .get_one(REQUEST_ID_HEADER)
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(Uuid::new_v4)
}

/// Values of the matched route's single-segment parameters, read from the same
/// decoded segments Rocket matched them against.
fn path_parameters(route: &Route, request: &Request<'_>) -> Vec<String> {
    route
        .uri
        .path()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .zip(request.uri().path().segments())
        .filter(|(template, _)| {
            template.starts_with('<')
                && template.ends_with('>')
                && !template.ends_with("..>")
        })
        .map(|(_, value)| value.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, U256};
    use rocket::Request;
    use rocket::http::{ContentType, Header, Status};
    use rocket::local::asynchronous::Client;
    use rocket::request::{FromRequest, Outcome};
    use serde::Deserialize;
    use st0x_issuance_dto::{
        BurnExcessCommon, BurnExcessInternalRequest, Network,
    };
    use tracing::Level;
    use tracing_test::traced_test;
    use uuid::uuid;

    use super::{
        AuditOutcome, AuditRole, Audited, AuditedBody, OperationsAuditEvent,
        OperationsAuditFairing, record, record_principal,
    };
    use crate::auth::OpsTier;
    use crate::test_utils::logs_contain_at;

    fn event(outcome: AuditOutcome) -> OperationsAuditEvent {
        OperationsAuditEvent {
            principal: "accounts.google.com:1234".to_string(),
            role: AuditRole::Ops(OpsTier::Capital),
            route: "/ops/capital/freeze/<underlying>".to_string(),
            request_id: uuid!("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            target_id: "AAPL".to_string(),
            reason: "incident 42".to_string(),
            outcome,
            timestamp: "2026-01-01T00:00:00.000Z".to_string(),
        }
    }

    struct AcceptedPrincipal;

    #[rocket::async_trait]
    impl<'request> FromRequest<'request> for AcceptedPrincipal {
        type Error = ();

        async fn from_request(
            request: &'request Request<'_>,
        ) -> Outcome<Self, Self::Error> {
            record_principal(request, "accounts.google.com:1234".to_string());
            Outcome::Success(Self)
        }
    }

    struct DeniedPrincipal;

    #[rocket::async_trait]
    impl<'request> FromRequest<'request> for DeniedPrincipal {
        type Error = ();

        async fn from_request(
            _request: &'request Request<'_>,
        ) -> Outcome<Self, Self::Error> {
            Outcome::Error((Status::Unauthorized, ()))
        }
    }

    #[derive(Deserialize)]
    struct CommandBody {
        issuer_request_id: String,
        reason: String,
    }

    impl AuditedBody for CommandBody {
        fn audit_targets(&self) -> Vec<String> {
            vec![self.issuer_request_id.clone()]
        }

        fn audit_reason(&self) -> Option<&str> {
            Some(&self.reason)
        }
    }

    #[rocket::post("/ops/debug/success/<id>", format = "json", data = "<body>")]
    fn successful_command(
        _principal: AcceptedPrincipal,
        id: &str,
        body: Audited<CommandBody>,
    ) -> Status {
        let _ = (id, body);
        Status::Ok
    }

    #[rocket::post("/ops/debug/failure/<id>", format = "json", data = "<body>")]
    fn failed_command(
        _principal: AcceptedPrincipal,
        id: &str,
        body: Audited<CommandBody>,
    ) -> Status {
        let _ = (id, body);
        Status::InternalServerError
    }

    #[rocket::post("/ops/debug/denied/<id>")]
    fn denied_command(_principal: DeniedPrincipal, id: &str) -> Status {
        let _ = id;
        Status::Ok
    }

    fn audited_rocket() -> rocket::Rocket<rocket::Build> {
        rocket::build().attach(OperationsAuditFairing).mount(
            "/",
            rocket::routes![successful_command, failed_command, denied_command],
        )
    }

    async fn dispatch_json<'client>(
        client: &'client Client,
        path: &'client str,
        request_id: &str,
        body: &str,
    ) -> rocket::local::asynchronous::LocalResponse<'client> {
        client
            .post(path)
            .header(ContentType::JSON)
            .header(Header::new("x-request-id", request_id.to_string()))
            .body(body)
            .dispatch()
            .await
    }

    #[traced_test]
    #[tokio::test]
    async fn fairing_audits_success_denial_validation_and_command_failure() {
        let client = Client::tracked(audited_rocket()).await.unwrap();
        let cases = [
            (
                "/ops/debug/success/widget-1",
                "11111111-1111-1111-1111-111111111111",
                r#"{"issuer_request_id":"op-1","reason":"incident 42"}"#,
                Status::Ok,
            ),
            (
                "/ops/debug/denied/widget-2",
                "22222222-2222-2222-2222-222222222222",
                "",
                Status::Unauthorized,
            ),
            (
                "/ops/debug/success/widget-3",
                "33333333-3333-3333-3333-333333333333",
                "{",
                Status::BadRequest,
            ),
            (
                "/ops/debug/failure/widget-4",
                "44444444-4444-4444-4444-444444444444",
                r#"{"issuer_request_id":"op-4","reason":"incident 42"}"#,
                Status::InternalServerError,
            ),
        ];

        for (path, request_id, body, expected_status) in cases {
            let response = dispatch_json(&client, path, request_id, body).await;
            assert_eq!(response.status(), expected_status);
            assert_eq!(
                response.headers().get_one("x-request-id"),
                Some(request_id)
            );
        }

        for (level, request_id, principal, target_id, reason, outcome) in [
            (
                Level::INFO,
                "11111111-1111-1111-1111-111111111111",
                "accounts.google.com:1234",
                "widget-1:op-1",
                "incident 42",
                "success",
            ),
            (
                Level::WARN,
                "22222222-2222-2222-2222-222222222222",
                "unauthenticated",
                "widget-2",
                "not_provided",
                "denied",
            ),
            (
                Level::WARN,
                "33333333-3333-3333-3333-333333333333",
                "accounts.google.com:1234",
                "widget-3",
                "not_provided",
                "validation_failure",
            ),
            (
                Level::WARN,
                "44444444-4444-4444-4444-444444444444",
                "accounts.google.com:1234",
                "widget-4:op-4",
                "incident 42",
                "command_failure",
            ),
        ] {
            assert!(
                logs_contain_at!(
                    level,
                    &[
                        "Operations audit event",
                        &format!("principal=\"{principal}\""),
                        "role=\"debug\"",
                        &format!("request_id={request_id}"),
                        &format!("target_id=\"{target_id}\""),
                        &format!("reason=\"{reason}\""),
                        &format!("outcome=\"{outcome}\""),
                    ]
                ),
                "missing {outcome} audit for request {request_id}"
            );
        }
    }

    /// Rocket's `Data::peek` returns at most 512 bytes, so a body larger than
    /// that must still yield its target and reason, and fields the typed body
    /// does not declare must not reach the record.
    #[traced_test]
    #[tokio::test]
    async fn typed_body_identity_survives_large_bodies_and_ignores_undeclared_fields()
     {
        let client = Client::tracked(audited_rocket()).await.unwrap();
        let reason = "r".repeat(600);
        let body = serde_json::json!({
            "issuer_request_id": "op-5",
            "reason": reason,
            "audit_reason": "decoy reason",
            "nested": { "issuer_request_id": "decoy", "reason": "forged" },
        })
        .to_string();
        assert!(body.len() > 512);

        let response = dispatch_json(
            &client,
            "/ops/debug/success/widget-5",
            "55555555-5555-5555-5555-555555555555",
            &body,
        )
        .await;
        assert_eq!(response.status(), Status::Ok);

        assert!(logs_contain_at!(
            Level::INFO,
            &[
                "Operations audit event",
                "request_id=55555555-5555-5555-5555-555555555555",
                "target_id=\"widget-5:op-5\"",
                &format!("reason=\"{reason}\""),
            ]
        ));
        assert!(!logs_contain("decoy"));
        assert!(!logs_contain("forged"));
    }

    /// Rocket routes on percent-decoded, empty-skipping segments, so an encoded
    /// or doubled-slash spelling reaches the same handler and must be audited
    /// under the matched route with the decoded path value.
    #[traced_test]
    #[tokio::test]
    async fn encoded_operator_paths_are_audited_like_their_decoded_route() {
        let client = Client::tracked(audited_rocket()).await.unwrap();

        for (path, request_id) in [
            (
                "/ops/%64ebug/success/widget%2D6",
                "66666666-6666-6666-6666-666666666666",
            ),
            (
                "//ops/debug//success/widget-6",
                "77777777-7777-7777-7777-777777777777",
            ),
        ] {
            let response = dispatch_json(
                &client,
                path,
                request_id,
                r#"{"issuer_request_id":"op-6","reason":"incident 42"}"#,
            )
            .await;
            assert_eq!(response.status(), Status::Ok, "{path}");
            assert_eq!(
                response.headers().get_one("x-request-id"),
                Some(request_id),
                "{path}"
            );
            assert!(
                logs_contain_at!(
                    Level::INFO,
                    &[
                        "Operations audit event",
                        &format!("request_id={request_id}"),
                        "route=\"/ops/debug/success/<id>\"",
                        "target_id=\"widget-6:op-6\"",
                    ]
                ),
                "{path}"
            );
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn unmatched_operator_paths_record_a_fixed_route_and_others_are_unaudited()
     {
        let client = Client::tracked(audited_rocket()).await.unwrap();

        let unmatched = dispatch_json(
            &client,
            "/ops/debug/not-a-command/attacker-chosen",
            "88888888-8888-8888-8888-888888888888",
            "{}",
        )
        .await;
        assert_eq!(unmatched.status(), Status::NotFound);
        assert_eq!(
            unmatched.headers().get_one("x-request-id"),
            Some("88888888-8888-8888-8888-888888888888")
        );

        let unrelated = dispatch_json(
            &client,
            "/not-an-operator-route",
            "99999999-9999-9999-9999-999999999999",
            "{}",
        )
        .await;
        assert_eq!(unrelated.status(), Status::NotFound);
        assert_eq!(unrelated.headers().get_one("x-request-id"), None);

        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "Operations audit event",
                "request_id=88888888-8888-8888-8888-888888888888",
                "principal=\"unauthenticated\"",
                "route=\"/ops/debug/<path..>\"",
                "target_id=\"not_identified\"",
                "outcome=\"validation_failure\"",
            ]
        ));
        assert!(!logs_contain("attacker-chosen"));
        assert!(!logs_contain("99999999-9999-9999-9999-999999999999"));
    }

    /// Every burn-excess call defaults to a dry run, so the record must say
    /// which of the four modes ran: a plan and a signed burn otherwise read
    /// the same.
    #[test]
    fn burn_excess_targets_name_the_mode_that_ran() {
        for (close, execute, mode) in [
            (false, false, "dry-run"),
            (false, true, "execute"),
            (true, false, "close-dry-run"),
            (true, true, "close"),
        ] {
            let request = BurnExcessInternalRequest {
                common: BurnExcessCommon {
                    issuer_request_id: uuid!(
                        "12345678-1234-1234-1234-123456789abc"
                    ),
                    deposit_tx_hash: B256::repeat_byte(1),
                    receipt_id: U256::from(7),
                    shares: "1".parse().unwrap(),
                    reason: "incident 42".to_string(),
                    incident_id: None,
                    acknowledged_inflows: Vec::new(),
                    network: Network::Base,
                    chain_id: 8453,
                    execute,
                    close,
                },
            };

            assert_eq!(
                request.audit_targets(),
                ["12345678-1234-1234-1234-123456789abc", mode],
                "close={close} execute={execute}"
            );
        }
    }

    #[test]
    fn response_statuses_have_stable_outcomes() {
        assert_eq!(
            AuditOutcome::from_status(Status::Ok.code),
            AuditOutcome::Success
        );
        assert_eq!(
            AuditOutcome::from_status(Status::Unauthorized.code),
            AuditOutcome::Denied
        );
        assert_eq!(
            AuditOutcome::from_status(Status::UnprocessableEntity.code),
            AuditOutcome::ValidationFailure
        );
        assert_eq!(
            AuditOutcome::from_status(Status::InternalServerError.code),
            AuditOutcome::CommandFailure
        );
        assert_eq!(
            AuditOutcome::from_status(Status::TooManyRequests.code),
            AuditOutcome::Denied
        );
    }

    #[traced_test]
    #[test]
    fn shared_query_fields_are_logged_for_every_outcome() {
        for outcome in [
            AuditOutcome::Success,
            AuditOutcome::Denied,
            AuditOutcome::ValidationFailure,
            AuditOutcome::CommandFailure,
        ] {
            record(&event(outcome));
        }

        for (level, outcome) in [
            (Level::INFO, "success"),
            (Level::WARN, "denied"),
            (Level::WARN, "validation_failure"),
            (Level::WARN, "command_failure"),
        ] {
            assert!(logs_contain_at!(
                level,
                &[
                    "audit_schema=\"st0x.operations.audit.v1\"",
                    "service=\"issuance\"",
                    "principal=\"accounts.google.com:1234\"",
                    "role=\"capital\"",
                    "route=\"/ops/capital/freeze/<underlying>\"",
                    "request_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "target_id=\"AAPL\"",
                    "reason=\"incident 42\"",
                    &format!("outcome=\"{outcome}\""),
                    "timestamp=2026-01-01T00:00:00.000Z",
                ]
            ));
        }
    }

    #[traced_test]
    #[test]
    fn reason_newlines_cannot_start_a_forged_record_line() {
        let mut forged = event(AuditOutcome::Success);
        forged.reason =
            "ok\noperations_audit: Operations audit event principal=attacker"
                .to_string();

        record(&forged);

        assert!(logs_contain_at!(
            Level::INFO,
            &["Operations audit event", "reason=\"ok\\noperations_audit"]
        ));
        logs_assert(|lines| {
            lines
                .iter()
                .find(|line| line.starts_with("operations_audit:"))
                .map_or(Ok(()), |line| {
                    Err(format!("forged record line: {line}"))
                })
        });
    }

    /// A path segment reaches `target_id` percent-decoded, so an encoded
    /// newline from an unauthenticated caller must stay inside its quoted
    /// value, and an oversized segment must not inflate the record.
    #[traced_test]
    #[tokio::test]
    async fn path_values_cannot_forge_or_inflate_a_record() {
        let client = Client::tracked(audited_rocket()).await.unwrap();
        let oversized = "a".repeat(300);

        for (path, request_id) in [
            (
                "/ops/debug/denied/x%0Aoperations_audit:%20forged".to_string(),
                "abababab-abab-abab-abab-abababababab",
            ),
            (
                format!("/ops/debug/denied/{oversized}"),
                "cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd",
            ),
        ] {
            let response = dispatch_json(&client, &path, request_id, "").await;
            assert_eq!(response.status(), Status::Unauthorized, "{path}");
        }

        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "request_id=abababab-abab-abab-abab-abababababab",
                "target_id=\"x\\noperations_audit: forged\"",
            ]
        ));
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "request_id=cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd",
                "target_id=\"<oversized: 300 bytes>\"",
            ]
        ));
        assert!(!logs_contain(&oversized));
        logs_assert(|lines| {
            lines
                .iter()
                .find(|line| line.starts_with("operations_audit:"))
                .map_or(Ok(()), |line| {
                    Err(format!("forged record line: {line}"))
                })
        });
    }
}
