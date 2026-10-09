//! Shared structured audit records for mutation-capable operator routes.

use chrono::{SecondsFormat, Utc};
use parking_lot::Mutex;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::{Header, Method, Status};
use rocket::{Data, Request, Response};
use serde_json::Value;
use tracing::{error, info, warn};
use uuid::Uuid;

const AUDIT_SCHEMA: &str = "st0x.operations.audit.v1";
const SERVICE: &str = "issuance";
const REQUEST_ID_HEADER: &str = "x-request-id";
const BODY_PEEK_LIMIT: usize = 64 * 1024;
const UNAUTHENTICATED: &str = "unauthenticated";
const NOT_IDENTIFIED: &str = "not_identified";
const NOT_PROVIDED: &str = "not_provided";

/// Request-local audit dimensions populated before Rocket runs guards and data
/// parsing. Authentication adds the verified principal after IAP succeeds.
#[derive(Debug)]
struct AuditContext {
    request_id: Uuid,
    role: &'static str,
    path: String,
    body_targets: Vec<String>,
    reason: String,
    principal: Mutex<String>,
}

impl AuditContext {
    fn new(request: &Request<'_>, body: Option<&Value>) -> Self {
        Self {
            request_id: request_id(request),
            role: role_for_path(request.uri().path().as_str()),
            path: request.uri().path().to_string(),
            body_targets: body.map_or_else(Vec::new, body_targets),
            reason: body
                .and_then(body_reason)
                .unwrap_or(NOT_PROVIDED)
                .to_string(),
            principal: Mutex::new(UNAUTHENTICATED.to_string()),
        }
    }

    fn set_principal(&self, principal: String) {
        *self.principal.lock() = principal;
    }

    fn event(&self, route: &str, status: Status) -> OperationsAuditEvent {
        let mut target_parts = dynamic_path_values(route, &self.path);
        for target in &self.body_targets {
            if !target_parts.contains(target) {
                target_parts.push(target.clone());
            }
        }

        OperationsAuditEvent {
            principal: self.principal.lock().clone(),
            role: self.role,
            route: route.to_string(),
            request_id: self.request_id,
            target_id: if target_parts.is_empty() {
                NOT_IDENTIFIED.to_string()
            } else {
                target_parts.join(":")
            },
            reason: self.reason.clone(),
            outcome: AuditOutcome::from_status(status.code),
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        }
    }
}

/// One versioned event shared with the liquidity bot.
#[derive(Debug)]
struct OperationsAuditEvent {
    principal: String,
    role: &'static str,
    route: String,
    request_id: Uuid,
    target_id: String,
    reason: String,
    outcome: AuditOutcome,
    timestamp: String,
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
            401 | 403 => Self::Denied,
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

trait OperationsAuditRecorder {
    fn record(
        &self,
        event: &OperationsAuditEvent,
    ) -> Result<(), AuditRecordError>;
}

#[derive(Debug, thiserror::Error)]
enum AuditRecordError {
    #[cfg(test)]
    #[error("simulated audit recorder failure")]
    Simulated,
}

struct LogOperationsAuditRecorder;

impl OperationsAuditRecorder for LogOperationsAuditRecorder {
    fn record(
        &self,
        event: &OperationsAuditEvent,
    ) -> Result<(), AuditRecordError> {
        match event.outcome {
            AuditOutcome::Success => info!(
                target: "operations_audit",
                audit_schema = AUDIT_SCHEMA,
                service = SERVICE,
                principal = %event.principal,
                role = event.role,
                route = %event.route,
                request_id = %event.request_id,
                target_id = %event.target_id,
                reason = %event.reason,
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
                principal = %event.principal,
                role = event.role,
                route = %event.route,
                request_id = %event.request_id,
                target_id = %event.target_id,
                reason = %event.reason,
                outcome = event.outcome.as_str(),
                timestamp = %event.timestamp,
                "Operations audit event"
            ),
        }

        Ok(())
    }
}

fn record_with(
    recorder: &dyn OperationsAuditRecorder,
    event: &OperationsAuditEvent,
) {
    if let Err(record_error) = recorder.record(event) {
        error!(
            target: "operations_audit",
            audit_schema = AUDIT_SCHEMA,
            service = SERVICE,
            principal = %event.principal,
            role = event.role,
            route = %event.route,
            request_id = %event.request_id,
            target_id = %event.target_id,
            reason = %event.reason,
            outcome = event.outcome.as_str(),
            timestamp = %event.timestamp,
            %record_error,
            "Operations audit recording failed"
        );
    }
}

/// Adds the verified identity to the context created by [`OperationsAuditFairing`].
pub(crate) fn record_principal(request: &Request<'_>, principal: String) {
    request
        .local_cache(|| AuditContext::new(request, None))
        .set_principal(principal);
}

/// Audits every request to a mutation-capable role-gated route after its final
/// status is known, including guard and data-validation failures.
pub(crate) struct OperationsAuditFairing;

#[rocket::async_trait]
impl Fairing for OperationsAuditFairing {
    fn info(&self) -> Info {
        Info {
            name: "role-gated operations audit",
            kind: Kind::Request | Kind::Response,
        }
    }

    async fn on_request(&self, request: &mut Request<'_>, data: &mut Data<'_>) {
        if !is_audited_request(request.method(), request.uri().path().as_str())
        {
            return;
        }

        let peeked = data.peek(BODY_PEEK_LIMIT).await;
        let body = serde_json::from_slice::<Value>(peeked).ok();
        request.local_cache(|| AuditContext::new(request, body.as_ref()));
    }

    async fn on_response<'request>(
        &self,
        request: &'request Request<'_>,
        response: &mut Response<'request>,
    ) {
        if !is_audited_request(request.method(), request.uri().path().as_str())
        {
            return;
        }

        let context = request.local_cache(|| AuditContext::new(request, None));
        let route = request.route().map_or_else(
            || context.path.clone(),
            |matched| matched.uri.to_string(),
        );
        let event = context.event(&route, response.status());
        response.set_header(Header::new(
            REQUEST_ID_HEADER,
            event.request_id.to_string(),
        ));
        record_with(&LogOperationsAuditRecorder, &event);
    }
}

fn is_audited_request(method: Method, path: &str) -> bool {
    matches!(
        method,
        Method::Post | Method::Put | Method::Patch | Method::Delete
    ) && ["/ops/debug/", "/ops/capital/", "/ops/breakglass/"]
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

fn role_for_path(path: &str) -> &'static str {
    if path.starts_with("/ops/debug/") {
        "debug"
    } else if path.starts_with("/ops/capital/") {
        "capital"
    } else if path.starts_with("/ops/breakglass/") {
        "breakglass"
    } else {
        "unknown"
    }
}

fn request_id(request: &Request<'_>) -> Uuid {
    request
        .headers()
        .get_one(REQUEST_ID_HEADER)
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(Uuid::new_v4)
}

fn body_reason(body: &Value) -> Option<&str> {
    find_string(body, &["reason", "audit_reason", "auditReason"])
}

fn body_targets(body: &Value) -> Vec<String> {
    const TARGET_KEYS: &[&str] = &[
        "issuer_request_id",
        "issuerRequestId",
        "operation_id",
        "operationId",
        "client_id",
        "clientId",
        "aggregate_id",
        "aggregateId",
        "underlying",
        "symbol",
        "email",
        "id",
        "tx_hash",
        "txHash",
        "wallet",
        "chain",
        "token",
        "vault_id",
        "vaultId",
        "view",
        "direction",
    ];

    let mut targets = Vec::new();
    collect_strings(body, TARGET_KEYS, &mut targets);
    targets
}

fn find_string<'value>(
    value: &'value Value,
    keys: &[&str],
) -> Option<&'value str> {
    match value {
        Value::Object(object) => object.iter().find_map(|(key, nested)| {
            if keys.contains(&key.as_str()) {
                nested.as_str()
            } else {
                find_string(nested, keys)
            }
        }),
        Value::Array(values) => {
            values.iter().find_map(|nested| find_string(nested, keys))
        }
        _ => None,
    }
}

fn collect_strings(value: &Value, keys: &[&str], targets: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for (key, nested) in object {
                if keys.contains(&key.as_str()) {
                    let rendered =
                        nested.as_str().map(ToOwned::to_owned).or_else(|| {
                            nested.as_u64().map(|number| number.to_string())
                        });
                    if let Some(rendered) = rendered
                        && !targets.contains(&rendered)
                    {
                        targets.push(rendered);
                    }
                }
                collect_strings(nested, keys, targets);
            }
        }
        Value::Array(values) => {
            for nested in values {
                collect_strings(nested, keys, targets);
            }
        }
        _ => {}
    }
}

fn dynamic_path_values(route: &str, path: &str) -> Vec<String> {
    let route_path = route.split('?').next().unwrap_or(route);
    route_path
        .trim_matches('/')
        .split('/')
        .zip(path.trim_matches('/').split('/'))
        .filter(|(template, _)| {
            template.starts_with('<') && template.ends_with('>')
        })
        .map(|(_, actual)| actual.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use rocket::Request;
    use rocket::http::{ContentType, Header, Status};
    use rocket::local::asynchronous::Client;
    use rocket::request::{FromRequest, Outcome};
    use rocket::serde::json::Json;
    use serde_json::Value;
    use tracing::Level;
    use tracing_test::traced_test;
    use uuid::uuid;

    use super::{
        AuditOutcome, AuditRecordError, LogOperationsAuditRecorder,
        OperationsAuditEvent, OperationsAuditFairing, OperationsAuditRecorder,
        body_targets, dynamic_path_values, is_audited_request,
        record_principal, record_with,
    };
    use crate::test_utils::logs_contain_at;

    struct FailingRecorder;

    impl OperationsAuditRecorder for FailingRecorder {
        fn record(
            &self,
            _event: &OperationsAuditEvent,
        ) -> Result<(), AuditRecordError> {
            Err(AuditRecordError::Simulated)
        }
    }

    fn event(outcome: AuditOutcome) -> OperationsAuditEvent {
        OperationsAuditEvent {
            principal: "accounts.google.com:1234".to_string(),
            role: "capital",
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

    #[rocket::post("/ops/debug/success/<id>", format = "json", data = "<body>")]
    fn successful_command(
        _principal: AcceptedPrincipal,
        id: &str,
        body: Json<Value>,
    ) -> Status {
        let _ = (id, body);
        Status::Ok
    }

    #[rocket::post("/ops/debug/failure/<id>", format = "json", data = "<body>")]
    fn failed_command(
        _principal: AcceptedPrincipal,
        id: &str,
        body: Json<Value>,
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
                r#"{"operationId":"op-1","reason":"incident 42"}"#,
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
                r#"{"operationId":"op-4","reason":"incident 42"}"#,
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

        logs_assert(|lines| {
            for (request_id, principal, target_id, reason, outcome) in [
                (
                    "11111111-1111-1111-1111-111111111111",
                    "accounts.google.com:1234",
                    "widget-1:op-1",
                    "incident 42",
                    "success",
                ),
                (
                    "22222222-2222-2222-2222-222222222222",
                    "unauthenticated",
                    "widget-2",
                    "not_provided",
                    "denied",
                ),
                (
                    "33333333-3333-3333-3333-333333333333",
                    "accounts.google.com:1234",
                    "widget-3",
                    "not_provided",
                    "validation_failure",
                ),
                (
                    "44444444-4444-4444-4444-444444444444",
                    "accounts.google.com:1234",
                    "widget-4:op-4",
                    "incident 42",
                    "command_failure",
                ),
            ] {
                let matched = lines.iter().any(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(&format!("principal={principal}"))
                        && line.contains(&format!("request_id={request_id}"))
                        && line.contains(&format!("target_id={target_id}"))
                        && line.contains(&format!("reason={reason}"))
                        && line.contains(&format!("outcome=\"{outcome}\""))
                });
                if !matched {
                    return Err(format!(
                        "missing {outcome} audit for request {request_id}: {lines:?}"
                    ));
                }
            }
            Ok(())
        });
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
    }

    #[test]
    fn every_issuance_mutation_route_is_audited() {
        for path in [
            "/ops/debug/recover/redemption/mint-1",
            "/ops/debug/reprocess/mint/mint-1",
            "/ops/debug/orchestrator-verify-signing/base/AAPL",
            "/ops/debug/accounts",
            "/ops/debug/accounts/client-1/wallets",
            "/ops/debug/tokenized-assets",
            "/ops/capital/freeze/AAPL",
            "/ops/capital/unfreeze/AAPL",
            "/ops/capital/freeze-schedules",
            "/ops/capital/orchestrator-approve/base/AAPL",
            "/ops/breakglass/force-complete/redemption/redemption-1",
            "/ops/breakglass/close/redemption/redemption-1",
            "/ops/breakglass/close/mint/mint-1",
            "/ops/breakglass/burn-excess/internal",
            "/ops/breakglass/burn-excess/expect-funding",
            "/ops/breakglass/burn-excess/external",
        ] {
            assert!(
                is_audited_request(rocket::http::Method::Post, path),
                "{path}"
            );
        }
        assert!(is_audited_request(
            rocket::http::Method::Delete,
            "/ops/debug/accounts/client-1/wallets/0x1234"
        ));
        assert!(!is_audited_request(
            rocket::http::Method::Get,
            "/ops/read/stuck"
        ));
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
            record_with(&LogOperationsAuditRecorder, &event(outcome));
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
                    "principal=accounts.google.com:1234",
                    "role=\"capital\"",
                    "route=/ops/capital/freeze/<underlying>",
                    "request_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "target_id=AAPL",
                    "reason=incident 42",
                    &format!("outcome=\"{outcome}\""),
                    "timestamp=2026-01-01T00:00:00.000Z",
                ]
            ));
        }
    }

    #[test]
    fn target_is_composed_from_path_and_body_identifiers() {
        let path_targets = dynamic_path_values(
            "/ops/debug/accounts/<client_id>/wallets/<wallet>",
            "/ops/debug/accounts/client-7/wallets/0x1234",
        );
        let body_targets = body_targets(&serde_json::json!({
            "common": { "issuer_request_id": "mint-9" },
            "reason": "incident 42"
        }));

        assert_eq!(path_targets, ["client-7", "0x1234"]);
        assert_eq!(body_targets, ["mint-9"]);
    }

    #[traced_test]
    #[test]
    fn recorder_failure_is_visible_and_cannot_replace_the_command_result() {
        record_with(&FailingRecorder, &event(AuditOutcome::Success));

        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "Operations audit recording failed",
                "principal=accounts.google.com:1234",
                "target_id=AAPL",
                "request_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "outcome=\"success\"",
            ]
        ));
    }
}
