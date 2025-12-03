use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{Catcher, Request, catch, catchers};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
    status: u16,
}

pub(crate) fn json_catchers() -> Vec<Catcher> {
    catchers![
        bad_request,
        unauthorized,
        forbidden,
        not_found,
        conflict,
        unprocessable_entity,
        internal_server_error,
        default_catcher
    ]
}

#[catch(400)]
fn bad_request() -> Json<ErrorResponse> {
    Json(ErrorResponse { error: "Bad Request".to_string(), status: 400 })
}

#[catch(401)]
fn unauthorized() -> Json<ErrorResponse> {
    Json(ErrorResponse { error: "Unauthorized".to_string(), status: 401 })
}

#[catch(403)]
fn forbidden(req: &Request<'_>) -> Json<ErrorResponse> {
    let ip = req.client_ip().map(|ip| ip.to_string()).unwrap_or_default();
    Json(ErrorResponse {
        error: format!("Forbidden: IP {ip} not authorized"),
        status: 403,
    })
}

#[catch(404)]
fn not_found() -> Json<ErrorResponse> {
    Json(ErrorResponse { error: "Not Found".to_string(), status: 404 })
}

#[catch(409)]
fn conflict() -> Json<ErrorResponse> {
    Json(ErrorResponse { error: "Conflict".to_string(), status: 409 })
}

#[catch(422)]
fn unprocessable_entity() -> Json<ErrorResponse> {
    Json(ErrorResponse {
        error: "Unprocessable Entity".to_string(),
        status: 422,
    })
}

#[catch(500)]
fn internal_server_error() -> Json<ErrorResponse> {
    Json(ErrorResponse {
        error: "Internal Server Error".to_string(),
        status: 500,
    })
}

#[catch(default)]
fn default_catcher(status: Status, _req: &Request<'_>) -> Json<ErrorResponse> {
    Json(ErrorResponse {
        error: status.reason_lossy().to_string(),
        status: status.code,
    })
}
