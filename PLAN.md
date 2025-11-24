# Implementation Plan: API Key + IP Whitelisting Authentication

## Overview

Implement authentication for issuer endpoints that Alpaca calls, using API key
validation combined with IP whitelisting. This matches Alpaca's own API
authentication pattern and provides good security without requiring complex
infrastructure.

## Scope

Protect these endpoints:

- `POST /accounts/connect` - Account linking
- `GET /tokenized-assets` - List supported assets
- `POST /inkind/issuance` - Mint request
- `POST /inkind/issuance/confirm` - Journal confirmation

## Design Decisions

### Authentication Method: API Key in Header

**Format**: `Authorization: Bearer <api_key>`

**Rationale**:

- Standard HTTP header that Alpaca already uses for their own API
- No custom header names to coordinate
- Works with all HTTP clients
- Easy to test with curl/Postman

**Alternative considered**: Custom header like `X-API-Key`

- Rejected: Standard `Authorization` header is more conventional

### IP Whitelisting

**Approach**: Maintain allowed IP ranges in configuration

**Rationale**:

- Adds defense in depth (attacker needs both valid key AND access from Alpaca
  IP)
- Protects against stolen API key attacks from outside networks
- Standard practice for financial B2B integrations

**Trade-off**: Requires coordination when Alpaca's IPs change, but this is rare
and manageable

### Configuration

**API Key Storage**: Environment variable `ISSUER_API_KEY`

- Different from `ALPACA_API_KEY` (which is our key to call Alpaca)
- Long random key (256-bit minimum)
- Rotate every 90 days

**IP Whitelist Storage**: Environment variable `ALPACA_IP_RANGES`

- Format: Comma-separated CIDR ranges (e.g., `"1.2.3.0/24,5.6.7.8/32"`)
- Can be single IPs or ranges
- Obtained from Alpaca documentation/support

## Task Breakdown

### Task 1. Add Configuration Types and Managed State

Add configuration fields for API key and IP whitelist, and make config available
to request guards.

**Subtasks**:

- [ ] Add `ipnetwork` crate: `cargo add ipnetwork`
- [ ] Add `issuer_api_key: String` field to `Config` struct
- [ ] Add `alpaca_ip_ranges: Vec<IpNetwork>` field to `Config` struct
- [ ] Update `.env.example` with `ISSUER_API_KEY` and `ALPACA_IP_RANGES` with
      explanatory comments
- [ ] Update `Config::parse()` to read both environment variables
- [ ] Add validation that API key is at least 32 characters
- [ ] Add parsing for CIDR notation IP ranges (comma-separated format)
- [ ] Add `Config` to Rocket managed state in `initialize_rocket()`

**Files to modify**:

- `src/config.rs`
- `.env.example`
- `src/lib.rs` (add Config to managed state)

**Acceptance criteria**:

- Config successfully parses IP ranges like `"1.2.3.0/24,5.6.7.8/32"`
- Config validates API key meets minimum length (32 chars)
- Missing/invalid config returns clear error message
- Config accessible via `rocket.state::<Config>()`
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  passes
- `cargo fmt` applied

---

### Task 2. Implement Authentication Request Guard

Create Rocket request guard that validates API key and IP address using config
from managed state.

**Subtasks**:

- [ ] Add `subtle` crate: `cargo add subtle`
- [ ] Create `src/auth.rs` module
- [ ] Define `AlpacaAuth` guard struct
- [ ] Implement `FromRequest` trait for `AlpacaAuth`
- [ ] Access `Config` from managed state via
      `request.rocket().state::<Config>()`
- [ ] Extract `Authorization` header and validate Bearer format
- [ ] Use constant-time comparison for API key validation (prevent timing
      attacks)
- [ ] Extract client IP from request
- [ ] Validate IP is in allowed ranges from config
- [ ] Define `AuthError` enum with variants: `MissingApiKey`, `InvalidApiKey`,
      `UnauthorizedIp`, `NoClientIp`, `ConfigMissing`
- [ ] Implement `Responder` for `AuthError` to return appropriate HTTP status
      codes
- [ ] Add structured logging for auth attempts (success and failure)
- [ ] Add `mod auth;` to `src/lib.rs`
- [ ] Export `AlpacaAuth` from `src/lib.rs` as `pub use auth::AlpacaAuth;`

**Files to create/modify**:

- Create `src/auth.rs`
- Modify `src/lib.rs`

**Implementation notes**:

```rust
// Constant-time comparison to prevent timing attacks
use subtle::ConstantTimeEq;

fn validate_api_key(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}
```

**Error responses**:

- Missing API key → 401 Unauthorized
- Invalid API key → 401 Unauthorized
- IP not whitelisted → 403 Forbidden
- Can't determine client IP → 400 Bad Request

**Logging**:

```rust
// Success
info!(
    ip = %client_ip,
    endpoint = %request.uri(),
    "Alpaca authentication success"
);

// Failure
warn!(
    ip = ?maybe_ip,
    reason = ?error,
    endpoint = %request.uri(),
    "Alpaca authentication failed"
);
```

**Acceptance criteria**:

- Request with valid key from whitelisted IP succeeds
- Request with invalid key returns 401
- Request from non-whitelisted IP returns 403
- Request without Authorization header returns 401
- All auth attempts are logged with IP and outcome
- Timing attack resistance verified (constant-time comparison)
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  passes
- `cargo fmt` applied

---

### Task 3. Add Auth Guard to Endpoints

Add `AlpacaAuth` guard to all issuer endpoints.

**Subtasks**:

- [ ] Add `_auth: AlpacaAuth` parameter to `connect_account` endpoint
- [ ] Add `_auth: AlpacaAuth` parameter to `list_tokenized_assets` endpoint
- [ ] Add `_auth: AlpacaAuth` parameter to `initiate_mint` endpoint
- [ ] Add `_auth: AlpacaAuth` parameter to `confirm_journal` endpoint
- [ ] Verify parameter is named with underscore prefix (unused in handler body)

**Files to modify**:

- `src/account/api.rs`
- `src/tokenized_asset/api.rs`
- `src/mint/api/initiate.rs`
- `src/mint/api/confirm.rs`

**Implementation example**:

```rust
#[post("/inkind/issuance", format = "json", data = "<request>")]
pub(crate) async fn initiate_mint(
    _auth: AlpacaAuth,  // Authentication happens before handler is called
    cqrs: &rocket::State<crate::MintCqrs>,
    pool: &rocket::State<sqlx::Pool<sqlx::Sqlite>>,
    request: Json<MintRequest>,
) -> Result<Json<MintResponse>, MintApiError> {
    // Handler logic - auth already validated
}
```

**Acceptance criteria**:

- Requests without valid auth return 401/403 before reaching handler logic
- Requests with valid auth reach handler as before
- All protected endpoints require authentication
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  passes
- `cargo fmt` applied

---

### Task 4. Add Unit Tests for Auth Guard

Test authentication logic in isolation.

**Subtasks**:

- [ ] Add `#[cfg(test)] mod tests` to `src/auth.rs`
- [ ] Test: Valid API key + whitelisted IP → Success
- [ ] Test: Invalid API key → 401 Unauthorized
- [ ] Test: Missing Authorization header → 401 Unauthorized
- [ ] Test: Malformed Authorization header → 401 Unauthorized
- [ ] Test: Valid key but non-whitelisted IP → 403 Forbidden
- [ ] Test: Request from 127.0.0.1 when whitelisted → Success
- [ ] Test: Request from 0.0.0.0/0 whitelist (allow all) → Success for any IP

**Files to modify**:

- `src/auth.rs`

**Testing approach**:

```rust
#[tokio::test]
async fn test_valid_auth_succeeds() {
    let rocket = rocket::build()
        .manage(Config {
            issuer_api_key: "test-key-12345".to_string(),
            alpaca_ip_ranges: vec!["127.0.0.1/32".parse().unwrap()],
            // ... other config
        })
        .mount("/", routes![test_endpoint]);

    let client = rocket::local::asynchronous::Client::tracked(rocket)
        .await
        .expect("valid rocket instance");

    let response = client
        .get("/test")
        .header(Header::new("Authorization", "Bearer test-key-12345"))
        .dispatch()
        .await;

    assert_eq!(response.status(), Status::Ok);
}
```

**Acceptance criteria**:

- All test scenarios pass
- Tests verify both API key and IP validation independently
- Edge cases covered (missing headers, malformed data)
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  passes
- `cargo fmt` applied

---

### Task 5. Add Integration Tests

Test authentication with actual endpoints.

**Subtasks**:

- [ ] Update existing endpoint tests to include valid auth
- [ ] Add test: `POST /accounts/connect` without auth → 401
- [ ] Add test: `GET /tokenized-assets` without auth → 401
- [ ] Add test: `POST /inkind/issuance` without auth → 401
- [ ] Add test: `POST /inkind/issuance/confirm` without auth → 401
- [ ] Add test: Valid auth allows request to succeed (existing happy path tests)
- [ ] Add test: Wrong IP returns 403 even with valid key

**Files to modify**:

- `src/account/api.rs` (test module)
- `src/tokenized_asset/api.rs` (test module)
- `src/mint/api/initiate.rs` (test module)
- `src/mint/api/confirm.rs` (test module)

**Implementation notes**: All existing tests need to be updated to include valid
authentication:

```rust
let response = client
    .post("/inkind/issuance")
    .header(ContentType::JSON)
    .header(Header::new("Authorization", "Bearer test-api-key"))  // Add this
    .body(request_body.to_string())
    .dispatch()
    .await;
```

**Acceptance criteria**:

- All existing tests pass with authentication added
- New negative tests verify auth is required
- IP whitelisting is tested in integration tests
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  passes
- `cargo fmt` applied

---

### Task 6. Add Rate Limiting

Prevent brute force attacks on API key.

**Subtasks**:

- [ ] Add `governor` crate for rate limiting: `cargo add governor`
- [ ] Create rate limiter fairings/guards
- [ ] Limit failed auth attempts: 10 per IP per minute
- [ ] Log when rate limit is triggered
- [ ] Return 429 Too Many Requests when rate limited

**Files to create/modify**:

- `src/auth.rs` or separate `src/rate_limit.rs`

**Implementation approach**:

```rust
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

// In Config or as managed state
let limiter = RateLimiter::keyed(
    Quota::per_minute(NonZeroU32::new(10).unwrap())
);

// In auth guard
if auth_failed {
    if limiter.check_key(&client_ip).is_err() {
        warn!(ip = %client_ip, "Rate limit exceeded for failed auth");
        return Outcome::Error((Status::TooManyRequests, AuthError::RateLimited));
    }
}
```

**Acceptance criteria**:

- 11th failed auth attempt from same IP within a minute returns 429
- Rate limit resets after 1 minute
- Successful authentications don't count against rate limit
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  passes
- `cargo fmt` applied

---

### Task 7. Documentation and Configuration Guide

Document the authentication system for deployment and Alpaca integration.

**Subtasks**:

- [ ] Update SPEC.md authentication section with implementation details
- [ ] Update README.md with authentication configuration
- [ ] Add comments to `.env.example` explaining auth variables
- [ ] Document API key rotation procedure
- [ ] Document how to update IP whitelist

**Files to modify**:

- `SPEC.md`
- `README.md`
- `.env.example`

**Documentation should cover**:

- How to generate a secure API key
- How to configure IP whitelist
- How Alpaca should send the Authorization header
- What error responses mean
- How to rotate the API key
- How to add/remove IP ranges

**Acceptance criteria**:

- Clear instructions for configuring authentication in README.md
- SPEC.md updated with authentication implementation details
- Example curl commands showing correct `Authorization: Bearer <key>` format
- API key rotation procedure documented
- IP whitelist update procedure documented
- Security best practices documented (HTTPS, key strength, rotation schedule)

---

### Task 8. End-to-End Authentication Testing

Test complete authentication flow in realistic scenario.

**Subtasks**:

- [ ] Test mint flow with authentication: initiate → confirm → callback
- [ ] Test account linking with authentication
- [ ] Test asset listing with authentication
- [ ] Verify all flows work with valid auth
- [ ] Verify all flows fail gracefully with invalid auth

**Files to modify**:

- `tests/` (end-to-end test files)

**Acceptance criteria**:

- Complete mint flow (initiate → confirm → callback) works with authentication
- Account linking flow works with authentication
- Asset listing flow works with authentication
- Auth failures at any step return appropriate errors (401/403)
- End-to-end tests pass with authentication integrated
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings`
  passes
- `cargo fmt` applied
