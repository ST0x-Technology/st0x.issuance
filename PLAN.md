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

**API Key Storage**: Environment variable `ALPACA_API_KEY_FOR_US`

- Different from `ALPACA_API_KEY` (which is our key to call Alpaca)
- Long random key (256-bit minimum)
- Rotate every 90 days

**IP Whitelist Storage**: Environment variable `ALPACA_IP_RANGES`

- Format: Comma-separated CIDR ranges (e.g., `"1.2.3.0/24,5.6.7.8/32"`)
- Can be single IPs or ranges
- Obtained from Alpaca documentation/support

## Task Breakdown

### Task 1. Add Configuration Types

Add configuration fields for API key and IP whitelist.

**Subtasks**:

- [ ] Add `alpaca_api_key_for_us: String` field to `Config` struct
- [ ] Add `alpaca_ip_ranges: Vec<IpNetwork>` field to `Config` struct
- [ ] Update `.env.example` with new environment variables
- [ ] Update `Config::parse()` to read `ALPACA_API_KEY_FOR_US` and
      `ALPACA_IP_RANGES`
- [ ] Add validation that API key is at least 32 characters
- [ ] Add parsing for CIDR notation IP ranges using `ipnetwork` crate

**Files to modify**:

- `src/config.rs` or wherever `Config` lives
- `.env.example`

**Dependencies**:

- Add `ipnetwork` crate for IP range parsing: `cargo add ipnetwork`

**Acceptance criteria**:

- Config successfully parses IP ranges like `"1.2.3.0/24,5.6.7.8/32"`
- Config validates API key meets minimum length
- Missing/invalid config returns clear error message

---

### Task 2. Implement Authentication Request Guard

Create Rocket request guard that validates API key and IP address.

**Subtasks**:

- [ ] Create `src/auth.rs` module
- [ ] Define `AlpacaAuth` guard struct
- [ ] Implement `FromRequest` trait for `AlpacaAuth`
- [ ] Extract `Authorization` header and validate format
- [ ] Use constant-time comparison for API key validation (prevent timing
      attacks)
- [ ] Extract client IP from request
- [ ] Validate IP is in allowed ranges
- [ ] Define `AuthError` enum with variants: `MissingApiKey`, `InvalidApiKey`,
      `UnauthorizedIp`, `NoClientIp`
- [ ] Implement `Responder` for `AuthError` to return appropriate HTTP status
      codes
- [ ] Add structured logging for auth attempts (success and failure)

**Files to create/modify**:

- Create `src/auth.rs`
- Modify `src/lib.rs` to add `mod auth;` and `pub use auth::AlpacaAuth;`

**Dependencies**:

- `subtle` crate for constant-time comparison: `cargo add subtle`

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

---

### Task 4. Provide Config to Request Guards

Make configuration available to request guards via Rocket managed state.

**Subtasks**:

- [ ] Add `Config` to Rocket managed state in `initialize_rocket()`
- [ ] Update `AlpacaAuth::from_request()` to access config via
      `request.rocket().state::<Config>()`
- [ ] Use config's `alpaca_api_key_for_us` and `alpaca_ip_ranges` for validation

**Files to modify**:

- `src/lib.rs` (or wherever `initialize_rocket` is)
- `src/auth.rs`

**Implementation notes**:

```rust
// In from_request
let config = request.rocket()
    .state::<Config>()
    .ok_or(AuthError::ConfigMissing)?;

let expected_key = &config.alpaca_api_key_for_us;
let allowed_ips = &config.alpaca_ip_ranges;
```

**Acceptance criteria**:

- Request guard successfully accesses configuration
- Authentication uses config values for validation

---

### Task 5. Add Unit Tests for Auth Guard

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
            alpaca_api_key_for_us: "test-key-12345".to_string(),
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

---

### Task 6. Add Integration Tests

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

---

### Task 7. Add Rate Limiting

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

---

### Task 8. Documentation and Configuration Guide

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

- Clear instructions for configuring authentication
- Example curl commands showing correct header format
- Rotation procedure documented

---

### Task 9. End-to-End Authentication Testing

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

- Complete mint flow works with authentication
- Auth failures at any step are handled correctly

---

## Testing Strategy

### Unit Tests

- `src/auth.rs`: Request guard logic in isolation
- Cover all auth failure scenarios
- Verify constant-time comparison

### Integration Tests

- Each endpoint module: Verify auth is required
- Test both success and failure cases
- Verify error responses are correct

### End-to-End Tests

- Complete flows work with authentication
- Auth integrates properly with CQRS framework

### Manual Testing

- Test with curl/Postman before Alpaca integration
- Verify error messages are clear
- Test from different IPs (local, VPN, etc.)

## Security Checklist

Before marking this complete:

- [ ] API key uses cryptographically secure random generation (at least 256
      bits)
- [ ] Constant-time comparison prevents timing attacks
- [ ] All auth attempts logged (success and failure)
- [ ] IP address logged for audit trail
- [ ] Rate limiting prevents brute force
- [ ] Error messages don't leak information (same response for invalid key vs.
      wrong IP)
- [ ] HTTPS required (enforce in production configuration)
- [ ] API key not logged in plaintext
- [ ] Configuration validation ensures strong keys
- [ ] Documentation includes rotation procedure

## Deployment Checklist

Before production:

- [ ] Generate strong API key (use: `openssl rand -hex 32`)
- [ ] Obtain Alpaca's IP ranges from their documentation/support
- [ ] Configure `ALPACA_API_KEY_FOR_US` in production environment
- [ ] Configure `ALPACA_IP_RANGES` in production environment
- [ ] Test authentication from Alpaca's staging environment
- [ ] Set up monitoring/alerts for auth failures
- [ ] Document API key in secure location (password manager, Vault)
- [ ] Schedule 90-day key rotation reminder
- [ ] Verify HTTPS enforced in production
