use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

pub(crate) struct FailedAuthRateLimiter(Arc<DefaultKeyedRateLimiter<IpAddr>>);

impl FailedAuthRateLimiter {
    pub(crate) fn new() -> Result<Self, RateLimitConfigError> {
        let rate_limit = NonZeroU32::new(10).ok_or(RateLimitConfigError)?;
        let quota = Quota::per_minute(rate_limit);
        Ok(Self(Arc::new(RateLimiter::keyed(quota))))
    }

    pub(crate) fn check(&self, ip: &IpAddr) -> bool {
        self.0.check_key(ip).is_ok()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid rate limit configuration")]
pub(crate) struct RateLimitConfigError;
