use ipnetwork::IpNetwork;
use std::net::IpAddr;
use std::str::FromStr;
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpWhitelist {
    AllowAll,
    Whitelist { first: IpNetwork, rest: Vec<IpNetwork> },
}

impl IpWhitelist {
    pub(crate) fn is_allowed(&self, ip: &IpAddr) -> bool {
        match self {
            Self::AllowAll => true,
            Self::Whitelist { first, rest } => {
                first.contains(*ip)
                    || rest.iter().any(|range| range.contains(*ip))
            }
        }
    }

    /// Returns true if this whitelist admits every IP address.
    ///
    /// A whitelist admits all addresses when it is `AllowAll`, or when any of
    /// its ranges has a prefix length of 0 (i.e. `0.0.0.0/0` or `::/0`).
    ///
    /// Note: this detects only a literal `/0` (`0.0.0.0/0` / `::/0`); it does
    /// NOT detect equivalent full-coverage expressed as split CIDRs such as
    /// `0.0.0.0/1,128.0.0.0/1`.
    pub(crate) fn allows_all_addresses(&self) -> bool {
        match self {
            Self::AllowAll => true,
            Self::Whitelist { first, rest } => {
                first.prefix() == 0
                    || rest.iter().any(|range| range.prefix() == 0)
            }
        }
    }

    /// Creates a whitelist with a single IP range.
    #[must_use]
    pub const fn single(range: IpNetwork) -> Self {
        Self::Whitelist { first: range, rest: Vec::new() }
    }

    /// Creates a whitelist from a slice of IP ranges.
    ///
    /// Returns `AllowAll` if the slice is empty.
    #[must_use]
    pub fn from_ranges(ranges: &[IpNetwork]) -> Self {
        match ranges.split_first() {
            None => Self::AllowAll,
            Some((first, rest)) => {
                Self::Whitelist { first: *first, rest: rest.to_vec() }
            }
        }
    }
}

impl FromStr for IpWhitelist {
    type Err = IpWhitelistParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            warn!(target: "auth", "IP whitelist is empty - all IPs will be allowed. \
                 This is NOT RECOMMENDED for production."
            );
            return Ok(Self::AllowAll);
        }

        let ranges: Vec<IpNetwork> = s
            .split(',')
            .map(str::trim)
            .filter(|range| !range.is_empty())
            .map(IpNetwork::from_str)
            .collect::<Result<Vec<_>, _>>()?;

        let (first, rest) =
            ranges.split_first().ok_or(IpWhitelistParseError::Empty)?;

        Ok(Self::Whitelist { first: *first, rest: rest.to_vec() })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IpWhitelistParseError {
    #[error("Failed to parse IP range: {0}")]
    InvalidRange(#[from] ipnetwork::IpNetworkError),
    #[error("Whitelist cannot be empty")]
    Empty,
    #[error(
        "Internal IP whitelist must not allow all addresses (empty, 0.0.0.0/0, or ::/0 are not permitted)"
    )]
    AllowsAllAddresses,
}

/// A validated IP whitelist for internal/admin endpoints.
///
/// Internal endpoints must never admit every address, so an empty string,
/// `0.0.0.0/0`, and `::/0` (the most common misconfigurations) are rejected at
/// parse time. This prevents a misconfigured `INTERNAL_IP_RANGES_EXTRA` deploy
/// secret from silently opening the admin API to all clients. Equivalent
/// full-coverage expressed as split CIDRs (e.g. `0.0.0.0/1,128.0.0.0/1`) is
/// not detected and is intentionally deferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalIpWhitelist(IpWhitelist);

impl InternalIpWhitelist {
    #[must_use]
    pub const fn whitelist(&self) -> &IpWhitelist {
        &self.0
    }
}

impl FromStr for InternalIpWhitelist {
    type Err = IpWhitelistParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Return early to avoid IpWhitelist::from_str emitting the "all IPs will be
        // allowed" warning before this wrapper rejects the value.
        if s.trim().is_empty() {
            return Err(IpWhitelistParseError::AllowsAllAddresses);
        }
        let whitelist = IpWhitelist::from_str(s).map_err(|err| match err {
            IpWhitelistParseError::Empty => {
                IpWhitelistParseError::AllowsAllAddresses
            }
            err @ (IpWhitelistParseError::InvalidRange(_)
            | IpWhitelistParseError::AllowsAllAddresses) => err,
        })?;
        if whitelist.allows_all_addresses() {
            return Err(IpWhitelistParseError::AllowsAllAddresses);
        }
        Ok(Self(whitelist))
    }
}

#[cfg(test)]
mod tests {
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;

    #[test]
    fn test_allow_all_allows_any_ip() {
        let whitelist = IpWhitelist::AllowAll;

        assert!(whitelist.is_allowed(&"127.0.0.1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"8.8.8.8".parse().unwrap()));
        assert!(whitelist.is_allowed(&"::1".parse().unwrap()));
    }

    #[test]
    fn test_single_range_allows_matching_ip() {
        let whitelist = IpWhitelist::single("10.0.0.0/24".parse().unwrap());

        assert!(whitelist.is_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"10.0.0.255".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"10.0.1.1".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_multiple_ranges_allows_any_matching() {
        let whitelist = IpWhitelist::from_ranges(&[
            "10.0.0.0/24".parse().unwrap(),
            "192.168.1.0/24".parse().unwrap(),
        ]);

        assert!(whitelist.is_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"192.168.1.100".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[traced_test]
    #[test]
    fn test_from_str_empty_string_returns_allow_all_and_warns() {
        let whitelist: IpWhitelist = "".parse().unwrap();
        assert_eq!(whitelist, IpWhitelist::AllowAll);
        assert!(logs_contain_at!(Level::WARN, &["IP whitelist is empty"]));
    }

    #[traced_test]
    #[test]
    fn test_from_str_whitespace_only_returns_allow_all_and_warns() {
        let whitelist: IpWhitelist = "  ".parse().unwrap();
        assert_eq!(whitelist, IpWhitelist::AllowAll);
        assert!(logs_contain_at!(Level::WARN, &["IP whitelist is empty"]));
    }

    #[test]
    fn test_from_str_single_range() {
        let whitelist: IpWhitelist = "10.0.0.0/24".parse().unwrap();

        assert!(whitelist.is_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_from_str_multiple_ranges() {
        let whitelist: IpWhitelist =
            "10.0.0.0/24, 192.168.1.0/24".parse().unwrap();

        assert!(whitelist.is_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"192.168.1.100".parse().unwrap()));
    }

    #[test]
    fn test_from_str_skips_empty_segments() {
        // A trailing comma (e.g. base ranges plus an unset `${...EXTRA}`
        // placeholder) must not break parsing or open the whitelist.
        let whitelist: IpWhitelist =
            "127.0.0.0/8,::1/128,172.16.0.0/12,".parse().unwrap();

        assert!(whitelist.is_allowed(&"127.0.0.1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"172.16.5.5".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_from_str_skips_blank_and_whitespace_segments() {
        let whitelist: IpWhitelist =
            "10.0.0.0/24, ,, 192.168.1.0/24 ".parse().unwrap();

        assert!(whitelist.is_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"192.168.1.100".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_from_str_invalid_range_returns_error() {
        let result: Result<IpWhitelist, _> = "invalid".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::InvalidRange(_)),
            "expected InvalidRange, got {err}"
        );
    }

    #[test]
    fn test_from_str_only_commas_returns_empty_error() {
        let result: Result<IpWhitelist, _> = ",".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::Empty),
            "expected Empty, got {err}"
        );
    }

    #[test]
    fn test_from_str_whitespace_and_commas_returns_empty_error() {
        let result: Result<IpWhitelist, _> = " , , ".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::Empty),
            "expected Empty, got {err}"
        );
    }

    #[test]
    fn test_allows_all_addresses_allow_all_variant() {
        assert!(IpWhitelist::AllowAll.allows_all_addresses());
    }

    #[test]
    fn test_allows_all_addresses_catch_all_ipv4() {
        let whitelist: IpWhitelist = "0.0.0.0/0".parse().unwrap();
        assert!(whitelist.allows_all_addresses());
    }

    #[test]
    fn test_allows_all_addresses_normal_range_is_false() {
        let whitelist: IpWhitelist = "10.0.0.0/24".parse().unwrap();
        assert!(!whitelist.allows_all_addresses());
    }

    #[test]
    fn test_allows_all_addresses_catch_all_in_rest_position() {
        let whitelist: IpWhitelist = "10.0.0.0/24,0.0.0.0/0".parse().unwrap();
        assert!(whitelist.allows_all_addresses());
    }

    #[test]
    fn test_internal_whitelist_rejects_empty_string() {
        let result: Result<InternalIpWhitelist, _> = "".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::AllowsAllAddresses),
            "expected AllowsAllAddresses, got {err}"
        );
    }

    #[test]
    fn test_internal_whitelist_rejects_ipv4_catch_all() {
        let result: Result<InternalIpWhitelist, _> = "0.0.0.0/0".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::AllowsAllAddresses),
            "expected AllowsAllAddresses, got {err}"
        );
    }

    #[test]
    fn test_internal_whitelist_rejects_ipv6_catch_all() {
        let result: Result<InternalIpWhitelist, _> = "::/0".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::AllowsAllAddresses),
            "expected AllowsAllAddresses, got {err}"
        );
    }

    #[test]
    fn test_internal_whitelist_rejects_mixed_with_catch_all() {
        let result: Result<InternalIpWhitelist, _> =
            "127.0.0.0/8,0.0.0.0/0".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::AllowsAllAddresses),
            "expected AllowsAllAddresses, got {err}"
        );
    }

    #[test]
    fn test_internal_whitelist_accepts_valid_ranges() {
        let internal: InternalIpWhitelist =
            "127.0.0.0/8,::1/128,172.16.0.0/12".parse().unwrap();
        let whitelist = internal.whitelist();
        assert!(whitelist.is_allowed(&"127.0.0.1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"::1".parse().unwrap()));
        assert!(whitelist.is_allowed(&"172.16.5.5".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_ipv6_support() {
        let whitelist = IpWhitelist::single("::1/128".parse().unwrap());

        assert!(whitelist.is_allowed(&"::1".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"::2".parse().unwrap()));
    }

    #[test]
    fn test_allows_all_addresses_catch_all_ipv6() {
        let whitelist: IpWhitelist = "::/0".parse().unwrap();
        assert!(whitelist.allows_all_addresses());
    }

    #[test]
    fn test_allows_all_addresses_catch_all_ipv6_in_rest_position() {
        let whitelist: IpWhitelist = "fe80::/64,::/0".parse().unwrap();
        assert!(whitelist.allows_all_addresses());
    }

    #[test]
    fn test_internal_whitelist_rejects_comma_only() {
        let result: Result<InternalIpWhitelist, _> = ",".parse();
        let err = result.unwrap_err();
        assert!(
            matches!(err, IpWhitelistParseError::AllowsAllAddresses),
            "expected AllowsAllAddresses, got {err}"
        );
    }
}
