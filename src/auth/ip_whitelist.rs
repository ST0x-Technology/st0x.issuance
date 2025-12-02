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
            warn!(
                "IP whitelist is empty - all IPs will be allowed. \
                 This is NOT RECOMMENDED for production."
            );
            return Ok(Self::AllowAll);
        }

        let ranges: Vec<IpNetwork> = s
            .split(',')
            .map(|range| IpNetwork::from_str(range.trim()))
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_from_str_empty_returns_allow_all() {
        let whitelist: IpWhitelist = "".parse().unwrap();
        assert_eq!(whitelist, IpWhitelist::AllowAll);

        let whitelist: IpWhitelist = "  ".parse().unwrap();
        assert_eq!(whitelist, IpWhitelist::AllowAll);
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
    fn test_from_str_invalid_range_returns_error() {
        let result: Result<IpWhitelist, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_ipv6_support() {
        let whitelist = IpWhitelist::single("::1/128".parse().unwrap());

        assert!(whitelist.is_allowed(&"::1".parse().unwrap()));
        assert!(!whitelist.is_allowed(&"::2".parse().unwrap()));
    }
}
