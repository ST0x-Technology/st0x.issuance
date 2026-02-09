use std::collections::HashMap;
use std::num::ParseIntError;
use std::path::Path;
use std::str::FromStr;

/// Fireblocks API User ID for authentication.
#[derive(Debug, Clone)]
pub(crate) struct ApiUserId(String);

impl ApiUserId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ApiUserId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Fireblocks vault account identifier.
#[derive(Debug, Clone)]
pub(crate) struct VaultAccountId(String);

impl VaultAccountId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for VaultAccountId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Fireblocks asset identifier (e.g., "ETH", "BASECHAIN_ETH").
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssetId(String);

impl AssetId {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AssetId {
    type Err = AssetIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(AssetIdError::Empty);
        }
        Ok(Self(trimmed.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AssetIdError {
    #[error("asset ID cannot be empty")]
    Empty,
}

/// Fireblocks environment selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Environment {
    #[default]
    Production,
    Sandbox,
}

/// Validated Fireblocks configuration.
#[derive(Clone)]
pub struct FireblocksConfig {
    pub(crate) api_user_id: ApiUserId,
    pub(crate) secret: Vec<u8>,
    pub(crate) vault_account_id: VaultAccountId,
    pub(crate) chain_asset_ids: ChainAssetIds,
    pub(crate) environment: Environment,
}

impl std::fmt::Debug for FireblocksConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FireblocksConfig")
            .field("api_user_id", &self.api_user_id)
            .field("secret", &"[REDACTED]")
            .field("vault_account_id", &self.vault_account_id)
            .field("chain_asset_ids", &self.chain_asset_ids)
            .field("environment", &self.environment)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FireblocksConfigError {
    #[error("IO error")]
    Io(#[from] std::io::Error),
}

impl FireblocksConfig {
    pub(crate) fn new(
        api_user_id: ApiUserId,
        secret_path: &Path,
        vault_account_id: VaultAccountId,
        chain_asset_ids: ChainAssetIds,
        environment: Environment,
    ) -> Result<Self, FireblocksConfigError> {
        let secret = std::fs::read(secret_path)?;

        Ok(Self {
            api_user_id,
            secret,
            vault_account_id,
            chain_asset_ids,
            environment,
        })
    }
}

/// Parsed chain ID to Fireblocks asset ID mapping.
#[derive(Debug, Clone)]
pub(crate) struct ChainAssetIds {
    map: HashMap<u64, AssetId>,
    default_asset_id: AssetId,
}

impl ChainAssetIds {
    pub(crate) fn get(&self, chain_id: u64) -> Option<&AssetId> {
        self.map.get(&chain_id)
    }

    pub(crate) const fn default_asset_id(&self) -> &AssetId {
        &self.default_asset_id
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChainAssetParseError {
    #[error("empty input: at least one chain:asset pair is required")]
    Empty,
    #[error(
        "invalid chain:asset pair {pair:?}: expected format like \"1:ETH\""
    )]
    InvalidFormat { pair: String },
    #[error("invalid chain ID")]
    InvalidChainId(#[from] ParseIntError),
    #[error("invalid asset ID")]
    InvalidAssetId(#[from] AssetIdError),
}

pub(crate) fn parse_chain_asset_ids(
    s: &str,
) -> Result<ChainAssetIds, ChainAssetParseError> {
    let pairs: Vec<(u64, AssetId)> = s
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(parse_single_pair)
        .collect::<Result<_, _>>()?;

    let (default_asset_id, map) = pairs.into_iter().enumerate().fold(
        (None, HashMap::new()),
        |(default, mut map), (idx, (chain_id, asset_id))| {
            map.insert(chain_id, asset_id.clone());
            let default = if idx == 0 { Some(asset_id) } else { default };
            (default, map)
        },
    );

    let default_asset_id =
        default_asset_id.ok_or(ChainAssetParseError::Empty)?;

    Ok(ChainAssetIds { map, default_asset_id })
}

fn parse_single_pair(
    pair: &str,
) -> Result<(u64, AssetId), ChainAssetParseError> {
    let (chain_str, asset_str) = pair.split_once(':').ok_or_else(|| {
        ChainAssetParseError::InvalidFormat { pair: pair.to_string() }
    })?;

    let chain_id: u64 = chain_str.trim().parse()?;
    let asset_id: AssetId = asset_str.parse()?;

    Ok((chain_id, asset_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_chain() {
        let ids = parse_chain_asset_ids("1:ETH").unwrap();
        assert_eq!(ids.get(1).unwrap().as_str(), "ETH");
        assert_eq!(ids.default_asset_id().as_str(), "ETH");
        assert!(ids.get(8453).is_none());
    }

    #[test]
    fn parse_multiple_chains() {
        let ids = parse_chain_asset_ids("1:ETH,8453:BASECHAIN_ETH").unwrap();
        assert_eq!(ids.get(1).unwrap().as_str(), "ETH");
        assert_eq!(ids.get(8453).unwrap().as_str(), "BASECHAIN_ETH");
        assert_eq!(ids.default_asset_id().as_str(), "ETH");
    }

    #[test]
    fn parse_with_whitespace() {
        let ids =
            parse_chain_asset_ids(" 1 : ETH , 8453 : BASECHAIN_ETH ").unwrap();
        assert_eq!(ids.get(1).unwrap().as_str(), "ETH");
        assert_eq!(ids.get(8453).unwrap().as_str(), "BASECHAIN_ETH");
    }

    #[test]
    fn parse_invalid_format() {
        let result = parse_chain_asset_ids("ETH");
        assert!(matches!(
            result,
            Err(ChainAssetParseError::InvalidFormat { .. })
        ));
    }

    #[test]
    fn parse_invalid_chain_id() {
        let result = parse_chain_asset_ids("abc:ETH");
        assert!(matches!(result, Err(ChainAssetParseError::InvalidChainId(_))));
    }

    #[test]
    fn parse_empty_string() {
        let result = parse_chain_asset_ids("");
        assert!(matches!(result, Err(ChainAssetParseError::Empty)));
    }

    #[test]
    fn parse_empty_asset_id() {
        let result = parse_chain_asset_ids("1:");
        assert!(matches!(result, Err(ChainAssetParseError::InvalidAssetId(_))));
    }

    #[test]
    fn default_is_first_entry() {
        let ids = parse_chain_asset_ids("8453:BASECHAIN_ETH,1:ETH").unwrap();
        assert_eq!(ids.default_asset_id().as_str(), "BASECHAIN_ETH");
    }
}
