//! Wire types for the st0x.issuance HTTP API, shared by the server, Rust
//! clients, and (via `ts-rs`) the TypeScript dashboard.
//!
//! These derive [`ts_rs::TS`] so the dashboard can build against generated
//! bindings without depending on the backend crate. The serde representation is
//! the API contract (`snake_case`) -- do not add `rename_all` without versioning
//! the endpoints.

use std::path::Path;
use std::str::FromStr;

use alloy_primitives::{Address, B256, Bytes, U256};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use uuid::Uuid;

/// Underlying equity symbol, e.g. `SGOV`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[ts(type = "string")]
pub struct UnderlyingSymbol(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnderlyingSymbolError {
    #[error("underlying symbol must not be empty")]
    Empty,
}

impl UnderlyingSymbol {
    /// Constructs an underlying symbol from a non-empty wire value.
    ///
    /// # Errors
    ///
    /// Returns [`UnderlyingSymbolError::Empty`] when `value` is empty or
    /// whitespace-only after trimming. Surrounding whitespace is stripped from
    /// the stored symbol.
    pub fn new(
        value: impl Into<String>,
    ) -> Result<Self, UnderlyingSymbolError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(UnderlyingSymbolError::Empty);
        }
        Ok(Self(trimmed.to_string()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for UnderlyingSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for UnderlyingSymbol {
    type Err = UnderlyingSymbolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for UnderlyingSymbol {
    type Error = UnderlyingSymbolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for UnderlyingSymbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Tokenized share symbol, e.g. `tSGOV`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    TS,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TokenSymbol(pub String);

impl TokenSymbol {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for TokenSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Blockchain network a tokenized asset lives on.
///
/// Variants we issue on today. Each serializes to the lowercase Alpaca ITN
/// `TokenizationNetwork` wire string (`"base"`, `"ethereum"`, ...). Valid
/// values are enumerated in Alpaca's redeem-callback OpenAPI:
/// <https://docs.alpaca.markets/reference/posttokenizationredeem>
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum Network {
    Base,
    /// Ethereum L1.
    ///
    /// The wire value `"ethereum"` follows the Alpaca ITN issuer guide's
    /// `network` enum
    /// (<https://docs.alpaca.markets/docs/tokenization-guide-for-issuer>);
    /// the staging sandbox mint/redeem validation confirms it against live
    /// Alpaca before any production cutover.
    Ethereum,
    /// HyperEVM, Hyperliquid's EVM layer (chain 999).
    ///
    /// Not yet listed in Alpaca's published `TokenizationNetwork` enum
    /// (<https://docs.alpaca.markets/reference/posttokenizationredeem>), so
    /// the `"hyperevm"` wire value must be confirmed against live Alpaca
    /// before ITN mint/redeem traffic names this network.
    #[serde(rename = "hyperevm")]
    HyperEvm,
    /// Robinhood Chain, Robinhood's Arbitrum Orbit L2 (chain 4663).
    ///
    /// Not yet listed in Alpaca's published `TokenizationNetwork` enum
    /// (<https://docs.alpaca.markets/reference/posttokenizationredeem>), so
    /// the `"robinhood"` wire value must be confirmed against live Alpaca
    /// before ITN mint/redeem traffic names this network.
    #[serde(rename = "robinhood")]
    Robinhood,
    /// BNB Smart Chain (chain 56).
    ///
    /// Alpaca publishes this network as `"binance"`, not `"bnb"`, in its
    /// `TokenizationNetwork` enum
    /// (<https://docs.alpaca.markets/reference/posttokenizationredeem>), so
    /// the wire value deliberately differs from the variant name: the wire
    /// string is Alpaca's contract, while the variant names the chain as it
    /// is known everywhere else.
    #[serde(rename = "binance")]
    BnbSmartChain,
}

impl Network {
    /// The lowercase wire string for this network, matching the serde encoding.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Ethereum => "ethereum",
            Self::HyperEvm => "hyperevm",
            Self::Robinhood => "robinhood",
            Self::BnbSmartChain => "binance",
        }
    }

    /// The canonical chain id this network denotes.
    ///
    /// Exists so configuration can reject a network label bound to a chain it
    /// does not name. Without it, `CHAIN_BASE_CHAIN_ID` pointed at a testnet
    /// yields a running `Network::Base` runtime on the wrong chain — and since
    /// the receipt inventory is keyed by chain id, that silently orphans every
    /// receipt aggregate the network already had.
    #[must_use]
    pub const fn chain_id(&self) -> u64 {
        match self {
            Self::Base => 8453,
            Self::Ethereum => 1,
            Self::HyperEvm => 999,
            Self::Robinhood => 4663,
            Self::BnbSmartChain => 56,
        }
    }

    /// The symbol of the native token that pays gas on this network.
    ///
    /// Used when rendering native balance amounts for operators: Base,
    /// Ethereum, and Robinhood Chain (an Arbitrum Orbit L2, chain 4663) pay
    /// gas in ETH, HyperEVM (chain 999) pays gas in HYPE, and BNB Smart Chain
    /// (chain 56) pays gas in BNB.
    #[must_use]
    pub const fn native_currency(&self) -> &'static str {
        match self {
            Self::Base | Self::Ethereum | Self::Robinhood => "ETH",
            Self::HyperEvm => "HYPE",
            Self::BnbSmartChain => "BNB",
        }
    }
}

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetworkParseError {
    #[error("unsupported network: {value}")]
    Unsupported { value: String },
}

impl FromStr for Network {
    type Err = NetworkParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "base" => Ok(Self::Base),
            "ethereum" => Ok(Self::Ethereum),
            "hyperevm" => Ok(Self::HyperEvm),
            "robinhood" => Ok(Self::Robinhood),
            "binance" => Ok(Self::BnbSmartChain),
            other => {
                Err(NetworkParseError::Unsupported { value: other.to_string() })
            }
        }
    }
}

/// Composite `TokenizedAsset` aggregate id and lookup key: `{underlying}:{network}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, TS)]
#[ts(type = "string")]
pub struct AssetKey {
    pub underlying: UnderlyingSymbol,
    pub network: Network,
}

/// `AssetKey` serializes as the plain `{underlying}:{network}` string, so its
/// OpenAPI schema must be a string as well -- the derived `ToSchema` would
/// advertise an object with `underlying`/`network` properties that never
/// appears on the wire.
#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for AssetKey {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        <String as utoipa::PartialSchema>::schema()
    }
}

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for AssetKey {}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AssetKeyParseError {
    #[error("asset key must be {{underlying}}:{{network}}, got: {value}")]
    InvalidFormat { value: String },
    #[error(transparent)]
    UnderlyingSymbol(#[from] UnderlyingSymbolError),
    #[error(transparent)]
    Network(#[from] NetworkParseError),
}

impl AssetKey {
    #[must_use]
    pub const fn new(underlying: UnderlyingSymbol, network: Network) -> Self {
        Self { underlying, network }
    }
}

impl std::fmt::Display for AssetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.underlying, self.network)
    }
}

impl FromStr for AssetKey {
    type Err = AssetKeyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (underlying, network_str) =
            value.rsplit_once(':').ok_or_else(|| {
                AssetKeyParseError::InvalidFormat { value: value.to_string() }
            })?;
        let underlying = UnderlyingSymbol::new(underlying)?;
        let network = network_str.parse()?;
        Ok(Self::new(underlying, network))
    }
}

impl Serialize for AssetKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for AssetKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Registered account email, normalized to trimmed lowercase.
///
/// Internal operator wire type deliberately not exported to the dashboard
/// TypeScript bindings (no `TS` derive): the dashboard never registers
/// accounts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Email(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EmailError {
    #[error("Invalid email format: {email}")]
    Invalid { email: String },
}

impl Email {
    /// Constructs an email from new input, enforcing every current rule.
    ///
    /// # Errors
    ///
    /// Returns [`EmailError::Invalid`] when the normalized value lacks exactly
    /// one `@` between a non-empty local part and a non-empty domain, or
    /// contains embedded whitespace or control characters.
    pub fn new(email: &str) -> Result<Self, EmailError> {
        let normalized = Self::checked_structure(email)?;

        // Reject embedded whitespace/control characters — `trim()` only strips
        // the ends, so "user @domain.com" or "user@do main.com" would otherwise
        // pass. No valid address contains them. New input only: values already
        // committed to the event log were accepted by the older, laxer
        // validator and must keep deserializing (see `deserialize_stored`).
        if normalized.contains(|character: char| {
            character.is_whitespace() || character.is_control()
        }) {
            return Err(EmailError::Invalid { email: normalized });
        }

        Ok(Self(normalized))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Deserializer for `Email` values already committed to the event log or
    /// projected view rows, used via
    /// `#[serde(deserialize_with = "Email::deserialize_stored")]`.
    ///
    /// Stored values were validated by the rules in force when they were
    /// written, so only the structural checks the validator has always
    /// enforced apply here. Checks added later — the embedded
    /// whitespace/control rejection in [`Email::new`] — must not apply
    /// retroactively: a historical event the old validator accepted would
    /// otherwise fail deserialization and brick event replay, view reads, and
    /// service startup. New input still validates strictly via [`Email::new`]
    /// (the default `Deserialize` impl, which API request bodies use).
    ///
    /// # Errors
    ///
    /// Returns the deserializer's error when the value is not a string or
    /// fails the structural checks.
    pub fn deserialize_stored<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::checked_structure(&value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }

    /// Normalization plus the structural checks the validator has enforced
    /// since account registration shipped: exactly one `@` separating a
    /// non-empty local part from a non-empty domain.
    fn checked_structure(email: &str) -> Result<String, EmailError> {
        let normalized = email.trim().to_lowercase();

        let structure_valid = match normalized.split_once('@') {
            Some((local, domain)) => {
                !local.is_empty() && !domain.is_empty() && !domain.contains('@')
            }
            None => false,
        };

        if !structure_valid {
            return Err(EmailError::Invalid { email: normalized });
        }

        Ok(normalized)
    }
}

impl std::fmt::Display for Email {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for Email {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(&value).map_err(serde::de::Error::custom)
    }
}

/// Single supported asset, as returned by `GET /tokenized-assets/<underlying>`.
///
/// Carries the full [`TokenizedAssetStatus`] rather than an `enabled: bool`: a
/// bool cannot represent `Frozen`, which forced a lossy mapping that reported a
/// frozen asset as enabled. The enum keeps the freeze state visible to
/// consumers and the contradictory states unrepresentable.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TokenizedAssetDetailResponse {
    pub underlying: UnderlyingSymbol,
    pub token: TokenSymbol,
    pub network: Network,
    #[ts(type = "string")]
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub vault: Address,
    pub status: TokenizedAssetStatus,
}

/// Whether a supported tokenized asset currently accepts new mints.
///
/// Mirrors the domain `AssetStatus` one-to-one. Freezing gates new mints, so the
/// liquidity rebalance guard skips `Frozen` assets. Unknown or unsupported
/// assets are surfaced as `404` (the client maps that to `None`), never a
/// variant here — so every value is a reachable state, and the contradictory
/// `{ enabled, frozen }` boolean combinations (e.g. de-listed yet frozen) that
/// the old two-bool shape allowed are now unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum TokenizedAssetStatus {
    /// Accepting new mints (not frozen).
    Enabled,
    /// New mints are gated; the rebalance guard must skip this asset.
    Frozen,
}

/// Which minting path the issuance bot uses for an asset.
///
/// The liquidity bot's cue for which assets need a signed `MintAuthV1`
/// delivered before their mints can submit: `Orchestrator` assets do,
/// `VaultDirect` assets do not. Deliberately omits the orchestrator address —
/// consumers only need the tag; the issuance bot's config stays the single
/// source of truth for addresses during the cutover.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, TS,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum VaultModeTag {
    /// Mints deposit directly into the vault; no recipient authorization.
    /// The default: a server that predates the field can only vault-direct.
    #[default]
    VaultDirect,
    /// Mints go through the ST0xOrchestrator and require a recipient
    /// authorization before submission.
    Orchestrator,
}

/// Per-asset status, returned by
/// `GET /tokenized-assets/<underlying>/status` and consumed by the liquidity
/// rebalance guard.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TokenizedAssetStatusResponse {
    pub underlying: UnderlyingSymbol,
    pub status: TokenizedAssetStatus,
    /// Additive: absent in responses from servers that predate the field,
    /// which only ever mint vault-direct — so the default is truthful. The
    /// TS binding mirrors that absence-tolerance as an optional field
    /// (`vault_mode?`), so a consumer generated from it handles the
    /// rolling-deploy window where a pre-field server omits the value.
    #[serde(default)]
    #[ts(as = "Option<VaultModeTag>", optional)]
    pub vault_mode: VaultModeTag,
}

/// Request body of `POST /internal/mints/<tokenization_request_id>/authorization`.
///
/// The liquidity bot's signed `MintAuthV1` recipient authorization for one
/// orchestrator-mode mint, correlated by the path's `tokenization_request_id`
/// which is the only mint identifier the liquidity bot holds.
///
/// Internal service-to-service wire type deliberately not exported to the
/// dashboard TypeScript bindings (no `TS` derive) — the dashboard never
/// sees this channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct MintAuthorizationRequest {
    /// Recipient-chosen random 32-byte nonce, hex-encoded. Fixed per mint:
    /// a retried delivery MUST carry the same nonce byte-identically.
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub nonce: B256,
    /// EIP-712 `MintAuthV1` signature over `(token, to, amount, nonce)` by
    /// the recipient wallet key, hex-encoded. May be `"0x"` (empty) for
    /// contract recipients authorized via the orchestrator's
    /// `authorizeMint` callback.
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub signature: Bytes,
}

/// Response body of a successful (`200`) mint-authorization delivery.
///
/// Internal service-to-service wire type; not exported to the dashboard
/// bindings (see [`MintAuthorizationRequest`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct MintAuthorizationResponse {
    /// The issuance bot's own id for the resolved mint. Opaque to callers —
    /// informational only; the wire correlation key remains the
    /// `tokenization_request_id`.
    pub issuer_request_id: String,
    /// `"authorized"` on success.
    pub status: String,
}

/// One entry in the `GET /tokenized-assets` list.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TokenizedAssetResponse {
    pub underlying: UnderlyingSymbol,
    pub token: TokenSymbol,
    pub networks: Vec<Network>,
}

/// Response body of `GET /tokenized-assets`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct TokenizedAssetsListResponse {
    pub tokens: Vec<TokenizedAssetResponse>,
}

/// Request body of `POST /tokenized-assets`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AddTokenizedAssetRequest {
    pub underlying: UnderlyingSymbol,
    pub token: TokenSymbol,
    pub network: Network,
    #[ts(type = "string")]
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub vault: Address,
}

/// Response body of `POST /tokenized-assets`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AddTokenizedAssetResponse {
    pub underlying: UnderlyingSymbol,
}

/// Request body of `POST /accounts` and its `POST /ops/debug/accounts` twin.
///
/// Internal operator wire type; not exported to the dashboard bindings (see
/// [`MintAuthorizationRequest`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RegisterAccountRequest {
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub email: Email,
}

/// Request body of `POST /accounts/<client_id>/wallets` and its
/// `POST /ops/debug/accounts/<client_id>/wallets` twin.
///
/// Internal operator wire type; not exported to the dashboard bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct WhitelistWalletRequest {
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub wallet: Address,
}

/// Request body of `POST /admin/freeze-schedules` and its
/// `POST /ops/capital/freeze-schedules` twin.
///
/// Internal operator wire type; not exported to the dashboard bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ScheduleFreezeWindowRequest {
    /// Underlying symbol whose supply freezes for the corporate action.
    pub underlying: UnderlyingSymbol,
    /// Instant the `Freeze` fires. May already be in the past for an
    /// in-progress window (the freeze then applies immediately).
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub freeze_at: DateTime<Utc>,
    /// Instant the `Unfreeze` fires. Must be after `freeze_at` and in the
    /// future.
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub unfreeze_at: DateTime<Utc>,
}

/// Request body of `POST /admin/close/redemption/<issuer_request_id>` and its
/// `POST /ops/breakglass/close/redemption/<issuer_request_id>` twin.
///
/// Internal operator wire type; not exported to the dashboard bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CloseRedemptionRequest {
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub acknowledged_unresolved_burn_tx_hash: Option<B256>,
}

/// Request body of `POST /admin/force-complete/redemption/<issuer_request_id>`
/// and its `POST /ops/breakglass/force-complete/redemption/<issuer_request_id>`
/// twin.
///
/// Internal operator wire type; not exported to the dashboard bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ForceCompleteRedemptionRequest {
    /// On-chain transaction hash that burned the redemption's shares. Verified
    /// against the chain before the redemption is terminalized.
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub burn_tx_hash: B256,
    /// Operator-supplied audit reason recorded with the terminal event.
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub acknowledged_unresolved_burn_tx_hash: Option<B256>,
}

/// Request body of `POST /admin/close/mint/<aggregate_id>` and its
/// `POST /ops/breakglass/close/mint/<aggregate_id>` twin.
///
/// Internal operator wire type; not exported to the dashboard bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CloseMintRequest {
    pub reason: String,
    /// Required when the mint still holds a prepared deposit identity: must
    /// equal the exact `MintTxIntended` / prepared hash. Omit only when the
    /// mint has no prepared identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub acknowledged_unresolved_mint_tx_hash: Option<B256>,
    /// Required only to close a `NonceReplayUnresolved` mint, and must
    /// exactly echo that mint's persisted authorization nonce; rejected on
    /// any other mint. Records that an operator verified the nonce's
    /// absence against a chain view outside this bot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub acknowledged_unresolved_mint_nonce: Option<B256>,
}

/// An 18-decimal fixed-point share amount in its decimal-string wire form,
/// e.g. `"0.750"`.
///
/// Parsed at construction, so an invalid or over-precise quantity is refused
/// before any handler runs. Private fields; [`FromStr`] (and the
/// [`Deserialize`] impl over it) is the only constructor, so a `DecimalShares`
/// that exists is a valid on-chain amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalShares {
    decimal: Decimal,
    fixed_point: U256,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DecimalSharesError {
    #[error("invalid shares decimal: {0}")]
    Decimal(#[from] rust_decimal::Error),
    #[error("shares must be greater than zero")]
    Zero,
    #[error("shares must not be negative: {value}")]
    Negative { value: Decimal },
    #[error("shares {value} overflow when scaled to 18 decimals")]
    Overflow { value: Decimal },
    #[error("shares {value} have more than 18 decimal places")]
    TooPrecise { value: Decimal },
    #[error("shares {value} exceed the representable on-chain amount")]
    OutOfRange { value: Decimal },
}

impl DecimalShares {
    /// The on-chain amount: the decimal scaled by 10^18.
    #[must_use]
    pub const fn to_u256(&self) -> U256 {
        self.fixed_point
    }
}

impl FromStr for DecimalShares {
    type Err = DecimalSharesError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // `from_str_exact`, not `from_str`: the latter rounds past 28
        // significant digits and falls back to a lossy scientific parser, so
        // an over-precise or exponent amount would be checked only after it
        // had already been rounded.
        let decimal = Decimal::from_str_exact(value)?;

        // Checked before the sign so `-0` reads as a zero burn, not a negative
        // one. A zero excess is never a burn to run: refusing it here keeps a
        // malformed request from pausing the redemption poller and fetching
        // the deposit proof before the engine's share-mismatch refusal.
        if decimal.is_zero() {
            return Err(DecimalSharesError::Zero);
        }

        if decimal.is_sign_negative() {
            return Err(DecimalSharesError::Negative { value: decimal });
        }

        let scaled = decimal
            .checked_mul(Decimal::from(10_u128.pow(18)))
            .ok_or(DecimalSharesError::Overflow { value: decimal })?;

        if scaled.fract() != Decimal::ZERO {
            return Err(DecimalSharesError::TooPrecise { value: decimal });
        }

        let units = scaled
            .to_u128()
            .ok_or(DecimalSharesError::OutOfRange { value: decimal })?;

        Ok(Self { decimal, fixed_point: U256::from(units) })
    }
}

impl std::fmt::Display for DecimalShares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.decimal)
    }
}

impl Serialize for DecimalShares {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for DecimalShares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Exact issuer-wallet inbound Transfer an operator has reconciled outside the
/// ordinary redemption/exclusion terminal paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AcknowledgedInboundTransfer {
    pub tx_hash: B256,
    pub log_index: u64,
}

impl std::fmt::Display for AcknowledgedInboundTransfer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.tx_hash, self.log_index)
    }
}

impl FromStr for AcknowledgedInboundTransfer {
    type Err = AcknowledgedInboundTransferError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (tx_hash, log_index) = value
            .rsplit_once(':')
            .ok_or(AcknowledgedInboundTransferError::MissingSeparator)?;
        Ok(Self { tx_hash: tx_hash.parse()?, log_index: log_index.parse()? })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AcknowledgedInboundTransferError {
    #[error("expected TX_HASH:LOG_INDEX")]
    MissingSeparator,
    #[error(transparent)]
    Hex(#[from] alloy_primitives::hex::FromHexError),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
}

/// Fields shared by every burn-excess request body. `#[serde(flatten)]` folds
/// these into each, so a field added here changes every route's contract at
/// once, never just one.
///
/// Internal operator wire type; not exported to the dashboard bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnExcessCommon {
    /// The mint whose deposit produced the excess.
    pub issuer_request_id: Uuid,
    /// Deposit transaction that created the excess receipt and shares.
    pub deposit_tx_hash: B256,
    /// Excess receipt id from the deposit.
    pub receipt_id: U256,
    pub shares: DecimalShares,
    /// Why this recovery is being run; recorded on events.
    pub reason: String,
    /// Optional incident or ticket id for the audit trail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incident_id: Option<String>,
    /// Exact inbound Transfers manually reconciled by an operator. Each
    /// identity is proven on-chain and persisted with the signed burn intent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acknowledged_inflows: Vec<AcknowledgedInboundTransfer>,
    pub network: Network,
    /// Validated against the network's configured chain entry.
    pub chain_id: u64,
    /// Perform the mutation (sign and broadcast the burn; the external path
    /// also writes the funding exclusion; `expect-funding` records the
    /// expectation). Default is a dry-run that proves the plan without
    /// touching chain or state.
    #[serde(default)]
    pub execute: bool,
    /// Close the stream instead of burning: a dead intended/submitted burn,
    /// or an expectation whose funding was never sent.
    #[serde(default)]
    pub close: bool,
}

/// Request body of `POST /ops/breakglass/burn-excess/internal`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnExcessInternalRequest {
    #[serde(flatten)]
    pub common: BurnExcessCommon,
}

/// Request body of `POST /ops/breakglass/burn-excess/expect-funding`, sent
/// before the external path's funding Transfer is broadcast.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnExcessExpectFundingRequest {
    #[serde(flatten)]
    pub common: BurnExcessCommon,
}

/// Request body of `POST /ops/breakglass/burn-excess/external`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnExcessExternalRequest {
    /// Funding Transfer that moved the excess shares into the issuer wallet.
    pub funding_tx_hash: B256,
    #[serde(flatten)]
    pub common: BurnExcessCommon,
}

/// Exports every DTO's TypeScript binding into `out_dir` (one `.ts` file per
/// type). The caller controls the output location explicitly.
///
/// # Errors
///
/// Returns [`ts_rs::ExportError`] if any binding file cannot be written.
pub fn export_bindings(out_dir: &Path) -> Result<(), ts_rs::ExportError> {
    UnderlyingSymbol::export_all_to(out_dir)?;
    TokenSymbol::export_all_to(out_dir)?;
    Network::export_all_to(out_dir)?;
    AssetKey::export_all_to(out_dir)?;
    TokenizedAssetDetailResponse::export_all_to(out_dir)?;
    TokenizedAssetStatus::export_all_to(out_dir)?;
    VaultModeTag::export_all_to(out_dir)?;
    TokenizedAssetStatusResponse::export_all_to(out_dir)?;
    TokenizedAssetResponse::export_all_to(out_dir)?;
    TokenizedAssetsListResponse::export_all_to(out_dir)?;
    AddTokenizedAssetRequest::export_all_to(out_dir)?;
    AddTokenizedAssetResponse::export_all_to(out_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    fn vault() -> Address {
        Address::from([0xab; 20])
    }

    #[test]
    fn underlying_symbol_rejects_empty_and_whitespace() {
        assert!(UnderlyingSymbol::new("").is_err());
        assert!(UnderlyingSymbol::new("   ").is_err());
        assert_eq!(UnderlyingSymbol::new("AAPL").unwrap().as_str(), "AAPL");
        assert_eq!(UnderlyingSymbol::new(" AAPL ").unwrap().as_str(), "AAPL");
        assert_eq!(
            serde_json::from_value::<UnderlyingSymbol>(json!(" AAPL "))
                .unwrap(),
            UnderlyingSymbol::new("AAPL").unwrap()
        );
    }

    prop_compose! {
        fn arb_trimmed_symbol()(core in "[A-Z0-9][A-Z0-9._/-]{0,9}") -> String {
            core
        }
    }

    prop_compose! {
        fn arb_whitespace()(ws in "[ \t]{0,8}") -> String {
            ws
        }
    }

    proptest! {
        #[test]
        fn underlying_symbol_stores_trimmed_value(
            core in arb_trimmed_symbol(),
            prefix in arb_whitespace(),
            suffix in arb_whitespace(),
        ) {
            let padded = format!("{prefix}{core}{suffix}");
            let symbol = UnderlyingSymbol::new(&padded).unwrap();
            prop_assert_eq!(symbol.as_str(), core);
            prop_assert_eq!(symbol.as_str().trim(), symbol.as_str());
        }

        #[test]
        fn underlying_symbol_rejects_whitespace_only(ws in "[ \t]+") {
            prop_assert!(UnderlyingSymbol::new(ws).is_err());
        }

        #[test]
        fn underlying_symbol_from_str_agrees_with_new(
            core in arb_trimmed_symbol(),
            prefix in arb_whitespace(),
            suffix in arb_whitespace(),
        ) {
            let padded = format!("{prefix}{core}{suffix}");
            prop_assert_eq!(
                UnderlyingSymbol::from_str(&padded).unwrap(),
                UnderlyingSymbol::new(&padded).unwrap(),
            );
        }

        #[test]
        fn underlying_symbol_serde_preserves_trimmed_value(
            core in arb_trimmed_symbol(),
            prefix in arb_whitespace(),
            suffix in arb_whitespace(),
        ) {
            let symbol = UnderlyingSymbol::new(&core).unwrap();
            let serialized = serde_json::to_value(&symbol).unwrap();
            prop_assert_eq!(serialized, json!(core));

            let from_padded = serde_json::from_value::<UnderlyingSymbol>(
                json!(format!("{prefix}{core}{suffix}"))
            )
            .unwrap();
            prop_assert_eq!(from_padded, symbol);
        }
    }

    #[test]
    fn newtypes_serialize_as_bare_strings() {
        assert_eq!(
            serde_json::to_value(UnderlyingSymbol::new("SGOV").unwrap())
                .unwrap(),
            json!("SGOV")
        );
        assert_eq!(
            serde_json::to_value(TokenSymbol::new("tSGOV")).unwrap(),
            json!("tSGOV")
        );
        assert_eq!(serde_json::to_value(Network::Base).unwrap(), json!("base"));
        assert_eq!(
            serde_json::to_value(Network::Ethereum).unwrap(),
            json!("ethereum")
        );
    }

    #[test]
    fn newtypes_deserialize_from_bare_strings() {
        assert_eq!(
            serde_json::from_value::<UnderlyingSymbol>(json!("SGOV")).unwrap(),
            UnderlyingSymbol::new("SGOV").unwrap()
        );
        assert_eq!(
            serde_json::from_value::<TokenSymbol>(json!("tSGOV")).unwrap(),
            TokenSymbol::new("tSGOV")
        );
        assert_eq!(
            serde_json::from_value::<Network>(json!("base")).unwrap(),
            Network::Base
        );
        assert_eq!(
            serde_json::from_value::<Network>(json!("ethereum")).unwrap(),
            Network::Ethereum
        );
        assert_eq!(
            serde_json::from_value::<Network>(json!("robinhood")).unwrap(),
            Network::Robinhood
        );
        // BNB Smart Chain rides on Alpaca's spelling, so the variant name and
        // the wire value diverge; the wire value is the contract.
        assert_eq!(
            serde_json::from_value::<Network>(json!("binance")).unwrap(),
            Network::BnbSmartChain
        );
    }

    // `Network` is a closed enum, so an unsupported or wrong-cased network must
    // fail to deserialize rather than flow through as an opaque string — the
    // invariant that replaced the old unvalidated `Network(String)` newtype.
    #[test]
    fn network_rejects_unknown_and_non_snake_case_variants() {
        for invalid in [
            json!("arbitrum"),
            json!("Base"),
            json!("BASE"),
            json!(""),
            // The chain's own name is not the wire value Alpaca publishes.
            json!("bnb"),
            json!("bnb_smart_chain"),
        ] {
            assert!(
                serde_json::from_value::<Network>(invalid.clone()).is_err(),
                "{invalid} must not deserialize as Network"
            );
        }
        assert!(
            serde_json::from_value::<UnderlyingSymbol>(json!("")).is_err(),
            "empty underlying must not deserialize"
        );
        assert!(
            serde_json::from_value::<UnderlyingSymbol>(json!("   ")).is_err(),
            "whitespace-only underlying must not deserialize"
        );
    }

    #[test]
    fn newtypes_display_their_inner_value() {
        assert_eq!(UnderlyingSymbol::new("SGOV").unwrap().to_string(), "SGOV");
        assert_eq!(TokenSymbol::new("tSGOV").to_string(), "tSGOV");
        assert_eq!(Network::Base.to_string(), "base");
        assert_eq!(Network::Ethereum.to_string(), "ethereum");
        assert_eq!(Network::Robinhood.to_string(), "robinhood");
        assert_eq!(Network::BnbSmartChain.to_string(), "binance");
    }

    // The chain id is what re-keys the receipt inventory when a network label
    // is bound to the wrong chain, and the native currency is what operator
    // gas alerts are denominated in; both are per-variant constants with no
    // other guard, so pin them.
    #[test]
    fn networks_carry_their_canonical_chain_id_and_native_currency() {
        let expected = [
            (Network::Base, 8453, "ETH"),
            (Network::Ethereum, 1, "ETH"),
            (Network::HyperEvm, 999, "HYPE"),
            // Robinhood Chain is an Arbitrum Orbit L2 and pays gas in ETH.
            (Network::Robinhood, 4663, "ETH"),
            (Network::BnbSmartChain, 56, "BNB"),
        ];

        for (network, chain_id, native_currency) in expected {
            assert_eq!(network.chain_id(), chain_id, "chain id for {network}");
            assert_eq!(
                network.native_currency(),
                native_currency,
                "native currency for {network}"
            );
        }
    }

    #[test]
    fn network_from_str_parses_wire_values() {
        assert_eq!("base".parse::<Network>().unwrap(), Network::Base);
        assert_eq!("ethereum".parse::<Network>().unwrap(), Network::Ethereum);
        assert_eq!("robinhood".parse::<Network>().unwrap(), Network::Robinhood);
        assert_eq!(
            "binance".parse::<Network>().unwrap(),
            Network::BnbSmartChain
        );
        assert!("arbitrum".parse::<Network>().is_err());
        assert!("bnb".parse::<Network>().is_err());
    }

    #[test]
    fn status_response_uses_snake_case_wire_format() {
        let response = TokenizedAssetStatusResponse {
            underlying: UnderlyingSymbol::new("SGOV").unwrap(),
            status: TokenizedAssetStatus::Frozen,
            vault_mode: VaultModeTag::Orchestrator,
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "underlying": "SGOV",
                "status": "frozen",
                "vault_mode": "orchestrator"
            })
        );
    }

    #[test]
    fn status_response_deserializes_from_wire() {
        let response: TokenizedAssetStatusResponse =
            serde_json::from_value(json!({
                "underlying": "SGOV",
                "status": "enabled",
                "vault_mode": "vault_direct"
            }))
            .unwrap();

        assert_eq!(response.underlying, UnderlyingSymbol::new("SGOV").unwrap());
        assert_eq!(response.status, TokenizedAssetStatus::Enabled);
        assert_eq!(response.vault_mode, VaultModeTag::VaultDirect);
    }

    /// The authorization wire shape is the cross-repo contract the liquidity
    /// bot signs against — pin it to exact JSON literals (hex-encoded
    /// `nonce`/`signature`), not just a round-trip, so a field rename or
    /// encoding change fails here.
    #[test]
    fn mint_authorization_request_pins_hex_wire_format() {
        let request = MintAuthorizationRequest {
            nonce: B256::repeat_byte(0x07),
            signature: Bytes::from(vec![0xab, 0xcd]),
        };

        let wire = json!({
            "nonce": "0x0707070707070707070707070707070707070707070707070707070707070707",
            "signature": "0xabcd"
        });
        assert_eq!(serde_json::to_value(&request).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<MintAuthorizationRequest>(wire).unwrap(),
            request
        );
    }

    /// `"0x"` is a valid signature on the wire: contract recipients
    /// authorize via the orchestrator's `authorizeMint` callback and send an
    /// empty signature (SPEC "Recipient Authorization", staged scope).
    #[test]
    fn mint_authorization_request_accepts_empty_signature() {
        let request: MintAuthorizationRequest = serde_json::from_value(json!({
            "nonce": "0x0707070707070707070707070707070707070707070707070707070707070707",
            "signature": "0x"
        }))
        .unwrap();

        assert!(request.signature.is_empty());
    }

    #[test]
    fn mint_authorization_response_round_trips_wire_format() {
        let wire = json!({
            "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "authorized"
        });

        let response: MintAuthorizationResponse =
            serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            response.issuer_request_id,
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(response.status, "authorized");
        assert_eq!(serde_json::to_value(&response).unwrap(), wire);
    }

    /// A response from a server that predates `vault_mode` still parses —
    /// and defaults to `VaultDirect`, the only mode such a server can mint.
    #[test]
    fn status_response_without_vault_mode_defaults_to_vault_direct() {
        let response: TokenizedAssetStatusResponse = serde_json::from_value(
            json!({"underlying": "SGOV", "status": "enabled"}),
        )
        .unwrap();

        assert_eq!(response.vault_mode, VaultModeTag::VaultDirect);
    }

    /// The wire format is snake_case, mirroring `TokenizedAssetStatus`: the
    /// PascalCase domain spelling and unknown variants must fail loudly.
    #[test]
    fn vault_mode_tag_rejects_non_snake_case_and_unknown_variants() {
        for invalid in
            [json!("VaultDirect"), json!("Orchestrator"), json!("direct")]
        {
            assert!(
                serde_json::from_value::<VaultModeTag>(invalid.clone())
                    .is_err(),
                "{invalid} must not deserialize as VaultModeTag"
            );
        }

        assert_eq!(
            serde_json::from_value::<VaultModeTag>(json!("vault_direct"))
                .unwrap(),
            VaultModeTag::VaultDirect
        );
        assert_eq!(
            serde_json::from_value::<VaultModeTag>(json!("orchestrator"))
                .unwrap(),
            VaultModeTag::Orchestrator
        );
    }

    // The wire format is snake_case: the PascalCase domain spelling (`Enabled`,
    // the form the view JSON stores `AssetStatus` as) and any unknown variant
    // must be rejected, so a producer/consumer that drifts onto the wrong casing
    // fails loudly instead of silently misreading freeze state.
    #[test]
    fn status_rejects_non_snake_case_and_unknown_variants() {
        for invalid in [json!("Enabled"), json!("Frozen"), json!("unavailable")]
        {
            assert!(
                serde_json::from_value::<TokenizedAssetStatus>(invalid.clone())
                    .is_err(),
                "{invalid} must not deserialize as TokenizedAssetStatus"
            );
        }
    }

    // The status endpoint deliberately moved off the old `{ enabled, frozen }`
    // two-bool body. Pin that break: the legacy shape must fail to deserialize
    // as the new response type (it has no `status` field), so the contract
    // change is explicit rather than a silent field mismatch for a stale client.
    #[test]
    fn status_response_rejects_legacy_two_bool_body() {
        assert!(
            serde_json::from_value::<TokenizedAssetStatusResponse>(json!({
                "underlying": "SGOV",
                "enabled": true,
                "frozen": false
            }))
            .is_err(),
            "the legacy two-bool body must not deserialize as the status enum response"
        );
    }

    #[test]
    fn list_response_uses_snake_case_wire_format() {
        let response = TokenizedAssetsListResponse {
            tokens: vec![TokenizedAssetResponse {
                underlying: UnderlyingSymbol::new("SGOV").unwrap(),
                token: TokenSymbol::new("tSGOV"),
                networks: vec![Network::Base],
            }],
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "tokens": [{
                    "underlying": "SGOV",
                    "token": "tSGOV",
                    "networks": ["base"]
                }]
            })
        );
    }

    #[test]
    fn detail_response_serializes_vault_as_string_and_round_trips() {
        let response = TokenizedAssetDetailResponse {
            underlying: UnderlyingSymbol::new("SGOV").unwrap(),
            token: TokenSymbol::new("tSGOV"),
            network: Network::Base,
            vault: vault(),
            status: TokenizedAssetStatus::Frozen,
        };

        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["underlying"], json!("SGOV"));
        // A frozen asset must serialize its real state, not collapse to a bool
        // that reads as "enabled" -- this is the regression the enum prevents.
        assert_eq!(value["status"], json!("frozen"));
        // Pin the exact lowercase 0x-hex form: external clients (the dashboard,
        // the RAI-1038 guard) parse this specific string, so a change to alloy's
        // Address serialization must break the test, not silently break clients.
        assert_eq!(
            value["vault"],
            json!("0xabababababababababababababababababababab")
        );

        let back: TokenizedAssetDetailResponse =
            serde_json::from_value(value).unwrap();
        assert_eq!(back.vault, vault());
    }

    #[test]
    fn add_request_deserializes_from_wire() {
        let request: AddTokenizedAssetRequest = serde_json::from_value(json!({
            "underlying": "SGOV",
            "token": "tSGOV",
            "network": "base",
            "vault": "0xabababababababababababababababababababab"
        }))
        .unwrap();

        assert_eq!(request.underlying, UnderlyingSymbol::new("SGOV").unwrap());
        assert_eq!(request.token, TokenSymbol::new("tSGOV"));
        assert_eq!(request.network, Network::Base);
        assert_eq!(request.vault, vault());
    }

    #[test]
    fn asset_key_serializes_as_underlying_colon_network() {
        let key = AssetKey::new(
            UnderlyingSymbol::new("SGOV").unwrap(),
            Network::Base,
        );
        assert_eq!(key.to_string(), "SGOV:base");
        assert_eq!(serde_json::to_value(&key).unwrap(), json!("SGOV:base"));
        assert_eq!("SGOV:base".parse::<AssetKey>().unwrap(), key);

        let eth_key = AssetKey::new(
            UnderlyingSymbol::new("TSLA").unwrap(),
            Network::Ethereum,
        );
        assert_eq!(eth_key.to_string(), "TSLA:ethereum");
        assert_eq!("TSLA:ethereum".parse::<AssetKey>().unwrap(), eth_key);
    }

    #[test]
    fn asset_key_from_str_rejects_invalid_inputs() {
        assert!(matches!(
            "SGOV".parse::<AssetKey>().unwrap_err(),
            AssetKeyParseError::InvalidFormat { .. }
        ));
        assert!(matches!(
            ":base".parse::<AssetKey>().unwrap_err(),
            AssetKeyParseError::UnderlyingSymbol(UnderlyingSymbolError::Empty)
        ));
        assert!(matches!(
            "SGOV:".parse::<AssetKey>().unwrap_err(),
            AssetKeyParseError::Network(NetworkParseError::Unsupported { .. })
        ));
        assert!(matches!(
            "SGOV:solana".parse::<AssetKey>().unwrap_err(),
            AssetKeyParseError::Network(NetworkParseError::Unsupported { .. })
        ));
    }

    #[test]
    fn add_response_uses_snake_case_wire_format() {
        let response = AddTokenizedAssetResponse {
            underlying: UnderlyingSymbol::new("SGOV").unwrap(),
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({"underlying": "SGOV"})
        );
    }

    #[test]
    fn export_bindings_writes_typescript_files() {
        let out_dir = std::env::temp_dir()
            .join(format!("st0x-issuance-dto-bindings-{}", std::process::id()));
        std::fs::create_dir_all(&out_dir).unwrap();

        export_bindings(&out_dir).unwrap();

        // Pin the generated TS shape, not just file presence: this is the other
        // half of the wire contract (the dashboard builds against these types),
        // and serde and ts_rs are derived independently — a dropped
        // `#[ts(type = "string")]` or a ts_rs change to newtype emission must
        // fail here rather than silently diverge from the JSON the server emits.
        let status_ts = std::fs::read_to_string(
            out_dir.join("TokenizedAssetStatusResponse.ts"),
        )
        .unwrap();
        assert!(
            status_ts.contains("status: TokenizedAssetStatus"),
            "status must reference the TokenizedAssetStatus union in TS:\n{status_ts}"
        );

        // The status enum must emit a string-literal union, not a struct — the
        // dashboard and the RAI-1038 guard switch on these exact wire strings.
        let status_enum_ts =
            std::fs::read_to_string(out_dir.join("TokenizedAssetStatus.ts"))
                .unwrap();
        assert!(
            status_enum_ts.contains("\"enabled\"")
                && status_enum_ts.contains("\"frozen\""),
            "TokenizedAssetStatus must be an \"enabled\" | \"frozen\" union in TS:\n{status_enum_ts}"
        );

        // Optional (`vault_mode?`), mirroring the serde default: a pre-field
        // server omits the value, and a generated consumer must not assume
        // it is always present during that rolling-deploy window.
        assert!(
            status_ts.contains("vault_mode?: VaultModeTag"),
            "vault_mode must be an OPTIONAL reference to the VaultModeTag \
             union in TS:\n{status_ts}"
        );
        let vault_mode_ts =
            std::fs::read_to_string(out_dir.join("VaultModeTag.ts")).unwrap();
        assert!(
            vault_mode_ts.contains("\"vault_direct\"")
                && vault_mode_ts.contains("\"orchestrator\""),
            "VaultModeTag must be a \"vault_direct\" | \"orchestrator\" union in TS:\n{vault_mode_ts}"
        );

        // `Network` is a closed enum, so ts_rs must emit a string-literal union
        // (`"base"`), not the bare `string` alias the old transparent newtype
        // produced — the dashboard switches on this exact wire string, so a
        // regression to `string` must fail here.
        let network_ts =
            std::fs::read_to_string(out_dir.join("Network.ts")).unwrap();
        assert!(
            network_ts.contains("\"base\""),
            "Network must be a \"base\" string-literal union in TS:\n{network_ts}"
        );

        // `AssetKey` serializes as `"underlying:network"`; pin the TS alias so
        // ts_rs can't regress to a struct shape the dashboard can't consume.
        let asset_key_ts =
            std::fs::read_to_string(out_dir.join("AssetKey.ts")).unwrap();
        assert!(
            asset_key_ts.contains("= string"),
            "AssetKey must be a string alias in TS:\n{asset_key_ts}"
        );

        // The newtypes must resolve to a bare `string`, matching their
        // transparent serde encoding — not an object like `{ 0: string }`.
        let underlying_ts =
            std::fs::read_to_string(out_dir.join("UnderlyingSymbol.ts"))
                .unwrap();
        assert!(
            underlying_ts.contains("= string"),
            "UnderlyingSymbol must be a string alias in TS:\n{underlying_ts}"
        );

        // The `vault` Address is forced to `string` via `#[ts(type = "string")]`;
        // pin it so removing that attribute (which would emit an alloy type the
        // dashboard can't consume) breaks the test.
        let add_request_ts = std::fs::read_to_string(
            out_dir.join("AddTokenizedAssetRequest.ts"),
        )
        .unwrap();
        assert!(
            add_request_ts.contains("vault: string"),
            "vault must be a string in TS:\n{add_request_ts}"
        );

        // The detail response carries both the `status` union and the
        // `#[ts(type = "string")]` vault, just like the status/add types above;
        // pin both so a dropped attribute or a status-field type change can't
        // diverge the detail binding from the JSON the server emits.
        let detail_ts = std::fs::read_to_string(
            out_dir.join("TokenizedAssetDetailResponse.ts"),
        )
        .unwrap();
        assert!(
            detail_ts.contains("status: TokenizedAssetStatus"),
            "detail status must reference the TokenizedAssetStatus union in TS:\n{detail_ts}"
        );
        assert!(
            detail_ts.contains("vault: string"),
            "detail vault must be a string in TS:\n{detail_ts}"
        );

        std::fs::remove_dir_all(&out_dir).unwrap();
    }

    #[test]
    fn test_email_smart_constructor_validates() {
        assert!(matches!(
            Email::new("not-an-email"),
            Err(EmailError::Invalid { email }) if email == "not-an-email"
        ));

        assert!(matches!(
            Email::new("@"),
            Err(EmailError::Invalid { email }) if email == "@"
        ));

        assert!(matches!(
            Email::new("user@"),
            Err(EmailError::Invalid { email }) if email == "user@"
        ));

        assert!(matches!(
            Email::new("@domain"),
            Err(EmailError::Invalid { email }) if email == "@domain"
        ));

        assert!(matches!(
            Email::new("user@@domain.com"),
            Err(EmailError::Invalid { email }) if email == "user@@domain.com"
        ));

        assert!(matches!(
            Email::new("user@domain@com"),
            Err(EmailError::Invalid { email }) if email == "user@domain@com"
        ));

        // Embedded whitespace/control chars in either part are rejected — only
        // leading/trailing whitespace is trimmed.
        assert!(matches!(
            Email::new("user @domain.com"),
            Err(EmailError::Invalid { email }) if email == "user @domain.com"
        ));
        assert!(matches!(
            Email::new("user@do main.com"),
            Err(EmailError::Invalid { email }) if email == "user@do main.com"
        ));
        assert!(matches!(
            Email::new("user\t@domain.com"),
            Err(EmailError::Invalid { email }) if email == "user\t@domain.com"
        ));

        assert!(Email::new("user@example.com").is_ok());
    }

    #[test]
    fn email_deserialize_stays_strict_for_new_input() {
        // API request bodies deserialize `Email` through the default
        // `Deserialize` impl, which must keep enforcing the full `Email::new`
        // rules — the stored-value tolerance is opt-in per field.
        let result: Result<Email, _> =
            serde_json::from_str(r#""user @domain.com""#);

        assert!(
            result.is_err(),
            "ingress deserialization must reject embedded whitespace"
        );
    }

    #[test]
    fn test_email_normalizes_trim_and_lowercase() {
        let email = Email::new("  User@Example.COM  ").unwrap();

        assert_eq!(email.0, "user@example.com");
    }

    #[test]
    fn decimal_shares_scale_a_decimal_to_18_fixed_point_places() {
        let shares: DecimalShares = "0.750".parse().unwrap();

        assert_eq!(shares.to_u256(), U256::from(750_000_000_000_000_000_u128));
        assert_eq!(
            serde_json::to_value(&shares).unwrap(),
            json!("0.750"),
            "the wire form round-trips the operator's decimal as written"
        );
        assert_eq!(
            "0.000000000000000001".parse::<DecimalShares>().unwrap().to_u256(),
            U256::from(1_u8)
        );
    }

    #[test]
    fn decimal_shares_refuse_amounts_the_chain_cannot_represent() {
        for zero in ["0", "0.000", "-0"] {
            assert!(
                matches!(
                    zero.parse::<DecimalShares>(),
                    Err(DecimalSharesError::Zero)
                ),
                "{zero} must be refused as a zero burn"
            );
        }
        assert!(matches!(
            "-1".parse::<DecimalShares>(),
            Err(DecimalSharesError::Negative { .. })
        ));
        assert!(matches!(
            "0.0000000000000000001".parse::<DecimalShares>(),
            Err(DecimalSharesError::TooPrecise { .. })
        ));
        assert!(matches!(
            "79228162514264337593543950335".parse::<DecimalShares>(),
            Err(DecimalSharesError::Overflow { .. })
        ));
        assert!(matches!(
            "one".parse::<DecimalShares>(),
            Err(DecimalSharesError::Decimal(_))
        ));
        assert!(
            "0.74999999999999999999999999999".parse::<DecimalShares>().is_err(),
            "29 fractional digits must be refused, not rounded to 0.75"
        );
        assert!(
            "0.00000000000000000000000000001".parse::<DecimalShares>().is_err(),
            "a nonzero amount must not round to zero"
        );
        assert!(
            "1e1".parse::<DecimalShares>().is_err(),
            "exponent notation goes through a lossy parser and is refused"
        );
        assert!(
            serde_json::from_value::<DecimalShares>(json!(
                "0.0000000000000000001"
            ))
            .is_err(),
            "a request body cannot carry an over-precise amount"
        );
    }
}
