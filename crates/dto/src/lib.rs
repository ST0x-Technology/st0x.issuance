//! Wire types for the st0x.issuance HTTP API, shared by the server, Rust
//! clients, and (via `ts-rs`) the TypeScript dashboard.
//!
//! These derive [`ts_rs::TS`] so the dashboard can build against generated
//! bindings without depending on the backend crate. The serde representation is
//! the API contract (snake_case) -- do not add `rename_all` without versioning
//! the endpoints.

use std::path::Path;
use std::str::FromStr;

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Underlying equity symbol, e.g. `SGOV`. Also the `TokenizedAsset` aggregate id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UnderlyingSymbol(pub String);

impl UnderlyingSymbol {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for UnderlyingSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for UnderlyingSymbol {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(value))
    }
}

/// Tokenized share symbol, e.g. `tSGOV`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
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
/// A closed set -- only `base` is supported today. Serialized as the lowercase
/// wire string (`"base"`) it has always used, so the JSON contract and the
/// values already persisted in the event store are unchanged. Modeling it as an
/// enum (rather than an opaque `String`) means an unsupported network is now a
/// deserialization error instead of a value that silently flows through.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum Network {
    Base,
}

impl Network {
    /// The lowercase wire string for this network, matching the serde encoding.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Base => "base",
        }
    }
}

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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

/// Per-asset status, returned by
/// `GET /tokenized-assets/<underlying>/status` and consumed by the liquidity
/// rebalance guard.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TokenizedAssetStatusResponse {
    pub underlying: UnderlyingSymbol,
    pub status: TokenizedAssetStatus,
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
    TokenizedAssetDetailResponse::export_all_to(out_dir)?;
    TokenizedAssetStatus::export_all_to(out_dir)?;
    TokenizedAssetStatusResponse::export_all_to(out_dir)?;
    TokenizedAssetResponse::export_all_to(out_dir)?;
    TokenizedAssetsListResponse::export_all_to(out_dir)?;
    AddTokenizedAssetRequest::export_all_to(out_dir)?;
    AddTokenizedAssetResponse::export_all_to(out_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn vault() -> Address {
        Address::from([0xab; 20])
    }

    #[test]
    fn newtypes_serialize_as_bare_strings() {
        assert_eq!(
            serde_json::to_value(UnderlyingSymbol::new("SGOV")).unwrap(),
            json!("SGOV")
        );
        assert_eq!(
            serde_json::to_value(TokenSymbol::new("tSGOV")).unwrap(),
            json!("tSGOV")
        );
        assert_eq!(serde_json::to_value(Network::Base).unwrap(), json!("base"));
    }

    #[test]
    fn newtypes_deserialize_from_bare_strings() {
        assert_eq!(
            serde_json::from_value::<UnderlyingSymbol>(json!("SGOV")).unwrap(),
            UnderlyingSymbol::new("SGOV")
        );
        assert_eq!(
            serde_json::from_value::<TokenSymbol>(json!("tSGOV")).unwrap(),
            TokenSymbol::new("tSGOV")
        );
        assert_eq!(
            serde_json::from_value::<Network>(json!("base")).unwrap(),
            Network::Base
        );
    }

    // `Network` is a closed enum, so an unsupported or wrong-cased network must
    // fail to deserialize rather than flow through as an opaque string — the
    // invariant that replaced the old unvalidated `Network(String)` newtype.
    #[test]
    fn network_rejects_unknown_and_non_snake_case_variants() {
        for invalid in
            [json!("ethereum"), json!("Base"), json!("BASE"), json!("")]
        {
            assert!(
                serde_json::from_value::<Network>(invalid.clone()).is_err(),
                "{invalid} must not deserialize as Network"
            );
        }
    }

    #[test]
    fn newtypes_display_their_inner_value() {
        assert_eq!(UnderlyingSymbol::new("SGOV").to_string(), "SGOV");
        assert_eq!(TokenSymbol::new("tSGOV").to_string(), "tSGOV");
        assert_eq!(Network::Base.to_string(), "base");
    }

    #[test]
    fn status_response_uses_snake_case_wire_format() {
        let response = TokenizedAssetStatusResponse {
            underlying: UnderlyingSymbol::new("SGOV"),
            status: TokenizedAssetStatus::Frozen,
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({"underlying": "SGOV", "status": "frozen"})
        );
    }

    #[test]
    fn status_response_deserializes_from_wire() {
        let response: TokenizedAssetStatusResponse = serde_json::from_value(
            json!({"underlying": "SGOV", "status": "enabled"}),
        )
        .unwrap();

        assert_eq!(response.underlying, UnderlyingSymbol::new("SGOV"));
        assert_eq!(response.status, TokenizedAssetStatus::Enabled);
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
                underlying: UnderlyingSymbol::new("SGOV"),
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
            underlying: UnderlyingSymbol::new("SGOV"),
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

        assert_eq!(request.underlying, UnderlyingSymbol::new("SGOV"));
        assert_eq!(request.token, TokenSymbol::new("tSGOV"));
        assert_eq!(request.network, Network::Base);
        assert_eq!(request.vault, vault());
    }

    #[test]
    fn add_response_uses_snake_case_wire_format() {
        let response = AddTokenizedAssetResponse {
            underlying: UnderlyingSymbol::new("SGOV"),
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
}
