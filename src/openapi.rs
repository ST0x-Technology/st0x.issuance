//! OpenAPI document for the internal and operator (admin) HTTP surface.
//!
//! The Alpaca ITN endpoints (`/accounts/connect`, `GET /tokenized-assets`,
//! `/inkind/issuance`, `/inkind/issuance/confirm`) are intentionally omitted:
//! their contract is governed by Alpaca's own documentation, not ours.

use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "st0x.issuance internal API",
        description = "Internal and operator (admin) endpoints. The Alpaca ITN \
endpoints are documented by Alpaca and excluded here.\n\nEvery endpoint here \
requires the `X-API-KEY` header AND a request from an allowlisted internal IP \
(`INTERNAL_IP_RANGES`): a missing or invalid key returns 401, and a valid key \
from a non-allowlisted address returns 403. The security scheme below documents \
the header; the IP allowlist is enforced at the network layer and cannot be \
expressed as an OpenAPI scheme."
    ),
    paths(
        crate::tokenized_asset::api::get_tokenized_asset,
        crate::tokenized_asset::api::get_tokenized_asset_status,
        crate::tokenized_asset::api::add_tokenized_asset,
        crate::account::api::register_account,
        crate::account::api::whitelist_wallet,
        crate::account::api::unwhitelist_wallet,
        crate::admin::recover_redemption,
        crate::admin::close_redemption,
        crate::admin::force_complete_redemption,
        crate::admin::reprocess_mint,
        crate::admin::close_mint,
        crate::admin::list_stuck,
        crate::admin::check_fireblocks_tx,
    ),
    components(schemas(
        st0x_issuance_dto::TokenizedAssetDetailResponse,
        st0x_issuance_dto::TokenizedAssetStatusResponse,
        st0x_issuance_dto::TokenizedAssetStatus,
        st0x_issuance_dto::AddTokenizedAssetRequest,
        st0x_issuance_dto::AddTokenizedAssetResponse,
        st0x_issuance_dto::UnderlyingSymbol,
        st0x_issuance_dto::TokenSymbol,
        st0x_issuance_dto::Network,
        crate::account::api::RegisterAccountRequest,
        crate::account::api::RegisterAccountResponse,
        crate::account::api::WhitelistWalletRequest,
        crate::account::api::WhitelistWalletResponse,
        crate::admin::AggregateKind,
        crate::admin::ReprocessResponse,
        crate::admin::StuckAggregate,
        crate::admin::StuckResponse,
        crate::admin::CloseRedemptionRequest,
        crate::admin::ForceCompleteRedemptionRequest,
        crate::admin::CloseMintRequest,
        crate::admin::FireblocksTxResponse,
        crate::vault::FireblocksTxStatus,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "tokenized-assets",
            description = "Internal tokenized-asset administration"),
        (name = "accounts", description = "Internal account management"),
        (name = "admin", description = "Operator admin and recovery endpoints")
    )
)]
pub(crate) struct ApiDoc;

/// Registers the `X-API-KEY` header auth scheme that the internal/admin
/// endpoints reference via `security(("internal_api_key" = []))`.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components =
            openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "internal_api_key",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new(
                "X-API-KEY",
            ))),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_internal_surface_and_excludes_alpaca_itn() {
        let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let paths = spec["paths"].as_object().expect("paths object");

        // Every internal/operator endpoint is documented. Listed exhaustively
        // (not a representative sample) so dropping a `#[utoipa::path]` from the
        // `ApiDoc` `paths(...)` set fails the test rather than silently shrinking
        // the published surface. `/tokenized-assets` (POST) is asserted below
        // alongside the GET-exclusion check.
        for path in [
            "/tokenized-assets/{underlying}",
            "/tokenized-assets/{underlying}/status",
            "/accounts",
            "/accounts/{client_id}/wallets",
            "/accounts/{client_id}/wallets/{wallet}",
            "/admin/stuck",
            "/admin/fireblocks/tx/{fireblocks_tx_id}",
            "/admin/recover/redemption/{issuer_request_id}",
            "/admin/close/redemption/{issuer_request_id}",
            "/admin/force-complete/redemption/{issuer_request_id}",
            "/admin/reprocess/mint/{aggregate_id}",
            "/admin/close/mint/{aggregate_id}",
        ] {
            assert!(
                paths.contains_key(path),
                "missing documented path: {path}"
            );
        }

        // The add-asset POST is documented, but the GET list is the Alpaca ITN
        // endpoint and must NOT be: same path, different method.
        assert!(paths["/tokenized-assets"]["post"].is_object());
        assert!(
            paths["/tokenized-assets"].get("get").is_none(),
            "GET /tokenized-assets is an Alpaca ITN endpoint and must be excluded"
        );

        // The remaining Alpaca ITN endpoints are absent entirely.
        for path in [
            "/accounts/connect",
            "/inkind/issuance",
            "/inkind/issuance/confirm",
        ] {
            assert!(
                !paths.contains_key(path),
                "Alpaca ITN endpoint must be excluded: {path}"
            );
        }

        // The X-API-KEY auth scheme the endpoints reference is registered, with
        // the exact semantics clients depend on. Asserting type/in/name (not just
        // presence) catches a drift away from the `X-API-KEY` header apiKey form.
        let scheme = &spec["components"]["securitySchemes"]["internal_api_key"];
        assert!(
            scheme.is_object(),
            "internal_api_key security scheme must be registered"
        );
        assert_eq!(scheme["type"], "apiKey");
        assert_eq!(scheme["in"], "header");
        assert_eq!(scheme["name"], "X-API-KEY");
    }

    // The DTO `ToSchema` derives and `schema(value_type = String)` overrides are
    // the OpenAPI half of the wire contract (the serde and ts_rs halves are
    // pinned in the dto crate's own tests). Without this, dropping a derive or a
    // `value_type` override silently drifts the published schema -- e.g. the
    // `UnderlyingSymbol(String)` newtype leaking as a wrapper object, or `vault`
    // exposing alloy's `Address` representation instead of the 0x-hex string the
    // dashboard parses. Assert the component shapes, not just their presence.
    #[test]
    fn dto_component_schemas_match_the_wire_contract() {
        let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let schemas =
            spec["components"]["schemas"].as_object().expect("schemas object");

        // The symbol newtypes are transparent strings on the wire: utoipa must
        // unwrap `UnderlyingSymbol(String)`/`TokenSymbol(String)` to a bare
        // string schema, never a `{ "0": string }` wrapper object.
        assert_eq!(schemas["UnderlyingSymbol"]["type"], "string");
        assert_eq!(schemas["TokenSymbol"]["type"], "string");

        // `vault` carries `schema(value_type = String)` to override alloy's
        // `Address`; the override must hold in both the request and the detail
        // response so the schema advertises the 0x-hex string clients parse.
        assert_eq!(
            schemas["AddTokenizedAssetRequest"]["properties"]["vault"]["type"],
            "string"
        );
        assert_eq!(
            schemas["TokenizedAssetDetailResponse"]["properties"]["vault"]["type"],
            "string"
        );

        // `Network` and `TokenizedAssetStatus` are closed enums; their schema
        // must be a string enum over the exact lowercase wire values clients
        // switch on, not an open string or a struct.
        assert_eq!(schemas["Network"]["type"], "string");
        assert_eq!(schemas["Network"]["enum"], serde_json::json!(["base"]));
        assert_eq!(schemas["TokenizedAssetStatus"]["type"], "string");
        assert_eq!(
            schemas["TokenizedAssetStatus"]["enum"],
            serde_json::json!(["enabled", "frozen"])
        );
    }
}
