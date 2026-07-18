//! Alpaca Instant Tokenization Network wire-format constants.
//!
//! Redeem (and mint) callback `network` values are defined by Alpaca's Broker
//! API OpenAPI schema `TokenizationNetwork`:
//! <https://docs.alpaca.markets/reference/posttokenizationredeem>
//!
//! The issuer guide lists the same values in prose:
//! <https://docs.alpaca.markets/us/docs/tokenization-guide-for-issuer>

/// OpenAPI reference for `POST .../tokenization/callback/redeem`.
pub(crate) const REDEEM_CALLBACK_OPENAPI_REFERENCE: &str =
    "https://docs.alpaca.markets/reference/posttokenizationredeem";

/// `TokenizationNetwork` enum values from Alpaca Broker API OpenAPI
/// (`components.schemas.TokenizationNetwork.enum`).
pub(crate) const TOKENIZATION_NETWORK_WIRE_STRINGS: &[&str] = &[
    "solana", "arbitrum", "ethereum", "binance", "base", "ton", "tron",
    "mantle",
];

/// Returns whether `wire` is a published Alpaca ITN `TokenizationNetwork` value.
#[must_use]
pub(crate) fn accepts_network_wire_string(wire: &str) -> bool {
    TOKENIZATION_NETWORK_WIRE_STRINGS.contains(&wire)
}

#[cfg(test)]
mod tests {
    use crate::tokenized_asset::Network;

    use super::{
        REDEEM_CALLBACK_OPENAPI_REFERENCE, accepts_network_wire_string,
    };

    #[test]
    fn issued_network_wire_strings_are_alpaca_itn_values() {
        for network in [Network::Base, Network::Ethereum] {
            let wire = network.as_str();
            assert!(
                accepts_network_wire_string(wire),
                "issued network {wire} must be listed in Alpaca TokenizationNetwork \
                 OpenAPI ({REDEEM_CALLBACK_OPENAPI_REFERENCE})"
            );
        }
    }
}
