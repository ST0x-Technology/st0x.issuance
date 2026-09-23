use alloy::primitives::{Address, B256};
#[cfg(test)]
use chrono::{DateTime, Utc};
use st0x_finance::{DecimalShares, DecimalSharesConversionError};

use crate::Quantity;
use crate::account::ClientId;
use crate::mint::TokenizationRequestId as IssuanceTokenizationRequestId;
use crate::redemption::{
    IssuerRedemptionRequestId, IssuerRedemptionRequestIdParseError,
};
use crate::tokenized_asset::{
    Network as IssuanceNetwork, TokenSymbol as IssuanceTokenSymbol,
    UnderlyingSymbol as IssuanceUnderlyingSymbol,
};

pub(crate) mod service;

pub use service::AlpacaConfig;
pub(crate) use st0x_alpaca::AlpacaError;
pub(crate) use st0x_alpaca::core::TokenizationRequestId;
#[cfg(test)]
pub(crate) use st0x_alpaca::issuer::{Fees, TokenizationRequestType};
pub(crate) use st0x_alpaca::issuer::{
    IssuerApi as AlpacaService, MintCallbackRequest, RedeemRequest,
    RedeemRequestStatus, RedeemResponse, TokenizationRequest,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaBoundaryError {
    #[error("Invalid Alpaca issuer request id {value:?}: {source}")]
    IssuerRequestId {
        value: String,
        #[source]
        source: IssuerRedemptionRequestIdParseError,
    },
    #[error(
        "Alpaca redeem response issuer request id {returned} does not match {requested}"
    )]
    IssuerRequestIdMismatch {
        requested: IssuerRedemptionRequestId,
        returned: IssuerRedemptionRequestId,
    },
    #[error("Invalid Alpaca symbol: {0}")]
    Symbol(#[from] st0x_finance::EmptySymbolError),
    #[error("Invalid Alpaca quantity: {0}")]
    Quantity(#[from] st0x_finance::FloatError),
    #[error(
        "Alpaca quantity {value} is not a valid issuance quantity: {source}"
    )]
    IssuanceQuantity {
        value: String,
        #[source]
        source: DecimalSharesConversionError,
    },
    #[error("Invalid Alpaca underlying symbol {value:?}: {source}")]
    IssuanceUnderlying {
        value: String,
        #[source]
        source: st0x_issuance_dto::UnderlyingSymbolError,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct RedeemRequestInput<'a> {
    pub(crate) issuer_request_id: &'a IssuerRedemptionRequestId,
    pub(crate) underlying: &'a IssuanceUnderlyingSymbol,
    pub(crate) token: &'a IssuanceTokenSymbol,
    pub(crate) client_id: ClientId,
    pub(crate) quantity: &'a Quantity,
    pub(crate) network: IssuanceNetwork,
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
}

pub(crate) fn mint_callback_request(
    tokenization_request_id: &IssuanceTokenizationRequestId,
    client_id: ClientId,
    wallet_address: Address,
    tx_hash: B256,
    network: IssuanceNetwork,
) -> MintCallbackRequest {
    MintCallbackRequest {
        tokenization_request_id: alpaca_tokenization_request_id(
            tokenization_request_id,
        ),
        client_id: st0x_alpaca::issuer::ClientId(client_id.into()),
        wallet_address,
        tx_hash,
        network: alpaca_network(network),
    }
}

pub(crate) fn redeem_request(
    input: RedeemRequestInput<'_>,
) -> Result<RedeemRequest, AlpacaBoundaryError> {
    Ok(RedeemRequest {
        issuer_request_id: st0x_alpaca::issuer::IssuerRequestId(
            input.issuer_request_id.to_string(),
        ),
        underlying: alpaca_underlying_symbol(input.underlying)?,
        token: alpaca_token_symbol(input.token)?,
        client_id: st0x_alpaca::issuer::ClientId(input.client_id.into()),
        quantity: alpaca_redeem_quantity(input.quantity)?,
        network: alpaca_network(input.network),
        wallet: input.wallet,
        tx_hash: input.tx_hash,
    })
}

pub(crate) fn alpaca_tokenization_request_id(
    value: &IssuanceTokenizationRequestId,
) -> TokenizationRequestId {
    TokenizationRequestId::new(value.0.clone())
}

pub(crate) fn issuance_tokenization_request_id(
    value: TokenizationRequestId,
) -> IssuanceTokenizationRequestId {
    IssuanceTokenizationRequestId(value.0)
}

pub(crate) fn issuance_issuer_request_id(
    value: &st0x_alpaca::issuer::IssuerRequestId,
) -> Result<IssuerRedemptionRequestId, AlpacaBoundaryError> {
    let st0x_alpaca::issuer::IssuerRequestId(wire) = value;
    wire.parse().map_err(|source| AlpacaBoundaryError::IssuerRequestId {
        value: wire.clone(),
        source,
    })
}

pub(crate) fn issuance_underlying_symbol(
    value: &st0x_alpaca::issuer::UnderlyingSymbol,
) -> Result<IssuanceUnderlyingSymbol, AlpacaBoundaryError> {
    let wire = value.0.as_str();
    IssuanceUnderlyingSymbol::new(wire).map_err(|source| {
        AlpacaBoundaryError::IssuanceUnderlying {
            value: wire.to_string(),
            source,
        }
    })
}

pub(crate) fn issuance_token_symbol(
    value: &st0x_alpaca::issuer::TokenSymbol,
) -> IssuanceTokenSymbol {
    IssuanceTokenSymbol::new(value.0.as_str())
}

pub(crate) fn issuance_quantity(
    value: &st0x_alpaca::issuer::Qty,
) -> Result<Quantity, AlpacaBoundaryError> {
    let st0x_alpaca::issuer::Qty(shares) = value;
    let decimal =
        DecimalShares::from_fractional_shares(*shares).map_err(|source| {
            AlpacaBoundaryError::IssuanceQuantity {
                value: shares.to_string(),
                source,
            }
        })?;
    Ok(Quantity::new(decimal.inner()))
}

/// Issuance-domain values of the identifying fields in a redeem request that
/// Alpaca returned.
pub(crate) struct IssuanceRedeemFields {
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) underlying: IssuanceUnderlyingSymbol,
    pub(crate) token: IssuanceTokenSymbol,
    pub(crate) quantity: Quantity,
    pub(crate) network: IssuanceNetwork,
}

/// Converts the identifying fields of an Alpaca redeem request to issuance
/// types, failing on the first field issuance rejects.
pub(crate) fn issuance_redeem_fields(
    issuer_request_id: &st0x_alpaca::issuer::IssuerRequestId,
    underlying: &st0x_alpaca::issuer::UnderlyingSymbol,
    token: &st0x_alpaca::issuer::TokenSymbol,
    quantity: &st0x_alpaca::issuer::Qty,
    network: st0x_alpaca::core::Network,
) -> Result<IssuanceRedeemFields, AlpacaBoundaryError> {
    Ok(IssuanceRedeemFields {
        issuer_request_id: issuance_issuer_request_id(issuer_request_id)?,
        underlying: issuance_underlying_symbol(underlying)?,
        token: issuance_token_symbol(token),
        quantity: issuance_quantity(quantity)?,
        network: issuance_network(network),
    })
}

pub(crate) const fn issuance_network(
    value: st0x_alpaca::core::Network,
) -> IssuanceNetwork {
    match value {
        st0x_alpaca::core::Network::Base => IssuanceNetwork::Base,
        st0x_alpaca::core::Network::Ethereum => IssuanceNetwork::Ethereum,
        st0x_alpaca::core::Network::HyperEvm => IssuanceNetwork::HyperEvm,
        st0x_alpaca::core::Network::Robinhood => IssuanceNetwork::Robinhood,
        st0x_alpaca::core::Network::BnbSmartChain => {
            IssuanceNetwork::BnbSmartChain
        }
    }
}

fn alpaca_underlying_symbol(
    value: &IssuanceUnderlyingSymbol,
) -> Result<st0x_alpaca::issuer::UnderlyingSymbol, AlpacaBoundaryError> {
    Ok(st0x_alpaca::issuer::UnderlyingSymbol::new(value.as_str())?)
}

fn alpaca_token_symbol(
    value: &IssuanceTokenSymbol,
) -> Result<st0x_alpaca::issuer::TokenSymbol, AlpacaBoundaryError> {
    // Validate without using Symbol's normalized value: the request must keep
    // the issuer's original token spelling (including surrounding spaces).
    st0x_finance::Symbol::new(value.0.clone())?;
    Ok(st0x_alpaca::issuer::TokenSymbol::new(value.0.clone()))
}

#[cfg(test)]
fn alpaca_quantity(
    value: &Quantity,
) -> Result<st0x_alpaca::issuer::Qty, AlpacaBoundaryError> {
    Ok(st0x_alpaca::issuer::Qty(value.to_string().parse()?))
}

fn alpaca_redeem_quantity(
    value: &Quantity,
) -> Result<st0x_alpaca::issuer::RedeemQty, AlpacaBoundaryError> {
    Ok(st0x_alpaca::issuer::RedeemQty::new(value.to_string())?)
}

const fn alpaca_network(value: IssuanceNetwork) -> st0x_alpaca::core::Network {
    match value {
        IssuanceNetwork::Base => st0x_alpaca::core::Network::Base,
        IssuanceNetwork::Ethereum => st0x_alpaca::core::Network::Ethereum,
        IssuanceNetwork::HyperEvm => st0x_alpaca::core::Network::HyperEvm,
        IssuanceNetwork::Robinhood => st0x_alpaca::core::Network::Robinhood,
        IssuanceNetwork::BnbSmartChain => {
            st0x_alpaca::core::Network::BnbSmartChain
        }
    }
}

#[cfg(test)]
pub(crate) struct TestRedeemResponse {
    pub(crate) tokenization_request_id: IssuanceTokenizationRequestId,
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) status: RedeemRequestStatus,
    pub(crate) underlying: IssuanceUnderlyingSymbol,
    pub(crate) token: IssuanceTokenSymbol,
    pub(crate) quantity: Quantity,
    pub(crate) network: IssuanceNetwork,
    pub(crate) wallet: Address,
    pub(crate) tx_hash: Option<B256>,
    pub(crate) updated_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
pub(crate) fn test_redeem_response(
    value: TestRedeemResponse,
) -> Result<TokenizationRequest, AlpacaBoundaryError> {
    Ok(TokenizationRequest::Redeem {
        id: alpaca_tokenization_request_id(&value.tokenization_request_id),
        issuer_request_id: st0x_alpaca::issuer::IssuerRequestId(
            value.issuer_request_id.to_string(),
        ),
        status: value.status,
        underlying: alpaca_underlying_symbol(&value.underlying)?,
        token: alpaca_token_symbol(&value.token)?,
        quantity: alpaca_quantity(&value.quantity)?,
        network: alpaca_network(value.network),
        wallet: value.wallet,
        tx_hash: value.tx_hash,
        updated_at: value.updated_at,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256};
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use st0x_alpaca::issuer::itn::{
        REDEEM_CALLBACK_OPENAPI_REFERENCE, accepts_network_wire_string,
    };
    use tracing_test::traced_test;

    use super::{
        AlpacaBoundaryError, RedeemRequestInput, TokenizationRequestId,
        issuance_issuer_request_id, issuance_quantity, issuance_token_symbol,
        issuance_tokenization_request_id, issuance_underlying_symbol,
        redeem_request,
    };
    use crate::Quantity;
    use crate::account::ClientId;
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    #[test]
    fn issued_network_wire_strings_are_alpaca_itn_values() {
        for network in [
            Network::Base,
            Network::Ethereum,
            Network::HyperEvm,
            Network::Robinhood,
            Network::BnbSmartChain,
        ] {
            let wire = network.as_str();
            assert!(
                accepts_network_wire_string(wire),
                "issued network {wire} must be on the Alpaca ITN \
                 TokenizationNetwork list ({REDEEM_CALLBACK_OPENAPI_REFERENCE})"
            );
        }
    }

    #[test]
    fn redeem_request_preserves_issuance_boundary_values_for_every_network() {
        for (network, expected) in [
            (Network::Base, st0x_alpaca::core::Network::Base),
            (Network::Ethereum, st0x_alpaca::core::Network::Ethereum),
            (Network::HyperEvm, st0x_alpaca::core::Network::HyperEvm),
            (Network::Robinhood, st0x_alpaca::core::Network::Robinhood),
            (Network::BnbSmartChain, st0x_alpaca::core::Network::BnbSmartChain),
        ] {
            let tx_hash = B256::repeat_byte(0x11);
            let issuer_request_id = IssuerRedemptionRequestId::new(tx_hash);
            let client_id = "00000000-0000-4000-8000-000000000001"
                .parse::<ClientId>()
                .unwrap();
            let underlying = UnderlyingSymbol::new("SPYM").unwrap();
            let token = TokenSymbol::new("tSPYM");
            let quantity = Quantity::new(dec!(1.234567891));
            let wallet = Address::repeat_byte(0x22);

            let request = redeem_request(RedeemRequestInput {
                issuer_request_id: &issuer_request_id,
                underlying: &underlying,
                token: &token,
                client_id,
                quantity: &quantity,
                network,
                wallet,
                tx_hash,
            })
            .unwrap();

            assert_eq!(
                request.issuer_request_id.0,
                issuer_request_id.to_string()
            );
            assert_eq!(request.underlying.0.as_str(), "SPYM");
            assert_eq!(request.token.0.as_str(), "tSPYM");
            assert_eq!(request.client_id.0.to_string(), client_id.to_string());
            assert_eq!(request.quantity.as_str(), "1.234567891");
            assert_eq!(request.network, expected);
            assert_eq!(request.wallet, wallet);
            assert_eq!(request.tx_hash, tx_hash);
        }
    }

    #[test]
    fn response_values_convert_back_to_issuance_domain_types() {
        let tokenization_request_id = issuance_tokenization_request_id(
            TokenizationRequestId::new("tok-redeem-1"),
        );
        let issuer_request_id =
            IssuerRedemptionRequestId::new(B256::repeat_byte(0x55));
        let shared_issuer_request_id =
            st0x_alpaca::issuer::IssuerRequestId(issuer_request_id.to_string());
        let shared_underlying =
            st0x_alpaca::issuer::UnderlyingSymbol::new("SPYM").unwrap();
        let shared_token = st0x_alpaca::issuer::TokenSymbol::new("tSPYM");
        let shared_quantity = st0x_alpaca::issuer::Qty(
            "1.234567891".parse::<st0x_finance::FractionalShares>().unwrap(),
        );

        assert_eq!(tokenization_request_id.0, "tok-redeem-1");
        assert_eq!(
            issuance_issuer_request_id(&shared_issuer_request_id).unwrap(),
            issuer_request_id
        );
        assert_eq!(
            issuance_underlying_symbol(&shared_underlying).unwrap(),
            UnderlyingSymbol::new("SPYM").unwrap()
        );
        assert_eq!(
            issuance_token_symbol(&shared_token),
            TokenSymbol::new("tSPYM")
        );
        assert_eq!(
            issuance_quantity(&shared_quantity).unwrap(),
            Quantity::new(dec!(1.234567891))
        );
    }

    #[test]
    fn response_issuer_request_id_rejects_an_unknown_wire_format() {
        let result = issuance_issuer_request_id(
            &st0x_alpaca::issuer::IssuerRequestId("not-an-id".to_string()),
        );

        let Err(AlpacaBoundaryError::IssuerRequestId { value, .. }) = result
        else {
            panic!("Expected IssuerRequestId error, got {result:?}");
        };
        assert_eq!(value, "not-an-id");
    }

    #[test]
    fn shared_underlying_rejects_empty_wire_before_domain_conversion() {
        let invalid = serde_json::from_str::<
            st0x_alpaca::issuer::UnderlyingSymbol,
        >("\"   \"");
        assert!(invalid.is_err());

        let shared =
            st0x_alpaca::issuer::UnderlyingSymbol::new(" AAPL ").unwrap();
        assert_eq!(shared.0.as_str(), "AAPL");
        assert_eq!(
            issuance_underlying_symbol(&shared).unwrap(),
            UnderlyingSymbol::new("AAPL").unwrap()
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn redeem_adapter_and_shared_client_preserve_exact_wire_body() {
        let tx_hash = B256::repeat_byte(0x11);
        let issuer_request_id = IssuerRedemptionRequestId::new(tx_hash);
        let underlying = UnderlyingSymbol::new("SPYM").unwrap();
        let token = TokenSymbol::new(" tSPYM ");
        let quantity = Quantity::new(dec!(100.50));
        let request = redeem_request(RedeemRequestInput {
            issuer_request_id: &issuer_request_id,
            underlying: &underlying,
            token: &token,
            client_id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
            quantity: &quantity,
            network: Network::Base,
            wallet: Address::repeat_byte(0x22),
            tx_hash,
        })
        .unwrap();
        let body = serde_json::to_value(&request).unwrap();
        assert_eq!(body["qty"], "100.50");
        assert_eq!(body["token_symbol"], " tSPYM ");

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account/tokenization/callback/redeem")
                .json_body(body);
            then.status(200).json_body(serde_json::json!({
                "tokenization_request_id": "tok-1",
                "issuer_request_id": issuer_request_id.to_string(),
                "created_at": "2026-09-23T00:00:00Z",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "SPYM",
                "token_symbol": " tSPYM ",
                "qty": "100.50",
                "issuer": "st0x",
                "network": "base",
                "wallet_address": Address::repeat_byte(0x22).to_string(),
                "tx_hash": tx_hash.to_string(),
                "fees": null
            }));
        });
        let mut config = super::AlpacaConfig::test_default();
        config.api_base_url = server.base_url();
        config.account_id = "test-account".into();
        config.service().unwrap().call_redeem_endpoint(request).await.unwrap();
        mock.assert();
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Alpaca redeem call finished", "success=true"]
        ));
    }
}
