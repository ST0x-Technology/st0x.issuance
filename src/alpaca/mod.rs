use alloy::primitives::{Address, B256};
#[cfg(test)]
use chrono::{DateTime, Utc};

use crate::Quantity;
use crate::account::ClientId;
use crate::mint::TokenizationRequestId as IssuanceTokenizationRequestId;
use crate::redemption::IssuerRedemptionRequestId;
use crate::tokenized_asset::{
    Network as IssuanceNetwork, TokenSymbol as IssuanceTokenSymbol,
    UnderlyingSymbol as IssuanceUnderlyingSymbol,
};

pub(crate) mod mock;
pub(crate) mod service;

pub use service::AlpacaConfig;
pub(crate) use st0x_alpaca::AlpacaError;
pub(crate) use st0x_alpaca::core::TokenizationRequestId;
#[cfg(test)]
pub(crate) use st0x_alpaca::issuer::RedeemResponse;
pub(crate) use st0x_alpaca::issuer::{
    IssuerApi as AlpacaService, MintCallbackRequest, RedeemRequest,
    RedeemRequestStatus, TokenizationRequest,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaBoundaryError {
    #[error("Invalid Alpaca issuer request id: {0}")]
    IssuerRequestId(String),
    #[error("Invalid Alpaca symbol: {0}")]
    Symbol(String),
    #[error("Invalid Alpaca quantity: {0}")]
    Quantity(String),
}

#[derive(Clone, Copy)]
pub(crate) struct RedeemRequestInput<'a> {
    pub(crate) issuer_request_id: &'a IssuerRedemptionRequestId,
    pub(crate) underlying: &'a IssuanceUnderlyingSymbol,
    pub(crate) token: &'a IssuanceTokenSymbol,
    pub(crate) client_id: ClientId,
    pub(crate) quantity: &'a Quantity,
    pub(crate) network: &'a IssuanceNetwork,
    pub(crate) wallet: Address,
    pub(crate) tx_hash: B256,
}

pub(crate) fn mint_callback_request(
    tokenization_request_id: &IssuanceTokenizationRequestId,
    client_id: ClientId,
    wallet_address: Address,
    tx_hash: B256,
    network: &IssuanceNetwork,
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
        quantity: alpaca_quantity(input.quantity)?,
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
    value.0.parse().map_err(
        |error: crate::redemption::IssuerRedemptionRequestIdParseError| {
            AlpacaBoundaryError::IssuerRequestId(error.to_string())
        },
    )
}

pub(crate) fn issuance_underlying_symbol(
    value: &st0x_alpaca::issuer::UnderlyingSymbol,
) -> IssuanceUnderlyingSymbol {
    IssuanceUnderlyingSymbol::new(value.0.as_str())
}

pub(crate) fn issuance_token_symbol(
    value: &st0x_alpaca::issuer::TokenSymbol,
) -> IssuanceTokenSymbol {
    IssuanceTokenSymbol::new(value.0.as_str())
}

pub(crate) fn issuance_quantity(
    value: &st0x_alpaca::issuer::Qty,
) -> Result<Quantity, AlpacaBoundaryError> {
    value
        .0
        .to_string()
        .parse::<rust_decimal::Decimal>()
        .map(Quantity::new)
        .map_err(|error| AlpacaBoundaryError::Quantity(error.to_string()))
}

fn alpaca_underlying_symbol(
    value: &IssuanceUnderlyingSymbol,
) -> Result<st0x_alpaca::issuer::UnderlyingSymbol, AlpacaBoundaryError> {
    st0x_alpaca::issuer::UnderlyingSymbol::new(value.0.clone())
        .map_err(|error| AlpacaBoundaryError::Symbol(error.to_string()))
}

fn alpaca_token_symbol(
    value: &IssuanceTokenSymbol,
) -> Result<st0x_alpaca::issuer::TokenSymbol, AlpacaBoundaryError> {
    st0x_alpaca::issuer::TokenSymbol::new(value.0.clone())
        .map_err(|error| AlpacaBoundaryError::Symbol(error.to_string()))
}

fn alpaca_quantity(
    value: &Quantity,
) -> Result<st0x_alpaca::issuer::Qty, AlpacaBoundaryError> {
    value
        .to_string()
        .parse::<st0x_finance::FractionalShares>()
        .map(st0x_alpaca::issuer::Qty)
        .map_err(|error| AlpacaBoundaryError::Quantity(error.to_string()))
}

const fn alpaca_network(value: &IssuanceNetwork) -> st0x_alpaca::core::Network {
    match value {
        IssuanceNetwork::Base => st0x_alpaca::core::Network::Base,
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
        wallet: value.wallet,
        tx_hash: value.tx_hash,
        updated_at: value.updated_at,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256};
    use rust_decimal_macros::dec;

    use super::{
        AlpacaBoundaryError, RedeemRequestInput, TokenizationRequestId,
        issuance_issuer_request_id, issuance_quantity, issuance_token_symbol,
        issuance_tokenization_request_id, issuance_underlying_symbol,
        mint_callback_request, redeem_request,
    };
    use crate::Quantity;
    use crate::account::ClientId;
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};

    #[test]
    fn redeem_request_preserves_the_truncated_issuance_boundary_values() {
        let tx_hash = B256::repeat_byte(0x11);
        let issuer_request_id = IssuerRedemptionRequestId::new(tx_hash);
        let client_id =
            "00000000-0000-4000-8000-000000000001".parse::<ClientId>().unwrap();
        let wallet = Address::repeat_byte(0x22);
        let quantity = Quantity::new(dec!(1.234567891));

        let underlying = UnderlyingSymbol::new("SPYM");
        let token = TokenSymbol::new("tSPYM");
        let network = Network::Base;

        let request = redeem_request(RedeemRequestInput {
            issuer_request_id: &issuer_request_id,
            underlying: &underlying,
            token: &token,
            client_id,
            quantity: &quantity,
            network: &network,
            wallet,
            tx_hash,
        })
        .unwrap();

        assert_eq!(request.issuer_request_id.0, issuer_request_id.to_string());
        assert_eq!(request.underlying.0.as_str(), "SPYM");
        assert_eq!(request.token.0.as_str(), "tSPYM");
        assert_eq!(request.client_id.0.to_string(), client_id.to_string());
        assert_eq!(request.quantity.0.to_string(), "1.234567891");
        assert_eq!(request.network, st0x_alpaca::core::Network::Base);
        assert_eq!(request.wallet, wallet);
        assert_eq!(request.tx_hash, tx_hash);
    }

    #[test]
    fn mint_callback_request_preserves_the_issuance_boundary_values() {
        let tokenization_request_id =
            crate::mint::TokenizationRequestId::new("tok-mint-1");
        let client_id =
            "00000000-0000-4000-8000-000000000002".parse::<ClientId>().unwrap();
        let wallet = Address::repeat_byte(0x33);
        let tx_hash = B256::repeat_byte(0x44);

        let request = mint_callback_request(
            &tokenization_request_id,
            client_id,
            wallet,
            tx_hash,
            &Network::Base,
        );

        assert_eq!(request.tokenization_request_id.0, "tok-mint-1");
        assert_eq!(request.client_id.0.to_string(), client_id.to_string());
        assert_eq!(request.wallet_address, wallet);
        assert_eq!(request.tx_hash, tx_hash);
        assert_eq!(request.network, st0x_alpaca::core::Network::Base);
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
        let shared_token =
            st0x_alpaca::issuer::TokenSymbol::new("tSPYM").unwrap();
        let shared_quantity = st0x_alpaca::issuer::Qty(
            "1.234567891".parse::<st0x_finance::FractionalShares>().unwrap(),
        );

        assert_eq!(tokenization_request_id.0, "tok-redeem-1");
        assert_eq!(
            issuance_issuer_request_id(&shared_issuer_request_id).unwrap(),
            issuer_request_id
        );
        assert_eq!(
            issuance_underlying_symbol(&shared_underlying),
            UnderlyingSymbol::new("SPYM")
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

        assert!(matches!(result, Err(AlpacaBoundaryError::IssuerRequestId(_))));
    }

    #[test]
    fn redeem_request_rejects_a_blank_symbol_at_the_shared_boundary() {
        let issuer_request_id =
            IssuerRedemptionRequestId::new(B256::repeat_byte(0x66));
        let underlying = UnderlyingSymbol::new(" ");
        let token = TokenSymbol::new("tSPYM");
        let quantity = Quantity::new(dec!(1));
        let network = Network::Base;

        let result = redeem_request(RedeemRequestInput {
            issuer_request_id: &issuer_request_id,
            underlying: &underlying,
            token: &token,
            client_id: "00000000-0000-4000-8000-000000000003"
                .parse::<ClientId>()
                .unwrap(),
            quantity: &quantity,
            network: &network,
            wallet: Address::repeat_byte(0x77),
            tx_hash: B256::repeat_byte(0x88),
        });

        assert!(matches!(result, Err(AlpacaBoundaryError::Symbol(_))));
    }
}
