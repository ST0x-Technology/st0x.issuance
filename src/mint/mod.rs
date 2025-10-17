mod api;

use alloy::primitives::Address;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub(crate) use api::initiate_mint;

pub(crate) use crate::account::ClientId;
pub(crate) use crate::tokenized_asset::{
    Network, TokenSymbol, UnderlyingSymbol,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl TokenizationRequestId {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IssuerRequestId(pub(crate) String);

impl IssuerRequestId {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Quantity(pub(crate) Decimal);

impl Quantity {
    pub(crate) fn new(value: Decimal) -> Self {
        Self(value)
    }
}
