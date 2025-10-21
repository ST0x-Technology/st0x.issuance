mod api;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub(crate) use api::initiate_mint;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IssuerRequestId(pub(crate) String);

impl IssuerRequestId {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Quantity(pub(crate) Decimal);
