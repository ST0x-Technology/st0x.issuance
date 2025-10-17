mod endpoint;

use serde::{Deserialize, Serialize};

pub(crate) use endpoint::list_tokenized_assets;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UnderlyingSymbol(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenSymbol(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Network(pub(crate) String);
