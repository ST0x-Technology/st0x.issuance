use apalis::prelude::Data;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

pub(crate) trait Job:
    Serialize + DeserializeOwned + Send + Sync + Unpin + Debug + 'static
{
    type Ctx: Clone + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn label(&self) -> Label;

    async fn run(self, ctx: Data<Self::Ctx>) -> Result<(), Self::Error>;
}

pub(crate) struct Label(String);

impl Label {
    pub(crate) fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }
}

impl Display for Label {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(formatter, "{}", self.0)
    }
}
