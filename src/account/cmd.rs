use serde::{Deserialize, Serialize};

use super::{AlpacaAccountNumber, Email};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum AccountCommand {
    LinkAccount { email: Email, alpaca_account: AlpacaAccountNumber },
}
