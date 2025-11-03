use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use super::{AlpacaAccountNumber, Email};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum AccountCommand {
    Link { email: Email, alpaca_account: AlpacaAccountNumber, wallet: Address },
}
