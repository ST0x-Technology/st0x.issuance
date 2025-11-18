use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use super::{AlpacaAccountNumber, ClientId, Email};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum AccountCommand {
    Link {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
    },
    WhitelistWallet {
        wallet: Address,
    },
}
