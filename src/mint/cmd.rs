use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use super::{
    ClientId, IssuerRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MintCommand {
    Initiate {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
    },
    ConfirmJournal {
        issuer_request_id: IssuerRequestId,
    },
    RejectJournal {
        issuer_request_id: IssuerRequestId,
        reason: String,
    },

    /// Executes the on-chain deposit (minting) operation.
    ///
    /// Calls the vault service to deposit, producing `MintingStarted`
    /// followed by either `TokensMinted` or `MintingFailed`.
    Deposit {
        issuer_request_id: IssuerRequestId,
    },

    /// Sends the callback to Alpaca to confirm mint completion.
    ///
    /// Calls the Alpaca service, producing `MintCompleted` on success.
    SendCallback {
        issuer_request_id: IssuerRequestId,
    },

    /// Recovers a mint stuck in an incomplete state.
    ///
    /// For mints in `Minting` or `MintingFailed` state:
    /// - Checks receipt inventory for existing receipt
    /// - If found: records the existing mint (produces `ExistingMintRecovered`)
    /// - If not found: retries the mint (produces `MintRetryStarted` then executes mint)
    ///
    /// For mints in `CallbackPending` state:
    /// - Retries sending the callback
    Recover {
        issuer_request_id: IssuerRequestId,
    },
        issuer_request_id: IssuerRequestId,
    },
}
