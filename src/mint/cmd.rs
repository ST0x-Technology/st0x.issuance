use alloy::primitives::{Address, TxHash};
use serde::{Deserialize, Serialize};

use super::{
    ClientId, IssuerMintRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MintCommand {
    Initiate {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
    },
    ConfirmJournal {
        issuer_request_id: IssuerMintRequestId,
    },
    RejectJournal {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
    },

    /// Executes the on-chain deposit (minting) operation.
    ///
    /// Calls the vault service to deposit, producing `MintingStarted`
    /// followed by either `TokensMinted` or `MintingFailed`.
    Deposit {
        issuer_request_id: IssuerMintRequestId,
    },

    /// Sends the callback to Alpaca to confirm mint completion.
    ///
    /// Calls the Alpaca service, producing `MintCompleted` on success.
    SendCallback {
        issuer_request_id: IssuerMintRequestId,
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
        issuer_request_id: IssuerMintRequestId,
    },

    /// Recovers a mint that failed during transaction submission but whose
    /// on-chain transaction actually succeeded, as evidenced by a receipt
    /// discovered by the receipt monitor.
    ///
    /// Only accepts `MintingFailed` and `CallbackPending` states. Unlike
    /// `Recover`, this rejects `JournalConfirmed` and `Minting` because
    /// receipt discovery only justifies recovery when the mint was marked
    /// as failed after submission.
    RecoverFromReceipt {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    },
}
