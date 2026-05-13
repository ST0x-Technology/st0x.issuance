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

    /// Records the intent to mint, transitioning from `JournalConfirmed` to
    /// `Minting` state. Pure state transition — no network call or vault lookup.
    ///
    /// Produces `MintingStarted`. The actual submission to the signing backend
    /// is handled by the subsequent `SubmitMint` command.
    Deposit {
        issuer_request_id: IssuerMintRequestId,
    },

    /// Submits the on-chain deposit (minting) transaction to the signing backend.
    ///
    /// Requires `Minting` state (set by prior `Deposit` command). Performs vault
    /// lookup, builds receipt info, and calls `submit_mint()`. Produces
    /// `FireblocksSubmitted` on success or `MintingFailed` on failure.
    SubmitMint {
        issuer_request_id: IssuerMintRequestId,
    },

    /// Confirms a previously submitted mint transaction.
    ///
    /// Polls the signing backend for the transaction identified by
    /// `fireblocks_tx_id`, then produces `TokensMinted` or `MintingFailed`.
    ConfirmMint {
        issuer_request_id: IssuerMintRequestId,
        fireblocks_tx_id: String,
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

    /// Admin-closes a mint that cannot be automatically recovered.
    ///
    /// Valid from any non-terminal state. Closed mints are excluded from
    /// recovery and stuck queries.
    CloseMint {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
    },

    /// Recovers a mint that failed during transaction submission but whose
    /// on-chain transaction actually succeeded, as evidenced by a receipt
    /// discovered by the receipt monitor.
    ///
    /// Only accepts `MintingFailed` state (with `Minting` predecessor).
    /// When recovery succeeds, atomically sends the Alpaca callback in the
    /// same command execution to avoid racing with the normal flow's
    /// `SendCallback` command.
    RecoverFromReceipt {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    },
}
