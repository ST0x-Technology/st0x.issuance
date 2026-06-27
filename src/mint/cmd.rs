use alloy::primitives::{Address, B256, U256};
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
    /// is performed by the durable `SubmitMintJob`.
    Deposit {
        issuer_request_id: IssuerMintRequestId,
    },

    /// Records the outcome of a successful on-chain mint submission performed
    /// by a durable `SubmitMintJob`. Pure: produces `FireblocksSubmitted` from
    /// the payload, no I/O. Idempotent — a no-op if the mint already advanced
    /// past `Minting` (an at-least-once job re-run is safe).
    RecordFireblocksSubmitted {
        issuer_request_id: IssuerMintRequestId,
        external_tx_id: String,
        fireblocks_tx_id: String,
    },

    /// Records the outcome of a successful on-chain mint confirmation performed
    /// by a durable `ConfirmMintJob`. Pure: produces `TokensMinted` from the
    /// payload, no I/O. Idempotent — a no-op if the mint already advanced past
    /// `FireblocksSubmitted`.
    RecordTokensMinted {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
    },

    /// Records the outcome of a successful Alpaca callback performed by a
    /// durable `SendCallbackJob`. Pure: produces `MintCompleted`, no I/O.
    /// Idempotent — a no-op if the mint is already `Completed`.
    RecordCallbackSent {
        issuer_request_id: IssuerMintRequestId,
    },

    /// Records a mint side-effect failure reported by a durable job (submission
    /// or confirmation). Pure: produces `MintingFailed` from the payload, no
    /// I/O. Idempotent — a no-op if the mint already left `Minting` /
    /// `FireblocksSubmitted`.
    RecordMintFailed {
        issuer_request_id: IssuerMintRequestId,
        error: String,
    },

    /// Retries a failed mint by transitioning `MintingFailed` -> `Minting`,
    /// advancing the automatic-retry attempt counter. Pure: produces
    /// `MintRetryStarted` (no I/O). Recovery sends this before re-enqueuing a
    /// `SubmitMintJob`. Idempotent — a no-op if the mint already left
    /// `MintingFailed`.
    RetryMint {
        issuer_request_id: IssuerMintRequestId,
    },

    /// Records a mint whose on-chain transaction already succeeded, as evidenced
    /// by a receipt the `SubmitMintJob` found before submitting. Pure: produces
    /// `ExistingMintRecovered` (no I/O) and advances to `CallbackPending`,
    /// avoiding a double-mint where re-submission would mint again. Idempotent —
    /// a no-op once the mint is already minted.
    RecordExistingMint {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
    },

    /// Admin-closes a mint that cannot be automatically recovered.
    ///
    /// Valid from any non-terminal state. Closed mints are excluded from
    /// recovery and stuck queries.
    CloseMint {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
    },
}
