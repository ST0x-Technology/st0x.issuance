use alloy::primitives::{Address, B256, TxHash, U256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::VaultMode;
use crate::vault::{MintAuthorization, PreparedMintTx, TxId};

use super::{
    ClientId, IssuerMintRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

/// Typed classification of an on-chain mint failure, persisted on
/// `MintingFailed`. Mirrors the burn side's `BurnFailureClassification`:
/// typed classifications are never auto-retried, and the halted-orchestrator
/// cases are surfaced distinctly from ordinary mint failures.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
pub(crate) enum MintFailureClassification {
    /// No typed cause was decoded — retryable on the automatic schedule.
    #[default]
    Unclassified,
    /// `vaultLogicIsExpected()` mismatch — the orchestrator is halted
    /// pending upgrade; resolves environment-wide, never per-mint.
    VaultLogicMismatch,
    /// The orchestrator's receipt-logic version lock tripped — halted like
    /// `VaultLogicMismatch`.
    ReceiptLogicMismatch,
    /// Assigned by recovery's full-match check (not decoded from a revert):
    /// a `Minted` log at the `(to, nonce)` pair WAS found and its token or
    /// amount disagrees with this mint's — affirmative proof a different
    /// mint consumed the pair, so this mint can never land. Manual
    /// reconciliation only.
    NonceConsumedByOtherMint,
    /// Assigned by recovery's full-match check (not decoded from a revert):
    /// `nonceUsed(to, nonce)` reports the nonce consumed but NO `Minted`
    /// log at the pair was found at all. The two cannot both be true of a
    /// healthy chain view, so the lookup itself is untrusted (window too
    /// narrow, RPC error, indexer lag) — an unknown outcome, never proof:
    /// this mint may well have landed. Non-retryable for SUBMISSION (the
    /// nonce is consumed either way) but retryable for RECONCILIATION:
    /// recovery re-runs the log query over a widened window, and a later
    /// full match resolves the mint forward (SPEC "Recipient Authorization"
    /// -> "Nonce").
    NonceReplayUnresolved,
    /// `BadRecipientSignature()` revert — the recipient signature did not
    /// recover to `to`. Deterministic for the stored authorization: only
    /// `CloseMint` plus a fresh `Initiate` with a new authorization resolves
    /// it (see SPEC "Failure States").
    BadRecipientSignature,
    /// `RecipientCallbackRejected(recipient)` revert — an `IMintRecipient`
    /// contract refused the mint via its `authorizeMint` callback.
    /// Deterministic like `BadRecipientSignature`.
    RecipientCallbackRejected,
    /// `VaultAmountMismatch(expected, actual)` revert — the orchestrator's
    /// on-chain 1:1 assertion failed (e.g. a share-ratio rebase mid-flight).
    /// Alert-and-investigate; the same aggregate resumes via admin reprocess
    /// once the ratio is restored (see SPEC "Failure States").
    VaultAmountMismatch,
}

impl MintFailureClassification {
    /// Whether this failure is an orchestrator-wide halt
    /// (`vaultLogicIsExpected()` / receipt-logic version lock). Per SPEC
    /// ("Failure States"), a halt resolves environment-wide by upgrade,
    /// never per-mint, so it must not advance the mint's automatic-retry
    /// attempt counter — otherwise every in-flight mint would burn its whole
    /// budget waiting out one halt.
    pub(crate) const fn is_environment_halt(self) -> bool {
        matches!(self, Self::VaultLogicMismatch | Self::ReceiptLogicMismatch)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintEvent {
    Initiated {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        /// The asset's `VaultMode` resolved from config at initiate time,
        /// before any possible mint submission. Anchors mode-derivation for
        /// this mint the same way `RedemptionDetected.burn_mode` anchors it
        /// for redemptions: `SubmitMint`/`ConfirmMint`/`Recover` derive mode
        /// from this persisted field, never from live config, and never from
        /// the presence of `mint_authorization`. Absent on historical events,
        /// which all predate orchestrator mode (`VaultDirect`).
        #[serde(default)]
        mint_mode: VaultMode,
    },
    JournalConfirmed {
        issuer_request_id: IssuerMintRequestId,
        confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    MintingStarted {
        issuer_request_id: IssuerMintRequestId,
        started_at: DateTime<Utc>,
    },
    /// Exact signed transaction persisted before any broadcast.
    MintTxIntended {
        issuer_request_id: IssuerMintRequestId,
        prepared_tx: PreparedMintTx,
        intended_at: DateTime<Utc>,
    },
    TokensMinted {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerMintRequestId,
        error: String,
        failed_at: DateTime<Utc>,
        /// Typed failure classification. Absent for pre-orchestrator events,
        /// which replay as `Unclassified`. Typed classifications are never
        /// auto-retried: `NonceConsumedByOtherMint` needs manual
        /// reconciliation and the logic-mismatch halts resolve
        /// environment-wide.
        #[serde(default)]
        classification: MintFailureClassification,
    },
    MintCompleted {
        issuer_request_id: IssuerMintRequestId,
        completed_at: DateTime<Utc>,
    },
    /// Indicates that an existing on-chain mint was discovered during recovery.
    ExistingMintRecovered {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    },
    /// Admin-closed mint that cannot be automatically recovered.
    /// Terminal state — closed mints do not appear in stuck queries.
    MintClosed {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        /// Exact prepared deposit hash the operator acknowledged when closing
        /// over an unresolved intent. `None` when the mint had no prepared
        /// identity. Older events omit this field.
        #[serde(default)]
        acknowledged_unresolved_mint_tx_hash: Option<B256>,
        closed_at: DateTime<Utc>,
        /// Present only when closing a `NonceReplayUnresolved` mint: the
        /// operator's recorded acknowledgement that the nonce's absence was
        /// verified against a chain view outside this bot, echoing the
        /// mint's persisted authorization nonce exactly (SPEC "Mint
        /// Aggregate" -> `CloseMint`). Additive: historic closes replay as
        /// `None`.
        #[serde(default)]
        acknowledged_unresolved_mint_nonce: Option<B256>,
    },

    /// Mint transaction submitted to the signing backend (Turnkey or local).
    /// Persists the backend transaction ID so that polling can resume after
    /// a restart without resubmitting (which would double-mint).
    #[serde(alias = "FireblocksSubmitted")]
    MintTxSubmitted {
        issuer_request_id: IssuerMintRequestId,
        external_tx_id: String,
        #[serde(alias = "fireblocks_tx_id")]
        tx_id: TxId,
        submitted_at: DateTime<Utc>,
    },

    /// Indicates that a mint retry has started during recovery.
    MintRetryStarted {
        issuer_request_id: IssuerMintRequestId,
        /// The on-chain tx hash that evidences the original mint succeeded.
        /// Present when recovery is triggered by receipt discovery, `None`
        /// when triggered by startup auto-recovery (which may retry the mint).
        #[serde(default)]
        tx_hash: Option<TxHash>,
        /// Present only for operator-authorized retries. Older and automatic
        /// retry events default to `None` during replay.
        #[serde(default)]
        manual_retry_id: Option<Uuid>,
        started_at: DateTime<Utc>,
    },

    /// The liquidity bot's validated `MintAuthV1` for this mint, delivered
    /// out-of-band via the internal mint-authorization call (orchestrator
    /// mode only). This is the persistence point for the nonce — `Initiated`
    /// is written on the Alpaca POST, strictly before the authorization
    /// exists, so it cannot carry one. Does not change the lifecycle state.
    MintAuthorizationReceived {
        issuer_request_id: IssuerMintRequestId,
        mint_authorization: MintAuthorization,
        received_at: DateTime<Utc>,
    },

    /// Orchestrator-mode mint succeeded on-chain (the counterpart of
    /// `TokensMinted`), parsed from the orchestrator's `Minted` event.
    /// Carries the consumed authorization `nonce` in place of vault-direct's
    /// `receipt_id` — the orchestrator, not the bot, holds receipt custody,
    /// so there is no bot-side receipt to record or register.
    OrchestratorTokensMinted {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        /// `Minted.nonce` — the authorization nonce this mint consumed.
        nonce: B256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },

    /// An orchestrator-mode mint that already landed on-chain, discovered by
    /// recovery's `Minted`-log lookup full-matching
    /// `(to, nonce, token, amount)` after a `NonceReplayed` revert. Carries
    /// no `gas_used` — a bare log does not expose it.
    OrchestratorMintRecovered {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        nonce: B256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    },
}

impl DomainEvent for MintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initiated { .. } => "MintEvent::Initiated".to_string(),
            Self::JournalConfirmed { .. } => {
                "MintEvent::JournalConfirmed".to_string()
            }
            Self::JournalRejected { .. } => {
                "MintEvent::JournalRejected".to_string()
            }
            Self::MintingStarted { .. } => {
                "MintEvent::MintingStarted".to_string()
            }
            Self::MintTxIntended { .. } => {
                "MintEvent::MintTxIntended".to_string()
            }
            Self::TokensMinted { .. } => "MintEvent::TokensMinted".to_string(),
            Self::MintingFailed { .. } => {
                "MintEvent::MintingFailed".to_string()
            }
            Self::MintCompleted { .. } => {
                "MintEvent::MintCompleted".to_string()
            }
            Self::ExistingMintRecovered { .. } => {
                "MintEvent::ExistingMintRecovered".to_string()
            }
            Self::MintClosed { .. } => "MintEvent::MintClosed".to_string(),
            Self::MintTxSubmitted { .. } => {
                "MintEvent::MintTxSubmitted".to_string()
            }
            Self::MintRetryStarted { .. } => {
                "MintEvent::MintRetryStarted".to_string()
            }
            Self::MintAuthorizationReceived { .. } => {
                "MintEvent::MintAuthorizationReceived".to_string()
            }
            Self::OrchestratorTokensMinted { .. } => {
                "MintEvent::OrchestratorTokensMinted".to_string()
            }
            Self::OrchestratorMintRecovered { .. } => {
                "MintEvent::OrchestratorMintRecovered".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        match self {
            Self::MintTxSubmitted { .. } => "2.0".to_string(),
            _ => "1.0".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn raw_fireblocks_submitted_event_replays_populated_legacy_tx_id() {
        let raw_event = json!({
            "FireblocksSubmitted": {
                "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
                "external_tx_id": "mint-550e8400-e29b-41d4-a716-446655440000",
                "fireblocks_tx_id": "07bdef3c-5314-4d1d-94f7-f3f346cd4c2f",
                "submitted_at": "2026-07-14T12:00:00Z"
            }
        });

        let event: MintEvent = serde_json::from_value(raw_event).unwrap();

        assert!(matches!(
            event,
            MintEvent::MintTxSubmitted {
                tx_id: TxId::Legacy(ref value),
                ref external_tx_id,
                ..
            } if value == "07bdef3c-5314-4d1d-94f7-f3f346cd4c2f"
                && external_tx_id
                    == "mint-550e8400-e29b-41d4-a716-446655440000"
        ));
    }

    #[test]
    fn mint_tx_submitted_uses_tagged_tx_id_event_version() {
        let event = MintEvent::MintTxSubmitted {
            issuer_request_id: "550e8400-e29b-41d4-a716-446655440000"
                .parse()
                .unwrap(),
            external_tx_id: "mint-550e8400-e29b-41d4-a716-446655440000"
                .to_string(),
            tx_id: TxId::Legacy(
                "07bdef3c-5314-4d1d-94f7-f3f346cd4c2f".to_string(),
            ),
            submitted_at: Utc::now(),
        };

        assert_eq!(event.event_version(), "2.0");
    }
}
