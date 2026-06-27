mod api;
mod cmd;
mod event;
pub(crate) mod job;
pub(crate) mod recovery;
mod view;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use event_sorcery::{EventSourced, Table};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use api::MintResponse;

pub(crate) use api::{confirm_journal, initiate_mint};
pub(crate) use cmd::MintCommand;
pub(crate) use event::MintEvent;
pub(crate) use view::{
    MintView, find_all_recoverable_mints, find_by_issuer_request_id, find_stuck,
};

pub(crate) use crate::account::ClientId;
pub(crate) use crate::tokenized_asset::{
    Network, TokenSymbol, UnderlyingSymbol,
};
pub(crate) use crate::{Quantity, QuantityConversionError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TokenizationRequestId {
    #[cfg(test)]
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IssuerMintRequestId(Uuid);

impl IssuerMintRequestId {
    #[must_use]
    pub const fn new(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) fn random() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for IssuerMintRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for IssuerMintRequestId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Mint {
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
    },
    JournalConfirmed {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    Minting {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        minting_started_at: DateTime<Utc>,
    },
    /// Transaction submitted to signing backend, awaiting on-chain confirmation.
    /// The `fireblocks_tx_id` enables recovery: on restart, the bot resumes
    /// polling this transaction instead of resubmitting (which would double-mint).
    FireblocksSubmitted {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        minting_started_at: DateTime<Utc>,
        external_tx_id: String,
        fireblocks_tx_id: String,
    },
    CallbackPending {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        error: String,
        failed_at: DateTime<Utc>,
        /// 1-indexed attempt number of the *next* retry, driving the automatic
        /// delay schedule and exhaustion cap. Unlike the `external_tx_id`-
        /// derived attempt (see `next_retry_attempt`), this advances on every
        /// failure — including submission failures that never reached Fireblocks
        /// (no `FireblocksSubmitted` predecessor) — so the schedule still
        /// escalates and eventually exhausts. The `external_tx_id` is derived
        /// separately and is reused unchanged across such failures to stay
        /// idempotent.
        ///
        /// Note: when a submission finally lands after pre-acceptance failures,
        /// the next failure re-seeds this from the new `FireblocksSubmitted`
        /// predecessor's retry number, which can lower it (the delay schedule
        /// then runs longer than the nominal 1m/10m/30m/1h). This only affects
        /// schedule *duration*; the number of distinct on-chain mint
        /// transactions stays hard-capped at four because `external_tx_id`
        /// (and thus a new `FireblocksSubmitted`) advances only on a successful
        /// submission. So there is no double-mint and no cap breach.
        attempts: u32,
        /// The state the mint was in before it failed. Used to determine
        /// whether receipt-triggered recovery is safe (only when failed
        /// from `Minting`, meaning a tx was actually submitted).
        failed_from: Box<Self>,
    },
    Completed {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
    Closed {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        closed_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AutomaticRetryDecision {
    Ready,
    Wait(std::time::Duration),
    Exhausted,
    NotRecoverable,
}

impl Mint {
    pub(crate) const MAX_AUTOMATIC_MINT_RETRY_ATTEMPT: u32 = 4;
    const RETRY_EXTERNAL_TX_MARKER: &'static str = "-retry-";

    const fn state_name(&self) -> &'static str {
        match self {
            Self::Initiated { .. } => "Initiated",
            Self::JournalConfirmed { .. } => "JournalConfirmed",
            Self::JournalRejected { .. } => "JournalRejected",
            Self::Minting { .. } => "Minting",
            Self::FireblocksSubmitted { .. } => "FireblocksSubmitted",
            Self::CallbackPending { .. } => "CallbackPending",
            Self::MintingFailed { .. } => "MintingFailed",
            Self::Completed { .. } => "Completed",
            Self::Closed { .. } => "Closed",
        }
    }

    fn retry_attempt_from_external_tx_id(external_tx_id: &str) -> Option<u32> {
        external_tx_id
            .rsplit_once(Self::RETRY_EXTERNAL_TX_MARKER)
            .and_then(|(_, attempt)| attempt.parse().ok())
    }

    const fn automatic_retry_delay(attempt: u32) -> Option<ChronoDuration> {
        match attempt {
            1 => Some(ChronoDuration::minutes(1)),
            2 => Some(ChronoDuration::minutes(10)),
            3 => Some(ChronoDuration::minutes(30)),
            4 => Some(ChronoDuration::hours(1)),
            _ => None,
        }
    }

    pub(crate) fn automatic_retry_decision(
        &self,
        now: DateTime<Utc>,
    ) -> AutomaticRetryDecision {
        let Self::MintingFailed { failed_at, attempts, .. } = self else {
            return match self {
                Self::JournalConfirmed { .. }
                | Self::Minting { .. }
                | Self::FireblocksSubmitted { .. }
                | Self::CallbackPending { .. } => AutomaticRetryDecision::Ready,
                _ => AutomaticRetryDecision::NotRecoverable,
            };
        };

        if *attempts > Self::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT {
            return AutomaticRetryDecision::Exhausted;
        }

        let Some(delay) = Self::automatic_retry_delay(*attempts) else {
            return AutomaticRetryDecision::Exhausted;
        };
        let retry_at = *failed_at + delay;
        if now >= retry_at {
            return AutomaticRetryDecision::Ready;
        }

        (retry_at - now)
            .to_std()
            .map_or(AutomaticRetryDecision::Ready, AutomaticRetryDecision::Wait)
    }

    pub(crate) const fn tokenization_request_id(
        &self,
    ) -> Option<&TokenizationRequestId> {
        match self {
            Self::Initiated { tokenization_request_id, .. }
            | Self::JournalConfirmed { tokenization_request_id, .. }
            | Self::JournalRejected { tokenization_request_id, .. }
            | Self::Minting { tokenization_request_id, .. }
            | Self::FireblocksSubmitted { tokenization_request_id, .. }
            | Self::CallbackPending { tokenization_request_id, .. }
            | Self::MintingFailed { tokenization_request_id, .. }
            | Self::Completed { tokenization_request_id, .. } => {
                Some(tokenization_request_id)
            }
            Self::Closed { .. } => None,
        }
    }

    fn handle_confirm_journal(
        &self,
        provided_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::JournalConfirmed {
            issuer_request_id: provided_id,
            confirmed_at: now,
        }])
    }

    fn handle_reject_journal(
        &self,
        provided_id: IssuerMintRequestId,
        reason: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::JournalRejected {
            issuer_request_id: provided_id,
            reason,
            rejected_at: now,
        }])
    }

    fn validate_issuer_request_id(
        expected: &IssuerMintRequestId,
        provided: &IssuerMintRequestId,
    ) -> Result<(), MintError> {
        if provided != expected {
            return Err(MintError::IssuerMintRequestIdMismatch {
                expected: expected.clone(),
                provided: provided.clone(),
            });
        }
        Ok(())
    }

    /// Records the intent to mint by transitioning from `JournalConfirmed`
    /// to `Minting` state. Pure state transition — no network call.
    fn handle_deposit(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::JournalConfirmed { issuer_request_id: expected_id, .. } =
            self
        else {
            return Err(MintError::NotInJournalConfirmedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        Ok(vec![MintEvent::MintingStarted {
            issuer_request_id,
            started_at: Utc::now(),
        }])
    }

    /// Records a successful on-chain mint submission reported by a durable
    /// `SubmitMintJob`. Pure — emits `FireblocksSubmitted` from the payload.
    /// Idempotent: a no-op once the mint has advanced past `Minting`, so an
    /// at-least-once job re-run cannot double-record the submission.
    fn handle_record_fireblocks_submitted(
        &self,
        issuer_request_id: IssuerMintRequestId,
        external_tx_id: String,
        fireblocks_tx_id: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Minting { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::FireblocksSubmitted {
                    issuer_request_id,
                    external_tx_id,
                    fireblocks_tx_id,
                    submitted_at: Utc::now(),
                }])
            }
            Self::FireblocksSubmitted { .. }
            | Self::CallbackPending { .. }
            | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records a confirmed on-chain mint reported by a durable `ConfirmMintJob`.
    /// Pure — emits `TokensMinted` from the payload. Idempotent: a no-op once
    /// the mint has advanced past `FireblocksSubmitted`.
    fn handle_record_tokens_minted(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::FireblocksSubmitted {
                issuer_request_id: expected_id,
                ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::TokensMinted {
                    issuer_request_id,
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used,
                    block_number,
                    minted_at: Utc::now(),
                }])
            }
            Self::CallbackPending { .. } | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInFireblocksSubmittedState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records a sent Alpaca callback reported by a durable `SendCallbackJob`.
    /// Pure — emits `MintCompleted`. Idempotent: a no-op once the mint is
    /// already `Completed`.
    fn handle_record_callback_sent(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::CallbackPending {
                issuer_request_id: expected_id, ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::MintCompleted {
                    issuer_request_id,
                    completed_at: Utc::now(),
                }])
            }
            Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInCallbackPendingState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Records a mint side-effect failure reported by a durable submission or
    /// confirmation job. Pure — emits `MintingFailed` from the payload.
    /// Idempotent and lenient: a stale failure report for a mint that already
    /// failed or advanced is ignored, so an at-least-once job re-run is safe.
    fn handle_record_mint_failed(
        &self,
        issuer_request_id: IssuerMintRequestId,
        error: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Minting { issuer_request_id: expected_id, .. }
            | Self::FireblocksSubmitted {
                issuer_request_id: expected_id,
                ..
            } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::MintingFailed {
                    issuer_request_id,
                    error,
                    failed_at: Utc::now(),
                }])
            }
            _ => Ok(vec![]),
        }
    }

    /// Retries a failed mint by transitioning `MintingFailed` -> `Minting`,
    /// advancing the automatic-retry attempt counter. Pure — emits
    /// `MintRetryStarted`. Recovery sends this before re-enqueuing a
    /// `SubmitMintJob`. Idempotent: a no-op if the mint already left
    /// `MintingFailed` (e.g. a concurrent retry already started).
    fn handle_retry_mint(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::MintingFailed { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::MintRetryStarted {
                    issuer_request_id,
                    tx_hash: None,
                    started_at: Utc::now(),
                }])
            }
            _ => Ok(vec![]),
        }
    }

    /// Records a mint whose on-chain transaction already succeeded (a receipt
    /// exists), reported by the `SubmitMintJob` before it re-submitted. Pure —
    /// emits `ExistingMintRecovered`, advancing to `CallbackPending` without
    /// re-minting. Idempotent: a no-op once the mint is already minted.
    fn handle_record_existing_mint(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Minting { issuer_request_id: expected_id, .. }
            | Self::FireblocksSubmitted {
                issuer_request_id: expected_id,
                ..
            }
            | Self::MintingFailed { issuer_request_id: expected_id, .. } => {
                Self::validate_issuer_request_id(
                    expected_id,
                    &issuer_request_id,
                )?;

                Ok(vec![MintEvent::ExistingMintRecovered {
                    issuer_request_id,
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    block_number,
                    recovered_at: Utc::now(),
                }])
            }
            Self::CallbackPending { .. } | Self::Completed { .. } => Ok(vec![]),
            _ => Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    fn apply_journal_confirmed(&mut self, confirmed_at: DateTime<Utc>) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at: confirmed_at,
        };
    }

    fn apply_journal_rejected(
        &mut self,
        reason: String,
        rejected_at: DateTime<Utc>,
    ) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalRejected {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            reason,
            rejected_at,
        };
    }

    fn apply_minting_started(&mut self, started_at: DateTime<Utc>) {
        let Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at: started_at,
        };
    }

    fn apply_fireblocks_submitted(
        &mut self,
        external_tx_id: String,
        fireblocks_tx_id: String,
        _submitted_at: DateTime<Utc>,
    ) {
        let Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::FireblocksSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at,
            external_tx_id,
            fireblocks_tx_id,
        };
    }

    fn apply_tokens_minted(
        &mut self,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
    ) {
        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }
        | Self::FireblocksSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        };
    }

    fn apply_minting_failed(
        &mut self,
        error: String,
        failed_at: DateTime<Utc>,
    ) {
        // Re-failure while already in MintingFailed (e.g. a recovery confirm
        // or resubmission attempt failed again): refresh error/failed_at and
        // advance the attempt counter so the delay schedule escalates and
        // eventually exhausts, but keep the original failed_from chain (and the
        // external_tx_id derived from it) so resubmission stays idempotent.
        if let Self::MintingFailed {
            error: existing_error,
            failed_at: existing_failed_at,
            attempts,
            ..
        } = self
        {
            *existing_error = error;
            *existing_failed_at = failed_at;
            *attempts += 1;
            return;
        }

        // First failure from a live state: seed the attempt counter from the
        // FireblocksSubmitted predecessor's retry number when present, else 1
        // (a submission that failed before Fireblocks accepted it).
        let attempts = match self {
            Self::FireblocksSubmitted { external_tx_id, .. } => {
                Self::retry_attempt_from_external_tx_id(external_tx_id)
                    .unwrap_or(0)
                    + 1
            }
            _ => 1,
        };

        let failed_from = Box::new(self.clone());

        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }
        | Self::FireblocksSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            error,
            failed_at,
            attempts,
            failed_from,
        };
    }

    fn apply_mint_completed(&mut self, completed_at: DateTime<Utc>) {
        let Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::Completed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
            completed_at,
        };
    }

    fn apply_existing_mint_recorded(
        &mut self,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    ) {
        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }
        | Self::FireblocksSubmitted {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }
        | Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used: None,
            block_number,
            minted_at: recovered_at,
        };
    }

    fn apply_mint_retry_started(&mut self, started_at: DateTime<Utc>) {
        let Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        } = self.clone()
        else {
            return;
        };

        *self = Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at: started_at,
        };
    }
    fn handle_close_mint(
        &self,
        issuer_request_id: IssuerMintRequestId,
        reason: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::Completed { .. } | Self::Closed { .. } => {
                Err(MintError::NotRecoverable {
                    current_state: self.state_name().to_string(),
                })
            }
            _ => Ok(vec![MintEvent::MintClosed {
                issuer_request_id,
                reason,
                closed_at: Utc::now(),
            }]),
        }
    }
}

#[async_trait]
impl EventSourced for Mint {
    type Id = IssuerMintRequestId;
    type Event = MintEvent;
    type Command = MintCommand;
    type Error = MintError;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "Mint";
    const PROJECTION: Table = Table("mint_view");
    const SCHEMA_VERSION: u64 = 2;

    // Snapshots are disabled: the pre-migration wiring never wrote snapshots,
    // and event-sorcery hardwires snapshot-every-N with no off switch, so
    // usize::MAX makes the next-snapshot threshold unreachable. The proper
    // fix is for event-sorcery to take the snapshot policy explicitly from
    // the consumer, including the option to disable snapshotting entirely.
    const SNAPSHOT_SIZE: usize = usize::MAX;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            MintEvent::Initiated {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at,
            } => Some(Self::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id: *client_id,
                wallet: *wallet,
                initiated_at: *initiated_at,
            }),
            _ => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        let mut next = entity.clone();
        next.apply_event(event.clone());
        Ok(Some(next))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            MintCommand::Initiate {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            } => Ok(vec![MintEvent::Initiated {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }]),
            MintCommand::ConfirmJournal { .. }
            | MintCommand::RejectJournal { .. } => {
                Err(MintError::NotInInitiatedState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::Deposit { .. } => {
                Err(MintError::NotInJournalConfirmedState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordFireblocksSubmitted { .. }
            | MintCommand::RecordExistingMint { .. } => {
                Err(MintError::NotInMintingState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordTokensMinted { .. } => {
                Err(MintError::NotInFireblocksSubmittedState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordCallbackSent { .. } => {
                Err(MintError::NotInCallbackPendingState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordMintFailed { .. }
            | MintCommand::RetryMint { .. } => Ok(vec![]),
            MintCommand::CloseMint { .. } => Err(MintError::NotRecoverable {
                current_state: "Uninitialized".to_string(),
            }),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            MintCommand::Initiate {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            } => {
                if matches!(self, Self::Initiated { .. }) {
                    Err(MintError::AlreadyInitiated {
                        tokenization_request_id: tokenization_request_id.0,
                    })
                } else {
                    Ok(vec![MintEvent::Initiated {
                        issuer_request_id,
                        tokenization_request_id,
                        quantity,
                        underlying,
                        token,
                        network,
                        client_id,
                        wallet,
                        initiated_at: Utc::now(),
                    }])
                }
            }
            MintCommand::ConfirmJournal { issuer_request_id } => {
                self.handle_confirm_journal(issuer_request_id)
            }
            MintCommand::RejectJournal { issuer_request_id, reason } => {
                self.handle_reject_journal(issuer_request_id, reason)
            }
            MintCommand::Deposit { issuer_request_id } => {
                self.handle_deposit(issuer_request_id)
            }
            MintCommand::RecordFireblocksSubmitted {
                issuer_request_id,
                external_tx_id,
                fireblocks_tx_id,
            } => self.handle_record_fireblocks_submitted(
                issuer_request_id,
                external_tx_id,
                fireblocks_tx_id,
            ),
            MintCommand::RecordTokensMinted {
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            } => self.handle_record_tokens_minted(
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            ),
            MintCommand::RecordCallbackSent { issuer_request_id } => {
                self.handle_record_callback_sent(issuer_request_id)
            }
            MintCommand::RecordMintFailed { issuer_request_id, error } => {
                self.handle_record_mint_failed(issuer_request_id, error)
            }
            MintCommand::RetryMint { issuer_request_id } => {
                self.handle_retry_mint(issuer_request_id)
            }
            MintCommand::RecordExistingMint {
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
            } => self.handle_record_existing_mint(
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
            ),
            MintCommand::CloseMint { issuer_request_id, reason } => {
                self.handle_close_mint(issuer_request_id, reason)
            }
        }
    }
}

impl Mint {
    fn apply_event(&mut self, event: MintEvent) {
        match event {
            MintEvent::Initiated {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at,
            } => {
                *self = Self::Initiated {
                    issuer_request_id,
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at,
                };
            }
            MintEvent::JournalConfirmed {
                issuer_request_id: _,
                confirmed_at,
            } => self.apply_journal_confirmed(confirmed_at),
            MintEvent::JournalRejected {
                issuer_request_id: _,
                reason,
                rejected_at,
            } => self.apply_journal_rejected(reason, rejected_at),
            MintEvent::MintingStarted { issuer_request_id: _, started_at } => {
                self.apply_minting_started(started_at);
            }
            MintEvent::TokensMinted {
                issuer_request_id: _,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
                minted_at,
            } => self.apply_tokens_minted(
                tx_hash,
                receipt_id,
                shares_minted,
                Some(gas_used),
                block_number,
                minted_at,
            ),
            MintEvent::MintingFailed {
                issuer_request_id: _,
                error,
                failed_at,
            } => self.apply_minting_failed(error, failed_at),
            MintEvent::MintCompleted { issuer_request_id: _, completed_at } => {
                self.apply_mint_completed(completed_at);
            }
            MintEvent::ExistingMintRecovered {
                issuer_request_id: _,
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
                recovered_at,
            } => self.apply_existing_mint_recorded(
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
                recovered_at,
            ),
            MintEvent::FireblocksSubmitted {
                issuer_request_id: _,
                external_tx_id,
                fireblocks_tx_id,
                submitted_at,
            } => {
                self.apply_fireblocks_submitted(
                    external_tx_id,
                    fireblocks_tx_id,
                    submitted_at,
                );
            }
            MintEvent::MintRetryStarted {
                issuer_request_id: _,
                started_at,
                ..
            } => {
                self.apply_mint_retry_started(started_at);
            }
            MintEvent::MintClosed { issuer_request_id, reason, closed_at } => {
                *self = Self::Closed { issuer_request_id, reason, closed_at };
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum MintError {
    #[error(
        "Mint already initiated for tokenization request: {tokenization_request_id}"
    )]
    AlreadyInitiated { tokenization_request_id: String },
    #[error("Mint not in Initiated state. Current state: {current_state}")]
    NotInInitiatedState { current_state: String },
    #[error(
        "Mint not in JournalConfirmed state. Current state: {current_state}"
    )]
    NotInJournalConfirmedState { current_state: String },
    #[error(
        "Mint not in CallbackPending state. Current state: {current_state}"
    )]
    NotInCallbackPendingState { current_state: String },
    #[error(
        "Mint not in FireblocksSubmitted state. Current state: {current_state}"
    )]
    NotInFireblocksSubmittedState { current_state: String },
    #[error(
        "Issuer request ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    IssuerMintRequestIdMismatch {
        expected: IssuerMintRequestId,
        provided: IssuerMintRequestId,
    },
    #[error("Mint not in Minting state. Current state: {current_state}")]
    NotInMintingState { current_state: String },
    #[error(
        "Mint not in Minting or MintingFailed state. Current state: {current_state}"
    )]
    NotInMintingOrMintingFailedState { current_state: String },
    #[error("Mint not in recoverable state. Current state: {current_state}")]
    NotRecoverable { current_state: String },
    #[error("Automatic mint retry is not due until {retry_at}")]
    RetryNotDue { retry_at: DateTime<Utc> },
    #[error("Automatic mint retries exhausted after {attempts} attempts")]
    AutomaticRetriesExhausted { attempts: u32 },
    #[error("Fireblocks mint transaction is still pending: {fireblocks_tx_id}")]
    FireblocksTxStillPending { fireblocks_tx_id: String },
    #[error("Retry delay out of range")]
    RetryDelayOutOfRange,
    #[error(
        "Fireblocks tx ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    FireblocksTxIdMismatch { expected: String, provided: String },
    #[error("Asset not found for underlying: {underlying}")]
    AssetNotFound { underlying: UnderlyingSymbol },
    #[error("Quantity conversion: {message}")]
    QuantityConversion { message: String },
    #[error("Asset view: {message}")]
    AssetView { message: String },
    #[error("Alpaca: {message}")]
    Alpaca { message: String },
    #[error("Receipt lookup: {message}")]
    ReceiptLookup { message: String },
    #[error("Vault: {message}")]
    Vault { message: String },
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::{Address, address, b256, uint};
    use chrono::{DateTime, Utc};
    use event_sorcery::{LifecycleError, StoreBuilder, TestHarness, replay};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use tracing::Level;
    use tracing_test::traced_test;
    use uuid::{Uuid, uuid};

    use super::{
        ClientId, IssuerMintRequestId, Mint, MintCommand, MintError, MintEvent,
        Network, Quantity, TokenSymbol, TokenizationRequestId,
        UnderlyingSymbol,
    };
    use crate::prepare_event_sourced_startup;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};

    pub(super) const VAULT: Address =
        address!("0xcccccccccccccccccccccccccccccccccccccccc");

    fn minting_events_for_retry(
        issuer_request_id: &IssuerMintRequestId,
        external_tx_id: String,
        failed_at: DateTime<Utc>,
    ) -> Vec<MintEvent> {
        vec![
            MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: failed_at,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: failed_at,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: failed_at,
            },
            MintEvent::FireblocksSubmitted {
                issuer_request_id: issuer_request_id.clone(),
                external_tx_id,
                fireblocks_tx_id: "fb-failed".to_string(),
                submitted_at: failed_at,
            },
            MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "terminal Fireblocks failure".to_string(),
                failed_at,
            },
        ]
    }

    fn events_through_minting(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        vec![
            MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: now,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: now,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: now,
            },
        ]
    }

    fn events_through_fireblocks_submitted(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_minting(issuer_request_id);
        events.push(MintEvent::FireblocksSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: "ext-1".to_string(),
            fireblocks_tx_id: "fb-1".to_string(),
            submitted_at: Utc::now(),
        });
        events
    }

    fn events_through_tokens_minted(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_fireblocks_submitted(issuer_request_id);
        events.push(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: uint!(42_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: 21_000,
            block_number: 1_000,
            minted_at: Utc::now(),
        });
        events
    }

    fn events_through_completed(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_tokens_minted(issuer_request_id);
        events.push(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at: Utc::now(),
        });
        events
    }

    #[tokio::test]
    async fn record_fireblocks_submitted_from_minting_emits_event() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::RecordFireblocksSubmitted {
                issuer_request_id: issuer_request_id.clone(),
                external_tx_id: "ext-1".to_string(),
                fireblocks_tx_id: "fb-1".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MintEvent::FireblocksSubmitted { fireblocks_tx_id, .. }
                if fireblocks_tx_id == "fb-1"
        ));
    }

    #[tokio::test]
    async fn record_fireblocks_submitted_is_idempotent_once_submitted() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_fireblocks_submitted(&issuer_request_id))
            .when(MintCommand::RecordFireblocksSubmitted {
                issuer_request_id,
                external_tx_id: "ext-1".to_string(),
                fireblocks_tx_id: "fb-1".to_string(),
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "re-recording an already-submitted mint must be a no-op"
        );
    }

    #[tokio::test]
    async fn record_tokens_minted_from_fireblocks_submitted_emits_event() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_fireblocks_submitted(&issuer_request_id))
            .when(MintCommand::RecordTokensMinted {
                issuer_request_id,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                receipt_id: uint!(7_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 21_000,
                block_number: 1_234,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], MintEvent::TokensMinted { .. }));
    }

    #[tokio::test]
    async fn record_tokens_minted_is_idempotent_once_minted() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tokens_minted(&issuer_request_id))
            .when(MintCommand::RecordTokensMinted {
                issuer_request_id,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                receipt_id: uint!(7_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 21_000,
                block_number: 1_234,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "re-recording an already-minted mint must be a no-op"
        );
    }

    #[tokio::test]
    async fn record_callback_sent_from_callback_pending_completes() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_tokens_minted(&issuer_request_id))
            .when(MintCommand::RecordCallbackSent {
                issuer_request_id: issuer_request_id.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], MintEvent::MintCompleted { .. }));
    }

    #[tokio::test]
    async fn record_callback_sent_is_idempotent_once_completed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_completed(&issuer_request_id))
            .when(MintCommand::RecordCallbackSent { issuer_request_id })
            .await
            .events();

        assert!(
            events.is_empty(),
            "re-recording the callback for a completed mint must be a no-op"
        );
    }

    #[tokio::test]
    async fn record_mint_failed_from_minting_emits_failed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::RecordMintFailed {
                issuer_request_id,
                error: "submission rejected".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MintEvent::MintingFailed { error, .. }
                if error == "submission rejected"
        ));
    }

    #[tokio::test]
    async fn record_mint_failed_is_ignored_once_completed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_completed(&issuer_request_id))
            .when(MintCommand::RecordMintFailed {
                issuer_request_id,
                error: "stale failure report".to_string(),
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "a stale failure report for a completed mint must be ignored"
        );
    }

    #[tokio::test]
    async fn retry_mint_from_minting_failed_emits_retry_started() {
        let issuer_request_id = IssuerMintRequestId::random();
        let failed_at = Utc::now() - chrono::Duration::hours(2);

        let events = TestHarness::<Mint>::with(())
            .given(minting_events_for_retry(
                &issuer_request_id,
                "ext-1".to_string(),
                failed_at,
            ))
            .when(MintCommand::RetryMint { issuer_request_id })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MintEvent::MintRetryStarted { tx_hash: None, .. }
        ));
    }

    #[tokio::test]
    async fn retry_mint_is_idempotent_when_not_failed() {
        let issuer_request_id = IssuerMintRequestId::random();

        let events = TestHarness::<Mint>::with(())
            .given(events_through_minting(&issuer_request_id))
            .when(MintCommand::RetryMint { issuer_request_id })
            .await
            .events();

        assert!(
            events.is_empty(),
            "retrying a mint that is not in MintingFailed must be a no-op"
        );
    }

    prop_compose! {
        pub(crate) fn arb_issuer_request_id()(bytes in any::<[u8; 16]>()) -> IssuerMintRequestId {
            IssuerMintRequestId::new(Uuid::from_bytes(bytes))
        }
    }

    #[tokio::test]
    async fn test_initiate_mint_creates_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<Mint>::with(())
            .given_no_previous_events()
            .when(MintCommand::Initiate {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id,
                wallet,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        match &events[0] {
            MintEvent::Initiated {
                issuer_request_id: event_issuer_id,
                tokenization_request_id: event_tokenization_id,
                quantity: event_quantity,
                underlying: event_underlying,
                token: event_token,
                network: event_network,
                client_id: event_client_id,
                wallet: event_wallet,
                initiated_at,
            } => {
                assert_eq!(event_issuer_id, &issuer_request_id);
                assert_eq!(event_tokenization_id, &tokenization_request_id);
                assert_eq!(event_quantity, &quantity);
                assert_eq!(event_underlying, &underlying);
                assert_eq!(event_token, &token);
                assert_eq!(event_network, &network);
                assert_eq!(event_client_id, &client_id);
                assert_eq!(event_wallet, &wallet);
                assert!(initiated_at.timestamp() > 0);
            }
            MintEvent::JournalConfirmed { .. }
            | MintEvent::JournalRejected { .. }
            | MintEvent::MintingStarted { .. }
            | MintEvent::FireblocksSubmitted { .. }
            | MintEvent::TokensMinted { .. }
            | MintEvent::MintingFailed { .. }
            | MintEvent::MintCompleted { .. }
            | MintEvent::ExistingMintRecovered { .. }
            | MintEvent::MintRetryStarted { .. }
            | MintEvent::MintClosed { .. } => {
                panic!("Expected MintInitiated event, got {:?}", &events[0])
            }
        }
    }

    #[tokio::test]
    async fn test_initiate_mint_when_already_initiated_returns_error() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id,
                wallet,
                initiated_at: chrono::Utc::now(),
            }])
            .when(MintCommand::Initiate {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::AlreadyInitiated { .. })
            ),
            "Expected AlreadyInitiated error, got {error:?}"
        );
    }

    #[test]
    fn test_apply_initiated_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(50));
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = chrono::Utc::now();

        let mint = replay::<Mint>(vec![MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
        }])
        .unwrap()
        .unwrap();

        let Mint::Initiated {
            issuer_request_id: applied_issuer_id,
            tokenization_request_id: applied_tokenization_id,
            quantity: applied_quantity,
            underlying: applied_underlying,
            token: applied_token,
            network: applied_network,
            client_id: applied_client_id,
            wallet: applied_wallet,
            initiated_at: applied_initiated_at,
        } = mint
        else {
            panic!("Expected Initiated, got Uninitialized")
        };

        assert_eq!(applied_issuer_id, issuer_request_id);
        assert_eq!(applied_tokenization_id, tokenization_request_id);
        assert_eq!(applied_quantity, quantity);
        assert_eq!(applied_underlying, underlying);
        assert_eq!(applied_token, token);
        assert_eq!(applied_network, network);
        assert_eq!(applied_client_id, client_id);
        assert_eq!(applied_wallet, wallet);
        assert_eq!(applied_initiated_at, initiated_at);
    }

    #[test]
    fn test_apply_journal_confirmed_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut mint = Mint::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };

        let confirmed_at = Utc::now();

        mint.apply_event(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        });

        let Mint::JournalConfirmed {
            issuer_request_id: state_issuer_id,
            journal_confirmed_at,
            ..
        } = mint
        else {
            panic!("Expected JournalConfirmed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(journal_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_apply_journal_rejected_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut mint = Mint::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };

        let rejected_at = Utc::now();
        let reason = "Insufficient funds".to_string();

        mint.apply_event(MintEvent::JournalRejected {
            issuer_request_id: issuer_request_id.clone(),
            reason: reason.clone(),
            rejected_at,
        });

        let Mint::JournalRejected {
            issuer_request_id: state_issuer_id,
            reason: state_reason,
            rejected_at: state_rejected_at,
            ..
        } = mint
        else {
            panic!("Expected JournalRejected state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_reason, reason);
        assert_eq!(state_rejected_at, rejected_at);
    }

    #[tokio::test]
    async fn test_confirm_journal_produces_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let events = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::ConfirmJournal { issuer_request_id })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], MintEvent::JournalConfirmed { .. }));
    }

    #[tokio::test]
    async fn test_confirm_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given_no_previous_events()
            .when(MintCommand::ConfirmJournal { issuer_request_id })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(MintError::NotInInitiatedState { .. })
        ));
    }

    #[tokio::test]
    async fn test_confirm_journal_for_already_confirmed_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at: Utc::now(),
                },
                MintEvent::JournalConfirmed {
                    issuer_request_id: issuer_request_id.clone(),
                    confirmed_at: Utc::now(),
                },
            ])
            .when(MintCommand::ConfirmJournal { issuer_request_id })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(MintError::NotInInitiatedState { .. })
        ));
    }

    #[tokio::test]
    async fn test_reject_journal_produces_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let reason = "Insufficient funds";

        let events = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::RejectJournal {
                issuer_request_id,
                reason: reason.to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let MintEvent::JournalRejected { reason: event_reason, .. } =
            &events[0]
        else {
            panic!("Expected JournalRejected event, got {:?}", &events[0]);
        };

        assert_eq!(event_reason, reason);
    }

    #[tokio::test]
    async fn test_reject_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();

        let error = TestHarness::<Mint>::with(())
            .given_no_previous_events()
            .when(MintCommand::RejectJournal {
                issuer_request_id,
                reason: "Test reason".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(MintError::NotInInitiatedState { .. })
        ));
    }

    #[tokio::test]
    async fn test_confirm_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id = IssuerMintRequestId::random();
        let wrong_issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: correct_issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::ConfirmJournal {
                issuer_request_id: wrong_issuer_request_id,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(
                    MintError::IssuerMintRequestIdMismatch { .. }
                )
            ),
            "Expected IssuerMintRequestIdMismatch error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn test_reject_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id = IssuerMintRequestId::random();
        let wrong_issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = TestHarness::<Mint>::with(())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: correct_issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::RejectJournal {
                issuer_request_id: wrong_issuer_request_id,
                reason: "Test reason".to_string(),
            })
            .await
            .then_expect_error();
        assert!(
            matches!(
                error,
                LifecycleError::Apply(
                    MintError::IssuerMintRequestIdMismatch { .. }
                )
            ),
            "Expected IssuerMintRequestIdMismatch error, got {error:?}"
        );
    }

    #[test]
    fn test_apply_minting_started_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut mint = Mint::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };

        let minting_started_at = Utc::now();
        mint.apply_event(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: minting_started_at,
        });

        let Mint::Minting {
            issuer_request_id: state_issuer_id,
            tokenization_request_id: state_tok_id,
            quantity: state_quantity,
            underlying: state_underlying,
            token: state_token,
            network: state_network,
            client_id: state_client_id,
            wallet: state_wallet,
            initiated_at: state_initiated_at,
            journal_confirmed_at: state_journal_confirmed_at,
            minting_started_at: state_minting_started_at,
        } = mint
        else {
            panic!("Expected Minting state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tok_id, tokenization_request_id);
        assert_eq!(state_quantity, quantity);
        assert_eq!(state_underlying, underlying);
        assert_eq!(state_token, token);
        assert_eq!(state_network, network);
        assert_eq!(state_client_id, client_id);
        assert_eq!(state_wallet, wallet);
        assert_eq!(state_initiated_at, initiated_at);
        assert_eq!(state_journal_confirmed_at, journal_confirmed_at);
        assert_eq!(state_minting_started_at, minting_started_at);
    }

    #[test]
    fn test_apply_tokens_minted_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut mint = Mint::Minting {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at,
        };

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        mint.apply_event(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        });

        let Mint::CallbackPending {
            issuer_request_id: state_issuer_id,
            tx_hash: state_tx_hash,
            receipt_id: state_receipt_id,
            shares_minted: state_shares_minted,
            gas_used: state_gas_used,
            block_number: state_block_number,
            minted_at: state_minted_at,
            ..
        } = mint
        else {
            panic!("Expected CallbackPending state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_receipt_id, receipt_id);
        assert_eq!(state_shares_minted, shares_minted);
        assert_eq!(state_gas_used, Some(gas_used));
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_minted_at, minted_at);
    }

    #[test]
    fn test_apply_mint_completed_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        let mut mint = Mint::CallbackPending {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used: Some(gas_used),
            block_number,
            minted_at,
        };

        let completed_at = Utc::now();

        mint.apply_event(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at,
        });

        let Mint::Completed {
            issuer_request_id: state_issuer_id,
            tx_hash: state_tx_hash,
            receipt_id: state_receipt_id,
            shares_minted: state_shares_minted,
            gas_used: state_gas_used,
            block_number: state_block_number,
            minted_at: state_minted_at,
            completed_at: state_completed_at,
            ..
        } = mint
        else {
            panic!("Expected Completed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_receipt_id, receipt_id);
        assert_eq!(state_shares_minted, shares_minted);
        assert_eq!(state_gas_used, Some(gas_used));
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_minted_at, minted_at);
        assert_eq!(state_completed_at, completed_at);
    }

    #[test]
    fn test_full_mint_flow_to_completed() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-flow-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::Base;
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();
        let completed_at = Utc::now();

        let mut mint = replay::<Mint>(vec![MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        }])
        .unwrap()
        .unwrap();

        assert!(
            matches!(mint, Mint::Initiated { .. }),
            "Expected Initiated state, got {mint:?}"
        );

        mint.apply_event(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        });

        assert!(
            matches!(mint, Mint::JournalConfirmed { .. }),
            "Expected JournalConfirmed state, got {mint:?}"
        );

        let minting_started_at = Utc::now();
        mint.apply_event(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: minting_started_at,
        });

        assert!(
            matches!(mint, Mint::Minting { .. }),
            "Expected Minting state, got {mint:?}"
        );

        mint.apply_event(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        });

        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "Expected CallbackPending state, got {mint:?}"
        );

        mint.apply_event(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at,
        });

        let Mint::Completed {
            issuer_request_id: final_id,
            completed_at: final_completed_at,
            ..
        } = mint
        else {
            panic!("Expected Completed state, got {mint:?}");
        };

        assert_eq!(final_id, issuer_request_id);
        assert_eq!(final_completed_at, completed_at);
    }

    /// Regression: pre-event-sorcery snapshot payloads (`{"Completed": ...}`)
    /// must be cleared by schema reconciliation before projection catch-up
    /// calls `load_with_context`, which deserializes into `Lifecycle<Mint>`.
    #[traced_test]
    #[tokio::test]
    async fn pre_lifecycle_snapshot_cleared_before_projection_catch_up() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let mint_id = IssuerMintRequestId::new(uuid!(
            "550e8400-e29b-41d4-a716-446655440000"
        ));
        let mint_id_str = mint_id.to_string();
        let now = Utc::now();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'SchemaRegistry',
                'schema',
                1,
                'SchemaRegistryEvent::VersionUpdated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(
            serde_json::json!({
                "VersionUpdated": { "name": "Mint", "version": 1 }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'Mint',
                ?,
                1,
                'MintEvent::Initiated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(mint_id_str.as_str())
        .bind(
            serde_json::json!({
                "Initiated": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO snapshots (
                aggregate_type,
                aggregate_id,
                last_sequence,
                snapshot_version,
                payload,
                timestamp
            )
            VALUES (
                'Mint',
                ?,
                1,
                0,
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ",
        )
        .bind(mint_id_str.as_str())
        .bind(
            serde_json::json!({
                "Completed": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
                    "journal_confirmed_at": now,
                    "tx_hash": "0xbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00d",
                    "receipt_id": "1",
                    "shares_minted": "1000000000000000000",
                    "gas_used": 21000,
                    "block_number": 1,
                    "minted_at": now,
                    "completed_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        let (asset_store, _asset_projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        asset_store
            .send(
                &UnderlyingSymbol::new("AAPL"),
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    vault: VAULT,
                },
            )
            .await
            .unwrap();

        prepare_event_sourced_startup::<Mint>(&pool).await.unwrap();
        StoreBuilder::<Mint>::new(pool.clone()).build(()).await.unwrap();

        assert!(logs_contain_at!(
            Level::INFO,
            &["Cleared stale snapshots", "Mint"]
        ));

        let stale_snapshot_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM snapshots
            WHERE aggregate_type = 'Mint'
              AND aggregate_id = ?
            ",
        )
        .bind(mint_id_str.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            stale_snapshot_count, 0,
            "Schema reconciliation must delete incompatible Mint snapshots"
        );
    }

    async fn seed_pre_lifecycle_mint_view_row(
        pool: &Pool<Sqlite>,
        mint_id: &str,
        version: i64,
        payload: &serde_json::Value,
    ) {
        sqlx::query(
            "
            INSERT INTO mint_view (view_id, version, payload)
            VALUES (?, ?, ?)
            ",
        )
        .bind(mint_id)
        .bind(version)
        .bind(payload.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    /// Regression: pre-event-sorcery `mint_view` payloads (`{"Completed": ...}`)
    /// must be cleared on schema version change before projection catch-up calls
    /// `load_with_context`, which deserializes into `Lifecycle<Mint>`.
    #[tokio::test]
    async fn pre_lifecycle_mint_view_cleared_before_projection_catch_up() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let mint_id = IssuerMintRequestId::new(uuid!(
            "550e8400-e29b-41d4-a716-446655440000"
        ));
        let mint_id_str = mint_id.to_string();
        let now = Utc::now();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'SchemaRegistry',
                'schema',
                1,
                'SchemaRegistryEvent::VersionUpdated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(
            serde_json::json!({
                "VersionUpdated": { "name": "Mint", "version": 1 }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'Mint',
                ?,
                1,
                'MintEvent::Initiated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(mint_id_str.as_str())
        .bind(
            serde_json::json!({
                "Initiated": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale-view",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        seed_pre_lifecycle_mint_view_row(
            &pool,
            &mint_id_str,
            1,
            &serde_json::json!({
                "Completed": {
                    "issuer_request_id": mint_id_str,
                    "tokenization_request_id": "tok-stale-view",
                    "quantity": "1.0",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "wallet": "0x1234567890123456789012345678901234567890",
                    "initiated_at": now,
                    "journal_confirmed_at": now,
                    "tx_hash": "0xbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00dbaadf00d",
                    "receipt_id": "1",
                    "shares_minted": "1000000000000000000",
                    "gas_used": 21000,
                    "block_number": 1,
                    "minted_at": now,
                    "completed_at": now,
                }
            }),
        )
        .await;

        let (asset_store, _asset_projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        asset_store
            .send(
                &UnderlyingSymbol::new("AAPL"),
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    vault: VAULT,
                },
            )
            .await
            .unwrap();

        prepare_event_sourced_startup::<Mint>(&pool).await.unwrap();
        StoreBuilder::<Mint>::new(pool.clone()).build(()).await.unwrap();

        let view_payload: String = sqlx::query_scalar(
            "
            SELECT payload
            FROM mint_view
            WHERE view_id = ?
            ",
        )
        .bind(mint_id_str.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();

        let payload: serde_json::Value =
            serde_json::from_str(&view_payload).unwrap();
        assert!(
            payload
                .get("Live")
                .and_then(|live| live.get("Initiated"))
                .is_some(),
            "Projection catch-up must rebuild mint_view with Lifecycle payload, got {payload}"
        );
    }

    #[test]
    fn test_issuer_request_id_display() {
        let uuid = Uuid::new_v4();
        let id = IssuerMintRequestId::new(uuid);
        assert_eq!(format!("{id}"), uuid.to_string());
    }

    #[test]
    fn test_tokenization_request_id_display() {
        let id = TokenizationRequestId::new("alp-456");
        assert_eq!(format!("{id}"), "alp-456");
    }
}
