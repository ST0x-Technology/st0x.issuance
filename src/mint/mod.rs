mod api;
mod cmd;
mod event;
pub(crate) mod job;
pub(crate) mod recovery;
mod view;

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use event_sorcery::{EventSourced, Table};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::alpaca::{AlpacaService, MintCallbackRequest};
use crate::receipt_inventory::{
    MintedReceiptParams, ReceiptId, ReceiptService, Shares,
};
use crate::tokenized_asset::view::find_vault_by_underlying;
use crate::vault::{FireblocksTxStatus, ReceiptInformation, VaultService};

pub use api::MintResponse;

pub(crate) use api::{confirm_journal, initiate_mint};
pub(crate) use cmd::{MintCommand, MintRecoveryMode};
pub(crate) use event::MintEvent;
pub(crate) use view::{
    MintView, find_all_recoverable_mints, find_by_issuer_request_id, find_stuck,
};

/// Services required by the Mint aggregate for command handling.
pub(crate) struct MintServices {
    pub(crate) vault: Arc<dyn VaultService>,
    pub(crate) alpaca: Arc<dyn AlpacaService>,
    pub(crate) receipts: Arc<dyn ReceiptService>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) bot: Address,
}

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

/// Groups mint submission parameters to keep `execute_mint_submission` under
/// the clippy argument limit.
struct MintSubmissionInput<'a> {
    issuer_request_id: IssuerMintRequestId,
    tokenization_request_id: &'a TokenizationRequestId,
    quantity: &'a Quantity,
    underlying: &'a UnderlyingSymbol,
    wallet: Address,
    journal_confirmed_at: DateTime<Utc>,
    receipt_note: Option<&'a str>,
    external_tx_id: Option<String>,
}

#[derive(Debug, Clone)]
struct KnownMintTx {
    external_tx_id: String,
    fireblocks_tx_id: String,
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

    /// Traverses the `failed_from` chain to find the last state before
    /// any failure. If this mint is not in `MintingFailed`, returns `self`.
    fn non_failed_predecessor(&self) -> &Self {
        match self {
            Self::MintingFailed { failed_from, .. } => {
                failed_from.non_failed_predecessor()
            }
            _ => self,
        }
    }

    fn latest_known_mint_tx(&self) -> Option<KnownMintTx> {
        match self.non_failed_predecessor() {
            Self::FireblocksSubmitted {
                external_tx_id,
                fireblocks_tx_id,
                ..
            } => Some(KnownMintTx {
                external_tx_id: external_tx_id.clone(),
                fireblocks_tx_id: fireblocks_tx_id.clone(),
            }),
            _ => None,
        }
    }

    /// The Fireblocks transaction id currently in flight for this mint (the
    /// latest `FireblocksSubmitted`, whether the aggregate is in that state or
    /// `MintingFailed` with it as the non-failed predecessor). `None` when no
    /// transaction has reached Fireblocks. Used by the scheduled-recovery loop
    /// to give each distinct transaction its own pending-poll budget.
    pub(crate) fn pending_fireblocks_tx_id(&self) -> Option<String> {
        self.latest_known_mint_tx().map(|known| known.fireblocks_tx_id)
    }

    fn base_mint_external_tx_id(
        issuer_request_id: &IssuerMintRequestId,
    ) -> String {
        format!("mint-{issuer_request_id}")
    }

    fn retry_mint_external_tx_id(
        issuer_request_id: &IssuerMintRequestId,
        attempt: u32,
    ) -> String {
        format!(
            "{}{}{}",
            Self::base_mint_external_tx_id(issuer_request_id),
            Self::RETRY_EXTERNAL_TX_MARKER,
            attempt,
        )
    }

    fn retry_attempt_from_external_tx_id(external_tx_id: &str) -> Option<u32> {
        external_tx_id
            .rsplit_once(Self::RETRY_EXTERNAL_TX_MARKER)
            .and_then(|(_, attempt)| attempt.parse().ok())
    }

    /// Attempt number for the *next* retry's `external_tx_id`, derived from the
    /// latest persisted `FireblocksSubmitted` predecessor. Stays unchanged when
    /// a submission fails before Fireblocks accepts it (no new
    /// `FireblocksSubmitted`), so the deterministic id is reused and Fireblocks
    /// dedupes — keeping retries idempotent. The delay/exhaustion schedule uses
    /// the separate `MintingFailed::attempts` counter instead.
    fn next_retry_attempt(&self) -> u32 {
        self.latest_known_mint_tx()
            .and_then(|known| {
                Self::retry_attempt_from_external_tx_id(&known.external_tx_id)
            })
            .unwrap_or(0)
            + 1
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

    /// Submits the on-chain mint transaction to the signing backend.
    /// Requires `Minting` state (set by prior `Deposit` command).
    async fn handle_submit_mint(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Minting {
            issuer_request_id: expected_id,
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        } = self
        else {
            return Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        let event = Self::execute_mint_submission(
            services,
            MintSubmissionInput {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                wallet: *wallet,
                journal_confirmed_at: *journal_confirmed_at,
                receipt_note: None,
                external_tx_id: None,
            },
        )
        .await?;

        Ok(vec![event])
    }

    /// Shared submission logic: vault lookup, receipt info, submit_mint call.
    /// Returns a single `FireblocksSubmitted` or `MintingFailed` event.
    async fn execute_mint_submission(
        services: &MintServices,
        input: MintSubmissionInput<'_>,
    ) -> Result<MintEvent, MintError> {
        let MintSubmissionInput {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            receipt_note,
            external_tx_id,
        } = input;
        let vault = find_vault_by_underlying(&services.pool, underlying)
            .await
            .map_err(|e| MintError::AssetView { message: e.to_string() })?
            .ok_or_else(|| MintError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        let assets = quantity.to_u256_with_18_decimals().map_err(|e| {
            MintError::QuantityConversion { message: e.to_string() }
        })?;

        let receipt_info = ReceiptInformation::new(
            tokenization_request_id.clone(),
            issuer_request_id.clone(),
            underlying.clone(),
            quantity.clone(),
            journal_confirmed_at,
            receipt_note.map(String::from),
        );

        let now = Utc::now();

        match services
            .vault
            .submit_mint(
                vault,
                assets,
                services.bot,
                wallet,
                receipt_info,
                external_tx_id,
            )
            .await
        {
            Ok(submitted) => {
                info!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    external_tx_id = %submitted.external_tx_id,
                    fireblocks_tx_id = %submitted.fireblocks_tx_id,
                    "Mint transaction submitted to signing backend"
                );

                Ok(MintEvent::FireblocksSubmitted {
                    issuer_request_id,
                    external_tx_id: submitted.external_tx_id,
                    fireblocks_tx_id: submitted.fireblocks_tx_id,
                    submitted_at: now,
                })
            }
            Err(err) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Mint submission failed"
                );

                Ok(MintEvent::MintingFailed {
                    issuer_request_id,
                    error: err.to_string(),
                    failed_at: now,
                })
            }
        }
    }

    /// Confirms a previously submitted mint transaction by polling the backend.
    /// Produces `TokensMinted` on success, or `MintingFailed` on failure.
    async fn handle_confirm_mint(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerMintRequestId,
        fireblocks_tx_id: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::FireblocksSubmitted {
            issuer_request_id: expected_id,
            tokenization_request_id,
            quantity,
            underlying,
            journal_confirmed_at,
            fireblocks_tx_id: stored_tx_id,
            ..
        } = self
        else {
            return Err(MintError::NotInFireblocksSubmittedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        if *stored_tx_id != fireblocks_tx_id {
            return Err(MintError::FireblocksTxIdMismatch {
                expected: stored_tx_id.clone(),
                provided: fireblocks_tx_id,
            });
        }

        let now = Utc::now();

        match services.vault.confirm_mint(&fireblocks_tx_id).await {
            Ok(result) => {
                info!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    tx_hash = %result.tx_hash,
                    receipt_id = %result.receipt_id,
                    shares_minted = %result.shares_minted,
                    "On-chain deposit confirmed"
                );

                // Best-effort receipt registration — vault lookup failure
                // must not prevent TokensMinted from being emitted.
                match find_vault_by_underlying(&services.pool, underlying).await
                {
                    Ok(Some(vault)) => {
                        let receipt_info = ReceiptInformation::new(
                            tokenization_request_id.clone(),
                            issuer_request_id.clone(),
                            underlying.clone(),
                            quantity.clone(),
                            *journal_confirmed_at,
                            None,
                        );

                        if let Err(err) = services
                            .receipts
                            .register_minted_receipt(MintedReceiptParams {
                                vault,
                                receipt_id: ReceiptId::from(result.receipt_id),
                                shares: Shares::from(result.shares_minted),
                                block_number: result.block_number,
                                tx_hash: result.tx_hash,
                                receipt_info,
                                receipt_info_bytes: result
                                    .receipt_info_bytes
                                    .clone(),
                            })
                            .await
                        {
                            warn!(
                                target: "mint",
                                issuer_request_id = %issuer_request_id,
                                error = %err,
                                "Failed to register minted receipt \
                                 (monitor/backfill will discover it)"
                            );
                        }
                    }
                    Ok(None) => {
                        warn!(
                            target: "mint",
                            issuer_request_id = %issuer_request_id,
                            underlying = %underlying,
                            "Vault not found for receipt registration \
                             (monitor/backfill will discover it)"
                        );
                    }
                    Err(err) => {
                        warn!(
                            target: "mint",
                            issuer_request_id = %issuer_request_id,
                            error = %err,
                            "Vault lookup failed for receipt registration \
                             (monitor/backfill will discover it)"
                        );
                    }
                }

                Ok(vec![MintEvent::TokensMinted {
                    issuer_request_id,
                    tx_hash: result.tx_hash,
                    receipt_id: result.receipt_id,
                    shares_minted: result.shares_minted,
                    gas_used: result.gas_used,
                    block_number: result.block_number,
                    minted_at: now,
                }])
            }
            Err(err) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "On-chain deposit confirmation failed"
                );

                Ok(vec![MintEvent::MintingFailed {
                    issuer_request_id,
                    error: err.to_string(),
                    failed_at: now,
                }])
            }
        }
    }

    async fn handle_send_callback(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::CallbackPending {
            issuer_request_id: expected_id,
            tokenization_request_id,
            client_id,
            wallet,
            tx_hash,
            network,
            ..
        } = self
        else {
            return Err(MintError::NotInCallbackPendingState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        let callback_request = MintCallbackRequest {
            tokenization_request_id: tokenization_request_id.clone(),
            client_id: *client_id,
            wallet_address: *wallet,
            tx_hash: *tx_hash,
            network: network.clone(),
        };

        services
            .alpaca
            .send_mint_callback(callback_request)
            .await
            .map_err(|e| MintError::Alpaca { message: e.to_string() })?;

        info!(
            target: "mint",
            issuer_request_id = %issuer_request_id,
            "Alpaca callback succeeded"
        );

        Ok(vec![MintEvent::MintCompleted {
            issuer_request_id,
            completed_at: Utc::now(),
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

    async fn handle_recover(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerMintRequestId,
        mode: MintRecoveryMode,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::JournalConfirmed { .. } => {
                // Emit MintingStarted (intent). drive_recovery will call
                // Recover again, which hits the Minting arm to submit.
                self.handle_deposit(issuer_request_id)
            }
            Self::FireblocksSubmitted { fireblocks_tx_id, .. } => {
                // Non-blocking pre-check: a pending tx must pause recovery (so
                // the scheduled loop backs off) instead of blocking in
                // confirm_mint's long poll, which the bounded startup recovery
                // would cancel before any follow-up is scheduled.
                if matches!(
                    services
                        .vault
                        .check_fireblocks_tx(fireblocks_tx_id)
                        .await
                        .map_err(|e| MintError::Vault {
                            message: e.to_string(),
                        })?,
                    Some(FireblocksTxStatus::Pending)
                ) {
                    return Err(MintError::FireblocksTxStillPending {
                        fireblocks_tx_id: fireblocks_tx_id.clone(),
                    });
                }

                let deposit_events = self
                    .handle_confirm_mint(
                        services,
                        issuer_request_id.clone(),
                        fireblocks_tx_id.clone(),
                    )
                    .await?;
                self.advance_through_callback(
                    services,
                    issuer_request_id,
                    deposit_events,
                )
                .await
            }
            Self::MintingFailed { .. } => {
                // Preserve the previous Fireblocks tx so recovery can
                // distinguish pending/completed transactions from terminal
                // failures that are safe to resubmit.
                let known_tx = self.latest_known_mint_tx();

                let deposit_events = self
                    .handle_recover_incomplete(
                        services,
                        issuer_request_id.clone(),
                        None,
                        known_tx,
                        mode,
                    )
                    .await?;
                self.advance_through_callback(
                    services,
                    issuer_request_id,
                    deposit_events,
                )
                .await
            }
            Self::Minting { .. } => {
                let deposit_events = self
                    .handle_recover_incomplete(
                        services,
                        issuer_request_id.clone(),
                        None,
                        None,
                        mode,
                    )
                    .await?;
                self.advance_through_callback(
                    services,
                    issuer_request_id,
                    deposit_events,
                )
                .await
            }
            Self::CallbackPending { .. } => {
                self.handle_send_callback(services, issuer_request_id).await
            }
            _ => Err(MintError::NotRecoverable {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// Handles recovery triggered by receipt discovery. Only accepts
    /// `MintingFailed` when the non-failed predecessor was `Minting`
    /// (meaning a tx was actually submitted).
    ///
    /// Unlike `handle_recover`, this does NOT accept `CallbackPending`
    /// because receipt discovery also fires during the normal mint flow
    /// (when the deposit creates a new on-chain receipt). Handling
    /// `CallbackPending` here would race with the normal flow's
    /// `SendCallback` command, causing duplicate Alpaca callbacks.
    async fn handle_recover_from_receipt(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::MintingFailed { .. }
                if matches!(
                    self.non_failed_predecessor(),
                    Self::Minting { .. } | Self::FireblocksSubmitted { .. }
                ) =>
            {
                let known_tx = self.latest_known_mint_tx();

                let deposit_events = self
                    .handle_recover_incomplete(
                        services,
                        issuer_request_id.clone(),
                        Some(tx_hash),
                        known_tx,
                        MintRecoveryMode::Manual,
                    )
                    .await?;
                self.advance_through_callback(
                    services,
                    issuer_request_id,
                    deposit_events,
                )
                .await
            }
            _ => Err(MintError::NotRecoverable {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// If `deposit_events` contain a successful deposit (`TokensMinted` or
    /// `ExistingMintRecovered`), advance through `CallbackPending` by
    /// sending the Alpaca callback in the same command execution and
    /// return the combined event list. Otherwise return `deposit_events`
    /// unchanged.
    ///
    /// Keeps recovery atomic: a single `Recover` command takes the
    /// aggregate from a stuck pre-callback state straight to `Completed`,
    /// so the aggregate never lingers in `CallbackPending` between
    /// commands and so any recovery entry-point (admin reprocess, the
    /// boot-time loop, or receipt-discovery handler) picks up the same
    /// advancing behavior.
    async fn advance_through_callback(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerMintRequestId,
        deposit_events: Vec<MintEvent>,
    ) -> Result<Vec<MintEvent>, MintError> {
        let deposit_succeeded = deposit_events.iter().any(|event| {
            matches!(
                event,
                MintEvent::TokensMinted { .. }
                    | MintEvent::ExistingMintRecovered { .. }
            )
        });

        if !deposit_succeeded {
            return Ok(deposit_events);
        }

        let mut intermediate = self.clone();
        for event in &deposit_events {
            intermediate.apply_event(event.clone());
        }

        // If callback delivery fails after a successful on-chain deposit,
        // preserve the deposit events so the aggregate advances to
        // CallbackPending. The next recovery picks up from there instead
        // of re-running the deposit path.
        let callback_events = match intermediate
            .handle_send_callback(services, issuer_request_id.clone())
            .await
        {
            Ok(events) => events,
            Err(err) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Callback failed after successful deposit; \
                     keeping mint in CallbackPending"
                );
                return Ok(deposit_events);
            }
        };

        Ok(deposit_events.into_iter().chain(callback_events).collect())
    }

    async fn handle_recover_incomplete(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerMintRequestId,
        receipt_tx_hash: Option<TxHash>,
        known_mint_tx: Option<KnownMintTx>,
        mode: MintRecoveryMode,
    ) -> Result<Vec<MintEvent>, MintError> {
        let (Self::Minting {
            issuer_request_id: expected_id,
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        }
        | Self::MintingFailed {
            issuer_request_id: expected_id,
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        }) = self
        else {
            return Err(MintError::NotInMintingOrMintingFailedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        let vault = find_vault_by_underlying(&services.pool, underlying)
            .await
            .map_err(|e| MintError::AssetView { message: e.to_string() })?
            .ok_or_else(|| MintError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        if let Some(events) = Self::recover_from_existing_receipt(
            services,
            &vault,
            &issuer_request_id,
        )
        .await?
        {
            return Ok(events);
        }

        let receipt_info = ReceiptInformation::new(
            tokenization_request_id.clone(),
            issuer_request_id.clone(),
            underlying.clone(),
            quantity.clone(),
            *journal_confirmed_at,
            Some("Recovery mint".to_string()),
        );

        let now = Utc::now();

        if let Some(known_tx) = known_mint_tx {
            // Known tx_id from a prior submission: go straight to confirm.
            info!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                fireblocks_tx_id = %known_tx.fireblocks_tx_id,
                "Resuming confirmation of previously submitted mint"
            );

            if let Some(status) = services
                .vault
                .check_fireblocks_tx(&known_tx.fireblocks_tx_id)
                .await
                .map_err(|e| MintError::Vault { message: e.to_string() })?
            {
                match status {
                    FireblocksTxStatus::Completed { .. } => {}
                    FireblocksTxStatus::Pending => {
                        return Err(MintError::FireblocksTxStillPending {
                            fireblocks_tx_id: known_tx.fireblocks_tx_id,
                        });
                    }
                    FireblocksTxStatus::Failed {
                        detail, sub_status, ..
                    } => {
                        warn!(
                            target: "mint",
                            issuer_request_id = %issuer_request_id,
                            fireblocks_tx_id = %known_tx.fireblocks_tx_id,
                            external_tx_id = %known_tx.external_tx_id,
                            sub_status = sub_status.as_deref().unwrap_or(""),
                            detail = %detail,
                            "Previous Fireblocks mint transaction failed; \
                             submitting retry"
                        );

                        return self
                            .submit_recovery_mint(
                                services,
                                MintSubmissionInput {
                                    issuer_request_id,
                                    tokenization_request_id,
                                    quantity,
                                    underlying,
                                    wallet: *wallet,
                                    journal_confirmed_at: *journal_confirmed_at,
                                    receipt_note: Some("Recovery mint"),
                                    external_tx_id: None,
                                },
                                receipt_tx_hash,
                                mode,
                            )
                            .await;
                    }
                }
            }

            return match services
                .vault
                .confirm_mint(&known_tx.fireblocks_tx_id)
                .await
            {
                Ok(result) => {
                    info!(
                        target: "mint",
                        issuer_request_id = %issuer_request_id,
                        tx_hash = %result.tx_hash,
                        "Recovery mint succeeded"
                    );

                    // Best-effort receipt registration
                    if let Err(err) = services
                        .receipts
                        .register_minted_receipt(MintedReceiptParams {
                            vault,
                            receipt_id: ReceiptId::from(result.receipt_id),
                            shares: Shares::from(result.shares_minted),
                            block_number: result.block_number,
                            tx_hash: result.tx_hash,
                            receipt_info: receipt_info.clone(),
                            receipt_info_bytes: result
                                .receipt_info_bytes
                                .clone(),
                        })
                        .await
                    {
                        warn!(
                            target: "mint",
                            issuer_request_id = %issuer_request_id,
                            error = %err,
                            "Failed to register recovered receipt \
                             (monitor/backfill will discover it)"
                        );
                    }

                    let retry_event =
                        matches!(self, Self::MintingFailed { .. }).then(|| {
                            MintEvent::MintRetryStarted {
                                issuer_request_id: issuer_request_id.clone(),
                                tx_hash: receipt_tx_hash,
                                started_at: now,
                            }
                        });

                    let minted = MintEvent::TokensMinted {
                        issuer_request_id,
                        tx_hash: result.tx_hash,
                        receipt_id: result.receipt_id,
                        shares_minted: result.shares_minted,
                        gas_used: result.gas_used,
                        block_number: result.block_number,
                        minted_at: now,
                    };

                    Ok(retry_event
                        .into_iter()
                        .chain(std::iter::once(minted))
                        .collect())
                }
                Err(err) => {
                    warn!(
                        target: "mint",
                        issuer_request_id = %issuer_request_id,
                        error = %err,
                        "Recovery mint confirmation failed — \
                         preserving fireblocks_tx_id for next retry"
                    );

                    // Do NOT emit MintRetryStarted here — that would
                    // move the aggregate from MintingFailed to Minting,
                    // losing the FireblocksSubmitted predecessor and its
                    // fireblocks_tx_id. Instead, re-emit MintingFailed
                    // which keeps the existing predecessor chain intact.
                    Ok(vec![MintEvent::MintingFailed {
                        issuer_request_id,
                        error: err.to_string(),
                        failed_at: now,
                    }])
                }
            };
        }

        // No prior tx_id: submit and persist FireblocksSubmitted.
        // Don't confirm here — let the next recovery pass handle
        // ConfirmMint from the FireblocksSubmitted state.
        self.submit_recovery_mint(
            services,
            MintSubmissionInput {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                wallet: *wallet,
                journal_confirmed_at: *journal_confirmed_at,
                receipt_note: Some("Recovery mint"),
                external_tx_id: None,
            },
            receipt_tx_hash,
            mode,
        )
        .await
    }

    async fn submit_recovery_mint(
        &self,
        services: &MintServices,
        mut input: MintSubmissionInput<'_>,
        receipt_tx_hash: Option<TxHash>,
        mode: MintRecoveryMode,
    ) -> Result<Vec<MintEvent>, MintError> {
        let retrying_failed_mint = matches!(self, Self::MintingFailed { .. });
        // The delay/cap gate uses the failure-count-based schedule
        // (`automatic_retry_decision`); the external_tx_id uses the
        // FireblocksSubmitted-derived attempt so it is reused unchanged across
        // submission failures (Fireblocks dedupes — keeps retries idempotent).
        if retrying_failed_mint && matches!(mode, MintRecoveryMode::Automatic) {
            let now = Utc::now();
            match self.automatic_retry_decision(now) {
                AutomaticRetryDecision::Exhausted => {
                    return Err(MintError::AutomaticRetriesExhausted {
                        attempts: Self::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT,
                    });
                }
                AutomaticRetryDecision::Wait(wait) => {
                    let retry_at = now
                        + ChronoDuration::from_std(wait)
                            .map_err(|_| MintError::RetryDelayOutOfRange)?;
                    return Err(MintError::RetryNotDue { retry_at });
                }
                AutomaticRetryDecision::Ready
                | AutomaticRetryDecision::NotRecoverable => {}
            }
        }

        let now = Utc::now();
        let external_attempt = self.next_retry_attempt();
        let issuer_request_id = input.issuer_request_id.clone();
        let external_tx_id = retrying_failed_mint.then(|| {
            Self::retry_mint_external_tx_id(
                &issuer_request_id,
                external_attempt,
            )
        });
        input.external_tx_id = external_tx_id;

        let submission_event =
            Self::execute_mint_submission(services, input).await?;

        // Only record MintRetryStarted alongside a *successful* submission.
        // execute_mint_submission returns Ok(MintingFailed) when the backend
        // rejects the submission; emitting MintRetryStarted in that case would
        // move the aggregate MintingFailed -> Minting, discarding the
        // FireblocksSubmitted predecessor (its fireblocks_tx_id and the retry
        // counter derived from external_tx_id). Re-emitting only the failure
        // keeps the predecessor chain intact for the next retry.
        let retry_event = matches!(
            (retrying_failed_mint, &submission_event),
            (true, MintEvent::FireblocksSubmitted { .. })
        )
        .then(|| MintEvent::MintRetryStarted {
            issuer_request_id,
            tx_hash: receipt_tx_hash,
            started_at: now,
        });

        Ok(retry_event
            .into_iter()
            .chain(std::iter::once(submission_event))
            .collect())
    }

    async fn recover_from_existing_receipt(
        services: &MintServices,
        vault: &Address,
        issuer_request_id: &IssuerMintRequestId,
    ) -> Result<Option<Vec<MintEvent>>, MintError> {
        let Some(receipt) = services
            .receipts
            .find_by_issuer_request_id(vault, issuer_request_id)
            .await
            .map_err(|e| MintError::ReceiptLookup { message: e.to_string() })?
        else {
            debug!(target: "mint", issuer_request_id = %issuer_request_id, "No receipt found, retrying mint");
            return Ok(None);
        };

        info!(
            target: "mint",
            issuer_request_id = %issuer_request_id,
            receipt_id = %receipt.receipt_id,
            "Found existing receipt, recording recovery"
        );

        Ok(Some(vec![MintEvent::ExistingMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: receipt.tx_hash,
            receipt_id: receipt.receipt_id,
            shares_minted: receipt.shares,
            block_number: receipt.block_number,
            recovered_at: Utc::now(),
        }]))
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
    type Services = MintServices;
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
            MintCommand::SubmitMint { .. }
            | MintCommand::RecordFireblocksSubmitted { .. } => {
                Err(MintError::NotInMintingState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::ConfirmMint { .. }
            | MintCommand::RecordTokensMinted { .. } => {
                Err(MintError::NotInFireblocksSubmittedState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::SendCallback { .. }
            | MintCommand::RecordCallbackSent { .. } => {
                Err(MintError::NotInCallbackPendingState {
                    current_state: "Uninitialized".to_string(),
                })
            }
            MintCommand::RecordMintFailed { .. }
            | MintCommand::RetryMint { .. } => Ok(vec![]),
            MintCommand::Recover { .. }
            | MintCommand::RecoverFromReceipt { .. }
            | MintCommand::CloseMint { .. } => Err(MintError::NotRecoverable {
                current_state: "Uninitialized".to_string(),
            }),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
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
            MintCommand::SubmitMint { issuer_request_id } => {
                self.handle_submit_mint(services, issuer_request_id).await
            }
            MintCommand::ConfirmMint {
                issuer_request_id,
                fireblocks_tx_id,
            } => {
                self.handle_confirm_mint(
                    services,
                    issuer_request_id,
                    fireblocks_tx_id,
                )
                .await
            }
            MintCommand::SendCallback { issuer_request_id } => {
                self.handle_send_callback(services, issuer_request_id).await
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
            MintCommand::Recover { issuer_request_id, mode } => {
                self.handle_recover(services, issuer_request_id, mode).await
            }
            MintCommand::RecoverFromReceipt { issuer_request_id, tx_hash } => {
                self.handle_recover_from_receipt(
                    services,
                    issuer_request_id,
                    tx_hash,
                )
                .await
            }
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
    use alloy::primitives::{Address, B256, U256, address, b256, uint};
    use async_trait::async_trait;
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use cqrs_es::DomainEvent;
    use event_sorcery::{
        LifecycleError, Store, StoreBuilder, TestHarness, replay, test_store,
    };
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;
    use uuid::{Uuid, uuid};

    use super::{
        AutomaticRetryDecision, ClientId, IssuerMintRequestId, Mint,
        MintCommand, MintError, MintEvent, MintRecoveryMode, MintServices,
        MintView, Network, Quantity, TokenSymbol, TokenizationRequestId,
        UnderlyingSymbol, find_by_issuer_request_id,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::prepare_event_sourced_startup;
    use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
    use crate::test_utils::{log_count_at, logs_contain_at};
    use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        BurnVerification, FireblocksTxStatus, MintResult, MultiBurnParams,
        MultiBurnResult, ReceiptInformation, SubmittedTx, VaultError,
        VaultService,
    };

    pub(super) const VAULT: Address =
        address!("0xcccccccccccccccccccccccccccccccccccccccc");

    pub(super) const BOT: Address =
        address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    struct TerminalFailedMintVault {
        submitted_external_tx_id: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl TerminalFailedMintVault {
        fn new() -> Self {
            Self {
                submitted_external_tx_id: Arc::new(std::sync::Mutex::new(None)),
            }
        }

        fn submitted_external_tx_id(&self) -> Option<String> {
            self.submitted_external_tx_id.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl VaultService for TerminalFailedMintVault {
        async fn submit_mint(
            &self,
            _vault: Address,
            _assets: U256,
            _bot: Address,
            _user: Address,
            _receipt_info: ReceiptInformation,
            external_tx_id: Option<String>,
        ) -> Result<SubmittedTx, VaultError> {
            let external_tx_id =
                external_tx_id.unwrap_or_else(|| "mint-base".to_string());
            *self.submitted_external_tx_id.lock().unwrap() =
                Some(external_tx_id.clone());

            Ok(SubmittedTx {
                external_tx_id,
                fireblocks_tx_id: "fb-retry".to_string(),
            })
        }

        async fn confirm_mint(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<MintResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn get_share_balance(
            &self,
            _vault: Address,
            _owner: Address,
        ) -> Result<U256, VaultError> {
            Ok(U256::ZERO)
        }

        async fn check_fireblocks_tx(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<Option<FireblocksTxStatus>, VaultError> {
            Ok(Some(FireblocksTxStatus::Failed {
                detail: "Failed".to_string(),
                sub_status: Some("INSUFFICIENT_FUNDS_FOR_FEE".to_string()),
                network_tx_hashes: vec![],
            }))
        }

        async fn submit_burn(
            &self,
            _params: MultiBurnParams,
        ) -> Result<SubmittedTx, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn confirm_burn(
            &self,
            _fireblocks_tx_id: &str,
            _expected_dust_shares: U256,
        ) -> Result<MultiBurnResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn verify_burn_tx(
            &self,
            _vault: Address,
            _owner: Address,
            _tx_hash: B256,
        ) -> Result<BurnVerification, VaultError> {
            Err(VaultError::InvalidReceipt)
        }
    }

    /// Vault whose prior Fireblocks tx is terminally failed and whose retry
    /// submission also fails. Exercises the failed-resubmission path.
    struct RetrySubmitFailsVault;

    #[async_trait]
    impl VaultService for RetrySubmitFailsVault {
        async fn submit_mint(
            &self,
            _vault: Address,
            _assets: U256,
            _bot: Address,
            _user: Address,
            _receipt_info: ReceiptInformation,
            _external_tx_id: Option<String>,
        ) -> Result<SubmittedTx, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn confirm_mint(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<MintResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn get_share_balance(
            &self,
            _vault: Address,
            _owner: Address,
        ) -> Result<U256, VaultError> {
            Ok(U256::ZERO)
        }

        async fn check_fireblocks_tx(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<Option<FireblocksTxStatus>, VaultError> {
            Ok(Some(FireblocksTxStatus::Failed {
                detail: "Failed".to_string(),
                sub_status: Some("INSUFFICIENT_FUNDS_FOR_FEE".to_string()),
                network_tx_hashes: vec![],
            }))
        }

        async fn submit_burn(
            &self,
            _params: MultiBurnParams,
        ) -> Result<SubmittedTx, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn confirm_burn(
            &self,
            _fireblocks_tx_id: &str,
            _expected_dust_shares: U256,
        ) -> Result<MultiBurnResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn verify_burn_tx(
            &self,
            _vault: Address,
            _owner: Address,
            _tx_hash: B256,
        ) -> Result<BurnVerification, VaultError> {
            Err(VaultError::InvalidReceipt)
        }
    }

    struct MintTestFixture {
        mint_store: Arc<Store<Mint>>,
        pool: Pool<Sqlite>,
    }

    impl MintTestFixture {
        async fn new_with_vault(vault: Arc<dyn VaultService>) -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .unwrap();

            sqlx::migrate!().run(&pool).await.unwrap();

            let (asset_store, _asset_projection) =
                StoreBuilder::<TokenizedAsset>::new(pool.clone())
                    .build(())
                    .await
                    .unwrap();

            let underlying = UnderlyingSymbol::new("AAPL");
            asset_store
                .send(
                    &underlying,
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new("tAAPL"),
                        network: Network::Base,
                        vault: VAULT,
                    },
                )
                .await
                .unwrap();

            let receipt_store =
                Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

            let services = MintServices {
                vault,
                alpaca: Arc::new(MockAlpacaService::new_success()),
                receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
                pool: pool.clone(),
                bot: BOT,
            };

            let mint_store =
                Arc::new(test_store::<Mint>(pool.clone(), services));

            Self { mint_store, pool }
        }

        /// Seeds a pre-existing `MintEvent` history directly into the event
        /// store so command tests can start from any lifecycle state.
        ///
        /// Inserts raw `MintEvent` rows — `Lifecycle<Mint>` persists the inner
        /// `MintEvent` verbatim (`event_type = "MintEvent::<Variant>"`, payload
        /// `{"<Variant>": {...}}`), so `mint_store.load`/`send` replay them
        /// exactly as if they had been produced by commands.
        async fn seed_mint_events(
            &self,
            aggregate_id: &str,
            events: Vec<MintEvent>,
        ) {
            for (index, event) in events.into_iter().enumerate() {
                let sequence = i64::try_from(index).unwrap() + 1;
                let event_type = event.event_type();
                let payload = serde_json::to_string(&event).unwrap();

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
                    VALUES ('Mint', ?, ?, ?, '1.0', ?, '{}')
                    ",
                )
                .bind(aggregate_id)
                .bind(sequence)
                .bind(event_type)
                .bind(payload)
                .execute(&self.pool)
                .await
                .unwrap();
            }
        }
    }

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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

    /// Counts persisted `Mint` events of a given `event_type` for one mint,
    /// reading the event store directly (recovery's audit-trail assertions
    /// inspect emitted events, which `Store::load` collapses into state).
    async fn count_events_of_type(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        event_type: &str,
    ) -> i64 {
        sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Mint'
              AND aggregate_id = ?
              AND event_type = ?
            ",
        )
        .bind(issuer_request_id.to_string())
        .bind(event_type)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn test_mint_services() -> MintServices {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");
        sqlx::migrate!().run(&pool).await.expect("Failed to run migrations");

        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
            pool,
            bot,
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let error = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let error = TestHarness::<Mint>::with(test_mint_services().await)
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

        let error = TestHarness::<Mint>::with(test_mint_services().await)
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

        let events = TestHarness::<Mint>::with(test_mint_services().await)
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

        let error = TestHarness::<Mint>::with(test_mint_services().await)
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

        let error = TestHarness::<Mint>::with(test_mint_services().await)
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

        let error = TestHarness::<Mint>::with(test_mint_services().await)
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
    fn test_apply_minting_failed_event_updates_state() {
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

        let error_message = "Transaction failed: insufficient gas";
        let failed_at = Utc::now();

        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: error_message.to_string(),
            failed_at,
        });

        let Mint::MintingFailed {
            issuer_request_id: state_issuer_id,
            error: state_error,
            failed_at: state_failed_at,
            failed_from,
            ..
        } = &mint
        else {
            panic!("Expected MintingFailed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, &issuer_request_id);
        assert_eq!(state_error, error_message);
        assert_eq!(state_failed_at, &failed_at);
        assert!(
            matches!(**failed_from, Mint::Minting { .. }),
            "Expected failed_from to be Minting, got {:?}",
            failed_from.state_name()
        );
        assert!(
            matches!(mint.non_failed_predecessor(), Mint::Minting { .. }),
            "Expected non_failed_predecessor to be Minting"
        );
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

    struct TestMintData {
        issuer_request_id: super::IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
    }

    impl TestMintData {
        fn new() -> Self {
            Self {
                issuer_request_id: IssuerMintRequestId::random(),
                tokenization_request_id: TokenizationRequestId::new(
                    "alp-integration-456",
                ),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::Base,
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            }
        }
    }

    /// Builds a projected `Store<Mint>` (with the `mint_view` projection wired)
    /// plus its pool, seeded with the AAPL tokenized asset, so the split mint
    /// flow can be driven end to end and the resulting `MintView` inspected via
    /// the view query helpers.
    async fn setup_projected_mint_store() -> (Arc<Store<Mint>>, Pool<Sqlite>) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

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

        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

        let services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
            pool: pool.clone(),
            bot: BOT,
        };

        let (mint_store, _mint_projection) =
            StoreBuilder::<Mint>::new(pool.clone())
                .build(services)
                .await
                .unwrap();

        (mint_store, pool)
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

        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

        let services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
            pool: pool.clone(),
            bot: BOT,
        };

        prepare_event_sourced_startup::<Mint>(&pool).await.unwrap();
        StoreBuilder::<Mint>::new(pool.clone()).build(services).await.unwrap();

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

        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

        let services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
            pool: pool.clone(),
            bot: BOT,
        };

        prepare_event_sourced_startup::<Mint>(&pool).await.unwrap();
        StoreBuilder::<Mint>::new(pool.clone()).build(services).await.unwrap();

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

    #[tokio::test]
    async fn test_complete_mint_flow_via_cqrs() {
        let (store, pool) = setup_projected_mint_store().await;
        let data = TestMintData::new();

        store
            .send(
                &data.issuer_request_id,
                MintCommand::Initiate {
                    issuer_request_id: data.issuer_request_id.clone(),
                    tokenization_request_id: data
                        .tokenization_request_id
                        .clone(),
                    quantity: data.quantity.clone(),
                    underlying: data.underlying.clone(),
                    token: data.token.clone(),
                    network: data.network.clone(),
                    client_id: data.client_id,
                    wallet: data.wallet,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &data.issuer_request_id,
                MintCommand::ConfirmJournal {
                    issuer_request_id: data.issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        // Deposit records intent (emits MintingStarted)
        store
            .send(
                &data.issuer_request_id,
                MintCommand::Deposit {
                    issuer_request_id: data.issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        // SubmitMint does the network call (emits FireblocksSubmitted)
        store
            .send(
                &data.issuer_request_id,
                MintCommand::SubmitMint {
                    issuer_request_id: data.issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        // Load the fireblocks_tx_id from the aggregate state
        let Some(Mint::FireblocksSubmitted { fireblocks_tx_id, .. }) =
            store.load(&data.issuer_request_id).await.unwrap()
        else {
            panic!("Expected FireblocksSubmitted state after SubmitMint");
        };

        // ConfirmMint polls the backend (emits TokensMinted)
        store
            .send(
                &data.issuer_request_id,
                MintCommand::ConfirmMint {
                    issuer_request_id: data.issuer_request_id.clone(),
                    fireblocks_tx_id,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &data.issuer_request_id,
                MintCommand::SendCallback {
                    issuer_request_id: data.issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        assert!(
            matches!(
                store.load(&data.issuer_request_id).await.unwrap(),
                Some(Mint::Completed { .. })
            ),
            "Expected Completed state"
        );

        // The split flow must emit exactly these six events in order.
        let event_types: Vec<String> = sqlx::query_scalar(
            "
            SELECT event_type
            FROM events
            WHERE aggregate_type = 'Mint' AND aggregate_id = ?
            ORDER BY sequence
            ",
        )
        .bind(data.issuer_request_id.to_string())
        .fetch_all(&pool)
        .await
        .unwrap();

        let event_types: Vec<&str> =
            event_types.iter().map(String::as_str).collect();
        assert_eq!(
            event_types,
            vec![
                "MintEvent::Initiated",
                "MintEvent::JournalConfirmed",
                "MintEvent::MintingStarted",
                "MintEvent::FireblocksSubmitted",
                "MintEvent::TokensMinted",
                "MintEvent::MintCompleted",
            ],
            "Expected 6 events in the split flow, in order"
        );

        let view = find_by_issuer_request_id(&pool, &data.issuer_request_id)
            .await
            .unwrap()
            .expect("MintView should exist after the flow completes");

        let MintView::Completed {
            issuer_request_id: view_issuer_id,
            tx_hash: view_tx_hash,
            completed_at: view_completed_at,
            initiated_at: view_initiated_at,
            journal_confirmed_at: view_journal_confirmed_at,
            minted_at: view_minted_at,
            ..
        } = view
        else {
            panic!("Expected Completed view, got {view:?}");
        };

        assert_eq!(view_issuer_id, data.issuer_request_id);
        assert!(view_tx_hash != B256::ZERO);
        assert!(view_initiated_at.timestamp() > 0);
        assert!(view_journal_confirmed_at.timestamp() > 0);
        assert!(view_minted_at.timestamp() > 0);
        assert!(view_completed_at.timestamp() > 0);
        assert!(view_completed_at >= view_minted_at);
        assert!(view_minted_at >= view_journal_confirmed_at);
        assert!(view_journal_confirmed_at >= view_initiated_at);
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

    #[traced_test]
    #[tokio::test]
    async fn automatic_recovery_retries_terminal_failed_fireblocks_mint_after_delay()
     {
        let issuer_request_id = IssuerMintRequestId::random();
        let failed_at = Utc::now() - ChronoDuration::minutes(2);
        let events = minting_events_for_retry(
            &issuer_request_id,
            format!("mint-{issuer_request_id}"),
            failed_at,
        );
        let vault = Arc::new(TerminalFailedMintVault::new());
        let fixture = MintTestFixture::new_with_vault(vault.clone()).await;
        fixture.seed_mint_events(&issuer_request_id.to_string(), events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            vault.submitted_external_tx_id(),
            Some(format!("mint-{issuer_request_id}-retry-1"))
        );

        let Some(Mint::FireblocksSubmitted { external_tx_id, .. }) =
            fixture.mint_store.load(&issuer_request_id).await.unwrap()
        else {
            panic!("Expected FireblocksSubmitted after retry");
        };
        assert_eq!(external_tx_id, format!("mint-{issuer_request_id}-retry-1"));

        // A successful retry must record MintRetryStarted as the permanent
        // audit-trail fact that recovery resubmitted the mint.
        let retry_started = count_events_of_type(
            &fixture.pool,
            &issuer_request_id,
            "MintEvent::MintRetryStarted",
        )
        .await;
        assert_eq!(
            retry_started, 1,
            "Expected exactly one MintRetryStarted event after a retry"
        );

        // The retry-submission warning is the primary operational signal that
        // recovery resubmitted after a terminal Fireblocks failure.
        let test = "automatic_recovery_retries_terminal_failed_fireblocks_mint_after_delay";
        assert_eq!(
            log_count_at!(Level::WARN, &[test, "submitting retry"]),
            1,
            "Expected the retry-submission warning to be emitted once"
        );
    }

    #[tokio::test]
    async fn automatic_recovery_waits_until_retry_delay_elapses() {
        let issuer_request_id = IssuerMintRequestId::random();
        let events = minting_events_for_retry(
            &issuer_request_id,
            format!("mint-{issuer_request_id}"),
            Utc::now(),
        );
        let vault = Arc::new(TerminalFailedMintVault::new());
        let fixture = MintTestFixture::new_with_vault(vault.clone()).await;
        fixture.seed_mint_events(&issuer_request_id.to_string(), events).await;

        let result = fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await;

        assert!(
            matches!(
                result,
                Err(cqrs_es::AggregateError::UserError(LifecycleError::Apply(
                    MintError::RetryNotDue { .. }
                )))
            ),
            "Expected RetryNotDue, got {result:?}"
        );
        assert_eq!(vault.submitted_external_tx_id(), None);
    }

    #[traced_test]
    #[tokio::test]
    async fn manual_reprocess_can_bypass_automatic_retry_cap() {
        let issuer_request_id = IssuerMintRequestId::random();
        let failed_at = Utc::now() - ChronoDuration::hours(2);
        let events = minting_events_for_retry(
            &issuer_request_id,
            format!("mint-{issuer_request_id}-retry-4"),
            failed_at,
        );
        let vault = Arc::new(TerminalFailedMintVault::new());
        let fixture = MintTestFixture::new_with_vault(vault.clone()).await;
        fixture.seed_mint_events(&issuer_request_id.to_string(), events).await;

        let automatic_result = fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await;
        assert!(
            matches!(
                automatic_result,
                Err(cqrs_es::AggregateError::UserError(LifecycleError::Apply(
                    MintError::AutomaticRetriesExhausted { attempts: 4 }
                )))
            ),
            "Expected AutomaticRetriesExhausted, got {automatic_result:?}"
        );

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Manual,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            vault.submitted_external_tx_id(),
            Some(format!("mint-{issuer_request_id}-retry-5"))
        );

        let mint = fixture
            .mint_store
            .load(&issuer_request_id)
            .await
            .unwrap()
            .expect("Expected a live mint after manual retry");
        let Mint::FireblocksSubmitted { external_tx_id, .. } = &mint else {
            panic!(
                "Expected FireblocksSubmitted after manual retry, got: {}",
                mint.state_name()
            );
        };
        assert_eq!(
            external_tx_id,
            &format!("mint-{issuer_request_id}-retry-5")
        );

        // The manual bypass submits the next retry transaction; the automatic
        // attempt before it was rejected by the cap and never submitted.
        let test = "manual_reprocess_can_bypass_automatic_retry_cap";
        let expected_external = format!("mint-{issuer_request_id}-retry-5");
        assert_eq!(
            log_count_at!(
                Level::INFO,
                &[
                    test,
                    "Mint transaction submitted to signing backend",
                    expected_external.as_str(),
                ]
            ),
            1,
            "Expected the manual retry-5 submission to be logged once"
        );
    }

    /// A retry whose *submission* fails (vs. confirmation) must not advance the
    /// aggregate past MintingFailed: emitting MintRetryStarted there would drop
    /// the FireblocksSubmitted predecessor and reset the retry counter to 1,
    /// reusing a spent external_tx_id and bypassing the automatic cap.
    #[tokio::test]
    async fn failed_retry_submission_preserves_predecessor_and_counter() {
        let issuer_request_id = IssuerMintRequestId::random();
        // Predecessor was retry attempt 1; failed 2h ago so attempt 2 is due.
        let original_failed_at = Utc::now() - ChronoDuration::hours(2);
        let events = minting_events_for_retry(
            &issuer_request_id,
            format!("mint-{issuer_request_id}-retry-1"),
            original_failed_at,
        );
        let fixture =
            MintTestFixture::new_with_vault(Arc::new(RetrySubmitFailsVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id.to_string(), events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mint = fixture
            .mint_store
            .load(&issuer_request_id)
            .await
            .unwrap()
            .expect("Expected a live mint after failed resubmission");

        let Mint::MintingFailed { failed_at, failed_from, .. } = &mint else {
            panic!(
                "Expected MintingFailed after failed resubmission, got: {}",
                mint.state_name()
            );
        };
        assert!(
            matches!(failed_from.as_ref(), Mint::FireblocksSubmitted { .. }),
            "Predecessor chain must be preserved on submission failure"
        );
        assert_eq!(
            mint.next_retry_attempt(),
            2,
            "Retry counter must not reset after a failed retry submission"
        );
        assert!(
            *failed_at > original_failed_at,
            "failed_at must advance so the backoff anchor moves forward"
        );

        let retry_started = count_events_of_type(
            &fixture.pool,
            &issuer_request_id,
            "MintEvent::MintRetryStarted",
        )
        .await;
        assert_eq!(
            retry_started, 0,
            "A failed submission must not emit MintRetryStarted"
        );
    }

    /// Manual recovery bypasses the not-yet-elapsed retry window, not just the
    /// attempt cap: an operator who has fixed the underlying issue can submit
    /// the next retry immediately instead of waiting for the automatic delay.
    #[tokio::test]
    async fn manual_reprocess_bypasses_retry_delay() {
        let issuer_request_id = IssuerMintRequestId::random();
        // Failed just now: automatic recovery would return RetryNotDue.
        let events = minting_events_for_retry(
            &issuer_request_id,
            format!("mint-{issuer_request_id}"),
            Utc::now(),
        );
        let vault = Arc::new(TerminalFailedMintVault::new());
        let fixture = MintTestFixture::new_with_vault(vault.clone()).await;
        fixture.seed_mint_events(&issuer_request_id.to_string(), events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Manual,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            vault.submitted_external_tx_id(),
            Some(format!("mint-{issuer_request_id}-retry-1")),
            "Manual recovery must submit immediately despite the unelapsed \
             retry window"
        );
    }

    /// Submission failures that never reach Fireblocks (no FireblocksSubmitted
    /// predecessor) must still escalate the retry schedule and eventually
    /// exhaust, while the external_tx_id stays at retry-1 so resubmission is
    /// idempotent (Fireblocks dedupes a spent id rather than double-minting).
    #[test]
    fn pre_acceptance_failures_escalate_attempts_but_reuse_external_id() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();
        let mut mint = replay::<Mint>(vec![
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
        ])
        .unwrap()
        .unwrap();

        // failed_at far in the past so every retry window has elapsed; the
        // decision then turns only on the escalating attempt counter.
        let failed_at = now - ChronoDuration::hours(3);

        // First submission failure from Minting: attempts = 1, retryable.
        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "submission rejected".to_string(),
            failed_at,
        });
        assert_eq!(
            mint.automatic_retry_decision(Utc::now()),
            AutomaticRetryDecision::Ready
        );

        // Three more pre-acceptance failures push attempts to 4 (still the last
        // retryable attempt), then a fifth exhausts the automatic schedule —
        // proving the counter escalates without a FireblocksSubmitted record.
        for _ in 0..3 {
            mint.apply_event(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
            });
        }
        assert_eq!(
            mint.automatic_retry_decision(Utc::now()),
            AutomaticRetryDecision::Ready
        );

        mint.apply_event(MintEvent::MintingFailed {
            issuer_request_id,
            error: "submission rejected".to_string(),
            failed_at,
        });
        assert_eq!(
            mint.automatic_retry_decision(Utc::now()),
            AutomaticRetryDecision::Exhausted
        );

        // The external_tx_id attempt never advanced — retries reuse retry-1.
        assert_eq!(mint.next_retry_attempt(), 1);
    }

    #[tokio::test]
    async fn test_recover_from_receipt_rejects_journal_confirmed() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();

        let error = TestHarness::<Mint>::with(test_mint_services().await)
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-123",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    client_id: ClientId::new(),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                    initiated_at: now,
                },
                MintEvent::JournalConfirmed {
                    issuer_request_id: issuer_request_id.clone(),
                    confirmed_at: now,
                },
            ])
            .when(MintCommand::RecoverFromReceipt {
                issuer_request_id,
                tx_hash: b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::NotRecoverable { .. })
            ),
            "RecoverFromReceipt should reject JournalConfirmed state"
        );
    }

    #[tokio::test]
    async fn test_recover_from_receipt_rejects_minting() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();

        let error = TestHarness::<Mint>::with(test_mint_services().await)
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-123",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Base,
                    client_id: ClientId::new(),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
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
            ])
            .when(MintCommand::RecoverFromReceipt {
                issuer_request_id,
                tx_hash: b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(MintError::NotRecoverable { .. })
            ),
            "RecoverFromReceipt should reject Minting state"
        );
    }

    #[tokio::test]
    async fn test_recover_from_receipt_rejects_minting_failed_with_non_minting_predecessor()
     {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();

        let journal_confirmed = Mint::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("tok-123"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: now,
            journal_confirmed_at: now,
        };

        let mint = Mint::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("tok-123"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: now,
            journal_confirmed_at: now,
            error: "some error".to_string(),
            failed_at: now,
            attempts: 1,
            failed_from: Box::new(journal_confirmed),
        };

        assert!(
            matches!(
                mint.non_failed_predecessor(),
                Mint::JournalConfirmed { .. }
            ),
            "Precondition: non_failed_predecessor should be JournalConfirmed"
        );

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

        let services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
            pool,
            bot: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        };

        let result = mint
            .handle_recover_from_receipt(
                &services,
                issuer_request_id,
                b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .await;

        assert!(
            matches!(result, Err(MintError::NotRecoverable { .. })),
            "RecoverFromReceipt should reject MintingFailed with non-Minting predecessor, got: {result:?}"
        );
    }
}
