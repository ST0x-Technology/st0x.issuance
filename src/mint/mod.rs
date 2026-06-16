mod api;
mod cmd;
mod event;
pub(crate) mod recovery;
mod view;

use alloy::primitives::{Address, B256, TxHash, U256};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use cqrs_es::Aggregate;
use cqrs_es::event_sink::EventSink;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::account::view::AccountViewError;
use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};
use crate::receipt_inventory::{
    MintedReceiptParams, ReceiptId, ReceiptLookupError, ReceiptService, Shares,
    view::ReceiptInventoryViewError,
};
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, find_vault_by_underlying,
};
use crate::vault::{
    FireblocksTxStatus, ReceiptInformation, VaultError, VaultService,
};

pub use api::MintResponse;

pub(crate) use api::{confirm_journal, initiate_mint};
pub(crate) use cmd::{MintCommand, MintRecoveryMode};
pub(crate) use event::MintEvent;
pub(crate) use view::{
    MintView, find_all_recoverable_mints, find_by_issuer_request_id,
    find_stuck, replay_mint_view,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) enum Mint {
    #[default]
    Uninitialized,
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
struct MintSubmissionParams<'a> {
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

struct IncompleteMintRecovery {
    tokenization_request_id: TokenizationRequestId,
    quantity: Quantity,
    underlying: UnderlyingSymbol,
    wallet: Address,
    journal_confirmed_at: DateTime<Utc>,
    is_minting_failed: bool,
}

struct KnownTxRecovery {
    issuer_request_id: IssuerMintRequestId,
    recovery: IncompleteMintRecovery,
    vault: Address,
    receipt_info: ReceiptInformation,
    receipt_tx_hash: Option<TxHash>,
    known_tx: KnownMintTx,
    mode: MintRecoveryMode,
}

impl IncompleteMintRecovery {
    fn from_mint(
        mint: &Mint,
    ) -> Result<(IssuerMintRequestId, Self), MintError> {
        match mint {
            Mint::Minting {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                wallet,
                journal_confirmed_at,
                ..
            } => Ok((
                issuer_request_id.clone(),
                Self {
                    tokenization_request_id: tokenization_request_id.clone(),
                    quantity: quantity.clone(),
                    underlying: underlying.clone(),
                    wallet: *wallet,
                    journal_confirmed_at: *journal_confirmed_at,
                    is_minting_failed: false,
                },
            )),
            Mint::MintingFailed {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                wallet,
                journal_confirmed_at,
                ..
            } => Ok((
                issuer_request_id.clone(),
                Self {
                    tokenization_request_id: tokenization_request_id.clone(),
                    quantity: quantity.clone(),
                    underlying: underlying.clone(),
                    wallet: *wallet,
                    journal_confirmed_at: *journal_confirmed_at,
                    is_minting_failed: true,
                },
            )),
            _ => Err(MintError::NotInMintingOrMintingFailedState {
                current_state: mint.state_name().to_string(),
            }),
        }
    }
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
            Self::Uninitialized => "Uninitialized",
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
            Self::Uninitialized | Self::Closed { .. } => None,
        }
    }

    async fn handle_confirm_journal(
        &mut self,
        sink: &EventSink<Self>,
        provided_id: IssuerMintRequestId,
    ) -> Result<(), MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        sink.write(
            MintEvent::JournalConfirmed {
                issuer_request_id: provided_id,
                confirmed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_reject_journal(
        &mut self,
        sink: &EventSink<Self>,
        provided_id: IssuerMintRequestId,
        reason: String,
    ) -> Result<(), MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        sink.write(
            MintEvent::JournalRejected {
                issuer_request_id: provided_id,
                reason,
                rejected_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
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
    async fn handle_deposit(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<(), MintError> {
        let Self::JournalConfirmed { issuer_request_id: expected_id, .. } =
            self
        else {
            return Err(MintError::NotInJournalConfirmedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        sink.write(
            MintEvent::MintingStarted {
                issuer_request_id,
                started_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    /// Submits the on-chain mint transaction to the signing backend.
    /// Requires `Minting` state (set by prior `Deposit` command).
    async fn handle_submit_mint(
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<(), MintError> {
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
            MintSubmissionParams {
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

        sink.write(event, self).await;

        Ok(())
    }

    /// Shared submission logic: vault lookup, receipt info, submit_mint call.
    /// Returns a single `FireblocksSubmitted` or `MintingFailed` event.
    async fn execute_mint_submission(
        services: &MintServices,
        input: MintSubmissionParams<'_>,
    ) -> Result<MintEvent, MintError> {
        let MintSubmissionParams {
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
            .await?
            .ok_or_else(|| MintError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        let assets = quantity.to_u256_with_18_decimals()?;

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
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
        fireblocks_tx_id: String,
    ) -> Result<(), MintError> {
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

                sink.write(
                    MintEvent::TokensMinted {
                        issuer_request_id,
                        tx_hash: result.tx_hash,
                        receipt_id: result.receipt_id,
                        shares_minted: result.shares_minted,
                        gas_used: result.gas_used,
                        block_number: result.block_number,
                        minted_at: now,
                    },
                    self,
                )
                .await;

                Ok(())
            }
            Err(err) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "On-chain deposit confirmation failed"
                );

                sink.write(
                    MintEvent::MintingFailed {
                        issuer_request_id,
                        error: err.to_string(),
                        failed_at: now,
                    },
                    self,
                )
                .await;

                Ok(())
            }
        }
    }

    async fn handle_send_callback(
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<(), MintError> {
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

        services.alpaca.send_mint_callback(callback_request).await?;

        info!(
            target: "mint",
            issuer_request_id = %issuer_request_id,
            "Alpaca callback succeeded"
        );

        sink.write(
            MintEvent::MintCompleted {
                issuer_request_id,
                completed_at: Utc::now(),
            },
            self,
        )
        .await;

        Ok(())
    }

    async fn handle_recover(
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
        mode: MintRecoveryMode,
    ) -> Result<(), MintError> {
        match &*self {
            Self::JournalConfirmed { .. } => {
                // Emit MintingStarted (intent). drive_recovery will call
                // Recover again, which hits the Minting arm to submit.
                self.handle_deposit(sink, issuer_request_id).await
            }
            Self::FireblocksSubmitted { fireblocks_tx_id, .. } => {
                let fireblocks_tx_id = fireblocks_tx_id.clone();

                // Non-blocking pre-check: a pending tx must pause recovery (so
                // the scheduled loop backs off) instead of blocking in
                // confirm_mint's long poll, which the bounded startup recovery
                // would cancel before any follow-up is scheduled.
                if matches!(
                    services
                        .vault
                        .check_fireblocks_tx(&fireblocks_tx_id)
                        .await?,
                    Some(FireblocksTxStatus::Pending)
                ) {
                    return Err(MintError::FireblocksTxStillPending {
                        fireblocks_tx_id,
                    });
                }

                self.handle_confirm_mint(
                    services,
                    sink,
                    issuer_request_id.clone(),
                    fireblocks_tx_id,
                )
                .await?;
                self.advance_through_callback(services, sink, issuer_request_id)
                    .await
            }
            Self::MintingFailed { .. } => {
                // Preserve the previous Fireblocks tx so recovery can
                // distinguish pending/completed transactions from terminal
                // failures that are safe to resubmit.
                let known_tx = self.latest_known_mint_tx();

                self.handle_recover_incomplete(
                    services,
                    sink,
                    issuer_request_id.clone(),
                    None,
                    known_tx,
                    mode,
                )
                .await?;
                self.advance_through_callback(services, sink, issuer_request_id)
                    .await
            }
            Self::Minting { .. } => {
                self.handle_recover_incomplete(
                    services,
                    sink,
                    issuer_request_id.clone(),
                    None,
                    None,
                    mode,
                )
                .await?;
                self.advance_through_callback(services, sink, issuer_request_id)
                    .await
            }
            Self::CallbackPending { .. } => {
                self.handle_send_callback(services, sink, issuer_request_id)
                    .await
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
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    ) -> Result<(), MintError> {
        match &*self {
            Self::MintingFailed { .. }
                if matches!(
                    self.non_failed_predecessor(),
                    Self::Minting { .. } | Self::FireblocksSubmitted { .. }
                ) =>
            {
                let known_tx = self.latest_known_mint_tx();

                self.handle_recover_incomplete(
                    services,
                    sink,
                    issuer_request_id.clone(),
                    Some(tx_hash),
                    known_tx,
                    MintRecoveryMode::Manual,
                )
                .await?;
                self.advance_through_callback(services, sink, issuer_request_id)
                    .await
            }
            _ => Err(MintError::NotRecoverable {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    /// If the aggregate reached `CallbackPending` after deposit recovery,
    /// advance through the Alpaca callback in the same command execution.
    /// Otherwise return immediately.
    ///
    /// Keeps recovery atomic: a single `Recover` command takes the
    /// aggregate from a stuck pre-callback state straight to `Completed`,
    /// so the aggregate never lingers in `CallbackPending` between
    /// commands and so any recovery entry-point (admin reprocess, the
    /// boot-time loop, or receipt-discovery handler) picks up the same
    /// advancing behavior.
    async fn advance_through_callback(
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
    ) -> Result<(), MintError> {
        if !matches!(self, Self::CallbackPending { .. }) {
            return Ok(());
        }

        // If callback delivery fails after a successful on-chain deposit,
        // preserve CallbackPending. The next recovery picks up from there
        // instead of re-running the deposit path.
        if let Err(err) = self
            .handle_send_callback(services, sink, issuer_request_id.clone())
            .await
        {
            warn!(
                target: "mint",
                issuer_request_id = %issuer_request_id,
                error = %err,
                "Callback failed after successful deposit; \
                 keeping mint in CallbackPending"
            );
        }

        Ok(())
    }

    async fn handle_recover_incomplete(
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
        receipt_tx_hash: Option<TxHash>,
        known_mint_tx: Option<KnownMintTx>,
        mode: MintRecoveryMode,
    ) -> Result<(), MintError> {
        let (expected_id, recovery) = IncompleteMintRecovery::from_mint(self)?;

        Self::validate_issuer_request_id(&expected_id, &issuer_request_id)?;

        let vault =
            find_vault_by_underlying(&services.pool, &recovery.underlying)
                .await?
                .ok_or_else(|| MintError::AssetNotFound {
                    underlying: recovery.underlying.clone(),
                })?;

        if Self::recover_from_existing_receipt(
            services,
            &vault,
            &issuer_request_id,
            sink,
            self,
        )
        .await?
        {
            return Ok(());
        }

        let receipt_info = ReceiptInformation::new(
            recovery.tokenization_request_id.clone(),
            issuer_request_id.clone(),
            recovery.underlying.clone(),
            recovery.quantity.clone(),
            recovery.journal_confirmed_at,
            Some("Recovery mint".to_string()),
        );

        if let Some(known_tx) = known_mint_tx {
            return self
                .resume_incomplete_recovery_known_tx(
                    services,
                    sink,
                    KnownTxRecovery {
                        issuer_request_id,
                        recovery,
                        vault,
                        receipt_info,
                        receipt_tx_hash,
                        known_tx,
                        mode,
                    },
                )
                .await;
        }

        // No prior tx_id: submit and persist FireblocksSubmitted.
        // Don't confirm here — let the next recovery pass handle
        // ConfirmMint from the FireblocksSubmitted state.
        self.submit_recovery_mint(
            services,
            sink,
            MintSubmissionParams {
                issuer_request_id,
                tokenization_request_id: &recovery.tokenization_request_id,
                quantity: &recovery.quantity,
                underlying: &recovery.underlying,
                wallet: recovery.wallet,
                journal_confirmed_at: recovery.journal_confirmed_at,
                receipt_note: Some("Recovery mint"),
                external_tx_id: None,
            },
            receipt_tx_hash,
            mode,
        )
        .await
    }

    async fn resume_incomplete_recovery_known_tx(
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        recovery: KnownTxRecovery,
    ) -> Result<(), MintError> {
        let KnownTxRecovery {
            issuer_request_id,
            recovery,
            vault,
            receipt_info,
            receipt_tx_hash,
            known_tx,
            mode,
        } = recovery;

        info!(
            target: "mint",
            issuer_request_id = %issuer_request_id,
            fireblocks_tx_id = %known_tx.fireblocks_tx_id,
            "Resuming confirmation of previously submitted mint"
        );

        if let Some(status) = services
            .vault
            .check_fireblocks_tx(&known_tx.fireblocks_tx_id)
            .await?
        {
            match status {
                FireblocksTxStatus::Completed { .. } => {}
                FireblocksTxStatus::Pending => {
                    return Err(MintError::FireblocksTxStillPending {
                        fireblocks_tx_id: known_tx.fireblocks_tx_id,
                    });
                }
                FireblocksTxStatus::Failed { detail, sub_status, .. } => {
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
                            sink,
                            MintSubmissionParams {
                                issuer_request_id,
                                tokenization_request_id: &recovery
                                    .tokenization_request_id,
                                quantity: &recovery.quantity,
                                underlying: &recovery.underlying,
                                wallet: recovery.wallet,
                                journal_confirmed_at: recovery
                                    .journal_confirmed_at,
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

        let now = Utc::now();

        match services.vault.confirm_mint(&known_tx.fireblocks_tx_id).await {
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
                        receipt_info_bytes: result.receipt_info_bytes.clone(),
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

                if recovery.is_minting_failed {
                    sink.write(
                        MintEvent::MintRetryStarted {
                            issuer_request_id: issuer_request_id.clone(),
                            tx_hash: receipt_tx_hash,
                            started_at: now,
                        },
                        self,
                    )
                    .await;
                }

                sink.write(
                    MintEvent::TokensMinted {
                        issuer_request_id,
                        tx_hash: result.tx_hash,
                        receipt_id: result.receipt_id,
                        shares_minted: result.shares_minted,
                        gas_used: result.gas_used,
                        block_number: result.block_number,
                        minted_at: now,
                    },
                    self,
                )
                .await;

                Ok(())
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
                sink.write(
                    MintEvent::MintingFailed {
                        issuer_request_id,
                        error: err.to_string(),
                        failed_at: now,
                    },
                    self,
                )
                .await;

                Ok(())
            }
        }
    }

    async fn submit_recovery_mint(
        &mut self,
        services: &MintServices,
        sink: &EventSink<Self>,
        mut input: MintSubmissionParams<'_>,
        receipt_tx_hash: Option<TxHash>,
        mode: MintRecoveryMode,
    ) -> Result<(), MintError> {
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

        if let Some(retry_event) = retry_event {
            sink.write(retry_event, self).await;
        }

        sink.write(submission_event, self).await;

        Ok(())
    }

    async fn recover_from_existing_receipt(
        services: &MintServices,
        vault: &Address,
        issuer_request_id: &IssuerMintRequestId,
        sink: &EventSink<Self>,
        aggregate: &mut Self,
    ) -> Result<bool, MintError> {
        let Some(receipt) = services
            .receipts
            .find_by_issuer_request_id(vault, issuer_request_id)
            .await?
        else {
            debug!(target: "mint", issuer_request_id = %issuer_request_id, "No receipt found, retrying mint");
            return Ok(false);
        };

        info!(
            target: "mint",
            issuer_request_id = %issuer_request_id,
            receipt_id = %receipt.receipt_id,
            "Found existing receipt, recording recovery"
        );

        sink.write(
            MintEvent::ExistingMintRecovered {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: receipt.tx_hash,
                receipt_id: receipt.receipt_id,
                shares_minted: receipt.shares,
                block_number: receipt.block_number,
                recovered_at: Utc::now(),
            },
            aggregate,
        )
        .await;

        Ok(true)
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
    async fn handle_close_mint(
        &mut self,
        sink: &EventSink<Self>,
        issuer_request_id: IssuerMintRequestId,
        reason: String,
    ) -> Result<(), MintError> {
        match self {
            Self::Completed { .. } | Self::Closed { .. } => {
                Err(MintError::NotRecoverable {
                    current_state: self.state_name().to_string(),
                })
            }
            Self::Uninitialized => Err(MintError::NotRecoverable {
                current_state: self.state_name().to_string(),
            }),
            _ => {
                sink.write(
                    MintEvent::MintClosed {
                        issuer_request_id,
                        reason,
                        closed_at: Utc::now(),
                    },
                    self,
                )
                .await;

                Ok(())
            }
        }
    }
}

impl Aggregate for Mint {
    type Command = MintCommand;
    type Event = MintEvent;
    type Error = MintError;
    type Services = MintServices;

    const TYPE: &'static str = "Mint";

    async fn handle(
        &mut self,
        command: Self::Command,
        services: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
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
                    return Err(MintError::AlreadyInitiated {
                        tokenization_request_id: tokenization_request_id.0,
                    });
                }

                sink.write(
                    MintEvent::Initiated {
                        issuer_request_id,
                        tokenization_request_id,
                        quantity,
                        underlying,
                        token,
                        network,
                        client_id,
                        wallet,
                        initiated_at: Utc::now(),
                    },
                    self,
                )
                .await;

                Ok(())
            }

            MintCommand::ConfirmJournal { issuer_request_id } => {
                self.handle_confirm_journal(sink, issuer_request_id).await
            }

            MintCommand::RejectJournal { issuer_request_id, reason } => {
                self.handle_reject_journal(sink, issuer_request_id, reason)
                    .await
            }

            MintCommand::Deposit { issuer_request_id } => {
                self.handle_deposit(sink, issuer_request_id).await
            }

            MintCommand::SubmitMint { issuer_request_id } => {
                self.handle_submit_mint(services, sink, issuer_request_id).await
            }

            MintCommand::ConfirmMint {
                issuer_request_id,
                fireblocks_tx_id,
            } => {
                self.handle_confirm_mint(
                    services,
                    sink,
                    issuer_request_id,
                    fireblocks_tx_id,
                )
                .await
            }

            MintCommand::SendCallback { issuer_request_id } => {
                self.handle_send_callback(services, sink, issuer_request_id)
                    .await
            }

            MintCommand::Recover { issuer_request_id, mode } => {
                self.handle_recover(services, sink, issuer_request_id, mode)
                    .await
            }

            MintCommand::RecoverFromReceipt { issuer_request_id, tx_hash } => {
                self.handle_recover_from_receipt(
                    services,
                    sink,
                    issuer_request_id,
                    tx_hash,
                )
                .await
            }

            MintCommand::CloseMint { issuer_request_id, reason } => {
                self.handle_close_mint(sink, issuer_request_id, reason).await
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
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

#[derive(Debug, thiserror::Error)]
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
    #[error("Quantity conversion: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("View: {0}")]
    View(#[from] TokenizedAssetViewError),
    #[error("Alpaca: {0}")]
    Alpaca(#[from] AlpacaError),
    #[error("Account view: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Receipt inventory view: {0}")]
    ReceiptInventoryView(#[from] ReceiptInventoryViewError),
    #[error(transparent)]
    ReceiptLookup(#[from] ReceiptLookupError),
    #[error("Vault: {0}")]
    Vault(#[from] VaultError),
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::{Address, B256, U256, address, b256, uint};
    use async_trait::async_trait;
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use cqrs_es::View;
    use cqrs_es::{
        Aggregate, AggregateContext, CqrsFramework, EventStore,
        event_sink::EventSink, mem_store::MemStore, test::TestFramework,
    };
    use event_sorcery::{Store, StoreBuilder, test_store};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;
    use uuid::Uuid;

    use super::{
        AutomaticRetryDecision, ClientId, IssuerMintRequestId, Mint,
        MintCommand, MintError, MintEvent, MintRecoveryMode, MintServices,
        MintView, Network, Quantity, TokenSymbol, TokenizationRequestId,
        UnderlyingSymbol,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
    use crate::test_utils::log_count_at;
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

    pub(super) struct MintTestFixture {
        pub(super) mint_cqrs: Arc<CqrsFramework<Mint, MemStore<Mint>>>,
        pub(super) mint_store: Arc<MemStore<Mint>>,
        pub(super) receipt_store: Arc<Store<ReceiptInventory>>,
    }

    impl MintTestFixture {
        pub(super) async fn new() -> Self {
            Self::new_with_vault(Arc::new(MockVaultService::new_success()))
                .await
        }

        pub(super) async fn new_with_vault(
            vault: Arc<dyn VaultService>,
        ) -> Self {
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
                        network: Network::new("base"),
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
                receipts: Arc::new(CqrsReceiptService::new(
                    receipt_store.clone(),
                )),
                pool: pool.clone(),
                bot: BOT,
            };

            let mint_store = Arc::new(MemStore::<Mint>::default());
            let mint_cqrs = Arc::new(CqrsFramework::new(
                (*mint_store).clone(),
                vec![],
                services,
            ));

            Self { mint_cqrs, mint_store, receipt_store }
        }

        pub(super) async fn seed_mint_events(
            &self,
            aggregate_id: &str,
            events: Vec<MintEvent>,
        ) {
            let context =
                self.mint_store.load_aggregate(aggregate_id).await.unwrap();

            self.mint_store
                .commit(events, context, HashMap::default())
                .await
                .unwrap();
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
                network: Network::new("base"),
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

    prop_compose! {
        pub(crate) fn arb_issuer_request_id()(bytes in any::<[u8; 16]>()) -> IssuerMintRequestId {
            IssuerMintRequestId::new(Uuid::from_bytes(bytes))
        }
    }

    type MintTestFramework = TestFramework<Mint>;

    fn test_mint_services() -> MintServices {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create runtime");

        let pool = rt.block_on(async {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!()
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            pool
        });

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

    #[test]
    fn test_initiate_mint_creates_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = MintTestFramework::with(test_mint_services())
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
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
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
                        assert_eq!(
                            event_tokenization_id,
                            &tokenization_request_id
                        );
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
                        panic!(
                            "Expected MintInitiated event, got {:?}",
                            &events[0]
                        )
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_initiate_mint_when_already_initiated_returns_error() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = MintTestFramework::with(test_mint_services())
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
            .inspect_result();

        assert!(
            matches!(result, Err(MintError::AlreadyInitiated { .. })),
            "Expected AlreadyInitiated error, got {result:?}"
        );
    }

    #[test]
    fn test_apply_initiated_event_updates_state() {
        let mut mint = Mint::default();

        assert!(matches!(mint, Mint::Uninitialized));

        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(50));
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = chrono::Utc::now();

        mint.apply(MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
        });

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
        let network = Network::new("base");
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

        mint.apply(MintEvent::JournalConfirmed {
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
        let network = Network::new("base");
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

        mint.apply(MintEvent::JournalRejected {
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

    #[test]
    fn test_confirm_journal_produces_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = MintTestFramework::with(test_mint_services())
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
            .when(MintCommand::ConfirmJournal { issuer_request_id });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], MintEvent::JournalConfirmed { .. }));
    }

    #[test]
    fn test_confirm_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();

        let validator = MintTestFramework::with(test_mint_services())
            .given_no_previous_events()
            .when(MintCommand::ConfirmJournal { issuer_request_id });

        let result = validator.inspect_result();

        assert!(matches!(result, Err(MintError::NotInInitiatedState { .. })));
    }

    #[test]
    fn test_confirm_journal_for_already_confirmed_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = MintTestFramework::with(test_mint_services())
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
            .when(MintCommand::ConfirmJournal { issuer_request_id });

        let result = validator.inspect_result();

        assert!(matches!(result, Err(MintError::NotInInitiatedState { .. })));
    }

    #[test]
    fn test_reject_journal_produces_event() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let reason = "Insufficient funds";

        let validator = MintTestFramework::with(test_mint_services())
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
            });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);

        let MintEvent::JournalRejected { reason: event_reason, .. } =
            &events[0]
        else {
            panic!("Expected JournalRejected event, got {:?}", &events[0]);
        };

        assert_eq!(event_reason, reason);
    }

    #[test]
    fn test_reject_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = IssuerMintRequestId::random();

        let validator = MintTestFramework::with(test_mint_services())
            .given_no_previous_events()
            .when(MintCommand::RejectJournal {
                issuer_request_id,
                reason: "Test reason".to_string(),
            });

        let result = validator.inspect_result();

        assert!(matches!(result, Err(MintError::NotInInitiatedState { .. })));
    }

    #[test]
    fn test_confirm_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id = IssuerMintRequestId::random();
        let wrong_issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = MintTestFramework::with(test_mint_services())
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
            .inspect_result();

        assert!(
            matches!(
                result,
                Err(MintError::IssuerMintRequestIdMismatch { .. })
            ),
            "Expected IssuerMintRequestIdMismatch error, got {result:?}"
        );
    }

    #[test]
    fn test_reject_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id = IssuerMintRequestId::random();
        let wrong_issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = MintTestFramework::with(test_mint_services())
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
            .inspect_result();
        assert!(
            matches!(
                result,
                Err(MintError::IssuerMintRequestIdMismatch { .. })
            ),
            "Expected IssuerMintRequestIdMismatch error, got {result:?}"
        );
    }

    #[test]
    fn test_apply_minting_started_event_updates_state() {
        let issuer_request_id = IssuerMintRequestId::random();
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
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
        mint.apply(MintEvent::MintingStarted {
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
        let network = Network::new("base");
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

        mint.apply(MintEvent::TokensMinted {
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
        let network = Network::new("base");
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

        mint.apply(MintEvent::MintingFailed {
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
        let network = Network::new("base");
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

        mint.apply(MintEvent::MintCompleted {
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
        let network = Network::new("base");
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

        let mut mint = Mint::default();

        mint.apply(MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        });

        assert!(
            matches!(mint, Mint::Initiated { .. }),
            "Expected Initiated state, got {mint:?}"
        );

        mint.apply(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        });

        assert!(
            matches!(mint, Mint::JournalConfirmed { .. }),
            "Expected JournalConfirmed state, got {mint:?}"
        );

        let minting_started_at = Utc::now();
        mint.apply(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: minting_started_at,
        });

        assert!(
            matches!(mint, Mint::Minting { .. }),
            "Expected Minting state, got {mint:?}"
        );

        mint.apply(MintEvent::TokensMinted {
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

        mint.apply(MintEvent::MintCompleted {
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
                network: Network::new("base"),
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            }
        }
    }

    async fn setup_cqrs_integration_test()
    -> (Arc<MemStore<Mint>>, Arc<CqrsFramework<Mint, MemStore<Mint>>>) {
        let fixture = MintTestFixture::new().await;
        (fixture.mint_store, fixture.mint_cqrs)
    }

    #[tokio::test]
    async fn test_complete_mint_flow_via_cqrs() {
        let (store, cqrs) = setup_cqrs_integration_test().await;
        let data = TestMintData::new();

        cqrs.execute(
            &data.issuer_request_id.to_string(),
            MintCommand::Initiate {
                issuer_request_id: data.issuer_request_id.clone(),
                tokenization_request_id: data.tokenization_request_id.clone(),
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

        cqrs.execute(
            &data.issuer_request_id.to_string(),
            MintCommand::ConfirmJournal {
                issuer_request_id: data.issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        // Deposit records intent (emits MintingStarted)
        cqrs.execute(
            &data.issuer_request_id.to_string(),
            MintCommand::Deposit {
                issuer_request_id: data.issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        // SubmitMint does the network call (emits FireblocksSubmitted)
        cqrs.execute(
            &data.issuer_request_id.to_string(),
            MintCommand::SubmitMint {
                issuer_request_id: data.issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        // Load the fireblocks_tx_id from the aggregate state
        let mut ctx = store
            .load_aggregate(&data.issuer_request_id.to_string())
            .await
            .unwrap();

        let Mint::FireblocksSubmitted { fireblocks_tx_id, .. } =
            ctx.aggregate()
        else {
            panic!(
                "Expected FireblocksSubmitted state after SubmitMint, got {:?}",
                ctx.aggregate()
            );
        };

        let fb_tx_id = fireblocks_tx_id.clone();

        // ConfirmMint polls the backend (emits TokensMinted)
        cqrs.execute(
            &data.issuer_request_id.to_string(),
            MintCommand::ConfirmMint {
                issuer_request_id: data.issuer_request_id.clone(),
                fireblocks_tx_id: fb_tx_id,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &data.issuer_request_id.to_string(),
            MintCommand::SendCallback {
                issuer_request_id: data.issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        let mut context = store
            .load_aggregate(&data.issuer_request_id.to_string())
            .await
            .unwrap();

        assert!(
            matches!(context.aggregate(), Mint::Completed { .. }),
            "Expected Completed state, got {:?}",
            context.aggregate()
        );

        let events = store
            .load_events(&data.issuer_request_id.to_string())
            .await
            .unwrap();

        assert_eq!(events.len(), 6, "Expected 6 events in the split flow");

        assert!(
            matches!(&events[0].payload, MintEvent::Initiated { .. }),
            "First event should be Initiated"
        );
        assert!(
            matches!(&events[1].payload, MintEvent::JournalConfirmed { .. }),
            "Second event should be JournalConfirmed"
        );
        assert!(
            matches!(&events[2].payload, MintEvent::MintingStarted { .. }),
            "Third event should be MintingStarted"
        );
        assert!(
            matches!(&events[3].payload, MintEvent::FireblocksSubmitted { .. }),
            "Fourth event should be FireblocksSubmitted"
        );
        assert!(
            matches!(&events[4].payload, MintEvent::TokensMinted { .. }),
            "Fifth event should be TokensMinted"
        );
        assert!(
            matches!(&events[5].payload, MintEvent::MintCompleted { .. }),
            "Sixth event should be MintCompleted"
        );

        let mut view = MintView::default();
        for event in &events {
            view.update(event);
        }

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
            .mint_cqrs
            .execute(
                &issuer_request_id.to_string(),
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

        let mut context = fixture
            .mint_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
            .unwrap();

        let Mint::FireblocksSubmitted { external_tx_id, .. } =
            context.aggregate()
        else {
            panic!("Expected FireblocksSubmitted after retry");
        };
        assert_eq!(
            external_tx_id,
            &format!("mint-{issuer_request_id}-retry-1")
        );

        // A successful retry must record MintRetryStarted as the permanent
        // audit-trail fact that recovery resubmitted the mint.
        let stored = fixture
            .mint_store
            .load_events(&issuer_request_id.to_string())
            .await
            .unwrap();
        let retry_started = stored
            .iter()
            .filter(|event| {
                matches!(event.payload, MintEvent::MintRetryStarted { .. })
            })
            .count();
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
            .mint_cqrs
            .execute(
                &issuer_request_id.to_string(),
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await;

        assert!(
            matches!(
                result,
                Err(cqrs_es::AggregateError::UserError(
                    MintError::RetryNotDue { .. }
                ))
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
            .mint_cqrs
            .execute(
                &issuer_request_id.to_string(),
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await;
        assert!(
            matches!(
                automatic_result,
                Err(cqrs_es::AggregateError::UserError(
                    MintError::AutomaticRetriesExhausted { attempts: 4 }
                ))
            ),
            "Expected AutomaticRetriesExhausted, got {automatic_result:?}"
        );

        fixture
            .mint_cqrs
            .execute(
                &issuer_request_id.to_string(),
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

        let mut context = fixture
            .mint_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
            .unwrap();
        let Mint::FireblocksSubmitted { external_tx_id, .. } =
            context.aggregate()
        else {
            panic!(
                "Expected FireblocksSubmitted after manual retry, got: {}",
                context.aggregate().state_name()
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
            .mint_cqrs
            .execute(
                &issuer_request_id.to_string(),
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mut context = fixture
            .mint_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
            .unwrap();
        let aggregate = context.aggregate().clone();

        let Mint::MintingFailed { failed_at, failed_from, .. } = &aggregate
        else {
            panic!(
                "Expected MintingFailed after failed resubmission, got: {}",
                aggregate.state_name()
            );
        };
        assert!(
            matches!(failed_from.as_ref(), Mint::FireblocksSubmitted { .. }),
            "Predecessor chain must be preserved on submission failure"
        );
        assert_eq!(
            aggregate.next_retry_attempt(),
            2,
            "Retry counter must not reset after a failed retry submission"
        );
        assert!(
            *failed_at > original_failed_at,
            "failed_at must advance so the backoff anchor moves forward"
        );

        let stored = fixture
            .mint_store
            .load_events(&issuer_request_id.to_string())
            .await
            .unwrap();
        let retry_started = stored
            .iter()
            .filter(|event| {
                matches!(event.payload, MintEvent::MintRetryStarted { .. })
            })
            .count();
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
            .mint_cqrs
            .execute(
                &issuer_request_id.to_string(),
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
        let mut mint = Mint::default();
        mint.apply(MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("tok-123"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: now,
        });
        mint.apply(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at: now,
        });
        mint.apply(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: now,
        });

        // failed_at far in the past so every retry window has elapsed; the
        // decision then turns only on the escalating attempt counter.
        let failed_at = now - ChronoDuration::hours(3);

        // First submission failure from Minting: attempts = 1, retryable.
        mint.apply(MintEvent::MintingFailed {
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
            mint.apply(MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "submission rejected".to_string(),
                failed_at,
            });
        }
        assert_eq!(
            mint.automatic_retry_decision(Utc::now()),
            AutomaticRetryDecision::Ready
        );

        mint.apply(MintEvent::MintingFailed {
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

    #[test]
    fn test_recover_from_receipt_rejects_journal_confirmed() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();

        let validator = MintTestFramework::with(test_mint_services())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-123",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
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
            });

        assert!(
            matches!(
                validator.inspect_result(),
                Err(MintError::NotRecoverable { .. })
            ),
            "RecoverFromReceipt should reject JournalConfirmed state"
        );
    }

    #[test]
    fn test_recover_from_receipt_rejects_minting() {
        let issuer_request_id = IssuerMintRequestId::random();
        let now = Utc::now();

        let validator = MintTestFramework::with(test_mint_services())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "tok-123",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
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
            });

        assert!(
            matches!(
                validator.inspect_result(),
                Err(MintError::NotRecoverable { .. })
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
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: now,
            journal_confirmed_at: now,
        };

        let mut mint = Mint::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new("tok-123"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::new("base"),
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

        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

        let services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
            pool,
            bot: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        };

        let sink = EventSink::default();

        let result = mint
            .handle_recover_from_receipt(
                &services,
                &sink,
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
