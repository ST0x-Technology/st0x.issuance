//! Durable jobs that perform the `Mint` aggregate's external side effects.
//!
//! Each job runs one step of the on-chain mint flow off the command handler:
//! it performs the external call (`vault.submit_mint` / `vault.confirm_mint` /
//! `alpaca.send_mint_callback`), reports the result back as an idempotent
//! outcome command (`RecordTxSubmitted` / `RecordTokensMinted` /
//! `RecordCallbackSent` / `RecordMintFailed`), and enqueues the next step. The
//! handlers stay pure; the jobs are the only place I/O happens.
//!
//! Jobs are **drainer-style** (no apalis retry layer): a domain failure becomes
//! a `MintingFailed` event that the recovery budget loop retries on its own
//! schedule — and after recording it the job immediately enqueues the scheduled
//! recovery job, so the first automatic retry starts right away instead of
//! waiting for the periodic reconciler — while an infrastructure failure
//! surfaces as a job error that apalis re-drives. Re-runs are safe: the exact
//! signed transaction is persisted before broadcast and reused until its
//! submission resolves, while every outcome command is a no-op once its event
//! is recorded.

use alloy::primitives::Address;
use apalis_sqlite::SqlitePool;
use event_sorcery::{SendError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, info, warn};

use super::recovery::enqueue_scheduled_mint_recovery;
use super::{
    IssuerMintRequestId, Mint, MintCommand, MintFailureClassification,
    Quantity, has_unresolved_mint_intent,
    orchestrator_mint_failure_classification,
};
use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};
use crate::config::VaultMode;
use crate::jobs::{Job, JobQueue, QueuePushError};
use crate::receipt_inventory::{
    MintedReceiptParams, ReceiptId, ReceiptLookupError, ReceiptService, Shares,
};
use crate::tokenized_asset::Network;
use crate::vault::{
    MintAuthorization, MintedLogQuery, MintedLogScan, NetworkVaultServices,
    OrchestratorMintParams, OrchestratorRevertReason, ReceiptInformation, TxId,
    UnconfiguredNetworkError, VaultError, VaultService,
};

/// Failure of a mint side-effect job. A domain rejection is recorded as a
/// `MintingFailed` event instead (see the module docs); these variants are the
/// infrastructure failures that make apalis re-drive the job.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MintJobError {
    #[error(transparent)]
    Store(#[from] SendError<Mint>),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
    #[error(transparent)]
    Alpaca(#[from] AlpacaError),
    #[error(transparent)]
    ReceiptLookup(#[from] ReceiptLookupError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(
        "mint {issuer_request_id} is waiting for another persisted wallet intent"
    )]
    UnresolvedWalletIntent { issuer_request_id: IssuerMintRequestId },
}

/// Submits the on-chain mint to the signing backend, then hands off to
/// [`ConfirmMintJob`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SubmitMintJob {
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) vault: Address,
    pub(crate) chain_id: u64,
}

pub(crate) struct SubmitMintContext {
    pub(crate) mint_store: Arc<Store<Mint>>,
    /// Per-network signing backends. Jobs carry a vault address + chain id,
    /// but the RPC/signer must come from the mint's `network` — a single
    /// shared `VaultService` would submit every mint against the default
    /// (Base) chain.
    pub(crate) vaults: NetworkVaultServices,
    pub(crate) receipts: Arc<dyn ReceiptService>,
    pub(crate) bot: Address,
    pub(crate) confirm_queue: JobQueue<ConfirmMintJob>,
    pub(crate) callback_queue: JobQueue<SendCallbackJob>,
    /// Event-store pool; the post-failure recovery enqueue releases terminal
    /// job rows on it (see [`enqueue_scheduled_mint_recovery`]).
    pub(crate) pool: Pool<Sqlite>,
    /// apalis pool the recovery job is pushed to.
    pub(crate) apalis_pool: SqlitePool,
}

impl SubmitMintContext {
    fn vault_for(
        &self,
        network: Network,
    ) -> Result<Arc<dyn VaultService>, UnconfiguredNetworkError> {
        self.vaults.service(network).cloned()
    }
}

impl Job<SubmitMintContext> for SubmitMintJob {
    type Output = ();
    type Error = MintJobError;

    #[allow(clippy::too_many_lines)]
    async fn perform(
        &self,
        ctx: &SubmitMintContext,
    ) -> Result<(), MintJobError> {
        let Some(mint) = ctx.mint_store.load(&self.issuer_request_id).await?
        else {
            return Ok(());
        };

        match &mint {
            Mint::Minting {
                tokenization_request_id,
                quantity,
                underlying,
                network,
                wallet,
                journal_confirmed_at,
                mint_mode,
                mint_authorization,
                ..
            } => {
                if self.record_existing_receipt(ctx).await? {
                    return Ok(());
                }

                let Some(vault) =
                    self.resolve_vault_service(ctx, *network).await?
                else {
                    return Ok(());
                };

                // A quantity that cannot be converted is deterministic for
                // the persisted mint: record a domain failure instead of
                // returning a job error apalis would re-drive forever.
                let assets = match quantity.to_u256_with_18_decimals() {
                    Ok(assets) => assets,
                    Err(error) => {
                        warn!(
                            target: "mint",
                            issuer_request_id = %self.issuer_request_id,
                            error = %error,
                            "Mint quantity cannot be converted to on-chain \
                             units"
                        );

                        ctx.mint_store
                            .send(
                                &self.issuer_request_id,
                                MintCommand::RecordMintFailed {
                                    issuer_request_id: self
                                        .issuer_request_id
                                        .clone(),
                                    error: error.to_string(),
                                    classification:
                                        MintFailureClassification::Unclassified,
                                },
                            )
                            .await?;

                        return Ok(());
                    }
                };
                let receipt_info = ReceiptInformation::new(
                    tokenization_request_id.clone(),
                    self.issuer_request_id.clone(),
                    underlying.clone(),
                    quantity.clone(),
                    *journal_confirmed_at,
                    None,
                );

                let external_tx_id = mint
                    .retry_submission_external_tx_id()
                    .map(super::MintExternalTxId::into_string);

                let wallet_guard = vault.lock_wallet().await;
                if has_unresolved_mint_intent(
                    &ctx.pool,
                    Some(&self.issuer_request_id),
                )
                .await?
                {
                    return Err(MintJobError::UnresolvedWalletIntent {
                        issuer_request_id: self.issuer_request_id.clone(),
                    });
                }
                let prepared = if let Some(prepared) =
                    mint.pending_prepared_tx()
                {
                    prepared
                } else {
                    let prepared = match mint_mode {
                        VaultMode::VaultDirect => {
                            vault
                                .prepare_mint_tx(
                                    self.vault,
                                    assets,
                                    ctx.bot,
                                    *wallet,
                                    receipt_info,
                                    external_tx_id,
                                )
                                .await
                        }
                        VaultMode::Orchestrator { address: orchestrator } => {
                            // An orchestrator mint without its recipient
                            // authorization cannot submit yet — and never
                            // falls back to vault-direct. No event is
                            // recorded: the mint stays in `Minting`, visible
                            // in `/admin/stuck` past the threshold, and the
                            // next drive retries once the liquidity bot
                            // delivers.
                            let Some(authorization) = mint_authorization else {
                                drop(wallet_guard);
                                warn!(
                                    target: "mint",
                                    issuer_request_id = %self.issuer_request_id,
                                    "Orchestrator mint is awaiting its \
                                     recipient authorization; deferring \
                                     submission"
                                );
                                return Ok(());
                            };

                            vault
                                .prepare_orchestrator_mint_tx(
                                    &OrchestratorMintParams {
                                        orchestrator: *orchestrator,
                                        token: self.vault,
                                        to: *wallet,
                                        amount: assets,
                                        authorization: authorization.clone(),
                                        receipt_info,
                                        external_tx_id,
                                    },
                                )
                                .await
                        }
                    };
                    let prepared = match prepared {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            drop(wallet_guard);
                            self.record_submission_failure(ctx, error).await?;
                            return Ok(());
                        }
                    };

                    ctx.mint_store
                        .send(
                            &self.issuer_request_id,
                            MintCommand::RecordTxIntended {
                                issuer_request_id: self
                                    .issuer_request_id
                                    .clone(),
                                prepared_tx: prepared.clone(),
                            },
                        )
                        .await?;
                    prepared
                };

                let submitted = vault.submit_mint(&prepared).await;

                match submitted {
                    Ok(submitted) => {
                        let tx_id = submitted.tx_id;
                        ctx.mint_store
                            .send(
                                &self.issuer_request_id,
                                MintCommand::RecordTxSubmitted {
                                    issuer_request_id: self
                                        .issuer_request_id
                                        .clone(),
                                    external_tx_id:
                                        super::MintExternalTxId::from_string(
                                            submitted.external_tx_id,
                                        ),
                                    tx_id: tx_id.clone(),
                                },
                            )
                            .await?;

                        self.enqueue_confirm(ctx, tx_id).await?;
                    }
                    Err(error) => {
                        self.record_submission_failure(ctx, error).await?;
                    }
                }
                drop(wallet_guard);
            }
            // Crash recovery for a prepare-then-submit job that persisted
            // intent via the inline PrepareMint path before the job rewrite.
            Mint::TxIntended { prepared_tx, network, .. } => {
                if self.record_existing_receipt(ctx).await? {
                    return Ok(());
                }
                let Some(vault) =
                    self.resolve_vault_service(ctx, *network).await?
                else {
                    return Ok(());
                };
                let wallet_guard = vault.lock_wallet().await;
                if has_unresolved_mint_intent(
                    &ctx.pool,
                    Some(&self.issuer_request_id),
                )
                .await?
                {
                    return Err(MintJobError::UnresolvedWalletIntent {
                        issuer_request_id: self.issuer_request_id.clone(),
                    });
                }

                match vault.submit_mint(prepared_tx).await {
                    Ok(submitted) => {
                        let tx_id = submitted.tx_id;
                        ctx.mint_store
                            .send(
                                &self.issuer_request_id,
                                MintCommand::RecordTxSubmitted {
                                    issuer_request_id: self
                                        .issuer_request_id
                                        .clone(),
                                    external_tx_id:
                                        super::MintExternalTxId::from_string(
                                            submitted.external_tx_id,
                                        ),
                                    tx_id: tx_id.clone(),
                                },
                            )
                            .await?;

                        self.enqueue_confirm(ctx, tx_id).await?;
                    }
                    Err(error) => {
                        self.record_submission_failure(ctx, error).await?;
                    }
                }
                drop(wallet_guard);
            }
            // A re-run after the submission was already recorded: the
            // confirm job may not have been enqueued before a crash, so
            // keep the chain moving.
            Mint::TxSubmitted { tx_id, .. } => {
                self.enqueue_confirm(ctx, tx_id.clone()).await?;
            }
            // Pre-minting (the Deposit step has not recorded intent yet),
            // past confirmation, failed (recovery owns retries), or terminal.
            Mint::Initiated { .. }
            | Mint::JournalConfirmed { .. }
            | Mint::JournalRejected { .. }
            | Mint::CallbackPending { .. }
            | Mint::MintingFailed { .. }
            | Mint::Completed { .. }
            | Mint::Closed { .. } => {}
        }

        Ok(())
    }
}

impl SubmitMintJob {
    async fn record_existing_receipt(
        &self,
        ctx: &SubmitMintContext,
    ) -> Result<bool, MintJobError> {
        let Some(receipt) = ctx
            .receipts
            .find_by_issuer_request_id(
                self.chain_id,
                &self.vault,
                &self.issuer_request_id,
            )
            .await?
        else {
            return Ok(false);
        };

        info!(
            target: "mint",
            issuer_request_id = %self.issuer_request_id,
            tx_hash = %receipt.tx_hash,
            block_number = receipt.block_number,
            "Found existing receipt, recording recovery"
        );
        ctx.mint_store
            .send(
                &self.issuer_request_id,
                MintCommand::RecordExistingMint {
                    issuer_request_id: self.issuer_request_id.clone(),
                    tx_hash: receipt.tx_hash,
                    receipt_id: receipt.receipt_id,
                    shares_minted: receipt.shares,
                    block_number: receipt.block_number,
                },
            )
            .await?;
        self.enqueue_callback(ctx).await?;
        Ok(true)
    }

    /// Resolves the signing backend for `network`. An unconfigured network is
    /// deterministic for this deploy, so it is recorded as `MintingFailed`
    /// instead of surfacing as a job error apalis would re-drive forever.
    async fn resolve_vault_service(
        &self,
        ctx: &SubmitMintContext,
        network: Network,
    ) -> Result<Option<Arc<dyn VaultService>>, MintJobError> {
        match ctx.vault_for(network) {
            Ok(vault) => Ok(Some(vault)),
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    network = %network,
                    error = %error,
                    "No vault service configured for mint network"
                );
                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordMintFailed {
                            issuer_request_id: self.issuer_request_id.clone(),
                            error: error.to_string(),
                            classification:
                                MintFailureClassification::Unclassified,
                        },
                    )
                    .await?;
                Ok(None)
            }
        }
    }

    async fn record_submission_failure(
        &self,
        ctx: &SubmitMintContext,
        error: VaultError,
    ) -> Result<(), MintJobError> {
        warn!(
            target: "mint",
            issuer_request_id = %self.issuer_request_id,
            error = %error,
            "Mint submission failed"
        );

        ctx.mint_store
            .send(
                &self.issuer_request_id,
                MintCommand::RecordMintFailed {
                    issuer_request_id: self.issuer_request_id.clone(),
                    error: error.to_string(),
                    classification: MintFailureClassification::Unclassified,
                },
            )
            .await?;

        kick_mint_recovery(
            &ctx.pool,
            &ctx.apalis_pool,
            &self.issuer_request_id,
        )
        .await;

        Ok(())
    }

    async fn enqueue_confirm(
        &self,
        ctx: &SubmitMintContext,
        tx_id: TxId,
    ) -> Result<(), MintJobError> {
        ctx.confirm_queue
            .clone()
            .push_with_idempotency_key(
                ConfirmMintJob {
                    issuer_request_id: self.issuer_request_id.clone(),
                    vault: self.vault,
                    chain_id: self.chain_id,
                    tx_id,
                },
                self.issuer_request_id.to_string(),
            )
            .await?;

        Ok(())
    }

    /// Enqueued after recording an already-succeeded mint
    /// (`RecordExistingMint` advances it straight to `CallbackPending`).
    async fn enqueue_callback(
        &self,
        ctx: &SubmitMintContext,
    ) -> Result<(), MintJobError> {
        ctx.callback_queue
            .clone()
            .push_with_idempotency_key(
                SendCallbackJob {
                    issuer_request_id: self.issuer_request_id.clone(),
                },
                self.issuer_request_id.to_string(),
            )
            .await?;

        Ok(())
    }
}

/// Confirms a submitted mint on-chain, registers the receipt (best-effort),
/// then hands off to [`SendCallbackJob`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConfirmMintJob {
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) vault: Address,
    pub(crate) chain_id: u64,
    pub(crate) tx_id: TxId,
}

pub(crate) struct ConfirmMintContext {
    pub(crate) mint_store: Arc<Store<Mint>>,
    /// Per-network signing backends; confirm polls the chain that submitted
    /// the mint (see [`SubmitMintContext::vaults`]).
    pub(crate) vaults: NetworkVaultServices,
    pub(crate) receipts: Arc<dyn ReceiptService>,
    pub(crate) callback_queue: JobQueue<SendCallbackJob>,
    /// Event-store pool; the post-failure recovery enqueue releases terminal
    /// job rows on it (see [`enqueue_scheduled_mint_recovery`]).
    pub(crate) pool: Pool<Sqlite>,
    /// apalis pool the recovery job is pushed to.
    pub(crate) apalis_pool: SqlitePool,
}

impl ConfirmMintContext {
    fn vault_for(
        &self,
        network: Network,
    ) -> Result<Arc<dyn VaultService>, UnconfiguredNetworkError> {
        self.vaults.service(network).cloned()
    }
}

impl Job<ConfirmMintContext> for ConfirmMintJob {
    type Output = ();
    type Error = MintJobError;

    async fn perform(
        &self,
        ctx: &ConfirmMintContext,
    ) -> Result<(), MintJobError> {
        let Some(mint) = ctx.mint_store.load(&self.issuer_request_id).await?
        else {
            return Ok(());
        };

        let Mint::TxSubmitted {
            tokenization_request_id,
            quantity,
            underlying,
            network,
            wallet,
            journal_confirmed_at,
            mint_mode,
            mint_authorization,
            ..
        } = &mint
        else {
            // A re-run after the mint was already confirmed: keep the chain
            // moving if it is awaiting its callback.
            if matches!(&mint, Mint::CallbackPending { .. }) {
                self.enqueue_callback(ctx).await?;
            }
            return Ok(());
        };

        let vault = match ctx.vault_for(*network) {
            Ok(vault) => vault,
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    network = %network,
                    error = %error,
                    "No vault service configured for mint network"
                );
                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordMintFailed {
                            issuer_request_id: self.issuer_request_id.clone(),
                            error: error.to_string(),
                            classification:
                                MintFailureClassification::Unclassified,
                        },
                    )
                    .await?;
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
                return Ok(());
            }
        };

        if let VaultMode::Orchestrator { address: orchestrator } = mint_mode {
            return self
                .confirm_orchestrator(
                    ctx,
                    vault,
                    *orchestrator,
                    *wallet,
                    quantity,
                    mint_authorization.as_ref(),
                )
                .await;
        }

        match vault.confirm_mint(&self.tx_id).await {
            Ok(result) => {
                let receipt_info = ReceiptInformation::new(
                    tokenization_request_id.clone(),
                    self.issuer_request_id.clone(),
                    underlying.clone(),
                    quantity.clone(),
                    *journal_confirmed_at,
                    None,
                );

                // Best-effort: a registration failure must not block
                // `TokensMinted` — the monitor/backfill rediscovers the receipt.
                if let Err(error) = ctx
                    .receipts
                    .register_minted_receipt(MintedReceiptParams {
                        chain_id: self.chain_id,
                        vault: self.vault,
                        receipt_id: ReceiptId::from(result.receipt_id),
                        shares: Shares::from(result.shares_minted),
                        block_number: result.block_number,
                        tx_hash: result.tx_hash,
                        receipt_info,
                        receipt_info_bytes: result.receipt_info_bytes.clone(),
                    })
                    .await
                {
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        error = %error,
                        "Failed to register minted receipt \
                         (monitor/backfill will discover it)"
                    );
                }

                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordTokensMinted {
                            issuer_request_id: self.issuer_request_id.clone(),
                            tx_id: self.tx_id.clone(),
                            tx_hash: result.tx_hash,
                            receipt_id: result.receipt_id,
                            shares_minted: result.shares_minted,
                            gas_used: result.gas_used,
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                self.enqueue_callback(ctx).await?;
            }
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %error,
                    "On-chain deposit confirmation failed"
                );

                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordMintFailed {
                            issuer_request_id: self.issuer_request_id.clone(),
                            error: error.to_string(),
                            classification:
                                MintFailureClassification::Unclassified,
                        },
                    )
                    .await?;

                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
        }

        Ok(())
    }
}

impl ConfirmMintJob {
    /// Orchestrator-mode confirmation: the job counterpart of
    /// `Mint::handle_record_orchestrator_tokens_minted`. Success records
    /// `OrchestratorTokensMinted` with NO receipt registration — the
    /// orchestrator holds receipt custody. A decoded `NonceReplayed` revert
    /// means an earlier transaction consumed this `(to, nonce)` pair, so the
    /// landed `Minted` log is full-matched on `(to, nonce, token, amount)`:
    /// only a full match may complete the mint; a consumed nonce whose token
    /// or amount differs fails as `NonceConsumedByOtherMint` for manual
    /// reconciliation, never a false completion.
    async fn confirm_orchestrator(
        &self,
        ctx: &ConfirmMintContext,
        vault_service: Arc<dyn VaultService>,
        orchestrator: Address,
        to: Address,
        quantity: &Quantity,
        authorization: Option<&MintAuthorization>,
    ) -> Result<(), MintJobError> {
        let revert = match vault_service
            .confirm_orchestrator_mint(&self.tx_id)
            .await
        {
            Ok(result) => {
                info!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %result.tx_hash,
                    nonce = %result.nonce,
                    shares_minted = %result.shares_minted,
                    "Orchestrator mint confirmed"
                );

                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordOrchestratorTokensMinted {
                            issuer_request_id: self.issuer_request_id.clone(),
                            tx_id: self.tx_id.clone(),
                            tx_hash: result.tx_hash,
                            nonce: result.nonce,
                            shares_minted: result.shares_minted,
                            gas_used: result.gas_used,
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                self.enqueue_callback(ctx).await?;
                return Ok(());
            }
            Err(error) => error,
        };

        let is_nonce_replayed = matches!(
            &revert,
            VaultError::OrchestratorReverted {
                reason: OrchestratorRevertReason::NonceReplayed { .. },
                ..
            }
        );

        let (Some(authorization), true) = (authorization, is_nonce_replayed)
        else {
            let classification =
                orchestrator_mint_failure_classification(&revert);
            warn!(
                target: "mint",
                issuer_request_id = %self.issuer_request_id,
                error = %revert,
                classification = ?classification,
                "Orchestrator mint confirmation failed"
            );
            self.record_classified_failure(
                ctx,
                revert.to_string(),
                classification,
            )
            .await?;
            return Ok(());
        };

        // A quantity that cannot be converted is deterministic for the
        // persisted mint: record a domain failure instead of a job error
        // apalis would re-drive forever.
        let amount = match quantity.to_u256_with_18_decimals() {
            Ok(amount) => amount,
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %error,
                    "Mint quantity cannot be converted to on-chain units"
                );
                self.record_classified_failure(
                    ctx,
                    error.to_string(),
                    MintFailureClassification::Unclassified,
                )
                .await?;
                return Ok(());
            }
        };

        // Two outcomes, never conflated (SPEC "Recipient Authorization" ->
        // "Nonce"): a log at the pair that disagrees on token/amount is
        // PROOF a different mint consumed it, while an empty scan alongside
        // the consumed nonce impeaches the chain view itself — an unknown
        // outcome that must not be recorded as consumed-by-other, because
        // this mint may well have landed.
        match vault_service
            .find_orchestrator_minted_log(MintedLogQuery {
                orchestrator,
                to,
                nonce: authorization.nonce,
                token: self.vault,
                amount,
                lookback_blocks: None,
            })
            .await
        {
            Ok(MintedLogScan::FullMatch(minted)) => {
                info!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %minted.tx_hash,
                    nonce = %minted.nonce,
                    shares_minted = %minted.shares_minted,
                    "Replayed nonce full-matched an earlier landed mint; \
                     recovering"
                );

                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordOrchestratorMintRecovered {
                            issuer_request_id: self.issuer_request_id.clone(),
                            tx_hash: minted.tx_hash,
                            nonce: minted.nonce,
                            shares_minted: minted.shares_minted,
                            block_number: minted.block_number,
                        },
                    )
                    .await?;

                self.enqueue_callback(ctx).await?;
            }
            Ok(MintedLogScan::Mismatch) => {
                error!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    to = %to,
                    nonce = %authorization.nonce,
                    token = %self.vault,
                    amount = %amount,
                    "Authorization nonce was consumed by a different mint; \
                     manual reconciliation required"
                );
                self.record_classified_failure(
                    ctx,
                    revert.to_string(),
                    MintFailureClassification::NonceConsumedByOtherMint,
                )
                .await?;
            }
            Ok(MintedLogScan::NotFound) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    to = %to,
                    nonce = %authorization.nonce,
                    token = %self.vault,
                    amount = %amount,
                    "Nonce is consumed but no Minted log was found at the \
                     pair; the chain view is untrusted — parking for \
                     reconciliation"
                );
                self.record_classified_failure(
                    ctx,
                    revert.to_string(),
                    MintFailureClassification::NonceReplayUnresolved,
                )
                .await?;
            }
            // A failed lookup proves nothing about whose mint consumed the
            // nonce — fail unclassified (retryable; resubmission just replays
            // the nonce again) rather than falsely classify.
            Err(lookup_error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %lookup_error,
                    "Minted-log lookup failed after a replayed nonce"
                );
                self.record_classified_failure(
                    ctx,
                    lookup_error.to_string(),
                    MintFailureClassification::Unclassified,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Records a `MintingFailed` and kicks recovery only for `Unclassified` —
    /// typed classifications are never auto-retried for SUBMISSION
    /// (`NonceConsumedByOtherMint` needs manual reconciliation;
    /// `NonceReplayUnresolved` is re-driven by recovery's scheduled
    /// reconciliation, not a submission retry; the logic-mismatch halts
    /// resolve environment-wide, not per-mint).
    async fn record_classified_failure(
        &self,
        ctx: &ConfirmMintContext,
        error: String,
        classification: MintFailureClassification,
    ) -> Result<(), MintJobError> {
        ctx.mint_store
            .send(
                &self.issuer_request_id,
                MintCommand::RecordMintFailed {
                    issuer_request_id: self.issuer_request_id.clone(),
                    error,
                    classification,
                },
            )
            .await?;

        if classification == MintFailureClassification::Unclassified {
            kick_mint_recovery(
                &ctx.pool,
                &ctx.apalis_pool,
                &self.issuer_request_id,
            )
            .await;
        }

        Ok(())
    }

    async fn enqueue_callback(
        &self,
        ctx: &ConfirmMintContext,
    ) -> Result<(), MintJobError> {
        ctx.callback_queue
            .clone()
            .push_with_idempotency_key(
                SendCallbackJob {
                    issuer_request_id: self.issuer_request_id.clone(),
                },
                self.issuer_request_id.to_string(),
            )
            .await?;

        Ok(())
    }
}

/// Sends the Alpaca completion callback for a minted, awaiting-callback mint.
///
/// Delivery is at-least-once: if `send_mint_callback` succeeds but recording
/// `RecordCallbackSent` fails, the re-driven job sends the callback again.
/// This matches the window the pre-jobs recovery flow already had (Alpaca is
/// called before `CallbackSent` is persisted there too); collapsing it needs
/// either a persisted send-intent event or a documented Alpaca-side dedup
/// guarantee, both of which are event-schema/spec decisions taken separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SendCallbackJob {
    pub(crate) issuer_request_id: IssuerMintRequestId,
}

pub(crate) struct SendCallbackContext {
    pub(crate) mint_store: Arc<Store<Mint>>,
    pub(crate) alpaca: Arc<dyn AlpacaService>,
}

impl Job<SendCallbackContext> for SendCallbackJob {
    type Output = ();
    type Error = MintJobError;

    async fn perform(
        &self,
        ctx: &SendCallbackContext,
    ) -> Result<(), MintJobError> {
        let Some(Mint::CallbackPending {
            tokenization_request_id,
            client_id,
            wallet,
            tx_hash,
            network,
            ..
        }) = ctx.mint_store.load(&self.issuer_request_id).await?
        else {
            return Ok(());
        };

        ctx.alpaca
            .send_mint_callback(MintCallbackRequest {
                tokenization_request_id,
                client_id,
                wallet_address: wallet,
                tx_hash,
                network,
            })
            .await?;

        ctx.mint_store
            .send(
                &self.issuer_request_id,
                MintCommand::RecordCallbackSent {
                    issuer_request_id: self.issuer_request_id.clone(),
                },
            )
            .await?;

        Ok(())
    }
}

/// Kicks the scheduled recovery loop immediately after a job recorded a
/// `MintingFailed`, so the first automatic retry does not wait for the
/// periodic reconciler. Enqueue failure is non-fatal: the reconciler
/// re-enqueues the mint on its next pass.
async fn kick_mint_recovery(
    pool: &Pool<Sqlite>,
    apalis_pool: &SqlitePool,
    issuer_request_id: &IssuerMintRequestId,
) {
    if let Err(error) = enqueue_scheduled_mint_recovery(
        pool,
        apalis_pool,
        issuer_request_id.clone(),
    )
    .await
    {
        warn!(
            target: "mint",
            issuer_request_id = %issuer_request_id,
            error = %error,
            "Failed to enqueue scheduled mint recovery after recorded failure"
        );
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, U256};
    use async_trait::async_trait;
    use cqrs_es::{AggregateError, DomainEvent};
    use event_sorcery::test_store;
    use std::any::type_name;
    use std::collections::HashMap;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::MintEvent;
    use crate::mint::api::test_utils::TestHarness;
    use crate::mint::recovery::MintRecoveryJob;
    use crate::mint::tests::{
        BOT, ORCHESTRATOR, VAULT, events_through_minting,
        events_through_tokens_minted, events_through_tx_submitted,
        orchestrator_events_through_minting,
        orchestrator_events_through_minting_authorized,
        orchestrator_events_through_tx_submitted, test_mint_authorization,
    };
    use crate::receipt_inventory::{
        BurnPlan, BurnTrackingError, CqrsReceiptService, ReceiptInventory,
        ReceiptLookupError, ReceiptRegistrationError, RecoveredReceipt,
    };
    use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
    use crate::test_utils::{
        ANVIL_CHAIN_ID, ETHEREUM_TEST_CHAIN_ID, logs_contain_at,
    };
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        NetworkVault, OrchestratorMintResult, OrchestratorMintedLog,
    };

    /// Seeds raw `Mint` events directly into the event store so job tests can
    /// start from any lifecycle state, mirroring the fixtures in
    /// `crate::mint::tests` and `crate::mint::recovery::tests`.
    async fn seed_mint_events(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        events: Vec<MintEvent>,
    ) {
        let aggregate_id = issuer_request_id.to_string();

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
            .bind(&aggregate_id)
            .bind(sequence)
            .bind(event_type)
            .bind(payload)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    fn submit_ctx(
        harness: &TestHarness,
        vault: Arc<dyn VaultService>,
    ) -> SubmitMintContext {
        SubmitMintContext {
            mint_store: harness.mint_store.clone(),
            vaults: NetworkVaultServices::with_single_vault(
                Network::Base,
                ANVIL_CHAIN_ID,
                vault,
            ),
            receipts: cqrs_receipts(&harness.pool),
            bot: BOT,
            confirm_queue: JobQueue::new(&harness.apalis_pool),
            callback_queue: JobQueue::new(&harness.apalis_pool),
            pool: harness.pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
        }
    }

    fn confirm_ctx(
        harness: &TestHarness,
        vault: Arc<dyn VaultService>,
        receipts: Arc<dyn ReceiptService>,
    ) -> ConfirmMintContext {
        ConfirmMintContext {
            mint_store: harness.mint_store.clone(),
            vaults: NetworkVaultServices::with_single_vault(
                Network::Base,
                ANVIL_CHAIN_ID,
                vault,
            ),
            receipts,
            callback_queue: JobQueue::new(&harness.apalis_pool),
            pool: harness.pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
        }
    }

    fn events_through_minting_on(
        issuer_request_id: &IssuerMintRequestId,
        network: Network,
    ) -> Vec<MintEvent> {
        let mut events = events_through_minting(issuer_request_id);
        if let MintEvent::Initiated { network: event_network, .. } =
            &mut events[0]
        {
            *event_network = network;
        }
        events
    }

    fn events_through_tx_intended(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let mut events = events_through_minting(issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: crate::vault::PreparedMintTx::valid_for_test(
                1,
                format!("mint-{issuer_request_id}"),
            ),
            intended_at: chrono::Utc::now(),
        });
        events
    }

    fn cqrs_receipts(pool: &Pool<Sqlite>) -> Arc<dyn ReceiptService> {
        Arc::new(CqrsReceiptService::new(Arc::new(test_store::<
            ReceiptInventory,
        >(pool.clone(), ()))))
    }

    async fn count_jobs(
        pool: &Pool<Sqlite>,
        job_type: &str,
        idempotency_key: &str,
    ) -> i64 {
        sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key = ?
            ",
        )
        .bind(job_type)
        .bind(idempotency_key)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// `ReceiptService` stub whose registration always fails, proving a
    /// registration failure cannot block `TokensMinted`. The remaining
    /// capabilities are inert (empty results) — the confirm job never uses
    /// them.
    struct FailingReceiptService;

    #[async_trait]
    impl ReceiptService for FailingReceiptService {
        async fn register_minted_receipt(
            &self,
            _params: MintedReceiptParams,
        ) -> Result<(), ReceiptRegistrationError> {
            Err(ReceiptRegistrationError::Aggregate(
                AggregateError::UnexpectedError(
                    "simulated registration failure".into(),
                ),
            ))
        }

        async fn for_burn(
            &self,
            _chain_id: u64,
            _vault: Address,
            _redemption_issuer_request_id: &IssuerRedemptionRequestId,
            _shares_to_burn: Shares,
            _dust: Shares,
        ) -> Result<BurnPlan, BurnTrackingError> {
            Ok(BurnPlan {
                allocations: vec![],
                total_burn: Shares::ZERO,
                dust: Shares::ZERO,
            })
        }

        async fn reserve_burn(
            &self,
            _chain_id: u64,
            _vault: Address,
            _redemption_issuer_request_id: IssuerRedemptionRequestId,
            _burns: Vec<BurnRecord>,
        ) -> Result<(), ReceiptRegistrationError> {
            Ok(())
        }

        async fn release_burn(
            &self,
            _chain_id: u64,
            _vault: Address,
            _redemption_issuer_request_id: IssuerRedemptionRequestId,
        ) -> Result<(), ReceiptRegistrationError> {
            Ok(())
        }

        async fn settle_burn(
            &self,
            _chain_id: u64,
            _vault: Address,
            _redemption_issuer_request_id: IssuerRedemptionRequestId,
        ) -> Result<(), ReceiptRegistrationError> {
            Ok(())
        }

        async fn reserved_redemptions(
            &self,
            _chain_id: u64,
            _vault: Address,
        ) -> Result<Vec<IssuerRedemptionRequestId>, ReceiptLookupError>
        {
            Ok(vec![])
        }

        async fn find_by_issuer_request_id(
            &self,
            _chain_id: u64,
            _vault: &Address,
            _issuer_request_id: &IssuerMintRequestId,
        ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError> {
            Ok(None)
        }
    }

    /// Multichain regression: a mint on Ethereum must call the Ethereum
    /// `VaultService`, not the Base one that happens to be the process default.
    #[tokio::test]
    async fn submit_mint_job_routes_vault_service_by_mint_network() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_minting_on(&issuer_request_id, Network::Ethereum),
        )
        .await;

        let base_vault = Arc::new(MockVaultService::new_submit_failure());
        let eth_vault = Arc::new(MockVaultService::new_success());
        let ctx = SubmitMintContext {
            mint_store: harness.mint_store.clone(),
            vaults: NetworkVaultServices::new(HashMap::from([
                (
                    Network::Base,
                    NetworkVault {
                        service: base_vault.clone(),
                        chain_id: ANVIL_CHAIN_ID,
                    },
                ),
                (
                    Network::Ethereum,
                    NetworkVault {
                        service: eth_vault.clone(),
                        chain_id: ETHEREUM_TEST_CHAIN_ID,
                    },
                ),
            ])),
            receipts: cqrs_receipts(&harness.pool),
            bot: BOT,
            confirm_queue: JobQueue::new(&harness.apalis_pool),
            callback_queue: JobQueue::new(&harness.apalis_pool),
            pool: harness.pool.clone(),
            apalis_pool: harness.apalis_pool.clone(),
        };

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ETHEREUM_TEST_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "Ethereum mint must submit via the Ethereum vault service, got: \
             {mint:?}"
        );
        assert_eq!(
            eth_vault.get_wallet_lock_call_count(),
            1,
            "Ethereum vault service must prepare/submit the mint"
        );
        assert_eq!(
            base_vault.get_wallet_lock_call_count(),
            0,
            "Base vault service must not be used for an Ethereum mint"
        );
    }

    /// A rejected submission must be recorded as a domain failure
    /// (`MintingFailed`) rather than surfacing as a job error apalis would
    /// re-drive forever.
    #[traced_test]
    #[tokio::test]
    async fn submit_mint_job_on_vault_failure_records_mint_failed() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_minting(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(MockVaultService::new_submit_failure());
        let ctx = submit_ctx(&harness, vault.clone());

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "a rejected submission must record MintingFailed, got: {mint:?}"
        );
        assert_eq!(
            vault.get_wallet_lock_call_count(),
            1,
            "mint preparation and submission must acquire the shared wallet lock"
        );

        let test = "submit_mint_job_on_vault_failure_records_mint_failed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Mint submission failed"]
        ));
    }

    #[tokio::test]
    async fn submit_mint_job_waits_for_another_persisted_wallet_intent() {
        let harness = TestHarness::new().await;
        let pending_id = IssuerMintRequestId::random();
        let mut pending_events = events_through_minting(&pending_id);
        pending_events.push(MintEvent::MintTxIntended {
            issuer_request_id: pending_id.clone(),
            prepared_tx: crate::vault::PreparedMintTx::valid_for_test(
                1,
                format!("mint-{pending_id}"),
            ),
            intended_at: chrono::Utc::now(),
        });
        seed_mint_events(&harness.pool, &pending_id, pending_events).await;

        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_minting(&issuer_request_id),
        )
        .await;
        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        SubmitMintJob {
            issuer_request_id,
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .expect_err("another unresolved signed transaction must defer minting");

        assert_eq!(
            vault.get_call_count(),
            0,
            "the blocked job must not prepare another signed transaction"
        );
    }

    #[tokio::test]
    async fn submit_mint_job_persists_signed_intent_before_resolution() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_minting(&issuer_request_id),
        )
        .await;
        let ctx =
            submit_ctx(&harness, Arc::new(MockVaultService::new_success()));

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let intent_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'Mint' AND aggregate_id = ? AND event_type = 'MintEvent::MintTxIntended'",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(intent_count, 1);
    }

    #[tokio::test]
    async fn submit_mint_job_retry_reuses_persisted_signed_intent() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: crate::vault::PreparedMintTx::valid_for_test(
                1,
                format!("mint-{issuer_request_id}"),
            ),
            intended_at: chrono::Utc::now(),
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "submission outcome unknown".to_string(),
            failed_at: chrono::Utc::now(),
            classification: MintFailureClassification::Unclassified,
        });
        events.push(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            started_at: chrono::Utc::now(),
        });
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;
        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            vault.get_call_count(),
            0,
            "recovery must rebroadcast the persisted bytes, not prepare a new transaction"
        );
        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(mint, Mint::TxSubmitted { .. }));
    }

    #[tokio::test]
    async fn tx_intended_submission_acquires_wallet_lock() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_intended(&issuer_request_id),
        )
        .await;
        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        SubmitMintJob {
            issuer_request_id,
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(vault.get_wallet_lock_call_count(), 1);
    }

    #[tokio::test]
    async fn tx_intended_with_existing_receipt_records_without_resubmitting() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_intended(&issuer_request_id),
        )
        .await;
        let receipts = cqrs_receipts(&harness.pool);
        let receipt_info = ReceiptInformation::new(
            super::super::TokenizationRequestId::new("tok-123"),
            issuer_request_id.clone(),
            super::super::UnderlyingSymbol::new("AAPL").unwrap(),
            crate::Quantity::new(rust_decimal::Decimal::from(100)),
            chrono::Utc::now(),
            None,
        );
        receipts
            .register_minted_receipt(MintedReceiptParams {
                chain_id: ANVIL_CHAIN_ID,
                vault: VAULT,
                receipt_id: ReceiptId::from(U256::from(7)),
                shares: Shares::new(U256::from(100)),
                block_number: 1_234,
                tx_hash: B256::ZERO,
                receipt_info_bytes: receipt_info.encode(None).unwrap(),
                receipt_info,
            })
            .await
            .unwrap();
        let vault = Arc::new(MockVaultService::new_success());
        let mut ctx = submit_ctx(&harness, vault.clone());
        ctx.receipts = receipts;

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(matches!(mint, Mint::CallbackPending { .. }));
        assert_eq!(vault.get_call_count(), 0);
    }

    /// A re-run of the submit job after the submission was already recorded
    /// must re-enqueue the confirm job (a crash may have dropped it) without
    /// re-submitting to the signing backend.
    #[tokio::test]
    async fn submit_mint_job_rerun_from_fireblocks_submitted_enqueues_confirm()
    {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let queued = count_jobs(
            &harness.pool,
            type_name::<ConfirmMintJob>(),
            &issuer_request_id.to_string(),
        )
        .await;
        assert_eq!(
            queued, 1,
            "a re-run from TxSubmitted must enqueue the confirm job"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "a re-run must not re-submit the mint to the signing backend"
        );
    }

    /// A failed on-chain confirmation must be recorded as a domain failure
    /// (`MintingFailed`) so recovery owns the retry.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_on_vault_failure_records_mint_failed() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        let ctx = confirm_ctx(
            &harness,
            Arc::new(MockVaultService::new_failure()),
            cqrs_receipts(&harness.pool),
        );

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            tx_id: TxId::Legacy("fb-1".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "a failed confirmation must record MintingFailed, got: {mint:?}"
        );

        let test = "confirm_mint_job_on_vault_failure_records_mint_failed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "On-chain deposit confirmation failed"]
        ));
    }

    /// A re-run of the confirm job after the mint was already confirmed must
    /// re-enqueue the callback job so the chain keeps moving.
    #[tokio::test]
    async fn confirm_mint_job_rerun_from_callback_pending_enqueues_callback() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tokens_minted(&issuer_request_id),
        )
        .await;

        let ctx = confirm_ctx(
            &harness,
            Arc::new(MockVaultService::new_success()),
            cqrs_receipts(&harness.pool),
        );

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            tx_id: TxId::Legacy("fb-1".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let queued = count_jobs(
            &harness.pool,
            type_name::<SendCallbackJob>(),
            &issuer_request_id.to_string(),
        )
        .await;
        assert_eq!(
            queued, 1,
            "a re-run from CallbackPending must enqueue the callback job"
        );
    }

    /// Receipt registration is best-effort: its failure must not block
    /// `TokensMinted` — the monitor/backfill rediscovers the receipt.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_receipt_registration_failure_does_not_block_tokens_minted()
     {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        let ctx = confirm_ctx(
            &harness,
            Arc::new(MockVaultService::new_success()),
            Arc::new(FailingReceiptService),
        );

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            tx_id: TxId::Legacy("fb-1".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "TokensMinted must be recorded despite the registration failure, \
             got: {mint:?}"
        );

        let test = "confirm_mint_job_receipt_registration_failure_does_not_block_tokens_minted";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Failed to register minted receipt"]
        ));
    }

    /// An orchestrator mint whose authorization has not arrived must be a
    /// silent no-op — no event, no vault call, no confirm job — so the mint
    /// stays in `Minting` until the liquidity bot delivers.
    #[traced_test]
    #[tokio::test]
    async fn submit_mint_job_orchestrator_defers_without_authorization() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            orchestrator_events_through_minting(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "an unauthorized orchestrator mint must stay in Minting, got: \
             {mint:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "an orchestrator mint must never fall back to the vault-direct \
             preparation"
        );
        assert_eq!(
            vault.orchestrator_mint_preparation_call_count(),
            0,
            "nothing may be signed before the authorization arrives"
        );
        assert_eq!(
            count_jobs(
                &harness.pool,
                type_name::<ConfirmMintJob>(),
                &issuer_request_id.to_string(),
            )
            .await,
            0,
            "no confirm job may be enqueued for a deferred submission"
        );

        let test = "submit_mint_job_orchestrator_defers_without_authorization";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "awaiting its recipient authorization"]
        ));
    }

    /// With the authorization persisted, the submit job prepares via
    /// `prepare_orchestrator_mint_tx` — carrying the anchored orchestrator
    /// address and the delivered authorization — and hands off to the
    /// confirm job like a vault-direct submission.
    #[tokio::test]
    async fn submit_mint_job_orchestrator_submits_via_orchestrator() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            orchestrator_events_through_minting_authorized(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "an authorized orchestrator mint must submit, got: {mint:?}"
        );

        let params = vault
            .get_last_orchestrator_mint_params()
            .expect("orchestrator preparation must record its params");
        assert_eq!(params.orchestrator, ORCHESTRATOR);
        assert_eq!(params.token, VAULT);
        assert_eq!(params.authorization, test_mint_authorization());
        assert_eq!(
            vault.get_call_count(),
            0,
            "the vault-direct preparation must not be invoked"
        );
        assert_eq!(
            count_jobs(
                &harness.pool,
                type_name::<ConfirmMintJob>(),
                &issuer_request_id.to_string(),
            )
            .await,
            1,
        );
    }

    /// Orchestrator confirmation records `OrchestratorTokensMinted` and never
    /// touches the receipt service — the orchestrator custodies the receipt.
    /// The failing receipt stub would log a warning if it were reached.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_orchestrator_records_without_receipts() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            orchestrator_events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        // The confirmed values must match the fixture's authorization nonce
        // and 18-decimal quantity — the record handler cross-checks them.
        let ctx = confirm_ctx(
            &harness,
            Arc::new(
                MockVaultService::new_success().with_orchestrator_mint_result(
                    OrchestratorMintResult {
                        tx_hash: B256::ZERO,
                        nonce: test_mint_authorization().nonce,
                        shares_minted: U256::from(
                            100_000_000_000_000_000_000u128,
                        ),
                        gas_used: 50_000,
                        block_number: 5_000,
                    },
                ),
            ),
            Arc::new(FailingReceiptService),
        );

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            tx_id: TxId::Legacy("fb-1".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                &mint,
                Mint::CallbackPending {
                    receipt_id: None,
                    mint_nonce: Some(_),
                    ..
                }
            ),
            "an orchestrator confirmation must record the nonce and no \
             receipt, got: {mint:?}"
        );
        assert_eq!(
            count_jobs(
                &harness.pool,
                type_name::<SendCallbackJob>(),
                &issuer_request_id.to_string(),
            )
            .await,
            1,
        );
        let test = "confirm_mint_job_orchestrator_records_without_receipts";
        assert!(
            !logs_contain_at!(
                Level::WARN,
                &[test, "Failed to register minted receipt"]
            ),
            "the receipt service must never be called for an orchestrator mint"
        );
    }

    /// A `NonceReplayed` revert whose `Minted` log full-matches completes the
    /// mint via `RecordOrchestratorMintRecovered` and moves on to the
    /// callback — no failure, no recovery kick.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_orchestrator_replayed_nonce_recovers() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            orchestrator_events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        let authorization = test_mint_authorization();
        let vault = Arc::new(
            MockVaultService::new_success()
                .with_orchestrator_mint_confirm_revert(
                    OrchestratorRevertReason::NonceReplayed {
                        to: Address::ZERO,
                        nonce: authorization.nonce,
                    },
                )
                .with_minted_log(OrchestratorMintedLog {
                    tx_hash: B256::ZERO,
                    nonce: authorization.nonce,
                    // The mock full-matches like the real lookup: the landed
                    // amount must equal the mint's 18-decimal share amount.
                    shares_minted: U256::from(100u64)
                        * U256::from(10u64).pow(U256::from(18u64)),
                    block_number: 777,
                }),
        );
        let ctx = confirm_ctx(&harness, vault, cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            tx_id: TxId::Legacy("fb-1".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(&mint, Mint::CallbackPending { mint_nonce: Some(_), .. }),
            "a full-matched replayed nonce must complete the mint, got: \
             {mint:?}"
        );
        assert_eq!(
            count_jobs(
                &harness.pool,
                type_name::<SendCallbackJob>(),
                &issuer_request_id.to_string(),
            )
            .await,
            1,
        );

        let test = "confirm_mint_job_orchestrator_replayed_nonce_recovers";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "full-matched an earlier landed mint"]
        ));
    }

    /// A replayed nonce whose `Minted` log at the pair disagrees on amount
    /// is the PROVEN mismatch — `NonceConsumedByOtherMint`, manual
    /// reconciliation, no recovery kick that would auto-retry it.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_nonce_consumed_by_other_mint_never_kicks() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            orchestrator_events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_orchestrator_mint_confirm_revert(
                    OrchestratorRevertReason::NonceReplayed {
                        to: Address::ZERO,
                        nonce: test_mint_authorization().nonce,
                    },
                )
                // The pair's one landing, under a DIFFERENT amount: the
                // scan's proven `Mismatch` verdict.
                .with_minted_log(OrchestratorMintedLog {
                    tx_hash: B256::ZERO,
                    nonce: test_mint_authorization().nonce,
                    shares_minted: U256::from(1u8),
                    block_number: 777,
                }),
        );
        let ctx = confirm_ctx(&harness, vault, cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            tx_id: TxId::Legacy("fb-1".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                &mint,
                Mint::MintingFailed {
                    classification:
                        MintFailureClassification::NonceConsumedByOtherMint,
                    ..
                }
            ),
            "a nonce consumed by another mint must fail classified, got: \
             {mint:?}"
        );
        assert_eq!(
            count_jobs(
                &harness.pool,
                type_name::<MintRecoveryJob>(),
                &issuer_request_id.to_string(),
            )
            .await,
            0,
            "a manual-reconciliation failure must not kick recovery"
        );

        let test = "confirm_mint_job_nonce_consumed_by_other_mint_never_kicks";
        assert!(logs_contain_at!(
            Level::ERROR,
            &[test, "consumed by a different mint"]
        ));
    }

    /// A replayed nonce with NO `Minted` log at the pair at all is the
    /// INCONCLUSIVE outcome — `NonceReplayUnresolved`, never conflated with
    /// the proven mismatch: the chain view itself is in doubt and this mint
    /// may well have landed. Parked without a recovery kick (submission can
    /// only revert); reconciliation owns the retry.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_unresolved_replay_parks_for_reconciliation() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            orchestrator_events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_orchestrator_mint_confirm_revert(
                    OrchestratorRevertReason::NonceReplayed {
                        to: Address::ZERO,
                        nonce: test_mint_authorization().nonce,
                    },
                ),
        );
        let ctx = confirm_ctx(&harness, vault, cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            tx_id: TxId::Legacy("fb-1".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                &mint,
                Mint::MintingFailed {
                    classification:
                        MintFailureClassification::NonceReplayUnresolved,
                    ..
                }
            ),
            "an empty scan alongside a consumed nonce must park as the \
             unresolved classification, got: {mint:?}"
        );
        assert_eq!(
            count_jobs(
                &harness.pool,
                type_name::<MintRecoveryJob>(),
                &issuer_request_id.to_string(),
            )
            .await,
            0,
            "an unresolved replay must not kick the submission retry"
        );

        let test =
            "confirm_mint_job_unresolved_replay_parks_for_reconciliation";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "no Minted log was found at the pair"]
        ));
    }

    /// Typed logic-mismatch halts are recorded without a recovery kick (they
    /// resolve environment-wide), while an unclassified revert still kicks
    /// the automatic retry.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_kicks_recovery_only_for_unclassified() {
        for (reason, expected_recovery_jobs, expected_classification) in [
            (
                OrchestratorRevertReason::VaultLogicMismatch,
                0,
                "VaultLogicMismatch",
            ),
            (OrchestratorRevertReason::Unknown, 1, "Unclassified"),
        ] {
            let harness = TestHarness::new().await;
            let issuer_request_id = IssuerMintRequestId::random();
            seed_mint_events(
                &harness.pool,
                &issuer_request_id,
                orchestrator_events_through_tx_submitted(&issuer_request_id),
            )
            .await;

            let vault = Arc::new(
                MockVaultService::new_success()
                    .with_orchestrator_mint_confirm_revert(reason),
            );
            let ctx =
                confirm_ctx(&harness, vault, cqrs_receipts(&harness.pool));

            ConfirmMintJob {
                issuer_request_id: issuer_request_id.clone(),
                vault: VAULT,
                chain_id: 1,
                tx_id: TxId::Legacy("fb-1".to_string()),
            }
            .perform(&ctx)
            .await
            .unwrap();

            let mint = harness
                .mint_store
                .load(&issuer_request_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(&mint, Mint::MintingFailed { .. }),
                "reason {reason:?} must record MintingFailed, got: {mint:?}"
            );
            assert_eq!(
                count_jobs(
                    &harness.pool,
                    type_name::<MintRecoveryJob>(),
                    &issuer_request_id.to_string(),
                )
                .await,
                expected_recovery_jobs,
                "recovery kick mismatch for {reason:?}"
            );
            assert!(
                logs_contain_at!(
                    Level::WARN,
                    &[
                        "Orchestrator mint confirmation failed",
                        expected_classification
                    ]
                ),
                "reason {reason:?} must WARN with its classification"
            );
        }
    }

    /// The callback job only acts on `CallbackPending`; from any other state it
    /// must be a no-op that never reaches Alpaca.
    #[tokio::test]
    async fn send_callback_job_noop_when_not_in_callback_pending() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_minting(&issuer_request_id),
        )
        .await;

        let alpaca = Arc::new(MockAlpacaService::new_success());
        let ctx = SendCallbackContext {
            mint_store: harness.mint_store.clone(),
            alpaca: alpaca.clone(),
        };

        SendCallbackJob { issuer_request_id: issuer_request_id.clone() }
            .perform(&ctx)
            .await
            .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "a callback job outside CallbackPending must not change state, \
             got: {mint:?}"
        );
        assert_eq!(
            alpaca.get_call_count(),
            0,
            "a callback job outside CallbackPending must not call Alpaca"
        );
    }
}
