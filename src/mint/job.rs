//! Durable jobs that perform the `Mint` aggregate's external side effects.
//!
//! Each job runs one step of the on-chain mint flow off the command handler:
//! it performs the external call (`vault.submit_mint` / `vault.confirm_mint` /
//! `alpaca.send_mint_callback`), reports the result back as an idempotent
//! outcome command (`RecordFireblocksSubmitted` / `RecordTokensMinted` /
//! `RecordCallbackSent` / `RecordMintFailed`), and enqueues the next step. The
//! handlers stay pure; the jobs are the only place I/O happens.
//!
//! Jobs are **drainer-style** (no apalis retry layer): a domain failure becomes
//! a `MintingFailed` event that the recovery budget loop retries on its own
//! schedule, and an infrastructure failure surfaces as a job error that apalis
//! re-drives. Re-runs are safe — `submit_mint` derives a deterministic
//! `external_tx_id` from the `issuer_request_id` (Fireblocks dedups duplicate
//! submissions), and every outcome command is a no-op once its event is
//! recorded.

use std::sync::Arc;

use alloy::primitives::Address;
use event_sorcery::{SendError, Store};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{IssuerMintRequestId, Mint, MintCommand};
use crate::QuantityConversionError;
use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};
use crate::jobs::{Job, JobQueue, QueuePushError};
use crate::receipt_inventory::{
    MintedReceiptParams, ReceiptId, ReceiptService, Shares,
};
use crate::vault::{ReceiptInformation, VaultService};

/// Failure of a mint side-effect job. A domain rejection is recorded as a
/// `MintingFailed` event instead (see the module docs); these variants are the
/// infrastructure failures that make apalis re-drive the job.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MintJobError {
    #[error(transparent)]
    Store(#[from] SendError<Mint>),
    #[error(transparent)]
    Quantity(#[from] QuantityConversionError),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
    #[error(transparent)]
    Alpaca(#[from] AlpacaError),
}

/// Submits the on-chain mint to the signing backend, then hands off to
/// [`ConfirmMintJob`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SubmitMintJob {
    pub(crate) issuer_request_id: IssuerMintRequestId,
    pub(crate) vault: Address,
}

pub(crate) struct SubmitMintContext {
    pub(crate) mint_store: Arc<Store<Mint>>,
    pub(crate) vault: Arc<dyn VaultService>,
    pub(crate) bot: Address,
    pub(crate) confirm_queue: JobQueue<ConfirmMintJob>,
}

impl Job<SubmitMintContext> for SubmitMintJob {
    type Output = ();
    type Error = MintJobError;

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
                wallet,
                journal_confirmed_at,
                ..
            } => {
                let assets = quantity.to_u256_with_18_decimals()?;
                let receipt_info = ReceiptInformation::new(
                    tokenization_request_id.clone(),
                    self.issuer_request_id.clone(),
                    underlying.clone(),
                    quantity.clone(),
                    *journal_confirmed_at,
                    None,
                );

                match ctx
                    .vault
                    .submit_mint(
                        self.vault,
                        assets,
                        ctx.bot,
                        *wallet,
                        receipt_info,
                        None,
                    )
                    .await
                {
                    Ok(submitted) => {
                        ctx.mint_store
                            .send(
                                &self.issuer_request_id,
                                MintCommand::RecordFireblocksSubmitted {
                                    issuer_request_id: self
                                        .issuer_request_id
                                        .clone(),
                                    external_tx_id: submitted.external_tx_id,
                                    fireblocks_tx_id: submitted
                                        .fireblocks_tx_id
                                        .clone(),
                                },
                            )
                            .await?;

                        self.enqueue_confirm(ctx, submitted.fireblocks_tx_id)
                            .await?;
                    }
                    Err(error) => {
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
                                    issuer_request_id: self
                                        .issuer_request_id
                                        .clone(),
                                    error: error.to_string(),
                                },
                            )
                            .await?;
                    }
                }
            }
            // A re-run after the submission was already recorded: the
            // confirm job may not have been enqueued before a crash, so
            // keep the chain moving.
            Mint::FireblocksSubmitted { fireblocks_tx_id, .. } => {
                self.enqueue_confirm(ctx, fireblocks_tx_id.clone()).await?;
            }
            // Past confirmation, failed (recovery owns retries), or terminal.
            _ => {}
        }

        Ok(())
    }
}

impl SubmitMintJob {
    async fn enqueue_confirm(
        &self,
        ctx: &SubmitMintContext,
        fireblocks_tx_id: String,
    ) -> Result<(), MintJobError> {
        ctx.confirm_queue
            .clone()
            .push_with_idempotency_key(
                ConfirmMintJob {
                    issuer_request_id: self.issuer_request_id.clone(),
                    vault: self.vault,
                    fireblocks_tx_id,
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
    pub(crate) fireblocks_tx_id: String,
}

pub(crate) struct ConfirmMintContext {
    pub(crate) mint_store: Arc<Store<Mint>>,
    pub(crate) vault: Arc<dyn VaultService>,
    pub(crate) receipts: Arc<dyn ReceiptService>,
    pub(crate) callback_queue: JobQueue<SendCallbackJob>,
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

        let Mint::FireblocksSubmitted {
            tokenization_request_id,
            quantity,
            underlying,
            journal_confirmed_at,
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

        match ctx.vault.confirm_mint(&self.fireblocks_tx_id).await {
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
                        },
                    )
                    .await?;
            }
        }

        Ok(())
    }
}

impl ConfirmMintJob {
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
