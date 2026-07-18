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

use alloy::primitives::{Address, U256};
use apalis_sqlite::SqlitePool;
use chrono::{DateTime, Utc};
use event_sorcery::{SendError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::recovery::{enqueue_scheduled_mint_recovery, release_terminal_job};
use super::{
    IssuerMintRequestId, Mint, MintCommand, Quantity, TokenizationRequestId,
    UnderlyingSymbol, has_unresolved_signer_intent,
};
use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};
use crate::burn_excess::has_unresolved_excess_burn_intent;
use crate::jobs::{Job, JobQueue, QueuePushError, job_type};
use crate::receipt_inventory::{
    MintedReceiptParams, ReceiptId, ReceiptLookupError, ReceiptService, Shares,
};
use crate::tokenized_asset::Network;
use crate::vault::{
    MintTxStatus, NetworkVaultServices, PreparedMintTx, ReceiptInformation,
    TxId, UnconfiguredNetworkError, VaultError, VaultService,
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
                ..
            } => {
                self.submit_from_minting(
                    ctx,
                    &mint,
                    SubmitFromMintingParams {
                        tokenization_request_id,
                        quantity,
                        underlying,
                        network: *network,
                        wallet: *wallet,
                        journal_confirmed_at: *journal_confirmed_at,
                    },
                )
                .await?;
            }
            // Crash recovery for a prepare-then-submit job that persisted
            // intent via the inline PrepareMint path before the job rewrite.
            // Classify first (same predicate as `resolve_prepared_for_submit`):
            // rebroadcast only when StillMineable/MinedSuccess; terminal
            // dead/revert records MintingFailed so recovery may replace;
            // uncertain observation preserves MintIntended (no MintingFailed).
            Mint::TxIntended { prepared_tx, network, .. } => {
                self.submit_from_tx_intended(ctx, prepared_tx, *network)
                    .await?;
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

struct SubmitFromMintingParams<'a> {
    tokenization_request_id: &'a TokenizationRequestId,
    quantity: &'a Quantity,
    underlying: &'a UnderlyingSymbol,
    network: Network,
    wallet: Address,
    journal_confirmed_at: DateTime<Utc>,
}

struct ResolvePreparedParams {
    assets: U256,
    wallet: Address,
    receipt_info: ReceiptInformation,
    external_tx_id: Option<String>,
}

impl SubmitMintJob {
    /// Refuses while another persisted intent holds this signer's nonce domain.
    ///
    /// The `active_signer_intents` reservation is network-keyed, so one check
    /// covers competing mint AND redemption-burn intents on this signer.
    /// `BurnExcess` holds no reservation row there, so it needs its own check.
    async fn refuse_behind_wallet_intents(
        &self,
        ctx: &SubmitMintContext,
        network: Network,
        stage: &'static str,
    ) -> Result<(), MintJobError> {
        let unresolved_intent = has_unresolved_signer_intent(
            &ctx.pool,
            network,
            Some(&self.issuer_request_id),
        )
        .await?;
        let unresolved_excess =
            has_unresolved_excess_burn_intent(&ctx.pool, None).await?;
        if !unresolved_intent && !unresolved_excess {
            return Ok(());
        }

        debug!(target: "mint",
            issuer_request_id = %self.issuer_request_id,
            unresolved_intent,
            unresolved_excess,
            stage,
            "Deferring mint behind another persisted wallet intent"
        );

        Err(MintJobError::UnresolvedWalletIntent {
            issuer_request_id: self.issuer_request_id.clone(),
        })
    }

    /// Rebroadcast a legacy `TxIntended` prepared identity under the wallet
    /// lock, after burn-parity classification (aligned with
    /// [`Self::resolve_prepared_for_submit`]).
    async fn submit_from_tx_intended(
        &self,
        ctx: &SubmitMintContext,
        prepared_tx: &PreparedMintTx,
        network: Network,
    ) -> Result<(), MintJobError> {
        if self.record_existing_receipt(ctx).await? {
            return Ok(());
        }

        let Some(vault) = self.resolve_vault_service(ctx, network).await?
        else {
            return Ok(());
        };

        let wallet_guard = vault.lock_wallet().await;
        self.refuse_behind_wallet_intents(ctx, network, "submission").await?;

        let owner = match prepared_tx.recover_signer() {
            Ok(owner) => owner,
            Err(error) => {
                // Corrupt envelope: cannot classify death/liveness; preserve
                // MintIntended for operator/restart (fail closed).
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared_tx.hash,
                    error = %error,
                    "Failed to recover signer from prepared mint; \
                     preserving MintIntended"
                );
                drop(wallet_guard);
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
                return Ok(());
            }
        };
        let classification = vault.classify_mint_tx(owner, prepared_tx).await;

        match classification {
            Ok(MintTxStatus::StillMineable | MintTxStatus::MinedSuccess) => {
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
                        // TxIntended always has live prepared bytes at submit.
                        self.record_submission_failure(
                            ctx,
                            error,
                            PreparedLiveness::Live,
                        )
                        .await?;
                    }
                }
            }
            Ok(MintTxStatus::MinedReverted | MintTxStatus::ProvablyDead) => {
                // Prefer inventory before terminal MintingFailed: a receipt
                // means the deposit already succeeded.
                if !self.record_existing_receipt(ctx).await? {
                    // TOCTOU: re-classify under the same wallet guard before
                    // recording failure that authorizes replacement prepare.
                    let recheck = match vault
                        .classify_mint_tx(owner, prepared_tx)
                        .await
                    {
                        Ok(status) => status,
                        Err(error) => {
                            warn!(
                                target: "mint",
                                issuer_request_id = %self.issuer_request_id,
                                tx_hash = %prepared_tx.hash,
                                nonce = prepared_tx.nonce,
                                error = %error,
                                "TxIntended recheck uncertain; preserving MintIntended"
                            );
                            drop(wallet_guard);
                            kick_mint_recovery(
                                &ctx.pool,
                                &ctx.apalis_pool,
                                &self.issuer_request_id,
                            )
                            .await;
                            return Ok(());
                        }
                    };
                    if matches!(
                        recheck,
                        MintTxStatus::MinedReverted
                            | MintTxStatus::ProvablyDead
                    ) {
                        warn!(
                            target: "mint",
                            issuer_request_id = %self.issuer_request_id,
                            tx_hash = %prepared_tx.hash,
                            nonce = prepared_tx.nonce,
                            recheck = ?recheck,
                            "TxIntended prepared identity terminal; recording MintingFailed"
                        );
                        ctx.mint_store
                            .send(
                                &self.issuer_request_id,
                                MintCommand::RecordMintFailed {
                                    issuer_request_id: self
                                        .issuer_request_id
                                        .clone(),
                                    error: format!(
                                        "Prepared mint terminal before \
                                         rebroadcast: {recheck:?}"
                                    ),
                                },
                            )
                            .await?;
                        kick_mint_recovery(
                            &ctx.pool,
                            &ctx.apalis_pool,
                            &self.issuer_request_id,
                        )
                        .await;
                    } else {
                        warn!(
                            target: "mint",
                            issuer_request_id = %self.issuer_request_id,
                            tx_hash = %prepared_tx.hash,
                            nonce = prepared_tx.nonce,
                            recheck = ?recheck,
                            "TxIntended recheck no longer terminal; preserving MintIntended"
                        );
                        drop(wallet_guard);
                        kick_mint_recovery(
                            &ctx.pool,
                            &ctx.apalis_pool,
                            &self.issuer_request_id,
                        )
                        .await;
                        return Ok(());
                    }
                }
            }
            Err(error) => {
                // Uncertain classification: keep MintIntended live; never
                // MintingFailed (would authorize a second deposit).
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared_tx.hash,
                    nonce = prepared_tx.nonce,
                    error = %error,
                    "TxIntended classification uncertain; preserving MintIntended"
                );
                drop(wallet_guard);
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
                return Ok(());
            }
        }

        drop(wallet_guard);
        Ok(())
    }

    /// Prepare (or rebroadcast) and submit while the aggregate is `Minting`.
    /// Classification of any live prepared identity gates replacement under
    /// the wallet guard so uncertain observation never signs a second deposit.
    async fn submit_from_minting(
        &self,
        ctx: &SubmitMintContext,
        mint: &Mint,
        params: SubmitFromMintingParams<'_>,
    ) -> Result<(), MintJobError> {
        if self.record_existing_receipt(ctx).await? {
            return Ok(());
        }

        let Some(vault) =
            self.resolve_vault_service(ctx, params.network).await?
        else {
            return Ok(());
        };

        // A quantity that cannot be converted is deterministic for the
        // persisted mint: record a domain failure instead of returning a job
        // error apalis would re-drive forever.
        let assets = match params.quantity.to_u256_with_18_decimals() {
            Ok(assets) => assets,
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %error,
                    "Mint quantity cannot be converted to on-chain units"
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

                return Ok(());
            }
        };
        let receipt_info = ReceiptInformation::new(
            params.tokenization_request_id.clone(),
            self.issuer_request_id.clone(),
            params.underlying.clone(),
            params.quantity.clone(),
            params.journal_confirmed_at,
            None,
        );

        let external_tx_id = mint
            .retry_submission_external_tx_id()
            .map(super::MintExternalTxId::into_string);

        let wallet_guard = vault.lock_wallet().await;
        self.refuse_behind_wallet_intents(ctx, params.network, "preparation")
            .await?;

        let Some(prepared) = self
            .resolve_prepared_for_submit(
                ctx,
                vault.as_ref(),
                mint,
                ResolvePreparedParams {
                    assets,
                    wallet: params.wallet,
                    receipt_info,
                    external_tx_id,
                },
            )
            .await?
        else {
            // Every `None` arm preserved the existing identity instead of
            // submitting. Re-drive recovery so re-observation is not left to
            // the periodic reconciler; the enqueue dedups on the mint's
            // idempotency key, so arms that already kicked cost nothing.
            drop(wallet_guard);
            kick_mint_recovery(
                &ctx.pool,
                &ctx.apalis_pool,
                &self.issuer_request_id,
            )
            .await;
            return Ok(());
        };

        match vault.submit_mint(&prepared).await {
            Ok(submitted) => {
                let tx_id = submitted.tx_id;
                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordTxSubmitted {
                            issuer_request_id: self.issuer_request_id.clone(),
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
                // Submit always has live prepared bytes (existing or just signed).
                self.record_submission_failure(
                    ctx,
                    error,
                    PreparedLiveness::Live,
                )
                .await?;
            }
        }
        drop(wallet_guard);
        Ok(())
    }

    /// Resolve prepared bytes: rebroadcast same hash when still mineable /
    /// mined success; prepare replacement only after terminal dead/revert with
    /// a wallet-guard TOCTOU recheck; `None` means fail closed (caller leaves
    /// state unchanged).
    async fn resolve_prepared_for_submit(
        &self,
        ctx: &SubmitMintContext,
        vault: &dyn VaultService,
        mint: &Mint,
        params: ResolvePreparedParams,
    ) -> Result<Option<PreparedMintTx>, MintJobError> {
        if let Some(existing) = mint.pending_prepared_tx() {
            let owner = match existing.recover_signer() {
                Ok(owner) => owner,
                Err(error) => {
                    // Corrupt envelope: cannot prove death; fail closed.
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        tx_hash = %existing.hash,
                        nonce = existing.nonce,
                        error = %error,
                        "Failed to recover signer from prepared mint; not replacing"
                    );
                    return Ok(None);
                }
            };
            return match vault.classify_mint_tx(owner, &existing).await {
                Ok(
                    MintTxStatus::StillMineable | MintTxStatus::MinedSuccess,
                ) => Ok(Some(existing)),
                Ok(
                    MintTxStatus::MinedReverted | MintTxStatus::ProvablyDead,
                ) => {
                    // TOCTOU: re-classify immediately under the same wallet
                    // guard before signing a replacement. A concurrent mine
                    // between the first classify and prepare must abort.
                    let recheck = match vault
                        .classify_mint_tx(owner, &existing)
                        .await
                    {
                        Ok(status) => status,
                        Err(error) => {
                            warn!(
                                target: "mint",
                                issuer_request_id = %self.issuer_request_id,
                                tx_hash = %existing.hash,
                                nonce = existing.nonce,
                                error = %error,
                                "Mint prepare-path recheck uncertain; not replacing"
                            );
                            return Ok(None);
                        }
                    };
                    if !matches!(
                        recheck,
                        MintTxStatus::MinedReverted
                            | MintTxStatus::ProvablyDead
                    ) {
                        warn!(
                            target: "mint",
                            issuer_request_id = %self.issuer_request_id,
                            tx_hash = %existing.hash,
                            nonce = existing.nonce,
                            recheck = ?recheck,
                            "Mint prepare-path recheck no longer terminal; not replacing"
                        );
                        return Ok(None);
                    }

                    // Inventory may have discovered a successful deposit for
                    // this issuer_request_id while we classified death of the
                    // prior identity — never prepare if a receipt already exists.
                    // Re-check under the wallet lock immediately before signing.
                    if self.record_existing_receipt(ctx).await? {
                        return Ok(None);
                    }

                    let prepared = match vault
                        .prepare_mint_tx(
                            self.vault,
                            params.assets,
                            ctx.bot,
                            params.wallet,
                            params.receipt_info,
                            params.external_tx_id,
                        )
                        .await
                    {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            // Prior prepared identity is still live on the mint.
                            self.record_submission_failure(
                                ctx,
                                error,
                                PreparedLiveness::Live,
                            )
                            .await?;
                            return Ok(None);
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
                    Ok(Some(prepared))
                }
                Err(error) => {
                    // Uncertain classification: keep the old intent live;
                    // never prepare a second deposit.
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        tx_hash = %existing.hash,
                        nonce = existing.nonce,
                        error = %error,
                        "Mint prepared-intent classification uncertain; \
                         preserving existing identity"
                    );
                    Ok(None)
                }
            };
        }

        // Defense in depth: post-submit identity without prepared bytes must
        // never free-prepare (recovery also fail-closes this path).
        if mint.has_unclassifiable_post_intent_identity() {
            error!(
                target: "mint",
                issuer_request_id = %self.issuer_request_id,
                tx_id = ?mint.latest_known_tx_id(),
                "Submit path refused free-prepare for post-submit identity \
                 without prepared_tx"
            );
            return Ok(None);
        }

        // Under wallet lock (caller holds it): re-check inventory immediately
        // before free-prepare so a concurrent discovery cannot race a second
        // deposit signature.
        if self.record_existing_receipt(ctx).await? {
            return Ok(None);
        }

        let prepared = match vault
            .prepare_mint_tx(
                self.vault,
                params.assets,
                ctx.bot,
                params.wallet,
                params.receipt_info,
                params.external_tx_id,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                // Free-prepare: no live prepared identity yet.
                self.record_submission_failure(
                    ctx,
                    error,
                    PreparedLiveness::None,
                )
                .await?;
                return Ok(None);
            }
        };

        ctx.mint_store
            .send(
                &self.issuer_request_id,
                MintCommand::RecordTxIntended {
                    issuer_request_id: self.issuer_request_id.clone(),
                    prepared_tx: prepared.clone(),
                },
            )
            .await?;
        Ok(Some(prepared))
    }

    async fn record_existing_receipt(
        &self,
        ctx: &SubmitMintContext,
    ) -> Result<bool, MintJobError> {
        record_existing_receipt_from_inventory(
            ctx.receipts.as_ref(),
            ctx.mint_store.as_ref(),
            &ctx.callback_queue,
            self.chain_id,
            self.vault,
            &self.issuer_request_id,
        )
        .await
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
                        },
                    )
                    .await?;
                Ok(None)
            }
        }
    }

    /// Records a domain failure after a vault prepare/submit error.
    ///
    /// `prepared_liveness` is the caller's knowledge at the failure site — do
    /// **not** re-load the mint to re-derive it. A load failure or race must not
    /// turn an uncertain broadcast of known-live prepared bytes into
    /// `MintingFailed` (that authorizes a replacement deposit).
    async fn record_submission_failure(
        &self,
        ctx: &SubmitMintContext,
        error: VaultError,
        prepared_liveness: PreparedLiveness,
    ) -> Result<(), MintJobError> {
        // Uncertain broadcast while a prepared identity is already live must
        // preserve MintIntended / leave the hash mineable — never MintingFailed
        // (SPEC: uncertain broadcast rebroadcasts the same bytes). Definitive
        // submit rejections (e.g. mock InvalidReceipt) still record failure.
        if prepared_liveness == PreparedLiveness::Live
            && is_uncertain_broadcast_error(&error)
        {
            warn!(
                target: "mint",
                issuer_request_id = %self.issuer_request_id,
                error = %error,
                "Uncertain mint broadcast with live prepared identity; \
                 preserving MintIntended"
            );
            // Preserving without re-driving would leave the mint in
            // TxIntended with no confirm job and no recovery job, waiting on
            // the periodic reconciler — the same reason every other preserve
            // arm here kicks.
            kick_mint_recovery(
                &ctx.pool,
                &ctx.apalis_pool,
                &self.issuer_request_id,
            )
            .await;
            return Ok(());
        }

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

    /// Enqueues [`ConfirmMintJob`], first freeing any TERMINAL prior confirm
    /// row for this mint (same pattern as recovery `enqueue_confirm`). Without
    /// the release, a prior `Done` confirm holds the idempotency key and apalis
    /// silently drops the re-enqueue — stranding a mint still in `TxSubmitted`.
    async fn enqueue_confirm(
        &self,
        ctx: &SubmitMintContext,
        tx_id: TxId,
    ) -> Result<(), MintJobError> {
        release_terminal_job(
            &ctx.pool,
            job_type::<ConfirmMintJob>(),
            &self.issuer_request_id.to_string(),
        )
        .await?;

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

        match &mint {
            Mint::TxSubmitted {
                tokenization_request_id,
                quantity,
                underlying,
                network,
                journal_confirmed_at,
                prepared_tx,
                tx_id: stored_tx_id,
                ..
            } => {
                self.confirm_while_tx_submitted(
                    ctx,
                    ConfirmWhileTxSubmitted {
                        tokenization_request_id: tokenization_request_id
                            .clone(),
                        quantity: quantity.clone(),
                        underlying: underlying.clone(),
                        network: *network,
                        journal_confirmed_at: *journal_confirmed_at,
                        prepared_tx: prepared_tx.clone(),
                        stored_tx_id: stored_tx_id.clone(),
                    },
                )
                .await?;
            }
            // Recovery enqueues confirm for a failed mint whose predecessor
            // still has a known tx_id (legacy post-submit without prepared
            // bytes, or re-observe after an earlier fail-closed path). Must
            // actually poll — never free-prepare from this job.
            Mint::MintingFailed {
                tokenization_request_id,
                quantity,
                underlying,
                network,
                journal_confirmed_at,
                ..
            } => {
                if self.matches_failed_identity(&mint) {
                    self.confirm_while_minting_failed(
                        ctx,
                        ConfirmWhileMintingFailed {
                            tokenization_request_id: tokenization_request_id
                                .clone(),
                            quantity: quantity.clone(),
                            underlying: underlying.clone(),
                            network: *network,
                            journal_confirmed_at: *journal_confirmed_at,
                            prepared_tx: mint.pending_prepared_tx(),
                        },
                    )
                    .await?;
                }
            }
            // A re-run after the mint was already confirmed: keep the chain
            // moving if it is awaiting its callback.
            Mint::CallbackPending { .. } => {
                self.enqueue_callback(ctx).await?;
            }
            Mint::Initiated { .. }
            | Mint::JournalConfirmed { .. }
            | Mint::JournalRejected { .. }
            | Mint::Minting { .. }
            | Mint::TxIntended { .. }
            | Mint::Completed { .. }
            | Mint::Closed { .. } => {}
        }

        Ok(())
    }
}

struct ConfirmWhileTxSubmitted {
    tokenization_request_id: TokenizationRequestId,
    quantity: Quantity,
    underlying: UnderlyingSymbol,
    network: Network,
    journal_confirmed_at: DateTime<Utc>,
    prepared_tx: Option<PreparedMintTx>,
    /// Aggregate's current submission identity; stale confirm jobs (older
    /// `tx_id` after a replacement) must not record `MintingFailed` for the
    /// successor attempt.
    stored_tx_id: TxId,
}

/// Owned fields from `Mint::MintingFailed` needed for re-observe + receipt
/// registration. Passing them explicitly avoids re-destructuring `mint` and
/// silently skipping registration if the match fails.
struct ConfirmWhileMintingFailed {
    tokenization_request_id: TokenizationRequestId,
    quantity: Quantity,
    underlying: UnderlyingSymbol,
    network: Network,
    journal_confirmed_at: DateTime<Utc>,
    prepared_tx: Option<PreparedMintTx>,
}

/// Whether a prepared mint identity was already live when a prepare/submit
/// error occurred. Call-site knowledge only — never re-load the mint to
/// re-derive this (a load race must not turn uncertain broadcast into
/// `MintingFailed`).
///
/// - [`Self::Live`]: replace-prepare after terminal prior identity, or
///   broadcast of bytes already on the mint / just recorded → preserve
///   `MintIntended` on uncertain broadcast.
/// - [`Self::None`]: free-prepare with no prepared identity yet → domain
///   `MintingFailed` is allowed so the retry schedule advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedLiveness {
    Live,
    None,
}

/// Broadcast-path uncertainty: node may already hold the signed tx. Never
/// `MintingFailed` while a prepared identity is live. Includes hash mismatch
/// after broadcast — the node may still mine the submitted envelope.
///
/// Exhaustive over [`VaultError`]: any new variant must be classified
/// deliberately (fail closed for unknown definitive-ness).
const fn is_uncertain_broadcast_error(error: &VaultError) -> bool {
    match error {
        VaultError::ConfirmationPending { .. }
        | VaultError::PendingTransaction(_)
        | VaultError::Rpc(_)
        | VaultError::ContradictoryDeathSignals { .. }
        | VaultError::BroadcastHashMismatch { .. } => true,
        VaultError::InvalidReceipt
        | VaultError::MissingBlockNumber { .. }
        | VaultError::EventNotFound { .. }
        | VaultError::Reverted { .. }
        | VaultError::OrchestratorReverted { .. }
        | VaultError::NotABurn { .. }
        | VaultError::BurnedEventMismatch { .. }
        | VaultError::PreparedMintHashMismatch { .. }
        | VaultError::PreparedMintNonceMismatch { .. }
        | VaultError::PreparedMintSignerMismatch { .. }
        | VaultError::PreparedBurnHashMismatch { .. }
        | VaultError::PreparedBurnNonceMismatch { .. }
        | VaultError::PreparedBurnSignerMismatch { .. }
        | VaultError::BurnReplacementDestinationMismatch { .. }
        | VaultError::BurnReplacementValueMismatch { .. }
        | VaultError::BurnReplacementInputMismatch { .. }
        | VaultError::SignerRecovery(_)
        | VaultError::Eip2718(_)
        | VaultError::Contract(_)
        | VaultError::ReceiptEncode(_)
        | VaultError::SendableTxErr(_) => false,
    }
}

/// Confirm-path uncertainty / invalid observation shapes. Fail closed — stay
/// `TxSubmitted`, never auto-replace. Includes receipt-shape errors that are
/// not mined-revert proofs.
///
/// Exhaustive over [`VaultError`]: new variants must choose uncertain vs
/// definitive deliberately.
const fn is_uncertain_confirm_observation(error: &VaultError) -> bool {
    match error {
        VaultError::ConfirmationPending { .. }
        | VaultError::PendingTransaction(_)
        | VaultError::Rpc(_)
        | VaultError::ContradictoryDeathSignals { .. }
        | VaultError::InvalidReceipt
        | VaultError::MissingBlockNumber { .. } => true,
        VaultError::EventNotFound { .. }
        | VaultError::Reverted { .. }
        | VaultError::OrchestratorReverted { .. }
        | VaultError::NotABurn { .. }
        // Burn-confirm-only variant, unreachable on the mint paths; grouped
        // with the definitive observations so nothing uncertain-retries an
        // integrity anomaly.
        | VaultError::BurnedEventMismatch { .. }
        | VaultError::BroadcastHashMismatch { .. }
        | VaultError::PreparedMintHashMismatch { .. }
        | VaultError::PreparedMintNonceMismatch { .. }
        | VaultError::PreparedMintSignerMismatch { .. }
        | VaultError::PreparedBurnHashMismatch { .. }
        | VaultError::PreparedBurnNonceMismatch { .. }
        | VaultError::PreparedBurnSignerMismatch { .. }
        | VaultError::BurnReplacementDestinationMismatch { .. }
        | VaultError::BurnReplacementValueMismatch { .. }
        | VaultError::BurnReplacementInputMismatch { .. }
        | VaultError::SignerRecovery(_)
        | VaultError::Eip2718(_)
        | VaultError::Contract(_)
        | VaultError::ReceiptEncode(_)
        | VaultError::SendableTxErr(_) => false,
    }
}

impl ConfirmMintJob {
    /// Whether this job's `tx_id` is the mint's latest known submission (or
    /// the prepared envelope hash when that is the only failed identity).
    fn matches_failed_identity(&self, mint: &Mint) -> bool {
        if mint.latest_known_tx_id().as_ref() == Some(&self.tx_id) {
            return true;
        }

        mint.pending_prepared_tx()
            .is_some_and(|prepared| TxId::from(prepared.hash) == self.tx_id)
    }

    async fn confirm_while_tx_submitted(
        &self,
        ctx: &ConfirmMintContext,
        params: ConfirmWhileTxSubmitted,
    ) -> Result<(), MintJobError> {
        let vault = match ctx.vault_for(params.network) {
            Ok(vault) => vault,
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    network = %params.network,
                    error = %error,
                    "No vault service configured for mint network"
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
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
                return Ok(());
            }
        };

        match vault.confirm_mint(&self.tx_id).await {
            Ok(result) => {
                let receipt_info = ReceiptInformation::new(
                    params.tokenization_request_id,
                    self.issuer_request_id.clone(),
                    params.underlying,
                    params.quantity,
                    params.journal_confirmed_at,
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
            Err(VaultError::Reverted { tx_hash }) => {
                // Same identity gate as success (`RecordTokensMinted` rejects
                // mismatched `tx_id`): a stale confirm job for a superseded
                // submission must not fail the current attempt.
                if params.stored_tx_id != self.tx_id {
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        job_tx_id = %self.tx_id,
                        stored_tx_id = %params.stored_tx_id,
                        tx_hash = %tx_hash,
                        "Ignoring reverted confirm for stale tx_id; \
                         does not match mint's current submission"
                    );
                    return Ok(());
                }

                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %tx_hash,
                    "On-chain deposit reverted (status=0); recording MintingFailed"
                );

                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordMintFailed {
                            issuer_request_id: self.issuer_request_id.clone(),
                            error: format!(
                                "Transaction reverted on-chain: {tx_hash:?}"
                            ),
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
            Err(VaultError::EventNotFound { tx_hash }) => {
                // Mined success body without Deposit is anomalous — never
                // authorize a second deposit. Leave TxSubmitted for ops, but
                // kick recovery so re-observe is not only the 300s orphan path.
                error!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %tx_hash,
                    "Mint mined without Deposit log; fail closed without \
                     MintingFailed (operator intervention required)"
                );
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Err(error) if is_uncertain_confirm_observation(&error) => {
                self.handle_uncertain_confirm(
                    ctx,
                    vault.as_ref(),
                    params.prepared_tx.as_ref(),
                    &params.stored_tx_id,
                    &error,
                )
                .await?;
            }
            Err(error) => {
                // Any other vault error is fail-closed: stay TxSubmitted and
                // re-drive recovery so re-observe continues.
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %error,
                    "On-chain deposit confirmation failed closed; staying TxSubmitted"
                );
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

    /// Re-observe a known submission while the aggregate is already
    /// `MintingFailed`. Success records `ExistingMint` (TokensMinted only
    /// applies from `TxSubmitted`); revert stays failed without free-prepare;
    /// EventNotFound / uncertain fail closed and kick recovery.
    async fn confirm_while_minting_failed(
        &self,
        ctx: &ConfirmMintContext,
        params: ConfirmWhileMintingFailed,
    ) -> Result<(), MintJobError> {
        let vault = match ctx.vault_for(params.network) {
            Ok(vault) => vault,
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    network = %params.network,
                    error = %error,
                    "No vault service configured for mint network during \
                     MintingFailed re-observe"
                );
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
                return Ok(());
            }
        };

        info!(
            target: "mint",
            issuer_request_id = %self.issuer_request_id,
            tx_id = %self.tx_id,
            "Re-observing mint while MintingFailed"
        );

        match vault.confirm_mint(&self.tx_id).await {
            Ok(result) => {
                let receipt_info = ReceiptInformation::new(
                    params.tokenization_request_id,
                    self.issuer_request_id.clone(),
                    params.underlying,
                    params.quantity,
                    params.journal_confirmed_at,
                    None,
                );

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
                        "Failed to register minted receipt during \
                         MintingFailed re-observe \
                         (monitor/backfill will discover it)"
                    );
                }

                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordExistingMint {
                            issuer_request_id: self.issuer_request_id.clone(),
                            tx_hash: result.tx_hash,
                            receipt_id: result.receipt_id,
                            shares_minted: result.shares_minted,
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                self.enqueue_callback(ctx).await?;
            }
            Err(VaultError::Reverted { tx_hash }) => {
                // Already MintingFailed: leave state unchanged and never
                // free-prepare a replacement from the confirm job.
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %tx_hash,
                    "On-chain deposit still reverted while MintingFailed; \
                     staying failed without free-prepare"
                );
            }
            Err(VaultError::EventNotFound { tx_hash }) => {
                error!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %tx_hash,
                    "Mint mined without Deposit log while MintingFailed; \
                     fail closed without free-prepare"
                );
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Err(error) if is_uncertain_confirm_observation(&error) => {
                // Prefer inventory before failing closed: a receipt means the
                // deposit already succeeded despite the uncertain confirm.
                if self.record_existing_receipt_if_present(ctx).await? {
                    return Ok(());
                }

                // When prepared bytes exist, classify so StillMineable can
                // rebroadcast without leaving MintingFailed forever; terminal
                // death stays failed (no free-prepare from this job).
                if let Some(prepared) = params.prepared_tx.as_ref() {
                    self.handle_uncertain_confirm_while_failed(
                        ctx,
                        vault.as_ref(),
                        prepared,
                        &error,
                    )
                    .await?;
                } else {
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        tx_id = %self.tx_id,
                        error = %error,
                        "MintingFailed re-observe uncertain without prepared \
                         identity; fail closed and kick recovery"
                    );
                    kick_mint_recovery(
                        &ctx.pool,
                        &ctx.apalis_pool,
                        &self.issuer_request_id,
                    )
                    .await;
                }
            }
            Err(error) => {
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %error,
                    "MintingFailed re-observe failed closed; staying failed"
                );
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

    /// Uncertain confirm while already `MintingFailed`: rebroadcast
    /// StillMineable under the wallet lock; never free-prepare. Terminal
    /// classify leaves MintingFailed for recovery's RetryMint path.
    async fn handle_uncertain_confirm_while_failed(
        &self,
        ctx: &ConfirmMintContext,
        vault: &dyn VaultService,
        prepared: &PreparedMintTx,
        error: &VaultError,
    ) -> Result<(), MintJobError> {
        warn!(
            target: "mint",
            issuer_request_id = %self.issuer_request_id,
            tx_id = %self.tx_id,
            error = %error,
            "MintingFailed re-observe uncertain; classifying prepared identity"
        );

        let owner = match prepared.recover_signer() {
            Ok(owner) => owner,
            Err(error) => {
                // Corrupt envelope: leave MintingFailed for recovery/ops.
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    error = %error,
                    "Failed to recover signer from prepared mint; \
                     staying MintingFailed without rebroadcast"
                );
                return Ok(());
            }
        };
        let wallet_guard = vault.lock_wallet().await;
        let classification = vault.classify_mint_tx(owner, prepared).await;

        match classification {
            Ok(MintTxStatus::StillMineable) => {
                info!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    nonce = prepared.nonce,
                    "Rebroadcasting still-mineable mint while MintingFailed"
                );
                if let Err(rebroadcast_error) =
                    vault.submit_mint(prepared).await
                {
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        tx_hash = %prepared.hash,
                        error = %rebroadcast_error,
                        "Mint rebroadcast while MintingFailed failed; \
                         will retry observe later"
                    );
                }
                drop(wallet_guard);
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Ok(MintTxStatus::MinedReverted | MintTxStatus::ProvablyDead) => {
                if self.record_existing_receipt_if_present(ctx).await? {
                    drop(wallet_guard);
                    return Ok(());
                }

                drop(wallet_guard);
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    "Prepared mint terminal while MintingFailed; staying failed \
                     without free-prepare"
                );
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Ok(MintTxStatus::MinedSuccess) => {
                drop(wallet_guard);
                info!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    "Mint appears mined while MintingFailed; re-driving recovery"
                );
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Err(classify_error) => {
                drop(wallet_guard);
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %classify_error,
                    "Mint classify uncertain while MintingFailed; fail closed"
                );
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

    /// Uncertain confirm observation: leave `TxSubmitted`, optionally
    /// rebroadcast StillMineable bytes, or record failure only when classify
    /// proves terminal dead/revert. Always re-drives recovery (except when
    /// already kicking after MintingFailed) so re-observe is not limited to
    /// the periodic reconciler / 300s orphan path.
    ///
    /// Classify + rebroadcast and terminal dead/revert → `RecordMintFailed`
    /// run under `vault.lock_wallet()` so a concurrent free-prepare cannot
    /// race a still-live or just-failed identity.
    async fn handle_uncertain_confirm(
        &self,
        ctx: &ConfirmMintContext,
        vault: &dyn VaultService,
        prepared_tx: Option<&PreparedMintTx>,
        stored_tx_id: &TxId,
        error: &VaultError,
    ) -> Result<(), MintJobError> {
        warn!(
            target: "mint",
            issuer_request_id = %self.issuer_request_id,
            tx_id = %self.tx_id,
            error = %error,
            "Mint confirmation uncertain/pending; classifying prepared identity"
        );

        let Some(prepared) = prepared_tx else {
            // No prepared bytes to classify/rebroadcast: stay TxSubmitted and
            // re-enqueue recovery so confirm is retried.
            kick_mint_recovery(
                &ctx.pool,
                &ctx.apalis_pool,
                &self.issuer_request_id,
            )
            .await;
            return Ok(());
        };

        let owner = match prepared.recover_signer() {
            Ok(owner) => owner,
            Err(error) => {
                // Corrupt envelope: stay TxSubmitted and re-drive recovery.
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    error = %error,
                    "Failed to recover signer from prepared mint; \
                     staying TxSubmitted without rebroadcast"
                );
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
                return Ok(());
            }
        };
        let wallet_guard = vault.lock_wallet().await;
        let classification = vault.classify_mint_tx(owner, prepared).await;

        match classification {
            Ok(MintTxStatus::StillMineable) => {
                info!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    nonce = prepared.nonce,
                    "Rebroadcasting still-mineable mint transaction"
                );
                if let Err(rebroadcast_error) =
                    vault.submit_mint(prepared).await
                {
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        tx_hash = %prepared.hash,
                        error = %rebroadcast_error,
                        "Mint rebroadcast failed; will retry observe later"
                    );
                }
                drop(wallet_guard);
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Ok(
                terminal @ (MintTxStatus::MinedReverted
                | MintTxStatus::ProvablyDead),
            ) => {
                // Prefer inventory before terminal MintingFailed: a receipt
                // means the deposit already succeeded and must not authorize
                // a replacement prepare via recovery.
                if self.record_existing_receipt_if_present(ctx).await? {
                    drop(wallet_guard);
                    return Ok(());
                }

                // Same identity gate as the reverted-confirm arm: a stale job
                // for a superseded submission must not record the failure that
                // authorizes a replacement prepare. Recovery re-observes under
                // the current tx_id.
                if stored_tx_id != &self.tx_id {
                    drop(wallet_guard);
                    warn!(
                        target: "mint",
                        issuer_request_id = %self.issuer_request_id,
                        job_tx_id = %self.tx_id,
                        stored_tx_id = %stored_tx_id,
                        tx_hash = %prepared.hash,
                        classification = ?terminal,
                        "Ignoring terminal classification for stale tx_id; \
                         does not match mint's current submission"
                    );
                    kick_mint_recovery(
                        &ctx.pool,
                        &ctx.apalis_pool,
                        &self.issuer_request_id,
                    )
                    .await;
                    return Ok(());
                }

                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    classification = ?terminal,
                    "Prepared mint is terminal after pending confirm; \
                     recording MintingFailed"
                );
                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        MintCommand::RecordMintFailed {
                            issuer_request_id: self.issuer_request_id.clone(),
                            error: format!(
                                "Prepared mint terminal after uncertain \
                                 confirm ({terminal:?}): {error}"
                            ),
                        },
                    )
                    .await?;
                drop(wallet_guard);
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Ok(MintTxStatus::MinedSuccess) => {
                // Race: classify saw success after confirm pending. Re-drive
                // confirm immediately so TokensMinted can complete without
                // waiting for the next recovery poll.
                drop(wallet_guard);
                info!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    tx_hash = %prepared.hash,
                    "Mint appears mined after uncertain confirm; re-driving confirm"
                );
                kick_mint_recovery(
                    &ctx.pool,
                    &ctx.apalis_pool,
                    &self.issuer_request_id,
                )
                .await;
            }
            Err(classify_error) => {
                drop(wallet_guard);
                warn!(
                    target: "mint",
                    issuer_request_id = %self.issuer_request_id,
                    error = %classify_error,
                    "Mint classify uncertain after pending confirm; fail closed"
                );
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

    /// Records `ExistingMint` + callback when inventory already holds a receipt
    /// for this mint. Used before terminal `MintingFailed` so a successful
    /// deposit is never abandoned for replacement.
    async fn record_existing_receipt_if_present(
        &self,
        ctx: &ConfirmMintContext,
    ) -> Result<bool, MintJobError> {
        record_existing_receipt_from_inventory(
            ctx.receipts.as_ref(),
            ctx.mint_store.as_ref(),
            &ctx.callback_queue,
            self.chain_id,
            self.vault,
            &self.issuer_request_id,
        )
        .await
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

/// Loads an inventory receipt for this mint and records `ExistingMint` +
/// enqueues the callback. Shared by submit and confirm jobs so a successful
/// deposit is never abandoned for replacement prepare.
async fn record_existing_receipt_from_inventory(
    receipts: &dyn ReceiptService,
    mint_store: &Store<Mint>,
    callback_queue: &JobQueue<SendCallbackJob>,
    chain_id: u64,
    vault: Address,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<bool, MintJobError> {
    let Some(receipt) = receipts
        .find_by_issuer_request_id(chain_id, &vault, issuer_request_id)
        .await?
    else {
        return Ok(false);
    };

    info!(
        target: "mint",
        issuer_request_id = %issuer_request_id,
        tx_hash = %receipt.tx_hash,
        block_number = receipt.block_number,
        "Found existing receipt, recording recovery"
    );
    mint_store
        .send(
            issuer_request_id,
            MintCommand::RecordExistingMint {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: receipt.tx_hash,
                receipt_id: receipt.receipt_id,
                shares_minted: receipt.shares,
                block_number: receipt.block_number,
            },
        )
        .await?;
    callback_queue
        .clone()
        .push_with_idempotency_key(
            SendCallbackJob { issuer_request_id: issuer_request_id.clone() },
            issuer_request_id.to_string(),
        )
        .await?;
    Ok(true)
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
    use alloy::primitives::{B256, Bytes, U256};
    use async_trait::async_trait;
    use cqrs_es::{AggregateError, DomainEvent};
    use event_sorcery::test_store;
    use std::any::type_name;
    use std::collections::HashMap;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::burn_excess::BurnExcessEvent;
    use crate::mint::MintEvent;
    use crate::mint::api::test_utils::TestHarness;
    use crate::mint::tests::{
        BOT, VAULT, events_through_minting, events_through_tokens_minted,
        events_through_tx_submitted,
    };
    use crate::receipt_inventory::{
        BurnPlan, BurnTrackingError, CqrsReceiptService, ReceiptInventory,
        ReceiptLookupError, ReceiptRegistrationError, RecoveredReceipt,
    };
    use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
    use crate::test_utils::{
        ANVIL_CHAIN_ID, ETHEREUM_TEST_CHAIN_ID, logs_contain_at,
    };
    use crate::vault::MintResult;
    use crate::vault::MintTxStatus;
    use crate::vault::NetworkVault;
    use crate::vault::mock::MockVaultService;

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

    /// Seeds an unresolved `BurnExcess` stream so the wallet intent gate sees a
    /// competing excess-burn recovery. `BurnExcess` holds no
    /// `active_signer_intents` row, so the gate reads the event stream directly
    /// and only `event_type` matters — an empty payload is enough.
    async fn seed_unresolved_excess_burn(
        pool: &Pool<Sqlite>,
        event_type: &str,
    ) {
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
                'BurnExcess',
                '0x00000000000000000000000000000000000000000000000000000000000000e1',
                1,
                ?,
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .bind(event_type)
        .execute(pool)
        .await
        .unwrap();
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

    /// TxSubmitted after MintTxIntended so `prepared_tx` is retained on the
    /// aggregate (needed for classify / rebroadcast on uncertain confirm).
    fn events_through_tx_submitted_with_prepared(
        issuer_request_id: &IssuerMintRequestId,
    ) -> (Vec<MintEvent>, crate::vault::PreparedMintTx) {
        let prepared = crate::vault::PreparedMintTx::valid_for_test(
            1,
            format!("mint-{issuer_request_id}"),
        );
        let hash = prepared.hash;
        // Built here rather than rewriting an index into
        // `events_through_tx_intended`: a silent shape change there would stop
        // the rewrite from landing, and the returned `prepared` would no longer
        // be the envelope stored on the aggregate that classify/rebroadcast
        // tests assert against.
        let mut events = events_through_minting(issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: prepared.clone(),
            intended_at: chrono::Utc::now(),
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(hash),
            submitted_at: chrono::Utc::now(),
        });
        (events, prepared)
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

    /// Free-prepare failure (`PreparedLiveness::None`): prepare_mint_tx rejects
    /// before any live identity exists — domain `MintingFailed` is allowed so
    /// the retry schedule advances.
    #[traced_test]
    #[tokio::test]
    async fn submit_mint_job_free_prepare_failure_records_mint_failed() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_minting(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(MockVaultService::new_prepare_tx_failure());
        let ctx = submit_ctx(&harness, vault.clone());

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
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "free-prepare failure must record MintingFailed (PreparedLiveness::None),              got: {mint:?}"
        );
        assert!(
            mint.pending_prepared_tx().is_none(),
            "failed free-prepare must not leave prepared bytes"
        );

        let test = "submit_mint_job_free_prepare_failure_records_mint_failed";
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

    /// `BurnExcess` holds no `active_signer_intents` reservation, so the
    /// network-keyed signer-intent query cannot see it. Without the separate
    /// gate a mint would free-prepare over an excess recovery's nonce.
    #[traced_test]
    #[tokio::test]
    async fn submit_from_minting_defers_to_an_unresolved_excess_burn_intent() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_minting(&issuer_request_id),
        )
        .await;
        // `FundingExcluded` holds no signed transaction yet, and must still
        // block: its exclusion write is permanent and it will sign against the
        // same issuer wallet.
        seed_unresolved_excess_burn(
            &harness.pool,
            BurnExcessEvent::FUNDING_EXCLUSION_RECORDED,
        )
        .await;

        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        let error = SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .expect_err("an unresolved excess burn must defer minting");

        assert!(
            matches!(error, MintJobError::UnresolvedWalletIntent { .. }),
            "expected UnresolvedWalletIntent, got: {error:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "the blocked job must not prepare a signed transaction"
        );
        let intent_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Mint'
              AND aggregate_id = ?
              AND event_type = 'MintEvent::MintTxIntended'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            intent_count, 0,
            "a blocked mint must persist no signed intent"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "Deferring mint behind another persisted wallet intent",
                "stage=\"preparation\"",
                "unresolved_intent=false",
                "unresolved_excess=true"
            ]
        ));
    }

    /// The rebroadcast path gates too: a persisted mint intent must not go back
    /// on the wire while an excess recovery is signing against the same wallet.
    #[traced_test]
    #[tokio::test]
    async fn submit_from_tx_intended_defers_to_an_unresolved_excess_burn_intent()
     {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_intended(&issuer_request_id),
        )
        .await;
        seed_unresolved_excess_burn(
            &harness.pool,
            BurnExcessEvent::EXCESS_BURN_INTENDED,
        )
        .await;

        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault.clone());

        let error = SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .expect_err("an unresolved excess burn must defer submission");

        assert!(
            matches!(error, MintJobError::UnresolvedWalletIntent { .. }),
            "expected UnresolvedWalletIntent, got: {error:?}"
        );
        let submitted_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Mint'
              AND aggregate_id = ?
              AND event_type = 'MintEvent::MintTxSubmitted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            submitted_count, 0,
            "a blocked rebroadcast must record no submission"
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &[
                "Deferring mint behind another persisted wallet intent",
                "stage=\"submission\"",
                "unresolved_intent=false",
                "unresolved_excess=true"
            ]
        ));
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
        });
        events.push(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            manual_retry_id: None,
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

    /// Uncertain confirmation (e.g. null receipt / InvalidReceipt) must leave
    /// the mint in `TxSubmitted` — never `MintingFailed` (production double-mint).
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_on_uncertain_confirmation_stays_tx_submitted() {
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
            matches!(mint, Mint::TxSubmitted { .. }),
            "uncertain confirmation must stay TxSubmitted, got: {mint:?}"
        );

        let test =
            "confirm_mint_job_on_uncertain_confirmation_stays_tx_submitted";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Mint confirmation uncertain/pending", "classifying"]
        ));
    }

    /// Mined revert (status=0) is the only confirmation path that records
    /// `MintingFailed` so recovery may prepare a replacement after classify.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_on_reverted_records_mint_failed() {
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
            Arc::new(MockVaultService::new_confirm_revert()),
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
            "status=0 revert must record MintingFailed, got: {mint:?}"
        );

        let test = "confirm_mint_job_on_reverted_records_mint_failed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "On-chain deposit reverted"]
        ));
    }

    /// ConfirmationPending must leave TxSubmitted and not kick MintingFailed.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_confirmation_pending_stays_tx_submitted() {
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
            Arc::new(MockVaultService::new_confirm_pending()),
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
            matches!(mint, Mint::TxSubmitted { .. }),
            "ConfirmationPending must stay TxSubmitted, got: {mint:?}"
        );

        let test = "confirm_mint_job_confirmation_pending_stays_tx_submitted";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "uncertain/pending", "classifying prepared identity"]
        ));
    }

    /// EventNotFound (mined without Deposit) fails closed — no MintingFailed.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_event_not_found_fails_closed() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(
            MockVaultService::new_success().with_confirm_mint_outcomes(vec![
                Err(VaultError::EventNotFound {
                    tx_hash: alloy::primitives::B256::ZERO,
                }),
            ]),
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
            matches!(mint, Mint::TxSubmitted { .. }),
            "EventNotFound must not MintingFailed, got: {mint:?}"
        );

        let test = "confirm_mint_job_event_not_found_fails_closed";
        assert!(logs_contain_at!(
            Level::ERROR,
            &[test, "without Deposit log", "fail closed"]
        ));
    }

    /// Receipt at the retry boundary after RetryMint: SubmitMintJob must
    /// record ExistingMint without preparing or broadcasting a second deposit.
    #[traced_test]
    #[tokio::test]
    async fn submit_mint_job_receipt_at_retry_boundary_records_existing() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared = crate::vault::PreparedMintTx::valid_for_test(
            3,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let now = chrono::Utc::now();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: prepared,
            intended_at: now,
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(expected_hash),
            submitted_at: now,
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "uncertain confirm misrecorded as failure".to_string(),
            failed_at: now - chrono::Duration::minutes(2),
        });
        events.push(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            manual_retry_id: None,
            started_at: now,
        });
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let receipts = cqrs_receipts(&harness.pool);
        let receipt_info = ReceiptInformation::new(
            super::super::TokenizationRequestId::new("tok-123"),
            issuer_request_id.clone(),
            super::super::UnderlyingSymbol::new("AAPL").unwrap(),
            crate::Quantity::new(rust_decimal::Decimal::from(100)),
            now,
            None,
        );
        receipts
            .register_minted_receipt(MintedReceiptParams {
                chain_id: ANVIL_CHAIN_ID,
                vault: VAULT,
                receipt_id: ReceiptId::from(U256::from(7u64)),
                shares: Shares::new(U256::from(100u64)),
                block_number: 1_234,
                tx_hash: expected_hash,
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
        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "inventory hit at retry boundary must record ExistingMint, got: {mint:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "must not prepare a second deposit when inventory already has the receipt"
        );

        let test = "submit_mint_job_receipt_at_retry_boundary_records_existing";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "Found existing receipt", "recording recovery"]
        ));
    }

    /// StillMineable after RetryMint rebroadcasts the same prepared hash —
    /// never `prepare_mint_tx` a replacement.
    #[tokio::test]
    async fn submit_mint_job_still_mineable_rebroadcasts_same_hash() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared = crate::vault::PreparedMintTx::valid_for_test(
            4,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let now = chrono::Utc::now();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: prepared.clone(),
            intended_at: now,
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(expected_hash),
            submitted_at: now,
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "still mineable after uncertain confirm".to_string(),
            failed_at: now - chrono::Duration::minutes(2),
        });
        events.push(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            manual_retry_id: None,
            started_at: now,
        });
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_mint_tx_status(MintTxStatus::StillMineable),
        );
        let ctx = submit_ctx(&harness, vault.clone());

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
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "StillMineable rebroadcast must re-record TxSubmitted, got: {mint:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "StillMineable must rebroadcast existing prepared bytes, not prepare"
        );
        assert_eq!(
            mint.pending_prepared_tx().map(|prepared| prepared.hash),
            Some(expected_hash)
        );
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

    /// TOCTOU: first classify terminal, recheck no longer terminal → abort
    /// without prepare_mint_tx.
    #[traced_test]
    #[tokio::test]
    async fn submit_mint_job_toctou_recheck_aborts_replacement() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared = crate::vault::PreparedMintTx::valid_for_test(
            4,
            format!("mint-{issuer_request_id}"),
        );
        let expected_hash = prepared.hash;
        let now = chrono::Utc::now();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: prepared,
            intended_at: now,
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: TxId::from(expected_hash),
            submitted_at: now,
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "prior attempt dead".to_string(),
            failed_at: now - chrono::Duration::minutes(2),
        });
        events.push(MintEvent::MintRetryStarted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: None,
            manual_retry_id: None,
            started_at: now,
        });
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(
            MockVaultService::new_success().with_mint_tx_status_sequence(vec![
                MintTxStatus::ProvablyDead,
                MintTxStatus::StillMineable,
            ]),
        );
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
            vault.mint_classification_call_count(),
            2,
            "must classify then recheck under the wallet guard"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "recheck no longer terminal must not prepare a replacement"
        );

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::Minting { .. }),
            "TOCTOU abort must leave Minting without new intent, got: {mint:?}"
        );

        let test = "submit_mint_job_toctou_recheck_aborts_replacement";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "recheck no longer terminal", "not replacing"]
        ));
    }

    /// Uncertain broadcast after intent is persisted must preserve MintIntended
    /// (never MintingFailed) so recovery rebroadcasts the same bytes.
    #[traced_test]
    #[tokio::test]
    async fn submit_mint_job_uncertain_broadcast_preserves_mint_intended() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_intended(&issuer_request_id),
        )
        .await;

        let vault =
            Arc::new(MockVaultService::new_success().with_submit_mint_error(
                VaultError::ConfirmationPending {
                    tx_id: TxId::Legacy("pending".to_string()),
                    message: "broadcast uncertain".to_string(),
                },
            ));
        let ctx = submit_ctx(&harness, vault);

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
        assert!(
            matches!(mint, Mint::TxIntended { .. }),
            "uncertain broadcast must preserve MintIntended, got: {mint:?}"
        );

        let test =
            "submit_mint_job_uncertain_broadcast_preserves_mint_intended";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Uncertain mint broadcast", "preserving MintIntended"]
        ));
    }

    /// ConfirmationPending + StillMineable with prepared bytes: rebroadcast
    /// the same hash under the wallet lock (never prepare a replacement).
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_pending_still_mineable_rebroadcasts_same_hash() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let (events, prepared) =
            events_through_tx_submitted_with_prepared(&issuer_request_id);
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(
            MockVaultService::new_confirm_pending()
                .with_mint_tx_status(MintTxStatus::StillMineable),
        );
        let ctx =
            confirm_ctx(&harness, vault.clone(), cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id: TxId::from(prepared.hash),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "StillMineable rebroadcast must stay TxSubmitted, got: {mint:?}"
        );
        assert_eq!(
            mint.pending_prepared_tx().map(|tx| tx.hash),
            Some(prepared.hash),
            "rebroadcast must keep the same prepared hash"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "confirm rebroadcast must not prepare a new mint"
        );
        assert!(
            vault.get_wallet_lock_call_count() >= 1,
            "classify + rebroadcast must hold the wallet lock"
        );

        let test =
            "confirm_mint_job_pending_still_mineable_rebroadcasts_same_hash";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "Rebroadcasting still-mineable mint transaction"]
        ));
    }

    /// The live race the Anvil e2e cannot reach: `confirm_mint` keeps
    /// answering `ConfirmationPending` (what an `Ok(None)` receipt through the
    /// whole poll budget produces — see `RealVaultService::confirm_mint`)
    /// while the first deposit is still mining. Across every poll in that
    /// window the mint must stay on one identity: same hash rebroadcast, never
    /// a replacement prepare, until the deposit finally mines.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_never_replaces_across_a_pending_poll_window() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let (events, prepared) =
            events_through_tx_submitted_with_prepared(&issuer_request_id);
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        const PENDING_POLLS: usize = 3;
        let mut confirm_outcomes: Vec<Result<MintResult, VaultError>> = (0
            ..PENDING_POLLS)
            .map(|poll| {
                Err(VaultError::ConfirmationPending {
                    tx_id: TxId::from(prepared.hash),
                    message: format!("no receipt yet (poll {poll})"),
                })
            })
            .collect();
        confirm_outcomes.push(Ok(MintResult {
            tx_hash: prepared.hash,
            receipt_id: U256::from(7),
            shares_minted: U256::from(100),
            gas_used: 21000,
            block_number: 1000,
            receipt_info_bytes: Bytes::new(),
        }));

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_confirm_mint_outcomes(confirm_outcomes)
                // Still mineable for every pending poll: the deposit is in the
                // mempool, not dead.
                .with_mint_tx_status_sequence(vec![
                    MintTxStatus::StillMineable;
                    PENDING_POLLS
                ]),
        );
        let ctx =
            confirm_ctx(&harness, vault.clone(), cqrs_receipts(&harness.pool));

        for poll in 0..PENDING_POLLS {
            ConfirmMintJob {
                issuer_request_id: issuer_request_id.clone(),
                vault: VAULT,
                chain_id: ANVIL_CHAIN_ID,
                tx_id: TxId::from(prepared.hash),
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
                matches!(mint, Mint::TxSubmitted { .. }),
                "poll {poll} must stay TxSubmitted, got: {mint:?}"
            );
            assert_eq!(
                mint.pending_prepared_tx().map(|tx| tx.hash),
                Some(prepared.hash),
                "poll {poll} must keep the original prepared hash"
            );
            assert_eq!(
                vault.get_call_count(),
                0,
                "poll {poll} must not prepare a replacement mint"
            );
        }

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id: TxId::from(prepared.hash),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(
                mint,
                Mint::CallbackPending { .. } | Mint::Completed { .. }
            ),
            "the original identity must be the one that completes, got: {mint:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "no poll in the window may prepare a second deposit"
        );

        let test =
            "confirm_mint_job_never_replaces_across_a_pending_poll_window";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "Rebroadcasting still-mineable mint transaction"]
        ));
    }

    /// ConfirmationPending + ProvablyDead with prepared bytes records
    /// MintingFailed under the wallet-lock path (no free-prepare race).
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_pending_provably_dead_records_mint_failed() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let (events, prepared) =
            events_through_tx_submitted_with_prepared(&issuer_request_id);
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(
            MockVaultService::new_confirm_pending()
                .with_mint_tx_status(MintTxStatus::ProvablyDead),
        );
        let ctx =
            confirm_ctx(&harness, vault.clone(), cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id: TxId::from(prepared.hash),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "ProvablyDead after pending confirm must record MintingFailed, got: {mint:?}"
        );
        assert!(
            vault.get_wallet_lock_call_count() >= 1,
            "terminal classify must hold the wallet lock before MintingFailed"
        );

        let test = "confirm_mint_job_pending_provably_dead_records_mint_failed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                test,
                "terminal after pending confirm",
                "recording MintingFailed"
            ]
        ));
    }

    /// BroadcastHashMismatch with live prepared bytes is uncertain — preserve
    /// MintIntended without reloading the aggregate to re-derive preparedness.
    #[traced_test]
    #[tokio::test]
    async fn submit_mint_job_broadcast_hash_mismatch_preserves_mint_intended() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_intended(&issuer_request_id),
        )
        .await;

        let expected = alloy::primitives::B256::from([0x11; 32]);
        let returned = alloy::primitives::B256::from([0x22; 32]);
        let vault =
            Arc::new(MockVaultService::new_success().with_submit_mint_error(
                VaultError::BroadcastHashMismatch { expected, returned },
            ));
        let ctx = submit_ctx(&harness, vault);

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
        assert!(
            matches!(mint, Mint::TxIntended { .. }),
            "BroadcastHashMismatch with live prepared must preserve MintIntended, \
             got: {mint:?}"
        );

        let test =
            "submit_mint_job_broadcast_hash_mismatch_preserves_mint_intended";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "Uncertain mint broadcast", "preserving MintIntended"]
        ));
    }

    /// SubmitMintJob wallet gate matches burn_manager: unresolved burn intent
    /// blocks prepare/submit under the wallet lock.
    #[tokio::test]
    async fn submit_mint_job_waits_for_unresolved_burn_intent() {
        let harness = TestHarness::new().await;
        let redemption_id =
            crate::redemption::IssuerRedemptionRequestId::random();
        // The reserve trigger requires the stream to carry exactly one
        // `Detected` event, so a bare `BurnIntended` cannot seed a reservation.
        for (sequence, event_type, payload) in [
            (
                1,
                "RedemptionEvent::Detected",
                r#"{"Detected":{"network":"base"}}"#,
            ),
            (2, "RedemptionEvent::BurnIntended", "{}"),
        ] {
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
                VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(redemption_id.to_string())
            .bind(sequence)
            .bind(event_type)
            .bind(payload)
            .execute(&harness.pool)
            .await
            .unwrap();
        }

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
        .expect_err("unresolved burn intent must defer minting");

        assert_eq!(
            vault.get_call_count(),
            0,
            "burn-intent gate must not prepare a mint"
        );
    }

    /// Uncertain confirm with prepared identity + classify Err stays
    /// TxSubmitted and never records MintingFailed.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_uncertain_classify_err_stays_tx_submitted() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let (events, prepared) =
            events_through_tx_submitted_with_prepared(&issuer_request_id);
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(
            MockVaultService::new_confirm_pending()
                .with_mint_tx_classification_failure(),
        );
        let ctx =
            confirm_ctx(&harness, vault.clone(), cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id: TxId::from(prepared.hash),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "uncertain confirm + classify Err must stay TxSubmitted, got: {mint:?}"
        );
        assert_eq!(
            vault.mint_classification_call_count(),
            1,
            "must classify prepared identity under the wallet lock"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "classify Err must not prepare a replacement"
        );

        let test = "confirm_mint_job_uncertain_classify_err_stays_tx_submitted";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "classify uncertain after pending confirm", "fail closed"]
        ));
    }

    /// Terminal ProvablyDead after pending confirm must prefer inventory over
    /// MintingFailed when a receipt already exists for the issuer request.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_provably_dead_with_inventory_records_existing() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let (events, prepared) =
            events_through_tx_submitted_with_prepared(&issuer_request_id);
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let receipts = cqrs_receipts(&harness.pool);
        let now = chrono::Utc::now();
        let receipt_info = ReceiptInformation::new(
            super::super::TokenizationRequestId::new("tok-123"),
            issuer_request_id.clone(),
            super::super::UnderlyingSymbol::new("AAPL").unwrap(),
            crate::Quantity::new(rust_decimal::Decimal::from(100)),
            now,
            None,
        );
        receipts
            .register_minted_receipt(MintedReceiptParams {
                chain_id: ANVIL_CHAIN_ID,
                vault: VAULT,
                receipt_id: ReceiptId::from(U256::from(9u64)),
                shares: Shares::new(U256::from(100u64)),
                block_number: 2_222,
                tx_hash: prepared.hash,
                receipt_info_bytes: receipt_info.encode(None).unwrap(),
                receipt_info,
            })
            .await
            .unwrap();

        let vault = Arc::new(
            MockVaultService::new_confirm_pending()
                .with_mint_tx_status(MintTxStatus::ProvablyDead),
        );
        let ctx = confirm_ctx(&harness, vault, receipts);

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id: TxId::from(prepared.hash),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "inventory hit before terminal MintingFailed must record ExistingMint, \
             got: {mint:?}"
        );

        let test =
            "confirm_mint_job_provably_dead_with_inventory_records_existing";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "Found existing receipt", "recording recovery"]
        ));
    }

    /// ConfirmMintJob must re-observe while already MintingFailed when the
    /// job tx_id matches the failed identity — success records ExistingMint.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_runs_under_minting_failed_records_existing() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared = crate::vault::PreparedMintTx::valid_for_test(
            5,
            format!("mint-{issuer_request_id}"),
        );
        let tx_id = TxId::from(prepared.hash);
        let now = chrono::Utc::now();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: prepared,
            intended_at: now,
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: tx_id.clone(),
            submitted_at: now,
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "earlier uncertain confirm misrecorded".to_string(),
            failed_at: now - chrono::Duration::minutes(2),
        });
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(MockVaultService::new_success());
        let ctx =
            confirm_ctx(&harness, vault.clone(), cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "successful confirm under MintingFailed must record ExistingMint, \
             got: {mint:?}"
        );

        let test =
            "confirm_mint_job_runs_under_minting_failed_records_existing";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "Re-observing mint while MintingFailed"]
        ));
    }

    /// ConfirmMintJob under MintingFailed + on-chain revert stays failed and
    /// never free-prepares a replacement.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_minting_failed_reverted_stays_failed() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let prepared = crate::vault::PreparedMintTx::valid_for_test(
            6,
            format!("mint-{issuer_request_id}"),
        );
        let tx_id = TxId::from(prepared.hash);
        let now = chrono::Utc::now();
        let mut events = events_through_minting(&issuer_request_id);
        events.push(MintEvent::MintTxIntended {
            issuer_request_id: issuer_request_id.clone(),
            prepared_tx: prepared,
            intended_at: now,
        });
        events.push(MintEvent::MintTxSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            tx_id: tx_id.clone(),
            submitted_at: now,
        });
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "prior attempt".to_string(),
            failed_at: now - chrono::Duration::minutes(2),
        });
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(MockVaultService::new_confirm_revert());
        let ctx =
            confirm_ctx(&harness, vault.clone(), cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "reverted re-observe under MintingFailed must stay failed, got: {mint:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "MintingFailed re-observe must never free-prepare"
        );

        let test = "confirm_mint_job_minting_failed_reverted_stays_failed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                test,
                "still reverted while MintingFailed",
                "without free-prepare"
            ]
        ));
    }

    /// Confirm fixtures retain prepared_tx: MinedSuccess after uncertain
    /// confirm kicks recovery so TokensMinted can complete.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_mined_success_after_uncertain_kicks_recovery() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        let (events, prepared) =
            events_through_tx_submitted_with_prepared(&issuer_request_id);
        seed_mint_events(&harness.pool, &issuer_request_id, events).await;

        let vault = Arc::new(
            MockVaultService::new_confirm_pending()
                .with_mint_tx_status(MintTxStatus::MinedSuccess),
        );
        let ctx = confirm_ctx(&harness, vault, cqrs_receipts(&harness.pool));

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
            tx_id: TxId::from(prepared.hash),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "MinedSuccess after uncertain must stay TxSubmitted for re-confirm, \
             got: {mint:?}"
        );

        let recovery_jobs = count_jobs(
            &harness.pool,
            job_type::<crate::mint::recovery::MintRecoveryJob>(),
            &issuer_request_id.to_string(),
        )
        .await;
        assert!(
            recovery_jobs >= 1,
            "must kick mint recovery so re-confirm is not only every 300s"
        );

        let test =
            "confirm_mint_job_mined_success_after_uncertain_kicks_recovery";
        assert!(logs_contain_at!(
            Level::INFO,
            &[test, "mined after uncertain confirm", "re-driving confirm"]
        ));
    }

    /// `SubmitMintJob::enqueue_confirm` must free a prior terminal ConfirmMintJob
    /// so re-enqueue after Done is not silently dropped by apalis idempotency.
    #[tokio::test]
    async fn submit_mint_job_enqueue_confirm_releases_terminal_done() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_submitted(&issuer_request_id),
        )
        .await;

        sqlx::query(
            "
            INSERT INTO Jobs (
                job,
                id,
                job_type,
                status,
                attempts,
                max_attempts,
                idempotency_key
            )
            VALUES (X'00', ?, ?, 'Done', 0, 25, ?)
            ",
        )
        .bind(format!("done-confirm-{issuer_request_id}"))
        .bind(type_name::<ConfirmMintJob>())
        .bind(issuer_request_id.to_string())
        .execute(&harness.pool)
        .await
        .unwrap();

        let vault = Arc::new(MockVaultService::new_success());
        let ctx = submit_ctx(&harness, vault);

        SubmitMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: ANVIL_CHAIN_ID,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let statuses: Vec<String> = sqlx::query_scalar(
            "
            SELECT status
            FROM Jobs
            WHERE
                job_type = ?
                AND idempotency_key = ?
            ",
        )
        .bind(type_name::<ConfirmMintJob>())
        .bind(issuer_request_id.to_string())
        .fetch_all(&harness.pool)
        .await
        .unwrap();

        assert_eq!(
            statuses,
            vec!["Pending".to_string()],
            "terminal Done confirm must be released so re-enqueue is Pending, \
             not silently dropped; got {statuses:?}"
        );
    }

    /// Stale ConfirmMintJob (job.tx_id ≠ mint's current submission) must not
    /// record MintingFailed when the old identity reverts.
    #[traced_test]
    #[tokio::test]
    async fn confirm_mint_job_reverted_stale_tx_id_is_ignored() {
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
            Arc::new(MockVaultService::new_confirm_revert()),
            cqrs_receipts(&harness.pool),
        );

        ConfirmMintJob {
            issuer_request_id: issuer_request_id.clone(),
            vault: VAULT,
            chain_id: 1,
            // Stored submission is Legacy("fb-1"); this is a superseded attempt.
            tx_id: TxId::Legacy("stale-old-attempt".to_string()),
        }
        .perform(&ctx)
        .await
        .unwrap();

        let mint =
            harness.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::TxSubmitted { .. }),
            "stale revert must not MintingFailed the current submission, got: \
             {mint:?}"
        );

        let test = "confirm_mint_job_reverted_stale_tx_id_is_ignored";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "stale tx_id", "does not match mint's current submission"]
        ));
    }

    /// TxIntended + terminal classify records MintingFailed (no blind rebroadcast
    /// of a dead identity); recovery may then prepare a replacement.
    #[traced_test]
    #[tokio::test]
    async fn tx_intended_terminal_classify_records_mint_failed() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_intended(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(
            MockVaultService::new_success().with_mint_tx_status_sequence(vec![
                MintTxStatus::ProvablyDead,
                MintTxStatus::ProvablyDead,
            ]),
        );
        let ctx = submit_ctx(&harness, vault.clone());

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
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "terminal TxIntended must record MintingFailed, got: {mint:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "terminal TxIntended must not prepare or rebroadcast"
        );

        let test = "tx_intended_terminal_classify_records_mint_failed";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "TxIntended prepared identity terminal", "MintingFailed"]
        ));
    }

    /// Uncertain classify on TxIntended preserves MintIntended (no MintingFailed).
    #[traced_test]
    #[tokio::test]
    async fn tx_intended_uncertain_classify_preserves_mint_intended() {
        let harness = TestHarness::new().await;
        let issuer_request_id = IssuerMintRequestId::random();
        seed_mint_events(
            &harness.pool,
            &issuer_request_id,
            events_through_tx_intended(&issuer_request_id),
        )
        .await;

        let vault = Arc::new(
            MockVaultService::new_success()
                .with_mint_tx_classification_failure(),
        );
        let ctx = submit_ctx(&harness, vault.clone());

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
        assert!(
            matches!(mint, Mint::TxIntended { .. }),
            "uncertain TxIntended classify must preserve MintIntended, got: \
             {mint:?}"
        );
        assert_eq!(
            vault.get_call_count(),
            0,
            "uncertain classify must not prepare or rebroadcast"
        );

        let test = "tx_intended_uncertain_classify_preserves_mint_intended";
        assert!(logs_contain_at!(
            Level::WARN,
            &[test, "TxIntended classification uncertain", "preserving"]
        ));
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
