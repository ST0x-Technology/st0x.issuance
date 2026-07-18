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
//! surfaces as a job error that apalis re-drives. Re-runs are safe —
//! `submit_mint` derives a deterministic `external_tx_id` from the
//! `issuer_request_id` (the signing backend dedups duplicate submissions), and
//! every outcome command is a no-op once its event is recorded.

use alloy::primitives::Address;
use apalis_sqlite::SqlitePool;
use event_sorcery::{SendError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::warn;

use super::recovery::enqueue_scheduled_mint_recovery;
use super::{IssuerMintRequestId, Mint, MintCommand};
use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};
use crate::jobs::{Job, JobQueue, QueuePushError};
use crate::receipt_inventory::{
    MintedReceiptParams, ReceiptId, ReceiptLookupError, ReceiptService, Shares,
};
use crate::tokenized_asset::Network;
use crate::vault::{
    NetworkVaultServices, ReceiptInformation, TxId, UnconfiguredNetworkError,
    VaultError, VaultService,
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
                // Defence against double-mint: if the on-chain mint already
                // succeeded (a receipt exists for this mint), record it instead
                // of re-submitting. Re-submission would mint again where the
                // signer does not dedup on the external_tx_id.
                if let Some(receipt) = ctx
                    .receipts
                    .find_by_issuer_request_id(
                        self.chain_id,
                        &self.vault,
                        &self.issuer_request_id,
                    )
                    .await?
                {
                    ctx.mint_store
                        .send(
                            &self.issuer_request_id,
                            MintCommand::RecordExistingMint {
                                issuer_request_id: self
                                    .issuer_request_id
                                    .clone(),
                                tx_hash: receipt.tx_hash,
                                receipt_id: receipt.receipt_id,
                                shares_minted: receipt.shares,
                                block_number: receipt.block_number,
                            },
                        )
                        .await?;

                    self.enqueue_callback(ctx).await?;
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
                let prepared = match vault
                    .prepare_mint_tx(
                        self.vault,
                        assets,
                        ctx.bot,
                        *wallet,
                        receipt_info,
                        external_tx_id,
                    )
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        drop(wallet_guard);
                        self.record_submission_failure(ctx, error).await?;
                        return Ok(());
                    }
                };

                let submitted = vault.submit_mint(&prepared).await;
                drop(wallet_guard);

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
            }
            // Crash recovery for a prepare-then-submit job that persisted
            // intent via the inline PrepareMint path before the job rewrite.
            Mint::TxIntended { prepared_tx, network, .. } => {
                let Some(vault) =
                    self.resolve_vault_service(ctx, *network).await?
                else {
                    return Ok(());
                };

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
