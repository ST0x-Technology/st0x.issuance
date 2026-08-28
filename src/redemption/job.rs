//! Durable jobs that perform the redemption burn's external side effects.
//!
//! Each job runs one step of the burn flow. `SubmitBurnJob` broadcasts the
//! persisted transaction and `ConfirmBurnJob` polls it. Both delegate to
//! `BurnManager`, which keeps all orchestration (reservation, signing, failure
//! classification) and performs the vault I/O outside any aggregate
//! transition, recording each outcome through a pure command. Prepare and sign
//! stay inline in the manager.
//!
//! Reruns are safe: the signed transaction is persisted before broadcast and
//! reused until its submission resolves, and every outcome command is a no-op
//! once its event is recorded. A domain failure is recorded by the manager and
//! left to the burn recovery reconciler; an infrastructure failure surfaces as
//! a job error that apalis redrives.

use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;

use super::burn_manager::{
    BurnConfirmPlan, BurnExecutionPlan, BurnManager, BurnManagerError,
};
use super::{IssuerRedemptionRequestId, RedemptionError};
use crate::jobs::{Job, JobQueue, QueuePushError, job_type};
use crate::mint::recovery::release_terminal_job;
use crate::vault::TxId;

/// Failure of a burn side effect job. A domain rejection is recorded by the
/// manager instead (a `BurningFailed` event or an ambiguous broadcast left for
/// the recovery reconciler); these variants are the infrastructure failures
/// that make apalis redrive the job.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnJobError {
    #[error(transparent)]
    Manager(Box<BurnManagerError>),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Broadcasts the persisted burn transaction, then hands off to
/// [`ConfirmBurnJob`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SubmitBurnJob {
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) execution: BurnExecutionPlan,
}

pub(crate) struct SubmitBurnContext {
    pub(crate) burn_manager: Arc<BurnManager>,
    pub(crate) confirm_queue: JobQueue<ConfirmBurnJob>,
    /// Event-store pool; `release_terminal_job` frees a terminal prior confirm
    /// row so the handoff re-push is not silently dropped.
    pub(crate) pool: Pool<Sqlite>,
}

impl Job<SubmitBurnContext> for SubmitBurnJob {
    type Output = ();
    type Error = BurnJobError;

    async fn perform(
        &self,
        ctx: &SubmitBurnContext,
    ) -> Result<(), BurnJobError> {
        match ctx
            .burn_manager
            .submit_intended_burn(&self.issuer_request_id, &self.execution)
            .await
        {
            Ok(tx_id) => {
                self.enqueue_confirm(ctx, tx_id).await?;
                Ok(())
            }
            // The manager recorded the failure or left an ambiguous broadcast
            // for the burn recovery reconciler; this is not an apalis redrive.
            Err(BurnManagerError::Redemption(RedemptionError::Vault {
                ..
            })) => Ok(()),
            Err(other) => Err(BurnJobError::Manager(Box::new(other))),
        }
    }
}

impl SubmitBurnJob {
    async fn enqueue_confirm(
        &self,
        ctx: &SubmitBurnContext,
        tx_id: TxId,
    ) -> Result<(), BurnJobError> {
        let idempotency_key = BurnManager::confirm_burn_idempotency_key(
            &self.issuer_request_id,
            &tx_id,
        );
        release_terminal_job(
            &ctx.pool,
            job_type::<ConfirmBurnJob>(),
            &idempotency_key,
        )
        .await?;

        ctx.confirm_queue
            .clone()
            .push_with_idempotency_key(
                ConfirmBurnJob {
                    issuer_request_id: self.issuer_request_id.clone(),
                    execution: self.execution.confirm_plan(),
                    tx_id,
                },
                idempotency_key,
            )
            .await?;

        Ok(())
    }
}

/// Confirms a previously broadcast burn transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConfirmBurnJob {
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) execution: BurnConfirmPlan,
    pub(crate) tx_id: TxId,
}

pub(crate) struct ConfirmBurnContext {
    pub(crate) burn_manager: Arc<BurnManager>,
}

impl Job<ConfirmBurnContext> for ConfirmBurnJob {
    type Output = ();
    type Error = BurnJobError;

    async fn perform(
        &self,
        ctx: &ConfirmBurnContext,
    ) -> Result<(), BurnJobError> {
        match ctx
            .burn_manager
            .confirm_submitted_burn(
                &self.issuer_request_id,
                &self.execution,
                self.tx_id.clone(),
            )
            .await
        {
            // A definitive or uncertain confirm failure is handled inside the
            // manager (recorded, or left for the recovery reconciler), so an Ok
            // confirm and a swallowed Vault error both mean no apalis redrive.
            Ok(())
            | Err(BurnManagerError::Redemption(RedemptionError::Vault {
                ..
            })) => Ok(()),
            Err(other) => Err(BurnJobError::Manager(Box::new(other))),
        }
    }
}
