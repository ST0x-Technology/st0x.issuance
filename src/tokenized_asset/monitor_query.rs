use alloy::primitives::Address;
use alloy::providers::DynProvider;
use apalis::prelude::Storage;
use apalis_sql::sqlite::SqliteStorage;
use async_trait::async_trait;
use cqrs_es::{EventEnvelope, Query};
use sqlx::{Pool, Sqlite};
use task_supervisor::{
    SupervisorBuilder, SupervisorHandle, SupervisorHandleError,
};
use tokio::time::Duration;
use tracing::{error, info};
use url::Url;

use super::{TokenizedAsset, TokenizedAssetEvent, UnderlyingSymbol, VaultCtx};
use crate::RedemptionManagers;
use crate::mint::recovery::MintRecoveryHandler;
use crate::receipt_inventory::backfill::ReceiptBackfillJob;
use crate::receipt_inventory::{ReceiptMonitor, ReceiptMonitorCtx};
use crate::redemption::backfill::TransferBackfillJob;
use crate::redemption::detector::{RedemptionDetector, RedemptionDetectorCtx};
use crate::{
    MintCqrs, ReceiptInventoryCqrs, RedemptionCqrs, RedemptionEventStore,
};

pub(crate) struct MonitorQuerySetup<'a> {
    pub(crate) rpc_url: Url,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) bot_wallet: Address,
    pub(crate) vault_ctxs: &'a [VaultCtx],
    pub(crate) provider: DynProvider,
    pub(crate) mint_cqrs: &'a MintCqrs,
    pub(crate) receipt_inventory_cqrs: ReceiptInventoryCqrs,
    pub(crate) redemption_cqrs: RedemptionCqrs,
    pub(crate) redemption_event_store: RedemptionEventStore,
    pub(crate) managers: RedemptionManagers,
    pub(crate) receipt_backfill_job_queue: SqliteStorage<ReceiptBackfillJob>,
    pub(crate) transfer_backfill_job_queue: SqliteStorage<TransferBackfillJob>,
}

/// Reacts to committed `TokenizedAssetEvent`s by starting/restarting monitors
/// and pushing backfill jobs. Registered as a CQRS query so it fires
/// automatically after each successful `cqrs.execute()`.
pub(crate) struct TokenizedAssetMonitorQuery {
    pub(crate) supervisor_handle: SupervisorHandle,
    pub(crate) provider: DynProvider,
    pub(crate) rpc_url: Url,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) bot_wallet: Address,
    pub(crate) receipt_inventory_cqrs: ReceiptInventoryCqrs,
    pub(crate) itn_handler: MintRecoveryHandler,
    pub(crate) redemption_cqrs: RedemptionCqrs,
    pub(crate) redemption_event_store: RedemptionEventStore,
    pub(crate) managers: RedemptionManagers,
    pub(crate) receipt_backfill_job_queue: SqliteStorage<ReceiptBackfillJob>,
    pub(crate) transfer_backfill_job_queue: SqliteStorage<TransferBackfillJob>,
}

#[async_trait]
impl Query<TokenizedAsset> for TokenizedAssetMonitorQuery {
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<TokenizedAsset>],
    ) {
        let underlying = UnderlyingSymbol::new(aggregate_id);

        for envelope in events {
            let result = match &envelope.payload {
                TokenizedAssetEvent::Added { .. } => {
                    info!(
                        underlying = %underlying,
                        "Starting monitors and backfills for new asset"
                    );

                    let vault_ctx = super::discover_vault(
                        &self.pool,
                        &self.provider,
                        &underlying,
                    )
                    .await;

                    match vault_ctx {
                        Ok(vault_ctx) => self
                            .push_backfill_jobs(&vault_ctx)
                            .await
                            .and_then(|()| self.add_monitors(&underlying)),
                        Err(err) => Err(err.into()),
                    }
                }
                TokenizedAssetEvent::VaultAddressUpdated { .. } => {
                    info!(
                        underlying = %underlying,
                        "Restarting monitors for address change"
                    );

                    self.restart_monitors(&underlying)
                }
            };

            if let Err(err) = result {
                error!(
                    error = %err,
                    underlying = %underlying,
                    "Failed to react to asset event"
                );
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum TokenizedAssetMonitorError {
    #[error(transparent)]
    VaultDiscovery(#[from] super::view::VaultDiscoveryError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Supervisor(#[from] SupervisorHandleError),
}

impl TokenizedAssetMonitorQuery {
    pub(crate) fn new(
        setup: MonitorQuerySetup<'_>,
    ) -> (Self, SupervisorHandle) {
        let mut supervisor_builder = SupervisorBuilder::new()
            .with_unlimited_restarts()
            .with_base_restart_delay(Duration::from_secs(5))
            .with_health_check_interval(Duration::from_secs(60));

        let itn_handler = MintRecoveryHandler::new(setup.mint_cqrs.clone());

        for vault_ctx in setup.vault_ctxs {
            let receipt_ctx = ReceiptMonitorCtx {
                underlying: vault_ctx.underlying.clone(),
                bot_wallet: setup.bot_wallet,
                pool: setup.pool.clone(),
            };
            let receipt_task_name = receipt_ctx.task_name();

            let receipt_monitor = ReceiptMonitor::new(
                setup.provider.clone(),
                receipt_ctx,
                setup.receipt_inventory_cqrs.clone(),
                itn_handler.clone(),
            );

            supervisor_builder = supervisor_builder
                .with_task(&receipt_task_name, receipt_monitor);

            let detector_ctx = RedemptionDetectorCtx {
                rpc_url: setup.rpc_url.clone(),
                underlying: vault_ctx.underlying.clone(),
                bot_wallet: setup.bot_wallet,
                pool: setup.pool.clone(),
            };
            let detector_task_name = detector_ctx.task_name();

            let detector = RedemptionDetector::new(
                detector_ctx,
                setup.redemption_cqrs.clone(),
                setup.redemption_event_store.clone(),
                setup.managers.redeem_call.clone(),
                setup.managers.journal.clone(),
                setup.managers.burn.clone(),
            );

            supervisor_builder =
                supervisor_builder.with_task(&detector_task_name, detector);
        }

        let supervisor = supervisor_builder.build();
        let supervisor_handle = supervisor.run();

        info!(
            vault_count = setup.vault_ctxs.len(),
            "Task supervisor started with all monitors"
        );

        let query = Self {
            supervisor_handle: supervisor_handle.clone(),
            provider: setup.provider,
            rpc_url: setup.rpc_url,
            pool: setup.pool,
            bot_wallet: setup.bot_wallet,
            receipt_inventory_cqrs: setup.receipt_inventory_cqrs,
            itn_handler,
            redemption_cqrs: setup.redemption_cqrs,
            redemption_event_store: setup.redemption_event_store,
            managers: setup.managers,
            receipt_backfill_job_queue: setup.receipt_backfill_job_queue,
            transfer_backfill_job_queue: setup.transfer_backfill_job_queue,
        };

        (query, supervisor_handle)
    }

    fn monitor_ctxs(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> (ReceiptMonitorCtx, RedemptionDetectorCtx) {
        let receipt = ReceiptMonitorCtx {
            underlying: underlying.clone(),
            bot_wallet: self.bot_wallet,
            pool: self.pool.clone(),
        };

        let detector = RedemptionDetectorCtx {
            rpc_url: self.rpc_url.clone(),
            underlying: underlying.clone(),
            bot_wallet: self.bot_wallet,
            pool: self.pool.clone(),
        };

        (receipt, detector)
    }

    fn restart_monitors(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<(), TokenizedAssetMonitorError> {
        let (receipt_ctx, detector_ctx) = self.monitor_ctxs(underlying);

        self.supervisor_handle.restart(&receipt_ctx.task_name())?;

        self.supervisor_handle.restart(&detector_ctx.task_name())?;

        Ok(())
    }

    async fn push_backfill_jobs(
        &self,
        vault_ctx: &super::VaultCtx,
    ) -> Result<(), TokenizedAssetMonitorError> {
        let mut receipt_queue = self.receipt_backfill_job_queue.clone();
        receipt_queue
            .push(ReceiptBackfillJob {
                vault: vault_ctx.vault,
                receipt_contract: vault_ctx.receipt_contract,
            })
            .await?;

        let mut transfer_queue = self.transfer_backfill_job_queue.clone();
        transfer_queue
            .push(TransferBackfillJob { vault: vault_ctx.vault })
            .await?;

        Ok(())
    }

    fn add_monitors(
        &self,
        underlying: &UnderlyingSymbol,
    ) -> Result<(), TokenizedAssetMonitorError> {
        let (receipt_ctx, detector_ctx) = self.monitor_ctxs(underlying);
        let receipt_task_name = receipt_ctx.task_name();
        let detector_task_name = detector_ctx.task_name();

        let receipt_monitor = ReceiptMonitor::new(
            self.provider.clone(),
            receipt_ctx,
            self.receipt_inventory_cqrs.clone(),
            self.itn_handler.clone(),
        );

        self.supervisor_handle.add_task(&receipt_task_name, receipt_monitor)?;

        let detector = RedemptionDetector::new(
            detector_ctx,
            self.redemption_cqrs.clone(),
            self.redemption_event_store.clone(),
            self.managers.redeem_call.clone(),
            self.managers.journal.clone(),
            self.managers.burn.clone(),
        );

        self.supervisor_handle.add_task(&detector_task_name, detector)?;

        Ok(())
    }
}
