use alloy::primitives::Address;
use alloy::providers::DynProvider;
use apalis::prelude::Data;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, info};

use super::backfill::TransferBackfiller;
use crate::{
    RedemptionCqrs, RedemptionEventStore, RedemptionManagers, VaultConfigs,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TransferBackfillJob;

pub(crate) struct TransferBackfillDeps {
    pub(crate) provider: DynProvider,
    pub(crate) bot_wallet: Address,
    pub(crate) cqrs: RedemptionCqrs,
    pub(crate) event_store: RedemptionEventStore,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) managers: RedemptionManagers,
    pub(crate) backfill_start_block: u64,
}

impl TransferBackfillJob {
    pub(crate) async fn run(
        self,
        deps: Data<Arc<TransferBackfillDeps>>,
        vault_configs: Data<VaultConfigs>,
    ) {
        let configs = {
            let guard = vault_configs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.clone()
        };

        let Some(configs) = configs else {
            error!(
                "Transfer backfill skipped: vault configs not available (receipt backfill may have failed)"
            );
            return;
        };

        if configs.is_empty() {
            info!("No enabled vaults, skipping transfer backfill");
            return;
        }

        let backfiller = TransferBackfiller {
            provider: deps.provider.clone(),
            bot_wallet: deps.bot_wallet,
            cqrs: deps.cqrs.clone(),
            event_store: deps.event_store.clone(),
            pool: deps.pool.clone(),
            redeem_call_manager: deps.managers.redeem_call.clone(),
            journal_manager: deps.managers.journal.clone(),
            burn_manager: deps.managers.burn.clone(),
        };

        info!(
            vault_count = configs.len(),
            "Running transfer backfill for all vaults"
        );

        for config in &configs {
            match backfiller
                .backfill_transfers(config.vault, deps.backfill_start_block)
                .await
            {
                Ok(result) => {
                    info!(
                        vault = %config.vault,
                        detected = result.detected_count,
                        skipped_mint = result.skipped_mint,
                        skipped_no_account = result.skipped_no_account,
                        "Transfer backfill complete for vault"
                    );
                }
                Err(err) => {
                    error!(
                        vault = %config.vault,
                        error = %err,
                        "Transfer backfill failed for vault"
                    );
                }
            }
        }
    }
}
