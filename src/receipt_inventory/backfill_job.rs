use alloy::primitives::Address;
use alloy::providers::DynProvider;
use apalis::prelude::Data;
use cqrs_es::{AggregateContext, EventStore};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, info};

use super::backfill::ReceiptBackfiller;
use crate::tokenized_asset::TokenizedAssetView;
use crate::tokenized_asset::VaultBackfillConfig;
use crate::tokenized_asset::view::list_enabled_assets;
use crate::{
    ReceiptInventoryCqrs, ReceiptInventoryEventStore, VaultConfigs, bindings,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReceiptBackfillJob;

pub(crate) struct ReceiptBackfillDeps {
    pub(crate) provider: DynProvider,
    pub(crate) receipt_inventory_cqrs: ReceiptInventoryCqrs,
    pub(crate) receipt_inventory_event_store: ReceiptInventoryEventStore,
    pub(crate) bot_wallet: Address,
    pub(crate) backfill_start_block: u64,
}

impl ReceiptBackfillJob {
    pub(crate) async fn run(
        self,
        pool: Data<Pool<Sqlite>>,
        deps: Data<Arc<ReceiptBackfillDeps>>,
        vault_configs: Data<VaultConfigs>,
    ) {
        match run_all_receipt_backfills(&pool, &deps).await {
            Ok(configs) => {
                let mut guard = vault_configs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *guard = Some(configs);
                drop(guard);
                info!("Receipt backfill complete");
            }
            Err(err) => {
                error!(error = %err, "Receipt backfill failed");
            }
        }
    }
}

async fn run_all_receipt_backfills(
    pool: &Pool<Sqlite>,
    deps: &ReceiptBackfillDeps,
) -> Result<Vec<VaultBackfillConfig>, anyhow::Error> {
    let assets = list_enabled_assets(pool).await?;

    if assets.is_empty() {
        info!("No enabled tokenized assets found, skipping receipt backfill");
        return Ok(vec![]);
    }

    info!(
        asset_count = assets.len(),
        "Running receipt backfill for all enabled assets"
    );

    let vaults: Vec<_> = assets
        .into_iter()
        .filter_map(|asset| match asset {
            TokenizedAssetView::Asset { vault, underlying, .. } => {
                Some((vault, underlying))
            }
            TokenizedAssetView::Unavailable => None,
        })
        .collect();

    stream::iter(vaults)
        .then(|(vault, underlying)| async move {
            run_single_vault_backfill(
                &deps.provider,
                vault,
                deps.backfill_start_block,
                &underlying.0,
                &deps.receipt_inventory_cqrs,
                &deps.receipt_inventory_event_store,
                deps.bot_wallet,
            )
            .await
        })
        .try_collect()
        .await
}

async fn run_single_vault_backfill(
    provider: &DynProvider,
    vault: Address,
    backfill_start_block: u64,
    underlying: &str,
    receipt_inventory_cqrs: &ReceiptInventoryCqrs,
    receipt_inventory_event_store: &ReceiptInventoryEventStore,
    bot_wallet: Address,
) -> Result<VaultBackfillConfig, anyhow::Error> {
    let vault_contract =
        bindings::OffchainAssetReceiptVault::new(vault, provider);
    let receipt_contract =
        Address::from(vault_contract.receipt().call().await?.0);

    let aggregate_context = receipt_inventory_event_store
        .load_aggregate(&vault.to_string())
        .await?;

    let from_block = aggregate_context
        .aggregate()
        .last_backfilled_block()
        .unwrap_or(backfill_start_block);

    info!(
        underlying,
        vault = %vault,
        receipt_contract = %receipt_contract,
        bot_wallet = %bot_wallet,
        from_block,
        "Running receipt backfill for vault"
    );

    let backfiller = ReceiptBackfiller::new(
        provider.clone(),
        receipt_contract,
        bot_wallet,
        vault,
        receipt_inventory_cqrs.clone(),
    );

    let result = backfiller.backfill_receipts(from_block).await?;

    info!(
        underlying,
        vault = %vault,
        processed = result.processed_count,
        skipped_zero_balance = result.skipped_zero_balance,
        "Receipt backfill complete for vault"
    );

    Ok(VaultBackfillConfig { vault, receipt_contract })
}
