use apalis::prelude::Data;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::info;

use crate::mint::replay_mint_view;
use crate::receipt_inventory::burn_tracking::replay_receipt_burns_view;
use crate::redemption::replay_redemption_view;
use crate::tokenized_asset::view::replay_tokenized_asset_view;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ViewReplayJob;

impl ViewReplayJob {
    pub(crate) async fn run(
        self,
        pool: Data<Pool<Sqlite>>,
    ) -> Result<(), anyhow::Error> {
        info!("Replaying views to ensure schema updates are applied");

        let pool: Pool<Sqlite> = (*pool).clone();

        replay_tokenized_asset_view(pool.clone()).await?;
        replay_mint_view(pool.clone()).await?;
        replay_redemption_view(pool.clone()).await?;
        replay_receipt_burns_view(pool).await?;

        info!("View replay complete");

        Ok(())
    }
}
