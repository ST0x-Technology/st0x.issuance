use apalis::prelude::Data;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::info;

use crate::job::{Job, Label};
use crate::mint::{MintViewError, replay_mint_view};
use crate::receipt_inventory::burn_tracking::{
    BurnTrackingError, replay_receipt_burns_view,
};
use crate::redemption::{RedemptionViewError, replay_redemption_view};
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, replay_tokenized_asset_view,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ViewReplayJob;

impl Job for ViewReplayJob {
    type Ctx = Pool<Sqlite>;
    type Error = ViewReplayError;

    fn label(&self) -> Label {
        Label::new("view-replay")
    }

    async fn run(
        self,
        pool: Data<Pool<Sqlite>>,
    ) -> Result<(), ViewReplayError> {
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum ViewReplayError {
    #[error("Tokenized asset view replay failed: {0}")]
    TokenizedAsset(#[from] TokenizedAssetViewError),
    #[error("Mint view replay failed: {0}")]
    Mint(#[from] MintViewError),
    #[error("Redemption view replay failed: {0}")]
    Redemption(#[from] RedemptionViewError),
    #[error("Burn tracking view replay failed: {0}")]
    BurnTracking(#[from] BurnTrackingError),
}
