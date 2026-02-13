use apalis::prelude::Data;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

use crate::RedemptionManagers;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RedemptionRecoveryJob;

impl RedemptionRecoveryJob {
    pub(crate) async fn run(self, managers: Data<Arc<RedemptionManagers>>) {
        info!("Starting redemption recovery");

        managers.redeem_call.recover_detected_redemptions().await;
        managers.journal.recover_alpaca_called_redemptions().await;
        managers.burn.recover_burning_redemptions().await;
        managers.burn.recover_burn_failed_redemptions().await;

        info!("Completed redemption recovery");
    }
}
