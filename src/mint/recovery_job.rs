use apalis::prelude::Data;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::{debug, error, info};

use super::{find_all_recoverable_mints, recovery::recover_mint};
use crate::MintCqrs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MintRecoveryJob;

impl MintRecoveryJob {
    pub(crate) async fn run(
        self,
        pool: Data<Pool<Sqlite>>,
        mint_cqrs: Data<MintCqrs>,
    ) {
        info!("Starting mint recovery");

        let recoverable_mints = match find_all_recoverable_mints(&pool).await {
            Ok(mints) => mints,
            Err(err) => {
                error!(error = %err, "Failed to query recoverable mints");
                return;
            }
        };

        if recoverable_mints.is_empty() {
            debug!("No mints to recover");
            return;
        }

        info!(count = recoverable_mints.len(), "Recovering mints");

        for (issuer_request_id, _view) in recoverable_mints {
            recover_mint(&mint_cqrs, issuer_request_id).await;
        }

        debug!("Completed mint recovery");
    }
}
