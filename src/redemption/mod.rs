mod cmd;
mod event;
mod view;

use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};

use crate::mint::{IssuerRequestId, Quantity};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
pub(crate) use cmd::RedemptionCommand;
pub(crate) use event::RedemptionEvent;
pub(crate) use view::RedemptionView;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Redemption {
    Uninitialized,
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        detected_tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
}

impl Default for Redemption {
    fn default() -> Self {
        Self::Uninitialized
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum RedemptionError {
    #[error("Redemption already detected for request: {issuer_request_id}")]
    AlreadyDetected { issuer_request_id: String },
}

#[async_trait]
impl Aggregate for Redemption {
    type Command = RedemptionCommand;
    type Event = RedemptionEvent;
    type Error = RedemptionError;
    type Services = ();

    fn aggregate_type() -> String {
        "Redemption".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            RedemptionCommand::Detect {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
            } => {
                if !matches!(self, Self::Uninitialized) {
                    return Err(RedemptionError::AlreadyDetected {
                        issuer_request_id: issuer_request_id.0,
                    });
                }

                let now = Utc::now();

                Ok(vec![RedemptionEvent::Detected {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                    detected_at: now,
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            } => {
                *self = Self::Detected {
                    issuer_request_id,
                    underlying,
                    token,
                    wallet,
                    quantity,
                    detected_tx_hash: tx_hash,
                    block_number,
                    detected_at,
                };
            }
        }
    }
}
