use std::sync::Arc;

use cqrs_es::{CqrsFramework, EventStore};
use tracing::{info, warn};

use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};

use super::{IssuerRequestId, Mint, MintCommand};

/// Orchestrates the Alpaca callback process in response to TokensMinted events.
///
/// The manager bridges ES/CQRS aggregates with external Alpaca services. It reacts to
/// TokensMinted events by calling the Alpaca service to send the mint callback, then records
/// the result back into the Mint aggregate via commands.
///
/// This pattern keeps aggregates pure (no side effects in command handlers) while enabling
/// integration with external systems.
pub(crate) struct CallbackManager<ES: EventStore<Mint>> {
    alpaca_service: Arc<dyn AlpacaService>,
    cqrs: Arc<CqrsFramework<Mint, ES>>,
}

impl<ES: EventStore<Mint>> CallbackManager<ES> {
    /// Creates a new callback manager.
    ///
    /// # Arguments
    ///
    /// * `alpaca_service` - Service for Alpaca API operations
    /// * `cqrs` - CQRS framework for executing commands on the Mint aggregate
    pub(crate) const fn new(
        alpaca_service: Arc<dyn AlpacaService>,
        cqrs: Arc<CqrsFramework<Mint, ES>>,
    ) -> Self {
        Self { alpaca_service, cqrs }
    }

    /// Handles a TokensMinted event by sending a callback to Alpaca.
    ///
    /// This method orchestrates the complete callback flow:
    /// 1. Validates the aggregate is in CallbackPending state
    /// 2. Builds MintCallbackRequest from aggregate data
    /// 3. Calls AlpacaService to send the callback
    /// 4. Records success (RecordCallback) via command
    ///
    /// # Arguments
    ///
    /// * `issuer_request_id` - ID of the mint request
    /// * `aggregate` - Current state of the Mint aggregate (must be CallbackPending)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if callback succeeded and RecordCallback command was executed.
    ///
    /// # Errors
    ///
    /// * `CallbackManagerError::InvalidAggregateState` - Aggregate is not in CallbackPending state
    /// * `CallbackManagerError::Alpaca` - Alpaca API call failed
    /// * `CallbackManagerError::Cqrs` - Command execution failed
    pub(crate) async fn handle_tokens_minted(
        &self,
        issuer_request_id: &IssuerRequestId,
        aggregate: &Mint,
    ) -> Result<(), CallbackManagerError> {
        let Mint::CallbackPending {
            tokenization_request_id,
            client_id,
            wallet,
            tx_hash,
            network,
            ..
        } = aggregate
        else {
            return Err(CallbackManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(aggregate).to_string(),
            });
        };

        let IssuerRequestId(issuer_request_id_str) = issuer_request_id;

        info!(
            issuer_request_id = %issuer_request_id_str,
            tx_hash = %tx_hash,
            "Starting Alpaca callback process"
        );

        let callback_request = MintCallbackRequest {
            tokenization_request_id: tokenization_request_id.clone(),
            client_id: client_id.clone(),
            wallet_address: *wallet,
            tx_hash: *tx_hash,
            network: network.clone(),
        };

        match self.alpaca_service.send_mint_callback(callback_request).await {
            Ok(()) => {
                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "Alpaca callback succeeded"
                );

                self.cqrs
                    .execute(
                        issuer_request_id_str,
                        MintCommand::RecordCallback {
                            issuer_request_id: issuer_request_id.clone(),
                        },
                    )
                    .await
                    .map_err(|e| CallbackManagerError::Cqrs(e.to_string()))?;

                info!(
                    issuer_request_id = %issuer_request_id_str,
                    "RecordCallback command executed successfully"
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    issuer_request_id = %issuer_request_id_str,
                    error = %e,
                    "Alpaca callback failed"
                );

                Err(CallbackManagerError::Alpaca(e))
            }
        }
    }
}

const fn aggregate_state_name(aggregate: &Mint) -> &'static str {
    match aggregate {
        Mint::Uninitialized => "Uninitialized",
        Mint::Initiated { .. } => "Initiated",
        Mint::JournalConfirmed { .. } => "JournalConfirmed",
        Mint::JournalRejected { .. } => "JournalRejected",
        Mint::CallbackPending { .. } => "CallbackPending",
        Mint::MintingFailed { .. } => "MintingFailed",
        Mint::Completed { .. } => "Completed",
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CallbackManagerError {
    #[error("Alpaca error: {0}")]
    Alpaca(#[from] AlpacaError),

    #[error("CQRS error: {0}")]
    Cqrs(String),

    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },
}
