mod api;
mod cmd;
mod event;
mod view;

use std::sync::Arc;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tracing::{info, warn};

use crate::account::view::AccountViewError;
use crate::alpaca::{AlpacaError, AlpacaService, MintCallbackRequest};
use crate::receipt_inventory::{
    ReceiptId, ReceiptLookupError, ReceiptService, Shares,
    view::ReceiptInventoryViewError,
};
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, find_vault_by_underlying,
};
use crate::vault::{MintResult, ReceiptInformation, VaultService};

pub use api::MintResponse;

pub(crate) use api::{confirm_journal, initiate_mint};
pub(crate) use cmd::MintCommand;
pub(crate) use event::MintEvent;
pub(crate) use view::{MintView, find_all_recoverable_mints, replay_mint_view};

/// Services required by the Mint aggregate for command handling.
pub(crate) struct MintServices {
    pub(crate) vault: Arc<dyn VaultService>,
    pub(crate) alpaca: Arc<dyn AlpacaService>,
    pub(crate) receipts: Arc<dyn ReceiptService>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) bot: Address,
}

pub(crate) use crate::account::ClientId;
pub(crate) use crate::tokenized_asset::{
    Network, TokenSymbol, UnderlyingSymbol,
};
pub(crate) use crate::{Quantity, QuantityConversionError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TokenizationRequestId {
    #[cfg(test)]
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IssuerRequestId(String);

impl IssuerRequestId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for IssuerRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Mint {
    Uninitialized,
    Initiated {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
    },
    JournalConfirmed {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    Minting {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        minting_started_at: DateTime<Utc>,
    },
    CallbackPending {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        error: String,
        failed_at: DateTime<Utc>,
    },
    Completed {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
}

impl Default for Mint {
    fn default() -> Self {
        Self::Uninitialized
    }
}

impl Mint {
    const fn state_name(&self) -> &'static str {
        match self {
            Self::Uninitialized => "Uninitialized",
            Self::Initiated { .. } => "Initiated",
            Self::JournalConfirmed { .. } => "JournalConfirmed",
            Self::JournalRejected { .. } => "JournalRejected",
            Self::Minting { .. } => "Minting",
            Self::CallbackPending { .. } => "CallbackPending",
            Self::MintingFailed { .. } => "MintingFailed",
            Self::Completed { .. } => "Completed",
        }
    }

    pub(crate) const fn tokenization_request_id(
        &self,
    ) -> Option<&TokenizationRequestId> {
        match self {
            Self::Initiated { tokenization_request_id, .. }
            | Self::JournalConfirmed { tokenization_request_id, .. }
            | Self::JournalRejected { tokenization_request_id, .. }
            | Self::Minting { tokenization_request_id, .. }
            | Self::CallbackPending { tokenization_request_id, .. }
            | Self::MintingFailed { tokenization_request_id, .. }
            | Self::Completed { tokenization_request_id, .. } => {
                Some(tokenization_request_id)
            }
            Self::Uninitialized => None,
        }
    }

    fn handle_confirm_journal(
        &self,
        provided_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::JournalConfirmed {
            issuer_request_id: provided_id,
            confirmed_at: now,
        }])
    }

    fn handle_reject_journal(
        &self,
        provided_id: IssuerRequestId,
        reason: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Initiated { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::JournalRejected {
            issuer_request_id: provided_id,
            reason,
            rejected_at: now,
        }])
    }

    fn validate_issuer_request_id(
        expected: &IssuerRequestId,
        provided: &IssuerRequestId,
    ) -> Result<(), MintError> {
        if provided != expected {
            return Err(MintError::IssuerRequestIdMismatch {
                expected: expected.0.clone(),
                provided: provided.0.clone(),
            });
        }
        Ok(())
    }

    async fn handle_deposit(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::JournalConfirmed {
            issuer_request_id: expected_id,
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        } = self
        else {
            return Err(MintError::NotInJournalConfirmedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        let vault = find_vault_by_underlying(&services.pool, underlying)
            .await?
            .ok_or_else(|| MintError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        let assets = quantity.to_u256_with_18_decimals()?;

        let receipt_info = ReceiptInformation::mint(
            tokenization_request_id.clone(),
            issuer_request_id.clone(),
            underlying.clone(),
            quantity.clone(),
            *journal_confirmed_at,
            None,
        );

        let now = Utc::now();

        match services
            .vault
            .mint_and_transfer_shares(
                vault,
                assets,
                services.bot,
                *wallet,
                receipt_info,
            )
            .await
        {
            Ok(result) => {
                Self::on_deposit_success(
                    services,
                    vault,
                    &issuer_request_id,
                    &result,
                    now,
                )
                .await
            }
            Err(err) => {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "On-chain deposit failed"
                );

                Ok(vec![
                    MintEvent::MintingStarted {
                        issuer_request_id: issuer_request_id.clone(),
                        started_at: now,
                    },
                    MintEvent::MintingFailed {
                        issuer_request_id,
                        error: err.to_string(),
                        failed_at: now,
                    },
                ])
            }
        }
    }

    async fn on_deposit_success(
        services: &MintServices,
        vault: Address,
        issuer_request_id: &IssuerRequestId,
        result: &MintResult,
        now: DateTime<Utc>,
    ) -> Result<Vec<MintEvent>, MintError> {
        info!(
            issuer_request_id = %issuer_request_id,
            tx_hash = %result.tx_hash,
            receipt_id = %result.receipt_id,
            shares_minted = %result.shares_minted,
            "On-chain deposit succeeded"
        );

        if let Err(err) = services
            .receipts
            .register_minted_receipt(
                vault,
                ReceiptId::from(result.receipt_id),
                Shares::from(result.shares_minted),
                result.block_number,
                result.tx_hash,
                issuer_request_id.clone(),
            )
            .await
        {
            warn!(
                issuer_request_id = %issuer_request_id,
                error = %err,
                "Failed to register minted receipt \
                 (monitor/backfill will discover it)"
            );
        }

        Ok(vec![
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: now,
            },
            MintEvent::TokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash: result.tx_hash,
                receipt_id: result.receipt_id,
                shares_minted: result.shares_minted,
                gas_used: result.gas_used,
                block_number: result.block_number,
                minted_at: now,
            },
        ])
    }

    async fn handle_send_callback(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::CallbackPending {
            issuer_request_id: expected_id,
            tokenization_request_id,
            client_id,
            wallet,
            tx_hash,
            network,
            ..
        } = self
        else {
            return Err(MintError::NotInCallbackPendingState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        let callback_request = MintCallbackRequest {
            tokenization_request_id: tokenization_request_id.clone(),
            client_id: *client_id,
            wallet_address: *wallet,
            tx_hash: *tx_hash,
            network: network.clone(),
        };

        services.alpaca.send_mint_callback(callback_request).await?;

        info!(
            issuer_request_id = %issuer_request_id,
            "Alpaca callback succeeded"
        );

        Ok(vec![MintEvent::MintCompleted {
            issuer_request_id,
            completed_at: Utc::now(),
        }])
    }

    async fn handle_recover(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        match self {
            Self::JournalConfirmed { .. } => {
                self.handle_deposit(services, issuer_request_id).await
            }
            Self::Minting { .. } | Self::MintingFailed { .. } => {
                self.handle_recover_incomplete(services, issuer_request_id)
                    .await
            }
            Self::CallbackPending { .. } => {
                self.handle_send_callback(services, issuer_request_id).await
            }
            _ => Err(MintError::NotRecoverable {
                current_state: self.state_name().to_string(),
            }),
        }
    }

    async fn handle_recover_incomplete(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let (Self::Minting {
            issuer_request_id: expected_id,
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        }
        | Self::MintingFailed {
            issuer_request_id: expected_id,
            tokenization_request_id,
            quantity,
            underlying,
            wallet,
            journal_confirmed_at,
            ..
        }) = self
        else {
            return Err(MintError::NotInMintingOrMintingFailedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &issuer_request_id)?;

        let vault = find_vault_by_underlying(&services.pool, underlying)
            .await?
            .ok_or_else(|| MintError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        if let Some(events) = Self::recover_from_existing_receipt(
            services,
            &vault,
            &issuer_request_id,
        )
        .await?
        {
            return Ok(events);
        }

        let receipt_info = ReceiptInformation::mint(
            tokenization_request_id.clone(),
            issuer_request_id.clone(),
            underlying.clone(),
            quantity.clone(),
            *journal_confirmed_at,
            Some("Recovery mint".to_string()),
        );

        let now = Utc::now();
        let retry_event =
            matches!(self, Self::MintingFailed { .. }).then(|| {
                MintEvent::MintRetryStarted {
                    issuer_request_id: issuer_request_id.clone(),
                    started_at: now,
                }
            });

        let mint_result = services
            .vault
            .mint_and_transfer_shares(
                vault,
                quantity.to_u256_with_18_decimals()?,
                services.bot,
                *wallet,
                receipt_info,
            )
            .await;

        let completion_event = match mint_result {
            Ok(result) => {
                info!(
                    issuer_request_id = %issuer_request_id,
                    tx_hash = %result.tx_hash,
                    "Recovery mint succeeded"
                );
                MintEvent::TokensMinted {
                    issuer_request_id,
                    tx_hash: result.tx_hash,
                    receipt_id: result.receipt_id,
                    shares_minted: result.shares_minted,
                    gas_used: result.gas_used,
                    block_number: result.block_number,
                    minted_at: now,
                }
            }
            Err(err) => {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Recovery mint failed"
                );
                MintEvent::MintingFailed {
                    issuer_request_id,
                    error: err.to_string(),
                    failed_at: now,
                }
            }
        };

        Ok(retry_event
            .into_iter()
            .chain(std::iter::once(completion_event))
            .collect())
    }

    async fn recover_from_existing_receipt(
        services: &MintServices,
        vault: &Address,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<Option<Vec<MintEvent>>, MintError> {
        let Some(receipt) = services
            .receipts
            .find_by_issuer_request_id(vault, issuer_request_id)
            .await?
        else {
            info!(issuer_request_id = %issuer_request_id, "No receipt found, retrying mint");
            return Ok(None);
        };

        info!(
            issuer_request_id = %issuer_request_id,
            receipt_id = %receipt.receipt_id,
            "Found existing receipt, recording recovery"
        );

        Ok(Some(vec![MintEvent::ExistingMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: receipt.tx_hash,
            receipt_id: receipt.receipt_id,
            shares_minted: receipt.shares,
            block_number: receipt.block_number,
            recovered_at: Utc::now(),
        }]))
    }

    fn apply_journal_confirmed(&mut self, confirmed_at: DateTime<Utc>) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at: confirmed_at,
        };
    }

    fn apply_journal_rejected(
        &mut self,
        reason: String,
        rejected_at: DateTime<Utc>,
    ) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalRejected {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            reason,
            rejected_at,
        };
    }

    fn apply_minting_started(&mut self, started_at: DateTime<Utc>) {
        let Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at: started_at,
        };
    }

    fn apply_tokens_minted(
        &mut self,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
    ) {
        let Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        } = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        };
    }

    fn apply_minting_failed(
        &mut self,
        error: String,
        failed_at: DateTime<Utc>,
    ) {
        let Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        } = self.clone()
        else {
            return;
        };

        *self = Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            error,
            failed_at,
        };
    }

    fn apply_mint_completed(&mut self, completed_at: DateTime<Utc>) {
        let Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::Completed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
            completed_at,
        };
    }

    fn apply_existing_mint_recorded(
        &mut self,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    ) {
        let (Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }
        | Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        }) = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used: None,
            block_number,
            minted_at: recovered_at,
        };
    }

    fn apply_mint_retry_started(&mut self, started_at: DateTime<Utc>) {
        let Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            ..
        } = self.clone()
        else {
            return;
        };

        *self = Self::Minting {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at: started_at,
        };
    }
}

#[async_trait]
impl Aggregate for Mint {
    type Command = MintCommand;
    type Event = MintEvent;
    type Error = MintError;
    type Services = MintServices;

    fn aggregate_type() -> String {
        "Mint".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            MintCommand::Initiate {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            } => {
                if matches!(self, Self::Initiated { .. }) {
                    return Err(MintError::AlreadyInitiated {
                        tokenization_request_id: tokenization_request_id.0,
                    });
                }

                let now = Utc::now();

                Ok(vec![MintEvent::Initiated {
                    issuer_request_id,
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at: now,
                }])
            }
            MintCommand::ConfirmJournal { issuer_request_id } => {
                self.handle_confirm_journal(issuer_request_id)
            }
            MintCommand::RejectJournal { issuer_request_id, reason } => {
                self.handle_reject_journal(issuer_request_id, reason)
            }
            MintCommand::Deposit { issuer_request_id } => {
                self.handle_deposit(services, issuer_request_id).await
            }
            MintCommand::SendCallback { issuer_request_id } => {
                self.handle_send_callback(services, issuer_request_id).await
            }
            MintCommand::Recover { issuer_request_id } => {
                self.handle_recover(services, issuer_request_id).await
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            MintEvent::Initiated {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at,
            } => {
                *self = Self::Initiated {
                    issuer_request_id,
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at,
                };
            }
            MintEvent::JournalConfirmed {
                issuer_request_id: _,
                confirmed_at,
            } => self.apply_journal_confirmed(confirmed_at),
            MintEvent::JournalRejected {
                issuer_request_id: _,
                reason,
                rejected_at,
            } => self.apply_journal_rejected(reason, rejected_at),
            MintEvent::MintingStarted { issuer_request_id: _, started_at } => {
                self.apply_minting_started(started_at);
            }
            MintEvent::TokensMinted {
                issuer_request_id: _,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
                minted_at,
            } => self.apply_tokens_minted(
                tx_hash,
                receipt_id,
                shares_minted,
                Some(gas_used),
                block_number,
                minted_at,
            ),
            MintEvent::MintingFailed {
                issuer_request_id: _,
                error,
                failed_at,
            } => self.apply_minting_failed(error, failed_at),
            MintEvent::MintCompleted { issuer_request_id: _, completed_at } => {
                self.apply_mint_completed(completed_at);
            }
            MintEvent::ExistingMintRecovered {
                issuer_request_id: _,
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
                recovered_at,
            } => self.apply_existing_mint_recorded(
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
                recovered_at,
            ),
            MintEvent::MintRetryStarted {
                issuer_request_id: _,
                started_at,
            } => {
                self.apply_mint_retry_started(started_at);
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintError {
    #[error(
        "Mint already initiated for tokenization request: {tokenization_request_id}"
    )]
    AlreadyInitiated { tokenization_request_id: String },
    #[error("Mint not in Initiated state. Current state: {current_state}")]
    NotInInitiatedState { current_state: String },
    #[error(
        "Mint not in JournalConfirmed state. Current state: {current_state}"
    )]
    NotInJournalConfirmedState { current_state: String },
    #[error(
        "Mint not in CallbackPending state. Current state: {current_state}"
    )]
    NotInCallbackPendingState { current_state: String },
    #[error(
        "Issuer request ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    IssuerRequestIdMismatch { expected: String, provided: String },
    #[error(
        "Mint not in Minting or MintingFailed state. Current state: {current_state}"
    )]
    NotInMintingOrMintingFailedState { current_state: String },
    #[error("Mint not in recoverable state. Current state: {current_state}")]
    NotRecoverable { current_state: String },
    #[error("Asset not found for underlying: {underlying}")]
    AssetNotFound { underlying: UnderlyingSymbol },
    #[error("Quantity conversion: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("View: {0}")]
    View(#[from] TokenizedAssetViewError),
    #[error("Alpaca: {0}")]
    Alpaca(#[from] AlpacaError),
    #[error("Account view: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Receipt inventory view: {0}")]
    ReceiptInventoryView(#[from] ReceiptInventoryViewError),
    #[error(transparent)]
    ReceiptLookup(#[from] ReceiptLookupError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use chrono::Utc;
    use cqrs_es::View;
    use cqrs_es::{
        Aggregate, AggregateContext, CqrsFramework, EventStore,
        mem_store::MemStore, persist::GenericQuery, test::TestFramework,
    };
    use rust_decimal::Decimal;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use std::sync::Arc;

    use super::{
        ClientId, Mint, MintCommand, MintError, MintEvent, MintServices,
        MintView, Network, Quantity, TokenSymbol, TokenizationRequestId,
        UnderlyingSymbol,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, view::TokenizedAssetView,
    };
    use crate::vault::mock::MockVaultService;

    type MintTestFramework = TestFramework<Mint>;

    fn test_mint_services() -> MintServices {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create runtime");

        let pool = rt.block_on(async {
            sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database")
        });

        let receipt_store = Arc::new(MemStore::<ReceiptInventory>::default());
        let receipt_cqrs =
            Arc::new(CqrsFramework::new((*receipt_store).clone(), vec![], ()));
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(
                receipt_store,
                receipt_cqrs,
            )),
            pool,
            bot,
        }
    }

    #[test]
    fn test_initiate_mint_creates_event() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = MintTestFramework::with(test_mint_services())
            .given_no_previous_events()
            .when(MintCommand::Initiate {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id,
                wallet,
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    MintEvent::Initiated {
                        issuer_request_id: event_issuer_id,
                        tokenization_request_id: event_tokenization_id,
                        quantity: event_quantity,
                        underlying: event_underlying,
                        token: event_token,
                        network: event_network,
                        client_id: event_client_id,
                        wallet: event_wallet,
                        initiated_at,
                    } => {
                        assert_eq!(event_issuer_id, &issuer_request_id);
                        assert_eq!(
                            event_tokenization_id,
                            &tokenization_request_id
                        );
                        assert_eq!(event_quantity, &quantity);
                        assert_eq!(event_underlying, &underlying);
                        assert_eq!(event_token, &token);
                        assert_eq!(event_network, &network);
                        assert_eq!(event_client_id, &client_id);
                        assert_eq!(event_wallet, &wallet);
                        assert!(initiated_at.timestamp() > 0);
                    }
                    MintEvent::JournalConfirmed { .. }
                    | MintEvent::JournalRejected { .. }
                    | MintEvent::MintingStarted { .. }
                    | MintEvent::TokensMinted { .. }
                    | MintEvent::MintingFailed { .. }
                    | MintEvent::MintCompleted { .. }
                    | MintEvent::ExistingMintRecovered { .. }
                    | MintEvent::MintRetryStarted { .. } => {
                        panic!(
                            "Expected MintInitiated event, got {:?}",
                            &events[0]
                        )
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_initiate_mint_when_already_initiated_returns_error() {
        let issuer_request_id = super::IssuerRequestId::new("iss-789");
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = MintTestFramework::with(test_mint_services())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id,
                wallet,
                initiated_at: chrono::Utc::now(),
            }])
            .when(MintCommand::Initiate {
                issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            })
            .inspect_result();

        assert!(
            matches!(result, Err(MintError::AlreadyInitiated { .. })),
            "Expected AlreadyInitiated error, got {result:?}"
        );
    }

    #[test]
    fn test_apply_initiated_event_updates_state() {
        let mut mint = Mint::default();

        assert!(matches!(mint, Mint::Uninitialized));

        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(50));
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = chrono::Utc::now();

        mint.apply(MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
        });

        let Mint::Initiated {
            issuer_request_id: applied_issuer_id,
            tokenization_request_id: applied_tokenization_id,
            quantity: applied_quantity,
            underlying: applied_underlying,
            token: applied_token,
            network: applied_network,
            client_id: applied_client_id,
            wallet: applied_wallet,
            initiated_at: applied_initiated_at,
        } = mint
        else {
            panic!("Expected Initiated, got Uninitialized")
        };

        assert_eq!(applied_issuer_id, issuer_request_id);
        assert_eq!(applied_tokenization_id, tokenization_request_id);
        assert_eq!(applied_quantity, quantity);
        assert_eq!(applied_underlying, underlying);
        assert_eq!(applied_token, token);
        assert_eq!(applied_network, network);
        assert_eq!(applied_client_id, client_id);
        assert_eq!(applied_wallet, wallet);
        assert_eq!(applied_initiated_at, initiated_at);
    }

    #[test]
    fn test_apply_journal_confirmed_event_updates_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut mint = Mint::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };

        let confirmed_at = Utc::now();

        mint.apply(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        });

        let Mint::JournalConfirmed {
            issuer_request_id: state_issuer_id,
            journal_confirmed_at,
            ..
        } = mint
        else {
            panic!("Expected JournalConfirmed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(journal_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_apply_journal_rejected_event_updates_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut mint = Mint::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };

        let rejected_at = Utc::now();
        let reason = "Insufficient funds".to_string();

        mint.apply(MintEvent::JournalRejected {
            issuer_request_id: issuer_request_id.clone(),
            reason: reason.clone(),
            rejected_at,
        });

        let Mint::JournalRejected {
            issuer_request_id: state_issuer_id,
            reason: state_reason,
            rejected_at: state_rejected_at,
            ..
        } = mint
        else {
            panic!("Expected JournalRejected state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_reason, reason);
        assert_eq!(state_rejected_at, rejected_at);
    }

    #[test]
    fn test_confirm_journal_produces_event() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = MintTestFramework::with(test_mint_services())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::ConfirmJournal { issuer_request_id });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], MintEvent::JournalConfirmed { .. }));
    }

    #[test]
    fn test_confirm_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = super::IssuerRequestId::new("iss-999");

        let validator = MintTestFramework::with(test_mint_services())
            .given_no_previous_events()
            .when(MintCommand::ConfirmJournal { issuer_request_id });

        let result = validator.inspect_result();

        assert!(matches!(result, Err(MintError::NotInInitiatedState { .. })));
    }

    #[test]
    fn test_confirm_journal_for_already_confirmed_mint_fails() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = MintTestFramework::with(test_mint_services())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at: Utc::now(),
                },
                MintEvent::JournalConfirmed {
                    issuer_request_id: issuer_request_id.clone(),
                    confirmed_at: Utc::now(),
                },
            ])
            .when(MintCommand::ConfirmJournal { issuer_request_id });

        let result = validator.inspect_result();

        assert!(matches!(result, Err(MintError::NotInInitiatedState { .. })));
    }

    #[test]
    fn test_reject_journal_produces_event() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let reason = "Insufficient funds";

        let validator = MintTestFramework::with(test_mint_services())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::RejectJournal {
                issuer_request_id,
                reason: reason.to_string(),
            });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);

        let MintEvent::JournalRejected { reason: event_reason, .. } =
            &events[0]
        else {
            panic!("Expected JournalRejected event, got {:?}", &events[0]);
        };

        assert_eq!(event_reason, reason);
    }

    #[test]
    fn test_reject_journal_for_uninitialized_mint_fails() {
        let issuer_request_id = super::IssuerRequestId::new("iss-999");

        let validator = MintTestFramework::with(test_mint_services())
            .given_no_previous_events()
            .when(MintCommand::RejectJournal {
                issuer_request_id,
                reason: "Test reason".to_string(),
            });

        let result = validator.inspect_result();

        assert!(matches!(result, Err(MintError::NotInInitiatedState { .. })));
    }

    #[test]
    fn test_confirm_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id =
            super::IssuerRequestId::new("iss-correct");
        let wrong_issuer_request_id = super::IssuerRequestId::new("iss-wrong");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = MintTestFramework::with(test_mint_services())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: correct_issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::ConfirmJournal {
                issuer_request_id: wrong_issuer_request_id,
            })
            .inspect_result();

        assert!(
            matches!(result, Err(MintError::IssuerRequestIdMismatch { .. })),
            "Expected IssuerRequestIdMismatch error, got {result:?}"
        );
    }

    #[test]
    fn test_reject_journal_with_mismatched_issuer_request_id_fails() {
        let correct_issuer_request_id =
            super::IssuerRequestId::new("iss-correct");
        let wrong_issuer_request_id = super::IssuerRequestId::new("iss-wrong");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = MintTestFramework::with(test_mint_services())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: correct_issuer_request_id,
                tokenization_request_id,
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
                initiated_at: Utc::now(),
            }])
            .when(MintCommand::RejectJournal {
                issuer_request_id: wrong_issuer_request_id,
                reason: "Test reason".to_string(),
            })
            .inspect_result();
        assert!(
            matches!(result, Err(MintError::IssuerRequestIdMismatch { .. })),
            "Expected IssuerRequestIdMismatch error, got {result:?}"
        );
    }

    #[test]
    fn test_apply_minting_started_event_updates_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut mint = Mint::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };

        let minting_started_at = Utc::now();
        mint.apply(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: minting_started_at,
        });

        let Mint::Minting {
            issuer_request_id: state_issuer_id,
            tokenization_request_id: state_tok_id,
            quantity: state_quantity,
            underlying: state_underlying,
            token: state_token,
            network: state_network,
            client_id: state_client_id,
            wallet: state_wallet,
            initiated_at: state_initiated_at,
            journal_confirmed_at: state_journal_confirmed_at,
            minting_started_at: state_minting_started_at,
        } = mint
        else {
            panic!("Expected Minting state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tok_id, tokenization_request_id);
        assert_eq!(state_quantity, quantity);
        assert_eq!(state_underlying, underlying);
        assert_eq!(state_token, token);
        assert_eq!(state_network, network);
        assert_eq!(state_client_id, client_id);
        assert_eq!(state_wallet, wallet);
        assert_eq!(state_initiated_at, initiated_at);
        assert_eq!(state_journal_confirmed_at, journal_confirmed_at);
        assert_eq!(state_minting_started_at, minting_started_at);
    }

    #[test]
    fn test_apply_tokens_minted_event_updates_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut mint = Mint::Minting {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at,
        };

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        mint.apply(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        });

        let Mint::CallbackPending {
            issuer_request_id: state_issuer_id,
            tx_hash: state_tx_hash,
            receipt_id: state_receipt_id,
            shares_minted: state_shares_minted,
            gas_used: state_gas_used,
            block_number: state_block_number,
            minted_at: state_minted_at,
            ..
        } = mint
        else {
            panic!("Expected CallbackPending state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_receipt_id, receipt_id);
        assert_eq!(state_shares_minted, shares_minted);
        assert_eq!(state_gas_used, Some(gas_used));
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_minted_at, minted_at);
    }

    #[test]
    fn test_apply_minting_failed_event_updates_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut mint = Mint::Minting {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            minting_started_at,
        };

        let error_message = "Transaction failed: insufficient gas";
        let failed_at = Utc::now();

        mint.apply(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: error_message.to_string(),
            failed_at,
        });

        let Mint::MintingFailed {
            issuer_request_id: state_issuer_id,
            error: state_error,
            failed_at: state_failed_at,
            ..
        } = mint
        else {
            panic!("Expected MintingFailed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_error, error_message);
        assert_eq!(state_failed_at, failed_at);
    }

    #[test]
    fn test_apply_mint_completed_event_updates_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        let mut mint = Mint::CallbackPending {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used: Some(gas_used),
            block_number,
            minted_at,
        };

        let completed_at = Utc::now();

        mint.apply(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at,
        });

        let Mint::Completed {
            issuer_request_id: state_issuer_id,
            tx_hash: state_tx_hash,
            receipt_id: state_receipt_id,
            shares_minted: state_shares_minted,
            gas_used: state_gas_used,
            block_number: state_block_number,
            minted_at: state_minted_at,
            completed_at: state_completed_at,
            ..
        } = mint
        else {
            panic!("Expected Completed state, got {mint:?}");
        };

        assert_eq!(state_issuer_id, issuer_request_id);
        assert_eq!(state_tx_hash, tx_hash);
        assert_eq!(state_receipt_id, receipt_id);
        assert_eq!(state_shares_minted, shares_minted);
        assert_eq!(state_gas_used, Some(gas_used));
        assert_eq!(state_block_number, block_number);
        assert_eq!(state_minted_at, minted_at);
        assert_eq!(state_completed_at, completed_at);
    }

    #[test]
    fn test_full_mint_flow_to_completed() {
        let issuer_request_id = super::IssuerRequestId::new("iss-flow-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-flow-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();
        let completed_at = Utc::now();

        let mut mint = Mint::default();

        mint.apply(MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        });

        assert!(
            matches!(mint, Mint::Initiated { .. }),
            "Expected Initiated state, got {mint:?}"
        );

        mint.apply(MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        });

        assert!(
            matches!(mint, Mint::JournalConfirmed { .. }),
            "Expected JournalConfirmed state, got {mint:?}"
        );

        let minting_started_at = Utc::now();
        mint.apply(MintEvent::MintingStarted {
            issuer_request_id: issuer_request_id.clone(),
            started_at: minting_started_at,
        });

        assert!(
            matches!(mint, Mint::Minting { .. }),
            "Expected Minting state, got {mint:?}"
        );

        mint.apply(MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        });

        assert!(
            matches!(mint, Mint::CallbackPending { .. }),
            "Expected CallbackPending state, got {mint:?}"
        );

        mint.apply(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at,
        });

        let Mint::Completed {
            issuer_request_id: final_id,
            completed_at: final_completed_at,
            ..
        } = mint
        else {
            panic!("Expected Completed state, got {mint:?}");
        };

        assert_eq!(final_id, issuer_request_id);
        assert_eq!(final_completed_at, completed_at);
    }

    struct TestMintData {
        issuer_request_id: super::IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: alloy::primitives::Address,
    }

    impl TestMintData {
        fn new() -> Self {
            Self {
                issuer_request_id: super::IssuerRequestId::new(
                    "iss-integration-123",
                ),
                tokenization_request_id: TokenizationRequestId::new(
                    "alp-integration-456",
                ),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            }
        }
    }

    async fn setup_cqrs_integration_test()
    -> (Arc<MemStore<Mint>>, Arc<CqrsFramework<Mint, MemStore<Mint>>>) {
        use sqlx::sqlite::SqlitePoolOptions;

        let store = Arc::new(MemStore::<Mint>::default());

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        let receipt_store = Arc::new(MemStore::<ReceiptInventory>::default());
        let receipt_cqrs =
            Arc::new(CqrsFramework::new((*receipt_store).clone(), vec![], ()));
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let mint_services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            pool: pool.clone(),
            bot,
            receipts: Arc::new(CqrsReceiptService::new(
                receipt_store,
                receipt_cqrs,
            )),
        };

        let cqrs = Arc::new(CqrsFramework::new(
            (*store).clone(),
            vec![],
            mint_services,
        ));

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let asset_view_repo = Arc::new(SqliteViewRepository::<
            TokenizedAssetView,
            TokenizedAsset,
        >::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));
        let asset_query = GenericQuery::new(asset_view_repo);
        let asset_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(asset_query)], ());

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");

        asset_cqrs
            .execute(
                &underlying.0,
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    vault,
                },
            )
            .await
            .expect("Failed to add tokenized asset");

        (store, cqrs)
    }

    #[tokio::test]
    async fn test_complete_mint_flow_via_cqrs() {
        let (store, cqrs) = setup_cqrs_integration_test().await;
        let data = TestMintData::new();

        cqrs.execute(
            &data.issuer_request_id.0,
            MintCommand::Initiate {
                issuer_request_id: data.issuer_request_id.clone(),
                tokenization_request_id: data.tokenization_request_id.clone(),
                quantity: data.quantity.clone(),
                underlying: data.underlying.clone(),
                token: data.token.clone(),
                network: data.network.clone(),
                client_id: data.client_id,
                wallet: data.wallet,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &data.issuer_request_id.0,
            MintCommand::ConfirmJournal {
                issuer_request_id: data.issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &data.issuer_request_id.0,
            MintCommand::Deposit {
                issuer_request_id: data.issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &data.issuer_request_id.0,
            MintCommand::SendCallback {
                issuer_request_id: data.issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        let context =
            store.load_aggregate(&data.issuer_request_id.0).await.unwrap();

        assert!(
            matches!(context.aggregate(), Mint::Completed { .. }),
            "Expected Completed state, got {:?}",
            context.aggregate()
        );

        let events =
            store.load_events(&data.issuer_request_id.0).await.unwrap();

        assert_eq!(events.len(), 5, "Expected 5 events in the flow");

        assert!(
            matches!(&events[0].payload, MintEvent::Initiated { .. }),
            "First event should be Initiated"
        );
        assert!(
            matches!(&events[1].payload, MintEvent::JournalConfirmed { .. }),
            "Second event should be JournalConfirmed"
        );
        assert!(
            matches!(&events[2].payload, MintEvent::MintingStarted { .. }),
            "Third event should be MintingStarted"
        );
        assert!(
            matches!(&events[3].payload, MintEvent::TokensMinted { .. }),
            "Fourth event should be TokensMinted"
        );
        assert!(
            matches!(&events[4].payload, MintEvent::MintCompleted { .. }),
            "Fifth event should be MintCompleted"
        );

        let mut view = MintView::default();
        for event in &events {
            view.update(event);
        }

        let MintView::Completed {
            issuer_request_id: view_issuer_id,
            tx_hash: view_tx_hash,
            completed_at: view_completed_at,
            initiated_at: view_initiated_at,
            journal_confirmed_at: view_journal_confirmed_at,
            minted_at: view_minted_at,
            ..
        } = view
        else {
            panic!("Expected Completed view, got {view:?}");
        };

        assert_eq!(view_issuer_id, data.issuer_request_id);
        assert!(view_tx_hash != alloy::primitives::B256::ZERO);
        assert!(view_initiated_at.timestamp() > 0);
        assert!(view_journal_confirmed_at.timestamp() > 0);
        assert!(view_minted_at.timestamp() > 0);
        assert!(view_completed_at.timestamp() > 0);
        assert!(view_completed_at >= view_minted_at);
        assert!(view_minted_at >= view_journal_confirmed_at);
        assert!(view_journal_confirmed_at >= view_initiated_at);
    }

    #[test]
    fn test_issuer_request_id_display() {
        let id = super::IssuerRequestId::new("iss-123");
        assert_eq!(format!("{id}"), "iss-123");
    }

    #[test]
    fn test_tokenization_request_id_display() {
        let id = TokenizationRequestId::new("alp-456");
        assert_eq!(format!("{id}"), "alp-456");
    }
}
