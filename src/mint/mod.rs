mod api;
mod callback_manager;
mod cmd;
mod event;
pub(crate) mod mint_manager;
mod view;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};

pub use api::MintResponse;

pub(crate) use api::{confirm_journal, initiate_mint};
pub(crate) use callback_manager::CallbackManager;
pub(crate) use cmd::MintCommand;
pub(crate) use event::MintEvent;
pub(crate) use view::MintView;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerRequestId(pub String);

impl IssuerRequestId {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
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
        gas_used: u64,
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
        gas_used: u64,
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

    pub(crate) const fn client_id(&self) -> Option<&ClientId> {
        match self {
            Self::Initiated { client_id, .. }
            | Self::JournalConfirmed { client_id, .. }
            | Self::JournalRejected { client_id, .. }
            | Self::Minting { client_id, .. }
            | Self::CallbackPending { client_id, .. }
            | Self::MintingFailed { client_id, .. }
            | Self::Completed { client_id, .. } => Some(client_id),
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

    fn handle_start_minting(
        &self,
        provided_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::JournalConfirmed { issuer_request_id: expected_id, .. } =
            self
        else {
            return Err(MintError::NotInJournalConfirmedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::MintingStarted {
            issuer_request_id: provided_id,
            started_at: now,
        }])
    }

    fn handle_record_mint_success(
        &self,
        provided_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Minting { issuer_request_id: expected_id, .. } = self else {
            return Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::TokensMinted {
            issuer_request_id: provided_id,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at: now,
        }])
    }

    fn handle_record_mint_failure(
        &self,
        provided_id: IssuerRequestId,
        error: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::Minting { issuer_request_id: expected_id, .. } = self else {
            return Err(MintError::NotInMintingState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::MintingFailed {
            issuer_request_id: provided_id,
            error,
            failed_at: now,
        }])
    }

    fn handle_record_callback(
        &self,
        provided_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let Self::CallbackPending { issuer_request_id: expected_id, .. } = self
        else {
            return Err(MintError::NotInCallbackPendingState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(expected_id, &provided_id)?;

        let now = Utc::now();

        Ok(vec![MintEvent::MintCompleted {
            issuer_request_id: provided_id,
            completed_at: now,
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
        gas_used: u64,
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
}

#[async_trait]
impl Aggregate for Mint {
    type Command = MintCommand;
    type Event = MintEvent;
    type Error = MintError;
    type Services = ();

    fn aggregate_type() -> String {
        "Mint".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
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
            MintCommand::StartMinting { issuer_request_id } => {
                self.handle_start_minting(issuer_request_id)
            }
            MintCommand::RecordMintSuccess {
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            } => self.handle_record_mint_success(
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            ),
            MintCommand::RecordMintFailure { issuer_request_id, error } => {
                self.handle_record_mint_failure(issuer_request_id, error)
            }
            MintCommand::RecordCallback { issuer_request_id } => {
                self.handle_record_callback(issuer_request_id)
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
                gas_used,
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
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
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

    #[error("Mint not in Minting state. Current state: {current_state}")]
    NotInMintingState { current_state: String },

    #[error(
        "Mint not in CallbackPending state. Current state: {current_state}"
    )]
    NotInCallbackPendingState { current_state: String },

    #[error(
        "Issuer request ID mismatch. Expected: {expected}, provided: {provided}"
    )]
    IssuerRequestIdMismatch { expected: String, provided: String },
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
        CallbackManager, ClientId, Mint, MintCommand, MintError, MintEvent,
        MintView, Network, Quantity, TokenSymbol, TokenizationRequestId,
        UnderlyingSymbol, mint_manager::MintManager,
    };
    use crate::account::AlpacaAccountNumber;
    use crate::alpaca::{AlpacaService, mock::MockAlpacaService};
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, view::TokenizedAssetView,
    };
    use crate::vault::{VaultService, mock::MockVaultService};

    type MintTestFramework = TestFramework<Mint>;

    fn test_alpaca_account() -> AlpacaAccountNumber {
        AlpacaAccountNumber("test-account".to_string())
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

        let validator = MintTestFramework::with(())
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
                    | MintEvent::MintCompleted { .. } => {
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

        MintTestFramework::with(())
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
                tokenization_request_id: tokenization_request_id.clone(),
                quantity,
                underlying,
                token,
                network,
                client_id,
                wallet,
            })
            .then_expect_error(MintError::AlreadyInitiated {
                tokenization_request_id: tokenization_request_id.0,
            });
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

        let validator = MintTestFramework::with(())
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

        let validator = MintTestFramework::with(())
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

        let validator = MintTestFramework::with(())
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

        let validator = MintTestFramework::with(())
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

        let validator = MintTestFramework::with(())
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

        MintTestFramework::with(())
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
            .then_expect_error(MintError::IssuerRequestIdMismatch {
                expected: "iss-correct".to_string(),
                provided: "iss-wrong".to_string(),
            });
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

        MintTestFramework::with(())
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
            .then_expect_error(MintError::IssuerRequestIdMismatch {
                expected: "iss-correct".to_string(),
                provided: "iss-wrong".to_string(),
            });
    }

    #[test]
    fn test_start_minting_from_journal_confirmed_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = MintTestFramework::with(())
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
            .when(MintCommand::StartMinting { issuer_request_id });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);

        let MintEvent::MintingStarted { started_at, .. } = &events[0] else {
            panic!("Expected MintingStarted event, got {:?}", &events[0]);
        };

        assert!(started_at.timestamp() > 0);
    }

    #[test]
    fn test_start_minting_from_wrong_state_fails() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        MintTestFramework::with(())
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
            .when(MintCommand::StartMinting { issuer_request_id })
            .then_expect_error(MintError::NotInJournalConfirmedState {
                current_state: "Initiated".to_string(),
            });
    }

    #[test]
    fn test_start_minting_with_mismatched_issuer_request_id_fails() {
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

        MintTestFramework::with(())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: correct_issuer_request_id.clone(),
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
                    issuer_request_id: correct_issuer_request_id,
                    confirmed_at: Utc::now(),
                },
            ])
            .when(MintCommand::StartMinting {
                issuer_request_id: wrong_issuer_request_id,
            })
            .then_expect_error(MintError::IssuerRequestIdMismatch {
                expected: "iss-correct".to_string(),
                provided: "iss-wrong".to_string(),
            });
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
    fn test_record_mint_success_from_minting_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;

        let validator = MintTestFramework::with(())
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
                MintEvent::MintingStarted {
                    issuer_request_id: issuer_request_id.clone(),
                    started_at: Utc::now(),
                },
            ])
            .when(MintCommand::RecordMintSuccess {
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);

        let MintEvent::TokensMinted {
            tx_hash: event_tx_hash,
            receipt_id: event_receipt_id,
            shares_minted: event_shares_minted,
            gas_used: event_gas_used,
            block_number: event_block_number,
            minted_at,
            ..
        } = &events[0]
        else {
            panic!("Expected TokensMinted event, got {:?}", &events[0]);
        };

        assert_eq!(event_tx_hash, &tx_hash);
        assert_eq!(event_receipt_id, &receipt_id);
        assert_eq!(event_shares_minted, &shares_minted);
        assert_eq!(event_gas_used, &gas_used);
        assert_eq!(event_block_number, &block_number);
        assert!(minted_at.timestamp() > 0);
    }

    #[test]
    fn test_record_mint_success_from_wrong_state_fails() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        MintTestFramework::with(())
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
            .when(MintCommand::RecordMintSuccess {
                issuer_request_id,
                tx_hash,
                receipt_id: uint!(1_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 50000,
                block_number: 1000,
            })
            .then_expect_error(MintError::NotInMintingState {
                current_state: "Initiated".to_string(),
            });
    }

    #[test]
    fn test_record_mint_failure_from_minting_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let error_message = "Transaction failed: insufficient gas";

        let validator = MintTestFramework::with(())
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
                MintEvent::MintingStarted {
                    issuer_request_id: issuer_request_id.clone(),
                    started_at: Utc::now(),
                },
            ])
            .when(MintCommand::RecordMintFailure {
                issuer_request_id,
                error: error_message.to_string(),
            });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);

        let MintEvent::MintingFailed { error, failed_at, .. } = &events[0]
        else {
            panic!("Expected MintingFailed event, got {:?}", &events[0]);
        };

        assert_eq!(error, error_message);
        assert!(failed_at.timestamp() > 0);
    }

    #[test]
    fn test_record_mint_failure_from_wrong_state_fails() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        MintTestFramework::with(())
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
            .when(MintCommand::RecordMintFailure {
                issuer_request_id,
                error: "Transaction failed".to_string(),
            })
            .then_expect_error(MintError::NotInMintingState {
                current_state: "Initiated".to_string(),
            });
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
        assert_eq!(state_gas_used, gas_used);
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
    fn test_record_mint_success_with_mismatched_issuer_request_id_fails() {
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

        MintTestFramework::with(())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: correct_issuer_request_id.clone(),
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
                    issuer_request_id: correct_issuer_request_id.clone(),
                    confirmed_at: Utc::now(),
                },
                MintEvent::MintingStarted {
                    issuer_request_id: correct_issuer_request_id,
                    started_at: Utc::now(),
                },
            ])
            .when(MintCommand::RecordMintSuccess {
                issuer_request_id: wrong_issuer_request_id,
                tx_hash: b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
                receipt_id: uint!(1_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: 50000,
                block_number: 1000,
            })
            .then_expect_error(MintError::IssuerRequestIdMismatch {
                expected: "iss-correct".to_string(),
                provided: "iss-wrong".to_string(),
            });
    }

    #[test]
    fn test_record_mint_failure_with_mismatched_issuer_request_id_fails() {
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

        MintTestFramework::with(())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: correct_issuer_request_id.clone(),
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
                    issuer_request_id: correct_issuer_request_id.clone(),
                    confirmed_at: Utc::now(),
                },
                MintEvent::MintingStarted {
                    issuer_request_id: correct_issuer_request_id,
                    started_at: Utc::now(),
                },
            ])
            .when(MintCommand::RecordMintFailure {
                issuer_request_id: wrong_issuer_request_id,
                error: "Transaction failed".to_string(),
            })
            .then_expect_error(MintError::IssuerRequestIdMismatch {
                expected: "iss-correct".to_string(),
                provided: "iss-wrong".to_string(),
            });
    }

    #[test]
    fn test_record_callback_from_callback_pending_state() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        let validator = MintTestFramework::with(())
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
                MintEvent::MintingStarted {
                    issuer_request_id: issuer_request_id.clone(),
                    started_at: Utc::now(),
                },
                MintEvent::TokensMinted {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash,
                    receipt_id: uint!(1_U256),
                    shares_minted: uint!(100_000000000000000000_U256),
                    gas_used: 50000,
                    block_number: 1000,
                    minted_at: Utc::now(),
                },
            ])
            .when(MintCommand::RecordCallback { issuer_request_id });

        let events = validator.inspect_result().unwrap();

        assert_eq!(events.len(), 1);

        let MintEvent::MintCompleted { completed_at, .. } = &events[0] else {
            panic!("Expected MintCompleted event, got {:?}", &events[0]);
        };

        assert!(completed_at.timestamp() > 0);
    }

    #[test]
    fn test_record_callback_from_wrong_state_fails() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        MintTestFramework::with(())
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
            .when(MintCommand::RecordCallback { issuer_request_id })
            .then_expect_error(MintError::NotInCallbackPendingState {
                current_state: "JournalConfirmed".to_string(),
            });
    }

    #[test]
    fn test_record_callback_with_mismatched_issuer_request_id_fails() {
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
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        MintTestFramework::with(())
            .given(vec![
                MintEvent::Initiated {
                    issuer_request_id: correct_issuer_request_id.clone(),
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
                    issuer_request_id: correct_issuer_request_id.clone(),
                    confirmed_at: Utc::now(),
                },
                MintEvent::MintingStarted {
                    issuer_request_id: correct_issuer_request_id.clone(),
                    started_at: Utc::now(),
                },
                MintEvent::TokensMinted {
                    issuer_request_id: correct_issuer_request_id,
                    tx_hash,
                    receipt_id: uint!(1_U256),
                    shares_minted: uint!(100_000000000000000000_U256),
                    gas_used: 50000,
                    block_number: 1000,
                    minted_at: Utc::now(),
                },
            ])
            .when(MintCommand::RecordCallback {
                issuer_request_id: wrong_issuer_request_id,
            })
            .then_expect_error(MintError::IssuerRequestIdMismatch {
                expected: "iss-correct".to_string(),
                provided: "iss-wrong".to_string(),
            });
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
            gas_used,
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
        assert_eq!(state_gas_used, gas_used);
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

    fn create_test_mint_manager(
        cqrs: Arc<CqrsFramework<Mint, MemStore<Mint>>>,
        store: Arc<MemStore<Mint>>,
        pool: sqlx::Pool<sqlx::Sqlite>,
    ) -> MintManager<MemStore<Mint>> {
        let blockchain_service =
            Arc::new(MockVaultService::new_success()) as Arc<dyn VaultService>;
        let bot = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        MintManager::new(blockchain_service, cqrs, store, pool, bot)
    }

    fn create_test_callback_manager(
        cqrs: Arc<CqrsFramework<Mint, MemStore<Mint>>>,
        store: Arc<MemStore<Mint>>,
        pool: sqlx::Pool<sqlx::Sqlite>,
    ) -> CallbackManager<MemStore<Mint>> {
        let alpaca_service = Arc::new(MockAlpacaService::new_success())
            as Arc<dyn AlpacaService>;

        CallbackManager::new(alpaca_service, cqrs, store, pool)
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

    async fn setup_managers_test() -> (
        Arc<MemStore<Mint>>,
        Arc<CqrsFramework<Mint, MemStore<Mint>>>,
        MintManager<MemStore<Mint>>,
        CallbackManager<MemStore<Mint>>,
    ) {
        use sqlx::sqlite::SqlitePoolOptions;

        let store = Arc::new(MemStore::<Mint>::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

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

        let mint_manager =
            create_test_mint_manager(cqrs.clone(), store.clone(), pool.clone());
        let callback_manager =
            create_test_callback_manager(cqrs.clone(), store.clone(), pool);

        (store, cqrs, mint_manager, callback_manager)
    }

    #[tokio::test]
    async fn test_complete_mint_flow_with_managers() {
        let (store, cqrs, mint_manager, callback_manager) =
            setup_managers_test().await;
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

        let context =
            store.load_aggregate(&data.issuer_request_id.0).await.unwrap();
        mint_manager
            .handle_journal_confirmed(
                &data.issuer_request_id,
                context.aggregate(),
            )
            .await
            .unwrap();

        let context =
            store.load_aggregate(&data.issuer_request_id.0).await.unwrap();
        callback_manager
            .handle_tokens_minted(
                &test_alpaca_account(),
                &data.issuer_request_id,
                context.aggregate(),
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
