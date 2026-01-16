mod api;
mod callback_manager;
pub(crate) mod mint_manager;

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

pub use api::MintResponse;

pub(crate) use api::{confirm_journal, initiate_mint};
pub(crate) use callback_manager::CallbackManager;

pub(crate) use crate::account::ClientId;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
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

impl std::fmt::Display for IssuerRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl IssuerRequestId {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MintCommand {
    Initiate {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
    },
    ConfirmJournal {
        issuer_request_id: IssuerRequestId,
    },
    RejectJournal {
        issuer_request_id: IssuerRequestId,
        reason: String,
    },
    RecordMintSuccess {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
    },
    RecordMintFailure {
        issuer_request_id: IssuerRequestId,
        error: String,
    },
    RecordCallback {
        issuer_request_id: IssuerRequestId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintEvent {
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
        confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerRequestId,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    TokensMinted {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerRequestId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    MintCompleted {
        issuer_request_id: IssuerRequestId,
        completed_at: DateTime<Utc>,
    },
}

impl DomainEvent for MintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initiated { .. } => "MintEvent::Initiated".to_string(),
            Self::JournalConfirmed { .. } => {
                "MintEvent::JournalConfirmed".to_string()
            }
            Self::JournalRejected { .. } => {
                "MintEvent::JournalRejected".to_string()
            }
            Self::TokensMinted { .. } => "MintEvent::TokensMinted".to_string(),
            Self::MintingFailed { .. } => {
                "MintEvent::MintingFailed".to_string()
            }
            Self::MintCompleted { .. } => {
                "MintEvent::MintCompleted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MintRequest {
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) tokenization_request_id: TokenizationRequestId,
    pub(crate) quantity: Quantity,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) client_id: ClientId,
    pub(crate) wallet: Address,
    pub(crate) initiated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MintState {
    Initiated,
    JournalConfirmed {
        journal_confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    CallbackPending {
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        journal_confirmed_at: DateTime<Utc>,
        error: String,
        failed_at: DateTime<Utc>,
    },
    Completed {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Mint {
    pub(crate) request: MintRequest,
    pub(crate) state: MintState,
}

impl Mint {
    pub(crate) const fn state_name(&self) -> &'static str {
        match &self.state {
            MintState::Initiated => "Initiated",
            MintState::JournalConfirmed { .. } => "JournalConfirmed",
            MintState::JournalRejected { .. } => "JournalRejected",
            MintState::CallbackPending { .. } => "CallbackPending",
            MintState::MintingFailed { .. } => "MintingFailed",
            MintState::Completed { .. } => "Completed",
        }
    }

    pub(crate) const fn tokenization_request_id(
        &self,
    ) -> &TokenizationRequestId {
        &self.request.tokenization_request_id
    }

    pub(crate) fn from_event(
        event: &MintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let MintEvent::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = event
        else {
            return Err(LifecycleError::Mismatch {
                expected: "Initiated event".to_string(),
                actual: event.event_type(),
            });
        };

        Ok(Self {
            request: MintRequest {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id: *client_id,
                wallet: *wallet,
                initiated_at: *initiated_at,
            },
            state: MintState::Initiated,
        })
    }

    pub(crate) fn apply_transition(
        event: &MintEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        let mut next = current.clone();

        match (&current.state, event) {
            (
                MintState::Initiated,
                MintEvent::JournalConfirmed { confirmed_at, .. },
            ) => {
                next.state = MintState::JournalConfirmed {
                    journal_confirmed_at: *confirmed_at,
                };
                Ok(next)
            }

            (
                MintState::Initiated,
                MintEvent::JournalRejected { reason, rejected_at, .. },
            ) => {
                next.state = MintState::JournalRejected {
                    reason: reason.clone(),
                    rejected_at: *rejected_at,
                };
                Ok(next)
            }

            (
                MintState::JournalConfirmed { journal_confirmed_at },
                MintEvent::TokensMinted {
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used,
                    block_number,
                    minted_at,
                    ..
                },
            ) => {
                next.state = MintState::CallbackPending {
                    journal_confirmed_at: *journal_confirmed_at,
                    tx_hash: *tx_hash,
                    receipt_id: *receipt_id,
                    shares_minted: *shares_minted,
                    gas_used: *gas_used,
                    block_number: *block_number,
                    minted_at: *minted_at,
                };
                Ok(next)
            }

            (
                MintState::JournalConfirmed { journal_confirmed_at },
                MintEvent::MintingFailed { error, failed_at, .. },
            ) => {
                next.state = MintState::MintingFailed {
                    journal_confirmed_at: *journal_confirmed_at,
                    error: error.clone(),
                    failed_at: *failed_at,
                };
                Ok(next)
            }

            (
                MintState::CallbackPending {
                    journal_confirmed_at,
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used,
                    block_number,
                    minted_at,
                },
                MintEvent::MintCompleted { completed_at, .. },
            ) => {
                next.state = MintState::Completed {
                    journal_confirmed_at: *journal_confirmed_at,
                    tx_hash: *tx_hash,
                    receipt_id: *receipt_id,
                    shares_minted: *shares_minted,
                    gas_used: *gas_used,
                    block_number: *block_number,
                    minted_at: *minted_at,
                    completed_at: *completed_at,
                };
                Ok(next)
            }

            (current_state, event) => Err(LifecycleError::Mismatch {
                expected: format!("valid transition from {:?}", current_state),
                actual: event.event_type(),
            }),
        }
    }

    fn handle_confirm_journal(
        &self,
        provided_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let MintState::Initiated = &self.state else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(
            &self.request.issuer_request_id,
            &provided_id,
        )?;

        Ok(vec![MintEvent::JournalConfirmed {
            issuer_request_id: provided_id,
            confirmed_at: Utc::now(),
        }])
    }

    fn handle_reject_journal(
        &self,
        provided_id: IssuerRequestId,
        reason: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        let MintState::Initiated = &self.state else {
            return Err(MintError::NotInInitiatedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(
            &self.request.issuer_request_id,
            &provided_id,
        )?;

        Ok(vec![MintEvent::JournalRejected {
            issuer_request_id: provided_id,
            reason,
            rejected_at: Utc::now(),
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
        let MintState::JournalConfirmed { .. } = &self.state else {
            return Err(MintError::NotInJournalConfirmedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(
            &self.request.issuer_request_id,
            &provided_id,
        )?;

        Ok(vec![MintEvent::TokensMinted {
            issuer_request_id: provided_id,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at: Utc::now(),
        }])
    }

    fn handle_record_mint_failure(
        &self,
        provided_id: IssuerRequestId,
        error: String,
    ) -> Result<Vec<MintEvent>, MintError> {
        let MintState::JournalConfirmed { .. } = &self.state else {
            return Err(MintError::NotInJournalConfirmedState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(
            &self.request.issuer_request_id,
            &provided_id,
        )?;

        Ok(vec![MintEvent::MintingFailed {
            issuer_request_id: provided_id,
            error,
            failed_at: Utc::now(),
        }])
    }

    fn handle_record_callback(
        &self,
        provided_id: IssuerRequestId,
    ) -> Result<Vec<MintEvent>, MintError> {
        let MintState::CallbackPending { .. } = &self.state else {
            return Err(MintError::NotInCallbackPendingState {
                current_state: self.state_name().to_string(),
            });
        };

        Self::validate_issuer_request_id(
            &self.request.issuer_request_id,
            &provided_id,
        )?;

        Ok(vec![MintEvent::MintCompleted {
            issuer_request_id: provided_id,
            completed_at: Utc::now(),
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
}

#[async_trait]
impl cqrs_es::Aggregate for Lifecycle<Mint, Never> {
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
                if self.live().is_ok() {
                    return Err(MintError::AlreadyInitiated {
                        tokenization_request_id: tokenization_request_id.0,
                    });
                }

                Ok(vec![MintEvent::Initiated {
                    issuer_request_id,
                    tokenization_request_id,
                    quantity,
                    underlying,
                    token,
                    network,
                    client_id,
                    wallet,
                    initiated_at: Utc::now(),
                }])
            }

            MintCommand::ConfirmJournal { issuer_request_id } => {
                let inner =
                    self.live().map_err(|_| MintError::NotInitialized)?;
                inner.handle_confirm_journal(issuer_request_id)
            }

            MintCommand::RejectJournal { issuer_request_id, reason } => {
                let inner =
                    self.live().map_err(|_| MintError::NotInitialized)?;
                inner.handle_reject_journal(issuer_request_id, reason)
            }

            MintCommand::RecordMintSuccess {
                issuer_request_id,
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            } => {
                let inner =
                    self.live().map_err(|_| MintError::NotInitialized)?;
                inner.handle_record_mint_success(
                    issuer_request_id,
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used,
                    block_number,
                )
            }

            MintCommand::RecordMintFailure { issuer_request_id, error } => {
                let inner =
                    self.live().map_err(|_| MintError::NotInitialized)?;
                inner.handle_record_mint_failure(issuer_request_id, error)
            }

            MintCommand::RecordCallback { issuer_request_id } => {
                let inner =
                    self.live().map_err(|_| MintError::NotInitialized)?;
                inner.handle_record_callback(issuer_request_id)
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, Mint::apply_transition)
            .or_initialize(&event, Mint::from_event);
    }
}

impl cqrs_es::View<Self> for Lifecycle<Mint, Never> {
    fn update(&mut self, event: &cqrs_es::EventEnvelope<Self>) {
        self.apply(event.payload.clone());
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum MintError {
    #[error(
        "Mint already initiated for tokenization request: {tokenization_request_id}"
    )]
    AlreadyInitiated { tokenization_request_id: String },

    #[error("Mint not initialized")]
    NotInitialized,

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
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

pub(crate) async fn find_by_issuer_request_id(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerRequestId,
) -> Result<Option<Lifecycle<Mint, Never>>, MintViewError> {
    let issuer_request_id_str = &issuer_request_id.0;
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM mint_view
        WHERE view_id = ?
        "#,
        issuer_request_id_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: Lifecycle<Mint, Never> = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use chrono::Utc;
    use cqrs_es::{
        Aggregate, AggregateContext, CqrsFramework, EventEnvelope, EventStore,
        View, mem_store::MemStore, test::TestFramework,
    };
    use rust_decimal::Decimal;
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::{
        CallbackManager, ClientId, IssuerRequestId, Lifecycle, Mint,
        MintCommand, MintError, MintEvent, MintRequest, MintState,
        MintViewError, Network, Never, Quantity, TokenSymbol,
        TokenizationRequestId, UnderlyingSymbol, find_by_issuer_request_id,
        mint_manager::MintManager,
    };
    use crate::alpaca::mock::MockAlpacaService;
    use crate::vault::mock::MockVaultService;

    type MintTestFramework = TestFramework<Lifecycle<Mint, Never>>;
    type TestCqrs =
        CqrsFramework<Lifecycle<Mint, Never>, MemStore<Lifecycle<Mint, Never>>>;
    type TestStore = MemStore<Lifecycle<Mint, Never>>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    async fn setup_test_db() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    fn test_mint_request() -> MintRequest {
        MintRequest {
            issuer_request_id: IssuerRequestId::new("iss-123"),
            tokenization_request_id: TokenizationRequestId::new("alp-456"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: Utc::now(),
        }
    }

    #[test]
    fn test_initiate_mint_creates_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
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
                    other => {
                        panic!("Expected MintInitiated event, got {:?}", other)
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_initiate_mint_when_already_initiated_returns_error() {
        let issuer_request_id = IssuerRequestId::new("iss-789");
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
    fn test_confirm_journal_creates_event() {
        let issuer_request_id = IssuerRequestId::new("iss-456");
        let tokenization_request_id = TokenizationRequestId::new("alp-789");
        let quantity = Quantity::new(Decimal::from(50));
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

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
                initiated_at: chrono::Utc::now(),
            }])
            .when(MintCommand::ConfirmJournal {
                issuer_request_id: issuer_request_id.clone(),
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    MintEvent::JournalConfirmed {
                        issuer_request_id: event_id,
                        confirmed_at,
                    } => {
                        assert_eq!(event_id, &issuer_request_id);
                        assert!(confirmed_at.timestamp() > 0);
                    }
                    other => panic!(
                        "Expected JournalConfirmed event, got {:?}",
                        other
                    ),
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_confirm_journal_when_not_initiated_returns_error() {
        let issuer_request_id = IssuerRequestId::new("iss-456");

        MintTestFramework::with(())
            .given_no_previous_events()
            .when(MintCommand::ConfirmJournal { issuer_request_id })
            .then_expect_error(MintError::NotInitialized);
    }

    #[test]
    fn test_record_mint_success_creates_event() {
        let issuer_request_id = IssuerRequestId::new("iss-789");
        let tokenization_request_id = TokenizationRequestId::new("alp-012");
        let quantity = Quantity::new(Decimal::from(200));
        let underlying = UnderlyingSymbol::new("GOOG");
        let token = TokenSymbol::new("tGOOG");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(200_000000000000000000_U256);
        let gas_used = 50000u64;
        let block_number = 1000u64;

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
                    initiated_at: chrono::Utc::now(),
                },
                MintEvent::JournalConfirmed {
                    issuer_request_id: issuer_request_id.clone(),
                    confirmed_at: chrono::Utc::now(),
                },
            ])
            .when(MintCommand::RecordMintSuccess {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    MintEvent::TokensMinted {
                        issuer_request_id: event_id,
                        tx_hash: event_tx_hash,
                        receipt_id: event_receipt_id,
                        shares_minted: event_shares,
                        gas_used: event_gas,
                        block_number: event_block,
                        minted_at,
                    } => {
                        assert_eq!(event_id, &issuer_request_id);
                        assert_eq!(event_tx_hash, &tx_hash);
                        assert_eq!(event_receipt_id, &receipt_id);
                        assert_eq!(event_shares, &shares_minted);
                        assert_eq!(event_gas, &gas_used);
                        assert_eq!(event_block, &block_number);
                        assert!(minted_at.timestamp() > 0);
                    }
                    other => {
                        panic!("Expected TokensMinted event, got {:?}", other)
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_record_callback_creates_event() {
        let issuer_request_id = IssuerRequestId::new("iss-callback");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-callback");
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
        let gas_used = 50000u64;
        let block_number = 1000u64;

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
                    initiated_at: chrono::Utc::now(),
                },
                MintEvent::JournalConfirmed {
                    issuer_request_id: issuer_request_id.clone(),
                    confirmed_at: chrono::Utc::now(),
                },
                MintEvent::TokensMinted {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    gas_used,
                    block_number,
                    minted_at: chrono::Utc::now(),
                },
            ])
            .when(MintCommand::RecordCallback {
                issuer_request_id: issuer_request_id.clone(),
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    MintEvent::MintCompleted {
                        issuer_request_id: event_id,
                        completed_at,
                    } => {
                        assert_eq!(event_id, &issuer_request_id);
                        assert!(completed_at.timestamp() > 0);
                    }
                    other => {
                        panic!("Expected MintCompleted event, got {:?}", other)
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_view_update_from_mint_initiated_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let event = MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = Lifecycle::<Mint, Never>::default();
        view.update(&envelope);

        let inner = view.live().expect("Expected Live state");
        assert_eq!(inner.request.issuer_request_id, issuer_request_id);
        assert_eq!(
            inner.request.tokenization_request_id,
            tokenization_request_id
        );
        assert_eq!(inner.request.quantity, quantity);
        assert!(matches!(inner.state, MintState::Initiated));
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_view() {
        let pool = setup_test_db().await;

        let issuer_request_id = IssuerRequestId::new("iss-999");
        let tokenization_request_id = TokenizationRequestId::new("alp-888");
        let quantity = Quantity::new(Decimal::from(50));
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = Utc::now();

        let mint = Mint {
            request: MintRequest {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id,
                wallet,
                initiated_at,
            },
            state: MintState::Initiated,
        };

        let view = Lifecycle::Live(mint);
        let payload =
            serde_json::to_string(&view).expect("Failed to serialize view");

        sqlx::query!(
            r"
            INSERT INTO mint_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            issuer_request_id.0,
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert view");

        let result = find_by_issuer_request_id(&pool, &issuer_request_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_some());

        let inner = result.unwrap().live().expect("Expected Live state");
        assert_eq!(inner.request.issuer_request_id, issuer_request_id);
        assert_eq!(
            inner.request.tokenization_request_id,
            tokenization_request_id
        );
        assert_eq!(inner.request.quantity, quantity);
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_none_when_not_found() {
        let pool = setup_test_db().await;

        let issuer_request_id = IssuerRequestId::new("nonexistent-id");

        let result = find_by_issuer_request_id(&pool, &issuer_request_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_none());
    }

    #[test]
    fn test_view_update_from_journal_confirmed_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut view = Lifecycle::<Mint, Never>::default();

        let init_event = MintEvent::Initiated {
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

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: init_event,
            metadata: HashMap::new(),
        });

        let confirmed_at = Utc::now();
        let confirm_event = MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        };

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: confirm_event,
            metadata: HashMap::new(),
        });

        let inner = view.live().expect("Expected Live state");
        let MintState::JournalConfirmed { journal_confirmed_at } = &inner.state
        else {
            panic!("Expected JournalConfirmed state")
        };
        assert_eq!(*journal_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_view_update_from_tokens_minted_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let mut view = Lifecycle::<Mint, Never>::default();

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: MintEvent::Initiated {
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
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: Utc::now(),
            },
            metadata: HashMap::new(),
        });

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 3,
            payload: MintEvent::TokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
                minted_at,
            },
            metadata: HashMap::new(),
        });

        let inner = view.live().expect("Expected Live state");
        let MintState::CallbackPending {
            tx_hash: state_tx_hash,
            receipt_id: state_receipt_id,
            shares_minted: state_shares,
            ..
        } = &inner.state
        else {
            panic!("Expected CallbackPending state, got {:?}", inner.state)
        };
        assert_eq!(*state_tx_hash, tx_hash);
        assert_eq!(*state_receipt_id, receipt_id);
        assert_eq!(*state_shares, shares_minted);
    }

    #[test]
    fn test_view_update_from_mint_completed_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
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

        let mut view = Lifecycle::<Mint, Never>::default();

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: MintEvent::Initiated {
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
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: Utc::now(),
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 3,
            payload: MintEvent::TokensMinted {
                issuer_request_id: issuer_request_id.clone(),
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
                minted_at: Utc::now(),
            },
            metadata: HashMap::new(),
        });

        let completed_at = Utc::now();
        view.update(&EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 4,
            payload: MintEvent::MintCompleted {
                issuer_request_id: issuer_request_id.clone(),
                completed_at,
            },
            metadata: HashMap::new(),
        });

        let inner = view.live().expect("Expected Live state");
        let MintState::Completed { completed_at: state_completed_at, .. } =
            &inner.state
        else {
            panic!("Expected Completed state, got {:?}", inner.state)
        };
        assert_eq!(*state_completed_at, completed_at);
    }
}
