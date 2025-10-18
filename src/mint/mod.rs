mod api;
mod cmd;
mod event;
mod view;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub(crate) use api::{confirm_journal, initiate_mint};
pub(crate) use cmd::MintCommand;
pub(crate) use event::MintEvent;
pub(crate) use view::MintView;

pub(crate) use crate::account::ClientId;
pub(crate) use crate::tokenized_asset::{
    Network, TokenSymbol, UnderlyingSymbol,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl TokenizationRequestId {
    #[cfg(test)]
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IssuerRequestId(pub(crate) String);

impl IssuerRequestId {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Quantity(pub(crate) Decimal);

impl Quantity {
    pub(crate) const fn new(value: Decimal) -> Self {
        Self(value)
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
}

impl Default for Mint {
    fn default() -> Self {
        Self::Uninitialized
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
                if !matches!(self, Self::Initiated { .. }) {
                    return Err(MintError::NotInInitiatedState {
                        current_state: format!("{self:?}"),
                    });
                }

                let now = Utc::now();

                Ok(vec![MintEvent::JournalConfirmed {
                    issuer_request_id,
                    confirmed_at: now,
                }])
            }
            MintCommand::RejectJournal { issuer_request_id, reason } => {
                if !matches!(self, Self::Initiated { .. }) {
                    return Err(MintError::NotInInitiatedState {
                        current_state: format!("{self:?}"),
                    });
                }

                let now = Utc::now();

                Ok(vec![MintEvent::JournalRejected {
                    issuer_request_id,
                    reason,
                    rejected_at: now,
                }])
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
            } => {
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
            MintEvent::JournalRejected {
                issuer_request_id: _,
                reason,
                rejected_at,
            } => {
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
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use chrono::Utc;
    use cqrs_es::{Aggregate, test::TestFramework};
    use rust_decimal::Decimal;

    use super::{
        ClientId, Mint, MintCommand, MintError, MintEvent, Network, Quantity,
        TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };

    type MintTestFramework = TestFramework<Mint>;

    #[test]
    fn test_initiate_mint_creates_event() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-123");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-456".to_string());
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
                client_id: client_id.clone(),
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
                    | MintEvent::JournalRejected { .. } => {
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
        let client_id = ClientId("client-456".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        MintTestFramework::with(())
            .given(vec![MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                quantity: quantity.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                client_id: client_id.clone(),
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
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = chrono::Utc::now();

        mint.apply(MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id: client_id.clone(),
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
    fn test_confirm_journal_produces_event() {
        let issuer_request_id = super::IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
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
        let client_id = ClientId("client-789".to_string());
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
        let client_id = ClientId("client-789".to_string());
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
}
