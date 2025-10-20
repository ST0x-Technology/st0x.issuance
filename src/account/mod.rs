mod api;
mod cmd;
mod event;
mod view;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) use api::connect_account;
pub(crate) use cmd::AccountCommand;
pub(crate) use event::AccountEvent;
pub(crate) use view::AccountView;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Email(String);

impl Email {
    pub(crate) fn new(email: String) -> Result<Self, AccountError> {
        let parts: Vec<&str> = email.split('@').collect();

        if parts.len() != 2 {
            return Err(AccountError::InvalidEmail { email });
        }

        let local = parts[0];
        let domain = parts[1];

        if local.is_empty() || domain.is_empty() {
            return Err(AccountError::InvalidEmail { email });
        }

        Ok(Self(email))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Email {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlpacaAccountNumber(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClientId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LinkedAccountStatus {
    Active,
    Inactive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Account {
    NotLinked,
    Linked {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
        status: LinkedAccountStatus,
        linked_at: DateTime<Utc>,
    },
}

impl Default for Account {
    fn default() -> Self {
        Self::NotLinked
    }
}

#[async_trait]
impl Aggregate for Account {
    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = AccountError;
    type Services = ();

    fn aggregate_type() -> String {
        "Account".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::LinkAccount { email, alpaca_account } => {
                if matches!(self, Self::Linked { .. }) {
                    return Err(AccountError::AccountAlreadyExists {
                        email: email.as_str().to_string(),
                    });
                }

                let client_id = ClientId(Uuid::new_v4().to_string());
                let now = Utc::now();

                Ok(vec![AccountEvent::AccountLinked {
                    client_id,
                    email,
                    alpaca_account,
                    linked_at: now,
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            AccountEvent::AccountLinked {
                client_id,
                email,
                alpaca_account,
                linked_at,
            } => {
                *self = Self::Linked {
                    client_id,
                    email,
                    alpaca_account,
                    status: LinkedAccountStatus::Active,
                    linked_at,
                };
            }
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum AccountError {
    #[error("Invalid email format: {email}")]
    InvalidEmail { email: String },

    #[error("Account already exists for email: {email}")]
    AccountAlreadyExists { email: String },
}

#[cfg(test)]
mod tests {
    use super::{
        Account, AccountCommand, AccountError, AccountEvent,
        AlpacaAccountNumber, Email, LinkedAccountStatus,
    };
    use cqrs_es::{Aggregate, test::TestFramework};

    type AccountTestFramework = TestFramework<Account>;

    #[test]
    fn test_link_account_creates_new_account() {
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        let validator = AccountTestFramework::with(())
            .given_no_previous_events()
            .when(AccountCommand::LinkAccount {
                email: email.clone(),
                alpaca_account: alpaca_account.clone(),
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    AccountEvent::AccountLinked {
                        client_id,
                        email: event_email,
                        alpaca_account: event_alpaca,
                        linked_at,
                    } => {
                        assert!(!client_id.0.is_empty());
                        assert_eq!(event_email, &email);
                        assert_eq!(event_alpaca, &alpaca_account);
                        assert!(linked_at.timestamp() > 0);
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_email_smart_constructor_validates() {
        assert!(matches!(
            Email::new("not-an-email".to_string()),
            Err(AccountError::InvalidEmail { email }) if email == "not-an-email"
        ));

        assert!(matches!(
            Email::new("@".to_string()),
            Err(AccountError::InvalidEmail { email }) if email == "@"
        ));

        assert!(matches!(
            Email::new("user@".to_string()),
            Err(AccountError::InvalidEmail { email }) if email == "user@"
        ));

        assert!(matches!(
            Email::new("@domain".to_string()),
            Err(AccountError::InvalidEmail { email }) if email == "@domain"
        ));

        assert!(matches!(
            Email::new("user@@domain.com".to_string()),
            Err(AccountError::InvalidEmail { email }) if email == "user@@domain.com"
        ));

        assert!(matches!(
            Email::new("user@domain@com".to_string()),
            Err(AccountError::InvalidEmail { email }) if email == "user@domain@com"
        ));

        assert!(Email::new("user@example.com".to_string()).is_ok());
    }

    #[test]
    fn test_link_account_when_already_linked_returns_error() {
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        AccountTestFramework::with(())
            .given(vec![AccountEvent::AccountLinked {
                client_id: super::ClientId("existing-client-id".to_string()),
                email: email.clone(),
                alpaca_account: AlpacaAccountNumber("ALPACA456".to_string()),
                linked_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::LinkAccount { email, alpaca_account })
            .then_expect_error(AccountError::AccountAlreadyExists {
                email: "user@example.com".to_string(),
            });
    }

    #[test]
    fn test_apply_account_linked_updates_state() {
        let mut account = Account::default();

        assert!(matches!(account, Account::NotLinked));

        let client_id = super::ClientId("test-client-123".to_string());
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = chrono::Utc::now();

        account.apply(AccountEvent::AccountLinked {
            client_id: client_id.clone(),
            email: email.clone(),
            alpaca_account: alpaca_account.clone(),
            linked_at,
        });

        match account {
            Account::Linked {
                client_id: linked_client_id,
                email: linked_email,
                alpaca_account: linked_alpaca,
                status,
                linked_at: linked_at_timestamp,
            } => {
                assert_eq!(linked_client_id, client_id);
                assert_eq!(linked_email, email);
                assert_eq!(linked_alpaca, alpaca_account);
                assert_eq!(status, LinkedAccountStatus::Active);
                assert_eq!(linked_at_timestamp, linked_at);
            }
            Account::NotLinked => panic!("Expected account to be linked"),
        }
    }
}
