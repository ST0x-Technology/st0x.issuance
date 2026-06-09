mod api;
mod cmd;
mod event;
pub(crate) mod view;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EventSourced, Table};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, encode::IsNull, sqlite::SqliteArgumentValue};
use std::str::FromStr;
use uuid::Uuid;

pub use api::{
    AccountLinkResponse, RegisterAccountResponse, WhitelistWalletResponse,
};
pub(crate) use api::{
    connect_account, register_account, unwhitelist_wallet, whitelist_wallet,
};
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
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlpacaAccountNumber(pub(crate) String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientId(Uuid);

impl ClientId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ClientId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(ClientId)
    }
}

impl sqlx::Type<Sqlite> for ClientId {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <String as sqlx::Type<Sqlite>>::type_info()
    }
}

impl<'q> sqlx::Encode<'q, Sqlite> for ClientId {
    fn encode_by_ref(
        &self,
        args: &mut Vec<SqliteArgumentValue<'q>>,
    ) -> Result<IsNull, sqlx::error::BoxDynError> {
        args.push(SqliteArgumentValue::Text(self.0.to_string().into()));
        Ok(IsNull::No)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Account {
    Registered {
        client_id: ClientId,
        email: Email,
        registered_at: DateTime<Utc>,
    },
    LinkedToAlpaca {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
        whitelisted_wallets: Vec<Address>,
        registered_at: DateTime<Utc>,
        linked_at: DateTime<Utc>,
    },
}

#[async_trait]
impl EventSourced for Account {
    type Id = ClientId;
    type Event = AccountEvent;
    type Command = AccountCommand;
    type Error = AccountError;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "Account";
    const PROJECTION: Table = Table("account_view");
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            AccountEvent::Registered { client_id, email, registered_at } => {
                Some(Self::Registered {
                    client_id: *client_id,
                    email: email.clone(),
                    registered_at: *registered_at,
                })
            }
            AccountEvent::LinkedToAlpaca { .. }
            | AccountEvent::WalletWhitelisted { .. }
            | AccountEvent::WalletUnwhitelisted { .. } => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        match (entity, event) {
            (
                Self::Registered { client_id, email, registered_at },
                AccountEvent::LinkedToAlpaca { alpaca_account, linked_at },
            ) => Ok(Some(Self::LinkedToAlpaca {
                client_id: *client_id,
                email: email.clone(),
                alpaca_account: alpaca_account.clone(),
                whitelisted_wallets: Vec::new(),
                registered_at: *registered_at,
                linked_at: *linked_at,
            })),
            (
                Self::LinkedToAlpaca { .. },
                AccountEvent::WalletWhitelisted { wallet, .. },
            ) => {
                let mut next = entity.clone();
                if let Self::LinkedToAlpaca { whitelisted_wallets, .. } =
                    &mut next
                {
                    whitelisted_wallets.push(*wallet);
                }
                Ok(Some(next))
            }
            (
                Self::LinkedToAlpaca { .. },
                AccountEvent::WalletUnwhitelisted { wallet, .. },
            ) => {
                let mut next = entity.clone();
                if let Self::LinkedToAlpaca { whitelisted_wallets, .. } =
                    &mut next
                {
                    whitelisted_wallets.retain(|existing| existing != wallet);
                }
                Ok(Some(next))
            }
            _ => Ok(None),
        }
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::Register { client_id, email } => {
                Ok(vec![AccountEvent::Registered {
                    client_id,
                    email,
                    registered_at: Utc::now(),
                }])
            }
            AccountCommand::LinkToAlpaca { .. }
            | AccountCommand::WhitelistWallet { .. }
            | AccountCommand::UnwhitelistWallet { .. } => {
                Err(AccountError::NotRegistered)
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AccountCommand::Register { email, .. } => {
                Err(AccountError::AccountAlreadyRegistered {
                    email: email.as_str().to_string(),
                })
            }
            AccountCommand::LinkToAlpaca { alpaca_account } => match self {
                Self::Registered { .. } => {
                    Ok(vec![AccountEvent::LinkedToAlpaca {
                        alpaca_account,
                        linked_at: Utc::now(),
                    }])
                }
                Self::LinkedToAlpaca { .. } => {
                    Err(AccountError::AlreadyLinkedToAlpaca)
                }
            },
            AccountCommand::WhitelistWallet { wallet } => match self {
                Self::Registered { .. } => Err(AccountError::NotLinkedToAlpaca),
                Self::LinkedToAlpaca { whitelisted_wallets, .. } => {
                    if whitelisted_wallets.contains(&wallet) {
                        Ok(vec![])
                    } else {
                        Ok(vec![AccountEvent::WalletWhitelisted {
                            wallet,
                            whitelisted_at: Utc::now(),
                        }])
                    }
                }
            },
            AccountCommand::UnwhitelistWallet { wallet } => match self {
                Self::Registered { .. } => Err(AccountError::NotLinkedToAlpaca),
                Self::LinkedToAlpaca { whitelisted_wallets, .. } => {
                    if whitelisted_wallets.contains(&wallet) {
                        Ok(vec![AccountEvent::WalletUnwhitelisted {
                            wallet,
                            unwhitelisted_at: Utc::now(),
                        }])
                    } else {
                        Ok(vec![])
                    }
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum AccountError {
    #[error("Invalid email format: {email}")]
    InvalidEmail { email: String },
    #[error("Account already registered for email: {email}")]
    AccountAlreadyRegistered { email: String },
    #[error("Account is not registered")]
    NotRegistered,
    #[error("Account is already linked to Alpaca")]
    AlreadyLinkedToAlpaca,
    #[error("Account is not linked to Alpaca")]
    NotLinkedToAlpaca,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use event_sorcery::{LifecycleError, TestHarness, replay};
    use uuid::Uuid;

    use super::{
        Account, AccountCommand, AccountError, AccountEvent,
        AlpacaAccountNumber, ClientId, Email,
    };

    #[tokio::test]
    async fn test_register_creates_new_account() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());

        let events = TestHarness::<Account>::with(())
            .given_no_previous_events()
            .when(AccountCommand::Register { client_id, email: email.clone() })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let AccountEvent::Registered {
            client_id: event_client_id,
            email: event_email,
            registered_at,
        } = &events[0]
        else {
            panic!("Expected Registered event, got {:?}", &events[0])
        };

        assert_eq!(event_client_id, &client_id);
        assert_eq!(event_email, &email);
        assert!(registered_at.timestamp() > 0);
    }

    #[tokio::test]
    async fn test_register_when_already_registered_returns_error() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());

        let err = TestHarness::<Account>::with(())
            .given(vec![AccountEvent::Registered {
                client_id: ClientId::new(),
                email: email.clone(),
                registered_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::Register { client_id, email })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(AccountError::AccountAlreadyRegistered {
                email,
            }) if email == "user@example.com"
        ));
    }

    #[tokio::test]
    async fn test_link_to_alpaca_on_registered_account() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        let events = TestHarness::<Account>::with(())
            .given(vec![AccountEvent::Registered {
                client_id,
                email,
                registered_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::LinkToAlpaca {
                alpaca_account: alpaca_account.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let AccountEvent::LinkedToAlpaca {
            alpaca_account: event_alpaca,
            linked_at,
        } = &events[0]
        else {
            panic!("Expected LinkedToAlpaca event, got {:?}", &events[0])
        };

        assert_eq!(event_alpaca, &alpaca_account);
        assert!(linked_at.timestamp() > 0);
    }

    #[tokio::test]
    async fn test_link_to_alpaca_on_not_registered_returns_error() {
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        let err = TestHarness::<Account>::with(())
            .given_no_previous_events()
            .when(AccountCommand::LinkToAlpaca { alpaca_account })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(AccountError::NotRegistered)
        ));
    }

    #[tokio::test]
    async fn test_link_to_alpaca_when_already_linked_returns_error() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());

        let err = TestHarness::<Account>::with(())
            .given(vec![
                AccountEvent::Registered {
                    client_id,
                    email,
                    registered_at: chrono::Utc::now(),
                },
                AccountEvent::LinkedToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                    linked_at: chrono::Utc::now(),
                },
            ])
            .when(AccountCommand::LinkToAlpaca {
                alpaca_account: AlpacaAccountNumber("ALPACA456".to_string()),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(AccountError::AlreadyLinkedToAlpaca)
        ));
    }

    #[test]
    fn test_apply_registered_updates_state() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = chrono::Utc::now();

        let account = replay::<Account>(vec![AccountEvent::Registered {
            client_id,
            email: email.clone(),
            registered_at,
        }])
        .unwrap()
        .unwrap();

        let Account::Registered {
            client_id: reg_client_id,
            email: reg_email,
            registered_at: reg_at,
        } = account
        else {
            panic!("Expected account to be Registered, got {account:?}")
        };

        assert_eq!(reg_client_id, client_id);
        assert_eq!(reg_email, email);
        assert_eq!(reg_at, registered_at);
    }

    #[test]
    fn test_apply_linked_to_alpaca_updates_state() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = chrono::Utc::now();
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = chrono::Utc::now();

        let account = replay::<Account>(vec![
            AccountEvent::Registered {
                client_id,
                email: email.clone(),
                registered_at,
            },
            AccountEvent::LinkedToAlpaca {
                alpaca_account: alpaca_account.clone(),
                linked_at,
            },
        ])
        .unwrap()
        .unwrap();

        let Account::LinkedToAlpaca {
            client_id: linked_client_id,
            email: linked_email,
            alpaca_account: linked_alpaca,
            whitelisted_wallets,
            registered_at: linked_reg_at,
            linked_at: linked_at_timestamp,
        } = account
        else {
            panic!("Expected account to be LinkedToAlpaca, got {account:?}")
        };

        assert_eq!(linked_client_id, client_id);
        assert_eq!(linked_email, email);
        assert_eq!(linked_alpaca, alpaca_account);
        assert!(whitelisted_wallets.is_empty());
        assert_eq!(linked_reg_at, registered_at);
        assert_eq!(linked_at_timestamp, linked_at);
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
    fn test_client_id_display() {
        let uuid = Uuid::new_v4();
        let id = ClientId(uuid);
        assert_eq!(format!("{id}"), uuid.to_string());
    }

    #[tokio::test]
    async fn test_whitelist_wallet_on_linked_to_alpaca_account() {
        let email = Email("user@example.com".to_string());
        let client_id = ClientId::new();
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();

        let wallet = address!("0x1111111111111111111111111111111111111111");

        let events = TestHarness::<Account>::with(())
            .given(vec![
                AccountEvent::Registered { client_id, email, registered_at },
                AccountEvent::LinkedToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                    linked_at,
                },
            ])
            .when(AccountCommand::WhitelistWallet { wallet })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let AccountEvent::WalletWhitelisted {
            wallet: event_wallet,
            whitelisted_at,
        } = &events[0]
        else {
            panic!("Expected WalletWhitelisted event, got {:?}", &events[0])
        };

        assert_eq!(event_wallet, &wallet);
        assert!(whitelisted_at.timestamp() > 0);
    }

    #[tokio::test]
    async fn test_whitelist_wallet_on_not_registered_account() {
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let err = TestHarness::<Account>::with(())
            .given_no_previous_events()
            .when(AccountCommand::WhitelistWallet { wallet })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(AccountError::NotRegistered)
        ));
    }

    #[tokio::test]
    async fn test_whitelist_wallet_on_registered_but_not_linked_account() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let err = TestHarness::<Account>::with(())
            .given(vec![AccountEvent::Registered {
                client_id,
                email,
                registered_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::WhitelistWallet { wallet })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(AccountError::NotLinkedToAlpaca)
        ));
    }

    #[tokio::test]
    async fn test_whitelist_already_whitelisted_wallet_is_idempotent() {
        let email = Email("user@example.com".to_string());
        let client_id = ClientId::new();
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();
        let wallet = address!("0x1111111111111111111111111111111111111111");
        let whitelisted_at = chrono::Utc::now();

        TestHarness::<Account>::with(())
            .given(vec![
                AccountEvent::Registered { client_id, email, registered_at },
                AccountEvent::LinkedToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                    linked_at,
                },
                AccountEvent::WalletWhitelisted { wallet, whitelisted_at },
            ])
            .when(AccountCommand::WhitelistWallet { wallet })
            .await
            .then_expect_events(&[]);
    }

    #[test]
    fn test_apply_wallet_whitelisted_adds_wallet() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let account = replay::<Account>(vec![
            AccountEvent::Registered { client_id, email, registered_at },
            AccountEvent::LinkedToAlpaca {
                alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
                linked_at,
            },
            AccountEvent::WalletWhitelisted {
                wallet,
                whitelisted_at: chrono::Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        let Account::LinkedToAlpaca { whitelisted_wallets, .. } = account
        else {
            panic!("Expected account to be LinkedToAlpaca")
        };

        assert_eq!(whitelisted_wallets.len(), 1);
        assert_eq!(whitelisted_wallets[0], wallet);
    }

    #[test]
    fn test_apply_wallet_whitelisted_adds_multiple_wallets() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();
        let wallet1 = address!("0x1111111111111111111111111111111111111111");
        let wallet2 = address!("0x2222222222222222222222222222222222222222");

        let account = replay::<Account>(vec![
            AccountEvent::Registered { client_id, email, registered_at },
            AccountEvent::LinkedToAlpaca {
                alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
                linked_at,
            },
            AccountEvent::WalletWhitelisted {
                wallet: wallet1,
                whitelisted_at: chrono::Utc::now(),
            },
            AccountEvent::WalletWhitelisted {
                wallet: wallet2,
                whitelisted_at: chrono::Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        let Account::LinkedToAlpaca { whitelisted_wallets, .. } = account
        else {
            panic!("Expected account to be LinkedToAlpaca")
        };

        assert_eq!(whitelisted_wallets.len(), 2);
        assert_eq!(whitelisted_wallets[0], wallet1);
        assert_eq!(whitelisted_wallets[1], wallet2);
    }

    #[tokio::test]
    async fn test_unwhitelist_wallet_on_linked_account() {
        let email = Email("user@example.com".to_string());
        let client_id = ClientId::new();
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();
        let wallet = address!("0x1111111111111111111111111111111111111111");
        let whitelisted_at = chrono::Utc::now();

        let events = TestHarness::<Account>::with(())
            .given(vec![
                AccountEvent::Registered { client_id, email, registered_at },
                AccountEvent::LinkedToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                    linked_at,
                },
                AccountEvent::WalletWhitelisted { wallet, whitelisted_at },
            ])
            .when(AccountCommand::UnwhitelistWallet { wallet })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let AccountEvent::WalletUnwhitelisted {
            wallet: event_wallet,
            unwhitelisted_at,
        } = &events[0]
        else {
            panic!("Expected WalletUnwhitelisted event, got {:?}", &events[0])
        };

        assert_eq!(event_wallet, &wallet);
        assert!(unwhitelisted_at.timestamp() > 0);
    }

    #[tokio::test]
    async fn test_unwhitelist_not_registered() {
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let err = TestHarness::<Account>::with(())
            .given_no_previous_events()
            .when(AccountCommand::UnwhitelistWallet { wallet })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(AccountError::NotRegistered)
        ));
    }

    #[tokio::test]
    async fn test_unwhitelist_not_linked() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let err = TestHarness::<Account>::with(())
            .given(vec![AccountEvent::Registered {
                client_id,
                email,
                registered_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::UnwhitelistWallet { wallet })
            .await
            .then_expect_error();

        assert!(matches!(
            err,
            LifecycleError::Apply(AccountError::NotLinkedToAlpaca)
        ));
    }

    #[tokio::test]
    async fn test_unwhitelist_already_absent_is_idempotent() {
        let email = Email("user@example.com".to_string());
        let client_id = ClientId::new();
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();
        let wallet = address!("0x1111111111111111111111111111111111111111");

        TestHarness::<Account>::with(())
            .given(vec![
                AccountEvent::Registered { client_id, email, registered_at },
                AccountEvent::LinkedToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                    linked_at,
                },
            ])
            .when(AccountCommand::UnwhitelistWallet { wallet })
            .await
            .then_expect_events(&[]);
    }

    #[test]
    fn test_apply_wallet_unwhitelisted_removes_wallet() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let account = replay::<Account>(vec![
            AccountEvent::Registered { client_id, email, registered_at },
            AccountEvent::LinkedToAlpaca {
                alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
                linked_at,
            },
            AccountEvent::WalletWhitelisted {
                wallet,
                whitelisted_at: chrono::Utc::now(),
            },
            AccountEvent::WalletUnwhitelisted {
                wallet,
                unwhitelisted_at: chrono::Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        let Account::LinkedToAlpaca { whitelisted_wallets, .. } = account
        else {
            panic!("Expected account to be LinkedToAlpaca")
        };

        assert!(
            whitelisted_wallets.is_empty(),
            "Wallet should be removed after unwhitelisting"
        );
    }

    #[test]
    fn test_apply_wallet_whitelisted_to_non_linked_returns_error() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let error = replay::<Account>(vec![
            AccountEvent::Registered {
                client_id,
                email,
                registered_at: chrono::Utc::now(),
            },
            AccountEvent::WalletWhitelisted {
                wallet,
                whitelisted_at: chrono::Utc::now(),
            },
        ])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::UnexpectedEvent { .. }));
    }

    #[test]
    fn test_apply_wallet_unwhitelisted_to_non_linked_returns_error() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let error = replay::<Account>(vec![
            AccountEvent::Registered {
                client_id,
                email,
                registered_at: chrono::Utc::now(),
            },
            AccountEvent::WalletUnwhitelisted {
                wallet,
                unwhitelisted_at: chrono::Utc::now(),
            },
        ])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::UnexpectedEvent { .. }));
    }
}
