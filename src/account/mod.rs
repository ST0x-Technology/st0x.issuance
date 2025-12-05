mod api;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite, encode::IsNull, sqlite::SqliteArgumentValue};
use std::str::FromStr;
use uuid::Uuid;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

pub use api::{
    AccountLinkResponse, RegisterAccountResponse, WhitelistWalletResponse,
};
pub(crate) use api::{connect_account, register_account, whitelist_wallet};

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
pub(crate) enum AccountCommand {
    Register { client_id: ClientId, email: Email },
    LinkToAlpaca { alpaca_account: AlpacaAccountNumber },
    WhitelistWallet { wallet: Address },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum AccountEvent {
    Registered {
        client_id: ClientId,
        email: Email,
        registered_at: DateTime<Utc>,
    },
    LinkedToAlpaca {
        alpaca_account: AlpacaAccountNumber,
        linked_at: DateTime<Utc>,
    },
    WalletWhitelisted {
        wallet: Address,
        whitelisted_at: DateTime<Utc>,
    },
}

impl DomainEvent for AccountEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Registered { .. } => "AccountEvent::Registered".to_string(),
            Self::LinkedToAlpaca { .. } => {
                "AccountEvent::LinkedToAlpaca".to_string()
            }
            Self::WalletWhitelisted { .. } => {
                "AccountEvent::WalletWhitelisted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Account {
    pub(crate) client_id: ClientId,
    pub(crate) email: Email,
    pub(crate) registered_at: DateTime<Utc>,
    pub(crate) alpaca: Option<AlpacaAccountNumber>,
    pub(crate) linked_at: Option<DateTime<Utc>>,
    pub(crate) whitelisted_wallets: Vec<Address>,
}

impl Account {
    pub(crate) fn from_event(
        event: &AccountEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            AccountEvent::Registered { client_id, email, registered_at } => {
                Ok(Self {
                    client_id: *client_id,
                    email: email.clone(),
                    registered_at: *registered_at,
                    alpaca: None,
                    linked_at: None,
                    whitelisted_wallets: Vec::new(),
                })
            }
            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".to_string(),
                event: event.event_type(),
            }),
        }
    }

    pub(crate) fn apply_transition(
        event: &AccountEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            AccountEvent::Registered { .. } => Err(LifecycleError::Mismatch {
                state: "Live".to_string(),
                event: event.event_type(),
            }),
            AccountEvent::LinkedToAlpaca { alpaca_account, linked_at } => {
                if current.alpaca.is_some() {
                    return Err(LifecycleError::Mismatch {
                        state: "already linked".to_string(),
                        event: event.event_type(),
                    });
                }
                Ok(Self {
                    client_id: current.client_id,
                    email: current.email.clone(),
                    registered_at: current.registered_at,
                    alpaca: Some(alpaca_account.clone()),
                    linked_at: Some(*linked_at),
                    whitelisted_wallets: Vec::new(),
                })
            }
            AccountEvent::WalletWhitelisted { wallet, .. } => {
                if current.alpaca.is_none() {
                    return Err(LifecycleError::Mismatch {
                        state: "not linked".to_string(),
                        event: event.event_type(),
                    });
                }
                let mut new_wallets = current.whitelisted_wallets.clone();
                new_wallets.push(*wallet);
                Ok(Self {
                    client_id: current.client_id,
                    email: current.email.clone(),
                    registered_at: current.registered_at,
                    alpaca: current.alpaca.clone(),
                    linked_at: current.linked_at,
                    whitelisted_wallets: new_wallets,
                })
            }
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum AccountError {
    #[error("Invalid email format: {email}")]
    InvalidEmail { email: String },
    #[error("Account already registered for email: {email}")]
    AccountAlreadyRegistered { email: String },
    #[error("Account is already linked to Alpaca")]
    AlreadyLinkedToAlpaca,
    #[error("Account is not linked to Alpaca")]
    NotLinkedToAlpaca,
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError<Never>),
}

#[async_trait]
impl Aggregate for Lifecycle<Account, Never> {
    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = AccountError;
    type Services = ();

    fn aggregate_type() -> String {
        "Account".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, Account::apply_transition)
            .or_initialize(&event, Account::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self.live(), &command) {
            (Ok(_), AccountCommand::Register { email, .. }) => {
                Err(AccountError::AccountAlreadyRegistered {
                    email: email.as_str().to_string(),
                })
            }

            (Err(_), AccountCommand::Register { client_id, email }) => {
                Ok(vec![AccountEvent::Registered {
                    client_id: *client_id,
                    email: email.clone(),
                    registered_at: Utc::now(),
                }])
            }

            (Err(e), _) => Err(e.into()),

            (Ok(account), AccountCommand::LinkToAlpaca { alpaca_account }) => {
                if account.alpaca.is_some() {
                    return Err(AccountError::AlreadyLinkedToAlpaca);
                }

                Ok(vec![AccountEvent::LinkedToAlpaca {
                    alpaca_account: alpaca_account.clone(),
                    linked_at: Utc::now(),
                }])
            }

            (Ok(account), AccountCommand::WhitelistWallet { wallet }) => {
                if account.alpaca.is_none() {
                    return Err(AccountError::NotLinkedToAlpaca);
                }

                if account.whitelisted_wallets.contains(wallet) {
                    Ok(vec![])
                } else {
                    Ok(vec![AccountEvent::WalletWhitelisted {
                        wallet: *wallet,
                        whitelisted_at: Utc::now(),
                    }])
                }
            }
        }
    }
}

impl View<Self> for Lifecycle<Account, Never> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, Account::apply_transition)
            .or_initialize(&event.payload, Account::from_event);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum AccountView {
    Unavailable,
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

impl Default for AccountView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl From<&Lifecycle<Account, Never>> for AccountView {
    fn from(lifecycle: &Lifecycle<Account, Never>) -> Self {
        match lifecycle {
            Lifecycle::Uninitialized | Lifecycle::Failed { .. } => {
                Self::Unavailable
            }
            Lifecycle::Live(account) => {
                match (&account.alpaca, account.linked_at) {
                    (Some(alpaca), Some(linked_at)) => Self::LinkedToAlpaca {
                        client_id: account.client_id,
                        email: account.email.clone(),
                        alpaca_account: alpaca.clone(),
                        whitelisted_wallets: account
                            .whitelisted_wallets
                            .clone(),
                        registered_at: account.registered_at,
                        linked_at,
                    },
                    _ => Self::Registered {
                        client_id: account.client_id,
                        email: account.email.clone(),
                        registered_at: account.registered_at,
                    },
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AccountViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

pub(crate) async fn find_by_client_id(
    pool: &Pool<Sqlite>,
    client_id: &ClientId,
) -> Result<Option<AccountView>, AccountViewError> {
    let client_id_str = client_id.to_string();
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE json_extract(payload, '$.Live.client_id') = ?
        "#,
        client_id_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let lifecycle: Lifecycle<Account, Never> =
        serde_json::from_str(&row.payload)?;
    Ok(Some(AccountView::from(&lifecycle)))
}

pub(crate) async fn find_by_email(
    pool: &Pool<Sqlite>,
    email: &Email,
) -> Result<Option<AccountView>, AccountViewError> {
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE json_extract(payload, '$.Live.email') = ?
        "#,
        email.0
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let lifecycle: Lifecycle<Account, Never> =
        serde_json::from_str(&row.payload)?;
    Ok(Some(AccountView::from(&lifecycle)))
}

pub(crate) async fn find_by_wallet(
    pool: &Pool<Sqlite>,
    wallet: &Address,
) -> Result<Option<AccountView>, AccountViewError> {
    let wallet_str = format!("{wallet:#x}");
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE EXISTS(
            SELECT 1
            FROM json_each(payload, '$.Live.whitelisted_wallets')
            WHERE value = ?
        )
        "#,
        wallet_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let lifecycle: Lifecycle<Account, Never> =
        serde_json::from_str(&row.payload)?;
    Ok(Some(AccountView::from(&lifecycle)))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::{Aggregate, test::TestFramework};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;
    use uuid::Uuid;

    use super::*;
    use crate::lifecycle::LifecycleError;

    type AccountTestFramework = TestFramework<Lifecycle<Account, Never>>;

    #[test]
    fn test_register_creates_new_account() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());

        let events = AccountTestFramework::with(())
            .given_no_previous_events()
            .when(AccountCommand::Register { client_id, email: email.clone() })
            .inspect_result()
            .unwrap();

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

    #[test]
    fn test_register_when_already_registered_returns_error() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());

        AccountTestFramework::with(())
            .given(vec![AccountEvent::Registered {
                client_id: ClientId::new(),
                email: email.clone(),
                registered_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::Register { client_id, email })
            .then_expect_error(AccountError::AccountAlreadyRegistered {
                email: "user@example.com".to_string(),
            });
    }

    #[test]
    fn test_link_to_alpaca_on_registered_account() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        let events = AccountTestFramework::with(())
            .given(vec![AccountEvent::Registered {
                client_id,
                email,
                registered_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::LinkToAlpaca {
                alpaca_account: alpaca_account.clone(),
            })
            .inspect_result()
            .unwrap();

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

    #[test]
    fn test_link_to_alpaca_on_not_registered_returns_error() {
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());

        AccountTestFramework::with(())
            .given_no_previous_events()
            .when(AccountCommand::LinkToAlpaca { alpaca_account })
            .then_expect_error(AccountError::Lifecycle(
                LifecycleError::Uninitialized,
            ));
    }

    #[test]
    fn test_link_to_alpaca_when_already_linked_returns_error() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());

        AccountTestFramework::with(())
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
            .then_expect_error(AccountError::AlreadyLinkedToAlpaca);
    }

    #[test]
    fn test_apply_registered_updates_state() {
        let mut account: Lifecycle<Account, Never> = Lifecycle::default();

        assert!(matches!(account, Lifecycle::Uninitialized));

        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = chrono::Utc::now();

        account.apply(AccountEvent::Registered {
            client_id,
            email: email.clone(),
            registered_at,
        });

        let Lifecycle::Live(inner) = account else {
            panic!("Expected Live state, got {account:?}")
        };

        assert_eq!(inner.client_id, client_id);
        assert_eq!(inner.email, email);
        assert_eq!(inner.registered_at, registered_at);
        assert!(inner.alpaca.is_none());
        assert!(inner.whitelisted_wallets.is_empty());
        assert!(inner.linked_at.is_none());
    }

    #[test]
    fn test_apply_linked_to_alpaca_updates_state() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = chrono::Utc::now();

        let mut account: Lifecycle<Account, Never> = Lifecycle::Live(Account {
            client_id,
            email: email.clone(),
            registered_at,
            alpaca: None,
            linked_at: None,
            whitelisted_wallets: Vec::new(),
        });

        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = chrono::Utc::now();

        account.apply(AccountEvent::LinkedToAlpaca {
            alpaca_account: alpaca_account.clone(),
            linked_at,
        });

        let Lifecycle::Live(inner) = account else {
            panic!("Expected Live state, got {account:?}")
        };

        assert_eq!(inner.client_id, client_id);
        assert_eq!(inner.email, email);
        assert_eq!(inner.alpaca, Some(alpaca_account));
        assert_eq!(inner.linked_at, Some(linked_at));
        assert!(inner.whitelisted_wallets.is_empty());
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

    #[test]
    fn test_whitelist_wallet_on_linked_to_alpaca_account() {
        let email = Email("user@example.com".to_string());
        let client_id = ClientId::new();
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();

        let wallet = address!("0x1111111111111111111111111111111111111111");

        let events = AccountTestFramework::with(())
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
            .inspect_result()
            .unwrap();

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

    #[test]
    fn test_whitelist_wallet_on_not_registered_account() {
        let wallet = address!("0x1111111111111111111111111111111111111111");

        AccountTestFramework::with(())
            .given_no_previous_events()
            .when(AccountCommand::WhitelistWallet { wallet })
            .then_expect_error(AccountError::Lifecycle(
                LifecycleError::Uninitialized,
            ));
    }

    #[test]
    fn test_whitelist_wallet_on_registered_but_not_linked_account() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let wallet = address!("0x1111111111111111111111111111111111111111");

        AccountTestFramework::with(())
            .given(vec![AccountEvent::Registered {
                client_id,
                email,
                registered_at: chrono::Utc::now(),
            }])
            .when(AccountCommand::WhitelistWallet { wallet })
            .then_expect_error(AccountError::NotLinkedToAlpaca);
    }

    #[test]
    fn test_whitelist_already_whitelisted_wallet_is_idempotent() {
        let email = Email("user@example.com".to_string());
        let client_id = ClientId::new();
        let registered_at = chrono::Utc::now();
        let linked_at = chrono::Utc::now();
        let wallet = address!("0x1111111111111111111111111111111111111111");
        let whitelisted_at = chrono::Utc::now();

        AccountTestFramework::with(())
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
            .then_expect_events(vec![]);
    }

    #[test]
    fn test_apply_wallet_whitelisted_adds_wallet() {
        let mut account: Lifecycle<Account, Never> = Lifecycle::Live(Account {
            client_id: ClientId::new(),
            email: Email("user@example.com".to_string()),
            registered_at: chrono::Utc::now(),
            alpaca: Some(AlpacaAccountNumber("ALPACA123".to_string())),
            linked_at: Some(chrono::Utc::now()),
            whitelisted_wallets: Vec::new(),
        });

        let wallet = address!("0x1111111111111111111111111111111111111111");

        account.apply(AccountEvent::WalletWhitelisted {
            wallet,
            whitelisted_at: chrono::Utc::now(),
        });

        let Lifecycle::Live(inner) = account else {
            panic!("Expected Live state")
        };

        assert_eq!(inner.whitelisted_wallets.len(), 1);
        assert_eq!(inner.whitelisted_wallets[0], wallet);
    }

    #[test]
    fn test_apply_wallet_whitelisted_adds_multiple_wallets() {
        let mut account: Lifecycle<Account, Never> = Lifecycle::Live(Account {
            client_id: ClientId::new(),
            email: Email("user@example.com".to_string()),
            registered_at: chrono::Utc::now(),
            alpaca: Some(AlpacaAccountNumber("ALPACA123".to_string())),
            linked_at: Some(chrono::Utc::now()),
            whitelisted_wallets: Vec::new(),
        });

        let wallet1 = address!("0x1111111111111111111111111111111111111111");
        let wallet2 = address!("0x2222222222222222222222222222222222222222");

        account.apply(AccountEvent::WalletWhitelisted {
            wallet: wallet1,
            whitelisted_at: chrono::Utc::now(),
        });

        account.apply(AccountEvent::WalletWhitelisted {
            wallet: wallet2,
            whitelisted_at: chrono::Utc::now(),
        });

        let Lifecycle::Live(inner) = account else {
            panic!("Expected Live state")
        };

        assert_eq!(inner.whitelisted_wallets.len(), 2);
        assert_eq!(inner.whitelisted_wallets[0], wallet1);
        assert_eq!(inner.whitelisted_wallets[1], wallet2);
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

    #[test]
    fn test_view_update_from_registered_event() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = Utc::now();

        let event = AccountEvent::Registered {
            client_id,
            email: email.clone(),
            registered_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: email.as_str().to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut lifecycle: Lifecycle<Account, Never> = Lifecycle::default();

        assert!(matches!(lifecycle, Lifecycle::Uninitialized));

        lifecycle.update(&envelope);

        let view = AccountView::from(&lifecycle);

        let AccountView::Registered {
            client_id: view_client_id,
            email: view_email,
            registered_at: view_registered_at,
        } = view
        else {
            panic!("Expected Registered, got {view:?}")
        };

        assert_eq!(view_client_id, client_id);
        assert_eq!(view_email, email);
        assert_eq!(view_registered_at, registered_at);
    }

    #[test]
    fn test_view_update_from_linked_to_alpaca_event() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let registered_at = Utc::now();

        let mut lifecycle: Lifecycle<Account, Never> =
            Lifecycle::Live(Account {
                client_id,
                email: email.clone(),
                registered_at,
                alpaca: None,
                linked_at: None,
                whitelisted_wallets: Vec::new(),
            });

        let alpaca = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = Utc::now();

        let event = AccountEvent::LinkedToAlpaca {
            alpaca_account: alpaca.clone(),
            linked_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: client_id.to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        lifecycle.update(&envelope);

        let view = AccountView::from(&lifecycle);

        let AccountView::LinkedToAlpaca {
            client_id: view_client_id,
            email: view_email,
            alpaca_account: view_alpaca,
            whitelisted_wallets,
            registered_at: view_registered_at,
            linked_at: view_linked_at,
        } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert_eq!(view_client_id, client_id);
        assert_eq!(view_email, email);
        assert_eq!(view_alpaca, alpaca);
        assert!(whitelisted_wallets.is_empty());
        assert_eq!(view_registered_at, registered_at);
        assert_eq!(view_linked_at, linked_at);
    }

    #[tokio::test]
    async fn test_find_by_client_id_returns_view() {
        let pool = setup_test_db().await;

        let client_id = ClientId::new();
        let email = Email("client@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA789".to_string());
        let registered_at = Utc::now();
        let linked_at = Utc::now();

        let lifecycle: Lifecycle<Account, Never> = Lifecycle::Live(Account {
            client_id,
            email: email.clone(),
            registered_at,
            alpaca: Some(alpaca_account.clone()),
            linked_at: Some(linked_at),
            whitelisted_wallets: Vec::new(),
        });

        let payload =
            serde_json::to_string(&lifecycle).expect("Failed to serialize");

        sqlx::query!(
            r"
            INSERT INTO account_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            client_id,
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert view");

        let view = find_by_client_id(&pool, &client_id)
            .await
            .expect("Query should succeed")
            .expect("View should exist");

        let AccountView::LinkedToAlpaca {
            client_id: found_client_id,
            email: found_email,
            alpaca_account: found_alpaca,
            ..
        } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert_eq!(found_client_id, client_id);
        assert_eq!(found_email, email);
        assert_eq!(found_alpaca, alpaca_account);
    }

    #[tokio::test]
    async fn test_find_by_client_id_returns_none_when_not_found() {
        let pool = setup_test_db().await;

        let client_id = ClientId(uuid::Uuid::new_v4());

        let result = find_by_client_id(&pool, &client_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_find_by_email_returns_linked_to_alpaca_view() {
        let pool = setup_test_db().await;

        let client_id = ClientId::new();
        let email = Email("email@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA999".to_string());
        let registered_at = Utc::now();
        let linked_at = Utc::now();

        let lifecycle: Lifecycle<Account, Never> = Lifecycle::Live(Account {
            client_id,
            email: email.clone(),
            registered_at,
            alpaca: Some(alpaca_account.clone()),
            linked_at: Some(linked_at),
            whitelisted_wallets: Vec::new(),
        });

        let payload =
            serde_json::to_string(&lifecycle).expect("Failed to serialize");

        sqlx::query!(
            r"
            INSERT INTO account_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            client_id,
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert view");

        let view = find_by_email(&pool, &email)
            .await
            .expect("Query should succeed")
            .expect("View should exist");

        let AccountView::LinkedToAlpaca {
            client_id: found_client_id,
            email: found_email,
            alpaca_account: found_alpaca,
            ..
        } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert_eq!(found_client_id, client_id);
        assert_eq!(found_email, email);
        assert_eq!(found_alpaca, alpaca_account);
    }

    #[tokio::test]
    async fn test_find_by_email_returns_registered_view() {
        let pool = setup_test_db().await;

        let client_id = ClientId::new();
        let email = Email("registered@example.com".to_string());
        let registered_at = Utc::now();

        let lifecycle: Lifecycle<Account, Never> = Lifecycle::Live(Account {
            client_id,
            email: email.clone(),
            registered_at,
            alpaca: None,
            linked_at: None,
            whitelisted_wallets: Vec::new(),
        });

        let payload =
            serde_json::to_string(&lifecycle).expect("Failed to serialize");

        sqlx::query!(
            r"
            INSERT INTO account_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            client_id,
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert view");

        let view = find_by_email(&pool, &email)
            .await
            .expect("Query should succeed")
            .expect("View should exist");

        let AccountView::Registered {
            client_id: found_client_id,
            email: found_email,
            ..
        } = view
        else {
            panic!("Expected Registered, got {view:?}")
        };

        assert_eq!(found_client_id, client_id);
        assert_eq!(found_email, email);
    }

    #[tokio::test]
    async fn test_find_by_email_returns_none_when_not_found() {
        let pool = setup_test_db().await;

        let email = Email("nonexistent@example.com".to_string());

        let result =
            find_by_email(&pool, &email).await.expect("Query should succeed");

        assert!(result.is_none());
    }

    #[test]
    fn test_view_update_from_wallet_whitelisted_event() {
        let client_id = ClientId(uuid::Uuid::new_v4());

        let mut lifecycle: Lifecycle<Account, Never> =
            Lifecycle::Live(Account {
                client_id,
                email: Email("user@example.com".to_string()),
                registered_at: Utc::now(),
                alpaca: Some(AlpacaAccountNumber("ALPACA123".to_string())),
                linked_at: Some(Utc::now()),
                whitelisted_wallets: Vec::new(),
            });

        let wallet = address!("0x1111111111111111111111111111111111111111");

        let event = AccountEvent::WalletWhitelisted {
            wallet,
            whitelisted_at: Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "user@example.com".to_string(),
            sequence: 3,
            payload: event,
            metadata: HashMap::new(),
        };

        lifecycle.update(&envelope);

        let view = AccountView::from(&lifecycle);

        let AccountView::LinkedToAlpaca { whitelisted_wallets, .. } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert_eq!(whitelisted_wallets.len(), 1);
        assert_eq!(whitelisted_wallets[0], wallet);
    }
}
