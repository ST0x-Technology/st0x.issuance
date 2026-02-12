use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{Account, AccountEvent, AlpacaAccountNumber, ClientId, Email};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AccountViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
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

impl View<Account> for AccountView {
    fn update(&mut self, event: &EventEnvelope<Account>) {
        match &event.payload {
            AccountEvent::Registered { client_id, email, registered_at } => {
                *self = Self::Registered {
                    client_id: *client_id,
                    email: email.clone(),
                    registered_at: *registered_at,
                };
            }

            AccountEvent::LinkedToAlpaca { alpaca_account, linked_at } => {
                if let Self::Registered { client_id, email, registered_at } =
                    self.clone()
                {
                    *self = Self::LinkedToAlpaca {
                        client_id,
                        email,
                        alpaca_account: alpaca_account.clone(),
                        whitelisted_wallets: Vec::new(),
                        registered_at,
                        linked_at: *linked_at,
                    };
                }
            }

            AccountEvent::WalletWhitelisted { wallet, .. } => {
                if let Self::LinkedToAlpaca { whitelisted_wallets, .. } = self {
                    whitelisted_wallets.push(*wallet);
                }
            }

            AccountEvent::WalletUnwhitelisted { wallet, .. } => {
                if let Self::LinkedToAlpaca { whitelisted_wallets, .. } = self {
                    whitelisted_wallets.retain(|w| w != wallet);
                }
            }
        }
    }
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
        WHERE json_extract(payload, '$.Registered.client_id') = ?
           OR json_extract(payload, '$.LinkedToAlpaca.client_id') = ?
        "#,
        client_id_str,
        client_id_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: AccountView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

pub(crate) async fn find_by_email(
    pool: &Pool<Sqlite>,
    email: &Email,
) -> Result<Option<AccountView>, AccountViewError> {
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE json_extract(payload, '$.Registered.email') = ?
           OR json_extract(payload, '$.LinkedToAlpaca.email') = ?
        "#,
        email.0,
        email.0
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: AccountView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
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
            FROM json_each(payload, '$.LinkedToAlpaca.whitelisted_wallets')
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

    let view: AccountView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::EventEnvelope;
    use cqrs_es::persist::GenericQuery;
    use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    use super::*;
    use crate::account::{Account, AccountCommand};

    struct TestHarness {
        pool: Pool<Sqlite>,
        cqrs: SqliteCqrs<Account>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let view_repo =
                Arc::new(SqliteViewRepository::<AccountView, Account>::new(
                    pool.clone(),
                    "account_view".to_string(),
                ));
            let query = GenericQuery::new(view_repo);
            let cqrs = sqlite_cqrs(pool.clone(), vec![Box::new(query)], ());

            Self { pool, cqrs }
        }

        async fn register_account(&self, client_id: ClientId, email: Email) {
            self.cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::Register { client_id, email },
                )
                .await
                .expect("Failed to register account");
        }

        async fn link_to_alpaca(
            &self,
            client_id: &ClientId,
            alpaca_account: AlpacaAccountNumber,
        ) {
            self.cqrs
                .execute(
                    &client_id.to_string(),
                    AccountCommand::LinkToAlpaca { alpaca_account },
                )
                .await
                .expect("Failed to link to Alpaca");
        }
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

        let mut view = AccountView::default();

        assert!(matches!(view, AccountView::Unavailable));

        view.update(&envelope);

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

        let mut view = AccountView::Registered {
            client_id,
            email: email.clone(),
            registered_at,
        };

        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = Utc::now();

        let event = AccountEvent::LinkedToAlpaca {
            alpaca_account: alpaca_account.clone(),
            linked_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: client_id.to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

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
        assert_eq!(view_alpaca, alpaca_account);
        assert!(whitelisted_wallets.is_empty());
        assert_eq!(view_registered_at, registered_at);
        assert_eq!(view_linked_at, linked_at);
    }

    #[tokio::test]
    async fn test_find_by_client_id_returns_view() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let client_id = ClientId::new();
        let email = Email("client@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA789".to_string());

        harness.register_account(client_id, email.clone()).await;
        harness.link_to_alpaca(&client_id, alpaca_account.clone()).await;

        let view = find_by_client_id(pool, &client_id)
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

        let client_id = ClientId(Uuid::new_v4());

        let result = find_by_client_id(&pool, &client_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_find_by_email_returns_linked_to_alpaca_view() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let client_id = ClientId::new();
        let email = Email("email@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA999".to_string());

        harness.register_account(client_id, email.clone()).await;
        harness.link_to_alpaca(&client_id, alpaca_account.clone()).await;

        let view = find_by_email(pool, &email)
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
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let client_id = ClientId::new();
        let email = Email("registered@example.com".to_string());

        harness.register_account(client_id, email.clone()).await;

        let view = find_by_email(pool, &email)
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
        let mut view = AccountView::LinkedToAlpaca {
            client_id: ClientId(Uuid::new_v4()),
            email: Email("user@example.com".to_string()),
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
            whitelisted_wallets: Vec::new(),
            registered_at: Utc::now(),
            linked_at: Utc::now(),
        };

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

        view.update(&envelope);

        let AccountView::LinkedToAlpaca { whitelisted_wallets, .. } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert_eq!(whitelisted_wallets.len(), 1);
        assert_eq!(whitelisted_wallets[0], wallet);
    }

    #[test]
    fn test_view_update_from_wallet_unwhitelisted_event() {
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let mut view = AccountView::LinkedToAlpaca {
            client_id: ClientId(Uuid::new_v4()),
            email: Email("user@example.com".to_string()),
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
            whitelisted_wallets: vec![wallet],
            registered_at: Utc::now(),
            linked_at: Utc::now(),
        };

        let event = AccountEvent::WalletUnwhitelisted {
            wallet,
            unwhitelisted_at: Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "user@example.com".to_string(),
            sequence: 4,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let AccountView::LinkedToAlpaca { whitelisted_wallets, .. } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert!(
            whitelisted_wallets.is_empty(),
            "Wallet should be removed after unwhitelisting"
        );
    }

    #[test]
    fn test_view_update_unwhitelist_absent_wallet_is_noop() {
        let wallet = address!("0x1111111111111111111111111111111111111111");

        let mut view = AccountView::LinkedToAlpaca {
            client_id: ClientId(Uuid::new_v4()),
            email: Email("user@example.com".to_string()),
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
            whitelisted_wallets: Vec::new(),
            registered_at: Utc::now(),
            linked_at: Utc::now(),
        };

        let event = AccountEvent::WalletUnwhitelisted {
            wallet,
            unwhitelisted_at: Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "user@example.com".to_string(),
            sequence: 4,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let AccountView::LinkedToAlpaca { whitelisted_wallets, .. } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert!(
            whitelisted_wallets.is_empty(),
            "Wallets should remain empty after unwhitelisting absent wallet"
        );
    }
}
