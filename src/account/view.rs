use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{
    Account, AccountEvent, AlpacaAccountNumber, ClientId, Email,
    LinkedAccountStatus,
};

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
    Account {
        client_id: ClientId,
        email: Email,
        alpaca_account: AlpacaAccountNumber,
        whitelisted_wallets: Vec<Address>,
        status: LinkedAccountStatus,
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
            AccountEvent::Linked {
                client_id,
                email,
                alpaca_account,
                linked_at,
            } => {
                *self = Self::Account {
                    client_id: *client_id,
                    email: email.clone(),
                    alpaca_account: alpaca_account.clone(),
                    whitelisted_wallets: Vec::new(),
                    status: LinkedAccountStatus::Active,
                    linked_at: *linked_at,
                };
            }
            AccountEvent::WalletWhitelisted { wallet, .. } => {
                if let Self::Account { whitelisted_wallets, .. } = self {
                    whitelisted_wallets.push(*wallet);
                }
            }
        }
    }
}

pub(crate) async fn find_by_client_id(
    pool: &Pool<Sqlite>,
    client_id: &ClientId,
) -> Result<Option<AccountView>, AccountViewError> {
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE client_id_indexed = ?
        "#,
        client_id
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
        WHERE json_extract(payload, '$.Account.email') = ?
        "#,
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
            FROM json_each(payload, '$.Account.whitelisted_wallets')
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
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;

    use super::*;

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
    fn test_view_update_from_account_linked_event() {
        let client_id = ClientId::new();
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = Utc::now();

        let event = AccountEvent::Linked {
            client_id,
            email: email.clone(),
            alpaca_account: alpaca_account.clone(),
            linked_at,
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

        let AccountView::Account {
            client_id: view_client_id,
            email: view_email,
            alpaca_account: view_alpaca,
            whitelisted_wallets,
            status,
            linked_at: view_linked_at,
        } = view
        else {
            panic!("Expected Account, got Unavailable")
        };

        assert_eq!(view_client_id, client_id);
        assert_eq!(view_email, email);
        assert_eq!(view_alpaca, alpaca_account);
        assert!(whitelisted_wallets.is_empty());
        assert_eq!(status, LinkedAccountStatus::Active);
        assert_eq!(view_linked_at, linked_at);
    }

    #[tokio::test]
    async fn test_find_by_client_id_returns_view() {
        let pool = setup_test_db().await;

        let client_id = ClientId::new();
        let email = Email("client@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA789".to_string());
        let linked_at = Utc::now();

        let view = AccountView::Account {
            client_id,
            email: email.clone(),
            alpaca_account: alpaca_account.clone(),
            whitelisted_wallets: Vec::new(),
            status: LinkedAccountStatus::Active,
            linked_at,
        };

        let payload =
            serde_json::to_string(&view).expect("Failed to serialize view");

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

        let result = find_by_client_id(&pool, &client_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_some());

        let AccountView::Account {
            client_id: found_client_id,
            email: found_email,
            alpaca_account: found_alpaca,
            status,
            ..
        } = result.unwrap()
        else {
            panic!("Expected Account, got Unavailable")
        };

        assert_eq!(found_client_id, client_id);
        assert_eq!(found_email, email);
        assert_eq!(found_alpaca, alpaca_account);
        assert_eq!(status, LinkedAccountStatus::Active);
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
    async fn test_find_by_email_returns_view() {
        let pool = setup_test_db().await;

        let client_id = ClientId::new();
        let email = Email("email@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA999".to_string());
        let linked_at = Utc::now();

        let view = AccountView::Account {
            client_id,
            email: email.clone(),
            alpaca_account: alpaca_account.clone(),
            whitelisted_wallets: Vec::new(),
            status: LinkedAccountStatus::Active,
            linked_at,
        };

        let payload =
            serde_json::to_string(&view).expect("Failed to serialize view");

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

        let result =
            find_by_email(&pool, &email).await.expect("Query should succeed");

        assert!(result.is_some());

        let AccountView::Account {
            client_id: found_client_id,
            email: found_email,
            alpaca_account: found_alpaca,
            status,
            ..
        } = result.unwrap()
        else {
            panic!("Expected Account, got Unavailable")
        };

        assert_eq!(found_client_id, client_id);
        assert_eq!(found_email, email);
        assert_eq!(found_alpaca, alpaca_account);
        assert_eq!(status, LinkedAccountStatus::Active);
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
        let mut view = AccountView::Account {
            client_id: ClientId(uuid::Uuid::new_v4()),
            email: Email("user@example.com".to_string()),
            alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
            whitelisted_wallets: Vec::new(),
            status: LinkedAccountStatus::Active,
            linked_at: Utc::now(),
        };

        let wallet = address!("0x1111111111111111111111111111111111111111");

        let event = AccountEvent::WalletWhitelisted {
            wallet,
            whitelisted_at: Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "user@example.com".to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let AccountView::Account { whitelisted_wallets, .. } = view else {
            panic!("Expected Account, got Unavailable")
        };

        assert_eq!(whitelisted_wallets.len(), 1);
        assert_eq!(whitelisted_wallets[0], wallet);
    }
}
