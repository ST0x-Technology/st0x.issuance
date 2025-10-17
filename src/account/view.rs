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
pub(crate) struct AccountView {
    pub(crate) client_id: ClientId,
    pub(crate) email: Email,
    pub(crate) alpaca_account: AlpacaAccountNumber,
    pub(crate) status: LinkedAccountStatus,
    pub(crate) linked_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

impl Default for AccountView {
    fn default() -> Self {
        Self {
            client_id: ClientId(String::new()),
            email: Email(String::new()),
            alpaca_account: AlpacaAccountNumber(String::new()),
            status: LinkedAccountStatus::Active,
            linked_at: DateTime::UNIX_EPOCH,
            updated_at: DateTime::UNIX_EPOCH,
        }
    }
}

impl View<Account> for AccountView {
    fn update(&mut self, event: &EventEnvelope<Account>) {
        match &event.payload {
            AccountEvent::AccountLinked {
                client_id,
                email,
                alpaca_account,
                linked_at,
            } => {
                self.client_id = client_id.clone();
                self.email = email.clone();
                self.alpaca_account = alpaca_account.clone();
                self.status = LinkedAccountStatus::Active;
                self.linked_at = *linked_at;
                self.updated_at = *linked_at;
            }
        }
    }
}

pub(crate) async fn find_by_email(
    pool: &Pool<Sqlite>,
    email: &Email,
) -> Result<Option<AccountView>, AccountViewError> {
    let email_str = email.as_str();
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE json_extract(payload, '$.email') = ?
        "#,
        email_str
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
    use super::*;
    use cqrs_es::EventEnvelope;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;

    #[test]
    fn test_view_update_from_account_linked_event() {
        let client_id = ClientId("test-client-123".to_string());
        let email = Email("user@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA123".to_string());
        let linked_at = Utc::now();

        let event = AccountEvent::AccountLinked {
            client_id: client_id.clone(),
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
        view.update(&envelope);

        assert_eq!(view.client_id, client_id);
        assert_eq!(view.email, email);
        assert_eq!(view.alpaca_account, alpaca_account);
        assert_eq!(view.status, LinkedAccountStatus::Active);
        assert_eq!(view.linked_at, linked_at);
        assert_eq!(view.updated_at, linked_at);
    }

    #[tokio::test]
    async fn test_find_by_email_returns_view() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let client_id = ClientId("test-client-456".to_string());
        let email = Email("test@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA456".to_string());
        let linked_at = Utc::now();

        let view = AccountView {
            client_id: client_id.clone(),
            email: email.clone(),
            alpaca_account: alpaca_account.clone(),
            status: LinkedAccountStatus::Active,
            linked_at,
            updated_at: linked_at,
        };

        let payload =
            serde_json::to_string(&view).expect("Failed to serialize view");

        sqlx::query!(
            r"
            INSERT INTO account_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            client_id.0,
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert view");

        let result =
            find_by_email(&pool, &email).await.expect("Query should succeed");

        assert!(result.is_some());
        let found_view = result.unwrap();
        assert_eq!(found_view.client_id, client_id);
        assert_eq!(found_view.email, email);
        assert_eq!(found_view.alpaca_account, alpaca_account);
        assert_eq!(found_view.status, LinkedAccountStatus::Active);
    }

    #[tokio::test]
    async fn test_find_by_email_returns_none_when_not_found() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let email = Email("nonexistent@example.com".to_string());

        let result =
            find_by_email(&pool, &email).await.expect("Query should succeed");

        assert!(result.is_none());
    }
}
