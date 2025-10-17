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

pub(crate) async fn find_by_client_id(
    pool: &Pool<Sqlite>,
    client_id: &ClientId,
) -> Result<Option<AccountView>, AccountViewError> {
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE view_id = ?
        "#,
        client_id.0
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

pub(crate) async fn find_by_alpaca_account(
    pool: &Pool<Sqlite>,
    alpaca_account: &AlpacaAccountNumber,
) -> Result<Option<AccountView>, AccountViewError> {
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM account_view
        WHERE json_extract(payload, '$.alpaca_account') = ?
        "#,
        alpaca_account.0
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: AccountView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}
