use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{AlpacaAccountNumber, ClientId, Email};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AccountViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

/// Read model for an account, projected from the `account_view` table.
///
/// `account_view` stores the event-sorcery `Lifecycle<Account>` payload
/// (`{"Live": {...}}`). The `find_by_*` queries extract the `$.Live` sub-object,
/// which mirrors these variants field-for-field, so it deserializes straight
/// into `AccountView`. Non-live rows (`Uninitialized` / `Failed`) have a NULL
/// `$.Live` and are treated as "not found".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum AccountView {
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

pub(crate) async fn find_by_client_id(
    pool: &Pool<Sqlite>,
    client_id: &ClientId,
) -> Result<Option<AccountView>, AccountViewError> {
    let client_id_str = client_id.to_string();
    let row = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM account_view
        WHERE json_extract(payload, '$.Live.Registered.client_id') = ?
           OR json_extract(payload, '$.Live.LinkedToAlpaca.client_id') = ?
        "#,
        client_id_str,
        client_id_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(live) = row.and_then(|row| row.live) else {
        return Ok(None);
    };

    let view: AccountView = serde_json::from_str(&live)?;

    Ok(Some(view))
}

pub(crate) async fn find_by_email(
    pool: &Pool<Sqlite>,
    email: &Email,
) -> Result<Option<AccountView>, AccountViewError> {
    let row = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM account_view
        WHERE json_extract(payload, '$.Live.Registered.email') = ?
           OR json_extract(payload, '$.Live.LinkedToAlpaca.email') = ?
        "#,
        email.0,
        email.0
    )
    .fetch_optional(pool)
    .await?;

    let Some(live) = row.and_then(|row| row.live) else {
        return Ok(None);
    };

    let view: AccountView = serde_json::from_str(&live)?;

    Ok(Some(view))
}

pub(crate) async fn find_by_wallet(
    pool: &Pool<Sqlite>,
    wallet: &Address,
) -> Result<Option<AccountView>, AccountViewError> {
    let wallet_str = format!("{wallet:#x}");
    let row = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM account_view
        WHERE EXISTS(
            SELECT 1
            FROM json_each(payload, '$.Live.LinkedToAlpaca.whitelisted_wallets')
            WHERE value = ?
        )
        "#,
        wallet_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(live) = row.and_then(|row| row.live) else {
        return Ok(None);
    };

    let view: AccountView = serde_json::from_str(&live)?;

    Ok(Some(view))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use event_sorcery::{Store, StoreBuilder};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::sync::Arc;
    use uuid::Uuid;

    use super::*;
    use crate::account::{Account, AccountCommand};

    struct TestHarness {
        pool: Pool<Sqlite>,
        store: Arc<Store<Account>>,
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

            let (store, _projection) =
                StoreBuilder::<Account>::new(pool.clone())
                    .build(())
                    .await
                    .expect("Failed to build account store");

            Self { pool, store }
        }

        async fn register_account(&self, client_id: ClientId, email: Email) {
            self.store
                .send(&client_id, AccountCommand::Register { client_id, email })
                .await
                .expect("Failed to register account");
        }

        async fn link_to_alpaca(
            &self,
            client_id: &ClientId,
            alpaca_account: AlpacaAccountNumber,
        ) {
            self.store
                .send(
                    client_id,
                    AccountCommand::LinkToAlpaca { alpaca_account },
                )
                .await
                .expect("Failed to link to Alpaca");
        }

        async fn whitelist_wallet(
            &self,
            client_id: &ClientId,
            wallet: Address,
        ) {
            self.store
                .send(client_id, AccountCommand::WhitelistWallet { wallet })
                .await
                .expect("Failed to whitelist wallet");
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

    #[tokio::test]
    async fn test_find_by_wallet_returns_linked_account() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let client_id = ClientId::new();
        let email = Email("wallet@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA-W".to_string());
        let wallet = address!("0x1111111111111111111111111111111111111111");

        harness.register_account(client_id, email).await;
        harness.link_to_alpaca(&client_id, alpaca_account).await;
        harness.whitelist_wallet(&client_id, wallet).await;

        let view = find_by_wallet(pool, &wallet)
            .await
            .expect("Query should succeed")
            .expect("View should exist");

        let AccountView::LinkedToAlpaca {
            client_id: found_client_id,
            whitelisted_wallets,
            ..
        } = view
        else {
            panic!("Expected LinkedToAlpaca, got {view:?}")
        };

        assert_eq!(found_client_id, client_id);
        assert!(whitelisted_wallets.contains(&wallet));
    }

    #[tokio::test]
    async fn test_find_by_wallet_returns_none_for_unknown_wallet() {
        let pool = setup_test_db().await;

        let wallet = address!("0x9999999999999999999999999999999999999999");

        let result =
            find_by_wallet(&pool, &wallet).await.expect("Query should succeed");

        assert!(result.is_none());
    }

    // Proves the account_view-to-lifecycle migration is data-safe: after the
    // view table is cleared (as the migration's DROP TABLE does), a fresh
    // `StoreBuilder::build` rebuilds every row from the event log via catch_up.
    #[tokio::test]
    async fn build_rebuilds_account_view_from_events_after_view_cleared() {
        let pool = setup_test_db().await;

        let client_id = ClientId::new();
        let email = Email("rebuild@example.com".to_string());
        let alpaca_account = AlpacaAccountNumber("ALPACA-R".to_string());
        let wallet = address!("0x2222222222222222222222222222222222222222");

        {
            let (store, _projection) =
                StoreBuilder::<Account>::new(pool.clone())
                    .build(())
                    .await
                    .expect("Failed to build account store");
            store
                .send(
                    &client_id,
                    AccountCommand::Register {
                        client_id,
                        email: email.clone(),
                    },
                )
                .await
                .expect("Failed to register account");
            store
                .send(
                    &client_id,
                    AccountCommand::LinkToAlpaca { alpaca_account },
                )
                .await
                .expect("Failed to link to Alpaca");
            store
                .send(&client_id, AccountCommand::WhitelistWallet { wallet })
                .await
                .expect("Failed to whitelist wallet");
        }

        // The migration drops account_view; simulate that, then rebuild from
        // the event log.
        sqlx::query("DELETE FROM account_view")
            .execute(&pool)
            .await
            .expect("Failed to clear account_view");

        let (_store, _projection) = StoreBuilder::<Account>::new(pool.clone())
            .build(())
            .await
            .expect("Failed to rebuild account store");

        let by_email = find_by_email(&pool, &email)
            .await
            .expect("Query should succeed")
            .expect("View should be rebuilt from events");
        assert!(matches!(by_email, AccountView::LinkedToAlpaca { .. }));

        let by_wallet = find_by_wallet(&pool, &wallet)
            .await
            .expect("Query should succeed")
            .expect("View should be rebuilt from events");
        let AccountView::LinkedToAlpaca { client_id: found_client_id, .. } =
            by_wallet
        else {
            panic!("Expected LinkedToAlpaca, got {by_wallet:?}")
        };
        assert_eq!(found_client_id, client_id);
    }
}
