use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{
    ClientId, IssuerRequestId, Mint, MintEvent, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintView {
    NotFound,
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

impl Default for MintView {
    fn default() -> Self {
        Self::NotFound
    }
}

impl View<Mint> for MintView {
    fn update(&mut self, event: &EventEnvelope<Mint>) {
        match &event.payload {
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
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    quantity: quantity.clone(),
                    underlying: underlying.clone(),
                    token: token.clone(),
                    network: network.clone(),
                    client_id: client_id.clone(),
                    wallet: *wallet,
                    initiated_at: *initiated_at,
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
                    journal_confirmed_at: *confirmed_at,
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
                    reason: reason.clone(),
                    rejected_at: *rejected_at,
                };
            }
        }
    }
}

pub(crate) async fn find_by_issuer_request_id(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerRequestId,
) -> Result<Option<MintView>, MintViewError> {
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

    let view: MintView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::EventEnvelope;
    use rust_decimal::Decimal;
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;

    use super::*;
    use crate::mint::MintEvent;

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
    fn test_view_update_from_mint_initiated_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let event = MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id: client_id.clone(),
            wallet,
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = MintView::default();

        assert!(matches!(view, MintView::NotFound));

        view.update(&envelope);

        let MintView::Initiated {
            issuer_request_id: view_issuer_id,
            tokenization_request_id: view_tokenization_id,
            quantity: view_quantity,
            underlying: view_underlying,
            token: view_token,
            network: view_network,
            client_id: view_client_id,
            wallet: view_wallet,
            initiated_at: view_initiated_at,
        } = view
        else {
            panic!("Expected Initiated variant")
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(view_tokenization_id, tokenization_request_id);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_network, network);
        assert_eq!(view_client_id, client_id);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_initiated_at, initiated_at);
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
        let client_id = ClientId("client-777".to_string());
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = Utc::now();

        let view = MintView::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id: client_id.clone(),
            wallet,
            initiated_at,
        };

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

        let MintView::Initiated {
            issuer_request_id: found_issuer_id,
            tokenization_request_id: found_tokenization_id,
            quantity: found_quantity,
            underlying: found_underlying,
            token: found_token,
            network: found_network,
            client_id: found_client_id,
            wallet: found_wallet,
            initiated_at: _,
        } = result.unwrap()
        else {
            panic!("Expected Initiated variant")
        };

        assert_eq!(found_issuer_id, issuer_request_id);
        assert_eq!(found_tokenization_id, tokenization_request_id);
        assert_eq!(found_quantity, quantity);
        assert_eq!(found_underlying, underlying);
        assert_eq!(found_token, token);
        assert_eq!(found_network, network);
        assert_eq!(found_client_id, client_id);
        assert_eq!(found_wallet, wallet);
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
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut view = MintView::Initiated {
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

        let confirmed_at = Utc::now();

        let event = MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let MintView::JournalConfirmed {
            issuer_request_id: view_issuer_id,
            journal_confirmed_at,
            ..
        } = view
        else {
            panic!("Expected JournalConfirmed variant")
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(journal_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_view_update_from_journal_rejected_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut view = MintView::Initiated {
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

        let rejected_at = Utc::now();
        let reason = "Insufficient funds".to_string();

        let event = MintEvent::JournalRejected {
            issuer_request_id: issuer_request_id.clone(),
            reason: reason.clone(),
            rejected_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let MintView::JournalRejected {
            issuer_request_id: view_issuer_id,
            reason: view_reason,
            rejected_at: view_rejected_at,
            ..
        } = view
        else {
            panic!("Expected JournalRejected variant")
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(view_reason, reason);
        assert_eq!(view_rejected_at, rejected_at);
    }
}
