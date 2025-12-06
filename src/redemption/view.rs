use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use thiserror::Error;

use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
use crate::redemption::{Redemption, RedemptionEvent};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionView {
    Unavailable,
    Detected {
        issuer_request_id: IssuerRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
    AlpacaCalled {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        called_at: DateTime<Utc>,
    },
    Burning {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        called_at: DateTime<Utc>,
        alpaca_journal_completed_at: DateTime<Utc>,
    },
    Completed {
        issuer_request_id: IssuerRequestId,
        burn_tx_hash: B256,
        block_number: u64,
        completed_at: DateTime<Utc>,
    },
    Failed {
        issuer_request_id: IssuerRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl Default for RedemptionView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl RedemptionView {
    fn update_alpaca_called(
        &mut self,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        called_at: DateTime<Utc>,
    ) {
        let Self::Detected {
            underlying,
            token,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
            ..
        } = self
        else {
            return;
        };

        *self = Self::AlpacaCalled {
            issuer_request_id,
            tokenization_request_id,
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: *wallet,
            quantity: quantity.clone(),
            tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at,
        };
    }

    fn update_alpaca_journal_completed(
        &mut self,
        issuer_request_id: IssuerRequestId,
        alpaca_journal_completed_at: DateTime<Utc>,
    ) {
        let Self::AlpacaCalled {
            tokenization_request_id,
            underlying,
            token,
            wallet,
            quantity,
            tx_hash,
            block_number,
            detected_at,
            called_at,
            ..
        } = self
        else {
            return;
        };

        *self = Self::Burning {
            issuer_request_id,
            tokenization_request_id: tokenization_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: *wallet,
            quantity: quantity.clone(),
            tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at: *called_at,
            alpaca_journal_completed_at,
        };
    }
}

impl View<Redemption> for RedemptionView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        match &event.payload {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            } => {
                *self = Self::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token: token.clone(),
                    wallet: *wallet,
                    quantity: quantity.clone(),
                    tx_hash: *tx_hash,
                    block_number: *block_number,
                    detected_at: *detected_at,
                };
            }
            RedemptionEvent::AlpacaCalled {
                issuer_request_id,
                tokenization_request_id,
                called_at,
            } => {
                self.update_alpaca_called(
                    issuer_request_id.clone(),
                    tokenization_request_id.clone(),
                    *called_at,
                );
            }
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id,
                error,
                failed_at,
            }
            | RedemptionEvent::RedemptionFailed {
                issuer_request_id,
                reason: error,
                failed_at,
            }
            | RedemptionEvent::BurningFailed {
                issuer_request_id,
                error,
                failed_at,
            } => {
                *self = Self::Failed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: error.clone(),
                    failed_at: *failed_at,
                };
            }
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id,
                alpaca_journal_completed_at,
            } => {
                self.update_alpaca_journal_completed(
                    issuer_request_id.clone(),
                    *alpaca_journal_completed_at,
                );
            }
            RedemptionEvent::TokensBurned {
                issuer_request_id,
                tx_hash,
                block_number,
                burned_at,
                ..
            } => {
                *self = Self::Completed {
                    issuer_request_id: issuer_request_id.clone(),
                    burn_tx_hash: *tx_hash,
                    block_number: *block_number,
                    completed_at: *burned_at,
                };
            }
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum RedemptionViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("JSON deserialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Finds all redemptions in the `Detected` state.
///
/// These are redemptions where a transfer was detected on-chain but the
/// Alpaca redeem API hasn't been called yet (or the call failed).
pub(crate) async fn find_detected(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRequestId, RedemptionView)>, RedemptionViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload!: String"
        FROM redemption_view
        WHERE json_extract(payload, '$.Detected') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let view: RedemptionView = serde_json::from_str(&row.payload)?;
            Ok((IssuerRequestId::new(&row.view_id), view))
        })
        .collect()
}

/// Finds all redemptions in the `AlpacaCalled` state.
///
/// These are redemptions where Alpaca's redeem API was called but the
/// journal hasn't completed yet.
pub(crate) async fn find_alpaca_called(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRequestId, RedemptionView)>, RedemptionViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload!: String"
        FROM redemption_view
        WHERE json_extract(payload, '$.AlpacaCalled') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let view: RedemptionView = serde_json::from_str(&row.payload)?;
            Ok((IssuerRequestId::new(&row.view_id), view))
        })
        .collect()
}

/// Finds all redemptions in the `Burning` state.
///
/// These are redemptions where Alpaca journal completed but the on-chain
/// burn hasn't been executed yet (or failed).
pub(crate) async fn find_burning(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRequestId, RedemptionView)>, RedemptionViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload!: String"
        FROM redemption_view
        WHERE json_extract(payload, '$.Burning') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let view: RedemptionView = serde_json::from_str(&row.payload)?;
            Ok((IssuerRequestId::new(&row.view_id), view))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use chrono::Utc;
    use cqrs_es::{EventEnvelope, View};
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;

    use super::{
        RedemptionView, find_alpaca_called, find_burning, find_detected,
    };
    use crate::mint::{IssuerRequestId, Quantity, TokenizationRequestId};
    use crate::redemption::{Redemption, RedemptionEvent};
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    #[test]
    fn test_view_starts_as_unavailable() {
        let view = RedemptionView::default();
        assert!(matches!(view, RedemptionView::Unavailable));
    }

    #[test]
    fn test_view_updates_on_detected_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-view-123");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;
        let detected_at = Utc::now();

        let event = EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        };

        view.update(&event);

        let RedemptionView::Detected {
            issuer_request_id: view_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
        } = view
        else {
            panic!("Expected Detected view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
    }

    #[test]
    fn test_view_updates_on_alpaca_called_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-alpaca-called-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-tok-456");
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;
        let detected_at = Utc::now();
        let called_at = Utc::now();

        let detected_event = EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        };

        view.update(&detected_event);

        let alpaca_called_event = EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                called_at,
            },
            metadata: HashMap::default(),
        };

        view.update(&alpaca_called_event);

        let RedemptionView::AlpacaCalled {
            issuer_request_id: view_id,
            tokenization_request_id: view_tok_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
            called_at: view_called_at,
        } = view
        else {
            panic!("Expected AlpacaCalled view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tok_id, tokenization_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
        assert_eq!(view_called_at, called_at);
    }

    #[test]
    fn test_view_updates_on_alpaca_journal_completed_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-burning-123");
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burning-456");
        let underlying = UnderlyingSymbol::new("NVDA");
        let token = TokenSymbol::new("tNVDA");
        let wallet = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let quantity = Quantity::new(Decimal::from(25));
        let tx_hash = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        let block_number = 99999;
        let detected_at = Utc::now();
        let called_at = Utc::now();
        let alpaca_journal_completed_at = Utc::now();

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                called_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 3,
            payload: RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at,
            },
            metadata: HashMap::default(),
        });

        let RedemptionView::Burning {
            issuer_request_id: view_id,
            tokenization_request_id: view_tok_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
            called_at: view_called_at,
            alpaca_journal_completed_at: view_alpaca_journal_completed_at,
        } = view
        else {
            panic!("Expected Burning view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tok_id, tokenization_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
        assert_eq!(view_called_at, called_at);
        assert_eq!(
            view_alpaca_journal_completed_at,
            alpaca_journal_completed_at
        );
    }

    #[test]
    fn test_view_updates_on_redemption_failed_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-failed-123");
        let underlying = UnderlyingSymbol::new("META");
        let token = TokenSymbol::new("tMETA");
        let wallet = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let quantity = Quantity::new(Decimal::from(75));
        let tx_hash = b256!(
            "0x3333333333333333333333333333333333333333333333333333333333333333"
        );
        let block_number = 11111;
        let detected_at = Utc::now();
        let reason = "Alpaca journal timeout".to_string();
        let failed_at = Utc::now();

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::RedemptionFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
                failed_at,
            },
            metadata: HashMap::default(),
        });

        let RedemptionView::Failed {
            issuer_request_id: view_id,
            reason: view_reason,
            failed_at: view_failed_at,
        } = view
        else {
            panic!("Expected Failed view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_reason, reason);
        assert_eq!(view_failed_at, failed_at);
    }

    #[test]
    fn test_view_updates_on_alpaca_call_failed_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRequestId::new("red-call-failed-123");
        let underlying = UnderlyingSymbol::new("GOOGL");
        let token = TokenSymbol::new("tGOOGL");
        let wallet = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let quantity = Quantity::new(Decimal::from(10));
        let tx_hash = b256!(
            "0x4444444444444444444444444444444444444444444444444444444444444444"
        );
        let block_number = 22222;
        let detected_at = Utc::now();
        let error = "Alpaca API timeout".to_string();
        let failed_at = Utc::now();

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCallFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: error.clone(),
                failed_at,
            },
            metadata: HashMap::default(),
        });

        let RedemptionView::Failed {
            issuer_request_id: view_id,
            reason: view_reason,
            failed_at: view_failed_at,
        } = view
        else {
            panic!("Expected Failed view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_reason, error);
        assert_eq!(view_failed_at, failed_at);
    }

    async fn setup_test_db() -> sqlx::Pool<sqlx::Sqlite> {
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

    async fn insert_view_row(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        view_id: &str,
        view: &RedemptionView,
    ) {
        let payload = serde_json::to_string(view).unwrap();
        sqlx::query!(
            r#"
            INSERT INTO redemption_view (view_id, version, payload)
            VALUES ($1, 1, $2)
            "#,
            view_id,
            payload
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_find_detected_returns_only_detected_views() {
        let pool = setup_test_db().await;

        let detected_id = "red-detected-1";
        let detected_view = RedemptionView::Detected {
            issuer_request_id: IssuerRequestId::new(detected_id),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(100)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        };
        insert_view_row(&pool, detected_id, &detected_view).await;

        let alpaca_called_id = "red-alpaca-called-1";
        let alpaca_called_view = RedemptionView::AlpacaCalled {
            issuer_request_id: IssuerRequestId::new(alpaca_called_id),
            tokenization_request_id: TokenizationRequestId::new("tok-1"),
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            wallet: address!("0x9876543210fedcba9876543210fedcba98765432"),
            quantity: Quantity::new(Decimal::from(50)),
            tx_hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            block_number: 54321,
            detected_at: Utc::now(),
            called_at: Utc::now(),
        };
        insert_view_row(&pool, alpaca_called_id, &alpaca_called_view).await;

        let completed_id = "red-completed-1";
        let completed_view = RedemptionView::Completed {
            issuer_request_id: IssuerRequestId::new(completed_id),
            burn_tx_hash: b256!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            block_number: 99999,
            completed_at: Utc::now(),
        };
        insert_view_row(&pool, completed_id, &completed_view).await;

        let result = find_detected(&pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.0, detected_id);
        assert!(matches!(result[0].1, RedemptionView::Detected { .. }));
    }

    #[tokio::test]
    async fn test_find_detected_returns_empty_when_none_exist() {
        let pool = setup_test_db().await;

        let completed_id = "red-completed-only";
        let completed_view = RedemptionView::Completed {
            issuer_request_id: IssuerRequestId::new(completed_id),
            burn_tx_hash: b256!(
                "0x3333333333333333333333333333333333333333333333333333333333333333"
            ),
            block_number: 88888,
            completed_at: Utc::now(),
        };
        insert_view_row(&pool, completed_id, &completed_view).await;

        let result = find_detected(&pool).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_detected_returns_multiple_detected_views() {
        let pool = setup_test_db().await;

        for i in 1_u64..=3 {
            let id = format!("red-detected-{i}");
            let view = RedemptionView::Detected {
                issuer_request_id: IssuerRequestId::new(&id),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(i * 10)),
                tx_hash: b256!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ),
                block_number: 12345 + i,
                detected_at: Utc::now(),
            };
            insert_view_row(&pool, &id, &view).await;
        }

        let result = find_detected(&pool).await.unwrap();

        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_find_alpaca_called_returns_only_alpaca_called_views() {
        let pool = setup_test_db().await;

        let detected_id = "red-detected-for-alpaca";
        let detected_view = RedemptionView::Detected {
            issuer_request_id: IssuerRequestId::new(detected_id),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(100)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        };
        insert_view_row(&pool, detected_id, &detected_view).await;

        let alpaca_called_id = "red-alpaca-called-query";
        let alpaca_called_view = RedemptionView::AlpacaCalled {
            issuer_request_id: IssuerRequestId::new(alpaca_called_id),
            tokenization_request_id: TokenizationRequestId::new("tok-query"),
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            wallet: address!("0x9876543210fedcba9876543210fedcba98765432"),
            quantity: Quantity::new(Decimal::from(50)),
            tx_hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            block_number: 54321,
            detected_at: Utc::now(),
            called_at: Utc::now(),
        };
        insert_view_row(&pool, alpaca_called_id, &alpaca_called_view).await;

        let burning_id = "red-burning-for-alpaca";
        let burning_view = RedemptionView::Burning {
            issuer_request_id: IssuerRequestId::new(burning_id),
            tokenization_request_id: TokenizationRequestId::new("tok-burn"),
            underlying: UnderlyingSymbol::new("NVDA"),
            token: TokenSymbol::new("tNVDA"),
            wallet: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            quantity: Quantity::new(Decimal::from(25)),
            tx_hash: b256!(
                "0x4444444444444444444444444444444444444444444444444444444444444444"
            ),
            block_number: 77777,
            detected_at: Utc::now(),
            called_at: Utc::now(),
            alpaca_journal_completed_at: Utc::now(),
        };
        insert_view_row(&pool, burning_id, &burning_view).await;

        let result = find_alpaca_called(&pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.0, alpaca_called_id);
        assert!(matches!(result[0].1, RedemptionView::AlpacaCalled { .. }));
    }

    #[tokio::test]
    async fn test_find_alpaca_called_returns_empty_when_none_exist() {
        let pool = setup_test_db().await;

        let detected_id = "red-only-detected";
        let detected_view = RedemptionView::Detected {
            issuer_request_id: IssuerRequestId::new(detected_id),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(100)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        };
        insert_view_row(&pool, detected_id, &detected_view).await;

        let result = find_alpaca_called(&pool).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_burning_returns_only_burning_views() {
        let pool = setup_test_db().await;

        let detected_id = "red-detected-for-burn";
        let detected_view = RedemptionView::Detected {
            issuer_request_id: IssuerRequestId::new(detected_id),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: Quantity::new(Decimal::from(100)),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        };
        insert_view_row(&pool, detected_id, &detected_view).await;

        let alpaca_called_id = "red-alpaca-for-burn";
        let alpaca_called_view = RedemptionView::AlpacaCalled {
            issuer_request_id: IssuerRequestId::new(alpaca_called_id),
            tokenization_request_id: TokenizationRequestId::new("tok-2"),
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            wallet: address!("0x9876543210fedcba9876543210fedcba98765432"),
            quantity: Quantity::new(Decimal::from(50)),
            tx_hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            block_number: 54321,
            detected_at: Utc::now(),
            called_at: Utc::now(),
        };
        insert_view_row(&pool, alpaca_called_id, &alpaca_called_view).await;

        let burning_id = "red-burning-query";
        let burning_view = RedemptionView::Burning {
            issuer_request_id: IssuerRequestId::new(burning_id),
            tokenization_request_id: TokenizationRequestId::new(
                "tok-burn-query",
            ),
            underlying: UnderlyingSymbol::new("NVDA"),
            token: TokenSymbol::new("tNVDA"),
            wallet: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            quantity: Quantity::new(Decimal::from(25)),
            tx_hash: b256!(
                "0x4444444444444444444444444444444444444444444444444444444444444444"
            ),
            block_number: 77777,
            detected_at: Utc::now(),
            called_at: Utc::now(),
            alpaca_journal_completed_at: Utc::now(),
        };
        insert_view_row(&pool, burning_id, &burning_view).await;

        let completed_id = "red-completed-for-burn";
        let completed_view = RedemptionView::Completed {
            issuer_request_id: IssuerRequestId::new(completed_id),
            burn_tx_hash: b256!(
                "0x5555555555555555555555555555555555555555555555555555555555555555"
            ),
            block_number: 88888,
            completed_at: Utc::now(),
        };
        insert_view_row(&pool, completed_id, &completed_view).await;

        let result = find_burning(&pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.0, burning_id);
        assert!(matches!(result[0].1, RedemptionView::Burning { .. }));
    }

    #[tokio::test]
    async fn test_find_burning_returns_empty_when_none_exist() {
        let pool = setup_test_db().await;

        let completed_id = "red-completed-only-burn";
        let completed_view = RedemptionView::Completed {
            issuer_request_id: IssuerRequestId::new(completed_id),
            burn_tx_hash: b256!(
                "0x6666666666666666666666666666666666666666666666666666666666666666"
            ),
            block_number: 99999,
            completed_at: Utc::now(),
        };
        insert_view_row(&pool, completed_id, &completed_view).await;

        let result = find_burning(&pool).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_burning_returns_multiple_burning_views() {
        let pool = setup_test_db().await;

        for i in 1_u64..=2 {
            let id = format!("red-burning-{i}");
            let view = RedemptionView::Burning {
                issuer_request_id: IssuerRequestId::new(&id),
                tokenization_request_id: TokenizationRequestId::new(format!(
                    "tok-{i}"
                )),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                quantity: Quantity::new(Decimal::from(i * 10)),
                tx_hash: b256!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ),
                block_number: 12345 + i,
                detected_at: Utc::now(),
                called_at: Utc::now(),
                alpaca_journal_completed_at: Utc::now(),
            };
            insert_view_row(&pool, &id, &view).await;
        }

        let result = find_burning(&pool).await.unwrap();

        assert_eq!(result.len(), 2);
    }
}
