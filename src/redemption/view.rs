use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};
use cqrs_es::persist::{GenericQuery, QueryReplay};
use cqrs_es::{AggregateError, EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

use super::IssuerRedemptionRequestId;
use crate::mint::{Quantity, TokenizationRequestId};
use crate::redemption::{Redemption, RedemptionError, RedemptionEvent};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RedemptionView {
    Unavailable,
    Detected {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
    AlpacaCalled {
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        called_at: DateTime<Utc>,
    },
    Burning {
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        called_at: DateTime<Utc>,
        alpaca_journal_completed_at: DateTime<Utc>,
    },
    Completed {
        issuer_request_id: IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        block_number: u64,
        completed_at: DateTime<Utc>,
    },
    Failed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    BurnFailed {
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        called_at: DateTime<Utc>,
        alpaca_journal_completed_at: DateTime<Utc>,
        error: String,
        failed_at: DateTime<Utc>,
    },
}

impl Default for RedemptionView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl RedemptionView {
    pub(crate) const fn underlying(&self) -> Option<&UnderlyingSymbol> {
        use RedemptionView::*;
        match self {
            Detected { underlying, .. }
            | AlpacaCalled { underlying, .. }
            | Burning { underlying, .. }
            | BurnFailed { underlying, .. } => Some(underlying),

            Unavailable | Completed { .. } | Failed { .. } => None,
        }
    }

    fn update_alpaca_called(
        &mut self,
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
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
            alpaca_quantity,
            dust_quantity,
            tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at,
        };
    }

    fn update_alpaca_journal_completed(
        &mut self,
        issuer_request_id: IssuerRedemptionRequestId,
        alpaca_journal_completed_at: DateTime<Utc>,
    ) {
        let Self::AlpacaCalled {
            tokenization_request_id,
            underlying,
            token,
            wallet,
            quantity,
            alpaca_quantity,
            dust_quantity,
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
            alpaca_quantity: alpaca_quantity.clone(),
            dust_quantity: dust_quantity.clone(),
            tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at: *called_at,
            alpaca_journal_completed_at,
        };
    }

    fn update_burning_failed(&mut self, error: &str, failed_at: DateTime<Utc>) {
        let Self::Burning {
            issuer_request_id,
            tokenization_request_id,
            underlying,
            token,
            wallet,
            quantity,
            alpaca_quantity,
            dust_quantity,
            tx_hash,
            block_number,
            detected_at,
            called_at,
            alpaca_journal_completed_at,
        } = self
        else {
            return;
        };

        *self = Self::BurnFailed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            wallet: *wallet,
            quantity: quantity.clone(),
            alpaca_quantity: alpaca_quantity.clone(),
            dust_quantity: dust_quantity.clone(),
            tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            called_at: *called_at,
            alpaca_journal_completed_at: *alpaca_journal_completed_at,
            error: error.to_string(),
            failed_at,
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
                alpaca_quantity,
                dust_quantity,
                called_at,
            } => {
                self.update_alpaca_called(
                    issuer_request_id.clone(),
                    tokenization_request_id.clone(),
                    alpaca_quantity.clone(),
                    dust_quantity.clone(),
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
            } => {
                *self = Self::Failed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: error.clone(),
                    failed_at: *failed_at,
                };
            }

            RedemptionEvent::BurningFailed { error, failed_at, .. } => {
                self.update_burning_failed(error, *failed_at);
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
    #[error(transparent)]
    IssuerRequestIdParse(#[from] super::IssuerRedemptionRequestIdParseError),
    #[error("Replay error: {0}")]
    Replay(#[from] AggregateError<RedemptionError>),
}

/// Replays all `Redemption` events through the `redemption_view`.
///
/// Uses `QueryReplay` to re-project the view from existing events in the event store.
/// This is used at startup to ensure the view includes new fields (alpaca_quantity,
/// dust_quantity) that were added after the original events were stored.
pub async fn replay_redemption_view(
    pool: Pool<Sqlite>,
) -> Result<(), RedemptionViewError> {
    info!("Rebuilding redemption view from events");

    sqlx::query!("DELETE FROM redemption_view").execute(&pool).await?;

    let view_repo =
        Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
            pool.clone(),
            "redemption_view".to_string(),
        ));
    let query = GenericQuery::new(view_repo);

    let event_repo = SqliteEventRepository::new(pool);
    let replay = QueryReplay::new(event_repo, query);
    replay.replay_all().await?;

    info!("Redemption view rebuild complete");

    Ok(())
}

/// Finds all redemptions in the `Detected` state.
///
/// These are redemptions where a transfer was detected on-chain but the
/// Alpaca redeem API hasn't been called yet (or the call failed).
pub(crate) async fn find_detected(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRedemptionRequestId, RedemptionView)>, RedemptionViewError>
{
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
            let id: IssuerRedemptionRequestId = row.view_id.parse()?;
            Ok((id, view))
        })
        .collect()
}

/// Finds all redemptions in the `AlpacaCalled` state.
///
/// These are redemptions where Alpaca's redeem API was called but the
/// journal hasn't completed yet.
pub(crate) async fn find_alpaca_called(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRedemptionRequestId, RedemptionView)>, RedemptionViewError>
{
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
            let id: IssuerRedemptionRequestId = row.view_id.parse()?;
            Ok((id, view))
        })
        .collect()
}

/// Finds all redemptions in the `Burning` state.
///
/// These are redemptions where Alpaca journal completed but the on-chain
/// burn hasn't been executed yet.
pub(crate) async fn find_burning(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRedemptionRequestId, RedemptionView)>, RedemptionViewError>
{
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
            let id: IssuerRedemptionRequestId = row.view_id.parse()?;
            Ok((id, view))
        })
        .collect()
}

/// Finds all redemptions in the `BurnFailed` state.
///
/// These are redemptions where Alpaca journal completed but the on-chain
/// burn failed. They need recovery - the burn should be retried.
pub(crate) async fn find_burn_failed(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRedemptionRequestId, RedemptionView)>, RedemptionViewError>
{
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload!: String"
        FROM redemption_view
        WHERE json_extract(payload, '$.BurnFailed') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let view: RedemptionView = serde_json::from_str(&row.payload)?;
            let id: IssuerRedemptionRequestId = row.view_id.parse()?;
            Ok((id, view))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address, b256};
    use chrono::Utc;
    use cqrs_es::persist::GenericQuery;
    use cqrs_es::{EventEnvelope, View};
    use rust_decimal::Decimal;
    use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::{
        RedemptionView, find_alpaca_called, find_burn_failed, find_burning,
        find_detected,
    };
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::{
        IssuerRedemptionRequestId, Redemption, RedemptionCommand,
        RedemptionEvent,
    };
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
    use crate::vault::MultiBurnEntry;
    use crate::vault::mock::MockVaultService;

    fn make_detected_envelope(
        aggregate_id: &str,
        sequence: usize,
        event: RedemptionEvent,
    ) -> EventEnvelope<Redemption> {
        EventEnvelope::<Redemption> {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload: event,
            metadata: HashMap::default(),
        }
    }

    async fn insert_event_raw(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) {
        sqlx::query!(
            r#"
            INSERT INTO events (
                aggregate_type, aggregate_id, sequence,
                event_type, event_version, payload, metadata
            )
            VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
            "#,
            aggregate_id,
            sequence,
            event_type,
            payload
        )
        .execute(pool)
        .await
        .unwrap();
    }

    struct TestHarness {
        pool: sqlx::Pool<sqlx::Sqlite>,
        cqrs: SqliteCqrs<Redemption>,
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

            let view_repo = Arc::new(SqliteViewRepository::<
                RedemptionView,
                Redemption,
            >::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
            let query = GenericQuery::new(view_repo);
            let vault_service = Arc::new(MockVaultService::new_success());
            let cqrs =
                sqlite_cqrs(pool.clone(), vec![Box::new(query)], vault_service);

            Self { pool, cqrs }
        }

        async fn detect_redemption(
            &self,
            id: &IssuerRedemptionRequestId,
            underlying: &str,
            wallet: Address,
            quantity: u64,
            tx_hash: B256,
            block_number: u64,
        ) {
            self.cqrs
                .execute(
                    &id.to_string(),
                    RedemptionCommand::Detect {
                        issuer_request_id: id.clone(),
                        underlying: UnderlyingSymbol::new(underlying),
                        token: TokenSymbol::new(format!("t{underlying}")),
                        wallet,
                        quantity: Quantity::new(Decimal::from(quantity)),
                        tx_hash,
                        block_number,
                    },
                )
                .await
                .expect("Failed to detect redemption");
        }

        async fn call_alpaca(
            &self,
            id: &IssuerRedemptionRequestId,
            tokenization_request_id: &str,
        ) {
            self.cqrs
                .execute(
                    &id.to_string(),
                    RedemptionCommand::RecordAlpacaCall {
                        issuer_request_id: id.clone(),
                        tokenization_request_id: TokenizationRequestId::new(
                            tokenization_request_id,
                        ),
                        alpaca_quantity: Quantity::new(Decimal::from(100)),
                        dust_quantity: Quantity::new(Decimal::ZERO),
                    },
                )
                .await
                .expect("Failed to record alpaca call");
        }

        async fn confirm_alpaca_complete(
            &self,
            id: &IssuerRedemptionRequestId,
        ) {
            self.cqrs
                .execute(
                    &id.to_string(),
                    RedemptionCommand::ConfirmAlpacaComplete {
                        issuer_request_id: id.clone(),
                    },
                )
                .await
                .expect("Failed to confirm alpaca complete");
        }

        async fn burn_tokens(&self, id: &IssuerRedemptionRequestId) {
            self.cqrs
                .execute(
                    &id.to_string(),
                    RedemptionCommand::BurnTokens {
                        issuer_request_id: id.clone(),
                        vault: address!(
                            "0xcccccccccccccccccccccccccccccccccccccccc"
                        ),
                        burns: vec![MultiBurnEntry {
                            receipt_id: U256::from(1),
                            burn_shares: U256::from(100),
                            receipt_info: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner: address!(
                            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
                        ),
                    },
                )
                .await
                .expect("Failed to burn tokens");
        }
    }

    #[test]
    fn test_view_starts_as_unavailable() {
        let view = RedemptionView::default();
        assert!(matches!(view, RedemptionView::Unavailable));
    }

    #[test]
    fn test_view_updates_on_detected_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
            aggregate_id: issuer_request_id.to_string(),
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
            aggregate_id: issuer_request_id.to_string(),
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
            aggregate_id: issuer_request_id.to_string(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                alpaca_quantity: quantity.clone(),
                dust_quantity: Quantity::new(Decimal::ZERO),
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
            alpaca_quantity: view_alpaca_quantity,
            dust_quantity: view_dust_quantity,
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
        assert_eq!(view_alpaca_quantity, quantity);
        assert_eq!(view_dust_quantity, Quantity::new(Decimal::ZERO));
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
        assert_eq!(view_called_at, called_at);
    }

    #[test]
    fn test_view_updates_on_alpaca_journal_completed_event() {
        let mut view = RedemptionView::default();

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
            aggregate_id: issuer_request_id.to_string(),
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
            aggregate_id: issuer_request_id.to_string(),
            sequence: 2,
            payload: RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                alpaca_quantity: quantity.clone(),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at,
            },
            metadata: HashMap::default(),
        });

        view.update(&EventEnvelope::<Redemption> {
            aggregate_id: issuer_request_id.to_string(),
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
            alpaca_quantity: view_alpaca_quantity,
            dust_quantity: view_dust_quantity,
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
        assert_eq!(view_alpaca_quantity, quantity);
        assert_eq!(view_dust_quantity, Quantity::new(Decimal::ZERO));
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
            aggregate_id: issuer_request_id.to_string(),
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
            aggregate_id: issuer_request_id.to_string(),
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

        let issuer_request_id = IssuerRedemptionRequestId::random();
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
            aggregate_id: issuer_request_id.to_string(),
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
            aggregate_id: issuer_request_id.to_string(),
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

    #[tokio::test]
    async fn test_find_detected_returns_only_detected_views() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let detected_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &detected_id,
                "AAPL",
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                100,
                b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                12345,
            )
            .await;

        let alpaca_called_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &alpaca_called_id,
                "TSLA",
                address!("0x9876543210fedcba9876543210fedcba98765432"),
                50,
                b256!("0x1111111111111111111111111111111111111111111111111111111111111111"),
                54321,
            )
            .await;
        harness.call_alpaca(&alpaca_called_id, "tok-1").await;

        let completed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &completed_id,
                "NVDA",
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                25,
                b256!("0x2222222222222222222222222222222222222222222222222222222222222222"),
                77777,
            )
            .await;
        harness.call_alpaca(&completed_id, "tok-2").await;
        harness.confirm_alpaca_complete(&completed_id).await;
        harness.burn_tokens(&completed_id).await;

        let result = find_detected(pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, detected_id);
        assert!(matches!(result[0].1, RedemptionView::Detected { .. }));
    }

    #[tokio::test]
    async fn test_find_detected_returns_empty_when_none_exist() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let completed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &completed_id,
                "AAPL",
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                100,
                b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                12345,
            )
            .await;
        harness.call_alpaca(&completed_id, "tok-1").await;
        harness.confirm_alpaca_complete(&completed_id).await;
        harness.burn_tokens(&completed_id).await;

        let result = find_detected(pool).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_detected_returns_multiple_detected_views() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        for i in 1_u64..=3 {
            let id = IssuerRedemptionRequestId::random();
            harness
                .detect_redemption(
                    &id,
                    "AAPL",
                    address!("0x1234567890abcdef1234567890abcdef12345678"),
                    i * 10,
                    b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                    12345 + i,
                )
                .await;
        }

        let result = find_detected(pool).await.unwrap();

        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_find_alpaca_called_returns_only_alpaca_called_views() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let detected_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &detected_id,
                "AAPL",
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                100,
                b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                12345,
            )
            .await;

        let alpaca_called_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &alpaca_called_id,
                "TSLA",
                address!("0x9876543210fedcba9876543210fedcba98765432"),
                50,
                b256!("0x1111111111111111111111111111111111111111111111111111111111111111"),
                54321,
            )
            .await;
        harness.call_alpaca(&alpaca_called_id, "tok-query").await;

        let burning_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &burning_id,
                "NVDA",
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                25,
                b256!("0x4444444444444444444444444444444444444444444444444444444444444444"),
                77777,
            )
            .await;
        harness.call_alpaca(&burning_id, "tok-burn").await;
        harness.confirm_alpaca_complete(&burning_id).await;

        let result = find_alpaca_called(pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, alpaca_called_id);
        assert!(matches!(result[0].1, RedemptionView::AlpacaCalled { .. }));
    }

    #[tokio::test]
    async fn test_find_alpaca_called_returns_empty_when_none_exist() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let detected_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &detected_id,
                "AAPL",
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                100,
                b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                12345,
            )
            .await;

        let result = find_alpaca_called(pool).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_burning_returns_only_burning_views() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let detected_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &detected_id,
                "AAPL",
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                100,
                b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                12345,
            )
            .await;

        let alpaca_called_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &alpaca_called_id,
                "TSLA",
                address!("0x9876543210fedcba9876543210fedcba98765432"),
                50,
                b256!("0x1111111111111111111111111111111111111111111111111111111111111111"),
                54321,
            )
            .await;
        harness.call_alpaca(&alpaca_called_id, "tok-2").await;

        let burning_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &burning_id,
                "NVDA",
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                25,
                b256!("0x4444444444444444444444444444444444444444444444444444444444444444"),
                77777,
            )
            .await;
        harness.call_alpaca(&burning_id, "tok-burn-query").await;
        harness.confirm_alpaca_complete(&burning_id).await;

        let completed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &completed_id,
                "META",
                address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                75,
                b256!("0x5555555555555555555555555555555555555555555555555555555555555555"),
                66666,
            )
            .await;
        harness.call_alpaca(&completed_id, "tok-complete").await;
        harness.confirm_alpaca_complete(&completed_id).await;
        harness.burn_tokens(&completed_id).await;

        let result = find_burning(pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, burning_id);
        assert!(matches!(result[0].1, RedemptionView::Burning { .. }));
    }

    #[tokio::test]
    async fn test_find_burning_returns_empty_when_none_exist() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let completed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &completed_id,
                "AAPL",
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                100,
                b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                12345,
            )
            .await;
        harness.call_alpaca(&completed_id, "tok-1").await;
        harness.confirm_alpaca_complete(&completed_id).await;
        harness.burn_tokens(&completed_id).await;

        let result = find_burning(pool).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_burning_returns_multiple_burning_views() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        for i in 1_u64..=2 {
            let id = IssuerRedemptionRequestId::random();
            harness
                .detect_redemption(
                    &id,
                    "AAPL",
                    address!("0x1234567890abcdef1234567890abcdef12345678"),
                    i * 10,
                    b256!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                    12345 + i,
                )
                .await;
            harness.call_alpaca(&id, &format!("tok-{i}")).await;
            harness.confirm_alpaca_complete(&id).await;
        }

        let result = find_burning(pool).await.unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_view_updates_on_burning_failed_event() {
        let mut view = RedemptionView::default();
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let id = issuer_request_id.to_string();
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burn-failed-456");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0xdddddddddddddddddddddddddddddddddddddddd");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0x6666666666666666666666666666666666666666666666666666666666666666"
        );
        let block_number = 88888;
        let detected_at = Utc::now();
        let called_at = Utc::now();
        let alpaca_journal_completed_at = Utc::now();
        let error = "On-chain burn failed: insufficient gas".to_string();
        let failed_at = Utc::now();

        view.update(&make_detected_envelope(
            &id,
            1,
            RedemptionEvent::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
                detected_at,
            },
        ));

        view.update(&make_detected_envelope(
            &id,
            2,
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                alpaca_quantity: quantity.clone(),
                dust_quantity: Quantity::new(Decimal::ZERO),
                called_at,
            },
        ));

        view.update(&make_detected_envelope(
            &id,
            3,
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: issuer_request_id.clone(),
                alpaca_journal_completed_at,
            },
        ));

        assert!(matches!(view, RedemptionView::Burning { .. }));

        view.update(&make_detected_envelope(
            &id,
            4,
            RedemptionEvent::BurningFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: error.clone(),
                failed_at,
            },
        ));

        let RedemptionView::BurnFailed {
            issuer_request_id: view_id,
            tokenization_request_id: view_tok_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            alpaca_quantity: view_alpaca_quantity,
            dust_quantity: view_dust_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            error: view_error,
            ..
        } = view
        else {
            panic!("Expected BurnFailed view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tok_id, tokenization_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_alpaca_quantity, quantity);
        assert_eq!(view_dust_quantity, Quantity::new(Decimal::ZERO));
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_error, error);
    }

    #[tokio::test]
    async fn test_find_burn_failed_returns_only_burn_failed_views() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, cqrs } = &harness;

        // Create a redemption in Detected state
        let detected_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &detected_id,
                "AAPL",
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                100,
                b256!(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ),
                12345,
            )
            .await;

        // Create a redemption in Burning state
        let burning_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &burning_id,
                "TSLA",
                address!("0x9876543210fedcba9876543210fedcba98765432"),
                50,
                b256!(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                ),
                54321,
            )
            .await;
        harness.call_alpaca(&burning_id, "tok-burning").await;
        harness.confirm_alpaca_complete(&burning_id).await;

        // Create a redemption in BurnFailed state
        let burn_failed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &burn_failed_id,
                "NVDA",
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                25,
                b256!(
                    "0x2222222222222222222222222222222222222222222222222222222222222222"
                ),
                77777,
            )
            .await;
        harness.call_alpaca(&burn_failed_id, "tok-burn-fail").await;
        harness.confirm_alpaca_complete(&burn_failed_id).await;

        // Record burn failure
        cqrs.execute(
            &burn_failed_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: burn_failed_id.clone(),
                error: "Insufficient gas".to_string(),
            },
        )
        .await
        .expect("Failed to record burn failure");

        let result = find_burn_failed(pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, burn_failed_id);
        assert!(matches!(result[0].1, RedemptionView::BurnFailed { .. }));
    }

    #[tokio::test]
    async fn test_replay_redemption_view_populates_from_events() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let id = issuer_request_id.to_string();
        let quantity = Quantity::new(Decimal::from(100));

        let detected_event = RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            quantity: quantity.clone(),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 12345,
            detected_at: Utc::now(),
        };

        let detected_payload = serde_json::to_string(&detected_event).unwrap();
        insert_event_raw(
            &pool,
            &id,
            1,
            "RedemptionEvent::Detected",
            &detected_payload,
        )
        .await;

        let alpaca_called_event = RedemptionEvent::AlpacaCalled {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new(
                "tok-replay-123",
            ),
            alpaca_quantity: quantity.clone(),
            dust_quantity: Quantity::new(Decimal::ZERO),
            called_at: Utc::now(),
        };

        let alpaca_payload =
            serde_json::to_string(&alpaca_called_event).unwrap();
        insert_event_raw(
            &pool,
            &id,
            2,
            "RedemptionEvent::AlpacaCalled",
            &alpaca_payload,
        )
        .await;

        let initial_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) as count FROM redemption_view"
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(initial_count, 0, "View should be empty before replay");

        super::replay_redemption_view(pool.clone())
            .await
            .expect("Replay should succeed");

        let row = sqlx::query!(
            r#"SELECT payload as "payload!: String" FROM redemption_view WHERE view_id = ?"#,
            id
        )
        .fetch_optional(&pool)
        .await
        .unwrap();

        let row = row.expect("View should be populated after replay");
        let view: RedemptionView = serde_json::from_str(&row.payload).unwrap();

        let RedemptionView::AlpacaCalled {
            issuer_request_id: view_id,
            alpaca_quantity: view_alpaca_qty,
            dust_quantity: view_dust_qty,
            ..
        } = view
        else {
            panic!("Expected AlpacaCalled view, got {view:?}");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_alpaca_qty, quantity);
        assert_eq!(view_dust_qty, Quantity::new(Decimal::ZERO));
    }
}
