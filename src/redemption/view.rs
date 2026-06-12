use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EntityList, Never, Reactor, deps};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite, SqlitePool};
use thiserror::Error;
use tracing::{debug, warn};

use super::IssuerRedemptionRequestId;
use crate::mint::{Quantity, TokenizationRequestId};
use crate::redemption::{Redemption, RedemptionEvent, TokensBurnedData};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) enum RedemptionView {
    #[default]
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
        /// When the view most recently entered `Detected`. Matches
        /// `detected_at` on first detection; updated to `reprocessed_at`
        /// when `Reprocessed` re-enters this state, so admin triage and the
        /// staleness gate reflect the latest entry rather than the
        /// original detection.
        detected_entered_at: DateTime<Utc>,
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
        /// When the view most recently entered `Burning`. Matches
        /// `alpaca_journal_completed_at` on the happy path; updated to
        /// `resumed_at` when `BurnResumed` re-enters this state, so admin
        /// triage and the staleness gate reflect the latest entry rather than
        /// the original journal completion.
        burning_entered_at: DateTime<Utc>,
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
        /// Fireblocks transaction ID, if a burn was already submitted before failure.
        /// Present when the failure occurred during confirmation (after submission).
        #[serde(default)]
        fireblocks_tx_id: Option<String>,
        /// Planned burns at the time of failure.
        #[serde(default)]
        planned_burns: Vec<super::BurnRecord>,
    },
    Closed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        closed_at: DateTime<Utc>,
    },
}

impl RedemptionView {
    fn update_alpaca_called(
        self,
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
    ) -> Self {
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
            return self;
        };

        Self::AlpacaCalled {
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
        }
    }

    fn update_alpaca_journal_completed(
        self,
        issuer_request_id: IssuerRedemptionRequestId,
        alpaca_journal_completed_at: DateTime<Utc>,
    ) -> Self {
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
            return self;
        };

        Self::Burning {
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
            burning_entered_at: alpaca_journal_completed_at,
        }
    }

    fn update_burning_failed(
        self,
        error: &str,
        failed_at: DateTime<Utc>,
        fireblocks_tx_id: Option<&String>,
        planned_burns: &[super::BurnRecord],
    ) -> Self {
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
            burning_entered_at: _,
        } = self
        else {
            return self;
        };

        Self::BurnFailed {
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
            error: error.to_string(),
            failed_at,
            fireblocks_tx_id: fireblocks_tx_id.cloned(),
            planned_burns: planned_burns.to_vec(),
        }
    }
}

impl RedemptionView {
    fn apply(self, event: &RedemptionEvent) -> Self {
        match event {
            RedemptionEvent::Detected {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
            } => Self::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet: *wallet,
                quantity: quantity.clone(),
                tx_hash: *tx_hash,
                block_number: *block_number,
                detected_at: *detected_at,
                detected_entered_at: *detected_at,
            },
            RedemptionEvent::Reprocessed {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
                reprocessed_at,
                ..
            } => Self::Detected {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token: token.clone(),
                wallet: *wallet,
                quantity: quantity.clone(),
                tx_hash: *tx_hash,
                block_number: *block_number,
                detected_at: *detected_at,
                detected_entered_at: *reprocessed_at,
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
            } => self.update_alpaca_called(
                issuer_request_id.clone(),
                tokenization_request_id.clone(),
                alpaca_quantity.clone(),
                dust_quantity.clone(),
                *called_at,
            ),
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id,
                error,
                failed_at,
            }
            | RedemptionEvent::RedemptionFailed {
                issuer_request_id,
                reason: error,
                failed_at,
            } => Self::Failed {
                issuer_request_id: issuer_request_id.clone(),
                reason: error.clone(),
                failed_at: *failed_at,
            },

            RedemptionEvent::BurningFailed {
                error,
                failed_at,
                fireblocks_tx_id,
                planned_burns,
                ..
            } => self.update_burning_failed(
                error,
                *failed_at,
                fireblocks_tx_id.as_ref(),
                planned_burns,
            ),

            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id,
                alpaca_journal_completed_at,
            } => self.update_alpaca_journal_completed(
                issuer_request_id.clone(),
                *alpaca_journal_completed_at,
            ),
            RedemptionEvent::BurnResumed {
                issuer_request_id,
                underlying,
                token,
                wallet,
                quantity,
                tx_hash,
                block_number,
                detected_at,
                tokenization_request_id,
                alpaca_quantity,
                dust_quantity,
                called_at,
                alpaca_journal_completed_at,
                resumed_at,
                ..
            } => Self::Burning {
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
                burning_entered_at: *resumed_at,
            },
            RedemptionEvent::TokensBurned(TokensBurnedData {
                issuer_request_id,
                tx_hash,
                block_number,
                burned_at,
                ..
            }) => Self::Completed {
                issuer_request_id: issuer_request_id.clone(),
                burn_tx_hash: *tx_hash,
                block_number: *block_number,
                completed_at: *burned_at,
            },
            RedemptionEvent::ExistingBurnRecovered {
                issuer_request_id,
                tx_hash,
                block_number,
                recovered_at,
                ..
            } => Self::Completed {
                issuer_request_id: issuer_request_id.clone(),
                burn_tx_hash: *tx_hash,
                block_number: *block_number,
                completed_at: *recovered_at,
            },
            // View stays in Burning — BurnFireblocksSubmitted is an internal
            // detail that doesn't change the query-facing state.
            RedemptionEvent::BurnFireblocksSubmitted { .. } => self,
            RedemptionEvent::RedemptionClosed {
                issuer_request_id,
                reason,
                closed_at,
            } => Self::Closed {
                issuer_request_id: issuer_request_id.clone(),
                reason: reason.clone(),
                closed_at: *closed_at,
            },
            // #169 force-complete: a manually-verified burn terminalizes the
            // redemption to Completed, same as a normal on-chain burn.
            RedemptionEvent::BurnForceCompleted {
                issuer_request_id,
                burn_tx_hash,
                block_number,
                completed_at,
                ..
            } => Self::Completed {
                issuer_request_id: issuer_request_id.clone(),
                burn_tx_hash: *burn_tx_hash,
                block_number: *block_number,
                completed_at: *completed_at,
            },
        }
    }
}

deps!(RedemptionViewReactor, [Redemption]);

/// Maintains the `redemption_view` table from the `Redemption` event stream.
///
/// event-sorcery allows only one canonical `Table` projection per aggregate
/// (and `Redemption` has none — its `Materialized` type is `Nil`), so this
/// query-facing view of the redemption lifecycle is kept up to date by an
/// explicit reactor rather than a projection.
pub(crate) struct RedemptionViewReactor {
    pool: SqlitePool,
}

impl RedemptionViewReactor {
    pub(crate) const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn project(
        &self,
        id: &IssuerRedemptionRequestId,
        event: &RedemptionEvent,
    ) {
        let view_id = id.to_string();

        let current = match self.load(&view_id).await {
            Ok(view) => view,
            Err(error) => {
                warn!(target: "redemption", view_id, error = %error,
                    "Failed to load redemption view; skipping update"
                );
                return;
            }
        };

        let updated = current.apply(event);

        if let Err(error) = self.upsert(&view_id, &updated).await {
            warn!(target: "redemption", view_id, error = %error,
                "Failed to write redemption view"
            );
        }
    }

    async fn load(
        &self,
        view_id: &str,
    ) -> Result<RedemptionView, RedemptionViewError> {
        let row = sqlx::query!(
            r#"
            SELECT payload as "payload!: String"
            FROM redemption_view
            WHERE view_id = ?
            "#,
            view_id
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(serde_json::from_str(&row.payload)?),
            None => Ok(RedemptionView::Unavailable),
        }
    }

    async fn upsert(
        &self,
        view_id: &str,
        view: &RedemptionView,
    ) -> Result<(), RedemptionViewError> {
        let payload = serde_json::to_string(view)?;

        sqlx::query!(
            "
            INSERT INTO redemption_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ON CONFLICT(view_id) DO UPDATE SET
                version = version + 1,
                payload = ?
            ",
            view_id,
            payload,
            payload
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Reactor for RedemptionViewReactor {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (id, redemption_event) = event.into_inner();
        self.project(&id, &redemption_event).await;
        Ok(())
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
}

/// Rebuilds the `redemption_view` table from the `Redemption` event stream.
///
/// Clears the table, then replays every stored `Redemption` event in
/// `(aggregate_id, sequence)` order through the same projection the live
/// `RedemptionViewReactor` uses. Folding events in that order reproduces the
/// incremental projection, so the rebuilt table matches what live reactions
/// would have produced. Used at startup to recover view state and pick up
/// fields added after the original events were stored.
pub(crate) async fn rebuild_redemption_view(
    pool: &Pool<Sqlite>,
) -> Result<(), RedemptionViewError> {
    debug!(target: "redemption", "Rebuilding redemption view from events");

    sqlx::query!("DELETE FROM redemption_view").execute(pool).await?;

    let reactor = RedemptionViewReactor::new(pool.clone());

    let rows = sqlx::query!(
        r#"
        SELECT
            aggregate_id as "aggregate_id!: String",
            payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Redemption'
        ORDER BY aggregate_id, sequence
        "#
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        let id = row.aggregate_id.parse::<IssuerRedemptionRequestId>()?;
        let event = serde_json::from_str::<RedemptionEvent>(&row.payload)?;
        reactor.project(&id, &event).await;
    }

    debug!(target: "redemption", "Redemption view rebuild complete");

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

/// Finds all redemptions that are not in a terminal state.
///
/// Returns every redemption whose view sits in `Detected`, `AlpacaCalled`,
/// `Burning`, `Failed`, or `BurnFailed` — i.e. anything that hasn't reached
/// `Completed` or `Closed`. Callers (`/admin/stuck`) apply the staleness gate
/// and decide which entries the operator must act on.
pub(crate) async fn find_stuck(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRedemptionRequestId, RedemptionView)>, RedemptionViewError>
{
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload!: String"
        FROM redemption_view
        WHERE json_extract(payload, '$.Detected')     IS NOT NULL
           OR json_extract(payload, '$.AlpacaCalled') IS NOT NULL
           OR json_extract(payload, '$.Burning')      IS NOT NULL
           OR json_extract(payload, '$.Failed')       IS NOT NULL
           OR json_extract(payload, '$.BurnFailed')   IS NOT NULL
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
    use event_sorcery::ReactorHarness;
    use rust_decimal::Decimal;
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::{
        RedemptionView, RedemptionViewReactor, find_alpaca_called,
        find_burn_failed, find_burning, find_detected, find_stuck,
    };
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::redemption::{
        BurnRecord, IssuerRedemptionRequestId, Redemption, RedemptionEvent,
        TokensBurnedData,
    };
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

    async fn migrated_pool() -> SqlitePool {
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

    /// Reads back the projected view for `id`, deserializing the row the reactor
    /// wrote. Returns `Unavailable` when no row exists yet.
    async fn load_view(
        pool: &SqlitePool,
        id: &IssuerRedemptionRequestId,
    ) -> RedemptionView {
        let view_id = id.to_string();
        let row = sqlx::query!(
            r#"
            SELECT payload as "payload!: String"
            FROM redemption_view
            WHERE view_id = ?
            "#,
            view_id
        )
        .fetch_optional(pool)
        .await
        .expect("Failed to query redemption view");

        match row {
            Some(row) => serde_json::from_str(&row.payload)
                .expect("Failed to deserialize redemption view"),
            None => RedemptionView::Unavailable,
        }
    }

    /// Seeds a `Redemption` event into the event store. Used to set up the
    /// rebuild scenario where stored events predate the view table.
    async fn insert_event_raw(
        pool: &SqlitePool,
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

    /// Drives the `redemption_view` projection by feeding events directly through
    /// the reactor — the same load-apply-upsert path the live reactor uses.
    struct TestHarness {
        pool: SqlitePool,
        reactor: ReactorHarness<RedemptionViewReactor>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let pool = migrated_pool().await;
            let reactor =
                ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

            Self { pool, reactor }
        }

        async fn emit(
            &self,
            id: &IssuerRedemptionRequestId,
            event: RedemptionEvent,
        ) {
            self.reactor
                .receive::<Redemption>(id.clone(), event)
                .await
                .expect("Failed to project redemption event");
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
            self.emit(
                id,
                RedemptionEvent::Detected {
                    issuer_request_id: id.clone(),
                    underlying: UnderlyingSymbol::new(underlying),
                    token: TokenSymbol::new(format!("t{underlying}")),
                    wallet,
                    quantity: Quantity::new(Decimal::from(quantity)),
                    tx_hash,
                    block_number,
                    detected_at: Utc::now(),
                },
            )
            .await;
        }

        async fn call_alpaca(
            &self,
            id: &IssuerRedemptionRequestId,
            tokenization_request_id: &str,
        ) {
            self.emit(
                id,
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        tokenization_request_id,
                    ),
                    alpaca_quantity: Quantity::new(Decimal::from(100)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at: Utc::now(),
                },
            )
            .await;
        }

        async fn confirm_alpaca_complete(
            &self,
            id: &IssuerRedemptionRequestId,
        ) {
            self.emit(
                id,
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: id.clone(),
                    alpaca_journal_completed_at: Utc::now(),
                },
            )
            .await;
        }

        async fn burn_tokens(&self, id: &IssuerRedemptionRequestId) {
            self.emit(
                id,
                RedemptionEvent::TokensBurned(TokensBurnedData {
                    issuer_request_id: id.clone(),
                    tx_hash: b256!(
                        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    ),
                    burns: vec![BurnRecord {
                        receipt_id: U256::from(1),
                        shares_burned: U256::from(100),
                    }],
                    dust_returned: U256::ZERO,
                    gas_used: 50000,
                    block_number: 2000,
                    burned_at: Utc::now(),
                }),
            )
            .await;
        }

        async fn record_alpaca_failure(
            &self,
            id: &IssuerRedemptionRequestId,
            error: &str,
        ) {
            self.emit(
                id,
                RedemptionEvent::AlpacaCallFailed {
                    issuer_request_id: id.clone(),
                    error: error.to_string(),
                    failed_at: Utc::now(),
                },
            )
            .await;
        }

        async fn record_burn_failure(
            &self,
            id: &IssuerRedemptionRequestId,
            error: &str,
        ) {
            self.emit(
                id,
                RedemptionEvent::BurningFailed {
                    issuer_request_id: id.clone(),
                    error: error.to_string(),
                    failed_at: Utc::now(),
                    fireblocks_tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await;
        }

        async fn close_redemption(
            &self,
            id: &IssuerRedemptionRequestId,
            reason: &str,
        ) {
            self.emit(
                id,
                RedemptionEvent::RedemptionClosed {
                    issuer_request_id: id.clone(),
                    reason: reason.to_string(),
                    closed_at: Utc::now(),
                },
            )
            .await;
        }
    }

    #[test]
    fn test_view_starts_as_unavailable() {
        let view = RedemptionView::default();
        assert!(matches!(view, RedemptionView::Unavailable));
    }

    #[tokio::test]
    async fn test_view_updates_on_detected_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

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

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
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
            )
            .await
            .unwrap();

        let RedemptionView::Detected {
            issuer_request_id: view_id,
            underlying: view_underlying,
            token: view_token,
            wallet: view_wallet,
            quantity: view_quantity,
            tx_hash: view_tx_hash,
            block_number: view_block_number,
            detected_at: view_detected_at,
            detected_entered_at: view_detected_entered_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Detected view");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
        // On first Detection, detected_entered_at mirrors detected_at — the
        // view has not yet been reprocessed.
        assert_eq!(view_detected_entered_at, detected_at);
    }

    #[tokio::test]
    async fn test_view_updates_on_alpaca_called_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

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

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
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
            )
            .await
            .unwrap();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    alpaca_quantity: quantity.clone(),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at,
                },
            )
            .await
            .unwrap();

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
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected AlpacaCalled view");
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

    #[tokio::test]
    async fn test_view_updates_on_alpaca_journal_completed_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

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

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
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
            )
            .await
            .unwrap();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    alpaca_quantity: quantity.clone(),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at,
                },
            )
            .await
            .unwrap();

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
            burning_entered_at: view_burning_entered_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Burning view");
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
        // On the AlpacaJournalCompleted (happy) path, burning_entered_at
        // mirrors alpaca_journal_completed_at — the view has not yet been
        // resumed.
        assert_eq!(view_burning_entered_at, alpaca_journal_completed_at);
    }

    #[tokio::test]
    async fn test_view_updates_on_redemption_failed_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

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

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                    detected_at,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::RedemptionFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: reason.clone(),
                    failed_at,
                },
            )
            .await
            .unwrap();

        let RedemptionView::Failed {
            issuer_request_id: view_id,
            reason: view_reason,
            failed_at: view_failed_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Failed view");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_reason, reason);
        assert_eq!(view_failed_at, failed_at);
    }

    #[tokio::test]
    async fn test_view_updates_on_alpaca_call_failed_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

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

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::Detected {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                    detected_at,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::AlpacaCallFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error.clone(),
                    failed_at,
                },
            )
            .await
            .unwrap();

        let RedemptionView::Failed {
            issuer_request_id: view_id,
            reason: view_reason,
            failed_at: view_failed_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Failed view");
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

    #[tokio::test]
    async fn test_view_updates_on_burning_failed_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

        let issuer_request_id = IssuerRedemptionRequestId::random();
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

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
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
            )
            .await
            .unwrap();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::AlpacaCalled {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    alpaca_quantity: quantity.clone(),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                    called_at,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::AlpacaJournalCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    alpaca_journal_completed_at,
                },
            )
            .await
            .unwrap();

        assert!(matches!(
            load_view(&pool, &issuer_request_id).await,
            RedemptionView::Burning { .. }
        ));

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::BurningFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error.clone(),
                    failed_at,
                    fireblocks_tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await
            .unwrap();

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
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected BurnFailed view");
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
        let TestHarness { pool, .. } = &harness;

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
        harness.record_burn_failure(&burn_failed_id, "Insufficient gas").await;

        let result = find_burn_failed(pool).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, burn_failed_id);
        assert!(matches!(result[0].1, RedemptionView::BurnFailed { .. }));
    }

    #[tokio::test]
    async fn test_replay_redemption_view_populates_from_events() {
        let pool = migrated_pool().await;

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
        assert_eq!(initial_count, 0, "View should be empty before rebuild");

        super::rebuild_redemption_view(&pool)
            .await
            .expect("Rebuild should succeed");

        let RedemptionView::AlpacaCalled {
            issuer_request_id: view_id,
            alpaca_quantity: view_alpaca_qty,
            dust_quantity: view_dust_qty,
            ..
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected AlpacaCalled view");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_alpaca_qty, quantity);
        assert_eq!(view_dust_qty, Quantity::new(Decimal::ZERO));
    }

    #[tokio::test]
    async fn test_find_stuck_returns_all_non_terminal_variants() {
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        // Detected — stuck candidate (in-progress)
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

        // AlpacaCalled — stuck candidate (in-progress)
        let alpaca_called_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &alpaca_called_id,
                "GOOG",
                address!("0xcccccccccccccccccccccccccccccccccccccccc"),
                30,
                b256!("0x4444444444444444444444444444444444444444444444444444444444444444"),
                11111,
            )
            .await;
        harness.call_alpaca(&alpaca_called_id, "tok-alp-called").await;

        // Burning — stuck candidate (in-progress)
        let burning_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &burning_id,
                "MSFT",
                address!("0xdddddddddddddddddddddddddddddddddddddddd"),
                40,
                b256!("0x5555555555555555555555555555555555555555555555555555555555555555"),
                22222,
            )
            .await;
        harness.call_alpaca(&burning_id, "tok-burning").await;
        harness.confirm_alpaca_complete(&burning_id).await;

        // Failed (AlpacaCallFailed) — stuck (terminal fail)
        let failed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &failed_id,
                "TSLA",
                address!("0x9876543210fedcba9876543210fedcba98765432"),
                50,
                b256!("0x1111111111111111111111111111111111111111111111111111111111111111"),
                54321,
            )
            .await;
        harness.record_alpaca_failure(&failed_id, "Alpaca bug").await;

        // BurnFailed — stuck
        let burn_failed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &burn_failed_id,
                "NVDA",
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                25,
                b256!("0x2222222222222222222222222222222222222222222222222222222222222222"),
                77777,
            )
            .await;
        harness.call_alpaca(&burn_failed_id, "tok-burn-fail").await;
        harness.confirm_alpaca_complete(&burn_failed_id).await;
        harness.record_burn_failure(&burn_failed_id, "Insufficient gas").await;

        // Completed — not stuck
        let completed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &completed_id,
                "META",
                address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                75,
                b256!("0x3333333333333333333333333333333333333333333333333333333333333333"),
                88888,
            )
            .await;
        harness.call_alpaca(&completed_id, "tok-complete").await;
        harness.confirm_alpaca_complete(&completed_id).await;
        harness.burn_tokens(&completed_id).await;

        // Closed — not stuck (admin close path). CloseRedemption requires
        // the aggregate to be Failed first.
        let closed_id = IssuerRedemptionRequestId::random();
        harness
            .detect_redemption(
                &closed_id,
                "NFLX",
                address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                10,
                b256!("0x6666666666666666666666666666666666666666666666666666666666666666"),
                33333,
            )
            .await;
        harness.record_alpaca_failure(&closed_id, "alpaca down").await;
        harness.close_redemption(&closed_id, "closed by admin").await;

        let result = find_stuck(pool).await.unwrap();

        let ids: Vec<_> = result.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(
            result.len(),
            5,
            "Expected 5 non-terminal redemptions, got ids: {ids:?}"
        );
        assert!(ids.contains(&detected_id));
        assert!(ids.contains(&alpaca_called_id));
        assert!(ids.contains(&burning_id));
        assert!(ids.contains(&failed_id));
        assert!(ids.contains(&burn_failed_id));
        assert!(!ids.contains(&completed_id), "Completed must be filtered out");
        assert!(!ids.contains(&closed_id), "Closed must be filtered out");
    }

    #[tokio::test]
    async fn test_find_stuck_returns_empty_when_only_completed() {
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
        harness.call_alpaca(&completed_id, "tok-only-completed").await;
        harness.confirm_alpaca_complete(&completed_id).await;
        harness.burn_tokens(&completed_id).await;

        let result = find_stuck(pool).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_view_updates_on_reprocessed_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("RKLB");
        let token = TokenSymbol::new("tRKLB");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let block_number = 12345_u64;
        let detected_at = Utc::now();

        // Start in Detected
        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
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
            )
            .await
            .unwrap();

        // Fail
        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::AlpacaCallFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    error: "Alpaca bug".to_string(),
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        assert!(matches!(
            load_view(&pool, &issuer_request_id).await,
            RedemptionView::Failed { .. }
        ));

        // Reprocess back to Detected. Use a reprocessed_at distinct from
        // detected_at so the projection's distinction is observable.
        let reprocessed_at = detected_at + chrono::Duration::days(2);
        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::Reprocessed {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                    detected_at,
                    previous_state: "Failed".to_string(),
                    reprocessed_at,
                },
            )
            .await
            .unwrap();

        let RedemptionView::Detected {
            issuer_request_id: view_id,
            underlying: view_underlying,
            detected_at: view_detected_at,
            detected_entered_at: view_detected_entered_at,
            ..
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Detected view after reprocess");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_underlying, underlying);
        // Reprocessed preserves the original detected_at for audit, but
        // updates detected_entered_at to reprocessed_at so the staleness
        // gate (and admin triage timestamp) reflects the most recent entry.
        assert_eq!(view_detected_at, detected_at);
        assert_eq!(view_detected_entered_at, reprocessed_at);
    }

    #[tokio::test]
    async fn test_view_updates_on_burn_resumed_event() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tokenization_request_id =
            TokenizationRequestId::new("tok-resumed-789");
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let quantity = Quantity::new(Decimal::from(42));
        let alpaca_quantity = Quantity::new(Decimal::from(42));
        let dust_quantity = Quantity::new(Decimal::ZERO);
        let tx_hash = b256!(
            "0x4444444444444444444444444444444444444444444444444444444444444444"
        );
        let block_number = 55555;
        let detected_at = Utc::now();
        let called_at = Utc::now();
        let alpaca_journal_completed_at = Utc::now();

        let resumed_at = Utc::now();
        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::BurnResumed {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token: token.clone(),
                    wallet,
                    quantity: quantity.clone(),
                    tx_hash,
                    block_number,
                    detected_at,
                    tokenization_request_id: tokenization_request_id.clone(),
                    alpaca_quantity: alpaca_quantity.clone(),
                    dust_quantity: dust_quantity.clone(),
                    called_at,
                    alpaca_journal_completed_at,
                    external_tx_id: None,
                    resumed_at,
                },
            )
            .await
            .unwrap();

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
            alpaca_journal_completed_at: view_journal_completed_at,
            burning_entered_at: view_burning_entered_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Burning view after BurnResumed");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tok_id, tokenization_request_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_alpaca_quantity, alpaca_quantity);
        assert_eq!(view_dust_quantity, dust_quantity);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_detected_at, detected_at);
        assert_eq!(view_called_at, called_at);
        assert_eq!(view_journal_completed_at, alpaca_journal_completed_at);
        // BurnResumed re-enters Burning, so burning_entered_at tracks
        // resumed_at — NOT the (older) alpaca_journal_completed_at.
        assert_eq!(view_burning_entered_at, resumed_at);
    }

    #[tokio::test]
    async fn test_existing_burn_recovered_projects_to_completed() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tx_hash = b256!(
            "0x5555555555555555555555555555555555555555555555555555555555555555"
        );
        let block_number = 77777;
        let recovered_at = Utc::now();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::ExistingBurnRecovered {
                    issuer_request_id: issuer_request_id.clone(),
                    fireblocks_tx_id: "fb-recovered-123".to_string(),
                    tx_hash,
                    burns: vec![],
                    block_number,
                    recovered_at,
                },
            )
            .await
            .unwrap();

        let RedemptionView::Completed {
            issuer_request_id: view_id,
            burn_tx_hash: view_tx_hash,
            block_number: view_block_number,
            completed_at: view_completed_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Completed view after ExistingBurnRecovered");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_completed_at, recovered_at);
    }

    #[tokio::test]
    async fn test_redemption_closed_projects_to_closed() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let reason = "Legacy entry — tokens already consumed".to_string();
        let closed_at = Utc::now();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::RedemptionClosed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: reason.clone(),
                    closed_at,
                },
            )
            .await
            .unwrap();

        let RedemptionView::Closed {
            issuer_request_id: view_id,
            reason: view_reason,
            closed_at: view_closed_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Closed view after RedemptionClosed");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_reason, reason);
        assert_eq!(view_closed_at, closed_at);
    }

    #[tokio::test]
    async fn test_burn_force_completed_projects_to_completed() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(RedemptionViewReactor::new(pool.clone()));

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0x6666666666666666666666666666666666666666666666666666666666666666"
        );
        let block_number = 88888;
        let completed_at = Utc::now();

        harness
            .receive::<Redemption>(
                issuer_request_id.clone(),
                RedemptionEvent::BurnForceCompleted {
                    issuer_request_id: issuer_request_id.clone(),
                    burn_tx_hash,
                    block_number,
                    reason: "admin recovery".to_string(),
                    completed_at,
                },
            )
            .await
            .unwrap();

        let RedemptionView::Completed {
            issuer_request_id: view_id,
            burn_tx_hash: view_tx_hash,
            block_number: view_block_number,
            completed_at: view_completed_at,
        } = load_view(&pool, &issuer_request_id).await
        else {
            panic!("Expected Completed view after BurnForceCompleted");
        };

        assert_eq!(view_id, issuer_request_id);
        assert_eq!(view_tx_hash, burn_tx_hash);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_completed_at, completed_at);
    }
}
