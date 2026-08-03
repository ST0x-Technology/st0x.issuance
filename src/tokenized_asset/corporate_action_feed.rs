//! Durable projection of Alpaca corporate-action stream mutations.

use chrono::{NaiveDate, Utc};
use futures::StreamExt;
use serde::Deserialize;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use super::schedule::{
    CorporateActionFreezeScheduler, CorporateActionScheduleError,
    CorporateActionScheduleState,
};
use super::view::{TokenizedAssetViewError, underlying_has_listing};
use super::{CorporateActionEventId, CorporateActionId, UnderlyingSymbol};
use crate::alpaca::AlpacaConfig;
use crate::notifications::{LifecycleNotification, LifecycleNotifier};

const INITIAL_STREAM_REPLAY_SINCE: &str = "1970-01-01T00:00:00Z";
const STREAM_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CorporateActionMutationKind {
    Insert,
    Update,
    Delete,
}

impl CorporateActionMutationKind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "insert" => Some(Self::Insert),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Deserialize)]
struct CorporateActionEnvelope {
    event_id: Option<String>,
    action: Option<String>,
    event_type: DividendCorporateActionEventType,
    region: CorporateActionRegion,
    ca: DividendCorporateActionPayload,
}

#[derive(Debug, Deserialize)]
enum DividendCorporateActionEventType {
    #[serde(rename = "cash_dividend_corporateaction_event")]
    CashDividend,
    #[serde(rename = "stock_dividend_corporateaction_event")]
    StockDividend,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CorporateActionRegion {
    Us,
    NonUs,
}

#[derive(Debug, Deserialize)]
struct DividendCorporateActionPayload {
    id: String,
    symbol: String,
    ex_date: NaiveDate,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionDecodeError {
    #[error("corporate-action SSE frame is missing its event id")]
    MissingEventId,
    #[error("invalid corporate-action event id {0}")]
    InvalidEventId(String),
    #[error("corporate-action SSE frame is missing its mutation event")]
    MissingMutation,
    #[error("unsupported corporate-action mutation {0}")]
    UnsupportedMutation(String),
    #[error(
        "corporate-action SSE event id {sse_event_id} does not match payload event id {payload_event_id}"
    )]
    EventIdMismatch { sse_event_id: String, payload_event_id: String },
    #[error(
        "corporate-action SSE mutation {sse_mutation} does not match payload action {payload_action}"
    )]
    MutationMismatch { sse_mutation: String, payload_action: String },
    #[error("corporate-action SSE frame is missing its data payload")]
    MissingData,
    #[error("invalid corporate-action payload: {0}")]
    InvalidPayload(#[from] serde_json::Error),
    #[error("corporate-action stream returned non-US event")]
    NonUsRegion,
    #[error("invalid corporate-action id {0}")]
    InvalidActionId(String),
    #[error("invalid corporate-action symbol {0}")]
    InvalidUnderlying(String),
}

const MAX_SSE_FRAME_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionStreamDecodeError {
    #[error("corporate-action SSE frame exceeded {MAX_SSE_FRAME_BYTES} bytes")]
    FrameTooLarge,
    #[error("corporate-action SSE frame was not UTF-8")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Event(#[from] CorporateActionDecodeError),
}

#[derive(Default)]
pub(crate) struct CorporateActionSseDecoder {
    buffer: Vec<u8>,
}

impl CorporateActionSseDecoder {
    pub(crate) fn push(
        &mut self,
        chunk: &[u8],
    ) -> Result<Vec<CorporateActionMutation>, CorporateActionStreamDecodeError>
    {
        self.buffer.extend_from_slice(chunk);
        let mut mutations = Vec::new();

        while let Some((frame_end, separator_len)) =
            frame_boundary(&self.buffer)
        {
            if frame_end > MAX_SSE_FRAME_BYTES {
                return Err(CorporateActionStreamDecodeError::FrameTooLarge);
            }
            let frame = self.buffer[..frame_end].to_vec();
            self.buffer.drain(..frame_end + separator_len);
            let frame = std::str::from_utf8(&frame)?;
            if frame
                .lines()
                .all(|line| line.is_empty() || line.starts_with(':'))
            {
                continue;
            }
            mutations.push(decode_sse_frame(frame)?);
        }

        if self.buffer.len() > MAX_SSE_FRAME_BYTES {
            return Err(CorporateActionStreamDecodeError::FrameTooLarge);
        }

        Ok(mutations)
    }
}

pub(crate) struct CorporateActionFeed {
    client: reqwest::Client,
    endpoint: String,
    api_key: String,
    api_secret: String,
    pool: Pool<Sqlite>,
    scheduler: CorporateActionFreezeScheduler,
    notifier: Arc<dyn LifecycleNotifier>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionFeedError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("corporate-action stream returned HTTP {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("corporate-action stream returned content type {0}")]
    InvalidContentType(String),
    #[error(transparent)]
    Decode(#[from] CorporateActionStreamDecodeError),
    #[error(transparent)]
    Projection(#[from] CorporateActionProjectionError),
    #[error(transparent)]
    Reconciliation(#[from] CorporateActionReconciliationError),
}

impl CorporateActionFeedError {
    const fn kind(&self) -> &'static str {
        match self {
            Self::Http(_) => "transport",
            Self::HttpStatus(_) => "http_status",
            Self::InvalidContentType(_) => "content_type",
            Self::Decode(_) => "decode",
            Self::Projection(_) => "projection",
            Self::Reconciliation(_) => "reconciliation",
        }
    }
}

impl CorporateActionFeed {
    pub(crate) fn new(
        config: &AlpacaConfig,
        pool: Pool<Sqlite>,
        apalis_pool: &apalis_sqlite::SqlitePool,
        notifier: Arc<dyn LifecycleNotifier>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(
                    config.connect_timeout_secs,
                ))
                .build()?,
            endpoint: config.corporate_actions_stream_url.clone(),
            api_key: config.api_key.clone(),
            api_secret: config.api_secret.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                apalis_pool,
                pool.clone(),
            ),
            pool,
            notifier,
        })
    }

    pub(crate) async fn run(mut self) -> Result<(), CorporateActionFeedError> {
        reconcile_pending_schedules(&self.pool, &mut self.scheduler).await?;

        loop {
            let cursor = load_cursor(&self.pool).await?;
            info!(
                target: "asset",
                state = "connecting",
                cursor = cursor.as_ref().map(CorporateActionEventId::as_str),
                "Connecting to Alpaca corporate-action stream"
            );

            match self.consume_connection(cursor.as_ref()).await {
                Ok(()) => {
                    debug!(
                        target: "asset",
                        state = "disconnected",
                        backoff_secs = STREAM_RECONNECT_BACKOFF.as_secs(),
                        "Alpaca corporate-action stream ended; reconnecting"
                    );
                }
                Err(
                    error @ (CorporateActionFeedError::Decode(_)
                    | CorporateActionFeedError::Projection(_)
                    | CorporateActionFeedError::Reconciliation(_)
                    | CorporateActionFeedError::InvalidContentType(_)),
                ) => {
                    self.notifier
                        .notify(
                            &LifecycleNotification::CorporateActionsSyncFailed,
                        )
                        .await;
                    return Err(error);
                }
                Err(CorporateActionFeedError::HttpStatus(status))
                    if status.is_client_error()
                        && status != reqwest::StatusCode::TOO_MANY_REQUESTS =>
                {
                    self.notifier
                        .notify(
                            &LifecycleNotification::CorporateActionsSyncFailed,
                        )
                        .await;
                    return Err(CorporateActionFeedError::HttpStatus(status));
                }
                Err(error) => {
                    debug!(
                        target: "asset",
                        state = "disconnected",
                        error = %error,
                        backoff_secs = STREAM_RECONNECT_BACKOFF.as_secs(),
                        "Alpaca corporate-action stream disconnected; reconnecting"
                    );
                }
            }

            tokio::time::sleep(STREAM_RECONNECT_BACKOFF).await;
        }
    }

    async fn consume_connection(
        &mut self,
        cursor: Option<&CorporateActionEventId>,
    ) -> Result<(), CorporateActionFeedError> {
        let mut request = self
            .client
            .get(&self.endpoint)
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret);
        if let Some(cursor) = cursor {
            request = request.header("Last-Event-Id", cursor.as_str());
        } else {
            request = request.query(&[("since", INITIAL_STREAM_REPLAY_SINCE)]);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(CorporateActionFeedError::HttpStatus(
                response.status(),
            ));
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !content_type.starts_with("text/event-stream") {
            return Err(CorporateActionFeedError::InvalidContentType(
                content_type.to_string(),
            ));
        }
        info!(
            target: "asset",
            state = "connected",
            "Connected to Alpaca corporate-action stream"
        );

        let mut chunks = response.bytes_stream();
        let mut decoder = CorporateActionSseDecoder::default();
        while let Some(chunk) = chunks.next().await {
            for mutation in decoder.push(&chunk?)? {
                let event_id = mutation.event_id.clone();
                let action_id = mutation.action.id.clone();
                let mutation_kind = mutation.kind;
                let outcome = apply_mutation(&self.pool, &mutation).await?;
                reconcile_pending_schedules(&self.pool, &mut self.scheduler)
                    .await?;
                info!(
                    target: "asset",
                    event_id = %event_id,
                    action_id = %action_id,
                    mutation = mutation_kind.as_str(),
                    outcome = ?outcome,
                    "Applied Alpaca corporate-action mutation"
                );
            }
        }

        Ok(())
    }
}

pub(crate) fn spawn_corporate_action_feed(feed: CorporateActionFeed) {
    tokio::spawn(async move {
        if let Err(error) = feed.run().await {
            error!(
                target: "asset",
                state = "poisoned",
                failure_kind = error.kind(),
                error = %error,
                "Alpaca corporate-action stream stopped on poison input; terminating service to fail closed"
            );
            std::process::exit(1);
        }
    });
}

fn frame_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf < crlf => Some((lf, 2)),
        (Some(_) | None, Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, None) => None,
    }
}

pub(crate) fn decode_sse_frame(
    frame: &str,
) -> Result<CorporateActionMutation, CorporateActionDecodeError> {
    let mut event_id = None;
    let mut mutation = None;
    let mut data = Vec::new();

    for line in frame.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" => event_id = Some(value.to_string()),
            "event" => mutation = Some(value.to_string()),
            "data" => data.push(value),
            _ => {}
        }
    }

    if data.is_empty() {
        return Err(CorporateActionDecodeError::MissingData);
    }
    let envelope: CorporateActionEnvelope =
        serde_json::from_str(&data.join("\n"))?;
    let event_id = match (event_id, envelope.event_id) {
        (Some(sse_event_id), Some(payload_event_id))
            if sse_event_id != payload_event_id =>
        {
            return Err(CorporateActionDecodeError::EventIdMismatch {
                sse_event_id,
                payload_event_id,
            });
        }
        (Some(sse_event_id), _) => sse_event_id,
        (None, Some(payload_event_id)) => payload_event_id,
        (None, None) => {
            return Err(CorporateActionDecodeError::MissingEventId);
        }
    };
    let event_id = CorporateActionEventId::new(&event_id).ok_or_else(|| {
        CorporateActionDecodeError::InvalidEventId(event_id.clone())
    })?;
    let mutation = match (mutation, envelope.action) {
        (Some(sse_mutation), Some(payload_action))
            if sse_mutation != payload_action =>
        {
            return Err(CorporateActionDecodeError::MutationMismatch {
                sse_mutation,
                payload_action,
            });
        }
        (Some(sse_mutation), _) => sse_mutation,
        (None, Some(payload_action)) => payload_action,
        (None, None) => {
            return Err(CorporateActionDecodeError::MissingMutation);
        }
    };
    let kind =
        CorporateActionMutationKind::parse(&mutation).ok_or_else(|| {
            CorporateActionDecodeError::UnsupportedMutation(mutation.clone())
        })?;
    match envelope.event_type {
        DividendCorporateActionEventType::CashDividend
        | DividendCorporateActionEventType::StockDividend => {}
    }
    if matches!(envelope.region, CorporateActionRegion::NonUs) {
        return Err(CorporateActionDecodeError::NonUsRegion);
    }
    let action_id =
        CorporateActionId::new(&envelope.ca.id).ok_or_else(|| {
            CorporateActionDecodeError::InvalidActionId(envelope.ca.id.clone())
        })?;
    let underlying =
        UnderlyingSymbol::new(&envelope.ca.symbol).map_err(|_| {
            CorporateActionDecodeError::InvalidUnderlying(
                envelope.ca.symbol.clone(),
            )
        })?;

    Ok(CorporateActionMutation {
        event_id,
        kind,
        action: DividendCorporateAction {
            id: action_id,
            underlying,
            ex_date: envelope.ca.ex_date,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DividendCorporateAction {
    pub(crate) id: CorporateActionId,
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) ex_date: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CorporateActionMutation {
    pub(crate) event_id: CorporateActionEventId,
    pub(crate) kind: CorporateActionMutationKind,
    pub(crate) action: DividendCorporateAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyMutationOutcome {
    Applied,
    Duplicate,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionProjectionError {
    #[error("corporate-action event cursor regressed from {current} to {next}")]
    CursorRegression {
        current: CorporateActionEventId,
        next: CorporateActionEventId,
    },
    #[error("stored corporate-action event cursor is invalid: {0}")]
    InvalidStoredCursor(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionReconciliationError {
    #[error("invalid projected corporate-action id {0}")]
    InvalidActionId(String),
    #[error("invalid projected corporate-action event id {0}")]
    InvalidEventId(String),
    #[error("invalid projected corporate-action underlying {0}")]
    InvalidUnderlying(String),
    #[error("invalid projected corporate-action ex-date {0}")]
    InvalidExDate(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Schedule(#[from] CorporateActionScheduleError),
    #[error(transparent)]
    View(#[from] TokenizedAssetViewError),
}

pub(crate) async fn reconcile_pending_schedules(
    pool: &Pool<Sqlite>,
    scheduler: &mut CorporateActionFreezeScheduler,
) -> Result<(), CorporateActionReconciliationError> {
    let pending: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        "
        SELECT action_id, event_id, underlying, ex_date, deleted
        FROM corporate_action_schedule
        WHERE reconciled_event_id IS NULL OR reconciled_event_id != event_id
        ORDER BY event_id
        ",
    )
    .fetch_all(pool)
    .await?;

    for (action_id, event_id, underlying, ex_date, deleted) in pending {
        let action_id =
            CorporateActionId::new(&action_id).ok_or_else(|| {
                CorporateActionReconciliationError::InvalidActionId(
                    action_id.clone(),
                )
            })?;
        let event_id =
            CorporateActionEventId::new(&event_id).ok_or_else(|| {
                CorporateActionReconciliationError::InvalidEventId(
                    event_id.clone(),
                )
            })?;
        let underlying = UnderlyingSymbol::new(&underlying).map_err(|_| {
            CorporateActionReconciliationError::InvalidUnderlying(
                underlying.clone(),
            )
        })?;
        let ex_date =
            NaiveDate::parse_from_str(&ex_date, "%Y-%m-%d").map_err(|_| {
                CorporateActionReconciliationError::InvalidExDate(
                    ex_date.clone(),
                )
            })?;

        let state = if deleted != 0 {
            CorporateActionScheduleState::Deleted
        } else if underlying_has_listing(pool, &underlying).await? {
            CorporateActionScheduleState::Active
        } else {
            info!(
                target: "asset",
                event_id = %event_id,
                action_id = %action_id,
                underlying = %underlying,
                "Aligning corporate action for an unlisted underlying as release-only"
            );
            CorporateActionScheduleState::Deleted
        };

        scheduler
            .schedule_revision(
                &action_id,
                &event_id,
                &underlying,
                ex_date,
                state,
                Utc::now(),
            )
            .await?;

        mark_reconciled(pool, &action_id, &event_id).await?;
    }

    Ok(())
}

async fn mark_reconciled(
    pool: &Pool<Sqlite>,
    action_id: &CorporateActionId,
    event_id: &CorporateActionEventId,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE corporate_action_schedule
        SET reconciled_event_id = event_id
        WHERE action_id = ? AND event_id = ?
        ",
    )
    .bind(action_id.as_str())
    .bind(event_id.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn apply_mutation(
    pool: &Pool<Sqlite>,
    mutation: &CorporateActionMutation,
) -> Result<ApplyMutationOutcome, CorporateActionProjectionError> {
    let mut transaction = pool.begin().await?;

    let duplicate: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM corporate_action_mutations WHERE event_id = ?)",
    )
    .bind(mutation.event_id.as_str())
    .fetch_one(&mut *transaction)
    .await?;
    if duplicate {
        transaction.commit().await?;
        return Ok(ApplyMutationOutcome::Duplicate);
    }

    let current = sqlx::query_scalar::<_, String>(
        "SELECT event_id FROM corporate_action_cursor WHERE singleton = 1",
    )
    .fetch_optional(&mut *transaction)
    .await?
    .map(parse_stored_cursor)
    .transpose()?;

    if let Some(current) = current
        && mutation.event_id < current
    {
        return Err(CorporateActionProjectionError::CursorRegression {
            current,
            next: mutation.event_id.clone(),
        });
    }

    let deleted =
        i64::from(matches!(mutation.kind, CorporateActionMutationKind::Delete));
    let ex_date = mutation.action.ex_date.to_string();

    sqlx::query(
        "
        INSERT INTO corporate_action_mutations (
            event_id,
            action_id,
            mutation,
            underlying,
            ex_date
        )
        VALUES (?, ?, ?, ?, ?)
        ",
    )
    .bind(mutation.event_id.as_str())
    .bind(mutation.action.id.as_str())
    .bind(mutation.kind.as_str())
    .bind(mutation.action.underlying.as_str())
    .bind(&ex_date)
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        "
        INSERT INTO corporate_action_schedule (
            action_id,
            event_id,
            underlying,
            ex_date,
            deleted,
            reconciled_event_id,
            revision
        )
        VALUES (?, ?, ?, ?, ?, NULL, 1)
        ON CONFLICT(action_id) DO UPDATE SET
            event_id = excluded.event_id,
            underlying = excluded.underlying,
            ex_date = excluded.ex_date,
            deleted = excluded.deleted,
            reconciled_event_id = NULL,
            revision = corporate_action_schedule.revision + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        ",
    )
    .bind(mutation.action.id.as_str())
    .bind(mutation.event_id.as_str())
    .bind(mutation.action.underlying.as_str())
    .bind(&ex_date)
    .bind(deleted)
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        "
        INSERT INTO corporate_action_cursor (singleton, event_id)
        VALUES (1, ?)
        ON CONFLICT(singleton) DO UPDATE SET
            event_id = excluded.event_id,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        ",
    )
    .bind(mutation.event_id.as_str())
    .execute(&mut *transaction)
    .await?;

    transaction.commit().await?;
    Ok(ApplyMutationOutcome::Applied)
}

pub(crate) async fn load_cursor(
    pool: &Pool<Sqlite>,
) -> Result<Option<CorporateActionEventId>, CorporateActionProjectionError> {
    let value: Option<String> = sqlx::query_scalar(
        "SELECT event_id FROM corporate_action_cursor WHERE singleton = 1",
    )
    .fetch_optional(pool)
    .await?;

    value.map(parse_stored_cursor).transpose()
}

fn parse_stored_cursor(
    value: String,
) -> Result<CorporateActionEventId, CorporateActionProjectionError> {
    CorporateActionEventId::new(&value)
        .ok_or(CorporateActionProjectionError::InvalidStoredCursor(value))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use httpmock::prelude::*;
    use std::sync::Arc;

    use super::*;
    use crate::mint::test_utils::TestHarness;
    use crate::notifications::NoopLifecycleNotifier;

    fn event(
        event_id: &str,
        kind: CorporateActionMutationKind,
        action_id: &str,
        ex_date: &str,
    ) -> CorporateActionMutation {
        CorporateActionMutation {
            event_id: CorporateActionEventId::new(event_id).unwrap(),
            kind,
            action: DividendCorporateAction {
                id: CorporateActionId::new(action_id).unwrap(),
                underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                ex_date: NaiveDate::parse_from_str(ex_date, "%Y-%m-%d")
                    .unwrap(),
            },
        }
    }

    #[test]
    fn decodes_the_documented_cash_dividend_insert_envelope() {
        let mutation = decode_sse_frame(
            "data: {\"action\":\"insert\",\"at\":\"2026-03-20T12:24:58.807230Z\",\"ca\":{\"currency\":\"USD\",\"cusip\":\"037833100\",\"ex_date\":\"2026-08-14\",\"foreign\":false,\"id\":\"ca-1\",\"payable_date\":\"2026-08-20\",\"process_date\":\"2026-08-20\",\"rate\":\"0.25\",\"record_date\":\"2026-08-15\",\"special\":false,\"symbol\":\"AAPL\"},\"event_id\":\"01J9RPMV5TKB8WX3M4F1KZ7QH2\",\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\"}",
        )
        .unwrap();

        assert_eq!(mutation.kind, CorporateActionMutationKind::Insert);
        assert_eq!(mutation.event_id.as_str(), "01J9RPMV5TKB8WX3M4F1KZ7QH2");
        assert_eq!(mutation.action.id.as_str(), "ca-1");
        assert_eq!(mutation.action.underlying.as_str(), "AAPL");
        assert_eq!(mutation.action.ex_date.to_string(), "2026-08-14");
    }

    #[test]
    fn decodes_a_documented_dividend_delete_payload() {
        let mutation = decode_sse_frame(
            "data: {\"action\":\"delete\",\"at\":\"2026-03-20T12:24:58.807230Z\",\"ca\":{\"currency\":\"USD\",\"cusip\":\"037833100\",\"ex_date\":\"2026-08-14\",\"foreign\":false,\"id\":\"ca-1\",\"process_date\":\"2026-08-20\",\"rate\":\"0.25\",\"special\":false,\"symbol\":\"AAPL\"},\"event_id\":\"01J9RPMV5TKB8WX3M4F1KZ7QH2\",\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\"}",
        )
        .unwrap();

        assert_eq!(mutation.kind, CorporateActionMutationKind::Delete);
        assert_eq!(mutation.action.id.as_str(), "ca-1");
        assert_eq!(mutation.action.underlying.as_str(), "AAPL");
        assert_eq!(mutation.action.ex_date.to_string(), "2026-08-14");
    }

    #[test]
    fn rejects_mismatched_sse_and_payload_identity() {
        let error = decode_sse_frame(
            "id: 01J9RPMV5TKB8WX3M4F1KZ7QH2\nevent: insert\ndata: {\"action\":\"update\",\"ca\":{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"},\"event_id\":\"01J9RPMV5TKB8WX3M4F1KZ7QH2\",\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\"}",
        )
        .unwrap_err();

        assert!(matches!(
            error,
            CorporateActionDecodeError::MutationMismatch { .. }
        ));
    }

    #[test]
    fn rejects_an_undocumented_mutation() {
        let error = decode_sse_frame(
            "id: 01J9RPMV5TKB8WX3M4F1KZ7QH2\nevent: revise\ndata: {\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}",
        )
        .unwrap_err();

        assert!(matches!(
            error,
            CorporateActionDecodeError::UnsupportedMutation(_)
        ));
    }

    #[test]
    fn buffers_fragmented_crlf_frames() {
        let mut decoder = CorporateActionSseDecoder::default();
        assert!(
            decoder
                .push(
                    b"id: 01J9RPMV5TKB8WX3M4F1KZ7QH2\r\nevent: insert\r\ndata: {\"event_type\":\"cash_dividend_corporateaction_event\","
                )
                .unwrap()
                .is_empty()
        );

        let mutations = decoder
            .push(
                b"\"region\":\"us\",\"ca\":{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}\r\n\r\n",
            )
            .unwrap();

        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].action.id.as_str(), "ca-1");
    }

    #[test]
    fn rejects_an_oversized_partial_frame() {
        let mut decoder = CorporateActionSseDecoder::default();
        let error =
            decoder.push(&vec![b'x'; MAX_SSE_FRAME_BYTES + 1]).unwrap_err();

        assert!(matches!(
            error,
            CorporateActionStreamDecodeError::FrameTooLarge
        ));
    }

    #[tokio::test]
    async fn initial_connection_requests_historical_replay() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .query_param("since", INITIAL_STREAM_REPLAY_SINCE)
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(": connected\n\n");
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.consume_connection(None).await.unwrap();

        stream.assert();
    }

    #[tokio::test]
    async fn duplicate_replay_is_a_noop() {
        let harness = TestHarness::new().await;
        let mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );

        assert_eq!(
            apply_mutation(&harness.pool, &mutation).await.unwrap(),
            ApplyMutationOutcome::Applied
        );
        assert_eq!(
            apply_mutation(&harness.pool, &mutation).await.unwrap(),
            ApplyMutationOutcome::Duplicate
        );

        let revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM corporate_action_schedule WHERE action_id = ?",
        )
        .bind(mutation.action.id.as_str())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(revision, 1);
    }

    #[tokio::test]
    async fn update_replaces_the_actions_desired_window() {
        let harness = TestHarness::new().await;
        let insert = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        let update = event(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Update,
            "ca-1",
            "2026-08-21",
        );

        apply_mutation(&harness.pool, &insert).await.unwrap();
        apply_mutation(&harness.pool, &update).await.unwrap();

        let (event_id, ex_date, deleted, revision):
            (String, String, i64, i64) = sqlx::query_as(
                "SELECT event_id, ex_date, deleted, revision FROM corporate_action_schedule WHERE action_id = ?",
            )
            .bind(update.action.id.as_str())
            .fetch_one(&harness.pool)
            .await
            .unwrap();
        assert_eq!(event_id, update.event_id.as_str());
        assert_eq!(ex_date, "2026-08-21");
        assert_eq!(deleted, 0);
        assert_eq!(revision, 2);
    }

    #[tokio::test]
    async fn cursor_regression_does_not_persist_the_event() {
        let harness = TestHarness::new().await;
        let newer = event(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        let older = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Update,
            "ca-1",
            "2026-08-21",
        );

        apply_mutation(&harness.pool, &newer).await.unwrap();
        let error = apply_mutation(&harness.pool, &older).await.unwrap_err();

        assert!(matches!(
            error,
            CorporateActionProjectionError::CursorRegression { .. }
        ));
        let persisted: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_mutations WHERE event_id = ?",
        )
        .bind(older.event_id.as_str())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(persisted, 0);
    }

    #[tokio::test]
    async fn reconciliation_enqueues_and_marks_the_projected_revision() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mutation = CorporateActionMutation {
            event_id: CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2")
                .unwrap(),
            kind: CorporateActionMutationKind::Insert,
            action: DividendCorporateAction {
                id: CorporateActionId::new("ca-1").unwrap(),
                underlying,
                ex_date: Utc::now().date_naive(),
            },
        };
        apply_mutation(&harness.pool, &mutation).await.unwrap();
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );

        reconcile_pending_schedules(&harness.pool, &mut scheduler)
            .await
            .unwrap();

        let reconciled_event_id: Option<String> = sqlx::query_scalar(
            "SELECT reconciled_event_id FROM corporate_action_schedule WHERE action_id = ?",
        )
        .bind(mutation.action.id.as_str())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            reconciled_event_id.as_deref(),
            Some(mutation.event_id.as_str())
        );
    }

    #[tokio::test]
    async fn reconciliation_aligns_unlisted_underlyings_release_only() {
        let harness = TestHarness::new().await;
        let mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-unsupported",
            "2026-08-14",
        );
        let mutation = CorporateActionMutation {
            action: DividendCorporateAction {
                underlying: UnderlyingSymbol::new("MSFT").unwrap(),
                ..mutation.action
            },
            ..mutation
        };
        apply_mutation(&harness.pool, &mutation).await.unwrap();
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );

        reconcile_pending_schedules(&harness.pool, &mut scheduler)
            .await
            .unwrap();

        let reconciled_event_id: Option<String> = sqlx::query_scalar(
            "SELECT reconciled_event_id FROM corporate_action_schedule WHERE action_id = ?",
        )
        .bind(mutation.action.id.as_str())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(
            reconciled_event_id.as_deref(),
            Some(mutation.event_id.as_str())
        );
        let jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ?",
        )
        .bind(crate::jobs::job_type::<
            crate::tokenized_asset::schedule::AlignCorporateActionFreeze,
        >())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(jobs, 1);
    }

    #[tokio::test]
    async fn invalid_stored_cursor_fails_closed() {
        let harness = TestHarness::new().await;
        sqlx::query(
            "INSERT INTO corporate_action_cursor (singleton, event_id) VALUES (1, 'not-a-ulid')",
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        let error = load_cursor(&harness.pool).await.unwrap_err();

        assert!(matches!(
            error,
            CorporateActionProjectionError::InvalidStoredCursor(_)
        ));
    }
}
