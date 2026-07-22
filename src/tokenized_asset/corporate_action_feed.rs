//! Durable projection of Alpaca corporate-action stream mutations.

use backon::{BackoffBuilder, ExponentialBuilder};
use chrono::{NaiveDate, Utc};
use futures::StreamExt;
use serde::Deserialize;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use url::{Host, Url};

use super::schedule::{
    CorporateActionFreezeScheduler, CorporateActionScheduleError,
    CorporateActionScheduleState, acquire_corporate_action_revision_guard,
};
use super::view::{TokenizedAssetViewError, underlying_has_listing};
use super::{CorporateActionEventId, CorporateActionId, UnderlyingSymbol};
use crate::alpaca::AlpacaConfig;
use crate::config::Environment;
use crate::notifications::{LifecycleNotification, LifecycleNotifier};

const BLOCKED_REASON_CURSOR_REGRESSION: &str = "cursor_regression";
const BLOCKED_REASON_POISON: &str = "poison";
const BLOCKED_REASON_REPLAY_GAP: &str = "replay_gap";
const STREAM_RECONNECT_MIN_BACKOFF: Duration = Duration::from_secs(5);
const STREAM_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(60);
const STREAM_RECONNECT_ALERT_THRESHOLD: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionProgress {
    Idle,
    AcceptedMutation,
}

struct ConnectionConsumption {
    progress: ConnectionProgress,
    result: Result<(), CorporateActionFeedError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconnectEscalation {
    BelowThreshold,
    AlertOperator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReconnectAttempt {
    consecutive_failures: usize,
    escalation: ReconnectEscalation,
}

#[derive(Debug, Default)]
struct ReconnectFailures {
    consecutive: usize,
}

impl ReconnectFailures {
    fn record(&mut self, progress: ConnectionProgress) -> ReconnectAttempt {
        if progress == ConnectionProgress::AcceptedMutation {
            self.consecutive = 0;
        }
        self.consecutive = self.consecutive.saturating_add(1);
        ReconnectAttempt {
            consecutive_failures: self.consecutive,
            escalation: if self.consecutive == STREAM_RECONNECT_ALERT_THRESHOLD
            {
                ReconnectEscalation::AlertOperator
            } else {
                ReconnectEscalation::BelowThreshold
            },
        }
    }
}

fn reconnect_backoff_builder() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(STREAM_RECONNECT_MIN_BACKOFF)
        .with_max_delay(STREAM_RECONNECT_MAX_BACKOFF)
        .without_max_times()
        .with_jitter()
}

fn next_reconnect_delay(
    backoff: &mut impl Iterator<Item = Duration>,
) -> Duration {
    backoff
        .next()
        .unwrap_or(STREAM_RECONNECT_MAX_BACKOFF)
        .min(STREAM_RECONNECT_MAX_BACKOFF)
}

async fn alert_on_reconnect_threshold(
    notifier: &dyn LifecycleNotifier,
    attempt: ReconnectAttempt,
    backoff: Duration,
) {
    if attempt.escalation != ReconnectEscalation::AlertOperator {
        return;
    }
    warn!(
        target: "asset",
        state = "reconnect_threshold_exceeded",
        consecutive_failures = attempt.consecutive_failures,
        backoff_secs = backoff.as_secs(),
        "Alpaca corporate-action stream remains disconnected"
    );
    notifier.notify(&LifecycleNotification::CorporateActionsSyncFailed).await;
}

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
struct CorporateActionIdentityEnvelope {
    event_id: Option<CorporateActionEventId>,
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
    EventIdMismatch {
        sse_event_id: CorporateActionEventId,
        payload_event_id: String,
    },
    #[error(
        "corporate-action SSE mutation {sse_mutation} does not match payload action {payload_action}"
    )]
    MutationMismatch { sse_mutation: String, payload_action: String },
    #[error("corporate-action SSE field was not UTF-8")]
    InvalidFieldUtf8(#[source] std::str::Utf8Error),
    #[error("corporate-action SSE frame is missing its data payload")]
    MissingData,
    #[error("invalid corporate-action payload for event {event_id}: {source}")]
    InvalidPayload {
        event_id: CorporateActionEventId,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid corporate-action payload without an event id: {0}")]
    InvalidPayloadWithoutEventId(#[source] serde_json::Error),
    #[error("corporate-action stream returned non-US event")]
    NonUsRegion,
    #[error("invalid corporate-action id {0}")]
    InvalidActionId(String),
    #[error("invalid corporate-action symbol {0}")]
    InvalidUnderlying(String),
}

const MAX_SSE_FRAME_BYTES: usize = 64 * 1024;
const MAX_SSE_SEPARATOR_BYTES: usize = 4;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionStreamDecodeError {
    #[error("corporate-action SSE frame exceeded {MAX_SSE_FRAME_BYTES} bytes")]
    FrameTooLarge,
    #[error("corporate-action SSE frame for event {event_id:?} was not UTF-8")]
    InvalidUtf8 {
        event_id: Option<CorporateActionEventId>,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("{source}")]
    Event {
        event_id: Option<CorporateActionEventId>,
        #[source]
        source: CorporateActionDecodeError,
    },
}

impl CorporateActionStreamDecodeError {
    const fn event_id(&self) -> Option<&CorporateActionEventId> {
        match self {
            Self::InvalidUtf8 { event_id, .. }
            | Self::Event { event_id, .. } => event_id.as_ref(),
            Self::FrameTooLarge => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum CorporateActionDecodeBatch {
    Complete(Vec<CorporateActionMutation>),
    Poison {
        completed: Vec<CorporateActionMutation>,
        error: CorporateActionStreamDecodeError,
    },
}

#[derive(Debug, Clone)]
enum SseEventIdentity {
    Absent,
    Valid(CorporateActionEventId),
    Invalid,
}

#[derive(Default)]
pub(crate) struct CorporateActionSseDecoder {
    buffer: Vec<u8>,
}

impl CorporateActionSseDecoder {
    /// Incrementally decodes bounded SSE frames without retaining poisoned
    /// input. Complete frames preceding a poison boundary are returned so the
    /// caller can commit them before stopping at the rejected event.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> CorporateActionDecodeBatch {
        let mut remaining = chunk;
        let mut mutations = Vec::new();

        while !remaining.is_empty() {
            let buffer_limit = MAX_SSE_FRAME_BYTES + MAX_SSE_SEPARATOR_BYTES;
            let available = buffer_limit.saturating_sub(self.buffer.len());
            if available == 0 {
                self.buffer = Vec::new();
                return CorporateActionDecodeBatch::Poison {
                    completed: mutations,
                    error: CorporateActionStreamDecodeError::FrameTooLarge,
                };
            }
            let accepted = remaining.len().min(available);
            self.buffer.extend_from_slice(&remaining[..accepted]);
            remaining = &remaining[accepted..];

            while let Some((frame_end, separator_len)) =
                frame_boundary(&self.buffer)
            {
                if frame_end > MAX_SSE_FRAME_BYTES {
                    self.buffer = Vec::new();
                    return CorporateActionDecodeBatch::Poison {
                        completed: mutations,
                        error: CorporateActionStreamDecodeError::FrameTooLarge,
                    };
                }
                let frame = self.buffer[..frame_end].to_vec();
                self.buffer.drain(..frame_end + separator_len);
                let event_identity = sse_event_identity(&frame);
                let frame = match std::str::from_utf8(&frame) {
                    Ok(frame) => frame,
                    Err(source) => {
                        self.buffer = Vec::new();
                        let event_id = match event_identity {
                            SseEventIdentity::Valid(event_id) => Some(event_id),
                            SseEventIdentity::Absent
                            | SseEventIdentity::Invalid => None,
                        };
                        return CorporateActionDecodeBatch::Poison {
                            completed: mutations,
                            error:
                                CorporateActionStreamDecodeError::InvalidUtf8 {
                                    event_id,
                                    source,
                                },
                        };
                    }
                };
                if sse_lines(frame.as_bytes())
                    .all(|line| line.is_empty() || line.starts_with(b":"))
                {
                    continue;
                }
                let event_id = match event_identity {
                    SseEventIdentity::Absent => {
                        validated_payload_event_id(frame)
                    }
                    SseEventIdentity::Valid(event_id) => Some(event_id),
                    SseEventIdentity::Invalid => None,
                };
                let mutation = match decode_sse_frame(frame) {
                    Ok(mutation) => mutation,
                    Err(source) => {
                        self.buffer = Vec::new();
                        return CorporateActionDecodeBatch::Poison {
                            completed: mutations,
                            error: CorporateActionStreamDecodeError::Event {
                                event_id,
                                source,
                            },
                        };
                    }
                };
                mutations.push(mutation);
            }

            if !can_still_terminate_within_limit(&self.buffer) {
                self.buffer = Vec::new();
                return CorporateActionDecodeBatch::Poison {
                    completed: mutations,
                    error: CorporateActionStreamDecodeError::FrameTooLarge,
                };
            }
        }

        CorporateActionDecodeBatch::Complete(mutations)
    }
}

pub(crate) struct CorporateActionFeed {
    client: reqwest::Client,
    endpoint: String,
    api_key: String,
    api_secret: String,
    stream_transport: CorporateActionStreamTransport,
    pool: Pool<Sqlite>,
    scheduler: CorporateActionFreezeScheduler,
    notifier: Arc<dyn LifecycleNotifier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorporateActionStreamTransport {
    AuthenticatedAlpaca,
    CredentialFreeDevelopment,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionFeedBuildError {
    #[error("invalid corporate-action stream URL")]
    InvalidEndpoint(#[from] url::ParseError),
    #[error("corporate-action stream URL must use HTTPS, got {0}")]
    InsecureEndpointScheme(String),
    #[error(
        "corporate-action stream URL must target stream.data.alpaca.markets"
    )]
    UnexpectedEndpointHost,
    #[error("failed to build corporate-action HTTP client")]
    Client(#[from] reqwest::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionFeedError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("corporate-action stream returned HTTP {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("corporate-action stream returned content type {0}")]
    InvalidContentType(String),
    #[error(
        "corporate-action projection has no baseline; snapshot repair is required"
    )]
    BaselineRequired,
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
            Self::BaselineRequired => "baseline_required",
            Self::Decode(_) => "decode",
            Self::Projection(_) => "projection",
            Self::Reconciliation(_) => "reconciliation",
        }
    }

    const fn event_id(&self) -> Option<&CorporateActionEventId> {
        match self {
            Self::Decode(error) => error.event_id(),
            Self::Projection(
                CorporateActionProjectionError::CursorRegression {
                    next, ..
                }
                | CorporateActionProjectionError::ReplayGap {
                    observed: next,
                    ..
                }
                | CorporateActionProjectionError::BlockedCursorRegression {
                    event_id: next,
                }
                | CorporateActionProjectionError::BlockedReplayGap {
                    event_id: next,
                },
            ) => Some(next),
            Self::Projection(
                CorporateActionProjectionError::BlockedPoison { event_id },
            ) => event_id.as_ref(),
            _ => None,
        }
    }
}

impl CorporateActionFeed {
    pub(crate) fn new(
        config: &AlpacaConfig,
        environment: Environment,
        pool: Pool<Sqlite>,
        apalis_pool: &apalis_sqlite::SqlitePool,
        notifier: Arc<dyn LifecycleNotifier>,
    ) -> Result<Self, CorporateActionFeedBuildError> {
        let stream_transport = validate_corporate_action_endpoint(
            &config.corporate_actions_stream_url,
            environment,
        )?;
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(
                    config.connect_timeout_secs,
                ))
                .read_timeout(Duration::from_secs(
                    config.corporate_actions_read_timeout_secs,
                ))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: config.corporate_actions_stream_url.clone(),
            api_key: config.api_key.clone(),
            api_secret: config.api_secret.clone(),
            stream_transport,
            scheduler: CorporateActionFreezeScheduler::new(
                apalis_pool,
                pool.clone(),
            ),
            pool,
            notifier,
        })
    }

    /// Lets a credential-free development stream establish its cursor through
    /// the normal decoder and projection path before the development service starts.
    /// Authenticated environments remain fail-closed until snapshot repair has
    /// established their production baseline.
    pub(crate) async fn establish_development_baseline(
        &mut self,
    ) -> Result<(), CorporateActionFeedError> {
        if self.stream_transport
            != CorporateActionStreamTransport::CredentialFreeDevelopment
            || load_cursor(&self.pool).await?.is_some()
        {
            return Ok(());
        }

        self.consume_connection(None).await.result
    }

    /// Reconciles every durably projected revision before connecting, then
    /// retries transport failures with backoff. Contract, projection, and
    /// reconciliation failures stop the feed so the service fails closed.
    pub(crate) async fn run(mut self) -> Result<(), CorporateActionFeedError> {
        reconcile_pending_schedules(&self.pool, &mut self.scheduler).await?;
        let reconnect_builder = reconnect_backoff_builder();
        let mut reconnect_backoff = reconnect_builder.build();
        let mut reconnect_failures = ReconnectFailures::default();

        loop {
            let cursor = load_cursor(&self.pool).await?;
            info!(
                target: "asset",
                state = "connecting",
                cursor = cursor.as_ref().map(CorporateActionEventId::as_str),
                "Connecting to Alpaca corporate-action stream"
            );

            let consumption = self.consume_connection(cursor.as_ref()).await;
            let progress = consumption.progress;
            let disconnect_error = match consumption.result {
                Ok(()) => {
                    debug!(
                        target: "asset",
                        state = "disconnected",
                        "Alpaca corporate-action stream ended; reconnecting"
                    );
                    None
                }
                Err(
                    error @ (CorporateActionFeedError::Decode(_)
                    | CorporateActionFeedError::Projection(_)
                    | CorporateActionFeedError::Reconciliation(_)
                    | CorporateActionFeedError::InvalidContentType(_)
                    | CorporateActionFeedError::BaselineRequired),
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
                        "Alpaca corporate-action stream disconnected; reconnecting"
                    );
                    Some(error)
                }
            };

            if progress == ConnectionProgress::AcceptedMutation {
                reconnect_backoff = reconnect_builder.build();
            }
            let attempt = reconnect_failures.record(progress);
            let backoff = next_reconnect_delay(&mut reconnect_backoff);
            debug!(
                target: "asset",
                state = "reconnecting",
                consecutive_failures = attempt.consecutive_failures,
                backoff_secs = backoff.as_secs(),
                error = disconnect_error.as_ref().map(ToString::to_string),
                "Backing off before reconnecting to Alpaca corporate-action stream"
            );
            alert_on_reconnect_threshold(
                self.notifier.as_ref(),
                attempt,
                backoff,
            )
            .await;

            tokio::time::sleep(backoff).await;
        }
    }

    async fn consume_connection(
        &mut self,
        cursor: Option<&CorporateActionEventId>,
    ) -> ConnectionConsumption {
        let mut progress = ConnectionProgress::Idle;
        let result =
            self.consume_connection_result(cursor, &mut progress).await;

        ConnectionConsumption { progress, result }
    }

    async fn consume_connection_result(
        &mut self,
        cursor: Option<&CorporateActionEventId>,
        progress: &mut ConnectionProgress,
    ) -> Result<(), CorporateActionFeedError> {
        if cursor.is_none()
            && self.stream_transport
                == CorporateActionStreamTransport::AuthenticatedAlpaca
        {
            return Err(CorporateActionFeedError::BaselineRequired);
        }
        let mut replay_anchor = cursor.cloned();
        let request = self.client.get(&self.endpoint);
        let request = match self.stream_transport {
            CorporateActionStreamTransport::AuthenticatedAlpaca => request
                .header("APCA-API-KEY-ID", &self.api_key)
                .header("APCA-API-SECRET-KEY", &self.api_secret),
            CorporateActionStreamTransport::CredentialFreeDevelopment => {
                request
            }
        };
        let request = if let Some(cursor) = cursor {
            request.query(&[("since_id", cursor.as_str())])
        } else {
            request
        };

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
        let mut applied_mutations = 0_usize;
        let mut last_accepted_event_id = None;
        while let Some(chunk) = chunks.next().await {
            let (mutations, decode_error) = match decoder.push(&chunk?) {
                CorporateActionDecodeBatch::Complete(mutations) => {
                    (mutations, None)
                }
                CorporateActionDecodeBatch::Poison { completed, error } => {
                    (completed, Some(error))
                }
            };
            for mutation in mutations {
                if let Some(expected) = replay_anchor.take()
                    && mutation.event_id != expected
                {
                    let observed = mutation.event_id.clone();
                    persist_replay_gap(&self.pool, &observed).await?;
                    return Err(CorporateActionProjectionError::ReplayGap {
                        expected,
                        observed,
                    }
                    .into());
                }

                let event_id = mutation.event_id.clone();
                let action_id = mutation.action.id.clone();
                let mutation_kind = mutation.kind;
                let outcome =
                    apply_stream_mutation(&self.pool, &mutation).await?;
                reconcile_pending_schedules(&self.pool, &mut self.scheduler)
                    .await?;
                *progress = ConnectionProgress::AcceptedMutation;
                applied_mutations = applied_mutations.saturating_add(1);
                last_accepted_event_id = Some(event_id.clone());
                debug!(
                    target: "asset",
                    event_id = %event_id,
                    action_id = %action_id,
                    mutation = mutation_kind.as_str(),
                    outcome = ?outcome,
                    "Applied Alpaca corporate-action mutation"
                );
            }
            if let Some(error) = decode_error {
                persist_poison_boundary(&self.pool, error.event_id()).await?;
                return Err(error.into());
            }
        }
        if applied_mutations > 0 {
            info!(
                target: "asset",
                applied_mutations,
                last_accepted_event_id = last_accepted_event_id
                    .as_ref()
                    .map(CorporateActionEventId::as_str),
                "Applied Alpaca corporate-action mutations"
            );
        }
        Ok(())
    }
}

fn validate_corporate_action_endpoint(
    endpoint: &str,
    environment: Environment,
) -> Result<CorporateActionStreamTransport, CorporateActionFeedBuildError> {
    let endpoint = Url::parse(endpoint)?;
    if is_development_loopback_endpoint(&endpoint, environment) {
        return Ok(CorporateActionStreamTransport::CredentialFreeDevelopment);
    }
    if endpoint.scheme() != "https" {
        return Err(CorporateActionFeedBuildError::InsecureEndpointScheme(
            endpoint.scheme().to_string(),
        ));
    }
    if endpoint.host_str() != Some("stream.data.alpaca.markets") {
        return Err(CorporateActionFeedBuildError::UnexpectedEndpointHost);
    }
    Ok(CorporateActionStreamTransport::AuthenticatedAlpaca)
}

fn is_development_loopback_endpoint(
    endpoint: &Url,
    environment: Environment,
) -> bool {
    if environment != Environment::Development || endpoint.scheme() != "http" {
        return false;
    }

    match endpoint.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(_)) | None => false,
    }
}

pub(crate) fn spawn_corporate_action_feed(
    feed: CorporateActionFeed,
    shutdown: watch::Receiver<bool>,
    service_shutdown: watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = run_until_shutdown(feed, shutdown).await {
            log_corporate_action_feed_failure(&error);
            let _ = service_shutdown.send(true);
        }
    })
}

async fn run_until_shutdown(
    feed: CorporateActionFeed,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CorporateActionFeedError> {
    tokio::select! {
        outcome = feed.run() => outcome,
        _ = shutdown.changed() => Ok(()),
    }
}

fn log_corporate_action_feed_failure(error: &CorporateActionFeedError) {
    error!(
        target: "asset",
        state = "poisoned",
        failure_kind = error.kind(),
        event_id = error.event_id().map(CorporateActionEventId::as_str),
        error = %error,
        "Alpaca corporate-action stream stopped on poison input; terminating service to fail closed"
    );
}

fn sse_event_identity(frame: &[u8]) -> SseEventIdentity {
    let Some(value) = sse_lines(frame)
        .filter_map(|line| {
            let (field, value) = sse_field(line);
            (field == b"id").then_some(value)
        })
        .last()
    else {
        return SseEventIdentity::Absent;
    };
    let value = value.strip_prefix(b" ").unwrap_or(value);
    let Ok(value) = std::str::from_utf8(value) else {
        return SseEventIdentity::Invalid;
    };

    CorporateActionEventId::new(value)
        .map_or(SseEventIdentity::Invalid, SseEventIdentity::Valid)
}

fn validated_payload_event_id(frame: &str) -> Option<CorporateActionEventId> {
    let data = sse_lines(frame.as_bytes())
        .filter_map(|line| {
            let (field, value) = sse_field(line);
            (field == b"data").then_some(value)
        })
        .map(|value| value.strip_prefix(b" ").unwrap_or(value))
        .map(std::str::from_utf8)
        .collect::<Result<Vec<_>, _>>()
        .ok()?
        .join("\n");
    if data.is_empty() {
        return None;
    }

    serde_json::from_str::<CorporateActionIdentityEnvelope>(&data)
        .ok()
        .and_then(|envelope| envelope.event_id)
}

fn sse_lines(frame: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut remaining = frame;

    std::iter::from_fn(move || {
        if remaining.is_empty() {
            return None;
        }
        let Some(line_end) =
            remaining.iter().position(|byte| matches!(*byte, b'\r' | b'\n'))
        else {
            let line = remaining;
            remaining = &[];
            return Some(line);
        };
        let line = &remaining[..line_end];
        let ending_len = line_ending_len(&remaining[line_end..])?;
        remaining = &remaining[line_end + ending_len..];
        Some(line)
    })
}

fn sse_field(line: &[u8]) -> (&[u8], &[u8]) {
    line.iter()
        .position(|byte| *byte == b':')
        .map_or((line, &[]), |colon| (&line[..colon], &line[colon + 1..]))
}

fn line_ending_len(input: &[u8]) -> Option<usize> {
    match input {
        [b'\r', b'\n', ..] => Some(2),
        [b'\r' | b'\n', ..] => Some(1),
        _ => None,
    }
}

fn can_still_terminate_within_limit(buffer: &[u8]) -> bool {
    const SEPARATORS: [&[u8]; 7] = [
        b"\n\n",
        b"\n\r",
        b"\n\r\n",
        b"\r\r",
        b"\r\n\n",
        b"\r\n\r",
        b"\r\n\r\n",
    ];

    if buffer.len() <= MAX_SSE_FRAME_BYTES {
        return true;
    }

    let separator_prefix = &buffer[MAX_SSE_FRAME_BYTES..];
    SEPARATORS.iter().any(|separator| separator.starts_with(separator_prefix))
}

fn frame_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    (0..buffer.len()).find_map(|frame_end| {
        let first_len = line_ending_len(&buffer[frame_end..])?;
        let second_start = frame_end + first_len;
        let second_len = line_ending_len(buffer.get(second_start..)?)?;
        Some((frame_end, first_len + second_len))
    })
}

pub(crate) fn decode_sse_frame(
    frame: &str,
) -> Result<CorporateActionMutation, CorporateActionDecodeError> {
    let mut event_id = None;
    let mut mutation = None;
    let mut data = Vec::new();

    for line in sse_lines(frame.as_bytes()) {
        if line.is_empty() || line.starts_with(b":") {
            continue;
        }
        let (field, value) = sse_field(line);
        let value = value.strip_prefix(b" ").unwrap_or(value);
        let value = std::str::from_utf8(value)
            .map_err(CorporateActionDecodeError::InvalidFieldUtf8)?;
        match field {
            b"id" => event_id = Some(value.to_string()),
            b"event" => mutation = Some(value.to_string()),
            b"data" => data.push(value),
            _ => {}
        }
    }

    if data.is_empty() {
        return Err(CorporateActionDecodeError::MissingData);
    }
    let sse_event_id = event_id
        .map(|event_id| {
            CorporateActionEventId::new(&event_id)
                .ok_or(CorporateActionDecodeError::InvalidEventId(event_id))
        })
        .transpose()?;
    let envelope: CorporateActionEnvelope =
        serde_json::from_str(&data.join("\n")).map_err(|source| {
            if let Some(event_id) = sse_event_id.clone() {
                CorporateActionDecodeError::InvalidPayload { event_id, source }
            } else {
                CorporateActionDecodeError::InvalidPayloadWithoutEventId(source)
            }
        })?;
    let event_id = match (sse_event_id, envelope.event_id) {
        (Some(sse_event_id), Some(payload_event_id))
            if sse_event_id.as_str() != payload_event_id =>
        {
            return Err(CorporateActionDecodeError::EventIdMismatch {
                sse_event_id,
                payload_event_id,
            });
        }
        (Some(sse_event_id), _) => sse_event_id,
        (None, Some(payload_event_id)) => CorporateActionEventId::new(
            &payload_event_id,
        )
        .ok_or(CorporateActionDecodeError::InvalidEventId(payload_event_id))?,
        (None, None) => {
            return Err(CorporateActionDecodeError::MissingEventId);
        }
    };
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
    IgnoredUnlisted,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionProjectionError {
    #[error("corporate-action event cursor regressed from {current} to {next}")]
    CursorRegression {
        current: CorporateActionEventId,
        next: CorporateActionEventId,
    },
    #[error(
        "corporate-action event processing is blocked at {event_id}: cursor regression"
    )]
    BlockedCursorRegression { event_id: CorporateActionEventId },
    #[error(
        "corporate-action event processing is blocked by poison input at {event_id:?}"
    )]
    BlockedPoison { event_id: Option<CorporateActionEventId> },
    #[error(
        "corporate-action replay did not begin at committed cursor {expected}; first event was {observed}"
    )]
    ReplayGap {
        expected: CorporateActionEventId,
        observed: CorporateActionEventId,
    },
    #[error(
        "corporate-action event processing is blocked at {event_id}: replay gap"
    )]
    BlockedReplayGap { event_id: CorporateActionEventId },
    #[error("stored corporate-action blocked event id is invalid: {0}")]
    InvalidStoredBlockedEventId(String),
    #[error(
        "stored corporate-action blocked event for {reason} has no event id"
    )]
    MissingStoredBlockedEventId { reason: String },
    #[error("stored corporate-action blocked event reason is invalid: {0}")]
    InvalidStoredBlockedReason(String),
    #[error("stored corporate-action event cursor is invalid: {0}")]
    InvalidStoredCursor(String),
    #[error(transparent)]
    View(#[from] TokenizedAssetViewError),
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

/// Enqueues each pending projection revision at least once, then marks that
/// exact event reconciled. A crash after enqueue but before the marker safely
/// repeats the idempotent schedule operation on startup.
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

/// Atomically persists one accepted mutation, its latest schedule revision,
/// and the monotonic replay cursor. Duplicate event IDs are no-ops; an unseen
/// lower ID records a durable blocked boundary before returning an error.
pub(crate) async fn apply_mutation(
    pool: &Pool<Sqlite>,
    mutation: &CorporateActionMutation,
) -> Result<ApplyMutationOutcome, CorporateActionProjectionError> {
    let _revision_guard = acquire_corporate_action_revision_guard().await;
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    if let Some(error) =
        projection_boundary(&mut transaction, &mutation.event_id).await?
    {
        transaction.commit().await?;
        return Err(error);
    }

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

async fn apply_stream_mutation(
    pool: &Pool<Sqlite>,
    mutation: &CorporateActionMutation,
) -> Result<ApplyMutationOutcome, CorporateActionProjectionError> {
    if underlying_has_listing(pool, &mutation.action.underlying).await? {
        return apply_mutation(pool, mutation).await;
    }

    let _revision_guard = acquire_corporate_action_revision_guard().await;
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    if let Some(error) =
        projection_boundary(&mut transaction, &mutation.event_id).await?
    {
        transaction.commit().await?;
        return Err(error);
    }
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

    Ok(ApplyMutationOutcome::IgnoredUnlisted)
}

async fn projection_boundary(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    next: &CorporateActionEventId,
) -> Result<
    Option<CorporateActionProjectionError>,
    CorporateActionProjectionError,
> {
    let blocked: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT event_id, reason FROM corporate_action_blocked_event WHERE singleton = 1",
    )
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(blocked) = blocked {
        return Err(parse_blocked_projection_error(blocked)?);
    }

    let current = sqlx::query_scalar::<_, String>(
        "SELECT event_id FROM corporate_action_cursor WHERE singleton = 1",
    )
    .fetch_optional(&mut **transaction)
    .await?
    .map(parse_stored_cursor)
    .transpose()?;
    let Some(current) = current.filter(|current| next < current) else {
        return Ok(None);
    };

    sqlx::query(
        "
        INSERT INTO corporate_action_blocked_event (
            singleton,
            event_id,
            reason
        )
        VALUES (1, ?, ?)
        ON CONFLICT(singleton) DO NOTHING
        ",
    )
    .bind(next.as_str())
    .bind(BLOCKED_REASON_CURSOR_REGRESSION)
    .execute(&mut **transaction)
    .await?;

    Ok(Some(CorporateActionProjectionError::CursorRegression {
        current,
        next: next.clone(),
    }))
}

/// Loads the last committed replay cursor, refusing startup when a durable
/// poison boundary requires operator repair before any reconnect.
pub(crate) async fn load_cursor(
    pool: &Pool<Sqlite>,
) -> Result<Option<CorporateActionEventId>, CorporateActionProjectionError> {
    let blocked: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT event_id, reason FROM corporate_action_blocked_event WHERE singleton = 1",
    )
    .fetch_optional(pool)
    .await?;
    if let Some(blocked) = blocked {
        return Err(parse_blocked_projection_error(blocked)?);
    }

    let value: Option<String> = sqlx::query_scalar(
        "SELECT event_id FROM corporate_action_cursor WHERE singleton = 1",
    )
    .fetch_optional(pool)
    .await?;

    value.map(parse_stored_cursor).transpose()
}

async fn persist_poison_boundary(
    pool: &Pool<Sqlite>,
    event_id: Option<&CorporateActionEventId>,
) -> Result<(), CorporateActionProjectionError> {
    let _revision_guard = acquire_corporate_action_revision_guard().await;
    sqlx::query(
        "
        INSERT INTO corporate_action_blocked_event (
            singleton,
            event_id,
            reason
        )
        VALUES (1, ?, ?)
        ON CONFLICT(singleton) DO NOTHING
        ",
    )
    .bind(event_id.map(CorporateActionEventId::as_str))
    .bind(BLOCKED_REASON_POISON)
    .execute(pool)
    .await?;

    Ok(())
}

async fn persist_replay_gap(
    pool: &Pool<Sqlite>,
    observed: &CorporateActionEventId,
) -> Result<(), CorporateActionProjectionError> {
    let _revision_guard = acquire_corporate_action_revision_guard().await;
    sqlx::query(
        "
        INSERT INTO corporate_action_blocked_event (
            singleton,
            event_id,
            reason
        )
        VALUES (1, ?, ?)
        ON CONFLICT(singleton) DO NOTHING
        ",
    )
    .bind(observed.as_str())
    .bind(BLOCKED_REASON_REPLAY_GAP)
    .execute(pool)
    .await?;

    Ok(())
}

fn parse_blocked_projection_error(
    (event_id, reason): (Option<String>, String),
) -> Result<CorporateActionProjectionError, CorporateActionProjectionError> {
    let event_id = event_id
        .map(|event_id| {
            CorporateActionEventId::new(&event_id).ok_or(
                CorporateActionProjectionError::InvalidStoredBlockedEventId(
                    event_id,
                ),
            )
        })
        .transpose()?;

    match reason.as_str() {
        BLOCKED_REASON_CURSOR_REGRESSION => {
            let event_id = event_id.ok_or_else(|| {
                CorporateActionProjectionError::MissingStoredBlockedEventId {
                    reason: reason.clone(),
                }
            })?;
            Ok(CorporateActionProjectionError::BlockedCursorRegression {
                event_id,
            })
        }
        BLOCKED_REASON_POISON => {
            Ok(CorporateActionProjectionError::BlockedPoison { event_id })
        }
        BLOCKED_REASON_REPLAY_GAP => {
            let event_id = event_id.ok_or_else(|| {
                CorporateActionProjectionError::MissingStoredBlockedEventId {
                    reason: reason.clone(),
                }
            })?;
            Ok(CorporateActionProjectionError::BlockedReplayGap { event_id })
        }
        _ => Err(CorporateActionProjectionError::InvalidStoredBlockedReason(
            reason,
        )),
    }
}

fn parse_stored_cursor(
    value: String,
) -> Result<CorporateActionEventId, CorporateActionProjectionError> {
    CorporateActionEventId::new(&value)
        .ok_or(CorporateActionProjectionError::InvalidStoredCursor(value))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, NaiveDate};
    use event_sorcery::StoreBuilder;
    use httpmock::prelude::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::jobs::{Job, job_type};
    use crate::mint::test_utils::TestHarness;
    use crate::notifications::{
        CapturingLifecycleNotifier, NoopLifecycleNotifier,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::schedule::{
        AlignCorporateActionFreeze, CorporateActionFreezeCtx,
        CorporateActionScheduleState,
    };
    use crate::underlying::{
        AssetStatus, FreezeHoldId, Underlying, UnderlyingCommand,
        load_freeze_status,
    };

    #[test]
    fn corporate_action_endpoint_restricts_credentials_to_trusted_hosts() {
        assert_eq!(
            validate_corporate_action_endpoint(
                "https://stream.data.alpaca.markets/v1beta1/events/corporate-actions",
                Environment::Production,
            )
            .unwrap(),
            CorporateActionStreamTransport::AuthenticatedAlpaca
        );
        assert!(matches!(
            validate_corporate_action_endpoint(
                "http://stream.data.alpaca.markets/v1beta1/events/corporate-actions",
                Environment::Production,
            ),
            Err(CorporateActionFeedBuildError::InsecureEndpointScheme(_))
        ));
        assert!(matches!(
            validate_corporate_action_endpoint(
                "https://attacker.example/v1beta1/events/corporate-actions",
                Environment::Production,
            ),
            Err(CorporateActionFeedBuildError::UnexpectedEndpointHost)
        ));
        assert_eq!(
            validate_corporate_action_endpoint(
                "http://127.0.0.1:12345/v1beta1/events/corporate-actions",
                Environment::Development,
            )
            .unwrap(),
            CorporateActionStreamTransport::CredentialFreeDevelopment
        );
        assert!(matches!(
            validate_corporate_action_endpoint(
                "http://127.0.0.1:12345/v1beta1/events/corporate-actions",
                Environment::Staging,
            ),
            Err(CorporateActionFeedBuildError::InsecureEndpointScheme(_))
        ));
        assert!(matches!(
            validate_corporate_action_endpoint(
                "http://attacker.example/v1beta1/events/corporate-actions",
                Environment::Development,
            ),
            Err(CorporateActionFeedBuildError::InsecureEndpointScheme(_))
        ));
    }

    #[tokio::test]
    async fn corporate_action_projection_schema_enforces_domain_invariants() {
        let harness = TestHarness::new().await;

        let null_event_id = sqlx::query(
            "INSERT INTO corporate_action_mutations (event_id, action_id, mutation, underlying, ex_date) VALUES (NULL, 'ca-null', 'insert', 'AAPL', '2026-08-14')",
        )
        .execute(&harness.pool)
        .await;
        assert!(null_event_id.is_err());

        let mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-revision",
            "2026-08-14",
        );
        apply_mutation(&harness.pool, &mutation).await.unwrap();
        let invalid_revision = sqlx::query(
            "UPDATE corporate_action_schedule SET revision = 0 WHERE action_id = ?",
        )
        .bind(mutation.action.id.as_str())
        .execute(&harness.pool)
        .await;
        assert!(invalid_revision.is_err());

        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM pragma_index_list('corporate_action_mutations')",
        )
        .fetch_all(&harness.pool)
        .await
        .unwrap();
        assert!(
            !indexes
                .iter()
                .any(|name| { name == "corporate_action_mutations_action_id" })
        );
    }

    #[test]
    fn reconnect_failures_alert_once_at_the_operator_threshold() {
        let mut failures = ReconnectFailures::default();

        for expected in 1..STREAM_RECONNECT_ALERT_THRESHOLD {
            assert_eq!(
                failures.record(ConnectionProgress::Idle),
                ReconnectAttempt {
                    consecutive_failures: expected,
                    escalation: ReconnectEscalation::BelowThreshold,
                }
            );
        }
        assert_eq!(
            failures.record(ConnectionProgress::Idle),
            ReconnectAttempt {
                consecutive_failures: STREAM_RECONNECT_ALERT_THRESHOLD,
                escalation: ReconnectEscalation::AlertOperator,
            }
        );
        assert_eq!(
            failures.record(ConnectionProgress::Idle).escalation,
            ReconnectEscalation::BelowThreshold
        );
    }

    #[test]
    fn accepted_mutation_resets_reconnect_failures() {
        let mut failures = ReconnectFailures::default();
        for _ in 1..STREAM_RECONNECT_ALERT_THRESHOLD {
            failures.record(ConnectionProgress::Idle);
        }

        assert_eq!(
            failures.record(ConnectionProgress::AcceptedMutation),
            ReconnectAttempt {
                consecutive_failures: 1,
                escalation: ReconnectEscalation::BelowThreshold,
            }
        );
    }

    #[test]
    fn reconnect_backoff_is_jittered_and_bounded() {
        let mut backoff =
            reconnect_backoff_builder().with_jitter_seed(42).build();

        for _ in 0..128 {
            let delay = next_reconnect_delay(&mut backoff);
            assert!(delay >= STREAM_RECONNECT_MIN_BACKOFF);
            assert!(delay <= STREAM_RECONNECT_MAX_BACKOFF);
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn reconnect_threshold_warns_and_notifies_the_operator_once() {
        let notifier = CapturingLifecycleNotifier::default();
        let below_threshold = ReconnectAttempt {
            consecutive_failures: STREAM_RECONNECT_ALERT_THRESHOLD - 1,
            escalation: ReconnectEscalation::BelowThreshold,
        };
        let threshold = ReconnectAttempt {
            consecutive_failures: STREAM_RECONNECT_ALERT_THRESHOLD,
            escalation: ReconnectEscalation::AlertOperator,
        };

        alert_on_reconnect_threshold(
            &notifier,
            below_threshold,
            STREAM_RECONNECT_MIN_BACKOFF,
        )
        .await;
        alert_on_reconnect_threshold(
            &notifier,
            threshold,
            STREAM_RECONNECT_MAX_BACKOFF,
        )
        .await;

        assert_eq!(
            notifier.notifications(),
            vec![LifecycleNotification::CorporateActionsSyncFailed]
        );
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "state=\"reconnect_threshold_exceeded\"",
                "consecutive_failures=5",
                "backoff_secs=60"
            ]
        ));
    }

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

    fn complete(
        batch: CorporateActionDecodeBatch,
    ) -> Vec<CorporateActionMutation> {
        match batch {
            CorporateActionDecodeBatch::Complete(mutations) => mutations,
            CorporateActionDecodeBatch::Poison { error, .. } => {
                panic!("expected a complete decode batch, got {error}")
            }
        }
    }

    fn poison(
        batch: CorporateActionDecodeBatch,
    ) -> CorporateActionStreamDecodeError {
        match batch {
            CorporateActionDecodeBatch::Poison { error, .. } => error,
            CorporateActionDecodeBatch::Complete(mutations) => panic!(
                "expected a poison decode batch, got {} mutations",
                mutations.len()
            ),
        }
    }

    fn sse_frame(
        event_id: &str,
        kind: CorporateActionMutationKind,
        action_id: &str,
        underlying: &UnderlyingSymbol,
        ex_date: NaiveDate,
    ) -> String {
        format!(
            "id: {event_id}\nevent: {}\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"{action_id}\",\"symbol\":\"{}\",\"ex_date\":\"{ex_date}\"}}}}\n\n",
            kind.as_str(),
            underlying.as_str()
        )
    }

    async fn consume_sse(
        harness: &TestHarness,
        body: String,
        cursor: Option<&CorporateActionEventId>,
    ) {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.consume_connection(cursor).await.result.unwrap();
    }

    async fn assert_sse_mutation_releases_only_its_hold(
        kind: CorporateActionMutationKind,
    ) {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(harness.pool.clone())
                .build(())
                .await
                .unwrap();
        underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Freeze { underlying: underlying.clone() },
            )
            .await
            .unwrap();
        let first_event_id =
            CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2").unwrap();
        let sibling_event_id =
            CorporateActionEventId::new("01J9RVB6Y4ZK8M3N7QD2WX1RFP").unwrap();
        let mutation_event_id =
            CorporateActionEventId::new("01J9S0V8N6QZ4K2M7RFT3XWCPB").unwrap();
        let today = Utc::now().date_naive();
        let baseline = CorporateActionMutation {
            event_id: first_event_id.clone(),
            kind: CorporateActionMutationKind::Insert,
            action: DividendCorporateAction {
                id: CorporateActionId::new("ca-1").unwrap(),
                underlying: underlying.clone(),
                ex_date: today,
            },
        };
        apply_mutation(&harness.pool, &baseline).await.unwrap();
        consume_sse(
            &harness,
            format!(
                "{}{}",
                sse_frame(
                    first_event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-1",
                    &underlying,
                    today,
                ),
                sse_frame(
                    sibling_event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-2",
                    &underlying,
                    today,
                )
            ),
            Some(&first_event_id),
        )
        .await;
        let ctx = CorporateActionFreezeCtx {
            underlying_store: underlying_store.clone(),
            pool: harness.pool.clone(),
            revision_read_test_hook: None,
        };
        for (action_id, event_id) in
            [("ca-1", first_event_id), ("ca-2", sibling_event_id.clone())]
        {
            AlignCorporateActionFreeze {
                action_id: CorporateActionId::new(action_id).unwrap(),
                expected_event_id: event_id,
            }
            .perform(&ctx)
            .await
            .unwrap();
        }
        let replacement_ex_date = match kind {
            CorporateActionMutationKind::Update => {
                today + ChronoDuration::days(1)
            }
            CorporateActionMutationKind::Delete => today,
            CorporateActionMutationKind::Insert => {
                panic!("ownership assertion requires update or delete")
            }
        };
        consume_sse(
            &harness,
            format!(
                "{}{}",
                sse_frame(
                    sibling_event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-2",
                    &underlying,
                    today,
                ),
                sse_frame(
                    mutation_event_id.as_str(),
                    kind,
                    "ca-1",
                    &underlying,
                    replacement_ex_date,
                )
            ),
            Some(&sibling_event_id),
        )
        .await;
        AlignCorporateActionFreeze {
            action_id: CorporateActionId::new("ca-1").unwrap(),
            expected_event_id: mutation_event_id,
        }
        .perform(&ctx)
        .await
        .unwrap();

        underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Unfreeze { underlying: underlying.clone() },
            )
            .await
            .unwrap();
        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Frozen,
            "the sibling action hold must survive the target action mutation"
        );
        underlying_store
            .send(
                &underlying,
                UnderlyingCommand::ReleaseFreezeHold {
                    underlying: underlying.clone(),
                    hold_id: FreezeHoldId::alpaca_corporate_action(
                        CorporateActionId::new("ca-2").unwrap(),
                    ),
                    released_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Enabled,
            "the mutated action must release only its own hold"
        );
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
            "id: 01J9RPMV5TKB8WX3M4F1KZ7QH2\nevent: insert\ndata: {\"action\":\"insert\",\"ca\":{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"},\"event_id\":\"01J9RPMV5TKB8WX3M4F1KZ7QH3\",\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\"}",
        )
        .unwrap_err();

        assert!(matches!(
            error,
            CorporateActionDecodeError::EventIdMismatch {
                sse_event_id,
                ..
            } if sse_event_id.as_str() == "01J9RPMV5TKB8WX3M4F1KZ7QH2"
        ));
    }

    #[test]
    fn rejects_a_bare_final_sse_id_without_payload_fallback() {
        let event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        let frame = format!(
            "id: {event_id}\nid\nevent: insert\ndata: {{\"event_id\":\"{event_id}\",\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}}}\n\n"
        );
        let mut decoder = CorporateActionSseDecoder::default();

        let error = poison(decoder.push(frame.as_bytes()));
        let feed_error = CorporateActionFeedError::Decode(error);

        assert!(feed_error.event_id().is_none());
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
            complete(decoder.push(
                b"id: 01J9RPMV5TKB8WX3M4F1KZ7QH2\r\nevent: insert\r\ndata: {\"event_type\":\"cash_dividend_corporateaction_event\","
            ))
            .is_empty()
        );

        let mutations = complete(decoder.push(
            b"\"region\":\"us\",\"ca\":{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}\r\n\r\n",
        ));

        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].action.id.as_str(), "ca-1");
    }

    #[test]
    fn rejects_an_oversized_partial_frame_without_retaining_it() {
        let mut decoder = CorporateActionSseDecoder::default();
        let error = poison(decoder.push(&vec![b'x'; MAX_SSE_FRAME_BYTES + 1]));

        assert!(matches!(
            error,
            CorporateActionStreamDecodeError::FrameTooLarge
        ));
        assert!(
            decoder.buffer.is_empty(),
            "an oversized untrusted frame must not remain allocated"
        );
        assert_eq!(
            decoder.buffer.capacity(),
            0,
            "rejecting an oversized frame must release its retained capacity"
        );
    }

    #[test]
    fn decodes_a_mixed_lf_crlf_frame_separator() {
        let mut decoder = CorporateActionSseDecoder::default();
        let mutations = complete(decoder.push(
            b"id: 01J9RPMV5TKB8WX3M4F1KZ7QH2\nevent: insert\ndata: {\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}\n\r\n",
        ));

        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].action.id.as_str(), "ca-1");
    }

    #[test]
    fn accepts_a_split_crlf_separator_at_the_frame_limit() {
        let mut decoder = CorporateActionSseDecoder::default();
        let mut first_chunk = vec![b'x'; MAX_SSE_FRAME_BYTES - 1];
        first_chunk[0] = b':';
        first_chunk.extend_from_slice(b"\r\n");

        assert!(complete(decoder.push(&first_chunk)).is_empty());
        assert!(complete(decoder.push(b"\r\n")).is_empty());
        assert!(decoder.buffer.is_empty());
    }

    #[traced_test]
    #[test]
    fn invalid_payload_error_logs_the_valid_sse_event_id() {
        let event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        let frame =
            format!("id: {event_id}\nevent: insert\ndata: not-json\n\n");
        let mut decoder = CorporateActionSseDecoder::default();
        let error = poison(decoder.push(frame.as_bytes()));

        assert!(
            error.to_string().contains(event_id),
            "a poison-event error must retain its safe replay identity: {error}"
        );
        let feed_error = CorporateActionFeedError::Decode(error);
        assert_eq!(
            feed_error.event_id().map(CorporateActionEventId::as_str),
            Some(event_id),
            "the poison log must expose the validated event ID as a structured field"
        );

        log_corporate_action_feed_failure(&feed_error);

        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "state=\"poisoned\"",
                "failure_kind=\"decode\"",
                &format!("event_id=\"{event_id}\"")
            ]
        ));
    }

    #[test]
    fn returns_completed_frames_before_a_poison_frame() {
        let event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        let valid = format!(
            "id: {event_id}\nevent: insert\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}}}\n\n"
        );
        let poison_event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH3";
        let chunk = format!(
            "{valid}id: {poison_event_id}\nevent: insert\ndata: not-json\n\n"
        );
        let mut decoder = CorporateActionSseDecoder::default();

        let CorporateActionDecodeBatch::Poison { completed, error } =
            decoder.push(chunk.as_bytes())
        else {
            panic!("expected the second frame to poison the batch");
        };

        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].event_id.as_str(), event_id);
        assert!(matches!(
            error,
            CorporateActionStreamDecodeError::Event {
                event_id: Some(ref event_id),
                ..
            } if event_id.as_str() == poison_event_id
        ));
        assert_eq!(decoder.buffer.capacity(), 0);
    }

    #[test]
    fn invalid_utf8_error_retains_the_valid_sse_event_id() {
        let event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        let mut frame =
            format!("id: {event_id}\nevent: insert\ndata: ").into_bytes();
        frame.push(0xff);
        frame.extend_from_slice(b"\n\n");
        let mut decoder = CorporateActionSseDecoder::default();
        let error = poison(decoder.push(&frame));
        let feed_error = CorporateActionFeedError::Decode(error);

        assert_eq!(
            feed_error.event_id().map(CorporateActionEventId::as_str),
            Some(event_id),
            "invalid UTF-8 telemetry must retain a validated ASCII SSE identity"
        );
    }

    #[test]
    fn semantic_poison_error_retains_the_valid_sse_event_id() {
        let event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        let frame = format!(
            "id: {event_id}\nevent: insert\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}}}\n\n"
        );
        let mut decoder = CorporateActionSseDecoder::default();
        let error = poison(decoder.push(frame.as_bytes()));
        let feed_error = CorporateActionFeedError::Decode(error);

        assert_eq!(
            feed_error.event_id().map(CorporateActionEventId::as_str),
            Some(event_id),
            "semantic poison telemetry must retain the validated SSE identity"
        );
    }

    #[test]
    fn payload_only_semantic_poison_retains_its_valid_event_id() {
        let event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        let frame = format!(
            "event: insert\ndata: {{\"event_id\":\"{event_id}\",\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}}}\n\n"
        );
        let mut decoder = CorporateActionSseDecoder::default();
        let error = poison(decoder.push(frame.as_bytes()));
        let feed_error = CorporateActionFeedError::Decode(error);

        assert_eq!(
            feed_error.event_id().map(CorporateActionEventId::as_str),
            Some(event_id),
            "payload-only poison telemetry must retain its validated replay identity"
        );
    }

    #[tokio::test]
    async fn sse_update_preserves_operator_and_sibling_action_holds() {
        assert_sse_mutation_releases_only_its_hold(
            CorporateActionMutationKind::Update,
        )
        .await;
    }

    #[tokio::test]
    async fn sse_delete_preserves_operator_and_sibling_action_holds() {
        assert_sse_mutation_releases_only_its_hold(
            CorporateActionMutationKind::Delete,
        )
        .await;
    }

    #[tokio::test]
    async fn poisoned_feed_requests_graceful_service_shutdown() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200)
                .header("content-type", "application/json")
                .body("{}");
        });
        let feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };
        let (service_shutdown, feed_shutdown) = watch::channel(false);
        let mut observed_shutdown = service_shutdown.subscribe();

        spawn_corporate_action_feed(feed, feed_shutdown, service_shutdown);

        tokio::time::timeout(
            Duration::from_secs(1),
            observed_shutdown.changed(),
        )
        .await
        .expect("poisoned feed must request service shutdown promptly")
        .expect("service shutdown sender must remain live for the signal");
        assert!(*observed_shutdown.borrow());
    }

    #[tokio::test]
    async fn applies_completed_frames_before_returning_a_poison_error() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let applied_event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        let poison_event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH3";
        let cursor_mutation = event(
            applied_event_id,
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        apply_mutation(&harness.pool, &cursor_mutation).await.unwrap();
        let body = format!(
            "id: {applied_event_id}\nevent: insert\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}}}\n\nid: {poison_event_id}\nevent: insert\ndata: not-json\n\n"
        );
        server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let error = feed
            .consume_connection(Some(&cursor_mutation.event_id))
            .await
            .result
            .unwrap_err();

        assert_eq!(
            error.event_id().map(CorporateActionEventId::as_str),
            Some(poison_event_id)
        );
        let restart_error = load_cursor(&harness.pool).await.unwrap_err();
        assert!(matches!(
            restart_error,
            CorporateActionProjectionError::BlockedPoison {
                event_id: Some(event_id)
            } if event_id.as_str() == poison_event_id
        ));
        let blocked: (Option<String>, String) = sqlx::query_as(
            "SELECT event_id, reason FROM corporate_action_blocked_event WHERE singleton = 1",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(blocked.0.as_deref(), Some(poison_event_id));
        assert_eq!(blocked.1, BLOCKED_REASON_POISON);
    }

    #[tokio::test]
    async fn poison_without_an_event_id_blocks_restart() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let cursor_mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        apply_mutation(&harness.pool, &cursor_mutation).await.unwrap();
        server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(vec![b'x'; MAX_SSE_FRAME_BYTES + 1]);
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let error = feed
            .consume_connection(Some(&cursor_mutation.event_id))
            .await
            .result
            .unwrap_err();

        assert!(matches!(
            error,
            CorporateActionFeedError::Decode(
                CorporateActionStreamDecodeError::FrameTooLarge
            )
        ));
        assert!(matches!(
            load_cursor(&harness.pool).await.unwrap_err(),
            CorporateActionProjectionError::BlockedPoison { event_id: None }
        ));
        let blocked: (Option<String>, String) = sqlx::query_as(
            "SELECT event_id, reason FROM corporate_action_blocked_event WHERE singleton = 1",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(blocked, (None, BLOCKED_REASON_POISON.to_string()));
    }

    #[tokio::test]
    async fn replay_connection_refuses_a_non_anchor_first_frame() {
        let harness = TestHarness::new().await;
        let cursor_mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        apply_mutation(&harness.pool, &cursor_mutation).await.unwrap();
        let observed_event_id = "01J9RVB6Y4ZK8M3N7QD2WX1RFP";
        let body = format!(
            "id: {observed_event_id}\nevent: insert\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"ca-2\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-21\"}}}}\n\n"
        );
        let server = MockServer::start();
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .query_param("since_id", cursor_mutation.event_id.as_str());
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let error = feed
            .consume_connection(Some(&cursor_mutation.event_id))
            .await
            .result
            .unwrap_err();

        stream.assert();
        assert_eq!(
            error.event_id().map(CorporateActionEventId::as_str),
            Some(observed_event_id)
        );
        let persisted: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_mutations WHERE event_id = ?",
        )
        .bind(observed_event_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(persisted, 0, "a replay gap must not advance the cursor");
        let blocked: (String, String) = sqlx::query_as(
            "SELECT event_id, reason FROM corporate_action_blocked_event WHERE singleton = 1",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(blocked.0, observed_event_id);
        assert_eq!(blocked.1, "replay_gap");
        assert!(
            load_cursor(&harness.pool).await.is_err(),
            "restart must refuse to reconnect across a persisted replay gap"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn replay_connection_accepts_the_anchor_before_new_events() {
        let harness = TestHarness::new().await;
        let cursor_mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        apply_mutation(&harness.pool, &cursor_mutation).await.unwrap();
        let next_event_id = "01J9RVB6Y4ZK8M3N7QD2WX1RFP";
        let body = format!(
            "id: {}\nevent: insert\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}}}\n\nid: {next_event_id}\nevent: update\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-21\"}}}}\n\n",
            cursor_mutation.event_id
        );
        let server = MockServer::start();
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .query_param("since_id", cursor_mutation.event_id.as_str());
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.consume_connection(Some(&cursor_mutation.event_id))
            .await
            .result
            .unwrap();

        stream.assert();
        assert_eq!(
            load_cursor(&harness.pool)
                .await
                .unwrap()
                .map(|event_id| event_id.to_string()),
            Some(next_event_id.to_string())
        );
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Applied Alpaca corporate-action mutation", next_event_id]
        ));
        assert!(logs_contain_at!(
            Level::INFO,
            &[
                "Applied Alpaca corporate-action mutations",
                "applied_mutations=2",
                next_event_id
            ]
        ));
    }

    #[tokio::test]
    async fn transport_error_preserves_accepted_connection_progress() {
        let harness = TestHarness::new().await;
        let cursor_mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        apply_mutation(&harness.pool, &cursor_mutation).await.unwrap();
        let body = format!(
            "id: {}\nevent: insert\ndata: {{\"event_type\":\"cash_dividend_corporateaction_event\",\"region\":\"us\",\"ca\":{{\"id\":\"ca-1\",\"symbol\":\"AAPL\",\"ex_date\":\"2026-08-14\"}}}}\n\n",
            cursor_mutation.event_id
        );
        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 1000\r\nconnection: close\r\n\r\n{body}"
        );
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let request_bytes = connection.read(&mut request).await.unwrap();
            assert!(request_bytes > 0);
            let request = std::str::from_utf8(&request[..request_bytes])
                .unwrap()
                .to_ascii_lowercase();
            assert!(!request.contains("apca-api-key-id:"));
            assert!(!request.contains("apca-api-secret-key:"));
            connection.write_all(response.as_bytes()).await.unwrap();
            connection.shutdown().await.unwrap();
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("http://{address}/corporate-actions"),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let consumption =
            feed.consume_connection(Some(&cursor_mutation.event_id)).await;

        server.await.unwrap();
        assert_eq!(consumption.progress, ConnectionProgress::AcceptedMutation);
        assert!(matches!(
            consumption.result,
            Err(CorporateActionFeedError::Http(_))
        ));
    }

    #[tokio::test]
    async fn credential_free_development_stream_establishes_its_own_baseline() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let event_id =
            CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2").unwrap();
        server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200).header("content-type", "text/event-stream").body(
                sse_frame(
                    event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-development-baseline",
                    &UnderlyingSymbol::new("UNLISTED").unwrap(),
                    Utc::now().date_naive(),
                ),
            );
        });
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.establish_development_baseline().await.unwrap();

        assert_eq!(load_cursor(&harness.pool).await.unwrap(), Some(event_id));
    }

    #[tokio::test]
    async fn initial_connection_refuses_incomplete_historical_replay() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let mut feed = CorporateActionFeed {
            client: reqwest::Client::new(),
            endpoint: format!("{}/corporate-actions", server.base_url()),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let error = feed.consume_connection(None).await.result.unwrap_err();

        assert!(matches!(error, CorporateActionFeedError::BaselineRequired));
    }

    #[tokio::test]
    async fn projection_transaction_rolls_back_cursor_when_schedule_write_fails()
     {
        let harness = TestHarness::new().await;
        sqlx::query(
            "
            CREATE TRIGGER reject_corporate_action_schedule_insert
            BEFORE INSERT ON corporate_action_schedule
            BEGIN
                SELECT RAISE(ABORT, 'injected schedule failure');
            END
            ",
        )
        .execute(&harness.pool)
        .await
        .unwrap();
        let mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );

        apply_mutation(&harness.pool, &mutation).await.unwrap_err();

        let mutations: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_mutations",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        let scheduled_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_schedule",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        let cursors: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM corporate_action_cursor")
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!((mutations, scheduled_rows, cursors), (0, 0, 0));
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
    async fn schedule_event_must_belong_to_the_same_action() {
        let harness = TestHarness::new().await;
        let first = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        let second = event(
            "01J9RVB6Y4ZK8M3N7QD2WX1RFP",
            CorporateActionMutationKind::Insert,
            "ca-2",
            "2026-08-21",
        );
        apply_mutation(&harness.pool, &first).await.unwrap();
        apply_mutation(&harness.pool, &second).await.unwrap();

        let error = sqlx::query(
            "UPDATE corporate_action_schedule SET event_id = ? WHERE action_id = ?",
        )
        .bind(second.event_id.as_str())
        .bind(first.action.id.as_str())
        .execute(&harness.pool)
        .await
        .unwrap_err();

        assert!(error.as_database_error().is_some());
    }

    #[tokio::test]
    async fn cursor_regression_persists_a_blocked_boundary() {
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
        let blocked: (String, String) = sqlx::query_as(
            "SELECT event_id, reason FROM corporate_action_blocked_event WHERE singleton = 1",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(blocked.0, older.event_id.as_str());
        assert_eq!(blocked.1, BLOCKED_REASON_CURSOR_REGRESSION);

        let restart_error = load_cursor(&harness.pool).await.unwrap_err();
        assert!(matches!(
            restart_error,
            CorporateActionProjectionError::BlockedCursorRegression {
                event_id
            } if event_id == older.event_id
        ));

        let later = event(
            "01J9S0V8N6QZ4K2M7RFT3XWCPB",
            CorporateActionMutationKind::Insert,
            "ca-2",
            "2026-08-28",
        );
        let blocked_error =
            apply_mutation(&harness.pool, &later).await.unwrap_err();
        assert!(matches!(
            blocked_error,
            CorporateActionProjectionError::BlockedCursorRegression {
                event_id
            } if event_id == older.event_id
        ));
        let later_persisted: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_mutations WHERE event_id = ?",
        )
        .bind(later.event_id.as_str())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(later_persisted, 0);
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
        assert_eq!(
            load_cursor(&harness.pool)
                .await
                .unwrap()
                .as_ref()
                .map(CorporateActionEventId::as_str),
            Some(mutation.event_id.as_str())
        );
        let pending: Option<String> = sqlx::query_scalar(
            "SELECT reconciled_event_id FROM corporate_action_schedule WHERE action_id = ?",
        )
        .bind(mutation.action.id.as_str())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(pending, None, "projection commit precedes job enqueue");
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
    async fn reconciliation_recovers_after_enqueue_before_marker() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let mutation = CorporateActionMutation {
            event_id: CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2")
                .unwrap(),
            kind: CorporateActionMutationKind::Insert,
            action: DividendCorporateAction {
                id: CorporateActionId::new("ca-1").unwrap(),
                underlying: underlying.clone(),
                ex_date: Utc::now().date_naive(),
            },
        };
        apply_mutation(&harness.pool, &mutation).await.unwrap();
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );
        scheduler
            .schedule_revision(
                &mutation.action.id,
                &mutation.event_id,
                &underlying,
                mutation.action.ex_date,
                CorporateActionScheduleState::Active,
                Utc::now(),
            )
            .await
            .unwrap();
        let before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<AlignCorporateActionFreeze>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();

        reconcile_pending_schedules(&harness.pool, &mut scheduler)
            .await
            .unwrap();

        let after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(job_type::<AlignCorporateActionFreeze>())
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(after, before, "recovery must not duplicate queued jobs");
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
    async fn unlisted_underlying_advances_only_the_stream_cursor() {
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
        assert_eq!(
            apply_stream_mutation(&harness.pool, &mutation).await.unwrap(),
            ApplyMutationOutcome::IgnoredUnlisted
        );
        let mut scheduler = CorporateActionFreezeScheduler::new(
            &harness.apalis_pool,
            harness.pool.clone(),
        );

        reconcile_pending_schedules(&harness.pool, &mut scheduler)
            .await
            .unwrap();

        let cursor = load_cursor(&harness.pool).await.unwrap();
        assert_eq!(
            cursor.as_ref().map(CorporateActionEventId::as_str),
            Some(mutation.event_id.as_str()),
        );
        let mutations: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_mutations",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        let scheduled_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM corporate_action_schedule",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        let jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ?",
        )
        .bind(crate::jobs::job_type::<
            crate::tokenized_asset::schedule::AlignCorporateActionFreeze,
        >())
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!((mutations, scheduled_rows, jobs), (0, 0, 0));
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
