//! Durable projection of Alpaca corporate-action stream mutations.

use backon::{BackoffBuilder, ExponentialBuilder};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use event_sorcery::Store;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use super::schedule::{
    AlignCorporateActionFreeze, CorporateActionFreezeCtx,
    CorporateActionFreezeError, CorporateActionFreezeScheduler,
    CorporateActionRevisionGuard, CorporateActionScheduleError,
    CorporateActionScheduleState, acquire_corporate_action_revision_guard,
    align_corporate_action_freeze_under_guards,
};
use super::view::{TokenizedAssetViewError, underlying_has_listing};
use super::{CorporateActionEventId, CorporateActionId, UnderlyingSymbol};
use crate::alpaca::{
    AlpacaConfig,
    service::{
        CorporateActionBootstrapSince, CorporateActionBootstrapSinceError,
    },
};
use crate::config::Environment;
use crate::notifications::{LifecycleNotification, LifecycleNotifier};
use crate::underlying::{
    FreezeAdmissionGuard, Underlying, acquire_freeze_admission,
};

const BLOCKED_REASON_CURSOR_REGRESSION: &str = "cursor_regression";
const BLOCKED_REASON_POISON: &str = "poison";
const BLOCKED_REASON_REPLAY_GAP: &str = "replay_gap";
const STREAM_RECONNECT_MIN_BACKOFF: Duration = Duration::from_secs(5);
const STREAM_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(60);
/// Longest reconnect wait a server-supplied `Retry-After` can impose. The
/// header is unbounded, so a hostile or broken value must not park the feed.
const STREAM_RECONNECT_MAX_RETRY_AFTER: Duration = Duration::from_mins(5);
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CorporateActionReplayUntil(DateTime<Utc>);

impl CorporateActionReplayUntil {
    const fn at(instant: DateTime<Utc>) -> Self {
        Self(instant)
    }

    fn query_value(&self) -> String {
        self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true)
    }
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

fn honor_retry_after(
    backoff: Duration,
    retry_after: Option<Duration>,
) -> Duration {
    backoff.max(
        retry_after.unwrap_or_default().min(STREAM_RECONNECT_MAX_RETRY_AFTER),
    )
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

#[cfg(test)]
use st0x_alpaca::ALPACA_TOKEN_URL;
use st0x_alpaca::AlpacaAuth;
pub(crate) use st0x_alpaca::corporate_actions::CorporateActionMutationKind;
use st0x_alpaca::corporate_actions::{
    CorporateActionDecodeBatch, CorporateActionEndpointError,
    CorporateActionReplay, CorporateActionStreamBuildError,
    CorporateActionStreamClient, CorporateActionStreamDecodeError,
    CorporateActionStreamEndpoint, CorporateActionStreamError,
    CorporateActionStreamTransport, DevelopmentLoopback,
};
#[cfg(test)]
use st0x_alpaca::corporate_actions::{
    CorporateActionDecodeError, CorporateActionSseDecoder,
};

pub(crate) struct CorporateActionFeed {
    client: CorporateActionStreamClient,
    stream_transport: CorporateActionStreamTransport,
    bootstrap_since: Option<CorporateActionBootstrapSince>,
    pool: Pool<Sqlite>,
    scheduler: CorporateActionFreezeScheduler,
    underlying_store: Arc<Store<Underlying>>,
    notifier: Arc<dyn LifecycleNotifier>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionFeedBuildError {
    #[error(transparent)]
    Endpoint(#[from] CorporateActionEndpointError),
    #[error(transparent)]
    Client(#[from] CorporateActionStreamBuildError),
    #[error(transparent)]
    AuthConfig(#[from] crate::alpaca::service::AlpacaAuthConfigError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionPostProjectionError {
    #[error(transparent)]
    Reconciliation(#[from] CorporateActionReconciliationError),
    #[error(transparent)]
    Alignment(#[from] CorporateActionFreezeError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CorporateActionFeedError {
    #[error(transparent)]
    Http(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Auth(#[from] st0x_alpaca::KmsJwtError),
    #[error("corporate-action stream returned HTTP {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("corporate-action stream was rate limited")]
    RateLimited { retry_after: Option<Duration> },
    #[error(
        "corporate-action stream returned content type {content_type} with HTTP {status}"
    )]
    InvalidContentType { status: reqwest::StatusCode, content_type: String },
    #[error(transparent)]
    NotSent(#[from] st0x_alpaca::request_id::GateClosed),
    #[error(
        "corporate-action projection has no cursor or explicit bootstrap boundary"
    )]
    BaselineRequired,
    #[error("bounded corporate-action replay ended inside an SSE frame")]
    BoundedReplayEndedMidFrame,
    #[error(transparent)]
    BootstrapSince(#[from] CorporateActionBootstrapSinceError),
    #[error(transparent)]
    Decode(#[from] CorporateActionStreamDecodeError),
    #[error(transparent)]
    Projection(#[from] CorporateActionProjectionError),
    #[error(transparent)]
    Reconciliation(#[from] CorporateActionReconciliationError),
    #[error(transparent)]
    Alignment(#[from] CorporateActionFreezeError),
    #[error(
        "corporate-action projection committed before a required effect failed: {source}"
    )]
    PostProjection {
        #[source]
        source: CorporateActionPostProjectionError,
        _admission_guard: FreezeAdmissionGuard,
    },
}

impl From<CorporateActionStreamError> for CorporateActionFeedError {
    fn from(error: CorporateActionStreamError) -> Self {
        match error {
            CorporateActionStreamError::Http(error) => {
                Self::Http(Box::new(error))
            }
            CorporateActionStreamError::HttpStatus(status) => {
                Self::HttpStatus(status)
            }
            CorporateActionStreamError::RateLimited { retry_after } => {
                Self::RateLimited { retry_after }
            }
            CorporateActionStreamError::InvalidContentType {
                status,
                content_type,
            } => Self::InvalidContentType { status, content_type },
            CorporateActionStreamError::Auth(error) => Self::Auth(error),
            CorporateActionStreamError::NotSent(error) => Self::NotSent(error),
        }
    }
}

impl CorporateActionFeedError {
    const fn kind(&self) -> &'static str {
        match self {
            Self::Http(_) => "transport",
            Self::Auth(_) => "auth",
            Self::HttpStatus(_) => "http_status",
            Self::RateLimited { .. } => "rate_limited",
            Self::InvalidContentType { .. } => "content_type",
            Self::NotSent(_) => "not_sent",
            Self::BaselineRequired => "baseline_required",
            Self::BoundedReplayEndedMidFrame => "bounded_replay_eof",
            Self::BootstrapSince(_) => "bootstrap_since",
            Self::Decode(_) => "decode",
            Self::Projection(_) => "projection",
            Self::Reconciliation(_) => "reconciliation",
            Self::Alignment(_) => "alignment",
            Self::PostProjection { .. } => "post_projection",
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
                | CorporateActionProjectionError::ReplayEndedBeforeAnchor {
                    expected: next,
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
            Self::Auth(_)
            | Self::Http(_)
            | Self::HttpStatus(_)
            | Self::RateLimited { .. }
            | Self::InvalidContentType { .. }
            | Self::NotSent(_)
            | Self::Projection(_)
            | Self::BaselineRequired
            | Self::BoundedReplayEndedMidFrame
            | Self::BootstrapSince(_)
            | Self::Reconciliation(_)
            | Self::Alignment(_)
            | Self::PostProjection { .. } => None,
        }
    }
}

impl CorporateActionFeed {
    pub(crate) fn new(
        config: &AlpacaConfig,
        environment: Environment,
        pool: Pool<Sqlite>,
        apalis_pool: &apalis_sqlite::SqlitePool,
        underlying_store: Arc<Store<Underlying>>,
        notifier: Arc<dyn LifecycleNotifier>,
    ) -> Result<Self, CorporateActionFeedBuildError> {
        let endpoint = CorporateActionStreamEndpoint::parse(
            &config.corporate_actions_stream_url,
            if environment == Environment::Development {
                DevelopmentLoopback::Allow
            } else {
                DevelopmentLoopback::Deny
            },
        )?;
        Self::with_auth(
            config,
            endpoint,
            pool,
            apalis_pool,
            underlying_store,
            notifier,
            (config.auth()?, config.token_url()?),
        )
    }

    fn with_auth(
        config: &AlpacaConfig,
        endpoint: CorporateActionStreamEndpoint,
        pool: Pool<Sqlite>,
        apalis_pool: &apalis_sqlite::SqlitePool,
        underlying_store: Arc<Store<Underlying>>,
        notifier: Arc<dyn LifecycleNotifier>,
        auth: (AlpacaAuth, &str),
    ) -> Result<Self, CorporateActionFeedBuildError> {
        let (auth, token_url) = auth;
        let stream_transport = endpoint.transport();
        Ok(Self {
            client: CorporateActionStreamClient::new(
                endpoint,
                auth,
                token_url,
                Duration::from_secs(config.connect_timeout_secs),
                Duration::from_secs(config.corporate_actions_read_timeout_secs),
            )?,
            stream_transport,
            bootstrap_since: config.corporate_actions_bootstrap_since.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                apalis_pool,
                pool.clone(),
            ),
            underlying_store,
            pool,
            notifier,
        })
    }

    /// Aligns every durable revision, then establishes any required replay
    /// baseline before the HTTP service can accept traffic.
    pub(crate) async fn establish_startup_baseline_at(
        &mut self,
        cutoff: DateTime<Utc>,
    ) -> Result<(), CorporateActionFeedError> {
        self.align_all_current_revisions().await?;
        match self.stream_transport {
            CorporateActionStreamTransport::AuthenticatedAlpaca => {
                self.establish_authenticated_baseline_at(cutoff).await
            }
            CorporateActionStreamTransport::CredentialFreeDevelopment => {
                self.establish_development_baseline().await
            }
        }
    }

    /// Lets a credential-free development stream establish its cursor through
    /// the normal decoder and projection path before the development service
    /// starts.
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

    /// Replays one finite operator-authorized history window before service
    /// readiness. Alpaca closes a `since` + `until` stream after the inclusive
    /// upper bound, so successful EOF proves the bounded replay completed.
    pub(crate) async fn establish_authenticated_baseline_at(
        &mut self,
        cutoff: DateTime<Utc>,
    ) -> Result<(), CorporateActionFeedError> {
        if self.stream_transport
            != CorporateActionStreamTransport::AuthenticatedAlpaca
            || load_cursor(&self.pool).await?.is_some()
        {
            return Ok(());
        }
        let Some(since) = self.bootstrap_since.clone() else {
            return Ok(());
        };
        let until = CorporateActionReplayUntil::at(cutoff);
        warn!(
            target: "asset",
            state = "bootstrapping",
            mode = "bounded_history",
            since = %since.query_value(),
            until = %until.query_value(),
            "Replaying a bounded Alpaca corporate-action window before service readiness"
        );
        self.consume_bootstrap_window(&since, &until).await.result?;
        if load_cursor(&self.pool).await?.is_none() {
            self.bootstrap_since =
                Some(CorporateActionBootstrapSince::try_from_instant(cutoff)?);
        }
        Ok(())
    }

    async fn align_all_current_revisions(
        &mut self,
    ) -> Result<(), CorporateActionFeedError> {
        let revision_guard = acquire_corporate_action_revision_guard().await;
        let admission_guard = acquire_freeze_admission().await;
        reconcile_pending_schedules(&self.pool, &mut self.scheduler).await?;
        let revisions: Vec<(String, String)> = sqlx::query_as(
            "
            SELECT action_id, event_id
            FROM corporate_action_schedule
            ORDER BY event_id
            ",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(CorporateActionReconciliationError::from)?;
        let ctx = CorporateActionFreezeCtx {
            underlying_store: self.underlying_store.clone(),
            pool: self.pool.clone(),
            #[cfg(test)]
            revision_read_test_hook: None,
        };
        for (action_id, event_id) in revisions {
            let action_id =
                CorporateActionId::new(&action_id).ok_or_else(|| {
                    CorporateActionReconciliationError::InvalidActionId(
                        action_id,
                    )
                })?;
            let event_id =
                CorporateActionEventId::new(&event_id).ok_or_else(|| {
                    CorporateActionReconciliationError::InvalidEventId(event_id)
                })?;
            align_corporate_action_freeze_under_guards(
                &AlignCorporateActionFreeze {
                    action_id,
                    expected_event_id: event_id,
                },
                &ctx,
                &revision_guard,
                &admission_guard,
            )
            .await?;
        }
        Ok(())
    }

    async fn apply_and_align_stream_mutation(
        &mut self,
        mutation: &CorporateActionMutation,
    ) -> Result<ApplyMutationOutcome, CorporateActionFeedError> {
        let revision_guard = acquire_corporate_action_revision_guard().await;
        // Revision always precedes admission; scheduled jobs use the same
        // ordering. Keep admission through projection and the hold commit so a
        // mint cannot validate inside the boundary.
        let admission_guard = acquire_freeze_admission().await;
        let outcome = apply_stream_mutation_under_guards(
            &self.pool,
            mutation,
            &revision_guard,
            &admission_guard,
        )
        .await?;
        if let Err(source) =
            reconcile_pending_schedules(&self.pool, &mut self.scheduler).await
        {
            return Err(CorporateActionFeedError::PostProjection {
                source: source.into(),
                _admission_guard: admission_guard,
            });
        }
        let ctx = CorporateActionFreezeCtx {
            underlying_store: self.underlying_store.clone(),
            pool: self.pool.clone(),
            #[cfg(test)]
            revision_read_test_hook: None,
        };
        let alignment = align_corporate_action_freeze_under_guards(
            &AlignCorporateActionFreeze {
                action_id: mutation.action.id.clone(),
                expected_event_id: mutation.event_id.clone(),
            },
            &ctx,
            &revision_guard,
            &admission_guard,
        )
        .await;
        if let Err(source) = alignment {
            return Err(CorporateActionFeedError::PostProjection {
                source: source.into(),
                _admission_guard: admission_guard,
            });
        }
        Ok(outcome)
    }

    /// Connects after startup alignment, then retries transport failures with
    /// backoff. Contract, projection, and reconciliation failures stop the
    /// feed so the service fails closed.
    pub(crate) async fn run(mut self) -> Result<(), CorporateActionFeedError> {
        if load_cursor(&self.pool).await?.is_none()
            && self.stream_transport
                == CorporateActionStreamTransport::AuthenticatedAlpaca
            && self.bootstrap_since.is_none()
        {
            info!(
                target: "asset",
                state = "disabled",
                reason = "baseline_required",
                "Corporate-action feed remains disabled without a cursor or explicit bootstrap boundary"
            );
            return Ok(());
        }
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
                    | CorporateActionFeedError::Alignment(_)
                    | CorporateActionFeedError::PostProjection { .. }
                    | CorporateActionFeedError::InvalidContentType { .. }
                    | CorporateActionFeedError::NotSent(_)
                    | CorporateActionFeedError::BaselineRequired
                    | CorporateActionFeedError::BoundedReplayEndedMidFrame),
                ) => {
                    self.notifier
                        .notify(
                            &LifecycleNotification::CorporateActionsSyncFailed,
                        )
                        .await;
                    return Err(error);
                }
                Err(CorporateActionFeedError::Auth(error)) if error.is_deterministic() => {
                    return Err(CorporateActionFeedError::Auth(error));
                }
                Err(CorporateActionFeedError::HttpStatus(status))
                    if status.is_client_error()
                        && status != reqwest::StatusCode::TOO_MANY_REQUESTS =>
                {
                    if !matches!(status, reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN) {
                        self.notifier
                            .notify(&LifecycleNotification::CorporateActionsSyncFailed)
                            .await;
                    }
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
            let backoff = match &disconnect_error {
                Some(CorporateActionFeedError::Auth(error)) => {
                    honor_retry_after(backoff, error.retry_after())
                }
                Some(CorporateActionFeedError::RateLimited { retry_after }) => {
                    honor_retry_after(backoff, *retry_after)
                }
                _ => backoff,
            };
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
        self.consume_connection_with_window(cursor, None).await
    }

    async fn consume_bootstrap_window(
        &mut self,
        since: &CorporateActionBootstrapSince,
        until: &CorporateActionReplayUntil,
    ) -> ConnectionConsumption {
        self.consume_connection_with_window(None, Some((since, until))).await
    }

    async fn consume_connection_with_window(
        &mut self,
        cursor: Option<&CorporateActionEventId>,
        bootstrap_window: Option<(
            &CorporateActionBootstrapSince,
            &CorporateActionReplayUntil,
        )>,
    ) -> ConnectionConsumption {
        let mut progress = ConnectionProgress::Idle;
        let result = self
            .consume_connection_result(cursor, bootstrap_window, &mut progress)
            .await;

        let credential_rejection = match &result {
            Err(CorporateActionFeedError::Auth(error)) => {
                error.is_deterministic()
            }
            Err(CorporateActionFeedError::HttpStatus(status)) => matches!(
                *status,
                reqwest::StatusCode::UNAUTHORIZED
                    | reqwest::StatusCode::FORBIDDEN
            ),
            _ => false,
        };
        if credential_rejection && let Err(error) = &result {
            error!(target: "operational_alert", error = %error, "Alpaca credential rejected");
            self.notifier
                .notify(&LifecycleNotification::CorporateActionsSyncFailed)
                .await;
        }
        ConnectionConsumption { progress, result }
    }

    async fn consume_connection_result(
        &mut self,
        cursor: Option<&CorporateActionEventId>,
        bootstrap_window: Option<(
            &CorporateActionBootstrapSince,
            &CorporateActionReplayUntil,
        )>,
        progress: &mut ConnectionProgress,
    ) -> Result<(), CorporateActionFeedError> {
        let bounded_replay = bootstrap_window.is_some();
        if cursor.is_none()
            && !bounded_replay
            && self.stream_transport
                == CorporateActionStreamTransport::AuthenticatedAlpaca
            && self.bootstrap_since.is_none()
        {
            return Err(CorporateActionFeedError::BaselineRequired);
        }
        let mut replay_anchor = cursor.cloned();
        let replay = if let Some(cursor) = cursor {
            CorporateActionReplay::SinceId(cursor.clone())
        } else if let Some((since, until)) = bootstrap_window {
            CorporateActionReplay::Window {
                since: since.clone(),
                until: st0x_alpaca::corporate_actions::CorporateActionReplayUntil::at(until.0),
            }
        } else if let Some(since) = self.bootstrap_since.as_ref()
            && self.stream_transport
                == CorporateActionStreamTransport::AuthenticatedAlpaca
        {
            CorporateActionReplay::Since(since.clone())
        } else {
            CorporateActionReplay::Live
        };
        let mut stream = self
            .client
            .connect(&replay)
            .await
            .map_err(CorporateActionFeedError::from)?;
        info!(
            target: "asset",
            state = "connected",
            "Connected to Alpaca corporate-action stream"
        );

        let mut applied_mutations = 0_usize;
        let mut last_accepted_event_id = None;
        loop {
            let batch = stream
                .next_batch()
                .await
                .map_err(CorporateActionStreamError::Http)?;
            let Some(batch) = batch else {
                break;
            };
            let (mutations, decode_error) = match batch {
                CorporateActionDecodeBatch::Complete(mutations) => {
                    (mutations, None)
                }
                CorporateActionDecodeBatch::Poison { completed, error } => {
                    (completed, Some(error))
                }
            };
            for mutation in mutations {
                let mutation = projection_mutation(mutation)?;
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
                    self.apply_and_align_stream_mutation(&mutation).await?;
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
        if bounded_replay && stream.has_pending_frame() {
            return Err(CorporateActionFeedError::BoundedReplayEndedMidFrame);
        }
        if let Some(expected) = replay_anchor {
            persist_replay_gap(&self.pool, &expected).await?;
            return Err(
                CorporateActionProjectionError::ReplayEndedBeforeAnchor {
                    expected,
                }
                .into(),
            );
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

pub(crate) fn spawn_corporate_action_feed(
    feed: CorporateActionFeed,
    shutdown: watch::Receiver<bool>,
    service_shutdown: watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = run_until_shutdown(feed, shutdown).await {
            let retain_admission = matches!(
                &error,
                CorporateActionFeedError::PostProjection { .. }
            );
            log_corporate_action_feed_failure(&error);
            let _ = service_shutdown.send(true);
            if retain_admission {
                // Projection committed before its required hold effect. Keep
                // mint admission closed until Rocket aborts this background
                // task during shutdown; dropping the error earlier would let
                // a request race the shutdown notification.
                let _retained_error = error;
                std::future::pending::<()>().await;
            }
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
        "Alpaca corporate-action stream stopped after a fatal consistency \
         failure; terminating service to fail closed"
    );
}

fn projection_mutation(
    mutation: st0x_alpaca::corporate_actions::CorporateActionMutation,
) -> Result<CorporateActionMutation, CorporateActionFeedError> {
    let symbol = mutation.action.underlying.as_str();
    let underlying = UnderlyingSymbol::new(symbol).map_err(|_| CorporateActionStreamDecodeError::Event {
        event_id: Some(mutation.event_id.clone()),
        source: st0x_alpaca::corporate_actions::CorporateActionDecodeError::InvalidUnderlying(symbol.to_string()),
    })?;
    Ok(CorporateActionMutation {
        event_id: mutation.event_id,
        kind: mutation.kind,
        action: DividendCorporateAction {
            id: mutation.action.id,
            underlying,
            ex_date: mutation.action.ex_date,
        },
    })
}

#[cfg(test)]
const MAX_SSE_FRAME_BYTES: usize = 64 * 1024;

#[cfg(test)]
fn validate_corporate_action_endpoint(
    endpoint: &str,
    environment: Environment,
) -> Result<CorporateActionStreamTransport, CorporateActionEndpointError> {
    CorporateActionStreamEndpoint::parse(
        endpoint,
        if environment == Environment::Development {
            DevelopmentLoopback::Allow
        } else {
            DevelopmentLoopback::Deny
        },
    )
    .map(|endpoint| endpoint.transport())
}

#[cfg(test)]
fn test_stream_client(
    endpoint: &str,
    transport: CorporateActionStreamTransport,
) -> CorporateActionStreamClient {
    let endpoint = match transport {
        CorporateActionStreamTransport::AuthenticatedAlpaca => {
            CorporateActionStreamEndpoint::authenticated_loopback(endpoint)
                .unwrap()
        }
        CorporateActionStreamTransport::CredentialFreeDevelopment => {
            CorporateActionStreamEndpoint::parse(
                endpoint,
                DevelopmentLoopback::Allow,
            )
            .unwrap()
        }
    };
    CorporateActionStreamClient::new(
        endpoint,
        AlpacaAuth::Basic {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
        },
        ALPACA_TOKEN_URL,
        Duration::from_secs(10),
        Duration::from_secs(90),
    )
    .unwrap()
}

#[cfg(test)]
fn decode_sse_frame(
    frame: &str,
) -> Result<CorporateActionMutation, CorporateActionDecodeError> {
    let mut decoder = CorporateActionSseDecoder::default();
    match decoder.push(format!("{frame}\n\n").as_bytes()) {
        CorporateActionDecodeBatch::Complete(mut mutations) => {
            let mutation = mutations.remove(0);
            Ok(projection_mutation(mutation).unwrap())
        }
        CorporateActionDecodeBatch::Poison {
            error: CorporateActionStreamDecodeError::Event { source, .. },
            ..
        } => Err(source),
        other @ CorporateActionDecodeBatch::Poison { .. } => {
            panic!("unexpected decode result: {other:?}")
        }
    }
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
        "corporate-action replay ended before committed cursor {expected} was returned"
    )]
    ReplayEndedBeforeAnchor { expected: CorporateActionEventId },
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
#[cfg(test)]
pub(crate) async fn apply_mutation(
    pool: &Pool<Sqlite>,
    mutation: &CorporateActionMutation,
) -> Result<ApplyMutationOutcome, CorporateActionProjectionError> {
    let revision_guard = acquire_corporate_action_revision_guard().await;
    let admission_guard = acquire_freeze_admission().await;
    apply_mutation_under_guards(
        pool,
        mutation,
        &revision_guard,
        &admission_guard,
    )
    .await
}

async fn apply_mutation_under_guards(
    pool: &Pool<Sqlite>,
    mutation: &CorporateActionMutation,
    _revision_guard: &CorporateActionRevisionGuard,
    _admission_guard: &FreezeAdmissionGuard,
) -> Result<ApplyMutationOutcome, CorporateActionProjectionError> {
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

#[cfg(test)]
async fn apply_stream_mutation(
    pool: &Pool<Sqlite>,
    mutation: &CorporateActionMutation,
) -> Result<ApplyMutationOutcome, CorporateActionProjectionError> {
    let revision_guard = acquire_corporate_action_revision_guard().await;
    let admission_guard = acquire_freeze_admission().await;
    apply_stream_mutation_under_guards(
        pool,
        mutation,
        &revision_guard,
        &admission_guard,
    )
    .await
}

async fn apply_stream_mutation_under_guards(
    pool: &Pool<Sqlite>,
    mutation: &CorporateActionMutation,
    revision_guard: &CorporateActionRevisionGuard,
    admission_guard: &FreezeAdmissionGuard,
) -> Result<ApplyMutationOutcome, CorporateActionProjectionError> {
    if underlying_has_listing(pool, &mutation.action.underlying).await? {
        return apply_mutation_under_guards(
            pool,
            mutation,
            revision_guard,
            admission_guard,
        )
        .await;
    }

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
    use chrono::{DateTime, Duration as ChronoDuration, NaiveDate};
    use event_sorcery::StoreBuilder;
    use httpmock::prelude::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::alpaca::service::CorporateActionBootstrapSince;
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

    #[tokio::test]
    #[traced_test]
    async fn deterministic_stream_credential_rejection_is_fatal_and_alerts() {
        use p256::pkcs8::EncodePrivateKey;
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let token = server.mock(|when, then| {
            when.method(POST).path("/token");
            then.status(401).body("invalid_client");
        });
        let mut config = AlpacaConfig::test_default();
        config.corporate_actions_bootstrap_since =
            Some("2026-08-31T00:00:00Z".parse().unwrap());
        let key = p256::SecretKey::from_slice(&[7_u8; 32]).unwrap();
        let pem =
            key.to_pkcs8_pem(p256::pkcs8::LineEnding::LF).unwrap().to_string();
        let endpoint = CorporateActionStreamEndpoint::authenticated_loopback(
            &format!("{}/corporate-actions", server.base_url()),
        )
        .unwrap();
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let feed = CorporateActionFeed::with_auth(
            &config,
            endpoint,
            harness.pool.clone(),
            &harness.apalis_pool,
            harness.underlying_store.clone(),
            notifier.clone(),
            (
                AlpacaAuth::PrivateKeyJwt {
                    client_id: "s01-test-client".to_string(),
                    private_key_pem: pem,
                },
                &format!("{}/token", server.base_url()),
            ),
        )
        .unwrap();
        assert!(
            matches!(feed.run().await.unwrap_err(), CorporateActionFeedError::Auth(error) if error.is_deterministic())
        );
        token.assert_calls(1);
        assert_eq!(
            notifier.notifications(),
            vec![LifecycleNotification::CorporateActionsSyncFailed]
        );
        logs_assert(|lines: &[&str]| {
            if lines.iter().any(|line| {
                line.contains("ERROR")
                    && line.contains(
                        "operational_alert: Alpaca credential rejected",
                    )
            }) {
                Ok(())
            } else {
                Err("credential rejection did not emit its operational alert"
                    .to_string())
            }
        });
    }

    #[tokio::test]
    #[traced_test]
    async fn bootstrap_credential_rejection_alerts_without_advancing_cursor() {
        use p256::pkcs8::EncodePrivateKey;
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let token = server.mock(|when, then| {
            when.method(POST).path("/token");
            then.status(401).body("invalid_client");
        });
        let mut config = AlpacaConfig::test_default();
        config.corporate_actions_bootstrap_since =
            Some("2026-08-31T00:00:00Z".parse().unwrap());
        let key = p256::SecretKey::from_slice(&[7_u8; 32]).unwrap();
        let pem =
            key.to_pkcs8_pem(p256::pkcs8::LineEnding::LF).unwrap().to_string();
        let endpoint = CorporateActionStreamEndpoint::authenticated_loopback(
            &format!("{}/corporate-actions", server.base_url()),
        )
        .unwrap();
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let mut feed = CorporateActionFeed::with_auth(
            &config,
            endpoint,
            harness.pool.clone(),
            &harness.apalis_pool,
            harness.underlying_store.clone(),
            notifier.clone(),
            (
                AlpacaAuth::PrivateKeyJwt {
                    client_id: "s01-test-client".to_string(),
                    private_key_pem: pem,
                },
                &format!("{}/token", server.base_url()),
            ),
        )
        .unwrap();
        assert!(
            matches!(feed.establish_authenticated_baseline_at(Utc::now()).await.unwrap_err(), CorporateActionFeedError::Auth(error) if error.is_deterministic())
        );
        token.assert_calls(1);
        assert!(load_cursor(&harness.pool).await.unwrap().is_none());
        assert_eq!(
            notifier.notifications(),
            vec![LifecycleNotification::CorporateActionsSyncFailed]
        );
        logs_assert(|lines: &[&str]| {
            if lines.iter().any(|line| {
                line.contains("ERROR")
                    && line.contains(
                        "operational_alert: Alpaca credential rejected",
                    )
            }) {
                Ok(())
            } else {
                Err("credential rejection did not emit its operational alert"
                    .to_string())
            }
        });
    }

    #[tokio::test]
    #[traced_test]
    async fn minted_bearer_rejected_by_stream_alerts_once_in_bootstrap_and_run()
    {
        use p256::pkcs8::EncodePrivateKey;
        for status in [401, 403] {
            for bootstrap in [true, false] {
                let harness = TestHarness::new().await;
                let server = MockServer::start();
                let token = server.mock(|when, then| {
                    when.method(POST).path("/token");
                    then.status(200).json_body(serde_json::json!({
                        "access_token":"rejected-stream-bearer", "token_type":"Bearer", "expires_in":900
                    }));
                });
                let stream = server.mock(|when, then| {
                    when.method(GET)
                        .path("/corporate-actions")
                        .header(
                            "Authorization",
                            "Bearer rejected-stream-bearer",
                        )
                        .header_missing("APCA-API-KEY-ID")
                        .header_missing("APCA-API-SECRET-KEY");
                    then.status(status);
                });
                let mut config = AlpacaConfig::test_default();
                config.corporate_actions_bootstrap_since =
                    Some("2026-08-31T00:00:00Z".parse().unwrap());
                let key = p256::SecretKey::from_slice(&[7_u8; 32]).unwrap();
                let pem = key
                    .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
                    .unwrap()
                    .to_string();
                let endpoint =
                    CorporateActionStreamEndpoint::authenticated_loopback(
                        &format!("{}/corporate-actions", server.base_url()),
                    )
                    .unwrap();
                let notifier = Arc::new(CapturingLifecycleNotifier::default());
                let mut feed = CorporateActionFeed::with_auth(
                    &config,
                    endpoint,
                    harness.pool.clone(),
                    &harness.apalis_pool,
                    harness.underlying_store.clone(),
                    notifier.clone(),
                    (
                        AlpacaAuth::PrivateKeyJwt {
                            client_id: "s01-test-client".to_string(),
                            private_key_pem: pem,
                        },
                        &format!("{}/token", server.base_url()),
                    ),
                )
                .unwrap();
                let result = if bootstrap {
                    feed.establish_authenticated_baseline_at(Utc::now()).await
                } else {
                    feed.run().await
                };
                assert!(
                    matches!(result.unwrap_err(), CorporateActionFeedError::HttpStatus(observed) if observed.as_u16() == status)
                );
                token.assert_calls(1);
                stream.assert_calls(1);
                assert!(load_cursor(&harness.pool).await.unwrap().is_none());
                assert_eq!(
                    notifier.notifications(),
                    vec![LifecycleNotification::CorporateActionsSyncFailed]
                );
            }
        }
        logs_assert(|lines: &[&str]| {
            let alerts = lines
                .iter()
                .filter(|line| {
                    line.contains("ERROR")
                        && line.contains(
                            "operational_alert: Alpaca credential rejected",
                        )
                })
                .count();
            if alerts == 4 {
                Ok(())
            } else {
                Err(format!(
                    "expected one rejection alert per connection, got {alerts}"
                ))
            }
        });
        assert!(!logs_contain("rejected-stream-bearer"));
    }

    #[tokio::test]
    #[traced_test]
    async fn feed_builder_uses_bearer_without_apca_headers() {
        use p256::pkcs8::EncodePrivateKey;
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let token = server.mock(|when, then| {
            when.method(POST).path("/token");
            then.status(200).json_body(serde_json::json!({"access_token":"feed-bearer", "token_type":"Bearer", "expires_in":900}));
        });
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .header("Authorization", "Bearer feed-bearer")
                .header_missing("APCA-API-KEY-ID")
                .header_missing("APCA-API-SECRET-KEY")
                .query_param("since", "2026-08-31T00:00:00Z");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body("");
        });
        let mut config = AlpacaConfig::test_default();
        config.corporate_actions_bootstrap_since =
            Some("2026-08-31T00:00:00Z".parse().unwrap());
        let key = p256::SecretKey::from_slice(&[7_u8; 32]).unwrap();
        let pem =
            key.to_pkcs8_pem(p256::pkcs8::LineEnding::LF).unwrap().to_string();
        let endpoint = CorporateActionStreamEndpoint::authenticated_loopback(
            &format!("{}/corporate-actions", server.base_url()),
        )
        .unwrap();
        let mut feed = CorporateActionFeed::with_auth(
            &config,
            endpoint,
            harness.pool.clone(),
            &harness.apalis_pool,
            harness.underlying_store.clone(),
            Arc::new(NoopLifecycleNotifier),
            (
                AlpacaAuth::PrivateKeyJwt {
                    client_id: "s01-test-client".to_string(),
                    private_key_pem: pem,
                },
                &format!("{}/token", server.base_url()),
            ),
        )
        .unwrap();
        feed.consume_connection(None).await.result.unwrap();
        feed.consume_connection(None).await.result.unwrap();
        token.assert_calls(1);
        stream.assert_calls(2);
        assert!(logs_contain_at!(
            Level::INFO,
            &["Connected to Alpaca corporate-action stream"]
        ));
        assert!(!logs_contain("feed-bearer"));
    }

    #[test]
    fn corporate_action_endpoint_rejects_userinfo_fragments_and_until_id() {
        for endpoint in [
            "https://user:secret@stream.data.alpaca.markets/events",
            "https://stream.data.alpaca.markets/events#fragment",
            "https://stream.data.alpaca.markets/events?until_id=cursor",
            "http://user:secret@127.0.0.1/events",
        ] {
            assert!(
                validate_corporate_action_endpoint(
                    endpoint,
                    Environment::Development
                )
                .is_err()
            );
        }
    }

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
            Err(CorporateActionEndpointError::InsecureEndpointScheme(_))
        ));
        assert!(matches!(
            validate_corporate_action_endpoint(
                "https://attacker.example/v1beta1/events/corporate-actions",
                Environment::Production,
            ),
            Err(CorporateActionEndpointError::UnexpectedEndpointHost)
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
            Err(CorporateActionEndpointError::InsecureEndpointScheme(_))
        ));
        assert!(matches!(
            validate_corporate_action_endpoint(
                "http://attacker.example/v1beta1/events/corporate-actions",
                Environment::Development,
            ),
            Err(CorporateActionEndpointError::InsecureEndpointScheme(_))
        ));
        for parameter in ["since", "since_id", "until", "until_id"] {
            let endpoint = format!(
                "https://stream.data.alpaca.markets/v1beta1/events/corporate-actions?{parameter}=reserved"
            );
            assert!(matches!(
                validate_corporate_action_endpoint(
                    &endpoint,
                    Environment::Production,
                ),
                Err(
                    CorporateActionEndpointError::ReservedReplayQueryParameter(
                        value
                    )
                ) if value == parameter
            ));
        }
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

    #[test]
    fn reconnect_honors_retry_after_up_to_the_cap() {
        assert_eq!(
            honor_retry_after(STREAM_RECONNECT_MIN_BACKOFF, None),
            STREAM_RECONNECT_MIN_BACKOFF
        );
        assert_eq!(
            honor_retry_after(
                STREAM_RECONNECT_MIN_BACKOFF,
                Some(Duration::from_secs(90))
            ),
            Duration::from_secs(90)
        );
        assert_eq!(
            honor_retry_after(
                STREAM_RECONNECT_MIN_BACKOFF,
                Some(Duration::from_hours(24))
            ),
            STREAM_RECONNECT_MAX_RETRY_AFTER
        );
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
            CorporateActionDecodeBatch::Complete(mutations) => mutations
                .into_iter()
                .map(|mutation| projection_mutation(mutation).unwrap())
                .collect(),
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
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
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
        drop(feed_error);
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
            !decoder.has_pending_frame(),
            "an oversized untrusted frame must not remain buffered"
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
        assert!(!decoder.has_pending_frame());
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
        drop(feed_error);
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
        assert!(!decoder.has_pending_frame());
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
        drop(feed_error);
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
        drop(feed_error);
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
        drop(feed_error);
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
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            underlying_store: harness.underlying_store.clone(),
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
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
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
        drop(error);
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
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
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
        drop(error);
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
    async fn replay_connection_refuses_eof_before_anchor() {
        let harness = TestHarness::new().await;
        let cursor_mutation = event(
            "01J9RPMV5TKB8WX3M4F1KZ7QH2",
            CorporateActionMutationKind::Insert,
            "ca-1",
            "2026-08-14",
        );
        apply_mutation(&harness.pool, &cursor_mutation).await.unwrap();
        let server = MockServer::start();
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .query_param("since_id", cursor_mutation.event_id.as_str());
            then.status(200)
                .header("content-type", "text/event-stream")
                .body("");
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let consumption =
            feed.consume_connection(Some(&cursor_mutation.event_id)).await;

        stream.assert();
        assert_eq!(consumption.progress, ConnectionProgress::Idle);
        assert!(consumption.result.is_err());
        drop(consumption);
        let blocked: (String, String) = sqlx::query_as(
            "SELECT event_id, reason FROM corporate_action_blocked_event WHERE singleton = 1",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(blocked.0, cursor_mutation.event_id.as_str());
        assert_eq!(blocked.1, BLOCKED_REASON_REPLAY_GAP);
        assert!(matches!(
            load_cursor(&harness.pool).await.unwrap_err(),
            CorporateActionProjectionError::BlockedReplayGap { event_id }
                if event_id == cursor_mutation.event_id
        ));
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
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
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
        drop(error);
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
        let bootstrap_since = "2026-08-01T00:00:00Z"
            .parse::<CorporateActionBootstrapSince>()
            .unwrap();
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
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret")
                .query_param("since_id", cursor_mutation.event_id.as_str())
                .query_param_missing("since")
                .query_param_missing("until");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: Some(bootstrap_since),
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
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
            client: test_stream_client(
                &format!("http://{address}/corporate-actions"),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            underlying_store: harness.underlying_store.clone(),
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
        drop(consumption);
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
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.establish_development_baseline().await.unwrap();

        assert_eq!(load_cursor(&harness.pool).await.unwrap(), Some(event_id));
    }

    #[tokio::test]
    #[traced_test]
    async fn authenticated_first_install_replays_from_explicit_timestamp() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let event_id =
            CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2").unwrap();
        let bootstrap_since = "2026-08-31T00:00:00Z"
            .parse::<CorporateActionBootstrapSince>()
            .unwrap();
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret")
                .query_param("since", "2026-08-31T00:00:00Z")
                .query_param_missing("since_id")
                .query_param_missing("until");
            then.status(200).header("content-type", "text/event-stream").body(
                sse_frame(
                    event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-bootstrap",
                    &UnderlyingSymbol::new("UNLISTED").unwrap(),
                    Utc::now().date_naive(),
                ),
            );
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: Some(bootstrap_since),
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let consumption = feed.consume_connection(None).await;

        stream.assert();
        consumption.result.unwrap();
        assert_eq!(load_cursor(&harness.pool).await.unwrap(), Some(event_id));
        assert!(!logs_contain("test-key"));
        assert!(!logs_contain("test-secret"));
    }

    #[tokio::test]
    #[traced_test]
    async fn authenticated_startup_replay_is_bounded_before_service_readiness()
    {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let bootstrap_since = "2026-08-31T00:00:00Z"
            .parse::<CorporateActionBootstrapSince>()
            .unwrap();
        let startup_cutoff =
            DateTime::parse_from_rfc3339("2026-09-01T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc);
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .header("APCA-API-KEY-ID", "test-key")
                .header("APCA-API-SECRET-KEY", "test-secret")
                .query_param("since", "2026-08-31T00:00:00Z")
                .query_param("until", "2026-09-01T23:59:59Z")
                .query_param_missing("since_id");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body("");
        });
        let live_event_id =
            CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2").unwrap();
        let live = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .query_param("since", "2026-09-01T23:59:59Z")
                .query_param_missing("since_id")
                .query_param_missing("until");
            then.status(200).header("content-type", "text/event-stream").body(
                sse_frame(
                    live_event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-after-empty-bootstrap",
                    &UnderlyingSymbol::new("UNLISTED").unwrap(),
                    Utc::now().date_naive(),
                ),
            );
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: Some(bootstrap_since),
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.establish_authenticated_baseline_at(startup_cutoff).await.unwrap();

        stream.assert();
        assert!(logs_contain_at!(
            Level::WARN,
            &[
                "Replaying a bounded Alpaca corporate-action window before service readiness",
                "mode=\"bounded_history\"",
                "since=2026-08-31T00:00:00Z",
                "until=2026-09-01T23:59:59Z"
            ]
        ));
        assert!(!logs_contain("test-key"));
        assert!(!logs_contain("test-secret"));
        assert_eq!(load_cursor(&harness.pool).await.unwrap(), None);
        feed.consume_connection(None).await.result.unwrap();
        live.assert();
        assert_eq!(
            load_cursor(&harness.pool).await.unwrap(),
            Some(live_event_id)
        );
    }

    #[tokio::test]
    async fn authenticated_startup_replay_keeps_original_bound_after_committing_cursor()
     {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let event_id =
            CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2").unwrap();
        let bootstrap_since = "2026-08-31T00:00:00Z"
            .parse::<CorporateActionBootstrapSince>()
            .unwrap();
        let startup_cutoff =
            DateTime::parse_from_rfc3339("2026-09-01T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc);
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .query_param("since", "2026-08-31T00:00:00Z")
                .query_param("until", "2026-09-01T23:59:59Z")
                .query_param_missing("since_id");
            then.status(200).header("content-type", "text/event-stream").body(
                sse_frame(
                    event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-bootstrap",
                    &UnderlyingSymbol::new("UNLISTED").unwrap(),
                    Utc::now().date_naive(),
                ),
            );
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: Some(bootstrap_since.clone()),
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.establish_authenticated_baseline_at(startup_cutoff).await.unwrap();

        stream.assert();
        assert_eq!(load_cursor(&harness.pool).await.unwrap(), Some(event_id));
        assert_eq!(feed.bootstrap_since, Some(bootstrap_since));
    }

    #[tokio::test]
    async fn authenticated_startup_refuses_eof_inside_an_sse_frame() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let stream = server.mock(|when, then| {
            when.method(GET)
                .path("/corporate-actions")
                .query_param("since", "2026-08-31T00:00:00Z")
                .query_param("until", "2026-09-01T23:59:59Z")
                .query_param_missing("since_id");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body("id: 01J9RPMV5TKB8WX3M4F1KZ7QH2\nevent: insert\ndata: {");
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: Some(
                "2026-08-31T00:00:00Z"
                    .parse::<CorporateActionBootstrapSince>()
                    .unwrap(),
            ),
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };
        let cutoff = DateTime::parse_from_rfc3339("2026-09-01T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);

        let error =
            feed.establish_authenticated_baseline_at(cutoff).await.unwrap_err();

        stream.assert();
        assert!(matches!(
            error,
            CorporateActionFeedError::BoundedReplayEndedMidFrame
        ));
        drop(error);
    }

    #[tokio::test]
    async fn authenticated_startup_refuses_a_durable_blocked_boundary() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let blocked_event_id = "01J9RPMV5TKB8WX3M4F1KZ7QH2";
        sqlx::query(
            "
            INSERT INTO corporate_action_blocked_event (
                singleton,
                event_id,
                reason
            )
            VALUES (1, ?, ?)
            ",
        )
        .bind(blocked_event_id)
        .bind(BLOCKED_REASON_POISON)
        .execute(&harness.pool)
        .await
        .unwrap();
        let unexpected_request = server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body("");
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: Some(
                "2026-08-31T00:00:00Z"
                    .parse::<CorporateActionBootstrapSince>()
                    .unwrap(),
            ),
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let error =
            feed.establish_startup_baseline_at(Utc::now()).await.unwrap_err();

        assert!(matches!(
            error,
            CorporateActionFeedError::Projection(
                CorporateActionProjectionError::BlockedPoison {
                    event_id: Some(ref event_id)
                }
            ) if event_id.as_str() == blocked_event_id
        ));
        drop(error);
        assert_eq!(unexpected_request.calls(), 0);
    }

    #[tokio::test]
    async fn accepted_active_mutation_is_aligned_before_connection_returns() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(harness.pool.clone())
                .build(())
                .await
                .unwrap();
        let server = MockServer::start();
        let event_id =
            CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2").unwrap();
        let stream = server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200).header("content-type", "text/event-stream").body(
                sse_frame(
                    event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-bootstrap-active",
                    &underlying,
                    Utc::now().date_naive(),
                ),
            );
        });
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store,
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.consume_connection(None).await.result.unwrap();

        stream.assert();
        assert_eq!(
            load_freeze_status(&harness.pool, &underlying).await.unwrap(),
            AssetStatus::Frozen,
            "connection completion must mean its active hold is aligned"
        );
    }

    #[tokio::test]
    async fn spawned_post_projection_failure_keeps_mint_admission_closed() {
        let harness = TestHarness::new().await;
        let underlying = harness.setup_account_and_asset().await.underlying;
        sqlx::query("DROP TABLE Jobs").execute(&harness.pool).await.unwrap();
        let server = MockServer::start();
        let event_id =
            CorporateActionEventId::new("01J9RPMV5TKB8WX3M4F1KZ7QH2").unwrap();
        let stream = server.mock(|when, then| {
            when.method(GET).path("/corporate-actions");
            then.status(200).header("content-type", "text/event-stream").body(
                sse_frame(
                    event_id.as_str(),
                    CorporateActionMutationKind::Insert,
                    "ca-schedule-failure",
                    &underlying,
                    Utc::now().date_naive(),
                ),
            );
        });
        let feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            ),
            stream_transport:
                CorporateActionStreamTransport::CredentialFreeDevelopment,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool.clone(),
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };
        let (service_shutdown, feed_shutdown) = watch::channel(false);
        let mut observed_shutdown = service_shutdown.subscribe();
        let task =
            spawn_corporate_action_feed(feed, feed_shutdown, service_shutdown);

        tokio::time::timeout(
            Duration::from_secs(1),
            observed_shutdown.changed(),
        )
        .await
        .expect("post-projection failure must request shutdown promptly")
        .expect("service shutdown sender must remain live");

        stream.assert();
        assert!(*observed_shutdown.borrow());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(25),
                acquire_freeze_admission(),
            )
            .await
            .is_err(),
            "production failure handling must keep mint admission closed"
        );
        task.abort();
        let _ = task.await;
        tokio::time::timeout(
            Duration::from_secs(1),
            acquire_freeze_admission(),
        )
        .await
        .expect(
            "aborting the failed feed during shutdown must release admission",
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn authenticated_feed_without_cursor_or_bootstrap_stays_disabled() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        feed.run().await.unwrap();

        assert!(logs_contain_at!(
            Level::INFO,
            &[
                "Corporate-action feed remains disabled without a cursor or explicit bootstrap boundary",
                "state=\"disabled\"",
                "reason=\"baseline_required\""
            ]
        ));
    }

    #[tokio::test]
    async fn initial_connection_refuses_incomplete_historical_replay() {
        let harness = TestHarness::new().await;
        let server = MockServer::start();
        let mut feed = CorporateActionFeed {
            client: test_stream_client(
                &format!("{}/corporate-actions", server.base_url()),
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            ),
            stream_transport:
                CorporateActionStreamTransport::AuthenticatedAlpaca,
            bootstrap_since: None,
            pool: harness.pool.clone(),
            scheduler: CorporateActionFreezeScheduler::new(
                &harness.apalis_pool,
                harness.pool,
            ),
            underlying_store: harness.underlying_store.clone(),
            notifier: Arc::new(NoopLifecycleNotifier),
        };

        let error = feed.consume_connection(None).await.result.unwrap_err();

        assert!(matches!(error, CorporateActionFeedError::BaselineRequired));
        drop(error);
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
