//! HyperDX observability integration for trace export.
//!
//! This module provides optional OpenTelemetry trace export to HyperDX for real-time
//! monitoring and debugging of bot operations. When enabled via the `HYPERDX_API_KEY`
//! environment variable, traces are batched and exported to HyperDX. When disabled,
//! the bot runs normally with console-only logging.
//!
//! # Architecture
//!
//! Uses OpenTelemetry's [`BatchSpanProcessor`] for efficient trace export:
//! - **Batch size**: 512 spans per batch
//! - **Queue size**: 2048 spans maximum
//! - **Export interval**: 3 seconds
//! - **Protocol**: OTLP/HTTP (protobuf)
//!
//! ## Blocking HTTP Client Requirement
//!
//! **CRITICAL**: The [`BatchSpanProcessor`] spawns background threads that run outside
//! the tokio runtime. These threads require a blocking HTTP client (from `reqwest::blocking`)
//! rather than an async client. Using an async client would panic with "no reactor running"
//! because the background threads have no tokio runtime available.
//!
//! The blocking client is created in a separate thread to avoid blocking the main tokio
//! runtime during initialization.
//!
//! # Usage
//!
//! ```ignore
//! // Optional telemetry setup through config
//! let config = env::Env::parse().into_config();
//!
//! let telemetry_guard = if let Some(ref hyperdx) = config.hyperdx {
//!     match hyperdx.setup_telemetry() {
//!         Ok(guard) => Some(guard),
//!         Err(e) => {
//!             eprintln!("Failed to setup telemetry: {e}");
//!             None
//!         }
//!     }
//! } else {
//!     None
//! };
//!
//! // Bot runs normally regardless of telemetry availability
//! // ...
//!
//! // Guard flushes remaining traces on drop
//! drop(telemetry_guard);
//! ```
//!
//! # Trace Filtering
//!
//! The module sets up per-layer filtering to control what gets logged to console vs
//! exported to HyperDX:
//! - Console layer: Respects `RUST_LOG` environment variable, defaults to bot crates only
//! - Telemetry layer: Independent filter, defaults to bot crates only
//!
//! This prevents external crate spam (e.g., from `alloy`, `rocket`) from cluttering
//! traces while still allowing those logs in console if needed.

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{
    ExporterBuildError, Protocol, SpanExporter, WithExportConfig,
    WithHttpConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider,
};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use thiserror::Error;
use tracing::Level;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Layer, SubscriberExt};

use crate::config::LogFormat;

/// Build the console fmt layer for `log_format`. The JSON arm is the single
/// place the console JSON wire shape is defined, so every subscriber emits
/// one JSON shape.
pub(crate) fn console_fmt_layer<S>(
    log_format: LogFormat,
    env_filter: tracing_subscriber::EnvFilter,
) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber
        + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    match log_format {
        LogFormat::Text => {
            tracing_subscriber::fmt::layer().with_filter(env_filter).boxed()
        }
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_filter(env_filter)
            .boxed(),
    }
}

#[derive(Clone, Debug)]
pub struct HyperDxConfig {
    pub(crate) api_key: HyperDxApiKey,
    pub(crate) service_name: String,
    pub(crate) log_level: Level,
    pub(crate) log_format: LogFormat,
}

/// HyperDX ingestion API key.
///
/// `Debug` renders `<redacted>` (matching the `AlpacaConfig` marker) so the
/// key cannot leak through debug-formatting of any struct that holds it —
/// redaction travels with the value instead of relying on each container's
/// hand-written `Debug` impl.
#[derive(Clone)]
pub struct HyperDxApiKey(String);

impl HyperDxApiKey {
    pub(crate) const fn new(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Debug for HyperDxApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl HyperDxConfig {
    pub fn setup_telemetry(&self) -> Result<TelemetryGuard, TelemetryError> {
        let headers = HashMap::from([(
            "authorization".to_string(),
            self.api_key.0.clone(),
        )]);

        let http_client = std::thread::spawn(|| {
            reqwest::blocking::Client::builder()
                .gzip(true)
                .build()
                .map_err(|e| format!("Failed to build HTTP client: {e}"))
        })
        .join()
        .map_err(|_| TelemetryError::ThreadSpawn)?
        .map_err(TelemetryError::HttpClient)?;

        // HyperDX ingests OTLP over HTTP at in-otel.hyperdx.io, with the API
        // key passed bare (no Bearer prefix) in the `authorization` header
        // (https://www.hyperdx.io/docs/install/opentelemetry). `/v1/traces` is
        // the standard OTLP/HTTP traces path; the exporter is built
        // `.with_http()`, so the wire format is OTLP/HTTP protobuf.
        let otlp_exporter = SpanExporter::builder()
            .with_http()
            .with_http_client(http_client)
            .with_endpoint("https://in-otel.hyperdx.io/v1/traces")
            .with_headers(headers)
            .with_protocol(Protocol::HttpBinary)
            .build()?;

        let batch_exporter = BatchSpanProcessor::builder(otlp_exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_export_batch_size(512)
                    .with_max_queue_size(2048)
                    .with_scheduled_delay(Duration::from_secs(3))
                    .build(),
            )
            .build();

        let tracer_provider = SdkTracerProvider::builder()
            .with_span_processor(batch_exporter)
            .with_resource(
                Resource::builder()
                    .with_service_name(self.service_name.clone())
                    .with_attributes(vec![KeyValue::new(
                        "deployment.environment",
                        "production",
                    )])
                    .build(),
            )
            .build();

        let tracer = tracer_provider.tracer(TRACER_NAME);

        let telemetry_layer =
            tracing_opentelemetry::layer().with_tracer(tracer);

        let default_filter = crate::config::default_log_filter(self.log_level);

        let fmt_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| default_filter.clone().into());

        let telemetry_filter =
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into());

        let fmt_layer = console_fmt_layer(self.log_format, fmt_filter);
        let telemetry_layer = telemetry_layer.with_filter(telemetry_filter);

        let subscriber =
            Registry::default().with(fmt_layer).with(telemetry_layer);

        tracing::subscriber::set_global_default(subscriber)?;

        Ok(TelemetryGuard { tracer_provider })
    }
}

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("Failed to build OTLP exporter")]
    OtlpExporter(#[from] ExporterBuildError),

    #[error("Failed to build HTTP client")]
    HttpClient(String),

    #[error("Failed to spawn HTTP client thread")]
    ThreadSpawn,

    #[error("Failed to set global subscriber")]
    Subscriber(#[from] tracing::subscriber::SetGlobalDefaultError),
}

pub struct TelemetryGuard {
    tracer_provider: SdkTracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Flush any pending spans to HyperDX before shutdown.
        // This blocks until all pending exports complete or timeout.
        // The BatchSpanProcessor uses scheduled_delay (3s) as its export interval,
        // and force_flush waits for pending exports with its own internal timeout.
        if let Err(err) = self.tracer_provider.force_flush() {
            eprintln!("Failed to flush telemetry spans: {err:?}");
        }

        // Shutdown the tracer provider to clean up background threads and resources.
        // This ensures the BatchSpanProcessor's background thread terminates cleanly.
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("Failed to shutdown telemetry provider: {err:?}");
        }
    }
}

/// Instrumentation library name used to identify the source of traces in the
/// OpenTelemetry system. This appears in telemetry backends as the library
/// that generated the spans.
///
/// This is distinct from the service name:
/// - Service name (e.g., "st0x-issuance"): Identifies which service the traces
///   come from in a distributed system. Shows as `service.name` resource
///   attribute.
/// - Tracer name (this constant): Identifies which instrumentation library
///   within the service created the spans. Used to distinguish between
///   application code ("st0x_tracer") and auto-instrumented libraries
///   (e.g., "reqwest", "sqlx").
///
/// Since we use a single tracer for all application code without library
/// auto-instrumentation, this distinction is somewhat artificial but
/// maintained for semantic clarity.
const TRACER_NAME: &str = "st0x-tracer";

#[cfg(test)]
mod tests {
    use std::io::Write;
    use tracing::Level;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    use super::{HyperDxApiKey, HyperDxConfig, LogFormat};

    #[test]
    fn hyperdx_config_debug_redacts_api_key() {
        let config = HyperDxConfig {
            api_key: HyperDxApiKey::new("super-secret-hyperdx-key".to_string()),
            service_name: "issuer".to_string(),
            log_level: Level::INFO,
            log_format: LogFormat::Text,
        };

        let rendered = format!("{config:?}");

        assert!(
            !rendered.contains("super-secret-hyperdx-key"),
            "Debug output must not leak the HyperDX API key: {rendered}"
        );
        assert!(
            rendered.contains("<redacted>"),
            "Debug output should redact the api_key: {rendered}"
        );
    }

    /// Captures everything written through a subscriber layer so tests can
    /// assert on the emitted bytes.
    #[derive(Clone, Default)]
    struct SharedWriter(std::sync::Arc<parking_lot::Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Pins the JSON console wire shape a log shipper parses: one JSON object
    /// per line carrying `timestamp`, `level`, `target`, the event fields
    /// under `fields`, and span fields under `span`. The same shape as the
    /// liquidity bot emits, so the shipper has one parse contract for both
    /// bots. A tracing subscriber upgrade that changes this shape must fail
    /// here, not in the shipper.
    #[test]
    fn json_console_layer_emits_one_parseable_object_per_line() {
        let writer = SharedWriter::default();
        let layer =
            tracing_subscriber::fmt::layer().json().with_writer(writer.clone());
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span =
                tracing::info_span!("redeem", issuer_request_id = "abc-123");
            let _entered = span.enter();
            tracing::info!(target: "redemption", "json shape pin");
        });

        let bytes = writer.0.lock().clone();
        let output = std::str::from_utf8(&bytes).unwrap();
        let line = output.lines().next().expect("one log line was emitted");

        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(entry["timestamp"].is_string(), "missing timestamp: {entry}");
        assert_eq!(entry["level"], "INFO");
        assert_eq!(entry["target"], "redemption");
        assert_eq!(entry["fields"]["message"], "json shape pin");
        assert_eq!(entry["span"]["issuer_request_id"], "abc-123");
    }
}
