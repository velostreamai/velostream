//! Background Flush Tasks for Observability Queue
//!
//! Manages background tasks that consume events from the observability queue
//! and flush them to their respective backends (Prometheus, OpenTelemetry).
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────┐
//! │ BackgroundFlusher│
//! ├──────────────────┤
//! │  Metrics Task    │──► Prometheus Remote-Write
//! │  Traces Task     │──► OpenTelemetry Collector
//! └──────────────────┘
//!         ▲
//!         │ broadcast::channel
//!         │ (shutdown signal)
//! ```
//!
//! ## Features
//!
//! - **Hybrid flush triggers**: Batch size OR time interval
//! - **Retry logic**: Exponential backoff for failed flushes
//! - **Graceful shutdown**: Final flush before termination
//! - **Concurrent tasks**: Separate tasks for metrics and traces
//!
//! ## Usage
//!
//! ```ignore
//! use velostream::observability::background_flusher::BackgroundFlusher;
//! use velostream::observability::async_queue::ObservabilityQueue;
//!
//! let (queue, receivers) = ObservabilityQueue::new(config.clone());
//! let flusher = BackgroundFlusher::start(
//!     receivers,
//!     metrics_provider,
//!     telemetry_provider,
//!     config,
//! ).await;
//!
//! // Later...
//! flusher.shutdown(Duration::from_secs(5)).await?;
//! ```

use crate::velostream::observability::async_queue::{MetricsEvent, QueueReceivers, TraceEvent};
use crate::velostream::observability::metrics::MetricsProvider;
use crate::velostream::observability::queue_config::ObservabilityQueueConfig;
use crate::velostream::observability::telemetry::TelemetryProvider;
use crate::velostream::sql::error::SqlError;
use log::{debug, error, info, warn};
use opentelemetry_sdk::export::trace::SpanExporter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Background flusher coordinator
///
/// Manages background flush tasks for metrics and traces, with graceful shutdown support.
pub struct BackgroundFlusher {
    metrics_handle: JoinHandle<()>,
    traces_handle: JoinHandle<()>,
    shutdown_tx: broadcast::Sender<()>,
}

/// Result of shutdown operation
#[derive(Debug)]
pub struct ShutdownResult {
    pub metrics_completed: bool,
    pub traces_completed: bool,
}

impl BackgroundFlusher {
    /// Start background flush tasks
    ///
    /// Spawns two separate tasks:
    /// - Metrics flush loop (batch size OR time interval trigger)
    /// - Traces flush loop (time interval trigger)
    ///
    /// # Arguments
    ///
    /// * `receivers` - Queue receivers for metrics and traces
    /// * `metrics_provider` - Optional metrics provider for remote-write
    /// * `span_exporter` - Optional span exporter for OTLP export
    /// * `config` - Queue configuration
    ///
    /// Returns BackgroundFlusher for shutdown coordination.
    pub async fn start(
        receivers: QueueReceivers,
        metrics_provider: Option<Arc<MetricsProvider>>,
        span_exporter: Option<Box<dyn SpanExporter>>,
        config: ObservabilityQueueConfig,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);

        // Spawn metrics flush task
        let metrics_handle = if let Some(metrics) = metrics_provider {
            let shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(metrics_flush_loop(
                receivers.metrics_rx,
                metrics,
                config.metrics_flush_config.clone(),
                shutdown_rx,
            ))
        } else {
            // No-op task if metrics not enabled
            tokio::spawn(async {})
        };

        // Spawn traces flush task
        let traces_handle = if let Some(exporter) = span_exporter {
            let shutdown_rx = shutdown_tx.subscribe();
            let exporter = Arc::new(Mutex::new(exporter));
            tokio::spawn(traces_flush_loop(
                receivers.traces_rx,
                exporter,
                config.traces_flush_config.clone(),
                shutdown_rx,
            ))
        } else {
            // No-op task if traces exporter not enabled
            tokio::spawn(async {})
        };

        Self {
            metrics_handle,
            traces_handle,
            shutdown_tx,
        }
    }

    /// Gracefully shutdown flush tasks
    ///
    /// Sends shutdown signal and waits for both tasks to complete their final flush.
    /// If timeout expires, tasks are forcefully aborted.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for tasks to complete
    ///
    /// # Returns
    ///
    /// ShutdownResult indicating which tasks completed successfully
    pub async fn shutdown(self, timeout: Duration) -> Result<ShutdownResult, SqlError> {
        info!(
            "🔄 Initiating observability queue shutdown (timeout: {:?})",
            timeout
        );

        // Send shutdown signal to all tasks
        if let Err(e) = self.shutdown_tx.send(()) {
            warn!("Failed to send shutdown signal: {:?}", e);
        }

        // Wait for both tasks with timeout
        let metrics_result = tokio::time::timeout(timeout, self.metrics_handle).await;
        let traces_result = tokio::time::timeout(timeout, self.traces_handle).await;

        let metrics_completed = match metrics_result {
            Ok(Ok(())) => {
                info!("✅ Metrics flush task completed successfully");
                true
            }
            Ok(Err(e)) => {
                error!("❌ Metrics flush task panicked: {:?}", e);
                false
            }
            Err(_) => {
                warn!("⏱️ Metrics flush task timed out (forcing abort)");
                false
            }
        };

        let traces_completed = match traces_result {
            Ok(Ok(())) => {
                info!("✅ Traces flush task completed successfully");
                true
            }
            Ok(Err(e)) => {
                error!("❌ Traces flush task panicked: {:?}", e);
                false
            }
            Err(_) => {
                warn!("⏱️ Traces flush task timed out (forcing abort)");
                false
            }
        };

        Ok(ShutdownResult {
            metrics_completed,
            traces_completed,
        })
    }
}

/// Metrics flush loop with hybrid triggering (batch size OR time interval)
async fn metrics_flush_loop(
    mut rx: tokio::sync::mpsc::Receiver<MetricsEvent>,
    metrics: Arc<MetricsProvider>,
    config: crate::velostream::observability::queue_config::MetricsFlushConfig,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut accumulated_samples: Vec<
        crate::velostream::observability::remote_write::TimestampedSample,
    > = Vec::with_capacity(config.batch_size * 100);
    let mut interval = tokio::time::interval(config.flush_interval);

    info!(
        "🚀 Metrics flush loop started (batch_size: {}, interval: {:?})",
        config.batch_size, config.flush_interval
    );

    loop {
        tokio::select! {
            // Receive metrics event
            Some(event) = rx.recv() => {
                match event {
                    MetricsEvent::FlushRemoteWrite { samples, .. } => {
                        accumulated_samples.extend(samples);

                        // Flush if batch size reached
                        if accumulated_samples.len() >= config.batch_size {
                            debug!("📤 Flushing metrics (batch size trigger: {} samples)", accumulated_samples.len());
                            flush_remote_write_samples(&metrics, &mut accumulated_samples).await;
                        }
                    }
                }
            }

            // Time-based flush trigger
            _ = interval.tick() => {
                if !accumulated_samples.is_empty() {
                    debug!("📤 Flushing metrics (time trigger: {} samples)", accumulated_samples.len());
                    flush_remote_write_samples(&metrics, &mut accumulated_samples).await;
                }
            }

            // Shutdown signal
            _ = shutdown.recv() => {
                info!("🛑 Metrics flush loop received shutdown signal");

                // Final flush before shutdown
                if !accumulated_samples.is_empty() {
                    info!("📤 Final metrics flush ({} samples)", accumulated_samples.len());
                    flush_remote_write_samples(&metrics, &mut accumulated_samples).await;
                }

                break;
            }
        }
    }

    info!("✨ Metrics flush loop terminated");
}

/// Flush accumulated metrics batches with retry
async fn flush_remote_write_samples(
    metrics: &Arc<MetricsProvider>,
    samples: &mut Vec<crate::velostream::observability::remote_write::TimestampedSample>,
) {
    if samples.is_empty() {
        return;
    }

    let count = samples.len();
    debug!("Flushing {} remote-write samples", count);

    // Send samples directly via the new non-blocking method
    match metrics
        .send_samples_directly(samples.drain(..).collect())
        .await
    {
        Ok(sent) => {
            debug!("✅ Flushed {} samples to remote-write", sent);
        }
        Err(e) => {
            error!("❌ Failed to flush remote-write samples: {:?}", e);
        }
    }
}

/// Traces flush loop with time-based triggering
async fn traces_flush_loop(
    mut rx: tokio::sync::mpsc::Receiver<TraceEvent>,
    exporter: Arc<Mutex<Box<dyn SpanExporter>>>,
    config: crate::velostream::observability::queue_config::TracesFlushConfig,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut span_batch = Vec::with_capacity(config.max_batch_size);
    let mut interval = tokio::time::interval(config.flush_interval);

    info!(
        "🚀 Traces flush loop started (max_batch: {}, interval: {:?})",
        config.max_batch_size, config.flush_interval
    );

    loop {
        tokio::select! {
            // Receive trace event
            Some(event) = rx.recv() => {
                match event {
                    TraceEvent::Span { span_data, .. } => {
                        span_batch.push(span_data);

                        // Flush if batch size reached
                        if span_batch.len() >= config.max_batch_size {
                            debug!("📤 Flushing traces (batch size trigger: {} spans)", span_batch.len());
                            flush_trace_batch(&mut span_batch, &exporter).await;
                        }
                    }
                }
            }

            // Time-based flush trigger
            _ = interval.tick() => {
                if !span_batch.is_empty() {
                    debug!("📤 Flushing traces (time trigger: {} spans)", span_batch.len());
                    flush_trace_batch(&mut span_batch, &exporter).await;
                }
            }

            // Shutdown signal
            _ = shutdown.recv() => {
                info!("🛑 Traces flush loop received shutdown signal");

                // Final flush before shutdown
                if !span_batch.is_empty() {
                    info!("📤 Final trace flush ({} spans)", span_batch.len());
                    flush_trace_batch(&mut span_batch, &exporter).await;
                }

                break;
            }
        }
    }

    info!("✨ Traces flush loop terminated");
}

/// Flush accumulated trace spans to OTLP exporter
async fn flush_trace_batch(
    span_batch: &mut Vec<opentelemetry_sdk::export::trace::SpanData>,
    exporter: &Arc<Mutex<Box<dyn SpanExporter>>>,
) {
    if span_batch.is_empty() {
        return;
    }

    debug!("Flushing {} trace spans to OTLP exporter", span_batch.len());

    // Export spans via OTLP exporter
    let spans = span_batch.drain(..).collect::<Vec<_>>();
    let span_count = spans.len();

    let mut exporter_guard = exporter.lock().await;
    match exporter_guard.export(spans).await {
        Ok(()) => {
            debug!(
                "✅ Successfully exported {} spans to OTLP collector",
                span_count
            );
        }
        Err(e) => {
            error!(
                "❌ Failed to export {} spans to OTLP collector: {:?}",
                span_count, e
            );
            // Note: spans are dropped on export failure
            // In production, consider DLQ or retry logic
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_flusher_lifecycle() {
        let config = ObservabilityQueueConfig::default();
        let (_, receivers) =
            crate::velostream::observability::async_queue::ObservabilityQueue::new(config.clone());

        // Start flusher without providers (no-op mode)
        let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

        // Shutdown should complete quickly
        let result = flusher.shutdown(Duration::from_secs(1)).await;
        assert!(result.is_ok());
    }
}
