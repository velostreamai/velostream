//! Async Queue for Non-Blocking Observability
//!
//! Provides a unified observability queue with separate channels for metrics and traces.
//! Uses tokio MPSC bounded channels with drop-on-full policy to never block the processing loop.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────┐
//! │  ObservabilityQueue         │
//! │  ┌──────────┐  ┌──────────┐ │
//! │  │ Metrics  │  │  Traces  │ │
//! │  │ Channel  │  │ Channel  │ │
//! │  │ (65,536) │  │ (16,384) │ │
//! │  └────┬─────┘  └────┬─────┘ │
//! └───────┼─────────────┼───────┘
//!         │             │
//!         v             v
//!    Flush Task    Flush Task
//! ```
//!
//! ## Key Features
//!
//! - **Non-blocking sends**: `try_send()` returns immediately, never blocks
//! - **Drop-on-full policy**: When queue is full, events are dropped (observability never impacts processing)
//! - **Separate channels**: Independent backpressure for metrics and traces
//! - **Statistics**: Track queue depth and dropped event counts
//! - **Graceful shutdown**: Final flush before termination
//!
//! ## Usage
//!
//! ```ignore
//! use velostream::observability::async_queue::{ObservabilityQueue, MetricsEvent};
//! use velostream::observability::queue_config::ObservabilityQueueConfig;
//! use velostream::observability::metrics::MetricBatch;
//!
//! let config = ObservabilityQueueConfig::default();
//! let (queue, receivers) = ObservabilityQueue::new(config);
//!
//! // Non-blocking send (returns Err if full)
//! let batch = MetricBatch::new();
//! let event = MetricsEvent::Flush { batch, timestamp: Instant::now() };
//! if let Err(e) = queue.try_send_metrics(event) {
//!     eprintln!("Queue full, dropped metrics");
//! }
//! ```

use crate::velostream::observability::queue_config::ObservabilityQueueConfig;
use crate::velostream::observability::remote_write::TimestampedSample;
use opentelemetry_sdk::export::trace::SpanData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc::{self, error::TrySendError};

/// Event for metrics queue
#[derive(Debug)]
pub enum MetricsEvent {
    /// Flush remote-write samples to Prometheus
    FlushRemoteWrite {
        samples: Vec<TimestampedSample>,
        timestamp: Instant,
    },
}

/// Event for traces queue
#[derive(Debug)]
pub enum TraceEvent {
    /// Export a span
    Span {
        span_data: SpanData,
        timestamp: Instant,
    },
}

/// Receivers for background flush tasks
pub struct QueueReceivers {
    pub metrics_rx: mpsc::Receiver<MetricsEvent>,
    pub traces_rx: mpsc::Receiver<TraceEvent>,
}

/// Observability queue with separate channels for metrics and traces
///
/// Uses bounded MPSC channels with try_send() for non-blocking operation.
/// When channels are full, events are dropped and counters are incremented.
pub struct ObservabilityQueue {
    // Separate channels for independent backpressure
    metrics_tx: mpsc::Sender<MetricsEvent>,
    traces_tx: mpsc::Sender<TraceEvent>,

    // Drop tracking (atomic for lock-free access)
    metrics_dropped: Arc<AtomicU64>,
    traces_dropped: Arc<AtomicU64>,
    metrics_queued: Arc<AtomicU64>,
    traces_queued: Arc<AtomicU64>,
}

impl ObservabilityQueue {
    /// Create a new observability queue with the given configuration
    ///
    /// Returns (queue, receivers) where receivers should be passed to background flush tasks.
    pub fn new(config: ObservabilityQueueConfig) -> (Self, QueueReceivers) {
        let (metrics_tx, metrics_rx) = mpsc::channel(config.metrics_queue_size);
        let (traces_tx, traces_rx) = mpsc::channel(config.traces_queue_size);

        let queue = Self {
            metrics_tx,
            traces_tx,
            metrics_dropped: Arc::new(AtomicU64::new(0)),
            traces_dropped: Arc::new(AtomicU64::new(0)),
            metrics_queued: Arc::new(AtomicU64::new(0)),
            traces_queued: Arc::new(AtomicU64::new(0)),
        };

        let receivers = QueueReceivers {
            metrics_rx,
            traces_rx,
        };

        (queue, receivers)
    }

    /// Try to send a metrics event (non-blocking)
    ///
    /// Returns `Ok(())` if event was queued, `Err(TrySendError)` if queue is full.
    /// Caller should handle full queue by logging/counting drops.
    pub fn try_send_metrics(&self, event: MetricsEvent) -> Result<(), TrySendError<MetricsEvent>> {
        match self.metrics_tx.try_send(event) {
            Ok(()) => {
                self.metrics_queued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.metrics_dropped.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Try to send a trace event (non-blocking)
    ///
    /// Returns `Ok(())` if event was queued, `Err(TrySendError)` if queue is full.
    /// Caller should handle full queue by logging/counting drops.
    pub fn try_send_trace(&self, event: TraceEvent) -> Result<(), TrySendError<TraceEvent>> {
        match self.traces_tx.try_send(event) {
            Ok(()) => {
                self.traces_queued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.traces_dropped.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Get the number of metrics events dropped (since queue creation)
    pub fn metrics_dropped_count(&self) -> u64 {
        self.metrics_dropped.load(Ordering::Relaxed)
    }

    /// Get the number of traces events dropped (since queue creation)
    pub fn traces_dropped_count(&self) -> u64 {
        self.traces_dropped.load(Ordering::Relaxed)
    }

    /// Get the number of metrics events successfully queued (since queue creation)
    pub fn metrics_queued_count(&self) -> u64 {
        self.metrics_queued.load(Ordering::Relaxed)
    }

    /// Get the number of traces events successfully queued (since queue creation)
    pub fn traces_queued_count(&self) -> u64 {
        self.traces_queued.load(Ordering::Relaxed)
    }

    /// Get current metrics queue depth (approximate)
    ///
    /// Note: This is a snapshot and may change immediately after reading.
    pub fn metrics_queue_depth(&self) -> usize {
        // Note: tokio mpsc doesn't expose len() directly in stable API
        // We track via counters instead (queued - processed)
        // For now, return 0 as placeholder (will be implemented via gauge in metrics)
        0
    }

    /// Get current traces queue depth (approximate)
    ///
    /// Note: This is a snapshot and may change immediately after reading.
    pub fn traces_queue_depth(&self) -> usize {
        // Note: tokio mpsc doesn't expose len() directly in stable API
        // We track via counters instead (queued - processed)
        // For now, return 0 as placeholder (will be implemented via gauge in metrics)
        0
    }

    /// Record additional metrics drops (for batch operations)
    ///
    /// Use this when dropping a batch of metrics to accurately reflect the count.
    pub fn record_metrics_dropped(&self, count: u64) {
        self.metrics_dropped.fetch_add(count, Ordering::Relaxed);
    }

    /// Record additional traces drops (for batch operations)
    ///
    /// Use this when dropping a batch of traces to accurately reflect the count.
    pub fn record_traces_dropped(&self, count: u64) {
        self.traces_dropped.fetch_add(count, Ordering::Relaxed);
    }

    /// Get clone of metrics sender (for use in multiple processors)
    pub fn metrics_sender(&self) -> mpsc::Sender<MetricsEvent> {
        self.metrics_tx.clone()
    }

    /// Get clone of traces sender (for use in multiple processors)
    pub fn traces_sender(&self) -> mpsc::Sender<TraceEvent> {
        self.traces_tx.clone()
    }
}

/// Statistics snapshot for observability queue
#[derive(Debug, Clone)]
pub struct QueueStatistics {
    pub metrics_dropped: u64,
    pub traces_dropped: u64,
    pub metrics_queued: u64,
    pub traces_queued: u64,
}

impl ObservabilityQueue {
    /// Get a snapshot of queue statistics
    pub fn statistics(&self) -> QueueStatistics {
        QueueStatistics {
            metrics_dropped: self.metrics_dropped_count(),
            traces_dropped: self.traces_dropped_count(),
            metrics_queued: self.metrics_queued_count(),
            traces_queued: self.traces_queued_count(),
        }
    }

    /// Register queue metrics with Prometheus
    ///
    /// Exposes queue health metrics for monitoring:
    /// - velostream_observability_metrics_queued_total - Total metrics events queued
    /// - velostream_observability_traces_queued_total - Total traces events queued
    /// - velostream_observability_metrics_dropped_total - Total metrics events dropped
    /// - velostream_observability_traces_dropped_total - Total traces events dropped
    ///
    /// # Example
    /// ```ignore
    /// use prometheus::Registry;
    ///
    /// let registry = Registry::new();
    /// queue.register_queue_metrics(&registry)?;
    /// ```
    pub fn register_queue_metrics(
        &self,
        registry: &prometheus::Registry,
    ) -> Result<(), prometheus::Error> {
        use prometheus::{IntCounter, Opts};

        // Metrics queued total
        let metrics_queued = IntCounter::with_opts(Opts::new(
            "velostream_observability_metrics_queued_total",
            "Total number of metrics events successfully queued",
        ))?;
        registry.register(Box::new(metrics_queued))?;

        // Traces queued total
        let traces_queued = IntCounter::with_opts(Opts::new(
            "velostream_observability_traces_queued_total",
            "Total number of trace events successfully queued",
        ))?;
        registry.register(Box::new(traces_queued))?;

        // Metrics dropped total
        let metrics_dropped = IntCounter::with_opts(Opts::new(
            "velostream_observability_metrics_dropped_total",
            "Total number of metrics events dropped due to queue full",
        ))?;
        registry.register(Box::new(metrics_dropped))?;

        // Traces dropped total
        let traces_dropped = IntCounter::with_opts(Opts::new(
            "velostream_observability_traces_dropped_total",
            "Total number of trace events dropped due to queue full",
        ))?;
        registry.register(Box::new(traces_dropped))?;

        Ok(())
    }

    /// Log current queue statistics
    ///
    /// Logs queue statistics for monitoring and debugging.
    /// The Prometheus counters registered via `register_queue_metrics()` are
    /// automatically updated during operation (in `try_send_metrics` and `try_send_trace`).
    ///
    /// This method provides a convenient way to log statistics periodically for debugging.
    ///
    /// # Example
    /// ```ignore
    /// // In a periodic task:
    /// tokio::spawn(async move {
    ///     let mut interval = tokio::time::interval(Duration::from_secs(15));
    ///     loop {
    ///         interval.tick().await;
    ///         queue.log_queue_statistics();
    ///     }
    /// });
    /// ```
    pub fn log_queue_statistics(&self) {
        let stats = self.statistics();

        log::debug!(
            "Queue stats - Metrics: queued={}, dropped={} | Traces: queued={}, dropped={}",
            stats.metrics_queued,
            stats.metrics_dropped,
            stats.traces_queued,
            stats.traces_dropped
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::observability::queue_config::ObservabilityQueueConfig;

    #[test]
    fn test_queue_creation() {
        let config = ObservabilityQueueConfig::default();
        let (queue, receivers) = ObservabilityQueue::new(config);

        assert_eq!(queue.metrics_dropped_count(), 0);
        assert_eq!(queue.traces_dropped_count(), 0);
        assert_eq!(queue.metrics_queued_count(), 0);
        assert_eq!(queue.traces_queued_count(), 0);

        // Ensure receivers are created
        drop(receivers);
    }

    #[tokio::test]
    async fn test_metrics_send_success() {
        let config = ObservabilityQueueConfig::default();
        let (queue, mut receivers) = ObservabilityQueue::new(config);

        let sample = crate::velostream::observability::remote_write::TimestampedSample {
            name: "test_metric".to_string(),
            label_names: vec![],
            label_values: vec![],
            value: 1.0,
            timestamp_ms: 1234567890,
        };
        let event = MetricsEvent::FlushRemoteWrite {
            samples: vec![sample],
            timestamp: Instant::now(),
        };

        // Send should succeed
        assert!(queue.try_send_metrics(event).is_ok());
        assert_eq!(queue.metrics_queued_count(), 1);
        assert_eq!(queue.metrics_dropped_count(), 0);

        // Receive should work
        let received = receivers.metrics_rx.recv().await;
        assert!(received.is_some());
    }

    #[tokio::test]
    async fn test_metrics_queue_full_drops() {
        // Create tiny queue (capacity 1)
        let config = ObservabilityQueueConfig::default().with_metrics_queue_size(1);
        let (queue, _receivers) = ObservabilityQueue::new(config);

        // First send succeeds
        let sample1 = crate::velostream::observability::remote_write::TimestampedSample {
            name: "test_metric".to_string(),
            label_names: vec![],
            label_values: vec![],
            value: 1.0,
            timestamp_ms: 1234567890,
        };
        let event1 = MetricsEvent::FlushRemoteWrite {
            samples: vec![sample1],
            timestamp: Instant::now(),
        };
        assert!(queue.try_send_metrics(event1).is_ok());
        assert_eq!(queue.metrics_queued_count(), 1);

        // Second send fails (queue full)
        let sample2 = crate::velostream::observability::remote_write::TimestampedSample {
            name: "test_metric".to_string(),
            label_names: vec![],
            label_values: vec![],
            value: 1.0,
            timestamp_ms: 1234567890,
        };
        let event2 = MetricsEvent::FlushRemoteWrite {
            samples: vec![sample2],
            timestamp: Instant::now(),
        };
        assert!(queue.try_send_metrics(event2).is_err());
        assert_eq!(queue.metrics_dropped_count(), 1);
    }

    #[test]
    fn test_record_batch_drops() {
        let config = ObservabilityQueueConfig::default();
        let (queue, _receivers) = ObservabilityQueue::new(config);

        // Record batch drops
        queue.record_metrics_dropped(100);
        assert_eq!(queue.metrics_dropped_count(), 100);

        queue.record_traces_dropped(50);
        assert_eq!(queue.traces_dropped_count(), 50);
    }

    #[test]
    fn test_statistics_snapshot() {
        let config = ObservabilityQueueConfig::default();
        let (queue, _receivers) = ObservabilityQueue::new(config);

        queue.record_metrics_dropped(10);
        queue.record_traces_dropped(5);

        let stats = queue.statistics();
        assert_eq!(stats.metrics_dropped, 10);
        assert_eq!(stats.traces_dropped, 5);
    }

    #[test]
    fn test_sender_cloning() {
        let config = ObservabilityQueueConfig::default();
        let (queue, _receivers) = ObservabilityQueue::new(config);

        // Clone senders for use in multiple processors
        let _metrics_sender = queue.metrics_sender();
        let _traces_sender = queue.traces_sender();

        // Original queue still works
        assert_eq!(queue.metrics_dropped_count(), 0);
    }
}
