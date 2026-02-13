//! ObservabilityWrapper - Unified observability and metrics initialization
//!
//! This module consolidates common observability, metrics, and tracing patterns
//! used across Simple, Transactional, and V2 Coordinator processors.
//!
//! It provides:
//! - Unified initialization of observability infrastructure
//! - Shared metrics collection and aggregation
//! - Consistent error tracking across processors
//! - Optional Dead Letter Queue (DLQ) support for failed record capture
//!
//! ## Dead Letter Queue (DLQ) Integration
//!
//! The ObservabilityWrapper manages the optional Dead Letter Queue which captures
//! failed records for inspection, debugging, and recovery:
//!
//! ### When to Enable DLQ
//! - **LogAndContinue strategy**: Records processed individually; failed records preserved
//! - **SendToDLQ strategy**: Batch-level failures sent to DLQ for retry
//! - **RetryWithBackoff**: Records exceeding max retries added to DLQ
//! - **FailBatch strategy**: DLQ should be disabled (batch rolls back atomically)
//!
//! ### Key Features
//! - **Capacity Management**: Optional size limits prevent unbounded growth
//! - **Atomic Operations**: Thread-safe entry addition and retrieval
//! - **Error Context Preservation**: Full record data and error messages stored
//! - **Metrics Integration**: Tracks entries added, rejected, and health status
//! - **Recovery Support**: Enables re-processing of failed records
//!
//! ### Usage Examples
//!
//! Enable DLQ for SimpleJobProcessor:
//! ```ignore
//! let wrapper = ObservabilityWrapper::with_dlq();
//! if let Some(dlq) = wrapper.dlq() {
//!     dlq.add_entry(record, error_msg, record_index, true).await;
//! }
//! ```
//!
//! Enable DLQ with observability:
//! ```ignore
//! let wrapper = ObservabilityWrapper::with_observability_and_dlq(Some(obs_manager));
//! let dlq = wrapper.dlq();  // Returns Option<&Arc<DeadLetterQueue>>
//! ```
//!
//! Check DLQ health:
//! ```ignore
//! if let Some(dlq) = wrapper.dlq() {
//!     let size = dlq.len().await;
//!     let capacity = dlq.capacity_usage_percent();
//!     let at_capacity = dlq.is_at_capacity();
//! }
//! ```

use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::observability::async_queue::ObservabilityQueue;
use crate::velostream::observability::background_flusher::BackgroundFlusher;
use crate::velostream::server::processors::common::DeadLetterQueue;
use crate::velostream::server::processors::error_tracking_helper::ErrorTracker;
use crate::velostream::server::processors::metrics_collector::MetricsCollector;
use crate::velostream::server::processors::metrics_helper::ProcessorMetricsHelper;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::StreamingQuery;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Unified observability and metrics wrapper for all processors
///
/// This struct consolidates all observability-related fields and provides
/// a single initialization point for metrics, tracing, and dead-letter handling.
///
/// # Features
/// - Optional observability/tracing support (for span creation and propagation)
/// - Unified metrics collection (counter, gauge, histogram via ProcessorMetricsHelper)
/// - Runtime metrics aggregation (via MetricsCollector)
/// - Optional Dead Letter Queue for failed record handling
/// - Thread-safe via Arc for shared access
///
/// # Initialization Patterns
///
/// Basic initialization (no observability):
/// ```ignore
/// let wrapper = ObservabilityWrapper::new();
/// ```
///
/// With observability/tracing:
/// ```ignore
/// let wrapper = ObservabilityWrapper::with_observability(Some(obs_manager));
/// ```
///
/// With Dead Letter Queue (non-transactional processors only):
/// ```ignore
/// let wrapper = ObservabilityWrapper::builder()
///     .with_observability(Some(obs_manager))
///     .with_dlq(true)
///     .build();
/// ```
#[derive(Clone)]
pub struct ObservabilityWrapper {
    /// Optional observability manager for tracing and span propagation
    observability: Option<SharedObservabilityManager>,

    /// SQL-annotated metrics helper for SQL query metrics
    /// Handles counter, gauge, and histogram metrics from SQL annotations
    metrics_helper: Arc<ProcessorMetricsHelper>,

    /// Runtime metrics collector for batch-level and record-level metrics
    metrics_collector: Arc<MetricsCollector>,

    /// Optional Dead Letter Queue for failed records
    /// Only used in non-transactional processors (Simple)
    dlq: Option<Arc<DeadLetterQueue>>,

    /// Async observability queue for non-blocking metrics and traces (Phase 4)
    /// When enabled, metrics flush and trace export happen in background tasks
    observability_queue: Option<Arc<ObservabilityQueue>>,

    /// Background flusher managing async flush tasks (Phase 4)
    /// Handles graceful shutdown and final flush
    background_flusher: Option<Arc<Mutex<Option<BackgroundFlusher>>>>,
}

impl ObservabilityWrapper {
    /// Create a new wrapper with default settings (no observability, no DLQ)
    pub fn new() -> Self {
        Self {
            observability: None,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: None,
            observability_queue: None,
            background_flusher: None,
        }
    }

    /// Create a new wrapper with observability support
    pub fn with_observability(observability: Option<SharedObservabilityManager>) -> Self {
        Self {
            observability,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: None,
            observability_queue: None,
            background_flusher: None,
        }
    }

    /// Create a new wrapper with DLQ support (for non-transactional processors)
    pub fn with_dlq() -> Self {
        Self {
            observability: None,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: Some(Arc::new(DeadLetterQueue::new())),
            observability_queue: None,
            background_flusher: None,
        }
    }

    /// Create a new wrapper with both observability and DLQ
    pub fn with_observability_and_dlq(observability: Option<SharedObservabilityManager>) -> Self {
        Self {
            observability,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: Some(Arc::new(DeadLetterQueue::new())),
            observability_queue: None,
            background_flusher: None,
        }
    }

    /// Create a new wrapper with observability and async queue support (Phase 4)
    ///
    /// Initializes the async observability queue and starts background flush tasks.
    /// This enables non-blocking metrics and trace submission.
    ///
    /// # Arguments
    ///
    /// * `observability` - Optional observability manager
    /// * `config` - Queue configuration (sizes, flush intervals, retry settings)
    ///
    /// # Returns
    ///
    /// ObservabilityWrapper with async queue enabled and optional queue-based tracing
    ///
    /// # Arguments
    ///
    /// * `observability` - Optional observability manager (for existing metrics/telemetry)
    /// * `tracing_config` - Optional tracing config for queue-based span export (recommended)
    /// * `config` - Queue configuration
    ///
    /// # Queue-Based Tracing
    ///
    /// If `tracing_config` is provided, a new queue-based TelemetryProvider will be created
    /// that exports spans asynchronously without blocking the processing loop.
    ///
    /// # Example
    /// ```ignore
    /// use velostream::observability::queue_config::ObservabilityQueueConfig;
    /// use velostream::sql::execution::config::TracingConfig;
    ///
    /// // With queue-based tracing (recommended)
    /// let tracing_config = TracingConfig {
    ///     service_name: "my_service".to_string(),
    ///     otlp_endpoint: Some("http://tempo:4317".to_string()),
    ///     sampling_ratio: 1.0,
    ///     ..Default::default()
    /// };
    ///
    /// let queue_config = ObservabilityQueueConfig::default();
    /// let wrapper = ObservabilityWrapper::with_observability_and_async_queue(
    ///     Some(obs_manager),
    ///     Some(tracing_config),
    ///     queue_config,
    /// ).await;
    /// ```
    pub async fn with_observability_and_async_queue(
        observability: Option<SharedObservabilityManager>,
        tracing_config: Option<crate::velostream::sql::execution::config::TracingConfig>,
        config: crate::velostream::observability::queue_config::ObservabilityQueueConfig,
    ) -> Self {
        use crate::velostream::observability::async_queue::ObservabilityQueue;
        use crate::velostream::observability::background_flusher::BackgroundFlusher;

        // Create async queue
        let (queue, receivers) = ObservabilityQueue::new(config.clone());
        let queue = Arc::new(queue);

        // Create queue-based telemetry if tracing config is provided
        let span_exporter = if let Some(tracing_cfg) = tracing_config {
            match crate::velostream::observability::telemetry::TelemetryProvider::new_with_queue(
                tracing_cfg,
                queue.clone(),
            )
            .await
            {
                Ok((_telemetry, exporter)) => {
                    log::info!("✅ Queue-based telemetry initialized for async span export");
                    exporter
                }
                Err(e) => {
                    log::error!("❌ Failed to create queue-based telemetry: {:?}", e);
                    None
                }
            }
        } else {
            log::info!("ℹ️  No tracing config provided, span export disabled");
            None
        };

        // Extract metrics provider from observability if available
        // Note: MetricsProvider is not cloneable, so we cannot extract it from ObservabilityManager
        // For full queue integration with metrics, use with_custom_providers() instead
        let metrics_provider: Option<
            Arc<crate::velostream::observability::metrics::MetricsProvider>,
        > = None;

        // Start background flush tasks with span exporter
        let flusher =
            BackgroundFlusher::start(receivers, metrics_provider, span_exporter, config).await;

        Self {
            observability,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: None,
            observability_queue: Some(queue),
            background_flusher: Some(Arc::new(Mutex::new(Some(flusher)))),
        }
    }

    /// Create wrapper with async queue and custom providers (advanced usage)
    ///
    /// This variant allows passing pre-configured providers including span exporter
    /// for full queue-based observability integration.
    ///
    /// # Example with Queue-Based Telemetry
    /// ```ignore
    /// // Create queue-based telemetry
    /// let (queue, receivers) = ObservabilityQueue::new(config.clone());
    /// let (telemetry, exporter) = TelemetryProvider::new_with_queue(
    ///     tracing_config,
    ///     Arc::new(queue.clone()),
    /// ).await?;
    ///
    /// // Create wrapper with exporter
    /// let wrapper = ObservabilityWrapper::with_custom_providers(
    ///     observability,
    ///     Some(metrics_provider),
    ///     exporter,
    ///     config,
    /// ).await;
    /// ```
    pub async fn with_custom_providers(
        observability: Option<SharedObservabilityManager>,
        metrics_provider: Option<Arc<crate::velostream::observability::metrics::MetricsProvider>>,
        span_exporter: Option<Box<dyn opentelemetry_sdk::export::trace::SpanExporter>>,
        config: crate::velostream::observability::queue_config::ObservabilityQueueConfig,
    ) -> Self {
        use crate::velostream::observability::async_queue::ObservabilityQueue;
        use crate::velostream::observability::background_flusher::BackgroundFlusher;

        // Create async queue
        let (queue, receivers) = ObservabilityQueue::new(config.clone());
        let queue = Arc::new(queue);

        // Start background flush tasks with custom providers
        let flusher =
            BackgroundFlusher::start(receivers, metrics_provider, span_exporter, config).await;

        Self {
            observability,
            metrics_helper: Arc::new(ProcessorMetricsHelper::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: None,
            observability_queue: Some(queue),
            background_flusher: Some(Arc::new(Mutex::new(Some(flusher)))),
        }
    }

    /// Create a new builder for more complex configurations
    pub fn builder() -> ObservabilityWrapperBuilder {
        ObservabilityWrapperBuilder::new()
    }

    // ===== Accessors =====

    /// Get reference to observability manager (if present)
    pub fn observability(&self) -> Option<&SharedObservabilityManager> {
        self.observability.as_ref()
    }

    /// Check if observability is enabled
    pub fn has_observability(&self) -> bool {
        self.observability.is_some()
    }

    /// Get observability as reference for helper functions
    /// This returns &Option which is what ObservabilityHelper expects
    pub fn observability_ref(&self) -> &Option<SharedObservabilityManager> {
        &self.observability
    }

    /// Get reference to metrics helper
    pub fn metrics_helper(&self) -> &ProcessorMetricsHelper {
        &self.metrics_helper
    }

    /// Get reference to metrics collector
    pub fn metrics_collector(&self) -> &MetricsCollector {
        &self.metrics_collector
    }

    /// Check if DLQ is enabled
    ///
    /// # Returns
    /// `true` if DLQ is enabled, `false` otherwise
    ///
    /// # Example
    /// ```ignore
    /// if wrapper.has_dlq() {
    ///     println!("DLQ is enabled");
    /// }
    /// ```
    pub fn has_dlq(&self) -> bool {
        self.dlq.is_some()
    }

    /// Get reference to observability queue (if enabled)
    ///
    /// Returns None if async queue is not enabled, otherwise returns a reference to the
    /// ObservabilityQueue for non-blocking metrics and trace submission.
    ///
    /// # Returns
    /// `Option<&Arc<ObservabilityQueue>>` - Queue reference or None if disabled
    ///
    /// # Example
    /// ```ignore
    /// if let Some(queue) = wrapper.observability_queue() {
    ///     let event = MetricsEvent::Flush { batch, timestamp: Instant::now() };
    ///     if let Err(e) = queue.try_send_metrics(event) {
    ///         warn!("Metrics queue full, dropping batch");
    ///     }
    /// }
    /// ```
    pub fn observability_queue(&self) -> Option<&Arc<ObservabilityQueue>> {
        self.observability_queue.as_ref()
    }

    /// Check if observability queue is enabled
    pub fn has_observability_queue(&self) -> bool {
        self.observability_queue.is_some()
    }

    /// Get reference to DLQ (if enabled)
    ///
    /// Returns None if DLQ is not enabled, otherwise returns a reference to the
    /// Dead Letter Queue. The returned Arc allows shared ownership across threads.
    ///
    /// # Returns
    /// `Option<&Arc<DeadLetterQueue>>` - DLQ reference or None if disabled
    ///
    /// # Example: Adding Failed Record to DLQ
    /// ```ignore
    /// if let Some(dlq) = wrapper.dlq() {
    ///     let added = dlq.add_entry(
    ///         record,
    ///         "SQL execution error".to_string(),
    ///         record_index,
    ///         true  // recoverable
    ///     ).await;
    ///
    ///     if added {
    ///         debug!("Record added to DLQ");
    ///     } else {
    ///         error!("DLQ at capacity, entry rejected");
    ///     }
    /// }
    /// ```
    ///
    /// # Example: Checking DLQ Health
    /// ```ignore
    /// if let Some(dlq) = wrapper.dlq() {
    ///     let size = dlq.len().await;
    ///     let max = dlq.max_size();
    ///     let usage = dlq.capacity_usage_percent();
    ///
    ///     if dlq.is_at_capacity() {
    ///         error!("DLQ at maximum capacity!");
    ///     }
    /// }
    /// ```
    ///
    /// # Example: Retrieving Failed Records
    /// ```ignore
    /// if let Some(dlq) = wrapper.dlq() {
    ///     let entries = dlq.get_entries().await;
    ///     for entry in entries {
    ///         println!("Failed Record {}: {}", entry.record_index, entry.error_message);
    ///     }
    /// }
    /// ```
    ///
    /// # Processor Compatibility
    /// - SimpleJobProcessor: DLQ enabled by default
    /// - PartitionReceiver: DLQ enabled for debugging per-partition failures
    /// - TransactionalJobProcessor: DLQ disabled (FailBatch strategy, no partial failures)
    pub fn dlq(&self) -> Option<&Arc<DeadLetterQueue>> {
        self.dlq.as_ref()
    }

    // ===== Metrics Methods =====

    /// Get total records processed
    pub fn total_records_processed(&self) -> u64 {
        self.metrics_collector.total_records()
    }

    /// Get total records failed
    pub fn total_records_failed(&self) -> u64 {
        self.metrics_collector.failed_records()
    }

    /// Record successful record processing
    pub fn record_success(&self, count: u64) {
        self.metrics_collector.add_records(count);
    }

    /// Record failed record processing
    pub fn record_failure(&self, count: u64) {
        self.metrics_collector.add_failed_records(count);
    }

    /// Get a summary of all observability metrics
    pub fn get_summary(&self) -> ObservabilityMetricsSummary {
        ObservabilityMetricsSummary {
            records_processed: self.total_records_processed(),
            records_failed: self.total_records_failed(),
            has_observability: self.has_observability(),
            has_dlq: self.has_dlq(),
        }
    }

    // ===== SQL Metrics Convenience Methods =====

    /// Register all SQL-annotated metrics (counter, gauge, histogram) in a single call
    ///
    /// This consolidates the three separate registration calls into one method,
    /// reducing boilerplate in processor implementations.
    ///
    /// # Arguments
    /// * `query` - Streaming query containing @metric annotations
    /// * `job_name` - Job name for metric labeling
    ///
    /// # Returns
    /// * `Ok(())` if all metrics registered successfully
    /// * `Err(...)` if any registration fails
    ///
    /// # Example
    /// ```ignore
    /// // Before: 15+ lines
    /// let obs = self.observability_wrapper.observability().cloned();
    /// self.observability_wrapper.metrics_helper()
    ///     .register_counter_metrics(&query, &obs, job_name).await?;
    /// self.observability_wrapper.metrics_helper()
    ///     .register_gauge_metrics(&query, &obs, job_name).await?;
    /// self.observability_wrapper.metrics_helper()
    ///     .register_histogram_metrics(&query, &obs, job_name).await?;
    ///
    /// // After: 1 line
    /// self.observability_wrapper.register_all_metrics(&query, job_name).await?;
    /// ```
    pub async fn register_all_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Register all metric types (counter, gauge, histogram)
        self.metrics_helper
            .register_counter_metrics(query, &self.observability, job_name)
            .await?;

        self.metrics_helper
            .register_gauge_metrics(query, &self.observability, job_name)
            .await?;

        self.metrics_helper
            .register_histogram_metrics(query, &self.observability, job_name)
            .await
    }

    /// Emit all SQL-annotated metrics (counter, gauge, histogram) in a single call
    ///
    /// This consolidates the three separate emission calls into one method,
    /// reducing boilerplate and avoiding multiple observability clones.
    ///
    /// # Arguments
    /// * `query` - Streaming query containing @metric annotations
    /// * `output_records` - Records to extract metric values from
    /// * `job_name` - Job name for metric labeling
    ///
    /// # Example
    /// ```ignore
    /// // Before: 12+ lines
    /// let obs = self.observability_wrapper.observability().cloned();
    /// let queue = self.observability_wrapper.observability_queue().cloned();
    /// self.observability_wrapper.metrics_helper()
    ///     .emit_counter_metrics(&query, output_records, &obs, &queue, job_name).await;
    /// self.observability_wrapper.metrics_helper()
    ///     .emit_gauge_metrics(&query, output_records, &obs, &queue, job_name).await;
    /// self.observability_wrapper.metrics_helper()
    ///     .emit_histogram_metrics(&query, output_records, &obs, &queue, job_name).await;
    ///
    /// // After: 1 line
    /// self.observability_wrapper.emit_all_metrics(&query, output_records, job_name).await;
    /// ```
    pub async fn emit_all_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[Arc<StreamRecord>],
        job_name: &str,
    ) {
        // Emit all metric types (counter, gauge, histogram)
        self.metrics_helper
            .emit_counter_metrics(query, output_records, &self.observability, &self.observability_queue, job_name)
            .await;

        self.metrics_helper
            .emit_gauge_metrics(query, output_records, &self.observability, &self.observability_queue, job_name)
            .await;

        self.metrics_helper
            .emit_histogram_metrics(query, output_records, &self.observability, &self.observability_queue, job_name)
            .await;
    }

    // ===== Error Tracking Convenience Methods =====

    /// Record an error to the error tracker
    ///
    /// This consolidates the ErrorTracker::record_error call pattern,
    /// eliminating repetitive observability cloning in error paths.
    ///
    /// # Arguments
    /// * `job_name` - Job name for error context
    /// * `error_msg` - Error message to record
    ///
    /// # Example
    /// ```ignore
    /// // Before: 3 lines per error
    /// ErrorTracker::record_error(
    ///     &self.observability_wrapper.observability().cloned(),
    ///     job_name,
    ///     error_msg,
    /// );
    ///
    /// // After: 1 line
    /// self.observability_wrapper.record_error(job_name, error_msg);
    /// ```
    pub fn record_error(&self, job_name: &str, error_msg: String) {
        ErrorTracker::record_error(&self.observability, job_name, error_msg);
    }

    /// Shutdown async observability queue and background flusher (Phase 4)
    ///
    /// Performs graceful shutdown of background flush tasks, ensuring final flush
    /// of pending metrics and traces before termination.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for tasks to complete
    ///
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// wrapper.shutdown_async_queue(Duration::from_secs(5)).await?;
    /// ```
    pub async fn shutdown_async_queue(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), crate::velostream::sql::error::SqlError> {
        if let Some(flusher_arc) = &self.background_flusher {
            let mut flusher_guard = flusher_arc.lock().await;
            if let Some(flusher) = flusher_guard.take() {
                log::info!("🔄 Shutting down observability async queue...");
                flusher.shutdown(timeout).await?;
                log::info!("✅ Observability async queue shut down successfully");
            }
        }
        Ok(())
    }
}

impl Default for ObservabilityWrapper {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for ObservabilityWrapper with more control over initialization
pub struct ObservabilityWrapperBuilder {
    observability: Option<SharedObservabilityManager>,
    enable_dlq: bool,
    app_name: Option<String>,
}

impl ObservabilityWrapperBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            observability: None,
            enable_dlq: false,
            app_name: None,
        }
    }

    /// Set observability manager
    pub fn with_observability(mut self, obs: Option<SharedObservabilityManager>) -> Self {
        self.observability = obs;
        self
    }

    /// Enable Dead Letter Queue
    pub fn with_dlq(mut self, enable: bool) -> Self {
        self.enable_dlq = enable;
        self
    }

    /// Set application name for `job` label injection in remote-write metrics
    pub fn with_app_name(mut self, app_name: Option<String>) -> Self {
        self.app_name = app_name;
        self
    }

    /// Build the wrapper
    pub fn build(self) -> ObservabilityWrapper {
        let mut helper = ProcessorMetricsHelper::new();
        if let Some(name) = self.app_name {
            helper.set_app_name(name);
        }
        ObservabilityWrapper {
            observability: self.observability,
            metrics_helper: Arc::new(helper),
            metrics_collector: Arc::new(MetricsCollector::new()),
            dlq: if self.enable_dlq {
                Some(Arc::new(DeadLetterQueue::new()))
            } else {
                None
            },
            observability_queue: None,
            background_flusher: None,
        }
    }
}

impl Default for ObservabilityWrapperBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of observability metrics
#[derive(Debug, Clone, Copy)]
pub struct ObservabilityMetricsSummary {
    pub records_processed: u64,
    pub records_failed: u64,
    pub has_observability: bool,
    pub has_dlq: bool,
}

impl std::fmt::Display for ObservabilityMetricsSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObservabilityMetricsSummary {{ records: {}/{}, observability: {}, dlq_enabled: {} }}",
            self.records_processed, self.records_failed, self.has_observability, self.has_dlq,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_wrapper_new() {
        let wrapper = ObservabilityWrapper::new();
        assert!(!wrapper.has_observability());
        assert!(!wrapper.has_dlq());
        assert_eq!(wrapper.total_records_processed(), 0);
    }

    #[test]
    fn test_observability_wrapper_with_dlq() {
        let wrapper = ObservabilityWrapper::with_dlq();
        assert!(!wrapper.has_observability());
        assert!(wrapper.has_dlq());
    }

    #[test]
    fn test_observability_wrapper_record_success() {
        let wrapper = ObservabilityWrapper::new();
        wrapper.record_success(10);
        assert_eq!(wrapper.total_records_processed(), 10);
    }

    #[test]
    fn test_observability_wrapper_record_failure() {
        let wrapper = ObservabilityWrapper::new();
        wrapper.record_failure(5);
        assert_eq!(wrapper.total_records_failed(), 5);
    }

    #[test]
    fn test_observability_wrapper_builder() {
        let wrapper = ObservabilityWrapper::builder().with_dlq(true).build();
        assert!(wrapper.has_dlq());
        assert!(!wrapper.has_observability());
    }

    #[test]
    fn test_observability_wrapper_cloneable() {
        let wrapper1 = ObservabilityWrapper::new();
        wrapper1.record_success(5);

        let wrapper2 = wrapper1.clone();
        // Both should share the same Arc references
        assert_eq!(wrapper2.total_records_processed(), 5);

        // Changes in one should be visible in the other
        wrapper2.record_success(3);
        assert_eq!(wrapper1.total_records_processed(), 8);
    }

    #[test]
    fn test_observability_metrics_summary() {
        let wrapper = ObservabilityWrapper::with_dlq();
        wrapper.record_success(100);
        wrapper.record_failure(5);

        let summary = wrapper.get_summary();
        assert_eq!(summary.records_processed, 100);
        assert_eq!(summary.records_failed, 5);
        assert!(summary.has_dlq);
        assert!(!summary.has_observability);
    }

    #[test]
    fn test_observability_wrapper_default() {
        let wrapper = ObservabilityWrapper::default();
        assert!(!wrapper.has_observability());
        assert!(!wrapper.has_dlq());
    }

    #[test]
    fn test_observability_wrapper_builder_default() {
        let wrapper = ObservabilityWrapperBuilder::default()
            .with_dlq(true)
            .build();
        assert!(wrapper.has_dlq());
    }

    #[test]
    fn test_observability_metrics_summary_display() {
        let summary = ObservabilityMetricsSummary {
            records_processed: 100,
            records_failed: 5,
            has_observability: true,
            has_dlq: true,
        };
        let display_str = summary.to_string();
        assert!(display_str.contains("100"));
        assert!(display_str.contains("5"));
        assert!(display_str.contains("dlq_enabled: true"));
    }
}
