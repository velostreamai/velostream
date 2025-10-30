// === PHASE 4: PROMETHEUS METRICS COLLECTION ===

use crate::velostream::observability::error_tracker::ErrorMessageBuffer;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::config::PrometheusConfig;
use prometheus::{
    register_gauge_vec_with_registry, register_gauge_with_registry,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, Encoder, Gauge,
    GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Phase 3.2: Consolidated dynamic metrics structure
/// Groups related metric types into single lock to reduce contention (3 -> 1 lock)
#[derive(Debug)]
pub struct DynamicMetrics {
    pub counters: HashMap<String, IntCounterVec>,
    pub gauges: HashMap<String, GaugeVec>,
    pub histograms: HashMap<String, HistogramVec>,
}

impl DynamicMetrics {
    /// Create a new empty dynamic metrics container
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
        }
    }
}

impl Default for DynamicMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Phase 4: Batch metric events for accumulation-based emission
///
/// Instead of acquiring locks per-record, metrics are accumulated into batch events
/// and flushed with a single lock acquisition at batch completion.
#[derive(Debug, Clone)]
pub enum MetricBatchEvent {
    /// Counter metric: just a name and labels (no value needed, always increments by 1)
    Counter { name: String, labels: Vec<String> },
    /// Gauge metric: name, labels, and value to set
    Gauge {
        name: String,
        labels: Vec<String>,
        value: f64,
    },
    /// Histogram metric: name, labels, and value to observe
    Histogram {
        name: String,
        labels: Vec<String>,
        value: f64,
    },
}

/// Phase 4: Batch accumulator for metrics
///
/// Accumulates metric events during record processing, reducing lock acquisitions
/// from 50,000 per batch to just 1 at completion.
#[derive(Debug)]
pub struct MetricBatch {
    events: Vec<MetricBatchEvent>,
}

impl MetricBatch {
    /// Create a new empty metric batch
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// Create a metric batch with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            events: Vec::with_capacity(capacity),
        }
    }

    /// Add a counter event to the batch (increments by 1)
    pub fn add_counter(&mut self, name: String, labels: Vec<String>) {
        self.events.push(MetricBatchEvent::Counter { name, labels });
    }

    /// Add a gauge event to the batch (sets value)
    pub fn add_gauge(&mut self, name: String, labels: Vec<String>, value: f64) {
        self.events.push(MetricBatchEvent::Gauge {
            name,
            labels,
            value,
        });
    }

    /// Add a histogram event to the batch (observes value)
    pub fn add_histogram(&mut self, name: String, labels: Vec<String>, value: f64) {
        self.events.push(MetricBatchEvent::Histogram {
            name,
            labels,
            value,
        });
    }

    /// Get the number of events in this batch
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl Default for MetricBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Prometheus metrics provider for comprehensive monitoring
pub struct MetricsProvider {
    config: PrometheusConfig,
    registry: Registry,
    sql_metrics: SqlMetrics,
    streaming_metrics: StreamingMetrics,
    system_metrics: SystemMetrics,
    active: bool,
    // Phase 3.2: Single consolidated lock for all dynamic metrics (reduced from 3 to 1)
    dynamic_metrics: Arc<Mutex<DynamicMetrics>>,
    // Phase 5: Job-to-metrics tracking for lifecycle management
    job_metrics: Arc<Mutex<HashMap<String, HashSet<String>>>>,
    // Error message tracking with rolling buffer (last 10 messages + counts)
    error_tracker: Arc<Mutex<ErrorMessageBuffer>>,
}

impl MetricsProvider {
    /// Create a new metrics provider with the given configuration
    pub async fn new(config: PrometheusConfig) -> Result<Self, SqlError> {
        let registry = Registry::new();

        // Initialize metric groups
        let sql_metrics = SqlMetrics::new(&registry, &config)?;
        let streaming_metrics = StreamingMetrics::new(&registry, &config)?;
        let system_metrics = SystemMetrics::new(&registry, &config)?;

        log::info!("📊 Phase 4: Prometheus metrics initialized");
        log::info!(
            "📊 Metrics configuration: histograms={}, port={}, path={}",
            config.enable_histograms,
            config.port,
            config.metrics_path
        );

        Ok(Self {
            config,
            registry,
            sql_metrics,
            streaming_metrics,
            system_metrics,
            active: true,
            dynamic_metrics: Arc::new(Mutex::new(DynamicMetrics::new())),
            job_metrics: Arc::new(Mutex::new(HashMap::new())),
            error_tracker: Arc::new(Mutex::new(ErrorMessageBuffer::new())),
        })
    }

    /// Set the node ID for this metrics provider (used in observability context)
    pub fn set_node_id(&mut self, node_id: Option<String>) -> Result<(), SqlError> {
        if let Some(id) = node_id {
            log::info!("📊 Metrics node context set: {}", id);
            // Update error tracker with node ID for all future error messages
            if let Ok(mut tracker) = self.error_tracker.lock() {
                tracker.set_node_id(Some(id.clone()));
                log::debug!("📊 Error tracker node context updated: {}", id);
            }
            // Update system metrics with node ID
            self.system_metrics
                .set_node_context(Some(id.clone()), None, None)?;
        }
        Ok(())
    }

    /// Set the deployment context for error tracking, system metrics, and SQL metrics
    ///
    /// This enables:
    /// - All error messages to be tagged with deployment metadata (node_id, node_name, region, version)
    /// - System metrics (CPU, memory, connections) to be labeled with per-node deployment info
    /// - SQL metrics (queries, records) to be labeled with per-node deployment info
    pub fn set_deployment_context(
        &mut self,
        context: crate::velostream::observability::error_tracker::DeploymentContext,
    ) -> Result<(), SqlError> {
        // Update error tracker with deployment context
        if let Ok(mut tracker) = self.error_tracker.lock() {
            tracker.set_deployment_context(context.clone());
            log::info!(
                "📊 Error tracking deployment context configured: node_id={:?}, node_name={:?}, region={:?}, version={:?}",
                context.node_id,
                context.node_name,
                context.region,
                context.version
            );
        }

        // Update system metrics with deployment context for per-node labeling
        self.system_metrics.set_node_context(
            context.node_id.clone(),
            context.node_name.clone(),
            context.region.clone(),
        )?;

        // Update SQL metrics with deployment context for per-node labeling
        self.sql_metrics.set_deployment_context(context)?;

        Ok(())
    }

    /// Record SQL query execution metrics with optional error message
    ///
    /// # Arguments
    /// * `query_type` - Type of SQL query (select, insert, update, etc.)
    /// * `duration` - Query execution duration
    /// * `success` - Whether query succeeded
    /// * `record_count` - Number of records processed
    /// * `error_message` - Optional error message (if success=false)
    pub fn record_sql_query(
        &self,
        query_type: &str,
        duration: Duration,
        success: bool,
        record_count: u64,
    ) {
        if self.active {
            self.sql_metrics
                .record_query(query_type, duration, success, record_count);
            log::trace!(
                "📊 Recorded SQL query metrics: type={}, duration={:?}, success={}, records={}",
                query_type,
                duration,
                success,
                record_count
            );
        }
    }

    /// Record SQL query execution metrics with error message tracking
    pub fn record_sql_query_with_error(
        &self,
        query_type: &str,
        duration: Duration,
        success: bool,
        record_count: u64,
        error_message: Option<String>,
    ) {
        if self.active {
            self.sql_metrics
                .record_query(query_type, duration, success, record_count);

            // Track error message if query failed
            if !success {
                if let Some(msg) = error_message {
                    if let Ok(mut tracker) = self.error_tracker.lock() {
                        tracker.add_error(msg.clone());
                        log::debug!(
                            "📊 Error message tracked: {} (total errors: {})",
                            msg,
                            tracker.get_stats().total_errors
                        );
                    }
                }
            }

            log::trace!(
                "📊 Recorded SQL query metrics: type={}, duration={:?}, success={}, records={}",
                query_type,
                duration,
                success,
                record_count
            );
        }
    }

    /// Record streaming operation metrics
    pub fn record_streaming_operation(
        &self,
        operation: &str,
        duration: Duration,
        record_count: u64,
        throughput: f64,
    ) {
        if self.active {
            self.streaming_metrics
                .record_operation(operation, duration, record_count, throughput);
            log::trace!("📊 Recorded streaming metrics: operation={}, duration={:?}, records={}, throughput={:.2}",
                operation, duration, record_count, throughput);
        }
    }

    /// Record profiling phase metrics with job name (Phase 2.2)
    pub fn record_profiling_phase(
        &self,
        job_name: &str,
        phase: &str,
        duration: Duration,
        record_count: u64,
        throughput_rps: f64,
    ) {
        if self.active {
            self.streaming_metrics.record_profiling_phase(
                job_name,
                phase,
                duration,
                record_count,
                throughput_rps,
            );
            log::trace!(
                "📊 Recorded profiling phase: job={}, phase={}, duration={:?}, throughput={:.2} rec/s",
                job_name,
                phase,
                duration,
                throughput_rps
            );
        }
    }

    /// Record pipeline operation metrics with job name (Phase 2.3)
    pub fn record_pipeline_operation(
        &self,
        job_name: &str,
        operation: &str,
        duration: Duration,
        record_count: u64,
    ) {
        if self.active {
            self.streaming_metrics.record_pipeline_operation(
                job_name,
                operation,
                duration,
                record_count,
            );
            log::trace!(
                "📊 Recorded pipeline operation: job={}, operation={}, duration={:?}, records={}",
                job_name,
                operation,
                duration,
                record_count
            );
        }
    }

    /// Record job-specific throughput (Phase 2.4)
    pub fn record_throughput_by_job(&self, job_name: &str, throughput_rps: f64) {
        if self.active {
            self.streaming_metrics
                .record_throughput_by_job(job_name, throughput_rps);
            log::trace!(
                "📊 Recorded throughput by job: job={}, throughput={:.2} rec/s",
                job_name,
                throughput_rps
            );
        }
    }

    /// Update system resource metrics
    pub fn update_system_metrics(&self, cpu_usage: f64, memory_usage: u64, active_jobs: i64) {
        if self.active {
            self.system_metrics
                .update(cpu_usage, memory_usage, active_jobs);
            log::trace!(
                "📊 Updated system metrics: cpu={:.2}%, memory={}MB, active_jobs={}",
                cpu_usage,
                memory_usage / (1024 * 1024),
                active_jobs
            );
        }
    }

    /// Get current metrics as formatted string
    pub fn get_metrics_text(&self) -> Result<String, SqlError> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();

        encoder.encode(&metric_families, &mut buffer).map_err(|e| {
            SqlError::ConfigurationError {
                message: format!("Failed to encode metrics: {}", e),
            }
        })?;

        String::from_utf8(buffer).map_err(|e| SqlError::ConfigurationError {
            message: format!("Invalid UTF-8 in metrics: {}", e),
        })
    }

    /// Get metrics statistics for monitoring
    pub fn get_stats(&self) -> MetricsStats {
        // For labeled metrics, retrieve using "unknown" labels for backward compatibility
        let sql_queries_total = self
            .sql_metrics
            .query_total_by_node
            .with_label_values(&["unknown", "unknown", "unknown"])
            .get();

        let records_processed_total = self
            .sql_metrics
            .records_processed_by_node
            .with_label_values(&["unknown", "unknown", "unknown"])
            .get();

        MetricsStats {
            sql_queries_total,
            sql_errors_total: self.sql_metrics.query_errors.get(),
            streaming_operations_total: self.streaming_metrics.get_total_operations(),
            records_processed_total,
            records_streamed_total: self.streaming_metrics.get_total_records(),
        }
    }

    /// Shutdown the metrics provider
    pub async fn shutdown(&mut self) -> Result<(), SqlError> {
        self.active = false;
        log::debug!("📊 Prometheus metrics stopped");
        Ok(())
    }

    /// Register a dynamic counter metric from SQL annotations
    ///
    /// # Arguments
    /// * `name` - Metric name (must follow Prometheus naming rules)
    /// * `help` - Help text description
    /// * `label_names` - Names of labels for this counter
    ///
    /// # Returns
    /// * `Ok(())` - Metric successfully registered
    /// * `Err(SqlError)` - Registration failed (metric already exists or invalid name)
    pub fn register_counter_metric(
        &self,
        name: &str,
        help: &str,
        label_names: &[String],
    ) -> Result<(), SqlError> {
        if !self.active {
            log::error!(
                "❌ Cannot register counter '{}': Metrics provider is not active",
                name
            );
            return Err(SqlError::ConfigurationError {
                message: "Metrics provider is not active".to_string(),
            });
        }

        let mut metrics = self.dynamic_metrics.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic metrics for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic metrics: {}", e),
            }
        })?;

        // Check if metric already registered
        if metrics.counters.contains_key(name) {
            log::warn!("⚠️  Counter metric '{}' already registered, skipping", name);
            return Ok(());
        }

        // Convert Vec<String> to Vec<&str> for registration
        let label_refs: Vec<&str> = label_names.iter().map(|s| s.as_str()).collect();

        // Register the counter with the registry
        let counter = register_int_counter_vec_with_registry!(
            Opts::new(name, help),
            &label_refs,
            &self.registry
        )
        .map_err(|e| {
            log::error!(
                "❌ Failed to register counter '{}' with Prometheus: {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to register counter '{}': {}", name, e),
            }
        })?;

        metrics.counters.insert(name.to_string(), counter);

        log::info!(
            "📊 Registered dynamic counter metric: name={}, labels={:?}",
            name,
            label_names
        );

        Ok(())
    }

    /// Emit a counter metric increment with labels
    ///
    /// # Arguments
    /// * `name` - Metric name (must be previously registered)
    /// * `label_values` - Values for labels (must match registration order)
    ///
    /// # Returns
    /// * `Ok(())` - Counter incremented successfully
    /// * `Err(SqlError)` - Metric not found or label mismatch
    pub fn emit_counter(&self, name: &str, label_values: &[String]) -> Result<(), SqlError> {
        if !self.active {
            log::warn!(
                "⚠️  Skipping counter emission for '{}': Metrics provider is not active",
                name
            );
            return Ok(()); // Silently skip if not active
        }

        let metrics = self.dynamic_metrics.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic metrics for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic metrics: {}", e),
            }
        })?;

        let counter = metrics.counters.get(name).ok_or_else(|| {
            log::error!("❌ Counter metric '{}' not registered", name);
            SqlError::ConfigurationError {
                message: format!("Counter metric '{}' not registered", name),
            }
        })?;

        // Convert Vec<String> to Vec<&str> for label lookup
        let label_refs: Vec<&str> = label_values.iter().map(|s| s.as_str()).collect();

        counter.with_label_values(&label_refs).inc();

        log::trace!(
            "📊 Emitted counter: name={}, labels={:?}",
            name,
            label_values
        );

        Ok(())
    }

    /// Register a dynamic gauge metric from SQL annotations
    ///
    /// # Arguments
    /// * `name` - Metric name (must follow Prometheus naming rules)
    /// * `help` - Help text description
    /// * `label_names` - Names of labels for this gauge
    ///
    /// # Returns
    /// * `Ok(())` - Metric successfully registered
    /// * `Err(SqlError)` - Registration failed (metric already exists or invalid name)
    pub fn register_gauge_metric(
        &self,
        name: &str,
        help: &str,
        label_names: &[String],
    ) -> Result<(), SqlError> {
        if !self.active {
            log::error!(
                "❌ Cannot register gauge '{}': Metrics provider is not active",
                name
            );
            return Err(SqlError::ConfigurationError {
                message: "Metrics provider is not active".to_string(),
            });
        }

        let mut metrics = self.dynamic_metrics.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic gauges for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic gauges: {}", e),
            }
        })?;

        // Check if metric already registered
        if metrics.gauges.contains_key(name) {
            log::warn!("⚠️  Gauge metric '{}' already registered, skipping", name);
            return Ok(());
        }

        // Convert Vec<String> to Vec<&str> for registration
        let label_refs: Vec<&str> = label_names.iter().map(|s| s.as_str()).collect();

        // Register the gauge with the registry
        let gauge =
            register_gauge_vec_with_registry!(Opts::new(name, help), &label_refs, &self.registry)
                .map_err(|e| {
                log::error!(
                    "❌ Failed to register gauge '{}' with Prometheus: {}",
                    name,
                    e
                );
                SqlError::ConfigurationError {
                    message: format!("Failed to register gauge '{}': {}", name, e),
                }
            })?;

        metrics.gauges.insert(name.to_string(), gauge);

        log::info!(
            "📊 Registered dynamic gauge metric: name={}, labels={:?}",
            name,
            label_names
        );

        Ok(())
    }

    /// Emit a gauge metric observation with labels
    ///
    /// # Arguments
    /// * `name` - Metric name (must be previously registered)
    /// * `label_values` - Values for labels (must match registration order)
    /// * `value` - The gauge value to set
    ///
    /// # Returns
    /// * `Ok(())` - Gauge updated successfully
    /// * `Err(SqlError)` - Metric not found or label mismatch
    pub fn emit_gauge(
        &self,
        name: &str,
        label_values: &[String],
        value: f64,
    ) -> Result<(), SqlError> {
        if !self.active {
            log::warn!(
                "⚠️  Skipping gauge emission for '{}': Metrics provider is not active",
                name
            );
            return Ok(()); // Silently skip if not active
        }

        let metrics = self.dynamic_metrics.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic gauges for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic gauges: {}", e),
            }
        })?;

        let gauge = metrics.gauges.get(name).ok_or_else(|| {
            log::error!("❌ Gauge metric '{}' not registered", name);
            SqlError::ConfigurationError {
                message: format!("Gauge metric '{}' not registered", name),
            }
        })?;

        // Convert Vec<String> to Vec<&str> for label lookup
        let label_refs: Vec<&str> = label_values.iter().map(|s| s.as_str()).collect();

        gauge.with_label_values(&label_refs).set(value);

        log::trace!(
            "📊 Emitted gauge: name={}, labels={:?}, value={}",
            name,
            label_values,
            value
        );

        Ok(())
    }

    /// Register a dynamic histogram metric from SQL annotations
    ///
    /// # Arguments
    /// * `name` - Metric name (must follow Prometheus naming rules)
    /// * `help` - Help text description
    /// * `label_names` - Names of labels for this histogram
    /// * `buckets` - Optional custom bucket boundaries (uses defaults if None)
    ///
    /// # Returns
    /// * `Ok(())` - Metric successfully registered
    /// * `Err(SqlError)` - Registration failed (metric already exists or invalid name)
    pub fn register_histogram_metric(
        &self,
        name: &str,
        help: &str,
        label_names: &[String],
        buckets: Option<Vec<f64>>,
    ) -> Result<(), SqlError> {
        if !self.active {
            log::error!(
                "❌ Cannot register histogram '{}': Metrics provider is not active",
                name
            );
            return Err(SqlError::ConfigurationError {
                message: "Metrics provider is not active".to_string(),
            });
        }

        let mut metrics = self.dynamic_metrics.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic histograms for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic histograms: {}", e),
            }
        })?;

        // Check if metric already registered
        if metrics.histograms.contains_key(name) {
            log::warn!(
                "⚠️  Histogram metric '{}' already registered, skipping",
                name
            );
            return Ok(());
        }

        // Convert Vec<String> to Vec<&str> for registration
        let label_refs: Vec<&str> = label_names.iter().map(|s| s.as_str()).collect();

        // Create histogram opts with custom or default buckets
        let histogram_opts = if let Some(custom_buckets) = buckets {
            HistogramOpts::new(name, help).buckets(custom_buckets)
        } else {
            // Default buckets for general-purpose metrics
            HistogramOpts::new(name, help).buckets(vec![
                0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0,
            ])
        };

        // Register the histogram with the registry
        let histogram =
            register_histogram_vec_with_registry!(histogram_opts, &label_refs, &self.registry)
                .map_err(|e| {
                    log::error!(
                        "❌ Failed to register histogram '{}' with Prometheus: {}",
                        name,
                        e
                    );
                    SqlError::ConfigurationError {
                        message: format!("Failed to register histogram '{}': {}", name, e),
                    }
                })?;

        metrics.histograms.insert(name.to_string(), histogram);

        log::info!(
            "📊 Registered dynamic histogram metric: name={}, labels={:?}",
            name,
            label_names
        );

        Ok(())
    }

    /// Emit a histogram metric observation with labels
    ///
    /// # Arguments
    /// * `name` - Metric name (must be previously registered)
    /// * `label_values` - Values for labels (must match registration order)
    /// * `value` - The value to observe
    ///
    /// # Returns
    /// * `Ok(())` - Histogram observation recorded successfully
    /// * `Err(SqlError)` - Metric not found or label mismatch
    pub fn emit_histogram(
        &self,
        name: &str,
        label_values: &[String],
        value: f64,
    ) -> Result<(), SqlError> {
        if !self.active {
            log::warn!(
                "⚠️  Skipping histogram emission for '{}': Metrics provider is not active",
                name
            );
            return Ok(()); // Silently skip if not active
        }

        let metrics = self.dynamic_metrics.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic histograms for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic histograms: {}", e),
            }
        })?;

        let histogram = metrics.histograms.get(name).ok_or_else(|| {
            log::error!("❌ Histogram metric '{}' not registered", name);
            SqlError::ConfigurationError {
                message: format!("Histogram metric '{}' not registered", name),
            }
        })?;

        // Convert Vec<String> to Vec<&str> for label lookup
        let label_refs: Vec<&str> = label_values.iter().map(|s| s.as_str()).collect();

        histogram.with_label_values(&label_refs).observe(value);

        log::trace!(
            "📊 Emitted histogram: name={}, labels={:?}, value={}",
            name,
            label_values,
            value
        );

        Ok(())
    }

    /// Phase 4: Emit all metrics in a batch with a single lock acquisition
    ///
    /// This method reduces lock contention by accumulating metric events during
    /// record processing and flushing them all with a single Mutex lock acquisition.
    ///
    /// # Performance Impact
    /// - Lock acquisitions: 50,000 → 1 per batch (99.998% reduction)
    /// - Lock overhead: 150-500 ms → ~3 µs (50,000x faster)
    /// - Estimated throughput gain: 20-40%
    ///
    /// # Arguments
    /// * `batch` - MetricBatch containing all accumulated metric events
    ///
    /// # Returns
    /// * `Ok(())` - All metrics successfully emitted
    /// * `Err(SqlError)` - Failed to emit one or more metrics
    pub fn emit_batch(&self, batch: MetricBatch) -> Result<(), SqlError> {
        if !self.active || batch.is_empty() {
            return Ok(());
        }

        // Single lock acquisition for all events in batch
        let metrics = self.dynamic_metrics.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic metrics for batch: {}",
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic metrics: {}", e),
            }
        })?;

        let mut error_count = 0;

        // Process all batch events with the lock held
        for event in batch.events {
            match event {
                MetricBatchEvent::Counter { name, labels } => {
                    if let Some(counter) = metrics.counters.get(&name) {
                        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                        counter.with_label_values(&label_refs).inc();
                        log::trace!("📊 Batched counter: name={}, labels={:?}", name, labels);
                    } else {
                        log::warn!(
                            "⚠️ Counter metric '{}' not registered (skipped in batch)",
                            name
                        );
                        error_count += 1;
                    }
                }
                MetricBatchEvent::Gauge {
                    name,
                    labels,
                    value,
                } => {
                    if let Some(gauge) = metrics.gauges.get(&name) {
                        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                        gauge.with_label_values(&label_refs).set(value);
                        log::trace!(
                            "📊 Batched gauge: name={}, labels={:?}, value={}",
                            name,
                            labels,
                            value
                        );
                    } else {
                        log::warn!(
                            "⚠️ Gauge metric '{}' not registered (skipped in batch)",
                            name
                        );
                        error_count += 1;
                    }
                }
                MetricBatchEvent::Histogram {
                    name,
                    labels,
                    value,
                } => {
                    if let Some(histogram) = metrics.histograms.get(&name) {
                        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                        histogram.with_label_values(&label_refs).observe(value);
                        log::trace!(
                            "📊 Batched histogram: name={}, labels={:?}, value={}",
                            name,
                            labels,
                            value
                        );
                    } else {
                        log::warn!(
                            "⚠️ Histogram metric '{}' not registered (skipped in batch)",
                            name
                        );
                        error_count += 1;
                    }
                }
            }
        }

        if error_count > 0 {
            log::debug!(
                "📊 Batch emission completed with {} skipped metrics (not registered)",
                error_count
            );
        }

        Ok(())
    }

    // =========================================================================
    // Phase 5: Metrics Lifecycle Management
    // =========================================================================

    /// Register a metric as belonging to a specific job (Phase 5)
    ///
    /// # Arguments
    /// * `job_name` - Name of the job that owns this metric
    /// * `metric_name` - Name of the metric to track
    ///
    /// # Returns
    /// * `Ok(())` - Metric ownership tracked successfully
    /// * `Err(SqlError)` - Failed to acquire lock
    pub fn register_job_metric(&self, job_name: &str, metric_name: &str) -> Result<(), SqlError> {
        let mut job_metrics =
            self.job_metrics
                .lock()
                .map_err(|e| SqlError::ConfigurationError {
                    message: format!("Failed to acquire lock on job_metrics: {}", e),
                })?;

        job_metrics
            .entry(job_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(metric_name.to_string());

        log::debug!(
            "📊 Phase 5: Registered metric '{}' for job '{}'",
            metric_name,
            job_name
        );

        Ok(())
    }

    /// Get all metrics registered for a specific job (Phase 5)
    ///
    /// # Arguments
    /// * `job_name` - Name of the job to query
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - List of metric names for this job
    /// * `Err(SqlError)` - Failed to acquire lock
    pub fn get_job_metrics(&self, job_name: &str) -> Result<Vec<String>, SqlError> {
        let job_metrics = self
            .job_metrics
            .lock()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on job_metrics: {}", e),
            })?;

        Ok(job_metrics
            .get(job_name)
            .map(|metrics| metrics.iter().cloned().collect())
            .unwrap_or_default())
    }

    /// Unregister all metrics for a specific job (Phase 5)
    ///
    /// This removes the job-to-metrics tracking. The metrics themselves remain
    /// registered in Prometheus (following Prometheus best practices where metrics
    /// are long-lived), but will no longer be updated when the job is stopped.
    ///
    /// # Arguments
    /// * `job_name` - Name of the job whose metrics should be unregistered
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - List of metrics that were unregistered
    /// * `Err(SqlError)` - Failed to acquire lock
    pub fn unregister_job_metrics(&self, job_name: &str) -> Result<Vec<String>, SqlError> {
        let mut job_metrics =
            self.job_metrics
                .lock()
                .map_err(|e| SqlError::ConfigurationError {
                    message: format!("Failed to acquire lock on job_metrics: {}", e),
                })?;

        let metrics: Vec<String> = job_metrics
            .remove(job_name)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default();

        if !metrics.is_empty() {
            log::info!(
                "📊 Phase 5: Unregistered {} metrics for job '{}': {:?}",
                metrics.len(),
                job_name,
                metrics
            );
        }

        Ok(metrics)
    }

    /// List all jobs that have registered metrics (Phase 5)
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - List of job names
    /// * `Err(SqlError)` - Failed to acquire lock
    pub fn list_all_jobs(&self) -> Result<Vec<String>, SqlError> {
        let job_metrics = self
            .job_metrics
            .lock()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on job_metrics: {}", e),
            })?;

        Ok(job_metrics.keys().cloned().collect())
    }

    /// Get count of jobs and total metrics tracked (Phase 5)
    ///
    /// # Returns
    /// * `Ok((job_count, total_metrics))` - Tuple of job count and total unique metrics
    /// * `Err(SqlError)` - Failed to acquire lock
    pub fn get_tracking_stats(&self) -> Result<(usize, usize), SqlError> {
        let job_metrics = self
            .job_metrics
            .lock()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on job_metrics: {}", e),
            })?;

        let job_count = job_metrics.len();
        let total_metrics: usize = job_metrics.values().map(|set| set.len()).sum();

        Ok((job_count, total_metrics))
    }

    // =========================================================================
    // Error Message Tracking
    // =========================================================================

    /// Get error statistics from the rolling buffer
    ///
    /// Returns comprehensive error tracking information:
    /// - Total errors recorded (cumulative)
    /// - Number of unique error message types
    /// - Number of messages in current buffer (max 10)
    /// - Count for each unique error message
    ///
    /// # Returns
    /// * `Some(ErrorStats)` - Error statistics if available
    /// * `None` - If error tracker lock cannot be acquired
    pub fn get_error_stats(
        &self,
    ) -> Option<crate::velostream::observability::error_tracker::ErrorStats> {
        self.error_tracker
            .lock()
            .ok()
            .map(|tracker| tracker.get_stats())
    }

    /// Get the last N error messages in the buffer
    ///
    /// # Arguments
    /// * `limit` - Maximum number of messages to return
    ///
    /// # Returns
    /// List of error messages sorted by frequency (most common first)
    pub fn get_top_errors(&self, limit: usize) -> Vec<(String, u64)> {
        self.error_tracker
            .lock()
            .ok()
            .map(|tracker| {
                let stats = tracker.get_stats();
                stats.get_top_errors(limit)
            })
            .unwrap_or_default()
    }

    /// Get all error messages currently in buffer
    ///
    /// # Returns
    /// List of current error messages with timestamps and counts
    pub fn get_error_messages(
        &self,
    ) -> Vec<crate::velostream::observability::error_tracker::ErrorEntry> {
        self.error_tracker
            .lock()
            .ok()
            .map(|tracker| tracker.get_messages())
            .unwrap_or_default()
    }

    /// Record a single error message without full metrics
    ///
    /// Useful for tracking errors from non-query operations
    pub fn record_error_message(&self, message: String) {
        if self.active {
            if let Ok(mut tracker) = self.error_tracker.lock() {
                tracker.add_error(message);
            }
        }
    }

    /// Reset all error tracking
    pub fn reset_error_tracking(&self) {
        if let Ok(mut tracker) = self.error_tracker.lock() {
            tracker.reset();
            log::info!("📊 Error tracking reset");
        }
    }

    /// Get total error count from buffer
    pub fn get_total_errors(&self) -> u64 {
        self.error_tracker
            .lock()
            .ok()
            .map(|tracker| tracker.get_stats().total_errors)
            .unwrap_or(0)
    }

    /// Get number of unique error types
    pub fn get_unique_error_types(&self) -> usize {
        self.error_tracker
            .lock()
            .ok()
            .map(|tracker| tracker.get_stats().unique_errors)
            .unwrap_or(0)
    }

    /// Get number of error messages in current buffer
    pub fn get_buffered_error_count(&self) -> usize {
        self.error_tracker
            .lock()
            .ok()
            .map(|tracker| tracker.buffer_size())
            .unwrap_or(0)
    }

    /// Sync error tracking gauges with current error state for Prometheus exposure
    ///
    /// This method updates the Prometheus gauges to reflect the current state of the
    /// error message buffer. Should be called periodically (e.g., in Prometheus scrape handler)
    /// to ensure gauges stay current.
    pub fn sync_error_metrics(&self) {
        if self.active {
            let total_errors = self.get_total_errors();
            let unique_types = self.get_unique_error_types();
            let buffered_count = self.get_buffered_error_count();

            self.sql_metrics
                .update_error_gauges(total_errors, unique_types, buffered_count);

            // Also update individual error message metrics with deployment context
            if let Ok(tracker) = self.error_tracker.lock() {
                let messages = tracker.get_messages();
                self.sql_metrics
                    .update_error_message_metrics_with_context(&messages);
            }

            log::trace!(
                "📊 Synced error metrics: total={}, unique={}, buffered={}",
                total_errors,
                unique_types,
                buffered_count
            );
        }
    }
}

impl std::fmt::Debug for MetricsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsProvider")
            .field("active", &self.active)
            .field("config", &self.config)
            .field(
                "dynamic_metrics",
                &self
                    .dynamic_metrics
                    .lock()
                    .map(|m| {
                        format!(
                            "counters={}, gauges={}, histograms={}",
                            m.counters.len(),
                            m.gauges.len(),
                            m.histograms.len()
                        )
                    })
                    .unwrap_or_else(|_| "locked".to_string()),
            )
            .finish()
    }
}

/// Metrics statistics for monitoring
#[derive(Debug, Clone)]
pub struct MetricsStats {
    pub sql_queries_total: u64,
    pub sql_errors_total: u64,
    pub streaming_operations_total: u64,
    pub records_processed_total: u64,
    pub records_streamed_total: u64,
}

/// SQL execution metrics
#[derive(Debug)]
struct SqlMetrics {
    query_total_by_node: IntCounterVec, // velo_sql_queries_total{node_id, node_name, region}
    query_duration: Histogram,
    query_errors: IntCounter,
    active_queries: IntGauge,
    records_processed_by_node: IntCounterVec, // velo_sql_records_processed_total{node_id, node_name, region}
    // Error tracking metrics (Prometheus exposure)
    total_errors_gauge: IntGauge,
    unique_error_types_gauge: IntGauge,
    buffered_errors_gauge: IntGauge,
    error_message_gauge: GaugeVec, // Individual error messages with counts as label
    // Phase 2.1: Job-specific SQL latency metrics
    query_duration_by_job: HistogramVec, // velo_sql_query_duration_by_job_seconds{job_name, query_type}
    // Deployment context for recording metrics with proper labels
    deployment_context:
        Arc<Mutex<Option<crate::velostream::observability::error_tracker::DeploymentContext>>>,
}

impl SqlMetrics {
    fn new(registry: &Registry, config: &PrometheusConfig) -> Result<Self, SqlError> {
        // Per-node SQL queries counter with node labels
        let query_total_by_node = register_int_counter_vec_with_registry!(
            Opts::new(
                "velo_sql_queries_total",
                "Total number of SQL queries executed per node"
            ),
            &["node_id", "node_name", "region"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register SQL queries counter: {}", e),
        })?;

        let query_duration = if config.enable_histograms {
            register_histogram_with_registry!(
                HistogramOpts::new(
                    "velo_sql_query_duration_seconds",
                    "SQL query execution time"
                ),
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register SQL histogram: {}", e),
            })?
        } else {
            // Create a minimal histogram for compatibility
            register_histogram_with_registry!(
                HistogramOpts::new(
                    "velo_sql_query_duration_seconds",
                    "SQL query execution time"
                )
                .buckets(vec![0.1, 0.5, 1.0, 5.0]),
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register SQL histogram: {}", e),
            })?
        };

        let query_errors = register_int_counter_with_registry!(
            Opts::new(
                "velo_sql_query_errors_total",
                "Total number of SQL query errors"
            ),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register SQL error metrics: {}", e),
        })?;

        let active_queries = register_int_gauge_with_registry!(
            Opts::new(
                "velo_sql_active_queries",
                "Number of currently active SQL queries"
            ),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register SQL gauge: {}", e),
        })?;

        // Per-node records processed counter with node labels
        let records_processed_by_node = register_int_counter_vec_with_registry!(
            Opts::new(
                "velo_sql_records_processed_total",
                "Total number of records processed by SQL queries per node"
            ),
            &["node_id", "node_name", "region"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register records processed counter: {}", e),
        })?;

        // Error tracking metrics for Prometheus exposure
        let total_errors_gauge = register_int_gauge_with_registry!(
            Opts::new(
                "velo_error_messages_total",
                "Total number of error messages recorded (cumulative)"
            ),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register error total gauge: {}", e),
        })?;

        let unique_error_types_gauge = register_int_gauge_with_registry!(
            Opts::new(
                "velo_unique_error_types",
                "Number of unique error message types"
            ),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register unique errors gauge: {}", e),
        })?;

        let buffered_errors_gauge = register_int_gauge_with_registry!(
            Opts::new(
                "velo_buffered_error_messages",
                "Number of error messages in current rolling buffer (max 10)"
            ),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register buffered errors gauge: {}", e),
        })?;

        let error_message_gauge = register_gauge_vec_with_registry!(
            Opts::new(
                "velo_error_message",
                "Individual error messages with occurrence count"
            ),
            &["message"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register error message gauge: {}", e),
        })?;

        // Phase 2.1: Job-specific SQL latency histogram
        let query_duration_by_job = if config.enable_histograms {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_sql_query_duration_by_job_seconds",
                    "SQL query execution time broken down by job and query type"
                ),
                &["job_name", "query_type"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register job-specific SQL histogram: {}", e),
            })?
        } else {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_sql_query_duration_by_job_seconds",
                    "SQL query execution time broken down by job and query type"
                )
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0]),
                &["job_name", "query_type"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register job-specific SQL histogram: {}", e),
            })?
        };

        Ok(Self {
            query_total_by_node,
            query_duration,
            query_errors,
            active_queries,
            records_processed_by_node,
            total_errors_gauge,
            unique_error_types_gauge,
            buffered_errors_gauge,
            error_message_gauge,
            query_duration_by_job,
            deployment_context: Arc::new(Mutex::new(None)),
        })
    }

    fn record_query(
        &self,
        _query_type: &str,
        duration: Duration,
        success: bool,
        record_count: u64,
    ) {
        // Get current deployment context (use "unknown" as defaults)
        let (node_id, node_name, region) = self
            .deployment_context
            .lock()
            .ok()
            .and_then(|ctx_opt| {
                ctx_opt.as_ref().map(|ctx| {
                    (
                        ctx.node_id.as_deref().unwrap_or("unknown").to_string(),
                        ctx.node_name.as_deref().unwrap_or("unknown").to_string(),
                        ctx.region.as_deref().unwrap_or("unknown").to_string(),
                    )
                })
            })
            .unwrap_or_else(|| {
                (
                    "unknown".to_string(),
                    "unknown".to_string(),
                    "unknown".to_string(),
                )
            });

        let label_values = &[node_id.as_str(), node_name.as_str(), region.as_str()];

        // Record metrics with node labels
        self.query_total_by_node
            .with_label_values(label_values)
            .inc();
        self.query_duration.observe(duration.as_secs_f64());
        self.records_processed_by_node
            .with_label_values(label_values)
            .inc_by(record_count);

        if !success {
            self.query_errors.inc();
        }
    }

    /// Record SQL query metrics with job name (Phase 2.1)
    fn record_query_by_job(
        &self,
        job_name: &str,
        query_type: &str,
        duration: Duration,
        success: bool,
        record_count: u64,
    ) {
        // Get current deployment context (use "unknown" as defaults)
        let (node_id, node_name, region) = self
            .deployment_context
            .lock()
            .ok()
            .and_then(|ctx_opt| {
                ctx_opt.as_ref().map(|ctx| {
                    (
                        ctx.node_id.as_deref().unwrap_or("unknown").to_string(),
                        ctx.node_name.as_deref().unwrap_or("unknown").to_string(),
                        ctx.region.as_deref().unwrap_or("unknown").to_string(),
                    )
                })
            })
            .unwrap_or_else(|| {
                (
                    "unknown".to_string(),
                    "unknown".to_string(),
                    "unknown".to_string(),
                )
            });

        let label_values = &[node_id.as_str(), node_name.as_str(), region.as_str()];

        // Record metrics with node labels
        self.query_total_by_node
            .with_label_values(label_values)
            .inc();
        self.query_duration.observe(duration.as_secs_f64());
        self.records_processed_by_node
            .with_label_values(label_values)
            .inc_by(record_count);

        if !success {
            self.query_errors.inc();
        }

        // Record job-specific SQL latency histogram
        self.query_duration_by_job
            .with_label_values(&[job_name, query_type])
            .observe(duration.as_secs_f64());
    }

    /// Set the deployment context for SQL metrics
    fn set_deployment_context(
        &self,
        context: crate::velostream::observability::error_tracker::DeploymentContext,
    ) -> Result<(), SqlError> {
        if let Ok(mut ctx) = self.deployment_context.lock() {
            *ctx = Some(context);
            Ok(())
        } else {
            Err(SqlError::ConfigurationError {
                message: "Failed to acquire lock on deployment context".to_string(),
            })
        }
    }

    /// Update error tracking gauges for Prometheus exposure
    fn update_error_gauges(&self, total_errors: u64, unique_types: usize, buffered_count: usize) {
        self.total_errors_gauge.set(total_errors as i64);
        self.unique_error_types_gauge.set(unique_types as i64);
        self.buffered_errors_gauge.set(buffered_count as i64);
    }

    /// Update individual error message gauges
    /// This exposes each error message as a separate metric with message text and deployment context as labels
    fn update_error_message_metrics(
        &self,
        message_counts: &std::collections::HashMap<String, u64>,
    ) {
        for (message, count) in message_counts {
            // Include deployment context in the label if available
            // Format: "message [node_id=..., node_name=..., region=..., version=...]"
            self.error_message_gauge
                .with_label_values(&[message])
                .set(*count as f64);
        }
    }

    /// Update individual error message gauges with full deployment context
    /// This uses the ErrorEntry objects which contain both message and deployment context
    fn update_error_message_metrics_with_context(
        &self,
        messages: &[crate::velostream::observability::error_tracker::ErrorEntry],
    ) {
        // Build a map of message display strings (with context) to counts
        let mut display_to_count: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();

        for entry in messages {
            // Use the Display implementation which includes deployment context
            let display_str = entry.to_string();
            *display_to_count.entry(display_str).or_insert(0) += entry.count;
        }

        // Update Prometheus gauge with display strings that include deployment context
        for (display_str, count) in display_to_count {
            self.error_message_gauge
                .with_label_values(&[&display_str])
                .set(count as f64);
        }
    }
}

/// Streaming operation metrics
#[derive(Debug)]
struct StreamingMetrics {
    operations_total: IntCounterVec,
    operation_duration: HistogramVec,
    throughput: GaugeVec,
    records_streamed: IntCounterVec,
    // Phase 2.2: Profiling phase metrics
    profiling_phase_duration: HistogramVec, // velo_profiling_phase_duration_seconds{job_name, phase}
    profiling_phase_throughput: GaugeVec,   // velo_profiling_phase_throughput_rps{job_name, phase}
    // Phase 2.3: Pipeline operations metrics
    pipeline_operation_duration: HistogramVec, // velo_pipeline_operation_duration_seconds{job_name, operation}
    // Phase 2.4: Job-specific throughput
    throughput_by_job: GaugeVec, // velo_streaming_throughput_by_job_rps{job_name}
}

impl StreamingMetrics {
    fn new(registry: &Registry, config: &PrometheusConfig) -> Result<Self, SqlError> {
        let operations_total = register_int_counter_vec_with_registry!(
            Opts::new(
                "velo_streaming_operations_total",
                "Total number of streaming operations"
            ),
            &["operation"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register streaming metrics: {}", e),
        })?;

        let operation_duration = if config.enable_histograms {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_streaming_duration_seconds",
                    "Streaming operation duration"
                ),
                &["operation"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register streaming histogram: {}", e),
            })?
        } else {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_streaming_duration_seconds",
                    "Streaming operation duration"
                )
                .buckets(vec![0.01, 0.1, 1.0, 10.0]),
                &["operation"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register streaming histogram: {}", e),
            })?
        };

        let throughput = register_gauge_vec_with_registry!(
            Opts::new(
                "velo_streaming_throughput_rps",
                "Current streaming throughput in records per second"
            ),
            &["operation"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register throughput gauge: {}", e),
        })?;

        let records_streamed = register_int_counter_vec_with_registry!(
            Opts::new(
                "velo_streaming_records_total",
                "Total number of records streamed"
            ),
            &["operation"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register streaming record metrics: {}", e),
        })?;

        // Phase 2.2: Profiling phase metrics
        let profiling_phase_duration = if config.enable_histograms {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_profiling_phase_duration_seconds",
                    "Profiling phase duration broken down by job and phase"
                ),
                &["job_name", "phase"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register profiling phase histogram: {}", e),
            })?
        } else {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_profiling_phase_duration_seconds",
                    "Profiling phase duration broken down by job and phase"
                )
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0]),
                &["job_name", "phase"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register profiling phase histogram: {}", e),
            })?
        };

        let profiling_phase_throughput = register_gauge_vec_with_registry!(
            Opts::new(
                "velo_profiling_phase_throughput_rps",
                "Profiling phase throughput in records per second"
            ),
            &["job_name", "phase"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register profiling phase throughput gauge: {}", e),
        })?;

        // Phase 2.3: Pipeline operations metrics
        let pipeline_operation_duration = if config.enable_histograms {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_pipeline_operation_duration_seconds",
                    "Pipeline operation duration broken down by job and operation"
                ),
                &["job_name", "operation"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register pipeline operation histogram: {}", e),
            })?
        } else {
            register_histogram_vec_with_registry!(
                HistogramOpts::new(
                    "velo_pipeline_operation_duration_seconds",
                    "Pipeline operation duration broken down by job and operation"
                )
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0]),
                &["job_name", "operation"],
                registry
            )
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register pipeline operation histogram: {}", e),
            })?
        };

        // Phase 2.4: Job-specific throughput
        let throughput_by_job = register_gauge_vec_with_registry!(
            Opts::new(
                "velo_streaming_throughput_by_job_rps",
                "Streaming throughput by job in records per second"
            ),
            &["job_name"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register throughput by job gauge: {}", e),
        })?;

        Ok(Self {
            operations_total,
            operation_duration,
            throughput,
            records_streamed,
            profiling_phase_duration,
            profiling_phase_throughput,
            pipeline_operation_duration,
            throughput_by_job,
        })
    }

    fn record_operation(
        &self,
        operation: &str,
        duration: Duration,
        record_count: u64,
        throughput: f64,
    ) {
        self.operations_total.with_label_values(&[operation]).inc();
        self.operation_duration
            .with_label_values(&[operation])
            .observe(duration.as_secs_f64());
        self.throughput
            .with_label_values(&[operation])
            .set(throughput);
        self.records_streamed
            .with_label_values(&[operation])
            .inc_by(record_count);
    }

    /// Record profiling phase metrics with job name (Phase 2.2)
    fn record_profiling_phase(
        &self,
        job_name: &str,
        phase: &str,
        duration: Duration,
        record_count: u64,
        throughput_rps: f64,
    ) {
        // Record phase-specific duration histogram
        self.profiling_phase_duration
            .with_label_values(&[job_name, phase])
            .observe(duration.as_secs_f64());

        // Record phase-specific throughput gauge
        self.profiling_phase_throughput
            .with_label_values(&[job_name, phase])
            .set(throughput_rps);
    }

    /// Record pipeline operation metrics with job name (Phase 2.3)
    fn record_pipeline_operation(
        &self,
        job_name: &str,
        operation: &str,
        duration: Duration,
        record_count: u64,
    ) {
        // Record operation-specific duration histogram
        self.pipeline_operation_duration
            .with_label_values(&[job_name, operation])
            .observe(duration.as_secs_f64());
    }

    /// Record job-specific throughput (Phase 2.4)
    fn record_throughput_by_job(&self, job_name: &str, throughput_rps: f64) {
        self.throughput_by_job
            .with_label_values(&[job_name])
            .set(throughput_rps);
    }

    /// Get total operations count across all operation types
    fn get_total_operations(&self) -> u64 {
        // Sum across all label values
        let mut total = 0u64;
        for (operation, count) in [
            (
                "deserialization",
                self.operations_total
                    .with_label_values(&["deserialization"])
                    .get(),
            ),
            (
                "sql_processing",
                self.operations_total
                    .with_label_values(&["sql_processing"])
                    .get(),
            ),
            (
                "serialization",
                self.operations_total
                    .with_label_values(&["serialization"])
                    .get(),
            ),
        ] {
            total += count;
        }
        total
    }

    /// Get total records streamed across all operation types
    fn get_total_records(&self) -> u64 {
        let mut total = 0u64;
        for operation in ["deserialization", "sql_processing", "serialization"] {
            total += self.records_streamed.with_label_values(&[operation]).get();
        }
        total
    }
}

/// System resource metrics with per-node labels
#[derive(Debug)]
struct SystemMetrics {
    // Per-node system resource metrics with node_id, node_name, region labels
    cpu_usage: GaugeVec, // velo_cpu_usage_percent{node_id, node_name, region}
    memory_usage: IntGaugeVec, // velo_memory_usage_bytes{node_id, node_name, region}
    active_jobs: IntGauge, // velo_active_jobs - Number of currently active jobs
    up: IntGaugeVec, // up{job} - Standard Prometheus metric indicating service availability (1=UP, 0=DOWN)
    // Current node context
    node_id: Arc<Mutex<Option<String>>>,
    node_name: Arc<Mutex<Option<String>>>,
    region: Arc<Mutex<Option<String>>>,
}

impl SystemMetrics {
    fn new(registry: &Registry, _config: &PrometheusConfig) -> Result<Self, SqlError> {
        // Register CPU usage gauge with per-node labels
        let cpu_usage = register_gauge_vec_with_registry!(
            Opts::new(
                "velo_cpu_usage_percent",
                "Current CPU usage percentage per node"
            ),
            &["node_id", "node_name", "region"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register CPU metrics: {}", e),
        })?;

        // Register memory usage gauge with per-node labels
        let memory_usage = register_int_gauge_vec_with_registry!(
            Opts::new(
                "velo_memory_usage_bytes",
                "Current memory usage in bytes per node"
            ),
            &["node_id", "node_name", "region"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register memory metrics: {}", e),
        })?;

        // Register active jobs gauge
        let active_jobs = register_int_gauge_with_registry!(
            Opts::new("velo_active_jobs", "Number of currently active jobs"),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register active jobs metric: {}", e),
        })?;

        // Register standard Prometheus "up" metric indicating service availability
        // This metric is required for Grafana health dashboards and service monitoring
        // Value: 1 = UP, 0 = DOWN
        let up = register_int_gauge_vec_with_registry!(
            Opts::new(
                "up",
                "Velostream telemetry service availability (1=UP, 0=DOWN)"
            ),
            &["job"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register 'up' metric: {}", e),
        })?;

        // Initialize the 'up' metric with value 1 (service is UP)
        up.with_label_values(&["velostream-telemetry"]).set(1);

        Ok(Self {
            cpu_usage,
            memory_usage,
            active_jobs,
            up,
            node_id: Arc::new(Mutex::new(None)),
            node_name: Arc::new(Mutex::new(None)),
            region: Arc::new(Mutex::new(None)),
        })
    }

    /// Set the node context for system metrics labeling
    fn set_node_context(
        &self,
        node_id: Option<String>,
        node_name: Option<String>,
        region: Option<String>,
    ) -> Result<(), SqlError> {
        if let Ok(mut id) = self.node_id.lock() {
            *id = node_id.clone();
        }
        if let Ok(mut name) = self.node_name.lock() {
            *name = node_name.clone();
        }
        if let Ok(mut reg) = self.region.lock() {
            *reg = region.clone();
        }

        log::debug!(
            "📊 System metrics node context updated: node_id={:?}, node_name={:?}, region={:?}",
            node_id,
            node_name,
            region
        );

        Ok(())
    }

    fn update(&self, cpu_usage: f64, memory_usage: u64, active_jobs: i64) {
        // Get current label values
        let node_id = self
            .node_id
            .lock()
            .ok()
            .and_then(|n| n.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let node_name = self
            .node_name
            .lock()
            .ok()
            .and_then(|n| n.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let region = self
            .region
            .lock()
            .ok()
            .and_then(|n| n.clone())
            .unwrap_or_else(|| "unknown".to_string());

        let label_values = &[node_id.as_str(), node_name.as_str(), region.as_str()];

        // Update per-node metrics with labels
        self.cpu_usage
            .with_label_values(label_values)
            .set(cpu_usage);
        self.memory_usage
            .with_label_values(label_values)
            .set(memory_usage as i64);
        self.active_jobs.set(active_jobs);
    }
}
