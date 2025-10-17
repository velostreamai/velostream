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

/// Prometheus metrics provider for comprehensive monitoring
pub struct MetricsProvider {
    config: PrometheusConfig,
    registry: Registry,
    sql_metrics: SqlMetrics,
    streaming_metrics: StreamingMetrics,
    system_metrics: SystemMetrics,
    active: bool,
    // Dynamic SQL-annotated metrics (Phase 2A-2B)
    dynamic_counters: Arc<Mutex<HashMap<String, IntCounterVec>>>,
    dynamic_gauges: Arc<Mutex<HashMap<String, GaugeVec>>>,
    dynamic_histograms: Arc<Mutex<HashMap<String, HistogramVec>>>,
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
            dynamic_counters: Arc::new(Mutex::new(HashMap::new())),
            dynamic_gauges: Arc::new(Mutex::new(HashMap::new())),
            dynamic_histograms: Arc::new(Mutex::new(HashMap::new())),
            job_metrics: Arc::new(Mutex::new(HashMap::new())),
            error_tracker: Arc::new(Mutex::new(ErrorMessageBuffer::new())),
        })
    }

    /// Set the node ID for this metrics provider (used in observability context)
    pub fn set_node_id(&mut self, node_id: Option<String>) -> Result<(), SqlError> {
        if let Some(ref id) = node_id {
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
    pub fn update_system_metrics(
        &self,
        cpu_usage: f64,
        memory_usage: u64,
        active_connections: i64,
    ) {
        if self.active {
            self.system_metrics
                .update(cpu_usage, memory_usage, active_connections);
            log::trace!(
                "📊 Updated system metrics: cpu={:.2}%, memory={}MB, connections={}",
                cpu_usage,
                memory_usage / (1024 * 1024),
                active_connections
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
        // For per-node metrics, get from "unknown" node (default if not set)
        let active_connections = self
            .system_metrics
            .active_connections
            .with_label_values(&["unknown", "unknown", "unknown"])
            .get();

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
            active_connections,
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

        let mut counters = self.dynamic_counters.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic counters for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic counters: {}", e),
            }
        })?;

        // Check if metric already registered
        if counters.contains_key(name) {
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

        counters.insert(name.to_string(), counter);

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

        let counters = self.dynamic_counters.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic counters for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic counters: {}", e),
            }
        })?;

        let counter = counters.get(name).ok_or_else(|| {
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

        let mut gauges = self.dynamic_gauges.lock().map_err(|e| {
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
        if gauges.contains_key(name) {
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

        gauges.insert(name.to_string(), gauge);

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

        let gauges = self.dynamic_gauges.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic gauges for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic gauges: {}", e),
            }
        })?;

        let gauge = gauges.get(name).ok_or_else(|| {
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

        let mut histograms = self.dynamic_histograms.lock().map_err(|e| {
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
        if histograms.contains_key(name) {
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

        histograms.insert(name.to_string(), histogram);

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

        let histograms = self.dynamic_histograms.lock().map_err(|e| {
            log::error!(
                "❌ Failed to acquire lock on dynamic histograms for '{}': {}",
                name,
                e
            );
            SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic histograms: {}", e),
            }
        })?;

        let histogram = histograms.get(name).ok_or_else(|| {
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
                "dynamic_counters_count",
                &self.dynamic_counters.lock().map(|c| c.len()).unwrap_or(0),
            )
            .field(
                "dynamic_gauges_count",
                &self.dynamic_gauges.lock().map(|g| g.len()).unwrap_or(0),
            )
            .field(
                "dynamic_histograms_count",
                &self.dynamic_histograms.lock().map(|h| h.len()).unwrap_or(0),
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
    pub active_connections: i64,
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
    active_connections: IntGaugeVec, // velo_active_connections{node_id, node_name, region}
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

        // Register active connections gauge with per-node labels
        let active_connections = register_int_gauge_vec_with_registry!(
            Opts::new(
                "velo_active_connections",
                "Number of active connections per node"
            ),
            &["node_id", "node_name", "region"],
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register connection metrics: {}", e),
        })?;

        Ok(Self {
            cpu_usage,
            memory_usage,
            active_connections,
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

    fn update(&self, cpu_usage: f64, memory_usage: u64, active_connections: i64) {
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
        self.active_connections
            .with_label_values(label_values)
            .set(active_connections);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_provider_creation() {
        let config = PrometheusConfig::lightweight();
        let provider = MetricsProvider::new(config).await;
        assert!(provider.is_ok());

        let provider = provider.unwrap();
        assert!(provider.active);
    }

    #[tokio::test]
    async fn test_metrics_recording() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Test SQL metrics
        provider.record_sql_query("select", Duration::from_millis(150), true, 100);
        provider.record_sql_query("insert", Duration::from_millis(250), false, 50);

        // Test streaming metrics
        provider.record_streaming_operation(
            "deserialization",
            Duration::from_millis(100),
            1000,
            500.0,
        );

        // Test system metrics
        provider.update_system_metrics(45.5, 1024 * 1024 * 1024, 10);

        // Test stats
        let stats = provider.get_stats();
        assert_eq!(stats.sql_queries_total, 2);
        assert_eq!(stats.sql_errors_total, 1);
        assert_eq!(stats.streaming_operations_total, 1);
        assert_eq!(stats.records_processed_total, 150);
        assert_eq!(stats.records_streamed_total, 1000);
        assert_eq!(stats.active_connections, 10);
    }

    #[tokio::test]
    async fn test_metrics_text_export() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        provider.record_sql_query("select", Duration::from_millis(100), true, 50);

        let metrics_text = provider.get_metrics_text();
        assert!(metrics_text.is_ok());

        let text = metrics_text.unwrap();
        assert!(text.contains("velo_sql_queries_total"));
        assert!(text.contains("velo_sql_query_duration_seconds"));
    }

    #[test]
    fn test_sql_metrics_creation() {
        let registry = Registry::new();
        let config = PrometheusConfig::default();

        let sql_metrics = SqlMetrics::new(&registry, &config);
        assert!(sql_metrics.is_ok());
    }

    #[test]
    fn test_streaming_metrics_creation() {
        let registry = Registry::new();
        let config = PrometheusConfig::default();

        let streaming_metrics = StreamingMetrics::new(&registry, &config);
        assert!(streaming_metrics.is_ok());
    }

    #[test]
    fn test_system_metrics_creation() {
        let registry = Registry::new();
        let config = PrometheusConfig::default();

        let system_metrics = SystemMetrics::new(&registry, &config);
        assert!(system_metrics.is_ok());
    }

    // === Dynamic Counter Metrics Tests ===

    #[tokio::test]
    async fn test_register_counter_metric_basic() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        let result = provider.register_counter_metric(
            "test_events_total",
            "Total number of test events",
            &vec![],
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_register_counter_metric_with_labels() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        let labels = vec!["status".to_string(), "source".to_string()];
        let result = provider.register_counter_metric(
            "test_requests_total",
            "Total number of test requests",
            &labels,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_register_counter_idempotent() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register once
        let result1 = provider.register_counter_metric("test_metric_total", "Test metric", &vec![]);
        assert!(result1.is_ok());

        // Register again (should succeed but not duplicate)
        let result2 = provider.register_counter_metric("test_metric_total", "Test metric", &vec![]);
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_emit_counter_basic() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register counter
        provider
            .register_counter_metric("test_events_total", "Test events", &vec![])
            .unwrap();

        // Emit counter
        let result = provider.emit_counter("test_events_total", &vec![]);
        assert!(result.is_ok());

        // Verify metric appears in output
        let metrics_text = provider.get_metrics_text().unwrap();
        assert!(metrics_text.contains("test_events_total"));
        assert!(metrics_text.contains(" 1")); // Value should be 1
    }

    #[tokio::test]
    async fn test_emit_counter_with_labels() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register counter with labels
        let labels = vec!["status".to_string(), "source".to_string()];
        provider
            .register_counter_metric("test_requests_total", "Test requests", &labels)
            .unwrap();

        // Emit counter with label values
        let label_values = vec!["success".to_string(), "api".to_string()];
        let result = provider.emit_counter("test_requests_total", &label_values);
        assert!(result.is_ok());

        // Emit again to increment
        let result = provider.emit_counter("test_requests_total", &label_values);
        assert!(result.is_ok());

        // Verify metric appears in output with labels
        let metrics_text = provider.get_metrics_text().unwrap();
        assert!(metrics_text.contains("test_requests_total"));
        assert!(metrics_text.contains("status=\"success\""));
        assert!(metrics_text.contains("source=\"api\""));
        assert!(metrics_text.contains(" 2")); // Value should be 2
    }

    #[tokio::test]
    async fn test_emit_counter_multiple_label_combinations() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register counter with labels
        let labels = vec!["status".to_string()];
        provider
            .register_counter_metric("test_status_total", "Test status", &labels)
            .unwrap();

        // Emit with different label values
        provider
            .emit_counter("test_status_total", &vec!["success".to_string()])
            .unwrap();
        provider
            .emit_counter("test_status_total", &vec!["success".to_string()])
            .unwrap();
        provider
            .emit_counter("test_status_total", &vec!["error".to_string()])
            .unwrap();

        // Verify both label combinations appear
        let metrics_text = provider.get_metrics_text().unwrap();
        assert!(metrics_text.contains("test_status_total"));
        assert!(metrics_text.contains("status=\"success\""));
        assert!(metrics_text.contains("status=\"error\""));
    }

    #[tokio::test]
    async fn test_emit_counter_before_register_error() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Try to emit before registering (should fail)
        let result = provider.emit_counter("unregistered_metric", &vec![]);
        assert!(result.is_err());

        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("not registered"));
    }

    #[tokio::test]
    async fn test_register_counter_when_inactive() {
        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        // Shutdown provider
        provider.shutdown().await.unwrap();

        // Try to register (should fail)
        let result = provider.register_counter_metric("test_metric", "Test", &vec![]);
        assert!(result.is_err());

        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("not active"));
    }

    #[tokio::test]
    async fn test_emit_counter_when_inactive_succeeds_silently() {
        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        // Register while active
        provider
            .register_counter_metric("test_metric", "Test", &vec![])
            .unwrap();

        // Shutdown provider
        provider.shutdown().await.unwrap();

        // Try to emit (should succeed silently)
        let result = provider.emit_counter("test_metric", &vec![]);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dynamic_counter_in_metrics_export() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register and emit a dynamic counter
        provider
            .register_counter_metric(
                "velo_trading_volume_spikes_total",
                "Total number of volume spikes",
                &vec!["symbol".to_string(), "spike_ratio".to_string()],
            )
            .unwrap();

        provider
            .emit_counter(
                "velo_trading_volume_spikes_total",
                &vec!["AAPL".to_string(), "2.5".to_string()],
            )
            .unwrap();
        provider
            .emit_counter(
                "velo_trading_volume_spikes_total",
                &vec!["AAPL".to_string(), "2.5".to_string()],
            )
            .unwrap();
        provider
            .emit_counter(
                "velo_trading_volume_spikes_total",
                &vec!["GOOGL".to_string(), "3.2".to_string()],
            )
            .unwrap();

        // Verify metrics export
        let metrics_text = provider.get_metrics_text().unwrap();
        assert!(metrics_text.contains("velo_trading_volume_spikes_total"));
        assert!(metrics_text.contains("symbol=\"AAPL\""));
        assert!(metrics_text.contains("spike_ratio=\"2.5\""));
        assert!(metrics_text.contains("symbol=\"GOOGL\""));
        assert!(metrics_text.contains("spike_ratio=\"3.2\""));

        // AAPL should have value 2, GOOGL should have value 1
        assert!(metrics_text.contains(" 2"));
        assert!(metrics_text.contains(" 1"));
    }

    // === Phase 5: Lifecycle Management Tests ===

    #[tokio::test]
    async fn test_register_job_metric() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register metrics for a job
        let result = provider.register_job_metric("job1", "metric1");
        assert!(result.is_ok());

        let result = provider.register_job_metric("job1", "metric2");
        assert!(result.is_ok());

        // Get metrics for job
        let metrics = provider.get_job_metrics("job1").unwrap();
        assert_eq!(metrics.len(), 2);
        assert!(metrics.contains(&"metric1".to_string()));
        assert!(metrics.contains(&"metric2".to_string()));
    }

    #[tokio::test]
    async fn test_unregister_job_metrics() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register metrics
        provider.register_job_metric("job1", "metric1").unwrap();
        provider.register_job_metric("job1", "metric2").unwrap();

        // Unregister all metrics for job
        let unregistered = provider.unregister_job_metrics("job1").unwrap();
        assert_eq!(unregistered.len(), 2);

        // Verify job has no metrics now
        let metrics = provider.get_job_metrics("job1").unwrap();
        assert_eq!(metrics.len(), 0);
    }

    #[tokio::test]
    async fn test_list_all_jobs() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register metrics for multiple jobs
        provider.register_job_metric("job1", "metric1").unwrap();
        provider.register_job_metric("job2", "metric2").unwrap();
        provider.register_job_metric("job3", "metric3").unwrap();

        // List all jobs
        let jobs = provider.list_all_jobs().unwrap();
        assert_eq!(jobs.len(), 3);
        assert!(jobs.contains(&"job1".to_string()));
        assert!(jobs.contains(&"job2".to_string()));
        assert!(jobs.contains(&"job3".to_string()));
    }

    #[tokio::test]
    async fn test_get_tracking_stats() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register metrics for multiple jobs
        provider.register_job_metric("job1", "metric1").unwrap();
        provider.register_job_metric("job1", "metric2").unwrap();
        provider.register_job_metric("job2", "metric3").unwrap();

        // Get tracking stats
        let (job_count, total_metrics) = provider.get_tracking_stats().unwrap();
        assert_eq!(job_count, 2); // 2 jobs
        assert_eq!(total_metrics, 3); // 3 total metrics
    }

    #[tokio::test]
    async fn test_job_metric_idempotency() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Register same metric multiple times
        provider.register_job_metric("job1", "metric1").unwrap();
        provider.register_job_metric("job1", "metric1").unwrap();
        provider.register_job_metric("job1", "metric1").unwrap();

        // Should only have one metric
        let metrics = provider.get_job_metrics("job1").unwrap();
        assert_eq!(metrics.len(), 1);
    }

    // === Error Message Tracking Tests ===

    #[tokio::test]
    async fn test_error_message_tracking_basic() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        provider.record_error_message("Connection timeout".to_string());
        assert_eq!(provider.get_total_errors(), 1);
        assert_eq!(provider.get_unique_error_types(), 1);
    }

    #[tokio::test]
    async fn test_error_message_tracking_duplicates() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        provider.record_error_message("Timeout".to_string());
        provider.record_error_message("Timeout".to_string());
        provider.record_error_message("Invalid query".to_string());

        assert_eq!(provider.get_total_errors(), 3);
        assert_eq!(provider.get_unique_error_types(), 2);
    }

    #[tokio::test]
    async fn test_error_stats() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        provider.record_error_message("Error A".to_string());
        provider.record_error_message("Error A".to_string());
        provider.record_error_message("Error B".to_string());

        let stats = provider.get_error_stats().unwrap();
        assert_eq!(stats.total_errors, 3);
        assert_eq!(stats.unique_errors, 2);
    }

    #[tokio::test]
    async fn test_top_errors() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        for _ in 0..5 {
            provider.record_error_message("Error 1".to_string());
        }
        for _ in 0..3 {
            provider.record_error_message("Error 2".to_string());
        }

        let top = provider.get_top_errors(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "Error 1");
        assert_eq!(top[0].1, 5);
    }

    #[tokio::test]
    async fn test_record_sql_query_with_error() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        provider.record_sql_query_with_error(
            "select",
            Duration::from_millis(100),
            false,
            0,
            Some("Database offline".to_string()),
        );

        assert_eq!(provider.get_total_errors(), 1);
        let messages = provider.get_error_messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message, "Database offline");
    }

    #[tokio::test]
    async fn test_reset_error_tracking() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        provider.record_error_message("Error".to_string());
        assert_eq!(provider.get_total_errors(), 1);

        provider.reset_error_tracking();
        assert_eq!(provider.get_total_errors(), 0);
        assert_eq!(provider.get_buffered_error_count(), 0);
    }

    #[tokio::test]
    async fn test_error_buffer_rolling_limit() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Add 15 unique errors (buffer max is 10)
        for i in 0..15 {
            provider.record_error_message(format!("Error {}", i));
        }

        // Only 10 should be in buffer, but total should still be 15
        assert_eq!(provider.get_buffered_error_count(), 10);
        assert_eq!(provider.get_total_errors(), 15);
    }

    #[tokio::test]
    async fn test_error_metrics_sync_to_prometheus() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Record some errors
        provider.record_error_message("Database error".to_string());
        provider.record_error_message("Database error".to_string());
        provider.record_error_message("Timeout error".to_string());

        // Sync error metrics to Prometheus gauges
        provider.sync_error_metrics();

        // Verify metrics appear in Prometheus output
        let metrics_text = provider.get_metrics_text().unwrap();

        // Check for error tracking metrics
        assert!(
            metrics_text.contains("velo_error_messages_total"),
            "Missing total errors metric"
        );
        assert!(
            metrics_text.contains("velo_unique_error_types"),
            "Missing unique error types metric"
        );
        assert!(
            metrics_text.contains("velo_buffered_error_messages"),
            "Missing buffered errors metric"
        );

        // Verify values are correct
        assert!(
            metrics_text.contains("velo_error_messages_total 3"),
            "Total errors should be 3"
        );
        assert!(
            metrics_text.contains("velo_unique_error_types 2"),
            "Unique error types should be 2"
        );
        assert!(
            metrics_text.contains("velo_buffered_error_messages 2"),
            "Buffered errors should be 2"
        );
    }

    #[tokio::test]
    async fn test_set_deployment_context_on_error_tracker() {
        use crate::velostream::observability::error_tracker::DeploymentContext;

        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        // Set deployment context
        let ctx = DeploymentContext::with_all(
            "prod-trading-cluster-1".to_string(),
            "Trading Analytics Platform".to_string(),
            "us-east-1".to_string(),
            "1.0.0".to_string(),
        );

        provider.set_deployment_context(ctx).unwrap();

        // Record error messages
        provider.record_error_message("Connection timeout".to_string());
        provider.record_error_message("Query execution failed".to_string());

        // Get error messages and verify deployment context is present
        let messages = provider.get_error_messages();
        assert_eq!(messages.len(), 2);

        // Verify first message has deployment context
        let first_msg = &messages[0];
        assert!(first_msg.deployment_context.is_some());
        let ctx = first_msg.deployment_context.as_ref().unwrap();
        assert_eq!(ctx.node_id, Some("prod-trading-cluster-1".to_string()));
        assert_eq!(
            ctx.node_name,
            Some("Trading Analytics Platform".to_string())
        );
        assert_eq!(ctx.region, Some("us-east-1".to_string()));
        assert_eq!(ctx.version, Some("1.0.0".to_string()));

        // Verify display includes all deployment context fields
        let display_str = first_msg.to_string();
        assert!(display_str.contains("node_id=prod-trading-cluster-1"));
        assert!(display_str.contains("node_name=Trading Analytics Platform"));
        assert!(display_str.contains("region=us-east-1"));
        assert!(display_str.contains("version=1.0.0"));
    }

    #[tokio::test]
    async fn test_set_node_id_propagates_to_error_tracker() {
        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        // Set node ID (should propagate to error tracker)
        provider
            .set_node_id(Some("prod-node-42".to_string()))
            .unwrap();

        // Record error messages
        provider.record_error_message("Deserialization error".to_string());
        provider.record_error_message("SQL processing error".to_string());

        // Get error messages and verify node context is present
        let messages = provider.get_error_messages();
        assert_eq!(messages.len(), 2);

        // Verify messages have node_id set
        let first_msg = &messages[0];
        assert_eq!(first_msg.node_id, Some("prod-node-42".to_string()));
        let second_msg = &messages[1];
        assert_eq!(second_msg.node_id, Some("prod-node-42".to_string()));

        // Verify display includes node_id
        let display_str = first_msg.to_string();
        assert!(display_str.contains("node=prod-node-42"));
    }

    #[tokio::test]
    async fn test_error_messages_include_job_context() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Simulate error recording like ErrorTracker does (with job name prefix)
        provider.record_error_message(
            "[financial-analytics-job] Market data connection failed".to_string(),
        );
        provider
            .record_error_message("[financial-analytics-job] Order execution timeout".to_string());
        provider.record_error_message("[reporting-job] Report generation failed".to_string());

        // Get error messages and verify they all contain job context
        let messages = provider.get_error_messages();
        assert_eq!(messages.len(), 3);

        // Verify messages maintain job context
        assert!(messages[0].message.contains("financial-analytics-job"));
        assert!(messages[1].message.contains("financial-analytics-job"));
        assert!(messages[2].message.contains("reporting-job"));

        // Verify error stats include all messages
        let stats = provider.get_error_stats().unwrap();
        assert_eq!(stats.total_errors, 3);
        assert_eq!(stats.unique_errors, 3);
    }

    // === Per-Node System Metrics Tests ===

    #[tokio::test]
    async fn test_per_node_cpu_and_memory_metrics() {
        use crate::velostream::observability::error_tracker::DeploymentContext;

        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        // Set deployment context with node info
        let ctx = DeploymentContext::with_all(
            "prod-node-1".to_string(),
            "Trading Engine - Node 1".to_string(),
            "us-west-2".to_string(),
            "2.1.0".to_string(),
        );

        provider.set_deployment_context(ctx).unwrap();

        // Update system metrics
        provider.update_system_metrics(45.5, 1024 * 1024 * 1024, 42);

        // Verify metrics appear in Prometheus output with per-node labels
        let metrics_text = provider.get_metrics_text().unwrap();

        // Check for CPU metrics with node labels
        assert!(metrics_text.contains("velo_cpu_usage_percent"));
        assert!(
            metrics_text.contains("node_id=\"prod-node-1\""),
            "Missing node_id label in CPU metric"
        );
        assert!(
            metrics_text.contains("node_name=\"Trading Engine - Node 1\""),
            "Missing node_name label in CPU metric"
        );
        assert!(
            metrics_text.contains("region=\"us-west-2\""),
            "Missing region label in CPU metric"
        );

        // Check for memory metrics with per-node labels
        assert!(metrics_text.contains("velo_memory_usage_bytes"));

        // Check for active connections with per-node labels
        assert!(metrics_text.contains("velo_active_connections"));
    }

    #[tokio::test]
    async fn test_multiple_nodes_system_metrics() {
        use crate::velostream::observability::error_tracker::DeploymentContext;

        let config1 = PrometheusConfig::default();
        let config2 = PrometheusConfig::default();
        let mut provider1 = MetricsProvider::new(config1).await.unwrap();
        let mut provider2 = MetricsProvider::new(config2).await.unwrap();

        // Set different deployment contexts for two nodes
        let ctx1 = DeploymentContext::with_all(
            "prod-node-1".to_string(),
            "Node 1".to_string(),
            "us-west-2".to_string(),
            "2.1.0".to_string(),
        );

        let ctx2 = DeploymentContext::with_all(
            "prod-node-2".to_string(),
            "Node 2".to_string(),
            "us-east-1".to_string(),
            "2.1.0".to_string(),
        );

        provider1.set_deployment_context(ctx1).unwrap();
        provider2.set_deployment_context(ctx2).unwrap();

        // Update metrics for each provider (simulating different nodes)
        provider1.update_system_metrics(35.2, 512 * 1024 * 1024, 20);
        provider2.update_system_metrics(62.8, 2048 * 1024 * 1024, 85);

        // Verify metrics from provider1
        let metrics_text1 = provider1.get_metrics_text().unwrap();
        assert!(metrics_text1.contains("node_id=\"prod-node-1\""));
        assert!(metrics_text1.contains("region=\"us-west-2\""));

        // Verify metrics from provider2
        let metrics_text2 = provider2.get_metrics_text().unwrap();
        assert!(metrics_text2.contains("node_id=\"prod-node-2\""));
        assert!(metrics_text2.contains("region=\"us-east-1\""));
    }

    #[tokio::test]
    async fn test_system_metrics_with_default_labels() {
        let config = PrometheusConfig::default();
        let provider = MetricsProvider::new(config).await.unwrap();

        // Update metrics without setting deployment context
        // Should use "unknown" as default labels
        provider.update_system_metrics(50.0, 2048 * 1024 * 1024, 64);

        // Verify metrics appear with default "unknown" labels
        let metrics_text = provider.get_metrics_text().unwrap();

        assert!(metrics_text.contains("velo_cpu_usage_percent"));
        assert!(
            metrics_text.contains("node_id=\"unknown\""),
            "Missing default node_id label"
        );
        assert!(
            metrics_text.contains("node_name=\"unknown\""),
            "Missing default node_name label"
        );
        assert!(
            metrics_text.contains("region=\"unknown\""),
            "Missing default region label"
        );
    }

    #[tokio::test]
    async fn test_set_node_id_updates_system_metrics() {
        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        // Set only node ID (no full deployment context)
        provider
            .set_node_id(Some("analytics-node-1".to_string()))
            .unwrap();

        // Update system metrics
        provider.update_system_metrics(55.5, 1536 * 1024 * 1024, 48);

        // Verify metrics appear with node_id set but other labels default
        let metrics_text = provider.get_metrics_text().unwrap();

        assert!(metrics_text.contains("node_id=\"analytics-node-1\""));
        assert!(metrics_text.contains("node_name=\"unknown\"")); // Should default
        assert!(metrics_text.contains("region=\"unknown\"")); // Should default
    }

    #[tokio::test]
    async fn test_system_metrics_label_updates_dynamically() {
        use crate::velostream::observability::error_tracker::DeploymentContext;

        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        // First, update metrics with default labels
        provider.update_system_metrics(30.0, 512 * 1024 * 1024, 10);

        // Then set deployment context
        let ctx = DeploymentContext::with_all(
            "prod-node-99".to_string(),
            "Updated Node".to_string(),
            "eu-west-1".to_string(),
            "3.0.0".to_string(),
        );
        provider.set_deployment_context(ctx).unwrap();

        // Update metrics again with new labels
        provider.update_system_metrics(45.0, 1024 * 1024 * 1024, 25);

        // Verify metrics now use new deployment context
        let metrics_text = provider.get_metrics_text().unwrap();

        assert!(metrics_text.contains("node_id=\"prod-node-99\""));
        assert!(metrics_text.contains("node_name=\"Updated Node\""));
        assert!(metrics_text.contains("region=\"eu-west-1\""));
    }

    #[tokio::test]
    async fn test_system_metrics_multiple_updates_per_node() {
        use crate::velostream::observability::error_tracker::DeploymentContext;

        let config = PrometheusConfig::default();
        let mut provider = MetricsProvider::new(config).await.unwrap();

        let ctx = DeploymentContext::with_all(
            "performance-node".to_string(),
            "High Performance".to_string(),
            "ap-southeast-1".to_string(),
            "2.5.0".to_string(),
        );

        provider.set_deployment_context(ctx).unwrap();

        // Simulate multiple metric updates over time
        provider.update_system_metrics(25.0, 256 * 1024 * 1024, 5);
        provider.update_system_metrics(40.0, 512 * 1024 * 1024, 15);
        provider.update_system_metrics(65.0, 1536 * 1024 * 1024, 50);

        // All updates should be recorded with same node labels
        let metrics_text = provider.get_metrics_text().unwrap();

        assert!(metrics_text.contains("node_id=\"performance-node\""));
        assert!(metrics_text.contains("node_name=\"High Performance\""));
        assert!(metrics_text.contains("region=\"ap-southeast-1\""));

        // The last values should be present
        assert!(metrics_text.contains("65")); // CPU usage
        assert!(metrics_text.contains("50")); // Active connections
    }
}
