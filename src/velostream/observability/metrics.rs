// === PHASE 4: PROMETHEUS METRICS COLLECTION ===

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::config::PrometheusConfig;
use prometheus::{
    register_gauge_vec_with_registry, register_gauge_with_registry,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts,
    HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use std::collections::HashMap;
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
        })
    }

    /// Record SQL query execution metrics
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
        MetricsStats {
            sql_queries_total: self.sql_metrics.query_total.get(),
            sql_errors_total: self.sql_metrics.query_errors.get(),
            streaming_operations_total: self.streaming_metrics.get_total_operations(),
            records_processed_total: self.sql_metrics.records_processed.get(),
            records_streamed_total: self.streaming_metrics.get_total_records(),
            active_connections: self.system_metrics.active_connections.get(),
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
            return Err(SqlError::ConfigurationError {
                message: "Metrics provider is not active".to_string(),
            });
        }

        let mut counters =
            self.dynamic_counters
                .lock()
                .map_err(|e| SqlError::ConfigurationError {
                    message: format!("Failed to acquire lock on dynamic counters: {}", e),
                })?;

        // Check if metric already registered
        if counters.contains_key(name) {
            log::debug!("📊 Counter metric '{}' already registered, skipping", name);
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
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register counter '{}': {}", name, e),
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
            return Ok(()); // Silently skip if not active
        }

        let counters = self
            .dynamic_counters
            .lock()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic counters: {}", e),
            })?;

        let counter = counters
            .get(name)
            .ok_or_else(|| SqlError::ConfigurationError {
                message: format!("Counter metric '{}' not registered", name),
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
            return Err(SqlError::ConfigurationError {
                message: "Metrics provider is not active".to_string(),
            });
        }

        let mut gauges = self
            .dynamic_gauges
            .lock()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic gauges: {}", e),
            })?;

        // Check if metric already registered
        if gauges.contains_key(name) {
            log::debug!("📊 Gauge metric '{}' already registered, skipping", name);
            return Ok(());
        }

        // Convert Vec<String> to Vec<&str> for registration
        let label_refs: Vec<&str> = label_names.iter().map(|s| s.as_str()).collect();

        // Register the gauge with the registry
        let gauge =
            register_gauge_vec_with_registry!(Opts::new(name, help), &label_refs, &self.registry)
                .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register gauge '{}': {}", name, e),
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
            return Ok(()); // Silently skip if not active
        }

        let gauges = self
            .dynamic_gauges
            .lock()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to acquire lock on dynamic gauges: {}", e),
            })?;

        let gauge = gauges
            .get(name)
            .ok_or_else(|| SqlError::ConfigurationError {
                message: format!("Gauge metric '{}' not registered", name),
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
            return Err(SqlError::ConfigurationError {
                message: "Metrics provider is not active".to_string(),
            });
        }

        let mut histograms =
            self.dynamic_histograms
                .lock()
                .map_err(|e| SqlError::ConfigurationError {
                    message: format!("Failed to acquire lock on dynamic histograms: {}", e),
                })?;

        // Check if metric already registered
        if histograms.contains_key(name) {
            log::debug!(
                "📊 Histogram metric '{}' already registered, skipping",
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
                .map_err(|e| SqlError::ConfigurationError {
                    message: format!("Failed to register histogram '{}': {}", name, e),
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
            return Ok(()); // Silently skip if not active
        }

        let histograms =
            self.dynamic_histograms
                .lock()
                .map_err(|e| SqlError::ConfigurationError {
                    message: format!("Failed to acquire lock on dynamic histograms: {}", e),
                })?;

        let histogram = histograms
            .get(name)
            .ok_or_else(|| SqlError::ConfigurationError {
                message: format!("Histogram metric '{}' not registered", name),
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
    query_total: IntCounter,
    query_duration: Histogram,
    query_errors: IntCounter,
    active_queries: IntGauge,
    records_processed: IntCounter,
}

impl SqlMetrics {
    fn new(registry: &Registry, config: &PrometheusConfig) -> Result<Self, SqlError> {
        let query_total = register_int_counter_with_registry!(
            Opts::new(
                "velo_sql_queries_total",
                "Total number of SQL queries executed"
            ),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register SQL metrics: {}", e),
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

        let records_processed = register_int_counter_with_registry!(
            Opts::new(
                "velo_sql_records_processed_total",
                "Total number of records processed by SQL queries"
            ),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register record metrics: {}", e),
        })?;

        Ok(Self {
            query_total,
            query_duration,
            query_errors,
            active_queries,
            records_processed,
        })
    }

    fn record_query(
        &self,
        _query_type: &str,
        duration: Duration,
        success: bool,
        record_count: u64,
    ) {
        self.query_total.inc();
        self.query_duration.observe(duration.as_secs_f64());
        self.records_processed.inc_by(record_count);

        if !success {
            self.query_errors.inc();
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

        Ok(Self {
            operations_total,
            operation_duration,
            throughput,
            records_streamed,
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

/// System resource metrics
#[derive(Debug)]
struct SystemMetrics {
    cpu_usage: Gauge,
    memory_usage: IntGauge,
    active_connections: IntGauge,
}

impl SystemMetrics {
    fn new(registry: &Registry, _config: &PrometheusConfig) -> Result<Self, SqlError> {
        let cpu_usage = register_gauge_with_registry!(
            Opts::new("velo_cpu_usage_percent", "Current CPU usage percentage"),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register CPU metrics: {}", e),
        })?;

        let memory_usage = register_int_gauge_with_registry!(
            Opts::new("velo_memory_usage_bytes", "Current memory usage in bytes"),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register memory metrics: {}", e),
        })?;

        let active_connections = register_int_gauge_with_registry!(
            Opts::new("velo_active_connections", "Number of active connections"),
            registry
        )
        .map_err(|e| SqlError::ConfigurationError {
            message: format!("Failed to register connection metrics: {}", e),
        })?;

        Ok(Self {
            cpu_usage,
            memory_usage,
            active_connections,
        })
    }

    fn update(&self, cpu_usage: f64, memory_usage: u64, active_connections: i64) {
        self.cpu_usage.set(cpu_usage);
        self.memory_usage.set(memory_usage as i64);
        self.active_connections.set(active_connections);
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
}
