// === PHASE 4: PROMETHEUS METRICS COLLECTION ===

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::config::PrometheusConfig;
use prometheus::{
    register_gauge_with_registry, register_histogram_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_gauge_vec_with_registry, Encoder, Gauge, GaugeVec,
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use std::time::Duration;

/// Prometheus metrics provider for comprehensive monitoring
#[derive(Debug)]
pub struct MetricsProvider {
    config: PrometheusConfig,
    registry: Registry,
    sql_metrics: SqlMetrics,
    streaming_metrics: StreamingMetrics,
    system_metrics: SystemMetrics,
    active: bool,
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
        self.operation_duration.with_label_values(&[operation]).observe(duration.as_secs_f64());
        self.throughput.with_label_values(&[operation]).set(throughput);
        self.records_streamed.with_label_values(&[operation]).inc_by(record_count);
    }

    /// Get total operations count across all operation types
    fn get_total_operations(&self) -> u64 {
        // Sum across all label values
        let mut total = 0u64;
        for (operation, count) in [
            ("deserialization", self.operations_total.with_label_values(&["deserialization"]).get()),
            ("sql_processing", self.operations_total.with_label_values(&["sql_processing"]).get()),
            ("serialization", self.operations_total.with_label_values(&["serialization"]).get()),
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
        provider.record_streaming_operation("ingest", Duration::from_millis(100), 1000, 500.0);

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
}
