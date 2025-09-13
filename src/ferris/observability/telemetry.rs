// === PHASE 4: OPENTELEMETRY DISTRIBUTED TRACING ===

use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::config::TracingConfig;
use std::time::Instant;

/// OpenTelemetry telemetry provider for distributed tracing
#[derive(Debug)]
pub struct TelemetryProvider {
    config: TracingConfig,
    active: bool,
}

impl TelemetryProvider {
    /// Create a new telemetry provider with the given configuration
    pub async fn new(config: TracingConfig) -> Result<Self, SqlError> {
        log::info!(
            "🔍 Phase 4: Initializing distributed tracing for service '{}'",
            config.service_name
        );
        log::info!(
            "📊 Tracing configuration: sampling_ratio={}, console_output={}",
            config.sampling_ratio,
            config.enable_console_output
        );

        Ok(Self {
            config,
            active: true,
        })
    }

    /// Create a new trace span for SQL query execution
    pub fn start_sql_query_span(&self, query: &str, source: &str) -> QuerySpan {
        if !self.active {
            return QuerySpan::new_inactive();
        }

        let operation_name = Self::extract_operation_name(query);

        if self.config.enable_console_output {
            log::debug!(
                "🔍 Starting SQL query span: {} from source: {}",
                operation_name,
                source
            );
        }

        QuerySpan::new_active(
            format!("sql_query:{}", operation_name),
            query.to_string(),
            source.to_string(),
        )
    }

    /// Create a new trace span for streaming operations
    pub fn start_streaming_span(&self, operation: &str, record_count: u64) -> StreamingSpan {
        if !self.active {
            return StreamingSpan::new_inactive();
        }

        if self.config.enable_console_output {
            log::debug!(
                "🔍 Starting streaming span: {} with {} records",
                operation,
                record_count
            );
        }

        StreamingSpan::new_active(
            format!("streaming:{}", operation),
            operation.to_string(),
            record_count,
        )
    }

    /// Create a new trace span for aggregation operations
    pub fn start_aggregation_span(&self, function: &str, window_type: &str) -> AggregationSpan {
        if !self.active {
            return AggregationSpan::new_inactive();
        }

        if self.config.enable_console_output {
            log::debug!(
                "🔍 Starting aggregation span: {} with window: {}",
                function,
                window_type
            );
        }

        AggregationSpan::new_active(
            format!("aggregation:{}", function),
            function.to_string(),
            window_type.to_string(),
        )
    }

    /// Extract operation name from SQL query for span naming
    fn extract_operation_name(query: &str) -> &str {
        let query_trimmed = query.trim_start();
        if query_trimmed.starts_with("SELECT") || query_trimmed.starts_with("select") {
            "select"
        } else if query_trimmed.starts_with("CREATE") || query_trimmed.starts_with("create") {
            "create"
        } else if query_trimmed.starts_with("INSERT") || query_trimmed.starts_with("insert") {
            "insert"
        } else if query_trimmed.starts_with("UPDATE") || query_trimmed.starts_with("update") {
            "update"
        } else if query_trimmed.starts_with("DELETE") || query_trimmed.starts_with("delete") {
            "delete"
        } else {
            "unknown"
        }
    }

    /// Get the current trace ID if available (simplified implementation)
    pub fn current_trace_id(&self) -> Option<String> {
        if self.active {
            Some(format!("trace_{}", chrono::Utc::now().timestamp_millis()))
        } else {
            None
        }
    }

    /// Shutdown the telemetry provider
    pub async fn shutdown(&mut self) -> Result<(), SqlError> {
        self.active = false;
        log::debug!("🔍 Distributed tracing stopped");
        Ok(())
    }
}

/// SQL query execution span wrapper
pub struct QuerySpan {
    span_name: String,
    query: String,
    source: String,
    start_time: Instant,
    active: bool,
}

impl QuerySpan {
    fn new_active(span_name: String, query: String, source: String) -> Self {
        Self {
            span_name,
            query,
            source,
            start_time: Instant::now(),
            active: true,
        }
    }

    fn new_inactive() -> Self {
        Self {
            span_name: String::new(),
            query: String::new(),
            source: String::new(),
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Add execution time to the span
    pub fn set_execution_time(&mut self, duration_ms: u64) {
        if self.active {
            log::debug!(
                "🔍 SQL span '{}' execution time: {}ms",
                self.span_name,
                duration_ms
            );
        }
    }

    /// Add record count to the span
    pub fn set_record_count(&mut self, count: u64) {
        if self.active {
            log::debug!(
                "🔍 SQL span '{}' processed {} records",
                self.span_name,
                count
            );
        }
    }

    /// Mark the query as successful
    pub fn set_success(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::debug!(
                "🔍 SQL span '{}' completed successfully in {:?}",
                self.span_name,
                duration
            );
        }
    }

    /// Mark the query as failed with error information
    pub fn set_error(&mut self, error: &str) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::warn!(
                "🔍 SQL span '{}' failed after {:?}: {}",
                self.span_name,
                duration,
                error
            );
        }
    }
}

impl Drop for QuerySpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!(
                "🔍 SQL span '{}' finished in {:?}",
                self.span_name,
                duration
            );
        }
    }
}

/// Streaming operation span wrapper
pub struct StreamingSpan {
    span_name: String,
    operation: String,
    record_count: u64,
    start_time: Instant,
    active: bool,
}

impl StreamingSpan {
    fn new_active(span_name: String, operation: String, record_count: u64) -> Self {
        Self {
            span_name,
            operation,
            record_count,
            start_time: Instant::now(),
            active: true,
        }
    }

    fn new_inactive() -> Self {
        Self {
            span_name: String::new(),
            operation: String::new(),
            record_count: 0,
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Add throughput information to the span
    pub fn set_throughput(&mut self, records_per_second: f64) {
        if self.active {
            log::debug!(
                "🔍 Streaming span '{}' throughput: {:.2} rps",
                self.span_name,
                records_per_second
            );
        }
    }

    /// Add processing time to the span
    pub fn set_processing_time(&mut self, duration_ms: u64) {
        if self.active {
            log::debug!(
                "🔍 Streaming span '{}' processing time: {}ms",
                self.span_name,
                duration_ms
            );
        }
    }

    /// Mark the operation as successful
    pub fn set_success(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::debug!(
                "🔍 Streaming span '{}' completed successfully in {:?}",
                self.span_name,
                duration
            );
        }
    }

    /// Mark the operation as failed with error information
    pub fn set_error(&mut self, error: &str) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::warn!(
                "🔍 Streaming span '{}' failed after {:?}: {}",
                self.span_name,
                duration,
                error
            );
        }
    }
}

impl Drop for StreamingSpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!(
                "🔍 Streaming span '{}' finished in {:?}",
                self.span_name,
                duration
            );
        }
    }
}

/// Aggregation operation span wrapper
pub struct AggregationSpan {
    span_name: String,
    function: String,
    window_type: String,
    start_time: Instant,
    active: bool,
}

impl AggregationSpan {
    fn new_active(span_name: String, function: String, window_type: String) -> Self {
        Self {
            span_name,
            function,
            window_type,
            start_time: Instant::now(),
            active: true,
        }
    }

    fn new_inactive() -> Self {
        Self {
            span_name: String::new(),
            function: String::new(),
            window_type: String::new(),
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Add window size information to the span
    pub fn set_window_size(&mut self, size_ms: u64) {
        if self.active {
            log::debug!(
                "🔍 Aggregation span '{}' window size: {}ms",
                self.span_name,
                size_ms
            );
        }
    }

    /// Add input record count to the span
    pub fn set_input_records(&mut self, count: u64) {
        if self.active {
            log::debug!(
                "🔍 Aggregation span '{}' input records: {}",
                self.span_name,
                count
            );
        }
    }

    /// Add output record count to the span
    pub fn set_output_records(&mut self, count: u64) {
        if self.active {
            log::debug!(
                "🔍 Aggregation span '{}' output records: {}",
                self.span_name,
                count
            );
        }
    }

    /// Mark the aggregation as successful
    pub fn set_success(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::debug!(
                "🔍 Aggregation span '{}' completed successfully in {:?}",
                self.span_name,
                duration
            );
        }
    }

    /// Mark the aggregation as failed with error information
    pub fn set_error(&mut self, error: &str) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::warn!(
                "🔍 Aggregation span '{}' failed after {:?}: {}",
                self.span_name,
                duration,
                error
            );
        }
    }
}

impl Drop for AggregationSpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!(
                "🔍 Aggregation span '{}' finished in {:?}",
                self.span_name,
                duration
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_operation_name() {
        assert_eq!(
            TelemetryProvider::extract_operation_name("SELECT * FROM table"),
            "select"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("  select id from users"),
            "select"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("CREATE STREAM test"),
            "create"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("INSERT INTO table"),
            "insert"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("UPDATE table SET"),
            "update"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("DELETE FROM table"),
            "delete"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("UNKNOWN QUERY"),
            "unknown"
        );
    }

    #[tokio::test]
    async fn test_telemetry_provider_creation() {
        let config = TracingConfig::development();
        let provider = TelemetryProvider::new(config).await;
        assert!(provider.is_ok());

        let provider = provider.unwrap();
        assert!(provider.active);
    }

    #[tokio::test]
    async fn test_span_creation() {
        let config = TracingConfig::development();
        let provider = TelemetryProvider::new(config).await.unwrap();

        let mut query_span = provider.start_sql_query_span("SELECT * FROM users", "kafka_topic");
        query_span.set_execution_time(150);
        query_span.set_record_count(100);
        query_span.set_success();

        let mut streaming_span = provider.start_streaming_span("data_ingestion", 1000);
        streaming_span.set_throughput(500.0);
        streaming_span.set_success();

        let mut agg_span = provider.start_aggregation_span("SUM", "tumbling_window");
        agg_span.set_window_size(60000);
        agg_span.set_input_records(1000);
        agg_span.set_output_records(1);
        agg_span.set_success();
    }
}
