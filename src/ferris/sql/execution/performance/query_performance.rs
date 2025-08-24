/*!
# Query Performance Tracking

Core data structures for tracking individual query performance metrics.
Provides detailed execution statistics, timing, and resource usage tracking.
*/

use super::metrics::MemoryMetrics;
use std::time::{Duration, Instant};

/// Query execution performance data
#[derive(Debug, Clone)]
pub struct QueryPerformance {
    /// Query identifier for tracking
    pub query_id: String,
    /// SQL query text (truncated for logging)
    pub query_text: String,
    /// Total execution time
    pub execution_time: Duration,
    /// Time spent in each processor
    pub processor_times: std::collections::HashMap<String, Duration>,
    /// Memory usage during execution
    pub memory_metrics: MemoryMetrics,
    /// Records processed
    pub records_processed: u64,
    /// Bytes processed
    pub bytes_processed: u64,
    /// Query start timestamp
    pub started_at: Instant,
    /// Query completion timestamp
    pub completed_at: Instant,
}

impl QueryPerformance {
    /// Create new query performance tracker
    pub fn new(query_id: String, query_text: String) -> Self {
        let now = Instant::now();
        Self {
            query_id,
            query_text,
            execution_time: Duration::default(),
            processor_times: std::collections::HashMap::new(),
            memory_metrics: MemoryMetrics::default(),
            records_processed: 0,
            bytes_processed: 0,
            started_at: now,
            completed_at: now,
        }
    }

    /// Calculate throughput in records per second
    pub fn records_per_second(&self) -> f64 {
        if self.execution_time.is_zero() {
            0.0
        } else {
            self.records_processed as f64 / self.execution_time.as_secs_f64()
        }
    }

    /// Calculate throughput in bytes per second
    pub fn bytes_per_second(&self) -> f64 {
        if self.execution_time.is_zero() {
            0.0
        } else {
            self.bytes_processed as f64 / self.execution_time.as_secs_f64()
        }
    }

    /// Get processor time percentage breakdown
    pub fn processor_time_breakdown(&self) -> std::collections::HashMap<String, f64> {
        let mut breakdown = std::collections::HashMap::new();
        let total_time = self.execution_time.as_nanos() as f64;

        if total_time > 0.0 {
            for (processor, duration) in &self.processor_times {
                let percentage = (duration.as_nanos() as f64 / total_time) * 100.0;
                breakdown.insert(processor.clone(), percentage);
            }
        }

        breakdown
    }

    /// Get execution summary as human-readable string
    pub fn summary(&self) -> String {
        format!(
            "Query [{}]: {}ms, {} records, {:.1} MB memory",
            self.query_id,
            self.execution_time.as_millis(),
            self.records_processed,
            self.memory_metrics.allocated_bytes as f64 / 1024.0 / 1024.0
        )
    }

    /// Check if query execution was efficient (good performance)
    pub fn is_efficient(&self) -> bool {
        const MAX_LATENCY_MS: u128 = 1000;
        const MIN_THROUGHPUT_RPS: f64 = 100.0;

        self.execution_time.as_millis() < MAX_LATENCY_MS
            && self.records_per_second() >= MIN_THROUGHPUT_RPS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_performance_creation() {
        let perf = QueryPerformance::new("q1".to_string(), "SELECT * FROM test".to_string());
        assert_eq!(perf.query_id, "q1");
        assert_eq!(perf.query_text, "SELECT * FROM test");
        assert_eq!(perf.records_processed, 0);
        assert_eq!(perf.bytes_processed, 0);
    }

    #[test]
    fn test_throughput_calculations() {
        let mut perf = QueryPerformance::new("q1".to_string(), "SELECT * FROM test".to_string());
        perf.records_processed = 1000;
        perf.bytes_processed = 10000;
        perf.execution_time = Duration::from_secs(1);

        assert_eq!(perf.records_per_second(), 1000.0);
        assert_eq!(perf.bytes_per_second(), 10000.0);
    }

    #[test]
    fn test_processor_breakdown() {
        let mut perf = QueryPerformance::new("q1".to_string(), "SELECT * FROM test".to_string());
        perf.execution_time = Duration::from_millis(100);
        perf.processor_times.insert("SelectProcessor".to_string(), Duration::from_millis(60));
        perf.processor_times.insert("FilterProcessor".to_string(), Duration::from_millis(40));

        let breakdown = perf.processor_time_breakdown();
        assert_eq!(breakdown.get("SelectProcessor").unwrap(), &60.0);
        assert_eq!(breakdown.get("FilterProcessor").unwrap(), &40.0);
    }

    #[test]
    fn test_efficiency_check() {
        let mut perf = QueryPerformance::new("q1".to_string(), "SELECT * FROM test".to_string());
        
        // Efficient query
        perf.execution_time = Duration::from_millis(500);
        perf.records_processed = 1000;
        assert!(perf.is_efficient());

        // Inefficient - high latency
        perf.execution_time = Duration::from_millis(2000);
        assert!(!perf.is_efficient());

        // Inefficient - low throughput
        perf.execution_time = Duration::from_millis(500);
        perf.records_processed = 10;
        assert!(!perf.is_efficient());
    }
}