/*!
# Performance Metrics

Core metrics collection for SQL engine performance monitoring.
Provides memory, throughput, and processor-specific metrics.
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Memory usage metrics
#[derive(Debug, Clone, Default)]
pub struct MemoryMetrics {
    /// Total memory allocated (bytes)
    pub allocated_bytes: u64,
    /// Peak memory usage during execution (bytes)
    pub peak_memory_bytes: u64,
    /// Memory used by GROUP BY accumulators (bytes)
    pub group_by_memory_bytes: u64,
    /// Memory used by window states (bytes)
    pub window_memory_bytes: u64,
    /// Memory used by JOIN operations (bytes)
    pub join_memory_bytes: u64,
    /// Number of memory allocations
    pub allocation_count: u64,
}

impl MemoryMetrics {
    /// Create new empty memory metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Combine with another metrics instance
    pub fn merge(&mut self, other: &MemoryMetrics) {
        self.allocated_bytes += other.allocated_bytes;
        self.peak_memory_bytes = self.peak_memory_bytes.max(other.peak_memory_bytes);
        self.group_by_memory_bytes += other.group_by_memory_bytes;
        self.window_memory_bytes += other.window_memory_bytes;
        self.join_memory_bytes += other.join_memory_bytes;
        self.allocation_count += other.allocation_count;
    }

    /// Calculate memory efficiency (bytes processed / memory used)
    pub fn memory_efficiency(&self, bytes_processed: u64) -> f64 {
        if self.allocated_bytes == 0 {
            0.0
        } else {
            bytes_processed as f64 / self.allocated_bytes as f64
        }
    }
}

/// Throughput metrics
#[derive(Debug, Clone, Default)]
pub struct ThroughputMetrics {
    /// Records processed per second
    pub records_per_second: f64,
    /// Bytes processed per second
    pub bytes_per_second: f64,
    /// Queries processed per second
    pub queries_per_second: f64,
    /// Average query latency (milliseconds)
    pub avg_query_latency_ms: f64,
    /// P50 query latency (milliseconds)
    pub p50_latency_ms: f64,
    /// P95 query latency (milliseconds)
    pub p95_latency_ms: f64,
    /// P99 query latency (milliseconds)
    pub p99_latency_ms: f64,
}

impl ThroughputMetrics {
    /// Calculate throughput from duration and counts
    pub fn calculate(
        duration: Duration,
        records_processed: u64,
        bytes_processed: u64,
        queries_processed: u64,
    ) -> Self {
        let seconds = duration.as_secs_f64();
        if seconds == 0.0 {
            return Self::default();
        }

        Self {
            records_per_second: records_processed as f64 / seconds,
            bytes_per_second: bytes_processed as f64 / seconds,
            queries_per_second: queries_processed as f64 / seconds,
            avg_query_latency_ms: (duration.as_millis() as f64) / queries_processed as f64,
            ..Default::default()
        }
    }

    /// Update percentile metrics from latency samples
    pub fn update_percentiles(&mut self, latencies: &mut [Duration]) {
        if latencies.is_empty() {
            return;
        }

        latencies.sort();
        let len = latencies.len();

        // Calculate percentiles
        let p50_idx = (len as f64 * 0.50) as usize;
        let p95_idx = (len as f64 * 0.95) as usize;
        let p99_idx = (len as f64 * 0.99) as usize;

        self.p50_latency_ms = latencies[p50_idx.min(len - 1)].as_millis() as f64;
        self.p95_latency_ms = latencies[p95_idx.min(len - 1)].as_millis() as f64;
        self.p99_latency_ms = latencies[p99_idx.min(len - 1)].as_millis() as f64;
    }
}

/// Per-processor performance metrics
#[derive(Debug, Clone, Default)]
pub struct ProcessorMetrics {
    /// Processor name
    pub processor_name: String,
    /// Number of records processed
    pub records_processed: u64,
    /// Total execution time
    pub execution_time: Duration,
    /// Memory used
    pub memory_usage: MemoryMetrics,
    /// Error count
    pub error_count: u64,
    /// Success count
    pub success_count: u64,
}

impl ProcessorMetrics {
    /// Create new processor metrics
    pub fn new(processor_name: String) -> Self {
        Self {
            processor_name,
            ..Default::default()
        }
    }

    /// Calculate processor efficiency (records/second)
    pub fn efficiency(&self) -> f64 {
        if self.execution_time.is_zero() {
            0.0
        } else {
            self.records_processed as f64 / self.execution_time.as_secs_f64()
        }
    }

    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.success_count + self.error_count;
        if total == 0 {
            0.0
        } else {
            self.success_count as f64 / total as f64
        }
    }

    /// Update metrics with new execution
    pub fn update(
        &mut self,
        records: u64,
        duration: Duration,
        memory: MemoryMetrics,
        success: bool,
    ) {
        self.records_processed += records;
        self.execution_time += duration;
        self.memory_usage.merge(&memory);

        if success {
            self.success_count += 1;
        } else {
            self.error_count += 1;
        }
    }
}

/// Overall performance metrics for the SQL engine
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Memory usage metrics
    pub memory: MemoryMetrics,
    /// Throughput metrics
    pub throughput: ThroughputMetrics,
    /// Per-processor metrics
    pub processors: HashMap<String, ProcessorMetrics>,
    /// Total queries executed
    pub total_queries: u64,
    /// Total execution time across all queries
    pub total_execution_time: Duration,
    /// Metrics collection start time
    pub start_time: Instant,
    /// Last update time
    pub last_update: Instant,
}

impl PerformanceMetrics {
    /// Create new performance metrics
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            memory: MemoryMetrics::default(),
            throughput: ThroughputMetrics::default(),
            processors: HashMap::new(),
            total_queries: 0,
            total_execution_time: Duration::default(),
            start_time: now,
            last_update: now,
        }
    }

    /// Update metrics with query performance data
    pub fn update_with_query(&mut self, query_performance: &super::QueryPerformance) {
        self.memory.merge(&query_performance.memory_metrics);
        self.total_queries += 1;
        self.total_execution_time += query_performance.execution_time;
        self.last_update = Instant::now();

        // Update processor-specific metrics
        for (processor_name, duration) in &query_performance.processor_times {
            let processor_metrics = self
                .processors
                .entry(processor_name.clone())
                .or_insert_with(|| ProcessorMetrics::new(processor_name.clone()));

            processor_metrics.update(
                query_performance.records_processed,
                *duration,
                query_performance.memory_metrics.clone(),
                true, // Assume success if we got performance data
            );
        }

        // Recalculate throughput metrics
        self.recalculate_throughput();
    }

    /// Recalculate throughput based on current metrics
    fn recalculate_throughput(&mut self) {
        let duration = self.last_update.duration_since(self.start_time);
        let total_records: u64 = self.processors.values().map(|p| p.records_processed).sum();
        let total_bytes = self.memory.allocated_bytes;

        self.throughput =
            ThroughputMetrics::calculate(duration, total_records, total_bytes, self.total_queries);
    }

    /// Get top processors by execution time
    pub fn top_processors_by_time(&self, limit: usize) -> Vec<&ProcessorMetrics> {
        let mut processors: Vec<&ProcessorMetrics> = self.processors.values().collect();
        processors.sort_by(|a, b| b.execution_time.cmp(&a.execution_time));
        processors.into_iter().take(limit).collect()
    }

    /// Get overall efficiency (records per second across all processors)
    pub fn overall_efficiency(&self) -> f64 {
        if self.total_execution_time.is_zero() {
            0.0
        } else {
            let total_records: u64 = self.processors.values().map(|p| p.records_processed).sum();
            total_records as f64 / self.total_execution_time.as_secs_f64()
        }
    }

    /// Get memory utilization summary
    pub fn memory_summary(&self) -> String {
        format!(
            "Total: {:.1}MB, Peak: {:.1}MB, GROUP BY: {:.1}MB, Windows: {:.1}MB, JOINs: {:.1}MB",
            self.memory.allocated_bytes as f64 / 1024.0 / 1024.0,
            self.memory.peak_memory_bytes as f64 / 1024.0 / 1024.0,
            self.memory.group_by_memory_bytes as f64 / 1024.0 / 1024.0,
            self.memory.window_memory_bytes as f64 / 1024.0 / 1024.0,
            self.memory.join_memory_bytes as f64 / 1024.0 / 1024.0,
        )
    }
}

/// Thread-safe metrics collector using atomic counters
#[derive(Debug)]
pub struct AtomicMetricsCollector {
    /// Total records processed
    pub records_processed: AtomicU64,
    /// Total bytes processed  
    pub bytes_processed: AtomicU64,
    /// Total queries executed
    pub queries_executed: AtomicU64,
    /// Total execution time (microseconds)
    pub total_execution_time_us: AtomicU64,
    /// Memory allocations
    pub memory_allocations: AtomicU64,
    /// Peak memory usage
    pub peak_memory_bytes: AtomicU64,
}

impl AtomicMetricsCollector {
    /// Create new atomic metrics collector
    pub fn new() -> Self {
        Self {
            records_processed: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
            total_execution_time_us: AtomicU64::new(0),
            memory_allocations: AtomicU64::new(0),
            peak_memory_bytes: AtomicU64::new(0),
        }
    }

    /// Record query execution
    pub fn record_query(&self, records: u64, bytes: u64, duration: Duration) {
        self.records_processed.fetch_add(records, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record memory allocation
    pub fn record_memory_allocation(&self, bytes: u64) {
        self.memory_allocations.fetch_add(1, Ordering::Relaxed);

        // Update peak memory (approximate, race condition possible but acceptable)
        let current_peak = self.peak_memory_bytes.load(Ordering::Relaxed);
        if bytes > current_peak {
            self.peak_memory_bytes
                .compare_exchange(current_peak, bytes, Ordering::Relaxed, Ordering::Relaxed)
                .ok();
        }
    }

    /// Get current snapshot of metrics
    pub fn snapshot(&self) -> PerformanceMetrics {
        let records = self.records_processed.load(Ordering::Relaxed);
        let bytes = self.bytes_processed.load(Ordering::Relaxed);
        let queries = self.queries_executed.load(Ordering::Relaxed);
        let total_time_us = self.total_execution_time_us.load(Ordering::Relaxed);
        let allocations = self.memory_allocations.load(Ordering::Relaxed);
        let peak_memory = self.peak_memory_bytes.load(Ordering::Relaxed);

        let total_time = Duration::from_micros(total_time_us);
        let throughput = ThroughputMetrics::calculate(total_time, records, bytes, queries);

        let memory = MemoryMetrics {
            allocated_bytes: bytes,
            peak_memory_bytes: peak_memory,
            allocation_count: allocations,
            ..Default::default()
        };

        PerformanceMetrics {
            memory,
            throughput,
            processors: HashMap::new(),
            total_queries: queries,
            total_execution_time: total_time,
            start_time: Instant::now(),
            last_update: Instant::now(),
        }
    }

    /// Reset all counters
    pub fn reset(&self) {
        self.records_processed.store(0, Ordering::Relaxed);
        self.bytes_processed.store(0, Ordering::Relaxed);
        self.queries_executed.store(0, Ordering::Relaxed);
        self.total_execution_time_us.store(0, Ordering::Relaxed);
        self.memory_allocations.store(0, Ordering::Relaxed);
        self.peak_memory_bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for AtomicMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_memory_metrics() {
        let mut metrics = MemoryMetrics::new();
        metrics.allocated_bytes = 1000;
        metrics.peak_memory_bytes = 1500;

        let mut other = MemoryMetrics::new();
        other.allocated_bytes = 500;
        other.peak_memory_bytes = 800;

        metrics.merge(&other);

        assert_eq!(metrics.allocated_bytes, 1500);
        assert_eq!(metrics.peak_memory_bytes, 1500); // Max of both
    }

    #[test]
    fn test_throughput_calculation() {
        let duration = Duration::from_secs(1);
        let throughput = ThroughputMetrics::calculate(duration, 1000, 8000, 10);

        assert_eq!(throughput.records_per_second, 1000.0);
        assert_eq!(throughput.bytes_per_second, 8000.0);
        assert_eq!(throughput.queries_per_second, 10.0);
        assert_eq!(throughput.avg_query_latency_ms, 100.0);
    }

    #[test]
    fn test_processor_metrics() {
        let mut metrics = ProcessorMetrics::new("TestProcessor".to_string());

        metrics.update(
            100,
            Duration::from_millis(50),
            MemoryMetrics::default(),
            true,
        );

        metrics.update(
            200,
            Duration::from_millis(100),
            MemoryMetrics::default(),
            false,
        );

        assert_eq!(metrics.records_processed, 300);
        assert_eq!(metrics.execution_time, Duration::from_millis(150));
        assert_eq!(metrics.success_count, 1);
        assert_eq!(metrics.error_count, 1);
        assert_eq!(metrics.success_rate(), 0.5);
        assert_eq!(metrics.efficiency(), 2000.0); // 300 records / 0.15 seconds = 2000 records/sec
    }

    #[test]
    fn test_atomic_collector() {
        let collector = AtomicMetricsCollector::new();

        collector.record_query(100, 1000, Duration::from_millis(10));
        collector.record_query(200, 2000, Duration::from_millis(20));
        collector.record_memory_allocation(5000);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.total_queries, 2);
        assert_eq!(snapshot.memory.allocated_bytes, 3000);
        assert_eq!(snapshot.memory.peak_memory_bytes, 5000);
    }

    #[test]
    fn test_percentile_calculation() {
        let mut throughput = ThroughputMetrics::default();
        let mut latencies = vec![
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(30),
            Duration::from_millis(40),
            Duration::from_millis(100),
        ];

        throughput.update_percentiles(&mut latencies);

        assert_eq!(throughput.p50_latency_ms, 30.0);
        assert_eq!(throughput.p95_latency_ms, 100.0);
        assert_eq!(throughput.p99_latency_ms, 100.0);
    }
}
