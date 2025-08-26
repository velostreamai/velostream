/*!
# Query Statistics Collection

Provides detailed statistics collection for individual queries and overall system performance.
This module tracks query execution patterns, resource usage, and performance trends.
*/

use super::metrics::{PerformanceMetrics, ThroughputMetrics};
use super::QueryPerformance;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Rolling window of query statistics
#[derive(Debug)]
pub struct QueryStatistics {
    /// Maximum number of queries to keep in history
    max_history: usize,
    /// Recent query performance data
    query_history: VecDeque<QueryPerformance>,
    /// Aggregated metrics
    aggregated_metrics: PerformanceMetrics,
    /// Query pattern statistics
    query_patterns: HashMap<String, QueryPatternStats>,
    /// Last statistics update
    last_update: Instant,
}

impl QueryStatistics {
    /// Create new query statistics collector
    pub fn new(max_history: usize) -> Self {
        Self {
            max_history,
            query_history: VecDeque::with_capacity(max_history),
            aggregated_metrics: PerformanceMetrics::new(),
            query_patterns: HashMap::new(),
            last_update: Instant::now(),
        }
    }

    /// Add a query performance record
    pub fn add_query_performance(&mut self, performance: QueryPerformance) {
        // Update aggregated metrics
        self.aggregated_metrics.update_with_query(&performance);

        // Update query patterns
        let pattern_key = self.extract_query_pattern(&performance.query_text);
        let pattern_stats = self
            .query_patterns
            .entry(pattern_key)
            .or_default();
        pattern_stats.update(&performance);

        // Add to history (with size limit)
        if self.query_history.len() >= self.max_history {
            self.query_history.pop_front();
        }
        self.query_history.push_back(performance);

        self.last_update = Instant::now();
    }

    /// Extract query pattern for classification (removes literals, parameters)
    fn extract_query_pattern(&self, query_text: &str) -> String {
        // Simple pattern extraction - normalize common variations
        let mut pattern = query_text.to_uppercase();

        // Remove string literals
        pattern = regex::Regex::new(r"'[^']*'")
            .unwrap_or_else(|_| regex::Regex::new("").unwrap())
            .replace_all(&pattern, "'?'")
            .to_string();

        // Remove numeric literals
        pattern = regex::Regex::new(r"\b\d+\b")
            .unwrap_or_else(|_| regex::Regex::new("").unwrap())
            .replace_all(&pattern, "?")
            .to_string();

        // Truncate to reasonable length
        if pattern.len() > 200 {
            pattern.truncate(200);
            pattern.push_str("...");
        }

        pattern
    }

    /// Get recent query performance (last N queries)
    pub fn get_recent_queries(&self, limit: usize) -> Vec<&QueryPerformance> {
        self.query_history.iter().rev().take(limit).collect()
    }

    /// Get aggregated performance metrics
    pub fn get_aggregated_metrics(&self) -> &PerformanceMetrics {
        &self.aggregated_metrics
    }

    /// Get query patterns sorted by frequency
    pub fn get_top_query_patterns(&self, limit: usize) -> Vec<(&String, &QueryPatternStats)> {
        let mut patterns: Vec<_> = self.query_patterns.iter().collect();
        patterns.sort_by(|a, b| b.1.execution_count.cmp(&a.1.execution_count));
        patterns.into_iter().take(limit).collect()
    }

    /// Calculate statistics for a time window
    pub fn get_window_statistics(&self, window_duration: Duration) -> WindowStatistics {
        let cutoff_time = self.last_update - window_duration;

        let relevant_queries: Vec<&QueryPerformance> = self
            .query_history
            .iter()
            .filter(|q| q.completed_at >= cutoff_time)
            .collect();

        if relevant_queries.is_empty() {
            return WindowStatistics::default();
        }

        let total_queries = relevant_queries.len() as u64;
        let total_records: u64 = relevant_queries.iter().map(|q| q.records_processed).sum();
        let total_bytes: u64 = relevant_queries.iter().map(|q| q.bytes_processed).sum();

        let mut execution_times: Vec<Duration> =
            relevant_queries.iter().map(|q| q.execution_time).collect();
        execution_times.sort();

        let avg_execution_time = if total_queries > 0 {
            execution_times.iter().sum::<Duration>() / total_queries as u32
        } else {
            Duration::default()
        };

        let throughput = ThroughputMetrics::calculate(
            window_duration,
            total_records,
            total_bytes,
            total_queries,
        );

        // Calculate percentiles
        let p50 = percentile(&execution_times, 0.5);
        let p95 = percentile(&execution_times, 0.95);
        let p99 = percentile(&execution_times, 0.99);

        WindowStatistics {
            window_duration,
            query_count: total_queries,
            total_records_processed: total_records,
            total_bytes_processed: total_bytes,
            avg_execution_time,
            p50_execution_time: p50,
            p95_execution_time: p95,
            p99_execution_time: p99,
            throughput,
        }
    }

    /// Get slowest queries in recent history
    pub fn get_slowest_queries(&self, limit: usize) -> Vec<&QueryPerformance> {
        let mut queries: Vec<&QueryPerformance> = self.query_history.iter().collect();
        queries.sort_by(|a, b| b.execution_time.cmp(&a.execution_time));
        queries.into_iter().take(limit).collect()
    }

    /// Get memory usage trend
    pub fn get_memory_trend(&self, window_duration: Duration) -> MemoryTrend {
        let cutoff_time = self.last_update - window_duration;

        let recent_queries: Vec<&QueryPerformance> = self
            .query_history
            .iter()
            .filter(|q| q.completed_at >= cutoff_time)
            .collect();

        if recent_queries.is_empty() {
            return MemoryTrend::default();
        }

        let total_memory: u64 = recent_queries
            .iter()
            .map(|q| q.memory_metrics.allocated_bytes)
            .sum();

        let peak_memory: u64 = recent_queries
            .iter()
            .map(|q| q.memory_metrics.peak_memory_bytes)
            .max()
            .unwrap_or(0);

        let avg_memory = total_memory / recent_queries.len() as u64;

        MemoryTrend {
            window_duration,
            avg_memory_usage: avg_memory,
            peak_memory_usage: peak_memory,
            total_allocations: recent_queries.len() as u64,
        }
    }
}

/// Statistics for a specific query pattern
#[derive(Debug, Clone)]
pub struct QueryPatternStats {
    /// Number of times this pattern was executed
    pub execution_count: u64,
    /// Total execution time for all executions
    pub total_execution_time: Duration,
    /// Average execution time
    pub avg_execution_time: Duration,
    /// Fastest execution
    pub min_execution_time: Duration,
    /// Slowest execution  
    pub max_execution_time: Duration,
    /// Total records processed
    pub total_records_processed: u64,
    /// Total memory used
    pub total_memory_used: u64,
    /// First seen timestamp
    pub first_seen: Instant,
    /// Last seen timestamp
    pub last_seen: Instant,
}

impl Default for QueryPatternStats {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryPatternStats {
    /// Create new query pattern statistics
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            execution_count: 0,
            total_execution_time: Duration::default(),
            avg_execution_time: Duration::default(),
            min_execution_time: Duration::MAX,
            max_execution_time: Duration::default(),
            total_records_processed: 0,
            total_memory_used: 0,
            first_seen: now,
            last_seen: now,
        }
    }

    /// Update statistics with new query execution
    pub fn update(&mut self, performance: &QueryPerformance) {
        self.execution_count += 1;
        self.total_execution_time += performance.execution_time;
        self.avg_execution_time = self.total_execution_time / self.execution_count as u32;

        if performance.execution_time < self.min_execution_time {
            self.min_execution_time = performance.execution_time;
        }
        if performance.execution_time > self.max_execution_time {
            self.max_execution_time = performance.execution_time;
        }

        self.total_records_processed += performance.records_processed;
        self.total_memory_used += performance.memory_metrics.allocated_bytes;
        self.last_seen = performance.completed_at;
    }

    /// Calculate efficiency (records per second)
    pub fn efficiency(&self) -> f64 {
        if self.total_execution_time.is_zero() {
            0.0
        } else {
            self.total_records_processed as f64 / self.total_execution_time.as_secs_f64()
        }
    }

    /// Calculate memory efficiency (records per MB)
    pub fn memory_efficiency(&self) -> f64 {
        if self.total_memory_used == 0 {
            0.0
        } else {
            let memory_mb = self.total_memory_used as f64 / 1024.0 / 1024.0;
            self.total_records_processed as f64 / memory_mb
        }
    }
}

/// Statistics for a specific time window
#[derive(Debug, Clone, Default)]
pub struct WindowStatistics {
    /// Duration of the window
    pub window_duration: Duration,
    /// Number of queries in window
    pub query_count: u64,
    /// Total records processed in window
    pub total_records_processed: u64,
    /// Total bytes processed in window
    pub total_bytes_processed: u64,
    /// Average execution time
    pub avg_execution_time: Duration,
    /// 50th percentile execution time
    pub p50_execution_time: Duration,
    /// 95th percentile execution time
    pub p95_execution_time: Duration,
    /// 99th percentile execution time
    pub p99_execution_time: Duration,
    /// Throughput metrics
    pub throughput: ThroughputMetrics,
}

/// Memory usage trend information
#[derive(Debug, Clone, Default)]
pub struct MemoryTrend {
    /// Duration analyzed
    pub window_duration: Duration,
    /// Average memory usage
    pub avg_memory_usage: u64,
    /// Peak memory usage
    pub peak_memory_usage: u64,
    /// Total memory allocations
    pub total_allocations: u64,
}

/// Thread-safe statistics collector
#[derive(Debug)]
pub struct StatisticsCollector {
    /// Internal statistics (protected by mutex)
    stats: Arc<Mutex<QueryStatistics>>,
    /// Global performance counters (lock-free)
    global_counters: Arc<super::metrics::AtomicMetricsCollector>,
}

impl StatisticsCollector {
    /// Create new statistics collector
    pub fn new(max_history: usize) -> Self {
        Self {
            stats: Arc::new(Mutex::new(QueryStatistics::new(max_history))),
            global_counters: Arc::new(super::metrics::AtomicMetricsCollector::new()),
        }
    }

    /// Record query performance
    pub fn record_query_performance(&self, performance: QueryPerformance) {
        // Update atomic counters first (fast path)
        self.global_counters.record_query(
            performance.records_processed,
            performance.bytes_processed,
            performance.execution_time,
        );

        // Update detailed statistics (slower, but provides rich data)
        if let Ok(mut stats) = self.stats.lock() {
            stats.add_query_performance(performance);
        }
    }

    /// Get current aggregated metrics
    pub fn get_current_metrics(&self) -> PerformanceMetrics {
        self.global_counters.snapshot()
    }

    /// Get detailed window statistics  
    pub fn get_window_statistics(&self, window_duration: Duration) -> Option<WindowStatistics> {
        self.stats
            .lock()
            .ok()
            .map(|stats| stats.get_window_statistics(window_duration))
    }

    /// Get top query patterns
    pub fn get_top_query_patterns(&self, limit: usize) -> Vec<(String, QueryPatternStats)> {
        self.stats
            .lock()
            .ok()
            .map(|stats| {
                stats
                    .get_top_query_patterns(limit)
                    .into_iter()
                    .map(|(pattern, stats)| (pattern.clone(), stats.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get recent slow queries
    pub fn get_recent_slow_queries(&self, limit: usize) -> Vec<QueryPerformance> {
        self.stats
            .lock()
            .ok()
            .map(|stats| {
                stats
                    .get_slowest_queries(limit)
                    .into_iter()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.global_counters.reset();
        if let Ok(mut stats) = self.stats.lock() {
            *stats = QueryStatistics::new(stats.max_history);
        }
    }

    /// Export metrics for Prometheus (simplified format)
    pub fn export_prometheus_metrics(&self) -> String {
        let metrics = self.get_current_metrics();

        format!(
            "# HELP ferrisstreams_queries_total Total number of queries executed\n\
            # TYPE ferrisstreams_queries_total counter\n\
            ferrisstreams_queries_total {}\n\
            \n\
            # HELP ferrisstreams_records_processed_total Total number of records processed\n\
            # TYPE ferrisstreams_records_processed_total counter\n\
            ferrisstreams_records_processed_total {}\n\
            \n\
            # HELP ferrisstreams_bytes_processed_total Total number of bytes processed\n\
            # TYPE ferrisstreams_bytes_processed_total counter\n\
            ferrisstreams_bytes_processed_total {}\n\
            \n\
            # HELP ferrisstreams_query_duration_seconds Query execution duration\n\
            # TYPE ferrisstreams_query_duration_seconds gauge\n\
            ferrisstreams_query_duration_seconds {}\n\
            \n\
            # HELP ferrisstreams_memory_allocated_bytes Total memory allocated\n\
            # TYPE ferrisstreams_memory_allocated_bytes gauge\n\
            ferrisstreams_memory_allocated_bytes {}\n\
            \n\
            # HELP ferrisstreams_throughput_records_per_second Current records per second\n\
            # TYPE ferrisstreams_throughput_records_per_second gauge\n\
            ferrisstreams_throughput_records_per_second {}\n",
            metrics.total_queries,
            metrics
                .processors
                .values()
                .map(|p| p.records_processed)
                .sum::<u64>(),
            metrics.memory.allocated_bytes,
            metrics.total_execution_time.as_secs_f64(),
            metrics.memory.allocated_bytes,
            metrics.throughput.records_per_second,
        )
    }
}

impl Clone for StatisticsCollector {
    fn clone(&self) -> Self {
        Self {
            stats: Arc::clone(&self.stats),
            global_counters: Arc::clone(&self.global_counters),
        }
    }
}

/// Calculate percentile from sorted duration vector using nearest-rank method
fn percentile(sorted_durations: &[Duration], percentile: f64) -> Duration {
    if sorted_durations.is_empty() {
        return Duration::default();
    }

    if percentile <= 0.0 {
        return sorted_durations[0];
    }

    if percentile >= 1.0 {
        return sorted_durations[sorted_durations.len() - 1];
    }

    // Use nearest-rank method: index = ceil(percentile * n) - 1
    let n = sorted_durations.len() as f64;
    let index = ((percentile * n).ceil() as usize).saturating_sub(1);
    sorted_durations[index.min(sorted_durations.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_performance(
        query_id: &str,
        execution_time_ms: u64,
        records: u64,
    ) -> QueryPerformance {
        let mut perf = QueryPerformance::new(
            query_id.to_string(),
            format!("SELECT * FROM test WHERE id = {}", records),
        );
        perf.execution_time = Duration::from_millis(execution_time_ms);
        perf.records_processed = records;
        perf.bytes_processed = records * 100; // 100 bytes per record
        perf.completed_at = Instant::now();
        perf
    }

    #[test]
    fn test_query_statistics_basic() {
        let mut stats = QueryStatistics::new(10);

        let perf1 = create_test_performance("q1", 100, 1000);
        let perf2 = create_test_performance("q2", 200, 2000);

        stats.add_query_performance(perf1);
        stats.add_query_performance(perf2);

        let recent = stats.get_recent_queries(5);
        assert_eq!(recent.len(), 2);

        let metrics = stats.get_aggregated_metrics();
        assert_eq!(metrics.total_queries, 2);
    }

    #[test]
    fn test_query_pattern_extraction() {
        let stats = QueryStatistics::new(10);

        let pattern1 = stats.extract_query_pattern("SELECT * FROM users WHERE id = 123");
        let pattern2 = stats.extract_query_pattern("SELECT * FROM users WHERE id = 456");
        let pattern3 = stats.extract_query_pattern("SELECT * FROM users WHERE name = 'John'");

        // Should normalize literals
        assert_eq!(pattern1, pattern2);
        assert_ne!(pattern1, pattern3); // Different pattern
    }

    #[test]
    fn test_window_statistics() {
        let mut stats = QueryStatistics::new(100);

        // Add several test queries
        for i in 1..=10 {
            let perf = create_test_performance(&format!("q{}", i), i * 10, i * 100);
            stats.add_query_performance(perf);
        }

        let window_stats = stats.get_window_statistics(Duration::from_secs(3600));
        assert_eq!(window_stats.query_count, 10);
        assert_eq!(window_stats.total_records_processed, 5500); // sum of 100,200,300...1000
    }

    #[test]
    fn test_query_pattern_stats() {
        let mut pattern_stats = QueryPatternStats::new();

        let perf1 = create_test_performance("q1", 100, 1000);
        let perf2 = create_test_performance("q2", 200, 1500);

        pattern_stats.update(&perf1);
        pattern_stats.update(&perf2);

        assert_eq!(pattern_stats.execution_count, 2);
        assert_eq!(pattern_stats.total_records_processed, 2500);
        assert_eq!(pattern_stats.min_execution_time, Duration::from_millis(100));
        assert_eq!(pattern_stats.max_execution_time, Duration::from_millis(200));
        assert_eq!(pattern_stats.avg_execution_time, Duration::from_millis(150));
    }

    #[test]
    fn test_statistics_collector() {
        let collector = StatisticsCollector::new(50);

        let perf = create_test_performance("test_query", 150, 1000);
        collector.record_query_performance(perf);

        let metrics = collector.get_current_metrics();
        assert_eq!(metrics.total_queries, 1);

        let patterns = collector.get_top_query_patterns(5);
        assert_eq!(patterns.len(), 1);
    }

    #[test]
    fn test_prometheus_export() {
        let collector = StatisticsCollector::new(10);

        let perf = create_test_performance("test", 100, 500);
        collector.record_query_performance(perf);

        let prometheus_output = collector.export_prometheus_metrics();
        assert!(prometheus_output.contains("ferrisstreams_queries_total 1"));
        assert!(prometheus_output.contains("ferrisstreams_records_processed_total"));
        assert!(prometheus_output.contains("ferrisstreams_memory_allocated_bytes"));
    }

    #[test]
    fn test_percentile_calculation() {
        let durations = vec![
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(30),
            Duration::from_millis(40),
            Duration::from_millis(100),
        ];

        assert_eq!(percentile(&durations, 0.5), Duration::from_millis(30));
        assert_eq!(percentile(&durations, 0.95), Duration::from_millis(100));
        assert_eq!(percentile(&durations, 0.0), Duration::from_millis(10));
        assert_eq!(percentile(&durations, 1.0), Duration::from_millis(100));
    }
}
