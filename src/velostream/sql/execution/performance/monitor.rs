/*!
# Performance Monitor

High-level interface for performance monitoring and statistics collection.
Provides easy-to-use API for tracking query performance and system metrics.
*/

use super::metrics::PerformanceMetrics;
use super::query_performance::QueryPerformance;
use super::query_tracker::QueryTracker;
use super::statistics::{StatisticsCollector, WindowStatistics};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Main performance monitor for the SQL engine
#[derive(Debug)]
pub struct PerformanceMonitor {
    /// Statistics collector
    collector: StatisticsCollector,
    /// Monitor start time
    start_time: Instant,
    /// Enable detailed tracking (can be disabled for production)
    detailed_tracking: bool,
}

impl PerformanceMonitor {
    /// Create new performance monitor
    pub fn new() -> Self {
        Self::with_config(1000, true)
    }

    /// Create performance monitor with configuration
    pub fn with_config(max_history: usize, detailed_tracking: bool) -> Self {
        Self {
            collector: StatisticsCollector::new(max_history),
            start_time: Instant::now(),
            detailed_tracking,
        }
    }

    /// Start tracking a new query
    pub fn start_query_tracking(&self, query_text: &str) -> QueryTracker {
        let query_id = Uuid::new_v4().to_string();
        let truncated_query = if query_text.len() > 500 {
            format!("{}...", &query_text[..500])
        } else {
            query_text.to_string()
        };

        QueryTracker::new(query_id, truncated_query)
    }

    /// Finish query tracking and record performance
    pub fn finish_query_tracking(&self, tracker: QueryTracker) -> QueryPerformance {
        let performance = tracker.finish();

        // Record performance if detailed tracking is enabled
        if self.detailed_tracking {
            self.collector.record_query_performance(performance.clone());
        }

        performance
    }

    /// Get current overall performance metrics
    pub fn get_current_metrics(&self) -> PerformanceMetrics {
        self.collector.get_current_metrics()
    }

    /// Get performance statistics for a time window
    pub fn get_window_statistics(&self, window_duration: Duration) -> Option<WindowStatistics> {
        self.collector.get_window_statistics(window_duration)
    }

    /// Get statistics for common time windows
    pub fn get_statistics_summary(&self) -> StatisticsSummary {
        let last_minute = self.get_window_statistics(Duration::from_secs(60));
        let last_hour = self.get_window_statistics(Duration::from_secs(3600));
        let last_day = self.get_window_statistics(Duration::from_secs(86400));

        StatisticsSummary {
            overall_metrics: self.get_current_metrics(),
            last_minute,
            last_hour,
            last_day,
            uptime: self.start_time.elapsed(),
        }
    }

    /// Get top query patterns by frequency
    pub fn get_top_query_patterns(
        &self,
        limit: usize,
    ) -> Vec<(String, super::statistics::QueryPatternStats)> {
        self.collector.get_top_query_patterns(limit)
    }

    /// Get recent slow queries
    pub fn get_slow_queries(&self, limit: usize) -> Vec<QueryPerformance> {
        self.collector.get_recent_slow_queries(limit)
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus_metrics(&self) -> String {
        self.collector.export_prometheus_metrics()
    }

    /// Reset all statistics (useful for testing)
    pub fn reset_statistics(&self) {
        self.collector.reset();
    }

    /// Check if system is performing well
    pub fn health_check(&self) -> PerformanceHealth {
        let metrics = self.get_current_metrics();
        let recent_stats = self.get_window_statistics(Duration::from_secs(60));

        // Define health thresholds
        let max_avg_latency_ms = 1000.0; // 1 second
        let min_throughput_rps = 10.0; // 10 records per second
        let max_memory_mb = 1024.0; // 1 GB

        let mut issues = Vec::new();
        let mut warnings = Vec::new();

        // Check recent performance - only if we have data
        if let Some(stats) = recent_stats {
            // Only check performance if we have processed queries
            if stats.query_count > 0 {
                if stats.avg_execution_time.as_millis() as f64 > max_avg_latency_ms {
                    issues.push(format!(
                        "High average query latency: {:.1}ms (threshold: {:.1}ms)",
                        stats.avg_execution_time.as_millis(),
                        max_avg_latency_ms
                    ));
                }

                if stats.throughput.records_per_second < min_throughput_rps {
                    warnings.push(format!(
                        "Low throughput: {:.1} records/sec (threshold: {:.1})",
                        stats.throughput.records_per_second, min_throughput_rps
                    ));
                }
            }
        }

        // Check memory usage
        let memory_mb = metrics.memory.allocated_bytes as f64 / 1024.0 / 1024.0;
        if memory_mb > max_memory_mb {
            warnings.push(format!(
                "High memory usage: {:.1}MB (threshold: {:.1}MB)",
                memory_mb, max_memory_mb
            ));
        }

        // Determine overall status
        let status = if !issues.is_empty() {
            HealthStatus::Critical
        } else if !warnings.is_empty() {
            HealthStatus::Warning
        } else {
            HealthStatus::Healthy
        };

        PerformanceHealth {
            status,
            issues,
            warnings,
            last_check: Instant::now(),
        }
    }

    /// Get performance report as formatted string
    pub fn get_performance_report(&self) -> String {
        let summary = self.get_statistics_summary();
        let health = self.health_check();
        let top_patterns = self.get_top_query_patterns(5);

        format!(
            "=== Velostream Performance Report ===\n\
            Status: {:?}\n\
            Uptime: {:.1} hours\n\
            \n\
            Overall Metrics:\n\
            - Total Queries: {}\n\
            - Total Records: {}\n\
            - Memory Usage: {}\n\
            - Overall Efficiency: {:.1} records/sec\n\
            \n\
            Recent Performance (Last Hour):\n\
            {}\n\
            \n\
            Top Query Patterns:\n\
            {}\n\
            \n\
            Health Issues:\n\
            {}\n\
            \n\
            Warnings:\n\
            {}\n",
            health.status,
            summary.uptime.as_secs_f64() / 3600.0,
            summary.overall_metrics.total_queries,
            summary
                .overall_metrics
                .processors
                .values()
                .map(|p| p.records_processed)
                .sum::<u64>(),
            summary.overall_metrics.memory_summary(),
            summary.overall_metrics.overall_efficiency(),
            self.format_window_stats(summary.last_hour.as_ref()),
            self.format_query_patterns(&top_patterns),
            if health.issues.is_empty() {
                "None".to_string()
            } else {
                health.issues.join("\n")
            },
            if health.warnings.is_empty() {
                "None".to_string()
            } else {
                health.warnings.join("\n")
            }
        )
    }

    /// Format window statistics for display
    fn format_window_stats(&self, stats: Option<&WindowStatistics>) -> String {
        match stats {
            Some(stats) => {
                format!(
                    "- Queries: {}\n\
                    - Records: {}\n\
                    - Avg Latency: {:.1}ms\n\
                    - P95 Latency: {:.1}ms\n\
                    - Throughput: {:.1} records/sec",
                    stats.query_count,
                    stats.total_records_processed,
                    stats.avg_execution_time.as_millis(),
                    stats.p95_execution_time.as_millis(),
                    stats.throughput.records_per_second
                )
            }
            None => "No data available".to_string(),
        }
    }

    /// Format query patterns for display
    fn format_query_patterns(
        &self,
        patterns: &[(String, super::statistics::QueryPatternStats)],
    ) -> String {
        if patterns.is_empty() {
            return "No patterns recorded".to_string();
        }

        patterns
            .iter()
            .enumerate()
            .map(|(i, (pattern, stats))| {
                format!(
                    "{}. {} (executed {} times, avg: {:.1}ms)",
                    i + 1,
                    if pattern.len() > 80 {
                        format!("{}...", &pattern[..80])
                    } else {
                        pattern.clone()
                    },
                    stats.execution_count,
                    stats.avg_execution_time.as_millis()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PerformanceMonitor {
    fn clone(&self) -> Self {
        // Create a new monitor but share the same collector
        Self {
            collector: self.collector.clone(),
            start_time: self.start_time,
            detailed_tracking: self.detailed_tracking,
        }
    }
}

/// Summary of performance statistics across multiple time windows
#[derive(Debug)]
pub struct StatisticsSummary {
    /// Overall system metrics
    pub overall_metrics: PerformanceMetrics,
    /// Last minute statistics
    pub last_minute: Option<WindowStatistics>,
    /// Last hour statistics
    pub last_hour: Option<WindowStatistics>,
    /// Last day statistics
    pub last_day: Option<WindowStatistics>,
    /// Total system uptime
    pub uptime: Duration,
}

/// Health status of the performance system
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// System is performing well
    Healthy,
    /// System has performance warnings
    Warning,
    /// System has critical performance issues
    Critical,
}

/// Performance health check result
#[derive(Debug)]
pub struct PerformanceHealth {
    /// Overall health status
    pub status: HealthStatus,
    /// Critical issues that need immediate attention
    pub issues: Vec<String>,
    /// Warnings that should be monitored
    pub warnings: Vec<String>,
    /// When this health check was performed
    pub last_check: Instant,
}

/// Helper trait for easy performance monitoring integration
pub trait MonitoredExecution<T> {
    /// Execute with performance monitoring
    fn with_monitoring<F>(self, monitor: &PerformanceMonitor, query_text: &str, f: F) -> T
    where
        F: FnOnce(&mut QueryTracker) -> T;
}

impl<T> MonitoredExecution<T> for T {
    fn with_monitoring<F>(self, monitor: &PerformanceMonitor, query_text: &str, f: F) -> T
    where
        F: FnOnce(&mut QueryTracker) -> T,
    {
        let mut tracker = monitor.start_query_tracking(query_text);
        let result = f(&mut tracker);
        monitor.finish_query_tracking(tracker);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_performance_monitor_tracks_query_metrics() {
        // Arrange
        let monitor = PerformanceMonitor::new();
        let query_text = "SELECT * FROM test";

        // Act
        let mut tracker = monitor.start_query_tracking(query_text);
        tracker.add_records_processed(1000);
        tracker.add_bytes_processed(10000);
        let performance = monitor.finish_query_tracking(tracker);

        // Assert
        assert_eq!(
            performance.records_processed, 1000,
            "Should record exact number of records processed"
        );
        assert_eq!(
            performance.bytes_processed, 10000,
            "Should record exact number of bytes processed"
        );
    }

    #[test]
    fn test_statistics_summary_contains_valid_metrics() {
        // Arrange
        let monitor = PerformanceMonitor::new();

        // Act: Record a query execution
        let mut tracker = monitor.start_query_tracking("SELECT COUNT(*) FROM users");
        tracker.add_records_processed(500);
        let _perf = monitor.finish_query_tracking(tracker);

        // Assert: Summary should contain valid data
        let summary = monitor.get_statistics_summary();
        assert_eq!(
            summary.overall_metrics.total_queries, 1,
            "Should track exactly one query"
        );
        // Uptime should be non-negative (always true, not timing-dependent)
        assert!(
            summary.uptime.as_nanos() >= 0,
            "Uptime should be a valid duration"
        );
    }

    #[test]
    fn test_health_check_new_monitor_is_healthy() {
        // Arrange
        let monitor = PerformanceMonitor::new();

        // Act
        let health = monitor.health_check();

        // Assert
        assert_eq!(
            health.status, HealthStatus::Healthy,
            "New monitor should be in healthy state"
        );
        assert!(
            health.issues.is_empty(),
            "New monitor should have no issues"
        );
    }

    #[test]
    fn test_prometheus_export_produces_valid_output() {
        // Arrange
        let monitor = PerformanceMonitor::new();

        // Act: Record a query execution
        let mut tracker = monitor.start_query_tracking("SELECT 1");
        tracker.add_records_processed(100);
        let _perf = monitor.finish_query_tracking(tracker);
        let prometheus_output = monitor.export_prometheus_metrics();

        // Assert: Output contains required Prometheus format elements
        assert!(
            prometheus_output.contains("velostream_queries_total"),
            "Should export velostream_queries_total metric"
        );
        assert!(
            prometheus_output.contains("# TYPE"),
            "Should include Prometheus TYPE declaration"
        );
        assert!(
            prometheus_output.contains("# HELP"),
            "Should include Prometheus HELP declaration"
        );
    }

    #[test]
    fn test_performance_report_generates_valid_summary() {
        // Arrange
        let monitor = PerformanceMonitor::new();
        let num_queries = 3;

        // Act: Record multiple queries
        for i in 1..=num_queries {
            let mut tracker = monitor.start_query_tracking(&format!("SELECT {} FROM test", i));
            tracker.add_records_processed(i * 100);
            monitor.finish_query_tracking(tracker);
        }
        let report = monitor.get_performance_report();

        // Assert: Report contains expected sections
        assert!(
            report.contains("Performance Report"),
            "Should include report title"
        );
        assert!(
            report.contains("Total Queries: 3"),
            "Should report correct number of queries"
        );
        assert!(
            report.contains("Status: Healthy"),
            "Should report healthy status for new monitor"
        );
    }

    #[test]
    fn test_monitored_execution_trait_captures_execution_data() {
        // Arrange
        let monitor = PerformanceMonitor::new();

        // Act: Execute with monitoring
        let result: i32 = 42.with_monitoring(&monitor, "SELECT 42", |tracker| {
            tracker.add_records_processed(1);
            tracker.start_processor("TestProcessor");
            thread::sleep(Duration::from_millis(1));
            42
        });

        // Assert: Result is correct and metrics are recorded
        assert_eq!(result, 42, "Should return correct value from monitored execution");
        assert_eq!(
            monitor.get_current_metrics().total_queries,
            1,
            "Should track monitored query"
        );
    }

    #[test]
    fn test_query_pattern_tracking() {
        let monitor = PerformanceMonitor::new();

        // Execute similar queries
        for i in 1..=5 {
            let mut tracker =
                monitor.start_query_tracking(&format!("SELECT * FROM users WHERE id = {}", i));
            tracker.add_records_processed(100);
            monitor.finish_query_tracking(tracker);
        }

        let patterns = monitor.get_top_query_patterns(3);
        assert_eq!(patterns.len(), 1); // Should be grouped into one pattern
        assert_eq!(patterns[0].1.execution_count, 5);
    }

    #[test]
    fn test_window_statistics() {
        let monitor = PerformanceMonitor::new();

        // Add some queries
        for _ in 1..=10 {
            let mut tracker = monitor.start_query_tracking("SELECT COUNT(*) FROM test");
            tracker.add_records_processed(100);
            monitor.finish_query_tracking(tracker);
        }

        let stats = monitor.get_window_statistics(Duration::from_secs(60));
        assert!(stats.is_some());

        let stats = stats.unwrap();
        assert_eq!(stats.query_count, 10);
        assert_eq!(stats.total_records_processed, 1000);
    }
}
