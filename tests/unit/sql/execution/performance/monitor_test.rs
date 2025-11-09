// Performance monitor tests following best practices
// Tests are organized in dedicated test files outside of implementation modules

use std::thread;
use std::time::Duration;
use velostream::velostream::sql::execution::performance::monitor::HealthStatus;
use velostream::velostream::sql::execution::performance::monitor::MonitoredExecution;
use velostream::velostream::sql::execution::performance::monitor::PerformanceMonitor;

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
    // Uptime should be a valid duration (non-zero means monitor is working)
    assert!(
        summary.uptime.as_nanos() > 0,
        "Uptime should be a positive duration"
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
        health.status,
        HealthStatus::Healthy,
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
    let result: i32 = 42_i32.with_monitoring(&monitor, "SELECT 42", |tracker| {
        tracker.add_records_processed(1);
        tracker.start_processor("TestProcessor");
        thread::sleep(Duration::from_millis(1));
        42
    });

    // Assert: Result is correct and metrics are recorded
    assert_eq!(
        result, 42,
        "Should return correct value from monitored execution"
    );
    assert_eq!(
        monitor.get_current_metrics().total_queries,
        1,
        "Should track monitored query"
    );
}

#[test]
fn test_query_pattern_tracking() {
    // Arrange
    let monitor = PerformanceMonitor::new();

    // Act: Execute similar queries
    for i in 1..=5 {
        let mut tracker =
            monitor.start_query_tracking(&format!("SELECT * FROM users WHERE id = {}", i));
        tracker.add_records_processed(100);
        monitor.finish_query_tracking(tracker);
    }
    let patterns = monitor.get_top_query_patterns(3);

    // Assert
    assert_eq!(patterns.len(), 1, "Should be grouped into one pattern");
    assert_eq!(patterns[0].1.execution_count, 5, "Should have 5 executions");
}

#[test]
fn test_window_statistics_collection() {
    // Arrange
    let monitor = PerformanceMonitor::new();

    // Act: Add some queries
    for _ in 1..=10 {
        let mut tracker = monitor.start_query_tracking("SELECT COUNT(*) FROM test");
        tracker.add_records_processed(100);
        monitor.finish_query_tracking(tracker);
    }
    let stats = monitor.get_window_statistics(Duration::from_secs(60));

    // Assert
    assert!(stats.is_some(), "Should have window statistics");

    let stats = stats.unwrap();
    assert_eq!(stats.query_count, 10, "Should have 10 queries");
    assert_eq!(
        stats.total_records_processed, 1000,
        "Should have processed 1000 total records"
    );
}
