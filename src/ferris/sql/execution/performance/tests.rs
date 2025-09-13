/*!
# Performance Module Integration Tests

Tests the integration of performance monitoring with the SQL processor system.
*/

#[cfg(test)]
mod tests {
    use crate::ferris::sql::ast::StreamingQuery;
    use crate::ferris::sql::execution::performance::{PerformanceMonitor, QueryTracker};
    use crate::ferris::sql::execution::processors::{ProcessorContext, QueryProcessor};
    use crate::ferris::sql::execution::{FieldValue, StreamRecord};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn create_test_record() -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        fields.insert("name".to_string(), FieldValue::String("test".to_string()));

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1234567890000,
            offset: 0,
            partition: 0,
        }
    }

    #[test]
    fn test_basic_performance_tracking() {
        let monitor = PerformanceMonitor::new();
        
        let mut tracker = monitor.start_query_tracking("SELECT * FROM test");
        tracker.start_processor("TestProcessor");
        tracker.add_records_processed(100);
        tracker.add_bytes_processed(1024);
        
        // Simulate some work
        thread::sleep(Duration::from_millis(1));
        
        let performance = monitor.finish_query_tracking(tracker);
        
        assert_eq!(performance.records_processed, 100);
        assert_eq!(performance.bytes_processed, 1024);
        assert!(performance.execution_time.as_millis() >= 1);
        assert!(performance.processor_times.contains_key("TestProcessor"));
        
        let metrics = monitor.get_current_metrics();
        assert_eq!(metrics.total_queries, 1);
    }

    #[test]
    fn test_processor_context_with_monitoring() {
        let monitor = Arc::new(PerformanceMonitor::new());
        let mut context = ProcessorContext::new("test_query");
        context.set_performance_monitor(Arc::clone(&monitor));
        
        assert!(context.get_performance_monitor().is_some());
        
        // Verify monitor can be used from context
        let monitor_ref = context.get_performance_monitor().unwrap();
        let mut tracker = monitor_ref.start_query_tracking("SELECT 1");
        tracker.add_records_processed(1);
        let _performance = monitor_ref.finish_query_tracking(tracker);
        
        assert_eq!(monitor.get_current_metrics().total_queries, 1);
    }

    #[test]
    fn test_performance_statistics_collection() {
        let monitor = PerformanceMonitor::new();
        
        // Execute multiple queries
        for i in 1..=5 {
            let mut tracker = monitor.start_query_tracking(&format!("SELECT {}", i));
            tracker.add_records_processed(i * 100);
            tracker.add_bytes_processed(i * 1000);
            
            // Simulate different execution times
            thread::sleep(Duration::from_millis(i));
            
            monitor.finish_query_tracking(tracker);
        }

        let metrics = monitor.get_current_metrics();
        assert_eq!(metrics.total_queries, 5);
        
        // Check window statistics
        let window_stats = monitor.get_window_statistics(Duration::from_secs(60));
        assert!(window_stats.is_some());
        
        let stats = window_stats.unwrap();
        assert_eq!(stats.query_count, 5);
        assert_eq!(stats.total_records_processed, 1500); // 100+200+300+400+500
        
        // Check query patterns
        let patterns = monitor.get_top_query_patterns(10);
        assert_eq!(patterns.len(), 5); // Each query has different pattern
    }

    #[test]
    fn test_health_check() {
        let monitor = PerformanceMonitor::new();
        
        // New monitor should be healthy
        let health = monitor.health_check();
        assert_eq!(health.status, super::super::monitor::HealthStatus::Healthy);
        assert!(health.issues.is_empty());
        
        // Add some normal queries
        for _ in 1..=10 {
            let mut tracker = monitor.start_query_tracking("SELECT COUNT(*) FROM test");
            tracker.add_records_processed(1000);
            thread::sleep(Duration::from_millis(1));
            monitor.finish_query_tracking(tracker);
        }
        
        let health_after = monitor.health_check();
        // Should still be healthy with reasonable performance
        assert_eq!(health_after.status, super::super::monitor::HealthStatus::Healthy);
    }

    #[test]
    fn test_prometheus_metrics_export() {
        let monitor = PerformanceMonitor::new();
        
        // Execute a few queries
        for _ in 1..=3 {
            let mut tracker = monitor.start_query_tracking("SELECT * FROM users");
            tracker.add_records_processed(500);
            tracker.add_bytes_processed(5000);
            monitor.finish_query_tracking(tracker);
        }

        let prometheus_output = monitor.export_prometheus_metrics();
        
        // Verify Prometheus format
        assert!(prometheus_output.contains("# HELP"));
        assert!(prometheus_output.contains("# TYPE"));
        assert!(prometheus_output.contains("ferrisstreams_queries_total 3"));
        assert!(prometheus_output.contains("ferrisstreams_records_processed_total"));
        assert!(prometheus_output.contains("ferrisstreams_bytes_processed_total"));
    }

    #[test]
    fn test_performance_report_generation() {
        let monitor = PerformanceMonitor::new();
        
        // Execute some queries with different patterns
        let queries = [
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM orders WHERE amount > 100", 
            "SELECT COUNT(*) FROM products",
        ];

        for (i, query) in queries.iter().enumerate() {
            let mut tracker = monitor.start_query_tracking(query);
            tracker.add_records_processed((i + 1) as u64 * 200);
            thread::sleep(Duration::from_millis(i as u64 + 1));
            monitor.finish_query_tracking(tracker);
        }

        let report = monitor.get_performance_report();
        
        // Verify report contains key sections
        assert!(report.contains("Performance Report"));
        assert!(report.contains("Status: Healthy"));
        assert!(report.contains("Total Queries: 3"));
        assert!(report.contains("Overall Metrics"));
        assert!(report.contains("Recent Performance"));
        assert!(report.contains("Top Query Patterns"));
    }

    #[test]
    fn test_memory_metrics_tracking() {
        let monitor = PerformanceMonitor::new();
        
        let mut tracker = monitor.start_query_tracking("SELECT * FROM large_table");
        
        // Simulate memory usage
        let mut memory_metrics = super::super::metrics::MemoryMetrics::default();
        memory_metrics.allocated_bytes = 1024 * 1024; // 1MB
        memory_metrics.peak_memory_bytes = 2 * 1024 * 1024; // 2MB
        memory_metrics.group_by_memory_bytes = 512 * 1024; // 512KB
        
        tracker.update_memory_metrics(memory_metrics);
        tracker.add_records_processed(10000);
        
        let performance = monitor.finish_query_tracking(tracker);
        
        assert_eq!(performance.memory_metrics.allocated_bytes, 1024 * 1024);
        assert_eq!(performance.memory_metrics.peak_memory_bytes, 2 * 1024 * 1024);
        assert_eq!(performance.memory_metrics.group_by_memory_bytes, 512 * 1024);
        
        // Verify memory efficiency calculation
        let efficiency = performance.memory_metrics.memory_efficiency(performance.bytes_processed);
        assert!(efficiency >= 0.0);
    }

    #[test]
    fn test_processor_time_breakdown() {
        let monitor = PerformanceMonitor::new();
        
        let mut tracker = monitor.start_query_tracking("SELECT * FROM test JOIN other");
        
        // Simulate multiple processors
        tracker.start_processor("SelectProcessor");
        thread::sleep(Duration::from_millis(5));
        
        tracker.start_processor("JoinProcessor");
        thread::sleep(Duration::from_millis(10));
        
        tracker.start_processor("FilterProcessor");
        thread::sleep(Duration::from_millis(3));
        
        let performance = monitor.finish_query_tracking(tracker);
        
        // Verify all processors were tracked
        assert_eq!(performance.processor_times.len(), 3);
        assert!(performance.processor_times.contains_key("SelectProcessor"));
        assert!(performance.processor_times.contains_key("JoinProcessor"));
        assert!(performance.processor_times.contains_key("FilterProcessor"));
        
        // Verify time breakdown percentages
        let breakdown = performance.processor_time_breakdown();
        let total_percentage: f64 = breakdown.values().sum();
        assert!((total_percentage - 100.0).abs() < 1.0); // Should sum to ~100%
        
        // JoinProcessor should have the highest percentage (longest sleep)
        assert!(breakdown["JoinProcessor"] > breakdown["SelectProcessor"]);
        assert!(breakdown["JoinProcessor"] > breakdown["FilterProcessor"]);
    }

    #[test]
    fn test_throughput_calculations() {
        let monitor = PerformanceMonitor::new();
        
        let mut tracker = monitor.start_query_tracking("SELECT * FROM high_volume_stream");
        tracker.add_records_processed(50000);
        tracker.add_bytes_processed(5000000); // 5MB
        
        thread::sleep(Duration::from_millis(100)); // 0.1 second
        
        let performance = monitor.finish_query_tracking(tracker);
        
        let rps = performance.records_per_second();
        let bps = performance.bytes_per_second();
        
        // Should be around 500k records/sec and 50MB/sec
        assert!(rps > 400000.0 && rps < 600000.0);
        assert!(bps > 40000000.0 && bps < 60000000.0);
    }

    #[test]
    fn test_query_pattern_normalization() {
        let monitor = PerformanceMonitor::new();
        
        // Execute queries with different literals but same pattern
        let queries = [
            "SELECT * FROM users WHERE id = 123",
            "SELECT * FROM users WHERE id = 456",
            "SELECT * FROM users WHERE id = 789",
        ];

        for query in &queries {
            let mut tracker = monitor.start_query_tracking(query);
            tracker.add_records_processed(100);
            monitor.finish_query_tracking(tracker);
        }

        let patterns = monitor.get_top_query_patterns(5);
        
        // Should normalize to single pattern despite different IDs
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].1.execution_count, 3);
    }
}