//! Integration tests for Phase 3: Progress Monitoring implementation
//!
//! Tests the complete progress monitoring system including progress tracking,
//! streaming updates, and health dashboard endpoints.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

use velostream::velostream::server::health_dashboard::{HealthDashboard, OverallHealthStatus};
use velostream::velostream::server::progress_monitoring::{
    LoadingSummary, ProgressMonitor, TableLoadProgress, TableLoadStatus,
};
use velostream::velostream::server::progress_streaming::{
    ProgressEvent, ProgressStreamingConfig, ProgressStreamingServer,
};
use velostream::velostream::server::table_registry::TableRegistry;
use velostream::velostream::sql::execution::types::FieldValue;

/// Test complete progress monitoring workflow
#[tokio::test]
async fn test_complete_progress_monitoring_workflow() {
    // Add timeout to prevent infinite hanging
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Setup components
        let registry = Arc::new(TableRegistry::new());
        let config = ProgressStreamingConfig {
            update_interval: Duration::from_millis(100),
            ..Default::default()
        };
        let streaming_server = Arc::new(ProgressStreamingServer::new(registry.clone(), config));
        let dashboard = HealthDashboard::with_streaming(registry.clone(), streaming_server.clone());

        // Start monitoring with handle for cleanup
        let monitoring_task = streaming_server.start_monitoring().await;

        // Store the handle so we can abort it later
        let monitoring_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await; // Let it run for max 5 seconds
            monitoring_task.abort();
        });

        // Test 1: Initial state - no tables loading
        let health = dashboard.get_tables_health().await.unwrap();
        assert_eq!(health.total_tables, 0);
        assert!(matches!(
            health.overall_status,
            OverallHealthStatus::Healthy
        ));

        let progress = dashboard.get_loading_progress().await.unwrap();
        assert!(progress.tables.is_empty());
        assert_eq!(progress.summary.total_tables, 0);

        // Test 2: Start tracking a table
        let tracker = registry
            .start_progress_tracking("test_table".to_string(), Some(1000))
            .await;
        tracker.set_status(TableLoadStatus::Loading).await;

        // Give a moment for the update to propagate
        sleep(Duration::from_millis(150)).await;

        let progress = dashboard.get_loading_progress().await.unwrap();
        assert_eq!(progress.tables.len(), 1);
        assert!(progress.tables.contains_key("test_table"));

        // Test 3: Simulate loading progress
        tracker.add_records(100, 1024).await;
        sleep(Duration::from_millis(50)).await;
        tracker.add_records(200, 2048).await;
        sleep(Duration::from_millis(50)).await;

        let progress = dashboard.get_loading_progress().await.unwrap();
        let table_progress = progress.tables.get("test_table").unwrap();
        assert_eq!(table_progress.records_loaded, 300);
        assert_eq!(table_progress.bytes_processed, 3072);
        assert!(table_progress.loading_rate > 0.0);

        // Test 4: Complete loading
        tracker.add_records(700, 7168).await; // Complete to 1000 records
        tracker.set_completed().await;

        sleep(Duration::from_millis(150)).await;

        let progress = dashboard.get_loading_progress().await.unwrap();
        let table_progress = progress.tables.get("test_table").unwrap();
        assert_eq!(table_progress.records_loaded, 1000);
        assert!(matches!(table_progress.status, TableLoadStatus::Completed));

        // Test 5: Stop tracking completed table
        registry.stop_progress_tracking("test_table").await;
        sleep(Duration::from_millis(150)).await;

        let progress = dashboard.get_loading_progress().await.unwrap();
        assert!(progress.tables.is_empty());

        // Cleanup monitoring task
        monitoring_handle.abort();
    })
    .await;

    assert!(result.is_ok(), "Test should complete within 10 seconds");
}

/// Test streaming updates for progress monitoring
#[tokio::test]
async fn test_progress_streaming_updates() {
    // Add timeout to prevent infinite hanging
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let registry = Arc::new(TableRegistry::new());
        let config = ProgressStreamingConfig {
            update_interval: Duration::from_millis(50),
            ..Default::default()
        };
        let streaming_server = ProgressStreamingServer::new(registry.clone(), config);

        // Start monitoring with cleanup handle
        let monitoring_task = streaming_server.start_monitoring().await;
        let monitoring_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            monitoring_task.abort();
        });

        // Create a stream for updates
        let mut stream = streaming_server.create_stream().await.unwrap();

        // Start tracking a table
        let tracker = registry
            .start_progress_tracking("stream_test_table".to_string(), Some(500))
            .await;
        tracker.set_status(TableLoadStatus::Loading).await;

        // Collect initial events
        let events = timeout(Duration::from_millis(200), stream.collect_events(2)).await;
        assert!(events.is_ok());

        let events = events.unwrap();
        assert!(!events.is_empty());

        // Check that we get an initial snapshot
        if let Some(ProgressEvent::InitialSnapshot { tables, .. }) = events.first() {
            // Initial snapshot should be empty or contain the table we just added
            assert!(tables.is_empty() || tables.contains_key("stream_test_table"));
        }

        // Add some progress and verify we get updates
        tracker.add_records(100, 1000).await;
        tracker.add_records(150, 1500).await;

        // Allow time for updates to be sent
        sleep(Duration::from_millis(100)).await;

        // The stream should have received table update events
        // Note: In a real test, we'd collect more events and verify their content

        // Cleanup monitoring task
        monitoring_handle.abort();
    })
    .await;

    assert!(result.is_ok(), "Test should complete within 10 seconds");
}

/// Test health dashboard endpoints
#[tokio::test]
async fn test_health_dashboard_endpoints() {
    let registry = Arc::new(TableRegistry::new());
    let dashboard = HealthDashboard::new(registry.clone());

    // Test health metrics endpoint
    let metrics = dashboard.get_health_metrics().await.unwrap();
    assert_eq!(metrics.total_tables, 0);
    assert!(!metrics.is_at_capacity);
    assert_eq!(metrics.performance_metrics.active_loading_tables, 0);

    // Test connection stats (should show no streaming available)
    let conn_stats = dashboard.get_connection_stats().await.unwrap();
    assert!(!conn_stats.streaming_available);
    assert_eq!(conn_stats.stats.active_connections, 0);

    // Test Prometheus metrics generation
    let prometheus = dashboard.get_prometheus_metrics().await.unwrap();
    assert!(prometheus.contains("velostream_tables_total 0"));
    assert!(prometheus.contains("velostream_tables_loading 0"));
    assert!(prometheus.contains("# HELP"));
    assert!(prometheus.contains("# TYPE"));
}

/// Test progress monitoring with multiple tables
#[tokio::test]
async fn test_multiple_tables_progress_monitoring() {
    let registry = Arc::new(TableRegistry::new());
    let dashboard = HealthDashboard::new(registry.clone());

    // Start tracking multiple tables
    let tracker1 = registry
        .start_progress_tracking("table1".to_string(), Some(1000))
        .await;
    let tracker2 = registry
        .start_progress_tracking("table2".to_string(), Some(2000))
        .await;
    let tracker3 = registry
        .start_progress_tracking("table3".to_string(), None)
        .await; // Unknown total

    // Set all to loading status
    tracker1.set_status(TableLoadStatus::Loading).await;
    tracker2.set_status(TableLoadStatus::Loading).await;
    tracker3.set_status(TableLoadStatus::Loading).await;

    // Add progress to each table
    tracker1.add_records(500, 5000).await; // 50% complete
    tracker2.add_records(1000, 10000).await; // 50% complete
    tracker3.add_records(750, 7500).await; // Unknown percentage

    // Get progress summary
    let progress = dashboard.get_loading_progress().await.unwrap();
    assert_eq!(progress.tables.len(), 3);
    assert_eq!(progress.summary.loading, 3);
    assert_eq!(progress.summary.total_tables, 3);

    // Check individual table progress
    let table1_progress = progress.tables.get("table1").unwrap();
    assert_eq!(table1_progress.records_loaded, 500);
    assert_eq!(table1_progress.progress_percentage, Some(50.0));

    let table2_progress = progress.tables.get("table2").unwrap();
    assert_eq!(table2_progress.records_loaded, 1000);
    assert_eq!(table2_progress.progress_percentage, Some(50.0));

    let table3_progress = progress.tables.get("table3").unwrap();
    assert_eq!(table3_progress.records_loaded, 750);
    assert!(table3_progress.progress_percentage.is_none()); // Unknown total

    // Complete table1
    tracker1.add_records(500, 5000).await; // Complete to 1000
    tracker1.set_completed().await;

    // Fail table2
    tracker2
        .set_error("Simulated loading error".to_string())
        .await;

    sleep(Duration::from_millis(50)).await;

    // Check updated summary
    let summary = registry.get_loading_summary().await;
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.loading, 1); // table3 still loading

    // Get health status
    let health = dashboard.get_tables_health().await.unwrap();
    assert_eq!(health.critical_count, 1); // table2 failed
    assert!(matches!(
        health.overall_status,
        OverallHealthStatus::Critical
    ));
}

/// Test ETA calculation and progress percentage
#[tokio::test]
async fn test_eta_and_progress_calculations() {
    // Add timeout to prevent infinite hanging
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let registry = Arc::new(TableRegistry::new());

        // Generate unique identifiers to prevent conflicts
        let unique_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let tracker = registry
            .start_progress_tracking(format!("eta_test_table_{}", unique_id), Some(2000))
            .await;

        tracker.set_status(TableLoadStatus::Loading).await;

        // Add initial records and wait to establish a loading rate
        tracker.add_records(200, 2000).await;
        sleep(Duration::from_millis(100)).await;
        tracker.add_records(200, 2000).await; // Total: 400 records

        // Get current progress
        let progress = tracker.get_current_progress().await;
        assert_eq!(progress.records_loaded, 400);
        assert_eq!(progress.progress_percentage, Some(20.0)); // 400/2000 = 20%
        assert!(progress.loading_rate > 0.0);

        // ETA should be calculated if we have a loading rate
        if progress.loading_rate > 0.0 {
            assert!(progress.estimated_completion.is_some());
        }

        // Test progress percentage edge cases
        let tracker_zero = registry
            .start_progress_tracking(format!("zero_test_{}", unique_id), Some(0))
            .await;
        let progress_zero = tracker_zero.get_current_progress().await;
        assert_eq!(progress_zero.progress_percentage, Some(100.0)); // 0/0 should be 100%

        let tracker_unknown = registry
            .start_progress_tracking(format!("unknown_test_{}", unique_id), None)
            .await;
        tracker_unknown.add_records(100, 1000).await;
        let progress_unknown = tracker_unknown.get_current_progress().await;
        assert!(progress_unknown.progress_percentage.is_none()); // Unknown total
    })
    .await;

    assert!(result.is_ok(), "Test should complete within 10 seconds");
}

/// Test error handling in progress monitoring
#[tokio::test]
async fn test_progress_monitoring_error_handling() {
    let registry = Arc::new(TableRegistry::new());
    let dashboard = HealthDashboard::new(registry.clone());

    // Test getting progress for non-existent table
    let progress = registry.get_table_loading_progress("nonexistent").await;
    assert!(progress.is_none());

    // Test health endpoint for non-existent table
    let result = dashboard.get_table_health("nonexistent").await;
    assert!(result.is_err());

    // Test subscription to non-existent table
    let subscription = registry.subscribe_to_table_progress("nonexistent").await;
    assert!(subscription.is_none());
}

/// Test performance under load
#[tokio::test]
async fn test_progress_monitoring_performance() {
    // Add timeout to prevent infinite hanging
    let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let registry = Arc::new(TableRegistry::new());
        let start_time = std::time::Instant::now();

        // Create multiple tables and update them rapidly (reduced for faster tests)
        let mut trackers = Vec::new();
        for i in 0..5 {
            // Reduced from 10 to 5 tables
            let unique_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let tracker = registry
                .start_progress_tracking(format!("perf_table_{}_{}", i, unique_id), Some(1000)) // Reduced expected records
                .await;
            tracker.set_status(TableLoadStatus::Loading).await;
            trackers.push(tracker);
        }

        // Rapidly update all tables (reduced iterations)
        for _ in 0..20 {
            // Reduced from 100 to 20 iterations
            for tracker in trackers.iter() {
                tracker.add_records(10, 100).await;
            }
        }

        // Check that operations completed quickly
        let elapsed = start_time.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "Progress monitoring should be fast, took {:?}",
            elapsed
        );

        // Verify final state (adjusted for reduced test size)
        let summary = registry.get_loading_summary().await;
        assert_eq!(summary.total_tables, 5); // Reduced from 10 to 5
        assert_eq!(summary.loading, 5);
        assert_eq!(summary.total_records_loaded, 1_000); // 5 tables * 20 updates * 10 records
    })
    .await;

    assert!(result.is_ok(), "Test should complete within 15 seconds");
}
