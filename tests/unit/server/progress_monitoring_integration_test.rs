//! Integration tests for Progress Monitoring implementation
//!
//! Tests the basic progress monitoring system functionality without complex async dependencies.

use std::sync::Arc;
use std::time::Duration;

use velostream::velostream::server::progress_monitoring::{
    LoadingSummary, ProgressMonitor, TableLoadProgress, TableLoadStatus, TableProgressTracker,
};

/// Test basic progress monitoring functionality
#[test]
fn test_progress_monitoring_basic_workflow() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Test ProgressMonitor directly
        let monitor = ProgressMonitor::new();

        // Test basic tracker creation and functionality
        let tracker1 = monitor
            .start_tracking("test_table_1".to_string(), Some(500))
            .await;

        let tracker2 = monitor
            .start_tracking("test_table_2".to_string(), Some(1000))
            .await;

        // Use atomic operations to avoid potential async deadlocks
        use std::sync::atomic::Ordering;
        tracker1.records_loaded.store(100, Ordering::Relaxed);
        tracker1.bytes_processed.store(1000, Ordering::Relaxed);

        tracker2.records_loaded.store(250, Ordering::Relaxed);
        tracker2.bytes_processed.store(2500, Ordering::Relaxed);

        // Test sync access to avoid hanging
        let progress1 = tracker1.get_current_progress_sync();
        assert_eq!(progress1.records_loaded, 100);
        assert_eq!(progress1.bytes_processed, 1000);
        assert_eq!(progress1.table_name, "test_table_1");

        let progress2 = tracker2.get_current_progress_sync();
        assert_eq!(progress2.records_loaded, 250);
        assert_eq!(progress2.bytes_processed, 2500);
        assert_eq!(progress2.table_name, "test_table_2");

        // Test progress percentages
        if let Some(percentage) = progress1.progress_percentage {
            assert!((percentage - 20.0).abs() < 0.1); // Should be ~20%
        }

        if let Some(percentage) = progress2.progress_percentage {
            assert!((percentage - 25.0).abs() < 0.1); // Should be ~25%
        }
    });
}

/// Test progress tracking updates
#[test]
fn test_progress_tracking_updates() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let monitor = ProgressMonitor::new();

        // Create a tracker
        let tracker = monitor
            .start_tracking("update_test_table".to_string(), Some(100))
            .await;

        // Test initial state
        let initial_progress = tracker.get_current_progress_sync();
        assert_eq!(initial_progress.records_loaded, 0);
        assert_eq!(initial_progress.bytes_processed, 0);

        // Test updates using atomic operations
        use std::sync::atomic::Ordering;
        tracker.records_loaded.store(50, Ordering::Relaxed);
        tracker.bytes_processed.store(5000, Ordering::Relaxed);

        let updated_progress = tracker.get_current_progress_sync();
        assert_eq!(updated_progress.records_loaded, 50);
        assert_eq!(updated_progress.bytes_processed, 5000);

        // Test percentage calculation
        if let Some(percentage) = updated_progress.progress_percentage {
            assert!((percentage - 50.0).abs() < 0.1); // Should be ~50%
        }
    });
}

/// Test multiple trackers performance
#[test]
fn test_multiple_trackers_performance() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let monitor = ProgressMonitor::new();
        let start_time = std::time::Instant::now();

        // Create multiple trackers
        let mut trackers = Vec::new();
        for i in 0..5 {
            let tracker = monitor
                .start_tracking(format!("perf_table_{}", i), Some(100))
                .await;
            trackers.push(tracker);
        }

        // Update all trackers using atomic operations
        use std::sync::atomic::Ordering;
        for (i, tracker) in trackers.iter().enumerate() {
            let records = (i + 1) * 20;
            let bytes = records * 100;
            tracker.records_loaded.store(records, Ordering::Relaxed);
            tracker.bytes_processed.store(bytes, Ordering::Relaxed);
        }

        // Verify all trackers
        for (i, tracker) in trackers.iter().enumerate() {
            let progress = tracker.get_current_progress_sync();
            let expected_records = (i + 1) * 20;
            assert_eq!(progress.records_loaded, expected_records);
            assert_eq!(progress.bytes_processed, expected_records * 100);
        }

        // Check that operations completed quickly
        let elapsed = start_time.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "Progress monitoring should be fast, took {:?}",
            elapsed
        );
    });
}

/// Test error handling and edge cases
#[test]
fn test_progress_monitoring_edge_cases() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let monitor = ProgressMonitor::new();

        // Test tracker with unknown total
        let tracker_unknown = monitor
            .start_tracking("unknown_total".to_string(), None)
            .await;

        use std::sync::atomic::Ordering;
        tracker_unknown.records_loaded.store(100, Ordering::Relaxed);
        tracker_unknown.bytes_processed.store(1000, Ordering::Relaxed);

        let progress = tracker_unknown.get_current_progress_sync();
        assert_eq!(progress.records_loaded, 100);
        assert_eq!(progress.bytes_processed, 1000);
        assert!(progress.progress_percentage.is_none()); // No percentage without total

        // Test tracker with zero total
        let tracker_zero = monitor
            .start_tracking("zero_total".to_string(), Some(0))
            .await;

        let progress_zero = tracker_zero.get_current_progress_sync();
        assert_eq!(progress_zero.progress_percentage, Some(100.0)); // 0/0 should be 100%
    });
}