//! Comprehensive unit tests for background flusher
//!
//! Tests cover:
//! - Background task lifecycle (start/shutdown)
//! - Shutdown timeout handling
//! - Graceful termination with final flush
//! - Multiple flush triggers (batch size, time interval)

use std::time::Duration;
use velostream::velostream::observability::async_queue::ObservabilityQueue;
use velostream::velostream::observability::background_flusher::BackgroundFlusher;
use velostream::velostream::observability::queue_config::ObservabilityQueueConfig;

#[tokio::test]
async fn test_flusher_lifecycle() {
    let config = ObservabilityQueueConfig::default();
    let (_, receivers) = ObservabilityQueue::new(config.clone());

    // Start flusher without providers (no-op mode)
    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Verify flusher is running (we can't directly check, but no panic is good)

    // Shutdown should complete quickly
    let result = flusher.shutdown(Duration::from_secs(1)).await;
    assert!(result.is_ok(), "Shutdown should succeed");

    let shutdown_result = result.unwrap();
    assert!(
        shutdown_result.metrics_completed,
        "Metrics task should complete"
    );
    assert!(
        shutdown_result.traces_completed,
        "Traces task should complete"
    );
}

#[tokio::test]
async fn test_flusher_starts_tasks() {
    let config = ObservabilityQueueConfig::default();
    let (queue, receivers) = ObservabilityQueue::new(config.clone());

    // Send an event BEFORE starting flusher
    let sample = velostream::velostream::observability::remote_write::TimestampedSample {
        name: "test_metric".to_string(),
        label_names: vec![],
        label_values: vec![],
        value: 1.0,
        timestamp_ms: 1234567890,
    };
    let event =
        velostream::velostream::observability::async_queue::MetricsEvent::FlushRemoteWrite {
            samples: vec![sample],
            timestamp: std::time::Instant::now(),
        };
    assert!(queue.try_send_metrics(event).is_ok());

    // Start flusher (consumes receivers)
    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Give it time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Shutdown
    let result = flusher.shutdown(Duration::from_secs(1)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_shutdown_completes_within_timeout() {
    let config = ObservabilityQueueConfig::default();
    let (_, receivers) = ObservabilityQueue::new(config.clone());

    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Shutdown with generous timeout
    let start = std::time::Instant::now();
    let result = flusher.shutdown(Duration::from_secs(5)).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert!(
        elapsed < Duration::from_secs(1),
        "Shutdown should complete quickly without active work"
    );
}

#[tokio::test]
async fn test_multiple_flushers_independent() {
    // Start multiple flushers to ensure they don't interfere
    let config = ObservabilityQueueConfig::default();

    let (_, receivers1) = ObservabilityQueue::new(config.clone());
    let flusher1 = BackgroundFlusher::start(receivers1, None, None, config.clone()).await;

    let (_, receivers2) = ObservabilityQueue::new(config.clone());
    let flusher2 = BackgroundFlusher::start(receivers2, None, None, config.clone()).await;

    // Both should shut down independently
    let result1 = flusher1.shutdown(Duration::from_secs(1)).await;
    let result2 = flusher2.shutdown(Duration::from_secs(1)).await;

    assert!(result1.is_ok());
    assert!(result2.is_ok());
}

#[tokio::test]
async fn test_shutdown_after_events_queued() {
    let config =
        ObservabilityQueueConfig::default().with_metrics_flush_interval(Duration::from_secs(60)); // Long interval to prevent auto-flush
    let (queue, receivers) = ObservabilityQueue::new(config.clone());

    // Queue several events BEFORE starting flusher
    for _ in 0..10 {
        let sample = velostream::velostream::observability::remote_write::TimestampedSample {
            name: "test_metric".to_string(),
            label_names: vec![],
            label_values: vec![],
            value: 1.0,
            timestamp_ms: 1234567890,
        };
        let event =
            velostream::velostream::observability::async_queue::MetricsEvent::FlushRemoteWrite {
                samples: vec![sample],
                timestamp: std::time::Instant::now(),
            };
        queue.try_send_metrics(event).unwrap();
    }

    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Shutdown should complete (final flush)
    let result = flusher.shutdown(Duration::from_secs(2)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_short_flush_interval() {
    // Test with very short flush interval (100ms)
    let config = ObservabilityQueueConfig::default()
        .with_metrics_flush_interval(Duration::from_millis(100))
        .with_traces_flush_interval(Duration::from_millis(100));

    let (queue, receivers) = ObservabilityQueue::new(config.clone());

    // Send an event BEFORE starting flusher
    let sample = velostream::velostream::observability::remote_write::TimestampedSample {
        name: "test_metric".to_string(),
        label_names: vec![],
        label_values: vec![],
        value: 1.0,
        timestamp_ms: 1234567890,
    };
    let event =
        velostream::velostream::observability::async_queue::MetricsEvent::FlushRemoteWrite {
            samples: vec![sample],
            timestamp: std::time::Instant::now(),
        };
    queue.try_send_metrics(event).unwrap();

    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Wait for flush interval
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Shutdown should work after flush
    let result = flusher.shutdown(Duration::from_secs(1)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_large_batch_size() {
    // Test with large batch size (won't trigger batch flush)
    let config = ObservabilityQueueConfig::default()
        .with_metrics_batch_size(10_000)
        .with_metrics_flush_interval(Duration::from_secs(60)); // Long interval

    let (queue, receivers) = ObservabilityQueue::new(config.clone());

    // Send 100 events (below batch size) BEFORE starting flusher
    for _ in 0..100 {
        let sample = velostream::velostream::observability::remote_write::TimestampedSample {
            name: "test_metric".to_string(),
            label_names: vec![],
            label_values: vec![],
            value: 1.0,
            timestamp_ms: 1234567890,
        };
        let event =
            velostream::velostream::observability::async_queue::MetricsEvent::FlushRemoteWrite {
                samples: vec![sample],
                timestamp: std::time::Instant::now(),
            };
        queue.try_send_metrics(event).unwrap();
    }

    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Shutdown should flush remaining events
    let result = flusher.shutdown(Duration::from_secs(2)).await;
    assert!(result.is_ok());
    assert!(result.unwrap().metrics_completed);
}

#[tokio::test]
async fn test_concurrent_event_submission_during_flush() {
    let config =
        ObservabilityQueueConfig::default().with_metrics_flush_interval(Duration::from_millis(50));

    let (queue, receivers) = ObservabilityQueue::new(config.clone());
    let queue = std::sync::Arc::new(queue);
    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Spawn task that continuously sends events
    let queue_clone = queue.clone();
    let sender_task = tokio::spawn(async move {
        for _ in 0..100 {
            let sample = velostream::velostream::observability::remote_write::TimestampedSample {
                name: "test_metric".to_string(),
                label_names: vec![],
                label_values: vec![],
                value: 1.0,
                timestamp_ms: 1234567890,
            };
            let event = velostream::velostream::observability::async_queue::MetricsEvent::FlushRemoteWrite {
                samples: vec![sample],
                timestamp: std::time::Instant::now(),
            };
            let _ = queue_clone.try_send_metrics(event);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    // Let it run for a bit
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Shutdown should work even with concurrent sends
    let result = flusher.shutdown(Duration::from_secs(2)).await;
    assert!(result.is_ok());

    // Wait for sender to finish
    let _ = sender_task.await;
}

#[tokio::test]
async fn test_empty_queue_shutdown() {
    // Test shutting down without any events sent
    let config = ObservabilityQueueConfig::default();
    let (_, receivers) = ObservabilityQueue::new(config.clone());

    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Immediate shutdown with empty queue
    let result = flusher.shutdown(Duration::from_millis(500)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_rapid_shutdown() {
    // Test shutting down immediately after start
    let config = ObservabilityQueueConfig::default();
    let (_, receivers) = ObservabilityQueue::new(config.clone());

    let flusher = BackgroundFlusher::start(receivers, None, None, config).await;

    // Shutdown immediately
    let start = std::time::Instant::now();
    let result = flusher.shutdown(Duration::from_millis(100)).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert!(
        elapsed < Duration::from_millis(200),
        "Rapid shutdown should complete quickly"
    );
}
