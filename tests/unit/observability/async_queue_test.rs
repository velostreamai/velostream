//! Comprehensive unit tests for async observability queue
//!
//! Tests cover:
//! - Normal operation (send/receive)
//! - Queue overflow behavior (drop-on-full)
//! - Concurrent access from multiple processors
//! - Statistics tracking
//! - Separate channel independence

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use velostream::velostream::observability::async_queue::{
    MetricsEvent, ObservabilityQueue, TraceEvent,
};
use velostream::velostream::observability::queue_config::ObservabilityQueueConfig;
use velostream::velostream::observability::remote_write::TimestampedSample;

// Import shared test helper
use crate::unit::observability_test_helpers::create_test_span;

/// Helper to create a test metrics event
fn create_test_metrics_event() -> MetricsEvent {
    let sample = TimestampedSample {
        name: "test_metric".to_string(),
        label_names: vec!["label1".to_string()],
        label_values: vec!["value1".to_string()],
        value: 42.0,
        timestamp_ms: 1234567890,
    };
    MetricsEvent::FlushRemoteWrite {
        samples: vec![sample],
        timestamp: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn test_queue_normal_operation() {
    let config = ObservabilityQueueConfig::default();
    let (queue, mut receivers) = ObservabilityQueue::new(config);

    // Send metrics event
    let event = create_test_metrics_event();

    assert!(queue.try_send_metrics(event).is_ok());
    assert_eq!(queue.metrics_queued_count(), 1);
    assert_eq!(queue.metrics_dropped_count(), 0);

    // Receive should work
    let received = receivers.metrics_rx.recv().await;
    assert!(received.is_some());
}

#[tokio::test]
async fn test_queue_overflow_drops_events() {
    // Create tiny queue (capacity 2)
    let config = ObservabilityQueueConfig::default().with_metrics_queue_size(2);
    let (queue, _receivers) = ObservabilityQueue::new(config);

    // Fill queue to capacity
    for _ in 0..2 {
        let event = create_test_metrics_event();
        assert!(queue.try_send_metrics(event).is_ok());
    }

    assert_eq!(queue.metrics_queued_count(), 2);
    assert_eq!(queue.metrics_dropped_count(), 0);
    assert_eq!(
        queue.metrics_queue_depth(),
        2,
        "Queue should be at capacity"
    );

    // Third send should fail (queue full)
    let event = create_test_metrics_event();
    assert!(queue.try_send_metrics(event).is_err());
    assert_eq!(queue.metrics_dropped_count(), 1);
    assert_eq!(
        queue.metrics_queue_depth(),
        2,
        "Queue depth should remain at capacity after drop"
    );
}

#[tokio::test]
async fn test_concurrent_send_from_multiple_processors() {
    let config = ObservabilityQueueConfig::default().with_metrics_queue_size(1000);
    let (queue, mut receivers) = ObservabilityQueue::new(config);
    let queue = Arc::new(queue);

    // Spawn 10 concurrent tasks, each sending 100 events
    let mut handles = vec![];
    for i in 0..10 {
        let queue_clone = queue.clone();
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                let sample = TimestampedSample {
                    name: "test_metric".to_string(),
                    label_names: vec![],
                    label_values: vec![],
                    value: 1.0,
                    timestamp_ms: 1234567890,
                };
                let event = MetricsEvent::FlushRemoteWrite {
                    samples: vec![sample],
                    timestamp: std::time::Instant::now(),
                };
                if queue_clone.try_send_metrics(event).is_err() {
                    eprintln!("Task {} failed to send event {}", i, j);
                }
                // Small delay to simulate processing
                tokio::task::yield_now().await;
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Should have queued 1000 events (up to capacity)
    // Some may be dropped if queue filled up
    let queued = queue.metrics_queued_count();
    let dropped = queue.metrics_dropped_count();
    assert_eq!(queued + dropped, 1000, "Total events should be 1000");

    // Drain the queue to verify events are there
    let mut received_count = 0;
    while receivers.metrics_rx.try_recv().is_ok() {
        received_count += 1;
    }
    assert!(received_count > 0, "Should have received some events");
}

#[tokio::test]
async fn test_metrics_traces_channels_independent() {
    // Fill metrics queue to capacity
    let config = ObservabilityQueueConfig::default()
        .with_metrics_queue_size(2)
        .with_traces_queue_size(100);
    let (queue, _receivers) = ObservabilityQueue::new(config);

    // Fill metrics queue
    for _ in 0..2 {
        let event = create_test_metrics_event();
        assert!(queue.try_send_metrics(event).is_ok());
    }

    // Metrics queue is full
    let event = create_test_metrics_event();
    assert!(queue.try_send_metrics(event).is_err());
    assert_eq!(queue.metrics_dropped_count(), 1);

    // But traces queue should still work
    let span_data = create_test_span();
    let trace_event = TraceEvent::Span {
        span_data,
        timestamp: std::time::Instant::now(),
    };
    assert!(
        queue.try_send_trace(trace_event).is_ok(),
        "Traces queue should still accept events"
    );
    assert_eq!(queue.traces_queued_count(), 1);
    assert_eq!(queue.traces_dropped_count(), 0);
}

#[tokio::test]
async fn test_record_batch_drops() {
    let config = ObservabilityQueueConfig::default();
    let (queue, _receivers) = ObservabilityQueue::new(config);

    // Record batch drops
    queue.record_metrics_dropped(100);
    assert_eq!(queue.metrics_dropped_count(), 100);

    queue.record_traces_dropped(50);
    assert_eq!(queue.traces_dropped_count(), 50);
}

#[tokio::test]
async fn test_statistics_snapshot() {
    let config = ObservabilityQueueConfig::default();
    let (queue, _receivers) = ObservabilityQueue::new(config);

    // Queue some events
    let event = create_test_metrics_event();
    queue.try_send_metrics(event).unwrap();

    queue.record_metrics_dropped(10);
    queue.record_traces_dropped(5);

    let stats = queue.statistics();
    assert_eq!(stats.metrics_queued, 1);
    assert_eq!(stats.metrics_dropped, 10);
    assert_eq!(stats.traces_dropped, 5);
}

#[tokio::test]
async fn test_sender_cloning() {
    let config = ObservabilityQueueConfig::default();
    let (queue, mut receivers) = ObservabilityQueue::new(config);

    // Clone senders for use in multiple processors
    let metrics_sender = queue.metrics_sender();
    let _traces_sender = queue.traces_sender();

    // Send via cloned sender
    let event = create_test_metrics_event();
    metrics_sender.try_send(event).unwrap();

    // Should be receivable
    let received = receivers.metrics_rx.recv().await;
    assert!(received.is_some());
}

#[tokio::test]
async fn test_high_frequency_metrics() {
    // Simulate high-frequency metric emission (10K records/sec)
    let config = ObservabilityQueueConfig::default().with_metrics_queue_size(10_000);
    let (queue, mut receivers) = ObservabilityQueue::new(config);

    // Send 1000 events rapidly
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        let event = create_test_metrics_event();
        let _ = queue.try_send_metrics(event);
    }
    let duration = start.elapsed();

    // Should complete very quickly (non-blocking)
    assert!(
        duration < Duration::from_millis(100),
        "Sending should be fast: {:?}",
        duration
    );

    // Verify some events were queued
    assert!(queue.metrics_queued_count() > 0);

    // Drain queue to verify
    let mut received_count = 0;
    loop {
        tokio::select! {
            Some(_) = receivers.metrics_rx.recv() => {
                received_count += 1;
            }
            _ = sleep(Duration::from_millis(10)) => {
                break;
            }
        }
    }
    assert!(received_count > 0, "Should have received some events");
}

#[tokio::test]
async fn test_trace_events() {
    let config = ObservabilityQueueConfig::default();
    let (queue, mut receivers) = ObservabilityQueue::new(config);

    // Send trace event
    let span_data = create_test_span();
    let event = TraceEvent::Span {
        span_data,
        timestamp: std::time::Instant::now(),
    };

    assert!(queue.try_send_trace(event).is_ok());
    assert_eq!(queue.traces_queued_count(), 1);
    assert_eq!(queue.traces_dropped_count(), 0);

    // Receive should work
    let received = receivers.traces_rx.recv().await;
    assert!(received.is_some());
}

#[tokio::test]
async fn test_traces_overflow_drops() {
    // Tiny traces queue
    let config = ObservabilityQueueConfig::default().with_traces_queue_size(2);
    let (queue, _receivers) = ObservabilityQueue::new(config);

    // Fill queue
    for _ in 0..2 {
        let span_data = create_test_span();
        let event = TraceEvent::Span {
            span_data,
            timestamp: std::time::Instant::now(),
        };
        assert!(queue.try_send_trace(event).is_ok());
    }

    // Third should fail
    let span_data = create_test_span();
    let event = TraceEvent::Span {
        span_data,
        timestamp: std::time::Instant::now(),
    };
    assert!(queue.try_send_trace(event).is_err());
    assert_eq!(queue.traces_dropped_count(), 1);
}

#[tokio::test]
async fn test_queue_depth_tracking() {
    // Create queue with small capacity
    let config = ObservabilityQueueConfig::default().with_metrics_queue_size(10);
    let (queue, mut receivers) = ObservabilityQueue::new(config);

    // Initially empty
    assert_eq!(queue.metrics_queue_depth(), 0);

    // Send 5 events
    for _ in 0..5 {
        let event = create_test_metrics_event();
        queue.try_send_metrics(event).unwrap();
    }

    // Should show depth of 5
    assert_eq!(queue.metrics_queue_depth(), 5);

    // Receive 2 events
    receivers.metrics_rx.recv().await;
    receivers.metrics_rx.recv().await;

    // Should show depth of 3
    assert_eq!(queue.metrics_queue_depth(), 3);

    // Drain remaining
    while receivers.metrics_rx.try_recv().is_ok() {}

    // Should be empty again
    assert_eq!(queue.metrics_queue_depth(), 0);
}

#[tokio::test]
async fn test_queue_depth_gauges_registration() {
    let config = ObservabilityQueueConfig::default();
    let (queue, _receivers) = ObservabilityQueue::new(config);

    // Register metrics
    let registry = prometheus::Registry::new();
    queue.register_queue_metrics(&registry).unwrap();

    // Send some events
    for _ in 0..5 {
        let event = create_test_metrics_event();
        let _ = queue.try_send_metrics(event);
    }

    // Verify queue depth is correctly tracked
    assert_eq!(queue.metrics_queue_depth(), 5);

    // Update gauges
    queue.update_queue_depth_gauges();

    // Verify metrics are exported
    let metric_families = registry.gather();
    let depth_metric = metric_families
        .iter()
        .find(|m| m.name() == "velostream_observability_metrics_queue_depth");

    assert!(
        depth_metric.is_some(),
        "Queue depth gauge should be registered"
    );

    // Verify gauge has metrics (actual value verification requires private API access)
    let metric = depth_metric.unwrap();
    assert!(!metric.get_metric().is_empty(), "Gauge should have values");
}

#[tokio::test]
async fn test_traces_queue_depth() {
    let config = ObservabilityQueueConfig::default().with_traces_queue_size(10);
    let (queue, mut receivers) = ObservabilityQueue::new(config);

    // Initially empty
    assert_eq!(queue.traces_queue_depth(), 0);

    // Send 3 trace events
    for _ in 0..3 {
        let span_data = create_test_span();
        let event = TraceEvent::Span {
            span_data,
            timestamp: std::time::Instant::now(),
        };
        queue.try_send_trace(event).unwrap();
    }

    // Should show depth of 3
    assert_eq!(queue.traces_queue_depth(), 3);

    // Receive one
    receivers.traces_rx.recv().await;

    // Should show depth of 2
    assert_eq!(queue.traces_queue_depth(), 2);
}
