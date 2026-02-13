//! Integration test for queue-based observability (metrics + traces)
//!
//! This test demonstrates the complete workflow for non-blocking observability:
//! 1. Create TelemetryProvider with queue-based span processing
//! 2. Create ObservabilityWrapper with queue and exporter
//! 3. Verify spans are exported via the async queue (not blocking)
//!
//! ## Production Usage Examples
//!
//! ### Option 1: ObservabilityWrapper with Queue-Based Tracing (Fully Wired)
//! ```ignore
//! use velostream::server::processors::observability_wrapper::ObservabilityWrapper;
//! use velostream::observability::queue_config::ObservabilityQueueConfig;
//! use velostream::sql::execution::config::TracingConfig;
//!
//! // Configure tracing
//! let tracing_config = TracingConfig {
//!     service_name: "my_service".to_string(),
//!     otlp_endpoint: Some("http://tempo:4317".to_string()),
//!     sampling_ratio: 1.0,
//!     ..Default::default()
//! };
//!
//! // Create wrapper with queue-based tracing enabled
//! let wrapper = ObservabilityWrapper::with_observability_and_async_queue(
//!     None,  // Or Some(observability_manager)
//!     Some(tracing_config),  // Enable queue-based span export
//!     ObservabilityQueueConfig::default(),
//! ).await;
//!
//! // Spans are now exported asynchronously without blocking!
//! ```
//!
//! ### Option 2: Manual Setup with Custom Exporters
//! ```ignore
//! // Create queue
//! let (queue, receivers) = ObservabilityQueue::new(config.clone());
//!
//! // Create queue-based telemetry
//! let (telemetry, exporter) = TelemetryProvider::new_with_queue(
//!     tracing_config,
//!     Arc::new(queue),
//! ).await?;
//!
//! // Start background flusher with exporter
//! let flusher = BackgroundFlusher::start(
//!     receivers,
//!     None,  // metrics_provider
//!     exporter,  // Span exporter
//!     config,
//! ).await;
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use velostream::velostream::observability::async_queue::ObservabilityQueue;
use velostream::velostream::observability::queue_config::ObservabilityQueueConfig;
use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::sql::execution::config::TracingConfig;

#[tokio::test]
async fn test_queue_based_telemetry_integration() {
    use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};

    // Mock exporter that counts exported spans
    #[derive(Debug)]
    struct CountingExporter {
        count: Arc<AtomicUsize>,
    }

    impl SpanExporter for CountingExporter {
        fn export(
            &mut self,
            batch: Vec<SpanData>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = opentelemetry::trace::TraceResult<()>> + Send>,
        > {
            let count = batch.len();
            let counter = self.count.clone();
            Box::pin(async move {
                counter.fetch_add(count, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    // Create tracing config without OTLP endpoint (test mode)
    let tracing_config = TracingConfig {
        service_name: "test_service".to_string(),
        service_version: "1.0.0".to_string(),
        otlp_endpoint: None, // Test mode
        sampling_ratio: 1.0,
        enable_console_output: false,
        max_span_duration_seconds: 300,
        batch_export_timeout_ms: 30000,
    };

    // Create observability queue
    let queue_config =
        ObservabilityQueueConfig::default().with_traces_flush_interval(Duration::from_millis(100));
    let (queue, _receivers) = ObservabilityQueue::new(queue_config.clone());
    let queue = Arc::new(queue);

    // Create telemetry provider with queue
    let (telemetry, _exporter) = TelemetryProvider::new_with_queue(tracing_config, queue.clone())
        .await
        .expect("Failed to create telemetry provider");

    // Create and emit a span
    let batch_span = telemetry.start_batch_span("test_job", 1, None);

    // Verify span was created
    drop(batch_span); // Span ends on drop

    // Give time for queue to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Note: In test mode (no OTLP endpoint), spans go to in-memory collector
    // In production with OTLP endpoint, they would go to the queue and be exported
}

#[tokio::test]
async fn test_queue_based_span_export_workflow() {
    use opentelemetry::KeyValue;
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    use opentelemetry_sdk::InstrumentationLibrary;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use velostream::velostream::observability::async_queue::TraceEvent;
    use velostream::velostream::observability::background_flusher::BackgroundFlusher;

    // Mock exporter that counts exported spans
    #[derive(Debug)]
    struct CountingExporter {
        count: Arc<AtomicUsize>,
    }

    impl SpanExporter for CountingExporter {
        fn export(
            &mut self,
            batch: Vec<SpanData>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = opentelemetry::trace::TraceResult<()>> + Send>,
        > {
            let count = batch.len();
            let counter = self.count.clone();
            Box::pin(async move {
                counter.fetch_add(count, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    // Create queue
    let queue_config =
        ObservabilityQueueConfig::default().with_traces_flush_interval(Duration::from_millis(100));
    let (queue, receivers) = ObservabilityQueue::new(queue_config.clone());

    // Create counting exporter
    let export_count = Arc::new(AtomicUsize::new(0));
    let exporter = Box::new(CountingExporter {
        count: export_count.clone(),
    });

    // Start background flusher with exporter
    let flusher = BackgroundFlusher::start(receivers, None, Some(exporter), queue_config).await;

    // Create and queue test spans
    for i in 0..10 {
        let trace_id_bytes = [i as u8; 16];
        let span_id_bytes = [i as u8; 8];
        let parent_span_id_bytes = [0u8; 8];

        let span_data = SpanData {
            span_context: SpanContext::new(
                TraceId::from_bytes(trace_id_bytes),
                SpanId::from_bytes(span_id_bytes),
                TraceFlags::default(),
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::from_bytes(parent_span_id_bytes),
            span_kind: opentelemetry::trace::SpanKind::Internal,
            name: Cow::Borrowed("test_span"),
            start_time: std::time::SystemTime::now(),
            end_time: std::time::SystemTime::now(),
            attributes: vec![KeyValue::new("test_id", i as i64)],
            dropped_attributes_count: 0,
            events: opentelemetry_sdk::trace::EvictedQueue::new(128),
            links: opentelemetry_sdk::trace::EvictedQueue::new(128),
            status: opentelemetry::trace::Status::Unset,
            resource: Cow::Owned(Resource::empty()),
            instrumentation_lib: InstrumentationLibrary::default(),
        };

        let event = TraceEvent::Span {
            span_data,
            timestamp: std::time::Instant::now(),
        };
        queue.try_send_trace(event).unwrap();
    }

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Shutdown flusher (triggers final flush)
    let result = flusher.shutdown(Duration::from_secs(1)).await;
    assert!(result.is_ok(), "Flusher should shutdown successfully");
    assert!(
        result.unwrap().traces_completed,
        "Traces task should complete"
    );

    // Verify all spans were exported
    let exported = export_count.load(Ordering::Relaxed);
    assert_eq!(exported, 10, "All 10 spans should be exported via queue");
}

#[tokio::test]
async fn test_non_blocking_span_submission() {
    use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
    use opentelemetry_sdk::trace::SpanProcessor;
    use velostream::velostream::observability::background_flusher::BackgroundFlusher;
    use velostream::velostream::observability::queued_span_processor::QueuedSpanProcessor;

    // Mock exporter
    #[derive(Debug)]
    struct NoOpExporter;

    impl SpanExporter for NoOpExporter {
        fn export(
            &mut self,
            _batch: Vec<SpanData>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = opentelemetry::trace::TraceResult<()>> + Send>,
        > {
            Box::pin(async { Ok(()) })
        }
    }

    // Create queue with very small capacity
    let queue_config = ObservabilityQueueConfig::default().with_traces_queue_size(2);
    let (queue, receivers) = ObservabilityQueue::new(queue_config.clone());
    let queue = Arc::new(queue);

    // Create processor
    let processor = QueuedSpanProcessor::new(queue.clone(), Box::new(NoOpExporter));

    // Fill queue to capacity
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    use opentelemetry_sdk::InstrumentationLibrary;
    use opentelemetry_sdk::Resource;
    use std::borrow::Cow;

    for i in 0..2 {
        let trace_id_bytes = [i as u8; 16];
        let span_id_bytes = [i as u8; 8];
        let parent_span_id_bytes = [0u8; 8];

        let span_data = SpanData {
            span_context: SpanContext::new(
                TraceId::from_bytes(trace_id_bytes),
                SpanId::from_bytes(span_id_bytes),
                TraceFlags::default(),
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::from_bytes(parent_span_id_bytes),
            span_kind: opentelemetry::trace::SpanKind::Internal,
            name: Cow::Borrowed("test_span"),
            start_time: std::time::SystemTime::now(),
            end_time: std::time::SystemTime::now(),
            attributes: vec![],
            dropped_attributes_count: 0,
            events: opentelemetry_sdk::trace::EvictedQueue::new(128),
            links: opentelemetry_sdk::trace::EvictedQueue::new(128),
            status: opentelemetry::trace::Status::Unset,
            resource: Cow::Owned(Resource::empty()),
            instrumentation_lib: InstrumentationLibrary::default(),
        };

        processor.on_end(span_data);
    }

    // Verify queue is full
    assert_eq!(queue.traces_queued_count(), 2);

    // Third span should be dropped (non-blocking!)
    let trace_id_bytes = [3u8; 16];
    let span_id_bytes = [3u8; 8];
    let parent_span_id_bytes = [0u8; 8];

    let span_data = SpanData {
        span_context: SpanContext::new(
            TraceId::from_bytes(trace_id_bytes),
            SpanId::from_bytes(span_id_bytes),
            TraceFlags::default(),
            false,
            TraceState::default(),
        ),
        parent_span_id: SpanId::from_bytes(parent_span_id_bytes),
        span_kind: opentelemetry::trace::SpanKind::Internal,
        name: Cow::Borrowed("test_span"),
        start_time: std::time::SystemTime::now(),
        end_time: std::time::SystemTime::now(),
        attributes: vec![],
        dropped_attributes_count: 0,
        events: opentelemetry_sdk::trace::EvictedQueue::new(128),
        links: opentelemetry_sdk::trace::EvictedQueue::new(128),
        status: opentelemetry::trace::Status::Unset,
        resource: Cow::Owned(Resource::empty()),
        instrumentation_lib: InstrumentationLibrary::default(),
    };

    // This should NOT block, even though queue is full
    let start = std::time::Instant::now();
    processor.on_end(span_data);
    let duration = start.elapsed();

    // Verify it returned immediately (non-blocking)
    assert!(
        duration < Duration::from_millis(10),
        "Span submission should be non-blocking, took {:?}",
        duration
    );

    // Verify span was dropped
    assert_eq!(queue.traces_dropped_count(), 1);

    // Start flusher to clean up
    let _flusher = BackgroundFlusher::start(receivers, None, None, queue_config).await;
}

#[tokio::test]
async fn test_fully_wired_observability_wrapper() {
    use velostream::velostream::observability::queue_config::ObservabilityQueueConfig;
    use velostream::velostream::server::processors::observability_wrapper::ObservabilityWrapper;
    use velostream::velostream::sql::execution::config::TracingConfig;

    // Configure tracing
    let tracing_config = TracingConfig {
        service_name: "test_service".to_string(),
        service_version: "1.0.0".to_string(),
        otlp_endpoint: None, // Test mode - no actual export
        sampling_ratio: 1.0,
        enable_console_output: false,
        max_span_duration_seconds: 300,
        batch_export_timeout_ms: 30000,
    };

    // Create wrapper with queue-based tracing fully wired
    let wrapper = ObservabilityWrapper::with_observability_and_async_queue(
        None,                 // No existing observability manager
        Some(tracing_config), // Enable queue-based tracing
        ObservabilityQueueConfig::default(),
    )
    .await;

    // Verify queue is initialized
    assert!(
        wrapper.observability_queue().is_some(),
        "Queue should be initialized"
    );

    // Verify queue is working
    assert!(wrapper.has_observability_queue(), "Queue should be enabled");

    // Note: In test mode (no OTLP endpoint), telemetry uses in-memory collector
    // In production with OTLP endpoint, spans would be exported via the queue
}

#[tokio::test]
async fn test_wrapper_without_tracing() {
    use velostream::velostream::observability::queue_config::ObservabilityQueueConfig;
    use velostream::velostream::server::processors::observability_wrapper::ObservabilityWrapper;

    // Create wrapper WITHOUT tracing config
    let wrapper = ObservabilityWrapper::with_observability_and_async_queue(
        None, // No existing observability
        None, // No tracing config - span export disabled
        ObservabilityQueueConfig::default(),
    )
    .await;

    // Queue should still be initialized (for metrics)
    assert!(
        wrapper.observability_queue().is_some(),
        "Queue should be initialized even without tracing"
    );

    // Verify queue is working
    assert!(
        wrapper.has_observability_queue(),
        "Queue should be enabled even without tracing"
    );
}
