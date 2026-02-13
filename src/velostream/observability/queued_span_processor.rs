//! Queued Span Processor - Non-blocking span export via async queue
//!
//! Replaces OpenTelemetry's BatchSpanProcessor with a queue-based approach
//! that never blocks the processing loop.

use crate::velostream::observability::async_queue::{ObservabilityQueue, TraceEvent};
use opentelemetry::trace::TraceResult;
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::trace::SpanProcessor;
use std::fmt;
use std::sync::Arc;

/// Span processor that submits spans to async queue (non-blocking)
///
/// Unlike BatchSpanProcessor which blocks when the queue is full,
/// this processor drops spans when the queue is full, ensuring
/// tracing never impacts processing performance.
pub struct QueuedSpanProcessor {
    queue: Arc<ObservabilityQueue>,
    #[allow(dead_code)]
    exporter: Box<dyn SpanExporter>,
}

impl fmt::Debug for QueuedSpanProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueuedSpanProcessor")
            .field("queue", &"<Arc<ObservabilityQueue>>")
            .field("exporter", &"<Box<dyn SpanExporter>>")
            .finish()
    }
}

impl QueuedSpanProcessor {
    /// Create a new queued span processor
    ///
    /// # Arguments
    ///
    /// * `queue` - Async observability queue for span submission
    /// * `exporter` - Span exporter (stored for future use, not used directly)
    pub fn new(queue: Arc<ObservabilityQueue>, exporter: Box<dyn SpanExporter>) -> Self {
        Self { queue, exporter }
    }
}

impl SpanProcessor for QueuedSpanProcessor {
    fn on_start(&self, _span: &mut opentelemetry_sdk::trace::Span, _cx: &opentelemetry::Context) {
        // No-op: We don't need to do anything on span start
    }

    fn on_end(&self, span: SpanData) {
        // Submit span to queue (non-blocking)
        let event = TraceEvent::Span {
            span_data: span,
            timestamp: std::time::Instant::now(),
        };

        if let Err(_) = self.queue.try_send_trace(event) {
            // Silently drop on full (never block)
            // The drop counter is incremented by try_send_trace
            log::debug!("Trace queue full, dropping span");
        }
    }

    fn force_flush(&self) -> TraceResult<()> {
        // Trigger flush via queue (non-blocking)
        // In practice, background flusher handles this
        Ok(())
    }

    fn shutdown(&mut self) -> TraceResult<()> {
        // Shutdown is handled by BackgroundFlusher
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::observability::queue_config::ObservabilityQueueConfig;
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    use opentelemetry_sdk::InstrumentationLibrary;
    use opentelemetry_sdk::Resource;
    use std::borrow::Cow;

    // Mock exporter for testing
    #[derive(Debug)]
    struct MockExporter;

    impl SpanExporter for MockExporter {
        fn export(
            &mut self,
            _batch: Vec<SpanData>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TraceResult<()>> + Send + 'static>>
        {
            Box::pin(async { Ok(()) })
        }
    }

    fn create_test_span() -> SpanData {
        let trace_id_bytes = [0u8; 16];
        let span_id_bytes = [1u8; 8];
        let parent_span_id_bytes = [0u8; 8];

        SpanData {
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
        }
    }

    #[test]
    fn test_processor_creation() {
        let config = ObservabilityQueueConfig::default();
        let (queue, _receivers) = ObservabilityQueue::new(config);
        let queue = Arc::new(queue);

        let exporter = Box::new(MockExporter);
        let processor = QueuedSpanProcessor::new(queue, exporter);

        // Just verify it compiles and can be created
        assert!(processor.force_flush().is_ok());
    }

    #[test]
    fn test_on_end_queues_span() {
        let config = ObservabilityQueueConfig::default();
        let (queue, _receivers) = ObservabilityQueue::new(config);
        let queue = Arc::new(queue);

        let exporter = Box::new(MockExporter);
        let processor = QueuedSpanProcessor::new(queue.clone(), exporter);

        let span = create_test_span();
        processor.on_end(span);

        // Verify span was queued
        assert_eq!(queue.traces_queued_count(), 1);
        assert_eq!(queue.traces_dropped_count(), 0);
    }

    #[test]
    fn test_on_end_drops_when_full() {
        // Tiny queue
        let config = ObservabilityQueueConfig::default().with_traces_queue_size(1);
        let (queue, _receivers) = ObservabilityQueue::new(config);
        let queue = Arc::new(queue);

        let exporter = Box::new(MockExporter);
        let processor = QueuedSpanProcessor::new(queue.clone(), exporter);

        // Fill queue
        processor.on_end(create_test_span());
        assert_eq!(queue.traces_queued_count(), 1);

        // Second span should be dropped
        processor.on_end(create_test_span());
        assert_eq!(queue.traces_dropped_count(), 1);
    }
}
