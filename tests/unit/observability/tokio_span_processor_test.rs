//! Tests for TokioSpanProcessor configuration validation and behavior
//!
//! Covers config validation, dropped count tracking, force_flush, and shutdown.

use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceResult, TraceState};
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::trace::SpanProcessor;
use opentelemetry_sdk::{InstrumentationLibrary, Resource};
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use velostream::velostream::observability::tokio_span_processor::{
    TokioSpanProcessor, TokioSpanProcessorConfig,
};

// =========================================================================
// Config Validation Tests
// =========================================================================

#[test]
fn test_config_default_is_valid() {
    let config = TokioSpanProcessorConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_rejects_zero_queue_size() {
    let config = TokioSpanProcessorConfig {
        max_queue_size: 0,
        ..Default::default()
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("max_queue_size"), "Error: {}", err);
}

#[test]
fn test_config_rejects_zero_batch_size() {
    let config = TokioSpanProcessorConfig {
        max_export_batch_size: 0,
        ..Default::default()
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("max_export_batch_size"), "Error: {}", err);
}

#[test]
fn test_config_rejects_zero_scheduled_delay() {
    let config = TokioSpanProcessorConfig {
        scheduled_delay: Duration::ZERO,
        ..Default::default()
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("scheduled_delay"), "Error: {}", err);
}

#[test]
fn test_config_rejects_zero_export_timeout() {
    let config = TokioSpanProcessorConfig {
        export_timeout: Duration::ZERO,
        ..Default::default()
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("export_timeout"), "Error: {}", err);
}

#[test]
#[should_panic(expected = "TokioSpanProcessorConfig validation failed")]
fn test_processor_panics_on_invalid_config() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let exporter = CountingExporter::new();
        let _processor = TokioSpanProcessor::new(
            exporter,
            TokioSpanProcessorConfig {
                max_export_batch_size: 0,
                ..Default::default()
            },
        );
    });
}

// =========================================================================
// Processor Behavior Tests
// =========================================================================

/// Mock exporter that counts exported spans
#[derive(Debug, Clone)]
struct CountingExporter {
    exported: Arc<Mutex<Vec<String>>>,
}

impl CountingExporter {
    fn new() -> Self {
        Self {
            exported: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl SpanExporter for CountingExporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TraceResult<()>> + Send + 'static>>
    {
        let exported = self.exported.clone();
        Box::pin(async move {
            let mut guard = exported.lock().unwrap();
            for span in batch {
                guard.push(span.name.to_string());
            }
            Ok(())
        })
    }
}

fn create_test_span(name: &str) -> SpanData {
    let trace_bytes: [u8; 16] = rand::random();
    let span_bytes: [u8; 8] = rand::random();

    SpanData {
        span_context: SpanContext::new(
            TraceId::from_bytes(trace_bytes),
            SpanId::from_bytes(span_bytes),
            TraceFlags::default(),
            false,
            TraceState::default(),
        ),
        parent_span_id: SpanId::from_bytes([0u8; 8]),
        span_kind: opentelemetry::trace::SpanKind::Internal,
        name: Cow::Owned(name.to_string()),
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

#[tokio::test]
async fn test_dropped_count_starts_at_zero() {
    let exporter = CountingExporter::new();
    let processor = TokioSpanProcessor::new(
        exporter,
        TokioSpanProcessorConfig {
            max_queue_size: 100,
            ..Default::default()
        },
    );
    assert_eq!(processor.dropped_count(), 0);
}

#[tokio::test]
async fn test_dropped_count_increments_when_queue_full() {
    let exporter = CountingExporter::new();

    // Tiny queue that fills quickly
    let processor = TokioSpanProcessor::new(
        exporter,
        TokioSpanProcessorConfig {
            max_queue_size: 2,
            max_export_batch_size: 1,
            scheduled_delay: Duration::from_secs(60), // Long delay so queue stays full
            export_timeout: Duration::from_secs(1),
        },
    );

    // Send more spans than the queue can hold
    for i in 0..20 {
        processor.on_end(create_test_span(&format!("overflow_{}", i)));
    }

    // Some spans should have been dropped
    assert!(
        processor.dropped_count() > 0,
        "Expected drops with queue size 2 and 20 spans, got {}",
        processor.dropped_count()
    );
}

#[tokio::test]
async fn test_force_flush_exports_buffered_spans() {
    let exporter = CountingExporter::new();
    let exported = exporter.exported.clone();

    let processor = TokioSpanProcessor::new(
        exporter,
        TokioSpanProcessorConfig {
            max_queue_size: 100,
            max_export_batch_size: 100,
            scheduled_delay: Duration::from_secs(60), // Long delay — force_flush should trigger
            export_timeout: Duration::from_secs(5),
        },
    );

    // Send spans
    for i in 0..5 {
        processor.on_end(create_test_span(&format!("flush_test_{}", i)));
    }

    // Small sleep to let the export loop buffer the spans
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Force flush
    processor.force_flush().unwrap();

    // Wait a bit for the async acknowledgement
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = exported.lock().unwrap().len();
    assert_eq!(count, 5, "force_flush should export all 5 buffered spans");
}

#[tokio::test]
async fn test_shutdown_drains_remaining_spans() {
    let exporter = CountingExporter::new();
    let exported = exporter.exported.clone();

    let mut processor = TokioSpanProcessor::new(
        exporter,
        TokioSpanProcessorConfig {
            max_queue_size: 100,
            max_export_batch_size: 100,
            scheduled_delay: Duration::from_secs(60), // Long delay — only shutdown should flush
            export_timeout: Duration::from_secs(5),
        },
    );

    // Send spans
    for i in 0..8 {
        processor.on_end(create_test_span(&format!("shutdown_test_{}", i)));
    }

    // Small sleep to let spans enter the buffer
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Shutdown should drain and export remaining spans
    processor.shutdown().unwrap();

    // Wait for async processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = exported.lock().unwrap().len();
    assert_eq!(
        count, 8,
        "shutdown should drain and export all 8 remaining spans"
    );
}
