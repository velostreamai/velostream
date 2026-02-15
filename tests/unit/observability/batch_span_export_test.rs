//! Test that validates continuous span export across multiple flush cycles.
//!
//! This test was created to reproduce and verify the fix for the
//! `BatchSpanProcessor` bug in `opentelemetry_sdk 0.21.2`, where the
//! background export loop stops after the first cycle due to a
//! `stream::select` / `FusedStream` interaction.
//!
//! The `TokioSpanProcessor` replacement uses `tokio::select!` instead,
//! which polls futures independently and avoids the bug.

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

/// Mock exporter that records exported spans with timestamps for wave analysis
#[derive(Debug, Clone)]
struct WaveRecordingExporter {
    exported: Arc<Mutex<Vec<(String, std::time::Instant)>>>,
}

impl WaveRecordingExporter {
    fn new() -> Self {
        Self {
            exported: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl SpanExporter for WaveRecordingExporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TraceResult<()>> + Send + 'static>>
    {
        let exported = self.exported.clone();
        let now = std::time::Instant::now();
        Box::pin(async move {
            let mut guard = exported.lock().unwrap();
            for span in batch {
                guard.push((span.name.to_string(), now));
            }
            Ok(())
        })
    }
}

fn create_test_span(name: &str) -> SpanData {
    // Use random-ish IDs so spans are distinguishable
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

/// Core test: Spans from multiple waves are ALL exported, not just the first wave.
///
/// This is the exact scenario that fails with `BatchSpanProcessor` 0.21.2:
/// - Wave 1 spans are exported on the first flush cycle
/// - The background task dies after the first cycle
/// - Wave 2 and Wave 3 spans are never exported
///
/// With `TokioSpanProcessor`, all waves are exported correctly.
#[tokio::test]
async fn test_multi_wave_continuous_export() {
    let exporter = WaveRecordingExporter::new();
    let exported = exporter.exported.clone();

    let config = TokioSpanProcessorConfig {
        max_queue_size: 4096,
        max_export_batch_size: 1024,
        scheduled_delay: Duration::from_millis(100), // Short flush interval for test
        ..Default::default()
    };

    let processor = TokioSpanProcessor::new(exporter, config);

    // Wave 1: Send 10 spans
    for i in 0..10 {
        processor.on_end(create_test_span(&format!("wave1_span_{}", i)));
    }

    // Wait for first flush cycle
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify wave 1 was exported
    let wave1_count = exported.lock().unwrap().len();
    assert_eq!(
        wave1_count, 10,
        "Wave 1: all 10 spans should be exported after first flush"
    );

    // Wave 2: Send 10 more spans (this is the wave that BatchSpanProcessor drops)
    for i in 0..10 {
        processor.on_end(create_test_span(&format!("wave2_span_{}", i)));
    }

    // Wait for second flush cycle
    tokio::time::sleep(Duration::from_millis(150)).await;

    let wave2_count = exported.lock().unwrap().len();
    assert_eq!(
        wave2_count, 20,
        "Wave 2: all 20 spans (wave1 + wave2) should be exported after second flush"
    );

    // Wave 3: Send 10 more spans
    for i in 0..10 {
        processor.on_end(create_test_span(&format!("wave3_span_{}", i)));
    }

    // Wait for third flush cycle
    tokio::time::sleep(Duration::from_millis(150)).await;

    let total_count = exported.lock().unwrap().len();
    assert_eq!(
        total_count, 30,
        "Wave 3: all 30 spans should be exported after three flush cycles"
    );

    // Verify spans from all waves are present
    let spans = exported.lock().unwrap();
    let wave1_spans: Vec<_> = spans
        .iter()
        .filter(|(n, _)| n.starts_with("wave1_"))
        .collect();
    let wave2_spans: Vec<_> = spans
        .iter()
        .filter(|(n, _)| n.starts_with("wave2_"))
        .collect();
    let wave3_spans: Vec<_> = spans
        .iter()
        .filter(|(n, _)| n.starts_with("wave3_"))
        .collect();

    assert_eq!(wave1_spans.len(), 10, "All wave 1 spans present");
    assert_eq!(wave2_spans.len(), 10, "All wave 2 spans present");
    assert_eq!(wave3_spans.len(), 10, "All wave 3 spans present");
}

/// Test that the processor continues to work after many flush cycles
#[tokio::test]
async fn test_sustained_export_over_many_cycles() {
    let exporter = WaveRecordingExporter::new();
    let exported = exporter.exported.clone();

    let config = TokioSpanProcessorConfig {
        max_queue_size: 4096,
        max_export_batch_size: 1024,
        scheduled_delay: Duration::from_millis(50),
        ..Default::default()
    };

    let processor = TokioSpanProcessor::new(exporter, config);

    // Send spans across 5 cycles
    for cycle in 0..5 {
        for i in 0..5 {
            processor.on_end(create_test_span(&format!("cycle{}_span_{}", cycle, i)));
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }

    // Final wait for last flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    let total = exported.lock().unwrap().len();
    assert_eq!(
        total, 25,
        "All 25 spans across 5 cycles should be exported, got {}",
        total
    );
}
