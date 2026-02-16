//! Tests for continuous span creation across multiple record processing cycles
//!
//! These tests validate that spans are created AND exported continuously,
//! not just during the first flush cycle. They target the full span pipeline:
//!
//!   TelemetryProvider::start_record_span()
//!     -> OpenTelemetry Tracer creates span
//!     -> Span dropped -> SpanProcessor::on_end()
//!     -> SpanExporter::export()
//!
//! The production bug was: spans were exported during the first ~2 flush cycles
//! (startup lifecycle spans), then no more spans reached the exporter despite
//! batches being actively processed. These tests reproduce and verify the fix.

use serial_test::serial;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceResult, TraceState};
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::trace::{SpanProcessor, TracerProvider};

use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::observability::tokio_span_processor::{
    TokioSpanProcessor, TokioSpanProcessorConfig,
};
use velostream::velostream::observability::trace_propagation;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::config::TracingConfig;

use crate::unit::observability_test_helpers::create_test_observability_manager;

// =============================================================================
// Helper: Mock exporter that records span names and counts per wave
// =============================================================================

#[derive(Debug, Clone)]
struct WaveTrackingExporter {
    spans: Arc<Mutex<Vec<String>>>,
}

impl WaveTrackingExporter {
    fn new() -> Self {
        Self {
            spans: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl SpanExporter for WaveTrackingExporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TraceResult<()>> + Send + 'static>>
    {
        let spans = self.spans.clone();
        Box::pin(async move {
            let mut guard = spans.lock().unwrap();
            for span in batch {
                guard.push(span.name.to_string());
            }
            Ok(())
        })
    }
}

// =============================================================================
// Test 1: TelemetryProvider creates record spans continuously via in-memory collector
// =============================================================================

#[tokio::test]
#[serial]
async fn test_telemetry_provider_creates_record_spans_across_multiple_cycles() {
    let config = TracingConfig {
        service_name: "continuous-span-test".to_string(),
        otlp_endpoint: None, // In-memory span collection
        sampling_ratio: 1.0,
        ..Default::default()
    };
    let telemetry = TelemetryProvider::new(config)
        .await
        .expect("Failed to create telemetry provider");

    // Simulate 10 consecutive record processing cycles
    for i in 0..10u64 {
        let record_span = telemetry.start_record_span("test-job", None);

        // Verify each record span has a valid span context
        let span_ctx = record_span
            .span_context()
            .unwrap_or_else(|| panic!("Record #{} should have a valid span context", i));

        assert!(
            span_ctx.is_valid(),
            "Record #{}: span context should be valid (trace_id={}, span_id={})",
            i,
            span_ctx.trace_id(),
            span_ctx.span_id()
        );

        // Drop the span (triggers on_end -> collector)
        drop(record_span);
    }

    // All 10 record spans should have been collected
    let collected = telemetry.span_count();
    assert_eq!(
        collected, 10,
        "All 10 record spans should be collected in memory, got {}",
        collected
    );
}

// =============================================================================
// Test 2: ObservabilityHelper::start_record_span returns Some continuously
// =============================================================================

#[tokio::test]
#[serial]
async fn test_observability_helper_creates_record_spans_continuously() {
    let obs_manager = create_test_observability_manager("obs-helper-continuous-test").await;
    let obs_option = Some(obs_manager);

    // Simulate 20 consecutive record span calls
    for i in 0..20u64 {
        let record_span = ObservabilityHelper::start_record_span(&obs_option, "test-job", None);

        assert!(
            record_span.is_some(),
            "Record #{}: ObservabilityHelper::start_record_span should return Some",
            i
        );

        let span = record_span.unwrap();
        let ctx = span.span_context();
        assert!(
            ctx.is_some(),
            "Record #{}: RecordSpan should have a span context",
            i
        );
        assert!(
            ctx.unwrap().is_valid(),
            "Record #{}: span context should be valid",
            i
        );

        // Drop span to trigger on_end
        drop(span);
    }
}

// =============================================================================
// Test 3: Record spans with upstream trace context are created continuously
// =============================================================================

#[tokio::test]
#[serial]
async fn test_continuous_record_span_creation_with_upstream_context() {
    let obs_manager = create_test_observability_manager("upstream-context-continuous-test").await;
    let obs_option = Some(obs_manager);

    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let upstream_ctx = trace_propagation::extract_trace_context(&{
        let mut h = std::collections::HashMap::new();
        h.insert("traceparent".to_string(), traceparent.to_string());
        h
    })
    .expect("Should extract upstream context");

    // Simulate 10 records with upstream trace context
    for i in 0..10u64 {
        let record_span =
            ObservabilityHelper::start_record_span(&obs_option, "linked-job", Some(&upstream_ctx));

        assert!(
            record_span.is_some(),
            "Record #{}: should create span even with upstream context",
            i
        );

        let span = record_span.unwrap();
        let ctx = span.span_context().unwrap();

        // The child span should inherit the parent's trace_id
        assert_eq!(
            format!("{}", ctx.trace_id()),
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "Record #{}: child span should have parent's trace_id",
            i
        );

        // But should have its own unique span_id (different from parent)
        assert_ne!(
            format!("{}", ctx.span_id()),
            "00f067aa0ba902b7",
            "Record #{}: child span should have unique span_id",
            i
        );

        drop(span);
    }
}

// =============================================================================
// Test 4: Full pipeline - TokioSpanProcessor exports spans across many cycles
// =============================================================================

#[tokio::test]
async fn test_tokio_span_processor_exports_across_multiple_flush_cycles() {
    let exporter = WaveTrackingExporter::new();
    let exported_spans = exporter.spans.clone();

    let config = TokioSpanProcessorConfig {
        max_queue_size: 4096,
        max_export_batch_size: 512,
        scheduled_delay: Duration::from_millis(50), // Short interval for test
        export_timeout: Duration::from_secs(5),
    };

    let processor = TokioSpanProcessor::new(exporter, config);

    // Wave 1: Send spans, wait for flush
    for i in 0..5 {
        processor.on_end(create_test_span_data(&format!("wave1_span_{}", i)));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let wave1_count = exported_spans.lock().unwrap().len();
    assert_eq!(wave1_count, 5, "Wave 1: all 5 spans should be exported");

    // Wave 2: More spans after first flush cycle
    for i in 0..5 {
        processor.on_end(create_test_span_data(&format!("wave2_span_{}", i)));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let wave2_count = exported_spans.lock().unwrap().len();
    assert_eq!(
        wave2_count, 10,
        "Wave 2: all 10 spans (wave1+wave2) should be exported"
    );

    // Wave 3: Third flush cycle
    for i in 0..5 {
        processor.on_end(create_test_span_data(&format!("wave3_span_{}", i)));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let wave3_count = exported_spans.lock().unwrap().len();
    assert_eq!(
        wave3_count, 15,
        "Wave 3: all 15 spans should be exported after three cycles"
    );

    // Wave 4 and 5: Continued operation
    for wave in 4..=5 {
        for i in 0..5 {
            processor.on_end(create_test_span_data(&format!("wave{}_span_{}", wave, i)));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let final_count = exported_spans.lock().unwrap().len();
    assert_eq!(
        final_count, 25,
        "All 25 spans across 5 waves should be exported, got {}",
        final_count
    );

    // Verify wave distribution
    let names = exported_spans.lock().unwrap().clone();
    for wave in 1..=5 {
        let wave_spans: Vec<_> = names
            .iter()
            .filter(|n| n.starts_with(&format!("wave{}_", wave)))
            .collect();
        assert_eq!(
            wave_spans.len(),
            5,
            "Wave {}: should have exactly 5 spans, got {}",
            wave,
            wave_spans.len()
        );
    }
}

// =============================================================================
// Test 5: Full end-to-end - TracerProvider + TokioSpanProcessor + global tracer
// =============================================================================

#[tokio::test]
#[serial]
async fn test_end_to_end_tracer_to_exporter_across_batches() {
    use opentelemetry::trace::{Span, SpanKind, Status, Tracer};
    use opentelemetry_sdk::trace::config;

    let exporter = WaveTrackingExporter::new();
    let exported_spans = exporter.spans.clone();

    let processor = TokioSpanProcessor::new(
        exporter,
        TokioSpanProcessorConfig {
            max_queue_size: 4096,
            max_export_batch_size: 512,
            scheduled_delay: Duration::from_millis(50),
            export_timeout: Duration::from_secs(5),
        },
    );

    // Build a TracerProvider with TokioSpanProcessor (mimics production setup)
    let provider = TracerProvider::builder()
        .with_span_processor(processor)
        .with_config(
            config()
                .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
                .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default()),
        )
        .build();

    // Set as global (same as production code does)
    opentelemetry::global::set_tracer_provider(provider);

    let tracer = opentelemetry::global::tracer("e2e-test-service");

    // Simulate 10 batch processing cycles through the global tracer
    for batch_id in 0..10 {
        let mut span = tracer
            .span_builder(format!("batch:{}", batch_id))
            .with_kind(SpanKind::Internal)
            .start(&tracer);
        span.set_status(Status::Ok);
        // Drop triggers on_end -> TokioSpanProcessor -> exporter
        drop(span);
    }

    // Wait for flush cycle
    tokio::time::sleep(Duration::from_millis(150)).await;

    let total = exported_spans.lock().unwrap().len();
    assert_eq!(
        total, 10,
        "All 10 spans created via global tracer should reach the exporter, got {}",
        total
    );

    // Verify each batch span was exported
    let names = exported_spans.lock().unwrap().clone();
    for batch_id in 0..10 {
        let expected_name = format!("batch:{}", batch_id);
        assert!(
            names.contains(&expected_name),
            "Span '{}' should have been exported",
            expected_name
        );
    }
}

// =============================================================================
// Test 6: Concurrent read lock doesn't block record span creation
// =============================================================================

#[tokio::test]
#[serial]
async fn test_record_span_creation_under_concurrent_read_locks() {
    let obs_manager = create_test_observability_manager("concurrent-lock-test").await;
    let obs_option = Some(obs_manager.clone());

    // Hold a read lock while creating record spans (simulates concurrent access)
    let _read_guard = obs_manager.read().await;

    // Create record spans while read lock is held - should succeed because
    // try_read() allows multiple concurrent readers
    for i in 0..5u64 {
        let record_span =
            ObservabilityHelper::start_record_span(&obs_option, "concurrent-job", None);

        assert!(
            record_span.is_some(),
            "Record #{}: should create span even with concurrent read lock held",
            i
        );
    }
}

// =============================================================================
// Test 7: Mixed upstream/no-upstream context across record spans
// =============================================================================

#[tokio::test]
#[serial]
async fn test_alternating_upstream_context_across_record_spans() {
    let obs_manager = create_test_observability_manager("alternating-context-test").await;
    let obs_option = Some(obs_manager);

    let traceparent = "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01";
    let upstream_ctx = trace_propagation::extract_trace_context(&{
        let mut h = std::collections::HashMap::new();
        h.insert("traceparent".to_string(), traceparent.to_string());
        h
    })
    .expect("Should extract upstream context");

    // Alternate between records with and without upstream trace context
    for i in 0..10u64 {
        let upstream = if i % 2 == 0 {
            Some(&upstream_ctx)
        } else {
            None
        };

        let record_span =
            ObservabilityHelper::start_record_span(&obs_option, "alternating-job", upstream);

        assert!(
            record_span.is_some(),
            "Record #{}: should create span regardless of upstream context presence",
            i
        );

        let ctx = record_span.unwrap().span_context().unwrap();
        assert!(
            ctx.is_valid(),
            "Record #{}: span context should always be valid",
            i
        );
    }
}

// =============================================================================
// Test 8: ParentBased sampler honors sampling across batches
// =============================================================================

#[tokio::test]
#[serial]
async fn test_parent_based_sampler_continuous_sampling() {
    use opentelemetry::trace::{Span, SpanKind, Status, Tracer};
    use opentelemetry_sdk::trace::{Sampler, config};

    let exporter = WaveTrackingExporter::new();
    let exported_spans = exporter.spans.clone();

    let processor = TokioSpanProcessor::new(
        exporter,
        TokioSpanProcessorConfig {
            max_queue_size: 4096,
            max_export_batch_size: 512,
            scheduled_delay: Duration::from_millis(50),
            export_timeout: Duration::from_secs(5),
        },
    );

    // Use ParentBased(AlwaysOn) sampler - same as production with sampling_ratio >= 0.99
    let provider = TracerProvider::builder()
        .with_span_processor(processor)
        .with_config(
            config()
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
                .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default()),
        )
        .build();

    opentelemetry::global::set_tracer_provider(provider);
    let tracer = opentelemetry::global::tracer("parent-based-test");

    // Create root spans (no parent) across multiple cycles
    for i in 0..10 {
        let mut span = tracer
            .span_builder(format!("root_span_{}", i))
            .with_kind(SpanKind::Internal)
            .start(&tracer);
        span.set_status(Status::Ok);
        drop(span);
    }

    tokio::time::sleep(Duration::from_millis(150)).await;

    let count = exported_spans.lock().unwrap().len();
    assert_eq!(
        count, 10,
        "ParentBased(AlwaysOn) should sample all root spans, got {}",
        count
    );

    // Now test with parent context (simulating upstream Kafka trace)
    for i in 0..10 {
        use opentelemetry::trace::TraceContextExt;

        let parent_ctx = SpanContext::new(
            TraceId::from_bytes([0xAB; 16]),
            SpanId::from_bytes([(i + 1) as u8; 8]),
            TraceFlags::SAMPLED,
            true, // remote
            TraceState::default(),
        );

        let parent_cx = opentelemetry::Context::new().with_remote_span_context(parent_ctx);
        let mut span = tracer
            .span_builder(format!("child_span_{}", i))
            .with_kind(SpanKind::Consumer)
            .start_with_context(&tracer, &parent_cx);
        span.set_status(Status::Ok);
        drop(span);
    }

    tokio::time::sleep(Duration::from_millis(150)).await;

    let total = exported_spans.lock().unwrap().len();
    assert_eq!(
        total, 20,
        "ParentBased should also sample child spans with SAMPLED parent, got {}",
        total
    );
}

// =============================================================================
// Test 9: Record spans created continuously for each record in a batch
// =============================================================================

#[tokio::test]
#[serial]
async fn test_record_spans_created_for_each_sampled_record() {
    let obs_manager = create_test_observability_manager("record-spans-continuous-test").await;
    let obs_option = Some(obs_manager.clone());

    // Simulate full record processing pipeline across 5 batches of sampled records
    for batch_id in 0..5u64 {
        // Each batch has 4 sampled records
        for record_idx in 0..4u64 {
            let mut record_span =
                ObservabilityHelper::start_record_span(&obs_option, "pipeline-job", None);
            assert!(
                record_span.is_some(),
                "Batch #{}, Record #{}: record span should be created",
                batch_id,
                record_idx
            );

            let span = record_span.as_mut().unwrap();
            span.set_output_count(1);
            span.set_success();
            drop(record_span);
        }

        // Also record metrics for the batch (metrics-only, 4 args)
        ObservabilityHelper::record_deserialization(&obs_option, "pipeline-job", 20, 5);
        ObservabilityHelper::record_serialization_success(&obs_option, "pipeline-job", 20, 3);
    }

    // Verify spans were collected: 4 record spans per batch * 5 batches = 20 spans
    let obs_lock = obs_manager.read().await;
    if let Some(telemetry) = obs_lock.telemetry() {
        let span_count = telemetry.span_count();
        assert_eq!(
            span_count, 20,
            "Should have 20 record spans (4 per batch * 5 batches), got {}",
            span_count
        );
    }
}

// =============================================================================
// Test 10: Rapid-fire span creation doesn't lose spans
// =============================================================================

#[tokio::test]
async fn test_rapid_span_creation_no_loss() {
    let exporter = WaveTrackingExporter::new();
    let exported_spans = exporter.spans.clone();

    let processor = TokioSpanProcessor::new(
        exporter,
        TokioSpanProcessorConfig {
            max_queue_size: 8192,
            max_export_batch_size: 1024,
            scheduled_delay: Duration::from_millis(50),
            export_timeout: Duration::from_secs(5),
        },
    );

    // Rapidly create 1000 spans without any delay
    for i in 0..1000 {
        processor.on_end(create_test_span_data(&format!("rapid_span_{}", i)));
    }

    // Wait for export
    tokio::time::sleep(Duration::from_millis(200)).await;

    let total = exported_spans.lock().unwrap().len();
    assert_eq!(
        total, 1000,
        "All 1000 rapidly-created spans should be exported, got {}",
        total
    );
}

// =============================================================================
// Helper: Create SpanData for direct processor testing
// =============================================================================

fn create_test_span_data(name: &str) -> SpanData {
    use std::borrow::Cow;

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
        resource: Cow::Owned(opentelemetry_sdk::Resource::empty()),
        instrumentation_lib: opentelemetry_sdk::InstrumentationLibrary::default(),
    }
}
