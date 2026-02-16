//! Tests for per-record span context with upstream parent linking
//!
//! With the per-record sampling migration, batch→child span hierarchy was removed.
//! RecordSpan is now the only span type created per-record.
//! These tests verify that upstream trace context (from traceparent headers) correctly
//! creates parent-child relationships at the per-record level.

#[cfg(test)]
mod span_linking_tests {
    use opentelemetry::trace::{SpanId, TraceFlags, TraceId, TraceState};
    use serial_test::serial;
    use velostream::velostream::observability::telemetry::TelemetryProvider;
    use velostream::velostream::sql::execution::config::TracingConfig;

    async fn create_test_telemetry(service_name: &str) -> TelemetryProvider {
        let config = TracingConfig {
            service_name: service_name.to_string(),
            otlp_endpoint: None,
            ..Default::default()
        };
        TelemetryProvider::new(config)
            .await
            .expect("Failed to create telemetry provider")
    }

    #[tokio::test]
    #[serial]
    async fn test_record_span_with_upstream_parent() {
        let telemetry = create_test_telemetry("record-span-parent-test").await;

        let upstream_ctx = opentelemetry::trace::SpanContext::new(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );

        let upstream_trace_id = upstream_ctx.trace_id();

        let mut span = telemetry.start_record_span("test-job", Some(&upstream_ctx));
        span.set_success();
        drop(span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let record_span = spans
            .iter()
            .find(|s| s.name.as_ref().contains("process:test-job"))
            .expect("Should find record span");

        assert_eq!(
            record_span.span_context.trace_id(),
            upstream_trace_id,
            "Record span should inherit upstream trace_id"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_record_span_without_upstream_is_root() {
        let telemetry = create_test_telemetry("record-span-root-test").await;

        let mut span = telemetry.start_record_span("test-job", None);
        span.set_success();
        drop(span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let record_span = spans
            .iter()
            .find(|s| s.name.as_ref().contains("process:test-job"))
            .expect("Should find record span");

        assert_eq!(
            record_span.parent_span_id,
            SpanId::INVALID,
            "Record span without upstream context should be root (no parent)"
        );

        assert!(
            record_span.span_context.is_valid(),
            "Root record span should have valid span context"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_independent_record_spans_have_separate_traces() {
        let telemetry = create_test_telemetry("independent-records-test").await;

        let span1 = telemetry.start_record_span("job-1", None);
        let ctx1 = span1.span_context().expect("should have context");
        let trace_id_1 = ctx1.trace_id();
        drop(span1);

        let span2 = telemetry.start_record_span("job-2", None);
        let ctx2 = span2.span_context().expect("should have context");
        let trace_id_2 = ctx2.trace_id();
        drop(span2);

        assert_ne!(
            trace_id_1, trace_id_2,
            "Independent record spans must have different trace IDs"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_streaming_span_still_works_with_parent() {
        let telemetry = create_test_telemetry("streaming-span-test").await;

        let record_span = telemetry.start_record_span("test-job", None);
        let parent_ctx = record_span.span_context().expect("should have context");
        let parent_trace_id = parent_ctx.trace_id();

        let mut streaming_span =
            telemetry.start_streaming_span("test-job", "deserialization", 100, Some(parent_ctx));
        streaming_span.set_success();
        drop(streaming_span);
        drop(record_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let streaming = spans
            .iter()
            .find(|s| s.name.contains("streaming:deserialization"))
            .expect("Should find streaming span");

        assert_eq!(
            streaming.span_context.trace_id(),
            parent_trace_id,
            "Streaming span should share trace_id with record span"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_job_lifecycle_span_still_works() {
        let telemetry = create_test_telemetry("lifecycle-test").await;

        let record_span = telemetry.start_record_span("test-job", None);
        let parent_ctx = record_span.span_context().expect("should have context");
        let parent_trace_id = parent_ctx.trace_id();

        let mut lifecycle =
            telemetry.start_job_lifecycle_span("test-job", "execute", Some(parent_ctx));
        lifecycle.set_success();
        drop(lifecycle);
        drop(record_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let lifecycle_span = spans
            .iter()
            .find(|s| s.name.contains("job.lifecycle:execute"))
            .expect("Should find lifecycle span");

        assert_eq!(
            lifecycle_span.span_context.trace_id(),
            parent_trace_id,
            "Lifecycle span should share trace_id with parent"
        );
    }
}
