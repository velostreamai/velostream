//! Tests for span parent-child hierarchy using start_with_context() pattern
//!
//! Verifies that child spans (deserialization, SQL processing, serialization)
//! are true children of their parent batch span via SpanData.parent_span_id.

#[cfg(test)]
mod span_linking_tests {
    use opentelemetry::trace::SpanId;
    use serial_test::serial;
    use velostream::velostream::observability::telemetry::TelemetryProvider;
    use velostream::velostream::sql::execution::config::TracingConfig;

    /// Create a test telemetry provider with in-memory span collection
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

    /// Test 1: Streaming span is a child of batch span (same trace_id, correct parent_span_id)
    #[tokio::test]
    #[serial]
    async fn test_streaming_span_is_child_of_batch_span() {
        let telemetry = create_test_telemetry("streaming-child-test").await;

        // Create parent batch span
        let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
        let parent_ctx = batch_span
            .span_context()
            .expect("batch should have context");
        let parent_trace_id = parent_ctx.trace_id();
        let parent_span_id = parent_ctx.span_id();

        // Create child streaming span with parent context
        let mut child_span =
            telemetry.start_streaming_span("test-job", "deserialization", 100, Some(parent_ctx));
        child_span.set_success();
        drop(child_span);
        drop(batch_span);

        // Collect and verify spans
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let streaming_span = spans
            .iter()
            .find(|s| s.name.contains("streaming:deserialization"))
            .expect("Should find streaming span");

        // Verify parent-child: same trace_id, parent_span_id matches batch span_id
        assert_eq!(
            streaming_span.span_context.trace_id(),
            parent_trace_id,
            "Child streaming span must share trace_id with parent batch span"
        );
        assert_eq!(
            streaming_span.parent_span_id, parent_span_id,
            "Streaming span parent_span_id must match batch span's span_id"
        );
    }

    /// Test 2: SQL query span is a child of batch span
    #[tokio::test]
    #[serial]
    async fn test_sql_query_span_is_child_of_batch_span() {
        let telemetry = create_test_telemetry("sql-child-test").await;

        let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
        let parent_ctx = batch_span
            .span_context()
            .expect("batch should have context");
        let parent_trace_id = parent_ctx.trace_id();
        let parent_span_id = parent_ctx.span_id();

        let mut sql_span = telemetry.start_sql_query_span(
            "test-job",
            "SELECT * FROM test",
            "test_source",
            Some(parent_ctx),
        );
        sql_span.set_success();
        drop(sql_span);
        drop(batch_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let sql_span_data = spans
            .iter()
            .find(|s| s.name.contains("sql_query"))
            .expect("Should find SQL query span");

        assert_eq!(
            sql_span_data.span_context.trace_id(),
            parent_trace_id,
            "SQL span must share trace_id with parent"
        );
        assert_eq!(
            sql_span_data.parent_span_id, parent_span_id,
            "SQL span parent_span_id must match batch span's span_id"
        );
    }

    /// Test 3: No parent context starts a new trace (root span)
    #[tokio::test]
    #[serial]
    async fn test_no_parent_context_starts_new_trace() {
        let telemetry = create_test_telemetry("root-span-test").await;

        let mut span = telemetry.start_streaming_span("test-job", "standalone", 50, None);
        span.set_success();
        drop(span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let standalone = spans
            .iter()
            .find(|s| s.name.contains("streaming:standalone"))
            .expect("Should find standalone span");

        // Root span should have invalid (zero) parent_span_id
        assert_eq!(
            standalone.parent_span_id,
            SpanId::INVALID,
            "Root span should have no parent"
        );

        // But should have a valid trace_id
        assert!(
            standalone.span_context.is_valid(),
            "Root span should have valid span context"
        );
    }

    /// Test 4: Full pipeline hierarchy (batch -> deser -> sql -> ser)
    #[tokio::test]
    #[serial]
    async fn test_full_pipeline_span_hierarchy() {
        let telemetry = create_test_telemetry("pipeline-hierarchy-test").await;

        // Create batch span
        let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
        let parent_ctx = batch_span
            .span_context()
            .expect("batch should have context");
        let batch_trace_id = parent_ctx.trace_id();
        let batch_span_id = parent_ctx.span_id();

        // Create deserialization child
        let mut deser = telemetry.start_streaming_span(
            "test-job",
            "deserialization",
            100,
            Some(parent_ctx.clone()),
        );
        deser.set_success();
        drop(deser);

        // Create SQL processing child
        let mut sql = telemetry.start_sql_query_span(
            "test-job",
            "SELECT id FROM stream",
            "stream",
            Some(parent_ctx.clone()),
        );
        sql.set_success();
        drop(sql);

        // Create serialization child
        let mut ser = telemetry.start_streaming_span(
            "test-job",
            "serialization",
            95,
            Some(parent_ctx.clone()),
        );
        ser.set_success();
        drop(ser);

        drop(batch_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        assert_eq!(spans.len(), 4, "Should have 4 spans: batch + 3 children");

        // All spans must share the same trace_id
        for span in &spans {
            assert_eq!(
                span.span_context.trace_id(),
                batch_trace_id,
                "All spans must share batch trace_id, but '{}' has different trace_id",
                span.name
            );
        }

        // All child spans must have batch as parent
        let child_spans: Vec<_> = spans
            .iter()
            .filter(|s| !s.name.contains("batch:"))
            .collect();
        assert_eq!(child_spans.len(), 3, "Should have 3 child spans");

        for child in &child_spans {
            assert_eq!(
                child.parent_span_id, batch_span_id,
                "Child span '{}' must have batch span as parent",
                child.name
            );
        }
    }

    /// Test 5: Job lifecycle span is a child of batch span
    #[tokio::test]
    #[serial]
    async fn test_job_lifecycle_span_is_child_of_parent() {
        let telemetry = create_test_telemetry("lifecycle-child-test").await;

        let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
        let parent_ctx = batch_span
            .span_context()
            .expect("batch should have context");
        let parent_trace_id = parent_ctx.trace_id();
        let parent_span_id = parent_ctx.span_id();

        let mut lifecycle =
            telemetry.start_job_lifecycle_span("test-job", "execute", Some(parent_ctx));
        lifecycle.set_success();
        drop(lifecycle);
        drop(batch_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let lifecycle_span = spans
            .iter()
            .find(|s| s.name.contains("job.lifecycle:execute"))
            .expect("Should find lifecycle span");

        assert_eq!(
            lifecycle_span.span_context.trace_id(),
            parent_trace_id,
            "Lifecycle span must share trace_id with parent"
        );
        assert_eq!(
            lifecycle_span.parent_span_id, parent_span_id,
            "Lifecycle span parent_span_id must match parent's span_id"
        );
    }

    /// Test 6: Multiple batches produce independent traces
    #[tokio::test]
    #[serial]
    async fn test_independent_batches_have_separate_traces() {
        let telemetry = create_test_telemetry("independent-batches-test").await;

        let batch1 = telemetry.start_batch_span("job-1", 1, None, Vec::new());
        let ctx1 = batch1.span_context().expect("batch1 should have context");
        let trace_id_1 = ctx1.trace_id();
        drop(batch1);

        let batch2 = telemetry.start_batch_span("job-2", 2, None, Vec::new());
        let ctx2 = batch2.span_context().expect("batch2 should have context");
        let trace_id_2 = ctx2.trace_id();
        drop(batch2);

        assert_ne!(
            trace_id_1, trace_id_2,
            "Independent batches must have different trace IDs"
        );
    }

    /// Test 7: Invalid parent context falls back to new trace
    #[tokio::test]
    #[serial]
    async fn test_invalid_parent_context_starts_new_trace() {
        let telemetry = create_test_telemetry("invalid-parent-test").await;

        // Create an invalid SpanContext
        let invalid_ctx = opentelemetry::trace::SpanContext::empty_context();
        assert!(!invalid_ctx.is_valid(), "Empty context should be invalid");

        let mut span = telemetry.start_streaming_span(
            "test-job",
            "with-invalid-parent",
            10,
            Some(invalid_ctx),
        );
        span.set_success();
        drop(span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let span_data = spans
            .iter()
            .find(|s| s.name.contains("with-invalid-parent"))
            .expect("Should find span");

        // With invalid parent, should be a root span (no parent)
        assert_eq!(
            span_data.parent_span_id,
            SpanId::INVALID,
            "Invalid parent context should result in root span"
        );
    }

    /// Test 8: Child spans from same parent all share trace_id
    #[tokio::test]
    #[serial]
    async fn test_sibling_spans_share_trace_id() {
        let telemetry = create_test_telemetry("sibling-spans-test").await;

        let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
        let parent_ctx = batch_span
            .span_context()
            .expect("batch should have context");
        let batch_trace_id = parent_ctx.trace_id();

        // Create multiple sibling spans from same parent
        let operations = vec![
            "kafka-read",
            "deserialize-avro",
            "sql-transform",
            "serialize-json",
        ];
        for op in &operations {
            let mut span =
                telemetry.start_streaming_span("test-job", op, 50, Some(parent_ctx.clone()));
            span.set_success();
            drop(span);
        }
        drop(batch_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        // All spans (including batch) should share same trace_id
        for span in &spans {
            assert_eq!(
                span.span_context.trace_id(),
                batch_trace_id,
                "Span '{}' must share trace_id with parent batch",
                span.name
            );
        }
    }

    /// Test 9: Batch span is a root span (no parent) when no upstream context
    #[tokio::test]
    #[serial]
    async fn test_batch_span_is_root_without_upstream() {
        let telemetry = create_test_telemetry("batch-root-test").await;

        let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
        drop(batch_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let batch = spans
            .iter()
            .find(|s| s.name.contains("batch:"))
            .expect("Should find batch span");

        // Batch with no upstream context is a root span
        assert_eq!(
            batch.parent_span_id,
            SpanId::INVALID,
            "Batch span without upstream context should be root"
        );
    }

    /// Test 10: Batch span with upstream context is a child of upstream
    #[tokio::test]
    #[serial]
    async fn test_batch_span_child_of_upstream_context() {
        let telemetry = create_test_telemetry("batch-upstream-test").await;

        // Create a "upstream" batch span to get a valid SpanContext
        let upstream_span = telemetry.start_batch_span("upstream-job", 0, None, Vec::new());
        let upstream_ctx = upstream_span
            .span_context()
            .expect("upstream should have context");
        let upstream_trace_id = upstream_ctx.trace_id();
        let upstream_span_id = upstream_ctx.span_id();
        drop(upstream_span);

        // Create downstream batch span with upstream as parent
        let downstream_span =
            telemetry.start_batch_span("downstream-job", 1, Some(upstream_ctx), Vec::new());
        drop(downstream_span);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let spans = telemetry.collected_spans();

        let downstream = spans
            .iter()
            .find(|s| s.name.contains("batch:downstream"))
            .expect("Should find downstream batch span");

        assert_eq!(
            downstream.span_context.trace_id(),
            upstream_trace_id,
            "Downstream batch must share upstream trace_id"
        );
        assert_eq!(
            downstream.parent_span_id, upstream_span_id,
            "Downstream batch parent must be upstream span"
        );
    }
}
