//! Tests for observability per-record enrichment
//!
//! With the per-record sampling migration, batch span enrichment (query metadata,
//! record metadata) was removed. RecordSpan attributes are minimal by design.
//! Detailed query/record metadata is now exclusively in Prometheus metrics.

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
async fn test_record_span_has_job_name_attribute() {
    let telemetry = create_test_telemetry("record-span-enrichment").await;

    {
        let mut span = telemetry.start_record_span("test-job", None);
        span.set_output_count(5);
        span.set_success();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let spans = telemetry.collected_spans();
    assert!(!spans.is_empty(), "Should have collected at least one span");

    let record_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("process:test-job"))
        .expect("Should find record span with name process:test-job");

    let attrs: std::collections::HashMap<String, String> = record_span
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), format!("{}", kv.value)))
        .collect();

    assert_eq!(
        attrs.get("job.name").map(|v| v.as_str()),
        Some("test-job"),
        "Record span should have job.name attribute"
    );
    assert_eq!(
        attrs.get("record.output_count").map(|v| v.as_str()),
        Some("5"),
        "Record span should have output_count attribute"
    );
}
