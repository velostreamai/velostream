//! Integration tests for span enrichment with QuerySpanMetadata
//!
//! Tests that BatchSpan and QuerySpan correctly receive sql.* attributes
//! when set_query_metadata() is called.

use serial_test::serial;
use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::observability::query_metadata::QuerySpanMetadata;
use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::execution::StreamRecord;
use velostream::velostream::sql::execution::config::TracingConfig;
use velostream::velostream::sql::execution::types::FieldValue;

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
async fn test_batch_span_query_metadata_attributes() {
    let telemetry = create_test_telemetry("span-enrichment-batch").await;

    // Build a query with join + window + group by
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("orders".to_string()),
        from_alias: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::Inner,
            right_source: StreamSource::Stream("customers".to_string()),
            right_alias: None,
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("orders.cust_id".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Column("customers.id".to_string())),
            },
            window: None,
        }]),
        where_clause: None,
        group_by: Some(vec![Expr::Column("symbol".to_string())]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: None,
        }),
        order_by: None,
        limit: None,
        emit_mode: Some(EmitMode::Changes),
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    // Create batch span, enrich, and drop to flush
    {
        let mut span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
        span.set_query_metadata(&metadata);
        span.set_success();
        // span dropped here, flushed to collector
    }

    // Wait for span to be collected
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let spans = telemetry.collected_spans();
    assert!(!spans.is_empty(), "Should have collected at least one span");

    let batch_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("batch:"))
        .expect("Should find batch span");

    // Verify sql.* attributes are present
    let attrs: std::collections::HashMap<String, String> = batch_span
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), format!("{}", kv.value)))
        .collect();

    assert_eq!(
        attrs.get("sql.has_join").map(|v| v.as_str()),
        Some("true"),
        "sql.has_join should be true"
    );
    assert_eq!(
        attrs.get("sql.join_type").map(|v| v.as_str()),
        Some("INNER"),
        "sql.join_type should be INNER"
    );
    assert!(
        attrs.contains_key("sql.join_sources"),
        "sql.join_sources should be present"
    );
    assert_eq!(
        attrs.get("sql.has_window").map(|v| v.as_str()),
        Some("true"),
        "sql.has_window should be true"
    );
    assert_eq!(
        attrs.get("sql.window_type").map(|v| v.as_str()),
        Some("tumbling"),
        "sql.window_type should be tumbling"
    );
    assert!(
        attrs.contains_key("sql.window_size_ms"),
        "sql.window_size_ms should be present"
    );
    assert_eq!(
        attrs.get("sql.has_group_by").map(|v| v.as_str()),
        Some("true"),
        "sql.has_group_by should be true"
    );
    assert_eq!(
        attrs.get("sql.group_by_fields").map(|v| v.as_str()),
        Some("symbol"),
        "sql.group_by_fields should be symbol"
    );
    assert_eq!(
        attrs.get("sql.emit_mode").map(|v| v.as_str()),
        Some("changes"),
        "sql.emit_mode should be changes"
    );
}

#[tokio::test]
#[serial]
async fn test_batch_span_kafka_record_metadata() {
    let telemetry = create_test_telemetry("span-enrichment-kafka").await;

    // Create batch span and set record metadata
    {
        let mut span = telemetry.start_batch_span("test-kafka-job", 1, None, Vec::new());
        span.set_input_topic("trades-topic");
        span.set_input_partition(3);
        span.set_message_key("AAPL");
        span.set_success();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let spans = telemetry.collected_spans();
    let batch_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("batch:"))
        .expect("Should find batch span");

    let attrs: std::collections::HashMap<String, String> = batch_span
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), format!("{}", kv.value)))
        .collect();

    assert_eq!(
        attrs.get("kafka.input_topic").map(|v| v.as_str()),
        Some("trades-topic"),
    );
    assert_eq!(
        attrs.get("kafka.input_partition").map(|v| v.as_str()),
        Some("3"),
    );
    assert_eq!(
        attrs.get("kafka.message_key").map(|v| v.as_str()),
        Some("AAPL"),
    );
}

#[tokio::test]
#[serial]
async fn test_query_span_metadata_attributes() {
    let telemetry = create_test_telemetry("span-enrichment-query").await;

    let metadata = QuerySpanMetadata {
        has_join: false,
        join_type: None,
        join_sources: None,
        join_key_fields: None,
        has_window: true,
        window_type: Some("session".to_string()),
        window_size_ms: Some(30_000),
        has_group_by: false,
        group_by_fields: None,
        emit_mode: Some("final".to_string()),
    };

    // Create query span, enrich, and drop
    {
        let mut span = telemetry.start_sql_query_span("test-job", "SELECT * FROM t", "test", None);
        span.set_query_metadata(&metadata);
        span.set_success();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let spans = telemetry.collected_spans();
    let query_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("sql_query:"))
        .expect("Should find SQL query span");

    let attrs: std::collections::HashMap<String, String> = query_span
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), format!("{}", kv.value)))
        .collect();

    assert_eq!(
        attrs.get("sql.has_window").map(|v| v.as_str()),
        Some("true"),
    );
    assert_eq!(
        attrs.get("sql.window_type").map(|v| v.as_str()),
        Some("session"),
    );
    assert_eq!(
        attrs.get("sql.emit_mode").map(|v| v.as_str()),
        Some("final"),
    );
    assert_eq!(attrs.get("sql.has_join").map(|v| v.as_str()), Some("false"),);
}

#[tokio::test]
#[serial]
async fn test_empty_metadata_sets_false_attributes() {
    let telemetry = create_test_telemetry("span-enrichment-empty").await;

    let metadata = QuerySpanMetadata::empty();

    {
        let mut span = telemetry.start_batch_span("empty-metadata-job", 1, None, Vec::new());
        span.set_query_metadata(&metadata);
        span.set_success();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let spans = telemetry.collected_spans();
    let batch_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("batch:"))
        .expect("Should find batch span");

    let attrs: std::collections::HashMap<String, String> = batch_span
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), format!("{}", kv.value)))
        .collect();

    assert_eq!(attrs.get("sql.has_join").map(|v| v.as_str()), Some("false"));
    assert_eq!(
        attrs.get("sql.has_window").map(|v| v.as_str()),
        Some("false")
    );
    assert_eq!(
        attrs.get("sql.has_group_by").map(|v| v.as_str()),
        Some("false")
    );
    // Optional fields should NOT be present
    assert!(!attrs.contains_key("sql.join_type"));
    assert!(!attrs.contains_key("sql.window_type"));
    assert!(!attrs.contains_key("sql.emit_mode"));
}

#[tokio::test]
#[serial]
async fn test_enrich_batch_span_with_record_metadata_empty_batch() {
    let telemetry = create_test_telemetry("span-enrichment-empty-batch").await;

    // Enrich with empty batch — should not panic, kafka.* attributes absent
    {
        let span = telemetry.start_batch_span("empty-batch-job", 1, None, Vec::new());
        let empty_records: Vec<StreamRecord> = vec![];
        let mut opt_span = Some(span);
        ObservabilityHelper::enrich_batch_span_with_record_metadata(&mut opt_span, &empty_records);
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let spans = telemetry.collected_spans();
    let batch_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("batch:"))
        .expect("Should find batch span");

    let attrs: HashMap<String, String> = batch_span
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), format!("{}", kv.value)))
        .collect();

    // No kafka.* attributes when batch is empty
    assert!(
        !attrs.contains_key("kafka.input_topic"),
        "No topic when batch is empty"
    );
    assert!(
        !attrs.contains_key("kafka.message_key"),
        "No key when batch is empty"
    );
}

#[tokio::test]
#[serial]
async fn test_enrich_batch_span_with_non_string_topic_and_key() {
    let telemetry = create_test_telemetry("span-enrichment-non-string").await;

    // Create a record with non-String FieldValue for topic and key
    let mut record = StreamRecord::new(HashMap::new());
    record.topic = Some(FieldValue::Integer(42)); // Not a String
    record.key = Some(FieldValue::Boolean(true)); // Not a String
    record.partition = 7;

    {
        let mut span = Some(telemetry.start_batch_span("non-string-job", 1, None, Vec::new()));
        ObservabilityHelper::enrich_batch_span_with_record_metadata(&mut span, &[record]);
        if let Some(s) = span.as_mut() {
            s.set_success();
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let spans = telemetry.collected_spans();
    let batch_span = spans
        .iter()
        .find(|s| s.name.as_ref().contains("batch:"))
        .expect("Should find batch span");

    let attrs: HashMap<String, String> = batch_span
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), format!("{}", kv.value)))
        .collect();

    // Non-String topic/key are silently ignored
    assert!(
        !attrs.contains_key("kafka.input_topic"),
        "Integer topic should be skipped"
    );
    assert!(
        !attrs.contains_key("kafka.message_key"),
        "Boolean key should be skipped"
    );
    // Partition is always set (i32, not wrapped in FieldValue)
    assert_eq!(
        attrs.get("kafka.input_partition").map(|v| v.as_str()),
        Some("7"),
        "Partition should still be set"
    );
}

#[tokio::test]
#[serial]
async fn test_enrich_batch_span_with_none_span_is_noop() {
    // Calling enrichment with None span should not panic
    let empty_records: Vec<StreamRecord> = vec![];
    let metadata = QuerySpanMetadata::empty();

    ObservabilityHelper::enrich_batch_span_with_query_metadata(&mut None, &metadata);
    ObservabilityHelper::enrich_batch_span_with_record_metadata(&mut None, &empty_records);
    // If we get here without panicking, the test passes
}
