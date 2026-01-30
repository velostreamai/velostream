//! Unit tests for executor internal functions
//!
//! Tests for infer_file_format, field_values_to_json, DEFAULT_JOB_VERSION,
//! parse_with_properties, extract_sources_and_sinks, extract_stream_name,
//! CapturedOutput, and dependency tracking.
//!
//! Moved from inline #[cfg(test)] in executor.rs to follow project convention.

use std::collections::{HashMap, HashSet};
use velostream::velostream::sql::execution::FieldValue;
use velostream::velostream::test_harness::executor::{
    CapturedOutput, DEFAULT_JOB_VERSION, extract_sources_and_sinks, extract_stream_name,
    field_values_to_json, infer_file_format, parse_with_properties,
};
use velostream::velostream::test_harness::spec::{FileFormat, QueryTest};

#[test]
fn test_infer_file_format_csv() {
    assert!(matches!(infer_file_format("data.csv"), FileFormat::Csv));
    assert!(matches!(infer_file_format("data.CSV"), FileFormat::Csv));
    assert!(matches!(
        infer_file_format("/path/to/data.csv"),
        FileFormat::Csv
    ));
}

#[test]
fn test_infer_file_format_json() {
    assert!(matches!(infer_file_format("data.json"), FileFormat::Json));
    assert!(matches!(infer_file_format("data.JSON"), FileFormat::Json));
}

#[test]
fn test_infer_file_format_jsonl() {
    assert!(matches!(
        infer_file_format("data.jsonl"),
        FileFormat::JsonLines
    ));
    assert!(matches!(
        infer_file_format("data.ndjson"),
        FileFormat::JsonLines
    ));
    assert!(matches!(
        infer_file_format("data.NDJSON"),
        FileFormat::JsonLines
    ));
}

#[test]
fn test_infer_file_format_unknown() {
    // Unknown extensions default to JSON Lines
    assert!(matches!(
        infer_file_format("data.txt"),
        FileFormat::JsonLines
    ));
    assert!(matches!(
        infer_file_format("data.parquet"),
        FileFormat::JsonLines
    ));
}

#[test]
fn test_extract_stream_name() {
    assert_eq!(
        extract_stream_name("CREATE STREAM my_stream AS SELECT * FROM source"),
        Some("my_stream".to_string())
    );

    assert_eq!(
        extract_stream_name("CREATE STREAM enriched_data AS SELECT a, b FROM source"),
        Some("enriched_data".to_string())
    );

    assert_eq!(extract_stream_name("SELECT * FROM source"), None);
}

#[test]
fn test_field_values_to_json() {
    let mut record = HashMap::new();
    record.insert("id".to_string(), FieldValue::Integer(42));
    record.insert("name".to_string(), FieldValue::String("test".to_string()));
    record.insert("active".to_string(), FieldValue::Boolean(true));

    let json = field_values_to_json(&record);

    assert!(json.is_object());
    assert_eq!(json["id"], 42);
    assert_eq!(json["name"], "test");
    assert_eq!(json["active"], true);
}

#[test]
fn test_default_job_version_constant() {
    assert_eq!(DEFAULT_JOB_VERSION, "1.0.0");
    assert!(DEFAULT_JOB_VERSION.contains('.'));
}

#[test]
fn test_parse_with_properties_basic() {
    let sql = "CREATE STREAM test AS SELECT * FROM source WITH ('topic' = 'my_topic')";
    let props = parse_with_properties(sql);
    assert_eq!(props.get("topic"), Some(&"my_topic".to_string()));
}

#[test]
fn test_parse_with_properties_multiple() {
    let sql = r#"CREATE STREAM test AS SELECT * FROM source
            WITH ('topic' = 'my_topic', 'format' = 'json', 'key' = 'id')"#;
    let props = parse_with_properties(sql);
    assert_eq!(props.get("topic"), Some(&"my_topic".to_string()));
    assert_eq!(props.get("format"), Some(&"json".to_string()));
    assert_eq!(props.get("key"), Some(&"id".to_string()));
}

#[test]
fn test_parse_with_properties_empty() {
    let sql = "SELECT * FROM source";
    let props = parse_with_properties(sql);
    assert!(props.is_empty());
}

#[test]
fn test_extract_sources_and_sinks_kafka_source() {
    let mut props = HashMap::new();
    props.insert("input.type".to_string(), "kafka_source".to_string());
    props.insert("input.topic".to_string(), "input_topic".to_string());

    let (sources, sinks) = extract_sources_and_sinks(&props);

    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].0, "input");
    assert_eq!(sources[0].1, Some("input_topic".to_string()));
    assert!(sinks.is_empty());
}

#[test]
fn test_extract_sources_and_sinks_kafka_sink() {
    let mut props = HashMap::new();
    props.insert("output.type".to_string(), "kafka_sink".to_string());
    props.insert("output.topic".to_string(), "output_topic".to_string());

    let (sources, sinks) = extract_sources_and_sinks(&props);

    assert!(sources.is_empty());
    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].0, "output");
    assert_eq!(sinks[0].1, Some("output_topic".to_string()));
}

#[test]
fn test_extract_table_name_ctas() {
    assert_eq!(
        extract_stream_name("CREATE TABLE my_table AS SELECT * FROM source"),
        Some("my_table".to_string())
    );

    assert_eq!(
        extract_stream_name("CREATE TABLE reference_data AS SELECT id, name FROM source"),
        Some("reference_data".to_string())
    );
}

#[test]
fn test_captured_output_location_with_topic() {
    let output = CapturedOutput {
        query_name: "test".to_string(),
        sink_name: "sink".to_string(),
        topic: Some("my_topic".to_string()),
        records: Vec::new(),
        execution_time_ms: 100,
        warnings: Vec::new(),
        memory_peak_bytes: None,
        memory_growth_bytes: None,
    };
    assert_eq!(output.location(), "topic 'my_topic'");
}

#[test]
fn test_captured_output_location_without_topic() {
    let output = CapturedOutput {
        query_name: "test".to_string(),
        sink_name: "file_sink".to_string(),
        topic: None,
        records: Vec::new(),
        execution_time_ms: 100,
        warnings: Vec::new(),
        memory_peak_bytes: None,
        memory_growth_bytes: None,
    };
    assert_eq!(output.location(), "sink 'file_sink'");
}

fn create_test_query_with_deps(name: &str, deps: Vec<&str>) -> QueryTest {
    QueryTest {
        name: name.to_string(),
        description: None,
        skip: false,
        dependencies: deps.iter().map(|s| s.to_string()).collect(),
        inputs: Vec::new(),
        output: None,
        outputs: Vec::new(),
        assertions: Vec::new(),
        timeout_ms: None,
        capture_format: Default::default(),
        capture_schema: None,
        metric_assertions: Vec::new(),
    }
}

#[test]
fn test_deployed_dependencies_tracking() {
    let mut deployed = HashSet::new();
    assert!(deployed.insert("dep_a".to_string()));
    assert!(!deployed.insert("dep_a".to_string()));
    assert!(deployed.insert("dep_b".to_string()));
    assert!(deployed.contains("dep_a"));
    assert!(deployed.contains("dep_b"));
    assert!(!deployed.contains("dep_c"));
}

#[test]
fn test_query_with_empty_dependencies_no_error() {
    let query = create_test_query_with_deps("simple_query", vec![]);
    assert!(query.dependencies.is_empty());
}
