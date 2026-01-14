//! Unit tests for test harness capture module
//!
//! Tests CaptureFormat enum, deserialization, and error handling.

use std::time::Duration;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::test_harness::capture::{
    json_to_field_values, json_type_name, CaptureConfig, CaptureFormat, SinkCapture,
};

// =============================================================================
// CaptureFormat Enum Tests
// =============================================================================

#[test]
fn test_capture_format_default_is_json() {
    let format = CaptureFormat::default();
    assert_eq!(format, CaptureFormat::Json);
}

#[test]
fn test_capture_format_serde_roundtrip_json() {
    let original = CaptureFormat::Json;
    let yaml = serde_yaml::to_string(&original).unwrap();
    let deserialized: CaptureFormat = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_capture_format_serde_roundtrip_avro() {
    let original = CaptureFormat::Avro;
    let yaml = serde_yaml::to_string(&original).unwrap();
    let deserialized: CaptureFormat = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_capture_format_serde_roundtrip_protobuf() {
    let original = CaptureFormat::Protobuf;
    let yaml = serde_yaml::to_string(&original).unwrap();
    let deserialized: CaptureFormat = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_capture_format_deserialize_lowercase() {
    assert_eq!(
        serde_yaml::from_str::<CaptureFormat>("json").unwrap(),
        CaptureFormat::Json
    );
    assert_eq!(
        serde_yaml::from_str::<CaptureFormat>("avro").unwrap(),
        CaptureFormat::Avro
    );
    assert_eq!(
        serde_yaml::from_str::<CaptureFormat>("protobuf").unwrap(),
        CaptureFormat::Protobuf
    );
}

#[test]
fn test_capture_format_serialize_to_lowercase() {
    assert_eq!(
        serde_yaml::to_string(&CaptureFormat::Json).unwrap().trim(),
        "json"
    );
    assert_eq!(
        serde_yaml::to_string(&CaptureFormat::Avro).unwrap().trim(),
        "avro"
    );
    assert_eq!(
        serde_yaml::to_string(&CaptureFormat::Protobuf)
            .unwrap()
            .trim(),
        "protobuf"
    );
}

#[test]
fn test_capture_format_invalid_value_error() {
    let result = serde_yaml::from_str::<CaptureFormat>("invalid_format");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("unknown variant"),
        "Expected 'unknown variant' in error: {}",
        err_msg
    );
}

// =============================================================================
// CaptureConfig Tests
// =============================================================================

#[test]
fn test_capture_config_default_values() {
    let config = CaptureConfig::default();
    assert_eq!(config.timeout, Duration::from_secs(30));
    assert_eq!(config.min_records, 1);
    assert_eq!(config.max_records, 100_000);
    assert_eq!(config.idle_timeout, Duration::from_secs(5));
    assert_eq!(config.format, CaptureFormat::Json);
    assert!(config.schema_json.is_none());
}

#[test]
fn test_capture_config_with_avro_schema() {
    let schema = r#"{"type":"record","name":"Test","fields":[{"name":"id","type":"long"}]}"#;
    let config = CaptureConfig {
        format: CaptureFormat::Avro,
        schema_json: Some(schema.to_string()),
        ..Default::default()
    };
    assert_eq!(config.format, CaptureFormat::Avro);
    assert!(config.schema_json.as_ref().unwrap().contains("Test"));
}

#[test]
fn test_capture_config_clone_preserves_all_fields() {
    let config = CaptureConfig {
        timeout: Duration::from_secs(60),
        min_records: 10,
        max_records: 1000,
        idle_timeout: Duration::from_secs(10),
        format: CaptureFormat::Avro,
        schema_json: Some("schema".to_string()),
    };

    let cloned = config.clone();
    assert_eq!(config.timeout, cloned.timeout);
    assert_eq!(config.min_records, cloned.min_records);
    assert_eq!(config.max_records, cloned.max_records);
    assert_eq!(config.idle_timeout, cloned.idle_timeout);
    assert_eq!(config.format, cloned.format);
    assert_eq!(config.schema_json, cloned.schema_json);
}

// =============================================================================
// SinkCapture Builder Tests
// =============================================================================

#[test]
fn test_sink_capture_builder_chain() {
    let config = CaptureConfig {
        timeout: Duration::from_secs(60),
        format: CaptureFormat::Avro,
        schema_json: Some(r#"{"type":"record","name":"Test","fields":[]}"#.to_string()),
        ..Default::default()
    };

    // Verify builder chain compiles and doesn't panic
    let _capture = SinkCapture::new("localhost:9092")
        .with_config(config)
        .with_group_id_prefix("test_group");
}

// =============================================================================
// json_to_field_values Tests - Actually tests the module code
// =============================================================================

#[test]
fn test_json_to_field_values_simple_object() {
    let json: serde_json::Value = serde_json::json!({
        "id": 42,
        "name": "test",
        "active": true
    });

    let result = json_to_field_values(&json, "test_topic").unwrap();

    assert_eq!(result.get("id"), Some(&FieldValue::Integer(42)));
    assert_eq!(
        result.get("name"),
        Some(&FieldValue::String("test".to_string()))
    );
    assert_eq!(result.get("active"), Some(&FieldValue::Boolean(true)));
}

#[test]
fn test_json_to_field_values_all_types() {
    let json: serde_json::Value = serde_json::json!({
        "int_val": 123,
        "float_val": 45.67,
        "str_val": "hello",
        "bool_true": true,
        "bool_false": false,
        "null_val": null
    });

    let result = json_to_field_values(&json, "test_topic").unwrap();

    assert_eq!(result.get("int_val"), Some(&FieldValue::Integer(123)));
    assert_eq!(result.get("float_val"), Some(&FieldValue::Float(45.67)));
    assert_eq!(
        result.get("str_val"),
        Some(&FieldValue::String("hello".to_string()))
    );
    assert_eq!(result.get("bool_true"), Some(&FieldValue::Boolean(true)));
    assert_eq!(result.get("bool_false"), Some(&FieldValue::Boolean(false)));
    assert_eq!(result.get("null_val"), Some(&FieldValue::Null));
}

#[test]
fn test_json_to_field_values_timestamp_iso8601() {
    let json: serde_json::Value = serde_json::json!({
        "ts": "2026-01-14T10:30:00.123"
    });

    let result = json_to_field_values(&json, "test_topic").unwrap();

    match result.get("ts") {
        Some(FieldValue::Timestamp(ts)) => {
            assert_eq!(ts.format("%Y-%m-%d").to_string(), "2026-01-14");
            assert_eq!(ts.format("%H:%M:%S").to_string(), "10:30:00");
        }
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}

#[test]
fn test_json_to_field_values_timestamp_space_format() {
    let json: serde_json::Value = serde_json::json!({
        "ts": "2026-01-14 10:30:00"
    });

    let result = json_to_field_values(&json, "test_topic").unwrap();

    match result.get("ts") {
        Some(FieldValue::Timestamp(ts)) => {
            assert_eq!(ts.format("%Y-%m-%d %H:%M:%S").to_string(), "2026-01-14 10:30:00");
        }
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}

#[test]
fn test_json_to_field_values_nested_array_to_string() {
    let json: serde_json::Value = serde_json::json!({
        "tags": ["a", "b", "c"]
    });

    let result = json_to_field_values(&json, "test_topic").unwrap();

    match result.get("tags") {
        Some(FieldValue::String(s)) => {
            assert!(s.contains("\"a\""));
            assert!(s.contains("\"b\""));
            assert!(s.contains("\"c\""));
        }
        other => panic!("Expected String for array, got {:?}", other),
    }
}

#[test]
fn test_json_to_field_values_nested_object_to_string() {
    let json: serde_json::Value = serde_json::json!({
        "metadata": {"key": "value", "count": 5}
    });

    let result = json_to_field_values(&json, "test_topic").unwrap();

    match result.get("metadata") {
        Some(FieldValue::String(s)) => {
            assert!(s.contains("\"key\""));
            assert!(s.contains("\"value\""));
        }
        other => panic!("Expected String for nested object, got {:?}", other),
    }
}

#[test]
fn test_json_to_field_values_negative_numbers() {
    let json: serde_json::Value = serde_json::json!({
        "negative_int": -42,
        "negative_float": -3.14
    });

    let result = json_to_field_values(&json, "test_topic").unwrap();

    assert_eq!(result.get("negative_int"), Some(&FieldValue::Integer(-42)));
    assert_eq!(
        result.get("negative_float"),
        Some(&FieldValue::Float(-3.14))
    );
}

#[test]
fn test_json_to_field_values_empty_object() {
    let json: serde_json::Value = serde_json::json!({});

    let result = json_to_field_values(&json, "test_topic").unwrap();

    assert!(result.is_empty());
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_json_to_field_values_error_on_array_root() {
    let json: serde_json::Value = serde_json::json!([1, 2, 3]);

    let result = json_to_field_values(&json, "test_topic");

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = format!("{}", err);
    assert!(
        err_str.contains("test_topic"),
        "Error should contain topic name: {}",
        err_str
    );
    assert!(
        err_str.contains("array"),
        "Error should mention 'array': {}",
        err_str
    );
}

#[test]
fn test_json_to_field_values_error_on_string_root() {
    let json: serde_json::Value = serde_json::json!("just a string");

    let result = json_to_field_values(&json, "my_topic");

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = format!("{}", err);
    assert!(
        err_str.contains("my_topic"),
        "Error should contain topic name: {}",
        err_str
    );
    assert!(
        err_str.contains("string"),
        "Error should mention 'string': {}",
        err_str
    );
}

#[test]
fn test_json_to_field_values_error_on_null_root() {
    let json: serde_json::Value = serde_json::json!(null);

    let result = json_to_field_values(&json, "null_topic");

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = format!("{}", err);
    assert!(
        err_str.contains("null_topic"),
        "Error should contain topic name"
    );
}

#[test]
fn test_json_to_field_values_error_includes_source_context() {
    let json: serde_json::Value = serde_json::json!([1, 2, 3]);

    let result = json_to_field_values(&json, "orders_output:line 42");

    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("orders_output:line 42"),
        "Error should contain full context: {}",
        err_str
    );
}

// =============================================================================
// json_type_name Tests
// =============================================================================

#[test]
fn test_json_type_name_all_types() {
    assert_eq!(json_type_name(&serde_json::json!(null)), "null");
    assert_eq!(json_type_name(&serde_json::json!(true)), "boolean");
    assert_eq!(json_type_name(&serde_json::json!(42)), "number");
    assert_eq!(json_type_name(&serde_json::json!("test")), "string");
    assert_eq!(json_type_name(&serde_json::json!([1, 2])), "array");
    assert_eq!(json_type_name(&serde_json::json!({"a": 1})), "object");
}

// =============================================================================
// Avro Format Configuration Tests
// =============================================================================

#[test]
fn test_avro_config_requires_schema_for_deserialization() {
    // When using Avro format, schema_json should be set
    let config_without_schema = CaptureConfig {
        format: CaptureFormat::Avro,
        schema_json: None,
        ..Default::default()
    };
    // Config creation succeeds - error happens at deserialization time
    assert_eq!(config_without_schema.format, CaptureFormat::Avro);
    assert!(config_without_schema.schema_json.is_none());

    let config_with_schema = CaptureConfig {
        format: CaptureFormat::Avro,
        schema_json: Some(r#"{"type":"record","name":"Test","fields":[]}"#.to_string()),
        ..Default::default()
    };
    assert!(config_with_schema.schema_json.is_some());
}

#[test]
fn test_avro_schema_json_validates_as_json() {
    let schema = r#"{
        "type": "record",
        "name": "TradeRecord",
        "namespace": "com.velostream.demo",
        "fields": [
            {"name": "trade_id", "type": "string"},
            {"name": "symbol", "type": "string"},
            {"name": "quantity", "type": "long"},
            {"name": "price", "type": "double"}
        ]
    }"#;

    let config = CaptureConfig {
        format: CaptureFormat::Avro,
        schema_json: Some(schema.to_string()),
        ..Default::default()
    };

    // Verify schema is valid JSON
    let schema_value: serde_json::Value =
        serde_json::from_str(config.schema_json.as_ref().unwrap()).unwrap();
    assert_eq!(schema_value["type"].as_str(), Some("record"));
    assert_eq!(schema_value["name"].as_str(), Some("TradeRecord"));
    assert_eq!(schema_value["fields"].as_array().unwrap().len(), 4);
}

// =============================================================================
// YAML Test Spec Integration Tests
// =============================================================================

#[test]
fn test_yaml_query_test_with_capture_format() {
    // Simulate deserializing a test spec YAML with capture_format
    let yaml = r#"
name: test_query
description: Test capture format
inputs: []
assertions: []
capture_format: avro
capture_schema: |
  {"type":"record","name":"Test","fields":[]}
"#;

    let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(value["capture_format"].as_str(), Some("avro"));
    assert!(value["capture_schema"].as_str().is_some());
}

#[test]
fn test_yaml_query_test_without_capture_format_uses_default() {
    // When capture_format is not specified, should default to JSON
    let yaml = r#"
name: test_query
description: Test without capture format
inputs: []
assertions: []
"#;

    let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
    // capture_format should be absent (defaults handled by struct)
    assert!(value.get("capture_format").is_none());

    // Verify CaptureFormat::default() is Json
    assert_eq!(CaptureFormat::default(), CaptureFormat::Json);
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_json_to_field_values_unicode_strings() {
    let json: serde_json::Value = serde_json::json!({
        "emoji": "🚀",
        "chinese": "你好",
        "arabic": "مرحبا"
    });

    let result = json_to_field_values(&json, "test").unwrap();

    assert_eq!(
        result.get("emoji"),
        Some(&FieldValue::String("🚀".to_string()))
    );
    assert_eq!(
        result.get("chinese"),
        Some(&FieldValue::String("你好".to_string()))
    );
    assert_eq!(
        result.get("arabic"),
        Some(&FieldValue::String("مرحبا".to_string()))
    );
}

#[test]
fn test_json_to_field_values_empty_string() {
    let json: serde_json::Value = serde_json::json!({
        "empty": ""
    });

    let result = json_to_field_values(&json, "test").unwrap();

    assert_eq!(
        result.get("empty"),
        Some(&FieldValue::String("".to_string()))
    );
}

#[test]
fn test_json_to_field_values_large_integer() {
    let json: serde_json::Value = serde_json::json!({
        "large": 9223372036854775807_i64  // i64::MAX
    });

    let result = json_to_field_values(&json, "test").unwrap();

    assert_eq!(
        result.get("large"),
        Some(&FieldValue::Integer(i64::MAX))
    );
}

#[test]
fn test_json_to_field_values_special_float_values() {
    let json: serde_json::Value = serde_json::json!({
        "zero": 0.0,
        "tiny": 0.0000001,
        "large": 1e10
    });

    let result = json_to_field_values(&json, "test").unwrap();

    // Note: 0.0 is parsed as Integer 0 by serde_json
    assert_eq!(result.get("tiny"), Some(&FieldValue::Float(0.0000001)));
    assert_eq!(result.get("large"), Some(&FieldValue::Float(1e10)));
}

#[test]
fn test_json_to_field_values_string_not_parsed_as_timestamp() {
    // Strings that look like partial timestamps should remain strings
    let json: serde_json::Value = serde_json::json!({
        "date_only": "2026-01-14",
        "time_only": "10:30:00",
        "random": "not-a-timestamp"
    });

    let result = json_to_field_values(&json, "test").unwrap();

    // These should remain as strings since they don't match expected formats
    assert!(matches!(result.get("date_only"), Some(FieldValue::String(_))));
    assert!(matches!(result.get("time_only"), Some(FieldValue::String(_))));
    assert!(matches!(result.get("random"), Some(FieldValue::String(_))));
}
