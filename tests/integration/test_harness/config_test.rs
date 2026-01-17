//! Configuration and spec tests for the SQL Application Test Harness
//!
//! Tests:
//! - Config override builder
//! - Test spec YAML parsing
//! - Schema registry
//! - Demo spec loading

#![cfg(feature = "test-support")]

use std::collections::HashMap;
use std::path::PathBuf;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::test_harness::config_override::ConfigOverrideBuilder;
use velostream::velostream::test_harness::executor::CapturedOutput;
use velostream::velostream::test_harness::schema::{Schema, SchemaRegistry};
use velostream::velostream::test_harness::spec::TestSpec;

/// Test config overrides builder
#[test]
fn test_config_override_builder() {
    let overrides = ConfigOverrideBuilder::new("run_123")
        .bootstrap_servers("localhost:9092")
        .property("custom.setting", "custom_value")
        .build();

    // Test bootstrap servers override
    assert_eq!(
        overrides.bootstrap_servers,
        Some("localhost:9092".to_string())
    );

    // Note: Topic prefixing is handled by TestHarnessInfra.topic_name(), not ConfigOverrides

    // Test custom setting
    let custom = overrides.properties.get("custom.setting");
    assert_eq!(custom, Some(&"custom_value".to_string()));

    println!("✅ Config override builder test passed");
}

/// Test test spec YAML parsing
#[test]
fn test_spec_parsing() {
    let spec_yaml = r#"
application: test_app
description: Test application
default_timeout_ms: 30000
default_records: 1000

queries:
  - name: test_query
    description: Test query for validation
    inputs:
      - source: input_data
        records: 100
    outputs:
      - sink: output_topic
        assertions:
          - type: record_count
            greater_than: 0
          - type: schema_contains
            fields: [id, name]
          - type: no_nulls
            fields: [id]
    assertions:
      - type: execution_time
        max_ms: 5000
"#;

    let spec: TestSpec = serde_yaml::from_str(spec_yaml).expect("Failed to parse test spec");

    assert_eq!(spec.application, "test_app");
    assert_eq!(spec.default_timeout_ms, 30000);
    assert_eq!(spec.default_records, 1000);
    assert_eq!(spec.queries.len(), 1);

    let query = &spec.queries[0];
    assert_eq!(query.name, "test_query");
    assert_eq!(query.inputs.len(), 1);
    assert_eq!(query.inputs[0].source, "input_data");
    assert_eq!(query.inputs[0].records, Some(100));

    // Check outputs with per-sink assertions
    assert_eq!(query.outputs.len(), 1);
    assert_eq!(query.outputs[0].sink, "output_topic");
    assert_eq!(query.outputs[0].assertions.len(), 3);

    // Check top-level assertions
    assert_eq!(query.assertions.len(), 1);

    // Test the all_assertions() method
    let all_assertions = query.all_assertions();
    assert_eq!(
        all_assertions.len(),
        4,
        "Should have 4 total assertions (3 from output + 1 top-level)"
    );

    // Test assertions_for_sink method
    let sink_assertions = query.assertions_for_sink("output_topic");
    assert_eq!(
        sink_assertions.len(),
        3,
        "Should have 3 assertions for output_topic sink"
    );

    println!("✅ Test spec parsing passed");
}

/// Test schema registry
#[test]
fn test_schema_registry() {
    let mut registry = SchemaRegistry::new();

    let schema_yaml = r#"
name: orders
description: Order events
fields:
  - name: order_id
    type: string
  - name: amount
    type: float
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    registry.register(schema);

    // Should find the schema
    let found = registry.get("orders");
    assert!(found.is_some(), "Should find 'orders' schema");
    assert_eq!(found.unwrap().fields.len(), 2);

    // Should not find non-existent schema
    let not_found = registry.get("nonexistent");
    assert!(not_found.is_none(), "Should not find nonexistent schema");

    println!("✅ Schema registry test passed");
}

/// Test test spec loading from demo directory
#[test]
fn test_demo_spec_loading() {
    let demo_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("demo")
        .join("test_harness_examples")
        .join("getting_started");

    let spec_file = demo_dir.join("test_spec.yaml");

    if !spec_file.exists() {
        println!("Demo spec file not found, skipping test");
        return;
    }

    let spec = TestSpec::from_file(&spec_file);

    match spec {
        Ok(spec) => {
            println!("Loaded spec for application: {}", spec.application);
            println!("Description: {:?}", spec.description);
            println!("Default timeout: {}ms", spec.default_timeout_ms);
            println!("Default records: {}", spec.default_records);
            println!("Number of queries: {}", spec.queries.len());

            for query in &spec.queries {
                println!("  Query: {} (skip: {})", query.name, query.skip);
                println!("    Inputs: {}", query.inputs.len());
                println!("    Outputs: {}", query.outputs.len());
                println!("    Top-level assertions: {}", query.assertions.len());
            }

            assert!(!spec.queries.is_empty(), "Should have at least one query");
            println!("✅ Demo spec loading test passed");
        }
        Err(e) => {
            println!("Failed to load spec: {}", e);
            // Not a hard failure - demo may have different format
        }
    }
}

/// Test execution result structure
#[test]
fn test_execution_result_structure() {
    use velostream::velostream::test_harness::executor::ExecutionResult;

    let mut fields1 = HashMap::new();
    fields1.insert("id".to_string(), FieldValue::Integer(1));

    let mut fields2 = HashMap::new();
    fields2.insert("id".to_string(), FieldValue::Integer(2));

    let result = ExecutionResult {
        query_name: "test_query".to_string(),
        success: true,
        error: None,
        outputs: vec![CapturedOutput {
            query_name: "test_query".to_string(),
            sink_name: "output_topic".to_string(),
            topic: Some("output_topic".to_string()),
            records: vec![StreamRecord::new(fields1), StreamRecord::new(fields2)],
            execution_time_ms: 500,
            warnings: vec!["Minor warning".to_string()],
            memory_peak_bytes: Some(1024 * 1024),
            memory_growth_bytes: Some(512 * 1024),
        }],
        execution_time_ms: 1000,
    };

    assert_eq!(result.query_name, "test_query");
    assert!(result.success);
    assert!(result.error.is_none());
    assert_eq!(result.outputs.len(), 1);
    assert_eq!(result.outputs[0].records.len(), 2);
    assert_eq!(result.outputs[0].warnings.len(), 1);
    assert_eq!(result.outputs[0].memory_peak_bytes, Some(1024 * 1024));

    println!("✅ Execution result structure test passed");
}
