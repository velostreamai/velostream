//! Assertion framework tests for the SQL Application Test Harness
//!
//! Tests assertion types:
//! - Record count assertions
//! - Schema contains assertions
//! - No nulls assertions
//! - Assertion context with templates

#![cfg(feature = "test-support")]

use std::collections::HashMap;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::test_harness::assertions::{AssertionContext, AssertionRunner};
use velostream::velostream::test_harness::executor::CapturedOutput;
use velostream::velostream::test_harness::spec::{
    AssertionConfig, NoNullsAssertion, RecordCountAssertion, SchemaContainsAssertion,
};

/// Test assertion runner with record count assertions
#[test]
fn test_assertion_runner_record_count() {
    let mut records = Vec::new();
    for i in 0..50 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "name".to_string(),
            FieldValue::String(format!("item_{}", i)),
        );
        record.insert("value".to_string(), FieldValue::Float(i as f64 * 1.5));
        records.push(record);
    }

    let output = CapturedOutput {
        query_name: "test_query".to_string(),
        sink_name: "test_output".to_string(),
        topic: Some("test_output".to_string()),
        records,
        execution_time_ms: 1000,
        warnings: Vec::new(),
        memory_peak_bytes: None,
        memory_growth_bytes: None,
    };

    let runner = AssertionRunner::new();

    let assertions = vec![
        // Should pass: greater_than 0
        AssertionConfig::RecordCount(RecordCountAssertion {
            equals: None,
            greater_than: Some(0),
            less_than: None,
            between: None,
            expression: None,
        }),
        // Should pass: equals 50
        AssertionConfig::RecordCount(RecordCountAssertion {
            equals: Some(50),
            greater_than: None,
            less_than: None,
            between: None,
            expression: None,
        }),
        // Should pass: less_than 100
        AssertionConfig::RecordCount(RecordCountAssertion {
            equals: None,
            greater_than: None,
            less_than: Some(100),
            between: None,
            expression: None,
        }),
        // Should pass: between 40 and 60
        AssertionConfig::RecordCount(RecordCountAssertion {
            equals: None,
            greater_than: None,
            less_than: None,
            between: Some((40, 60)),
            expression: None,
        }),
    ];

    let results = runner.run_assertions(&output, &assertions);

    for result in &results {
        assert!(
            result.passed,
            "Assertion {} should pass: {}",
            result.assertion_type, result.message
        );
    }

    println!(
        "✅ Record count assertion test passed ({} assertions)",
        results.len()
    );
}

/// Test schema_contains assertion
#[test]
fn test_assertion_runner_schema_contains() {
    let mut records = Vec::new();
    let mut record = HashMap::new();
    record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    record.insert("price".to_string(), FieldValue::Float(150.0));
    record.insert("volume".to_string(), FieldValue::Integer(1000));
    records.push(record);

    let output = CapturedOutput {
        query_name: "test_query".to_string(),
        sink_name: "test_output".to_string(),
        topic: Some("test_output".to_string()),
        records,
        execution_time_ms: 100,
        warnings: Vec::new(),
        memory_peak_bytes: None,
        memory_growth_bytes: None,
    };

    let runner = AssertionRunner::new();

    // Test schema contains - should pass
    let pass_assertion = AssertionConfig::SchemaContains(SchemaContainsAssertion {
        fields: vec![
            "symbol".to_string(),
            "price".to_string(),
            "volume".to_string(),
        ],
    });

    let results = runner.run_assertions(&output, &[pass_assertion]);
    assert!(
        results[0].passed,
        "Schema contains should pass: {}",
        results[0].message
    );

    // Test schema contains - should fail (missing field)
    let fail_assertion = AssertionConfig::SchemaContains(SchemaContainsAssertion {
        fields: vec!["symbol".to_string(), "nonexistent_field".to_string()],
    });

    let results = runner.run_assertions(&output, &[fail_assertion]);
    assert!(
        !results[0].passed,
        "Schema contains should fail for missing field"
    );

    println!("✅ Schema contains assertion test passed");
}

/// Test no_nulls assertion
#[test]
fn test_assertion_runner_no_nulls() {
    let mut records = Vec::new();

    let mut record1 = HashMap::new();
    record1.insert("id".to_string(), FieldValue::Integer(1));
    record1.insert("name".to_string(), FieldValue::String("test".to_string()));
    records.push(record1);

    let mut record2 = HashMap::new();
    record2.insert("id".to_string(), FieldValue::Integer(2));
    record2.insert("name".to_string(), FieldValue::Null); // Null value!
    records.push(record2);

    let output = CapturedOutput {
        query_name: "test_query".to_string(),
        sink_name: "test_output".to_string(),
        topic: Some("test_output".to_string()),
        records,
        execution_time_ms: 100,
        warnings: Vec::new(),
        memory_peak_bytes: None,
        memory_growth_bytes: None,
    };

    let runner = AssertionRunner::new();

    // Test no_nulls on id field - should pass
    let pass_assertion = AssertionConfig::NoNulls(NoNullsAssertion {
        fields: vec!["id".to_string()],
    });

    let results = runner.run_assertions(&output, &[pass_assertion]);
    assert!(results[0].passed, "No nulls on 'id' should pass");

    // Test no_nulls on name field - should fail (has null)
    let fail_assertion = AssertionConfig::NoNulls(NoNullsAssertion {
        fields: vec!["name".to_string()],
    });

    let results = runner.run_assertions(&output, &[fail_assertion]);
    assert!(
        !results[0].passed,
        "No nulls on 'name' should fail due to null value"
    );

    println!("✅ No nulls assertion test passed");
}

/// Test assertion context with template variables
#[test]
fn test_assertion_context_templates() {
    let context = AssertionContext::new()
        .with_input_records("orders", vec![HashMap::new(); 100])
        .with_output_count("enriched_orders", 95)
        .with_variable("threshold", "90");

    // Test input count template
    let input_template = "{{inputs.orders.count}}";
    let resolved = context.resolve_template(input_template);
    assert_eq!(resolved, Some("100".to_string()));

    // Test output count template
    let output_template = "{{outputs.enriched_orders.count}}";
    let resolved = context.resolve_template(output_template);
    assert_eq!(resolved, Some("95".to_string()));

    // Test variable template
    let var_template = "{{variables.threshold}}";
    let resolved = context.resolve_template(var_template);
    assert_eq!(resolved, Some("90".to_string()));

    // Test resolve_or_original
    let literal = "100";
    let resolved = context.resolve_or_original(literal);
    assert_eq!(resolved, "100");

    println!("✅ Assertion context templates test passed");
}
