//! Unit tests for all assertion types in the test harness
//!
//! Tests cover:
//! - Field value assertions (field_in_set, field_values)
//! - Aggregate assertions (aggregate_check)
//! - JOIN assertions (join_coverage)
//! - Template assertions
//! - Performance assertions (execution_time, memory_usage, throughput)
//! - Advanced assertions (no_duplicates, ordering, completeness, data_quality)
//! - Temporal assertions (window_boundary, latency, event_ordering)
//! - Statistical assertions (distribution, percentile)

use std::collections::HashMap;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::test_harness::assertions::{AssertionContext, AssertionRunner};
use velostream::velostream::test_harness::executor::CapturedOutput;
use velostream::velostream::test_harness::spec::{
    AggregateCheckAssertion, AggregateFunction, AssertionConfig, ComparisonOperator,
    CompletenessAssertion, DataQualityAssertion, DistributionAssertion, DistributionType,
    EventOrderingAssertion, ExecutionTimeAssertion, FieldInSetAssertion, FieldValuesAssertion,
    JoinCoverageAssertion, LatencyAssertion, MemoryUsageAssertion, NoDuplicatesAssertion,
    OrderDirection, OrderingAssertion, PercentileAssertion, PercentileMode, TemplateAssertion,
    ThroughputAssertion, WindowBoundaryAssertion,
};

/// Helper to create test output with records
fn create_test_output(records: Vec<HashMap<String, FieldValue>>) -> CapturedOutput {
    CapturedOutput {
        query_name: "test_query".to_string(),
        sink_name: "test_output".to_string(),
        topic: Some("test_topic".to_string()),
        records: records.into_iter().map(StreamRecord::new).collect(),
        execution_time_ms: 500,
        warnings: Vec::new(),
        memory_peak_bytes: Some(1024 * 1024),  // 1 MB
        memory_growth_bytes: Some(512 * 1024), // 512 KB
    }
}

/// Helper to create a record with fields
fn record(fields: Vec<(&str, FieldValue)>) -> HashMap<String, FieldValue> {
    fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

// =============================================================================
// Field Value Assertions
// =============================================================================

#[test]
fn test_field_in_set_pass() {
    let records = vec![
        record(vec![("status", FieldValue::String("active".to_string()))]),
        record(vec![("status", FieldValue::String("pending".to_string()))]),
        record(vec![("status", FieldValue::String("active".to_string()))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::FieldInSet(FieldInSetAssertion {
        field: "status".to_string(),
        values: vec![
            "active".to_string(),
            "pending".to_string(),
            "completed".to_string(),
        ],
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "Should pass: {}", results[0].message);
}

#[test]
fn test_field_in_set_fail() {
    let records = vec![
        record(vec![("status", FieldValue::String("active".to_string()))]),
        record(vec![("status", FieldValue::String("unknown".to_string()))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::FieldInSet(FieldInSetAssertion {
        field: "status".to_string(),
        values: vec!["active".to_string(), "pending".to_string()],
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(!results[0].passed, "Should fail for 'unknown' value");
}

#[test]
fn test_field_values_greater_than() {
    let records = vec![
        record(vec![("price", FieldValue::Float(100.0))]),
        record(vec![("price", FieldValue::Float(150.0))]),
        record(vec![("price", FieldValue::Float(200.0))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::FieldValues(FieldValuesAssertion {
        field: "price".to_string(),
        operator: ComparisonOperator::GreaterThan,
        value: serde_yaml::Value::Number(serde_yaml::Number::from(50)),
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "All prices > 50: {}", results[0].message);
}

#[test]
fn test_field_values_contains() {
    let records = vec![
        record(vec![(
            "message",
            FieldValue::String("Error: connection failed".to_string()),
        )]),
        record(vec![(
            "message",
            FieldValue::String("Error: timeout".to_string()),
        )]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::FieldValues(FieldValuesAssertion {
        field: "message".to_string(),
        operator: ComparisonOperator::Contains,
        value: serde_yaml::Value::String("Error".to_string()),
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "All messages contain 'Error': {}",
        results[0].message
    );
}

#[test]
fn test_field_values_equals() {
    let records = vec![
        record(vec![("status", FieldValue::String("active".to_string()))]),
        record(vec![("status", FieldValue::String("active".to_string()))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::FieldValues(FieldValuesAssertion {
        field: "status".to_string(),
        operator: ComparisonOperator::Equals,
        value: serde_yaml::Value::String("active".to_string()),
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "All status = 'active': {}",
        results[0].message
    );
}

// =============================================================================
// Aggregate Assertions
// =============================================================================

#[test]
fn test_aggregate_sum() {
    let records = vec![
        record(vec![("amount", FieldValue::Integer(100))]),
        record(vec![("amount", FieldValue::Integer(200))]),
        record(vec![("amount", FieldValue::Integer(300))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::AggregateCheck(AggregateCheckAssertion {
        field: "amount".to_string(),
        function: AggregateFunction::Sum,
        expected: "600".to_string(),
        tolerance: Some(0.01),
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "SUM should be 600: {}",
        results[0].message
    );
}

#[test]
fn test_aggregate_avg() {
    let records = vec![
        record(vec![("score", FieldValue::Float(80.0))]),
        record(vec![("score", FieldValue::Float(90.0))]),
        record(vec![("score", FieldValue::Float(100.0))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::AggregateCheck(AggregateCheckAssertion {
        field: "score".to_string(),
        function: AggregateFunction::Avg,
        expected: "90".to_string(),
        tolerance: Some(0.01),
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "AVG should be 90: {}",
        results[0].message
    );
}

#[test]
fn test_aggregate_min_max() {
    let records = vec![
        record(vec![("value", FieldValue::Integer(5))]),
        record(vec![("value", FieldValue::Integer(15))]),
        record(vec![("value", FieldValue::Integer(10))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let min_assertion = AssertionConfig::AggregateCheck(AggregateCheckAssertion {
        field: "value".to_string(),
        function: AggregateFunction::Min,
        expected: "5".to_string(),
        tolerance: None,
    });

    let max_assertion = AssertionConfig::AggregateCheck(AggregateCheckAssertion {
        field: "value".to_string(),
        function: AggregateFunction::Max,
        expected: "15".to_string(),
        tolerance: None,
    });

    let results = runner.run_assertions(&output, &[min_assertion, max_assertion]);
    assert!(results[0].passed, "MIN should be 5: {}", results[0].message);
    assert!(
        results[1].passed,
        "MAX should be 15: {}",
        results[1].message
    );
}

// =============================================================================
// JOIN Coverage Assertions
// =============================================================================

#[test]
fn test_join_coverage_pass() {
    // 8 output records from 10 input records = 80% match rate
    let records: Vec<_> = (0..8)
        .map(|i| record(vec![("id", FieldValue::Integer(i))]))
        .collect();
    let output = create_test_output(records);

    let input: Vec<_> = (0..10)
        .map(|i| record(vec![("id", FieldValue::Integer(i))]))
        .collect();

    let runner = AssertionRunner::new()
        .with_context(AssertionContext::new().with_input_records("orders", input));

    let assertion = AssertionConfig::JoinCoverage(JoinCoverageAssertion {
        left_source: Some("orders".to_string()),
        right_source: None,
        min_match_rate: 0.7, // 70% required
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "80% >= 70% match rate: {}",
        results[0].message
    );
}

#[test]
fn test_join_coverage_fail() {
    // 3 output records from 10 input records = 30% match rate
    let records: Vec<_> = (0..3)
        .map(|i| record(vec![("id", FieldValue::Integer(i))]))
        .collect();
    let output = create_test_output(records);

    let input: Vec<_> = (0..10)
        .map(|i| record(vec![("id", FieldValue::Integer(i))]))
        .collect();

    let runner = AssertionRunner::new()
        .with_context(AssertionContext::new().with_input_records("orders", input));

    let assertion = AssertionConfig::JoinCoverage(JoinCoverageAssertion {
        left_source: Some("orders".to_string()),
        right_source: None,
        min_match_rate: 0.5, // 50% required
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        !results[0].passed,
        "30% < 50% should fail: {}",
        results[0].message
    );
}

// =============================================================================
// Template Assertions
// =============================================================================

#[test]
fn test_template_simple_expression() {
    let records = vec![
        record(vec![("value", FieldValue::Integer(10))]),
        record(vec![("value", FieldValue::Integer(20))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Template(TemplateAssertion {
        expression: "len(records) == 2".to_string(),
        description: Some("Should have exactly 2 records".to_string()),
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "Template: {}", results[0].message);
}

#[test]
fn test_template_aggregate_expression() {
    let records = vec![
        record(vec![("amount", FieldValue::Integer(100))]),
        record(vec![("amount", FieldValue::Integer(200))]),
        record(vec![("amount", FieldValue::Integer(300))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    // Template engine supports sum(field) or sum(expr for item in records)
    let assertion = AssertionConfig::Template(TemplateAssertion {
        expression: "sum(amount) == 600".to_string(),
        description: Some("Sum of amounts should be 600".to_string()),
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "Template sum: {}", results[0].message);
}

// =============================================================================
// Performance Assertions
// =============================================================================

#[test]
fn test_execution_time_pass() {
    let output = create_test_output(vec![record(vec![("id", FieldValue::Integer(1))])]);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::ExecutionTime(ExecutionTimeAssertion {
        max_ms: Some(1000), // Output has 500ms
        min_ms: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "500ms < 1000ms: {}", results[0].message);
}

#[test]
fn test_execution_time_fail() {
    let output = create_test_output(vec![record(vec![("id", FieldValue::Integer(1))])]);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::ExecutionTime(ExecutionTimeAssertion {
        max_ms: Some(100), // Output has 500ms
        min_ms: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        !results[0].passed,
        "500ms > 100ms should fail: {}",
        results[0].message
    );
}

#[test]
fn test_memory_usage_pass() {
    let output = create_test_output(vec![record(vec![("id", FieldValue::Integer(1))])]);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::MemoryUsage(MemoryUsageAssertion {
        max_bytes: Some(2 * 1024 * 1024), // 2 MB (output has 1 MB peak)
        max_mb: None,
        max_growth_bytes: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "1MB < 2MB: {}", results[0].message);
}

#[test]
fn test_throughput_pass() {
    // 100 records in 500ms = 200 records/sec
    let records: Vec<_> = (0..100)
        .map(|i| record(vec![("id", FieldValue::Integer(i))]))
        .collect();
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Throughput(ThroughputAssertion {
        min_records_per_second: Some(100.0), // 200 > 100
        max_records_per_second: None,
        expected_records_per_second: None,
        tolerance_percent: 20.0,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "200 rec/s >= 100 rec/s: {}",
        results[0].message
    );
}

// =============================================================================
// Advanced Assertions
// =============================================================================

#[test]
fn test_no_duplicates_pass() {
    let records = vec![
        record(vec![("id", FieldValue::Integer(1))]),
        record(vec![("id", FieldValue::Integer(2))]),
        record(vec![("id", FieldValue::Integer(3))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::NoDuplicates(NoDuplicatesAssertion {
        key_fields: vec!["id".to_string()],
        allow_updates: false,
        max_duplicate_percent: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "No duplicates: {}", results[0].message);
}

#[test]
fn test_no_duplicates_fail() {
    let records = vec![
        record(vec![("id", FieldValue::Integer(1))]),
        record(vec![("id", FieldValue::Integer(2))]),
        record(vec![("id", FieldValue::Integer(1))]), // Duplicate
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::NoDuplicates(NoDuplicatesAssertion {
        key_fields: vec!["id".to_string()],
        allow_updates: false,
        max_duplicate_percent: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        !results[0].passed,
        "Duplicate id=1 should fail: {}",
        results[0].message
    );
}

#[test]
fn test_ordering_ascending() {
    let records = vec![
        record(vec![("timestamp", FieldValue::Integer(100))]),
        record(vec![("timestamp", FieldValue::Integer(200))]),
        record(vec![("timestamp", FieldValue::Integer(300))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Ordering(OrderingAssertion {
        field: "timestamp".to_string(),
        direction: OrderDirection::Ascending,
        partition_by: None,
        allow_equal: true,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "Ascending order: {}", results[0].message);
}

#[test]
fn test_ordering_descending_fail() {
    let records = vec![
        record(vec![("timestamp", FieldValue::Integer(100))]),
        record(vec![("timestamp", FieldValue::Integer(200))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Ordering(OrderingAssertion {
        field: "timestamp".to_string(),
        direction: OrderDirection::Descending,
        partition_by: None,
        allow_equal: false,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(!results[0].passed, "Not descending: {}", results[0].message);
}

#[test]
fn test_completeness_pass() {
    // Output has keys 1, 2, 3 (matches input)
    let output_records = vec![
        record(vec![("order_id", FieldValue::Integer(1))]),
        record(vec![("order_id", FieldValue::Integer(2))]),
        record(vec![("order_id", FieldValue::Integer(3))]),
    ];
    let output = create_test_output(output_records);

    let input = vec![
        record(vec![("order_id", FieldValue::Integer(1))]),
        record(vec![("order_id", FieldValue::Integer(2))]),
        record(vec![("order_id", FieldValue::Integer(3))]),
    ];

    let runner = AssertionRunner::new()
        .with_context(AssertionContext::new().with_input_records("orders", input));

    let assertion = AssertionConfig::Completeness(CompletenessAssertion {
        input_source: "orders".to_string(),
        key_field: "order_id".to_string(),
        min_completeness: 1.0, // 100%
        required_fields: vec![],
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "100% completeness: {}",
        results[0].message
    );
}

#[test]
fn test_data_quality_no_nulls() {
    let records = vec![
        record(vec![
            ("name", FieldValue::String("Alice".to_string())),
            ("age", FieldValue::Integer(30)),
        ]),
        record(vec![
            ("name", FieldValue::String("Bob".to_string())),
            ("age", FieldValue::Integer(25)),
        ]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::DataQuality(DataQualityAssertion {
        no_nulls_in: vec!["name".to_string(), "age".to_string()],
        no_empty_strings_in: vec!["name".to_string()],
        numeric_ranges: vec![],
        string_patterns: vec![],
        referential_integrity: vec![],
        min_quality_score: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "Data quality pass: {}",
        results[0].message
    );
}

#[test]
fn test_data_quality_fail_on_null() {
    let records = vec![record(vec![
        ("name", FieldValue::String("Alice".to_string())),
        ("age", FieldValue::Null),
    ])];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::DataQuality(DataQualityAssertion {
        no_nulls_in: vec!["age".to_string()],
        no_empty_strings_in: vec![],
        numeric_ranges: vec![],
        string_patterns: vec![],
        referential_integrity: vec![],
        min_quality_score: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        !results[0].passed,
        "Null in age should fail: {}",
        results[0].message
    );
}

// =============================================================================
// Temporal Assertions
// =============================================================================

#[test]
fn test_event_ordering_by_timestamp() {
    let records = vec![
        record(vec![("event_time", FieldValue::Integer(1000))]),
        record(vec![("event_time", FieldValue::Integer(2000))]),
        record(vec![("event_time", FieldValue::Integer(3000))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::EventOrdering(EventOrderingAssertion {
        timestamp_field: "event_time".to_string(),
        partition_field: None,
        window_size_ms: None,
        direction: OrderDirection::Ascending,
        allow_gaps: true,
        max_out_of_order_percent: None,
        max_gap_ms: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "Events in order: {}", results[0].message);
}

#[test]
fn test_event_ordering_descending() {
    // Events in descending order
    let records = vec![
        record(vec![("event_time", FieldValue::Integer(3000))]),
        record(vec![("event_time", FieldValue::Integer(2000))]),
        record(vec![("event_time", FieldValue::Integer(1000))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::EventOrdering(EventOrderingAssertion {
        timestamp_field: "event_time".to_string(),
        partition_field: None,
        window_size_ms: None,
        direction: OrderDirection::Descending,
        allow_gaps: true,
        max_out_of_order_percent: None,
        max_gap_ms: None,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "Within tolerance: {}",
        results[0].message
    );
}

#[test]
fn test_window_boundary_containment() {
    let records = vec![
        record(vec![
            ("event_time", FieldValue::Integer(1500)),
            ("window_start", FieldValue::Integer(1000)),
            ("window_end", FieldValue::Integer(2000)),
        ]),
        record(vec![
            ("event_time", FieldValue::Integer(2500)),
            ("window_start", FieldValue::Integer(2000)),
            ("window_end", FieldValue::Integer(3000)),
        ]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::WindowBoundary(WindowBoundaryAssertion {
        timestamp_field: "event_time".to_string(),
        window_start_field: Some("window_start".to_string()),
        window_end_field: Some("window_end".to_string()),
        window_size_ms: None,
        tolerance_ms: 0,
        verify_containment: true,
        verify_alignment: false,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "Events within windows: {}",
        results[0].message
    );
}

#[test]
fn test_latency_within_bounds() {
    // Use recent timestamps (current time - small offset)
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let records = vec![
        record(vec![("event_time", FieldValue::Integer(now_ms - 100))]),
        record(vec![("event_time", FieldValue::Integer(now_ms - 200))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Latency(LatencyAssertion {
        timestamp_field: "event_time".to_string(),
        max_latency_ms: Some(5000), // 5 second max
        p99_latency_ms: None,
        p95_latency_ms: None,
        p50_latency_ms: None,
        avg_latency_ms: None,
        min_records: 1,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "Latency within bounds: {}",
        results[0].message
    );
}

// =============================================================================
// Statistical Assertions
// =============================================================================

#[test]
fn test_distribution_normal() {
    // Generate values roughly around mean=100, std=10
    let records = vec![
        record(vec![("value", FieldValue::Float(95.0))]),
        record(vec![("value", FieldValue::Float(100.0))]),
        record(vec![("value", FieldValue::Float(105.0))]),
        record(vec![("value", FieldValue::Float(98.0))]),
        record(vec![("value", FieldValue::Float(102.0))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Distribution(DistributionAssertion {
        field: "value".to_string(),
        distribution_type: DistributionType::Normal,
        mean: Some(100.0),
        std_dev: Some(5.0),
        min_value: None,
        max_value: None,
        tolerance: 0.5, // 50% tolerance
        min_records: 3,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(
        results[0].passed,
        "Normal distribution: {}",
        results[0].message
    );
}

#[test]
fn test_percentile_p99() {
    // Values: 1-100, p99 should be around 99
    let records: Vec<_> = (1..=100)
        .map(|i| record(vec![("latency", FieldValue::Integer(i))]))
        .collect();
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Percentile(PercentileAssertion {
        field: "latency".to_string(),
        p50: None,
        p75: None,
        p90: None,
        p95: None,
        p99: Some(100.0), // p99 should be <= 100
        p999: None,
        mode: PercentileMode::LessThan,
        min_records: 10,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "P99 <= 100: {}", results[0].message);
}

#[test]
fn test_percentile_p50_median() {
    let records = vec![
        record(vec![("value", FieldValue::Integer(10))]),
        record(vec![("value", FieldValue::Integer(20))]),
        record(vec![("value", FieldValue::Integer(30))]),
        record(vec![("value", FieldValue::Integer(40))]),
        record(vec![("value", FieldValue::Integer(50))]),
    ];
    let output = create_test_output(records);
    let runner = AssertionRunner::new();

    let assertion = AssertionConfig::Percentile(PercentileAssertion {
        field: "value".to_string(),
        p50: Some(35.0), // Median around 30
        p75: None,
        p90: None,
        p95: None,
        p99: None,
        p999: None,
        mode: PercentileMode::LessThan,
        min_records: 3,
    });

    let results = runner.run_assertions(&output, &[assertion]);
    assert!(results[0].passed, "P50 <= 35: {}", results[0].message);
}
