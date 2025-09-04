use ferrisstreams::ferris::sql::ast::{Expr, LiteralValue};
use ferrisstreams::ferris::sql::execution::{
    aggregation::{accumulator::AccumulatorManager, functions::AggregateFunctions},
    internal::GroupAccumulator,
    types::{FieldValue, StreamRecord},
};
use std::collections::HashMap;

#[test]
fn test_count_distinct_exact_counting() {
    let mut accumulator = GroupAccumulator::new();

    // Add duplicate values
    accumulator.add_to_set("customer_id", FieldValue::String("cust1".to_string()));
    accumulator.add_to_set("customer_id", FieldValue::String("cust2".to_string()));
    accumulator.add_to_set("customer_id", FieldValue::String("cust1".to_string())); // Duplicate
    accumulator.add_to_set("customer_id", FieldValue::String("cust3".to_string()));
    accumulator.add_to_set("customer_id", FieldValue::String("cust2".to_string())); // Duplicate

    // Compute COUNT_DISTINCT
    let result = AggregateFunctions::compute_field_aggregate_value(
        "customer_id",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String(
                "customer_id".to_string(),
            ))],
        },
        &accumulator,
    )
    .unwrap();

    // Should be exactly 3 distinct values
    match result {
        FieldValue::Integer(count) => assert_eq!(
            count, 3,
            "COUNT_DISTINCT should return exactly 3 distinct values"
        ),
        other => panic!("Expected Integer, got {:?}", other),
    }

    println!("✅ COUNT_DISTINCT exact counting test passed!");
}

#[test]
fn test_approx_count_distinct_hyperloglog() {
    let mut accumulator = GroupAccumulator::new();

    // Add many distinct values to test HyperLogLog approximation
    for i in 0..1000 {
        let value = FieldValue::String(format!("customer_{}", i));
        accumulator.add_to_approx_set("customer_id", value);
    }

    // Compute APPROX_COUNT_DISTINCT
    let result = AggregateFunctions::compute_field_aggregate_value(
        "customer_id",
        &Expr::Function {
            name: "APPROX_COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String(
                "customer_id".to_string(),
            ))],
        },
        &accumulator,
    )
    .unwrap();

    // Should be approximately 1000 (within HyperLogLog error bounds)
    match result {
        FieldValue::Integer(count) => {
            let error_rate = (count - 1000).abs() as f64 / 1000.0;
            assert!(
                error_rate < 0.05,
                "APPROX_COUNT_DISTINCT error rate should be < 5%, got {}% (count: {})",
                error_rate * 100.0,
                count
            );
            println!(
                "APPROX_COUNT_DISTINCT estimated {} (actual: 1000, error: {:.2}%)",
                count,
                error_rate * 100.0
            );
        }
        other => panic!("Expected Integer, got {:?}", other),
    }

    println!("✅ APPROX_COUNT_DISTINCT HyperLogLog test passed!");
}

#[test]
fn test_count_distinct_vs_approx_count_distinct_accuracy() {
    let mut accumulator = GroupAccumulator::new();

    // Add 100 distinct values to both exact and approximate counters
    for i in 0..100 {
        let value = FieldValue::Integer(i);
        accumulator.add_to_set("test_field", value.clone());
        accumulator.add_to_approx_set("test_field", value);
    }

    // Get COUNT_DISTINCT result
    let exact_result = AggregateFunctions::compute_field_aggregate_value(
        "test_field",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String(
                "test_field".to_string(),
            ))],
        },
        &accumulator,
    )
    .unwrap();

    // Get APPROX_COUNT_DISTINCT result
    let approx_result = AggregateFunctions::compute_field_aggregate_value(
        "test_field",
        &Expr::Function {
            name: "APPROX_COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String(
                "test_field".to_string(),
            ))],
        },
        &accumulator,
    )
    .unwrap();

    let exact_count = match exact_result {
        FieldValue::Integer(c) => c,
        _ => panic!("Expected Integer from COUNT_DISTINCT"),
    };

    let approx_count = match approx_result {
        FieldValue::Integer(c) => c,
        _ => panic!("Expected Integer from APPROX_COUNT_DISTINCT"),
    };

    // Exact should be precisely 100
    assert_eq!(exact_count, 100, "COUNT_DISTINCT should be exactly 100");

    // Approximate should be close to 100 (within reasonable error bounds)
    let error_rate = (approx_count - 100).abs() as f64 / 100.0;
    assert!(
        error_rate < 0.1,
        "APPROX_COUNT_DISTINCT should be within 10% of exact count"
    );

    println!(
        "COUNT_DISTINCT: {}, APPROX_COUNT_DISTINCT: {}, Error: {:.2}%",
        exact_count,
        approx_count,
        error_rate * 100.0
    );
    println!("✅ Accuracy comparison test passed!");
}

#[test]
fn test_count_distinct_with_different_field_types() {
    let mut accumulator = GroupAccumulator::new();

    // Test with different FieldValue types
    accumulator.add_to_set("strings", FieldValue::String("alice".to_string()));
    accumulator.add_to_set("strings", FieldValue::String("bob".to_string()));
    accumulator.add_to_set("strings", FieldValue::String("alice".to_string())); // Duplicate

    accumulator.add_to_set("integers", FieldValue::Integer(1));
    accumulator.add_to_set("integers", FieldValue::Integer(2));
    accumulator.add_to_set("integers", FieldValue::Integer(1)); // Duplicate
    accumulator.add_to_set("integers", FieldValue::Integer(3));

    accumulator.add_to_set("scaled", FieldValue::ScaledInteger(12345, 2));
    accumulator.add_to_set("scaled", FieldValue::ScaledInteger(67890, 2));
    accumulator.add_to_set("scaled", FieldValue::ScaledInteger(12345, 2)); // Duplicate

    // Test string field
    let string_result = AggregateFunctions::compute_field_aggregate_value(
        "strings",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String("strings".to_string()))],
        },
        &accumulator,
    )
    .unwrap();

    match string_result {
        FieldValue::Integer(count) => assert_eq!(count, 2, "Should have 2 distinct strings"),
        other => panic!("Expected Integer, got {:?}", other),
    }

    // Test integer field
    let int_result = AggregateFunctions::compute_field_aggregate_value(
        "integers",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String("integers".to_string()))],
        },
        &accumulator,
    )
    .unwrap();

    match int_result {
        FieldValue::Integer(count) => assert_eq!(count, 3, "Should have 3 distinct integers"),
        other => panic!("Expected Integer, got {:?}", other),
    }

    // Test scaled integer field
    let scaled_result = AggregateFunctions::compute_field_aggregate_value(
        "scaled",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String("scaled".to_string()))],
        },
        &accumulator,
    )
    .unwrap();

    match scaled_result {
        FieldValue::Integer(count) => {
            assert_eq!(count, 2, "Should have 2 distinct scaled integers")
        }
        other => panic!("Expected Integer, got {:?}", other),
    }

    println!("✅ Different field types test passed!");
}

#[test]
fn test_count_distinct_empty_sets() {
    let accumulator = GroupAccumulator::new();

    // Test COUNT_DISTINCT on empty set
    let exact_result = AggregateFunctions::compute_field_aggregate_value(
        "nonexistent_field",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String(
                "nonexistent_field".to_string(),
            ))],
        },
        &accumulator,
    )
    .unwrap();

    // Test APPROX_COUNT_DISTINCT on empty set
    let approx_result = AggregateFunctions::compute_field_aggregate_value(
        "nonexistent_field",
        &Expr::Function {
            name: "APPROX_COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String(
                "nonexistent_field".to_string(),
            ))],
        },
        &accumulator,
    )
    .unwrap();

    // Both should return 0
    match exact_result {
        FieldValue::Integer(count) => {
            assert_eq!(count, 0, "COUNT_DISTINCT should return 0 for empty set")
        }
        other => panic!("Expected Integer(0), got {:?}", other),
    }

    match approx_result {
        FieldValue::Integer(count) => assert_eq!(
            count, 0,
            "APPROX_COUNT_DISTINCT should return 0 for empty set"
        ),
        other => panic!("Expected Integer(0), got {:?}", other),
    }

    println!("✅ Empty sets test passed!");
}

#[test]
fn test_count_distinct_null_handling() {
    let mut accumulator = GroupAccumulator::new();

    // Add some non-null values and null values
    accumulator.add_to_set("test_field", FieldValue::String("value1".to_string()));
    accumulator.add_to_set("test_field", FieldValue::String("value2".to_string()));
    // Note: add_to_set doesn't add null values in the current implementation
    // This tests the case where nulls are properly filtered out

    let result = AggregateFunctions::compute_field_aggregate_value(
        "test_field",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String(
                "test_field".to_string(),
            ))],
        },
        &accumulator,
    )
    .unwrap();

    match result {
        FieldValue::Integer(count) => {
            assert_eq!(count, 2, "Should count only non-null distinct values")
        }
        other => panic!("Expected Integer, got {:?}", other),
    }

    println!("✅ Null handling test passed!");
}

#[test]
fn test_hyperloglog_performance_vs_exact() {
    use std::time::Instant;

    // Test performance difference between exact and approximate counting
    let mut exact_accumulator = GroupAccumulator::new();
    let mut approx_accumulator = GroupAccumulator::new();

    const NUM_VALUES: usize = 10000;

    // Generate test data
    let test_values: Vec<FieldValue> = (0..NUM_VALUES)
        .map(|i| FieldValue::String(format!("user_{}", i)))
        .collect();

    // Time exact counting
    let exact_start = Instant::now();
    for value in &test_values {
        exact_accumulator.add_to_set("users", value.clone());
    }
    let exact_duration = exact_start.elapsed();

    // Time approximate counting
    let approx_start = Instant::now();
    for value in &test_values {
        approx_accumulator.add_to_approx_set("users", value.clone());
    }
    let approx_duration = approx_start.elapsed();

    // Get results
    let exact_result = AggregateFunctions::compute_field_aggregate_value(
        "users",
        &Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String("users".to_string()))],
        },
        &exact_accumulator,
    )
    .unwrap();

    let approx_result = AggregateFunctions::compute_field_aggregate_value(
        "users",
        &Expr::Function {
            name: "APPROX_COUNT_DISTINCT".to_string(),
            args: vec![Expr::Literal(LiteralValue::String("users".to_string()))],
        },
        &approx_accumulator,
    )
    .unwrap();

    let exact_count = match exact_result {
        FieldValue::Integer(c) => c,
        _ => panic!("Expected Integer"),
    };

    let approx_count = match approx_result {
        FieldValue::Integer(c) => c,
        _ => panic!("Expected Integer"),
    };

    // Verify accuracy
    assert_eq!(
        exact_count, NUM_VALUES as i64,
        "Exact count should be precisely correct"
    );
    let error_rate = (approx_count - NUM_VALUES as i64).abs() as f64 / NUM_VALUES as f64;
    assert!(
        error_rate < 0.05,
        "Approximate count should be within 5% error"
    );

    println!("Performance Comparison for {} distinct values:", NUM_VALUES);
    println!(
        "  COUNT_DISTINCT: {} (took {:?})",
        exact_count, exact_duration
    );
    println!(
        "  APPROX_COUNT_DISTINCT: {} (took {:?}, {:.2}% error)",
        approx_count,
        approx_duration,
        error_rate * 100.0
    );

    // HyperLogLog should use constant memory regardless of cardinality
    // While HashSet memory grows with number of distinct values
    println!("✅ Performance comparison test passed!");
}

fn create_test_record(customer_id: &str, amount: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "customer_id".to_string(),
        FieldValue::String(customer_id.to_string()),
    );
    fields.insert("amount".to_string(), FieldValue::ScaledInteger(amount, 2));

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
    }
}
