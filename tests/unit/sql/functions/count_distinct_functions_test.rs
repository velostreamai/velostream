use ferrisstreams::ferris::sql::ast::{Expr, LiteralValue};
use ferrisstreams::ferris::sql::execution::expression::functions::BuiltinFunctions;
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;

#[test]
fn test_count_distinct_function_registration() {
    // Test that COUNT_DISTINCT is registered in the function registry
    let record = create_test_record();
    let args = vec![Expr::Literal(LiteralValue::String("customer1".to_string()))];

    // Test COUNT_DISTINCT function call
    let result = BuiltinFunctions::evaluate_function_by_name("COUNT_DISTINCT", &args, &record);
    assert!(
        result.is_ok(),
        "COUNT_DISTINCT should be registered and callable"
    );

    // Should return 1 for non-null value (per-record logic)
    match result.unwrap() {
        FieldValue::Integer(count) => assert_eq!(count, 1),
        other => panic!("Expected Integer, got {:?}", other),
    }
}

#[test]
fn test_approx_count_distinct_function_registration() {
    // Test that APPROX_COUNT_DISTINCT is registered in the function registry
    let record = create_test_record();
    let args = vec![Expr::Literal(LiteralValue::String("customer1".to_string()))];

    // Test APPROX_COUNT_DISTINCT function call
    let result =
        BuiltinFunctions::evaluate_function_by_name("APPROX_COUNT_DISTINCT", &args, &record);
    assert!(
        result.is_ok(),
        "APPROX_COUNT_DISTINCT should be registered and callable"
    );

    // Should return 1 for non-null value (per-record logic)
    match result.unwrap() {
        FieldValue::Integer(count) => assert_eq!(count, 1),
        other => panic!("Expected Integer, got {:?}", other),
    }
}

#[test]
fn test_count_distinct_null_handling() {
    let record = create_test_record();
    let args = vec![Expr::Literal(LiteralValue::Null)];

    let result = BuiltinFunctions::evaluate_function_by_name("COUNT_DISTINCT", &args, &record);
    assert!(result.is_ok(), "COUNT_DISTINCT should handle null values");

    match result.unwrap() {
        FieldValue::Integer(count) => assert_eq!(count, 0, "NULL values should return 0"),
        other => panic!("Expected Integer(0), got {:?}", other),
    }
}

#[test]
fn test_approx_count_distinct_null_handling() {
    let record = create_test_record();
    let args = vec![Expr::Literal(LiteralValue::Null)];

    let result =
        BuiltinFunctions::evaluate_function_by_name("APPROX_COUNT_DISTINCT", &args, &record);
    assert!(
        result.is_ok(),
        "APPROX_COUNT_DISTINCT should handle null values"
    );

    match result.unwrap() {
        FieldValue::Integer(count) => assert_eq!(count, 0, "NULL values should return 0"),
        other => panic!("Expected Integer(0), got {:?}", other),
    }
}

#[test]
fn test_function_argument_validation() {
    let record = create_test_record();

    // Test no arguments
    let result = BuiltinFunctions::evaluate_function_by_name("COUNT_DISTINCT", &[], &record);
    assert!(
        result.is_err(),
        "COUNT_DISTINCT should require exactly one argument"
    );

    // Test too many arguments
    let args = vec![
        Expr::Literal(LiteralValue::String("customer1".to_string())),
        Expr::Literal(LiteralValue::String("customer2".to_string())),
    ];
    let result = BuiltinFunctions::evaluate_function_by_name("COUNT_DISTINCT", &args, &record);
    assert!(
        result.is_err(),
        "COUNT_DISTINCT should require exactly one argument"
    );
}

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "customer_id".to_string(),
        FieldValue::String("customer1".to_string()),
    );
    fields.insert("amount".to_string(), FieldValue::ScaledInteger(12345, 2));
    fields.insert(
        "merchant_category".to_string(),
        FieldValue::String("restaurant".to_string()),
    );

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    }
}

#[test]
fn test_both_functions_produce_same_result() {
    // Since APPROX_COUNT_DISTINCT uses same logic as COUNT_DISTINCT for now
    let record = create_test_record();
    let args = vec![Expr::Literal(LiteralValue::String(
        "test_value".to_string(),
    ))];

    let count_distinct_result =
        BuiltinFunctions::evaluate_function_by_name("COUNT_DISTINCT", &args, &record);
    let approx_count_distinct_result =
        BuiltinFunctions::evaluate_function_by_name("APPROX_COUNT_DISTINCT", &args, &record);

    assert!(count_distinct_result.is_ok());
    assert!(approx_count_distinct_result.is_ok());

    // Both should produce same result for now
    assert_eq!(
        count_distinct_result.unwrap(),
        approx_count_distinct_result.unwrap()
    );
}
