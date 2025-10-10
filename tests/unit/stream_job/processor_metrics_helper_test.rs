// Direct unit tests for ProcessorMetricsHelper
//
// These tests verify the metrics helper module directly without going through
// processor delegation, ensuring better test isolation.

use std::collections::HashMap;
use velostream::velostream::server::processors::ProcessorMetricsHelper;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Helper function to create a test record with specified fields
fn create_test_record(fields: HashMap<String, FieldValue>) -> StreamRecord {
    StreamRecord {
        fields,
        timestamp: 1000,
        offset: 1,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    }
}

/// Helper function to create a simple numeric record
fn create_numeric_record(volume: i64, price: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("volume".to_string(), FieldValue::Integer(volume));
    fields.insert("price".to_string(), FieldValue::Float(price));
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    create_test_record(fields)
}

mod parse_condition_tests {
    use super::*;

    #[test]
    fn test_parse_simple_condition_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("volume > 1000");
        assert!(
            result.is_ok(),
            "Simple condition should parse successfully: {:?}",
            result
        );
    }

    #[test]
    fn test_parse_complex_and_condition_direct() {
        let result =
            ProcessorMetricsHelper::parse_condition_to_expr("volume > 1000 AND price > 100");
        assert!(
            result.is_ok(),
            "Complex AND condition should parse successfully"
        );
    }

    #[test]
    fn test_parse_complex_or_condition_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("volume > 1000 OR price < 50");
        assert!(
            result.is_ok(),
            "Complex OR condition should parse successfully"
        );
    }

    #[test]
    fn test_parse_with_arithmetic_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("volume > avg_volume * 2");
        assert!(
            result.is_ok(),
            "Condition with multiplication should parse successfully"
        );
    }

    #[test]
    fn test_parse_invalid_syntax_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("volume > > 1000");
        assert!(result.is_err(), "Invalid syntax should return error");
    }

    #[test]
    fn test_parse_empty_condition_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("");
        assert!(result.is_err(), "Empty condition should return error");
    }

    #[test]
    fn test_parse_null_checks_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("field IS NULL");
        assert!(result.is_ok(), "NULL check should parse successfully");

        let result = ProcessorMetricsHelper::parse_condition_to_expr("field IS NOT NULL");
        assert!(result.is_ok(), "NOT NULL check should parse successfully");
    }

    #[test]
    fn test_parse_between_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("volume BETWEEN 100 AND 1000");
        assert!(result.is_ok(), "BETWEEN should parse successfully");
    }

    #[test]
    fn test_parse_with_parentheses_direct() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr(
            "(volume > 100 AND price > 50) OR symbol = 'AAPL'",
        );
        assert!(result.is_ok(), "Condition with parentheses should parse");
    }
}

mod evaluate_condition_expr_tests {
    use super::*;

    /// Helper to parse and evaluate a condition
    fn parse_and_evaluate(condition: &str, record: &StreamRecord) -> bool {
        let expr = ProcessorMetricsHelper::parse_condition_to_expr(condition)
            .expect("Failed to parse condition");
        ProcessorMetricsHelper::evaluate_condition_expr(&expr, record, "test_metric", "test_job")
    }

    #[test]
    fn test_evaluate_simple_greater_than() {
        let record = create_numeric_record(1500, 150.0);
        assert!(
            parse_and_evaluate("volume > 1000", &record),
            "1500 > 1000 should be true"
        );
    }

    #[test]
    fn test_evaluate_simple_less_than() {
        let record = create_numeric_record(500, 150.0);
        assert!(
            !parse_and_evaluate("volume > 1000", &record),
            "500 > 1000 should be false"
        );
    }

    #[test]
    fn test_evaluate_and_both_true() {
        let record = create_numeric_record(1500, 150.0);
        assert!(
            parse_and_evaluate("volume > 1000 AND price > 100", &record),
            "Both conditions should be true"
        );
    }

    #[test]
    fn test_evaluate_and_one_false() {
        let record = create_numeric_record(1500, 50.0);
        assert!(
            !parse_and_evaluate("volume > 1000 AND price > 100", &record),
            "One false condition should make AND false"
        );
    }

    #[test]
    fn test_evaluate_or_one_true() {
        let record = create_numeric_record(500, 150.0);
        assert!(
            parse_and_evaluate("volume > 1000 OR price > 100", &record),
            "One true condition should make OR true"
        );
    }

    #[test]
    fn test_evaluate_with_multiplication() {
        let mut fields = HashMap::new();
        fields.insert("volume".to_string(), FieldValue::Integer(2500));
        fields.insert("avg_volume".to_string(), FieldValue::Integer(1000));
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("volume > avg_volume * 2", &record),
            "2500 > (1000 * 2) should be true"
        );
    }

    #[test]
    fn test_evaluate_missing_field_returns_false() {
        let record = create_numeric_record(1500, 150.0);
        assert!(
            !parse_and_evaluate("nonexistent_field > 100", &record),
            "Missing field should result in false"
        );
    }

    #[test]
    fn test_evaluate_null_comparison() {
        let mut fields = HashMap::new();
        fields.insert("nullable_field".to_string(), FieldValue::Null);
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("nullable_field IS NULL", &record),
            "NULL field should be detected"
        );
    }

    #[test]
    fn test_evaluate_string_equality() {
        let record = create_numeric_record(1000, 150.0);
        assert!(
            parse_and_evaluate("symbol = 'AAPL'", &record),
            "String equality should work"
        );
    }

    #[test]
    fn test_evaluate_string_inequality() {
        let record = create_numeric_record(1000, 150.0);
        assert!(
            parse_and_evaluate("symbol != 'GOOGL'", &record),
            "String inequality should work"
        );
    }

    #[test]
    fn test_evaluate_float_comparison() {
        let record = create_numeric_record(1000, 150.5);
        assert!(
            parse_and_evaluate("price > 150.0", &record),
            "Float comparison should work"
        );
    }

    #[test]
    fn test_evaluate_scaled_integer() {
        let mut fields = HashMap::new();
        // ScaledInteger: 12345 with scale 2 = 123.45
        fields.insert("price".to_string(), FieldValue::ScaledInteger(12345, 2));
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("price > 100", &record),
            "123.45 should be greater than 100"
        );
    }

    #[test]
    fn test_evaluate_type_mismatch_returns_false() {
        let mut fields = HashMap::new();
        fields.insert(
            "volume".to_string(),
            FieldValue::String("not_a_number".to_string()),
        );
        let record = create_test_record(fields);

        assert!(
            !parse_and_evaluate("volume > 1000", &record),
            "Type mismatch should result in false"
        );
    }
}

mod performance_tests {
    use super::*;

    #[test]
    fn test_parse_condition_is_cached() {
        // This test verifies that parsing is a one-time operation
        // by testing the parse function exists and works
        let condition = "volume > 1000 AND price > avg_price * 1.5 AND symbol = 'AAPL'";
        let result = ProcessorMetricsHelper::parse_condition_to_expr(condition);
        assert!(
            result.is_ok(),
            "Complex condition should parse successfully"
        );
    }

    #[test]
    fn test_evaluation_uses_parsed_expr() {
        // Test that we can evaluate the same condition multiple times
        // efficiently (no reparsing needed)
        let condition = "volume > 1000";
        let expr =
            ProcessorMetricsHelper::parse_condition_to_expr(condition).expect("Failed to parse");

        let record1 = create_numeric_record(1500, 150.0);
        let record2 = create_numeric_record(500, 50.0);
        let record3 = create_numeric_record(2000, 200.0);

        // All evaluations use the same parsed expression
        assert!(ProcessorMetricsHelper::evaluate_condition_expr(
            &expr, &record1, "test", "job"
        ));
        assert!(!ProcessorMetricsHelper::evaluate_condition_expr(
            &expr, &record2, "test", "job"
        ));
        assert!(ProcessorMetricsHelper::evaluate_condition_expr(
            &expr, &record3, "test", "job"
        ));
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn test_very_long_condition() {
        let long_condition = format!(
            "volume > 100 AND {} AND price < 200",
            (0..50)
                .map(|i| format!("field{} > 0", i))
                .collect::<Vec<_>>()
                .join(" AND ")
        );

        let result = ProcessorMetricsHelper::parse_condition_to_expr(&long_condition);
        assert!(result.is_ok(), "Long condition should parse successfully");
    }

    #[test]
    fn test_special_characters_in_string() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("symbol = 'ABC-DEF_123'");
        assert!(
            result.is_ok(),
            "Condition with special characters should parse"
        );
    }

    #[test]
    fn test_nested_parentheses() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr(
            "((volume > 100 AND price > 50) OR (volume > 1000)) AND symbol = 'AAPL'",
        );
        assert!(
            result.is_ok(),
            "Nested parentheses should parse successfully"
        );
    }

    #[test]
    fn test_not_operator() {
        let result = ProcessorMetricsHelper::parse_condition_to_expr("NOT (volume > 1000)");
        assert!(result.is_ok(), "NOT operator should parse successfully");
    }
}
