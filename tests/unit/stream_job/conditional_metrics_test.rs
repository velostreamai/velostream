// Unit tests for conditional metric evaluation in SimpleJobProcessor
//
// These tests verify the condition parsing, evaluation, and edge case handling
// for the Phase 4 conditional metrics feature.

use std::collections::HashMap;
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
    use velostream::velostream::server::processors::common::JobProcessingConfig;
    use velostream::velostream::server::processors::simple::SimpleJobProcessor;

    #[test]
    fn test_parse_simple_condition() {
        let result = SimpleJobProcessor::parse_condition_to_expr("volume > 1000");
        assert!(result.is_ok(), "Simple condition should parse successfully");
    }

    #[test]
    fn test_parse_complex_condition() {
        let result = SimpleJobProcessor::parse_condition_to_expr("volume > 1000 AND price > 100");
        assert!(
            result.is_ok(),
            "Complex AND condition should parse successfully"
        );
    }

    #[test]
    fn test_parse_condition_with_multiplication() {
        let result = SimpleJobProcessor::parse_condition_to_expr("volume > avg_volume * 2");
        assert!(
            result.is_ok(),
            "Condition with multiplication should parse successfully"
        );
    }

    #[test]
    fn test_parse_invalid_syntax() {
        let result = SimpleJobProcessor::parse_condition_to_expr("volume > > 1000");
        assert!(result.is_err(), "Invalid syntax should return error");
    }

    #[test]
    fn test_parse_empty_condition() {
        let result = SimpleJobProcessor::parse_condition_to_expr("");
        assert!(result.is_err(), "Empty condition should return error");
    }

    #[test]
    fn test_parse_whitespace_only() {
        let result = SimpleJobProcessor::parse_condition_to_expr("   ");
        assert!(
            result.is_err(),
            "Whitespace-only condition should return error"
        );
    }

    #[test]
    fn test_parse_condition_with_null() {
        let result = SimpleJobProcessor::parse_condition_to_expr("field IS NULL");
        assert!(
            result.is_ok(),
            "NULL check condition should parse successfully"
        );
    }

    #[test]
    fn test_parse_condition_with_not_null() {
        let result = SimpleJobProcessor::parse_condition_to_expr("field IS NOT NULL");
        assert!(
            result.is_ok(),
            "NOT NULL check condition should parse successfully"
        );
    }

    #[test]
    fn test_parse_condition_with_between() {
        let result = SimpleJobProcessor::parse_condition_to_expr("volume BETWEEN 100 AND 1000");
        assert!(
            result.is_ok(),
            "BETWEEN condition should parse successfully"
        );
    }

    #[test]
    fn test_parse_condition_with_in() {
        let result =
            SimpleJobProcessor::parse_condition_to_expr("symbol IN ('AAPL', 'GOOGL', 'MSFT')");
        assert!(result.is_ok(), "IN condition should parse successfully");
    }
}

mod evaluate_condition_expr_tests {
    use super::*;
    use velostream::velostream::server::processors::simple::SimpleJobProcessor;
    use velostream::velostream::sql::parser::StreamingSqlParser;

    /// Helper to parse a condition and evaluate it
    fn parse_and_evaluate(condition: &str, record: &StreamRecord) -> bool {
        let expr = SimpleJobProcessor::parse_condition_to_expr(condition)
            .expect("Failed to parse condition");

        SimpleJobProcessor::evaluate_condition_expr(&expr, record, "test_metric", "test_job")
    }

    #[test]
    fn test_evaluate_simple_comparison_true() {
        let record = create_numeric_record(1500, 150.0);
        assert!(
            parse_and_evaluate("volume > 1000", &record),
            "1500 > 1000 should be true"
        );
    }

    #[test]
    fn test_evaluate_simple_comparison_false() {
        let record = create_numeric_record(500, 150.0);
        assert!(
            !parse_and_evaluate("volume > 1000", &record),
            "500 > 1000 should be false"
        );
    }

    #[test]
    fn test_evaluate_complex_and_condition_true() {
        let record = create_numeric_record(1500, 150.0);
        assert!(
            parse_and_evaluate("volume > 1000 AND price > 100", &record),
            "Both conditions should be true"
        );
    }

    #[test]
    fn test_evaluate_complex_and_condition_false() {
        let record = create_numeric_record(1500, 50.0);
        assert!(
            !parse_and_evaluate("volume > 1000 AND price > 100", &record),
            "Price condition should make this false"
        );
    }

    #[test]
    fn test_evaluate_complex_or_condition() {
        let record = create_numeric_record(500, 150.0);
        assert!(
            parse_and_evaluate("volume > 1000 OR price > 100", &record),
            "Price condition should make this true"
        );
    }

    #[test]
    fn test_evaluate_with_missing_field() {
        let record = create_numeric_record(1500, 150.0);
        // 'nonexistent_field' doesn't exist in the record
        // Should return false (evaluation error)
        assert!(
            !parse_and_evaluate("nonexistent_field > 100", &record),
            "Missing field should result in false"
        );
    }

    #[test]
    fn test_evaluate_null_comparison() {
        let mut fields = HashMap::new();
        fields.insert("nullable_field".to_string(), FieldValue::Null);
        fields.insert("volume".to_string(), FieldValue::Integer(1000));
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("nullable_field IS NULL", &record),
            "NULL field should be detected"
        );
    }

    #[test]
    fn test_evaluate_not_null_comparison() {
        let mut fields = HashMap::new();
        fields.insert("volume".to_string(), FieldValue::Integer(1000));
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("volume IS NOT NULL", &record),
            "Non-NULL field should be detected"
        );
    }

    #[test]
    fn test_evaluate_between_condition() {
        let record = create_numeric_record(500, 150.0);
        assert!(
            parse_and_evaluate("volume BETWEEN 100 AND 1000", &record),
            "500 should be between 100 and 1000"
        );
    }

    #[test]
    fn test_evaluate_string_equality() {
        let record = create_numeric_record(1000, 150.0);
        assert!(
            parse_and_evaluate("symbol = 'AAPL'", &record),
            "Symbol should equal AAPL"
        );
    }

    #[test]
    fn test_evaluate_string_inequality() {
        let record = create_numeric_record(1000, 150.0);
        assert!(
            parse_and_evaluate("symbol != 'GOOGL'", &record),
            "Symbol should not equal GOOGL"
        );
    }

    #[test]
    fn test_evaluate_float_comparison() {
        let record = create_numeric_record(1000, 150.5);
        assert!(
            parse_and_evaluate("price > 150.0", &record),
            "150.5 should be greater than 150.0"
        );
    }

    #[test]
    fn test_evaluate_multiplication_in_condition() {
        let mut fields = HashMap::new();
        fields.insert("volume".to_string(), FieldValue::Integer(2500));
        fields.insert("avg_volume".to_string(), FieldValue::Integer(1000));
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("volume > avg_volume * 2", &record),
            "2500 should be greater than 1000 * 2"
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

        // Comparing string to number should fail evaluation
        assert!(
            !parse_and_evaluate("volume > 1000", &record),
            "Type mismatch should result in false"
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
}

mod edge_cases {
    use super::*;
    use velostream::velostream::server::processors::simple::SimpleJobProcessor;

    #[test]
    fn test_parse_very_long_condition() {
        let long_condition = format!(
            "volume > 100 AND {} AND price < 200",
            (0..50)
                .map(|i| format!("field{} > 0", i))
                .collect::<Vec<_>>()
                .join(" AND ")
        );

        let result = SimpleJobProcessor::parse_condition_to_expr(&long_condition);
        assert!(result.is_ok(), "Long condition should parse successfully");
    }

    #[test]
    fn test_parse_condition_with_special_characters_in_string() {
        let result = SimpleJobProcessor::parse_condition_to_expr("symbol = 'ABC-DEF_123'");
        assert!(
            result.is_ok(),
            "Condition with special characters should parse"
        );
    }

    #[test]
    fn test_parse_condition_with_parentheses() {
        let result = SimpleJobProcessor::parse_condition_to_expr(
            "(volume > 100 AND price > 50) OR symbol = 'AAPL'",
        );
        assert!(result.is_ok(), "Condition with parentheses should parse");
    }

    #[test]
    fn test_parse_condition_with_not() {
        let result = SimpleJobProcessor::parse_condition_to_expr("NOT (volume > 1000)");
        assert!(result.is_ok(), "Condition with NOT should parse");
    }
}

mod documentation_examples {
    use super::*;
    use velostream::velostream::server::processors::simple::SimpleJobProcessor;

    /// Test examples from the documentation to ensure they work

    #[test]
    fn test_doc_example_simple_condition() {
        // From docs: volume > 1000
        let result = SimpleJobProcessor::parse_condition_to_expr("volume > 1000");
        assert!(result.is_ok(), "Documentation example should parse");
    }

    #[test]
    fn test_doc_example_complex_condition() {
        // From docs: volume > avg_volume * 2 AND price > 100
        let result =
            SimpleJobProcessor::parse_condition_to_expr("volume > avg_volume * 2 AND price > 100");
        assert!(result.is_ok(), "Documentation example should parse");
    }

    #[test]
    fn test_doc_example_execution() {
        let mut fields = HashMap::new();
        fields.insert("volume".to_string(), FieldValue::Integer(2500));
        fields.insert("avg_volume".to_string(), FieldValue::Integer(1000));
        fields.insert("price".to_string(), FieldValue::Float(150.0));
        let record = create_test_record(fields);

        let expr =
            SimpleJobProcessor::parse_condition_to_expr("volume > avg_volume * 2 AND price > 100")
                .expect("Should parse");

        let result =
            SimpleJobProcessor::evaluate_condition_expr(&expr, &record, "test_metric", "test_job");

        assert!(result, "Documentation example should evaluate to true");
    }
}
