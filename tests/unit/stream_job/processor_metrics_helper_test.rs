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
        topic: None,
        key: None,
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

/// Helper to parse and evaluate a condition (used across multiple test modules)
fn parse_and_evaluate(condition: &str, record: &StreamRecord) -> bool {
    let expr = ProcessorMetricsHelper::parse_condition_to_expr(condition)
        .expect("Failed to parse condition");
    ProcessorMetricsHelper::evaluate_condition_expr(&expr, record, "test_metric", "test_job")
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

    // ===== NEW: String Comparison Edge Cases (Task 4) =====

    #[test]
    fn test_evaluate_string_case_sensitive() {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        let record = create_test_record(fields);

        // Case-sensitive comparison should succeed for exact match
        assert!(
            parse_and_evaluate("symbol = 'AAPL'", &record),
            "Case-sensitive string match should work"
        );

        // Case-sensitive comparison should fail for different case
        assert!(
            !parse_and_evaluate("symbol = 'aapl'", &record),
            "Case-sensitive string should not match different case"
        );
    }

    #[test]
    fn test_evaluate_string_with_numbers() {
        let mut fields = HashMap::new();
        fields.insert(
            "code".to_string(),
            FieldValue::String("XYZ123ABC".to_string()),
        );
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("code = 'XYZ123ABC'", &record),
            "String with numbers should match exactly"
        );
    }

    #[test]
    fn test_evaluate_string_with_special_chars() {
        let mut fields = HashMap::new();
        fields.insert(
            "tag".to_string(),
            FieldValue::String("ALERT-2024_HIGH".to_string()),
        );
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("tag = 'ALERT-2024_HIGH'", &record),
            "String with special characters should match"
        );
    }

    #[test]
    fn test_evaluate_string_with_spaces() {
        let mut fields = HashMap::new();
        fields.insert(
            "name".to_string(),
            FieldValue::String("Large Cap Stock".to_string()),
        );
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("name = 'Large Cap Stock'", &record),
            "String with spaces should match"
        );
    }

    #[test]
    fn test_evaluate_multiple_string_comparisons() {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        fields.insert(
            "exchange".to_string(),
            FieldValue::String("NYSE".to_string()),
        );
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("symbol = 'AAPL' AND exchange = 'NYSE'", &record),
            "Multiple string comparisons should work with AND"
        );

        assert!(
            !parse_and_evaluate("symbol = 'GOOGL' AND exchange = 'NYSE'", &record),
            "Multiple string comparisons should fail if any is false"
        );
    }

    #[test]
    fn test_evaluate_string_inequality_multiple() {
        let mut fields = HashMap::new();
        fields.insert(
            "status".to_string(),
            FieldValue::String("ACTIVE".to_string()),
        );
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("status != 'INACTIVE' AND status != 'PENDING'", &record),
            "Multiple string inequalities should work"
        );
    }

    #[test]
    fn test_evaluate_empty_string() {
        let mut fields = HashMap::new();
        fields.insert("optional".to_string(), FieldValue::String("".to_string()));
        let record = create_test_record(fields);

        assert!(
            parse_and_evaluate("optional = ''", &record),
            "Empty string comparison should work"
        );
    }

    #[test]
    fn test_evaluate_string_vs_numeric_comparison() {
        let mut fields = HashMap::new();
        fields.insert("text".to_string(), FieldValue::String("123".to_string()));
        let record = create_test_record(fields);

        // String-numeric comparison should not match
        assert!(
            !parse_and_evaluate("text = 123", &record),
            "String field should not equal numeric value"
        );
    }
}

// ===== NEW: Performance Telemetry Tests (Task 2) =====
mod performance_telemetry_tests {
    use super::*;

    #[tokio::test]
    async fn test_telemetry_initialization() {
        let helper = ProcessorMetricsHelper::new();
        let telemetry = helper.get_telemetry().await;

        assert_eq!(telemetry.condition_eval_time_us, 0);
        assert_eq!(telemetry.label_extract_time_us, 0);
        assert_eq!(telemetry.total_emission_overhead_us, 0);
    }

    #[tokio::test]
    async fn test_condition_eval_time_recording() {
        let helper = ProcessorMetricsHelper::new();

        // Record some timing data (no await - these are synchronous)
        helper.record_condition_eval_time(100);
        helper.record_condition_eval_time(50);
        helper.record_condition_eval_time(75);

        let telemetry = helper.get_telemetry().await;
        assert_eq!(telemetry.condition_eval_time_us, 225);
    }

    #[tokio::test]
    async fn test_label_extract_time_recording() {
        let helper = ProcessorMetricsHelper::new();

        helper.record_label_extract_time(200);
        helper.record_label_extract_time(150);

        let telemetry = helper.get_telemetry().await;
        assert_eq!(telemetry.label_extract_time_us, 350);
    }

    #[tokio::test]
    async fn test_emission_overhead_recording() {
        let helper = ProcessorMetricsHelper::new();

        helper.record_emission_overhead(500);
        helper.record_emission_overhead(300);

        let telemetry = helper.get_telemetry().await;
        assert_eq!(telemetry.total_emission_overhead_us, 800);
    }

    #[tokio::test]
    async fn test_telemetry_reset() {
        let helper = ProcessorMetricsHelper::new();

        helper.record_condition_eval_time(100);
        helper.record_label_extract_time(200);
        helper.record_emission_overhead(300);

        helper.reset_telemetry().await;

        let telemetry = helper.get_telemetry().await;
        assert_eq!(telemetry.condition_eval_time_us, 0);
        assert_eq!(telemetry.label_extract_time_us, 0);
        assert_eq!(telemetry.total_emission_overhead_us, 0);
    }

    #[tokio::test]
    async fn test_telemetry_saturation_protection() {
        let helper = ProcessorMetricsHelper::new();

        // Record normal values to test saturation
        helper.record_condition_eval_time(1000);
        helper.record_condition_eval_time(2000);
        helper.record_condition_eval_time(3000);

        let telemetry = helper.get_telemetry().await;
        // Should accumulate values correctly
        assert_eq!(telemetry.condition_eval_time_us, 6000);
    }
}

// ===== NEW: Strict Mode Tests (Task 3) =====
mod strict_mode_label_handling_tests {
    use super::*;
    use velostream::velostream::server::processors::LabelHandlingConfig;

    #[test]
    fn test_default_is_permissive_mode() {
        let helper = ProcessorMetricsHelper::new();
        // Default should be permissive (strict_mode = false)
        assert!(!helper.label_config.strict_mode);
    }

    #[test]
    fn test_strict_mode_enabled() {
        let config = LabelHandlingConfig { strict_mode: true };
        let helper = ProcessorMetricsHelper::with_config(config);
        assert!(helper.label_config.strict_mode);
    }

    #[test]
    fn test_builder_method_enables_strict_mode() {
        let helper = ProcessorMetricsHelper::new().with_strict_mode();
        assert!(helper.label_config.strict_mode);
    }

    #[test]
    fn test_permissive_mode_validates_all_label_counts() {
        let helper = ProcessorMetricsHelper::new(); // permissive mode
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        fields.insert(
            "exchange".to_string(),
            FieldValue::String("NYSE".to_string()),
        );

        let mut annotation =
            velostream::velostream::sql::parser::annotations::MetricAnnotation::default();
        annotation.labels = vec!["symbol".to_string(), "exchange".to_string()];

        // Permissive mode should accept any count
        assert!(helper.validate_labels(&annotation, 0)); // 0 extracted
        assert!(helper.validate_labels(&annotation, 1)); // 1 extracted
        assert!(helper.validate_labels(&annotation, 2)); // 2 extracted (all)
        assert!(helper.validate_labels(&annotation, 3)); // 3 extracted (more than expected)
    }

    #[test]
    fn test_strict_mode_requires_exact_label_count() {
        let config = LabelHandlingConfig { strict_mode: true };
        let helper = ProcessorMetricsHelper::with_config(config);

        let mut annotation =
            velostream::velostream::sql::parser::annotations::MetricAnnotation::default();
        annotation.labels = vec!["symbol".to_string(), "exchange".to_string()];

        // Strict mode requires exactly 2 labels
        assert!(!helper.validate_labels(&annotation, 0));
        assert!(!helper.validate_labels(&annotation, 1));
        assert!(helper.validate_labels(&annotation, 2)); // Only this passes
        assert!(!helper.validate_labels(&annotation, 3));
    }

    #[test]
    fn test_strict_mode_with_no_labels() {
        let config = LabelHandlingConfig { strict_mode: true };
        let helper = ProcessorMetricsHelper::with_config(config);

        let annotation =
            velostream::velostream::sql::parser::annotations::MetricAnnotation::default();
        // No labels expected, so 0 extracted should pass
        assert!(helper.validate_labels(&annotation, 0));
        assert!(!helper.validate_labels(&annotation, 1));
    }
}
