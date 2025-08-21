/*!
# Statistical Functions Tests

Tests for statistical functions including STDDEV, STDDEV_SAMP, STDDEV_POP, VARIANCE, VAR_SAMP, VAR_POP, MEDIAN.
Tests both parsing and execution functionality.

Note: In streaming context with single records, these functions return simplified values:
- STDDEV functions return 0.0 (no variance with single value)
- VARIANCE functions return 0.0 (no variance with single value)
- MEDIAN returns the value itself (median of single value is the value)

In real windowed implementations, these would calculate actual statistics across multiple records.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{
    Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use std::collections::HashMap;
use std::f64::consts::PI;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record() -> HashMap<String, InternalValue> {
    let mut record = HashMap::new();
    record.insert("value_int".to_string(), InternalValue::Integer(42));
    record.insert("value_float".to_string(), InternalValue::Number(PI));
    record.insert("negative_value".to_string(), InternalValue::Integer(-10));
    record.insert("zero_value".to_string(), InternalValue::Integer(0));
    record.insert("null_value".to_string(), InternalValue::Null);
    record.insert("large_value".to_string(), InternalValue::Number(1000.5));
    record
}

#[tokio::test]
async fn test_stddev_functions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let functions = vec!["STDDEV", "STDDEV_SAMP"]; // Test both aliases

    for function_name in functions {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![Expr::Column("value_int".to_string())],
                },
                alias: Some("stddev_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok(), "{} execution failed", function_name);

        let output = rx.try_recv().unwrap();
        match output.get("stddev_result") {
            Some(InternalValue::Number(f)) => {
                // In streaming single-record context, stddev should be 0.0
                assert!(
                    (*f - 0.0).abs() < 0.0001,
                    "{} should return 0.0 for single record, got {}",
                    function_name,
                    f
                );
            }
            _ => panic!("Expected float result for {}", function_name),
        }
    }
}

#[tokio::test]
async fn test_stddev_pop_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "STDDEV_POP".to_string(),
                args: vec![Expr::Column("value_float".to_string())],
            },
            alias: Some("stddev_pop_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(result.is_ok(), "STDDEV_POP execution failed");

    let output = rx.try_recv().unwrap();
    match output.get("stddev_pop_result") {
        Some(InternalValue::Number(f)) => {
            // In streaming single-record context, stddev_pop should be 0.0
            assert!(
                (*f - 0.0).abs() < 0.0001,
                "STDDEV_POP should return 0.0 for single record, got {}",
                f
            );
        }
        _ => panic!("Expected float result for STDDEV_POP"),
    }
}

#[tokio::test]
async fn test_variance_functions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let functions = vec!["VARIANCE", "VAR_SAMP"]; // Test both aliases

    for function_name in functions {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![Expr::Column("large_value".to_string())],
                },
                alias: Some("variance_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok(), "{} execution failed", function_name);

        let output = rx.try_recv().unwrap();
        match output.get("variance_result") {
            Some(InternalValue::Number(f)) => {
                // In streaming single-record context, variance should be 0.0
                assert!(
                    (*f - 0.0).abs() < 0.0001,
                    "{} should return 0.0 for single record, got {}",
                    function_name,
                    f
                );
            }
            _ => panic!("Expected float result for {}", function_name),
        }
    }
}

#[tokio::test]
async fn test_var_pop_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "VAR_POP".to_string(),
                args: vec![Expr::Column("negative_value".to_string())],
            },
            alias: Some("var_pop_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(result.is_ok(), "VAR_POP execution failed");

    let output = rx.try_recv().unwrap();
    match output.get("var_pop_result") {
        Some(InternalValue::Number(f)) => {
            // In streaming single-record context, var_pop should be 0.0
            assert!(
                (*f - 0.0).abs() < 0.0001,
                "VAR_POP should return 0.0 for single record, got {}",
                f
            );
        }
        _ => panic!("Expected float result for VAR_POP"),
    }
}

#[tokio::test]
async fn test_median_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let test_cases = vec![
        // (column_name, expected_type, expected_value)
        ("value_int", "integer", 42.0),
        ("value_float", "float", PI),
        ("negative_value", "integer", -10.0),
        ("zero_value", "integer", 0.0),
        ("large_value", "float", 1000.5),
    ];

    for (column_name, expected_type, expected_value) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "MEDIAN".to_string(),
                    args: vec![Expr::Column(column_name.to_string())],
                },
                alias: Some("median_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(
            result.is_ok(),
            "MEDIAN execution failed for {}: {:?}",
            column_name,
            result.unwrap_err()
        );

        let output = rx.try_recv().unwrap();
        match (output.get("median_result"), expected_type) {
            (Some(InternalValue::Integer(i)), "integer") => {
                assert_eq!(
                    *i as f64, expected_value,
                    "MEDIAN({}) should return {}, got {}",
                    column_name, expected_value, i
                );
            }
            (Some(InternalValue::Number(f)), "float") => {
                assert!(
                    (*f - expected_value).abs() < 0.0001,
                    "MEDIAN({}) should return {}, got {}",
                    column_name,
                    expected_value,
                    f
                );
            }
            _ => panic!(
                "Expected {} result for MEDIAN({})",
                expected_type, column_name
            ),
        }
    }
}

#[tokio::test]
async fn test_statistical_function_null_handling() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let functions = vec![
        "STDDEV",
        "STDDEV_SAMP",
        "STDDEV_POP",
        "VARIANCE",
        "VAR_SAMP",
        "VAR_POP",
        "MEDIAN",
    ];

    for function_name in functions {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![Expr::Column("null_value".to_string())],
                },
                alias: Some("result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok(), "{} with NULL should succeed", function_name);

        let output = rx.try_recv().unwrap();
        assert!(
            matches!(output.get("result"), Some(InternalValue::Null)),
            "{} with NULL should return NULL",
            function_name
        );
    }
}

#[tokio::test]
async fn test_statistical_function_error_cases() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let error_cases = vec![
        // (function_name, args, expected_error_message_contains)
        ("STDDEV", vec![], "requires exactly one argument"),
        ("STDDEV_SAMP", vec![], "requires exactly one argument"),
        ("STDDEV_POP", vec![], "requires exactly one argument"),
        ("VARIANCE", vec![], "requires exactly one argument"),
        ("VAR_SAMP", vec![], "requires exactly one argument"),
        ("VAR_POP", vec![], "requires exactly one argument"),
        ("MEDIAN", vec![], "requires exactly one argument"),
    ];

    for (function_name, args, expected_error) in error_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args,
                },
                alias: Some("result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(
            result.is_err(),
            "{} with wrong args should fail",
            function_name
        );
        let error_msg = format!("{:?}", result.err().unwrap());
        assert!(
            error_msg.contains(expected_error),
            "Error message should contain '{}', got: {}",
            expected_error,
            error_msg
        );
    }
}

#[tokio::test]
async fn test_statistical_functions_with_non_numeric_types() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let functions = vec![
        "STDDEV",
        "STDDEV_SAMP",
        "STDDEV_POP",
        "VARIANCE",
        "VAR_SAMP",
        "VAR_POP",
        "MEDIAN",
    ];

    // Create record with non-numeric field
    let mut record = HashMap::new();
    record.insert(
        "string_field".to_string(),
        InternalValue::String("not_a_number".to_string()),
    );

    for function_name in functions {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![Expr::Column("string_field".to_string())],
                },
                alias: Some("result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
        };

        let result = engine.execute(&query, record.clone()).await;
        assert!(
            result.is_err(),
            "{} with non-numeric argument should fail",
            function_name
        );
        let error_msg = format!("{:?}", result.err().unwrap());
        assert!(
            error_msg.contains("requires numeric argument") || error_msg.contains("numeric"),
            "Error message should mention numeric requirement for {}, got: {}",
            function_name,
            error_msg
        );
    }
}

#[tokio::test]
async fn test_statistical_functions_with_literal_values() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    // Test with literal values instead of columns
    let test_cases = vec![
        ("STDDEV", LiteralValue::Integer(100), 0.0),
        ("VARIANCE", LiteralValue::Float(25.5), 0.0),
        ("MEDIAN", LiteralValue::Integer(50), 50.0),
        ("MEDIAN", LiteralValue::Float(7.5), 7.5),
    ];

    for (function_name, literal_value, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![Expr::Literal(literal_value.clone())],
                },
                alias: Some("result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(
            result.is_ok(),
            "{} with literal {:?} should succeed",
            function_name,
            literal_value
        );

        let output = rx.try_recv().unwrap();
        match output.get("result") {
            Some(InternalValue::Integer(i)) => {
                assert_eq!(
                    *i as f64, expected,
                    "{} with literal {:?} should return {}, got {}",
                    function_name, literal_value, expected, i
                );
            }
            Some(InternalValue::Number(f)) => {
                assert!(
                    (*f - expected).abs() < 0.0001,
                    "{} with literal {:?} should return {}, got {}",
                    function_name,
                    literal_value,
                    expected,
                    f
                );
            }
            _ => panic!(
                "Expected numeric result for {} with literal {:?}",
                function_name, literal_value
            ),
        }
    }
}

#[tokio::test]
async fn test_multiple_statistical_functions_in_single_query() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    // Test using multiple statistical functions in one query
    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Expression {
                expr: Expr::Function {
                    name: "STDDEV".to_string(),
                    args: vec![Expr::Column("value_int".to_string())],
                },
                alias: Some("stddev_result".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "VARIANCE".to_string(),
                    args: vec![Expr::Column("value_int".to_string())],
                },
                alias: Some("variance_result".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "MEDIAN".to_string(),
                    args: vec![Expr::Column("value_int".to_string())],
                },
                alias: Some("median_result".to_string()),
            },
        ],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(result.is_ok(), "err:{:}", result.unwrap_err());

    let output = rx.try_recv().unwrap();

    // Check STDDEV result
    match output.get("stddev_result") {
        Some(InternalValue::Number(f)) => {
            assert!(
                (*f - 0.0).abs() < 0.0001,
                "STDDEV should return 0.0, got {}",
                f
            );
        }
        _ => panic!("Expected float result for STDDEV"),
    }

    // Check VARIANCE result
    match output.get("variance_result") {
        Some(InternalValue::Number(f)) => {
            assert!(
                (*f - 0.0).abs() < 0.0001,
                "VARIANCE should return 0.0, got {}",
                f
            );
        }
        _ => panic!("Expected float result for VARIANCE"),
    }

    // Check MEDIAN result
    match output.get("median_result") {
        Some(InternalValue::Integer(i)) => {
            assert_eq!(*i, 42, "MEDIAN should return 42, got {}", i);
        }
        _ => panic!("Expected integer result for MEDIAN"),
    }
}
