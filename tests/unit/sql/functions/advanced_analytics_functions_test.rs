/*!
# Advanced Analytics Functions Tests

Tests for advanced analytics functions including PERCENTILE_CONT, PERCENTILE_DISC, CORR,
COVAR_POP, COVAR_SAMP, REGR_SLOPE, REGR_INTERCEPT, and REGR_R2.
Tests both parsing and execution functionality.

Note: In streaming context with single records, these functions return simplified values:
- PERCENTILE functions return approximated values with warnings
- CORR returns 1.0 (perfect correlation placeholder)
- COVAR_POP returns 0.0 (no covariance with single value)
- COVAR_SAMP returns NULL (sample requires multiple values)
- REGR functions return NULL (regression requires multiple points)

In real windowed implementations, these would calculate actual statistics across multiple records.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::ast::{
    Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("price".to_string(), FieldValue::Float(100.5));
    fields.insert("volume".to_string(), FieldValue::Integer(1000));
    fields.insert("high_value".to_string(), FieldValue::Float(250.75));
    fields.insert("low_value".to_string(), FieldValue::Integer(50));
    fields.insert("zero_value".to_string(), FieldValue::Integer(0));
    fields.insert("null_value".to_string(), FieldValue::Null);

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 1,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    }
}

#[tokio::test]
async fn test_percentile_cont_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let test_cases = vec![
        (0.5, 50.0),   // 50th percentile (median)
        (0.25, 25.0),  // 25th percentile
        (0.75, 75.0),  // 75th percentile
        (0.95, 95.0),  // 95th percentile
        (0.0, 0.0),    // 0th percentile
        (1.0, 100.0),  // 100th percentile
    ];

    for (percentile, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "PERCENTILE_CONT".to_string(),
                    args: vec![Expr::Literal(LiteralValue::Float(percentile))],
                },
                alias: Some("percentile_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok(), "PERCENTILE_CONT({}) execution failed", percentile);

        let output = rx.try_recv().unwrap();
        match output.fields.get("percentile_result") {
            Some(FieldValue::Float(f)) => {
                assert!(
                    (*f - expected).abs() < 0.0001,
                    "PERCENTILE_CONT({}) should return {}, got {}",
                    percentile, expected, f
                );
            }
            _ => panic!("Expected float result for PERCENTILE_CONT({})", percentile),
        }
    }
}

#[tokio::test]
async fn test_percentile_disc_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let test_cases = vec![
        (0.5, 50),    // 50th percentile (median)
        (0.25, 25),   // 25th percentile
        (0.95, 95),   // 95th percentile
        (0.0, 0),     // 0th percentile
        (1.0, 100),   // 100th percentile
    ];

    for (percentile, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "PERCENTILE_DISC".to_string(),
                    args: vec![Expr::Literal(LiteralValue::Float(percentile))],
                },
                alias: Some("percentile_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok(), "PERCENTILE_DISC({}) execution failed", percentile);

        let output = rx.try_recv().unwrap();
        match output.fields.get("percentile_result") {
            Some(FieldValue::Integer(i)) => {
                assert_eq!(
                    *i, expected,
                    "PERCENTILE_DISC({}) should return {}, got {}",
                    percentile, expected, i
                );
            }
            _ => panic!("Expected integer result for PERCENTILE_DISC({})", percentile),
        }
    }
}

#[tokio::test]
async fn test_percentile_functions_validation() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let invalid_cases = vec![
        // (function_name, percentile_value, expected_error_substring)
        ("PERCENTILE_CONT", -0.1, "must be between 0 and 1"),
        ("PERCENTILE_CONT", 1.5, "must be between 0 and 1"),
        ("PERCENTILE_DISC", -0.5, "must be between 0 and 1"),
        ("PERCENTILE_DISC", 2.0, "must be between 0 and 1"),
    ];

    for (function_name, percentile, expected_error) in invalid_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![Expr::Literal(LiteralValue::Float(percentile))],
                },
                alias: Some("result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(
            result.is_err(),
            "{}({}) should fail validation",
            function_name, percentile
        );
        let error_msg = format!("{:?}", result.err().unwrap());
        assert!(
            error_msg.contains(expected_error),
            "Error message should contain '{}', got: {}",
            expected_error, error_msg
        );
    }
}

#[tokio::test]
async fn test_corr_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let test_cases = vec![
        // (column1, column2, expected_correlation)
        ("price", "volume", 1.0),      // Perfect correlation placeholder
        ("high_value", "low_value", 1.0), // Perfect correlation placeholder
        ("price", "zero_value", 1.0),  // Perfect correlation placeholder
    ];

    for (col1, col2, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "CORR".to_string(),
                    args: vec![
                        Expr::Column(col1.to_string()),
                        Expr::Column(col2.to_string()),
                    ],
                },
                alias: Some("corr_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok(), "CORR({}, {}) execution failed", col1, col2);

        let output = rx.try_recv().unwrap();
        match output.fields.get("corr_result") {
            Some(FieldValue::Float(f)) => {
                assert!(
                    (*f - expected).abs() < 0.0001,
                    "CORR({}, {}) should return {}, got {}",
                    col1, col2, expected, f
                );
            }
            _ => panic!("Expected float result for CORR({}, {})", col1, col2),
        }
    }
}

#[tokio::test]
async fn test_covar_pop_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let test_cases = vec![
        // (column1, column2, expected_covariance)
        ("price", "volume", 0.0),      // Zero covariance for single record
        ("high_value", "low_value", 0.0), // Zero covariance for single record
    ];

    for (col1, col2, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "COVAR_POP".to_string(),
                    args: vec![
                        Expr::Column(col1.to_string()),
                        Expr::Column(col2.to_string()),
                    ],
                },
                alias: Some("covar_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok(), "COVAR_POP({}, {}) execution failed", col1, col2);

        let output = rx.try_recv().unwrap();
        match output.fields.get("covar_result") {
            Some(FieldValue::Float(f)) => {
                assert!(
                    (*f - expected).abs() < 0.0001,
                    "COVAR_POP({}, {}) should return {}, got {}",
                    col1, col2, expected, f
                );
            }
            _ => panic!("Expected float result for COVAR_POP({}, {})", col1, col2),
        }
    }
}

#[tokio::test]
async fn test_covar_samp_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // COVAR_SAMP should return NULL for single record (sample requires n > 1)
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "COVAR_SAMP".to_string(),
                args: vec![
                    Expr::Column("price".to_string()),
                    Expr::Column("volume".to_string()),
                ],
            },
            alias: Some("covar_samp_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
            from_alias: None,        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let record = create_test_record();
    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok(), "COVAR_SAMP execution failed");

    let output = rx.try_recv().unwrap();
    assert!(
        matches!(output.fields.get("covar_samp_result"), Some(FieldValue::Null)),
        "COVAR_SAMP should return NULL for single record"
    );
}

#[tokio::test]
async fn test_regression_functions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let regression_functions = vec!["REGR_SLOPE", "REGR_INTERCEPT", "REGR_R2"];

    for function_name in regression_functions {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![
                        Expr::Column("price".to_string()),
                        Expr::Column("volume".to_string()),
                    ],
                },
                alias: Some("regr_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok(), "{} execution failed", function_name);

        let output = rx.try_recv().unwrap();
        // All regression functions should return NULL for single record
        assert!(
            matches!(output.fields.get("regr_result"), Some(FieldValue::Null)),
            "{} should return NULL for single record",
            function_name
        );
    }
}

#[tokio::test]
async fn test_analytics_functions_null_handling() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let functions_and_args = vec![
        // (function_name, args, expected_result)
        ("PERCENTILE_CONT", vec![Expr::Column("null_value".to_string())], "null"),
        ("PERCENTILE_DISC", vec![Expr::Column("null_value".to_string())], "null"),
        ("CORR", vec![Expr::Column("null_value".to_string()), Expr::Column("price".to_string())], "null"),
        ("CORR", vec![Expr::Column("price".to_string()), Expr::Column("null_value".to_string())], "null"),
        ("COVAR_POP", vec![Expr::Column("null_value".to_string()), Expr::Column("price".to_string())], "null"),
        ("COVAR_SAMP", vec![Expr::Column("null_value".to_string()), Expr::Column("price".to_string())], "null"),
        ("REGR_SLOPE", vec![Expr::Column("null_value".to_string()), Expr::Column("price".to_string())], "null"),
        ("REGR_INTERCEPT", vec![Expr::Column("null_value".to_string()), Expr::Column("price".to_string())], "null"),
        ("REGR_R2", vec![Expr::Column("null_value".to_string()), Expr::Column("price".to_string())], "null"),
    ];

    for (function_name, args, expected_result) in functions_and_args {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args,
                },
                alias: Some("result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok(), "{} with NULL should succeed", function_name);

        let output = rx.try_recv().unwrap();
        match expected_result {
            "null" => {
                assert!(
                    matches!(output.fields.get("result"), Some(FieldValue::Null)),
                    "{} with NULL should return NULL",
                    function_name
                );
            }
            _ => panic!("Unexpected expected result: {}", expected_result),
        }
    }
}

#[tokio::test]
async fn test_analytics_functions_error_cases() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let error_cases = vec![
        // (function_name, args, expected_error_message_contains)
        ("PERCENTILE_CONT", vec![], "requires exactly one argument"),
        ("PERCENTILE_DISC", vec![], "requires exactly one argument"),
        ("CORR", vec![Expr::Column("price".to_string())], "requires exactly two arguments"),
        ("COVAR_POP", vec![Expr::Column("price".to_string())], "requires exactly two arguments"),
        ("COVAR_SAMP", vec![Expr::Column("price".to_string())], "requires exactly two arguments"),
        ("REGR_SLOPE", vec![Expr::Column("price".to_string())], "requires exactly two arguments"),
        ("REGR_INTERCEPT", vec![Expr::Column("price".to_string())], "requires exactly two arguments"),
        ("REGR_R2", vec![Expr::Column("price".to_string())], "requires exactly two arguments"),
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
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
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
async fn test_multiple_analytics_functions_in_single_query() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Test using multiple analytics functions in one query
    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Expression {
                expr: Expr::Function {
                    name: "PERCENTILE_CONT".to_string(),
                    args: vec![Expr::Literal(LiteralValue::Float(0.5))],
                },
                alias: Some("median".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "CORR".to_string(),
                    args: vec![
                        Expr::Column("price".to_string()),
                        Expr::Column("volume".to_string()),
                    ],
                },
                alias: Some("correlation".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COVAR_POP".to_string(),
                    args: vec![
                        Expr::Column("price".to_string()),
                        Expr::Column("volume".to_string()),
                    ],
                },
                alias: Some("covariance".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "REGR_SLOPE".to_string(),
                    args: vec![
                        Expr::Column("price".to_string()),
                        Expr::Column("volume".to_string()),
                    ],
                },
                alias: Some("slope".to_string()),
            },
        ],
        from: StreamSource::Stream("test".to_string()),
            from_alias: None,        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let record = create_test_record();
    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok(), "Multiple analytics functions failed: {:?}", result.unwrap_err());

    let output = rx.try_recv().unwrap();

    // Check PERCENTILE_CONT result (median = 50th percentile)
    match output.fields.get("median") {
        Some(FieldValue::Float(f)) => {
            assert!(
                (*f - 50.0).abs() < 0.0001,
                "PERCENTILE_CONT(0.5) should return 50.0, got {}",
                f
            );
        }
        _ => panic!("Expected float result for PERCENTILE_CONT"),
    }

    // Check CORR result (perfect correlation placeholder)
    match output.fields.get("correlation") {
        Some(FieldValue::Float(f)) => {
            assert!(
                (*f - 1.0).abs() < 0.0001,
                "CORR should return 1.0, got {}",
                f
            );
        }
        _ => panic!("Expected float result for CORR"),
    }

    // Check COVAR_POP result (zero covariance for single record)
    match output.fields.get("covariance") {
        Some(FieldValue::Float(f)) => {
            assert!(
                (*f - 0.0).abs() < 0.0001,
                "COVAR_POP should return 0.0, got {}",
                f
            );
        }
        _ => panic!("Expected float result for COVAR_POP"),
    }

    // Check REGR_SLOPE result (NULL for single record)
    assert!(
        matches!(output.fields.get("slope"), Some(FieldValue::Null)),
        "REGR_SLOPE should return NULL for single record"
    );
}

#[tokio::test]
async fn test_analytics_functions_with_literal_values() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let test_cases = vec![
        // (function_name, args, expected_result_type, expected_value)
        ("PERCENTILE_CONT", vec![Expr::Literal(LiteralValue::Float(0.75))], "float", 75.0),
        ("PERCENTILE_DISC", vec![Expr::Literal(LiteralValue::Float(0.25))], "integer", 25.0),
        ("CORR", vec![Expr::Literal(LiteralValue::Integer(100)), Expr::Literal(LiteralValue::Integer(200))], "float", 1.0),
        ("COVAR_POP", vec![Expr::Literal(LiteralValue::Float(50.5)), Expr::Literal(LiteralValue::Integer(100))], "float", 0.0),
    ];

    for (function_name, args, expected_type, expected_value) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args,
                },
                alias: Some("result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        };

        let record = create_test_record();
        let result = engine.execute_with_record(&query, record).await;
        assert!(
            result.is_ok(),
            "{} with literals should succeed",
            function_name
        );

        let output = rx.try_recv().unwrap();
        match (output.fields.get("result"), expected_type) {
            (Some(FieldValue::Integer(i)), "integer") => {
                assert_eq!(
                    *i as f64, expected_value,
                    "{} should return {}, got {}",
                    function_name, expected_value, i
                );
            }
            (Some(FieldValue::Float(f)), "float") => {
                assert!(
                    (*f - expected_value).abs() < 0.0001,
                    "{} should return {}, got {}",
                    function_name, expected_value, f
                );
            }
            _ => panic!(
                "Expected {} result for {}",
                expected_type, function_name
            ),
        }
    }
}