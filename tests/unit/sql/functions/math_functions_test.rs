/*!
# Math Functions Tests

Tests for mathematical functions including ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT.
Tests both parsing and execution functionality.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{
    Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record() -> HashMap<String, InternalValue> {
    let mut record = HashMap::new();
    record.insert("positive_int".to_string(), InternalValue::Integer(42));
    record.insert("negative_int".to_string(), InternalValue::Integer(-15));
    record.insert("positive_float".to_string(), InternalValue::Number(3.14159));
    record.insert("negative_float".to_string(), InternalValue::Number(-2.5));
    record.insert("zero_int".to_string(), InternalValue::Integer(0));
    record.insert("zero_float".to_string(), InternalValue::Number(0.0));
    record.insert("large_float".to_string(), InternalValue::Number(123.456789));
    record.insert("null_value".to_string(), InternalValue::Null);
    record
}

#[tokio::test]
async fn test_abs_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let test_cases = vec![
        // (column_name, expected_result)
        ("positive_int", 42.0),
        ("negative_int", 15.0),
        ("positive_float", 3.14159),
        ("negative_float", 2.5),
        ("zero_int", 0.0),
        ("zero_float", 0.0),
    ];

    for (column_name, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "ABS".to_string(),
                    args: vec![Expr::Column(column_name.to_string())],
                },
                alias: Some("abs_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(
            result.is_ok(),
            "ABS function execution failed for {}",
            column_name
        );

        let output = rx.try_recv().unwrap();
        match output.get("abs_result") {
            Some(InternalValue::Integer(i)) => {
                assert_eq!(*i as f64, expected, "ABS({}) failed", column_name);
            }
            Some(InternalValue::Number(f)) => {
                assert!(
                    (f - expected).abs() < 0.0001,
                    "ABS({}) failed: {} != {}",
                    column_name,
                    f,
                    expected
                );
            }
            _ => panic!("Expected numeric result for ABS({})", column_name),
        }
    }

    // Test NULL case
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "ABS".to_string(),
                args: vec![Expr::Column("null_value".to_string())],
            },
            alias: Some("abs_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    assert!(matches!(
        output.get("abs_result"),
        Some(InternalValue::Null)
    ));
}

#[tokio::test]
async fn test_round_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    // Test ROUND without precision
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "ROUND".to_string(),
                args: vec![Expr::Literal(LiteralValue::Float(3.7))],
            },
            alias: Some("round_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    match output.get("round_result") {
        Some(InternalValue::Number(f)) => {
            assert!(
                (f - 4.0).abs() < 0.0001,
                "ROUND(3.7) should be 4.0, got {}",
                f
            );
        }
        _ => panic!("Expected float result for ROUND"),
    }

    // Test ROUND with precision
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "ROUND".to_string(),
                args: vec![
                    Expr::Literal(LiteralValue::Float(3.14159)),
                    Expr::Literal(LiteralValue::Integer(2)),
                ],
            },
            alias: Some("round_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    match output.get("round_result") {
        Some(InternalValue::Number(f)) => {
            assert!(
                (f - 3.14).abs() < 0.0001,
                "ROUND(3.14159, 2) should be 3.14, got {}",
                f
            );
        }
        _ => panic!("Expected float result for ROUND with precision"),
    }
}

#[tokio::test]
async fn test_ceil_floor_functions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let test_cases = vec![
        ("CEIL", 3.2, 4),
        ("CEILING", 3.2, 4), // Test both aliases
        ("CEIL", -2.8, -2),
        ("FLOOR", 3.8, 3),
        ("FLOOR", -2.2, -3),
    ];

    for (function, input, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function.to_string(),
                    args: vec![Expr::Literal(LiteralValue::Float(input))],
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
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok(), "{}({}) execution failed", function, input);

        let output = rx.try_recv().unwrap();
        match output.get("result") {
            Some(InternalValue::Integer(i)) => {
                assert_eq!(
                    *i, expected,
                    "{}({}) should be {}, got {}",
                    function, input, expected, i
                );
            }
            _ => panic!("Expected integer result for {}({})", function, input),
        }
    }
}

#[tokio::test]
async fn test_mod_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let test_cases = vec![
        // (dividend, divisor, expected)
        (10, 3, 1),
        (15, 4, 3),
        (-10, 3, -1),
        (10, -3, 1),
    ];

    for (dividend, divisor, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "MOD".to_string(),
                    args: vec![
                        Expr::Literal(LiteralValue::Integer(dividend)),
                        Expr::Literal(LiteralValue::Integer(divisor)),
                    ],
                },
                alias: Some("mod_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(
            result.is_ok(),
            "MOD({}, {}) execution failed",
            dividend,
            divisor
        );

        let output = rx.try_recv().unwrap();
        match output.get("mod_result") {
            Some(InternalValue::Integer(i)) => {
                assert_eq!(
                    *i, expected,
                    "MOD({}, {}) should be {}, got {}",
                    dividend, divisor, expected, i
                );
            }
            _ => panic!("Expected integer result for MOD({}, {})", dividend, divisor),
        }
    }

    // Test division by zero
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "MOD".to_string(),
                args: vec![
                    Expr::Literal(LiteralValue::Integer(10)),
                    Expr::Literal(LiteralValue::Integer(0)),
                ],
            },
            alias: Some("mod_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(
        result.is_err(),
        "MOD(10, 0) should fail with division by zero"
    );
}

#[tokio::test]
async fn test_power_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let test_cases = vec![
        // (base, exponent, expected)
        (2, 3, 8.0),
        (5, 2, 25.0),
        (10, 0, 1.0),
        (-2, 3, -8.0),
    ];

    for (base, exponent, expected) in test_cases {
        // Test both POWER and POW aliases
        for function_name in ["POWER", "POW"] {
            let query = StreamingQuery::Select {
                fields: vec![SelectField::Expression {
                    expr: Expr::Function {
                        name: function_name.to_string(),
                        args: vec![
                            Expr::Literal(LiteralValue::Integer(base)),
                            Expr::Literal(LiteralValue::Integer(exponent)),
                        ],
                    },
                    alias: Some("power_result".to_string()),
                }],
                from: StreamSource::Stream("test".to_string()),
                joins: None,
                where_clause: None,
                window: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            };

            let record = create_test_record();
            let result = engine.execute(&query, record).await;
            assert!(
                result.is_ok(),
                "{}({}, {}) execution failed",
                function_name,
                base,
                exponent
            );

            let output = rx.try_recv().unwrap();
            match output.get("power_result") {
                Some(InternalValue::Integer(i)) => {
                    assert_eq!(
                        *i as f64, expected,
                        "{}({}, {}) should be {}, got {}",
                        function_name, base, exponent, expected, i
                    );
                }
                Some(InternalValue::Number(f)) => {
                    assert!(
                        (f - expected).abs() < 0.0001,
                        "{}({}, {}) should be {}, got {}",
                        function_name,
                        base,
                        exponent,
                        expected,
                        f
                    );
                }
                _ => panic!(
                    "Expected numeric result for {}({}, {})",
                    function_name, base, exponent
                ),
            }
        }
    }
}

#[tokio::test]
async fn test_sqrt_function() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let test_cases = vec![
        // (input, expected)
        (4, 2.0),
        (9, 3.0),
        (16, 4.0),
        (25, 5.0),
        (0, 0.0),
    ];

    for (input, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "SQRT".to_string(),
                    args: vec![Expr::Literal(LiteralValue::Integer(input))],
                },
                alias: Some("sqrt_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            joins: None,
            where_clause: None,
            window: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok(), "SQRT({}) execution failed", input);

        let output = rx.try_recv().unwrap();
        match output.get("sqrt_result") {
            Some(InternalValue::Number(f)) => {
                assert!(
                    (f - expected).abs() < 0.0001,
                    "SQRT({}) should be {}, got {}",
                    input,
                    expected,
                    f
                );
            }
            _ => panic!("Expected float result for SQRT({})", input),
        }
    }

    // Test negative number (should fail)
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "SQRT".to_string(),
                args: vec![Expr::Literal(LiteralValue::Integer(-4))],
            },
            alias: Some("sqrt_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
    };

    let record = create_test_record();
    let result = engine.execute(&query, record).await;
    assert!(
        result.is_err(),
        "SQRT(-4) should fail with negative number error"
    );
}

#[tokio::test]
async fn test_math_function_error_cases() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let error_cases = vec![
        // (function_name, args, expected_error_message_contains)
        ("ABS", vec![], "requires exactly one argument"),
        ("ROUND", vec![], "requires 1 or 2 arguments"),
        ("CEIL", vec![], "requires exactly one argument"),
        ("FLOOR", vec![], "requires exactly one argument"),
        ("MOD", vec![], "requires exactly two arguments"),
        ("POWER", vec![], "requires exactly two arguments"),
        ("SQRT", vec![], "requires exactly one argument"),
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
async fn test_math_function_null_handling() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let functions = vec!["ABS", "ROUND", "CEIL", "FLOOR", "SQRT"];

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

    // Test two-argument functions with NULL
    let two_arg_functions = vec!["MOD", "POWER"];

    for function_name in two_arg_functions {
        // First argument NULL
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![
                        Expr::Column("null_value".to_string()),
                        Expr::Literal(LiteralValue::Integer(2)),
                    ],
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
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(
            result.is_ok(),
            "{} with first arg NULL should succeed",
            function_name
        );

        let output = rx.try_recv().unwrap();
        assert!(
            matches!(output.get("result"), Some(InternalValue::Null)),
            "{} with first arg NULL should return NULL",
            function_name
        );

        // Second argument NULL
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: function_name.to_string(),
                    args: vec![
                        Expr::Literal(LiteralValue::Integer(10)),
                        Expr::Column("null_value".to_string()),
                    ],
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
        };

        let record = create_test_record();
        let result = engine.execute(&query, record).await;
        assert!(
            result.is_ok(),
            "{} with second arg NULL should succeed",
            function_name
        );

        let output = rx.try_recv().unwrap();
        assert!(
            matches!(output.get("result"), Some(InternalValue::Null)),
            "{} with second arg NULL should return NULL",
            function_name
        );
    }
}
