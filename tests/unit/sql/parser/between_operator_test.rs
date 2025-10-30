// Tests for SQL BETWEEN operator
use std::collections::HashMap;
use velostream::velostream::sql::{
    ast::{BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery},
    execution::{expression::evaluator::ExpressionEvaluator, types::FieldValue, StreamRecord},
    parser::StreamingSqlParser,
};

#[test]
fn test_parse_between_operator() {
    let parser = StreamingSqlParser::new();

    let sql = "SELECT * FROM test_stream WHERE price BETWEEN 10 AND 100";
    let result = parser.parse(sql).unwrap();

    match result {
        StreamingQuery::Select { where_clause, .. } => {
            assert!(where_clause.is_some());
            match where_clause.unwrap() {
                Expr::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    assert!(!negated);
                    match expr.as_ref() {
                        Expr::Column(name) => assert_eq!(name, "price"),
                        _ => panic!("Expected column expression"),
                    }
                    match low.as_ref() {
                        Expr::Literal(LiteralValue::Integer(10)) => {}
                        _ => panic!("Expected low value 10"),
                    }
                    match high.as_ref() {
                        Expr::Literal(LiteralValue::Integer(100)) => {}
                        _ => panic!("Expected high value 100"),
                    }
                }
                _ => panic!("Expected BETWEEN expression"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_parse_not_between_operator() {
    let parser = StreamingSqlParser::new();

    let sql = "SELECT * FROM test_stream WHERE amount NOT BETWEEN 50.0 AND 200.0";
    let result = parser.parse(sql).unwrap();

    match result {
        StreamingQuery::Select { where_clause, .. } => {
            assert!(where_clause.is_some());
            match where_clause.unwrap() {
                Expr::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    assert!(negated); // NOT BETWEEN should be negated
                    match expr.as_ref() {
                        Expr::Column(name) => assert_eq!(name, "amount"),
                        _ => panic!("Expected column expression"),
                    }
                    match low.as_ref() {
                        Expr::Literal(LiteralValue::Decimal(s)) => assert_eq!(s, "50.0"),
                        _ => panic!("Expected low value 50.0"),
                    }
                    match high.as_ref() {
                        Expr::Literal(LiteralValue::Decimal(s)) => assert_eq!(s, "200.0"),
                        _ => panic!("Expected high value 200.0"),
                    }
                }
                _ => panic!("Expected BETWEEN expression"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_evaluate_between_integer_in_range() {
    let mut fields = HashMap::new();
    fields.insert("value".to_string(), FieldValue::Integer(50));
    let record = StreamRecord::new(fields);

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // 50 is between 10 and 100
}

#[test]
fn test_evaluate_between_integer_out_of_range() {
    let mut fields = HashMap::new();
    fields.insert("value".to_string(), FieldValue::Integer(150));
    let record = StreamRecord::new(fields);

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(!result); // 150 is not between 10 and 100
}

#[test]
fn test_evaluate_between_integer_boundary_values() {
    let mut record = StreamRecord::new(HashMap::new());

    // Test lower boundary
    record
        .fields
        .insert("value".to_string(), FieldValue::Integer(10));
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };
    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // 10 should be included (boundary)

    // Test upper boundary
    record
        .fields
        .insert("value".to_string(), FieldValue::Integer(100));
    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // 100 should be included (boundary)
}

#[test]
fn test_evaluate_not_between_operator() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("value".to_string(), FieldValue::Integer(50));

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: true, // NOT BETWEEN
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(!result); // NOT (50 between 10 and 100) = false

    // Test with value outside range
    record
        .fields
        .insert("value".to_string(), FieldValue::Integer(150));
    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // NOT (150 between 10 and 100) = true
}

#[test]
fn test_evaluate_between_float_values() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("price".to_string(), FieldValue::Float(75.5));

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("price".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Float(50.0))),
        high: Box::new(Expr::Literal(LiteralValue::Float(100.0))),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // 75.5 is between 50.0 and 100.0
}

#[test]
fn test_evaluate_between_string_values() {
    let mut record = StreamRecord::new(HashMap::new());
    record.fields.insert(
        "name".to_string(),
        FieldValue::String("Charlie".to_string()),
    );

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("name".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::String("Alice".to_string()))),
        high: Box::new(Expr::Literal(LiteralValue::String("David".to_string()))),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // "Charlie" is lexicographically between "Alice" and "David"

    // Test with value outside range
    record
        .fields
        .insert("name".to_string(), FieldValue::String("Zoe".to_string()));
    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(!result); // "Zoe" is not between "Alice" and "David"
}

#[test]
fn test_evaluate_between_scaled_integer_values() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("amount".to_string(), FieldValue::ScaledInteger(7550, 2)); // $75.50

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("amount".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Decimal("50.00".to_string()))), // $50.00
        high: Box::new(Expr::Literal(LiteralValue::Decimal("100.00".to_string()))), // $100.00
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // $75.50 is between $50.00 and $100.00
}

#[test]
fn test_evaluate_between_different_scale_integers() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("amount".to_string(), FieldValue::ScaledInteger(7550, 2)); // $75.50 (scale 2)

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("amount".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Decimal("50.0".to_string()))), // $50.0 (scale 1)
        high: Box::new(Expr::Literal(LiteralValue::Decimal("100.000".to_string()))), // $100.000 (scale 3)
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // Should handle different scales correctly
}

#[test]
fn test_evaluate_between_mixed_types() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("value".to_string(), FieldValue::Integer(75));

    // Test integer between floats (should convert to common type)
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Float(50.0))),
        high: Box::new(Expr::Literal(LiteralValue::Float(100.0))),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // 75 should be between 50.0 and 100.0
}

#[test]
fn test_evaluate_between_with_null_values() {
    let mut record = StreamRecord::new(HashMap::new());
    record.fields.insert("value".to_string(), FieldValue::Null);

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(!result); // NULL BETWEEN should return false

    // Test with NULL bounds
    record
        .fields
        .insert("value".to_string(), FieldValue::Integer(50));
    let expr_null_low = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Null)),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr_null_low, &record).unwrap();
    assert!(!result); // Any NULL bound should return false
}

#[test]
fn test_evaluate_between_with_expressions() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("value".to_string(), FieldValue::Integer(50));
    record
        .fields
        .insert("min_val".to_string(), FieldValue::Integer(10));
    record
        .fields
        .insert("max_val".to_string(), FieldValue::Integer(100));

    // Test BETWEEN with column expressions for bounds
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Column("min_val".to_string())),
        high: Box::new(Expr::Column("max_val".to_string())),
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // 50 is between 10 and 100
}

#[test]
fn test_between_operator_precedence() {
    let parser = StreamingSqlParser::new();

    // Test that BETWEEN has proper precedence with other operators
    let sql = "SELECT * FROM test_stream WHERE price BETWEEN 10 AND 100 AND active = true";
    let result = parser.parse(sql);
    assert!(result.is_ok()); // Should parse correctly with proper precedence
}

#[test]
fn test_between_with_arithmetic_expressions() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("base_price".to_string(), FieldValue::Integer(80));

    // Test BETWEEN with arithmetic in the bounds
    let sql = "SELECT * FROM test_stream WHERE base_price BETWEEN 50 + 10 AND 200 - 100";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(sql);
    assert!(result.is_ok()); // Should parse arithmetic expressions in bounds
}

#[test]
fn test_complex_between_query() {
    let parser = StreamingSqlParser::new();

    // Test complex query with multiple BETWEEN clauses
    let sql = r"
        SELECT customer_id, amount 
        FROM transactions 
        WHERE amount BETWEEN 100.0 AND 1000.0 
        AND transaction_date BETWEEN '2023-01-01' AND '2023-12-31'
        AND customer_id NOT BETWEEN 1000 AND 2000
    ";

    let result = parser.parse(sql);
    assert!(result.is_ok()); // Should handle complex BETWEEN queries
}

#[test]
fn test_between_error_handling() {
    let parser = StreamingSqlParser::new();

    // Test malformed BETWEEN (missing AND)
    let sql = "SELECT * FROM test_stream WHERE price BETWEEN 10 100";
    let result = parser.parse(sql);
    assert!(result.is_err()); // Should fail without AND

    // Test BETWEEN without bounds
    let sql2 = "SELECT * FROM test_stream WHERE price BETWEEN";
    let result2 = parser.parse(sql2);
    assert!(result2.is_err()); // Should fail without bounds
}

#[test]
fn test_financial_precision_between() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("price".to_string(), FieldValue::ScaledInteger(9999, 2)); // $99.99

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("price".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Decimal("50.00".to_string()))), // $50.00
        high: Box::new(Expr::Literal(LiteralValue::Decimal("100.00".to_string()))), // $100.00
        negated: false,
    };

    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // $99.99 is between $50.00 and $100.00

    // Test boundary case with exact precision
    record
        .fields
        .insert("price".to_string(), FieldValue::ScaledInteger(10000, 2)); // $100.00
    let result = ExpressionEvaluator::evaluate_expression(&expr, &record).unwrap();
    assert!(result); // $100.00 should be included (boundary)
}

#[test]
fn test_between_value_evaluation() {
    let mut record = StreamRecord::new(HashMap::new());
    record
        .fields
        .insert("score".to_string(), FieldValue::Integer(85));

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("score".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(70))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(90))),
        negated: false,
    };

    // Test value evaluation (should return boolean FieldValue)
    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    match result {
        FieldValue::Boolean(true) => {} // Expected
        _ => panic!("BETWEEN should return FieldValue::Boolean(true)"),
    }

    // Test NOT BETWEEN
    let expr_not = Expr::Between {
        expr: Box::new(Expr::Column("score".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(70))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(90))),
        negated: true,
    };

    let result_not = ExpressionEvaluator::evaluate_expression_value(&expr_not, &record).unwrap();
    match result_not {
        FieldValue::Boolean(false) => {} // Expected (NOT true = false)
        _ => panic!("NOT BETWEEN should return FieldValue::Boolean(false)"),
    }
}
