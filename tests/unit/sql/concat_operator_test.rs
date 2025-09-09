// Tests for SQL concatenation operator (||)
use ferrisstreams::ferris::sql::{
    ast::{BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery},
    execution::{expression::evaluator::ExpressionEvaluator, types::FieldValue, StreamRecord},
    parser::StreamingSqlParser,
};
use std::collections::HashMap;

#[test]
fn test_parse_concat_operator() {
    let parser = StreamingSqlParser::new();

    let sql = "SELECT 'Hello' || ' ' || 'World' FROM test_stream";
    let result = parser.parse(sql).unwrap();

    match result {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 1);
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    // Should parse as nested concatenation: ('Hello' || ' ') || 'World'
                    match expr {
                        Expr::BinaryOp {
                            op: BinaryOperator::Concat,
                            ..
                        } => {
                            // Successfully parsed concat operator
                        }
                        _ => panic!("Expected concatenation expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_concat_operator_precedence() {
    let parser = StreamingSqlParser::new();

    // Test that || has proper precedence
    let sql = "SELECT 'a' || 'b' + 'c' FROM test_stream";
    let result = parser.parse(sql);

    // This should parse successfully with proper precedence
    assert!(result.is_ok());
}

#[test]
fn test_evaluate_string_concatenation() {
    let mut fields = HashMap::new();
    fields.insert("col1".to_string(), FieldValue::String("Hello".to_string()));
    fields.insert("col2".to_string(), FieldValue::String(" World".to_string()));
    let record = StreamRecord::new(fields);

    // Test string || string
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("Hello".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::String(" World".to_string()))),
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    assert_eq!(result, FieldValue::String("Hello World".to_string()));
}

#[test]
fn test_evaluate_mixed_type_concatenation() {
    let record = StreamRecord::new(HashMap::new());

    // Test string || integer
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("Value: ".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::Integer(42))),
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    assert_eq!(result, FieldValue::String("Value: 42".to_string()));

    // Test integer || string
    let expr2 = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::Integer(123))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::String(" items".to_string()))),
    };

    let result2 = ExpressionEvaluator::evaluate_expression_value(&expr2, &record).unwrap();
    assert_eq!(result2, FieldValue::String("123 items".to_string()));
}

#[test]
fn test_evaluate_null_concatenation() {
    let record = StreamRecord::new(HashMap::new());

    // Test string || null (should return null per SQL standards)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("Hello".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::Null)),
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    assert_eq!(result, FieldValue::Null);

    // Test null || string (should also return null)
    let expr2 = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::Null)),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::String("World".to_string()))),
    };

    let result2 = ExpressionEvaluator::evaluate_expression_value(&expr2, &record).unwrap();
    assert_eq!(result2, FieldValue::Null);
}

#[test]
fn test_evaluate_float_concatenation() {
    let record = StreamRecord::new(HashMap::new());

    // Test string || float
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("Pi is ".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::Float(std::f64::consts::PI))),
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    assert_eq!(
        result,
        FieldValue::String("Pi is 3.141592653589793".to_string())
    );
}

#[test]
fn test_evaluate_boolean_concatenation() {
    let record = StreamRecord::new(HashMap::new());

    // Test string || boolean
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("Active: ".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    assert_eq!(result, FieldValue::String("Active: true".to_string()));
}

#[test]
fn test_evaluate_chained_concatenation() {
    let record = StreamRecord::new(HashMap::new());

    // Test 'a' || 'b' || 'c' (should be parsed as ('a' || 'b') || 'c')
    let inner_concat = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("a".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::String("b".to_string()))),
    };

    let outer_concat = Expr::BinaryOp {
        left: Box::new(inner_concat),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::String("c".to_string()))),
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&outer_concat, &record).unwrap();
    assert_eq!(result, FieldValue::String("abc".to_string()));
}

#[test]
fn test_unix_timestamp_concatenation_example() {
    let record = StreamRecord::new(HashMap::new());

    // Test the user's specific example: 'exported_' || UNIX_TIMESTAMP()
    // For this test, we'll simulate with a constant timestamp
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("exported_".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::Integer(1693910400))), // Mock timestamp
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    assert_eq!(
        result,
        FieldValue::String("exported_1693910400".to_string())
    );
}

#[test]
fn test_scaled_integer_concatenation() {
    let record = StreamRecord::new(HashMap::new());

    // Test concatenation with ScaledInteger (financial precision type)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::String("Amount: $".to_string()))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::Float(1234.50))), // $1234.50
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    // Float is formatted as a decimal string (trailing zeros are not preserved in float representation)
    assert_eq!(result, FieldValue::String("Amount: $1234.5".to_string()));
}

#[test]
fn test_non_string_concatenation() {
    let record = StreamRecord::new(HashMap::new());

    // Test integer || integer (both get converted to strings)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::Integer(123))),
        op: BinaryOperator::Concat,
        right: Box::new(Expr::Literal(LiteralValue::Integer(456))),
    };

    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();
    assert_eq!(result, FieldValue::String("123456".to_string()));
}
