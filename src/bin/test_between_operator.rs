// Simple test binary for BETWEEN operator
use ferrisstreams::ferris::sql::{
    ast::{Expr, LiteralValue},
    execution::{expression::evaluator::ExpressionEvaluator, types::FieldValue, StreamRecord},
    parser::StreamingSqlParser,
};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing BETWEEN operator implementation...");

    // Test 1: Basic parsing
    println!("\n1. Testing parsing of BETWEEN operator:");
    let parser = StreamingSqlParser::new();
    let sql = "SELECT * FROM test_stream WHERE price BETWEEN 10 AND 100";
    match parser.parse(sql) {
        Ok(_) => println!("✓ Successfully parsed: {}", sql),
        Err(e) => println!("✗ Failed to parse: {:?}", e),
    }

    // Test 2: NOT BETWEEN parsing
    println!("\n2. Testing parsing of NOT BETWEEN operator:");
    let sql2 = "SELECT * FROM test_stream WHERE price NOT BETWEEN 10 AND 100";
    match parser.parse(sql2) {
        Ok(_) => println!("✓ Successfully parsed: {}", sql2),
        Err(e) => println!("✗ Failed to parse: {:?}", e),
    }

    // Test 3: Expression evaluation
    println!("\n3. Testing BETWEEN expression evaluation:");
    let mut fields = HashMap::new();
    fields.insert("value".to_string(), FieldValue::Integer(50));
    let record = StreamRecord::new(fields);

    let expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };

    match ExpressionEvaluator::evaluate_expression(&expr, &record) {
        Ok(result) => println!("✓ 50 BETWEEN 10 AND 100 = {}", result),
        Err(e) => println!("✗ Error evaluating BETWEEN: {:?}", e),
    }

    // Test 4: NOT BETWEEN evaluation
    println!("\n4. Testing NOT BETWEEN expression evaluation:");
    let expr_not = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: true,
    };

    match ExpressionEvaluator::evaluate_expression(&expr_not, &record) {
        Ok(result) => println!("✓ 50 NOT BETWEEN 10 AND 100 = {}", result),
        Err(e) => println!("✗ Error evaluating NOT BETWEEN: {:?}", e),
    }

    // Test 5: Financial precision with ScaledInteger
    println!("\n5. Testing BETWEEN with financial precision:");
    let mut financial_fields = HashMap::new();
    financial_fields.insert("price".to_string(), FieldValue::ScaledInteger(7550, 2)); // $75.50
    let financial_record = StreamRecord::new(financial_fields);

    let financial_expr = Expr::Between {
        expr: Box::new(Expr::Column("price".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Decimal("50.00".to_string()))), // $50.00
        high: Box::new(Expr::Literal(LiteralValue::Decimal("100.00".to_string()))), // $100.00
        negated: false,
    };

    match ExpressionEvaluator::evaluate_expression(&financial_expr, &financial_record) {
        Ok(result) => println!("✓ $75.50 BETWEEN $50.00 AND $100.00 = {}", result),
        Err(e) => println!("✗ Error evaluating financial BETWEEN: {:?}", e),
    }

    // Test 6: String lexicographic comparison
    println!("\n6. Testing BETWEEN with string values:");
    let mut string_fields = HashMap::new();
    string_fields.insert(
        "name".to_string(),
        FieldValue::String("Charlie".to_string()),
    );
    let string_record = StreamRecord::new(string_fields);

    let string_expr = Expr::Between {
        expr: Box::new(Expr::Column("name".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::String("Alice".to_string()))),
        high: Box::new(Expr::Literal(LiteralValue::String("David".to_string()))),
        negated: false,
    };

    match ExpressionEvaluator::evaluate_expression(&string_expr, &string_record) {
        Ok(result) => println!("✓ 'Charlie' BETWEEN 'Alice' AND 'David' = {}", result),
        Err(e) => println!("✗ Error evaluating string BETWEEN: {:?}", e),
    }

    // Test 7: Mixed type comparison
    println!("\n7. Testing BETWEEN with mixed types:");
    let mixed_expr = Expr::Between {
        expr: Box::new(Expr::Literal(LiteralValue::Integer(75))),
        low: Box::new(Expr::Literal(LiteralValue::Float(50.0))),
        high: Box::new(Expr::Literal(LiteralValue::Float(100.0))),
        negated: false,
    };

    match ExpressionEvaluator::evaluate_expression(&mixed_expr, &record) {
        Ok(result) => println!("✓ 75 BETWEEN 50.0 AND 100.0 = {}", result),
        Err(e) => println!("✗ Error evaluating mixed type BETWEEN: {:?}", e),
    }

    // Test 8: NULL handling
    println!("\n8. Testing BETWEEN with NULL values:");
    let mut null_fields = HashMap::new();
    null_fields.insert("value".to_string(), FieldValue::Null);
    let null_record = StreamRecord::new(null_fields);

    let null_expr = Expr::Between {
        expr: Box::new(Expr::Column("value".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };

    match ExpressionEvaluator::evaluate_expression(&null_expr, &null_record) {
        Ok(result) => println!("✓ NULL BETWEEN 10 AND 100 = {}", result),
        Err(e) => println!("✗ Error evaluating NULL BETWEEN: {:?}", e),
    }

    println!("\n✅ BETWEEN operator testing completed!");
    Ok(())
}
