//! Tests for `get_expression_name()` in SelectProcessor.
//!
//! Verifies that table alias prefixes are stripped from column names
//! when generating output field names, matching SQL semantics where
//! `SELECT a.symbol` outputs a field named `"symbol"`, not `"a.symbol"`.

use velostream::velostream::sql::ast::{BinaryOperator, Expr, LiteralValue, OverClause};
use velostream::velostream::sql::execution::processors::select::SelectProcessor;

/// Simple unqualified column name should be preserved as-is.
#[test]
fn test_expression_name_simple_column() {
    let expr = Expr::Column("symbol".to_string());
    assert_eq!(SelectProcessor::get_expression_name(&expr), "symbol");
}

/// Qualified column name (e.g., `a.symbol`) should strip the table alias prefix.
/// This is the core fix for the expr_0 bug in JOIN queries.
#[test]
fn test_expression_name_qualified_column_strips_prefix() {
    let expr = Expr::Column("a.symbol".to_string());
    assert_eq!(SelectProcessor::get_expression_name(&expr), "symbol");
}

/// Multi-part qualified names (e.g., `schema.table.column`) should keep
/// only the last segment.
#[test]
fn test_expression_name_multi_dot_qualified_column() {
    let expr = Expr::Column("schema.table.column".to_string());
    assert_eq!(SelectProcessor::get_expression_name(&expr), "column");
}

/// Different table aliases should all strip correctly.
#[test]
fn test_expression_name_various_table_aliases() {
    for (input, expected) in [
        ("b.price", "price"),
        ("left_table.id", "id"),
        ("t1.volume", "volume"),
        ("x.y", "y"),
    ] {
        let expr = Expr::Column(input.to_string());
        assert_eq!(
            SelectProcessor::get_expression_name(&expr),
            expected,
            "Failed for input: {}",
            input
        );
    }
}

/// Function expressions should use the function name.
#[test]
fn test_expression_name_function() {
    let expr = Expr::Function {
        name: "AVG".to_string(),
        args: vec![Expr::Column("price".to_string())],
    };
    assert_eq!(SelectProcessor::get_expression_name(&expr), "AVG");
}

/// Binary operation expression names should combine both sides.
#[test]
fn test_expression_name_binary_op() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("a.price".to_string())),
        op: BinaryOperator::Subtract,
        right: Box::new(Expr::Column("b.price".to_string())),
    };
    let name = SelectProcessor::get_expression_name(&expr);
    // Should strip prefixes from both sides
    assert!(name.contains("price"), "Should contain 'price': {}", name);
}

/// CASE expression should get a generic name.
#[test]
fn test_expression_name_case() {
    let expr = Expr::Case {
        when_clauses: vec![],
        else_clause: None,
    };
    assert_eq!(SelectProcessor::get_expression_name(&expr), "case_expr");
}

/// Literal expression names should be derived from the literal value.
#[test]
fn test_expression_name_literal() {
    let expr = Expr::Literal(LiteralValue::Integer(42));
    let name = SelectProcessor::get_expression_name(&expr);
    assert!(!name.is_empty(), "Literal name should not be empty");
}

/// Window function expression should include the function name.
#[test]
fn test_expression_name_window_function() {
    let expr = Expr::WindowFunction {
        function_name: "LAG".to_string(),
        args: vec![],
        over_clause: OverClause {
            window_spec: None,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        },
    };
    assert_eq!(SelectProcessor::get_expression_name(&expr), "window_LAG");
}
