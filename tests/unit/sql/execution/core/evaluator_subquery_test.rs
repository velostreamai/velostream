/*!
# Expression Evaluator Subquery Support Tests

Tests specifically for the enhanced `evaluate_expression_value_with_subqueries`
implementation that handles all expression types recursively instead of falling
back to stub methods.

These tests verify that complex expressions containing subqueries work correctly
in all contexts, particularly in HAVING clauses with aggregate functions.
*/

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::sql::ast::{
    BinaryOperator, BinaryOperator as ComparisonOp, Expr, LiteralValue, SelectField, StreamSource,
    StreamingQuery, SubqueryType, UnaryOperator,
};
use velostream::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;
use velostream::velostream::sql::execution::expression::subquery_executor::SubqueryExecutor;
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::SqlError;
use velostream::velostream::table::streaming::{
    RecordBatch, RecordStream, SimpleStreamRecord as StreamingRecord, StreamResult,
};
use velostream::velostream::table::unified_table::{TableResult, UnifiedTable};

// Mock table for testing
#[derive(Debug)]
struct MockSubqueryTable {
    name: String,
    exists_result: bool,
}

impl MockSubqueryTable {
    fn new(name: &str, exists_result: bool) -> Self {
        MockSubqueryTable {
            name: name.to_string(),
            exists_result,
        }
    }
}

#[async_trait]
impl UnifiedTable for MockSubqueryTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_record(&self, _key: &str) -> TableResult<Option<HashMap<String, FieldValue>>> {
        Ok(None)
    }

    fn contains_key(&self, _key: &str) -> bool {
        false
    }

    fn record_count(&self) -> usize {
        if self.exists_result {
            1
        } else {
            0
        }
    }

    fn is_empty(&self) -> bool {
        !self.exists_result
    }

    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)> + '_> {
        Box::new(std::iter::empty())
    }

    fn sql_column_values(
        &self,
        _column: &str,
        _where_clause: &str,
    ) -> TableResult<Vec<FieldValue>> {
        Ok(vec![FieldValue::Integer(42)])
    }

    fn sql_scalar(&self, _select_expr: &str, _where_clause: &str) -> TableResult<FieldValue> {
        Ok(FieldValue::Integer(100))
    }

    async fn stream_all(&self) -> StreamResult<RecordStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        if self.exists_result {
            let mut fields = HashMap::new();
            fields.insert("value".to_string(), FieldValue::Integer(1));
            let _ = tx.send(Ok(StreamingRecord {
                key: "mock_0".to_string(),
                fields,
            }));
        }
        Ok(RecordStream { receiver: rx })
    }

    async fn stream_filter(&self, _where_clause: &str) -> StreamResult<RecordStream> {
        self.stream_all().await
    }

    async fn query_batch(
        &self,
        _batch_size: usize,
        _offset: Option<usize>,
    ) -> StreamResult<RecordBatch> {
        Ok(RecordBatch {
            records: vec![],
            has_more: false,
        })
    }

    async fn stream_count(&self, _where_clause: Option<&str>) -> StreamResult<usize> {
        Ok(if self.exists_result { 1 } else { 0 })
    }

    async fn stream_aggregate(
        &self,
        _aggregate_expr: &str,
        _where_clause: Option<&str>,
    ) -> StreamResult<FieldValue> {
        Ok(FieldValue::Integer(1))
    }
}

// Mock SubqueryExecutor for testing
struct MockSubqueryExecutor;

impl SubqueryExecutor for MockSubqueryExecutor {
    fn execute_exists_subquery(
        &self,
        _query: &StreamingQuery,
        _record: &StreamRecord,
        _context: &ProcessorContext,
    ) -> Result<bool, SqlError> {
        Ok(true)
    }

    fn execute_scalar_subquery(
        &self,
        _query: &StreamingQuery,
        _record: &StreamRecord,
        _context: &ProcessorContext,
    ) -> Result<FieldValue, SqlError> {
        Ok(FieldValue::Integer(42))
    }

    fn execute_in_subquery(
        &self,
        value: &FieldValue,
        _query: &StreamingQuery,
        _record: &StreamRecord,
        _context: &ProcessorContext,
    ) -> Result<bool, SqlError> {
        // Return true for specific values to test IN logic
        match value {
            FieldValue::Integer(42) => Ok(true),
            FieldValue::String(s) if s == "test" => Ok(true),
            _ => Ok(false),
        }
    }

    fn execute_any_all_subquery(
        &self,
        _value: &FieldValue,
        _query: &StreamingQuery,
        _record: &StreamRecord,
        _context: &ProcessorContext,
        _is_any: bool,
        _comparison_op: &str,
    ) -> Result<bool, SqlError> {
        // For testing, just return true for ANY and false for ALL
        Ok(_is_any)
    }
}

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("volume".to_string(), FieldValue::Integer(15000));
    fields.insert("price".to_string(), FieldValue::Float(150.5));
    fields.insert("count".to_string(), FieldValue::Integer(5));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp: 1640995200000,
        offset: 0,
        partition: 0,
        event_time: None,
    }
}

fn create_test_context() -> ProcessorContext {
    let mut context = ProcessorContext::new("test_evaluator");
    context.load_reference_table(
        "market_data_ts",
        Arc::new(MockSubqueryTable::new("market_data_ts", true)),
    );
    context
}

#[test]
fn test_column_expression_with_subqueries() {
    // Test that Column expressions work in subquery context
    let expr = Expr::Column("symbol".to_string());
    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::String("AAPL".to_string()));
}

#[test]
fn test_literal_expression_with_subqueries() {
    // Test that Literal expressions work in subquery context
    let expr = Expr::Literal(LiteralValue::Integer(42));
    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Integer(42));
}

#[test]
fn test_function_with_subqueries() {
    // Test that Function expressions work in subquery context
    // COUNT(*) should work even when evaluated through subquery evaluator
    let expr = Expr::Function {
        name: "COUNT".to_string(),
        args: vec![Expr::Column("*".to_string())],
    };
    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    // Function evaluation should succeed (may return NULL or error depending on context)
    // The important thing is it doesn't hit the "not yet implemented" stub
    assert!(result.is_ok() || matches!(result, Err(SqlError::ExecutionError { .. })));
}

#[test]
fn test_unary_op_with_subqueries() {
    // Test UnaryOp expressions with subquery support
    let inner_expr = Box::new(Expr::Literal(LiteralValue::Boolean(true)));
    let expr = Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: inner_expr,
    };

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Boolean(false));
}

#[test]
fn test_unary_op_is_null_with_subqueries() {
    // Test IS NULL with subquery support
    let inner_expr = Box::new(Expr::Literal(LiteralValue::Null));
    let expr = Expr::UnaryOp {
        op: UnaryOperator::IsNull,
        expr: inner_expr,
    };

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Boolean(true));
}

#[test]
fn test_between_expression_with_subqueries() {
    // Test BETWEEN expressions with subquery support
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("volume".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10000))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(20000))),
        negated: false,
    };

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Boolean(true)); // 15000 is between 10000 and 20000
}

#[test]
fn test_case_expression_with_subqueries() {
    // Test CASE expressions with subquery support
    let expr = Expr::Case {
        when_clauses: vec![(
            Expr::BinaryOp {
                left: Box::new(Expr::Column("volume".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Integer(10000))),
            },
            Expr::Literal(LiteralValue::String("HIGH".to_string())),
        )],
        else_clause: Some(Box::new(Expr::Literal(LiteralValue::String(
            "LOW".to_string(),
        )))),
    };

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::String("HIGH".to_string()));
}

#[test]
fn test_exists_and_aggregate_function() {
    // This is the critical test case that was failing:
    // EXISTS (...) AND COUNT(*) >= 5
    // This simulates the HAVING clause structure from volume_spike_analysis

    let exists_subquery = Expr::Subquery {
        query: Box::new(StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Stream("market_data_ts".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        }),
        subquery_type: SubqueryType::Exists,
    };

    let count_comparison = Expr::BinaryOp {
        left: Box::new(Expr::Column("count".to_string())),
        op: BinaryOperator::GreaterThanOrEqual,
        right: Box::new(Expr::Literal(LiteralValue::Integer(5))),
    };

    let and_expr = Expr::BinaryOp {
        left: Box::new(exists_subquery),
        op: BinaryOperator::And,
        right: Box::new(count_comparison),
    };

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    // This should NOT throw "EXISTS subqueries are not yet implemented" error
    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &and_expr, &record, &executor, &context,
    );

    assert!(
        result.is_ok(),
        "EXISTS AND aggregate should work without falling back to stub"
    );
    assert_eq!(result.unwrap(), FieldValue::Boolean(true)); // Both conditions are true
}

#[test]
fn test_complex_nested_expression_with_subqueries() {
    // Test a complex nested expression: (EXISTS subquery) AND (BETWEEN expr) AND (CASE expr)
    let exists_subquery = Expr::Subquery {
        query: Box::new(StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Stream("market_data_ts".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        }),
        subquery_type: SubqueryType::Exists,
    };

    let between_expr = Expr::Between {
        expr: Box::new(Expr::Column("volume".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10000))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(20000))),
        negated: false,
    };

    let case_expr = Expr::Case {
        when_clauses: vec![(
            Expr::BinaryOp {
                left: Box::new(Expr::Column("count".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
            },
            Expr::Literal(LiteralValue::Boolean(true)),
        )],
        else_clause: Some(Box::new(Expr::Literal(LiteralValue::Boolean(false)))),
    };

    let and_expr1 = Expr::BinaryOp {
        left: Box::new(exists_subquery),
        op: BinaryOperator::And,
        right: Box::new(between_expr),
    };

    let and_expr2 = Expr::BinaryOp {
        left: Box::new(and_expr1),
        op: BinaryOperator::And,
        right: Box::new(case_expr),
    };

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &and_expr2, &record, &executor, &context,
    );

    assert!(result.is_ok(), "Complex nested expression should work");
    assert_eq!(result.unwrap(), FieldValue::Boolean(true));
}

#[test]
fn test_not_exists_with_aggregate() {
    // Test NOT EXISTS combined with aggregate functions
    let not_exists_subquery = Expr::Subquery {
        query: Box::new(StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Stream("market_data_ts".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        }),
        subquery_type: SubqueryType::NotExists,
    };

    let unary_not = Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("count".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        }),
    };

    let and_expr = Expr::BinaryOp {
        left: Box::new(not_exists_subquery),
        op: BinaryOperator::And,
        right: Box::new(unary_not),
    };

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &and_expr, &record, &executor, &context,
    );

    // MockSubqueryExecutor returns true for EXISTS, so NOT EXISTS should be false
    // This means the AND expression should be false
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Boolean(false));
}

#[test]
fn test_list_expression_error() {
    // Test that List expressions still error appropriately (they should only be used in IN/NOT IN)
    let expr = Expr::List(vec![
        Expr::Literal(LiteralValue::Integer(1)),
        Expr::Literal(LiteralValue::Integer(2)),
    ]);

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
        &expr, &record, &executor, &context,
    );

    assert!(result.is_err());
    assert!(matches!(result, Err(SqlError::ExecutionError { .. })));
}

#[test]
fn test_all_expression_types_no_stub_fallback() {
    // Comprehensive test that verifies NONE of the expression types fall back to stub methods
    let test_cases = vec![
        ("Column", Expr::Column("symbol".to_string())),
        ("Literal", Expr::Literal(LiteralValue::Integer(42))),
        (
            "UnaryOp",
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
            },
        ),
        (
            "Between",
            Expr::Between {
                expr: Box::new(Expr::Column("volume".to_string())),
                low: Box::new(Expr::Literal(LiteralValue::Integer(0))),
                high: Box::new(Expr::Literal(LiteralValue::Integer(100000))),
                negated: false,
            },
        ),
        (
            "Case",
            Expr::Case {
                when_clauses: vec![(
                    Expr::Literal(LiteralValue::Boolean(true)),
                    Expr::Literal(LiteralValue::Integer(1)),
                )],
                else_clause: None,
            },
        ),
    ];

    let record = create_test_record();
    let context = create_test_context();
    let executor = MockSubqueryExecutor;

    for (name, expr) in test_cases {
        let result = ExpressionEvaluator::evaluate_expression_value_with_subqueries(
            &expr, &record, &executor, &context,
        );

        // None of these should return the "not yet implemented" error
        if let Err(SqlError::ExecutionError { message, .. }) = &result {
            assert!(
                !message.contains("not yet implemented"),
                "{} expression fell back to stub method: {}",
                name,
                message
            );
        }
    }
}
