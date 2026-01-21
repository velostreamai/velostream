//! Tests for aggregation accumulator management

use std::collections::HashMap;
use velostream::velostream::sql::ast::{Expr, SelectField};
use velostream::velostream::sql::execution::aggregation::AccumulatorManager;
use velostream::velostream::sql::execution::internal::GroupAccumulator;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

fn create_test_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
    let mut field_map = HashMap::new();
    for (key, value) in fields {
        field_map.insert(key.to_string(), value);
    }

    StreamRecord {
        fields: field_map,
        timestamp: 0,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

#[test]
fn test_field_value_to_string() {
    assert_eq!(
        AccumulatorManager::field_value_to_string(&FieldValue::String("test".to_string())),
        "test"
    );
    assert_eq!(
        AccumulatorManager::field_value_to_string(&FieldValue::Integer(42)),
        "42"
    );
    assert_eq!(
        AccumulatorManager::field_value_to_string(&FieldValue::Null),
        "NULL"
    );
}

#[test]
fn test_is_aggregate_expression() {
    let count_expr = Expr::Function {
        name: "COUNT".to_string(),
        args: vec![],
    };
    assert!(AccumulatorManager::is_aggregate_expression(&count_expr));

    let sum_expr = Expr::Function {
        name: "SUM".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };
    assert!(AccumulatorManager::is_aggregate_expression(&sum_expr));

    let non_agg_expr = Expr::Function {
        name: "UPPER".to_string(),
        args: vec![Expr::Column("name".to_string())],
    };
    assert!(!AccumulatorManager::is_aggregate_expression(&non_agg_expr));

    let identifier_expr = Expr::Column("name".to_string());
    assert!(!AccumulatorManager::is_aggregate_expression(
        &identifier_expr
    ));
}

#[test]
fn test_generate_field_name() {
    let count_expr = Expr::Function {
        name: "COUNT".to_string(),
        args: vec![],
    };
    assert_eq!(
        AccumulatorManager::generate_field_name(&count_expr),
        "COUNT(*)"
    );

    let sum_expr = Expr::Function {
        name: "SUM".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };
    assert_eq!(
        AccumulatorManager::generate_field_name(&sum_expr),
        "SUM(amount)"
    );
}

#[test]
fn test_extract_aggregate_expressions() {
    let select_fields = vec![
        SelectField::Expression {
            expr: Expr::Column("category".to_string()),
            alias: None,
        },
        SelectField::Expression {
            expr: Expr::Function {
                name: "COUNT".to_string(),
                args: vec![],
            },
            alias: Some("total_count".to_string()),
        },
        SelectField::Expression {
            expr: Expr::Function {
                name: "SUM".to_string(),
                args: vec![Expr::Column("amount".to_string())],
            },
            alias: None,
        },
    ];

    let aggregates = AccumulatorManager::extract_aggregate_expressions(&select_fields);
    assert_eq!(aggregates.len(), 2);
    assert_eq!(aggregates[0].0, "total_count");
    assert_eq!(aggregates[1].0, "SUM(amount)");
}

#[test]
fn test_process_record_basic_count() {
    let mut accumulator = GroupAccumulator::new();
    let record = create_test_record(vec![("category", FieldValue::String("test".to_string()))]);

    let aggregate_expressions = vec![];
    let select_aliases = std::collections::HashSet::new();

    let result = AccumulatorManager::process_record_into_accumulator(
        &mut accumulator,
        &record,
        &aggregate_expressions,
        &select_aliases,
    );

    assert!(result.is_ok());
    assert_eq!(accumulator.count, 1);
    assert!(accumulator.sample_record.is_some());
}
