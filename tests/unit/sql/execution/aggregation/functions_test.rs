//! Tests for aggregate function computation

use ferrisstreams::ferris::sql::ast::{Expr, LiteralValue};
use ferrisstreams::ferris::sql::execution::aggregation::AggregateFunctions;
use ferrisstreams::ferris::sql::execution::internal::GroupAccumulator;
use ferrisstreams::ferris::sql::execution::types::FieldValue;
use std::collections::HashSet;

fn create_test_accumulator() -> GroupAccumulator {
    let mut accumulator = GroupAccumulator::new();
    accumulator.count = 10;

    // Add some test data
    accumulator.non_null_counts.insert("amount".to_string(), 8);
    accumulator.sums.insert("amount".to_string(), 150.0);
    accumulator
        .mins
        .insert("amount".to_string(), FieldValue::Float(5.0));
    accumulator
        .maxs
        .insert("amount".to_string(), FieldValue::Float(50.0));

    // Add numeric values for AVG, STDDEV, VARIANCE
    accumulator
        .numeric_values
        .insert("amount".to_string(), vec![10.0, 20.0, 30.0, 40.0, 50.0]);

    // Add string values for STRING_AGG
    accumulator.string_values.insert(
        "names".to_string(),
        vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ],
    );

    // Add distinct values for COUNT_DISTINCT
    let mut distinct_set = HashSet::new();
    distinct_set.insert("A".to_string());
    distinct_set.insert("B".to_string());
    distinct_set.insert("C".to_string());
    accumulator
        .distinct_values
        .insert("category".to_string(), distinct_set);

    // Add first/last values
    accumulator
        .first_values
        .insert("name".to_string(), FieldValue::String("First".to_string()));
    accumulator
        .last_values
        .insert("name".to_string(), FieldValue::String("Last".to_string()));

    accumulator
}

#[test]
fn test_compute_count_star() {
    let accumulator = create_test_accumulator();
    let count_expr = Expr::Function {
        name: "COUNT".to_string(),
        args: vec![], // COUNT(*)
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("count_star", &count_expr, &accumulator)
            .unwrap();

    assert_eq!(result, FieldValue::Integer(10));
}

#[test]
fn test_compute_count_column() {
    let accumulator = create_test_accumulator();
    let count_expr = Expr::Function {
        name: "COUNT".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("amount", &count_expr, &accumulator)
            .unwrap();

    assert_eq!(result, FieldValue::Integer(8));
}

#[test]
fn test_compute_sum() {
    let accumulator = create_test_accumulator();
    let sum_expr = Expr::Function {
        name: "SUM".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("amount", &sum_expr, &accumulator)
            .unwrap();

    assert_eq!(result, FieldValue::Float(150.0));
}

#[test]
fn test_compute_avg() {
    let accumulator = create_test_accumulator();
    let avg_expr = Expr::Function {
        name: "AVG".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("amount", &avg_expr, &accumulator)
            .unwrap();

    // Average of [10, 20, 30, 40, 50] = 30.0
    assert_eq!(result, FieldValue::Float(30.0));
}

#[test]
fn test_compute_min_max() {
    let accumulator = create_test_accumulator();

    let min_expr = Expr::Function {
        name: "MIN".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };
    let result =
        AggregateFunctions::compute_field_aggregate_value("amount", &min_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::Float(5.0));

    let max_expr = Expr::Function {
        name: "MAX".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };
    let result =
        AggregateFunctions::compute_field_aggregate_value("amount", &max_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::Float(50.0));
}

#[test]
fn test_compute_stddev_variance() {
    let accumulator = create_test_accumulator();

    let stddev_expr = Expr::Function {
        name: "STDDEV".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };
    let result =
        AggregateFunctions::compute_field_aggregate_value("amount", &stddev_expr, &accumulator)
            .unwrap();

    // For values [10, 20, 30, 40, 50], sample stddev should be calculated
    if let FieldValue::Float(stddev) = result {
        assert!(stddev > 15.0 && stddev < 17.0); // Approximate check
    } else {
        panic!("Expected Float result for STDDEV");
    }
}

#[test]
fn test_compute_first_last() {
    let accumulator = create_test_accumulator();

    let first_expr = Expr::Function {
        name: "FIRST".to_string(),
        args: vec![Expr::Column("name".to_string())],
    };
    let result =
        AggregateFunctions::compute_field_aggregate_value("name", &first_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::String("First".to_string()));

    let last_expr = Expr::Function {
        name: "LAST".to_string(),
        args: vec![Expr::Column("name".to_string())],
    };
    let result =
        AggregateFunctions::compute_field_aggregate_value("name", &last_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::String("Last".to_string()));
}

#[test]
fn test_compute_string_agg() {
    let accumulator = create_test_accumulator();
    let string_agg_expr = Expr::Function {
        name: "STRING_AGG".to_string(),
        args: vec![
            Expr::Column("names".to_string()),
            Expr::Literal(LiteralValue::String(";".to_string())),
        ],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("names", &string_agg_expr, &accumulator)
            .unwrap();

    assert_eq!(result, FieldValue::String("Alice;Bob;Charlie".to_string()));
}

#[test]
fn test_compute_count_distinct() {
    let accumulator = create_test_accumulator();
    let count_distinct_expr = Expr::Function {
        name: "COUNT_DISTINCT".to_string(),
        args: vec![Expr::Column("category".to_string())],
    };

    let result = AggregateFunctions::compute_field_aggregate_value(
        "category",
        &count_distinct_expr,
        &accumulator,
    )
    .unwrap();

    assert_eq!(result, FieldValue::Integer(3));
}

#[test]
fn test_is_aggregate_function() {
    let count_expr = Expr::Function {
        name: "COUNT".to_string(),
        args: vec![],
    };
    assert!(AggregateFunctions::is_aggregate_function(&count_expr));

    let upper_expr = Expr::Function {
        name: "UPPER".to_string(),
        args: vec![Expr::Column("name".to_string())],
    };
    assert!(!AggregateFunctions::is_aggregate_function(&upper_expr));

    let identifier_expr = Expr::Column("name".to_string());
    assert!(!AggregateFunctions::is_aggregate_function(&identifier_expr));
}

#[test]
fn test_supported_functions() {
    let functions = AggregateFunctions::supported_functions();
    assert!(functions.contains(&"COUNT"));
    assert!(functions.contains(&"SUM"));
    assert!(functions.contains(&"AVG"));
    assert!(functions.contains(&"MIN"));
    assert!(functions.contains(&"MAX"));
    assert!(functions.contains(&"STDDEV"));
    assert!(functions.contains(&"VARIANCE"));
    assert!(functions.contains(&"COUNT_DISTINCT"));
    assert!(functions.contains(&"FIRST"));
    assert!(functions.contains(&"LAST"));
    assert!(functions.contains(&"STRING_AGG"));
    assert!(functions.contains(&"GROUP_CONCAT"));
}
