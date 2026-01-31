//! Tests for aggregate function computation

use std::collections::HashSet;
use velostream::velostream::sql::ast::{Expr, LiteralValue};
use velostream::velostream::sql::execution::aggregation::AggregateFunctions;
use velostream::velostream::sql::execution::aggregation::compute::WelfordState;
use velostream::velostream::sql::execution::internal::GroupAccumulator;
use velostream::velostream::sql::execution::types::FieldValue;

/// Helper: build a WelfordState from a slice of f64 values
fn welford_from_values(values: &[f64]) -> WelfordState {
    let mut state = WelfordState::new();
    for &v in values {
        state.update(v);
    }
    state
}

fn create_test_accumulator() -> GroupAccumulator {
    let mut accumulator = GroupAccumulator::new();
    accumulator.count = 10;

    // Add some test data
    accumulator.non_null_counts.insert("amount".to_string(), 8);
    accumulator.sums.insert("amount".to_string(), 150.0);
    accumulator
        .sum_has_values
        .insert("amount".to_string(), true);
    accumulator
        .sum_all_integer
        .insert("amount".to_string(), false);
    accumulator
        .mins
        .insert("amount".to_string(), FieldValue::Float(5.0));
    accumulator
        .maxs
        .insert("amount".to_string(), FieldValue::Float(50.0));

    // Add Welford state for AVG, STDDEV, VARIANCE
    accumulator.welford_states.insert(
        "amount".to_string(),
        welford_from_values(&[10.0, 20.0, 30.0, 40.0, 50.0]),
    );

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
    // Test SUM with float inputs returns Float
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
fn test_sum_integer_inputs_returns_integer() {
    let mut accumulator = GroupAccumulator::new();
    accumulator.sums.insert("qty".to_string(), 42.0);
    accumulator.sum_has_values.insert("qty".to_string(), true);
    accumulator.sum_all_integer.insert("qty".to_string(), true);

    let sum_expr = Expr::Function {
        name: "SUM".to_string(),
        args: vec![Expr::Column("qty".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("qty", &sum_expr, &accumulator).unwrap();

    assert_eq!(result, FieldValue::Integer(42));
}

#[test]
fn test_sum_empty_returns_null() {
    let accumulator = GroupAccumulator::new();
    let sum_expr = Expr::Function {
        name: "SUM".to_string(),
        args: vec![Expr::Column("amount".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("amount", &sum_expr, &accumulator)
            .unwrap();

    assert_eq!(result, FieldValue::Null);
}

#[test]
fn test_sum_mixed_int_float_returns_float() {
    let mut accumulator = GroupAccumulator::new();
    accumulator.sums.insert("val".to_string(), 15.5);
    accumulator.sum_has_values.insert("val".to_string(), true);
    accumulator.sum_all_integer.insert("val".to_string(), false);

    let sum_expr = Expr::Function {
        name: "SUM".to_string(),
        args: vec![Expr::Column("val".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("val", &sum_expr, &accumulator).unwrap();

    assert_eq!(result, FieldValue::Float(15.5));
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
fn test_stddev_single_value_returns_null() {
    let mut accumulator = GroupAccumulator::new();
    accumulator
        .welford_states
        .insert("x".to_string(), welford_from_values(&[42.0]));

    let expr = Expr::Function {
        name: "STDDEV".to_string(),
        args: vec![Expr::Column("x".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("x", &expr, &accumulator).unwrap();
    assert_eq!(result, FieldValue::Null);
}

#[test]
fn test_variance_single_value_returns_null() {
    let mut accumulator = GroupAccumulator::new();
    accumulator
        .welford_states
        .insert("x".to_string(), welford_from_values(&[42.0]));

    let expr = Expr::Function {
        name: "VARIANCE".to_string(),
        args: vec![Expr::Column("x".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("x", &expr, &accumulator).unwrap();
    assert_eq!(result, FieldValue::Null);
}

#[test]
fn test_stddev_pop_aggregate() {
    let mut accumulator = GroupAccumulator::new();
    accumulator.welford_states.insert(
        "x".to_string(),
        welford_from_values(&[10.0, 20.0, 30.0, 40.0, 50.0]),
    );

    let expr = Expr::Function {
        name: "STDDEV_POP".to_string(),
        args: vec![Expr::Column("x".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("x", &expr, &accumulator).unwrap();

    if let FieldValue::Float(stddev) = result {
        // Population stddev of [10,20,30,40,50] = sqrt(200) ≈ 14.142
        assert!((stddev - 14.1421).abs() < 0.01);
    } else {
        panic!("Expected Float result for STDDEV_POP");
    }
}

#[test]
fn test_stddev_pop_single_value_returns_zero() {
    let mut accumulator = GroupAccumulator::new();
    accumulator
        .welford_states
        .insert("x".to_string(), welford_from_values(&[42.0]));

    let expr = Expr::Function {
        name: "STDDEV_POP".to_string(),
        args: vec![Expr::Column("x".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("x", &expr, &accumulator).unwrap();
    assert_eq!(result, FieldValue::Float(0.0));
}

#[test]
fn test_var_pop_aggregate() {
    let mut accumulator = GroupAccumulator::new();
    accumulator.welford_states.insert(
        "x".to_string(),
        welford_from_values(&[10.0, 20.0, 30.0, 40.0, 50.0]),
    );

    let expr = Expr::Function {
        name: "VAR_POP".to_string(),
        args: vec![Expr::Column("x".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("x", &expr, &accumulator).unwrap();

    if let FieldValue::Float(var) = result {
        // Population variance of [10,20,30,40,50] = 200.0
        assert!((var - 200.0).abs() < 0.01);
    } else {
        panic!("Expected Float result for VAR_POP");
    }
}

#[test]
fn test_median_aggregate() {
    // Odd number of values
    let mut accumulator = GroupAccumulator::new();
    accumulator
        .numeric_values
        .insert("x".to_string(), vec![10.0, 30.0, 20.0, 50.0, 40.0]);

    let expr = Expr::Function {
        name: "MEDIAN".to_string(),
        args: vec![Expr::Column("x".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("x", &expr, &accumulator).unwrap();
    assert_eq!(result, FieldValue::Float(30.0));

    // Even number of values
    let mut accumulator2 = GroupAccumulator::new();
    accumulator2
        .numeric_values
        .insert("x".to_string(), vec![10.0, 20.0, 30.0, 40.0]);

    let result2 =
        AggregateFunctions::compute_field_aggregate_value("x", &expr, &accumulator2).unwrap();
    assert_eq!(result2, FieldValue::Float(25.0));
}

#[test]
fn test_first_value_last_value_aliases() {
    let accumulator = create_test_accumulator();

    let first_expr = Expr::Function {
        name: "FIRST_VALUE".to_string(),
        args: vec![Expr::Column("name".to_string())],
    };
    let result =
        AggregateFunctions::compute_field_aggregate_value("name", &first_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::String("First".to_string()));

    let last_expr = Expr::Function {
        name: "LAST_VALUE".to_string(),
        args: vec![Expr::Column("name".to_string())],
    };
    let result =
        AggregateFunctions::compute_field_aggregate_value("name", &last_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::String("Last".to_string()));
}

#[test]
fn test_listagg_routing() {
    let accumulator = create_test_accumulator();
    let listagg_expr = Expr::Function {
        name: "LISTAGG".to_string(),
        args: vec![
            Expr::Column("names".to_string()),
            Expr::Literal(LiteralValue::String(",".to_string())),
        ],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("names", &listagg_expr, &accumulator)
            .unwrap();

    assert_eq!(result, FieldValue::String("Alice,Bob,Charlie".to_string()));
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

    // Test new aliases
    for name in &[
        "STDDEV_SAMP",
        "STDDEV_POP",
        "VAR_SAMP",
        "VAR_POP",
        "MEDIAN",
        "FIRST_VALUE",
        "LAST_VALUE",
        "LISTAGG",
        "COLLECT",
    ] {
        let expr = Expr::Function {
            name: name.to_string(),
            args: vec![],
        };
        assert!(
            AggregateFunctions::is_aggregate_function(&expr),
            "{} should be aggregate",
            name
        );
    }

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
    let s = |name: &str| name.to_string();
    // Core aggregate functions (primary names from the catalog)
    assert!(functions.contains(&s("COUNT")));
    assert!(functions.contains(&s("SUM")));
    assert!(functions.contains(&s("AVG")));
    assert!(functions.contains(&s("MIN")));
    assert!(functions.contains(&s("MAX")));
    assert!(functions.contains(&s("STDDEV")));
    assert!(functions.contains(&s("STDDEV_POP")));
    assert!(functions.contains(&s("VARIANCE")));
    assert!(functions.contains(&s("VAR_POP")));
    assert!(functions.contains(&s("MEDIAN")));
    assert!(functions.contains(&s("COUNT_DISTINCT")));
    assert!(functions.contains(&s("FIRST_VALUE")));
    assert!(functions.contains(&s("LAST_VALUE")));
    assert!(functions.contains(&s("STRING_AGG")));
    assert!(functions.contains(&s("LISTAGG")));
    // Aliases (FIRST, LAST, STDDEV_SAMP, VAR_SAMP, GROUP_CONCAT, COLLECT) are resolved
    // through is_aggregate_function but not listed as primary names in supported_functions
}

#[test]
fn test_min_max_with_timestamps() {
    use chrono::NaiveDateTime;

    let mut accumulator = GroupAccumulator::new();
    let ts1 = NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    let ts2 = NaiveDateTime::parse_from_str("2024-06-15 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    accumulator.update_min("ts", FieldValue::Timestamp(ts2));
    accumulator.update_min("ts", FieldValue::Timestamp(ts1));
    assert_eq!(
        accumulator.mins.get("ts").unwrap(),
        &FieldValue::Timestamp(ts1)
    );

    accumulator.update_max("ts", FieldValue::Timestamp(ts1));
    accumulator.update_max("ts", FieldValue::Timestamp(ts2));
    assert_eq!(
        accumulator.maxs.get("ts").unwrap(),
        &FieldValue::Timestamp(ts2)
    );
}

#[test]
fn test_min_max_with_strings() {
    let mut accumulator = GroupAccumulator::new();

    accumulator.update_min("s", FieldValue::String("banana".to_string()));
    accumulator.update_min("s", FieldValue::String("apple".to_string()));
    assert_eq!(
        accumulator.mins.get("s").unwrap(),
        &FieldValue::String("apple".to_string())
    );

    accumulator.update_max("s", FieldValue::String("apple".to_string()));
    accumulator.update_max("s", FieldValue::String("banana".to_string()));
    assert_eq!(
        accumulator.maxs.get("s").unwrap(),
        &FieldValue::String("banana".to_string())
    );
}

#[test]
fn test_min_max_mixed_int_float() {
    let mut accumulator = GroupAccumulator::new();

    accumulator.update_min("v", FieldValue::Float(5.5));
    accumulator.update_min("v", FieldValue::Integer(3));
    assert_eq!(accumulator.mins.get("v").unwrap(), &FieldValue::Integer(3));

    accumulator.update_max("v", FieldValue::Integer(3));
    accumulator.update_max("v", FieldValue::Float(5.5));
    assert_eq!(accumulator.maxs.get("v").unwrap(), &FieldValue::Float(5.5));
}
