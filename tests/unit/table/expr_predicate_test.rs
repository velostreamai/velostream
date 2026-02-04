// Tests for AST-based expression evaluation against table records
// Validates that evaluate_expr_against_record handles all SQL expression types
// that CachedPredicate silently falls back to AlwaysTrue for.

use std::collections::HashMap;
use velostream::velostream::sql::ast::{BinaryOperator, Expr, LiteralValue, UnaryOperator};
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::unified_table::evaluate_expr_against_record;

fn make_record(fields: Vec<(&str, FieldValue)>) -> HashMap<String, FieldValue> {
    fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

// === Simple equality ===

#[test]
fn test_field_equals_string() {
    let record = make_record(vec![("status", FieldValue::String("active".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("status".to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_field_equals_integer() {
    let record = make_record(vec![("id", FieldValue::Integer(42))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(42))),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_field_not_equal() {
    let record = make_record(vec![("id", FieldValue::Integer(42))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(99))),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

// === AND ===

#[test]
fn test_and_both_true() {
    let record = make_record(vec![
        ("a", FieldValue::Integer(1)),
        ("b", FieldValue::Integer(2)),
    ]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("b".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(2))),
        }),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_and_one_false() {
    let record = make_record(vec![
        ("a", FieldValue::Integer(1)),
        ("b", FieldValue::Integer(99)),
    ]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("b".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(2))),
        }),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

// === OR (previously fell back to AlwaysTrue) ===

#[test]
fn test_or_first_true() {
    let record = make_record(vec![("status", FieldValue::String("active".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
        }),
        op: BinaryOperator::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("pending".to_string()))),
        }),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_or_second_true() {
    let record = make_record(vec![("status", FieldValue::String("pending".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
        }),
        op: BinaryOperator::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("pending".to_string()))),
        }),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_or_neither_true() {
    let record = make_record(vec![("status", FieldValue::String("deleted".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
        }),
        op: BinaryOperator::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("pending".to_string()))),
        }),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

// === NOT (previously fell back to AlwaysTrue) ===

#[test]
fn test_not_true_becomes_false() {
    let record = make_record(vec![("active", FieldValue::Boolean(true))]);
    let expr = Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::Column("active".to_string())),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_not_false_becomes_true() {
    let record = make_record(vec![("active", FieldValue::Boolean(false))]);
    let expr = Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::Column("active".to_string())),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

// === IN list ===

#[test]
fn test_in_string_list_match() {
    let record = make_record(vec![("status", FieldValue::String("gold".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("status".to_string())),
        op: BinaryOperator::In,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::String("gold".to_string())),
            Expr::Literal(LiteralValue::String("platinum".to_string())),
        ])),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_in_string_list_no_match() {
    let record = make_record(vec![("status", FieldValue::String("bronze".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("status".to_string())),
        op: BinaryOperator::In,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::String("gold".to_string())),
            Expr::Literal(LiteralValue::String("platinum".to_string())),
        ])),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

// === NOT IN (previously fell back to AlwaysTrue) ===

#[test]
fn test_not_in_list_excluded() {
    let record = make_record(vec![("id", FieldValue::Integer(1))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::NotIn,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::Integer(1)),
            Expr::Literal(LiteralValue::Integer(2)),
            Expr::Literal(LiteralValue::Integer(3)),
        ])),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_not_in_list_not_excluded() {
    let record = make_record(vec![("id", FieldValue::Integer(99))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::NotIn,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::Integer(1)),
            Expr::Literal(LiteralValue::Integer(2)),
            Expr::Literal(LiteralValue::Integer(3)),
        ])),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

// === LIKE (previously fell back to AlwaysTrue) ===

#[test]
fn test_like_pattern_match() {
    let record = make_record(vec![("name", FieldValue::String("test_user".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("name".to_string())),
        op: BinaryOperator::Like,
        right: Box::new(Expr::Literal(LiteralValue::String("%test%".to_string()))),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_like_pattern_no_match() {
    let record = make_record(vec![("name", FieldValue::String("admin".to_string()))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("name".to_string())),
        op: BinaryOperator::Like,
        right: Box::new(Expr::Literal(LiteralValue::String("%test%".to_string()))),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

// === BETWEEN (previously fell back to AlwaysTrue) ===

#[test]
fn test_between_in_range() {
    let record = make_record(vec![("x", FieldValue::Integer(5))]);
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("x".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        negated: false,
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_between_out_of_range() {
    let record = make_record(vec![("x", FieldValue::Integer(15))]);
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("x".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        negated: false,
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_not_between() {
    let record = make_record(vec![("x", FieldValue::Integer(15))]);
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("x".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        negated: true,
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

// === IS NULL / IS NOT NULL (previously fell back to AlwaysTrue) ===

#[test]
fn test_is_null_on_null() {
    let record = make_record(vec![("field", FieldValue::Null)]);
    let expr = Expr::UnaryOp {
        op: UnaryOperator::IsNull,
        expr: Box::new(Expr::Column("field".to_string())),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_is_null_on_non_null() {
    let record = make_record(vec![("field", FieldValue::Integer(42))]);
    let expr = Expr::UnaryOp {
        op: UnaryOperator::IsNull,
        expr: Box::new(Expr::Column("field".to_string())),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_is_not_null_on_non_null() {
    let record = make_record(vec![("field", FieldValue::Integer(42))]);
    let expr = Expr::UnaryOp {
        op: UnaryOperator::IsNotNull,
        expr: Box::new(Expr::Column("field".to_string())),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_is_not_null_on_null() {
    let record = make_record(vec![("field", FieldValue::Null)]);
    let expr = Expr::UnaryOp {
        op: UnaryOperator::IsNotNull,
        expr: Box::new(Expr::Column("field".to_string())),
    };
    assert!(!evaluate_expr_against_record(&expr, &record).unwrap());
}

// === Missing field treated as NULL ===

#[test]
fn test_missing_field_is_null() {
    let record = make_record(vec![]);
    let expr = Expr::UnaryOp {
        op: UnaryOperator::IsNull,
        expr: Box::new(Expr::Column("nonexistent".to_string())),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

// === Nested: (a = 1 OR b = 2) AND c = 3 ===

#[test]
fn test_nested_or_and() {
    let record = make_record(vec![
        ("a", FieldValue::Integer(1)),
        ("b", FieldValue::Integer(99)),
        ("c", FieldValue::Integer(3)),
    ]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("a".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            }),
            op: BinaryOperator::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("b".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Integer(2))),
            }),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("c".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
        }),
    };
    // (1==1 OR 99==2) AND 3==3 => (true OR false) AND true => true
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

// === Comparison operators ===

#[test]
fn test_greater_than() {
    let record = make_record(vec![("amount", FieldValue::Integer(150))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("amount".to_string())),
        op: BinaryOperator::GreaterThan,
        right: Box::new(Expr::Literal(LiteralValue::Integer(100))),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

#[test]
fn test_less_than_or_equal() {
    let record = make_record(vec![("amount", FieldValue::Integer(100))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("amount".to_string())),
        op: BinaryOperator::LessThanOrEqual,
        right: Box::new(Expr::Literal(LiteralValue::Integer(100))),
    };
    assert!(evaluate_expr_against_record(&expr, &record).unwrap());
}

// === OptimizedTableImpl with AST-based methods ===

#[test]
fn test_optimized_table_column_values_with_expr() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![
                ("status", FieldValue::String("active".to_string())),
                ("id", FieldValue::Integer(1)),
            ]),
        )
        .unwrap();
    table
        .insert(
            "k2".to_string(),
            make_record(vec![
                ("status", FieldValue::String("inactive".to_string())),
                ("id", FieldValue::Integer(2)),
            ]),
        )
        .unwrap();
    table
        .insert(
            "k3".to_string(),
            make_record(vec![
                ("status", FieldValue::String("active".to_string())),
                ("id", FieldValue::Integer(3)),
            ]),
        )
        .unwrap();

    // OR query: status = 'active' OR status = 'inactive'
    let or_expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
        }),
        op: BinaryOperator::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("inactive".to_string()))),
        }),
    };
    let values = table.sql_column_values_with_expr("id", &or_expr).unwrap();
    assert_eq!(values.len(), 3);

    // Only active: status = 'active'
    let active_expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("status".to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
    };
    let values = table
        .sql_column_values_with_expr("id", &active_expr)
        .unwrap();
    assert_eq!(values.len(), 2);
}

#[test]
fn test_optimized_table_exists_with_expr() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("x", FieldValue::Integer(5))]),
        )
        .unwrap();

    // BETWEEN: x BETWEEN 1 AND 10 — should exist
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("x".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        negated: false,
    };
    assert!(table.sql_exists_with_expr(&expr).unwrap());

    // BETWEEN: x BETWEEN 100 AND 200 — should not exist
    let expr2 = Expr::Between {
        expr: Box::new(Expr::Column("x".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(200))),
        negated: false,
    };
    assert!(!table.sql_exists_with_expr(&expr2).unwrap());
}

// === sql_scalar_with_expr aggregate tests ===

fn create_numeric_table() -> velostream::velostream::table::unified_table::OptimizedTableImpl {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    for (i, val) in [10i64, 20, 30, 20, 10].iter().enumerate() {
        table
            .insert(
                format!("k{}", i),
                make_record(vec![
                    ("amount", FieldValue::Integer(*val)),
                    ("label", FieldValue::String(format!("item{}", i))),
                ]),
            )
            .unwrap();
    }
    table
}

fn always_true_expr() -> Expr {
    // 1 = 1
    Expr::BinaryOp {
        left: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
    }
}

#[test]
fn test_scalar_count() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("COUNT(*)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Integer(5));
}

#[test]
fn test_scalar_sum() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("SUM(amount)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Integer(90));
}

#[test]
fn test_scalar_avg() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("AVG(amount)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 18.0).abs() < 0.001),
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_max() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("MAX(amount)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Integer(30));
}

#[test]
fn test_scalar_min() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("MIN(amount)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Integer(10));
}

#[test]
fn test_scalar_count_distinct() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    // Values are [10, 20, 30, 20, 10] → 3 distinct
    let result = table
        .sql_scalar_with_expr("COUNT(DISTINCT amount)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Integer(3));
}

#[test]
fn test_scalar_sum_distinct() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    // Distinct values: 10, 20, 30 → sum = 60
    let result = table
        .sql_scalar_with_expr("SUM(DISTINCT amount)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Integer(60));
}

#[test]
fn test_scalar_first_value() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("FIRST_VALUE(amount)", &always_true_expr())
        .unwrap();
    // Should be a non-null Integer
    match result {
        FieldValue::Integer(_) => {} // order is hash-map dependent, just check non-null
        other => panic!("Expected Integer, got {:?}", other),
    }
}

#[test]
fn test_scalar_last_value() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("LAST_VALUE(amount)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Integer(_) => {}
        other => panic!("Expected Integer, got {:?}", other),
    }
}

#[test]
fn test_scalar_listagg() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("LISTAGG(label)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::String(s) => {
            let parts: Vec<&str> = s.split(',').collect();
            assert_eq!(parts.len(), 5);
        }
        other => panic!("Expected String, got {:?}", other),
    }
}

#[test]
fn test_scalar_stddev_pop() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("STDDEV_POP(amount)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => {
            // values [10,20,30,20,10], mean=18, var_pop=56, stddev_pop≈7.483
            assert!(f > 7.0 && f < 8.0, "stddev_pop was {}", f);
        }
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_stddev_samp() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("STDDEV_SAMP(amount)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => {
            // var_samp = 56*5/4 = 70, stddev_samp ≈ 8.367
            assert!(f > 8.0 && f < 9.0, "stddev_samp was {}", f);
        }
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_var_pop() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("VAR_POP(amount)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => {
            // mean=18, sum_sq = (10-18)^2+(20-18)^2+(30-18)^2+(20-18)^2+(10-18)^2
            //        = 64+4+144+4+64 = 280, var_pop = 280/5 = 56
            assert!((f - 56.0).abs() < 0.001, "var_pop was {}", f);
        }
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_var_samp() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("VAR_SAMP(amount)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => {
            // var_samp = 280/4 = 70
            assert!((f - 70.0).abs() < 0.001, "var_samp was {}", f);
        }
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_count_with_key_lookup_optimization() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "user_123".to_string(),
            make_record(vec![("name", FieldValue::String("Alice".to_string()))]),
        )
        .unwrap();

    // key = 'user_123' — should use O(1) key lookup
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("key".to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::String("user_123".to_string()))),
    };
    let result = table.sql_scalar_with_expr("COUNT(*)", &expr).unwrap();
    assert_eq!(result, FieldValue::Integer(1));

    // key = 'nonexistent'
    let expr2 = Expr::BinaryOp {
        left: Box::new(Expr::Column("key".to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::String(
            "nonexistent".to_string(),
        ))),
    };
    let result2 = table.sql_scalar_with_expr("COUNT(*)", &expr2).unwrap();
    assert_eq!(result2, FieldValue::Integer(0));
}

#[test]
fn test_scalar_with_where_filter() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    // SUM(amount) WHERE amount > 15
    let where_expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("amount".to_string())),
        op: BinaryOperator::GreaterThan,
        right: Box::new(Expr::Literal(LiteralValue::Integer(15))),
    };
    let result = table
        .sql_scalar_with_expr("SUM(amount)", &where_expr)
        .unwrap();
    // Values > 15: [20, 30, 20] → sum = 70
    assert_eq!(result, FieldValue::Integer(70));
}

#[test]
fn test_scalar_empty_result() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    // WHERE amount > 1000 — no matches
    let where_expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("amount".to_string())),
        op: BinaryOperator::GreaterThan,
        right: Box::new(Expr::Literal(LiteralValue::Integer(1000))),
    };
    let result = table
        .sql_scalar_with_expr("AVG(amount)", &where_expr)
        .unwrap();
    assert_eq!(result, FieldValue::Null);
}

#[test]
fn test_scalar_collect_alias() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    let result = table
        .sql_scalar_with_expr("COLLECT(label)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::String(s) => {
            let parts: Vec<&str> = s.split(',').collect();
            assert_eq!(parts.len(), 5);
        }
        other => panic!("Expected String, got {:?}", other),
    }
}

#[test]
fn test_scalar_var_samp_single_element_returns_null() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("amount", FieldValue::Integer(42))]),
        )
        .unwrap();
    // Sample variance with n=1 should return Null (undefined)
    let result = table
        .sql_scalar_with_expr("VAR_SAMP(amount)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Null);
}

#[test]
fn test_scalar_stddev_samp_single_element_returns_null() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("amount", FieldValue::Integer(42))]),
        )
        .unwrap();
    let result = table
        .sql_scalar_with_expr("STDDEV_SAMP(amount)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Null);
}

#[test]
fn test_scalar_column_filter_optimization() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("status", FieldValue::String("active".to_string()))]),
        )
        .unwrap();
    table
        .insert(
            "k2".to_string(),
            make_record(vec![("status", FieldValue::String("active".to_string()))]),
        )
        .unwrap();
    table
        .insert(
            "k3".to_string(),
            make_record(vec![("status", FieldValue::String("inactive".to_string()))]),
        )
        .unwrap();

    // status = 'active' — should use column index O(1) path
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("status".to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
    };
    let result = table.sql_scalar_with_expr("COUNT(*)", &expr).unwrap();
    assert_eq!(result, FieldValue::Integer(2));
}

#[test]
fn test_scalar_sum_with_float_values() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("price", FieldValue::Float(1.5))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("price", FieldValue::Float(2.5))]),
        )
        .unwrap();
    let result = table
        .sql_scalar_with_expr("SUM(price)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 4.0).abs() < 0.001, "SUM was {}", f),
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_max_with_float_values() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("price", FieldValue::Float(1.5))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("price", FieldValue::Float(2.5))]),
        )
        .unwrap();
    let result = table
        .sql_scalar_with_expr("MAX(price)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 2.5).abs() < 0.001, "MAX was {}", f),
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_min_with_float_values() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("price", FieldValue::Float(1.5))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("price", FieldValue::Float(2.5))]),
        )
        .unwrap();
    let result = table
        .sql_scalar_with_expr("MIN(price)", &always_true_expr())
        .unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 1.5).abs() < 0.001, "MIN was {}", f),
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_avg_with_scaled_integer() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    // ScaledInteger(12345, 2) = 123.45
    // ScaledInteger(67890, 2) = 678.90
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("price", FieldValue::ScaledInteger(12345, 2))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("price", FieldValue::ScaledInteger(67890, 2))]),
        )
        .unwrap();
    let result = table
        .sql_scalar_with_expr("AVG(price)", &always_true_expr())
        .unwrap();
    match result {
        // AVG of 123.45 and 678.90 = 401.175
        FieldValue::Float(f) => assert!((f - 401.175).abs() < 0.001, "AVG was {}", f),
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_sum_with_scaled_integer() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    // ScaledInteger(12345, 2) = 123.45
    // ScaledInteger(67890, 2) = 678.90
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("price", FieldValue::ScaledInteger(12345, 2))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("price", FieldValue::ScaledInteger(67890, 2))]),
        )
        .unwrap();
    let result = table
        .sql_scalar_with_expr("SUM(price)", &always_true_expr())
        .unwrap();
    match result {
        // SUM of 123.45 and 678.90 = 802.35
        FieldValue::Float(f) => assert!((f - 802.35).abs() < 0.01, "SUM was {}", f),
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_scalar_count_with_no_where_clause_via_full_scan() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_numeric_table();
    // Compound WHERE that doesn't match key-lookup or column-filter patterns
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("amount".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(5))),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("amount".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(25))),
        }),
    };
    // Values: [10, 20, 30, 20, 10] — values in (5, 25) are [10, 20, 20, 10] = 4
    let result = table.sql_scalar_with_expr("COUNT(*)", &expr).unwrap();
    assert_eq!(result, FieldValue::Integer(4));
}

/// Test that all numeric aggregates work correctly with mixed Integer + Float inputs.
/// When any Float is present, results should be Float (not Integer).
#[test]
fn test_scalar_aggregates_mixed_int_float() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    // Mixed: Integer(10), Float(20.5), Integer(30)
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("val", FieldValue::Integer(10))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("val", FieldValue::Float(20.5))]),
        )
        .unwrap();
    table
        .insert(
            "k2".to_string(),
            make_record(vec![("val", FieldValue::Integer(30))]),
        )
        .unwrap();

    let expr = &always_true_expr();

    // SUM: 10 + 20.5 + 30 = 60.5 → Float (mixed types)
    let result = table.sql_scalar_with_expr("SUM(val)", expr).unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 60.5).abs() < 0.001, "SUM was {}", f),
        other => panic!("Expected Float for mixed SUM, got {:?}", other),
    }

    // AVG: 60.5 / 3 = 20.1667 → Float
    let result = table.sql_scalar_with_expr("AVG(val)", expr).unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 20.1667).abs() < 0.01, "AVG was {}", f),
        other => panic!("Expected Float for mixed AVG, got {:?}", other),
    }

    // MAX: 30 → Float (because mixed types force Float)
    let result = table.sql_scalar_with_expr("MAX(val)", expr).unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 30.0).abs() < 0.001, "MAX was {}", f),
        other => panic!("Expected Float for mixed MAX, got {:?}", other),
    }

    // MIN: 10 → Float (because mixed types force Float)
    let result = table.sql_scalar_with_expr("MIN(val)", expr).unwrap();
    match result {
        FieldValue::Float(f) => assert!((f - 10.0).abs() < 0.001, "MIN was {}", f),
        other => panic!("Expected Float for mixed MIN, got {:?}", other),
    }

    // STDDEV_POP: should produce Float
    let result = table.sql_scalar_with_expr("STDDEV_POP(val)", expr).unwrap();
    match result {
        FieldValue::Float(f) => assert!(f > 0.0, "STDDEV_POP should be positive, got {}", f),
        other => panic!("Expected Float for mixed STDDEV_POP, got {:?}", other),
    }

    // VAR_POP: should produce Float
    let result = table.sql_scalar_with_expr("VAR_POP(val)", expr).unwrap();
    match result {
        FieldValue::Float(f) => assert!(f > 0.0, "VAR_POP should be positive, got {}", f),
        other => panic!("Expected Float for mixed VAR_POP, got {:?}", other),
    }
}

// =============================================================================
// Type safety tests: numeric aggregates reject strings, MAX/MIN support strings
// =============================================================================

fn create_string_table() -> velostream::velostream::table::unified_table::OptimizedTableImpl {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    for (i, name) in ["charlie", "alice", "bob", "delta"].iter().enumerate() {
        table
            .insert(
                format!("k{}", i),
                make_record(vec![("name", FieldValue::String(name.to_string()))]),
            )
            .unwrap();
    }
    table
}

#[test]
fn test_sum_on_string_field_returns_error() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    let result = table.sql_scalar_with_expr("SUM(name)", &always_true_expr());
    assert!(result.is_err(), "SUM on strings should return an error");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("requires numeric input"),
        "Error should mention numeric requirement, got: {}",
        err_msg
    );
}

#[test]
fn test_avg_on_string_field_returns_error() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    let result = table.sql_scalar_with_expr("AVG(name)", &always_true_expr());
    assert!(result.is_err(), "AVG on strings should return an error");
}

#[test]
fn test_stddev_pop_on_string_field_returns_error() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    let result = table.sql_scalar_with_expr("STDDEV_POP(name)", &always_true_expr());
    assert!(
        result.is_err(),
        "STDDEV_POP on strings should return an error"
    );
}

#[test]
fn test_var_samp_on_string_field_returns_error() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    let result = table.sql_scalar_with_expr("VAR_SAMP(name)", &always_true_expr());
    assert!(
        result.is_err(),
        "VAR_SAMP on strings should return an error"
    );
}

#[test]
fn test_max_on_string_field_lexicographic() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    // ["charlie", "alice", "bob", "delta"] → MAX = "delta"
    let result = table
        .sql_scalar_with_expr("MAX(name)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::String("delta".to_string()));
}

#[test]
fn test_min_on_string_field_lexicographic() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    // ["charlie", "alice", "bob", "delta"] → MIN = "alice"
    let result = table
        .sql_scalar_with_expr("MIN(name)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::String("alice".to_string()));
}

#[test]
fn test_max_on_timestamp_field() {
    use chrono::NaiveDate;
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    let ts1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(10, 0, 0)
        .unwrap();
    let ts2 = NaiveDate::from_ymd_opt(2024, 6, 20)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let ts3 = NaiveDate::from_ymd_opt(2024, 3, 10)
        .unwrap()
        .and_hms_opt(8, 0, 0)
        .unwrap();
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("ts", FieldValue::Timestamp(ts1))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("ts", FieldValue::Timestamp(ts2))]),
        )
        .unwrap();
    table
        .insert(
            "k2".to_string(),
            make_record(vec![("ts", FieldValue::Timestamp(ts3))]),
        )
        .unwrap();
    let result = table
        .sql_scalar_with_expr("MAX(ts)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Timestamp(ts2));

    let result = table
        .sql_scalar_with_expr("MIN(ts)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Timestamp(ts1));
}

#[test]
fn test_max_mixed_string_and_int_returns_error() {
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};
    let table = OptimizedTableImpl::new();
    table
        .insert(
            "k0".to_string(),
            make_record(vec![("val", FieldValue::String("hello".to_string()))]),
        )
        .unwrap();
    table
        .insert(
            "k1".to_string(),
            make_record(vec![("val", FieldValue::Integer(42))]),
        )
        .unwrap();
    let result = table.sql_scalar_with_expr("MAX(val)", &always_true_expr());
    assert!(
        result.is_err(),
        "MAX on mixed string+int should return an error"
    );
}

#[test]
fn test_count_on_string_field_works() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    // COUNT is type-agnostic — should work on any type
    let result = table
        .sql_scalar_with_expr("COUNT(*)", &always_true_expr())
        .unwrap();
    assert_eq!(result, FieldValue::Integer(4));
}

#[test]
fn test_listagg_on_string_field_works() {
    use velostream::velostream::table::unified_table::UnifiedTable;
    let table = create_string_table();
    let result = table
        .sql_scalar_with_expr("LISTAGG(name)", &always_true_expr())
        .unwrap();
    // Order is non-deterministic (HashMap), but should contain all 4 names
    match result {
        FieldValue::String(s) => {
            let parts: Vec<&str> = s.split(',').collect();
            assert_eq!(parts.len(), 4);
            assert!(parts.contains(&"alice"));
            assert!(parts.contains(&"bob"));
            assert!(parts.contains(&"charlie"));
            assert!(parts.contains(&"delta"));
        }
        other => panic!("Expected String, got {:?}", other),
    }
}
