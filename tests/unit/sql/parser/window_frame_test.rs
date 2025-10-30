/*!
# Tests for Window Frame Support (ROWS BETWEEN clauses)

Comprehensive test suite for OVER clauses and window frame specifications.
Tests parsing of PARTITION BY, ORDER BY, and ROWS/RANGE BETWEEN clauses.
*/

use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_basic_over_clause_parsing() {
    let parser = StreamingSqlParser::new();

    // Test basic OVER () clause
    let query = "SELECT ROW_NUMBER() OVER () as row_num FROM test_stream";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse basic OVER clause: {:?}",
        result.err()
    );

    // Verify the AST structure
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 1);
            match &fields[0] {
                SelectField::Expression { expr, alias } => {
                    assert_eq!(alias.as_ref().unwrap(), "row_num");
                    match expr {
                        Expr::WindowFunction {
                            function_name,
                            args,
                            over_clause,
                        } => {
                            assert_eq!(function_name, "ROW_NUMBER");
                            assert_eq!(args.len(), 0);
                            assert!(over_clause.partition_by.is_empty());
                            assert!(over_clause.order_by.is_empty());
                            assert!(over_clause.window_frame.is_none());
                        }
                        _ => panic!("Expected WindowFunction expression, got: {:?}", expr),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_partition_by_parsing() {
    let parser = StreamingSqlParser::new();

    // Test PARTITION BY clause
    let query = "SELECT ROW_NUMBER() OVER (PARTITION BY customer_id) as row_num FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse PARTITION BY: {:?}",
        result.err()
    );

    // Verify PARTITION BY columns
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    assert_eq!(over_clause.partition_by, vec!["customer_id"]);
                    assert!(over_clause.order_by.is_empty());
                    assert!(over_clause.window_frame.is_none());
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_multiple_partition_columns() {
    let parser = StreamingSqlParser::new();

    // Test multiple PARTITION BY columns
    let query =
        "SELECT COUNT(*) OVER (PARTITION BY customer_id, product_category) as count FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse multiple PARTITION BY columns: {:?}",
        result.err()
    );

    // Verify multiple partition columns
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    assert_eq!(
                        over_clause.partition_by,
                        vec!["customer_id", "product_category"]
                    );
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_order_by_parsing() {
    let parser = StreamingSqlParser::new();

    // Test ORDER BY clause
    let query = "SELECT ROW_NUMBER() OVER (ORDER BY order_date ASC) as row_num FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse ORDER BY: {:?}",
        result.err()
    );

    // Verify ORDER BY clause
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    assert!(over_clause.partition_by.is_empty());
                    assert_eq!(over_clause.order_by.len(), 1);
                    match &over_clause.order_by[0].expr {
                        Expr::Column(col) => assert_eq!(col, "order_date"),
                        _ => panic!("Expected column expression in ORDER BY"),
                    }
                    assert_eq!(over_clause.order_by[0].direction, OrderDirection::Asc);
                    assert!(over_clause.window_frame.is_none());
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_order_by_desc() {
    let parser = StreamingSqlParser::new();

    // Test ORDER BY DESC
    let query = "SELECT ROW_NUMBER() OVER (ORDER BY order_date DESC) as row_num FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse ORDER BY DESC: {:?}",
        result.err()
    );

    // Verify ORDER BY DESC
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    assert_eq!(over_clause.order_by[0].direction, OrderDirection::Desc);
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_rows_between_unbounded_preceding() {
    let parser = StreamingSqlParser::new();

    // Test ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    let query = "SELECT SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse ROWS BETWEEN UNBOUNDED PRECEDING: {:?}",
        result.err()
    );

    // Verify window frame
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    let window_frame = over_clause.window_frame.as_ref().unwrap();
                    assert_eq!(window_frame.frame_type, FrameType::Rows);
                    assert_eq!(window_frame.start_bound, FrameBound::UnboundedPreceding);
                    assert_eq!(
                        window_frame.end_bound.as_ref().unwrap(),
                        &FrameBound::CurrentRow
                    );
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_rows_between_current_and_following() {
    let parser = StreamingSqlParser::new();

    // Test ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
    let query = "SELECT AVG(amount) OVER (ORDER BY order_date ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) as avg_next FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse ROWS BETWEEN CURRENT AND FOLLOWING: {:?}",
        result.err()
    );

    // Verify window frame
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    let window_frame = over_clause.window_frame.as_ref().unwrap();
                    assert_eq!(window_frame.frame_type, FrameType::Rows);
                    assert_eq!(window_frame.start_bound, FrameBound::CurrentRow);
                    assert_eq!(
                        window_frame.end_bound.as_ref().unwrap(),
                        &FrameBound::Following(1)
                    );
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_rows_between_preceding() {
    let parser = StreamingSqlParser::new();

    // Test ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
    let query = "SELECT COUNT(*) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) as prev_count FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse ROWS BETWEEN PRECEDING: {:?}",
        result.err()
    );

    // Verify window frame
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    let window_frame = over_clause.window_frame.as_ref().unwrap();
                    assert_eq!(window_frame.frame_type, FrameType::Rows);
                    assert_eq!(window_frame.start_bound, FrameBound::Preceding(2));
                    assert_eq!(
                        window_frame.end_bound.as_ref().unwrap(),
                        &FrameBound::Preceding(1)
                    );
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_rows_between_unbounded_following() {
    let parser = StreamingSqlParser::new();

    // Test ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    let query = "SELECT MAX(amount) OVER (ORDER BY order_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as max_remaining FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse ROWS BETWEEN UNBOUNDED FOLLOWING: {:?}",
        result.err()
    );

    // Verify window frame
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    let window_frame = over_clause.window_frame.as_ref().unwrap();
                    assert_eq!(window_frame.frame_type, FrameType::Rows);
                    assert_eq!(window_frame.start_bound, FrameBound::CurrentRow);
                    assert_eq!(
                        window_frame.end_bound.as_ref().unwrap(),
                        &FrameBound::UnboundedFollowing
                    );
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_range_between() {
    let parser = StreamingSqlParser::new();

    // Test RANGE BETWEEN (instead of ROWS BETWEEN)
    let query = "SELECT SUM(amount) OVER (ORDER BY order_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as range_sum FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse RANGE BETWEEN: {:?}",
        result.err()
    );

    // Verify window frame
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    let window_frame = over_clause.window_frame.as_ref().unwrap();
                    assert_eq!(window_frame.frame_type, FrameType::Range);
                    assert_eq!(window_frame.start_bound, FrameBound::UnboundedPreceding);
                    assert_eq!(
                        window_frame.end_bound.as_ref().unwrap(),
                        &FrameBound::CurrentRow
                    );
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_complete_over_clause() {
    let parser = StreamingSqlParser::new();

    // Test complete OVER clause with all features
    let query = "SELECT ROW_NUMBER() OVER (PARTITION BY customer_id, region ORDER BY order_date DESC ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) as row_num FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse complete OVER clause: {:?}",
        result.err()
    );

    // Verify all components
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::WindowFunction { over_clause, .. } => {
                            // Verify PARTITION BY
                            assert_eq!(over_clause.partition_by, vec!["customer_id", "region"]);

                            // Verify ORDER BY
                            assert_eq!(over_clause.order_by.len(), 1);
                            match &over_clause.order_by[0].expr {
                                Expr::Column(col) => assert_eq!(col, "order_date"),
                                _ => panic!("Expected column expression in ORDER BY"),
                            }
                            assert_eq!(over_clause.order_by[0].direction, OrderDirection::Desc);

                            // Verify window frame
                            let window_frame = over_clause.window_frame.as_ref().unwrap();
                            assert_eq!(window_frame.frame_type, FrameType::Rows);
                            assert_eq!(window_frame.start_bound, FrameBound::Preceding(1));
                            assert_eq!(
                                window_frame.end_bound.as_ref().unwrap(),
                                &FrameBound::Following(2)
                            );
                        }
                        _ => panic!("Expected WindowFunction expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_multiple_order_by_columns() {
    let parser = StreamingSqlParser::new();

    // Test multiple ORDER BY columns
    let query = "SELECT LAG(amount) OVER (ORDER BY order_date ASC, customer_id DESC) as lag_amount FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse multiple ORDER BY columns: {:?}",
        result.err()
    );

    // Verify multiple ORDER BY columns
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::WindowFunction { over_clause, .. } => {
                            assert_eq!(over_clause.order_by.len(), 2);

                            // First ORDER BY column
                            match &over_clause.order_by[0].expr {
                                Expr::Column(col) => assert_eq!(col, "order_date"),
                                _ => panic!("Expected column expression in first ORDER BY"),
                            }
                            assert_eq!(over_clause.order_by[0].direction, OrderDirection::Asc);

                            // Second ORDER BY column
                            match &over_clause.order_by[1].expr {
                                Expr::Column(col) => assert_eq!(col, "customer_id"),
                                _ => panic!("Expected column expression in second ORDER BY"),
                            }
                            assert_eq!(over_clause.order_by[1].direction, OrderDirection::Desc);
                        }
                        _ => panic!("Expected WindowFunction expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_window_function_with_arguments() {
    let parser = StreamingSqlParser::new();

    // Test window function with arguments (LAG with offset)
    let query = "SELECT LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse window function with arguments: {:?}",
        result.err()
    );

    // Verify function name and arguments
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::WindowFunction {
                            function_name,
                            args,
                            ..
                        } => {
                            assert_eq!(function_name, "LAG");
                            assert_eq!(args.len(), 2);

                            // First argument should be a column
                            match &args[0] {
                                Expr::Column(col) => assert_eq!(col, "amount"),
                                _ => panic!("Expected column as first argument"),
                            }

                            // Second argument should be a literal 1
                            match &args[1] {
                                Expr::Literal(LiteralValue::Integer(val)) => assert_eq!(*val, 1),
                                _ => panic!("Expected integer literal as second argument"),
                            }
                        }
                        _ => panic!("Expected WindowFunction expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_common_window_functions() {
    let parser = StreamingSqlParser::new();

    let test_queries = vec![
        "SELECT ROW_NUMBER() OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY order_date) as row_num FROM orders",
        "SELECT RANK() OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY amount DESC) as rank FROM orders",
        "SELECT DENSE_RANK() OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY amount DESC) as dense_rank FROM orders",
        "SELECT LAG(amount) OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY order_date) as prev_amount FROM orders",
        "SELECT LEAD(amount, 2) OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY order_date) as next_amount FROM orders",
        "SELECT FIRST_VALUE(amount) OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY order_date) as first_amount FROM orders",
        "SELECT LAST_VALUE(amount) OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY order_date) as last_amount FROM orders",
    ];

    for query in test_queries {
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse query: {}, error: {:?}",
            query,
            result.err()
        );

        // Verify it's parsed as a WindowFunction
        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, .. } => {
                        match expr {
                            Expr::WindowFunction { .. } => {
                                // Successfully parsed as window function
                            }
                            _ => panic!("Expected WindowFunction expression for query: {}", query),
                        }
                    }
                    _ => panic!("Expected expression field for query: {}", query),
                }
            }
            _ => panic!("Expected SELECT query for: {}", query),
        }
    }
}

#[test]
fn test_window_frame_error_cases() {
    let parser = StreamingSqlParser::new();

    let invalid_queries = vec![
        // Missing BETWEEN
        "SELECT SUM(amount) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) FROM orders",
        // Missing AND
        "SELECT SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING) FROM orders",
        // Invalid frame bound
        "SELECT SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN INVALID PRECEDING AND CURRENT ROW) FROM orders",
        // Missing number before PRECEDING
        "SELECT SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN PRECEDING AND CURRENT ROW) FROM orders",
        // Invalid combination
        "SELECT SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN CURRENT AND CURRENT ROW) FROM orders",
    ];

    for query in invalid_queries {
        let result = parser.parse(query);
        assert!(
            result.is_err(),
            "Query should have failed but succeeded: {}",
            query
        );
    }
}
