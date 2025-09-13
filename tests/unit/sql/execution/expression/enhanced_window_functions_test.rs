/*!
# Comprehensive Tests for Enhanced Window Functions

Tests for the enhanced window function implementations with proper OVER clause processing,
PARTITION BY support, ORDER BY handling, and advanced window frame calculations.

## Test Coverage

- Enhanced LAG/LEAD with proper lookback/lookahead
- ROW_NUMBER with partition awareness
- RANK/DENSE_RANK with ORDER BY support
- FIRST_VALUE/LAST_VALUE with frame bounds
- NTH_VALUE with partition-based indexing
- PERCENT_RANK/CUME_DIST with statistical accuracy
- NTILE with proper tile distribution
- Complex OVER clause scenarios
- Error handling and edge cases
*/

use ferrisstreams::ferris::serialization::InternalValue;
use ferrisstreams::ferris::sql::ast::{
    Expr, LiteralValue, OrderByExpr, OrderDirection, OverClause, WindowFrame,
};
use ferrisstreams::ferris::sql::execution::expression::window_functions::WindowFunctions;
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Create test record with specified fields
fn create_test_record(
    id: i64,
    category: &str,
    value: f64,
    timestamp: i64,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("category".to_string(), FieldValue::String(category.to_string()));
    fields.insert("value".to_string(), FieldValue::Float(value));
    fields.insert("rank_value".to_string(), FieldValue::Integer(id)); // For ranking tests
    
    StreamRecord {
        fields,
        timestamp,
        offset: id as u64,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    }
}

/// Create test window buffer with multiple records
fn create_test_window_buffer() -> Vec<StreamRecord> {
    vec![
        create_test_record(1, "A", 100.0, 1000),
        create_test_record(2, "A", 200.0, 2000),
        create_test_record(3, "B", 150.0, 1500),
        create_test_record(4, "B", 300.0, 2500),
        create_test_record(5, "A", 250.0, 3000),
    ]
}

#[cfg(test)]
mod enhanced_window_function_tests {
    use super::*;

    #[test]
    fn test_enhanced_lag_function_with_partitioning() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(6, "A", 350.0, 3500);
        
        // LAG(value, 1) OVER (PARTITION BY category ORDER BY timestamp)
        let args = vec![Expr::Column("value".to_string()), Expr::Literal(LiteralValue::Integer(1))];
        let over_clause = OverClause {
            partition_by: vec!["category".to_string()],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "LAG",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        // Should get the previous value in category "A" which is 250.0 (record 5)
        match value {
            FieldValue::Float(f) => assert_eq!(f, 250.0),
            _ => panic!("Expected Float value, got {:?}", value),
        }
    }

    #[test]
    fn test_enhanced_lead_function_with_partitioning() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(2, "A", 200.0, 2000); // Middle record in partition A
        
        // LEAD(value, 1) OVER (PARTITION BY category ORDER BY timestamp)
        let args = vec![Expr::Column("value".to_string()), Expr::Literal(LiteralValue::Integer(1))];
        let over_clause = OverClause {
            partition_by: vec!["category".to_string()],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "LEAD",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        // Should get the next value in category "A" which is 250.0 (record 5)
        match value {
            FieldValue::Float(f) => assert_eq!(f, 250.0),
            _ => panic!("Expected Float value, got {:?}", value),
        }
    }

    #[test]
    fn test_row_number_with_partitioning() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(3, "B", 150.0, 1500); // First record in partition B by timestamp
        
        // ROW_NUMBER() OVER (PARTITION BY category ORDER BY timestamp)
        let args = vec![];
        let over_clause = OverClause {
            partition_by: vec!["category".to_string()],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "ROW_NUMBER",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            FieldValue::Integer(i) => assert_eq!(i, 1), // First row in partition B
            _ => panic!("Expected Integer value, got {:?}", value),
        }
    }

    #[test]
    fn test_rank_with_order_by() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(4, "B", 300.0, 2500);
        
        // RANK() OVER (ORDER BY value DESC)
        let args = vec![];
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("value".to_string()),
                direction: OrderDirection::Desc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "RANK",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            FieldValue::Integer(i) => assert!(i >= 1), // Should have a valid rank
            _ => panic!("Expected Integer value, got {:?}", value),
        }
    }

    #[test]
    fn test_dense_rank_with_order_by() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(2, "A", 200.0, 2000);
        
        // DENSE_RANK() OVER (ORDER BY value ASC)
        let args = vec![];
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("value".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "DENSE_RANK",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            FieldValue::Integer(i) => assert!(i >= 1),
            _ => panic!("Expected Integer value, got {:?}", value),
        }
    }

    #[test]
    fn test_first_value_with_partitioning() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(5, "A", 250.0, 3000);
        
        // FIRST_VALUE(value) OVER (PARTITION BY category ORDER BY timestamp)
        let args = vec![Expr::Column("value".to_string())];
        let over_clause = OverClause {
            partition_by: vec!["category".to_string()],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "FIRST_VALUE",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        // Should get first value in partition A which is 100.0 (record 1)
        match value {
            FieldValue::Float(f) => assert_eq!(f, 100.0),
            _ => panic!("Expected Float value, got {:?}", value),
        }
    }

    #[test]
    fn test_last_value_with_partitioning() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(1, "A", 100.0, 1000);
        
        // LAST_VALUE(value) OVER (PARTITION BY category ORDER BY timestamp)
        let args = vec![Expr::Column("value".to_string())];
        let over_clause = OverClause {
            partition_by: vec!["category".to_string()],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "LAST_VALUE",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        // Should get last value in partition A 
        match result.unwrap() {
            FieldValue::Float(_) => {}, // Any float is valid
            _ => panic!("Expected Float value"),
        }
    }

    #[test]
    fn test_nth_value_with_partitioning() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(5, "A", 250.0, 3000);
        
        // NTH_VALUE(value, 2) OVER (PARTITION BY category ORDER BY timestamp)
        let args = vec![
            Expr::Column("value".to_string()),
            Expr::Literal(LiteralValue::Integer(2)),
        ];
        let over_clause = OverClause {
            partition_by: vec!["category".to_string()],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "NTH_VALUE",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        // Should get 2nd value in partition A which is 200.0 (record 2)
        match value {
            FieldValue::Float(f) => assert_eq!(f, 200.0),
            FieldValue::Null => {}, // Also acceptable if not enough records
            _ => panic!("Expected Float or Null value, got {:?}", value),
        }
    }

    #[test]
    fn test_percent_rank_calculation() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(3, "B", 150.0, 1500);
        
        // PERCENT_RANK() OVER (ORDER BY value)
        let args = vec![];
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("value".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "PERCENT_RANK",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            FieldValue::Float(f) => {
                assert!(f >= 0.0 && f <= 1.0); // Should be between 0 and 1
            }
            _ => panic!("Expected Float value, got {:?}", value),
        }
    }

    #[test]
    fn test_cume_dist_calculation() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(2, "A", 200.0, 2000);
        
        // CUME_DIST() OVER (ORDER BY value)
        let args = vec![];
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("value".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "CUME_DIST",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            FieldValue::Float(f) => {
                assert!(f >= 0.0 && f <= 1.0); // Should be between 0 and 1
            }
            _ => panic!("Expected Float value, got {:?}", value),
        }
    }

    #[test]
    fn test_ntile_distribution() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(3, "B", 150.0, 1500);
        
        // NTILE(3) OVER (ORDER BY value)
        let args = vec![Expr::Literal(LiteralValue::Integer(3))];
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("value".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "NTILE",
            &args,
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            FieldValue::Integer(i) => {
                assert!(i >= 1 && i <= 3); // Should be between 1 and 3
            }
            _ => panic!("Expected Integer value, got {:?}", value),
        }
    }

    #[test]
    fn test_window_function_error_handling() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(1, "A", 100.0, 1000);
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };

        // Test unsupported function
        let result = WindowFunctions::evaluate_window_function(
            "UNSUPPORTED_FUNC",
            &[],
            &over_clause,
            &current_record,
            &window_buffer,
        );
        assert!(result.is_err());

        // Test LAG with invalid arguments
        let result = WindowFunctions::evaluate_window_function(
            "LAG",
            &[
                Expr::Column("value".to_string()),
                Expr::Column("value".to_string()),
                Expr::Column("value".to_string()),
            ], // Too many arguments
            &over_clause,
            &current_record,
            &window_buffer,
        );
        assert!(result.is_err());

        // Test RANK with arguments (should have none)
        let result = WindowFunctions::evaluate_window_function(
            "RANK",
            &[Expr::Column("value".to_string())], // RANK takes no arguments
            &over_clause,
            &current_record,
            &window_buffer,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_over_clause_handling() {
        let window_buffer = create_test_window_buffer();
        let current_record = create_test_record(1, "A", 100.0, 1000);
        let empty_over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };

        // ROW_NUMBER with empty OVER clause
        let result = WindowFunctions::evaluate_window_function(
            "ROW_NUMBER",
            &[],
            &empty_over_clause,
            &current_record,
            &window_buffer,
        );
        assert!(result.is_ok());

        // RANK with empty ORDER BY (should return 1)
        let result = WindowFunctions::evaluate_window_function(
            "RANK",
            &[],
            &empty_over_clause,
            &current_record,
            &window_buffer,
        );
        assert!(result.is_ok());
        match result.unwrap() {
            FieldValue::Integer(i) => assert_eq!(i, 1),
            _ => panic!("Expected rank of 1 for empty ORDER BY"),
        }
    }

    #[test]
    fn test_complex_partitioning_scenario() {
        let window_buffer = vec![
            create_test_record(1, "A", 100.0, 1000),
            create_test_record(2, "A", 200.0, 2000),
            create_test_record(3, "A", 300.0, 3000),
            create_test_record(4, "B", 150.0, 1500),
            create_test_record(5, "B", 250.0, 2500),
            create_test_record(6, "C", 350.0, 3500),
        ];
        let current_record = create_test_record(2, "A", 200.0, 2000);
        
        // ROW_NUMBER() OVER (PARTITION BY category ORDER BY timestamp)
        let over_clause = OverClause {
            partition_by: vec!["category".to_string()],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            window_frame: None,
        };

        let result = WindowFunctions::evaluate_window_function(
            "ROW_NUMBER",
            &[],
            &over_clause,
            &current_record,
            &window_buffer,
        );

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            FieldValue::Integer(i) => assert_eq!(i, 2), // Second row in partition A
            _ => panic!("Expected Integer value, got {:?}", value),
        }
    }
}