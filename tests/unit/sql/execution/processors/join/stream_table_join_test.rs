//! Tests for Stream-Table JOIN functionality
//!
//! Validates the optimized Stream-Table join processor for financial services use cases.

use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::sql::ast::{BinaryOperator, Expr, JoinClause, JoinType, LiteralValue, StreamSource};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::processors::stream_table_join::StreamTableJoinProcessor;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::SqlError;
use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};

/// Create a test stream record representing a trade
fn create_trade_record(trade_id: &str, user_id: i64, symbol: &str, quantity: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("trade_id".to_string(), FieldValue::String(trade_id.to_string()));
    fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert("quantity".to_string(), FieldValue::Integer(quantity));

    StreamRecord {
        timestamp: Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        fields,
        headers: HashMap::new(),
        event_time: Some(Utc::now()),
    }
}

/// Create a test user profile table
fn create_user_profiles_table() -> Arc<OptimizedTableImpl> {
    let table = Arc::new(OptimizedTableImpl::new());

    // Add user profiles
    let mut user1 = HashMap::new();
    user1.insert("user_id".to_string(), FieldValue::Integer(1));
    user1.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    user1.insert("tier".to_string(), FieldValue::String("GOLD".to_string()));
    user1.insert("risk_score".to_string(), FieldValue::Integer(75));

    let mut user2 = HashMap::new();
    user2.insert("user_id".to_string(), FieldValue::Integer(2));
    user2.insert("name".to_string(), FieldValue::String("Bob".to_string()));
    user2.insert("tier".to_string(), FieldValue::String("SILVER".to_string()));
    user2.insert("risk_score".to_string(), FieldValue::Integer(50));

    let mut user3 = HashMap::new();
    user3.insert("user_id".to_string(), FieldValue::Integer(3));
    user3.insert("name".to_string(), FieldValue::String("Charlie".to_string()));
    user3.insert("tier".to_string(), FieldValue::String("BRONZE".to_string()));
    user3.insert("risk_score".to_string(), FieldValue::Integer(25));

    // Insert records
    table.insert("user_1".to_string(), user1).unwrap();
    table.insert("user_2".to_string(), user2).unwrap();
    table.insert("user_3".to_string(), user3).unwrap();

    table
}

/// Create a market data table
fn create_market_data_table() -> Arc<OptimizedTableImpl> {
    let table = Arc::new(OptimizedTableImpl::new());

    // Add market data
    let mut aapl = HashMap::new();
    aapl.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    aapl.insert("current_price".to_string(), FieldValue::Float(150.25));
    aapl.insert("volume".to_string(), FieldValue::Integer(1000000));

    let mut googl = HashMap::new();
    googl.insert("symbol".to_string(), FieldValue::String("GOOGL".to_string()));
    googl.insert("current_price".to_string(), FieldValue::Float(2750.50));
    googl.insert("volume".to_string(), FieldValue::Integer(500000));

    let mut msft = HashMap::new();
    msft.insert("symbol".to_string(), FieldValue::String("MSFT".to_string()));
    msft.insert("current_price".to_string(), FieldValue::Float(380.75));
    msft.insert("volume".to_string(), FieldValue::Integer(750000));

    // Insert records
    table.insert("market_aapl".to_string(), aapl).unwrap();
    table.insert("market_googl".to_string(), googl).unwrap();
    table.insert("market_msft".to_string(), msft).unwrap();

    table
}

#[test]
fn test_stream_table_join_detection() {
    // Test that we correctly identify stream-table join patterns
    assert!(StreamTableJoinProcessor::is_stream_table_join(
        &StreamSource::Stream("trades".to_string()),
        &StreamSource::Table("user_profiles".to_string())
    ));

    assert!(StreamTableJoinProcessor::is_stream_table_join(
        &StreamSource::Table("user_profiles".to_string()),
        &StreamSource::Stream("trades".to_string())
    ));

    assert!(!StreamTableJoinProcessor::is_stream_table_join(
        &StreamSource::Stream("trades".to_string()),
        &StreamSource::Stream("orders".to_string())
    ));

    assert!(!StreamTableJoinProcessor::is_stream_table_join(
        &StreamSource::Table("users".to_string()),
        &StreamSource::Table("products".to_string())
    ));
}

#[test]
fn test_stream_table_inner_join() {
    let processor = StreamTableJoinProcessor::new();

    // Create test data
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let user_table = create_user_profiles_table();

    // Create processor context with the table
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert("user_profiles".to_string(), user_table.clone() as Arc<dyn UnifiedTable>);

    // Create join clause: trades.user_id = user_profiles.user_id
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: Some("u".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        window: None,
    };

    // Process the join
    let results = processor
        .process_stream_table_join(&trade, &join_clause, &mut context)
        .unwrap();

    // Verify results
    assert_eq!(results.len(), 1, "Inner join should produce one result");
    let joined = &results[0];

    // Check stream fields are present
    assert_eq!(
        joined.fields.get("trade_id"),
        Some(&FieldValue::String("T001".to_string()))
    );
    assert_eq!(joined.fields.get("user_id"), Some(&FieldValue::Integer(1)));
    assert_eq!(
        joined.fields.get("symbol"),
        Some(&FieldValue::String("AAPL".to_string()))
    );

    // Check table fields are added with alias
    assert_eq!(
        joined.fields.get("u.name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(
        joined.fields.get("u.tier"),
        Some(&FieldValue::String("GOLD".to_string()))
    );
    assert_eq!(joined.fields.get("u.risk_score"), Some(&FieldValue::Integer(75)));
}

#[test]
fn test_stream_table_left_join_with_match() {
    let processor = StreamTableJoinProcessor::new();

    // Create test data
    let trade = create_trade_record("T002", 2, "GOOGL", 50);
    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert("user_profiles".to_string(), user_table.clone() as Arc<dyn UnifiedTable>);

    // Create LEFT JOIN clause
    let join_clause = JoinClause {
        join_type: JoinType::Left,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: None,  // No alias
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        window: None,
    };

    // Process the join
    let results = processor
        .process_stream_table_join(&trade, &join_clause, &mut context)
        .unwrap();

    // Verify results
    assert_eq!(results.len(), 1, "Left join with match should produce one result");
    let joined = &results[0];

    // Check both stream and table fields are present
    assert_eq!(joined.fields.get("user_id"), Some(&FieldValue::Integer(2)));
    assert_eq!(
        joined.fields.get("name"),
        Some(&FieldValue::String("Bob".to_string()))
    );
    assert_eq!(
        joined.fields.get("tier"),
        Some(&FieldValue::String("SILVER".to_string()))
    );
}

#[test]
fn test_stream_table_left_join_without_match() {
    let processor = StreamTableJoinProcessor::new();

    // Create trade with non-existent user_id
    let trade = create_trade_record("T003", 999, "MSFT", 200);
    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert("user_profiles".to_string(), user_table.clone() as Arc<dyn UnifiedTable>);

    // Create LEFT JOIN clause
    let join_clause = JoinClause {
        join_type: JoinType::Left,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: Some("u".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        window: None,
    };

    // Process the join
    let results = processor
        .process_stream_table_join(&trade, &join_clause, &mut context)
        .unwrap();

    // Verify results
    assert_eq!(
        results.len(),
        1,
        "Left join without match should still produce stream record"
    );
    let joined = &results[0];

    // Check stream fields are present
    assert_eq!(joined.fields.get("user_id"), Some(&FieldValue::Integer(999)));
    assert_eq!(
        joined.fields.get("trade_id"),
        Some(&FieldValue::String("T003".to_string()))
    );

    // Table fields should not be present (no match)
    assert!(!joined.fields.contains_key("u.name"));
    assert!(!joined.fields.contains_key("u.tier"));
    assert!(!joined.fields.contains_key("u.risk_score"));
}

#[test]
fn test_batch_stream_table_join() {
    let processor = StreamTableJoinProcessor::new();

    // Create multiple trades
    let trades = vec![
        create_trade_record("T001", 1, "AAPL", 100),
        create_trade_record("T002", 2, "GOOGL", 50),
        create_trade_record("T003", 3, "MSFT", 75),
        create_trade_record("T004", 999, "TSLA", 25),  // Non-existent user
    ];

    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert("user_profiles".to_string(), user_table.clone() as Arc<dyn UnifiedTable>);

    // Create INNER JOIN clause
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: Some("user".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        window: None,
    };

    // Process batch join
    let results = processor
        .process_batch_stream_table_join(trades, &join_clause, &mut context)
        .unwrap();

    // Verify results - should have 3 matches (user 999 doesn't exist)
    assert_eq!(results.len(), 3, "Should have 3 successful inner joins");

    // Verify first result
    let first = &results[0];
    assert_eq!(
        first.fields.get("trade_id"),
        Some(&FieldValue::String("T001".to_string()))
    );
    assert_eq!(
        first.fields.get("user.name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(
        first.fields.get("user.tier"),
        Some(&FieldValue::String("GOLD".to_string()))
    );
}

#[test]
fn test_multi_field_join_condition() {
    let processor = StreamTableJoinProcessor::new();

    // Create trade record
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let market_table = create_market_data_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert("market_data".to_string(), market_table.clone() as Arc<dyn UnifiedTable>);

    // Create join on symbol
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("market_data".to_string()),
        right_alias: Some("m".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("symbol".to_string())),
            right: Box::new(Expr::Column("symbol".to_string())),
        },
        window: None,
    };

    // Process the join
    let results = processor
        .process_stream_table_join(&trade, &join_clause, &mut context)
        .unwrap();

    // Verify results
    assert_eq!(results.len(), 1, "Should match on symbol");
    let joined = &results[0];

    // Check market data fields are added
    assert_eq!(
        joined.fields.get("m.current_price"),
        Some(&FieldValue::Float(150.25))
    );
    assert_eq!(
        joined.fields.get("m.volume"),
        Some(&FieldValue::Integer(1000000))
    );
}

#[test]
fn test_complex_join_condition_with_and() {
    let processor = StreamTableJoinProcessor::new();

    // Create a trade with specific fields
    let mut trade = create_trade_record("T001", 1, "AAPL", 100);
    trade.fields.insert("tier".to_string(), FieldValue::String("GOLD".to_string()));

    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert("user_profiles".to_string(), user_table.clone() as Arc<dyn UnifiedTable>);

    // Create complex join condition: user_id = user_id AND tier = tier
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: Some("u".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Column("user_id".to_string())),
                right: Box::new(Expr::Column("user_id".to_string())),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Column("tier".to_string())),
                right: Box::new(Expr::Column("tier".to_string())),
            }),
        },
        window: None,
    };

    // Process the join
    let results = processor
        .process_stream_table_join(&trade, &join_clause, &mut context)
        .unwrap();

    // Verify results
    assert_eq!(results.len(), 1, "Should match on both conditions");
    let joined = &results[0];
    assert_eq!(
        joined.fields.get("u.name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
}

#[test]
fn test_qualified_identifier_join() {
    let processor = StreamTableJoinProcessor::new();

    // Create test data
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert("user_profiles".to_string(), user_table.clone() as Arc<dyn UnifiedTable>);

    // Create join with qualified identifiers: t.user_id = u.user_id
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: Some("u".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        window: None,
    };

    // Process the join
    let results = processor
        .process_stream_table_join(&trade, &join_clause, &mut context)
        .unwrap();

    // Verify results
    assert_eq!(results.len(), 1, "Should match with qualified identifiers");
    let joined = &results[0];
    assert_eq!(
        joined.fields.get("u.name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
}

// Performance test - commented out for normal test runs
// #[test]
/// Test error handling for missing table
#[test]
fn test_stream_table_join_missing_table() {
    let processor = StreamTableJoinProcessor::new();
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let mut context = ProcessorContext::new("test_query");

    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("nonexistent_table".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        right_alias: Some("u".to_string()),
        window: None,
    };

    let result = processor.process_stream_table_join(&trade, &join_clause, &mut context);
    assert!(result.is_err());

    let error = result.unwrap_err();
    if let SqlError::ExecutionError { message, .. } = error {
        assert!(message.contains("nonexistent_table"));
        assert!(message.contains("not found"));
    } else {
        panic!("Expected SqlError::ExecutionError");
    }
}

/// Test error handling for unsupported JOIN conditions
#[test]
fn test_stream_table_join_unsupported_condition() {
    let processor = StreamTableJoinProcessor::new();
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let mut context = ProcessorContext::new("test_query");

    // Add table to context
    let table = create_user_profiles_table();
    context.load_reference_table("user_profiles", table);

    // Test unsupported operator (greater than)
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        right_alias: Some("u".to_string()),
        window: None,
    };

    let result = processor.process_stream_table_join(&trade, &join_clause, &mut context);
    assert!(result.is_err());

    let error = result.unwrap_err();
    if let SqlError::ExecutionError { message, .. } = error {
        assert!(message.contains("Unsupported binary operator"));
        assert!(message.contains("GreaterThan"));
    } else {
        panic!("Expected SqlError::ExecutionError");
    }
}

/// Test error handling for non-column expressions in JOIN condition
#[test]
fn test_stream_table_join_non_column_condition() {
    let processor = StreamTableJoinProcessor::new();
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let mut context = ProcessorContext::new("test_query");

    // Add table to context
    let table = create_user_profiles_table();
    context.load_reference_table("user_profiles", table);

    // Test literal value in JOIN condition (invalid)
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Literal(LiteralValue::Integer(123))),
        },
        right_alias: Some("u".to_string()),
        window: None,
    };

    let result = processor.process_stream_table_join(&trade, &join_clause, &mut context);
    assert!(result.is_err());

    let error = result.unwrap_err();
    if let SqlError::ExecutionError { message, .. } = error {
        assert!(message.contains("must compare two column references"));
    } else {
        panic!("Expected SqlError::ExecutionError");
    }
}

/// Test error handling for missing fields in stream record
#[test]
fn test_stream_table_join_missing_stream_fields() {
    let processor = StreamTableJoinProcessor::new();
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let mut context = ProcessorContext::new("test_query");

    // Add table to context
    let table = create_user_profiles_table();
    context.load_reference_table("user_profiles", table);

    // Test JOIN condition with field not in stream record
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("nonexistent_field".to_string())),
            right: Box::new(Expr::Column("other_nonexistent_field".to_string())),
        },
        right_alias: Some("u".to_string()),
        window: None,
    };

    let result = processor.process_stream_table_join(&trade, &join_clause, &mut context);
    assert!(result.is_err());

    let error = result.unwrap_err();
    if let SqlError::ExecutionError { message, .. } = error {
        assert!(message.contains("neither is present in the stream record"));
        assert!(message.contains("nonexistent_field"));
        assert!(message.contains("other_nonexistent_field"));
        assert!(message.contains("Available fields"));
    } else {
        panic!("Expected SqlError::ExecutionError");
    }
}

/// Test error handling for complex unsupported conditions
#[test]
fn test_stream_table_join_complex_unsupported_condition() {
    let processor = StreamTableJoinProcessor::new();
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let mut context = ProcessorContext::new("test_query");

    // Add table to context
    let table = create_user_profiles_table();
    context.load_reference_table("user_profiles", table);

    // Test unsupported function call in JOIN condition
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        condition: Expr::Literal(LiteralValue::String("UNSUPPORTED_COMPLEX_CONDITION".to_string())),
        right_alias: Some("u".to_string()),
        window: None,
    };

    let result = processor.process_stream_table_join(&trade, &join_clause, &mut context);
    assert!(result.is_err());

    let error = result.unwrap_err();
    if let SqlError::ExecutionError { message, .. } = error {
        assert!(message.contains("Unsupported JOIN condition"));
        assert!(message.contains("Only equality conditions and AND combinations are supported"));
    } else {
        panic!("Expected SqlError::ExecutionError");
    }
}

/// Test error handling for wrong source type
#[test]
fn test_stream_table_join_wrong_source_type() {
    let processor = StreamTableJoinProcessor::new();
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let mut context = ProcessorContext::new("test_query");

    // Test stream-stream join (should fail)
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Stream("other_stream".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        right_alias: Some("s".to_string()),
        window: None,
    };

    let result = processor.process_stream_table_join(&trade, &join_clause, &mut context);
    assert!(result.is_err());

    let error = result.unwrap_err();
    if let SqlError::ExecutionError { message, .. } = error {
        assert!(message.contains("Stream-Table join requires a table on the right side"));
    } else {
        panic!("Expected SqlError::ExecutionError");
    }
}

/// Test error handling with empty join keys extraction
#[test]
fn test_stream_table_join_empty_keys() {
    let processor = StreamTableJoinProcessor::new();

    // Create trade with different field names
    let mut fields = HashMap::new();
    fields.insert("trade_id".to_string(), FieldValue::String("T001".to_string()));
    fields.insert("different_field".to_string(), FieldValue::Integer(1));

    let trade = StreamRecord {
        timestamp: Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        fields,
        headers: HashMap::new(),
        event_time: Some(Utc::now()),
    };

    let mut context = ProcessorContext::new("test_query");

    // Add table to context
    let table = create_user_profiles_table();
    context.load_reference_table("user_profiles", table);

    // Test JOIN condition that won't extract any keys
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("missing_field1".to_string())),
            right: Box::new(Expr::Column("missing_field2".to_string())),
        },
        right_alias: Some("u".to_string()),
        window: None,
    };

    let result = processor.process_stream_table_join(&trade, &join_clause, &mut context);
    assert!(result.is_err());

    let error = result.unwrap_err();
    if let SqlError::ExecutionError { message, .. } = error {
        assert!(message.contains("No valid join keys could be extracted"));
        assert!(message.contains("Ensure the condition references fields present in the stream record"));
    } else {
        panic!("Expected SqlError::ExecutionError");
    }
}

// fn test_stream_table_join_performance() {
//     use std::time::Instant;
//
//     let processor = StreamTableJoinProcessor::new();
//
//     // Create large user table
//     let table = Arc::new(OptimizedTableImpl::new());
//     for i in 1..=10000 {
//         let mut user = HashMap::new();
//         user.insert("user_id".to_string(), FieldValue::Integer(i));
//         user.insert("name".to_string(), FieldValue::String(format!("User{}", i)));
//         user.insert("tier".to_string(), FieldValue::String("GOLD".to_string()));
//         user.insert("risk_score".to_string(), FieldValue::Integer(50));
//         table.insert(format!("user_{}", i), user).unwrap();
//     }
//
//     // Create 1000 trades
//     let trades: Vec<_> = (1..=1000)
//         .map(|i| create_trade_record(&format!("T{:04}", i), (i % 10000) + 1, "AAPL", 100))
//         .collect();
//
//     let mut context = ProcessorContext::new("test_query");
//     context.register_table("user_profiles", table as Arc<dyn UnifiedTable>);
//
//     let join_clause = JoinClause {
//         join_type: JoinType::Inner,
//         right_source: StreamSource::Table("user_profiles".to_string()),
//         right_alias: Some("u".to_string()),
//         condition: Expr::BinaryOp {
//             op: "=".to_string(),
//             left: Box::new(Expr::Identifier("user_id".to_string())),
//             right: Box::new(Expr::Identifier("user_id".to_string())),
//         },
//         window: None,
//     };
//
//     let start = Instant::now();
//     let results = processor
//         .process_batch_stream_table_join(trades, &join_clause, &mut context)
//         .unwrap();
//     let elapsed = start.elapsed();
//
//     assert_eq!(results.len(), 1000);
//     println!(
//         "Batch stream-table join of 1000 trades: {:?} ({:.0} joins/sec)",
//         elapsed,
//         1000.0 / elapsed.as_secs_f64()
//     );
// }