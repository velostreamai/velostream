//! Integration tests for Stream-Table Join functionality
//!
//! These tests exercise real join processing logic and require actual table implementations.

use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::sql::ast::{BinaryOperator, Expr, JoinClause, JoinType, StreamSource};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::processors::stream_table_join::StreamTableJoinProcessor;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};

fn create_trade_record(trade_id: &str, user_id: i64, symbol: &str, quantity: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "trade_id".to_string(),
        FieldValue::String(trade_id.to_string()),
    );
    fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert("quantity".to_string(), FieldValue::Integer(quantity));

    StreamRecord {
        id: trade_id.to_string(),
        timestamp: chrono::Utc::now().naive_utc(),
        event_time: None,
        fields,
        metadata: HashMap::new(),
    }
}

fn create_user_profiles_table() -> Arc<OptimizedTableImpl> {
    let table = Arc::new(OptimizedTableImpl::new());

    // Add user profiles
    let mut user1 = HashMap::new();
    user1.insert("user_id".to_string(), FieldValue::Integer(1));
    user1.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    user1.insert("tier".to_string(), FieldValue::String("GOLD".to_string()));
    user1.insert("risk_score".to_string(), FieldValue::Integer(85));

    let mut user2 = HashMap::new();
    user2.insert("user_id".to_string(), FieldValue::Integer(2));
    user2.insert("name".to_string(), FieldValue::String("Bob".to_string()));
    user2.insert("tier".to_string(), FieldValue::String("SILVER".to_string()));
    user2.insert("risk_score".to_string(), FieldValue::Integer(70));

    let mut user3 = HashMap::new();
    user3.insert("user_id".to_string(), FieldValue::Integer(3));
    user3.insert("name".to_string(), FieldValue::String("Charlie".to_string()));
    user3.insert("tier".to_string(), FieldValue::String("BRONZE".to_string()));
    user3.insert("risk_score".to_string(), FieldValue::Integer(60));

    table.insert("1".to_string(), user1);
    table.insert("2".to_string(), user2);
    table.insert("3".to_string(), user3);

    table
}

#[test]
fn test_stream_table_inner_join_integration() {
    let processor = StreamTableJoinProcessor::new();

    // Create trade and user table
    let trade = create_trade_record("T001", 1, "AAPL", 100);
    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert(
        "user_profiles".to_string(),
        user_table.clone() as Arc<dyn UnifiedTable>,
    );

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

    // Process the join
    let results = processor
        .process_stream_table_join(&trade, &join_clause, &mut context)
        .unwrap();

    // Verify results
    assert_eq!(results.len(), 1, "Should have one joined record");
    let joined = &results[0];

    // Verify trade fields
    assert_eq!(
        joined.fields.get("trade_id"),
        Some(&FieldValue::String("T001".to_string()))
    );
    assert_eq!(joined.fields.get("user_id"), Some(&FieldValue::Integer(1)));

    // Verify joined user fields
    assert_eq!(
        joined.fields.get("user.name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(
        joined.fields.get("user.tier"),
        Some(&FieldValue::String("GOLD".to_string()))
    );
}

#[test]
fn test_stream_table_left_join_with_match_integration() {
    let processor = StreamTableJoinProcessor::new();

    // Create trade and user table
    let trade = create_trade_record("T002", 2, "GOOGL", 50);
    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert(
        "user_profiles".to_string(),
        user_table.clone() as Arc<dyn UnifiedTable>,
    );

    // Create LEFT JOIN clause
    let join_clause = JoinClause {
        join_type: JoinType::Left,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: Some("p".to_string()),
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
    assert_eq!(results.len(), 1, "Should have one joined record");
    let joined = &results[0];

    // Verify trade fields are preserved
    assert_eq!(
        joined.fields.get("trade_id"),
        Some(&FieldValue::String("T002".to_string()))
    );
    assert_eq!(joined.fields.get("user_id"), Some(&FieldValue::Integer(2)));

    // Verify joined user fields
    assert_eq!(
        joined.fields.get("p.name"),
        Some(&FieldValue::String("Bob".to_string()))
    );
    assert_eq!(
        joined.fields.get("p.tier"),
        Some(&FieldValue::String("SILVER".to_string()))
    );
}

#[test]
fn test_stream_table_left_join_without_match_integration() {
    let processor = StreamTableJoinProcessor::new();

    // Create trade with non-existent user_id
    let trade = create_trade_record("T003", 999, "MSFT", 200);
    let user_table = create_user_profiles_table();

    // Create processor context
    let mut context = ProcessorContext::new("test_query");
    context.state_tables.insert(
        "user_profiles".to_string(),
        user_table.clone() as Arc<dyn UnifiedTable>,
    );

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
    assert_eq!(results.len(), 1, "Should have one record (left join preserves stream)");
    let joined = &results[0];

    // Verify trade fields are preserved
    assert_eq!(
        joined.fields.get("trade_id"),
        Some(&FieldValue::String("T003".to_string()))
    );
    assert_eq!(joined.fields.get("user_id"), Some(&FieldValue::Integer(999)));
    assert_eq!(
        joined.fields.get("symbol"),
        Some(&FieldValue::String("MSFT".to_string()))
    );
    assert_eq!(joined.fields.get("quantity"), Some(&FieldValue::Integer(200)));

    // Table fields should not be present (no match)
    assert!(!joined.fields.contains_key("u.name"));
    assert!(!joined.fields.contains_key("u.tier"));
    assert!(!joined.fields.contains_key("u.risk_score"));
}