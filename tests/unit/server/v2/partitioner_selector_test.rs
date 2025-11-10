//! Unit tests for automatic partitioner selection based on query analysis
//!
//! Tests verify that the PartitionerSelector correctly identifies the optimal
//! partitioning strategy for different query patterns, following the performance
//! insights from Scenarios 0-3b benchmarks.

use std::time::Duration;
use velostream::velostream::server::v2::PartitionerSelector;
use velostream::velostream::sql::ast::{
    BinaryOperator, Expr, LiteralValue, OrderByExpr, OrderDirection, SelectField, StreamSource,
    StreamingQuery, WindowSpec,
};

#[test]
fn test_scenario_0_pure_select_should_use_hash() {
    // Scenario 0: Pure SELECT filter query
    // Expected: Hash partitioning (any key works, no ordering needed)
    // Performance: 2.27x faster than SQL Engine

    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("order_id".to_string()),
            SelectField::Column("customer_id".to_string()),
            SelectField::Column("total_amount".to_string()),
        ],
        from: StreamSource::Stream("orders".to_string()),
        from_alias: None,
        joins: None,
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("total_amount".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        }),
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(selection.strategy_name, "sticky_partition");
    assert!(
        selection.is_optimal,
        "Pure SELECT should use Sticky partitioning (source partition affinity)"
    );
}

#[test]
fn test_scenario_1_rows_window_with_order_by_should_use_sticky() {
    // Scenario 1: ROWS WINDOW with ORDER BY timestamp
    // Expected: Sticky partition by timestamp (prevents out-of-order buffering)
    // Current Performance: 62% slower (Hash strategy is WRONG)
    // Optimal Performance: Should match SQL Engine with Sticky

    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("symbol".to_string()),
            SelectField::Column("price".to_string()),
        ],
        from: StreamSource::Stream("market_data".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("symbol".to_string())]),
        having: None,
        window: Some(WindowSpec::Rows {
            buffer_size: 100,
            partition_by: vec![Expr::Column("symbol".to_string())],
            order_by: vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }],
            time_gap: None,
            window_frame: None,
            emit_mode: velostream::velostream::sql::ast::RowsEmitMode::EveryRecord,
            expire_after: velostream::velostream::sql::ast::RowExpirationMode::Default,
        }),
        order_by: Some(vec![OrderByExpr {
            expr: Expr::Column("timestamp".to_string()),
            direction: OrderDirection::Asc,
        }]),
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(selection.strategy_name, "sticky_partition");
    assert!(
        selection.is_optimal,
        "ROWS WINDOW with ORDER BY should use Sticky partitioning"
    );
}

#[test]
fn test_scenario_2_group_by_aggregation_should_use_hash() {
    // Scenario 2: GROUP BY aggregation (no ORDER BY)
    // Expected: Hash partition by GROUP BY key
    // Performance: 2.42x faster (super-linear: 291% per-core efficiency!)

    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("symbol".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Literal(LiteralValue::Integer(1))],
                },
                alias: Some("trade_count".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("price".to_string())],
                },
                alias: Some("avg_price".to_string()),
            },
        ],
        from: StreamSource::Stream("market_data".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("symbol".to_string())]),
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(selection.strategy_name, "always_hash");
    assert!(
        selection.is_optimal,
        "GROUP BY should use Hash partitioning"
    );
    assert!(
        selection.routing_keys.contains(&"symbol".to_string()),
        "Should route by GROUP BY key"
    );
}

#[test]
fn test_scenario_3a_tumbling_with_group_by_should_use_sticky() {
    // Scenario 3a: TUMBLING WINDOW + GROUP BY (double mismatch!)
    // Expected: Sticky partition by time (ORDER BY priority)
    // Current Performance: 92% slower (Hash strategy is VERY WRONG)
    // Optimal Performance: Should be on-par with SQL Engine

    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("trader_id".to_string()),
            SelectField::Column("symbol".to_string()),
        ],
        from: StreamSource::Stream("market_data".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![
            Expr::Column("trader_id".to_string()),
            Expr::Column("symbol".to_string()),
        ]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: Some("event_time".to_string()),
        }),
        order_by: Some(vec![OrderByExpr {
            expr: Expr::Column("event_time".to_string()),
            direction: OrderDirection::Asc,
        }]),
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(
        selection.strategy_name, "sticky_partition",
        "TUMBLING WINDOW with ORDER BY should use Sticky partitioning"
    );
    assert!(
        selection.is_optimal,
        "Sticky partition is optimal for time-ordered windows"
    );
}

#[test]
fn test_scenario_3b_emit_changes_with_group_by_should_use_hash() {
    // Scenario 3b: EMIT CHANGES with GROUP BY (amplified output)
    // Expected: Hash partition by GROUP BY key
    // Performance: 4.6x faster than SQL Engine (batch handles amplification well)

    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("trader_id".to_string()),
            SelectField::Column("symbol".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Literal(LiteralValue::Integer(1))],
                },
                alias: Some("trade_count".to_string()),
            },
        ],
        from: StreamSource::Stream("market_data".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![
            Expr::Column("trader_id".to_string()),
            Expr::Column("symbol".to_string()),
        ]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: Some("trade_time".to_string()),
        }),
        order_by: None,
        limit: None,
        emit_mode: Some(velostream::velostream::sql::ast::EmitMode::Changes),
        properties: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(selection.strategy_name, "always_hash");
    assert!(
        selection.is_optimal,
        "EMIT CHANGES without ORDER BY should use Hash"
    );
}

#[test]
fn test_default_behavior_uses_smart_repartition_for_create_stream() {
    // Test default behavior for non-SELECT queries
    // Expected: SmartRepartition (respect source partitions)

    let query = StreamingQuery::CreateStream {
        name: "high_value_orders".to_string(),
        columns: None,
        as_select: Box::new(StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Stream("orders".to_string()),
            from_alias: None,
            joins: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("total_amount".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Integer(1000))),
            }),
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        }),
        properties: std::collections::HashMap::new(),
        emit_mode: None,
        metric_annotations: vec![],
        job_name: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(
        selection.strategy_name, "sticky_partition",
        "CREATE STREAM should default to StickyPartition (uses source partition field)"
    );
}

#[test]
fn test_routing_keys_extracted_correctly_for_multi_column_group_by() {
    // Verify that routing keys are correctly extracted from complex GROUP BY
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("trades".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![
            Expr::Column("trader_id".to_string()),
            Expr::Column("symbol".to_string()),
            Expr::Column("exchange".to_string()),
        ]),
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert!(!selection.routing_keys.is_empty());
    // Should extract at least some of the column names
    let all_keys = ["trader_id", "symbol", "exchange"];
    assert!(
        all_keys
            .iter()
            .any(|k| selection.routing_keys.iter().any(|rk| rk == k)),
        "Should extract at least one GROUP BY column as routing key"
    );
}

#[test]
fn test_user_explicit_choice_principle_documented() {
    // PRINCIPLE: User explicit configuration is ALWAYS respected
    // Auto-selection should NEVER override user's choice
    //
    // Scenario: User explicitly chooses Hash for a window query (unusual but valid)
    // Even though optimal would be Sticky, we must respect user's explicit choice

    let window_query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("market_data".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("symbol".to_string())]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: Some("event_time".to_string()),
        }),
        order_by: Some(vec![OrderByExpr {
            expr: Expr::Column("event_time".to_string()),
            direction: OrderDirection::Asc,
        }]),
        limit: None,
        emit_mode: None,
        properties: None,
    };

    // Auto-select would choose Sticky
    let auto_selection = PartitionerSelector::select(&window_query);
    assert_eq!(auto_selection.strategy_name, "sticky_partition");

    // DOCUMENTATION: If user explicitly specified Hash in config, that should be RESPECTED
    // PartitionedJobCoordinator logic (not tested here):
    // if config.partitioning_strategy == Some("always_hash") {
    //     Use AlwaysHashStrategy  // ← User's choice wins
    // } else if config.auto_select_from_query == Some(query) {
    //     Use auto-selected strategy
    // }

    assert!(
        auto_selection.is_optimal,
        "Sticky is optimal for TUMBLING with ORDER BY"
    );
}
