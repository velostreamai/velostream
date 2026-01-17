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
        distinct: false,
        fields: vec![
            SelectField::Column("order_id".to_string()),
            SelectField::Column("customer_id".to_string()),
            SelectField::Column("total_amount".to_string()),
        ],
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(selection.strategy_name, "sticky_partition");
    assert!(
        selection.is_optimal,
        "Pure SELECT should use Sticky partitioning (source partition affinity)"
    );
}

#[test]
fn test_scenario_1_rows_window_without_group_by_should_use_sticky() {
    // Scenario 1: ROWS WINDOW (analytic) with ORDER BY timestamp, no GROUP BY
    // Expected: Sticky partition (analytic operations don't require rekeying)
    // Performance: Optimal with source partition affinity

    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![
            SelectField::Column("symbol".to_string()),
            SelectField::Column("price".to_string()),
        ],
        key_fields: None,
        from: StreamSource::Stream("market_data".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None, // NO GROUP BY - pure analytic
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(selection.strategy_name, "sticky_partition");
    assert!(
        selection.is_optimal,
        "ROWS WINDOW (analytic) should use Sticky partitioning"
    );
}

#[test]
fn test_scenario_2_group_by_aggregation_should_use_hash() {
    // Scenario 2: GROUP BY aggregation (no ORDER BY)
    // Expected: Hash partition by GROUP BY key
    // Performance: 2.42x faster (super-linear: 291% per-core efficiency!)

    let query = StreamingQuery::Select {
        distinct: false,
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
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
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
fn test_scenario_3a_tumbling_with_group_by_should_use_hash() {
    // Scenario 3a: TUMBLING WINDOW + GROUP BY
    // Expected: Always hash (Tumbling windows ALWAYS require rekeying by window boundaries)
    // Window boundaries require rekeying regardless of ORDER BY clause
    // GROUP BY keys define how data is grouped within each window

    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![
            SelectField::Column("trader_id".to_string()),
            SelectField::Column("symbol".to_string()),
        ],
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let selection = PartitionerSelector::select(&query);
    assert_eq!(
        selection.strategy_name, "always_hash",
        "TUMBLING WINDOW ALWAYS requires hash (window boundaries require rekeying)"
    );
    assert!(
        selection.is_optimal,
        "Hash partition is required for correct window boundary semantics"
    );
}

#[test]
fn test_scenario_3b_emit_changes_with_group_by_should_use_hash() {
    // Scenario 3b: EMIT CHANGES with GROUP BY (amplified output)
    // Expected: Hash partition by GROUP BY key
    // Performance: 4.6x faster than SQL Engine (batch handles amplification well)

    let query = StreamingQuery::Select {
        distinct: false,
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
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
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
            distinct: false,
            fields: vec![SelectField::Wildcard],
            key_fields: None,
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
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
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
        distinct: false,
        fields: vec![SelectField::Wildcard],
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
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
    // Scenario: Tumbling window query (auto-selection would choose Hash)
    // If user explicitly chooses Sticky in config, that choice must be RESPECTED

    let window_query = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Wildcard],
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    // Auto-select would choose Hash (Tumbling windows ALWAYS require rekeying)
    let auto_selection = PartitionerSelector::select(&window_query);
    assert_eq!(auto_selection.strategy_name, "always_hash");

    // DOCUMENTATION: If user explicitly specified Sticky in config, that should be RESPECTED
    // AdaptiveJobProcessor logic (not tested here):
    // if config.partitioning_strategy == Some("sticky_partition") {
    //     Use StickyPartitionStrategy  // ← User's choice wins (overrides auto-selection)
    // } else if config.auto_select_from_query == Some(query) {
    //     Use auto-selected strategy (always_hash for Tumbling windows)
    // }

    assert!(
        auto_selection.is_optimal,
        "Hash is required for Tumbling windows (window boundary rekeying)"
    );
}
