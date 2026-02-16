//! Unit tests for QuerySpanMetadata extraction from StreamingQuery AST

use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::observability::query_metadata::QuerySpanMetadata;
use velostream::velostream::sql::ast::*;

/// Helper to build a basic SELECT query with just a FROM clause
fn simple_select(source: &str) -> StreamingQuery {
    StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream(source.to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
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
    }
}

#[test]
fn test_simple_select_no_join_window_groupby() {
    let query = simple_select("orders");
    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(!metadata.has_join);
    assert!(metadata.join_type.is_none());
    assert!(metadata.join_sources.is_none());
    assert!(metadata.join_key_fields.is_none());
    assert!(!metadata.has_window);
    assert!(metadata.window_type.is_none());
    assert!(metadata.window_size_ms.is_none());
    assert!(!metadata.has_group_by);
    assert!(metadata.group_by_fields.is_none());
    assert!(metadata.emit_mode.is_none());
}

#[test]
fn test_tumbling_window_with_group_by() {
    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("symbol".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Column("*".to_string())],
                },
                alias: Some("cnt".to_string()),
            },
        ],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("trades".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![
            Expr::Column("symbol".to_string()),
            Expr::Column("exchange".to_string()),
        ]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(300),
            time_column: None,
        }),
        order_by: None,
        limit: None,
        emit_mode: Some(EmitMode::Changes),
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(!metadata.has_join);
    assert!(metadata.has_window);
    assert_eq!(metadata.window_type.as_deref(), Some("tumbling"));
    assert_eq!(metadata.window_size_ms, Some(300_000));
    assert!(metadata.has_group_by);
    assert_eq!(metadata.group_by_fields.as_deref(), Some("symbol,exchange"));
    assert_eq!(metadata.emit_mode.as_deref(), Some("changes"));
}

#[test]
fn test_inner_join_extraction() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("orders".to_string()),
        from_alias: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::Inner,
            right_source: StreamSource::Stream("customers".to_string()),
            right_alias: None,
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("orders.customer_id".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Column("customers.id".to_string())),
            },
            window: None,
        }]),
        where_clause: None,
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

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_join);
    assert_eq!(metadata.join_type.as_deref(), Some("INNER"));
    assert_eq!(metadata.join_sources.as_deref(), Some("orders,customers"));
    // extract_join_keys handles qualified names by extracting the column part
    assert!(metadata.join_key_fields.is_some());
    let key_fields = metadata.join_key_fields.unwrap();
    assert!(
        key_fields.contains("customer_id"),
        "Should contain customer_id, got: {}",
        key_fields
    );
    assert!(
        key_fields.contains("id"),
        "Should contain id, got: {}",
        key_fields
    );
}

#[test]
fn test_left_join_with_compound_keys() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("trades".to_string()),
        from_alias: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::Left,
            right_source: StreamSource::Stream("quotes".to_string()),
            right_alias: None,
            condition: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("trades.symbol".to_string())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expr::Column("quotes.symbol".to_string())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("trades.exchange".to_string())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expr::Column("quotes.exchange".to_string())),
                }),
            },
            window: None,
        }]),
        where_clause: None,
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

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_join);
    assert_eq!(metadata.join_type.as_deref(), Some("LEFT"));
    assert_eq!(metadata.join_sources.as_deref(), Some("trades,quotes"));
    // Should extract both key pairs
    let key_fields = metadata.join_key_fields.as_ref().unwrap();
    assert!(
        key_fields.contains("symbol"),
        "Should contain symbol, got: {}",
        key_fields
    );
    assert!(
        key_fields.contains("exchange"),
        "Should contain exchange, got: {}",
        key_fields
    );
}

#[test]
fn test_csas_wrapping_extracts_metadata() {
    let inner_select = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("input".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("symbol".to_string())]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: None,
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let csas = StreamingQuery::CreateStream {
        name: "output_stream".to_string(),
        columns: None,
        as_select: Box::new(inner_select),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Final),
        metric_annotations: vec![],
    };

    let metadata = QuerySpanMetadata::from_query(&csas);

    assert!(metadata.has_window);
    assert_eq!(metadata.window_type.as_deref(), Some("tumbling"));
    assert_eq!(metadata.window_size_ms, Some(60_000));
    assert!(metadata.has_group_by);
    assert_eq!(metadata.group_by_fields.as_deref(), Some("symbol"));
    // CSAS emit_mode overrides inner SELECT
    assert_eq!(metadata.emit_mode.as_deref(), Some("final"));
}

#[test]
fn test_session_window() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("events".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Session {
            gap: Duration::from_secs(30),
            time_column: None,
            partition_by: vec![],
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_window);
    assert_eq!(metadata.window_type.as_deref(), Some("session"));
    assert_eq!(metadata.window_size_ms, Some(30_000));
}

#[test]
fn test_rows_window() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("trades".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Rows {
            buffer_size: 100,
            partition_by: vec![],
            order_by: vec![],
            time_gap: None,
            window_frame: None,
            emit_mode: RowsEmitMode::EveryRecord,
            expire_after: RowExpirationMode::Default,
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_window);
    assert_eq!(metadata.window_type.as_deref(), Some("rows"));
    assert_eq!(metadata.window_size_ms, Some(100));
}

#[test]
fn test_sliding_window() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("trades".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Sliding {
            size: Duration::from_secs(600),
            advance: Duration::from_secs(60),
            time_column: None,
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_window);
    assert_eq!(metadata.window_type.as_deref(), Some("sliding"));
    assert_eq!(metadata.window_size_ms, Some(600_000));
}

#[test]
fn test_non_select_query_returns_empty() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Jobs,
        pattern: None,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(!metadata.has_join);
    assert!(!metadata.has_window);
    assert!(!metadata.has_group_by);
    assert!(metadata.emit_mode.is_none());
}

#[test]
fn test_emit_final_mode() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("input".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: Some(EmitMode::Final),
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    assert_eq!(metadata.emit_mode.as_deref(), Some("final"));
}

#[test]
fn test_start_job_wrapper_extracts_metadata() {
    let inner = simple_select("input");
    let query = StreamingQuery::StartJob {
        name: "test_job".to_string(),
        query: Box::new(inner),
        properties: HashMap::new(),
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    // Should extract through the wrapper
    assert!(!metadata.has_join);
    assert!(!metadata.has_window);
    assert!(!metadata.has_group_by);
}

#[test]
fn test_empty_metadata() {
    let metadata = QuerySpanMetadata::empty();

    assert!(!metadata.has_join);
    assert!(metadata.join_type.is_none());
    assert!(metadata.join_sources.is_none());
    assert!(metadata.join_key_fields.is_none());
    assert!(!metadata.has_window);
    assert!(metadata.window_type.is_none());
    assert!(metadata.window_size_ms.is_none());
    assert!(!metadata.has_group_by);
    assert!(metadata.group_by_fields.is_none());
    assert!(metadata.emit_mode.is_none());
}

#[test]
fn test_full_outer_join_type() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("left_stream".to_string()),
        from_alias: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::FullOuter,
            right_source: StreamSource::Stream("right_stream".to_string()),
            right_alias: None,
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Column("id".to_string())),
            },
            window: None,
        }]),
        where_clause: None,
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

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_join);
    assert_eq!(metadata.join_type.as_deref(), Some("FULL_OUTER"));
    assert_eq!(
        metadata.join_sources.as_deref(),
        Some("left_stream,right_stream")
    );
}

#[test]
fn test_right_join_type() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("a".to_string()),
        from_alias: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::Right,
            right_source: StreamSource::Table("b".to_string()),
            right_alias: None,
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("key".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Column("key".to_string())),
            },
            window: None,
        }]),
        where_clause: None,
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

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_join);
    assert_eq!(metadata.join_type.as_deref(), Some("RIGHT"));
    // Table source should be included
    assert_eq!(metadata.join_sources.as_deref(), Some("a,b"));
}

#[test]
fn test_multi_join_only_extracts_first_join() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("a".to_string()),
        from_alias: None,
        joins: Some(vec![
            JoinClause {
                join_type: JoinType::Inner,
                right_source: StreamSource::Stream("b".to_string()),
                right_alias: None,
                condition: Expr::BinaryOp {
                    left: Box::new(Expr::Column("a.id".to_string())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expr::Column("b.id".to_string())),
                },
                window: None,
            },
            JoinClause {
                join_type: JoinType::Left,
                right_source: StreamSource::Stream("c".to_string()),
                right_alias: None,
                condition: Expr::BinaryOp {
                    left: Box::new(Expr::Column("b.key".to_string())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expr::Column("c.key".to_string())),
                },
                window: None,
            },
        ]),
        where_clause: None,
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

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_join);
    // Only first join type is reported
    assert_eq!(metadata.join_type.as_deref(), Some("INNER"));
    // Only first join sources (a,b), not c
    assert_eq!(metadata.join_sources.as_deref(), Some("a,b"));
}

#[test]
fn test_deploy_job_wrapper_extracts_metadata() {
    let inner = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("trades".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("symbol".to_string())]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(120),
            time_column: None,
        }),
        order_by: None,
        limit: None,
        emit_mode: Some(EmitMode::Changes),
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let query = StreamingQuery::DeployJob {
        name: "agg_job".to_string(),
        version: "1.0.0".to_string(),
        query: Box::new(inner),
        properties: HashMap::new(),
        strategy: DeploymentStrategy::BlueGreen,
    };

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_window);
    assert_eq!(metadata.window_type.as_deref(), Some("tumbling"));
    assert_eq!(metadata.window_size_ms, Some(120_000));
    assert!(metadata.has_group_by);
    assert_eq!(metadata.group_by_fields.as_deref(), Some("symbol"));
    assert_eq!(metadata.emit_mode.as_deref(), Some("changes"));
}

#[test]
fn test_ctas_wrapping_extracts_metadata() {
    let inner_select = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("events".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Session {
            gap: Duration::from_secs(45),
            time_column: None,
            partition_by: vec![],
        }),
        order_by: None,
        limit: None,
        emit_mode: Some(EmitMode::Changes),
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let ctas = StreamingQuery::CreateTable {
        name: "output_table".to_string(),
        columns: None,
        as_select: Box::new(inner_select),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Final),
    };

    let metadata = QuerySpanMetadata::from_query(&ctas);

    assert!(metadata.has_window);
    assert_eq!(metadata.window_type.as_deref(), Some("session"));
    assert_eq!(metadata.window_size_ms, Some(45_000));
    // CTAS emit_mode overrides inner SELECT
    assert_eq!(metadata.emit_mode.as_deref(), Some("final"));
}

#[test]
fn test_uri_source_name() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Uri("file:///data/input.csv".to_string()),
        from_alias: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::Inner,
            right_source: StreamSource::Stream("lookup".to_string()),
            right_alias: None,
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Column("id".to_string())),
            },
            window: None,
        }]),
        where_clause: None,
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

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_join);
    assert_eq!(
        metadata.join_sources.as_deref(),
        Some("file:///data/input.csv,lookup")
    );
}

#[test]
fn test_subquery_source_name() {
    let subquery = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Stream("inner".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
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

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        distinct: false,
        key_fields: None,
        from: StreamSource::Subquery(Box::new(subquery)),
        from_alias: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::Inner,
            right_source: StreamSource::Stream("other".to_string()),
            right_alias: None,
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Column("id".to_string())),
            },
            window: None,
        }]),
        where_clause: None,
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

    let metadata = QuerySpanMetadata::from_query(&query);

    assert!(metadata.has_join);
    assert_eq!(metadata.join_sources.as_deref(), Some("subquery,other"));
}
