//! Tests for extract_group_by_columns — partition routing key extraction from query AST.

use std::collections::HashMap;
use velostream::velostream::server::v2::AdaptiveJobProcessor;
use velostream::velostream::sql::ast::{
    EmitMode, Expr, SelectField, ShowResourceType, StreamSource, StreamingQuery,
};

/// Helper: create a SELECT query with the given GROUP BY columns.
fn select_with_group_by(columns: Vec<&str>) -> StreamingQuery {
    StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Wildcard],
        key_fields: None,
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(
            columns
                .iter()
                .map(|c| Expr::Column(c.to_string()))
                .collect(),
        ),
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

/// Helper: create a SELECT query with no GROUP BY.
fn select_without_group_by() -> StreamingQuery {
    StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Wildcard],
        key_fields: None,
        from: StreamSource::Stream("test_stream".to_string()),
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

// ─── SELECT ───────────────────────────────────────────────────────────────────

#[test]
fn test_select_single_group_by_column() {
    let query = select_with_group_by(vec!["station"]);
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert_eq!(cols, vec!["station"]);
}

#[test]
fn test_select_multiple_group_by_columns() {
    let query = select_with_group_by(vec!["region", "station", "year"]);
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert_eq!(cols, vec!["region", "station", "year"]);
}

#[test]
fn test_select_no_group_by() {
    let query = select_without_group_by();
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert!(cols.is_empty());
}

#[test]
fn test_select_group_by_with_non_column_expressions() {
    // Mix of Column and non-Column expressions — only columns are extracted
    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Wildcard],
        key_fields: None,
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![
            Expr::Column("station".to_string()),
            Expr::Literal(velostream::velostream::sql::ast::LiteralValue::Integer(1)),
            Expr::Column("region".to_string()),
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
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert_eq!(cols, vec!["station", "region"]);
}

// ─── CREATE STREAM ────────────────────────────────────────────────────────────

#[test]
fn test_create_stream_with_group_by() {
    let query = StreamingQuery::CreateStream {
        name: "results".to_string(),
        columns: None,
        as_select: Box::new(select_with_group_by(vec!["station"])),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Final),
        metric_annotations: Vec::new(),
    };
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert_eq!(cols, vec!["station"]);
}

#[test]
fn test_create_stream_without_group_by() {
    let query = StreamingQuery::CreateStream {
        name: "results".to_string(),
        columns: None,
        as_select: Box::new(select_without_group_by()),
        properties: HashMap::new(),
        emit_mode: None,
        metric_annotations: Vec::new(),
    };
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert!(cols.is_empty());
}

// ─── CREATE TABLE ─────────────────────────────────────────────────────────────

#[test]
fn test_create_table_with_group_by() {
    let query = StreamingQuery::CreateTable {
        name: "summary".to_string(),
        columns: None,
        as_select: Box::new(select_with_group_by(vec!["user_id", "category"])),
        properties: HashMap::new(),
        emit_mode: None,
    };
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert_eq!(cols, vec!["user_id", "category"]);
}

#[test]
fn test_create_table_without_group_by() {
    let query = StreamingQuery::CreateTable {
        name: "snapshot".to_string(),
        columns: None,
        as_select: Box::new(select_without_group_by()),
        properties: HashMap::new(),
        emit_mode: None,
    };
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert!(cols.is_empty());
}

// ─── EDGE CASES ──────────────────────────────────────────────────────────────

#[test]
fn test_select_explicit_empty_group_by() {
    // group_by: Some(vec![]) should return empty, same as group_by: None
    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Wildcard],
        key_fields: None,
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![]),
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
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert!(cols.is_empty());
}

#[test]
fn test_create_stream_with_non_select_inner() {
    // as_select wrapping a non-Select query — should return empty
    let inner = StreamingQuery::StopJob {
        name: "inner".to_string(),
        force: false,
    };
    let query = StreamingQuery::CreateStream {
        name: "results".to_string(),
        columns: None,
        as_select: Box::new(inner),
        properties: HashMap::new(),
        emit_mode: None,
        metric_annotations: Vec::new(),
    };
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert!(cols.is_empty());
}

// ─── OTHER QUERY TYPES ───────────────────────────────────────────────────────

#[test]
fn test_stop_job_returns_empty() {
    let query = StreamingQuery::StopJob {
        name: "my_job".to_string(),
        force: false,
    };
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert!(cols.is_empty());
}

#[test]
fn test_show_returns_empty() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Streams,
        pattern: None,
    };
    let cols = AdaptiveJobProcessor::extract_group_by_columns(&query);
    assert!(cols.is_empty());
}
