//! Tests for CTAS (CREATE TABLE AS SELECT) validation
//!
//! This module tests CTAS-specific validation rules including:
//! - Table name validation
//! - WITH clause property validation
//! - EMIT mode compatibility
//! - Aggregation and GROUP BY validation
//! - Memory usage warnings

use std::collections::HashMap;
use velostream::velostream::sql::ast::{EmitMode, Expr, SelectField, StreamSource, StreamingQuery};
use velostream::velostream::sql::validator::SqlValidator;

// Helper to create a basic SELECT query
fn create_basic_select() -> StreamingQuery {
    StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("orders_stream".to_string()),
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
    }
}

#[test]
fn test_valid_basic_ctas() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Basic CTAS should not produce errors
    assert_eq!(validator.errors().len(), 0);
}

#[test]
fn test_empty_table_name() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateTable {
        name: "".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce error for empty table name
    assert!(validator.errors().len() > 0);
    let error = &validator.errors()[0];
    assert!(error.message.contains("name cannot be empty"));
}

#[test]
fn test_whitespace_table_name() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateTable {
        name: "   ".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce error for whitespace-only table name
    assert!(validator.errors().len() > 0);
    let error = &validator.errors()[0];
    assert!(error.message.contains("name cannot be empty"));
}

#[test]
fn test_invalid_table_name_special_characters() {
    let validator = SqlValidator::new();

    let invalid_names = vec![
        "table-name",
        "table name",
        "table.name",
        "table@name",
        "123table",
    ];

    for name in invalid_names {
        let query = StreamingQuery::CreateTable {
            name: name.to_string(),
            columns: None,
            as_select: Box::new(create_basic_select()),
            properties: HashMap::new(),
            emit_mode: Some(EmitMode::Changes),
        };

        validator.validate(&query);

        // Should produce warning for potentially invalid table name
        assert!(
            validator.warnings().len() > 0 || validator.errors().len() > 0,
            "Expected warning or error for table name: {}",
            name
        );
    }
}

#[test]
fn test_valid_table_names() {
    let validator = SqlValidator::new();

    let valid_names = vec![
        "orders",
        "orders_table",
        "OrdersTable",
        "orders123",
        "table_v2",
    ];

    for name in valid_names {
        let query = StreamingQuery::CreateTable {
            name: name.to_string(),
            columns: None,
            as_select: Box::new(create_basic_select()),
            properties: HashMap::new(),
            emit_mode: Some(EmitMode::Changes),
        };

        validator.validate(&query);

        // Should not produce errors
        assert_eq!(
            validator.errors().len(),
            0,
            "Unexpected error for valid table name: {}",
            name
        );
    }
}

#[test]
fn test_invalid_table_model_property() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "invalid_model".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce error for invalid table_model
    assert!(validator.errors().len() > 0);
    let error = &validator.errors()[0];
    assert!(error.message.contains("table_model"));
    assert!(error.message.contains("normal") || error.message.contains("compact"));
}

#[test]
fn test_valid_table_model_normal() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "normal".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should not produce errors
    assert_eq!(validator.errors().len(), 0);
}

#[test]
fn test_valid_table_model_compact() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "compact".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should not produce errors
    assert_eq!(validator.errors().len(), 0);
}

#[test]
fn test_invalid_retention_format() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("retention".to_string(), "invalid_format".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce error for invalid retention format
    assert!(validator.errors().len() > 0);
    let error = &validator.errors()[0];
    assert!(error.message.contains("retention"));
}

#[test]
fn test_valid_retention_formats() {
    let validator = SqlValidator::new();

    let valid_retentions = vec!["7 days", "1 day", "24 hours", "30 days"];

    for retention in valid_retentions {
        let mut properties = HashMap::new();
        properties.insert("retention".to_string(), retention.to_string());

        let query = StreamingQuery::CreateTable {
            name: "orders_table".to_string(),
            columns: None,
            as_select: Box::new(create_basic_select()),
            properties,
            emit_mode: Some(EmitMode::Changes),
        };

        validator.validate(&query);

        // Should not produce errors for valid retention
        assert_eq!(
            validator.errors().len(),
            0,
            "Unexpected error for retention: {}",
            retention
        );
    }
}

#[test]
fn test_emit_final_without_window_warning() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Final), // Using EMIT FINAL
    };

    validator.validate(&query);

    // Should produce warning about EMIT FINAL without window
    assert!(validator.warnings().len() > 0);
    let warning = &validator.warnings()[0];
    assert!(warning.message.contains("EMIT FINAL"));
    assert!(warning.message.contains("WINDOW") || warning.message.contains("window"));
}

#[test]
fn test_emit_changes_is_valid() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // EMIT CHANGES should not produce warnings
    assert_eq!(validator.warnings().len(), 0);
    assert_eq!(validator.errors().len(), 0);
}

#[test]
fn test_long_retention_without_compact_warning() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("retention".to_string(), "90 days".to_string());
    properties.insert("table_model".to_string(), "normal".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce warning about long retention without compact model
    assert!(validator.warnings().len() > 0);
    let warning = &validator.warnings()[0];
    assert!(warning.message.contains("retention") || warning.message.contains("memory"));
    assert!(warning.message.contains("compact"));
}

#[test]
fn test_long_retention_with_compact_no_warning() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("retention".to_string(), "90 days".to_string());
    properties.insert("table_model".to_string(), "compact".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should not produce memory warnings with compact model
    let memory_warnings = validator
        .warnings()
        .iter()
        .filter(|w| w.message.contains("memory") || w.message.contains("compact"))
        .count();

    assert_eq!(memory_warnings, 0);
}

#[test]
fn test_aggregation_without_group_by_warning() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Column("*".to_string())],
                },
                alias: Some("total_count".to_string()),
            }],
            from: StreamSource::Stream("orders_stream".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None, // No GROUP BY
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        }),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce warning about aggregation without GROUP BY
    assert!(validator.warnings().len() > 0);
    let warning = &validator.warnings()[0];
    assert!(warning.message.contains("aggregation") || warning.message.contains("GROUP BY"));
}

#[test]
fn test_aggregation_with_group_by_valid() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(StreamingQuery::Select {
            fields: vec![
                SelectField::Expression {
                    expr: Expr::Column("customer_id".to_string()),
                    alias: None,
                },
                SelectField::Expression {
                    expr: Expr::Function {
                        name: "COUNT".to_string(),
                        args: vec![Expr::Column("*".to_string())],
                    },
                    alias: Some("total_count".to_string()),
                },
            ],
            from: StreamSource::Stream("orders_stream".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: Some(vec![Expr::Column("customer_id".to_string())]),
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        }),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should not produce aggregation warnings with GROUP BY
    let agg_warnings = validator
        .warnings()
        .iter()
        .filter(|w| w.message.contains("aggregation") || w.message.contains("GROUP BY"))
        .count();

    assert_eq!(agg_warnings, 0);
}

#[test]
fn test_compression_property_validation() {
    let validator = SqlValidator::new();

    let valid_compressions = vec!["snappy", "gzip", "zstd", "lz4"];

    for compression in valid_compressions {
        let mut properties = HashMap::new();
        properties.insert("compression".to_string(), compression.to_string());

        let query = StreamingQuery::CreateTable {
            name: "orders_table".to_string(),
            columns: None,
            as_select: Box::new(create_basic_select()),
            properties,
            emit_mode: Some(EmitMode::Changes),
        };

        validator.validate(&query);

        // Valid compression should not produce errors
        assert_eq!(
            validator.errors().len(),
            0,
            "Unexpected error for compression: {}",
            compression
        );
    }
}

#[test]
fn test_invalid_compression_property() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("compression".to_string(), "invalid_compression".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce error for invalid compression
    assert!(validator.errors().len() > 0);
    let error = &validator.errors()[0];
    assert!(error.message.contains("compression"));
}

#[test]
fn test_kafka_batch_size_validation() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("kafka.batch.size".to_string(), "not_a_number".to_string());

    let query = StreamingQuery::CreateTable {
        name: "orders_table".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Changes),
    };

    validator.validate(&query);

    // Should produce error for invalid kafka.batch.size
    assert!(validator.errors().len() > 0);
    let error = &validator.errors()[0];
    assert!(error.message.contains("kafka.batch.size") || error.message.contains("integer"));
}

#[test]
fn test_multiple_validation_errors() {
    let validator = SqlValidator::new();

    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "invalid_model".to_string());
    properties.insert("compression".to_string(), "invalid_compression".to_string());
    properties.insert("retention".to_string(), "invalid_format".to_string());

    let query = StreamingQuery::CreateTable {
        name: "".to_string(), // Empty name (error)
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties,
        emit_mode: Some(EmitMode::Final), // EMIT FINAL without window (warning)
    };

    validator.validate(&query);

    // Should produce multiple errors
    assert!(validator.errors().len() >= 3);
    // Should produce at least one warning
    assert!(validator.warnings().len() >= 1);
}

#[test]
fn test_csas_validation() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateStream {
        name: "filtered_orders".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
        metric_annotations: Vec::new(), // FR-073 Phase 1
    };

    validator.validate(&query);

    // CSAS should also validate basic rules
    assert_eq!(validator.errors().len(), 0);
}

#[test]
fn test_csas_empty_name() {
    let validator = SqlValidator::new();

    let query = StreamingQuery::CreateStream {
        name: "".to_string(),
        columns: None,
        as_select: Box::new(create_basic_select()),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Changes),
        metric_annotations: Vec::new(), // FR-073 Phase 1
    };

    validator.validate(&query);

    // Should produce error for empty stream name
    assert!(validator.errors().len() > 0);
    let error = &validator.errors()[0];
    assert!(error.message.contains("name cannot be empty"));
}
