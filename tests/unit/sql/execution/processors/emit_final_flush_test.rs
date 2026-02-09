//! Tests for EMIT FINAL without WINDOW on bounded sources
//!
//! Verifies that:
//! - EMIT FINAL + GROUP BY without WINDOW suppresses per-record output
//! - flush_final_aggregations() produces correct final aggregation records
//! - EMIT CHANGES behavior is unchanged (regression)

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::{
    ast::{EmitMode, Expr, SelectField, StreamSource, StreamingQuery},
    execution::{FieldValue, StreamRecord, engine::StreamExecutionEngine},
};

fn create_station_record(station: &str, temperature: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "station".to_string(),
        FieldValue::String(station.to_string()),
    );
    fields.insert("temperature".to_string(), FieldValue::Float(temperature));
    StreamRecord::new(fields)
}

fn create_engine() -> StreamExecutionEngine {
    let (tx, _rx) = mpsc::unbounded_channel();
    StreamExecutionEngine::new(tx)
}

fn create_emit_final_group_by_query() -> StreamingQuery {
    StreamingQuery::Select {
        distinct: false,
        fields: vec![
            SelectField::Column("station".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "MIN".to_string(),
                    args: vec![Expr::Column("temperature".to_string())],
                },
                alias: Some("min_temp".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("temperature".to_string())],
                },
                alias: Some("avg_temp".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "MAX".to_string(),
                    args: vec![Expr::Column("temperature".to_string())],
                },
                alias: Some("max_temp".to_string()),
            },
        ],
        key_fields: None,
        from: StreamSource::Stream("measurements".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("station".to_string())]),
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
    }
}

fn create_emit_changes_group_by_query() -> StreamingQuery {
    StreamingQuery::Select {
        distinct: false,
        fields: vec![
            SelectField::Column("station".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Column("station".to_string())],
                },
                alias: Some("cnt".to_string()),
            },
        ],
        key_fields: None,
        from: StreamSource::Stream("measurements".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("station".to_string())]),
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: Some(EmitMode::Changes),
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    }
}

#[test]
fn test_emit_final_no_window_suppresses_per_record_output() {
    let mut engine = create_engine();
    let query = create_emit_final_group_by_query();

    // Process 5 records — EMIT FINAL should suppress all per-record output
    let records = vec![
        create_station_record("London", 15.0),
        create_station_record("Paris", 20.0),
        create_station_record("London", 10.0),
        create_station_record("Paris", 25.0),
        create_station_record("London", 20.0),
    ];

    for record in &records {
        let results = engine.execute_with_record_sync(&query, record).unwrap();
        assert!(
            results.is_empty(),
            "EMIT FINAL should not produce per-record output"
        );
    }
}

#[test]
fn test_emit_final_flush_produces_correct_aggregates() {
    let mut engine = create_engine();
    let query = create_emit_final_group_by_query();

    // Process records for 3 stations
    let records = vec![
        create_station_record("London", 15.0),
        create_station_record("Paris", 20.0),
        create_station_record("Tokyo", 30.0),
        create_station_record("London", 10.0),
        create_station_record("Paris", 25.0),
        create_station_record("Tokyo", 35.0),
        create_station_record("London", 20.0),
        create_station_record("Paris", 15.0),
        create_station_record("Tokyo", 25.0),
    ];

    for record in &records {
        let _ = engine.execute_with_record_sync(&query, record).unwrap();
    }

    // Flush final aggregations (simulating source exhaustion)
    let final_results = engine.flush_final_aggregations(&query).unwrap();

    // Should produce exactly 3 records (one per station)
    assert_eq!(
        final_results.len(),
        3,
        "Should produce one record per unique station"
    );

    // Build a map for easier assertion
    let mut result_map: HashMap<String, &StreamRecord> = HashMap::new();
    for record in &final_results {
        if let Some(FieldValue::String(station)) = record.fields.get("station") {
            result_map.insert(station.clone(), record);
        }
    }

    // Verify London: min=10, avg=15, max=20
    let london = result_map.get("London").expect("London should be present");
    assert_eq!(
        london.fields.get("min_temp"),
        Some(&FieldValue::Float(10.0))
    );
    assert_eq!(
        london.fields.get("avg_temp"),
        Some(&FieldValue::Float(15.0))
    );
    assert_eq!(
        london.fields.get("max_temp"),
        Some(&FieldValue::Float(20.0))
    );

    // Verify Paris: min=15, avg=20, max=25
    let paris = result_map.get("Paris").expect("Paris should be present");
    assert_eq!(paris.fields.get("min_temp"), Some(&FieldValue::Float(15.0)));
    assert_eq!(paris.fields.get("avg_temp"), Some(&FieldValue::Float(20.0)));
    assert_eq!(paris.fields.get("max_temp"), Some(&FieldValue::Float(25.0)));

    // Verify Tokyo: min=25, avg=30, max=35
    let tokyo = result_map.get("Tokyo").expect("Tokyo should be present");
    assert_eq!(tokyo.fields.get("min_temp"), Some(&FieldValue::Float(25.0)));
    assert_eq!(tokyo.fields.get("avg_temp"), Some(&FieldValue::Float(30.0)));
    assert_eq!(tokyo.fields.get("max_temp"), Some(&FieldValue::Float(35.0)));
}

#[test]
fn test_emit_final_flush_empty_when_no_records() {
    let mut engine = create_engine();
    let query = create_emit_final_group_by_query();

    // Flush without processing any records
    let final_results = engine.flush_final_aggregations(&query).unwrap();
    assert!(
        final_results.is_empty(),
        "Flush with no records should produce empty results"
    );
}

#[test]
fn test_emit_changes_still_emits_per_record() {
    let mut engine = create_engine();
    let query = create_emit_changes_group_by_query();

    // Process records — EMIT CHANGES should produce output for each
    let record = create_station_record("London", 15.0);
    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    assert_eq!(
        results.len(),
        1,
        "EMIT CHANGES should produce output for each record"
    );
}

#[test]
fn test_emit_final_flush_with_create_stream_wrapper() {
    // This tests the production path: queries arrive as CreateStream { as_select: Select { ... } }
    // The query_id must match between execution and flush (both use the outer CreateStream id)
    let mut engine = create_engine();
    let inner_select = create_emit_final_group_by_query();
    let query = StreamingQuery::CreateStream {
        name: "results".to_string(),
        columns: None,
        as_select: Box::new(inner_select),
        properties: HashMap::new(),
        emit_mode: Some(EmitMode::Final),
        metric_annotations: Vec::new(),
        job_name: None,
    };

    // Process records through CreateStream wrapper
    let records = vec![
        create_station_record("London", 15.0),
        create_station_record("Paris", 20.0),
        create_station_record("London", 10.0),
        create_station_record("Paris", 25.0),
    ];

    for record in &records {
        let results = engine.execute_with_record_sync(&query, record).unwrap();
        assert!(
            results.is_empty(),
            "EMIT FINAL should suppress output even through CreateStream wrapper"
        );
    }

    // Flush should find group state registered under create_stream_results query_id
    let final_results = engine.flush_final_aggregations(&query).unwrap();
    assert_eq!(
        final_results.len(),
        2,
        "Should produce one record per station (London, Paris)"
    );

    let mut result_map: HashMap<String, &StreamRecord> = HashMap::new();
    for record in &final_results {
        if let Some(FieldValue::String(station)) = record.fields.get("station") {
            result_map.insert(station.clone(), record);
        }
    }

    let london = result_map.get("London").expect("London should be present");
    assert_eq!(
        london.fields.get("min_temp"),
        Some(&FieldValue::Float(10.0))
    );
    assert_eq!(
        london.fields.get("avg_temp"),
        Some(&FieldValue::Float(12.5))
    );
    assert_eq!(
        london.fields.get("max_temp"),
        Some(&FieldValue::Float(15.0))
    );
}

#[test]
fn test_emit_final_flush_no_debug_format_field_names() {
    // Regression test: field names must use proper names, not Debug format like `Column("station")`
    let mut engine = create_engine();
    let query = create_emit_final_group_by_query();

    let records = vec![
        create_station_record("London", 15.0),
        create_station_record("London", 20.0),
    ];

    for record in &records {
        let _ = engine.execute_with_record_sync(&query, record).unwrap();
    }

    let final_results = engine.flush_final_aggregations(&query).unwrap();
    assert_eq!(final_results.len(), 1);

    let record = &final_results[0];
    // Verify no field names contain Debug format artifacts
    for key in record.fields.keys() {
        assert!(
            !key.contains("Column("),
            "Field name '{}' contains Debug format — should use plain name",
            key
        );
        assert!(
            !key.contains("Function("),
            "Field name '{}' contains Debug format — should use plain name",
            key
        );
    }

    // Verify exactly the expected fields exist
    let expected_fields: Vec<&str> = vec!["station", "min_temp", "avg_temp", "max_temp"];
    assert_eq!(
        record.fields.len(),
        expected_fields.len(),
        "Expected {} fields but got {}: {:?}",
        expected_fields.len(),
        record.fields.len(),
        record.fields.keys().collect::<Vec<_>>()
    );
    for field in &expected_fields {
        assert!(
            record.fields.contains_key(*field),
            "Missing expected field '{}'",
            field
        );
    }
}

#[test]
fn test_emit_final_flush_does_nothing_for_emit_changes() {
    let mut engine = create_engine();
    let query = create_emit_changes_group_by_query();

    // Process some records with EMIT CHANGES
    let records = vec![
        create_station_record("London", 15.0),
        create_station_record("Paris", 20.0),
    ];
    for record in &records {
        let _ = engine.execute_with_record_sync(&query, record).unwrap();
    }

    // Flush should produce nothing for EMIT CHANGES queries
    let final_results = engine.flush_final_aggregations(&query).unwrap();
    assert!(
        final_results.is_empty(),
        "flush_final_aggregations should not produce results for EMIT CHANGES"
    );
}
