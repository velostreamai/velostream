//! Tests for ISO 8601 date/datetime parsing in the CSV file reader.

use std::collections::HashMap;
use velostream::velostream::datasource::file::config::{FileFormat, FileSourceConfig};
use velostream::velostream::datasource::file::reader::FileReader;
use velostream::velostream::datasource::traits::DataReader;
use velostream::velostream::sql::execution::types::FieldValue;

async fn read_csv(headers: &[&str], values: &[&str]) -> HashMap<String, FieldValue> {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    let header_line = headers.join(",");
    let value_line = values.join(",");
    std::fs::write(&csv_path, format!("{}\n{}\n", header_line, value_line)).unwrap();

    let config = FileSourceConfig {
        path: csv_path.to_str().unwrap().to_string(),
        format: FileFormat::Csv,
        watch_for_changes: false,
        polling_interval_ms: None,
        csv_delimiter: ',',
        ..Default::default()
    };

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();
    assert!(!records.is_empty(), "Expected at least one record");
    records.into_iter().next().unwrap().fields
}

#[tokio::test]
async fn test_date_field_parses_iso8601_date() {
    let fields = read_csv(&["id", "effective_date"], &["1", "2025-01-01"]).await;
    match fields.get("effective_date").unwrap() {
        FieldValue::Date(d) => assert_eq!(d.to_string(), "2025-01-01"),
        other => panic!("Expected Date, got {:?}", other),
    }
}

#[tokio::test]
async fn test_date_field_parses_iso8601_datetime() {
    let fields = read_csv(&["id", "created_datetime"], &["1", "2025-06-15T14:30:00"]).await;
    match fields.get("created_datetime").unwrap() {
        FieldValue::Timestamp(dt) => assert_eq!(dt.to_string(), "2025-06-15 14:30:00"),
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}

#[tokio::test]
async fn test_non_date_field_fallback_parses_iso8601_date() {
    // Even without "date" in name, infer_field_type_simple should catch YYYY-MM-DD
    let fields = read_csv(&["id", "start"], &["1", "2025-03-20"]).await;
    match fields.get("start").unwrap() {
        FieldValue::Date(d) => assert_eq!(d.to_string(), "2025-03-20"),
        other => panic!("Expected Date from fallback, got {:?}", other),
    }
}

#[tokio::test]
async fn test_expiry_date_field() {
    let fields = read_csv(&["entity", "expiry_date"], &["ACME", "2025-12-31"]).await;
    match fields.get("expiry_date").unwrap() {
        FieldValue::Date(d) => assert_eq!(d.to_string(), "2025-12-31"),
        other => panic!("Expected Date, got {:?}", other),
    }
}
