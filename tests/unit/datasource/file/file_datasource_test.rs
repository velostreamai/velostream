//! Tests for file data source and file reader functionality
//!
//! Covers: FileDataSource creation/initialization, FileReader CSV/JSONL reading,
//! custom delimiters, no-header mode, empty files, skip_lines, has_more,
//! max_records, from_properties, batch reading, and error handling.

use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;
use velostream::velostream::datasource::file::FileDataSource;
use velostream::velostream::datasource::file::config::{FileFormat, FileSourceConfig};
use velostream::velostream::datasource::file::reader::FileReader;
use velostream::velostream::datasource::traits::{DataReader, DataSource};
use velostream::velostream::datasource::{BatchConfig, BatchStrategy};
use velostream::velostream::sql::execution::types::FieldValue;

// ---------------------------------------------------------------------------
// FileDataSource trait tests
// ---------------------------------------------------------------------------

#[test]
fn test_file_data_source_creation() {
    let source = FileDataSource::new();
    assert!(source.config().is_none());
    assert!(source.supports_batch());
}

#[test]
fn test_file_data_source_from_properties() {
    let mut props = HashMap::new();
    props.insert("path".to_string(), "/data/test.csv".to_string());
    props.insert("format".to_string(), "csv".to_string());
    props.insert("delimiter".to_string(), ";".to_string());
    props.insert("has_headers".to_string(), "false".to_string());

    let source = FileDataSource::from_properties(&props);
    let config = source.config().expect("config should be set");
    assert_eq!(config.path, "/data/test.csv");
    assert_eq!(config.csv_delimiter, ';');
    assert!(!config.csv_has_header);
}

#[test]
fn test_file_data_source_from_properties_source_prefix() {
    let mut props = HashMap::new();
    props.insert("source.path".to_string(), "/data/orders.csv".to_string());
    props.insert("source.format".to_string(), "csv_no_header".to_string());

    let source = FileDataSource::from_properties(&props);
    let config = source.config().expect("config should be set");
    assert_eq!(config.path, "/data/orders.csv");
    assert_eq!(config.format, FileFormat::CsvNoHeader);
}

#[tokio::test]
async fn test_file_data_source_initialize() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "name,age\nAlice,30\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut source = FileDataSource::new();
    let source_config = config.into();
    let result = source.initialize(source_config).await;
    assert!(result.is_ok());
    assert!(source.supports_batch());
}

#[tokio::test]
async fn test_file_data_source_metadata() {
    let source = FileDataSource::new();
    let metadata = source.metadata();
    assert_eq!(metadata.source_type, "file");
}

// ---------------------------------------------------------------------------
// FileReader — CSV reading
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_csv_basic_reading() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(
        &file_path,
        "name,age,city\nJohn,30,New York\nJane,25,London\nBob,35,Paris\n",
    )
    .unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 3);

    // First record
    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("John".to_string()))
    );
    assert_eq!(records[0].fields.get("age"), Some(&FieldValue::Integer(30)));
    assert_eq!(
        records[0].fields.get("city"),
        Some(&FieldValue::String("New York".to_string()))
    );

    // Second record
    assert_eq!(
        records[1].fields.get("name"),
        Some(&FieldValue::String("Jane".to_string()))
    );
    assert_eq!(records[1].fields.get("age"), Some(&FieldValue::Integer(25)));
}

#[tokio::test]
async fn test_csv_custom_delimiter() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(
        &file_path,
        "name;age;city\nJohn;30;New York\nJane;25;London\n",
    )
    .unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv)
        .with_csv_options(';', '"', None, true);

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 2);

    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("John".to_string()))
    );
    assert_eq!(records[0].fields.get("age"), Some(&FieldValue::Integer(30)));
    assert_eq!(
        records[0].fields.get("city"),
        Some(&FieldValue::String("New York".to_string()))
    );
}

#[tokio::test]
async fn test_csv_no_header() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "John,30,New York\nJane,25,London\n").unwrap();

    let config = FileSourceConfig {
        path: file_path.to_string_lossy().to_string(),
        format: FileFormat::CsvNoHeader,
        csv_has_header: false,
        ..Default::default()
    };

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 2);

    // Without headers, fields are column_0, column_1, etc.
    assert_eq!(
        records[0].fields.get("column_0"),
        Some(&FieldValue::String("John".to_string()))
    );
    assert_eq!(
        records[0].fields.get("column_1"),
        Some(&FieldValue::Integer(30))
    );
    assert_eq!(
        records[0].fields.get("column_2"),
        Some(&FieldValue::String("New York".to_string()))
    );
}

// ---------------------------------------------------------------------------
// FileReader — JSONL reading
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonl_reading() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.jsonl");

    let jsonl_content = r#"{"name": "John", "age": 30, "active": true}
{"name": "Jane", "age": 25, "active": false}
{"name": "Bob", "age": 35, "city": "Paris"}
"#;
    fs::write(&file_path, jsonl_content).unwrap();

    let config = FileSourceConfig::new(
        file_path.to_string_lossy().to_string(),
        FileFormat::JsonLines,
    );

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 3);

    // First record
    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("John".to_string()))
    );
    assert_eq!(records[0].fields.get("age"), Some(&FieldValue::Integer(30)));
    assert_eq!(
        records[0].fields.get("active"),
        Some(&FieldValue::Boolean(true))
    );

    // Second record
    assert_eq!(
        records[1].fields.get("name"),
        Some(&FieldValue::String("Jane".to_string()))
    );
    assert_eq!(
        records[1].fields.get("active"),
        Some(&FieldValue::Boolean(false))
    );

    // Third record (different schema — has city instead of active)
    assert_eq!(
        records[2].fields.get("name"),
        Some(&FieldValue::String("Bob".to_string()))
    );
    assert_eq!(
        records[2].fields.get("city"),
        Some(&FieldValue::String("Paris".to_string()))
    );
}

// ---------------------------------------------------------------------------
// FileReader — batch reading, limits, and state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_reading() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");

    let mut csv_content = "id,name\n".to_string();
    for i in 1..=10 {
        csv_content.push_str(&format!("{},Person_{}\n", i, i));
    }
    fs::write(&file_path, csv_content).unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    // Use small fixed batch size
    let batch_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(5),
        max_batch_size: 10000,
        batch_timeout: std::time::Duration::from_millis(1000),
    };

    let mut reader = FileReader::new_with_batch_config(config, batch_config, None)
        .await
        .unwrap();

    // Read all batches and verify total record count
    let mut total = 0;
    loop {
        let batch = reader.read().await.unwrap();
        if batch.is_empty() {
            break;
        }
        // Each batch should not exceed batch_size
        assert!(
            batch.len() <= 5,
            "batch size {} exceeds limit 5",
            batch.len()
        );
        total += batch.len();
    }
    assert_eq!(total, 10);
}

#[tokio::test]
async fn test_max_records_limit() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");

    let mut csv_content = "id,name\n".to_string();
    for i in 1..=10 {
        csv_content.push_str(&format!("{},Person_{}\n", i, i));
    }
    fs::write(&file_path, csv_content).unwrap();

    let mut config =
        FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);
    config.max_records = Some(3);

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();

    // Should be limited by max_records
    assert!(records.len() <= 3);
}

#[tokio::test]
async fn test_has_more_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "name,age\nJohn,30\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut reader = FileReader::new(config).await.unwrap();

    // Should have more data initially
    assert!(reader.has_more().await.unwrap());

    // Read all records
    let _records = reader.read().await.unwrap();

    // After reading everything, should not have more
    assert!(!reader.has_more().await.unwrap());
}

#[tokio::test]
async fn test_empty_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("empty.csv");
    fs::write(&file_path, "").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();
    assert!(records.is_empty());
}

#[tokio::test]
async fn test_skip_lines_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");

    let csv_content = "# Comment line 1\n# Comment line 2\nname,age\nJohn,30\nJane,25\n";
    fs::write(&file_path, csv_content).unwrap();

    let mut config =
        FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);
    config.skip_lines = 2; // Skip the two comment lines

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();

    // After skipping 2 comment lines, the header "name,age" is consumed, then data rows
    assert!(!records.is_empty());
    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("John".to_string()))
    );
}

// ---------------------------------------------------------------------------
// FileReader — type inference
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_type_inference_integer() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "count\n42\n-17\n0\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();

    assert_eq!(
        records[0].fields.get("count"),
        Some(&FieldValue::Integer(42))
    );
    assert_eq!(
        records[1].fields.get("count"),
        Some(&FieldValue::Integer(-17))
    );
    assert_eq!(
        records[2].fields.get("count"),
        Some(&FieldValue::Integer(0))
    );
}

#[tokio::test]
async fn test_type_inference_float() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "temperature\n12.5\n-7.83\n0.0\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();

    assert_eq!(
        records[0].fields.get("temperature"),
        Some(&FieldValue::Float(12.5))
    );
    assert_eq!(
        records[1].fields.get("temperature"),
        Some(&FieldValue::Float(-7.83))
    );
}

#[tokio::test]
async fn test_type_inference_boolean() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "flag\ntrue\nfalse\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut reader = FileReader::new(config).await.unwrap();
    let records = reader.read().await.unwrap();

    assert_eq!(
        records[0].fields.get("flag"),
        Some(&FieldValue::Boolean(true))
    );
    assert_eq!(
        records[1].fields.get("flag"),
        Some(&FieldValue::Boolean(false))
    );
}

// ---------------------------------------------------------------------------
// FileReader — error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_error_file_not_found() {
    let config = FileSourceConfig::new("/nonexistent/path/file.csv".to_string(), FileFormat::Csv);

    let result = FileReader::new(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_error_invalid_jsonl() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("invalid.jsonl");
    fs::write(&file_path, "this is not json\n").unwrap();

    let config = FileSourceConfig::new(
        file_path.to_string_lossy().to_string(),
        FileFormat::JsonLines,
    );

    let mut reader = FileReader::new(config).await.unwrap();
    let result = reader.read().await;

    // Invalid JSON should produce an error or skip the malformed record
    // The behavior depends on the implementation — either is acceptable
    match result {
        Ok(records) => {
            // If it skips bad records, we should get 0
            assert!(records.is_empty());
        }
        Err(_) => {
            // Error on bad JSON is also valid
        }
    }
}

// ---------------------------------------------------------------------------
// FileDataSource — schema inference
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_csv_schema_inference() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "name,age,city\nAlice,30,NYC\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut source = FileDataSource::new();
    source.initialize(config.into()).await.unwrap();

    let schema = source.fetch_schema().await.unwrap();
    assert_eq!(schema.fields.len(), 3);
    assert_eq!(schema.fields[0].name, "name");
    assert_eq!(schema.fields[1].name, "age");
    assert_eq!(schema.fields[2].name, "city");
}
