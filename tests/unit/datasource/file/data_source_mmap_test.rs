//! Tests for memory-mapped file data source (FileMmapDataSource)
//!
//! Covers: creation, from_properties, initialization, metadata,
//! supports_batch/streaming, partition_count, fetch_schema, create_reader.

use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;
use velostream::velostream::datasource::file::config::{FileFormat, FileSourceConfig};
use velostream::velostream::datasource::file::data_source_mmap::FileMmapDataSource;
use velostream::velostream::datasource::traits::DataSource;

// ---------------------------------------------------------------------------
// FileMmapDataSource — construction
// ---------------------------------------------------------------------------

#[test]
fn test_mmap_data_source_creation() {
    let source = FileMmapDataSource::new();
    assert!(source.supports_batch());
    assert!(!source.supports_streaming());
}

#[test]
fn test_mmap_data_source_default() {
    let source = FileMmapDataSource::default();
    assert!(source.supports_batch());
    assert!(!source.supports_streaming());
}

#[test]
fn test_mmap_data_source_partition_count() {
    let source = FileMmapDataSource::new();
    // mmap source returns None — doesn't constrain processing parallelism
    assert_eq!(source.partition_count(), None);
}

// ---------------------------------------------------------------------------
// FileMmapDataSource — from_properties
// ---------------------------------------------------------------------------

#[test]
fn test_mmap_data_source_from_properties() {
    let mut props = HashMap::new();
    props.insert("path".to_string(), "/data/test.csv".to_string());
    props.insert("format".to_string(), "csv".to_string());
    props.insert("delimiter".to_string(), ";".to_string());
    props.insert("has_headers".to_string(), "false".to_string());

    let source = FileMmapDataSource::from_properties(&props);
    assert!(source.supports_batch());

    let config = source.to_source_config();
    match config {
        velostream::velostream::datasource::config::SourceConfig::File { path, .. } => {
            assert_eq!(path, "/data/test.csv");
        }
        _ => panic!("Expected File source config"),
    }
}

#[test]
fn test_mmap_data_source_from_properties_source_prefix() {
    let mut props = HashMap::new();
    props.insert("source.path".to_string(), "/data/orders.csv".to_string());
    props.insert("source.format".to_string(), "csv_no_header".to_string());
    props.insert("source.delimiter".to_string(), ";".to_string());

    let source = FileMmapDataSource::from_properties(&props);

    let config = source.to_source_config();
    match config {
        velostream::velostream::datasource::config::SourceConfig::File { path, .. } => {
            assert_eq!(path, "/data/orders.csv");
        }
        _ => panic!("Expected File source config"),
    }
}

#[test]
fn test_mmap_data_source_from_properties_defaults() {
    // Empty properties should use defaults
    let props = HashMap::new();
    let source = FileMmapDataSource::from_properties(&props);
    assert!(source.supports_batch());

    let config = source.to_source_config();
    match config {
        velostream::velostream::datasource::config::SourceConfig::File { path, .. } => {
            // Default path
            assert_eq!(path, "./data/input.csv");
        }
        _ => panic!("Expected File source config"),
    }
}

// ---------------------------------------------------------------------------
// FileMmapDataSource — initialize
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_data_source_initialize() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "name,age\nAlice,30\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut source = FileMmapDataSource::new();
    let source_config = config.into();
    let result = source.initialize(source_config).await;
    assert!(result.is_ok());
    assert!(source.supports_batch());
    assert!(!source.supports_streaming());
}

#[tokio::test]
async fn test_mmap_data_source_initialize_nonexistent_file() {
    let config = FileSourceConfig::new("/nonexistent/path/file.csv".to_string(), FileFormat::Csv);

    let mut source = FileMmapDataSource::new();
    let source_config = config.into();
    let result = source.initialize(source_config).await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// FileMmapDataSource — metadata
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_data_source_metadata_before_init() {
    let source = FileMmapDataSource::new();
    let metadata = source.metadata();
    assert_eq!(metadata.source_type, "file_mmap");
    assert!(metadata.supports_batch);
    assert!(!metadata.supports_streaming);
}

#[tokio::test]
async fn test_mmap_data_source_metadata_after_init() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "name,age\nAlice,30\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut source = FileMmapDataSource::new();
    source.initialize(config.into()).await.unwrap();

    let metadata = source.metadata();
    assert_eq!(metadata.source_type, "file_mmap");
    assert_eq!(metadata.version, "1.0");
    assert!(metadata.supports_batch);
    assert!(!metadata.supports_streaming);
    assert!(!metadata.supports_schema_evolution);
    assert!(metadata.capabilities.contains(&"mmap".to_string()));
    assert!(
        metadata
            .capabilities
            .contains(&"schema_inference".to_string())
    );
}

// ---------------------------------------------------------------------------
// FileMmapDataSource — schema inference
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_data_source_csv_schema_inference() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "name,age,city\nAlice,30,NYC\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut source = FileMmapDataSource::new();
    source.initialize(config.into()).await.unwrap();

    let schema = source.fetch_schema().await.unwrap();
    assert_eq!(schema.fields.len(), 3);
    assert_eq!(schema.fields[0].name, "name");
    assert_eq!(schema.fields[1].name, "age");
    assert_eq!(schema.fields[2].name, "city");
}

#[tokio::test]
async fn test_mmap_data_source_schema_not_initialized() {
    let source = FileMmapDataSource::new();
    let result = source.fetch_schema().await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// FileMmapDataSource — create_reader
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_data_source_create_reader() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    fs::write(&file_path, "name,age\nAlice,30\nBob,25\n").unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut source = FileMmapDataSource::new();
    source.initialize(config.into()).await.unwrap();

    let mut reader = source.create_reader().await.unwrap();
    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn test_mmap_data_source_create_reader_not_initialized() {
    let source = FileMmapDataSource::new();
    let result = source.create_reader().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mmap_data_source_create_reader_with_batch_config() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");

    let mut csv_content = "id,name\n".to_string();
    for i in 1..=20 {
        csv_content.push_str(&format!("{},Person_{}\n", i, i));
    }
    fs::write(&file_path, csv_content).unwrap();

    let config = FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

    let mut source = FileMmapDataSource::new();
    source.initialize(config.into()).await.unwrap();

    let batch_config = velostream::velostream::datasource::BatchConfig {
        enable_batching: true,
        strategy: velostream::velostream::datasource::BatchStrategy::FixedSize(5),
        max_batch_size: 10000,
        batch_timeout: std::time::Duration::from_millis(1000),
    };

    let mut reader = source
        .create_reader_with_batch_config(batch_config)
        .await
        .unwrap();

    // First batch should be <= 5 records
    let batch = reader.read().await.unwrap();
    assert!(
        batch.len() <= 5,
        "batch size {} exceeds limit 5",
        batch.len()
    );

    // Read all remaining and verify total
    let mut total = batch.len();
    while reader.has_more().await.unwrap() {
        let batch = reader.read().await.unwrap();
        if batch.is_empty() {
            break;
        }
        total += batch.len();
    }
    assert_eq!(total, 20);
}

// ---------------------------------------------------------------------------
// FileMmapDataSource — to_source_config
// ---------------------------------------------------------------------------

#[test]
fn test_mmap_data_source_to_source_config_no_init() {
    let source = FileMmapDataSource::new();
    let config = source.to_source_config();
    match config {
        velostream::velostream::datasource::config::SourceConfig::File { path, .. } => {
            assert_eq!(path, "./data/input.csv");
        }
        _ => panic!("Expected File source config"),
    }
}

#[test]
fn test_mmap_data_source_to_source_config_csv() {
    let mut props = HashMap::new();
    props.insert("path".to_string(), "/data/input.csv".to_string());
    props.insert("format".to_string(), "csv".to_string());

    let source = FileMmapDataSource::from_properties(&props);
    let config = source.to_source_config();

    match config {
        velostream::velostream::datasource::config::SourceConfig::File { path, format, .. } => {
            assert_eq!(path, "/data/input.csv");
            match format {
                velostream::velostream::datasource::config::FileFormat::Csv { header, .. } => {
                    assert!(header);
                }
                _ => panic!("Expected Csv format"),
            }
        }
        _ => panic!("Expected File source config"),
    }
}

#[test]
fn test_mmap_data_source_to_source_config_jsonl() {
    let mut props = HashMap::new();
    props.insert("path".to_string(), "/data/events.jsonl".to_string());
    props.insert("format".to_string(), "jsonlines".to_string());

    let source = FileMmapDataSource::from_properties(&props);
    let config = source.to_source_config();

    match config {
        velostream::velostream::datasource::config::SourceConfig::File { path, format, .. } => {
            assert_eq!(path, "/data/events.jsonl");
            match format {
                velostream::velostream::datasource::config::FileFormat::Json => {}
                _ => panic!("Expected Json format for jsonlines"),
            }
        }
        _ => panic!("Expected File source config"),
    }
}
