//! Tests for memory-mapped file reader (FileMmapReader)

use std::io::Write;
use std::time::Duration;
use tempfile::NamedTempFile;
use velostream::velostream::datasource::file::config::{FileFormat, FileSourceConfig};
use velostream::velostream::datasource::file::reader_mmap::FileMmapReader;
use velostream::velostream::datasource::traits::DataReader;
use velostream::velostream::datasource::types::SourceOffset;
use velostream::velostream::datasource::{BatchConfig, BatchStrategy};
use velostream::velostream::sql::execution::types::FieldValue;

/// Create a temp file with the given content and return a FileSourceConfig pointing to it
fn create_temp_csv(
    content: &str,
    format: FileFormat,
    delimiter: char,
) -> (NamedTempFile, FileSourceConfig) {
    let mut tmp = NamedTempFile::new().expect("Failed to create temp file");
    write!(tmp, "{}", content).expect("Failed to write temp file");
    tmp.flush().expect("Failed to flush temp file");

    let has_header = format == FileFormat::Csv;
    let config = FileSourceConfig {
        path: tmp.path().to_string_lossy().to_string(),
        format,
        csv_delimiter: delimiter,
        csv_has_header: has_header,
        ..Default::default()
    };

    (tmp, config)
}

fn default_batch_config() -> BatchConfig {
    BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 10000,
        batch_timeout: Duration::from_millis(1000),
    }
}

#[tokio::test]
async fn test_mmap_reader_csv_with_header() {
    let content = "name,value\nalice,42\nbob,99\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::Csv, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 2);

    // First record: alice,42
    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("alice".to_string()))
    );
    assert_eq!(
        records[0].fields.get("value"),
        Some(&FieldValue::Integer(42))
    );

    // Second record: bob,99
    assert_eq!(
        records[1].fields.get("name"),
        Some(&FieldValue::String("bob".to_string()))
    );
    assert_eq!(
        records[1].fields.get("value"),
        Some(&FieldValue::Integer(99))
    );
}

#[tokio::test]
async fn test_mmap_reader_csv_no_header() {
    let content = "alice,42\nbob,99\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::CsvNoHeader, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 2);

    // Without headers, fields are column_0, column_1
    assert_eq!(
        records[0].fields.get("column_0"),
        Some(&FieldValue::String("alice".to_string()))
    );
    assert_eq!(
        records[0].fields.get("column_1"),
        Some(&FieldValue::Integer(42))
    );
}

#[tokio::test]
async fn test_mmap_reader_semicolon_delimiter() {
    // 1BRC format: station;temperature
    let content = "Hamburg;12.0\nBerlin;10.3\nMunich;-5.7\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::CsvNoHeader, ';');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 3);

    assert_eq!(
        records[0].fields.get("column_0"),
        Some(&FieldValue::String("Hamburg".to_string()))
    );
    assert_eq!(
        records[0].fields.get("column_1"),
        Some(&FieldValue::Float(12.0))
    );

    assert_eq!(
        records[2].fields.get("column_0"),
        Some(&FieldValue::String("Munich".to_string()))
    );
    assert_eq!(
        records[2].fields.get("column_1"),
        Some(&FieldValue::Float(-5.7))
    );
}

#[tokio::test]
async fn test_mmap_reader_has_more() {
    let content = "a;1\nb;2\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::CsvNoHeader, ';');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    assert!(reader.has_more().await.expect("has_more failed"));

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 2);

    // After consuming all records, has_more should return false
    assert!(!reader.has_more().await.expect("has_more failed"));
}

#[tokio::test]
async fn test_mmap_reader_empty_file() {
    let (_tmp, config) = create_temp_csv("", FileFormat::CsvNoHeader, ',');

    // Empty file should be handled gracefully (the file has 0 bytes)
    // The mmap reader skips empty files, so it should be finished
    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    let records = reader.read().await.expect("Failed to read records");
    assert!(records.is_empty());
    assert!(!reader.has_more().await.expect("has_more failed"));
}

#[tokio::test]
async fn test_mmap_reader_type_inference() {
    let content = "hello;42;2.72;true\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::CsvNoHeader, ';');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 1);

    let record = &records[0];
    assert_eq!(
        record.fields.get("column_0"),
        Some(&FieldValue::String("hello".to_string()))
    );
    assert_eq!(
        record.fields.get("column_1"),
        Some(&FieldValue::Integer(42))
    );
    assert_eq!(
        record.fields.get("column_2"),
        Some(&FieldValue::Float(2.72))
    );
    assert_eq!(
        record.fields.get("column_3"),
        Some(&FieldValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_mmap_reader_batch_reading() {
    // Generate 50 rows
    let mut content = String::new();
    for i in 0..50 {
        content.push_str(&format!("station_{};{:.1}\n", i, i as f64 * 0.5));
    }

    let (_tmp, config) = create_temp_csv(&content, FileFormat::CsvNoHeader, ';');

    // Use small batch size to test multi-batch reading
    let batch_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(10),
        max_batch_size: 10000,
        batch_timeout: Duration::from_millis(1000),
    };

    let mut reader = FileMmapReader::new(config, batch_config, None)
        .await
        .expect("Failed to create mmap reader");

    let batch1 = reader.read().await.expect("Failed to read batch 1");
    assert_eq!(batch1.len(), 10);

    let batch2 = reader.read().await.expect("Failed to read batch 2");
    assert_eq!(batch2.len(), 10);

    // Read remaining
    let mut total = batch1.len() + batch2.len();
    while reader.has_more().await.expect("has_more failed") {
        let batch = reader.read().await.expect("Failed to read batch");
        if batch.is_empty() {
            break;
        }
        total += batch.len();
    }
    assert_eq!(total, 50);
}

#[tokio::test]
async fn test_mmap_reader_seek() {
    let content = "aaa;1\nbbb;2\nccc;3\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::CsvNoHeader, ';');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    // Seek to byte offset 6 (start of "bbb;2\n")
    reader
        .seek(SourceOffset::File {
            path: String::new(),
            byte_offset: 6,
            line_number: 0,
        })
        .await
        .expect("seek failed");

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 2); // bbb and ccc

    assert_eq!(
        records[0].fields.get("column_0"),
        Some(&FieldValue::String("bbb".to_string()))
    );
}

#[tokio::test]
async fn test_mmap_reader_max_records() {
    let mut content = String::new();
    for i in 0..100 {
        content.push_str(&format!("row_{};{}\n", i, i));
    }

    let (_tmp, mut config) = create_temp_csv(&content, FileFormat::CsvNoHeader, ';');
    config.max_records = Some(5);

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 5);

    // has_more should be false due to max_records
    assert!(!reader.has_more().await.expect("has_more failed"));
}

#[tokio::test]
async fn test_mmap_reader_windows_line_endings() {
    let content = "a;1\r\nb;2\r\nc;3\r\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::CsvNoHeader, ';');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .expect("Failed to create mmap reader");

    let records = reader.read().await.expect("Failed to read records");
    assert_eq!(records.len(), 3);

    // Ensure \r is stripped
    assert_eq!(
        records[0].fields.get("column_0"),
        Some(&FieldValue::String("a".to_string()))
    );
    assert_eq!(
        records[0].fields.get("column_1"),
        Some(&FieldValue::Integer(1))
    );
}
