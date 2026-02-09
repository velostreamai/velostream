//! Tests for memory-mapped file reader (FileMmapReader)
//!
//! Covers: CSV reading (with/without headers, custom delimiters), JSONL reading,
//! type inference, batch reading, seek, has_more, max_records, empty files,
//! skip_lines, Windows line endings, commit, error handling.

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

// ---------------------------------------------------------------------------
// JSONL format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_reader_jsonl_reading() {
    let jsonl_content = r#"{"name": "John", "age": 30, "active": true}
{"name": "Jane", "age": 25, "active": false}
{"name": "Bob", "age": 35, "city": "Paris"}
"#;
    let mut tmp = NamedTempFile::new().unwrap();
    write!(tmp, "{}", jsonl_content).unwrap();
    tmp.flush().unwrap();

    let config = FileSourceConfig {
        path: tmp.path().to_string_lossy().to_string(),
        format: FileFormat::JsonLines,
        ..Default::default()
    };

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

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

    // Third record — different schema (city instead of active)
    assert_eq!(
        records[2].fields.get("name"),
        Some(&FieldValue::String("Bob".to_string()))
    );
    assert_eq!(
        records[2].fields.get("city"),
        Some(&FieldValue::String("Paris".to_string()))
    );
}

#[tokio::test]
async fn test_mmap_reader_jsonl_with_null() {
    let jsonl_content = r#"{"name": "Alice", "value": null}
"#;
    let mut tmp = NamedTempFile::new().unwrap();
    write!(tmp, "{}", jsonl_content).unwrap();
    tmp.flush().unwrap();

    let config = FileSourceConfig {
        path: tmp.path().to_string_lossy().to_string(),
        format: FileFormat::JsonLines,
        ..Default::default()
    };

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(records[0].fields.get("value"), Some(&FieldValue::Null));
}

// ---------------------------------------------------------------------------
// skip_lines
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_reader_skip_lines() {
    let content = "# Comment line 1\n# Comment line 2\nname,age\nJohn,30\nJane,25\n";
    let mut tmp = NamedTempFile::new().unwrap();
    write!(tmp, "{}", content).unwrap();
    tmp.flush().unwrap();

    let config = FileSourceConfig {
        path: tmp.path().to_string_lossy().to_string(),
        format: FileFormat::Csv,
        csv_has_header: true,
        skip_lines: 2,
        ..Default::default()
    };

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    let records = reader.read().await.unwrap();
    assert!(!records.is_empty());
    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("John".to_string()))
    );
    assert_eq!(records[0].fields.get("age"), Some(&FieldValue::Integer(30)));
}

// ---------------------------------------------------------------------------
// Type inference — separate tests per type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_reader_type_inference_integer() {
    let content = "count\n42\n-17\n0\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::Csv, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

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
async fn test_mmap_reader_type_inference_float() {
    let content = "temperature\n12.5\n-7.83\n0.0\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::Csv, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

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
async fn test_mmap_reader_type_inference_boolean() {
    let content = "flag\ntrue\nfalse\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::Csv, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

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
// commit (no-op)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_reader_commit() {
    let content = "a;1\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::CsvNoHeader, ';');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    // commit() is a no-op — should succeed
    assert!(reader.commit().await.is_ok());

    // Should still be able to read after commit
    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 1);
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_reader_error_file_not_found() {
    let config = FileSourceConfig {
        path: "/nonexistent/path/file.csv".to_string(),
        format: FileFormat::Csv,
        ..Default::default()
    };

    let result = FileMmapReader::new(config, default_batch_config(), None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mmap_reader_error_invalid_jsonl() {
    let mut tmp = NamedTempFile::new().unwrap();
    write!(tmp, "this is not json\n").unwrap();
    tmp.flush().unwrap();

    let config = FileSourceConfig {
        path: tmp.path().to_string_lossy().to_string(),
        format: FileFormat::JsonLines,
        ..Default::default()
    };

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    let result = reader.read().await;
    // Invalid JSON should produce an error
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mmap_reader_error_json_array_unsupported() {
    let mut tmp = NamedTempFile::new().unwrap();
    write!(tmp, "[{{\"a\": 1}}, {{\"a\": 2}}]\n").unwrap();
    tmp.flush().unwrap();

    let config = FileSourceConfig {
        path: tmp.path().to_string_lossy().to_string(),
        format: FileFormat::Json,
        ..Default::default()
    };

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    let result = reader.read().await;
    // JSON array format is explicitly unsupported in mmap reader
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// CSV edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mmap_reader_csv_quoted_fields() {
    let content = "name,city\n\"John, Jr.\",\"New York\"\n\"Jane\",\"London\"\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::Csv, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].fields.get("name"),
        Some(&FieldValue::String("John, Jr.".to_string()))
    );
    assert_eq!(
        records[0].fields.get("city"),
        Some(&FieldValue::String("New York".to_string()))
    );
}

#[tokio::test]
async fn test_mmap_reader_csv_escaped_quotes() {
    let content = "message\n\"She said \"\"Hello\"\"\"\n\"Normal\"\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::Csv, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    let records = reader.read().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].fields.get("message"),
        Some(&FieldValue::String("She said \"Hello\"".to_string()))
    );
}

#[tokio::test]
async fn test_mmap_reader_csv_header_only() {
    let content = "name,age,city\n";
    let (_tmp, config) = create_temp_csv(content, FileFormat::Csv, ',');

    let mut reader = FileMmapReader::new(config, default_batch_config(), None)
        .await
        .unwrap();

    let records = reader.read().await.unwrap();
    assert!(records.is_empty());
    assert!(!reader.has_more().await.unwrap());
}
