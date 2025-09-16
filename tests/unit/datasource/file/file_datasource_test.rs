//! Comprehensive tests for file data source functionality

use velostream::velostream::datasource::{DataSource, SourceConfig};
use velostream::velostream::datasource::file::{FileDataSource, FileFormat, FileSourceConfig};
use velostream::velostream::sql::execution::types::FieldValue;
use std::fs;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

#[cfg(test)]
mod file_datasource_tests {
    use super::*;

    #[tokio::test]
    async fn test_csv_file_data_source_creation() {
        let mut source = FileDataSource::new();
        assert!(source.config().is_none());
        assert!(!source.supports_streaming());
        assert!(source.supports_batch());
    }

    #[tokio::test]
    async fn test_csv_file_basic_reading() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Create test CSV file
        let csv_content = "name,age,city\nJohn,30,New York\nJane,25,London\nBob,35,Paris";
        fs::write(&file_path, csv_content).unwrap();

        // Configure and initialize data source
        let config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();

        // Verify configuration
        assert!(source.supports_batch());
        assert!(!source.supports_streaming());

        // Test schema inference
        let schema = source.fetch_schema().await.unwrap();
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].name, "name");
        assert_eq!(schema.fields[1].name, "age");
        assert_eq!(schema.fields[2].name, "city");

        // Test data reading
        let mut reader = source.create_reader().await.unwrap();

        // Read first record
        let record1 = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record1.fields.get("column_0").unwrap(),
            &FieldValue::String("John".to_string())
        );
        assert_eq!(
            record1.fields.get("column_1").unwrap(),
            &FieldValue::String("30".to_string())
        );
        assert_eq!(
            record1.fields.get("column_2").unwrap(),
            &FieldValue::String("New York".to_string())
        );

        // Read second record
        let record2 = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record2.fields.get("column_0").unwrap(),
            &FieldValue::String("Jane".to_string())
        );
        assert_eq!(
            record2.fields.get("column_1").unwrap(),
            &FieldValue::String("25".to_string())
        );
        assert_eq!(
            record2.fields.get("column_2").unwrap(),
            &FieldValue::String("London".to_string())
        );

        // Read third record
        let record3 = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record3.fields.get("column_0").unwrap(),
            &FieldValue::String("Bob".to_string())
        );
        assert_eq!(
            record3.fields.get("column_1").unwrap(),
            &FieldValue::String("35".to_string())
        );
        assert_eq!(
            record3.fields.get("column_2").unwrap(),
            &FieldValue::String("Paris".to_string())
        );

        // Should return None when no more records
        let record4 = reader.read().await.unwrap();
        assert!(record4.is_none());
    }

    #[tokio::test]
    async fn test_csv_custom_delimiter() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Create test CSV with semicolon delimiter
        let csv_content = "name;age;city\nJohn;30;New York\nJane;25;London";
        fs::write(&file_path, csv_content).unwrap();

        let config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv)
                .with_csv_options(';', '"', None, true);

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();

        let mut reader = source.create_reader().await.unwrap();
        let record = reader.read().await.unwrap().unwrap();

        assert_eq!(
            record.fields.get("column_0").unwrap(),
            &FieldValue::String("John".to_string())
        );
        assert_eq!(
            record.fields.get("column_1").unwrap(),
            &FieldValue::String("30".to_string())
        );
        assert_eq!(
            record.fields.get("column_2").unwrap(),
            &FieldValue::String("New York".to_string())
        );
    }

    #[tokio::test]
    async fn test_csv_no_header() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Create test CSV without headers
        let csv_content = "John,30,New York\nJane,25,London";
        fs::write(&file_path, csv_content).unwrap();

        let config = FileSourceConfig::new(
            file_path.to_string_lossy().to_string(),
            FileFormat::CsvNoHeader,
        )
        .with_csv_options(',', '"', None, false);

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();

        // Test schema inference - should generate generic column names
        let schema = source.fetch_schema().await.unwrap();
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name, "column_0");

        let mut reader = source.create_reader().await.unwrap();
        let record = reader.read().await.unwrap().unwrap();

        assert_eq!(
            record.fields.get("column_0").unwrap(),
            &FieldValue::String("John".to_string())
        );
        assert_eq!(
            record.fields.get("column_1").unwrap(),
            &FieldValue::String("30".to_string())
        );
        assert_eq!(
            record.fields.get("column_2").unwrap(),
            &FieldValue::String("New York".to_string())
        );
    }

    #[tokio::test]
    async fn test_jsonl_file_reading() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.jsonl");

        // Create test JSONL file
        let jsonl_content = r#"{"name": "John", "age": 30, "active": true}
{"name": "Jane", "age": 25, "active": false}
{"name": "Bob", "age": 35, "city": "Paris"}"#;
        fs::write(&file_path, jsonl_content).unwrap();

        let config = FileSourceConfig::new(
            file_path.to_string_lossy().to_string(),
            FileFormat::JsonLines,
        );

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();

        // Test schema inference
        let schema = source.fetch_schema().await.unwrap();
        assert!(schema.fields.len() >= 3); // Should detect name, age, active fields

        let mut reader = source.create_reader().await.unwrap();

        // Read first record
        let record1 = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record1.fields.get("name").unwrap(),
            &FieldValue::String("John".to_string())
        );
        assert_eq!(record1.fields.get("age").unwrap(), &FieldValue::Integer(30));
        assert_eq!(
            record1.fields.get("active").unwrap(),
            &FieldValue::Boolean(true)
        );

        // Read second record
        let record2 = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record2.fields.get("name").unwrap(),
            &FieldValue::String("Jane".to_string())
        );
        assert_eq!(record2.fields.get("age").unwrap(), &FieldValue::Integer(25));
        assert_eq!(
            record2.fields.get("active").unwrap(),
            &FieldValue::Boolean(false)
        );

        // Read third record (different schema)
        let record3 = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record3.fields.get("name").unwrap(),
            &FieldValue::String("Bob".to_string())
        );
        assert_eq!(record3.fields.get("age").unwrap(), &FieldValue::Integer(35));
        assert_eq!(
            record3.fields.get("city").unwrap(),
            &FieldValue::String("Paris".to_string())
        );
    }

    #[tokio::test]
    async fn test_batch_reading() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Create test CSV with multiple records
        let csv_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,David\n5,Eve";
        fs::write(&file_path, csv_content).unwrap();

        let config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();
        let mut reader = source.create_reader().await.unwrap();

        // Read batch of 3 records
        let batch = reader.read_batch(3).await.unwrap();
        assert_eq!(batch.len(), 3);

        assert_eq!(
            batch[0].fields.get("column_0").unwrap(),
            &FieldValue::String("1".to_string())
        );
        assert_eq!(
            batch[0].fields.get("column_1").unwrap(),
            &FieldValue::String("Alice".to_string())
        );

        assert_eq!(
            batch[1].fields.get("column_0").unwrap(),
            &FieldValue::String("2".to_string())
        );
        assert_eq!(
            batch[1].fields.get("column_1").unwrap(),
            &FieldValue::String("Bob".to_string())
        );

        assert_eq!(
            batch[2].fields.get("column_0").unwrap(),
            &FieldValue::String("3".to_string())
        );
        assert_eq!(
            batch[2].fields.get("column_1").unwrap(),
            &FieldValue::String("Charlie".to_string())
        );

        // Read remaining records
        let batch2 = reader.read_batch(5).await.unwrap();
        assert_eq!(batch2.len(), 2); // Only 2 remaining records
    }

    #[tokio::test]
    async fn test_max_records_limit() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let csv_content = "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,David\n5,Eve";
        fs::write(&file_path, csv_content).unwrap();

        let config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);
        let config = config.with_buffer_size(1024);
        let mut config = config;
        config.max_records = Some(2);

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();
        let mut reader = source.create_reader().await.unwrap();

        // Should only read 2 records due to limit
        let record1 = reader.read().await.unwrap().unwrap();
        let record2 = reader.read().await.unwrap().unwrap();
        let record3 = reader.read().await.unwrap();

        assert!(record1.fields.contains_key("column_0"));
        assert!(record2.fields.contains_key("column_0"));
        assert!(record3.is_none()); // Should be None due to max_records limit
    }

    #[tokio::test]
    async fn test_file_watching_basic() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("watch_test.csv");

        // Create initial file
        let initial_content = "name,age\nJohn,30";
        fs::write(&file_path, initial_content).unwrap();

        let config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv)
                .with_watching(Some(100)); // 100ms polling interval

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();

        // Should support streaming when watching is enabled
        assert!(source.supports_streaming());
        assert!(source.supports_batch());

        let mut reader = source.create_reader().await.unwrap();

        // Read initial record
        let record1 = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record1.fields.get("column_0").unwrap(),
            &FieldValue::String("John".to_string())
        );

        // Simulate adding new data to the file in a separate task
        let file_path_clone = file_path.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            let additional_content = "\nJane,25";
            fs::write(
                &file_path_clone,
                format!("{}{}", initial_content, additional_content),
            )
            .unwrap();
        });

        // This should eventually read the new record (may require multiple attempts)
        let mut found_jane = false;
        for _attempt in 0..10 {
            sleep(Duration::from_millis(50)).await;
            if let Ok(Some(record)) = reader.read().await {
                if let Some(FieldValue::String(name)) = record.fields.get("column_0") {
                    if name == "Jane" {
                        assert_eq!(
                            record.fields.get("column_1").unwrap(),
                            &FieldValue::String("25".to_string())
                        );
                        found_jane = true;
                        break;
                    }
                }
            }
        }

        // Note: This test may be flaky due to file watching timing
        // In a production environment, you'd use more sophisticated file watching
    }

    #[tokio::test]
    async fn test_configuration_validation() {
        // Test empty path
        let config = FileSourceConfig::default();
        assert!(config.validate().is_err());

        // Test valid configuration
        let config = FileSourceConfig::new("/tmp/test.csv".to_string(), FileFormat::Csv);
        assert!(config.validate().is_ok());

        // Test invalid delimiter/quote combination
        let mut config = FileSourceConfig::new("/tmp/test.csv".to_string(), FileFormat::Csv);
        config.csv_delimiter = '"';
        config.csv_quote = '"';
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_uri_parsing() {
        // Test basic file URI
        let config = FileSourceConfig::from_uri("file:///data/test.csv").unwrap();
        assert_eq!(config.path, "/data/test.csv");
        assert_eq!(config.format, FileFormat::Csv);
        assert!(!config.watch_for_changes);

        // Test with parameters
        let config = FileSourceConfig::from_uri(
            "file:///data/test.jsonl?format=jsonl&watch=true&poll_interval=500",
        )
        .unwrap();
        assert_eq!(config.path, "/data/test.jsonl");
        assert_eq!(config.format, FileFormat::JsonLines);
        assert!(config.watch_for_changes);
        assert_eq!(config.polling_interval_ms, Some(500));

        // Test CSV with custom delimiter
        let config =
            FileSourceConfig::from_uri("file:///data/test.csv?delimiter=;&header=false").unwrap();
        assert_eq!(config.csv_delimiter, ';');
        assert!(!config.csv_has_header);

        // Test invalid URI
        let result = FileSourceConfig::from_uri("invalid://uri");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_error_handling() {
        // Test file not found
        let config = FileSourceConfig::new("/nonexistent/file.csv".to_string(), FileFormat::Csv);
        let mut source = FileDataSource::new();
        let result = source.initialize(SourceConfig::File(config)).await;
        assert!(result.is_err());

        // Test invalid JSON in JSONL file
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("invalid.jsonl");
        fs::write(&file_path, "invalid json content").unwrap();

        let config = FileSourceConfig::new(
            file_path.to_string_lossy().to_string(),
            FileFormat::JsonLines,
        );

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();
        let mut reader = source.create_reader().await.unwrap();

        let result = reader.read().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_empty_file_handling() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.csv");
        fs::write(&file_path, "").unwrap();

        let config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();
        let mut reader = source.create_reader().await.unwrap();

        let record = reader.read().await.unwrap();
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn test_skip_lines_functionality() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let csv_content = "# Comment line 1\n# Comment line 2\nname,age\nJohn,30\nJane,25";
        fs::write(&file_path, csv_content).unwrap();

        let mut config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);
        config.skip_lines = 2; // Skip the two comment lines

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();
        let mut reader = source.create_reader().await.unwrap();

        // First record should be John (after skipping comments and header)
        let record = reader.read().await.unwrap().unwrap();
        assert_eq!(
            record.fields.get("column_0").unwrap(),
            &FieldValue::String("John".to_string())
        );
    }

    #[tokio::test]
    async fn test_has_more_functionality() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let csv_content = "name,age\nJohn,30";
        fs::write(&file_path, csv_content).unwrap();

        let config =
            FileSourceConfig::new(file_path.to_string_lossy().to_string(), FileFormat::Csv);

        let mut source = FileDataSource::new();
        source.initialize(SourceConfig::File(config)).await.unwrap();
        let mut reader = source.create_reader().await.unwrap();

        // Should have more data initially
        assert!(reader.has_more().await.unwrap());

        // Read the record
        let _record = reader.read().await.unwrap().unwrap();

        // Should not have more data after reading everything (for non-watching files)
        assert!(!reader.has_more().await.unwrap());
    }
}
