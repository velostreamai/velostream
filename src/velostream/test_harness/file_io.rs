//! File-based source/sink support for test harness
//!
//! Provides functionality for:
//! - Loading test data from CSV/JSON files
//! - Writing query output to files
//! - Creating FileDataSource and FileDataSink from test specs

use super::error::{TestHarnessError, TestHarnessResult};
use super::spec::{FileFormat, SinkType, SourceType};
use super::utils::{field_value_to_csv_string, field_value_to_json, resolve_path};
use crate::velostream::datasource::file::config::{FileSinkConfig, FileSourceConfig};
use crate::velostream::datasource::file::reader::FileReader;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// Factory for creating file-based data sources
pub struct FileSourceFactory;

impl FileSourceFactory {
    /// Create a FileReader from SourceType configuration
    pub async fn create_reader(
        source_type: &SourceType,
        base_dir: &Path,
    ) -> TestHarnessResult<FileReader> {
        match source_type {
            SourceType::File {
                path,
                format,
                watch,
            } => {
                let full_path = resolve_path(path, base_dir);

                let config = FileSourceConfig {
                    path: full_path.to_string_lossy().to_string(),
                    format: format.clone(),
                    watch_for_changes: *watch,
                    ..Default::default()
                };

                FileReader::new(config)
                    .await
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to create file reader: {}", e),
                        source: None,
                    })
            }
            SourceType::Kafka { .. } => Err(TestHarnessError::ConfigError {
                message: "Cannot create FileReader from Kafka source type".to_string(),
            }),
        }
    }

    /// Load all records from a file into memory
    pub fn load_records(path: &Path, format: &FileFormat) -> TestHarnessResult<Vec<StreamRecord>> {
        match format {
            FileFormat::Csv | FileFormat::CsvNoHeader => Self::load_csv_records(path, format),
            FileFormat::Json | FileFormat::JsonLines => Self::load_json_records(path, format),
        }
    }

    /// Load records from a CSV file
    fn load_csv_records(path: &Path, format: &FileFormat) -> TestHarnessResult<Vec<StreamRecord>> {
        let file = File::open(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut records = Vec::new();

        // Get headers
        let headers: Vec<String> = if *format == FileFormat::Csv {
            match lines.next() {
                Some(Ok(header_line)) => split_csv_line(&header_line)
                    .iter()
                    .map(|s| s.trim().to_string())
                    .collect(),
                Some(Err(e)) => {
                    return Err(TestHarnessError::IoError {
                        message: format!("Failed to read CSV header: {}", e),
                        path: path.display().to_string(),
                    });
                }
                None => {
                    return Err(TestHarnessError::IoError {
                        message: "CSV file is empty".to_string(),
                        path: path.display().to_string(),
                    });
                }
            }
        } else {
            // For CsvNoHeader, use column indices as names
            Vec::new()
        };

        for (line_num, line_result) in lines.enumerate() {
            let line = line_result.map_err(|e| TestHarnessError::IoError {
                message: format!("Failed to read line {}: {}", line_num + 2, e),
                path: path.display().to_string(),
            })?;

            if line.trim().is_empty() {
                continue;
            }

            let values = split_csv_line(&line);
            let mut fields = HashMap::new();

            for (i, value) in values.iter().enumerate() {
                let field_name = if i < headers.len() {
                    headers[i].clone()
                } else {
                    format!("column_{}", i)
                };

                let field_value = parse_csv_value(value);
                fields.insert(field_name, field_value);
            }

            records.push(StreamRecord::new(fields));
        }

        Ok(records)
    }

    /// Load records from a JSON or JSON Lines file
    fn load_json_records(path: &Path, format: &FileFormat) -> TestHarnessResult<Vec<StreamRecord>> {
        let content = std::fs::read_to_string(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        match format {
            FileFormat::JsonLines => {
                let mut records = Vec::new();
                for (line_num, line) in content.lines().enumerate() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let record = parse_json_line(line, line_num, path)?;
                    records.push(record);
                }
                Ok(records)
            }
            FileFormat::Json => {
                // Parse as JSON array
                let json_value: serde_json::Value =
                    serde_json::from_str(&content).map_err(|e| TestHarnessError::IoError {
                        message: format!("Failed to parse JSON: {}", e),
                        path: path.display().to_string(),
                    })?;

                match json_value {
                    serde_json::Value::Array(arr) => {
                        let mut records = Vec::new();
                        for (i, item) in arr.into_iter().enumerate() {
                            let record = json_value_to_record(item, i, path)?;
                            records.push(record);
                        }
                        Ok(records)
                    }
                    serde_json::Value::Object(_) => {
                        // Single object
                        let record = json_value_to_record(json_value, 0, path)?;
                        Ok(vec![record])
                    }
                    _ => Err(TestHarnessError::IoError {
                        message: "JSON file must contain an array or object".to_string(),
                        path: path.display().to_string(),
                    }),
                }
            }
            _ => Err(TestHarnessError::ConfigError {
                message: format!("Unexpected format for JSON loading: {:?}", format),
            }),
        }
    }
}

/// Factory for creating file-based data sinks
pub struct FileSinkFactory;

impl FileSinkFactory {
    /// Create a FileSinkConfig from SinkType configuration
    pub fn create_config(
        sink_type: &SinkType,
        base_dir: &Path,
    ) -> TestHarnessResult<FileSinkConfig> {
        match sink_type {
            SinkType::File { path, format } => {
                let full_path = resolve_path(path, base_dir);

                Ok(FileSinkConfig {
                    path: full_path.to_string_lossy().to_string(),
                    format: format.clone(),
                    ..Default::default()
                })
            }
            SinkType::Kafka { .. } => Err(TestHarnessError::ConfigError {
                message: "Cannot create FileSinkConfig from Kafka sink type".to_string(),
            }),
        }
    }

    /// Write records to a file
    pub fn write_records(
        path: &Path,
        format: &FileFormat,
        records: &[StreamRecord],
    ) -> TestHarnessResult<()> {
        match format {
            FileFormat::Csv | FileFormat::CsvNoHeader => {
                Self::write_csv_records(path, format, records)
            }
            FileFormat::Json | FileFormat::JsonLines => {
                Self::write_json_records(path, format, records)
            }
        }
    }

    /// Write records to a CSV file
    fn write_csv_records(
        path: &Path,
        format: &FileFormat,
        records: &[StreamRecord],
    ) -> TestHarnessResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| TestHarnessError::IoError {
                message: format!("Failed to create directory: {}", e),
                path: parent.display().to_string(),
            })?;
        }

        let mut file = File::create(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        if records.is_empty() {
            return Ok(());
        }

        // Collect all unique field names
        let mut field_names: Vec<String> = records
            .first()
            .map(|r| r.fields.keys().cloned().collect())
            .unwrap_or_default();
        field_names.sort();

        // Write header if format is Csv (not CsvNoHeader)
        if *format == FileFormat::Csv {
            writeln!(file, "{}", field_names.join(",")).map_err(|e| TestHarnessError::IoError {
                message: format!("Failed to write CSV header: {}", e),
                path: path.display().to_string(),
            })?;
        }

        // Write records
        for record in records {
            let values: Vec<String> = field_names
                .iter()
                .map(|name| {
                    record
                        .fields
                        .get(name)
                        .map(|v| field_value_to_csv_string(v))
                        .unwrap_or_default()
                })
                .collect();
            writeln!(file, "{}", values.join(",")).map_err(|e| TestHarnessError::IoError {
                message: format!("Failed to write CSV record: {}", e),
                path: path.display().to_string(),
            })?;
        }

        Ok(())
    }

    /// Write records to a JSON or JSON Lines file
    fn write_json_records(
        path: &Path,
        format: &FileFormat,
        records: &[StreamRecord],
    ) -> TestHarnessResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| TestHarnessError::IoError {
                message: format!("Failed to create directory: {}", e),
                path: parent.display().to_string(),
            })?;
        }

        let mut file = File::create(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        match format {
            FileFormat::JsonLines => {
                for record in records {
                    let json = record_to_json(record);
                    writeln!(file, "{}", json).map_err(|e| TestHarnessError::IoError {
                        message: format!("Failed to write JSON line: {}", e),
                        path: path.display().to_string(),
                    })?;
                }
            }
            FileFormat::Json => {
                let json_array: Vec<serde_json::Value> = records
                    .iter()
                    .map(|r| {
                        serde_json::from_str(&record_to_json(r)).unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                let output = serde_json::to_string_pretty(&json_array).map_err(|e| {
                    TestHarnessError::IoError {
                        message: format!("Failed to serialize JSON: {}", e),
                        path: path.display().to_string(),
                    }
                })?;
                write!(file, "{}", output).map_err(|e| TestHarnessError::IoError {
                    message: format!("Failed to write JSON: {}", e),
                    path: path.display().to_string(),
                })?;
            }
            _ => {
                return Err(TestHarnessError::ConfigError {
                    message: format!("Unexpected format for JSON writing: {:?}", format),
                });
            }
        }

        Ok(())
    }

    /// Read captured output from a file sink
    pub fn read_output(path: &Path, format: &FileFormat) -> TestHarnessResult<Vec<StreamRecord>> {
        FileSourceFactory::load_records(path, format)
    }
}

/// Split a CSV line into fields, respecting double-quoted fields that may contain commas.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    // Escaped quote ("")
                    current.push('"');
                    chars.next();
                } else {
                    // End of quoted field
                    in_quotes = false;
                    current.push('"');
                }
            } else {
                current.push(c);
            }
        } else if c == '"' {
            in_quotes = true;
            current.push('"');
        } else if c == ',' {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(c);
        }
    }
    fields.push(current);
    fields
}

/// Parse a CSV value into a FieldValue
fn parse_csv_value(value: &str) -> FieldValue {
    let trimmed = value.trim();

    // Check for empty/null
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return FieldValue::Null;
    }

    // Check for quoted string
    if trimmed.starts_with('"') && trimmed.ends_with('"') {
        return FieldValue::String(trimmed[1..trimmed.len() - 1].to_string());
    }

    // Try to parse as integer
    if let Ok(i) = trimmed.parse::<i64>() {
        return FieldValue::Integer(i);
    }

    // Try to parse as float
    if let Ok(f) = trimmed.parse::<f64>() {
        return FieldValue::Float(f);
    }

    // Try to parse as boolean
    if trimmed.eq_ignore_ascii_case("true") {
        return FieldValue::Boolean(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return FieldValue::Boolean(false);
    }

    // Default to string
    FieldValue::String(trimmed.to_string())
}

/// Parse a JSON line into a StreamRecord
fn parse_json_line(line: &str, line_num: usize, path: &Path) -> TestHarnessResult<StreamRecord> {
    let json_value: serde_json::Value =
        serde_json::from_str(line).map_err(|e| TestHarnessError::IoError {
            message: format!("Failed to parse JSON at line {}: {}", line_num + 1, e),
            path: path.display().to_string(),
        })?;

    json_value_to_record(json_value, line_num, path)
}

/// Convert a JSON value to a StreamRecord
fn json_value_to_record(
    value: serde_json::Value,
    index: usize,
    path: &Path,
) -> TestHarnessResult<StreamRecord> {
    match value {
        serde_json::Value::Object(obj) => {
            let mut fields = HashMap::new();
            for (key, val) in obj {
                fields.insert(key, json_to_field_value(val));
            }
            Ok(StreamRecord::new(fields))
        }
        _ => Err(TestHarnessError::IoError {
            message: format!("Expected JSON object at index {}, got {:?}", index, value),
            path: path.display().to_string(),
        }),
    }
}

/// Convert a JSON value to a FieldValue
fn json_to_field_value(value: serde_json::Value) -> FieldValue {
    match value {
        serde_json::Value::Null => FieldValue::Null,
        serde_json::Value::Bool(b) => FieldValue::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FieldValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                FieldValue::Float(f)
            } else {
                FieldValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => FieldValue::String(s),
        serde_json::Value::Array(arr) => {
            // Convert array to string representation
            FieldValue::String(serde_json::to_string(&arr).unwrap_or_default())
        }
        serde_json::Value::Object(obj) => {
            // Convert nested object to string representation
            FieldValue::String(serde_json::to_string(&obj).unwrap_or_default())
        }
    }
}

/// Convert a StreamRecord to a JSON string
fn record_to_json(record: &StreamRecord) -> String {
    let mut obj = serde_json::Map::new();
    for (key, value) in &record.fields {
        obj.insert(key.clone(), field_value_to_json(value));
    }
    serde_json::to_string(&obj).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_value() {
        assert_eq!(parse_csv_value("123"), FieldValue::Integer(123));
        assert_eq!(parse_csv_value("123.45"), FieldValue::Float(123.45));
        assert_eq!(parse_csv_value("true"), FieldValue::Boolean(true));
        assert_eq!(parse_csv_value("false"), FieldValue::Boolean(false));
        assert_eq!(
            parse_csv_value("hello"),
            FieldValue::String("hello".to_string())
        );
        assert_eq!(parse_csv_value("null"), FieldValue::Null);
        assert_eq!(parse_csv_value(""), FieldValue::Null);
        assert_eq!(
            parse_csv_value("\"quoted\""),
            FieldValue::String("quoted".to_string())
        );
    }

    #[test]
    fn test_split_csv_line_with_quoted_commas() {
        // Simple fields
        let fields = split_csv_line("a,b,c");
        assert_eq!(fields, vec!["a", "b", "c"]);

        // Quoted field containing comma (the bug this function was written to fix)
        let fields = split_csv_line("\"Washington, D.C.\",10.5,20.3,15.0");
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0], "\"Washington, D.C.\"");
        assert_eq!(fields[1], "10.5");

        // Escaped quotes inside quoted field
        let fields = split_csv_line("\"He said \"\"hello\"\"\",42");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], "\"He said \"hello\"\"");
        assert_eq!(fields[1], "42");

        // Empty fields
        let fields = split_csv_line(",b,");
        assert_eq!(fields, vec!["", "b", ""]);

        // Single field
        let fields = split_csv_line("alone");
        assert_eq!(fields, vec!["alone"]);
    }

    #[test]
    fn test_csv_load_quoted_fields_with_commas() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "station,min_temp,max_temp").unwrap();
        writeln!(temp_file, "\"Washington, D.C.\",10.5,20.3").unwrap();
        writeln!(temp_file, "London,5.0,15.0").unwrap();

        let records =
            FileSourceFactory::load_records(temp_file.path(), &super::FileFormat::Csv).unwrap();
        assert_eq!(records.len(), 2);

        let r1 = &records[0];
        assert_eq!(
            r1.fields.get("station"),
            Some(&FieldValue::String("Washington, D.C.".to_string()))
        );
        assert_eq!(r1.fields.get("min_temp"), Some(&FieldValue::Float(10.5)));

        let r2 = &records[1];
        assert_eq!(
            r2.fields.get("station"),
            Some(&FieldValue::String("London".to_string()))
        );
    }

    #[test]
    fn test_json_to_field_value() {
        assert_eq!(
            json_to_field_value(serde_json::json!(null)),
            FieldValue::Null
        );
        assert_eq!(
            json_to_field_value(serde_json::json!(true)),
            FieldValue::Boolean(true)
        );
        assert_eq!(
            json_to_field_value(serde_json::json!(123)),
            FieldValue::Integer(123)
        );
        assert_eq!(
            json_to_field_value(serde_json::json!(123.45)),
            FieldValue::Float(123.45)
        );
        assert_eq!(
            json_to_field_value(serde_json::json!("hello")),
            FieldValue::String("hello".to_string())
        );
    }

    // Note: Tests for field_value_to_csv_string, field_value_to_json, and resolve_path
    // are now in utils.rs where those functions are defined

    #[test]
    fn test_csv_load_round_trip() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Create a temporary CSV file with header
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,value,active").unwrap();
        writeln!(temp_file, "1,Alice,100.5,true").unwrap();
        writeln!(temp_file, "2,Bob,200,false").unwrap();
        writeln!(temp_file, "3,Charlie,,true").unwrap(); // Empty value = null

        // Load records
        let records =
            FileSourceFactory::load_records(temp_file.path(), &super::FileFormat::Csv).unwrap();

        assert_eq!(records.len(), 3);

        // Check first record
        let r1 = &records[0];
        assert_eq!(r1.fields.get("id"), Some(&FieldValue::Integer(1)));
        assert_eq!(
            r1.fields.get("name"),
            Some(&FieldValue::String("Alice".to_string()))
        );
        assert_eq!(r1.fields.get("value"), Some(&FieldValue::Float(100.5)));
        assert_eq!(r1.fields.get("active"), Some(&FieldValue::Boolean(true)));

        // Check null value handling
        let r3 = &records[2];
        assert_eq!(r3.fields.get("value"), Some(&FieldValue::Null));
    }

    #[test]
    fn test_csv_write_and_read() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("output.csv");

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("id".to_string(), FieldValue::Integer(1));
        fields1.insert("name".to_string(), FieldValue::String("Test".to_string()));
        fields1.insert("value".to_string(), FieldValue::Float(99.5));

        let mut fields2 = HashMap::new();
        fields2.insert("id".to_string(), FieldValue::Integer(2));
        fields2.insert("name".to_string(), FieldValue::String("Data".to_string()));
        fields2.insert("value".to_string(), FieldValue::Null);

        let records = vec![
            crate::velostream::sql::execution::types::StreamRecord::new(fields1),
            crate::velostream::sql::execution::types::StreamRecord::new(fields2),
        ];

        // Write records
        FileSinkFactory::write_records(&output_path, &super::FileFormat::Csv, &records).unwrap();

        // Read them back
        let loaded =
            FileSourceFactory::load_records(&output_path, &super::FileFormat::Csv).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].fields.get("id"), Some(&FieldValue::Integer(1)));
        assert_eq!(loaded[1].fields.get("id"), Some(&FieldValue::Integer(2)));
    }

    #[test]
    fn test_json_lines_load_round_trip() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Create a temporary JSON Lines file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"id": 1, "name": "Alice", "score": 95.5}}"#).unwrap();
        writeln!(
            temp_file,
            r#"{{"id": 2, "name": "Bob", "score": 88.0, "active": true}}"#
        )
        .unwrap();
        writeln!(temp_file, r#"{{"id": 3, "name": "Charlie"}}"#).unwrap();

        // Load records
        let records =
            FileSourceFactory::load_records(temp_file.path(), &super::FileFormat::JsonLines)
                .unwrap();

        assert_eq!(records.len(), 3);

        // Check first record
        let r1 = &records[0];
        assert_eq!(r1.fields.get("id"), Some(&FieldValue::Integer(1)));
        assert_eq!(
            r1.fields.get("name"),
            Some(&FieldValue::String("Alice".to_string()))
        );
        assert_eq!(r1.fields.get("score"), Some(&FieldValue::Float(95.5)));

        // Check third record (missing fields don't exist)
        let r3 = &records[2];
        assert_eq!(r3.fields.get("score"), None);
    }

    #[test]
    fn test_json_array_load() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Create a temporary JSON array file
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(
            temp_file,
            r#"[
            {{"id": 1, "value": "first"}},
            {{"id": 2, "value": "second"}}
        ]"#
        )
        .unwrap();

        // Load records
        let records =
            FileSourceFactory::load_records(temp_file.path(), &super::FileFormat::Json).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].fields.get("id"), Some(&FieldValue::Integer(1)));
        assert_eq!(records[1].fields.get("id"), Some(&FieldValue::Integer(2)));
    }

    #[test]
    fn test_json_lines_write_and_read() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("output.jsonl");

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("id".to_string(), FieldValue::Integer(100));
        fields1.insert(
            "message".to_string(),
            FieldValue::String("Hello".to_string()),
        );
        fields1.insert("enabled".to_string(), FieldValue::Boolean(true));

        let records = vec![crate::velostream::sql::execution::types::StreamRecord::new(
            fields1,
        )];

        // Write records
        FileSinkFactory::write_records(&output_path, &super::FileFormat::JsonLines, &records)
            .unwrap();

        // Read them back
        let loaded =
            FileSourceFactory::load_records(&output_path, &super::FileFormat::JsonLines).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].fields.get("id"), Some(&FieldValue::Integer(100)));
        assert_eq!(
            loaded[0].fields.get("enabled"),
            Some(&FieldValue::Boolean(true))
        );
    }

    #[test]
    fn test_csv_no_header() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Create a temporary CSV file without header
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "1,Alice,100").unwrap();
        writeln!(temp_file, "2,Bob,200").unwrap();

        // Load records
        let records =
            FileSourceFactory::load_records(temp_file.path(), &super::FileFormat::CsvNoHeader)
                .unwrap();

        assert_eq!(records.len(), 2);

        // Columns should be named column_0, column_1, etc.
        let r1 = &records[0];
        assert_eq!(r1.fields.get("column_0"), Some(&FieldValue::Integer(1)));
        assert_eq!(
            r1.fields.get("column_1"),
            Some(&FieldValue::String("Alice".to_string()))
        );
        assert_eq!(r1.fields.get("column_2"), Some(&FieldValue::Integer(100)));
    }

    #[test]
    fn test_empty_file_handling() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // CSV with only header
        let mut csv_file = NamedTempFile::new().unwrap();
        writeln!(csv_file, "id,name").unwrap();

        let records =
            FileSourceFactory::load_records(csv_file.path(), &super::FileFormat::Csv).unwrap();
        assert_eq!(records.len(), 0);

        // Empty JSON Lines file
        let jsonl_file = NamedTempFile::new().unwrap();
        let records =
            FileSourceFactory::load_records(jsonl_file.path(), &super::FileFormat::JsonLines)
                .unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_file_not_found_error() {
        use std::path::Path;

        let result = FileSourceFactory::load_records(
            Path::new("/nonexistent/file.csv"),
            &super::FileFormat::Csv,
        );
        assert!(result.is_err());

        match result {
            Err(super::TestHarnessError::IoError { path, .. }) => {
                assert!(path.contains("nonexistent"));
            }
            _ => panic!("Expected IoError"),
        }
    }
}
