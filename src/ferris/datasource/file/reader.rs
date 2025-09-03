//! File Data Reader Implementation

use crate::ferris::datasource::traits::DataReader;
use crate::ferris::datasource::types::SourceOffset;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use tokio::time::{sleep, Duration};

use super::config::{FileFormat, FileSourceConfig};
use super::error::FileDataSourceError;

/// File reader that can read various file formats
pub struct FileReader {
    config: FileSourceConfig,
    current_file: Option<BufReader<File>>,
    current_position: u64,
    line_number: usize,
    records_read: usize,
    finished: bool,
    file_list: Vec<String>,
    current_file_index: usize,
    eof_reached: bool,
    csv_headers: Option<Vec<String>>, // Store CSV column headers
}

impl FileReader {
    /// Create a new file reader with configuration
    pub async fn new(config: FileSourceConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut reader = Self {
            config,
            current_file: None,
            current_position: 0,
            line_number: 0,
            records_read: 0,
            finished: false,
            file_list: Vec::new(),
            current_file_index: 0,
            eof_reached: false,
            csv_headers: None,
        };

        reader.initialize_files().await?;
        reader.open_next_file().await?;

        Ok(reader)
    }

    /// Initialize the list of files to process
    async fn initialize_files(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = &self.config.path;

        if path.contains('*') || path.contains('?') {
            // Handle glob patterns
            self.file_list = self.expand_glob(path).await?;
        } else {
            // Single file
            self.file_list = vec![path.clone()];
        }

        if self.file_list.is_empty() {
            return Err(Box::new(FileDataSourceError::FileNotFound(format!(
                "No files found matching pattern: {}",
                path
            ))));
        }

        // Sort files for consistent ordering
        self.file_list.sort();

        Ok(())
    }

    /// Expand glob pattern to file list
    async fn expand_glob(
        &self,
        pattern: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        use std::fs;

        let path = Path::new(pattern);
        let parent = path
            .parent()
            .ok_or_else(|| FileDataSourceError::InvalidPath("Invalid glob pattern".to_string()))?;

        let file_name = path
            .file_name()
            .ok_or_else(|| FileDataSourceError::InvalidPath("Invalid glob pattern".to_string()))?;

        let mut files = Vec::new();

        // Simple glob implementation - in production, use the glob crate
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    if let Some(name) = entry_path.file_name() {
                        let name_str = name.to_string_lossy();
                        let pattern_str = file_name.to_string_lossy();

                        // Simple wildcard matching
                        if pattern_str.contains('*') {
                            let parts: Vec<&str> = pattern_str.split('*').collect();
                            if parts.len() == 2 {
                                let prefix = parts[0];
                                let suffix = parts[1];
                                if name_str.starts_with(prefix) && name_str.ends_with(suffix) {
                                    files.push(entry_path.to_string_lossy().to_string());
                                }
                            }
                        } else if name_str == pattern_str {
                            files.push(entry_path.to_string_lossy().to_string());
                        }
                    }
                }
            }
        }

        Ok(files)
    }

    /// Open the next file in the list
    async fn open_next_file(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.current_file_index >= self.file_list.len() {
            self.finished = true;
            return Ok(());
        }

        let file_path = &self.file_list[self.current_file_index];
        let file =
            File::open(file_path).map_err(|e| FileDataSourceError::IoError(e.to_string()))?;

        let mut reader = BufReader::new(file);

        // Skip lines if configured
        for _ in 0..self.config.skip_lines {
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
        }

        self.current_file = Some(reader);
        self.current_position = 0;
        self.line_number = self.config.skip_lines;

        // Reset headers for each new file
        self.csv_headers = None;

        Ok(())
    }

    /// Read a single record from CSV file
    async fn read_csv_record(
        &mut self,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        loop {
            let reader = match self.current_file.as_mut() {
                Some(r) => r,
                None => return Ok(None),
            };

            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    // End of file, mark EOF and try next file
                    self.eof_reached = true;
                    self.current_file_index += 1;
                    self.open_next_file().await?;

                    // If no more files, return None
                    if self.finished {
                        return Ok(None);
                    }
                    continue; // Try again with next file
                }
                Ok(_) => {
                    self.line_number += 1;

                    // Parse header line if configured and this is the first line
                    if self.config.csv_has_header && self.line_number == self.config.skip_lines + 1
                    {
                        // Parse headers and store them
                        if let Ok(headers) = self.parse_csv_fields(line.trim()) {
                            self.csv_headers = Some(headers);
                        }
                        continue; // Skip this line and try again
                    }

                    // Parse CSV line
                    let line_content = line.trim().to_string(); // Store line content

                    // Peek ahead to see if we're at EOF for has_more() accuracy
                    let current_pos = reader
                        .stream_position()
                        .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                    let mut peek_line = String::new();
                    match reader.read_line(&mut peek_line) {
                        Ok(0) => {
                            // Next read will be EOF
                            self.eof_reached = true;
                        }
                        Ok(_) => {
                            // Rewind to where we were
                            reader
                                .seek(SeekFrom::Start(current_pos))
                                .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                        }
                        Err(_) => {
                            // Error peeking, rewind and continue
                            reader
                                .seek(SeekFrom::Start(current_pos))
                                .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                        }
                    }

                    // Now parse the CSV line with the stored content
                    let record = self.parse_csv_line(&line_content)?;
                    self.records_read += 1;

                    return Ok(Some(record));
                }
                Err(e) => return Err(Box::new(FileDataSourceError::IoError(e.to_string()))),
            }
        }
    }

    /// Parse a CSV line into a StreamRecord with proper RFC 4180 compliance
    fn parse_csv_line(&self, line: &str) -> Result<StreamRecord, Box<dyn Error + Send + Sync>> {
        let parsed_fields = self.parse_csv_fields(line)?;

        let mut fields = HashMap::new();

        // Use header names if available, otherwise fall back to column indices
        for (i, field_value) in parsed_fields.iter().enumerate() {
            let field_name = if let Some(ref headers) = self.csv_headers {
                headers
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("column_{}", i))
            } else {
                format!("column_{}", i)
            };

            // Smart type inference with financial precision detection
            let field_val = self.infer_field_type(&field_name, field_value);
            fields.insert(field_name, field_val);
        }

        Ok(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: self.current_position as i64,
            partition: 0,
            headers: HashMap::new(),
        })
    }

    /// RFC 4180 compliant CSV field parsing
    fn parse_csv_fields(&self, line: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                // Handle quote character
                c if c == self.config.csv_quote => {
                    if in_quotes {
                        // Check for escaped quote (double quote)
                        if chars.peek() == Some(&self.config.csv_quote) {
                            current_field.push(self.config.csv_quote);
                            chars.next(); // Consume the second quote
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                // Handle delimiter
                c if c == self.config.csv_delimiter && !in_quotes => {
                    fields.push(current_field.trim().to_string());
                    current_field.clear();
                }
                // Regular character
                c => {
                    current_field.push(c);
                }
            }
        }

        // Don't forget the last field
        fields.push(current_field.trim().to_string());

        Ok(fields)
    }

    /// Smart type inference with financial precision detection
    fn infer_field_type(&self, field_name: &str, field_value: &str) -> FieldValue {
        if field_value.is_empty() {
            return FieldValue::Null;
        }

        // Financial amount detection
        if self.is_financial_field(field_name) {
            if let Ok(amount) = field_value.parse::<f64>() {
                // Convert to ScaledInteger with 4 decimal places for financial precision
                let scaled_amount = (amount * 10000.0).round() as i64;
                return FieldValue::ScaledInteger(scaled_amount, 4);
            }
        }

        // Timestamp detection (Unix timestamp)
        if field_name.contains("timestamp") {
            if let Ok(ts) = field_value.parse::<i64>() {
                // Convert Unix timestamp to NaiveDateTime
                if let Some(dt) = chrono::NaiveDateTime::from_timestamp_opt(ts, 0) {
                    return FieldValue::Timestamp(dt);
                }
                // Fallback to integer if conversion fails
                return FieldValue::Integer(ts);
            }
        }

        // Integer detection
        if let Ok(i) = field_value.parse::<i64>() {
            return FieldValue::Integer(i);
        }

        // Float detection
        if let Ok(f) = field_value.parse::<f64>() {
            return FieldValue::Float(f);
        }

        // Boolean detection
        match field_value.to_lowercase().as_str() {
            "true" | "yes" | "1" => FieldValue::Boolean(true),
            "false" | "no" | "0" => FieldValue::Boolean(false),
            _ => FieldValue::String(field_value.to_string()),
        }
    }

    /// Detect if a field contains financial/monetary data
    fn is_financial_field(&self, field_name: &str) -> bool {
        let financial_patterns = [
            "amount", "price", "cost", "fee", "total", "balance", "value", "payment", "charge",
            "rate", "salary",
        ];

        let lower_name = field_name.to_lowercase();
        financial_patterns
            .iter()
            .any(|pattern| lower_name.contains(pattern))
    }

    /// Read a single record from JSON Lines file
    async fn read_jsonl_record(
        &mut self,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        loop {
            let reader = match self.current_file.as_mut() {
                Some(r) => r,
                None => return Ok(None),
            };

            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    // End of file, mark EOF and try next file
                    self.eof_reached = true;
                    self.current_file_index += 1;
                    self.open_next_file().await?;

                    // If no more files, return None
                    if self.finished {
                        return Ok(None);
                    }
                    continue; // Try again with next file
                }
                Ok(_) => {
                    self.line_number += 1;

                    let line = line.trim();
                    if line.is_empty() {
                        // Skip empty lines
                        continue; // Try again with next line
                    }

                    // Parse JSON line
                    let json_value: serde_json::Value = serde_json::from_str(line)
                        .map_err(|e| FileDataSourceError::JsonParseError(e.to_string()))?;

                    // Convert JSON to FieldValue map
                    let mut fields = HashMap::new();
                    match json_value {
                        serde_json::Value::Object(obj) => {
                            for (key, val) in obj {
                                let field_val = match val {
                                    serde_json::Value::String(s) => FieldValue::String(s),
                                    serde_json::Value::Number(n) if n.is_i64() => {
                                        FieldValue::Integer(n.as_i64().unwrap())
                                    }
                                    serde_json::Value::Number(n) => {
                                        FieldValue::Float(n.as_f64().unwrap_or(0.0))
                                    }
                                    serde_json::Value::Bool(b) => FieldValue::Boolean(b),
                                    serde_json::Value::Null => FieldValue::Null,
                                    _ => FieldValue::String(val.to_string()),
                                };
                                fields.insert(key, field_val);
                            }
                        }
                        _ => {
                            // If not an object, store the entire value as a "data" field
                            let field_val = match json_value {
                                serde_json::Value::String(s) => FieldValue::String(s),
                                serde_json::Value::Number(n) if n.is_i64() => {
                                    FieldValue::Integer(n.as_i64().unwrap())
                                }
                                serde_json::Value::Number(n) => {
                                    FieldValue::Float(n.as_f64().unwrap_or(0.0))
                                }
                                serde_json::Value::Bool(b) => FieldValue::Boolean(b),
                                serde_json::Value::Null => FieldValue::Null,
                                _ => FieldValue::String(json_value.to_string()),
                            };
                            fields.insert("data".to_string(), field_val);
                        }
                    }

                    let record = StreamRecord {
                        fields,
                        timestamp: chrono::Utc::now().timestamp_millis(),
                        offset: self.current_position as i64,
                        partition: 0,
                        headers: HashMap::new(),
                    };

                    self.records_read += 1;
                    self.eof_reached = false; // We successfully read a record

                    return Ok(Some(record));
                }
                Err(e) => return Err(Box::new(FileDataSourceError::IoError(e.to_string()))),
            }
        }
    }

    /// Check for new data in watched files
    async fn check_for_new_data(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        if !self.config.watch_for_changes {
            return Ok(false);
        }

        // For file watching, we would typically:
        // 1. Check if current file has grown
        // 2. Check for new files matching the pattern
        // 3. Re-scan the directory for changes

        // Simple implementation: just check if file has more data
        if let Some(reader) = self.current_file.as_mut() {
            let mut test_line = String::new();
            match reader.read_line(&mut test_line) {
                Ok(0) => {
                    // No new data in current file
                    // Check if there are new files
                    let old_count = self.file_list.len();
                    self.initialize_files().await?;

                    if self.file_list.len() > old_count {
                        // New files detected
                        return Ok(true);
                    }

                    // Wait before checking again
                    if let Some(interval) = self.config.polling_interval_ms {
                        sleep(Duration::from_millis(interval)).await;
                    }

                    Ok(false)
                }
                Ok(_) => {
                    // New data available, seek back
                    let pos = reader.stream_position()? - test_line.len() as u64;
                    reader.seek(SeekFrom::Start(pos))?;
                    Ok(true)
                }
                Err(e) => Err(Box::new(FileDataSourceError::IoError(e.to_string()))),
            }
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl DataReader for FileReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        // Use configured batch size from the source config
        let batch_size = self
            .config
            .batch_config
            .as_ref()
            .and_then(|bc| match &bc.strategy {
                crate::ferris::datasource::config::BatchStrategy::FixedSize(size) => Some(*size),
                _ => None,
            })
            .unwrap_or(100); // Default batch size for files

        let mut records = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            // Check if we've reached the maximum number of records
            if let Some(max) = self.config.max_records {
                if self.records_read >= max {
                    break;
                }
            }

            // If finished and not watching, break
            if self.finished && !self.config.watch_for_changes {
                break;
            }

            // Try to read a record based on format
            let result = match self.config.format {
                FileFormat::Csv | FileFormat::CsvNoHeader => self.read_csv_record().await,
                FileFormat::JsonLines => self.read_jsonl_record().await,
                FileFormat::Json => {
                    // For JSON arrays, we'd need different logic
                    let err: Box<dyn Error + Send + Sync> =
                        Box::new(FileDataSourceError::UnsupportedFormat(
                            "JSON array format not yet implemented".to_string(),
                        ));
                    return Err(err);
                }
            };

            match result {
                Ok(Some(record)) => {
                    records.push(record);
                }
                Ok(None) => {
                    // No more data, check if we should wait for new data
                    if self.config.watch_for_changes && !self.finished {
                        if self.check_for_new_data().await? {
                            // New data available, try reading again by continuing loop
                            continue;
                        }
                    }
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // For file sources, commit is typically a no-op
        // Could be used to checkpoint reading position
        Ok(())
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Extract position from SourceOffset
        let position = match offset {
            SourceOffset::File { byte_offset, .. } => byte_offset,
            _ => {
                return Err(Box::new(FileDataSourceError::InvalidPath(
                    "Invalid offset type for file source".to_string(),
                )))
            }
        };

        if let Some(reader) = self.current_file.as_mut() {
            reader
                .seek(SeekFrom::Start(position))
                .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
            self.current_position = position;
        }

        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // If we have a max_records limit, check that first
        if let Some(max) = self.config.max_records {
            return Ok(self.records_read < max);
        }

        // If finished and not watching, no more data
        if self.finished && !self.config.watch_for_changes {
            return Ok(false);
        }

        // If we're finished but watching, we might get more data
        if self.finished && self.config.watch_for_changes {
            return Ok(true);
        }

        // If we've reached EOF on the current file and there's only one file,
        // and it's not being watched, then we have no more data
        if self.eof_reached && self.file_list.len() == 1 && !self.config.watch_for_changes {
            return Ok(false);
        }

        // If we've reached EOF but there are more files, we have more data
        if self.eof_reached && self.current_file_index < self.file_list.len() - 1 {
            return Ok(true);
        }

        // If not finished and not at EOF, we likely have more data
        Ok(!self.eof_reached || self.config.watch_for_changes)
    }
}
