//! Memory-Mapped File Data Reader Implementation
//!
//! High-throughput file reader using `memmap2::Mmap` for zero-copy access.
//! Designed for batch workloads like the 1BRC challenge where large files
//! need to be processed efficiently.

use crate::velostream::datasource::traits::DataReader;
use crate::velostream::datasource::types::SourceOffset;
use crate::velostream::datasource::{BatchConfig, BatchStrategy, EventTimeConfig};
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use memmap2::Mmap;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;

use super::config::{FileFormat, FileSourceConfig};
use super::error::FileDataSourceError;

/// Memory-mapped file reader for high-throughput batch processing.
///
/// Uses `memmap2::Mmap` for zero-copy file access, scanning bytes directly
/// from the memory-mapped region instead of buffered line reads.
pub struct FileMmapReader {
    config: FileSourceConfig,
    batch_config: BatchConfig,
    event_time_config: Option<EventTimeConfig>,
    mmap: Option<Mmap>,
    position: usize,
    line_number: usize,
    records_read: usize,
    finished: bool,
    file_list: Vec<String>,
    current_file_index: usize,
    csv_headers: Option<Vec<String>>,
}

impl FileMmapReader {
    /// Create a new mmap reader with configuration and batch config
    pub async fn new(
        config: FileSourceConfig,
        batch_config: BatchConfig,
        event_time_config: Option<EventTimeConfig>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut reader = Self {
            config,
            batch_config,
            event_time_config,
            mmap: None,
            position: 0,
            line_number: 0,
            records_read: 0,
            finished: false,
            file_list: Vec::new(),
            current_file_index: 0,
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
            self.file_list = self.expand_glob(path).await?;
        } else {
            self.file_list = vec![path.clone()];
        }

        if self.file_list.is_empty() {
            return Err(Box::new(FileDataSourceError::FileNotFound(format!(
                "No files found matching pattern: {}",
                path
            ))));
        }

        self.file_list.sort();
        Ok(())
    }

    /// Expand glob pattern to file list
    async fn expand_glob(
        &self,
        pattern: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let path = Path::new(pattern);
        let parent = path
            .parent()
            .ok_or_else(|| FileDataSourceError::InvalidPath("Invalid glob pattern".to_string()))?;

        let file_name = path
            .file_name()
            .ok_or_else(|| FileDataSourceError::InvalidPath("Invalid glob pattern".to_string()))?;

        let mut files = Vec::new();

        if let Ok(entries) = std::fs::read_dir(parent) {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_file()
                    && let Some(name) = entry_path.file_name()
                {
                    let name_str = name.to_string_lossy();
                    let pattern_str = file_name.to_string_lossy();

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

        Ok(files)
    }

    /// Open the next file in the list using mmap
    async fn open_next_file(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            if self.current_file_index >= self.file_list.len() {
                self.finished = true;
                return Ok(());
            }

            let file_path = &self.file_list[self.current_file_index];

            let file = File::open(file_path).map_err(|e| {
                let current_dir = std::env::current_dir()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| "unknown".to_string());
                let error_msg = format!(
                    "Failed to open file '{}' from directory '{}': {}",
                    file_path, current_dir, e
                );
                log::error!("{}", error_msg);
                FileDataSourceError::IoError(error_msg)
            })?;

            let metadata = file.metadata().map_err(|e| {
                FileDataSourceError::IoError(format!("Failed to get file metadata: {}", e))
            })?;

            if metadata.len() == 0 {
                // Empty file — skip to next
                self.current_file_index += 1;
                continue;
            }

            // Safety: the file is opened read-only and we hold the File handle
            let mmap = unsafe {
                Mmap::map(&file).map_err(|e| {
                    FileDataSourceError::IoError(format!(
                        "Failed to mmap file '{}': {}",
                        file_path, e
                    ))
                })?
            };

            self.mmap = Some(mmap);
            self.position = 0;
            self.line_number = 0;
            self.csv_headers = None;

            // Skip configured lines
            for _ in 0..self.config.skip_lines {
                self.skip_line();
            }

            // For CSV with headers, parse the header line
            if self.config.csv_has_header
                && self.config.format == FileFormat::Csv
                && let Some(header_line) = self.next_line().map(|s| s.to_string())
                && let Ok(headers) = self.parse_csv_fields(&header_line)
            {
                self.csv_headers = Some(headers);
            }

            return Ok(());
        }
    }

    /// Skip one line in the mmap (advance position past the next newline)
    fn skip_line(&mut self) {
        if let Some(mmap) = &self.mmap {
            let data = &mmap[self.position..];
            match data.iter().position(|&b| b == b'\n') {
                Some(offset) => {
                    self.position += offset + 1;
                    self.line_number += 1;
                }
                None => {
                    self.position = mmap.len();
                }
            }
        }
    }

    /// Read the next line from the mmap as a &str, advancing position.
    /// Returns None if at end of mmap.
    fn next_line(&mut self) -> Option<&str> {
        let mmap = self.mmap.as_ref()?;
        if self.position >= mmap.len() {
            return None;
        }

        let remaining = &mmap[self.position..];
        let line_end = remaining
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(remaining.len());

        let line_bytes = &remaining[..line_end];
        self.position += line_end + 1; // +1 to skip the newline
        self.line_number += 1;

        // Handle \r\n line endings
        let line_bytes = if line_bytes.last() == Some(&b'\r') {
            &line_bytes[..line_bytes.len() - 1]
        } else {
            line_bytes
        };

        std::str::from_utf8(line_bytes).ok()
    }

    /// Check if mmap has more data at current position
    fn mmap_has_more(&self) -> bool {
        if let Some(mmap) = &self.mmap {
            self.position < mmap.len()
        } else {
            false
        }
    }

    /// Read a batch of records from the current mmap
    fn read_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(batch_size);

        while records.len() < batch_size {
            // Check max_records limit
            if let Some(max) = self.config.max_records
                && self.records_read >= max
            {
                self.finished = true;
                break;
            }

            // Try to read next line from current mmap
            // We need to use raw pointer tricks to satisfy the borrow checker
            // since next_line borrows self mutably but returns a reference to mmap data
            let line = {
                let mmap = match self.mmap.as_ref() {
                    Some(m) => m,
                    None => break,
                };
                if self.position >= mmap.len() {
                    // End of current file, try next
                    None
                } else {
                    let remaining = &mmap[self.position..];
                    let line_end = remaining
                        .iter()
                        .position(|&b| b == b'\n')
                        .unwrap_or(remaining.len());

                    let line_bytes = &remaining[..line_end];
                    self.position += line_end + 1;
                    self.line_number += 1;

                    // Handle \r\n
                    let line_bytes = if line_bytes.last() == Some(&b'\r') {
                        &line_bytes[..line_bytes.len() - 1]
                    } else {
                        line_bytes
                    };

                    match std::str::from_utf8(line_bytes) {
                        Ok(s) if !s.is_empty() => Some(s.to_string()),
                        _ => Some(String::new()), // empty line, will be skipped
                    }
                }
            };

            match line {
                Some(line) if !line.is_empty() => match self.parse_line_to_record(&line)? {
                    Some(record) => {
                        records.push(record);
                        self.records_read += 1;
                    }
                    None => continue,
                },
                Some(_) => continue, // empty line
                None => {
                    // End of current file, try next
                    self.current_file_index += 1;
                    // Use a block to scope the async call
                    // Since we can't call async from sync, mark as needing file switch
                    self.mmap = None;
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Parse a line into a StreamRecord based on file format
    fn parse_line_to_record(
        &self,
        line: &str,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        match self.config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => self.parse_csv_line(line),
            FileFormat::JsonLines => self.parse_jsonl_line(line),
            FileFormat::Json => Err(Box::new(FileDataSourceError::UnsupportedFormat(
                "JSON array format not supported in mmap reader".to_string(),
            ))),
        }
    }

    /// Parse a CSV line into a StreamRecord
    fn parse_csv_line(
        &self,
        line: &str,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let parsed_fields = self.parse_csv_fields(line)?;

        let mut fields = HashMap::new();
        for (i, field_value) in parsed_fields.iter().enumerate() {
            let field_name = if let Some(headers) = &self.csv_headers {
                headers
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("column_{}", i))
            } else {
                format!("column_{}", i)
            };

            let field_val = self.infer_field_type(field_value);
            fields.insert(field_name, field_val);
        }

        let event_time = self.extract_event_time_from_fields(&fields);

        Ok(Some(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: self.position as i64,
            partition: 0,
            headers: HashMap::new(),
            event_time,
            topic: None,
            key: None,
        }))
    }

    /// Parse a JSONL line into a StreamRecord
    fn parse_jsonl_line(
        &self,
        line: &str,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let json_value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| FileDataSourceError::JsonParseError(e.to_string()))?;

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
                fields.insert(
                    "data".to_string(),
                    FieldValue::String(json_value.to_string()),
                );
            }
        }

        let event_time = self.extract_event_time_from_fields(&fields);

        Ok(Some(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: self.position as i64,
            partition: 0,
            headers: HashMap::new(),
            event_time,
            topic: None,
            key: None,
        }))
    }

    /// RFC 4180 compliant CSV field parsing
    fn parse_csv_fields(&self, line: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                c if c == self.config.csv_quote => {
                    if in_quotes {
                        if chars.peek() == Some(&self.config.csv_quote) {
                            current_field.push(self.config.csv_quote);
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                c if c == self.config.csv_delimiter && !in_quotes => {
                    fields.push(current_field.trim().to_string());
                    current_field.clear();
                }
                c => {
                    current_field.push(c);
                }
            }
        }

        fields.push(current_field.trim().to_string());
        Ok(fields)
    }

    /// Infer field type from string value
    fn infer_field_type(&self, value: &str) -> FieldValue {
        if value.is_empty() {
            return FieldValue::Null;
        }

        if let Ok(i) = value.parse::<i64>() {
            return FieldValue::Integer(i);
        }

        if let Ok(f) = value.parse::<f64>() {
            return FieldValue::Float(f);
        }

        match value.to_lowercase().as_str() {
            "true" | "yes" => return FieldValue::Boolean(true),
            "false" | "no" => return FieldValue::Boolean(false),
            _ => {}
        }

        FieldValue::String(value.to_string())
    }

    /// Extract event_time from fields if configured
    fn extract_event_time_from_fields(
        &self,
        fields: &HashMap<String, FieldValue>,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        if let Some(config) = &self.event_time_config {
            use crate::velostream::datasource::extract_event_time;
            match extract_event_time(fields, config) {
                Ok(dt) => Some(dt),
                Err(e) => {
                    log::warn!("Failed to extract event_time: {}. Falling back to None", e);
                    None
                }
            }
        } else {
            None
        }
    }
}

#[async_trait]
impl DataReader for FileMmapReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        if self.finished {
            return Ok(vec![]);
        }

        let batch_size = match &self.batch_config.strategy {
            BatchStrategy::FixedSize(size) => *size,
            BatchStrategy::MegaBatch { batch_size, .. } => *batch_size,
            _ => 1000,
        };
        let batch_size = batch_size.min(self.batch_config.max_batch_size);

        loop {
            let records = self.read_batch(batch_size)?;

            if !records.is_empty() {
                return Ok(records);
            }

            // If mmap is None, we need to open next file
            if self.mmap.is_none() {
                self.open_next_file().await?;
                if self.finished {
                    return Ok(vec![]);
                }
                continue;
            }

            // No more data in current file and no switch needed
            self.finished = true;
            return Ok(vec![]);
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // No-op for mmap file sources
        Ok(())
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>> {
        let position = match offset {
            SourceOffset::File { byte_offset, .. } => byte_offset,
            _ => {
                return Err(Box::new(FileDataSourceError::InvalidPath(
                    "Invalid offset type for file source".to_string(),
                )));
            }
        };

        self.position = position as usize;
        self.finished = false; // Reset finished state on seek
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        if let Some(max) = self.config.max_records
            && self.records_read >= max
        {
            return Ok(false);
        }

        if self.finished {
            return Ok(false);
        }

        // Check if current mmap has more data
        if self.mmap_has_more() {
            return Ok(true);
        }

        // Check if there are more files to process
        Ok(self.current_file_index + 1 < self.file_list.len())
    }
}
