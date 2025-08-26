//! File Data Reader Implementation

use crate::ferris::sql::datasource::traits::DataReader;
use crate::ferris::sql::datasource::types::SourceOffset;
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
            return Err(Box::new(FileDataSourceError::FileNotFound(
                format!("No files found matching pattern: {}", path)
            )));
        }
        
        // Sort files for consistent ordering
        self.file_list.sort();
        
        Ok(())
    }
    
    /// Expand glob pattern to file list
    async fn expand_glob(&self, pattern: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        use std::fs;
        
        let path = Path::new(pattern);
        let parent = path.parent().ok_or_else(|| {
            FileDataSourceError::InvalidPath("Invalid glob pattern".to_string())
        })?;
        
        let file_name = path.file_name().ok_or_else(|| {
            FileDataSourceError::InvalidPath("Invalid glob pattern".to_string())
        })?;
        
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
        let file = File::open(file_path)
            .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
        
        let mut reader = BufReader::new(file);
        
        // Skip lines if configured
        for _ in 0..self.config.skip_lines {
            let mut line = String::new();
            reader.read_line(&mut line)
                .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
        }
        
        self.current_file = Some(reader);
        self.current_position = 0;
        self.line_number = self.config.skip_lines;
        
        Ok(())
    }
    
    /// Read a single record from CSV file
    async fn read_csv_record(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
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
                    
                    // Skip header line if configured and this is the first line
                    if self.config.csv_has_header && self.line_number == self.config.skip_lines + 1 {
                        continue; // Skip this line and try again
                    }
                    
                    // Parse CSV line  
                    let line_content = line.trim().to_string(); // Store line content
                    
                    // Peek ahead to see if we're at EOF for has_more() accuracy
                    let current_pos = reader.stream_position().map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                    let mut peek_line = String::new();
                    match reader.read_line(&mut peek_line) {
                        Ok(0) => {
                            // Next read will be EOF
                            self.eof_reached = true;
                        }
                        Ok(_) => {
                            // Rewind to where we were
                            reader.seek(SeekFrom::Start(current_pos)).map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                        }
                        Err(_) => {
                            // Error peeking, rewind and continue
                            reader.seek(SeekFrom::Start(current_pos)).map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
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
    
    /// Parse a CSV line into a StreamRecord
    fn parse_csv_line(&self, line: &str) -> Result<StreamRecord, Box<dyn Error + Send + Sync>> {
        // Simple CSV parsing - in production, use csv crate
        let fields: Vec<&str> = line.split(self.config.csv_delimiter).collect();
        
        let mut data = HashMap::new();
        for (i, field) in fields.iter().enumerate() {
            let key = format!("column_{}", i);
            data.insert(key, serde_json::Value::String(field.trim_matches(self.config.csv_quote).to_string()));
        }
        
        // Convert JSON values to FieldValue
        let mut fields = HashMap::new();
        for (key, json_val) in data {
            let field_val = match json_val {
                serde_json::Value::String(s) => FieldValue::String(s),
                serde_json::Value::Number(n) if n.is_i64() => FieldValue::Integer(n.as_i64().unwrap()),
                serde_json::Value::Number(n) => FieldValue::Float(n.as_f64().unwrap_or(0.0)),
                serde_json::Value::Bool(b) => FieldValue::Boolean(b),
                serde_json::Value::Null => FieldValue::Null,
                _ => FieldValue::String(json_val.to_string()),
            };
            fields.insert(key, field_val);
        }

        Ok(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: self.current_position as i64,
            partition: 0,
            headers: HashMap::new(),
        })
    }
    
    /// Read a single record from JSON Lines file
    async fn read_jsonl_record(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
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
                                serde_json::Value::Number(n) if n.is_i64() => FieldValue::Integer(n.as_i64().unwrap()),
                                serde_json::Value::Number(n) => FieldValue::Float(n.as_f64().unwrap_or(0.0)),
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
                            serde_json::Value::Number(n) if n.is_i64() => FieldValue::Integer(n.as_i64().unwrap()),
                            serde_json::Value::Number(n) => FieldValue::Float(n.as_f64().unwrap_or(0.0)),
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
    async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        loop {
            // Check if we've reached the maximum number of records
            if let Some(max) = self.config.max_records {
                if self.records_read >= max {
                    return Ok(None);
                }
            }
            
            // If finished and not watching, return None
            if self.finished && !self.config.watch_for_changes {
                return Ok(None);
            }
            
            // Try to read a record based on format
            let result = match self.config.format {
                FileFormat::Csv | FileFormat::CsvNoHeader => {
                    self.read_csv_record().await
                }
                FileFormat::JsonLines => {
                    self.read_jsonl_record().await
                }
                FileFormat::Json => {
                    // For JSON arrays, we'd need different logic
                    let err: Box<dyn Error + Send + Sync> = Box::new(FileDataSourceError::UnsupportedFormat(
                        "JSON array format not yet implemented".to_string()
                    ));
                    return Err(err);
                }
            };
            
            match result {
                Ok(Some(record)) => return Ok(Some(record)),
                Ok(None) => {
                    // No more data, check if we should wait for new data
                    if self.config.watch_for_changes && !self.finished {
                        if self.check_for_new_data().await? {
                            // New data available, try reading again by continuing loop
                            continue;
                        }
                    }
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn read_batch(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(max_size);
        
        for _ in 0..max_size {
            match self.read().await? {
                Some(record) => records.push(record),
                None => break,
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
            _ => return Err(Box::new(FileDataSourceError::InvalidPath(
                "Invalid offset type for file source".to_string()
            ))),
        };
        
        if let Some(reader) = self.current_file.as_mut() {
            reader.seek(SeekFrom::Start(position))
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