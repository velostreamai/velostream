//! File Data Reader Implementation

use crate::velostream::datasource::traits::DataReader;
use crate::velostream::datasource::types::SourceOffset;
use crate::velostream::datasource::{BatchConfig, BatchStrategy, EventTimeConfig};
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::time::Instant;
use tokio::time::{Duration, sleep};

use super::config::{FileFormat, FileSourceConfig};
use super::error::FileDataSourceError;

/// File reader that can read various file formats
pub struct FileReader {
    config: FileSourceConfig,
    batch_config: BatchConfig,
    event_time_config: Option<EventTimeConfig>,
    current_file: Option<BufReader<File>>,
    current_position: u64,
    line_number: usize,
    records_read: usize,
    finished: bool,
    file_list: Vec<String>,
    current_file_index: usize,
    eof_reached: bool,
    csv_headers: Option<Vec<String>>, // Store CSV column headers
    // State for adaptive batching
    adaptive_state: FileAdaptiveBatchState,
}

/// State tracking for adaptive batch sizing in file reading
#[derive(Debug, Clone)]
struct FileAdaptiveBatchState {
    current_size: usize,
    recent_latencies: Vec<Duration>,
    last_adjustment: Instant,
    bytes_per_record_estimate: Option<usize>,
}

impl FileAdaptiveBatchState {
    fn new(initial_size: usize) -> Self {
        Self {
            current_size: initial_size,
            recent_latencies: Vec::with_capacity(10),
            last_adjustment: Instant::now(),
            bytes_per_record_estimate: None,
        }
    }

    fn record_latency(&mut self, latency: Duration) {
        self.recent_latencies.push(latency);
        if self.recent_latencies.len() > 10 {
            self.recent_latencies.remove(0);
        }
    }

    fn average_latency(&self) -> Option<Duration> {
        if self.recent_latencies.is_empty() {
            None
        } else {
            let total: Duration = self.recent_latencies.iter().sum();
            Some(total / self.recent_latencies.len() as u32)
        }
    }
}

impl FileReader {
    /// Create a new file reader with configuration
    pub async fn new(config: FileSourceConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_batch_config(config, BatchConfig::default(), None).await
    }

    /// Create a new file reader with configuration and batch config
    pub async fn new_with_batch_config(
        config: FileSourceConfig,
        batch_config: BatchConfig,
        event_time_config: Option<EventTimeConfig>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let initial_size = match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => *size,
            BatchStrategy::AdaptiveSize { min_size, .. } => *min_size,
            BatchStrategy::LowLatency { max_batch_size, .. } => *max_batch_size,
            _ => 100,
        };

        let mut reader = Self {
            config,
            batch_config,
            event_time_config,
            current_file: None,
            current_position: 0,
            line_number: 0,
            records_read: 0,
            finished: false,
            file_list: Vec::new(),
            current_file_index: 0,
            eof_reached: false,
            csv_headers: None,
            adaptive_state: FileAdaptiveBatchState::new(initial_size),
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

        // Debug info: log current working directory and file path
        let current_dir = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        println!("DEBUG: FileReader attempting to open file");
        println!("DEBUG: Current working directory: {}", current_dir);
        println!("DEBUG: File path: {}", file_path);
        println!("DEBUG: File exists: {}", Path::new(file_path).exists());

        let file = File::open(file_path).map_err(|e| {
            let error_msg = format!(
                "Failed to open file '{}' from directory '{}': {}",
                file_path, current_dir, e
            );
            println!("DEBUG: {}", error_msg);
            FileDataSourceError::IoError(error_msg)
        })?;

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

    /// Parse a CSV line into a StreamRecord with proper RFC 4180 compliance
    fn parse_csv_line(&self, line: &str) -> Result<StreamRecord, Box<dyn Error + Send + Sync>> {
        let parsed_fields = self.parse_csv_fields(line)?;

        let mut fields = HashMap::new();

        // Use header names if available, otherwise fall back to column indices
        for (i, field_value) in parsed_fields.iter().enumerate() {
            let field_name = if let Some(headers) = &self.csv_headers {
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

        // Extract event_time if configured
        let event_time = self.extract_event_time_from_fields(&fields);

        Ok(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: self.current_position as i64,
            partition: 0,
            headers: HashMap::new(),
            event_time,
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
                if let Some(dt) = chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.naive_utc()) {
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

                    // Extract event_time if configured
                    let event_time = self.extract_event_time_from_fields(&fields);

                    let record = StreamRecord {
                        fields,
                        timestamp: chrono::Utc::now().timestamp_millis(),
                        offset: self.current_position as i64,
                        partition: 0,
                        headers: HashMap::new(),
                        event_time,
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
        if !self.batch_config.enable_batching {
            return self.read_single().await;
        }

        match &self.batch_config.strategy {
            BatchStrategy::FixedSize(size) => self.read_fixed_size(*size).await,
            BatchStrategy::TimeWindow(duration) => self.read_time_window(*duration).await,
            BatchStrategy::AdaptiveSize {
                min_size,
                max_size,
                target_latency,
            } => {
                self.read_adaptive(*min_size, *max_size, *target_latency)
                    .await
            }
            BatchStrategy::MemoryBased(max_bytes) => self.read_memory_based(*max_bytes).await,
            BatchStrategy::LowLatency {
                max_batch_size,
                max_wait_time,
                eager_processing,
            } => {
                self.read_low_latency(*max_batch_size, *max_wait_time, *eager_processing)
                    .await
            }
            BatchStrategy::MegaBatch { batch_size, .. } => {
                // High-throughput mega-batch processing (Phase 4 optimization)
                self.read_fixed_size(*batch_size).await
            }
        }
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
                )));
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

impl FileReader {
    /// Read a single record (when batching is disabled)
    async fn read_single(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        if let Some(record) = self.read_one_record().await? {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }

    /// Read fixed number of records with optimized batch I/O
    async fn read_fixed_size(
        &mut self,
        size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let batch_size = size.min(self.batch_config.max_batch_size);

        // Use optimized batch reading for different formats
        match self.config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader | FileFormat::JsonLines => {
                // Line-based formats can benefit from buffered batch reading
                self.read_lines_batch(batch_size).await
            }
            FileFormat::Json => {
                // JSON array format - fallback to single record reading
                self.read_fixed_size_fallback(batch_size).await
            }
        }
    }

    /// Optimized batch reading for line-based formats (CSV, JSONL)
    async fn read_lines_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(batch_size);

        // Calculate optimal read buffer size based on batch size and estimated line length
        let estimated_line_length = self.adaptive_state.bytes_per_record_estimate.unwrap_or(256);
        let buffer_size = (batch_size * estimated_line_length).clamp(8192, 1024 * 1024); // 8KB to 1MB

        let mut lines_buffer = Vec::with_capacity(batch_size);
        let mut total_bytes_read = 0;

        // Read multiple lines in one I/O operation
        while records.len() < batch_size {
            // Check limits
            if let Some(max) = self.config.max_records {
                if self.records_read >= max {
                    break;
                }
            }

            if self.finished && !self.config.watch_for_changes {
                break;
            }

            let reader = match self.current_file.as_mut() {
                Some(r) => r,
                None => break,
            };

            // Read a chunk of lines
            let chunk_buffer = String::with_capacity(buffer_size);
            let mut lines_in_chunk = 0;

            while lines_in_chunk < batch_size && chunk_buffer.len() < buffer_size {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        // EOF reached
                        self.eof_reached = true;
                        self.open_next_file().await?;
                        break;
                    }
                    Ok(bytes_read) => {
                        total_bytes_read += bytes_read;
                        self.line_number += 1;
                        lines_buffer.push(line);
                        lines_in_chunk += 1;
                    }
                    Err(e) => {
                        return Err(Box::new(FileDataSourceError::IoError(e.to_string())));
                    }
                }
            }

            // Process all lines in the chunk
            for line in lines_buffer.drain(..) {
                if records.len() >= batch_size {
                    break;
                }

                // Parse the line based on format
                match self.parse_line_to_record(line).await? {
                    Some(record) => {
                        records.push(record);
                        self.records_read += 1;
                    }
                    None => continue, // Skip invalid/empty lines
                }
            }

            // Update bytes per record estimate for adaptive batching
            if !records.is_empty() && total_bytes_read > 0 {
                let bytes_per_record = total_bytes_read / records.len();
                self.adaptive_state.bytes_per_record_estimate = Some(
                    self.adaptive_state
                        .bytes_per_record_estimate
                        .map(|avg| (avg + bytes_per_record) / 2)
                        .unwrap_or(bytes_per_record),
                );
            }

            if self.finished && !self.config.watch_for_changes {
                break;
            }

            // Check for new data if watching
            if self.eof_reached
                && records.is_empty()
                && self.config.watch_for_changes
                && !self.check_for_new_data().await?
            {
                break;
            }
        }

        Ok(records)
    }

    /// Fallback to single-record reading for complex formats
    async fn read_fixed_size_fallback(
        &mut self,
        batch_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            // Check limits
            if let Some(max) = self.config.max_records {
                if self.records_read >= max {
                    break;
                }
            }

            if self.finished && !self.config.watch_for_changes {
                break;
            }

            if let Some(record) = self.read_one_record().await? {
                records.push(record);
            } else {
                // No more data, check if we should wait for new data
                if self.config.watch_for_changes
                    && !self.finished
                    && self.check_for_new_data().await?
                {
                    continue;
                }
                break;
            }
        }

        Ok(records)
    }

    /// Read records within a time window
    async fn read_time_window(
        &mut self,
        duration: Duration,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::new();
        let start_time = Instant::now();

        while start_time.elapsed() < duration && records.len() < self.batch_config.max_batch_size {
            // Check record limits
            if let Some(max) = self.config.max_records {
                if self.records_read >= max {
                    break;
                }
            }

            if let Some(record) = self.read_one_record().await? {
                records.push(record);
            } else {
                // No more data available
                if self.config.watch_for_changes && !self.finished {
                    // Sleep briefly and check for new data
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if self.check_for_new_data().await? {
                        continue;
                    }
                }
                break;
            }
        }

        Ok(records)
    }

    /// Read records with adaptive batch sizing
    async fn read_adaptive(
        &mut self,
        min_size: usize,
        max_size: usize,
        target_latency: Duration,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let batch_start = Instant::now();
        let current_size = self
            .adaptive_state
            .current_size
            .clamp(min_size, max_size.min(self.batch_config.max_batch_size));

        // Read current adaptive batch size
        let records = self.read_fixed_size(current_size).await?;
        let batch_latency = batch_start.elapsed();

        // Record latency for adaptive adjustment
        self.adaptive_state.record_latency(batch_latency);

        // Adjust batch size every 10 seconds
        if self.adaptive_state.last_adjustment.elapsed() > Duration::from_secs(10) {
            if let Some(avg_latency) = self.adaptive_state.average_latency() {
                if avg_latency > target_latency && current_size > min_size {
                    // Too slow, reduce batch size
                    self.adaptive_state.current_size = (current_size as f64 * 0.8) as usize;
                    self.adaptive_state.current_size =
                        self.adaptive_state.current_size.max(min_size);
                } else if avg_latency < target_latency / 2 && current_size < max_size {
                    // Too fast, increase batch size
                    self.adaptive_state.current_size = (current_size as f64 * 1.2) as usize;
                    self.adaptive_state.current_size =
                        self.adaptive_state.current_size.min(max_size);
                }
            }
            self.adaptive_state.last_adjustment = Instant::now();
        }

        Ok(records)
    }

    /// Read records up to a memory limit (approximate)
    async fn read_memory_based(
        &mut self,
        max_bytes: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::new();
        let mut estimated_size = 0usize;

        while estimated_size < max_bytes && records.len() < self.batch_config.max_batch_size {
            // Check record limits
            if let Some(max) = self.config.max_records {
                if self.records_read >= max {
                    break;
                }
            }

            if let Some(record) = self.read_one_record().await? {
                // Estimate record size: 24 bytes overhead + field data
                let record_size = 24
                    + record
                        .fields
                        .iter()
                        .map(|(k, v)| k.len() + self.estimate_field_size(v))
                        .sum::<usize>();

                if estimated_size + record_size > max_bytes && !records.is_empty() {
                    // Would exceed memory limit, return current batch
                    break;
                }

                estimated_size += record_size;
                records.push(record);

                // Update adaptive state with bytes per record estimate
                self.adaptive_state.bytes_per_record_estimate = Some(
                    self.adaptive_state
                        .bytes_per_record_estimate
                        .map(|avg| (avg + record_size) / 2)
                        .unwrap_or(record_size),
                );
            } else {
                // No more data available
                if self.config.watch_for_changes
                    && !self.finished
                    && self.check_for_new_data().await?
                {
                    continue;
                }
                break;
            }
        }

        Ok(records)
    }

    /// Read records with low-latency optimization for file sources
    async fn read_low_latency(
        &mut self,
        max_batch_size: usize,
        max_wait_time: Duration,
        eager_processing: bool,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let batch_size = max_batch_size.min(self.batch_config.max_batch_size);
        let start_time = Instant::now();

        if eager_processing {
            // Eager processing: return immediately with first available record
            if let Some(record) = self.read_one_record().await? {
                return Ok(vec![record]);
            } else {
                // No data available, check for new data briefly if watching
                if self.config.watch_for_changes && !self.finished {
                    let brief_wait = std::cmp::min(max_wait_time, Duration::from_millis(1));
                    tokio::time::sleep(brief_wait).await;
                    if self.check_for_new_data().await? {
                        if let Some(record) = self.read_one_record().await? {
                            return Ok(vec![record]);
                        }
                    }
                }
                return Ok(vec![]);
            }
        }

        // Standard low-latency processing with small batches and tight timeouts
        let mut records = Vec::with_capacity(batch_size);

        while records.len() < batch_size && start_time.elapsed() < max_wait_time {
            // Check limits
            if let Some(max) = self.config.max_records {
                if self.records_read >= max {
                    break;
                }
            }

            if self.finished && !self.config.watch_for_changes {
                break;
            }

            if let Some(record) = self.read_one_record().await? {
                records.push(record);

                // In low-latency mode, prefer returning smaller batches quickly
                // Return immediately if we have any records and aggressive timing is requested
                if max_wait_time <= Duration::from_millis(5) && !records.is_empty() {
                    break;
                }
            } else {
                // No more data available
                if self.config.watch_for_changes && !self.finished {
                    // Brief check for new data
                    let remaining_time = max_wait_time.saturating_sub(start_time.elapsed());
                    if remaining_time > Duration::from_millis(0)
                        && self.check_for_new_data().await?
                    {
                        continue; // Try reading again
                    }
                }
                break;
            }
        }

        Ok(records)
    }

    /// Helper method to read a single record
    async fn read_one_record(
        &mut self,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        // Try to read a record based on format
        match self.config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => self.read_csv_record().await,
            FileFormat::JsonLines => self.read_jsonl_record().await,
            FileFormat::Json => {
                // For JSON arrays, we'd need different logic
                Err(Box::new(FileDataSourceError::UnsupportedFormat(
                    "JSON array format not yet implemented".to_string(),
                )))
            }
        }
    }

    /// Parse a single line into a StreamRecord based on format
    async fn parse_line_to_record(
        &mut self,
        line: String,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        // Skip empty lines
        if line.trim().is_empty() {
            return Ok(None);
        }

        match self.config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => self.parse_csv_line_batch(line).await,
            FileFormat::JsonLines => self.parse_jsonl_line_batch(line).await,
            FileFormat::Json => Err(Box::new(FileDataSourceError::UnsupportedFormat(
                "JSON array format not supported in batch line parsing".to_string(),
            ))),
        }
    }

    /// Parse a CSV line into a StreamRecord (for batch processing)
    async fn parse_csv_line_batch(
        &mut self,
        line: String,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(None);
        }

        // Parse CSV fields (simple split for now - could be enhanced with proper CSV parsing)
        let fields_raw: Vec<&str> = line.split(',').map(|f| f.trim()).collect();

        let mut fields = HashMap::new();

        // Handle headers
        if self.csv_headers.is_none() && self.config.format == FileFormat::Csv {
            // First line is headers
            self.csv_headers = Some(fields_raw.iter().map(|s| s.to_string()).collect());
            return Ok(None); // Skip header line
        }

        // Use headers if available, otherwise generate field names
        let headers = match &self.csv_headers {
            Some(h) => h.clone(),
            None => (0..fields_raw.len())
                .map(|i| format!("field_{}", i))
                .collect(),
        };

        // Create field map
        for (i, value) in fields_raw.iter().enumerate() {
            let field_name = headers
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("field_{}", i));
            let field_value = self.infer_field_type_simple(value);
            fields.insert(field_name, field_value);
        }

        // Extract event_time if configured
        let event_time = self.extract_event_time_from_fields(&fields);

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: self.current_position as i64,
            partition: 0,
            headers: HashMap::new(),
            event_time,
        };

        Ok(Some(record))
    }

    /// Parse a JSONL line into a StreamRecord (for batch processing)
    async fn parse_jsonl_line_batch(
        &mut self,
        line: String,
    ) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(None);
        }

        // Parse JSON
        let json_value: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Failed to parse JSON line: {}, error: {}", line, e);
                return Ok(None); // Skip invalid JSON
            }
        };

        // Convert JSON to fields map
        let mut fields = HashMap::new();
        if let serde_json::Value::Object(obj) = json_value {
            for (key, value) in obj {
                let field_value = self.json_value_to_field_value(&value);
                fields.insert(key, field_value);
            }
        }

        // Extract event_time if configured
        let event_time = self.extract_event_time_from_fields(&fields);

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: self.current_position as i64,
            partition: 0,
            headers: HashMap::new(),
            event_time,
        };

        Ok(Some(record))
    }

    /// Convert JSON value to FieldValue
    fn json_value_to_field_value(&self, value: &serde_json::Value) -> FieldValue {
        match value {
            serde_json::Value::Null => FieldValue::Null,
            serde_json::Value::Bool(b) => FieldValue::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    FieldValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    FieldValue::Float(f)
                } else {
                    FieldValue::String(n.to_string())
                }
            }
            serde_json::Value::String(s) => FieldValue::String(s.clone()),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                FieldValue::String(value.to_string()) // Serialize complex types as JSON strings
            }
        }
    }

    /// Infer field type from string value (simple version for batch processing)
    fn infer_field_type_simple(&self, value: &str) -> FieldValue {
        // Try parsing as different types
        if value.is_empty() {
            return FieldValue::Null;
        }

        // Try boolean
        match value.to_lowercase().as_str() {
            "true" | "yes" | "1" => return FieldValue::Boolean(true),
            "false" | "no" | "0" => return FieldValue::Boolean(false),
            _ => {}
        }

        // Try integer
        if let Ok(i) = value.parse::<i64>() {
            return FieldValue::Integer(i);
        }

        // Try float
        if let Ok(f) = value.parse::<f64>() {
            return FieldValue::Float(f);
        }

        // Default to string
        FieldValue::String(value.to_string())
    }

    /// Estimate memory size of a FieldValue
    fn estimate_field_size(&self, field: &FieldValue) -> usize {
        match field {
            FieldValue::String(s) => s.len(),
            FieldValue::Integer(_) => 8,
            FieldValue::Float(_) => 8,
            FieldValue::Boolean(_) => 1,
            FieldValue::ScaledInteger(_, _) => 16,
            FieldValue::Timestamp(_) => 16,
            FieldValue::Date(_) => 8,
            FieldValue::Decimal(_) => 16,
            FieldValue::Null => 0,
            FieldValue::Interval { .. } => 16,
            FieldValue::Array(arr) => {
                24 + arr
                    .iter()
                    .map(|v| self.estimate_field_size(v))
                    .sum::<usize>()
            }
            FieldValue::Map(map) => {
                24 + map
                    .iter()
                    .map(|(k, v)| k.len() + self.estimate_field_size(v))
                    .sum::<usize>()
            }
            FieldValue::Struct(s) => {
                24 + s
                    .iter()
                    .map(|(k, v)| k.len() + self.estimate_field_size(v))
                    .sum::<usize>()
            }
        }
    }
}
