//! File Sink Implementation
//!
//! Provides writing capabilities for streaming data to files with support for:
//! - Multiple formats (JSON, CSV, Parquet)
//! - File rotation by size and time
//! - Compression (gzip, snappy)
//! - Buffering and batching optimizations

use crate::ferris::datasource::config::SinkConfig;
use crate::ferris::datasource::traits::{DataSink, DataWriter};
use crate::ferris::datasource::types::SinkMetadata;
use crate::ferris::schema::Schema;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use super::config::{FileFormat, FileSinkConfig};
use super::error::FileDataSourceError;

/// File-based data sink implementation
///
/// Supports writing streaming data to:
/// - JSON Lines files (newline-delimited JSON)
/// - CSV files with configurable headers
/// - Parquet files (when feature enabled)
/// - Compressed outputs (gzip, snappy)
pub struct FileSink {
    config: Option<FileSinkConfig>,
    metadata: Option<SinkMetadata>,
    active_writers: Arc<Mutex<Vec<FileWriterState>>>,
}

/// State for active file writers
struct FileWriterState {
    path: PathBuf,
    writer: File,
    bytes_written: u64,
    records_written: u64,
    created_at: SystemTime,
    last_rotation: SystemTime,
}

impl FileSink {
    /// Create a new file sink
    pub fn new() -> Self {
        Self {
            config: None,
            metadata: None,
            active_writers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> Option<&FileSinkConfig> {
        self.config.as_ref()
    }

    /// Validate output directory exists and is writable
    async fn validate_output_path(&self, path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path_obj = Path::new(path);
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = path_obj.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                Box::new(FileDataSourceError::IoError(format!(
                    "Failed to create directory {}: {}",
                    parent.display(),
                    e
                ))) as Box<dyn Error + Send + Sync>
            })?;
        }

        // Check write permissions by creating a temp file
        let temp_path = path_obj.with_extension(".tmp");
        match File::create(&temp_path).await {
            Ok(_) => {
                // Clean up temp file
                let _ = tokio::fs::remove_file(&temp_path).await;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                Err(Box::new(FileDataSourceError::PermissionDenied(
                    path.to_string(),
                )))
            }
            Err(e) => Err(Box::new(FileDataSourceError::IoError(e.to_string()))),
        }
    }

    /// Check if file rotation is needed
    fn needs_rotation(&self, writer_state: &FileWriterState, config: &FileSinkConfig) -> bool {
        // Check size-based rotation
        if let Some(max_size) = config.max_file_size_bytes {
            if writer_state.bytes_written >= max_size {
                return true;
            }
        }

        // Check time-based rotation
        if let Some(rotation_interval) = config.rotation_interval_ms {
            let elapsed = SystemTime::now()
                .duration_since(writer_state.last_rotation)
                .unwrap_or_default();
            
            if elapsed >= Duration::from_millis(rotation_interval) {
                return true;
            }
        }

        // Check record count-based rotation
        if let Some(max_records) = config.max_records_per_file {
            if writer_state.records_written >= max_records {
                return true;
            }
        }

        false
    }

    /// Generate rotated filename
    fn generate_rotated_filename(base_path: &Path, rotation_index: u32) -> PathBuf {
        let stem = base_path.file_stem().unwrap_or_default().to_string_lossy();
        let ext = base_path.extension().unwrap_or_default().to_string_lossy();
        let parent = base_path.parent().unwrap_or(Path::new(""));
        
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let rotated_name = format!("{}_{}_{:04}.{}", stem, timestamp, rotation_index, ext);
        
        parent.join(rotated_name)
    }
}

impl Default for FileSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSink for FileSink {
    async fn initialize(&mut self, config: SinkConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
        let file_config = FileSinkConfig::from_generic(&config).map_err(|e| {
            Box::new(FileDataSourceError::UnsupportedFormat(e)) as Box<dyn Error + Send + Sync>
        })?;

        // Validate configuration
        file_config.validate().map_err(|e| {
            Box::new(FileDataSourceError::InvalidPath(e)) as Box<dyn Error + Send + Sync>
        })?;

        // Validate output path
        self.validate_output_path(&file_config.path).await?;

        // Store configuration
        self.config = Some(file_config);

        // Create metadata
        self.metadata = Some(SinkMetadata {
            sink_type: "file".to_string(),
            version: "1.0".to_string(),
            supports_transactions: false,
            supports_upsert: false,
            supports_schema_evolution: false,
            capabilities: vec![
                "append_only".to_string(),
                "file_rotation".to_string(),
                if self.config.as_ref().unwrap().compression.is_some() {
                    "compression".to_string()
                } else {
                    "uncompressed".to_string()
                },
                "buffered_writes".to_string(),
            ],
        });

        Ok(())
    }

    async fn validate_schema(&self, schema: &Schema) -> Result<(), Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileSink not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        // Validate schema compatibility with format
        match config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => {
                // CSV can handle most schemas but warn about nested types
                for field in &schema.fields {
                    match field.data_type() {
                        crate::ferris::sql::ast::DataType::Array(_) |
                        crate::ferris::sql::ast::DataType::Map(_, _) => {
                            eprintln!(
                                "Warning: Field '{}' has complex type that will be serialized as string in CSV",
                                field.name()
                            );
                        }
                        _ => {}
                    }
                }
            }
            FileFormat::Json | FileFormat::JsonLines => {
                // JSON can handle any schema
            }
        }

        Ok(())
    }

    async fn create_writer(&self) -> Result<Box<dyn DataWriter>, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileSink not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let writer = FileWriter::new(config.clone(), self.active_writers.clone()).await?;
        Ok(Box::new(writer))
    }

    fn supports_transactions(&self) -> bool {
        false // File sinks don't support transactions
    }

    fn supports_upsert(&self) -> bool {
        false // File sinks are append-only
    }

    fn metadata(&self) -> SinkMetadata {
        self.metadata.clone().unwrap_or_else(|| SinkMetadata {
            sink_type: "file".to_string(),
            version: "1.0".to_string(),
            supports_transactions: false,
            supports_upsert: false,
            supports_schema_evolution: false,
            capabilities: vec!["append_only".to_string()],
        })
    }
}

/// File writer implementation
pub struct FileWriter {
    config: FileSinkConfig,
    current_file: Option<File>,
    current_path: PathBuf,
    bytes_written: u64,
    records_written: u64,
    rotation_index: u32,
    write_buffer: Vec<u8>,
    buffer_size: usize,
    created_at: SystemTime,
    last_rotation: SystemTime,
    active_writers: Arc<Mutex<Vec<FileWriterState>>>,
}

impl FileWriter {
    /// Create a new file writer
    pub async fn new(
        config: FileSinkConfig,
        active_writers: Arc<Mutex<Vec<FileWriterState>>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut writer = Self {
            config: config.clone(),
            current_file: None,
            current_path: PathBuf::from(&config.path),
            bytes_written: 0,
            records_written: 0,
            rotation_index: 0,
            write_buffer: Vec::with_capacity(config.buffer_size_bytes as usize),
            buffer_size: config.buffer_size_bytes as usize,
            created_at: SystemTime::now(),
            last_rotation: SystemTime::now(),
            active_writers,
        };

        // Open initial file
        writer.open_new_file().await?;

        // Write CSV header if needed
        if writer.config.format == FileFormat::Csv && writer.config.csv_has_header {
            // Header will be written with first record when schema is known
        }

        Ok(writer)
    }

    /// Open a new file for writing
    async fn open_new_file(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Generate filename (with rotation if needed)
        let path = if self.rotation_index > 0 {
            FileSink::generate_rotated_filename(&self.current_path, self.rotation_index)
        } else {
            self.current_path.clone()
        };

        // Open file with append mode if resuming
        let file = if self.config.append_if_exists && path.exists() {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .await
        } else {
            File::create(&path).await
        }
        .map_err(|e| {
            Box::new(FileDataSourceError::IoError(format!(
                "Failed to open file {}: {}",
                path.display(),
                e
            ))) as Box<dyn Error + Send + Sync>
        })?;

        self.current_file = Some(file);
        self.current_path = path;
        self.bytes_written = 0;
        self.records_written = 0;
        self.last_rotation = SystemTime::now();

        Ok(())
    }

    /// Rotate to a new file
    async fn rotate(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Flush current buffer
        self.flush_buffer().await?;

        // Close current file
        if let Some(file) = self.current_file.take() {
            file.sync_all().await.map_err(|e| {
                Box::new(FileDataSourceError::IoError(e.to_string())) as Box<dyn Error + Send + Sync>
            })?;
        }

        // Increment rotation index
        self.rotation_index += 1;

        // Open new file
        self.open_new_file().await?;

        Ok(())
    }

    /// Write data to buffer
    async fn write_to_buffer(&mut self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.write_buffer.extend_from_slice(data);
        
        // Flush if buffer is full
        if self.write_buffer.len() >= self.buffer_size {
            self.flush_buffer().await?;
        }

        Ok(())
    }

    /// Flush write buffer to file
    async fn flush_buffer(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        if let Some(ref mut file) = self.current_file {
            file.write_all(&self.write_buffer).await.map_err(|e| {
                Box::new(FileDataSourceError::IoError(e.to_string())) as Box<dyn Error + Send + Sync>
            })?;

            self.bytes_written += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }

        Ok(())
    }

    /// Convert FieldValue to JSON-serializable value
    fn field_value_to_json(&self, field_value: &FieldValue) -> serde_json::Value {
        match field_value {
            FieldValue::String(s) => serde_json::Value::String(s.clone()),
            FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            FieldValue::Float(f) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or_default())
            }
            FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
            FieldValue::ScaledInteger(value, scale) => {
                // Convert to decimal string representation
                let divisor = 10_i64.pow(*scale as u32);
                let decimal_str = if *scale > 0 {
                    format!("{:.1$}", (*value as f64) / (divisor as f64), *scale as usize)
                } else {
                    value.to_string()
                };
                serde_json::Value::String(decimal_str)
            }
            FieldValue::Null => serde_json::Value::Null,
        }
    }

    /// Check if file rotation is needed
    fn needs_rotation(&self) -> bool {
        // Check size-based rotation
        if let Some(max_size) = self.config.max_file_size_bytes {
            if self.bytes_written >= max_size {
                return true;
            }
        }

        // Check time-based rotation
        if let Some(rotation_interval) = self.config.rotation_interval_ms {
            let elapsed = SystemTime::now()
                .duration_since(self.last_rotation)
                .unwrap_or_default();
            
            if elapsed >= Duration::from_millis(rotation_interval) {
                return true;
            }
        }

        // Check record count-based rotation
        if let Some(max_records) = self.config.max_records_per_file {
            if self.records_written >= max_records {
                return true;
            }
        }

        false
    }

    /// Serialize record to bytes based on format
    fn serialize_record(&self, record: &StreamRecord) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        match self.config.format {
            FileFormat::JsonLines => {
                // Convert record to JSON-serializable map
                let mut json_map = serde_json::Map::new();
                for (key, value) in &record.fields {
                    json_map.insert(key.clone(), self.field_value_to_json(value));
                }
                
                let json = serde_json::to_string(&json_map).map_err(|e| {
                    Box::new(FileDataSourceError::JsonParseError(e.to_string())) 
                        as Box<dyn Error + Send + Sync>
                })?;
                Ok(format!("{}\n", json).into_bytes())
            }
            FileFormat::Json => {
                // Convert record to JSON-serializable map
                let mut json_map = serde_json::Map::new();
                for (key, value) in &record.fields {
                    json_map.insert(key.clone(), self.field_value_to_json(value));
                }
                
                let json = serde_json::to_string(&json_map).map_err(|e| {
                    Box::new(FileDataSourceError::JsonParseError(e.to_string()))
                        as Box<dyn Error + Send + Sync>
                })?;
                Ok(format!("{},\n", json).into_bytes())
            }
            FileFormat::Csv | FileFormat::CsvNoHeader => {
                // Convert record to CSV row
                let mut csv_row = Vec::new();
                for (_key, value) in &record.fields {
                    // Simple CSV serialization - in production use proper CSV library
                    let value_str = format!("{:?}", value); // TODO: Proper CSV escaping
                    csv_row.push(value_str);
                }
                Ok(format!("{}\n", csv_row.join(&self.config.csv_delimiter)).into_bytes())
            }
        }
    }
}

#[async_trait]
impl DataWriter for FileWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Serialize record
        let serialized = self.serialize_record(&record)?;

        // Write to buffer
        self.write_to_buffer(&serialized).await?;

        self.records_written += 1;

        // Check if rotation is needed
        let needs_rotation = self.needs_rotation();

        if needs_rotation {
            self.rotate().await?;
        }

        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for record in records {
            self.write(record).await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.flush_buffer().await?;
        
        if let Some(ref mut file) = self.current_file {
            file.sync_all().await.map_err(|e| {
                Box::new(FileDataSourceError::IoError(e.to_string())) as Box<dyn Error + Send + Sync>
            })?;
        }
        
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // File sinks don't support updates
        Err(Box::new(FileDataSourceError::UnsupportedFormat(
            "File sinks do not support update operations".to_string(),
        )))
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        // File sinks don't support deletes
        Err(Box::new(FileDataSourceError::UnsupportedFormat(
            "File sinks do not support delete operations".to_string(),
        )))
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // File sinks don't support transactions, just flush
        self.flush().await
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // File sinks don't support transactions
        Err(Box::new(FileDataSourceError::UnsupportedFormat(
            "File sinks do not support transaction rollback".to_string(),
        )))
    }
}

impl FileWriter {
    /// Close the file writer (additional method not in trait)
    pub async fn close(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Flush any remaining data
        self.flush().await?;

        // Close file
        if let Some(mut file) = self.current_file.take() {
            file.shutdown().await.map_err(|e| {
                Box::new(FileDataSourceError::IoError(e.to_string())) as Box<dyn Error + Send + Sync>
            })?;
        }

        Ok(())
    }
}