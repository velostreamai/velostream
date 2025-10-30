//! File Sink Implementation
//!
//! Provides writing capabilities for streaming data to files with support for:
//! - Multiple formats (JSON, CSV, Parquet)
//! - File rotation by size and time
//! - Compression (gzip, snappy)
//! - Buffering and batching optimizations

use crate::velostream::config::{
    ConfigSchemaProvider, GlobalSchemaContext, PropertyDefault, PropertyValidation,
};
use crate::velostream::datasource::config::SinkConfig;
use crate::velostream::datasource::traits::{DataSink, DataWriter};
use crate::velostream::datasource::types::SinkMetadata;
use crate::velostream::datasource::{BatchConfig, BatchStrategy};
use crate::velostream::schema::Schema;
use crate::velostream::serialization::helpers::field_value_to_json;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
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
pub struct FileDataSink {
    config: Option<FileSinkConfig>,
    metadata: Option<SinkMetadata>,
    active_writers: Arc<Mutex<Vec<FileWriterState>>>,
}

/// State for active file writers
pub struct FileWriterState {
    path: PathBuf,
    writer: File,
    bytes_written: u64,
    records_written: u64,
    created_at: SystemTime,
    last_rotation: SystemTime,
}

impl FileDataSink {
    /// Create a new file sink
    pub fn new() -> Self {
        Self {
            config: None,
            metadata: None,
            active_writers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create a file sink from properties
    pub fn from_properties(props: &std::collections::HashMap<String, String>) -> Self {
        use super::config::{FileFormat, FileSinkConfig};

        // Helper function to get property with sink. prefix fallback
        let get_sink_prop = |key: &str| {
            props
                .get(&format!("sink.{}", key))
                .or_else(|| props.get(key))
                .cloned()
        };

        let path = get_sink_prop("path").unwrap_or_else(|| "output.json".to_string());
        let format = match get_sink_prop("format").as_deref() {
            Some("csv") => FileFormat::Csv,
            Some("json") => FileFormat::Json,
            Some("jsonlines") => FileFormat::JsonLines,
            _ => FileFormat::Json, // Default to JSON
        };

        let config = FileSinkConfig {
            path,
            format,
            append_if_exists: get_sink_prop("append")
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            buffer_size_bytes: get_sink_prop("buffer_size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(8192),
            max_file_size_bytes: None,
            rotation_interval_ms: None,
            max_records_per_file: None,
            compression: None, // TODO: Parse compression from props
            csv_delimiter: ",".to_string(),
            csv_has_header: get_sink_prop("has_headers")
                .and_then(|s| s.parse().ok())
                .unwrap_or(true),
            writer_threads: 1,
        };

        Self {
            config: Some(config),
            metadata: None,
            active_writers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> Option<&FileSinkConfig> {
        self.config.as_ref()
    }

    /// Validate File sink configuration properties
    /// Returns (errors, warnings, recommendations) tuples
    pub fn validate_sink_config(
        properties: &HashMap<String, String>,
        name: &str,
    ) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();

        // Required properties
        let required_keys = vec!["path", "format"];
        for key in &required_keys {
            if !properties.contains_key(*key) {
                errors.push(format!(
                    "File sink '{}' missing required config: {}",
                    name, key
                ));
            }
        }

        // Validate path directory exists
        if let Some(path) = properties.get("path") {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.exists() {
                    warnings.push(format!(
                        "File sink '{}' output directory does not exist: {}",
                        name,
                        parent.display()
                    ));
                }
            }
        }

        // Validate format
        if let Some(format) = properties.get("format") {
            let valid_formats = ["json", "csv", "jsonlines", "parquet"];
            if !valid_formats.contains(&format.as_str()) {
                errors.push(format!(
                    "File sink '{}' has invalid format '{}'. Valid formats: {}",
                    name,
                    format,
                    valid_formats.join(", ")
                ));
            }
        }

        // Performance recommendations
        if !properties.contains_key("buffer_size") {
            recommendations.push(format!(
                "File sink '{}' could benefit from buffer_size configuration for better write performance",
                name
            ));
        }

        if properties.get("format") == Some(&"csv".to_string())
            && !properties.contains_key("has_headers")
        {
            recommendations.push(format!(
                "File sink '{}' using CSV format should specify has_headers configuration",
                name
            ));
        }

        (errors, warnings, recommendations)
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
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Err(Box::new(
                FileDataSourceError::PermissionDenied(path.to_string()),
            )),
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

    /// Optimize file sink configuration based on batch strategy
    fn optimize_config_for_batch_strategy(
        &self,
        base_config: &FileSinkConfig,
        batch_config: &BatchConfig,
    ) -> FileSinkConfig {
        let mut optimized_config = base_config.clone();

        if !batch_config.enable_batching {
            // Suggest disabling batching optimizations - immediate writes (only if using default buffer size)
            if optimized_config.buffer_size_bytes == 65536 {
                // Default buffer size
                optimized_config.buffer_size_bytes = 0; // No buffering
            }
            return optimized_config;
        }

        match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => {
                // Suggest buffer size to accommodate fixed batch size (only if using default buffer size)
                if optimized_config.buffer_size_bytes == 65536 {
                    // Default buffer size
                    let buffer_size = (*size * 4096).min(batch_config.max_batch_size * 4096); // 4KB per record estimate
                    optimized_config.buffer_size_bytes = buffer_size as u64;
                }
            }
            BatchStrategy::TimeWindow(_duration) => {
                // Suggest larger buffer for time-based batching (only if using default buffer size)
                if optimized_config.buffer_size_bytes == 65536 {
                    // Default buffer size
                    optimized_config.buffer_size_bytes = 1024 * 1024; // 1MB buffer for time-based batching
                }
            }
            BatchStrategy::AdaptiveSize { .. } => {
                // Suggest moderate buffer for adaptive sizing (only if using default buffer size)
                if optimized_config.buffer_size_bytes == 65536 {
                    // Default buffer size
                    optimized_config.buffer_size_bytes = 512 * 1024; // 512KB adaptive buffer
                }
            }
            BatchStrategy::MemoryBased(max_bytes) => {
                // Suggest buffer size based on memory target (only if using default buffer size)
                if optimized_config.buffer_size_bytes == 65536 {
                    // Default buffer size
                    let buffer_size = (*max_bytes).min(16 * 1024 * 1024); // Max 16MB buffer
                    optimized_config.buffer_size_bytes = buffer_size as u64;
                }

                // Suggest compression for large batches only if not explicitly set
                let buffer_size = optimized_config.buffer_size_bytes;
                if optimized_config.compression.is_none() && buffer_size > 1024 * 1024 {
                    optimized_config.compression = Some(super::config::CompressionType::Gzip);
                }
            }
            BatchStrategy::LowLatency {
                eager_processing, ..
            } => {
                // Suggest optimizations for low latency (only if using default buffer size)
                if optimized_config.buffer_size_bytes == 65536 {
                    // Default buffer size
                    if *eager_processing {
                        // Immediate write mode
                        optimized_config.buffer_size_bytes = 0;
                    } else {
                        // Small buffer for minimal latency
                        optimized_config.buffer_size_bytes = 4096; // 4KB minimal buffer
                    }
                }
            }
            BatchStrategy::MegaBatch { batch_size, .. } => {
                // High-throughput mega-batch processing (Phase 4 optimization)
                if optimized_config.buffer_size_bytes == 65536 {
                    // Default buffer size
                    let buffer_size = (*batch_size * 8192).min(100 * 1024 * 1024); // 8KB per record estimate, max 100MB
                    optimized_config.buffer_size_bytes = buffer_size as u64;
                }

                // Suggest compression for very large batches only if not explicitly set
                if optimized_config.compression.is_none()
                    && optimized_config.buffer_size_bytes > 10 * 1024 * 1024
                {
                    use super::config::CompressionType;
                    optimized_config.compression = Some(CompressionType::Gzip);
                }
            }
        }

        optimized_config
    }

    /// Log the file writer configuration for debugging and monitoring
    fn log_file_writer_config(&self, config: &FileSinkConfig, batch_config: &BatchConfig) {
        use log::info;

        info!("=== File Writer Configuration ===");
        info!("Batch Strategy: {:?}", batch_config.strategy);
        info!("Batch Configuration:");
        info!("  - Enable Batching: {}", batch_config.enable_batching);
        info!("  - Max Batch Size: {}", batch_config.max_batch_size);
        info!("  - Batch Timeout: {:?}", batch_config.batch_timeout);

        info!("Applied File Writer Settings:");
        info!("  - buffer_size_bytes: {}", config.buffer_size_bytes);
        info!("  - compression: {:?}", config.compression);
        info!("  - format: {:?}", config.format);
        info!("  - path: {}", config.path);
        info!("  - append_if_exists: {}", config.append_if_exists);
        if let Some(max_file_size) = config.max_file_size_bytes {
            info!("  - max_file_size: {}MB", max_file_size / (1024 * 1024));
        }
        if let Some(rotation_interval_ms) = config.rotation_interval_ms {
            info!("  - rotation_interval: {}ms", rotation_interval_ms);
        }
        info!("=====================================");
    }
}

impl Default for FileDataSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSink for FileDataSink {
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
                    match &field.data_type {
                        crate::velostream::sql::ast::DataType::Array(_)
                        | crate::velostream::sql::ast::DataType::Map(_, _) => {
                            eprintln!(
                                "Warning: Field '{}' has complex type that will be serialized as string in CSV",
                                field.name
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

    async fn create_writer_with_batch_config(
        &self,
        batch_config: BatchConfig,
    ) -> Result<Box<dyn DataWriter>, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileSink not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        // Create an optimized config based on batch strategy
        let optimized_config = self.optimize_config_for_batch_strategy(config, &batch_config);

        // Log the optimized configuration
        self.log_file_writer_config(&optimized_config, &batch_config);

        let writer = FileWriter::new_with_batch_config(
            optimized_config,
            self.active_writers.clone(),
            batch_config,
        )
        .await?;
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

    /// Create a new file writer with batch configuration optimizations
    pub async fn new_with_batch_config(
        config: FileSinkConfig,
        active_writers: Arc<Mutex<Vec<FileWriterState>>>,
        batch_config: BatchConfig,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Apply batch configuration optimizations to buffer size
        let buffer_size = config.buffer_size_bytes.max(64 * 1024) as usize; // Default 64KB
        let optimized_buffer_size = if batch_config.enable_batching {
            match &batch_config.strategy {
                BatchStrategy::FixedSize(size) => {
                    (*size * 1024).min(buffer_size.max(8 * 1024)) // At least 8KB, scale with batch size
                }
                BatchStrategy::MemoryBased(max_bytes) => {
                    (*max_bytes).min(16 * 1024 * 1024) // Max 16MB
                }
                BatchStrategy::LowLatency {
                    eager_processing: true,
                    ..
                } => {
                    0 // No buffering for eager processing
                }
                BatchStrategy::LowLatency { .. } => {
                    4096 // 4KB minimal buffer
                }
                _ => buffer_size, // Use configured buffer size
            }
        } else {
            0 // No batching, no buffering
        };

        let mut writer = Self {
            config: config.clone(),
            current_file: None,
            current_path: PathBuf::from(&config.path),
            bytes_written: 0,
            records_written: 0,
            rotation_index: 0,
            write_buffer: Vec::with_capacity(optimized_buffer_size),
            buffer_size: optimized_buffer_size,
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
            FileDataSink::generate_rotated_filename(&self.current_path, self.rotation_index)
        } else {
            self.current_path.clone()
        };

        // Open file with append mode if resuming
        let file = if self.config.append_if_exists && path.exists() {
            tokio::fs::OpenOptions::new().append(true).open(&path).await
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
                Box::new(FileDataSourceError::IoError(e.to_string()))
                    as Box<dyn Error + Send + Sync>
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

        if let Some(file) = self.current_file.as_mut() {
            file.write_all(&self.write_buffer).await.map_err(|e| {
                Box::new(FileDataSourceError::IoError(e.to_string()))
                    as Box<dyn Error + Send + Sync>
            })?;

            self.bytes_written += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }

        Ok(())
    }

    /// Convert FieldValue to JSON-serializable value using the standard serialization helper
    fn field_value_to_json_value(
        &self,
        field_value: &FieldValue,
    ) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
        field_value_to_json(field_value).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
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
    fn serialize_record(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        match self.config.format {
            FileFormat::JsonLines => {
                // Convert record to JSON-serializable map
                let mut json_map = serde_json::Map::new();
                for (key, value) in &record.fields {
                    json_map.insert(key.clone(), self.field_value_to_json_value(value)?);
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
                    json_map.insert(key.clone(), self.field_value_to_json_value(value)?);
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
                for value in record.fields.values() {
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

        if let Some(file) = self.current_file.as_mut() {
            file.sync_all().await.map_err(|e| {
                Box::new(FileDataSourceError::IoError(e.to_string()))
                    as Box<dyn Error + Send + Sync>
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
                Box::new(FileDataSourceError::IoError(e.to_string()))
                    as Box<dyn Error + Send + Sync>
            })?;
        }

        Ok(())
    }
}

/// ConfigSchemaProvider implementation for FileSink
impl ConfigSchemaProvider for FileDataSink {
    fn config_type_id() -> &'static str {
        "file_sink"
    }

    fn inheritable_properties() -> Vec<&'static str> {
        vec![
            "buffer_size_bytes",
            "batch.size",
            "batch.timeout_ms",
            "compression",
            "writer_threads",
            "max_file_size_bytes",
        ]
    }

    fn required_named_properties() -> Vec<&'static str> {
        vec!["path"]
    }

    fn optional_properties_with_defaults() -> HashMap<&'static str, PropertyDefault> {
        let mut defaults = HashMap::new();
        defaults.insert("format", PropertyDefault::Static("jsonlines".to_string()));
        defaults.insert(
            "append_if_exists",
            PropertyDefault::Static("false".to_string()),
        );
        defaults.insert(
            "buffer_size_bytes",
            PropertyDefault::GlobalLookup("file.sink.buffer_size_bytes".to_string()),
        );
        defaults.insert("csv_delimiter", PropertyDefault::Static(",".to_string()));
        defaults.insert(
            "csv_has_header",
            PropertyDefault::Static("true".to_string()),
        );
        defaults.insert(
            "writer_threads",
            PropertyDefault::GlobalLookup("file.sink.writer_threads".to_string()),
        );
        defaults.insert(
            "compression",
            PropertyDefault::GlobalLookup("file.sink.compression".to_string()),
        );
        defaults
    }

    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "path" => {
                if value.is_empty() {
                    return Err(vec!["path cannot be empty".to_string()]);
                }

                // Validate output path - check for dangerous patterns
                if value.contains("..") && !value.contains("%") {
                    // Allow ".." in strftime patterns like "%Y-%m-%d/../../archive"
                    return Err(vec![
                        "path contains '..' which could be a security risk".to_string()
                    ]);
                }

                // Validate path doesn't end with directory separator without filename
                if value.ends_with('/') || value.ends_with('\\') {
                    return Err(vec![
                        "path must specify a filename, not just a directory".to_string()
                    ]);
                }
            }
            "format" => {
                let valid_formats = ["json", "jsonlines", "csv", "csv_no_header"];
                if !valid_formats.contains(&value) {
                    return Err(vec![format!(
                        "format must be one of: {}. Got: '{}'",
                        valid_formats.join(", "),
                        value
                    )]);
                }
            }
            "append_if_exists" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec![format!(
                        "append_if_exists must be 'true' or 'false'. Got: '{}'",
                        value
                    )]);
                }
            }
            "buffer_size_bytes" => {
                if let Ok(size) = value.parse::<u64>() {
                    if size < 1024 {
                        return Err(vec![
                            "buffer_size_bytes must be at least 1024 bytes".to_string()
                        ]);
                    }
                    if size > 1024 * 1024 * 1024 {
                        // 1GB max
                        return Err(vec![
                            "buffer_size_bytes must not exceed 1GB (1,073,741,824 bytes)"
                                .to_string(),
                        ]);
                    }
                } else {
                    return Err(vec![
                        "buffer_size_bytes must be a valid number in bytes".to_string()
                    ]);
                }
            }
            "max_file_size_bytes" => {
                if let Ok(size) = value.parse::<u64>() {
                    if size < 1024 {
                        return Err(vec![
                            "max_file_size_bytes must be at least 1024 bytes".to_string()
                        ]);
                    }
                    if size > 1024u64.pow(4) {
                        // 1TB max
                        return Err(vec!["max_file_size_bytes must not exceed 1TB".to_string()]);
                    }
                } else {
                    return Err(vec![
                        "max_file_size_bytes must be a valid number in bytes".to_string()
                    ]);
                }
            }
            "rotation_interval_ms" => {
                if let Ok(interval) = value.parse::<u64>() {
                    if interval < 1000 {
                        return Err(vec![
                            "rotation_interval_ms must be at least 1000ms (1 second)".to_string(),
                        ]);
                    }
                    if interval > 86400000 {
                        // 24 hours max
                        return Err(vec![
                            "rotation_interval_ms must not exceed 86,400,000ms (24 hours)"
                                .to_string(),
                        ]);
                    }
                } else {
                    return Err(vec![
                        "rotation_interval_ms must be a valid number in milliseconds".to_string(),
                    ]);
                }
            }
            "max_records_per_file" => {
                if let Ok(max) = value.parse::<u64>() {
                    if max == 0 {
                        return Err(vec![
                            "max_records_per_file must be greater than 0".to_string()
                        ]);
                    }
                } else {
                    return Err(vec![
                        "max_records_per_file must be a valid positive number".to_string()
                    ]);
                }
            }
            "compression" => {
                let valid_compressions = ["none", "gzip", "snappy", "zstd"];
                if !valid_compressions.contains(&value) {
                    return Err(vec![format!(
                        "compression must be one of: {}. Got: '{}'",
                        valid_compressions.join(", "),
                        value
                    )]);
                }
            }
            "csv_delimiter" => {
                if value.len() != 1 {
                    return Err(vec!["csv_delimiter must be a single character".to_string()]);
                }
                let delimiter_char = value.chars().next().unwrap();
                if delimiter_char.is_alphabetic() || delimiter_char.is_numeric() {
                    return Err(vec![
                        "csv_delimiter should not be alphanumeric (could conflict with data)"
                            .to_string(),
                    ]);
                }
            }
            "csv_has_header" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec![format!(
                        "csv_has_header must be 'true' or 'false'. Got: '{}'",
                        value
                    )]);
                }
            }
            "writer_threads" => {
                if let Ok(threads) = value.parse::<usize>() {
                    if threads == 0 {
                        return Err(vec!["writer_threads must be greater than 0".to_string()]);
                    }
                    if threads > 64 {
                        return Err(vec![
                            "writer_threads must not exceed 64 for performance reasons".to_string(),
                        ]);
                    }
                } else {
                    return Err(vec![
                        "writer_threads must be a valid positive number".to_string()
                    ]);
                }
            }
            key if key.starts_with("batch.") => {
                // Delegate batch property validation to BatchConfig
                // This will be handled by the batch config schema provider
            }
            _ => {
                // Allow other file-related properties with basic validation
                if value.is_empty() && !key.starts_with("custom.") {
                    return Err(vec![format!("Property '{}' cannot be empty", key)]);
                }
            }
        }
        Ok(())
    }

    fn json_schema() -> Value {
        serde_json::json!({
            "type": "object",
            "title": "File Data Sink Configuration Schema",
            "description": "Configuration schema for file-based data sinks in Velostream",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Output file path (supports strftime formatting for rotation)",
                    "examples": [
                        "./output.json",
                        "/data/output-%Y-%m-%d.csv",
                        "./logs/app-%H%M.jsonl"
                    ]
                },
                "format": {
                    "type": "string",
                    "enum": ["json", "jsonlines", "csv", "csv_no_header"],
                    "default": "jsonlines",
                    "description": "Output file format"
                },
                "append_if_exists": {
                    "type": "boolean",
                    "default": false,
                    "description": "Whether to append to existing files or overwrite them"
                },
                "buffer_size_bytes": {
                    "type": "integer",
                    "minimum": 1024,
                    "maximum": 1073741824,
                    "default": 65536,
                    "description": "Buffer size for writing files (bytes)"
                },
                "max_file_size_bytes": {
                    "type": "integer",
                    "minimum": 1024,
                    "description": "Maximum file size before rotation (bytes)"
                },
                "rotation_interval_ms": {
                    "type": "integer",
                    "minimum": 1000,
                    "maximum": 86400000,
                    "description": "Time interval between file rotations (milliseconds)"
                },
                "max_records_per_file": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum records per file before rotation"
                },
                "compression": {
                    "type": "string",
                    "enum": ["none", "gzip", "snappy", "zstd"],
                    "description": "Compression type for output files"
                },
                "csv_delimiter": {
                    "type": "string",
                    "maxLength": 1,
                    "default": ",",
                    "description": "CSV field delimiter character",
                    "examples": [",", ";", "|", "\t"]
                },
                "csv_has_header": {
                    "type": "boolean",
                    "default": true,
                    "description": "Whether to write CSV header row"
                },
                "writer_threads": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 64,
                    "default": 1,
                    "description": "Number of writer threads for parallel writing"
                }
            },
            "required": ["path"],
            "additionalProperties": true
        })
    }

    fn property_validations() -> Vec<PropertyValidation> {
        vec![
            PropertyValidation {
                key: "path".to_string(),
                required: true,
                default: None,
                description: "Output file path (supports strftime formatting)".to_string(),
                json_type: "string".to_string(),
                validation_pattern: None,
            },
            PropertyValidation {
                key: "format".to_string(),
                required: false,
                default: Some(PropertyDefault::Static("jsonlines".to_string())),
                description: "Output file format".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("json|jsonlines|csv|csv_no_header".to_string()),
            },
            PropertyValidation {
                key: "buffer_size_bytes".to_string(),
                required: false,
                default: Some(PropertyDefault::GlobalLookup(
                    "file.sink.buffer_size_bytes".to_string(),
                )),
                description: "Buffer size for writing files in bytes".to_string(),
                json_type: "integer".to_string(),
                validation_pattern: Some(
                    "^(?:102[4-9]|10[3-9][0-9]|1[1-9][0-9]{2}|[2-9][0-9]{3,9}|1073741824)$"
                        .to_string(),
                ),
            },
            PropertyValidation {
                key: "compression".to_string(),
                required: false,
                default: Some(PropertyDefault::GlobalLookup(
                    "file.sink.compression".to_string(),
                )),
                description: "Compression type for output files".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("none|gzip|snappy|zstd".to_string()),
            },
        ]
    }

    fn supports_custom_properties() -> bool {
        true // Allow custom file output properties
    }

    fn global_schema_dependencies() -> Vec<&'static str> {
        vec!["batch_config", "file_global"]
    }

    fn resolve_property_with_inheritance(
        &self,
        key: &str,
        local_value: Option<&str>,
        global_context: &GlobalSchemaContext,
    ) -> Result<Option<String>, String> {
        // Local value takes precedence
        if let Some(value) = local_value {
            return Ok(Some(value.to_string()));
        }

        // Property-specific inheritance logic
        match key {
            "buffer_size_bytes" => {
                if let Some(global_buffer) = global_context
                    .global_properties
                    .get("file.sink.buffer_size_bytes")
                {
                    return Ok(Some(global_buffer.clone()));
                }
                Ok(Some("65536".to_string())) // Default 64KB buffer
            }
            "compression" => {
                if let Some(global_compression) = global_context
                    .global_properties
                    .get("file.sink.compression")
                {
                    return Ok(Some(global_compression.clone()));
                }
                // Environment-based compression defaults
                let dev_default = "dev".to_string();
                let env_profile = global_context
                    .environment_variables
                    .get("ENVIRONMENT")
                    .unwrap_or(&dev_default);
                let default_compression = if env_profile == "production" {
                    "gzip"
                } else {
                    "none"
                };
                Ok(Some(default_compression.to_string()))
            }
            "writer_threads" => {
                if let Some(global_threads) = global_context
                    .global_properties
                    .get("file.sink.writer_threads")
                {
                    return Ok(Some(global_threads.clone()));
                }
                // Environment-based thread count
                let dev_default = "dev".to_string();
                let env_profile = global_context
                    .environment_variables
                    .get("ENVIRONMENT")
                    .unwrap_or(&dev_default);
                let default_threads = if env_profile == "production" {
                    "4"
                } else {
                    "1"
                };
                Ok(Some(default_threads.to_string()))
            }
            "max_file_size_bytes" => {
                if let Some(global_max_size) = global_context
                    .global_properties
                    .get("file.sink.max_file_size_bytes")
                {
                    return Ok(Some(global_max_size.clone()));
                }
                // Environment-based file size limits
                let dev_default = "dev".to_string();
                let env_profile = global_context
                    .environment_variables
                    .get("ENVIRONMENT")
                    .unwrap_or(&dev_default);
                if env_profile == "production" {
                    Ok(Some("1073741824".to_string())) // 1GB in production
                } else {
                    Ok(Some("10485760".to_string())) // 10MB in development
                }
            }
            _ => {
                // Check global properties for other keys
                if let Some(global_value) = global_context.global_properties.get(key) {
                    return Ok(Some(global_value.clone()));
                }
                Ok(None)
            }
        }
    }

    fn schema_version() -> &'static str {
        "2.0.0" // Updated version with comprehensive file sink validation
    }
}
