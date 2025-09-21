//! File Data Source Implementation

use crate::velostream::config::{
    ConfigSchemaProvider, GlobalSchemaContext, PropertyDefault, PropertyValidation,
};
use crate::velostream::datasource::config::SourceConfig;
use crate::velostream::datasource::traits::{DataReader, DataSource};
use crate::velostream::datasource::types::SourceMetadata;
use crate::velostream::schema::Schema;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use super::config::{FileFormat, FileSourceConfig};
use super::error::FileDataSourceError;
use super::reader::FileReader;
use super::watcher::FileWatcher;

/// File-based data source implementation
///
/// Supports reading from:
/// - CSV files with header inference
/// - JSON Lines files (newline-delimited JSON)
/// - Single JSON array files
/// - File glob patterns for multiple files
/// - Real-time file watching and streaming
pub struct FileDataSource {
    config: Option<FileSourceConfig>,
    metadata: Option<SourceMetadata>,
    watcher: Option<FileWatcher>,
}

impl FileDataSource {
    /// Create a new file data source
    pub fn new() -> Self {
        Self {
            config: None,
            metadata: None,
            watcher: None,
        }
    }

    /// Create a file data source from properties
    pub fn from_properties(props: &std::collections::HashMap<String, String>) -> Self {
        let mut datasource = Self::new();

        // Helper function to get property with source. prefix fallback
        let get_source_prop = |key: &str| {
            props
                .get(&format!("source.{}", key))
                .or_else(|| props.get(key))
                .cloned()
        };

        // Extract path and format from properties
        let path = get_source_prop("path").unwrap_or_else(|| "./demo_data/sample.csv".to_string());
        let format_str = get_source_prop("format").unwrap_or_else(|| "csv".to_string());

        // Parse format
        let format = Self::parse_file_format(&format_str);

        // Create config
        let config = FileSourceConfig {
            path,
            format,
            watch_for_changes: get_source_prop("watching")
                .or_else(|| get_source_prop("watch"))
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false),
            polling_interval_ms: get_source_prop("polling_interval")
                .and_then(|v| v.parse::<u64>().ok()),
            csv_delimiter: get_source_prop("delimiter")
                .and_then(|v| v.chars().next())
                .unwrap_or(','),
            csv_has_header: get_source_prop("has_headers")
                .or_else(|| get_source_prop("header"))
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(true),
            ..Default::default()
        };

        datasource.config = Some(config);
        datasource
    }

    /// Parse file format string into FileFormat enum
    fn parse_file_format(format_str: &str) -> FileFormat {
        match format_str.to_lowercase().as_str() {
            "json" => FileFormat::Json,
            "jsonlines" | "json_lines" => FileFormat::JsonLines,
            "csv_no_header" => FileFormat::CsvNoHeader,
            _ => FileFormat::Csv, // Default to CSV with header
        }
    }

    /// Generate SourceConfig from current state
    pub fn to_source_config(&self) -> SourceConfig {
        if let Some(config) = &self.config {
            // Convert file-specific FileFormat to generic FileFormat
            let generic_format = match config.format {
                FileFormat::Csv => crate::velostream::datasource::config::FileFormat::Csv {
                    header: config.csv_has_header,
                    delimiter: config.csv_delimiter,
                    quote: config.csv_quote,
                },
                FileFormat::CsvNoHeader => crate::velostream::datasource::config::FileFormat::Csv {
                    header: false,
                    delimiter: config.csv_delimiter,
                    quote: config.csv_quote,
                },
                FileFormat::JsonLines => crate::velostream::datasource::config::FileFormat::Json,
                FileFormat::Json => crate::velostream::datasource::config::FileFormat::Json,
            };

            SourceConfig::File {
                path: config.path.clone(),
                format: generic_format,
                properties: std::collections::HashMap::new(),
                batch_config: crate::velostream::datasource::BatchConfig::default(),
            }
        } else {
            // Return default if not configured
            SourceConfig::File {
                path: "./demo_data/sample.csv".to_string(),
                format: crate::velostream::datasource::config::FileFormat::Csv {
                    header: true,
                    delimiter: ',',
                    quote: '"',
                },
                properties: std::collections::HashMap::new(),
                batch_config: crate::velostream::datasource::BatchConfig::default(),
            }
        }
    }

    /// Self-initialize with current configuration
    pub async fn self_initialize(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let config = self.to_source_config();
        self.initialize(config).await
    }

    /// Get the current configuration
    pub fn config(&self) -> Option<&FileSourceConfig> {
        self.config.as_ref()
    }

    /// Validate file path exists and is accessible
    async fn validate_path(&self, path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path_obj = Path::new(path);

        // Handle glob patterns
        if path.contains('*') || path.contains('?') {
            // For glob patterns, validate the parent directory exists
            if let Some(parent) = path_obj.parent() {
                if !parent.exists() {
                    return Err(Box::new(FileDataSourceError::FileNotFound(
                        parent.to_string_lossy().to_string(),
                    )));
                }
            }
        } else {
            // For specific files, check file exists and is readable
            if !path_obj.exists() {
                return Err(Box::new(FileDataSourceError::FileNotFound(
                    path.to_string(),
                )));
            }

            if !path_obj.is_file() {
                return Err(Box::new(FileDataSourceError::InvalidPath(format!(
                    "Path is not a file: {}",
                    path
                ))));
            }

            // Try to open file to check permissions
            std::fs::File::open(path_obj).map_err(|e| {
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    Box::new(FileDataSourceError::PermissionDenied(path.to_string()))
                } else {
                    Box::new(FileDataSourceError::IoError(e.to_string()))
                }
            })?;
        }

        Ok(())
    }

    /// Infer schema from file content
    async fn infer_schema(
        &self,
        config: &FileSourceConfig,
    ) -> Result<Schema, Box<dyn Error + Send + Sync>> {
        match config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => self.infer_csv_schema(config).await,
            FileFormat::JsonLines | FileFormat::Json => self.infer_json_schema(config).await,
        }
    }

    /// Infer schema from CSV file
    async fn infer_csv_schema(
        &self,
        config: &FileSourceConfig,
    ) -> Result<Schema, Box<dyn Error + Send + Sync>> {
        use std::fs::File;
        use std::io::BufReader;

        let file =
            File::open(&config.path).map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
        let mut reader = BufReader::new(file);

        // For now, return a basic schema - in a full implementation,
        // we would parse CSV headers and infer column types
        let mut fields = Vec::new();

        if config.csv_has_header {
            // Read first line to get column names
            use std::io::BufRead;
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;

            // Simple CSV parsing - in production, use a proper CSV library
            let columns: Vec<&str> = line.trim().split(config.csv_delimiter).collect();
            for (i, column) in columns.iter().enumerate() {
                use crate::velostream::schema::FieldDefinition;
                use crate::velostream::sql::ast::DataType;

                fields.push(FieldDefinition::new(
                    if column.is_empty() {
                        format!("column_{}", i)
                    } else {
                        column.to_string()
                    },
                    DataType::String,
                    true, // nullable
                ));
            }
        } else {
            // Generate generic column names
            use crate::velostream::schema::FieldDefinition;
            use crate::velostream::sql::ast::DataType;

            fields.push(FieldDefinition::new(
                "column_0".to_string(),
                DataType::String,
                true,
            ));
        }

        Ok(Schema::new(fields))
    }

    /// Infer schema from JSON file
    async fn infer_json_schema(
        &self,
        config: &FileSourceConfig,
    ) -> Result<Schema, Box<dyn Error + Send + Sync>> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let file =
            File::open(&config.path).map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
        let reader = BufReader::new(file);

        // For JsonLines, read first line to infer schema
        // For Json, would need to parse the entire array
        let mut fields = Vec::new();

        match config.format {
            FileFormat::JsonLines => {
                // Read first non-empty line
                for line in reader.lines().take(1) {
                    let line = line.map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                    if !line.trim().is_empty() {
                        // Parse JSON to infer schema
                        let json: serde_json::Value = serde_json::from_str(&line)
                            .map_err(|e| FileDataSourceError::JsonParseError(e.to_string()))?;

                        if let Some(obj) = json.as_object() {
                            use crate::velostream::schema::FieldDefinition;
                            use crate::velostream::sql::ast::DataType;

                            for (key, value) in obj {
                                let data_type = match value {
                                    serde_json::Value::String(_) => DataType::String,
                                    serde_json::Value::Number(n) if n.is_i64() => DataType::Integer,
                                    serde_json::Value::Number(_) => DataType::Float,
                                    serde_json::Value::Bool(_) => DataType::Boolean,
                                    _ => DataType::String,
                                };
                                fields.push(FieldDefinition::new(key.clone(), data_type, true));
                            }
                        }
                        break;
                    }
                }
            }
            FileFormat::Json => {
                // For JSON arrays, we'd need to parse the structure differently
                // For now, add a generic schema
                use crate::velostream::schema::FieldDefinition;
                use crate::velostream::sql::ast::DataType;

                fields.push(FieldDefinition::new(
                    "data".to_string(),
                    DataType::String,
                    true,
                ));
            }
            _ => unreachable!(),
        }

        Ok(Schema::new(fields))
    }
}

impl Default for FileDataSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for FileDataSource {
    async fn initialize(
        &mut self,
        config: SourceConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let file_config = FileSourceConfig::from_generic(&config).map_err(|e| {
            Box::new(FileDataSourceError::UnsupportedFormat(e)) as Box<dyn Error + Send + Sync>
        })?;

        // Validate configuration
        file_config.validate().map_err(|e| {
            Box::new(FileDataSourceError::InvalidPath(e)) as Box<dyn Error + Send + Sync>
        })?;

        // Validate file path
        self.validate_path(&file_config.path).await?;

        // Initialize file watcher if needed
        if file_config.watch_for_changes {
            let mut watcher = FileWatcher::new();
            watcher
                .watch(&file_config.path, file_config.polling_interval_ms)
                .await?;
            self.watcher = Some(watcher);
        }

        // Store configuration
        self.config = Some(file_config);

        // Create metadata
        self.metadata = Some(SourceMetadata {
            source_type: "file".to_string(),
            version: "1.0".to_string(),
            supports_streaming: self.config.as_ref().unwrap().watch_for_changes,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec![
                "batch_read".to_string(),
                if self.config.as_ref().unwrap().watch_for_changes {
                    "streaming".to_string()
                } else {
                    "static".to_string()
                },
                "schema_inference".to_string(),
            ],
        });

        Ok(())
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileDataSource not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        self.infer_schema(config).await
    }

    async fn create_reader(&self) -> Result<Box<dyn DataReader>, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileDataSource not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let reader = FileReader::new(config.clone()).await?;
        Ok(Box::new(reader))
    }

    async fn create_reader_with_batch_config(
        &self,
        batch_config: crate::velostream::datasource::BatchConfig,
    ) -> Result<Box<dyn DataReader>, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileDataSource not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let reader = FileReader::new_with_batch_config(config.clone(), batch_config).await?;
        Ok(Box::new(reader))
    }

    fn supports_streaming(&self) -> bool {
        self.config
            .as_ref()
            .map(|c| c.watch_for_changes)
            .unwrap_or(false)
    }

    fn supports_batch(&self) -> bool {
        true // File sources always support batch reading
    }

    fn metadata(&self) -> SourceMetadata {
        self.metadata.clone().unwrap_or_else(|| SourceMetadata {
            source_type: "file".to_string(),
            version: "1.0".to_string(),
            supports_streaming: false,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec!["batch_read".to_string()],
        })
    }
}

/// ConfigSchemaProvider implementation for FileDataSource
impl ConfigSchemaProvider for FileDataSource {
    fn config_type_id() -> &'static str {
        "file_source"
    }

    fn inheritable_properties() -> Vec<&'static str> {
        vec![
            "buffer_size",
            "batch.size",
            "batch.timeout_ms",
            "polling_interval",
            "recursive",
        ]
    }

    fn required_named_properties() -> Vec<&'static str> {
        vec!["path"]
    }

    fn optional_properties_with_defaults() -> HashMap<&'static str, PropertyDefault> {
        let mut defaults = HashMap::new();
        defaults.insert("format", PropertyDefault::Static("csv".to_string()));
        defaults.insert("delimiter", PropertyDefault::Static(",".to_string()));
        defaults.insert("quote", PropertyDefault::Static("\"".to_string()));
        defaults.insert("has_headers", PropertyDefault::Static("true".to_string()));
        defaults.insert("watching", PropertyDefault::Static("false".to_string()));
        defaults.insert(
            "polling_interval",
            PropertyDefault::Static("1000".to_string()),
        );
        defaults.insert(
            "buffer_size",
            PropertyDefault::GlobalLookup("file.buffer_size".to_string()),
        );
        defaults.insert(
            "max_records",
            PropertyDefault::GlobalLookup("file.max_records".to_string()),
        );
        defaults.insert("recursive", PropertyDefault::Static("false".to_string()));
        defaults
    }

    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "path" => {
                if value.is_empty() {
                    return Err(vec!["path cannot be empty".to_string()]);
                }

                // Basic path validation - check for dangerous patterns
                if value.contains("..") && !value.contains("*") {
                    return Err(vec![
                        "path contains '..' which could be a security risk".to_string()
                    ]);
                }

                // Validate glob patterns have proper syntax
                if (value.contains('*') || value.contains('?')) && value.ends_with('/') {
                    return Err(vec![
                        "glob pattern cannot end with '/' - specify file pattern".to_string(),
                    ]);
                }
            }
            "format" => {
                let valid_formats = ["csv", "csv_no_header", "json", "jsonlines", "jsonl"];
                if !valid_formats.contains(&value) {
                    return Err(vec![format!(
                        "format must be one of: {}. Got: '{}'",
                        valid_formats.join(", "),
                        value
                    )]);
                }
            }
            "delimiter" => {
                if value.len() != 1 {
                    return Err(vec!["delimiter must be a single character".to_string()]);
                }
                let delimiter_char = value.chars().next().unwrap();
                if delimiter_char.is_alphabetic() || delimiter_char.is_numeric() {
                    return Err(vec![
                        "delimiter should not be alphanumeric (could conflict with data)"
                            .to_string(),
                    ]);
                }
            }
            "quote" => {
                if value.len() != 1 {
                    return Err(vec!["quote must be a single character".to_string()]);
                }
            }
            "has_headers" | "watching" | "recursive" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec![format!(
                        "{} must be 'true' or 'false'. Got: '{}'",
                        key, value
                    )]);
                }
            }
            "polling_interval" => {
                if let Ok(interval) = value.parse::<u64>() {
                    if interval == 0 {
                        return Err(vec!["polling_interval must be greater than 0".to_string()]);
                    }
                    if interval > 3600000 {
                        // 1 hour max
                        return Err(vec![
                            "polling_interval must not exceed 3,600,000ms (1 hour)".to_string(),
                        ]);
                    }
                } else {
                    return Err(vec![
                        "polling_interval must be a valid number in milliseconds".to_string(),
                    ]);
                }
            }
            "buffer_size" => {
                if let Ok(size) = value.parse::<usize>() {
                    if size < 1024 {
                        return Err(vec!["buffer_size must be at least 1024 bytes".to_string()]);
                    }
                    if size > 100 * 1024 * 1024 {
                        // 100MB max
                        return Err(vec![
                            "buffer_size must not exceed 100MB (104,857,600 bytes)".to_string(),
                        ]);
                    }
                } else {
                    return Err(vec![
                        "buffer_size must be a valid number in bytes".to_string()
                    ]);
                }
            }
            "max_records" => {
                if let Ok(max) = value.parse::<usize>() {
                    if max == 0 {
                        return Err(vec!["max_records must be greater than 0".to_string()]);
                    }
                } else {
                    return Err(vec![
                        "max_records must be a valid positive number".to_string()
                    ]);
                }
            }
            "skip_lines" => {
                if value.parse::<usize>().is_err() {
                    return Err(vec![
                        "skip_lines must be a valid non-negative number".to_string()
                    ]);
                }
            }
            "extension_filter" => {
                if value.is_empty() {
                    return Err(vec!["extension_filter cannot be empty".to_string()]);
                }
                // Basic validation - should look like file extensions
                if !value.chars().all(|c| c.is_alphanumeric() || c == '.') {
                    return Err(vec![
                        "extension_filter should only contain alphanumeric characters and '.'"
                            .to_string(),
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
            "title": "File Data Source Configuration Schema",
            "description": "Configuration schema for file-based data sources in Velostream",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path or glob pattern for source files",
                    "examples": [
                        "./data/sample.csv",
                        "/logs/*.json",
                        "./data/**/*.csv"
                    ]
                },
                "format": {
                    "type": "string",
                    "enum": ["csv", "csv_no_header", "json", "jsonlines", "jsonl"],
                    "default": "csv",
                    "description": "File format to parse"
                },
                "delimiter": {
                    "type": "string",
                    "maxLength": 1,
                    "default": ",",
                    "description": "CSV field delimiter character",
                    "examples": [",", ";", "|", "\t"]
                },
                "quote": {
                    "type": "string",
                    "maxLength": 1,
                    "default": "\"",
                    "description": "CSV quote character"
                },
                "has_headers": {
                    "type": "boolean",
                    "default": true,
                    "description": "Whether CSV files have header rows"
                },
                "watching": {
                    "type": "boolean",
                    "default": false,
                    "description": "Whether to watch for file changes and new files"
                },
                "polling_interval": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 3600000,
                    "default": 1000,
                    "description": "Polling interval for file watching (milliseconds)"
                },
                "buffer_size": {
                    "type": "integer",
                    "minimum": 1024,
                    "maximum": 104857600,
                    "default": 8192,
                    "description": "Buffer size for reading files (bytes)"
                },
                "max_records": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum number of records to read (unlimited if not specified)"
                },
                "skip_lines": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 0,
                    "description": "Number of lines to skip at the beginning of files"
                },
                "recursive": {
                    "type": "boolean",
                    "default": false,
                    "description": "Whether to recursively search directories in glob patterns"
                },
                "extension_filter": {
                    "type": "string",
                    "pattern": "^[a-zA-Z0-9.]+$",
                    "description": "File extension filter for directory scanning",
                    "examples": ["csv", "json", "txt"]
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
                description: "File path or glob pattern for source files".to_string(),
                json_type: "string".to_string(),
                validation_pattern: None,
            },
            PropertyValidation {
                key: "format".to_string(),
                required: false,
                default: Some(PropertyDefault::Static("csv".to_string())),
                description: "File format to parse".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("csv|csv_no_header|json|jsonlines|jsonl".to_string()),
            },
            PropertyValidation {
                key: "delimiter".to_string(),
                required: false,
                default: Some(PropertyDefault::Static(",".to_string())),
                description: "CSV field delimiter character".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("^.$".to_string()),
            },
            PropertyValidation {
                key: "buffer_size".to_string(),
                required: false,
                default: Some(PropertyDefault::GlobalLookup(
                    "file.buffer_size".to_string(),
                )),
                description: "Buffer size for reading files in bytes".to_string(),
                json_type: "integer".to_string(),
                validation_pattern: Some("^(?:102[4-9]|10[3-9][0-9]|1[1-9][0-9]{2}|[2-9][0-9]{3}|[1-9][0-9]{4,7}|1048576[0-9]{2})$".to_string()),
            },
        ]
    }

    fn supports_custom_properties() -> bool {
        true // Allow custom file processing properties
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
            "buffer_size" => {
                if let Some(global_buffer) =
                    global_context.global_properties.get("file.buffer_size")
                {
                    return Ok(Some(global_buffer.clone()));
                }
                Ok(Some("8192".to_string())) // Default 8KB buffer
            }
            "polling_interval" => {
                if let Some(global_interval) = global_context
                    .global_properties
                    .get("file.polling_interval")
                {
                    return Ok(Some(global_interval.clone()));
                }
                Ok(Some("1000".to_string())) // Default 1 second
            }
            "recursive" => {
                if let Some(global_recursive) =
                    global_context.global_properties.get("file.recursive")
                {
                    return Ok(Some(global_recursive.clone()));
                }
                // Environment-based default
                let dev_default = "dev".to_string();
                let env_profile = global_context
                    .environment_variables
                    .get("ENVIRONMENT")
                    .unwrap_or(&dev_default);
                let default_recursive = if env_profile == "development" {
                    "true"
                } else {
                    "false"
                };
                Ok(Some(default_recursive.to_string()))
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
        "2.0.0" // Updated version with comprehensive file validation
    }
}
