//! Memory-Mapped File Data Source Implementation
//!
//! Provides a `file_source_mmap` datasource type that uses memory-mapped I/O
//! for high-throughput batch file processing. Parallel to `FileDataSource` but
//! uses `FileMmapReader` instead of `FileReader`.

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
use std::time::Duration;

use super::config::{FileFormat, FileSourceConfig};
use super::error::FileDataSourceError;
use super::reader_mmap::FileMmapReader;

/// Memory-mapped file data source for high-throughput batch processing.
///
/// Supports:
/// - CSV files (with or without headers, custom delimiters)
/// - JSON Lines files
/// - File glob patterns for multiple files
///
/// Does NOT support file watching / streaming — this is a batch-only source.
pub struct FileMmapDataSource {
    config: Option<FileSourceConfig>,
    metadata: Option<SourceMetadata>,
    event_time_config: Option<crate::velostream::datasource::EventTimeConfig>,
}

impl FileMmapDataSource {
    pub fn new() -> Self {
        Self {
            config: None,
            metadata: None,
            event_time_config: None,
        }
    }

    /// Create from properties (used by the datasource resolution chain)
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let mut datasource = Self::new();

        let get_source_prop = |key: &str| {
            props
                .get(&format!("source.{}", key))
                .or_else(|| props.get(key))
                .cloned()
        };

        let path = get_source_prop("path").unwrap_or_else(|| "./data/input.csv".to_string());
        let format_str = get_source_prop("format").unwrap_or_else(|| "csv".to_string());
        let format = Self::parse_file_format(&format_str);

        let mut properties = HashMap::new();
        for (key, value) in props {
            properties.insert(key.clone(), value.clone());
        }

        let config = FileSourceConfig {
            path,
            format,
            watch_for_changes: false, // mmap is batch-only
            polling_interval_ms: None,
            csv_delimiter: get_source_prop("delimiter")
                .and_then(|v| v.chars().next())
                .unwrap_or(','),
            csv_has_header: get_source_prop("has_headers")
                .or_else(|| get_source_prop("header"))
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(true),
            properties,
            ..Default::default()
        };

        datasource.config = Some(config);
        datasource
    }

    fn parse_file_format(format_str: &str) -> FileFormat {
        match format_str.to_lowercase().as_str() {
            "json" => FileFormat::Json,
            "jsonlines" | "json_lines" => FileFormat::JsonLines,
            "csv_no_header" => FileFormat::CsvNoHeader,
            _ => FileFormat::Csv,
        }
    }

    /// Generate SourceConfig from current state
    pub fn to_source_config(&self) -> SourceConfig {
        if let Some(config) = &self.config {
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
                properties: HashMap::new(),
                batch_config: crate::velostream::datasource::BatchConfig::default(),
                event_time_config: None,
            }
        } else {
            SourceConfig::File {
                path: "./data/input.csv".to_string(),
                format: crate::velostream::datasource::config::FileFormat::Csv {
                    header: true,
                    delimiter: ',',
                    quote: '"',
                },
                properties: HashMap::new(),
                batch_config: crate::velostream::datasource::BatchConfig::default(),
                event_time_config: None,
            }
        }
    }

    /// Self-initialize with current configuration
    pub async fn self_initialize(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let config = self.to_source_config();
        self.initialize(config).await
    }

    /// Validate file path exists and is accessible
    async fn validate_path(&self, path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        use crate::velostream::table::retry_utils::{
            parse_duration, wait_for_file_to_exist, wait_for_pattern_match,
        };

        let path_obj = Path::new(path);

        let (wait_timeout, retry_interval) = if let Some(config) = &self.config {
            let default_timeout = "0s".to_string();
            let default_interval = "5s".to_string();

            let timeout_str = config
                .properties
                .get("file.wait.timeout")
                .or_else(|| config.properties.get("wait.timeout"))
                .unwrap_or(&default_timeout);
            let interval_str = config
                .properties
                .get("file.retry.interval")
                .or_else(|| config.properties.get("retry.interval"))
                .unwrap_or(&default_interval);

            let timeout = parse_duration(timeout_str).unwrap_or(Duration::from_secs(0));
            let interval = parse_duration(interval_str).unwrap_or(Duration::from_secs(5));
            (timeout, interval)
        } else {
            (Duration::from_secs(0), Duration::from_secs(5))
        };

        if path.contains('*') || path.contains('?') {
            match wait_for_pattern_match(path, wait_timeout, retry_interval).await {
                Ok(files) if !files.is_empty() => {
                    log::info!("Found {} files matching pattern '{}'", files.len(), path);
                    Ok(())
                }
                Ok(_) => Err(Box::new(FileDataSourceError::FileNotFound(format!(
                    "No files found matching pattern: {}",
                    path
                )))),
                Err(e) => Err(Box::new(FileDataSourceError::FileNotFound(e))),
            }
        } else {
            match wait_for_file_to_exist(path, wait_timeout, retry_interval).await {
                Ok(()) => {
                    if !path_obj.is_file() {
                        return Err(Box::new(FileDataSourceError::InvalidPath(format!(
                            "Path is not a file: {}",
                            path
                        ))));
                    }
                    std::fs::File::open(path_obj).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::PermissionDenied {
                            Box::new(FileDataSourceError::PermissionDenied(path.to_string()))
                        } else {
                            Box::new(FileDataSourceError::IoError(e.to_string()))
                        }
                    })?;
                    Ok(())
                }
                Err(e) => Err(Box::new(FileDataSourceError::FileNotFound(e))),
            }
        }
    }

    /// Infer schema from file content (used by future schema discovery features)
    #[allow(dead_code)]
    async fn infer_schema(
        &self,
        config: &FileSourceConfig,
    ) -> Result<Schema, Box<dyn Error + Send + Sync>> {
        use crate::velostream::schema::FieldDefinition;
        use crate::velostream::sql::ast::DataType;
        use std::io::{BufRead, BufReader};

        let file = std::fs::File::open(&config.path)
            .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
        let mut reader = BufReader::new(file);

        let mut fields = Vec::new();

        match config.format {
            FileFormat::Csv => {
                let mut line = String::new();
                reader
                    .read_line(&mut line)
                    .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                let columns: Vec<&str> = line.trim().split(config.csv_delimiter).collect();
                for (i, column) in columns.iter().enumerate() {
                    fields.push(FieldDefinition::new(
                        if column.is_empty() {
                            format!("column_{}", i)
                        } else {
                            column.to_string()
                        },
                        DataType::String,
                        true,
                    ));
                }
            }
            FileFormat::CsvNoHeader => {
                fields.push(FieldDefinition::new(
                    "column_0".to_string(),
                    DataType::String,
                    true,
                ));
            }
            FileFormat::JsonLines => {
                for line in reader.lines().take(1) {
                    let line = line.map_err(|e| FileDataSourceError::IoError(e.to_string()))?;
                    if !line.trim().is_empty() {
                        let json: serde_json::Value = serde_json::from_str(&line)
                            .map_err(|e| FileDataSourceError::JsonParseError(e.to_string()))?;
                        if let Some(obj) = json.as_object() {
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
                    }
                }
            }
            FileFormat::Json => {
                fields.push(FieldDefinition::new(
                    "data".to_string(),
                    DataType::String,
                    true,
                ));
            }
        }

        Ok(Schema::new(fields))
    }
}

impl Default for FileMmapDataSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataSource for FileMmapDataSource {
    async fn initialize(
        &mut self,
        config: SourceConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let event_time_config = match &config {
            SourceConfig::File {
                event_time_config, ..
            } => event_time_config.clone(),
            _ => None,
        };

        let file_config = FileSourceConfig::from_generic(&config).map_err(|e| {
            Box::new(FileDataSourceError::UnsupportedFormat(e)) as Box<dyn Error + Send + Sync>
        })?;

        file_config.validate().map_err(|e| {
            Box::new(FileDataSourceError::InvalidPath(e)) as Box<dyn Error + Send + Sync>
        })?;

        self.validate_path(&file_config.path).await?;
        self.event_time_config = event_time_config;
        self.config = Some(file_config);

        self.metadata = Some(SourceMetadata {
            source_type: "file_mmap".to_string(),
            version: "1.0".to_string(),
            supports_streaming: false,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec![
                "batch_read".to_string(),
                "mmap".to_string(),
                "schema_inference".to_string(),
            ],
        });

        Ok(())
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileMmapDataSource not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        self.infer_schema(config).await
    }

    async fn create_reader(&self) -> Result<Box<dyn DataReader>, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileMmapDataSource not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let reader = FileMmapReader::new(
            config.clone(),
            crate::velostream::datasource::BatchConfig::default(),
            self.event_time_config.clone(),
        )
        .await?;
        Ok(Box::new(reader))
    }

    async fn create_reader_with_batch_config(
        &self,
        batch_config: crate::velostream::datasource::BatchConfig,
    ) -> Result<Box<dyn DataReader>, Box<dyn Error + Send + Sync>> {
        let config = self.config.as_ref().ok_or_else(|| {
            Box::new(FileDataSourceError::InvalidPath(
                "FileMmapDataSource not initialized".to_string(),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let reader =
            FileMmapReader::new(config.clone(), batch_config, self.event_time_config.clone())
                .await?;
        Ok(Box::new(reader))
    }

    fn supports_streaming(&self) -> bool {
        false // mmap is batch-only
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn partition_count(&self) -> Option<usize> {
        Some(1)
    }

    fn metadata(&self) -> SourceMetadata {
        self.metadata.clone().unwrap_or_else(|| SourceMetadata {
            source_type: "file_mmap".to_string(),
            version: "1.0".to_string(),
            supports_streaming: false,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec!["batch_read".to_string(), "mmap".to_string()],
        })
    }
}

impl ConfigSchemaProvider for FileMmapDataSource {
    fn config_type_id() -> &'static str {
        "file_source_mmap"
    }

    fn inheritable_properties() -> Vec<&'static str> {
        vec!["buffer_size", "batch.size", "batch.timeout_ms"]
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
        defaults.insert(
            "max_records",
            PropertyDefault::GlobalLookup("file.max_records".to_string()),
        );
        defaults
    }

    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "path" => {
                if value.is_empty() {
                    return Err(vec!["path cannot be empty".to_string()]);
                }
                if value.contains("..") && !value.contains("*") {
                    return Err(vec![
                        "path contains '..' which could be a security risk".to_string(),
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
            }
            "has_headers" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec![format!(
                        "has_headers must be 'true' or 'false'. Got: '{}'",
                        value
                    )]);
                }
            }
            "max_records" => {
                if let Ok(max) = value.parse::<usize>() {
                    if max == 0 {
                        return Err(vec!["max_records must be greater than 0".to_string()]);
                    }
                } else {
                    return Err(vec![
                        "max_records must be a valid positive number".to_string(),
                    ]);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn json_schema() -> Value {
        serde_json::json!({
            "type": "object",
            "title": "Memory-Mapped File Data Source Configuration Schema",
            "description": "Configuration schema for mmap-based file data sources (batch-only, high-throughput)",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path or glob pattern for source files"
                },
                "format": {
                    "type": "string",
                    "enum": ["csv", "csv_no_header", "json", "jsonlines"],
                    "default": "csv",
                    "description": "File format to parse"
                },
                "delimiter": {
                    "type": "string",
                    "maxLength": 1,
                    "default": ",",
                    "description": "CSV field delimiter character"
                },
                "has_headers": {
                    "type": "boolean",
                    "default": true,
                    "description": "Whether CSV files have header rows"
                },
                "max_records": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum number of records to read"
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
                validation_pattern: Some("csv|csv_no_header|json|jsonlines".to_string()),
            },
            PropertyValidation {
                key: "delimiter".to_string(),
                required: false,
                default: Some(PropertyDefault::Static(",".to_string())),
                description: "CSV field delimiter character".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("^.$".to_string()),
            },
        ]
    }

    fn supports_custom_properties() -> bool {
        true
    }

    fn global_schema_dependencies() -> Vec<&'static str> {
        vec!["batch_config"]
    }

    fn resolve_property_with_inheritance(
        &self,
        key: &str,
        local_value: Option<&str>,
        global_context: &GlobalSchemaContext,
    ) -> Result<Option<String>, String> {
        if let Some(value) = local_value {
            return Ok(Some(value.to_string()));
        }
        if let Some(global_value) = global_context.global_properties.get(key) {
            return Ok(Some(global_value.clone()));
        }
        Ok(None)
    }

    fn schema_version() -> &'static str {
        "1.0.0"
    }
}
