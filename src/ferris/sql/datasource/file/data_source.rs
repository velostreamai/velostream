//! File Data Source Implementation

use crate::ferris::sql::datasource::config::SourceConfig;
use crate::ferris::sql::datasource::traits::{DataReader, DataSource};
use crate::ferris::sql::datasource::types::SourceMetadata;
use crate::ferris::schema::Schema;
use async_trait::async_trait;
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
                use crate::ferris::sql::ast::DataType;
                use crate::ferris::schema::FieldDefinition;

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
            use crate::ferris::sql::ast::DataType;
            use crate::ferris::schema::FieldDefinition;

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
                            use crate::ferris::sql::ast::DataType;
                            use crate::ferris::schema::FieldDefinition;

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
                use crate::ferris::sql::ast::DataType;
                use crate::ferris::schema::FieldDefinition;

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
        let file_config = match config {
            SourceConfig::File(config) => config,
            _ => {
                return Err(Box::new(FileDataSourceError::UnsupportedFormat(
                    "Expected File source configuration".to_string(),
                )))
            }
        };

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
