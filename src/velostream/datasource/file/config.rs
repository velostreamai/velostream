//! File Data Source Configuration

use crate::velostream::datasource::config::{BatchConfig, SourceConfig};
use serde::{Deserialize, Serialize};

/// Supported file formats for file data sources
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum FileFormat {
    /// CSV files with header row
    #[default]
    Csv,
    /// CSV files without header row (schema must be provided)
    CsvNoHeader,
    /// JSON Lines format (newline-delimited JSON)
    JsonLines,
    /// Single JSON array file
    Json,
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormat::Csv => write!(f, "csv"),
            FileFormat::CsvNoHeader => write!(f, "csv_no_header"),
            FileFormat::JsonLines => write!(f, "jsonl"),
            FileFormat::Json => write!(f, "json"),
        }
    }
}

impl std::str::FromStr for FileFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(FileFormat::Csv),
            "csv_no_header" | "csvnoheader" => Ok(FileFormat::CsvNoHeader),
            "jsonl" | "jsonlines" | "json_lines" => Ok(FileFormat::JsonLines),
            "json" => Ok(FileFormat::Json),
            _ => Err(format!("Unknown file format: {}", s)),
        }
    }
}

/// Configuration for file-based data sources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSourceConfig {
    /// File path or glob pattern (e.g., "/data/*.csv", "/logs/app-*.json")
    pub path: String,

    /// File format
    pub format: FileFormat,

    /// Whether to watch for file changes and new files
    pub watch_for_changes: bool,

    /// Polling interval for file watching (milliseconds)
    /// If None, uses system default
    pub polling_interval_ms: Option<u64>,

    /// CSV delimiter character
    pub csv_delimiter: char,

    /// CSV quote character
    pub csv_quote: char,

    /// CSV escape character
    pub csv_escape: Option<char>,

    /// Whether CSV has header row
    pub csv_has_header: bool,

    /// Skip first N lines in files
    pub skip_lines: usize,

    /// Maximum number of records to read (None = unlimited)
    pub max_records: Option<usize>,

    /// Buffer size for reading files (bytes)
    pub buffer_size: usize,

    /// Whether to recursively search directories in glob patterns
    pub recursive: bool,

    /// File extension filter for directory scanning
    pub extension_filter: Option<String>,

    /// Batch configuration for reading records
    pub batch_config: Option<BatchConfig>,

    /// Additional properties for file processing (e.g., retry configuration)
    pub properties: std::collections::HashMap<String, String>,
}

impl Default for FileSourceConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            format: FileFormat::Csv,
            watch_for_changes: false,
            polling_interval_ms: Some(1000),
            csv_delimiter: ',',
            csv_quote: '"',
            csv_escape: None,
            csv_has_header: true,
            skip_lines: 0,
            max_records: None,
            buffer_size: 8192,
            recursive: false,
            extension_filter: None,
            batch_config: None,
            properties: std::collections::HashMap::new(),
        }
    }
}

impl FileSourceConfig {
    /// Create a new file source configuration with required parameters
    pub fn new(path: String, format: FileFormat) -> Self {
        Self {
            path,
            format,
            ..Default::default()
        }
    }

    /// Create from generic SourceConfig::File
    pub fn from_generic(config: &SourceConfig) -> Result<Self, String> {
        if let SourceConfig::File {
            path,
            format,
            properties,
            ..
        } = config
        {
            let mut file_config = Self::new(path.clone(), FileFormat::Csv);

            // Convert generic FileFormat back to local FileFormat
            match format {
                crate::velostream::datasource::config::FileFormat::Csv {
                    header,
                    delimiter,
                    quote,
                } => {
                    file_config.format = if *header {
                        FileFormat::Csv
                    } else {
                        FileFormat::CsvNoHeader
                    };
                    file_config.csv_delimiter = *delimiter;
                    file_config.csv_quote = *quote;
                    file_config.csv_has_header = *header;
                }
                crate::velostream::datasource::config::FileFormat::Json => {
                    file_config.format = FileFormat::JsonLines;
                }
                _ => {
                    return Err(format!("Unsupported file format: {}", format));
                }
            }

            // Parse properties back
            for (key, value) in properties {
                match key.as_str() {
                    "watch_for_changes" => {
                        file_config.watch_for_changes = value.parse().unwrap_or(false)
                    }
                    "polling_interval_ms" => file_config.polling_interval_ms = value.parse().ok(),
                    "csv_delimiter" => {
                        if let Some(c) = value.chars().next() {
                            file_config.csv_delimiter = c;
                        }
                    }
                    "csv_quote" => {
                        if let Some(c) = value.chars().next() {
                            file_config.csv_quote = c;
                        }
                    }
                    "csv_escape" => {
                        if let Some(c) = value.chars().next() {
                            file_config.csv_escape = Some(c);
                        }
                    }
                    "csv_has_header" => file_config.csv_has_header = value.parse().unwrap_or(true),
                    "skip_lines" => file_config.skip_lines = value.parse().unwrap_or(0),
                    "max_records" => file_config.max_records = value.parse().ok(),
                    "buffer_size" => file_config.buffer_size = value.parse().unwrap_or(8192),
                    "recursive" => file_config.recursive = value.parse().unwrap_or(false),
                    "extension_filter" => file_config.extension_filter = Some(value.clone()),
                    // Store all properties including retry configuration
                    _ => {
                        file_config.properties.insert(key.clone(), value.clone());
                    }
                }
            }

            Ok(file_config)
        } else {
            Err("Expected File source configuration".to_string())
        }
    }

    /// Enable file watching with optional polling interval
    pub fn with_watching(mut self, polling_interval_ms: Option<u64>) -> Self {
        self.watch_for_changes = true;
        self.polling_interval_ms = polling_interval_ms;
        self
    }

    /// Configure CSV parsing options
    pub fn with_csv_options(
        mut self,
        delimiter: char,
        quote: char,
        escape: Option<char>,
        has_header: bool,
    ) -> Self {
        self.csv_delimiter = delimiter;
        self.csv_quote = quote;
        self.csv_escape = escape;
        self.csv_has_header = has_header;
        self
    }

    /// Set buffer size for file reading
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Enable recursive directory scanning
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.path.is_empty() {
            return Err("File path cannot be empty".to_string());
        }

        if self.buffer_size == 0 {
            return Err("Buffer size must be greater than 0".to_string());
        }

        if let Some(interval) = self.polling_interval_ms {
            if interval == 0 {
                return Err("Polling interval must be greater than 0".to_string());
            }
        }

        // Validate CSV configuration
        match self.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => {
                if self.csv_delimiter == self.csv_quote {
                    return Err("CSV delimiter and quote character cannot be the same".to_string());
                }
                if let Some(escape) = self.csv_escape {
                    if escape == self.csv_delimiter || escape == self.csv_quote {
                        return Err(
                            "CSV escape character cannot be the same as delimiter or quote"
                                .to_string(),
                        );
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Parse configuration from URI format
    /// Examples:
    /// - "file:///data/orders.csv"
    /// - "file:///data/orders.csv?format=csv&watch=true&delimiter=;"
    /// - "file:///logs/*.jsonl?format=jsonl&watch=true&poll_interval=500"
    pub fn from_uri(uri: &str) -> Result<Self, String> {
        if !uri.starts_with("file://") {
            return Err("File URI must start with 'file://'".to_string());
        }

        let uri_without_scheme = &uri[7..]; // Remove "file://"
        let (path, query) = if let Some(pos) = uri_without_scheme.find('?') {
            (&uri_without_scheme[..pos], &uri_without_scheme[pos + 1..])
        } else {
            (uri_without_scheme, "")
        };

        let mut config = FileSourceConfig::new(path.to_string(), FileFormat::Csv);

        // Auto-detect format from file extension if not specified
        if let Some(ext) = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
        {
            config.format = match ext.to_lowercase().as_str() {
                "csv" => FileFormat::Csv,
                "json" => FileFormat::Json,
                "jsonl" => FileFormat::JsonLines,
                _ => FileFormat::Csv,
            };
        }

        // Parse query parameters
        for param in query.split('&') {
            if param.is_empty() {
                continue;
            }

            let parts: Vec<&str> = param.splitn(2, '=').collect();
            if parts.len() != 2 {
                continue;
            }

            let key = parts[0];
            let value = parts[1];

            match key {
                "format" => {
                    config.format = value
                        .parse()
                        .map_err(|e| format!("Invalid format parameter: {}", e))?;
                }
                "watch" => {
                    config.watch_for_changes = value.parse().unwrap_or(false);
                }
                "poll_interval" => {
                    config.polling_interval_ms = value.parse().ok();
                }
                "delimiter" => {
                    if value.len() == 1 {
                        config.csv_delimiter = value.chars().next().unwrap();
                    }
                }
                "quote" => {
                    if value.len() == 1 {
                        config.csv_quote = value.chars().next().unwrap();
                    }
                }
                "header" => {
                    config.csv_has_header = value.parse().unwrap_or(true);
                }
                "skip_lines" => {
                    config.skip_lines = value.parse().unwrap_or(0);
                }
                "max_records" => {
                    config.max_records = value.parse().ok();
                }
                "buffer_size" => {
                    config.buffer_size = value.parse().unwrap_or(8192);
                }
                "recursive" => {
                    config.recursive = value.parse().unwrap_or(false);
                }
                _ => {
                    // Ignore unknown parameters
                }
            }
        }

        config.validate()?;
        Ok(config)
    }
}

impl From<FileSourceConfig> for SourceConfig {
    fn from(config: FileSourceConfig) -> Self {
        let mut properties = std::collections::HashMap::new();

        // Convert FileSourceConfig to generic File format
        properties.insert(
            "watch_for_changes".to_string(),
            config.watch_for_changes.to_string(),
        );
        if let Some(interval) = config.polling_interval_ms {
            properties.insert("polling_interval_ms".to_string(), interval.to_string());
        }
        properties.insert(
            "csv_delimiter".to_string(),
            config.csv_delimiter.to_string(),
        );
        properties.insert("csv_quote".to_string(), config.csv_quote.to_string());
        if let Some(escape) = config.csv_escape {
            properties.insert("csv_escape".to_string(), escape.to_string());
        }
        properties.insert(
            "csv_has_header".to_string(),
            config.csv_has_header.to_string(),
        );
        properties.insert("skip_lines".to_string(), config.skip_lines.to_string());
        if let Some(max) = config.max_records {
            properties.insert("max_records".to_string(), max.to_string());
        }
        properties.insert("buffer_size".to_string(), config.buffer_size.to_string());
        properties.insert("recursive".to_string(), config.recursive.to_string());
        if let Some(ext) = config.extension_filter {
            properties.insert("extension_filter".to_string(), ext);
        }

        // Map FileFormat to generic FileFormat
        let generic_format = match config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => {
                crate::velostream::datasource::config::FileFormat::Csv {
                    header: config.csv_has_header,
                    delimiter: config.csv_delimiter,
                    quote: config.csv_quote,
                }
            }
            FileFormat::JsonLines | FileFormat::Json => {
                crate::velostream::datasource::config::FileFormat::Json
            }
        };

        SourceConfig::File {
            path: config.path,
            format: generic_format,
            properties,
            batch_config: Default::default(),
            event_time_config: None,
        }
    }
}

/// Configuration for file-based data sinks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSinkConfig {
    /// Output file path or pattern (supports strftime formatting for rotation)
    pub path: String,

    /// File format for output
    pub format: FileFormat,

    /// Whether to append to existing files or overwrite
    pub append_if_exists: bool,

    /// Buffer size for writing files (bytes)
    pub buffer_size_bytes: u64,

    /// Maximum file size before rotation (bytes)
    pub max_file_size_bytes: Option<u64>,

    /// Time interval between rotations (milliseconds)
    pub rotation_interval_ms: Option<u64>,

    /// Maximum records per file before rotation
    pub max_records_per_file: Option<u64>,

    /// Compression type (if any)
    pub compression: Option<CompressionType>,

    /// CSV delimiter character
    pub csv_delimiter: String,

    /// Whether to write CSV header
    pub csv_has_header: bool,

    /// Number of writer threads for parallel writing
    pub writer_threads: usize,
}

/// Compression types supported for file outputs
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Zstd,
}

impl Default for FileSinkConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            format: FileFormat::JsonLines,
            append_if_exists: false,
            buffer_size_bytes: 65536, // 64KB
            max_file_size_bytes: None,
            rotation_interval_ms: None,
            max_records_per_file: None,
            compression: None,
            csv_delimiter: ",".to_string(),
            csv_has_header: true,
            writer_threads: 1,
        }
    }
}

impl FileSinkConfig {
    /// Create a new file sink configuration
    pub fn new(path: String, format: FileFormat) -> Self {
        Self {
            path,
            format,
            ..Default::default()
        }
    }

    /// Create from generic SinkConfig
    pub fn from_generic(
        config: &crate::velostream::datasource::config::SinkConfig,
    ) -> Result<Self, String> {
        if let crate::velostream::datasource::config::SinkConfig::File {
            path,
            format,
            compression: _,
            properties,
        } = config
        {
            let mut sink_config = Self::new(path.clone(), FileFormat::JsonLines);

            // Convert generic FileFormat to local FileFormat
            match format {
                crate::velostream::datasource::config::FileFormat::Csv {
                    header,
                    delimiter,
                    ..
                } => {
                    sink_config.format = if *header {
                        FileFormat::Csv
                    } else {
                        FileFormat::CsvNoHeader
                    };
                    sink_config.csv_delimiter = delimiter.to_string();
                    sink_config.csv_has_header = *header;
                }
                crate::velostream::datasource::config::FileFormat::Json => {
                    sink_config.format = FileFormat::JsonLines;
                }
                _ => {
                    return Err(format!("Unsupported file format for sink: {}", format));
                }
            }

            // Parse properties
            for (key, value) in properties {
                match key.as_str() {
                    "append_if_exists" => {
                        sink_config.append_if_exists = value.parse().unwrap_or(false)
                    }
                    "buffer_size_bytes" => {
                        sink_config.buffer_size_bytes = value.parse().unwrap_or(65536)
                    }
                    "max_file_size_bytes" => sink_config.max_file_size_bytes = value.parse().ok(),
                    "rotation_interval_ms" => sink_config.rotation_interval_ms = value.parse().ok(),
                    "max_records_per_file" => sink_config.max_records_per_file = value.parse().ok(),
                    "compression" => {
                        sink_config.compression = match value.as_str() {
                            "none" | "" => None,
                            "gzip" => Some(CompressionType::Gzip),
                            "snappy" => Some(CompressionType::Snappy),
                            "zstd" => Some(CompressionType::Zstd),
                            _ => None,
                        };
                    }
                    "csv_delimiter" => sink_config.csv_delimiter = value.clone(),
                    "csv_has_header" => sink_config.csv_has_header = value.parse().unwrap_or(true),
                    "writer_threads" => sink_config.writer_threads = value.parse().unwrap_or(1),
                    _ => {} // Ignore unknown properties
                }
            }

            Ok(sink_config)
        } else {
            Err("Expected File sink configuration".to_string())
        }
    }

    /// Enable file rotation with size limit
    pub fn with_rotation_size(mut self, max_bytes: u64) -> Self {
        self.max_file_size_bytes = Some(max_bytes);
        self
    }

    /// Enable file rotation with time interval
    pub fn with_rotation_interval(mut self, interval_ms: u64) -> Self {
        self.rotation_interval_ms = Some(interval_ms);
        self
    }

    /// Enable compression
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.path.is_empty() {
            return Err("File path cannot be empty".to_string());
        }

        if self.buffer_size_bytes == 0 {
            return Err("Buffer size must be greater than 0".to_string());
        }

        if let Some(max_size) = self.max_file_size_bytes {
            if max_size == 0 {
                return Err("Max file size must be greater than 0".to_string());
            }
        }

        if let Some(interval) = self.rotation_interval_ms {
            if interval == 0 {
                return Err("Rotation interval must be greater than 0".to_string());
            }
        }

        if self.writer_threads == 0 {
            return Err("Must have at least 1 writer thread".to_string());
        }

        Ok(())
    }
}

impl From<FileSinkConfig> for crate::velostream::datasource::config::SinkConfig {
    fn from(config: FileSinkConfig) -> Self {
        let mut properties = std::collections::HashMap::new();

        properties.insert(
            "append_if_exists".to_string(),
            config.append_if_exists.to_string(),
        );
        properties.insert(
            "buffer_size_bytes".to_string(),
            config.buffer_size_bytes.to_string(),
        );
        if let Some(size) = config.max_file_size_bytes {
            properties.insert("max_file_size_bytes".to_string(), size.to_string());
        }
        if let Some(interval) = config.rotation_interval_ms {
            properties.insert("rotation_interval_ms".to_string(), interval.to_string());
        }
        if let Some(max) = config.max_records_per_file {
            properties.insert("max_records_per_file".to_string(), max.to_string());
        }
        if let Some(compression) = config.compression {
            let compression_str = match compression {
                CompressionType::None => "none",
                CompressionType::Gzip => "gzip",
                CompressionType::Snappy => "snappy",
                CompressionType::Zstd => "zstd",
            };
            properties.insert("compression".to_string(), compression_str.to_string());
        }
        properties.insert("csv_delimiter".to_string(), config.csv_delimiter.clone());
        properties.insert(
            "csv_has_header".to_string(),
            config.csv_has_header.to_string(),
        );
        properties.insert(
            "writer_threads".to_string(),
            config.writer_threads.to_string(),
        );

        // Map FileFormat to generic FileFormat
        let generic_format = match config.format {
            FileFormat::Csv | FileFormat::CsvNoHeader => {
                crate::velostream::datasource::config::FileFormat::Csv {
                    header: config.csv_has_header,
                    delimiter: config.csv_delimiter.chars().next().unwrap_or(','),
                    quote: '"',
                }
            }
            FileFormat::JsonLines | FileFormat::Json => {
                crate::velostream::datasource::config::FileFormat::Json
            }
        };

        crate::velostream::datasource::config::SinkConfig::File {
            path: config.path,
            format: generic_format,
            compression: None, // Default to no compression
            properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_format_parsing() {
        assert_eq!("csv".parse::<FileFormat>().unwrap(), FileFormat::Csv);
        assert_eq!(
            "jsonl".parse::<FileFormat>().unwrap(),
            FileFormat::JsonLines
        );
        assert_eq!("json".parse::<FileFormat>().unwrap(), FileFormat::Json);
        assert!("invalid".parse::<FileFormat>().is_err());
    }

    #[test]
    fn test_config_validation() {
        let mut config = FileSourceConfig::default();

        // Empty path should fail
        assert!(config.validate().is_err());

        // Valid config should pass
        config.path = "/data/test.csv".to_string();
        assert!(config.validate().is_ok());

        // Invalid delimiter/quote combination should fail
        config.csv_delimiter = '"';
        config.csv_quote = '"';
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_uri_parsing() {
        let config =
            FileSourceConfig::from_uri("file:///data/orders.csv?format=csv&watch=true&delimiter=;")
                .unwrap();
        assert_eq!(config.path, "/data/orders.csv");
        assert_eq!(config.format, FileFormat::Csv);
        assert!(config.watch_for_changes);
        assert_eq!(config.csv_delimiter, ';');

        let config = FileSourceConfig::from_uri("file:///logs/app.jsonl").unwrap();
        assert_eq!(config.path, "/logs/app.jsonl");
        assert_eq!(config.format, FileFormat::JsonLines);
    }
}
