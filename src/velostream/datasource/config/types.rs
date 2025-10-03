//! Generic configuration system for data sources
//!
//! This module provides generic configuration abstractions that are independent
//! of SQL or any specific query engine. These can be used by any data processing system.

use crate::velostream::config::{
    ConfigSchemaProvider, GlobalSchemaContext, PropertyDefault, PropertyValidation,
};
use crate::velostream::datasource::DataSourceError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// Batch processing strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BatchStrategy {
    /// Fixed number of records per batch
    FixedSize(usize),
    /// Time-based batching (collect records for specified duration)
    TimeWindow(#[serde(with = "duration_serde")] Duration),
    /// Adaptive batching based on processing latency
    AdaptiveSize {
        min_size: usize,
        max_size: usize,
        #[serde(with = "duration_serde")]
        target_latency: Duration,
    },
    /// Memory-based batching (approximate bytes)
    MemoryBased(usize),
    /// Low-latency batching optimized for minimal delay
    /// Prioritizes speed over throughput with very small batches and aggressive timeouts
    LowLatency {
        max_batch_size: usize, // Small batch size (e.g., 1-10 records)
        #[serde(with = "duration_serde")]
        max_wait_time: Duration, // Aggressive timeout (e.g., 1-5ms)
        eager_processing: bool, // Process immediately when any record is available
    },
    /// High-throughput mega-batch processing (NEW - Phase 4 optimization)
    /// Optimized for maximum throughput with large batch sizes and optional parallelism
    MegaBatch {
        batch_size: usize, // Large batch size (10K-100K records)
        parallel: bool, // Enable parallel batch processing
        reuse_buffer: bool, // Use ring buffer for allocation reuse
    },
}

impl Default for BatchStrategy {
    fn default() -> Self {
        BatchStrategy::FixedSize(100)
    }
}

/// Batch processing configuration for datasources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub strategy: BatchStrategy,
    pub max_batch_size: usize, // Hard limit to prevent memory issues
    #[serde(with = "duration_serde")]
    pub batch_timeout: Duration, // Maximum time to wait for batch to fill
    pub enable_batching: bool, // Allow disabling batching per job
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            strategy: BatchStrategy::default(),
            max_batch_size: 50_000, // Increased from 1000 to 50K (Phase 4 optimization)
            batch_timeout: Duration::from_millis(1000),
            enable_batching: true,
        }
    }
}

impl BatchConfig {
    /// Create a high-throughput configuration optimized for maximum throughput
    /// Uses large batch sizes with optional parallel processing
    pub fn high_throughput() -> Self {
        Self {
            strategy: BatchStrategy::MegaBatch {
                batch_size: 50_000,
                parallel: true,
                reuse_buffer: true,
            },
            max_batch_size: 100_000,
            batch_timeout: Duration::from_millis(100),
            enable_batching: true,
        }
    }

    /// Create an ultra-high-throughput configuration for maximum performance
    /// Uses 100K batch sizes with parallel processing and buffer reuse
    pub fn ultra_throughput() -> Self {
        Self {
            strategy: BatchStrategy::MegaBatch {
                batch_size: 100_000,
                parallel: true,
                reuse_buffer: true,
            },
            max_batch_size: 100_000,
            batch_timeout: Duration::from_millis(50),
            enable_batching: true,
        }
    }
}

/// Helper module for Duration serialization
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        duration.as_millis().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

/// Generic configuration for data sources (inputs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceConfig {
    /// Kafka source configuration
    Kafka {
        brokers: String,
        topic: String,
        group_id: Option<String>,
        /// Serialization format for keys and values
        /// Expected keys: "key.serializer", "value.serializer"
        /// Expected values: "json", "avro", "protobuf"
        properties: HashMap<String, String>,
        /// Batch processing configuration
        #[serde(default)]
        batch_config: BatchConfig,
    },
    /// File system source configuration  
    File {
        path: String,
        format: FileFormat,
        properties: HashMap<String, String>,
        /// Batch processing configuration
        #[serde(default)]
        batch_config: BatchConfig,
    },
    /// S3 source configuration
    S3 {
        bucket: String,
        prefix: String,
        region: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        format: FileFormat,
        properties: HashMap<String, String>,
        /// Batch processing configuration
        #[serde(default)]
        batch_config: BatchConfig,
    },
    /// Database source configuration (for CDC)
    Database {
        connection_string: String,
        tables: Vec<String>,
        cdc_format: CdcFormat,
        properties: HashMap<String, String>,
        /// Batch processing configuration
        #[serde(default)]
        batch_config: BatchConfig,
    },
    /// ClickHouse source configuration
    ClickHouse {
        connection_string: String,
        database: String,
        table: String,
        query: Option<String>,
        properties: HashMap<String, String>,
        /// Batch processing configuration
        #[serde(default)]
        batch_config: BatchConfig,
    },
    /// Generic configuration for custom sources
    Generic {
        source_type: String,
        properties: HashMap<String, String>,
        /// Batch processing configuration
        #[serde(default)]
        batch_config: BatchConfig,
    },
}

/// Generic configuration for data sinks (outputs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SinkConfig {
    /// Kafka sink configuration
    Kafka {
        brokers: String,
        topic: String,
        properties: HashMap<String, String>,
    },
    /// File system sink configuration
    File {
        path: String,
        format: FileFormat,
        compression: Option<CompressionType>,
        properties: HashMap<String, String>,
    },
    /// S3 sink configuration
    S3 {
        bucket: String,
        prefix: String,
        region: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        format: FileFormat,
        compression: Option<CompressionType>,
        properties: HashMap<String, String>,
    },
    /// Database sink configuration
    Database {
        connection_string: String,
        table: String,
        mode: WriteMode,
        properties: HashMap<String, String>,
    },
    /// ClickHouse sink configuration
    ClickHouse {
        connection_string: String,
        database: String,
        table: String,
        properties: HashMap<String, String>,
    },
    /// Delta Lake sink configuration
    Delta {
        table_path: String,
        mode: WriteMode,
        properties: HashMap<String, String>,
    },
    /// Iceberg table sink configuration
    Iceberg {
        catalog_uri: String,
        warehouse: String,
        namespace: String,
        table: String,
        properties: HashMap<String, String>,
    },
    /// Generic configuration for custom sinks
    Generic {
        sink_type: String,
        properties: HashMap<String, String>,
    },
}

/// Supported file formats
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum FileFormat {
    /// JSON format (single JSON object)
    #[default]
    Json,
    /// JSON Lines format (one JSON object per line)
    JsonLines,
    /// CSV format with optional header
    Csv {
        header: bool,
        delimiter: char,
        quote: char,
    },
    /// Parquet columnar format
    Parquet,
    /// Apache Avro format
    Avro,
    /// ORC format
    Orc,
    /// Custom format with properties
    Custom {
        format_name: String,
        properties: HashMap<String, String>,
    },
}

/// Compression types for file formats
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Write modes for sinks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WriteMode {
    Append,
    Overwrite,
    Upsert,
    Merge,
}

/// Change Data Capture formats
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CdcFormat {
    /// Debezium CDC format
    Debezium,
    /// Maxwell CDC format
    Maxwell,
    /// Custom CDC format
    Custom {
        format_name: String,
        properties: HashMap<String, String>,
    },
}

impl fmt::Display for FileFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileFormat::Json => write!(f, "json"),
            FileFormat::JsonLines => write!(f, "jsonlines"),
            FileFormat::Csv { .. } => write!(f, "csv"),
            FileFormat::Parquet => write!(f, "parquet"),
            FileFormat::Avro => write!(f, "avro"),
            FileFormat::Orc => write!(f, "orc"),
            FileFormat::Custom { format_name, .. } => write!(f, "{}", format_name),
        }
    }
}

impl fmt::Display for CdcFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CdcFormat::Debezium => write!(f, "debezium"),
            CdcFormat::Maxwell => write!(f, "maxwell"),
            CdcFormat::Custom { format_name, .. } => write!(f, "{}", format_name),
        }
    }
}

/// Connection string parser for URI-based configuration
#[derive(Debug, Clone)]
pub struct ConnectionString {
    pub scheme: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub path: String,
    pub params: HashMap<String, String>,
}

impl ConnectionString {
    /// Parse a URI into a ConnectionString
    ///
    /// Examples:
    /// - `kafka://localhost:9092/orders?group_id=analytics`
    /// - `s3://bucket/path/*.parquet?region=us-west-2&compression=snappy`
    /// - `file:///data/input.json?watch=true&format=json`
    /// - `iceberg://catalog.namespace.table?warehouse=s3://warehouse/`
    pub fn parse(uri: &str) -> Result<Self, DataSourceError> {
        // Simple URI parsing - in production, use a proper URI parser
        let parts: Vec<&str> = uri.splitn(2, "://").collect();
        if parts.len() != 2 {
            return Err(DataSourceError::Configuration(format!(
                "Invalid URI format: {}",
                uri
            )));
        }

        let scheme = parts[0].to_string();
        let remainder = parts[1];

        // Split path and query parameters
        let (path_part, query_part) = match remainder.find('?') {
            Some(pos) => (&remainder[..pos], Some(&remainder[pos + 1..])),
            None => (remainder, None),
        };

        // Extract host:port and path based on scheme
        let (host, port, path) = if path_part.starts_with('/') {
            // File path: file:///path/to/file
            (None, None, path_part.to_string())
        } else if scheme == "s3" {
            // S3: s3://bucket/path/*.parquet -> path = bucket/path/*.parquet
            (None, None, path_part.to_string())
        } else if path_part.contains('/') {
            // Host with path: kafka://localhost:9092/topic
            let host_path: Vec<&str> = path_part.splitn(2, '/').collect();
            let host_port = host_path[0];
            let path = format!("/{}", host_path[1]);

            let (host, port) = if host_port.contains(':') {
                let hp: Vec<&str> = host_port.splitn(2, ':').collect();
                let host = Some(hp[0].to_string());
                let port = hp[1].parse::<u16>().ok();
                (host, port)
            } else {
                (Some(host_port.to_string()), None)
            };

            (host, port, path)
        } else {
            // Host only or special format: iceberg://catalog.namespace.table
            (None, None, path_part.to_string())
        };

        // Parse query parameters
        let mut params = HashMap::new();
        if let Some(query) = query_part {
            for param in query.split('&') {
                let kv: Vec<&str> = param.splitn(2, '=').collect();
                if kv.len() == 2 {
                    params.insert(kv[0].to_string(), kv[1].to_string());
                }
            }
        }

        Ok(ConnectionString {
            scheme,
            host,
            port,
            path,
            params,
        })
    }

    /// Convert to a source configuration
    pub fn to_source_config(&self) -> Result<SourceConfig, DataSourceError> {
        match self.scheme.as_str() {
            "kafka" => {
                let brokers = match (&self.host, self.port) {
                    (Some(host), Some(port)) => format!("{}:{}", host, port),
                    (Some(host), None) => format!("{}:9092", host),
                    _ => {
                        return Err(DataSourceError::Configuration(
                            "Kafka source requires host".to_string(),
                        ));
                    }
                };

                let topic = self.path.trim_start_matches('/').to_string();
                if topic.is_empty() {
                    return Err(DataSourceError::Configuration(
                        "Kafka source requires topic".to_string(),
                    ));
                }

                Ok(SourceConfig::Kafka {
                    brokers,
                    topic,
                    group_id: self.params.get("group_id").cloned(),
                    properties: self.params.clone(),
                    batch_config: Default::default(),
                })
            }
            "file" => {
                let format = match self.params.get("format").map(|s| s.as_str()) {
                    Some("json") => FileFormat::Json,
                    Some("jsonl") | Some("jsonlines") => FileFormat::JsonLines,
                    Some("csv") => FileFormat::Csv {
                        header: self
                            .params
                            .get("header")
                            .map(|h| h.parse().unwrap_or(true))
                            .unwrap_or(true),
                        delimiter: self
                            .params
                            .get("delimiter")
                            .and_then(|d| d.chars().next())
                            .unwrap_or(','),
                        quote: self
                            .params
                            .get("quote")
                            .and_then(|q| q.chars().next())
                            .unwrap_or('"'),
                    },
                    Some("parquet") => FileFormat::Parquet,
                    Some("avro") => FileFormat::Avro,
                    Some("orc") => FileFormat::Orc,
                    _ => FileFormat::Json, // Default
                };

                Ok(SourceConfig::File {
                    path: self.path.clone(),
                    format,
                    properties: self.params.clone(),
                    batch_config: Default::default(),
                })
            }
            "s3" => {
                // Parse s3://bucket/prefix
                let path_parts: Vec<&str> =
                    self.path.trim_start_matches('/').splitn(2, '/').collect();

                if path_parts.is_empty() {
                    return Err(DataSourceError::Configuration(
                        "S3 source requires bucket".to_string(),
                    ));
                }

                let bucket = path_parts[0].to_string();
                let prefix = if path_parts.len() > 1 {
                    path_parts[1].to_string()
                } else {
                    String::new()
                };

                let format = self
                    .params
                    .get("format")
                    .map(|f| match f.as_str() {
                        "json" => FileFormat::Json,
                        "jsonlines" => FileFormat::JsonLines,
                        "csv" => FileFormat::Csv {
                            header: true,
                            delimiter: ',',
                            quote: '"',
                        },
                        "parquet" => FileFormat::Parquet,
                        "avro" => FileFormat::Avro,
                        "orc" => FileFormat::Orc,
                        _ => FileFormat::Parquet, // Default for S3
                    })
                    .unwrap_or(FileFormat::Parquet);

                Ok(SourceConfig::S3 {
                    bucket,
                    prefix,
                    region: self.params.get("region").cloned(),
                    access_key: self.params.get("access_key").cloned(),
                    secret_key: self.params.get("secret_key").cloned(),
                    format,
                    properties: self.params.clone(),
                    batch_config: Default::default(),
                })
            }
            "clickhouse" => {
                let connection_string = match (&self.host, self.port) {
                    (Some(host), Some(port)) => format!("{}:{}", host, port),
                    (Some(host), None) => format!("{}:8123", host), // Default HTTP port
                    _ => {
                        return Err(DataSourceError::Configuration(
                            "ClickHouse source requires host".to_string(),
                        ));
                    }
                };

                let database = self.path.trim_start_matches('/').to_string();
                if database.is_empty() {
                    return Err(DataSourceError::Configuration(
                        "ClickHouse source requires database".to_string(),
                    ));
                }

                let table = self
                    .params
                    .get("table")
                    .ok_or_else(|| {
                        DataSourceError::Configuration(
                            "ClickHouse source requires table parameter".to_string(),
                        )
                    })?
                    .clone();

                Ok(SourceConfig::ClickHouse {
                    connection_string,
                    database,
                    table,
                    query: self.params.get("query").cloned(),
                    properties: self.params.clone(),
                    batch_config: Default::default(),
                })
            }
            _ => Ok(SourceConfig::Generic {
                source_type: self.scheme.clone(),
                properties: self.params.clone(),
                batch_config: Default::default(),
            }),
        }
    }

    /// Convert to a sink configuration
    pub fn to_sink_config(&self) -> Result<SinkConfig, DataSourceError> {
        match self.scheme.as_str() {
            "kafka" => {
                let brokers = match (&self.host, self.port) {
                    (Some(host), Some(port)) => format!("{}:{}", host, port),
                    (Some(host), None) => format!("{}:9092", host),
                    _ => {
                        return Err(DataSourceError::Configuration(
                            "Kafka sink requires host".to_string(),
                        ));
                    }
                };

                let topic = self.path.trim_start_matches('/').to_string();
                if topic.is_empty() {
                    return Err(DataSourceError::Configuration(
                        "Kafka sink requires topic".to_string(),
                    ));
                }

                Ok(SinkConfig::Kafka {
                    brokers,
                    topic,
                    properties: self.params.clone(),
                })
            }
            "file" => {
                let format = match self.params.get("format").map(|s| s.as_str()) {
                    Some("json") => FileFormat::Json,
                    Some("jsonl") | Some("jsonlines") => FileFormat::JsonLines,
                    Some("csv") => FileFormat::Csv {
                        header: self
                            .params
                            .get("header")
                            .map(|h| h.parse().unwrap_or(true))
                            .unwrap_or(true),
                        delimiter: self
                            .params
                            .get("delimiter")
                            .and_then(|d| d.chars().next())
                            .unwrap_or(','),
                        quote: self
                            .params
                            .get("quote")
                            .and_then(|q| q.chars().next())
                            .unwrap_or('"'),
                    },
                    Some("parquet") => FileFormat::Parquet,
                    Some("avro") => FileFormat::Avro,
                    Some("orc") => FileFormat::Orc,
                    _ => FileFormat::JsonLines, // Default
                };

                let compression = self.params.get("compression").map(|c| match c.as_str() {
                    "gzip" => CompressionType::Gzip,
                    "snappy" => CompressionType::Snappy,
                    "lz4" => CompressionType::Lz4,
                    "zstd" => CompressionType::Zstd,
                    _ => CompressionType::None,
                });

                Ok(SinkConfig::File {
                    path: self.path.clone(),
                    format,
                    compression,
                    properties: self.params.clone(),
                })
            }
            "iceberg" => {
                // Parse iceberg://catalog.namespace.table
                let table_parts: Vec<&str> = self.path.split('.').collect();
                if table_parts.len() != 3 {
                    return Err(DataSourceError::Configuration(
                        "Iceberg sink requires catalog.namespace.table format".to_string(),
                    ));
                }

                Ok(SinkConfig::Iceberg {
                    catalog_uri: self
                        .params
                        .get("catalog_uri")
                        .unwrap_or(&"http://localhost:8181".to_string())
                        .clone(),
                    warehouse: self
                        .params
                        .get("warehouse")
                        .unwrap_or(&"s3://warehouse/".to_string())
                        .clone(),
                    namespace: format!("{}.{}", table_parts[0], table_parts[1]),
                    table: table_parts[2].to_string(),
                    properties: self.params.clone(),
                })
            }
            "clickhouse" => {
                let connection_string = match (&self.host, self.port) {
                    (Some(host), Some(port)) => format!("{}:{}", host, port),
                    (Some(host), None) => format!("{}:8123", host), // Default HTTP port
                    _ => {
                        return Err(DataSourceError::Configuration(
                            "ClickHouse sink requires host".to_string(),
                        ));
                    }
                };

                let database = self.path.trim_start_matches('/').to_string();
                if database.is_empty() {
                    return Err(DataSourceError::Configuration(
                        "ClickHouse sink requires database".to_string(),
                    ));
                }

                let table = self
                    .params
                    .get("table")
                    .ok_or_else(|| {
                        DataSourceError::Configuration(
                            "ClickHouse sink requires table parameter".to_string(),
                        )
                    })?
                    .clone();

                Ok(SinkConfig::ClickHouse {
                    connection_string,
                    database,
                    table,
                    properties: self.params.clone(),
                })
            }
            _ => Ok(SinkConfig::Generic {
                sink_type: self.scheme.clone(),
                properties: self.params.clone(),
            }),
        }
    }
}

impl FromStr for ConnectionString {
    type Err = DataSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// ConfigSchemaProvider implementation for BatchConfig
/// This provides validation and schema information for batch processing configuration
impl ConfigSchemaProvider for BatchConfig {
    fn config_type_id() -> &'static str {
        "batch_config"
    }

    fn inheritable_properties() -> Vec<&'static str> {
        vec![
            "batch.size",
            "batch.timeout",
            "batch.strategy",
            "batch.enable",
            "batch.max_size",
            "batch.min_size",
            "batch.target_latency",
            "batch.max_wait_time",
            "batch.eager_processing",
        ]
    }

    fn required_named_properties() -> Vec<&'static str> {
        vec![] // All batch properties have sensible defaults
    }

    fn optional_properties_with_defaults() -> HashMap<&'static str, PropertyDefault> {
        let mut defaults = HashMap::new();
        defaults.insert("batch.size", PropertyDefault::Static("100".to_string()));
        defaults.insert("batch.timeout", PropertyDefault::Static("1000".to_string()));
        defaults.insert(
            "batch.strategy",
            PropertyDefault::Static("FixedSize".to_string()),
        );
        defaults.insert("batch.enable", PropertyDefault::Static("true".to_string()));
        defaults.insert(
            "batch.max_size",
            PropertyDefault::Static("1000".to_string()),
        );
        defaults
    }

    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "batch.size" | "max_batch_size" => {
                if let Ok(size) = value.parse::<usize>() {
                    if size == 0 {
                        return Err(vec!["batch.size must be greater than 0".to_string()]);
                    }
                    if size > 100_000 {
                        return Err(vec!["batch.size must not exceed 100,000".to_string()]);
                    }
                } else {
                    return Err(vec![
                        "batch.size must be a valid positive integer".to_string()
                    ]);
                }
            }
            "batch.timeout" | "batch_timeout" => {
                if let Ok(timeout_ms) = value.parse::<u64>() {
                    if timeout_ms == 0 {
                        return Err(vec!["batch.timeout must be greater than 0ms".to_string()]);
                    }
                    if timeout_ms > 300_000 {
                        // 5 minutes max
                        return Err(vec![
                            "batch.timeout must not exceed 300,000ms (5 minutes)".to_string()
                        ]);
                    }
                } else {
                    return Err(vec![
                        "batch.timeout must be a valid timeout in milliseconds".to_string(),
                    ]);
                }
            }
            "batch.strategy" => {
                let valid_strategies = [
                    "FixedSize",
                    "TimeWindow",
                    "AdaptiveSize",
                    "MemoryBased",
                    "LowLatency",
                ];
                if !valid_strategies.contains(&value) {
                    return Err(vec![format!(
                        "batch.strategy must be one of: {}. Got: {}",
                        valid_strategies.join(", "),
                        value
                    )]);
                }
            }
            "batch.enable" | "enable_batching" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec!["batch.enable must be 'true' or 'false'".to_string()]);
                }
            }
            "batch.min_size" => {
                if let Ok(size) = value.parse::<usize>() {
                    if size == 0 {
                        return Err(vec!["batch.min_size must be greater than 0".to_string()]);
                    }
                } else {
                    return Err(vec![
                        "batch.min_size must be a valid positive integer".to_string()
                    ]);
                }
            }
            "batch.max_size" => {
                if let Ok(size) = value.parse::<usize>() {
                    if size == 0 {
                        return Err(vec!["batch.max_size must be greater than 0".to_string()]);
                    }
                    if size > 1_000_000 {
                        return Err(vec!["batch.max_size must not exceed 1,000,000".to_string()]);
                    }
                } else {
                    return Err(vec![
                        "batch.max_size must be a valid positive integer".to_string()
                    ]);
                }
            }
            "batch.target_latency" => {
                if let Ok(latency_ms) = value.parse::<u64>() {
                    if latency_ms == 0 {
                        return Err(vec![
                            "batch.target_latency must be greater than 0ms".to_string()
                        ]);
                    }
                } else {
                    return Err(vec![
                        "batch.target_latency must be a valid timeout in milliseconds".to_string(),
                    ]);
                }
            }
            "batch.max_wait_time" => {
                if let Ok(wait_ms) = value.parse::<u64>() {
                    if wait_ms > 10_000 {
                        // 10 seconds max for low latency
                        return Err(vec![
                            "batch.max_wait_time must not exceed 10,000ms for low latency batching"
                                .to_string(),
                        ]);
                    }
                } else {
                    return Err(vec![
                        "batch.max_wait_time must be a valid timeout in milliseconds".to_string(),
                    ]);
                }
            }
            "batch.eager_processing" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec![
                        "batch.eager_processing must be 'true' or 'false'".to_string()
                    ]);
                }
            }
            _ => {
                // Allow unknown batch properties for extensibility
                if !key.starts_with("batch.") {
                    return Err(vec![format!(
                        "Unknown batch property: {}. Batch properties must start with 'batch.'",
                        key
                    )]);
                }
            }
        }
        Ok(())
    }

    fn json_schema() -> Value {
        serde_json::json!({
            "type": "object",
            "title": "Batch Configuration Schema",
            "description": "Configuration schema for batch processing in Velostream",
            "properties": {
                "batch.size": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100000,
                    "default": 100,
                    "description": "Number of records to process in each batch"
                },
                "batch.timeout": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 300000,
                    "default": 1000,
                    "description": "Maximum time to wait for batch to fill (milliseconds)"
                },
                "batch.strategy": {
                    "type": "string",
                    "enum": ["FixedSize", "TimeWindow", "AdaptiveSize", "MemoryBased", "LowLatency"],
                    "default": "FixedSize",
                    "description": "Batching strategy to use for processing"
                },
                "batch.enable": {
                    "type": "boolean",
                    "default": true,
                    "description": "Whether to enable batch processing"
                },
                "batch.max_size": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 1000000,
                    "default": 1000,
                    "description": "Hard limit for maximum batch size to prevent memory issues"
                },
                "batch.min_size": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Minimum batch size for adaptive batching"
                },
                "batch.target_latency": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Target latency for adaptive batching (milliseconds)"
                },
                "batch.max_wait_time": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 10000,
                    "description": "Maximum wait time for low latency batching (milliseconds)"
                },
                "batch.eager_processing": {
                    "type": "boolean",
                    "description": "Whether to process immediately when any record is available"
                }
            },
            "additionalProperties": false
        })
    }

    fn property_validations() -> Vec<PropertyValidation> {
        vec![
            PropertyValidation {
                key: "batch.size".to_string(),
                required: false,
                default: Some(PropertyDefault::Static("100".to_string())),
                description: "Number of records to process in each batch".to_string(),
                json_type: "integer".to_string(),
                validation_pattern: Some("1-100000".to_string()),
            },
            PropertyValidation {
                key: "batch.timeout".to_string(),
                required: false,
                default: Some(PropertyDefault::Static("1000".to_string())),
                description: "Maximum time to wait for batch to fill (milliseconds)".to_string(),
                json_type: "integer".to_string(),
                validation_pattern: Some("1-300000".to_string()),
            },
            PropertyValidation {
                key: "batch.strategy".to_string(),
                required: false,
                default: Some(PropertyDefault::Static("FixedSize".to_string())),
                description: "Batching strategy to use for processing".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some(
                    "FixedSize|TimeWindow|AdaptiveSize|MemoryBased|LowLatency".to_string(),
                ),
            },
        ]
    }

    fn supports_custom_properties() -> bool {
        true // Allow custom batch.* properties for extensibility
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

        // Check global context for batch properties
        if let Some(global_value) = global_context.global_properties.get(key) {
            return Ok(Some(global_value.clone()));
        }

        // Use defaults from the BatchConfig implementation
        match key {
            "batch.size" => Ok(Some("100".to_string())),
            "batch.timeout" => Ok(Some("1000".to_string())),
            "batch.strategy" => Ok(Some("FixedSize".to_string())),
            "batch.enable" => Ok(Some("true".to_string())),
            "batch.max_size" => Ok(Some("1000".to_string())),
            _ => Ok(None),
        }
    }
}
