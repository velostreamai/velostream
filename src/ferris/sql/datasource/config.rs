//! Configuration system for pluggable data sources
//!
//! This module provides configuration abstractions for different data source types.
//! It supports both URI-based configuration (e.g., "kafka://localhost:9092/topic")
//! and structured configuration objects.

use crate::ferris::sql::datasource::DataSourceError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

/// Configuration for data sources (inputs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceConfig {
    /// Kafka source configuration
    Kafka {
        brokers: String,
        topic: String,
        group_id: Option<String>,
        properties: HashMap<String, String>,
    },
    /// File system source configuration  
    File(crate::ferris::sql::datasource::file::FileSourceConfig),
    /// S3 source configuration
    S3 {
        bucket: String,
        prefix: String,
        region: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        format: FileFormat,
        properties: HashMap<String, String>,
    },
    /// Database source configuration (for CDC)
    Database {
        connection_string: String,
        tables: Vec<String>,
        cdc_format: CdcFormat,
        properties: HashMap<String, String>,
    },
    /// ClickHouse source configuration
    ClickHouse {
        connection_string: String,
        database: String,
        table: String,
        query: Option<String>,
        properties: HashMap<String, String>,
    },
    /// Generic configuration for custom sources
    Generic {
        source_type: String,
        properties: HashMap<String, String>,
    },
}

/// Configuration for data sinks (outputs)
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
    /// Iceberg table sink configuration
    Iceberg {
        catalog_uri: String,
        warehouse: String,
        namespace: String,
        table: String,
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
    /// Database sink configuration
    Database {
        connection_string: String,
        table: String,
        mode: WriteMode,
        properties: HashMap<String, String>,
    },
    /// Generic configuration for custom sinks
    Generic {
        sink_type: String,
        properties: HashMap<String, String>,
    },
}

/// Supported file formats
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFormat {
    Json,
    JsonLines,
    Csv,
    Parquet,
    Avro,
    Orc,
}

/// Compression types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Write modes for sinks
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteMode {
    Append,
    Overwrite,
    Upsert,
    Merge,
}

/// CDC formats
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcFormat {
    Debezium,
    Maxwell,
    Custom(String),
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
                })
            }
            "file" => {
                // Use the new FileSourceConfig URI parsing
                let uri = format!("file://{}", self.path);
                let mut config =
                    crate::ferris::sql::datasource::file::FileSourceConfig::from_uri(&uri)
                        .map_err(DataSourceError::Configuration)?;

                // Override with any additional parameters from the connection string
                for (key, value) in &self.params {
                    match key.as_str() {
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
                        "header" => {
                            config.csv_has_header = value.parse().unwrap_or(true);
                        }
                        _ => {} // Ignore other parameters
                    }
                }

                Ok(SourceConfig::File(config))
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
                        "csv" => FileFormat::Csv,
                        "parquet" => FileFormat::Parquet,
                        "avro" => FileFormat::Avro,
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
                })
            }
            _ => Ok(SourceConfig::Generic {
                source_type: self.scheme.clone(),
                properties: self.params.clone(),
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
                    Some("csv") => FileFormat::Csv,
                    Some("parquet") => FileFormat::Parquet,
                    Some("avro") => FileFormat::Avro,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_uri_parsing() {
        let conn =
            ConnectionString::parse("kafka://localhost:9092/orders?group_id=analytics").unwrap();

        assert_eq!(conn.scheme, "kafka");
        assert_eq!(conn.host, Some("localhost".to_string()));
        assert_eq!(conn.port, Some(9092));
        assert_eq!(conn.path, "/orders");
        assert_eq!(conn.params.get("group_id"), Some(&"analytics".to_string()));
    }

    #[test]
    fn test_s3_uri_parsing() {
        let conn =
            ConnectionString::parse("s3://my-bucket/data/*.parquet?region=us-west-2").unwrap();

        assert_eq!(conn.scheme, "s3");
        assert_eq!(conn.path, "my-bucket/data/*.parquet");
        assert_eq!(conn.params.get("region"), Some(&"us-west-2".to_string()));
    }

    #[test]
    fn test_file_uri_parsing() {
        let conn = ConnectionString::parse("file:///data/input.json?watch=true").unwrap();

        assert_eq!(conn.scheme, "file");
        assert_eq!(conn.path, "/data/input.json");
        assert_eq!(conn.params.get("watch"), Some(&"true".to_string()));
    }

    #[test]
    fn test_kafka_source_config() {
        let conn = ConnectionString::parse("kafka://localhost:9092/orders?group_id=test").unwrap();
        let config = conn.to_source_config().unwrap();

        match config {
            SourceConfig::Kafka {
                brokers,
                topic,
                group_id,
                ..
            } => {
                assert_eq!(brokers, "localhost:9092");
                assert_eq!(topic, "orders");
                assert_eq!(group_id, Some("test".to_string()));
            }
            _ => panic!("Expected Kafka config"),
        }
    }

    #[test]
    fn test_file_source_config() {
        let conn = ConnectionString::parse("file:///data/input.jsonl?format=jsonlines&watch=true")
            .unwrap();
        let config = conn.to_source_config().unwrap();

        match config {
            SourceConfig::File(file_config) => {
                assert_eq!(file_config.path, "/data/input.jsonl");
                assert_eq!(
                    file_config.format,
                    crate::ferris::sql::datasource::file::FileFormat::JsonLines
                );
                assert!(file_config.watch_for_changes);
            }
            _ => panic!("Expected File config"),
        }
    }
}
