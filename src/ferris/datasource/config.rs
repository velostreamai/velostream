//! Generic configuration system for data sources
//!
//! This module provides generic configuration abstractions that are independent
//! of SQL or any specific query engine. These can be used by any data processing system.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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
    },
    /// File system source configuration  
    File {
        path: String,
        format: FileFormat,
        properties: HashMap<String, String>,
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
    },
    /// Database source configuration (for CDC)
    Database {
        connection_string: String,
        tables: Vec<String>,
        cdc_format: CdcFormat,
        properties: HashMap<String, String>,
    },
    /// Generic configuration for custom sources
    Generic {
        source_type: String,
        properties: HashMap<String, String>,
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
        properties: HashMap<String, String>,
    },
    /// Database sink configuration
    Database {
        connection_string: String,
        table: String,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FileFormat {
    /// JSON format (one JSON object per line)
    Json,
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
    /// Custom format with properties
    Custom {
        format_name: String,
        properties: HashMap<String, String>,
    },
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

impl Default for FileFormat {
    fn default() -> Self {
        FileFormat::Json
    }
}

impl fmt::Display for FileFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileFormat::Json => write!(f, "json"),
            FileFormat::Csv { .. } => write!(f, "csv"),
            FileFormat::Parquet => write!(f, "parquet"),
            FileFormat::Avro => write!(f, "avro"),
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
