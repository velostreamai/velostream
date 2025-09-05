//! Configuration management for datasources
//!
//! This module provides configuration abstractions and implementations for various
//! datasource types including Kafka, Files, and others. It includes both basic
//! configuration types and advanced unified configuration management.

pub mod unified;

// Re-export commonly used types from the unified module
pub use unified::{
    defaults, BatchConfigApplicator, ConfigFactory, ConfigLogger, PropertySuggestor,
    StructConfigSuggestor,
};

// Import legacy configuration types
pub mod legacy;

// Re-export configuration types from the legacy module for compatibility
pub use legacy::{
    BatchConfig, BatchStrategy, CdcFormat, CompressionType, ConnectionString, FileFormat,
    SinkConfig, SourceConfig, WriteMode,
};
