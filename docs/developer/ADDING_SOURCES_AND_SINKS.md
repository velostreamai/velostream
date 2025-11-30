# Developer Guide: Adding New Data Sources and Sinks

This guide walks you through implementing custom data sources and sinks for Velostream. Follow the phased approach to ensure a complete, production-ready implementation.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Phase 1: Core Implementation](#phase-1-core-implementation)
3. [Phase 2: Configuration & Factory](#phase-2-configuration--factory)
4. [Phase 3: Registry Integration](#phase-3-registry-integration)
5. [Phase 4: Test Harness Support](#phase-4-test-harness-support)
6. [Phase 5: SQL Integration](#phase-5-sql-integration)
7. [Phase 6: Testing](#phase-6-testing)
8. [Reference Implementations](#reference-implementations)
9. [Checklist](#implementation-checklist)

---

## Architecture Overview

Velostream uses a layered architecture for data sources and sinks:

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQL Layer                                 │
│   CREATE STREAM orders FROM kafka://... WITH (...)              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Registry Layer                                │
│   DataSourceRegistry.create_source("kafka://...")               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Factory Layer                                 │
│   KafkaDataSource::from_properties(props)                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Trait Layer                                   │
│   DataSource → DataReader → StreamRecord                        │
│   DataSink   → DataWriter → StreamRecord                        │
└─────────────────────────────────────────────────────────────────┘
```

### Core Traits

| Trait | Purpose | Key Methods |
|-------|---------|-------------|
| `DataSource` | Input abstraction | `initialize()`, `create_reader()`, `fetch_schema()` |
| `DataSink` | Output abstraction | `initialize()`, `create_writer()`, `validate_schema()` |
| `DataReader` | Streaming input | `read()`, `commit()`, `seek()`, `has_more()` |
| `DataWriter` | Streaming output | `write()`, `write_batch()`, `flush()`, `commit()` |

### Key Files Reference

| Component | Path |
|-----------|------|
| Core Traits | `src/velostream/datasource/traits.rs` |
| Configuration Types | `src/velostream/datasource/config/types.rs` |
| Registry | `src/velostream/datasource/registry.rs` |
| Kafka Implementation | `src/velostream/datasource/kafka/` |
| File Implementation | `src/velostream/datasource/file/` |
| Test Harness Spec | `src/velostream/test_harness/spec.rs` |

---

## Phase 1: Core Implementation

**Goal:** Implement the fundamental `DataSource`/`DataSink` and `DataReader`/`DataWriter` traits.

### Step 1.1: Create Module Structure

```bash
mkdir -p src/velostream/datasource/redis
touch src/velostream/datasource/redis/mod.rs
touch src/velostream/datasource/redis/data_source.rs
touch src/velostream/datasource/redis/data_sink.rs
touch src/velostream/datasource/redis/reader.rs
touch src/velostream/datasource/redis/writer.rs
touch src/velostream/datasource/redis/config.rs
touch src/velostream/datasource/redis/error.rs
```

### Step 1.2: Define Configuration

```rust
// src/velostream/datasource/redis/config.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for Redis data source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisSourceConfig {
    /// Redis connection URL (e.g., "redis://localhost:6379")
    pub url: String,

    /// Key pattern to subscribe to (e.g., "events:*")
    pub key_pattern: String,

    /// Data format (json, msgpack, etc.)
    #[serde(default = "default_format")]
    pub format: String,

    /// Batch size for reads
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Connection pool size
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,

    /// Additional properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

fn default_format() -> String { "json".to_string() }
fn default_batch_size() -> usize { 1000 }
fn default_pool_size() -> usize { 4 }

impl Default for RedisSourceConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            key_pattern: "*".to_string(),
            format: default_format(),
            batch_size: default_batch_size(),
            pool_size: default_pool_size(),
            properties: HashMap::new(),
        }
    }
}

impl RedisSourceConfig {
    /// Parse from properties HashMap with fallback chain
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        // Helper: check source.{key} first, then {key}
        let get_prop = |key: &str| -> Option<String> {
            props.get(&format!("source.{}", key))
                .or_else(|| props.get(key))
                .cloned()
        };

        Self {
            url: get_prop("url")
                .or_else(|| get_prop("connection_string"))
                .unwrap_or_else(|| "redis://localhost:6379".to_string()),
            key_pattern: get_prop("key_pattern")
                .or_else(|| get_prop("pattern"))
                .unwrap_or_else(|| "*".to_string()),
            format: get_prop("format").unwrap_or_else(default_format),
            batch_size: get_prop("batch_size")
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_batch_size),
            pool_size: get_prop("pool_size")
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_pool_size),
            properties: props.clone(),
        }
    }
}
```

### Step 1.3: Implement DataSource

```rust
// src/velostream/datasource/redis/data_source.rs

use async_trait::async_trait;
use std::collections::HashMap;
use crate::velostream::datasource::traits::{DataSource, DataReader};
use crate::velostream::datasource::config::types::{
    SourceConfig, SourceMetadata, BatchConfig,
};
use crate::velostream::sql::execution::types::Schema;
use super::config::RedisSourceConfig;
use super::reader::RedisDataReader;
use super::error::RedisError;

pub struct RedisDataSource {
    config: Option<RedisSourceConfig>,
    metadata: Option<SourceMetadata>,
    // Connection pool would go here
    // pool: Option<redis::aio::ConnectionManager>,
}

impl RedisDataSource {
    pub fn new() -> Self {
        Self {
            config: None,
            metadata: None,
        }
    }

    /// Factory method from properties (most common entry point)
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let config = RedisSourceConfig::from_properties(props);
        Self {
            config: Some(config),
            metadata: Some(Self::build_metadata()),
        }
    }

    fn build_metadata() -> SourceMetadata {
        SourceMetadata {
            source_type: "redis".to_string(),
            version: "1.0.0".to_string(),
            supports_streaming: true,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec![
                "pub_sub".to_string(),
                "streams".to_string(),
                "key_value".to_string(),
            ],
        }
    }
}

#[async_trait]
impl DataSource for RedisDataSource {
    async fn initialize(&mut self, config: SourceConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Extract config from SourceConfig enum
        let redis_config = match config {
            SourceConfig::Generic { properties, .. } => {
                RedisSourceConfig::from_properties(&properties)
            }
            _ => return Err("Unsupported config type for Redis".into()),
        };

        // Initialize connection pool here
        // self.pool = Some(create_connection_pool(&redis_config).await?);

        self.config = Some(redis_config);
        self.metadata = Some(Self::build_metadata());

        Ok(())
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        // Redis is schema-less, return a generic schema or infer from data
        // For structured data, you might sample records to infer schema
        Ok(Schema::default())
    }

    async fn create_reader(&self) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config.as_ref()
            .ok_or("DataSource not initialized")?;

        let reader = RedisDataReader::new(config.clone()).await?;
        Ok(Box::new(reader))
    }

    async fn create_reader_with_batch_config(
        &self,
        batch_config: BatchConfig
    ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config.as_ref()
            .ok_or("DataSource not initialized")?;

        let mut reader = RedisDataReader::new(config.clone()).await?;
        reader.set_batch_config(batch_config);
        Ok(Box::new(reader))
    }

    fn supports_streaming(&self) -> bool {
        true  // Redis Streams and Pub/Sub support streaming
    }

    fn supports_batch(&self) -> bool {
        true  // Can also read keys in batch
    }

    fn metadata(&self) -> SourceMetadata {
        self.metadata.clone().unwrap_or_else(Self::build_metadata)
    }
}
```

### Step 1.4: Implement DataReader

```rust
// src/velostream/datasource/redis/reader.rs

use async_trait::async_trait;
use std::collections::HashMap;
use crate::velostream::datasource::traits::DataReader;
use crate::velostream::datasource::config::types::{SourceOffset, BatchConfig};
use crate::velostream::sql::execution::types::{StreamRecord, FieldValue};
use super::config::RedisSourceConfig;

pub struct RedisDataReader {
    config: RedisSourceConfig,
    batch_config: Option<BatchConfig>,
    // Redis-specific state
    cursor: Option<String>,
    has_more: bool,
}

impl RedisDataReader {
    pub async fn new(config: RedisSourceConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Initialize Redis connection
        // let client = redis::Client::open(config.url.as_str())?;

        Ok(Self {
            config,
            batch_config: None,
            cursor: None,
            has_more: true,
        })
    }

    pub fn set_batch_config(&mut self, config: BatchConfig) {
        self.batch_config = Some(config);
    }

    fn parse_record(&self, key: &str, value: &[u8]) -> Result<StreamRecord, Box<dyn std::error::Error + Send + Sync>> {
        // Parse based on format (JSON, MessagePack, etc.)
        match self.config.format.as_str() {
            "json" => {
                let json: serde_json::Value = serde_json::from_slice(value)?;
                let mut fields = HashMap::new();

                if let serde_json::Value::Object(obj) = json {
                    for (k, v) in obj {
                        fields.insert(k, self.json_to_field_value(v));
                    }
                }

                // Add the key as a field
                fields.insert("_key".to_string(), FieldValue::String(key.to_string()));

                Ok(StreamRecord::new(fields))
            }
            _ => Err(format!("Unsupported format: {}", self.config.format).into()),
        }
    }

    fn json_to_field_value(&self, value: serde_json::Value) -> FieldValue {
        match value {
            serde_json::Value::Null => FieldValue::Null,
            serde_json::Value::Bool(b) => FieldValue::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    FieldValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    FieldValue::Float(f)
                } else {
                    FieldValue::String(n.to_string())
                }
            }
            serde_json::Value::String(s) => FieldValue::String(s),
            serde_json::Value::Array(arr) => {
                FieldValue::Array(arr.into_iter().map(|v| self.json_to_field_value(v)).collect())
            }
            serde_json::Value::Object(obj) => {
                let map: HashMap<String, FieldValue> = obj
                    .into_iter()
                    .map(|(k, v)| (k, self.json_to_field_value(v)))
                    .collect();
                FieldValue::Struct(map)
            }
        }
    }
}

#[async_trait]
impl DataReader for RedisDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let batch_size = self.batch_config
            .as_ref()
            .map(|c| c.max_batch_size)
            .unwrap_or(self.config.batch_size);

        // TODO: Implement actual Redis read
        // Example using SCAN for key-value:
        // let (new_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
        //     .arg(&self.cursor.unwrap_or("0"))
        //     .arg("MATCH").arg(&self.config.key_pattern)
        //     .arg("COUNT").arg(batch_size)
        //     .query_async(&mut conn).await?;

        // For now, return empty to indicate no more data
        self.has_more = false;
        Ok(vec![])
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For Redis Streams, acknowledge processed messages
        // For key-value mode, this is a no-op
        Ok(())
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match offset {
            SourceOffset::Generic(cursor) => {
                self.cursor = Some(cursor);
                self.has_more = true;
                Ok(())
            }
            _ => Err("Unsupported offset type for Redis".into()),
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.has_more)
    }

    // Transaction support (optional for Redis)
    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(false)  // Redis doesn't support read transactions in this context
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        false
    }
}
```

### Step 1.5: Define Error Types

```rust
// src/velostream/datasource/redis/error.rs

use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum RedisError {
    ConnectionError(String),
    ConfigurationError(String),
    ParseError(String),
    ReadError(String),
    WriteError(String),
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisError::ConnectionError(msg) => write!(f, "Redis connection error: {}", msg),
            RedisError::ConfigurationError(msg) => write!(f, "Redis configuration error: {}", msg),
            RedisError::ParseError(msg) => write!(f, "Redis parse error: {}", msg),
            RedisError::ReadError(msg) => write!(f, "Redis read error: {}", msg),
            RedisError::WriteError(msg) => write!(f, "Redis write error: {}", msg),
        }
    }
}

impl Error for RedisError {}

// Enable conversion to Box<dyn Error>
impl From<RedisError> for Box<dyn Error + Send + Sync> {
    fn from(err: RedisError) -> Self {
        Box::new(err)
    }
}
```

### Step 1.6: Create Module Entry Point

```rust
// src/velostream/datasource/redis/mod.rs

pub mod config;
pub mod data_source;
pub mod data_sink;
pub mod reader;
pub mod writer;
pub mod error;

pub use config::RedisSourceConfig;
pub use data_source::RedisDataSource;
pub use data_sink::RedisDataSink;
pub use reader::RedisDataReader;
pub use writer::RedisDataWriter;
pub use error::RedisError;
```

---

## Phase 2: Configuration & Factory

**Goal:** Enable flexible configuration through properties, URIs, and config files.

### Step 2.1: Support Property Fallback Chain

The standard property resolution order is:

1. `source.{key}` (explicit source prefix)
2. `{key}` (direct key)
3. Default value

```rust
impl RedisSourceConfig {
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let get_prop = |key: &str| -> Option<String> {
            // 1. Try source.{key}
            props.get(&format!("source.{}", key))
                // 2. Try {key} directly
                .or_else(|| props.get(key))
                .cloned()
        };

        // Build config with defaults
        Self {
            url: get_prop("url")
                .or_else(|| get_prop("connection_string"))
                .or_else(|| get_prop("redis.url"))
                .unwrap_or_else(|| "redis://localhost:6379".to_string()),
            // ... other fields
        }
    }
}
```

### Step 2.2: Support URI Parsing

```rust
impl RedisSourceConfig {
    /// Parse from URI: redis://host:port/db?key_pattern=events:*
    pub fn from_uri(uri: &str) -> Result<Self, RedisError> {
        let url = url::Url::parse(uri)
            .map_err(|e| RedisError::ConfigurationError(e.to_string()))?;

        let mut config = Self::default();
        config.url = format!("{}://{}:{}",
            url.scheme(),
            url.host_str().unwrap_or("localhost"),
            url.port().unwrap_or(6379)
        );

        // Parse query parameters
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "key_pattern" | "pattern" => config.key_pattern = value.to_string(),
                "format" => config.format = value.to_string(),
                "batch_size" => config.batch_size = value.parse().unwrap_or(1000),
                "pool_size" => config.pool_size = value.parse().unwrap_or(4),
                _ => { config.properties.insert(key.to_string(), value.to_string()); }
            }
        }

        Ok(config)
    }
}
```

### Step 2.3: Support Config File Loading

```rust
impl RedisSourceConfig {
    /// Load from YAML/JSON config file
    pub fn from_file(path: &str) -> Result<Self, RedisError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| RedisError::ConfigurationError(format!("Failed to read config: {}", e)))?;

        if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_yaml::from_str(&content)
                .map_err(|e| RedisError::ConfigurationError(e.to_string()))
        } else {
            serde_json::from_str(&content)
                .map_err(|e| RedisError::ConfigurationError(e.to_string()))
        }
    }

    /// Merge with config file if specified in properties
    pub fn from_properties_with_file(props: &HashMap<String, String>) -> Result<Self, RedisError> {
        // Check for config file reference
        let config_file = props.get("source.config_file")
            .or_else(|| props.get("config_file"));

        let base_config = if let Some(path) = config_file {
            Self::from_file(path)?
        } else {
            Self::default()
        };

        // Override with explicit properties
        Ok(Self::from_properties(props).merge_with(base_config))
    }

    fn merge_with(self, base: Self) -> Self {
        Self {
            url: if self.url != Self::default().url { self.url } else { base.url },
            key_pattern: if self.key_pattern != Self::default().key_pattern { self.key_pattern } else { base.key_pattern },
            // ... merge other fields
            ..self
        }
    }
}
```

---

## Phase 3: Registry Integration

**Goal:** Register the new source/sink in the global registry for URI-based creation.

### Step 3.1: Create Registration Function

```rust
// src/velostream/datasource/redis/mod.rs (add)

use crate::velostream::datasource::registry::DataSourceRegistry;
use crate::velostream::datasource::config::types::SourceConfig;

/// Register Redis source with the global registry
pub fn register_redis_source(registry: &mut DataSourceRegistry) {
    registry.register_source("redis", |config| {
        match config {
            SourceConfig::Generic { source_type, properties, .. } if source_type == "redis" => {
                let mut source = RedisDataSource::new();
                // Initialize synchronously for factory pattern
                let redis_config = RedisSourceConfig::from_properties(&properties);
                source.config = Some(redis_config);
                source.metadata = Some(source.build_metadata());
                Ok(Box::new(source))
            }
            _ => Err("Invalid config for Redis source".into()),
        }
    });
}

/// Register Redis sink with the global registry
pub fn register_redis_sink(registry: &mut DataSourceRegistry) {
    registry.register_sink("redis", |config| {
        // Similar pattern for sink
        Ok(Box::new(RedisDataSink::new()))
    });
}
```

### Step 3.2: Add to Default Registry

```rust
// src/velostream/datasource/registry.rs (modify)

impl DataSourceRegistry {
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();

        // Register built-in sources
        crate::velostream::datasource::kafka::register_kafka_source(&mut registry);
        crate::velostream::datasource::file::register_file_source(&mut registry);
        crate::velostream::datasource::redis::register_redis_source(&mut registry);  // Add this

        // Register built-in sinks
        crate::velostream::datasource::kafka::register_kafka_sink(&mut registry);
        crate::velostream::datasource::file::register_file_sink(&mut registry);
        crate::velostream::datasource::redis::register_redis_sink(&mut registry);  // Add this

        registry
    }
}
```

### Step 3.3: Add URI Scheme Support

```rust
// src/velostream/datasource/config/types.rs (modify ConnectionString)

impl ConnectionString {
    pub fn to_source_config(&self) -> Result<SourceConfig, Box<dyn Error + Send + Sync>> {
        match self.scheme.as_str() {
            "kafka" => Ok(SourceConfig::Kafka { /* ... */ }),
            "file" => Ok(SourceConfig::File { /* ... */ }),
            "redis" => Ok(SourceConfig::Generic {
                source_type: "redis".to_string(),
                properties: self.params.clone(),
                batch_config: BatchConfig::default(),
                event_time_config: None,
            }),
            _ => Err(format!("Unknown scheme: {}", self.scheme).into()),
        }
    }
}
```

---

## Phase 4: Test Harness Support

**Goal:** Enable the new source/sink in test specifications.

### Step 4.1: Add to SourceType/SinkType Enums

```rust
// src/velostream/test_harness/spec.rs (modify)

/// Source type specification for test inputs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceType {
    /// Kafka topic source
    Kafka {
        #[serde(default)]
        topic: Option<String>,
    },
    /// File-based source
    File {
        path: String,
        #[serde(default)]
        format: FileFormat,
        #[serde(default)]
        watch: bool,
    },
    /// Redis source (NEW)
    Redis {
        #[serde(default)]
        url: Option<String>,
        #[serde(default)]
        key_pattern: Option<String>,
        #[serde(default)]
        format: Option<String>,
    },
}

/// Sink type specification for test outputs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkType {
    Kafka {
        #[serde(default)]
        topic: Option<String>,
    },
    File {
        path: String,
        #[serde(default)]
        format: FileFormat,
    },
    /// Redis sink (NEW)
    Redis {
        #[serde(default)]
        url: Option<String>,
        #[serde(default)]
        key_pattern: Option<String>,
    },
}
```

### Step 4.2: Add Factory Support

```rust
// src/velostream/test_harness/redis_io.rs (new file)

use super::error::{TestHarnessError, TestHarnessResult};
use super::spec::SourceType;
use crate::velostream::datasource::redis::{RedisDataSource, RedisSourceConfig};
use crate::velostream::sql::execution::types::StreamRecord;
use std::path::Path;

/// Factory for creating Redis-based data sources in tests
pub struct RedisSourceFactory;

impl RedisSourceFactory {
    /// Create a Redis source from test spec configuration
    pub async fn create_source(
        source_type: &SourceType,
        _base_dir: &Path,  // Not used for Redis but kept for API consistency
    ) -> TestHarnessResult<RedisDataSource> {
        match source_type {
            SourceType::Redis { url, key_pattern, format } => {
                let config = RedisSourceConfig {
                    url: url.clone().unwrap_or_else(|| "redis://localhost:6379".to_string()),
                    key_pattern: key_pattern.clone().unwrap_or_else(|| "*".to_string()),
                    format: format.clone().unwrap_or_else(|| "json".to_string()),
                    ..Default::default()
                };

                RedisDataSource::new(config).await
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to create Redis source: {}", e),
                        source: None,
                    })
            }
            _ => Err(TestHarnessError::ConfigError {
                message: "Cannot create Redis source from non-Redis source type".to_string(),
            }),
        }
    }
}
```

### Step 4.3: Register in Test Harness Module

```rust
// src/velostream/test_harness/mod.rs (add)

pub mod redis_io;  // Add this line

pub use redis_io::RedisSourceFactory;  // Re-export
```

---

## Phase 5: SQL Integration

**Goal:** Enable SQL syntax support for the new source/sink.

### Step 5.1: Document SQL Usage

The SQL layer already supports URI-based sources via the `StreamSource::Uri` variant:

```sql
-- Using URI syntax (already supported)
CREATE STREAM events FROM redis://localhost:6379?key_pattern=events:*&format=json;

-- Using WITH clause for configuration
CREATE STREAM orders AS
SELECT * FROM events
WITH (
    'source.type' = 'redis',
    'source.url' = 'redis://localhost:6379',
    'source.key_pattern' = 'orders:*',
    'source.format' = 'json'
);

-- Output to Redis
INSERT INTO redis://localhost:6379?key_pattern=results:{id}
SELECT id, total FROM aggregates;
```

### Step 5.2: Add Parser Support (if needed)

If special SQL syntax is required:

```rust
// src/velostream/sql/parser/source_parser.rs (example)

fn parse_redis_source(&mut self) -> Result<StreamSource, SqlError> {
    // REDIS 'url' KEY PATTERN 'pattern' FORMAT JSON
    self.expect_keyword("REDIS")?;
    let url = self.parse_string_literal()?;

    let key_pattern = if self.match_keywords(&["KEY", "PATTERN"]) {
        Some(self.parse_string_literal()?)
    } else {
        None
    };

    let format = if self.match_keyword("FORMAT") {
        Some(self.parse_identifier()?)
    } else {
        None
    };

    Ok(StreamSource::Uri(format!(
        "redis://{}?key_pattern={}&format={}",
        url,
        key_pattern.unwrap_or_default(),
        format.unwrap_or_else(|| "json".to_string())
    )))
}
```

---

## Phase 6: Testing

**Goal:** Comprehensive test coverage for the new source/sink.

### Step 6.1: Unit Tests

```rust
// tests/unit/datasource/redis_test.rs

use velostream::velostream::datasource::redis::{
    RedisSourceConfig, RedisDataSource, RedisDataReader,
};
use std::collections::HashMap;

#[test]
fn test_config_from_properties() {
    let mut props = HashMap::new();
    props.insert("source.url".to_string(), "redis://myhost:6380".to_string());
    props.insert("source.key_pattern".to_string(), "events:*".to_string());
    props.insert("source.format".to_string(), "json".to_string());
    props.insert("source.batch_size".to_string(), "500".to_string());

    let config = RedisSourceConfig::from_properties(&props);

    assert_eq!(config.url, "redis://myhost:6380");
    assert_eq!(config.key_pattern, "events:*");
    assert_eq!(config.format, "json");
    assert_eq!(config.batch_size, 500);
}

#[test]
fn test_config_from_uri() {
    let config = RedisSourceConfig::from_uri(
        "redis://localhost:6379/0?key_pattern=orders:*&format=json&batch_size=1000"
    ).unwrap();

    assert_eq!(config.url, "redis://localhost:6379");
    assert_eq!(config.key_pattern, "orders:*");
    assert_eq!(config.format, "json");
    assert_eq!(config.batch_size, 1000);
}

#[test]
fn test_config_defaults() {
    let config = RedisSourceConfig::default();

    assert_eq!(config.url, "redis://localhost:6379");
    assert_eq!(config.key_pattern, "*");
    assert_eq!(config.format, "json");
    assert_eq!(config.batch_size, 1000);
}

#[test]
fn test_property_fallback_chain() {
    // Test that source.url takes precedence over url
    let mut props = HashMap::new();
    props.insert("url".to_string(), "redis://fallback:6379".to_string());
    props.insert("source.url".to_string(), "redis://primary:6379".to_string());

    let config = RedisSourceConfig::from_properties(&props);
    assert_eq!(config.url, "redis://primary:6379");
}
```

### Step 6.2: Integration Tests

```rust
// tests/integration/datasource/redis_integration_test.rs

use velostream::velostream::datasource::redis::RedisDataSource;
use velostream::velostream::datasource::traits::DataSource;
use velostream::velostream::datasource::config::types::SourceConfig;
use std::collections::HashMap;

#[tokio::test]
#[ignore]  // Requires running Redis
async fn test_redis_source_lifecycle() {
    let mut props = HashMap::new();
    props.insert("url".to_string(), "redis://localhost:6379".to_string());
    props.insert("key_pattern".to_string(), "test:*".to_string());

    let mut source = RedisDataSource::from_properties(&props);

    // Initialize
    let config = SourceConfig::Generic {
        source_type: "redis".to_string(),
        properties: props,
        batch_config: Default::default(),
        event_time_config: None,
    };
    source.initialize(config).await.expect("Failed to initialize");

    // Check metadata
    let metadata = source.metadata();
    assert_eq!(metadata.source_type, "redis");
    assert!(metadata.supports_streaming);

    // Create reader
    let reader = source.create_reader().await.expect("Failed to create reader");
    assert!(!reader.supports_transactions());
}
```

### Step 6.3: Test Harness Integration Tests

```yaml
# tests/fixtures/redis_test_spec.yaml
application: redis_source_test
description: Test Redis as data source

queries:
  - name: redis_to_kafka
    inputs:
      - source: events
        source_type:
          type: redis
          url: redis://localhost:6379
          key_pattern: "events:*"
          format: json
    output:
      sink_type:
        type: kafka
        topic: output_events
    assertions:
      - type: record_count
        operator: greater_than
        expected: 0
      - type: schema_contains
        fields: [_key, event_type, timestamp]
```

### Step 6.4: Register Test Module

```rust
// tests/unit/datasource/mod.rs

pub mod kafka_test;
pub mod file_test;
pub mod redis_test;  // Add this
```

---

## Reference Implementations

### Kafka (Full-Featured)

The Kafka implementation is the most complete reference:

| File | Description |
|------|-------------|
| `datasource/kafka/data_source.rs` | `KafkaDataSource` with consumer group management |
| `datasource/kafka/reader.rs` | `KafkaDataReader` with offset tracking, transactions |
| `datasource/kafka/writer.rs` | `KafkaDataWriter` with batching, compression |
| `datasource/kafka/property_keys.rs` | Configuration constants |

**Key patterns:**
- Consumer group ID generation with app-awareness
- Schema registry integration
- Transaction support (exactly-once semantics)
- Batch configuration with multiple strategies

### File (Simple)

The File implementation is a good starting point:

| File | Description |
|------|-------------|
| `datasource/file/data_source.rs` | Basic file source |
| `datasource/file/reader.rs` | CSV/JSON parsing |
| `datasource/file/watcher.rs` | File watching for streaming |

**Key patterns:**
- Format detection and parsing
- Schema inference from data
- Watch mode for streaming

---

## Implementation Checklist

### Phase 1: Core Implementation
- [ ] Create module directory structure
- [ ] Define configuration struct with serde support
- [ ] Implement `DataSource` trait
- [ ] Implement `DataReader` trait
- [ ] Define error types
- [ ] Create mod.rs with exports

### Phase 2: Configuration & Factory
- [ ] Property fallback chain (`source.{key}` → `{key}`)
- [ ] URI parsing support
- [ ] Config file loading (YAML/JSON)
- [ ] Config merging/override logic

### Phase 3: Registry Integration
- [ ] Create `register_{type}_source()` function
- [ ] Create `register_{type}_sink()` function
- [ ] Add to `DataSourceRegistry::with_defaults()`
- [ ] Add URI scheme to `ConnectionString::to_source_config()`

### Phase 4: Test Harness Support
- [ ] Add variant to `SourceType` enum
- [ ] Add variant to `SinkType` enum
- [ ] Create `{Type}SourceFactory`
- [ ] Add to test harness mod.rs

### Phase 5: SQL Integration
- [ ] Document SQL syntax
- [ ] Add parser support (if custom syntax needed)
- [ ] Test WITH clause properties

### Phase 6: Testing
- [ ] Unit tests for configuration
- [ ] Unit tests for parsing
- [ ] Integration tests (with real system)
- [ ] Test harness spec example
- [ ] Register test module in mod.rs

---

## Best Practices

1. **Lazy Initialization**: Initialize connections in `DataSource::initialize()`, not constructor
2. **Async by Default**: Use `#[async_trait]` for all I/O operations
3. **Error Chaining**: Preserve source errors with `Box<dyn Error + Send + Sync>`
4. **Batch Optimization**: Honor `BatchConfig` settings for throughput
5. **Zero-Copy**: Use `Arc<StreamRecord>` for efficient batch operations
6. **Capability Declaration**: Accurately report capabilities in `metadata()`
7. **Graceful Shutdown**: Implement proper cleanup in `Drop` or shutdown methods
8. **Connection Pooling**: Use connection pools for database-like sources
9. **Retry Logic**: Implement exponential backoff for transient failures
10. **Logging**: Use `log` crate with appropriate levels (debug for detail, info for lifecycle)

---

## Getting Help

- **Existing implementations**: Study `datasource/kafka/` and `datasource/file/`
- **Trait definitions**: See `datasource/traits.rs`
- **Configuration types**: See `datasource/config/types.rs`
- **Test patterns**: See `tests/unit/datasource/`
