//! Schema Management System
//!
//! This module provides comprehensive schema management for heterogeneous data sources.
//! It includes schema discovery, evolution, caching, and validation across different
//! data source types.
//!
//! ## Key Components
//!
//! - **SchemaRegistry**: Central registry for managing schemas across data sources
//! - **SchemaProvider**: Plugin system for source-specific schema discovery
//! - **SchemaEvolution**: Handles backward/forward compatibility and migrations
//! - **SchemaCache**: TTL-based caching with version tracking
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use ferrisstreams::ferris::sql::schema::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let mut registry = SchemaRegistry::new();
//!     
//!     // Discover schema from Kafka topic
//!     let schema = registry.discover("kafka://localhost:9092/orders").await?;
//!     println!("Discovered schema: {:?}", schema);
//!     
//!     // Check schema evolution compatibility
//!     let evolution = SchemaEvolution::new();
//!     let old_schema = schema.clone(); // Example: use discovered schema as old
//!     let new_schema = schema; // Example: use same schema as new
//!     let can_migrate = evolution.can_evolve(&old_schema, &new_schema);
//!     
//!     Ok(())
//! }
//! ```

pub mod cache;
pub mod enhanced_cache;
pub mod evolution;
pub mod providers;
pub mod reference_resolver;
pub mod registry;
pub mod registry_backend;
pub mod registry_client;
pub mod schema_backends;
pub mod unified_registry_client;

// Re-export key components for easy access
pub use cache::{CacheConfig, SchemaCache};
pub use enhanced_cache::{CacheConfig as EnhancedCacheConfig, CacheMetrics, EnhancedSchemaCache};
pub use evolution::SchemaEvolution;
pub use providers::create_default_registry;
pub use reference_resolver::{
    CompatibilityLevel, CompatibilityResult, MigrationPlan, ResolverConfig, RolloutPlan,
    RolloutStrategy, SchemaReferenceResolver,
};
pub use registry::{SchemaProvider, SchemaRegistry};
pub use registry_backend::{
    BackendCapabilities, BackendConfig, BackendMetadata, HealthStatus, SchemaRegistryBackend,
    SchemaRegistryBackendFactory, SchemaResponse,
};
pub use registry_client::{
    AuthConfig, DependencyGraph, ResolvedSchema, SchemaDependency, SchemaReference,
    SchemaRegistryClient,
};
pub use unified_registry_client::{
    UnifiedClientBuilder, UnifiedClientConfig, UnifiedSchemaRegistryClient,
};

use crate::ferris::sql::ast::DataType;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub fields: Vec<FieldDefinition>,
    pub version: Option<String>,
    pub metadata: SchemaMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub description: Option<String>,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaMetadata {
    pub source_type: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub tags: HashMap<String, String>,
    pub compatibility: CompatibilityMode,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityMode {
    /// No compatibility checking
    None,
    /// Only backward compatible changes allowed
    Backward,
    /// Only forward compatible changes allowed
    Forward,
    /// Both backward and forward compatible changes allowed
    Full,
    /// No schema changes allowed
    Strict,
}

#[derive(Debug, Clone)]
pub struct StreamHandle {
    pub id: String,
    pub topic: String,
    pub schema_id: String,
}

#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub record_count: u64,
    pub last_updated: i64,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub partition_id: i32,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

impl Schema {
    pub fn new(fields: Vec<FieldDefinition>) -> Self {
        Self {
            fields,
            version: None,
            metadata: SchemaMetadata::default(),
        }
    }

    pub fn with_version(mut self, version: String) -> Self {
        self.version = Some(version);
        self
    }

    pub fn with_metadata(mut self, metadata: SchemaMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.iter().any(|field| field.name == name)
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldDefinition> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn get_field_type(&self, name: &str) -> Option<&DataType> {
        self.get_field(name).map(|field| &field.data_type)
    }

    pub fn field_names(&self) -> Vec<&str> {
        self.fields
            .iter()
            .map(|field| field.name.as_str())
            .collect()
    }

    pub fn validate_record(&self, record: &HashMap<String, serde_json::Value>) -> bool {
        // Check that all non-nullable fields are present
        for field in &self.fields {
            if !field.nullable && !record.contains_key(&field.name) {
                return false;
            }

            if let Some(value) = record.get(&field.name) {
                if !self.value_matches_type(value, &field.data_type) {
                    return false;
                }
            }
        }
        true
    }

    fn value_matches_type(&self, value: &serde_json::Value, data_type: &DataType) -> bool {
        match (value, data_type) {
            (serde_json::Value::Null, _) => true, // Null is always acceptable
            (serde_json::Value::Bool(_), DataType::Boolean) => true,
            (serde_json::Value::Number(n), DataType::Integer) => n.is_i64(),
            (serde_json::Value::Number(n), DataType::Float) => n.is_f64(),
            (serde_json::Value::String(_), DataType::String) => true,
            (serde_json::Value::Number(n), DataType::Timestamp) => n.is_i64(),
            (serde_json::Value::Array(_), DataType::Array(_)) => true, // Simplified validation
            (serde_json::Value::Object(_), DataType::Map(_, _)) => true, // Simplified validation
            _ => false,
        }
    }

    /// Compare this schema with another for compatibility
    pub fn is_compatible_with(&self, other: &Schema) -> bool {
        match &self.metadata.compatibility {
            CompatibilityMode::None => true,
            CompatibilityMode::Strict => self == other,
            _ => {
                // TODO: Implement proper compatibility checking based on mode
                // For now, just check field compatibility
                self.fields_compatible_with(&other.fields)
            }
        }
    }

    fn fields_compatible_with(&self, other_fields: &[FieldDefinition]) -> bool {
        // Basic compatibility: all required fields in other must exist here
        for other_field in other_fields {
            if !other_field.nullable && !self.has_field(&other_field.name) {
                return false;
            }
        }
        true
    }
}

impl FieldDefinition {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
            description: None,
            default_value: None,
        }
    }

    pub fn required(name: String, data_type: DataType) -> Self {
        Self::new(name, data_type, false)
    }

    pub fn optional(name: String, data_type: DataType) -> Self {
        Self::new(name, data_type, true)
    }

    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    pub fn with_default(mut self, default_value: String) -> Self {
        self.default_value = Some(default_value);
        self
    }
}

impl SchemaMetadata {
    pub fn new(source_type: String) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            source_type,
            created_at: now,
            updated_at: now,
            tags: HashMap::new(),
            compatibility: CompatibilityMode::Backward,
        }
    }

    pub fn with_compatibility(mut self, mode: CompatibilityMode) -> Self {
        self.compatibility = mode;
        self
    }

    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }
}

impl Default for SchemaMetadata {
    fn default() -> Self {
        Self::new("unknown".to_string())
    }
}

impl StreamHandle {
    pub fn new(id: String, topic: String, schema_id: String) -> Self {
        Self {
            id,
            topic,
            schema_id,
        }
    }
}

impl StreamMetadata {
    pub fn new() -> Self {
        Self {
            record_count: 0,
            last_updated: chrono::Utc::now().timestamp(),
            partitions: Vec::new(),
        }
    }

    pub fn with_partitions(mut self, partitions: Vec<PartitionMetadata>) -> Self {
        self.partitions = partitions;
        self
    }

    pub fn update_record_count(&mut self, count: u64) {
        self.record_count = count;
        self.last_updated = chrono::Utc::now().timestamp();
    }
}

impl Default for StreamMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionMetadata {
    pub fn new(partition_id: i32, low_watermark: i64, high_watermark: i64) -> Self {
        Self {
            partition_id,
            low_watermark,
            high_watermark,
        }
    }
}

/// Error types for schema management operations
#[derive(Debug)]
pub enum SchemaError {
    NotFound {
        source: String,
    },
    Incompatible {
        reason: String,
    },
    Provider {
        source: String,
        message: String,
    },
    Cache {
        message: String,
    },
    Evolution {
        from: String,
        to: String,
        reason: String,
    },
    Validation {
        message: String,
    },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::NotFound { source } => write!(f, "Schema not found: {}", source),
            SchemaError::Incompatible { reason } => write!(f, "Schema incompatible: {}", reason),
            SchemaError::Provider { source, message } => {
                write!(f, "Schema provider error: {} - {}", source, message)
            }
            SchemaError::Cache { message } => write!(f, "Schema cache error: {}", message),
            SchemaError::Evolution { from, to, reason } => {
                write!(f, "Schema evolution error: {} -> {}: {}", from, to, reason)
            }
            SchemaError::Validation { message } => {
                write!(f, "Schema validation error: {}", message)
            }
        }
    }
}

impl std::error::Error for SchemaError {}

pub type SchemaResult<T> = Result<T, SchemaError>;
