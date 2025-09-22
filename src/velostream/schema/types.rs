//! Core Schema Types
//!
//! This module contains the fundamental schema types used throughout the
//! Velostream schema system.

use crate::velostream::sql::ast::DataType;
use std::collections::HashMap;

/// A schema definition containing field definitions and metadata
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub fields: Vec<FieldDefinition>,
    pub version: Option<String>,
    pub metadata: SchemaMetadata,
}

/// Definition of a single field in a schema
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub description: Option<String>,
    pub default_value: Option<String>,
}

/// Metadata associated with a schema
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaMetadata {
    pub source_type: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub tags: HashMap<String, String>,
    pub compatibility: CompatibilityMode,
}

/// Schema compatibility modes
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

/// Handle to a stream with schema information
#[derive(Debug, Clone)]
pub struct StreamHandle {
    pub id: String,
    pub topic: String,
    pub schema_id: String,
}

/// Metadata about a stream
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub record_count: u64,
    pub last_updated: i64,
    pub partitions: Vec<PartitionMetadata>,
}

/// Metadata about a stream partition
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub partition_id: i32,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

// Implementation methods from the original mod.rs
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
