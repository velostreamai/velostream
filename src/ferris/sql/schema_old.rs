// Re-export the new comprehensive schema management system
pub use self::schema_management::*;

// Provide a compatibility module that maintains the old interface
mod schema_management {
    pub use super::super::schema::{
        registry, providers, evolution, cache,
        Schema, FieldDefinition, SchemaMetadata, CompatibilityMode,
        SchemaError, SchemaResult,
        StreamHandle, StreamMetadata, PartitionMetadata,
    };
}

use crate::ferris::sql::ast::DataType;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<FieldDefinition>,
}

#[derive(Debug, Clone)]
pub struct FieldDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
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
        Self { fields }
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
}

impl FieldDefinition {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }

    pub fn required(name: String, data_type: DataType) -> Self {
        Self::new(name, data_type, false)
    }

    pub fn optional(name: String, data_type: DataType) -> Self {
        Self::new(name, data_type, true)
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
